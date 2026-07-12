//! Broadcast (lossy) ring tests: round trips across the self-timed wait
//! strategies, independent multi-consumer delivery, late subscription, the
//! seqlock torture test (torn-read detection plus exact loss accounting
//! under a flat-out producer), deterministic lag/reposition mechanics, the
//! slack knob, skip-to-latest, the closed contract, and construction
//! validation.

#![cfg(target_has_atomic = "64")]

use rust_rb::broadcast::{Consumer, NoUninit, PopError, Producer, RingBuffer};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, SleepWait, YieldWait};
use std::time::Duration;

fn make<C: SelfTimed + Send>(min_capacity: usize) -> (Producer<u64>, Consumer<u64, C>) {
    RingBuffer::<u64, C>::with_wait_strategies(min_capacity)
}

// -----------------------------------------------------------------------------
// 1. SINGLE-CONSUMER ROUND TRIP ACROSS WAIT STRATEGIES
// -----------------------------------------------------------------------------

fn round_trip<C: SelfTimed + Send + 'static>() {
    // Blocking, single-threaded.
    let (mut tx, mut rx) = make::<C>(64);
    assert_eq!(rx.try_pop(), Ok(None));
    for i in 0..64 {
        tx.push(i);
    }
    for i in 0..64 {
        assert_eq!(rx.pop(), Ok(i));
    }
    assert_eq!(rx.try_pop(), Ok(None));

    // Threaded: the ring is larger than the message count, so the producer
    // can never lap the reader — every message must arrive, in order.
    let messages: u64 = if cfg!(miri) { 500 } else { 20_000 };
    let capacity = (messages as usize).next_power_of_two() * 2;
    let (mut tx, mut rx) = make::<C>(capacity);
    let consumer = std::thread::spawn(move || {
        let mut expected = 0u64;
        loop {
            match rx.pop() {
                Ok(v) => {
                    assert_eq!(v, expected, "messages must arrive in order, gap-free");
                    expected += 1;
                }
                Err(PopError::Lagged { .. }) => panic!("a ring wider than the stream never laps"),
                Err(PopError::Closed) => break,
            }
        }
        assert_eq!(expected, messages, "every message must be seen");
    });
    for i in 0..messages {
        tx.push(i);
    }
    drop(tx); // closes the ring
    consumer.join().unwrap();
}

#[test]
fn round_trip_pause() {
    round_trip::<PauseWait>();
}

#[test]
fn round_trip_yield() {
    round_trip::<YieldWait>();
}

#[test]
fn round_trip_noop() {
    round_trip::<NoOpWait>();
}

#[test]
fn round_trip_backoff() {
    round_trip::<BackoffWait>();
}

#[test]
fn round_trip_sleep() {
    round_trip::<SleepWait<1_000>>();
}

// -----------------------------------------------------------------------------
// 2. MULTI-CONSUMER: EVERYONE SEES EVERYTHING (KEEPING-UP REGIME)
// -----------------------------------------------------------------------------

#[test]
fn four_consumers_see_all_messages() {
    let (messages, capacity): (u64, usize) = if cfg!(miri) {
        (2_000, 4_096)
    } else {
        (100_000, 131_072)
    };
    // Ring wider than the whole stream: lag is impossible by construction,
    // so all four consumers must independently see all messages in order.
    let (mut tx, rx0) = make::<YieldWait>(capacity);
    let mut consumers = vec![rx0];
    for _ in 1..4 {
        consumers.push(tx.subscribe::<YieldWait>());
    }

    let mut joins = Vec::new();
    for mut rx in consumers {
        joins.push(std::thread::spawn(move || {
            let mut expected = 0u64;
            let mut sum = 0u64;
            loop {
                match rx.pop() {
                    Ok(v) => {
                        assert_eq!(v, expected, "in order, gap-free");
                        expected += 1;
                        sum = sum.wrapping_add(v);
                    }
                    Err(PopError::Lagged { .. }) => panic!("must not lag: ring wider than stream"),
                    Err(PopError::Closed) => break,
                }
            }
            assert_eq!(expected, messages, "every message must be seen");
            sum
        }));
    }

    let producer = std::thread::spawn(move || {
        for i in 0..messages {
            tx.push(i);
        }
    });
    producer.join().unwrap();

    let want = messages * (messages - 1) / 2;
    for join in joins {
        assert_eq!(join.join().unwrap(), want);
    }
}

// -----------------------------------------------------------------------------
// 3. LATE SUBSCRIBE: JOIN AT TAIL
// -----------------------------------------------------------------------------

#[test]
fn late_subscribe_sees_only_post_join() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    for i in 0..8 {
        tx.push(i);
    }

    // Subscribe from the producer handle: joins at the current tail.
    let mut late = tx.subscribe::<YieldWait>();
    assert_eq!(
        late.try_pop(),
        Ok(None),
        "nothing published pre-join is seen"
    );
    assert_eq!(late.lag(), 0);

    for i in 8..12 {
        tx.push(i);
    }
    for i in 8..12 {
        assert_eq!(late.pop(), Ok(i), "everything post-join is seen");
    }
    assert_eq!(late.try_pop(), Ok(None));

    // Subscribe from a consumer handle: same join-at-tail contract.
    let mut late2 = rx.subscribe();
    assert_eq!(late2.try_pop(), Ok(None));
    tx.push(12);
    assert_eq!(late2.pop(), Ok(12));

    // The original consumer still sees the full stream (never lapped).
    for i in 0..13 {
        assert_eq!(rx.pop(), Ok(i));
    }
}

// -----------------------------------------------------------------------------
// 4. THE SEQLOCK TEST: TORN READS + EXACT LOSS ACCOUNTING
// -----------------------------------------------------------------------------

const PRIME: u64 = 0x9E37_79B9_7F4A_7C15;

/// Message `i` carries `[i, i * PRIME]`. A torn read mixes words of two
/// different messages (same slot, generations `capacity` apart), breaking
/// the invariant — so every *accepted* value must satisfy it, positions must
/// advance exactly, and `accepted + missed` must equal the total pushed.
fn seqlock_torture(consumers: usize) {
    let n: u64 = if cfg!(miri) { 2_000 } else { 1_000_000 };
    let (mut tx, rx0) = RingBuffer::<[u64; 2], PauseWait>::with_wait_strategies(8);
    let mut rxs = vec![rx0];
    for _ in 1..consumers {
        rxs.push(tx.subscribe::<PauseWait>());
    }

    let mut joins = Vec::new();
    for mut rx in rxs {
        joins.push(std::thread::spawn(move || {
            let mut next = 0u64; // position accounting: accepted + missed
            let mut accepted = 0u64;
            let mut missed_total = 0u64;
            loop {
                match rx.pop() {
                    Ok(v) => {
                        assert_eq!(
                            v[1],
                            v[0].wrapping_mul(PRIME),
                            "torn read: seqlock validation must have rejected this"
                        );
                        assert_eq!(
                            v[0], next,
                            "accepted positions must be exact and increasing"
                        );
                        next += 1;
                        accepted += 1;
                        // Slow-ish reader: force the producer to lap us.
                        if accepted % 64 == 0 {
                            std::thread::yield_now();
                        }
                    }
                    Err(PopError::Lagged { missed }) => {
                        next += missed;
                        missed_total += missed;
                    }
                    Err(PopError::Closed) => break,
                }
            }
            assert_eq!(next, n, "gap-free accounting: old_pos + missed == new_pos");
            assert_eq!(accepted + missed_total, n, "exact loss accounting");
        }));
    }

    let producer = std::thread::spawn(move || {
        for i in 0..n {
            tx.push([i, i.wrapping_mul(PRIME)]);
        }
    });
    producer.join().unwrap();
    for join in joins {
        join.join().unwrap();
    }
}

#[test]
fn seqlock_validation_one_consumer() {
    seqlock_torture(1);
}

#[test]
fn seqlock_validation_two_consumers() {
    seqlock_torture(2);
}

// -----------------------------------------------------------------------------
// 5. LAGGED MECHANICS, DETERMINISTIC
// -----------------------------------------------------------------------------

#[test]
fn lagged_reposition_exact() {
    let (mut tx, mut rx) = RingBuffer::<u64, YieldWait>::with_slack(8, 2);
    assert_eq!(rx.capacity(), 8);
    assert_eq!(tx.capacity(), 8);

    for i in 0..20 {
        tx.push(i);
    }
    assert_eq!(tx.tail(), 20);
    assert_eq!(rx.lag(), 20);

    // new_pos = tail - capacity + slack = 20 - 8 + 2 = 14; old_pos = 0.
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 14 }));

    // Pops proceed from position 14: capacity - slack = 6 messages remain.
    let mut got = Vec::new();
    while let Ok(Some(v)) = rx.try_pop() {
        got.push(v);
    }
    assert_eq!(got, (14..20).collect::<Vec<_>>());
    assert_eq!(rx.try_pop(), Ok(None));
    assert_eq!(rx.lag(), 0);
}

#[test]
fn lagged_accounting_is_gap_free_across_errors() {
    let (mut tx, mut rx) = RingBuffer::<u64, YieldWait>::with_slack(8, 2);
    let mut next = 0u64;
    let mut accepted = 0u64;
    let mut missed_total = 0u64;
    // Interleave laps and partial drains; the accounting must never gap or
    // overlap.
    for round in 0..5u64 {
        for i in round * 20..(round + 1) * 20 {
            tx.push(i);
        }
        for _ in 0..3 {
            match rx.pop() {
                Ok(v) => {
                    assert_eq!(v, next);
                    next += 1;
                    accepted += 1;
                }
                Err(PopError::Lagged { missed }) => {
                    next += missed;
                    missed_total += missed;
                }
                Err(PopError::Closed) => unreachable!(),
            }
        }
    }
    drop(tx);
    loop {
        match rx.pop() {
            Ok(v) => {
                assert_eq!(v, next);
                next += 1;
                accepted += 1;
            }
            Err(PopError::Lagged { missed }) => {
                next += missed;
                missed_total += missed;
            }
            Err(PopError::Closed) => break,
        }
    }
    assert_eq!(next, 100);
    assert_eq!(accepted + missed_total, 100);
}

// -----------------------------------------------------------------------------
// 6. THE SLACK KNOB
// -----------------------------------------------------------------------------

#[test]
#[should_panic(expected = "slack must be less than the capacity")]
fn slack_at_capacity_rejected() {
    let _ = RingBuffer::<u64, YieldWait>::with_slack(8, 8);
}

#[test]
fn slack_zero_allowed_maximal_salvage() {
    let (mut tx, mut rx) = RingBuffer::<u64, YieldWait>::with_slack(8, 0);
    for i in 0..17 {
        tx.push(i);
    }
    // new_pos = 17 - 8 + 0 = 9: a full capacity of messages is salvaged.
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 9 }));
    for i in 9..17 {
        assert_eq!(rx.pop(), Ok(i));
    }
}

#[test]
fn slack_max_is_capacity_minus_one() {
    let (mut tx, mut rx) = RingBuffer::<u64, YieldWait>::with_slack(8, 7);
    for i in 0..9 {
        tx.push(i);
    }
    // new_pos = 9 - 8 + 7 = 8: only the newest message remains readable.
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 8 }));
    assert_eq!(rx.pop(), Ok(8));
}

#[test]
fn default_slack_is_capacity_over_8_clamped() {
    // capacity 16 -> slack 2.
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    for i in 0..32 {
        tx.push(i);
    }
    assert_eq!(
        rx.pop(),
        Err(PopError::Lagged {
            missed: 32 - 16 + 2
        })
    );
    assert_eq!(rx.pop(), Ok(18));

    // capacity 4 -> 4/8 == 0, clamped up to 1.
    let (mut tx, mut rx) = RingBuffer::<u64>::new(4);
    for i in 0..8 {
        tx.push(i);
    }
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 8 - 4 + 1 }));
    assert_eq!(rx.pop(), Ok(5));
}

#[test]
fn capacity_one_is_a_latest_value_cell() {
    // Floor is 1 (no gating machinery to protect); default slack is 0.
    let (mut tx, mut rx) = RingBuffer::<u64>::new(1);
    assert_eq!(rx.capacity(), 1);
    for i in 0..3 {
        tx.push(i);
    }
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 2 }));
    assert_eq!(rx.pop(), Ok(2));
    assert_eq!(rx.try_pop(), Ok(None));
}

// -----------------------------------------------------------------------------
// 7. SKIP TO LATEST
// -----------------------------------------------------------------------------

#[test]
fn skip_to_latest_jumps_to_tail() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    assert_eq!(rx.skip_to_latest(), 0, "already at the tail");

    for i in 0..10 {
        tx.push(i);
    }
    assert_eq!(rx.skip_to_latest(), 10, "returns the skipped count");
    assert_eq!(rx.try_pop(), Ok(None), "positioned at the tail: empty");
    assert_eq!(rx.lag(), 0);

    tx.push(10);
    assert_eq!(rx.pop(), Ok(10), "next message after the skip point");
}

// -----------------------------------------------------------------------------
// 8. CLOSED CONTRACT
// -----------------------------------------------------------------------------

#[test]
fn closed_only_after_drain() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(8);
    for i in 0..5 {
        tx.push(i);
    }
    drop(tx);
    // Published slots stay readable after producer death (seqs are stable).
    for i in 0..5 {
        assert_eq!(rx.pop(), Ok(i));
    }
    assert_eq!(rx.pop(), Err(PopError::Closed));
    assert_eq!(rx.try_pop(), Err(PopError::Closed));
}

#[test]
fn lagged_consumer_drains_remainder_after_close() {
    let (mut tx, mut rx) = RingBuffer::<u64, YieldWait>::with_slack(8, 2);
    for i in 0..20 {
        tx.push(i);
    }
    drop(tx);
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 14 }));
    for i in 14..20 {
        assert_eq!(rx.pop(), Ok(i));
    }
    assert_eq!(rx.pop(), Err(PopError::Closed));
    assert_eq!(rx.try_pop(), Err(PopError::Closed));
}

fn parked_pop_wakes_on_close<C: SelfTimed + Send + 'static>() {
    let (tx, mut rx) = make::<C>(8);
    let waiter = std::thread::spawn(move || rx.pop());
    // Let the consumer park in the blocking pop (self-timed strategies need
    // no notify — the producer drop is flag-only).
    std::thread::sleep(Duration::from_millis(50));
    drop(tx);
    assert_eq!(waiter.join().unwrap(), Err(PopError::Closed));
}

#[test]
fn parked_pop_wakes_on_close_sleep() {
    parked_pop_wakes_on_close::<SleepWait<1_000>>();
}

#[test]
fn parked_pop_wakes_on_close_backoff() {
    parked_pop_wakes_on_close::<BackoffWait>();
}

#[test]
fn subscribe_after_close_pops_closed() {
    let (mut tx, rx) = RingBuffer::<u64>::new(8);
    tx.push(1);
    drop(tx);
    // Subscribing to a closed ring succeeds (join point = tail) and the new
    // consumer, born drained, pops Closed.
    let mut late = rx.subscribe();
    assert_eq!(late.pop(), Err(PopError::Closed));
    assert_eq!(late.try_pop(), Err(PopError::Closed));
}

// -----------------------------------------------------------------------------
// 9. ZERO CONSUMERS: FREE RUN
// -----------------------------------------------------------------------------

#[test]
fn zero_consumers_free_run() {
    // `T: NoUninit` implies `Copy`, so no element type can have a `Drop`
    // impl — free-running over unread values cannot leak by construction.
    let (mut tx, rx) = RingBuffer::<u64>::new(4);
    drop(rx);
    for i in 0..100 {
        tx.push(i); // never blocks, no consumer state to consult
    }
    assert_eq!(tx.tail(), 100);
    let mut late = tx.subscribe::<YieldWait>();
    assert_eq!(late.try_pop(), Ok(None));
    drop(tx);
    assert_eq!(late.pop(), Err(PopError::Closed));
}

// -----------------------------------------------------------------------------
// 10. CONSTRUCTION VALIDATION
// -----------------------------------------------------------------------------

#[test]
#[should_panic(expected = "zero-sized")]
fn zst_rejected() {
    let _ = RingBuffer::<[u64; 0]>::new(8);
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_rejected() {
    let _ = RingBuffer::<u64>::new(0);
}

// -----------------------------------------------------------------------------
// 11. CUSTOM NoUninit ELEMENT
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
struct Tick {
    price: u64,
    qty: u64,
}

// SAFETY: two `u64` fields under `repr(C)` — no padding bytes, no uninit
// niches; every byte of the value representation is initialized.
unsafe impl NoUninit for Tick {}

#[test]
fn custom_no_uninit_struct_round_trips() {
    let (mut tx, mut rx) = RingBuffer::<Tick>::new(8);
    tx.push(Tick { price: 101, qty: 7 });
    tx.push(Tick { price: 102, qty: 9 });
    assert_eq!(rx.pop(), Ok(Tick { price: 101, qty: 7 }));
    assert_eq!(rx.pop(), Ok(Tick { price: 102, qty: 9 }));
    assert_eq!(rx.try_pop(), Ok(None));
}
