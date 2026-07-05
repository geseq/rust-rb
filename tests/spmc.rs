//! SPMC gating ring tests: SPSC-shaped round trips across wait-strategy
//! combinations, plus the SPMC-specific machinery — broadcast delivery,
//! gating on the slowest consumer, dynamic membership (mid-stream subscribe,
//! detach, free-run), the closed contract, drop-on-overwrite accounting, and
//! panic injection at every user-code call point.

use rust_rb::spmc::{Closed, Consumer, Producer, RingBuffer, SubscribeError};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, YieldWait};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

fn make<P, C>(min_capacity: usize) -> (Producer<u64, P, C>, Consumer<u64, P, C>)
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    RingBuffer::<u64, P, C>::with_wait_strategies(min_capacity)
}

// -----------------------------------------------------------------------------
// 1. SINGLE-CONSUMER ROUND TRIP ACROSS WAIT STRATEGIES
// -----------------------------------------------------------------------------

fn round_trip<P, C>()
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    // Blocking, single-threaded.
    let (mut tx, mut rx) = make::<P, C>(64);
    for i in 0..64 {
        tx.push(i);
    }
    for i in 0..64 {
        assert_eq!(rx.pop(), Ok(i));
    }

    // Non-blocking: fill to capacity, overflow hands the value back.
    for i in 0..64 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(999), Err(999));
    for i in 0..64 {
        assert_eq!(rx.try_pop(), Ok(Some(i)));
    }
    assert_eq!(rx.try_pop(), Ok(None));

    // Threaded blocking round trip; producer drop closes the ring.
    let (mut tx, mut rx) = make::<P, C>(64);
    let producer = std::thread::spawn(move || {
        for i in 0..10_000u64 {
            tx.push(i);
        }
    });
    let mut expected = 0u64;
    while let Ok(v) = rx.pop() {
        assert_eq!(v, expected);
        expected += 1;
    }
    assert_eq!(expected, 10_000);
    producer.join().unwrap();
}

#[test]
fn strategy_matrix() {
    round_trip::<PauseWait, PauseWait>();
    round_trip::<YieldWait, YieldWait>();
    round_trip::<NoOpWait, YieldWait>();
    round_trip::<YieldWait, NoOpWait>();
    round_trip::<BackoffWait, BackoffWait>();
}

// -----------------------------------------------------------------------------
// 2. EVERY CONSUMER SEES EVERY MESSAGE
// -----------------------------------------------------------------------------

fn broadcast_all<P, C>(consumers: usize, messages: u64)
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    let (mut tx, rx0) = make::<P, C>(256);
    let mut handles = vec![rx0];
    for _ in 1..consumers {
        handles.push(tx.subscribe().unwrap());
    }
    assert_eq!(tx.consumer_count(), consumers);

    let mut joins = Vec::new();
    for mut rx in handles {
        joins.push(std::thread::spawn(move || {
            let mut expected = 0u64;
            let mut sum = 0u64;
            while let Ok(v) = rx.pop() {
                assert_eq!(v, expected, "messages must arrive in order, gap-free");
                expected += 1;
                sum = sum.wrapping_add(v);
            }
            assert_eq!(expected, messages, "every message must be seen");
            sum
        }));
    }

    let producer = std::thread::spawn(move || {
        for i in 0..messages {
            tx.push(i);
        }
        // tx drops here: closes the ring.
    });
    producer.join().unwrap();

    let want = messages * (messages - 1) / 2;
    for join in joins {
        assert_eq!(join.join().unwrap(), want);
    }
}

#[test]
fn two_consumers_see_every_message() {
    broadcast_all::<YieldWait, YieldWait>(2, 100_000);
}

#[test]
fn four_consumers_see_every_message() {
    broadcast_all::<PauseWait, PauseWait>(4, 100_000);
}

// -----------------------------------------------------------------------------
// 3. GATING ON THE SLOWEST CONSUMER
// -----------------------------------------------------------------------------

#[test]
fn slow_consumer_gates_producer() {
    let (mut tx, mut fast) = RingBuffer::<u64>::new(4);
    let mut slow = tx.subscribe().unwrap();

    for i in 0..4 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(4), Err(4), "full ring must gate");

    // The fast consumer draining everything does not open the gate: the
    // producer gates on the MINIMUM cursor.
    for i in 0..4 {
        assert_eq!(fast.pop(), Ok(i));
    }
    assert_eq!(tx.try_push(4), Err(4), "slow consumer still gates");
    assert!(tx.is_full());

    // One slot opens per slow-consumer advance.
    assert_eq!(slow.pop(), Ok(0));
    assert!(tx.try_push(4).is_ok());
    assert_eq!(tx.try_push(5), Err(5));
    assert_eq!(slow.pop(), Ok(1));
    assert!(tx.try_push(5).is_ok());

    // Blocking variant: a parked push is released by the slow consumer.
    let producer = std::thread::spawn(move || {
        tx.push(6); // blocks until `slow` frees a slot
        tx
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(slow.pop(), Ok(2));
    let _tx = producer.join().unwrap();
    for i in 3..7 {
        assert_eq!(slow.pop(), Ok(i));
    }
    for i in 4..7 {
        assert_eq!(fast.pop(), Ok(i));
    }
}

// -----------------------------------------------------------------------------
// 4. SUBSCRIBE MID-STREAM: JOIN POINT
// -----------------------------------------------------------------------------

#[test]
fn subscribe_mid_stream_sees_only_post_join() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    for i in 0..8 {
        tx.push(i);
    }

    let mut late = tx.subscribe().unwrap();
    assert_eq!(
        late.try_pop(),
        Ok(None),
        "nothing published pre-join is seen"
    );
    assert!(late.is_empty());

    for i in 8..12 {
        tx.push(i);
    }
    for i in 8..12 {
        assert_eq!(late.pop(), Ok(i), "everything post-join is seen");
    }
    assert_eq!(late.try_pop(), Ok(None));

    // The original consumer still sees the full stream.
    for i in 0..12 {
        assert_eq!(rx.pop(), Ok(i));
    }
}

#[test]
fn subscribe_mid_stream_threaded() {
    const MESSAGES: u64 = 50_000;
    let (mut tx, mut rx) = make::<PauseWait, PauseWait>(64);

    let producer = std::thread::spawn(move || {
        for i in 0..MESSAGES {
            tx.push(i);
        }
    });

    // Drain the first chunk on the original consumer, then join mid-stream.
    for i in 0..10_000 {
        assert_eq!(rx.pop(), Ok(i));
    }
    let mut late = rx.subscribe().unwrap();

    let late_thread = std::thread::spawn(move || {
        let mut first = None;
        let mut expected = 0u64;
        while let Ok(v) = late.pop() {
            if first.is_some() {
                assert_eq!(v, expected, "post-join suffix must be gap-free");
            } else {
                first = Some(v);
            }
            expected = v + 1;
        }
        let first = first.expect("joiner must see the tail of the stream");
        assert!(
            first >= 10_000,
            "join point is at or past the re-read cursor"
        );
        assert_eq!(expected, MESSAGES, "joiner must see everything post-join");
    });

    let mut expected = 10_000u64;
    while let Ok(v) = rx.pop() {
        assert_eq!(v, expected);
        expected += 1;
    }
    assert_eq!(expected, MESSAGES);

    producer.join().unwrap();
    late_thread.join().unwrap();
}

/// Regression for the [M-F2] registration order: the joiner must set its
/// bitmap bit *before* its `SeqCst` fence. The producer's rescan observes
/// consumers only through the bitmap, so a bit set after the fence lets a
/// scan miss the joiner while the joiner reads a stale write cursor — the
/// producer then laps a consumer it never saw and overwrites the element it
/// is reading (a data race Miri catches; same shape as the byte ring).
///
/// Chained handoff keeps exactly one freshly subscribed consumer live at a
/// time on a tiny ring, so every generation re-runs the subscribe-vs-rescan
/// window against a producer lapping at full speed.
#[test]
fn subscribe_churn_under_running_producer() {
    let messages: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    let (mut tx, rx) = make::<PauseWait, PauseWait>(64);

    let producer = std::thread::spawn(move || {
        for i in 0..messages {
            tx.push(i);
        }
    });

    let mut cur = rx;
    'churn: loop {
        // The producer may finish (and close the ring) at any point.
        let Ok(mut next) = cur.subscribe() else {
            break 'churn;
        };
        drop(cur);
        // A few pops per generation: the post-join suffix must be gap-free
        // even when the producer runs between the subscribe and the first
        // pop.
        let mut prev = None;
        for _ in 0..3 {
            match next.pop() {
                Ok(v) => {
                    if let Some(p) = prev {
                        assert_eq!(v, p + 1, "post-join suffix must be gap-free");
                    }
                    prev = Some(v);
                }
                Err(Closed) => break 'churn,
            }
        }
        cur = next;
    }
    producer.join().unwrap();
}

// -----------------------------------------------------------------------------
// 5. DETACH: DROPPED CONSUMERS STOP GATING; FREE-RUN WITH ZERO CONSUMERS
// -----------------------------------------------------------------------------

#[test]
fn dropped_consumer_stops_gating() {
    const MESSAGES: u64 = 100_000;
    let (mut tx, mut keeper) = make::<YieldWait, YieldWait>(64);
    let mut quitter = tx.subscribe().unwrap();

    let producer = std::thread::spawn(move || {
        for i in 0..MESSAGES {
            tx.push(i);
        }
    });

    let quitter_thread = std::thread::spawn(move || {
        // Consume a prefix, then detach mid-stream. If the detach failed to
        // un-gate the producer, the whole test would deadlock.
        for i in 0..1_000 {
            assert_eq!(quitter.pop(), Ok(i));
        }
        drop(quitter);
    });

    let mut expected = 0u64;
    while let Ok(v) = keeper.pop() {
        assert_eq!(
            v, expected,
            "remaining consumer is unaffected by the detach"
        );
        expected += 1;
    }
    assert_eq!(expected, MESSAGES);

    producer.join().unwrap();
    quitter_thread.join().unwrap();
}

/// Counts drops per identity so double drops and leaks are both visible.
struct Counted {
    id: usize,
    counts: Arc<Vec<AtomicUsize>>,
}

impl Counted {
    fn new(id: usize, counts: &Arc<Vec<AtomicUsize>>) -> Self {
        Self {
            id,
            counts: Arc::clone(counts),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.counts[self.id].fetch_add(1, Ordering::Relaxed);
    }
}

fn drop_counts(n: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect())
}

fn total(counts: &Arc<Vec<AtomicUsize>>) -> usize {
    counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
}

fn assert_each_dropped_once(counts: &Arc<Vec<AtomicUsize>>) {
    for (id, count) in counts.iter().enumerate() {
        assert_eq!(
            count.load(Ordering::Relaxed),
            1,
            "value {id} must be dropped exactly once (no leak, no double drop)"
        );
    }
}

#[test]
fn free_run_with_zero_consumers_drops_overwritten() {
    const N: usize = 1_000; // many laps of an 8-slot ring
    let counts = drop_counts(N);
    {
        let (mut tx, rx) = RingBuffer::<Counted>::new(8);
        drop(rx); // zero consumers

        for id in 0..N {
            assert!(
                tx.try_push(Counted::new(id, &counts)).is_ok(),
                "an audience-less producer must free-run, never gate"
            );
        }
        // Every overwrite dropped the old occupant; the last lap is live.
        assert_eq!(total(&counts), N - 8);
        drop(tx);
    }
    // Teardown drops the final lap — exact accounting, no double drop.
    assert_each_dropped_once(&counts);
}

#[test]
fn joiner_after_free_run_gates_and_reads_fresh_values() {
    // Catches the M-F1 regression: after an audience-less free-run the
    // producer must still notice a joiner within one lap and gate on it.
    let (mut tx, rx) = RingBuffer::<u64>::new(4);
    drop(rx);
    for i in 0..100 {
        assert!(tx.try_push(i).is_ok());
    }

    let mut late = tx.subscribe().unwrap();
    for i in 100..104 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(104), Err(104), "the joiner gates the producer");

    for i in 100..104 {
        assert_eq!(
            late.pop(),
            Ok(i),
            "the joiner sees exactly the post-join values"
        );
    }
    assert!(tx.try_push(104).is_ok());
    assert_eq!(late.pop(), Ok(104));
}

// -----------------------------------------------------------------------------
// 6. CLOSED CONTRACT
// -----------------------------------------------------------------------------

#[test]
fn closed_after_drain() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(8);
    tx.push(1);
    tx.push(2);
    tx.push(3);
    drop(tx);

    // Everything published before the close is still delivered.
    assert_eq!(rx.pop(), Ok(1));
    assert_eq!(rx.try_pop(), Ok(Some(2)));
    {
        let msg = rx
            .pop_ref()
            .expect("published message outlives the producer");
        assert_eq!(*msg, 3);
    }

    // Drained: every entry point reports Closed.
    assert_eq!(rx.try_pop(), Err(Closed));
    assert_eq!(rx.pop(), Err(Closed));
    assert_eq!(rx.pop_ref().err(), Some(Closed));
    assert!(matches!(rx.try_pop_ref(), Err(Closed)));

    // A new subscription on a closed ring is refused.
    assert_eq!(rx.subscribe().err(), Some(SubscribeError::Closed));
}

fn blocking_pop_returns_on_close<P, C>()
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    let (tx, mut rx) = make::<P, C>(8);
    let consumer = std::thread::spawn(move || rx.pop());
    std::thread::sleep(std::time::Duration::from_millis(50));
    drop(tx);
    assert_eq!(
        consumer.join().unwrap(),
        Err(Closed),
        "a parked pop must return, not hang, when the producer drops"
    );
}

#[test]
fn blocking_pop_returns_when_producer_drops() {
    blocking_pop_returns_on_close::<YieldWait, YieldWait>();
    blocking_pop_returns_on_close::<PauseWait, PauseWait>();
    blocking_pop_returns_on_close::<YieldWait, BackoffWait>();
}

// -----------------------------------------------------------------------------
// 7. PANIC INJECTION
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct ArmedClone {
    value: u64,
    arm: Arc<AtomicBool>,
}

impl ArmedClone {
    fn clone_or_boom(&self) -> Self {
        if self.arm.swap(false, Ordering::Relaxed) {
            panic!("clone boom");
        }
        Self {
            value: self.value,
            arm: Arc::clone(&self.arm),
        }
    }
}

#[test]
fn panicking_clone_leaves_element_consumable() {
    // Wrap the panicky clone in a type whose Clone impl delegates to it.
    struct PanicClone(ArmedClone);
    impl Clone for PanicClone {
        fn clone(&self) -> Self {
            PanicClone(self.0.clone_or_boom())
        }
    }

    let arm = Arc::new(AtomicBool::new(true));
    let (mut tx, mut rx) = RingBuffer::<PanicClone>::new(4);
    tx.push(PanicClone(ArmedClone {
        value: 7,
        arm: Arc::clone(&arm),
    }));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = rx.pop();
    }));
    assert!(result.is_err(), "the first clone panics");

    // Clone-then-advance: the cursor never moved, so the SAME element is
    // redelivered — and the disarmed clone now succeeds.
    let redelivered = rx.pop().expect("element must still be consumable");
    assert_eq!(redelivered.0.value, 7);

    tx.push(PanicClone(ArmedClone {
        value: 8,
        arm: Arc::clone(&arm),
    }));
    assert_eq!(rx.pop().unwrap().0.value, 8, "ring remains usable");
}

struct PanicDrop {
    id: usize,
    armed: bool,
    counts: Arc<Vec<AtomicUsize>>,
}

impl Drop for PanicDrop {
    fn drop(&mut self) {
        self.counts[self.id].fetch_add(1, Ordering::Relaxed);
        if self.armed && !std::thread::panicking() {
            panic!("drop boom");
        }
    }
}

#[test]
fn panicking_drop_during_overwrite_no_double_drop() {
    let counts = drop_counts(7);
    let item = |id: usize, armed: bool| PanicDrop {
        id,
        armed,
        counts: Arc::clone(&counts),
    };
    {
        let (mut tx, rx) = RingBuffer::<PanicDrop>::new(4);
        drop(rx); // free-run so the overwrite path is deterministic

        tx.push(item(0, true)); // will panic when overwrite-dropped
        for id in 1..4 {
            tx.push(item(id, false));
        }
        assert_eq!(total(&counts), 0);

        // The 5th push overwrites value 0, whose drop panics. The push
        // unwinds; its argument (value 4) is dropped by the unwind.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tx.push(item(4, false));
        }));
        assert!(result.is_err(), "the overwrite drop panics");
        assert_eq!(counts[0].load(Ordering::Relaxed), 1, "value 0 dropped once");
        assert_eq!(
            counts[4].load(Ordering::Relaxed),
            1,
            "unwound argument dropped"
        );

        // The ring remains usable: the watermark already excludes value 0,
        // so the retried slot is never re-dropped.
        tx.push(item(5, false)); // reuses value 0's slot, no drop runs
        assert_eq!(counts[0].load(Ordering::Relaxed), 1);
        tx.push(item(6, false)); // overwrites value 1
        assert_eq!(counts[1].load(Ordering::Relaxed), 1);

        drop(tx);
    }
    // Teardown releases values 2, 3, 5, 6 — everything exactly once.
    assert_each_dropped_once(&counts);
}

#[test]
fn abandoned_claim_is_not_redropped() {
    let counts = drop_counts(9);
    {
        let (mut tx, rx) = RingBuffer::<Counted>::new(8);
        drop(rx);
        for id in 0..8 {
            tx.push(Counted::new(id, &counts));
        }
        assert_eq!(total(&counts), 0);

        // Claiming the 9th slot drops value 0 at claim time; abandoning the
        // claim publishes nothing.
        {
            let _abandoned = tx.try_claim().unwrap();
        }
        assert_eq!(counts[0].load(Ordering::Relaxed), 1);

        // Re-claiming the same sequence must NOT drop the (now dead) slot
        // again; committing publishes it.
        {
            let mut slot = tx.try_claim().unwrap();
            slot.uninit().write(Counted::new(8, &counts));
            // SAFETY: the slot was fully initialized above.
            unsafe { slot.commit_init() };
        }
        assert_eq!(counts[0].load(Ordering::Relaxed), 1, "no double drop");
        drop(tx);
    }
    assert_each_dropped_once(&counts);
}

// -----------------------------------------------------------------------------
// 8. POP_REF: CONCURRENT SHARED READS, FORGET = REDELIVERY
// -----------------------------------------------------------------------------

#[test]
fn pop_ref_concurrent_reads_of_same_slot() {
    let (mut tx, mut rx1) = RingBuffer::<u64>::new(4);
    let mut rx2 = tx.subscribe().unwrap();
    tx.push(42);

    let barrier = Barrier::new(2);
    std::thread::scope(|s| {
        s.spawn(|| {
            let msg = rx1.pop_ref().unwrap();
            barrier.wait(); // both guards live simultaneously
            assert_eq!(*msg, 42);
            barrier.wait();
        });
        s.spawn(|| {
            let msg = rx2.pop_ref().unwrap();
            barrier.wait();
            assert_eq!(*msg, 42);
            barrier.wait();
        });
    });

    // Both guards dropped: the ring proceeds.
    tx.push(7);
    assert_eq!(rx1.pop(), Ok(7));
    assert_eq!(rx2.pop(), Ok(7));
}

#[test]
fn forgotten_pop_ref_redelivers() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(4);
    tx.push(7);

    let msg = rx.pop_ref().unwrap();
    std::mem::forget(msg);

    // The cursor never advanced: the same element is delivered again.
    assert_eq!(rx.pop(), Ok(7));
    assert_eq!(rx.try_pop(), Ok(None));
}

// -----------------------------------------------------------------------------
// 9. DROP ACCOUNTING OVER MANY LAPS
// -----------------------------------------------------------------------------

#[test]
fn total_drops_equal_total_pushes_over_laps() {
    const N: usize = 100; // 12.5 laps of an 8-slot ring
    let counts = drop_counts(N);
    {
        let (mut tx, mut rx) = RingBuffer::<Counted>::new(8);
        for id in 0..N {
            tx.push(Counted::new(id, &counts));
            // pop_ref advances without dropping: the producer's overwrite
            // path (and teardown) own every drop.
            drop(rx.pop_ref().unwrap());
        }
        assert_eq!(
            total(&counts),
            N - 8,
            "one overwrite drop per push past lap one"
        );
        drop(rx);
        drop(tx);
    }
    assert_each_dropped_once(&counts);
}

// -----------------------------------------------------------------------------
// 10. LEN / IS_EMPTY / CAPACITY VIEWS
// -----------------------------------------------------------------------------

#[test]
fn len_views_and_caveats() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(8);
    assert_eq!(tx.capacity(), 8);
    assert_eq!(rx.capacity(), 8);
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    assert_eq!(tx.len(), 0);
    assert_eq!(rx.len(), 0);
    assert!(!tx.is_full());

    tx.push(1);
    tx.push(2);
    // The consumer view is exact.
    assert_eq!(rx.len(), 2);
    assert!(!rx.is_empty());
    // The producer view is fresh here (its cache was exact at the last scan).
    assert_eq!(tx.len(), 2);

    assert_eq!(rx.pop(), Ok(1));
    assert_eq!(
        rx.len(),
        1,
        "consumer view tracks the private cursor exactly"
    );
    // The producer's cached view may lag (over-count), never under-count.
    assert!(tx.len() >= rx.len());

    for i in 0..7 {
        tx.push(10 + i);
    }
    assert!(tx.is_full());
    assert_eq!(rx.len(), 8);
}

#[test]
fn capacity_floor_is_two() {
    // SPMC rounds capacity to a power of two with a floor of 2: the
    // audience-less gating default (own cursor minus one) could never open
    // a capacity-1 ring's gate.
    let (tx, _rx) = RingBuffer::<u64>::new(1);
    assert_eq!(tx.capacity(), 2);
    let (tx, _rx) = RingBuffer::<u64>::new(3);
    assert_eq!(tx.capacity(), 4);
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_panics() {
    let _ = RingBuffer::<u64>::new(0);
}

// -----------------------------------------------------------------------------
// 11. DRAIN
// -----------------------------------------------------------------------------

#[test]
fn drain_consumes_one_batch() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(64); // publish batch = 8
    for i in 0..10 {
        tx.push(i);
    }

    let mut seen = Vec::new();
    let n = rx.drain(|v| seen.push(*v));
    assert_eq!(n, 8, "drain caps at one publish batch");
    assert_eq!(seen, (0..8).collect::<Vec<_>>());

    let n = rx.drain(|v| seen.push(*v));
    assert_eq!(n, 2);
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    assert_eq!(rx.drain(|_| unreachable!()), 0);

    // Progress was published: the producer can fill the whole ring again.
    for i in 0..64 {
        assert!(tx.try_push(i).is_ok());
    }
}

#[test]
fn drain_publishes_on_unwind() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16); // publish batch = 2
    for i in 0..4 {
        tx.push(i);
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rx.drain(|v| {
            if *v == 1 {
                panic!("callback boom");
            }
        });
    }));
    assert!(result.is_err());

    // Elements 0 and 1 were consumed (cursor advanced before the callback);
    // the unwound drain never re-delivers them.
    let mut seen = Vec::new();
    while rx.drain(|v| seen.push(*v)) != 0 {}
    assert_eq!(seen, vec![2, 3]);
}

// -----------------------------------------------------------------------------
// 12. REGISTRY GROWTH PAST ONE CHUNK
// -----------------------------------------------------------------------------

#[test]
fn registry_grows_past_64_consumers() {
    const CONSUMERS: usize = 70;
    let (mut tx, rx0) = RingBuffer::<u64>::new(8);
    let mut consumers = vec![rx0];
    for _ in 1..CONSUMERS {
        consumers.push(tx.subscribe().unwrap());
    }
    assert_eq!(tx.consumer_count(), CONSUMERS);

    for i in 0..8 {
        tx.push(i);
    }
    assert_eq!(
        tx.try_push(8),
        Err(8),
        "all 70 consumers gate, chunk two included"
    );

    // A second-chunk consumer alone holding the gate closed.
    let laggard = consumers.pop().unwrap(); // slot 69: in the appended chunk
    for rx in consumers.iter_mut() {
        for i in 0..8 {
            assert_eq!(rx.pop(), Ok(i));
        }
    }
    assert_eq!(
        tx.try_push(8),
        Err(8),
        "the second-chunk laggard still gates"
    );
    drop(laggard);
    assert!(tx.try_push(8).is_ok(), "detach in chunk two opens the gate");
    assert_eq!(tx.consumer_count(), CONSUMERS - 1);

    for rx in consumers.iter_mut() {
        assert_eq!(rx.pop(), Ok(8));
    }
}

// -----------------------------------------------------------------------------
// 13. ZERO-COPY WRITE PATH (claim / commit)
// -----------------------------------------------------------------------------

#[test]
fn claim_commit_round_trip() {
    let (mut tx, mut rx) = RingBuffer::<[u64; 4]>::new(8);

    let mut slot = tx.claim();
    slot.uninit().write([1, 2, 3, 4]);
    // SAFETY: the slot was fully initialized above.
    unsafe { slot.commit_init() };
    assert_eq!(rx.pop(), Ok([1, 2, 3, 4]));

    // An abandoned claim publishes nothing.
    {
        let _abandoned = tx.try_claim().unwrap();
    }
    assert_eq!(rx.try_pop(), Ok(None));

    // commit() moves a value into the reserved slot.
    tx.claim().commit([5, 6, 7, 8]);
    assert_eq!(rx.pop(), Ok([5, 6, 7, 8]));

    // try_claim reports a gated ring like try_push.
    for i in 0..8 {
        tx.claim().commit([i; 4]);
    }
    assert!(tx.try_claim().is_none());
    assert_eq!(rx.pop(), Ok([0; 4]));
    tx.try_claim().unwrap().commit([9; 4]);
}
