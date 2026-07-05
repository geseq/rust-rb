//! SPMC gating byte-ring tests: SPSC-shaped round trips across wait-strategy
//! combinations, plus the SPMC-specific machinery — broadcast delivery of
//! variable-size frames, gating on the slowest consumer, dynamic membership
//! (mid-stream subscribe at a record boundary, detach, free-run), the closed
//! contract, the lag-filtered starving release, and forget-redelivery.

use rust_rb::spmc_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer, Closed, SubscribeError};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, YieldWait};

fn make<P, C>(min_capacity: usize) -> (BytesProducer<P, C>, BytesConsumer<P, C>)
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    BytesRingBuffer::<P, C>::with_wait_strategies(min_capacity)
}

/// Deterministic payload for message `seq`: `len` bytes of `(seq + i) as u8`.
fn payload(seq: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seq + i) as u8).collect()
}

/// Sequence-stamped payload of varying length: the first 8 bytes carry `seq`
/// little-endian, the rest is deterministic filler. Total length sweeps
/// `8..=max` so wrap padding lands at many different offsets.
fn stamped(seq: u64, max: usize) -> Vec<u8> {
    let len = 8 + (seq as usize * 31 + 7) % (max - 7);
    let mut v = vec![0u8; len];
    v[..8].copy_from_slice(&seq.to_le_bytes());
    for (i, b) in v[8..].iter_mut().enumerate() {
        *b = (seq as usize).wrapping_add(i) as u8;
    }
    v
}

/// Read the stamp back and check the whole message against [`stamped`].
fn check_stamped(msg: &[u8], max: usize) -> u64 {
    let seq = u64::from_le_bytes(msg[..8].try_into().unwrap());
    assert_eq!(msg, stamped(seq, max).as_slice(), "message {seq} corrupted");
    seq
}

// -----------------------------------------------------------------------------
// 1. SINGLE-CONSUMER ROUND TRIP ACROSS WAIT STRATEGIES
// -----------------------------------------------------------------------------

fn round_trip<P, C>()
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    // Blocking, single-threaded, sizes sweeping empty to max.
    let (mut tx, mut rx) = make::<P, C>(256);
    let max = tx.max_message_len();
    let sizes = [0usize, 1, 3, 4, 5, 31, 64, max];
    for (seq, &len) in sizes.iter().enumerate() {
        tx.push(&payload(seq, len));
        assert_eq!(&*rx.pop().unwrap(), payload(seq, len).as_slice());
    }
    assert!(rx.is_empty());

    // Non-blocking: fill to capacity, overflow reports the gate. 12-byte
    // payloads make 16-byte records; exactly 4 fill the 64-byte ring.
    let (mut tx, mut rx) = make::<P, C>(64);
    for seq in 0..4 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(!tx.try_push(&payload(4, 12)));
    assert!(tx.try_claim(12).is_none());
    for seq in 0..4 {
        assert_eq!(
            &*rx.try_pop().unwrap().unwrap(),
            payload(seq, 12).as_slice()
        );
    }
    assert!(rx.try_pop().unwrap().is_none());

    // Threaded blocking round trip with mixed sizes; producer drop closes.
    let messages: usize = if cfg!(miri) { 500 } else { 20_000 };
    let (mut tx, mut rx) = make::<P, C>(256);
    let max = tx.max_message_len();
    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            let len = (seq * 31 + 7) % (max + 1);
            tx.push(&payload(seq, len));
        }
    });
    let mut seq = 0usize;
    while let Ok(msg) = rx.pop() {
        let len = (seq * 31 + 7) % (max + 1);
        assert_eq!(&*msg, payload(seq, len).as_slice(), "message {seq}");
        seq += 1;
    }
    assert_eq!(seq, messages, "closed only after everything was delivered");
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

/// Force the wrap-padding path on every lap for two consumers at once:
/// 20-byte payloads make 24-byte records, and 24 does not divide 64.
#[test]
fn wrap_padding_every_lap_two_consumers() {
    let iters = if cfg!(miri) { 500 } else { 10_000 };
    let (mut tx, mut a) = BytesRingBuffer::new(64);
    let mut b = tx.subscribe().unwrap();
    for seq in 0..iters {
        tx.push(&payload(seq, 20));
        assert_eq!(&*a.pop().unwrap(), payload(seq, 20).as_slice());
        assert_eq!(&*b.pop().unwrap(), payload(seq, 20).as_slice());
    }
}

#[test]
fn max_message_len_boundary_round_trips() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    let max = tx.max_message_len();
    assert_eq!(max, 124); // capacity / 2 - 4
    assert_eq!(rx.max_message_len(), 124);
    // Repeat so the max-size record also exercises the padding path.
    let iters = if cfg!(miri) { 20 } else { 100 };
    for seq in 0..iters {
        tx.push(&payload(seq, max));
        assert_eq!(&*rx.pop().unwrap(), payload(seq, max).as_slice());
    }
}

#[test]
#[should_panic(expected = "exceeds max_message_len")]
fn oversized_message_panics() {
    let (mut tx, _rx) = BytesRingBuffer::new(256);
    let too_big = vec![0u8; tx.max_message_len() + 1];
    tx.push(&too_big);
}

#[test]
fn capacity_rounds_up_with_byte_floor() {
    let (tx, rx) = BytesRingBuffer::new(100);
    assert_eq!(tx.capacity(), 128);
    assert_eq!(rx.capacity(), 128);

    // Minimum ring: capacity floor of 8 bytes means max_message_len is 0 —
    // only empty messages fit, and they round-trip through both consumers.
    let (mut tx, mut rx) = BytesRingBuffer::new(1);
    let mut rx2 = tx.subscribe().unwrap();
    assert_eq!(tx.capacity(), 8);
    assert_eq!(tx.max_message_len(), 0);
    for _ in 0..32 {
        tx.push(b"");
        assert_eq!(&*rx.pop().unwrap(), b"");
        assert_eq!(&*rx2.pop().unwrap(), b"");
    }
    assert!(rx.try_pop().unwrap().is_none());
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_panics() {
    let _ = BytesRingBuffer::new(0);
}

// -----------------------------------------------------------------------------
// 2. EVERY CONSUMER SEES EVERY MESSAGE, CONTENT-EXACT
// -----------------------------------------------------------------------------

fn broadcast_all<P, C>(consumers: usize, messages: u64)
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    let (mut tx, rx0) = make::<P, C>(1024);
    let max = tx.max_message_len();
    let mut handles = vec![rx0];
    for _ in 1..consumers {
        handles.push(tx.subscribe().unwrap());
    }
    assert_eq!(tx.consumer_count(), consumers);

    let mut joins = Vec::new();
    for mut rx in handles {
        joins.push(std::thread::spawn(move || {
            let mut expected = 0u64;
            while let Ok(msg) = rx.pop() {
                let seq = check_stamped(&msg, max);
                assert_eq!(seq, expected, "messages must arrive in order, gap-free");
                expected += 1;
            }
            assert_eq!(expected, messages, "every message must be seen");
        }));
    }

    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            tx.push(&stamped(seq, max));
        }
        // tx drops here: closes the ring.
    });
    producer.join().unwrap();

    for join in joins {
        join.join().unwrap();
    }
}

#[test]
fn two_consumers_see_every_message() {
    let messages = if cfg!(miri) { 1_000 } else { 100_000 };
    broadcast_all::<YieldWait, YieldWait>(2, messages);
}

#[test]
fn four_consumers_see_every_message() {
    let messages = if cfg!(miri) { 500 } else { 100_000 };
    broadcast_all::<PauseWait, PauseWait>(4, messages);
}

// -----------------------------------------------------------------------------
// 3. GATING ON THE SLOWEST CONSUMER
// -----------------------------------------------------------------------------

#[test]
fn slow_consumer_gates_producer() {
    // 12-byte payloads -> 16-byte records; 4 fill the 64-byte ring exactly.
    let (mut tx, mut fast) = BytesRingBuffer::new(64);
    let mut slow = tx.subscribe().unwrap();

    for seq in 0..4 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(!tx.try_push(&payload(4, 12)), "full ring must gate");

    // The fast consumer draining everything does not open the gate: the
    // producer gates on the MINIMUM cursor.
    for seq in 0..4 {
        assert_eq!(&*fast.pop().unwrap(), payload(seq, 12).as_slice());
    }
    assert!(!tx.try_push(&payload(4, 12)), "slow consumer still gates");

    // One record's worth of space opens per slow-consumer pop.
    assert_eq!(&*slow.pop().unwrap(), payload(0, 12).as_slice());
    assert!(tx.try_push(&payload(4, 12)));
    assert!(!tx.try_push(&payload(5, 12)));
    assert_eq!(&*slow.pop().unwrap(), payload(1, 12).as_slice());
    assert!(tx.try_push(&payload(5, 12)));

    // Blocking variant: a parked push is released by the slow consumer.
    let producer = std::thread::spawn(move || {
        tx.push(&payload(6, 12)); // blocks until `slow` frees a record
        tx
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(&*slow.pop().unwrap(), payload(2, 12).as_slice());
    let _tx = producer.join().unwrap();
    for seq in 3..7 {
        assert_eq!(&*slow.pop().unwrap(), payload(seq, 12).as_slice());
    }
    for seq in 4..7 {
        assert_eq!(&*fast.pop().unwrap(), payload(seq, 12).as_slice());
    }
}

// -----------------------------------------------------------------------------
// 4. SUBSCRIBE MID-STREAM: JOIN POINT IS A RECORD BOUNDARY
// -----------------------------------------------------------------------------

#[test]
fn subscribe_mid_stream_sees_only_post_join() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    for seq in 0..8 {
        tx.push(&payload(seq, 12));
    }

    let mut late = tx.subscribe().unwrap();
    assert!(
        late.try_pop().unwrap().is_none(),
        "nothing published pre-join is seen"
    );
    assert!(late.is_empty());

    for seq in 8..12 {
        tx.push(&payload(seq, 12));
    }
    for seq in 8..12 {
        // Content-exact: the joiner started at a record boundary and parses
        // valid frames from its first message on.
        assert_eq!(
            &*late.pop().unwrap(),
            payload(seq, 12).as_slice(),
            "everything post-join is seen"
        );
    }
    assert!(late.try_pop().unwrap().is_none());

    // The original consumer still sees the full stream.
    for seq in 0..12 {
        assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
    }
}

#[test]
fn subscribe_mid_stream_threaded() {
    let messages: u64 = if cfg!(miri) { 2_000 } else { 50_000 };
    let prefix: u64 = messages / 5;
    let (mut tx, mut rx) = make::<PauseWait, PauseWait>(256);
    let max = tx.max_message_len();

    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            tx.push(&stamped(seq, max));
        }
    });

    // Drain the first chunk on the original consumer, then join mid-stream.
    for seq in 0..prefix {
        assert_eq!(check_stamped(&rx.pop().unwrap(), max), seq);
    }
    let mut late = rx.subscribe().unwrap();

    let late_thread = std::thread::spawn(move || {
        let mut first = None;
        let mut expected = 0u64;
        while let Ok(msg) = late.pop() {
            // `check_stamped` verifies the full frame: a joiner starting
            // mid-record would misparse and fail here.
            let seq = check_stamped(&msg, max);
            if first.is_some() {
                assert_eq!(seq, expected, "post-join suffix must be gap-free");
            } else {
                first = Some(seq);
            }
            expected = seq + 1;
        }
        let first = first.expect("joiner must see the tail of the stream");
        assert!(
            first >= prefix,
            "join point is at or past the re-read cursor"
        );
        assert_eq!(expected, messages, "joiner must see everything post-join");
    });

    let mut expected = prefix;
    while let Ok(msg) = rx.pop() {
        assert_eq!(check_stamped(&msg, max), expected);
        expected += 1;
    }
    assert_eq!(expected, messages);

    producer.join().unwrap();
    late_thread.join().unwrap();
}

/// Regression for the [M-F2] registration order: the joiner must set its
/// bitmap bit *before* its `SeqCst` fence. The producer's rescan observes
/// consumers only through the bitmap, so a bit set after the fence lets a
/// scan miss the joiner while the joiner reads a stale write cursor — the
/// producer then laps a consumer it never saw and overwrites the record it
/// is reading (a data race Miri catches at the header read).
///
/// Chained handoff keeps exactly one freshly subscribed consumer live at a
/// time on a tiny ring, so every generation re-runs the subscribe-vs-rescan
/// window against a producer lapping at full speed.
#[test]
fn subscribe_churn_under_running_producer() {
    let messages: u64 = if cfg!(miri) { 1_000 } else { 100_000 };
    let (mut tx, rx) = make::<PauseWait, PauseWait>(64);
    let max = tx.max_message_len();

    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            tx.push(&stamped(seq, max));
        }
    });

    let mut cur = rx;
    'churn: loop {
        // The producer may finish (and close the ring) at any point.
        let Ok(mut next) = cur.subscribe() else {
            break 'churn;
        };
        drop(cur);
        // A few pops per generation: every frame must parse cleanly even
        // when the producer runs between the subscribe and the first pop.
        for _ in 0..3 {
            match next.pop() {
                Ok(msg) => {
                    check_stamped(&msg, max);
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
    let messages: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    let quit_after: u64 = messages / 100;
    let (mut tx, mut keeper) = make::<YieldWait, YieldWait>(256);
    let max = tx.max_message_len();
    let mut quitter = tx.subscribe().unwrap();

    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            tx.push(&stamped(seq, max));
        }
    });

    let quitter_thread = std::thread::spawn(move || {
        // Consume a prefix, then detach mid-stream. If the detach failed to
        // un-gate the producer, the whole test would deadlock.
        for seq in 0..quit_after {
            assert_eq!(check_stamped(&quitter.pop().unwrap(), max), seq);
        }
        drop(quitter);
    });

    let mut expected = 0u64;
    while let Ok(msg) = keeper.pop() {
        assert_eq!(
            check_stamped(&msg, max),
            expected,
            "remaining consumer is unaffected by the detach"
        );
        expected += 1;
    }
    assert_eq!(expected, messages);

    producer.join().unwrap();
    quitter_thread.join().unwrap();
}

#[test]
fn free_run_with_zero_consumers_never_gates() {
    let (mut tx, rx) = BytesRingBuffer::new(64);
    drop(rx); // zero consumers
    let max = tx.max_message_len();
    let iters = if cfg!(miri) { 200 } else { 5_000 };
    for seq in 0..iters {
        let len = (seq * 31 + 7) % (max + 1); // padding at many offsets
        assert!(
            tx.try_push(&payload(seq, len)),
            "an audience-less producer must free-run, never gate"
        );
    }
    assert_eq!(tx.consumer_count(), 0);
}

#[test]
fn joiner_after_free_run_gates_and_reads_fresh_frames() {
    // Catches the M-F1 regression: after an audience-less free-run the
    // producer must still notice a joiner within one lap and gate on it.
    // 12-byte payloads -> 16-byte records; 4 fill the 64-byte ring.
    let (mut tx, rx) = BytesRingBuffer::new(64);
    drop(rx);
    for seq in 0..100 {
        assert!(tx.try_push(&payload(seq, 12)));
    }

    let mut late = tx.subscribe().unwrap();
    for seq in 100..104 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(
        !tx.try_push(&payload(104, 12)),
        "the joiner gates the producer"
    );

    for seq in 100..104 {
        assert_eq!(
            &*late.pop().unwrap(),
            payload(seq, 12).as_slice(),
            "the joiner sees exactly the post-join frames"
        );
    }
    assert!(tx.try_push(&payload(104, 12)));
    assert_eq!(&*late.pop().unwrap(), payload(104, 12).as_slice());
}

// -----------------------------------------------------------------------------
// 6. CLOSED CONTRACT
// -----------------------------------------------------------------------------

#[test]
fn closed_after_drain() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    tx.push(b"one");
    tx.push(b"two");
    tx.push(b"three");
    drop(tx);

    // Everything published before the close is still delivered.
    assert_eq!(&*rx.pop().unwrap(), b"one");
    assert_eq!(&*rx.try_pop().unwrap().unwrap(), b"two");
    let mut seen = Vec::new();
    assert_eq!(rx.drain(|m| seen.push(m.to_vec())), 1);
    assert_eq!(seen, vec![b"three".to_vec()]);

    // Drained: pop/try_pop report Closed; drain reports 0 (by contract it
    // never reports the close).
    assert!(matches!(rx.try_pop(), Err(Closed)));
    assert!(matches!(rx.pop(), Err(Closed)));
    assert_eq!(rx.drain(|_| unreachable!()), 0);

    // A new subscription on a closed ring is refused.
    assert_eq!(rx.subscribe().err(), Some(SubscribeError::Closed));
}

fn blocking_pop_returns_on_close<P, C>()
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    let (tx, mut rx) = make::<P, C>(64);
    let consumer = std::thread::spawn(move || matches!(rx.pop(), Err(Closed)));
    std::thread::sleep(std::time::Duration::from_millis(50));
    drop(tx);
    assert!(
        consumer.join().unwrap(),
        "a parked pop must return Closed, not hang, when the producer drops"
    );
}

#[test]
fn blocking_pop_returns_when_producer_drops() {
    blocking_pop_returns_on_close::<YieldWait, YieldWait>();
    blocking_pop_returns_on_close::<PauseWait, PauseWait>();
    blocking_pop_returns_on_close::<YieldWait, BackoffWait>();
}

// -----------------------------------------------------------------------------
// 7. STARVING RELEASE: THE GATE'S POP FREES A BLOCKED PRODUCER IMMEDIATELY
// -----------------------------------------------------------------------------

/// The gating consumer's single pop must release a producer blocked on wrap
/// padding, via the lag-filtered starving flush — the freed 24 bytes are far
/// below the 128-byte publish batch, so only the immediate trigger can wake
/// the producer here (port of the SPSC `pop_releases_producer_blocked_on_
/// padding` regressions to multi-consumer).
#[test]
fn gating_pop_releases_producer_blocked_on_padding() {
    let (mut tx, mut gate) = BytesRingBuffer::new(1024);
    let mut fast = tx.subscribe().unwrap();
    // 20-byte payload -> 24-byte record; 42 records = 1008 bytes, to_end = 16.
    let msg = [9u8; 20];
    for _ in 0..42 {
        assert!(tx.try_push(&msg));
    }
    // Next push needs pad(16) + record(24) = 40 > 16 free: blocked (and the
    // failed rescan raises the starving flag).
    assert!(!tx.try_push(&msg));

    // The fast consumer draining everything does not help: `gate` is the min.
    while let Ok(Some(m)) = fast.try_pop() {
        assert_eq!(&*m, &msg[..]);
    }
    assert!(!tx.try_push(&msg));

    // One pop frees 24 bytes (24 < the 128-byte batch, not caught up): only
    // the lag-filtered starving flush publishes it immediately.
    drop(gate.pop().unwrap());
    assert!(
        tx.try_push(&msg),
        "the gating consumer's first pop must release the blocked producer"
    );
}

/// A producer parked in a blocking `push` of a max-size message resumes when
/// the gating consumer pops one max-size message.
#[test]
fn blocked_producer_resumes_on_gating_max_size_pop() {
    let (mut tx, mut gate) = BytesRingBuffer::new(1024);
    let mut fast = tx.subscribe().unwrap();
    let max = tx.max_message_len(); // 508 -> 512-byte records
    tx.push(&payload(0, max));
    tx.push(&payload(1, max)); // two records fill the 1024-byte ring exactly

    // The fast consumer catches up fully; the gate does not move.
    drop(fast.pop().unwrap());
    drop(fast.pop().unwrap());
    assert!(!tx.try_push(&payload(2, max)));

    let producer = std::thread::spawn(move || {
        tx.push(&payload(2, max)); // parks until the gate frees a record
        tx
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert_eq!(&*gate.pop().unwrap(), payload(0, max).as_slice());
    let tx = producer.join().unwrap();

    for seq in 1..3 {
        assert_eq!(&*gate.pop().unwrap(), payload(seq, max).as_slice());
    }
    assert_eq!(&*fast.pop().unwrap(), payload(2, max).as_slice());
    drop(tx);
    assert!(matches!(gate.pop(), Err(Closed)));
    assert!(matches!(fast.pop(), Err(Closed)));
}

// -----------------------------------------------------------------------------
// 8. FORGET = REDELIVERY (AND GLOBAL GATING)
// -----------------------------------------------------------------------------

#[test]
fn forgotten_msg_redelivers_and_gates() {
    // 12-byte payloads -> 16-byte records; 4 fill the 64-byte ring.
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    for seq in 0..4 {
        tx.push(&payload(seq, 12));
    }

    let msg = rx.pop().unwrap();
    std::mem::forget(msg);

    // The cursor never advanced: the producer is still gated by this
    // consumer...
    assert!(!tx.try_push(&payload(4, 12)));
    // ...and the same message is delivered again.
    assert_eq!(&*rx.pop().unwrap(), payload(0, 12).as_slice());
    assert!(tx.try_push(&payload(4, 12)));
    for seq in 1..5 {
        assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
    }
    assert!(rx.try_pop().unwrap().is_none());
}

// -----------------------------------------------------------------------------
// 9. CONCURRENT STRESS: POP + DRAIN CONSUMERS AGAINST A CHURNING PRODUCER
// -----------------------------------------------------------------------------

#[test]
fn concurrent_pop_and_drain_consumers() {
    let messages: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    // A small ring so the producer blocks constantly and the starving
    // machinery churns under both consumer styles.
    let (mut tx, mut popper) = make::<PauseWait, PauseWait>(256);
    let max = tx.max_message_len();
    let mut drainer = tx.subscribe().unwrap();

    let pop_thread = std::thread::spawn(move || {
        let mut expected = 0u64;
        while let Ok(msg) = popper.pop() {
            assert_eq!(check_stamped(&msg, max), expected);
            expected += 1;
        }
        assert_eq!(expected, messages);
    });

    let drain_thread = std::thread::spawn(move || {
        let mut expected = 0u64;
        while expected < messages {
            drainer.drain(|msg| {
                assert_eq!(check_stamped(msg, max), expected);
                expected += 1;
            });
            std::hint::spin_loop();
        }
        assert_eq!(drainer.drain(|_| unreachable!()), 0);
    });

    let producer = std::thread::spawn(move || {
        for seq in 0..messages {
            tx.push(&stamped(seq, max));
        }
    });

    producer.join().unwrap();
    pop_thread.join().unwrap();
    drain_thread.join().unwrap();
}

// -----------------------------------------------------------------------------
// 10. DRAIN GRANULARITY
// -----------------------------------------------------------------------------

#[test]
fn drain_consumes_about_one_publish_batch() {
    // Capacity 1024 -> publish batch 128 bytes; 12-byte payloads -> 16-byte
    // records, so a drain stops after 8 records.
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    for seq in 0..20 {
        tx.push(&payload(seq, 12));
    }

    let mut seen = Vec::new();
    let n = rx.drain(|m| seen.push(m.to_vec()));
    assert_eq!(n, 8, "drain caps at one publish batch of bytes");
    let n = rx.drain(|m| seen.push(m.to_vec()));
    assert_eq!(n, 8);
    let n = rx.drain(|m| seen.push(m.to_vec()));
    assert_eq!(n, 4);
    for (seq, msg) in seen.iter().enumerate() {
        assert_eq!(msg.as_slice(), payload(seq, 12).as_slice());
    }
    assert_eq!(rx.drain(|_| unreachable!()), 0);

    // Progress was published: the producer can fill the whole ring again.
    for seq in 0..64 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
}

// -----------------------------------------------------------------------------
// 11. ZERO-COPY WRITE PATH (claim / commit)
// -----------------------------------------------------------------------------

#[test]
fn claim_commit_zero_copy() {
    let (mut tx, mut rx) = BytesRingBuffer::new(128);
    let mut rx2 = tx.subscribe().unwrap();

    let mut slot = tx.claim(8);
    slot.copy_from_slice(&[9u8; 8]);
    slot.commit();
    assert_eq!(&*rx.pop().unwrap(), &[9u8; 8]);
    assert_eq!(&*rx2.pop().unwrap(), &[9u8; 8]);

    // An abandoned claim publishes nothing and its space is reused.
    {
        let _abandoned = tx.try_claim(16).unwrap();
    }
    assert!(rx.try_pop().unwrap().is_none());
    assert!(rx2.try_pop().unwrap().is_none());

    let mut slot = tx.try_claim(16).unwrap();
    slot.copy_from_slice(&payload(7, 16));
    slot.commit();
    assert_eq!(&*rx.pop().unwrap(), payload(7, 16).as_slice());
    assert_eq!(&*rx2.pop().unwrap(), payload(7, 16).as_slice());
}

// -----------------------------------------------------------------------------
// 12. REGISTRY GROWTH PAST ONE CHUNK
// -----------------------------------------------------------------------------

#[test]
fn registry_grows_past_64_consumers() {
    const CONSUMERS: usize = 70;
    // 12-byte payloads -> 16-byte records; 4 fill the 64-byte ring.
    let (mut tx, rx0) = BytesRingBuffer::new(64);
    let mut consumers = vec![rx0];
    for _ in 1..CONSUMERS {
        consumers.push(tx.subscribe().unwrap());
    }
    assert_eq!(tx.consumer_count(), CONSUMERS);

    for seq in 0..4 {
        tx.push(&payload(seq, 12));
    }
    assert!(
        !tx.try_push(&payload(4, 12)),
        "all 70 consumers gate, chunk two included"
    );

    // A second-chunk consumer alone holding the gate closed.
    let laggard = consumers.pop().unwrap(); // slot 69: in the appended chunk
    for rx in consumers.iter_mut() {
        for seq in 0..4 {
            assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
        }
    }
    assert!(
        !tx.try_push(&payload(4, 12)),
        "the second-chunk laggard still gates"
    );
    drop(laggard);
    assert!(
        tx.try_push(&payload(4, 12)),
        "detach in chunk two opens the gate"
    );
    assert_eq!(tx.consumer_count(), CONSUMERS - 1);

    for rx in consumers.iter_mut() {
        assert_eq!(&*rx.pop().unwrap(), payload(4, 12).as_slice());
    }
}
