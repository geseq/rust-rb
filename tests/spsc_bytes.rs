//! Tests for the variable-size-message ring: framing, wrap padding, the
//! zero-copy claim/commit and drain paths, capacity edge cases, and
//! multi-threaded stress with heavily mixed message sizes.

use rust_rb::spsc_bytes::BytesRingBuffer;
use rust_rb::wait::{NoOpWait, PauseWait, YieldWait};

/// Deterministic payload for message `seq`: `len` bytes of `(seq + i) as u8`.
fn payload(seq: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seq + i) as u8).collect()
}

#[test]
fn round_trip_mixed_sizes() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    assert!(tx.is_empty());
    assert_eq!(tx.capacity(), 256);
    assert_eq!(tx.max_message_len(), 124);

    let sizes = [0, 1, 3, 4, 5, 31, 64, 124];
    for (seq, &len) in sizes.iter().enumerate() {
        tx.push(&payload(seq, len));
        let msg = rx.pop();
        assert_eq!(&*msg, payload(seq, len).as_slice());
    }
    assert!(rx.is_empty());
}

#[test]
fn capacity_rounds_up_and_has_floor() {
    let (tx, _rx) = BytesRingBuffer::new(100);
    assert_eq!(tx.capacity(), 128);
    let (tx, _rx) = BytesRingBuffer::new(1);
    assert_eq!(tx.capacity(), 8);
    assert_eq!(tx.max_message_len(), 0);
}

#[test]
fn zero_length_messages() {
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    for _ in 0..100 {
        tx.push(b"");
        assert_eq!(&*rx.pop(), b"");
    }
}

/// Force the wrap-padding path on every lap: 20-byte payloads make 24-byte
/// records, and 24 does not divide 64, so records regularly meet the end of
/// the buffer mid-record.
#[test]
fn wrap_padding_every_lap() {
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    for seq in 0..10_000 {
        tx.push(&payload(seq, 20));
        let msg = rx.pop();
        assert_eq!(&*msg, payload(seq, 20).as_slice());
    }
}

#[test]
fn try_push_full_then_recovers() {
    // 12-byte payloads make 16-byte records; exactly 4 fill the 64-byte ring
    // with no padding involved.
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    for seq in 0..4 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(!tx.try_push(&payload(4, 12)));
    assert!(tx.try_claim(12).is_none());

    assert_eq!(&*rx.pop(), payload(0, 12).as_slice());
    assert!(tx.try_push(&payload(4, 12)));

    for seq in 1..5 {
        assert_eq!(&*rx.pop(), payload(seq, 12).as_slice());
    }
    assert!(rx.try_pop().is_none());
}

#[test]
fn max_message_len_round_trips() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    let max = tx.max_message_len();
    // Repeat so the max-size record also exercises the padding path.
    for seq in 0..100 {
        tx.push(&payload(seq, max));
        assert_eq!(&*rx.pop(), payload(seq, max).as_slice());
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
fn claim_commit_zero_copy() {
    let (mut tx, mut rx) = BytesRingBuffer::new(128);

    let mut slot = tx.claim(8);
    slot.copy_from_slice(&[9u8; 8]);
    slot.commit();
    assert_eq!(&*rx.pop(), &[9u8; 8]);

    // An abandoned claim publishes nothing and its space is reused.
    {
        let _abandoned = tx.try_claim(16).unwrap();
    }
    assert!(rx.try_pop().is_none());

    let mut slot = tx.try_claim(16).unwrap();
    slot.copy_from_slice(&payload(7, 16));
    slot.commit();
    assert_eq!(&*rx.pop(), payload(7, 16).as_slice());
}

#[test]
fn drain_batches_everything() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    for seq in 0..10 {
        tx.push(&payload(seq, seq * 3));
    }

    let mut seen = Vec::new();
    let count = rx.drain(|msg| seen.push(msg.to_vec()));
    assert_eq!(count, 10);
    for (seq, msg) in seen.iter().enumerate() {
        assert_eq!(msg.as_slice(), payload(seq, seq * 3).as_slice());
    }

    assert_eq!(rx.drain(|_| panic!("ring should be empty")), 0);
    assert!(rx.is_empty() && tx.is_empty());
}

/// Blocking producer/consumer stress with message lengths sweeping the whole
/// legal range, so wrap padding lands at many different offsets.
fn threaded_stress<P, C>()
where
    P: rust_rb::wait::WaitStrategy + Send + Sync + 'static,
    C: rust_rb::wait::WaitStrategy + Send + Sync + 'static,
{
    const MESSAGES: usize = 200_000;
    let (mut tx, mut rx) = BytesRingBuffer::<P, C>::with_wait_strategies(256);
    let max = tx.max_message_len();

    let producer = std::thread::spawn(move || {
        for seq in 0..MESSAGES {
            let len = (seq * 31 + 7) % (max + 1);
            tx.push(&payload(seq, len));
        }
    });

    for seq in 0..MESSAGES {
        let len = (seq * 31 + 7) % (max + 1);
        let msg = rx.pop();
        assert_eq!(&*msg, payload(seq, len).as_slice(), "message {seq}");
    }
    assert!(rx.try_pop().is_none());
    producer.join().unwrap();
}

#[test]
fn threaded_stress_pause() {
    threaded_stress::<PauseWait, PauseWait>();
}

#[test]
fn threaded_stress_yield() {
    threaded_stress::<YieldWait, YieldWait>();
}

#[test]
fn threaded_stress_noop() {
    threaded_stress::<NoOpWait, NoOpWait>();
}

/// Non-blocking paths under contention: try_push/try_claim spin-loops feeding
/// a drain-based consumer, checking both content and message count.
#[test]
fn threaded_try_and_drain() {
    const MESSAGES: usize = 100_000;
    let (mut tx, mut rx) = BytesRingBuffer::<NoOpWait, NoOpWait>::with_wait_strategies(512);
    let max = tx.max_message_len();

    let producer = std::thread::spawn(move || {
        for seq in 0..MESSAGES {
            let len = (seq * 13 + 5) % (max + 1);
            let msg = payload(seq, len);
            if seq % 2 == 0 {
                while !tx.try_push(&msg) {
                    std::hint::spin_loop();
                }
            } else {
                loop {
                    if let Some(mut slot) = tx.try_claim(len) {
                        slot.copy_from_slice(&msg);
                        slot.commit();
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    });

    let mut seq = 0;
    while seq < MESSAGES {
        rx.drain(|msg| {
            let len = (seq * 13 + 5) % (max + 1);
            assert_eq!(msg, payload(seq, len).as_slice(), "message {seq}");
            seq += 1;
        });
        std::hint::spin_loop();
    }
    producer.join().unwrap();
}

// -----------------------------------------------------------------------------
// REVIEW REGRESSIONS: producer liveness over the full watermark, drain
// freshness, and capacity boundaries.
// -----------------------------------------------------------------------------

/// Pop from an exactly-full ring (no padding involved) must publish
/// immediately and release a blocked producer.
#[test]
fn pop_from_exact_full_publishes_immediately() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    // 60-byte payload -> 64-byte record; 16 records fill 1024 exactly.
    let msg = [7u8; 60];
    for _ in 0..16 {
        assert!(tx.try_push(&msg));
    }
    assert!(!tx.try_push(&msg));
    drop(rx.pop());
    assert!(tx.try_push(&msg), "pop from exact-full must publish");
}

/// A producer blocked below exact-full (contiguous space exhausted: the next
/// record needs wrap padding) must be released by the first pop — the
/// full-watermark rule (occupancy > capacity/2 publishes per message), not
/// the batch boundary. Review finding: this previously deferred until the
/// 128-byte batch, stalling the producer with ample space free.
#[test]
fn pop_releases_producer_blocked_on_padding() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    // 20-byte payload -> 24-byte record; 42 records = 1008 bytes, to_end = 16.
    let msg = [9u8; 20];
    for _ in 0..42 {
        assert!(tx.try_push(&msg));
    }
    // Next push needs pad(16) + record(24) = 40 > 16 free: blocked.
    assert!(!tx.try_push(&msg));

    // First pop frees 24 bytes -> free = 40 = exactly what is needed, and
    // occupancy (1008) is over the watermark (512), so it publishes at once.
    drop(rx.pop());
    assert!(
        tx.try_push(&msg),
        "first pop must release the blocked producer"
    );
}

/// Same guarantee when the frame at the read cursor *starts with wrap
/// padding*: the pad skip happens in the frame decoder before the release
/// accounting, which previously made the immediate flush miss. Review
/// finding with verified repro.
#[test]
fn pop_of_padded_frame_from_exact_full_publishes() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    // 36-byte payload -> 40-byte record. 25 records = 1000 bytes, to_end = 24.
    let msg = [1u8; 36];
    for _ in 0..25 {
        assert!(tx.try_push(&msg));
    }
    assert!(!tx.try_push(&msg)); // needs pad(24) + 40 = 64 > 24 free
    while rx.drain(|_| {}) > 0 {}

    // Cursors sit at 1000; the next record lands behind a 24-byte pad.
    // 1 padded record (64) + 24 plain records (960) = exactly 1024 occupied.
    for _ in 0..25 {
        assert!(tx.try_push(&msg));
    }
    assert!(!tx.try_push(&msg));

    // The frame at the read cursor is pad(24) + record(40): popping it frees
    // 64 bytes >= the 40 needed, and must publish immediately.
    drop(rx.pop());
    assert!(
        tx.try_push(&msg),
        "pop of a padded frame from exact-full must publish"
    );
}

/// drain() must consume everything published at call time, even when the
/// consumer's cached cursor view is stale-but-non-empty. Review finding:
/// deriving the bound from the cached view silently stopped early.
#[test]
fn drain_sees_messages_published_after_cache_went_stale() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    tx.push(&payload(0, 8));
    tx.push(&payload(1, 8));
    // This pop caches the producer cursor (2 records) and consumes one:
    // the cache is now stale-but-non-empty.
    assert_eq!(&*rx.pop(), payload(0, 8).as_slice());

    tx.push(&payload(2, 8));
    tx.push(&payload(3, 8));

    let mut seen = Vec::new();
    let count = rx.drain(|m| seen.push(m.to_vec()));
    assert_eq!(count, 3, "drain must refresh and consume everything");
    for (i, m) in seen.iter().enumerate() {
        assert_eq!(m.as_slice(), payload(i + 1, 8).as_slice());
    }
}

/// Minimum ring: capacity floor of 8 bytes means max_message_len is 0 —
/// only empty messages fit, and they round-trip.
#[test]
fn minimum_capacity_ring_boundaries() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1);
    assert_eq!(tx.capacity(), 8);
    assert_eq!(tx.max_message_len(), 0);
    for _ in 0..32 {
        tx.push(b"");
        assert_eq!(&*rx.pop(), b"");
    }
    assert!(rx.try_pop().is_none());
}
