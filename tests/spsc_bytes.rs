//! Tests for the variable-size-message ring: framing, wrap padding, the
//! zero-copy claim/commit and drain paths, capacity edge cases, and
//! multi-threaded stress with heavily mixed message sizes.

use rust_rb::spsc_bytes::SpscBytes;
use rust_rb::wait::{NoOpWait, PauseWait, YieldWait};

/// Deterministic payload for message `seq`: `len` bytes of `(seq + i) as u8`.
fn payload(seq: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seq + i) as u8).collect()
}

#[test]
fn round_trip_mixed_sizes() {
    let (mut tx, mut rx) = SpscBytes::<256>::new();
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
    let (tx, _rx) = SpscBytes::<100>::new();
    assert_eq!(tx.capacity(), 128);
    let (tx, _rx) = SpscBytes::<1>::new();
    assert_eq!(tx.capacity(), 8);
    assert_eq!(tx.max_message_len(), 0);
}

#[test]
fn zero_length_messages() {
    let (mut tx, mut rx) = SpscBytes::<64>::new();
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
    let (mut tx, mut rx) = SpscBytes::<64>::new();
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
    let (mut tx, mut rx) = SpscBytes::<64>::new();
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
    let (mut tx, mut rx) = SpscBytes::<256>::new();
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
    let (mut tx, _rx) = SpscBytes::<256>::new();
    let too_big = vec![0u8; tx.max_message_len() + 1];
    tx.push(&too_big);
}

#[test]
fn claim_commit_zero_copy() {
    let (mut tx, mut rx) = SpscBytes::<128>::new();

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
    let (mut tx, mut rx) = SpscBytes::<1024>::new();
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
    let (mut tx, mut rx) = SpscBytes::<256, P, C>::new();
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
    let (mut tx, mut rx) = SpscBytes::<512, NoOpWait, NoOpWait>::new();
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
