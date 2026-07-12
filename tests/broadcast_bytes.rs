//! Lossy broadcast **bytes** ring tests (Agrona three-counter design):
//! variable-size round trips across the self-timed wait strategies,
//! independent multi-consumer delivery, wrap-padding paths, the
//! seqlock-torture analog (every accepted message internally consistent and
//! in order, byte-exact loss accounting against an offline framing replay,
//! repositions landing on record boundaries, no panic/OOB ever),
//! deterministic lap mechanics, the max-message-length bound,
//! skip-to-latest/lag in bytes, the closed contract, zero-consumer free-run,
//! and construction validation.

#![cfg(target_has_atomic = "64")]

use rust_rb::broadcast_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer, PopError};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, SleepWait, YieldWait};
use std::sync::Arc;
use std::time::Duration;

const PRIME: u64 = 0x9E37_79B9_7F4A_7C15;

fn make<C: SelfTimed + Send>(min_capacity: usize) -> (BytesProducer, BytesConsumer<C>) {
    BytesRingBuffer::<C>::with_wait_strategies(min_capacity)
}

// -----------------------------------------------------------------------------
// OFFLINE FRAMING REPLAY
//
// Framing is a pure function of the message-length history (4-byte header,
// records 8-byte aligned, wrap padding when a record does not fit before the
// buffer end), so tests can replay it and verify byte-exact loss accounting.
// -----------------------------------------------------------------------------

fn record_len(len: usize) -> u64 {
    (4 + len as u64 + 7) & !7
}

struct Frames {
    /// Byte position of each record's header (after any wrap padding).
    starts: Vec<u64>,
    /// Byte position just past each record (== the tail after its push).
    ends: Vec<u64>,
    /// Total framed bytes == the producer's final tail.
    total: u64,
}

fn simulate_frames(capacity: u64, lens: impl IntoIterator<Item = usize>) -> Frames {
    let mask = capacity - 1;
    let mut pos = 0u64;
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for len in lens {
        let record = record_len(len);
        let off = pos & mask;
        let to_end = capacity - off;
        let pad = if record <= to_end { 0 } else { to_end };
        starts.push(pos + pad);
        pos += pad + record;
        ends.push(pos);
    }
    Frames {
        starts,
        ends,
        total: pos,
    }
}

// -----------------------------------------------------------------------------
// SEQUENCE-STAMPED MESSAGES
// -----------------------------------------------------------------------------

/// A variable-size message whose entire content is a function of `seq`:
/// `[seq: u64 LE][filler][checksum: u64 LE]`. A consumer can regenerate the
/// expected bytes from the sequence stamp alone, and the trailing checksum
/// makes internal consistency independently checkable.
fn stamped_len(seq: u64, max: usize) -> usize {
    assert!(max >= 16);
    16 + ((seq.wrapping_mul(PRIME) >> 33) as usize) % (max - 16 + 1)
}

fn stamped_msg(seq: u64, max: usize) -> Vec<u8> {
    let len = stamped_len(seq, max);
    let mut m = vec![0u8; len];
    m[..8].copy_from_slice(&seq.to_le_bytes());
    for (i, b) in m[8..len - 8].iter_mut().enumerate() {
        *b = (seq.wrapping_mul(PRIME).wrapping_add(i as u64) >> 7) as u8;
    }
    let sum = checksum(&m[..len - 8]);
    m[len - 8..].copy_from_slice(&sum.to_le_bytes());
    m
}

fn checksum(body: &[u8]) -> u64 {
    body.iter()
        .map(|&b| b as u64)
        .sum::<u64>()
        .wrapping_mul(PRIME)
}

// -----------------------------------------------------------------------------
// 1. ROUND TRIP, VARIABLE SIZES, ACROSS WAIT STRATEGIES
// -----------------------------------------------------------------------------

fn round_trip<C: SelfTimed + Send + 'static>() {
    // Single-threaded, keeping up: every interesting size from the empty
    // message to max_message_len, crossing the 8-byte alignment boundaries.
    let (mut tx, mut rx) = make::<C>(4096);
    let max = tx.max_message_len();
    assert_eq!(max, 4096 / 8, "Aeron bound: capacity / 8");
    assert_eq!(rx.try_pop(), Ok(None));
    let mut buf = Vec::new();
    let sizes = [
        0usize, 1, 2, 3, 4, 5, 7, 8, 9, 11, 12, 13, 15, 16, 17, 63, 64, 65, 255, 256, 511, 512,
    ];
    for &len in &sizes {
        assert!(len <= max);
        let msg: Vec<u8> = (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(len as u8))
            .collect();
        tx.push(&msg);
        rx.pop_into(&mut buf).unwrap();
        assert_eq!(buf, msg, "content-exact round trip at len {len}");
    }
    assert_eq!(rx.try_pop_into(&mut buf), Ok(false), "empty-but-alive");

    // Threaded: ring wider than the whole framed stream, so a lap is
    // impossible by construction — every message must arrive in order,
    // content-exact.
    let n: u64 = if cfg!(miri) { 300 } else { 20_000 };
    let capacity = ((n as usize) * 72).next_power_of_two();
    let (mut tx, mut rx) = make::<C>(capacity);
    let consumer = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let mut expected = 0u64;
        loop {
            match rx.pop_into(&mut buf) {
                Ok(()) => {
                    let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    assert_eq!(seq, expected, "in order, gap-free");
                    assert_eq!(buf.len(), 8 + (seq as usize % 57));
                    for (i, &b) in buf[8..].iter().enumerate() {
                        assert_eq!(b, (seq as u8).wrapping_add(i as u8));
                    }
                    expected += 1;
                }
                Err(PopError::Lagged { .. }) => panic!("ring wider than the stream never laps"),
                Err(PopError::Closed) => break,
            }
        }
        assert_eq!(expected, n, "every message must be seen");
    });
    for seq in 0..n {
        let mut msg = seq.to_le_bytes().to_vec();
        msg.extend((0..(seq as usize % 57)).map(|i| (seq as u8).wrapping_add(i as u8)));
        tx.push(&msg);
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

fn consumers_see_all(consumers: usize) {
    let n: u64 = if cfg!(miri) { 300 } else { 50_000 };
    let max = 64usize;
    // Ring wider than the whole framed stream: lag impossible, so every
    // consumer must independently see every message, content-exact.
    let capacity = ((n as usize) * (record_len(max) as usize)).next_power_of_two();
    let (mut tx, rx0) = make::<YieldWait>(capacity);
    let mut rxs = vec![rx0];
    for _ in 1..consumers {
        rxs.push(tx.subscribe::<YieldWait>());
    }

    let mut joins = Vec::new();
    for mut rx in rxs {
        joins.push(std::thread::spawn(move || {
            let mut buf = Vec::new();
            let mut expected = 0u64;
            loop {
                match rx.pop_into(&mut buf) {
                    Ok(()) => {
                        let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        assert_eq!(seq, expected, "in order, gap-free");
                        assert_eq!(buf, stamped_msg(seq, max), "content-exact");
                        expected += 1;
                    }
                    Err(PopError::Lagged { .. }) => panic!("must not lag: ring wider than stream"),
                    Err(PopError::Closed) => break,
                }
            }
            assert_eq!(expected, n, "every message must be seen");
        }));
    }

    let producer = std::thread::spawn(move || {
        for seq in 0..n {
            tx.push(&stamped_msg(seq, max));
        }
    });
    producer.join().unwrap();
    for join in joins {
        join.join().unwrap();
    }
}

#[test]
fn two_consumers_see_all_messages() {
    consumers_see_all(2);
}

#[test]
fn four_consumers_see_all_messages() {
    consumers_see_all(4);
}

// -----------------------------------------------------------------------------
// 3. WRAP PADDING PATHS
// -----------------------------------------------------------------------------

#[test]
fn wrap_padding_skipped_correctly() {
    // Capacity 64, max message 8: records are 8 or 16 bytes. The 8-byte
    // records shift the write position so 16-byte records periodically
    // straddle the wrap boundary and force a padding record.
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    assert_eq!(tx.max_message_len(), 8);
    let lens: Vec<usize> = (0..200).map(|i| if i % 4 == 0 { 1 } else { 8 }).collect();
    let frames = simulate_frames(64, lens.iter().copied());
    let pads = frames
        .starts
        .iter()
        .zip(std::iter::once(&0u64).chain(frames.ends.iter()))
        .filter(|(start, prev_end)| start != prev_end)
        .count();
    assert!(pads > 0, "the schedule must actually force wrap padding");

    let mut buf = Vec::new();
    for (i, &len) in lens.iter().enumerate() {
        let msg: Vec<u8> = (0..len).map(|j| (i as u8).wrapping_add(j as u8)).collect();
        tx.push(&msg);
        rx.pop_into(&mut buf).unwrap();
        assert_eq!(buf, msg, "message {i} must round-trip across padding");
        assert_eq!(rx.lag(), 0, "keeping up: fully drained after each pop");
    }
    assert_eq!(
        tx.tail(),
        frames.total,
        "producer framing matches the replay"
    );
}

// -----------------------------------------------------------------------------
// 4. TORTURE: OUT-OF-BAND VALIDATION UNDER A FLAT-OUT PRODUCER
//
// Small ring, variable-size self-checksummed messages, slow consumers. Every
// ACCEPTED message must be internally consistent and in order; every
// reposition must land on a record boundary; missed_bytes plus consumed
// bytes must sum to the producer's total. This is also the
// garbage-resistance test: the consumer is lapped mid-parse constantly, and
// a torn/garbage length must never cause a panic or an out-of-bounds access.
// -----------------------------------------------------------------------------

fn torture(consumers: usize) {
    let n: u64 = if cfg!(miri) { 2_000 } else { 1_000_000 };
    let capacity = 256usize;
    let (mut tx, rx0) = make::<PauseWait>(capacity);
    let max = tx.max_message_len();
    assert_eq!(max, 32);
    let frames = Arc::new(simulate_frames(
        capacity as u64,
        (0..n).map(|s| stamped_len(s, max)),
    ));

    let mut rxs = vec![rx0];
    for _ in 1..consumers {
        rxs.push(tx.subscribe::<PauseWait>());
    }

    let mut joins = Vec::new();
    for mut rx in rxs {
        let frames = Arc::clone(&frames);
        joins.push(std::thread::spawn(move || {
            let mut pos = 0u64; // expected byte position, replayed
            let mut last_seq: Option<u64> = None;
            let mut accepted = 0u64;
            let mut consumed_bytes = 0u64;
            let mut missed_total = 0u64;
            let mut buf = Vec::new();
            loop {
                match rx.pop_into(&mut buf) {
                    Ok(()) => {
                        assert!(buf.len() >= 16, "runt message accepted");
                        let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        assert!(seq < n, "sequence stamp out of range");
                        // Internal consistency: the embedded checksum.
                        let body = buf.len() - 8;
                        assert_eq!(
                            buf[body..],
                            checksum(&buf[..body]).to_le_bytes(),
                            "torn message accepted: checksum mismatch"
                        );
                        // Content-exact against the regenerated message.
                        assert_eq!(buf, stamped_msg(seq, max), "accepted bytes must be exact");
                        // Strictly in order across laps.
                        if let Some(prev) = last_seq {
                            assert!(seq > prev, "out of order: {seq} after {prev}");
                        }
                        last_seq = Some(seq);
                        // Byte accounting: we advanced either from the
                        // previous record's end (sequential, padding skip
                        // included) or from this record's start (fresh from
                        // a reposition).
                        let i = seq as usize;
                        let sequential = if i == 0 { 0 } else { frames.ends[i - 1] };
                        assert!(
                            pos == frames.starts[i] || pos == sequential,
                            "accepted seq {seq} from non-boundary position {pos}"
                        );
                        consumed_bytes += frames.ends[i] - pos;
                        pos = frames.ends[i];
                        accepted += 1;
                        // Slow-ish reader: force the producer to lap us.
                        if accepted % 64 == 0 {
                            std::thread::yield_now();
                        }
                    }
                    Err(PopError::Lagged { missed_bytes }) => {
                        pos += missed_bytes;
                        missed_total += missed_bytes;
                        if missed_bytes > 0 {
                            assert!(
                                frames.starts.binary_search(&pos).is_ok(),
                                "reposition must land on a record boundary (pos {pos})"
                            );
                        }
                    }
                    Err(PopError::Closed) => break,
                }
            }
            assert_eq!(pos, frames.total, "gap-free byte accounting");
            assert_eq!(
                consumed_bytes + missed_total,
                frames.total,
                "consumed + missed must sum to the produced total"
            );
        }));
    }

    let producer = std::thread::spawn(move || {
        for seq in 0..n {
            tx.push(&stamped_msg(seq, max));
        }
        tx.tail()
    });
    let total = producer.join().unwrap();
    assert_eq!(total, frames.total, "producer framing matches the replay");
    for join in joins {
        join.join().unwrap();
    }
}

#[test]
fn torture_one_consumer() {
    torture(1);
}

#[test]
fn torture_two_consumers() {
    torture(2);
}

// -----------------------------------------------------------------------------
// 5. DETERMINISTIC LAP MECHANICS
// -----------------------------------------------------------------------------

#[test]
fn deterministic_lap_reports_exact_missed_bytes() {
    // Capacity 64; ten 8-byte messages are 16-byte records (no padding:
    // 16 divides 64), so tail = 160, latest = 144, intent = 160.
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    let frames = simulate_frames(64, std::iter::repeat(8).take(10));
    for i in 0..10u64 {
        tx.push(&i.wrapping_mul(PRIME).to_le_bytes());
    }
    assert_eq!(tx.tail(), frames.total);
    assert_eq!(tx.tail(), 160);
    assert_eq!(rx.lag(), 160);

    // Idle consumer at 0: the producer's declared frontier is more than one
    // capacity ahead, so the first pop laps and repositions to `latest` —
    // the start of the most recent record.
    assert_eq!(frames.starts[9], 144);
    assert_eq!(
        rx.pop(),
        Err(PopError::Lagged { missed_bytes: 144 }),
        "missed_bytes == latest - old position, exactly"
    );
    // The latest record is intact and readable.
    assert_eq!(rx.pop().unwrap(), 9u64.wrapping_mul(PRIME).to_le_bytes());
    assert_eq!(rx.try_pop(), Ok(None));
    assert_eq!(rx.lag(), 0);
}

#[test]
fn deterministic_lap_with_padding_before_latest() {
    // Craft the stream so the LATEST record is preceded by wrap padding:
    // records 8,16,16,16 end at 56; the fifth record (16 bytes) does not fit
    // in the remaining 8, so 8 bytes of padding precede it at position 64.
    // missed_bytes must cover the padding too — the reposition target is the
    // record start, after the pad.
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    let lens = [1usize, 8, 8, 8, 8];
    let frames = simulate_frames(64, lens.iter().copied());
    assert_eq!(frames.starts[4], 64, "padding must precede the last record");
    for (i, &len) in lens.iter().enumerate() {
        tx.push(&vec![i as u8; len]);
    }
    assert_eq!(tx.tail(), 80);
    assert_eq!(
        rx.pop(),
        Err(PopError::Lagged { missed_bytes: 64 }),
        "missed_bytes includes the wrap padding"
    );
    assert_eq!(rx.pop().unwrap(), vec![4u8; 8]);
    assert_eq!(rx.try_pop(), Ok(None));
}

// -----------------------------------------------------------------------------
// 6. MAX MESSAGE LENGTH: BOUNDARY AND OVER-LIMIT
// -----------------------------------------------------------------------------

#[test]
fn max_message_len_boundary_round_trips() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    assert_eq!(tx.max_message_len(), 256 / 8);
    assert_eq!(rx.max_message_len(), 256 / 8);
    let msg = vec![0xABu8; 32];
    tx.push(&msg);
    assert_eq!(rx.pop().unwrap(), msg);
}

#[test]
#[should_panic(expected = "exceeds max_message_len")]
fn over_limit_push_panics() {
    let (mut tx, _rx) = BytesRingBuffer::new(256);
    tx.push(&[0u8; 33]);
}

// -----------------------------------------------------------------------------
// 7. CLOSED CONTRACT
// -----------------------------------------------------------------------------

#[test]
fn closed_only_after_drain() {
    let (mut tx, mut rx) = BytesRingBuffer::new(1024);
    let msgs: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 3 + i as usize * 7]).collect();
    for msg in &msgs {
        tx.push(msg);
    }
    drop(tx);
    // Published records stay readable after producer death.
    for msg in &msgs {
        assert_eq!(&rx.pop().unwrap(), msg);
    }
    assert_eq!(rx.pop(), Err(PopError::Closed));
    assert_eq!(rx.try_pop(), Err(PopError::Closed));
    let mut buf = Vec::new();
    assert_eq!(rx.try_pop_into(&mut buf), Err(PopError::Closed));
}

#[test]
fn lagged_consumer_drains_remainder_after_close() {
    let (mut tx, mut rx) = BytesRingBuffer::new(64);
    for i in 0..10u64 {
        tx.push(&i.to_le_bytes());
    }
    drop(tx);
    assert_eq!(rx.pop(), Err(PopError::Lagged { missed_bytes: 144 }));
    assert_eq!(rx.pop().unwrap(), 9u64.to_le_bytes());
    assert_eq!(rx.pop(), Err(PopError::Closed));
}

fn parked_pop_wakes_on_close<C: SelfTimed + Send + 'static>() {
    let (tx, mut rx) = make::<C>(64);
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
    let (mut tx, rx) = BytesRingBuffer::new(64);
    tx.push(b"x");
    drop(tx);
    // Subscribing never fails; a consumer born on a closed ring joins at the
    // tail, is born drained, and pops Closed.
    let mut late = rx.subscribe();
    assert_eq!(late.pop(), Err(PopError::Closed));
    assert_eq!(late.try_pop(), Err(PopError::Closed));
}

// -----------------------------------------------------------------------------
// 8. SKIP TO LATEST AND LAG, IN BYTES
// -----------------------------------------------------------------------------

#[test]
fn skip_to_latest_and_lag_in_bytes() {
    let (mut tx, mut rx) = BytesRingBuffer::new(128);
    assert_eq!(rx.skip_to_latest(), 0, "already at the tail");
    assert_eq!(rx.lag(), 0);

    tx.push(&[1u8; 4]); // record 8
    assert_eq!(rx.lag(), 8, "lag counts framed bytes, header included");
    tx.push(&[2u8; 10]); // record 16
    assert_eq!(rx.lag(), 24);

    assert_eq!(rx.skip_to_latest(), 24, "returns the skipped byte count");
    assert_eq!(rx.lag(), 0);
    assert_eq!(rx.try_pop(), Ok(None), "positioned at the tail: empty");

    tx.push(&[3u8; 3]);
    assert_eq!(rx.pop().unwrap(), [3u8; 3], "next message after the skip");
}

// -----------------------------------------------------------------------------
// 9. ZERO CONSUMERS: FREE RUN
// -----------------------------------------------------------------------------

#[test]
fn zero_consumers_free_run() {
    let (mut tx, rx) = BytesRingBuffer::new(64);
    drop(rx);
    let lens: Vec<usize> = (0..100).map(|i| i % 9).collect();
    let frames = simulate_frames(64, lens.iter().copied());
    for &len in &lens {
        tx.push(&vec![7u8; len]); // never blocks, no consumer state to consult
    }
    assert_eq!(tx.tail(), frames.total);
    let mut late = tx.subscribe::<YieldWait>();
    assert_eq!(late.try_pop(), Ok(None));
    drop(tx);
    assert_eq!(late.pop(), Err(PopError::Closed));
}

// -----------------------------------------------------------------------------
// 10. CONSTRUCTION VALIDATION
// -----------------------------------------------------------------------------

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_rejected() {
    let _ = BytesRingBuffer::new(0);
}

#[test]
fn tiny_capacity_rounds_up_to_minimum() {
    let (tx, _rx) = BytesRingBuffer::new(1);
    assert_eq!(tx.capacity(), 8);
    assert_eq!(tx.max_message_len(), 1);
}
