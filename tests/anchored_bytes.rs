//! Anchored (composed) **byte** ring tests: the spmc_bytes gate and the
//! broadcast_bytes three-counter protocol on one buffer. Covers the
//! composition matrix — anchor-only round trips (spmc_bytes semantics,
//! variable sizes with boundary/empty/padding paths), observer-only
//! free-run (broadcast_bytes semantics with exact byte-count losses and
//! boundary landings), the combined torture test, free-run anchor joins at
//! byte granularity (the §9.6 proof obligation), the normative
//! gate-before-intent ordering regression [audit F3], the lag-filtered
//! starving release, forget/detach mechanics, both roles' closed contracts,
//! the commit-only write slot, and construction validation.

#![cfg(target_has_atomic = "64")]

use rust_rb::anchored_bytes::{
    BytesAnchor, BytesProducer, BytesRingBuffer, Closed, PopError, SubscribeError,
};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, YieldWait};
use std::sync::Arc;
use std::time::Duration;

const PRIME: u64 = 0x9E37_79B9_7F4A_7C15;

fn make<P, C>(min_capacity: usize) -> (BytesProducer<P, C>, BytesAnchor<P, C>)
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    BytesRingBuffer::<P, C>::with_wait_strategies(min_capacity)
}

/// Rate limiter for torture tests: real sleeps natively, yields under Miri
/// (whose virtual clock makes sleeps meaningless for pacing).
fn throttle(micros: u64) {
    if cfg!(miri) {
        std::thread::yield_now();
    } else {
        std::thread::sleep(Duration::from_micros(micros));
    }
}

/// Deterministic payload for message `seq`: `len` bytes of `(seq + i) as u8`.
fn payload(seq: usize, len: usize) -> Vec<u8> {
    (0..len).map(|i| (seq + i) as u8).collect()
}

// -----------------------------------------------------------------------------
// OFFLINE FRAMING REPLAY
//
// Framing is a pure function of the message-length history (4-byte header,
// records 8-byte aligned, wrap padding when a record does not fit before the
// buffer end), so tests can replay it and verify byte-exact loss accounting
// and boundary landings.
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
// SEQUENCE-STAMPED, CHECKSUMMED MESSAGES
// -----------------------------------------------------------------------------

/// A variable-size message whose entire content is a function of `seq`:
/// `[seq: u64 LE][filler][checksum: u64 LE]`. A consumer can regenerate the
/// expected bytes from the sequence stamp alone, and the trailing checksum
/// makes internal consistency (no torn accept) independently checkable.
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

/// Read the stamp back and check the whole message against [`stamped_msg`].
fn check_stamped(msg: &[u8], max: usize) -> u64 {
    assert!(msg.len() >= 16, "runt message");
    let seq = u64::from_le_bytes(msg[..8].try_into().unwrap());
    let body = msg.len() - 8;
    assert_eq!(
        msg[body..],
        checksum(&msg[..body]).to_le_bytes(),
        "message {seq}: checksum mismatch (torn read)"
    );
    assert_eq!(
        msg,
        stamped_msg(seq, max).as_slice(),
        "message {seq} corrupted"
    );
    seq
}

// -----------------------------------------------------------------------------
// 1. PRODUCER + ONE ANCHOR = SPMC_BYTES SEMANTICS (STRATEGY MINI-MATRIX)
// -----------------------------------------------------------------------------

fn round_trip<P, C>()
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    // Blocking, single-threaded, sizes sweeping empty to max (boundary
    // lengths crossing the 8-byte record alignment).
    let (mut tx, mut rx) = make::<P, C>(256);
    let max = tx.max_message_len();
    assert_eq!(max, 32, "capacity / 8");
    let sizes = [0usize, 1, 3, 4, 5, 8, 12, 13, 31, max];
    for (seq, &len) in sizes.iter().enumerate() {
        tx.push(&payload(seq, len));
        assert_eq!(&*rx.pop().unwrap(), payload(seq, len).as_slice());
    }
    assert!(rx.is_empty());

    // Non-blocking: fill to capacity, overflow reports the gate. 12-byte
    // payloads make 16-byte records; exactly 8 fill the 128-byte ring (the
    // smallest ring whose `capacity / 8` message cap admits 12 bytes).
    let (mut tx, mut rx) = make::<P, C>(128);
    for seq in 0..8 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(!tx.try_push(&payload(8, 12)));
    assert!(tx.try_claim(12).is_none());
    for seq in 0..8 {
        assert_eq!(
            &*rx.try_pop().unwrap().unwrap(),
            payload(seq, 12).as_slice()
        );
    }
    assert!(rx.try_pop().unwrap().is_none());

    // Threaded blocking round trip with mixed sizes (padding at many
    // offsets); producer drop closes.
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
    round_trip::<YieldWait, YieldWait>();
    round_trip::<PauseWait, PauseWait>();
    round_trip::<NoOpWait, BackoffWait>();
}

/// Force the wrap-padding path on every lap for two anchors at once:
/// alternating 8- and 16-byte records shift the write offset so records
/// periodically straddle the wrap boundary of the 64-byte ring.
#[test]
fn wrap_padding_every_lap_two_anchors() {
    let iters = if cfg!(miri) { 400 } else { 10_000 };
    let (mut tx, mut a) = BytesRingBuffer::new(64);
    let mut b = tx.subscribe_anchor().unwrap();
    let lens: Vec<usize> = (0..iters).map(|i| if i % 4 == 0 { 1 } else { 8 }).collect();
    let frames = simulate_frames(64, lens.iter().copied());
    let pads = frames
        .starts
        .iter()
        .zip(std::iter::once(&0u64).chain(frames.ends.iter()))
        .filter(|(start, prev_end)| start != prev_end)
        .count();
    assert!(pads > 0, "the schedule must actually force wrap padding");
    for (seq, &len) in lens.iter().enumerate() {
        tx.push(&payload(seq, len));
        assert_eq!(&*a.pop().unwrap(), payload(seq, len).as_slice());
        assert_eq!(&*b.pop().unwrap(), payload(seq, len).as_slice());
    }
    assert_eq!(tx.tail(), frames.total, "framing matches the replay");
}

#[test]
fn slow_anchor_gates_producer() {
    // 12-byte payloads -> 16-byte records; 8 fill the 128-byte ring exactly.
    let (mut tx, mut fast) = BytesRingBuffer::new(128);
    let mut slow = tx.subscribe_anchor().unwrap();

    for seq in 0..8 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(!tx.try_push(&payload(8, 12)), "full ring must gate");

    // The fast anchor draining everything does not open the gate: the
    // producer gates on the MINIMUM cursor.
    for seq in 0..8 {
        assert_eq!(&*fast.pop().unwrap(), payload(seq, 12).as_slice());
    }
    assert!(!tx.try_push(&payload(8, 12)), "slow anchor still gates");

    // One record's worth of space opens per slow-anchor pop.
    assert_eq!(&*slow.pop().unwrap(), payload(0, 12).as_slice());
    assert!(tx.try_push(&payload(8, 12)));
    assert!(!tx.try_push(&payload(9, 12)));
    assert_eq!(&*slow.pop().unwrap(), payload(1, 12).as_slice());
    assert!(tx.try_push(&payload(9, 12)));
}

// -----------------------------------------------------------------------------
// 2. ZERO ANCHORS + OBSERVERS = BROADCAST_BYTES SEMANTICS EXACTLY
// -----------------------------------------------------------------------------

#[test]
fn zero_anchors_deterministic_lap_exact_missed_bytes() {
    // Capacity 64; ten 8-byte messages are 16-byte records (no padding:
    // 16 divides 64), so tail = 160, latest = 144.
    let (mut tx, anchor) = BytesRingBuffer::new(64);
    let mut obs = tx.subscribe_observer();
    drop(anchor); // zero anchors: pure lossy broadcast regime
    let frames = simulate_frames(64, std::iter::repeat(8).take(10));
    for i in 0..10u64 {
        assert!(tx.try_push(&i.wrapping_mul(PRIME).to_le_bytes()));
    }
    assert_eq!(tx.tail(), frames.total);
    assert_eq!(tx.tail(), 160);
    assert_eq!(obs.lag(), 160);

    // Idle observer at 0: lapped; jumps to `latest` — the start of the most
    // recent record — and reports the skipped bytes exactly.
    assert_eq!(frames.starts[9], 144);
    assert_eq!(
        obs.pop(),
        Err(PopError::Lagged { missed_bytes: 144 }),
        "missed_bytes == latest - old position, exactly"
    );
    assert_eq!(obs.pop().unwrap(), 9u64.wrapping_mul(PRIME).to_le_bytes());
    assert_eq!(obs.try_pop(), Ok(None));
    assert_eq!(obs.lag(), 0);
}

#[test]
fn zero_anchors_lap_with_padding_before_latest() {
    // Craft the stream so the LATEST record is preceded by wrap padding:
    // records 8,16,16,16 end at 56; the fifth record (16 bytes) does not fit
    // in the remaining 8, so 8 bytes of padding precede it at position 64.
    // missed_bytes must cover the padding too — the boundary landing is the
    // record start, after the pad.
    let (mut tx, anchor) = BytesRingBuffer::new(64);
    let mut obs = tx.subscribe_observer();
    drop(anchor);
    let lens = [1usize, 8, 8, 8, 8];
    let frames = simulate_frames(64, lens.iter().copied());
    assert_eq!(frames.starts[4], 64, "padding must precede the last record");
    for (i, &len) in lens.iter().enumerate() {
        assert!(tx.try_push(&vec![i as u8; len]));
    }
    assert_eq!(tx.tail(), 80);
    assert_eq!(
        obs.pop(),
        Err(PopError::Lagged { missed_bytes: 64 }),
        "missed_bytes includes the wrap padding"
    );
    assert_eq!(obs.pop().unwrap(), vec![4u8; 8]);
    assert_eq!(obs.try_pop(), Ok(None));
}

#[test]
fn zero_anchors_observer_accounting_exact_threaded() {
    let n: u64 = if cfg!(miri) { 1_500 } else { 400_000 };
    let capacity = 256usize;
    let (mut tx, anchor) = make::<PauseWait, PauseWait>(capacity);
    let max = tx.max_message_len();
    let mut obs = tx.subscribe_observer();
    drop(anchor);
    let frames = Arc::new(simulate_frames(
        capacity as u64,
        (0..n).map(|s| stamped_len(s, max)),
    ));

    let reader = {
        let frames = Arc::clone(&frames);
        std::thread::spawn(move || {
            let mut pos = 0u64;
            let mut last_seq: Option<u64> = None;
            let mut consumed_bytes = 0u64;
            let mut missed_total = 0u64;
            let mut buf = Vec::new();
            loop {
                match obs.pop_into(&mut buf) {
                    Ok(()) => {
                        let seq = check_stamped(&buf, max);
                        assert!(seq < n);
                        if let Some(prev) = last_seq {
                            assert!(seq > prev, "out of order: {seq} after {prev}");
                        }
                        last_seq = Some(seq);
                        let i = seq as usize;
                        let sequential = if i == 0 { 0 } else { frames.ends[i - 1] };
                        assert!(
                            pos == frames.starts[i] || pos == sequential,
                            "accepted seq {seq} from non-boundary position {pos}"
                        );
                        consumed_bytes += frames.ends[i] - pos;
                        pos = frames.ends[i];
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
        })
    };

    for seq in 0..n {
        tx.push(&stamped_msg(seq, max)); // never blocks: zero anchors
    }
    let total = tx.tail();
    assert_eq!(total, frames.total, "producer framing matches the replay");
    drop(tx);
    reader.join().unwrap();
}

// -----------------------------------------------------------------------------
// 3. THE COMBINED TORTURE TEST: 1 RATE-LIMITED ANCHOR + 2 OBSERVERS
// -----------------------------------------------------------------------------

/// One rate-limited anchor (the gate), one keeping-up observer, one
/// permanently lagging observer, checksummed variable-size payloads. The
/// anchor must see ALL messages in order exactly (anchors structurally
/// cannot lag); the keeping-up observer must see all; the laggard's
/// accepted + missed byte accounting must equal the produced total with no
/// torn accepts and every reposition landing on a record boundary.
///
/// The keeping-up observer is interleaved with the anchor on one thread and
/// drained while the anchor's [`Msg`] guard is still live: with the guard
/// held the anchor's published cursor cannot pass the pinned record, so the
/// producer's declared intent never exceeds `record_start + capacity <=
/// keeper_pos + capacity` — the keeper's "never Lagged" assertion is
/// deterministic, not probabilistic.
#[test]
fn combined_torture_anchor_plus_observers() {
    let n: u64 = if cfg!(miri) { 2_000 } else { 1_000_000 };
    let capacity = 1024usize;
    let (mut tx, mut anchor) = make::<PauseWait, PauseWait>(capacity);
    let max = tx.max_message_len();
    let mut keeper = tx.subscribe_observer();
    let mut laggard = tx.subscribe_observer();
    let frames = Arc::new(simulate_frames(
        capacity as u64,
        (0..n).map(|s| stamped_len(s, max)),
    ));

    let laggard_thread = {
        let frames = Arc::clone(&frames);
        std::thread::spawn(move || {
            let mut pos = 0u64;
            let mut accepted = 0u64;
            let mut consumed_bytes = 0u64;
            let mut missed_total = 0u64;
            let mut buf = Vec::new();
            loop {
                match laggard.pop_into(&mut buf) {
                    Ok(()) => {
                        let seq = check_stamped(&buf, max); // no torn accepts
                        let i = seq as usize;
                        let sequential = if i == 0 { 0 } else { frames.ends[i - 1] };
                        assert!(
                            pos == frames.starts[i] || pos == sequential,
                            "laggard accepted seq {seq} from non-boundary position {pos}"
                        );
                        consumed_bytes += frames.ends[i] - pos;
                        pos = frames.ends[i];
                        accepted += 1;
                        if accepted % 16 == 0 {
                            throttle(200); // permanently lagging
                        }
                    }
                    Err(PopError::Lagged { missed_bytes }) => {
                        pos += missed_bytes;
                        missed_total += missed_bytes;
                        if missed_bytes > 0 {
                            assert!(
                                frames.starts.binary_search(&pos).is_ok(),
                                "laggard reposition off a record boundary (pos {pos})"
                            );
                        }
                    }
                    Err(PopError::Closed) => break,
                }
            }
            assert_eq!(pos, frames.total, "laggard accounting must be gap-free");
            assert_eq!(
                consumed_bytes + missed_total,
                frames.total,
                "laggard accepted + missed bytes must sum to the produced total"
            );
            missed_total
        })
    };

    let combo_thread = std::thread::spawn(move || {
        let mut anchor_expected = 0u64;
        let mut keeper_expected = 0u64;
        let mut keeper_buf = Vec::new();
        while let Ok(msg) = anchor.pop() {
            let seq = check_stamped(&msg, max);
            assert_eq!(seq, anchor_expected, "anchor sees ALL, in order, exactly");
            // Keeper drains while the guard pins the gate (see the test doc
            // for why this makes it lag-proof).
            loop {
                match keeper.try_pop_into(&mut keeper_buf) {
                    Ok(true) => {
                        let kseq = check_stamped(&keeper_buf, max);
                        assert_eq!(kseq, keeper_expected, "keeper must see all, in order");
                        keeper_expected += 1;
                    }
                    Ok(false) => break,
                    Err(PopError::Lagged { .. }) => {
                        panic!("keeping-up observer must never lag")
                    }
                    Err(PopError::Closed) => break,
                }
            }
            drop(msg);
            anchor_expected += 1;
            if anchor_expected % 256 == 0 {
                throttle(5); // the rate limit the producer must track
            }
        }
        // Producer gone: the keeper drains the stable remainder.
        loop {
            match keeper.pop_into(&mut keeper_buf) {
                Ok(()) => {
                    let kseq = check_stamped(&keeper_buf, max);
                    assert_eq!(kseq, keeper_expected);
                    keeper_expected += 1;
                }
                Err(PopError::Lagged { .. }) => panic!("keeper must never lag"),
                Err(PopError::Closed) => break,
            }
        }
        assert_eq!(anchor_expected, n, "the anchor must see every message");
        assert_eq!(
            keeper_expected, n,
            "the keeping-up observer must see every message"
        );
    });

    let producer = std::thread::spawn(move || {
        for seq in 0..n {
            tx.push(&stamped_msg(seq, max));
        }
        tx.tail()
    });

    let total = producer.join().unwrap();
    assert_eq!(total, frames.total, "producer framing matches the replay");
    combo_thread.join().unwrap();
    let missed = laggard_thread.join().unwrap();
    if !cfg!(miri) {
        assert!(missed > 0, "the laggard must actually have lagged");
    }
}

// -----------------------------------------------------------------------------
// 4. FREE-RUN ANCHOR JOIN AT BYTE GRANULARITY (§9.6 PROOF OBLIGATION)
// -----------------------------------------------------------------------------

#[test]
fn anchor_joining_after_free_run_gates_and_parses_cleanly() {
    // Single-threaded M-F1 analog: after an audience-less free-run the
    // producer must notice a joiner within one lap of bytes and gate on it;
    // the joiner starts at a record boundary and parses exactly the
    // post-join frames with no validation.
    let (mut tx, a0) = BytesRingBuffer::new(128);
    let mut obs = tx.subscribe_observer();
    drop(a0);
    for seq in 0..100 {
        // 12-byte payloads -> 16-byte records; many laps of the 128-byte
        // ring (100 records = 12.5 laps).
        assert!(tx.try_push(&payload(seq, 12)), "free-run must never gate");
    }

    let mut late = tx.subscribe_anchor().unwrap();
    assert!(late.try_pop().unwrap().is_none(), "born at the tail");
    for seq in 100..108 {
        assert!(tx.try_push(&payload(seq, 12)));
    }
    assert!(
        !tx.try_push(&payload(108, 12)),
        "the joiner gates the producer"
    );
    for seq in 100..108 {
        assert_eq!(
            &*late.pop().unwrap(),
            payload(seq, 12).as_slice(),
            "joiner parses exactly the post-join frames"
        );
    }
    assert!(tx.try_push(&payload(108, 12)));
    assert_eq!(&*late.pop().unwrap(), payload(108, 12).as_slice());

    // The observer that watched the whole free-run accounts exactly.
    let produced = tx.tail();
    drop(tx);
    let mut pos = 0u64;
    let mut buf = Vec::new();
    loop {
        match obs.pop_into(&mut buf) {
            Ok(()) => pos += record_len(buf.len()),
            Err(PopError::Lagged { missed_bytes }) => pos += missed_bytes,
            Err(PopError::Closed) => break,
        }
    }
    assert_eq!(
        pos, produced,
        "observer byte accounting exact across regimes"
    );
}

/// Threaded free-run joins: the producer free-runs many laps of a small ring
/// (an observer watching), periodically subscribes an anchor and hands it to
/// a consumer thread through a rendezvous channel. From its join point every
/// anchor must parse EVERY frame cleanly, in order — an anchor starting off
/// a record boundary or racing an unseen lap shows up as a checksum or
/// contiguity failure (natively) or a data race (Miri). Anchors that miss
/// the rendezvous are dropped on the spot: join/detach churn under free-run.
#[test]
fn free_run_join_mid_stream_parses_every_frame() {
    let n: u64 = if cfg!(miri) { 3_000 } else { 300_000 };
    let interval = n / 30;
    let pops_per_generation = 300u64;
    let capacity = 1024usize;
    let (mut tx, a0) = make::<PauseWait, PauseWait>(capacity);
    let max = tx.max_message_len();
    let mut obs = tx.subscribe_observer();
    drop(a0);

    // The deterministic frame replay: accept accounting must fold in wrap
    // padding (an accept that crossed a padding frame advances the position
    // by pad + record, not just the record).
    let frames = Arc::new(simulate_frames(
        capacity as u64,
        (0..n).map(|s| stamped_len(s, max)),
    ));

    let (send, recv) = std::sync::mpsc::sync_channel::<BytesAnchor<PauseWait, PauseWait>>(0);

    let consumer = std::thread::spawn(move || {
        let mut generations = 0u64;
        let mut last_seen = 0u64;
        while let Ok(mut anchor) = recv.recv() {
            generations += 1;
            let mut expected: Option<u64> = None;
            for _ in 0..pops_per_generation {
                match anchor.pop() {
                    Ok(msg) => {
                        let seq = check_stamped(&msg, max);
                        if let Some(e) = expected {
                            assert_eq!(seq, e, "anchor misses nothing from its join point");
                        } else {
                            assert!(seq >= last_seen, "join points are monotone");
                        }
                        expected = Some(seq + 1);
                    }
                    Err(Closed) => break,
                }
            }
            if let Some(e) = expected {
                last_seen = e;
            }
        }
        generations
    });

    let observer_thread = {
        let frames = Arc::clone(&frames);
        std::thread::spawn(move || {
            let mut pos = 0u64;
            let mut buf = Vec::new();
            loop {
                match obs.pop_into(&mut buf) {
                    Ok(()) => {
                        // No torn accepts across regimes — and the replay
                        // commits the accept's exact position (pad folded in).
                        let seq = check_stamped(&buf, max) as usize;
                        let sequential = if seq == 0 { 0 } else { frames.ends[seq - 1] };
                        assert!(
                            pos == frames.starts[seq] || pos == sequential,
                            "observer accepted seq {seq} from non-boundary position {pos}"
                        );
                        pos = frames.ends[seq];
                    }
                    Err(PopError::Lagged { missed_bytes }) => pos += missed_bytes,
                    Err(PopError::Closed) => break,
                }
            }
            pos
        })
    };

    for seq in 0..n {
        tx.push(&stamped_msg(seq, max));
        if seq % interval == 0 {
            let anchor = tx.subscribe_anchor().expect("ring is open");
            if seq == 0 {
                // Guarantee at least one full generation: rendezvous.
                send.send(anchor).expect("consumer is alive");
            } else if send.try_send(anchor).is_err() {
                // Consumer busy: the fresh anchor is dropped right here —
                // join-then-instant-detach churn under free-run.
            }
        }
    }
    let produced = tx.tail();
    drop(send);
    drop(tx);

    let generations = consumer.join().unwrap();
    assert!(
        generations >= 1,
        "at least one mid-stream join must complete"
    );
    let observed = observer_thread.join().unwrap();
    // The observer's accepted + missed bytes are folded into one position;
    // note it cannot exceed the produced total and must reach it (the close
    // path drains the stable remainder).
    assert_eq!(observed, produced, "observer accounting exact");
}

/// The d0549dc regression shape (registration-RMW-before-SeqCst-fence),
/// byte-anchored: chained anchor handoff keeps exactly one freshly
/// subscribed anchor live on a tiny ring while the producer laps at full
/// speed. A subscribe choreography violation lets the producer lap an
/// unseen joiner — the joiner's unvalidated plain reads then race the
/// producer's lane stores (Miri flags it; natively the frame checks fail).
#[test]
fn anchor_subscribe_churn_under_running_producer() {
    let messages: u64 = if cfg!(miri) { 1_000 } else { 100_000 };
    // 128 bytes is the smallest ring whose `capacity / 8` cap fits the
    // 16-byte-minimum stamped messages; still tiny — constant lapping.
    let (mut tx, rx) = make::<PauseWait, PauseWait>(128);
    let max = tx.max_message_len();

    let producer = std::thread::spawn(move || {
        for _ in 0..messages {
            tx.push(&stamped_msg(0, max));
        }
    });

    let mut cur = rx;
    'churn: loop {
        let Ok(mut next) = cur.subscribe_anchor() else {
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
// 5. GATE-BEFORE-INTENT (THE NORMATIVE §9.3 ORDER, AUDIT F3)
// -----------------------------------------------------------------------------

/// THE normative-order regression test: an idle anchor freezes the producer
/// mid-stream — first gated in `try_push`/`try_claim`, then parked inside a
/// blocking `push` — and observers behind the frozen tail must drain ALL
/// available bytes with ZERO spurious `Lagged`, then wait. An
/// intent-before-gate implementation publishes `intent = tail + total`
/// while stalled, which fails the window check of any observer more than
/// `capacity - total` behind and loops it on `Lagged` against fully intact
/// data. Release the anchor and everything resumes.
#[test]
fn frozen_gate_observers_drain_all_with_zero_lagged() {
    // 12-byte payloads -> 16-byte records; 8 fill the 128-byte ring.
    let (mut tx, mut anchor) = BytesRingBuffer::new(128);
    let mut obs = tx.subscribe_observer(); // at position 0: the worst case
    for seq in 0..8 {
        assert!(tx.try_push(&payload(seq, 12)));
    }

    // Regime 1: producer gated in try_push/try_claim (intent untouched).
    assert!(!tx.try_push(&payload(8, 12)), "idle anchor must gate");
    assert!(tx.try_claim(12).is_none());

    // The observer — a full capacity behind the frozen tail — drains ALL
    // eight records with zero Lagged…
    for seq in 0..8 {
        match obs.try_pop() {
            Ok(Some(msg)) => assert_eq!(msg, payload(seq, 12), "record {seq}"),
            other => panic!("record {seq}: expected a clean pop, got {other:?}"),
        }
    }
    // …then waits: empty-but-alive, never a spurious Lagged against a
    // stalled producer frontier.
    assert_eq!(obs.try_pop(), Ok(None));
    assert_eq!(obs.lag(), 0);

    // Regime 2: producer parked inside a blocking push at the gate. A
    // second cold observer must again drain everything lag-free while the
    // producer sits mid-push.
    let mut obs2 = tx.subscribe_observer();
    let producer = std::thread::spawn(move || {
        tx.push(&payload(8, 12)); // parks at the gate until the anchor moves
        tx
    });
    std::thread::sleep(Duration::from_millis(50));
    // obs2 joined at the tail (position 128); the frozen frontier shows it
    // nothing — and crucially no Lagged either.
    assert_eq!(obs2.try_pop(), Ok(None), "no spurious Lagged mid-freeze");
    assert_eq!(obs2.lag(), 0);

    // Release the anchor: the parked push completes, everything resumes.
    assert_eq!(&*anchor.pop().unwrap(), payload(0, 12).as_slice());
    let mut tx = producer.join().unwrap();
    assert_eq!(obs.pop().unwrap(), payload(8, 12), "observer resumes");
    assert_eq!(obs2.pop().unwrap(), payload(8, 12));
    for seq in 1..9 {
        assert_eq!(&*anchor.pop().unwrap(), payload(seq, 12).as_slice());
    }
    assert!(tx.try_push(&payload(9, 12)), "ring fully live again");
}

// -----------------------------------------------------------------------------
// 6. STARVING RELEASE THROUGH THE LAG FILTER
// -----------------------------------------------------------------------------

/// The gating anchor's single pop must release a producer blocked on wrap
/// padding, via the lag-filtered starving flush — the freed 24 bytes are far
/// below the 128-byte publish batch, so only the immediate trigger can wake
/// the producer (the spmc_bytes test-7 shape). The starving flag carries the
/// blocked push's exact span — pad(16) + record(24) = 40 bytes — so the
/// release threshold is `capacity - 40 = 984` bytes of published occupancy
/// (tighter still than the worst-case `capacity - max_record_span` =
/// 1024 - 264 = 760 this ring's `capacity / 8` cap guarantees): the blocked
/// producer's gate sits at 1008 > 984, so the release must pass the filter.
#[test]
fn gating_pop_releases_producer_blocked_on_padding() {
    let (mut tx, mut gate) = BytesRingBuffer::new(1024);
    let mut fast = tx.subscribe_anchor().unwrap();
    // 20-byte payload -> 24-byte record; 42 records = 1008 bytes, to_end = 16.
    let msg = [9u8; 20];
    for _ in 0..42 {
        assert!(tx.try_push(&msg));
    }
    // Next push needs pad(16) + record(24) = 40 > 16 free: blocked (and the
    // failed rescan raises the starving flag).
    assert!(!tx.try_push(&msg));

    // The fast anchor draining everything does not help: `gate` is the min.
    while let Ok(Some(m)) = fast.try_pop() {
        assert_eq!(&*m, &msg[..]);
    }
    assert!(!tx.try_push(&msg));

    // One pop frees 24 bytes (24 < the 128-byte batch, not caught up): only
    // the lag-filtered starving flush publishes it immediately — and the
    // gate's occupancy (1008) exceeds the tighter 760-byte threshold.
    drop(gate.pop().unwrap());
    assert!(
        tx.try_push(&msg),
        "the gating anchor's first pop must release the blocked producer"
    );
}

/// A producer parked in a blocking `push` of a max-size message resumes when
/// the gating anchor pops one max-size record.
#[test]
fn blocked_producer_resumes_on_gating_max_size_pop() {
    let (mut tx, mut gate) = BytesRingBuffer::new(1024);
    let mut fast = tx.subscribe_anchor().unwrap();
    let max = tx.max_message_len(); // 128 -> 136-byte records
    for seq in 0..7 {
        tx.push(&payload(seq, max)); // 7 records = 952 of 1024 bytes
    }
    // The fast anchor catches up fully; the gate does not move.
    for seq in 0..7 {
        assert_eq!(&*fast.pop().unwrap(), payload(seq, max).as_slice());
    }
    // The next max push needs pad(72) + record(136) = 208 > 72 free.
    assert!(!tx.try_push(&payload(7, max)));

    let producer = std::thread::spawn(move || {
        tx.push(&payload(7, max)); // parks until the gate frees a record
        tx
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(&*gate.pop().unwrap(), payload(0, max).as_slice());
    let tx = producer.join().unwrap();

    for seq in 1..8 {
        assert_eq!(&*gate.pop().unwrap(), payload(seq, max).as_slice());
    }
    assert_eq!(&*fast.pop().unwrap(), payload(7, max).as_slice());
    drop(tx);
    assert!(matches!(gate.pop(), Err(Closed)));
    assert!(matches!(fast.pop(), Err(Closed)));
}

// -----------------------------------------------------------------------------
// 7. FORGET = REDELIVERY + STALL; CLOSED CONTRACTS; COMMIT-ONLY WRITE SLOT
// -----------------------------------------------------------------------------

#[test]
fn forgotten_msg_redelivers_and_stalls_the_ring() {
    // 12-byte payloads -> 16-byte records; 8 fill the 128-byte ring.
    let (mut tx, mut rx) = BytesRingBuffer::new(128);
    let mut obs = tx.subscribe_observer();
    for seq in 0..8 {
        tx.push(&payload(seq, 12));
    }

    let msg = rx.pop().unwrap();
    std::mem::forget(msg);

    // The cursor never advanced: the producer is still gated by this
    // anchor…
    assert!(!tx.try_push(&payload(8, 12)));
    // …and observers, having drained to the frozen tail, stall with it —
    // with zero spurious Lagged (the F3 order again).
    for seq in 0..8 {
        assert_eq!(obs.pop().unwrap(), payload(seq, 12), "record {seq}");
    }
    assert_eq!(obs.try_pop(), Ok(None), "stalled, not lagged");
    // The same message is delivered again; consuming it opens the gate.
    assert_eq!(&*rx.pop().unwrap(), payload(0, 12).as_slice());
    assert!(tx.try_push(&payload(8, 12)));
    for seq in 1..9 {
        assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
    }
    assert!(rx.try_pop().unwrap().is_none());
}

#[test]
fn closed_contract_anchor() {
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

    // New anchors are refused on a closed ring; observers are born drained.
    assert_eq!(rx.subscribe_anchor().err(), Some(SubscribeError::Closed));
    let mut late_obs = rx.subscribe_observer();
    assert_eq!(late_obs.pop(), Err(PopError::Closed));
}

#[test]
fn closed_contract_observer_drains_then_closed() {
    let (mut tx, rx) = BytesRingBuffer::new(1024);
    let mut obs = tx.subscribe_observer();
    let msgs: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 3 + i as usize * 7]).collect();
    for msg in &msgs {
        tx.push(msg);
    }
    drop(tx);
    drop(rx);
    // Published records stay readable after producer death.
    for msg in &msgs {
        assert_eq!(&obs.pop().unwrap(), msg);
    }
    assert_eq!(obs.pop(), Err(PopError::Closed));
    assert_eq!(obs.try_pop(), Err(PopError::Closed));
    let mut buf = Vec::new();
    assert_eq!(obs.try_pop_into(&mut buf), Err(PopError::Closed));
}

#[test]
fn blocking_pops_return_when_producer_drops() {
    let (tx, mut rx) = make::<YieldWait, BackoffWait>(64);
    let mut obs = tx.subscribe_observer();
    let anchor_waiter = std::thread::spawn(move || rx.pop().map(|m| m.to_vec()));
    let observer_waiter = std::thread::spawn(move || obs.pop());
    std::thread::sleep(Duration::from_millis(50));
    drop(tx);
    assert_eq!(anchor_waiter.join().unwrap(), Err(Closed));
    assert_eq!(observer_waiter.join().unwrap(), Err(PopError::Closed));
}

#[test]
fn claim_commit_only_round_trip_and_abandon() {
    let (mut tx, mut rx) = BytesRingBuffer::new(128);
    let mut obs = tx.subscribe_observer();

    // The write slot is commit-only (no Deref/DerefMut serialize-in-place:
    // observers race the payload lanes, so the producer owns the copy-in).
    tx.claim(8).commit(&[9u8; 8]);
    assert_eq!(&*rx.pop().unwrap(), &[9u8; 8]);
    assert_eq!(obs.pop().unwrap(), [9u8; 8]);

    // An abandoned claim publishes nothing — to either role — and leaves
    // the counters (tail_intent included) untouched.
    {
        let _abandoned = tx.try_claim(16).unwrap();
    }
    assert!(rx.try_pop().unwrap().is_none());
    assert_eq!(obs.try_pop(), Ok(None));

    // The ring stays fully usable after the abandon; the space is reused.
    tx.try_claim(16).unwrap().commit(&payload(7, 16));
    assert_eq!(&*rx.pop().unwrap(), payload(7, 16).as_slice());
    assert_eq!(obs.pop().unwrap(), payload(7, 16));

    // try_claim reports a gated ring like try_push. First walk the write
    // position (currently 40) up to a lap boundary so the fill below meets
    // no wrap padding: 4 x 16-byte records + 1 x 24-byte record = 88 bytes,
    // cursor 40 -> 128 (the 24-byte record ends exactly at the boundary).
    for seq in 0..4 {
        tx.claim(12).commit(&payload(seq, 12));
    }
    tx.claim(16).commit(&payload(4, 16));
    // Drain the anchor completely so only the fill below occupies the ring.
    while rx.try_pop().unwrap().is_some() {}
    // 12-byte payloads make 16-byte records; 8 fill the 128-byte ring
    // exactly (offset 0 of a fresh lap: no padding anywhere).
    for seq in 0..8 {
        tx.claim(12).commit(&payload(seq, 12));
    }
    assert!(tx.try_claim(12).is_none());
    assert_eq!(&*rx.pop().unwrap(), payload(0, 12).as_slice());
    tx.try_claim(12).unwrap().commit(&payload(8, 12));
}

#[test]
#[should_panic(expected = "committed message length must equal the claimed length")]
fn commit_length_mismatch_panics() {
    let (mut tx, _rx) = BytesRingBuffer::new(128);
    tx.claim(8).commit(&[0u8; 4]);
}

// -----------------------------------------------------------------------------
// 8. VIEWS, DRAIN GRANULARITY, MEMBERSHIP MECHANICS, CONSTRUCTION
// -----------------------------------------------------------------------------

#[test]
fn mid_stream_joins_see_only_post_join() {
    let (mut tx, mut rx) = BytesRingBuffer::new(256);
    for seq in 0..8 {
        tx.push(&payload(seq, 12));
    }

    let mut late_anchor = tx.subscribe_anchor().unwrap();
    assert!(late_anchor.try_pop().unwrap().is_none(), "born at the tail");
    assert!(late_anchor.is_empty());

    let mut late_obs = rx.subscribe_observer();
    assert_eq!(late_obs.try_pop(), Ok(None), "observer joins at the tail");
    assert_eq!(late_obs.lag(), 0);

    for seq in 8..12 {
        tx.push(&payload(seq, 12));
    }
    for seq in 8..12 {
        assert_eq!(&*late_anchor.pop().unwrap(), payload(seq, 12).as_slice());
        assert_eq!(late_obs.pop().unwrap(), payload(seq, 12));
    }

    // The original anchor still sees the full stream.
    for seq in 0..12 {
        assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
    }
}

#[test]
fn observer_lag_and_skip_to_latest_in_bytes() {
    let (mut tx, mut rx) = BytesRingBuffer::new(128);
    let mut obs = tx.subscribe_observer();
    assert_eq!(obs.skip_to_latest(), 0, "already at the tail");
    assert_eq!(obs.lag(), 0);

    tx.push(&[1u8; 4]); // record 8
    assert_eq!(obs.lag(), 8, "lag counts framed bytes, header included");
    tx.push(&[2u8; 10]); // record 16
    assert_eq!(obs.lag(), 24);

    assert_eq!(obs.skip_to_latest(), 24, "returns the skipped byte count");
    assert_eq!(obs.lag(), 0);
    assert_eq!(obs.try_pop(), Ok(None), "positioned at the tail: empty");

    tx.push(&[3u8; 3]);
    assert_eq!(obs.pop().unwrap(), [3u8; 3], "next message after the skip");
    assert_eq!(&*rx.pop().unwrap(), &[1u8; 4]); // the anchor missed nothing
}

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

#[test]
fn dropped_anchor_mid_stream_releases_the_gate() {
    let n: u64 = if cfg!(miri) { 1_500 } else { 100_000 };
    let capacity = 256usize;
    let (mut tx, mut anchor) = make::<YieldWait, YieldWait>(capacity);
    let max = tx.max_message_len();
    let mut obs = tx.subscribe_observer();
    // Deterministic frame replay for pad-exact accept accounting.
    let frames = simulate_frames(capacity as u64, (0..n).map(|s| stamped_len(s, max)));

    let producer = std::thread::spawn(move || {
        for seq in 0..n {
            tx.push(&stamped_msg(seq, max));
        }
        tx.tail()
    });

    let quitter = std::thread::spawn(move || {
        // Consume a prefix, then detach mid-stream. If the detach failed to
        // release the gate, the whole test would deadlock.
        for seq in 0..200 {
            assert_eq!(check_stamped(&anchor.pop().unwrap(), max), seq);
        }
        drop(anchor);
    });

    // The observer rides the gated -> free-run transition with exact byte
    // accounting throughout (accepts commit their replayed end position, so
    // wrap padding folded into an accept is accounted too).
    let mut pos = 0u64;
    let mut buf = Vec::new();
    loop {
        match obs.pop_into(&mut buf) {
            Ok(()) => {
                let seq = check_stamped(&buf, max) as usize;
                pos = frames.ends[seq];
            }
            Err(PopError::Lagged { missed_bytes }) => pos += missed_bytes,
            Err(PopError::Closed) => break,
        }
    }
    let produced = producer.join().unwrap();
    assert_eq!(pos, produced, "exact accounting across the regime change");
    quitter.join().unwrap();
}

#[test]
fn registry_grows_past_64_anchors() {
    const ANCHORS: usize = 70;
    // 12-byte payloads -> 16-byte records; 8 fill the 128-byte ring.
    let (mut tx, rx0) = BytesRingBuffer::new(128);
    let mut anchors = vec![rx0];
    for _ in 1..ANCHORS {
        anchors.push(tx.subscribe_anchor().unwrap());
    }
    assert_eq!(tx.anchor_count(), ANCHORS);

    for seq in 0..8 {
        tx.push(&payload(seq, 12));
    }
    assert!(
        !tx.try_push(&payload(8, 12)),
        "all 70 anchors gate, chunk two included"
    );

    // A second-chunk anchor alone holding the gate closed.
    let laggard = anchors.pop().unwrap(); // slot 69: in the appended chunk
    for rx in anchors.iter_mut() {
        for seq in 0..8 {
            assert_eq!(&*rx.pop().unwrap(), payload(seq, 12).as_slice());
        }
    }
    assert!(
        !tx.try_push(&payload(8, 12)),
        "the second-chunk laggard still gates"
    );
    drop(laggard);
    assert!(
        tx.try_push(&payload(8, 12)),
        "detach in chunk two opens the gate"
    );
    assert_eq!(tx.anchor_count(), ANCHORS - 1);

    for rx in anchors.iter_mut() {
        assert_eq!(&*rx.pop().unwrap(), payload(8, 12).as_slice());
    }
}

#[test]
fn capacity_rounds_up_with_16_byte_floor() {
    let (tx, rx) = BytesRingBuffer::new(100);
    assert_eq!(tx.capacity(), 128);
    assert_eq!(rx.capacity(), 128);
    assert_eq!(tx.max_message_len(), 16); // capacity / 8
    assert_eq!(rx.max_message_len(), 16);
    let obs = tx.subscribe_observer();
    assert_eq!(obs.capacity(), 128);
    assert_eq!(obs.max_message_len(), 16);

    // Minimum ring: the floor is 16 (not the other byte rings' 8 — an
    // 8-byte ring's only 8-aligned frame is the whole capacity, which the
    // audience-less gating default could never grant). Max message: 2.
    let (mut tx, mut rx) = BytesRingBuffer::new(1);
    assert_eq!(tx.capacity(), 16);
    assert_eq!(tx.max_message_len(), 2);
    for seq in 0..32 {
        tx.push(&payload(seq, 2));
        assert_eq!(&*rx.pop().unwrap(), payload(seq, 2).as_slice());
        tx.push(b"");
        assert_eq!(&*rx.pop().unwrap(), b"");
    }
    // And the free-run degeneration works at the floor (the reason it is 16).
    drop(rx);
    for seq in 0..32 {
        assert!(tx.try_push(&payload(seq, 2)), "floor ring must free-run");
    }
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_rejected() {
    let _ = BytesRingBuffer::new(0);
}

#[test]
#[should_panic(expected = "exceeds max_message_len")]
fn oversized_message_panics() {
    let (mut tx, _rx) = BytesRingBuffer::new(256);
    let too_big = vec![0u8; tx.max_message_len() + 1];
    tx.push(&too_big);
}
