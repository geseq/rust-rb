//! Anchored (composed) ring tests: the spmc gate and the broadcast slot
//! protocol on one buffer. Covers the composition matrix — anchor-only
//! round trips (spmc semantics), observer-only free-run (broadcast
//! semantics), the combined torture test, free-run anchor joins (the §9.6
//! proof obligation), anchor gating with observers draining a frozen tail,
//! forget/detach gate mechanics, both roles' closed contracts, the
//! commit-only write slot, and construction validation.

#![cfg(target_has_atomic = "64")]

use rust_rb::anchored::{Anchor, Closed, NoUninit, PopError, Producer, RingBuffer, SubscribeError};
use rust_rb::wait::{BackoffWait, NoOpWait, PauseWait, SelfTimed, YieldWait};
use std::time::Duration;

const PRIME: u64 = 0x9E37_79B9_7F4A_7C15;

fn make<P, C>(min_capacity: usize) -> (Producer<u64, P, C>, Anchor<u64, P, C>)
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    RingBuffer::<u64, P, C>::with_wait_strategies(min_capacity)
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

// -----------------------------------------------------------------------------
// 1. PRODUCER + ONE ANCHOR = SPMC SEMANTICS (STRATEGY MINI-MATRIX)
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
    let messages: u64 = if cfg!(miri) { 2_000 } else { 10_000 };
    let (mut tx, mut rx) = make::<P, C>(64);
    let producer = std::thread::spawn(move || {
        for i in 0..messages {
            tx.push(i);
        }
    });
    let mut expected = 0u64;
    while let Ok(v) = rx.pop() {
        assert_eq!(v, expected, "an anchor sees every message, in order");
        expected += 1;
    }
    assert_eq!(expected, messages);
    producer.join().unwrap();
}

#[test]
fn round_trip_yield() {
    round_trip::<YieldWait, YieldWait>();
}

#[test]
fn round_trip_pause() {
    round_trip::<PauseWait, PauseWait>();
}

#[test]
fn round_trip_noop_backoff() {
    round_trip::<NoOpWait, BackoffWait>();
}

// -----------------------------------------------------------------------------
// 2. ANCHOR GATING; OBSERVERS DRAIN A FROZEN TAIL WITHOUT SPURIOUS LAG
// -----------------------------------------------------------------------------

#[test]
fn idle_anchor_gates_producer_observer_drains_freely() {
    let (mut tx, mut anchor) = RingBuffer::<u64>::new(4);
    let mut obs = tx.subscribe_observer();

    for i in 0..4 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(4), Err(4), "idle anchor must gate a full ring");
    assert!(tx.is_full());

    // The observer drains freely up to the frozen tail…
    for i in 0..4 {
        assert_eq!(obs.pop(), Ok(i));
    }
    // …then waits: empty-but-alive, never a spurious Lagged against a gated
    // (stalled) producer frontier.
    assert_eq!(obs.try_pop(), Ok(None));
    assert_eq!(obs.lag(), 0);
    // Observer progress opens nothing: the gate is anchors-only.
    assert_eq!(tx.try_push(4), Err(4));

    // One slot opens per anchor advance.
    assert_eq!(anchor.pop(), Ok(0));
    assert!(tx.try_push(4).is_ok());
    assert_eq!(tx.try_push(5), Err(5));

    // Blocking push released by the anchor.
    let producer = std::thread::spawn(move || {
        tx.push(5); // blocks until the anchor frees a slot
        tx
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(anchor.pop(), Ok(1));
    let _tx = producer.join().unwrap();
    for i in 2..6 {
        assert_eq!(anchor.pop(), Ok(i));
    }
    assert_eq!(obs.pop(), Ok(4));
    assert_eq!(obs.pop(), Ok(5));
}

// -----------------------------------------------------------------------------
// 3. ZERO ANCHORS: FREE-RUN, OBSERVERS GET BROADCAST SEMANTICS EXACTLY
// -----------------------------------------------------------------------------

#[test]
fn zero_anchors_free_run_observer_lagged_exact() {
    let (mut tx, anchor) = RingBuffer::<u64, YieldWait, YieldWait>::with_slack(8, 2);
    let mut obs = tx.subscribe_observer();
    drop(anchor); // zero anchors: pure broadcast regime

    for i in 0..20 {
        assert!(tx.try_push(i).is_ok(), "audience-less producer free-runs");
    }
    assert_eq!(tx.tail(), 20);
    assert_eq!(obs.lag(), 20);

    // new_pos = tail - capacity + slack = 20 - 8 + 2 = 14; old_pos = 0.
    assert_eq!(obs.pop(), Err(PopError::Lagged { missed: 14 }));
    let mut got = Vec::new();
    while let Ok(Some(v)) = obs.try_pop() {
        got.push(v);
    }
    assert_eq!(got, (14..20).collect::<Vec<_>>());
    assert_eq!(obs.try_pop(), Ok(None));
    assert_eq!(obs.lag(), 0);
}

#[test]
fn zero_anchors_observer_accounting_exact_threaded() {
    let n: u64 = if cfg!(miri) { 2_000 } else { 500_000 };
    let (mut tx, anchor) = RingBuffer::<[u64; 2], PauseWait, PauseWait>::with_wait_strategies(8);
    let mut obs = tx.subscribe_observer();
    drop(anchor);

    let reader = std::thread::spawn(move || {
        let mut next = 0u64;
        let mut accepted = 0u64;
        let mut missed_total = 0u64;
        loop {
            match obs.pop() {
                Ok(v) => {
                    assert_eq!(v[1], v[0].wrapping_mul(PRIME), "torn observer read");
                    assert_eq!(v[0], next, "accepted positions must be exact");
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
        assert_eq!(next, n, "gap-free accounting");
        assert_eq!(accepted + missed_total, n, "exact loss accounting");
    });

    for i in 0..n {
        tx.push([i, i.wrapping_mul(PRIME)]); // never blocks: zero anchors
    }
    drop(tx);
    reader.join().unwrap();
}

// -----------------------------------------------------------------------------
// 4. THE COMBINED TORTURE TEST (§9.6): 1 RATE-LIMITED ANCHOR + 2 OBSERVERS
// -----------------------------------------------------------------------------

/// One rate-limited anchor (the gate), one keeping-up observer, one
/// permanently lagging observer, checksummed payloads. The anchor must see
/// ALL messages in order exactly (anchors structurally cannot lag); the
/// keeping-up observer must see all; the laggard's accepted + missed
/// accounting must be exact with no torn accepts. Producer throughput tracks
/// the anchor by the gate.
///
/// The keeping-up observer is interleaved with the anchor on one thread and
/// drained while the anchor's `PopRef` guard is still live: with the guard
/// held the anchor's cursor (hence the gate) cannot pass the message under
/// it, so the producer can never reach `keeper_pos + capacity` — the
/// keeper's "never Lagged" assertion is deterministic, not probabilistic.
#[test]
fn combined_torture_anchor_plus_observers() {
    let n: u64 = if cfg!(miri) { 2_000 } else { 1_000_000 };
    let (mut tx, mut anchor) =
        RingBuffer::<[u64; 2], PauseWait, PauseWait>::with_wait_strategies(1024);
    let mut keeper = tx.subscribe_observer();
    let mut laggard = tx.subscribe_observer();

    let laggard_thread = std::thread::spawn(move || {
        let mut next = 0u64;
        let mut accepted = 0u64;
        let mut missed_total = 0u64;
        loop {
            match laggard.pop() {
                Ok(v) => {
                    assert_eq!(v[1], v[0].wrapping_mul(PRIME), "torn laggard accept");
                    assert_eq!(v[0], next, "laggard accepts must be position-exact");
                    next += 1;
                    accepted += 1;
                    if accepted % 16 == 0 {
                        throttle(200); // permanently lagging
                    }
                }
                Err(PopError::Lagged { missed }) => {
                    next += missed;
                    missed_total += missed;
                }
                Err(PopError::Closed) => break,
            }
        }
        assert_eq!(next, n, "laggard accounting must be gap-free");
        assert_eq!(
            accepted + missed_total,
            n,
            "laggard accounting must be exact"
        );
        missed_total
    });

    let combo_thread = std::thread::spawn(move || {
        let mut anchor_expected = 0u64;
        let mut keeper_expected = 0u64;
        let drain_keeper =
            |keeper: &mut rust_rb::anchored::Observer<[u64; 2], PauseWait, PauseWait>,
             keeper_expected: &mut u64| {
                loop {
                    match keeper.try_pop() {
                        Ok(Some(v)) => {
                            assert_eq!(v[1], v[0].wrapping_mul(PRIME), "torn keeper read");
                            assert_eq!(v[0], *keeper_expected, "keeper must see all, in order");
                            *keeper_expected += 1;
                        }
                        Ok(None) => break,
                        Err(PopError::Lagged { .. }) => {
                            panic!("keeping-up observer must never lag")
                        }
                        Err(PopError::Closed) => break,
                    }
                }
            };
        while let Ok(msg) = anchor.pop_ref() {
            assert_eq!(
                msg[0], anchor_expected,
                "anchor sees ALL, in order, exactly"
            );
            assert_eq!(msg[1], anchor_expected.wrapping_mul(PRIME));
            // Keeper drains while the guard pins the gate (see the test doc
            // for why this makes it lag-proof).
            drain_keeper(&mut keeper, &mut keeper_expected);
            drop(msg);
            anchor_expected += 1;
            if anchor_expected % 256 == 0 {
                throttle(5); // the rate limit the producer must track
            }
        }
        // Producer gone: the keeper drains the stable remainder.
        loop {
            match keeper.pop() {
                Ok(v) => {
                    assert_eq!(v[0], keeper_expected);
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
        for i in 0..n {
            tx.push([i, i.wrapping_mul(PRIME)]);
        }
    });

    producer.join().unwrap();
    combo_thread.join().unwrap();
    let missed = laggard_thread.join().unwrap();
    if !cfg!(miri) {
        assert!(missed > 0, "the laggard must actually have lagged");
    }
}

// -----------------------------------------------------------------------------
// 5. FREE-RUN ANCHOR JOIN (§9.6 PROOF OBLIGATION AS A TEST)
// -----------------------------------------------------------------------------

#[test]
fn anchor_joining_after_free_run_gates_and_reads_exactly() {
    // Single-threaded M-F1 analog: after an audience-less free-run the
    // producer must notice a joiner within one lap and gate on it, and the
    // joiner reads exactly the post-join values with no validation.
    let (mut tx, a0) = RingBuffer::<u64>::new(4);
    let mut obs = tx.subscribe_observer();
    drop(a0);
    for i in 0..100 {
        assert!(tx.try_push(i).is_ok(), "free-run must never gate");
    }

    let mut late = tx.subscribe_anchor().unwrap();
    for i in 100..104 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(104), Err(104), "the joiner gates the producer");
    for i in 100..104 {
        assert_eq!(
            late.pop(),
            Ok(i),
            "joiner sees exactly the post-join stream"
        );
    }
    assert!(tx.try_push(104).is_ok());
    assert_eq!(late.pop(), Ok(104));

    // The observer that watched the whole free-run accounts exactly.
    drop(tx);
    let mut next = 0u64;
    let mut accepted = 0u64;
    let mut missed_total = 0u64;
    loop {
        match obs.pop() {
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
    assert_eq!(accepted + missed_total, 105);
}

/// Threaded free-run joins: the producer free-runs many laps of a small ring
/// (observers watching), periodically subscribes an anchor and hands it to a
/// consumer thread through a rendezvous channel. From its join point every
/// anchor must see EVERY message, in order, with values matching the
/// sequence exactly — an unvalidated torn or lapped anchor read shows up as
/// a checksum or contiguity failure. Anchors that miss the rendezvous are
/// dropped on the spot: join/detach churn under free-run.
#[test]
fn free_run_join_mid_stream_sees_every_message_unvalidated() {
    type Elem = [u64; 2];
    let n: u64 = if cfg!(miri) { 3_000 } else { 300_000 };
    let interval = n / 30;
    let pops_per_generation = 500u64;
    let (mut tx, a0) = RingBuffer::<Elem, PauseWait, PauseWait>::with_wait_strategies(1024);
    let mut obs = tx.subscribe_observer();
    drop(a0);

    let (send, recv) = std::sync::mpsc::sync_channel::<Anchor<Elem, PauseWait, PauseWait>>(0);

    let consumer = std::thread::spawn(move || {
        let mut generations = 0u64;
        let mut last_seen = 0u64;
        while let Ok(mut anchor) = recv.recv() {
            generations += 1;
            let mut expected: Option<u64> = None;
            for _ in 0..pops_per_generation {
                match anchor.pop() {
                    Ok(v) => {
                        assert_eq!(v[1], v[0].wrapping_mul(PRIME), "torn anchor read");
                        if let Some(e) = expected {
                            assert_eq!(v[0], e, "anchor misses nothing from its join point");
                        } else {
                            assert!(v[0] >= last_seen, "join points are monotone");
                        }
                        expected = Some(v[0] + 1);
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

    let observer_thread = std::thread::spawn(move || {
        let mut next = 0u64;
        let mut accepted = 0u64;
        let mut missed_total = 0u64;
        loop {
            match obs.pop() {
                Ok(v) => {
                    assert_eq!(v[1], v[0].wrapping_mul(PRIME), "torn observer read");
                    assert_eq!(v[0], next);
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
        assert_eq!(
            accepted + missed_total,
            n,
            "observer accounting exact across regimes"
        );
    });

    for i in 0..n {
        tx.push([i, i.wrapping_mul(PRIME)]);
        if i % interval == 0 {
            let anchor = tx.subscribe_anchor().expect("ring is open");
            if i == 0 {
                // Guarantee at least one full generation: rendezvous.
                send.send(anchor).expect("consumer is alive");
            } else if send.try_send(anchor).is_err() {
                // Consumer busy: the fresh anchor is dropped right here —
                // join-then-instant-detach churn under free-run.
            }
        }
    }
    drop(send);
    drop(tx);

    let generations = consumer.join().unwrap();
    assert!(
        generations >= 1,
        "at least one mid-stream join must complete"
    );
    observer_thread.join().unwrap();
}

/// The d0549dc regression shape (registration-RMW-before-SeqCst-fence),
/// element-anchored analog of spmc's subscribe churn: chained anchor handoff
/// keeps exactly one freshly subscribed anchor live on a tiny ring while the
/// producer laps at full speed. A subscribe choreography violation lets the
/// producer lap an unseen joiner — the joiner's unvalidated plain read then
/// races the producer's overwrite (Miri flags it; natively the contiguity
/// assert fails).
#[test]
fn anchor_subscribe_churn_under_running_producer() {
    let messages: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    let (mut tx, rx) = make::<PauseWait, PauseWait>(64);

    let producer = std::thread::spawn(move || {
        for i in 0..messages {
            tx.push(i);
        }
    });

    let mut cur = rx;
    'churn: loop {
        let Ok(mut next) = cur.subscribe_anchor() else {
            break 'churn;
        };
        drop(cur);
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
// 6. FORGET = REDELIVERY + STALL; ANCHOR DROP RELEASES THE GATE
// -----------------------------------------------------------------------------

#[test]
fn forgotten_pop_ref_redelivers_and_stalls_the_ring() {
    let (mut tx, mut anchor) = RingBuffer::<u64>::new(4);
    tx.push(7);

    let msg = anchor.pop_ref().unwrap();
    assert_eq!(*msg, 7);
    std::mem::forget(msg);

    // The cursor never advanced: three more pushes fill the ring, then the
    // un-advanced anchor stalls the producer globally.
    for i in 0..3 {
        assert!(tx.try_push(i).is_ok());
    }
    assert_eq!(tx.try_push(99), Err(99), "forget-then-idle stalls the ring");

    // The same element is delivered again; consuming it opens the gate.
    assert_eq!(anchor.pop(), Ok(7));
    assert!(tx.try_push(99).is_ok());
}

#[test]
fn dropping_anchor_mid_stream_releases_the_gate() {
    let n: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    let (mut tx, mut anchor) = make::<YieldWait, YieldWait>(64);
    let mut obs = tx.subscribe_observer();

    let producer = std::thread::spawn(move || {
        for i in 0..n {
            tx.push(i);
        }
    });

    let quitter = std::thread::spawn(move || {
        // Consume a prefix, then detach mid-stream. If the detach failed to
        // release the gate, the whole test would deadlock.
        for i in 0..500 {
            assert_eq!(anchor.pop(), Ok(i));
        }
        drop(anchor);
    });

    // The observer rides the gated -> free-run transition with exact
    // accounting throughout.
    let mut next = 0u64;
    let mut accepted = 0u64;
    let mut missed_total = 0u64;
    loop {
        match obs.pop() {
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
    assert_eq!(accepted + missed_total, n);
    producer.join().unwrap();
    quitter.join().unwrap();
}

// -----------------------------------------------------------------------------
// 7. CLOSED CONTRACTS, BOTH ROLES
// -----------------------------------------------------------------------------

#[test]
fn closed_contract_anchor() {
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

    // Drained: every anchor entry point reports Closed.
    assert_eq!(rx.try_pop(), Err(Closed));
    assert_eq!(rx.pop(), Err(Closed));
    assert_eq!(rx.pop_ref().err(), Some(Closed));
    assert!(matches!(rx.try_pop_ref(), Err(Closed)));

    // New anchors are refused; new observers are born drained.
    assert_eq!(rx.subscribe_anchor().err(), Some(SubscribeError::Closed));
    let mut late_obs = rx.subscribe_observer();
    assert_eq!(late_obs.pop(), Err(PopError::Closed));
}

#[test]
fn closed_contract_observer_drains_then_closed() {
    let (mut tx, rx) = RingBuffer::<u64>::new(8);
    let mut obs = tx.subscribe_observer();
    for i in 0..5 {
        tx.push(i);
    }
    drop(tx);
    drop(rx);
    // Published slots stay readable after producer death.
    for i in 0..5 {
        assert_eq!(obs.pop(), Ok(i));
    }
    assert_eq!(obs.pop(), Err(PopError::Closed));
    assert_eq!(obs.try_pop(), Err(PopError::Closed));
}

#[test]
fn blocking_pops_return_when_producer_drops() {
    let (tx, mut rx) = make::<YieldWait, BackoffWait>(8);
    let mut obs = tx.subscribe_observer();
    let anchor_waiter = std::thread::spawn(move || rx.pop());
    let observer_waiter = std::thread::spawn(move || obs.pop());
    std::thread::sleep(Duration::from_millis(50));
    drop(tx);
    assert_eq!(anchor_waiter.join().unwrap(), Err(Closed));
    assert_eq!(observer_waiter.join().unwrap(), Err(PopError::Closed));
}

// -----------------------------------------------------------------------------
// 8. WRITE SLOT: COMMIT-ONLY API; CLAIM-ABANDON IS SAFE
// -----------------------------------------------------------------------------

#[test]
fn claim_commit_round_trip_and_abandon() {
    let (mut tx, mut rx) = RingBuffer::<[u64; 4]>::new(8);
    let mut obs = tx.subscribe_observer();

    // The write slot is commit-only (no `uninit`/`commit_init`: observers
    // race the payload write, so the producer must own the atomic copy-in).
    tx.claim().commit([1, 2, 3, 4]);
    assert_eq!(rx.pop(), Ok([1, 2, 3, 4]));
    assert_eq!(obs.pop(), Ok([1, 2, 3, 4]));

    // An abandoned claim publishes nothing — to either role.
    {
        let _abandoned = tx.try_claim().unwrap();
    }
    assert_eq!(rx.try_pop(), Ok(None));
    assert_eq!(obs.try_pop(), Ok(None));

    // The ring stays fully usable after the abandon.
    tx.claim().commit([5, 6, 7, 8]);
    assert_eq!(rx.pop(), Ok([5, 6, 7, 8]));
    assert_eq!(obs.pop(), Ok([5, 6, 7, 8]));

    // try_claim reports a gated ring like try_push.
    for i in 0..8 {
        tx.claim().commit([i; 4]);
    }
    assert!(tx.try_claim().is_none());
    assert_eq!(rx.pop(), Ok([0; 4]));
    tx.try_claim().unwrap().commit([9; 4]);
}

// -----------------------------------------------------------------------------
// 9. JOIN POINTS, VIEWS, DRAIN, MEMBERSHIP MECHANICS
// -----------------------------------------------------------------------------

#[test]
fn mid_stream_joins_see_only_post_join() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    for i in 0..8 {
        tx.push(i);
    }

    let mut late_anchor = tx.subscribe_anchor().unwrap();
    assert_eq!(late_anchor.try_pop(), Ok(None), "nothing pre-join is seen");
    assert!(late_anchor.is_empty());

    let mut late_obs = rx.subscribe_observer();
    assert_eq!(late_obs.try_pop(), Ok(None), "observer joins at the tail");
    assert_eq!(late_obs.lag(), 0);

    for i in 8..12 {
        tx.push(i);
    }
    for i in 8..12 {
        assert_eq!(late_anchor.pop(), Ok(i));
        assert_eq!(late_obs.pop(), Ok(i));
    }

    // The original anchor still sees the full stream.
    for i in 0..12 {
        assert_eq!(rx.pop(), Ok(i));
    }
}

#[test]
fn observer_lag_and_skip_to_latest() {
    let (mut tx, mut rx) = RingBuffer::<u64>::new(16);
    let mut obs = tx.subscribe_observer();
    assert_eq!(obs.skip_to_latest(), 0);
    for i in 0..10 {
        tx.push(i);
    }
    assert_eq!(obs.lag(), 10);
    assert_eq!(obs.skip_to_latest(), 10);
    assert_eq!(obs.try_pop(), Ok(None));
    tx.push(10);
    assert_eq!(obs.pop(), Ok(10));
    for i in 0..11 {
        assert_eq!(rx.pop(), Ok(i));
    }
}

#[test]
fn drain_consumes_one_batch_and_publishes_once() {
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
fn registry_grows_past_64_anchors() {
    const ANCHORS: usize = 70;
    let (mut tx, rx0) = RingBuffer::<u64>::new(8);
    let mut anchors = vec![rx0];
    for _ in 1..ANCHORS {
        anchors.push(tx.subscribe_anchor().unwrap());
    }
    assert_eq!(tx.anchor_count(), ANCHORS);

    for i in 0..8 {
        tx.push(i);
    }
    assert_eq!(tx.try_push(8), Err(8), "all 70 anchors gate");

    // A second-chunk anchor alone holding the gate closed.
    let laggard = anchors.pop().unwrap(); // slot 69: in the appended chunk
    for rx in anchors.iter_mut() {
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
    assert_eq!(tx.anchor_count(), ANCHORS - 1);

    for rx in anchors.iter_mut() {
        assert_eq!(rx.pop(), Ok(8));
    }
}

#[test]
fn pop_ref_concurrent_shared_reads_with_observer_copy() {
    let (mut tx, mut rx1) = RingBuffer::<u64>::new(4);
    let mut rx2 = tx.subscribe_anchor().unwrap();
    let mut obs = tx.subscribe_observer();
    tx.push(42);

    let barrier = std::sync::Barrier::new(3);
    std::thread::scope(|s| {
        s.spawn(|| {
            let msg = rx1.pop_ref().unwrap();
            barrier.wait(); // both borrows plus the copy, simultaneously
            assert_eq!(*msg, 42);
            barrier.wait();
        });
        s.spawn(|| {
            let msg = rx2.pop_ref().unwrap();
            barrier.wait();
            assert_eq!(*msg, 42);
            barrier.wait();
        });
        s.spawn(|| {
            barrier.wait();
            assert_eq!(obs.pop(), Ok(42)); // validated copy of the same slot
            barrier.wait();
        });
    });

    tx.push(7);
    assert_eq!(rx1.pop(), Ok(7));
    assert_eq!(rx2.pop(), Ok(7));
}

// -----------------------------------------------------------------------------
// 10. CONSTRUCTION VALIDATION AND CUSTOM ELEMENTS
// -----------------------------------------------------------------------------

#[test]
fn capacity_floor_is_two() {
    // The audience-less gating default (own cursor minus one) could never
    // open a capacity-1 ring's gate.
    let (tx, _rx) = RingBuffer::<u64>::new(1);
    assert_eq!(tx.capacity(), 2);
    let (tx, rx) = RingBuffer::<u64>::new(3);
    assert_eq!(tx.capacity(), 4);
    assert_eq!(rx.capacity(), 4);
}

#[test]
#[should_panic(expected = "capacity must be greater than zero")]
fn zero_capacity_rejected() {
    let _ = RingBuffer::<u64>::new(0);
}

#[test]
#[should_panic(expected = "zero-sized")]
fn zst_rejected() {
    let _ = RingBuffer::<[u64; 0]>::new(8);
}

#[test]
#[should_panic(expected = "slack must be less than the capacity")]
fn slack_at_capacity_rejected() {
    let _ = RingBuffer::<u64, YieldWait, YieldWait>::with_slack(8, 8);
}

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
fn custom_no_uninit_struct_through_both_roles() {
    let (mut tx, mut rx) = RingBuffer::<Tick>::new(8);
    let mut obs = tx.subscribe_observer();
    tx.push(Tick { price: 101, qty: 7 });
    tx.push(Tick { price: 102, qty: 9 });
    assert_eq!(rx.pop(), Ok(Tick { price: 101, qty: 7 }));
    assert_eq!(rx.pop(), Ok(Tick { price: 102, qty: 9 }));
    assert_eq!(obs.pop(), Ok(Tick { price: 101, qty: 7 }));
    assert_eq!(obs.pop(), Ok(Tick { price: 102, qty: 9 }));
    assert_eq!(rx.try_pop(), Ok(None));
    assert_eq!(obs.try_pop(), Ok(None));
}
