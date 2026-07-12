//! Benchmark suite for the mixed ring (`rust_rb::anchored`): required
//! gating **anchors** + lossy spinning **observers** on one stream.
//!
//! The grid brackets the composition against its two parents:
//!
//! * A=1 K=0 — pure-gating degenerate case; compare against `bench_spmc`
//!   N=1 (the anchor machinery should price like an spmc consumer).
//! * A=0 K∈{1,4} — pure-lossy free-run; compare against `bench_broadcast`
//!   k∈{1,4} (with zero anchors the producer must free-run like broadcast).
//! * A=1 K∈{1,4} and A=2 K=2 — the mixed regimes the ring exists for.
//! * Straggling anchor (+1 observer) — the producer must track the ~50 ns
//!   rate-limited anchor (gating contract) while the observer rides along.
//! * Observer lap on a small free-run ring — exact `Lagged` accounting
//!   (accepted + missed == pushed).
//!
//! ```text
//! cargo run --release --example bench_anchored                 # default cores
//! cargo run --release --example bench_anchored 15 16 17 18 19  # producer, consumers
//! ```
//!
//! First core id is the producer; the rest serve anchors first, then
//! observers. Pin to one cluster; run twice; quote the second (warm) pass.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin, spin_delay, spin_iters_for_ns};

use rust_rb::anchored::{Observer, PopError, RingBuffer};
use rust_rb::wait::PauseWait;

const NUM_ITERATIONS: u64 = 20_000_000;
const CAPACITY: usize = 32_768;

type P = PauseWait;
type C = PauseWait;

/// Observer side: spin on `try_pop` until closed and drained; return
/// (elapsed, accepted, lag events, missed).
fn observe_all(rx: &mut Observer<u64, P, C>, delay: u32) -> (Duration, u64, u64, u64) {
    let start = Instant::now();
    let (mut accepted, mut lag_events, mut missed) = (0u64, 0u64, 0u64);
    loop {
        match rx.try_pop() {
            Ok(Some(v)) => {
                std::hint::black_box(v);
                accepted += 1;
                if delay > 0 {
                    spin_delay(delay);
                }
            }
            Ok(None) => {}
            Err(PopError::Lagged { missed: m }) => {
                lag_events += 1;
                missed += m;
            }
            Err(PopError::Closed) => break,
        }
    }
    (start.elapsed(), accepted, lag_events, missed)
}

/// One grid point: `anchor_delay_ns.len()` anchors (blocking `pop`, each
/// rate-limited by its own ns knob, 0 = flat out) plus `observers`
/// free-spinning observers. Producer ns/push measured over the push loop
/// plus the anchors' drain (the gating contract makes that the honest
/// number); observers report loss accounting.
fn run(
    name: &str,
    iters: u64,
    capacity: usize,
    anchor_delay_ns: &[u32],
    observers: usize,
    observer_delay_ns: u32,
    cores: &[usize],
) {
    let consumers = anchor_delay_ns.len() + observers;
    assert!(cores.len() > consumers, "not enough consumer cores");
    let (mut tx, first) = RingBuffer::<u64, P, C>::with_wait_strategies(capacity);
    let mut anchors = Vec::with_capacity(anchor_delay_ns.len());
    if anchor_delay_ns.is_empty() {
        drop(first);
    } else {
        anchors.push(first);
        for _ in 1..anchor_delay_ns.len() {
            anchors.push(tx.subscribe_anchor().expect("subscribe_anchor"));
        }
    }
    let obs: Vec<_> = (0..observers).map(|_| tx.subscribe_observer()).collect();

    let barrier = Arc::new(Barrier::new(consumers + 1));
    let anchor_threads: Vec<_> = anchors
        .into_iter()
        .zip(anchor_delay_ns)
        .zip(&cores[1..])
        .map(|((mut rx, &ns), &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                let delay = spin_iters_for_ns(ns);
                barrier.wait();
                let start = Instant::now();
                for _ in 0..iters {
                    std::hint::black_box(rx.pop().expect("open ring"));
                    if delay > 0 {
                        spin_delay(delay);
                    }
                }
                start.elapsed()
            })
        })
        .collect();
    let observer_threads: Vec<_> = obs
        .into_iter()
        .zip(&cores[1 + anchor_delay_ns.len()..])
        .map(|(mut rx, &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                let delay = spin_iters_for_ns(observer_delay_ns);
                barrier.wait();
                observe_all(&mut rx, delay)
            })
        })
        .collect();

    pin(cores[0]);
    barrier.wait();
    let start = Instant::now();
    for i in 0..iters {
        tx.push(i);
    }
    let anchor_stats: Vec<_> = anchor_threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect();
    let elapsed = start.elapsed();
    drop(tx); // close: observers drain what is reachable and exit
    let observer_stats: Vec<_> = observer_threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect();

    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    let mops = iters as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{name:<26} {ns_per_op:>6.2} ns/push  {mops:>6.1} M msgs/s   {:>5} ms",
        elapsed.as_millis()
    );
    for (i, e) in anchor_stats.iter().enumerate() {
        let a_ns = e.as_nanos() as f64 / iters as f64;
        println!("    anchor {i}: {a_ns:>6.2} ns/msg");
    }
    for (i, (e, accepted, lag_events, missed)) in observer_stats.iter().enumerate() {
        let o_ns = e.as_nanos() as f64 / (*accepted).max(1) as f64;
        let exact = accepted + missed == iters;
        println!(
            "    observer {i}: accepted {accepted}/{iters} ({o_ns:.2} ns/msg), \
             lagged {lag_events}x missed {missed} [{}]",
            if *missed == 0 {
                "caught up".to_string()
            } else if exact {
                "LAGGED, count exact".to_string()
            } else {
                format!("LAGGED, ACCOUNTING BROKEN: {accepted} + {missed} != {iters}")
            }
        );
    }
}

fn main() {
    announce_machine();
    let cores = core_list_announced("bench_anchored", &[15, 16, 17, 18, 19]);
    assert!(
        cores.len() >= 5,
        "bench_anchored needs a producer core and four consumer cores"
    );

    const D50_NS: u32 = 50;

    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        // 1. Degenerate parities: pure gating (vs bench_spmc N=1) and pure
        //    lossy free-run (vs bench_broadcast k∈{1,4}).
        run("ANCH A=1 K=0", NUM_ITERATIONS, CAPACITY, &[0], 0, 0, &cores);
        run("ANCH A=0 K=1", NUM_ITERATIONS, CAPACITY, &[], 1, 0, &cores);
        run("ANCH A=0 K=4", NUM_ITERATIONS, CAPACITY, &[], 4, 0, &cores);
        // 2. The mixed regimes.
        run("ANCH A=1 K=1", NUM_ITERATIONS, CAPACITY, &[0], 1, 0, &cores);
        run("ANCH A=1 K=3", NUM_ITERATIONS, CAPACITY, &[0], 3, 0, &cores);
        run(
            "ANCH A=2 K=2",
            NUM_ITERATIONS,
            CAPACITY,
            &[0, 0],
            2,
            0,
            &cores,
        );
        // 3. Straggling anchor + observer: the producer must track the
        //    rate-limited anchor; the observer stays caught up for free.
        run(
            "ANCH_straggler A=1 K=1",
            NUM_ITERATIONS / 4,
            CAPACITY,
            &[D50_NS],
            1,
            0,
            &cores,
        );
        // 4. Observer lap on a small free-run ring: exact loss accounting.
        run(
            "ANCH_lap A=0 K=1 cap=64",
            NUM_ITERATIONS,
            64,
            &[],
            1,
            200,
            &cores,
        );
    }
}
