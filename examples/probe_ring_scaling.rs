//! Ring-level attribution probe for the two open scaling findings
//! (`rust-rb-vio`: gating N-scaling, `rust-rb-6l0`: lossy k-coupling).
//!
//! `probe_coherence` establishes this box's synthetic floor: a publish line
//! with k spinning readers costs the writer almost nothing (~1–2 ns), but a
//! *second* writer-hot line that readers touch per observation collapses the
//! writer 100×. The ring analogue of that second line is the **slot line**:
//! both rings pack several slots per cache line (8 × `i64` for the gating
//! buffer, 4 × 16-byte seqlock slots for the lossy one), so a caught-up
//! consumer copying message `s` holds the very line the producer writes at
//! `s+1`.
//!
//! This probe runs the same caught-up scaling grids as the bench suites,
//! twice each: once with `i64` (slots share lines — the shipped layout) and
//! once with a 64-byte line-aligned element (every slot alone on its
//! line(s) — adjacent-slot sharing gone). If the padded curve flattens, the
//! finding is attributed to adjacent-slot false sharing; whatever residue
//! remains is the per-message ping-pong of the slot being read itself plus
//! the cursor/tail line.
//!
//! ```text
//! cargo run --release --example probe_ring_scaling                # default cores
//! cargo run --release --example probe_ring_scaling 15 5 6 7 8 9 16 17 18
//! ```
//!
//! First core id is the producer; the rest are consumer cores (an N/k run
//! uses the first N). Pin to ONE cluster; run twice; quote the second pass.

use std::sync::{Arc, Barrier};
use std::time::Instant;

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin};

use rust_rb::broadcast::{self, NoUninit, PopError};
use rust_rb::spmc;
use rust_rb::wait::{NoOpWait, YieldWait};

const ITERS: u64 = 50_000_000;
const CAPACITY: usize = 32_768;

/// 64 bytes of real data, alone on its cache line: no padding bytes (the
/// array fills the type exactly), so the `NoUninit` contract holds.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Padded([u64; 8]);

// SAFETY: `[u64; 8]` under `repr(C, align(64))` is size 64 == content size —
// every byte of the value representation is initialized, no niches.
unsafe impl NoUninit for Padded {}

trait Payload: Copy + Send + Sync + 'static {
    fn make(i: u64) -> Self;
}
impl Payload for i64 {
    fn make(i: u64) -> Self {
        i as i64
    }
}
impl Payload for Padded {
    fn make(i: u64) -> Self {
        Padded([i; 8])
    }
}

/// Gating grid point: N caught-up consumers (blocking `pop`, Yield — the
/// strategy `rust-rb-vio`'s numbers were taken with), producer ns/push.
fn run_spmc<T: Payload>(name: &str, n: usize, cores: &[usize]) {
    assert!(cores.len() > n, "not enough consumer cores");
    let (mut tx, rx) = spmc::RingBuffer::<T, YieldWait, YieldWait>::with_wait_strategies(CAPACITY);
    let mut consumers = vec![rx];
    for _ in 1..n {
        consumers.push(tx.subscribe().expect("subscribe"));
    }

    let barrier = Arc::new(Barrier::new(n + 1));
    let threads: Vec<_> = consumers
        .into_iter()
        .zip(&cores[1..])
        .map(|(mut rx, &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                barrier.wait();
                for _ in 0..ITERS {
                    std::hint::black_box(rx.pop().unwrap());
                }
            })
        })
        .collect();

    pin(cores[0]);
    barrier.wait();
    let start = Instant::now();
    for i in 0..ITERS {
        tx.push(T::make(i));
    }
    for t in threads {
        t.join().unwrap();
    }
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!("{name:<22} N={n}   {ns:>6.2} ns/push");
}

/// Lossy grid point: k caught-up spinning consumers (tight `try_pop`, the
/// maximal-pressure regime `rust-rb-6l0` measured), producer ns/push over
/// the push loop alone.
fn run_bcast<T: Payload + NoUninit>(name: &str, k: usize, cores: &[usize]) {
    assert!(cores.len() > k, "not enough consumer cores");
    let (mut tx, rx) = broadcast::RingBuffer::<T, NoOpWait>::with_wait_strategies(CAPACITY);
    let mut consumers = Vec::new();
    if k == 0 {
        drop(rx);
    } else {
        consumers.push(rx);
        for _ in 1..k {
            consumers.push(tx.subscribe::<NoOpWait>());
        }
    }

    let barrier = Arc::new(Barrier::new(k + 1));
    let threads: Vec<_> = consumers
        .into_iter()
        .zip(&cores[1..])
        .map(|(mut rx, &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                barrier.wait();
                let mut sink = 0u64;
                loop {
                    match rx.try_pop() {
                        Ok(Some(v)) => {
                            std::hint::black_box(v);
                            sink += 1;
                        }
                        Ok(None) => {}
                        Err(PopError::Lagged { .. }) => {}
                        Err(PopError::Closed) => break,
                    }
                }
                sink
            })
        })
        .collect();

    pin(cores[0]);
    barrier.wait();
    let start = Instant::now();
    for i in 0..ITERS {
        tx.push(T::make(i));
    }
    let elapsed = start.elapsed();
    drop(tx);
    for t in threads {
        t.join().unwrap();
    }
    let ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!("{name:<22} k={k}   {ns:>6.2} ns/push");
}

fn main() {
    announce_machine();
    let cores = core_list_announced("probe_ring_scaling", &[15, 5, 6, 7, 8, 9, 16, 17, 18]);
    assert!(
        cores.len() >= 9,
        "probe_ring_scaling wants a producer core and eight consumer cores"
    );

    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        for &n in &[1usize, 2, 4, 8] {
            run_spmc::<i64>("SPMC_i64 (shared)", n, &cores);
        }
        for &n in &[1usize, 2, 4, 8] {
            run_spmc::<Padded>("SPMC_pad64 (isolated)", n, &cores);
        }
        for &k in &[0usize, 1, 2, 4, 8] {
            run_bcast::<i64>("BCAST_i64 (shared)", k, &cores);
        }
        for &k in &[0usize, 1, 2, 4, 8] {
            run_bcast::<Padded>("BCAST_pad64 (isolated)", k, &cores);
        }
    }
}
