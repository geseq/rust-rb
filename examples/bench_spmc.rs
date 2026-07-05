//! Benchmark suite for the gating multicast ring (`rust_rb::spmc`).
//!
//! Covers the core of the SPMC bench plan (docs/design/spmc.md §5):
//! SPSC-parity at N=1, consumer-count scaling at N∈{2,4}, sustained
//! backpressure push-latency percentiles on a small ring, and the straggler
//! regime (two fast consumers + one rate-limited).
//!
//! ```text
//! cargo run --release --example bench_spmc                 # default cores
//! cargo run --release --example bench_spmc 15 16 17 18 19  # producer, consumers
//! ```
//!
//! The first core id is the producer; the rest are consumer cores (an
//! N-consumer run uses the first N of them). Latency is dominated by the
//! core-to-core topology, so pin to dedicated cores of one cluster for
//! meaningful numbers. Run twice per invocation; quote the second (warm)
//! pass.

use std::time::Instant;

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin, spin_delay, spin_ns_per_iter};

use rust_rb::spmc::RingBuffer;
use rust_rb::wait::{NoOpWait, PauseWait, SelfTimed, YieldWait};

const NUM_ITERATIONS: i64 = 100_000_000;
const CAPACITY: usize = 32_768;

/// Push `iters` items through a ring with one consumer per `delays` entry
/// (0 = flat out, n = spin-delay iterations per pop) and report the
/// producer's end-to-end throughput plus each consumer's own throughput.
///
/// Every consumer observes every message (multicast), so each pops `iters`
/// items. Consumers time themselves from their first pop to strip
/// startup skew.
fn run<P, C>(name: &str, iters: i64, capacity: usize, delays: &[u32], cores: &[usize])
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    assert!(cores.len() > delays.len(), "not enough consumer cores");
    let (mut tx, rx) = RingBuffer::<i64, P, C>::with_wait_strategies(capacity);
    let mut consumers = Vec::with_capacity(delays.len());
    consumers.push(rx);
    for _ in 1..delays.len() {
        consumers.push(tx.subscribe().expect("subscribe"));
    }

    let threads: Vec<_> = consumers
        .into_iter()
        .zip(delays)
        .zip(&cores[1..])
        .map(|((mut rx, &delay), &core)| {
            std::thread::spawn(move || {
                pin(core);
                let _ = rx.pop().unwrap();
                let start = Instant::now();
                for _ in 1..iters {
                    let _ = rx.pop().unwrap();
                    if delay > 0 {
                        spin_delay(delay);
                    }
                }
                start.elapsed()
            })
        })
        .collect();

    pin(cores[0]);
    let start = Instant::now();
    for i in 0..iters {
        tx.push(i);
    }
    let per_consumer: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let elapsed = start.elapsed();

    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    let mops = iters as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{name:<22} {ns_per_op:>6.2} ns/op   {mops:>6.1} M msgs/s   {:>5} ms",
        elapsed.as_millis()
    );
    for (i, e) in per_consumer.iter().enumerate() {
        let c_ns = e.as_nanos() as f64 / (iters - 1) as f64;
        let c_mops = (iters - 1) as f64 / e.as_secs_f64() / 1e6;
        println!("    consumer {i}: {c_ns:>6.2} ns/op   {c_mops:>6.1} M msgs/s");
    }
}

/// Sustained-gating push latency: a small ring plus a rate-limited consumer
/// keep the producer permanently gated; every push is timed individually
/// into a pre-allocated buffer and the distribution reported. `Instant` on
/// aarch64 Linux reads the generic timer (a few ns per read), so per-op
/// sampling is honest at this rate — but it does add ~2 timer reads of
/// overhead per push, so the run's *throughput* is not quotable.
fn run_backpressure<P, C>(name: &str, iters: usize, capacity: usize, delay: u32, cores: &[usize])
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    let (mut tx, mut rx) = RingBuffer::<i64, P, C>::with_wait_strategies(capacity);
    let consumer_core = cores[1];

    // Latency percentiles (the max especially) must not be poisoned by the
    // consumer thread's spawn/pin time, so wait until it is running.
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let consumer = {
        let barrier = std::sync::Arc::clone(&barrier);
        std::thread::spawn(move || {
            pin(consumer_core);
            barrier.wait();
            let start = Instant::now();
            for _ in 0..iters {
                let _ = rx.pop().unwrap();
                spin_delay(delay);
            }
            start.elapsed()
        })
    };

    pin(cores[0]);
    barrier.wait();
    let mut samples: Vec<u64> = Vec::with_capacity(iters);
    for i in 0..iters {
        let t = Instant::now();
        tx.push(i as i64);
        samples.push(t.elapsed().as_nanos() as u64);
    }
    let consumer_elapsed = consumer.join().unwrap();

    samples.sort_unstable();
    let pct = |p: f64| samples[((samples.len() - 1) as f64 * p) as usize];
    let mean = samples.iter().sum::<u64>() as f64 / samples.len() as f64;
    println!(
        "{name:<22} push ns: p50 {} p90 {} p99 {} p99.9 {} max {} (mean {mean:.1})",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        samples[samples.len() - 1],
    );
    let c_ns = consumer_elapsed.as_nanos() as f64 / iters as f64;
    println!("    rate-limited consumer: {c_ns:.2} ns/op");
}

fn main() {
    announce_machine();
    let cores = core_list_announced("bench_spmc", &[15, 16, 17, 18, 19]);
    assert!(
        cores.len() >= 5,
        "bench_spmc needs a producer core and four consumer cores"
    );

    // Calibrate the rate-limit knob on the (pinned) producer core.
    pin(cores[0]);
    let spin_ns = spin_ns_per_iter();
    let d50 = ((50.0 / spin_ns) as u32).max(1);
    println!("spin_loop hint: {spin_ns:.2} ns/iter; ~50 ns rate limit = {d50} iters");

    // Run twice, as the other benches do, to let caches/governors settle;
    // quote the second pass.
    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        // 1. SPSC-parity, N=1 (compare against `bench` on the same pair).
        run::<PauseWait, PauseWait>("SPMC_Pause N=1", NUM_ITERATIONS, CAPACITY, &[0], &cores);
        run::<YieldWait, YieldWait>("SPMC_Yield N=1", NUM_ITERATIONS, CAPACITY, &[0], &cores);
        run::<NoOpWait, NoOpWait>("SPMC_NoOp N=1", NUM_ITERATIONS, CAPACITY, &[0], &cores);
        // 2. N-scaling with all consumers keeping up.
        run::<YieldWait, YieldWait>("SPMC_Yield N=2", NUM_ITERATIONS, CAPACITY, &[0; 2], &cores);
        run::<YieldWait, YieldWait>("SPMC_Yield N=4", NUM_ITERATIONS, CAPACITY, &[0; 4], &cores);
        run::<PauseWait, PauseWait>("SPMC_Pause N=2", NUM_ITERATIONS, CAPACITY, &[0; 2], &cores);
        run::<PauseWait, PauseWait>("SPMC_Pause N=4", NUM_ITERATIONS, CAPACITY, &[0; 4], &cores);
        // 3. Backpressure latency percentiles on a small, permanently-gated
        //    ring.
        run_backpressure::<PauseWait, PauseWait>(
            "SPMC_gated cap=1024",
            2_000_000,
            1024,
            d50,
            &cores,
        );
        // 4. Straggler: two fast consumers + one rate-limited; the producer
        //    should track the straggler (the gating contract), not collapse
        //    below it.
        run::<PauseWait, PauseWait>(
            "SPMC_straggler N=3",
            20_000_000,
            CAPACITY,
            &[0, 0, d50],
            &cores,
        );
    }
}
