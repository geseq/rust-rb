//! Benchmark suite for the lossy broadcast ring (`rust_rb::broadcast`).
//!
//! Covers the core of the broadcast bench plan (docs/design/spmc.md §5):
//! producer-throughput independence vs k pinned spinning consumers (the
//! tail-spin design's gate), the strict-vs-volatile copy A/B at payload
//! sizes {8, 64, 256} B, and lap behavior with a deliberately slow consumer
//! on a tiny ring.
//!
//! ```text
//! cargo run --release --example bench_broadcast                 # default cores
//! cargo run --release --example bench_broadcast 15 16 17 18 19  # producer, consumers
//! RUSTFLAGS="--cfg rust_rb_volatile_copy" \
//!     cargo run --release --example bench_broadcast             # volatile A/B side
//! ```
//!
//! The first core id is the producer; the rest are consumer cores (a
//! k-consumer run uses the first k of them). Run twice per invocation;
//! quote the second (warm) pass.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin, spin_delay, spin_ns_per_iter};

use rust_rb::broadcast::{Consumer, NoUninit, PopError, RingBuffer};
use rust_rb::wait::NoOpWait;

const NUM_ITERATIONS: u64 = 100_000_000;
const CAPACITY: usize = 32_768;

/// Spin on `try_pop` until the ring is closed and drained, optionally
/// rate-limited, and return (elapsed, accepted, lag events, total missed).
/// A tight `try_pop` loop is the maximal-pressure caught-up spinner the
/// tail-spin protocol is designed for.
fn consume_all<T: NoUninit>(
    rx: &mut Consumer<T, NoOpWait>,
    delay: u32,
) -> (Duration, u64, u64, u64) {
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

fn print_consumer_stats(stats: &[(Duration, u64, u64, u64)], pushed: u64) {
    for (i, (e, accepted, lag_events, missed)) in stats.iter().enumerate() {
        let c_ns = e.as_nanos() as f64 / (*accepted).max(1) as f64;
        let caught_up = if *missed == 0 { "caught up" } else { "LAGGED" };
        println!(
            "    consumer {i}: accepted {accepted}/{pushed} ({c_ns:.2} ns/msg), \
             lagged {lag_events}x missed {missed} [{caught_up}]"
        );
    }
}

/// Bench 5/7 driver: push `iters` items with `delays.len()` pinned spinning
/// consumers attached (one rate-limit knob each) and report the producer's
/// push throughput — measured over the push loop alone, since a lossy
/// producer never waits for consumers — plus per-consumer loss accounting.
fn run_broadcast(name: &str, iters: u64, capacity: usize, delays: &[u32], cores: &[usize]) {
    assert!(cores.len() > delays.len(), "not enough consumer cores");
    let (mut tx, rx) = RingBuffer::<i64, NoOpWait>::with_wait_strategies(capacity);
    let mut consumers = Vec::with_capacity(delays.len());
    if delays.is_empty() {
        drop(rx);
    } else {
        consumers.push(rx);
        for _ in 1..delays.len() {
            consumers.push(tx.subscribe::<NoOpWait>());
        }
    }

    let barrier = Arc::new(Barrier::new(delays.len() + 1));
    let threads: Vec<_> = consumers
        .into_iter()
        .zip(delays)
        .zip(&cores[1..])
        .map(|((mut rx, &delay), &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                barrier.wait();
                consume_all(&mut rx, delay)
            })
        })
        .collect();

    pin(cores[0]);
    barrier.wait();
    let start = Instant::now();
    for i in 0..iters {
        tx.push(i as i64);
    }
    let elapsed = start.elapsed();
    drop(tx); // close: consumers drain the remaining published slots and exit
    let stats: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    let mops = iters as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{name:<22} {ns_per_op:>6.2} ns/push  {mops:>6.1} M msgs/s   {:>5} ms",
        elapsed.as_millis()
    );
    print_consumer_stats(&stats, iters);
}

/// Copy A/B, concurrent side: one pinned pair streaming `[u8; L]` payloads;
/// producer push ns measured over the push loop, consumer copy-out cost as
/// its own elapsed over accepted messages.
fn run_copy_stream<const L: usize>(iters: u64, capacity: usize, cores: &[usize]) {
    let (mut tx, mut rx) = RingBuffer::<[u8; L], NoOpWait>::with_wait_strategies(capacity);
    let consumer_core = cores[1];

    let barrier = Arc::new(Barrier::new(2));
    let consumer = {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            pin(consumer_core);
            barrier.wait();
            consume_all(&mut rx, 0)
        })
    };

    pin(cores[0]);
    let payload = [0xa5u8; L];
    barrier.wait();
    let start = Instant::now();
    for _ in 0..iters {
        tx.push(payload);
    }
    let elapsed = start.elapsed();
    drop(tx);
    let stats = consumer.join().unwrap();

    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    // Multiply in f64: `iters as usize * L` truncates the u64 count and can
    // overflow on 32-bit targets (bytes = iters * L easily exceeds 2^32).
    let gb_per_s = iters as f64 * L as f64 / elapsed.as_nanos() as f64;
    println!(
        "BCAST_stream {L:>3}B      {ns_per_op:>6.2} ns/push  {gb_per_s:>6.2} GB/s   {:>5} ms",
        elapsed.as_millis()
    );
    print_consumer_stats(&[stats], iters);
}

/// Copy A/B, isolated side: producer and consumer handles on ONE pinned
/// thread, alternating a full-capacity push window with a full-capacity pop
/// window. No cross-core traffic — the per-message numbers isolate the copy
/// codegen (word-wise atomic vs volatile), which is what the ADR 0002
/// decision rule compares.
fn run_copy_pingpong<const L: usize>(rounds: usize, capacity: usize, cores: &[usize]) {
    pin(cores[0]);
    let (mut tx, mut rx) = RingBuffer::<[u8; L], NoOpWait>::with_wait_strategies(capacity);
    let payload = [0xa5u8; L];
    let (mut push_ns, mut pop_ns) = (0u128, 0u128);
    let mut sink = 0u64;
    for _ in 0..rounds {
        let t = Instant::now();
        for _ in 0..capacity {
            tx.push(payload);
        }
        push_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        for _ in 0..capacity {
            match rx.try_pop() {
                Ok(Some(v)) => sink = sink.wrapping_add(v[0] as u64),
                other => panic!("ping-pong consumer fell behind: {other:?}"),
            }
        }
        pop_ns += t.elapsed().as_nanos();
    }
    std::hint::black_box(sink);
    // Multiply in f64 (message count can exceed a 32-bit usize).
    let msgs = rounds as f64 * capacity as f64;
    let pop_gb_per_s = msgs * L as f64 / pop_ns as f64;
    println!(
        "BCAST_pingpong {L:>3}B    push {:>6.2} ns  pop {:>6.2} ns  (pop copy {pop_gb_per_s:>6.2} GB/s)",
        push_ns as f64 / msgs,
        pop_ns as f64 / msgs,
    );
}

fn main() {
    announce_machine();
    println!(
        "copy mode: {}",
        if cfg!(rust_rb_volatile_copy) {
            "VOLATILE (--cfg rust_rb_volatile_copy)"
        } else {
            "strict word-wise atomic (default)"
        }
    );
    let cores = core_list_announced("bench_broadcast", &[15, 16, 17, 18, 19]);
    assert!(
        cores.len() >= 5,
        "bench_broadcast needs a producer core and four consumer cores"
    );

    // Calibrate the rate-limit knob on the (pinned) producer core.
    pin(cores[0]);
    let spin_ns = spin_ns_per_iter();
    let d200 = ((200.0 / spin_ns) as u32).max(1);
    println!("spin_loop hint: {spin_ns:.2} ns/iter; ~200 ns rate limit = {d200} iters");

    // Run twice, as the other benches do, to let caches/governors settle;
    // quote the second pass.
    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        // 5. Producer-throughput independence vs k caught-up spinning
        //    consumers — the tail-spin design's gate: flat-ish vs k.
        run_broadcast("BCAST_i64 k=0", NUM_ITERATIONS, CAPACITY, &[], &cores);
        run_broadcast("BCAST_i64 k=1", NUM_ITERATIONS, CAPACITY, &[0; 1], &cores);
        run_broadcast("BCAST_i64 k=2", NUM_ITERATIONS, CAPACITY, &[0; 2], &cores);
        run_broadcast("BCAST_i64 k=4", NUM_ITERATIONS, CAPACITY, &[0; 4], &cores);
        // 6. Copy A/B (this build's side; rebuild with the volatile cfg for
        //    the other side).
        run_copy_stream::<8>(20_000_000, CAPACITY, &cores);
        run_copy_stream::<64>(20_000_000, CAPACITY, &cores);
        run_copy_stream::<256>(20_000_000, CAPACITY, &cores);
        run_copy_pingpong::<8>(4096, 4096, &cores);
        run_copy_pingpong::<64>(4096, 4096, &cores);
        run_copy_pingpong::<256>(4096, 4096, &cores);
        // 7. Lap behavior: tiny ring, one deliberately slow consumer — the
        //    producer should be unaffected; loss accounting must be exact
        //    (accepted + missed == pushed).
        run_broadcast("BCAST_lap cap=64", 20_000_000, 64, &[d200], &cores);
    }
}
