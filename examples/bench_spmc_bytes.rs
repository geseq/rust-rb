//! Benchmark suite for the gating multicast **byte** ring
//! (`rust_rb::spmc_bytes`) — the byte-framing counterpart of `bench_spmc`.
//!
//! Grid: SPSC-bytes parity at N=1 across message sizes {8, 64, 256} B (pop
//! and drain paths — compare against `bench_bytes` on the same pair),
//! consumer-count scaling at N∈{2,4} at 64 B, and the straggler regime (two
//! fast consumers + one ~50 ns rate-limited one).
//!
//! ```text
//! cargo run --release --example bench_spmc_bytes                 # default cores
//! cargo run --release --example bench_spmc_bytes 15 16 17 18 19  # producer, consumers
//! ```
//!
//! The first core id is the producer; the rest are consumer cores (an
//! N-consumer run uses the first N). Pin to one cluster; run twice; quote
//! the second (warm) pass.

use std::time::Instant;

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin, spin_delay, spin_iters_for_ns};

use rust_rb::spmc_bytes::BytesRingBuffer;
use rust_rb::wait::{PauseWait, SelfTimed, YieldWait};

const NUM_MESSAGES: usize = 20_000_000;
const CAPACITY: usize = 64 * 1024;

/// Push `NUM_MESSAGES` `msg_len`-byte messages through a ring with one
/// consumer per `delay_ns` entry (0 = flat out; n = a ~n ns spin per
/// message, calibrated on that consumer's own core). Every consumer sees
/// every message. `drain` selects the batched path (rate-limited consumers
/// always use `pop` so the limit applies per message).
fn run<P, C>(name: &str, msg_len: usize, drain: bool, delay_ns: &[u32], cores: &[usize])
where
    P: SelfTimed + Send + Sync + 'static,
    C: SelfTimed + Send + Sync + 'static,
{
    assert!(cores.len() > delay_ns.len(), "not enough consumer cores");
    let (mut tx, rx) = BytesRingBuffer::<P, C>::with_wait_strategies(CAPACITY);
    let mut consumers = Vec::with_capacity(delay_ns.len());
    consumers.push(rx);
    for _ in 1..delay_ns.len() {
        consumers.push(tx.subscribe().expect("subscribe"));
    }

    let threads: Vec<_> = consumers
        .into_iter()
        .zip(delay_ns)
        .zip(&cores[1..])
        .map(|((mut rx, &ns), &core)| {
            std::thread::spawn(move || {
                pin(core);
                let delay = spin_iters_for_ns(ns);
                let mut consumed = 0usize;
                let mut bytes = 0usize;
                // First message outside the timed window (startup skew).
                bytes += rx.pop().expect("open ring").len();
                consumed += 1;
                let start = Instant::now();
                if drain && delay == 0 {
                    while consumed < NUM_MESSAGES {
                        consumed += rx.drain(|msg| bytes += msg.len());
                    }
                } else {
                    while consumed < NUM_MESSAGES {
                        bytes += rx.pop().expect("open ring").len();
                        consumed += 1;
                        if delay > 0 {
                            spin_delay(delay);
                        }
                    }
                }
                let elapsed = start.elapsed();
                assert_eq!(bytes, NUM_MESSAGES * msg_len, "multicast delivery");
                elapsed
            })
        })
        .collect();

    pin(cores[0]);
    let msg = vec![0xa5u8; msg_len];
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        tx.push(&msg);
    }
    let per_consumer: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let elapsed = start.elapsed();

    let ns_per_msg = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    let gb_per_s = (NUM_MESSAGES * msg_len) as f64 / elapsed.as_nanos() as f64;
    println!(
        "{name:<26} {msg_len:>4} B  {ns_per_msg:>6.2} ns/msg  {gb_per_s:>6.2} GB/s   {:>5} ms",
        elapsed.as_millis()
    );
    for (i, e) in per_consumer.iter().enumerate() {
        let c_ns = e.as_nanos() as f64 / (NUM_MESSAGES - 1) as f64;
        println!("    consumer {i}: {c_ns:>6.2} ns/msg");
    }
}

fn main() {
    announce_machine();
    let cores = core_list_announced("bench_spmc_bytes", &[15, 16, 17, 18, 19]);
    assert!(
        cores.len() >= 4,
        "bench_spmc_bytes needs a producer core and three consumer cores"
    );

    const D50_NS: u32 = 50;

    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        // 1. SPSC-bytes parity at N=1 (compare against `bench_bytes`).
        for &len in &[8usize, 64, 256] {
            run::<PauseWait, PauseWait>("SPMCB_Pause_pop N=1", len, false, &[0], &cores);
            run::<PauseWait, PauseWait>("SPMCB_Pause_drain N=1", len, true, &[0], &cores);
        }
        // 2. N-scaling with all consumers keeping up, 64 B messages.
        run::<PauseWait, PauseWait>("SPMCB_Pause_pop N=2", 64, false, &[0; 2], &cores);
        run::<YieldWait, YieldWait>("SPMCB_Yield_pop N=2", 64, false, &[0; 2], &cores);
        if cores.len() > 4 {
            run::<PauseWait, PauseWait>("SPMCB_Pause_pop N=4", 64, false, &[0; 4], &cores);
            run::<YieldWait, YieldWait>("SPMCB_Yield_pop N=4", 64, false, &[0; 4], &cores);
        }
        // 3. Straggler: two fast + one ~50 ns rate-limited consumer; the
        //    producer should track the straggler, not collapse below it.
        run::<PauseWait, PauseWait>("SPMCB_straggler N=3", 64, false, &[0, 0, D50_NS], &cores);
    }
}
