//! Benchmark suite for the lossy broadcast **byte** ring
//! (`rust_rb::broadcast_bytes`) — the Agrona-framing counterpart of
//! `bench_broadcast`.
//!
//! Grid: producer-throughput independence vs k pinned spinning consumers at
//! 64 B (k∈{0,1,2,4}), a message-size sweep {8, 64, 256} B at k=1, and lap
//! behavior on a small ring with one deliberately slow consumer — loss
//! accounting must be exact in **bytes** (accepted + missed == pushed).
//!
//! ```text
//! cargo run --release --example bench_broadcast_bytes                 # default cores
//! cargo run --release --example bench_broadcast_bytes 15 16 17 18 19  # producer, consumers
//! ```
//!
//! First core id is the producer; the rest are consumer cores. Pin to one
//! cluster; run twice; quote the second (warm) pass.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin, spin_delay, spin_iters_for_ns};

use rust_rb::broadcast_bytes::{BytesConsumer, BytesRingBuffer, PopError};
use rust_rb::wait::NoOpWait;

const NUM_MESSAGES: u64 = 20_000_000;
const CAPACITY: usize = 64 * 1024;

/// Spin on `try_pop_into` until closed and drained; return (elapsed,
/// accepted msgs, accepted bytes, lag events, missed bytes).
fn consume_all(
    rx: &mut BytesConsumer<NoOpWait>,
    delay: u32,
) -> (Duration, u64, u64, u64, u64) {
    let mut buf = Vec::with_capacity(4096);
    let start = Instant::now();
    let (mut accepted, mut bytes, mut lag_events, mut missed) = (0u64, 0u64, 0u64, 0u64);
    loop {
        match rx.try_pop_into(&mut buf) {
            Ok(true) => {
                std::hint::black_box(buf.as_slice());
                accepted += 1;
                bytes += buf.len() as u64;
                if delay > 0 {
                    spin_delay(delay);
                }
            }
            Ok(false) => {}
            Err(PopError::Lagged { missed_bytes: m }) => {
                lag_events += 1;
                missed += m;
            }
            Err(PopError::Closed) => break,
        }
    }
    (start.elapsed(), accepted, bytes, lag_events, missed)
}

/// Push `iters` messages of `msg_len` bytes with one pinned spinning
/// consumer per `delay_ns` entry; report producer push throughput (push
/// loop alone — a lossy producer never waits) plus per-consumer byte-exact
/// loss accounting.
fn run(name: &str, iters: u64, msg_len: usize, capacity: usize, delay_ns: &[u32], cores: &[usize]) {
    assert!(cores.len() > delay_ns.len(), "not enough consumer cores");
    let (mut tx, rx) = BytesRingBuffer::<NoOpWait>::with_wait_strategies(capacity);
    let mut consumers = Vec::with_capacity(delay_ns.len());
    if delay_ns.is_empty() {
        drop(rx);
    } else {
        consumers.push(rx);
        for _ in 1..delay_ns.len() {
            consumers.push(tx.subscribe::<NoOpWait>());
        }
    }

    let barrier = Arc::new(Barrier::new(delay_ns.len() + 1));
    let threads: Vec<_> = consumers
        .into_iter()
        .zip(delay_ns)
        .zip(&cores[1..])
        .map(|((mut rx, &ns), &core)| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                let delay = spin_iters_for_ns(ns);
                barrier.wait();
                consume_all(&mut rx, delay)
            })
        })
        .collect();

    pin(cores[0]);
    let msg = vec![0xa5u8; msg_len];
    barrier.wait();
    let start = Instant::now();
    for _ in 0..iters {
        tx.push(&msg);
    }
    let elapsed = start.elapsed();
    drop(tx); // close: consumers drain what is still reachable and exit
    let stats: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

    let ns_per_msg = elapsed.as_nanos() as f64 / iters as f64;
    let gb_per_s = iters as f64 * msg_len as f64 / elapsed.as_nanos() as f64;
    println!(
        "{name:<26} {msg_len:>4} B  {ns_per_msg:>6.2} ns/push  {gb_per_s:>6.2} GB/s   {:>5} ms",
        elapsed.as_millis()
    );
    for (i, (e, accepted, bytes, lag_events, missed)) in stats.iter().enumerate() {
        let c_ns = e.as_nanos() as f64 / (*accepted).max(1) as f64;
        // `missed` is FRAMED bytes (header + padding + payload) while
        // `bytes` is payload — not directly summable; the byte-exactness
        // contract itself is covered by tests/broadcast_bytes.rs.
        println!(
            "    consumer {i}: accepted {accepted} msgs / {bytes} payload B \
             ({c_ns:.2} ns/msg), lagged {lag_events}x missed {missed} framed B [{}]",
            if *missed == 0 { "caught up" } else { "LAGGED" }
        );
    }
}

fn main() {
    announce_machine();
    let cores = core_list_announced("bench_broadcast_bytes", &[15, 16, 17, 18, 19]);
    assert!(
        cores.len() >= 5,
        "bench_broadcast_bytes needs a producer core and four consumer cores"
    );

    const D200_NS: u32 = 200;
    pin(cores[0]);

    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        // 1. Producer independence vs k caught-up spinning consumers, 64 B.
        run("BCASTB k=0", NUM_MESSAGES, 64, CAPACITY, &[], &cores);
        run("BCASTB k=1", NUM_MESSAGES, 64, CAPACITY, &[0; 1], &cores);
        run("BCASTB k=2", NUM_MESSAGES, 64, CAPACITY, &[0; 2], &cores);
        run("BCASTB k=4", NUM_MESSAGES, 64, CAPACITY, &[0; 4], &cores);
        // 2. Message-size sweep at k=1 (framing + copy cost per size).
        run("BCASTB size", NUM_MESSAGES, 8, CAPACITY, &[0; 1], &cores);
        run("BCASTB size", NUM_MESSAGES, 256, CAPACITY, &[0; 1], &cores);
        // 3. Lap behavior: small ring, one ~200 ns rate-limited consumer —
        //    producer unaffected, byte accounting exact. (Ring must hold
        //    the 64 B messages: broadcast-bytes max_message_len is
        //    capacity/8, so 4 KiB comfortably frames 64 B.)
        run("BCASTB_lap cap=4K", NUM_MESSAGES, 64, 4096, &[D200_NS], &cores);
    }
}
