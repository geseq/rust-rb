//! Throughput benchmark, in the spirit of `bench/simple_bench.cpp`.
//!
//! Spawns a producer and a consumer on separate threads, pushes
//! `NUM_ITERATIONS` items through, and reports nanoseconds per operation.
//!
//! ```text
//! cargo run --release --example bench
//! ```
//!
//! For stable numbers pin the threads to dedicated cores, e.g.
//! `taskset -c 2,3 cargo run --release --example bench`.

use std::time::Instant;

use rust_rb::spsc::Spsc;
use rust_rb::wait::{NoOpWait, PauseWait, WaitStrategy, YieldWait};

const NUM_ITERATIONS: i64 = 100_000_000;
const CAPACITY: usize = 32_768;

fn run<P, G>(name: &str)
where
    P: WaitStrategy + Send + Sync + 'static,
    G: WaitStrategy + Send + Sync + 'static,
{
    let (mut tx, mut rx) = Spsc::<i64, CAPACITY, P, G>::new();

    let consumer = std::thread::spawn(move || {
        let mut consumed: i64 = 0;
        while consumed < NUM_ITERATIONS {
            let _ = rx.get();
            consumed += 1;
        }
    });

    let start = Instant::now();
    for i in 0..NUM_ITERATIONS {
        tx.put(i);
    }
    consumer.join().unwrap();
    let elapsed = start.elapsed();

    let ns_per_op = elapsed.as_nanos() as f64 / NUM_ITERATIONS as f64;
    println!(
        "{name:<18} {NUM_ITERATIONS} ops  {ns_per_op:>5.2} ns/op  {:>5} ms",
        elapsed.as_millis()
    );
}

fn main() {
    // Run twice, as the C++ benchmark does, to let caches/governors settle.
    for _ in 0..2 {
        run::<PauseWait, PauseWait>("SPSC_Pause");
        run::<YieldWait, YieldWait>("SPSC_Yield");
        run::<NoOpWait, NoOpWait>("SPSC_NoOp");
    }
}
