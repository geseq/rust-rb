//! Throughput benchmark, in the spirit of `bench/simple_bench.cpp`.
//!
//! Spawns a producer and a consumer on separate threads, pushes
//! `NUM_ITERATIONS` items through, and reports nanoseconds per operation.
//!
//! ```text
//! cargo run --release --example bench            # unpinned
//! cargo run --release --example bench 18 19      # pin producer->18, consumer->19
//! ```
//!
//! Latency is dominated by the core-to-core topology of the producer/consumer
//! pair, so for meaningful numbers pin both threads to dedicated cores.

use std::time::Instant;

use rust_rb::spsc::RingBuffer;
use rust_rb::wait::{NoOpWait, PauseWait, WaitStrategy, YieldWait};

const NUM_ITERATIONS: i64 = 100_000_000;
const CAPACITY: usize = 32_768;

#[cfg(unix)]
fn pin(core: usize) {
    // SAFETY: zero-initialising a cpu_set_t and calling the libc affinity
    // helpers with valid arguments is sound.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

#[cfg(not(unix))]
fn pin(_core: usize) {}

fn run<P, C>(name: &str, cores: Option<(usize, usize)>)
where
    P: WaitStrategy + Send + Sync + 'static,
    C: WaitStrategy + Send + Sync + 'static,
{
    let (mut tx, mut rx) = RingBuffer::<i64, P, C>::with_wait_strategies(CAPACITY);
    let consumer_core = cores.map(|(_, c)| c);

    let consumer = std::thread::spawn(move || {
        if let Some(c) = consumer_core {
            pin(c);
        }
        let mut consumed: i64 = 0;
        while consumed < NUM_ITERATIONS {
            let _ = rx.pop();
            consumed += 1;
        }
    });

    if let Some((p, _)) = cores {
        pin(p);
    }

    let start = Instant::now();
    for i in 0..NUM_ITERATIONS {
        tx.push(i);
    }
    consumer.join().unwrap();
    let elapsed = start.elapsed();

    let ns_per_op = elapsed.as_nanos() as f64 / NUM_ITERATIONS as f64;
    let mops = NUM_ITERATIONS as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{name:<14} {ns_per_op:>5.2} ns/op   {mops:>6.1} M msgs/s   {:>5} ms",
        elapsed.as_millis()
    );
}

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let cores = match args.as_slice() {
        [p, c] => {
            println!("pinning producer -> core {p}, consumer -> core {c}");
            Some((*p, *c))
        }
        _ => {
            println!("unpinned (pass two core ids to pin, e.g. `bench 18 19`)");
            None
        }
    };

    // Run twice, as the C++ benchmark does, to let caches/governors settle.
    for _ in 0..2 {
        run::<PauseWait, PauseWait>("SPSC_Pause", cores);
        run::<YieldWait, YieldWait>("SPSC_Yield", cores);
        run::<NoOpWait, NoOpWait>("SPSC_NoOp", cores);
    }
}
