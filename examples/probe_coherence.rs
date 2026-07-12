//! Synthetic cache-coherence floor probe — no ring code at all.
//!
//! Both open perf findings (`rust-rb-vio`, `rust-rb-6l0`) are shapes of the
//! same question: what does it cost a single writer to publish through a
//! shared cache line while k pinned readers spin on it? This probe measures
//! exactly that, so ring numbers can be split into "hardware coherence
//! floor" and "ring overhead above the floor" on any box.
//!
//! Two configurations, k∈{0,1,2,4,8} readers each:
//!
//! * `one-line` — the writer Release-stores a counter to line A; readers
//!   spin Acquire-loading A. This is the spmc `write_cursor` (and the
//!   broadcast `tail`) traffic pattern: one published line, k sharers to
//!   invalidate per store.
//! * `two-line` — the writer additionally Release-stores to line B each
//!   iteration, and a reader that observes A advance loads B once. This is
//!   the broadcast pattern: the tail line readers spin on plus the slot
//!   line they then copy from (and the producer next overwrites).
//!
//! ```text
//! cargo run --release --example probe_coherence                # default cores
//! cargo run --release --example probe_coherence 15 5 6 7 8 9 16 17 18
//! ```
//!
//! First core id is the writer; the rest are reader cores (a k-reader run
//! uses the first k). Pin every core to ONE cluster — cross-cluster runs
//! measure the interconnect, not the protocol. Run twice; quote the second
//! (warm) pass.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[path = "common/mod.rs"]
mod common;
use common::{announce_machine, core_list_announced, pin};

const ITERS: u64 = 50_000_000;
/// Writer's end-of-run sentinel (never a real counter value at this ITERS).
const STOP: u64 = u64::MAX;

/// One counter alone on its cache line.
#[repr(align(128))]
struct Line(AtomicU64);

struct Shared {
    a: Line,
    b: Line,
}

/// Run one configuration and return the writer's ns per store-iteration.
fn run(name: &str, k: usize, two_line: bool, cores: &[usize]) {
    assert!(cores.len() > k, "not enough reader cores");
    let shared = Arc::new(Shared {
        a: Line(AtomicU64::new(0)),
        b: Line(AtomicU64::new(0)),
    });
    let barrier = Arc::new(Barrier::new(k + 1));

    let readers: Vec<_> = cores[1..=k]
        .iter()
        .map(|&core| {
            let shared = Arc::clone(&shared);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(core);
                barrier.wait();
                let mut last = 0u64;
                let mut sink = 0u64;
                loop {
                    let v = shared.a.0.load(Ordering::Acquire);
                    if v == STOP {
                        break;
                    }
                    if two_line && v != last {
                        last = v;
                        sink = sink.wrapping_add(shared.b.0.load(Ordering::Acquire));
                    }
                }
                std::hint::black_box(sink);
            })
        })
        .collect();

    pin(cores[0]);
    barrier.wait();
    let start = Instant::now();
    for i in 1..=ITERS {
        if two_line {
            shared.b.0.store(i, Ordering::Release);
        }
        shared.a.0.store(i, Ordering::Release);
    }
    let elapsed = start.elapsed();
    shared.a.0.store(STOP, Ordering::Release);
    for r in readers {
        r.join().unwrap();
    }

    let ns = elapsed.as_nanos() as f64 / ITERS as f64;
    println!("{name:<16} k={k}   {ns:>6.2} ns/store");
}

fn main() {
    announce_machine();
    let cores = core_list_announced("probe_coherence", &[15, 5, 6, 7, 8, 9, 16, 17, 18]);
    assert!(
        cores.len() >= 9,
        "probe_coherence wants a writer core and eight reader cores"
    );

    for pass in 1..=2 {
        println!("--- pass {pass} ---");
        for &k in &[0usize, 1, 2, 4, 8] {
            run("one-line", k, false, &cores);
        }
        for &k in &[0usize, 1, 2, 4, 8] {
            run("two-line", k, true, &cores);
        }
    }
}
