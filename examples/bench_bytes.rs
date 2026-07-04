//! Throughput benchmark for the variable-size-message ring.
//!
//! Pushes `NUM_MESSAGES` payloads of each size through a 64 KiB ring and
//! reports nanoseconds per message and effective payload bandwidth, for both
//! the per-message `pop` path and the batched `drain` path.
//!
//! ```text
//! cargo run --release --example bench_bytes            # unpinned
//! cargo run --release --example bench_bytes 18 19      # pin producer, consumer
//! ```

use std::time::Instant;

use rust_rb::spsc_bytes::BytesRingBuffer;
use rust_rb::wait::{NoOpWait, PauseWait, WaitStrategy};

const NUM_MESSAGES: usize = 20_000_000;
const CAPACITY: usize = 64 * 1024;

#[cfg(any(target_os = "linux", target_os = "android"))]
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

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn pin(_core: usize) {}

fn run<P, C>(name: &str, msg_len: usize, drain: bool, cores: Option<(usize, usize)>)
where
    P: WaitStrategy + Send + Sync + 'static,
    C: WaitStrategy + Send + Sync + 'static,
{
    let (mut tx, mut rx) = BytesRingBuffer::<P, C>::with_wait_strategies(CAPACITY);
    let consumer_core = cores.map(|(_, c)| c);

    let consumer = std::thread::spawn(move || {
        if let Some(c) = consumer_core {
            pin(c);
        }
        let mut consumed = 0usize;
        let mut bytes = 0usize;
        if drain {
            while consumed < NUM_MESSAGES {
                consumed += rx.drain(|msg| bytes += msg.len());
            }
        } else {
            while consumed < NUM_MESSAGES {
                bytes += rx.pop().len();
                consumed += 1;
            }
        }
        assert_eq!(bytes, NUM_MESSAGES * msg_len);
    });

    if let Some((p, _)) = cores {
        pin(p);
    }

    let msg = vec![0xa5u8; msg_len];
    let start = Instant::now();
    for _ in 0..NUM_MESSAGES {
        tx.push(&msg);
    }
    consumer.join().unwrap();
    let elapsed = start.elapsed();

    let ns_per_msg = elapsed.as_nanos() as f64 / NUM_MESSAGES as f64;
    let gb_per_s = (NUM_MESSAGES * msg_len) as f64 / elapsed.as_nanos() as f64;
    println!("{name:<24} {msg_len:>4} B  {ns_per_msg:>6.2} ns/msg  {gb_per_s:>6.2} GB/s");
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
            println!("unpinned (pass two core ids to pin, e.g. `bench_bytes 18 19`)");
            None
        }
    };

    // Run twice, as the fixed-size benchmark does, to let caches settle.
    for _ in 0..2 {
        for &len in &[8usize, 64, 256] {
            run::<PauseWait, PauseWait>("BYTES_Pause_pop", len, false, cores);
            run::<PauseWait, PauseWait>("BYTES_Pause_drain", len, true, cores);
            run::<NoOpWait, NoOpWait>("BYTES_NoOp_drain", len, true, cores);
        }
    }
}
