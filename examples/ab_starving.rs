//! Temporary A/B microbench: cost of the eager starving-flag load in
//! Msg::drop on the caught-up path (single-threaded alternating push/pop,
//! so the caught-up trigger fires on every release and the flag's value is
//! never needed).

use std::time::Instant;

use rust_rb::spsc_bytes::BytesRingBuffer;
use rust_rb::wait::NoOpWait;

fn main() {
    const N: usize = 100_000_000;
    let (mut tx, mut rx) = BytesRingBuffer::<NoOpWait, NoOpWait>::with_wait_strategies(64 * 1024);
    let msg = [0xa5u8; 32];
    let mut sum = 0usize;
    for rep in 0..4 {
        let start = Instant::now();
        for _ in 0..N {
            tx.push(&msg);
            sum += rx.pop().len();
        }
        let e = start.elapsed();
        println!("rep {rep}: {:.3} ns/op", e.as_nanos() as f64 / N as f64);
    }
    assert!(sum > 0);
}
