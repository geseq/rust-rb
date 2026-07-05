//! Throughput benchmark for the shared-memory-backed rings.
//!
//! Three configurations, all over a memfd-backed mapping:
//!  - same-process, two threads (isolates the shm backing from IPC effects;
//!    expected identical to the heap ring — the hot path is the same code)
//!  - cross-process element ring (i64), producer in a child process
//!  - cross-process byte ring (8-byte messages)
//!
//! ```text
//! cargo run --release --features shm --example bench_shm            # unpinned
//! cargo run --release --features shm --example bench_shm 18 19      # pinned
//! ```
//!
//! Cross-process timing starts at the parent's first pop, so child startup
//! is excluded; the number is sustained consumer-side throughput.

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
mod bench {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
    use std::time::Instant;

    use super::common::{cores, pin};

    use rust_rb::spsc_bytes::BytesRingBuffer;
    use rust_rb::wait::PauseWait;
    use rust_rb::{memfd, RingBuffer};

    const NUM_ITEMS: i64 = 100_000_000;
    const CAPACITY: usize = 32_768;
    const BYTES_MSGS: usize = 20_000_000;
    const BYTES_CAPACITY: usize = 64 * 1024;

    /// Same process, two threads, ring state in the shm mapping.
    fn same_process(cores: Option<(usize, usize)>) {
        let fd = memfd("bench-shm-local").unwrap();
        // SAFETY: fresh private memfd, cooperating handles only.
        let (mut tx, mut rx) = unsafe {
            RingBuffer::<i64, PauseWait, PauseWait>::create_shm_with(fd.as_fd(), CAPACITY)
        }
        .unwrap();

        let consumer_core = cores.map(|(_, c)| c);
        let consumer = std::thread::spawn(move || {
            if let Some(c) = consumer_core {
                pin(c);
            }
            for _ in 0..NUM_ITEMS {
                let _ = rx.pop();
            }
        });
        if let Some((p, _)) = cores {
            pin(p);
        }
        let start = Instant::now();
        for i in 0..NUM_ITEMS {
            tx.push(i);
        }
        consumer.join().unwrap();
        report("SHM_same_process_i64", NUM_ITEMS as usize, start.elapsed());
    }

    fn report(name: &str, ops: usize, elapsed: std::time::Duration) {
        let ns = elapsed.as_nanos() as f64 / ops as f64;
        let mops = ops as f64 / elapsed.as_secs_f64() / 1e6;
        println!("{name:<26} {ns:>6.2} ns/op  {mops:>7.1} M msgs/s");
    }

    /// Child-process producer entry (selected via env vars set by the parent).
    fn maybe_run_child() {
        let Ok(role) = std::env::var("BENCH_SHM_ROLE") else {
            return;
        };
        let fd_num: i32 = std::env::var("BENCH_SHM_FD").unwrap().parse().unwrap();
        let core: i64 = std::env::var("BENCH_SHM_CORE").unwrap().parse().unwrap();
        // SAFETY: the parent passed this inherited, open memfd.
        let fd = unsafe { OwnedFd::from_raw_fd(fd_num) };
        if core >= 0 {
            pin(core as usize);
        }

        match role.as_str() {
            "elem-producer" => {
                // SAFETY: cooperating handles; the parent holds only the consumer.
                let mut tx = unsafe {
                    RingBuffer::<i64, PauseWait, PauseWait>::attach_shm_producer(fd.as_fd())
                }
                .unwrap();
                for i in 0..NUM_ITEMS {
                    tx.push(i);
                }
            }
            "bytes-producer" => {
                // SAFETY: cooperating handles; the parent holds only the consumer.
                let mut tx = unsafe {
                    BytesRingBuffer::<PauseWait, PauseWait>::attach_shm_producer(fd.as_fd())
                }
                .unwrap();
                let msg = [0xa5u8; 8];
                for _ in 0..BYTES_MSGS {
                    tx.push(&msg);
                }
            }
            other => panic!("unknown role {other}"),
        }
        std::process::exit(0);
    }

    fn spawn_child(fd: &OwnedFd, role: &str, core: Option<usize>) -> std::process::Child {
        // memfd() sets close-on-exec; this child is the intended inheritor.
        // SAFETY: valid fd; clearing FD_CLOEXEC is benign.
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, 0) };
        std::process::Command::new(std::env::current_exe().unwrap())
            .env("BENCH_SHM_ROLE", role)
            .env("BENCH_SHM_FD", fd.as_raw_fd().to_string())
            .env(
                "BENCH_SHM_CORE",
                core.map(|c| c as i64).unwrap_or(-1).to_string(),
            )
            .spawn()
            .expect("spawn child producer")
    }

    /// Wait for the producer to publish something, but do not spin forever if
    /// the child died before pushing (e.g. a failed core pin): poll the
    /// child's liveness while waiting for the first item.
    fn await_first_or_child_death(
        child: &mut std::process::Child,
        mut try_consume_one: impl FnMut() -> bool,
    ) {
        loop {
            if try_consume_one() {
                return;
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("child producer exited before producing (status {status:?})");
            }
            std::hint::spin_loop();
        }
    }

    /// Cross-process element ring: child produces, parent consumes.
    fn cross_process_elems(cores: Option<(usize, usize)>) {
        let fd = memfd("bench-shm-xproc-elem").unwrap();
        // Parent creates the ring, keeps the consumer, frees the producer role
        // for the child.
        // SAFETY: fresh private memfd, cooperating handles only.
        let (tx, mut rx) = unsafe {
            RingBuffer::<i64, PauseWait, PauseWait>::create_shm_with(fd.as_fd(), CAPACITY)
        }
        .unwrap();
        drop(tx);

        let mut child = spawn_child(&fd, "elem-producer", cores.map(|(p, _)| p));
        if let Some((_, c)) = cores {
            pin(c);
        }

        // Exclude child startup: time from the first message. Guard against
        // a child that never produces (failed pin) rather than hanging.
        await_first_or_child_death(&mut child, || match rx.try_pop() {
            Some(v) => {
                assert_eq!(v, 0);
                true
            }
            None => false,
        });
        let start = Instant::now();
        for i in 1..NUM_ITEMS {
            let got = rx.pop();
            debug_assert_eq!(got, i);
        }
        let elapsed = start.elapsed();
        child.wait().unwrap();
        report("SHM_cross_process_i64", (NUM_ITEMS - 1) as usize, elapsed);
    }

    /// Cross-process byte ring: child produces 8-byte messages, parent consumes.
    fn cross_process_bytes(cores: Option<(usize, usize)>) {
        let fd = memfd("bench-shm-xproc-bytes").unwrap();
        // SAFETY: fresh private memfd, cooperating handles only.
        let (tx, mut rx) = unsafe {
            BytesRingBuffer::<PauseWait, PauseWait>::create_shm_with(fd.as_fd(), BYTES_CAPACITY)
        }
        .unwrap();
        drop(tx);

        let mut child = spawn_child(&fd, "bytes-producer", cores.map(|(p, _)| p));
        if let Some((_, c)) = cores {
            pin(c);
        }

        // first message: producer is up (guarded against a dead child).
        await_first_or_child_death(&mut child, || rx.try_pop().is_some());
        let start = Instant::now();
        for _ in 1..BYTES_MSGS {
            drop(rx.pop());
        }
        let elapsed = start.elapsed();
        child.wait().unwrap();
        report("SHM_cross_process_bytes8", BYTES_MSGS - 1, elapsed);
    }

    pub fn run() {
        maybe_run_child();
        let cores = cores();
        match cores {
            Some((p, c)) => println!("pinning producer -> core {p}, consumer -> core {c}"),
            None => println!("unpinned (pass two core ids to pin, e.g. `bench_shm 18 19`)"),
        }
        // Run twice, as the other benches do, to let caches settle.
        for _ in 0..2 {
            same_process(cores);
            cross_process_elems(cores);
            cross_process_bytes(cores);
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
fn main() {
    bench::run();
}

/// `required-features = ["shm"]` gates the *feature*, but the example must
/// still produce a `main` when the feature is enabled on non-Linux targets
/// (where the shm API does not exist).
#[cfg(not(all(feature = "shm", target_os = "linux", target_has_atomic = "64")))]
fn main() {
    eprintln!("bench_shm requires Linux (the shm feature is Linux-only)");
}
