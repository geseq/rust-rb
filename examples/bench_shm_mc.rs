//! Cross-process benchmark for the **multi-consumer** shared-memory rings —
//! the shm counterpart of `bench_spmc` / `bench_broadcast` / `bench_anchored`
//! (`bench_shm` covers the SPSC rings).
//!
//! The parent holds the producer (so the coupling metric — producer ns/push —
//! is measured in-process); each consumer runs in its own **child process**:
//!
//! * `SPMC_SHM  N∈{1,2}` — gating consumers attach read-write with a lease.
//! * `BCAST_SHM k∈{1,2}` — lossy observers attach **read-only**
//!   (`PROT_READ`), the lease-free path this backing exists for.
//! * `ANCH_SHM  A=1 K=1` — one required anchor + one read-only observer.
//!
//! Children signal readiness over piped stdout before the parent starts
//! pushing (so "caught-up k" configurations really start caught up), consume
//! until the ring closes, then report their own throughput/loss lines, which
//! the parent forwards indented.
//!
//! ```text
//! cargo run --release --features shm --example bench_shm_mc            # default cores
//! cargo run --release --features shm --example bench_shm_mc 15 16 17 18 19
//! ```
//!
//! First core id is the parent producer; the rest are child cores in spawn
//! order. Pin to one cluster; run twice; quote the second (warm) pass.

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
mod bench {
    use std::io::{BufRead, BufReader, Read};
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
    use std::time::Instant;

    use super::common::{announce_machine, core_list_announced, pin};

    use rust_rb::memfd;
    use rust_rb::{anchored, broadcast, spmc};

    const ITERS: u64 = 20_000_000;
    const CAPACITY: usize = 32_768;

    // ---------------------------------------------------------------- child

    /// Child entry (selected via env vars set by the parent): attach to the
    /// inherited ring fd, print `READY`, consume until closed-and-drained,
    /// print one stats line, exit.
    pub(super) fn maybe_run_child() {
        let Ok(role) = std::env::var("BENCH_SHMMC_ROLE") else {
            return;
        };
        let fd_num: i32 = std::env::var("BENCH_SHMMC_FD").unwrap().parse().unwrap();
        let core: i64 = std::env::var("BENCH_SHMMC_CORE").unwrap().parse().unwrap();
        // SAFETY: the parent passed this inherited, open memfd.
        let fd = unsafe { OwnedFd::from_raw_fd(fd_num) };
        if core >= 0 {
            pin(core as usize);
        }

        let ready = || println!("READY");

        match role.as_str() {
            "spmc-consumer" => {
                // SAFETY: cooperating handles on a ring the parent created.
                let mut rx =
                    unsafe { spmc::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
                ready();
                let first = rx.pop().expect("open ring");
                std::hint::black_box(first);
                let start = Instant::now();
                let mut count = 1u64;
                // Closed-and-drained (`Err(spmc::Closed)`) ends the run.
                while let Ok(v) = rx.pop() {
                    std::hint::black_box(v);
                    count += 1;
                }
                let ns = start.elapsed().as_nanos() as f64 / (count - 1).max(1) as f64;
                println!("gating consumer: {count} msgs, {ns:.2} ns/msg");
            }
            "bcast-observer" => {
                // SAFETY: cooperating handles; this attach maps PROT_READ.
                let mut rx =
                    unsafe { broadcast::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
                        .unwrap();
                ready();
                let (mut accepted, mut lag_events, mut missed) = (0u64, 0u64, 0u64);
                let start = Instant::now();
                loop {
                    match rx.try_pop() {
                        Ok(Some(v)) => {
                            std::hint::black_box(v);
                            accepted += 1;
                        }
                        Ok(None) => {}
                        Err(broadcast::PopError::Lagged { missed: m }) => {
                            lag_events += 1;
                            missed += m;
                        }
                        Err(broadcast::PopError::Closed) => break,
                    }
                }
                let ns = start.elapsed().as_nanos() as f64 / accepted.max(1) as f64;
                println!(
                    "read-only observer: accepted {accepted} ({ns:.2} ns/msg incl. wait), \
                     lagged {lag_events}x missed {missed}"
                );
            }
            "anch-anchor" => {
                // SAFETY: cooperating handles on a ring the parent created.
                let mut rx =
                    unsafe { anchored::RingBuffer::<u64>::attach_shm_anchor(fd.as_fd()) }.unwrap();
                ready();
                let first = rx.pop().expect("open ring");
                std::hint::black_box(first);
                let start = Instant::now();
                let mut count = 1u64;
                // Closed-and-drained (`Err(anchored::Closed)`) ends the run.
                while let Ok(v) = rx.pop() {
                    std::hint::black_box(v);
                    count += 1;
                }
                let ns = start.elapsed().as_nanos() as f64 / (count - 1).max(1) as f64;
                println!("anchor: {count} msgs, {ns:.2} ns/msg");
            }
            "anch-observer" => {
                // SAFETY: cooperating handles; this attach maps PROT_READ.
                let mut rx =
                    unsafe { anchored::RingBuffer::<u64>::attach_shm_observer(fd.as_fd()) }
                        .unwrap();
                ready();
                let (mut accepted, mut lag_events, mut missed) = (0u64, 0u64, 0u64);
                let start = Instant::now();
                loop {
                    match rx.try_pop() {
                        Ok(Some(v)) => {
                            std::hint::black_box(v);
                            accepted += 1;
                        }
                        Ok(None) => {}
                        Err(anchored::PopError::Lagged { missed: m }) => {
                            lag_events += 1;
                            missed += m;
                        }
                        Err(anchored::PopError::Closed) => break,
                    }
                }
                let ns = start.elapsed().as_nanos() as f64 / accepted.max(1) as f64;
                println!(
                    "observer: accepted {accepted} ({ns:.2} ns/msg incl. wait), \
                     lagged {lag_events}x missed {missed}"
                );
            }
            other => panic!("unknown role {other}"),
        }
        std::process::exit(0);
    }

    // --------------------------------------------------------------- parent

    struct Child {
        proc: std::process::Child,
        out: BufReader<std::process::ChildStdout>,
    }

    fn spawn_child(fd: &OwnedFd, role: &str, core: usize) -> Child {
        // memfd() sets close-on-exec; this child is the intended inheritor.
        // SAFETY: valid fd; clearing FD_CLOEXEC is benign.
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, 0) };
        let mut proc = std::process::Command::new(std::env::current_exe().unwrap())
            .env("BENCH_SHMMC_ROLE", role)
            .env("BENCH_SHMMC_FD", fd.as_raw_fd().to_string())
            .env("BENCH_SHMMC_CORE", core.to_string())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn child consumer");
        let out = BufReader::new(proc.stdout.take().unwrap());
        Child { proc, out }
    }

    /// Block until the child prints `READY` (its attach succeeded), guarding
    /// against a child that died first.
    fn await_ready(child: &mut Child) {
        let mut line = String::new();
        let n = child.out.read_line(&mut line).expect("child stdout");
        assert!(
            n > 0 && line.trim() == "READY",
            "child failed before READY: {line:?} (status {:?})",
            child.proc.try_wait()
        );
    }

    /// Reap the child and forward its stats lines, indented.
    fn finish(mut child: Child) {
        let status = child.proc.wait().expect("child wait");
        assert!(status.success(), "child exited with {status:?}");
        let mut rest = String::new();
        child.out.read_to_string(&mut rest).expect("child stdout");
        for line in rest.lines() {
            println!("    {line}");
        }
    }

    fn report(name: &str, elapsed: std::time::Duration) {
        let ns = elapsed.as_nanos() as f64 / ITERS as f64;
        let mops = ITERS as f64 / elapsed.as_secs_f64() / 1e6;
        println!(
            "{name:<26} {ns:>6.2} ns/push  {mops:>6.1} M msgs/s   {:>5} ms",
            elapsed.as_millis()
        );
    }

    /// Gating ring, N child consumers over shm.
    fn spmc_xproc(n: usize, cores: &[usize]) {
        let fd = memfd("bench-shm-mc-spmc").unwrap();
        // Parent keeps the producer; the initial consumer detaches on drop,
        // freeing its table slot for the children.
        // SAFETY: fresh private memfd; u64 is ShmItem.
        let (mut tx, rx) =
            unsafe { spmc::RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY, 4) }.unwrap();
        drop(rx);

        let mut children: Vec<_> = (0..n)
            .map(|i| spawn_child(&fd, "spmc-consumer", cores[1 + i]))
            .collect();
        children.iter_mut().for_each(await_ready);

        pin(cores[0]);
        let start = Instant::now();
        for i in 0..ITERS {
            tx.push(i);
        }
        let elapsed = start.elapsed();
        drop(tx); // graceful close: children drain, see Closed, report, exit
        report(&format!("SPMC_SHM_xproc N={n}"), elapsed);
        children.into_iter().for_each(finish);
    }

    /// Lossy ring, k read-only child observers over shm.
    fn bcast_xproc(k: usize, cores: &[usize]) {
        let fd = memfd("bench-shm-mc-bcast").unwrap();
        // SAFETY: fresh private memfd; u64 is ShmItem.
        let mut tx =
            unsafe { broadcast::RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY) }.unwrap();

        let mut children: Vec<_> = (0..k)
            .map(|i| spawn_child(&fd, "bcast-observer", cores[1 + i]))
            .collect();
        children.iter_mut().for_each(await_ready);

        pin(cores[0]);
        let start = Instant::now();
        for i in 0..ITERS {
            tx.push(i);
        }
        let elapsed = start.elapsed();
        drop(tx);
        report(&format!("BCAST_SHM_xproc k={k}"), elapsed);
        children.into_iter().for_each(finish);
    }

    /// Mixed ring, one child anchor + one read-only child observer.
    fn anch_xproc(cores: &[usize]) {
        let fd = memfd("bench-shm-mc-anch").unwrap();
        // SAFETY: fresh private memfd; u64 is ShmItem.
        let (mut tx, rx) =
            unsafe { anchored::RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY, 4) }.unwrap();
        drop(rx); // the initial anchor detaches; the child anchor gates instead

        let mut children = vec![
            spawn_child(&fd, "anch-anchor", cores[1]),
            spawn_child(&fd, "anch-observer", cores[2]),
        ];
        children.iter_mut().for_each(await_ready);

        pin(cores[0]);
        let start = Instant::now();
        for i in 0..ITERS {
            tx.push(i);
        }
        let elapsed = start.elapsed();
        drop(tx);
        report("ANCH_SHM_xproc A=1 K=1", elapsed);
        children.into_iter().for_each(finish);
    }

    pub(super) fn main() {
        maybe_run_child(); // children never return from this

        announce_machine();
        let cores = core_list_announced("bench_shm_mc", &[15, 16, 17, 18, 19]);
        assert!(
            cores.len() >= 3,
            "bench_shm_mc needs a producer core and two consumer cores"
        );

        for pass in 1..=2 {
            println!("--- pass {pass} ---");
            spmc_xproc(1, &cores);
            spmc_xproc(2, &cores);
            bcast_xproc(1, &cores);
            bcast_xproc(2, &cores);
            anch_xproc(&cores);
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
fn main() {
    bench::main();
}

/// `required-features = ["shm"]` gates the *feature*, but the example must
/// still produce a `main` when the feature is enabled on non-Linux targets
/// (where the shm API does not exist).
#[cfg(not(all(feature = "shm", target_os = "linux", target_has_atomic = "64")))]
fn main() {
    eprintln!("bench_shm_mc requires Linux (the shm feature is Linux-only)");
}
