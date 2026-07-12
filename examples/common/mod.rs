//! Shared helpers for the benchmark examples (pinning, arg parsing).
//!
//! Included by each bench via `#[path = "common/mod.rs"] mod common;`.
//! Files under `examples/` subdirectories are not built as example targets,
//! so this is not compiled on its own.
#![allow(dead_code)]

/// Pin the current thread to `core`. Only the Linux/Android affinity API is
/// used; elsewhere it is a no-op.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn pin(core: usize) {
    // SAFETY: zero-initialising a cpu_set_t and calling the libc affinity
    // helpers with valid arguments is sound.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        // Reported numbers claim to be pinned; a silent failure (offline
        // core, cgroup cpuset) would publish unpinned results as pinned.
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0,
            "failed to pin to core {core}: {}",
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn pin(_core: usize) {}

/// Parse the pin pair from argv and print the pin/unpinned announcement,
/// returning the pair. `example` is used in the "how to pin" hint.
pub fn cores_announced(example: &str) -> Option<(usize, usize)> {
    let cores = cores();
    match cores {
        Some((p, c)) => println!("pinning producer -> core {p}, consumer -> core {c}"),
        None => println!("unpinned (pass two core ids to pin, e.g. `{example} 18 19`)"),
    }
    cores
}

/// Parse an optional `(producer_core, consumer_core)` pin pair from argv.
pub fn cores() -> Option<(usize, usize)> {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    match args.as_slice() {
        [p, c] => Some((*p, *c)),
        _ => None,
    }
}

/// Parse a pinned core list from argv — the first id is the producer core,
/// the rest are consumer cores — falling back to `defaults` when no numeric
/// args are given, and print the resulting layout. `example` is used in the
/// "how to pin" hint.
pub fn core_list_announced(example: &str, defaults: &[usize]) -> Vec<usize> {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let cores = if args.is_empty() {
        defaults.to_vec()
    } else {
        args
    };
    assert!(
        cores.len() >= 2,
        "need a producer core and at least one consumer core, e.g. `{example} 15 16 17 18 19`"
    );
    println!(
        "pinning producer -> core {}, consumers -> cores {:?}",
        cores[0],
        &cores[1..]
    );
    cores
}

/// Print a one-line machine identification so bench logs are attributable.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn announce_machine() {
    // SAFETY: uname fills a zero-initialised utsname; the fields are
    // NUL-terminated C strings by contract.
    unsafe {
        let mut u: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut u) == 0 {
            let s = |f: *const libc::c_char| std::ffi::CStr::from_ptr(f).to_string_lossy();
            println!(
                "machine: {} {} {} ({} cpus)",
                s(u.nodename.as_ptr()),
                s(u.release.as_ptr()),
                s(u.machine.as_ptr()),
                std::thread::available_parallelism().map_or(0, |n| n.get()),
            );
            return;
        }
    }
    announce_machine_fallback();
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn announce_machine() {
    announce_machine_fallback();
}

fn announce_machine_fallback() {
    println!(
        "machine: {} ({} cpus)",
        std::env::consts::ARCH,
        std::thread::available_parallelism().map_or(0, |n| n.get()),
    );
}

/// Busy-wait for `iters` spin-loop hints — the rate-limit knob for the
/// deliberately-slow-consumer benches. Calibrate with [`spin_ns_per_iter`].
#[inline]
pub fn spin_delay(iters: u32) {
    for _ in 0..iters {
        std::hint::spin_loop();
    }
}

/// Measure the cost of one `spin_loop` hint on the current core, in ns.
/// Pin the thread first: the clusters of a heterogeneous part differ.
pub fn spin_ns_per_iter() -> f64 {
    let start = std::time::Instant::now();
    for _ in 0..1_000_000 {
        std::hint::spin_loop();
    }
    start.elapsed().as_nanos() as f64 / 1e6
}

/// Convert a target delay to `spin_delay` iterations, calibrated on the
/// **current** core. Call from the spinning thread itself, *after* pinning:
/// on a heterogeneous part a knob calibrated on one cluster is off by the
/// clusters' spin-cost ratio on the other (`0` stays `0` — no rate limit).
pub fn spin_iters_for_ns(ns: u32) -> u32 {
    if ns == 0 {
        return 0;
    }
    ((ns as f64 / spin_ns_per_iter()) as u32).max(1)
}
