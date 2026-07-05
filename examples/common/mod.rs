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
