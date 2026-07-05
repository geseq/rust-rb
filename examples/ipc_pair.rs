//! Minimal two-process example: one `u64` ring shared over a memfd between a
//! parent and a `fork`ed child.
//!
//! The child is the producer, the parent is the consumer. They talk through a
//! single ring whose state lives in a `memfd`-backed mapping that both
//! processes share; the round-trip at the end proves every value the child
//! pushed arrived, in order, in the parent.
//!
//! ```text
//! cargo run --example ipc_pair --features shm
//! ```
//!
//! This is the runnable companion to the `guide::shm_ipc` chapter — read that
//! for the trust model, lease/recovery semantics, and the SCM_RIGHTS
//! alternative to fork-inheritance. Here we favor a short, correct walkthrough
//! over completeness.

// The shm API only exists on Linux with a 64-bit atomic (same gate the crate
// puts on the `shm` module). When that does not hold we still need *a* `main`,
// so the real body lives in a cfg'd module and there is a fallback below.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
mod ipc {
    use std::os::fd::AsFd;

    use rust_rb::{memfd, RingBuffer};

    // A handful of messages is plenty to demonstrate the hand-off; the ring
    // itself is sized far larger so the child never has to block.
    const COUNT: u64 = 8;
    const CAPACITY: usize = 1024;

    pub fn run() {
        // 1. Back the ring with an anonymous, in-memory file. `memfd` sets
        //    close-on-exec; we do NOT exec here (we fork), so the child keeps
        //    the inherited fd across the fork and no CLOEXEC clearing is
        //    needed. `u64` is a `ShmItem` (plain data, every bit pattern
        //    valid), so it is safe to ship across the process boundary.
        let fd = memfd("ipc-pair").expect("memfd");

        // 2. Initialize a fresh ring in that region and take BOTH roles. The
        //    default wait strategy is YieldWait, which is a `CrossProcess`
        //    spin — required here, because a condvar-based strategy would only
        //    work within a single address space.
        //
        //    SAFETY: the region is a fresh, private memfd that only this
        //    program's cooperating rust-rb handles will ever touch.
        let (tx, mut rx) =
            unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY) }.expect("create_shm");

        // 3. Hand the producer role to the child. Dropping `tx` releases its
        //    lease, so the child's `attach_shm_producer` can claim it; the
        //    parent keeps `rx` and stays the sole consumer. We drop BEFORE the
        //    fork so the child never inherits a live producer handle.
        drop(tx);

        // 4. Fork. The child inherits the open memfd (shared pages, same ring)
        //    and a bit-identical copy of every handle in scope — here just
        //    `rx`. Teardown is pid-guarded, so the child exiting cannot revoke
        //    the parent's consumer lease, but the child must never *use* that
        //    inherited handle (doing so would break single-consumer). It
        //    attaches its own producer instead.
        //
        //    SAFETY: `fork` in a program this small is well behaved — no
        //    threads are running and the child only makes async-signal-safe /
        //    ring calls before `_exit`.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            panic!("fork failed: {}", std::io::Error::last_os_error());
        }

        if pid == 0 {
            child(&fd, rx);
        } else {
            parent(&mut rx, pid);
        }
    }

    /// Child process: claim the freed producer role and push `COUNT` values.
    fn child(fd: &std::os::fd::OwnedFd, inherited_rx: rust_rb::Consumer<u64>) {
        // The child inherited a bit-identical copy of the parent's consumer
        // handle. Teardown is pid-guarded, so this copy's `Drop` is already a
        // no-op in the child — it won't flush the shared read cursor or release
        // the parent's lease (both are gated on the creating pid). We
        // `mem::forget` it anyway as belt-and-suspenders: it makes "the child
        // never touches the inherited handle" explicit and prevents any use.
        std::mem::forget(inherited_rx);

        // Claim the producer role the parent released. `attach_shm_producer`
        // returns `AddrInUse` if the lease is still held — proof the drop in
        // the parent actually freed it.
        //
        // SAFETY: cooperating handles only; the parent holds just the consumer.
        let mut tx =
            unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }.expect("attach producer");

        for i in 0..COUNT {
            tx.push(i * 10);
        }

        // Exit without running destructors: the parent is still draining, and
        // everything we pushed is already published through the ring's Release
        // cursor store, so a clean `_exit` loses nothing.
        //
        // SAFETY: `_exit` terminates the child immediately; no cleanup needed.
        unsafe { libc::_exit(0) };
    }

    /// Parent process: drain `COUNT` values and reap the child.
    fn parent(rx: &mut rust_rb::Consumer<u64>, child_pid: libc::pid_t) {
        for i in 0..COUNT {
            // `pop` spins (YieldWait) until the child publishes the next value.
            let got = rx.pop();
            assert_eq!(got, i * 10, "value {i} did not round-trip intact");
        }

        // Reap the child so it does not linger as a zombie.
        let mut status = 0;
        // SAFETY: valid pid we forked; `status` is a valid out-pointer.
        let waited = unsafe { libc::waitpid(child_pid, &mut status, 0) };
        assert_eq!(waited, child_pid, "waitpid failed");

        println!("round-trip OK: parent received {COUNT} values from the forked child");
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
fn main() {
    ipc::run();
}

/// `required-features = ["shm"]` gates the *feature*, but the example must
/// still produce a `main` when the feature is enabled on a target without the
/// shm API (non-Linux, or no 64-bit atomics).
#[cfg(not(all(feature = "shm", target_os = "linux", target_has_atomic = "64")))]
fn main() {
    eprintln!("ipc_pair requires Linux (the shm feature is Linux-only)");
}
