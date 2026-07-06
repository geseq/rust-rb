//! Shared-memory LOSSY broadcast ring tests (feature `shm`, Linux).
//!
//! Mirrors `tests/spmc_shm.rs`: `fork`ed children exercise real
//! cross-address-space consumers (attach fresh handles in the child — never
//! use inherited ones). The crown jewel here is the **PROT_READ
//! enforcement**: lossy consumers attach over a read-only mapping, take no
//! lease, and write nothing — a full consume cycle in a forked child proves
//! it (any store in the consumer path would be a deterministic SIGSEGV,
//! which the parent would see in the wait status), and a negative control
//! proves the mapping protection is real.
#![cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

use rust_rb::broadcast::{PopError, RingBuffer};
use rust_rb::broadcast_bytes::{BytesRingBuffer, PopError as BytesPopError};
use rust_rb::{memfd, YieldWait};

/// The default-strategy byte ring, spelled out where inference needs help
/// (associated functions on a generic type do not apply parameter defaults).
type BytesRing = BytesRingBuffer<YieldWait>;

// Header offsets (mirrors of src/shm.rs — corruption/healing pokes only).
const OFF_CAPACITY: usize = 16;
const OFF_GENERATION: usize = 56;
const OFF_TAIL: usize = 128;
const OFF_CLOSED: usize = 136;
const OFF_SLACK: usize = 144;
const OFF_INTENT: usize = 256;
const OFF_LATEST: usize = 384;

/// Run `f` in a forked child. The child NEVER returns into the parent's test
/// harness: it exits 0 on success and 101 if `f` panicked.
fn fork_child(f: impl FnOnce()) -> libc::pid_t {
    // SAFETY: fork in the test process; the child only runs `f` (ring ops on
    // fresh handles + libc calls) and `_exit`s without touching the harness.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0, "fork failed: {}", std::io::Error::last_os_error());
    if pid == 0 {
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_ok();
        // SAFETY: terminating the child immediately is the point — no
        // destructors, no harness continuation.
        unsafe { libc::_exit(if ok { 0 } else { 101 }) };
    }
    pid
}

/// Reap `pid` and assert it exited cleanly (status 0). In the PROT_READ
/// tests this is the enforcement check: a consumer-path store would have
/// SIGSEGV'd the child, and the status would show the signal, not exit 0.
fn wait_child(pid: libc::pid_t) {
    let mut status = 0;
    // SAFETY: valid pid we forked; `status` is a valid out-pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid, "waitpid failed");
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child failed: status {status:#x} (signaled: {})",
        libc::WIFSIGNALED(status)
    );
}

/// Reap `pid` and assert it died of SIGSEGV — the negative control for the
/// read-only mapping.
fn wait_child_segv(pid: libc::pid_t) {
    let mut status = 0;
    // SAFETY: valid pid we forked; `status` is a valid out-pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid, "waitpid failed");
    assert!(
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGSEGV,
        "child must die of SIGSEGV, got status {status:#x}"
    );
}

/// A byte-granular sync channel between parent and child (survives fork).
struct Pipe {
    r: OwnedFd,
    w: OwnedFd,
}

fn pipe() -> Pipe {
    let mut fds = [0i32; 2];
    // SAFETY: valid out-array.
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0, "pipe failed");
    // SAFETY: both fds are freshly created and owned here.
    unsafe {
        Pipe {
            r: OwnedFd::from_raw_fd(fds[0]),
            w: OwnedFd::from_raw_fd(fds[1]),
        }
    }
}

impl Pipe {
    fn send(&self) {
        let b = [1u8];
        // SAFETY: valid fd and buffer.
        assert_eq!(
            unsafe { libc::write(self.w.as_raw_fd(), b.as_ptr().cast(), 1) },
            1
        );
    }

    fn recv(&self) {
        let mut b = [0u8];
        // SAFETY: valid fd and buffer.
        assert_eq!(
            unsafe { libc::read(self.r.as_raw_fd(), b.as_mut_ptr().cast(), 1) },
            1
        );
    }
}

/// Store a u64 into the region header at `off` through an independent
/// mapping (corruption/crash-simulation injection).
fn poke_u64(fd: BorrowedFd<'_>, off: usize, val: u64) {
    // SAFETY: fresh page-sized shared mapping of a valid region fd.
    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd.as_raw_fd(),
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        (*ptr
            .cast::<u8>()
            .add(off)
            .cast::<std::sync::atomic::AtomicU64>())
        .store(val, std::sync::atomic::Ordering::Release);
        libc::munmap(ptr, 4096);
    }
}

/// Load a u64 from the region header at `off`.
fn peek_u64(fd: BorrowedFd<'_>, off: usize) -> u64 {
    // SAFETY: as for `poke_u64`, read-only use.
    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd.as_raw_fd(),
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        let v = (*ptr
            .cast::<u8>()
            .add(off)
            .cast::<std::sync::atomic::AtomicU64>())
        .load(std::sync::atomic::Ordering::Acquire);
        libc::munmap(ptr, 4096);
        v
    }
}

// ---------------------------------------------------------------------------
// 1. Round trips cross-process: parent producer, forked child consumer via
//    the inherited fd — content-exact (both rings).
// ---------------------------------------------------------------------------

#[test]
fn element_round_trip_cross_process() {
    const N: u64 = 1000;
    let fd = memfd("bcast-elem-roundtrip").unwrap();
    // SAFETY: fresh private memfd; u64 is ShmItem + NoUninit.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), N as usize) }.unwrap();
    assert!(tx.capacity() >= N as usize, "no loss possible in this test");
    let ready = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only (read-only mapping, no lease).
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i), "content-exact stream");
        }
        assert_eq!(crx.try_pop(), Ok(None), "drained but alive");
    });

    // Do not publish before the child has joined (its join point is the
    // tail at attach).
    ready.recv();
    for i in 0..N {
        tx.push(i);
    }
    wait_child(pid);

    // In-process subscribe off the (shm) producer: a consumer over the
    // producer's read-write mapping, joining at the current tail.
    let mut rx2 = tx.subscribe::<YieldWait>();
    tx.push(4242);
    assert_eq!(rx2.pop(), Ok(4242));
    // ... and a sibling subscribed off that consumer joins at ITS tail.
    let mut rx3 = rx2.subscribe();
    tx.push(4343);
    assert_eq!(rx3.pop(), Ok(4343));
    assert_eq!(rx2.pop(), Ok(4343));
}

#[test]
fn bytes_round_trip_cross_process() {
    const N: u32 = 1000;
    let fd = memfd("bcast-bytes-roundtrip").unwrap();
    // 16-byte records * 1000 fits a 64 KiB ring: no loss possible.
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 65536) }.unwrap();
    let ready = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only (read-only mapping, no lease).
        let mut crx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        let mut out = Vec::new();
        for seq in 0..N {
            crx.pop_into(&mut out).unwrap();
            assert_eq!(&*out, &(seq as u64).to_le_bytes(), "content-exact");
        }
        assert_eq!(crx.try_pop(), Ok(None), "drained but alive");
    });

    ready.recv();
    for seq in 0..N {
        tx.push(&(seq as u64).to_le_bytes());
    }
    wait_child(pid);

    // In-process subscribe off the producer and a consumer sibling.
    let mut rx2 = tx.subscribe::<YieldWait>();
    let mut rx3 = rx2.subscribe();
    tx.push(b"post-join");
    assert_eq!(&*rx2.pop().unwrap(), b"post-join");
    assert_eq!(&*rx3.pop().unwrap(), b"post-join");
}

// ---------------------------------------------------------------------------
// 2. Multi-consumer cross-process: two forked children each see everything
//    (keeping up: capacity covers the whole stream).
// ---------------------------------------------------------------------------

#[test]
fn two_forked_consumers_each_see_everything() {
    const N: u64 = 4096;
    let fd = memfd("bcast-two-children").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), N as usize) }.unwrap();
    let ready = pipe();

    let child = || {
        // SAFETY: cooperating handles only (read-only mapping, no lease —
        // membership is unbounded).
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i));
        }
    };
    let pid_a = fork_child(child);
    let pid_b = fork_child(child);

    ready.recv();
    ready.recv();
    for i in 0..N {
        tx.push(i);
    }
    wait_child(pid_a);
    wait_child(pid_b);
}

// ---------------------------------------------------------------------------
// 3. Lossy across processes: tiny ring, slow child — exact Lagged
//    accounting; the child recovers and continues (element: exact missed;
//    bytes: latest-record boundary landing).
// ---------------------------------------------------------------------------

#[test]
fn lossy_element_lagged_exact_and_recovers() {
    // capacity 8, default slack = 1.
    let fd = memfd("bcast-lossy-elem").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8) }.unwrap();
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        c2p.send();
        p2c.recv(); // parent pushed 100 while we idled: we are lapped
                    // Reposition target: tail - capacity + slack = 100 - 8 + 1 = 93;
                    // exact and gap-free from position 0.
        assert_eq!(crx.pop(), Err(PopError::Lagged { missed: 93 }));
        for i in 93..100 {
            assert_eq!(crx.pop(), Ok(i), "salvaged window is content-exact");
        }
        assert_eq!(crx.try_pop(), Ok(None));
        c2p.send();
        p2c.recv(); // parent pushed 10 more (100..110): lapped again
        assert_eq!(
            crx.pop(),
            Err(PopError::Lagged { missed: 3 }),
            "gap-free accounting across successive laps"
        );
        for i in 103..110 {
            assert_eq!(crx.pop(), Ok(i), "child recovers and continues");
        }
        c2p.send();
        // Parent drops the producer: drained + closed.
        assert_eq!(crx.pop(), Err(PopError::Closed));
    });

    c2p.recv();
    for i in 0..100u64 {
        tx.push(i); // never blocks — the idle child cannot gate this
    }
    p2c.send();
    c2p.recv();
    for i in 100..110u64 {
        tx.push(i);
    }
    p2c.send();
    c2p.recv();
    drop(tx); // graceful close
    wait_child(pid);
}

#[test]
fn lossy_bytes_lagged_lands_on_latest_record() {
    // capacity 64 (max message 8): 16-byte records, 4 per lap.
    let fd = memfd("bcast-lossy-bytes").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 64) }.unwrap();
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only.
        let mut crx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
        c2p.send();
        p2c.recv(); // parent pushed 20 records (320 framed bytes) while we idled
                    // Reposition lands on `latest` — the start of the newest record
                    // (byte 304), a guaranteed boundary; the loss is exact in BYTES.
        assert_eq!(crx.pop(), Err(BytesPopError::Lagged { missed_bytes: 304 }));
        // The landing is a parseable record boundary: the newest message.
        assert_eq!(&*crx.pop().unwrap(), &19u64.to_le_bytes());
        assert_eq!(crx.try_pop(), Ok(None));
        c2p.send();
        // Parent pushes a few more within capacity: no further loss.
        p2c.recv();
        for seq in 20..23u64 {
            assert_eq!(&*crx.pop().unwrap(), &seq.to_le_bytes());
        }
        c2p.send();
        assert_eq!(crx.pop(), Err(BytesPopError::Closed));
    });

    c2p.recv();
    for seq in 0..20u64 {
        tx.push(&seq.to_le_bytes());
    }
    p2c.send();
    c2p.recv();
    for seq in 20..23u64 {
        tx.push(&seq.to_le_bytes());
    }
    p2c.send();
    c2p.recv();
    drop(tx);
    wait_child(pid);
}

// ---------------------------------------------------------------------------
// 4. THE PROT_READ enforcement: a child consumer over a read-only mapping
//    runs the FULL consume cycle — blocking pop, try_pop, the Lagged
//    reposition path, lag(), skip_to_latest(), subscribe, and the Closed
//    drain. Any store anywhere in that path segfaults the child; the parent
//    asserts a clean exit. A negative control proves the mapping protection
//    is real (so the clean exit means something).
// ---------------------------------------------------------------------------

#[test]
fn prot_read_full_consume_cycle_element() {
    let fd = memfd("bcast-protread-elem").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8) }.unwrap();
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only. The mapping is PROT_READ.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        c2p.send();
        // Blocking pop path (kept-up phase; includes the SelfTimed wait).
        for i in 0..4 {
            assert_eq!(crx.pop(), Ok(i));
        }
        c2p.send();
        p2c.recv(); // parent lapped us
                    // Lagged reposition path, lag, skip_to_latest, empty try_pop.
        assert!(matches!(crx.pop(), Err(PopError::Lagged { .. })));
        let _ = crx.lag();
        let _ = crx.skip_to_latest();
        assert_eq!(crx.try_pop(), Ok(None));
        // A read-only sibling via subscribe.
        let mut sib = crx.subscribe();
        assert_eq!(sib.try_pop(), Ok(None));
        c2p.send();
        // Closed drain path.
        assert_eq!(crx.pop(), Err(PopError::Closed));
        assert_eq!(sib.pop(), Err(PopError::Closed));
        // Consumer drop over the read-only mapping: munmap only.
    });

    c2p.recv();
    for i in 0..4u64 {
        tx.push(i);
    }
    c2p.recv();
    for i in 4..104u64 {
        tx.push(i);
    }
    p2c.send();
    c2p.recv();
    drop(tx);
    // Clean exit == not one store happened on the entire consumer path.
    wait_child(pid);
}

#[test]
fn prot_read_full_consume_cycle_bytes() {
    let fd = memfd("bcast-protread-bytes").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 64) }.unwrap();
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only. The mapping is PROT_READ.
        let mut crx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
        c2p.send();
        let mut out = Vec::new();
        for seq in 0..4u64 {
            crx.pop_into(&mut out).unwrap();
            assert_eq!(&*out, &seq.to_le_bytes());
        }
        c2p.send();
        p2c.recv(); // parent lapped us
        assert!(matches!(crx.pop(), Err(BytesPopError::Lagged { .. })));
        let _ = crx.lag();
        let _ = crx.skip_to_latest();
        assert_eq!(crx.try_pop(), Ok(None));
        let mut sib = crx.subscribe();
        assert!(matches!(sib.try_pop_into(&mut out), Ok(false)));
        c2p.send();
        assert_eq!(crx.pop(), Err(BytesPopError::Closed));
        assert_eq!(sib.pop(), Err(BytesPopError::Closed));
    });

    c2p.recv();
    for seq in 0..4u64 {
        tx.push(&seq.to_le_bytes());
    }
    c2p.recv();
    for seq in 4..54u64 {
        tx.push(&seq.to_le_bytes());
    }
    p2c.send();
    c2p.recv();
    drop(tx);
    wait_child(pid);
}

/// Negative control: a deliberate write through a read-only mapping of the
/// very same region DOES segfault — proving the enforcement in the two
/// tests above is real, not a mapping that silently fell back to
/// read-write.
#[test]
fn prot_read_negative_control_write_faults() {
    let fd = memfd("bcast-protread-negative").unwrap();
    // SAFETY: fresh private memfd.
    let _tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8) }.unwrap();

    let pid = fork_child(|| {
        // SAFETY: scratch child; the store below is MEANT to fault.
        unsafe {
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                4096,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            );
            assert_ne!(ptr, libc::MAP_FAILED);
            // The deliberate consumer-side sin: one store.
            std::ptr::write_volatile(ptr.cast::<u8>(), 1);
        }
    });
    wait_child_segv(pid);
}

// ---------------------------------------------------------------------------
// 5. Producer crash + force_attach_shm_producer: consumers keep draining
//    published data mid-recovery; the recovered producer continues; the
//    byte ring's intent/latest healing is observable.
// ---------------------------------------------------------------------------

#[test]
fn producer_crash_recovery_element() {
    let fd = memfd("bcast-recover-elem").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64) }.unwrap();
    for i in 0..10u64 {
        tx.push(i);
    }
    // Simulated crash: no destructors run — the closed flag is never set
    // and the producer lease is still held.
    std::mem::forget(tx);

    let c2p = pipe();
    let p2c = pipe();
    let pid = fork_child(|| {
        // Everything published stays drainable with the producer dead —
        // consumers need nothing from recovery.
        // SAFETY: cooperating handles only.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        // The join point is the tail (10): a lossy joiner sees only
        // post-join messages, so recovery continuity is asserted on those.
        assert_eq!(crx.try_pop(), Ok(None));
        c2p.send();
        p2c.recv(); // parent force-attached and pushed the new session
        for i in 10..20u64 {
            assert_eq!(crx.pop(), Ok(i), "the new session flows seamlessly");
        }
        c2p.send();
    });

    c2p.recv();
    // The dead producer's lease blocks a polite attach...
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }
        .err()
        .expect("held lease must refuse polite attach");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    // ... and force-attach is the caller-asserts-death recovery.
    // SAFETY: the previous producer is gone (forgotten, never used again).
    let mut tx2 = unsafe { RingBuffer::<u64>::force_attach_shm_producer(fd.as_fd()) }.unwrap();
    assert_eq!(tx2.tail(), 10, "resume exactly after the last publish");
    for i in 10..20u64 {
        tx2.push(i);
    }
    p2c.send();
    c2p.recv();
    wait_child(pid);

    // recover_shm is the same operation under the crate-wide recovery name.
    std::mem::forget(tx2);
    // SAFETY: as above.
    let mut tx3 = unsafe { RingBuffer::<u64>::recover_shm(fd.as_fd()) }.unwrap();
    let mut rx3 = tx3.subscribe::<YieldWait>();
    tx3.push(77);
    assert_eq!(rx3.pop(), Ok(77));
}

#[test]
fn producer_crash_recovery_bytes_heals_intent_and_latest() {
    let fd = memfd("bcast-recover-bytes").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 64) }.unwrap();
    for seq in 0..3u64 {
        tx.push(&seq.to_le_bytes()); // 16-byte records: tail = 48
    }
    std::mem::forget(tx); // crash: lease held, counters as-is

    // A consumer attached against the crashed ring: published records
    // stay drainable (it joins at tail 48, so assert emptiness + the
    // subsequent new-session flow).
    // SAFETY: cooperating handles only.
    let mut rx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(rx.try_pop(), Ok(None));

    // Simulate the mid-push death state: the dead producer declared 16 more
    // bytes (intent 64) and had already stored a `latest` pointing into the
    // never-committed span.
    poke_u64(fd.as_fd(), OFF_INTENT, 64);
    poke_u64(fd.as_fd(), OFF_LATEST, 56);
    assert_eq!(peek_u64(fd.as_fd(), OFF_TAIL), 48);

    // SAFETY: the previous producer is gone (forgotten, never used again).
    let mut tx2 = unsafe { BytesRing::force_attach_shm_producer(fd.as_fd()) }.unwrap();
    // Healing: `latest` repaired to the committed tail (the one guaranteed
    // record boundary)...
    assert_eq!(
        peek_u64(fd.as_fd(), OFF_LATEST),
        48,
        "latest must be repaired to the committed tail"
    );
    // ... and the declared frontier never regresses: an 8-byte record would
    // declare 56, but the floor keeps it at the dead producer's 64.
    tx2.push(b"");
    assert_eq!(
        peek_u64(fd.as_fd(), OFF_INTENT),
        64,
        "intent must stay monotonic across producer sessions"
    );
    // The live consumer sees exactly the new session: the empty record,
    // then ordinary content — never a fabricated or torn message.
    assert_eq!(&*rx.pop().unwrap(), b"");
    tx2.push(b"after");
    assert_eq!(&*rx.pop().unwrap(), b"after");
    // Once the new tail passes the floor, declarations move again.
    tx2.push(b"and-on");
    assert!(peek_u64(fd.as_fd(), OFF_INTENT) > 64);
    assert_eq!(&*rx.pop().unwrap(), b"and-on");

    // recover_shm: same operation, recovery name.
    std::mem::forget(tx2);
    // SAFETY: as above.
    let mut tx3 = unsafe { BytesRing::recover_shm(fd.as_fd()) }.unwrap();
    tx3.push(b"rec-ok");
    assert_eq!(&*rx.pop().unwrap(), b"rec-ok");
}

/// A lapped consumer facing a producer that died between its `latest` and
/// `tail` stores (`latest` is stored first, so it can point past the
/// committed tail — the never-healed state, no recovery attach here). The
/// reposition lands at the dead `latest` — past the committed tail — where
/// the availability check guarantees no read will ever run (no tail covers
/// the position). The load-bearing regression is the close-out: the
/// drained check must be `tail <= pos`, not equality — an equality check
/// never fires for a position stranded past the final tail, and the
/// consumer's drain would livelock on the dead ring instead of reporting
/// `Closed`.
#[test]
fn dead_producer_latest_past_tail_drains_to_closed() {
    let fd = memfd("bcast-latest-past-tail").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 64) }.unwrap();
    // The consumer joins at tail 0 and never pops: it will be lapped.
    // SAFETY: cooperating handles only.
    let mut rx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
    for seq in 0..5u64 {
        tx.push(&seq.to_le_bytes()); // 16-byte records: tail = 80 (one lap)
    }
    std::mem::forget(tx); // crash: lease held, counters as-is

    // Inject the mid-push death: the dead producer declared 16 more bytes
    // and stored `latest` for the never-committed record (8 bytes of wrap
    // padding: latest = 88 > tail = 80), then died before the tail store.
    poke_u64(fd.as_fd(), OFF_INTENT, 96);
    poke_u64(fd.as_fd(), OFF_LATEST, 88);
    assert_eq!(peek_u64(fd.as_fd(), OFF_TAIL), 80);
    // The producer is gone for good; mark the session closed (what its
    // graceful drop would have stored) so the drain can terminate.
    poke_u64(fd.as_fd(), OFF_CLOSED, 1);

    // The lap: the jump target is the dead producer's `latest` (88) — 8
    // declared-but-never-committed bytes past the tail. That is safe (no
    // read can run there: no tail ever covers the position) and the
    // accounting stays position-gap-free.
    match rx.try_pop() {
        Err(BytesPopError::Lagged { missed_bytes }) => assert_eq!(
            missed_bytes, 88,
            "reposition jumps to the dead latest (position accounting stays gap-free)"
        ),
        other => panic!("expected the lap, got {other:?}"),
    }
    // Position 88 is stranded PAST the final tail (80): only the `<=`
    // drained check reports Closed here — `==` would spin this drain
    // forever on the dead ring.
    assert_eq!(
        rx.pop().unwrap_err(),
        BytesPopError::Closed,
        "the drain must terminate on the dead ring"
    );
}

// ---------------------------------------------------------------------------
// 6. Closed across processes: graceful producer drop -> the child drains
//    then sees Closed; a new producer attach reopens the session.
// ---------------------------------------------------------------------------

#[test]
fn producer_drop_closes_across_processes_and_reattach_reopens() {
    const N: u64 = 100;
    let fd = memfd("bcast-closed").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 256) }.unwrap();
    let ready = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        // Drain everything published, then observe Closed.
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i));
        }
        assert_eq!(crx.pop(), Err(PopError::Closed));
    });

    ready.recv();
    for i in 0..N {
        tx.push(i);
    }
    drop(tx); // graceful close: sets the header closed word
    wait_child(pid);

    // Attaching a consumer to a CLOSED broadcast ring succeeds (mirroring
    // the heap subscribe contract — and unlike the gating SPMC attach): it
    // is born drained and pops Closed.
    // SAFETY: cooperating handles only.
    let mut late = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(late.try_pop(), Err(PopError::Closed));

    // A new producer attach re-opens the ring (shm Closed is
    // end-of-session): the very same consumer sees the new session.
    // SAFETY: the previous producer is gone (dropped).
    let mut tx2 = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }.unwrap();
    assert_eq!(late.try_pop(), Ok(None), "reopened: alive again");
    tx2.push(1);
    assert_eq!(late.pop(), Ok(1));
}

#[test]
fn bytes_producer_drop_closes() {
    let fd = memfd("bcast-bytes-closed").unwrap();
    // SAFETY: fresh private memfd.
    let mut tx = unsafe { BytesRing::create_shm(fd.as_fd(), 4096) }.unwrap();
    // SAFETY: cooperating handles only.
    let mut rx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
    tx.push(b"last");
    drop(tx);
    assert_eq!(&*rx.pop().unwrap(), b"last");
    assert_eq!(rx.pop(), Err(BytesPopError::Closed), "drained + closed");
}

// ---------------------------------------------------------------------------
// 7. Header validation: wrong-kind attaches rejected in every direction;
//    corrupt headers rejected; the producer lease conflicts.
// ---------------------------------------------------------------------------

#[test]
fn attach_validates_kinds_types_and_leases() {
    // A broadcast element ring rejects every other attach flavor.
    let fd = memfd("bcast-validate").unwrap();
    // SAFETY: fresh private memfd.
    let tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64) }.unwrap();

    // Producer role held: polite attach conflicts.
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }
        .err()
        .expect("attach must fail while the producer role is held");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    // SAFETY: cooperating handles only (all rejected before touching state).
    unsafe {
        // Wrong kinds on a broadcast element fd.
        assert!(BytesRing::attach_shm_consumer(fd.as_fd()).is_err());
        assert!(rust_rb::spmc::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        assert!(rust_rb::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        // Element size mismatch (unit_size records size_of::<T>).
        assert!(RingBuffer::<u32>::attach_shm_consumer(fd.as_fd()).is_err());
    }
    drop(tx);

    // Foreign fds into broadcast attaches: SPMC (kind 4) and SPSC (kind 2).
    let spmc_fd = memfd("bcast-validate-spmc").unwrap();
    // SAFETY: fresh private memfd.
    let _pair =
        unsafe { rust_rb::spmc::RingBuffer::<u64>::create_shm(spmc_fd.as_fd(), 64, 2) }.unwrap();
    let spsc_fd = memfd("bcast-validate-spsc").unwrap();
    // SAFETY: fresh private memfd.
    let _pair2 = unsafe { rust_rb::RingBuffer::<u64>::create_shm(spsc_fd.as_fd(), 64) }.unwrap();
    // SAFETY: cooperating handles only.
    unsafe {
        assert!(RingBuffer::<u64>::attach_shm_consumer(spmc_fd.as_fd()).is_err());
        assert!(RingBuffer::<u64>::attach_shm_consumer(spsc_fd.as_fd()).is_err());
        assert!(RingBuffer::<u64>::attach_shm_producer(spsc_fd.as_fd()).is_err());
        assert!(BytesRing::attach_shm_consumer(spmc_fd.as_fd()).is_err());
        // Cross-broadcast-kind: an element fd is not a byte fd and vice
        // versa.
        let bytes_fd = memfd("bcast-validate-bytes").unwrap();
        let _tx = BytesRing::create_shm(bytes_fd.as_fd(), 64).unwrap();
        assert!(RingBuffer::<u64>::attach_shm_consumer(bytes_fd.as_fd()).is_err());
    }
}

#[test]
fn corrupt_header_fields_are_rejected() {
    // Non-power-of-two capacity.
    let fd = memfd("bcast-corrupt-cap").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64) }.unwrap();
    }
    poke_u64(fd.as_fd(), OFF_CAPACITY, 3);
    // SAFETY: cooperating handles only (rejected before touching state).
    assert!(unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.is_err());

    // Slack at (or beyond) capacity violates the constructor invariant.
    let fd = memfd("bcast-corrupt-slack").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _tx = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64) }.unwrap();
    }
    poke_u64(fd.as_fd(), OFF_SLACK, 64);
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("slack >= capacity must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // The generation seqlock: an odd generation (mid-initialization) is
    // rejected rather than read as a chimera.
    let fd = memfd("bcast-odd-gen").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _tx = unsafe { BytesRing::create_shm(fd.as_fd(), 64) }.unwrap();
    }
    let gen = peek_u64(fd.as_fd(), OFF_GENERATION);
    poke_u64(fd.as_fd(), OFF_GENERATION, gen | 1);
    // SAFETY: cooperating handles only.
    let err = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("odd generation must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("being initialized"),
        "unexpected error: {err}"
    );

    // An explicit slack knob out of range is an error at create, too.
    let fd = memfd("bcast-slack-range").unwrap();
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::create_shm_with_slack(fd.as_fd(), 64, 64) }
        .err()
        .expect("slack >= capacity must be refused at create");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
