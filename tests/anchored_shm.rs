//! Shared-memory ANCHORED ring tests (feature `shm`, Linux) — the composed
//! machine over a mapped region: gating anchors through the SPMC consumer
//! table (per-slot lease + epoch|state control, compare-and-retire, table
//! reset on recover) and lossy observers over broadcast's lease-free
//! **PROT_READ** mappings.
//!
//! Mirrors `tests/spmc_shm.rs` (fork + pipe choreography, SIGKILL +
//! force-detach, header pokes) and `tests/broadcast_shm.rs` (the read-only
//! enforcement pair, intent/latest healing): the union of both suites over
//! kinds 9/10.
#![cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

use rust_rb::anchored::{Closed, PopError, RingBuffer, SubscribeError};
use rust_rb::anchored_bytes::{BytesRingBuffer, Closed as BytesClosed, PopError as BytesPopError};
use rust_rb::{memfd, YieldWait};

/// The default-strategy byte ring, spelled out where inference needs help
/// (associated functions on a generic type do not apply parameter defaults).
type BytesRing = BytesRingBuffer<YieldWait, YieldWait>;
/// The default-strategy element ring, same reason.
type ElemRing = RingBuffer<u64, YieldWait, YieldWait>;

// Header offsets (mirrors of src/shm.rs — corruption/healing pokes only).
const OFF_CAPACITY: usize = 16;
const OFF_MAX_ANCHORS: usize = 36;
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

/// Non-blocking reap: `Some(())` once `pid` exited (asserting a clean exit),
/// `None` while it is still running.
fn try_wait_child(pid: libc::pid_t) -> Option<()> {
    let mut status = 0;
    // SAFETY: valid pid we forked; `status` is a valid out-pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    if waited == 0 {
        return None;
    }
    assert_eq!(waited, pid, "waitpid failed");
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child failed: status {status:#x} (signaled: {})",
        libc::WIFSIGNALED(status)
    );
    Some(())
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

    /// Send a `u32` payload (the child reports its slot epoch this way — a
    /// pipe write of 4 bytes is atomic).
    fn send_u32(&self, v: u32) {
        let b = v.to_ne_bytes();
        // SAFETY: valid fd and buffer.
        assert_eq!(
            unsafe { libc::write(self.w.as_raw_fd(), b.as_ptr().cast(), 4) },
            4
        );
    }

    fn recv_u32(&self) -> u32 {
        let mut b = [0u8; 4];
        // SAFETY: valid fd and buffer.
        assert_eq!(
            unsafe { libc::read(self.r.as_raw_fd(), b.as_mut_ptr().cast(), 4) },
            4
        );
        u32::from_ne_bytes(b)
    }
}

/// Store a u32 into the region header at `off` through an independent
/// mapping (corruption injection for the validation tests).
fn poke_u32(fd: BorrowedFd<'_>, off: usize, val: u32) {
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
            .cast::<std::sync::atomic::AtomicU32>())
        .store(val, std::sync::atomic::Ordering::Release);
        libc::munmap(ptr, 4096);
    }
}

/// Store a u64 into the region header at `off` (see [`poke_u32`]).
fn poke_u64(fd: BorrowedFd<'_>, off: usize, val: u64) {
    // SAFETY: as for `poke_u32`.
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
    // SAFETY: as for `poke_u32`, read-only use.
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
// 1. Cross-process round trips: parent producer, forked anchor child +
//    forked observer child — both see everything (keeping up; capacity
//    covers the stream, so the observer cannot lose either).
// ---------------------------------------------------------------------------

#[test]
fn element_round_trip_anchor_and_observer_children() {
    const N: u64 = 1000;
    let fd = memfd("anch-elem-roundtrip").unwrap();
    // SAFETY: fresh private memfd; u64 is ShmItem + NoUninit.
    let (mut tx, mut rx) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), N as usize, 4) }.unwrap();
    assert!(tx.capacity() >= N as usize, "no loss possible in this test");
    let ready = pipe();

    let anchor_pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        ready.send();
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i), "anchors are lossless, content-exact");
        }
    });
    let observer_pid = fork_child(|| {
        // SAFETY: cooperating handles only (read-only mapping, no lease).
        let mut orx = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();
        ready.send();
        for i in 0..N {
            assert_eq!(orx.pop(), Ok(i), "kept-up observers are content-exact");
        }
        assert_eq!(orx.try_pop(), Ok(None), "drained but alive");
    });

    // Do not publish before both children have joined (the observer's join
    // point is the tail at attach).
    ready.recv();
    ready.recv();
    for i in 0..N {
        tx.push(i);
        assert_eq!(rx.pop(), Ok(i)); // keep the parent's anchor caught up
    }
    wait_child(anchor_pid);
    wait_child(observer_pid);

    // In-process subscribes off live shm handles: an observer over the
    // producer's read-write mapping, and a further anchor claiming a table
    // slot (the shm face of subscribe_anchor).
    let mut obs = tx.subscribe_observer();
    let mut rx2 = tx.subscribe_anchor().unwrap();
    tx.push(4242);
    assert_eq!(rx.pop(), Ok(4242));
    assert_eq!(rx2.pop(), Ok(4242));
    assert_eq!(obs.pop(), Ok(4242));
    // ... and siblings subscribed off an anchor handle work too.
    let mut obs2 = rx2.subscribe_observer();
    let mut rx3 = rx2.subscribe_anchor().unwrap();
    tx.push(4343);
    for a in [&mut rx, &mut rx2, &mut rx3] {
        assert_eq!(a.pop(), Ok(4343));
    }
    assert_eq!(obs2.pop(), Ok(4343));
}

#[test]
fn bytes_round_trip_anchor_and_observer_children() {
    const N: u64 = 1000;
    let fd = memfd("anch-bytes-roundtrip").unwrap();
    // 16-byte records * 1000 fits a 64 KiB ring: no loss possible.
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 65536, 4) }.unwrap();
    let ready = pipe();

    let anchor_pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { BytesRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        ready.send();
        for seq in 0..N {
            assert_eq!(&*crx.pop().unwrap(), &seq.to_le_bytes(), "content-exact");
        }
    });
    let observer_pid = fork_child(|| {
        // SAFETY: cooperating handles only (read-only mapping, no lease).
        let mut orx = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();
        ready.send();
        let mut out = Vec::new();
        for seq in 0..N {
            orx.pop_into(&mut out).unwrap();
            assert_eq!(&*out, &seq.to_le_bytes(), "content-exact");
        }
        assert_eq!(orx.try_pop(), Ok(None), "drained but alive");
    });

    ready.recv();
    ready.recv();
    for seq in 0..N {
        tx.push(&seq.to_le_bytes());
        assert_eq!(&*rx.pop().unwrap(), &seq.to_le_bytes());
    }
    wait_child(anchor_pid);
    wait_child(observer_pid);

    // In-process subscribes off live shm handles.
    let mut obs = tx.subscribe_observer();
    let mut rx2 = tx.subscribe_anchor().unwrap();
    tx.push(b"post-join");
    assert_eq!(&*rx.pop().unwrap(), b"post-join");
    assert_eq!(&*rx2.pop().unwrap(), b"post-join");
    assert_eq!(&*obs.pop().unwrap(), b"post-join");
}

// ---------------------------------------------------------------------------
// 2. Cross-process gating: an idle anchor child gates the parent producer;
//    observers drain to the frozen tail with ZERO spurious Lagged — the
//    §9.3/F3 regression cross-process (a stalled producer keeps
//    intent == tail, so an observer behind a frozen tail always passes its
//    window check) — then release -> resume.
// ---------------------------------------------------------------------------

#[test]
fn idle_anchor_child_gates_and_observer_drains_frozen_tail_elems() {
    const CAPACITY: u64 = 8;
    const EXTRA: u64 = 24;
    let fd = memfd("anch-elem-gating").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY as usize, 2) }.unwrap();
    drop(rx0); // the child is the only anchor
    let ready = pipe();
    let go = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        ready.send();
        go.recv(); // stall (do not consume) until the parent verified the gate
        for i in 0..CAPACITY + EXTRA {
            assert_eq!(crx.pop(), Ok(i));
        }
    });

    ready.recv();
    // An observer joined before anything was published.
    // SAFETY: cooperating handles only (read-only mapping).
    let mut obs = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();

    // The child is attached but stalled: exactly CAPACITY pushes fit.
    for i in 0..CAPACITY {
        assert!(tx.try_push(i).is_ok(), "push {i} must fit");
    }
    assert!(
        tx.try_push(u64::MAX).is_err(),
        "a stalled anchor must gate the producer"
    );

    // The frozen frontier: the observer drains everything published with
    // zero spurious Lagged (the slots are stable while the producer stalls).
    for i in 0..CAPACITY {
        assert_eq!(obs.pop(), Ok(i), "no spurious Lagged against a frozen tail");
    }
    assert_eq!(obs.try_pop(), Ok(None));

    // Release: the child resumes and blocking pushes proceed as it drains.
    go.send();
    for i in 0..EXTRA {
        tx.push(CAPACITY + i);
    }
    wait_child(pid);
}

#[test]
fn idle_anchor_child_gates_and_observer_drains_frozen_tail_bytes() {
    // capacity 128, 16-byte records: exactly 8 records fill the ring.
    const RECORDS: u64 = 8;
    const EXTRA: u64 = 24;
    let fd = memfd("anch-bytes-gating").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 128, 2) }.unwrap();
    drop(rx0); // the child is the only anchor
    let ready = pipe();
    let go = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { BytesRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        ready.send();
        go.recv(); // stall until the parent verified the gate
        for seq in 0..RECORDS + EXTRA {
            assert_eq!(&*crx.pop().unwrap(), &seq.to_le_bytes());
        }
    });

    ready.recv();
    // SAFETY: cooperating handles only (read-only mapping).
    let mut obs = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();

    for seq in 0..RECORDS {
        assert!(tx.try_push(&seq.to_le_bytes()), "record {seq} must fit");
    }
    assert!(
        !tx.try_push(&u64::MAX.to_le_bytes()),
        "a stalled anchor must gate the producer"
    );

    // THE F3 regression, cross-process: the gated producer never declared
    // intent for the blocked push (gate strictly before intent), so
    // intent == tail and the observer's window checks pass on every intact
    // record below the frozen tail — zero spurious Lagged{missed_bytes: 0}.
    let mut out = Vec::new();
    for seq in 0..RECORDS {
        assert_eq!(
            obs.pop_into(&mut out),
            Ok(()),
            "no spurious Lagged against a gated producer's frozen frontier"
        );
        assert_eq!(&*out, &seq.to_le_bytes());
    }
    assert_eq!(obs.try_pop(), Ok(None));

    go.send();
    for seq in 0..EXTRA {
        tx.push(&(RECORDS + seq).to_le_bytes());
    }
    wait_child(pid);
}

// ---------------------------------------------------------------------------
// 3. THE PROT_READ enforcement pair: a forked observer runs the FULL consume
//    cycle over a read-only mapping — blocking pop, the Lagged reposition
//    path, lag(), skip_to_latest(), a sibling subscribe, and the Closed
//    drain. Any store anywhere in that path segfaults the child; the parent
//    asserts a clean exit. The negative control proves the protection is
//    real.
// ---------------------------------------------------------------------------

#[test]
fn prot_read_full_observer_cycle_element() {
    let fd = memfd("anch-protread-elem").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8, 2) }.unwrap();
    drop(rx0); // zero anchors: the producer free-runs, so laps can happen
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only. The mapping is PROT_READ.
        let mut orx = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();
        c2p.send();
        // Blocking pop path (kept-up phase; includes the SelfTimed wait).
        for i in 0..4 {
            assert_eq!(orx.pop(), Ok(i));
        }
        c2p.send();
        p2c.recv(); // parent lapped us
                    // Lagged reposition path, lag, skip_to_latest, empty try_pop.
        assert!(matches!(orx.pop(), Err(PopError::Lagged { .. })));
        let _ = orx.lag();
        let _ = orx.skip_to_latest();
        assert_eq!(orx.try_pop(), Ok(None));
        // A read-only sibling via subscribe_observer.
        let mut sib = orx.subscribe_observer();
        assert_eq!(sib.try_pop(), Ok(None));
        c2p.send();
        // Closed drain path.
        assert_eq!(orx.pop(), Err(PopError::Closed));
        assert_eq!(sib.pop(), Err(PopError::Closed));
        // Observer drop over the read-only mapping: munmap only.
    });

    c2p.recv();
    for i in 0..4u64 {
        tx.push(i);
    }
    c2p.recv();
    for i in 4..104u64 {
        tx.push(i); // free-run: the idle observer cannot gate this
    }
    p2c.send();
    c2p.recv();
    drop(tx);
    // Clean exit == not one store happened on the entire observer path.
    wait_child(pid);
}

#[test]
fn prot_read_full_observer_cycle_bytes() {
    let fd = memfd("anch-protread-bytes").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 64, 2) }.unwrap();
    drop(rx0); // zero anchors: free-run
    let c2p = pipe();
    let p2c = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only. The mapping is PROT_READ.
        let mut orx = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();
        c2p.send();
        let mut out = Vec::new();
        for seq in 0..4u64 {
            orx.pop_into(&mut out).unwrap();
            assert_eq!(&*out, &seq.to_le_bytes());
        }
        c2p.send();
        p2c.recv(); // parent lapped us
        assert!(matches!(orx.pop(), Err(BytesPopError::Lagged { .. })));
        let _ = orx.lag();
        let _ = orx.skip_to_latest();
        assert_eq!(orx.try_pop(), Ok(None));
        let mut sib = orx.subscribe_observer();
        assert!(matches!(sib.try_pop_into(&mut out), Ok(false)));
        c2p.send();
        assert_eq!(orx.pop(), Err(BytesPopError::Closed));
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
/// very same region DOES segfault — proving the enforcement above is real,
/// not a mapping that silently fell back to read-write.
#[test]
fn prot_read_negative_control_write_faults() {
    let fd = memfd("anch-protread-negative").unwrap();
    // SAFETY: fresh private memfd.
    let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8, 2) }.unwrap();

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
            // The deliberate observer-side sin: one store.
            std::ptr::write_volatile(ptr.cast::<u8>(), 1);
        }
    });
    wait_child_segv(pid);
}

// ---------------------------------------------------------------------------
// 4. Anchor crash: SIGKILLed anchor child -> the producer stays gated ->
//    force_detach_anchor(slot, epoch) un-gates it; a stale epoch is refused;
//    recover_shm resets the table (retired slots become issuable again).
// ---------------------------------------------------------------------------

#[test]
fn force_detach_dead_anchor_and_recover() {
    const CAPACITY: u64 = 8;
    let fd = memfd("anch-force-detach").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY as usize, 4) }.unwrap();
    drop(rx0); // slot 0 freed; the child will claim it
    let ready = pipe();
    let consumed = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        let (slot, epoch) = crx.shm_slot_epoch().unwrap();
        assert_eq!(slot, 0);
        // Report the occupancy epoch: the parent's compare-and-retire proof.
        ready.send_u32(epoch);
        for i in 0..3u64 {
            assert_eq!(crx.pop(), Ok(i));
        }
        consumed.send();
        // Stall forever mid-consumption; the parent SIGKILLs us here.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    });

    let child_epoch = ready.recv_u32();
    for i in 0..3u64 {
        tx.push(i);
    }
    consumed.recv();
    // The child (cursor at 3) now stalls: the ring fills and gates.
    for i in 0..CAPACITY {
        tx.push(3 + i);
    }
    assert!(tx.try_push(u64::MAX).is_err(), "stalled child must gate");

    // Kill the child mid-consumption. The slot stays ACTIVE (a crash runs no
    // destructors); the producer stays gated.
    // SAFETY: our own child pid.
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut status = 0;
    // SAFETY: valid pid; status out-pointer.
    unsafe { libc::waitpid(pid, &mut status, 0) };
    assert!(
        tx.try_push(u64::MAX).is_err(),
        "a dead anchor's slot still gates"
    );

    // A stale epoch must NOT retire the slot: compare-and-retire is the
    // proof the caller diagnosed THIS occupancy dead, not a successor's.
    // SAFETY: same trust register as below; the stale CAS fails harmlessly.
    let err = unsafe { ElemRing::force_detach_anchor(fd.as_fd(), 0, child_epoch.wrapping_add(1)) }
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        tx.try_push(u64::MAX).is_err(),
        "an epoch-mismatched force_detach must not retire the slot"
    );

    // The caller (who reaped the child) asserts its death: retire the slot.
    // SAFETY: the previous holder of slot 0 is dead (SIGKILLed and reaped).
    unsafe { ElemRing::force_detach_anchor(fd.as_fd(), 0, child_epoch) }.unwrap();
    assert!(
        tx.try_push(100).is_ok(),
        "retiring the slot un-gates the producer"
    );
    assert_eq!(tx.anchor_count(), 0);

    // A retired slot is never re-issued: a new anchor gets a DIFFERENT slot.
    // SAFETY: cooperating handles only.
    let rx_new = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
    assert_eq!(rx_new.shm_slot(), Some(1), "retired slot 0 must be skipped");
    drop(rx_new);

    // recover_shm resets the whole table: slot 0 becomes issuable again.
    drop(tx);
    // SAFETY: all previous holders are gone (dropped or dead).
    let (mut tx2, mut rx2) = unsafe { RingBuffer::<u64>::recover_shm(fd.as_fd()) }.unwrap();
    assert_eq!(rx2.shm_slot(), Some(0), "recover_shm must reset the table");
    tx2.push(7);
    assert_eq!(rx2.pop(), Ok(7));
}

/// The byte ring shares the table machinery: SIGKILLed anchor gates, the
/// compare-and-retire un-gates, and recovery resumes at a record boundary.
#[test]
fn force_detach_dead_anchor_bytes() {
    // capacity 128: 16-byte records, 8 per ring.
    let fd = memfd("anch-bytes-force-detach").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 128, 4) }.unwrap();
    drop(rx0);
    let ready = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let crx = unsafe { BytesRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        let (slot, epoch) = crx.shm_slot_epoch().unwrap();
        assert_eq!(slot, 0);
        ready.send_u32(epoch);
        // Stall forever without consuming; the parent SIGKILLs us here.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    });

    let child_epoch = ready.recv_u32();
    for seq in 0..8u64 {
        assert!(tx.try_push(&seq.to_le_bytes()), "record {seq} must fit");
    }
    assert!(!tx.try_push(&[0; 8]), "idle child must gate");

    // SAFETY: our own child pid.
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut status = 0;
    // SAFETY: valid pid; status out-pointer.
    unsafe { libc::waitpid(pid, &mut status, 0) };

    // Stale epoch refused, then the real retire un-gates.
    // SAFETY: trust register as documented; the stale CAS fails harmlessly.
    let err = unsafe { BytesRing::force_detach_anchor(fd.as_fd(), 0, child_epoch.wrapping_add(1)) }
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    // SAFETY: the previous holder of slot 0 is dead (SIGKILLed and reaped).
    unsafe { BytesRing::force_detach_anchor(fd.as_fd(), 0, child_epoch) }.unwrap();
    assert!(tx.try_push(&100u64.to_le_bytes()), "retired slot un-gates");

    // recover_shm: table reset. The dead anchor's cursor (0) now lags MORE
    // than one capacity behind the tail (the post-retire push moved it to
    // 144 > 128) — an implausible leftover the producer had already stopped
    // honoring, so the reset ignores it and the returned anchor resumes at
    // the tail (its bytes were already overwritten; a resume there would
    // hand out destroyed frames).
    drop(tx);
    // SAFETY: all previous holders are gone (dropped or dead).
    let (mut tx2, mut rx2) = unsafe { BytesRingBuffer::recover_shm(fd.as_fd()) }.unwrap();
    assert_eq!(rx2.shm_slot(), Some(0), "recover_shm must reset the table");
    tx2.push(b"onward");
    assert_eq!(
        &*rx2.pop().unwrap(),
        b"onward",
        "the recovered session flows"
    );
}

/// Recovery keeps published-but-unconsumed records when the dead anchor's
/// lag is within one capacity (at-least-once, mirroring the SPMC recover
/// contract): the returned anchor resumes at the slowest registered cursor,
/// which is always a record boundary.
#[test]
fn bytes_recover_redelivers_from_slowest_dead_anchor() {
    let fd = memfd("anch-bytes-recover-resume").unwrap();
    // capacity 128: 16-byte records.
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 128, 2) }.unwrap();
    for seq in 0..4u64 {
        tx.push(&seq.to_le_bytes()); // tail = 64
    }
    assert_eq!(&*rx.pop().unwrap(), &0u64.to_le_bytes()); // cursor -> 16
                                                          // Simulated crash of both sides: no destructors run.
    std::mem::forget(tx);
    std::mem::forget(rx);

    // SAFETY: both previous holders are gone (forgotten, never used again).
    let (mut tx2, mut rx2) = unsafe { BytesRingBuffer::recover_shm(fd.as_fd()) }.unwrap();
    // The dead anchor's published cursor (a record boundary) is the resume
    // point: at-least-once redelivery of records 1..4.
    for seq in 1..4u64 {
        assert_eq!(&*rx2.pop().unwrap(), &seq.to_le_bytes(), "redelivered");
    }
    tx2.push(b"onward");
    assert_eq!(&*rx2.pop().unwrap(), b"onward");
}

// ---------------------------------------------------------------------------
// 5. Producer crash (bytes): mid-push death simulated via header pokes ->
//    attach heals `latest` and carries the intent floor; the observer never
//    accepts torn bytes; the dead-producer drained-Closed regression (the
//    `<=` check).
// ---------------------------------------------------------------------------

#[test]
fn bytes_producer_crash_attach_heals_intent_and_latest() {
    let fd = memfd("anch-bytes-heal").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 64, 2) }.unwrap();
    drop(rx0); // zero anchors: the healing story is observer-facing
    for seq in 0..3u64 {
        tx.push(&seq.to_le_bytes()); // 16-byte records: tail = 48
    }
    std::mem::forget(tx); // crash: lease held, counters as-is

    // An observer attached against the crashed ring: published records stay
    // drainable (it joins at tail 48, so assert emptiness + the new-session
    // flow).
    // SAFETY: cooperating handles only.
    let mut rx = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();
    assert_eq!(rx.try_pop(), Ok(None));

    // Simulate the mid-push death state: the dead producer declared 16 more
    // bytes (intent 64) and had already stored a `latest` pointing into the
    // never-committed span.
    poke_u64(fd.as_fd(), OFF_INTENT, 64);
    poke_u64(fd.as_fd(), OFF_LATEST, 56);
    assert_eq!(peek_u64(fd.as_fd(), OFF_TAIL), 48);

    // The dead producer's lease blocks a polite attach...
    // SAFETY: cooperating handles only.
    let err = unsafe { BytesRing::attach_shm_producer(fd.as_fd()) }
        .err()
        .expect("held lease must refuse polite attach");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    // ... and force-attach is the caller-asserts-death recovery.
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
    // declare 56, but the floor keeps it at the dead producer's 64 — the
    // §9.3 order composes with the floor as declared = max(new_tail, floor)
    // at step (2), strictly after the gate.
    tx2.push(b"");
    assert_eq!(
        peek_u64(fd.as_fd(), OFF_INTENT),
        64,
        "intent must stay monotonic across producer sessions"
    );
    // The live observer sees exactly the new session: the empty record, then
    // ordinary content — never a fabricated or torn message.
    assert_eq!(&*rx.pop().unwrap(), b"");
    tx2.push(b"after");
    assert_eq!(&*rx.pop().unwrap(), b"after");
    // Once the new tail passes the floor, declarations move again.
    tx2.push(b"and-on");
    assert!(peek_u64(fd.as_fd(), OFF_INTENT) > 64);
    assert_eq!(&*rx.pop().unwrap(), b"and-on");

    // recover_shm heals the same way (and returns a fresh anchor pair).
    std::mem::forget(tx2);
    // SAFETY: as above.
    let (mut tx3, mut rx3) = unsafe { BytesRingBuffer::recover_shm(fd.as_fd()) }.unwrap();
    tx3.push(b"rec-ok");
    assert_eq!(&*rx3.pop().unwrap(), b"rec-ok");
    assert_eq!(&*rx.pop().unwrap(), b"rec-ok");
}

/// A lapped observer facing a producer that died between its `latest` and
/// `tail` stores (never healed — no recovery attach here). The reposition
/// lands at the dead `latest`, past the committed tail, where no read can
/// ever run; the load-bearing regression is the close-out: the drained
/// check must be `tail <= pos`, not equality, or the drain livelocks on the
/// dead ring instead of reporting `Closed`.
#[test]
fn bytes_dead_producer_latest_past_tail_drains_to_closed() {
    let fd = memfd("anch-latest-past-tail").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 64, 2) }.unwrap();
    drop(rx0); // zero anchors: free-run laps the observer below
               // The observer joins at tail 0 and never pops: it will be lapped.
               // SAFETY: cooperating handles only.
    let mut rx = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();
    for seq in 0..5u64 {
        tx.push(&seq.to_le_bytes()); // 16-byte records: tail = 80 (one lap)
    }
    std::mem::forget(tx); // crash: lease held, counters as-is

    // Inject the mid-push death: 16 more bytes declared, `latest` stored for
    // the never-committed record (8 bytes of wrap padding: latest = 88 >
    // tail = 80), death before the tail store. Mark the session closed (what
    // a graceful drop would have stored) so the drain can terminate.
    poke_u64(fd.as_fd(), OFF_INTENT, 96);
    poke_u64(fd.as_fd(), OFF_LATEST, 88);
    assert_eq!(peek_u64(fd.as_fd(), OFF_TAIL), 80);
    poke_u64(fd.as_fd(), OFF_CLOSED, 1);

    // The lap: the jump target is the dead `latest` (88) — safe (no tail
    // ever covers the position, so no read runs there) and gap-free.
    match rx.try_pop() {
        Err(BytesPopError::Lagged { missed_bytes }) => assert_eq!(
            missed_bytes, 88,
            "reposition jumps to the dead latest (accounting stays gap-free)"
        ),
        other => panic!("expected the lap, got {other:?}"),
    }
    // Position 88 is stranded PAST the final tail (80): only the `<=`
    // drained check reports Closed here — `==` would spin forever.
    assert_eq!(
        rx.pop().unwrap_err(),
        BytesPopError::Closed,
        "the drain must terminate on the dead ring"
    );
}

/// Element-ring producer crash: no healing needed (slot seqlocks self-heal);
/// force-attach resumes exactly after the last published message and a
/// keeping-up anchor child sees the sessions seamlessly.
#[test]
fn element_producer_crash_force_attach_resumes() {
    let fd = memfd("anch-elem-recover").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 2) }.unwrap();
    drop(rx0);
    for i in 0..10u64 {
        tx.push(i);
    }
    std::mem::forget(tx); // crash: lease held, closed never set

    // SAFETY: cooperating handles only; a free table slot exists.
    let mut rx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
    assert_eq!(rx.try_pop(), Ok(None), "anchor joins at the committed tail");

    // SAFETY: the previous producer is gone (forgotten, never used again).
    let mut tx2 = unsafe { ElemRing::force_attach_shm_producer(fd.as_fd()) }.unwrap();
    assert_eq!(tx2.tail(), 10, "resume exactly after the last publish");
    for i in 10..20u64 {
        tx2.push(i);
    }
    for i in 10..20u64 {
        assert_eq!(rx.pop(), Ok(i), "the new session flows seamlessly");
    }
}

// ---------------------------------------------------------------------------
// 6. Zero-anchor free-run cross-process with observers; an anchor joins
//    mid-free-run and is content-exact from its join point — the §9.6
//    induction cross-process.
// ---------------------------------------------------------------------------

#[test]
fn zero_anchor_free_run_and_anchor_joins_mid_run() {
    const JOIN_POPS: u64 = 2000;
    const OBSERVER_POPS: u64 = 500;
    let fd = memfd("anch-free-run-join").unwrap();
    // Small capacity so the free-running producer laps constantly.
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 16, 2) }.unwrap();
    drop(rx0); // zero anchors: free-run

    // A lossy observer riding the free-run: every accepted value is exact
    // and strictly increasing; laps are allowed (and expected).
    let observer_pid = fork_child(|| {
        // SAFETY: cooperating handles only (read-only mapping).
        let mut orx = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();
        let mut last: Option<u64> = None;
        let mut accepted = 0u64;
        while accepted < OBSERVER_POPS {
            match orx.pop() {
                Ok(v) => {
                    if let Some(prev) = last {
                        assert!(v > prev, "accepted values are exact, in order");
                    }
                    last = Some(v);
                    accepted += 1;
                }
                Err(PopError::Lagged { missed }) => assert!(missed > 0, "laps are exact"),
                Err(PopError::Closed) => panic!("ring must not close during the run"),
            }
        }
    });

    // The §9.6 join: an anchor attaches WHILE the producer free-runs. Its
    // first pop is its join point; from there the stream must be strictly
    // consecutive — unvalidated anchor reads are only sound if the induction
    // holds cross-process.
    let anchor_pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
        let first = crx.pop().unwrap();
        for k in 1..JOIN_POPS {
            assert_eq!(crx.pop(), Ok(first + k), "content-exact from the join");
        }
    });

    // Free-run until both children finish (the join gates us naturally once
    // the anchor lands; before that, pushes never block).
    let mut i = 0u64;
    let mut observer_done = false;
    let mut anchor_done = false;
    while !(observer_done && anchor_done) {
        tx.push(i);
        i += 1;
        if i % 256 == 0 {
            observer_done = observer_done || try_wait_child(observer_pid).is_some();
            anchor_done = anchor_done || try_wait_child(anchor_pid).is_some();
        }
    }
}

// ---------------------------------------------------------------------------
// 7. Kind/geometry validation: wrong-kind attaches rejected in every
//    direction; table geometry mismatches rejected; the generation seqlock
//    and the closed/full attach contracts hold.
// ---------------------------------------------------------------------------

#[test]
fn attach_validates_kinds_types_and_leases() {
    let fd = memfd("anch-validate").unwrap();
    // SAFETY: fresh private memfd.
    let (tx, _rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();

    // Producer role held: polite attach conflicts.
    // SAFETY: cooperating handles only.
    let err = unsafe { ElemRing::attach_shm_producer(fd.as_fd()) }
        .err()
        .expect("attach must fail while the producer role is held");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    // Wrong kinds on an anchored element fd: every other family refuses.
    // SAFETY: cooperating handles only (all rejected before touching state).
    unsafe {
        assert!(BytesRing::attach_shm_observer(fd.as_fd()).is_err());
        assert!(BytesRing::attach_shm_anchor(fd.as_fd()).is_err());
        assert!(rust_rb::spmc::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        assert!(rust_rb::broadcast::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        assert!(rust_rb::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        // Element size mismatch (unit_size records size_of::<T>).
        assert!(RingBuffer::<u32, YieldWait, YieldWait>::attach_shm_anchor(fd.as_fd()).is_err());
    }
    drop(tx);

    // Foreign fds into anchored attaches: SPMC (kind 4), broadcast (kind 5),
    // SPSC (kind 2), and the sibling anchored kind (9 vs 10).
    let spmc_fd = memfd("anch-validate-spmc").unwrap();
    // SAFETY: fresh private memfd.
    let _spmc =
        unsafe { rust_rb::spmc::RingBuffer::<u64>::create_shm(spmc_fd.as_fd(), 64, 2) }.unwrap();
    let bcast_fd = memfd("anch-validate-bcast").unwrap();
    // SAFETY: fresh private memfd.
    let _bcast =
        unsafe { rust_rb::broadcast::RingBuffer::<u64>::create_shm(bcast_fd.as_fd(), 64) }.unwrap();
    let bytes_fd = memfd("anch-validate-bytes").unwrap();
    // SAFETY: fresh private memfd.
    let _bytes = unsafe { BytesRingBuffer::create_shm(bytes_fd.as_fd(), 64, 2) }.unwrap();
    // SAFETY: cooperating handles only.
    unsafe {
        assert!(ElemRing::attach_shm_anchor(spmc_fd.as_fd()).is_err());
        assert!(ElemRing::attach_shm_observer(bcast_fd.as_fd()).is_err());
        assert!(ElemRing::attach_shm_anchor(bytes_fd.as_fd()).is_err());
        assert!(ElemRing::attach_shm_observer(bytes_fd.as_fd()).is_err());
        assert!(BytesRing::attach_shm_anchor(spmc_fd.as_fd()).is_err());
    }
}

#[test]
fn corrupt_header_fields_are_rejected() {
    // max_anchors = 0 is corrupt table geometry.
    let fd = memfd("anch-corrupt-ma").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    poke_u32(fd.as_fd(), OFF_MAX_ANCHORS, 0);
    // SAFETY: cooperating handles only (rejected before touching state).
    let err = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }
        .err()
        .expect("max_anchors = 0 must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // Non-power-of-two capacity is corrupt.
    let fd = memfd("anch-corrupt-cap").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    poke_u64(fd.as_fd(), OFF_CAPACITY, 3);
    // SAFETY: cooperating handles only.
    assert!(unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.is_err());

    // Slack at (or beyond) capacity violates the constructor invariant
    // (element kind only — the byte kind has no slack knob).
    let fd = memfd("anch-corrupt-slack").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    poke_u64(fd.as_fd(), OFF_SLACK, 64);
    // SAFETY: cooperating handles only.
    let err = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }
        .err()
        .expect("slack >= capacity must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // The generation seqlock: an odd generation (mid-initialization) is
    // rejected rather than read as a chimera.
    let fd = memfd("anch-odd-gen").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 64, 2) }.unwrap();
    }
    let gen = peek_u64(fd.as_fd(), OFF_GENERATION);
    poke_u64(fd.as_fd(), OFF_GENERATION, gen | 1);
    // SAFETY: cooperating handles only.
    let err = unsafe { BytesRing::attach_shm_anchor(fd.as_fd()) }
        .err()
        .expect("odd generation must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("being initialized"),
        "unexpected error: {err}"
    );

    // An explicit slack knob out of range is an error at create, too.
    let fd = memfd("anch-slack-range").unwrap();
    // SAFETY: cooperating handles only.
    let err = unsafe { ElemRing::create_shm_with_slack(fd.as_fd(), 64, 4, 64) }
        .err()
        .expect("slack >= capacity must be refused at create");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn table_full_closed_and_detach_contracts() {
    let fd = memfd("anch-table-full").unwrap();
    // max_anchors = 2: the initial anchor occupies slot 0.
    // SAFETY: fresh private memfd.
    let (tx, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8, 2) }.unwrap();
    assert_eq!(rx0.shm_slot(), Some(0));

    // SAFETY: cooperating handles only.
    let rx1 = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
    assert_eq!(rx1.shm_slot(), Some(1));

    // Table full: attach reports AddrInUse, subscribe_anchor reports Full.
    // SAFETY: cooperating handles only.
    let err = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }
        .err()
        .expect("full table must refuse attach");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    assert!(matches!(tx.subscribe_anchor(), Err(SubscribeError::Full)));
    // Observers are unbounded regardless.
    // SAFETY: cooperating handles only.
    let _obs = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();

    // Graceful detach frees the slot; reattach claims it again.
    drop(rx1);
    // SAFETY: cooperating handles only.
    let rx1b = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
    assert_eq!(rx1b.shm_slot(), Some(1), "a freed slot is reissued");
    drop(rx1b);
    drop(rx0);

    // Closed contract: dropping the producer closes the session; anchors
    // drain-then-Closed, new anchor attaches are refused, observer attaches
    // succeed (born drained) — and a producer re-attach reopens.
    let mut tx = tx;
    tx.push(1);
    // SAFETY: cooperating handles only.
    let mut rx = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }.unwrap();
    drop(tx); // graceful close
    assert_eq!(rx.pop(), Err(Closed), "joined at tail: born drained");
    // SAFETY: cooperating handles only.
    let err = unsafe { ElemRing::attach_shm_anchor(fd.as_fd()) }
        .err()
        .expect("anchor attach on a closed ring must be refused");
    assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    // SAFETY: cooperating handles only.
    let mut late_obs = unsafe { ElemRing::attach_shm_observer(fd.as_fd()) }.unwrap();
    assert_eq!(late_obs.try_pop(), Err(PopError::Closed));

    // Reopen: shm Closed is end-of-session, both roles see the new session.
    // SAFETY: the previous producer is gone (dropped).
    let mut tx2 = unsafe { ElemRing::attach_shm_producer(fd.as_fd()) }.unwrap();
    assert_eq!(late_obs.try_pop(), Ok(None), "reopened: alive again");
    assert_eq!(rx.try_pop(), Ok(None), "the anchor sees the reopen too");
    tx2.push(2);
    assert_eq!(rx.pop(), Ok(2));
    assert_eq!(late_obs.pop(), Ok(2));
}

#[test]
fn bytes_closed_contract() {
    let fd = memfd("anch-bytes-closed").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096, 2) }.unwrap();
    // SAFETY: cooperating handles only.
    let mut obs = unsafe { BytesRing::attach_shm_observer(fd.as_fd()) }.unwrap();
    tx.push(b"last");
    drop(tx);
    assert_eq!(&*rx.pop().unwrap(), b"last");
    assert!(
        matches!(rx.pop(), Err(BytesClosed)),
        "anchor: drained + closed"
    );
    assert_eq!(&*obs.pop().unwrap(), b"last");
    assert_eq!(
        obs.pop(),
        Err(BytesPopError::Closed),
        "observer: drained + closed"
    );
}

// ---------------------------------------------------------------------------
// Recover must never PANIC when a concurrent attach races its table reset.
//
// A `subscribe`/`attach` whose `claim_table_slot` CAS lands between recover's
// `reset_gate_table` and its own claim grabs the just-freed slot; its
// post-claim generation re-check then fails and `release_table_claim` leaves
// the slot ACTIVE-but-ownerless (a phantom). On a single-slot table that
// phantom used to leave recover with no free slot and a `.expect` panic.
// The fix re-frees (reset also clears phantoms) and retries, and falls back
// to an `AddrInUse` error under a sustained storm — never a panic. This test
// hammers the window; it cannot false-fail (the fixed path is panic-free by
// construction), it only guards against the panic returning.
// ---------------------------------------------------------------------------

#[test]
fn recover_never_panics_racing_concurrent_attach() {
    use std::sync::atomic::{AtomicBool, Ordering as O};
    use std::sync::Arc;

    let fd = memfd("anch-recover-race").unwrap();
    // A single anchor slot maximizes the odds that a racing attach steals the
    // recoverer's only slot — the case that used to panic.
    // SAFETY: fresh private memfd.
    let (tx0, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 16, 1) }.unwrap();
    drop(rx0);
    drop(tx0);

    let raw = fd.as_raw_fd();
    let stop = Arc::new(AtomicBool::new(false));

    // Two attacker threads spin attach/detach on the shared fd.
    let attackers: Vec<_> = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                // SAFETY: `raw` names the live memfd the parent keeps open for
                // the whole test; each attach makes its own fresh handle.
                let borrowed = unsafe { BorrowedFd::borrow_raw(raw) };
                while !stop.load(O::Relaxed) {
                    if let Ok(a) = unsafe { ElemRing::attach_shm_anchor(borrowed) } {
                        drop(a);
                    }
                }
            })
        })
        .collect();

    // The recoverer loops; every outcome must be Ok or the specific
    // AddrInUse fallback — reaching either proves no panic fired.
    for _ in 0..3000 {
        // SAFETY: cooperating handles only; recover force-claims the producer.
        match unsafe { RingBuffer::<u64>::recover_shm(fd.as_fd()) } {
            Ok((tx, rx)) => {
                drop(tx);
                drop(rx);
            }
            Err(e) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::AddrInUse,
                "recover may only fall back to AddrInUse, never panic or error otherwise"
            ),
        }
    }

    stop.store(true, O::Relaxed);
    for a in attackers {
        a.join().unwrap();
    }
}
