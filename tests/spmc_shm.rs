//! Shared-memory GATING SPMC ring tests (feature `shm`, Linux).
//!
//! Mirrors `tests/shm.rs`: same-process double mappings catch
//! absolute-pointer assumptions; `fork`ed children exercise real
//! cross-address-space consumers (attach fresh handles in the child — never
//! use inherited ones). The consumer-table protocol (slot claim/retire,
//! zombie blast radius, recovery reset) is exercised end to end.
#![cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]

use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

use rust_rb::memfd;
use rust_rb::spmc::{RingBuffer, SubscribeError};
use rust_rb::spmc_bytes::BytesRingBuffer;
use rust_rb::YieldWait;

/// The default-strategy byte ring, spelled out where inference needs help
/// (associated functions on a generic type do not apply parameter defaults).
type BytesRing = BytesRingBuffer<YieldWait, YieldWait>;

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

/// Reap `pid` and assert it exited cleanly (status 0).
fn wait_child(pid: libc::pid_t) {
    let mut status = 0;
    // SAFETY: valid pid we forked; `status` is a valid out-pointer.
    let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
    assert_eq!(waited, pid, "waitpid failed");
    assert!(
        libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
        "child failed: status {status:#x}"
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
            libc::PROT_READ | libc::PROT_WRITE,
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
// 1. Same-process round trips (create + attach through a second mapping).
// ---------------------------------------------------------------------------

#[test]
fn element_ring_round_trips_in_shm() {
    let fd = memfd("spmc-elem-roundtrip").unwrap();
    // SAFETY: fresh private memfd; u64 is ShmItem.
    let (mut tx, mut rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 1024, 4) }.unwrap();

    for i in 0..10_000u64 {
        tx.push(i);
        assert_eq!(rx.pop(), Ok(i));
    }
    assert_eq!(rx.try_pop(), Ok(None));

    // A second consumer attached through an independent mapping sees only
    // messages published after its join, and all of them.
    // SAFETY: cooperating handles only.
    let mut rx2 = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    for i in 0..100u64 {
        tx.push(i);
    }
    for i in 0..100u64 {
        assert_eq!(rx.pop(), Ok(i));
        assert_eq!(rx2.pop(), Ok(i));
    }
    assert_eq!(rx2.try_pop(), Ok(None));
}

#[test]
fn bytes_ring_round_trips_in_shm() {
    let fd = memfd("spmc-bytes-roundtrip").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096, 4) }.unwrap();

    for seq in 0..10_000u32 {
        let msg = seq.to_le_bytes();
        tx.push(&msg);
        assert_eq!(&*rx.pop().unwrap(), &msg);
    }
    assert_eq!(rx.try_pop().map(|m| m.is_some()), Ok(false));

    // Second mapping.
    // SAFETY: cooperating handles only.
    let mut rx2 = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
    for seq in 0..100u32 {
        tx.push(&seq.to_le_bytes());
    }
    for seq in 0..100u32 {
        assert_eq!(&*rx.pop().unwrap(), &seq.to_le_bytes());
        assert_eq!(&*rx2.pop().unwrap(), &seq.to_le_bytes());
    }
}

// ---------------------------------------------------------------------------
// 2. Cross-process: a forked child attaches as a consumer and receives
//    every message (both rings).
// ---------------------------------------------------------------------------

#[test]
fn forked_consumer_receives_everything_elems() {
    const N: u64 = 4096;
    let fd = memfd("spmc-fork-elems").unwrap();
    // SAFETY: fresh private memfd; the child attaches its own fresh handles.
    let (mut tx, mut rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 1024, 4) }.unwrap();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i));
        }
    });

    // Do not publish anything before the child has joined.
    while tx.consumer_count() < 2 {
        std::thread::yield_now();
    }
    for i in 0..N {
        tx.push(i);
        assert_eq!(rx.pop(), Ok(i)); // keep the parent's consumer caught up
    }
    wait_child(pid);
}

#[test]
fn forked_consumer_receives_everything_bytes() {
    const N: u32 = 4096;
    let fd = memfd("spmc-fork-bytes").unwrap();
    // SAFETY: fresh private memfd; the child attaches its own fresh handles.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096, 4) }.unwrap();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { BytesRing::attach_shm_consumer(fd.as_fd()) }.unwrap();
        for seq in 0..N {
            assert_eq!(&*crx.pop().unwrap(), &seq.to_le_bytes());
        }
    });

    while tx.consumer_count() < 2 {
        std::thread::yield_now();
    }
    for seq in 0..N {
        tx.push(&seq.to_le_bytes());
        assert_eq!(&*rx.pop().unwrap(), &seq.to_le_bytes());
    }
    wait_child(pid);
}

// ---------------------------------------------------------------------------
// 3. Multi-consumer cross-process: two forked consumers each see everything.
// ---------------------------------------------------------------------------

#[test]
fn two_forked_consumers_each_see_everything() {
    const N: u64 = 10_000;
    let fd = memfd("spmc-two-children").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 256, 4) }.unwrap();
    drop(rx); // the children are the only consumers

    let child = || {
        // SAFETY: cooperating handles only; free table slots exist.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i));
        }
    };
    let pid_a = fork_child(child);
    let pid_b = fork_child(child);

    while tx.consumer_count() < 2 {
        std::thread::yield_now();
    }
    // N far exceeds the capacity: the producer is gated on the slower child
    // throughout, and both still see every message in order.
    for i in 0..N {
        tx.push(i);
    }
    wait_child(pid_a);
    wait_child(pid_b);
}

// ---------------------------------------------------------------------------
// 4. Gating across processes: a stalled child consumer blocks try_push; the
//    producer proceeds once the child resumes.
// ---------------------------------------------------------------------------

#[test]
fn gating_across_processes() {
    const CAPACITY: u64 = 8;
    const EXTRA: u64 = 24;
    let fd = memfd("spmc-gating").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY as usize, 2) }.unwrap();
    drop(rx); // the child is the only consumer
    let ready = pipe();
    let go = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        go.recv(); // stall (do not consume) until the parent has verified the gate
        for i in 0..CAPACITY + EXTRA {
            assert_eq!(crx.pop(), Ok(i));
        }
    });

    ready.recv();
    // The child is attached but stalled: exactly `CAPACITY` pushes fit.
    for i in 0..CAPACITY {
        assert!(tx.try_push(i).is_ok(), "push {i} must fit");
    }
    assert!(
        tx.try_push(u64::MAX).is_err(),
        "a stalled consumer must gate the producer"
    );
    go.send();
    // The child resumed: blocking pushes proceed as it drains.
    for i in 0..EXTRA {
        tx.push(CAPACITY + i);
    }
    wait_child(pid);
}

// ---------------------------------------------------------------------------
// 5. Table full -> SubscribeError::Full / AddrInUse; graceful detach frees
//    the slot for reattach.
// ---------------------------------------------------------------------------

#[test]
fn table_full_and_graceful_detach_frees_slot() {
    let fd = memfd("spmc-table-full").unwrap();
    // max_consumers = 2: the initial consumer occupies slot 0.
    // SAFETY: fresh private memfd.
    let (tx, rx0) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8, 2) }.unwrap();
    assert_eq!(rx0.shm_slot(), Some(0));

    // SAFETY: cooperating handles only.
    let rx1 = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(rx1.shm_slot(), Some(1));

    // Table full: attach reports AddrInUse, subscribe reports Full.
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("full table must refuse attach");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    assert!(matches!(tx.subscribe(), Err(SubscribeError::Full)));

    // Graceful detach frees the slot; reattach claims it again.
    drop(rx1);
    // SAFETY: cooperating handles only.
    let rx1b = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(rx1b.shm_slot(), Some(1), "a freed slot is reissued");

    // subscribe() on a live shm handle claims table slots too.
    drop(rx1b);
    let rx1c = tx.subscribe().unwrap();
    assert_eq!(rx1c.shm_slot(), Some(1));
}

// ---------------------------------------------------------------------------
// 6. force_detach_consumer: SIGKILLed child, gated producer resumes; the
//    slot is RETIRED until recover_shm resets the table.
// ---------------------------------------------------------------------------

#[test]
fn force_detach_dead_consumer_and_recover() {
    const CAPACITY: u64 = 8;
    let fd = memfd("spmc-force-detach").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY as usize, 4) }.unwrap();
    drop(rx0); // slot 0 freed; the child will claim it
    let ready = pipe();
    let consumed = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        assert_eq!(crx.shm_slot(), Some(0));
        ready.send();
        for i in 0..3u64 {
            assert_eq!(crx.pop(), Ok(i));
        }
        consumed.send();
        // Stall forever mid-consumption; the parent SIGKILLs us here.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    });

    ready.recv();
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
        "a dead consumer's slot still gates"
    );

    // The caller (who reaped the child) asserts its death: retire the slot.
    // SAFETY: the previous holder of slot 0 is dead (SIGKILLed and reaped).
    unsafe { RingBuffer::<u64>::force_detach_consumer(fd.as_fd(), 0) }.unwrap();
    assert!(
        tx.try_push(100).is_ok(),
        "retiring the slot un-gates the producer"
    );
    assert_eq!(tx.consumer_count(), 0);

    // A retired slot is never re-issued: a new consumer gets a DIFFERENT slot.
    // SAFETY: cooperating handles only.
    let rx_new = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
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

// ---------------------------------------------------------------------------
// 7. Zombie protection [A-4.1]: force_detach a consumer that is STILL ALIVE.
//    Its flushes land on the retired slot; the producer ignores it; a new
//    consumer on a different slot is unaffected.
// ---------------------------------------------------------------------------

#[test]
fn force_detach_live_zombie_blast_radius() {
    const CAPACITY: u64 = 8;
    let fd = memfd("spmc-zombie").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx0) =
        unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), CAPACITY as usize, 4) }.unwrap();
    drop(rx0); // slot 0 freed

    // The zombie-to-be: a live consumer through a second mapping.
    // SAFETY: cooperating handles only.
    let mut zombie = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(zombie.shm_slot(), Some(0));

    for i in 0..4u64 {
        tx.push(i);
    }
    assert_eq!(zombie.pop(), Ok(0));
    assert_eq!(zombie.pop(), Ok(1));

    // Wrongly assert its death: the slot is retired while the handle lives.
    // SAFETY: this is the blast-radius test — the "victim" cooperates by
    // being a handle we control; its read validity is revoked from here on.
    unsafe { RingBuffer::<u64>::force_detach_consumer(fd.as_fd(), 0) }.unwrap();
    assert_eq!(tx.consumer_count(), 0, "a retired slot is not counted");

    // A new consumer lands on a different slot and is unaffected.
    // SAFETY: cooperating handles only.
    let mut fresh = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    assert_eq!(fresh.shm_slot(), Some(1));

    // The producer ignores the zombie (cursor 2): with only `fresh` (cursor
    // 4) gating, exactly CAPACITY more pushes fit.
    for i in 0..CAPACITY {
        assert!(
            tx.try_push(4 + i).is_ok(),
            "the zombie must not gate push {i}"
        );
    }
    assert!(tx.try_push(u64::MAX).is_err(), "fresh still gates normally");

    // The zombie's own pops/flushes land on the retired slot: harmless to
    // everyone else (its reads lost gating protection — values may already
    // be overwritten, so only the mechanics are asserted, not the data).
    let _ = zombie.try_pop();
    let _ = zombie.try_pop();
    assert!(
        tx.try_push(u64::MAX).is_err(),
        "zombie flushes must not un-gate the producer"
    );

    // `fresh` drains normally.
    for i in 0..CAPACITY {
        assert_eq!(fresh.pop(), Ok(4 + i));
    }
    assert!(tx.try_push(100).is_ok());
}

// ---------------------------------------------------------------------------
// 8. Closed: graceful producer drop propagates Closed across the fd.
// ---------------------------------------------------------------------------

#[test]
fn producer_drop_closes_across_processes() {
    const N: u64 = 100;
    let fd = memfd("spmc-closed").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 256, 2) }.unwrap();
    drop(rx);
    let ready = pipe();

    let pid = fork_child(|| {
        // SAFETY: cooperating handles only; a free table slot exists.
        let mut crx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
        ready.send();
        // Drain everything published, then observe Closed.
        for i in 0..N {
            assert_eq!(crx.pop(), Ok(i));
        }
        assert_eq!(crx.pop(), Err(rust_rb::spmc::Closed));
    });

    ready.recv();
    for i in 0..N {
        tx.push(i);
    }
    drop(tx); // graceful close: sets the header closed word
    wait_child(pid);

    // A late consumer attach on a closed ring is refused (mirrors the heap
    // ring's SubscribeError::Closed).
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("attach on a closed ring must be refused");
    assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);

    // A new producer attach re-opens the ring (resets the closed word).
    // SAFETY: the previous producer is gone (dropped).
    let mut tx2 = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }.unwrap();
    // SAFETY: cooperating handles only.
    let mut rx2 = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.unwrap();
    tx2.push(1);
    assert_eq!(rx2.pop(), Ok(1));
}

#[test]
fn bytes_producer_drop_closes() {
    let fd = memfd("spmc-bytes-closed").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096, 2) }.unwrap();
    tx.push(b"last");
    drop(tx);
    assert_eq!(&*rx.pop().unwrap(), b"last");
    assert!(rx.pop().is_err(), "drained + closed");
}

// ---------------------------------------------------------------------------
// 9. Validation: kind/capacity/max_consumers mismatches rejected; the
//    generation seqlock is still enforced.
// ---------------------------------------------------------------------------

#[test]
fn attach_validates_and_leases_conflict() {
    let fd = memfd("spmc-validate").unwrap();
    // SAFETY: fresh private memfd.
    let (tx, _rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();

    // Producer role held: polite attach conflicts.
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd()) }
        .err()
        .expect("attach must fail while the producer role is held");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    // Kind mismatches: SPMC bytes / SPSC attaches on an SPMC element ring.
    // SAFETY: cooperating handles only (all rejected before touching state).
    unsafe {
        assert!(BytesRing::attach_shm_consumer(fd.as_fd()).is_err());
        assert!(rust_rb::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()).is_err());
        // Element size mismatch.
        assert!(RingBuffer::<u32>::attach_shm_consumer(fd.as_fd()).is_err());
    }
    drop(tx);

    // An SPSC region must reject SPMC attaches (kind 2 vs 4).
    let spsc_fd = memfd("spmc-validate-spsc").unwrap();
    // SAFETY: fresh private memfd.
    let _pair = unsafe { rust_rb::RingBuffer::<u64>::create_shm(spsc_fd.as_fd(), 64) }.unwrap();
    // SAFETY: cooperating handles only.
    assert!(unsafe { RingBuffer::<u64>::attach_shm_consumer(spsc_fd.as_fd()) }.is_err());
}

#[test]
fn corrupt_header_fields_are_rejected() {
    const OFF_CAPACITY: usize = 16;
    const OFF_MAX_CONSUMERS: usize = 36;
    const OFF_GENERATION: usize = 56;

    // max_consumers = 0 is corrupt.
    let fd = memfd("spmc-corrupt-mc").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    poke_u32(fd.as_fd(), OFF_MAX_CONSUMERS, 0);
    // SAFETY: cooperating handles only (rejected before touching state).
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("max_consumers = 0 must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    // Non-power-of-two capacity is corrupt.
    let fd = memfd("spmc-corrupt-cap").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    poke_u64(fd.as_fd(), OFF_CAPACITY, 3);
    // SAFETY: cooperating handles only.
    assert!(unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }.is_err());

    // The generation seqlock: an odd generation (mid-initialization) is
    // rejected rather than read as a chimera.
    let fd = memfd("spmc-odd-gen").unwrap();
    {
        // SAFETY: fresh private memfd.
        let _pair = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 4) }.unwrap();
    }
    let gen = peek_u64(fd.as_fd(), OFF_GENERATION);
    poke_u64(fd.as_fd(), OFF_GENERATION, gen | 1);
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
        .err()
        .expect("odd generation must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        err.to_string().contains("being initialized"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// Recovery keeps published-but-unconsumed messages (at-least-once, mirroring
// the SPSC recover contract).
// ---------------------------------------------------------------------------

#[test]
fn recover_redelivers_from_slowest_dead_consumer() {
    let fd = memfd("spmc-recover-resume").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, mut rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 8, 2) }.unwrap();
    for i in 0..4u64 {
        tx.push(i);
    }
    assert_eq!(rx.pop(), Ok(0)); // published cursor advances to 1 (caught-up flush lags)
                                 // Simulated crash of both sides: no destructors run.
    std::mem::forget(tx);
    std::mem::forget(rx);

    // SAFETY: both previous holders are gone (forgotten, never used again).
    let (mut tx2, mut rx2) = unsafe { RingBuffer::<u64>::recover_shm(fd.as_fd()) }.unwrap();
    // The dead consumer's published cursor is the resume point: at-least-once
    // redelivery of everything it had not published past.
    let first = rx2.pop().unwrap();
    assert!(first <= 1, "resume at or before the dead consumer's cursor");
    let mut expect = first + 1;
    while let Ok(Some(v)) = rx2.try_pop() {
        assert_eq!(v, expect);
        expect += 1;
    }
    assert_eq!(expect, 4, "everything published must be redelivered");
    tx2.push(9);
    assert_eq!(rx2.pop(), Ok(9));
}
