//! Shared-memory ring tests (feature `shm`, Linux).
//!
//! Cross-address-space behavior is exercised two ways: by mapping the same
//! memfd twice in this process (different base addresses, same physical
//! pages — catches any absolute-pointer assumption), and by a real
//! subprocess that creates a ring, publishes, and exits without cleanup so
//! the parent can crash-recover it.
#![cfg(all(feature = "shm", target_os = "linux"))]

use std::os::fd::{AsFd, AsRawFd};

use rust_rb::spsc_bytes::BytesRingBuffer;
use rust_rb::wait::PauseWait;
use rust_rb::{memfd, RingBuffer};

#[test]
fn bytes_ring_round_trips_in_shm() {
    let fd = memfd("rb-bytes-roundtrip").unwrap();
    // SAFETY: fresh private memfd; only this test touches it.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096) }.unwrap();

    for seq in 0..10_000u32 {
        let msg = seq.to_le_bytes();
        tx.push(&msg);
        assert_eq!(&*rx.pop(), &msg);
    }
    assert!(rx.try_pop().is_none());
}

#[test]
fn element_ring_round_trips_in_shm() {
    let fd = memfd("rb-elem-roundtrip").unwrap();
    // SAFETY: fresh private memfd; u64 is ShmItem.
    let (mut tx, mut rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 1024) }.unwrap();

    for i in 0..10_000u64 {
        tx.push(i);
        assert_eq!(rx.pop(), i);
    }
    assert!(rx.try_pop().is_none());
}

/// The same fd mapped twice in one process: producer through the first
/// mapping, consumer attached through a second, independent mapping.
#[test]
fn second_mapping_attach_consumes() {
    let fd = memfd("rb-second-mapping").unwrap();
    // SAFETY: fresh private memfd.
    let (mut tx, rx) =
        unsafe { BytesRingBuffer::<PauseWait, PauseWait>::create_shm_with(fd.as_fd(), 4096) }
            .unwrap();

    // Free the consumer role so a second mapping can claim it.
    drop(rx);
    // SAFETY: cooperating handles only; consumer role was just released.
    let mut rx2 =
        unsafe { BytesRingBuffer::<PauseWait, PauseWait>::attach_shm_consumer(fd.as_fd()) }
            .unwrap();

    // Records are 8 bytes (4 header + 4 payload): batches of 400 fit the
    // 4096-byte ring, so each round pushes fully, then drains fully through
    // the second mapping.
    for round in 0..5u32 {
        for seq in 0..400u32 {
            tx.push(&(round * 400 + seq).to_le_bytes());
        }
        for seq in 0..400u32 {
            assert_eq!(&*rx2.pop(), &(round * 400 + seq).to_le_bytes());
        }
    }
    assert!(rx2.try_pop().is_none());
}

#[test]
fn attach_validates_and_leases_conflict() {
    let fd = memfd("rb-validate").unwrap();
    // SAFETY: fresh private memfd.
    let (tx, rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096) }.unwrap();

    // Both roles held by live us: attaching must fail with AddrInUse.
    // SAFETY: cooperating handles only.
    let err =
        match unsafe { BytesRingBuffer::<PauseWait, PauseWait>::attach_shm_producer(fd.as_fd()) } {
            Err(e) => e,
            Ok(_) => panic!("attach must fail while the role is held"),
        };
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    // Kind mismatch: an element-ring attach on a byte ring must fail.
    // SAFETY: cooperating handles only.
    let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) };
    assert!(err.is_err());

    // Recovery must refuse while the holders are alive.
    // SAFETY: cooperating handles only.
    let err = match unsafe { BytesRingBuffer::recover_shm(fd.as_fd()) } {
        Err(e) => e,
        Ok(_) => panic!("recovery must refuse while holders are alive"),
    };
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    drop(tx);
    drop(rx);

    // A non-ring fd must be rejected outright.
    let junk = memfd("rb-junk").unwrap();
    // SAFETY: cooperating handles only (none exist).
    let err = unsafe { BytesRingBuffer::<PauseWait, PauseWait>::attach_shm_consumer(junk.as_fd()) };
    assert!(err.is_err(), "junk region must not validate");
}

/// End-to-end crash recovery: a child process creates the ring, pushes 100
/// messages, and exits without any cleanup. The parent recovers the region,
/// finds every published message intact, drains them, and keeps using the
/// ring.
#[test]
fn crash_recovery_drains_everything_published() {
    let fd = memfd("rb-crash-recovery").unwrap();

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_child_entry")
        .arg("--nocapture")
        .arg("--include-ignored")
        .env("RUST_RB_SHM_CHILD_FD", fd.as_fd().as_raw_fd().to_string())
        .status()
        .expect("spawn child");
    assert!(status.success(), "child crashed abnormally: {status:?}");

    // The child is dead; its leases are stale. Recover and drain.
    // SAFETY: cooperating handles; the only other holder is a dead process.
    let (mut tx, mut rx) = unsafe { BytesRingBuffer::recover_shm(fd.as_fd()) }.unwrap();

    let mut seen = 0u32;
    while let Some(msg) = rx.try_pop() {
        let mut b = [0u8; 4];
        b.copy_from_slice(&msg);
        drop(msg);
        assert_eq!(u32::from_le_bytes(b), seen);
        seen += 1;
    }
    assert_eq!(seen, 100, "every published message must survive the crash");

    // The recovered ring keeps working.
    tx.push(b"post-recovery");
    assert_eq!(&*rx.pop(), b"post-recovery");
}

/// Not a real test: the crash-recovery child. Runs only when the parent
/// spawns this binary with the env var set; ignored otherwise.
#[test]
#[ignore = "child-process entry for crash_recovery_drains_everything_published"]
fn crash_child_entry() {
    use std::os::fd::{FromRawFd, OwnedFd};

    let fd_num: i32 = std::env::var("RUST_RB_SHM_CHILD_FD")
        .expect("child entry requires RUST_RB_SHM_CHILD_FD")
        .parse()
        .expect("fd number");
    // SAFETY: the parent passed this inherited, open memfd.
    let fd = unsafe { OwnedFd::from_raw_fd(fd_num) };

    // SAFETY: fresh region; the parent does not touch it until we exit.
    let (mut tx, _rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 4096) }.unwrap();
    for seq in 0..100u32 {
        tx.push(&seq.to_le_bytes());
    }
    // Simulated crash: exit without running any destructors — leases stay
    // set to our (soon dead) pid, deferred consumer state is irrelevant, and
    // only the producer's published cursor matters.
    std::process::exit(0);
}
