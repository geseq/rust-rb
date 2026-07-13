//! The per-slot seqlock shared by the lossy read paths — one definition of
//! the slot layout and one copy of the validate-copy-revalidate sequence,
//! used by [`crate::broadcast`]'s `Consumer` and [`crate::anchored`]'s
//! `Observer` (the composed ring documents its observer protocol as
//! broadcast's, verbatim — this module is what makes "verbatim" literal, so
//! a fence-discipline fix cannot reach one ring and miss the other).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicU64, Ordering};

use crate::atomic_copy::read_payload;
use crate::broadcast::NoUninit;

/// One ring slot: a per-slot seqlock.
///
/// `seq` encodes `2·s + phase` for global sequence `s`: `2s + 1` while
/// message `s` is being written, `2s + 2` once it is published, `0`
/// initially (below every expected value — an untouched slot can never be
/// accepted). The series one slot takes (`2s+1, 2s+2, 2(s+capacity)+1, …`)
/// is strictly increasing, so exact-match acceptance is generation-unique.
///
/// `repr(C)` pins the payload at offset `max(8, align_of::<T>())` from an
/// (at least) 8-aligned base — always a multiple of the machine word, which
/// the word-wise copy helpers require (and debug-assert).
#[repr(C)]
pub(crate) struct Slot<T> {
    pub(crate) seq: AtomicU64,
    pub(crate) data: UnsafeCell<MaybeUninit<T>>,
}

/// The seqlock read: validate the generation, copy the payload word-wise
/// atomically, revalidate. `Some(T)` is the complete, untorn message at
/// generation `expected`; `None` means the slot has moved on (the reader is
/// lapped) or tore the copy — the caller repositions.
///
/// The caller must have established `tail > s` for `expected = 2s + 2`:
/// because the tail is Release-stored after the slot publish and the caller
/// Acquire-read it, the generation here is at least `expected` — an "empty"
/// slot is unobservable past the tail check.
#[inline]
pub(crate) fn read_valid<T: NoUninit>(slot: &Slot<T>, expected: u64) -> Option<T> {
    let v1 = slot.seq.load(Ordering::Acquire);
    debug_assert!(v1 >= expected, "slot behind the published tail");
    if v1 == expected {
        let mut out = MaybeUninit::<T>::uninit();
        // SAFETY: the slot was published at least once (generation reached
        // `expected`), so every payload byte is initialized; torn bytes
        // stay `MaybeUninit` until revalidation below.
        unsafe { read_payload(slot.data.get(), &mut out) };
        // Order the payload loads before the revalidating load: fence +
        // relaxed re-load is the sound shape (an `Acquire` re-load would
        // order the wrong direction) [M-F11].
        fence(Ordering::Acquire);
        let v2 = slot.seq.load(Ordering::Relaxed);
        if v2 == v1 {
            // SAFETY: generation unchanged across the copy — the bytes are
            // the complete, untorn message; `T: NoUninit` makes every byte
            // pattern of a published value initialized data.
            return Some(unsafe { out.assume_init() });
        }
    }
    None
}

/// Byte stride of one shm slot for element type `T` — the slot layout the
/// heap ring uses, reused verbatim in the mapped region so attach
/// validation can check the creator's geometry.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) fn shm_slot_stride<T>() -> usize {
    std::mem::size_of::<Slot<T>>()
}

/// Alignment of one shm slot (see [`shm_slot_stride`]); the shm buffer
/// offset must satisfy it.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) fn shm_slot_align<T>() -> usize {
    std::mem::align_of::<Slot<T>>()
}
