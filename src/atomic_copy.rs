//! The strict atomic payload-copy helpers shared by the lossy and composed
//! rings — one copy of each racing-copy protocol.
//!
//! Two independent families live here:
//!
//! * **Word-wise copies** ([`copy_in_words`]/[`copy_out_words`] and the
//!   typed [`write_payload`]/[`read_payload`] wrappers) — the element rings'
//!   seqlock payload copy ([`crate::broadcast`] and [`crate::anchored`]).
//! * **4-byte-lane copies** ([`copy_in_lanes`]/[`copy_out_lanes`]) — the
//!   byte rings' Agrona-style payload copy ([`crate::broadcast_bytes`] and
//!   [`crate::anchored_bytes`]). The single-lane header accessors
//!   (`store_lane`/`load_lane`) stay in those modules: their behaviour under
//!   the `rust_rb_volatile_copy` dev cfg deliberately differs per module
//!   (broadcast_bytes keeps header lanes atomic under the A/B switch;
//!   anchored_bytes flips them volatile with the rest of its accesses).
//!
//! The `rust_rb_volatile_copy` dev cfg (set via `RUSTFLAGS`, never a
//! feature) swaps the strict copies for whole-payload/lane volatile
//! accesses — the classic (formally racy) seqlock shape, kept off the
//! default build strictly as the A/B benchmark alternative.

#[cfg(not(rust_rb_volatile_copy))]
use std::mem::size_of;
use std::mem::MaybeUninit;
#[cfg(not(rust_rb_volatile_copy))]
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};

use crate::broadcast::NoUninit;

// -----------------------------------------------------------------------------
// Word-wise copies (element rings)
// -----------------------------------------------------------------------------

/// Copy `len` bytes from private memory into a slot payload using
/// machine-word **atomic** `Relaxed` stores (tail bytes byte-wise). Plain
/// stores would be UB against a racing reader's atomic copy and could be
/// compiler-hoisted above the invalidation fence [M-F10] — the strict copy
/// is mandatory on the producer side too.
///
/// # Safety
///
/// `src..src + len` must be readable (any alignment); `dst..dst + len` must
/// be writable, `dst` word-aligned, and concurrently accessed only through
/// atomics.
#[cfg(not(rust_rb_volatile_copy))]
#[inline(always)]
unsafe fn copy_in_words(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(
        dst as usize % std::mem::align_of::<usize>(),
        0,
        "slot payload must be word-aligned (repr(C) guarantees it)"
    );
    let word = size_of::<usize>();
    let mut off = 0;
    while off + word <= len {
        // SAFETY: `off + word <= len` keeps the read in range; the source is
        // a private value of `T`, whose alignment may be below `usize`'s —
        // hence `read_unaligned`.
        let v = unsafe { src.add(off).cast::<usize>().read_unaligned() };
        // SAFETY: in range and word-aligned (base asserted above, offsets
        // are word multiples); a shared atomic reference over the slot's
        // `UnsafeCell` storage is the sanctioned way to store while readers
        // race.
        unsafe { &*(dst.add(off).cast::<AtomicUsize>()) }.store(v, Ordering::Relaxed);
        off += word;
    }
    while off < len {
        // SAFETY: `off < len`.
        let v = unsafe { *src.add(off) };
        // SAFETY: in range; byte atomics have no alignment requirement.
        unsafe { &*(dst.add(off).cast::<AtomicU8>()) }.store(v, Ordering::Relaxed);
        off += 1;
    }
}

/// Copy `len` bytes out of a slot payload into private memory using
/// machine-word **atomic** `Relaxed` loads (tail bytes byte-wise). The bytes
/// may be torn (a racing overwrite); the caller must treat the destination
/// as `MaybeUninit` until its out-of-band validation (the seqlock
/// generation) revalidates [M-F11].
///
/// # Safety
///
/// `src..src + len` must be readable, `src` word-aligned, initialized (the
/// caller observed the slot published at least once), and concurrently
/// accessed only through atomics; `dst..dst + len` must be writable (any
/// alignment) and private to the caller.
#[cfg(not(rust_rb_volatile_copy))]
#[inline(always)]
unsafe fn copy_out_words(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(
        src as usize % std::mem::align_of::<usize>(),
        0,
        "slot payload must be word-aligned (repr(C) guarantees it)"
    );
    let word = size_of::<usize>();
    let mut off = 0;
    while off + word <= len {
        // SAFETY: in range and word-aligned, as in `copy_in_words`; every
        // byte was initialized by a prior publish, so the atomic load reads
        // initialized (if possibly torn) data.
        let v = unsafe { &*(src.add(off).cast::<AtomicUsize>()) }.load(Ordering::Relaxed);
        // SAFETY: `off + word <= len`; the destination is a private local,
        // possibly under-aligned for `usize` — hence `write_unaligned`.
        unsafe { dst.add(off).cast::<usize>().write_unaligned(v) };
        off += word;
    }
    while off < len {
        // SAFETY: in range; byte atomics have no alignment requirement.
        let v = unsafe { &*(src.add(off).cast::<AtomicU8>()) }.load(Ordering::Relaxed);
        // SAFETY: `off < len`.
        unsafe { *dst.add(off) = v };
        off += 1;
    }
}

/// Store `value` into a slot payload with the strict word-wise atomic copy
/// (or, under the private `rust_rb_volatile_copy` dev cfg, one volatile
/// write — the A/B benchmark alternative; formally racy, kept off the
/// default build).
///
/// # Safety
///
/// `dst` must be the payload of a live slot this producer owns for writing
/// (readers may race through atomics; the seqlock brackets the write).
#[inline(always)]
pub(crate) unsafe fn write_payload<T: NoUninit>(dst: *mut MaybeUninit<T>, value: &T) {
    #[cfg(not(rust_rb_volatile_copy))]
    // SAFETY: `dst` is a valid slot payload, word-aligned by the `repr(C)`
    // slot layout; `value` is a live `T` with every byte initialized
    // (`NoUninit`); readers only race through atomics.
    unsafe {
        copy_in_words(
            (value as *const T).cast::<u8>(),
            dst.cast::<u8>(),
            size_of::<T>(),
        )
    };
    #[cfg(rust_rb_volatile_copy)]
    // SAFETY: `dst` is a valid, suitably aligned slot payload; `T: Copy`.
    // The concurrent volatile read on the reader side makes this the
    // classic (formally racy) seqlock shape — dev switch only.
    unsafe {
        dst.cast::<T>().write_volatile(*value)
    };
}

/// Copy a slot payload into `out` with the strict word-wise atomic copy (or
/// the volatile alternative under `rust_rb_volatile_copy`). The result may
/// be torn: it stays `MaybeUninit` until the caller revalidates the
/// generation.
///
/// # Safety
///
/// `src` must be the payload of a live slot observed published at least
/// once (every byte initialized).
#[inline(always)]
pub(crate) unsafe fn read_payload<T: NoUninit>(
    src: *const MaybeUninit<T>,
    out: &mut MaybeUninit<T>,
) {
    #[cfg(not(rust_rb_volatile_copy))]
    // SAFETY: `src` is a valid slot payload, word-aligned by the `repr(C)`
    // slot layout, initialized by a prior publish; `out` is a private local.
    unsafe {
        copy_out_words(
            src.cast::<u8>(),
            out.as_mut_ptr().cast::<u8>(),
            size_of::<T>(),
        )
    };
    #[cfg(rust_rb_volatile_copy)]
    {
        // SAFETY: `src` is a valid, suitably aligned slot payload; torn
        // bytes land in a `MaybeUninit` and are never interpreted before
        // validation. Dev switch only (see `write_payload`).
        *out = unsafe { src.read_volatile() };
    }
}

// -----------------------------------------------------------------------------
// 4-byte-lane copies (byte rings)
// -----------------------------------------------------------------------------

/// Copy `len` payload bytes from private memory into the ring using 4-byte
/// atomic `Relaxed` stores, one per lane; the final partial lane is
/// zero-padded (the extra bytes stay inside this record's 8-aligned
/// footprint). Plain stores would be UB against the readers' concurrent
/// atomic copies and could be compiler-hoisted above the intent fence — the
/// strict copy is mandatory on the producer side too.
///
/// Every racing ring access in the byte rings — headers included — is a
/// 4-byte atomic on the same 4-aligned lane grid, so two racing accesses are
/// always same-size and identically aligned. Record boundaries shift across
/// laps, so a mixed-size scheme (machine-word payload stores racing another
/// lap's header loads) would put differently-sized atomics on the same
/// bytes: formally unspecified, and rejected by Miri.
///
/// # Safety
///
/// `src..src + len` must be readable (any alignment); the destination lanes
/// (`dst` 4-aligned, `align_up(len)` bytes) must be in bounds, writable by
/// this producer, and concurrently accessed only through atomics.
#[cfg(not(rust_rb_volatile_copy))]
#[inline(always)]
pub(crate) unsafe fn copy_in_lanes(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(dst as usize % 4, 0, "payload base must be lane-aligned");
    let mut off = 0;
    while off + 4 <= len {
        // SAFETY: `off + 4 <= len` keeps the read in range; the source is a
        // caller slice of arbitrary alignment — hence `read_unaligned`.
        let v = unsafe { src.add(off).cast::<u32>().read_unaligned() };
        // SAFETY: lane in range per the contract (`off < len <= align_up(len)`).
        unsafe { &*(dst.add(off).cast::<AtomicU32>()) }.store(v, Ordering::Relaxed);
        off += 4;
    }
    if off < len {
        let mut lane = [0u8; 4];
        // SAFETY: `len - off < 4` remaining source bytes, all readable.
        unsafe { std::ptr::copy_nonoverlapping(src.add(off), lane.as_mut_ptr(), len - off) };
        // SAFETY: the lane straddling the payload tail is still inside the
        // record's 8-aligned footprint (`align_up(HEADER + len)`).
        unsafe { &*(dst.add(off).cast::<AtomicU32>()) }
            .store(u32::from_ne_bytes(lane), Ordering::Relaxed);
    }
}

/// Copy `len` payload bytes out of the ring into private memory using 4-byte
/// atomic `Relaxed` loads, one per lane. The bytes may be torn (a racing
/// overwrite); the caller must not expose them until the out-of-band window
/// check revalidates.
///
/// # Safety
///
/// The source lanes (`src` 4-aligned, `align_up(len)` bytes) must be in
/// bounds, initialized (the buffer is zeroed at construction), and
/// concurrently accessed only through atomics; `dst..dst + len` must be
/// writable (any alignment) and private to the caller.
#[cfg(not(rust_rb_volatile_copy))]
#[inline(always)]
pub(crate) unsafe fn copy_out_lanes(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(src as usize % 4, 0, "payload base must be lane-aligned");
    let mut off = 0;
    while off + 4 <= len {
        // SAFETY: lane in range and 4-aligned per the contract; every byte
        // is initialized, so the load reads initialized (if torn) data.
        let v = unsafe { &*(src.add(off).cast::<AtomicU32>()) }.load(Ordering::Relaxed);
        // SAFETY: `off + 4 <= len`; the destination may be under-aligned —
        // hence `write_unaligned`.
        unsafe { dst.add(off).cast::<u32>().write_unaligned(v) };
        off += 4;
    }
    if off < len {
        // SAFETY: the lane straddling the payload tail is inside the
        // record's 8-aligned footprint, hence in bounds.
        let v = unsafe { &*(src.add(off).cast::<AtomicU32>()) }.load(Ordering::Relaxed);
        let lane = v.to_ne_bytes();
        // SAFETY: `len - off < 4` remaining destination bytes, all writable.
        unsafe { std::ptr::copy_nonoverlapping(lane.as_ptr(), dst.add(off), len - off) };
    }
}

/// The `rust_rb_volatile_copy` A/B alternative: identical lane walk with
/// volatile instead of atomic lane accesses — formally racy, kept off the
/// default build (see the module docs).
#[cfg(rust_rb_volatile_copy)]
#[inline(always)]
pub(crate) unsafe fn copy_in_lanes(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(dst as usize % 4, 0, "payload base must be lane-aligned");
    let mut off = 0;
    while off + 4 <= len {
        // SAFETY: as in the atomic variant; volatile lane stores are the
        // classic (formally racy) A/B shape — dev switch only.
        unsafe {
            let v = src.add(off).cast::<u32>().read_unaligned();
            dst.add(off).cast::<u32>().write_volatile(v);
        }
        off += 4;
    }
    if off < len {
        let mut lane = [0u8; 4];
        // SAFETY: as in the atomic variant.
        unsafe {
            std::ptr::copy_nonoverlapping(src.add(off), lane.as_mut_ptr(), len - off);
            dst.add(off)
                .cast::<u32>()
                .write_volatile(u32::from_ne_bytes(lane));
        }
    }
}

/// See the atomic `copy_out_lanes`; volatile A/B variant (dev switch only).
#[cfg(rust_rb_volatile_copy)]
#[inline(always)]
pub(crate) unsafe fn copy_out_lanes(src: *const u8, dst: *mut u8, len: usize) {
    debug_assert_eq!(src as usize % 4, 0, "payload base must be lane-aligned");
    let mut off = 0;
    while off + 4 <= len {
        // SAFETY: as in the atomic variant — dev switch only.
        unsafe {
            let v = src.add(off).cast::<u32>().read_volatile();
            dst.add(off).cast::<u32>().write_unaligned(v);
        }
        off += 4;
    }
    if off < len {
        // SAFETY: as in the atomic variant.
        unsafe {
            let v = src.add(off).cast::<u32>().read_volatile();
            let lane = v.to_ne_bytes();
            std::ptr::copy_nonoverlapping(lane.as_ptr(), dst.add(off), len - off);
        }
    }
}
