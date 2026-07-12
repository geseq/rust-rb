//! Shared-memory-backed rings (Linux, feature `shm`).
//!
//! Backs [`RingBuffer`] and [`BytesRingBuffer`] with a mapped
//! region (memfd, `shm_open`, or any mappable fd) so the producer and
//! consumer can live in **different processes**. The handles returned are
//! the ordinary [`Producer`]/[`Consumer`]/[`BytesProducer`]/[`BytesConsumer`]
//! types — the hot paths are identical to the heap-backed rings.
//!
//! # Region layout (stable, validated on attach)
//!
//! A fixed header at raw byte offsets (no Rust struct layout involved), then
//! the buffer:
//!
//! ```text
//! 0    magic     u64      "rust_rb1"
//! 8    version   u32
//! 12   kind      u32      1 = byte ring, 2 = element ring,
//!                         3 = SPMC byte ring, 4 = SPMC element ring,
//!                         5 = broadcast element ring, 6 = broadcast byte ring,
//!                         9 = anchored element ring, 10 = anchored byte ring
//! 16   capacity  u64      cursor units (power of two)
//! 24   unit_size u64      bytes per cursor unit (1, or size_of::<T>())
//! 32   arch_bits u32      usize width; cross-arch attach is rejected
//! 36   max_consumers u32  SPMC kinds only: consumer-table slots (>= 1);
//!                         0 on every other kind (broadcast membership is
//!                         unbounded — consumers keep no shared state)
//! 40   producer_lease u64 (atomic) opaque token of the producer holder, 0 = free
//! 48   consumer_lease u64 (atomic) opaque token of the consumer holder, 0 = free
//!                         (SPSC kinds only; SPMC leases are per table slot;
//!                         broadcast consumers are lease-free)
//! 56   generation u64 (atomic) seqlock: odd while (re)initializing
//! 128  write_cursor  (atomic, own 128-byte slot) SPSC kinds: usize.
//!                         SPMC/broadcast/anchored kinds: **u64** (the
//!                         multi-consumer engines' cursor domain — count of
//!                         published messages / committed bytes) — the one
//!                         line consumers spin on
//! 136  closed    u64 (atomic) SPMC + broadcast kinds: graceful-close flag
//!                         (in the write-cursor slot — the line consumers
//!                         poll)
//! 144  aux       u64 (atomic) SPMC kinds: starving word (byte ring) /
//!                         dropped_through (element ring; unused, `ShmItem`
//!                         has no drop) — same padded slot.
//!                         Broadcast element kind: the u64 reposition slack
//!                         (create-time config, read once at attach).
//! 152  stride    u64      broadcast kinds only: exact byte stride of one
//!                         capacity unit (element slot stride; 1 for the
//!                         byte kind) — written at create, validated for
//!                         equality on every open; 0 on every other kind
//! 256  read_cursor   usize (atomic, own 128-byte slot; SPSC kinds only)
//!                         broadcast BYTE kind: the u64 `tail_intent`
//!                         (declared write frontier, own padded slot)
//! 264  starving  usize (atomic) producer out-of-space signal (in read slot;
//!                         SPSC kinds only)
//! SPSC kinds + broadcast element kind (5):
//! 384  buffer        capacity * unit_size bytes (SPSC), or capacity slots
//!                    (broadcast elements; see the slot-stride math below)
//! SPMC kinds:
//! 384  consumer table: max_consumers slots of 128 bytes, each
//!        +0  lease   u64  (atomic) opaque token of the slot holder, 0 = free
//!        +8  control u64  (atomic) high u32 epoch | low u32 state
//!                         (0 = FREE, 1 = ACTIVE, 2 = RETIRED)
//!        +16 cursor  u64  (atomic) the consumer's published read cursor
//!            (u64::MAX = detached sentinel; the shared gating engine's
//!             cursor domain — `arch_bits` still rejects a cross-arch
//!             attach on top)
//!      (one line per consumer: the producer's scan reads control + cursor
//!       from the line the consumer's flush already owns)
//! 384 + 128 * max_consumers  buffer  capacity * unit_size bytes
//! Broadcast byte kind (6):
//! 384  latest    u64 (atomic) start of the most recent record — the
//!                    lap-recovery jump target, own 128-byte slot
//! 512  buffer        capacity bytes
//! Anchored kinds (9/10): broadcast's counters ∪ the SPMC table shape —
//! `max_consumers` holds `max_anchors`; the u64 tail at 128 is the unified
//! cursor (both roles spin on it); closed at 136; the third tail-slot word
//! at 144 is the element kind's slack / the byte kind's starving span; the
//! stride word at 152 as for the broadcast kinds. Table slots are exactly
//! the SPMC shape (u64 cursor word):
//! Anchored element kind (9):
//! 384  anchor table   max_anchors slots of 128 bytes
//!        {lease u64, control u64 (epoch|state), cursor u64}
//! 384 + 128 * max_anchors  buffer  capacity element slots
//! Anchored byte kind (10):
//! 256  tail_intent   u64 (atomic) declared write frontier, own slot
//! 384  latest        u64 (atomic) lap-recovery jump target, own slot
//! 512  anchor table   max_anchors slots of 128 bytes (as for kind 9)
//! 512 + 128 * max_anchors  buffer  capacity bytes
//! ```
//!
//! Broadcast element slot-stride math (kind 5): each of the `capacity` slots
//! is the heap ring's `repr(C)` `{ seq: AtomicU64, payload: T-storage }`, so
//! the physical stride is `align_up(max(8, align_of::<T>()) + size_of::<T>(),
//! max(8, align_of::<T>()))` — the payload sits at offset `max(8, align)`
//! (`repr(C)` pads between the seq word and an over-aligned payload) and the
//! whole slot rounds to the slot alignment; in code this is simply
//! `size_of::<Slot<T>>()`, which equals it by construction. The header's `unit_size` records `size_of::<T>()` (the
//! stronger type check); both create and open recompute the stride from the
//! instantiated `T`, and the region-length validation catches a stride
//! mismatch (same-size, higher-alignment `T`s inflate the required length).
//!
//! # Broadcast kinds: lossy rings, read-only lease-free consumers
//!
//! The broadcast kinds (5/6) back [`crate::broadcast`] and
//! [`crate::broadcast_bytes`]. Only the **producer** is a role: it takes the
//! producer lease exactly like every other ring. Consumers are pure readers
//! with no shared state at all — [`attach_shm_consumer`](crate::broadcast::RingBuffer::attach_shm_consumer)
//! validates the header, maps the region **`PROT_READ`**, takes **no
//! lease**, and never writes a byte; membership is unbounded and dropping a
//! consumer is just `munmap`. The read-only mapping is also the enforcement:
//! any store regression in the consumer path is a deterministic SIGSEGV.
//!
//! Like the SPMC kinds, the broadcast `closed` flag is **end-of-session**,
//! not terminal: a new producer attach resets it and the ring is open again
//! (a *crashed* producer never sets it at all — crash detection stays
//! lease/watchdog territory). Producer crash recovery is
//! `force_attach_shm_producer` (or `recover_shm`, the same thing here:
//! consumers keep no shared state, so there is nothing else to reset).
//! Everything published stays drainable throughout; a consumer racing the
//! recovered producer self-heals via the ordinary validation (slot seqlock
//! generations on the element ring; the declared-intent window — kept
//! monotonic across producer sessions — on the byte ring).
//!
//! All multi-byte fields use the host's **native** byte order, and a region is
//! **same-host only**: the producer and consumer are two processes on one
//! machine, so they share endianness (and `arch_bits` rejects a mismatched
//! `usize` width on top of that). This is a live IPC layout, not a portable
//! on-disk or on-wire format — do not persist a region and map it elsewhere.
//!
//! # Trust model
//!
//! **Do not `fork` while holding shm ring handles.** A forked child inherits
//! bit-identical handles; teardown is pid-guarded so the child's exit will
//! not release the parent's roles, but any *use* of an inherited handle
//! violates single-producer/single-consumer and corrupts the ring. Spawn
//! children first, or pass the fd and attach fresh handles in the child.
//!
//! Header validation catches *accidents* (wrong fd, wrong ring type, wrong
//! architecture, corrupted cursors), not adversaries: every process mapping
//! the region can scribble over it, and the rings trust payload bit
//! patterns. Hence all constructors are `unsafe` — the caller asserts the
//! region is only ever touched by cooperating rust-rb handles.
//!
//! # Roles, leases, and crash recovery
//!
//! Each side holds a *lease* — an opaque random token — in the header;
//! dropping a handle releases its lease with a guarded CAS (a stale handle
//! whose role was taken over cannot revoke the successor, and its teardown
//! skips the shared-cursor flush too). Tokens deliberately carry **no
//! liveness meaning**: pids are namespace-relative, zombies look alive, and
//! pids get reused, so whether a holder is really gone is knowledge only the
//! application has. [`create_shm`](BytesRingBuffer::create_shm) takes both
//! roles; `attach_*` claims a free role (`AddrInUse` if held);
//! `force_attach_shm_producer`/`_consumer` unconditionally replace one role
//! and `recover_shm` replaces both — the caller asserts, via the `unsafe`
//! contract, that the previous holder(s) are gone.
//!
//! Because a record becomes visible only through the producer's single
//! `Release` cursor store, a producer that dies mid-write leaves the region
//! fully consistent: everything published is drainable, the unpublished
//! partial record is simply invisible and its space is reused once the
//! producer role is re-taken. Consumer-side recovery is **at-least-once**:
//! the dead consumer's unpublished progress is delivered again — its
//! deferred window (`capacity / 8`, max 4096 bytes / 64 elements) plus, on
//! the byte ring, any in-flight message's record and wrap padding, or the
//! whole in-progress batch if it died mid-`drain`. Worst case approaches a
//! full ring: size deduplication accordingly.
//!
//! Only [`CrossProcess`] wait strategies are accepted: the spin strategies
//! work across processes as-is, while `CvWait`'s mutex/condvar are
//! process-local.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cursor::{consumer_core_from_raw, producer_core_from_raw, AnchorKind, SlotCleanup};
use crate::spsc::{Consumer, Producer, RingBuffer};
use crate::spsc_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer};
use crate::wait::{CrossProcess, SelfTimed};

const MAGIC: u64 = u64::from_le_bytes(*b"rust_rb1");
/// VERSION stays 1 across the SPMC kinds' addition: old binaries reject
/// unknown `kind` values, which is exactly the compatibility story needed.
const VERSION: u32 = 1;
const KIND_BYTES: u32 = 1;
const KIND_ELEMS: u32 = 2;
const KIND_SPMC_BYTES: u32 = 3;
const KIND_SPMC_ELEMS: u32 = 4;
const KIND_BCAST_ELEMS: u32 = 5;
const KIND_BCAST_BYTES: u32 = 6;
const KIND_ANCH_ELEMS: u32 = 9;
const KIND_ANCH_BYTES: u32 = 10;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_KIND: usize = 12;
const OFF_CAPACITY: usize = 16;
const OFF_UNIT_SIZE: usize = 24;
const OFF_ARCH_BITS: usize = 32;
/// SPMC kinds only: the consumer-table size, fixed at create (a mapped
/// layout cannot grow). Fills the 4-byte gap after `arch_bits`.
const OFF_MAX_CONSUMERS: usize = 36;
const OFF_PRODUCER_LEASE: usize = 40;
const OFF_CONSUMER_LEASE: usize = 48;
/// Seqlock-style initialization generation: odd while `create_shm` is
/// (re)writing the header, bumped to even when complete. Validators read it
/// before their header reads and re-check after reads and lease claims; any
/// change (or an odd value) means a concurrent re-create, and the attach
/// fails rather than trusting a chimera of old and new fields.
const OFF_GENERATION: usize = 56;
/// Producer-starving flag (usize atomic): raised by the producer when a
/// space check fails, consumed by the byte ring's immediate-release rule.
/// Lives inside the read-cursor's 128-byte slot: that line already carries
/// the flush/poll traffic, so the flag adds no new line.
const OFF_STARVING: usize = OFF_READ_CURSOR + 8;
const OFF_WRITE_CURSOR: usize = 128;
/// SPMC kinds: the graceful-close flag, co-resident in the write-cursor's
/// 128-byte slot (the line consumers already poll; written once by the
/// producer's graceful drop — a crashed producer never sets it).
const OFF_SPMC_CLOSED: usize = OFF_WRITE_CURSOR + 8;
/// SPMC kinds: the ring's auxiliary write-side word, also in the
/// write-cursor slot — the producer-starving flag for the byte ring, the
/// `dropped_through` watermark position for the element ring (never used:
/// `ShmItem` types have no drop, so the watermark machinery compiles away).
const OFF_SPMC_AUX: usize = OFF_WRITE_CURSOR + 16;
const OFF_READ_CURSOR: usize = 256;
/// Buffer start: past the cursor slots, 128-byte aligned (mappings are
/// page-aligned, so every offset here is honored in memory).
const BUFFER_OFFSET: usize = 384;

// --- Broadcast counter geometry (kinds 5/6) ----------------------------------

/// Broadcast kinds: the u64 tail (the write-cursor slot — every kind's
/// producer-published line).
const OFF_BCAST_TAIL: usize = OFF_WRITE_CURSOR;
/// Broadcast kinds: the graceful-close word, co-resident in the tail's
/// padded slot (the one line consumers spin on) — the same offset as the
/// SPMC closed word.
const OFF_BCAST_CLOSED: usize = OFF_WRITE_CURSOR + 8;
/// Broadcast element kind: the reposition slack (a create-time constructor
/// knob every consumer inherits, validated `< capacity` on attach). Parked
/// in the tail slot's third word — written only at create, so it adds no
/// write traffic to the polled line.
const OFF_BCAST_SLACK: usize = OFF_WRITE_CURSOR + 16;
/// Broadcast kinds: the exact byte stride of one capacity unit in the file
/// (the element kind's slot stride — seq word + payload, alignment-padded;
/// 1 for the byte kind). Written only at create, in the tail slot's fourth
/// word next to `closed`/`slack`; validated for **exact equality** on every
/// open. `unit_size` alone (`size_of::<T>()`) cannot distinguish two types
/// of equal size but different alignment, and a length check only rejects
/// *larger* strides — a lower-alignment `T` would silently misindex every
/// slot.
const OFF_BCAST_STRIDE: usize = OFF_WRITE_CURSOR + 24;
/// Broadcast byte kind: `tail_intent` (the declared write frontier, stored
/// per push, loaded twice per pop) in its own 128-byte slot — the SPSC
/// read-cursor slot, which is meaningless for a lossy ring.
const OFF_BCAST_INTENT: usize = OFF_READ_CURSOR;
/// Broadcast byte kind: `latest` (the lap-recovery jump target, stored per
/// push) in its own 128-byte slot after the intent's — which pushes the
/// byte kind's buffer start to 512.
const OFF_BCAST_LATEST: usize = 384;
/// Broadcast byte kind's buffer start (the element kind keeps the common
/// [`BUFFER_OFFSET`]: it needs no third counter slot).
const BCAST_BYTES_BUFFER_OFFSET: usize = 512;

// --- Anchored counter/table geometry (kinds 9/10) ----------------------------
//
// The anchored kinds are the union of the two shipped designs: broadcast's
// counter slots (the u64 tail doubles as the unified cursor both roles spin
// on) plus the SPMC consumer table (here: the ANCHOR table) with the same
// 128-byte {lease, epoch|state control, cursor} slots — except the cursor
// word is a u64 (the anchored rings' unified cursor domain; the slot
// generations `2s+1/2s+2` require it).
//
// Element kind (9): tail@128, closed@136, slack@144, stride@152 (all in the
// tail's padded slot, broadcast offsets verbatim); table@384 (the SPMC table
// offset — element observers validate via the slot seqlocks and need no
// extra counters); buffer@384 + 128 * max_anchors.
//
// Byte kind (10): tail@128, closed@136, starving@144 (the span-valued
// producer-starving word — the byte kind's third word, where the element
// kind parks its slack), stride@152 (= 1); tail_intent@256 and latest@384
// in their own padded slots (broadcast-bytes offsets verbatim — but 384 is
// the SPMC table offset, so the anchor table moves); table@512;
// buffer@512 + 128 * max_anchors.

/// Anchored element kind: the anchor table starts where the SPMC consumer
/// table does (no intent/latest slots needed). `pub(crate)`: the anchored
/// modules' in-process subscribe paths address their kind's table directly.
pub(crate) const ANCH_ELEMS_TABLE_OFFSET: usize = 384;
/// Anchored byte kind: the table starts past the `latest` slot at 384.
pub(crate) const ANCH_BYTES_TABLE_OFFSET: usize = 512;
/// Anchored byte kind: the producer-starving word (blocked-push span), in
/// the tail slot's third word — the same offset the element kind uses for
/// its slack and the SPMC kinds for their aux word.
const OFF_ANCH_STARVING: usize = OFF_WRITE_CURSOR + 16;

// --- SPMC consumer table geometry (kinds 3/4) --------------------------------

/// The consumer table starts where the SPSC buffer would (the header shape
/// up to here is shared across kinds). `pub(crate)`: the gating modules'
/// in-process subscribe paths address their kind's table directly, exactly
/// as the anchored modules do with their table offsets.
pub(crate) const SPMC_TABLE_OFFSET: usize = BUFFER_OFFSET;
/// One 128-byte slot per consumer: lease, control, and cursor co-resident on
/// one line — the producer's scan touches one line per consumer, and that
/// line already carries the consumer's flush traffic.
const SPMC_SLOT_STRIDE: usize = 128;
const SLOT_LEASE: usize = 0;
const SLOT_CONTROL: usize = 8;
const SLOT_CURSOR: usize = 16;

/// Consumer-cursor detached sentinel — the same `u64::MAX` the heap
/// registries use (`crate::registry::DETACHED`): a claimed-but-not-yet-joined
/// or freed slot imposes no gating constraint.
const SLOT_DETACHED: u64 = u64::MAX;

/// Control-word states (low u32; high u32 is the retirement epoch [A-4.1]).
const STATE_FREE: u32 = 0;
const STATE_ACTIVE: u32 = 1;
const STATE_RETIRED: u32 = 2;

#[inline(always)]
const fn control_word(epoch: u32, state: u32) -> u64 {
    ((epoch as u64) << 32) | state as u64
}

#[inline(always)]
const fn control_epoch(control: u64) -> u32 {
    (control >> 32) as u32
}

#[inline(always)]
const fn control_state(control: u64) -> u32 {
    control as u32
}

/// Whether a consumer-table control word is in the ACTIVE state — the only
/// state whose cursor gates the producer (used by the rings' rescan walks).
#[inline(always)]
pub(crate) const fn control_is_active(control: u64) -> bool {
    control_state(control) == STATE_ACTIVE
}

/// The cursor-sentinel guard, mirrored from the heap registries: a published
/// cursor must never equal [`SLOT_DETACHED`]; one unit less only gates the
/// producer more.
#[inline(always)]
const fn slot_guard(cursor: u64) -> u64 {
    if cursor == SLOT_DETACHED {
        cursor.wrapping_sub(1)
    } else {
        cursor
    }
}

/// Marker for element types that may cross a process boundary through a
/// shared-memory ring.
///
/// # Safety
///
/// Implementors assert that the type is plain data: `Copy`, no pointers,
/// references, or handles that are only meaningful in one address space, and
/// **valid for the bit patterns a cooperating peer writes** (the ring trusts
/// the region's contents — see the module's trust model). Types with
/// validity invariants (`bool`, `char`, most `enum`s, anything with niches)
/// must not be implemented unless the peer is trusted to uphold them.
///
/// # Examples
///
/// Integers, floats, and arrays of them are ready to use:
///
/// ```
/// use std::os::fd::AsFd;
/// use rust_rb::{memfd, RingBuffer};
/// # fn main() -> std::io::Result<()> {
/// let fd = memfd("shmitem-doc")?;
/// // SAFETY: fresh private memfd, only cooperating handles touch it.
/// let (mut tx, mut rx) = unsafe { RingBuffer::<[u64; 4]>::create_shm(fd.as_fd(), 64)? };
/// tx.push([1, 2, 3, 4]);
/// assert_eq!(rx.pop(), [1, 2, 3, 4]);
/// # Ok(())
/// # }
/// ```
///
/// A `#[repr(C)]` plain-old-data struct can opt in — but only if it is valid
/// for *every* bit pattern a peer might write (so no `bool`, `char`, `enum`,
/// or reference fields):
///
/// ```
/// use rust_rb::ShmItem;
///
/// #[derive(Clone, Copy)]
/// #[repr(C)]
/// struct Tick {
///     price: u64,
///     qty: u64,
/// }
///
/// // SAFETY: two plain `u64`s; every bit pattern is a valid `Tick`.
/// unsafe impl ShmItem for Tick {}
/// ```
pub unsafe trait ShmItem: Copy {}

macro_rules! shm_item {
    ($($t:ty),*) => {$(
        // SAFETY: plain integer/float data; every bit pattern is valid.
        unsafe impl ShmItem for $t {}
    )*};
}
shm_item!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

// SAFETY: arrays of plain data are plain data.
unsafe impl<T: ShmItem, const N: usize> ShmItem for [T; N] {}

/// Create a memfd suitable for backing a shared ring.
///
/// Created with close-on-exec set (the safe default — the fd does not leak
/// into unrelated exec'd children). To hand the ring to a child process by
/// fd inheritance, clear the flag first (`fcntl(fd, F_SETFD, 0)`) or pass it
/// over a unix socket.
pub fn memfd(name: &str) -> io::Result<OwnedFd> {
    let cname = std::ffi::CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "memfd name contains NUL"))?;
    // SAFETY: valid NUL-terminated name pointer; flags value is valid.
    let fd = unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a freshly created, owned file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A mapped shared region. Unmapped on drop; the fd itself stays owned by
/// the caller.
pub(crate) struct ShmRegion {
    base: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is process-shared memory; the region wrapper itself
// carries no thread affinity.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // SAFETY: base/len are the exact mapping created in `map`.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

impl ShmRegion {
    fn map(fd: BorrowedFd<'_>, len: usize) -> io::Result<Self> {
        Self::map_prot(fd, len, libc::PROT_READ | libc::PROT_WRITE)
    }

    /// Map `len` bytes of `fd` **read-only**. The broadcast kinds' consumer
    /// attach uses this: a lossy consumer is a pure reader, and the
    /// `PROT_READ` mapping turns any accidental store in its path into a
    /// deterministic SIGSEGV [P-F8].
    fn map_read_only(fd: BorrowedFd<'_>, len: usize) -> io::Result<Self> {
        Self::map_prot(fd, len, libc::PROT_READ)
    }

    fn map_prot(fd: BorrowedFd<'_>, len: usize, prot: libc::c_int) -> io::Result<Self> {
        // SAFETY: length is non-zero and the fd is valid for the borrow.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                prot,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            base: NonNull::new(ptr.cast()).expect("mmap returned null"),
            len,
        })
    }

    #[inline]
    fn at(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset < self.len);
        // SAFETY: offset is within the mapping (validated region size).
        unsafe { self.base.as_ptr().add(offset) }
    }

    /// # Safety
    ///
    /// `offset` must be within the mapping and naturally aligned for `A`.
    unsafe fn atomic<A>(&self, offset: usize) -> &A {
        // SAFETY: per the caller contract; the mapping outlives `&self`.
        unsafe { &*self.at(offset).cast::<A>() }
    }

    /// Atomic header reads: another cooperating process may be initializing
    /// the region concurrently; `read_magic` is the Acquire that orders the
    /// remaining (Relaxed) field reads.
    fn read_magic(&self) -> u64 {
        // SAFETY: OFF_MAGIC is 8-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU64>(OFF_MAGIC) }.load(Ordering::Acquire)
    }

    fn read_u64(&self, offset: usize) -> u64 {
        // SAFETY: header offsets are 8-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU64>(offset) }.load(Ordering::Relaxed)
    }

    fn read_u32(&self, offset: usize) -> u32 {
        // SAFETY: header offsets are 4-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU32>(offset) }.load(Ordering::Relaxed)
    }
}

/// Which lease a handle holds.
#[derive(Clone, Copy)]
pub(crate) enum Role {
    Producer,
    Consumer,
}

impl Role {
    fn lease_offset(self) -> usize {
        match self {
            Role::Producer => OFF_PRODUCER_LEASE,
            Role::Consumer => OFF_CONSUMER_LEASE,
        }
    }
}

/// The shm side of [`AnchorKind`]: keeps the mapping alive, carries this
/// handle's (per-process, [`CrossProcess`]) wait strategies, and releases
/// the role lease on drop.
pub(crate) struct ShmAnchor<P, C> {
    region: Arc<ShmRegion>,
    role: Role,
    /// The opaque token this handle wrote into its role lease.
    token: u64,
    /// The process that constructed this handle. A `fork`ed child inherits a
    /// bit-identical anchor (same token), so token equality alone cannot
    /// tell the original from the copy — teardown and ownership checks also
    /// require running in the constructing process, or the child's exit
    /// would release (or flush over) the parent's live role.
    owner_pid: libc::pid_t,
    pub(crate) producer_wait: P,
    pub(crate) consumer_wait: C,
}

impl<P, C> ShmAnchor<P, C> {
    /// Whether the role lease still holds this handle's token (i.e. no one
    /// has force-taken the role). Consulted before teardown touches shared
    /// state; a takeover racing this check is the force caller's asserted
    /// responsibility ("the previous holder is gone").
    pub(crate) fn owns_lease(&self) -> bool {
        // SAFETY: lease offsets are 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(self.role.lease_offset()) };
        lease.load(Ordering::Acquire) == self.token
    }

    /// Whether we are the process that constructed this handle. The getpid
    /// comparison is same-lineage only (never cross-namespace): a forked
    /// child is not the owner even though its token matches bit-for-bit.
    /// Syscall cost — teardown paths only.
    pub(crate) fn owned_by_current_process(&self) -> bool {
        // SAFETY: getpid is always safe.
        (unsafe { libc::getpid() }) == self.owner_pid
    }
}

impl<P, C> Drop for ShmAnchor<P, C> {
    fn drop(&mut self) {
        // Guarded release: free the lease only if (a) we are the process
        // that constructed the handle — a fork-inherited copy carries a
        // bit-identical token and its exit must NOT release the parent's
        // live role — and (b) the lease still holds our token (after a
        // force-steal it holds the successor's, and a zombie's late drop
        // must not revoke it).
        // SAFETY: getpid is always safe; lease offsets are 8-aligned and
        // inside the mapping.
        if !self.owned_by_current_process() {
            return;
        }
        // SAFETY: lease offsets are 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(self.role.lease_offset()) };
        let _ = lease.compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// A fresh opaque lease token: random (per-handle), never 0.
///
/// Tokens deliberately carry **no liveness meaning**. An earlier design
/// stored the holder's pid and probed it with `kill(pid, 0)`; that is wrong
/// in exactly the situations shm rings are for — pids are namespace-relative
/// (a container's pid 42 is not the host's), zombies still "exist", pids get
/// reused, and a u64 lease does not even fit `pid_t`. Whether a previous
/// holder is really gone is knowledge only the caller can have, which is why
/// the force/recover constructors are `unsafe` and unconditional.
fn lease_token() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    loop {
        // RandomState is freshly seeded per instance.
        let mut h = RandomState::new().build_hasher();
        h.write_u64(std::process::id() as u64);
        let t = h.finish();
        if t != 0 {
            return t;
        }
    }
}

/// Claim a free `role` in the region. Fails with `AddrInUse` if any holder's
/// token is present — cooperative exclusivity, no liveness guessing.
/// `generation` is the seqlock snapshot from validation: if `create_shm`
/// re-initialized the region between the header reads and this claim, the
/// claim is rolled back and the attach fails — otherwise the creator's
/// unconditional lease stores could silently overwrite ours, leaving two
/// holders of one role.
fn claim_lease(region: &ShmRegion, role: Role, generation: u64) -> io::Result<u64> {
    // SAFETY: lease offsets are 8-aligned and inside the mapping.
    let lease: &AtomicU64 = unsafe { region.atomic(role.lease_offset()) };
    let token = lease_token();
    match lease.compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => {
            // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
            let gen_now =
                unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
            if gen_now != generation {
                let _ = lease.compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "ring was re-initialized during attach",
                ));
            }
            Ok(token)
        }
        Err(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "ring role already held (drop the existing handle, or use the \
             force/recover constructors if the holder is known dead)",
        )),
    }
}

/// Unconditionally take `role`, replacing whatever token is there. The
/// caller (via the `unsafe` constructor contract) asserts the previous
/// holder is gone; its guarded Drop can no longer release the new token.
fn force_claim_lease(region: &ShmRegion, role: Role) -> u64 {
    // SAFETY: lease offsets are 8-aligned and inside the mapping.
    let lease: &AtomicU64 = unsafe { region.atomic(role.lease_offset()) };
    let token = lease_token();
    lease.swap(token, Ordering::AcqRel);
    token
}

fn region_len(capacity: usize, unit_size: usize) -> io::Result<usize> {
    let len = capacity
        .checked_mul(unit_size)
        .and_then(|b| b.checked_add(BUFFER_OFFSET))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region"))?;
    // `ftruncate`/`mmap` take `off_t`, which is 32-bit on some 32-bit Linux
    // targets — a length in [2^31, off_t::MAX+1) would sign-flip negative and
    // surface as a bare EINVAL instead of this validation error.
    if len as u64 > libc::off_t::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "region length exceeds the platform file-offset limit",
        ));
    }
    Ok(len)
}

/// Initialize a fresh region: size the fd, map it, write the header, take
/// both leases.
fn create_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    capacity: usize,
    unit_size: usize,
) -> io::Result<(Arc<ShmRegion>, u64, u64)> {
    let len = region_len(capacity, unit_size)?;
    // SAFETY: valid fd for the borrow; `region_len` confirmed `len` fits off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // All header accesses go through atomics: plain stores would let the
    // compiler elide the magic clear (a "dead" store) and give a concurrent
    // attacher in another process no ordering at all.
    // SAFETY: all header offsets are naturally aligned and inside the mapping.
    let (producer_token, consumer_token) = unsafe {
        // Seqlock write protocol: make the generation odd (initializing)
        // BEFORE touching anything, so a concurrent validator that read any
        // pre-clear state sees the generation change on its re-check and
        // discards what it read — it can never act on a chimera of old and
        // new fields, nor keep a lease the stores below overwrite.
        let generation = region.atomic::<AtomicU64>(OFF_GENERATION);
        let g = generation.load(Ordering::Relaxed);
        generation.store(g | 1, Ordering::SeqCst);
        // Invalidate any previous ring in this fd (SeqCst: neither elidable
        // nor able to sink past the field stores).
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        region
            .atomic::<AtomicU32>(OFF_VERSION)
            .store(VERSION, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_KIND)
            .store(kind, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CAPACITY)
            .store(capacity as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_UNIT_SIZE)
            .store(unit_size as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_ARCH_BITS)
            .store(usize::BITS, Ordering::Relaxed);
        // The consumer-table size is an SPMC-kind field; keep it
        // deterministically zero on the SPSC kinds — a reused fd may carry a
        // previous SPMC ring's non-zero value here.
        region
            .atomic::<AtomicU32>(OFF_MAX_CONSUMERS)
            .store(0, Ordering::Relaxed);
        let pt = lease_token();
        let ct = lease_token();
        region
            .atomic::<AtomicU64>(OFF_PRODUCER_LEASE)
            .store(pt, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CONSUMER_LEASE)
            .store(ct, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_WRITE_CURSOR)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_READ_CURSOR)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_STARVING)
            .store(0, Ordering::Relaxed);
        // The stride word is a broadcast-kind field; keep it
        // deterministically zero here too (a reused fd may carry a previous
        // broadcast ring's stride).
        region
            .atomic::<AtomicU64>(OFF_BCAST_STRIDE)
            .store(0, Ordering::Relaxed);
        // Publish the magic last with Release: an attacher that Acquire-loads
        // it sees every header field above.
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(MAGIC, Ordering::Release);
        // Seqlock close: even generation, strictly greater than before.
        generation.store((g | 1).wrapping_add(1), Ordering::Release);
        (pt, ct)
    };

    // Match the heap ring's zeroed-buffer guarantee even when the fd is
    // reused and carries old contents (one pass at create time). This also
    // pre-faults and commits every buffer page here rather than on the first
    // hot-path touch — desirable for a latency-sensitive ring, and the whole
    // constructor is cold.
    // SAFETY: the buffer span is inside the mapping.
    unsafe {
        std::ptr::write_bytes(region.at(BUFFER_OFFSET), 0, len - BUFFER_OFFSET);
    }
    Ok((Arc::new(region), producer_token, consumer_token))
}

/// Size of the file behind `fd`.
fn fd_len(fd: BorrowedFd<'_>) -> io::Result<u64> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid fd for the borrow; `st` is a zeroed out-param.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st.st_size as u64)
}

/// Map and validate an existing region. `min_capacity`/`cursor_align` are
/// the ring kind's extra invariants: the byte ring requires capacity >= 8
/// (its `max_message_len` arithmetic underflows below that) and
/// record-aligned cursors (its frame decoder does aligned u32 header reads).
fn open_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    unit_size: usize,
    min_capacity: usize,
    cursor_align: usize,
) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
    // Touching mapped pages past the file's end is SIGBUS, not an error
    // return — validate the size (once) before mapping.
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let file_len = fd_len(fd)?;
    if file_len < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn the capacity.
    let header = ShmRegion::map(fd, BUFFER_OFFSET)?;
    // Seqlock read: snapshot the generation before any header read; callers
    // re-check it after their lease claim. Odd = mid-initialization.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if generation & 1 == 1 {
        return Err(err("ring is being initialized by another process"));
    }
    if header.read_magic() != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch (bytes vs element ring)"));
    }
    if unit_size == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if header.read_u64(OFF_UNIT_SIZE) != unit_size as u64 {
        return Err(err("element size mismatch"));
    }
    if header.read_u32(OFF_ARCH_BITS) != usize::BITS {
        return Err(err("architecture (usize width) mismatch"));
    }
    let capacity = header.read_u64(OFF_CAPACITY) as usize;
    if capacity == 0 || !capacity.is_power_of_two() || capacity < min_capacity {
        return Err(err("corrupt capacity"));
    }
    drop(header);

    let len = region_len(capacity, unit_size)?;
    if file_len < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = ShmRegion::map(fd, len)?;

    // Cursor invariant: occupancy (wrapped) within capacity. The two loads
    // are separate snapshots, and on a live busy ring the pair can be
    // mutually inconsistent (read sampled after the producer moved write),
    // which is NOT corruption — retry for a stable pair, and only judge
    // occupancy on one. Alignment holds for every individually-published
    // value, so it can be checked on any snapshot.
    // SAFETY: cursor offsets are aligned and inside the mapping.
    let write_at = unsafe { region.atomic::<AtomicUsize>(OFF_WRITE_CURSOR) };
    let read_at = unsafe { region.atomic::<AtomicUsize>(OFF_READ_CURSOR) };
    let mut stable = None;
    for _ in 0..64 {
        let w1 = write_at.load(Ordering::Acquire);
        let r = read_at.load(Ordering::Acquire);
        let w2 = write_at.load(Ordering::Acquire);
        if w1 % cursor_align != 0 || r % cursor_align != 0 {
            return Err(err("corrupt cursors: not record-aligned"));
        }
        if w1 == w2 {
            stable = Some((w1, r));
            break;
        }
    }
    // A ring too busy for 64 stable snapshots is by definition live, not
    // corrupt; skip the occupancy judgement rather than misdiagnose it.
    if let Some((write, read)) = stable {
        if write.wrapping_sub(read) > capacity {
            return Err(err("corrupt cursors: occupancy exceeds capacity"));
        }
    }
    // Seqlock re-check on the full mapping: if the header changed while we
    // were reading it, everything above may be a chimera.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    if unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire) != generation {
        return Err(err("ring was re-initialized during validation"));
    }
    Ok((Arc::new(region), capacity, generation))
}

impl ShmRegion {
    fn cursors(
        &self,
    ) -> (
        NonNull<AtomicUsize>,
        NonNull<AtomicUsize>,
        NonNull<AtomicUsize>,
    ) {
        (
            NonNull::new(self.at(OFF_WRITE_CURSOR).cast()).expect("mapping is non-null"),
            NonNull::new(self.at(OFF_READ_CURSOR).cast()).expect("mapping is non-null"),
            NonNull::new(self.at(OFF_STARVING).cast()).expect("mapping is non-null"),
        )
    }

    fn buffer<B>(&self) -> NonNull<B> {
        debug_assert!(BUFFER_OFFSET % std::mem::align_of::<B>() == 0);
        NonNull::new(self.at(BUFFER_OFFSET).cast()).expect("mapping is non-null")
    }

    // --- SPMC accessors (meaningful only over regions of the SPMC kinds;
    //     offsets are validated header geometry, inside any mapping that
    //     passed `open_spmc_region`/`create_spmc_region`) ---

    /// The producer's published write cursor (the SPMC kinds' u64 cursor
    /// word — the same offset the broadcast/anchored kinds call `tail`).
    pub(crate) fn spmc_write_cursor(&self) -> &AtomicU64 {
        // SAFETY: OFF_WRITE_CURSOR is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_WRITE_CURSOR) }
    }

    /// The graceful-close word (0 = open).
    pub(crate) fn spmc_closed(&self) -> &AtomicU64 {
        // SAFETY: OFF_SPMC_CLOSED is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_SPMC_CLOSED) }
    }

    /// The auxiliary write-side word (byte ring: starving flag; element
    /// ring: reserved).
    pub(crate) fn spmc_aux(&self) -> &AtomicU64 {
        // SAFETY: OFF_SPMC_AUX is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_SPMC_AUX) }
    }

    /// The region's seqlock generation (Acquire), for the in-process
    /// subscribe paths' claim re-check (the shm attach paths read the same
    /// word through `open_*_region`).
    pub(crate) fn generation(&self) -> u64 {
        // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire)
    }

    /// A consumer slot's published read cursor (u64 — the shared gating
    /// engine's cursor domain; the SPMC-kind shortcut for
    /// [`anch_slot_cursor`](Self::anch_slot_cursor) at `SPMC_TABLE_OFFSET`).
    /// `slot` must be below the region's `max_consumers` (upheld by every
    /// caller; the table is inside the validated mapping exactly up to
    /// there). The lease/control words are reached through the
    /// table-offset-parameterized accessors below.
    pub(crate) fn slot_cursor(&self, slot: usize) -> &AtomicU64 {
        self.anch_slot_cursor(SPMC_TABLE_OFFSET, slot)
    }

    /// Base of an SPMC region's buffer: past the consumer table, 128-byte
    /// aligned (element alignment above 128 is rejected at construction).
    pub(crate) fn spmc_buffer(&self, max_consumers: usize) -> NonNull<u8> {
        NonNull::new(self.at(SPMC_TABLE_OFFSET + max_consumers * SPMC_SLOT_STRIDE))
            .expect("mapping is non-null")
    }

    // --- Broadcast accessors (meaningful only over regions of the
    //     broadcast kinds; offsets are validated header geometry, inside
    //     any mapping that passed `open_bcast_region`/`create_bcast_region`.
    //     Consumers hold these through read-only mappings: loads only.) ---

    /// The broadcast tail (published message count / committed bytes).
    pub(crate) fn bcast_tail(&self) -> &AtomicU64 {
        // SAFETY: OFF_BCAST_TAIL is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_BCAST_TAIL) }
    }

    /// The broadcast graceful-close word (0 = open).
    pub(crate) fn bcast_closed(&self) -> &AtomicU64 {
        // SAFETY: OFF_BCAST_CLOSED is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_BCAST_CLOSED) }
    }

    /// The broadcast element ring's reposition slack (create-time config).
    pub(crate) fn bcast_slack(&self) -> u64 {
        self.read_u64(OFF_BCAST_SLACK)
    }

    /// The broadcast byte ring's `tail_intent` (declared write frontier).
    pub(crate) fn bcast_intent(&self) -> &AtomicU64 {
        // SAFETY: OFF_BCAST_INTENT is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_BCAST_INTENT) }
    }

    /// The broadcast byte ring's `latest` (lap-recovery jump target).
    pub(crate) fn bcast_latest(&self) -> &AtomicU64 {
        // SAFETY: OFF_BCAST_LATEST is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_BCAST_LATEST) }
    }

    /// Base of a broadcast element region's slot buffer (128-byte aligned;
    /// slot alignment above 128 is rejected at construction).
    pub(crate) fn bcast_elem_buffer(&self) -> NonNull<u8> {
        NonNull::new(self.at(BUFFER_OFFSET)).expect("mapping is non-null")
    }

    /// Base of a broadcast byte region's buffer (past the third counter
    /// slot).
    pub(crate) fn bcast_bytes_buffer(&self) -> NonNull<u8> {
        NonNull::new(self.at(BCAST_BYTES_BUFFER_OFFSET)).expect("mapping is non-null")
    }

    // --- Anchored accessors (meaningful only over regions of the anchored
    //     kinds; offsets are validated header geometry, inside any mapping
    //     that passed `open_anch_region`/`create_anch_region`). The counter
    //     words reuse the broadcast accessors above — the anchored kinds
    //     share those offsets by design (tail/closed at 128/136, the byte
    //     kind's intent/latest at 256/384); only the table geometry is new,
    //     parameterized by the kind's table offset (384 elems / 512 bytes).
    //     Observers hold these through read-only mappings: loads only. ---

    /// The anchored byte kind's producer-starving word (blocked-push span).
    pub(crate) fn anch_starving(&self) -> &AtomicU64 {
        // SAFETY: OFF_ANCH_STARVING is 8-aligned and inside the mapping.
        unsafe { self.atomic(OFF_ANCH_STARVING) }
    }

    /// Byte offset of an anchor-table slot field.
    #[inline]
    fn anch_slot_off(table_offset: usize, slot: usize, field: usize) -> usize {
        table_offset + slot * SPMC_SLOT_STRIDE + field
    }

    /// An anchor slot's lease word (`slot` below the region's `max_anchors`,
    /// upheld by every caller — as for the SPMC table).
    pub(crate) fn anch_slot_lease(&self, table_offset: usize, slot: usize) -> &AtomicU64 {
        // SAFETY: 8-aligned (stride 128, field 0) and inside the mapping for
        // any valid slot index.
        unsafe { self.atomic(Self::anch_slot_off(table_offset, slot, SLOT_LEASE)) }
    }

    /// An anchor slot's control word (`epoch | state`).
    pub(crate) fn anch_slot_control(&self, table_offset: usize, slot: usize) -> &AtomicU64 {
        // SAFETY: 8-aligned (stride 128, field 8) and inside the mapping for
        // any valid slot index.
        unsafe { self.atomic(Self::anch_slot_off(table_offset, slot, SLOT_CONTROL)) }
    }

    /// An anchor slot's published **u64** read cursor, matching the SPMC
    /// table's [`slot_cursor`](Self::slot_cursor) — both consumer tables share
    /// the unified `u64` cursor domain.
    pub(crate) fn anch_slot_cursor(&self, table_offset: usize, slot: usize) -> &AtomicU64 {
        // SAFETY: 8-aligned (stride 128, field 16) and inside the mapping
        // for any valid slot index.
        unsafe { self.atomic(Self::anch_slot_off(table_offset, slot, SLOT_CURSOR)) }
    }

    /// Base of an anchored region's buffer: past the anchor table, 128-byte
    /// aligned (slot alignment above 128 is rejected at construction).
    pub(crate) fn anch_buffer(&self, table_offset: usize, max_anchors: usize) -> NonNull<u8> {
        NonNull::new(self.at(table_offset + max_anchors * SPMC_SLOT_STRIDE))
            .expect("mapping is non-null")
    }
}

/// Build one producer handle core over a validated region.
///
/// # Safety
///
/// Region layout must match `B` (validated by `open_region`/`create_region`).
unsafe fn shm_producer_core<B, P, C>(
    region: Arc<ShmRegion>,
    capacity: usize,
    token: u64,
) -> crate::cursor::ProducerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read, starving) = region.cursors();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(Box::new(ShmAnchor {
        region,
        role: Role::Producer,
        token,
        // SAFETY: getpid is always safe.
        owner_pid: unsafe { libc::getpid() },
        producer_wait: P::default(),
        consumer_wait: C::default(),
    }));
    // SAFETY: pointers reference the live mapping the anchor keeps alive;
    // cursor invariant validated by the caller.
    unsafe { producer_core_from_raw(buf, capacity, write, read, starving, anchor) }
}

/// Build one consumer handle core over a validated region (see
/// `shm_producer_core`).
///
/// # Safety
///
/// As for `shm_producer_core`.
unsafe fn shm_consumer_core<B, P, C>(
    region: Arc<ShmRegion>,
    capacity: usize,
    token: u64,
) -> crate::cursor::ConsumerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read, starving) = region.cursors();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(Box::new(ShmAnchor {
        region,
        role: Role::Consumer,
        token,
        // SAFETY: getpid is always safe.
        owner_pid: unsafe { libc::getpid() },
        producer_wait: P::default(),
        consumer_wait: C::default(),
    }));
    // SAFETY: as for `shm_producer_core`.
    unsafe { consumer_core_from_raw(buf, capacity, write, read, starving, anchor) }
}

/// Claim (or force-take) the producer role over a validated region and wrap
/// the core in the ring's handle type. One implementation for all ten
/// attach/force/recover constructors across both rings.
///
/// # Safety
///
/// As for `shm_producer_core`: the region layout must match `B`.
unsafe fn attach_producer_role<B, P, C, H>(
    opened: (Arc<ShmRegion>, usize, u64),
    force: bool,
    wrap: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> H,
) -> io::Result<H>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, capacity, generation) = opened;
    let token = if force {
        force_claim_lease(&region, Role::Producer)
    } else {
        claim_lease(&region, Role::Producer, generation)?
    };
    // SAFETY: forwarded caller contract (validated region).
    Ok(wrap(unsafe {
        shm_producer_core::<B, P, C>(region, capacity, token)
    }))
}

/// Build both handle cores over a freshly-created region and wrap them in the
/// ring's handle types — the shared body of both rings' `create_shm_with`.
///
/// # Safety
///
/// The region must have been initialized by `create_region` for this ring
/// kind (layout matches `B`).
unsafe fn create_pair<B, P, C, TX, RX>(
    created: (Arc<ShmRegion>, u64, u64),
    capacity: usize,
    wrap_tx: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> TX,
    wrap_rx: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> RX,
) -> (TX, RX)
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, pt, ct) = created;
    // SAFETY: freshly initialized region matches the ring layout.
    unsafe {
        (
            wrap_tx(shm_producer_core::<B, P, C>(region.clone(), capacity, pt)),
            wrap_rx(shm_consumer_core::<B, P, C>(region, capacity, ct)),
        )
    }
}

/// Force-take BOTH roles over a validated region and wrap them in the ring's
/// handle types — the shared body of both rings' `recover_shm_with`. Force
/// cannot fail, so no partial-failure path can leak a lease.
///
/// # Safety
///
/// As for [`attach_producer_role`]/[`attach_consumer_role`].
#[allow(clippy::type_complexity)]
unsafe fn recover_pair<B, P, C, TX, RX>(
    opened: (Arc<ShmRegion>, usize, u64),
    wrap_tx: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> TX,
    wrap_rx: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> RX,
) -> io::Result<(TX, RX)>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let producer_opened = (opened.0.clone(), opened.1, opened.2);
    // SAFETY: forwarded caller contract (validated region).
    unsafe {
        let tx = attach_producer_role(producer_opened, true, wrap_tx)?;
        let rx = attach_consumer_role(opened, true, wrap_rx)?;
        Ok((tx, rx))
    }
}

/// Consumer analog of [`attach_producer_role`].
///
/// # Safety
///
/// As for `shm_consumer_core`.
unsafe fn attach_consumer_role<B, P, C, H>(
    opened: (Arc<ShmRegion>, usize, u64),
    force: bool,
    wrap: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> H,
) -> io::Result<H>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, capacity, generation) = opened;
    let token = if force {
        force_claim_lease(&region, Role::Consumer)
    } else {
        claim_lease(&region, Role::Consumer, generation)?
    };
    // SAFETY: forwarded caller contract (validated region).
    Ok(wrap(unsafe {
        shm_consumer_core::<B, P, C>(region, capacity, token)
    }))
}

/// Both halves of a shm-backed byte ring.
pub type BytesPair<P, C> = (BytesProducer<P, C>, BytesConsumer<P, C>);
/// Both halves of a shm-backed element ring.
pub type ElemPair<T, P, C> = (Producer<T, P, C>, Consumer<T, P, C>);

/// Byte-ring capacity floor and record alignment, shared with the ring's own
/// constructor and frame decoder so they cannot drift.
const BYTES_MIN_CAPACITY: usize = crate::spsc_bytes::MIN_CAPACITY;
const BYTES_CURSOR_ALIGN: usize = crate::spsc_bytes::ALIGN;

impl BytesRingBuffer {
    /// Initialize `fd` as a fresh shm-backed byte ring and return both
    /// halves, with default ([`YieldWait`](crate::wait::YieldWait)) wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self): the region must only ever be
    /// accessed by cooperating rust-rb handles.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<(BytesProducer, BytesConsumer)> {
        // SAFETY: forwarded caller contract.
        unsafe { BytesRingBuffer::<_, _>::create_shm_with(fd, min_capacity) }
    }

    /// Unconditionally take over **both** roles of an existing ring —
    /// typically after both holders died. Everything already published is
    /// intact and drainable (see the module docs on crash consistency);
    /// messages the dead consumer had consumed but not yet published — up to
    /// the deferred-publish window (`capacity / 8`, max 4096 bytes) — are
    /// **delivered again** (recovery is at-least-once).
    ///
    /// # Safety
    ///
    /// Trust model, plus: the takeover is unconditional. The caller asserts
    /// both previous holders are gone; a still-live holder would keep
    /// writing concurrently and corrupt the ring. (Their late `Drop`s are
    /// harmless — lease release is guarded by token.)
    pub unsafe fn recover_shm(fd: BorrowedFd<'_>) -> io::Result<(BytesProducer, BytesConsumer)> {
        // SAFETY: forwarded caller contract.
        unsafe { BytesRingBuffer::<_, _>::recover_shm_with(fd) }
    }
}

impl<P, C> BytesRingBuffer<P, C>
where
    P: CrossProcess + Send + Sync,
    C: CrossProcess + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
        open_region(fd, KIND_BYTES, 1, BYTES_MIN_CAPACITY, BYTES_CURSOR_ALIGN)
    }

    /// [`create_shm`](BytesRingBuffer::create_shm) with explicit
    /// [`CrossProcess`] wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<BytesPair<P, C>> {
        let capacity = crate::cursor::round_capacity(min_capacity, BYTES_MIN_CAPACITY);
        let created = create_region(fd, KIND_BYTES, capacity, 1)?;
        // SAFETY: freshly initialized region matches the byte-ring layout.
        Ok(unsafe {
            create_pair(
                created,
                capacity,
                BytesProducer::from_core,
                BytesConsumer::from_core,
            )
        })
    }

    /// Attach to an existing ring as the producer. Fails with `AddrInUse`
    /// while the role's lease is held (cooperative exclusivity — see
    /// [`force_attach_shm_producer`](Self::force_attach_shm_producer) when
    /// the holder is known dead).
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer: the caller asserts no other live
    /// producer handle exists (the lease enforces this against cooperating
    /// processes only).
    pub unsafe fn attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<BytesProducer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, false, BytesProducer::from_core) }
    }

    /// Attach to an existing ring as the consumer (see
    /// [`attach_shm_producer`](Self::attach_shm_producer)).
    ///
    /// # Safety
    ///
    /// Trust model, plus single-consumer.
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, false, BytesConsumer::from_core) }
    }

    /// Unconditionally take over the **producer** role — single-side crash
    /// recovery while the consumer keeps running. Publishing resumes exactly
    /// after the last record the dead producer published (a partial,
    /// unpublished record is invisible and its space is reused).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone (a live one would corrupt the ring).
    pub unsafe fn force_attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<BytesProducer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, true, BytesProducer::from_core) }
    }

    /// Unconditionally take over the **consumer** role — single-side crash
    /// recovery while the producer keeps running. Consumption resumes at the
    /// dead consumer's last *published* cursor: messages it consumed but had
    /// not yet published are delivered again (at-least-once). The window is
    /// the deferred-publish clamp (`capacity / 8`, max 4096 bytes) **plus
    /// any in-flight message's record and wrap padding** (each up to
    /// `capacity / 2`) if it died holding a [`Msg`](crate::spsc_bytes::Msg),
    /// or the entire in-progress batch if it died mid-`drain` — size
    /// deduplication windows for up to a full ring of redelivery.
    ///
    /// # Safety
    ///
    /// As for [`force_attach_shm_producer`](Self::force_attach_shm_producer),
    /// for the consumer role.
    pub unsafe fn force_attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, true, BytesConsumer::from_core) }
    }

    /// [`recover_shm`](BytesRingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](BytesRingBuffer::recover_shm).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<BytesPair<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe {
            recover_pair(
                Self::open(fd)?,
                BytesProducer::from_core,
                BytesConsumer::from_core,
            )
        }
    }
}

/// Element-type invariants for shm rings, enforced consistently on create
/// AND attach as errors (never panics — these surface on fallible paths).
fn check_elem_type<T>() -> io::Result<()> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    if std::mem::size_of::<T>() == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if std::mem::align_of::<T>() > 128 {
        return Err(err("element alignment exceeds the buffer offset alignment"));
    }
    Ok(())
}

impl<T: ShmItem + Send> RingBuffer<T> {
    /// Initialize `fd` as a fresh shm-backed element ring (default wait
    /// strategies).
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<(Producer<T>, Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { RingBuffer::<T, _, _>::create_shm_with(fd, min_capacity) }
    }

    /// Unconditionally take over both roles of an existing element ring (see
    /// [`BytesRingBuffer::recover_shm`], including the at-least-once
    /// re-delivery window — up to `capacity / 8`, max 64 elements).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::recover_shm`].
    pub unsafe fn recover_shm(fd: BorrowedFd<'_>) -> io::Result<(Producer<T>, Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { RingBuffer::<T, _, _>::recover_shm_with(fd) }
    }
}

impl<T, P, C> RingBuffer<T, P, C>
where
    T: ShmItem + Send,
    P: CrossProcess + Send + Sync,
    C: CrossProcess + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
        // The invariants create enforces must hold on ATTACH too — an
        // attacher instantiated with a different `T` of the same size but
        // higher alignment would otherwise get misaligned slots (UB). These
        // are environmental/typed misuses on a fallible path: errors, not
        // panics.
        check_elem_type::<T>()?;
        // Element cursors count whole elements; any value is decodable.
        open_region(fd, KIND_ELEMS, std::mem::size_of::<T>(), 1, 1)
    }

    /// [`create_shm`](RingBuffer::create_shm) with explicit [`CrossProcess`]
    /// wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<ElemPair<T, P, C>> {
        check_elem_type::<T>()?;
        let capacity = crate::cursor::round_capacity(min_capacity, 1);
        let created = create_region(fd, KIND_ELEMS, capacity, std::mem::size_of::<T>())?;
        // SAFETY: freshly initialized region matches the element-ring layout.
        Ok(unsafe { create_pair(created, capacity, Producer::from_core, Consumer::from_core) })
    }

    /// Attach to an existing element ring as the producer.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size is validated).
    pub unsafe fn attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, false, Producer::from_core) }
    }

    /// Attach to an existing element ring as the consumer.
    ///
    /// # Safety
    ///
    /// As for [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<Consumer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, false, Consumer::from_core) }
    }

    /// Unconditionally take over the producer role (see
    /// [`BytesRingBuffer::force_attach_shm_producer`]).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::force_attach_shm_producer`], plus the `T`
    /// caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, true, Producer::from_core) }
    }

    /// Unconditionally take over the consumer role (see
    /// [`BytesRingBuffer::force_attach_shm_consumer`]; the at-least-once
    /// window is `capacity / 8`, max 64 elements).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::force_attach_shm_consumer`], plus the `T`
    /// caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<Consumer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, true, Consumer::from_core) }
    }

    /// [`recover_shm`](RingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::recover_shm`], plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<ElemPair<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { recover_pair(Self::open(fd)?, Producer::from_core, Consumer::from_core) }
    }
}

// =============================================================================
// GATING SPMC rings (kinds 3/4): consumer table, slot leases, retirement.
// =============================================================================
//
// The producer role reuses the SPSC lease at `OFF_PRODUCER_LEASE` verbatim.
// Consumers hold *per-slot* leases inside the consumer table instead of the
// single `OFF_CONSUMER_LEASE`: membership is dynamic, so exclusivity is per
// slot, not per role. The zombie answer [A-4.1] is slot retirement: the
// control word carries `{epoch | state}`, `force_detach_consumer` is a
// compare-and-retire (`ACTIVE@epoch -> RETIRED@epoch+1`, refusing a
// mismatched occupancy), and a retired slot is never re-issued until
// `recover_shm` resets the whole table. A live "zombie" (a wrong death
// assertion) keeps flushing into a slot no scan reads — the blast radius is
// one burned slot plus the zombie's own reads losing gating protection
// (`force_detach` revokes the victim's read validity — the same trust
// register as `force_attach`).

/// Region length for an SPMC ring: header + consumer table + buffer.
fn spmc_region_len(capacity: usize, unit_size: usize, max_consumers: usize) -> io::Result<usize> {
    let err = || io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region");
    let table = max_consumers
        .checked_mul(SPMC_SLOT_STRIDE)
        .ok_or_else(err)?;
    let len = capacity
        .checked_mul(unit_size)
        .and_then(|b| b.checked_add(SPMC_TABLE_OFFSET))
        .and_then(|b| b.checked_add(table))
        .ok_or_else(err)?;
    // Same off_t clamp as `region_len` (32-bit sign-flip guard).
    if len as u64 > libc::off_t::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "region length exceeds the platform file-offset limit",
        ));
    }
    Ok(len)
}

/// Initialize a fresh SPMC region: size the fd, map it, write the header and
/// a fully-FREE consumer table, take the producer lease. The seqlock write
/// protocol is identical to `create_region`'s.
fn create_spmc_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    capacity: usize,
    unit_size: usize,
    max_consumers: usize,
) -> io::Result<(Arc<ShmRegion>, u64)> {
    if max_consumers == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_consumers must be at least 1",
        ));
    }
    // The header stores the table size as a u32: reject anything the store
    // below would silently truncate (before any layout math trusts it).
    if max_consumers > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_consumers exceeds the header field width (u32)",
        ));
    }
    let len = spmc_region_len(capacity, unit_size, max_consumers)?;
    // SAFETY: valid fd for the borrow; `spmc_region_len` confirmed `len`
    // fits off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // SAFETY: all header offsets are naturally aligned and inside the
    // mapping (see `create_region` for the seqlock/atomics rationale).
    let producer_token = unsafe {
        // Seqlock open: odd generation before touching anything.
        let generation = region.atomic::<AtomicU64>(OFF_GENERATION);
        let g = generation.load(Ordering::Relaxed);
        generation.store(g | 1, Ordering::SeqCst);
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        region
            .atomic::<AtomicU32>(OFF_VERSION)
            .store(VERSION, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_KIND)
            .store(kind, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CAPACITY)
            .store(capacity as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_UNIT_SIZE)
            .store(unit_size as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_ARCH_BITS)
            .store(usize::BITS, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_MAX_CONSUMERS)
            .store(max_consumers as u32, Ordering::Relaxed);
        let pt = lease_token();
        region
            .atomic::<AtomicU64>(OFF_PRODUCER_LEASE)
            .store(pt, Ordering::Relaxed);
        // The SPSC consumer lease is unused by SPMC kinds; keep it
        // deterministically zero.
        region
            .atomic::<AtomicU64>(OFF_CONSUMER_LEASE)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_WRITE_CURSOR)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_SPMC_CLOSED)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_SPMC_AUX)
            .store(0, Ordering::Relaxed);
        // The stride word is a broadcast-kind field; keep it
        // deterministically zero on the SPMC kinds (a reused fd may carry a
        // previous broadcast ring's stride).
        region
            .atomic::<AtomicU64>(OFF_BCAST_STRIDE)
            .store(0, Ordering::Relaxed);
        // Table + buffer: zero wholesale (leases 0, controls FREE@0; also
        // pre-faults every page and matches the zeroed-buffer guarantee),
        // then set every cursor to the detached sentinel. Still pre-publish:
        // a racing validator sees the odd generation and discards.
        std::ptr::write_bytes(region.at(SPMC_TABLE_OFFSET), 0, len - SPMC_TABLE_OFFSET);
        for slot in 0..max_consumers {
            region
                .slot_cursor(slot)
                .store(SLOT_DETACHED, Ordering::Relaxed);
        }
        // Publish the magic last with Release, then close the seqlock.
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(MAGIC, Ordering::Release);
        generation.store((g | 1).wrapping_add(1), Ordering::Release);
        pt
    };
    Ok((Arc::new(region), producer_token))
}

/// A validated SPMC region plus the header facts every constructor needs.
struct SpmcOpened {
    region: Arc<ShmRegion>,
    capacity: usize,
    max_consumers: usize,
    generation: u64,
}

/// Map and validate an existing SPMC region (the SPMC face of
/// `open_region`). There is no single-pair occupancy check here: the table
/// holds N consumer cursors that are protocol-maintained lower bounds, and
/// judging them against a live producer is inherently racy — per the trust
/// model, validation catches accidents (wrong fd/kind/type/architecture),
/// not adversaries. The write cursor's record alignment is still checked.
fn open_spmc_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    unit_size: usize,
    min_capacity: usize,
    cursor_align: usize,
) -> io::Result<SpmcOpened> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let file_len = fd_len(fd)?;
    if file_len < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn capacity and max_consumers.
    let header = ShmRegion::map(fd, BUFFER_OFFSET)?;
    // Seqlock read (see `open_region`).
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if generation & 1 == 1 {
        return Err(err("ring is being initialized by another process"));
    }
    if header.read_magic() != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch"));
    }
    if unit_size == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if header.read_u64(OFF_UNIT_SIZE) != unit_size as u64 {
        return Err(err("element size mismatch"));
    }
    if header.read_u32(OFF_ARCH_BITS) != usize::BITS {
        return Err(err("architecture (usize width) mismatch"));
    }
    let capacity = header.read_u64(OFF_CAPACITY) as usize;
    if capacity == 0 || !capacity.is_power_of_two() || capacity < min_capacity {
        return Err(err("corrupt capacity"));
    }
    let max_consumers = header.read_u32(OFF_MAX_CONSUMERS) as usize;
    if max_consumers == 0 {
        return Err(err("corrupt max_consumers"));
    }
    drop(header);

    let len = spmc_region_len(capacity, unit_size, max_consumers)
        .map_err(|_| err("corrupt geometry: region length overflows"))?;
    if file_len < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = ShmRegion::map(fd, len)?;

    // Alignment holds for every individually-published cursor value.
    if region.spmc_write_cursor().load(Ordering::Acquire) % cursor_align as u64 != 0 {
        return Err(err("corrupt cursors: not record-aligned"));
    }
    // Seqlock re-check on the full mapping.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    if unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire) != generation {
        return Err(err("ring was re-initialized during validation"));
    }
    Ok(SpmcOpened {
        region: Arc::new(region),
        capacity,
        max_consumers,
        generation,
    })
}

// The consumer-table claim/release/retire/reset choreography and the shm
// producer/consumer backing types are shared with the anchored kinds — one
// copy of each protocol, parameterized by the kind's table offset (see
// `SlotClaim`, `claim_table_slot`, `release_table_claim`,
// `force_detach_table_slot`, `reset_gate_table`, `GateShmProducer`, and
// `GateShmConsumer` below). The SPMC kinds pass `SPMC_TABLE_OFFSET`.

/// Claim (or force-take) the producer role over a validated SPMC region and
/// reset the closed word — a (re)attached producer re-opens the ring (only
/// the producer ever writes that word, and we now hold its lease).
fn spmc_attach_producer_anchor<C: Default>(
    opened: &SpmcOpened,
    force: bool,
) -> io::Result<Box<GateShmProducer<C>>> {
    let region = Arc::clone(&opened.region);
    let token = if force {
        force_claim_lease(&region, Role::Producer)
    } else {
        claim_lease(&region, Role::Producer, opened.generation)?
    };
    region.spmc_closed().store(0, Ordering::Release);
    Ok(Box::new(GateShmProducer::new(
        region,
        token,
        opened.max_consumers,
        SPMC_TABLE_OFFSET,
    )))
}

/// Claim a consumer-table slot over a validated SPMC region: refuse closed
/// rings (mirroring the heap `SubscribeError::Closed`), map a full table to
/// `AddrInUse` (the role-conflict error), and re-check the seqlock
/// generation after the claim exactly as `claim_lease` does. Returns the
/// anchor plus the join point (returned directly from the claim — the slot
/// word holds only its sentinel-guarded image).
fn spmc_attach_consumer_anchor<P: Default, C: Default>(
    opened: &SpmcOpened,
) -> io::Result<(Box<GateShmConsumer<P, C>>, u64)> {
    let region = Arc::clone(&opened.region);
    if region.spmc_closed().load(Ordering::Acquire) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "ring closed: producer dropped (attach a new producer to reopen)",
        ));
    }
    let claim =
        claim_table_slot(&region, SPMC_TABLE_OFFSET, opened.max_consumers).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                "consumer table is full (max_consumers is fixed at creation; \
                 retired slots free only via recover_shm)",
            )
        })?;
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let gen_now = unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if gen_now != opened.generation {
        // Conditional rollback: `release_table_claim` re-checks the
        // generation and leaves a re-initialized table strictly alone.
        release_table_claim(&region, SPMC_TABLE_OFFSET, &claim, opened.generation);
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "ring was re-initialized during attach",
        ));
    }
    let joined = claim.joined;
    Ok((
        Box::new(GateShmConsumer::new(
            region,
            claim,
            opened.max_consumers,
            SPMC_TABLE_OFFSET,
        )),
        joined,
    ))
}

/// Both halves of a shm-backed gating SPMC element ring.
pub type SpmcElemPair<T, P, C> = (
    crate::spmc::Producer<T, P, C>,
    crate::spmc::Consumer<T, P, C>,
);
/// Both halves of a shm-backed gating SPMC byte ring.
pub type SpmcBytesPair<P, C> = (
    crate::spmc_bytes::BytesProducer<P, C>,
    crate::spmc_bytes::BytesConsumer<P, C>,
);

impl<T: ShmItem + Send + Sync> crate::spmc::RingBuffer<T> {
    /// Initialize `fd` as a fresh shm-backed gating SPMC element ring with a
    /// `max_consumers`-slot consumer table and return the producer plus one
    /// initial consumer (attach or [`subscribe`](crate::spmc::Producer::subscribe)
    /// more, up to the table size), with default
    /// ([`YieldWait`](crate::wait::YieldWait)) wait strategies.
    ///
    /// Unlike heap membership (unbounded), `max_consumers` is fixed at
    /// creation — a mapped layout cannot grow. That constraint is physical,
    /// not a design choice.
    ///
    /// ```
    /// use std::os::fd::AsFd;
    /// use rust_rb::{memfd, spmc};
    /// # fn main() -> std::io::Result<()> {
    /// let fd = memfd("spmc-doc")?;
    /// // SAFETY: fresh private memfd, only cooperating handles touch it.
    /// let (mut tx, mut rx) =
    ///     unsafe { spmc::RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 8)? };
    /// tx.push(7);
    /// assert_eq!(rx.pop(), Ok(7));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self): the region must only ever be
    /// accessed by cooperating rust-rb handles.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::type_complexity)]
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_consumers: usize,
    ) -> io::Result<(crate::spmc::Producer<T>, crate::spmc::Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe {
            crate::spmc::RingBuffer::<T, _, _>::create_shm_with(fd, min_capacity, max_consumers)
        }
    }

    /// Unconditionally take over an existing SPMC ring: force-take the
    /// producer role, **reset the whole consumer table** (leases zeroed,
    /// every slot FREE at a bumped epoch — retired slots become issuable
    /// again), and return a fresh pair. The returned consumer resumes at the
    /// slowest previously-registered cursor, so recovery is at-least-once:
    /// everything published but not consumed by every dead consumer is
    /// delivered again.
    ///
    /// # Safety
    ///
    /// Trust model, plus: the takeover is unconditional — the caller asserts
    /// **every** previous holder (producer and all consumers) is gone. A
    /// still-live consumer would be silently unregistered (its flushes are
    /// suppressed by the slot-lease guard, but its reads lose all gating
    /// protection); a still-live producer would corrupt the ring.
    #[allow(clippy::type_complexity)]
    pub unsafe fn recover_shm(
        fd: BorrowedFd<'_>,
    ) -> io::Result<(crate::spmc::Producer<T>, crate::spmc::Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { crate::spmc::RingBuffer::<T, _, _>::recover_shm_with(fd) }
    }
}

impl<T, P, C> crate::spmc::RingBuffer<T, P, C>
where
    T: ShmItem + Send + Sync,
    P: CrossProcess + SelfTimed + Send + Sync,
    C: CrossProcess + SelfTimed + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<SpmcOpened> {
        // Same attach-side type re-validation as the SPSC element ring.
        check_elem_type::<T>()?;
        // Capacity floor 2 = the heap SPMC constructor's floor (an
        // audience-less producer's gating default needs it); element cursors
        // are always decodable, so no alignment constraint.
        open_spmc_region(fd, KIND_SPMC_ELEMS, std::mem::size_of::<T>(), 2, 1)
    }

    /// [`create_shm`](crate::spmc::RingBuffer::create_shm) with explicit
    /// [`CrossProcess`] + [`SelfTimed`] wait strategies (both bounds on both
    /// sides: the strategy must survive a process boundary *and* never need
    /// a peer notify — the spin family).
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_consumers: usize,
    ) -> io::Result<SpmcElemPair<T, P, C>> {
        check_elem_type::<T>()?;
        let capacity = crate::cursor::round_capacity(min_capacity, 2);
        let (region, producer_token) = create_spmc_region(
            fd,
            KIND_SPMC_ELEMS,
            capacity,
            std::mem::size_of::<T>(),
            max_consumers,
        )?;
        let claim = claim_table_slot(&region, SPMC_TABLE_OFFSET, max_consumers)
            .expect("fresh table has free slots");
        let joined = claim.joined;
        let producer_anchor = Box::new(GateShmProducer::new(
            Arc::clone(&region),
            producer_token,
            max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        let consumer_anchor = Box::new(GateShmConsumer::new(
            region,
            claim,
            max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        // SAFETY: freshly initialized region matches this ring's layout.
        unsafe {
            Ok((
                crate::spmc::Producer::from_shm(producer_anchor, capacity),
                crate::spmc::Consumer::from_shm(consumer_anchor, capacity, joined),
            ))
        }
    }

    /// Attach to an existing SPMC element ring as the producer. Fails with
    /// `AddrInUse` while the producer lease is held; resets the graceful
    /// `closed` flag (the ring is open again). The gating caches are rebuilt
    /// from the live consumer table on the first push — never from defaults.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size is validated).
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc::Producer<T, P, C>> {
        let opened = Self::open(fd)?;
        let anchor = spmc_attach_producer_anchor::<C>(&opened, false)?;
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::spmc::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Unconditionally take over the producer role (single-side crash
    /// recovery while consumers keep running; see
    /// [`BytesRingBuffer::force_attach_shm_producer`] for the lease story).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone, plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc::Producer<T, P, C>> {
        let opened = Self::open(fd)?;
        let anchor = spmc_attach_producer_anchor::<C>(&opened, true)?;
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::spmc::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Attach a **new consumer**: claims a FREE consumer-table slot (the
    /// shm face of `subscribe`; the join point is the producer's published
    /// cursor at claim time). Fails with `AddrInUse` when the table is full
    /// and `BrokenPipe` when the ring is closed.
    ///
    /// # Safety
    ///
    /// Trust model, plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_consumer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc::Consumer<T, P, C>> {
        let opened = Self::open(fd)?;
        let (anchor, joined) = spmc_attach_consumer_anchor::<P, C>(&opened)?;
        // SAFETY: region validated by open(); the claim choreography just
        // ran (its final store published `joined` into the slot).
        Ok(unsafe { crate::spmc::Consumer::from_shm(anchor, opened.capacity, joined) })
    }

    /// Retire consumer-table slot `slot` [A-4.1]: bump its epoch and mark it
    /// RETIRED, **iff it is still `ACTIVE` at `epoch`**. `(slot, epoch)` is
    /// what the victim's [`Consumer::shm_slot_epoch`](crate::spmc::Consumer::shm_slot_epoch)
    /// reported: the epoch is how the caller proves it is retiring the same
    /// occupancy it observed dead — every claim bumps the slot's epoch, so
    /// if the dead consumer's slot was gracefully freed and re-claimed by a
    /// healthy consumer in the meantime, the epochs differ and this fails
    /// with `InvalidInput` instead of retiring the living. The producer's
    /// next rescan stops honoring a retired slot's cursor (un-gating a
    /// producer blocked on a dead consumer); the slot is **never re-issued**
    /// until [`recover_shm`](crate::spmc::RingBuffer::recover_shm) resets
    /// the table.
    ///
    /// # Safety
    ///
    /// The caller asserts the holder of `(slot, epoch)` is **dead**. This is
    /// the same trust register as `force_attach`: if the holder is actually
    /// alive, the ring itself stays consistent (the zombie's flushes land on
    /// the retired slot, which nothing reads), but the zombie's **reads lose
    /// all gating protection** — the producer may overwrite data it still
    /// borrows. `force_detach_consumer` revokes the victim's read validity.
    pub unsafe fn force_detach_consumer(
        fd: BorrowedFd<'_>,
        slot: usize,
        epoch: u32,
    ) -> io::Result<()> {
        let opened = Self::open(fd)?;
        if slot >= opened.max_consumers {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slot index out of range for this ring's consumer table",
            ));
        }
        force_detach_table_slot(&opened.region, SPMC_TABLE_OFFSET, slot, epoch)
    }

    /// [`recover_shm`](crate::spmc::RingBuffer::recover_shm) with explicit
    /// wait strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](crate::spmc::RingBuffer::recover_shm), plus the
    /// `T` caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<SpmcElemPair<T, P, C>> {
        let opened = Self::open(fd)?;
        let producer_anchor = spmc_attach_producer_anchor::<C>(&opened, true)?;
        let resume = reset_gate_table(
            &opened.region,
            SPMC_TABLE_OFFSET,
            opened.max_consumers,
            opened.capacity as u64,
        );
        let claim = claim_table_slot(&opened.region, SPMC_TABLE_OFFSET, opened.max_consumers)
            .expect("freshly reset table has free slots");
        // Move the fresh slot back to the resume point (a lower cursor only
        // gates the producer more — and the producer is us, not yet pushing).
        opened
            .region
            .slot_cursor(claim.slot)
            .store(slot_guard(resume), Ordering::Release);
        let consumer_anchor = Box::new(GateShmConsumer::new(
            Arc::clone(&opened.region),
            claim,
            opened.max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        // SAFETY: region validated by open(); forwarded caller contract.
        unsafe {
            Ok((
                crate::spmc::Producer::from_shm(producer_anchor, opened.capacity),
                crate::spmc::Consumer::from_shm(consumer_anchor, opened.capacity, resume),
            ))
        }
    }
}

impl crate::spmc_bytes::BytesRingBuffer {
    /// Initialize `fd` as a fresh shm-backed gating SPMC **byte** ring (see
    /// [`spmc::RingBuffer::create_shm`](crate::spmc::RingBuffer::create_shm);
    /// capacity is in bytes, minimum 8).
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::type_complexity)]
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_consumers: usize,
    ) -> io::Result<(
        crate::spmc_bytes::BytesProducer,
        crate::spmc_bytes::BytesConsumer,
    )> {
        // SAFETY: forwarded caller contract.
        unsafe {
            crate::spmc_bytes::BytesRingBuffer::<_, _>::create_shm_with(
                fd,
                min_capacity,
                max_consumers,
            )
        }
    }

    /// Unconditionally take over an existing SPMC byte ring (see
    /// [`spmc::RingBuffer::recover_shm`](crate::spmc::RingBuffer::recover_shm):
    /// full table reset; at-least-once resume from the slowest
    /// previously-registered cursor, which is always a record boundary).
    ///
    /// # Safety
    ///
    /// See [`spmc::RingBuffer::recover_shm`](crate::spmc::RingBuffer::recover_shm).
    #[allow(clippy::type_complexity)]
    pub unsafe fn recover_shm(
        fd: BorrowedFd<'_>,
    ) -> io::Result<(
        crate::spmc_bytes::BytesProducer,
        crate::spmc_bytes::BytesConsumer,
    )> {
        // SAFETY: forwarded caller contract.
        unsafe { crate::spmc_bytes::BytesRingBuffer::<_, _>::recover_shm_with(fd) }
    }
}

impl<P, C> crate::spmc_bytes::BytesRingBuffer<P, C>
where
    P: CrossProcess + SelfTimed + Send + Sync,
    C: CrossProcess + SelfTimed + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<SpmcOpened> {
        open_spmc_region(
            fd,
            KIND_SPMC_BYTES,
            1,
            BYTES_MIN_CAPACITY,
            BYTES_CURSOR_ALIGN,
        )
    }

    /// [`create_shm`](crate::spmc_bytes::BytesRingBuffer::create_shm) with
    /// explicit [`CrossProcess`] + [`SelfTimed`] wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_consumers: usize,
    ) -> io::Result<SpmcBytesPair<P, C>> {
        let capacity = crate::cursor::round_capacity(min_capacity, BYTES_MIN_CAPACITY);
        let (region, producer_token) =
            create_spmc_region(fd, KIND_SPMC_BYTES, capacity, 1, max_consumers)?;
        let claim = claim_table_slot(&region, SPMC_TABLE_OFFSET, max_consumers)
            .expect("fresh table has free slots");
        let joined = claim.joined;
        let producer_anchor = Box::new(GateShmProducer::new(
            Arc::clone(&region),
            producer_token,
            max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        let consumer_anchor = Box::new(GateShmConsumer::new(
            region,
            claim,
            max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        // SAFETY: freshly initialized region matches this ring's layout.
        unsafe {
            Ok((
                crate::spmc_bytes::BytesProducer::from_shm(producer_anchor, capacity),
                crate::spmc_bytes::BytesConsumer::from_shm(consumer_anchor, capacity, joined),
            ))
        }
    }

    /// Attach to an existing SPMC byte ring as the producer (see
    /// [`spmc::RingBuffer::attach_shm_producer`](crate::spmc::RingBuffer::attach_shm_producer)).
    /// Also resets the starving flag a departed predecessor may have left
    /// set, exactly as the SPSC engine's producer attach does.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer.
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc_bytes::BytesProducer<P, C>> {
        let opened = Self::open(fd)?;
        let anchor = spmc_attach_producer_anchor::<C>(&opened, false)?;
        // A newly-attached producer is not starving (only the producer
        // writes this flag, and we now hold its lease).
        opened.region.spmc_aux().store(0, Ordering::Release);
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::spmc_bytes::BytesProducer::from_shm(anchor, opened.capacity) })
    }

    /// Unconditionally take over the producer role (see
    /// [`force_attach_shm_producer`](crate::spmc::RingBuffer::force_attach_shm_producer)
    /// on the element ring).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone.
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc_bytes::BytesProducer<P, C>> {
        let opened = Self::open(fd)?;
        let anchor = spmc_attach_producer_anchor::<C>(&opened, true)?;
        opened.region.spmc_aux().store(0, Ordering::Release);
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::spmc_bytes::BytesProducer::from_shm(anchor, opened.capacity) })
    }

    /// Attach a new consumer (see
    /// [`spmc::RingBuffer::attach_shm_consumer`](crate::spmc::RingBuffer::attach_shm_consumer);
    /// the join point is always a record boundary).
    ///
    /// # Safety
    ///
    /// Trust model.
    pub unsafe fn attach_shm_consumer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::spmc_bytes::BytesConsumer<P, C>> {
        let opened = Self::open(fd)?;
        let (anchor, joined) = spmc_attach_consumer_anchor::<P, C>(&opened)?;
        // SAFETY: region validated by open(); the claim choreography just
        // ran (its final store published `joined` into the slot).
        Ok(unsafe { crate::spmc_bytes::BytesConsumer::from_shm(anchor, opened.capacity, joined) })
    }

    /// Retire consumer-table slot `slot` iff still `ACTIVE` at `epoch` (see
    /// [`spmc::RingBuffer::force_detach_consumer`](crate::spmc::RingBuffer::force_detach_consumer);
    /// `(slot, epoch)` comes from the victim's
    /// [`BytesConsumer::shm_slot_epoch`](crate::spmc_bytes::BytesConsumer::shm_slot_epoch)).
    ///
    /// # Safety
    ///
    /// The caller asserts the holder of `(slot, epoch)` is dead; a live
    /// holder's reads lose all gating protection (revoked read validity).
    pub unsafe fn force_detach_consumer(
        fd: BorrowedFd<'_>,
        slot: usize,
        epoch: u32,
    ) -> io::Result<()> {
        let opened = Self::open(fd)?;
        if slot >= opened.max_consumers {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slot index out of range for this ring's consumer table",
            ));
        }
        force_detach_table_slot(&opened.region, SPMC_TABLE_OFFSET, slot, epoch)
    }

    /// [`recover_shm`](crate::spmc_bytes::BytesRingBuffer::recover_shm) with
    /// explicit wait strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](crate::spmc_bytes::BytesRingBuffer::recover_shm).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<SpmcBytesPair<P, C>> {
        let opened = Self::open(fd)?;
        let producer_anchor = spmc_attach_producer_anchor::<C>(&opened, true)?;
        opened.region.spmc_aux().store(0, Ordering::Release);
        let resume = reset_gate_table(
            &opened.region,
            SPMC_TABLE_OFFSET,
            opened.max_consumers,
            opened.capacity as u64,
        );
        let claim = claim_table_slot(&opened.region, SPMC_TABLE_OFFSET, opened.max_consumers)
            .expect("freshly reset table has free slots");
        opened
            .region
            .slot_cursor(claim.slot)
            .store(slot_guard(resume), Ordering::Release);
        let consumer_anchor = Box::new(GateShmConsumer::new(
            Arc::clone(&opened.region),
            claim,
            opened.max_consumers,
            SPMC_TABLE_OFFSET,
        ));
        // SAFETY: region validated by open(); forwarded caller contract.
        unsafe {
            Ok((
                crate::spmc_bytes::BytesProducer::from_shm(producer_anchor, opened.capacity),
                crate::spmc_bytes::BytesConsumer::from_shm(
                    consumer_anchor,
                    opened.capacity,
                    resume,
                ),
            ))
        }
    }
}

// =============================================================================
// LOSSY broadcast rings (kinds 5/6): producer lease only, read-only
// lease-free consumers.
// =============================================================================
//
// The producer role reuses the SPSC lease at `OFF_PRODUCER_LEASE` verbatim.
// There is no consumer-side shared state whatsoever [§3.4/§3.7]: consumers
// attach over a PROT_READ mapping, take no lease, write nothing, and are
// unbounded in count — their drop is just an munmap. That is also why
// `recover_shm` degenerates to force-attaching the producer: there is
// nothing else to reset.

/// Region length for a broadcast ring: header (+ counter slots) + buffer.
/// `stride` is the bytes one capacity unit occupies in the file — the slot
/// stride for the element kind (seq word + payload, see the module docs'
/// layout math), 1 for the byte kind.
fn bcast_region_len(capacity: usize, stride: usize, buffer_offset: usize) -> io::Result<usize> {
    let len = capacity
        .checked_mul(stride)
        .and_then(|b| b.checked_add(buffer_offset))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region"))?;
    // Same off_t clamp as `region_len` (32-bit sign-flip guard).
    if len as u64 > libc::off_t::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "region length exceeds the platform file-offset limit",
        ));
    }
    Ok(len)
}

/// Initialize a fresh broadcast region: size the fd, map it, write the
/// header (zeroed counters, the element kind's slack) and a zeroed buffer,
/// take the producer lease — the only lease a broadcast ring has. The
/// seqlock write protocol is identical to `create_region`'s.
fn create_bcast_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    capacity: usize,
    unit_size: usize,
    stride: usize,
    buffer_offset: usize,
    slack: u64,
) -> io::Result<(Arc<ShmRegion>, u64)> {
    let len = bcast_region_len(capacity, stride, buffer_offset)?;
    // SAFETY: valid fd for the borrow; `bcast_region_len` confirmed `len`
    // fits off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // SAFETY: all header offsets are naturally aligned and inside the
    // mapping (see `create_region` for the seqlock/atomics rationale).
    let producer_token = unsafe {
        // Seqlock open: odd generation before touching anything.
        let generation = region.atomic::<AtomicU64>(OFF_GENERATION);
        let g = generation.load(Ordering::Relaxed);
        generation.store(g | 1, Ordering::SeqCst);
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        region
            .atomic::<AtomicU32>(OFF_VERSION)
            .store(VERSION, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_KIND)
            .store(kind, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CAPACITY)
            .store(capacity as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_UNIT_SIZE)
            .store(unit_size as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_ARCH_BITS)
            .store(usize::BITS, Ordering::Relaxed);
        // No consumer table: membership is unbounded (pure readers).
        region
            .atomic::<AtomicU32>(OFF_MAX_CONSUMERS)
            .store(0, Ordering::Relaxed);
        let pt = lease_token();
        region
            .atomic::<AtomicU64>(OFF_PRODUCER_LEASE)
            .store(pt, Ordering::Relaxed);
        // The SPSC consumer lease is unused by broadcast kinds (consumers
        // are lease-free); keep it deterministically zero.
        region
            .atomic::<AtomicU64>(OFF_CONSUMER_LEASE)
            .store(0, Ordering::Relaxed);
        // Counters + buffer: zero wholesale from the cursor area down
        // (tail, closed, slack, intent, latest, buffer — also pre-faults
        // every page and matches the heap rings' zeroed-buffer guarantee:
        // the byte ring's lapped readers legitimately load bytes the
        // producer never wrote, and an element slot's seq 0 is below every
        // accepted generation). Still pre-publish: a racing validator sees
        // the odd generation and discards.
        std::ptr::write_bytes(region.at(OFF_WRITE_CURSOR), 0, len - OFF_WRITE_CURSOR);
        region
            .atomic::<AtomicU64>(OFF_BCAST_SLACK)
            .store(slack, Ordering::Relaxed);
        // The exact per-unit stride: open validates it for equality —
        // `unit_size` cannot distinguish same-size/different-alignment
        // element types, whose strides (and thus slot offsets) differ.
        region
            .atomic::<AtomicU64>(OFF_BCAST_STRIDE)
            .store(stride as u64, Ordering::Relaxed);
        // Publish the magic last with Release, then close the seqlock.
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(MAGIC, Ordering::Release);
        generation.store((g | 1).wrapping_add(1), Ordering::Release);
        pt
    };
    Ok((Arc::new(region), producer_token))
}

/// A validated broadcast region plus the header facts every constructor
/// needs.
struct BcastOpened {
    region: Arc<ShmRegion>,
    capacity: usize,
    generation: u64,
    slack: u64,
}

/// Map and validate an existing broadcast region (the broadcast face of
/// `open_region`). `read_only` selects the consumer's `PROT_READ` mapping —
/// this path performs **no writes at all** (no lease claim, no cursor
/// touch), so it works over either protection. There is no occupancy check:
/// a lossy ring has no consumer cursors to judge, and the tail is a bare
/// monotonic count (only its record alignment is checked, on the byte
/// kind).
// One geometry knob per header invariant; a param struct would only re-name
// the call sites (there are exactly two, one per kind).
#[allow(clippy::too_many_arguments)]
fn open_bcast_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    unit_size: usize,
    stride: usize,
    buffer_offset: usize,
    min_capacity: usize,
    tail_align: u64,
    read_only: bool,
) -> io::Result<BcastOpened> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let file_len = fd_len(fd)?;
    if file_len < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn the capacity (read-only: this probe
    // never writes, whichever role is attaching).
    let header = ShmRegion::map_read_only(fd, BUFFER_OFFSET)?;
    // Seqlock read (see `open_region`).
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if generation & 1 == 1 {
        return Err(err("ring is being initialized by another process"));
    }
    if header.read_magic() != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch"));
    }
    if unit_size == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if header.read_u64(OFF_UNIT_SIZE) != unit_size as u64 {
        return Err(err("element size mismatch"));
    }
    if header.read_u32(OFF_ARCH_BITS) != usize::BITS {
        return Err(err("architecture (usize width) mismatch"));
    }
    let capacity = header.read_u64(OFF_CAPACITY) as usize;
    if capacity == 0 || !capacity.is_power_of_two() || capacity < min_capacity {
        return Err(err("corrupt capacity"));
    }
    // The slack is meaningful on the element kind (the byte kind writes 0);
    // `slack < capacity` is the constructor invariant either way.
    let slack = header.read_u64(OFF_BCAST_SLACK);
    if slack >= capacity as u64 {
        return Err(err("corrupt slack: not below the capacity"));
    }
    // Exact-stride check: the length check below only rejects *smaller*
    // regions, so a `T` with the created type's size but a lower alignment
    // (smaller stride) would otherwise be accepted and misindex every slot.
    if header.read_u64(OFF_BCAST_STRIDE) != stride as u64 {
        return Err(err("element slot stride mismatch"));
    }
    drop(header);

    let len = bcast_region_len(capacity, stride, buffer_offset)
        .map_err(|_| err("corrupt geometry: region length overflows"))?;
    if file_len < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = if read_only {
        ShmRegion::map_read_only(fd, len)?
    } else {
        ShmRegion::map(fd, len)?
    };

    // Alignment holds for every individually-published tail value (byte
    // kind: committed tails are record boundaries; element kind: any count
    // decodes, align 1).
    if region.bcast_tail().load(Ordering::Acquire) % tail_align != 0 {
        return Err(err("corrupt tail: not record-aligned"));
    }
    // Seqlock re-check on the full mapping.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    if unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire) != generation {
        return Err(err("ring was re-initialized during validation"));
    }
    Ok(BcastOpened {
        region: Arc::new(region),
        capacity,
        generation,
        slack,
    })
}

/// The shm anchor for a lossy broadcast **producer**: keeps the read-write
/// mapping alive and holds the producer role lease (same offset and
/// discipline as every other ring's producer). It carries no consumer-side
/// state at all — lossy consumers are lease-free pure readers, and every
/// wait strategy lives in a ([`SelfTimed`]) consumer handle.
pub(crate) struct BcastProducerAnchor {
    region: Arc<ShmRegion>,
    token: u64,
    /// Fork guard, exactly as in [`ShmAnchor`].
    owner_pid: libc::pid_t,
}

impl BcastProducerAnchor {
    fn new(region: Arc<ShmRegion>, token: u64) -> Self {
        Self {
            region,
            token,
            // SAFETY: getpid is always safe.
            owner_pid: unsafe { libc::getpid() },
        }
    }

    pub(crate) fn region(&self) -> &Arc<ShmRegion> {
        &self.region
    }

    /// Whether the producer lease still holds this handle's token (see
    /// [`ShmAnchor::owns_lease`]).
    pub(crate) fn owns_lease(&self) -> bool {
        // SAFETY: the lease offset is 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(OFF_PRODUCER_LEASE) };
        lease.load(Ordering::Acquire) == self.token
    }

    /// Fork guard (see [`ShmAnchor::owned_by_current_process`]).
    pub(crate) fn owned_by_current_process(&self) -> bool {
        // SAFETY: getpid is always safe.
        (unsafe { libc::getpid() }) == self.owner_pid
    }
}

impl Drop for BcastProducerAnchor {
    fn drop(&mut self) {
        // Guarded lease release, exactly as [`ShmAnchor`]'s: pid guard
        // against fork copies, token CAS against force-takeovers.
        if !self.owned_by_current_process() {
            return;
        }
        // SAFETY: the lease offset is 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(OFF_PRODUCER_LEASE) };
        let _ = lease.compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Claim (or force-take) the producer role over a validated broadcast
/// region and reset the closed word — a (re)attached producer re-opens the
/// ring (shm `Closed` is end-of-session, not terminal; only the producer
/// ever writes that word, and we now hold its lease).
fn bcast_attach_producer_anchor(
    opened: &BcastOpened,
    force: bool,
) -> io::Result<Box<BcastProducerAnchor>> {
    let region = Arc::clone(&opened.region);
    let token = if force {
        force_claim_lease(&region, Role::Producer)
    } else {
        claim_lease(&region, Role::Producer, opened.generation)?
    };
    region.bcast_closed().store(0, Ordering::Release);
    Ok(Box::new(BcastProducerAnchor::new(region, token)))
}

/// Element-type invariants for the broadcast element ring, enforced on
/// create AND attach (as errors — fallible paths). The slot carries the seq
/// word in front of the payload, so the alignment bound is the *slot's*.
fn check_bcast_elem_type<T>() -> io::Result<()> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    if std::mem::size_of::<T>() == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if crate::broadcast::shm_slot_align::<T>() > 128 {
        return Err(err("element alignment exceeds the buffer offset alignment"));
    }
    Ok(())
}

/// Open a broadcast element region for `T` (shared by every constructor;
/// `read_only` is the consumer path).
fn open_bcast_elems<T: ShmItem + crate::broadcast::NoUninit>(
    fd: BorrowedFd<'_>,
    read_only: bool,
) -> io::Result<BcastOpened> {
    check_bcast_elem_type::<T>()?;
    open_bcast_region(
        fd,
        KIND_BCAST_ELEMS,
        std::mem::size_of::<T>(),
        crate::broadcast::shm_slot_stride::<T>(),
        BUFFER_OFFSET,
        1,
        1,
        read_only,
    )
}

impl<T> crate::broadcast::RingBuffer<T>
where
    // Both element bounds, because the two traits assert *different*
    // directions of bit-validity: `ShmItem` says every bit pattern a
    // cooperating peer writes is a valid `T` (cross-process reads of
    // untrusted-by-construction memory), while `NoUninit` says every byte of
    // a valid `T` is initialized data (the racing word-wise atomic copies
    // read and write every byte of the representation). Neither implies the
    // other — see their docs.
    T: ShmItem + crate::broadcast::NoUninit + Send,
{
    /// Initialize `fd` as a fresh shm-backed lossy broadcast element ring
    /// and return **the producer** (there is no initial consumer: broadcast
    /// consumers are unbounded lease-free pure readers — attach any number
    /// with [`attach_shm_consumer`](Self::attach_shm_consumer), each over
    /// its own **read-only** mapping).
    ///
    /// The real capacity is `min_capacity` rounded up to the next power of
    /// two; the reposition slack defaults to `capacity / 8`, clamped exactly
    /// as the heap constructor's (see
    /// [`create_shm_with_slack`](Self::create_shm_with_slack) for the knob).
    ///
    /// ```
    /// use std::os::fd::AsFd;
    /// use rust_rb::{broadcast, memfd};
    /// # fn main() -> std::io::Result<()> {
    /// let fd = memfd("bcast-doc")?;
    /// // SAFETY: fresh private memfd, only cooperating handles touch it.
    /// let mut tx = unsafe { broadcast::RingBuffer::<u64>::create_shm(fd.as_fd(), 64)? };
    /// // Consumers attach lease-free, over a read-only mapping.
    /// // SAFETY: cooperating handles only.
    /// let mut rx = unsafe { broadcast::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd())? };
    /// tx.push(7);
    /// assert_eq!(rx.pop(), Ok(7));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self): the region must only ever be
    /// accessed by cooperating rust-rb handles.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<crate::broadcast::Producer<T>> {
        let capacity = crate::cursor::round_capacity(min_capacity, 1);
        let slack = crate::broadcast::shm_default_slack(capacity as u64);
        // SAFETY: forwarded caller contract.
        unsafe { Self::create_shm_with_slack(fd, min_capacity, slack as usize) }
    }

    /// [`create_shm`](Self::create_shm) with an explicit reposition `slack`
    /// [A-3.2] — the create-time knob every consumer of this ring inherits
    /// (it is stored in the region header and validated on attach). See
    /// [`crate::broadcast::RingBuffer::with_slack`] for the semantics.
    ///
    /// # Safety
    ///
    /// See [`create_shm`](Self::create_shm).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`. Unlike the heap constructor,
    /// `slack >= capacity` is an **error** (`InvalidInput`), not a panic —
    /// shm constructors surface misuse on their fallible path.
    pub unsafe fn create_shm_with_slack(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        slack: usize,
    ) -> io::Result<crate::broadcast::Producer<T>> {
        check_bcast_elem_type::<T>()?;
        let capacity = crate::cursor::round_capacity(min_capacity, 1);
        if slack >= capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slack must be less than the capacity",
            ));
        }
        let (region, producer_token) = create_bcast_region(
            fd,
            KIND_BCAST_ELEMS,
            capacity,
            std::mem::size_of::<T>(),
            crate::broadcast::shm_slot_stride::<T>(),
            BUFFER_OFFSET,
            slack as u64,
        )?;
        let anchor = Box::new(BcastProducerAnchor::new(region, producer_token));
        // SAFETY: freshly initialized region matches this ring's layout.
        Ok(unsafe { crate::broadcast::Producer::from_shm(anchor, capacity) })
    }

    /// Attach to an existing broadcast element ring as the producer. Fails
    /// with `AddrInUse` while the producer lease is held; resets the
    /// graceful `closed` flag (the ring is open again — shm `Closed` is
    /// end-of-session, and live consumers simply see the new session).
    /// Publishing resumes exactly after the last published message.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size is validated).
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast::Producer<T>> {
        let opened = open_bcast_elems::<T>(fd, false)?;
        let anchor = bcast_attach_producer_anchor(&opened, false)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe { crate::broadcast::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Unconditionally take over the producer role — crash recovery while
    /// consumers keep running (they need no help: everything published
    /// stays drainable, and a reader racing the recovered producer's
    /// re-publishes self-heals via the slot seqlock generations).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone (a live one would corrupt the ring), plus
    /// the `T` caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast::Producer<T>> {
        let opened = open_bcast_elems::<T>(fd, false)?;
        let anchor = bcast_attach_producer_anchor(&opened, true)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe { crate::broadcast::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Unconditionally take over an existing broadcast ring — the same
    /// operation as
    /// [`force_attach_shm_producer`](Self::force_attach_shm_producer),
    /// under the crate-wide recovery name: the producer is a broadcast
    /// ring's **only** role, and consumers keep no shared state, so there
    /// is nothing else to reset. (Contrast `recover_shm` on the SPSC/SPMC
    /// rings, which must also reclaim consumer leases or tables.)
    ///
    /// # Safety
    ///
    /// See [`force_attach_shm_producer`](Self::force_attach_shm_producer).
    pub unsafe fn recover_shm(fd: BorrowedFd<'_>) -> io::Result<crate::broadcast::Producer<T>> {
        // SAFETY: forwarded caller contract.
        unsafe { Self::force_attach_shm_producer(fd) }
    }
}

impl<T, C> crate::broadcast::RingBuffer<T, C>
where
    T: ShmItem + crate::broadcast::NoUninit + Send,
    // `SelfTimed` is the ring's own consumer bound (nobody ever notifies a
    // lossy reader); `CrossProcess` is the shm bound — and every SelfTimed
    // tier is CrossProcess by construction, so the pair costs nothing.
    C: CrossProcess + SelfTimed + Send,
{
    /// Attach a **new consumer**: validates the header and maps the region
    /// **read-only** (`PROT_READ`). The consumer takes **no lease** and
    /// never writes a byte of shared state — membership is unbounded, and
    /// dropping the consumer just unmaps. Its join point is the tail at
    /// attach time.
    ///
    /// Attaching to a *closed* ring succeeds (mirroring the heap
    /// `subscribe`): the consumer is born drained and pops
    /// [`Closed`](crate::broadcast::PopError::Closed) — until a new
    /// producer attach reopens the session.
    ///
    /// The read-only mapping doubles as enforcement: a store accidentally
    /// introduced anywhere in the consumer path is a deterministic SIGSEGV
    /// rather than silent corruption.
    ///
    /// # Safety
    ///
    /// Trust model, plus the `T` caveat of
    /// [`attach_shm_producer`](crate::broadcast::RingBuffer::attach_shm_producer).
    pub unsafe fn attach_shm_consumer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast::Consumer<T, C>> {
        let opened = open_bcast_elems::<T>(fd, true)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe {
            crate::broadcast::Consumer::from_shm(opened.region, opened.capacity, opened.slack)
        })
    }
}

/// Byte-ring capacity floor and record alignment for the broadcast byte
/// ring, shared with the ring's own constructor and frame decoder so they
/// cannot drift.
const BCAST_BYTES_MIN_CAPACITY: usize = crate::broadcast_bytes::MIN_CAPACITY;
const BCAST_BYTES_TAIL_ALIGN: u64 = crate::broadcast_bytes::ALIGN as u64;

/// Open a broadcast byte region (shared by every constructor; `read_only`
/// is the consumer path).
fn open_bcast_bytes(fd: BorrowedFd<'_>, read_only: bool) -> io::Result<BcastOpened> {
    open_bcast_region(
        fd,
        KIND_BCAST_BYTES,
        1,
        1,
        BCAST_BYTES_BUFFER_OFFSET,
        BCAST_BYTES_MIN_CAPACITY,
        BCAST_BYTES_TAIL_ALIGN,
        read_only,
    )
}

/// The byte ring's producer-attach healing: compute the intent floor and
/// repair `latest` after a mid-push crash.
///
/// A producer that died between declaring `tail_intent` and committing
/// `tail` leaves `intent > tail`, with the bytes just under that declared
/// frontier destroyed. Two repairs, both writes only the lease holder may
/// make (cold, attach-time):
///
/// * **Intent floor** = `max(intent, tail)`: the new producer's pushes
///   declare `max(new_tail, floor)`, so the declared frontier never
///   regresses across sessions — the destroyed bytes stay strictly below
///   `intent - capacity`, permanently outside every consumer's validation
///   window. (Without the floor, a first record smaller than the dead one
///   would re-admit a sliver of destroyed bytes to the window check.)
/// * **`latest = tail`** when `intent != tail`: the dead push may have
///   stored `latest` pointing into its never-committed span; `tail` is the
///   one boundary guaranteed committed, and no consumer reads at or past it
///   until the new session publishes there. A consumer that repositioned to
///   the *old* `latest` before this repair lands is protected by the
///   tail-wait, not the window check (the dead intent is within a capacity
///   of the old latest, so the window check alone would pass): its
///   reposition refreshed `tail_cache = tail <= pos`, so it *waits*, and by
///   the time `tail` moves past its position the new session has committed
///   real bytes there.
fn bcast_bytes_heal_on_attach(opened: &BcastOpened) -> u64 {
    bytes_heal_on_attach(&opened.region)
}

/// The counter-level body of [`bcast_bytes_heal_on_attach`], shared with the
/// anchored byte kind (whose tail/intent/latest live at the same offsets).
fn bytes_heal_on_attach(region: &ShmRegion) -> u64 {
    let tail = region.bcast_tail().load(Ordering::Acquire);
    let intent = region.bcast_intent().load(Ordering::Acquire);
    if intent != tail {
        region.bcast_latest().store(tail, Ordering::Release);
    }
    intent.max(tail)
}

impl crate::broadcast_bytes::BytesRingBuffer {
    /// Initialize `fd` as a fresh shm-backed lossy broadcast **byte** ring
    /// and return **the producer** (capacity is in bytes, minimum 8; there
    /// is no initial consumer — attach any number with
    /// [`attach_shm_consumer`](Self::attach_shm_consumer), each lease-free
    /// over its own **read-only** mapping).
    ///
    /// ```
    /// use std::os::fd::AsFd;
    /// use rust_rb::{broadcast_bytes, memfd};
    /// # fn main() -> std::io::Result<()> {
    /// let fd = memfd("bcast-bytes-doc")?;
    /// // SAFETY: fresh private memfd, only cooperating handles touch it.
    /// let mut tx =
    ///     unsafe { broadcast_bytes::BytesRingBuffer::create_shm(fd.as_fd(), 4096)? };
    /// // SAFETY: cooperating handles only. (The annotation picks the
    /// // default `YieldWait` consumer strategy.)
    /// let mut rx: broadcast_bytes::BytesConsumer =
    ///     unsafe { broadcast_bytes::BytesRingBuffer::attach_shm_consumer(fd.as_fd())? };
    /// tx.push(b"tick");
    /// assert_eq!(rx.pop().unwrap(), b"tick");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<crate::broadcast_bytes::BytesProducer> {
        let capacity = crate::cursor::round_capacity(min_capacity, BCAST_BYTES_MIN_CAPACITY);
        let (region, producer_token) = create_bcast_region(
            fd,
            KIND_BCAST_BYTES,
            capacity,
            1,
            1,
            BCAST_BYTES_BUFFER_OFFSET,
            0,
        )?;
        let anchor = Box::new(BcastProducerAnchor::new(region, producer_token));
        // SAFETY: freshly initialized region matches this ring's layout
        // (fresh counters: the intent floor is 0).
        Ok(unsafe { crate::broadcast_bytes::BytesProducer::from_shm(anchor, capacity, 0) })
    }

    /// Attach to an existing broadcast byte ring as the producer (see
    /// [`broadcast::RingBuffer::attach_shm_producer`](crate::broadcast::RingBuffer::attach_shm_producer)
    /// for the lease and reopen story). Also heals a predecessor's mid-push
    /// crash: the declared-intent frontier stays monotonic across producer
    /// sessions and `latest` is repaired to the committed tail, so
    /// consumers' validation windows never re-admit destroyed bytes.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer.
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast_bytes::BytesProducer> {
        let opened = open_bcast_bytes(fd, false)?;
        let anchor = bcast_attach_producer_anchor(&opened, false)?;
        let floor = bcast_bytes_heal_on_attach(&opened);
        // SAFETY: region validated by the open; forwarded caller contract;
        // `floor` is the sampled `max(intent, tail)`.
        Ok(unsafe {
            crate::broadcast_bytes::BytesProducer::from_shm(anchor, opened.capacity, floor)
        })
    }

    /// Unconditionally take over the producer role — crash recovery while
    /// consumers keep running (see
    /// [`attach_shm_producer`](Self::attach_shm_producer) for the mid-push
    /// healing; everything published stays drainable throughout).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone.
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast_bytes::BytesProducer> {
        let opened = open_bcast_bytes(fd, false)?;
        let anchor = bcast_attach_producer_anchor(&opened, true)?;
        let floor = bcast_bytes_heal_on_attach(&opened);
        // SAFETY: region validated by the open; forwarded caller contract;
        // `floor` is the sampled `max(intent, tail)`.
        Ok(unsafe {
            crate::broadcast_bytes::BytesProducer::from_shm(anchor, opened.capacity, floor)
        })
    }

    /// Unconditionally take over an existing broadcast byte ring — the same
    /// operation as
    /// [`force_attach_shm_producer`](Self::force_attach_shm_producer):
    /// the producer is the only role, and consumers keep no shared state
    /// (see the element ring's
    /// [`recover_shm`](crate::broadcast::RingBuffer::recover_shm)).
    ///
    /// # Safety
    ///
    /// See [`force_attach_shm_producer`](Self::force_attach_shm_producer).
    pub unsafe fn recover_shm(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast_bytes::BytesProducer> {
        // SAFETY: forwarded caller contract.
        unsafe { Self::force_attach_shm_producer(fd) }
    }
}

impl<C> crate::broadcast_bytes::BytesRingBuffer<C>
where
    C: CrossProcess + SelfTimed + Send,
{
    /// Attach a **new consumer** over a **read-only** (`PROT_READ`) mapping:
    /// no lease, no shared writes, unbounded membership — see the element
    /// ring's
    /// [`attach_shm_consumer`](crate::broadcast::RingBuffer::attach_shm_consumer)
    /// for the full contract (including closed-ring attaches succeeding).
    /// The join point is the tail at attach time — always a record boundary.
    ///
    /// # Safety
    ///
    /// Trust model.
    pub unsafe fn attach_shm_consumer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::broadcast_bytes::BytesConsumer<C>> {
        let opened = open_bcast_bytes(fd, true)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe {
            crate::broadcast_bytes::BytesConsumer::from_shm(opened.region, opened.capacity)
        })
    }
}

// =============================================================================
// ANCHORED rings (kinds 9/10): the union of the two shipped designs — the
// gating anchor table (SPMC's consumer-table machinery verbatim, u64
// cursors) plus broadcast's lease-free PROT_READ observers and counters.
// =============================================================================
//
// The producer role reuses the SPSC lease at `OFF_PRODUCER_LEASE` verbatim.
// Anchors hold per-slot leases in the anchor table (claim/epoch/retire
// exactly as the SPMC kinds — `force_detach_anchor` is the compare-and-retire
// zombie answer [A-4.1], `recover_shm` the seqlock-armed table reset).
// Observers keep NO shared state at all: they attach over a `PROT_READ`
// mapping, take no lease, never write a byte, and are unbounded — their drop
// is an munmap, and the read-only mapping turns any store regression in the
// observer path into a deterministic SIGSEGV [P-F8].

/// Region length for an anchored ring: header (+ counter slots) + anchor
/// table + buffer. `stride` is the bytes one capacity unit occupies (the
/// element kind's slot stride; 1 for the byte kind); `table_offset` is the
/// kind's table start (384 elems / 512 bytes).
fn anch_region_len(
    capacity: usize,
    stride: usize,
    table_offset: usize,
    max_anchors: usize,
) -> io::Result<usize> {
    let err = || io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region");
    let table = max_anchors.checked_mul(SPMC_SLOT_STRIDE).ok_or_else(err)?;
    let len = capacity
        .checked_mul(stride)
        .and_then(|b| b.checked_add(table_offset))
        .and_then(|b| b.checked_add(table))
        .ok_or_else(err)?;
    // Same off_t clamp as `region_len` (32-bit sign-flip guard).
    if len as u64 > libc::off_t::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "region length exceeds the platform file-offset limit",
        ));
    }
    Ok(len)
}

/// Initialize a fresh anchored region: size the fd, map it, write the header
/// (zeroed counters, the element kind's slack, the exact stride), a
/// fully-FREE anchor table with sentinel cursors, and a zeroed buffer; take
/// the producer lease. The seqlock write protocol is identical to
/// `create_region`'s.
#[allow(clippy::too_many_arguments)] // one knob per header invariant, as for the broadcast open
fn create_anch_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    capacity: usize,
    unit_size: usize,
    stride: usize,
    table_offset: usize,
    max_anchors: usize,
    slack: u64,
) -> io::Result<(Arc<ShmRegion>, u64)> {
    if max_anchors == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_anchors must be at least 1",
        ));
    }
    // The header stores the table size as a u32: reject anything the store
    // below would silently truncate (before any layout math trusts it).
    if max_anchors > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_anchors exceeds the header field width (u32)",
        ));
    }
    let len = anch_region_len(capacity, stride, table_offset, max_anchors)?;
    // SAFETY: valid fd for the borrow; `anch_region_len` confirmed `len`
    // fits off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // SAFETY: all header offsets are naturally aligned and inside the
    // mapping (see `create_region` for the seqlock/atomics rationale).
    let producer_token = unsafe {
        // Seqlock open: odd generation before touching anything.
        let generation = region.atomic::<AtomicU64>(OFF_GENERATION);
        let g = generation.load(Ordering::Relaxed);
        generation.store(g | 1, Ordering::SeqCst);
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        region
            .atomic::<AtomicU32>(OFF_VERSION)
            .store(VERSION, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_KIND)
            .store(kind, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CAPACITY)
            .store(capacity as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_UNIT_SIZE)
            .store(unit_size as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_ARCH_BITS)
            .store(usize::BITS, Ordering::Relaxed);
        // `max_consumers` holds the anchor-table size (observers are
        // unbounded and keep no shared state — nothing to size for them).
        region
            .atomic::<AtomicU32>(OFF_MAX_CONSUMERS)
            .store(max_anchors as u32, Ordering::Relaxed);
        let pt = lease_token();
        region
            .atomic::<AtomicU64>(OFF_PRODUCER_LEASE)
            .store(pt, Ordering::Relaxed);
        // The SPSC consumer lease is unused (anchors lease per slot,
        // observers are lease-free); keep it deterministically zero.
        region
            .atomic::<AtomicU64>(OFF_CONSUMER_LEASE)
            .store(0, Ordering::Relaxed);
        // Counters + table + buffer: zero wholesale from the cursor area
        // down (tail, closed, slack/starving, stride, intent, latest, the
        // table, the buffer — also pre-faults every page and matches the
        // heap rings' zeroed-buffer guarantee: element slot seq 0 is below
        // every accepted generation, and the byte ring's lapped readers
        // legitimately load bytes the producer never wrote). Still
        // pre-publish: a racing validator sees the odd generation and
        // discards.
        std::ptr::write_bytes(region.at(OFF_WRITE_CURSOR), 0, len - OFF_WRITE_CURSOR);
        // The element kind's observer slack (the byte kind passes 0: its
        // third word is the starving span, which starts clear either way).
        region
            .atomic::<AtomicU64>(OFF_BCAST_SLACK)
            .store(slack, Ordering::Relaxed);
        // The exact per-unit stride, validated for equality on every open
        // (see the broadcast kinds for why `unit_size` alone is not enough).
        region
            .atomic::<AtomicU64>(OFF_BCAST_STRIDE)
            .store(stride as u64, Ordering::Relaxed);
        // Anchor-table cursors to the detached sentinel (leases/controls
        // stay zeroed: FREE@0).
        for slot in 0..max_anchors {
            region
                .anch_slot_cursor(table_offset, slot)
                .store(SLOT_DETACHED, Ordering::Relaxed);
        }
        // Publish the magic last with Release, then close the seqlock.
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(MAGIC, Ordering::Release);
        generation.store((g | 1).wrapping_add(1), Ordering::Release);
        pt
    };
    Ok((Arc::new(region), producer_token))
}

/// A validated anchored region plus the header facts every constructor
/// needs.
pub(crate) struct AnchOpened {
    region: Arc<ShmRegion>,
    capacity: usize,
    max_anchors: usize,
    table_offset: usize,
    generation: u64,
    slack: u64,
}

/// Map and validate an existing anchored region — the SPMC open (table
/// geometry) ∪ the broadcast open (stride equality, the element kind's
/// slack, the observer's `read_only` `PROT_READ` mapping). No occupancy
/// check beyond tail alignment: the table holds protocol-maintained lower
/// bounds and judging them against a live producer is inherently racy (the
/// trust model — validation catches accidents, not adversaries).
#[allow(clippy::too_many_arguments)] // one knob per header invariant, as for the broadcast open
fn open_anch_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    unit_size: usize,
    stride: usize,
    table_offset: usize,
    min_capacity: usize,
    tail_align: u64,
    read_only: bool,
) -> io::Result<AnchOpened> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let file_len = fd_len(fd)?;
    if file_len < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn capacity and max_anchors (the
    // probe is read-only: it never writes, whichever role is attaching).
    let header = ShmRegion::map_read_only(fd, BUFFER_OFFSET)?;
    // Seqlock read (see `open_region`).
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if generation & 1 == 1 {
        return Err(err("ring is being initialized by another process"));
    }
    if header.read_magic() != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch"));
    }
    if unit_size == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if header.read_u64(OFF_UNIT_SIZE) != unit_size as u64 {
        return Err(err("element size mismatch"));
    }
    if header.read_u32(OFF_ARCH_BITS) != usize::BITS {
        return Err(err("architecture (usize width) mismatch"));
    }
    let capacity = header.read_u64(OFF_CAPACITY) as usize;
    if capacity == 0 || !capacity.is_power_of_two() || capacity < min_capacity {
        return Err(err("corrupt capacity"));
    }
    let max_anchors = header.read_u32(OFF_MAX_CONSUMERS) as usize;
    if max_anchors == 0 {
        return Err(err("corrupt max_anchors"));
    }
    // The slack is meaningful on the element kind (the byte kind writes 0
    // and its third word is the starving span — dynamic state, unjudged);
    // `slack < capacity` is the element constructor invariant.
    let slack = if kind == KIND_ANCH_ELEMS {
        let slack = header.read_u64(OFF_BCAST_SLACK);
        if slack >= capacity as u64 {
            return Err(err("corrupt slack: not below the capacity"));
        }
        slack
    } else {
        0
    };
    // Exact-stride check (see the broadcast open for the rationale).
    if header.read_u64(OFF_BCAST_STRIDE) != stride as u64 {
        return Err(err("element slot stride mismatch"));
    }
    drop(header);

    let len = anch_region_len(capacity, stride, table_offset, max_anchors)
        .map_err(|_| err("corrupt geometry: region length overflows"))?;
    if file_len < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = if read_only {
        ShmRegion::map_read_only(fd, len)?
    } else {
        ShmRegion::map(fd, len)?
    };

    // Alignment holds for every individually-published tail value (byte
    // kind: committed tails are record boundaries; element kind: any count
    // decodes, align 1).
    if region.bcast_tail().load(Ordering::Acquire) % tail_align != 0 {
        return Err(err("corrupt tail: not record-aligned"));
    }
    // Seqlock re-check on the full mapping.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    if unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire) != generation {
        return Err(err("ring was re-initialized during validation"));
    }
    Ok(AnchOpened {
        region: Arc::new(region),
        capacity,
        max_anchors,
        table_offset,
        generation,
        slack,
    })
}

/// A freshly claimed consumer/anchor-table slot (SPMC and anchored kinds —
/// one table shape, one claim protocol).
pub(crate) struct SlotClaim {
    pub(crate) slot: usize,
    /// The epoch the slot was claimed at (`ACTIVE@epoch` until detach or
    /// retirement).
    pub(crate) epoch: u32,
    /// The slot lease token this claimant wrote.
    pub(crate) token: u64,
    /// The join point: only messages published after this cursor are seen.
    pub(crate) joined: u64,
}

/// The subscribe choreography over a consumer/anchor table — the shm
/// mapping of the heap registries' [M-F2] protocol, with the control word
/// playing the bitmap's role, parameterized by the kind's `table_offset`
/// (`SPMC_TABLE_OFFSET` for the gating kinds; the anchored kinds' offsets
/// for kinds 9/10 — the cursor word at offset 128 is the producer's
/// published cursor for every one of them). Returns `None` when no FREE
/// slot exists (table full; RETIRED slots stay unavailable until
/// `recover_shm`).
///
/// Order (each step before the next):
/// 1. CAS control `FREE@e -> ACTIVE@e+1` — the claim and the **registration
///    event** in one RMW (the epoch bump gives every *occupancy* of a slot
///    a distinct epoch, which is what lets `force_detach_consumer` prove it
///    is retiring the occupancy the caller diagnosed dead and not a
///    successor that re-claimed a gracefully freed slot), strictly BEFORE
///    the SeqCst fence: the producer's
///    rescan observes consumers only through the control word, so this is
///    the store the [M-F2] fence dichotomy is about (set after the fence, a
///    scan could miss the slot *while* the re-read below returns a stale
///    write cursor, and the producer would lap a consumer it never saw).
/// 2. Store the provisional cursor (a lower bound of the join point). The
///    window between 1 and 2 — control ACTIVE, cursor still the previous
///    occupant's sentinel/value — is covered on the scan side: a sentinel
///    read is skipped without caching (and by the fence dichotomy such a
///    joiner's join point is past the scan's wrap point); a leftover real
///    cursor is at most the write cursor at the previous detach, which the
///    new join point cannot undercut — a valid lower bound either way.
/// 3. `fence(SeqCst)` — pairs with the producer's pre-scan fence.
/// 4. Re-read the write cursor: **the join point is the re-read**; publish
///    it as the final cursor (Release).
/// 5. Take the slot lease (opaque random token, exactly like the role
///    leases; teardown and the flush guard check it).
pub(crate) fn claim_table_slot(
    region: &ShmRegion,
    table_offset: usize,
    max_slots: usize,
) -> Option<SlotClaim> {
    // The producer's published cursor: `bcast_tail`/`spmc_write_cursor` are
    // the same u64 word at offset 128 for every table-bearing kind.
    let tail = region.bcast_tail();
    'slots: for slot in 0..max_slots {
        let control = region.anch_slot_control(table_offset, slot);
        let mut current = control.load(Ordering::Acquire);
        loop {
            if control_state(current) != STATE_FREE {
                continue 'slots;
            }
            // Bump the epoch on claim: each occupancy of the slot gets its
            // own epoch (graceful detach keeps it, so FREE@e means "last
            // occupied by epoch e").
            let epoch = control_epoch(current).wrapping_add(1);
            match control.compare_exchange(
                current,
                control_word(epoch, STATE_ACTIVE),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let cursor = region.anch_slot_cursor(table_offset, slot);
                    let provisional = slot_guard(tail.load(Ordering::Acquire));
                    cursor.store(provisional, Ordering::Release);
                    std::sync::atomic::fence(Ordering::SeqCst);
                    let joined = tail.load(Ordering::Acquire);
                    cursor.store(slot_guard(joined), Ordering::Release);
                    let token = lease_token();
                    region
                        .anch_slot_lease(table_offset, slot)
                        .store(token, Ordering::Release);
                    return Some(SlotClaim {
                        slot,
                        epoch,
                        token,
                        joined,
                    });
                }
                // Lost a race (concurrent joiner or force-detach): re-examine
                // this slot with the fresh value.
                Err(fresh) => current = fresh,
            }
        }
    }
    None
}

/// Roll a just-made claim back (attach-time seqlock conflict): the graceful
/// detach sequence minus the flush — sentinel, then availability, then the
/// lease — **but only while the region generation still equals
/// `generation`, the one the claim was made under**. A generation change
/// means a concurrent `create_shm`/`recover_shm` reset the whole table (and
/// restarted epochs), so the words this claim wrote no longer belong to it:
/// an unconditional rollback could store the sentinel over a *new* ring's
/// freshly-claimed cursor and CAS its `ACTIVE@e` control (epochs restart, so
/// `e` can collide) back to FREE. In that case the only safe move is to
/// touch nothing — at worst our claim landed after the reset and leaks one
/// slot in the new table until the next `recover_shm`, which is the safe
/// direction (a leak, never a clobber). Under an unchanged generation the
/// claimant still owns the ACTIVE slot exclusively (only `force_detach` can
/// move it, and the epoch-conditional CAS below fails harmlessly on a
/// retired slot), so the sentinel store and the CASes are sound.
pub(crate) fn release_table_claim(
    region: &ShmRegion,
    table_offset: usize,
    claim: &SlotClaim,
    generation: u64,
) {
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let gen_now = unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if gen_now != generation {
        return;
    }
    region
        .anch_slot_cursor(table_offset, claim.slot)
        .store(SLOT_DETACHED, Ordering::Release);
    let _ = region
        .anch_slot_control(table_offset, claim.slot)
        .compare_exchange(
            control_word(claim.epoch, STATE_ACTIVE),
            control_word(claim.epoch, STATE_FREE),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    let _ = region
        .anch_slot_lease(table_offset, claim.slot)
        .compare_exchange(claim.token, 0, Ordering::AcqRel, Ordering::Acquire);
}

/// Retire a consumer/anchor-table slot [A-4.1]: one compare-and-retire CAS
/// `ACTIVE@epoch -> RETIRED@epoch+1`. The epoch is the caller's proof that
/// it is retiring the *same occupancy it diagnosed dead* (each claim bumps
/// the epoch, so a healthy consumer that re-claimed the slot after the dead
/// one gracefully freed it holds a different epoch and the CAS fails instead
/// of retiring the living). A retired slot is never re-issued until
/// `recover_shm`; the (possibly live) previous holder's stores land on words
/// nobody reads. The lease and cursor words are left as-is — they are dead
/// until the table reset.
fn force_detach_table_slot(
    region: &ShmRegion,
    table_offset: usize,
    slot: usize,
    epoch: u32,
) -> io::Result<()> {
    region
        .anch_slot_control(table_offset, slot)
        .compare_exchange(
            control_word(epoch, STATE_ACTIVE),
            control_word(epoch.wrapping_add(1), STATE_RETIRED),
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(drop)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "slot epoch mismatch — the slot no longer belongs to the \
                 anchor you diagnosed (freed, re-claimed, or already \
                 retired)",
            )
        })
}

/// Reset a whole consumer/anchor table (recover): compute the
/// at-least-once resume point — the slowest previously-registered cursor,
/// ignoring implausible values (lag beyond one capacity: stale zombie
/// leftovers the producer had already stopped honoring) — then free every
/// slot with a bumped epoch, a zeroed lease, and the detached sentinel.
/// Returns the resume cursor (the producer's published cursor itself when
/// no consumer had registered). The rewrite is **seqlock-armed** exactly
/// like the create path (odd generation before the first table write, `+2`
/// close after the last): the reset is a re-initialization event for the
/// table, and without the bump a concurrent attach/subscribe whose claim
/// interleaves the rewrite would pass its post-claim generation re-check
/// and keep a slot the reset just freed out from under it.
fn reset_gate_table(
    region: &ShmRegion,
    table_offset: usize,
    max_slots: usize,
    capacity: u64,
) -> u64 {
    // Seqlock open (see `create_region` for the ordering rationale).
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) };
    let g = generation.load(Ordering::Relaxed);
    generation.store(g | 1, Ordering::SeqCst);
    let tail = region.bcast_tail().load(Ordering::Acquire);
    let mut max_lag = 0u64;
    for slot in 0..max_slots {
        if control_state(
            region
                .anch_slot_control(table_offset, slot)
                .load(Ordering::Acquire),
        ) == STATE_FREE
        {
            continue;
        }
        let cursor = region
            .anch_slot_cursor(table_offset, slot)
            .load(Ordering::Acquire);
        if cursor == SLOT_DETACHED {
            continue;
        }
        let lag = tail.wrapping_sub(cursor);
        if lag <= capacity && lag > max_lag {
            max_lag = lag;
        }
    }
    for slot in 0..max_slots {
        // Detach order per slot: sentinel, lease, availability last.
        region
            .anch_slot_cursor(table_offset, slot)
            .store(SLOT_DETACHED, Ordering::Release);
        region
            .anch_slot_lease(table_offset, slot)
            .store(0, Ordering::Release);
        let control = region.anch_slot_control(table_offset, slot);
        let current = control.load(Ordering::Acquire);
        control.store(
            control_word(control_epoch(current).wrapping_add(1), STATE_FREE),
            Ordering::Release,
        );
    }
    // Seqlock close: even generation, strictly greater than before.
    generation.store((g | 1).wrapping_add(1), Ordering::Release);
    tail.wrapping_sub(max_lag)
}

/// The shm backing for a gating **producer** (SPMC and anchored kinds):
/// keeps the mapping alive, holds the producer role lease (same offset and
/// discipline as the SPSC rings), and exposes the consumer/anchor table to
/// the producer's rescans, parameterized by the kind's table offset. Only
/// the consumer-side wait strategy is carried — the producer's own waits
/// use fresh per-call instances of its (stateless, `CrossProcess`)
/// strategy.
pub(crate) struct GateShmProducer<C> {
    region: Arc<ShmRegion>,
    token: u64,
    /// Fork guard, exactly as in [`ShmAnchor`].
    owner_pid: libc::pid_t,
    max_slots: usize,
    table_offset: usize,
    pub(crate) consumer_wait: C,
}

impl<C: Default> GateShmProducer<C> {
    fn new(region: Arc<ShmRegion>, token: u64, max_slots: usize, table_offset: usize) -> Self {
        Self {
            region,
            token,
            // SAFETY: getpid is always safe.
            owner_pid: unsafe { libc::getpid() },
            max_slots,
            table_offset,
            consumer_wait: C::default(),
        }
    }
}

impl<C> GateShmProducer<C> {
    pub(crate) fn region(&self) -> &Arc<ShmRegion> {
        &self.region
    }

    /// The table size (`max_consumers`/`max_anchors`, fixed at creation).
    pub(crate) fn max_slots(&self) -> usize {
        self.max_slots
    }

    pub(crate) fn table_offset(&self) -> usize {
        self.table_offset
    }

    #[inline(always)]
    pub(crate) fn slot_control(&self, slot: usize) -> &AtomicU64 {
        self.region.anch_slot_control(self.table_offset, slot)
    }

    #[inline(always)]
    pub(crate) fn slot_cursor(&self, slot: usize) -> &AtomicU64 {
        self.region.anch_slot_cursor(self.table_offset, slot)
    }

    /// Number of ACTIVE table slots (snapshot). Lossy observers are not
    /// counted: nothing tracks them.
    pub(crate) fn active_count(&self) -> usize {
        (0..self.max_slots)
            .filter(|&slot| control_is_active(self.slot_control(slot).load(Ordering::Relaxed)))
            .count()
    }

    /// Whether the producer lease still holds this handle's token (see
    /// [`ShmAnchor::owns_lease`]).
    pub(crate) fn owns_lease(&self) -> bool {
        // SAFETY: the lease offset is 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(OFF_PRODUCER_LEASE) };
        lease.load(Ordering::Acquire) == self.token
    }

    /// Fork guard (see [`ShmAnchor::owned_by_current_process`]).
    pub(crate) fn owned_by_current_process(&self) -> bool {
        // SAFETY: getpid is always safe.
        (unsafe { libc::getpid() }) == self.owner_pid
    }
}

impl<C> Drop for GateShmProducer<C> {
    fn drop(&mut self) {
        // Guarded lease release, exactly as [`ShmAnchor`]'s: pid guard
        // against fork copies, token CAS against force-takeovers.
        if !self.owned_by_current_process() {
            return;
        }
        // SAFETY: the lease offset is 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(OFF_PRODUCER_LEASE) };
        let _ = lease.compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// The shm backing for a gating **consumer** (an SPMC consumer or an
/// anchored ring's anchor): the mapping, this handle's table-slot
/// coordinates (slot index, claim epoch, slot lease token), the fork-guard
/// pid, and the per-handle wait strategies, parameterized by the kind's
/// table offset.
pub(crate) struct GateShmConsumer<P, C> {
    region: Arc<ShmRegion>,
    slot: usize,
    epoch: u32,
    token: u64,
    owner_pid: libc::pid_t,
    max_slots: usize,
    table_offset: usize,
    pub(crate) producer_wait: P,
    pub(crate) consumer_wait: C,
}

impl<P: Default, C: Default> GateShmConsumer<P, C> {
    pub(crate) fn new(
        region: Arc<ShmRegion>,
        claim: SlotClaim,
        max_slots: usize,
        table_offset: usize,
    ) -> Self {
        Self {
            region,
            slot: claim.slot,
            epoch: claim.epoch,
            token: claim.token,
            // SAFETY: getpid is always safe.
            owner_pid: unsafe { libc::getpid() },
            max_slots,
            table_offset,
            producer_wait: P::default(),
            consumer_wait: C::default(),
        }
    }
}

impl<P, C> GateShmConsumer<P, C> {
    pub(crate) fn region(&self) -> &Arc<ShmRegion> {
        &self.region
    }

    pub(crate) fn slot(&self) -> usize {
        self.slot
    }

    /// The epoch this handle's slot was claimed at — the occupancy proof
    /// `force_detach_consumer`/`force_detach_anchor` take.
    pub(crate) fn epoch(&self) -> u32 {
        self.epoch
    }

    /// The table size (`max_consumers`/`max_anchors`, fixed at creation).
    pub(crate) fn max_slots(&self) -> usize {
        self.max_slots
    }

    pub(crate) fn table_offset(&self) -> usize {
        self.table_offset
    }

    /// Whether the slot lease still holds this handle's token. One Acquire
    /// load of the slot's own line (which this consumer's flush traffic
    /// already owns) — the hot-flush zombie guard. Note a force-detached
    /// zombie still holds its lease: its stores keep landing on the RETIRED
    /// slot, which no scan reads [A-4.1]; only `recover_shm`'s table reset
    /// (which zeroes leases) silences it here.
    #[inline]
    pub(crate) fn owns_slot(&self) -> bool {
        self.region
            .anch_slot_lease(self.table_offset, self.slot)
            .load(Ordering::Acquire)
            == self.token
    }

    /// Fork guard (see [`ShmAnchor::owned_by_current_process`]).
    pub(crate) fn owned_by_current_process(&self) -> bool {
        // SAFETY: getpid is always safe.
        (unsafe { libc::getpid() }) == self.owner_pid
    }

    /// Graceful detach, called from the consumer's Drop after the flush and
    /// the cursor-sentinel store: return the slot (CAS `ACTIVE@e -> FREE@e`
    /// — epoch UNCHANGED, the slot is immediately reusable; the CAS fails
    /// harmlessly on a force-retired slot, preserving retirement), release
    /// the slot lease (guarded CAS), and wake a gated producer [A-1.3].
    pub(crate) fn detach(&self)
    where
        P: crate::wait::WaitStrategy,
    {
        let _ = self
            .region
            .anch_slot_control(self.table_offset, self.slot)
            .compare_exchange(
                control_word(self.epoch, STATE_ACTIVE),
                control_word(self.epoch, STATE_FREE),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        let _ = self
            .region
            .anch_slot_lease(self.table_offset, self.slot)
            .compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
        self.producer_wait.notify();
    }
}

/// Claim (or force-take) the producer role over a validated anchored region
/// and reset the closed word — a (re)attached producer re-opens the ring
/// (shm `Closed` is end-of-session; only the producer ever writes that
/// word, and we now hold its lease).
fn anch_attach_producer_anchor<C: Default>(
    opened: &AnchOpened,
    force: bool,
) -> io::Result<Box<GateShmProducer<C>>> {
    let region = Arc::clone(&opened.region);
    let token = if force {
        force_claim_lease(&region, Role::Producer)
    } else {
        claim_lease(&region, Role::Producer, opened.generation)?
    };
    region.bcast_closed().store(0, Ordering::Release);
    Ok(Box::new(GateShmProducer::new(
        region,
        token,
        opened.max_anchors,
        opened.table_offset,
    )))
}

/// Claim an anchor-table slot over a validated anchored region: refuse
/// closed rings (mirroring the heap `SubscribeError::Closed`), map a full
/// table to `AddrInUse` (the role-conflict error), and re-check the seqlock
/// generation after the claim exactly as `claim_lease` does (the rollback is
/// itself generation-conditional). Returns the backing plus the join point.
fn anch_attach_anchor<P: Default, C: Default>(
    opened: &AnchOpened,
) -> io::Result<(Box<GateShmConsumer<P, C>>, u64)> {
    let region = Arc::clone(&opened.region);
    if region.bcast_closed().load(Ordering::Acquire) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "ring closed: producer dropped (attach a new producer to reopen)",
        ));
    }
    let claim =
        claim_table_slot(&region, opened.table_offset, opened.max_anchors).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                "anchor table is full (max_anchors is fixed at creation; \
                 retired slots free only via recover_shm)",
            )
        })?;
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let gen_now = unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if gen_now != opened.generation {
        // Conditional rollback: `release_table_claim` re-checks the
        // generation and leaves a re-initialized table strictly alone.
        release_table_claim(&region, opened.table_offset, &claim, opened.generation);
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "ring was re-initialized during attach",
        ));
    }
    let joined = claim.joined;
    Ok((
        Box::new(GateShmConsumer::new(
            region,
            claim,
            opened.max_anchors,
            opened.table_offset,
        )),
        joined,
    ))
}

/// Both required halves of a shm-backed anchored element ring (observers
/// attach separately, unbounded).
pub type AnchElemPair<T, P, C> = (
    crate::anchored::Producer<T, P, C>,
    crate::anchored::Anchor<T, P, C>,
);
/// Both required halves of a shm-backed anchored byte ring.
pub type AnchBytesPair<P, C> = (
    crate::anchored_bytes::BytesProducer<P, C>,
    crate::anchored_bytes::BytesAnchor<P, C>,
);

/// Element-type invariants for the anchored element ring, enforced on
/// create AND attach (as errors — fallible paths). The slot carries the seq
/// word in front of the payload, so the alignment bound is the *slot's*.
fn check_anch_elem_type<T>() -> io::Result<()> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    if std::mem::size_of::<T>() == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if crate::anchored::shm_slot_align::<T>() > 128 {
        return Err(err("element alignment exceeds the buffer offset alignment"));
    }
    Ok(())
}

impl<T> crate::anchored::RingBuffer<T>
where
    // All four bounds pull their weight: `ShmItem` asserts every bit pattern
    // a cooperating peer writes is a valid `T` (cross-process reads of
    // memory that is untrusted by construction); `NoUninit` asserts every
    // byte of a valid `T` is initialized data (the observers' racing
    // word-wise atomic copies read and write every byte — neither trait
    // implies the other); `Send` because observers copy values across the
    // process/thread boundary and teardown frees producer-written storage;
    // `Sync` because anchors take shared `&T` borrows of the *same* mapped
    // element from several processes at once.
    T: ShmItem + crate::broadcast::NoUninit + Send + Sync,
{
    /// Initialize `fd` as a fresh shm-backed anchored element ring with a
    /// `max_anchors`-slot anchor table and return the producer plus one
    /// initial anchor, with default ([`YieldWait`](crate::wait::YieldWait))
    /// wait strategies. Observers are **not** table-bound: attach any number
    /// with [`attach_shm_observer`](Self::attach_shm_observer), each
    /// lease-free over its own **read-only** mapping.
    ///
    /// Unlike heap anchor membership (unbounded), `max_anchors` is fixed at
    /// creation — a mapped layout cannot grow. That constraint is physical,
    /// not a design choice. The observer reposition slack defaults to
    /// `capacity / 8` (clamped to at least 1), as on the heap.
    ///
    /// ```
    /// use std::os::fd::AsFd;
    /// use rust_rb::{anchored, memfd};
    /// # fn main() -> std::io::Result<()> {
    /// let fd = memfd("anchored-doc")?;
    /// // SAFETY: fresh private memfd, only cooperating handles touch it.
    /// let (mut tx, mut anchor) =
    ///     unsafe { anchored::RingBuffer::<u64>::create_shm(fd.as_fd(), 64, 8)? };
    /// // Observers attach lease-free, over a read-only mapping.
    /// // SAFETY: cooperating handles only.
    /// let mut observer = unsafe {
    ///     anchored::RingBuffer::<u64>::attach_shm_observer(fd.as_fd())?
    /// };
    /// tx.push(7);
    /// assert_eq!(anchor.pop(), Ok(7));
    /// assert_eq!(observer.pop(), Ok(7));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self): the region must only ever be
    /// accessed by cooperating rust-rb handles.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::type_complexity)]
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_anchors: usize,
    ) -> io::Result<(crate::anchored::Producer<T>, crate::anchored::Anchor<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe {
            crate::anchored::RingBuffer::<T, _, _>::create_shm_with(fd, min_capacity, max_anchors)
        }
    }

    /// Unconditionally take over an existing anchored ring: force-take the
    /// producer role, **reset the whole anchor table** (leases zeroed, every
    /// slot FREE at a bumped epoch — retired slots become issuable again),
    /// and return a fresh pair. The returned anchor resumes at the slowest
    /// previously-registered anchor cursor, so recovery is at-least-once;
    /// the producer resumes at the committed tail with its gating caches
    /// seeded to rescan. Live observers need nothing: everything published
    /// stays drainable throughout.
    ///
    /// # Safety
    ///
    /// Trust model, plus: the takeover is unconditional — the caller asserts
    /// **every** previous holder (producer and all anchors) is gone. A
    /// still-live anchor would be silently unregistered (its flushes are
    /// suppressed by the slot-lease guard, but its reads lose all gating
    /// protection); a still-live producer would corrupt the ring.
    #[allow(clippy::type_complexity)]
    pub unsafe fn recover_shm(
        fd: BorrowedFd<'_>,
    ) -> io::Result<(crate::anchored::Producer<T>, crate::anchored::Anchor<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { crate::anchored::RingBuffer::<T, _, _>::recover_shm_with(fd) }
    }
}

impl<T, P, C> crate::anchored::RingBuffer<T, P, C>
where
    T: ShmItem + crate::broadcast::NoUninit + Send + Sync,
    P: CrossProcess + SelfTimed + Send + Sync,
    C: CrossProcess + SelfTimed + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>, read_only: bool) -> io::Result<AnchOpened> {
        check_anch_elem_type::<T>()?;
        // Capacity floor 2 = the heap constructor's floor (the empty-registry
        // gating default `next_seq - 1` needs it); element cursors are always
        // decodable, so no alignment constraint (align 1).
        open_anch_region(
            fd,
            KIND_ANCH_ELEMS,
            std::mem::size_of::<T>(),
            crate::anchored::shm_slot_stride::<T>(),
            ANCH_ELEMS_TABLE_OFFSET,
            2,
            1,
            read_only,
        )
    }

    /// [`create_shm`](crate::anchored::RingBuffer::create_shm) with explicit
    /// [`CrossProcess`] + [`SelfTimed`] wait strategies (both bounds on both
    /// sides: the strategy must survive a process boundary *and* never need
    /// a peer notify — the spin family) and the default observer slack.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_anchors: usize,
    ) -> io::Result<AnchElemPair<T, P, C>> {
        let capacity = crate::cursor::round_capacity(min_capacity, 2);
        let slack = crate::anchored::shm_default_slack(capacity as u64);
        // SAFETY: forwarded caller contract.
        unsafe { Self::create_shm_with_slack(fd, min_capacity, max_anchors, slack as usize) }
    }

    /// [`create_shm_with`](Self::create_shm_with) with an explicit observer
    /// reposition `slack` [A-3.2] — the create-time knob every observer of
    /// this ring inherits (stored in the region header and validated on
    /// attach). Anchors never lag, so the slack concerns observers only.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`. Unlike the heap constructor,
    /// `slack >= capacity` is an **error** (`InvalidInput`), not a panic —
    /// shm constructors surface misuse on their fallible path.
    pub unsafe fn create_shm_with_slack(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_anchors: usize,
        slack: usize,
    ) -> io::Result<AnchElemPair<T, P, C>> {
        check_anch_elem_type::<T>()?;
        let capacity = crate::cursor::round_capacity(min_capacity, 2);
        if slack >= capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slack must be less than the capacity",
            ));
        }
        let (region, producer_token) = create_anch_region(
            fd,
            KIND_ANCH_ELEMS,
            capacity,
            std::mem::size_of::<T>(),
            crate::anchored::shm_slot_stride::<T>(),
            ANCH_ELEMS_TABLE_OFFSET,
            max_anchors,
            slack as u64,
        )?;
        let claim = claim_table_slot(&region, ANCH_ELEMS_TABLE_OFFSET, max_anchors)
            .expect("fresh table has free slots");
        let joined = claim.joined;
        let producer_anchor = Box::new(GateShmProducer::new(
            Arc::clone(&region),
            producer_token,
            max_anchors,
            ANCH_ELEMS_TABLE_OFFSET,
        ));
        let anchor_backing = Box::new(GateShmConsumer::new(
            region,
            claim,
            max_anchors,
            ANCH_ELEMS_TABLE_OFFSET,
        ));
        // SAFETY: freshly initialized region matches this ring's layout.
        unsafe {
            Ok((
                crate::anchored::Producer::from_shm(producer_anchor, capacity),
                crate::anchored::Anchor::from_shm(anchor_backing, capacity, joined),
            ))
        }
    }

    /// Attach to an existing anchored element ring as the producer. Fails
    /// with `AddrInUse` while the producer lease is held; resets the
    /// graceful `closed` flag (the ring is open again — shm `Closed` is
    /// end-of-session for both consumer roles). The gating caches are
    /// rebuilt from the live anchor table before the handle is returned —
    /// never from defaults.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size and slot stride are
    /// validated).
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored::Producer<T, P, C>> {
        let opened = Self::open(fd, false)?;
        let anchor = anch_attach_producer_anchor::<C>(&opened, false)?;
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::anchored::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Unconditionally take over the producer role (single-side crash
    /// recovery while anchors and observers keep running; the element ring
    /// needs no counter healing — a reader racing the recovered producer's
    /// re-publishes self-heals via the slot seqlock generations).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone, plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored::Producer<T, P, C>> {
        let opened = Self::open(fd, false)?;
        let anchor = anch_attach_producer_anchor::<C>(&opened, true)?;
        // SAFETY: region validated by open(); forwarded caller contract.
        Ok(unsafe { crate::anchored::Producer::from_shm(anchor, opened.capacity) })
    }

    /// Attach a **new anchor**: claims a FREE anchor-table slot (the shm
    /// face of `subscribe_anchor`; the join point is the unified cursor at
    /// claim time — from there the anchor sees **every** message, even
    /// against a free-running producer [§9.6]). Fails with `AddrInUse` when
    /// the table is full and `BrokenPipe` when the ring is closed.
    ///
    /// # Safety
    ///
    /// Trust model, plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_anchor(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored::Anchor<T, P, C>> {
        let opened = Self::open(fd, false)?;
        let (backing, joined) = anch_attach_anchor::<P, C>(&opened)?;
        // SAFETY: region validated by open(); the claim choreography just
        // ran (its final store published `joined` into the slot).
        Ok(unsafe { crate::anchored::Anchor::from_shm(backing, opened.capacity, joined) })
    }

    /// Attach a **new observer**: validates the header and maps the region
    /// **read-only** (`PROT_READ`). The observer takes **no lease**, claims
    /// no table slot, and never writes a byte of shared state — membership
    /// is unbounded, and dropping it just unmaps. Its join point is the
    /// unified cursor at attach time.
    ///
    /// Attaching to a *closed* ring succeeds (mirroring the heap
    /// `subscribe_observer`): the observer is born drained and pops
    /// [`PopError::Closed`](crate::anchored::PopError::Closed) — until a new
    /// producer attach reopens the session.
    ///
    /// The read-only mapping doubles as enforcement: a store accidentally
    /// introduced anywhere in the observer path is a deterministic SIGSEGV
    /// rather than silent corruption.
    ///
    /// # Safety
    ///
    /// Trust model, plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_observer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored::Observer<T, P, C>> {
        let opened = Self::open(fd, true)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe {
            crate::anchored::Observer::from_shm(
                opened.region,
                opened.capacity,
                opened.slack,
                opened.max_anchors,
            )
        })
    }

    /// Retire anchor-table slot `slot` [A-4.1]: bump its epoch and mark it
    /// RETIRED, **iff it is still `ACTIVE` at `epoch`**. `(slot, epoch)` is
    /// what the victim's
    /// [`Anchor::shm_slot_epoch`](crate::anchored::Anchor::shm_slot_epoch)
    /// reported — the epoch proves the caller is retiring the same occupancy
    /// it observed dead (every claim bumps the epoch, so a healthy anchor
    /// that re-claimed a gracefully freed slot holds a different epoch and
    /// this fails with `InvalidInput` instead of retiring the living). The
    /// producer's next rescan stops honoring a retired slot's cursor
    /// (un-gating a producer blocked on a dead anchor); the slot is **never
    /// re-issued** until [`recover_shm`](crate::anchored::RingBuffer::recover_shm)
    /// resets the table.
    ///
    /// # Safety
    ///
    /// The caller asserts the holder of `(slot, epoch)` is **dead**. Same
    /// trust register as `force_attach`: a live holder's flushes land on the
    /// retired slot (harmless), but its **reads lose all gating
    /// protection** — the producer may overwrite data it still borrows.
    pub unsafe fn force_detach_anchor(
        fd: BorrowedFd<'_>,
        slot: usize,
        epoch: u32,
    ) -> io::Result<()> {
        let opened = Self::open(fd, false)?;
        if slot >= opened.max_anchors {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slot index out of range for this ring's anchor table",
            ));
        }
        force_detach_table_slot(&opened.region, opened.table_offset, slot, epoch)
    }

    /// [`recover_shm`](crate::anchored::RingBuffer::recover_shm) with
    /// explicit wait strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](crate::anchored::RingBuffer::recover_shm), plus
    /// the `T` caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<AnchElemPair<T, P, C>> {
        let opened = Self::open(fd, false)?;
        let producer_anchor = anch_attach_producer_anchor::<C>(&opened, true)?;
        let resume = reset_gate_table(
            &opened.region,
            opened.table_offset,
            opened.max_anchors,
            opened.capacity as u64,
        );
        let claim = claim_table_slot(&opened.region, opened.table_offset, opened.max_anchors)
            .expect("freshly reset table has free slots");
        // Move the fresh slot back to the resume point (a lower cursor only
        // gates the producer more — and the producer is us, not yet pushing).
        opened
            .region
            .anch_slot_cursor(opened.table_offset, claim.slot)
            .store(slot_guard(resume), Ordering::Release);
        let anchor_backing = Box::new(GateShmConsumer::new(
            Arc::clone(&opened.region),
            claim,
            opened.max_anchors,
            opened.table_offset,
        ));
        // SAFETY: region validated by open(); forwarded caller contract.
        unsafe {
            Ok((
                crate::anchored::Producer::from_shm(producer_anchor, opened.capacity),
                crate::anchored::Anchor::from_shm(anchor_backing, opened.capacity, resume),
            ))
        }
    }
}

/// Byte-ring capacity floor and record alignment for the anchored byte
/// ring, shared with the ring's own constructor and frame decoder so they
/// cannot drift.
const ANCH_BYTES_MIN_CAPACITY: usize = crate::anchored_bytes::MIN_CAPACITY;
const ANCH_BYTES_TAIL_ALIGN: u64 = crate::broadcast_bytes::ALIGN as u64;

impl crate::anchored_bytes::BytesRingBuffer {
    /// Initialize `fd` as a fresh shm-backed anchored **byte** ring (see
    /// [`anchored::RingBuffer::create_shm`](crate::anchored::RingBuffer::create_shm);
    /// capacity is in bytes, minimum 16; observers attach separately with
    /// [`attach_shm_observer`](Self::attach_shm_observer), lease-free over
    /// read-only mappings).
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::type_complexity)]
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_anchors: usize,
    ) -> io::Result<(
        crate::anchored_bytes::BytesProducer,
        crate::anchored_bytes::BytesAnchor,
    )> {
        // SAFETY: forwarded caller contract.
        unsafe {
            crate::anchored_bytes::BytesRingBuffer::<_, _>::create_shm_with(
                fd,
                min_capacity,
                max_anchors,
            )
        }
    }

    /// Unconditionally take over an existing anchored byte ring (see
    /// [`anchored::RingBuffer::recover_shm`](crate::anchored::RingBuffer::recover_shm):
    /// full table reset; the returned anchor resumes at the slowest
    /// previously-registered anchor cursor — always a record boundary —
    /// at-least-once). Also heals a predecessor's mid-push crash exactly as
    /// [`attach_shm_producer`](Self::attach_shm_producer) does.
    ///
    /// # Safety
    ///
    /// See [`anchored::RingBuffer::recover_shm`](crate::anchored::RingBuffer::recover_shm).
    #[allow(clippy::type_complexity)]
    pub unsafe fn recover_shm(
        fd: BorrowedFd<'_>,
    ) -> io::Result<(
        crate::anchored_bytes::BytesProducer,
        crate::anchored_bytes::BytesAnchor,
    )> {
        // SAFETY: forwarded caller contract.
        unsafe { crate::anchored_bytes::BytesRingBuffer::<_, _>::recover_shm_with(fd) }
    }
}

impl<P, C> crate::anchored_bytes::BytesRingBuffer<P, C>
where
    P: CrossProcess + SelfTimed + Send + Sync,
    C: CrossProcess + SelfTimed + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>, read_only: bool) -> io::Result<AnchOpened> {
        open_anch_region(
            fd,
            KIND_ANCH_BYTES,
            1,
            1,
            ANCH_BYTES_TABLE_OFFSET,
            ANCH_BYTES_MIN_CAPACITY,
            ANCH_BYTES_TAIL_ALIGN,
            read_only,
        )
    }

    /// [`create_shm`](crate::anchored_bytes::BytesRingBuffer::create_shm)
    /// with explicit [`CrossProcess`] + [`SelfTimed`] wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
        max_anchors: usize,
    ) -> io::Result<AnchBytesPair<P, C>> {
        let capacity = crate::cursor::round_capacity(min_capacity, ANCH_BYTES_MIN_CAPACITY);
        let (region, producer_token) = create_anch_region(
            fd,
            KIND_ANCH_BYTES,
            capacity,
            1,
            1,
            ANCH_BYTES_TABLE_OFFSET,
            max_anchors,
            0,
        )?;
        let claim = claim_table_slot(&region, ANCH_BYTES_TABLE_OFFSET, max_anchors)
            .expect("fresh table has free slots");
        let joined = claim.joined;
        let producer_anchor = Box::new(GateShmProducer::new(
            Arc::clone(&region),
            producer_token,
            max_anchors,
            ANCH_BYTES_TABLE_OFFSET,
        ));
        let anchor_backing = Box::new(GateShmConsumer::new(
            region,
            claim,
            max_anchors,
            ANCH_BYTES_TABLE_OFFSET,
        ));
        // SAFETY: freshly initialized region matches this ring's layout
        // (fresh counters: the intent floor is 0).
        unsafe {
            Ok((
                crate::anchored_bytes::BytesProducer::from_shm(producer_anchor, capacity, 0),
                crate::anchored_bytes::BytesAnchor::from_shm(anchor_backing, capacity, joined),
            ))
        }
    }

    /// Attach to an existing anchored byte ring as the producer (see the
    /// element ring's
    /// [`attach_shm_producer`](crate::anchored::RingBuffer::attach_shm_producer)
    /// for the lease and reopen story). Also resets the starving flag a
    /// departed predecessor may have left set, and heals a mid-push crash:
    /// the declared-intent frontier stays **monotonic across producer
    /// sessions** (the new producer's pushes declare
    /// `max(new_tail, floor)` at §9.3 step (2) — after the gate, as always)
    /// and `latest` is repaired to the committed tail, so observers'
    /// validation windows never re-admit destroyed bytes.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer.
    pub unsafe fn attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored_bytes::BytesProducer<P, C>> {
        let opened = Self::open(fd, false)?;
        let anchor = anch_attach_producer_anchor::<C>(&opened, false)?;
        // A newly-attached producer is not starving (only the producer
        // writes this flag, and we now hold its lease).
        opened.region.anch_starving().store(0, Ordering::Release);
        let floor = bytes_heal_on_attach(&opened.region);
        // SAFETY: region validated by open(); forwarded caller contract;
        // `floor` is the sampled `max(intent, tail)`.
        Ok(unsafe {
            crate::anchored_bytes::BytesProducer::from_shm(anchor, opened.capacity, floor)
        })
    }

    /// Unconditionally take over the producer role (see
    /// [`attach_shm_producer`](Self::attach_shm_producer) for the starving
    /// reset and the mid-push healing; everything published stays drainable
    /// throughout).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone.
    pub unsafe fn force_attach_shm_producer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored_bytes::BytesProducer<P, C>> {
        let opened = Self::open(fd, false)?;
        let anchor = anch_attach_producer_anchor::<C>(&opened, true)?;
        opened.region.anch_starving().store(0, Ordering::Release);
        let floor = bytes_heal_on_attach(&opened.region);
        // SAFETY: region validated by open(); forwarded caller contract;
        // `floor` is the sampled `max(intent, tail)`.
        Ok(unsafe {
            crate::anchored_bytes::BytesProducer::from_shm(anchor, opened.capacity, floor)
        })
    }

    /// Attach a new anchor (see the element ring's
    /// [`attach_shm_anchor`](crate::anchored::RingBuffer::attach_shm_anchor);
    /// the join point is always a record boundary).
    ///
    /// # Safety
    ///
    /// Trust model.
    pub unsafe fn attach_shm_anchor(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored_bytes::BytesAnchor<P, C>> {
        let opened = Self::open(fd, false)?;
        let (backing, joined) = anch_attach_anchor::<P, C>(&opened)?;
        // SAFETY: region validated by open(); the claim choreography just
        // ran (its final store published `joined` into the slot).
        Ok(unsafe {
            crate::anchored_bytes::BytesAnchor::from_shm(backing, opened.capacity, joined)
        })
    }

    /// Attach a new observer over a **read-only** (`PROT_READ`) mapping: no
    /// lease, no table slot, no shared writes, unbounded membership — see
    /// the element ring's
    /// [`attach_shm_observer`](crate::anchored::RingBuffer::attach_shm_observer)
    /// for the full contract (including closed-ring attaches succeeding).
    /// The join point is the unified cursor at attach time — always a
    /// record boundary.
    ///
    /// # Safety
    ///
    /// Trust model.
    pub unsafe fn attach_shm_observer(
        fd: BorrowedFd<'_>,
    ) -> io::Result<crate::anchored_bytes::BytesObserver<P, C>> {
        let opened = Self::open(fd, true)?;
        // SAFETY: region validated by the open; forwarded caller contract.
        Ok(unsafe {
            crate::anchored_bytes::BytesObserver::from_shm(
                opened.region,
                opened.capacity,
                opened.max_anchors,
            )
        })
    }

    /// Retire anchor-table slot `slot` iff still `ACTIVE` at `epoch` (see
    /// the element ring's
    /// [`force_detach_anchor`](crate::anchored::RingBuffer::force_detach_anchor);
    /// `(slot, epoch)` comes from the victim's
    /// [`BytesAnchor::shm_slot_epoch`](crate::anchored_bytes::BytesAnchor::shm_slot_epoch)).
    ///
    /// # Safety
    ///
    /// The caller asserts the holder of `(slot, epoch)` is dead; a live
    /// holder's reads lose all gating protection (revoked read validity).
    pub unsafe fn force_detach_anchor(
        fd: BorrowedFd<'_>,
        slot: usize,
        epoch: u32,
    ) -> io::Result<()> {
        let opened = Self::open(fd, false)?;
        if slot >= opened.max_anchors {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "slot index out of range for this ring's anchor table",
            ));
        }
        force_detach_table_slot(&opened.region, opened.table_offset, slot, epoch)
    }

    /// [`recover_shm`](crate::anchored_bytes::BytesRingBuffer::recover_shm)
    /// with explicit wait strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](crate::anchored_bytes::BytesRingBuffer::recover_shm).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<AnchBytesPair<P, C>> {
        let opened = Self::open(fd, false)?;
        let producer_anchor = anch_attach_producer_anchor::<C>(&opened, true)?;
        opened.region.anch_starving().store(0, Ordering::Release);
        // Heal BEFORE the table reset: the floor and the latest repair are
        // producer-session state, independent of the anchor table.
        let floor = bytes_heal_on_attach(&opened.region);
        let resume = reset_gate_table(
            &opened.region,
            opened.table_offset,
            opened.max_anchors,
            opened.capacity as u64,
        );
        let claim = claim_table_slot(&opened.region, opened.table_offset, opened.max_anchors)
            .expect("freshly reset table has free slots");
        opened
            .region
            .anch_slot_cursor(opened.table_offset, claim.slot)
            .store(slot_guard(resume), Ordering::Release);
        let anchor_backing = Box::new(GateShmConsumer::new(
            Arc::clone(&opened.region),
            claim,
            opened.max_anchors,
            opened.table_offset,
        ));
        // SAFETY: region validated by open(); forwarded caller contract.
        unsafe {
            Ok((
                crate::anchored_bytes::BytesProducer::from_shm(
                    producer_anchor,
                    opened.capacity,
                    floor,
                ),
                crate::anchored_bytes::BytesAnchor::from_shm(
                    anchor_backing,
                    opened.capacity,
                    resume,
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsFd;

    use super::*;

    /// Build a valid element ring in a fresh memfd, then let both handles drop
    /// (releasing their leases). The initialized region persists in the fd.
    fn fresh_region_fd() -> OwnedFd {
        let fd = memfd("rb-seqlock-unit").unwrap();
        // SAFETY: fresh private memfd, cooperating handles only.
        let (_tx, _rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 64) }.unwrap();
        fd
    }

    /// The seqlock guard: a region observed mid-(re)initialization — generation
    /// odd — must be rejected on attach rather than read as a half-written
    /// chimera of old and new header fields.
    #[test]
    fn odd_generation_is_rejected_as_initializing() {
        let fd = fresh_region_fd();

        // Force the seqlock generation odd, as if a writer were mid-init.
        let header = ShmRegion::map(fd.as_fd(), BUFFER_OFFSET).unwrap();
        // SAFETY: OFF_GENERATION is 8-aligned and inside the header mapping;
        // the store lands in the shared pages and outlives this mapping.
        let g = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) };
        g.store(g.load(Ordering::Acquire) | 1, Ordering::Release);
        drop(header);

        // SAFETY: cooperating handle; the region is otherwise valid.
        // `.err()` drops the `Ok` value so we don't require `Consumer: Debug`.
        let err = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) }
            .err()
            .expect("odd generation must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("being initialized"),
            "unexpected error: {err}"
        );
    }

    /// The same region, with its (even) generation intact, attaches fine —
    /// proof the rejection above is the generation guard, not a bad fd.
    #[test]
    fn even_generation_attaches() {
        let fd = fresh_region_fd();
        // SAFETY: cooperating handle; region is valid and idle.
        let rx = unsafe { RingBuffer::<u64>::attach_shm_consumer(fd.as_fd()) };
        assert!(rx.is_ok(), "a valid even-generation ring must attach");
    }
}
