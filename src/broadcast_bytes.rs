//! Single-producer / **multi**-consumer broadcast ring buffer (lossy) for
//! **variable-size** byte messages.
//!
//! Where [`crate::broadcast::RingBuffer`] loss-fully broadcasts items of one
//! fixed type `T`, this ring broadcasts discrete byte messages of differing
//! lengths — serialized structs, wire frames, log records — through one
//! shared byte buffer. Every consumer observes the stream independently, and
//! nobody can slow the producer down: [`BytesProducer::push`] **never blocks
//! and never reads a byte of consumer state**. A consumer that falls a full
//! lap behind *loses* bytes instead of gating the producer, detects the loss
//! with an exact byte count ([`PopError::Lagged`]), and repositions to the
//! start of the most recent record. The lossless alternative — a slow
//! consumer gates the producer — is the separate [`crate::spmc_bytes`]
//! machine; the two are different machines, not one type with a mode flag.
//!
//! # Quick start
//!
//! ```
//! use rust_rb::broadcast_bytes::{BytesRingBuffer, PopError};
//!
//! let (mut tx, mut rx) = BytesRingBuffer::new(64);
//! let mut rx2 = rx.subscribe(); // dynamic membership: never fails
//!
//! tx.push(b"tick");
//! assert_eq!(rx.pop().unwrap(), b"tick");
//! assert_eq!(rx2.pop().unwrap(), b"tick"); // both consumers see the message
//!
//! drop(tx); // producer drop closes the ring
//! assert_eq!(rx.pop(), Err(PopError::Closed));
//! ```
//!
//! # Framing
//!
//! Each message is a *record*: a 4-byte little-endian length header followed
//! by the payload, with the whole record rounded up to an **8-byte**
//! boundary. Records never wrap around the end of the buffer: when one does
//! not fit in the space remaining, the producer writes a *padding* header
//! (`u32::MAX`) there and restarts at offset zero; consumers skip padding
//! transparently. Padding skips are bounds-safe by construction — a capacity
//! multiple is always a record boundary, on every lap.
//!
//! [`max_message_len`](BytesProducer::max_message_len) is **`capacity / 8`**
//! (the Aeron bound), *not* the gating rings' `capacity / 2 - 4`: here the
//! cap is set by loss tolerance, not framing. A consumer validates its copy
//! *after* taking it, and that post-copy check only tolerates the producer
//! advancing up to a full capacity past the record being read — capping
//! records at an eighth of the ring keeps roughly a lap of headroom for the
//! copy to complete, where half-capacity records would leave almost none and
//! turn every slightly-behind reader into a permanent lagger.
//!
//! # The three counters (Agrona), and why validation is out-of-band
//!
//! The element ring validates reads with a per-slot seqlock. **That does not
//! port to variable-size records** [M-F15]: record boundaries shift across
//! laps, so an in-band sequence word can land inside another message's
//! payload — which can then *forge* the expected value. Validation here is
//! therefore **out-of-band**, against three `u64` byte positions, all
//! written only by the producer, each on its own padded cache line
//! (the Agrona `BroadcastTransmitter`/`Receiver` shape):
//!
//! * `tail_intent` — the position the producer is **about to** invalidate up
//!   to, stored *before* it writes a byte: "I will destroy everything below
//!   this".
//! * `tail` — the committed position, stored *after* the record is fully
//!   written. This is the only line consumers spin on.
//! * `latest` — the start of the most recent record: the lap-recovery jump
//!   target (stored before `tail`, so any consumer that sees the new tail
//!   also sees a coherent `latest`).
//!
//! A read at position `pos` is valid iff `tail_intent <= pos + capacity` —
//! the producer's declared write frontier has not reached this cell. The
//! consumer checks that window **before parsing the header** (a torn or
//! garbage length is bounds-checked before any use and never trusted) and
//! **re-checks it after copying the payload out**; only then is the copy
//! accepted.
//!
//! # Loss semantics: reposition jumps to the **latest record**, in **bytes**
//!
//! The element ring repositions a lapped consumer to
//! `tail - capacity + slack` — any slot index is a message boundary there.
//! **Neither the slack knob nor that target ports to bytes**: an arbitrary
//! byte offset is not a record boundary, and parsing from one would read
//! garbage lengths. A lapped byte consumer instead jumps to `latest` — the
//! start of the most recent record, the one boundary the producer
//! guarantees; that jump is also what *repairs* boundary misalignment after
//! a lap. And `missed_bytes` is reported in **bytes** (exactly
//! `new_position - old_position`, headers and padding included), not
//! messages — with variable-size records the message count of an overwritten
//! region is unknowable. Hence this module's own [`PopError`] rather than
//! [`crate::broadcast::PopError`]: reusing the element ring's `missed`
//! *message* count here would lie about units.
//!
//! ```
//! use rust_rb::broadcast_bytes::{BytesRingBuffer, PopError};
//!
//! let (mut tx, mut rx) = BytesRingBuffer::new(64); // max message: 8 bytes
//! for i in 0..10u64 {
//!     tx.push(&i.to_le_bytes()); // 16-byte records; never blocks
//! }
//! // The idle reader was lapped: it jumps to the latest record (byte 144)
//! // and reports the skipped distance in bytes, exactly.
//! assert_eq!(rx.pop(), Err(PopError::Lagged { missed_bytes: 144 }));
//! assert_eq!(rx.pop().unwrap(), 9u64.to_le_bytes());
//! ```
//!
//! Summing `missed_bytes` across errors plus the bytes consumed (each
//! record's framed length, padding included) reproduces the producer's
//! [`tail`](BytesProducer::tail) — accounting is gap-free and overlap-free.
//! For self-contained streams (market data), skip recovery entirely with
//! [`BytesConsumer::skip_to_latest`].
//!
//! # Reads are copies, never borrows
//!
//! The producer overwrites at will, so the lossy bytes ring **cannot hand
//! out zero-copy borrows** into the buffer (unlike the gating
//! [`crate::spmc_bytes`]): every accepted read is a validated copy-out.
//! [`pop_into`](BytesConsumer::pop_into)/[`try_pop_into`](BytesConsumer::try_pop_into)
//! reuse caller storage (no allocation once the scratch buffer is warm — the
//! hot-path form); [`pop`](BytesConsumer::pop)/[`try_pop`](BytesConsumer::try_pop)
//! allocate a fresh `Vec` for convenience.
//!
//! The copies use the crate's strict policy (ADR 0002): the bytes race
//! consumer copies, so plain loads/stores would be UB — **every ring access
//! is a 4-byte atomic on one 4-aligned lane grid**, headers and payload
//! both, on both sides. Uniform lanes matter here: record boundaries shift
//! across laps, so any mixed-size scheme (word stores racing byte loads)
//! would put differently-sized atomics on the same bytes — formally
//! unspecified and rejected by Miri. (The element ring can use full
//! machine-word copies because its slot layout never shifts.) The
//! `read_volatile`/`write_volatile` alternative stays behind the private
//! `rust_rb_volatile_copy` dev cfg for A/B benchmarking — formally racy,
//! never the default.
//!
//! # Membership and the closed contract
//!
//! Consumers are **pure readers**: no registry, no leases, unbounded count.
//! [`BytesProducer::subscribe`]/[`BytesConsumer::subscribe`] read the current
//! tail and start there (never fails); dropping a consumer is a no-op for
//! everyone else; with zero consumers the producer free-runs. Dropping the
//! [`BytesProducer`] closes the ring: [`BytesConsumer::pop`] returns
//! `Err(`[`PopError::Closed`]`)` only once the producer is gone **and** this
//! consumer has drained every published record it can still reach (published
//! records stay readable after producer death — the counters are stable).
//! [`BytesConsumer::try_pop`] returns `Ok(None)` for empty-but-alive.
//!
//! All positions are `u64`: byte cursors at 32 bits would wrap in minutes,
//! and monotonic 64-bit positions make every comparison exact (the module is
//! gated on `target_has_atomic = "64"`).
//!
//! Consumer wait strategies must be [`SelfTimed`] — the producer keeps zero
//! consumer knowledge, so nobody will ever notify a parked reader:
//!
//! ```compile_fail
//! // CvWait is not SelfTimed: the broadcast bytes ring rejects it.
//! use rust_rb::{broadcast_bytes, CvWait};
//! let _ = broadcast_bytes::BytesRingBuffer::<CvWait>::with_wait_strategies(64);
//! ```

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crate::atomic_copy::{copy_in_lanes, copy_out_lanes};
use crate::cache_padded::CachePadded;
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

/// The buffer word type: `u64` so the base is 8-aligned (a `Box<[u8]>`
/// allocation only guarantees alignment 1). All access goes through 4-byte
/// atomic lanes over raw `u8` pointers; the words are never read as `u64`s.
///
/// Zero-initialized on construction so every byte is always initialized —
/// atomic loads over uninitialized memory would be UB, and a lapped consumer
/// legitimately loads bytes the producer never wrote.
type Word = UnsafeCell<u64>;

/// Size of the length header preceding each payload. (`pub(crate)`: shared
/// with `anchored_bytes` — the two lossy byte rings' framing is normatively
/// identical, so the constants and helpers have exactly one definition.)
pub(crate) const HEADER: usize = 4;
/// Record alignment: every record (and padding) start is 8-aligned, so
/// headers are naturally aligned lanes and capacity multiples are record
/// boundaries on every lap. (`pub(crate)`: the shm attach validates the
/// region's tail against it.)
pub(crate) const ALIGN: usize = 8;
/// Header value marking a padding record that runs to the end of the buffer.
pub(crate) const PADDING: u32 = u32::MAX;
/// Smallest legal capacity: one 8-byte record (an empty message).
/// (`pub(crate)`: shared with the shm constructors so they cannot drift.)
pub(crate) const MIN_CAPACITY: usize = 8;

#[inline(always)]
pub(crate) const fn align_up(n: usize) -> usize {
    (n + (ALIGN - 1)) & !(ALIGN - 1)
}

/// Bytes a record with a `len`-byte payload occupies in the ring.
#[inline(always)]
pub(crate) const fn record_len(len: usize) -> usize {
    align_up(HEADER + len)
}

/// The largest payload a single message may carry: `capacity / 8` (the
/// Aeron bound — see the module docs; loss tolerance, not framing, binds),
/// clamped below the `u32` header space where `u32::MAX` marks padding.
#[inline(always)]
pub(crate) const fn max_message_len(capacity: usize) -> usize {
    let cap = capacity / 8;
    let header_space = (PADDING - 1) as usize;
    if cap < header_space {
        cap
    } else {
        header_space
    }
}

/// Round a requested minimum capacity to the ring's real capacity: the next
/// power of two, floor [`MIN_CAPACITY`], via the one crate-wide rounding
/// policy — shared with the shm constructors so heap and shm cannot round
/// the same request differently.
///
/// # Panics
///
/// Panics if `min_capacity == 0` or the rounding overflows `usize`.
fn round_capacity(min_capacity: usize) -> usize {
    crate::cursor::round_capacity(min_capacity, MIN_CAPACITY)
}

/// Error returned by consumer pops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// The producer lapped this consumer: `missed_bytes` bytes of framed
    /// stream (headers and wrap padding included) were overwritten before
    /// they could be read. The consumer has already repositioned to the
    /// start of the most recent record, so the *next* pop reads from there;
    /// accounting is exact and gap-free
    /// (`old_position + missed_bytes == new_position`), so summing
    /// `missed_bytes` across errors plus the framed bytes consumed
    /// reproduces the producer's total. The count is **bytes, not
    /// messages** — with variable-size records the message count of an
    /// overwritten region is unknowable. (A transient `missed_bytes == 0`
    /// is possible when the reposition races the producer on a very small
    /// ring.)
    Lagged {
        /// Framed bytes irrecoverably skipped, exact as of detection.
        missed_bytes: u64,
    },
    /// The ring is **closed and drained**: the producer was dropped and this
    /// consumer has consumed every published record still reachable.
    Closed,
}

impl core::fmt::Display for PopError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PopError::Lagged { missed_bytes } => {
                write!(
                    f,
                    "consumer lagged: {missed_bytes} bytes overwritten unread"
                )
            }
            PopError::Closed => {
                f.write_str("ring closed: producer dropped and all published messages consumed")
            }
        }
    }
}

impl std::error::Error for PopError {}

// -----------------------------------------------------------------------------
// The 4-byte-lane atomic copy (the strict copy policy, uniform-size edition)
// -----------------------------------------------------------------------------

/// Load the `u32` lane at byte offset `off` (`Relaxed`).
///
/// # Safety
///
/// `off` must be 4-aligned and `off + 4 <= capacity`; `base` must be the
/// (8-aligned) live buffer base.
#[inline(always)]
unsafe fn load_lane(base: *const u8, off: usize) -> u32 {
    debug_assert_eq!(off % 4, 0, "ring accesses stay on the 4-byte lane grid");
    // SAFETY: in bounds and 4-aligned per the contract; a shared atomic
    // reference over the `UnsafeCell` storage is the sanctioned way to load
    // while the producer races.
    unsafe { &*(base.add(off).cast::<AtomicU32>()) }.load(Ordering::Relaxed)
}

/// Store `v` into the `u32` lane at byte offset `off` (`Relaxed`).
///
/// # Safety
///
/// As for [`load_lane`], plus the lane must be part of a record the single
/// producer currently owns for writing (consumers may race through atomics).
#[inline(always)]
unsafe fn store_lane(base: *mut u8, off: usize, v: u32) {
    debug_assert_eq!(off % 4, 0, "ring accesses stay on the 4-byte lane grid");
    // SAFETY: as in `load_lane`.
    unsafe { &*(base.add(off).cast::<AtomicU32>()) }.store(v, Ordering::Relaxed);
}

// The bulk payload copies (`copy_in_lanes`/`copy_out_lanes`, atomic lanes;
// volatile under the `rust_rb_volatile_copy` dev cfg) live in
// `crate::atomic_copy`, shared with the composed `crate::anchored_bytes`
// ring. The single-lane header accessors above stay local: this module
// deliberately keeps them atomic even under the volatile A/B switch (the
// composed ring flips its own with everything else).

// -----------------------------------------------------------------------------
// Shared state
// -----------------------------------------------------------------------------

/// The producer-published cache line consumers spin on: the committed tail
/// plus, co-located in the same padded slot, the `closed` flag (written once
/// by `BytesProducer::drop`, read only on consumer would-block paths — the
/// line consumers already poll).
///
/// `closed` is a whole word (0 = open, nonzero = closed), not a bool, so the
/// shm header can host the very same field at a fixed offset with one atomic
/// type on both backings.
struct TailSide {
    tail: AtomicU64,
    closed: AtomicU64,
}

/// The state all handles share, kept alive by an `Arc`. The three counters
/// are each `CachePadded`: `tail_intent` and `latest` are stored on every
/// push but only *loaded* by consumers (twice per pop, no spinning), while
/// `tail` is the one line consumers spin on.
struct Shared {
    buf: Box<[Word]>,
    /// `capacity - 1`, in the u64 domain of all position arithmetic.
    mask: u64,
    /// Byte position the producer is about to invalidate up to (stored
    /// before any byte of a push is written).
    tail_intent: CachePadded<AtomicU64>,
    /// Byte position of the start of the most recent record — the
    /// lap-recovery jump target (stored before `tail`).
    latest: CachePadded<AtomicU64>,
    tail_side: CachePadded<TailSide>,
}

// SAFETY: buffer bytes are written only by the single producer under the
// three-counter protocol; consumers copy bytes out with atomic lane loads
// and expose them only after out-of-band validation. All other shared state
// is atomics.
unsafe impl Sync for Shared {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl Send for Shared {}

/// Builder/namespace for constructing a lossy broadcast **bytes** ring.
///
/// [`new`](Self::new) takes the minimum capacity in **bytes** at runtime
/// (rounded up to the next power of two, minimum 8) and uses [`YieldWait`]
/// consumers; pick another consumer [`WaitStrategy`] with
/// [`with_wait_strategies`](Self::with_wait_strategies).
///
/// There is **no producer-side strategy parameter** (the producer
/// structurally cannot wait) and — unlike [`crate::broadcast::RingBuffer`] —
/// **no reposition slack knob**: a byte ring can only reposition to a record
/// boundary, and the one guaranteed boundary is the latest record (see the
/// module docs). Consumer strategies must be [`SelfTimed`]: the producer
/// keeps zero consumer knowledge, so a notify-dependent strategy could park
/// forever.
pub struct BytesRingBuffer<C = YieldWait>(core::marker::PhantomData<C>);

impl BytesRingBuffer {
    /// Create a ring with the default consumer wait strategy and return its
    /// producer and one initial consumer (subscribe more from either
    /// handle).
    ///
    /// The real capacity is `min_capacity` rounded up to the next power of
    /// two, minimum 8.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new(min_capacity: usize) -> (BytesProducer, BytesConsumer) {
        BytesRingBuffer::<YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<C: SelfTimed + Send> BytesRingBuffer<C> {
    /// Create a ring with an explicit consumer wait strategy and return its
    /// producer and one initial consumer.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (BytesProducer, BytesConsumer<C>) {
        let capacity = round_capacity(min_capacity);
        let mut words = Vec::with_capacity(capacity / ALIGN);
        words.resize_with(capacity / ALIGN, || UnsafeCell::new(0u64));

        let shared = Arc::new(Shared {
            buf: words.into_boxed_slice(),
            mask: capacity as u64 - 1,
            tail_intent: CachePadded::new(AtomicU64::new(0)),
            latest: CachePadded::new(AtomicU64::new(0)),
            tail_side: CachePadded::new(TailSide {
                tail: AtomicU64::new(0),
                closed: AtomicU64::new(0),
            }),
        });

        let consumer = subscribe_from(&shared);
        let tail_intent = NonNull::from(&*shared.tail_intent);
        let latest = NonNull::from(&*shared.latest);
        let tail = NonNull::from(&shared.tail_side.tail);
        let closed = NonNull::from(&shared.tail_side.closed);
        let producer = BytesProducer {
            base: base_of(&shared),
            mask: shared.mask,
            next: 0,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            intent_floor: 0,
            tail_intent,
            latest,
            tail,
            closed,
            anchor: ProducerAnchor::Heap(shared),
        };
        (producer, consumer)
    }
}

/// Base of the byte buffer, derived from the whole-slice `as_ptr` (not a
/// first-element reference) so it keeps provenance over every word.
fn base_of(shared: &Arc<Shared>) -> NonNull<u8> {
    NonNull::new(shared.buf.as_ptr().cast_mut().cast::<u8>()).expect("buffer is non-null")
}

/// Register a new consumer: read the tail and start there. Trivially
/// dynamic — a consumer is pure reader state, so there is no registry to
/// claim, nothing that can fail, and no bound on the count.
fn subscribe_from<C: SelfTimed>(shared: &Arc<Shared>) -> BytesConsumer<C> {
    let shared = Arc::clone(shared);
    // The join point: only records published after this tail are seen — and
    // the tail is always a record boundary. A consumer subscribed to a
    // closed ring is born drained and pops Closed.
    let pos = shared.tail_side.tail.load(Ordering::Acquire);
    let tail_intent = NonNull::from(&*shared.tail_intent);
    let latest = NonNull::from(&*shared.latest);
    let tail = NonNull::from(&shared.tail_side.tail);
    let closed = NonNull::from(&shared.tail_side.closed);
    BytesConsumer {
        base: base_of(&shared),
        mask: shared.mask,
        pos,
        tail_cache: pos,
        wait: C::default(),
        tail_intent,
        latest,
        tail,
        closed,
        anchor: ConsumerAnchor::Heap(shared),
    }
}

/// Where the producing handle's shared state lives — the anchor seam,
/// mirroring `crate::broadcast` (and `crate::spmc`'s registry seam): every
/// hot atomic is reached through the handle's cached raw pointers (identical
/// for both variants); the anchor is consulted only on cold paths
/// (subscribe, teardown).
enum ProducerAnchor {
    /// In-process ring: the shared state lives on the heap in an `Arc`.
    Heap(Arc<Shared>),
    /// Cross-process ring: the state lives in a mapped shared region; the
    /// anchor holds the producer role lease. Boxed so enabling the feature
    /// does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::BcastProducerAnchor>),
}

impl ProducerAnchor {
    /// Whether teardown may write the ring-wide closed word (see
    /// `crate::broadcast`'s twin for the rationale).
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ProducerAnchor::Heap(_) => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => anchor.owned_by_current_process() && anchor.owns_lease(),
        }
    }
}

/// The consuming handle's side of the anchor seam: purely a keep-alive. A
/// lossy consumer is a pure reader — dropping it releases no lease and
/// writes nothing, so the shm variant is nothing but the mapping `Arc`
/// (mapped **read-only**: any accidental store in the consumer path would be
/// a deterministic SIGSEGV, which is the enforcement).
enum ConsumerAnchor {
    Heap(Arc<Shared>),
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Arc<crate::shm::ShmRegion>),
}

/// The producing half of a [`BytesRingBuffer`]. `Send` but not `Clone`:
/// exactly one producer, enforced by the type system.
///
/// The producer **never blocks and never reads consumer state** — hence no
/// `try_push` (a push cannot fail), no `len`/`is_full`/`is_empty` (the ring
/// is never full from the producer's point of view, and "length" is
/// per-consumer: see [`BytesConsumer::lag`]), and no wait-strategy
/// parameter.
///
/// Dropping the producer **closes** the ring: consumers drain what was
/// published and then see [`PopError::Closed`]. The close is a flag store
/// with no notify — consumer strategies are [`SelfTimed`] by construction,
/// so a parked reader re-checks and wakes itself.
pub struct BytesProducer {
    /// Base of the byte buffer (cached; stable for the anchor's lifetime).
    base: NonNull<u8>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next byte position to write (producer-private; equals the published
    /// tail between pushes).
    next: u64,
    /// The shm intent floor: a recovered producer must never declare an
    /// intent below a dead predecessor's frontier — the bytes the
    /// predecessor destroyed mid-push sit just under that frontier, and a
    /// regressed declaration would re-admit them to the consumers' window
    /// check. Heap rings (and shm rings with no crash to heal) carry 0, so
    /// the push-path `max` folds to a no-op.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    intent_floor: u64,
    /// The three counters plus the closed word (cached raw pointers; heap:
    /// into the `Arc`, shm: into the mapped region — the hot push path is
    /// identical).
    tail_intent: NonNull<AtomicU64>,
    latest: NonNull<AtomicU64>,
    tail: NonNull<AtomicU64>,
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc` or shm mapping + lease).
    anchor: ProducerAnchor,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive.
unsafe impl Send for BytesProducer {}

impl Drop for BytesProducer {
    fn drop(&mut self) {
        // Flag only, no notify: consumers use SelfTimed strategies (enforced
        // at construction), whose waits re-check the closed flag without a
        // peer wake — the producer keeps zero consumer knowledge even at
        // teardown. Guarded for shm (heap: constant true): only a graceful
        // drop by the live lease holder closes the ring — a fork-inherited
        // copy or superseded zombie must not end the successor's session.
        if self.anchor.teardown_allowed() {
            // SAFETY: `closed` points into shared state the anchor keeps
            // alive.
            unsafe { self.closed.as_ref() }.store(1, Ordering::Release);
        }
    }
}

impl BytesProducer {
    /// Enqueue a copy of `msg`. **Never blocks, never fails, never reads
    /// consumer state** — a consumer that has not kept up is lapped and will
    /// observe [`PopError::Lagged`]. Empty messages are legal (an 8-byte
    /// record carrying zero payload bytes), mirroring the other byte rings.
    ///
    /// The write follows the three-counter protocol: declare intent, write
    /// the record with atomic lane stores, publish `latest` then `tail` —
    /// so a racing reader either takes a validated complete copy or detects
    /// the lap. See the module docs.
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()` — an over-limit
    /// message would erode the loss-tolerance window the validation
    /// protocol depends on.
    #[inline]
    pub fn push(&mut self, msg: &[u8]) {
        let len = msg.len();
        let capacity = self.mask + 1;
        let max = max_message_len(capacity as usize);
        assert!(
            len <= max,
            "message length {len} exceeds max_message_len ({max})"
        );
        let record = record_len(len) as u64;
        let off = self.next & self.mask;
        let to_end = capacity - off;
        // Pad to the wrap boundary when the record will not fit before the
        // buffer end (records never wrap).
        let (pad, total) = if record <= to_end {
            (0, record)
        } else {
            (to_end, to_end + record)
        };
        let new_tail = self.next + total;

        // The shm crash-recovery floor (see the field docs): declarations
        // are monotonic across producer sessions. Heap: floor is 0 and the
        // `max` folds away; compiled out entirely without the `shm` feature.
        #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
        let declared = new_tail.max(self.intent_floor);
        #[cfg(not(all(feature = "shm", target_os = "linux", target_has_atomic = "64")))]
        let declared = new_tail;

        // 1. Declare intent: "I will destroy everything below new_tail".
        //    Readers of any byte this push will touch now fail their window
        //    check.
        // SAFETY: `tail_intent` points into shared state the anchor keeps
        // alive.
        unsafe { self.tail_intent.as_ref() }.store(declared, Ordering::Release);
        // 2. The load-bearing fence: the byte stores below must not be
        //    hoisted above the intent store (and pair with the readers'
        //    post-copy acquire fence).
        fence(Ordering::Release);

        let base = self.base.as_ptr();
        let rec_off = ((self.next + pad) & self.mask) as usize;
        // SAFETY (whole block): offsets are `& mask`, so in bounds; record
        // and padding starts are 8-aligned (records are ALIGN multiples from
        // an 8-aligned base), so every header is an aligned lane; the record
        // fits contiguously (`record <= capacity - rec_off` by the pad
        // computation, and `record <= capacity` because
        // `max_message_len <= capacity / 8`); readers race only through
        // atomics.
        unsafe {
            if pad > 0 {
                // `frame` only pads mid-buffer, where at least one lane
                // remains before the end. (PADDING is all-ones:
                // endian-proof.)
                store_lane(base, off as usize, PADDING);
            }
            // Headers are little-endian on every target, matching the other
            // byte rings (free on LE machines; a byte swap on BE ones).
            store_lane(base, rec_off, (len as u32).to_le());
            copy_in_lanes(msg.as_ptr(), base.add(rec_off + HEADER), len);
        }

        // 3. Publish the jump target, then the frontier. `latest` first, so
        //    any consumer that sees the new tail also sees a coherent
        //    latest; `tail` is the only line consumers spin on.
        // SAFETY: `latest`/`tail` point into shared state the anchor keeps
        // alive.
        unsafe { self.latest.as_ref() }.store(self.next + pad, Ordering::Release);
        unsafe { self.tail.as_ref() }.store(new_tail, Ordering::Release);
        self.next = new_tail;
    }

    /// Subscribe a new consumer with wait strategy `C`; its join point is
    /// the current tail — it sees only records published after this call.
    /// Never fails: consumers are unbounded pure readers.
    ///
    /// `C` cannot be inferred from `self` (the producer carries no consumer
    /// strategy — by design); name it, e.g.
    /// `tx.subscribe::<rust_rb::YieldWait>()`, or subscribe from an existing
    /// consumer.
    ///
    /// On a shared-memory ring the subscriber shares this producer's
    /// **read-write** mapping (same process, same pages) — the read-only
    /// enforcement story belongs to
    /// [`attach_shm_consumer`](BytesRingBuffer::attach_shm_consumer), which
    /// maps the region afresh with `PROT_READ`.
    pub fn subscribe<C: SelfTimed + Send>(&self) -> BytesConsumer<C> {
        match &self.anchor {
            ProducerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this capacity
            // when this handle was constructed.
            ProducerAnchor::Shm(anchor) => unsafe {
                BytesConsumer::from_shm(Arc::clone(anchor.region()), (self.mask + 1) as usize)
            },
        }
    }

    /// Total framed bytes published so far (headers and wrap padding
    /// included) — the ring's frontier. Producer-local and exact.
    #[inline]
    pub fn tail(&self) -> u64 {
        self.next
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 8` (see
    /// the module docs for why this differs from the gating rings'
    /// `capacity / 2 - 4`).
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }
}

/// A consuming handle of a [`BytesRingBuffer`]: a **pure reader** — private
/// byte position, private tail cache, its own wait strategy instance, and
/// nothing the producer (or any other consumer) ever looks at. `Send` but
/// not `Clone`; create more consumers with [`subscribe`](Self::subscribe).
///
/// Dropping a consumer is a no-op for everyone else: there is no registry
/// slot to release and nobody gates on this reader.
pub struct BytesConsumer<C: WaitStrategy = YieldWait> {
    /// Base of the byte buffer (cached; stable for the anchor's lifetime).
    base: NonNull<u8>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next byte position to read — always a record boundary (join at tail,
    /// advance by whole records, reposition to `latest`: all boundaries).
    pos: u64,
    /// Cached snapshot of the producer's tail.
    tail_cache: u64,
    /// This consumer's own wait strategy instance ([`SelfTimed`] by
    /// construction — waiting is purely local, no notify ever arrives).
    wait: C,
    /// The three counters plus the closed word (cached raw pointers;
    /// **loads only** — the whole consumer path is write-free, which is what
    /// lets the shm variant hold a read-only mapping).
    tail_intent: NonNull<AtomicU64>,
    latest: NonNull<AtomicU64>,
    tail: NonNull<AtomicU64>,
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc` or shm mapping).
    anchor: ConsumerAnchor,
}

// SAFETY: the consumer only touches consumer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive.
unsafe impl<C: WaitStrategy + Send> Send for BytesConsumer<C> {}

impl<C: WaitStrategy> BytesConsumer<C> {
    /// Block until a message is available, then dequeue it into a fresh
    /// `Vec` by validated copy — the allocating convenience form of
    /// [`pop_into`](Self::pop_into).
    ///
    /// Returns `Err(`[`PopError::Lagged`]`)` if the producer lapped this
    /// consumer (the position has already been repositioned to the latest
    /// record — the next pop proceeds from there), or
    /// `Err(`[`PopError::Closed`]`)` once the producer is dropped **and**
    /// everything reachable has been drained.
    #[inline]
    pub fn pop(&mut self) -> Result<Vec<u8>, PopError> {
        let mut out = Vec::new();
        self.pop_into(&mut out)?;
        Ok(out)
    }

    /// Block until a message is available, then dequeue it into `out` by
    /// validated copy: `out` is cleared and filled with exactly the payload
    /// bytes — the hot-path form; no allocation once `out`'s capacity has
    /// warmed up to the stream's message sizes. On `Err`, `out`'s contents
    /// are unspecified (empty in the current implementation).
    ///
    /// Errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn pop_into(&mut self, out: &mut Vec<u8>) -> Result<(), PopError> {
        // Clear at entry, not only in the accept path: the doc promises
        // "cleared" (and empty-on-`Err`), but a reposition can error out of
        // `read_record` before its own clear runs, which would leave a
        // previous pop's payload in `out`.
        out.clear();
        self.wait_for_item()?;
        self.read_record(out)
    }

    /// Dequeue a message into a fresh `Vec` without blocking. `Ok(None)`
    /// means empty-but-alive; the errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<Vec<u8>>, PopError> {
        let mut out = Vec::new();
        Ok(self.try_pop_into(&mut out)?.then_some(out))
    }

    /// Dequeue a message into `out` without blocking: `Ok(true)` and `out`
    /// cleared-and-filled on success, `Ok(false)` (with `out` untouched) for
    /// empty-but-alive; the errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn try_pop_into(&mut self, out: &mut Vec<u8>) -> Result<bool, PopError> {
        if self.has_item() {
            return self.read_record(out).map(|()| true);
        }
        self.check_closed()?;
        if self.available() {
            // The close re-check refreshed the tail and found a final record
            // published just before the producer dropped.
            return self.read_record(out).map(|()| true);
        }
        Ok(false)
    }

    /// Jump this consumer to the current tail, abandoning everything
    /// published but unread. Returns how many framed **bytes** were skipped.
    ///
    /// The explicit market-data recovery: after a lap (or on demand), start
    /// from the freshest record instead of salvaging the retained window.
    #[inline]
    pub fn skip_to_latest(&mut self) -> u64 {
        let tail = self.refresh();
        let skipped = tail.saturating_sub(self.pos);
        // Never move backwards: a reposition can transiently put `pos` ahead
        // of a stale tail observation.
        self.pos = self.pos.max(tail);
        skipped
    }

    /// How far this consumer trails the producer, in framed **bytes**
    /// (headers and wrap padding included): `tail - position` per a fresh
    /// tail read (saturating). `0` means fully caught up; a lag of
    /// `capacity` or more means the next pop will report
    /// [`PopError::Lagged`].
    #[inline]
    pub fn lag(&self) -> u64 {
        // SAFETY: `tail` points into shared state the anchor keeps alive.
        unsafe { self.tail.as_ref() }
            .load(Ordering::Acquire)
            .saturating_sub(self.pos)
    }

    /// Subscribe a further consumer with the same wait strategy; its join
    /// point is the current tail. Never fails (see
    /// [`BytesProducer::subscribe`]). On a shared-memory ring the sibling
    /// shares this consumer's mapping (read-only if this one attached
    /// read-only).
    pub fn subscribe(&self) -> BytesConsumer<C>
    where
        C: SelfTimed + Send,
    {
        match &self.anchor {
            ConsumerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the region was validated for this capacity when this
            // handle was constructed.
            ConsumerAnchor::Shm(region) => unsafe {
                BytesConsumer::from_shm(Arc::clone(region), (self.mask + 1) as usize)
            },
        }
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 8`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }

    /// Whether the cached tail shows an available record.
    #[inline(always)]
    fn available(&self) -> bool {
        self.tail_cache > self.pos
    }

    /// Unconditionally reload the cached tail (`Acquire`) and return it.
    #[inline(always)]
    fn refresh(&mut self) -> u64 {
        // SAFETY: `tail` points into shared state the anchor keeps alive.
        self.tail_cache = unsafe { self.tail.as_ref() }.load(Ordering::Acquire);
        self.tail_cache
    }

    /// Check for at least one available record, reloading the tail at most
    /// once.
    #[inline(always)]
    fn has_item(&mut self) -> bool {
        if !self.available() {
            self.refresh();
            return self.available();
        }
        true
    }

    /// The would-block close check: if the producer is gone, re-read the
    /// tail once more (the `Acquire` load of `closed` synchronizes with the
    /// producer's `Release` store, which follows its final publish) and
    /// report [`PopError::Closed`] only if genuinely drained.
    #[inline]
    fn check_closed(&mut self) -> Result<(), PopError> {
        // SAFETY: `closed` points into shared state the anchor keeps alive.
        if unsafe { self.closed.as_ref() }.load(Ordering::Acquire) != 0 {
            self.refresh();
            // Drained is `<=`, not `==`: a header poke (or a producer crash
            // observed through stale counters) can transiently leave `pos`
            // *ahead* of the committed tail, and an equality check would
            // then never report `Closed` — a drain livelock on a dead ring.
            if self.tail_cache <= self.pos {
                return Err(PopError::Closed);
            }
        }
        Ok(())
    }

    /// Spin/park (per this consumer's wait strategy) until a record is
    /// available or the ring is closed and drained. The wait spins on the
    /// shared **tail** (one line, stored once per push), never on the
    /// intent/latest lines; its predicate also checks `closed`.
    #[inline(always)]
    fn wait_for_item(&mut self) -> Result<(), PopError> {
        loop {
            if self.has_item() {
                return Ok(());
            }
            self.check_closed()?;
            if self.available() {
                return Ok(());
            }
            // SAFETY: the pointers reference shared state the anchor keeps
            // alive.
            let (tail, closed) = unsafe { (self.tail.as_ref(), self.closed.as_ref()) };
            let pos = self.pos;
            self.wait
                .wait(|| tail.load(Ordering::Acquire) > pos || closed.load(Ordering::Acquire) != 0);
        }
    }

    /// The validated read at the current position (the caller established
    /// `tail > pos`): window-check, parse, copy, re-check — or detect the
    /// lap and reposition. The out-of-band protocol; see the module docs.
    #[inline]
    fn read_record(&mut self, out: &mut Vec<u8>) -> Result<(), PopError> {
        let capacity = self.mask + 1;
        let base = self.base.as_ptr();
        // The frame anchor: every window check and any reposition are
        // measured from here. A padding skip advances only the local
        // `cursor` — the position commits (pad and record together) on
        // accept, so a lap detected mid-frame reports the pad bytes inside
        // `missed_bytes` instead of silently dropping them from the
        // accounting.
        let pos = self.pos;
        debug_assert!(self.tail_cache > pos, "caller must confirm availability");
        debug_assert_eq!(pos % ALIGN as u64, 0, "positions stay record-aligned");
        let mut cursor = pos;
        loop {
            // Pre-validate the window BEFORE parsing anything: the frame at
            // `pos` is untouched iff the producer's declared write frontier
            // has not reached its cells (checking against the frame anchor
            // is the strict end of the window — it also covers any padding
            // lane already read). The Acquire keeps the header load below
            // from hoisting above the check.
            // SAFETY: `tail_intent` points into shared state the anchor
            // keeps alive.
            let intent = unsafe { self.tail_intent.as_ref() }.load(Ordering::Acquire);
            if intent.wrapping_sub(pos) > capacity {
                return Err(self.reposition());
            }
            let off = (cursor & self.mask) as usize;
            // SAFETY: `off` is 8-aligned and in bounds; header lanes race
            // the producer only through atomics.
            let header = u32::from_le(unsafe { load_lane(base, off) });
            if header == PADDING {
                // Re-validate out-of-band before trusting the skip: the
                // marker itself may be a racing overwrite. The fence orders
                // the header load before the re-load (fence + relaxed
                // re-load is the sound shape).
                fence(Ordering::Acquire);
                // SAFETY: as for the pre-validation load above.
                let intent = unsafe { self.tail_intent.as_ref() }.load(Ordering::Relaxed);
                if intent.wrapping_sub(pos) > capacity {
                    return Err(self.reposition());
                }
                // Genuine padding. Skip to the wrap boundary — a record
                // boundary on every lap, so this is bounds-safe by
                // construction. `tail_cache > pos` still covers the record
                // there: the padding's own push committed the record that
                // follows it (one tail store covers both), and tail values
                // below that never exceed the padding's start. Genuine
                // padding is never followed by padding (offset zero always
                // fits a record), so this branch runs at most once per
                // frame.
                cursor += capacity - off as u64;
                continue;
            }
            let len = header as usize;
            // Bounds-check the length before ANY use: a torn/garbage header
            // must never cause an out-of-bounds access. A genuine record
            // always passes both checks, so a failure proves a lap — do not
            // trust the value, reposition.
            if len > max_message_len(capacity as usize) {
                return Err(self.reposition());
            }
            let record = record_len(len) as u64;
            if record > capacity - off as u64 {
                return Err(self.reposition());
            }
            // Copy the payload out. The bytes may be torn — they stay
            // unexposed (raw spare capacity, length not yet set) until the
            // post-copy window check accepts them.
            out.clear();
            out.reserve(len);
            // SAFETY: `[off + HEADER, off + record)` is in bounds (the
            // fits-check above) and lane-aligned; `out` has `len` writable
            // bytes of spare capacity.
            unsafe { copy_out_lanes(base.add(off + HEADER), out.as_mut_ptr(), len) };
            // Re-validate after the copy: order every lane load (header
            // included) before the re-load, then check the window again.
            // If any load raced a newer push, that push declared
            // `intent > pos + capacity` before writing a byte, and the
            // fence pairing guarantees we see it here.
            fence(Ordering::Acquire);
            // SAFETY: as for the pre-validation load above.
            let intent = unsafe { self.tail_intent.as_ref() }.load(Ordering::Relaxed);
            if intent.wrapping_sub(pos) > capacity {
                return Err(self.reposition());
            }
            // SAFETY: `copy_out_lanes` initialized exactly `len` bytes.
            unsafe { out.set_len(len) };
            // Accept: commit pad-skipped and record together.
            self.pos = cursor + record;
            return Ok(());
        }
    }

    /// Lap recovery: jump to `latest` — the start of the most recent record,
    /// the one boundary the producer guarantees — per fresh counter reads,
    /// and return the exact number of framed bytes skipped. Never moves
    /// backwards (a stale observation clamps to the current position).
    ///
    /// This is where the byte ring diverges from the element ring's
    /// slack-based `tail - capacity + slack` target: an arbitrary byte
    /// offset is not a record boundary (see the module docs).
    #[cold]
    fn reposition(&mut self) -> PopError {
        // Refresh the tail first (the next pop's availability check), then
        // take the jump target.
        self.refresh();
        // SAFETY: `latest` points into shared state the anchor keeps alive.
        let latest = unsafe { self.latest.as_ref() }.load(Ordering::Acquire);
        // `latest` may transiently exceed the tail loaded above — benignly
        // (a fresh push's `latest` landed between the two loads; `latest` is
        // stored first) or terminally (a producer died between its `latest`
        // and `tail` stores). Do NOT clamp the jump to the refreshed tail:
        // a tail value is a frame *end*, which is a record start only when
        // the next frame carries no wrap padding — clamping would land
        // repositions off record starts under the benign race. Landing
        // ahead of the tail is safe as-is: the availability check holds
        // every read back until a tail actually covers the position, and
        // the `check_closed` drained test is `<=`, so a position stranded
        // past a dead producer's final tail still terminates the drain
        // instead of livelocking (a crashed shm producer's counters are
        // additionally healed at the next recovery attach).
        let new_pos = latest.max(self.pos);
        let missed_bytes = new_pos - self.pos;
        self.pos = new_pos;
        PopError::Lagged { missed_bytes }
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). The handles built here are the ordinary `BytesProducer`/
// `BytesConsumer` types over region pointers — the hot paths are
// byte-identical to the heap ring's; only the anchor differs (plus the
// producer's crash-recovery intent floor). Consumers hold a READ-ONLY
// mapping: the entire consumer path is loads plus private state, so an
// accidental store regression is a deterministic SIGSEGV.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl BytesProducer {
    /// Build a producer over a validated shm region. Seeds `next` from the
    /// live tail (publishing resumes exactly after the last committed
    /// record; a predecessor's partial record was never covered by a tail
    /// store, so its space is reused) and takes the caller-computed
    /// `intent_floor` — `max(tail_intent, tail)` as sampled by the attach,
    /// which keeps declared frontiers monotonic across producer sessions.
    ///
    /// # Safety
    ///
    /// The anchor's region must be a validated broadcast byte ring of
    /// exactly this `capacity` (`create`/`open` in `crate::shm`), the anchor
    /// must hold the producer lease, and `intent_floor` must be at least the
    /// region's current `tail_intent`.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::BcastProducerAnchor>,
        capacity: usize,
        intent_floor: u64,
    ) -> Self {
        let region = anchor.region();
        let tail_intent = NonNull::from(region.bcast_intent());
        let latest = NonNull::from(region.bcast_latest());
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let base = region.bcast_bytes_buffer();
        // SAFETY: `tail` references the live mapping (per contract).
        let next = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        BytesProducer {
            base,
            mask: capacity as u64 - 1,
            next,
            intent_floor,
            tail_intent,
            latest,
            tail,
            closed,
            anchor: ProducerAnchor::Shm(anchor),
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<C: WaitStrategy> BytesConsumer<C> {
    /// Build a consumer over a (typically read-only) mapping of a validated
    /// shm region: pure reader state — no lease, no registration, nothing
    /// written, ever. The join point is the tail at this call (always a
    /// record boundary).
    ///
    /// # Safety
    ///
    /// The region must be a validated broadcast byte ring of exactly this
    /// `capacity`.
    pub(crate) unsafe fn from_shm(region: Arc<crate::shm::ShmRegion>, capacity: usize) -> Self {
        let tail_intent = NonNull::from(region.bcast_intent());
        let latest = NonNull::from(region.bcast_latest());
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let base = region.bcast_bytes_buffer();
        // SAFETY: `tail` references the live mapping (per contract).
        let pos = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        BytesConsumer {
            base,
            mask: capacity as u64 - 1,
            pos,
            tail_cache: pos,
            wait: C::default(),
            tail_intent,
            latest,
            tail,
            closed,
            anchor: ConsumerAnchor::Shm(region),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_len_is_8_aligned_header_inclusive() {
        assert_eq!(record_len(0), 8);
        assert_eq!(record_len(1), 8);
        assert_eq!(record_len(4), 8);
        assert_eq!(record_len(5), 16);
        assert_eq!(record_len(8), 16);
        assert_eq!(record_len(12), 16);
        assert_eq!(record_len(13), 24);
    }

    #[test]
    fn max_message_len_is_capacity_over_8() {
        assert_eq!(max_message_len(8), 1);
        assert_eq!(max_message_len(64), 8);
        assert_eq!(max_message_len(4096), 512);
        // A max-size record always fits within the capacity.
        for cap in [8usize, 16, 64, 4096] {
            assert!(record_len(max_message_len(cap)) <= cap);
        }
    }

    #[test]
    fn round_capacity_policy() {
        assert_eq!(round_capacity(1), MIN_CAPACITY);
        assert_eq!(round_capacity(8), 8);
        assert_eq!(round_capacity(9), 16);
        assert_eq!(round_capacity(4000), 4096);
    }

    #[test]
    fn lane_copy_round_trips_every_phase() {
        // A private 8-aligned scratch ring; copy in then out at every
        // length 0..=9 to cover full-lane and partial-lane tails.
        let scratch: Vec<UnsafeCell<u64>> = (0..4).map(|_| UnsafeCell::new(0)).collect();
        let base = scratch.as_ptr().cast::<u8>().cast_mut();
        for len in 0..=9usize {
            let src: Vec<u8> = (0..len).map(|i| i as u8 + 100).collect();
            let mut dst = vec![0u8; len];
            // SAFETY: the scratch buffer is 8-aligned, 32 bytes, private to
            // this test; offset 4 mirrors the payload phase in the ring.
            unsafe {
                copy_in_lanes(src.as_ptr(), base.add(4), len);
                copy_out_lanes(base.add(4), dst.as_mut_ptr(), len);
            }
            assert_eq!(src, dst, "len {len}");
        }
    }
}
