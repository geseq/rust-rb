//! Single-producer ring with **required anchors and lossy observers** for
//! **variable-size byte messages** — the composed byte machine:
//! [`crate::spmc_bytes`]'s byte-granularity gating registry wrapped around
//! [`crate::broadcast_bytes`]'s Agrona three-counter protocol, on one buffer
//! with one unified cursor.
//!
//! Two consumer roles share the stream:
//!
//! * [`BytesAnchor`] — a **required** consumer with the full spmc_bytes
//!   surface: zero-copy [`Msg`] borrows (`&[u8]` into the ring),
//!   [`drain`](BytesAnchor::drain), `Result<_, `[`Closed`]`>`. The producer
//!   min-gates on the anchors' published byte cursors, so an anchor **never
//!   loses a message** — and a stalled anchor eventually blocks the
//!   producer. Membership is dynamic through the spmc chunk registry.
//! * [`BytesObserver`] — an unbounded **lossy** pure reader with the
//!   broadcast_bytes surface: window-validated copy-out
//!   ([`pop`](BytesObserver::pop)/[`pop_into`](BytesObserver::pop_into)),
//!   exact byte-count [`Lagged`](PopError::Lagged) on a lap with a
//!   reposition to the latest record,
//!   [`skip_to_latest`](BytesObserver::skip_to_latest). Observers never gate
//!   anybody and cost the producer nothing.
//!
//! "At least one consumer must have read" is one `BytesAnchor`. With
//! **zero** anchors the ring degenerates to a pure lossy broadcast: the
//! producer free-runs and observers take losses; the gate's own-cursor
//! default forces a registry rescan at least once per lap of bytes, so a
//! joining anchor is noticed in time and, from its join point (always a
//! record boundary) on, sees every message — the §9.6 free-run join
//! induction lifted to bytes (see `docs/design/spmc.md`).
//!
//! # Quick start
//!
//! ```
//! use rust_rb::anchored_bytes::{BytesRingBuffer, Closed, PopError};
//!
//! let (mut tx, mut anchor) = BytesRingBuffer::new(64);
//! let mut observer = tx.subscribe_observer();
//!
//! tx.push(b"tick");
//! assert_eq!(&*anchor.pop().unwrap(), b"tick"); // lossless, gate-protected borrow
//! assert_eq!(observer.pop().unwrap(), b"tick"); // lossy, validated copy
//!
//! drop(tx); // producer drop closes the ring
//! assert!(matches!(anchor.pop(), Err(Closed)));
//! assert_eq!(observer.pop(), Err(PopError::Closed));
//! ```
//!
//! # Framing
//!
//! One framing serves both roles, and it is [`crate::broadcast_bytes`]'s:
//! each message is a *record* — a 4-byte little-endian length header
//! followed by the payload, the whole record rounded up to an **8-byte**
//! boundary. Records never wrap: when one does not fit before the buffer
//! end, the producer writes a *padding* header (`u32::MAX`) there and
//! restarts at offset zero; both roles skip padding transparently.
//!
//! [`max_message_len`](BytesProducer::max_message_len) is **`capacity / 8`**
//! — the observers' window tolerance binds. Anchors alone would allow the
//! gating rings' `capacity / 2 - 4` (framing is the only constraint when
//! nothing is ever overwritten unread), but an observer validates its copy
//! *after* taking it against a window only one capacity wide; half-capacity
//! records would leave a slightly-behind observer almost no headroom and
//! turn it into a permanent lagger. The ring must satisfy both roles, so the
//! tighter Aeron bound wins. A pleasant side effect: the anchors'
//! lag-filtered starving release (below) is *meaningfully* selective even
//! in the worst case — the flagged span never exceeds a quarter of the
//! ring, where spmc_bytes' worst-case bound approaches the whole capacity.
//!
//! The capacity floor is **16 bytes** (not the other byte rings' 8): with
//! 8-aligned records, an 8-byte ring's only possible frame occupies the
//! whole capacity, which the audience-less gating default (own cursor minus
//! one — normative, see below) could never grant; the zero-anchor free-run
//! would deadlock.
//!
//! # The write order is normative [audit F3]
//!
//! Per push, in exactly this order:
//!
//! 1. **Gate** — wait until every anchor has published its way past
//!    `new_tail - capacity`;
//! 2. declare **`tail_intent`** (`new_tail`);
//! 3. **`fence(Release)`**;
//! 4. **write the lanes** (padding marker, header, payload — uniform
//!    4-byte atomic lanes);
//! 5. publish **`latest`, then `tail`** (the unified cursor, strictly
//!    last).
//!
//! The intent declaration must NEVER precede the gate wait: a producer
//! stalled at the gate having already published `intent = tail + total`
//! makes nearly every observer fail its window check against fully intact
//! bytes and loop on spurious `Lagged` (the reposition clamps to
//! `latest <= pos`, so `Lagged { missed_bytes: 0 }`) until the gate opens —
//! a livelock on readable data. Gate-first keeps `intent == tail` whenever
//! the producer is stalled, so observers behind a frozen tail drain
//! everything and then simply wait.
//!
//! # Mixed atomicity: anchors borrow, observers copy
//!
//! The producer writes every buffer byte through **4-byte atomic lanes**
//! (headers and payload, the [`crate::broadcast_bytes`] strict-copy policy —
//! observers race those writes and copy out through the same lanes, and
//! record boundaries shift across laps, so any mixed-size scheme would put
//! differently-sized atomics on the same bytes). Anchors, however, parse
//! frames **in place with plain reads** and hand out plain `&[u8]` borrows:
//! the gate guarantees the producer never rewrites a byte until every
//! anchor's published cursor passed it, so an anchor's read races nothing —
//! and a *plain* read of bytes last written by *atomic* stores is race-free
//! given happens-before (`Acquire` on the unified cursor; the anchor's
//! `Release` cursor flush before the producer's rescan `Acquire` fence
//! orders the other direction). Mixed atomicity only matters for RACING
//! accesses; anchors therefore skip all validation — they cannot tear.
//!
//! # Closed contract
//!
//! Dropping the [`BytesProducer`] closes the ring. Anchors drain what was
//! published, then pop `Err(`[`Closed`]`)`; observers drain everything
//! still reachable, then pop `Err(`[`PopError::Closed`]`)`. New anchors are
//! refused on a closed ring ([`SubscribeError::Closed`]); new observers
//! always succeed and are born drained.
//!
//! # Gotchas
//!
//! * `mem::forget` on a [`Msg`] means **redelivery** to that anchor — and
//!   the un-advanced cursor gates the producer globally, so forget-then-idle
//!   stalls the whole ring (observers included, once they drain to the
//!   frozen tail). That is the gating contract, not a leak.
//! * The write slot is **commit-only** (no in-place `&mut [u8]` access):
//!   observers race the payload lanes, so the producer must own the atomic
//!   copy-in — see [`WriteSlot`].
//! * Producer-side [`is_empty`](BytesProducer::is_empty) is an
//!   approximation against the cached gating minimum (it can transiently
//!   over-count, never under-count); anchor-side views are exact.

use std::cell::UnsafeCell;
use std::ptr::NonNull;
#[cfg(not(rust_rb_volatile_copy))]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::sync::Arc;

use crate::atomic_copy::{copy_in_lanes, copy_out_lanes};
use crate::broadcast_bytes::ALIGN;
use crate::cache_padded::CachePadded;
use crate::cursor::round_capacity;
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
use crate::registry::scan_shm_table;
use crate::registry::{
    guard_sentinel, lacks_space, publish_batch_bytes, rescan_gate, scan_chunk_registry,
    subscribe_slot, Chunk, FlushOnDrop, FlushPending, DETACHED,
};
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

#[doc(inline)]
pub use crate::broadcast_bytes::PopError;
#[doc(inline)]
pub use crate::spmc_bytes::{Closed, SubscribeError};

/// The buffer word type: `u64` so the base is 8-aligned (a `Box<[u8]>`
/// allocation only guarantees alignment 1). The producer and the observers
/// go through 4-byte atomic lanes over raw `u8` pointers; anchors read the
/// same bytes plainly (gate-protected). The words are never read as `u64`s.
///
/// Zero-initialized on construction so every byte is always initialized:
/// anchors hand out `&[u8]` views and observers atomically load bytes the
/// producer may never have written — either over uninitialized memory would
/// be instant UB.
type Word = UnsafeCell<u64>;

/// Size of the length header preceding each payload.
const HEADER: usize = 4;
/// Header value marking a padding record that runs to the end of the buffer.
const PADDING: u32 = u32::MAX;
/// Smallest legal capacity: **16**, one power of two above the other byte
/// rings' 8. With 8-aligned records an 8-byte ring's only frame needs the
/// whole capacity, which the normative empty-registry gating default (own
/// cursor minus one) can never grant — free-run would deadlock (see the
/// module docs).
pub(crate) const MIN_CAPACITY: usize = 16;

#[inline(always)]
const fn align_up(n: usize) -> usize {
    (n + (ALIGN - 1)) & !(ALIGN - 1)
}

/// Bytes a record with a `len`-byte payload occupies in the ring.
#[inline(always)]
const fn record_len(len: usize) -> usize {
    align_up(HEADER + len)
}

/// The largest payload a single message may carry: `capacity / 8` — the
/// observers' loss-tolerance bound (see the module docs), clamped below the
/// `u32` header space where `u32::MAX` marks padding.
#[inline(always)]
const fn max_message_len(capacity: usize) -> usize {
    let cap = capacity / 8;
    let header_space = (PADDING - 1) as usize;
    if cap < header_space {
        cap
    } else {
        header_space
    }
}

/// The widest footprint one push can require free, in bytes: wrap padding
/// plus a maximum-size record.
///
/// Derivation from the framing (as in `crate::spmc_bytes`): a record is at
/// most `R = record_len(max_message_len)` bytes, and padding is written only
/// when the record does not fit before the end of the buffer, so
/// `pad = to_end < R`. Both are multiples of [`ALIGN`], hence
/// `pad <= R - ALIGN` and `pad + record <= 2R - ALIGN`. This bounds the
/// *actual* span a starving producer publishes in its flag for the anchors'
/// lag filter (threshold `capacity - span`, see [`Msg`]): with this ring's
/// `capacity / 8` message cap the bound is roughly **a quarter of the
/// ring**, so even the worst episode's threshold sits around three quarters
/// of the capacity — only an anchor whose published occupancy is in the top
/// quarter can possibly be the gate of a starving producer. Contrast
/// spmc_bytes, whose `capacity / 2 - 4` cap pushes the worst-case bound to
/// `capacity - ALIGN` (which is why both rings flag the exact span rather
/// than assuming this constant).
#[inline(always)]
const fn max_record_span(capacity: usize) -> usize {
    2 * record_len(max_message_len(capacity)) - ALIGN
}

/// Decode the record at byte cursor `cur` with **plain** reads — the
/// anchor-side frame parser, `crate::spmc_bytes::decode_record` lifted to
/// u64 cursors. Skips a padding record if present and returns `(cursor at
/// the record header, payload length, payload ptr)`.
///
/// # Safety
///
/// A fully published record must exist at `cur` (availability confirmed via
/// an `Acquire` load of the unified cursor), and the caller must be an
/// anchor (gate-protected: the producer cannot be writing these bytes — see
/// the module's mixed-atomicity section). The producer publishes padding
/// together with the record that follows it (one cursor store covers both),
/// so after a padding skip a record is guaranteed at offset zero.
#[inline(always)]
unsafe fn decode_record(base: *const u8, mask: u64, mut cur: u64) -> (u64, usize, *const u8) {
    let mut pos = (cur & mask) as usize;
    // SAFETY: header reads are 4-aligned (records and padding start on ALIGN
    // boundaries, base is 8-aligned) and in bounds via the mask.
    let mut header = u32::from_le(unsafe { base.add(pos).cast::<u32>().read() });
    if header == PADDING {
        cur = cur.wrapping_add((mask + 1) - pos as u64);
        pos = 0;
        // SAFETY: as above, at offset zero.
        header = u32::from_le(unsafe { base.cast::<u32>().read() });
        debug_assert!(header != PADDING, "padding is never followed by padding");
    }
    let len = header as usize;
    // SAFETY: the record is contiguous: `pos + HEADER + len <= capacity`.
    (cur, len, unsafe { base.add(pos + HEADER) })
}

// -----------------------------------------------------------------------------
// The 4-byte-lane atomic copy. The bulk payload copies (`copy_in_lanes`/
// `copy_out_lanes`) are shared with `crate::broadcast_bytes` via
// `crate::atomic_copy`; the single-lane header accessors stay local because
// this module's `rust_rb_volatile_copy` A/B behaviour deliberately differs
// (here the header lanes flip volatile with everything else;
// broadcast_bytes keeps its header lanes atomic under the switch).
// -----------------------------------------------------------------------------

/// Store `v` into the `u32` lane at byte offset `off` (`Relaxed`).
///
/// # Safety
///
/// `off` must be 4-aligned and `off + 4 <= capacity`; `base` must be the
/// (8-aligned) live buffer base; the lane must be part of a record the
/// single producer currently owns for writing (observers may race through
/// atomics; anchors cannot reach it, per the gate).
#[inline(always)]
unsafe fn store_lane(base: *mut u8, off: usize, v: u32) {
    debug_assert_eq!(off % 4, 0, "ring accesses stay on the 4-byte lane grid");
    #[cfg(not(rust_rb_volatile_copy))]
    // SAFETY: in bounds and 4-aligned per the contract; a shared atomic
    // reference over the `UnsafeCell` storage is the sanctioned way to store
    // while observers race.
    unsafe {
        (*(base.add(off).cast::<AtomicU32>())).store(v, Ordering::Relaxed)
    };
    #[cfg(rust_rb_volatile_copy)]
    // SAFETY: as above; the volatile store is the classic (formally racy)
    // A/B shape — dev switch only.
    unsafe {
        base.add(off).cast::<u32>().write_volatile(v)
    };
}

/// Load the `u32` lane at byte offset `off` (`Relaxed`) — the observer-side
/// header read.
///
/// # Safety
///
/// `off` must be 4-aligned and `off + 4 <= capacity`; `base` must be the
/// (8-aligned) live buffer base.
#[inline(always)]
unsafe fn load_lane(base: *const u8, off: usize) -> u32 {
    debug_assert_eq!(off % 4, 0, "ring accesses stay on the 4-byte lane grid");
    #[cfg(not(rust_rb_volatile_copy))]
    // SAFETY: in bounds and 4-aligned per the contract; a shared atomic
    // reference over the `UnsafeCell` storage is the sanctioned way to load
    // while the producer races.
    unsafe {
        (*(base.add(off).cast::<AtomicU32>())).load(Ordering::Relaxed)
    }
    #[cfg(rust_rb_volatile_copy)]
    // SAFETY: as above (dev switch only).
    unsafe {
        base.add(off).cast::<u32>().read_volatile()
    }
}

// -----------------------------------------------------------------------------
// Shared state
// -----------------------------------------------------------------------------

/// The producer-published cache line both roles spin on: the unified cursor
/// (`tail` ≡ the spmc `write_cursor` ≡ the broadcast committed `tail`)
/// plus, co-located in the same padded slot, the `closed` flag (written once
/// by `BytesProducer::drop`, read only on would-block paths) and the
/// `starving` word (holds the blocked push's required byte span while even
/// a fresh registry scan leaves no room — the anchors' exact release
/// threshold — and 0 otherwise; read by anchors on message release).
/// Consumers already poll this line for the cursor, so neither flag adds
/// coherence traffic.
struct TailSide {
    tail: AtomicU64,
    closed: AtomicU64,
    starving: AtomicU64,
}

/// The state all handles share on a **heap** ring, kept alive by an `Arc`.
/// The shm variant (design §9.4) keeps this same state in a mapped region
/// instead; the handles reach either through the backing seam below.
struct Shared<P, C> {
    buf: Box<[Word]>,
    /// `capacity - 1`, in the u64 domain of all position arithmetic.
    mask: u64,
    /// Byte position the producer is about to invalidate up to (stored
    /// before any lane of a push is written — but strictly AFTER the gate;
    /// see the module docs).
    tail_intent: CachePadded<AtomicU64>,
    /// Byte position of the start of the most recent record — the observer
    /// lap-recovery jump target (stored before `tail`).
    latest: CachePadded<AtomicU64>,
    tail_side: CachePadded<TailSide>,
    /// First registry chunk (anchors only), inline; growth cold-appends.
    registry: Chunk,
    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: buffer bytes are written only by the single producer through
// atomic lanes. Anchors take shared `&[u8]` views of gate-protected records
// (their last reads happen-before the producer's overwrites via the cursor
// choreography); observers copy bytes out with atomic lane loads and expose
// them only after out-of-band validation. All other shared state is atomics.
unsafe impl<P: Send + Sync, C: Send + Sync> Sync for Shared<P, C> {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl<P: Send + Sync, C: Send + Sync> Send for Shared<P, C> {}

impl<P, C> Drop for Shared<P, C> {
    fn drop(&mut self) {
        // The buffer is plain bytes — nothing to drop. Free the appended
        // registry chunks (the first chunk is inline).
        self.registry.free_appended();
    }
}

/// Builder/namespace for constructing an anchored byte ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity in **bytes** at runtime
/// (rounded up to the next power of two, at least 16) and uses [`YieldWait`]
/// on both sides. Pick other strategies with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (gate) strategy, `C` the consumer-side strategy — anchors
/// wait on the shared `C` instance (spmc's choreography, so the producer's
/// publish `notify` reaches them), while each observer carries its **own**
/// `C` instance (broadcast's ownership: the producer never notifies an
/// observer, so a shared instance would be a lie). Both must be
/// [`SelfTimed`].
///
/// There is **no reposition slack knob** (unlike [`crate::anchored`]): a
/// byte ring can only reposition to a record boundary, and the one boundary
/// the producer guarantees is the latest record (see
/// [`crate::broadcast_bytes`]).
pub struct BytesRingBuffer<P = YieldWait, C = YieldWait>(core::marker::PhantomData<(P, C)>);

impl BytesRingBuffer {
    /// Create a ring with the default wait strategies and return its
    /// producer and one initial anchor (subscribe more consumers of either
    /// role from any handle afterwards).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/anchor pair
    pub fn new(min_capacity: usize) -> (BytesProducer, BytesAnchor) {
        BytesRingBuffer::<YieldWait, YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<P, C> BytesRingBuffer<P, C>
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    /// Create the ring with explicit wait strategies and return its producer
    /// and one initial anchor.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (BytesProducer<P, C>, BytesAnchor<P, C>) {
        let capacity = round_capacity(min_capacity, MIN_CAPACITY);

        // `capacity / 8` u64 words; zeroed so every byte any view can reach
        // is initialized memory.
        let mut buf = Vec::with_capacity(capacity / ALIGN);
        buf.resize_with(capacity / ALIGN, || UnsafeCell::new(0u64));

        let shared = Arc::new(Shared {
            buf: buf.into_boxed_slice(),
            mask: capacity as u64 - 1,
            tail_intent: CachePadded::new(AtomicU64::new(0)),
            latest: CachePadded::new(AtomicU64::new(0)),
            tail_side: CachePadded::new(TailSide {
                tail: AtomicU64::new(0),
                closed: AtomicU64::new(0),
                starving: AtomicU64::new(0),
            }),
            registry: Chunk::new(),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        let anchor = subscribe_from(&shared).expect("a fresh ring is not closed");
        let producer = BytesProducer {
            buf: base_of(&shared),
            mask: shared.mask,
            next_seq: 0,
            cached_min: 0,
            cached_cursors: Vec::new(),
            raised_starving: false,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            intent_floor: 0,
            tail_intent: NonNull::from(&*shared.tail_intent),
            latest: NonNull::from(&*shared.latest),
            tail: NonNull::from(&shared.tail_side.tail),
            closed: NonNull::from(&shared.tail_side.closed),
            starving: NonNull::from(&shared.tail_side.starving),
            backing: ProducerBacking::Heap(shared),
        };
        (producer, anchor)
    }
}

/// Base of the byte buffer, derived from the whole-slice `as_ptr` (not a
/// first-element reference) so it keeps provenance over every word.
fn base_of<P, C>(shared: &Arc<Shared<P, C>>) -> NonNull<u8> {
    NonNull::new(shared.buf.as_ptr().cast_mut().cast::<u8>()).expect("buffer is non-null")
}

/// Register a new anchor on live shared state — spmc's `addSequences`
/// choreography [M-F2], provided by the shared gating engine
/// ([`subscribe_slot`]): claim, bitmap RMW strictly before the `SeqCst`
/// fence (the d0549dc regression), join point = the post-fence re-read of
/// the unified cursor — always a record boundary, since the producer
/// publishes whole frames, so parsing starts clean.
fn subscribe_from<P, C>(shared: &Arc<Shared<P, C>>) -> Result<BytesAnchor<P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    // Clone the Arc *before* touching the registry [A-2.2]: the new slot can
    // never outlive the shared state it points into.
    let shared = Arc::clone(shared);
    if shared.tail_side.closed.load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }

    // The [M-F2] claim/activate/fence/re-read choreography (see
    // `crate::registry::subscribe_slot`).
    let slot = subscribe_slot(&shared.registry, &shared.tail_side.tail);

    let buf = base_of(&shared);
    let mask = shared.mask;
    Ok(BytesAnchor {
        buf,
        mask,
        cursor_slot: slot.cursor_slot,
        tail: NonNull::from(&shared.tail_side.tail),
        closed: NonNull::from(&shared.tail_side.closed),
        starving: NonNull::from(&shared.tail_side.starving),
        read_cursor: slot.joined,
        published: slot.published,
        tail_cache: slot.joined,
        backing: AnchorBacking::Heap {
            shared,
            chunk: slot.chunk,
            slot_idx: slot.slot_idx,
        },
    })
}

/// Register a new observer: read the tail and start there. Trivially
/// dynamic — an observer is pure reader state; nothing can fail and nothing
/// bounds the count (an observer subscribed to a closed ring is born
/// drained and pops `Closed`).
fn observe_from<P, C>(shared: &Arc<Shared<P, C>>) -> BytesObserver<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    let shared = Arc::clone(shared);
    let pos = shared.tail_side.tail.load(Ordering::Acquire);
    BytesObserver {
        buf: base_of(&shared),
        mask: shared.mask,
        pos,
        tail_cache: pos,
        wait: C::default(),
        tail_intent: NonNull::from(&*shared.tail_intent),
        latest: NonNull::from(&*shared.latest),
        tail: NonNull::from(&shared.tail_side.tail),
        closed: NonNull::from(&shared.tail_side.closed),
        backing: ObserverBacking::Heap(shared),
    }
}

/// Where the producing handle's shared state lives — the backing seam,
/// mirroring [`crate::anchored`]'s (named `*Backing` per the same rationale:
/// this module's public consumer type is the [`BytesAnchor`]): every hot
/// atomic is reached through the handle's cached raw pointers (identical
/// for both variants); the backing is consulted on cold paths plus the
/// wait-strategy `notify()` accessor on publish.
enum ProducerBacking<P, C> {
    /// In-process ring: the shared state lives on the heap in an `Arc`; the
    /// anchor registry is the append-only chunk list.
    Heap(Arc<Shared<P, C>>),
    /// Cross-process ring: the state lives in a mapped shared region; the
    /// registry is the flat anchor table. Boxed so enabling the feature
    /// does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmProducer<C>>),
}

impl<P: WaitStrategy, C: WaitStrategy> ProducerBacking<P, C> {
    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            ProducerBacking::Heap(shared) => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerBacking::Shm(backing) => &backing.consumer_wait,
        }
    }

    /// Whether teardown may touch shared state (see [`crate::anchored`]'s
    /// twin).
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ProducerBacking::Heap(_) => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerBacking::Shm(backing) => {
                backing.owned_by_current_process() && backing.owns_lease()
            }
        }
    }
}

/// The [`BytesAnchor`] handle's side of the backing seam (see
/// [`ProducerBacking`]).
enum AnchorBacking<P, C> {
    /// Heap ring: the `Arc` plus this anchor's chunk/slot coordinates for
    /// the cold detach.
    Heap {
        shared: Arc<Shared<P, C>>,
        chunk: NonNull<Chunk>,
        slot_idx: usize,
    },
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmConsumer<P, C>>),
}

impl<P: WaitStrategy, C: WaitStrategy> AnchorBacking<P, C> {
    #[inline(always)]
    fn producer_wait(&self) -> &P {
        match self {
            AnchorBacking::Heap { shared, .. } => &shared.producer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            AnchorBacking::Shm(backing) => &backing.producer_wait,
        }
    }

    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            AnchorBacking::Heap { shared, .. } => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            AnchorBacking::Shm(backing) => &backing.consumer_wait,
        }
    }

    /// Whether teardown may touch shared state (see [`crate::anchored`]'s
    /// twin: fork copies and superseded slot leases must not flush or free).
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            AnchorBacking::Heap { .. } => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            AnchorBacking::Shm(backing) => {
                backing.owned_by_current_process() && backing.owns_slot()
            }
        }
    }

    /// The registry de-registration half of anchor teardown (the caller has
    /// already flushed and stored the cursor sentinel); both variants wake
    /// a producer blocked on the gate [A-1.3].
    fn detach(&self) {
        match self {
            AnchorBacking::Heap {
                shared,
                chunk,
                slot_idx,
            } => {
                // SAFETY: the chunk lives until `Shared::drop`; we hold the
                // `Arc`.
                unsafe { chunk.as_ref() }.deactivate(*slot_idx);
                shared.producer_wait.notify();
            }
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            AnchorBacking::Shm(backing) => backing.detach(),
        }
    }
}

/// The [`BytesObserver`] handle's side of the backing seam: purely a
/// keep-alive (shm: the **read-only** mapping — the enforcement).
enum ObserverBacking<P, C> {
    Heap(Arc<Shared<P, C>>),
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm {
        region: Arc<crate::shm::ShmRegion>,
        /// The table size, needed to re-derive the buffer base for sibling
        /// subscribes.
        max_anchors: usize,
    },
}

/// The producing half of a [`BytesRingBuffer`]: spmc_bytes' byte gate
/// composed with broadcast_bytes' three-counter write, in the normative §9.3
/// order (gate → intent → fence → lanes → latest → tail). `Send` but not
/// `Clone`: exactly one producer, enforced by the type system.
///
/// Dropping the producer **closes** the ring: anchors and observers drain
/// what was published and then see their role's closed error.
pub struct BytesProducer<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the byte buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<u8>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next byte position to write (private; equals the published tail
    /// between pushes — a claim does not advance it until committed).
    next_seq: u64,
    /// Cached minimum of the active anchors' byte cursors — the gate. A
    /// lower bound; the fast-path space check touches no shared line.
    cached_min: u64,
    /// Per-slot cached anchor cursors, mirroring the registry geometry.
    /// Monotonicity makes every cached value a permanent lower bound — for
    /// later occupants of the slot too [P-F3].
    cached_cursors: Vec<[u64; crate::registry::CHUNK_SLOTS]>,
    /// Whether we raised the starving flag and have not yet cleared it
    /// (producer-local; keeps the never-starved hot path free of any flag
    /// access).
    raised_starving: bool,
    /// The shm intent floor: a recovered producer must never declare an
    /// intent below a dead predecessor's frontier — the bytes the
    /// predecessor destroyed mid-push sit just under that frontier, and a
    /// regressed declaration would re-admit them to the observers' window
    /// check. Heap rings (and shm rings with no crash to heal) carry 0, so
    /// the push-path `max` folds to a no-op.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    intent_floor: u64,
    /// The three counters plus the closed word and the starving flag
    /// (cached raw pointers; heap: into the `Arc`, shm: into the mapped
    /// region — the hot push path is identical).
    tail_intent: NonNull<AtomicU64>,
    latest: NonNull<AtomicU64>,
    tail: NonNull<AtomicU64>,
    closed: NonNull<AtomicU64>,
    starving: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive, carries the wait strategies, and
    /// names the registry (heap chunks vs shm table) for the cold paths.
    backing: ProducerBacking<P, C>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the backing keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesProducer<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesProducer<P, C> {
    fn drop(&mut self) {
        // Flag-then-notify [A-1.1]: an anchor that checked the flag just
        // before this store is parked in a wait whose predicate re-checks
        // `closed`, and the notify wakes it. Observers are `SelfTimed` and
        // re-check on their own; the notify is for anchors. Guarded for shm
        // (heap: constant true): only a graceful drop by the live lease
        // holder sets the ring-wide closed word, per the trust model.
        if self.backing.teardown_allowed() {
            // SAFETY: `closed` points into the live shared state.
            unsafe { self.closed.as_ref() }.store(1, Ordering::Release);
            self.backing.consumer_wait().notify();
        }
    }
}

impl<P, C> BytesProducer<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until the slowest anchor frees enough bytes, then enqueue a
    /// copy of `msg`.
    ///
    /// With zero anchors this never blocks (free-run): observers that have
    /// not kept up are lapped and will observe [`PopError::Lagged`].
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()` — such a message could
    /// never be sent, so waiting for room would deadlock.
    #[inline]
    pub fn push(&mut self, msg: &[u8]) {
        let (pad, total) = self.frame(msg.len());
        // (1) The gate — strictly BEFORE the intent declaration [audit F3].
        self.wait_for_space(total);
        // SAFETY: `frame` sized the record and `wait_for_space` confirmed
        // `total` free bytes.
        unsafe { self.write_frame(pad, msg) };
    }

    /// Enqueue a copy of `msg` without blocking. Returns `false` if the ring
    /// is gated (not enough free space for the slowest anchor) after one
    /// full registry rescan — with `tail_intent` untouched, so a gated
    /// `try_push` never disturbs the observers.
    ///
    /// "Free" is judged against the anchors' *published* progress; while an
    /// anchor defers publishes in the backed-up regime this can spuriously
    /// fail with up to `capacity / 8` (max 4096) bytes consumed but not yet
    /// published. A blocking [`push`](Self::push) is woken as soon as the
    /// gating anchor flushes.
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()`.
    #[inline]
    #[must_use]
    pub fn try_push(&mut self, msg: &[u8]) -> bool {
        let (pad, total) = self.frame(msg.len());
        if !self.has_space(total) {
            return false;
        }
        // SAFETY: as in `push`.
        unsafe { self.write_frame(pad, msg) };
        true
    }

    /// Block until there is room for a `len`-byte message, then return a
    /// claim on that space. Publish with [`WriteSlot::commit`]; dropping the
    /// claim uncommitted publishes nothing (see [`WriteSlot`] for why the
    /// slot is commit-only, unlike `spmc_bytes`' serialize-in-place slot).
    ///
    /// # Panics
    ///
    /// Panics if `len > self.max_message_len()`.
    #[inline]
    pub fn claim(&mut self, len: usize) -> WriteSlot<'_, P, C> {
        let (pad, _total) = self.frame(len);
        self.wait_for_space(_total);
        WriteSlot {
            producer: self,
            pad,
            len,
        }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if the ring is
    /// gated.
    ///
    /// # Panics
    ///
    /// Panics if `len > self.max_message_len()`.
    #[inline]
    pub fn try_claim(&mut self, len: usize) -> Option<WriteSlot<'_, P, C>> {
        let (pad, total) = self.frame(len);
        if !self.has_space(total) {
            return None;
        }
        Some(WriteSlot {
            producer: self,
            pad,
            len,
        })
    }

    /// Subscribe a new anchor. Its join point is the currently published
    /// cursor — always a record boundary — so it sees only messages
    /// published after this call returns, and **all** of them, even if the
    /// producer was free-running (the §9.6 join induction lifted to bytes;
    /// no anchor-side validation is needed or performed).
    ///
    /// Cold: the producer's gating caches pick the newcomer up on the next
    /// rescan, which the gating default forces at least once per lap.
    ///
    /// On a shared-memory ring the anchor table is fixed at creation, so
    /// this can additionally fail with [`SubscribeError::Full`].
    pub fn subscribe_anchor(&self) -> Result<BytesAnchor<P, C>, SubscribeError> {
        match &self.backing {
            ProducerBacking::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this capacity
            // when this handle was constructed.
            ProducerBacking::Shm(backing) => unsafe {
                shm_subscribe_anchor(backing.region(), backing.max_slots(), self.mask + 1)
            },
        }
    }

    /// Subscribe a new observer at the current tail. Never fails: observers
    /// are unbounded pure readers (one subscribed to a closed ring is born
    /// drained and pops [`PopError::Closed`]).
    ///
    /// On a shared-memory ring the subscriber shares this producer's
    /// **read-write** mapping — the read-only enforcement story belongs to
    /// [`attach_shm_observer`](BytesRingBuffer::attach_shm_observer).
    pub fn subscribe_observer(&self) -> BytesObserver<P, C> {
        match &self.backing {
            ProducerBacking::Heap(shared) => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this capacity
            // when this handle was constructed.
            ProducerBacking::Shm(backing) => unsafe {
                BytesObserver::from_shm(
                    Arc::clone(backing.region()),
                    (self.mask + 1) as usize,
                    backing.max_slots(),
                )
            },
        }
    }

    /// Number of currently attached anchors (a registry scan — cold; a
    /// racing subscribe/detach makes it a snapshot, not a guarantee).
    /// Observers are not counted: nothing tracks them.
    pub fn anchor_count(&self) -> usize {
        match &self.backing {
            ProducerBacking::Heap(shared) => shared.registry.active_count(),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerBacking::Shm(backing) => backing.active_count(),
        }
    }

    /// Fast space check against the cached gating minimum; on a miss, one
    /// full registry rescan. Zero shared loads in the common case. Also
    /// maintains the starving flag with hysteresis (verbatim from
    /// spmc_bytes): raised once per episode when even a full rescan leaves
    /// no room, kept up while space only appears via rescans, cleared once
    /// the cached check passes comfortably. The flag carries the blocked
    /// push's **actual required span** (`needed` = pad + record bytes),
    /// which is what makes the anchors' release threshold
    /// `capacity - span` exact: while one push is blocked the write cursor
    /// cannot move, so `frame` is deterministic and every check of the
    /// episode carries the same span.
    #[inline(always)]
    fn has_space(&mut self, needed: u64) -> bool {
        if lacks_space(self.next_seq, needed, self.cached_min, self.mask + 1) {
            if self.rescan(needed) {
                // Space appeared only after a rescan: still running tight —
                // keep the flag up (hysteresis; no store churn while the
                // ring hovers at the edge of starvation).
                return true;
            }
            // Starving: even the freshest registry scan leaves no room.
            // Publish the episode's span once per change (set-if-different:
            // while one episode persists this is a read of a line the
            // producer polls anyway) so the gating anchor's lag-filtered
            // release can free us with an exact threshold.
            debug_assert!(
                needed <= max_record_span((self.mask + 1) as usize) as u64,
                "a legal frame never exceeds max_record_span"
            );
            // SAFETY: `starving` points into the live shared state.
            let starving = unsafe { self.starving.as_ref() };
            if starving.load(Ordering::Relaxed) != needed {
                starving.store(needed, Ordering::Release);
            }
            self.raised_starving = true;
            return false;
        }
        // The *cached* check passed: comfortably unstarved. Clear our flag
        // once; the local bool keeps this branch untaken (a register test)
        // on the never-starved hot path.
        if self.raised_starving {
            self.raised_starving = false;
            // SAFETY: `starving` points into the live shared state.
            unsafe { self.starving.as_ref() }.store(0, Ordering::Release);
        }
        true
    }

    /// Spin/park (per the producer wait strategy) until the gate opens.
    /// `tail_intent` is untouched throughout — the normative gate-first
    /// order [audit F3]: a stalled producer keeps `intent == tail`.
    #[inline(always)]
    fn wait_for_space(&mut self, needed: u64) {
        if self.has_space(needed) {
            return;
        }
        // A separate handle on the wait strategy, so the predicate below can
        // borrow `self` mutably (cold path; one refcount bump). Shm backings
        // carry per-handle `CrossProcess` strategies, for which a fresh
        // default instance IS the same (stateless, self-timed) strategy.
        let heap = match &self.backing {
            ProducerBacking::Heap(shared) => Some(Arc::clone(shared)),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerBacking::Shm(_) => None,
        };
        match heap {
            Some(shared) => {
                while !self.has_space(needed) {
                    // The predicate re-runs the FULL scan [M-F4]: a cached
                    // minimum here is a deadlock, and rescanning is also
                    // what lets the wait terminate when every gating anchor
                    // detaches.
                    shared.producer_wait.wait(|| self.rescan(needed));
                }
            }
            None => {
                let wait = P::default();
                while !self.has_space(needed) {
                    // Full-scan predicate [M-F4], as above.
                    wait.wait(|| self.rescan(needed));
                }
            }
        }
    }

    /// The gate-miss slow path: rescan the registry and recompute
    /// `cached_min` — the [M-F2]/[P-F1]/[M-F1, §9.6] fence discipline (and
    /// the load-bearing empty-registry `- 1`) lives in [`rescan_gate`]; this
    /// supplies the registry seam (one walk per registry kind, same cache
    /// geometry). The free-run grant is sound here because any legal frame
    /// needs at most `max_record_span < capacity` bytes, which the 16-byte
    /// capacity floor guarantees. Returns whether `needed` bytes are now
    /// free.
    fn rescan(&mut self, needed: u64) -> bool {
        let capacity = self.mask + 1;
        let next_seq = self.next_seq;
        let backing = &self.backing;
        let cached_cursors = &mut self.cached_cursors;
        rescan_gate(
            next_seq,
            needed,
            capacity,
            &mut self.cached_min,
            || match backing {
                ProducerBacking::Heap(shared) => scan_chunk_registry(
                    &shared.registry,
                    cached_cursors,
                    next_seq,
                    needed,
                    capacity,
                ),
                #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
                ProducerBacking::Shm(backing) => {
                    scan_shm_table(backing, cached_cursors, next_seq, needed, capacity)
                }
            },
        )
    }

    /// Compute the record framing for a `len`-byte message at the current
    /// write position: `(padding_bytes, total_bytes_consumed)`.
    #[inline]
    fn frame(&self, len: usize) -> (u64, u64) {
        let capacity = self.mask + 1;
        assert!(
            len <= max_message_len(capacity as usize),
            "message length {len} exceeds max_message_len ({})",
            max_message_len(capacity as usize),
        );
        let record = record_len(len) as u64;
        let to_end = capacity - (self.next_seq & self.mask);
        if record <= to_end {
            (0, record)
        } else {
            (to_end, to_end + record)
        }
    }

    /// Steps (2)–(5) of the normative §9.3 write order — the caller holds
    /// the gate (1): declare `tail_intent`, `fence(Release)`, write the
    /// lanes, publish `latest` then the unified `tail`, then notify anchors.
    ///
    /// # Safety
    ///
    /// The caller must have confirmed `pad + record_len(msg.len())` free
    /// bytes via the gate, with `pad` computed by `frame(msg.len())` at the
    /// current write cursor.
    #[inline(always)]
    unsafe fn write_frame(&mut self, pad: u64, msg: &[u8]) {
        let len = msg.len();
        let record = record_len(len) as u64;
        let new_tail = self.next_seq + pad + record;

        // The shm crash-recovery floor (see the field docs): declarations
        // are monotonic across producer sessions. The floor composes with
        // the normative §9.3 order — it adjusts only WHAT step (2) declares
        // (`max(new_tail, floor)`), never WHEN: the declaration stays
        // strictly after the gate, so a stalled producer still keeps
        // `intent == tail` (the floor equals the session-start intent then,
        // which the stalled tail already matches or the predecessor already
        // declared). Heap: floor is 0 and the `max` folds away; compiled
        // out entirely without the `shm` feature.
        #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
        let declared = new_tail.max(self.intent_floor);
        #[cfg(not(all(feature = "shm", target_os = "linux", target_has_atomic = "64")))]
        let declared = new_tail;

        // (2) Declare intent: "I will destroy everything below new_tail".
        //     Observers reading any byte this push will touch now fail
        //     their window check. Strictly after the gate — see the module
        //     docs for the stalled-gate livelock this order prevents.
        // SAFETY: `tail_intent` points into the live shared state.
        unsafe { self.tail_intent.as_ref() }.store(declared, Ordering::Release);
        // (3) The load-bearing fence: the lane stores below must not be
        //     hoisted above the intent store (pairs with the observers'
        //     post-copy acquire fence).
        fence(Ordering::Release);

        // (4) The lanes.
        let base = self.buf.as_ptr();
        let off = (self.next_seq & self.mask) as usize;
        let rec_off = ((self.next_seq + pad) & self.mask) as usize;
        // SAFETY (whole block): offsets are `& mask`, so in bounds; record
        // and padding starts are 8-aligned (records are ALIGN multiples
        // from an 8-aligned base), so every header is an aligned lane; the
        // record fits contiguously by the pad computation. The gate
        // confirmed the space free of anchor borrows (their Release cursor
        // stores synchronize with the rescan's Acquire fence); observers
        // race only through atomic lanes and revalidate out-of-band.
        unsafe {
            if pad > 0 {
                // `frame` only pads mid-buffer, where at least one lane
                // remains before the end. (PADDING is all-ones:
                // endian-proof.)
                store_lane(base, off, PADDING);
            }
            // Headers are little-endian on every target, matching the other
            // byte rings (free on LE machines; a byte swap on BE ones).
            store_lane(base, rec_off, (len as u32).to_le());
            copy_in_lanes(msg.as_ptr(), base.add(rec_off + HEADER), len);
        }

        // (5) Publish the jump target, then the frontier — `latest` first,
        //     so any consumer that sees the new tail also sees a coherent
        //     latest; the unified `tail` strictly LAST (both roles spin on
        //     it; per-push publish keeps `Lagged` counts, join points, and
        //     the §9.6 induction exact).
        // SAFETY: `latest`/`tail` point into the live shared state.
        unsafe { self.latest.as_ref() }.store(self.next_seq + pad, Ordering::Release);
        unsafe { self.tail.as_ref() }.store(new_tail, Ordering::Release);
        self.next_seq = new_tail;
        // Wake anchors blocked on data (a no-op for the spin strategies);
        // observers are `SelfTimed` and never wait for a notify.
        self.backing.consumer_wait().notify();
    }

    /// Whether the ring looks empty per the producer's **cached** gating
    /// view.
    ///
    /// An approximation: the cache is only refreshed on gate misses, so this
    /// can transiently report `false` for a ring every anchor has fully
    /// drained (and always does after the producer has free-run with no
    /// anchors attached). It never reports `true` for a ring some anchor
    /// still has messages to read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.next_seq.wrapping_sub(self.cached_min) == 0
    }

    /// Total framed bytes published so far (headers and wrap padding
    /// included) — the ring's frontier. Producer-local and exact.
    #[inline]
    pub fn tail(&self) -> u64 {
        self.next_seq
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 16).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 8` (the
    /// observers' bound — see the module docs for why this is not the
    /// gating rings' `capacity / 2 - 4`).
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }
}

/// A claimed, not-yet-published message slot — **commit-only**, unlike
/// `spmc_bytes`' write slot.
///
/// There is deliberately no `Deref`/`DerefMut` serialize-in-place path:
/// observers race the payload lanes, so every byte must go in through the
/// 4-byte **atomic** lane copy the producer controls. Handing out
/// `&mut [u8]` would let the user write with plain stores, which is
/// undefined behaviour against a concurrent observer's atomic copy-out —
/// the same reason [`crate::anchored`]'s slot dropped the element ring's
/// `uninit()`/`commit_init()` pair. [`push`](BytesProducer::push) is the
/// primary API; the claim exists to reserve space early (and to make a
/// gated ring observable via [`try_claim`](BytesProducer::try_claim)).
///
/// Dropping the slot uncommitted publishes nothing: no counter moved — not
/// even `tail_intent`, which is declared only inside
/// [`commit`](Self::commit) — so no consumer of either role can observe the
/// abandoned claim, and observers' window checks are never disturbed by it.
pub struct WriteSlot<'a, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut BytesProducer<P, C>,
    /// Wrap padding computed at claim time (the producer cursor cannot move
    /// while this exclusive borrow lives).
    pad: u64,
    /// The payload length the claim reserved space for.
    len: usize,
}

impl<P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, P, C> {
    /// Copy `msg` into the reserved space and publish it (equivalent to
    /// `push` on a claim that already passed the gate).
    ///
    /// # Panics
    ///
    /// Panics if `msg.len()` differs from the length the claim reserved.
    #[inline]
    pub fn commit(self, msg: &[u8]) {
        let Self { producer, pad, len } = self;
        assert_eq!(
            msg.len(),
            len,
            "committed message length must equal the claimed length"
        );
        // SAFETY: space for `(pad, len)` was confirmed when the slot was
        // created, and the producer cursor has not moved since (`self`
        // borrowed it exclusively).
        unsafe { producer.write_frame(pad, msg) };
    }
}

/// A required consumer of a [`BytesRingBuffer`] — spmc_bytes' consumer over
/// the unified u64 cursor. Owns a private byte read cursor and one registry
/// slot, and parses frames in place; the producer min-gates on it, so an
/// anchor **sees every message**. `Send` but not `Clone`; create more with
/// [`subscribe_anchor`](Self::subscribe_anchor).
///
/// Dropping the anchor detaches it: it stops gating the producer and wakes
/// a producer blocked on it.
pub struct BytesAnchor<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the byte buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<u8>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// This anchor's cursor word — the hot flush target.
    cursor_slot: NonNull<AtomicU64>,
    /// The producer's unified cursor (cached raw pointer).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (read on would-block paths only).
    closed: NonNull<AtomicU64>,
    /// The shared producer-starving flag (read behind the lag filter).
    starving: NonNull<AtomicU64>,
    /// Next byte to read (private to this thread).
    read_cursor: u64,
    /// The value of `read_cursor` last published to the registry slot (see
    /// [`advance`](Self::advance) for the adaptive publish rule).
    published: u64,
    /// Cached snapshot of the producer's unified cursor.
    tail_cache: u64,
    /// Keeps the ring's memory alive, carries the wait strategies, and
    /// names the registry (heap chunk coordinates vs shm table slot) for
    /// the cold detach.
    backing: AnchorBacking<P, C>,
}

// SAFETY: the anchor only touches anchor-private state plus atomics; the
// cached pointers reference state the backing keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesAnchor<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesAnchor<P, C> {
    fn drop(&mut self) {
        // Guarded teardown (heap: constant true): a fork-inherited copy or a
        // handle whose slot lease was superseded must not flush over — or
        // free — live state it no longer owns.
        if !self.backing.teardown_allowed() {
            return;
        }
        // Publish any deferred progress first (harmless — the detach store
        // below supersedes it, but a concurrent rescan between the two sees
        // the freshest cursor instead of a stale one).
        self.flush_pending();
        // Detach order matters: sentinel first, then the registry
        // de-registration (heap: bitmap bit clear; shm: control-word FREE) —
        // a subscriber only claims fully-detached slots, which this ordering
        // proves (see `claim_registry_slot` and the shm claim choreography).
        // SAFETY: `cursor_slot` points into the live shared state.
        unsafe { self.cursor_slot.as_ref() }.store(DETACHED, Ordering::Release);
        // De-register and wake a producer blocked on the gate [A-1.3]: a
        // producer parked waiting for the minimum to move would stall
        // forever if its last gating anchor detached silently.
        self.backing.detach();
    }
}

impl<P, C> BytesAnchor<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until a message is available, then return a zero-copy view of
    /// it. The message is released (this anchor's cursor advances past the
    /// record) when the returned [`Msg`] drops; the bytes stay in the ring
    /// for the other consumers.
    ///
    /// Returns `Err(`[`Closed`]`)` only when the producer has been dropped
    /// *and* every published message has been consumed.
    #[inline]
    pub fn pop(&mut self) -> Result<Msg<'_, P, C>, Closed> {
        self.wait_for_item()?;
        Ok(self.next_msg())
    }

    /// Return the next message without blocking. `Ok(None)` means
    /// empty-but-alive; `Err(`[`Closed`]`)` means closed **and** drained.
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<Msg<'_, P, C>>, Closed> {
        if self.has_item() {
            return Ok(Some(self.next_msg()));
        }
        self.check_closed()?;
        if self.available_cached() != 0 {
            // The close re-check refreshed the cursor and found a final
            // message published just before the producer dropped.
            return Ok(Some(self.next_msg()));
        }
        Ok(None)
    }

    /// Consume up to roughly one publish batch (`capacity / 8`, max 4096
    /// bytes — always at least one message) of available messages, calling
    /// `f` on each in place, and return how many were consumed. The read
    /// cursor is published **once**, after the last message — one `Release`
    /// store (and one wake-up) for the whole batch.
    ///
    /// Returns `0` on an empty ring; a closed ring is **not** reported here
    /// (a drained, closed ring also returns `0`) — use
    /// [`pop`](Self::pop)/[`try_pop`](Self::try_pop) to observe [`Closed`].
    ///
    /// The private cursor advances over each record *before* `f` sees it,
    /// and the publish happens even if `f` panics (an unwound drain never
    /// re-delivers already-processed messages to this anchor). The slice
    /// handed to `f` stays valid throughout: the producer cannot reuse the
    /// batch's bytes until the final publish, which is strictly after `f`.
    pub fn drain<F: FnMut(&[u8])>(&mut self, mut f: F) -> usize {
        // Unconditionally refresh: the contract is "what is currently in the
        // ring", which a stale non-empty cache must not bound.
        let end = self.refresh();
        if end.wrapping_sub(self.read_cursor) == 0 {
            return 0;
        }
        let batch = publish_batch_bytes(self.mask + 1);
        let start = self.read_cursor;

        // Publish on exit — including an unwind out of `f` (the engine's
        // `FlushOnDrop` guard over this anchor's `flush_pending`).
        let guard = FlushOnDrop(self);
        let base = guard.0.buf.as_ptr();
        let mask = guard.0.mask;
        let mut count = 0;

        while end.wrapping_sub(guard.0.read_cursor) != 0
            && guard.0.read_cursor.wrapping_sub(start) < batch
        {
            // SAFETY: records below `end` are fully published; this is an
            // anchor (gate-protected plain reads).
            let (cur, len, payload) = unsafe { decode_record(base, mask, guard.0.read_cursor) };
            // Advance before the callback: the record counts as consumed
            // even if `f` unwinds. The payload slice stays valid — the
            // producer cannot reuse it until the guard publishes, strictly
            // after `f`.
            guard.0.read_cursor = cur.wrapping_add(record_len(len) as u64);
            // SAFETY: payload is contiguous, in bounds, and fully published.
            f(unsafe { std::slice::from_raw_parts(payload, len) });
            count += 1;
        }
        count
    }

    /// Subscribe a further anchor; see [`BytesProducer::subscribe_anchor`].
    pub fn subscribe_anchor(&self) -> Result<BytesAnchor<P, C>, SubscribeError> {
        match &self.backing {
            AnchorBacking::Heap { shared, .. } => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this capacity
            // when this handle was constructed.
            AnchorBacking::Shm(backing) => unsafe {
                shm_subscribe_anchor(backing.region(), backing.max_slots(), self.mask + 1)
            },
        }
    }

    /// Subscribe an observer; see [`BytesProducer::subscribe_observer`].
    pub fn subscribe_observer(&self) -> BytesObserver<P, C> {
        match &self.backing {
            AnchorBacking::Heap { shared, .. } => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: as for [`BytesProducer::subscribe_observer`]'s shm arm.
            AnchorBacking::Shm(backing) => unsafe {
                BytesObserver::from_shm(
                    Arc::clone(backing.region()),
                    (self.mask + 1) as usize,
                    backing.max_slots(),
                )
            },
        }
    }

    /// Whether this anchor has nothing to read. Exact on this side: uses
    /// the anchor's private cursor, which is always current.
    #[inline]
    pub fn is_empty(&self) -> bool {
        // SAFETY: `tail` points into the live shared state.
        unsafe { self.tail.as_ref() }
            .load(Ordering::Acquire)
            .wrapping_sub(self.read_cursor)
            == 0
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 16).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 8`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }

    /// Bytes available per the cached view of the producer's cursor.
    #[inline(always)]
    fn available_cached(&self) -> u64 {
        self.tail_cache.wrapping_sub(self.read_cursor)
    }

    /// Unconditionally reload the cached view of the producer's cursor
    /// (`Acquire`) and return it.
    #[inline(always)]
    fn refresh(&mut self) -> u64 {
        // SAFETY: `tail` points into the live shared state.
        self.tail_cache = unsafe { self.tail.as_ref() }.load(Ordering::Acquire);
        self.tail_cache
    }

    /// Check for at least one available message, reloading the producer's
    /// cursor at most once. The producer publishes whole frames, so any
    /// nonzero availability is at least one complete record.
    #[inline(always)]
    fn has_item(&mut self) -> bool {
        if self.available_cached() == 0 {
            self.refresh();
            if self.available_cached() == 0 {
                return false;
            }
        }
        true
    }

    /// The would-block close check: if the producer is gone, re-read the
    /// cursor once more (the `Acquire` load of `closed` synchronizes with
    /// the producer's `Release` store, which follows its final publish) and
    /// report [`Closed`] only if genuinely drained.
    #[inline]
    fn check_closed(&mut self) -> Result<(), Closed> {
        // SAFETY: `closed` points into the live shared state.
        if unsafe { self.closed.as_ref() }.load(Ordering::Acquire) != 0 {
            self.refresh();
            if self.available_cached() == 0 {
                return Err(Closed);
            }
        }
        Ok(())
    }

    /// Spin/park (per the consumer wait strategy) until data arrives or the
    /// ring is closed and drained.
    #[inline(always)]
    fn wait_for_item(&mut self) -> Result<(), Closed> {
        loop {
            if self.has_item() {
                return Ok(());
            }
            self.check_closed()?;
            if self.available_cached() != 0 {
                return Ok(());
            }
            let tail = self.tail;
            let closed = self.closed;
            let read = self.read_cursor;
            self.backing.consumer_wait().wait(|| {
                // SAFETY: the pointers reference live shared state the
                // backing keeps alive for the duration of the wait.
                unsafe {
                    tail.as_ref().load(Ordering::Acquire).wrapping_sub(read) != 0
                        || closed.as_ref().load(Ordering::Acquire) != 0
                }
            });
        }
    }

    /// Common tail of `pop`/`try_pop`: availability is already confirmed;
    /// decode the record at the read cursor with plain reads (gate-protected
    /// — see the module's mixed-atomicity section). Skipped wrap padding is
    /// folded into the private cursor here; [`Msg`]'s drop advances (and
    /// accounts) the record itself.
    #[inline(always)]
    fn next_msg(&mut self) -> Msg<'_, P, C> {
        // SAFETY: availability was confirmed by the caller; this is an
        // anchor (gate-protected plain reads).
        let (cur, len, payload) =
            unsafe { decode_record(self.buf.as_ptr(), self.mask, self.read_cursor) };
        self.read_cursor = cur;
        // SAFETY: `payload` is derived from the non-null buffer base.
        let payload = unsafe { NonNull::new_unchecked(payload.cast_mut()) };
        Msg {
            payload,
            len,
            anchor: self,
        }
    }

    /// Release `amount` just-consumed bytes with the adaptive publish:
    /// immediate when caught up, batched (`capacity / 8`, max 4096 bytes)
    /// while backed up — plus the **lag-filtered starving release** [M-F8]:
    /// when the producer's starving flag is up, publish immediately, but
    /// only if this anchor could actually be the gate. The filter is
    /// anchor-local: the flag carries the blocked push's **actual required
    /// span** (pad + record bytes — constant for the whole episode, because
    /// the blocked producer's write cursor cannot move and the framing is a
    /// pure function of it), so the gating anchor's *published* occupancy
    /// provably exceeds `capacity - span`; an anchor below that exact
    /// threshold is not the gate and keeps batching. The check runs against
    /// `published` — not the private cursor — so deferred progress and
    /// skipped wrap padding cannot make a true gate look innocent.
    ///
    /// The span never exceeds [`max_record_span`] — about a quarter of this
    /// ring (the `capacity / 8` message cap) — so even the *worst* episode's
    /// threshold sits at three quarters of the capacity: only anchors in
    /// the top quarter of occupancy ever react, and a small blocked push
    /// filters tighter still. The filter stays conservative under
    /// staleness: a stale `tail_cache` can only *under*-state occupancy
    /// (defer the flush, never publish a wrong cursor), and a deferred gate
    /// still flushes on the caught-up or batch triggers below.
    #[inline(always)]
    fn advance(&mut self, amount: u64) {
        let capacity = self.mask + 1;
        // SAFETY: `starving` points into the live shared state.
        let span = unsafe { self.starving.as_ref() }.load(Ordering::Acquire);
        let publish_now =
            span != 0 && self.tail_cache.wrapping_sub(self.published) >= capacity - span;
        self.read_cursor = self.read_cursor.wrapping_add(amount);
        if publish_now
            || self.read_cursor == self.tail_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch_bytes(capacity)
        {
            self.flush();
        }
    }

    /// Publish the private read cursor to this anchor's registry slot and
    /// wake a producer blocked on the gate (a no-op for spin strategies).
    ///
    /// Guarded by slot-lease ownership on shm rings (heap: no check at all)
    /// — see [`crate::anchored`]'s twin for the zombie rationale [A-4.1].
    #[inline(always)]
    fn flush(&mut self) {
        #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
        if let AnchorBacking::Shm(backing) = &self.backing {
            if !backing.owns_slot() {
                // Mark as published so retry paths don't spin on the dead
                // lease.
                self.published = self.read_cursor;
                return;
            }
        }
        // Never publish the DETACHED sentinel; one unit less only gates the
        // producer more, and the next flush publishes past it.
        // SAFETY: `cursor_slot` points into the live shared state.
        unsafe { self.cursor_slot.as_ref() }
            .store(guard_sentinel(self.read_cursor), Ordering::Release);
        self.published = self.read_cursor;
        self.backing.producer_wait().notify();
    }

    /// [`flush`](Self::flush) only if there is unpublished progress.
    #[inline(always)]
    fn flush_pending(&mut self) {
        if self.read_cursor != self.published {
            self.flush();
        }
    }
}

impl<P: WaitStrategy, C: WaitStrategy> FlushPending for BytesAnchor<P, C> {
    #[inline(always)]
    fn flush_pending(&mut self) {
        BytesAnchor::flush_pending(self);
    }
}

/// A zero-copy view of one received message, still in the ring.
///
/// Dereferences to the payload bytes, shared with every other consumer
/// reading the same record — so the view is read-only. The message is
/// released — this anchor's cursor published past the record (and any wrap
/// padding it skipped) with the adaptive, lag-filtered publish (see
/// [`BytesAnchor::drain`] and [`BytesAnchor`]'s `advance`) — when this
/// drops. Copy out anything you need to keep.
///
/// Forgetting the guard (`mem::forget`) does **not** consume the message:
/// the cursor never advances, so the *same message is delivered again* by
/// this anchor's next pop or drain. Safe — but the un-advanced cursor also
/// gates the producer globally, so forget-then-idle stalls the whole ring
/// for every consumer (observers included, once they drain to the frozen
/// tail). That is the gating contract, not a leak.
pub struct Msg<'a, P: WaitStrategy, C: WaitStrategy> {
    anchor: &'a mut BytesAnchor<P, C>,
    /// Payload start, cached when the record was framed (compute
    /// `cursor & mask` once, not on every deref).
    payload: NonNull<u8>,
    len: usize,
}

impl<P: WaitStrategy, C: WaitStrategy> core::ops::Deref for Msg<'_, P, C> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `payload` points at this record's `len` payload bytes,
        // which are contiguous, in bounds, and fully published; the producer
        // cannot overwrite them until this anchor's cursor advances (on drop
        // of this guard) — the gate-protected plain-read argument in the
        // module docs. Other consumers only ever read.
        unsafe { std::slice::from_raw_parts(self.payload.as_ptr(), self.len) }
    }
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for Msg<'_, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Release the record (the skipped wrap padding was already folded
        // into the private cursor by `next_msg`; `advance` publishes both
        // together). See `BytesAnchor::advance` for the adaptive publish
        // and the lag-filtered starving release.
        self.anchor.advance(record_len(self.len) as u64);
    }
}

/// A lossy pure-reader handle of a [`BytesRingBuffer`] — broadcast_bytes'
/// consumer, verbatim: private byte position, private tail cache, its
/// **own** wait-strategy instance, and nothing the producer or any other
/// consumer ever looks at. `Send` but not `Clone`; create more with
/// [`subscribe_observer`](Self::subscribe_observer).
///
/// An observer that falls a full lap behind loses bytes instead of gating
/// anybody, detects the loss with an exact byte count
/// ([`PopError::Lagged`]), and repositions to the latest record — the one
/// boundary the producer guarantees (no slack knob: an arbitrary byte
/// offset is not a record boundary). Dropping an observer is a no-op for
/// everyone else.
pub struct BytesObserver<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the byte buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<u8>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next byte position to read — always a record boundary (join at tail,
    /// advance by whole records, reposition to `latest`: all boundaries).
    pos: u64,
    /// Cached snapshot of the producer's unified cursor.
    tail_cache: u64,
    /// This observer's own wait strategy instance ([`SelfTimed`] by
    /// construction — waiting is purely local, no notify ever arrives).
    wait: C,
    /// The three counters plus the closed word (cached raw pointers;
    /// **loads only** — the whole observer path is write-free).
    tail_intent: NonNull<AtomicU64>,
    latest: NonNull<AtomicU64>,
    tail: NonNull<AtomicU64>,
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc`, or the shm mapping —
    /// **read-only** for observers attached through `attach_shm_observer`).
    backing: ObserverBacking<P, C>,
}

// SAFETY: the observer only touches observer-private state plus atomics; the
// cached pointers reference state the backing keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesObserver<P, C>
{
}

impl<P, C> BytesObserver<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until a message is available, then dequeue it into a fresh
    /// `Vec` by validated copy — the allocating convenience form of
    /// [`pop_into`](Self::pop_into).
    ///
    /// Returns `Err(`[`PopError::Lagged`]`)` if the producer lapped this
    /// observer (the position has already been repositioned to the latest
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
    /// warmed up. On `Err`, `out`'s contents are unspecified.
    ///
    /// Errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn pop_into(&mut self, out: &mut Vec<u8>) -> Result<(), PopError> {
        // Clear at entry (the broadcast_bytes shape): the doc promises
        // "cleared", and a reposition can error out of `read_record` before
        // its own clear runs.
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

    /// Jump this observer to the current tail, abandoning everything
    /// published but unread. Returns how many framed **bytes** were skipped.
    #[inline]
    pub fn skip_to_latest(&mut self) -> u64 {
        let tail = self.refresh();
        let skipped = tail.saturating_sub(self.pos);
        // Never move backwards: a reposition can transiently put `pos` ahead
        // of a stale tail observation.
        self.pos = self.pos.max(tail);
        skipped
    }

    /// How far this observer trails the producer, in framed **bytes**
    /// (headers and wrap padding included): `tail - position` per a fresh
    /// tail read (saturating). `0` means fully caught up; a lag of
    /// `capacity` or more means the next pop will report
    /// [`PopError::Lagged`].
    #[inline]
    pub fn lag(&self) -> u64 {
        // SAFETY: `tail` points into the live shared state.
        unsafe { self.tail.as_ref() }
            .load(Ordering::Acquire)
            .saturating_sub(self.pos)
    }

    /// Subscribe a further observer; its join point is the current tail.
    /// Never fails (see [`BytesProducer::subscribe_observer`]). Observers
    /// cannot subscribe anchors — anchors join from the [`BytesProducer`]
    /// or a [`BytesAnchor`]. On a shared-memory ring the sibling shares
    /// this observer's mapping (read-only when this observer's is).
    pub fn subscribe_observer(&self) -> BytesObserver<P, C> {
        match &self.backing {
            ObserverBacking::Heap(shared) => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this capacity
            // when this handle was constructed.
            ObserverBacking::Shm {
                region,
                max_anchors,
            } => unsafe {
                BytesObserver::from_shm(Arc::clone(region), (self.mask + 1) as usize, *max_anchors)
            },
        }
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 16).
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
        // SAFETY: `tail` points into the live shared state.
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
    /// tail once more and report [`PopError::Closed`] only if genuinely
    /// drained.
    #[inline]
    fn check_closed(&mut self) -> Result<(), PopError> {
        // SAFETY: `closed` points into the live shared state.
        if unsafe { self.closed.as_ref() }.load(Ordering::Acquire) != 0 {
            self.refresh();
            // Drained is `<=`, not `==` (the broadcast_bytes shape): a
            // position transiently ahead of the committed tail must still
            // terminate the drain, never livelock it.
            if self.tail_cache <= self.pos {
                return Err(PopError::Closed);
            }
        }
        Ok(())
    }

    /// Spin/park (per this observer's own wait strategy) until a record is
    /// available or the ring is closed and drained. Spins on the shared
    /// unified cursor (one line, stored once per push), never on the
    /// intent/latest lines.
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
            // SAFETY: the pointers reference live shared state the `Arc`
            // keeps alive.
            let (tail, closed) = unsafe { (self.tail.as_ref(), self.closed.as_ref()) };
            let pos = self.pos;
            self.wait
                .wait(|| tail.load(Ordering::Acquire) > pos || closed.load(Ordering::Acquire) != 0);
        }
    }

    /// The validated read at the current position (the caller established
    /// `tail > pos`): window-check **before parsing**, parse, copy,
    /// re-check — or detect the lap and reposition. Broadcast_bytes'
    /// out-of-band protocol, verbatim. Note the window check is against
    /// `tail_intent`, which the producer only advances *after* passing the
    /// gate — a stalled producer keeps `intent == tail`, so an observer
    /// behind a frozen tail always passes here [audit F3].
    #[inline]
    fn read_record(&mut self, out: &mut Vec<u8>) -> Result<(), PopError> {
        let capacity = self.mask + 1;
        let base = self.buf.as_ptr();
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
            // SAFETY: `tail_intent` points into the live shared state.
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
                // construction; genuine padding is never followed by
                // padding, so this branch runs at most once per frame.
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
    #[cold]
    fn reposition(&mut self) -> PopError {
        // Refresh the tail first (the next pop's availability check), then
        // take the jump target.
        self.refresh();
        // SAFETY: `latest` points into the live shared state.
        let latest = unsafe { self.latest.as_ref() }.load(Ordering::Acquire);
        // `latest` may transiently exceed the tail loaded above (a fresh
        // push's `latest` landed between the two loads; it is stored
        // first). Do NOT clamp to the refreshed tail — a tail value is a
        // frame *end*, a record start only when the next frame carries no
        // padding, so clamping would land repositions off record starts
        // (see `crate::broadcast_bytes::reposition`). Landing ahead is safe
        // as-is: the availability check holds reads back until a tail
        // covers the position, and the `check_closed` drained test is `<=`.
        // Never move backwards: a stale observation clamps to the current
        // position.
        let new_pos = latest.max(self.pos);
        let missed_bytes = new_pos - self.pos;
        self.pos = new_pos;
        PopError::Lagged { missed_bytes }
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). The handles built here are the ordinary `BytesProducer`/
// `BytesAnchor`/`BytesObserver` types over region pointers — the hot paths
// are byte-identical to the heap ring's; only the backing seam differs.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<P: WaitStrategy, C: WaitStrategy> BytesProducer<P, C> {
    /// Build a producer over a validated shm region. Seeds `next_seq` from
    /// the live unified cursor (an attached or recovered producer resumes
    /// exactly after the last *committed* frame), the gating cache with an
    /// always-gating value [M-F17] — then runs **one real table rescan**
    /// before returning (so `is_empty` reflects the live table from the
    /// start) — and carries `intent_floor` = `max(tail_intent, tail)` as
    /// sampled by the attach, which keeps declared frontiers monotonic
    /// across producer sessions.
    ///
    /// # Safety
    ///
    /// The backing's region must be a validated anchored byte ring of
    /// exactly this `capacity` (`create`/`open` in `crate::shm`), the
    /// backing must hold the producer lease, and `intent_floor` must be at
    /// least the region's current `max(tail_intent, tail)`.
    pub(crate) unsafe fn from_shm(
        backing: Box<crate::shm::GateShmProducer<C>>,
        capacity: usize,
        intent_floor: u64,
    ) -> Self {
        let region = backing.region();
        let tail_intent = NonNull::from(region.bcast_intent());
        let latest = NonNull::from(region.bcast_latest());
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let starving = NonNull::from(region.anch_starving());
        let buf = region.anch_buffer(backing.table_offset(), backing.max_slots());
        // SAFETY: `tail` references the live mapping (per contract).
        let next_seq = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        let mut producer = BytesProducer {
            buf,
            mask: capacity as u64 - 1,
            next_seq,
            // Always-gating seed (lag == capacity): the pre-scan value only
            // — the rescan below replaces it before anything reads it
            // [M-F17].
            cached_min: next_seq.wrapping_sub(capacity as u64),
            cached_cursors: Vec::new(),
            raised_starving: false,
            intent_floor,
            tail_intent,
            latest,
            tail,
            closed,
            starving,
            backing: ProducerBacking::Shm(backing),
        };
        // One real registry rescan at construction (see the element ring's
        // twin for the phantom-full livelock this prevents).
        producer.rescan(1);
        producer
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<P: WaitStrategy, C: WaitStrategy> BytesAnchor<P, C> {
    /// Build an anchor over a claimed table slot. `read_cursor` is the join
    /// point from the claim choreography (or the recovery resume point) —
    /// always a record boundary.
    ///
    /// # Safety
    ///
    /// As for [`BytesProducer::from_shm`]; the backing must hold a slot
    /// claimed by the `crate::shm` claim choreography whose cursor word
    /// currently holds (the sentinel-guarded image of) `read_cursor`.
    pub(crate) unsafe fn from_shm(
        backing: Box<crate::shm::GateShmConsumer<P, C>>,
        capacity: usize,
        read_cursor: u64,
    ) -> Self {
        let region = backing.region();
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let starving = NonNull::from(region.anch_starving());
        let cursor_slot =
            NonNull::from(region.anch_slot_cursor(backing.table_offset(), backing.slot()));
        let buf = region.anch_buffer(backing.table_offset(), backing.max_slots());
        BytesAnchor {
            buf,
            mask: capacity as u64 - 1,
            cursor_slot,
            tail,
            closed,
            starving,
            read_cursor,
            published: guard_sentinel(read_cursor),
            tail_cache: read_cursor,
            backing: AnchorBacking::Shm(backing),
        }
    }

    /// The anchor-table slot this handle occupies in its shared-memory
    /// region, or `None` for a heap ring (see
    /// [`shm_slot_epoch`](Self::shm_slot_epoch)).
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot(&self) -> Option<usize> {
        self.shm_slot_epoch().map(|(slot, _)| slot)
    }

    /// The anchor-table `(slot, epoch)` this handle occupies in its
    /// shared-memory region, or `None` for a heap ring — the pair
    /// [`force_detach_anchor`](BytesRingBuffer::force_detach_anchor) takes
    /// (see the element ring's
    /// [`Anchor::shm_slot_epoch`](crate::anchored::Anchor::shm_slot_epoch)).
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot_epoch(&self) -> Option<(usize, u32)> {
        match &self.backing {
            AnchorBacking::Heap { .. } => None,
            AnchorBacking::Shm(backing) => Some((backing.slot(), backing.epoch())),
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<P: WaitStrategy, C: WaitStrategy> BytesObserver<P, C> {
    /// Build an observer over a (typically read-only) mapping of a
    /// validated shm region: pure reader state — no lease, no table slot,
    /// nothing written, ever. The join point is the unified cursor at this
    /// call — always a record boundary.
    ///
    /// # Safety
    ///
    /// The region must be a validated anchored byte ring of exactly this
    /// `capacity` and `max_anchors`.
    pub(crate) unsafe fn from_shm(
        region: Arc<crate::shm::ShmRegion>,
        capacity: usize,
        max_anchors: usize,
    ) -> Self {
        let tail_intent = NonNull::from(region.bcast_intent());
        let latest = NonNull::from(region.bcast_latest());
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let buf = region.anch_buffer(crate::shm::ANCH_BYTES_TABLE_OFFSET, max_anchors);
        // SAFETY: `tail` references the live mapping (per contract).
        let pos = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        BytesObserver {
            buf,
            mask: capacity as u64 - 1,
            pos,
            tail_cache: pos,
            wait: C::default(),
            tail_intent,
            latest,
            tail,
            closed,
            backing: ObserverBacking::Shm {
                region,
                max_anchors,
            },
        }
    }
}

/// Subscribe an anchor through a live shm handle — the byte-ring face of
/// [`crate::anchored`]'s `shm_subscribe_anchor` (same claim choreography and
/// generation re-check, over the byte kind's table).
///
/// # Safety
///
/// `region` must be the validated anchored byte region (`capacity` bytes,
/// `max_anchors` table slots) the calling handle was built over.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
unsafe fn shm_subscribe_anchor<P, C>(
    region: &Arc<crate::shm::ShmRegion>,
    max_anchors: usize,
    capacity: u64,
) -> Result<BytesAnchor<P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    const TABLE: usize = crate::shm::ANCH_BYTES_TABLE_OFFSET;
    let region = Arc::clone(region);
    // Seqlock snapshot before the claim (see the element ring's twin).
    let generation = region.generation();
    if generation & 1 == 1 {
        return Err(SubscribeError::Closed);
    }
    if region.bcast_closed().load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }
    let claim =
        crate::shm::claim_table_slot(&region, TABLE, max_anchors).ok_or(SubscribeError::Full)?;
    // Post-claim re-check with the generation-conditional rollback (a leak,
    // never a clobber — see the element ring's twin).
    if region.generation() != generation {
        crate::shm::release_table_claim(&region, TABLE, &claim, generation);
        return Err(SubscribeError::Closed);
    }
    let joined = claim.joined;
    let backing = Box::new(crate::shm::GateShmConsumer::new(
        region,
        claim,
        max_anchors,
        TABLE,
    ));
    // SAFETY: forwarded caller contract; the claim choreography just ran.
    Ok(unsafe { BytesAnchor::from_shm(backing, capacity as usize, joined) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_len_is_8_aligned_header_inclusive() {
        assert_eq!(record_len(0), 8);
        assert_eq!(record_len(4), 8);
        assert_eq!(record_len(5), 16);
        assert_eq!(record_len(12), 16);
        assert_eq!(record_len(13), 24);
    }

    #[test]
    fn max_message_len_is_capacity_over_8() {
        assert_eq!(max_message_len(16), 2);
        assert_eq!(max_message_len(64), 8);
        assert_eq!(max_message_len(1024), 128);
        for cap in [16usize, 64, 1024, 4096] {
            assert!(record_len(max_message_len(cap)) <= cap);
        }
    }

    /// The free-run soundness bound behind the 16-byte capacity floor: the
    /// empty-registry gating default grants `capacity - 1` bytes, so the
    /// widest legal frame must need strictly less — at 8 bytes it would not
    /// (the only frame is the whole ring), at 16 and above it always does.
    #[test]
    fn max_record_span_fits_under_the_free_run_grant() {
        let mut cap = MIN_CAPACITY;
        while cap <= 1 << 20 {
            assert!(
                max_record_span(cap) < cap,
                "span {} must undercut capacity {cap}",
                max_record_span(cap)
            );
            cap *= 2;
        }
    }

    /// The lag filter's tighter-threshold claim (see `BytesAnchor::advance`):
    /// the flagged span never exceeds `max_record_span`, so with
    /// `capacity / 8` records even the *worst* episode's release threshold
    /// sits well above half the ring — contrast spmc_bytes, whose worst-case
    /// bound degenerates to ALIGN.
    #[test]
    fn starving_filter_threshold_is_meaningful() {
        for cap in [64usize, 256, 1024, 4096] {
            let threshold = cap - max_record_span(cap);
            assert!(
                threshold > cap / 2,
                "threshold {threshold} must exceed half of {cap}"
            );
        }
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
