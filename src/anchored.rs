//! Single-producer ring with **required anchors and lossy observers** — the
//! composed machine: [`crate::spmc`]'s gating registry wrapped around
//! [`crate::broadcast`]'s per-slot seqlock protocol, on one buffer with one
//! unified cursor.
//!
//! Two consumer roles share the stream:
//!
//! * [`Anchor`] — a **required** consumer with the full spmc surface
//!   ([`pop_ref`](Anchor::pop_ref) `&T` borrows, copy-out
//!   [`pop`](Anchor::pop), [`drain`](Anchor::drain), `Result<_, `[`Closed`]`>`).
//!   The producer min-gates on the anchors' published cursors, so an anchor
//!   **never loses a message** — and a stalled anchor eventually blocks the
//!   producer. Membership is dynamic through the spmc chunk registry.
//! * [`Observer`] — an unbounded **lossy** pure reader with the broadcast
//!   surface: validated word-atomic copy-out, exact
//!   [`Lagged`](PopError::Lagged) counts on a lap, reposition
//!   [slack](RingBuffer::with_slack), [`skip_to_latest`](Observer::skip_to_latest).
//!   Observers never gate anybody and cost the producer nothing.
//!
//! "At least one consumer must have read" is one `Anchor`. With **zero**
//! anchors the ring degenerates to a pure broadcast: the producer free-runs
//! and observers take losses; the gate's own-cursor default forces a
//! registry rescan at least once per lap, so a joining anchor is noticed in
//! time and, from its join point on, sees every message (the §9.6 free-run
//! join induction — see `docs/design/spmc.md`).
//!
//! # Quick start
//!
//! ```
//! use rust_rb::anchored::{Closed, PopError, RingBuffer};
//!
//! let (mut tx, mut anchor) = RingBuffer::new(8);
//! let mut observer = tx.subscribe_observer();
//!
//! tx.push(1u64);
//! assert_eq!(anchor.pop(), Ok(1)); // lossless, gate-protected
//! assert_eq!(observer.pop(), Ok(1)); // lossy, validated copy
//!
//! drop(tx); // producer drop closes the ring
//! assert_eq!(anchor.pop(), Err(Closed));
//! assert_eq!(observer.pop(), Err(PopError::Closed));
//! ```
//!
//! # Element bound: [`NoUninit`] **and** `Sync`
//!
//! `T: NoUninit` (no padding bytes, no uninit niches) because observers copy
//! payloads in and out word-wise atomically, reading every byte; `T: Sync`
//! because anchors take shared `&T` borrows of the *same* element from
//! several threads at once. `NoUninit` implies `Copy`, so there is no
//! drop-on-overwrite machinery at all.
//!
//! # Closed contract
//!
//! Dropping the [`Producer`] closes the ring. Anchors drain what was
//! published, then pop `Err(`[`Closed`]`)`; observers drain everything still
//! reachable, then pop `Err(`[`PopError::Closed`]`)`. New anchors are
//! refused on a closed ring ([`SubscribeError::Closed`]); new observers
//! always succeed and are born drained.
//!
//! # Gotchas
//!
//! * `mem::forget` on a [`PopRef`] means **redelivery** to that anchor — and
//!   the un-advanced cursor gates the producer globally, so forget-then-idle
//!   stalls the whole ring (observers included, once they drain to the
//!   frozen tail). That is the gating contract, not a leak.
//! * The write slot is **commit-only** (no in-place `uninit` access):
//!   observers race the payload write, so the producer must own the atomic
//!   copy-in — see [`WriteSlot`].
//! * Producer-side [`len`](Producer::len)/[`is_full`](Producer::is_full) are
//!   approximations against the cached gating minimum (they can transiently
//!   over-count, never under-count); anchor-side views are exact.

use std::cell::UnsafeCell;
use std::mem::{size_of, MaybeUninit};
use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::sync::Arc;

use crate::atomic_copy::{read_payload, write_payload};
use crate::cache_padded::CachePadded;
use crate::cursor::round_capacity;
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
use crate::registry::scan_shm_table;
use crate::registry::{
    guard_sentinel, lacks_space, publish_batch_elems as publish_batch, rescan_gate,
    scan_chunk_registry, subscribe_slot, Chunk, FlushOnDrop, FlushPending, DETACHED,
};
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

#[doc(inline)]
pub use crate::broadcast::{NoUninit, PopError};
#[doc(inline)]
pub use crate::spmc::{Closed, SubscribeError};

/// The default reposition slack for observers: `capacity / 8`, clamped to at
/// least 1 (capacity is at least 2 here, so broadcast's capacity-1 special
/// case does not arise).
#[inline]
const fn default_slack(capacity: u64) -> u64 {
    let slack = capacity / 8;
    if slack == 0 {
        1
    } else {
        slack
    }
}

/// The observer lap-recovery target: `tail - capacity + slack`, computed
/// underflow-safe (replicated from the broadcast ring).
#[inline(always)]
const fn reposition_target(tail: u64, capacity: u64, slack: u64) -> u64 {
    tail.saturating_add(slack).saturating_sub(capacity)
}

/// One ring slot: broadcast's per-slot seqlock, verbatim.
///
/// `seq` encodes `2·s + phase` for global sequence `s`: `2s + 1` while
/// message `s` is being written, `2s + 2` once it is published, `0`
/// initially. Observers validate against it; anchors never read it (the
/// gate makes their reads race-free).
///
/// `repr(C)` pins the payload at a word-aligned offset, which the word-wise
/// copy helpers require (and debug-assert).
#[repr(C)]
struct Slot<T> {
    seq: AtomicU64,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// The producer-published cache line: the unified cursor (`tail` ≡ the spmc
/// `write_cursor` — both roles spin on this one line) plus, co-located, the
/// `closed` flag (written once by `Producer::drop`, read only on would-block
/// paths).
struct TailSide {
    tail: AtomicU64,
    closed: AtomicU64,
}

/// The state all handles share on a **heap** ring, kept alive by an `Arc`.
/// The shm variant (design §9.4) keeps this same state in a mapped region
/// instead; the handles reach either through the backing seam below.
struct Shared<T, P, C> {
    slots: Box<[Slot<T>]>,
    /// `capacity - 1`, in the u64 domain of all position arithmetic.
    mask: u64,
    /// The observer reposition slack (validated `< capacity` at construction).
    slack: u64,
    tail_side: CachePadded<TailSide>,
    /// First registry chunk (anchors only), inline; growth cold-appends.
    registry: Chunk,
    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: slot payloads are written only by the single producer, under the
// seqlock bracket. Anchors take shared `&T` borrows of gate-protected slots
// from several threads at once — that is `T: Sync`; observers copy values
// out across threads and the teardown frees producer-written storage — that
// is `T: Send`.
unsafe impl<T: Send + Sync, P: Send + Sync, C: Send + Sync> Sync for Shared<T, P, C> {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl<T: Send + Sync, P: Send + Sync, C: Send + Sync> Send for Shared<T, P, C> {}

impl<T, P, C> Drop for Shared<T, P, C> {
    fn drop(&mut self) {
        // `T: NoUninit` is `Copy`: elements never need dropping. Only the
        // appended registry chunks are freed (the first chunk is inline).
        self.registry.free_appended();
    }
}

/// Builder/namespace for constructing an anchored ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity at runtime (rounded up to
/// the next power of two, minimum 2) and uses [`YieldWait`] on both sides.
/// Pick other strategies with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (gate) strategy, `C` the consumer-side strategy — anchors
/// wait on the shared `C` instance (spmc's choreography, so the producer's
/// publish `notify` reaches them), while each observer carries its **own**
/// `C` instance (broadcast's ownership: the producer never notifies an
/// observer, so a shared instance would be a lie). Both must be
/// [`SelfTimed`].
///
/// The observer reposition slack is set with [`with_slack`](Self::with_slack)
/// (default `capacity / 8`, clamped to at least 1). Anchors never lag, so
/// the slack concerns observers only.
pub struct RingBuffer<T, P = YieldWait, C = YieldWait>(core::marker::PhantomData<(T, P, C)>);

impl<T: NoUninit + Send + Sync> RingBuffer<T> {
    /// Create a ring buffer with the default wait strategies and return its
    /// producer and one initial anchor (subscribe more consumers of either
    /// role from any handle afterwards).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0` or `T` is zero-sized.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/anchor pair
    pub fn new(min_capacity: usize) -> (Producer<T>, Anchor<T>) {
        RingBuffer::<T, YieldWait, YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<T, P, C> RingBuffer<T, P, C>
where
    T: NoUninit + Send + Sync,
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    /// Create a ring buffer with explicit wait strategies and the default
    /// observer slack, and return its producer and one initial anchor.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0` or `T` is zero-sized.
    pub fn with_wait_strategies(min_capacity: usize) -> (Producer<T, P, C>, Anchor<T, P, C>) {
        let capacity = round_capacity(min_capacity, 2);
        Self::build(capacity, default_slack(capacity as u64) as usize)
    }

    /// Create a ring buffer with an explicit observer reposition `slack`.
    ///
    /// After a lap an observer repositions to `tail - capacity + slack`:
    /// `capacity - slack` messages are immediately readable and the producer
    /// must advance at least `slack` before that observer can lag again.
    /// Anchors are unaffected (they never lag).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`, if `slack >= capacity` (after
    /// power-of-two rounding), or if `T` is zero-sized.
    pub fn with_slack(min_capacity: usize, slack: usize) -> (Producer<T, P, C>, Anchor<T, P, C>) {
        let capacity = round_capacity(min_capacity, 2);
        assert!(slack < capacity, "slack must be less than the capacity");
        Self::build(capacity, slack)
    }

    fn build(capacity: usize, slack: usize) -> (Producer<T, P, C>, Anchor<T, P, C>) {
        assert!(
            size_of::<T>() != 0,
            "zero-sized element types are not supported"
        );
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || Slot {
            seq: AtomicU64::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        });

        let shared = Arc::new(Shared {
            slots: slots.into_boxed_slice(),
            mask: capacity as u64 - 1,
            slack: slack as u64,
            tail_side: CachePadded::new(TailSide {
                tail: AtomicU64::new(0),
                closed: AtomicU64::new(0),
            }),
            registry: Chunk::new(),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        let anchor = subscribe_from(&shared).expect("a fresh ring is not closed");
        // The buffer pointer is derived from the whole-slice `as_ptr` (not a
        // first-element reference) so it keeps provenance over every slot.
        let buf = NonNull::new(shared.slots.as_ptr().cast_mut()).expect("buffer is non-null");
        let producer = Producer {
            buf,
            mask: shared.mask,
            next_seq: 0,
            cached_min: 0,
            cached_cursors: Vec::new(),
            tail: NonNull::from(&shared.tail_side.tail),
            closed: NonNull::from(&shared.tail_side.closed),
            backing: ProducerBacking::Heap(shared),
        };
        (producer, anchor)
    }
}

/// Register a new anchor on live shared state — spmc's `addSequences`
/// choreography [M-F2], provided by the shared gating engine
/// ([`subscribe_slot`]): claim, bitmap RMW strictly before the `SeqCst`
/// fence (the d0549dc regression), join point = the post-fence re-read of
/// the unified cursor.
fn subscribe_from<T, P, C>(shared: &Arc<Shared<T, P, C>>) -> Result<Anchor<T, P, C>, SubscribeError>
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

    let buf = NonNull::new(shared.slots.as_ptr().cast_mut()).expect("buffer is non-null");
    let mask = shared.mask;
    Ok(Anchor {
        buf,
        mask,
        cursor_slot: slot.cursor_slot,
        tail: NonNull::from(&shared.tail_side.tail),
        closed: NonNull::from(&shared.tail_side.closed),
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
fn observe_from<T, P, C>(shared: &Arc<Shared<T, P, C>>) -> Observer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    let shared = Arc::clone(shared);
    let pos = shared.tail_side.tail.load(Ordering::Acquire);
    let buf = NonNull::new(shared.slots.as_ptr().cast_mut()).expect("buffer is non-null");
    Observer {
        buf,
        mask: shared.mask,
        slack: shared.slack,
        pos,
        tail_cache: pos,
        wait: C::default(),
        tail: NonNull::from(&shared.tail_side.tail),
        closed: NonNull::from(&shared.tail_side.closed),
        backing: ObserverBacking::Heap(shared),
    }
}

/// Where the producing handle's shared state lives — the backing seam,
/// mirroring `crate::spmc`'s registry seam (named `*Backing` here because
/// this module's public consumer type is itself called [`Anchor`]): every
/// hot atomic is reached through the handle's cached raw pointers
/// (identical for both variants); the backing is consulted on cold paths
/// (blocking waits, subscribe, the gate-miss rescan walk, teardown) plus
/// the wait-strategy `notify()` accessor on publish (a no-op the spin
/// strategies inline away).
enum ProducerBacking<T, P, C> {
    /// In-process ring: the shared state lives on the heap in an `Arc`; the
    /// anchor registry is the append-only chunk list.
    Heap(Arc<Shared<T, P, C>>),
    /// Cross-process ring: the state lives in a mapped shared region; the
    /// registry is the flat anchor table. Boxed so enabling the feature
    /// does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmProducer<C>>),
}

impl<T, P: WaitStrategy, C: WaitStrategy> ProducerBacking<T, P, C> {
    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            ProducerBacking::Heap(shared) => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerBacking::Shm(backing) => &backing.consumer_wait,
        }
    }

    /// Whether teardown may touch shared state (the ring-wide closed word).
    /// Heap: always. Shm: only the current producer-lease holder in the
    /// constructing process — a crashed producer never gets here, and a fork
    /// child or superseded zombie must not close the successor's ring.
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

/// The [`Anchor`] handle's side of the backing seam (see
/// [`ProducerBacking`]).
enum AnchorBacking<T, P, C> {
    /// Heap ring: the `Arc` plus this anchor's chunk/slot coordinates for
    /// the cold detach (the hot flush goes through the handle's cached
    /// cursor-slot pointer).
    Heap {
        shared: Arc<Shared<T, P, C>>,
        chunk: NonNull<Chunk>,
        slot_idx: usize,
    },
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmConsumer<P, C>>),
}

impl<T, P: WaitStrategy, C: WaitStrategy> AnchorBacking<T, P, C> {
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

    /// Whether teardown may touch shared state. Heap: always. Shm: only the
    /// slot-lease holder in the constructing process — a fork-inherited copy
    /// must not flush over (or free) the parent's live slot, and a zombie
    /// whose slot was reset by `recover_shm` must not resurrect it.
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
    /// already flushed and stored the cursor sentinel). Heap: clear the
    /// bitmap bit. Shm: return the table slot (a guarded control-word CAS —
    /// it fails harmlessly if the slot was force-retired) and release the
    /// slot lease. Both then wake a producer blocked on the gate [A-1.3].
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

/// The [`Observer`] handle's side of the backing seam: purely a keep-alive.
/// An observer is a pure reader — dropping it releases nothing and writes
/// nothing, so the shm variant is nothing but the mapping `Arc` (mapped
/// **read-only**: any accidental store in the observer path would be a
/// deterministic SIGSEGV, which is the enforcement).
enum ObserverBacking<T, P, C> {
    Heap(Arc<Shared<T, P, C>>),
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm {
        region: Arc<crate::shm::ShmRegion>,
        /// The table size, needed to re-derive the buffer base when this
        /// observer subscribes a sibling (the region wrapper is geometry-
        /// agnostic).
        max_anchors: usize,
    },
}

/// The producing half of a [`RingBuffer`]: spmc's gate composed with
/// broadcast's write bracket over one unified u64 cursor. `Send` but not
/// `Clone`: exactly one producer, enforced by the type system.
///
/// Dropping the producer **closes** the ring: anchors and observers drain
/// what was published and then see their role's closed error.
pub struct Producer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next sequence to write (private; equals the published tail between
    /// pushes — a claim does not advance it until committed).
    next_seq: u64,
    /// Cached minimum of the active anchors' cursors — the gate. A lower
    /// bound; the fast-path space check touches no shared line.
    cached_min: u64,
    /// Per-slot cached anchor cursors, mirroring the registry geometry.
    /// Monotonicity makes every cached value a permanent lower bound — for
    /// later occupants of the slot too [P-F3].
    cached_cursors: Vec<[u64; crate::registry::CHUNK_SLOTS]>,
    /// The shared unified cursor (cached raw pointer; heap: into the `Arc`,
    /// shm: into the mapped region — the hot publish path is identical).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (written once, on drop).
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive, carries the wait strategies, and
    /// names the registry (heap chunks vs shm table) for the cold paths.
    backing: ProducerBacking<T, P, C>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the backing keeps alive. `T: Send + Sync`
// per the shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send + Sync, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Producer<T, P, C>
{
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for Producer<T, P, C> {
    fn drop(&mut self) {
        // Flag-then-notify [A-1.1]: an anchor that checked the flag just
        // before this store is parked in a wait whose predicate re-checks
        // `closed`, and the notify wakes it. Observers are `SelfTimed` and
        // re-check on their own; the notify is for anchors. Guarded for shm
        // (heap: constant true): only a graceful drop by the live lease
        // holder sets the ring-wide closed word — a crashed producer never
        // runs this, per the trust model.
        if self.backing.teardown_allowed() {
            // SAFETY: `closed` points into the live shared state.
            unsafe { self.closed.as_ref() }.store(1, Ordering::Release);
            self.backing.consumer_wait().notify();
        }
    }
}

impl<T, P, C> Producer<T, P, C>
where
    T: NoUninit,
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until the slowest anchor frees a slot, then enqueue `value`.
    ///
    /// With zero anchors this never blocks (free-run): observers that have
    /// not kept up are lapped and will observe [`PopError::Lagged`].
    #[inline]
    pub fn push(&mut self, value: T) {
        self.wait_for_space(1);
        self.write(value);
    }

    /// Enqueue `value` without blocking. Returns `Err(value)` if the ring is
    /// gated (full for the slowest anchor) after one full registry rescan.
    ///
    /// "Full" is judged against the anchors' *published* progress; while an
    /// anchor defers publishes in the backed-up regime this can spuriously
    /// fail with up to `capacity / 8` (max 64) slots consumed but not yet
    /// published. A blocking [`push`](Self::push) is woken as soon as the
    /// gating anchor flushes.
    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if !self.has_space(1) {
            return Err(value);
        }
        self.write(value);
        Ok(())
    }

    /// Block until there is room, then return a claim on the next slot.
    /// Publish with [`WriteSlot::commit`]; dropping the claim uncommitted
    /// publishes nothing (see [`WriteSlot`]).
    #[inline]
    pub fn claim(&mut self) -> WriteSlot<'_, T, P, C> {
        self.wait_for_space(1);
        WriteSlot { producer: self }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if the ring is
    /// gated.
    #[inline]
    pub fn try_claim(&mut self) -> Option<WriteSlot<'_, T, P, C>> {
        if !self.has_space(1) {
            return None;
        }
        Some(WriteSlot { producer: self })
    }

    /// Subscribe a new anchor. Its join point is the currently published
    /// cursor: it sees only messages published after this call returns, and
    /// **all** of them — even if the producer was free-running (the §9.6
    /// join induction; no anchor-side validation is needed or performed).
    ///
    /// Cold: the producer's gating caches pick the newcomer up on the next
    /// rescan, which the gating default forces at least once per lap.
    ///
    /// On a shared-memory ring the anchor table is fixed at creation, so
    /// this can additionally fail with [`SubscribeError::Full`].
    pub fn subscribe_anchor(&self) -> Result<Anchor<T, P, C>, SubscribeError> {
        match &self.backing {
            ProducerBacking::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this `T` and
            // capacity when this handle was constructed.
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
    /// **read-write** mapping (same process, same pages) — the read-only
    /// enforcement story belongs to
    /// [`attach_shm_observer`](RingBuffer::attach_shm_observer), which maps
    /// the region afresh with `PROT_READ`.
    pub fn subscribe_observer(&self) -> Observer<T, P, C> {
        match &self.backing {
            ProducerBacking::Heap(shared) => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this `T` and
            // capacity when this handle was constructed; the slack is the
            // create-time header field every observer inherits.
            ProducerBacking::Shm(backing) => {
                let region = Arc::clone(backing.region());
                let slack = region.bcast_slack();
                unsafe {
                    Observer::from_shm(region, (self.mask + 1) as usize, slack, backing.max_slots())
                }
            }
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
    /// full registry rescan. Zero shared loads in the common case.
    #[inline(always)]
    fn has_space(&mut self, needed: u64) -> bool {
        if !lacks_space(self.next_seq, needed, self.cached_min, self.mask + 1) {
            return true;
        }
        self.rescan(needed)
    }

    /// Spin/park (per the producer wait strategy) until the gate opens.
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
    /// geometry). Returns whether `needed` slots are now free.
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

    /// Reference to the slot sequence `s` maps to (in bounds by masking).
    #[inline(always)]
    fn slot(&self, s: u64) -> &Slot<T> {
        // SAFETY: `s & mask` is in `0..capacity`; `buf` is the live buffer
        // the `Arc` keeps alive.
        unsafe { &*self.buf.as_ptr().add((s & self.mask) as usize) }
    }

    /// Common tail of `push`/`try_push`/`commit` — the gate already passed.
    /// This is broadcast's write bracket followed by the unified-cursor
    /// publish, in the normative §9.2 order: invalidate → `Release` fence →
    /// word-atomic payload → `Release` slot publish → **cursor LAST**
    /// (`Release`) — observers' window/validation invariants assume the
    /// cursor never runs ahead of a published slot.
    #[inline(always)]
    fn write(&mut self, value: T) {
        let s = self.next_seq;
        let slot = self.slot(s);
        // Invalidate: observers reading the old occupant now fail
        // revalidation. Relaxed suffices — the fence orders it [M-F10].
        slot.seq.store(2 * s + 1, Ordering::Relaxed);
        fence(Ordering::Release);
        // SAFETY: single producer, and the gate confirmed every anchor
        // published its way past `s - capacity`, so no anchor borrow of this
        // slot can exist; observers race only through atomics (the strict
        // copy) and revalidate the generation.
        unsafe { write_payload(slot.data.get(), &value) };
        // Publish the slot: an exact-match observer accepts `2s + 2`.
        slot.seq.store(2 * s + 2, Ordering::Release);
        self.next_seq = s + 1;
        // Publish the frontier — per push, cursor strictly LAST. Both roles
        // spin on this one line; per-push publish also keeps `Lagged`
        // counts, join points, and the §9.6 induction exact.
        // SAFETY: `tail` points into the live shared state.
        unsafe { self.tail.as_ref() }.store(self.next_seq, Ordering::Release);
        // Wake anchors blocked on data (a no-op for the spin strategies);
        // observers are `SelfTimed` and never wait for a notify.
        self.backing.consumer_wait().notify();
    }

    /// Number of elements queued ahead of the slowest anchor, per the
    /// producer's **cached** gating view. An approximation: refreshed only
    /// on gate misses, it can transiently over-count (and reports at least 1
    /// after a free-run); it never under-counts.
    #[inline]
    pub fn len(&self) -> usize {
        self.next_seq.wrapping_sub(self.cached_min) as usize
    }

    /// Whether the ring looks empty per the producer's cached view (see
    /// [`len`](Self::len)).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring looks full (a `push` would block) per the producer's
    /// cached view. May transiently report `true` while anchors defer their
    /// cursor publishes; never reports `false` for a truly gated ring.
    #[inline]
    pub fn is_full(&self) -> bool {
        lacks_space(self.next_seq, 1, self.cached_min, self.mask + 1)
    }

    /// Number of messages published so far (the ring's frontier).
    /// Producer-local and exact.
    #[inline]
    pub fn tail(&self) -> u64 {
        self.next_seq
    }

    /// The ring's true capacity (the requested minimum rounded up to a power
    /// of two, minimum 2).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }
}

/// A claimed, not-yet-published slot — **commit-only**, unlike the spmc
/// ring's write slot.
///
/// There is deliberately no `uninit()`/`commit_init()` in-place path:
/// observers race the payload write, so every byte must go in through the
/// strict word-wise **atomic** copy the producer controls. Handing out
/// `&mut MaybeUninit<T>` would let the user write with plain stores, which
/// is undefined behaviour against a concurrent observer's atomic copy-out.
///
/// Dropping the slot uncommitted publishes nothing: neither the slot's
/// seqlock word nor the cursor moved, so no consumer of either role can
/// observe the abandoned claim.
pub struct WriteSlot<'a, T: NoUninit, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut Producer<T, P, C>,
}

impl<T: NoUninit, P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, T, P, C> {
    /// Move `value` into the slot and publish it (equivalent to `push` on a
    /// slot that is already reserved).
    #[inline]
    pub fn commit(self, value: T) {
        let Self { producer } = self;
        producer.write(value);
    }
}

/// A required consumer of a [`RingBuffer`] — spmc's consumer over the
/// unified u64 cursor. Owns a private read cursor and one registry slot;
/// the producer min-gates on it, so an anchor **sees every message**.
/// `Send` but not `Clone`; create more with
/// [`subscribe_anchor`](Self::subscribe_anchor).
///
/// Dropping the anchor detaches it: it stops gating the producer and wakes
/// a producer blocked on it.
pub struct Anchor<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// This anchor's cursor word — the hot flush target.
    cursor_slot: NonNull<AtomicU64>,
    /// The producer's unified cursor (cached raw pointer).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (read on would-block paths only).
    closed: NonNull<AtomicU64>,
    /// Next sequence to read (private to this thread).
    read_cursor: u64,
    /// The value of `read_cursor` last published to the registry slot (see
    /// [`advance_one`](Self::advance_one) for the adaptive publish rule).
    published: u64,
    /// Cached snapshot of the producer's unified cursor.
    tail_cache: u64,
    /// Keeps the ring's memory alive, carries the wait strategies, and
    /// names the registry (heap chunk coordinates vs shm table slot) for
    /// the cold detach.
    backing: AnchorBacking<T, P, C>,
}

// SAFETY: the anchor only touches anchor-private state plus atomics; the
// cached pointers reference state the backing keeps alive. `T: Send + Sync`
// per the shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send + Sync, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Anchor<T, P, C>
{
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for Anchor<T, P, C> {
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

impl<T, P, C> Anchor<T, P, C>
where
    T: NoUninit,
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until an element is available, then dequeue it by copy.
    ///
    /// Returns `Err(`[`Closed`]`)` only when the producer has been dropped
    /// *and* every published message has been consumed.
    #[inline]
    pub fn pop(&mut self) -> Result<T, Closed> {
        self.wait_for_item()?;
        Ok(self.read())
    }

    /// Dequeue an element by copy without blocking. `Ok(None)` means
    /// empty-but-alive; `Err(`[`Closed`]`)` means closed **and** drained.
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<T>, Closed> {
        if self.has_item() {
            return Ok(Some(self.read()));
        }
        self.check_closed()?;
        if self.available_cached() != 0 {
            // The close re-check refreshed the cursor and found a final
            // message published just before the producer dropped.
            return Ok(Some(self.read()));
        }
        Ok(None)
    }

    /// Block until an element is available, then return a zero-copy view of
    /// it in the buffer. The slot is released (this anchor's cursor
    /// advances) when the returned [`PopRef`] drops; the element itself
    /// stays in the ring for the other consumers.
    #[inline]
    pub fn pop_ref(&mut self) -> Result<PopRef<'_, T, P, C>, Closed> {
        self.wait_for_item()?;
        Ok(PopRef { anchor: self })
    }

    /// Non-blocking [`pop_ref`](Self::pop_ref). `Ok(None)` means
    /// empty-but-alive; `Err(`[`Closed`]`)` means closed **and** drained.
    #[inline]
    pub fn try_pop_ref(&mut self) -> Result<Option<PopRef<'_, T, P, C>>, Closed> {
        if self.has_item() {
            return Ok(Some(PopRef { anchor: self }));
        }
        self.check_closed()?;
        if self.available_cached() != 0 {
            return Ok(Some(PopRef { anchor: self }));
        }
        Ok(None)
    }

    /// Consume up to one publish batch (`capacity / 8`, max 64) of available
    /// elements, calling `f` on each in place, and return how many were
    /// consumed. The read cursor is published **once**, after the last
    /// element — one `Release` store (and one wake-up) for the whole batch.
    ///
    /// The private cursor advances over each element *before* `f` sees it,
    /// and the publish happens even if `f` panics (an unwound drain never
    /// re-delivers already-processed elements to this anchor). The borrow
    /// handed to `f` stays valid throughout: the producer cannot reuse the
    /// batch's slots until the final publish, which is strictly after `f`.
    pub fn drain<F: FnMut(&T)>(&mut self, mut f: F) -> usize {
        // Unconditionally refresh: the contract is "what is currently in the
        // ring", which a stale non-empty cache must not bound.
        let end = self.refresh();
        let available = end.wrapping_sub(self.read_cursor);
        if available == 0 {
            return 0;
        }
        let count = available.min(publish_batch(self.mask + 1));

        // Publish on exit — including an unwind out of `f` (the engine's
        // `FlushOnDrop` guard over this anchor's `flush_pending`).
        let guard = FlushOnDrop(self);
        for _ in 0..count {
            let data = guard.0.slot().data.get();
            // Advance before the callback: the element counts as consumed
            // even if `f` unwinds.
            guard.0.read_cursor = guard.0.read_cursor.wrapping_add(1);
            // SAFETY: every seq below `end` is published, so the slot holds
            // an initialized `T`; the borrow is race-free per the gate (see
            // `read` for the mixed-atomicity argument) and stays valid until
            // the guard's final publish, strictly after `f`.
            f(unsafe { (*data).assume_init_ref() });
        }
        count as usize
    }

    /// Subscribe a further anchor; see [`Producer::subscribe_anchor`].
    pub fn subscribe_anchor(&self) -> Result<Anchor<T, P, C>, SubscribeError> {
        match &self.backing {
            AnchorBacking::Heap { shared, .. } => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this `T` and
            // capacity when this handle was constructed.
            AnchorBacking::Shm(backing) => unsafe {
                shm_subscribe_anchor(backing.region(), backing.max_slots(), self.mask + 1)
            },
        }
    }

    /// Subscribe an observer; see [`Producer::subscribe_observer`].
    pub fn subscribe_observer(&self) -> Observer<T, P, C> {
        match &self.backing {
            AnchorBacking::Heap { shared, .. } => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: as for [`Producer::subscribe_observer`]'s shm arm.
            AnchorBacking::Shm(backing) => {
                let region = Arc::clone(backing.region());
                let slack = region.bcast_slack();
                unsafe {
                    Observer::from_shm(region, (self.mask + 1) as usize, slack, backing.max_slots())
                }
            }
        }
    }

    /// Number of elements available to this anchor. Exact on this side:
    /// uses the anchor's private cursor, which is always current.
    #[inline]
    pub fn len(&self) -> usize {
        // SAFETY: `tail` points into the live shared state.
        unsafe { self.tail.as_ref() }
            .load(Ordering::Acquire)
            .wrapping_sub(self.read_cursor) as usize
    }

    /// Whether this anchor has nothing to read. Exact on this side.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The ring's true capacity (the requested minimum rounded up to a power
    /// of two, minimum 2).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// Elements available per the cached view of the producer's cursor.
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

    /// Check for at least one available element, reloading the producer's
    /// cursor at most once.
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

    /// Reference to the slot the read cursor designates (in bounds by
    /// masking).
    #[inline(always)]
    fn slot(&self) -> &Slot<T> {
        // SAFETY: `read_cursor & mask` is in `0..capacity`; `buf` is the
        // live buffer the `Arc` keeps alive.
        unsafe {
            &*self
                .buf
                .as_ptr()
                .add((self.read_cursor & self.mask) as usize)
        }
    }

    /// Common tail of `pop`/`try_pop`: **plain** copy out, then advance.
    ///
    /// The plain (non-atomic) read of bytes last written by the producer's
    /// *atomic* stores is sound — the novel §9.2 claim, audit-verified:
    /// mixed atomicity only matters for RACING accesses, and this read
    /// races nothing. Happens-before holds both ways: the `Acquire` read of
    /// the unified cursor (`tail >= read_cursor + 1`) synchronizes with the
    /// producer's `Release` cursor store after writing this message, and
    /// the producer's *next* write of this slot (seq `read_cursor +
    /// capacity`) is gate-blocked until this anchor's `Release` cursor
    /// flush is observed by a rescan's `Acquire` fence. Anchors therefore
    /// skip seq validation entirely — they cannot tear.
    #[inline(always)]
    fn read(&mut self) -> T {
        // SAFETY: the read cursor is below the published cursor, so the slot
        // holds a fully initialized `T` (every payload byte was stored), and
        // the access is race-free per the gate argument above.
        let value = unsafe { (*self.slot().data.get()).assume_init_read() };
        self.advance_one();
        value
    }

    /// Release one slot with the adaptive publish (verbatim from the spmc
    /// engine): immediate when caught up or when the ring was observed full
    /// per this side's own cached view (a purely anchor-local check),
    /// batched while backed up.
    #[inline(always)]
    fn advance_one(&mut self) {
        let capacity = self.mask + 1;
        let was_full = self.tail_cache.wrapping_sub(self.read_cursor) > capacity - 1;
        self.read_cursor = self.read_cursor.wrapping_add(1);
        if was_full
            || self.read_cursor == self.tail_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch(capacity)
        {
            self.flush();
        }
    }
}

/// Cursor machinery with no payload involvement — kept free of the
/// `T: NoUninit` bound so the detach path (`Drop`) can flush.
impl<T, P: WaitStrategy, C: WaitStrategy> Anchor<T, P, C> {
    /// Publish the private read cursor to this anchor's registry slot and
    /// wake a producer blocked on the gate (a no-op for spin strategies).
    ///
    /// Guarded by slot-lease ownership on shm rings (heap: no check at all):
    /// every publish path funnels through here, so a zombie handle whose
    /// slot was reset by `recover_shm` can never store over a successor's
    /// cursor. A force-detached zombie still holds its lease and still
    /// stores — onto its RETIRED slot, which no scan reads [A-4.1].
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

impl<T, P: WaitStrategy, C: WaitStrategy> FlushPending for Anchor<T, P, C> {
    #[inline(always)]
    fn flush_pending(&mut self) {
        Anchor::flush_pending(self);
    }
}

/// A zero-copy view of an anchor's next element, still in the buffer.
///
/// Dereferences to `&T` only — never `&mut T`: other anchors may be reading
/// the *same* element concurrently (and observers may be copying it). When
/// the guard drops, this anchor's cursor advances past the element; the
/// element itself is not touched (`T: NoUninit` is `Copy` — there is
/// nothing to drop, ever).
///
/// Forgetting the guard (`mem::forget`) does **not** consume the element:
/// the cursor never advances, so the *same element is delivered again* by
/// this anchor's next pop. Safe — but the un-advanced cursor also gates the
/// producer globally, so forget-then-idle stalls the whole ring for every
/// consumer. That is the gating contract, not a leak.
pub struct PopRef<'a, T: NoUninit, P: WaitStrategy, C: WaitStrategy> {
    anchor: &'a mut Anchor<T, P, C>,
}

impl<T: NoUninit, P: WaitStrategy, C: WaitStrategy> core::ops::Deref for PopRef<'_, T, P, C> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: the read cursor is below the published cursor, so the slot
        // holds an initialized `T`; the producer cannot reuse it until this
        // anchor's cursor advances (on drop of this guard) — the same
        // race-free mixed-atomicity argument as `Anchor::read`. Other
        // readers hold `&T` at most (`T: Sync` by the construction bound).
        unsafe { (*self.anchor.slot().data.get()).assume_init_ref() }
    }
}

impl<T: NoUninit, P: WaitStrategy, C: WaitStrategy> Drop for PopRef<'_, T, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Advance-only [M-F7]: never a destructor — the value stays live for
        // the other consumers (and is `Copy` anyway).
        self.anchor.advance_one();
    }
}

/// A lossy pure-reader handle of a [`RingBuffer`] — broadcast's consumer,
/// verbatim: private position, private tail cache, its **own** wait-strategy
/// instance, and nothing the producer or any other consumer ever looks at.
/// `Send` but not `Clone`; create more with
/// [`subscribe_observer`](Self::subscribe_observer).
///
/// An observer that falls a full lap behind loses messages instead of
/// gating anybody, detects the loss with an exact count
/// ([`PopError::Lagged`]), and repositions to the oldest retained message
/// plus the ring's [slack](RingBuffer::with_slack). Dropping an observer is
/// a no-op for everyone else.
pub struct Observer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Reposition slack (cached).
    slack: u64,
    /// Next position to read (private; `pos <= tail` always).
    pos: u64,
    /// Cached snapshot of the producer's unified cursor.
    tail_cache: u64,
    /// This observer's own wait strategy instance ([`SelfTimed`] by
    /// construction — waiting is purely local, no notify ever arrives).
    wait: C,
    /// The shared unified cursor (loads only — the whole observer path is
    /// write-free).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (loads only, would-block paths).
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc`, or the shm mapping —
    /// **read-only** for observers attached through `attach_shm_observer`).
    backing: ObserverBacking<T, P, C>,
}

// SAFETY: the observer only touches observer-private state plus atomics; the
// cached pointers reference state the backing keeps alive. `T: Send + Sync`
// per the shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send + Sync, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Observer<T, P, C>
{
}

impl<T, P, C> Observer<T, P, C>
where
    T: NoUninit,
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until a message is available, then dequeue it by validated
    /// copy.
    ///
    /// Returns `Err(`[`PopError::Lagged`]`)` if the producer lapped this
    /// observer (the position has already been repositioned — the next pop
    /// proceeds from there), or `Err(`[`PopError::Closed`]`)` once the
    /// producer is dropped **and** everything reachable has been drained.
    #[inline]
    pub fn pop(&mut self) -> Result<T, PopError> {
        self.wait_for_item()?;
        self.read_slot()
    }

    /// Dequeue a message without blocking. `Ok(None)` means
    /// empty-but-alive; the errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<T>, PopError> {
        if self.has_item() {
            return self.read_slot().map(Some);
        }
        self.check_closed()?;
        if self.available() {
            // The close re-check refreshed the tail and found a final
            // message published just before the producer dropped.
            return self.read_slot().map(Some);
        }
        Ok(None)
    }

    /// Jump this observer to the current tail, abandoning everything
    /// published but unread. Returns how many messages were skipped.
    #[inline]
    pub fn skip_to_latest(&mut self) -> u64 {
        // Saturate + never move backwards (mirrors the broadcast twins):
        // a stale tail observation must not underflow the count or drag
        // the position back.
        let tail = self.refresh();
        let skipped = tail.saturating_sub(self.pos);
        self.pos = self.pos.max(tail);
        skipped
    }

    /// How far this observer trails the producer: `tail - position` per a
    /// fresh tail read (saturating). `0` means fully caught up; a lag of
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
    /// Never fails (see [`Producer::subscribe_observer`]). Observers cannot
    /// subscribe anchors — anchors join from the [`Producer`] or an
    /// [`Anchor`]. On a shared-memory ring the sibling shares this
    /// observer's mapping (read-only when this observer's is).
    pub fn subscribe_observer(&self) -> Observer<T, P, C> {
        match &self.backing {
            ObserverBacking::Heap(shared) => observe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the backing's region was validated for this `T` and
            // capacity when this handle was constructed; the sibling
            // inherits the same cached slack.
            ObserverBacking::Shm {
                region,
                max_anchors,
            } => unsafe {
                Observer::from_shm(
                    Arc::clone(region),
                    (self.mask + 1) as usize,
                    self.slack,
                    *max_anchors,
                )
            },
        }
    }

    /// The ring's true capacity (the requested minimum rounded up to a power
    /// of two, minimum 2).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// Whether the cached tail shows an available message.
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

    /// Check for at least one available message, reloading the tail at most
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
            // Drained is `<=`, not `==` (mirrors the broadcast twins): a
            // position past a stale tail must still terminate the drain.
            if self.tail_cache <= self.pos {
                return Err(PopError::Closed);
            }
        }
        Ok(())
    }

    /// Spin/park (per this observer's own wait strategy) until a message is
    /// available or the ring is closed and drained. Spins on the shared
    /// unified cursor (one line, stored once per push), never on a slot's
    /// seqlock word.
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

    /// The seqlock read at the current position (the caller established
    /// `tail > pos`): validate, copy, revalidate — or detect the lap and
    /// reposition. Broadcast's validated copy-out, verbatim.
    #[inline]
    fn read_slot(&mut self) -> Result<T, PopError> {
        let s = self.pos;
        let slot = self.slot(s);
        let expected = 2 * s + 2;
        // Because the unified cursor is Release-stored strictly after the
        // slot publish (cursor LAST) and we Acquire-read `tail > s`, the
        // generation here is at least `expected` — a gated (stalled)
        // producer frontier can never expose an unwritten slot.
        let v1 = slot.seq.load(Ordering::Acquire);
        debug_assert!(v1 >= expected, "slot behind the published tail");
        if v1 == expected {
            let mut out = MaybeUninit::<T>::uninit();
            // SAFETY: the slot was published at least once (generation
            // reached `expected`), so every payload byte is initialized;
            // torn bytes stay `MaybeUninit` until revalidation below.
            unsafe { read_payload(slot.data.get(), &mut out) };
            // Order the payload loads before the revalidating load: fence +
            // relaxed re-load is the sound shape [M-F11].
            fence(Ordering::Acquire);
            let v2 = slot.seq.load(Ordering::Relaxed);
            if v2 == v1 {
                self.pos = s + 1;
                // SAFETY: generation unchanged across the copy — the bytes
                // are the complete, untorn message `s`; `T: NoUninit` makes
                // every byte pattern of a published value initialized data.
                return Ok(unsafe { out.assume_init() });
            }
        }
        // Lapped: the slot moved on to a newer generation (or tore the
        // copy). Reposition first, then report.
        Err(PopError::Lagged {
            missed: self.reposition(),
        })
    }

    /// Lap recovery: jump to `tail - capacity + slack` per a fresh tail read
    /// and return the exact number of messages skipped. Never moves
    /// backwards (a stale tail observation clamps to the current position).
    #[cold]
    fn reposition(&mut self) -> u64 {
        let tail = self.refresh();
        let new_pos = reposition_target(tail, self.mask + 1, self.slack).max(self.pos);
        let missed = new_pos - self.pos;
        self.pos = new_pos;
        missed
    }

    /// Reference to the slot sequence `s` maps to (in bounds by masking).
    #[inline(always)]
    fn slot(&self, s: u64) -> &Slot<T> {
        // SAFETY: `s & mask` is in `0..capacity`; `buf` is the live buffer
        // the `Arc` keeps alive.
        unsafe { &*self.buf.as_ptr().add((s & self.mask) as usize) }
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). The handles built here are the ordinary `Producer`/`Anchor`/
// `Observer` types over region pointers — the hot paths are byte-identical
// to the heap ring's; only the backing seam (registry/keep-alive) differs.
// ---------------------------------------------------------------------------

/// The byte stride of one shm slot: `size_of::<Slot<T>>()` — the `repr(C)`
/// `{ seq: AtomicU64, data: T-storage }` layout the region create/open
/// validate for exact equality (see `crate::broadcast`'s twin for the
/// layout math; the header's `unit_size` records `size_of::<T>()`).
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) fn shm_slot_stride<T>() -> usize {
    size_of::<Slot<T>>()
}

/// Alignment of one shm slot (see [`shm_slot_stride`]); the shm buffer
/// offset is 128-aligned, so alignments above 128 are rejected at
/// construction.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) fn shm_slot_align<T>() -> usize {
    std::mem::align_of::<Slot<T>>()
}

/// The default-slack policy, shared with the shm constructors so they cannot
/// drift from the heap ring's.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) const fn shm_default_slack(capacity: u64) -> u64 {
    default_slack(capacity)
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, P, C> Producer<T, P, C>
where
    T: NoUninit,
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Build a producer over a validated shm region. Seeds `next_seq` from
    /// the live unified cursor and the gating cache with an always-gating
    /// value — an attached or recovered producer never trusts defaults
    /// [M-F17] — then runs **one real table rescan** before returning, so
    /// the seed is only ever the pre-scan value: without it, `&self`
    /// [`len`](Producer::len)/[`is_empty`](Producer::is_empty)/
    /// [`is_full`](Producer::is_full) would report a full ring until the
    /// first push (the `while producer.is_full() {}` livelock the semantics
    /// guide warns about).
    ///
    /// # Safety
    ///
    /// The backing's region must be a validated anchored element ring of
    /// exactly this `T` and `capacity` (`create`/`open` in `crate::shm`),
    /// and the backing must hold the producer lease.
    pub(crate) unsafe fn from_shm(
        backing: Box<crate::shm::GateShmProducer<C>>,
        capacity: usize,
    ) -> Self {
        let region = backing.region();
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let buf = region
            .anch_buffer(backing.table_offset(), backing.max_slots())
            .cast::<Slot<T>>();
        // SAFETY: `tail` references the live mapping (per contract).
        let next_seq = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        let mut producer = Producer {
            buf,
            mask: capacity as u64 - 1,
            next_seq,
            // Always-gating seed (lag == capacity): the pre-scan value only
            // — the rescan below replaces it before anything reads it
            // [M-F17].
            cached_min: next_seq.wrapping_sub(capacity as u64),
            cached_cursors: Vec::new(),
            tail,
            closed,
            backing: ProducerBacking::Shm(backing),
        };
        // One real registry rescan at construction: the gating view (and
        // with it `len`/`is_empty`/`is_full`) reflects the live table from
        // the start instead of lying "full" until the first push.
        producer.rescan(1);
        producer
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, P: WaitStrategy, C: WaitStrategy> Anchor<T, P, C> {
    /// Build an anchor over a claimed table slot. `read_cursor` is the join
    /// point from the claim choreography (or the recovery resume point,
    /// which `recover_shm` stores into the slot before calling this).
    ///
    /// # Safety
    ///
    /// As for [`Producer::from_shm`]; the backing must hold a slot claimed
    /// by the `crate::shm` claim choreography whose cursor word currently
    /// holds (the sentinel-guarded image of) `read_cursor`.
    pub(crate) unsafe fn from_shm(
        backing: Box<crate::shm::GateShmConsumer<P, C>>,
        capacity: usize,
        read_cursor: u64,
    ) -> Self {
        let region = backing.region();
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let cursor_slot =
            NonNull::from(region.anch_slot_cursor(backing.table_offset(), backing.slot()));
        let buf = region
            .anch_buffer(backing.table_offset(), backing.max_slots())
            .cast::<Slot<T>>();
        Anchor {
            buf,
            mask: capacity as u64 - 1,
            cursor_slot,
            tail,
            closed,
            read_cursor,
            published: guard_sentinel(read_cursor),
            tail_cache: read_cursor,
            backing: AnchorBacking::Shm(backing),
        }
    }

    /// The anchor-table slot this handle occupies in its shared-memory
    /// region, or `None` for a heap ring (see
    /// [`shm_slot_epoch`](Self::shm_slot_epoch) for the pair
    /// [`force_detach_anchor`](RingBuffer::force_detach_anchor) takes).
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot(&self) -> Option<usize> {
        self.shm_slot_epoch().map(|(slot, _)| slot)
    }

    /// The anchor-table `(slot, epoch)` this handle occupies in its
    /// shared-memory region, or `None` for a heap ring. The pair identifies
    /// this exact *occupancy* — every claim bumps the slot's epoch — and is
    /// what [`force_detach_anchor`](RingBuffer::force_detach_anchor) takes,
    /// so a watchdog holding a dead anchor's pair can never retire a
    /// healthy successor that re-claimed the slot.
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot_epoch(&self) -> Option<(usize, u32)> {
        match &self.backing {
            AnchorBacking::Heap { .. } => None,
            AnchorBacking::Shm(backing) => Some((backing.slot(), backing.epoch())),
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, P: WaitStrategy, C: WaitStrategy> Observer<T, P, C> {
    /// Build an observer over a (typically read-only) mapping of a
    /// validated shm region: pure reader state — no lease, no table slot,
    /// nothing written, ever. The join point is the unified cursor at this
    /// call.
    ///
    /// # Safety
    ///
    /// The region must be a validated anchored element ring of exactly this
    /// `T`, `capacity`, and `max_anchors`, with `slack` the region's
    /// create-time slack (validated `< capacity`).
    pub(crate) unsafe fn from_shm(
        region: Arc<crate::shm::ShmRegion>,
        capacity: usize,
        slack: u64,
        max_anchors: usize,
    ) -> Self {
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let buf = region
            .anch_buffer(crate::shm::ANCH_ELEMS_TABLE_OFFSET, max_anchors)
            .cast::<Slot<T>>();
        // SAFETY: `tail` references the live mapping (per contract).
        let pos = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        Observer {
            buf,
            mask: capacity as u64 - 1,
            slack,
            pos,
            tail_cache: pos,
            wait: C::default(),
            tail,
            closed,
            backing: ObserverBacking::Shm {
                region,
                max_anchors,
            },
        }
    }
}

/// Subscribe an anchor through a live shm handle: the anchor-table claim
/// choreography (the shm analog of [`subscribe_from`], with the control-word
/// CAS as the [M-F2] registration event) plus handle construction. The `Arc`
/// clone precedes the claim, mirroring [A-2.2]; the post-claim generation
/// re-check (with its generation-conditional rollback) mirrors the attach
/// path in `crate::shm`.
///
/// # Safety
///
/// `region` must be the validated anchored element region (element type
/// `T`, `capacity` slots, `max_anchors` table slots) the calling handle was
/// built over.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
unsafe fn shm_subscribe_anchor<T, P, C>(
    region: &Arc<crate::shm::ShmRegion>,
    max_anchors: usize,
    capacity: u64,
) -> Result<Anchor<T, P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    const TABLE: usize = crate::shm::ANCH_ELEMS_TABLE_OFFSET;
    let region = Arc::clone(region);
    // Seqlock snapshot before the claim: an odd generation is a concurrent
    // `create_shm`/`recover_shm` mid-rewrite — claiming into a table being
    // reset would keep a slot the reset frees out from under us.
    let generation = region.generation();
    if generation & 1 == 1 {
        return Err(SubscribeError::Closed);
    }
    if region.bcast_closed().load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }
    let claim =
        crate::shm::claim_table_slot(&region, TABLE, max_anchors).ok_or(SubscribeError::Full)?;
    // Post-claim re-check: a generation change means the table was reset
    // while we claimed. The rollback is itself generation-conditional, so a
    // re-initialized table is left strictly alone (at worst one leaked slot
    // until the next `recover_shm` — a leak, never a clobber). The ring
    // this handle was subscribed from is gone, which is `Closed`.
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
    Ok(unsafe { Anchor::from_shm(backing, capacity as usize, joined) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_batch_policy() {
        assert_eq!(publish_batch(2), 1);
        assert_eq!(publish_batch(64), 8);
        assert_eq!(publish_batch(1024), 64);
        assert_eq!(
            publish_batch(1 << 20),
            crate::registry::MAX_PUBLISH_BATCH_ELEMS
        );
    }

    #[test]
    fn default_slack_policy() {
        assert_eq!(default_slack(2), 1);
        assert_eq!(default_slack(8), 1);
        assert_eq!(default_slack(16), 2);
        assert_eq!(default_slack(1024), 128);
    }

    #[test]
    fn reposition_target_math() {
        assert_eq!(reposition_target(20, 8, 2), 14);
        assert_eq!(reposition_target(17, 8, 0), 9);
        assert_eq!(reposition_target(1, 8, 2), 0);
        assert_eq!(reposition_target(u64::MAX, 8, 2), u64::MAX - 8);
    }

    #[test]
    fn slot_payload_word_aligned() {
        fn data_offset_of<T>() -> usize {
            let s = Slot::<T> {
                seq: AtomicU64::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            };
            s.data.get() as usize - &s as *const _ as usize
        }
        assert_eq!(data_offset_of::<u8>() % std::mem::align_of::<usize>(), 0);
        assert_eq!(
            data_offset_of::<[u8; 3]>() % std::mem::align_of::<usize>(),
            0
        );
        assert_eq!(data_offset_of::<u128>() % std::mem::align_of::<usize>(), 0);
    }
}
