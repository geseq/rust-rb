//! Single-producer / **multi**-consumer broadcast ring buffer (gating).
//!
//! Every consumer observes **every** message — lossless multicast in the
//! LMAX Disruptor mold. The producer gates on the *slowest* consumer's
//! published cursor: a consumer that stops consuming eventually blocks the
//! producer (that is the contract; the lossy alternative is a separate
//! machine). Consumers never move values out of the ring: reads are `&T`
//! borrows ([`Consumer::pop_ref`]) or clones ([`Consumer::pop`], `T: Clone`),
//! and the producer drops the old occupant when it overwrites a slot.
//!
//! # Quick start
//!
//! ```
//! use rust_rb::spmc::{Closed, RingBuffer};
//!
//! let (mut tx, mut rx) = RingBuffer::new(8);
//! let mut rx2 = tx.subscribe().unwrap(); // dynamic membership
//!
//! tx.push(1u64);
//! assert_eq!(rx.pop(), Ok(1));
//! assert_eq!(rx2.pop(), Ok(1)); // both consumers see every message
//!
//! drop(tx); // producer drop closes the ring
//! assert_eq!(rx.pop(), Err(Closed));
//! ```
//!
//! # Membership
//!
//! Membership is dynamic and unbounded: [`Producer::subscribe`] /
//! [`Consumer::subscribe`] add a consumer whose **join point** is the
//! producer's published cursor at subscribe time — it sees only messages
//! published after that, and all of them. Dropping a consumer detaches it
//! (a departed consumer never gates the producer). With **zero** consumers
//! the producer free-runs: pushes succeed and overwritten values are dropped
//! — there is no retention contract for future subscribers.
//!
//! # Closed contract
//!
//! Dropping the [`Producer`] closes the ring. [`Consumer::pop`] returns
//! `Err(`[`Closed`]`)` only once the producer is gone **and** this consumer
//! has drained every published message; [`Consumer::try_pop`] returns
//! `Ok(None)` for empty-but-alive and `Err(Closed)` for closed-and-drained.
//! The flag is only consulted on would-block paths, so it costs the hot path
//! nothing.
//!
//! # Why it is fast
//!
//! The hot paths are the SPSC ring's, generalized:
//!
//! * **Monotonic masked u64 cursors** with wrap-safe `wrapping_sub`
//!   difference comparisons everywhere (the shared gating engine's cursor
//!   domain — ABA-immune on every target).
//! * **Producer-local gating cache.** The producer keeps a cached minimum of
//!   the consumers' cursors and per-slot cached cursors; the common-case
//!   space check touches no shared line. On a gate miss it walks a bitmap of
//!   active registry slots, reloading **only** the cursors that are actually
//!   blocking (`Relaxed` loads, one trailing `Acquire` fence, so the misses
//!   overlap).
//! * **Adaptive read-cursor publish** per consumer, verbatim from the SPSC
//!   engine: per element while caught up or when the ring was observed full,
//!   batched (capacity/8, max 64) while backed up. The producer's gate
//!   inherits at most *one* consumer's deferral, so the producer-visible
//!   bound is the SPSC one.
//!
//! # Gotchas
//!
//! * `mem::forget` on a [`PopRef`] means **redelivery** of the same element
//!   to that consumer — and, because the un-advanced cursor gates the
//!   producer globally, a forget-then-idle consumer stalls the whole ring.
//!   That is the gating contract, not a leak.
//! * Producer-side [`len`](Producer::len)/[`is_full`](Producer::is_full) are
//!   approximations against the cached gating minimum and can transiently
//!   over-count (never under-count); consumer-side views are exact.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::cursor::round_capacity;
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
use crate::registry::scan_shm_table;
use crate::registry::{
    guard_sentinel, lacks_space, publish_batch_elems as publish_batch, rescan_gate,
    scan_chunk_registry, subscribe_slot, Chunk, FlushOnDrop, FlushPending, DETACHED,
};
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

/// The slot type: a cell the producer writes (and overwrite-drops) and the
/// consumers borrow from, ordered by the cursor atomics.
type Slot<T> = UnsafeCell<MaybeUninit<T>>;

/// The producer-published cache line: the write cursor plus, co-located in
/// the same padded slot, the `closed` flag (written once by `Producer::drop`,
/// read only on consumer would-block paths — the line consumers already
/// poll) and the `dropped_through` overwrite watermark (written by the
/// producer *before* each overwrite-drop; teardown's lower bound).
struct WriteSide {
    write_cursor: AtomicU64,
    /// 0 = open, nonzero = closed. A whole word (not a bool) so the shm
    /// layout can pin it at a fixed header offset with one atomic type.
    closed: AtomicU64,
    dropped_through: AtomicU64,
}

/// The state all handles share, kept alive by an `Arc`.
struct Shared<T, P, C> {
    buffer: Box<[Slot<T>]>,
    /// `capacity - 1`, in the u64 domain of all cursor arithmetic.
    mask: u64,
    write_side: CachePadded<WriteSide>,
    /// First registry chunk, inline; growth cold-appends via `next`.
    registry: Chunk,
    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: buffer slots are written only by the single producer; consumers
// take shared `&T` borrows of published slots, ordered by the cursor
// atomics. Sharing requires `T: Sync` (the same element is read through `&T`
// from several consumer threads at once) and `T: Send` (the producer or the
// teardown path drops values that consumer threads produced borrows of).
unsafe impl<T: Send + Sync, P: Send + Sync, C: Send + Sync> Sync for Shared<T, P, C> {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl<T: Send + Sync, P: Send + Sync, C: Send + Sync> Send for Shared<T, P, C> {}

impl<T, P, C> Drop for Shared<T, P, C> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<T>() {
            // No concurrent access at drop time, so relaxed loads suffice.
            // Live occupants are exactly `[dropped_through, write_cursor)`:
            // the watermark — not any consumer cursor — is the lower bound,
            // which makes a double drop after a panicking overwrite-drop or
            // an abandoned `WriteSlot` unreachable [A-2.1]. If a teardown
            // drop panics, it propagates and the remaining occupants (and
            // extra chunks) leak — the `Box<[T]>` policy, stated not silent.
            let mut head = self.write_side.dropped_through.load(Ordering::Relaxed);
            let tail = self.write_side.write_cursor.load(Ordering::Relaxed);
            // By construction the watermark trails the write cursor by at
            // most one lap (an overwrite advances it before publishing).
            debug_assert!(
                tail.wrapping_sub(head) <= self.mask + 1,
                "dropped_through fell more than a lap behind"
            );
            while head != tail {
                // SAFETY: every seq in `[dropped_through, write_cursor)` was
                // published and has not been overwrite-dropped, so the slot
                // holds an initialized value; we drop it exactly once.
                unsafe { (*self.buffer[(head & self.mask) as usize].get()).assume_init_drop() };
                head = head.wrapping_add(1);
            }
        }
        // Free the appended registry chunks (the first chunk is inline).
        self.registry.free_appended();
    }
}

/// Error returned by consumer pops once the ring is **closed and drained**:
/// the producer was dropped and this consumer has consumed every published
/// message.
///
/// On heap rings `Closed` is **terminal** — nothing can reopen the ring. On
/// shared-memory rings (`shm` feature) it marks the **end of a producer
/// session**: a new producer attaching to the region reopens the ring, and a
/// consumer handle that already observed `Closed` will pop the new session's
/// messages if it keeps polling. Treat `Closed` as "this producer is done",
/// not "this ring can never speak again", when the ring lives in shared
/// memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl core::fmt::Display for Closed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ring closed: producer dropped and all published messages consumed")
    }
}

impl std::error::Error for Closed {}

/// Error returned by [`Producer::subscribe`]/[`Consumer::subscribe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    /// The producer has been dropped; a new consumer could never receive a
    /// message.
    Closed,
    /// The consumer registry is full. Never returned by heap rings (the
    /// registry grows without bound); reserved for shared-memory rings,
    /// whose mapped layout fixes `max_consumers` at creation.
    Full,
}

impl core::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SubscribeError::Closed => f.write_str("ring closed: producer dropped"),
            SubscribeError::Full => f.write_str("consumer registry is full"),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// Builder/namespace for constructing an SPMC ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity at runtime (rounded up to
/// the next power of two, minimum 2) and uses [`YieldWait`] on both sides.
/// Pick other [`WaitStrategy`]s with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (push) strategy, `C` the consumer-side (pop) strategy.
///
/// `T: Sync` is required in addition to `T: Send`: this is a broadcast ring,
/// so several consumer threads hold `&T` borrows of the *same* element at
/// once.
pub struct RingBuffer<T, P = YieldWait, C = YieldWait>(core::marker::PhantomData<(T, P, C)>);

impl<T: Send + Sync> RingBuffer<T> {
    /// Create a ring buffer with the default wait strategies and return its
    /// producer and one initial consumer (subscribe more from either handle).
    ///
    /// The real capacity is `min_capacity` rounded up to the next power of
    /// two, with a floor of 2 (an audience-less producer's gating default is
    /// its own cursor minus one, which a capacity-1 ring could never pass).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new(min_capacity: usize) -> (Producer<T>, Consumer<T>) {
        RingBuffer::<T, YieldWait, YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<T, P, C> RingBuffer<T, P, C>
where
    T: Send + Sync,
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    /// Create a ring buffer with explicit wait strategies and return its
    /// producer and one initial consumer.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (Producer<T, P, C>, Consumer<T, P, C>) {
        let capacity = round_capacity(min_capacity, 2);

        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        let shared = Arc::new(Shared {
            buffer: buffer.into_boxed_slice(),
            mask: capacity as u64 - 1,
            write_side: CachePadded::new(WriteSide {
                write_cursor: AtomicU64::new(0),
                closed: AtomicU64::new(0),
                dropped_through: AtomicU64::new(0),
            }),
            registry: Chunk::new(),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        let consumer = subscribe_from(&shared).expect("a fresh ring is not closed");
        // The buffer pointer is derived from the whole-slice `as_ptr` (not a
        // first-element reference) so it keeps provenance over every slot.
        let buf = NonNull::new(shared.buffer.as_ptr().cast_mut()).expect("buffer is non-null");
        let producer = Producer {
            buf,
            mask: capacity as u64 - 1,
            next_seq: 0,
            cached_min: 0,
            dropped_through_local: 0,
            cached_cursors: Vec::new(),
            write_cursor: NonNull::from(&shared.write_side.write_cursor),
            closed: NonNull::from(&shared.write_side.closed),
            dropped_through: NonNull::from(&shared.write_side.dropped_through),
            anchor: ProducerAnchor::Heap(shared),
        };
        (producer, consumer)
    }
}

/// Register a new consumer on live shared state — the Disruptor
/// `addSequences` choreography [M-F2], provided by the shared gating engine
/// ([`subscribe_slot`]): claim, bitmap RMW strictly before the `SeqCst`
/// fence, join point = the post-fence re-read of the write cursor.
fn subscribe_from<T, P, C>(
    shared: &Arc<Shared<T, P, C>>,
) -> Result<Consumer<T, P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    // Clone the Arc *before* touching the registry [A-2.2]: the new slot can
    // never outlive the shared state it points into, making the
    // subscribe-vs-teardown race structurally unreachable.
    let shared = Arc::clone(shared);
    if shared.write_side.closed.load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }

    // The [M-F2] claim/activate/fence/re-read choreography (see
    // `crate::registry::subscribe_slot`).
    let slot = subscribe_slot(&shared.registry, &shared.write_side.write_cursor);

    let buf = NonNull::new(shared.buffer.as_ptr().cast_mut()).expect("buffer is non-null");
    let mask = shared.mask;
    Ok(Consumer {
        buf,
        mask,
        cursor_slot: slot.cursor_slot,
        write_cursor: NonNull::from(&shared.write_side.write_cursor),
        closed: NonNull::from(&shared.write_side.closed),
        read_cursor: slot.joined,
        published: slot.published,
        write_cache: slot.joined,
        anchor: ConsumerAnchor::Heap {
            shared,
            chunk: slot.chunk,
            slot_idx: slot.slot_idx,
        },
    })
}

/// Where the producing handle's shared state lives — the registry seam.
/// Consulted on cold paths (blocking waits, subscribe, the gate-miss rescan
/// walk, teardown) plus two bounded hot-path touches: the wait-strategy
/// `notify()` accessor on publish/flush (a no-op the spin strategies inline
/// away) and, on shm consumers only, the cfg-gated per-flush lease guard
/// (one predictable branch; compiled out entirely without the `shm`
/// feature). Everything else goes through the handle's cached raw pointers,
/// identical for both variants.
enum ProducerAnchor<T, P, C> {
    /// In-process ring: the shared state lives on the heap in an `Arc`; the
    /// registry is the append-only chunk list.
    Heap(Arc<Shared<T, P, C>>),
    /// Cross-process ring: the state lives in a mapped shared region; the
    /// registry is the flat consumer table. Boxed so enabling the feature
    /// does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmProducer<C>>),
}

impl<T, P: WaitStrategy, C: WaitStrategy> ProducerAnchor<T, P, C> {
    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            ProducerAnchor::Heap(shared) => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => &anchor.consumer_wait,
        }
    }

    /// Whether teardown may touch shared state (the ring-wide closed word).
    /// Heap: always. Shm: only the current producer-lease holder in the
    /// constructing process — a crashed producer never gets here, and a fork
    /// child or superseded zombie must not close the successor's ring.
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ProducerAnchor::Heap(_) => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => anchor.owned_by_current_process() && anchor.owns_lease(),
        }
    }
}

/// The consuming handle's side of the registry seam (see [`ProducerAnchor`]).
enum ConsumerAnchor<T, P, C> {
    /// Heap ring: the `Arc` plus this consumer's chunk/slot coordinates for
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

impl<T, P: WaitStrategy, C: WaitStrategy> ConsumerAnchor<T, P, C> {
    #[inline(always)]
    fn producer_wait(&self) -> &P {
        match self {
            ConsumerAnchor::Heap { shared, .. } => &shared.producer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ConsumerAnchor::Shm(anchor) => &anchor.producer_wait,
        }
    }

    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            ConsumerAnchor::Heap { shared, .. } => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ConsumerAnchor::Shm(anchor) => &anchor.consumer_wait,
        }
    }

    /// Whether teardown may touch shared state. Heap: always. Shm: only the
    /// slot-lease holder in the constructing process — a fork-inherited copy
    /// must not flush over (or free) the parent's live slot, and a zombie
    /// whose slot was reset by `recover_shm` must not resurrect it.
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ConsumerAnchor::Heap { .. } => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ConsumerAnchor::Shm(anchor) => anchor.owned_by_current_process() && anchor.owns_slot(),
        }
    }

    /// The registry de-registration half of consumer teardown (the caller
    /// has already flushed and stored the cursor sentinel). Heap: clear the
    /// bitmap bit. Shm: return the slot (a guarded control-word CAS — it
    /// fails harmlessly if the slot was force-retired) and release the slot
    /// lease. Both then wake a producer blocked on the gate — the missing
    /// dual of the producer's close-notify [A-1.3]: a producer parked
    /// waiting for the minimum to move would stall forever if its last
    /// gating consumer detached silently.
    fn detach(&self) {
        match self {
            ConsumerAnchor::Heap {
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
            ConsumerAnchor::Shm(anchor) => anchor.detach(),
        }
    }
}

/// The producing half of a [`RingBuffer`]. Owns the private write cursor and
/// the gating caches. `Send` but not `Clone`: exactly one producer, enforced
/// by the type system.
///
/// Dropping the producer **closes** the ring: consumers drain what was
/// published and then see [`Closed`].
pub struct Producer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next sequence to write (private; the published cursor trails it by
    /// the not-yet-committed claim, if any).
    next_seq: u64,
    /// Cached minimum of the active consumers' cursors — the gate. A lower
    /// bound; the fast-path space check touches no shared line.
    cached_min: u64,
    /// Producer-local mirror of the shared `dropped_through` watermark (the
    /// producer is its only writer, so the mirror is exact).
    dropped_through_local: u64,
    /// Per-slot cached consumer cursors, mirroring the registry geometry
    /// (one 64-wide block per chunk or per 64 table slots, sized lazily).
    /// Monotonicity makes every cached value a permanent lower bound — for
    /// later occupants of the slot too, since a joiner's cursor starts at
    /// the then-current write cursor, which any earlier cached value cannot
    /// exceed [P-F3].
    cached_cursors: Vec<[u64; crate::registry::CHUNK_SLOTS]>,
    /// The shared write cursor (cached raw pointer; heap: into the `Arc`,
    /// shm: into the mapped region — the hot publish path is identical).
    write_cursor: NonNull<AtomicU64>,
    /// The shared closed word (written once, on drop).
    closed: NonNull<AtomicU64>,
    /// The shared overwrite watermark [A-2.1].
    dropped_through: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive, carries the wait strategies, and names
    /// the registry (heap chunks vs shm table) for the cold paths.
    anchor: ProducerAnchor<T, P, C>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive. `T: Send + Sync`
// per the shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send + Sync, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Producer<T, P, C>
{
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for Producer<T, P, C> {
    fn drop(&mut self) {
        // Flag-then-notify [A-1.1]: a consumer that checked the flag just
        // before this store is parked (or about to park) in a wait whose
        // predicate re-checks `closed`, and the notify wakes it. Guarded for
        // shm (heap: constant true): only a graceful drop by the live lease
        // holder sets the ring-wide closed word — a crashed producer never
        // runs this, and consumers distinguish that case via the lease and
        // their own liveness assertions, per the trust model.
        if self.anchor.teardown_allowed() {
            // SAFETY: `closed` points into the live shared state.
            unsafe { self.closed.as_ref() }.store(1, Ordering::Release);
            self.anchor.consumer_wait().notify();
        }
    }
}

impl<T, P, C> Producer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until the slowest consumer frees a slot, then enqueue `value`.
    ///
    /// With zero consumers this never blocks (free-run): the overwritten
    /// occupant is dropped and the value is published to nobody.
    #[inline]
    pub fn push(&mut self, value: T) {
        self.wait_for_space(1);
        self.prepare_slot();
        self.write(value);
    }

    /// Enqueue `value` without blocking. Returns `Err(value)` if the ring is
    /// gated (full for the slowest consumer) after one full registry rescan,
    /// handing the item back to the caller.
    ///
    /// "Full" is judged against the consumers' *published* progress; while a
    /// consumer defers publishes in the backed-up regime this can spuriously
    /// fail with up to `capacity / 8` (max 64) slots consumed but not yet
    /// published. A blocking [`push`](Self::push) is woken as soon as the
    /// gating consumer flushes.
    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if !self.has_space(1) {
            return Err(value);
        }
        self.prepare_slot();
        self.write(value);
        Ok(())
    }

    /// Block until there is room, then return the free slot for in-place
    /// construction — the zero-copy alternative to [`push`](Self::push).
    ///
    /// Write through [`WriteSlot::uninit`] and publish with
    /// [`WriteSlot::commit_init`], or move a value in with
    /// [`WriteSlot::commit`]. See [`WriteSlot`] for the semantics of
    /// dropping the slot uncommitted.
    #[inline]
    pub fn claim(&mut self) -> WriteSlot<'_, T, P, C> {
        self.wait_for_space(1);
        // Drop-on-overwrite happens at claim time, before the storage is
        // handed out.
        self.prepare_slot();
        WriteSlot { producer: self }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if the ring is
    /// gated.
    #[inline]
    pub fn try_claim(&mut self) -> Option<WriteSlot<'_, T, P, C>> {
        if !self.has_space(1) {
            return None;
        }
        self.prepare_slot();
        Some(WriteSlot { producer: self })
    }

    /// Subscribe a new consumer. Its join point is the currently published
    /// cursor: it sees only messages published after this call returns, and
    /// all of them.
    ///
    /// Cold: the producer's gating caches pick the newcomer up on the next
    /// rescan, which the gating default forces at least once per lap.
    ///
    /// On a shared-memory ring the consumer table is fixed at creation, so
    /// this can additionally fail with [`SubscribeError::Full`].
    pub fn subscribe(&self) -> Result<Consumer<T, P, C>, SubscribeError> {
        match &self.anchor {
            ProducerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this `T` and
            // capacity when this handle was constructed.
            ProducerAnchor::Shm(anchor) => unsafe {
                shm_subscribe(anchor.region(), anchor.max_slots(), self.mask + 1)
            },
        }
    }

    /// Number of currently attached consumers (a registry scan — cold; a
    /// racing subscribe/detach makes it a snapshot, not a guarantee).
    pub fn consumer_count(&self) -> usize {
        match &self.anchor {
            ProducerAnchor::Heap(shared) => shared.registry.active_count(),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => anchor.active_count(),
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
        // borrow `self` mutably (cold path; one refcount bump). Shm anchors
        // carry per-handle `CrossProcess` strategies, for which a fresh
        // default instance IS the same (stateless, self-timed) strategy.
        let heap = match &self.anchor {
            ProducerAnchor::Heap(shared) => Some(Arc::clone(shared)),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(_) => None,
        };
        match heap {
            Some(shared) => {
                while !self.has_space(needed) {
                    // The predicate re-runs the FULL scan [M-F4]: a cached
                    // minimum here is a deadlock, and rescanning is also what
                    // lets the wait terminate when every gating consumer
                    // detaches (the detach raises the minimum or empties the
                    // registry).
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
    /// `cached_min` — the [M-F2]/[P-F1]/[M-F1] fence discipline lives in
    /// [`rescan_gate`]; this supplies the registry seam (one walk per
    /// registry kind, same cache geometry — the walks are cold relative to
    /// the fast path; the match costs nothing measurable there). Returns
    /// whether `needed` slots are now free.
    fn rescan(&mut self, needed: u64) -> bool {
        let capacity = self.mask + 1;
        let next_seq = self.next_seq;
        let anchor = &self.anchor;
        let cached_cursors = &mut self.cached_cursors;
        rescan_gate(
            next_seq,
            needed,
            capacity,
            &mut self.cached_min,
            || match anchor {
                ProducerAnchor::Heap(shared) => scan_chunk_registry(
                    &shared.registry,
                    cached_cursors,
                    next_seq,
                    needed,
                    capacity,
                ),
                #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
                ProducerAnchor::Shm(anchor) => {
                    scan_shm_table(anchor, cached_cursors, next_seq, needed, capacity)
                }
            },
        )
    }

    /// Drop-on-overwrite (runs at claim time, before the slot is written or
    /// handed out): if the slot still holds a live occupant from a lap ago,
    /// drop it. For `!needs_drop` types this is one const-folded branch.
    #[inline(always)]
    fn prepare_slot(&mut self) {
        // Wrap-safe "the occupant `next_seq - capacity` exists and has not
        // been dropped yet": its distance past the watermark is what makes
        // it a member of the live window `[dropped_through, write_cursor)`.
        // Subsumes the first-lap check (the watermark starts at 0).
        if std::mem::needs_drop::<T>()
            && self.next_seq.wrapping_sub(self.dropped_through_local) > self.mask
        {
            self.drop_overwritten();
        }
    }

    /// Drop the old occupant of the slot `next_seq` is about to reuse.
    #[cold]
    fn drop_overwritten(&mut self) {
        let capacity = self.mask + 1;
        let old = self.next_seq.wrapping_sub(capacity);
        let mark = old.wrapping_add(1);
        // Advance the watermark BEFORE the drop begins [M-F5]: once
        // `drop_in_place` starts, the occupant counts as dropped, so a
        // panicking drop (or a subsequently abandoned claim) leaves a
        // watermark that already excludes it — teardown and push-retry can
        // never double-drop it.
        // SAFETY: `dropped_through` points into the live shared state.
        unsafe { self.dropped_through.as_ref() }.store(mark, Ordering::Release);
        self.dropped_through_local = mark;
        // SAFETY: the gate passed for `next_seq` (min consumer cursor is at
        // least `old + 1`), so every consumer published its way past `old`:
        // no `&T` borrow of this slot can exist, and the consumers' Release
        // cursor stores synchronize with the rescan's Acquire fence, so
        // their last reads happen-before this drop. The slot holds an
        // initialized `T` (it is inside `[dropped_through, write_cursor)`
        // per the check in `prepare_slot`); we drop it exactly once.
        unsafe { std::ptr::drop_in_place((*self.slot()).as_mut_ptr()) };
    }

    /// Pointer to the slot `next_seq` designates (in bounds by masking).
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: `next_seq & mask` is in `0..capacity`; `buf` is the live
        // buffer the `Arc` keeps alive; `get` on the `UnsafeCell` is how the
        // single producer accesses its storage.
        unsafe { (*self.buf.as_ptr().add((self.next_seq & self.mask) as usize)).get() }
    }

    /// Common tail of `push`/`try_push`/`commit`: store the value and
    /// publish it. The slot was prepared (old occupant dropped) beforehand.
    #[inline(always)]
    fn write(&mut self, value: T) {
        // SAFETY: we are the single producer, the gate confirmed every
        // consumer is past the previous occupant, and `prepare_slot` already
        // dropped it — the slot is dead storage we may overwrite.
        unsafe { (*self.slot()).write(value) };
        self.publish();
    }

    /// Advance and publish the write cursor (one `Release` store), then wake
    /// blocked consumers (a no-op for the spin strategies).
    #[inline(always)]
    fn publish(&mut self) {
        self.next_seq = self.next_seq.wrapping_add(1);
        // SAFETY: `write_cursor` points into the live shared state.
        unsafe { self.write_cursor.as_ref() }.store(self.next_seq, Ordering::Release);
        self.anchor.consumer_wait().notify();
    }

    /// Number of elements queued ahead of the slowest consumer, per the
    /// producer's **cached** gating view.
    ///
    /// An approximation: the cache is only refreshed on gate misses, so this
    /// can transiently over-count by up to a capacity's worth of
    /// already-consumed elements (and reports at least 1 after the producer
    /// has run with no consumers attached). It never under-counts.
    #[inline]
    pub fn len(&self) -> usize {
        self.next_seq.wrapping_sub(self.cached_min) as usize
    }

    /// Whether the ring looks empty per the producer's cached view. Never
    /// reports `true` for a ring some consumer still has elements to read
    /// (see [`len`](Self::len)).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the ring looks full (a `push` would block) per the producer's
    /// cached view. May transiently report `true` while consumers defer
    /// their cursor publishes (see [`len`](Self::len)); never reports
    /// `false` for a truly gated ring.
    #[inline]
    pub fn is_full(&self) -> bool {
        lacks_space(self.next_seq, 1, self.cached_min, self.mask + 1)
    }

    /// The ring's true capacity (the requested minimum rounded up to a power
    /// of two, minimum 2).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }
}

/// A claimed, not-yet-published slot in the ring — the zero-copy write path.
///
/// Construct the element directly in the buffer via [`uninit`](Self::uninit)
/// and publish with [`commit_init`](Self::commit_init), or move a value in
/// with [`commit`](Self::commit).
///
/// Dropping the slot uncommitted publishes nothing — consumers never see the
/// slot, because the write cursor never advanced. The slot's *previous*
/// occupant was already dropped when the claim was created (and the
/// `dropped_through` watermark advanced past it), so after an abandoned
/// claim the slot holds uninitialized storage; the watermark guarantees the
/// next claim or push of the same sequence never tries to drop it again.
pub struct WriteSlot<'a, T, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut Producer<T, P, C>,
}

impl<T, P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, T, P, C> {
    /// The slot's storage, for in-place initialization.
    ///
    /// The contents are unspecified until written (the previous occupant was
    /// dropped when the slot was claimed) — initialize before reading.
    #[inline]
    pub fn uninit(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: the gate was confirmed when the claim was created and the
        // producer cursor has not moved since (`self` borrows the producer
        // exclusively); the previous occupant is already dropped, so this is
        // dead storage only the single producer may touch.
        unsafe { &mut *self.producer.slot() }
    }

    /// Move `value` into the slot and publish it (equivalent to `push` on a
    /// slot that is already reserved).
    #[inline]
    pub fn commit(self, value: T) {
        let Self { producer } = self;
        producer.write(value);
    }

    /// Publish a slot that was initialized through [`uninit`](Self::uninit).
    ///
    /// # Safety
    ///
    /// The slot must contain a fully initialized `T`.
    #[inline]
    pub unsafe fn commit_init(self) {
        let Self { producer } = self;
        producer.publish();
    }
}

/// A consuming handle of a [`RingBuffer`]. Owns a private read cursor and
/// one registry slot. `Send` but not `Clone`; create more consumers with
/// [`subscribe`](Self::subscribe).
///
/// Dropping the consumer detaches it: it stops gating the producer and wakes
/// a producer blocked on it.
pub struct Consumer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the anchor's lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// This consumer's cursor word — the hot flush target (heap: its chunk
    /// slot; shm: its table slot's cursor; the store is identical).
    cursor_slot: NonNull<AtomicU64>,
    /// The producer's published cursor (cached raw pointer, both variants).
    write_cursor: NonNull<AtomicU64>,
    /// The shared closed word (read on would-block paths only).
    closed: NonNull<AtomicU64>,
    /// Next sequence to read (private to this thread).
    read_cursor: u64,
    /// The value of `read_cursor` last published to the registry slot (see
    /// [`advance_one`](Self::advance_one) for the adaptive publish rule).
    published: u64,
    /// Cached snapshot of the producer's write cursor.
    write_cache: u64,
    /// Keeps the ring's memory alive, carries the wait strategies, and names
    /// the registry (heap chunks vs shm table) for the cold paths.
    anchor: ConsumerAnchor<T, P, C>,
}

// SAFETY: the consumer only touches consumer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive. `T: Send + Sync`
// per the shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send + Sync, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Consumer<T, P, C>
{
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for Consumer<T, P, C> {
    fn drop(&mut self) {
        // Guarded teardown (heap: constant true): a fork-inherited copy or a
        // handle whose slot lease was superseded must not flush over — or
        // free — live state it no longer owns.
        if !self.anchor.teardown_allowed() {
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
        self.anchor.detach();
    }
}

impl<T, P, C> Consumer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until an element is available, then dequeue it **by clone**.
    ///
    /// Returns `Err(`[`Closed`]`)` only when the producer has been dropped
    /// *and* every published message has been consumed. The clone happens
    /// before the cursor advances, so a panicking `Clone` leaves the element
    /// unconsumed (redelivered by the next pop).
    #[inline]
    pub fn pop(&mut self) -> Result<T, Closed>
    where
        T: Clone,
    {
        self.wait_for_item()?;
        Ok(self.read())
    }

    /// Dequeue an element by clone without blocking. `Ok(None)` means
    /// empty-but-alive; `Err(`[`Closed`]`)` means closed **and** drained.
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<T>, Closed>
    where
        T: Clone,
    {
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
    /// it in the buffer. The slot is released (this consumer's cursor
    /// advances) when the returned [`PopRef`] drops; the element itself
    /// stays in the ring for the other consumers.
    ///
    /// Returns `Err(`[`Closed`]`)` when closed and drained, like
    /// [`pop`](Self::pop).
    #[inline]
    pub fn pop_ref(&mut self) -> Result<PopRef<'_, T, P, C>, Closed> {
        self.wait_for_item()?;
        Ok(PopRef { consumer: self })
    }

    /// Non-blocking [`pop_ref`](Self::pop_ref). `Ok(None)` means
    /// empty-but-alive; `Err(`[`Closed`]`)` means closed **and** drained.
    #[inline]
    pub fn try_pop_ref(&mut self) -> Result<Option<PopRef<'_, T, P, C>>, Closed> {
        if self.has_item() {
            return Ok(Some(PopRef { consumer: self }));
        }
        self.check_closed()?;
        if self.available_cached() != 0 {
            return Ok(Some(PopRef { consumer: self }));
        }
        Ok(None)
    }

    /// Consume up to one publish batch (`capacity / 8`, max 64) of available
    /// elements, calling `f` on each in place, and return how many were
    /// consumed. The read cursor is published **once**, after the last
    /// element — one `Release` store (and one wake-up) for the whole batch,
    /// giving a deterministic publish granularity.
    ///
    /// The private cursor advances over each element *before* `f` sees it,
    /// and the publish happens even if `f` panics (an unwound drain never
    /// re-delivers already-processed elements to this consumer). The borrow
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
        // `FlushOnDrop` guard over this consumer's `flush_pending`).
        let guard = FlushOnDrop(self);
        for _ in 0..count {
            let slot = guard.0.slot();
            // Advance before the callback: the element counts as consumed
            // even if `f` unwinds.
            guard.0.read_cursor = guard.0.read_cursor.wrapping_add(1);
            // SAFETY: every seq below `end` is published, so the slot holds
            // an initialized `T`; consumers take shared `&T` borrows only,
            // and the producer cannot overwrite it before this consumer's
            // cursor is published (by the guard, strictly after `f`).
            f(unsafe { (*slot).assume_init_ref() });
        }
        count as usize
    }

    /// Subscribe a further consumer; see [`Producer::subscribe`].
    pub fn subscribe(&self) -> Result<Consumer<T, P, C>, SubscribeError> {
        match &self.anchor {
            ConsumerAnchor::Heap { shared, .. } => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this `T` and
            // capacity when this handle was constructed.
            ConsumerAnchor::Shm(anchor) => unsafe {
                shm_subscribe(anchor.region(), anchor.max_slots(), self.mask + 1)
            },
        }
    }

    /// Number of elements available to this consumer. Exact on this side:
    /// uses the consumer's private cursor, which is always current.
    #[inline]
    pub fn len(&self) -> usize {
        // SAFETY: `write_cursor` points into the live shared state.
        unsafe { self.write_cursor.as_ref() }
            .load(Ordering::Acquire)
            .wrapping_sub(self.read_cursor) as usize
    }

    /// Whether this consumer has nothing to read. Exact on this side (see
    /// [`len`](Self::len)).
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
        self.write_cache.wrapping_sub(self.read_cursor)
    }

    /// Unconditionally reload the cached view of the producer's cursor
    /// (`Acquire`) and return it.
    #[inline(always)]
    fn refresh(&mut self) -> u64 {
        // SAFETY: `write_cursor` points into the live shared state.
        self.write_cache = unsafe { self.write_cursor.as_ref() }.load(Ordering::Acquire);
        self.write_cache
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
    /// write cursor once more (the `Acquire` load of `closed` synchronizes
    /// with the producer's `Release` store, which follows its final publish)
    /// and report [`Closed`] only if genuinely drained.
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
            let write_cursor = self.write_cursor;
            let closed = self.closed;
            let read = self.read_cursor;
            self.anchor.consumer_wait().wait(|| {
                // SAFETY: the pointers reference live shared state the
                // anchor keeps alive for the duration of the wait.
                unsafe {
                    write_cursor
                        .as_ref()
                        .load(Ordering::Acquire)
                        .wrapping_sub(read)
                        != 0
                        || closed.as_ref().load(Ordering::Acquire) != 0
                }
            });
        }
    }

    /// Pointer to the slot the read cursor designates (in bounds by
    /// masking).
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: `read_cursor & mask` is in `0..capacity`; `buf` is the
        // live buffer the `Arc` keeps alive.
        unsafe {
            (*self
                .buf
                .as_ptr()
                .add((self.read_cursor & self.mask) as usize))
            .get()
        }
    }

    /// Common tail of `pop`/`try_pop`: clone the value out, **then** advance
    /// [A-5] — a panicking clone leaves the element unconsumed.
    #[inline(always)]
    fn read(&mut self) -> T
    where
        T: Clone,
    {
        // SAFETY: the read cursor is below the producer's published cursor,
        // so the slot holds an initialized `T`. Consumers only ever take
        // shared `&T` borrows (several consumers may hold one concurrently —
        // hence the `T: Sync` construction bound), and the producer cannot
        // drop or overwrite the slot until this consumer's cursor is
        // published past it.
        let value = unsafe { (*self.slot()).assume_init_ref() }.clone();
        self.advance_one();
        value
    }

    /// Release one slot with the adaptive publish (verbatim from the SPSC
    /// engine): immediate when caught up or when the ring was observed full
    /// per this side's own cached view (a purely consumer-local check —
    /// producer liveness without per-element line ping-pong), batched while
    /// backed up.
    #[inline(always)]
    fn advance_one(&mut self) {
        let capacity = self.mask + 1;
        let was_full = self.write_cache.wrapping_sub(self.read_cursor) > capacity - 1;
        self.read_cursor = self.read_cursor.wrapping_add(1);
        if was_full
            || self.read_cursor == self.write_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch(capacity)
        {
            self.flush();
        }
    }

    /// Publish the private read cursor to this consumer's registry slot and
    /// wake a producer blocked on the gate (a no-op for spin strategies).
    ///
    /// Guarded by slot-lease ownership on shm rings (heap: no check at all):
    /// every publish path funnels through here, so a zombie handle whose
    /// slot was reset by `recover_shm` can never store over a successor's
    /// cursor. A force-detached zombie still holds its lease and still
    /// stores — onto its RETIRED slot, which no scan reads [A-4.1]. Mirrors
    /// the SPSC engine's guarded flush (measured there: no throughput
    /// change; one Acquire load of the slot's own line).
    #[inline(always)]
    fn flush(&mut self) {
        #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
        if let ConsumerAnchor::Shm(anchor) = &self.anchor {
            if !anchor.owns_slot() {
                // Mark as published so retry paths don't spin on the dead
                // lease.
                self.published = self.read_cursor;
                return;
            }
        }
        // Never publish the DETACHED sentinel (exact-wraparound collision);
        // one unit less only gates the producer more, and the next flush
        // publishes past it.
        // SAFETY: `cursor_slot` points into the live shared state.
        unsafe { self.cursor_slot.as_ref() }
            .store(guard_sentinel(self.read_cursor), Ordering::Release);
        self.published = self.read_cursor;
        self.anchor.producer_wait().notify();
    }

    /// [`flush`](Self::flush) only if there is unpublished progress.
    #[inline(always)]
    fn flush_pending(&mut self) {
        if self.read_cursor != self.published {
            self.flush();
        }
    }
}

impl<T, P: WaitStrategy, C: WaitStrategy> FlushPending for Consumer<T, P, C> {
    #[inline(always)]
    fn flush_pending(&mut self) {
        Consumer::flush_pending(self);
    }
}

/// A zero-copy view of the next element, still in the buffer.
///
/// Dereferences to `&T` only — never `&mut T`: this is a broadcast ring, and
/// other consumers may be reading the *same* element concurrently. When the
/// guard drops, this consumer's cursor advances past the element; the
/// element itself is **not** dropped (it stays live for the other consumers;
/// the producer drops it on overwrite, or teardown does).
///
/// Forgetting the guard (`mem::forget`) does **not** consume the element:
/// the cursor never advances, so the *same element is delivered again* by
/// this consumer's next pop. Safe — but the un-advanced cursor also gates
/// the producer globally, so forget-then-idle stalls the whole ring for
/// every consumer. That is the gating contract, not a leak.
pub struct PopRef<'a, T, P: WaitStrategy, C: WaitStrategy> {
    consumer: &'a mut Consumer<T, P, C>,
}

impl<T, P: WaitStrategy, C: WaitStrategy> core::ops::Deref for PopRef<'_, T, P, C> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: the read cursor is below the producer's published cursor,
        // so the slot holds an initialized `T`; the producer cannot drop or
        // reuse it until this consumer's cursor advances (on drop of this
        // guard). Other consumers hold `&T` at most — shared borrows only
        // (`T: Sync` by the construction bound).
        unsafe { (*self.consumer.slot()).assume_init_ref() }
    }
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for PopRef<'_, T, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Advance-only [M-F7]: never `drop_in_place` — the value stays live
        // for the other consumers; ownership of the drop belongs to the
        // producer's overwrite path (or teardown).
        self.consumer.advance_one();
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). The handles built here are the ordinary `Producer`/
// `Consumer` types over region pointers — the hot paths are byte-identical
// to the heap ring's; only the registry seam (the anchor) differs.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, P: WaitStrategy, C: WaitStrategy> Producer<T, P, C> {
    /// Build a producer over a validated shm region. Seeds `next_seq` from
    /// the live write cursor and the gating cache with an always-gating
    /// value — an attached or recovered producer never trusts defaults
    /// [M-F17] — then runs **one real table rescan** before returning, so
    /// the seed is only ever the pre-scan value: without it, `&self`
    /// [`len`](Producer::len)/[`is_empty`](Producer::is_empty)/
    /// [`is_full`](Producer::is_full) would report a full ring until the
    /// first push, and the `while producer.is_full() {}` shape the
    /// semantics guide warns about would livelock on a fresh attach.
    ///
    /// # Safety
    ///
    /// The anchor's region must be a validated SPMC element ring of exactly
    /// this `T` and `capacity` (`create`/`open` in `crate::shm`), and the
    /// anchor must hold the producer lease.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::GateShmProducer<C>>,
        capacity: usize,
    ) -> Self {
        let region = anchor.region();
        let write_cursor = NonNull::from(region.spmc_write_cursor());
        let closed = NonNull::from(region.spmc_closed());
        let dropped_through = NonNull::from(region.spmc_aux());
        let buf = region.spmc_buffer(anchor.max_slots()).cast::<Slot<T>>();
        // SAFETY: `write_cursor` references the live mapping (per contract).
        let next_seq = unsafe { write_cursor.as_ref() }.load(Ordering::Acquire);
        let mut producer = Producer {
            buf,
            mask: capacity as u64 - 1,
            next_seq,
            // Always-gating seed (lag == capacity): the pre-scan value only
            // — the rescan below replaces it before anything reads it; 0
            // would be wrong after cursor wraparound, exactly as for fresh
            // cache blocks [M-F17].
            cached_min: next_seq.wrapping_sub(capacity as u64),
            // `T: ShmItem` is `Copy`, so the drop-on-overwrite machinery is
            // const-folded away; the mirror is kept trivially consistent.
            dropped_through_local: next_seq,
            cached_cursors: Vec::new(),
            write_cursor,
            closed,
            dropped_through,
            anchor: ProducerAnchor::Shm(anchor),
        };
        // One real registry rescan at construction: the gating view (and
        // with it `len`/`is_empty`/`is_full`) reflects the live table from
        // the start instead of lying "full" until the first push (see the
        // function doc for the documented livelock this fixes).
        producer.rescan(1);
        producer
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, P: WaitStrategy, C: WaitStrategy> Consumer<T, P, C> {
    /// Build a consumer over a claimed table slot. `read_cursor` is the join
    /// point from the claim choreography (or the recovery resume point,
    /// which `recover_shm` stores into the slot before calling this).
    ///
    /// # Safety
    ///
    /// As for [`Producer::from_shm`]; the anchor must hold a slot claimed by
    /// the `crate::shm` claim choreography whose cursor word currently holds
    /// (the sentinel-guarded image of) `read_cursor`.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::GateShmConsumer<P, C>>,
        capacity: usize,
        read_cursor: u64,
    ) -> Self {
        let region = anchor.region();
        let write_cursor = NonNull::from(region.spmc_write_cursor());
        let closed = NonNull::from(region.spmc_closed());
        let cursor_slot = NonNull::from(region.slot_cursor(anchor.slot()));
        let buf = region.spmc_buffer(anchor.max_slots()).cast::<Slot<T>>();
        Consumer {
            buf,
            mask: capacity as u64 - 1,
            cursor_slot,
            write_cursor,
            closed,
            read_cursor,
            published: guard_sentinel(read_cursor),
            write_cache: read_cursor,
            anchor: ConsumerAnchor::Shm(anchor),
        }
    }

    /// The consumer-table slot this handle occupies in its shared-memory
    /// region, or `None` for a heap ring (see
    /// [`shm_slot_epoch`](Self::shm_slot_epoch) for the pair
    /// [`force_detach_consumer`](RingBuffer::force_detach_consumer) takes).
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot(&self) -> Option<usize> {
        self.shm_slot_epoch().map(|(slot, _)| slot)
    }

    /// The consumer-table `(slot, epoch)` this handle occupies in its
    /// shared-memory region, or `None` for a heap ring. The pair identifies
    /// this exact *occupancy* — every claim bumps the slot's epoch — and is
    /// what [`force_detach_consumer`](RingBuffer::force_detach_consumer)
    /// takes, so a watchdog holding a dead consumer's pair can never retire
    /// a healthy successor that re-claimed the slot.
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot_epoch(&self) -> Option<(usize, u32)> {
        match &self.anchor {
            ConsumerAnchor::Heap { .. } => None,
            ConsumerAnchor::Shm(anchor) => Some((anchor.slot(), anchor.epoch())),
        }
    }
}

/// Subscribe through a live shm handle: the consumer-table claim
/// choreography (the shm analog of [`subscribe_from`], with the control-word
/// CAS as the [M-F2] registration event) plus handle construction. The `Arc`
/// clone precedes the claim, mirroring [A-2.2].
///
/// # Safety
///
/// `region` must be the validated SPMC element region (element type `T`,
/// `capacity` slots, `max_consumers` table slots) the calling handle was
/// built over.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
unsafe fn shm_subscribe<T, P, C>(
    region: &Arc<crate::shm::ShmRegion>,
    max_consumers: usize,
    capacity: u64,
) -> Result<Consumer<T, P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    const TABLE: usize = crate::shm::SPMC_TABLE_OFFSET;
    let region = Arc::clone(region);
    // Seqlock snapshot before the claim (the in-process mirror of
    // `crate::shm`'s attach choreography): an odd generation is a concurrent
    // `create_shm`/`recover_shm` mid-rewrite — claiming into a table being
    // reset would keep a slot the reset frees out from under us.
    let generation = region.generation();
    if generation & 1 == 1 {
        return Err(SubscribeError::Closed);
    }
    if region.spmc_closed().load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }
    let claim =
        crate::shm::claim_table_slot(&region, TABLE, max_consumers).ok_or(SubscribeError::Full)?;
    // Post-claim re-check: a generation change means the table was reset
    // while we claimed. The rollback is itself generation-conditional, so a
    // re-initialized table is left strictly alone (at worst one leaked slot
    // until the next `recover_shm` — a leak, never a clobber). The ring this
    // handle was subscribed from is gone, which is `Closed`.
    if region.generation() != generation {
        crate::shm::release_table_claim(&region, TABLE, &claim, generation);
        return Err(SubscribeError::Closed);
    }
    let joined = claim.joined;
    let anchor = Box::new(crate::shm::GateShmConsumer::new(
        region,
        claim,
        max_consumers,
        TABLE,
    ));
    // SAFETY: forwarded caller contract; the claim choreography just ran.
    Ok(unsafe { Consumer::from_shm(anchor, capacity as usize, joined) })
}
