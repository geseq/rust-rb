//! Single-producer / **multi**-consumer broadcast ring buffer (gating) for
//! **variable-size** byte messages.
//!
//! Where [`crate::spmc::RingBuffer`] broadcasts items of one fixed type `T`,
//! this ring broadcasts discrete byte messages of differing lengths —
//! serialized structs, wire frames, log records — through one shared byte
//! buffer. Every consumer observes **every** message, parsing frames
//! independently from its own byte cursor; the producer gates on the
//! *slowest* consumer's published cursor, so a consumer that stops consuming
//! eventually blocks the producer (that is the contract; the lossy
//! alternative is a separate machine).
//!
//! # Framing
//!
//! Identical to [`crate::spsc_bytes`]: each message is a *record* — a 4-byte
//! little-endian length header followed by the payload, the whole record
//! rounded up to a 4-byte boundary so headers stay naturally aligned.
//! Records never wrap around the end of the buffer: when one does not fit in
//! the space remaining, the producer writes a *padding* header (`u32::MAX`)
//! there and restarts at offset zero; consumers skip padding transparently.
//! Because a message may need that wrap padding *in addition to* its own
//! record, records are capped at half the capacity —
//! [`max_message_len`](BytesProducer::max_message_len) is
//! `capacity / 2 - 4` bytes, which guarantees any legal message can always
//! be written eventually (gating means backpressure, never loss, exactly as
//! in the SPSC ring).
//!
//! # Quick start
//!
//! ```
//! use rust_rb::spmc_bytes::BytesRingBuffer;
//!
//! let (mut tx, mut rx) = BytesRingBuffer::new(64);
//! let mut rx2 = tx.subscribe().unwrap(); // dynamic membership
//!
//! tx.push(b"tick");
//! assert_eq!(&*rx.pop().unwrap(), b"tick");
//! assert_eq!(&*rx2.pop().unwrap(), b"tick"); // both consumers see every message
//!
//! drop(tx); // producer drop closes the ring
//! assert!(rx.pop().is_err());
//! ```
//!
//! # Membership
//!
//! Membership is dynamic and unbounded, exactly as in [`crate::spmc`]:
//! [`BytesProducer::subscribe`] / [`BytesConsumer::subscribe`] add a consumer
//! whose **join point** is the producer's published byte cursor at subscribe
//! time — always a record boundary, so a joiner never starts parsing
//! mid-record. It sees only messages published after that, and all of them.
//! Dropping a consumer detaches it (a departed consumer never gates the
//! producer). With **zero** consumers the producer free-runs: pushes succeed
//! and old bytes are simply overwritten — there is no retention contract for
//! future subscribers.
//!
//! # Closed contract
//!
//! Dropping the [`BytesProducer`] closes the ring. [`BytesConsumer::pop`]
//! returns `Err(`[`Closed`]`)` only once the producer is gone **and** this
//! consumer has drained every published message; [`BytesConsumer::try_pop`]
//! returns `Ok(None)` for empty-but-alive and `Err(Closed)` for
//! closed-and-drained. [`BytesConsumer::drain`] reports no close at all — it
//! returns `0` on a drained ring, closed or not; use `pop`/`try_pop` to
//! observe the close. The flag is only consulted on would-block paths.
//!
//! # Why it is fast
//!
//! The hot paths combine the SPSC byte ring's framing with the gating
//! element ring's cursor machinery, all at byte granularity:
//!
//! * **Monotonic masked u64 byte cursors** compared by wrapped difference
//!   everywhere (the shared gating engine's cursor domain — ABA-immune on
//!   every target).
//! * **Producer-local gating cache**: the common-case space check touches no
//!   shared line; a gate miss walks a bitmap of active registry slots,
//!   reloading only the cursors that are actually blocking (`Relaxed` loads,
//!   one trailing `Acquire` fence).
//! * **Adaptive read-cursor publish** per consumer: immediate when caught
//!   up, batched (`capacity / 8`, max 4096 bytes) while backed up — plus a
//!   **lag-filtered starving release**: when the producer signals it is
//!   starving — the flag carries the blocked push's exact byte span — only
//!   a consumer far enough behind to possibly be the gate publishes per
//!   message (see [`Msg`]); the other N-1 keep batching.
//!
//! # Gotchas
//!
//! * `mem::forget` on a [`Msg`] means **redelivery** of the same message to
//!   that consumer — and, because the un-advanced cursor gates the producer
//!   globally, a forget-then-idle consumer stalls the whole ring. That is
//!   the gating contract, not a leak.
//! * Producer-side [`is_empty`](BytesProducer::is_empty) is an approximation
//!   against the cached gating minimum: it can transiently report non-empty
//!   for a fully drained ring (and always does after a free-run with no
//!   consumers attached); it never reports empty for a non-empty ring.
//!   Consumer-side views are exact.

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::cursor::round_capacity;
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
use crate::registry::scan_shm_table;
use crate::registry::{
    guard_sentinel, lacks_space, publish_batch_bytes as publish_batch, rescan_gate,
    scan_chunk_registry, subscribe_slot, Chunk, FlushOnDrop, FlushPending, DETACHED,
};
use crate::spsc_bytes::{max_message_len, record_len, ALIGN, HEADER, MIN_CAPACITY, PADDING};
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

pub use crate::spmc::{Closed, SubscribeError};

/// The buffer word type: `u64` so the base is 8-aligned (a `Box<[u8]>`
/// allocation only guarantees alignment 1). All access goes through raw `u8`
/// pointers; the words are never read as `u64`s.
///
/// Zero-initialized on construction so every byte is always initialized:
/// `WriteSlot`/`Msg` hand out `&[u8]` views into the ring, which would be
/// instant UB over uninitialized memory.
type Word = UnsafeCell<u64>;

/// The widest footprint one push can require free, in bytes: wrap padding
/// plus a maximum-size record.
///
/// Derivation from the framing: a record is at most
/// `R = record_len(max_message_len)` bytes, and padding is written only when
/// the record does not fit before the end of the buffer, so
/// `pad = to_end < R`. Both are multiples of [`ALIGN`], hence
/// `pad <= R - ALIGN` and `pad + record <= 2R - ALIGN`. This bounds the
/// *actual* span a starving producer publishes in the flag for the
/// consumers' lag filter (see [`Msg`]): a blocked producer needs exactly
/// `span <= max_record_span` bytes free, so its gating consumer's published
/// occupancy exceeds `capacity - span` — a consumer below that cannot be
/// the gate. (With this ring's `capacity / 2 - 4` message cap the
/// worst-case bound itself degenerates to `capacity - ALIGN`, which is why
/// the filter uses the flagged span, not this constant.)
#[inline(always)]
const fn max_record_span(capacity: usize) -> usize {
    2 * record_len(max_message_len(capacity)) - ALIGN
}

/// Decode the record at byte cursor `cur`: skip a padding record if present
/// and return `(cursor at the record header, payload length, payload ptr)`.
/// The single source of truth for the frame format — used by `pop`, `drain`,
/// and (via [`record_len`]) `Msg::drop`.
///
/// # Safety
///
/// A fully published record must exist at `cur` (availability confirmed via
/// an `Acquire` load of the producer's cursor). The producer publishes
/// padding together with the record that follows it (one cursor store covers
/// both), so after a padding skip a record is guaranteed at offset zero.
#[inline(always)]
unsafe fn decode_record(base: *const u8, mask: u64, mut cur: u64) -> (u64, usize, *const u8) {
    let mut pos = (cur & mask) as usize;
    // SAFETY: header reads are 4-aligned (records and padding are ALIGN
    // multiples, base is 8-aligned) and in bounds via the mask.
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

/// The producer-published cache line: the write cursor plus, co-located in
/// the same padded slot, the `closed` flag (written once by
/// `BytesProducer::drop`, read only on consumer would-block paths) and the
/// `starving` word (holds the blocked push's required byte span while the
/// producer starves — the consumers' exact release threshold — and 0
/// otherwise; written on span change, cleared with hysteresis; read by
/// consumers on message release). Consumers already poll this line for the
/// write cursor, so neither flag adds coherence traffic.
struct WriteSide {
    write_cursor: AtomicU64,
    /// 0 = open, nonzero = closed. A whole word (not a bool) so the shm
    /// layout can pin it at a fixed header offset with one atomic type.
    closed: AtomicU64,
    starving: AtomicU64,
}

/// The state all handles share, kept alive by an `Arc`.
struct Shared<P, C> {
    buffer: Box<[Word]>,
    /// `capacity - 1`, in the u64 domain of all cursor arithmetic.
    mask: u64,
    write_side: CachePadded<WriteSide>,
    /// First registry chunk, inline; growth cold-appends via `next`.
    registry: Chunk,
    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: buffer bytes are written only by the single producer; consumers
// take shared `&[u8]` views of published records, ordered by the cursor
// atomics (the producer's rescan `Acquire` fence pairs with the consumers'
// `Release` cursor stores before any byte is overwritten). The payload is
// plain bytes, so no `T`-bounds are involved.
unsafe impl<P: Send + Sync, C: Send + Sync> Sync for Shared<P, C> {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl<P: Send + Sync, C: Send + Sync> Send for Shared<P, C> {}

impl<P, C> Drop for Shared<P, C> {
    fn drop(&mut self) {
        // The buffer is plain bytes — nothing to drop; teardown frees the
        // allocation only. Free the appended registry chunks (the first
        // chunk is inline).
        self.registry.free_appended();
    }
}

/// Builder/namespace for constructing an SPMC byte ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity in **bytes** at runtime
/// (rounded up to the next power of two, at least 8) and uses [`YieldWait`]
/// on both sides. Pick other strategies with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (push) strategy, `C` the consumer-side (pop) strategy.
/// Both must be [`SelfTimed`] — with N consumers, a notify-dependent
/// strategy needs per-waiter wake state this ring does not carry.
pub struct BytesRingBuffer<P = YieldWait, C = YieldWait>(core::marker::PhantomData<(P, C)>);

impl BytesRingBuffer {
    /// Create a ring with the default wait strategies and return its
    /// producer and one initial consumer (subscribe more from either
    /// handle).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new(min_capacity: usize) -> (BytesProducer, BytesConsumer) {
        BytesRingBuffer::<YieldWait, YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<P, C> BytesRingBuffer<P, C>
where
    P: SelfTimed + Send + Sync,
    C: SelfTimed + Send + Sync,
{
    /// Create the ring with explicit wait strategies and return its producer
    /// and one initial consumer.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (BytesProducer<P, C>, BytesConsumer<P, C>) {
        let capacity = round_capacity(min_capacity, MIN_CAPACITY);

        // `capacity / 8` u64 words; zeroed so every byte the `&[u8]` views
        // can reach is initialized memory.
        let mut buffer = Vec::with_capacity(capacity / 8);
        buffer.resize_with(capacity / 8, || UnsafeCell::new(0u64));

        let shared = Arc::new(Shared {
            buffer: buffer.into_boxed_slice(),
            mask: capacity as u64 - 1,
            write_side: CachePadded::new(WriteSide {
                write_cursor: AtomicU64::new(0),
                closed: AtomicU64::new(0),
                starving: AtomicU64::new(0),
            }),
            registry: Chunk::new(),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        let consumer = subscribe_from(&shared).expect("a fresh ring is not closed");
        // The buffer pointer is derived from the whole-slice `as_ptr` (not a
        // first-element reference) so it keeps provenance over every slot.
        let buf = NonNull::new(shared.buffer.as_ptr().cast_mut()).expect("buffer is non-null");
        let producer = BytesProducer {
            buf,
            mask: capacity as u64 - 1,
            next_seq: 0,
            cached_min: 0,
            cached_cursors: Vec::new(),
            raised_starving: false,
            write_cursor: NonNull::from(&shared.write_side.write_cursor),
            closed: NonNull::from(&shared.write_side.closed),
            starving: NonNull::from(&shared.write_side.starving),
            anchor: ProducerAnchor::Heap(shared),
        };
        (producer, consumer)
    }
}

/// Register a new consumer on live shared state — the Disruptor
/// `addSequences` choreography [M-F2], provided by the shared gating engine
/// ([`subscribe_slot`]): claim, bitmap RMW strictly before the `SeqCst`
/// fence, join point = the post-fence re-read of the write cursor — always
/// a record boundary, since the producer publishes whole frames.
fn subscribe_from<P, C>(shared: &Arc<Shared<P, C>>) -> Result<BytesConsumer<P, C>, SubscribeError>
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
    Ok(BytesConsumer {
        buf,
        mask,
        cursor_slot: slot.cursor_slot,
        write_cursor: NonNull::from(&shared.write_side.write_cursor),
        closed: NonNull::from(&shared.write_side.closed),
        starving: NonNull::from(&shared.write_side.starving),
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

/// Where the producing handle's shared state lives — the registry seam
/// (identical in shape to `crate::spmc`'s; see there for the rationale).
/// Cold paths only; the hot paths go through cached raw pointers.
enum ProducerAnchor<P, C> {
    /// In-process ring: heap `Arc`; chunk-list registry.
    Heap(Arc<Shared<P, C>>),
    /// Cross-process ring: mapped region; flat consumer table. Boxed so
    /// enabling the feature does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmProducer<C>>),
}

impl<P: WaitStrategy, C: WaitStrategy> ProducerAnchor<P, C> {
    #[inline(always)]
    fn consumer_wait(&self) -> &C {
        match self {
            ProducerAnchor::Heap(shared) => &shared.consumer_wait,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => &anchor.consumer_wait,
        }
    }

    /// Teardown gate (see `crate::spmc`): heap always; shm only the live
    /// lease holder in the constructing process.
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
enum ConsumerAnchor<P, C> {
    Heap {
        shared: Arc<Shared<P, C>>,
        chunk: NonNull<Chunk>,
        slot_idx: usize,
    },
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::GateShmConsumer<P, C>>),
}

impl<P: WaitStrategy, C: WaitStrategy> ConsumerAnchor<P, C> {
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

    /// Teardown gate (see `crate::spmc::ConsumerAnchor`).
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ConsumerAnchor::Heap { .. } => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ConsumerAnchor::Shm(anchor) => anchor.owned_by_current_process() && anchor.owns_slot(),
        }
    }

    /// Registry de-registration (the caller stored the cursor sentinel).
    /// Then wake a gated producer — the close-notify's missing dual [A-1.3].
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

/// The producing half of a [`BytesRingBuffer`]. Owns the private write
/// cursor and the gating caches. `Send` but not `Clone`: exactly one
/// producer, enforced by the type system.
///
/// Dropping the producer **closes** the ring: consumers drain what was
/// published and then see [`Closed`].
pub struct BytesProducer<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the word buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Word>,
    /// `capacity - 1` in bytes (cached).
    mask: u64,
    /// Next byte to write (private; the published cursor trails it by the
    /// not-yet-committed claim, if any).
    next_seq: u64,
    /// Cached minimum of the active consumers' byte cursors — the gate. A
    /// lower bound; the fast-path space check touches no shared line.
    cached_min: u64,
    /// Per-slot cached consumer cursors, mirroring the registry geometry
    /// (one 64-wide block per chunk, sized lazily). Monotonicity makes every
    /// cached value a permanent lower bound — for later occupants of the
    /// slot too, since a joiner's cursor starts at the then-current write
    /// cursor, which any earlier cached value cannot exceed [P-F3].
    cached_cursors: Vec<[u64; crate::registry::CHUNK_SLOTS]>,
    /// Whether we raised the starving flag and have not yet cleared it
    /// (producer-local; keeps the never-starved hot path free of any flag
    /// access).
    raised_starving: bool,
    /// The shared write cursor (cached raw pointer; heap or shm, the hot
    /// publish path is identical).
    write_cursor: NonNull<AtomicU64>,
    /// The shared closed word (written once, on drop).
    closed: NonNull<AtomicU64>,
    /// The shared producer-starving flag.
    starving: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive, carries the wait strategies, and names
    /// the registry (heap chunks vs shm table) for the cold paths.
    anchor: ProducerAnchor<P, C>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesProducer<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesProducer<P, C> {
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

impl<P, C> BytesProducer<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until the slowest consumer frees enough bytes, then enqueue a
    /// copy of `msg`.
    ///
    /// With zero consumers this never blocks (free-run): the old bytes are
    /// overwritten and the message is published to nobody.
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()` — such a message could
    /// never be sent, so waiting for room would deadlock.
    #[inline]
    pub fn push(&mut self, msg: &[u8]) {
        let (pad, total) = self.frame(msg.len());
        self.wait_for_space(total);
        // SAFETY: `frame` sized the record and `wait_for_space` confirmed
        // `total` free bytes.
        unsafe { self.write_frame(pad, msg.len(), Some(msg.as_ptr())) };
    }

    /// Enqueue a copy of `msg` without blocking. Returns `false` if the ring
    /// is gated (not enough free space for the slowest consumer) after one
    /// full registry rescan.
    ///
    /// "Free" is judged against the consumers' *published* progress; while a
    /// consumer defers publishes in the backed-up regime this can spuriously
    /// fail with up to `capacity / 8` (max 4096) bytes consumed but not yet
    /// published. A blocking [`push`](Self::push) is woken as soon as the
    /// gating consumer flushes.
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
        unsafe { self.write_frame(pad, msg.len(), Some(msg.as_ptr())) };
        true
    }

    /// Block until there is room for a `len`-byte message, then return a
    /// [`WriteSlot`] to serialize it into — the zero-copy alternative to
    /// [`push`](Self::push). The message is published when the slot is
    /// [committed](WriteSlot::commit); dropping the slot uncommitted
    /// abandons the space for reuse by the next claim.
    ///
    /// # Panics
    ///
    /// Panics if `len > self.max_message_len()`.
    #[inline]
    pub fn claim(&mut self, len: usize) -> WriteSlot<'_, P, C> {
        let (pad, total) = self.frame(len);
        self.wait_for_space(total);
        let payload = self.payload_ptr(pad);
        WriteSlot {
            producer: self,
            payload,
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
        let payload = self.payload_ptr(pad);
        Some(WriteSlot {
            producer: self,
            payload,
            pad,
            len,
        })
    }

    /// Subscribe a new consumer. Its join point is the currently published
    /// byte cursor — always a record boundary — so it sees only messages
    /// published after this call returns, and all of them.
    ///
    /// Cold: the producer's gating caches pick the newcomer up on the next
    /// rescan, which the gating default forces at least once per lap.
    ///
    /// On a shared-memory ring the consumer table is fixed at creation, so
    /// this can additionally fail with [`SubscribeError::Full`].
    pub fn subscribe(&self) -> Result<BytesConsumer<P, C>, SubscribeError> {
        match &self.anchor {
            ProducerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this ring kind
            // and capacity when this handle was constructed.
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
    /// full registry rescan. Zero shared loads in the common case. Also
    /// maintains the starving flag with hysteresis (mirrors the SPSC byte
    /// engine): raised once per episode when even a full rescan leaves no
    /// room, kept up while space only appears via rescans, cleared once the
    /// cached check passes comfortably. The flag carries the blocked push's
    /// **actual required span** (`needed` = pad + record bytes), which is
    /// what makes the consumers' release threshold `capacity - span` exact:
    /// while one push is blocked the write cursor cannot move, so `frame`
    /// is deterministic and every check of the episode carries the same
    /// span.
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
            // producer polls anyway) so the gating consumer's lag-filtered
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
    /// the fast path). The free-run grant is sound here because any legal
    /// frame needs at most `capacity - ALIGN` bytes (see
    /// [`max_record_span`]). Returns whether `needed` bytes are now free.
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

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr().cast::<u8>()
    }

    /// Where the payload of a record claimed with `pad` bytes of wrap
    /// padding will start.
    #[inline(always)]
    fn payload_ptr(&self, pad: u64) -> NonNull<u8> {
        let pos = (self.next_seq.wrapping_add(pad) & self.mask) as usize;
        // SAFETY: in bounds — `frame` reserved `HEADER + len` contiguous
        // bytes starting at `pos`, and the buffer base is non-null.
        unsafe { NonNull::new_unchecked(self.base().add(pos + HEADER)) }
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

    /// Write padding (if any) and the record header, copy the payload when
    /// `src` is given (a `claim` has already filled it in place otherwise),
    /// then publish everything with one `Release` store.
    ///
    /// # Safety
    ///
    /// The caller must have confirmed `pad + record_len(len)` free bytes,
    /// with `(pad, _)` computed by `frame(len)` at the current write cursor.
    /// `src`, when given, must point to `len` readable bytes.
    #[inline(always)]
    unsafe fn write_frame(&mut self, pad: u64, len: usize, src: Option<*const u8>) {
        let base = self.base();
        let mask = self.mask;
        let mut cur = self.next_seq;

        // SAFETY (whole block): offsets are `& mask`, so in bounds; every
        // record boundary is 4-aligned (records and padding are multiples of
        // ALIGN and the base is 8-aligned), so the u32 header accesses are
        // aligned. The gate confirmed the space free: every consumer
        // published its way past these bytes, no `&[u8]` view of them can
        // exist, and the consumers' Release cursor stores synchronize with
        // the rescan's Acquire fence, so their last reads happen-before
        // these writes.
        unsafe {
            if pad > 0 {
                // `frame` only pads mid-buffer, where at least HEADER bytes
                // remain before the end. (PADDING is all-ones: endian-proof.)
                base.add((cur & mask) as usize)
                    .cast::<u32>()
                    .write(PADDING.to_le());
                cur = cur.wrapping_add(pad); // now at a capacity boundary
            }
            let pos = (cur & mask) as usize;
            // Headers are little-endian on every target, as the module docs
            // promise (free on LE machines; a byte swap on BE ones).
            base.add(pos).cast::<u32>().write((len as u32).to_le());
            if let Some(src) = src {
                std::ptr::copy_nonoverlapping(src, base.add(pos + HEADER), len);
            }
        }

        // One publish covers the padding and the record together.
        self.publish(pad + record_len(len) as u64);
    }

    /// Advance and publish the write cursor (one `Release` store), then wake
    /// blocked consumers (a no-op for the spin strategies).
    #[inline(always)]
    fn publish(&mut self, amount: u64) {
        self.next_seq = self.next_seq.wrapping_add(amount);
        // SAFETY: `write_cursor` points into the live shared state.
        unsafe { self.write_cursor.as_ref() }.store(self.next_seq, Ordering::Release);
        self.anchor.consumer_wait().notify();
    }

    /// Whether the ring looks empty per the producer's **cached** gating
    /// view.
    ///
    /// An approximation: the cache is only refreshed on gate misses, so this
    /// can transiently report `false` for a ring every consumer has fully
    /// drained (and always does after the producer has run with no
    /// consumers attached). It never reports `true` for a ring some
    /// consumer still has messages to read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.next_seq.wrapping_sub(self.cached_min) == 0
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 8).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }
}

/// A claimed, not-yet-published message slot in the ring.
///
/// Dereferences to the `len`-byte payload slice so it can be serialized into
/// directly. Call [`commit`](Self::commit) to publish; dropping the slot
/// without committing publishes nothing — consumers never see it, because
/// the write cursor never advanced.
///
/// The slice's initial contents are unspecified but always initialized
/// memory (zeroed at construction, previous records afterwards) — reading
/// before writing is safe but yields garbage, and committing without fully
/// writing publishes that garbage to every consumer.
pub struct WriteSlot<'a, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut BytesProducer<P, C>,
    /// Payload start, cached at claim time (the same handle-caching idea as
    /// the cursors: compute `(cursor + pad) & mask` once, not per deref).
    payload: NonNull<u8>,
    pad: u64,
    len: usize,
}

impl<P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, P, C> {
    /// Publish the message. Writes the headers and makes the record visible
    /// to every consumer with one `Release` store.
    #[inline]
    pub fn commit(self) {
        let Self {
            producer, pad, len, ..
        } = self;
        // SAFETY: space for `(pad, len)` was confirmed when the slot was
        // created, and the producer cursor has not moved since (`self`
        // borrowed it exclusively).
        unsafe { producer.write_frame(pad, len, None) };
    }
}

impl<P: WaitStrategy, C: WaitStrategy> core::ops::Deref for WriteSlot<'_, P, C> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `payload` points at the `len` reserved bytes (always
        // initialized memory); the producer exclusively owns this
        // unpublished region.
        unsafe { std::slice::from_raw_parts(self.payload.as_ptr(), self.len) }
    }
}

impl<P: WaitStrategy, C: WaitStrategy> core::ops::DerefMut for WriteSlot<'_, P, C> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as for `deref`.
        unsafe { std::slice::from_raw_parts_mut(self.payload.as_ptr(), self.len) }
    }
}

/// A consuming handle of a [`BytesRingBuffer`]. Owns a private byte read
/// cursor and one registry slot, and parses frames independently of every
/// other consumer. `Send` but not `Clone`; create more consumers with
/// [`subscribe`](Self::subscribe).
///
/// Dropping the consumer detaches it: it stops gating the producer and wakes
/// a producer blocked on it.
pub struct BytesConsumer<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the word buffer (cached; stable for the anchor's lifetime).
    buf: NonNull<Word>,
    /// `capacity - 1` in bytes (cached).
    mask: u64,
    /// This consumer's cursor word — the hot flush target (heap: its chunk
    /// slot; shm: its table slot's cursor; the store is identical).
    cursor_slot: NonNull<AtomicU64>,
    /// The producer's published cursor (cached raw pointer, both variants).
    write_cursor: NonNull<AtomicU64>,
    /// The shared closed word (read on would-block paths only).
    closed: NonNull<AtomicU64>,
    /// The shared producer-starving flag (read behind the lag filter).
    starving: NonNull<AtomicU64>,
    /// Next byte to read (private to this thread).
    read_cursor: u64,
    /// The value of `read_cursor` last published to the registry slot (see
    /// [`advance`](Self::advance) for the adaptive publish rule).
    published: u64,
    /// Cached snapshot of the producer's write cursor.
    write_cache: u64,
    /// Keeps the ring's memory alive, carries the wait strategies, and names
    /// the registry (heap chunks vs shm table) for the cold paths.
    anchor: ConsumerAnchor<P, C>,
}

// SAFETY: the consumer only touches consumer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesConsumer<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesConsumer<P, C> {
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

impl<P, C> BytesConsumer<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until a message is available, then return a zero-copy view of
    /// it. The message is released (this consumer's cursor advances past the
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
    /// store (and one wake-up) for the whole batch, giving a deterministic
    /// publish granularity.
    ///
    /// Returns `0` on an empty ring; a closed ring is **not** reported here
    /// (a drained, closed ring also returns `0`) — use
    /// [`pop`](Self::pop)/[`try_pop`](Self::try_pop) to observe [`Closed`].
    ///
    /// The private cursor advances over each record *before* `f` sees it,
    /// and the publish happens even if `f` panics (an unwound drain never
    /// re-delivers already-processed messages to this consumer). The slice
    /// handed to `f` stays valid throughout: the producer cannot reuse the
    /// batch's bytes until the final publish, which is strictly after `f`.
    pub fn drain<F: FnMut(&[u8])>(&mut self, mut f: F) -> usize {
        // Unconditionally refresh: the contract is "what is currently in the
        // ring", which a stale non-empty cache must not bound.
        let end = self.refresh();
        if end.wrapping_sub(self.read_cursor) == 0 {
            return 0;
        }
        let batch = publish_batch(self.mask + 1);
        let start = self.read_cursor;

        // Publish on exit — including an unwind out of `f` (the engine's
        // `FlushOnDrop` guard over this consumer's `flush_pending`).
        let guard = FlushOnDrop(self);
        let base = guard.0.base();
        let mask = guard.0.mask;
        let mut count = 0;

        while end.wrapping_sub(guard.0.read_cursor) != 0
            && guard.0.read_cursor.wrapping_sub(start) < batch
        {
            // SAFETY: records below `end` are fully published.
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

    /// Subscribe a further consumer; see [`BytesProducer::subscribe`].
    pub fn subscribe(&self) -> Result<BytesConsumer<P, C>, SubscribeError> {
        match &self.anchor {
            ConsumerAnchor::Heap { shared, .. } => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this ring kind
            // and capacity when this handle was constructed.
            ConsumerAnchor::Shm(anchor) => unsafe {
                shm_subscribe(anchor.region(), anchor.max_slots(), self.mask + 1)
            },
        }
    }

    /// Whether this consumer has nothing to read. Exact on this side: uses
    /// the consumer's private cursor, which is always current.
    #[inline]
    pub fn is_empty(&self) -> bool {
        // SAFETY: `write_cursor` points into the live shared state.
        unsafe { self.write_cursor.as_ref() }
            .load(Ordering::Acquire)
            .wrapping_sub(self.read_cursor)
            == 0
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 8).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len((self.mask + 1) as usize)
    }

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr().cast::<u8>()
    }

    /// Bytes available per the cached view of the producer's cursor.
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

    /// Common tail of `pop`/`try_pop`: availability is already confirmed;
    /// decode the record at the read cursor. Skipped wrap padding is folded
    /// into the private cursor here; [`Msg`]'s drop advances (and accounts)
    /// the record itself.
    #[inline(always)]
    fn next_msg(&mut self) -> Msg<'_, P, C> {
        // SAFETY: availability was confirmed by the caller.
        let (cur, len, payload) =
            unsafe { decode_record(self.base(), self.mask, self.read_cursor) };
        self.read_cursor = cur;
        // SAFETY: `payload` is derived from the non-null buffer base.
        let payload = unsafe { NonNull::new_unchecked(payload.cast_mut()) };
        Msg {
            payload,
            len,
            consumer: self,
        }
    }

    /// Release `amount` just-consumed bytes with the adaptive publish:
    /// immediate when caught up, batched (`capacity / 8`, max 4096 bytes)
    /// while backed up — plus the **lag-filtered starving release** [M-F8]:
    /// when the producer's starving flag is up, publish immediately, but
    /// only if this consumer could actually be the gate. The filter is
    /// consumer-local: the flag carries the blocked push's **actual
    /// required span** (pad + record bytes — constant for the whole
    /// episode, because the blocked producer's write cursor cannot move and
    /// the framing is a pure function of it), so the gating consumer's
    /// *published* occupancy provably exceeds `capacity - span`; a consumer
    /// below that exact threshold is not the gate and keeps batching. The
    /// check runs against `published` — not the private cursor — so
    /// deferred progress and skipped wrap padding cannot make a true gate
    /// look innocent.
    ///
    /// The filter stays conservative under staleness: a stale `write_cache`
    /// can only *under*-state occupancy (defer the flush, never publish a
    /// wrong cursor), and a deferred gate still flushes on the caught-up or
    /// batch triggers below, which bounds the producer's extra wait by one
    /// refresh cycle.
    #[inline(always)]
    fn advance(&mut self, amount: u64) {
        let capacity = self.mask + 1;
        // SAFETY: `starving` points into the live shared state.
        let span = unsafe { self.starving.as_ref() }.load(Ordering::Acquire);
        let publish_now =
            span != 0 && self.write_cache.wrapping_sub(self.published) >= capacity - span;
        self.read_cursor = self.read_cursor.wrapping_add(amount);
        if publish_now
            || self.read_cursor == self.write_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch(capacity)
        {
            self.flush();
        }
    }

    /// Publish the private read cursor to this consumer's registry slot and
    /// wake a producer blocked on the gate (a no-op for spin strategies).
    ///
    /// Guarded by slot-lease ownership on shm rings (heap: no check at all);
    /// see `crate::spmc::Consumer::flush` for the zombie rationale [A-4.1].
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

impl<P: WaitStrategy, C: WaitStrategy> FlushPending for BytesConsumer<P, C> {
    #[inline(always)]
    fn flush_pending(&mut self) {
        BytesConsumer::flush_pending(self);
    }
}

/// A zero-copy view of one received message.
///
/// Dereferences to the payload bytes, which live in the ring itself — shared
/// with every other consumer reading the same record, so the view is
/// read-only. The message is released — this consumer's cursor published
/// past the record (and any wrap padding it skipped) with the adaptive,
/// lag-filtered publish (see [`BytesConsumer::drain`] and the module docs) —
/// when this drops. Copy out anything you need to keep.
///
/// Forgetting the guard (`mem::forget`) does **not** consume the message:
/// the cursor never advances, so the *same message is delivered again* by
/// this consumer's next pop or drain. Safe — but the un-advanced cursor also
/// gates the producer globally, so forget-then-idle stalls the whole ring
/// for every consumer. That is the gating contract, not a leak.
pub struct Msg<'a, P: WaitStrategy, C: WaitStrategy> {
    consumer: &'a mut BytesConsumer<P, C>,
    /// Payload start, cached when the record was framed (the same
    /// handle-caching idea as the cursors: compute `cursor & mask` once, not
    /// on every deref).
    payload: NonNull<u8>,
    len: usize,
}

impl<P: WaitStrategy, C: WaitStrategy> core::ops::Deref for Msg<'_, P, C> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `payload` points at this record's `len` payload bytes,
        // which are contiguous, in bounds, and fully published; the producer
        // cannot overwrite them until this consumer's cursor advances (on
        // drop of this guard). Other consumers only ever read.
        unsafe { std::slice::from_raw_parts(self.payload.as_ptr(), self.len) }
    }
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for Msg<'_, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Release the record (the skipped wrap padding was already folded
        // into the private cursor by `next_msg`; `advance` publishes both
        // together). See `BytesConsumer::advance` for the adaptive publish
        // and the lag-filtered starving release.
        self.consumer.advance(record_len(self.len) as u64);
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). Mirrors `crate::spmc`'s: ordinary handles over region
// pointers, differing only in the registry seam.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<P: WaitStrategy, C: WaitStrategy> BytesProducer<P, C> {
    /// Build a producer over a validated shm region (see
    /// `crate::spmc::Producer::from_shm` — same always-gating cache seeding
    /// [M-F17] followed by one real construction-time rescan, so
    /// [`is_empty`](BytesProducer::is_empty) never lies "full" before the
    /// first push; the caller has already reset the starving flag, mirroring
    /// the SPSC engine's attach).
    ///
    /// # Safety
    ///
    /// The anchor's region must be a validated SPMC byte ring of `capacity`
    /// bytes, and the anchor must hold the producer lease.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::GateShmProducer<C>>,
        capacity: usize,
    ) -> Self {
        let region = anchor.region();
        let write_cursor = NonNull::from(region.spmc_write_cursor());
        let closed = NonNull::from(region.spmc_closed());
        let starving = NonNull::from(region.spmc_aux());
        let buf = region.spmc_buffer(anchor.max_slots()).cast::<Word>();
        // SAFETY: `write_cursor` references the live mapping (per contract).
        let next_seq = unsafe { write_cursor.as_ref() }.load(Ordering::Acquire);
        let mut producer = BytesProducer {
            buf,
            mask: capacity as u64 - 1,
            next_seq,
            // Always-gating seed — the pre-scan value only [M-F17]; the
            // rescan below replaces it before anything reads it.
            cached_min: next_seq.wrapping_sub(capacity as u64),
            cached_cursors: Vec::new(),
            raised_starving: false,
            write_cursor,
            closed,
            starving,
            anchor: ProducerAnchor::Shm(anchor),
        };
        // One real registry rescan at construction (see the element twin):
        // the gating view reflects the live table from the start instead of
        // lying "full" until the first push.
        producer.rescan(1);
        producer
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<P: WaitStrategy, C: WaitStrategy> BytesConsumer<P, C> {
    /// Build a consumer over a claimed table slot (see
    /// `crate::spmc::Consumer::from_shm`).
    ///
    /// # Safety
    ///
    /// As for [`BytesProducer::from_shm`]; the anchor must hold a slot
    /// claimed by the `crate::shm` claim choreography whose cursor word
    /// currently holds (the sentinel-guarded image of) `read_cursor`, which
    /// must be a record boundary.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::GateShmConsumer<P, C>>,
        capacity: usize,
        read_cursor: u64,
    ) -> Self {
        let region = anchor.region();
        let write_cursor = NonNull::from(region.spmc_write_cursor());
        let closed = NonNull::from(region.spmc_closed());
        let starving = NonNull::from(region.spmc_aux());
        let cursor_slot = NonNull::from(region.slot_cursor(anchor.slot()));
        let buf = region.spmc_buffer(anchor.max_slots()).cast::<Word>();
        BytesConsumer {
            buf,
            mask: capacity as u64 - 1,
            cursor_slot,
            write_cursor,
            closed,
            starving,
            read_cursor,
            published: guard_sentinel(read_cursor),
            write_cache: read_cursor,
            anchor: ConsumerAnchor::Shm(anchor),
        }
    }

    /// The consumer-table slot this handle occupies in its shared-memory
    /// region, or `None` for a heap ring (see
    /// [`shm_slot_epoch`](Self::shm_slot_epoch) for the pair
    /// [`force_detach_consumer`](BytesRingBuffer::force_detach_consumer)
    /// takes).
    #[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
    pub fn shm_slot(&self) -> Option<usize> {
        self.shm_slot_epoch().map(|(slot, _)| slot)
    }

    /// The consumer-table `(slot, epoch)` this handle occupies in its
    /// shared-memory region, or `None` for a heap ring. The pair identifies
    /// this exact *occupancy* — every claim bumps the slot's epoch — and is
    /// what
    /// [`force_detach_consumer`](BytesRingBuffer::force_detach_consumer)
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

/// Subscribe through a live shm handle (see `crate::spmc::shm_subscribe`;
/// the join point is a record boundary because the producer publishes whole
/// frames).
///
/// # Safety
///
/// `region` must be the validated SPMC byte region (`capacity` bytes,
/// `max_consumers` table slots) the calling handle was built over.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
unsafe fn shm_subscribe<P, C>(
    region: &Arc<crate::shm::ShmRegion>,
    max_consumers: usize,
    capacity: u64,
) -> Result<BytesConsumer<P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    const TABLE: usize = crate::shm::SPMC_TABLE_OFFSET;
    let region = Arc::clone(region);
    // Seqlock snapshot + post-claim re-check, exactly as in
    // `crate::spmc::shm_subscribe` (see there for the reset-race rationale).
    let generation = region.generation();
    if generation & 1 == 1 {
        return Err(SubscribeError::Closed);
    }
    if region.spmc_closed().load(Ordering::Acquire) != 0 {
        return Err(SubscribeError::Closed);
    }
    let claim =
        crate::shm::claim_table_slot(&region, TABLE, max_consumers).ok_or(SubscribeError::Full)?;
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
    Ok(unsafe { BytesConsumer::from_shm(anchor, capacity as usize, joined) })
}
