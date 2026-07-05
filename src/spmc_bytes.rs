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
//! * **Monotonic masked byte cursors** compared by wrapped difference
//!   everywhere (sound at 2^32 wraparound on 32-bit targets).
//! * **Producer-local gating cache**: the common-case space check touches no
//!   shared line; a gate miss walks a bitmap of active registry slots,
//!   reloading only the cursors that are actually blocking (`Relaxed` loads,
//!   one trailing `Acquire` fence).
//! * **Adaptive read-cursor publish** per consumer: immediate when caught
//!   up, batched (`capacity / 8`, max 4096 bytes) while backed up — plus a
//!   **lag-filtered starving release**: when the producer signals it is
//!   starving, only a consumer far enough behind to possibly be the gate
//!   publishes per message (see [`Msg`]); the other N-1 keep batching.
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
use std::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::cursor::{publish_batch, round_capacity};
use crate::spsc_bytes::{ALIGN, MIN_CAPACITY};
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

/// Size of the length header preceding each payload (mirrors
/// `crate::spsc_bytes` — the framing is normatively identical).
const HEADER: usize = 4;
/// Header value marking a padding record that runs to the end of the buffer.
const PADDING: u32 = u32::MAX;

/// Registry slot sentinel: no consumer owns this slot. A correctness
/// backstop *under* the bitmap — the producer skips a slot that reads
/// `DETACHED` even when its bitmap bit is (transiently) set.
const DETACHED: usize = usize::MAX;

/// Registry chunk width: one bitmap word of consumer slots.
const CHUNK_SLOTS: usize = 64;

/// The byte ring's clamp for the shared publish-batch policy: at most 4096
/// bytes of deferred, already-consumed progress per consumer — bounding the
/// absolute amount of freed-but-unpublished space a blocked producer can be
/// waiting behind (and, via the gate, at most *one* consumer's deferral).
const MAX_PUBLISH_BATCH_BYTES: usize = 4096;

#[inline(always)]
const fn align_up(n: usize) -> usize {
    (n + (ALIGN - 1)) & !(ALIGN - 1)
}

/// Bytes a record with a `len`-byte payload occupies in the ring.
#[inline(always)]
const fn record_len(len: usize) -> usize {
    align_up(HEADER + len)
}

#[inline(always)]
const fn max_message_len(capacity: usize) -> usize {
    // Records are capped at capacity / 2 (see module docs); the header is
    // part of the record. Also stay below the u32 header space, where
    // u32::MAX is reserved for padding.
    let cap = capacity / 2 - HEADER;
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
/// Derivation from the framing: a record is at most
/// `R = record_len(max_message_len)` bytes, and padding is written only when
/// the record does not fit before the end of the buffer, so
/// `pad = to_end < R`. Both are multiples of [`ALIGN`], hence
/// `pad <= R - ALIGN` and `pad + record <= 2R - ALIGN`. This is what the
/// consumers' lag filter (see [`Msg`]) is derived from: a blocked producer
/// implies its gating consumer's published occupancy exceeds
/// `capacity - max_record_span`, so a consumer below that cannot be the
/// gate.
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
unsafe fn decode_record(base: *const u8, mask: usize, mut cur: usize) -> (usize, usize, *const u8) {
    let mut pos = cur & mask;
    // SAFETY: header reads are 4-aligned (records and padding are ALIGN
    // multiples, base is 8-aligned) and in bounds via the mask.
    let mut header = u32::from_le(unsafe { base.add(pos).cast::<u32>().read() });
    if header == PADDING {
        cur = cur.wrapping_add((mask + 1) - pos);
        pos = 0;
        // SAFETY: as above, at offset zero.
        header = u32::from_le(unsafe { base.cast::<u32>().read() });
        debug_assert!(header != PADDING, "padding is never followed by padding");
    }
    let len = header as usize;
    // SAFETY: the record is contiguous: `pos + HEADER + len <= capacity`.
    (cur, len, unsafe { base.add(pos + HEADER) })
}

/// The wrap-safe fullness predicate: would writing `needed` more bytes past
/// `write` overrun a `capacity`-byte ring whose (slowest) consumer has read
/// up to `read`? The single source of truth for "gated", in the same
/// wrapped-difference form as the SPSC engine — never an absolute compare
/// (32-bit cursors wrap after 2^32 bytes).
#[inline(always)]
const fn lacks_space(write: usize, needed: usize, read: usize, capacity: usize) -> bool {
    write.wrapping_add(needed).wrapping_sub(read) > capacity
}

/// The producer-published cache line: the write cursor plus, co-located in
/// the same padded slot, the `closed` flag (written once by
/// `BytesProducer::drop`, read only on consumer would-block paths) and the
/// `starving` flag (raised by the producer when a space check fails against
/// the freshest registry scan, cleared with hysteresis; read by consumers on
/// message release, behind the lag filter). Consumers already poll this line
/// for the write cursor, so neither flag adds coherence traffic.
struct WriteSide {
    write_cursor: AtomicUsize,
    closed: AtomicBool,
    starving: AtomicUsize,
}

/// One 64-slot block of the consumer registry.
///
/// `bitmap` marks the active slots (written only on subscribe/detach — cold;
/// L1-resident for the producer's rescans). Each cursor slot is written by
/// exactly one consumer and sits on its own padded line. `next` links the
/// append-only chunk list; chunks are never moved or freed until the shared
/// state drops, so cached chunk pointers stay valid for the ring's lifetime.
struct Chunk {
    bitmap: CachePadded<AtomicU64>,
    next: AtomicPtr<Chunk>,
    slots: [CachePadded<AtomicUsize>; CHUNK_SLOTS],
}

impl Chunk {
    fn new() -> Self {
        Self {
            bitmap: CachePadded::new(AtomicU64::new(0)),
            next: AtomicPtr::new(std::ptr::null_mut()),
            slots: std::array::from_fn(|_| CachePadded::new(AtomicUsize::new(DETACHED))),
        }
    }
}

/// The state all handles share, kept alive by an `Arc`.
struct Shared<P, C> {
    buffer: Box<[Word]>,
    mask: usize,
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
        let mut next = *self.registry.next.get_mut();
        while !next.is_null() {
            // SAFETY: appended chunks were created via `Box::into_raw` and
            // are unreachable now (no handle outlives the shared state).
            let chunk = unsafe { Box::from_raw(next) };
            next = chunk.next.load(Ordering::Relaxed);
        }
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
            mask: capacity - 1,
            write_side: CachePadded::new(WriteSide {
                write_cursor: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                starving: AtomicUsize::new(0),
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
            mask: capacity - 1,
            next_seq: 0,
            cached_min: 0,
            cached_cursors: Vec::new(),
            raised_starving: false,
            shared,
        };
        (producer, consumer)
    }
}

/// Register a new consumer on live shared state — the Disruptor
/// `addSequences` choreography [M-F2]. The naive CAS-once protocol is
/// formally broken: store-buffering lets the producer's scan miss the joiner
/// while the joiner reads a stale write cursor. The `SeqCst` fence here
/// pairs with the producer's pre-scan fence, so at least one side sees the
/// other; the **join point is the post-fence re-read** of the write cursor —
/// always a record boundary, since the producer publishes whole frames.
fn subscribe_from<P, C>(shared: &Arc<Shared<P, C>>) -> Result<BytesConsumer<P, C>, SubscribeError>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    // Clone the Arc *before* touching the registry [A-2.2]: the new slot can
    // never outlive the shared state it points into, making the
    // subscribe-vs-teardown race structurally unreachable.
    let shared = Arc::clone(shared);
    if shared.write_side.closed.load(Ordering::Acquire) {
        return Err(SubscribeError::Closed);
    }

    // 1. Claim a free registry slot with a provisional cursor.
    let (chunk, slot_idx) = claim_registry_slot(&shared);
    // SAFETY: chunks live until `Shared::drop`, and we hold the `Arc`.
    let chunk_ref = unsafe { chunk.as_ref() };

    // 2. Activate the slot for the producer's rescans (cold RMW). This MUST
    //    precede the fence below: the rescan observes consumers only through
    //    the bitmap, so the bit — not the slot store — is the registration
    //    the [M-F2] dichotomy is about. Set after the fence, a scan could
    //    miss the bit *while* the re-read below returns a stale cursor, and
    //    the producer would lap a consumer it never saw. The slot already
    //    holds the provisional cursor (a lower bound of the join point), so
    //    a scan that sees the bit this early only gates more.
    chunk_ref
        .bitmap
        .fetch_or(1u64 << slot_idx, Ordering::AcqRel);

    // 3. Pair with the producer's pre-scan fence [M-F2]: either that scan's
    //    bitmap load sees the bit set above, or this fence follows the
    //    scan's in the SC order and the re-read below returns a write cursor
    //    at least as fresh as the scan's wrap point.
    fence(Ordering::SeqCst);

    // 4. The join point: re-read the write cursor and publish it as this
    //    consumer's cursor. Only messages published after `joined` are seen.
    let joined = shared.write_side.write_cursor.load(Ordering::Acquire);
    let published = guard_sentinel(joined);
    chunk_ref.slots[slot_idx].store(published, Ordering::Release);

    let buf = NonNull::new(shared.buffer.as_ptr().cast_mut()).expect("buffer is non-null");
    let mask = shared.mask;
    Ok(BytesConsumer {
        buf,
        mask,
        chunk,
        slot_idx,
        read_cursor: joined,
        published,
        write_cache: joined,
        shared,
    })
}

/// A cursor value about to be stored into a registry slot must never equal
/// the `DETACHED` sentinel (reachable only at exact cursor wraparound —
/// 2^32 bytes in on 32-bit targets). Publishing one unit less is always
/// safe: a lower published cursor only gates the producer more.
#[inline(always)]
const fn guard_sentinel(cursor: usize) -> usize {
    if cursor == DETACHED {
        cursor.wrapping_sub(1)
    } else {
        cursor
    }
}

/// Find (or append) a registry slot and claim it: CAS `DETACHED` → a
/// provisional read of the write cursor.
///
/// Only slots whose bitmap bit is **clear** are candidates: a detaching
/// consumer stores `DETACHED` *before* clearing its bit, so observing the
/// bit clear (`Acquire`, pairing with the detacher's `AcqRel` RMW) proves
/// the detach fully completed — claiming a mid-detach slot would let the
/// departing consumer's belated bitmap clear erase the newcomer's bit and
/// un-gate it forever.
fn claim_registry_slot<P, C>(shared: &Shared<P, C>) -> (NonNull<Chunk>, usize) {
    let mut chunk: &Chunk = &shared.registry;
    loop {
        let bitmap = chunk.bitmap.load(Ordering::Acquire);
        let mut free = !bitmap;
        while free != 0 {
            let idx = free.trailing_zeros() as usize;
            free &= free - 1;
            let provisional =
                guard_sentinel(shared.write_side.write_cursor.load(Ordering::Acquire));
            // A bit-clear slot that is not DETACHED is a concurrent joiner
            // that has not set its bit yet; skip it.
            if chunk.slots[idx]
                .compare_exchange(DETACHED, provisional, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return (NonNull::from(chunk), idx);
            }
        }
        let next = chunk.next.load(Ordering::Acquire);
        if !next.is_null() {
            // SAFETY: chunks are never freed while the shared state lives.
            chunk = unsafe { &*next };
            continue;
        }
        // Every chunk is full: cold-append a new one. On a lost CAS race the
        // winner's chunk is used (and searched) instead.
        let fresh = Box::into_raw(Box::new(Chunk::new()));
        match chunk.next.compare_exchange(
            std::ptr::null_mut(),
            fresh,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            // SAFETY: we just leaked `fresh`; it is live and now published.
            Ok(_) => chunk = unsafe { &*fresh },
            Err(winner) => {
                // SAFETY: `fresh` was never published; reclaim and free it.
                drop(unsafe { Box::from_raw(fresh) });
                // SAFETY: the winner's chunk is published and never freed.
                chunk = unsafe { &*winner };
            }
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
    mask: usize,
    /// Next byte to write (private; the published cursor trails it by the
    /// not-yet-committed claim, if any).
    next_seq: usize,
    /// Cached minimum of the active consumers' byte cursors — the gate. A
    /// lower bound; the fast-path space check touches no shared line.
    cached_min: usize,
    /// Per-slot cached consumer cursors, mirroring the registry geometry
    /// (one 64-wide block per chunk, sized lazily). Monotonicity makes every
    /// cached value a permanent lower bound — for later occupants of the
    /// slot too, since a joiner's cursor starts at the then-current write
    /// cursor, which any earlier cached value cannot exceed [P-F3].
    cached_cursors: Vec<[usize; CHUNK_SLOTS]>,
    /// Whether we raised the starving flag and have not yet cleared it
    /// (producer-local; keeps the never-starved hot path free of any flag
    /// access).
    raised_starving: bool,
    /// Keeps the ring's memory alive and carries the wait strategies.
    shared: Arc<Shared<P, C>>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointer references the `Arc<Shared>` it keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesProducer<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesProducer<P, C> {
    fn drop(&mut self) {
        // Flag-then-notify [A-1.1]: a consumer that checked the flag just
        // before this store is parked (or about to park) in a wait whose
        // predicate re-checks `closed`, and the notify wakes it.
        self.shared.write_side.closed.store(true, Ordering::Release);
        self.shared.consumer_wait.notify();
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
    pub fn subscribe(&self) -> Result<BytesConsumer<P, C>, SubscribeError> {
        subscribe_from(&self.shared)
    }

    /// Number of currently attached consumers (a registry scan — cold; a
    /// racing subscribe/detach makes it a snapshot, not a guarantee).
    pub fn consumer_count(&self) -> usize {
        let mut chunk: &Chunk = &self.shared.registry;
        let mut count = 0usize;
        loop {
            count += chunk.bitmap.load(Ordering::Relaxed).count_ones() as usize;
            let next = chunk.next.load(Ordering::Acquire);
            if next.is_null() {
                return count;
            }
            // SAFETY: chunks are never freed while the shared state lives.
            chunk = unsafe { &*next };
        }
    }

    /// Fast space check against the cached gating minimum; on a miss, one
    /// full registry rescan. Zero shared loads in the common case. Also
    /// maintains the starving flag with hysteresis (mirrors the SPSC byte
    /// engine): raised once per episode when even a full rescan leaves no
    /// room, kept up while space only appears via rescans, cleared once the
    /// cached check passes comfortably.
    #[inline(always)]
    fn has_space(&mut self, needed: usize) -> bool {
        if lacks_space(self.next_seq, needed, self.cached_min, self.mask + 1) {
            if self.rescan(needed) {
                // Space appeared only after a rescan: still running tight —
                // keep the flag up (hysteresis; no store churn while the
                // ring hovers at the edge of starvation).
                return true;
            }
            // Starving: even the freshest registry scan leaves no room.
            // Raise the flag once per episode (set-if-zero: while starvation
            // persists this is a read of a line the producer polls anyway)
            // so the gating consumer's lag-filtered release can free us.
            let starving = &self.shared.write_side.starving;
            if starving.load(Ordering::Relaxed) == 0 {
                starving.store(1, Ordering::Release);
            }
            self.raised_starving = true;
            return false;
        }
        // The *cached* check passed: comfortably unstarved. Clear our flag
        // once; the local bool keeps this branch untaken (a register test)
        // on the never-starved hot path.
        if self.raised_starving {
            self.raised_starving = false;
            self.shared.write_side.starving.store(0, Ordering::Release);
        }
        true
    }

    /// Spin/park (per the producer wait strategy) until the gate opens.
    #[inline(always)]
    fn wait_for_space(&mut self, needed: usize) {
        if self.has_space(needed) {
            return;
        }
        // A separate handle on the wait strategy, so the predicate below can
        // borrow `self` mutably (cold path; one refcount bump).
        let shared = Arc::clone(&self.shared);
        while !self.has_space(needed) {
            // The predicate re-runs the FULL scan [M-F4]: a cached minimum
            // here is a deadlock, and rescanning is also what lets the wait
            // terminate when every gating consumer detaches (the detach
            // raises the minimum or empties the registry).
            shared.producer_wait.wait(|| self.rescan(needed));
        }
    }

    /// The gate-miss slow path: rescan the registry and recompute
    /// `cached_min`. Returns whether `needed` bytes are now free.
    fn rescan(&mut self, needed: usize) -> bool {
        // Disruptor `setVolatile` analog: pairs with the subscriber's fence
        // [M-F2] — either this scan sees the joiner's registration, or the
        // joiner's post-fence re-read saw a write cursor at least as high as
        // everything we published before this fence, so its cursor cannot be
        // behind our current wrap point.
        fence(Ordering::SeqCst);
        let capacity = self.mask + 1;
        let mut any_active = false;
        let mut max_lag = 0usize;
        let mut ci = 0usize;
        let mut chunk: &Chunk = &self.shared.registry;
        loop {
            if self.cached_cursors.len() == ci {
                // Fresh cache block: seed with a value that always compares
                // as gating (lag == capacity), forcing a real load before
                // first use — 0 would be wrong after cursor wraparound.
                self.cached_cursors
                    .push([self.next_seq.wrapping_sub(capacity); CHUNK_SLOTS]);
            }
            let cache = &mut self.cached_cursors[ci];
            let mut bits = chunk.bitmap.load(Ordering::Relaxed);
            while bits != 0 {
                let idx = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let mut cursor = cache[idx];
                // Selective refresh [P-F3]: reload only slots whose cached
                // cursor is still behind the wrap point — monotonicity makes
                // cached values permanent lower bounds, so a slot already
                // known past the wrap point cannot be gating.
                if lacks_space(self.next_seq, needed, cursor, capacity) {
                    // Relaxed: the single Acquire fence below orders the
                    // whole batch, so the cache misses overlap in the MLP
                    // window instead of serializing [P-F1].
                    let fresh = chunk.slots[idx].load(Ordering::Relaxed);
                    if fresh == DETACHED {
                        // Backstop: a mid-detach slot (bit still set) imposes
                        // no constraint; do not poison the cache with the
                        // sentinel.
                        continue;
                    }
                    cache[idx] = fresh;
                    cursor = fresh;
                }
                any_active = true;
                let lag = self.next_seq.wrapping_sub(cursor);
                if lag > max_lag {
                    max_lag = lag;
                }
            }
            let next = chunk.next.load(Ordering::Acquire);
            if next.is_null() {
                break;
            }
            // SAFETY: chunks are never freed while the shared state lives.
            chunk = unsafe { &*next };
            ci += 1;
        }
        // One fence for the whole scan [P-F1]: everything the gating
        // consumers did before publishing the cursors read above (their last
        // reads of the bytes we are about to overwrite) happens-before our
        // writes after this fence.
        fence(Ordering::Acquire);
        self.cached_min = if any_active {
            // The minimum in wrapped terms: the cursor with the largest
            // wrapped distance behind `next_seq`.
            self.next_seq.wrapping_sub(max_lag)
        } else {
            // Empty registry: the producer's own published position, NEVER
            // an unbounded value [M-F1] — an unbounded cache would disable
            // the only rescan trigger and make joiners invisible for
            // unbounded laps (torn reads over reused bytes). Own-cursor
            // keeps an audience-less producer free-running (any legal frame
            // needs at most `capacity - ALIGN` bytes, see
            // [`max_record_span`]) while forcing at least one rescan per
            // lap.
            self.next_seq.wrapping_sub(1)
        };
        !lacks_space(self.next_seq, needed, self.cached_min, capacity)
    }

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr().cast::<u8>()
    }

    /// Where the payload of a record claimed with `pad` bytes of wrap
    /// padding will start.
    #[inline(always)]
    fn payload_ptr(&self, pad: usize) -> NonNull<u8> {
        let pos = self.next_seq.wrapping_add(pad) & self.mask;
        // SAFETY: in bounds — `frame` reserved `HEADER + len` contiguous
        // bytes starting at `pos`, and the buffer base is non-null.
        unsafe { NonNull::new_unchecked(self.base().add(pos + HEADER)) }
    }

    /// Compute the record framing for a `len`-byte message at the current
    /// write position: `(padding_bytes, total_bytes_consumed)`.
    #[inline]
    fn frame(&self, len: usize) -> (usize, usize) {
        let capacity = self.mask + 1;
        assert!(
            len <= max_message_len(capacity),
            "message length {len} exceeds max_message_len ({})",
            max_message_len(capacity),
        );
        let record = record_len(len);
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
    unsafe fn write_frame(&mut self, pad: usize, len: usize, src: Option<*const u8>) {
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
                base.add(cur & mask).cast::<u32>().write(PADDING.to_le());
                cur = cur.wrapping_add(pad); // now at a capacity boundary
            }
            let pos = cur & mask;
            // Headers are little-endian on every target, as the module docs
            // promise (free on LE machines; a byte swap on BE ones).
            base.add(pos).cast::<u32>().write((len as u32).to_le());
            if let Some(src) = src {
                std::ptr::copy_nonoverlapping(src, base.add(pos + HEADER), len);
            }
        }

        // One publish covers the padding and the record together.
        self.publish(pad + record_len(len));
    }

    /// Advance and publish the write cursor (one `Release` store), then wake
    /// blocked consumers (a no-op for the spin strategies).
    #[inline(always)]
    fn publish(&mut self, amount: usize) {
        self.next_seq = self.next_seq.wrapping_add(amount);
        self.shared
            .write_side
            .write_cursor
            .store(self.next_seq, Ordering::Release);
        self.shared.consumer_wait.notify();
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
        self.mask + 1
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len(self.mask + 1)
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
    pad: usize,
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
    /// Base of the word buffer (cached; stable for the `Arc`'s lifetime).
    buf: NonNull<Word>,
    /// `capacity - 1` in bytes (cached).
    mask: usize,
    /// The registry chunk holding this consumer's cursor slot (chunks are
    /// never moved or freed while the `Arc` lives).
    chunk: NonNull<Chunk>,
    /// Index of this consumer's slot within `chunk`.
    slot_idx: usize,
    /// Next byte to read (private to this thread).
    read_cursor: usize,
    /// The value of `read_cursor` last published to the registry slot (see
    /// [`advance`](Self::advance) for the adaptive publish rule).
    published: usize,
    /// Cached snapshot of the producer's write cursor.
    write_cache: usize,
    /// Keeps the ring's memory alive and carries the wait strategies.
    shared: Arc<Shared<P, C>>,
}

// SAFETY: the consumer only touches consumer-private state plus atomics; the
// cached pointers reference the `Arc<Shared>` it keeps alive.
unsafe impl<P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for BytesConsumer<P, C>
{
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for BytesConsumer<P, C> {
    fn drop(&mut self) {
        // Publish any deferred progress first (harmless — the detach store
        // below supersedes it, but a concurrent rescan between the two sees
        // the freshest cursor instead of a stale one).
        self.flush_pending();
        // SAFETY: the chunk lives until `Shared::drop`; we hold the `Arc`.
        let chunk = unsafe { self.chunk.as_ref() };
        // Detach order matters: sentinel first, then the bitmap bit — a
        // subscriber only claims slots whose bit is clear, which proves this
        // whole sequence completed (see `claim_registry_slot`).
        chunk.slots[self.slot_idx].store(DETACHED, Ordering::Release);
        chunk
            .bitmap
            .fetch_and(!(1u64 << self.slot_idx), Ordering::AcqRel);
        // The missing dual of the producer's close-notify [A-1.3]: a
        // producer parked waiting for the minimum to move would stall
        // forever if its last gating consumer detached silently.
        self.shared.producer_wait.notify();
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
        let batch = publish_batch(self.mask + 1, MAX_PUBLISH_BATCH_BYTES);
        let start = self.read_cursor;

        // Publish on exit — including an unwind out of `f`.
        struct FlushOnDrop<'a, P: WaitStrategy, C: WaitStrategy>(&'a mut BytesConsumer<P, C>);
        impl<P: WaitStrategy, C: WaitStrategy> Drop for FlushOnDrop<'_, P, C> {
            fn drop(&mut self) {
                self.0.flush_pending();
            }
        }

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
            guard.0.read_cursor = cur.wrapping_add(record_len(len));
            // SAFETY: payload is contiguous, in bounds, and fully published.
            f(unsafe { std::slice::from_raw_parts(payload, len) });
            count += 1;
        }
        count
    }

    /// Subscribe a further consumer; see [`BytesProducer::subscribe`].
    pub fn subscribe(&self) -> Result<BytesConsumer<P, C>, SubscribeError> {
        subscribe_from(&self.shared)
    }

    /// Whether this consumer has nothing to read. Exact on this side: uses
    /// the consumer's private cursor, which is always current.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared
            .write_side
            .write_cursor
            .load(Ordering::Acquire)
            .wrapping_sub(self.read_cursor)
            == 0
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two, minimum 8).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len(self.mask + 1)
    }

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.buf.as_ptr().cast::<u8>()
    }

    /// Bytes available per the cached view of the producer's cursor.
    #[inline(always)]
    fn available_cached(&self) -> usize {
        self.write_cache.wrapping_sub(self.read_cursor)
    }

    /// Unconditionally reload the cached view of the producer's cursor
    /// (`Acquire`) and return it.
    #[inline(always)]
    fn refresh(&mut self) -> usize {
        self.write_cache = self.shared.write_side.write_cursor.load(Ordering::Acquire);
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
        if self.shared.write_side.closed.load(Ordering::Acquire) {
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
            let write_side = &self.shared.write_side;
            let read = self.read_cursor;
            self.shared.consumer_wait.wait(|| {
                write_side
                    .write_cursor
                    .load(Ordering::Acquire)
                    .wrapping_sub(read)
                    != 0
                    || write_side.closed.load(Ordering::Acquire)
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
    /// consumer-local: a blocked producer needs at most
    /// [`max_record_span`] bytes free, so its gating consumer's *published*
    /// occupancy exceeds `capacity - max_record_span`; a consumer below
    /// that threshold provably is not the gate and keeps batching. The
    /// check runs against `published` — not the private cursor — so
    /// deferred progress and skipped wrap padding cannot make a true gate
    /// look innocent.
    ///
    /// Honesty note on the filter's strength: `max_record_span` is derived
    /// from the *worst-case* record (`2·record_len(max) − ALIGN` =
    /// `capacity − ALIGN`), so the threshold reduces to `ALIGN` bytes of
    /// published occupancy — only fully-caught-up consumers are filtered;
    /// any consumer with one unpublished record reacts during a starvation
    /// episode. That still stops caught-up consumers from flooding, but a
    /// tighter filter needs the producer's *actual* required span (tracked
    /// as a bench-guided refinement in bd).
    #[inline(always)]
    fn advance(&mut self, amount: usize) {
        let capacity = self.mask + 1;
        let publish_now = self.write_cache.wrapping_sub(self.published)
            >= capacity - max_record_span(capacity)
            && self.shared.write_side.starving.load(Ordering::Acquire) != 0;
        self.read_cursor = self.read_cursor.wrapping_add(amount);
        if publish_now
            || self.read_cursor == self.write_cache
            || self.read_cursor.wrapping_sub(self.published)
                >= publish_batch(capacity, MAX_PUBLISH_BATCH_BYTES)
        {
            self.flush();
        }
    }

    /// Publish the private read cursor to this consumer's registry slot and
    /// wake a producer blocked on the gate (a no-op for spin strategies).
    #[inline(always)]
    fn flush(&mut self) {
        // SAFETY: the chunk lives until `Shared::drop`; we hold the `Arc`.
        let chunk = unsafe { self.chunk.as_ref() };
        // Never publish the DETACHED sentinel (exact-wraparound collision);
        // one unit less only gates the producer more, and the next flush
        // publishes past it.
        chunk.slots[self.slot_idx].store(guard_sentinel(self.read_cursor), Ordering::Release);
        self.published = self.read_cursor;
        self.shared.producer_wait.notify();
    }

    /// [`flush`](Self::flush) only if there is unpublished progress.
    #[inline(always)]
    fn flush_pending(&mut self) {
        if self.read_cursor != self.published {
            self.flush();
        }
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
        self.consumer.advance(record_len(self.len));
    }
}
