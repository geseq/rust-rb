//! Single-producer / single-consumer ring buffer for **variable-size**
//! messages.
//!
//! Where [`crate::spsc::RingBuffer`] moves items of one fixed type `T`, this ring
//! transports discrete byte messages of differing lengths — serialized
//! structs, wire frames, log records — through one shared byte buffer.
//!
//! # Framing
//!
//! Each message is written as a *record*: a 4-byte little-endian length
//! header followed by the payload, with the whole record rounded up to a
//! 4-byte boundary so headers are always naturally aligned. Records never
//! wrap around the end of the buffer: if a record does not fit in the space
//! remaining before the end, the producer writes a *padding* header (length
//! `u32::MAX`) there and restarts at offset zero. The consumer skips padding
//! transparently. Keeping every record contiguous is what makes zero-copy
//! reads possible.
//!
//! Because a message may need that wrap padding *in addition to* its own
//! record, records are capped at half the capacity — this guarantees any
//! message up to [`max_message_len`](BytesProducer::max_message_len)
//! (`capacity / 2 - 4` bytes) can always be written eventually, no matter
//! where the cursors sit. Without the cap, a large message could arrive at an
//! unlucky offset where padding plus record exceed the whole buffer and never
//! fit, deadlocking a blocking `push`.
//!
//! # Why it is fast
//!
//! The concurrency machinery is the crate's shared cursor engine (the same
//! code as [`crate::spsc`], instantiated at byte granularity): monotonic
//! masked byte cursors compared by wrapped difference, per-side cursor
//! caching, cache-padded shared atomics, one `Release` store publishing a
//! whole record (padding included), and adaptive read-cursor publishes.
//! On top of that, this ring is zero-copy on both sides:
//! [`claim`](BytesProducer::claim) hands the producer a slice to serialize
//! into directly, [`pop`](BytesConsumer::pop) hands the consumer a borrowed
//! view of the payload in place, and [`drain`](BytesConsumer::drain) consumes
//! every available message with a single cursor publish.

use std::cell::UnsafeCell;
use std::ptr::NonNull;

use crate::cursor::{
    channel, publish_batch, shared_is_empty, ConsumerCore, ProducerCore, SlotCleanup,
};
use crate::wait::{WaitStrategy, YieldWait};

/// The buffer word type: `u64` so the base is 8-aligned (a `Box<[u8]>`
/// allocation only guarantees alignment 1). All access goes through raw `u8`
/// pointers; the words are never read as `u64`s.
///
/// Zero-initialized on construction so every byte is always initialized:
/// `WriteSlot`/`Msg` hand out `&[u8]` views into the ring, which would be
/// instant UB over uninitialized memory. Zeroing costs one pass at
/// construction and nothing on the hot path.
type Word = UnsafeCell<u64>;

// The byte ring's slots are plain words with nothing to drop, and its cursors
// are byte-granular, so the engine's teardown slot-walk is skipped.
impl SlotCleanup for Word {
    const NEEDS_CLEANUP: bool = false;

    #[inline]
    unsafe fn cleanup(&self) {}
}

/// Size of the length header preceding each payload.
const HEADER: usize = 4;
/// Record alignment. Keeps every header read/write naturally aligned.
const ALIGN: usize = 4;
/// Header value marking a padding record that runs to the end of the buffer.
const PADDING: u32 = u32::MAX;

#[inline(always)]
const fn align_up(n: usize) -> usize {
    (n + (ALIGN - 1)) & !(ALIGN - 1)
}

/// Bytes a record with a `len`-byte payload occupies in the ring.
#[inline(always)]
const fn record_len(len: usize) -> usize {
    align_up(HEADER + len)
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
    let mut header = unsafe { base.add(pos).cast::<u32>().read() };
    if header == PADDING {
        cur = cur.wrapping_add((mask + 1) - pos);
        pos = 0;
        // SAFETY: as above, at offset zero.
        header = unsafe { base.cast::<u32>().read() };
        debug_assert!(header != PADDING, "padding is never followed by padding");
    }
    let len = header as usize;
    // SAFETY: the record is contiguous: `pos + HEADER + len <= capacity`.
    (cur, len, unsafe { base.add(pos + HEADER) })
}

/// The byte ring's clamp for the shared publish-batch policy: at most 4096
/// bytes of deferred, already-consumed progress — bounding the absolute
/// amount of freed-but-unpublished space a blocked producer can be waiting
/// behind.
const MAX_PUBLISH_BATCH_BYTES: usize = 4096;

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

/// Builder/namespace for constructing a variable-size-message SPSC ring.
///
/// [`new`](Self::new) takes the minimum capacity in **bytes** at runtime
/// (rounded up to the next power of two, at least 8) and uses [`YieldWait`]
/// on both sides. Pick other [`WaitStrategy`]s with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (push) strategy, `C` the consumer-side (pop) strategy.
pub struct BytesRingBuffer<P = YieldWait, C = YieldWait>(core::marker::PhantomData<(P, C)>);

impl BytesRingBuffer {
    /// Create a ring with the default wait strategies and return its producer
    /// and consumer halves.
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
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    /// Create the ring with explicit wait strategies and return its producer
    /// and consumer halves.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (BytesProducer<P, C>, BytesConsumer<P, C>) {
        assert!(min_capacity > 0, "capacity must be greater than zero");
        let capacity = min_capacity
            .checked_next_power_of_two()
            .expect("capacity too large to round up to a power of two")
            .max(8);

        let (producer, consumer) = channel(capacity, capacity / 8, || UnsafeCell::new(0u64));
        (
            BytesProducer { core: producer },
            BytesConsumer { core: consumer },
        )
    }
}

/// The producing half of a [`BytesRingBuffer`]. Owns the private write cursor.
pub struct BytesProducer<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    core: ProducerCore<Word, P, C>,
}

impl<P, C> BytesProducer<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until there is room, then enqueue a copy of `msg`.
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()` — such a message could
    /// never be sent, so waiting for room would deadlock.
    #[inline]
    pub fn push(&mut self, msg: &[u8]) {
        let (pad, total) = self.frame(msg.len());
        self.core.wait_for_space(total);
        // SAFETY: `frame` sized the record and `wait_for_space` confirmed
        // `total` free bytes.
        unsafe { self.write_frame(pad, msg.len(), Some(msg.as_ptr())) };
    }

    /// Enqueue a copy of `msg` without blocking. Returns `false` if there is
    /// not enough free space.
    ///
    /// "Free" is judged against the consumer's *published* progress; while
    /// the consumer defers publishes in the backed-up regime this can
    /// spuriously fail with up to `capacity / 8` (max 4096) bytes consumed
    /// but not yet published.
    ///
    /// # Panics
    ///
    /// Panics if `msg.len() > self.max_message_len()`.
    #[inline]
    #[must_use]
    pub fn try_push(&mut self, msg: &[u8]) -> bool {
        let (pad, total) = self.frame(msg.len());
        if !self.core.has_space(total) {
            return false;
        }
        // SAFETY: as in `push`.
        unsafe { self.write_frame(pad, msg.len(), Some(msg.as_ptr())) };
        true
    }

    /// Block until there is room for a `len`-byte message, then return a
    /// [`WriteSlot`] to serialize it into — the zero-copy alternative to
    /// [`push`](Self::push). The message is published when the slot is
    /// [committed](WriteSlot::commit); dropping the slot uncommitted abandons
    /// the space for reuse by the next claim.
    ///
    /// # Panics
    ///
    /// Panics if `len > self.max_message_len()`.
    #[inline]
    pub fn claim(&mut self, len: usize) -> WriteSlot<'_, P, C> {
        let (pad, total) = self.frame(len);
        self.core.wait_for_space(total);
        let payload = self.payload_ptr(pad);
        WriteSlot {
            producer: self,
            payload,
            pad,
            len,
        }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if there is not
    /// enough free space.
    ///
    /// # Panics
    ///
    /// Panics if `len > self.max_message_len()`.
    #[inline]
    pub fn try_claim(&mut self, len: usize) -> Option<WriteSlot<'_, P, C>> {
        let (pad, total) = self.frame(len);
        if !self.core.has_space(total) {
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

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.core.buf.as_ptr().cast::<u8>()
    }

    /// Where the payload of a record claimed with `pad` bytes of wrap padding
    /// will start.
    #[inline(always)]
    fn payload_ptr(&self, pad: usize) -> NonNull<u8> {
        let pos = self.core.write_cursor.wrapping_add(pad) & self.core.mask;
        // SAFETY: in bounds — `frame` reserved `HEADER + len` contiguous
        // bytes starting at `pos`, and the buffer base is non-null.
        unsafe { NonNull::new_unchecked(self.base().add(pos + HEADER)) }
    }

    /// Compute the record framing for a `len`-byte message at the current
    /// write position: `(padding_bytes, total_bytes_consumed)`.
    #[inline]
    fn frame(&self, len: usize) -> (usize, usize) {
        let capacity = self.core.capacity();
        assert!(
            len <= max_message_len(capacity),
            "message length {len} exceeds max_message_len ({})",
            max_message_len(capacity),
        );
        let record = record_len(len);
        let to_end = capacity - (self.core.write_cursor & self.core.mask);
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
        let mask = self.core.mask;
        let mut cur = self.core.write_cursor;

        // SAFETY (whole block): offsets are `& mask`, so in bounds; every
        // record boundary is 4-aligned (records and padding are multiples of
        // ALIGN and the base is 8-aligned), so the u32 header accesses are
        // aligned; the space was confirmed free, so we never overwrite bytes
        // the consumer has yet to read.
        unsafe {
            if pad > 0 {
                // `frame` only pads mid-buffer, where at least HEADER bytes
                // remain before the end.
                base.add(cur & mask).cast::<u32>().write(PADDING);
                cur = cur.wrapping_add(pad); // now at a capacity boundary
            }
            let pos = cur & mask;
            base.add(pos).cast::<u32>().write(len as u32);
            if let Some(src) = src {
                std::ptr::copy_nonoverlapping(src, base.add(pos + HEADER), len);
            }
        }

        // One publish covers the padding and the record together.
        self.core.publish(pad + record_len(len));
    }

    /// Whether the ring currently holds no messages.
    ///
    /// The consumer publishes its progress adaptively, so this may
    /// transiently report `false` for a ring the consumer has fully drained;
    /// it never reports `true` for a non-empty ring.
    #[inline]
    pub fn is_empty(&self) -> bool {
        shared_is_empty(&self.core.inner)
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len(self.core.capacity())
    }
}

/// A claimed, not-yet-published message slot in the ring.
///
/// Dereferences to the `len`-byte payload slice so it can be serialized into
/// directly. Call [`commit`](Self::commit) to publish; dropping the slot
/// without committing publishes nothing.
///
/// The slice's initial contents are unspecified but always initialized
/// memory (zeroed at construction, previous records afterwards) — reading
/// before writing is safe but yields garbage, and committing without fully
/// writing publishes that garbage to the consumer.
pub struct WriteSlot<'a, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut BytesProducer<P, C>,
    /// Payload start, cached at claim time (the same handle-caching idea as
    /// the cursors: compute `(cursor + pad) & mask` once, not on every deref).
    payload: NonNull<u8>,
    pad: usize,
    len: usize,
}

impl<P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, P, C> {
    /// Publish the message. Writes the headers and makes the record visible
    /// to the consumer with one `Release` store.
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

/// The consuming half of a [`BytesRingBuffer`]. Owns the private read cursor.
///
/// Dropping the consumer publishes any deferred progress and wakes a blocked
/// producer (handled by the shared cursor engine).
pub struct BytesConsumer<P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    core: ConsumerCore<Word, P, C>,
}

impl<P, C> BytesConsumer<P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until a message is available, then return a zero-copy view of
    /// it. The message is released (its bytes freed for the producer) when
    /// the returned [`Msg`] drops.
    #[inline]
    pub fn pop(&mut self) -> Msg<'_, P, C> {
        self.core.wait_for_item();
        self.next_msg()
    }

    /// Return the next message without blocking, or `None` if the ring is
    /// empty.
    #[inline]
    pub fn try_pop(&mut self) -> Option<Msg<'_, P, C>> {
        if !self.core.has_item() {
            return None;
        }
        Some(self.next_msg())
    }

    /// Consume every message currently in the ring, calling `f` on each, and
    /// return how many were consumed. The read cursor is published **once**,
    /// after the last message — one `Release` store (and one wake-up) for the
    /// whole batch, the cheapest way to drain a busy ring. The flip side is
    /// that the producer sees no space freed until the batch completes, so
    /// keep `f` short or prefer [`pop`](Self::pop) when the producer is
    /// starved for space.
    ///
    /// Delivery is at-most-once: the cursor advances over each record before
    /// `f` sees it, and progress is published even if `f` panics, so an
    /// unwound drain never re-delivers already-processed messages.
    pub fn drain<F: FnMut(&[u8])>(&mut self, mut f: F) -> usize {
        // Unconditionally refresh the view of the producer's cursor: the
        // contract is "everything currently in the ring", which a stale
        // non-empty cache must not bound.
        let end = self.core.refresh();
        if end.wrapping_sub(self.core.read_cursor) == 0 {
            return 0;
        }

        // Publish on exit — including an unwind out of `f`.
        struct FlushOnDrop<'a, P: WaitStrategy, C: WaitStrategy>(&'a mut BytesConsumer<P, C>);
        impl<P: WaitStrategy, C: WaitStrategy> Drop for FlushOnDrop<'_, P, C> {
            fn drop(&mut self) {
                self.0.core.flush_pending();
            }
        }

        let guard = FlushOnDrop(self);
        let base = guard.0.base();
        let mask = guard.0.core.mask;
        let mut count = 0;

        while end.wrapping_sub(guard.0.core.read_cursor) != 0 {
            // SAFETY: records below `end` are fully published.
            let (cur, len, payload) =
                unsafe { decode_record(base, mask, guard.0.core.read_cursor) };
            // Advance before the callback: the record counts as consumed even
            // if `f` unwinds. The payload slice stays valid — the producer
            // cannot reuse it until the guard publishes, strictly after `f`.
            guard.0.core.read_cursor = cur.wrapping_add(record_len(len));
            // SAFETY: payload is contiguous, in bounds, and fully published.
            f(unsafe { std::slice::from_raw_parts(payload, len) });
            count += 1;
        }
        count
    }

    /// Base of the byte buffer.
    #[inline(always)]
    fn base(&self) -> *mut u8 {
        self.core.buf.as_ptr().cast::<u8>()
    }

    /// Common tail of `pop`/`try_pop`: availability is already confirmed;
    /// decode the record at the read cursor.
    #[inline(always)]
    fn next_msg(&mut self) -> Msg<'_, P, C> {
        // SAFETY: availability was confirmed by the caller.
        let (cur, len, payload) =
            unsafe { decode_record(self.base(), self.core.mask, self.core.read_cursor) };
        self.core.read_cursor = cur;
        // SAFETY: `payload` is derived from the non-null buffer base.
        let payload = unsafe { NonNull::new_unchecked(payload.cast_mut()) };
        Msg {
            payload,
            len,
            consumer: self,
        }
    }

    /// Whether the ring currently holds no messages. Exact on this side: uses
    /// the consumer's private cursor, which is always current.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.core.available() == 0
    }

    /// The ring's capacity in bytes (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// The largest payload a single message may carry: `capacity / 2 - 4`.
    #[inline]
    pub fn max_message_len(&self) -> usize {
        max_message_len(self.core.capacity())
    }
}

/// A zero-copy view of one received message.
///
/// Dereferences to the payload bytes, which live in the ring itself. The
/// message is released — its bytes handed back to the producer and the read
/// cursor published — when this drops. Copy out anything you need to keep.
///
/// Forgetting the guard (`mem::forget`) does **not** consume the message:
/// the cursor never advances, so the *same message is delivered again* by
/// the next pop or drain. Safe, but re-processing side effects is on the
/// caller.
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
        // which are contiguous, in bounds, and fully published.
        unsafe { std::slice::from_raw_parts(self.payload.as_ptr(), self.len) }
    }
}

impl<P: WaitStrategy, C: WaitStrategy> Drop for Msg<'_, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Release the record with an adaptive publish (see the cursor
        // engine): immediate when caught up or the ring was observed full,
        // one publish per `publish_batch_bytes` while backed up — a full-ring
        // producer's polling cannot force a per-message cache-line ping-pong,
        // and the clamp bounds how much freed space a blocked producer can
        // transiently not see.
        let c = &mut self.consumer.core;
        let capacity = c.capacity();
        // Watermark = capacity / 2: a byte producer blocks whenever
        // contiguous space runs short (free < pad + record), which can happen
        // well below exactly full — and the frame decoder consumes wrap
        // padding into the cursor before this advance, further skewing plain
        // occupancy. Records are capped at capacity / 2, so any blocked
        // producer implies occupancy above this watermark, guaranteeing the
        // immediate release the engine's liveness rule promises.
        c.advance(
            record_len(self.len),
            publish_batch(capacity, MAX_PUBLISH_BATCH_BYTES),
            capacity / 2,
        );
    }
}
