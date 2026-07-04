//! Single-producer / single-consumer ring buffer for **variable-size**
//! messages.
//!
//! Where [`crate::spsc::Spsc`] moves items of one fixed type `T`, this ring
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
//! The same machinery as [`crate::spsc`], at byte granularity:
//!
//! * monotonic byte cursors masked by `capacity - 1` (capacity is a power of
//!   two), so no modulo and the whole buffer is usable;
//! * each side caches the other side's cursor and only reloads the shared
//!   atomic when the buffer looks full/empty;
//! * the two shared atomics live on their own cache lines; each side's
//!   private cursors live in its handle;
//! * one `Release` store publishes a whole record (padding included), one
//!   `Acquire` load observes it;
//! * adaptive read-cursor publishes, as in [`crate::spsc`]: per-message while
//!   the consumer is caught up, deferred/batched while the ring is backed up
//!   so a full-ring producer's polling cannot force a cache-line ping-pong on
//!   every message;
//! * zero-copy on both sides: [`claim`](BytesProducer::claim) hands the
//!   producer a slice to serialize into directly, [`pop`](BytesConsumer::pop)
//!   hands the consumer a borrowed view of the payload in place, and
//!   [`drain`](BytesConsumer::drain) consumes every available message with a
//!   single cursor publish.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::wait::{WaitStrategy, YieldWait};

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

/// Rejects zero capacities when `new` is monomorphized.
struct AssertCapacity<const N: usize>;

impl<const N: usize> AssertCapacity<N> {
    const NON_ZERO: () = assert!(N > 0, "capacity must be greater than zero");
}

struct Inner<P, C> {
    /// The byte buffer, stored as `u64` words so the base is 8-aligned (a
    /// `Box<[u8]>` allocation only guarantees alignment 1). All access goes
    /// through raw `u8` pointers; the words are never read as `u64`s.
    buffer: Box<[UnsafeCell<MaybeUninit<u64>>]>,
    mask: usize,

    /// Byte cursor published by the producer (Release), read by the consumer
    /// (Acquire). Always advances by whole records.
    write_cursor: CachePadded<AtomicUsize>,
    /// Byte cursor published by the consumer (Release), read by the producer
    /// (Acquire).
    read_cursor: CachePadded<AtomicUsize>,

    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: the buffer bytes are only ever written by the single producer and
// read by the single consumer, ordered by the atomic cursors, exactly as in
// `spsc::Inner`. The payload is plain bytes, so there is no `T: Send` to ask.
unsafe impl<P: Send + Sync, C: Send + Sync> Send for Inner<P, C> {}
unsafe impl<P: Send + Sync, C: Send + Sync> Sync for Inner<P, C> {}

/// Builder/namespace for constructing a variable-size-message SPSC ring.
///
/// `N` is the requested minimum capacity in **bytes**; the real capacity is
/// `N` rounded up to the next power of two (and at least 8). `P` and `C` are
/// the push-side and pop-side [`WaitStrategy`]s, as in [`crate::spsc::Spsc`].
pub struct SpscBytes<const N: usize, P = YieldWait, C = YieldWait>(
    core::marker::PhantomData<(P, C)>,
);

impl<const N: usize, P, C> SpscBytes<N, P, C>
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    /// Create the ring and return its producer and consumer halves.
    ///
    /// `N == 0` is rejected at compile time.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new() -> (BytesProducer<P, C>, BytesConsumer<P, C>) {
        let () = AssertCapacity::<N>::NON_ZERO;
        let capacity = N.next_power_of_two().max(8);

        let mut words = Vec::with_capacity(capacity / 8);
        words.resize_with(capacity / 8, || UnsafeCell::new(MaybeUninit::uninit()));

        let inner = Arc::new(Inner {
            buffer: words.into_boxed_slice(),
            mask: capacity - 1,
            write_cursor: CachePadded::new(AtomicUsize::new(0)),
            read_cursor: CachePadded::new(AtomicUsize::new(0)),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        // Cache the hot-path constants in each handle, as `spsc` does.
        let buf = unsafe { NonNull::new_unchecked(inner.buffer.as_ptr().cast_mut().cast::<u8>()) };
        let mask = inner.mask;
        let next_free = unsafe {
            NonNull::new_unchecked((&*inner.write_cursor as *const AtomicUsize).cast_mut())
        };
        let reader = unsafe {
            NonNull::new_unchecked((&*inner.read_cursor as *const AtomicUsize).cast_mut())
        };

        (
            BytesProducer {
                buf,
                mask,
                next_free,
                reader,
                write_cursor: 0,
                read_cursor_cache: 0,
                inner: inner.clone(),
            },
            BytesConsumer {
                buf,
                mask,
                next_free,
                reader,
                read_cursor: 0,
                published: 0,
                write_cursor_cache: 0,
                inner,
            },
        )
    }
}

/// The producing half of an [`SpscBytes`]. Owns the private write cursor.
pub struct BytesProducer<P, C> {
    buf: NonNull<u8>,
    mask: usize,
    /// Our published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// The consumer's published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next byte to write. Private to this thread.
    write_cursor: usize,
    /// Cached snapshot of the consumer's `read_cursor`.
    read_cursor_cache: usize,
    inner: Arc<Inner<P, C>>,
}

// SAFETY: as for `spsc::Producer` — only producer-private state plus atomics.
unsafe impl<P: Send + Sync, C: Send + Sync> Send for BytesProducer<P, C> {}

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
        self.wait_for_space(total);
        // SAFETY: `frame` sized the record and `wait_for_space` confirmed
        // `total` free bytes.
        unsafe { self.write_frame(pad, msg.len(), Some(msg.as_ptr())) };
    }

    /// Enqueue a copy of `msg` without blocking. Returns `false` if there is
    /// not enough free space.
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
    /// [committed](WriteSlot::commit); dropping the slot uncommitted abandons
    /// the space for reuse by the next claim.
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

    /// Non-blocking [`claim`](Self::claim). Returns `None` if there is not
    /// enough free space.
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

    /// Where the payload of a record claimed with `pad` bytes of wrap padding
    /// will start.
    #[inline(always)]
    fn payload_ptr(&self, pad: usize) -> NonNull<u8> {
        let pos = self.write_cursor.wrapping_add(pad) & self.mask;
        // SAFETY: in bounds — `frame` reserved `HEADER + len` contiguous
        // bytes starting at `pos`, and `buf` is non-null.
        unsafe { NonNull::new_unchecked(self.buf.as_ptr().add(pos + HEADER)) }
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
        let record = align_up(HEADER + len);
        let to_end = capacity - (self.write_cursor & self.mask);
        if record <= to_end {
            (0, record)
        } else {
            (to_end, to_end + record)
        }
    }

    /// Check for `total` free bytes, reloading the consumer's cursor at most
    /// once — the same reload-once shape as `spsc::Producer::try_push`.
    #[inline]
    fn has_space(&mut self, total: usize) -> bool {
        let capacity = self.mask + 1;
        if self.write_cursor.wrapping_add(total) > self.read_cursor_cache.wrapping_add(capacity) {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            self.read_cursor_cache = unsafe { (*self.reader.as_ptr()).load(Ordering::Acquire) };
            if self.write_cursor.wrapping_add(total) > self.read_cursor_cache.wrapping_add(capacity)
            {
                return false;
            }
        }
        true
    }

    /// Spin/park until `total` free bytes are available.
    #[inline]
    fn wait_for_space(&mut self, total: usize) {
        while !self.has_space(total) {
            let target = self.write_cursor.wrapping_add(total);
            let capacity = self.mask + 1;
            let reader = self.reader.as_ptr();
            self.inner.producer_wait.wait(|| {
                target <= unsafe { (*reader).load(Ordering::Acquire) }.wrapping_add(capacity)
            });
        }
    }

    /// Write padding (if any) and the record header, copy the payload when
    /// `src` is given (a `claim` has already filled it in place otherwise),
    /// then publish everything with one `Release` store.
    ///
    /// # Safety
    ///
    /// The caller must have confirmed `pad + align_up(HEADER + len)` free
    /// bytes, with `(pad, _)` computed by `frame(len)` at the current
    /// `write_cursor`. `src`, when given, must point to `len` readable bytes.
    #[inline(always)]
    unsafe fn write_frame(&mut self, pad: usize, len: usize, src: Option<*const u8>) {
        let base = self.buf.as_ptr();
        let mask = self.mask;
        let mut cur = self.write_cursor;

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
            cur = cur.wrapping_add(align_up(HEADER + len));
            self.write_cursor = cur;
            (*self.next_free.as_ptr()).store(cur, Ordering::Release);
        }

        // Wake a consumer blocked in `pop`. A no-op for the spin strategies.
        self.inner.consumer_wait.notify();
    }

    /// Whether the ring currently holds no messages.
    #[inline]
    pub fn is_empty(&self) -> bool {
        is_empty(&self.inner)
    }

    /// The ring's capacity in bytes (`N` rounded up to a power of two).
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
/// without committing publishes nothing.
///
/// The slice's initial contents are unspecified (whatever the ring last held
/// there) — write your message before reading anything back.
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
        // SAFETY: `payload` points at the `len` reserved bytes; the producer
        // exclusively owns this unpublished region.
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

/// The consuming half of an [`SpscBytes`]. Owns the private read cursor.
pub struct BytesConsumer<P, C> {
    buf: NonNull<u8>,
    mask: usize,
    /// The producer's published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// Our published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next byte to read. Private to this thread.
    read_cursor: usize,
    /// The value of `read_cursor` last published to the shared atomic. As in
    /// `spsc::Consumer`, publishes are per-message while the consumer is
    /// caught up and deferred/batched while the ring is backed up, so a
    /// full-ring producer's polling cannot steal the cursor's cache line
    /// between every message.
    published: usize,
    /// Cached snapshot of the producer's `write_cursor`.
    write_cursor_cache: usize,
    inner: Arc<Inner<P, C>>,
}

impl<P, C> Drop for BytesConsumer<P, C> {
    fn drop(&mut self) {
        // Publish any deferred progress so a surviving producer sees the
        // freed bytes. (No `notify`: it would need a `WaitStrategy` bound,
        // and a `CvWait` producer re-checks every 100 ns regardless.)
        if self.read_cursor != self.published {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            unsafe { (*self.reader.as_ptr()).store(self.read_cursor, Ordering::Release) };
        }
    }
}

// SAFETY: as for `spsc::Consumer`.
unsafe impl<P: Send + Sync, C: Send + Sync> Send for BytesConsumer<P, C> {}

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
        while self.read_cursor >= self.write_cursor_cache {
            // SAFETY: `next_free` is a `NonNull` into the live `inner`.
            self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
            if self.read_cursor >= self.write_cursor_cache {
                let read_cursor = self.read_cursor;
                let next_free = self.next_free.as_ptr();
                self.inner
                    .consumer_wait
                    .wait(|| read_cursor < unsafe { (*next_free).load(Ordering::Acquire) });
            }
        }

        self.next_msg()
    }

    /// Return the next message without blocking, or `None` if the ring is
    /// empty.
    #[inline]
    pub fn try_pop(&mut self) -> Option<Msg<'_, P, C>> {
        if self.read_cursor >= self.write_cursor_cache {
            // SAFETY: `next_free` is a `NonNull` into the live `inner`.
            self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
            if self.read_cursor >= self.write_cursor_cache {
                return None;
            }
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
    pub fn drain<F: FnMut(&[u8])>(&mut self, mut f: F) -> usize {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
        let end = self.write_cursor_cache;
        let start = self.read_cursor;

        let base = self.buf.as_ptr();
        let mask = self.mask;
        let mut cur = start;
        let mut count = 0;

        while cur != end {
            let pos = cur & mask;
            // SAFETY: `cur < end` means the producer published a whole record
            // here; header reads are 4-aligned and in bounds (see
            // `write_frame`).
            let header = unsafe { base.add(pos).cast::<u32>().read() };
            if header == PADDING {
                cur = cur.wrapping_add((mask + 1) - pos);
                continue;
            }
            let len = header as usize;
            // SAFETY: the record is contiguous: `pos + HEADER + len` is in
            // bounds and the payload was fully written before the publish.
            f(unsafe { std::slice::from_raw_parts(base.add(pos + HEADER), len) });
            cur = cur.wrapping_add(align_up(HEADER + len));
            count += 1;
        }

        if cur != start {
            self.read_cursor = cur;
            self.flush();
        }
        count
    }

    /// Publish the private read cursor to the shared atomic and wake a
    /// producer blocked in `push`. The wake-up is a no-op for spin strategies.
    #[inline(always)]
    fn flush(&mut self) {
        // SAFETY: `reader` is a `NonNull` into the live `inner`.
        unsafe { (*self.reader.as_ptr()).store(self.read_cursor, Ordering::Release) };
        self.published = self.read_cursor;
        self.inner.producer_wait.notify();
    }

    /// Common tail of `pop`/`try_pop`: availability is already confirmed;
    /// skip padding and wrap the record at the read cursor.
    #[inline(always)]
    fn next_msg(&mut self) -> Msg<'_, P, C> {
        let base = self.buf.as_ptr();
        let mask = self.mask;

        let pos = self.read_cursor & mask;
        // SAFETY: a record is published at the read cursor (availability was
        // checked); header reads are 4-aligned and in bounds.
        let mut header = unsafe { base.add(pos).cast::<u32>().read() };
        if header == PADDING {
            // The producer publishes padding together with the record that
            // follows it (one cursor store covers both), so a record is
            // guaranteed to sit at offset zero — no need to re-check
            // availability.
            self.read_cursor = self.read_cursor.wrapping_add((mask + 1) - pos);
            // SAFETY: as above, at offset zero.
            header = unsafe { base.cast::<u32>().read() };
            debug_assert!(header != PADDING, "padding is never followed by padding");
        }

        let len = header as usize;
        let pos = self.read_cursor & mask;
        // SAFETY: the record is contiguous and in bounds (`pos + HEADER + len
        // <= capacity`), and `base` is non-null.
        let payload = unsafe { NonNull::new_unchecked(base.add(pos + HEADER)) };
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
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        self.read_cursor >= unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) }
    }

    /// The ring's capacity in bytes (`N` rounded up to a power of two).
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

/// A zero-copy view of one received message.
///
/// Dereferences to the payload bytes, which live in the ring itself. The
/// message is released — its bytes handed back to the producer and the read
/// cursor published — when this drops. Copy out anything you need to keep.
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
        let c = &mut *self.consumer;
        c.read_cursor = c.read_cursor.wrapping_add(align_up(HEADER + self.len));
        // Adaptive publish, as in `spsc::Consumer::read`: publish immediately
        // when caught up (uncontended, latency-critical, and it guarantees
        // the consumer never waits or reports empty with progress deferred);
        // defer to one publish per `capacity / 8` bytes while the ring is
        // backed up, where a full-ring producer polls this line and
        // per-message publishes would degrade both threads into a lockstep
        // cache-line ping-pong.
        if c.read_cursor == c.write_cursor_cache
            || c.read_cursor.wrapping_sub(c.published) >= (c.mask + 1) / 8
        {
            c.flush();
        }
    }
}

#[inline]
fn is_empty<P, C>(inner: &Inner<P, C>) -> bool {
    inner.read_cursor.load(Ordering::Acquire) >= inner.write_cursor.load(Ordering::Acquire)
}
