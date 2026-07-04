//! The shared SPSC cursor/publish engine.
//!
//! Everything concurrency-critical that the fixed-size ring
//! ([`crate::spsc`]) and the variable-size byte ring ([`crate::spsc_bytes`])
//! have in common lives here, in exactly one copy: the shared state layout
//! (cache-padded cursor atomics + wait strategies), the handle-cached pointer
//! set, the reload-once occupancy checks and wait loops, and the adaptive
//! read-cursor publish. The rings layer their element/frame semantics on
//! top.
//!
//! Cursors are monotonic and wrap `usize`; every occupancy check therefore
//! compares the wrapped *difference* `write.wrapping_sub(read)` (the true
//! occupancy in cursor units), never absolute values — on 32-bit targets the
//! cursors wrap after only 2^32 units.
//!
//! The unit of a cursor tick is the ring's choice: the fixed-size ring counts
//! elements, the byte ring counts bytes. The core never touches the buffer;
//! `mask` is `capacity - 1` in the ring's units and is used only for
//! occupancy arithmetic (and teardown cleanup, where applicable).

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::wait::WaitStrategy;

/// Per-slot teardown behavior for [`Shared`]'s buffer.
///
/// The fixed-size ring must drop the initialized `T`s left in
/// `[read, write)` when the ring is torn down; the byte ring's buffer is
/// plain words with nothing to drop (and its cursors are byte-granular, so a
/// slot walk would be meaningless — `NEEDS_CLEANUP = false` skips it).
pub(crate) trait SlotCleanup {
    const NEEDS_CLEANUP: bool;

    /// Drop the slot's live contents.
    ///
    /// # Safety
    ///
    /// Only called from `Shared::drop` for slots in `[read, write)`, which
    /// hold initialized values by the ring invariant.
    unsafe fn cleanup(&self);
}

impl<T> SlotCleanup for UnsafeCell<MaybeUninit<T>> {
    const NEEDS_CLEANUP: bool = true;

    #[inline]
    unsafe fn cleanup(&self) {
        // SAFETY: per the trait contract, the slot holds an initialized `T`.
        unsafe { (*self.get()).assume_init_drop() };
    }
}

/// The deferred-publish bound for the adaptive publish: `capacity / 8`
/// cursor units, clamped to `[1, max_batch]`. Each ring picks its own
/// `max_batch` (64 elements for the fixed ring, 4096 bytes for the byte
/// ring) — one policy, per-ring constants.
#[inline(always)]
pub(crate) const fn publish_batch(capacity: usize, max_batch: usize) -> usize {
    let batch = capacity / 8;
    if batch == 0 {
        1
    } else if batch > max_batch {
        max_batch
    } else {
        batch
    }
}

/// The wrap-safe fullness predicate: would writing `needed` more units past
/// `write` overrun a `capacity`-unit ring whose consumer has read up to
/// `read`? The single source of truth for "full" (used by the space check
/// and the producer wait predicate).
#[inline(always)]
const fn lacks_space(write: usize, needed: usize, read: usize, capacity: usize) -> bool {
    write.wrapping_add(needed).wrapping_sub(read) > capacity
}

/// The wrap-safe emptiness predicate (used by the item check and the
/// consumer wait predicate).
#[inline(always)]
const fn no_item(write: usize, read: usize) -> bool {
    write.wrapping_sub(read) == 0
}

/// The state both handles share, kept alive by an `Arc`.
pub(crate) struct Shared<B: SlotCleanup, P, C> {
    pub(crate) buffer: Box<[B]>,
    pub(crate) mask: usize,

    /// Published by the producer (Release), read by the consumer (Acquire).
    write_cursor: CachePadded<AtomicUsize>,
    /// Published by the consumer (Release), read by the producer (Acquire).
    read_cursor: CachePadded<AtomicUsize>,

    pub(crate) producer_wait: P,
    pub(crate) consumer_wait: C,
}

// SAFETY: The buffer slots are only ever written by the single producer and
// read by the single consumer, with access ordered by the atomic cursors.
// Sharing/sending `Shared` between threads is sound as long as the slot type
// is `Send` (for the fixed ring that means `T: Send`).
unsafe impl<B: SlotCleanup + Send, P: Send + Sync, C: Send + Sync> Send for Shared<B, P, C> {}
unsafe impl<B: SlotCleanup + Send, P: Send + Sync, C: Send + Sync> Sync for Shared<B, P, C> {}

impl<B: SlotCleanup, P, C> Drop for Shared<B, P, C> {
    fn drop(&mut self) {
        if B::NEEDS_CLEANUP {
            // No concurrent access at drop time, so relaxed loads suffice.
            // Drop the values still queued: cursor units are slots here
            // (`NEEDS_CLEANUP` is only set by the fixed-size ring, whose
            // buffer length is `mask + 1`).
            let mut head = self.read_cursor.load(Ordering::Relaxed);
            let tail = self.write_cursor.load(Ordering::Relaxed);
            while head != tail {
                // SAFETY: every index in `[read, write)` was produced and not
                // consumed, so the slot holds an initialized value.
                unsafe { self.buffer[head & self.mask].cleanup() };
                head = head.wrapping_add(1);
            }
        }
    }
}

/// Create the shared state and the two handle cores.
///
/// `capacity` is the ring capacity in cursor units (a power of two;
/// `mask = capacity - 1`), `slots` the buffer length in `B` items (equal to
/// `capacity` for the fixed ring, `capacity / 8` for the byte ring's `u64`
/// words).
pub(crate) fn channel<B, P, C>(
    capacity: usize,
    slots: usize,
    make_slot: impl FnMut() -> B,
) -> (ProducerCore<B, P, C>, ConsumerCore<B, P, C>)
where
    B: SlotCleanup,
    P: WaitStrategy,
    C: WaitStrategy,
{
    debug_assert!(capacity.is_power_of_two());

    let mut buffer = Vec::with_capacity(slots);
    buffer.resize_with(slots, make_slot);

    let inner = Arc::new(Shared {
        buffer: buffer.into_boxed_slice(),
        mask: capacity - 1,
        write_cursor: CachePadded::new(AtomicUsize::new(0)),
        read_cursor: CachePadded::new(AtomicUsize::new(0)),
        producer_wait: P::default(),
        consumer_wait: C::default(),
    });

    // Cache the constants each side needs on its hot path. These `NonNull`s
    // stay valid for as long as the `Arc<Shared>` the core holds is alive.
    // The buffer pointer is derived from the whole-slice `as_ptr` (not a
    // first-element reference) so it keeps provenance over every slot.
    let buf = NonNull::new(inner.buffer.as_ptr().cast_mut()).expect("buffer is non-null");
    let mask = inner.mask;
    let next_free = NonNull::from(&*inner.write_cursor);
    let reader = NonNull::from(&*inner.read_cursor);

    (
        ProducerCore {
            buf,
            mask,
            next_free,
            reader,
            write_cursor: 0,
            read_cursor_cache: 0,
            inner: inner.clone(),
        },
        ConsumerCore {
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

/// The producer half of the engine: private write cursor, cached view of the
/// consumer's cursor, and the publish machinery.
pub(crate) struct ProducerCore<B: SlotCleanup, P: WaitStrategy, C: WaitStrategy> {
    /// Base of the slot buffer (cached from `inner`; stable for its lifetime).
    pub(crate) buf: NonNull<B>,
    /// `capacity - 1` in cursor units (cached from `inner`).
    pub(crate) mask: usize,
    /// Our published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// The consumer's published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next cursor unit to write (the C++ `next_free_index_2_`). Private to
    /// this thread.
    pub(crate) write_cursor: usize,
    /// Cached snapshot of the consumer's cursor (the C++ `reader_index_cache_`).
    read_cursor_cache: usize,
    /// Keeps the shared allocation alive and carries the wait strategies.
    pub(crate) inner: Arc<Shared<B, P, C>>,
}

// SAFETY: the producer core only touches producer-private state plus atomics.
// The cached `NonNull`s reference the `Arc<Shared>` it keeps alive.
unsafe impl<B: SlotCleanup + Send, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync>
    Send for ProducerCore<B, P, C>
{
}

impl<B: SlotCleanup, P, C> ProducerCore<B, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Check for `needed` free cursor units, reloading the consumer's cursor
    /// at most once (the index-caching idea: in steady state this never
    /// touches the consumer's cache line).
    #[inline(always)]
    pub(crate) fn has_space(&mut self, needed: usize) -> bool {
        let capacity = self.mask + 1;
        if lacks_space(self.write_cursor, needed, self.read_cursor_cache, capacity) {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            self.read_cursor_cache = unsafe { (*self.reader.as_ptr()).load(Ordering::Acquire) };
            if lacks_space(self.write_cursor, needed, self.read_cursor_cache, capacity) {
                return false;
            }
        }
        true
    }

    /// Spin/park (per the producer wait strategy) until `needed` cursor units
    /// are free.
    #[inline(always)]
    pub(crate) fn wait_for_space(&mut self, needed: usize) {
        while !self.has_space(needed) {
            let write = self.write_cursor;
            let capacity = self.mask + 1;
            let reader = self.reader.as_ptr();
            self.inner.producer_wait.wait(|| {
                !lacks_space(
                    write,
                    needed,
                    unsafe { (*reader).load(Ordering::Acquire) },
                    capacity,
                )
            });
        }
    }

    /// Advance the write cursor over `amount` just-written units and publish
    /// them with one `Release` store, then wake a blocked consumer (a no-op
    /// for the spin strategies, which the compiler elides entirely).
    #[inline(always)]
    pub(crate) fn publish(&mut self, amount: usize) {
        self.write_cursor = self.write_cursor.wrapping_add(amount);
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).store(self.write_cursor, Ordering::Release) };
        self.inner.consumer_wait.notify();
    }

    /// The ring's capacity in cursor units.
    #[inline(always)]
    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Pointer to the slot the write cursor designates. `cursor & mask` is
    /// always in `0..capacity`, so the pointer is in bounds without a bounds
    /// check; the caller must have confirmed the slot is free before writing.
    #[inline(always)]
    pub(crate) fn slot_ptr(&self) -> *mut B {
        // SAFETY: in bounds by masking; `buf` is a live allocation.
        unsafe { self.buf.as_ptr().add(self.write_cursor & self.mask) }
    }
}

/// The consumer half of the engine: private read cursor, cached view of the
/// producer's cursor, and the adaptive publish machinery.
pub(crate) struct ConsumerCore<B: SlotCleanup, P: WaitStrategy, C: WaitStrategy> {
    /// Base of the slot buffer (cached from `inner`; stable for its lifetime).
    pub(crate) buf: NonNull<B>,
    /// `capacity - 1` in cursor units (cached from `inner`).
    pub(crate) mask: usize,
    /// The producer's published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// Our published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next cursor unit to read (the C++ `reader_index_2_`). Private to this
    /// thread.
    pub(crate) read_cursor: usize,
    /// The value of `read_cursor` last published to the shared atomic (see
    /// [`advance`](Self::advance) for the adaptive publish rule).
    pub(crate) published: usize,
    /// Cached snapshot of the producer's cursor (the C++
    /// `next_free_index_cache_`).
    pub(crate) write_cursor_cache: usize,
    /// Keeps the shared allocation alive and carries the wait strategies.
    pub(crate) inner: Arc<Shared<B, P, C>>,
}

// SAFETY: the consumer core only touches consumer-private state plus atomics.
unsafe impl<B: SlotCleanup + Send, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync>
    Send for ConsumerCore<B, P, C>
{
}

impl<B: SlotCleanup, P: WaitStrategy, C: WaitStrategy> Drop for ConsumerCore<B, P, C> {
    fn drop(&mut self) {
        // Publish any deferred progress and wake a blocked producer. Without
        // the publish, `Shared::drop` would re-drop values we already moved
        // out, and a surviving producer would never see the space we freed;
        // without the notify, a producer parked in a genuinely blocking
        // custom `WaitStrategy` would never wake.
        self.flush_pending();
    }
}

impl<B: SlotCleanup, P, C> ConsumerCore<B, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Check for at least one available cursor unit, reloading the producer's
    /// cursor at most once.
    #[inline(always)]
    pub(crate) fn has_item(&mut self) -> bool {
        if no_item(self.write_cursor_cache, self.read_cursor) {
            self.refresh();
            if no_item(self.write_cursor_cache, self.read_cursor) {
                return false;
            }
        }
        true
    }

    /// Unconditionally reload the cached view of the producer's cursor
    /// (`Acquire`) and return it. `drain`-style batch consumers need this:
    /// `has_item` alone skips the reload while the stale cache still shows
    /// items, which must not bound a "consume everything currently here"
    /// operation.
    #[inline(always)]
    pub(crate) fn refresh(&mut self) -> usize {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
        self.write_cursor_cache
    }

    /// Spin/park (per the consumer wait strategy) until data arrives.
    #[inline(always)]
    pub(crate) fn wait_for_item(&mut self) {
        while !self.has_item() {
            let read_cursor = self.read_cursor;
            let next_free = self.next_free.as_ptr();
            self.inner
                .consumer_wait
                .wait(|| !no_item(unsafe { (*next_free).load(Ordering::Acquire) }, read_cursor));
        }
    }

    /// Advance the read cursor over `amount` just-consumed units and publish
    /// progress adaptively.
    ///
    /// A publish only costs something when the *other* side is polling the
    /// published line, and the producer only polls the read cursor when the
    /// ring is full. So the publish rule adapts to the regime the consumer
    /// can already see for free:
    ///
    /// * **Observed over the full watermark** (occupancy per the latest view
    ///   exceeds `full_watermark`): publish immediately so a producer blocked
    ///   for space is released by the first consume that observes the
    ///   pressure. The fixed ring passes `mask` (fires only at exactly full,
    ///   once per cursor-reload cycle — no ping-pong). The byte ring passes
    ///   `capacity / 2`: its producer blocks whenever contiguous space runs
    ///   out (`free < pad + record`, which can happen well below exactly
    ///   full, and wrap padding consumed by the frame decoder further skews
    ///   plain occupancy), and since records are capped at `capacity / 2`,
    ///   any blocked byte producer implies occupancy above that watermark —
    ///   its publishes above the watermark are per-message, which the
    ///   bandwidth-bound byte ring absorbs without measurable cost.
    /// * **Caught up** (the ring looks empty after this consume): publish
    ///   immediately — the line is uncontended, and this is the
    ///   latency-sensitive regime. Identical to the per-element publish of
    ///   the C++ original.
    /// * **Behind** (the backpressure regime, where a full-ring producer
    ///   polls this line): defer, and publish once per `batch` units.
    ///   Per-element publishes here let the producer's polling steal the
    ///   cache line between every store, collapsing both threads into a
    ///   lockstep line ping-pong (~3.5x lower throughput end to end).
    ///
    /// Because catching up always flushes, the consumer can never wait (or
    /// report empty) with progress unpublished. The residual window: if the
    /// consumer's cached view predates the ring becoming full and the
    /// consumer then stops consuming mid-batch, a blocked producer waits
    /// until the consumer's next flush (at most `batch` units of consumption
    /// away, or the consumer catching up or dropping).
    #[inline(always)]
    pub(crate) fn advance(&mut self, amount: usize, batch: usize, full_watermark: usize) {
        let over_watermark =
            self.write_cursor_cache.wrapping_sub(self.read_cursor) > full_watermark;
        self.read_cursor = self.read_cursor.wrapping_add(amount);
        if over_watermark
            || self.read_cursor == self.write_cursor_cache
            || self.read_cursor.wrapping_sub(self.published) >= batch
        {
            self.flush();
        }
    }

    /// Publish the private read cursor to the shared atomic and wake a
    /// producer blocked on a full ring. The wake-up is a no-op for spin
    /// strategies.
    #[inline(always)]
    pub(crate) fn flush(&mut self) {
        // SAFETY: `reader` is a `NonNull` into the live `inner`.
        unsafe { (*self.reader.as_ptr()).store(self.read_cursor, Ordering::Release) };
        self.published = self.read_cursor;
        self.inner.producer_wait.notify();
    }

    /// [`flush`](Self::flush) only if there is unpublished progress.
    #[inline(always)]
    pub(crate) fn flush_pending(&mut self) {
        if self.read_cursor != self.published {
            self.flush();
        }
    }

    /// Cursor units currently available to consume. Exact on this side: uses
    /// the consumer's private cursor, which is always current.
    #[inline(always)]
    pub(crate) fn available(&self) -> usize {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) }.wrapping_sub(self.read_cursor)
    }

    /// Occupancy relative to the freshest producer cursor, for the consumer's
    /// exact `is_full` view.
    #[inline(always)]
    pub(crate) fn occupied_relaxed(&self) -> usize {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).load(Ordering::Relaxed) }.wrapping_sub(self.read_cursor)
    }

    /// The ring's capacity in cursor units.
    #[inline(always)]
    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Pointer to the slot the read cursor designates (in bounds by masking;
    /// see `ProducerCore::slot_ptr`).
    #[inline(always)]
    pub(crate) fn slot_ptr(&self) -> *mut B {
        // SAFETY: in bounds by masking; `buf` is a live allocation.
        unsafe { self.buf.as_ptr().add(self.read_cursor & self.mask) }
    }
}

/// Occupancy in cursor units per the two shared atomics (the producer-side
/// view; may transiently over-count while the consumer defers publishes).
#[inline]
pub(crate) fn shared_len<B: SlotCleanup, P, C>(inner: &Shared<B, P, C>) -> usize {
    inner
        .write_cursor
        .load(Ordering::Acquire)
        .wrapping_sub(inner.read_cursor.load(Ordering::Acquire))
}

#[inline]
pub(crate) fn shared_is_empty<B: SlotCleanup, P, C>(inner: &Shared<B, P, C>) -> bool {
    shared_len(inner) == 0
}

#[inline]
pub(crate) fn shared_is_full<B: SlotCleanup, P, C>(inner: &Shared<B, P, C>) -> bool {
    inner
        .write_cursor
        .load(Ordering::Relaxed)
        .wrapping_sub(inner.read_cursor.load(Ordering::Acquire))
        > inner.mask
}
