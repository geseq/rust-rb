//! Single-producer / single-consumer ring buffer.
//!
//! A port of `spsc.hpp` preserving its performance design. Construct with
//! [`RingBuffer::new`], which hands back a [`Producer`] and a [`Consumer`].
//! Each handle is `Send` but neither is `Clone`: the type system enforces the
//! single-producer / single-consumer contract that the C++ original left to
//! the programmer.
//!
//! Elements move by value through [`Producer::push`] / [`Consumer::pop`], or
//! zero-copy: [`Producer::claim`] reserves a slot to construct the element
//! in place, and [`Consumer::pop_ref`] returns a guard that reads the element
//! in the buffer and releases its slot on drop.
//!
//! # Why it is fast
//!
//! * **Monotonic masked indices.** `write_cursor` and `read_cursor` only
//!   ever increase; the slot is `index & mask` with `mask = capacity - 1`.
//!   No modulo, and no wasted "one empty slot" — the whole power-of-two
//!   capacity is usable.
//! * **Index caching.** The producer keeps a private `read_cursor_cache`; it
//!   only reloads the consumer's atomic when the buffer *looks* full. The
//!   consumer mirrors this with `write_cursor_cache`. In steady state
//!   neither side touches the other's cache line.
//! * **No false sharing.** The shared atomics each sit on their own cache line,
//!   and each handle's private cursor fields live in the handle (owned by one
//!   thread) rather than in shared memory.
//! * **No indirection on the hot path.** Each handle caches the buffer base
//!   pointer, the mask, and raw pointers to the two shared atomics, so `push` /
//!   `pop` never chase through `Arc<Inner>` to re-read constants — mirroring the
//!   C++ where these are fixed offsets from `this`.
//! * **Adaptive read-cursor publishes.** A publish only costs something when
//!   the other side is polling the published line, and the producer only
//!   polls the read cursor when the queue is full. The consumer therefore
//!   publishes per element while it is caught up (uncontended and
//!   latency-critical — identical to the C++ behavior) but defers to batched
//!   publishes while the queue is backed up, where per-element publishes
//!   would let the polling producer steal the cursor's cache line between
//!   every store and collapse both threads into a lockstep line ping-pong.
//!   See [`Consumer`] internals for the full analysis.
//!
//! Capacity is chosen at runtime (rounded up to the next power of two). The
//! mask lives in each handle and stays in a register on the hot path, so a
//! runtime capacity costs nothing over a compile-time one.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::wait::{WaitStrategy, YieldWait};

struct Inner<T, P, C> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,

    /// Published by the producer (Release), read by the consumer (Acquire).
    write_cursor: CachePadded<AtomicUsize>,
    /// Published by the consumer (Release), read by the producer (Acquire).
    read_cursor: CachePadded<AtomicUsize>,

    producer_wait: P,
    consumer_wait: C,
}

// SAFETY: The `UnsafeCell` slots are only ever written by the single producer
// and read by the single consumer, with access ordered by the atomic indices.
// Sending the shared `Inner` between threads is sound as long as `T` is `Send`.
unsafe impl<T: Send, P: Send + Sync, C: Send + Sync> Send for Inner<T, P, C> {}
unsafe impl<T: Send, P: Send + Sync, C: Send + Sync> Sync for Inner<T, P, C> {}

impl<T, P, C> Drop for Inner<T, P, C> {
    fn drop(&mut self) {
        // No concurrent access at drop time, so relaxed loads suffice. Drop the
        // elements still in the queue: indices [read_cursor, write_cursor).
        let mut head = self.read_cursor.load(Ordering::Relaxed);
        let tail = self.write_cursor.load(Ordering::Relaxed);
        while head != tail {
            let slot = &self.buffer[head & self.mask];
            // SAFETY: every index in this range was produced and not consumed,
            // so the slot holds an initialized `T`.
            unsafe { (*slot.get()).assume_init_drop() };
            head = head.wrapping_add(1);
        }
    }
}

/// The deferred-publish bound for the consumer's adaptive publish (see
/// `Consumer::advance`): `capacity / 8`, clamped to `[1, 64]`. Large enough to
/// amortize the release store and keep a full-queue producer off the
/// consumer's cache line, small enough that the producer never sees more than
/// 12.5% of the buffer as phantom occupancy.
#[inline(always)]
const fn publish_batch(capacity: usize) -> usize {
    let batch = capacity / 8;
    if batch == 0 {
        1
    } else if batch > 64 {
        64
    } else {
        batch
    }
}

/// Builder/namespace for constructing an SPSC ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity at runtime (rounded up to
/// the next power of two) and uses [`YieldWait`] on both sides, matching the
/// C++ template defaults. Pick other [`WaitStrategy`]s with
/// [`with_wait_strategies`](Self::with_wait_strategies): `P` is the
/// producer-side (push) strategy, `C` the consumer-side (pop) strategy.
pub struct RingBuffer<T, P = YieldWait, C = YieldWait>(core::marker::PhantomData<(T, P, C)>);

impl<T: Send> RingBuffer<T> {
    /// Create a ring buffer with the default wait strategies and return its
    /// producer and consumer halves.
    ///
    /// The real capacity is `min_capacity` rounded up to the next power of
    /// two.
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
    T: Send,
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    /// Create a ring buffer with explicit wait strategies and return its
    /// producer and consumer halves.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub fn with_wait_strategies(min_capacity: usize) -> (Producer<T, P, C>, Consumer<T, P, C>) {
        assert!(min_capacity > 0, "capacity must be greater than zero");
        let capacity = min_capacity
            .checked_next_power_of_two()
            .expect("capacity too large to round up to a power of two");

        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        let inner = Arc::new(Inner {
            buffer: slots.into_boxed_slice(),
            mask: capacity - 1,
            write_cursor: CachePadded::new(AtomicUsize::new(0)),
            read_cursor: CachePadded::new(AtomicUsize::new(0)),
            producer_wait: P::default(),
            consumer_wait: C::default(),
        });

        // Cache the constants each side needs on its hot path. These `NonNull`s
        // stay valid for as long as the `Arc<Inner>` the handle holds is alive.
        let buf = unsafe { NonNull::new_unchecked(inner.buffer.as_ptr().cast_mut()) };
        let mask = inner.mask;
        let next_free = unsafe {
            NonNull::new_unchecked((&*inner.write_cursor as *const AtomicUsize).cast_mut())
        };
        let reader = unsafe {
            NonNull::new_unchecked((&*inner.read_cursor as *const AtomicUsize).cast_mut())
        };

        (
            Producer {
                buf,
                mask,
                next_free,
                reader,
                write_cursor: 0,
                read_cursor_cache: 0,
                inner: inner.clone(),
            },
            Consumer {
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

/// The producing half of a [`RingBuffer`]. Owns the private write cursor.
pub struct Producer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached from `inner`; stable for its lifetime).
    buf: NonNull<UnsafeCell<MaybeUninit<T>>>,
    /// `capacity - 1` (cached from `inner`).
    mask: usize,
    /// Our published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// The consumer's published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next index to write (the C++ `next_free_index_2_`). Private to this thread.
    write_cursor: usize,
    /// Cached snapshot of the consumer's `read_cursor` (the C++ `reader_index_cache_`).
    read_cursor_cache: usize,
    /// Keeps the shared allocation alive and carries the wait strategies.
    inner: Arc<Inner<T, P, C>>,
}

// SAFETY: the producer half only touches producer-private state plus atomics.
// The cached `NonNull`s reference the `Arc<Inner>` it keeps alive.
unsafe impl<T: Send, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Producer<T, P, C>
{
}

impl<T, P, C> Producer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until there is room, then enqueue `value`.
    #[inline]
    pub fn push(&mut self, value: T) {
        self.wait_for_space();
        self.write(value);
    }

    /// Enqueue `value` without blocking. Returns `Err(value)` if the buffer is
    /// full, handing the item back to the caller.
    ///
    /// "Full" is judged against the consumer's *published* progress; while
    /// the consumer defers publishes in the backed-up regime this can
    /// spuriously fail with up to `capacity / 8` (max 64) slots consumed but
    /// not yet published. A blocking [`push`](Self::push) is woken as soon
    /// as the consumer flushes.
    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if !self.has_space() {
            return Err(value);
        }
        self.write(value);
        Ok(())
    }

    /// Block until there is room, then return the free slot for in-place
    /// construction — the zero-copy alternative to [`push`](Self::push).
    ///
    /// Write through [`WriteSlot::uninit`] and publish with
    /// [`WriteSlot::commit_init`], or move a value in with
    /// [`WriteSlot::commit`]. Dropping the slot uncommitted publishes
    /// nothing; the same slot is handed out again by the next claim or push.
    #[inline]
    pub fn claim(&mut self) -> WriteSlot<'_, T, P, C> {
        self.wait_for_space();
        WriteSlot { producer: self }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if the buffer is
    /// full.
    #[inline]
    pub fn try_claim(&mut self) -> Option<WriteSlot<'_, T, P, C>> {
        if !self.has_space() {
            return None;
        }
        Some(WriteSlot { producer: self })
    }

    /// Check for a free slot, reloading the consumer's cursor at most once.
    ///
    /// Fullness is judged on the wrapped *difference* of the monotonic
    /// cursors (`write - read`, which is always the true occupancy), never on
    /// the absolute values — the cursors wrap `usize`, which on 32-bit
    /// targets happens after mere 2^32 elements.
    #[inline(always)]
    fn has_space(&mut self) -> bool {
        if self.write_cursor.wrapping_sub(self.read_cursor_cache) > self.mask {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            self.read_cursor_cache = unsafe { (*self.reader.as_ptr()).load(Ordering::Acquire) };
            if self.write_cursor.wrapping_sub(self.read_cursor_cache) > self.mask {
                return false;
            }
        }
        true
    }

    /// Spin/park (per the producer wait strategy) until a slot is free.
    #[inline(always)]
    fn wait_for_space(&mut self) {
        while !self.has_space() {
            let next = self.write_cursor;
            let reader = self.reader.as_ptr();
            let mask = self.mask;
            self.inner
                .producer_wait
                .wait(|| next.wrapping_sub(unsafe { (*reader).load(Ordering::Acquire) }) <= mask);
        }
    }

    /// Pointer to the slot the write cursor designates.
    ///
    /// # Safety of the returned pointer
    ///
    /// `index & mask` is always in `0..capacity`, so the pointer is in
    /// bounds; skipping the bounds check keeps the hot path branch-free, as
    /// in the C++ `contents_[i & mask]`. The caller must have confirmed the
    /// slot is free before writing through it.
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: in bounds, see above; `buf` is a live allocation.
        unsafe { (*self.buf.as_ptr().add(self.write_cursor & self.mask)).get() }
    }

    /// Common tail of `push`/`try_push`: store the value and publish it.
    #[inline(always)]
    fn write(&mut self, value: T) {
        // SAFETY: we are the single producer and the caller confirmed the
        // slot is free (the consumer moved its occupant out).
        unsafe { (*self.slot()).write(value) };
        self.publish();
    }

    /// Advance the write cursor over the just-written slot and publish it.
    #[inline(always)]
    fn publish(&mut self) {
        self.write_cursor = self.write_cursor.wrapping_add(1);
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).store(self.write_cursor, Ordering::Release) };

        // Wake a consumer blocked in `pop`. A no-op for the spin strategies,
        // which the compiler elides entirely.
        self.inner.consumer_wait.notify();
    }

    /// Number of elements currently queued.
    ///
    /// While the queue is backed up, the consumer defers its cursor publishes
    /// (see `Consumer::advance`), so this may transiently over-count by up to
    /// `capacity / 8` (max 64) already-consumed elements. It is exact
    /// whenever the consumer has caught up, and never under-counts.
    #[inline]
    pub fn len(&self) -> usize {
        len(&self.inner)
    }

    /// Whether the queue is empty. Exact whenever the consumer has caught up
    /// (see [`len`](Self::len)); never reports `true` for a non-empty queue.
    #[inline]
    pub fn is_empty(&self) -> bool {
        is_empty(&self.inner)
    }

    /// Whether the queue is full (no room for another `push`). May transiently
    /// report `true` while the consumer defers publishes in the backed-up
    /// regime (see [`len`](Self::len)); never reports `false` for a truly
    /// full queue.
    #[inline]
    pub fn is_full(&self) -> bool {
        is_full(&self.inner)
    }

    /// The buffer's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

/// A claimed, not-yet-published slot in the ring — the zero-copy write path.
///
/// Construct the element directly in the buffer via [`uninit`](Self::uninit)
/// and publish with [`commit_init`](Self::commit_init), or move a value in
/// with [`commit`](Self::commit). Dropping the slot uncommitted publishes
/// nothing.
pub struct WriteSlot<'a, T, P: WaitStrategy, C: WaitStrategy> {
    producer: &'a mut Producer<T, P, C>,
}

impl<T, P: WaitStrategy, C: WaitStrategy> WriteSlot<'_, T, P, C> {
    /// The slot's storage, for in-place initialization.
    ///
    /// The contents are unspecified until written (a previous occupant's
    /// remains, moved out by the consumer) — initialize before reading.
    #[inline]
    pub fn uninit(&mut self) -> &mut MaybeUninit<T> {
        // SAFETY: the slot was confirmed free when the claim was created and
        // the producer cursor has not moved since (`self` borrows it
        // exclusively); the single producer may write it.
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

/// The consuming half of a [`RingBuffer`]. Owns the private read cursor.
pub struct Consumer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached from `inner`; stable for its lifetime).
    buf: NonNull<UnsafeCell<MaybeUninit<T>>>,
    /// `capacity - 1` (cached from `inner`).
    mask: usize,
    /// The producer's published cursor (cached `NonNull` into `inner`).
    next_free: NonNull<AtomicUsize>,
    /// Our published cursor (cached `NonNull` into `inner`).
    reader: NonNull<AtomicUsize>,
    /// Next index to read (the C++ `reader_index_2_`). Private to this thread.
    read_cursor: usize,
    /// The value of `read_cursor` last published to the shared atomic (see
    /// [`advance`](Self::advance) for the adaptive publish rule).
    published: usize,
    /// Cached snapshot of the producer's `write_cursor` (the C++ `next_free_index_cache_`).
    write_cursor_cache: usize,
    /// Keeps the shared allocation alive and carries the wait strategies.
    inner: Arc<Inner<T, P, C>>,
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for Consumer<T, P, C> {
    fn drop(&mut self) {
        // Publish any deferred progress and wake a blocked producer. Without
        // the publish, `Inner::drop` would re-drop elements we already moved
        // out, and a surviving producer would never see the space we freed;
        // without the notify, a producer parked in a genuinely blocking
        // custom `WaitStrategy` would never wake.
        if self.read_cursor != self.published {
            self.flush();
        }
    }
}

// SAFETY: the consumer half only touches consumer-private state plus atomics.
unsafe impl<T: Send, P: WaitStrategy + Send + Sync, C: WaitStrategy + Send + Sync> Send
    for Consumer<T, P, C>
{
}

impl<T, P, C> Consumer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    /// Block until an element is available, then dequeue it by value.
    #[inline]
    pub fn pop(&mut self) -> T {
        self.wait_for_item();
        self.read()
    }

    /// Dequeue an element by value without blocking, or return `None` if
    /// empty.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        if !self.has_item() {
            return None;
        }
        Some(self.read())
    }

    /// Block until an element is available, then return a zero-copy view of
    /// it in the buffer. The element is dropped in place and its slot
    /// released when the returned [`PopRef`] drops — nothing is moved or
    /// copied.
    ///
    /// Prefer this when the element is processed where it lies before the
    /// consumer moves on; prefer [`pop`](Self::pop) to drain quickly, since
    /// moving the value out releases the slot immediately.
    #[inline]
    pub fn pop_ref(&mut self) -> PopRef<'_, T, P, C> {
        self.wait_for_item();
        PopRef { consumer: self }
    }

    /// Non-blocking [`pop_ref`](Self::pop_ref). Returns `None` if empty.
    #[inline]
    pub fn try_pop_ref(&mut self) -> Option<PopRef<'_, T, P, C>> {
        if !self.has_item() {
            return None;
        }
        Some(PopRef { consumer: self })
    }

    /// Check for an available element, reloading the producer's cursor at
    /// most once. Emptiness is judged on the wrapped cursor difference (see
    /// `Producer::has_space`).
    #[inline(always)]
    fn has_item(&mut self) -> bool {
        if self.write_cursor_cache.wrapping_sub(self.read_cursor) == 0 {
            // SAFETY: `next_free` is a `NonNull` into the live `inner`.
            self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
            if self.write_cursor_cache.wrapping_sub(self.read_cursor) == 0 {
                return false;
            }
        }
        true
    }

    /// Spin/park (per the consumer wait strategy) until an element arrives.
    #[inline(always)]
    fn wait_for_item(&mut self) {
        while !self.has_item() {
            let read_cursor = self.read_cursor;
            let next_free = self.next_free.as_ptr();
            self.inner.consumer_wait.wait(|| {
                unsafe { (*next_free).load(Ordering::Acquire) }.wrapping_sub(read_cursor) != 0
            });
        }
    }

    /// Pointer to the slot the read cursor designates (in bounds by masking;
    /// see `Producer::slot`).
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: in bounds; `buf` is a live allocation.
        unsafe { (*self.buf.as_ptr().add(self.read_cursor & self.mask)).get() }
    }

    /// Common tail of `pop`/`try_pop`: move the value out and release the
    /// slot.
    #[inline(always)]
    fn read(&mut self) -> T {
        // SAFETY: the index is below the producer's published `write_cursor`,
        // so the slot holds an initialized `T` that we move out exactly once.
        let value = unsafe { (*self.slot()).assume_init_read() };
        self.advance();
        value
    }

    /// Advance the read cursor over the just-released slot and publish
    /// progress adaptively.
    ///
    /// A publish only costs something when the *other* side is polling the
    /// published line, and the producer only polls `read_cursor` when the
    /// queue is full. So the publish rule adapts to the regime the consumer
    /// can already see for free:
    ///
    /// * **Caught up** (`read_cursor == write_cursor_cache`, the queue looks
    ///   empty): publish immediately — the line is uncontended, and this is
    ///   the latency-sensitive regime. Behaves exactly like the per-element
    ///   publish of the C++ original.
    /// * **Behind** (more items already known available — the backpressure
    ///   regime, where a full-queue producer polls this line): defer, and
    ///   publish once per `publish_batch(capacity)` elements. Per-element
    ///   publishes here let the producer's polling steal the cache line
    ///   between every store, collapsing both threads into a lockstep
    ///   line ping-pong (~3.5x lower throughput end to end).
    ///
    /// Because catching up always flushes, the consumer can never wait (or
    /// report empty) with progress unpublished. A pop that *observes a full
    /// queue* (per its latest view of the producer cursor) also flushes
    /// immediately, so a producer blocked on a full queue is released by the
    /// first pop that sees it — this fires once per cursor-reload cycle, not
    /// per element, so it does not reintroduce the ping-pong. The residual
    /// window: if the consumer's cached view predates the queue becoming
    /// full and the consumer then stops popping mid-batch, a blocked
    /// producer waits until the consumer's next flush (at most the batch
    /// slack of pops away, or the consumer catching up or dropping).
    #[inline(always)]
    fn advance(&mut self) {
        // Observed occupancy before this element is released; == capacity
        // means the producer is (or was about to be) blocked on full.
        let was_full = self.write_cursor_cache.wrapping_sub(self.read_cursor) > self.mask;
        self.read_cursor = self.read_cursor.wrapping_add(1);
        if was_full
            || self.read_cursor == self.write_cursor_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch(self.mask + 1)
        {
            self.flush();
        }
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

    /// Number of elements currently queued. Exact on this side: uses the
    /// consumer's private cursor, which is always current.
    #[inline]
    pub fn len(&self) -> usize {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) }.wrapping_sub(self.read_cursor)
    }

    /// Whether the queue is empty. Exact on this side (see [`len`](Self::len)).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the queue is full. Exact on this side (see [`len`](Self::len)).
    #[inline]
    pub fn is_full(&self) -> bool {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        let write = unsafe { (*self.next_free.as_ptr()).load(Ordering::Relaxed) };
        write.wrapping_sub(self.read_cursor) > self.mask
    }

    /// The buffer's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

/// A zero-copy view of the next element, still in the buffer.
///
/// Dereferences to the element (mutably too, e.g. to take parts out of it).
/// When this drops, the element is dropped in place and its slot released to
/// the producer.
///
/// Forgetting the guard (`mem::forget`) does **not** consume the element:
/// the cursor never advances, so the *same element is delivered again* by
/// the next pop. This is safe, but if the element carries side-effectful
/// semantics (a command, an order), re-processing it is on the caller.
pub struct PopRef<'a, T, P: WaitStrategy, C: WaitStrategy> {
    consumer: &'a mut Consumer<T, P, C>,
}

impl<T, P: WaitStrategy, C: WaitStrategy> core::ops::Deref for PopRef<'_, T, P, C> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // SAFETY: the read cursor is below the producer's published
        // `write_cursor`, so the slot holds an initialized `T`; the producer
        // cannot reuse it until the cursor advances (on drop).
        unsafe { (*self.consumer.slot()).assume_init_ref() }
    }
}

impl<T, P: WaitStrategy, C: WaitStrategy> core::ops::DerefMut for PopRef<'_, T, P, C> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as for `deref`; the guard borrows the consumer exclusively.
        unsafe { (*self.consumer.slot()).assume_init_mut() }
    }
}

impl<T, P: WaitStrategy, C: WaitStrategy> Drop for PopRef<'_, T, P, C> {
    #[inline]
    fn drop(&mut self) {
        // Advance via a guard so the cursor moves even if `T::drop` unwinds:
        // once `drop_in_place` begins, the element counts as dropped, and
        // leaving the cursor on it would let the unwind path (or the next
        // pop) drop it a second time. The publish still happens strictly
        // after `drop_in_place` returns or unwinds, so the producer cannot
        // overwrite the slot while the destructor is running.
        struct AdvanceOnDrop<'a, T, P: WaitStrategy, C: WaitStrategy>(&'a mut Consumer<T, P, C>);
        impl<T, P: WaitStrategy, C: WaitStrategy> Drop for AdvanceOnDrop<'_, T, P, C> {
            fn drop(&mut self) {
                self.0.advance();
            }
        }

        let guard = AdvanceOnDrop(&mut *self.consumer);
        let slot = guard.0.slot();
        // SAFETY: the slot holds an initialized `T` that no one else can
        // observe; drop it exactly once. The guard releases the slot after.
        unsafe { std::ptr::drop_in_place((*slot).as_mut_ptr()) };
    }
}

#[inline]
fn len<T, P, C>(inner: &Inner<T, P, C>) -> usize {
    inner
        .write_cursor
        .load(Ordering::Acquire)
        .wrapping_sub(inner.read_cursor.load(Ordering::Acquire))
}

#[inline]
fn is_empty<T, P, C>(inner: &Inner<T, P, C>) -> bool {
    len(inner) == 0
}

#[inline]
fn is_full<T, P, C>(inner: &Inner<T, P, C>) -> bool {
    inner
        .write_cursor
        .load(Ordering::Relaxed)
        .wrapping_sub(inner.read_cursor.load(Ordering::Acquire))
        > inner.mask
}
