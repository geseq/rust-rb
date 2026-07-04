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
//! The concurrency machinery lives in the crate's shared cursor engine (one
//! copy for both this ring and [`crate::spsc_bytes`]):
//!
//! * **Monotonic masked indices.** The cursors only ever increase; the slot
//!   is `index & mask` with `mask = capacity - 1`. No modulo, no wasted "one
//!   empty slot", and all occupancy checks compare wrapped cursor
//!   differences, so wraparound (2^32 elements on 32-bit targets) is sound.
//! * **Index caching.** The producer keeps a private cache of the consumer's
//!   cursor and only reloads the shared atomic when the buffer *looks* full;
//!   the consumer mirrors this. In steady state neither side touches the
//!   other's cache line.
//! * **No false sharing.** The shared atomics each sit on their own cache
//!   line; each side's private cursors live in its handle.
//! * **No indirection on the hot path.** Each handle caches the buffer base
//!   pointer, the mask, and raw pointers to the two shared atomics.
//! * **Adaptive read-cursor publishes.** Per element while caught up or when
//!   the queue was observed full (latency and producer liveness), batched
//!   while backed up (throughput — a polling producer cannot force a
//!   per-element cache-line ping-pong).
//!
//! Capacity is chosen at runtime (rounded up to the next power of two). The
//! mask lives in each handle and stays in a register on the hot path, so a
//! runtime capacity costs nothing over a compile-time one.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

use crate::cursor::{channel, publish_batch, ConsumerCore, ProducerCore};
use crate::wait::{WaitStrategy, YieldWait};

/// The slot type: a cell the producer writes and the consumer moves out of,
/// ordered by the cursor atomics.
type Slot<T> = UnsafeCell<MaybeUninit<T>>;

/// The fixed ring's clamp for the shared publish-batch policy: at most 64
/// elements of deferred, already-consumed progress.
const MAX_PUBLISH_BATCH: usize = 64;

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

        let (producer, consumer) =
            channel(
                capacity,
                capacity,
                || UnsafeCell::new(MaybeUninit::uninit()),
            );
        (Producer { core: producer }, Consumer { core: consumer })
    }
}

/// The producing half of a [`RingBuffer`]. Owns the private write cursor.
pub struct Producer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    core: ProducerCore<Slot<T>, P, C>,
}

impl<T, P, C> Producer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    #[cfg(all(feature = "shm", target_os = "linux"))]
    pub(crate) fn from_core(core: ProducerCore<Slot<T>, P, C>) -> Self {
        Self { core }
    }

    /// Block until there is room, then enqueue `value`.
    #[inline]
    pub fn push(&mut self, value: T) {
        self.core.wait_for_space(1);
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
        if !self.core.has_space(1) {
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
        self.core.wait_for_space(1);
        WriteSlot { producer: self }
    }

    /// Non-blocking [`claim`](Self::claim). Returns `None` if the buffer is
    /// full.
    #[inline]
    pub fn try_claim(&mut self) -> Option<WriteSlot<'_, T, P, C>> {
        if !self.core.has_space(1) {
            return None;
        }
        Some(WriteSlot { producer: self })
    }

    /// Pointer to the slot the write cursor designates. The caller must have
    /// confirmed the slot is free before writing through it.
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: the core hands back an in-bounds slot pointer; `get` on the
        // `UnsafeCell` is how the single producer accesses its storage.
        unsafe { (*self.core.slot_ptr()).get() }
    }

    /// Common tail of `push`/`try_push`: store the value and publish it.
    #[inline(always)]
    fn write(&mut self, value: T) {
        // SAFETY: we are the single producer and the caller confirmed the
        // slot is free (the consumer moved its occupant out).
        unsafe { (*self.slot()).write(value) };
        self.core.publish(1);
    }

    /// Number of elements currently queued.
    ///
    /// While the queue is backed up, the consumer defers its cursor publishes
    /// (see the crate's adaptive publish rule), so this may transiently
    /// over-count by up to `capacity / 8` (max 64) already-consumed elements.
    /// It is exact whenever the consumer has caught up, and never
    /// under-counts.
    #[inline]
    pub fn len(&self) -> usize {
        self.core.occupancy()
    }

    /// Whether the queue is empty. Exact whenever the consumer has caught up
    /// (see [`len`](Self::len)); never reports `true` for a non-empty queue.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.core.occupancy() == 0
    }

    /// Whether the queue is full (no room for another `push`). May transiently
    /// report `true` while the consumer defers publishes in the backed-up
    /// regime (see [`len`](Self::len)); never reports `false` for a truly
    /// full queue.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.core.is_full_view()
    }

    /// The buffer's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
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
        producer.core.publish(1);
    }
}

/// The consuming half of a [`RingBuffer`]. Owns the private read cursor.
///
/// Dropping the consumer publishes any deferred progress and wakes a blocked
/// producer (handled by the shared cursor engine).
pub struct Consumer<T, P: WaitStrategy = YieldWait, C: WaitStrategy = YieldWait> {
    core: ConsumerCore<Slot<T>, P, C>,
}

impl<T, P, C> Consumer<T, P, C>
where
    P: WaitStrategy,
    C: WaitStrategy,
{
    #[cfg(all(feature = "shm", target_os = "linux"))]
    pub(crate) fn from_core(core: ConsumerCore<Slot<T>, P, C>) -> Self {
        Self { core }
    }

    /// Block until an element is available, then dequeue it by value.
    #[inline]
    pub fn pop(&mut self) -> T {
        self.core.wait_for_item();
        self.read()
    }

    /// Dequeue an element by value without blocking, or return `None` if
    /// empty.
    #[inline]
    pub fn try_pop(&mut self) -> Option<T> {
        if !self.core.has_item() {
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
        self.core.wait_for_item();
        PopRef { consumer: self }
    }

    /// Non-blocking [`pop_ref`](Self::pop_ref). Returns `None` if empty.
    #[inline]
    pub fn try_pop_ref(&mut self) -> Option<PopRef<'_, T, P, C>> {
        if !self.core.has_item() {
            return None;
        }
        Some(PopRef { consumer: self })
    }

    /// Pointer to the slot the read cursor designates.
    #[inline(always)]
    fn slot(&self) -> *mut MaybeUninit<T> {
        // SAFETY: the core hands back an in-bounds slot pointer.
        unsafe { (*self.core.slot_ptr()).get() }
    }

    /// Common tail of `pop`/`try_pop`: move the value out and release the
    /// slot with an adaptive publish.
    #[inline(always)]
    fn read(&mut self) -> T {
        // SAFETY: the index is below the producer's published cursor, so the
        // slot holds an initialized `T` that we move out exactly once.
        let value = unsafe { (*self.slot()).assume_init_read() };
        self.advance_one();
        value
    }

    /// Release one slot (see the cursor engine for the adaptive publish
    /// rule: immediate when caught up or the queue was observed full,
    /// batched while backed up).
    #[inline(always)]
    fn advance_one(&mut self) {
        let capacity = self.core.capacity();
        // Watermark = mask: the immediate flush fires only when the queue was
        // observed exactly full (a blocked element producer needs one slot).
        self.core
            .advance(1, publish_batch(capacity, MAX_PUBLISH_BATCH), capacity - 1);
    }

    /// Number of elements currently queued. Exact on this side: uses the
    /// consumer's private cursor, which is always current.
    #[inline]
    pub fn len(&self) -> usize {
        self.core.available()
    }

    /// Whether the queue is empty. Exact on this side (see [`len`](Self::len)).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.core.available() == 0
    }

    /// Whether the queue is full. Exact on this side (see [`len`](Self::len)).
    #[inline]
    pub fn is_full(&self) -> bool {
        self.core.occupied_relaxed() > self.core.mask
    }

    /// The buffer's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
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
        // SAFETY: the read cursor is below the producer's published cursor,
        // so the slot holds an initialized `T`; the producer cannot reuse it
        // until the cursor advances (on drop).
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
                self.0.advance_one();
            }
        }

        let guard = AdvanceOnDrop(&mut *self.consumer);
        let slot = guard.0.slot();
        // SAFETY: the slot holds an initialized `T` that no one else can
        // observe; drop it exactly once. The guard releases the slot after.
        unsafe { std::ptr::drop_in_place((*slot).as_mut_ptr()) };
    }
}
