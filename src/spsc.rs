//! Single-producer / single-consumer ring buffer.
//!
//! A faithful port of `spsc.hpp`. Construct with [`Spsc::new`], which hands back
//! a [`Producer`] and a [`Consumer`]. Each handle is `Send` but neither is
//! `Clone`: the type system enforces the single-producer / single-consumer
//! contract that the C++ original left to the programmer.
//!
//! # Why it is fast
//!
//! * **Monotonic masked indices.** `next_free_index` and `reader_index` only
//!   ever increase; the slot is `index & mask` with `mask = capacity - 1`.
//!   No modulo, and no wasted "one empty slot" — the whole power-of-two
//!   capacity is usable.
//! * **Index caching.** The producer keeps a private `reader_index_cache`; it
//!   only reloads the consumer's atomic when the buffer *looks* full. The
//!   consumer mirrors this with `next_free_index_cache`. In steady state
//!   neither side touches the other's cache line.
//! * **No false sharing.** The shared atomics each sit on their own cache line,
//!   and each handle's private cursor fields live in the handle (owned by one
//!   thread) rather than in shared memory.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::wait::{WaitStrategy, YieldWait};

struct Inner<T, P, G> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,

    /// Published by the producer (Release), read by the consumer (Acquire).
    next_free_index: CachePadded<AtomicUsize>,
    /// Published by the consumer (Release), read by the producer (Acquire).
    reader_index: CachePadded<AtomicUsize>,

    put_wait: P,
    get_wait: G,
}

// SAFETY: The `UnsafeCell` slots are only ever written by the single producer
// and read by the single consumer, with access ordered by the atomic indices.
// Sending the shared `Inner` between threads is sound as long as `T` is `Send`.
unsafe impl<T: Send, P: Send + Sync, G: Send + Sync> Send for Inner<T, P, G> {}
unsafe impl<T: Send, P: Send + Sync, G: Send + Sync> Sync for Inner<T, P, G> {}

impl<T, P, G> Drop for Inner<T, P, G> {
    fn drop(&mut self) {
        // No concurrent access at drop time, so relaxed loads suffice. Drop the
        // elements still in the queue: indices [reader_index, next_free_index).
        let mut head = self.reader_index.load(Ordering::Relaxed);
        let tail = self.next_free_index.load(Ordering::Relaxed);
        while head != tail {
            let slot = &self.buffer[head & self.mask];
            // SAFETY: every index in this range was produced and not consumed,
            // so the slot holds an initialized `T`.
            unsafe { (*slot.get()).assume_init_drop() };
            head = head.wrapping_add(1);
        }
    }
}

/// Builder/namespace for constructing an SPSC ring buffer.
///
/// `N` is the requested minimum capacity; the real capacity is `N` rounded up
/// to the next power of two. `P` and `G` are the put-side and get-side
/// [`WaitStrategy`]s, defaulting to [`YieldWait`] exactly as the C++ template
/// defaults do.
pub struct Spsc<T, const N: usize, P = YieldWait, G = YieldWait>(core::marker::PhantomData<(T, P, G)>);

impl<T, const N: usize, P, G> Spsc<T, N, P, G>
where
    T: Send,
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    /// Create a ring buffer and return its producer and consumer halves.
    ///
    /// # Panics
    ///
    /// Panics if `N == 0`.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new() -> (Producer<T, P, G>, Consumer<T, P, G>) {
        assert!(N > 0, "capacity must be greater than zero");
        let capacity = N.next_power_of_two();

        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        let inner = Arc::new(Inner {
            buffer: slots.into_boxed_slice(),
            mask: capacity - 1,
            next_free_index: CachePadded::new(AtomicUsize::new(0)),
            reader_index: CachePadded::new(AtomicUsize::new(0)),
            put_wait: P::default(),
            get_wait: G::default(),
        });

        (
            Producer {
                inner: inner.clone(),
                next_free_index: 0,
                reader_index_cache: 0,
            },
            Consumer {
                inner,
                reader_index: 0,
                next_free_index_cache: 0,
            },
        )
    }
}

/// The producing half of an [`Spsc`]. Owns the private write cursor.
pub struct Producer<T, P, G> {
    inner: Arc<Inner<T, P, G>>,
    /// Next index to write (the C++ `next_free_index_2_`). Private to this thread.
    next_free_index: usize,
    /// Cached snapshot of the consumer's `reader_index` (the C++ `reader_index_cache_`).
    reader_index_cache: usize,
}

// SAFETY: the producer half only touches producer-private state plus atomics.
unsafe impl<T: Send, P: Send + Sync, G: Send + Sync> Send for Producer<T, P, G> {}

impl<T, P, G> Producer<T, P, G>
where
    P: WaitStrategy,
    G: WaitStrategy,
{
    /// Block until there is room, then enqueue `value`.
    #[inline]
    pub fn put(&mut self, value: T) {
        let inner = &*self.inner;
        let mask = inner.mask;

        while self.next_free_index > self.reader_index_cache.wrapping_add(mask) {
            self.reader_index_cache = inner.reader_index.load(Ordering::Acquire);
            if self.next_free_index > self.reader_index_cache.wrapping_add(mask) {
                let next = self.next_free_index;
                inner
                    .put_wait
                    .wait(|| next <= inner.reader_index.load(Ordering::Acquire).wrapping_add(mask));
            }
        }

        self.write(value);
    }

    /// Enqueue `value` without blocking. Returns `Err(value)` if the buffer is
    /// full, handing the item back to the caller.
    #[inline]
    pub fn try_put(&mut self, value: T) -> Result<(), T> {
        let inner = &*self.inner;
        let mask = inner.mask;

        if self.next_free_index > self.reader_index_cache.wrapping_add(mask) {
            self.reader_index_cache = inner.reader_index.load(Ordering::Acquire);
            if self.next_free_index > self.reader_index_cache.wrapping_add(mask) {
                return Err(value);
            }
        }

        self.write(value);
        Ok(())
    }

    /// Common tail of `put`/`try_put`: store the value and publish it.
    #[inline(always)]
    fn write(&mut self, value: T) {
        let inner = &*self.inner;
        let slot = &inner.buffer[self.next_free_index & inner.mask];
        // SAFETY: we are the only producer and have confirmed the slot is free
        // (the consumer has moved its previous occupant out).
        unsafe { (*slot.get()).write(value) };

        self.next_free_index = self.next_free_index.wrapping_add(1);
        inner
            .next_free_index
            .store(self.next_free_index, Ordering::Release);

        // Wake a consumer blocked in `get`. A no-op for the spin strategies.
        inner.get_wait.notify();
    }

    /// Number of elements currently queued.
    #[inline]
    pub fn len(&self) -> usize {
        len(&self.inner)
    }

    /// Whether the queue is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        is_empty(&self.inner)
    }

    /// Whether the queue is full (no room for another `put`).
    #[inline]
    pub fn is_full(&self) -> bool {
        is_full(&self.inner)
    }

    /// The buffer's true capacity (`N` rounded up to a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }
}

/// The consuming half of an [`Spsc`]. Owns the private read cursor.
pub struct Consumer<T, P, G> {
    inner: Arc<Inner<T, P, G>>,
    /// Next index to read (the C++ `reader_index_2_`). Private to this thread.
    reader_index: usize,
    /// Cached snapshot of the producer's `next_free_index` (the C++ `next_free_index_cache_`).
    next_free_index_cache: usize,
}

// SAFETY: the consumer half only touches consumer-private state plus atomics.
unsafe impl<T: Send, P: Send + Sync, G: Send + Sync> Send for Consumer<T, P, G> {}

impl<T, P, G> Consumer<T, P, G>
where
    P: WaitStrategy,
    G: WaitStrategy,
{
    /// Block until an element is available, then dequeue it.
    #[inline]
    pub fn get(&mut self) -> T {
        let inner = &*self.inner;

        while self.reader_index >= self.next_free_index_cache {
            self.next_free_index_cache = inner.next_free_index.load(Ordering::Acquire);
            if self.reader_index >= self.next_free_index_cache {
                let reader = self.reader_index;
                inner
                    .get_wait
                    .wait(|| reader < inner.next_free_index.load(Ordering::Acquire));
            }
        }

        self.read()
    }

    /// Dequeue an element without blocking, or return `None` if empty.
    #[inline]
    pub fn try_get(&mut self) -> Option<T> {
        let inner = &*self.inner;

        if self.reader_index >= self.next_free_index_cache {
            self.next_free_index_cache = inner.next_free_index.load(Ordering::Acquire);
            if self.reader_index >= self.next_free_index_cache {
                return None;
            }
        }

        Some(self.read())
    }

    /// Common tail of `get`/`try_get`: move the value out and publish progress.
    #[inline(always)]
    fn read(&mut self) -> T {
        let inner = &*self.inner;
        let slot = &inner.buffer[self.reader_index & inner.mask];
        // SAFETY: the index is below the producer's published `next_free_index`,
        // so the slot holds an initialized `T` that we move out exactly once.
        let value = unsafe { (*slot.get()).assume_init_read() };

        self.reader_index = self.reader_index.wrapping_add(1);
        inner.reader_index.store(self.reader_index, Ordering::Release);

        // Wake a producer blocked in `put`. A no-op for the spin strategies.
        inner.put_wait.notify();

        value
    }

    /// Number of elements currently queued.
    #[inline]
    pub fn len(&self) -> usize {
        len(&self.inner)
    }

    /// Whether the queue is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        is_empty(&self.inner)
    }

    /// Whether the queue is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        is_full(&self.inner)
    }

    /// The buffer's true capacity (`N` rounded up to a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.mask + 1
    }
}

#[inline]
fn len<T, P, G>(inner: &Inner<T, P, G>) -> usize {
    inner
        .next_free_index
        .load(Ordering::Acquire)
        .wrapping_sub(inner.reader_index.load(Ordering::Acquire))
}

#[inline]
fn is_empty<T, P, G>(inner: &Inner<T, P, G>) -> bool {
    inner.reader_index.load(Ordering::Acquire) >= inner.next_free_index.load(Ordering::Acquire)
}

#[inline]
fn is_full<T, P, G>(inner: &Inner<T, P, G>) -> bool {
    inner.next_free_index.load(Ordering::Relaxed)
        > inner.reader_index.load(Ordering::Acquire).wrapping_add(inner.mask)
}
