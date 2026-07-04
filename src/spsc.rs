//! Single-producer / single-consumer ring buffer.
//!
//! A faithful port of `spsc.hpp`. Construct with [`Spsc::new`], which hands back
//! a [`Producer`] and a [`Consumer`]. Each handle is `Send` but neither is
//! `Clone`: the type system enforces the single-producer / single-consumer
//! contract that the C++ original left to the programmer.
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
//!   pointer, the mask, and raw pointers to the two shared atomics, so `put` /
//!   `get` never chase through `Arc<Inner>` to re-read constants — mirroring the
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

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cache_padded::CachePadded;
use crate::wait::{WaitStrategy, YieldWait};

struct Inner<T, P, G> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,

    /// Published by the producer (Release), read by the consumer (Acquire).
    write_cursor: CachePadded<AtomicUsize>,
    /// Published by the consumer (Release), read by the producer (Acquire).
    read_cursor: CachePadded<AtomicUsize>,

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
/// `Consumer::read`): `capacity / 8`, clamped to `[1, 64]`. Large enough to
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

/// Rejects zero capacities when `new` is monomorphized, turning what would be
/// a runtime panic into a compile error.
struct AssertCapacity<const N: usize>;

impl<const N: usize> AssertCapacity<N> {
    const NON_ZERO: () = assert!(N > 0, "capacity must be greater than zero");
}

/// Builder/namespace for constructing an SPSC ring buffer.
///
/// `N` is the requested minimum capacity; the real capacity is `N` rounded up
/// to the next power of two. `P` and `G` are the put-side and get-side
/// [`WaitStrategy`]s, defaulting to [`YieldWait`] exactly as the C++ template
/// defaults do.
pub struct Spsc<T, const N: usize, P = YieldWait, G = YieldWait>(
    core::marker::PhantomData<(T, P, G)>,
);

impl<T, const N: usize, P, G> Spsc<T, N, P, G>
where
    T: Send,
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    /// Create a ring buffer and return its producer and consumer halves.
    ///
    /// `N == 0` is rejected at compile time.
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new() -> (Producer<T, P, G>, Consumer<T, P, G>) {
        // Evaluated at monomorphization: `Spsc::<T, 0>::new()` fails to compile.
        let () = AssertCapacity::<N>::NON_ZERO;
        let capacity = N.next_power_of_two();

        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || UnsafeCell::new(MaybeUninit::uninit()));

        let inner = Arc::new(Inner {
            buffer: slots.into_boxed_slice(),
            mask: capacity - 1,
            write_cursor: CachePadded::new(AtomicUsize::new(0)),
            read_cursor: CachePadded::new(AtomicUsize::new(0)),
            put_wait: P::default(),
            get_wait: G::default(),
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

/// The producing half of an [`Spsc`]. Owns the private write cursor.
pub struct Producer<T, P, G> {
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
    inner: Arc<Inner<T, P, G>>,
}

// SAFETY: the producer half only touches producer-private state plus atomics.
// The cached `NonNull`s reference the `Arc<Inner>` it keeps alive.
unsafe impl<T: Send, P: Send + Sync, G: Send + Sync> Send for Producer<T, P, G> {}

impl<T, P, G> Producer<T, P, G>
where
    P: WaitStrategy,
    G: WaitStrategy,
{
    /// Block until there is room, then enqueue `value`.
    #[inline]
    pub fn put(&mut self, value: T) {
        while self.write_cursor > self.read_cursor_cache.wrapping_add(self.mask) {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            self.read_cursor_cache = unsafe { (*self.reader.as_ptr()).load(Ordering::Acquire) };
            if self.write_cursor > self.read_cursor_cache.wrapping_add(self.mask) {
                let next = self.write_cursor;
                let reader = self.reader.as_ptr();
                self.inner.put_wait.wait(|| {
                    next <= unsafe { (*reader).load(Ordering::Acquire) }.wrapping_add(self.mask)
                });
            }
        }

        self.write(value);
    }

    /// Enqueue `value` without blocking. Returns `Err(value)` if the buffer is
    /// full, handing the item back to the caller.
    #[inline]
    pub fn try_put(&mut self, value: T) -> Result<(), T> {
        if self.write_cursor > self.read_cursor_cache.wrapping_add(self.mask) {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            self.read_cursor_cache = unsafe { (*self.reader.as_ptr()).load(Ordering::Acquire) };
            if self.write_cursor > self.read_cursor_cache.wrapping_add(self.mask) {
                return Err(value);
            }
        }

        self.write(value);
        Ok(())
    }

    /// Common tail of `put`/`try_put`: store the value and publish it.
    #[inline(always)]
    fn write(&mut self, value: T) {
        // SAFETY: `index & mask` is always in `0..capacity`, so the pointer is
        // in bounds; skipping the bounds check keeps the hot path branch-free,
        // as in the C++ `contents_[i & mask]`. We are the only producer and have
        // confirmed the slot is free (the consumer moved its occupant out).
        unsafe {
            let slot = &*self.buf.as_ptr().add(self.write_cursor & self.mask);
            (*slot.get()).write(value);
        }

        self.write_cursor = self.write_cursor.wrapping_add(1);
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        unsafe { (*self.next_free.as_ptr()).store(self.write_cursor, Ordering::Release) };

        // Wake a consumer blocked in `get`. A no-op for the spin strategies,
        // which the compiler elides entirely.
        self.inner.get_wait.notify();
    }

    /// Number of elements currently queued.
    ///
    /// While the queue is backed up, the consumer defers its cursor publishes
    /// (see `Consumer::read`), so this may transiently over-count by up to
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

    /// Whether the queue is full (no room for another `put`). May transiently
    /// report `true` while the consumer defers publishes in the backed-up
    /// regime (see [`len`](Self::len)); never reports `false` for a truly
    /// full queue.
    #[inline]
    pub fn is_full(&self) -> bool {
        is_full(&self.inner)
    }

    /// The buffer's true capacity (`N` rounded up to a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

/// The consuming half of an [`Spsc`]. Owns the private read cursor.
pub struct Consumer<T, P, G> {
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
    /// [`read`](Self::read) for the adaptive publish rule).
    published: usize,
    /// Cached snapshot of the producer's `write_cursor` (the C++ `next_free_index_cache_`).
    write_cursor_cache: usize,
    /// Keeps the shared allocation alive and carries the wait strategies.
    inner: Arc<Inner<T, P, G>>,
}

impl<T, P, G> Drop for Consumer<T, P, G> {
    fn drop(&mut self) {
        // Publish any deferred progress. Without this, `Inner::drop` would
        // re-drop elements we already moved out, and a surviving producer
        // would never see the space we freed. (No `notify`: it would need a
        // `WaitStrategy` bound, and a `CvWait` producer re-checks every
        // 100 ns regardless.)
        if self.read_cursor != self.published {
            // SAFETY: `reader` is a `NonNull` into the live `inner`.
            unsafe { (*self.reader.as_ptr()).store(self.read_cursor, Ordering::Release) };
        }
    }
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
        while self.read_cursor >= self.write_cursor_cache {
            // SAFETY: `next_free` is a `NonNull` into the live `inner`.
            self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
            if self.read_cursor >= self.write_cursor_cache {
                let read_cursor = self.read_cursor;
                let next_free = self.next_free.as_ptr();
                self.inner
                    .get_wait
                    .wait(|| read_cursor < unsafe { (*next_free).load(Ordering::Acquire) });
            }
        }

        self.read()
    }

    /// Dequeue an element without blocking, or return `None` if empty.
    #[inline]
    pub fn try_get(&mut self) -> Option<T> {
        if self.read_cursor >= self.write_cursor_cache {
            // SAFETY: `next_free` is a `NonNull` into the live `inner`.
            self.write_cursor_cache = unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) };
            if self.read_cursor >= self.write_cursor_cache {
                return None;
            }
        }

        Some(self.read())
    }

    /// Common tail of `get`/`try_get`: move the value out and publish progress
    /// adaptively.
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
    /// report empty) with progress unpublished, so a blocked producer is
    /// stalled by at most the batch slack while the consumer is actively
    /// consuming.
    #[inline(always)]
    fn read(&mut self) -> T {
        // SAFETY: `index & mask` is always in bounds (see `Producer::write`).
        // The index is below the producer's published `write_cursor`, so the
        // slot holds an initialized `T` that we move out exactly once.
        let value = unsafe {
            let slot = &*self.buf.as_ptr().add(self.read_cursor & self.mask);
            (*slot.get()).assume_init_read()
        };

        self.read_cursor = self.read_cursor.wrapping_add(1);
        if self.read_cursor == self.write_cursor_cache
            || self.read_cursor.wrapping_sub(self.published) >= publish_batch(self.mask + 1)
        {
            self.flush();
        }

        value
    }

    /// Publish the private read cursor to the shared atomic and wake a
    /// producer blocked in `put`. The wake-up is a no-op for spin strategies.
    #[inline(always)]
    fn flush(&mut self) {
        // SAFETY: `reader` is a `NonNull` into the live `inner`.
        unsafe { (*self.reader.as_ptr()).store(self.read_cursor, Ordering::Release) };
        self.published = self.read_cursor;
        self.inner.put_wait.notify();
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
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        self.read_cursor >= unsafe { (*self.next_free.as_ptr()).load(Ordering::Acquire) }
    }

    /// Whether the queue is full. Exact on this side (see [`len`](Self::len)).
    #[inline]
    pub fn is_full(&self) -> bool {
        // SAFETY: `next_free` is a `NonNull` into the live `inner`.
        let write = unsafe { (*self.next_free.as_ptr()).load(Ordering::Relaxed) };
        write > self.read_cursor.wrapping_add(self.mask)
    }

    /// The buffer's true capacity (`N` rounded up to a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }
}

#[inline]
fn len<T, P, G>(inner: &Inner<T, P, G>) -> usize {
    inner
        .write_cursor
        .load(Ordering::Acquire)
        .wrapping_sub(inner.read_cursor.load(Ordering::Acquire))
}

#[inline]
fn is_empty<T, P, G>(inner: &Inner<T, P, G>) -> bool {
    inner.read_cursor.load(Ordering::Acquire) >= inner.write_cursor.load(Ordering::Acquire)
}

#[inline]
fn is_full<T, P, G>(inner: &Inner<T, P, G>) -> bool {
    inner.write_cursor.load(Ordering::Relaxed)
        > inner
            .read_cursor
            .load(Ordering::Acquire)
            .wrapping_add(inner.mask)
}
