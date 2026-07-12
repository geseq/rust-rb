//! Single-producer / **multi**-consumer broadcast ring buffer (lossy).
//!
//! Every consumer independently observes the message stream — but nobody can
//! slow the producer down: [`Producer::push`] **never blocks and never reads
//! a byte of consumer state**. A consumer that falls a full lap behind
//! *loses* messages instead of gating the producer, detects the loss with an
//! exact count ([`PopError::Lagged`]), and repositions to the oldest
//! retained message plus a configurable [slack](RingBuffer::with_slack). The
//! lossless alternative — a slow consumer gates the producer — is the
//! separate [`crate::spmc`] machine; the two are different machines, not one
//! type with a mode flag.
//!
//! # Quick start
//!
//! ```
//! use rust_rb::broadcast::{PopError, RingBuffer};
//!
//! let (mut tx, mut rx) = RingBuffer::new(8);
//! let mut rx2 = rx.subscribe(); // dynamic membership: never fails
//!
//! tx.push(1u64);
//! assert_eq!(rx.pop(), Ok(1));
//! assert_eq!(rx2.pop(), Ok(1)); // both consumers see the message
//!
//! drop(tx); // producer drop closes the ring
//! assert_eq!(rx.pop(), Err(PopError::Closed));
//! ```
//!
//! # Loss semantics
//!
//! A lap is detected at the slot, not guessed from counters: each slot is a
//! tiny seqlock, and a reader that finds a newer generation (or a torn
//! payload) repositions and reports **exactly** how many messages it missed:
//!
//! ```
//! use rust_rb::broadcast::{PopError, RingBuffer};
//!
//! let (mut tx, mut rx) = RingBuffer::<u64>::with_slack(8, 2);
//! for i in 0..20 {
//!     tx.push(i); // never blocks — the reader cannot gate it
//! }
//! // The reader lost the overwritten prefix: new position is
//! // tail - capacity + slack = 20 - 8 + 2 = 14 …
//! assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 14 }));
//! // … and resumes there with gap-free accounting.
//! assert_eq!(rx.pop(), Ok(14));
//! ```
//!
//! The `slack` keeps a freshly repositioned reader from lagging again on the
//! very next push (the lag-storm livelock of naive jump-to-oldest): after a
//! reposition, `capacity - slack` messages are immediately readable and the
//! producer must advance at least `slack` more before this consumer can lag
//! again. Larger slack = fewer, bigger loss events; smaller = maximal
//! salvage. For self-contained streams (market data), skip recovery entirely
//! with [`Consumer::skip_to_latest`].
//!
//! # Membership
//!
//! Consumers are **pure readers**: no registry, no leases, unbounded count.
//! [`Producer::subscribe`]/[`Consumer::subscribe`] just read the current
//! tail and start there (never fails — subscribing to a closed ring succeeds
//! and pops [`PopError::Closed`]); dropping a consumer is a no-op for
//! everyone else.
//!
//! # Closed contract
//!
//! Dropping the [`Producer`] closes the ring. [`Consumer::pop`] returns
//! `Err(`[`PopError::Closed`]`)` only once the producer is gone **and** this
//! consumer has drained every published message it can still reach —
//! published slots stay readable after producer death (slot generations are
//! stable). [`Consumer::try_pop`] returns `Ok(None)` for empty-but-alive.
//!
//! # Element bound: [`NoUninit`], and `T: Send` but *not* `T: Sync`
//!
//! Payloads are copied in and out **word-wise atomically**, which reads
//! every byte of the value representation — so `T` must carry no padding
//! bytes or uninit niches ([`NoUninit`], a tightened `Copy`). And unlike
//! [`crate::spmc`], consumers never take `&T` borrows into the ring: every
//! accepted read is a validated copy-out, so values only ever *move* (by
//! copy) across threads — `T: Send` suffices and `T: Sync` is not required.
//!
//! # Why it is fast
//!
//! * **Consumers spin on the shared `tail`, not the slot generations**: the
//!   producer writes each slot's seqlock word 2–3× per message, so parking
//!   spinners there would put per-message stores on a polled line. The tail
//!   is one cursor line written once per push — the SPSC caught-up profile.
//! * **Per-slot seqlock, validate-only**: one generation fetch before the
//!   copy, one fence + re-check after. Torn bytes are discarded as
//!   `MaybeUninit` and never materialized as `T`.
//! * **All position arithmetic in `u64`** — a slot's generation series is
//!   strictly increasing, so exact-match acceptance is ABA-free (u64 wraps
//!   in ~29 years at 10 G msg/s).
//!
//! Consumer wait strategies must be [`SelfTimed`] — the producer keeps zero
//! consumer knowledge, so nobody will ever notify a parked reader:
//!
//! ```compile_fail
//! // CvWait is not SelfTimed: the broadcast ring rejects it.
//! use rust_rb::{broadcast, CvWait};
//! let _ = broadcast::RingBuffer::<u64, CvWait>::with_wait_strategies(8);
//! ```

use std::cell::UnsafeCell;
use std::mem::{size_of, MaybeUninit};
use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicU64, Ordering};
use std::sync::Arc;

use crate::atomic_copy::write_payload;
use crate::cache_padded::CachePadded;
use crate::seqlock::{read_valid, Slot};
use crate::wait::{SelfTimed, WaitStrategy, YieldWait};

/// Marker for element types whose value representation has **no padding
/// bytes and no uninitialized niches** — every byte is initialized data.
///
/// The broadcast ring copies payloads in and out with word-wise atomic
/// loads/stores, which read *every* byte of the value; an atomic load over a
/// padding byte is undefined behaviour even single-threaded, so bare `Copy`
/// is not enough. This is the same contract as `bytemuck::NoUninit` (users
/// of that crate can forward their impls); this crate keeps zero
/// dependencies, hence the local trait.
///
/// Note the **difference from [`crate::shm::ShmItem`]**, which points the
/// other way: `ShmItem` asserts that *any bit pattern a peer writes is a
/// valid `T`* (reads of untrusted memory), while `NoUninit` asserts that
/// *every byte of a valid `T` is initialized* (writes/reads touch every
/// byte). `bool`, for example, *would* satisfy `NoUninit`'s requirement but
/// not `ShmItem`'s — though this crate deliberately implements neither for
/// it (only the integer/float primitives and their arrays are provided;
/// anything else is an explicit user opt-in).
///
/// # Safety
///
/// Implementors assert that a value of the type contains **no padding bytes
/// and no uninitialized/niche bytes anywhere in its representation** — a
/// byte-wise copy of the value reads only initialized memory.
///
/// # Examples
///
/// Integers, floats, and arrays of them are ready to use. A `#[repr(C)]`
/// struct with no padding can opt in:
///
/// ```
/// use rust_rb::broadcast::NoUninit;
///
/// #[derive(Clone, Copy)]
/// #[repr(C)]
/// struct Tick {
///     price: u64,
///     qty: u64,
/// }
///
/// // SAFETY: two `u64`s under `repr(C)` — no padding, every byte
/// // initialized.
/// unsafe impl NoUninit for Tick {}
/// ```
pub unsafe trait NoUninit: Copy {}

macro_rules! no_uninit {
    ($($t:ty),*) => {$(
        // SAFETY: primitive numeric type — no padding, every byte of the
        // representation is initialized.
        unsafe impl NoUninit for $t {}
    )*};
}
no_uninit!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

// SAFETY: an array's stride equals its element size — no padding is added
// between or around elements.
unsafe impl<T: NoUninit, const N: usize> NoUninit for [T; N] {}

/// Error returned by consumer pops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// The producer lapped this consumer: `missed` messages were overwritten
    /// before they could be read. The consumer has already repositioned to
    /// `tail - capacity + slack`, so the *next* pop reads from there;
    /// accounting is exact and gap-free (`old_pos + missed == new_pos`), so
    /// summing `missed` across errors plus the accepted count reproduces the
    /// number pushed. With `slack == 0` a transient `missed == 0` is
    /// possible while the producer is overwriting exactly one lap ahead.
    Lagged {
        /// Number of messages irrecoverably skipped, exact as of detection.
        missed: u64,
    },
    /// The ring is **closed and drained**: the producer was dropped and this
    /// consumer has consumed every published message still reachable.
    Closed,
}

impl core::fmt::Display for PopError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PopError::Lagged { missed } => {
                write!(f, "consumer lagged: {missed} messages overwritten unread")
            }
            PopError::Closed => {
                f.write_str("ring closed: producer dropped and all published messages consumed")
            }
        }
    }
}

impl std::error::Error for PopError {}

/// Round a requested minimum capacity to the ring's real capacity: the next
/// power of two, via the one crate-wide rounding policy (floor 1 — there is
/// no gating machinery a tiny ring could break; capacity 1 is a "latest
/// value" cell). Shared with the shm constructors so heap and shm cannot
/// round the same request differently.
///
/// # Panics
///
/// Panics if `min_capacity == 0` or the rounding overflows `usize`.
fn round_capacity(min_capacity: usize) -> usize {
    crate::cursor::round_capacity(min_capacity, 1)
}

/// The default reposition slack: `capacity / 8`, clamped to at least 1 (and
/// to 0 for a capacity-1 ring, where any positive slack would reach the
/// unwritten future).
#[inline]
pub(crate) const fn default_slack(capacity: u64) -> u64 {
    if capacity == 1 {
        0
    } else {
        let slack = capacity / 8;
        if slack == 0 {
            1
        } else {
            slack
        }
    }
}

/// The lap-recovery target: `tail - capacity + slack`, computed
/// underflow-safe (a stale tail observation cannot wrap below zero — the
/// caller additionally clamps to never move backwards).
#[inline(always)]
pub(crate) const fn reposition_target(tail: u64, capacity: u64, slack: u64) -> u64 {
    tail.saturating_add(slack).saturating_sub(capacity)
}

/// The producer-published cache line: the tail (count of published
/// messages, stored once per push) plus, co-located in the same padded slot,
/// the `closed` flag (written once by `Producer::drop`, read only on
/// consumer would-block paths — the line consumers already poll).
///
/// `closed` is a whole word (0 = open, nonzero = closed), not a bool, so the
/// shm header can host the very same field at a fixed offset with one atomic
/// type on both backings.
struct TailSide {
    tail: AtomicU64,
    closed: AtomicU64,
}

/// The state all handles share, kept alive by an `Arc`.
///
/// No `Drop` impl: `T: NoUninit` implies `Copy`, so elements never need
/// dropping — teardown is just freeing the slot storage.
struct Shared<T> {
    slots: Box<[Slot<T>]>,
    /// `capacity - 1`, in the u64 domain of all position arithmetic.
    mask: u64,
    /// The reposition slack (validated `< capacity` at construction).
    slack: u64,
    tail_side: CachePadded<TailSide>,
}

// SAFETY: slot payloads are written only by the single producer, under the
// seqlock protocol; consumers copy bytes out with atomic loads and
// materialize a `T` only after generation validation. Each accepted copy
// transfers a value (by copy) to the consumer's thread — that is `T: Send`.
// No `&T` into the ring is ever shared across threads (copy-out only), so
// `T: Sync` is NOT required — unlike the gating `spmc` ring, whose readers
// borrow the same element concurrently.
unsafe impl<T: Send> Sync for Shared<T> {}
// SAFETY: as above; the owning handle may move between threads.
unsafe impl<T: Send> Send for Shared<T> {}

// The strict word-wise atomic payload copy ([M-F10]/[M-F11]) lives in
// `crate::atomic_copy` (`write_payload`/`read_payload`), shared with the
// composed `crate::anchored` ring; the `rust_rb_volatile_copy` dev cfg swaps
// in whole-payload volatile copies there, exactly as before.

/// Builder/namespace for constructing a lossy broadcast ring buffer.
///
/// [`new`](Self::new) takes the minimum capacity at runtime (rounded up to
/// the next power of two, minimum 1) and uses [`YieldWait`] consumers. Pick
/// another consumer [`WaitStrategy`] with
/// [`with_wait_strategies`](Self::with_wait_strategies), and the reposition
/// slack with [`with_slack`](Self::with_slack).
///
/// There is **no producer-side strategy parameter**: the producer
/// structurally cannot wait, and a phantom parameter on it would be a
/// type-level lie. Consumer strategies must be [`SelfTimed`] (spin, yield,
/// sleep, backoff): the producer keeps zero consumer knowledge, so a
/// notify-dependent strategy could park forever.
pub struct RingBuffer<T, C = YieldWait>(core::marker::PhantomData<(T, C)>);

impl<T: NoUninit + Send> RingBuffer<T> {
    /// Create a ring buffer with the default consumer wait strategy and
    /// return its producer and one initial consumer (subscribe more from
    /// either handle).
    ///
    /// The real capacity is `min_capacity` rounded up to the next power of
    /// two; the reposition slack defaults to `capacity / 8`, clamped to at
    /// least 1 (0 for a capacity-1 ring).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0` or `T` is zero-sized (a ZST carries no
    /// data to broadcast and would break the word-wise copy contract).
    #[allow(clippy::new_ret_no_self)] // intentionally returns the producer/consumer pair
    pub fn new(min_capacity: usize) -> (Producer<T>, Consumer<T>) {
        RingBuffer::<T, YieldWait>::with_wait_strategies(min_capacity)
    }
}

impl<T, C> RingBuffer<T, C>
where
    T: NoUninit + Send,
    C: SelfTimed + Send,
{
    /// Create a ring buffer with an explicit consumer wait strategy and the
    /// default slack, and return its producer and one initial consumer.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0` or `T` is zero-sized.
    pub fn with_wait_strategies(min_capacity: usize) -> (Producer<T>, Consumer<T, C>) {
        let capacity = round_capacity(min_capacity);
        Self::build(capacity, default_slack(capacity as u64) as usize)
    }

    /// Create a ring buffer with an explicit reposition `slack` [A-3.2].
    ///
    /// After a lap, a consumer repositions to `tail - capacity + slack`:
    /// `capacity - slack` messages are immediately readable and the producer
    /// must advance at least `slack` before that consumer can lag again.
    /// `slack == 0` maximizes salvage but allows back-to-back lag events
    /// (and a transient `Lagged { missed: 0 }` while the producer is
    /// overwriting exactly one lap ahead).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`, if `slack >= capacity` (after
    /// power-of-two rounding), or if `T` is zero-sized.
    pub fn with_slack(min_capacity: usize, slack: usize) -> (Producer<T>, Consumer<T, C>) {
        let capacity = round_capacity(min_capacity);
        assert!(slack < capacity, "slack must be less than the capacity");
        Self::build(capacity, slack)
    }

    fn build(capacity: usize, slack: usize) -> (Producer<T>, Consumer<T, C>) {
        assert!(
            size_of::<T>() != 0,
            "zero-sized element types are not supported"
        );
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || Slot {
            seq: AtomicU64::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        });

        let shared = Arc::new(Shared {
            slots: slots.into_boxed_slice(),
            mask: capacity as u64 - 1,
            slack: slack as u64,
            tail_side: CachePadded::new(TailSide {
                tail: AtomicU64::new(0),
                closed: AtomicU64::new(0),
            }),
        });

        let consumer = subscribe_from(&shared);
        // The buffer pointer is derived from the whole-slice `as_ptr` (not a
        // first-element reference) so it keeps provenance over every slot.
        let buf = NonNull::new(shared.slots.as_ptr().cast_mut()).expect("buffer is non-null");
        let tail = NonNull::from(&shared.tail_side.tail);
        let closed = NonNull::from(&shared.tail_side.closed);
        let producer = Producer {
            buf,
            mask: shared.mask,
            next_seq: 0,
            tail_batch: 1,
            unpublished: 0,
            tail,
            closed,
            anchor: ProducerAnchor::Heap(shared),
        };
        (producer, consumer)
    }
}

/// Register a new consumer: read the tail and start there. Trivially
/// dynamic — a consumer is pure reader state, so there is no registry to
/// claim, nothing that can fail, and no bound on the count.
fn subscribe_from<T, C: SelfTimed>(shared: &Arc<Shared<T>>) -> Consumer<T, C> {
    let shared = Arc::clone(shared);
    // The join point: only messages published after this tail are seen. A
    // consumer subscribed to a closed ring is born drained and pops Closed.
    let pos = shared.tail_side.tail.load(Ordering::Acquire);
    let buf = NonNull::new(shared.slots.as_ptr().cast_mut()).expect("buffer is non-null");
    let tail = NonNull::from(&shared.tail_side.tail);
    let closed = NonNull::from(&shared.tail_side.closed);
    Consumer {
        buf,
        mask: shared.mask,
        slack: shared.slack,
        pos,
        tail_cache: pos,
        wait: C::default(),
        tail,
        closed,
        anchor: ConsumerAnchor::Heap(shared),
    }
}

/// Where the producing handle's shared state lives — the anchor seam,
/// mirroring `crate::spmc`'s registry seam: every hot atomic is reached
/// through the handle's cached raw pointers (identical for both variants);
/// the anchor is consulted only on cold paths (subscribe, teardown).
enum ProducerAnchor<T> {
    /// In-process ring: the shared state lives on the heap in an `Arc`.
    Heap(Arc<Shared<T>>),
    /// Cross-process ring: the state lives in a mapped shared region; the
    /// anchor holds the producer role lease. Boxed so enabling the feature
    /// does not grow heap handles.
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Box<crate::shm::BcastProducerAnchor>),
}

impl<T> ProducerAnchor<T> {
    /// Whether teardown may write the ring-wide closed word. Heap: always.
    /// Shm: only the current producer-lease holder in the constructing
    /// process — a fork-inherited copy or a superseded zombie must not close
    /// the successor's session (and a crashed producer never runs Drop at
    /// all, which is why shm `Closed` covers graceful drops only).
    #[inline]
    fn teardown_allowed(&self) -> bool {
        match self {
            ProducerAnchor::Heap(_) => true,
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            ProducerAnchor::Shm(anchor) => anchor.owned_by_current_process() && anchor.owns_lease(),
        }
    }
}

/// The consuming handle's side of the anchor seam: purely a keep-alive. A
/// lossy consumer is a pure reader — dropping it releases no lease and
/// writes nothing, so the shm variant is nothing but the mapping `Arc`
/// (mapped **read-only**: any accidental store in the consumer path would be
/// a deterministic SIGSEGV, which is the enforcement).
enum ConsumerAnchor<T> {
    Heap(Arc<Shared<T>>),
    #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
    Shm(Arc<crate::shm::ShmRegion>),
}

/// The producing half of a [`RingBuffer`]. `Send` but not `Clone`: exactly
/// one producer, enforced by the type system.
///
/// The producer **never blocks and never reads consumer state** — hence no
/// `try_push` (a push cannot fail), no `len`/`is_full`/`is_empty` (the ring
/// is never full from the producer's point of view, and "length" is
/// per-consumer: see [`Consumer::lag`]), and no wait-strategy parameter.
///
/// Dropping the producer **closes** the ring: consumers drain what was
/// published and then see [`PopError::Closed`]. The close is a flag store
/// with no notify — consumer strategies are [`SelfTimed`] by construction,
/// so a parked reader re-checks and wakes itself.
pub struct Producer<T> {
    /// Base of the slot buffer (cached; stable for the anchor's lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Next sequence to write (producer-private; equals the published tail
    /// between pushes whenever `unpublished == 0`).
    next_seq: u64,
    /// Publish the tail at least every this many pushes (default 1 —
    /// per-push, the exact-visibility baseline). See
    /// [`set_tail_batch`](Self::set_tail_batch).
    tail_batch: u64,
    /// Pushes since the last tail publication (always `< tail_batch`).
    unpublished: u64,
    /// The shared tail (cached raw pointer; heap: into the `Arc`, shm: into
    /// the mapped region — the hot publish path is identical).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (written once, on drop).
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc` or shm mapping + lease).
    anchor: ProducerAnchor<T>,
}

// SAFETY: the producer only touches producer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive. `T: Send` per the
// shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send> Send for Producer<T> {}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Flag only, no notify [A-1.2]: consumers use SelfTimed strategies
        // (enforced at construction), whose waits re-check the closed flag
        // without a peer wake — the producer keeps zero consumer knowledge
        // even at teardown. Guarded for shm (heap: constant true): only a
        // graceful drop by the live lease holder closes the ring — a
        // fork-inherited copy or superseded zombie must not end the
        // successor's session.
        if self.anchor.teardown_allowed() {
            // Publish any tail debt a `set_tail_batch` window is holding —
            // closed-and-drained must mean drained of *everything pushed*.
            // Same guard as the close flag: a superseded zombie or
            // fork-inherited copy must not touch the successor's tail.
            if self.unpublished > 0 {
                // SAFETY: `tail` points into shared state the anchor keeps
                // alive.
                unsafe { self.tail.as_ref() }.store(self.next_seq, Ordering::Release);
            }
            // SAFETY: `closed` points into shared state the anchor keeps
            // alive.
            unsafe { self.closed.as_ref() }.store(1, Ordering::Release);
        }
    }
}

impl<T: NoUninit> Producer<T> {
    /// Enqueue `value`. **Never blocks, never fails, never reads consumer
    /// state** — a consumer that has not kept up is lapped and will observe
    /// [`PopError::Lagged`].
    ///
    /// The write is bracketed by the slot's seqlock generation (odd while
    /// writing, even when published) and the payload is stored word-wise
    /// atomically, so a racing reader either gets the complete message or
    /// rejects the copy — never a torn `T`.
    #[inline]
    pub fn push(&mut self, value: T) {
        let s = self.next_seq;
        let slot = self.slot(s);
        // Invalidate: readers of the old occupant now fail revalidation.
        // Relaxed suffices — the fence below does the ordering [M-F10].
        slot.seq.store(2 * s + 1, Ordering::Relaxed);
        // Order the invalidation before the payload stores (and keep the
        // payload stores from hoisting above it).
        fence(Ordering::Release);
        // SAFETY: single producer; the slot is ours to write and readers
        // race only through atomics (see `write_payload`).
        unsafe { write_payload(slot.data.get(), &value) };
        // Publish the slot: an exact-match reader accepts generation 2s+2.
        slot.seq.store(2 * s + 2, Ordering::Release);
        self.next_seq = s + 1;
        // Publish the frontier: per push by default [P-F4] — the tail is
        // the only line consumers spin on, and per-push publication makes
        // visibility, `Lagged` counts, and subscribe join points exact for
        // free. Under `set_tail_batch(n)` the store is amortized to every
        // n-th push: caught-up spinning readers steal the tail line on
        // every store, so each skipped store is a coherence round the
        // producer does not pay (`rust-rb-6l0`).
        self.unpublished += 1;
        if self.unpublished >= self.tail_batch {
            self.unpublished = 0;
            // SAFETY: `tail` points into shared state the anchor keeps
            // alive.
            unsafe { self.tail.as_ref() }.store(self.next_seq, Ordering::Release);
        }
    }

    /// Publish everything pushed so far, making it visible to consumers
    /// immediately.
    ///
    /// A no-op at the default per-push publication; under
    /// [`set_tail_batch`](Self::set_tail_batch) call this at burst
    /// boundaries so the tail never trails a quiet producer.
    #[inline]
    pub fn flush(&mut self) {
        if self.unpublished > 0 {
            self.unpublished = 0;
            // SAFETY: `tail` points into shared state the anchor keeps
            // alive.
            unsafe { self.tail.as_ref() }.store(self.next_seq, Ordering::Release);
        }
    }

    /// Publish the tail at least every `batch` pushes instead of on every
    /// push (clamped to `[1, capacity / 8]`; `1` — the default — restores
    /// exact per-push visibility). Returns the clamp actually applied.
    ///
    /// **The trade**: caught-up *spinning* consumers pull the tail line
    /// away after every store, so per-push publication couples the
    /// producer to its readers — on a GB10/X925, 4.5 ns/push alone became
    /// 25.9/35.6/56.6 ns with 1/2/4 spinning readers, and `batch = 8` cut
    /// those to 12.7/16.6/19.6 (`rust-rb-6l0`). In exchange, up to
    /// `batch - 1` already-pushed messages stay invisible until the next
    /// boundary, [`flush`](Self::flush), or producer drop — so bursty or
    /// latency-critical streams should keep the default and flush-heavy
    /// pipelines should call `flush` when going idle.
    ///
    /// Everything else is unchanged: consumers never see torn records
    /// (the per-slot seqlock validates independently of the tail),
    /// `Lagged` accounting stays exact against the published frontier, and
    /// a subscriber's join point is the published tail.
    pub fn set_tail_batch(&mut self, batch: usize) -> usize {
        let max = ((self.mask + 1) / 8).max(1);
        let batch = (batch as u64).clamp(1, max);
        self.tail_batch = batch;
        // Shrinking the window must not leave an already-oversized debt
        // pending past the new bound.
        if self.unpublished >= batch {
            self.flush();
        }
        batch as usize
    }

    /// Subscribe a new consumer with wait strategy `C`; its join point is
    /// the current tail — it sees only messages published after this call.
    /// Never fails: consumers are unbounded pure readers (subscribing to a
    /// ring whose producer is *about to* drop simply yields a consumer that
    /// drains and pops [`PopError::Closed`]).
    ///
    /// `C` cannot be inferred from `self` (the producer carries no consumer
    /// strategy — by design); name it, e.g.
    /// `tx.subscribe::<rust_rb::YieldWait>()`, or subscribe from an existing
    /// consumer.
    ///
    /// On a shared-memory ring the subscriber shares this producer's
    /// **read-write** mapping (same process, same pages) — the read-only
    /// enforcement story belongs to
    /// [`attach_shm_consumer`](RingBuffer::attach_shm_consumer), which maps
    /// the region afresh with `PROT_READ`.
    pub fn subscribe<C: SelfTimed + Send>(&self) -> Consumer<T, C> {
        match &self.anchor {
            ProducerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the anchor's region was validated for this `T` and
            // capacity when this handle was constructed; the slack is the
            // create-time header field every consumer inherits.
            ProducerAnchor::Shm(anchor) => {
                let region = Arc::clone(anchor.region());
                let slack = region.bcast_slack();
                unsafe { Consumer::from_shm(region, (self.mask + 1) as usize, slack) }
            }
        }
    }

    /// Number of messages **pushed** so far. Producer-local and exact.
    ///
    /// This is also the published frontier, except under
    /// [`set_tail_batch`](Self::set_tail_batch), where the published tail
    /// may trail it by up to `batch - 1` messages until the next boundary,
    /// [`flush`](Self::flush), or drop.
    #[inline]
    pub fn tail(&self) -> u64 {
        self.next_seq
    }

    /// The ring's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// Reference to the slot sequence `s` maps to (in bounds by masking).
    #[inline(always)]
    fn slot(&self, s: u64) -> &Slot<T> {
        // SAFETY: `s & mask` is in `0..capacity`; `buf` is the live buffer
        // the `Arc` keeps alive.
        unsafe { &*self.buf.as_ptr().add((s & self.mask) as usize) }
    }
}

/// A consuming handle of a [`RingBuffer`]: a **pure reader** — private
/// position, private tail cache, its own wait strategy instance, and nothing
/// the producer (or any other consumer) ever looks at. `Send` but not
/// `Clone`; create more consumers with [`subscribe`](Self::subscribe).
///
/// Dropping a consumer is a no-op for everyone else: there is no registry
/// slot to release and nobody gates on this reader.
pub struct Consumer<T, C: WaitStrategy = YieldWait> {
    /// Base of the slot buffer (cached; stable for the anchor's lifetime).
    buf: NonNull<Slot<T>>,
    /// `capacity - 1` (cached).
    mask: u64,
    /// Reposition slack (cached).
    slack: u64,
    /// Next position to read (private; `pos <= tail` always).
    pos: u64,
    /// Cached snapshot of the producer's tail.
    tail_cache: u64,
    /// This consumer's own wait strategy instance ([`SelfTimed`] by
    /// construction — waiting is purely local, no notify ever arrives).
    wait: C,
    /// The shared tail (cached raw pointer; **loads only** — the whole
    /// consumer path is write-free, which is what lets the shm variant hold
    /// a read-only mapping).
    tail: NonNull<AtomicU64>,
    /// The shared closed word (loads only, would-block paths).
    closed: NonNull<AtomicU64>,
    /// Keeps the ring's memory alive (heap `Arc` or shm mapping).
    anchor: ConsumerAnchor<T>,
}

// SAFETY: the consumer only touches consumer-private state plus atomics; the
// cached pointers reference state the anchor keeps alive. `T: Send` per the
// shared-state contract (see `Shared`'s impls).
unsafe impl<T: Send, C: WaitStrategy + Send> Send for Consumer<T, C> {}

impl<T: NoUninit, C: WaitStrategy> Consumer<T, C> {
    /// Block until a message is available, then dequeue it by validated
    /// copy.
    ///
    /// Returns `Err(`[`PopError::Lagged`]`)` if the producer lapped this
    /// consumer (the position has already been repositioned — the next pop
    /// proceeds from there), or `Err(`[`PopError::Closed`]`)` once the
    /// producer is dropped **and** everything reachable has been drained.
    ///
    /// Panic-free: no user code runs (`T: `[`NoUninit`] is `Copy`), and torn
    /// bytes are discarded before ever becoming a `T`.
    #[inline]
    pub fn pop(&mut self) -> Result<T, PopError> {
        self.wait_for_item()?;
        self.read_slot()
    }

    /// Dequeue a message without blocking. `Ok(None)` means
    /// empty-but-alive; the errors are as for [`pop`](Self::pop).
    #[inline]
    pub fn try_pop(&mut self) -> Result<Option<T>, PopError> {
        if self.has_item() {
            return self.read_slot().map(Some);
        }
        self.check_closed()?;
        if self.available() {
            // The close re-check refreshed the tail and found a final
            // message published just before the producer dropped.
            return self.read_slot().map(Some);
        }
        Ok(None)
    }

    /// Jump this consumer to the current tail, abandoning everything
    /// published but unread. Returns how many messages were skipped.
    ///
    /// The explicit market-data recovery: after a lap (or on demand), start
    /// from the freshest message instead of salvaging the retained window.
    #[inline]
    pub fn skip_to_latest(&mut self) -> u64 {
        let tail = self.refresh();
        let skipped = tail.saturating_sub(self.pos);
        // Never move backwards (mirrors the byte twin): a position ahead of
        // a stale tail observation must clamp, not regress — an unchecked
        // `tail - pos` would also underflow-panic there.
        self.pos = self.pos.max(tail);
        skipped
    }

    /// How far this consumer trails the producer: `tail - position` per a
    /// fresh tail read (saturating). `0` means fully caught up; a lag of
    /// `capacity` or more means the next pop will report
    /// [`PopError::Lagged`].
    #[inline]
    pub fn lag(&self) -> u64 {
        // SAFETY: `tail` points into shared state the anchor keeps alive.
        unsafe { self.tail.as_ref() }
            .load(Ordering::Acquire)
            .saturating_sub(self.pos)
    }

    /// Subscribe a further consumer with the same wait strategy; its join
    /// point is the current tail. Never fails (see
    /// [`Producer::subscribe`]). On a shared-memory ring the sibling shares
    /// this consumer's mapping (read-only if this one attached read-only).
    pub fn subscribe(&self) -> Consumer<T, C>
    where
        C: SelfTimed + Send,
    {
        match &self.anchor {
            ConsumerAnchor::Heap(shared) => subscribe_from(shared),
            #[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
            // SAFETY: the region was validated for this `T` and capacity
            // when this handle was constructed; slack is ring-wide config.
            ConsumerAnchor::Shm(region) => unsafe {
                Consumer::from_shm(Arc::clone(region), (self.mask + 1) as usize, self.slack)
            },
        }
    }

    /// The ring's true capacity (the requested minimum rounded up to a
    /// power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.mask + 1) as usize
    }

    /// Whether the cached tail shows an available message.
    #[inline(always)]
    fn available(&self) -> bool {
        self.tail_cache > self.pos
    }

    /// Unconditionally reload the cached tail (`Acquire`) and return it.
    #[inline(always)]
    fn refresh(&mut self) -> u64 {
        // SAFETY: `tail` points into shared state the anchor keeps alive.
        self.tail_cache = unsafe { self.tail.as_ref() }.load(Ordering::Acquire);
        self.tail_cache
    }

    /// Check for at least one available message, reloading the tail at most
    /// once.
    #[inline(always)]
    fn has_item(&mut self) -> bool {
        if !self.available() {
            self.refresh();
            return self.available();
        }
        true
    }

    /// The would-block close check: if the producer is gone, re-read the
    /// tail once more (the `Acquire` load of `closed` synchronizes with the
    /// producer's `Release` store, which follows its final publish) and
    /// report [`PopError::Closed`] only if genuinely drained.
    #[inline]
    fn check_closed(&mut self) -> Result<(), PopError> {
        // SAFETY: `closed` points into shared state the anchor keeps alive.
        if unsafe { self.closed.as_ref() }.load(Ordering::Acquire) != 0 {
            self.refresh();
            // Drained is `<=`, not `==`: a header poke (or a producer crash
            // observed through stale counters) can transiently leave `pos`
            // *ahead* of the committed tail, and an equality check would
            // then never report `Closed` — a drain livelock on a dead ring.
            if self.tail_cache <= self.pos {
                return Err(PopError::Closed);
            }
        }
        Ok(())
    }

    /// Spin/park (per this consumer's wait strategy) until a message is
    /// available or the ring is closed and drained. The wait spins on the
    /// shared **tail** (one line, stored once per push), never on a slot's
    /// seqlock word [P-F4]; its predicate also checks `closed` [A-1.2].
    #[inline(always)]
    fn wait_for_item(&mut self) -> Result<(), PopError> {
        loop {
            if self.has_item() {
                return Ok(());
            }
            self.check_closed()?;
            if self.available() {
                return Ok(());
            }
            // SAFETY: the pointers reference shared state the anchor keeps
            // alive.
            let (tail, closed) = unsafe { (self.tail.as_ref(), self.closed.as_ref()) };
            let pos = self.pos;
            self.wait
                .wait(|| tail.load(Ordering::Acquire) > pos || closed.load(Ordering::Acquire) != 0);
        }
    }

    /// The seqlock read at the current position (the caller established
    /// `tail > pos`): validate, copy, revalidate — or detect the lap and
    /// reposition.
    #[inline]
    fn read_slot(&mut self) -> Result<T, PopError> {
        let s = self.pos;
        // The caller established `tail > s`, which is `read_valid`'s
        // precondition (tail Release-stored after the slot publish).
        if let Some(v) = read_valid(self.slot(s), 2 * s + 2) {
            self.pos = s + 1;
            return Ok(v);
        }
        // Lapped: the slot moved on to a newer generation (or tore the
        // copy). Reposition first, then report [A-3].
        Err(PopError::Lagged {
            missed: self.reposition(),
        })
    }

    /// Lap recovery: jump to `tail - capacity + slack` per a fresh tail read
    /// and return the exact number of messages skipped. Never moves
    /// backwards (a stale tail observation clamps to the current position).
    #[cold]
    fn reposition(&mut self) -> u64 {
        let tail = self.refresh();
        let new_pos = reposition_target(tail, self.mask + 1, self.slack).max(self.pos);
        let missed = new_pos - self.pos;
        self.pos = new_pos;
        missed
    }

    /// Reference to the slot sequence `s` maps to (in bounds by masking).
    #[inline(always)]
    fn slot(&self, s: u64) -> &Slot<T> {
        // SAFETY: `s & mask` is in `0..capacity`; `buf` is the live buffer
        // the `Arc` keeps alive.
        unsafe { &*self.buf.as_ptr().add((s & self.mask) as usize) }
    }
}

// ---------------------------------------------------------------------------
// Shared-memory plumbing (crate-internal; the public constructors live in
// `crate::shm`). The handles built here are the ordinary `Producer`/
// `Consumer` types over region pointers — the hot paths are byte-identical
// to the heap ring's; only the anchor differs. Consumers hold a READ-ONLY
// mapping: the entire consumer path is loads plus private state, so an
// accidental store regression is a deterministic SIGSEGV.
// ---------------------------------------------------------------------------

/// The default-slack policy, shared with the shm constructors so they cannot
/// drift from the heap ring's.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) const fn shm_default_slack(capacity: u64) -> u64 {
    default_slack(capacity)
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T> Producer<T> {
    /// Build a producer over a validated shm region. Seeds `next_seq` from
    /// the live tail, so an attached or recovered producer resumes exactly
    /// after the last *published* message (a slot the predecessor died
    /// writing was never covered by a tail store, so re-publishing it is the
    /// SPSC crash-consistency story; a consumer racing the re-publish
    /// self-heals via the slot's seqlock generation).
    ///
    /// # Safety
    ///
    /// The anchor's region must be a validated broadcast element ring of
    /// exactly this `T` and `capacity` (`create`/`open` in `crate::shm`),
    /// and the anchor must hold the producer lease.
    pub(crate) unsafe fn from_shm(
        anchor: Box<crate::shm::BcastProducerAnchor>,
        capacity: usize,
    ) -> Self {
        let region = anchor.region();
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let buf = region.bcast_elem_buffer().cast::<Slot<T>>();
        // SAFETY: `tail` references the live mapping (per contract).
        let next_seq = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        Producer {
            buf,
            mask: capacity as u64 - 1,
            next_seq,
            tail_batch: 1,
            unpublished: 0,
            tail,
            closed,
            anchor: ProducerAnchor::Shm(anchor),
        }
    }
}

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
impl<T, C: WaitStrategy> Consumer<T, C> {
    /// Build a consumer over a (typically read-only) mapping of a validated
    /// shm region: pure reader state — no lease, no registration, nothing
    /// written, ever. The join point is the tail at this call.
    ///
    /// # Safety
    ///
    /// The region must be a validated broadcast element ring of exactly this
    /// `T` and `capacity`, with `slack` the region's create-time slack
    /// (validated `< capacity`).
    pub(crate) unsafe fn from_shm(
        region: Arc<crate::shm::ShmRegion>,
        capacity: usize,
        slack: u64,
    ) -> Self {
        let tail = NonNull::from(region.bcast_tail());
        let closed = NonNull::from(region.bcast_closed());
        let buf = region.bcast_elem_buffer().cast::<Slot<T>>();
        // SAFETY: `tail` references the live mapping (per contract).
        let pos = unsafe { tail.as_ref() }.load(Ordering::Acquire);
        Consumer {
            buf,
            mask: capacity as u64 - 1,
            slack,
            pos,
            tail_cache: pos,
            wait: C::default(),
            tail,
            closed,
            anchor: ConsumerAnchor::Shm(region),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reposition_target_math() {
        // The §3.2 shape: tail - capacity + slack.
        assert_eq!(reposition_target(20, 8, 2), 14);
        assert_eq!(reposition_target(17, 8, 0), 9);
        // A stale tail observation clamps at zero instead of wrapping.
        assert_eq!(reposition_target(1, 8, 2), 0);
        assert_eq!(reposition_target(0, 8, 0), 0);
        // u64 boundary: saturates instead of wrapping (unreachable in ~29
        // years of pushing, but the arithmetic must stay sane) [M-F13].
        assert_eq!(reposition_target(u64::MAX, 8, 2), u64::MAX - 8);
    }

    #[test]
    fn default_slack_policy() {
        assert_eq!(default_slack(1), 0);
        assert_eq!(default_slack(2), 1);
        assert_eq!(default_slack(8), 1);
        assert_eq!(default_slack(16), 2);
        assert_eq!(default_slack(1024), 128);
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    fn data_offset_of<T>() -> usize {
        let s = Slot::<T> {
            seq: AtomicU64::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        };
        let slot_ptr = &s as *const _ as usize;
        let data_ptr = s.data.get() as usize;
        data_ptr - slot_ptr
    }

    #[test]
    fn u8_data_aligned() {
        assert_eq!(data_offset_of::<u8>() % std::mem::align_of::<usize>(), 0);
    }
    #[test]
    fn u16_data_aligned() {
        assert_eq!(data_offset_of::<u16>() % std::mem::align_of::<usize>(), 0);
    }
    #[test]
    fn u32_data_aligned() {
        assert_eq!(data_offset_of::<u32>() % std::mem::align_of::<usize>(), 0);
    }
    #[test]
    fn u128_data_aligned() {
        assert_eq!(data_offset_of::<u128>() % std::mem::align_of::<usize>(), 0);
    }
    #[test]
    fn arr3_u8_data_aligned() {
        assert_eq!(
            data_offset_of::<[u8; 3]>() % std::mem::align_of::<usize>(),
            0
        );
    }
}
