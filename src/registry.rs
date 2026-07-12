//! The shared gating-registry engine — the multi-consumer analog of
//! [`crate::cursor`], in exactly one copy.
//!
//! Everything concurrency-critical that the four gating machines
//! ([`crate::spmc`], [`crate::spmc_bytes`], [`crate::anchored`],
//! [`crate::anchored_bytes`]) have in common lives here: the chunk-list
//! consumer registry with its bitmap, the claim/subscribe choreography
//! ([M-F2]), the gate-miss rescan walks over both backings (heap chunks and
//! the shm consumer table) with their fence discipline ([P-F1]/[P-F3]), the
//! detach protocol ([A-1.3]), the `DETACHED` sentinel and its guard, and the
//! publish-batch policy constants. The rings layer their payload semantics
//! (element slots, byte frames, seqlock brackets) on top.
//!
//! # Cursor domain: `u64`
//!
//! All gating cursors are **u64** — on 64-bit targets identical to the
//! former `usize` cursors, and on 32-bit targets immune to the 2^32
//! wraparound that made the sentinel guard and the wrapped-difference
//! comparisons load-bearing (they are kept in wrapped form anyway, so the
//! arithmetic stays the audited shape). The composed rings' slot
//! generations (`2s + 1`/`2s + 2`) require the width; the gating rings
//! inherit it so the engine exists once.
//!
//! Positions index buffers as `(cursor & mask) as usize` — sound because a
//! real capacity always fits `usize`.

use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, Ordering};

use crate::cache_padded::CachePadded;

/// Registry slot sentinel: no consumer owns this slot. A correctness
/// backstop *under* the bitmap — the producer skips a slot that reads
/// `DETACHED` even when its bitmap bit is (transiently) set.
pub(crate) const DETACHED: u64 = u64::MAX;

/// Registry chunk width: one bitmap word of consumer slots.
pub(crate) const CHUNK_SLOTS: usize = 64;

/// The element rings' clamp for the shared publish-batch policy: at most 64
/// elements of deferred, already-consumed progress per consumer (mirrors the
/// SPSC engine).
pub(crate) const MAX_PUBLISH_BATCH_ELEMS: u64 = 64;

/// The byte rings' clamp: at most 4096 bytes of deferred, already-consumed
/// progress per consumer — bounding the absolute amount of
/// freed-but-unpublished space a blocked producer can be waiting behind
/// (and, via the gate, at most *one* consumer's deferral).
pub(crate) const MAX_PUBLISH_BATCH_BYTES: u64 = 4096;

/// The deferred-publish bound for the element rings' adaptive publish:
/// `capacity / 8`, clamped to `[1, 64]` (the SPSC policy, element constant),
/// in the u64 cursor domain.
#[inline(always)]
pub(crate) const fn publish_batch_elems(capacity: u64) -> u64 {
    // A real capacity always fits usize (the buffer was allocated).
    crate::cursor::publish_batch(capacity as usize, MAX_PUBLISH_BATCH_ELEMS as usize) as u64
}

/// The byte rings' adaptive-publish bound: `capacity / 8` bytes, clamped to
/// `[1, 4096]`, in the u64 cursor domain.
#[inline(always)]
pub(crate) const fn publish_batch_bytes(capacity: u64) -> u64 {
    crate::cursor::publish_batch(capacity as usize, MAX_PUBLISH_BATCH_BYTES as usize) as u64
}

/// The wrap-safe fullness predicate: would writing `needed` more units past
/// `write` overrun a `capacity`-unit ring whose (slowest) consumer has read
/// up to `read`? The single source of truth for "gated", in the same
/// wrapped-difference form as the SPSC engine — never an absolute compare
/// (kept wrapped even though u64 cursors cannot practically wrap, so the
/// arithmetic stays the audited shape).
#[inline(always)]
pub(crate) const fn lacks_space(write: u64, needed: u64, read: u64, capacity: u64) -> bool {
    write.wrapping_add(needed).wrapping_sub(read) > capacity
}

/// A cursor value about to be stored into a registry slot must never equal
/// the `DETACHED` sentinel. Publishing one unit less is always safe: a lower
/// published cursor only gates the producer more, and the next flush
/// publishes past it. (Unreachable for u64 cursors in practice; kept for the
/// audited shape.)
#[inline(always)]
pub(crate) const fn guard_sentinel(cursor: u64) -> u64 {
    if cursor == DETACHED {
        cursor.wrapping_sub(1)
    } else {
        cursor
    }
}

/// One 64-slot block of the consumer registry.
///
/// `bitmap` marks the active slots (written only on subscribe/detach — cold;
/// L1-resident for the producer's rescans). Each cursor slot is written by
/// exactly one consumer and sits on its own padded line. `next` links the
/// append-only chunk list; chunks are never moved or freed until the shared
/// state drops, so cached chunk pointers stay valid for the ring's lifetime.
pub(crate) struct Chunk {
    pub(crate) bitmap: CachePadded<AtomicU64>,
    pub(crate) next: AtomicPtr<Chunk>,
    pub(crate) slots: [CachePadded<AtomicU64>; CHUNK_SLOTS],
}

impl Chunk {
    pub(crate) fn new() -> Self {
        Self {
            bitmap: CachePadded::new(AtomicU64::new(0)),
            next: AtomicPtr::new(std::ptr::null_mut()),
            slots: std::array::from_fn(|_| CachePadded::new(AtomicU64::new(DETACHED))),
        }
    }

    /// The registry half of the heap detach (the caller has already flushed
    /// and stored the cursor sentinel, and follows this with the producer
    /// wake [A-1.3]): clear the bitmap bit. The `AcqRel` RMW pairs with the
    /// claim path's `Acquire` bitmap load, so a subscriber that observes the
    /// bit clear has proof the detach fully completed.
    pub(crate) fn deactivate(&self, slot_idx: usize) {
        self.bitmap.fetch_and(!(1u64 << slot_idx), Ordering::AcqRel);
    }

    /// Number of active slots across the whole chunk list (a registry
    /// scan — cold; a racing subscribe/detach makes it a snapshot, not a
    /// guarantee).
    pub(crate) fn active_count(&self) -> usize {
        let mut chunk: &Chunk = self;
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

    /// Free the appended chunks (the first chunk is inline in the shared
    /// state). Teardown only: `&mut self` proves no concurrent access.
    pub(crate) fn free_appended(&mut self) {
        let mut next = *self.next.get_mut();
        while !next.is_null() {
            // SAFETY: appended chunks were created via `Box::into_raw` and
            // are unreachable now (no handle outlives the shared state).
            let chunk = unsafe { Box::from_raw(next) };
            next = chunk.next.load(Ordering::Relaxed);
        }
    }
}

/// Find (or append) a registry slot and claim it: CAS `DETACHED` → a
/// provisional read of the producer's published cursor (`join_cursor`).
///
/// Only slots whose bitmap bit is **clear** are candidates: a detaching
/// consumer stores `DETACHED` *before* clearing its bit, so observing the
/// bit clear (`Acquire`, pairing with the detacher's `AcqRel` RMW) proves
/// the detach fully completed — claiming a mid-detach slot would let the
/// departing consumer's belated bitmap clear erase the newcomer's bit and
/// un-gate it forever.
pub(crate) fn claim_registry_slot(
    registry: &Chunk,
    join_cursor: &AtomicU64,
) -> (NonNull<Chunk>, usize) {
    let mut chunk: &Chunk = registry;
    loop {
        let bitmap = chunk.bitmap.load(Ordering::Acquire);
        let mut free = !bitmap;
        while free != 0 {
            let idx = free.trailing_zeros() as usize;
            free &= free - 1;
            let provisional = guard_sentinel(join_cursor.load(Ordering::Acquire));
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

/// A freshly registered consumer slot: the coordinates the ring's consumer
/// handle keeps for its cold detach, the hot cursor-slot pointer, and the
/// join point.
pub(crate) struct JoinedSlot {
    pub(crate) chunk: NonNull<Chunk>,
    pub(crate) slot_idx: usize,
    /// The join point: only messages published after this cursor are seen.
    pub(crate) joined: u64,
    /// The sentinel-guarded image of `joined` stored into the slot.
    pub(crate) published: u64,
    /// This consumer's cursor word — the hot flush target.
    pub(crate) cursor_slot: NonNull<AtomicU64>,
}

/// Register a new consumer on a live registry — the Disruptor `addSequences`
/// choreography [M-F2]. The naive CAS-once protocol is formally broken:
/// store-buffering lets the producer's scan miss the joiner while the joiner
/// reads a stale write cursor. The `SeqCst` fence here pairs with the
/// producer's pre-scan fence, so at least one side sees the other; the
/// **join point is the post-fence re-read** of the producer's published
/// cursor.
///
/// The caller has already taken its keep-alive (`Arc` cloned *before*
/// touching the registry [A-2.2]: the new slot can never outlive the shared
/// state it points into) and checked the closed flag.
pub(crate) fn subscribe_slot(registry: &Chunk, join_cursor: &AtomicU64) -> JoinedSlot {
    // 1. Claim a free registry slot with a provisional cursor.
    let (chunk, slot_idx) = claim_registry_slot(registry, join_cursor);
    // SAFETY: chunks live until the shared state drops, and the caller
    // holds its keep-alive.
    let chunk_ref = unsafe { chunk.as_ref() };

    // 2. Activate the slot for the producer's rescans (cold RMW). This MUST
    //    precede the fence below (the d0549dc regression): the rescan
    //    observes consumers only through the bitmap, so the bit — not the
    //    slot store — is the registration the [M-F2] dichotomy is about. Set
    //    after the fence, a scan could miss the bit *while* the re-read
    //    below returns a stale cursor, and the producer would lap a consumer
    //    it never saw. The slot already holds the provisional cursor (a
    //    lower bound of the join point), so a scan that sees the bit this
    //    early only gates more.
    chunk_ref
        .bitmap
        .fetch_or(1u64 << slot_idx, Ordering::AcqRel);

    // 3. Pair with the producer's pre-scan fence [M-F2]: either that scan's
    //    bitmap load sees the bit set above, or this fence follows the
    //    scan's in the SC order and the re-read below returns a cursor at
    //    least as fresh as the scan's wrap point.
    fence(Ordering::SeqCst);

    // 4. The join point: re-read the producer's cursor and publish it as
    //    this consumer's cursor. Only messages published after `joined` are
    //    seen.
    let joined = join_cursor.load(Ordering::Acquire);
    let published = guard_sentinel(joined);
    chunk_ref.slots[slot_idx].store(published, Ordering::Release);

    JoinedSlot {
        chunk,
        slot_idx,
        joined,
        published,
        cursor_slot: NonNull::from(&*chunk_ref.slots[slot_idx]),
    }
}

/// The gate-miss rescan's fence discipline and cache policy — the one place
/// the [M-F2]/[P-F1]/[M-F1] producer-side protocol lives. `walk` is the
/// registry walk for the ring's backing ([`scan_chunk_registry`] or
/// [`scan_shm_table`]), run between the fences; it returns
/// `(any_active, max_lag)` over the active slots. Recomputes `cached_min`
/// and returns whether `needed` units are now free.
#[inline]
pub(crate) fn rescan_gate(
    next_seq: u64,
    needed: u64,
    capacity: u64,
    cached_min: &mut u64,
    walk: impl FnOnce() -> (bool, u64),
) -> bool {
    // Disruptor `setVolatile` analog: pairs with the subscriber's fence
    // [M-F2] — either this scan sees the joiner's registration, or the
    // joiner's post-fence re-read saw a cursor at least as high as
    // everything published before this fence, so its cursor cannot be
    // behind our current wrap point.
    fence(Ordering::SeqCst);
    let (any_active, max_lag) = walk();
    // One fence for the whole scan [P-F1]: everything the gating consumers
    // did before publishing the cursors read above (their last reads of the
    // slots/bytes we are about to overwrite) happens-before our writes after
    // this fence.
    fence(Ordering::Acquire);
    *cached_min = if any_active {
        // The minimum in wrapped terms: the cursor with the largest wrapped
        // distance behind `next_seq`.
        next_seq.wrapping_sub(max_lag)
    } else {
        // Empty registry: the producer's own published position MINUS ONE,
        // never anything else [M-F1, §9.6]. An unbounded cache would disable
        // the only rescan trigger and make joiners invisible for unbounded
        // laps (use-after-free / torn reads over reused storage). The `- 1`
        // is load-bearing twice over: it keeps an audience-less producer
        // free-running while forcing at least one rescan per lap (so a
        // joiner is noticed in time), and it caps a free-run grant strictly
        // below any joiner's post-fence re-read — which is what makes
        // unvalidated gated reads sound after a join (the §9.6 free-run
        // join induction). Do not "optimize" it.
        next_seq.wrapping_sub(1)
    };
    !lacks_space(next_seq, needed, *cached_min, capacity)
}

/// The chunk-list walk of the gate-miss rescan — the heap registry side of
/// the seam (the surrounding [M-F2] SeqCst and [P-F1] Acquire fences are
/// supplied by [`rescan_gate`], for both registry kinds). Returns
/// `(any_active, max_lag)` over the active slots.
pub(crate) fn scan_chunk_registry(
    registry: &Chunk,
    cached_cursors: &mut Vec<[u64; CHUNK_SLOTS]>,
    next_seq: u64,
    needed: u64,
    capacity: u64,
) -> (bool, u64) {
    let mut any_active = false;
    let mut max_lag = 0u64;
    let mut ci = 0usize;
    let mut chunk: &Chunk = registry;
    loop {
        if cached_cursors.len() == ci {
            // Fresh cache block: seed with a value that always compares
            // as gating (lag == capacity), forcing a real load before
            // first use — 0 would be wrong after cursor wraparound.
            cached_cursors.push([next_seq.wrapping_sub(capacity); CHUNK_SLOTS]);
        }
        let cache = &mut cached_cursors[ci];
        let mut bits = chunk.bitmap.load(Ordering::Relaxed);
        while bits != 0 {
            let idx = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let mut cursor = cache[idx];
            // Selective refresh [P-F3]: reload only slots whose cached
            // cursor is still behind the wrap point — monotonicity makes
            // cached values permanent lower bounds, so a slot already
            // known past the wrap point cannot be gating.
            if lacks_space(next_seq, needed, cursor, capacity) {
                // Relaxed: the single Acquire fence after the scan orders
                // the whole batch, so the cache misses overlap in the MLP
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
            let lag = next_seq.wrapping_sub(cursor);
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
    (any_active, max_lag)
}

/// The consumer-table walk of the gate-miss rescan — the shm registry side
/// of the seam (fence discipline supplied by [`rescan_gate`], exactly as for
/// [`scan_chunk_registry`]). The control word plays the bitmap's role in the
/// [M-F2] dichotomy: it is read first (Relaxed, covered by the trailing
/// Acquire fence) and non-ACTIVE slots are skipped **regardless of cursor
/// content** — FREE slots hold sentinels or a leftover cursor, RETIRED slots
/// are force-detached zombies whose words nobody may trust [A-4.1].
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
pub(crate) fn scan_shm_table<C>(
    backing: &crate::shm::GateShmProducer<C>,
    cached_cursors: &mut Vec<[u64; CHUNK_SLOTS]>,
    next_seq: u64,
    needed: u64,
    capacity: u64,
) -> (bool, u64) {
    let mut any_active = false;
    let mut max_lag = 0u64;
    for slot in 0..backing.max_slots() {
        let ci = slot / CHUNK_SLOTS;
        let idx = slot % CHUNK_SLOTS;
        if cached_cursors.len() == ci {
            // Fresh cache block, always-gating seed (see the heap walk).
            cached_cursors.push([next_seq.wrapping_sub(capacity); CHUNK_SLOTS]);
        }
        if !crate::shm::control_is_active(backing.slot_control(slot).load(Ordering::Relaxed)) {
            continue;
        }
        let cache = &mut cached_cursors[ci];
        let mut cursor = cache[idx];
        // Selective refresh [P-F3] — the P-F3 lower-bound argument holds
        // across slot reuse here too: a freed slot's leftover cursor is at
        // most the producer's published cursor at its detach, which a later
        // claimant's join point can never undercut.
        if lacks_space(next_seq, needed, cursor, capacity) {
            let fresh = backing.slot_cursor(slot).load(Ordering::Relaxed);
            if fresh == DETACHED {
                // Backstop, the shm face of the heap walk's mid-detach
                // skip: an ACTIVE slot whose joiner has not stored its
                // provisional cursor yet imposes no constraint — by the
                // [M-F2] fence dichotomy, a joiner this scan can still see
                // a sentinel for joins at or past this scan's wrap point.
                // Do not poison the cache.
                continue;
            }
            cache[idx] = fresh;
            cursor = fresh;
        }
        any_active = true;
        let lag = next_seq.wrapping_sub(cursor);
        if lag > max_lag {
            max_lag = lag;
        }
    }
    (any_active, max_lag)
}

/// The engine face of a gating consumer's cursor publish — the trait behind
/// [`FlushOnDrop`]. Implemented by each ring's consumer handle over its
/// inherent `flush_pending` (publish the private read cursor iff it moved
/// past the last published value).
pub(crate) trait FlushPending {
    fn flush_pending(&mut self);
}

/// Publish-on-exit guard for the batch consumers (`drain`): publishes the
/// wrapped consumer's deferred progress when dropped — **including an unwind
/// out of the user callback**, so an unwound drain never re-delivers
/// already-processed items to this consumer.
pub(crate) struct FlushOnDrop<'a, F: FlushPending>(pub(crate) &'a mut F);

impl<F: FlushPending> Drop for FlushOnDrop<'_, F> {
    fn drop(&mut self) {
        self.0.flush_pending();
    }
}
