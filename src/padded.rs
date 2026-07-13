//! [`Padded<T>`] — a cache-line-aligned element wrapper for flat gating
//! fan-out.

use core::ops::{Deref, DerefMut};

/// Pads and aligns `T` to a 64-byte cache line, giving every ring slot its
/// own line.
///
/// # Why
///
/// The [`spmc`](crate::spmc) ring packs elements densely: eight `i64`
/// slots share one cache line. With N caught-up consumers, every consumer
/// copying element `s` holds the very line the producer is writing at
/// `s+1..s+7`, and producer cost grows with N — measured on a GB10/X925
/// (same-cluster, Yield): 3.66 / 6.95 / 12.65 ns/push at N=1/2/4 for
/// `i64`, versus **3.89 / 3.90 / 4.11** for a line-isolated element
/// (`rust-rb-vio`; `examples/probe_ring_scaling.rs` reproduces the
/// comparison on any box).
///
/// Wrapping the element type in `Padded<T>` is that isolation:
/// `spmc::RingBuffer::<Padded<Order>>` instead of
/// `spmc::RingBuffer::<Order>`. No ring code changes, no hot-path
/// arithmetic — just one line per slot. It works on the heap and (for
/// `T: `[`ShmItem`](crate::ShmItem)) over shared memory — `Padded<T>` is
/// `ShmItem` whenever `T` is.
///
/// # Where it does NOT apply
///
/// Only the **spmc** ring takes it. The seqlock-based rings —
/// [`broadcast`](crate::broadcast) and **both halves of
/// [`anchored`](crate::anchored)** — require
/// [`NoUninit`](crate::NoUninit) elements, and a padded type can never be
/// `NoUninit` (its padding bytes are uninitialized by definition). That is
/// not a loss: on those rings slot isolation measured *slower* (1.7× at
/// k=1 — the packed layout amortizes seqlock line transfers), so the
/// mitigation for their coupling is
/// [`broadcast::Producer::set_tail_batch`](crate::broadcast::Producer::set_tail_batch),
/// not padding.
///
/// # When
///
/// Reach for it when **fan-out scaling** matters more than footprint: the
/// ring's memory grows to `capacity × 64 B` (for small `T`), and per-line
/// batching is lost, so single-consumer throughput can dip slightly. For
/// N=1 rings, or rings where consumers lag more than they spin, plain `T`
/// is the better default.
///
/// # Why 64 bytes (and not `CachePadded`'s 128)
///
/// The crate's internal hot-field wrapper pads to 128 on x86-64/aarch64 to
/// defeat adjacent-line prefetch pairing between two *statically adjacent
/// hot fields*. Ring slots are different traffic: the producer walks the
/// buffer sequentially and each slot is hot only transiently, and 64-byte
/// isolation measured **flat** through N=4 on the X925 grid above. If a
/// future x86-64 measurement shows prefetch pairing re-coupling padded
/// slots, revisit with `probe_ring_scaling` — the instrument exists for
/// exactly that question.
///
/// # Example
///
/// ```
/// use rust_rb::{spmc, Padded};
///
/// let (mut tx, mut rx) = spmc::RingBuffer::<Padded<u64>>::new(1024);
/// tx.push(Padded::new(7));
/// assert_eq!(*rx.pop().unwrap(), 7);
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(align(64))]
pub struct Padded<T>(T);

impl<T> Padded<T> {
    /// Wrap `value` for line-isolated storage.
    #[inline]
    pub const fn new(value: T) -> Self {
        Padded(value)
    }

    /// Unwrap back into the inner value.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for Padded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for Padded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> From<T> for Padded<T> {
    #[inline]
    fn from(value: T) -> Self {
        Padded(value)
    }
}

#[cfg(test)]
mod tests {
    use super::Padded;

    #[test]
    fn layout_isolates_a_cache_line() {
        assert_eq!(core::mem::align_of::<Padded<u64>>(), 64);
        assert_eq!(core::mem::size_of::<Padded<u64>>(), 64);
        // Larger-than-line payloads keep line alignment and round up whole.
        assert_eq!(core::mem::align_of::<Padded<[u8; 100]>>(), 64);
        assert_eq!(core::mem::size_of::<Padded<[u8; 100]>>(), 128);
    }

    #[test]
    fn wrap_and_unwrap() {
        let p = Padded::new(41u64);
        assert_eq!(*p, 41);
        let mut p = Padded::from(41u64);
        *p += 1;
        assert_eq!(p.into_inner(), 42);
    }
}
