//! [`Padded<T>`] — a cache-line-aligned element wrapper for flat gating
//! fan-out.

use core::ops::{Deref, DerefMut};

/// Pads and aligns `T` to a 64-byte cache line, giving every ring slot its
/// own line.
///
/// # Why
///
/// The gating rings ([`spmc`](crate::spmc), [`anchored`](crate::anchored)
/// anchors) pack elements densely: eight `i64` slots share one cache line.
/// With N caught-up consumers, every consumer copying element `s` holds the
/// very line the producer is writing at `s+1..s+7`, and producer cost grows
/// with N — measured on a GB10/X925 (same-cluster, Yield): 3.66 / 6.95 /
/// 12.65 ns/push at N=1/2/4 for `i64`, versus **3.89 / 3.90 / 4.11** for a
/// line-isolated element (`rust-rb-vio`; `examples/probe_ring_scaling.rs`
/// reproduces the comparison on any box).
///
/// Wrapping the element type in `Padded<T>` is that isolation:
/// `RingBuffer::<Padded<Order>>` instead of `RingBuffer::<Order>`. No ring
/// code changes, no hot-path arithmetic — just one line per slot.
///
/// # When
///
/// Reach for it when **fan-out scaling** matters more than footprint: the
/// ring's memory grows to `capacity × 64 B` (for small `T`), and per-line
/// batching is lost, so single-consumer throughput can dip slightly. For
/// N=1 rings, or rings where consumers lag more than they spin, plain `T`
/// is the better default. The **lossy** rings are the opposite trade —
/// isolation measured *slower* there (the packed layout amortizes seqlock
/// line transfers), and `Padded<T>` contains padding bytes, so it cannot
/// implement [`NoUninit`](crate::NoUninit) at all.
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
