//! Cache-line padding to prevent false sharing.
//!
//! Mirrors `alignas(std::hardware_destructive_interference_size)` from the C++
//! original. The *destructive* interference size is larger than a single cache
//! line on the common server targets: x86-64 parts pull cache lines in aligned
//! pairs (the adjacent-line/spatial prefetcher) and recent ARM cores
//! (Neoverse, Apple) do the same or use 128-byte granules outright, so two
//! values 64 bytes apart can still ping-pong. We therefore pad to 128 on
//! x86-64 and AArch64, and to a plain 64-byte line elsewhere — the same choice
//! crossbeam and folly make.

/// Wraps a value so that it occupies its own cache line(s).
///
/// Two `CachePadded` values are guaranteed to live at least a destructive
/// interference distance apart, which keeps a producer's and a consumer's hot
/// fields from ping-ponging the same line (or prefetched line pair) between
/// cores — the single biggest win in the original design after index caching.
#[derive(Default)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(128)))]
#[cfg_attr(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    repr(align(64))
)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    #[inline(always)]
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> core::ops::Deref for CachePadded<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> core::ops::DerefMut for CachePadded<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
