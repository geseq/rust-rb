//! Cache-line padding to prevent false sharing.
//!
//! Mirrors `alignas(hardware_destructive_interference_size)` from the C++
//! original. The destructive interference size is 64 bytes on the x86-64 and
//! AArch64 targets the original was tuned for, so we align (and pad) to 64.

/// Wraps a value so that it occupies its own cache line(s).
///
/// Two `CachePadded` values are guaranteed to live on distinct cache lines,
/// which keeps a producer's and a consumer's hot fields from ping-ponging the
/// same line between cores (false sharing) — the single biggest win in the
/// original design after index caching.
#[derive(Default)]
#[repr(align(64))]
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
