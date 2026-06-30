//! High-performance **single-producer / single-consumer** ring buffer.
//!
//! A faithful Rust port of the SPSC queue from
//! [`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), preserving the
//! design choices that make it fast: monotonic masked indices, per-side caching
//! of the other side's cursor, cache-line padding to avoid false sharing, and
//! compile-time-selectable wait strategies.
//!
//! # Quick start
//!
//! ```
//! use rust_rb::spsc::Spsc;
//!
//! // Capacity is rounded up to the next power of two (1024 here).
//! let (mut tx, mut rx) = Spsc::<u64, 1000>::new();
//!
//! tx.put(42);
//! assert_eq!(rx.get(), 42);
//! ```
//!
//! Pick wait strategies explicitly (defaults are [`YieldWait`] for both sides,
//! matching the C++ template defaults):
//!
//! ```
//! use rust_rb::spsc::Spsc;
//! use rust_rb::wait::PauseWait;
//!
//! let (mut tx, mut rx) = Spsc::<i32, 4096, PauseWait, PauseWait>::new();
//! assert!(tx.try_put(1).is_ok());
//! assert_eq!(rx.try_get(), Some(1));
//! assert_eq!(rx.try_get(), None);
//! ```
//!
//! The producer and consumer move to their respective threads; the buffer lives
//! in a shared [`std::sync::Arc`] and is freed when both halves drop.
//!
//! [`YieldWait`]: wait::YieldWait

#![deny(unsafe_op_in_unsafe_fn)]

mod cache_padded;

pub mod spsc;
pub mod wait;

#[doc(inline)]
pub use spsc::{Consumer, Producer, Spsc};
#[doc(inline)]
pub use wait::{CvWait, NoOpWait, PauseWait, WaitStrategy, YieldWait};
