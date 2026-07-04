//! High-performance **single-producer / single-consumer** ring buffers.
//!
//! A Rust port of the SPSC queue from
//! [`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), preserving the
//! design choices that make it fast — monotonic masked indices, per-side
//! caching of the other side's cursor, cache-line padding to avoid false
//! sharing, and compile-time-selectable wait strategies — and adding adaptive
//! read-cursor publishing and zero-copy in-place access on top.
//!
//! # Quick start
//!
//! ```
//! use rust_rb::RingBuffer;
//!
//! // Capacity is chosen at runtime, rounded up to a power of two (1024 here).
//! let (mut tx, mut rx) = RingBuffer::new(1000);
//!
//! tx.push(42u64);
//! assert_eq!(rx.pop(), 42);
//! ```
//!
//! Pick wait strategies explicitly (the default is [`YieldWait`] on both
//! sides, matching the C++ template defaults):
//!
//! ```
//! use rust_rb::{RingBuffer, PauseWait};
//!
//! let (mut tx, mut rx) =
//!     RingBuffer::<i32, PauseWait, PauseWait>::with_wait_strategies(4096);
//! assert!(tx.try_push(1).is_ok());
//! assert_eq!(rx.try_pop(), Some(1));
//! assert_eq!(rx.try_pop(), None);
//! ```
//!
//! Zero-copy in-place access on both sides:
//!
//! ```
//! use rust_rb::RingBuffer;
//!
//! let (mut tx, mut rx) = RingBuffer::new(16);
//!
//! // Construct directly in the buffer instead of moving a value in.
//! let mut slot = tx.claim();
//! slot.uninit().write([0u8; 64]);
//! unsafe { slot.commit_init() };
//!
//! // Read in place; the slot is released when the guard drops.
//! let msg = rx.pop_ref();
//! assert_eq!(msg[0], 0);
//! drop(msg);
//! ```
//!
//! The producer and consumer move to their respective threads; the buffer
//! lives in a shared [`std::sync::Arc`] and is freed when both halves drop.
//!
//! # Variable-size messages
//!
//! When the payload is not one fixed type — serialized structs, wire frames,
//! log records of differing lengths — use [`BytesRingBuffer`], a framed byte
//! ring with the same design and zero-copy reads and writes:
//!
//! ```
//! use rust_rb::BytesRingBuffer;
//!
//! // Capacity is in bytes, rounded up to the next power of two.
//! let (mut tx, mut rx) = BytesRingBuffer::new(4096);
//!
//! tx.push(b"tick");
//! tx.push(b"a longer message");
//! assert_eq!(&*rx.pop(), b"tick");
//! assert_eq!(&*rx.pop(), b"a longer message");
//! ```
//!
//! [`YieldWait`]: wait::YieldWait

#![deny(unsafe_op_in_unsafe_fn)]

mod cache_padded;
mod cursor;

#[cfg(all(feature = "shm", target_os = "linux"))]
pub mod shm;
pub mod spsc;
pub mod spsc_bytes;
pub mod wait;

#[doc(inline)]
pub use spsc::{Consumer, Producer, RingBuffer};
#[doc(inline)]
pub use spsc_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer};
#[doc(inline)]
pub use wait::{CrossProcess, CvWait, NoOpWait, PauseWait, WaitStrategy, YieldWait};

#[cfg(all(feature = "shm", target_os = "linux"))]
#[doc(inline)]
pub use shm::{memfd, ShmItem};
