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
//! # Which ring do I want?
//!
//! - **One fixed `T`** (a struct, an `i64`, a `[u8; N]`) → [`RingBuffer<T>`]. It
//!   stores `T` by value with no per-message framing overhead.
//! - **Variable-length byte payloads** (serialized messages, wire frames) →
//!   [`BytesRingBuffer`]. Each record is length-framed inside one contiguous
//!   ring.
//! - **Two processes sharing one ring** (`shm` feature, Linux) → the
//!   [`create_shm`](RingBuffer::create_shm)/[`attach_shm_producer`](RingBuffer::attach_shm_producer)
//!   constructors on either ring. See the [shared-memory guide](guide::shm_ipc).
//!
//! Both rings are **single-producer / single-consumer**: the [`Producer`] and
//! [`Consumer`] halves are `Send` but not `Clone`, so the SPSC contract is
//! enforced at compile time.
//!
//! # Module map
//!
//! - [`spsc`] — the fixed-size element ring ([`RingBuffer`], [`Producer`],
//!   [`Consumer`]).
//! - [`spsc_bytes`] — the variable-size byte ring ([`BytesRingBuffer`] and its
//!   handles).
//! - [`spmc`] — the single-producer / **multi**-consumer gating broadcast
//!   ring ([`spmc::RingBuffer`] and its handles): every consumer observes
//!   every message; a slow consumer gates the producer.
//! - [`wait`] — the [`WaitStrategy`] trait and the [`PauseWait`], [`YieldWait`],
//!   [`NoOpWait`], and [`CvWait`] implementations selected per side as type
//!   parameters `P` (producer) and `C` (consumer).
//! - [`shm`] — shared-memory backing for cross-process rings (Linux, behind the
//!   `shm` feature).
//!
//! # Guides
//!
//! The [`guide`] module holds task-oriented walkthroughs that go beyond the
//! item-by-item reference:
//!
//! - [Configuration](guide::configuration) — choosing a capacity and a wait
//!   strategy.
//! - [API usage](guide::api_usage) — which method to reach for, per use case.
//! - [Semantics & gotchas](guide::semantics) — the behaviours that surprise
//!   people (transient `len`/`is_full` over-count, `mem::forget` re-delivery,
//!   the single-P/single-C contract).
//! - [Performance tuning](guide::performance) — core pinning, the adaptive
//!   read-cursor publish, and a reproducible benchmarking recipe.
//! - [Shared memory / IPC](guide::shm_ipc) — running a ring across two
//!   processes, the trust model, and crash recovery.
//!
//! # Feature flags
//!
//! - **`shm`** (Linux only) — enables shared-memory backing constructors
//!   ([`shm`], [`RingBuffer::create_shm`], etc.) for cross-process rings. Pulls
//!   in `libc`. A no-op on non-Linux targets.
//!
//! [`YieldWait`]: wait::YieldWait
//! [`PauseWait`]: wait::PauseWait
//! [`NoOpWait`]: wait::NoOpWait
//! [`CvWait`]: wait::CvWait
//! [`WaitStrategy`]: wait::WaitStrategy
//! [`RingBuffer<T>`]: RingBuffer

#![deny(unsafe_op_in_unsafe_fn)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod cache_padded;
mod cursor;

pub mod guide;

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
pub mod shm;
pub mod spmc;
pub mod spsc;
pub mod spsc_bytes;
pub mod wait;

#[doc(inline)]
pub use spsc::{Consumer, Producer, RingBuffer};
#[doc(inline)]
pub use spsc_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer};
#[doc(inline)]
pub use wait::{
    BackoffWait, CrossProcess, CvWait, NoOpWait, PauseWait, SelfTimed, SleepWait, WaitStrategy,
    YieldWait,
};

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
#[doc(inline)]
pub use shm::{memfd, ShmItem};
