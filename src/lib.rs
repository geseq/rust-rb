//! High-performance **single-producer** ring buffers: SPSC queues and
//! single-producer / multi-consumer broadcasts (lossless and lossy).
//!
//! The SPSC core is a Rust port of the queue from
//! [`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), preserving the
//! design choices that make it fast — monotonic masked indices, per-side
//! caching of the other side's cursor, cache-line padding to avoid false
//! sharing, and compile-time-selectable wait strategies — and adding adaptive
//! read-cursor publishing and zero-copy in-place access on top. The
//! multi-consumer rings generalize the same engine.
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
//! Three questions: is the payload **one fixed `T`** (a struct, an `i64`, a
//! `[u8; N]` — stored by value, no framing) **or variable-length bytes**
//! (serialized messages, wire frames — length-framed in one contiguous
//! ring)? Is there **one consumer or many**? And with many: should a slow
//! consumer **gate the producer (lossless)** or **lose messages and get an
//! exact count (lossy)**?
//!
//! | Payload | One consumer | Many — lossless (gating) | Many — lossy |
//! | --- | --- | --- | --- |
//! | Fixed `T` | [`RingBuffer<T>`] | [`spmc::RingBuffer`] | [`broadcast::RingBuffer`] |
//! | Byte messages | [`BytesRingBuffer`] | [`spmc_bytes::BytesRingBuffer`] | [`broadcast_bytes::BytesRingBuffer`] |
//!
//! All six can span **two processes** (`shm` feature, Linux) through their
//! `create_shm`/`attach_shm_*` constructors; see the
//! [shared-memory guide](guide::shm_ipc). The
//! [API guide](guide::api_usage) has a worked snippet per ring.
//!
//! Every ring is **single-producer**: each producer half is `Send` but not
//! `Clone`, so that side of the contract is enforced at compile time. The two
//! SPSC rings also enforce the **single consumer** the same way; the four
//! multi-consumer rings instead let consumers `subscribe` dynamically. The
//! [semantics guide](guide::semantics) carries the full per-machine contract
//! matrix.
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
//! - [`spmc_bytes`] — the gating multi-consumer ring for **variable-size
//!   byte messages** ([`spmc_bytes::BytesRingBuffer`] and its handles): the
//!   SPSC byte framing with every consumer parsing frames independently;
//!   the producer gates on the slowest consumer's byte cursor.
//! - [`broadcast`] — the single-producer / multi-consumer **lossy** broadcast
//!   ring ([`broadcast::RingBuffer`] and its handles): the producer never
//!   blocks and never reads consumer state; a slow consumer loses messages
//!   and gets an exact [`Lagged`](broadcast::PopError::Lagged) count.
//! - [`broadcast_bytes`] — the lossy broadcast ring for **variable-size byte
//!   messages** ([`broadcast_bytes::BytesRingBuffer`] and its handles): the
//!   Agrona three-counter protocol with out-of-band validation; a lapped
//!   consumer repositions to the latest record and reports the loss in
//!   exact **bytes**
//!   ([`Lagged`](broadcast_bytes::PopError::Lagged)`.missed_bytes`).
//! - [`anchored`] — the **composed** multi-consumer ring
//!   ([`anchored::RingBuffer`] and its handles): required
//!   [`Anchor`](anchored::Anchor)s get the lossless gating contract while
//!   unbounded lossy [`Observer`](anchored::Observer)s tap the same stream
//!   with exact [`Lagged`](anchored::PopError::Lagged) accounting; with zero
//!   anchors the producer free-runs like the lossy broadcast.
//! - [`anchored_bytes`] — the composed ring for **variable-size byte
//!   messages** ([`anchored_bytes::BytesRingBuffer`] and its handles):
//!   required [`BytesAnchor`](anchored_bytes::BytesAnchor)s parse frames
//!   zero-copy under the lossless gating contract while unbounded lossy
//!   [`BytesObserver`](anchored_bytes::BytesObserver)s take validated
//!   copies with exact byte-count
//!   [`Lagged`](anchored_bytes::PopError::Lagged) accounting.
//! - [`wait`] — the [`WaitStrategy`] trait and the [`PauseWait`], [`YieldWait`],
//!   [`NoOpWait`], [`SleepWait`], [`BackoffWait`], and [`CvWait`]
//!   implementations selected per side as type parameters `P` (producer) and
//!   `C` (consumer) — plus the [`CrossProcess`] (shm) and [`SelfTimed`]
//!   (multi-consumer) marker traits that constrain the choice.
//! - [`shm`] — shared-memory backing for cross-process rings (Linux, behind the
//!   `shm` feature).
//!
//! # Guides
//!
//! The [`guide`] module holds task-oriented walkthroughs that go beyond the
//! item-by-item reference:
//!
//! - [Configuration](guide::configuration) — choosing a capacity, a wait
//!   strategy (including the `SelfTimed` constraint on the multi-consumer
//!   rings), and the broadcast reposition slack.
//! - [API usage](guide::api_usage) — which ring, then which method, per use
//!   case; worked snippets for all six rings.
//! - [Semantics & gotchas](guide::semantics) — the behaviours that surprise
//!   people (transient `len`/`is_full` over-count, `mem::forget` re-delivery)
//!   and the per-machine contract matrix (counters / forget / closed / panic
//!   sites across SPSC, gating, and lossy).
//! - [Performance tuning](guide::performance) — core pinning, the adaptive
//!   read-cursor publish, a reproducible benchmarking recipe, and the honest
//!   multi-consumer numbers.
//! - [Shared memory / IPC](guide::shm_ipc) — running a ring across processes,
//!   the trust model, crash recovery, the gating consumer table, and the
//!   lossy rings' read-only consumers.
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
//! [`SleepWait`]: wait::SleepWait
//! [`BackoffWait`]: wait::BackoffWait
//! [`CvWait`]: wait::CvWait
//! [`WaitStrategy`]: wait::WaitStrategy
//! [`CrossProcess`]: wait::CrossProcess
//! [`SelfTimed`]: wait::SelfTimed
//! [`RingBuffer<T>`]: RingBuffer

#![deny(unsafe_op_in_unsafe_fn)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod cache_padded;
mod cursor;

#[cfg(target_has_atomic = "64")]
pub mod anchored;
#[cfg(target_has_atomic = "64")]
pub mod anchored_bytes;
#[cfg(target_has_atomic = "64")]
pub mod broadcast;
#[cfg(target_has_atomic = "64")]
pub mod broadcast_bytes;
pub mod guide;

#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
pub mod shm;
#[cfg(target_has_atomic = "64")]
pub mod spmc;
#[cfg(target_has_atomic = "64")]
pub mod spmc_bytes;
pub mod spsc;
pub mod spsc_bytes;
pub mod wait;

#[cfg(target_has_atomic = "64")]
#[doc(inline)]
pub use broadcast::NoUninit;
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
