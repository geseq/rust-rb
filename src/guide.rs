//! Long-form guides for using `rust-rb`.
//!
//! The API reference tells you *what* each item is; these guides tell you *how
//! to choose and use them*. Start with [`configuration`] and [`api_usage`], reach
//! for [`semantics`] when behaviour surprises you, [`performance`] when you are
//! tuning, and [`shm_ipc`] for cross-process rings.
//!
//! These modules carry no code — they exist only to host documentation.

/// **Configuration** — picking a capacity and a wait strategy.
#[doc = include_str!("../docs/guide/configuration.md")]
pub mod configuration {}

/// **API usage** — which method to call for which job.
#[doc = include_str!("../docs/guide/api_usage.md")]
pub mod api_usage {}

/// **Semantics & gotchas** — the behaviours that surprise people.
#[doc = include_str!("../docs/guide/semantics.md")]
pub mod semantics {}

/// **Performance tuning** — core pinning, adaptive publish, benchmarking.
#[doc = include_str!("../docs/guide/performance.md")]
pub mod performance {}

/// **Shared memory / IPC** — running a ring across two processes.
///
/// Gated on the `shm` feature (Linux): its examples reference the
/// shared-memory constructors, so they are only compiled as doctests when
/// that API is present.
#[cfg(all(feature = "shm", target_os = "linux", target_has_atomic = "64"))]
#[cfg_attr(docsrs, doc(cfg(feature = "shm")))]
#[doc = include_str!("../docs/guide/shm_ipc.md")]
pub mod shm_ipc {}
