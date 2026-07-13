# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/).

## [0.2.0] — unreleased

### Added

- **Six multi-consumer rings** alongside the SPSC pair, all single-producer,
  heap- or shared-memory-backed:
  - `spmc` / `spmc_bytes` — **gating** (lossless) multicast: every consumer
    sees every message; the slowest consumer gates the producer. Disruptor
    -style cached-min gate with selective refresh, unbounded dynamic
    membership, `Result<_, Closed>` close contract.
  - `broadcast` / `broadcast_bytes` — **lossy** broadcast: the producer
    never blocks and never reads consumer state; a lapped consumer is
    repositioned and told exactly what it missed (message count on the
    element ring, framed-byte count on the byte ring). Per-slot seqlock
    (element) / Agrona three-counter framing (bytes); shm consumers attach
    over a **read-only** mapping, lease-free.
  - `anchored` / `anchored_bytes` — **mixed**: required *anchors* get the
    gating contract while unbounded lossy *observers* tap the same stream;
    with zero anchors the producer free-runs like the lossy ring.
- **Shared-memory backings** (`shm` feature, Linux) for all eight rings:
  memfd/mmap constructors, producer leases with epoch-based zombie
  retirement, crash recovery, fork-based cross-process test suites.
- **Wait strategies**: `SleepWait` and `BackoffWait`; the `SelfTimed` marker
  gates the multi-consumer rings at the type level (`CvWait` rejected at
  compile time).
- **`Padded<T>`** — cache-line-aligned element wrapper: flattens the
  **spmc** ring's caught-up fan-out curve (adjacent-slot false sharing) at
  the cost of footprint; `ShmItem` whenever `T` is, so it works over shm.
  Not applicable to the seqlock rings (broadcast/anchored need `NoUninit`,
  and their packed layout measures faster anyway).
- **`broadcast::Producer::set_tail_batch` / `flush`** — amortized tail
  publication for spinning-reader workloads (measured 24.6 → 15.2 ns/push
  at k=1, 50.1 → 19.7 at k=4 on GB10/X925); default unchanged (exact
  per-push visibility). The batch is clamped to `min(capacity/8, slack)`
  so publication debt can never outrun the reposition headroom; note that
  batching widens *crash* loss to up to `batch - 1` accepted messages
  (only a graceful drop flushes) and shrinks post-reposition headroom to
  `slack - debt`.
- **Bench + probe suite**: per-ring benchmark examples (element, bytes,
  anchored, cross-process shm) and two diagnostic probes
  (`probe_coherence`, `probe_ring_scaling`) that split any box's numbers
  into hardware coherence floor vs ring overhead.
- Five task-oriented guides (configuration, API usage, semantics,
  performance, shared memory) built into the rustdoc.

### Fixed

- Broadcast drain livelock on a dead ring: the closed-and-drained check is
  `tail <= pos`, not `==` — a producer crash observed through stale
  counters could otherwise spin a drain forever.
- Benchmark spin-delay calibration on heterogeneous parts: rate-limit knobs
  are ns-denominated and calibrated on each consumer's own pinned core.

## [0.1.0] — 2026-07-04

Initial release: SPSC element ring (`RingBuffer<T>`) and variable-size byte
ring (`BytesRingBuffer`), ported from
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan) — monotonic masked
indices, per-side cursor caching, cache-line padding, compile-time wait
strategies (`PauseWait`, `YieldWait`, `NoOpWait`, `CvWait`) — plus adaptive
read-cursor publishing, zero-copy `claim`/`pop_ref`/`drain`, and the initial
shared-memory backing for both rings.
