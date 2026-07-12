# ADR 0001: SPMC broadcast ships as two separate machines; distribution semantics deferred to routing

Date: 2026-07-05 · Status: accepted · Tracked: rust-rb-owp

## Context

SPMC has several distinct semantics: broadcast-lossless (slowest consumer
blocks the producer), broadcast-lossy (producer never blocks, slow consumers
lose), and distribution (competing consumers / keyed sharding / load
balancing, each message to exactly one consumer). Research across LMAX
Disruptor, Agrona/Aeron, Seastar, DPDK, and the Rust ecosystem
(2026-07-05 session, notes on rust-rb-owp) established:

- The two broadcast semantics require different slot layouts (per-slot
  sequence word vs none), different element bounds (`T: Send` vs
  `T: NoUninit`), and different read protocols (borrow vs
  copy-out-validate).
- Exactly-once distribution on one shared ring costs ≥1 shared RMW per
  message (~5–10× the crate's 1.15 ns/op SPSC baseline uncontended; DPDK
  measured 8→983 cyc/obj under contention). LMAX shipped this (`WorkerPool`)
  and removed it in Disruptor 4.0 as "not a good fit for the underlying
  technology".

## Decision

1. Ship two types: `spmc::RingBuffer` (gating, Disruptor-multicast-style)
   and `broadcast::RingBuffer` (lossy, seqlock/Aeron-style). Two machines,
   no shared mode flag, no hot-path mode branch.
2. Do not build a competing-consumer ring. Distribution semantics, when
   pursued, are a thin router over N existing SPSC rings; round-robin
   partitioning is documented as the `sequence % N == ordinal` consumer-side
   filter idiom on the gating ring.

## Consequences

- Each machine's contract is honest and independently optimal; users pick by
  semantics, not by flags.
- The crate keeps the single-writer principle everywhere — no CAS/RMW on any
  hot path, preserving the performance brand.
- Full design: `docs/design/spmc.md` (audit-hardened, three adversarial
  audits folded).
