# ADR 0002: Lossy-read soundness — portable orderings, symmetric atomic copy, bench-gated relaxation

Date: 2026-07-05 · Status: accepted; **A/B resolved 2026-07-05 — strict copy is
permanent** · Tracked: rust-rb-owp

## Resolution (bench outcome)

The copy A/B ran on GB10/Cortex-X925 (same-core ping-pong isolating copy
codegen; `examples/bench_broadcast.rs`). The decision rule ("volatile ships
per-arch only if strict is >25% slower at 64 B") was **not met on either
side**: strict push/pop 6.72/5.87 ns at 64 B vs volatile 5.80/15.44 (pop
2.6× *slower* volatile); at 256 B volatile collapses (69 ns pop vs 18) —
LLVM cannot coalesce or vectorize volatile accesses, while the atomic word
loop optimizes. The `rust_rb_volatile_copy` dev cfg remains only as a bench
artifact; the strict word-wise atomic copy is the sole shipping
implementation.

## Context

The lossy broadcast reader copies a payload that the producer may
concurrently overwrite, validating a per-slot sequence afterwards
(seqlock pattern). That racing copy is formally undefined behaviour in the
C++/Rust memory models (Boehm, MSPC 2012; P1478). Industry practice splits:
Aeron/Rigtorp accept the race with compiler-only fences — benign on x86 TSO
only; the sound alternative is word-wise relaxed-atomic copies. rust-rb
targets both x86-64 and ARM (primary bench hardware is aarch64
Grace/Neoverse V2), so the TSO shortcut is unavailable. The memory-model
audit additionally established that the *producer's* payload stores must be
atomic too (plain stores are compiler-hoistable above the invalidate store
and racy against the reader's atomic loads), and that word-wise atomic loads
over padding bytes are UB — requiring a no-padding element bound.

## Decision

1. Memory orderings are portable: real `fence(Acquire)`/`fence(Release)`,
   never signal-fence-only shortcuts.
2. The payload copy is **word-wise relaxed-atomic on both sides** (producer
   stores and consumer loads), tail bytes byte-wise.
3. Element bound is **`T: NoUninit`** (no padding/uninit niches), not bare
   `Copy`.
4. A `read_volatile`-based variant exists behind a private dev switch for
   A/B benchmarking (both push and pop paths, payload sizes 8–1024 B, both
   architectures). Any relaxation ships only as an explicit, measured,
   per-architecture decision — never a silent default. Decision rule fixed
   before the bench runs.

## Consequences

- The lossy engine stays Miri- and loom-checkable.
- Vectorized copies (NEON/AVX) are foregone by default; the measured cost at
  each payload size decides whether a relaxed variant is ever offered.
- An accepted value is provably never torn on any supported architecture.
