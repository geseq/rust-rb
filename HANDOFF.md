# rust-rb — Handoff & Next Steps

_Last updated: 2026-07-05. Branch `docs-handoff`, based on `main` @ the PR #6
merge (`facb223`)._

## Where the crate stands

`rust-rb` is a Rust port of the SPSC ring buffers from
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), extended well past the
original. It is **feature-complete and hardened** — six rounds of adversarial
multi-agent review converged to zero correctness findings, ~58 confirmed issues
fixed across the loop, Miri-clean on the reachable `unsafe`, and CI green on
Linux + macOS + MSRV (1.70) + rustdoc.

**What shipped (all merged to `main`):**

| Area | State |
| --- | --- |
| `RingBuffer<T>` — fixed-size element ring | ✅ push/try_push, pop/try_pop, zero-copy `claim`/`pop_ref` |
| `BytesRingBuffer` — variable-size message ring | ✅ framed records, zero-copy `claim`/`Msg`, `drain` |
| Shared cursor engine (`cursor.rs`) | ✅ one copy for both rings; adaptive publish; wrap-safe |
| Wait strategies | ✅ Pause / Yield / NoOp / CvWait, per-side (`P`, `C`) |
| Runtime capacity | ✅ `new(min_capacity)` / `with_wait_strategies` |
| Shared-memory / IPC (`shm` feature, Linux) | ✅ create/attach/force_attach/recover, leases, crash recovery |
| CI (fmt, clippy, tests ×2 OS, MSRV, rustdoc) | ✅ green |
| Benchmarks | ✅ `bench`, `bench_bytes`, `bench_shm` (pinned) |

**Measured (NVIDIA Grace / Neoverse V2, pinned core pair):** element ring
~830–905 M msgs/s (~1.15 ns/op); byte ring ~4.8 ns/msg at 8 B (bandwidth-bound
above that); shm same-process matches the heap ring; shm cross-process
~680–820 M msgs/s. Roughly 2× the C++ original on the same cores.

## The focus for next: user-facing documentation

The API has **zero missing-docs** and the README covers the major topics, but
the docs are **reference-style and terse** — they describe *what* each item is,
not *how to choose and use it*. A new user cannot currently go from "I have a
producer/consumer problem" to a correctly-configured ring without reading the
source. Closing that gap is the goal.

Tracked as beads epic **`rust-rb-1q3`** with eight children. Suggested order
(most user value first):

1. **`rust-rb-1q3.6` — Shared-memory / IPC guide + runnable example.**
   Highest-value gap. The shm API is powerful but entirely `unsafe` with a
   subtle trust/lease/recovery model, and there is no end-to-end two-process
   example. Ship a committed `examples/ipc_pair.rs` (parent creates, hands the
   fd to a child, child attaches) plus a doc walkthrough of the trust model,
   `CrossProcess`/`ShmItem` constraints, and the crash-recovery lifecycle
   (`force_attach_*` single-side vs `recover_shm` both; at-least-once bounds).

2. **`rust-rb-1q3.2` — Configuration guide.** The two knobs users actually turn:
   capacity (power-of-two rounding, the `capacity/8` publish window, footprint,
   burst-vs-steady sizing) and the wait-strategy **decision matrix** with
   concrete "pick X when" guidance. This is where "how do I configure it"
   lives.

3. **`rust-rb-1q3.3` — API usage guide (which method when).** A task-oriented
   map from use-case → method: `push` vs `try_push`; `pop` vs `try_pop` vs
   `pop_ref` (drain-fast by-value vs zero-copy read-in-place); `claim`/`commit`;
   `drain`. A worked snippet per pattern.

4. **`rust-rb-1q3.4` — Semantics & gotchas.** The behaviors that surprise:
   producer-side `len`/`is_full` transiently over-count under backpressure
   (consumer-side is exact); `mem::forget` on a `Msg`/`PopRef` is *re-delivery*,
   not a leak; `PopRef`/`drain` panic-safety; the single-P/single-C contract;
   wrap-around safety. Cross-link from each affected method.

5. **`rust-rb-1q3.5` — Performance tuning.** Core pinning (core-to-core topology
   dominates), the adaptive read-cursor publish and its observable effects,
   false-sharing design, and a reproducible benchmarking recipe.

6. **`rust-rb-1q3.1` — Crate-level guide (lib.rs).** Turn the quick-start crate
   doc into a real landing page: mental model, module map, feature flags,
   "which ring do I want". The hub the above link into.

7. **`rust-rb-1q3.7` — Doctested examples on every public method.** Make the
   docs executable (and CI-verified — `cargo test` runs doctests). Prioritize
   the non-obvious: `claim`/`commit_init`, `pop_ref`, `drain`, `try_*`
   full/empty behavior, shm constructors (`no_run`).

8. **`rust-rb-1q3.8` — Migration notes from cpp-fastchan.** Name mapping
   (`put`/`get` → `push`/`pop`), SPSC-only scope, type-level single-P/C
   enforcement, runtime vs compile-time capacity, the behavioral additions.

### Working notes for whoever picks this up

- **Docs only.** No API or behavior changes. If a doc pass uncovers a real
  behavior gap, file it as its own bd issue rather than fixing inline.
- **Validate the way CI does.** rustdoc was silently red for the whole shm PR
  because the lint only errors under `RUSTDOCFLAGS=-D warnings`; plain
  `cargo doc` exits 0 on warnings. Always run:
  ```
  RUSTFLAGS="-D warnings" RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
  ```
  and re-check the **actual PR CI**, not just local output.
- **Doctests are tests.** Every ```` ```rust ```` block compiles and runs under
  `cargo test`; shm/IPC examples that need a process or Linux should be
  `no_run` (or `ignore` with a note). Keep the existing runnable quick-starts
  runnable.
- **Ground claims in the benches.** Any performance numbers in docs should come
  from `examples/bench*` pinned runs, with the machine named and the
  core-topology caveat stated (numbers vary a lot by core pair).
- **Source-of-truth for behavior** already lives in the code comments —
  `cursor.rs` (adaptive publish, watermark, starving flag), `spsc_bytes.rs`
  (framing, padding, redelivery bounds), `shm.rs` (trust model, lease protocol,
  crash consistency). The doc effort is mostly *surfacing and organizing* that
  for users, not re-deriving it.

## Backlog beyond docs

- **`rust-rb-cz2` was completed** (shm backing) — closed. **`rust-rb-qtt`
  (cursor-core extraction) — closed.** Both done.
- Open non-doc ideas, none filed yet (raise as bd issues if pursued):
  - Configurable publish-batch bound (const-generic or constructor param) —
    the `/8`, max-64/4096 policy is fixed; a knob was deemed low-value but is a
    clean addition. See the "Why is publish batch a constant" discussion.
  - A cross-process futex-based `WaitStrategy` so shm rings can block (today
    only the spin strategies are `CrossProcess`; `CvWait` is process-local).
  - Windows/BSD shm support (currently Linux-only via memfd/`target_os`).

## Repo conventions (from CLAUDE.md)

- **Track everything in `bd`** (beads), not markdown TODOs. `bd ready` for
  available work, `bd update <id> --claim`, `bd close <id>`. The `.beads/`
  export is committed and travels with the branch.
- **Push before done** — work is not complete until `git push` succeeds.
