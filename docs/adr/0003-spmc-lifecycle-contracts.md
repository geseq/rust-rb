# ADR 0003: SPMC lifecycle — closed contract, unbounded heap membership, retirement-based shm force-detach

Date: 2026-07-05 · Status: accepted · Tracked: rust-rb-owp

## Context

SPMC decouples producer and consumer lifecycles (unlike SPSC, where both
halves are structural). The API/lifecycle audit showed: blocking APIs can
provably hang forever on a dead peer without a closed signal; a fixed
registry contradicts the dynamic-membership decision on heap; and shm
`force_detach` of a consumer that is actually alive ("zombie") would corrupt
gating math or an innocent re-subscriber under the naive design.

## Decision

1. **Closed contract.** One `closed` flag written solely by producer `Drop`
   (flag-then-notify), read only on would-block paths. Gating
   `pop() -> Result<T, Closed>`; lossy `pop() -> Result<T, PopError>`,
   `enum PopError { Lagged { missed: u64 }, Closed }`; `Closed` only after
   drain. `subscribe() -> Result<_, SubscribeError{Closed, Full}>`.
   Zero-consumer `push` succeeds (free-run). SPSC signatures unchanged.
   In shm, `Closed` covers graceful drop only; crash detection remains
   lease/watchdog territory.
2. **Membership.** Heap: unbounded, append-only chunk list of 64-slot
   registry blocks (blocks never move; growth is cold). shm: `max_consumers`
   fixed at create — a physical constraint of a mapped layout, documented as
   such. Subscribe only via a live-handle method (Arc cloned before the
   registry CAS). Lossy consumers are pure readers: no registry at all,
   unbounded, `PROT_READ`-attachable in shm.
3. **Loss policy (lossy).** Exact `Lagged { missed }`; reposition to
   `tail − capacity + slack` with `slack` a constructor knob (default
   `capacity/8`); `skip_to_latest()` as the explicit alternative.
4. **shm zombie.** Per-consumer-slot control word `{u32 epoch | u32 state}`;
   `force_detach_consumer` retires the slot (bump epoch, RETIRED) and the
   slot is not re-issued until `recover_shm`. Producer scan skips
   non-ACTIVE slots regardless of cursor content — a live zombie's stores
   land on state nobody reads. `force_detach` is documented as revoking the
   victim's read validity (caller-asserts-death, same trust register as
   `force_attach`).
5. **Wait strategies (v1).** The self-timed family on all sides of both
   machines: `NoOpWait`, `PauseWait`, `YieldWait`, plus new `BackoffWait`
   (spin → yield → escalating sleep; Aeron `BackoffIdleStrategy` shape).
   No `CvWait` in SPMC v1 (single-flag elision has an N-waiter lost-wakeup
   defect; a blocking option requires per-consumer wait words + waiter
   counters + targeted wake).

## Consequences

- No SPMC blocking call can hang forever on a gracefully dropped peer.
- A wrong death assertion in shm burns one registry slot instead of
  corrupting the ring.
- The lossy ring's producer has no `P` type parameter (it structurally never
  waits) — types encode truths.
- Full protocol details and audit findings: `docs/design/spmc.md`.
