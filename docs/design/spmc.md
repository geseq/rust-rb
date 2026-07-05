# SPMC design: `spmc::RingBuffer` (gating) and `broadcast::RingBuffer` (lossy)

_Status: REVISED after adversarial audits #1 (memory model) and #2 (performance);
API/lifecycle audit in flight — sections marked **[PENDING-AUDIT-3]**. Tracked
as `rust-rb-owp`. **[DECIDED]** = settled in the 2026-07-05 grilling session;
**[OPEN]** = final grill round. Audit finding refs: M-Fn (memory), P-Fn (perf)._

## 1. Scope

Two single-producer / multi-consumer **broadcast** rings; every consumer
observes messages independently:

| Type | Semantics | Slow consumer | Producer |
| --- | --- | --- | --- |
| `rust_rb::spmc::RingBuffer` | lossless multicast (Disruptor-style) | **blocks the producer** (gating) | gates on `min(consumer cursors)` |
| `rust_rb::broadcast::RingBuffer` | lossy broadcast (seqlock/Aeron-style) | **loses messages**, detects loss | never blocks, never reads consumer state |

**[DECIDED]** Naming: `spmc::` gating, `broadcast::` lossy. Scope: element AND
bytes, heap AND shm. Membership: dynamic. Gating `T: Send`
(drop-on-overwrite); lossy `T: Copy` — tightened to **`T: NoUninit`** by
M-F12. Both x86-64 and aarch64 first-class; strict (formally sound) copy
first, relaxation only by A/B bench (both sides — M-F10 extends it to the
producer).

**[DECIDED]** Deferred: distribution semantics (competing consumers, keyed
sharding, load balancing) — different perf profiles, revisit per-semantics
after these machines are benchmarked. Research verdict on record (rust-rb-owp):

- **No competing-consumer ring type.** Claim floor = 1 shared RMW/msg
  (~5–10× the 1.15 ns baseline uncontended; DPDK measured 8→983 cyc/obj
  contended). LMAX shipped `WorkerPool`, never optimized it, removed it in
  4.0. moodycamel's MPMC is internally per-producer sub-queues.
- **Sharding/load-balance = thin router over N SPSC rings** (Seastar/DPDK/
  FastFlow/exchange-sequencer school). Producer pays hash + uncontended SPSC
  push.
- **Round-robin is free once `spmc::` exists**: `sequence % N == ordinal`
  consumer-side filter (the official Disruptor 4.x idiom). Document, don't
  build.

The two machines are **different machines, not one type with a mode flag**:
different slot layouts, element bounds, read protocols. No hot-path mode
branch.

## 2. `spmc::RingBuffer` — the gating machine

### 2.1 Shared state

```
CachePadded write_cursor           AtomicUsize  (written: producer only)
CachePadded active_bitmap          AtomicU64    (written: subscribe/detach, cold)   [P-F2]
CachePadded consumer_slots[MAX]    AtomicUsize× (each written by exactly ONE consumer)
             DETACHED = usize::MAX sentinel (correctness backstop under the bitmap)
buffer      [Slot<T>; capacity]                 (written: producer only)
```

Single-writer everywhere; every cross-thread word `CachePadded` (128 B).
`MAX_CONSUMERS`: constructor parameter, **default 8** (not 64 — the shm
header pays the table; the bitmap word caps MAX at 64) **[OPEN: default]**.

### 2.2 Producer — gate, scan, publish

Producer-local plain fields (padded): `next_seq`, `cached_min`, and a
**per-slot cached cursor array** `cached_cursor[MAX]` [P-F1/P-F3].

- Fast path: `wrap_point = next_seq + needed - capacity;
  wrap_point <= cached_min` → zero shared loads. Instruction-identical to the
  SPSC check for `!needs_drop::<T>` (drop-on-overwrite adds a const-folded
  branch otherwise).
- Gate failure (slow path):
  1. `fence(SeqCst)` (Disruptor `setVolatile` analog — pairs with the
     joiner fence, M-F2, and bounds staleness vs parked consumers M-F4).
  2. Load `active_bitmap` (Relaxed; L1-resident — written only on membership
     change).
  3. **Selective refresh** [P-F3]: for each set bit whose
     `cached_cursor[i] < wrap_point`, reload slot i (Relaxed); one
     `fence(Acquire)` after the loop [P-F1] — misses overlap in the MLP
     window instead of serializing (LDAR would). Slots already known past
     the wrap point are NOT reloaded — monotonicity makes cached values
     permanent lower bounds. Straggler regime ⇒ exactly 1 line polled = the
     SPSC shape.
  4. `cached_min = min(over set bits, DETACHED skipped as backstop)`.
     **Empty active set ⇒ `cached_min = next_seq - 1` (own published
     cursor), NEVER unbounded** [M-F1: an unbounded cache disables the only
     rescan trigger and makes joiners invisible for unbounded laps — UAF].
     Own-cursor keeps an audience-less producer unblocked (`wrap_point ≤
     published` always) while forcing ≥1 rescan per lap.
- Publish: `Release` store of `write_cursor` + notify (unchanged).
- **Honest cost model** [P-F1]: saturated, the min advances in
  `publish_batch` (= min(capacity/8, 64)) steps, so rescans amortize per-64
  pushes, not per-capacity:
  `push_ns ≈ 1.15 + N_blocking × line_transfer / publish_batch`.
  With selective refresh, `N_blocking` is the number of consumers actually
  near the wrap point (typically 1), not N. Bench gate: p50/p99/p99.9/max
  push latency at N∈{1,4,8,16}, ring pinned full (§5).

`push`/`try_push`/`claim`/`try_claim`/`WriteSlot::{commit, commit_init,
uninit}` keep their SPSC shapes.

### 2.3 Consumers

Hot path = SPSC consumer: private cursor + cached `write_cursor`, reload on
empty-looking (Acquire), **adaptive publish of own cursor verbatim**
(deferral bound does NOT compound across N: the min inherits ONE consumer's
deferral, ≤ capacity/8 max 64 — same producer-visible bound as SPSC
[M-F9/P-F3]).

- Reads are **`&T` borrows**: `pop_ref()`/`try_pop_ref()`. SPMC `PopRef`
  differs from SPSC's: **no `DerefMut`** (concurrent `&T` readers on the
  same slot), **advance-only drop** (never `drop_in_place` — the value stays
  live for other consumers) [M-F7].
- `pop()`/`try_pop()` where `T: Clone`: clone out + advance.
- `mem::forget(PopRef)` = **redelivery** (cursor never advanced; next pop
  re-reads the same seq — mirrors SPSC) [M-F6]. New consequence to document:
  the un-advanced cursor also gates the producer globally, so
  forget-then-idle stalls the ring — the gating contract, not a leak.
- `drain(|&T| ...)` ships for **API parity and deterministic publish
  granularity** (bounded shm redelivery window) — NOT as a throughput
  lever: adaptive publish already elides per-element publishes; the explicit
  batch saves at most one Release store to an unpolled line per 64 elements
  [P-F6].

### 2.4 Dynamic membership **[DECIDED: dynamic]**

- `subscribe()` (cold) — the **Disruptor `addSequences` choreography**
  [M-F2; the naive CAS-once protocol is formally broken — store-buffering
  lets the producer miss the joiner while the joiner reads a stale cursor]:
  1. CAS a DETACHED slot → provisional `write_cursor.load(Acquire)`.
  2. `fence(SeqCst)` (pairs with the producer's pre-scan fence).
  3. Re-read `write_cursor` (Acquire); final `slot.store(w', Release)`.
  4. `active_bitmap.fetch_or(bit)` (cold RMW).
  **The join point is the re-read**: delivery contract = "messages published
  after `w'`".
- Detach (consumer `Drop`): `slot.store(DETACHED, Release)`;
  `active_bitmap.fetch_and(!bit)`. Mandatory (a dropped-attached consumer
  gates forever). Mid-scan detach is safe: a DETACHED slot imposes no
  constraint — the departing consumer's borrows are dead by lifetime rules
  before Drop runs [M-F3]. Re-subscribe of the same slot goes through the
  full choreography (no stale low re-init possible).
- Registry full ⇒ `subscribe()` errors **[PENDING-AUDIT-3: error type +
  subscribe-during-teardown race]**.

### 2.5 `T: Send`, producer-drops-on-overwrite **[DECIDED]**

- Consumers never move values out; the producer drops the old occupant when
  overwriting. The drop happens **at claim time** (before `uninit()` is
  handed out).
- **`dropped_through` watermark** (producer-local), advanced *before*
  `drop_in_place` of the old occupant [M-F5: without it, panic-in-drop or an
  abandoned `claim` double-drops — the unwound `push`'s occupant is still
  inside the naive teardown window]. Push-retry and re-claim consult the
  watermark and skip the drop. Subsumes the first-lap check (starts at 0).
- Teardown (last handle): drop
  `[max(next_seq - capacity, dropped_through_boundary), next_seq)` via
  `SlotCleanup` **[PENDING-AUDIT-3: ownership graph, who is "last" with
  dynamic membership]**.
- Panic in the old value's drop: propagates out of `push`/`claim`; the
  watermark keeps state consistent; document in the semantics guide.
- shm: `T: ShmItem` ⇒ no Drop ⇒ this machinery compiles away.

### 2.6 Wait strategies

- Consumer waits on `write_cursor`: existing machinery.
- Producer waits on min-cursor movement. **v1: spin-only, both sides
  [OPEN: confirm]** — with a CvWait producer, every consumer flush pays
  lock+signal at peak backpressure (N-contended mutex exactly when it
  hurts); a shared consumer-side CvWait has a real lost-wakeup defect for
  N waiters (`waiting: AtomicBool` — A wakes, clears the flag, parked B is
  skipped; saved only by the 100 ns timed recheck) [M-F4/P-F7]. If blocking
  ever ships: per-consumer wait words + targeted wake + waiter *counter*.
- Producer wait predicate must **re-scan the min inside the wait loop**
  (a cached min in the predicate is a deadlock) [M-F4].

### 2.7 Bytes variant (`spmc::BytesRingBuffer`)

SPSC framing (u32 LE header, padding records); per-consumer frame parsing;
gating min in bytes. Starving flag [AUDIT resolved, M-F8]: keep the
producer-owned flag, but consumers react only through a **consumer-local lag
filter** (`write_cache − read_cursor ≥ capacity − max_record`) — only the
actual gate can observe that; without the filter, one starvation episode
collapses all N consumers into publish-per-message (the ~20× regime the
element ring's local check avoids).

### 2.8 shm variant

- Header: consumer-cursor table (MAX fixed at create) + per-slot leases +
  `active_bitmap`. New header kinds + version bump
  **[PENDING-AUDIT-3: exact geometry]**.
- Crash story: dead attached consumer gates the producer until
  `force_detach_consumer(slot)` (unsafe; caller asserts death). The
  **zombie problem** — a force-detached-but-alive consumer keeps
  Release-storing its cursor into the slot — needs a mechanism (slot epoch /
  per-flush lease check / documented cooperative-only)
  **[PENDING-AUDIT-3: the hard one]**.
- Recovered producer must rebuild `cached_min` by a scan — never trust a
  default [M-F17].
- Only `CrossProcess` strategies.

### 2.9 Performance targets (post-audit, honest)

- **Un-saturated + consumers keeping up**: parity with SPSC (~1.15 ns/op)
  for `!needs_drop` T — the fast path is instruction-identical.
- **Saturated**: `≈ 1.15 + N_blocking × transfer/64` ns/push; with selective
  refresh N_blocking ≈ 1 (straggler regime) → target ≤ 2× SPSC at N=8.
  Without the P-F1/2/3 triple this is ~5 ns/push and ~400 ns tails — the
  triple is **mandatory, not optional**.
- Payload lines crossing to N cores is physics; the design adds ~zero
  protocol traffic on top (verify via `perf c2c`/snoop counters, §5).

## 3. `broadcast::RingBuffer` — the lossy machine

### 3.1 Slot protocol (REVISED per M-F10/M-F11/P-F4)

Slot: `#[repr(C)] { seq: AtomicU64, payload: [AtomicUsize; ceil] }` — seq at
offset 0, payload 8-aligned; **payload accessed word-wise atomically on BOTH
sides** (tail bytes byte-wise). `seq` = `2·global_seq + phase`.

```
producer, writing message s into slot (s & mask):
  seq.store(2s+1, Relaxed)          // invalidate (Relaxed suffices — the
  fence(Release)                     //  fence does the ordering) [M-F10]
  payload: word-wise Relaxed ATOMIC stores    // plain stores are UB against
                                     //  the reader's atomic copy AND
                                     //  compiler-hoistable above the fence
  seq.store(2s+2, Release)          // publish slot
  tail.store(s, Release)            // publish frontier — PER PUSH [P-F4]

consumer at position s (hot path):
  spin on tail (Acquire) until tail >= s      // NOT on the slot seq!
  v1 = slot.seq.load(Acquire)                 // expect 2s+2
  copy payload out (word-wise Relaxed loads)
  fence(Acquire)
  v2 = slot.seq.load(Relaxed)                 // fence + relaxed re-load is
  accept iff v1 == v2 == 2s+2; else Lagged    //  the sound shape; an Acquire
                                              //  re-load would NOT be [M-F11]
```

- **Consumers spin on `tail`, not the frontier slot's seq** [P-F4 — the
  killed claim]: frontier-slot spinning puts k spinners on the very line the
  producer stores 2–3× per message — per-element stores to a polled line is
  the exact pathology adaptive publish removed for the 135M→860M win,
  reinstated by construction with no batching escape. Tail-spin collapses it
  to one cursor line written once per push — **the SPSC caught-up profile**,
  which is the 1.15 ns path. Slot seqs become validate-only (one fetch, no
  spin residency).
- `tail` is therefore **load-bearing and per-push** (also required by
  subscribe/lag [M-F14]; per-push makes `Lagged(n)` exact for free
  **[PENDING-AUDIT-3: final contract]**). Release-stored after the slot
  publish; holds ≤ highest published seq.
- ABA: sound — slot series `2s+1, 2s+2, 2(s+cap)+1, …` is strictly
  increasing; exact-match accept is generation-unique; u64 wraps in ~29
  years at 10 G msg/s [M-F13].
- Producer cost (honest, replaces "strictly cheaper than SPSC" — **deleted**
  [P-F4]): 2 seq stores + word-wise payload + tail store + one
  `fence(Release)` (DMB ISH on aarch64) vs SPSC's 1 Release store. Target:
  **≈ SPSC caught-up profile**; throughput independence vs k is the bench
  gate, now achievable because spinners share the tail line only.

### 3.2 Loss semantics **[OPEN — final grill]**

Lap ⇒ `Err(Lagged { missed })` (exact — per-push tail). Reposition:
**jump-to-oldest-retained + slack** (slack bounds the lag-storm where a slow
reader re-laps immediately; proposal: capacity/8)
**[PENDING-AUDIT-3: storm bound + position-after-error]**; `skip_to_latest()`
as the explicit market-data alternative. Proposed naming:
`pop() -> Result<T, Lagged>`, `try_pop() -> Result<Option<T>, Lagged>`
**[OPEN]**.

### 3.3 The strict copy **[DECIDED, extended by audits]**

- Word-wise relaxed atomic copy on **both sides** (M-F10 made the producer
  side mandatory, not optional). Tail bytes byte-wise atomic.
- **`T: NoUninit`** (bytemuck-style: no padding bytes, no uninit niches)
  [M-F12: word-wise atomic loads over padding bytes are UB even
  single-threaded; bare `Copy` is insufficient]. Marginal practical cost
  over `Copy`; keeps the engine Miri-clean.
- The `read_volatile` variant stays behind a dev switch for the A/B — which
  now covers **push and pop paths both**, payload ∈ {8,16,64,256,1024} B ×
  {strict, volatile} × {idle, saturating} [P-F5: vectorization loss —
  atomic-per-word forbids NEON/AVX; crossover where copy dominates pop ≈
  64–128 B; decision rule written before the bench runs, e.g. "volatile
  ships per-arch if strict >25% slower at 64 B"].

### 3.4 Membership: trivially dynamic

Consumer = pure reader state; `subscribe()` = read `tail`, start there
(joiner replay/`lag()` under-report bounded by tail staleness — zero with
per-push tail). No registry, no leases, unbounded count, no-op Drop.

### 3.5 Wait strategies

Producer never waits (no P strategy — signature question
**[PENDING-AUDIT-3]**). Consumers: **spin-only, forced** — a parked consumer
would need producer notifies, violating the zero-consumer-knowledge design.
Spinning targets the shared `tail` line (§3.1).

### 3.6 Bytes variant — **Agrona three-counter design** [M-F15 resolved the fork]

Per-record seqs are **structurally unsound** for variable-size records:
record boundaries shift across laps, so an in-band "seq" word can be another
message's payload forging the expected value. Adopt Agrona:

- Counters: `tail_intent` (invalidate-first), `tail` (commit), `latest`
  (jump target), all u64, producer-only writers.
- Validation = **out-of-band window check**: `(cursor + capacity) >
  tail_intent`, checked before parse AND re-checked after copy.
- Torn-field inventory: `length` read as aligned relaxed AtomicU32 →
  bounds-checked (`0 < len ≤ max ∧ fits`) before ANY use; type/padding
  marker dispatched only after the window check; payload garbage tolerated
  (copied out bounded by validated length, then window re-check).
- `max_message_len = capacity/8` (Aeron) — the post-copy window must
  tolerate producer progress during the copy; `capacity/2` would halve the
  tolerance. Differs from SPSC bytes (`capacity/2 − 4`) because loss
  tolerance, not framing, binds here.
- Lap ⇒ jump to `latest` (guaranteed-valid record start — also what repairs
  boundary misalignment, which per-record seqs cannot).

### 3.7 shm variant — read-only consumers

Consumer path audited write-free (loads + local state only) [P-F8]:
attach with **`PROT_READ`** mapping. Requires a **lease-free consumer attach
variant** (the current attach path writes a lease — spec this; consumers
take no lease at all, matching §3.4). Producer recovery = SPSC story; a
recovered producer re-initializing slots mid-read self-heals via validation.
**Enforcement is free: run the whole lossy-consumer test suite against a
read-only mapping — any accidental store is a deterministic SIGSEGV** (§5).

### 3.8 Performance targets (post-audit, honest)

- Producer: ≈ SPSC caught-up profile (per-push store to one k-shared line);
  independence vs k is the gate — measured with **caught-up, pinned,
  spinning consumers** (lagging consumers hide the effect entirely) [P-F9].
- Consumer: SPSC pop + one validate load + fence + copy-out; strict-copy
  delta is the A/B.
- Loss statistically negligible at sane capacities; capacity = lag-tolerance
  knob.

## 4. Engine reuse

Gating: `cursor.rs` generalizes (cached peer cursor → per-slot cached
cursors + bitmap; `ConsumerCore` nearly as-is; `SlotCleanup`, `AnchorKind`,
wait plumbing, rounding reused) — likely a `cursor::multi` sibling.
Inheritance hazards to re-derive, not assume [M-F17]: `Shared::drop`'s walk
(watermark-adjusted window, not any single cursor), shm lease guard
per-table-slot (not per-role), recovered producer rebuilds `cached_min` by
scan, `CvWait` waiter-counter before any blocking option ships.
Lossy: shares `CachePadded`, rounding, shm region scaffolding only.

## 5. Verification plan (extended per audits)

- **loom**: gating publish/gate/subscribe/detach interleavings (finds M-F2
  if the choreography regresses); lossy write-bracket vs read-validate
  (finds M-F10) — capacity 2–4.
- **Miri**: all new unsafe; the NoUninit bound keeps the lossy engine clean
  (finds M-F12).
- **Panic injection**: panicking `T::drop`/`T::clone`/closures at every
  user-code call point; assert no double-drop (finds M-F5) and documented
  post-panic state.
- **Fuzz**: lossy reader under random producer pacing; accepted values ∈
  published set; `Lagged` counts exact.
- **PROT_READ suite**: lossy shm consumer tests run against a read-only
  mapping — accidental writes SIGSEGV deterministically [P-F8].
- **Bench** (the P-F9 nine, gates per PR):
  1. SPSC-parity N=1 (gating); 2. N∈{2,4,8,16} scaling with **pre-registered
  curve shapes** (gating: `a + b·N_blocking/64`; lossy consumer: flat);
  3. **straggler** (N−1 fast + 1 rate-limited) — flat with selective
  refresh, ~1/N without; 4. pinned-full saturation AND 50%-occupancy (drain
  benches fake parity); 5. push-latency p50/p99/p99.9/max (tail phenomenon);
  6. MAX_CONSUMERS sensitivity {4,64} at 2 active (flat after bitmap);
  7. **lossy with caught-up spinning pinned consumers k∈{1,2,4,8}** — THE
  bench (lagging consumers hide P-F4); 8. lap-storm (permanently-slow
  readers: reposition cost, producer degradation); 9. membership churn under
  load (~1 ms subscribe/detach cycle); plus copy A/B (§3.3) and
  `perf c2c`/`l2d_cache_refill` on Grace at N=8 for the "zero protocol
  traffic" claim.
- **Adversarial audits** per implementation PR, as for the SPSC rounds.

## 6. Implementation phasing

1. **PR-1** `spmc` element heap — with the P-F1/2/3 triple (bitmap,
   selective refresh, relaxed-scan+fence) and M-F1/2/5 fixes from day one;
   loom + panic-injection + benches 1–6.
2. **PR-2** `broadcast` element heap — tail-spin protocol, symmetric strict
   copy, NoUninit; copy A/B decides the copy story; benches 7–8 + fuzz.
3. **PR-3** bytes variants — spmc bytes (lag-filtered starving flag);
   broadcast bytes (Agrona three-counter).
4. **PR-4** shm — gating consumer table + zombie answer; broadcast
   read-only lease-free attach + PROT_READ suite; churn bench.

## 7. Open questions (final grill)

1. `MAX_CONSUMERS` default (proposal: 8; hard cap 64 = bitmap word).
2. Lossy reposition: oldest+slack (slack = capacity/8?) default +
   `skip_to_latest()` — confirm. `Lagged` exact (free with per-push tail).
3. Lossy naming: `pop() -> Result<T, Lagged>` vs `recv`-family.
4. v1 spin-only end-to-end (gating producer side + all lossy) — confirm.
5. Channel-closed semantics — **[PENDING-AUDIT-3]** proposal to review.
6. Gating-shm zombie consumer mechanism — **[PENDING-AUDIT-3]** options to
   decide.
