# SPMC design: `spmc::RingBuffer` (gating) and `broadcast::RingBuffer` (lossy)

_Status: REVISED after all three adversarial audits — #1 memory model (M-Fn),
#2 performance (P-Fn), #3 API/lifecycle (A-n). Tracked as `rust-rb-owp`.
**[DECIDED]** = settled in the 2026-07-05 grilling session; §7 lists the
final owner decisions before implementation._

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
             + closed: AtomicBool  (same padded slot — the line consumers
                                    already poll; written ONCE by producer
                                    Drop, read only on would-block paths) [A-1.1]
             + dropped_through     AtomicUsize  (written: producer only,
                                    advanced BEFORE each overwrite-drop;
                                    teardown's lower bound) [A-2.1]
CachePadded active_bitmap          AtomicU64    (written: subscribe/detach, cold)   [P-F2]
CachePadded consumer_slots[MAX]    AtomicUsize× (each written by exactly ONE consumer)
             DETACHED = usize::MAX sentinel (correctness backstop under the bitmap)
buffer      [Slot<T>; capacity]                 (written: producer only)
```

Audit-3 refs: A-n. `dropped_through` is **shared, not producer-local**: the
teardown walk must read it (a producer-local field is unreachable from
`Shared::Drop`, and the naive `[write_cursor - cap, write_cursor)` window
double-drops after a panicking overwrite-drop or a forgotten `WriteSlot`).

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
- `pop()`/`try_pop()` where `T: Clone`: clone out, **then** advance
  (normative order — a panicking clone leaves the element unconsumed)
  [A-5].
- **Closed contract** [A-1.1]: `pop() -> Result<T, Closed>` — `Err` only
  when the producer is dropped AND this consumer has drained all published
  messages; `try_pop() -> Result<Option<T>, Closed>` (`Ok(None)` =
  empty-but-alive). The `closed` flag is read only inside the would-block
  loop (zero hot-path cost); producer `Drop` = flag store **then notify**
  (flag-then-notify closes the missed-wakeup window). shm caveat: a
  *crashed* producer never sets the flag — `Closed` covers graceful drop;
  crash detection stays lease/watchdog territory.
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
  `active_bitmap.fetch_and(!bit)`; **then notify the producer wait** [A-1.3:
  the missing dual — a producer parked on min-movement stalls forever when
  its last gating consumer detaches silently]. Mid-scan detach is safe: a
  DETACHED slot imposes no constraint — the departing consumer's borrows are
  dead by lifetime rules before Drop runs [M-F3]. Re-subscribe goes through
  the full choreography (no stale low re-init possible).
- `subscribe()` exists **only as a method on a live handle**
  (`Producer::subscribe`/`Consumer::subscribe`), which clones the `Arc`
  *before* the registry CAS — makes the subscribe-vs-teardown race
  structurally unreachable [A-2.2]. Returns
  `Result<Consumer, SubscribeError>`, `enum SubscribeError { Closed, Full }`
  [A-1.4].
- Zero consumers: `push`/`try_push` **succeed** (free-run +
  drop-on-overwrite; an error would race every subscribe and joiners can't
  see pre-join messages anyway — unlike tokio there's no retention
  contract). `Producer::consumer_count()` via the registry scan [A-1.3].
- Ownership graph [A-2.3]: `Arc<SpmcShared>`; handles =
  `Producer { arc, next_seq, cached_min, cached_cursor[] }`,
  `Consumer { arc, slot_idx, read_cursor, cached_write }`. A registry slot
  is exclusively owned by its consumer from CAS-acquire to DETACHED-store,
  and the DETACHED store happens in `Consumer::Drop` before the `Arc`
  release — a slot never outlives its refcount. Teardown = last `Arc` drop →
  `Shared::Drop` walks `[dropped_through, write_cursor)`; no borrow can be
  live (every `PopRef` borrows a `Consumer`, which holds the `Arc`).

### 2.5 `T: Send`, producer-drops-on-overwrite **[DECIDED]**

- Consumers never move values out; the producer drops the old occupant when
  overwriting. The drop happens **at claim time** (before `uninit()` is
  handed out).
- **`dropped_through` watermark** (producer-local), advanced *before*
  `drop_in_place` of the old occupant [M-F5: without it, panic-in-drop or an
  abandoned `claim` double-drops — the unwound `push`'s occupant is still
  inside the naive teardown window]. Push-retry and re-claim consult the
  watermark and skip the drop. Subsumes the first-lap check (starts at 0).
- Teardown (last `Arc` drop): `Shared::Drop` walks
  `[dropped_through, write_cursor)` via `SlotCleanup` — `dropped_through`
  (shared, §2.1) is the lower bound, making the double-drop via panicking
  overwrite-drop or forgotten `WriteSlot` unreachable [A-2.1]. Panic in a
  teardown drop: `Box<[T]>` policy — propagate the first panic, remaining
  window elements leak (stated, not silent) [A-5].
- Panic in the old value's drop during push/claim: propagates; the watermark
  keeps state consistent; producer handle remains usable; consumers
  unaffected. Document in the semantics guide.
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

- Header [A-6.3]: new kinds `KIND_SPMC_BYTES=3, KIND_SPMC_ELEMS=4,
  KIND_BCAST_BYTES=5, KIND_BCAST_ELEMS=6` (VERSION stays 1 — old binaries
  reject unknown kinds); `max_consumers: u32` at offset 36 (current gap);
  `closed` in the write-cursor's padded slot; consumer table at offset 384,
  **one 128-byte slot per consumer**: `{ lease: u64, control: u64
  (epoch|state), cursor: usize }` co-resident (producer scan touches one
  line per consumer; that line already carries the consumer's flush).
  Buffer at `384 + 128·max_consumers`.
- **Zombie consumer — slot retirement + epoch [A-4.1, adopted]**: the
  control word is `{u32 epoch | u32 state}`, separate from the cursor word.
  `subscribe` CASes state=ACTIVE at the current epoch;
  `force_detach_consumer(slot)` (unsafe; caller asserts death) bumps the
  epoch and sets RETIRED — **a retired slot is never re-issued until
  `recover_shm`** (full quiesce). The producer's min-scan reads the control
  word in the same pass and skips non-ACTIVE slots regardless of cursor
  content. A live zombie's stores land on a retired word nobody reads: the
  blast radius of a wrong death assertion degrades from "ring corrupted /
  innocent re-subscriber clobbered" to "one slot burned + the zombie's own
  reads lose gating protection" (documented: `force_detach` revokes the
  victim's read validity — same trust register as `force_attach`).
  Rejected alternatives, for the record: packed `gen|cursor` in one word
  (32-bit cursors wrap in seconds on byte rings); per-flush lease check
  (TOCTOU — only a CAS-per-flush closes it, abandoning single-writer for
  every well-behaved consumer).
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
  subscribe/lag [M-F14]; per-push makes `Lagged(n)` exact for free — the
  contract adopted in §3.2 [A-3.1]). Release-stored after the slot publish;
  holds ≤ highest published seq.
- ABA: sound — slot series `2s+1, 2s+2, 2(s+cap)+1, …` is strictly
  increasing; exact-match accept is generation-unique; u64 wraps in ~29
  years at 10 G msg/s [M-F13].
- Producer cost (honest, replaces "strictly cheaper than SPSC" — **deleted**
  [P-F4]): 2 seq stores + word-wise payload + tail store + one
  `fence(Release)` (DMB ISH on aarch64) vs SPSC's 1 Release store. Target:
  **≈ SPSC caught-up profile**; throughput independence vs k is the bench
  gate, now achievable because spinners share the tail line only.

### 3.2 Loss semantics [A-3, resolved — confirm in final grill]

- Lap ⇒ `Err(Lagged { missed: u64 })`, **exact** as of detection (per-push
  tail makes it free; the every-k question is dead).
- Reposition [A-3.2]: `new_pos = tail − capacity + SLACK`,
  **SLACK = capacity/8** (same constant family as adaptive publish).
  Guarantees: `capacity − capacity/8` messages immediately readable; the
  producer must advance ≥ capacity/8 before the consumer can lag again —
  `Lagged` frequency bounded to one per capacity/8 pushes, and cumulative
  `missed` across successive errors is gap-free and overlap-free
  (`missed = new_pos − old_pos`). Kills the lag-storm livelock of naive
  jump-to-oldest.
- `skip_to_latest()` (`pos = tail`) as the explicit market-data method.
- API + closed contract unified [A-1.2]:
  `pop() -> Result<T, PopError>`, `try_pop() -> Result<Option<T>, PopError>`,
  `enum PopError { Lagged { missed: u64 }, Closed }`. `Closed` only after
  remaining published slots are drained (slot seqs stay stable after
  producer death). **Keep `pop` naming** — the changed contract is carried
  by the `Result`, per crate convention [A-6.2].
- Lossy pop is **panic-free at the API layer** (`T: Copy`/NoUninit, no user
  code runs; torn bytes discarded as `MaybeUninit`). Lossy `drain`: deferred
  from v1 (per-message validation erodes the batching win) [A-5].

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

Producer never waits ⇒ **no `P` type parameter**:
`broadcast::RingBuffer<T, C = PauseWait>` — a phantom `P` on a producer that
structurally cannot wait is a type-level lie; the crate's precedent is
types-encode-truths (handles aren't `Clone`) [A-6.1]. Consumers:
**spin-only, forced** — a parked consumer would need producer notifies,
violating the zero-consumer-knowledge design. Spinning targets the shared
`tail` line (§3.1). Consumer would-block loop also checks `closed` [A-1.2]
(one header flag, producer-Drop-written).

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

## 5b. Documentation restructure this forces [A-6.4]

`docs/guide/semantics.md` statements change meaning per machine: producer
`len`/`is_full` ("one publish window" → "min over N windows" / meaningless
under lossy free-run); `mem::forget` (SPSC: benign self-redelivery; gating:
redelivery **plus global stall of every other consumer**; lossy: no guards,
inapplicable); "exactly one producer and one consumer — enforced by types"
(false for both new machines); drain at-most-once (becomes per-consumer);
wrap-around (gains the lossy u64 seq story). Split into `semantics.md`
(shared invariants) + per-machine contract pages, each carrying the
closed/forget/counters/panic table. Lands with PR-1/PR-2 docs.

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

## 7. Open questions (final grill round)

All audit findings have adopted resolutions in the text above; these are the
decisions that remain the owner's:

1. **`MAX_CONSUMERS` default** — proposal: 8 (hard cap 64 = the bitmap
   word; shm header pays 128 B/slot).
2. **Confirm the closed-contract API shape** — gating `pop()` becomes
   `Result<T, Closed>` (SPSC `pop()` stays `-> T`); lossy
   `Result<T, PopError{Lagged, Closed}>`. Necessary (blocking-forever is
   unshippable) but it does diverge the two rings' signatures from SPSC's.
3. **Confirm lossy reposition** — oldest + capacity/8 slack default,
   `skip_to_latest()` opt-in, `pop` naming kept.
4. **Confirm v1 spin-only** end-to-end (gating producer side + all lossy).
5. **Confirm the zombie answer** — slot retirement + epoch control word;
   retired slots unavailable until `recover_shm`; `force_detach` documented
   as revoking the victim's read validity.
