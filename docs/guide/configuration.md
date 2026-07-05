Two decisions shape every ring you create: **how big** it is (capacity) and
**how each side waits** when it has nothing to do (wait strategy). Both are set
once, at construction, through [`RingBuffer::new`](crate::RingBuffer::new) or
[`RingBuffer::with_wait_strategies`](crate::RingBuffer::with_wait_strategies)
(and the byte-oriented equivalents). This guide explains what the knobs do and
how to choose them.

# Capacity

## Power-of-two rounding

Every constructor takes a *minimum* capacity at runtime and rounds it **up to
the next power of two**. Ask for `1000` slots and you get `1024`; ask for
`1025` and you get `2048`. The value you actually got is reported by
[`capacity()`](crate::Producer::capacity) on either half — never assume you
received exactly what you requested.

```rust
use rust_rb::RingBuffer;

let (producer, consumer) = RingBuffer::<i32>::new(1000);
assert_eq!(producer.capacity(), 1024);
assert_eq!(consumer.capacity(), 1024);
```

The rounding is not cosmetic. A power-of-two capacity lets the ring turn the
"wrap the index back to the start" step into a single bitwise `AND` with a mask
(`index & (capacity - 1)`) instead of a `%` modulo. That mask is on the hot path
of every push and pop, so keeping it branch-free and division-free matters. The
cost of the convenience is that you can never size a ring to, say, exactly 3000
slots — you round up to 4096 and absorb the slack.

Requesting a capacity of `0` panics; there is no meaningful zero-length ring.

## Byte rings measure capacity in bytes

The variable-length ring, [`BytesRingBuffer`](crate::BytesRingBuffer), takes its
`min_capacity` in **bytes**, not messages. It is rounded up to a power of two
exactly like the typed ring, but the unit is the size of the backing byte
buffer. Because each message carries a small length header and the ring reserves
headroom so a single message can never straddle the wrap point, the largest
message you can push is `capacity / 2 - 4` bytes, reported by
`max_message_len()`.

```rust
use rust_rb::BytesRingBuffer;

let (producer, consumer) = BytesRingBuffer::new(4096);
assert_eq!(producer.capacity(), 4096);
// capacity / 2 - 4 == 2044
assert_eq!(producer.max_message_len(), 2044);
```

If you know your largest message size, size the ring for it: pick a capacity of
at least `2 * (max_message_len + 4)`, then round up, then add whatever extra
room you want for buffering multiple in-flight messages.

## The memory-footprint trade-off

Capacity is a direct multiplier on the ring's resident memory. A typed
`RingBuffer<T>` reserves `capacity` slots of `T`; a byte ring reserves
`capacity` bytes. Doubling capacity doubles the footprint, and because the
buffer is touched from two cores it also occupies cache and TLB entries on both.
Bigger is not free.

The tension is between **absorbing bursts** and **staying small**:

- A **larger** ring absorbs traffic bursts. When the producer briefly outruns
  the consumer, a deep ring gives the backlog somewhere to sit instead of
  stalling the producer. It also *widens the adaptive publish window* (below),
  which reduces cursor-update overhead under sustained load.
- A **smaller** ring has a smaller footprint and stays hotter in cache. For
  steady, well-matched producer/consumer rates where the queue rarely fills, a
  small ring is cheaper and just as fast.

## Sizing and the publish window

Capacity interacts with one behavioural knob: the **adaptive publish window**.
While the queue is backed up, the consumer stops publishing its cursor on every
element and instead batches publishes to one per `capacity / 8` elements (capped
at 64 elements, or 4096 bytes for the byte ring). This amortises the cost of the
cross-core cursor write when there is a long backlog to drain.

The sizing consequence is subtle but worth knowing: a bigger capacity means a
bigger publish window, which means the producer-side view of occupancy can lag
further behind reality. Specifically, [`Producer::len`](crate::Producer::len)
and `is_full()` may transiently **over-count** by up to one window's worth of
elements — they can report the ring as fuller than it truly is until the next
batched publish lands. This is a conservative, never-lose-data skew, but if you
drive control logic off `len()` you should account for it. See the
[semantics guide](crate::guide::semantics) for the full treatment of when these
counters are exact and when they are approximate.

# Wait strategy

When a side has nothing to do — the consumer on an empty ring, or a blocking
producer on a full one — it runs a *wait strategy* between rechecks. The
strategy is a **compile-time type parameter**, chosen independently for each
side: `P` is the producer-side strategy, `C` the consumer-side strategy. The
default for both is [`YieldWait`](crate::YieldWait), matching the `cpp-fastchan`
template default. Pick others with
[`RingBuffer::with_wait_strategies`](crate::RingBuffer::with_wait_strategies).

There are six strategies, all implementing [`WaitStrategy`](crate::WaitStrategy):

| Strategy                            | Wake latency                | CPU cost           | Under oversubscription | Power draw | Cross-process | Self-timed |
| ----------------------------------- | --------------------------- | ------------------ | ---------------------- | ---------- | ------------- | ---------- |
| [`NoOpWait`](crate::NoOpWait)       | lowest                      | highest (no hint)  | starves peers          | highest    | yes           | yes        |
| [`PauseWait`](crate::PauseWait)     | very low                    | high (spins core)  | poor                   | high       | yes           | yes        |
| [`YieldWait`](crate::YieldWait)     | low–moderate                | moderate           | tolerant               | moderate   | yes           | yes        |
| [`SleepWait`](crate::SleepWait)     | timer-tick (tens of µs)     | very low (sleeps)  | good                   | low        | yes           | yes        |
| [`BackoffWait`](crate::BackoffWait) | escalates: low → timer-tick | escalates: high → very low | good           | escalates  | yes           | yes        |
| [`CvWait`](crate::CvWait)           | highest                     | lowest (can sleep) | best                   | lowest     | **no**        | **no**     |

What each one actually does:

- [`NoOpWait`](crate::NoOpWait) — an empty busy loop with no `pause` hint and no
  yield. Absolute lowest latency and hammers the core hardest. Use only when you
  fully own the core and specifically want no `pause` instruction in the loop.
- [`PauseWait`](crate::PauseWait) — issues `core::hint::spin_loop()` each
  iteration (a CPU `pause`). Lowest practical wake latency while being a little
  kinder to a hyperthreaded sibling than a raw spin. Still burns a full core.
  Best when the peer runs on a dedicated sibling core.
- [`YieldWait`](crate::YieldWait) — calls `std::thread::yield_now()` each
  iteration. The balanced default: it cooperates with the OS scheduler and
  tolerates oversubscription (more busy threads than cores) far better than a
  pure spin, at the cost of some wake latency.
- [`SleepWait<NANOS>`](crate::SleepWait) — sleeps a fixed `NANOS` nanoseconds
  (default 100 µs) each iteration. The lowest-CPU strategy that is still
  *self-timed*: it never needs the peer to wake it, so — unlike `CvWait` — it
  works across processes and on the multi-consumer rings. The effective
  granularity is the OS timer (tens of microseconds on Linux with default
  timerslack), so treat `NANOS` as a floor, not a promise.
- [`BackoffWait<SPINS, YIELDS, MIN, MAX>`](crate::BackoffWait) — Aeron-style
  escalation: spin `SPINS` turns, yield `YIELDS` turns, then sleep with
  exponential doubling from `MIN` to `MAX` nanoseconds (defaults: 100 spins,
  100 yields, 1 µs → 1 ms), consulting the wake condition every turn. Each
  blocking call starts a fresh episode. The right choice when waits are
  *usually* short but must not burn a core when they are long.
- [`CvWait`](crate::CvWait) — parks on a `Mutex`/`Condvar` with a ~100 ns timed
  recheck, and is woken by the peer's `notify()`. The only strategy whose sleep
  is cut short by a peer **notification** rather than a timer, so it combines
  minimal CPU with the best behaviour under oversubscription — at the highest
  wake latency. It is **not** usable cross-process (it is not
  [`CrossProcess`](crate::CrossProcess): its mutex/condvar live in one
  process's memory) and **not** usable on the multi-consumer rings (it is not
  [`SelfTimed`](crate::SelfTimed) — see below); both reject it at compile time.

## Picking a strategy

- Pick [`PauseWait`](crate::PauseWait) when latency is critical and you can
  dedicate a core (ideally a sibling of the peer's core) to the busy side.
- Pick [`NoOpWait`](crate::NoOpWait) when you want the last few nanoseconds off
  `PauseWait`, own the core outright, and have measured that dropping the `pause`
  helps your hardware. This is a niche choice.
- Pick [`YieldWait`](crate::YieldWait) — the default — when you are unsure, when
  the ring may run on a machine with more threads than cores, or when you want a
  reasonable latency/CPU balance without pinning.
- Pick [`BackoffWait`](crate::BackoffWait) when traffic is bursty: it reacts
  like a spin inside a burst and decays to a timer sleep between bursts,
  without needing a peer notification. This is the strategy to try first on
  the multi-consumer rings when you cannot dedicate cores.
- Pick [`SleepWait`](crate::SleepWait) when a side is expected to be idle for
  long stretches and a timer-tick wake latency is acceptable — the flat
  lowest-CPU choice that still works everywhere (cross-process,
  multi-consumer).
- Pick [`CvWait`](crate::CvWait) when idle CPU and power matter more than wake
  latency *and* the ring is an in-process SPSC ring: bursty or low-rate traffic
  where a side would otherwise spin doing nothing. Not available across
  processes or on the multi-consumer rings.

## `SelfTimed`: what the multi-consumer rings require

The four multi-consumer rings constrain the strategy choice with the
[`SelfTimed`](crate::SelfTimed) marker — a strategy that makes progress
**without ever needing a peer notify**: the pure spins, the yield, the timed
sleep, and the backoff all qualify; [`CvWait`](crate::CvWait) does not, and is
rejected at compile time.

- [`spmc`](crate::spmc) / [`spmc_bytes`](crate::spmc_bytes) require `SelfTimed`
  on **both** sides. With N waiting consumers, a notify-dependent strategy
  needs per-waiter wake state — `CvWait`'s single shared flag can skip a
  parked waiter, silently adding its full timeout to the wake latency — and
  the gating producer's publish path must never pay a lock/signal per
  consumer flush.
- [`broadcast`](crate::broadcast) / [`broadcast_bytes`](crate::broadcast_bytes)
  require `SelfTimed` on the **consumer** side (there is no producer-side
  strategy at all: a lossy push never blocks). The producer keeps zero
  consumer knowledge by design, so nobody will ever notify a parked reader —
  a reader's wait must time itself.

The SPSC rings accept any [`WaitStrategy`](crate::WaitStrategy), including
`CvWait`.

## The broadcast reposition `slack`

The lossy element ring has one knob of its own:
[`broadcast::RingBuffer::with_slack`](crate::broadcast::RingBuffer::with_slack).
After a lap, a consumer repositions to `tail - capacity + slack`: `capacity -
slack` messages are immediately readable, and the producer must advance at
least `slack` more before that consumer can lag again. The default is
`capacity / 8` (minimum 1; a capacity-1 "latest value" ring uses 0). Larger
slack means fewer, bigger loss events;
`slack == 0` maximizes salvage but allows back-to-back lag events (and a
transient `Lagged { missed: 0 }` while the producer is overwriting exactly
one lap ahead). `slack >= capacity` panics.

```rust
use rust_rb::broadcast::RingBuffer;

// Reposition a lapped reader to tail - 1024 + 256.
let (tx, rx) = RingBuffer::<u64>::with_slack(1024, 256);
let _ = (tx, rx);
```

The byte ring has no slack knob: an arbitrary byte offset is not a record
boundary, so a lapped [`broadcast_bytes`](crate::broadcast_bytes) consumer
always jumps to the start of the most recent record instead. On shared-memory
broadcast rings the slack is a create-time setting stored in the region
header, inherited by every consumer.

## Different strategies per side

Because `P` and `C` are independent type parameters, you can tune each side to
its role. A common pattern pairs a latency-critical consumer that must react
instantly with a producer that only fires in bursts and should sleep between
them:

```rust
use rust_rb::{RingBuffer, PauseWait, CvWait};

// Consumer spins for minimum wake latency; producer parks between bursts.
let (producer, consumer) =
    RingBuffer::<u64, CvWait, PauseWait>::with_wait_strategies(4096);
let _ = (producer, consumer);
```

The type parameters are `<T, P, C>`: the item type first, then the
producer-side and consumer-side strategies. The byte ring is the same without
the item type — `BytesRingBuffer::<P, C>`:

```rust
use rust_rb::{BytesRingBuffer, PauseWait, NoOpWait};

// Both sides busy-wait, tuned for a fully core-pinned, latency-first setup.
let (producer, consumer) =
    BytesRingBuffer::<PauseWait, NoOpWait>::with_wait_strategies(8192);
let _ = (producer, consumer);
```

If both sides should use the same non-default strategy, name it twice:

```rust
use rust_rb::{RingBuffer, PauseWait};

let (producer, consumer) =
    RingBuffer::<i32, PauseWait, PauseWait>::with_wait_strategies(1024);
let _ = (producer, consumer);
```

Remember that any ring destined for shared memory must use a `CrossProcess`
strategy on both sides, which rules out [`CvWait`](crate::CvWait); the
shared-memory constructors enforce this. See the
[shared-memory guide](crate::guide::shm_ipc) for details.
