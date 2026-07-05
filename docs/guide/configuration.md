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

There are four strategies, all implementing [`WaitStrategy`](crate::WaitStrategy):

| Strategy                        | Wake latency | CPU cost           | Under oversubscription | Power draw | Cross-process |
| ------------------------------- | ------------ | ------------------ | ---------------------- | ---------- | ------------- |
| [`NoOpWait`](crate::NoOpWait)   | lowest       | highest (no hint)  | starves peers          | highest    | yes           |
| [`PauseWait`](crate::PauseWait) | very low     | high (spins core)  | poor                   | high       | yes           |
| [`YieldWait`](crate::YieldWait) | low–moderate | moderate           | tolerant               | moderate   | yes           |
| [`CvWait`](crate::CvWait)       | highest      | lowest (can sleep) | best                   | lowest     | **no**        |

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
- [`CvWait`](crate::CvWait) — parks on a `Mutex`/`Condvar` with a ~100 ns timed
  recheck, and is woken by the peer's `notify()`. The only strategy that lets a
  blocked side actually **sleep** instead of burning CPU, so it has by far the
  lowest power draw and behaves best under oversubscription — but the highest
  wake latency. It is **not** usable cross-process: it is not `CrossProcess`, and
  the shared-memory rings reject it at compile time.

## Picking a strategy

- Pick [`PauseWait`](crate::PauseWait) when latency is critical and you can
  dedicate a core (ideally a sibling of the peer's core) to the busy side.
- Pick [`NoOpWait`](crate::NoOpWait) when you want the last few nanoseconds off
  `PauseWait`, own the core outright, and have measured that dropping the `pause`
  helps your hardware. This is a niche choice.
- Pick [`YieldWait`](crate::YieldWait) — the default — when you are unsure, when
  the ring may run on a machine with more threads than cores, or when you want a
  reasonable latency/CPU balance without pinning.
- Pick [`CvWait`](crate::CvWait) when idle CPU and power matter more than wake
  latency: bursty or low-rate traffic where a side would otherwise spin for long
  stretches doing nothing. Not available across processes.

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
