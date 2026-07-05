# Migrating from `cpp-fastchan`

`rust-rb` is a Rust port of the SPSC ring from
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan). The fast path — masked
monotonic indices, per-side cursor caching, cache-line padding, and
compile-time-selectable wait strategies — is carried over faithfully, so if you
know the C++ queue the mental model transfers directly. What changes is mostly
surface: idiomatic Rust names, a runtime capacity, and the SPSC contract moved
from convention into the type system. A few behaviours are genuinely new.

## Name mapping

| `cpp-fastchan` concept | `rust-rb` equivalent | Notes |
| ---------------------- | -------------------- | ----- |
| `put(v)` (blocking) | [`Producer::push`](crate::Producer::push) | idiomatic Rust: blocking base name |
| non-blocking put | [`Producer::try_push`](crate::Producer::try_push) → `Result<(), T>` | returns the value back on full |
| `get()` (blocking) | [`Consumer::pop`](crate::Consumer::pop) → `T` | |
| non-blocking get | [`Consumer::try_pop`](crate::Consumer::try_pop) → `Option<T>` | `None` on empty |
| `FastChan<T, Size, ...>` compile-time `Size` | [`RingBuffer::new(min_capacity)`](crate::RingBuffer::new) runtime arg | rounded up to a power of two; read it back with [`capacity()`](crate::RingBuffer) |
| wait-strategy template argument | type parameters `P` / `C` on [`RingBuffer<T, P, C>`](crate::RingBuffer) | per side; set via [`with_wait_strategies`](crate::RingBuffer::with_wait_strategies) |
| default wait = yield | default `P = C =` [`YieldWait`](crate::YieldWait) | matches the C++ template default |
| construct a channel, then call `put`/`get` on it | `new()` returns the `(`[`Producer`](crate::Producer)`, `[`Consumer`](crate::Consumer)`)` pair | move each half to its thread |
| `size()` / occupancy check | `len()`, `is_empty()`, `is_full()` on either half | |

The Rust API deliberately does **not** expose `put`/`get`. `push`/`pop` (with
`try_push`/`try_pop` non-blocking variants) is the idiomatic convention shared
by `ringbuf`/`rtrb`: a blocking base name plus a `try_` prefix for the
fail-fast form. This is a permanent naming choice, not an alias.

## What's the same

- **The fast-path design.** Monotonic masked indices, cursor caching so neither
  side reads the other's cache line in steady state, a full destructive-
  interference gap between the shared atomics, and one `Release`/`Acquire`
  cursor hand-off per side. The hot path compiles to the same shape as the C++
  original.
- **Selectable wait strategies.** The same four behaviours are available:
  [`PauseWait`](crate::PauseWait) (spin with a pause hint),
  [`YieldWait`](crate::YieldWait) (yield the thread, the default),
  `NoOpWait` (tight spin), and `CvWait` (park on a condition variable). They
  are chosen at compile time, exactly as in C++ — just as type parameters
  rather than a template argument. See [`WaitStrategy`](crate::WaitStrategy).
- **SPSC discipline.** One producer, one consumer, no locks on the hot path.

## What's different / new

- **SPSC only (scope note).** `cpp-fastchan`'s MPSC and other multi-role
  variants are **not** ported. `rust-rb` is single-producer / single-consumer
  and nothing else. (An SPMC broadcast ring is on the backlog, not shipped.)
- **The SPSC contract is compile-time enforced.** [`Producer`](crate::Producer)
  and [`Consumer`](crate::Consumer) are `Send` but not `Clone`. Where the C++
  original lets you construct extra endpoints and trusts you not to, here a
  second producer or consumer simply will not compile.
- **Capacity is a runtime constructor argument.**
  [`RingBuffer::new(min_capacity)`](crate::RingBuffer::new) takes the capacity
  as a value, rounds it up to the next power of two, and `capacity()` returns
  the rounded result — versus the C++ compile-time size template parameter. The
  index mask still lives in a register on the hot path, so this costs nothing
  and keeps the door open for alternative backings such as shared memory.
- **Adaptive read-cursor publishing.** The consumer batches its cursor publishes
  while the queue is backed up, then reverts to per-element publishing once
  caught up — a throughput win over the C++ per-element publish under
  backpressure, at the cost of a transient producer-side `len()`/`is_full()`
  over-count. See the [performance guide](crate::guide::performance) and the
  [semantics guide](crate::guide::semantics).
- **Zero-copy in-place access.** On the write side,
  [`claim`](crate::Producer::claim) reserves a slot you construct into and
  publish with `commit_init`; on the read side,
  [`pop_ref`](crate::Consumer::pop_ref) returns a guard that dereferences to the
  element where it lies in the ring. No equivalent in the original.
- **A variable-size byte ring.** [`BytesRingBuffer`](crate::BytesRingBuffer)
  transports length-framed byte messages of differing sizes through one ring,
  with the same design and zero-copy reads and writes.
- **Shared-memory / cross-process backing.** Either ring can be backed by a
  mapped region so the producer and consumer live in different processes, with
  the same handle types and hot paths. See the
  [shared-memory guide](crate::guide::shm_ipc).

## Before / after

The C++ original, roughly:

```cpp
#include "fastchan.hpp"

// Size and wait strategy are template parameters.
fastchan::FastChan<uint64_t, 1024> chan;

chan.put(42);              // blocking
uint64_t v = chan.get();   // blocking
```

The `rust-rb` equivalent — capacity is a runtime argument, the wait strategy is
a defaulted type parameter, and construction hands back the two halves:

```rust
use rust_rb::RingBuffer;

// Capacity chosen at runtime, rounded up to a power of two (1024 here).
let (mut tx, mut rx) = RingBuffer::new(1000);

tx.push(42u64);              // blocking (uses the wait strategy)
assert_eq!(rx.pop(), 42);    // blocking
```

Selecting a non-default wait strategy is a type annotation on construction
rather than a template argument, and the choice can differ per side:

```rust
use rust_rb::{RingBuffer, PauseWait};

// PauseWait on the producer, PauseWait on the consumer.
let (mut tx, mut rx) =
    RingBuffer::<u64, PauseWait, PauseWait>::with_wait_strategies(1024);

assert!(tx.try_push(1).is_ok());
assert_eq!(rx.try_pop(), Some(1));
```

Moving the two halves to their threads is the same as splitting any Rust
channel — each half is `Send`, so it moves cleanly:

```no_run
use rust_rb::RingBuffer;
use std::thread;

let (mut tx, mut rx) = RingBuffer::new(1024);

let producer = thread::spawn(move || {
    for i in 0..1_000u64 {
        tx.push(i);
    }
});
let consumer = thread::spawn(move || {
    for _ in 0..1_000u64 {
        let _ = rx.pop();
    }
});

producer.join().unwrap();
consumer.join().unwrap();
```
