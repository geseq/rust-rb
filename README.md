# rust-rb

High-performance **single-producer / single-consumer** (SPSC) ring buffer for
Rust. A faithful port of the SPSC queue from
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), keeping every design
choice that makes the original fast and adding compile-time safety on top.

Gets and puts can each be **blocking** or **non-blocking**, and the blocking
wait behaviour is selectable: spin with a CPU pause hint, yield the thread, spin
with no hint, or park on a condition variable.

## Usage

```rust
use rust_rb::spsc::Spsc;

// Capacity is rounded up to the next power of two (1024 here).
let (mut tx, mut rx) = Spsc::<u64, 1000>::new();

tx.put(1);
tx.put(2);
tx.put(3);

assert_eq!(rx.get(), 1);
assert_eq!(rx.get(), 2);
```

`Spsc::<T, N, Put, Get>::new()` returns a `(Producer, Consumer)` pair. Move each
half to its thread; the buffer lives in a shared `Arc` and is freed when both
halves drop. Neither half is `Clone`, so the single-producer / single-consumer
contract — left to the programmer in the C++ original — is enforced by the type
system here.

Wait strategies are type parameters (defaulting to `YieldWait` on both sides,
matching the C++ template defaults):

```rust
use rust_rb::spsc::Spsc;
use rust_rb::wait::PauseWait;

let (mut tx, mut rx) = Spsc::<i32, 4096, PauseWait, PauseWait>::new();
```

### API

| Producer            | Consumer             | Behaviour                              |
| ------------------- | -------------------- | -------------------------------------- |
| `put(v)`            | `get() -> T`         | block (using the wait strategy)        |
| `try_put(v) -> Result<(), T>` | `try_get() -> Option<T>` | return immediately when full / empty |

Both halves also expose `len()`, `is_empty()`, `is_full()`, and `capacity()`.

### Wait strategies

| Type         | Behaviour while waiting                          | Trade-off                       |
| ------------ | ----------------------------------------------- | ------------------------------- |
| `PauseWait`  | spin with `PAUSE`/`YIELD` hint                  | lowest latency, burns a core    |
| `YieldWait`  | `thread::yield_now()` (default)                 | friendly to oversubscription    |
| `NoOpWait`   | tight spin, no hint                             | lowest latency, most power      |
| `CvWait`     | park on a condvar, recheck every 100 ns         | lowest CPU, highest latency     |

## What makes it fast

The same four ideas as the C++ original:

- **Monotonic masked indices.** The write and read cursors only ever increase;
  the slot is `index & (capacity - 1)`. No modulo, and the *entire*
  power-of-two capacity is usable (no sacrificial empty slot).
- **Cursor caching.** The producer keeps a private cached copy of the consumer's
  cursor and only reloads the shared atomic when the buffer *looks* full; the
  consumer does the mirror image. In steady state neither side reads the other's
  cache line.
- **No false sharing.** The two shared atomics each sit on their own 64-byte
  cache line, and each side's private cursors live in its handle (owned by one
  thread) rather than in shared memory.
- **Single-writer publish.** One `Release` store publishes each side's progress;
  the other side observes it with an `Acquire` load. On x86-64 these compile to
  plain `MOV`s, so the orderings are free while remaining correct on weakly
  ordered targets such as AArch64.

## Benchmark

```
cargo run --release --example bench
```

For meaningful numbers, pin the producer and consumer to dedicated cores, e.g.
`taskset -c 2,3 cargo run --release --example bench`. As in the original,
latency is dominated by the core-to-core topology of the producer/consumer pair.

## Testing

```
cargo test
```

The suite ports the original's single-threaded fill, single-threaded round-trip,
and multi-threaded producer/consumer tests, run across every wait-strategy
combination and both the blocking and non-blocking APIs, plus a drop-correctness
test for elements left in the buffer.

## License

MIT, same as the original.
