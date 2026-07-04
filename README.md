# rust-rb

High-performance **single-producer / single-consumer** (SPSC) ring buffer for
Rust. A faithful port of the SPSC queue from
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), keeping every design
choice that makes the original fast and adding compile-time safety on top.

Pushes and pops can each be **blocking** or **non-blocking**, and the blocking
wait behaviour is selectable: spin with a CPU pause hint, yield the thread, spin
with no hint, or park on a condition variable.

## Usage

```rust
use rust_rb::spsc::Spsc;

// Capacity is rounded up to the next power of two (1024 here).
let (mut tx, mut rx) = Spsc::<u64, 1000>::new();

tx.push(1);
tx.push(2);
tx.push(3);

assert_eq!(rx.pop(), 1);
assert_eq!(rx.pop(), 2);
```

`Spsc::<T, N, P, C>::new()` returns a `(Producer, Consumer)` pair (`P`/`C` are
the producer- and consumer-side wait strategies). Move each
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
| `push(v)`            | `pop() -> T`         | block (using the wait strategy)        |
| `try_push(v) -> Result<(), T>` | `try_pop() -> Option<T>` | return immediately when full / empty |

Both halves also expose `len()`, `is_empty()`, `is_full()`, and `capacity()`.

### Wait strategies

| Type         | Behaviour while waiting                          | Trade-off                       |
| ------------ | ----------------------------------------------- | ------------------------------- |
| `PauseWait`  | spin with `PAUSE`/`YIELD` hint                  | lowest latency, burns a core    |
| `YieldWait`  | `thread::yield_now()` (default)                 | friendly to oversubscription    |
| `NoOpWait`   | tight spin, no hint                             | lowest latency, most power      |
| `CvWait`     | park on a condvar, recheck every 100 ns         | lowest CPU, highest latency     |

## What makes it fast

The same four ideas as the C++ original, plus an adaptive refinement of the
publish side:

- **Monotonic masked indices.** The write and read cursors only ever increase;
  the slot is `index & (capacity - 1)`. No modulo, and the *entire*
  power-of-two capacity is usable (no sacrificial empty slot).
- **Cursor caching.** The producer keeps a private cached copy of the consumer's
  cursor and only reloads the shared atomic when the buffer *looks* full; the
  consumer does the mirror image. In steady state neither side reads the other's
  cache line.
- **No false sharing.** The two shared atomics are padded a full destructive
  interference distance apart — 128 bytes on x86-64 and AArch64, where
  adjacent-line prefetchers make 64-byte spacing insufficient — and each side's
  private cursors live in its handle (owned by one thread) rather than in
  shared memory.
- **Single-writer publish.** One `Release` store publishes each side's progress;
  the other side observes it with an `Acquire` load. On x86-64 these compile to
  plain `MOV`s, so the orderings are free while remaining correct on weakly
  ordered targets such as AArch64.
- **Adaptive read-cursor publishes.** A publish only costs something when the
  other side is polling the published line, and the producer only polls the
  read cursor when the queue is full. So the consumer publishes after every
  element while it is *caught up* — uncontended, latency-critical, and
  identical to the C++ behavior — but defers to one publish per `capacity / 8`
  (max 64) elements while the queue is *backed up*. In the backed-up regime a
  per-element publish lets the polling producer steal the cursor's cache line
  between every store, collapsing both threads into a lockstep line ping-pong;
  deferring amortizes the transfer and lets the producer push in bursts. On a
  Grace (Neoverse V2) core pair this takes the saturated spin-strategy
  benchmark from ~135 M to ~860 M msgs/s, roughly twice the C++ original's
  best on the same cores. The trade-off: while (and only while) the queue is
  backed up, producer-side `len()`/`is_full()` may transiently over-count by
  up to the deferral bound; consumer-side views are exact, and the consumer
  never waits, reports empty, or drops with progress unpublished.

## Variable-size messages: `SpscBytes`

When the payload is not one fixed type — serialized structs, wire frames, log
records of differing lengths — `SpscBytes` transports discrete byte messages
through one shared ring:

```rust
use rust_rb::spsc_bytes::SpscBytes;

// Capacity is in *bytes*, rounded up to the next power of two.
let (mut tx, mut rx) = SpscBytes::<4096>::new();

tx.push(b"tick");                 // copy in
assert_eq!(&*rx.pop(), b"tick"); // zero-copy view, released on drop
```

Each message is framed as a 4-byte length header plus the payload, rounded up
to a 4-byte boundary. Records never wrap: a record that would straddle the end
of the buffer is preceded by a padding marker and starts again at offset zero,
so every payload is contiguous and reads are zero-copy. Because a message may
need that padding *in addition to* its own record, a single message is capped
at `capacity / 2 - 4` bytes (`max_message_len()`) — this guarantees any legal
message can always be written eventually, whatever the cursor positions.

| Producer | Consumer | Behaviour |
| -------- | -------- | --------- |
| `push(&[u8])` | `pop() -> Msg` | block (using the wait strategy) |
| `try_push(&[u8]) -> bool` | `try_pop() -> Option<Msg>` | return immediately when full / empty |
| `claim(len)` / `try_claim(len)` | `drain(f) -> usize` | zero-copy write slot / batched consume |

`claim` returns a `WriteSlot` that dereferences to the payload slice, so you
serialize directly into the ring and `commit()`; dropping it uncommitted
abandons the space. `drain` consumes every available message with a **single**
cursor publish, amortizing the release store and wake-up across the batch.
`Msg` dereferences to the payload bytes in place; the bytes are handed back to
the producer when it drops.

The framing, cursor caching, padding, and memory-ordering design is identical
to the fixed-size ring; the wait strategies are shared between both.

## Benchmark

```
cargo run --release --example bench          # fixed-size ring
cargo run --release --example bench_bytes    # variable-size ring
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
