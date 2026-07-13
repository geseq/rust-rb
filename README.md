# rust-rb

High-performance **single-producer** ring buffers for Rust: an SPSC queue —
a faithful port of the one in
[`cpp-fastchan`](https://github.com/geseq/cpp-fastchan), keeping every design
choice that makes the original fast and adding compile-time safety on top —
plus single-producer/**multi-consumer** broadcast rings (lossless, lossy, and a
composed variant with required gating readers alongside lossy observers) built
on the same engine.

Pushes and pops can each be **blocking** or **non-blocking**, and the blocking
wait behaviour is selectable: spin with a CPU pause hint, yield the thread, spin
with no hint, or park on a condition variable.

## Usage

```rust
use rust_rb::RingBuffer;

// Capacity is chosen at runtime, rounded up to the next power of two.
let (mut tx, mut rx) = RingBuffer::new(1000);

tx.push(1u64);
tx.push(2);

assert_eq!(rx.pop(), 1);
assert_eq!(rx.pop(), 2);
```

`RingBuffer::new(capacity)` returns a `(Producer, Consumer)` pair. Move each
half to its thread; the buffer lives in a shared `Arc` and is freed when both
halves drop. Neither half is `Clone`, so the single-producer / single-consumer
contract — left to the programmer in the C++ original — is enforced by the type
system here. A runtime capacity costs nothing: the index mask lives in a
register on the hot path either way (verified on the saturated benchmark), and
it keeps the door open for alternative backings such as shared memory.

Wait strategies are type parameters (defaulting to `YieldWait` on both sides,
matching the C++ template defaults):

```rust
use rust_rb::{RingBuffer, PauseWait};

let (mut tx, mut rx) =
    RingBuffer::<i32, PauseWait, PauseWait>::with_wait_strategies(4096);
```

### API

| Producer            | Consumer             | Behaviour                              |
| ------------------- | -------------------- | -------------------------------------- |
| `push(v)`            | `pop() -> T`         | block (using the wait strategy)        |
| `try_push(v) -> Result<(), T>` | `try_pop() -> Option<T>` | return immediately when full / empty |
| `claim()` / `try_claim()` | `pop_ref()` / `try_pop_ref()` | zero-copy: write / read in place |

Both halves also expose `len()`, `is_empty()`, `is_full()`, and `capacity()`.

### Zero-copy access

`claim()` reserves the next slot and returns a `WriteSlot`: construct the
element directly in the buffer via `uninit()` and publish with the unsafe
`commit_init()`, or move a value in with `commit(v)`. Dropping the slot
uncommitted publishes nothing.

`pop_ref()` returns a `PopRef` guard that dereferences to the element where it
lies in the buffer (mutably too); the element is dropped in place and its slot
released when the guard drops. Prefer `pop()` to drain quickly — moving the
value out releases the slot immediately — and `pop_ref()` when the consumer
must finish with the element before continuing.

### Wait strategies

| Type          | Behaviour while waiting                             | Trade-off                       |
| ------------- | --------------------------------------------------- | ------------------------------- |
| `PauseWait`   | spin with `PAUSE`/`YIELD` hint                      | lowest latency, burns a core    |
| `YieldWait`   | `thread::yield_now()` (default)                     | friendly to oversubscription    |
| `NoOpWait`    | tight spin, no hint                                 | lowest latency, most power      |
| `SleepWait`   | fixed timed sleep (const `NANOS`, default 100 µs)   | lowest CPU that still works everywhere; timer-tick latency |
| `BackoffWait` | spin → yield → escalating sleep (Aeron-style)       | fast in bursts, cheap when idle |
| `CvWait`      | park on a condvar, recheck every 100 ns             | lowest CPU, highest latency; **SPSC in-process only** |

All but `CvWait` are *self-timed* (`SelfTimed`): they make progress without a
peer notification, which is why they — and only they — are accepted by the
multi-consumer rings and the shared-memory constructors.

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

## Variable-size messages: `BytesRingBuffer`

When the payload is not one fixed type — serialized structs, wire frames, log
records of differing lengths — `BytesRingBuffer` transports discrete byte
messages through one shared ring:

```rust
use rust_rb::BytesRingBuffer;

// Capacity is in *bytes*, rounded up to the next power of two.
let (mut tx, mut rx) = BytesRingBuffer::new(4096);

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

## Multi-consumer rings

Six more rings broadcast one producer's stream to **many consumers**. Three
policies × two payload shapes:

|                    | Fixed `T`             | Byte messages                |
| ------------------ | --------------------- | ---------------------------- |
| **Gating** (lossless: the slowest consumer gates the producer) | `spmc::RingBuffer` | `spmc_bytes::BytesRingBuffer` |
| **Lossy** (the producer never blocks; a lapped consumer loses messages and gets an exact count) | `broadcast::RingBuffer` | `broadcast_bytes::BytesRingBuffer` |
| **Mixed** (required *anchors* gate the producer while unbounded lossy *observers* tap the same stream) | `anchored::RingBuffer` | `anchored_bytes::BytesRingBuffer` |

Every ring stays single-producer (enforced by the types); consumers subscribe
dynamically. Dropping the producer closes the ring — consumers drain what was
published, then see `Closed`.

**Gating** — every consumer sees every message; backpressure, never loss:

```rust
use rust_rb::spmc::{Closed, RingBuffer};

let (mut tx, mut rx) = RingBuffer::new(1024);
let mut rx2 = tx.subscribe().unwrap();   // dynamic membership

tx.push(1u64);
assert_eq!(rx.pop(), Ok(1));             // every consumer
assert_eq!(rx2.pop(), Ok(1));            // sees every message

drop(tx);                                // closes the ring
assert_eq!(rx.pop(), Err(Closed));
```

**Lossy** — the producer free-runs; a lapped consumer is repositioned and told
exactly how many messages it missed:

```rust
use rust_rb::broadcast::{PopError, RingBuffer};

let (mut tx, mut rx) = RingBuffer::<u64>::with_slack(8, 2);
for i in 0..20 {
    tx.push(i);                          // never blocks
}
// Lapped: repositioned to tail - capacity + slack = 14, exact loss count.
assert_eq!(rx.pop(), Err(PopError::Lagged { missed: 14 }));
assert_eq!(rx.pop(), Ok(14));
```

Over shared memory, the lossy rings' consumers attach **lease-free over a
read-only (`PROT_READ`) mapping** — a consumer cannot corrupt the ring no
matter how it crashes, and dropping one is just an `munmap`.

Honest performance notes (GB10 DGX Spark, Cortex-X925, pinned): a gating ring
with one consumer runs at **0.99–1.06× SPSC** — the machinery is free until
you fan out — and a straggling consumer is tracked, not amplified. The two
caught-up-regime scaling shapes were root-caused and each ships a mitigation
knob: gating N-scaling is adjacent-slot false sharing — wrap the element in
`Padded<T>` and the curve is flat (3.9/3.9/4.1 ns at N=1/2/4 vs
3.7/7.0/12.7); lossy k-coupling is the per-push tail store into spinning
readers — `Producer::set_tail_batch(8)` cuts k=1/4 from 24.6/50.1 to
15.2/19.7 ns/push at a documented visibility/crash-loss trade (defaults
unchanged: exact per-push). The broadcast rings' word-wise atomic payload
copy is permanent — the volatile alternative lost the A/B in both directions
(2.6× slower on pop at 64 B, collapsing at 256 B).

## Shared memory / IPC (feature `shm`, Linux)

All eight rings can be backed by a mapped shared region so the producer and
consumer(s) live in **different processes** — same handle types, same hot
paths:

```rust,ignore
use rust_rb::{memfd, BytesRingBuffer};
use std::os::fd::AsFd;

let fd = memfd("my-ring")?;
// SAFETY: the region is only accessed by cooperating rust-rb handles.
let (mut tx, rx) = unsafe { BytesRingBuffer::create_shm(fd.as_fd(), 1 << 20)? };
// create_shm returns (and leases) BOTH halves; release the one the peer
// will own, then hand the fd over so it can attach:
drop(rx);
// in the peer process:
// let mut rx = unsafe { BytesRingBuffer::attach_shm_consumer(fd.as_fd())? };
```

See [`examples/ipc_pair.rs`](examples/ipc_pair.rs) for a complete, runnable
parent/child version — `cargo run --example ipc_pair --features shm`.

- Works with any mappable fd (`memfd` helper included, or `shm_open`).
- The region carries a validated header (magic/version/ring kind/element
  size/architecture/capacity, cursor sanity) — accidents are rejected;
  adversarial peers are out of scope, hence the `unsafe` constructors.
- **Crash recovery**: each side holds an opaque lease token in the header
  (released on drop, guarded so stale handles can't revoke a successor).
  Recovery is an explicit, unconditional takeover — the caller asserts the
  previous holder is gone (liveness is knowledge only the application has;
  pids are namespace-relative and deliberately not used):
  `force_attach_shm_producer`/`_consumer` replace one dead side while the
  peer keeps running, `recover_shm` takes over both. Everything published is
  intact and drainable — a record only becomes visible through the
  producer's single `Release` cursor store, so a mid-write crash leaves an
  invisible partial record whose space is reused. Consumer-side recovery is
  **at-least-once**: up to the deferred-publish window (`capacity / 8`, max
  4096 bytes / 64 elements) of already-consumed messages may be delivered
  again. Verified by a child-process crash test in `tests/shm.rs`.
- Only the spin wait strategies (`CrossProcess`) are allowed: `CvWait`'s
  mutex/condvar are process-local.
- Element rings require `T: ShmItem` (plain data, valid for peer-written bit
  patterns); byte rings carry any payload.

## Benchmark

```
cargo run --release --example bench          # fixed-size ring
cargo run --release --example bench_bytes    # variable-size ring
cargo run --release --features shm --example bench_shm   # shm rings (Linux)
```

For meaningful numbers, pin the producer and consumer to dedicated cores, e.g.
`taskset -c 2,3 cargo run --release --example bench`. As in the original,
latency is dominated by the core-to-core topology of the producer/consumer pair.

### Results

Measured on an NVIDIA Grace (Neoverse V2) core pair, pinned, spin wait
strategies, saturating producer:

| Ring | Payload | Rate | Payload bandwidth |
| ---- | ------- | ---- | ----------------- |
| `RingBuffer<i64>` (cap 32 Ki) | 8 B/element | **~1.15 ns/op, ~860–900 M msgs/s** | — |
| `BytesRingBuffer` (cap 64 KiB) | 8 B/msg | ~4.9 ns/msg, ~205 M msgs/s | ~1.6 GB/s |
| `BytesRingBuffer` (cap 64 KiB) | 64 B/msg | ~13 ns/msg, ~77 M msgs/s | ~5 GB/s |
| `BytesRingBuffer` (cap 64 KiB) | 256 B/msg | ~27–46 ns/msg, ~25–37 M msgs/s | ~6–9 GB/s |
| shm `RingBuffer<i64>`, same process | 8 B/element | ~1.13 ns/op, ~880 M msgs/s | — |
| shm `RingBuffer<i64>`, **cross-process** | 8 B/element | ~1.3–1.7 ns/op, ~580–795 M msgs/s | — |
| shm `BytesRingBuffer`, **cross-process** | 8 B/msg | ~6.2 ns/msg, ~162 M msgs/s | ~1.3 GB/s |

The shm same-process row matching the heap ring is the point: the backing is
free (identical hot path, different memory). Cross-process numbers include
real scheduler/IPC noise and vary more run to run.

The two benchmarks measure different work: the fixed-size ring hands off
8-byte values (pure queue overhead — the C++ original measures ~2.4 ns/op on
the same cores), while the bytes ring copies every payload into the ring and
transfers those cache lines between the cores, so it becomes bandwidth-bound
as messages grow — per-message overhead amortizes and GB/s rises with size.

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
