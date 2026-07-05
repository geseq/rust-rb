# Performance tuning

This ring is already fast by construction — masked monotonic indices, cursor
caching, single-writer `Release`/`Acquire` publishes, and cache-line padding.
Those are baked into the type; you do not tune them. What *you* control is the
handful of levers that decide whether the design's headroom actually shows up
on your machine: **which two cores** the endpoints run on, **which wait
strategy** they use, and **how you measure**. This guide covers those, and
explains the one observable quirk the fast path introduces.

If numbers below look surprising, remember the single most important caveat:
**absolute latency and throughput vary enormously by core pair and by machine.**
The SPSC figures quoted here were measured on a specific NVIDIA Grace
(Neoverse V2) core pair; the multi-consumer figures (section 6) on a GB10 DGX
Spark (Cortex-X925) — each section names its box. Treat them as *shapes to
expect*, never as targets to hit on different hardware.

## 1. Core pinning dominates everything else

Core-to-core hand-off latency is a property of the **specific pair of cores**,
not of the CPU as a whole. Two cores that share an L2 or sit on the same cluster
talk far faster than two cores on opposite sockets or across a mesh. Because a
saturated SPSC ring is essentially a cache line bouncing between the producer
and the consumer, the topology of that one pair sets your ceiling. Pinning both
endpoints to a well-chosen sibling pair is the single biggest lever you have —
larger than any wait-strategy or capacity change.

The benchmarks pin explicitly. They take two core ids on the command line and
hand each thread to a `pin` helper, which on Linux/Android calls
`libc::sched_setaffinity` (and asserts the pin succeeded, so an offline core or
a restrictive cgroup cpuset fails loudly rather than silently reporting
unpinned numbers as pinned):

```no_run
use rust_rb::spsc::RingBuffer;
use rust_rb::wait::PauseWait;

// producer_core / consumer_core chosen for a good sibling pair on this machine
let (producer_core, consumer_core) = (18usize, 19usize);

let (mut tx, mut rx) = RingBuffer::<i64, PauseWait, PauseWait>::with_wait_strategies(32_768);

let consumer = std::thread::spawn(move || {
    pin(consumer_core);
    for _ in 0..100_000_000i64 {
        let _ = rx.pop();
    }
});

pin(producer_core);
for i in 0..100_000_000i64 {
    tx.push(i);
}
consumer.join().unwrap();

# #[cfg(any(target_os = "linux", target_os = "android"))]
fn pin(core: usize) {
    // The examples' `common::pin`: zero a cpu_set_t, CPU_SET(core), then
    // sched_setaffinity(0, ..). Asserts the syscall returned 0.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}
# #[cfg(not(any(target_os = "linux", target_os = "android")))]
# fn pin(_core: usize) {}
```

The bundled `bench` example wires this up for you: pass the two core ids as
positional arguments.

```console
$ cargo run --release --example bench 18 19
pinning producer -> core 18, consumer -> core 19
```

You can also let the OS place the threads and pin the whole process with
`taskset`, which the README uses:

```console
$ taskset -c 2,3 cargo run --release --example bench
```

`taskset` confines the process to a cpuset; passing explicit ids to the example
additionally pins each *thread* to a *specific* core, which is what you want
when comparing pairs. Try several pairs — adjacent ids are not always siblings —
and keep the one that measures best on your box.

## 2. The adaptive read-cursor publish

The consumer does not blindly publish its read cursor after every element. It
adapts to whether the producer is actually watching (see the `advance` logic in
the cursor engine):

- **Caught up (queue looks empty after a consume):** publish immediately, one
  `Release` store per element. This is the uncontended, latency-critical regime
  — the line nobody else is polling — and it matches the C++ original's
  per-element behaviour exactly.
- **Backed up (producer blocked on a full ring):** publish only once per
  batch — `capacity / 8` elements, capped at 64 elements (4096 bytes for the
  byte ring). When the ring is full the producer is *spinning on the read
  cursor's cache line*; a per-element publish there lets it steal that line
  between every single store, collapsing both threads into a lockstep
  line ping-pong. Deferring amortizes the line transfer and lets the producer
  drain the backlog in bursts.

The measured effect is large: on a saturated Grace (Neoverse V2) core pair with
a spin wait strategy, this took the fixed-size ring from roughly **135 M** to
roughly **860 M msgs/s** — about twice the C++ original's best on the same
cores. The batch clamp bounds any deferral, and because *catching up always
flushes*, the consumer can never wait, report empty, or drop with progress left
unpublished.

There is one visible consequence, and it is a feature of the fast path rather
than a bug: **while (and only while) the queue is backed up, producer-side
[`Producer::len`](crate::Producer::len) and [`Producer::is_full`](crate::Producer::is_full)
may transiently over-count** by up to the deferral bound, because the producer
has not yet seen the consumer's most recent (deferred) progress. Consumer-side
views are always exact, and the guarantees above still hold. If you rely on
these counters, read the [semantics guide](crate::guide::semantics) for the
precise contract before treating them as authoritative under backpressure.

## 3. False-sharing design (why the two cursors never fight)

The two shared cursors — the producer's write cursor and the consumer's read
cursor — are each wrapped in an internal cache-padded cell that
pads them to a full *destructive interference distance*: **128 bytes on x86-64
and AArch64**, and 64 bytes on other targets. 128 is deliberate: x86-64 pulls
cache lines in aligned pairs (the adjacent-line/spatial prefetcher) and recent
ARM cores (Neoverse, Apple) prefetch pairs or use 128-byte granules outright,
so two atomics only 64 bytes apart can still ping-pong even though they are on
different 64-byte lines. Padding to 128 guarantees the write cursor and read
cursor never share a line or a prefetched line pair — the same choice crossbeam
and folly make.

On top of the padding, each side keeps a **private cached copy** of the peer's
cursor. The producer reloads the shared read cursor only when the ring *looks*
full; the consumer reloads the shared write cursor only when the ring *looks*
empty. In steady state neither side touches the other's atomic at all, so there
is no cross-core traffic on the hot path — the padding only has to matter at the
moments the caches genuinely need refreshing.

## 4. Wait strategy: throughput vs. latency

The wait strategy decides what a blocked endpoint *does* while it waits, and it
trades a burned core against wake-up latency. The full matrix lives in the
[configuration guide](crate::guide::configuration); do not duplicate it — pick
from it. The performance-relevant summary:

- **Pinned, latency-critical pairs:** use a spinning strategy —
  [`NoOpWait`](crate::NoOpWait) (busy-loop) or [`PauseWait`](crate::PauseWait)
  (busy-loop with a CPU `pause`/`yield` hint). These keep the consumer hot on
  the line so a newly published element is seen in nanoseconds. This is what the
  benchmarks use, and what the ~860–900 M msgs/s figures assume. The cost is a
  fully occupied core per spinning endpoint — which is exactly why you pin
  first.
- **When you cannot burn a core:** use [`CvWait`](crate::CvWait), which parks
  the thread and is woken by the publisher. You give up the tightest latency,
  but the core is free for other work. Prefer this whenever the ring is not the
  program's hottest path.

`YieldWait` sits between the two (spins, then yields to the scheduler); the
benchmarks include it as a middle data point.

## 5. A reproducible benchmarking recipe

Both benches spawn a producer and a consumer thread, push a fixed count through,
and print `ns/op` (or `ns/msg`) plus a rate. They accept the pin pair as two
positional core ids — **producer core first, consumer core second** — as also
noted in the [configuration guide](crate::guide::configuration):

```console
$ cargo run --release --example bench 18 19
pinning producer -> core 18, consumer -> core 19
SPSC_Pause      1.15 ns/op    865.0 M msgs/s     115 ms
SPSC_Yield      ...
SPSC_NoOp       ...
```

```console
$ cargo run --release --example bench_bytes 18 19
pinning producer -> core 18, consumer -> core 19
BYTES_Pause_pop      8 B    4.90 ns/msg    1.63 GB/s
BYTES_Pause_drain    8 B    ...
BYTES_NoOp_drain     8 B    ...
```

Both examples run the whole battery **twice** and you should read the *second*
pass: the first warms caches and lets frequency governors settle, so the second
is the representative (warm) number. Omitting the two ids runs unpinned, which
the examples announce and which is only useful as a sanity check, never as a
reported result.

`--release` is mandatory, and not only for optimization level. This crate's
release profile sets `lto = true`, `codegen-units = 1`, and `panic = "abort"`,
so the `Release`/`Acquire` stores inline to plain moves on x86-64 and the whole
hot loop collapses to a handful of instructions. A debug build measures the
compiler's inability to inline, not the ring; the numbers are meaningless.

When you report results, **name the machine and the exact core pair**, because
that is what the numbers actually describe. Follow the README's table shape:

```text
Machine: <cpu model>, cores <a>,<b> (siblings?), pinned, spin wait

| Ring                          | Payload      | Rate                    |
| ----------------------------- | ------------ | ----------------------- |
| RingBuffer<i64> (cap 32 Ki)   | 8 B/element  | ~1.15 ns/op, ~865 M/s   |
| BytesRingBuffer (cap 64 KiB)  | 64 B/msg     | ~13 ns/msg, ~5 GB/s     |
```

The fixed-size and byte benches measure different work: the fixed-size ring
hands off 8-byte values (pure queue overhead), while the byte ring copies every
payload into the ring and moves those cache lines between cores, so it becomes
bandwidth-bound as messages grow. Do not compare their `ns` figures directly.

Above all: **reproduce on your own hardware.** The Grace numbers here exist to
show you the *shape* of a good result — sub-2 ns/op on the fixed ring, rate
rising with payload size on the byte ring — not a bar to clear. A different core
pair, on the same chip, can differ by more than 2x.

## 6. Multi-consumer rings

The multi-consumer numbers were measured on a **different machine** from
everything above: a GB10 DGX Spark (Cortex-X925 cores, pinned), where the
SPSC yield-strategy baseline is 2.36 ns/op — *not* the NVIDIA Grace
(Neoverse V2) box the SPSC figures elsewhere in this guide come from. Do not
mix the two sets. The benches are `examples/bench_spmc.rs` and
`examples/bench_broadcast.rs`; as always, read the second (warm) pass.

What held, on that box:

- **Gating N=1 parity.** A [`spmc`](crate::spmc) ring with a single consumer
  runs at 0.99–1.06× the SPSC ring across wait strategies — you do not pay
  for the multi-consumer machinery until you use it.
- **Straggler isolation.** With several fast consumers plus one rate-limited
  one, the producer tracked the straggler (46.17 ns/op against the
  straggler's 45.98) instead of degrading below it — the selective
  cursor-refresh design doing its job.
- **Lossy lap accounting.** A permanently lagging broadcast consumer costs
  the producer ~7.6 ns/push, with exact loss accounting throughout
  (accepted + missed = pushed).
- **Copy strategy (A/B, resolved).** The broadcast rings' word-wise atomic
  ("strict") payload copy is permanent: the volatile-copy alternative
  measured **2.6× slower on pop at 64 B payloads and collapsed at 256 B**,
  so the strict copy won in both directions.

What did not (known, tracked, correctness unaffected):

- **Gating caught-up N-scaling is not flat**: with all consumers caught up
  and spinning, producer cost rose +24% at N=2 and +114% at N=4
  (`rust-rb-vio` in the issue tracker).
- **Lossy caught-up coupling**: caught-up *spinning* broadcast consumers
  couple the producer — 4.5 ns/push alone, 25.3 with one spinning reader,
  65.5 with four — while a lagging reader decouples it back to 8.7
  (`rust-rb-6l0`). The coupling is specific to the caught-up *tight-spinning*
  regime (the failing bench polled with raw `try_pop` loops); whether
  gentler consumer strategies dampen it is one of the open questions on the
  issue.

Both findings are performance-shape issues only: accounting stayed exact, no
torn reads were accepted, and nothing was lost under keep-up in any of the
failing configurations.
