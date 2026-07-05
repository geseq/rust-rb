Most of `rust-rb` behaves exactly as a queue should: you push on one end and
pop on the other. A handful of behaviours, though, surprise people the first
time they hit them — usually because the ring trades a little bookkeeping
accuracy for a lot of throughput, or because "zero-copy" means a guard object
is quietly in charge of when a slot is released. This page collects those sharp
edges. Every one of them is a deliberate, memory-safe design choice; the goal
here is that none of them bites you by surprise.

# Producer-side `len`/`is_full` are approximate; the consumer side is exact

The single most surprising counter behaviour: **the producer's view of how full
the ring is can be stale, but only ever in the safe direction.**

Under load the consumer does not publish its read cursor after every element.
While the queue is *backed up*, it batches those publishes — one per
`capacity / 8` elements (capped at 64 elements, or 4096 bytes for the byte
ring) — because a per-element publish under contention lets the polling
producer steal the cursor's cache line between every store, collapsing both
threads into a lockstep cache-line ping-pong that roughly *quarters* end-to-end
throughput. Deferring the publish amortises the cross-core transfer and lets
the producer push in bursts. (This is the "adaptive publish" rule; the
[performance guide](crate::guide::performance) covers the mechanism in full.)

The consequence for counters: the producer always reads the freshest read
cursor the consumer has *published* (a fresh `Acquire` load of the shared
atomic) — but while the ring is backed up the consumer defers publishing its
progress to that atomic, so the value the producer sees lags reality by up to
one publish window. The staleness lives in the deferred publish, not in a
producer-local cache. So:

- [`Producer::len`](crate::Producer::len) may **over-count** by up to
  `capacity / 8` (max 64) already-consumed elements. It is exact whenever the
  consumer has caught up, and it **never under-counts**.
- [`Producer::is_full`](crate::Producer::is_full) may transiently report `true`
  for a ring that has in fact drained. It **never reports `false` for a
  truly-full ring**.

The skew is always conservative — the producer can believe the ring is fuller
than it is, never emptier — so you can never lose data by trusting it. But you
*can* stall a producer that thinks it is out of room when it is not.

The consumer side has no such problem: [`Consumer::len`](crate::Consumer::len),
`is_empty()`, and `is_full()` all read the consumer's *private* read cursor,
which is always current. They are exact.

The practical mistake is to gate a push on `is_full()` or `len()`:

```rust
use rust_rb::RingBuffer;

let (mut producer, consumer) = RingBuffer::<u64>::new(1024);

// Don't do this: `is_full()` can lie (transiently) in the "true" direction,
// so this loop can spin-wait on a ring that actually has room.
//
//     while producer.is_full() { /* back off */ }
//     producer.push(value);

// Do this: let `try_push` tell you the truth. It checks against the freshest
// cursor it can see and hands the value back only when the ring is really full.
match producer.try_push(42) {
    Ok(()) => { /* enqueued */ }
    Err(value) => { /* genuinely full right now; `value` returned to you */
        let _ = value;
    }
}
let _ = consumer;
```

**Practical rule:** never use producer-side `len`/`is_full` as an exact gate —
treat them as hints, and drive backpressure off the `Result` from
[`try_push`](crate::Producer::try_push). Consumer-side counters are exact and
safe to gate on.

# `mem::forget` on a `PopRef` or `Msg` re-delivers — it is not a leak

Both zero-copy read guards, [`PopRef`](crate::spsc::PopRef) and
[`Msg`](crate::spsc_bytes::Msg), keep the element or message *in the ring*.
The read cursor only advances when the guard's `Drop` runs. That is what makes
the read zero-copy: nothing moves until you let go.

So if you `mem::forget` the guard, you skip that `Drop`, the cursor never
advances, and **the very same element is delivered again** by the next `pop`,
`pop_ref`, or `drain`. This is completely memory-safe — no double-free, no
use-after-free — but it is *re-delivery*, not a leak. If the payload carries
side-effectful semantics (an order to place, a command to run), re-processing
it is now on you.

```rust,no_run
use rust_rb::RingBuffer;
use std::mem;

let (mut producer, mut consumer) = RingBuffer::<u64>::new(1024);
producer.push(7);

{
    let guard = consumer.pop_ref();
    assert_eq!(*guard, 7);
    mem::forget(guard); // cursor does NOT advance
}

// The same element arrives a second time:
let again = consumer.pop_ref();
assert_eq!(*again, 7);
drop(again); // normal drop -> cursor advances, slot released to the producer
```

The normal path — letting the guard fall out of scope — is what advances the
cursor and hands the slot back. `mem::forget` is the only way to *not* consume
after taking a guard, and it is occasionally useful (peek-and-retry), but it is
never automatic and never accidental.

**Practical rule:** a guard consumes on drop; `mem::forget` means "deliver this
again." Only reach for it when re-delivery is exactly what you want.

# Panics while holding a `PopRef`, or inside a `drain` closure, are safe

Because the read cursor is advanced by a drop guard (for
[`Consumer::pop_ref`](crate::Consumer::pop_ref)) or at a single publish point
(for [`BytesConsumer::drain`](crate::BytesConsumer::drain)), a panic in your
code leaves the ring in a consistent state. Unwinding runs the guard on the way
out, so the ring is never left half-updated.

For `drain` the guarantee is precise, and worth stating exactly:

- The read cursor is advanced **past each record before your closure sees it**,
  so a record counts as consumed even if the closure unwinds on it.
- The cursor is published to the producer **exactly once**, at the end of the
  batch, via a drop guard that runs on both the normal exit *and* an unwind out
  of the closure.
- Therefore delivery is **at-most-once within the process**: an unwound `drain`
  never re-delivers messages it already handed to the closure, and the producer
  sees the freed space published once the batch (or its unwind) completes.

```rust,no_run
use rust_rb::BytesRingBuffer;

let (mut producer, mut consumer) = BytesRingBuffer::new(4096);
producer.push(b"order-1");
producer.push(b"order-2");

// If this closure panicked on "order-2", "order-1" would still count as
// consumed, the cursor would still publish exactly once on the way out, and a
// re-run would resume *after* the messages already delivered.
let n = consumer.drain(|msg| {
    let _ = msg.len();
});
assert_eq!(n, 2);
```

One caveat about the boundary of "within the process": the once-at-the-end
publish means the producer sees *no* space freed until a `drain` finishes, so
keep the closure short (or prefer `pop` when the producer is starved for room).
And across a genuine *process crash* mid-`drain` — a shared-memory concern only
— none of the in-progress batch was published, so crash recovery re-delivers
the whole interrupted drain; see the
[shared-memory guide](crate::guide::shm_ipc) for that at-least-once story.

**Practical rule:** you may panic freely inside a `PopRef` scope or a `drain`
closure; the ring stays consistent and never silently re-delivers within the
process. Just keep `drain` closures short.

# There is exactly one producer and one consumer — the type system enforces it

`rust-rb` is a single-producer, single-consumer ring, and the SPSC invariant is
not a documentation promise you have to uphold — it is enforced by the types.
The two halves are `Send`, so each can move to its own thread, but they are
deliberately **not `Clone`**. There is no way to manufacture a second
`Producer` or a second `Consumer` from a ring, so two writers or two readers
simply cannot exist.

```rust,no_run
use rust_rb::RingBuffer;
use std::thread;

let (mut producer, mut consumer) = RingBuffer::<u64>::new(1024);

// Each half moves to its own thread. `Send`, but not `Clone`:
// `producer.clone()` would not compile.
let writer = thread::spawn(move || {
    for i in 0..100 {
        producer.push(i);
    }
});
let reader = thread::spawn(move || {
    for _ in 0..100 {
        let _ = consumer.pop();
    }
});
writer.join().unwrap();
reader.join().unwrap();
```

If you need more than one producer or consumer, you need a different data
structure — this ring will not give you one. If you need the ring to span two
*processes* rather than two threads, that is the shared-memory story, which
still keeps one producer and one consumer, one per process; see the
[shared-memory guide](crate::guide::shm_ipc).

**Practical rule:** move one half to each thread; there is no supported way to
have two of either side, and the compiler will stop you from trying.

# Wrap-around is invisible, but cursor indices are opaque

Internally the ring's cursors are monotonic counters that keep incrementing and
wrap around the `usize` range; they are never reset to zero. Every occupancy
check compares the **wrapped difference** `write.wrapping_sub(read)` — the true
number of units in flight — never the absolute cursor values. That is why
correctness holds even when a cursor rolls over the top of `usize` (on a 32-bit
target this happens after 2^32 units; on 64-bit it is effectively unreachable).

You never see any of this: the public API exposes occupancy through
`len()`/`is_empty()`/`is_full()`/`capacity()`, all of which are already
computed from wrapped differences. The only thing to take away is that the
counters the ring hands you are *quantities*, not positions — there are no raw
indices in the public API, and nothing you can compare with `<` to reason about
ordering across the wrap.

**Practical rule:** treat ring positions as opaque; use the occupancy counters
the API gives you and never assume a monotonic index you can order with `<`.
