# API usage — which method for which job

`rust-rb` gives you two rings — an **element ring** ([`RingBuffer<T>`](crate::RingBuffer),
split into a [`Producer<T>`](crate::Producer) / [`Consumer<T>`](crate::Consumer)) and a
**byte ring** ([`BytesRingBuffer`](crate::BytesRingBuffer), split into a
[`BytesProducer`](crate::BytesProducer) / [`BytesConsumer`](crate::BytesConsumer)). Each
side exposes several methods that do "the same thing" but differ in *blocking*,
*copying*, and *batching*. This guide is a decision table plus a worked snippet
per choice. For the sharp behavioural edges those methods expose, see the
[semantics guide](crate::guide::semantics).

## Element ring `RingBuffer<T>`

| You want to…                                     | Producer                                          | Consumer                                                    |
| ------------------------------------------------ | ------------------------------------------------- | ---------------------------------------------------------- |
| enqueue, waiting if full                         | [`push`](crate::Producer::push)                   | —                                                          |
| enqueue, or get the value back if full           | [`try_push`](crate::Producer::try_push)           | —                                                          |
| dequeue, waiting if empty                        | —                                                 | [`pop`](crate::Consumer::pop)                              |
| dequeue, or `None` if empty                      | —                                                 | [`try_pop`](crate::Consumer::try_pop)                     |
| read the next item *without moving it*           | —                                                 | [`pop_ref`](crate::Consumer::pop_ref) / [`try_pop_ref`](crate::Consumer::try_pop_ref) |
| construct the value directly into the slot       | [`claim`](crate::Producer::claim) / [`try_claim`](crate::Producer::try_claim) | —                                             |

## Byte ring `BytesRingBuffer`

| You want to…                                     | Producer                                          | Consumer                                                    |
| ------------------------------------------------ | ------------------------------------------------- | ---------------------------------------------------------- |
| send bytes you already have, waiting if full     | [`push`](crate::BytesProducer::push)              | —                                                          |
| send bytes, or `false` if no space               | [`try_push`](crate::BytesProducer::try_push)      | —                                                          |
| reserve `len` bytes and write them in place       | [`claim`](crate::BytesProducer::claim) / [`try_claim`](crate::BytesProducer::try_claim) | —                                     |
| read the next message, waiting if empty          | —                                                 | [`pop`](crate::BytesConsumer::pop)                        |
| read the next message, or `None`                 | —                                                 | [`try_pop`](crate::BytesConsumer::try_pop)               |
| drain every currently-available message, fast    | —                                                 | [`drain`](crate::BytesConsumer::drain)                   |

---

## `push` vs `try_push` — block or apply backpressure

[`push`](crate::Producer::push) blocks the calling thread until a slot is free
(using the ring's wait strategy). Reach for it when the producer has nothing
better to do than wait, and dropping data is not an option.

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::new(16);
tx.push(1u64);
assert_eq!(rx.pop(), 1);
```

[`try_push`](crate::Producer::try_push) never blocks. On success it returns
`Ok(())`; if the ring is full it hands the value **back** to you as `Err(value)`
so nothing is lost — you decide whether to retry, drop, or account for it. This
is the backpressure-aware path for latency-sensitive producers.

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::new(16);
assert!(tx.try_push(1u64).is_ok());
assert_eq!(rx.try_pop(), Some(1));
assert_eq!(rx.try_pop(), None); // empty, but we did not block
```

The byte-ring equivalents mirror this: [`push`](crate::BytesProducer::push)
blocks (and panics if the message exceeds
[`max_message_len`](crate::BytesProducer::max_message_len)), while
[`try_push`](crate::BytesProducer::try_push) returns `false` when there is no
room rather than blocking.

## `pop` vs `try_pop` vs `pop_ref` — copy out or read in place

[`pop`](crate::Consumer::pop) blocks until an item is available and **moves it
out** of the ring by value. [`try_pop`](crate::Consumer::try_pop) is the
non-blocking variant returning `Option<T>`. Use these when you want to own the
value and the copy is cheap (small `Copy` types) or you were going to take
ownership anyway — this is the simplest and often the fastest drain path.

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::new(16);
tx.push(42u64);
assert_eq!(rx.pop(), 42);
```

[`pop_ref`](crate::Consumer::pop_ref) (blocking) and
[`try_pop_ref`](crate::Consumer::try_pop_ref) (non-blocking) instead hand you a
[`PopRef`](crate::spsc::PopRef) that `Deref`/`DerefMut`s to the `T` **still living in
the ring** — zero copy. The slot is released for reuse when the `PopRef` is
dropped. Prefer this when `T` is large and you only need to read (or mutate)
a few fields before discarding it, so you never pay for the move.

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::<[u8; 8]>::new(16);
tx.push([1, 2, 3, 4, 5, 6, 7, 8]);
let m = rx.pop_ref();      // borrows the slot, no copy
let first = m[0];          // read in place
assert_eq!(first, 1);
drop(m);                   // slot released here
```

Because the slot is only released on drop, dropping a `PopRef` matters — see the
[semantics guide](crate::guide::semantics) for what `mem::forget`-ing one does
(re-delivery). Note that holding a `PopRef` keeps the read cursor un-advanced,
so the *producer's* [`len`](crate::Producer::len)/[`is_full`](crate::Producer::is_full)
can transiently over-count until you drop it; the consumer-side counters are
always exact.

## `claim` / `commit` / `commit_init` — construct in place

When constructing a `T` is expensive or you would otherwise build it on the
stack and copy it in, reserve the slot first with
[`claim`](crate::Producer::claim) (blocking) or
[`try_claim`](crate::Producer::try_claim) (non-blocking) and write straight into
the ring. This avoids the extra move that [`push`](crate::Producer::push) does.

The safe finish is [`commit`](crate::spsc::WriteSlot::commit), which behaves exactly
like a `push` into the reserved slot:

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::new(16);
let slot = tx.claim();     // reserve
slot.commit(7u64);         // fill + publish
assert_eq!(rx.pop(), 7);
```

For the truly zero-copy path, write through
[`uninit`](crate::spsc::WriteSlot::uninit) (a `&mut MaybeUninit<T>`) and finish with
[`commit_init`](crate::spsc::WriteSlot::commit_init):

```rust
use rust_rb::RingBuffer;
let (mut tx, mut rx) = RingBuffer::<[u8; 8]>::new(16);
let mut slot = tx.claim();
slot.uninit().write([0u8; 8]);       // initialise the slot yourself
unsafe { slot.commit_init() };       // SAFETY: slot is fully initialised
assert_eq!(rx.pop(), [0u8; 8]);
```

[`commit_init`](crate::spsc::WriteSlot::commit_init) is `unsafe`: **you** promise the
slot has been fully initialised via `uninit().write(...)`. Committing an
uninitialised (or partially initialised) slot is undefined behaviour. Use
`commit` unless you have measured the copy and need it gone.

## Byte ring: `push` vs `claim(len)` and `pop` vs `drain`

### Producing bytes

[`push`](crate::BytesProducer::push) copies a slice you already have into the
ring. When you would otherwise serialise into a temporary buffer and then copy,
reserve the exact length with [`claim`](crate::BytesProducer::claim) instead and
serialise directly into the ring's memory. The returned
[`WriteSlot`](crate::spsc_bytes::WriteSlot) `Deref`s to a `&mut [u8]` of exactly
`len` bytes; publish it with [`commit`](crate::spsc_bytes::WriteSlot::commit).

```rust
use rust_rb::BytesRingBuffer;
let (mut tx, mut rx) = BytesRingBuffer::new(1024);
let mut slot = tx.claim(4);           // reserve 4 bytes
slot.copy_from_slice(b"tick");        // write in place
slot.commit();                        // publish
assert_eq!(&*rx.pop(), b"tick");
```

### Consuming bytes

[`pop`](crate::BytesConsumer::pop) returns one [`Msg`](crate::spsc_bytes::Msg)
borrowing the next message in place; it `Deref`s to `&[u8]` and releases the slot
on drop. Good for message-at-a-time processing.

[`drain`](crate::BytesConsumer::drain) is the **catch-up fast path**: it invokes
your closure once per currently-available message and publishes the read cursor a
**single** time at the end, rather than once per message. When a consumer has
fallen behind and many messages are queued, this amortises the synchronisation
cost across the whole batch. It returns how many messages it handled.

```rust
use rust_rb::BytesRingBuffer;
let (mut tx, mut rx) = BytesRingBuffer::new(1024);
tx.push(b"a");
tx.push(b"bb");
let n = rx.drain(|bytes| {
    // handle each message; borrowed from the ring, do not retain past the call
    let _ = bytes;
});
assert_eq!(n, 2);
```

Prefer [`pop`](crate::BytesConsumer::pop) when you react to messages one by one
or need to stop mid-stream; prefer [`drain`](crate::BytesConsumer::drain) when
throughput matters and you want to consume everything that is ready in one go.
As with `PopRef`, a `Msg` releases its slot on drop — see the
[semantics guide](crate::guide::semantics) for the `mem::forget` re-delivery
edge. Holding a `Msg` (or draining) leaves the read cursor unpublished, so the
*producer* can transiently see the ring as fuller than it is; the byte
consumer's own view (via [`is_empty`](crate::BytesConsumer::is_empty)) is exact.
