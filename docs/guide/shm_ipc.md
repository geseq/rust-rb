# Shared memory / IPC

The shm-backed rings let a single-producer/single-consumer queue span **two
processes**. The producer and consumer map the *same* region — a `memfd`,
a POSIX `shm_open` object, or any mappable file descriptor — and push/pop
through it exactly as they would a heap ring. The handle types
([`Producer`](crate::Producer), [`Consumer`](crate::Consumer),
[`BytesProducer`](crate::BytesProducer),
[`BytesConsumer`](crate::BytesConsumer)) and the hot paths are unchanged; only
the constructors differ.

This feature is Linux-only and lives behind the `shm` cargo feature. Everything
below assumes `--features shm` on a 64-bit-atomic target.

For a complete, compiled walkthrough see the runnable
[`examples/ipc_pair.rs`](https://github.com/geseq/rust-rb/blob/main/examples/ipc_pair.rs):
a parent and a `fork`ed child sharing one `u64` ring.

## What shm rings are for

Use a shm ring when the producer and consumer are separate OS processes that
need a low-latency, lock-free hand-off: a market-data feed handler feeding a
strategy process, a capture process feeding a writer, a privileged reader
feeding an unprivileged worker. If both ends are threads in one process, use a
plain heap ring instead — it is simpler and has no `unsafe` surface.

The region carries a small, fixed header (magic, version, ring kind, element
size, architecture width, capacity, the two role leases, cursors) that is
validated on attach. That validation rejects *accidents* — the wrong fd, a byte
ring opened as an element ring, a cross-architecture mapping, corrupted cursors
— and nothing more.

## The trust model — why everything is `unsafe`

Every shm constructor is `unsafe`, and the reason is not memory-management
bookkeeping: it is that a shared region is, by definition, writable by every
process that maps it. Header validation catches mistakes, **not adversaries**.
Any process with the fd can scribble over the buffer, and the rings trust the
bit patterns they read out of it. By calling a constructor you assert that the
region is only ever touched by *cooperating* rust-rb handles.

Two rules follow, and both matter:

- **Do not `fork` while holding shm ring handles in a way that duplicates a
  role.** A forked child inherits bit-identical handles. Teardown is
  pid-guarded, so the child's exit will not release the parent's leases — but
  any *use* of an inherited handle violates single-producer/single-consumer and
  corrupts the ring. Either spawn the child before creating the ring, or (as in
  the example) release the role the child will own before forking and never
  touch the inherited handle in the child.
- **The region must only be mapped by cooperating peers.** There is no defense
  against a hostile mapper; that is out of scope by design.

## `ShmItem`: what may cross the boundary

Element rings are generic over `T`, but only types that implement
[`ShmItem`](crate::ShmItem) may cross a process boundary. `ShmItem` is an
`unsafe` marker for **plain data**: `Copy`, no pointers, references, or handles
that are only meaningful in one address space, and — critically — valid for
*every* bit pattern a peer might write.

The crate implements it for the integer and float primitives (`u8`..`u128`,
`i8`..`i128`, `usize`, `isize`, `f32`, `f64`) and for arrays `[T; N]` of such
types. Do **not** implement it for `bool`, `char`, most `enum`s, `NonNull`, or
anything else with a validity invariant or niche: a garbage or hostile peer
could write a bit pattern that is invalid for the type, which is instant
undefined behaviour. If you need to send a struct, send its fields as a
plain-data array or a `#[repr(C)]` POD you have vetted, or use the byte ring and
parse defensively.

The byte ring ([`BytesRingBuffer`](crate::BytesRingBuffer)) has no such
constraint — it carries opaque bytes — but you still own the parsing, and a
peer can hand you arbitrary bytes, so validate what you decode.

## Creating vs. attaching: roles and leases

There are two ways a process obtains handles:

- [`RingBuffer::create_shm`](crate::RingBuffer::create_shm) /
  [`BytesRingBuffer::create_shm`](crate::BytesRingBuffer::create_shm)
  **initialize a fresh region** and return *both* halves, claiming both role
  leases. Exactly one process should call this, once, per region.
- `attach_shm_producer` / `attach_shm_consumer` **claim a single free role** on
  an already-initialized region. Attaching a role whose lease is currently held
  fails with `io::ErrorKind::AddrInUse` — this is the cooperative
  single-producer/single-consumer guard.

Each side holds a *lease*: an opaque, random token stored in the header,
released on drop via a guarded compare-and-swap. Leases enforce cooperative
exclusivity — one producer, one consumer — and nothing else. In particular they
carry **no liveness meaning**: pids are namespace-relative, zombies look alive,
and pids get reused, so a token cannot tell you whether its holder is still
running. Whether a peer is really gone is knowledge only your application has,
which is why crash recovery (below) is always an explicit assertion, never
automatic.

### The lease hand-off across a fork

The typical single-fd, single-machine pattern: the parent creates the ring
(taking both leases), releases the role the child will own, then forks. Because
the child inherits the open fd, it can attach the freed role directly.

```no_run
use std::os::fd::AsFd;
use rust_rb::{memfd, RingBuffer};

# fn main() -> std::io::Result<()> {
let fd = memfd("demo-ring")?;
// SAFETY: fresh private memfd, only cooperating handles will touch it.
let (tx, mut rx) = unsafe { RingBuffer::<u64>::create_shm(fd.as_fd(), 1024)? };

// Give the producer role to the child: dropping frees its lease.
drop(tx);

// ... fork here; in the child, attach the freed producer role:
// SAFETY: cooperating handles; the parent kept only the consumer.
let mut tx = unsafe { RingBuffer::<u64>::attach_shm_producer(fd.as_fd())? };
tx.push(42);
assert_eq!(rx.pop(), 42);
# Ok(())
# }
```

If you get the ordering wrong and fork *before* dropping `tx`, the child
inherits a live producer handle and the attach will fail with `AddrInUse` —
which is the guard doing its job. Drop first, fork second.

## Passing the fd between processes

The ring lives wherever the fd points; getting handles into two processes is
just a matter of getting the fd into both.

- **Fork inheritance.** After `fork`, the child already has the fd. Note that
  [`memfd`](crate::memfd) sets close-on-exec, which is exactly right for a plain
  `fork` (no exec) but means that if you `fork` *and* `exec`, the fd is closed
  before the new image runs. To hand a ring to an exec'd child, clear the flag
  first with `fcntl(fd, F_SETFD, 0)` and pass the raw fd number (e.g. via an
  environment variable) so the child can rebuild an `OwnedFd` from it.
- **`SCM_RIGHTS` over a unix socket.** For processes that are not parent/child
  — a daemon accepting clients, say — send the fd as ancillary data over an
  `AF_UNIX` socket with `SCM_RIGHTS`. The receiving process gets its own fd
  referring to the same region and attaches the free role. This is the general
  mechanism when there is no fork relationship.

Either way, exactly one process calls `create_shm`; every other process
`attach_*`s.

## Wait strategies: spin only

Cross-process rings accept only [`CrossProcess`](crate::CrossProcess) wait
strategies. The three spin strategies qualify and work across address spaces
as-is:

- [`NoOpWait`](crate::NoOpWait) — tightest spin.
- [`PauseWait`](crate::PauseWait) — spin with a CPU `pause` hint.
- [`YieldWait`](crate::YieldWait) — spin yielding to the scheduler; the default
  for `create_shm` / `recover_shm`.

[`CvWait`](crate::CvWait) is **not** `CrossProcess`: its mutex and condvar live
in process-local memory and cannot coordinate a peer in another address space,
so it will not compile against the shm constructors. Pick a spin strategy
explicitly with the `*_with` constructors
([`create_shm_with`](crate::RingBuffer::create_shm_with),
`recover_shm_with`) when you want something other than the `YieldWait` default:

```no_run
use std::os::fd::AsFd;
use rust_rb::{memfd, RingBuffer};
use rust_rb::wait::PauseWait;

# fn main() -> std::io::Result<()> {
let fd = memfd("paused-ring")?;
// SAFETY: fresh private memfd, cooperating handles only.
let (mut tx, mut rx) = unsafe {
    RingBuffer::<u64, PauseWait, PauseWait>::create_shm_with(fd.as_fd(), 1024)?
};
tx.push(1);
assert_eq!(rx.pop(), 1);
# Ok(())
# }
```

## Crash recovery

Because publishing is a single `Release` store of the producer's cursor, a
process that dies mid-write leaves the region **fully consistent**: everything
already published is drainable, and the partial, unpublished record is simply
invisible — its space is reclaimed once the producer role is re-taken. There is
no torn state to repair.

Recovery is always an explicit, unconditional takeover, because — as noted
above — leases cannot tell you whether a holder is alive. By calling a recovery
constructor you assert, through the `unsafe` contract, that the previous
holder(s) are gone. A still-live holder writing concurrently would corrupt the
ring; their *late* `Drop`s, however, are harmless (lease release is guarded by
token, so a stale handle cannot revoke the successor).

- `force_attach_shm_producer` / `force_attach_shm_consumer` replace **one**
  role while the peer keeps running — single-side recovery.
- [`recover_shm`](crate::RingBuffer::recover_shm) replaces **both** roles at
  once — use it when both ends are gone.

### Producer recovery is exact; consumer recovery is at-least-once

Taking over a dead **producer** resumes publishing right after its last
published record; nothing is lost and nothing is duplicated.

Taking over a dead **consumer** is **at-least-once**. Consumption resumes at the
dead consumer's last *published* read cursor, so anything it consumed but had
not yet published is **delivered again**. The redelivery window is bounded but
not tiny:

- the deferred-publish window, `capacity / 8`, capped at **64 elements** on the
  element ring or **4096 bytes** on the byte ring; plus
- on the byte ring, any in-flight message's record and wrap padding if the
  consumer died holding a `Msg`, or the entire in-progress batch if it died
  mid-`drain` — worst case approaching a full ring.

Design consumers to tolerate this: make message handling **idempotent**, or
carry a sequence number and keep a dedup window at least as large as the bound
above (`capacity / 8` for element rings; size for up to a full ring of
redelivery on byte rings that use `Msg`/`drain`). This is the price of not
tracking liveness in the region.

```no_run
use std::os::fd::AsFd;
use rust_rb::{memfd, BytesRingBuffer};

# fn main() -> std::io::Result<()> {
let fd = memfd("recover-me")?;
// ... the previous holders have both died; we know this out-of-band ...
// SAFETY: cooperating handles; the only other holders are gone.
let (mut tx, mut rx) = unsafe { BytesRingBuffer::recover_shm(fd.as_fd())? };

// Everything published survived; drain it (some may be redelivered).
while let Some(msg) = rx.try_pop() {
    handle(&msg); // must be idempotent
}
tx.push(b"back in business");
# fn handle(_m: &[u8]) {}
# Ok(())
# }
```

## Full runnable example

[`examples/ipc_pair.rs`](https://github.com/geseq/rust-rb/blob/main/examples/ipc_pair.rs)
puts the whole lifecycle together — `memfd`, `create_shm`, the lease hand-off,
`fork`, the child's `attach_shm_producer`, and a `waitpid` round-trip assertion
— in about a hundred commented lines. Run it with:

```text
cargo run --example ipc_pair --features shm
```
