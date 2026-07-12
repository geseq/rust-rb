# Shared memory / IPC

The shm-backed rings let a queue span **multiple processes**. The producer and
consumer(s) map the *same* region — a `memfd`, a POSIX `shm_open` object, or
any mappable file descriptor — and push/pop through it exactly as they would a
heap ring. The handle types ([`Producer`](crate::Producer),
[`Consumer`](crate::Consumer), [`BytesProducer`](crate::BytesProducer),
[`BytesConsumer`](crate::BytesConsumer), and their
[`spmc`](crate::spmc)/[`broadcast`](crate::broadcast) counterparts) and the
hot paths are unchanged; only the constructors differ.

Most of this page walks the single-producer/single-consumer story; the
[multi-consumer rings](#the-multi-consumer-rings-over-shm) reuse all of it
(trust model, fd passing, leases) and add their own membership machinery,
covered at the end.

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

## Wait strategies: self-timed only

Cross-process rings accept only [`CrossProcess`](crate::CrossProcess) wait
strategies — the five self-timed ones, which make progress without a peer
notification and so work across address spaces as-is:

- [`NoOpWait`](crate::NoOpWait) — tightest spin.
- [`PauseWait`](crate::PauseWait) — spin with a CPU `pause` hint.
- [`YieldWait`](crate::YieldWait) — spin yielding to the scheduler; the default
  for `create_shm` / `recover_shm`.
- [`SleepWait`](crate::SleepWait) — fixed timed sleep per recheck.
- [`BackoffWait`](crate::BackoffWait) — spin → yield → escalating sleep.

[`CvWait`](crate::CvWait) is **not** `CrossProcess`: its mutex and condvar live
in process-local memory and cannot coordinate a peer in another address space,
so it will not compile against the shm constructors. (The multi-consumer shm
rings additionally require [`SelfTimed`](crate::SelfTimed), which every
strategy above already satisfies — see the
[configuration guide](crate::guide::configuration).) Pick a strategy
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

## The multi-consumer rings over shm

All six multi-consumer rings ([`spmc`](crate::spmc),
[`spmc_bytes`](crate::spmc_bytes), [`broadcast`](crate::broadcast),
[`broadcast_bytes`](crate::broadcast_bytes), [`anchored`](crate::anchored),
[`anchored_bytes`](crate::anchored_bytes)) have shm backings. The trust
model, fd passing, and producer lease are exactly as above; what changes is
consumer membership — and the two membership machines answer it in opposite
ways, with the anchored pair carrying both at once.

One shared difference from the SPSC rings up front: on shared memory,
**`Closed` means end-of-session, not end-of-ring**. A graceful producer drop
sets the closed flag and consumers drain to `Closed` as on the heap — but a
new producer attach *resets* the flag and the ring is open again; live
consumers simply see the new session. (A *crashed* producer never sets the
flag at all — detecting a dead peer remains your application's job, exactly
as in the lease discussion above.)

### Gating rings: a fixed consumer table

Heap membership is unbounded, but a mapped layout cannot grow, so the gating
shm constructors take a **`max_consumers`** argument at creation — a physical
constraint, not a design choice. The region carries a consumer *table* of
that many slots; each consumer holds a per-slot lease and publishes its read
cursor there:

```no_run
use std::os::fd::AsFd;
use rust_rb::{memfd, spmc};

# fn main() -> std::io::Result<()> {
let fd = memfd("orders")?;
// SAFETY: fresh private memfd, cooperating handles only.
let (mut tx, mut rx) =
    unsafe { spmc::RingBuffer::<u64>::create_shm(fd.as_fd(), 1024, 8)? };

// In other processes: claim a free table slot (up to 8 total here).
// SAFETY: cooperating handles only.
let mut rx2 = unsafe { spmc::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd())? };
tx.push(7);
assert_eq!(rx.pop(), Ok(7));
assert_eq!(rx2.pop(), Ok(7));
# Ok(())
# }
```

`attach_shm_consumer` is the shm face of `subscribe`: the join point is the
producer's published cursor at claim time. It fails with `AddrInUse` when the
table is full and `BrokenPipe` when the ring is closed.

**Zombie consumers and `force_detach_consumer`.** Gating means a dead
consumer's frozen cursor eventually blocks the producer forever — the
lossless contract has no way to shrug it off. The escape hatch is
`force_detach_consumer(fd, slot, epoch)`, a *compare-and-retire*: the slot is
retired **iff** it is still held by the occupancy the caller diagnosed dead
(every claim bumps the slot's epoch, so if the slot was gracefully freed and
re-claimed by a healthy consumer in the meantime the epochs differ and the
call fails with `InvalidInput` instead of retiring the living). A retired
slot's cursor drops out of the producer's next rescan, un-gating the ring,
and the slot is **never re-issued** (so a straggling zombie cannot be
confused with a new consumer) until `recover_shm` resets the whole table.
Each consumer can report its own `(slot, epoch)` pair via
`shm_slot_epoch()` — publish it at startup so an operator or watchdog holds
the exact proof the retire call takes.

`force_detach_consumer` sits in the same trust register as the
`force_attach_*` constructors: by calling it you assert the slot's holder is
**dead**. If it is actually alive, the ring itself stays consistent (the
zombie's cursor flushes land on a retired slot nothing reads), but the
zombie's *reads* lose all gating protection — the producer may overwrite data
it is still reading.

**Full recovery.** `recover_shm` on a gating ring force-takes the producer
role *and* resets the consumer table (leases zeroed, every slot reissuable at
a bumped epoch). The returned consumer resumes at the slowest
previously-registered cursor, so recovery is at-least-once across all dead
consumers — the idempotence advice above applies with the table-wide bound.

### Lossy rings: read-only, lease-free consumers

The broadcast shm rings take the opposite stance: **consumers keep no shared
state at all**. `create_shm` returns only the producer (the ring's only role
and only lease); consumers attach lease-free, in unbounded numbers, and each
maps the region **read-only** (`PROT_READ`):

```no_run
use std::os::fd::AsFd;
use rust_rb::{broadcast, memfd};

# fn main() -> std::io::Result<()> {
let fd = memfd("prices")?;
// SAFETY: fresh private memfd, cooperating handles only.
let mut tx = unsafe { broadcast::RingBuffer::<u64>::create_shm(fd.as_fd(), 1024)? };

// Any number of consumers, each over its own read-only mapping.
// SAFETY: cooperating handles only.
let mut rx = unsafe { broadcast::RingBuffer::<u64>::attach_shm_consumer(fd.as_fd())? };
tx.push(42);
assert_eq!(rx.pop(), Ok(42));
# Ok(())
# }
```

The read-only mapping is more than hygiene — it is **fault isolation**. A
consumer *cannot* corrupt the ring, no matter how it crashes: any store a bug
introduces into the consumer path is a deterministic SIGSEGV rather than
silent shared-state damage. Dropping (or losing) a consumer is just an
`munmap`; there is nothing to clean up, no lease to leak, no cursor to gate
anyone. This is the deployment shape for one trusted publisher feeding many
untrusted-ish readers.

**Producer crash recovery** is `force_attach_shm_producer` (`recover_shm` is
the same operation here — with no consumer state there is nothing else to
reset). Everything published stays drainable throughout, and running
consumers need no coordination: they self-heal through the same validation
they always run. On the element ring that is the per-slot generation check.
On the byte ring, the new producer additionally *heals* a mid-push crash at
attach time: it floors its declared-write frontier at the dead producer's
(so the bytes the dead push destroyed stay permanently outside every
consumer's validation window) and repairs the lap-recovery jump target to the
last committed record. You call one constructor; the healing is automatic.

### Mixed rings: an anchor table *and* lease-free observers

The anchored pair ([`anchored`](crate::anchored),
[`anchored_bytes`](crate::anchored_bytes)) is the composition of the two
membership models on one region — because it composes their two contracts. Its
[`create_shm`](crate::anchored::RingBuffer::create_shm) takes the same
**`max_anchors`** sizing argument as the gating constructors, `create_shm(fd,
capacity, max_anchors)`, and lays out a fixed **anchor table** of that many
slots: each required anchor claims one with
[`attach_shm_anchor`](crate::anchored::RingBuffer::attach_shm_anchor),
publishing a lease + cursor exactly as a gating consumer does, and gates the
producer from its join point on. A stuck anchor is the same liability as a
zombie gating consumer and is reclaimed the same way, with
[`force_detach_anchor`](crate::anchored::RingBuffer::force_detach_anchor)`(fd,
slot, epoch)` — the identical compare-and-retire on a `(slot, epoch)` pair.

Observers, meanwhile, are exactly the lossy rings' consumers:
[`attach_shm_observer`](crate::anchored::RingBuffer::attach_shm_observer) maps
the region **read-only** (`PROT_READ`), keeps no shared state, and joins in
unbounded numbers — never gating anyone, costing the producer nothing, and
self-healing through the seqlock validation it always runs. So the anchored
region is the gating table for its required readers and the lease-free
read-only fan-out for everyone else, side by side.
[`recover_shm`](crate::anchored::RingBuffer::recover_shm) resets the anchor
table exactly like the gating rings; observers need nothing.

```text
kind        membership            consumer state in region     consumer mapping
gating      max_consumers, fixed  lease + cursor per slot      read-write
lossy       unbounded             none                         read-only (PROT_READ)
mixed       max_anchors + unbounded  anchors: lease + cursor; observers: none  anchors: read-write; observers: read-only
```

## Full runnable example

[`examples/ipc_pair.rs`](https://github.com/geseq/rust-rb/blob/main/examples/ipc_pair.rs)
puts the whole lifecycle together — `memfd`, `create_shm`, the lease hand-off,
`fork`, the child's `attach_shm_producer`, and a `waitpid` round-trip assertion
— in about a hundred commented lines. Run it with:

```text
cargo run --example ipc_pair --features shm
```
