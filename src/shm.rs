//! Shared-memory-backed rings (Linux, feature `shm`).
//!
//! Backs [`RingBuffer`] and [`BytesRingBuffer`] with a mapped
//! region (memfd, `shm_open`, or any mappable fd) so the producer and
//! consumer can live in **different processes**. The handles returned are
//! the ordinary [`Producer`]/[`Consumer`]/[`BytesProducer`]/[`BytesConsumer`]
//! types — the hot paths are identical to the heap-backed rings.
//!
//! # Region layout (stable, validated on attach)
//!
//! A fixed header at raw byte offsets (no Rust struct layout involved), then
//! the buffer:
//!
//! ```text
//! 0    magic     u64      "rust_rb1"
//! 8    version   u32
//! 12   kind      u32      1 = byte ring, 2 = element ring
//! 16   capacity  u64      cursor units (power of two)
//! 24   unit_size u64      bytes per cursor unit (1, or size_of::<T>())
//! 32   arch_bits u32      usize width; cross-arch attach is rejected
//! 40   producer_lease u64 (atomic) opaque token of the producer holder, 0 = free
//! 48   consumer_lease u64 (atomic) opaque token of the consumer holder, 0 = free
//! 56   generation u64 (atomic) seqlock: odd while (re)initializing
//! 264  starving  usize (atomic) producer out-of-space signal (read slot)
//! 128  write_cursor  usize (atomic, own 128-byte slot)
//! 256  read_cursor   usize (atomic, own 128-byte slot)
//! 384  buffer        capacity * unit_size bytes
//! ```
//!
//! # Trust model
//!
//! **Do not `fork` while holding shm ring handles.** A forked child inherits
//! bit-identical handles; teardown is pid-guarded so the child's exit will
//! not release the parent's roles, but any *use* of an inherited handle
//! violates single-producer/single-consumer and corrupts the ring. Spawn
//! children first, or pass the fd and attach fresh handles in the child.
//!
//! Header validation catches *accidents* (wrong fd, wrong ring type, wrong
//! architecture, corrupted cursors), not adversaries: every process mapping
//! the region can scribble over it, and the rings trust payload bit
//! patterns. Hence all constructors are `unsafe` — the caller asserts the
//! region is only ever touched by cooperating rust-rb handles.
//!
//! # Roles, leases, and crash recovery
//!
//! Each side holds a *lease* — an opaque random token — in the header;
//! dropping a handle releases its lease with a guarded CAS (a stale handle
//! whose role was taken over cannot revoke the successor, and its teardown
//! skips the shared-cursor flush too). Tokens deliberately carry **no
//! liveness meaning**: pids are namespace-relative, zombies look alive, and
//! pids get reused, so whether a holder is really gone is knowledge only the
//! application has. [`create_shm`](BytesRingBuffer::create_shm) takes both
//! roles; `attach_*` claims a free role (`AddrInUse` if held);
//! `force_attach_shm_producer`/`_consumer` unconditionally replace one role
//! and `recover_shm` replaces both — the caller asserts, via the `unsafe`
//! contract, that the previous holder(s) are gone.
//!
//! Because a record becomes visible only through the producer's single
//! `Release` cursor store, a producer that dies mid-write leaves the region
//! fully consistent: everything published is drainable, the unpublished
//! partial record is simply invisible and its space is reused once the
//! producer role is re-taken. Consumer-side recovery is **at-least-once**:
//! the dead consumer's unpublished progress is delivered again — its
//! deferred window (`capacity / 8`, max 4096 bytes / 64 elements) plus, on
//! the byte ring, any in-flight message's record and wrap padding, or the
//! whole in-progress batch if it died mid-`drain`. Worst case approaches a
//! full ring: size deduplication accordingly.
//!
//! Only [`CrossProcess`] wait strategies are accepted: the spin strategies
//! work across processes as-is, while `CvWait`'s mutex/condvar are
//! process-local.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cursor::{consumer_core_from_raw, producer_core_from_raw, AnchorKind, SlotCleanup};
use crate::spsc::{Consumer, Producer, RingBuffer};
use crate::spsc_bytes::{BytesConsumer, BytesProducer, BytesRingBuffer};
use crate::wait::CrossProcess;

const MAGIC: u64 = u64::from_le_bytes(*b"rust_rb1");
const VERSION: u32 = 1;
const KIND_BYTES: u32 = 1;
const KIND_ELEMS: u32 = 2;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_KIND: usize = 12;
const OFF_CAPACITY: usize = 16;
const OFF_UNIT_SIZE: usize = 24;
const OFF_ARCH_BITS: usize = 32;
const OFF_PRODUCER_LEASE: usize = 40;
const OFF_CONSUMER_LEASE: usize = 48;
/// Seqlock-style initialization generation: odd while `create_shm` is
/// (re)writing the header, bumped to even when complete. Validators read it
/// before their header reads and re-check after reads and lease claims; any
/// change (or an odd value) means a concurrent re-create, and the attach
/// fails rather than trusting a chimera of old and new fields.
const OFF_GENERATION: usize = 56;
/// Producer-starving flag (usize atomic): raised by the producer when a
/// space check fails, consumed by the byte ring's immediate-release rule.
/// Lives inside the read-cursor's 128-byte slot: that line already carries
/// the flush/poll traffic, so the flag adds no new line.
const OFF_STARVING: usize = OFF_READ_CURSOR + 8;
const OFF_WRITE_CURSOR: usize = 128;
const OFF_READ_CURSOR: usize = 256;
/// Buffer start: past the cursor slots, 128-byte aligned (mappings are
/// page-aligned, so every offset here is honored in memory).
const BUFFER_OFFSET: usize = 384;

/// Marker for element types that may cross a process boundary through a
/// shared-memory ring.
///
/// # Safety
///
/// Implementors assert that the type is plain data: `Copy`, no pointers,
/// references, or handles that are only meaningful in one address space, and
/// **valid for the bit patterns a cooperating peer writes** (the ring trusts
/// the region's contents — see the module's trust model). Types with
/// validity invariants (`bool`, `char`, most `enum`s, anything with niches)
/// must not be implemented unless the peer is trusted to uphold them.
pub unsafe trait ShmItem: Copy {}

macro_rules! shm_item {
    ($($t:ty),*) => {$(
        // SAFETY: plain integer/float data; every bit pattern is valid.
        unsafe impl ShmItem for $t {}
    )*};
}
shm_item!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64);

// SAFETY: arrays of plain data are plain data.
unsafe impl<T: ShmItem, const N: usize> ShmItem for [T; N] {}

/// Create a memfd suitable for backing a shared ring.
///
/// Created with close-on-exec set (the safe default — the fd does not leak
/// into unrelated exec'd children). To hand the ring to a child process by
/// fd inheritance, clear the flag first (`fcntl(fd, F_SETFD, 0)`) or pass it
/// over a unix socket.
pub fn memfd(name: &str) -> io::Result<OwnedFd> {
    let cname = std::ffi::CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "memfd name contains NUL"))?;
    // SAFETY: valid NUL-terminated name pointer; flags value is valid.
    let fd = unsafe { libc::memfd_create(cname.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a freshly created, owned file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// A mapped shared region. Unmapped on drop; the fd itself stays owned by
/// the caller.
pub(crate) struct ShmRegion {
    base: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is process-shared memory; the region wrapper itself
// carries no thread affinity.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // SAFETY: base/len are the exact mapping created in `map`.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

impl ShmRegion {
    fn map(fd: BorrowedFd<'_>, len: usize) -> io::Result<Self> {
        // SAFETY: length is non-zero and the fd is valid for the borrow.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            base: NonNull::new(ptr.cast()).expect("mmap returned null"),
            len,
        })
    }

    #[inline]
    fn at(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset < self.len);
        // SAFETY: offset is within the mapping (validated region size).
        unsafe { self.base.as_ptr().add(offset) }
    }

    /// # Safety
    ///
    /// `offset` must be within the mapping and naturally aligned for `A`.
    unsafe fn atomic<A>(&self, offset: usize) -> &A {
        // SAFETY: per the caller contract; the mapping outlives `&self`.
        unsafe { &*self.at(offset).cast::<A>() }
    }

    /// Atomic header reads: another cooperating process may be initializing
    /// the region concurrently; `read_magic` is the Acquire that orders the
    /// remaining (Relaxed) field reads.
    fn read_magic(&self) -> u64 {
        // SAFETY: OFF_MAGIC is 8-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU64>(OFF_MAGIC) }.load(Ordering::Acquire)
    }

    fn read_u64(&self, offset: usize) -> u64 {
        // SAFETY: header offsets are 8-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU64>(offset) }.load(Ordering::Relaxed)
    }

    fn read_u32(&self, offset: usize) -> u32 {
        // SAFETY: header offsets are 4-aligned and inside the mapping.
        unsafe { self.atomic::<AtomicU32>(offset) }.load(Ordering::Relaxed)
    }
}

/// Which lease a handle holds.
#[derive(Clone, Copy)]
pub(crate) enum Role {
    Producer,
    Consumer,
}

impl Role {
    fn lease_offset(self) -> usize {
        match self {
            Role::Producer => OFF_PRODUCER_LEASE,
            Role::Consumer => OFF_CONSUMER_LEASE,
        }
    }
}

/// The shm side of [`AnchorKind`]: keeps the mapping alive, carries this
/// handle's (per-process, [`CrossProcess`]) wait strategies, and releases
/// the role lease on drop.
pub(crate) struct ShmAnchor<P, C> {
    region: Arc<ShmRegion>,
    role: Role,
    /// The opaque token this handle wrote into its role lease.
    token: u64,
    /// The process that constructed this handle. A `fork`ed child inherits a
    /// bit-identical anchor (same token), so token equality alone cannot
    /// tell the original from the copy — teardown and ownership checks also
    /// require running in the constructing process, or the child's exit
    /// would release (or flush over) the parent's live role.
    owner_pid: libc::pid_t,
    pub(crate) producer_wait: P,
    pub(crate) consumer_wait: C,
}

impl<P, C> ShmAnchor<P, C> {
    /// Whether the role lease still holds this handle's token (i.e. no one
    /// has force-taken the role). Consulted before teardown touches shared
    /// state; a takeover racing this check is the force caller's asserted
    /// responsibility ("the previous holder is gone").
    pub(crate) fn owns_lease(&self) -> bool {
        // SAFETY: lease offsets are 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(self.role.lease_offset()) };
        lease.load(Ordering::Acquire) == self.token
    }

    /// Whether we are the process that constructed this handle. The getpid
    /// comparison is same-lineage only (never cross-namespace): a forked
    /// child is not the owner even though its token matches bit-for-bit.
    /// Syscall cost — teardown paths only.
    pub(crate) fn owned_by_current_process(&self) -> bool {
        // SAFETY: getpid is always safe.
        (unsafe { libc::getpid() }) == self.owner_pid
    }
}

impl<P, C> Drop for ShmAnchor<P, C> {
    fn drop(&mut self) {
        // Guarded release: free the lease only if (a) we are the process
        // that constructed the handle — a fork-inherited copy carries a
        // bit-identical token and its exit must NOT release the parent's
        // live role — and (b) the lease still holds our token (after a
        // force-steal it holds the successor's, and a zombie's late drop
        // must not revoke it).
        // SAFETY: getpid is always safe; lease offsets are 8-aligned and
        // inside the mapping.
        if !self.owned_by_current_process() {
            return;
        }
        // SAFETY: lease offsets are 8-aligned and inside the mapping.
        let lease: &AtomicU64 = unsafe { self.region.atomic(self.role.lease_offset()) };
        let _ = lease.compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// A fresh opaque lease token: random (per-handle), never 0.
///
/// Tokens deliberately carry **no liveness meaning**. An earlier design
/// stored the holder's pid and probed it with `kill(pid, 0)`; that is wrong
/// in exactly the situations shm rings are for — pids are namespace-relative
/// (a container's pid 42 is not the host's), zombies still "exist", pids get
/// reused, and a u64 lease does not even fit `pid_t`. Whether a previous
/// holder is really gone is knowledge only the caller can have, which is why
/// the force/recover constructors are `unsafe` and unconditional.
fn lease_token() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    loop {
        // RandomState is freshly seeded per instance.
        let mut h = RandomState::new().build_hasher();
        h.write_u64(std::process::id() as u64);
        let t = h.finish();
        if t != 0 {
            return t;
        }
    }
}

/// Claim a free `role` in the region. Fails with `AddrInUse` if any holder's
/// token is present — cooperative exclusivity, no liveness guessing.
/// `generation` is the seqlock snapshot from validation: if `create_shm`
/// re-initialized the region between the header reads and this claim, the
/// claim is rolled back and the attach fails — otherwise the creator's
/// unconditional lease stores could silently overwrite ours, leaving two
/// holders of one role.
fn claim_lease(region: &ShmRegion, role: Role, generation: u64) -> io::Result<u64> {
    // SAFETY: lease offsets are 8-aligned and inside the mapping.
    let lease: &AtomicU64 = unsafe { region.atomic(role.lease_offset()) };
    let token = lease_token();
    match lease.compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => {
            // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
            let gen_now =
                unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
            if gen_now != generation {
                let _ = lease.compare_exchange(token, 0, Ordering::AcqRel, Ordering::Acquire);
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "ring was re-initialized during attach",
                ));
            }
            Ok(token)
        }
        Err(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "ring role already held (drop the existing handle, or use the \
             force/recover constructors if the holder is known dead)",
        )),
    }
}

/// Unconditionally take `role`, replacing whatever token is there. The
/// caller (via the `unsafe` constructor contract) asserts the previous
/// holder is gone; its guarded Drop can no longer release the new token.
fn force_claim_lease(region: &ShmRegion, role: Role) -> u64 {
    // SAFETY: lease offsets are 8-aligned and inside the mapping.
    let lease: &AtomicU64 = unsafe { region.atomic(role.lease_offset()) };
    let token = lease_token();
    lease.swap(token, Ordering::AcqRel);
    token
}

fn region_len(capacity: usize, unit_size: usize) -> io::Result<usize> {
    let len = capacity
        .checked_mul(unit_size)
        .and_then(|b| b.checked_add(BUFFER_OFFSET))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region"))?;
    // `ftruncate`/`mmap` take `off_t`, which is 32-bit on some 32-bit Linux
    // targets — a length in [2^31, off_t::MAX+1) would sign-flip negative and
    // surface as a bare EINVAL instead of this validation error.
    if len as u64 > libc::off_t::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "region length exceeds the platform file-offset limit",
        ));
    }
    Ok(len)
}

/// Initialize a fresh region: size the fd, map it, write the header, take
/// both leases.
fn create_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    capacity: usize,
    unit_size: usize,
) -> io::Result<(Arc<ShmRegion>, u64, u64)> {
    let len = region_len(capacity, unit_size)?;
    // SAFETY: valid fd for the borrow; `region_len` confirmed `len` fits off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // All header accesses go through atomics: plain stores would let the
    // compiler elide the magic clear (a "dead" store) and give a concurrent
    // attacher in another process no ordering at all.
    // SAFETY: all header offsets are naturally aligned and inside the mapping.
    let (producer_token, consumer_token) = unsafe {
        // Seqlock write protocol: make the generation odd (initializing)
        // BEFORE touching anything, so a concurrent validator that read any
        // pre-clear state sees the generation change on its re-check and
        // discards what it read — it can never act on a chimera of old and
        // new fields, nor keep a lease the stores below overwrite.
        let generation = region.atomic::<AtomicU64>(OFF_GENERATION);
        let g = generation.load(Ordering::Relaxed);
        generation.store(g | 1, Ordering::SeqCst);
        // Invalidate any previous ring in this fd (SeqCst: neither elidable
        // nor able to sink past the field stores).
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(0, Ordering::SeqCst);
        region
            .atomic::<AtomicU32>(OFF_VERSION)
            .store(VERSION, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_KIND)
            .store(kind, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CAPACITY)
            .store(capacity as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_UNIT_SIZE)
            .store(unit_size as u64, Ordering::Relaxed);
        region
            .atomic::<AtomicU32>(OFF_ARCH_BITS)
            .store(usize::BITS, Ordering::Relaxed);
        let pt = lease_token();
        let ct = lease_token();
        region
            .atomic::<AtomicU64>(OFF_PRODUCER_LEASE)
            .store(pt, Ordering::Relaxed);
        region
            .atomic::<AtomicU64>(OFF_CONSUMER_LEASE)
            .store(ct, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_WRITE_CURSOR)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_READ_CURSOR)
            .store(0, Ordering::Relaxed);
        region
            .atomic::<AtomicUsize>(OFF_STARVING)
            .store(0, Ordering::Relaxed);
        // Publish the magic last with Release: an attacher that Acquire-loads
        // it sees every header field above.
        region
            .atomic::<AtomicU64>(OFF_MAGIC)
            .store(MAGIC, Ordering::Release);
        // Seqlock close: even generation, strictly greater than before.
        generation.store((g | 1).wrapping_add(1), Ordering::Release);
        (pt, ct)
    };

    // Match the heap ring's zeroed-buffer guarantee even when the fd is
    // reused and carries old contents (one pass at create time). This also
    // pre-faults and commits every buffer page here rather than on the first
    // hot-path touch — desirable for a latency-sensitive ring, and the whole
    // constructor is cold.
    // SAFETY: the buffer span is inside the mapping.
    unsafe {
        std::ptr::write_bytes(region.at(BUFFER_OFFSET), 0, len - BUFFER_OFFSET);
    }
    Ok((Arc::new(region), producer_token, consumer_token))
}

/// Size of the file behind `fd`.
fn fd_len(fd: BorrowedFd<'_>) -> io::Result<u64> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid fd for the borrow; `st` is a zeroed out-param.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st.st_size as u64)
}

/// Map and validate an existing region. `min_capacity`/`cursor_align` are
/// the ring kind's extra invariants: the byte ring requires capacity >= 8
/// (its `max_message_len` arithmetic underflows below that) and
/// record-aligned cursors (its frame decoder does aligned u32 header reads).
fn open_region(
    fd: BorrowedFd<'_>,
    kind: u32,
    unit_size: usize,
    min_capacity: usize,
    cursor_align: usize,
) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
    // Touching mapped pages past the file's end is SIGBUS, not an error
    // return — validate the size (once) before mapping.
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    let file_len = fd_len(fd)?;
    if file_len < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn the capacity.
    let header = ShmRegion::map(fd, BUFFER_OFFSET)?;
    // Seqlock read: snapshot the generation before any header read; callers
    // re-check it after their lease claim. Odd = mid-initialization.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    let generation = unsafe { header.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire);
    if generation & 1 == 1 {
        return Err(err("ring is being initialized by another process"));
    }
    if header.read_magic() != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch (bytes vs element ring)"));
    }
    if unit_size == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if header.read_u64(OFF_UNIT_SIZE) != unit_size as u64 {
        return Err(err("element size mismatch"));
    }
    if header.read_u32(OFF_ARCH_BITS) != usize::BITS {
        return Err(err("architecture (usize width) mismatch"));
    }
    let capacity = header.read_u64(OFF_CAPACITY) as usize;
    if capacity == 0 || !capacity.is_power_of_two() || capacity < min_capacity {
        return Err(err("corrupt capacity"));
    }
    drop(header);

    let len = region_len(capacity, unit_size)?;
    if file_len < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = ShmRegion::map(fd, len)?;

    // Cursor invariant: occupancy (wrapped) within capacity. The two loads
    // are separate snapshots, and on a live busy ring the pair can be
    // mutually inconsistent (read sampled after the producer moved write),
    // which is NOT corruption — retry for a stable pair, and only judge
    // occupancy on one. Alignment holds for every individually-published
    // value, so it can be checked on any snapshot.
    // SAFETY: cursor offsets are aligned and inside the mapping.
    let write_at = unsafe { region.atomic::<AtomicUsize>(OFF_WRITE_CURSOR) };
    let read_at = unsafe { region.atomic::<AtomicUsize>(OFF_READ_CURSOR) };
    let mut stable = None;
    for _ in 0..64 {
        let w1 = write_at.load(Ordering::Acquire);
        let r = read_at.load(Ordering::Acquire);
        let w2 = write_at.load(Ordering::Acquire);
        if w1 % cursor_align != 0 || r % cursor_align != 0 {
            return Err(err("corrupt cursors: not record-aligned"));
        }
        if w1 == w2 {
            stable = Some((w1, r));
            break;
        }
    }
    // A ring too busy for 64 stable snapshots is by definition live, not
    // corrupt; skip the occupancy judgement rather than misdiagnose it.
    if let Some((write, read)) = stable {
        if write.wrapping_sub(read) > capacity {
            return Err(err("corrupt cursors: occupancy exceeds capacity"));
        }
    }
    // Seqlock re-check on the full mapping: if the header changed while we
    // were reading it, everything above may be a chimera.
    // SAFETY: OFF_GENERATION is 8-aligned and inside the mapping.
    if unsafe { region.atomic::<AtomicU64>(OFF_GENERATION) }.load(Ordering::Acquire) != generation {
        return Err(err("ring was re-initialized during validation"));
    }
    Ok((Arc::new(region), capacity, generation))
}

impl ShmRegion {
    fn cursors(
        &self,
    ) -> (
        NonNull<AtomicUsize>,
        NonNull<AtomicUsize>,
        NonNull<AtomicUsize>,
    ) {
        (
            NonNull::new(self.at(OFF_WRITE_CURSOR).cast()).expect("mapping is non-null"),
            NonNull::new(self.at(OFF_READ_CURSOR).cast()).expect("mapping is non-null"),
            NonNull::new(self.at(OFF_STARVING).cast()).expect("mapping is non-null"),
        )
    }

    fn buffer<B>(&self) -> NonNull<B> {
        debug_assert!(BUFFER_OFFSET % std::mem::align_of::<B>() == 0);
        NonNull::new(self.at(BUFFER_OFFSET).cast()).expect("mapping is non-null")
    }
}

/// Build one producer handle core over a validated region.
///
/// # Safety
///
/// Region layout must match `B` (validated by `open_region`/`create_region`).
unsafe fn shm_producer_core<B, P, C>(
    region: Arc<ShmRegion>,
    capacity: usize,
    token: u64,
) -> crate::cursor::ProducerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read, starving) = region.cursors();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(Box::new(ShmAnchor {
        region,
        role: Role::Producer,
        token,
        // SAFETY: getpid is always safe.
        owner_pid: unsafe { libc::getpid() },
        producer_wait: P::default(),
        consumer_wait: C::default(),
    }));
    // SAFETY: pointers reference the live mapping the anchor keeps alive;
    // cursor invariant validated by the caller.
    unsafe { producer_core_from_raw(buf, capacity, write, read, starving, anchor) }
}

/// Build one consumer handle core over a validated region (see
/// `shm_producer_core`).
///
/// # Safety
///
/// As for `shm_producer_core`.
unsafe fn shm_consumer_core<B, P, C>(
    region: Arc<ShmRegion>,
    capacity: usize,
    token: u64,
) -> crate::cursor::ConsumerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read, starving) = region.cursors();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(Box::new(ShmAnchor {
        region,
        role: Role::Consumer,
        token,
        // SAFETY: getpid is always safe.
        owner_pid: unsafe { libc::getpid() },
        producer_wait: P::default(),
        consumer_wait: C::default(),
    }));
    // SAFETY: as for `shm_producer_core`.
    unsafe { consumer_core_from_raw(buf, capacity, write, read, starving, anchor) }
}

/// Claim (or force-take) the producer role over a validated region and wrap
/// the core in the ring's handle type. One implementation for all ten
/// attach/force/recover constructors across both rings.
///
/// # Safety
///
/// As for `shm_producer_core`: the region layout must match `B`.
unsafe fn attach_producer_role<B, P, C, H>(
    opened: (Arc<ShmRegion>, usize, u64),
    force: bool,
    wrap: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> H,
) -> io::Result<H>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, capacity, generation) = opened;
    let token = if force {
        force_claim_lease(&region, Role::Producer)
    } else {
        claim_lease(&region, Role::Producer, generation)?
    };
    // SAFETY: forwarded caller contract (validated region).
    Ok(wrap(unsafe {
        shm_producer_core::<B, P, C>(region, capacity, token)
    }))
}

/// Build both handle cores over a freshly-created region and wrap them in the
/// ring's handle types — the shared body of both rings' `create_shm_with`.
///
/// # Safety
///
/// The region must have been initialized by `create_region` for this ring
/// kind (layout matches `B`).
unsafe fn create_pair<B, P, C, TX, RX>(
    created: (Arc<ShmRegion>, u64, u64),
    capacity: usize,
    wrap_tx: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> TX,
    wrap_rx: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> RX,
) -> (TX, RX)
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, pt, ct) = created;
    // SAFETY: freshly initialized region matches the ring layout.
    unsafe {
        (
            wrap_tx(shm_producer_core::<B, P, C>(region.clone(), capacity, pt)),
            wrap_rx(shm_consumer_core::<B, P, C>(region, capacity, ct)),
        )
    }
}

/// Force-take BOTH roles over a validated region and wrap them in the ring's
/// handle types — the shared body of both rings' `recover_shm_with`. Force
/// cannot fail, so no partial-failure path can leak a lease.
///
/// # Safety
///
/// As for [`attach_producer_role`]/[`attach_consumer_role`].
#[allow(clippy::type_complexity)]
unsafe fn recover_pair<B, P, C, TX, RX>(
    opened: (Arc<ShmRegion>, usize, u64),
    wrap_tx: impl FnOnce(crate::cursor::ProducerCore<B, P, C>) -> TX,
    wrap_rx: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> RX,
) -> io::Result<(TX, RX)>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let producer_opened = (opened.0.clone(), opened.1, opened.2);
    // SAFETY: forwarded caller contract (validated region).
    unsafe {
        let tx = attach_producer_role(producer_opened, true, wrap_tx)?;
        let rx = attach_consumer_role(opened, true, wrap_rx)?;
        Ok((tx, rx))
    }
}

/// Consumer analog of [`attach_producer_role`].
///
/// # Safety
///
/// As for `shm_consumer_core`.
unsafe fn attach_consumer_role<B, P, C, H>(
    opened: (Arc<ShmRegion>, usize, u64),
    force: bool,
    wrap: impl FnOnce(crate::cursor::ConsumerCore<B, P, C>) -> H,
) -> io::Result<H>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (region, capacity, generation) = opened;
    let token = if force {
        force_claim_lease(&region, Role::Consumer)
    } else {
        claim_lease(&region, Role::Consumer, generation)?
    };
    // SAFETY: forwarded caller contract (validated region).
    Ok(wrap(unsafe {
        shm_consumer_core::<B, P, C>(region, capacity, token)
    }))
}

/// Both halves of a shm-backed byte ring.
pub type BytesPair<P, C> = (BytesProducer<P, C>, BytesConsumer<P, C>);
/// Both halves of a shm-backed element ring.
pub type ElemPair<T, P, C> = (Producer<T, P, C>, Consumer<T, P, C>);

/// Byte-ring capacity floor and record alignment, shared with the ring's own
/// constructor and frame decoder so they cannot drift.
const BYTES_MIN_CAPACITY: usize = crate::spsc_bytes::MIN_CAPACITY;
const BYTES_CURSOR_ALIGN: usize = crate::spsc_bytes::ALIGN;

impl BytesRingBuffer {
    /// Initialize `fd` as a fresh shm-backed byte ring and return both
    /// halves, with default ([`YieldWait`](crate::wait::YieldWait)) wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self): the region must only ever be
    /// accessed by cooperating rust-rb handles.
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<(BytesProducer, BytesConsumer)> {
        // SAFETY: forwarded caller contract.
        unsafe { BytesRingBuffer::<_, _>::create_shm_with(fd, min_capacity) }
    }

    /// Unconditionally take over **both** roles of an existing ring —
    /// typically after both holders died. Everything already published is
    /// intact and drainable (see the module docs on crash consistency);
    /// messages the dead consumer had consumed but not yet published — up to
    /// the deferred-publish window (`capacity / 8`, max 4096 bytes) — are
    /// **delivered again** (recovery is at-least-once).
    ///
    /// # Safety
    ///
    /// Trust model, plus: the takeover is unconditional. The caller asserts
    /// both previous holders are gone; a still-live holder would keep
    /// writing concurrently and corrupt the ring. (Their late `Drop`s are
    /// harmless — lease release is guarded by token.)
    pub unsafe fn recover_shm(fd: BorrowedFd<'_>) -> io::Result<(BytesProducer, BytesConsumer)> {
        // SAFETY: forwarded caller contract.
        unsafe { BytesRingBuffer::<_, _>::recover_shm_with(fd) }
    }
}

impl<P, C> BytesRingBuffer<P, C>
where
    P: CrossProcess + Send + Sync,
    C: CrossProcess + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
        open_region(fd, KIND_BYTES, 1, BYTES_MIN_CAPACITY, BYTES_CURSOR_ALIGN)
    }

    /// [`create_shm`](BytesRingBuffer::create_shm) with explicit
    /// [`CrossProcess`] wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<BytesPair<P, C>> {
        let capacity = crate::cursor::round_capacity(min_capacity, BYTES_MIN_CAPACITY);
        let created = create_region(fd, KIND_BYTES, capacity, 1)?;
        // SAFETY: freshly initialized region matches the byte-ring layout.
        Ok(unsafe {
            create_pair(
                created,
                capacity,
                BytesProducer::from_core,
                BytesConsumer::from_core,
            )
        })
    }

    /// Attach to an existing ring as the producer. Fails with `AddrInUse`
    /// while the role's lease is held (cooperative exclusivity — see
    /// [`force_attach_shm_producer`](Self::force_attach_shm_producer) when
    /// the holder is known dead).
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer: the caller asserts no other live
    /// producer handle exists (the lease enforces this against cooperating
    /// processes only).
    pub unsafe fn attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<BytesProducer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, false, BytesProducer::from_core) }
    }

    /// Attach to an existing ring as the consumer (see
    /// [`attach_shm_producer`](Self::attach_shm_producer)).
    ///
    /// # Safety
    ///
    /// Trust model, plus single-consumer.
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, false, BytesConsumer::from_core) }
    }

    /// Unconditionally take over the **producer** role — single-side crash
    /// recovery while the consumer keeps running. Publishing resumes exactly
    /// after the last record the dead producer published (a partial,
    /// unpublished record is invisible and its space is reused).
    ///
    /// # Safety
    ///
    /// Trust model, plus: unconditional takeover — the caller asserts the
    /// previous producer is gone (a live one would corrupt the ring).
    pub unsafe fn force_attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<BytesProducer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, true, BytesProducer::from_core) }
    }

    /// Unconditionally take over the **consumer** role — single-side crash
    /// recovery while the producer keeps running. Consumption resumes at the
    /// dead consumer's last *published* cursor: messages it consumed but had
    /// not yet published are delivered again (at-least-once). The window is
    /// the deferred-publish clamp (`capacity / 8`, max 4096 bytes) **plus
    /// any in-flight message's record and wrap padding** (each up to
    /// `capacity / 2`) if it died holding a [`Msg`](crate::spsc_bytes::Msg),
    /// or the entire in-progress batch if it died mid-`drain` — size
    /// deduplication windows for up to a full ring of redelivery.
    ///
    /// # Safety
    ///
    /// As for [`force_attach_shm_producer`](Self::force_attach_shm_producer),
    /// for the consumer role.
    pub unsafe fn force_attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, true, BytesConsumer::from_core) }
    }

    /// [`recover_shm`](BytesRingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](BytesRingBuffer::recover_shm).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<BytesPair<P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe {
            recover_pair(
                Self::open(fd)?,
                BytesProducer::from_core,
                BytesConsumer::from_core,
            )
        }
    }
}

/// Element-type invariants for shm rings, enforced consistently on create
/// AND attach as errors (never panics — these surface on fallible paths).
fn check_elem_type<T>() -> io::Result<()> {
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    if std::mem::size_of::<T>() == 0 {
        return Err(err("zero-sized elements are not supported in shm rings"));
    }
    if std::mem::align_of::<T>() > 128 {
        return Err(err("element alignment exceeds the buffer offset alignment"));
    }
    Ok(())
}

impl<T: ShmItem + Send> RingBuffer<T> {
    /// Initialize `fd` as a fresh shm-backed element ring (default wait
    /// strategies).
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    ///
    /// # Panics
    ///
    /// Panics if `min_capacity == 0`.
    pub unsafe fn create_shm(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<(Producer<T>, Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { RingBuffer::<T, _, _>::create_shm_with(fd, min_capacity) }
    }

    /// Unconditionally take over both roles of an existing element ring (see
    /// [`BytesRingBuffer::recover_shm`], including the at-least-once
    /// re-delivery window — up to `capacity / 8`, max 64 elements).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::recover_shm`].
    pub unsafe fn recover_shm(fd: BorrowedFd<'_>) -> io::Result<(Producer<T>, Consumer<T>)> {
        // SAFETY: forwarded caller contract.
        unsafe { RingBuffer::<T, _, _>::recover_shm_with(fd) }
    }
}

impl<T, P, C> RingBuffer<T, P, C>
where
    T: ShmItem + Send,
    P: CrossProcess + Send + Sync,
    C: CrossProcess + Send + Sync,
{
    fn open(fd: BorrowedFd<'_>) -> io::Result<(Arc<ShmRegion>, usize, u64)> {
        // The invariants create enforces must hold on ATTACH too — an
        // attacher instantiated with a different `T` of the same size but
        // higher alignment would otherwise get misaligned slots (UB). These
        // are environmental/typed misuses on a fallible path: errors, not
        // panics.
        check_elem_type::<T>()?;
        // Element cursors count whole elements; any value is decodable.
        open_region(fd, KIND_ELEMS, std::mem::size_of::<T>(), 1, 1)
    }

    /// [`create_shm`](RingBuffer::create_shm) with explicit [`CrossProcess`]
    /// wait strategies.
    ///
    /// # Safety
    ///
    /// See [the module's trust model](self).
    pub unsafe fn create_shm_with(
        fd: BorrowedFd<'_>,
        min_capacity: usize,
    ) -> io::Result<ElemPair<T, P, C>> {
        check_elem_type::<T>()?;
        let capacity = crate::cursor::round_capacity(min_capacity, 1);
        let created = create_region(fd, KIND_ELEMS, capacity, std::mem::size_of::<T>())?;
        // SAFETY: freshly initialized region matches the element-ring layout.
        Ok(unsafe { create_pair(created, capacity, Producer::from_core, Consumer::from_core) })
    }

    /// Attach to an existing element ring as the producer.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size is validated).
    pub unsafe fn attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, false, Producer::from_core) }
    }

    /// Attach to an existing element ring as the consumer.
    ///
    /// # Safety
    ///
    /// As for [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<Consumer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, false, Consumer::from_core) }
    }

    /// Unconditionally take over the producer role (see
    /// [`BytesRingBuffer::force_attach_shm_producer`]).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::force_attach_shm_producer`], plus the `T`
    /// caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_producer_role(Self::open(fd)?, true, Producer::from_core) }
    }

    /// Unconditionally take over the consumer role (see
    /// [`BytesRingBuffer::force_attach_shm_consumer`]; the at-least-once
    /// window is `capacity / 8`, max 64 elements).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::force_attach_shm_consumer`], plus the `T`
    /// caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<Consumer<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { attach_consumer_role(Self::open(fd)?, true, Consumer::from_core) }
    }

    /// [`recover_shm`](RingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::recover_shm`], plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<ElemPair<T, P, C>> {
        // SAFETY: forwarded caller contract; region validated by open().
        unsafe { recover_pair(Self::open(fd)?, Producer::from_core, Consumer::from_core) }
    }
}
