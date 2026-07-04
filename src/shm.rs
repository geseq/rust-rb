//! Shared-memory-backed rings (Linux, feature `shm`).
//!
//! Backs [`RingBuffer`](crate::spsc::RingBuffer) and
//! [`BytesRingBuffer`](crate::spsc_bytes::BytesRingBuffer) with a mapped
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
//! 40   producer_lease u64 (atomic) pid holding the producer role, 0 = free
//! 48   consumer_lease u64 (atomic) pid holding the consumer role, 0 = free
//! 128  write_cursor  usize (atomic, own 128-byte slot)
//! 256  read_cursor   usize (atomic, own 128-byte slot)
//! 384  buffer        capacity * unit_size bytes
//! ```
//!
//! # Trust model
//!
//! Header validation catches *accidents* (wrong fd, wrong ring type, wrong
//! architecture, corrupted cursors), not adversaries: every process mapping
//! the region can scribble over it, and the rings trust payload bit
//! patterns. Hence all constructors are `unsafe` — the caller asserts the
//! region is only ever touched by cooperating rust-rb handles.
//!
//! # Roles, leases, and crash recovery
//!
//! Each side holds a *lease* (its pid) in the header; dropping a handle
//! releases its lease. [`create_shm`](BytesRingBuffer::create_shm) takes
//! both roles; `attach_*` claims one free role; `recover_shm` reclaims roles
//! whose holder is dead (best-effort `kill(pid, 0)` liveness probe — pid
//! reuse can defeat it, which is part of the `unsafe` contract). Because a
//! record becomes visible only through the producer's single `Release`
//! cursor store, a producer that dies mid-write leaves the region fully
//! consistent: everything published is drainable, the unpublished partial
//! record is simply invisible and its space is reused once the producer role
//! is re-attached.
//!
//! Only [`CrossProcess`] wait strategies are accepted: the spin strategies
//! work across processes as-is, while `CvWait`'s mutex/condvar are
//! process-local.

use std::cell::UnsafeCell;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

    fn read_u64(&self, offset: usize) -> u64 {
        // SAFETY: header offsets are 8-aligned and inside the mapping.
        unsafe { self.at(offset).cast::<u64>().read() }
    }

    fn read_u32(&self, offset: usize) -> u32 {
        // SAFETY: header offsets are 4-aligned and inside the mapping.
        unsafe { self.at(offset).cast::<u32>().read() }
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
    pub(crate) producer_wait: P,
    pub(crate) consumer_wait: C,
}

impl<P, C> Drop for ShmAnchor<P, C> {
    fn drop(&mut self) {
        // Guarded release: free the lease only if it still holds OUR token.
        // After a force-steal the lease holds someone else's token, and a
        // zombie handle's late drop must not revoke the new holder's
        // exclusivity. (A `fork`ed child duplicates the token and can still
        // release the parent's lease — inheriting shm handles across fork is
        // outside the constructors' contract.)
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
fn claim_lease(region: &ShmRegion, role: Role) -> io::Result<u64> {
    // SAFETY: lease offsets are 8-aligned and inside the mapping.
    let lease: &AtomicU64 = unsafe { region.atomic(role.lease_offset()) };
    let token = lease_token();
    match lease.compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Ok(token),
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
    capacity
        .checked_mul(unit_size)
        .and_then(|b| b.checked_add(BUFFER_OFFSET))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "capacity overflows region"))
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
    // SAFETY: valid fd for the borrow; len fits off_t for any real capacity.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let region = ShmRegion::map(fd, len)?;

    // Header writes happen before any handle exists, so plain stores are
    // fine; the leases and cursors are atomics from first use.
    // SAFETY: all header offsets are aligned and inside the mapping.
    let (producer_token, consumer_token) = unsafe {
        // Invalidate any previous ring in this fd FIRST, so the
        // publish-magic-last torn-header guard below is meaningful even when
        // the fd is being reused.
        region.at(OFF_MAGIC).cast::<u64>().write(0);
        region.at(OFF_VERSION).cast::<u32>().write(VERSION);
        region.at(OFF_KIND).cast::<u32>().write(kind);
        region.at(OFF_CAPACITY).cast::<u64>().write(capacity as u64);
        region
            .at(OFF_UNIT_SIZE)
            .cast::<u64>()
            .write(unit_size as u64);
        region.at(OFF_ARCH_BITS).cast::<u32>().write(usize::BITS);
        let pt = lease_token();
        let ct = lease_token();
        region.at(OFF_PRODUCER_LEASE).cast::<u64>().write(pt);
        region.at(OFF_CONSUMER_LEASE).cast::<u64>().write(ct);
        region.at(OFF_WRITE_CURSOR).cast::<usize>().write(0);
        region.at(OFF_READ_CURSOR).cast::<usize>().write(0);
        // Publish the magic last: an attacher that sees it sees a complete
        // header (same-process ordering suffices for cooperating processes
        // that coordinate fd hand-off, which happens-after this call).
        region.at(OFF_MAGIC).cast::<u64>().write(MAGIC);
        (pt, ct)
    };
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
) -> io::Result<Arc<ShmRegion>> {
    // Touching mapped pages past the file's end is SIGBUS, not an error
    // return — validate the size before every mapping.
    let err = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    if fd_len(fd)? < BUFFER_OFFSET as u64 {
        return Err(err("region too small to hold a ring header"));
    }
    // Map just the header first to learn the capacity.
    let header = ShmRegion::map(fd, BUFFER_OFFSET)?;
    if header.read_u64(OFF_MAGIC) != MAGIC {
        return Err(err("bad magic: not a rust-rb shm ring"));
    }
    if header.read_u32(OFF_VERSION) != VERSION {
        return Err(err("unsupported ring version"));
    }
    if header.read_u32(OFF_KIND) != kind {
        return Err(err("ring kind mismatch (bytes vs element ring)"));
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
    if fd_len(fd)? < len as u64 {
        return Err(err("region smaller than its declared capacity"));
    }
    let region = ShmRegion::map(fd, len)?;

    // Cursor invariant: occupancy (wrapped) within capacity.
    // SAFETY: cursor offsets are aligned and inside the mapping.
    let write = unsafe { region.atomic::<AtomicUsize>(OFF_WRITE_CURSOR) }.load(Ordering::Acquire);
    let read = unsafe { region.atomic::<AtomicUsize>(OFF_READ_CURSOR) }.load(Ordering::Acquire);
    if write.wrapping_sub(read) > capacity {
        return Err(err("corrupt cursors: occupancy exceeds capacity"));
    }
    if write % cursor_align != 0 || read % cursor_align != 0 {
        return Err(err("corrupt cursors: not record-aligned"));
    }
    Ok(Arc::new(region))
}

impl ShmRegion {
    fn capacity(&self) -> usize {
        self.read_u64(OFF_CAPACITY) as usize
    }

    fn cursors(&self) -> (NonNull<AtomicUsize>, NonNull<AtomicUsize>) {
        (
            NonNull::new(self.at(OFF_WRITE_CURSOR).cast()).expect("mapping is non-null"),
            NonNull::new(self.at(OFF_READ_CURSOR).cast()).expect("mapping is non-null"),
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
    token: u64,
) -> crate::cursor::ProducerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read) = region.cursors();
    let capacity = region.capacity();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(ShmAnchor {
        region,
        role: Role::Producer,
        token,
        producer_wait: P::default(),
        consumer_wait: C::default(),
    });
    // SAFETY: pointers reference the live mapping the anchor keeps alive;
    // cursor invariant validated by the caller.
    unsafe { producer_core_from_raw(buf, capacity, write, read, anchor) }
}

/// Build one consumer handle core over a validated region (see
/// `shm_producer_core`).
///
/// # Safety
///
/// As for `shm_producer_core`.
unsafe fn shm_consumer_core<B, P, C>(
    region: Arc<ShmRegion>,
    token: u64,
) -> crate::cursor::ConsumerCore<B, P, C>
where
    B: SlotCleanup,
    P: CrossProcess + Default,
    C: CrossProcess + Default,
{
    let (write, read) = region.cursors();
    let capacity = region.capacity();
    let buf = region.buffer::<B>();
    let anchor = AnchorKind::Shm(ShmAnchor {
        region,
        role: Role::Consumer,
        token,
        producer_wait: P::default(),
        consumer_wait: C::default(),
    });
    // SAFETY: as for `shm_producer_core`.
    unsafe { consumer_core_from_raw(buf, capacity, write, read, anchor) }
}

type Word = UnsafeCell<u64>;
/// Both halves of a shm-backed byte ring.
pub type BytesPair<P, C> = (BytesProducer<P, C>, BytesConsumer<P, C>);
/// Both halves of a shm-backed element ring.
pub type ElemPair<T, P, C> = (Producer<T, P, C>, Consumer<T, P, C>);

/// Byte-ring capacity floor (see `spsc_bytes`): below 8 the max-message
/// arithmetic underflows.
const BYTES_MIN_CAPACITY: usize = 8;
/// Byte-ring cursors advance in ALIGN(4)-byte records; the frame decoder's
/// aligned u32 header reads rely on it.
const BYTES_CURSOR_ALIGN: usize = 4;

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
    fn open(fd: BorrowedFd<'_>) -> io::Result<Arc<ShmRegion>> {
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
        assert!(min_capacity > 0, "capacity must be greater than zero");
        let capacity = min_capacity
            .checked_next_power_of_two()
            .expect("capacity too large to round up to a power of two")
            .max(BYTES_MIN_CAPACITY);
        let (region, pt, ct) = create_region(fd, KIND_BYTES, capacity, 1)?;
        // SAFETY: freshly initialized region matches the byte-ring layout.
        Ok(unsafe {
            (
                BytesProducer::from_core(shm_producer_core::<Word, P, C>(region.clone(), pt)),
                BytesConsumer::from_core(shm_consumer_core::<Word, P, C>(region, ct)),
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
        let region = Self::open(fd)?;
        let token = claim_lease(&region, Role::Producer)?;
        // SAFETY: validated byte-ring region.
        Ok(unsafe { BytesProducer::from_core(shm_producer_core::<Word, P, C>(region, token)) })
    }

    /// Attach to an existing ring as the consumer (see
    /// [`attach_shm_producer`](Self::attach_shm_producer)).
    ///
    /// # Safety
    ///
    /// Trust model, plus single-consumer.
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        let region = Self::open(fd)?;
        let token = claim_lease(&region, Role::Consumer)?;
        // SAFETY: validated byte-ring region.
        Ok(unsafe { BytesConsumer::from_core(shm_consumer_core::<Word, P, C>(region, token)) })
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
        let region = Self::open(fd)?;
        let token = force_claim_lease(&region, Role::Producer);
        // SAFETY: validated byte-ring region.
        Ok(unsafe { BytesProducer::from_core(shm_producer_core::<Word, P, C>(region, token)) })
    }

    /// Unconditionally take over the **consumer** role — single-side crash
    /// recovery while the producer keeps running. Consumption resumes at the
    /// dead consumer's last *published* cursor: messages it consumed but had
    /// not yet published — up to the deferred-publish window (`capacity / 8`,
    /// max 4096 bytes) — are delivered again (at-least-once).
    ///
    /// # Safety
    ///
    /// As for [`force_attach_shm_producer`](Self::force_attach_shm_producer),
    /// for the consumer role.
    pub unsafe fn force_attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<BytesConsumer<P, C>> {
        let region = Self::open(fd)?;
        let token = force_claim_lease(&region, Role::Consumer);
        // SAFETY: validated byte-ring region.
        Ok(unsafe { BytesConsumer::from_core(shm_consumer_core::<Word, P, C>(region, token)) })
    }

    /// [`recover_shm`](BytesRingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`recover_shm`](BytesRingBuffer::recover_shm).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<BytesPair<P, C>> {
        let region = Self::open(fd)?;
        // Unconditional: force both roles. No partial-failure path exists
        // (force cannot fail), so no lease can leak.
        let pt = force_claim_lease(&region, Role::Producer);
        let ct = force_claim_lease(&region, Role::Consumer);
        // SAFETY: validated byte-ring region; caches are rebuilt from the
        // live cursors by the core constructors.
        Ok(unsafe {
            (
                BytesProducer::from_core(shm_producer_core::<Word, P, C>(region.clone(), pt)),
                BytesConsumer::from_core(shm_consumer_core::<Word, P, C>(region, ct)),
            )
        })
    }
}

type Slot<T> = UnsafeCell<MaybeUninit<T>>;

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
    fn open(fd: BorrowedFd<'_>) -> io::Result<Arc<ShmRegion>> {
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
        assert!(min_capacity > 0, "capacity must be greater than zero");
        assert!(
            std::mem::align_of::<T>() <= 128,
            "element alignment exceeds the buffer offset alignment"
        );
        let capacity = min_capacity
            .checked_next_power_of_two()
            .expect("capacity too large to round up to a power of two");
        let (region, pt, ct) = create_region(fd, KIND_ELEMS, capacity, std::mem::size_of::<T>())?;
        // SAFETY: freshly initialized region matches the element-ring layout.
        Ok(unsafe {
            (
                Producer::from_core(shm_producer_core::<Slot<T>, P, C>(region.clone(), pt)),
                Consumer::from_core(shm_consumer_core::<Slot<T>, P, C>(region, ct)),
            )
        })
    }

    /// Attach to an existing element ring as the producer.
    ///
    /// # Safety
    ///
    /// Trust model, plus single-producer, plus `T` must be the exact type
    /// the ring was created with (only its size is validated).
    pub unsafe fn attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        let region = Self::open(fd)?;
        let token = claim_lease(&region, Role::Producer)?;
        // SAFETY: validated element-ring region.
        Ok(unsafe { Producer::from_core(shm_producer_core::<Slot<T>, P, C>(region, token)) })
    }

    /// Attach to an existing element ring as the consumer.
    ///
    /// # Safety
    ///
    /// As for [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn attach_shm_consumer(fd: BorrowedFd<'_>) -> io::Result<Consumer<T, P, C>> {
        let region = Self::open(fd)?;
        let token = claim_lease(&region, Role::Consumer)?;
        // SAFETY: validated element-ring region.
        Ok(unsafe { Consumer::from_core(shm_consumer_core::<Slot<T>, P, C>(region, token)) })
    }

    /// Unconditionally take over the producer role (see
    /// [`BytesRingBuffer::force_attach_shm_producer`]).
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::force_attach_shm_producer`], plus the `T`
    /// caveat of [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn force_attach_shm_producer(fd: BorrowedFd<'_>) -> io::Result<Producer<T, P, C>> {
        let region = Self::open(fd)?;
        let token = force_claim_lease(&region, Role::Producer);
        // SAFETY: validated element-ring region.
        Ok(unsafe { Producer::from_core(shm_producer_core::<Slot<T>, P, C>(region, token)) })
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
        let region = Self::open(fd)?;
        let token = force_claim_lease(&region, Role::Consumer);
        // SAFETY: validated element-ring region.
        Ok(unsafe { Consumer::from_core(shm_consumer_core::<Slot<T>, P, C>(region, token)) })
    }

    /// [`recover_shm`](RingBuffer::recover_shm) with explicit wait
    /// strategies.
    ///
    /// # Safety
    ///
    /// See [`BytesRingBuffer::recover_shm`], plus the `T` caveat of
    /// [`attach_shm_producer`](Self::attach_shm_producer).
    pub unsafe fn recover_shm_with(fd: BorrowedFd<'_>) -> io::Result<ElemPair<T, P, C>> {
        let region = Self::open(fd)?;
        let pt = force_claim_lease(&region, Role::Producer);
        let ct = force_claim_lease(&region, Role::Consumer);
        // SAFETY: validated element-ring region.
        Ok(unsafe {
            (
                Producer::from_core(shm_producer_core::<Slot<T>, P, C>(region.clone(), pt)),
                Consumer::from_core(shm_consumer_core::<Slot<T>, P, C>(region, ct)),
            )
        })
    }
}
