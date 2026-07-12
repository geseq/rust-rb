//! Wait strategies.
//!
//! A faithful port of `wait_strategy.hpp`. Each strategy is a zero-sized (or,
//! for the condition-variable strategy, small) type chosen at compile time, so
//! the queue's hot path pays nothing for the abstraction — the calls inline
//! away exactly as the C++ `class` template parameters do.
//!
//! `wait` takes a predicate. The spin strategies ignore it (they just hint the
//! CPU and let the caller re-check the loop condition), matching the C++ where
//! only [`CvWait`] consults the predicate.

use std::sync::atomic::{fence, AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Marker for wait strategies that are safe across process boundaries.
///
/// A cross-process strategy must carry no process-local state that the
/// *other* side's `notify` needs to reach: the spin strategies qualify
/// (waiting is purely local re-checking; `notify` is a no-op), while
/// [`CvWait`] does not — its mutex/condvar live in one process's memory.
///
/// # Safety
///
/// Implementors assert that `wait` makes progress without ever requiring a
/// `notify` delivered from another process (e.g. by re-checking the
/// predicate or spinning), and that the strategy value works correctly when
/// each process constructs its own instance.
pub unsafe trait CrossProcess: WaitStrategy {}

/// Marker for wait strategies that make progress **without ever needing a
/// peer notify**: pure spins, yields, and timed sleeps.
///
/// The multi-consumer rings ([`crate::spmc`]) require this on both sides:
/// with N waiters, a notify-dependent strategy needs per-waiter wake state
/// ([`CvWait`]'s single shared flag can skip a parked waiter, adding its
/// full timeout to the wake latency), and the gating producer's publish
/// path must never pay a lock/signal per consumer flush. The SPSC rings
/// accept any [`WaitStrategy`], including [`CvWait`].
///
/// ```compile_fail
/// // CvWait is not SelfTimed: the multi-consumer ring rejects it.
/// use rust_rb::{spmc, CvWait};
/// let _ = spmc::RingBuffer::<u64, CvWait, CvWait>::with_wait_strategies(8);
/// ```
pub trait SelfTimed: WaitStrategy {}

impl SelfTimed for NoOpWait {}
impl SelfTimed for PauseWait {}
impl SelfTimed for YieldWait {}
impl<const NANOS: u64> SelfTimed for SleepWait<NANOS> {}
impl<const S: u32, const Y: u32, const MIN: u64, const MAX: u64> SelfTimed
    for BackoffWait<S, Y, MIN, MAX>
{
}

// SAFETY: pure spinning; no shared state, notify is a no-op.
unsafe impl CrossProcess for NoOpWait {}
// SAFETY: pure spinning with a CPU hint; no shared state.
unsafe impl CrossProcess for PauseWait {}
// SAFETY: spinning with a scheduler yield; no shared state.
unsafe impl CrossProcess for YieldWait {}
// SAFETY: self-timed sleeping; progress needs no peer notify.
unsafe impl<const NANOS: u64> CrossProcess for SleepWait<NANOS> {}
// SAFETY: spins/yields/sleeps re-checking the predicate locally; no notify.
unsafe impl<const S: u32, const Y: u32, const MIN: u64, const MAX: u64> CrossProcess
    for BackoffWait<S, Y, MIN, MAX>
{
}

/// Behaviour shared by every wait strategy.
///
/// Implementors must be cheap to default-construct: the queue builds one
/// instance for the push side and one for the pop side.
pub trait WaitStrategy: Default {
    /// Called while a blocking `push`/`pop` is parked waiting for progress.
    ///
    /// `pred` returns `true` once the waited-for condition holds. Spin
    /// strategies ignore it; blocking strategies may use it to avoid lost
    /// wake-ups.
    fn wait<P: FnMut() -> bool>(&self, pred: P);

    /// Wake any thread parked in [`wait`](WaitStrategy::wait). A no-op for the
    /// spin strategies.
    ///
    /// Contract for implementors: if `wait` can park indefinitely (an
    /// untimed futex/park), a `notify` issued after the waited-for condition
    /// became true must wake it. The queue calls `notify` after every
    /// progress publish; strategies whose `wait` re-checks on a timeout (as
    /// all built-in ones do) may treat `notify` as advisory.
    #[inline(always)]
    fn notify(&self) {}
}

/// Busy-spin emitting the architecture's "pause"/"yield" hint each turn
/// (`PAUSE` on x86, `YIELD`/`ISB` on ARM via [`core::hint::spin_loop`]).
///
/// Lowest latency; burns a core. Port of `PauseWaitStrategy`.
#[derive(Default)]
pub struct PauseWait;

impl WaitStrategy for PauseWait {
    #[inline(always)]
    fn wait<P: FnMut() -> bool>(&self, _pred: P) {
        core::hint::spin_loop();
    }
}

/// Yield the remainder of the thread's time slice to the scheduler each turn.
///
/// Port of `YieldWaitStrategy`. The default strategy, matching the C++.
#[derive(Default)]
pub struct YieldWait;

impl WaitStrategy for YieldWait {
    #[inline(always)]
    fn wait<P: FnMut() -> bool>(&self, _pred: P) {
        std::thread::yield_now();
    }
}

/// Spin as tightly as possible, doing nothing between checks.
///
/// Port of `NoOpWaitStrategy`. Slightly lower latency than [`PauseWait`] on
/// some microarchitectures at the cost of more power and SMT-sibling
/// starvation.
#[derive(Default)]
pub struct NoOpWait;

impl WaitStrategy for NoOpWait {
    #[inline(always)]
    fn wait<P: FnMut() -> bool>(&self, _pred: P) {}
}

/// Sleep a fixed `NANOS` nanoseconds each turn (`parkNanos` shape).
///
/// The lowest-CPU *self-timed* tier: no notify from the peer is ever needed
/// (unlike [`CvWait`]), so it works across processes. The effective
/// granularity is the OS timer — tens of microseconds on Linux with default
/// timerslack — so treat `NANOS` as a floor, not a promise.
///
/// ```
/// use rust_rb::{RingBuffer, SleepWait, YieldWait};
///
/// // Producer sleeps 50µs between full-ring re-checks; consumer yields.
/// let (mut tx, mut rx) =
///     RingBuffer::<u64, SleepWait<50_000>, YieldWait>::with_wait_strategies(16);
/// tx.push(7);
/// assert_eq!(rx.pop(), 7);
/// ```
#[derive(Default)]
pub struct SleepWait<const NANOS: u64 = 100_000>;

impl<const NANOS: u64> WaitStrategy for SleepWait<NANOS> {
    #[inline]
    fn wait<P: FnMut() -> bool>(&self, _pred: P) {
        std::thread::sleep(Duration::from_nanos(NANOS));
    }
}

/// Escalating backoff: spin `SPINS` turns, yield `YIELDS` turns, then sleep
/// with exponential doubling from `MIN_SLEEP_NANOS` to `MAX_SLEEP_NANOS`
/// (the Aeron `BackoffIdleStrategy` shape).
///
/// Runs one full escalation episode *internally*, consulting `pred` every
/// turn and returning as soon as it holds — so each blocking call starts a
/// fresh episode and no cross-call state is needed. Latency-friendly when
/// waits are usually short but must not burn a core when they are long.
///
/// ```
/// use rust_rb::{BackoffWait, RingBuffer};
///
/// let (mut tx, mut rx) =
///     RingBuffer::<u64, BackoffWait, BackoffWait>::with_wait_strategies(16);
/// tx.push(1);
/// assert_eq!(rx.pop(), 1);
/// ```
#[derive(Default)]
pub struct BackoffWait<
    const SPINS: u32 = 100,
    const YIELDS: u32 = 100,
    const MIN_SLEEP_NANOS: u64 = 1_000,
    const MAX_SLEEP_NANOS: u64 = 1_000_000,
>;

impl<
        const SPINS: u32,
        const YIELDS: u32,
        const MIN_SLEEP_NANOS: u64,
        const MAX_SLEEP_NANOS: u64,
    > WaitStrategy for BackoffWait<SPINS, YIELDS, MIN_SLEEP_NANOS, MAX_SLEEP_NANOS>
{
    fn wait<P: FnMut() -> bool>(&self, mut pred: P) {
        for _ in 0..SPINS {
            if pred() {
                return;
            }
            core::hint::spin_loop();
        }
        for _ in 0..YIELDS {
            if pred() {
                return;
            }
            std::thread::yield_now();
        }
        let mut ns = MIN_SLEEP_NANOS.max(1);
        loop {
            if pred() {
                return;
            }
            std::thread::sleep(Duration::from_nanos(ns));
            ns = ns.saturating_mul(2).min(MAX_SLEEP_NANOS);
        }
    }
}

/// Park on a condition variable with a timed re-check (nominally 100 ns;
/// effectively the OS timer granularity, tens of microseconds on Linux with
/// default timerslack).
///
/// Port of `CVWaitStrategy`. Lowest CPU usage; highest wake-up latency. Unlike
/// the spin strategies this consults `pred` so a notification that arrives
/// before the thread parks is not lost.
pub struct CvWait {
    mutex: Mutex<()>,
    cv: Condvar,
    /// Set (under the mutex) while the peer is inside [`wait`](Self::wait).
    /// Lets `notify` skip the mutex entirely on the hot path when nobody is
    /// parked — which is the overwhelmingly common case.
    waiting: AtomicBool,
}

impl Default for CvWait {
    #[inline]
    fn default() -> Self {
        Self {
            mutex: Mutex::new(()),
            cv: Condvar::new(),
            waiting: AtomicBool::new(false),
        }
    }
}

impl WaitStrategy for CvWait {
    #[inline]
    fn wait<P: FnMut() -> bool>(&self, mut pred: P) {
        let guard = self.mutex.lock().unwrap();
        self.waiting.store(true, Ordering::Relaxed);
        // Order the `waiting` store before the predicate's load. Paired with
        // the notifier's fence, this is the classic store-buffering pattern:
        // either the notifier sees `waiting == true` and takes the slow path,
        // or this predicate check sees the notifier's published progress and
        // never parks — a stale read on both sides at once is impossible.
        fence(Ordering::SeqCst);
        // wait_timeout_while returns immediately if `pred` is already
        // satisfied. The timed re-check bounds any residual staleness; note
        // the effective granularity is the OS timer (tens of microseconds
        // with default Linux timerslack), not literally 100 ns.
        let _ = self
            .cv
            .wait_timeout_while(guard, Duration::from_nanos(100), |_| !pred())
            .unwrap();
        self.waiting.store(false, Ordering::Relaxed);
    }

    #[inline]
    fn notify(&self) {
        // Order the caller's progress publish (a `Release` store) before the
        // `waiting` load — see the matching fence in `wait`.
        fence(Ordering::SeqCst);
        // Fast path: nobody is parked (or about to park), so there is nothing
        // to wake and we avoid the mutex altogether.
        if self.waiting.load(Ordering::Relaxed) {
            // Take the lock so we never signal between the waiter's predicate
            // check and its park, which would otherwise be a lost wake-up.
            let _guard = self.mutex.lock().unwrap();
            self.cv.notify_all();
        }
    }
}
