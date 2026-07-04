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
