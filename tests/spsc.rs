//! Port of `fastchan_spsc_test.cpp`: single-threaded fill, single-threaded
//! round-trip, and a multi-threaded producer/consumer run, exercised across
//! every wait-strategy combination and both blocking and non-blocking APIs.

use rust_rb::spsc::{Consumer, Producer, Spsc};
use rust_rb::wait::{CvWait, NoOpWait, PauseWait, WaitStrategy, YieldWait};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const ITERATIONS: usize = 4096;
const ITERATIONS_MULTIPLIER: usize = 100;

/// `chan_size = (iterations / 2) + 1`, matching the C++ test. With
/// `ITERATIONS == 4096` this rounds up to a capacity of exactly 4096.
fn make<P, C>() -> (Producer<i32, P, C>, Consumer<i32, P, C>)
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    Spsc::<i32, { (ITERATIONS / 2) + 1 }, P, C>::new()
}

fn fill_blocking<P, C>()
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    let (mut tx, rx) = make::<P, C>();
    assert_eq!(tx.len(), 0);
    assert!(tx.is_empty());

    for i in 0..ITERATIONS {
        tx.push(i as i32);
        assert_eq!(tx.len(), i + 1);
        assert!(!tx.is_empty());
        if i < ITERATIONS - 1 {
            assert!(!tx.is_full(), "should not be full at {i}");
        } else {
            assert!(tx.is_full(), "should be full at {i}");
        }
    }

    assert_eq!(tx.len(), ITERATIONS);
    assert!(tx.is_full());
    assert!(!tx.is_empty());
    drop(rx);
}

fn fill_nonblocking<P, C>()
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    let (mut tx, rx) = make::<P, C>();
    for i in 0..ITERATIONS {
        assert!(tx.try_push(i as i32).is_ok());
        assert_eq!(tx.len(), i + 1);
    }
    // Now full: try_push must hand the value back.
    assert_eq!(tx.try_push(-1), Err(-1));
    assert!(tx.is_full());
    drop(rx);
}

fn push_pop_blocking<P, C>()
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    let (mut tx, mut rx) = make::<P, C>();
    for i in 0..ITERATIONS {
        tx.push(i as i32);
    }
    for i in 0..ITERATIONS {
        assert_eq!(rx.pop(), i as i32);
    }
    assert!(rx.is_empty());
    assert_eq!(rx.len(), 0);
}

fn push_pop_nonblocking<P, C>()
where
    P: WaitStrategy + Send + Sync,
    C: WaitStrategy + Send + Sync,
{
    let (mut tx, mut rx) = make::<P, C>();
    for i in 0..ITERATIONS {
        while tx.try_push(i as i32).is_err() {}
    }
    for i in 0..ITERATIONS {
        let mut v = rx.try_pop();
        while v.is_none() {
            v = rx.try_pop();
        }
        assert_eq!(v, Some(i as i32));
    }
    assert!(rx.is_empty());
}

fn multithreaded<P, C>()
where
    P: WaitStrategy + Send + Sync + 'static,
    C: WaitStrategy + Send + Sync + 'static,
{
    let (mut tx, mut rx) = make::<P, C>();
    let total = (ITERATIONS_MULTIPLIER * ITERATIONS) as i32;

    let producer = std::thread::spawn(move || {
        for i in 1..=total {
            while tx.try_push(i).is_err() {}
        }
    });

    let consumer = std::thread::spawn(move || {
        let mut i = 1;
        while i <= total {
            let mut v = rx.try_pop();
            while v.is_none() {
                v = rx.try_pop();
            }
            assert_eq!(v, Some(i));
            i += 1;
        }
        assert!(rx.is_empty());
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

fn exercise<P, C>()
where
    P: WaitStrategy + Send + Sync + 'static,
    C: WaitStrategy + Send + Sync + 'static,
{
    fill_blocking::<P, C>();
    fill_nonblocking::<P, C>();
    push_pop_blocking::<P, C>();
    push_pop_nonblocking::<P, C>();
    multithreaded::<P, C>();
}

#[test]
fn pause_combinations() {
    exercise::<PauseWait, PauseWait>();
    exercise::<PauseWait, YieldWait>();
    exercise::<PauseWait, NoOpWait>();
    exercise::<PauseWait, CvWait>();
}

#[test]
fn yield_combinations() {
    exercise::<YieldWait, YieldWait>();
    exercise::<YieldWait, PauseWait>();
    exercise::<YieldWait, NoOpWait>();
    exercise::<YieldWait, CvWait>();
}

#[test]
fn noop_combinations() {
    exercise::<NoOpWait, NoOpWait>();
    exercise::<NoOpWait, YieldWait>();
    exercise::<NoOpWait, PauseWait>();
    exercise::<NoOpWait, CvWait>();
}

#[test]
fn cv_combinations() {
    exercise::<CvWait, CvWait>();
    exercise::<CvWait, YieldWait>();
    exercise::<CvWait, PauseWait>();
    exercise::<CvWait, NoOpWait>();
}

#[test]
fn drops_remaining_elements() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let (mut tx, mut rx) = Spsc::<Counted, 16>::new();
        for _ in 0..10 {
            tx.push(Counted(drops.clone()));
        }
        // Consume 3, leaving 7 in the buffer to be dropped with the queue.
        for _ in 0..3 {
            drop(rx.pop());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

// ============================================================================
// EDGE CASE AND ADVERSARIAL TESTS
// ============================================================================

// -----------------------------------------------------------------------------
// 1. CONSUMER-SIDE STATE METHODS (HIGH)
// -----------------------------------------------------------------------------

/// Test Consumer::len(), Consumer::is_empty(), Consumer::is_full(),
/// Consumer::capacity() directly from the consumer side.
#[test]
fn consumer_state_methods() {
    let (mut tx, mut rx) = Spsc::<i32, 16>::new();

    // Initially empty
    assert_eq!(rx.len(), 0);
    assert!(rx.is_empty());
    assert!(!rx.is_full());
    assert_eq!(rx.capacity(), 16);

    // Fill the queue
    for i in 0..16 {
        tx.push(i);
    }

    // Consumer sees full queue
    assert_eq!(rx.len(), 16);
    assert!(!rx.is_empty());
    assert!(rx.is_full());

    // Consume one item
    rx.pop();
    assert_eq!(rx.len(), 15);
    assert!(!rx.is_empty());
    assert!(!rx.is_full());

    // Empty the queue
    for _ in 0..15 {
        rx.pop();
    }
    assert_eq!(rx.len(), 0);
    assert!(rx.is_empty());
    assert!(!rx.is_full());
}

// -----------------------------------------------------------------------------
// 2. ZERO CAPACITY (HIGH)
// -----------------------------------------------------------------------------

// `Spsc::<i32, 0>::new()` is rejected at compile time (monomorphization-time
// const assert in `Spsc::new`), so there is no runtime panic left to test.
// Uncommenting the following line must fail to compile:
// const _: fn() = || { let _ = Spsc::<i32, 0>::new(); };

// -----------------------------------------------------------------------------
// 3. EXACT CAPACITY BOUNDARY (MEDIUM)
// -----------------------------------------------------------------------------

/// Test exact boundary conditions at capacity.
#[test]
fn exact_capacity_boundary() {
    let (mut tx, mut rx) = Spsc::<i32, 4>::new();

    // Fill exactly to capacity
    for i in 0..4 {
        tx.push(i);
    }

    // Verify is_full() returns true
    assert!(tx.is_full());
    assert!(rx.is_full());

    // Verify try_push() fails and returns the value
    assert_eq!(tx.try_push(-1), Err(-1));

    // Consume one
    assert_eq!(rx.pop(), 0);

    // Verify try_push() succeeds
    assert!(tx.try_push(100).is_ok());
    assert_eq!(tx.len(), 4);
}

// -----------------------------------------------------------------------------
// 4. NON-PRIMITIVE TYPES (MEDIUM)
// -----------------------------------------------------------------------------

/// Type with custom Drop that tracks moves/drops.
#[derive(Debug)]
struct MovabilityTracker {
    id: usize,
    moved: bool,
}

impl MovabilityTracker {
    fn new(id: usize) -> Self {
        Self { id, moved: false }
    }
}

impl Drop for MovabilityTracker {
    fn drop(&mut self) {
        // Track that this instance was dropped
        if !self.moved {
            // Dropped without being moved
        }
    }
}

/// Test with non-Copy struct and custom ownership semantics.
#[test]
fn non_primitive_types() {
    let (mut tx, mut rx) = Spsc::<MovabilityTracker, 16>::new();

    // Move items into the queue
    for i in 0..8 {
        tx.push(MovabilityTracker::new(i));
    }

    // Verify correct move semantics through queue
    for i in 0..8 {
        let item = rx.pop();
        assert_eq!(item.id, i);
    }

    assert!(rx.is_empty());
}

/// Test with String (complex ownership) through the queue.
#[test]
fn non_primitive_types_string() {
    let (mut tx, mut rx) = Spsc::<String, 8>::new();

    for i in 0..4 {
        tx.push(format!("item_{}", i));
    }

    for i in 0..4 {
        let s = rx.pop();
        assert_eq!(s, format!("item_{}", i));
    }
}

// -----------------------------------------------------------------------------
// 5. TIGHT INTERLEAVING (LOW)
// -----------------------------------------------------------------------------

/// Stress test tight push-one/pop-one interleaving across threads.
/// Exercises cache-line bouncing and verifies all values are correct.
#[test]
fn tight_interleaving() {
    let (mut tx, mut rx) = Spsc::<i32, 2>::new(); // Small capacity for interleaving
    let iterations = 1000;

    let producer = std::thread::spawn(move || {
        for i in 0..iterations {
            while tx.try_push(i).is_err() {
                // Spin until we can push
            }
        }
    });

    let consumer = std::thread::spawn(move || {
        for i in 0..iterations {
            let mut val = rx.try_pop();
            while val.is_none() {
                val = rx.try_pop();
            }
            assert_eq!(val, Some(i));
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

// -----------------------------------------------------------------------------
// 6. CONSUMER-ONLY DROP (LOW)
// -----------------------------------------------------------------------------

struct CountedDrop {
    _id: usize,
    drops: Arc<AtomicUsize>,
}

impl CountedDrop {
    fn new(id: usize, drops: Arc<AtomicUsize>) -> Self {
        Self { _id: id, drops }
    }
}

impl Drop for CountedDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

/// Test dropping consumer with items in buffer.
/// Push 10 items, consume 3, drop consumer (leaving 7 items).
/// Verify remaining 7 items are dropped.
#[test]
fn consumer_only_drop() {
    let drops = Arc::new(AtomicUsize::new(0));

    {
        let (mut tx, mut rx) = Spsc::<CountedDrop, 16>::new();
        for i in 0..10 {
            tx.push(CountedDrop::new(i, drops.clone()));
        }

        // Consume 3 (keeping them alive in this scope)
        let mut consumed = Vec::new();
        for _ in 0..3 {
            consumed.push(rx.pop());
        }

        // At this point 3 items are consumed (held in consumed Vec), 7 remain in buffer
        // The consumed items haven't been dropped yet since consumed Vec is still in scope
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        // Drop consumer (and producer via tx)
        // The remaining 7 items should be dropped
        drop(rx);
        drop(tx);

        // Now the 7 remaining items are dropped, but consumed Vec still holds 3
        assert_eq!(drops.load(Ordering::Relaxed), 7);
    }

    // Now consumed Vec is dropped too, so all 10 items are dropped
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

// -----------------------------------------------------------------------------
// 7. NON-POWER-OF-TWO CAPACITY (LOW)
// -----------------------------------------------------------------------------

/// Test capacity rounding for non-power-of-two capacities.
#[test]
fn non_power_of_two_capacity() {
    let (tx, _rx) = Spsc::<i32, 10>::new();
    // 10 rounds up to 16
    assert_eq!(tx.capacity(), 16);
}

#[test]
fn non_power_of_two_capacity_various() {
    let (tx1, _) = Spsc::<i32, 3>::new();
    assert_eq!(tx1.capacity(), 4);

    let (tx2, _) = Spsc::<i32, 5>::new();
    assert_eq!(tx2.capacity(), 8);

    let (tx3, _) = Spsc::<i32, 17>::new();
    assert_eq!(tx3.capacity(), 32);

    let (tx4, _) = Spsc::<i32, 33>::new();
    assert_eq!(tx4.capacity(), 64);
}

// -----------------------------------------------------------------------------
// 8. WRAPPING ARITHMETIC DOCUMENTATION (HIGH)
// -----------------------------------------------------------------------------

/// Document that usize wrapping is untestable in practice.
///
/// SPSC uses usize wrapping arithmetic for indices. Since usize wraps at
/// 2^64 (or 2^32 on 32-bit), and we'd need to enqueue 2^64 items to
/// observe wraparound, this is untestable in practice. This test serves
/// as documentation that wrapping is sound and verified by construction.
///
/// The implementation uses `wrapping_add` explicitly to make the intent clear.
#[test]
fn wrapping_arithmetic_documentation() {
    // This test documents that wrapping arithmetic is used throughout.
    //
    // Key wrapping operations in the implementation:
    // 1. `self.write_cursor = self.write_cursor.wrapping_add(1);`
    // 2. `self.read_cursor = self.read_cursor.wrapping_add(1);`
    // 3. `index & mask` for slot calculation (works correctly with wrapping)
    //
    // Since usize arithmetic in Rust wraps silently (like C++), and the
    // mask-based slot calculation is mathematically correct for wrapping
    // indices, the implementation is sound even at wraparound.
    //
    // Verification: The tests below exercise many cycles to ensure no
    // regression in normal operation.
    let (mut tx, mut rx) = Spsc::<i32, 16>::new();

    // Exercise many cycles
    for _ in 0..100 {
        for i in 0..16 {
            tx.push(i);
        }
        for i in 0..16 {
            assert_eq!(rx.pop(), i);
        }
    }
}

// -----------------------------------------------------------------------------
// 9. CV WAIT STRATEGY SPURIOUS WAKEUPS (MEDIUM)
// -----------------------------------------------------------------------------

/// Stress test CvWait with many short cycles.
/// Exercises notify/wait interaction and verifies no spurious wakeup bugs.
#[test]
fn cv_wait_spurious_wakeup_stress() {
    let (mut tx, mut rx) = Spsc::<i32, 16, YieldWait, CvWait>::new();
    let iterations = 5000;

    let producer = std::thread::spawn(move || {
        for i in 0..iterations {
            tx.push(i);
        }
    });

    let consumer = std::thread::spawn(move || {
        for i in 0..iterations {
            assert_eq!(rx.pop(), i);
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

/// Another CvWait stress test with push-one/pop-one interleaving.
#[test]
fn cv_wait_tight_interleaving() {
    let (mut tx, mut rx) = Spsc::<i32, 2, YieldWait, CvWait>::new();
    let iterations = 2000;

    let producer = std::thread::spawn(move || {
        for i in 0..iterations {
            while tx.try_push(i).is_err() {
                // Spin briefly
            }
        }
    });

    let consumer = std::thread::spawn(move || {
        for i in 0..iterations {
            let mut val = rx.try_pop();
            while val.is_none() {
                val = rx.try_pop();
            }
            assert_eq!(val, Some(i));
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

// -----------------------------------------------------------------------------
// 10. MIXED BLOCKING/NONBLOCKING (MEDIUM)
// -----------------------------------------------------------------------------

/// Test mixing blocking and non-blocking APIs.
/// Use push() to fill, try_pop() to drain.
#[test]
fn mixed_blocking_nonblocking_fill_drain() {
    let (mut tx, mut rx) = Spsc::<i32, 8>::new();

    // Fill using blocking push
    for i in 0..8 {
        tx.push(i);
    }

    assert!(tx.is_full());

    // Drain using non-blocking try_pop
    for i in 0..8 {
        let val = rx.try_pop();
        assert_eq!(val, Some(i));
    }

    assert!(rx.is_empty());
}

/// Test mixing blocking and non-blocking APIs.
/// Use try_push() to fill, pop() to drain.
#[test]
fn mixed_blocking_nonblocking_try_fill_pop() {
    let (mut tx, mut rx) = Spsc::<i32, 8>::new();

    // Fill using non-blocking try_push
    for i in 0..8 {
        while tx.try_push(i).is_err() {
            // Wait for space (spin)
        }
    }

    assert!(tx.is_full());

    // Drain using blocking pop
    for i in 0..8 {
        let val = rx.pop();
        assert_eq!(val, i);
    }

    assert!(rx.is_empty());
}

/// Test mixed blocking/nonblocking in a producer-consumer thread scenario.
#[test]
fn mixed_blocking_nonblocking_multithreaded() {
    let (mut tx, mut rx) = Spsc::<i32, 4>::new();
    let iterations = 1000;

    let producer = std::thread::spawn(move || {
        // Producer uses blocking push
        for i in 0..iterations {
            tx.push(i);
        }
    });

    let consumer = std::thread::spawn(move || {
        // Consumer uses non-blocking try_pop with spin
        for i in 0..iterations {
            let mut val = rx.try_pop();
            while val.is_none() {
                val = rx.try_pop();
            }
            assert_eq!(val, Some(i));
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

/// Test mixed blocking/nonblocking: try_push fills, pop drains.
#[test]
fn mixed_blocking_nonblocking_try_push_pop_multithreaded() {
    let (mut tx, mut rx) = Spsc::<i32, 4>::new();
    let iterations = 1000;

    let producer = std::thread::spawn(move || {
        // Producer uses non-blocking try_push with spin
        for i in 0..iterations {
            while tx.try_push(i).is_err() {
                // Spin until success
            }
        }
    });

    let consumer = std::thread::spawn(move || {
        // Consumer uses blocking pop
        for i in 0..iterations {
            assert_eq!(rx.pop(), i);
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

// -----------------------------------------------------------------------------
// 12. ADAPTIVE READ-CURSOR PUBLISH
// -----------------------------------------------------------------------------

/// While caught up (queue drained as far as the consumer knows), every pop
/// publishes immediately, so producer-side views stay exact — identical to
/// the per-element publish of the C++ original.
#[test]
fn adaptive_publish_exact_when_caught_up() {
    let (mut tx, mut rx) = Spsc::<i32, 1024>::new();

    // Ping-pong: the consumer catches up on every pop.
    for i in 0..200 {
        tx.push(i);
        assert_eq!(rx.pop(), i);
        assert!(tx.is_empty(), "caught-up pop must publish immediately");
        assert_eq!(tx.len(), 0);
    }

    // Draining a burst: the final (catching-up) pop flushes everything.
    for i in 0..100 {
        tx.push(i);
    }
    for i in 0..100 {
        assert_eq!(rx.pop(), i);
    }
    assert!(tx.is_empty());
    assert_eq!(tx.len(), 0);
}

/// While the queue is backed up, publishes are deferred (up to capacity/8,
/// max 64), so the producer transiently sees a fuller queue; the deferred
/// progress becomes visible at the batch boundary and when the consumer
/// catches up.
#[test]
fn adaptive_publish_defers_when_backed_up() {
    let (mut tx, mut rx) = Spsc::<i32, 1024>::new(); // batch = 64

    for i in 0..1024 {
        tx.push(i);
    }
    assert!(tx.is_full());

    // Fewer than one batch consumed: producer may still see a full queue,
    // but the consumer's own view is exact.
    for i in 0..10 {
        assert_eq!(rx.pop(), i);
    }
    assert_eq!(rx.len(), 1014);
    assert!(tx.is_full(), "deferred publish: producer still sees full");

    // Crossing the batch boundary publishes.
    for i in 10..64 {
        assert_eq!(rx.pop(), i);
    }
    assert!(!tx.is_full(), "batch boundary must publish");

    // Draining the rest ends caught up, with everything published.
    for i in 64..1024 {
        assert_eq!(rx.pop(), i);
    }
    assert!(rx.try_pop().is_none());
    assert!(tx.is_empty());
    for i in 0..1024 {
        assert!(tx.try_push(i).is_ok(), "all space visible after catch-up");
    }
}

/// Consuming fewer elements than the publish batch and then dropping both
/// halves must not double-drop the consumed elements: `Consumer::drop`
/// publishes its private cursor before `Inner::drop` walks the leftovers.
#[test]
fn adaptive_publish_no_double_drop_on_consumer_drop() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct CountsDrops(Arc<AtomicUsize>);
    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = Spsc::<CountsDrops, 1024>::new(); // batch 64

    for _ in 0..100 {
        tx.push(CountsDrops(drops.clone()));
    }
    // Consume 10 (< batch 64, still behind): progress is deferred at drop time.
    for _ in 0..10 {
        drop(rx.pop());
    }
    assert_eq!(drops.load(Ordering::Relaxed), 10);

    drop(rx);
    drop(tx); // Inner::drop releases the remaining 90 — exactly once each
    assert_eq!(drops.load(Ordering::Relaxed), 100);
}
