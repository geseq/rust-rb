//! Port of `fastchan_spsc_test.cpp`: single-threaded fill, single-threaded
//! round-trip, and a multi-threaded producer/consumer run, exercised across
//! every wait-strategy combination and both blocking and non-blocking APIs.

use rust_rb::spsc::{Consumer, Producer, Spsc};
use rust_rb::wait::{CvWait, NoOpWait, PauseWait, WaitStrategy, YieldWait};

const ITERATIONS: usize = 4096;
const ITERATIONS_MULTIPLIER: usize = 100;

/// `chan_size = (iterations / 2) + 1`, matching the C++ test. With
/// `ITERATIONS == 4096` this rounds up to a capacity of exactly 4096.
fn make<P, G>() -> (Producer<i32, P, G>, Consumer<i32, P, G>)
where
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    Spsc::<i32, { (ITERATIONS / 2) + 1 }, P, G>::new()
}

fn fill_blocking<P, G>()
where
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    let (mut tx, rx) = make::<P, G>();
    assert_eq!(tx.len(), 0);
    assert!(tx.is_empty());

    for i in 0..ITERATIONS {
        tx.put(i as i32);
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

fn fill_nonblocking<P, G>()
where
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    let (mut tx, rx) = make::<P, G>();
    for i in 0..ITERATIONS {
        assert!(tx.try_put(i as i32).is_ok());
        assert_eq!(tx.len(), i + 1);
    }
    // Now full: try_put must hand the value back.
    assert_eq!(tx.try_put(-1), Err(-1));
    assert!(tx.is_full());
    drop(rx);
}

fn put_get_blocking<P, G>()
where
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    let (mut tx, mut rx) = make::<P, G>();
    for i in 0..ITERATIONS {
        tx.put(i as i32);
    }
    for i in 0..ITERATIONS {
        assert_eq!(rx.get(), i as i32);
    }
    assert!(rx.is_empty());
    assert_eq!(rx.len(), 0);
}

fn put_get_nonblocking<P, G>()
where
    P: WaitStrategy + Send + Sync,
    G: WaitStrategy + Send + Sync,
{
    let (mut tx, mut rx) = make::<P, G>();
    for i in 0..ITERATIONS {
        while tx.try_put(i as i32).is_err() {}
    }
    for i in 0..ITERATIONS {
        let mut v = rx.try_get();
        while v.is_none() {
            v = rx.try_get();
        }
        assert_eq!(v, Some(i as i32));
    }
    assert!(rx.is_empty());
}

fn multithreaded<P, G>()
where
    P: WaitStrategy + Send + Sync + 'static,
    G: WaitStrategy + Send + Sync + 'static,
{
    let (mut tx, mut rx) = make::<P, G>();
    let total = (ITERATIONS_MULTIPLIER * ITERATIONS) as i32;

    let producer = std::thread::spawn(move || {
        for i in 1..=total {
            while tx.try_put(i).is_err() {}
        }
    });

    let consumer = std::thread::spawn(move || {
        let mut i = 1;
        while i <= total {
            let mut v = rx.try_get();
            while v.is_none() {
                v = rx.try_get();
            }
            assert_eq!(v, Some(i));
            i += 1;
        }
        assert!(rx.is_empty());
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

fn exercise<P, G>()
where
    P: WaitStrategy + Send + Sync + 'static,
    G: WaitStrategy + Send + Sync + 'static,
{
    fill_blocking::<P, G>();
    fill_nonblocking::<P, G>();
    put_get_blocking::<P, G>();
    put_get_nonblocking::<P, G>();
    multithreaded::<P, G>();
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
            tx.put(Counted(drops.clone()));
        }
        // Consume 3, leaving 7 in the buffer to be dropped with the queue.
        for _ in 0..3 {
            drop(rx.get());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}
