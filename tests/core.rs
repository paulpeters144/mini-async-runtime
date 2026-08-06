use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use mini_async_runtime::blocking::spawn_blocking;
use mini_async_runtime::executor::Runtime;
use mini_async_runtime::io::{read, write};
use mini_async_runtime::timer_wheel::{sleep, yield_now};

#[test]
fn concurrent_sleeps_and_yield_counter() {
    let mut runtime = Runtime::new();

    let sleeps_completed = Rc::new(Cell::new(0usize));
    let durations = [Duration::from_millis(50), Duration::from_millis(100)];

    for &d in &durations {
        let sc = sleeps_completed.clone();
        runtime.spawn(async move {
            sleep(d).await;
            sc.set(sc.get() + 1);
        });
    }

    let yields = 5;
    let counter = Rc::new(Cell::new(0usize));
    {
        let c = counter.clone();
        runtime.spawn(async move {
            for _ in 0..yields {
                yield_now().await;
                c.set(c.get() + 1);
            }
        });
    }

    let start = Instant::now();
    runtime.run().expect("run should not fail");
    let elapsed = start.elapsed();

    assert_eq!(sleeps_completed.get(), durations.len());
    assert_eq!(counter.get(), yields);
    assert!(elapsed >= Duration::from_millis(100));
    assert!(elapsed < Duration::from_millis(200));
}

// Phase 2 milestone: two concurrent tasks exchange bytes over a socket pair
// using the public `io::read` / `io::write` futures, all inside one `run()`.
// This proves the full reactor chain: WouldBlock → register → park → dispatch →
// repoll → read/write → deregister → reactor empties → run returns.
#[test]
fn echo_demo_reader_and_writer_over_socket_pair() {
    let mut runtime = Runtime::new();
    let (tx, rx) = mio::net::UnixStream::pair().unwrap();

    runtime.spawn(async move {
        write(tx, b"hello echo".to_vec()).await;
    });

    let got = Rc::new(RefCell::new(Vec::new()));
    let got_writer = got.clone();
    runtime.spawn(async move {
        *got_writer.borrow_mut() = read(rx).await;
    });

    runtime.run().expect("run should not fail");

    assert_eq!(got.borrow().as_slice(), b"hello echo");
}

// Phase 3 milestone: `spawn_blocking` interleaves with a timer — the blocking
// closure runs on a worker thread and does not stall the executor. The 50ms
// timer task must fire while the 200ms closure is still on the worker, so the
// total wall time is the max (≈200ms), not the sum (250ms).
#[test]
fn spawn_blocking_interleaves_with_timer() {
    let mut runtime = Runtime::new();

    let blocking_done = Rc::new(Cell::new(false));
    let timer_fired = Rc::new(Cell::new(false));

    {
        let done = blocking_done.clone();
        runtime.spawn(async move {
            let result = spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(200));
                42u32
            })
            .await;
            assert_eq!(result, 42);
            done.set(true);
        });
    }

    {
        let fired = timer_fired.clone();
        runtime.spawn(async move {
            sleep(Duration::from_millis(50)).await;
            fired.set(true);
        });
    }

    let start = Instant::now();
    runtime.run().expect("run should not fail");
    let elapsed = start.elapsed();

    assert!(blocking_done.get());
    assert!(timer_fired.get());
    assert!(elapsed >= Duration::from_millis(200));
    // Serial execution would take 250ms; interleaving beats that.
    assert!(elapsed < Duration::from_millis(240));
}

// A BlockingTask yields exactly the closure's return value across the worker
// boundary.
#[test]
fn spawn_blocking_round_trip_value() {
    let mut runtime = Runtime::new();
    let got = Rc::new(RefCell::new(String::new()));
    {
        let got = got.clone();
        runtime.spawn(async move {
            let result = spawn_blocking(|| "hello worker".to_string()).await;
            *got.borrow_mut() = result;
        });
    }

    runtime.run().expect("run should not fail");

    assert_eq!(got.borrow().as_str(), "hello worker");
}

// Two `spawn_blocking` futures in two tasks both complete. With a single
// worker they run one after another on the worker thread, but the executor
// stays live throughout and both results cross back.
#[test]
fn two_spawn_blockings_both_complete() {
    let mut runtime = Runtime::new();
    let completed = Rc::new(Cell::new(0usize));
    for i in 0..2usize {
        let completed = completed.clone();
        runtime.spawn(async move {
            let result = spawn_blocking(move || i * 10).await;
            assert_eq!(result, i * 10);
            completed.set(completed.get() + 1);
        });
    }

    runtime.run().expect("run should not fail");

    assert_eq!(completed.get(), 2);
}

// Step 14 — panic semantics: a panicking closure ships its payload back over
// the result channel and it is resumed inside the waiting task's poll, so
// `run()` panics with the original payload — never hangs.
#[test]
#[should_panic(expected = "blocking closure exploded")]
fn panicking_blocking_closure_makes_run_panic() {
    let mut runtime = Runtime::new();
    runtime.spawn(async {
        let _ = spawn_blocking(|| -> () {
            panic!("blocking closure exploded");
        })
        .await;
    });
    let _ = runtime.run();
}
