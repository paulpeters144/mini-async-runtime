use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use mar::Mar;
use mar::task::spawn_blocking;
use mar::time::sleep;

// Phase 3 milestone: `spawn_blocking` interleaves with a timer. `spawn_blocking`
// eagerly sends the job to a worker so the 200ms closure starts running before
// the 50ms `sleep` is even polled. Both timers run concurrently, so the total
// wall time is the max (~200ms), not the sum (250ms).
#[test]
fn spawn_blocking_interleaves_with_timer() {
    let blocking_done = Rc::new(Cell::new(false));
    let timer_fired = Rc::new(Cell::new(false));

    let start = Instant::now();

    {
        let blocking_done = blocking_done.clone();
        let timer_fired = timer_fired.clone();
        Mar::run(async move {
            let blocking = spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(200));
                42u32
            });

            // Worker is already running the 200ms closure. Park on the
            // timer heap for 50ms while the worker keeps ticking.
            sleep(Duration::from_millis(50)).await;
            timer_fired.set(true);

            let result = blocking.await;
            assert_eq!(result, 42);
            blocking_done.set(true);
        })
        .expect("run should not fail");
    }

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
    let got = Rc::new(RefCell::new(String::new()));
    {
        let got = got.clone();
        Mar::run(async move {
            let result = spawn_blocking(|| "hello worker".to_string()).await;
            *got.borrow_mut() = result;
        })
        .expect("run should not fail");
    }

    assert_eq!(got.borrow().as_str(), "hello worker");
}

// Two `spawn_blocking` futures in one root future both complete. With a single
// worker they run one after another, but the executor stays live throughout and
// both results cross back.
#[test]
fn two_spawn_blockings_both_complete() {
    let completed = Rc::new(Cell::new(0usize));
    {
        let completed = completed.clone();
        Mar::run(async move {
            let result = spawn_blocking(move || 0u32).await;
            assert_eq!(result, 0);
            completed.set(completed.get() + 1);

            let result = spawn_blocking(move || 10u32).await;
            assert_eq!(result, 10);
            completed.set(completed.get() + 1);
        })
        .expect("run should not fail");
    }

    assert_eq!(completed.get(), 2);
}

// Step 14 — panic semantics: a panicking closure ships its payload back over
// the result channel and it is resumed inside the waiting task's poll, so
// `run()` panics with the original payload — never hangs.
#[test]
#[should_panic(expected = "blocking closure exploded")]
fn panicking_blocking_closure_makes_run_panic() {
    let _ = Mar::run(async {
        let _ = spawn_blocking(|| -> () {
            panic!("blocking closure exploded");
        })
        .await;
    });
}
