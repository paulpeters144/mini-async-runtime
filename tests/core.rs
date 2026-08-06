use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

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
