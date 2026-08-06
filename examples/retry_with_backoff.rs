use std::time::Duration;

use mar::blocking::spawn_blocking;
use mar::executor::Runtime;
use mar::timer_wheel::sleep;

// A job that may fail (here: the first two attempts fail, then it succeeds),
// retried with exponential backoff. Each attempt runs on the worker pool; the
// backoff wait parks the executor on the timer wheel between attempts.
fn main() {
    let mut runtime = Runtime::new();
    runtime
        .run(async move {
            let mut attempt = 0;
            let mut backoff = Duration::from_millis(50);

            let result = loop {
                attempt += 1;
                match spawn_blocking(move || flaky(attempt)).await {
                    Ok(value) => break value,
                    Err(reason) => {
                        println!("  attempt {attempt} failed: {reason}");
                        sleep(backoff).await;
                        backoff *= 2;
                    }
                }
            };

            println!("succeeded on attempt {attempt}: {result}");
        })
        .expect("run failed");
}

// A stand-in for flaky work (a network call, a lock, a service): fails until it
// has been called a few times.
fn flaky(attempt: u32) -> Result<u32, &'static str> {
    if attempt < 3 {
        Err("not yet")
    } else {
        Ok(attempt * 100)
    }
}
