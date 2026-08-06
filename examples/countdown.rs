use std::time::{Duration, Instant};

use mar::executor::Runtime;
use mar::timer_wheel::sleep;

// The simplest possible runtime program: a stopwatch that counts down. Each
// `sleep` parks the executor on the timer wheel; the loop wakes every second,
// prints the remaining time, and keeps going until zero.
//
// This example uses ONLY the timer wheel — no blocking work, no I/O.
fn main() {
    let seconds = 5;
    let start = Instant::now();

    let mut runtime = Runtime::new();
    runtime
        .run(async move {
            for remaining in (0..seconds).rev() {
                sleep(Duration::from_secs(1)).await;
                println!("  {remaining}...");
            }
        })
        .expect("run failed");

    println!("times up! (elapsed: {:?})", start.elapsed());
}
