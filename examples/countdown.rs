use std::time::{Duration, Instant};

use mar::Mar;
use mar::time::sleep;

// The simplest possible runtime program: a stopwatch that counts down. Each
// `sleep` blocks the executor on the timer registry; the loop wakes every
// second, prints the remaining time, and keeps going until zero.
//
// This example uses ONLY the timer registry — no blocking work, no I/O.
fn main() {
    Mar::run(async move {
        let seconds = 5;
        let start = Instant::now();
        for remaining in (0..seconds).rev() {
            sleep(Duration::from_secs(3)).await;
            println!("  {remaining}...");
        }
        println!("times up! (elapsed: {:?})", start.elapsed());
    })
    .expect("run failed");
}
