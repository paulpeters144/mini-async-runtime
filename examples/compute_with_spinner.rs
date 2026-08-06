use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mar::Mar;
use mar::task::spawn_blocking;
use mar::time::sleep;

// A long-running blocking job and a progress indicator that run in parallel.
// `spawn_blocking` sends the closure to a worker thread EAGERLY, so the job
// starts before the root future first yields. The root future then loops on
// `sleep` ticks, reading a shared progress counter the job updates, and finally
// awaits the job's return value.
//
// The wall time is ~the job duration, not the sum: the spinner sleeps while the
// worker grinds. This is the interleaving `tests/core.rs` proves.
fn main() {
    Mar::run(async move {
        let progress = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));

        let job_progress = Arc::clone(&progress);
        let job_done = Arc::clone(&done);
        // A stand-in for real work (compute, file copy, download). It
        // reports its percent complete through the shared atomics.
        let job = spawn_blocking(move || {
            let mut acc: u64 = 0;
            for step in 0..10u32 {
                // A few hundred thousand cheap iterations per chunk...
                for _ in 0..200_000 {
                    acc = acc.wrapping_mul(31).wrapping_add(1);
                }
                // ...then a small sleep so the demo takes a visible beat.
                std::thread::sleep(Duration::from_millis(50));
                job_progress.store(((step + 1) * 10) as usize, Ordering::SeqCst);
            }
            job_done.store(true, Ordering::SeqCst);
            acc
        });

        let mut last = 0usize;
        while !done.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(50)).await;
            let p = progress.load(Ordering::SeqCst);
            if p != last {
                println!("  {p}%");
                last = p;
            }
        }

        let result = job.await;
        println!("job finished with {result}");
    })
    .expect("run failed");
}
