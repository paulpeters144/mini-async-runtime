use std::time::Duration;

use mar::Mar;
use mar::task::spawn_blocking;
use mar::time::sleep;

fn main() {
    Mar::run(async move {
        let job_a = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(200));
            "alpha"
        });
        let job_b = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(300));
            "beta"
        });
        let job_c = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(250));
            "gamma"
        });

        println!("dispatched 3 jobs");

        for i in 1..=3 {
            sleep(Duration::from_millis(100)).await;
            println!("  tick {i}");
        }

        println!("{} {} {}", job_a.await, job_b.await, job_c.await);
    })
    .expect("run failed");
}
