use std::time::{Duration, Instant};

use mar::Mar;
use mar::task;
use mar::task::spawn_blocking;

fn main() {
    Mar::run(async move {
        let start = Instant::now();

        let jobs = [
            spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(200));
                "job 1 done"
            }),
            spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(300));
                "job 2 done"
            }),
            spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(100));
                "job 3 done"
            }),
        ];

        println!("started 3 blocking tasks");

        let [r1, r2, r3] = task::all(jobs).await;

        println!("complete: {r1}, {r2}, {r3}");
        println!("total: {:?}", start.elapsed());
    })
    .expect("run failed");
}
