use std::time::{Duration, Instant};

use mar::Mar;
use mar::task::spawn_blocking;
use mar::time::sleep;

fn main() {
    Mar::run(async move {
        let start = Instant::now();

        let job_1 = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(200));
            "job 1"
        });
        let job_2 = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(300));
            "job 2"
        });
        let job_3 = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(250));
            "job 3"
        });

        println!("dispatched 3 jobs");

        for i in 1..=3 {
            sleep(Duration::from_millis(100)).await;
            println!("  tick {i}");
        }

        let r1 = job_1.await;
        println!("{r1} done — {:?}", start.elapsed());

        let r2 = job_2.await;
        println!("{r2} done — {:?}", start.elapsed());

        let r3 = job_3.await;
        println!("{r3} done — {:?}", start.elapsed());

        println!("total: {:?}", start.elapsed());
    })
    .expect("run failed");
}
