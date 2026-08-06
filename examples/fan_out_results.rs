use std::time::{Duration, Instant};

use mar::Mar;
use mar::task;
use mar::time::sleep;

async fn delay(msg: &str, dur: Duration) -> &str {
    sleep(dur).await;
    msg
}

fn main() {
    Mar::run(async move {
        let start = Instant::now();

        let jobs = [
            delay("job 1", Duration::from_millis(200)),
            delay("job 2", Duration::from_millis(300)),
            delay("job 3", Duration::from_millis(100)),
        ];

        println!("started {} jobs", jobs.len());

        let [r1, r2, r3] = task::all(jobs).await;

        println!("complete: {r1}, {r2}, {r3}");
        println!("total: {:?}", start.elapsed());
    })
    .expect("run failed");
}
