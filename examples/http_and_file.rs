use std::time::Duration;

use mini_async_runtime::blocking::spawn_blocking;
use mini_async_runtime::executor::Runtime;
use mini_async_runtime::timer_wheel::sleep;

fn main() {
    let mut runtime = Runtime::new();

    runtime.spawn(async move {
        let result = reqwest::get("https://httpbin.org/get").await.expect("GET failed");
        let body = result.text().await.expect("failed to read body");
        println!("HTTP response: {:.80}...", &body);
    });

    runtime.spawn(async move {
        let contents = spawn_blocking(|| {
            std::fs::read_to_string("Cargo.toml").expect("failed to read Cargo.toml")
        })
        .await;
        println!("file contents:\n{contents}");
    });

    runtime.spawn(async move {
        sleep(Duration::from_millis(200)).await;
        println!("timer fired");
    });

    println!("starting runtime");
    runtime.run().expect("run failed");
    println!("runtime finished");
}
