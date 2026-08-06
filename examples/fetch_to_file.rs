use mar::Mar;
use mar::blocking::spawn_blocking;

// The whole program is a single future passed to `Mar::run()`: fetch the
// body, write it to examples/get-data.json, read it back, and print it. The
// blocking HTTP and file I/O run on the worker pool via `spawn_blocking`; the
// root future just awaits each result in sequence.
//
// The output file always lands in this repo's examples/ dir regardless of the
// current working directory (CARGO_MANIFEST_DIR resolves at compile time).
fn main() {
    Mar::run(async move {
        let body = spawn_blocking(|| {
            reqwest::blocking::get("https://jsonplaceholder.typicode.com/todos/1")
                .expect("GET failed")
                .text()
                .expect("failed to read body")
        })
        .await;
        let len = body.len();

        let output = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("get-data.json");

        // Each spawn_blocking closure owns what it captures, so the write and
        // read closures each get their own copy of the path.
        let write_path = output.clone();
        spawn_blocking(move || {
            std::fs::write(&write_path, body).expect("failed to write get-data.json");
        })
        .await;

        let contents = spawn_blocking(move || {
            std::fs::read_to_string(&output).expect("failed to read get-data.json")
        })
        .await;

        println!("wrote {len} bytes to get-data.json");
        println!("contents:\n{contents}");
    })
    .expect("run failed");
}

