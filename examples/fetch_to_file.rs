use mar::Mar;

// The whole program is a single future passed to `Mar::run()`: fetch the
// body asynchronously via rrest, write it to examples/get-data.json, read it
// back, and print it.
//
// The output file always lands in this repo's examples/ dir regardless of the
// current working directory (CARGO_MANIFEST_DIR resolves at compile time).
fn main() {
    Mar::run(async move {
        let url = "https://jsonplaceholder.typicode.com/todos/1";
        let response = rrest::Rrest::get(url).await.expect("GET failed");
        let body = String::from_utf8(response.body().clone()).expect("invalid UTF-8");

        let output = std::env::current_exe()
            .expect("failed to get exe path")
            .parent()
            .expect("exe has no parent dir")
            .join("get-data.json");

        std::fs::write(&output, &body).expect("failed to write get-data.json");

        let contents = std::fs::read_to_string(&output).expect("failed to read get-data.json");

        println!("contents:\n{contents}");
    })
    .expect("run failed");
}
