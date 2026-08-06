use rrest::Rrest;

fn main() {
    let future = Rrest::get("http://httpbin.org/json");
    let response = pollster::block_on(future).unwrap();
    let body = String::from_utf8_lossy(response.body());

    println!("status: {}", response.status());
    println!("body:   {body}");
}
