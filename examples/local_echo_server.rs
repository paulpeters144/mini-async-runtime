use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use mar::Mar;
use mar::blocking::spawn_blocking;
use mar::io;

// A "server" whose handler is one root future: accept two connections, read a
// request from the first and relay it to the second. A client thread connects
// to both, sends a message on the first and reads the reply on the second.
//
// Why two connections? The `io::read`/`io::write` futures are one-shot: each
// CONSUMES the socket it wraps, so a single connection cannot be read and then
// written by the same root future. Routing the request over one connection and
// the reply over another keeps each socket to a single operation.
fn main() {
    Mar::run(async move {
        let listener_a = TcpListener::bind("127.0.0.1:0").unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").unwrap();
        let port_a = listener_a.local_addr().unwrap().port();
        let port_b = listener_b.local_addr().unwrap().port();
        println!("relay listening on {port_a} -> {port_b}");

        let echoed = Arc::new(Mutex::new(String::new()));
        let client_echo = Arc::clone(&echoed);
        let client = thread::spawn(move || {
            let mut conn_a = TcpStream::connect(("127.0.0.1", port_a)).unwrap();
            let mut conn_b = TcpStream::connect(("127.0.0.1", port_b)).unwrap();

            conn_a.write_all(b"hello relay").unwrap();
            let mut buf = [0u8; 64];
            let n = conn_b.read(&mut buf).unwrap();
            *client_echo.lock().unwrap() = String::from_utf8_lossy(&buf[..n]).into_owned();
        });

        let (stream_a, _) = spawn_blocking(move || listener_a.accept().unwrap()).await;
        let (stream_b, _) = spawn_blocking(move || listener_b.accept().unwrap()).await;

        let stream_a = non_blocking(stream_a);
        let stream_b = non_blocking(stream_b);

        let request = io::read(stream_a).await;
        println!("server received {} bytes", request.len());

        io::write(stream_b, request).await;

        client.join().unwrap();
        println!("client got back: {}", echoed.lock().unwrap());
    })
    .expect("run failed");
}

// `mio::net::TcpStream::from_std` does NOT change blocking mode, so flip it
// first — the `io::read`/`io::write` futures rely on `WouldBlock`.
fn non_blocking(stream: TcpStream) -> mio::net::TcpStream {
    stream.set_nonblocking(true).unwrap();
    mio::net::TcpStream::from_std(stream)
}
