use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mar::Mar;
use mar::blocking::spawn_blocking;
use mar::io;

// A minimal HTTP-shaped server: accept a connection on the worker pool, read
// one request with `io::read`, log its request line, and move on to the next
// connection. A client thread makes a couple of requests so the server has
// something to read.
//
// There is deliberately no response written: `io::read` consumes the socket,
// and writing a reply would need a second handle to the same connection, which
// the current API does not return. Read it as a request logger.
fn main() {
    Mar::run(async move {
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").unwrap());
        let port = listener.local_addr().unwrap().port();
        println!("http logger on 127.0.0.1:{port}");

        let requests = 2;
        let client = thread::spawn(move || {
            for i in 0..requests {
                let mut conn = TcpStream::connect(("127.0.0.1", port)).unwrap();
                let req = format!("GET /page/{i} HTTP/1.0\r\nHost: example\r\n\r\n");
                conn.write_all(req.as_bytes()).unwrap();
                // Give the server a beat to read it before we drop the connection.
                thread::sleep(Duration::from_millis(10));
            }
        });

        for _ in 0..requests {
            let l = Arc::clone(&listener);
            let (stream, _) = spawn_blocking(move || l.accept().unwrap()).await;
            let stream = {
                stream.set_nonblocking(true).unwrap();
                mio::net::TcpStream::from_std(stream)
            };

            let request = io::read(stream).await;
            let first_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or("(empty request)")
                .to_string();
            println!("  {first_line}");
        }

        client.join().unwrap();
    })
    .expect("run failed");
}
