use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use crate::connection::{Connection, ReadFuture, WriteAllFuture};
use crate::error::Error;

pub struct TcpConnection {
    stream: TcpStream,
}

impl Connection for TcpConnection {
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFuture<'a> {
        Box::pin(async move {
            loop {
                match self.stream.read(buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }

    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAllFuture<'a> {
        Box::pin(async move {
            let mut written = 0;
            loop {
                match self.stream.write(&buf[written..]) {
                    Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero")),
                    Ok(n) => {
                        written += n;
                        if written == buf.len() {
                            return Ok(());
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }
}

pub async fn connect(url: &str) -> Result<Box<dyn Connection>, Error> {
    let parsed = url::Url::parse(url).map_err(|e| Error::InvalidUrl(e.to_string()))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::InvalidUrl("missing host".into()))?;
    let port = parsed.port().unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
    let addr = format!("{host}:{port}");

    let stream = TcpStream::connect(&addr).map_err(Error::Io)?;
    stream.set_nonblocking(true).map_err(Error::Io)?;

    Ok(Box::new(TcpConnection { stream }))
}
