# rrest Crate Plan

## Overview

A runtime-agnostic HTTP/1.1 GET client. The caller registers a connection factory once at startup. After that, any code in the process can call `Rrest::get("http://example.com").await` without knowing which async runtime is in use.

The crate handles URL parsing, request building, response parsing, DNS port extraction, and (with the `tls` feature) automatic TLS wrapping for `https://` URLs. The only thing the runtime provides is the ability to establish a raw TCP connection.

## Location

```
crates/rrest/
├── Cargo.toml
└── src/
    ├── lib.rs           # Rrest struct, static factory, re-exports
    ├── connection.rs    # Connection trait (read/write on an open socket)
    ├── factory.rs       # Factory trait (create connections from URLs)
    ├── client.rs        # HTTP/1.1 GET request/response logic
    ├── error.rs         # Error enum
    └── parse.rs         # httparse response parsing
    └── tls.rs           # TlsStream wrapper (feature: tls)
```

## Dependencies

```toml
[package]
name = "rrest"
version = "0.1.0"
edition = "2024"

[dependencies]
http = "1"          # Response, StatusCode, HeaderMap, Uri, Version
httparse = "1"      # parse HTTP/1.1 response headers
url = "2"           # URL parsing

# Optional TLS
rustls = { version = "0.23", optional = true }
rustls-pki-types = { version = "1", optional = true }
webpki-roots = { version = "0.26", optional = true }

[features]
default = []
tls = ["rustls", "rustls-pki-types", "webpki-roots"]

[dev-dependencies]
pollster = "0.4"    # block_on for tests
```

## Core Traits

### `Connection` — read/write on an already-open socket

```rust
pub trait Connection: Send + 'static {
    fn read<'a>(&'a mut self, buf: &'a mut [u8])
        -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>;
    fn write_all<'a>(&'a mut self, buf: &'a [u8])
        -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}
```

- `Send` bound required because the global factory returns `Box<dyn Connection>`.
- No `async_trait` — manual `Pin<Box<dyn Future>>` to avoid the dependency and keep the trait object-safe without surprises.

### `Factory` — create connections from URLs

```rust
pub trait Factory: Send + Sync + 'static {
    fn connect(&self, url: &str) -> Pin<Box<dyn Future<Output = Result<Box<dyn Connection>, Error>> + Send + '_>>;
}
```

- `&self` — the factory is stored in a static, shared across threads.
- Receives the full URL string. The factory is responsible for parsing the URL, resolving DNS, and establishing the TCP connection.
- Returns a raw TCP connection. rrest handles TLS wrapping automatically for `https://` URLs.

## Public API

### `Rrest` — the simple interface

```rust
/// Initialize the global connection factory. Call once at startup.
/// Panics if called more than once.
pub fn init(factory: impl Factory);

/// GET request using the registered factory.
/// Parses the URL, creates a connection (via factory), optionally wraps in TLS,
/// sends the request, and returns the full response body.
pub async fn get(url: &str) -> Result<http::Response<Vec<u8>>, Error>;
```

Usage:

```rust
// At startup (once)
Rrest::init(MyRuntimeConnect);

// Anywhere in the app
let resp = Rrest::get("http://example.com/path").await?;
println!("{}", String::from_utf8_lossy(resp.body()));
```

### `Http<C>` — the advanced interface

For callers who already have an open connection and want to skip the factory:

```rust
pub struct Http<C: Connection> { connection: C }

impl<C: Connection> Http<C> {
    pub fn new(connection: C) -> Self;
    pub async fn get(&mut self, url: &str) -> Result<http::Response<Vec<u8>>, Error>;
}
```

`Rrest::get()` internally uses `Http` after obtaining a connection from the factory.

## GET Request Flow

```
Rrest::get("http://example.com/path")
  → parse URL with url::Url
  → extract scheme, host, port, path
  → if factory not initialized → Error::NotInitialized
  → factory.connect(url).await → raw Connection
  → if scheme == "https" && feature "tls" enabled:
      → wrap in TlsStream::new(connection, server_name).await
  → Http::new(connection).get(url).await
    → build HTTP/1.1 request:
          GET /path HTTP/1.1\r\n
          Host: example.com\r\n
          Connection: close\r\n
          \r\n
    → connection.write_all(request_bytes)
    → read headers (accumulate until "\r\n\r\n")
    → httparse::Response::parse → status, headers
    → read body (Content-Length bytes, or until EOF)
    → if status not 2xx → Error::Http(status)
    → return Ok(Response)
```

## Error Enum

```rust
pub enum Error {
    NotInitialized,               // Rrest::init() not called
    InvalidUrl(String),           // URL parse failure
    Connect(String),              // factory connect failed
    Tls(String),                  // TLS handshake failed
    Http(http::StatusCode),       // non-2xx response
    Protocol(String),             // malformed HTTP response
    Io(std::io::Error),           // I/O error
}
```

## Constraints

- HTTP/1.1 only
- `Connection: close` — no keep-alive, no pooling
- No streaming — entire response body buffered into `Vec<u8>`
- No chunked transfer encoding
- No redirects
- No cookies
- GET only
- One factory per process (single `OnceLock`)

## TLS (feature: `tls`)

rrest automatically wraps the connection in `TlsStream` when the URL scheme is `https://` and the `tls` feature is enabled.

```rust
pub struct TlsStream<C: Connection> {
    inner: C,
    tls: rustls::ClientConnection,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
    consumed: usize,
}

impl<C: Connection> TlsStream<C> {
    pub async fn new(inner: C, server_name: ServerName) -> Result<Self, Error>;
}

impl<C: Connection> Connection for TlsStream<C> { ... }
```

- `ServerName` extracted from the URL host by rrest before wrapping.
- Certificate verification uses `webpki-roots`.
- `TlsStream::new()` performs the full TLS handshake (async I/O for handshake messages).

## Testing Strategy

Tests do **not** depend on the `mar` crate.

- **Unit tests:** URL parsing, request building, `httparse` parsing — no real I/O.
- **Integration tests:** A test-only `Factory` that uses `std::net::TcpStream` (blocking) wrapped in a `Send` adapter. Tests run via `pollster::block_on`.

```rust
#[cfg(test)]
mod tests {
    struct BlockingTcp(std::net::TcpStream);

    // Safety: TcpStream is Send on Linux. The blocking ops complete
    // immediately in tests, so no real cross-thread sharing occurs.
    unsafe impl Send for BlockingTcp {}

    impl Connection for BlockingTcp {
        fn read<'a>(&'a mut self, buf: &'a mut [u8])
            -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>>
        {
            Box::pin(async move { self.0.read(buf) })
        }
        fn write_all<'a>(&'a mut self, buf: &'a [u8])
            -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>
        {
            Box::pin(async move { self.0.write_all(buf) })
        }
    }

    struct TestFactory;

    impl Factory for TestFactory {
        fn connect(&self, url: &str) -> Pin<Box<dyn Future<Output = Result<Box<dyn Connection>, Error>> + Send + '_>> {
            Box::pin(async move {
                let parsed = url::Url::parse(url).map_err(|e| Error::InvalidUrl(e.to_string()))?;
                let host = parsed.host_str().ok_or_else(|| Error::InvalidUrl("no host".into()))?;
                let port = parsed.port_or_known_default().unwrap_or(80);
                let tcp = std::net::TcpStream::connect(format!("{host}:{port}"))
                    .map_err(|e| Error::Io(e))?;
                Ok(Box::new(BlockingTcp(tcp)) as Box<dyn Connection>)
            })
        }
    }

    #[test]
    fn test_get_example_com() {
        // Must init before first use; tests that call Rrest::get should
        // use std::sync::Once or run in a single test binary.
        Rrest::init(TestFactory);
        let resp = pollster::block_on(Rrest::get("http://example.com/")).unwrap();
        assert_eq!(resp.status(), 200);
    }
}
```

## What This Crate Does NOT Provide

- No connection pooling
- No redirects
- No cookies
- No POST/PUT/DELETE/HEAD
- No HTTP/2
- No streaming response body
- No mar integration — mar implements `Connection` + `Factory` on its own types

## Integration with mar (out of scope for this crate)

A future step: mar provides a `Factory` impl that uses `spawn_blocking` for DNS + TCP connect, then wraps the resulting `mio::net::TcpStream` in a `MarTcpStream` that implements `Connection` by driving the reactor.

```rust
// Not in rrest. In mar's codebase eventually.
struct MarFactory;

impl rrest::Factory for MarFactory {
    fn connect(&self, url: &str) -> Pin<Box<dyn Future<Output = Result<Box<dyn rrest::Connection>, rrest::Error>> + Send + '_>> {
        Box::pin(async move {
            let tcp = mar::spawn_blocking(move || {
                std::net::TcpStream::connect("example.com:80")
            }).await?;
            let stream = MarTcpStream::from_std(tcp);
            Ok(Box::new(stream) as Box<dyn rrest::Connection>)
        })
    }
}

// At startup
rrest::Rrest::init(MarFactory);

// Anywhere in mar runtime
let body = rrest::Rrest::get("http://example.com").await.unwrap();
```
