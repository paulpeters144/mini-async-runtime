# rrest Crate Plan

## Overview

A runtime-agnostic HTTP/1.1 GET client crate (`rrest`) that depends only on a `Connection` trait. Any async runtime can use it by implementing `Connection` on its own I/O type. No dependency on any specific async runtime.

## Location

```
crates/rrest/
├── Cargo.toml
└── src/
    ├── lib.rs           # re-exports, Http struct
    ├── connection.rs    # Connection trait
    ├── client.rs        # Http::get() implementation
    ├── error.rs         # Error enum
    └── parse.rs         # httparse response parsing
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
async-trait = "0.1" # async fn in Connection trait

# Optional TLS
rustls = { version = "0.23", optional = true }
rustls-pki-types = { version = "1", optional = true }
webpki-roots = { version = "0.26", optional = true }

[features]
default = []
tls = ["rustls", "rustls-pki-types", "webpki-roots"]

[dev-dependencies]
```

## Core Trait: `Connection`

```rust
#[async_trait]
pub trait Connection {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
}
```

- No `Send` bound — allows mar-style `!Send` implementations while also allowing tokio `Send` implementations.
- `async-trait` macro to avoid boxed futures (zero-alloc trait calls).

## Public API

```rust
pub struct Http<C: Connection> {
    connection: C,
}

impl<C: Connection> Http<C> {
    pub fn new(connection: C) -> Self;
    pub async fn get(&mut self, url: &str) -> Result<http::Response<Vec<u8>>, Error>;
}

pub enum Error {
    InvalidUrl(String),           // URL parse failure
    Http(http::StatusCode),       // non-2xx response
    Protocol(String),             // malformed HTTP response
    Io(std::io::Error),            // I/O error
}
```

## GET Request Flow

```
Http::get("http://example.com/path")
  → parse URL → extract host + port + path
  → build HTTP/1.1 request:
        GET /path HTTP/1.1\r\n
        Host: host\r\n
        Connection: close\r\n
        \r\n
  → connection.write_all(request_bytes).await
  → read headers (accumulate bytes until "\r\n\r\n")
  → httparse::Response::parse(&bytes) → status, headers
  → read body (Content-Length bytes, or until EOF if no Content-Length)
  → if status not 2xx → return Err(Error::Http(status))
  → return Ok(http::Response<Vec<u8>>)
```

## Constraints

- HTTP/1.1 only
- `Connection: close` — no keep-alive, no pooling
- No streaming — entire response body buffered into `Vec<u8>`
- No chunked transfer encoding
- No redirects
- No cookies
- GET only — no POST, PUT, HEAD, etc.

## TLS (feature: `tls`)

Gated behind `#[cfg(feature = "tls")]`:

```rust
pub struct TlsStream<C: Connection> {
    inner: C,
    tls: rustls::ClientConnection,
    // buffers for rustls encode/decode loop
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
    consumed: usize,
}

impl<C: Connection> TlsStream<C> {
    pub async fn new(inner: C, server_name: rustls_pki_types::ServerName) -> Result<Self, Error>;
}

#[async_trait]
impl<C: Connection> Connection for TlsStream<C> { ... }
```

`TlsStream::new()` does the full TLS handshake before returning (async I/O for the handshake messages). After that, `read`/`write_all` delegate to `rustls::ClientConnection::read_tls`/`write_tls` with the inner connection.

`ServerName` comes from `rustls-pki-types` (no full rustls dep needed for the type). Certificate verification uses `webpki-roots`.

## Error Handling

| Scenario | Error |
|---|---|
| Malformed URL | `Error::InvalidUrl` |
| URL has no host | `Error::InvalidUrl` |
| URL has no http/https scheme | `Error::InvalidUrl` |
| I/O error during read/write | `Error::Io` |
| HTTP response not parseable | `Error::Protocol` |
| Status code not 2xx | `Error::Http(status)` |
| No Content-Length and connection closed mid-body | `Error::Protocol` |

Note: `Error::Http` carries the non-2xx status code so callers can inspect it. This differs from reqwest which returns the response body regardless of status.

## Testing Strategy

Tests do **not** depend on the `mar` crate. Tests provide a trivial `Connection` impl:

- **Unit tests:** Test URL parsing, request building, and `httparse` integration without real I/O.
- **Integration tests:** Use `std::net::TcpStream` in blocking mode. The `async_trait` macro generates a future, but since the inner ops complete immediately (blocking), the future resolves in one poll. Tests run via `#[test]` (non-async) using `futures::executor::block_on` or `pollster::block_on`.

```rust
#[cfg(test)]
struct BlockingTcp(std::net::TcpStream);

#[cfg(test)]
#[async_trait]
impl Connection for BlockingTcp {
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }
}

#[test]
fn test_get_example_com() {
    let tcp = std::net::TcpStream::connect("example.com:80").unwrap();
    let mut client = Http::new(BlockingTcp(tcp));
    let resp = pollster::block_on(client.get("http://example.com/")).unwrap();
    assert_eq!(resp.status(), 200);
}
```

`pollster` as dev-dependency for `block_on`.

## What This Crate Does NOT Provide

- No connection opening (DNS + TCP connect) — the caller provides an already-open connection
- No connection pooling
- No HTTPS URL enforcement — the caller is responsible for wrapping in `TlsStream` for HTTPS URLs
- No mar integration — mar implements `Connection` on its own types

## Integration with mar (out of scope for this crate)

A future step: mar provides a `MarTcpStream` wrapper around `mio::net::TcpStream` that implements `Connection` using the reactor (`register_source`, `dispatch`, waker storage). The caller does DNS + connect via `spawn_blocking`, wraps the result in `MarTcpStream`, and passes it to `Http::new()`.

```rust
// Not in rrest. In mar's examples/ or src/support/ eventually.
Mar::run(async {
    let tcp = spawn_blocking(|| std::net::TcpStream::connect("example.com:80").unwrap()).await;
    let stream = MarTcpStream::from_std(tcp);
    let mut client = Http::new(stream);
    let resp = client.get("http://example.com/").await.unwrap();
});
```
