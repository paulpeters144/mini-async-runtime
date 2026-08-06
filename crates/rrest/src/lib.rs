pub mod connection;
mod error;
pub mod net;
mod parse;
mod tls;

use std::future::Future;
use std::pin::Pin;

use http::Response;

use connection::Connection;
pub use connection::{ReadFuture, WriteAllFuture};
pub use error::Error;

pub type GetFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Response<Vec<u8>>, Error>> + Send + 'a>>;

pub struct Rrest;

impl Rrest {
    #[must_use]
    pub fn get(url: &str) -> GetFuture<'_> {
        Box::pin(async move {
            let conn = net::connect(url)?;

            #[cfg(feature = "tls")]
            let conn = {
                let parsed = url::Url::parse(url)
                    .map_err(|e| Error::InvalidUrl(e.to_string()))?;
                if parsed.scheme() == "https" {
                    let host = parsed
                        .host_str()
                        .ok_or_else(|| Error::InvalidUrl("no host in URL".into()))?;
                    tls::wrap_tls(conn, host).await?
                } else {
                    conn
                }
            };

            let mut http = Http::new(conn);
            http.get(url).await
        })
    }
}

struct Http<C: Connection> {
    connection: C,
}

impl<C: Connection> Http<C> {
    const fn new(connection: C) -> Self {
        Self { connection }
    }

    async fn get(&mut self, url: &str) -> Result<Response<Vec<u8>>, Error> {
        let parsed = url::Url::parse(url).map_err(|e| Error::InvalidUrl(e.to_string()))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| Error::InvalidUrl("no host in URL".into()))?;

        let path = parsed.path();
        let query = parsed.query().map(|q| format!("?{q}")).unwrap_or_default();
        let request_path = format!("{path}{query}");
        let request_path = if request_path.is_empty() { "/" } else { &request_path };

        let request = format!(
            "GET {request_path} HTTP/1.1\r\n\
             Host: {host}\r\n\
             Connection: close\r\n\
             \r\n"
        );

        self.connection.write_all(request.as_bytes()).await?;

        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];

        loop {
            let parsed = parse::parse_response(&buf);
            if let Ok(pr) = parsed {
                let body_start = pr.head_len;
                let mut body = buf.split_off(body_start);

                let content_length = pr
                    .headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<usize>().ok());

                match content_length {
                    Some(len) => {
                        while body.len() < len {
                            let n = self.connection.read(&mut tmp).await?;
                            if n == 0 {
                                break;
                            }
                            body.extend_from_slice(&tmp[..n]);
                        }
                    }
                    None => {
                        loop {
                            let n = self.connection.read(&mut tmp).await?;
                            if n == 0 {
                                break;
                            }
                            body.extend_from_slice(&tmp[..n]);
                        }
                    }
                }

                if !pr.status.is_success() {
                    return Err(Error::Http(pr.status));
                }

                let mut response = Response::builder()
                    .status(pr.status)
                    .version(pr.version);
                *response.headers_mut().unwrap() = pr.headers;
                return Ok(response.body(body).unwrap());
            }

            let n = self.connection.read(&mut tmp).await?;
            if n == 0 {
                return Err(Error::Protocol("connection closed before headers".into()));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    }
}
