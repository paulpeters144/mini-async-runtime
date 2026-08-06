mod client;
pub mod connection;
mod error;
pub mod net;
mod parse;
mod tls;

use std::future::Future;
use std::pin::Pin;

use http::Response;

use client::Http;
pub use connection::Connection;
pub use connection::{ReadFuture, WriteAllFuture};
pub use error::Error;

pub type GetFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Response<Vec<u8>>, Error>> + Send + 'a>>;

pub struct Rrest;

impl Rrest {
    pub fn get(url: &str) -> GetFuture<'_> {
        Box::pin(async move {
            let conn = net::connect(url).await?;

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
