#[cfg(feature = "tls")]
mod tls_impl {
    use std::future::Future;
    use std::io;
    use std::io::{Read, Write};
    use std::pin::Pin;
    use std::sync::Arc;

    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, RootCertStore};

    use crate::connection::Connection;
    use crate::error::Error;

    pub struct TlsStream<C: Connection> {
        inner: C,
        tls: rustls::ClientConnection,
        send_buf: Vec<u8>,
    }

    impl<C: Connection> TlsStream<C> {
        pub async fn new(inner: C, host: &str) -> Result<Self, Error> {
            let root_store: RootCertStore = webpki_roots::TLS_SERVER_ROOTS
                .iter()
                .cloned()
                .collect();

            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let server_name = ServerName::try_from(host.to_string())
                .map_err(|e| Error::Io(io::Error::other(format!("invalid server name: {e}"))))?;

            let tls = rustls::ClientConnection::new(Arc::new(config), server_name)
                .map_err(|e| Error::Io(io::Error::other(format!("failed to create TLS connection: {e}"))))?;

            let mut stream = Self {
                inner,
                tls,
                send_buf: Vec::new(),
            };

            stream.handshake().await?;
            Ok(stream)
        }

        async fn handshake(&mut self) -> Result<(), Error> {
            let mut recv_buf = vec![0u8; 16384];

            loop {
                if self.tls.wants_write() {
                    self.send_buf.clear();
                    self.tls
                        .write_tls(&mut self.send_buf)
                        .map_err(|e| Error::Io(io::Error::other(format!("TLS error: {e}"))))?;
                    if !self.send_buf.is_empty() {
                        self.inner.write_all(&self.send_buf).await?;
                    }
                }

                if self.tls.is_handshaking() && self.tls.wants_read() {
                    let n = self.inner.read(&mut recv_buf).await?;
                    if n == 0 {
                        return Err(Error::Io(io::Error::other("connection closed during handshake")));
                    }
                    let mut cursor = io::Cursor::new(&recv_buf[..n]);
                    self.tls
                        .read_tls(&mut cursor)
                        .map_err(|e| Error::Io(io::Error::other(format!("TLS error: {e}"))))?;
                }

                match self.tls.process_new_packets() {
                    Ok(_) => {}
                    Err(e) => {
                        // Ignore WouldBlock-like errors during handshake; keep going
                        _ = e;
                    }
                }

                if !self.tls.is_handshaking() {
                    break;
                }
            }

            Ok(())
        }

        async fn flush_writes(&mut self) -> Result<(), Error> {
            loop {
                self.send_buf.clear();
                match self.tls.write_tls(&mut self.send_buf) {
                    Ok(wrote) => {
                        if wrote == 0 {
                            break;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(Error::Io(io::Error::other(format!("TLS error: {e}")))),
                }

                if !self.send_buf.is_empty() {
                    self.inner.write_all(&self.send_buf).await?;
                }
            }

            Ok(())
        }
    }

    impl<C: Connection> Connection for TlsStream<C> {
        fn read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async move {
                let mut recv_buf = vec![0u8; 16384];

                loop {
                    match self.tls.reader().read(buf) {
                        Ok(0) => {}
                        Ok(n) => return Ok(n),
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) => return Err(e),
                    }

                    let n = self.inner.read(&mut recv_buf).await?;
                    if n == 0 {
                        return Ok(0);
                    }

                    let mut cursor = io::Cursor::new(&recv_buf[..n]);
                    match self.tls.read_tls(&mut cursor) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(e),
                    }

                    self.tls
                        .process_new_packets()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                }
            })
        }

        fn write_all<'a>(
            &'a mut self,
            buf: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.tls
                    .writer()
                    .write_all(buf)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                self.flush_writes()
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))?;
                Ok(())
            })
        }
    }

    pub async fn wrap_tls<C: Connection>(inner: C, host: &str) -> Result<Box<dyn Connection>, Error> {
        let stream = TlsStream::new(inner, host).await?;
        Ok(Box::new(stream))
    }
}

#[cfg(feature = "tls")]
pub use tls_impl::wrap_tls;
