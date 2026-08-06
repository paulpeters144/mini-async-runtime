use std::fmt;

pub enum Error {
    InvalidUrl(String),
    Http(http::StatusCode),
    Protocol(String),
    Io(std::io::Error),
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(msg) => write!(f, "invalid URL: {msg}"),
            Self::Http(status) => write!(f, "HTTP {status}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
