use http::HeaderMap;
use http::StatusCode;
use http::Version;

use crate::error::Error;

pub struct ParsedResponse {
    pub version: Version,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub head_len: usize,
}

pub fn parse_response(buf: &[u8]) -> Result<ParsedResponse, Error> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut resp = httparse::Response::new(&mut headers);

    let head_len = match resp.parse(buf) {
        Ok(httparse::Status::Complete(len)) => len,
        Ok(httparse::Status::Partial) => {
            return Err(Error::Protocol("incomplete response headers".into()))
        }
        Err(e) => return Err(Error::Protocol(format!("parse error: {e}"))),
    };

    let status = StatusCode::from_u16(resp.code.ok_or_else(|| {
        Error::Protocol("missing status code".into())
    })?)
    .map_err(|_| Error::Protocol("invalid status code".into()))?;

    let version = match resp.version {
        Some(0) => Version::HTTP_10,
        _ => Version::HTTP_11,
    };

    let mut header_map = HeaderMap::new();
    for header in resp.headers.iter() {
        let name = http::HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|_| Error::Protocol(format!("invalid header name: {}", header.name)))?;
        let value = http::HeaderValue::from_bytes(header.value)
            .map_err(|_| Error::Protocol("invalid header value".into()))?;
        header_map.append(name, value);
    }

    Ok(ParsedResponse {
        version,
        status,
        headers: header_map,
        head_len,
    })
}
