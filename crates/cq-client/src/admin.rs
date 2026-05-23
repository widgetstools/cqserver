//! Thin client for the server's admin HTTP API. Uses `hyper` directly
//! to avoid pulling in a heavier HTTP-client dependency for callers
//! that only need the four `GET` endpoints.

use crate::error::{ClientError, ClientResult};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Talks to `http://host:port/...` admin endpoints. Single-request
/// model — each call opens, reads, and closes a TCP connection. The
/// admin API has tiny payloads and a low call rate, so this is fine.
pub struct AdminClient {
    host: String,
    port: u16,
}

impl AdminClient {
    /// `addr` is `host:port`, e.g. `"127.0.0.1:8085"`.
    pub fn new(addr: &str) -> ClientResult<Self> {
        let (host, port_str) = addr
            .rsplit_once(':')
            .ok_or_else(|| ClientError::InvalidUrl(addr.into()))?;
        let port: u16 = port_str
            .parse()
            .map_err(|_| ClientError::InvalidUrl(addr.into()))?;
        Ok(AdminClient {
            host: host.to_string(),
            port,
        })
    }

    pub async fn healthz(&self) -> ClientResult<String> {
        let resp = self.get("/healthz").await?;
        Ok(String::from_utf8_lossy(&resp).into_owned())
    }

    pub async fn stats(&self) -> ClientResult<Value> {
        let body = self.get("/stats").await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn topics(&self) -> ClientResult<Value> {
        let body = self.get("/topics").await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn metrics(&self) -> ClientResult<String> {
        let body = self.get("/metrics").await?;
        Ok(String::from_utf8_lossy(&body).into_owned())
    }

    async fn get(&self, path: &str) -> ClientResult<Vec<u8>> {
        let addr = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect(&addr).await?;
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            path, self.host
        );
        stream.write_all(req.as_bytes()).await?;
        let mut raw = Vec::with_capacity(4096);
        stream.read_to_end(&mut raw).await?;

        // Split status line + headers from body at the first blank line.
        let split = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| {
                ClientError::UnexpectedResponse("malformed HTTP response".into())
            })?;
        let header = &raw[..split];
        let body = raw[split + 4..].to_vec();

        // Status check.
        let first_line_end = header.iter().position(|b| *b == b'\r').unwrap_or(header.len());
        let status_line = std::str::from_utf8(&header[..first_line_end]).unwrap_or("");
        if !status_line.contains(" 200 ") {
            return Err(ClientError::Server(status_line.to_string()));
        }
        Ok(body)
    }
}
