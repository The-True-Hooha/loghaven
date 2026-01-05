use super::protocol::{Request, Response};
use crate::error::{LogHavenError, Result};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
use tokio::net::UnixStream;

#[cfg(windows)]
use tokio::net::TcpStream;

#[allow(dead_code)]
pub struct IpcClient {
    socket_path: PathBuf,
    tcp_port: u16,
}

impl IpcClient {
    pub fn new(socket_path: PathBuf, tcp_port: u16) -> Self {
        Self {
            socket_path,
            tcp_port,
        }
    }

    pub async fn send(&self, request: Request) -> Result<Response> {
        let request_json = serde_json::to_string(&request)
            .map_err(|e| LogHavenError::Daemon(format!("Failed to serialize request: {}", e)))?;

        #[cfg(unix)]
        {
            let mut stream = UnixStream::connect(&self.socket_path).await.map_err(|e| {
                LogHavenError::Daemon(format!("Failed to connect to daemon: {}", e))
            })?;

            stream.write_all(request_json.as_bytes()).await?;

            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await?;

            let response: Response = serde_json::from_slice(&buf[..n])
                .map_err(|e| LogHavenError::Daemon(format!("Failed to parse response: {}", e)))?;

            Ok(response)
        }

        #[cfg(windows)]
        {
            let addr = format!("127.0.0.1:{}", self.tcp_port);
            let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
                LogHavenError::Daemon(format!("Failed to connect to daemon: {}", e))
            })?;
            stream.write_all(request_json.as_bytes()).await?;

            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await?;

            let response: Response = serde_json::from_slice(&buf[..n])
                .map_err(|e| LogHavenError::Daemon(format!("Failed to parse response: {}", e)))?;
            Ok(response)
        }
    }
}
