use super::super::ipc::protocol::{Request, Response, ResponseData, StatusData};
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(windows)]
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

#[allow(dead_code)]
pub struct DaemonServer {
    socket_path: PathBuf,
    tcp_port: u16,
}

impl DaemonServer {
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            socket_path: config.daemon.socket_path.clone(),
            tcp_port: config.daemon.tcp_port,
        })
    }

    #[cfg(unix)]
    pub async fn listen(&self) -> Result<()> {
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(&self.socket_path)
            .map_err(|e| LogHavenError::Daemon(format!("Failed to bind socket: {}", e)))?;

        println!("Daemon listening on {:?}", self.socket_path);

        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 1024];

                        match stream.read(&mut buf).await {
                            Ok(n) => {
                                let request = String::from_utf8_lossy(&buf[..n]);
                                let response = Self::handle_request(&request);
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            Err(e) => eprintln!("Read error: {}", e),
                        }
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }

    #[cfg(windows)]
    pub async fn listen(&self) -> Result<()> {
        let addr = format!("127.0.0.1:{}", self.tcp_port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| LogHavenError::Daemon(format!("Failed to bind TCP: {}", e)))?;

        println!("Daemon listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 1024];

                        match stream.read(&mut buf).await {
                            Ok(n) => {
                                let request = String::from_utf8_lossy(&buf[..n]);
                                let response = Self::handle_request(&request);
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                            Err(e) => eprintln!("Read error: {}", e),
                        }
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }

    fn handle_request(request: &str) -> String {
        let response = match serde_json::from_str::<Request>(request.trim()) {
            Ok(Request::Status) => Response::success(ResponseData::Status(StatusData {
                pid: std::process::id(),
                uptime: 0,
                version: env!("CARGO_PKG_VERSION").to_string(),
                status: "running".to_string(),
                storage_backend: "local".to_string(),
                log_level: "info".to_string(),
            })),
            Ok(Request::Stop) => {
                Response::success(ResponseData::Message("Shutting down".to_string()))
            }
            Ok(Request::Reload) => {
                Response::success(ResponseData::Message("Configuration reloaded".to_string()))
            }
            Err(_) => Response::error("Invalid request".to_string()),
        };

        serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_string())
    }
}
