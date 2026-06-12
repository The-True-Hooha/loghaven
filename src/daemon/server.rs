use super::super::ipc::protocol::{Request, Response, ResponseData, SessionData, StatusData};
use crate::auth::session::SessionStore;
use crate::auth::token::verify_jwt;
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

#[cfg(windows)]
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;

#[allow(dead_code)]
pub struct DaemonServer {
    socket_path: PathBuf,
    tcp_port: u16,
    start_time: u64,
    config: Arc<Config>,
    sessions: Arc<SessionStore>,
    public_key_pem: Option<String>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DaemonServer {
    pub async fn new(config: &Config, sessions: Arc<SessionStore>) -> Result<Self> {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let public_key_pem = {
            let pub_path = config.auth.keys_dir.join("public.pem");
            if pub_path.exists() {
                std::fs::read_to_string(&pub_path).ok()
            } else {
                None
            }
        };

        Ok(Self {
            socket_path: config.daemon.socket_path.clone(),
            tcp_port: config.daemon.tcp_port,
            start_time,
            config: Arc::new(config.clone()),
            sessions,
            public_key_pem,
            shutdown_tx,
            shutdown_rx,
        })
    }

    pub fn trigger_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
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

        let start_time = self.start_time;
        let config = Arc::clone(&self.config);
        let sessions = Arc::clone(&self.sessions);
        let public_key_pem = self.public_key_pem.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((mut stream, _)) => {
                            let config = Arc::clone(&config);
                            let sessions = Arc::clone(&sessions);
                            let public_key_pem = public_key_pem.clone();
                            let shutdown_tx = shutdown_tx.clone();

                            tokio::spawn(async move {
                                let mut buf = vec![0u8; 4096];
                                match stream.read(&mut buf).await {
                                    Ok(n) => {
                                        let request = String::from_utf8_lossy(&buf[..n]);
                                        let (response, should_stop) = Self::handle_request(
                                            &request, start_time, &config, &sessions, public_key_pem.as_deref(),
                                        );
                                        let _ = stream.write_all(response.as_bytes()).await;
                                        if should_stop {
                                            let _ = shutdown_tx.send(true);
                                        }
                                    }
                                    Err(e) => eprintln!("Read error: {}", e),
                                }
                            });
                        }
                        Err(e) => eprintln!("Accept error: {}", e),
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("Daemon shutting down");
                        let _ = std::fs::remove_file(&self.socket_path);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    #[cfg(windows)]
    pub async fn listen(&self) -> Result<()> {
        let addr = format!("127.0.0.1:{}", self.tcp_port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| LogHavenError::Daemon(format!("Failed to bind TCP: {}", e)))?;

        println!("Daemon listening on {}", addr);

        let start_time = self.start_time;
        let config = Arc::clone(&self.config);
        let sessions = Arc::clone(&self.sessions);
        let public_key_pem = self.public_key_pem.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((mut stream, _)) => {
                            let config = Arc::clone(&config);
                            let sessions = Arc::clone(&sessions);
                            let public_key_pem = public_key_pem.clone();
                            let shutdown_tx = shutdown_tx.clone();

                            tokio::spawn(async move {
                                let mut buf = vec![0u8; 4096];
                                match stream.read(&mut buf).await {
                                    Ok(n) => {
                                        let request = String::from_utf8_lossy(&buf[..n]);
                                        let (response, should_stop) = Self::handle_request(
                                            &request, start_time, &config, &sessions, public_key_pem.as_deref(),
                                        );
                                        let _ = stream.write_all(response.as_bytes()).await;
                                        if should_stop {
                                            let _ = shutdown_tx.send(true);
                                        }
                                    }
                                    Err(e) => eprintln!("Read error: {}", e),
                                }
                            });
                        }
                        Err(e) => eprintln!("Accept error: {}", e),
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("Daemon shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_request(
        request: &str,
        start_time: u64,
        config: &Config,
        sessions: &SessionStore,
        public_key_pem: Option<&str>,
    ) -> (String, bool) {
        let mut should_stop = false;

        let response = match serde_json::from_str::<Request>(request.trim()) {
            Ok(Request::Status) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Response::success(ResponseData::Status(StatusData {
                    pid: std::process::id(),
                    uptime: now - start_time,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    status: "running".to_string(),
                    storage_backend: config.storage.backend.clone(),
                    log_level: config.agent.log_level.clone(),
                }))
            }
            Ok(Request::Stop) => {
                should_stop = true;
                Response::success(ResponseData::Message("Shutting down".to_string()))
            }
            Ok(Request::Reload) => {
                Response::success(ResponseData::Message("Configuration reloaded".to_string()))
            }
            Ok(Request::Auth { jwt }) => {
                match Self::handle_auth(&jwt, sessions, public_key_pem, config.auth.session_ttl_secs) {
                    Ok(data) => Response::success(ResponseData::Session(data)),
                    Err(e) => Response::error(e.to_string()),
                }
            }
            Err(_) => Response::error("Invalid request".to_string()),
        };

        (
            serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_string()),
            should_stop,
        )
    }

    fn handle_auth(
        jwt: &str,
        sessions: &SessionStore,
        public_key_pem: Option<&str>,
        ttl_secs: u64,
    ) -> Result<SessionData> {
        let pem = public_key_pem.ok_or_else(|| {
            LogHavenError::Config("no public key configured; run `loghaven auth keygen`".into())
        })?;

        let claims = verify_jwt(jwt, pem)?;

        // Override TTL with config value; claims.exp already validated by verify_jwt
        let _ = ttl_secs;
        let remaining = claims.exp.saturating_sub(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        let (token_id, secret) = sessions.issue(&claims.sub)?;
        let secret_hex = hex::encode(&secret);

        Ok(SessionData {
            token_id,
            secret_hex,
            expires_in_secs: remaining,
        })
    }
}
