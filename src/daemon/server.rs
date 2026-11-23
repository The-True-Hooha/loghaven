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
            ensure_private_if_chosen_for_length(parent)?;
        }

        check_socket_path_len(&self.socket_path)?;
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
            Ok(Request::Reload) => Response::error(
                "reload is not supported; run 'loghaven stop' then 'loghaven run'".to_string(),
            ),
            Ok(Request::Auth { jwt }) => match Self::handle_auth(&jwt, sessions, public_key_pem) {
                Ok(data) => Response::success(ResponseData::Session(data)),
                Err(e) => Response::error(e.to_string()),
            },
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
    ) -> Result<SessionData> {
        let pem = public_key_pem.ok_or_else(|| {
            LogHavenError::Config("no public key configured; run `loghaven auth keygen`".into())
        })?;

        let claims = verify_jwt(jwt, pem)?;

        let (token_id, secret, expires_in_secs) = sessions.issue_capped(
            &claims.sub,
            claims.exp.saturating_sub(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
        )?;

        Ok(SessionData {
            token_id,
            secret_hex: hex::encode(&secret),
            expires_in_secs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_is_an_explicit_error_not_a_fake_success() {
        let cfg = crate::config::default_config();
        let sessions = SessionStore::new(60);
        let (json, stop) = DaemonServer::handle_request("\"Reload\"", 0, &cfg, &sessions, None);
        let r: Response = serde_json::from_str(&json).unwrap();
        assert!(!r.success);
        assert!(!stop);
    }

    #[test]
    fn stop_sets_flag_and_garbage_is_rejected() {
        let cfg = crate::config::default_config();
        let sessions = SessionStore::new(60);
        assert!(DaemonServer::handle_request("\"Stop\"", 0, &cfg, &sessions, None).1);
        let (json, stop) = DaemonServer::handle_request("nonsense", 0, &cfg, &sessions, None);
        assert!(!stop);
        assert!(!serde_json::from_str::<Response>(&json).unwrap().success);
    }
}

#[cfg(unix)]
use crate::config::defaults::SUN_PATH_MAX;

/// The folder `defaults::socket_path` picks when the default is too long lives in a temp
/// directory that other users can write to, so it must be ours and closed to everyone else.
#[cfg(unix)]
fn ensure_private_if_chosen_for_length(dir: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let chosen = dir.file_name().is_some_and(|n| {
        n.to_string_lossy()
            .starts_with(crate::config::defaults::SHORT_SOCKET_DIR_PREFIX)
    });
    if !chosen {
        return Ok(());
    }
    let meta = std::fs::symlink_metadata(dir)?;
    let me = dirs::home_dir()
        .and_then(|h| std::fs::metadata(h).ok())
        .map(|m| m.uid());
    if !meta.is_dir() || me.is_some_and(|uid| uid != meta.uid()) {
        return Err(LogHavenError::Daemon(format!(
            "{} exists but is not a folder of yours; set daemon.socket_path in the config",
            dir.display()
        )));
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(unix)]
fn check_socket_path_len(path: &std::path::Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let len = path.as_os_str().as_bytes().len();
    if len >= SUN_PATH_MAX {
        return Err(LogHavenError::Daemon(format!(
            "socket path is {len} bytes but this system allows at most {}: {}; \
             set daemon.socket_path in the config to a shorter path, or use a shorter HOME",
            SUN_PATH_MAX - 1,
            path.display()
        )));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod socket_path_tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn a_folder_chosen_for_length_is_made_private_and_others_are_left_alone() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("lh-sock-{}", std::process::id()));
        let chosen = base.join(format!(
            "{}abc",
            crate::config::defaults::SHORT_SOCKET_DIR_PREFIX
        ));
        let plain = base.join("plain");
        for d in [&chosen, &plain] {
            std::fs::create_dir_all(d).unwrap();
            std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o755)).unwrap();
            ensure_private_if_chosen_for_length(d).unwrap();
        }
        let mode = |d: &PathBuf| std::fs::metadata(d).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&chosen), 0o700);
        assert_eq!(mode(&plain), 0o755);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn socket_path_at_the_limit_is_accepted_and_one_more_byte_is_not() {
        let at_limit = PathBuf::from(format!("/{}", "a".repeat(SUN_PATH_MAX - 2)));
        assert!(check_socket_path_len(&at_limit).is_ok());
        let over = PathBuf::from(format!("/{}", "a".repeat(SUN_PATH_MAX - 1)));
        let e = check_socket_path_len(&over).unwrap_err().to_string();
        assert!(e.contains("daemon.socket_path"), "{e}");
        assert!(e.contains(&format!("{}", SUN_PATH_MAX)), "{e}");
    }
}
