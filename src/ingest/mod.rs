pub mod payload;

use crate::auth::session::SessionStore;
use crate::error::Result;
use crate::storage::StorageRouter;
use payload::IngestPayload;
use std::sync::Arc;
use tokio::net::UdpSocket;

const MAX_DATAGRAM: usize = 65507;

pub struct IngestServer {
    port: u16,
    storage: Arc<StorageRouter>,
    sessions: Arc<SessionStore>,
    require_auth: bool,
}

impl IngestServer {
    pub fn new(
        port: u16,
        storage: Arc<StorageRouter>,
        sessions: Arc<SessionStore>,
        require_auth: bool,
    ) -> Self {
        Self {
            port,
            storage,
            sessions,
            require_auth,
        }
    }

    pub async fn listen(&self) -> Result<()> {
        // Loopback only — never expose ingest to the network
        let addr = format!("127.0.0.1:{}", self.port);
        let socket = UdpSocket::bind(&addr).await.map_err(|e| {
            crate::error::LogHavenError::Daemon(format!("ingest bind failed on {}: {}", addr, e))
        })?;

        println!("Ingest server listening on UDP {}", addr);

        let mut buf = vec![0u8; MAX_DATAGRAM];
        let mut rejected: u64 = 0;

        loop {
            let (n, _peer) = socket.recv_from(&mut buf).await.map_err(|e| {
                crate::error::LogHavenError::Daemon(format!("ingest recv error: {}", e))
            })?;

            // Verified inline so forged datagrams never cost a spawned task.
            let (session_app, payload_bytes) =
                match authenticate(&buf[..n], &self.sessions, self.require_auth) {
                    Ok(v) => v,
                    Err(e) => {
                        // loopback can be flooded; keep stderr I/O off the hot path
                        rejected += 1;
                        if rejected == 1 || rejected.is_multiple_of(1000) {
                            eprintln!("Ingest: auth failed ({} so far): {}", rejected, e);
                        }
                        continue;
                    }
                };

            let storage = Arc::clone(&self.storage);
            tokio::spawn(async move {
                process_payload(payload_bytes, session_app, storage).await;
            });
        }
    }
}

/// Returns the session's app (None when auth is disabled) and the payload bytes.
fn authenticate(
    data: &[u8],
    sessions: &SessionStore,
    require_auth: bool,
) -> Result<(Option<String>, Vec<u8>)> {
    if require_auth {
        let (app, bytes) = sessions.verify_datagram(data)?;
        Ok((Some(app), bytes))
    } else {
        Ok((None, data.to_vec()))
    }
}

async fn process_payload(
    payload_bytes: Vec<u8>,
    session_app: Option<String>,
    storage: Arc<StorageRouter>,
) {
    let payload: IngestPayload = match parse_payload(&payload_bytes) {
        Some(p) => p,
        None => {
            eprintln!(
                "Ingest: failed to parse datagram ({} bytes)",
                payload_bytes.len()
            );
            return;
        }
    };

    if let Err(e) = crate::storage::validate_app_name(&payload.app) {
        eprintln!("Ingest: invalid app: {}", e);
        return;
    }

    if let Some(session_app) = &session_app
        && *session_app != payload.app
    {
        eprintln!(
            "Ingest: session for '{}' tried to write to '{}'",
            session_app, payload.app
        );
        return;
    }

    for record in payload.records {
        let log_record = record.into_log_record(&payload.app);
        if let Err(e) = storage.write(&payload.app, log_record).await {
            eprintln!("Ingest write error: {}", e);
        }
    }
}

fn parse_payload(data: &[u8]) -> Option<IngestPayload> {
    if let Ok(p) = serde_json::from_slice::<IngestPayload>(data) {
        return Some(p);
    }

    if data
        .first()
        .map(|&b| b != b'{' && b != b'[')
        .unwrap_or(false)
        && let Ok(p) = rmp_serde::from_slice::<IngestPayload>(data)
    {
        return Some(p);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::sign_datagram;

    fn router(name: &str) -> (Arc<StorageRouter>, std::path::PathBuf) {
        let tmp = std::env::temp_dir().join(format!("lh-i-{}-{}", name, std::process::id()));
        let mut cfg = crate::config::default_config();
        cfg.storage.backend = "local".into();
        cfg.storage.local.path = tmp.clone();
        (Arc::new(StorageRouter::new(&cfg).unwrap()), tmp)
    }

    fn payload(app: &str) -> Vec<u8> {
        format!(r#"{{"app":"{app}","records":[{{"level":"info","source":"s","message":"m"}}]}}"#)
            .into_bytes()
    }

    #[test]
    fn authenticate_passthrough_when_auth_disabled() {
        let s = SessionStore::new(60);
        let (app, bytes) = authenticate(b"raw", &s, false).unwrap();
        assert!(app.is_none());
        assert_eq!(bytes, b"raw");
    }

    #[test]
    fn authenticate_rejects_unsigned_and_replayed_when_required() {
        let s = SessionStore::new(60);
        let (id, secret, _) = s.issue("web").unwrap();
        assert!(authenticate(&payload("web"), &s, true).is_err());
        let d = sign_datagram(&id, &secret, 1, &payload("web")).unwrap();
        let (app, bytes) = authenticate(&d, &s, true).unwrap();
        assert_eq!(app.as_deref(), Some("web"));
        assert_eq!(bytes, payload("web"));
        assert!(authenticate(&d, &s, true).is_err());
    }

    #[tokio::test]
    async fn session_cannot_write_to_another_app() {
        let (storage, tmp) = router("scope");
        process_payload(payload("api"), Some("web".into()), Arc::clone(&storage)).await;
        storage.shutdown_all().await.unwrap();
        assert!(!tmp.join("api").exists());
        assert!(!tmp.join("web").exists());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn matching_app_is_written() {
        let (storage, tmp) = router("match");
        process_payload(payload("web"), Some("web".into()), Arc::clone(&storage)).await;
        storage.shutdown_all().await.unwrap();
        assert!(tmp.join("web").exists());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn malformed_and_invalid_payloads_write_nothing() {
        let (storage, tmp) = router("bad");
        for bytes in [
            b"not json".to_vec(),
            payload("../evil"),
            payload(""),
            b"{}".to_vec(),
        ] {
            process_payload(bytes, None, Arc::clone(&storage)).await;
        }
        storage.shutdown_all().await.unwrap();
        assert!(!tmp.exists() || std::fs::read_dir(&tmp).unwrap().next().is_none());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn end_to_end_signed_udp_to_query() {
        let (storage, tmp) = router("e2e");
        let sessions = Arc::new(SessionStore::new(60));
        let (id, secret, _) = sessions.issue("web").unwrap();

        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = {
            let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let server = IngestServer::new(port, Arc::clone(&storage), Arc::clone(&sessions), true);
        let handle = tokio::spawn(async move { server.listen().await });
        let dest = format!("127.0.0.1:{port}");
        let q = || {
            let f = crate::storage::query::QueryFilter {
                app: "web".into(),
                ..Default::default()
            };
            crate::storage::query::query(&tmp, &f).unwrap().len()
        };

        // the first datagram may be sent before the server is bound: resend with fresh counters
        let mut counter = 1;
        let mut count = 0;
        for _ in 0..100 {
            assert!(!handle.is_finished(), "ingest server exited early");
            let d = sign_datagram(&id, &secret, counter, &payload("web")).unwrap();
            sock.send_to(&d, &dest).await.unwrap();
            counter += 1;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            storage.flush_all().await.unwrap();
            count = q();
            if count > 0 {
                break;
            }
        }
        assert!(count >= 1, "signed datagram never landed");
        let landed = count;

        // everything below must be rejected; send, give the server time, re-check
        let last = sign_datagram(&id, &secret, counter - 1, &payload("web")).unwrap();
        sock.send_to(&last, &dest).await.unwrap(); // replay
        let other_app = sign_datagram(&id, &secret, counter, &payload("api")).unwrap();
        sock.send_to(&other_app, &dest).await.unwrap(); // wrong app
        sock.send_to(&payload("web"), &dest).await.unwrap(); // unsigned
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        storage.flush_all().await.unwrap();
        assert_eq!(
            q(),
            landed,
            "replayed, cross-app and unsigned datagrams are dropped"
        );
        assert!(!tmp.join("api").exists());

        handle.abort();
        storage.shutdown_all().await.unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
