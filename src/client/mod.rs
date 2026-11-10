//! Client side of LogHaven: authenticate against a local daemon and send logs.
//!
//! The wire format is documented in `docs/WIRE_FORMAT.md`.

pub mod follow;
mod query_token;

pub use query_token::QueryToken;

use crate::auth::session::sign_datagram;
use crate::auth::token::issue_jwt;
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use crate::ipc::protocol::SessionData;
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

/// Largest UDP payload on IPv4 (65535 - 8 UDP header - 20 IP header).
#[cfg(not(target_vendor = "apple"))]
const MAX_DATAGRAM: usize = 65507;
/// macOS refuses larger datagrams with EMSGSIZE (`net.inet.udp.maxdgram` is 9216).
#[cfg(target_vendor = "apple")]
const MAX_DATAGRAM: usize = 9000;
/// Room for `token_id:counter:hmac\n` (64 + 1 + 20 + 1 + 64 + 1 bytes).
const HEADER_RESERVE: usize = 160;
const MAX_PAYLOAD: usize = MAX_DATAGRAM - HEADER_RESERVE;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClientRecord {
    pub level: String,
    pub source: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Signs a JWT for `app` with the daemon's private key.
pub fn mint_jwt(cfg: &Config, app: &str, ttl_secs: u64) -> Result<String> {
    crate::storage::validate_app_name(app)?;
    let path = cfg.auth.keys_dir.join("private.pem");
    let pem = std::fs::read_to_string(&path).map_err(|e| {
        LogHavenError::Config(format!(
            "cannot read private key {} ({}); run 'loghaven auth keygen'",
            path.display(),
            e
        ))
    })?;
    issue_jwt(app, &pem, ttl_secs)
}

/// Exchanges a JWT for a session via the daemon's `POST /auth`.
pub async fn exchange_jwt(query_port: u16, jwt: &str) -> Result<SessionData> {
    let url = format!("http://127.0.0.1:{query_port}/auth");
    let resp = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| LogHavenError::Daemon(e.to_string()))?
        .post(&url)
        .json(&serde_json::json!({ "jwt": jwt }))
        .send()
        .await
        .map_err(|e| LogHavenError::Daemon(format!("cannot reach query server at {url}: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        let hint = match status.as_u16() {
            401 => {
                " (JWT rejected: key mismatch or expired; did you re-run keygen without restarting the daemon?)"
            }
            503 => {
                " (daemon has no public key; run 'loghaven auth keygen' then restart the daemon)"
            }
            _ => "",
        };
        return Err(LogHavenError::Daemon(format!(
            "auth failed with {status}{hint}"
        )));
    }
    resp.json::<SessionData>()
        .await
        .map_err(|e| LogHavenError::Daemon(format!("bad auth response: {e}")))
}

/// Mints a JWT locally and exchanges it for a session scoped to `app`.
pub async fn open_session(cfg: &Config, app: &str, ttl_secs: u64) -> Result<SessionData> {
    let jwt = mint_jwt(cfg, app, ttl_secs)?;
    exchange_jwt(cfg.auth.query_port, &jwt).await
}

#[derive(serde::Deserialize)]
struct LogsResponse {
    records: Vec<crate::storage::record::LogRecord>,
}

/// Calls `GET /logs`. Records come back newest first.
pub async fn fetch_logs(
    query_port: u16,
    params: &[(&'static str, String)],
    token: Option<&str>,
) -> Result<Vec<crate::storage::record::LogRecord>> {
    let url = format!("http://127.0.0.1:{query_port}/logs");
    let mut req = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT * 4)
        .build()
        .map_err(|e| LogHavenError::Daemon(e.to_string()))?
        .get(&url)
        .query(params);
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = req
        .send()
        .await
        .map_err(|e| LogHavenError::Daemon(format!("cannot reach query server at {url}: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(LogHavenError::Daemon(format!(
            "query failed {status}: {body}"
        )));
    }
    Ok(resp
        .json::<LogsResponse>()
        .await
        .map_err(|e| LogHavenError::Daemon(format!("bad query response: {e}")))?
        .records)
}

enum Auth {
    Signed {
        token_id: String,
        secret: Vec<u8>,
        counter: u64,
        expires_at: Instant,
    },
    /// Daemon runs with `require_auth = false`.
    None,
}

pub struct IngestClient {
    socket: UdpSocket,
    dest: String,
    app: String,
    auth: Auth,
}

impl IngestClient {
    async fn bind(port: u16, app: &str, auth: Auth) -> Result<Self> {
        crate::storage::validate_app_name(app)?;
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        Ok(Self {
            socket,
            dest: format!("127.0.0.1:{port}"),
            app: app.to_string(),
            auth,
        })
    }

    pub async fn with_session(ingest_port: u16, app: &str, session: &SessionData) -> Result<Self> {
        let secret = hex::decode(&session.secret_hex)
            .map_err(|_| LogHavenError::Daemon("session secret is not hex".into()))?;
        let auth = Auth::Signed {
            token_id: session.token_id.clone(),
            secret,
            counter: 0,
            expires_at: Instant::now() + Duration::from_secs(session.expires_in_secs),
        };
        Self::bind(ingest_port, app, auth).await
    }

    pub async fn unauthenticated(ingest_port: u16, app: &str) -> Result<Self> {
        Self::bind(ingest_port, app, Auth::None).await
    }

    /// Connects the way the config says: signed session, or plain when the
    /// daemon does not require auth.
    pub async fn connect(cfg: &Config, app: &str, ttl_secs: u64) -> Result<Self> {
        if cfg.auth.require_auth {
            let session = open_session(cfg, app, ttl_secs).await?;
            Self::with_session(cfg.daemon.ingest_port, app, &session).await
        } else {
            Self::unauthenticated(cfg.daemon.ingest_port, app).await
        }
    }

    /// Sends records in as few datagrams as fit. Returns the number of datagrams sent.
    pub async fn send(&mut self, records: &[ClientRecord]) -> Result<usize> {
        let payloads = build_payloads(&self.app, records)?;
        for payload in &payloads {
            let datagram = self.frame(payload)?;
            self.socket.send_to(&datagram, &self.dest).await?;
        }
        Ok(payloads.len())
    }

    fn frame(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        match &mut self.auth {
            Auth::None => Ok(payload.to_vec()),
            Auth::Signed {
                token_id,
                secret,
                counter,
                expires_at,
            } => {
                if Instant::now() >= *expires_at {
                    return Err(LogHavenError::Daemon("session expired".into()));
                }
                *counter += 1;
                sign_datagram(token_id, secret, *counter, payload)
            }
        }
    }
}

/// Packs records into JSON payloads of at most `MAX_PAYLOAD` bytes each.
fn build_payloads(app: &str, records: &[ClientRecord]) -> Result<Vec<Vec<u8>>> {
    let head = format!(r#"{{"app":{},"records":["#, serde_json::to_string(app)?);
    let tail = "]}";
    let mut out = Vec::new();
    let mut cur: Vec<String> = Vec::new();
    let mut cur_len = head.len() + tail.len();

    let flush = |cur: &mut Vec<String>, cur_len: &mut usize, out: &mut Vec<Vec<u8>>| {
        if !cur.is_empty() {
            out.push(format!("{head}{}{tail}", cur.join(",")).into_bytes());
            cur.clear();
            *cur_len = head.len() + tail.len();
        }
    };

    for r in records {
        let json = serde_json::to_string(r)?;
        if head.len() + tail.len() + json.len() > MAX_PAYLOAD {
            return Err(LogHavenError::Config(format!(
                "a single record is {} bytes; the limit is {} per datagram",
                json.len(),
                MAX_PAYLOAD - head.len() - tail.len()
            )));
        }
        let extra = json.len() + usize::from(!cur.is_empty());
        if cur_len + extra > MAX_PAYLOAD {
            flush(&mut cur, &mut cur_len, &mut out);
        }
        cur_len += json.len() + usize::from(!cur.is_empty());
        cur.push(json);
    }
    flush(&mut cur, &mut cur_len, &mut out);
    Ok(out)
}

impl From<serde_json::Error> for LogHavenError {
    fn from(e: serde_json::Error) -> Self {
        LogHavenError::Config(format!("json: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::SessionStore;
    use crate::ingest::payload::IngestPayload;
    use std::sync::Arc;

    fn rec(msg: &str) -> ClientRecord {
        ClientRecord {
            level: "info".into(),
            source: "test".into(),
            message: msg.into(),
            ..Default::default()
        }
    }

    #[test]
    fn payload_parses_with_the_servers_own_types() {
        let mut r = rec("hello \"quoted\" \u{1f980}\nnewline");
        r.tx_hash = Some("0xabc".into());
        r.chain = Some("ethereum".into());
        r.tags = Some(serde_json::json!(["a", "b"]));
        r.metadata = Some(serde_json::json!({"k": 1}));
        let payloads = build_payloads("web", &[r.clone(), rec("two")]).unwrap();
        assert_eq!(payloads.len(), 1);
        let parsed: IngestPayload = serde_json::from_slice(&payloads[0]).unwrap();
        assert_eq!(parsed.app, "web");
        assert_eq!(parsed.records.len(), 2);
        assert_eq!(parsed.records[0].message, r.message);
        assert_eq!(parsed.records[0].tx_hash.as_deref(), Some("0xabc"));
        assert_eq!(parsed.records[1].trace_id, None);
    }

    #[tokio::test]
    async fn a_large_batch_is_accepted_by_the_os_socket() {
        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = sink.local_addr().unwrap().port();
        let mut client = IngestClient::unauthenticated(port, "web").await.unwrap();
        let records: Vec<_> = (0..2000)
            .map(|i| rec(&format!("{i}-{}", "x".repeat(500))))
            .collect();
        let sent = client.send(&records).await.unwrap();
        assert!(sent > 1);
    }

    #[test]
    fn empty_input_sends_nothing() {
        assert!(build_payloads("web", &[]).unwrap().is_empty());
    }

    #[test]
    fn large_batches_are_split_and_every_payload_fits() {
        let records: Vec<_> = (0..500)
            .map(|i| rec(&format!("{i}-{}", "x".repeat(1000))))
            .collect();
        let payloads = build_payloads("web", &records).unwrap();
        assert!(payloads.len() > 1);
        let mut total = 0;
        for p in &payloads {
            assert!(p.len() <= MAX_PAYLOAD, "{}", p.len());
            total += serde_json::from_slice::<IngestPayload>(p)
                .unwrap()
                .records
                .len();
        }
        assert_eq!(total, 500, "no record lost or duplicated");
    }

    #[test]
    fn single_record_too_large_is_an_error() {
        let big = rec(&"x".repeat(MAX_PAYLOAD));
        assert!(build_payloads("web", &[big]).is_err());
        let edge = rec(&"x".repeat(MAX_PAYLOAD - 200));
        assert!(build_payloads("web", &[edge]).is_ok());
    }

    #[test]
    fn exact_fit_boundaries_never_exceed_the_limit() {
        for pad in 0..40 {
            let records = vec![
                rec(&"a".repeat(MAX_PAYLOAD / 2 - 120 + pad)),
                rec(&"b".repeat(MAX_PAYLOAD / 2 - 120 + pad)),
            ];
            for p in build_payloads("web", &records).unwrap() {
                assert!(p.len() <= MAX_PAYLOAD);
            }
        }
    }

    #[tokio::test]
    async fn signed_frames_verify_with_increasing_counters() {
        let store = SessionStore::new(60);
        let (id, secret, ttl) = store.issue("web").unwrap();
        let session = SessionData {
            token_id: id,
            secret_hex: hex::encode(secret),
            expires_in_secs: ttl,
        };
        let mut c = IngestClient::with_session(1, "web", &session)
            .await
            .unwrap();
        for _ in 0..3 {
            let f = c.frame(b"{}").unwrap();
            assert_eq!(store.verify_datagram(&f).unwrap().0, "web");
        }
        let replay = c.frame(b"{}").unwrap();
        assert!(store.verify_datagram(&replay).is_ok());
        assert!(store.verify_datagram(&replay).is_err());
    }

    #[tokio::test]
    async fn expired_session_refuses_to_send() {
        let session = SessionData {
            token_id: "t".into(),
            secret_hex: "00".repeat(32),
            expires_in_secs: 0,
        };
        let mut c = IngestClient::with_session(1, "web", &session)
            .await
            .unwrap();
        assert!(c.send(&[rec("x")]).await.is_err());
        let bad = SessionData {
            secret_hex: "zz".into(),
            ..session
        };
        assert!(IngestClient::with_session(1, "web", &bad).await.is_err());
        assert!(IngestClient::unauthenticated(1, "../x").await.is_err());
    }

    #[test]
    fn jwt_minting_needs_keys_and_a_valid_app() {
        let mut cfg = crate::config::default_config();
        cfg.auth.keys_dir = std::env::temp_dir().join(format!("lh-nokeys-{}", std::process::id()));
        assert!(mint_jwt(&cfg, "web", 60).is_err());
        assert!(mint_jwt(&cfg, "../x", 60).is_err());
    }

    #[test]
    fn minted_jwt_verifies_against_the_generated_public_key() {
        let dir = std::env::temp_dir().join(format!("lh-keys-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        crate::auth::keys::generate_keypair(&dir.join("private.pem"), &dir.join("public.pem"))
            .unwrap();
        let mut cfg = crate::config::default_config();
        cfg.auth.keys_dir = dir.clone();
        let jwt = mint_jwt(&cfg, "web", 60).unwrap();
        let pub_pem = std::fs::read_to_string(dir.join("public.pem")).unwrap();
        let claims = crate::auth::token::verify_jwt(&jwt, &pub_pem).unwrap();
        assert_eq!(claims.sub, "web");
        let expired = crate::auth::token::issue_jwt(
            "web",
            &std::fs::read_to_string(dir.join("private.pem")).unwrap(),
            0,
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(crate::auth::token::verify_jwt(&expired, &pub_pem).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    async fn serve(
        name: &str,
        require_auth: bool,
    ) -> (
        u16,
        Arc<SessionStore>,
        Arc<crate::storage::StorageRouter>,
        std::path::PathBuf,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let tmp = std::env::temp_dir().join(format!("lh-cl-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let mut cfg = crate::config::default_config();
        cfg.storage.backend = "local".into();
        cfg.storage.local.path = tmp.clone();
        let storage = Arc::new(crate::storage::StorageRouter::new(&cfg).unwrap());
        let sessions = Arc::new(SessionStore::new(60));
        let port = {
            let p = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            p.local_addr().unwrap().port()
        };
        let server = crate::ingest::IngestServer::new(
            port,
            Arc::clone(&storage),
            Arc::clone(&sessions),
            require_auth,
        );
        let h = tokio::spawn(async move { server.listen().await });
        (port, sessions, storage, tmp, h)
    }

    async fn wait_for(
        storage: &crate::storage::StorageRouter,
        tmp: &std::path::Path,
        app: &str,
        want: usize,
    ) -> Vec<crate::storage::record::LogRecord> {
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            storage.flush_all().await.unwrap();
            let f = crate::storage::query::QueryFilter {
                app: app.into(),
                limit: 10_000,
                ..Default::default()
            };
            let got = crate::storage::query::query(tmp, &f).unwrap();
            if got.len() >= want {
                return got;
            }
        }
        panic!("records never arrived");
    }

    #[tokio::test]
    async fn real_client_to_real_server_with_auth() {
        let (port, sessions, storage, tmp, h) = serve("auth", true).await;
        let (id, secret, ttl) = sessions.issue("web").unwrap();
        let session = SessionData {
            token_id: id,
            secret_hex: hex::encode(secret),
            expires_in_secs: ttl,
        };
        let mut c = IngestClient::with_session(port, "web", &session)
            .await
            .unwrap();
        let mut r = rec("hello");
        r.tx_hash = Some("0xfeed".into());
        r.chain = Some("ethereum".into());
        let sent = c.send(&vec![r; 3]).await.unwrap();
        assert_eq!(sent, 1);
        c.send(&[rec("second")]).await.unwrap();
        let got = wait_for(&storage, &tmp, "web", 4).await;
        assert_eq!(got.len(), 4);
        assert!(got.iter().any(|r| r.tx_hash.as_deref() == Some("0xfeed")));

        // a client for another app cannot write to web
        let (id2, secret2, ttl2) = sessions.issue("api").unwrap();
        let other = SessionData {
            token_id: id2,
            secret_hex: hex::encode(secret2),
            expires_in_secs: ttl2,
        };
        let mut evil = IngestClient::with_session(port, "web", &other)
            .await
            .unwrap();
        evil.send(&[rec("forged")]).await.unwrap();
        // the marker travels the same path after the forged one, so once it
        // has landed the forged datagram has certainly been processed
        c.send(&[rec("marker")]).await.unwrap();
        let after = wait_for(&storage, &tmp, "web", 5).await;
        assert!(after.iter().any(|r| r.message == "marker"));
        assert!(after.iter().all(|r| r.message != "forged"));

        h.abort();
        storage.shutdown_all().await.unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn unauthenticated_client_works_when_auth_is_off() {
        let (port, _s, storage, tmp, h) = serve("noauth", false).await;
        let mut c = IngestClient::unauthenticated(port, "web").await.unwrap();
        c.send(&[rec("plain")]).await.unwrap();
        let got = wait_for(&storage, &tmp, "web", 1).await;
        assert_eq!(got[0].message, "plain");
        h.abort();
        storage.shutdown_all().await.unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
