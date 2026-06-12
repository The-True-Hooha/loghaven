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

        loop {
            let (n, _peer) = socket.recv_from(&mut buf).await.map_err(|e| {
                crate::error::LogHavenError::Daemon(format!("ingest recv error: {}", e))
            })?;

            let storage = Arc::clone(&self.storage);
            let sessions = Arc::clone(&self.sessions);
            let data = buf[..n].to_vec();
            let require_auth = self.require_auth;

            tokio::spawn(async move {
                handle_datagram(data, storage, sessions, require_auth).await;
            });
        }
    }
}

async fn handle_datagram(
    data: Vec<u8>,
    storage: Arc<StorageRouter>,
    sessions: Arc<SessionStore>,
    require_auth: bool,
) {
    let payload_bytes = if require_auth {
        match sessions.verify_datagram(&data) {
            Ok((_app, bytes)) => bytes,
            Err(e) => {
                eprintln!("Ingest: auth failed: {}", e);
                return;
            }
        }
    } else {
        data
    };

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

    if payload.app.is_empty() {
        eprintln!("Ingest: payload missing app field");
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
    {
        if let Ok(p) = rmp_serde::from_slice::<IngestPayload>(data) {
            return Some(p);
        }
    }

    None
}
