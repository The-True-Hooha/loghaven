use crate::error::{LogHavenError, Result};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

const TOKEN_BYTES: usize = 32;
const HMAC_TAG_LEN: usize = 32;

#[derive(Debug, Clone)]
pub struct SessionToken {
    pub app: String,
    pub secret: Vec<u8>, // 32-byte HMAC key
    pub expires_at: u64,
}

pub struct SessionStore {
    sessions: Mutex<HashMap<String, SessionToken>>, // keyed by token_id
    ttl_secs: u64,
}

impl SessionStore {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl_secs,
        }
    }

    /// Issue a session token for an app. Returns (token_id, hmac_secret).
    pub fn issue(&self, app: &str) -> Result<(String, Vec<u8>)> {
        let mut rng = rand::thread_rng();

        let mut token_id_bytes = [0u8; TOKEN_BYTES];
        rng.fill_bytes(&mut token_id_bytes);
        let token_id = hex::encode(token_id_bytes);

        let mut secret = vec![0u8; TOKEN_BYTES];
        rng.fill_bytes(&mut secret);

        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + self.ttl_secs;

        let session = SessionToken {
            app: app.to_string(),
            secret: secret.clone(),
            expires_at,
        };

        self.sessions
            .lock()
            .unwrap()
            .insert(token_id.clone(), session);

        Ok((token_id, secret))
    }

    /// Verify a Bearer token (token_id only — presence + expiry check for HTTP).
    pub fn verify_session_token(&self, token_id: &str) -> Result<String> {
        let sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get(token_id)
            .ok_or_else(|| LogHavenError::Config("unknown token".into()))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now > session.expires_at {
            return Err(LogHavenError::Config("session expired".into()));
        }

        Ok(session.app.clone())
    }

    /// Verify a datagram. Expected format: token_id:hmac_hex:payload_bytes
    /// Returns the app name if valid.
    pub fn verify_datagram(&self, data: &[u8]) -> Result<(String, Vec<u8>)> {
        // Split off the header line: "token_id:hmac_hex\n"
        let sep = data
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| LogHavenError::Config("missing auth header".into()))?;

        let header = std::str::from_utf8(&data[..sep])
            .map_err(|_| LogHavenError::Config("invalid auth header".into()))?;

        let mut parts = header.splitn(2, ':');
        let token_id = parts
            .next()
            .ok_or_else(|| LogHavenError::Config("missing token_id".into()))?;
        let hmac_hex = parts
            .next()
            .ok_or_else(|| LogHavenError::Config("missing hmac".into()))?;

        let payload = &data[sep + 1..];

        let sessions = self.sessions.lock().unwrap();
        let session = sessions
            .get(token_id)
            .ok_or_else(|| LogHavenError::Config("unknown token".into()))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now > session.expires_at {
            return Err(LogHavenError::Config("session expired".into()));
        }

        let expected_hmac = hex::decode(hmac_hex)
            .map_err(|_| LogHavenError::Config("invalid hmac encoding".into()))?;

        let mut mac = HmacSha256::new_from_slice(&session.secret)
            .map_err(|_| LogHavenError::Config("hmac init failed".into()))?;
        mac.update(payload);
        mac.verify_slice(&expected_hmac)
            .map_err(|_| LogHavenError::Config("hmac mismatch".into()))?;

        Ok((session.app.clone(), payload.to_vec()))
    }

    /// Remove expired sessions (call periodically).
    pub fn purge_expired(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.sessions
            .lock()
            .unwrap()
            .retain(|_, s| s.expires_at > now);
    }
}

/// Build a signed datagram: prepend "token_id:hmac_hex\n" before payload bytes.
/// SDKs call this before sending UDP.
pub fn sign_datagram(token_id: &str, secret: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| LogHavenError::Config("hmac init failed".into()))?;
    mac.update(payload);
    let tag = mac.finalize().into_bytes();
    let hmac_hex = hex::encode(tag);

    let header = format!("{}:{}\n", token_id, hmac_hex);
    let mut out = header.into_bytes();
    out.extend_from_slice(payload);
    Ok(out)
}
