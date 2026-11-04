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
const MAX_SESSIONS_PER_APP: usize = 32;
/// Counters seen within this many steps below the highest one are tracked, so
/// UDP reordering does not drop datagrams. Anything older is rejected.
const REPLAY_WINDOW: u64 = 64;

#[derive(Debug, Clone)]
pub struct SessionToken {
    pub app: String,
    pub secret: Vec<u8>, // 32-byte HMAC key
    pub expires_at: u64,
    pub last_counter: u64,
    seen: u64, // bit i set: counter (last_counter - 1 - i) was accepted
}

impl SessionToken {
    /// Accept `counter` once; false if replayed, too old, or the reserved maximum.
    fn accept_counter(&mut self, counter: u64) -> bool {
        if counter == 0 || counter == u64::MAX {
            return false;
        }
        if counter > self.last_counter {
            let shift = counter - self.last_counter;
            self.seen = if shift > REPLAY_WINDOW {
                0
            } else {
                // the previous highest counter becomes bit (shift - 1)
                let carried = if shift == REPLAY_WINDOW {
                    0
                } else {
                    self.seen << shift
                };
                carried | (1u64 << (shift - 1))
            };
            self.last_counter = counter;
            return true;
        }
        let back = self.last_counter - counter;
        if back == 0 || back > REPLAY_WINDOW {
            return false;
        }
        let bit = 1u64 << (back - 1);
        if self.seen & bit != 0 {
            return false;
        }
        self.seen |= bit;
        true
    }
}

pub struct SessionStore {
    sessions: Mutex<HashMap<String, SessionToken>>, // keyed by token_id
    ttl_secs: u64,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn datagram_mac(secret: &[u8], counter: u64, payload: &[u8]) -> Result<HmacSha256> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| LogHavenError::Config("hmac init failed".into()))?;
    mac.update(&counter.to_be_bytes());
    mac.update(payload);
    Ok(mac)
}

impl SessionStore {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl_secs,
        }
    }

    /// Issue a session token for an app.
    /// Returns (token_id, hmac_secret, seconds until expiry).
    pub fn issue(&self, app: &str) -> Result<(String, Vec<u8>, u64)> {
        self.issue_capped(app, u64::MAX)
    }

    /// Like `issue`, but the session never outlives `cap_secs` (the remaining
    /// life of the JWT that authorised it).
    pub fn issue_capped(&self, app: &str, cap_secs: u64) -> Result<(String, Vec<u8>, u64)> {
        crate::storage::validate_app_name(app)?;
        let ttl = self.ttl_secs.min(cap_secs);
        if ttl == 0 {
            return Err(LogHavenError::Config("credential already expired".into()));
        }

        let mut rng = rand::thread_rng();

        let mut token_id_bytes = [0u8; TOKEN_BYTES];
        rng.fill_bytes(&mut token_id_bytes);
        let token_id = hex::encode(token_id_bytes);

        let mut secret = vec![0u8; TOKEN_BYTES];
        rng.fill_bytes(&mut secret);

        let session = SessionToken {
            app: app.to_string(),
            secret: secret.clone(),
            expires_at: now_secs() + ttl,
            last_counter: 0,
            seen: 0,
        };

        let mut sessions = lock(&self.sessions);
        let now = now_secs();
        sessions.retain(|_, s| s.expires_at > now);
        let mut same_app: Vec<(String, u64)> = sessions
            .iter()
            .filter(|(_, s)| s.app == app)
            .map(|(id, s)| (id.clone(), s.expires_at))
            .collect();
        if same_app.len() >= MAX_SESSIONS_PER_APP {
            same_app.sort_by_key(|(_, exp)| *exp);
            let excess = same_app.len() + 1 - MAX_SESSIONS_PER_APP;
            for (id, _) in same_app.into_iter().take(excess) {
                sessions.remove(&id);
            }
        }
        sessions.insert(token_id.clone(), session);

        Ok((token_id, secret, ttl))
    }

    /// Verify a Bearer token (token_id only: presence + expiry check for HTTP).
    /// Returns the app the session belongs to.
    pub fn verify_session_token(&self, token_id: &str) -> Result<String> {
        let sessions = lock(&self.sessions);
        let session = sessions
            .get(token_id)
            .ok_or_else(|| LogHavenError::Config("unknown token".into()))?;

        if now_secs() >= session.expires_at {
            return Err(LogHavenError::Config("session expired".into()));
        }

        Ok(session.app.clone())
    }

    /// Verify a datagram. Format: `token_id:counter:hmac_hex\npayload`.
    /// The HMAC covers `counter (8 bytes big endian) || payload`. The counter
    /// must be strictly greater than the last accepted one, which blocks replay.
    /// Returns the session's app and the payload.
    pub fn verify_datagram(&self, data: &[u8]) -> Result<(String, Vec<u8>)> {
        let sep = data
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| LogHavenError::Config("missing auth header".into()))?;

        let header = std::str::from_utf8(&data[..sep])
            .map_err(|_| LogHavenError::Config("invalid auth header".into()))?;

        let mut parts = header.splitn(3, ':');
        let token_id = parts
            .next()
            .ok_or_else(|| LogHavenError::Config("missing token_id".into()))?;
        let counter: u64 = parts
            .next()
            .ok_or_else(|| LogHavenError::Config("missing counter".into()))?
            .parse()
            .map_err(|_| LogHavenError::Config("invalid counter".into()))?;
        let hmac_hex = parts
            .next()
            .ok_or_else(|| LogHavenError::Config("missing hmac".into()))?;

        let payload = &data[sep + 1..];

        let mut sessions = lock(&self.sessions);
        let session = sessions
            .get_mut(token_id)
            .ok_or_else(|| LogHavenError::Config("unknown token".into()))?;

        if now_secs() >= session.expires_at {
            return Err(LogHavenError::Config("session expired".into()));
        }

        let expected_hmac = hex::decode(hmac_hex)
            .map_err(|_| LogHavenError::Config("invalid hmac encoding".into()))?;

        datagram_mac(&session.secret, counter, payload)?
            .verify_slice(&expected_hmac)
            .map_err(|_| LogHavenError::Config("hmac mismatch".into()))?;

        // checked after the MAC so forged datagrams cannot probe or advance the counter
        if !session.accept_counter(counter) {
            return Err(LogHavenError::Config("replayed or stale datagram".into()));
        }

        Ok((session.app.clone(), payload.to_vec()))
    }

    /// Remove expired sessions (call periodically).
    pub fn purge_expired(&self) {
        let now = now_secs();
        lock(&self.sessions).retain(|_, s| s.expires_at > now);
    }
}

/// Build a signed datagram: prepend `token_id:counter:hmac_hex\n` before the
/// payload bytes. SDKs call this before sending UDP. `counter` must start at 1,
/// be unique per datagram of a session and stay below u64::MAX. The server
/// tolerates reordering within the last 64 counters.
pub fn sign_datagram(
    token_id: &str,
    secret: &[u8],
    counter: u64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    if counter == u64::MAX {
        return Err(LogHavenError::Config("counter exhausted".into()));
    }
    let tag = datagram_mac(secret, counter, payload)?
        .finalize()
        .into_bytes();

    let header = format!("{}:{}:{}\n", token_id, counter, hex::encode(tag));
    let mut out = header.into_bytes();
    out.extend_from_slice(payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (SessionStore, String, Vec<u8>) {
        let s = SessionStore::new(60);
        let (id, secret, _) = s.issue("web").unwrap();
        (s, id, secret)
    }

    #[test]
    fn round_trip() {
        let (s, id, secret) = store();
        let d = sign_datagram(&id, &secret, 1, b"hello").unwrap();
        let (app, payload) = s.verify_datagram(&d).unwrap();
        assert_eq!(app, "web");
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn payload_may_contain_newlines_and_colons() {
        let (s, id, secret) = store();
        let d = sign_datagram(&id, &secret, 1, b"a:b\nc:d\n").unwrap();
        assert_eq!(s.verify_datagram(&d).unwrap().1, b"a:b\nc:d\n");
    }

    #[test]
    fn replay_is_rejected() {
        let (s, id, secret) = store();
        let d = sign_datagram(&id, &secret, 1, b"x").unwrap();
        s.verify_datagram(&d).unwrap();
        assert!(s.verify_datagram(&d).is_err());
    }

    #[test]
    fn reordering_within_window_is_accepted_once() {
        let (s, id, secret) = store();
        let send = |c: u64| s.verify_datagram(&sign_datagram(&id, &secret, c, b"x").unwrap());
        send(5).unwrap();
        send(3).unwrap();
        send(4).unwrap();
        send(1).unwrap();
        for c in [5, 4, 3, 1] {
            assert!(send(c).is_err(), "{c}");
        }
        send(6).unwrap();
        send(2).unwrap();
    }

    #[test]
    fn counters_older_than_window_are_rejected() {
        let (s, id, secret) = store();
        let send = |c: u64| s.verify_datagram(&sign_datagram(&id, &secret, c, b"x").unwrap());
        send(100).unwrap();
        assert!(send(35).is_err());
        send(36).unwrap();
        send(101).unwrap();
        assert!(send(36).is_err());
        assert!(send(37).is_ok());
    }

    #[test]
    fn large_jump_clears_window() {
        let (s, id, secret) = store();
        let send = |c: u64| s.verify_datagram(&sign_datagram(&id, &secret, c, b"x").unwrap());
        send(10).unwrap();
        send(10 + 64).unwrap();
        assert!(send(10).is_err());
        send(11 + 64).unwrap();
        send(10 + 64 + 65).unwrap();
        assert!(send(10 + 64).is_err());
        send(10 + 64 + 64).unwrap();
    }

    #[test]
    fn window_boundary_shifts_are_exact() {
        for shift in [1u64, 2, 63, 64, 65] {
            let (s, id, secret) = store();
            let send = |c: u64| s.verify_datagram(&sign_datagram(&id, &secret, c, b"x").unwrap());
            send(1000).unwrap();
            send(1000 + shift).unwrap();
            assert!(send(1000).is_err(), "shift {shift}: old counter replay");
            assert!(send(1000 + shift).is_err(), "shift {shift}: top replay");
        }
    }

    #[test]
    fn counter_zero_is_rejected() {
        let (s, id, secret) = store();
        assert!(
            s.verify_datagram(&sign_datagram(&id, &secret, 0, b"x").unwrap())
                .is_err()
        );
    }

    #[test]
    fn forged_datagram_does_not_advance_counter() {
        let (s, id, secret) = store();
        let forged = sign_datagram(&id, &[9u8; 32], 1_000_000, b"x").unwrap();
        assert!(s.verify_datagram(&forged).is_err());
        s.verify_datagram(&sign_datagram(&id, &secret, 1, b"x").unwrap())
            .unwrap();
    }

    #[test]
    fn tampered_counter_or_payload_is_rejected() {
        let (s, id, secret) = store();
        let good = sign_datagram(&id, &secret, 1, b"payload").unwrap();

        let mut tampered_payload = good.clone();
        *tampered_payload.last_mut().unwrap() ^= 1;
        assert!(s.verify_datagram(&tampered_payload).is_err());

        let text = String::from_utf8(good).unwrap();
        let tampered_counter = text.replacen(":1:", ":2:", 1);
        assert!(s.verify_datagram(tampered_counter.as_bytes()).is_err());
    }

    #[test]
    fn unknown_token_is_rejected() {
        let (s, _, secret) = store();
        let d = sign_datagram(&"0".repeat(64), &secret, 1, b"x").unwrap();
        assert!(s.verify_datagram(&d).is_err());
    }

    #[test]
    fn expired_session_is_rejected() {
        let (s, id, secret) = store();
        s.sessions.lock().unwrap().get_mut(&id).unwrap().expires_at = 1;
        assert!(
            s.verify_datagram(&sign_datagram(&id, &secret, 1, b"x").unwrap())
                .is_err()
        );
        assert!(s.verify_session_token(&id).is_err());
        s.purge_expired();
        assert!(s.sessions.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_headers_are_rejected() {
        let (s, id, _) = store();
        let cases: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"no newline".to_vec(),
            b"\n".to_vec(),
            format!("{id}\npayload").into_bytes(),
            format!("{id}:1\npayload").into_bytes(),
            format!("{id}:abc:00\npayload").into_bytes(),
            format!("{id}:-1:00\npayload").into_bytes(),
            format!("{id}:1:zz\npayload").into_bytes(),
            format!("{id}:1:\npayload").into_bytes(),
            format!("{id}:18446744073709551616:00\npayload").into_bytes(),
            vec![0xff, 0xfe, b'\n', b'x'],
        ];
        for c in cases {
            assert!(
                s.verify_datagram(&c).is_err(),
                "{:?}",
                String::from_utf8_lossy(&c)
            );
        }
    }

    #[test]
    fn max_counter_is_reserved() {
        let (s, id, secret) = store();
        assert!(sign_datagram(&id, &secret, u64::MAX, b"x").is_err());
        let d = sign_datagram(&id, &secret, u64::MAX - 1, b"x").unwrap();
        s.verify_datagram(&d).unwrap();
        assert!(s.verify_datagram(&d).is_err());
    }

    #[test]
    fn issue_capped_never_outlives_credential() {
        let s = SessionStore::new(3600);
        assert_eq!(s.issue_capped("web", 10).unwrap().2, 10);
        assert_eq!(s.issue_capped("web", 99_999).unwrap().2, 3600);
        assert!(s.issue_capped("web", 0).is_err());
    }

    #[test]
    fn sessions_per_app_are_bounded_and_oldest_evicted() {
        let s = SessionStore::new(3600);
        let (first, _, _) = s.issue_capped("web", 100).unwrap();
        for _ in 0..(MAX_SESSIONS_PER_APP + 5) {
            s.issue("web").unwrap();
        }
        let (other, _, _) = s.issue("api").unwrap();
        let n_web = lock(&s.sessions)
            .values()
            .filter(|t| t.app == "web")
            .count();
        assert_eq!(n_web, MAX_SESSIONS_PER_APP);
        assert!(s.verify_session_token(&first).is_err());
        assert!(s.verify_session_token(&other).is_ok());
    }

    #[test]
    fn zero_ttl_session_is_never_valid() {
        let s = SessionStore::new(0);
        assert!(s.issue("web").is_err());
    }

    #[test]
    fn issue_rejects_invalid_app_and_reports_ttl() {
        let s = SessionStore::new(42);
        for app in ["", "../x", "a/b", "Web"] {
            assert!(s.issue(app).is_err(), "{app:?}");
        }
        assert_eq!(s.issue("ok").unwrap().2, 42);
    }

    #[test]
    fn sessions_are_independent() {
        let s = SessionStore::new(60);
        let (a, sa, _) = s.issue("a").unwrap();
        let (b, sb, _) = s.issue("b").unwrap();
        assert_eq!(
            s.verify_datagram(&sign_datagram(&a, &sa, 1, b"x").unwrap())
                .unwrap()
                .0,
            "a"
        );
        assert_eq!(
            s.verify_datagram(&sign_datagram(&b, &sb, 1, b"x").unwrap())
                .unwrap()
                .0,
            "b"
        );
        assert!(
            s.verify_datagram(&sign_datagram(&a, &sb, 2, b"x").unwrap())
                .is_err()
        );
    }
}
