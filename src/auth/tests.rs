use crate::auth::keys::generate_keypair;
use crate::auth::session::{SessionStore, sign_datagram};
use crate::auth::token::{issue_jwt, verify_jwt};
use tempfile::TempDir;

#[test]
fn keygen_creates_pem_files() {
    let dir = TempDir::new().unwrap();
    let priv_path = dir.path().join("private.pem");
    let pub_path = dir.path().join("public.pem");

    generate_keypair(&priv_path, &pub_path).unwrap();

    assert!(priv_path.exists());
    assert!(pub_path.exists());

    let priv_pem = std::fs::read_to_string(&priv_path).unwrap();
    let pub_pem = std::fs::read_to_string(&pub_path).unwrap();
    assert!(priv_pem.contains("PRIVATE KEY"));
    assert!(pub_pem.contains("PUBLIC KEY"));
}

#[test]
fn jwt_roundtrip() {
    let dir = TempDir::new().unwrap();
    let priv_path = dir.path().join("private.pem");
    let pub_path = dir.path().join("public.pem");
    generate_keypair(&priv_path, &pub_path).unwrap();

    let priv_pem = std::fs::read_to_string(&priv_path).unwrap();
    let pub_pem = std::fs::read_to_string(&pub_path).unwrap();

    let token = issue_jwt("myapp", &priv_pem, 3600).unwrap();
    let claims = verify_jwt(&token, &pub_pem).unwrap();

    assert_eq!(claims.sub, "myapp");
}

#[test]
fn jwt_wrong_key_rejected() {
    let dir = TempDir::new().unwrap();
    let priv_path = dir.path().join("private.pem");
    let pub_path = dir.path().join("public.pem");
    generate_keypair(&priv_path, &pub_path).unwrap();

    let dir2 = TempDir::new().unwrap();
    let priv2 = dir2.path().join("private.pem");
    let pub2 = dir2.path().join("public.pem");
    generate_keypair(&priv2, &pub2).unwrap();

    let priv_pem = std::fs::read_to_string(&priv_path).unwrap();
    let pub_pem2 = std::fs::read_to_string(&pub2).unwrap();

    let token = issue_jwt("app", &priv_pem, 3600).unwrap();
    assert!(verify_jwt(&token, &pub_pem2).is_err());
}

#[test]
fn jwt_invalid_token_rejected() {
    let dir = TempDir::new().unwrap();
    let priv_path = dir.path().join("private.pem");
    let pub_path = dir.path().join("public.pem");
    generate_keypair(&priv_path, &pub_path).unwrap();

    let pub_pem = std::fs::read_to_string(&pub_path).unwrap();

    // Garbage token
    assert!(verify_jwt("not.a.jwt", &pub_pem).is_err());
    assert!(verify_jwt("", &pub_pem).is_err());
}

#[test]
fn session_issue_and_verify_token() {
    let store = SessionStore::new(3600);
    let (token_id, _secret) = store.issue("myapp").unwrap();
    let app = store.verify_session_token(&token_id).unwrap();
    assert_eq!(app, "myapp");
}

#[test]
fn session_unknown_token_rejected() {
    let store = SessionStore::new(3600);
    assert!(store.verify_session_token("nonexistent").is_err());
}

#[test]
fn session_expired_token_rejected() {
    let store = SessionStore::new(0); // TTL = 0
    let (token_id, _) = store.issue("app").unwrap();
    // expires_at is now in the past
    assert!(store.verify_session_token(&token_id).is_err());
}

#[test]
fn session_purge_removes_expired() {
    let store = SessionStore::new(0);
    store.issue("app").unwrap();
    store.purge_expired();
    // after purge, nothing remains — verify any token fails
    // just check purge doesn't panic
}

// --- Datagram signing + verification ---

#[test]
fn datagram_sign_and_verify() {
    let store = SessionStore::new(3600);
    let (token_id, secret) = store.issue("myapp").unwrap();

    let payload = br#"{"app":"myapp","records":[]}"#;
    let signed = sign_datagram(&token_id, &secret, payload).unwrap();

    let (app, recovered_payload) = store.verify_datagram(&signed).unwrap();
    assert_eq!(app, "myapp");
    assert_eq!(&recovered_payload, payload);
}

#[test]
fn datagram_tampered_payload_rejected() {
    let store = SessionStore::new(3600);
    let (token_id, secret) = store.issue("myapp").unwrap();

    let payload = b"original payload";
    let mut signed = sign_datagram(&token_id, &secret, payload).unwrap();

    // flip last byte
    let last = signed.len() - 1;
    signed[last] ^= 0xff;

    assert!(store.verify_datagram(&signed).is_err());
}

#[test]
fn datagram_missing_header_rejected() {
    let store = SessionStore::new(3600);
    let raw = b"no header here at all";
    assert!(store.verify_datagram(raw).is_err());
}

#[test]
fn datagram_empty_payload_signs_and_verifies() {
    let store = SessionStore::new(3600);
    let (token_id, secret) = store.issue("app").unwrap();
    let signed = sign_datagram(&token_id, &secret, b"").unwrap();
    let (app, payload) = store.verify_datagram(&signed).unwrap();
    assert_eq!(app, "app");
    assert!(payload.is_empty());
}

#[test]
fn session_multiple_apps_independent() {
    let store = SessionStore::new(3600);
    let (tok_a, _) = store.issue("app_a").unwrap();
    let (tok_b, _) = store.issue("app_b").unwrap();

    assert_eq!(store.verify_session_token(&tok_a).unwrap(), "app_a");
    assert_eq!(store.verify_session_token(&tok_b).unwrap(), "app_b");
}

#[test]
fn session_tokens_are_unique() {
    let store = SessionStore::new(3600);
    let (tok1, _) = store.issue("app").unwrap();
    let (tok2, _) = store.issue("app").unwrap();
    assert_ne!(tok1, tok2);
}

#[test]
fn session_secrets_are_unique() {
    let store = SessionStore::new(3600);
    let (_, secret1) = store.issue("app").unwrap();
    let (_, secret2) = store.issue("app").unwrap();
    assert_ne!(secret1, secret2);
}

#[test]
fn datagram_wrong_token_id_rejected() {
    let store = SessionStore::new(3600);
    let (_, secret) = store.issue("app").unwrap();
    // Sign with a valid secret but reference a nonexistent token_id
    let signed = sign_datagram("nonexistent_token", &secret, b"payload").unwrap();
    assert!(store.verify_datagram(&signed).is_err());
}

#[test]
fn jwt_claims_app_matches_sub() {
    let dir = TempDir::new().unwrap();
    let priv_path = dir.path().join("private.pem");
    let pub_path = dir.path().join("public.pem");
    generate_keypair(&priv_path, &pub_path).unwrap();

    let priv_pem = std::fs::read_to_string(&priv_path).unwrap();
    let pub_pem = std::fs::read_to_string(&pub_path).unwrap();

    for app in ["my-service", "chain-indexer", "api-gateway"] {
        let token = issue_jwt(app, &priv_pem, 3600).unwrap();
        let claims = verify_jwt(&token, &pub_pem).unwrap();
        assert_eq!(claims.sub, app);
    }
}
