use super::Config;
use crate::error::{LogHavenError, Result};

/// One listing request must hold the backfill plus the transaction the cursor sits on.
const MAX_BACKFILL: u64 = 100;

pub fn validate(config: &Config) -> Result<()> {
    match config.storage.backend.as_str() {
        "auto" => {}
        "local" => validate_local_storage(config)?,
        "s3" => validate_s3_storage(config)?,
        "r2" => validate_r2_storage(config)?,
        "minio" => validate_minio_storage(config)?,
        backend => {
            return Err(LogHavenError::Config(format!(
                "Unknown storage backend: {}. Valid: auto, local, s3, r2, minio",
                backend
            )));
        }
    }

    match config.agent.log_level.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => {}
        level => {
            return Err(LogHavenError::Config(format!(
                "Invalid log level: {}. Must be one of: trace, debug, info, warn, error",
                level
            )));
        }
    }

    for chain in &config.chains.enabled {
        match chain.as_str() {
            "ethereum" => match &config.chains.ethereum {
                None => {
                    return Err(LogHavenError::Config(
                        "Ethereum enabled but no config provided".to_string(),
                    ));
                }
                Some(eth) => validate_ethereum(eth)?,
            },
            "solana" => match &config.chains.solana {
                None => {
                    return Err(LogHavenError::Config(
                        "Solana enabled but no config provided".to_string(),
                    ));
                }
                Some(sol) => validate_solana(sol)?,
            },
            "stellar" => match &config.chains.stellar {
                None => {
                    return Err(LogHavenError::Config(
                        "Stellar enabled but no config provided".to_string(),
                    ));
                }
                Some(xlm) => validate_stellar(xlm)?,
            },
            unknown => {
                return Err(LogHavenError::Config(format!("Unknown chain: {}", unknown)));
            }
        }
    }

    Ok(())
}

fn validate_local_storage(config: &Config) -> Result<()> {
    let path = &config.storage.local.path;

    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent)?;
    }

    Ok(())
}

/// The URL itself is never printed: it may hold credentials.
fn check_endpoint(what: &str, url: &str) -> Result<()> {
    if (url.starts_with("http://") || url.starts_with("https://")) && url.len() > 8 {
        Ok(())
    } else {
        Err(LogHavenError::Config(format!(
            "{what} must be an http:// or https:// URL"
        )))
    }
}

fn validate_s3_storage(config: &Config) -> Result<()> {
    let s3 = config.storage.s3.as_ref().ok_or_else(|| {
        LogHavenError::Config("S3 backend selected but no [storage.s3] config provided".into())
    })?;
    if s3.access_key.is_none() || s3.secret_key.is_none() {
        return Err(LogHavenError::Config(
            "S3 config requires access_key and secret_key".into(),
        ));
    }
    if let Some(endpoint) = &s3.endpoint {
        check_endpoint("storage.s3.endpoint", endpoint)?;
    }
    Ok(())
}

fn validate_r2_storage(config: &Config) -> Result<()> {
    let r2 = config.storage.r2.as_ref().ok_or_else(|| {
        LogHavenError::Config("R2 backend selected but no [storage.r2] config provided".into())
    })?;
    if r2.access_key.is_none() || r2.secret_key.is_none() {
        return Err(LogHavenError::Config(
            "R2 config requires access_key and secret_key".into(),
        ));
    }
    Ok(())
}

fn validate_minio_storage(config: &Config) -> Result<()> {
    let minio = config.storage.minio.as_ref().ok_or_else(|| {
        LogHavenError::Config(
            "MinIO backend selected but no [storage.minio] config provided".into(),
        )
    })?;
    if minio.access_key.is_empty() || minio.secret_key.is_empty() {
        return Err(LogHavenError::Config(
            "MinIO config requires access_key and secret_key".into(),
        ));
    }
    check_endpoint("storage.minio.endpoint", &minio.endpoint)?;
    Ok(())
}

fn is_hex_with_len(s: &str, hex_len: usize) -> bool {
    s.strip_prefix("0x")
        .is_some_and(|h| h.len() == hex_len && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Checked when the chain is enabled and again by `loghaven watch ethereum`.
pub fn validate_ethereum(eth: &super::EthereumConfig) -> Result<()> {
    let bad = |msg: String| Err(LogHavenError::Config(format!("chains.ethereum: {msg}")));
    if !(eth.rpc_url.starts_with("http://") || eth.rpc_url.starts_with("https://")) {
        return bad("rpc_url must start with http:// or https://".into());
    }
    if let Some(ws) = &eth.ws_url
        && !(ws.starts_with("ws://") || ws.starts_with("wss://"))
    {
        return bad("ws_url must start with ws:// or wss://".into());
    }
    crate::storage::validate_app_name(&eth.app)?;
    if eth.addresses.is_empty() {
        return bad("addresses must list at least one contract address".into());
    }
    if let Some(a) = eth.addresses.iter().find(|a| !is_hex_with_len(a, 40)) {
        return bad(format!("'{a}' is not a 0x-prefixed 20-byte address"));
    }
    if let Some(t) = eth.topics.iter().find(|t| !is_hex_with_len(t, 64)) {
        return bad(format!("'{t}' is not a 0x-prefixed 32-byte topic"));
    }
    if eth.poll_interval_secs == 0 {
        return bad("poll_interval_secs must be at least 1".into());
    }
    if !(1..=10_000).contains(&eth.max_block_range) {
        return bad("max_block_range must be between 1 and 10000".into());
    }
    Ok(())
}

/// Base58 text of a 32-byte address is 32 to 44 characters.
pub fn is_base58(s: &str, min_len: usize, max_len: usize) -> bool {
    const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    (min_len..=max_len).contains(&s.len()) && s.chars().all(|c| ALPHABET.contains(c))
}

/// Checked when the chain is enabled and again by `loghaven watch solana`.
pub fn validate_solana(sol: &super::SolanaConfig) -> Result<()> {
    let bad = |msg: String| Err(LogHavenError::Config(format!("chains.solana: {msg}")));
    if !(sol.rpc_url.starts_with("http://") || sol.rpc_url.starts_with("https://")) {
        return bad("rpc_url must start with http:// or https://".into());
    }
    crate::storage::validate_app_name(&sol.app)?;
    if sol.addresses.is_empty() {
        return bad("addresses must list at least one account or program address".into());
    }
    if let Some(a) = sol.addresses.iter().find(|a| !is_base58(a, 32, 44)) {
        return bad(format!("'{a}' is not a base58 address"));
    }
    if sol.poll_interval_secs == 0 {
        return bad("poll_interval_secs must be at least 1".into());
    }
    if !(1..=1_000).contains(&sol.max_signatures) {
        return bad("max_signatures must be between 1 and 1000".into());
    }
    if sol.backfill > MAX_BACKFILL {
        return bad(format!("backfill must be at most {MAX_BACKFILL}"));
    }
    Ok(())
}

/// A Stellar account id: `G` and 55 base32 characters.
pub fn is_stellar_account(s: &str) -> bool {
    s.len() == 56
        && s.starts_with('G')
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b))
}

/// Checked when the chain is enabled and again by `loghaven watch stellar`.
pub fn validate_stellar(xlm: &super::StellarConfig) -> Result<()> {
    let bad = |msg: String| Err(LogHavenError::Config(format!("chains.stellar: {msg}")));
    if !(xlm.horizon_url.starts_with("http://") || xlm.horizon_url.starts_with("https://")) {
        return bad("horizon_url must start with http:// or https://".into());
    }
    crate::storage::validate_app_name(&xlm.app)?;
    if xlm.accounts.is_empty() {
        return bad("accounts must list at least one account id".into());
    }
    if let Some(a) = xlm.accounts.iter().find(|a| !is_stellar_account(a)) {
        return bad(format!(
            "'{a}' is not a Stellar account id (G..., 56 characters)"
        ));
    }
    if xlm.poll_interval_secs == 0 {
        return bad("poll_interval_secs must be at least 1".into());
    }
    if !(1..=1_000).contains(&xlm.max_records) {
        return bad("max_records must be between 1 and 1000".into());
    }
    if xlm.backfill > MAX_BACKFILL {
        return bad(format!("backfill must be at most {MAX_BACKFILL}"));
    }
    Ok(())
}
