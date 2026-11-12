use super::Config;
use crate::error::{LogHavenError, Result};

/// One listing request must hold the backfill plus the transaction the cursor sits on.
const MAX_BACKFILL: u64 = 100;

pub fn validate(config: &Config) -> Result<()> {
    if !config.storage.targets.is_empty() {
        validate_targets(config)?;
    } else {
        validate_legacy_backend(config)?;
    }
    validate_rest(config)
}

fn validate_legacy_backend(config: &Config) -> Result<()> {
    let st = &config.storage;
    for (section, prefix) in [
        (
            "storage.s3",
            st.s3.as_ref().and_then(|c| c.prefix.as_deref()),
        ),
        (
            "storage.r2",
            st.r2.as_ref().and_then(|c| c.prefix.as_deref()),
        ),
        (
            "storage.minio",
            st.minio.as_ref().and_then(|c| c.prefix.as_deref()),
        ),
    ] {
        if let Some(p) = prefix {
            check_prefix(section, p)?;
        }
    }
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

    Ok(())
}

fn validate_rest(config: &Config) -> Result<()> {
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

const MAX_NAME: usize = 32;

/// A prefix is a path of plain segments: nothing empty, `.` or `..` and no control characters,
/// or every upload would fail the object-key check.
pub fn check_prefix(at: &str, prefix: &str) -> Result<()> {
    let ok = prefix
        .trim_matches('/')
        .split('/')
        .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
        && !prefix.contains('\\')
        && !prefix.chars().any(char::is_control);
    if prefix.trim_matches('/').is_empty() || ok {
        Ok(())
    } else {
        Err(LogHavenError::Config(format!(
            "{at}: prefix must be plain path segments, with no empty, '.' or '..' segment, backslash or control character"
        )))
    }
}

fn validate_targets(config: &Config) -> Result<()> {
    let bad = |m: String| Err(LogHavenError::Config(m));
    let st = &config.storage;
    if st.secondary.is_some()
        || st.s3.is_some()
        || st.r2.is_some()
        || st.minio.is_some()
        || !matches!(st.backend.as_str(), "auto" | "local")
    {
        return bad("[[storage.targets]] cannot be combined with backend, secondary or the [storage.s3|r2|minio] sections; move them into the list".into());
    }
    let mut seen = std::collections::HashSet::new();
    for (i, t) in st.targets.iter().enumerate() {
        let at = format!("storage.targets[{i}]");
        if t.name.is_empty()
            || t.name.len() > MAX_NAME
            || !t
                .name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        {
            return bad(format!(
                "{at}: name must be 1 to {MAX_NAME} characters of a-z, 0-9, '-' and '_'"
            ));
        }
        if !seen.insert(t.name.as_str()) {
            return bad(format!("{at}: name '{}' is used twice", t.name));
        }
        if let Some(p) = &t.prefix {
            check_prefix(&at, p)?;
        }
        match &t.kind {
            super::TargetKind::S3 {
                bucket,
                endpoint,
                path_style,
                access_key,
                access_key_env,
                secret_key,
                secret_key_env,
                ..
            } => {
                if bucket.is_empty() {
                    return bad(format!("{at}: bucket is required"));
                }
                if let Some(e) = endpoint {
                    check_endpoint(&format!("{at}.endpoint"), e)?;
                    if !*path_style && !crate::storage::host_takes_a_bucket_prefix(e) {
                        return bad(format!(
                            "{at}: this endpoint is an IP address or a bare host name, which cannot carry the bucket in the host: set path_style = true"
                        ));
                    }
                }
                one_source(&at, "access_key", access_key, access_key_env)?;
                one_source(&at, "secret_key", secret_key, secret_key_env)?;
                if access_key.is_some() != secret_key.is_some()
                    || access_key_env.is_some() != secret_key_env.is_some()
                    || (access_key.is_some() && access_key_env.is_some())
                {
                    return bad(format!(
                        "{at}: give access_key and secret_key together, both plain or both as _env"
                    ));
                }
            }
            super::TargetKind::Azure {
                account,
                container,
                endpoint,
                access_key,
                access_key_env,
                sas_token,
                sas_token_env,
            } => {
                if account.is_empty() || container.is_empty() {
                    return bad(format!("{at}: account and container are required"));
                }
                if let Some(e) = endpoint {
                    check_endpoint(&format!("{at}.endpoint"), e)?;
                }
                one_source(&at, "access_key", access_key, access_key_env)?;
                one_source(&at, "sas_token", sas_token, sas_token_env)?;
                if (access_key.is_some() || access_key_env.is_some())
                    && (sas_token.is_some() || sas_token_env.is_some())
                {
                    return bad(format!("{at}: use an access key or a SAS token, not both"));
                }
            }
            super::TargetKind::Gcs { bucket, .. } => {
                if bucket.is_empty() {
                    return bad(format!("{at}: bucket is required"));
                }
            }
        }
    }
    Ok(())
}

/// A secret is given in the config or as the name of an environment variable, not both.
fn one_source(at: &str, what: &str, plain: &Option<String>, env: &Option<String>) -> Result<()> {
    if plain.is_some() && env.is_some() {
        return Err(LogHavenError::Config(format!(
            "{at}: give {what} or {what}_env, not both"
        )));
    }
    if env.as_deref().is_some_and(|e| e.trim().is_empty()) {
        return Err(LogHavenError::Config(format!("{at}: {what}_env is empty")));
    }
    if plain.as_deref().is_some_and(|p| p.trim().is_empty()) {
        return Err(LogHavenError::Config(format!("{at}: {what} is empty")));
    }
    Ok(())
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
