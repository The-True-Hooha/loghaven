mod defaults;
mod validation;

use crate::error::{LogHavenError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaConfig {
    #[serde(default = "defaults::config_version")]
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "defaults::agent_name")]
    pub name: String,

    #[serde(default = "defaults::log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "defaults::auth_keys_dir")]
    pub keys_dir: PathBuf,

    #[serde(default = "defaults::session_ttl_secs")]
    pub session_ttl_secs: u64,

    #[serde(default = "defaults::query_port")]
    pub query_port: u16,

    /// When false, ingest accepts unauthenticated datagrams (dev mode)
    #[serde(default = "bool_true")]
    pub require_auth: bool,
}

fn bool_true() -> bool {
    true
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            keys_dir: defaults::auth_keys_dir(),
            session_ttl_secs: defaults::session_ttl_secs(),
            query_port: defaults::query_port(),
            require_auth: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "defaults::socket_path")]
    pub socket_path: PathBuf,

    #[serde(default = "defaults::tcp_port")]
    pub tcp_port: u16,

    #[serde(default = "defaults::ingest_port")]
    pub ingest_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "defaults::storage_backend")]
    pub backend: String,

    #[serde(default)]
    pub primary: String,

    pub secondary: Option<String>,

    /// Remove a chunk from local disk once it is uploaded. Queries only read
    /// local chunks, so deleted chunks are no longer searchable.
    #[serde(default)]
    pub delete_after_upload: bool,

    #[serde(default)]
    pub local: LocalStorageConfig,

    pub s3: Option<S3StorageConfig>,
    pub r2: Option<R2StorageConfig>,
    pub minio: Option<MinioStorageConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    #[serde(default = "defaults::local_storage_path")]
    pub path: PathBuf,

    /// Oldest day directories are removed when local data exceeds this. 0 = no limit.
    #[serde(default = "defaults::max_size_gb")]
    pub max_size_gb: u64,

    /// Ignored: chunks are written whole on every flush. Kept so existing
    /// config files still parse.
    #[serde(default = "defaults::rotate_size_mb")]
    pub rotate_size_mb: u64,

    /// Maximum records per chunk file.
    #[serde(default = "defaults::rotate_records")]
    pub rotate_records: u64,

    #[serde(default = "defaults::flush_interval_secs")]
    pub flush_interval_secs: u64,

    /// Day directories older than this are removed. 0 = keep forever.
    #[serde(default = "defaults::retention_days")]
    pub retention_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub region: String,
    pub prefix: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct R2StorageConfig {
    pub account_id: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinioStorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChainsConfig {
    #[serde(default)]
    pub enabled: Vec<String>,

    pub ethereum: Option<EthereumConfig>,
    pub solana: Option<SolanaConfig>,
    pub stellar: Option<StellarConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub ws_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StellarConfig {
    pub horizon_url: String,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            name: defaults::agent_name(),
            log_level: defaults::log_level(),
        }
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            version: defaults::config_version(),
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            socket_path: defaults::socket_path(),
            tcp_port: defaults::tcp_port(),
            ingest_port: defaults::ingest_port(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: defaults::storage_backend(),
            primary: "local".to_string(),
            secondary: None,
            delete_after_upload: false,
            local: LocalStorageConfig::default(),
            s3: None,
            r2: None,
            minio: None,
        }
    }
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            path: defaults::local_storage_path(),
            max_size_gb: defaults::max_size_gb(),
            rotate_size_mb: defaults::rotate_size_mb(),
            rotate_records: defaults::rotate_records(),
            flush_interval_secs: defaults::flush_interval_secs(),
            retention_days: defaults::retention_days(),
        }
    }
}

impl Config {
    pub fn load(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&content)?;

        config.apply_env_overrides()?;

        Ok(config)
    }

    pub fn save(&self, path: &PathBuf) -> Result<()> {
        let toml = toml::to_string_pretty(self)?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(path, toml)?;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        validation::validate(self)
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(backend) = std::env::var("LOGHAVEN_STORAGE_BACKEND") {
            self.storage.backend = backend;
        }

        if let Ok(log_level) = std::env::var("LOGHAVEN_LOG_LEVEL") {
            self.agent.log_level = log_level;
        }

        if let Ok(port) = std::env::var("LOGHAVEN_DAEMON_PORT") {
            self.daemon.tcp_port = port
                .parse()
                .map_err(|_| LogHavenError::Env("Invalid port number".to_string()))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub meta: MetaConfig,

    #[serde(default)]
    pub agent: AgentConfig,

    #[serde(default)]
    pub daemon: DaemonConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub chains: ChainsConfig,

    #[serde(default)]
    pub auth: AuthConfig,
}

pub fn get_config_path(profile: Option<&str>) -> PathBuf {
    let config_dir = dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("loghaven");
    config_path_in(&config_dir, profile)
}

/// Renames a legacy `config_toml` to `config.toml` when only the legacy file
/// exists. Returns the new path if it migrated.
pub fn migrate_legacy_config() -> Result<Option<PathBuf>> {
    let dir = dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("loghaven");
    migrate_legacy_in(&dir)
}

fn migrate_legacy_in(dir: &std::path::Path) -> Result<Option<PathBuf>> {
    let (new, legacy) = (dir.join("config.toml"), dir.join("config_toml"));
    if new.exists() || !legacy.exists() {
        return Ok(None);
    }
    std::fs::rename(&legacy, &new)?;
    Ok(Some(new))
}

fn config_path_in(config_dir: &std::path::Path, profile: Option<&str>) -> PathBuf {
    match profile {
        Some(p) => config_dir.join(format!("{}.toml", p)),
        None => {
            let path = config_dir.join("config.toml");
            // earlier versions wrote "config_toml" by mistake
            let legacy = config_dir.join("config_toml");
            if !path.exists() && legacy.exists() {
                legacy
            } else {
                path
            }
        }
    }
}

pub fn default_config() -> Config {
    Config {
        meta: MetaConfig::default(),
        agent: AgentConfig::default(),
        daemon: DaemonConfig::default(),
        storage: StorageConfig::default(),
        chains: ChainsConfig::default(),
        auth: AuthConfig::default(),
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn default_name_and_legacy_fallback() {
        let dir = std::env::temp_dir().join(format!("lh-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(config_path_in(&dir, None), dir.join("config.toml"));
        assert_eq!(config_path_in(&dir, Some("dev")), dir.join("dev.toml"));
        std::fs::write(dir.join("config_toml"), "").unwrap();
        assert_eq!(config_path_in(&dir, None), dir.join("config_toml"));
        assert_eq!(
            migrate_legacy_in(&dir).unwrap(),
            Some(dir.join("config.toml"))
        );
        assert!(!dir.join("config_toml").exists());
        assert_eq!(config_path_in(&dir, None), dir.join("config.toml"));
        assert_eq!(
            migrate_legacy_in(&dir).unwrap(),
            None,
            "nothing left to migrate"
        );
        std::fs::write(dir.join("config_toml"), "other").unwrap();
        assert_eq!(
            migrate_legacy_in(&dir).unwrap(),
            None,
            "never overwrites config.toml"
        );
        assert!(dir.join("config_toml").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
