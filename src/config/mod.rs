mod defaults;
mod validation;
pub use validation::{
    is_base58, is_stellar_account, validate_ethereum, validate_solana, validate_stellar,
};

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

    /// Encrypt chunks with AES-256-GCM before they are uploaded. The key is created in
    /// `keys_dir/storage.key` on first use. Local chunks stay readable by queries.
    #[serde(default)]
    pub encrypt_uploads: bool,

    #[serde(default)]
    pub local: LocalStorageConfig,

    pub s3: Option<S3StorageConfig>,
    pub r2: Option<R2StorageConfig>,
    pub minio: Option<MinioStorageConfig>,

    /// Where chunks are uploaded, as a list: `[[storage.targets]]`. Replaces `backend`,
    /// `secondary` and the `[storage.s3|r2|minio]` sections, which stay supported on their own
    /// but cannot be mixed with this list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<StorageTarget>,
}

/// One upload destination.
#[derive(Debug, Clone, Serialize)]
pub struct StorageTarget {
    /// Names the target in the `.synced-<name>` markers of a chunk that is not yet in every
    /// target. Lowercase letters, digits, `-` and `_`, at most 32, unique. Do not change it
    /// while chunks are waiting to be uploaded.
    pub name: String,
    /// Put every object under this prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(flatten)]
    pub kind: TargetKind,
}

/// Credentials come from the config (`x`), from an environment variable (`x_env`, which keeps
/// them out of `config.toml`), or, when neither is given, from the provider's usual ambient
/// sources (`AWS_*`, `AZURE_STORAGE_*`, `GOOGLE_APPLICATION_CREDENTIALS`, instance roles).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum TargetKind {
    /// AWS S3 and anything that speaks the S3 API.
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        /// Another S3-compatible service; unset is AWS.
        #[serde(default)]
        endpoint: Option<String>,
        /// Bucket in the URL path instead of the host name.
        #[serde(default)]
        path_style: bool,
        #[serde(default)]
        access_key: Option<String>,
        #[serde(default)]
        access_key_env: Option<String>,
        #[serde(default)]
        secret_key: Option<String>,
        #[serde(default)]
        secret_key_env: Option<String>,
    },
    /// Azure Blob Storage.
    Azure {
        account: String,
        container: String,
        /// Another endpoint, for a sovereign cloud or the Azurite emulator.
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        access_key: Option<String>,
        #[serde(default)]
        access_key_env: Option<String>,
        /// A SAS token such as `sv=...&sig=...`.
        #[serde(default)]
        sas_token: Option<String>,
        #[serde(default)]
        sas_token_env: Option<String>,
    },
    /// Google Cloud Storage, through its own API.
    Gcs {
        bucket: String,
        /// A service-account JSON file.
        #[serde(default)]
        credentials_file: Option<String>,
    },
}

/// Read by hand so that a misspelt or misplaced key is an error naming the target. With the kind
/// flattened into the struct serde would ignore it, and a typo such as `acess_key_env` would
/// quietly fall back to ambient credentials.
impl<'de> Deserialize<'de> for StorageTarget {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let mut table = toml::Table::deserialize(d)?;
        let mut text = |key: &str| match table.remove(key) {
            None => Ok(None),
            Some(toml::Value::String(s)) => Ok(Some(s)),
            Some(_) => Err(D::Error::custom(format!(
                "storage target: {key} must be a string"
            ))),
        };
        let name =
            text("name")?.ok_or_else(|| D::Error::custom("storage target: name is required"))?;
        let prefix = text("prefix")?;
        let kind = TargetKind::deserialize(toml::Value::Table(table))
            .map_err(|e| D::Error::custom(format!("storage target '{name}': {e}")))?;
        Ok(Self { name, prefix, kind })
    }
}

impl TargetKind {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "s3",
            Self::Azure { .. } => "azure",
            Self::Gcs { .. } => "gcs",
        }
    }
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
    /// Another service that speaks the S3 API (Backblaze B2, Wasabi, DigitalOcean Spaces, Ceph,
    /// Google Cloud Storage's interoperability endpoint, ...). Unset means AWS.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Address the bucket in the URL path (`host/bucket/key`) instead of the host name. Most
    /// self-hosted servers need this; AWS and hosted services do not.
    #[serde(default)]
    pub path_style: bool,
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
    /// Default `us-east-1`, which MinIO accepts.
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    /// Default true, which MinIO needs.
    #[serde(default)]
    pub path_style: Option<bool>,
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
    /// Optional WebSocket endpoint (`ws://` or `wss://`) of the same chain. New blocks announced
    /// there start a poll at once; reading still goes over `rpc_url`.
    pub ws_url: Option<String>,
    /// App the chain events are recorded under.
    #[serde(default = "defaults::ethereum_app")]
    pub app: String,
    /// Contract addresses to watch (`0x` + 40 hex). At least one is required.
    #[serde(default)]
    pub addresses: Vec<String>,
    /// Optional event signature hashes (topic0, `0x` + 64 hex); any of them matches.
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default = "defaults::ethereum_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Only blocks this many behind the head are read, as protection against reorgs.
    #[serde(default = "defaults::ethereum_confirmations")]
    pub confirmations: u64,
    /// Largest block span of one `eth_getLogs` call.
    #[serde(default = "defaults::ethereum_max_block_range")]
    pub max_block_range: u64,
    /// First block of the first run; defaults to the newest confirmed block.
    pub start_block: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaConfig {
    pub rpc_url: String,
    /// Not used yet: the adapter polls `rpc_url` over HTTP.
    pub ws_url: Option<String>,
    /// App the chain events are recorded under.
    #[serde(default = "defaults::solana_app")]
    pub app: String,
    /// Account or program addresses (base58) whose transactions are recorded. At least one is required.
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default = "defaults::solana_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Most transactions recorded per address in one poll; a larger backlog takes several polls.
    #[serde(default = "defaults::solana_max_signatures")]
    pub max_signatures: u64,
    /// On the first run, record this many of each address's newest transactions instead of starting at the tip.
    #[serde(default)]
    pub backfill: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StellarConfig {
    pub horizon_url: String,
    /// App the transactions are recorded under.
    #[serde(default = "defaults::stellar_app")]
    pub app: String,
    /// Account ids (`G...`, 56 characters) whose transactions are recorded. At least one is required.
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default = "defaults::stellar_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Most transactions recorded per account in one poll; a larger backlog takes several polls.
    #[serde(default = "defaults::stellar_max_records")]
    pub max_records: u64,
    /// On the first run, record this many of each account's newest transactions instead of starting at the tip.
    #[serde(default)]
    pub backfill: u64,
    /// Also record each transaction's operation types; costs one more Horizon request per transaction.
    #[serde(default)]
    pub operations: bool,
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
            encrypt_uploads: false,
            local: LocalStorageConfig::default(),
            s3: None,
            r2: None,
            minio: None,
            targets: vec![],
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
    if !legacy.exists() {
        return Ok(None);
    }
    if new.exists() {
        eprintln!(
            "Warning: both {} and {} exist; {} is used and the old file is ignored",
            new.display(),
            legacy.display(),
            new.display()
        );
        return Ok(None);
    }
    std::fs::rename(&legacy, &new).map_err(|e| {
        LogHavenError::Config(format!(
            "cannot rename {} to {}: {}",
            legacy.display(),
            new.display(),
            e
        ))
    })?;
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
