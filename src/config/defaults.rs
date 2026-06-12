use std::path::PathBuf;

pub fn config_version() -> u32 {
    1
}

pub fn agent_name() -> String {
    "loghaven-agent".to_string()
}

pub fn log_level() -> String {
    "info".to_string()
}

pub fn socket_path() -> PathBuf {
    let base = dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("loghaven");

    #[cfg(unix)]
    return base.join("loghaven.sock");

    #[cfg(windows)]
    return base.join("loghaven.pipe");
}

pub fn tcp_port() -> u16 {
    9090
}

pub fn ingest_port() -> u16 {
    9100
}

pub fn storage_backend() -> String {
    "auto".to_string()
}

pub fn local_storage_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("loghaven")
        .join("data")
}

pub fn max_size_gb() -> u64 {
    10
}

pub fn rotate_size_mb() -> u64 {
    128
}

pub fn rotate_records() -> u64 {
    500_000
}

pub fn flush_interval_secs() -> u64 {
    30
}

pub fn retention_days() -> u32 {
    30
}

pub fn auth_keys_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("loghaven")
        .join("keys")
}

pub fn session_ttl_secs() -> u64 {
    3600 // 1 hour
}

pub fn query_port() -> u16 {
    9200
}
