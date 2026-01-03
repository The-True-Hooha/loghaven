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

pub fn storage_backend() -> String {
    "local".to_string()
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
