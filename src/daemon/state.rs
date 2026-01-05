use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonState {
    pub pid: u32,
    pub started_at: u64,
    pub version: String,
    pub status: DaemonStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DaemonStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
}

impl DaemonState {
    pub fn new() -> Self {
        Self {
            pid: std::process::id(),
            started_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            status: DaemonStatus::Starting,
        }
    }

    pub fn mark_started(&mut self) {
        self.status = DaemonStatus::Running
    }

    pub fn mark_stopping(&mut self) {
        self.status = DaemonStatus::Stopping
    }

    pub fn uptime(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now - self.started_at
    }
}
