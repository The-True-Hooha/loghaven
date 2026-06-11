mod process;
mod server;
mod state;

use crate::config::Config;
use crate::error::{LogHavenError, Result};
use crate::storage::LocalBackend;
use std::sync::Arc;
pub use process::DaemonProcess;
pub use server::DaemonServer;
pub use state::DaemonState;

pub struct Daemon {
    config: Config,
    state: DaemonState,
}

impl Daemon {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            state: DaemonState::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.state.mark_started();

        let app = if self.config.agent.name == "loghaven-agent" {
            return Err(LogHavenError::Config(
                "agent.name must be set to your app name before running".into(),
            ));
        } else {
            self.config.agent.name.clone()
        };

        let storage = Arc::new(
            LocalBackend::new(app, &self.config.storage.local)
                .map_err(|e| LogHavenError::Storage(e.to_string()))?,
        );

        tokio::spawn(crate::storage::pruner::run_pruner(
            self.config.storage.local.path.clone(),
            self.config.storage.local.retention_days,
            self.config.storage.local.max_size_gb,
        ));

        let server = DaemonServer::new(&self.config).await?;

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm = signal(SignalKind::terminate())
                .map_err(|e| LogHavenError::Daemon(format!("signal error: {}", e)))?;
            let mut sigint = signal(SignalKind::interrupt())
                .map_err(|e| LogHavenError::Daemon(format!("signal error: {}", e)))?;

            tokio::select! {
                result = server.listen() => result?,
                _ = sigterm.recv() => {
                    println!("Received SIGTERM, shutting down");
                    server.trigger_shutdown();
                }
                _ = sigint.recv() => {
                    println!("Received SIGINT, shutting down");
                    server.trigger_shutdown();
                }
            }
        }

        #[cfg(windows)]
        {
            tokio::select! {
                result = server.listen() => result?,
                _ = tokio::signal::ctrl_c() => {
                    println!("Received Ctrl+C, shutting down");
                    server.trigger_shutdown();
                }
            }
        }

        storage.shutdown().await?;
        Ok(())
    }
}
