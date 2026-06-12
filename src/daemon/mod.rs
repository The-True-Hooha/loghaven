mod process;
mod server;
mod state;

use crate::auth::session::SessionStore;
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use crate::ingest::IngestServer;
use crate::query::QueryServer;
use crate::storage::StorageRouter;
pub use process::DaemonProcess;
pub use server::DaemonServer;
pub use state::DaemonState;
use std::sync::Arc;

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

        let storage = Arc::new(
            StorageRouter::new(&self.config).map_err(|e| LogHavenError::Storage(e.to_string()))?,
        );

        let sessions = Arc::new(SessionStore::new(self.config.auth.session_ttl_secs));

        // Purge expired sessions hourly
        {
            let sessions_clone = Arc::clone(&sessions);
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(tokio::time::Duration::from_secs(3600));
                loop {
                    ticker.tick().await;
                    sessions_clone.purge_expired();
                }
            });
        }

        tokio::spawn(crate::storage::pruner::run_pruner(
            self.config.storage.local.path.clone(),
            self.config.storage.local.retention_days,
            self.config.storage.local.max_size_gb,
        ));

        let ingest = IngestServer::new(
            self.config.daemon.ingest_port,
            Arc::clone(&storage),
            Arc::clone(&sessions),
            self.config.auth.require_auth,
        );
        tokio::spawn(async move {
            if let Err(e) = ingest.listen().await {
                eprintln!("Ingest server error: {}", e);
            }
        });

        let query_server = QueryServer::new(&self.config, Arc::clone(&sessions));
        tokio::spawn(async move {
            if let Err(e) = query_server.listen().await {
                eprintln!("Query server error: {}", e);
            }
        });

        let ipc = DaemonServer::new(&self.config, Arc::clone(&sessions)).await?;

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let mut sigterm = signal(SignalKind::terminate())
                .map_err(|e| LogHavenError::Daemon(format!("signal error: {}", e)))?;
            let mut sigint = signal(SignalKind::interrupt())
                .map_err(|e| LogHavenError::Daemon(format!("signal error: {}", e)))?;

            tokio::select! {
                result = ipc.listen() => result?,
                _ = sigterm.recv() => {
                    println!("Received SIGTERM, shutting down");
                    ipc.trigger_shutdown();
                }
                _ = sigint.recv() => {
                    println!("Received SIGINT, shutting down");
                    ipc.trigger_shutdown();
                }
            }
        }

        #[cfg(windows)]
        {
            tokio::select! {
                result = ipc.listen() => result?,
                _ = tokio::signal::ctrl_c() => {
                    println!("Received Ctrl+C, shutting down");
                    ipc.trigger_shutdown();
                }
            }
        }

        storage.shutdown_all().await?;
        Ok(())
    }
}
