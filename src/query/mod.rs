mod dashboard;
mod handlers;

use crate::auth::session::SessionStore;
use crate::config::Config;
use crate::error::{LogHavenError, Result};
use axum::Router;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone)]
pub struct QueryState {
    pub sessions: Arc<SessionStore>,
    pub public_key_pem: Option<String>,
    pub data_path: std::path::PathBuf,
    pub require_auth: bool,
    /// Bounds concurrent scans; each holds a blocking thread and reads whole chunks.
    pub query_slots: Arc<tokio::sync::Semaphore>,
}

fn router(state: QueryState) -> Router {
    Router::new()
        .merge(handlers::routes())
        .merge(dashboard::routes())
        .layer(axum::middleware::from_fn(dashboard::local_host_only))
        .with_state(state)
}

pub const MAX_CONCURRENT_QUERIES: usize = 4;

pub struct QueryServer {
    port: u16,
    state: QueryState,
}

impl QueryServer {
    pub fn new(config: &Config, sessions: Arc<SessionStore>) -> Self {
        let public_key_pem = {
            let pub_path = config.auth.keys_dir.join("public.pem");
            std::fs::read_to_string(&pub_path).ok()
        };

        Self {
            port: config.auth.query_port,
            state: QueryState {
                sessions,
                public_key_pem,
                data_path: config.storage.local.path.clone(),
                require_auth: config.auth.require_auth,
                query_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES)),
            },
        }
    }

    pub async fn listen(&self) -> Result<()> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            LogHavenError::Daemon(format!("query server bind failed on {}: {}", addr, e))
        })?;

        println!("Query server listening on http://{}", addr);

        axum::serve(listener, router(self.state.clone()))
            .await
            .map_err(|e| LogHavenError::Daemon(format!("query server error: {}", e)))?;

        Ok(())
    }
}
