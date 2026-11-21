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
    /// Fetches chunks that are only in a storage target; off unless `query.remote_target` is set.
    pub remote: Option<Arc<crate::storage::remote::RemoteReader>>,
}

/// The cache sits beside the data directory, not in it, so nothing that scans the data
/// directory (the uploader, the pruner, a query) meets it.
fn remote_reader(config: &Config) -> Result<Option<Arc<crate::storage::remote::RemoteReader>>> {
    let Some(name) = &config.query.remote_target else {
        return Ok(None);
    };
    let targets = crate::storage::resolve_targets(config)?;
    let target = targets.iter().find(|t| &t.name == name).ok_or_else(|| {
        LogHavenError::Config(format!("query.remote_target '{name}' is not a target"))
    })?;
    let data = &config.storage.local.path;
    let mut cache = data.clone().into_os_string();
    cache.push("-remote-cache");
    Ok(Some(Arc::new(crate::storage::remote::RemoteReader::new(
        crate::storage::cloud::Target::from_config(target)?,
        config.auth.keys_dir.clone(),
        cache.into(),
        &config.query,
    ))))
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
    pub fn new(config: &Config, sessions: Arc<SessionStore>) -> Result<Self> {
        let remote = remote_reader(config)?;
        let public_key_pem = {
            let pub_path = config.auth.keys_dir.join("public.pem");
            std::fs::read_to_string(&pub_path).ok()
        };

        Ok(Self {
            port: config.auth.query_port,
            state: QueryState {
                sessions,
                public_key_pem,
                data_path: config.storage.local.path.clone(),
                require_auth: config.auth.require_auth,
                query_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_QUERIES)),
                remote,
            },
        })
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
