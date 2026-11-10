//! Background fetching for the terminal UI: polls each app's new records with the
//! same lookback logic as `logs --follow`, and answers trace requests.

use super::model::{Msg, Request};
use crate::client::QueryToken;
use crate::client::fetch_logs;
use crate::client::follow::FollowState;
use crate::config::Config;
use crate::storage::record::LogRecord;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const POLL: Duration = Duration::from_secs(1);
/// Records loaded per app when the UI opens.
const INITIAL: usize = 200;
const PAGE: usize = 1000;
const TRACE_LIMIT: usize = 1000;
const SESSION_TTL_SECS: u64 = 3600;

struct AppFeed {
    app: String,
    token: QueryToken,
    state: FollowState,
    started: bool,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn params(app: &str, limit: usize, from_ms: Option<i64>) -> Vec<(&'static str, String)> {
    let mut p = vec![("app", app.to_string()), ("limit", limit.to_string())];
    if let Some(from) = from_ms {
        p.push(("from_ms", from.to_string()));
    }
    p
}

impl AppFeed {
    async fn poll(&mut self, port: u16) -> crate::error::Result<Vec<LogRecord>> {
        let (limit, from) = if self.started {
            (PAGE, Some(self.state.next_from_ms()))
        } else {
            (INITIAL, None)
        };
        let token = self.token.get().await?;
        let batch = fetch_logs(port, &params(&self.app, limit, from), token.as_deref()).await?;
        self.started = true;
        Ok(self.state.accept(batch))
    }
}

async fn trace(
    cfg: &Config,
    apps: &[String],
    explicit: &Option<String>,
    hash: &str,
) -> Result<Vec<LogRecord>, String> {
    let mut all = vec![];
    let mut errors = vec![];
    for app in apps {
        let mut token = QueryToken::new(cfg, app, explicit.clone(), SESSION_TTL_SECS);
        let mut p = params(app, TRACE_LIMIT, None);
        p.push(("tx_hash", hash.to_string()));
        let got = async {
            let t = token.get().await?;
            fetch_logs(cfg.auth.query_port, &p, t.as_deref()).await
        }
        .await;
        match got {
            Ok(r) => all.extend(r),
            Err(e) => errors.push(format!("{app}: {e}")),
        }
    }
    if all.is_empty() && !errors.is_empty() {
        return Err(errors.join("; "));
    }
    Ok(all)
}

pub async fn run(
    cfg: Config,
    apps: Vec<String>,
    explicit: Option<String>,
    tx: UnboundedSender<Msg>,
    mut requests: UnboundedReceiver<Request>,
) {
    let mut feeds: Vec<AppFeed> = apps
        .iter()
        .map(|app| AppFeed {
            app: app.clone(),
            token: QueryToken::new(&cfg, app, explicit.clone(), SESSION_TTL_SECS),
            state: FollowState::new(
                now_ms(),
                (cfg.storage.local.flush_interval_secs as i64 * 2 + 5) * 1000,
            ),
            started: false,
        })
        .collect();
    let mut last_status = String::new();
    let mut tick = tokio::time::interval(POLL);
    loop {
        tokio::select! {
            req = requests.recv() => {
                let Some(Request::Trace(hash)) = req else { return };
                let result = trace(&cfg, &apps, &explicit, &hash).await;
                if tx.send(Msg::Trace(hash, result)).is_err() {
                    return;
                }
            }
            _ = tick.tick() => {
                let mut fresh = vec![];
                let mut errors = vec![];
                for feed in &mut feeds {
                    match feed.poll(cfg.auth.query_port).await {
                        Ok(r) => fresh.extend(r),
                        Err(e) => errors.push(format!("{}: {e}", feed.app)),
                    }
                }
                if !fresh.is_empty() && tx.send(Msg::Records(fresh)).is_err() {
                    return;
                }
                let status = errors.join("; ");
                if status != last_status {
                    last_status = status.clone();
                    if tx.send(Msg::Status(status)).is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
