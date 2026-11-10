//! Background fetching for the terminal UI: polls each app's new records with the
//! same lookback logic as `logs --follow`, and answers trace requests.

use super::model::{Msg, Request};
use crate::client::QueryToken;
use crate::client::fetch_logs;
use crate::client::follow::FollowState;
use crate::config::Config;
use crate::storage::record::LogRecord;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::MissedTickBehavior;

const POLL: Duration = Duration::from_secs(1);
/// Records loaded per app when the UI opens.
const INITIAL: usize = 200;
const PAGE: usize = 1000;
const MAX_PAGE: usize = 10_000;
const TRACE_LIMIT: usize = 1000;
const SESSION_TTL_SECS: u64 = 3600;

type SharedToken = Arc<Mutex<QueryToken>>;

struct AppFeed {
    app: String,
    token: SharedToken,
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

async fn fetch(
    port: u16,
    token: &SharedToken,
    p: &[(&'static str, String)],
) -> crate::error::Result<Vec<LogRecord>> {
    let t = token.lock().await.get().await?;
    fetch_logs(port, p, t.as_deref()).await
}

impl AppFeed {
    /// New records, and a warning when a page was full even at the largest size.
    async fn poll(&mut self, port: u16) -> crate::error::Result<(Vec<LogRecord>, Option<String>)> {
        let (limit, from) = if self.started {
            (PAGE, Some(self.state.next_from_ms()))
        } else {
            (INITIAL, None)
        };
        let mut batch = fetch(port, &self.token, &params(&self.app, limit, from)).await?;
        let mut warning = None;
        // like `logs --follow`: a full page may hide older records, so ask for more once
        if self.started && batch.len() >= PAGE {
            batch = fetch(port, &self.token, &params(&self.app, MAX_PAGE, from)).await?;
            if batch.len() >= MAX_PAGE {
                warning = Some(format!(
                    "{}: more than {MAX_PAGE} records in one poll; some may be missing",
                    self.app
                ));
            }
        }
        self.started = true;
        Ok((self.state.accept(batch), warning))
    }
}

async fn trace(
    port: u16,
    tokens: Vec<(String, SharedToken)>,
    hash: String,
) -> Result<(Vec<LogRecord>, Option<String>), String> {
    let mut all = vec![];
    let mut errors = vec![];
    let mut cut = vec![];
    for (app, token) in &tokens {
        let mut p = params(app, TRACE_LIMIT, None);
        p.push(("tx_hash", hash.clone()));
        match fetch(port, token, &p).await {
            Ok(r) => {
                if r.len() >= TRACE_LIMIT {
                    cut.push(app.clone());
                }
                all.extend(r);
            }
            Err(e) => errors.push(format!("{app}: {e}")),
        }
    }
    if all.is_empty() && !errors.is_empty() {
        return Err(errors.join("; "));
    }
    let mut notes = vec![];
    if !cut.is_empty() {
        notes.push(format!(
            "showing only the newest {TRACE_LIMIT} records of {}",
            cut.join(", ")
        ));
    }
    if !errors.is_empty() {
        notes.push(format!("incomplete: {}", errors.join("; ")));
    }
    Ok((all, (!notes.is_empty()).then(|| notes.join("; "))))
}

/// Problems per app, joined into the one status line the UI shows.
type Problems = Arc<std::sync::Mutex<BTreeMap<String, String>>>;

/// One app's polling loop. Apps run independently, so a hung one delays only itself.
async fn follow_app(
    mut feed: AppFeed,
    port: u16,
    tx: UnboundedSender<Msg>,
    problems: Problems,
    last_sent: Arc<std::sync::Mutex<String>>,
) {
    let mut tick = tokio::time::interval(POLL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let problem = match feed.poll(port).await {
            Ok((fresh, warning)) => {
                if !fresh.is_empty() && tx.send(Msg::Records(fresh)).is_err() {
                    return;
                }
                warning.unwrap_or_default()
            }
            Err(e) => format!("{}: {e}", feed.app),
        };
        let status = {
            let mut all = problems.lock().unwrap();
            if problem.is_empty() {
                all.remove(&feed.app);
            } else {
                all.insert(feed.app.clone(), problem);
            }
            all.values().cloned().collect::<Vec<_>>().join("; ")
        };
        let mut last = last_sent.lock().unwrap();
        if *last != status {
            last.clone_from(&status);
            if tx.send(Msg::Status(status)).is_err() {
                return;
            }
        }
    }
}

pub async fn run(
    cfg: Config,
    apps: Vec<String>,
    explicit: Option<String>,
    tx: UnboundedSender<Msg>,
    mut requests: UnboundedReceiver<Request>,
) {
    let port = cfg.auth.query_port;
    let problems: Problems = Arc::default();
    let last_sent = Arc::new(std::sync::Mutex::new(String::new()));
    let mut tokens: Vec<(String, SharedToken)> = vec![];
    let mut tasks = tokio::task::JoinSet::new();
    for app in &apps {
        let token = Arc::new(Mutex::new(QueryToken::new(
            &cfg,
            app,
            explicit.clone(),
            SESSION_TTL_SECS,
        )));
        tokens.push((app.clone(), token.clone()));
        let feed = AppFeed {
            app: app.clone(),
            token,
            state: FollowState::new(
                now_ms(),
                (cfg.storage.local.flush_interval_secs as i64 * 2 + 5) * 1000,
            ),
            started: false,
        };
        tasks.spawn(follow_app(
            feed,
            port,
            tx.clone(),
            problems.clone(),
            last_sent.clone(),
        ));
    }
    // the polling tasks end with this function (JoinSet aborts them on drop)
    loop {
        tokio::select! {
            req = requests.recv() => {
                let Some(Request::Trace(id, hash)) = req else { return };
                let tokens = tokens.clone();
                let reply = tx.clone();
                tokio::spawn(async move {
                    let result = trace(port, tokens, hash).await;
                    let _ = reply.send(Msg::Trace(id, result));
                });
            }
            _ = tx.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests;
