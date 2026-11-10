//! Stellar adapter: pages through each watched account's transactions on a
//! Horizon server and records one entry per transaction with `chain = "stellar"`
//! and the transaction hash as `tx_hash`, so `loghaven trace <hash>` can line it
//! up with off-chain logs.
//!
//! Horizon only serves closed ledgers, which are final, so there is no reorg
//! window. The record timestamp is the ingest time; the ledger close time is in
//! `metadata.created_at`. The cursor holds Horizon's `paging_token` of the newest
//! recorded transaction per account.

use crate::chain::rpc::{Get, HttpGet};
use crate::chain::watch::{Step, Watcher, take_counts};
use crate::client::ClientRecord;
use crate::config::StellarConfig;
use crate::error::{LogHavenError, Result};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Horizon's own maximum page size.
const PAGE: usize = 200;
const MAX_MEMO_CHARS: usize = 128;

struct Tx {
    hash: String,
    token: u64,
    ledger: u64,
    created_at: Option<String>,
    successful: bool,
    fee_charged: Option<u64>,
    operation_count: Option<u64>,
    source_account: Option<String>,
    memo_type: Option<String>,
    memo: Option<String>,
}

/// Horizon sends some numbers as strings and some as numbers.
fn number(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn text(v: &Value, key: &str, max: usize) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(|s| s.chars().filter(|c| !c.is_control()).take(max).collect())
}

fn parse_tx(v: &Value) -> Result<Tx> {
    let bad =
        |what: &str| LogHavenError::Daemon(format!("horizon: transaction has no valid {what}"));
    let hash = v
        .get("hash")
        .and_then(Value::as_str)
        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| bad("hash"))?
        .to_ascii_lowercase();
    Ok(Tx {
        hash,
        token: number(v.get("paging_token")).ok_or_else(|| bad("paging_token"))?,
        ledger: number(v.get("ledger")).ok_or_else(|| bad("ledger"))?,
        created_at: text(v, "created_at", 40),
        successful: v.get("successful").and_then(Value::as_bool).unwrap_or(true),
        fee_charged: number(v.get("fee_charged")),
        operation_count: number(v.get("operation_count")),
        source_account: text(v, "source_account", 56),
        memo_type: text(v, "memo_type", 16),
        memo: text(v, "memo", MAX_MEMO_CHARS),
    })
}

/// Up to `want` transactions after `after`, oldest first. Horizon pages ascending from
/// a cursor, so only the part that is going to be recorded is read.
async fn page_after(
    get: &impl Get,
    account: &str,
    after: Option<u64>,
    want: usize,
) -> Result<Vec<Tx>> {
    let mut out: Vec<Tx> = vec![];
    let mut cursor = after;
    while out.len() < want {
        let limit = (want - out.len()).min(PAGE);
        let mut path =
            format!("/accounts/{account}/transactions?order=asc&include_failed=true&limit={limit}");
        if let Some(c) = cursor {
            path.push_str(&format!("&cursor={c}"));
        }
        let Some(reply) = get.get(&path).await? else {
            break;
        };
        let records = reply
            .pointer("/_embedded/records")
            .and_then(Value::as_array)
            .ok_or_else(|| LogHavenError::Daemon("horizon: reply has no records".into()))?;
        if records.is_empty() {
            break;
        }
        for v in records.iter().take(limit) {
            let tx = parse_tx(v)?;
            // a node that does not move forward would make this loop, or a cursor, go backwards
            if cursor.is_some_and(|c| tx.token <= c) {
                return Err(LogHavenError::Daemon(
                    "horizon: transactions are not in ascending order".into(),
                ));
            }
            cursor = Some(tx.token);
            out.push(tx);
        }
    }
    Ok(out)
}

async fn tip(get: &impl Get, account: &str) -> Result<Option<u64>> {
    let path = format!("/accounts/{account}/transactions?order=desc&include_failed=true&limit=1");
    let Some(reply) = get.get(&path).await? else {
        return Ok(None);
    };
    reply
        .pointer("/_embedded/records/0")
        .map(|v| parse_tx(v).map(|t| t.token))
        .transpose()
}

fn to_record(account: &str, tx: &Tx) -> ClientRecord {
    let outcome = if tx.successful { "ok" } else { "failed" };
    ClientRecord {
        level: if tx.successful { "info" } else { "error" }.into(),
        source: "stellar".into(),
        message: format!("tx {} ledger {} {outcome}", tx.hash, tx.ledger),
        chain: Some("stellar".into()),
        tx_hash: Some(tx.hash.clone()),
        metadata: Some(json!({
            "account": account,
            "ledger": tx.ledger,
            "created_at": tx.created_at,
            "successful": tx.successful,
            "fee_charged": tx.fee_charged,
            "operation_count": tx.operation_count,
            "source_account": tx.source_account,
            "memo_type": tx.memo_type,
            "memo": tx.memo,
        })),
        ..Default::default()
    }
}

pub struct Batch {
    pub records: Vec<ClientRecord>,
    /// Paging token of the newest recorded transaction per account, to save once `records` are sent.
    pub last: BTreeMap<String, u64>,
    /// True when an account has more waiting than `max_records`.
    pub more: bool,
}

/// Reads the next transactions after `saved`. The caller owns the cursor.
/// Without a cursor it starts at each account's newest transaction and records nothing.
pub async fn poll(get: &impl Get, cfg: &StellarConfig, saved: Option<&Cursor>) -> Result<Batch> {
    let accounts: BTreeSet<&String> = cfg.accounts.iter().collect();
    let max = cfg.max_records.max(1) as usize;
    let mut last = BTreeMap::new();
    let Some(saved) = saved else {
        for account in accounts {
            if let Some(t) = tip(get, account).await? {
                last.insert(account.clone(), t);
            }
        }
        return Ok(Batch {
            records: vec![],
            last,
            more: false,
        });
    };

    // one more than `max`, to know whether the account has a backlog beyond this poll
    let mut listed = vec![];
    for account in accounts {
        let until = saved.last.get(account).copied();
        listed.push((
            account,
            until,
            page_after(get, account, until, max + 1).await?,
        ));
    }
    let slices: Vec<&[Tx]> = listed.iter().map(|(_, _, p)| p.as_slice()).collect();
    let counts = take_counts(&slices, max, |t| t.ledger);

    let mut records = vec![];
    let mut seen = BTreeSet::new();
    let mut more = false;
    for ((account, until, pending), count) in listed.iter().zip(counts) {
        more |= count < pending.len();
        let take = &pending[..count];
        for tx in take {
            // a transaction touching several watched accounts is recorded once
            if seen.insert(tx.hash.clone()) {
                records.push(to_record(account, tx));
            }
        }
        if let Some(newest) = take.last().map(|t| t.token).or(*until) {
            last.insert((*account).clone(), newest);
        }
    }
    Ok(Batch {
        records,
        last,
        more,
    })
}

pub struct Cursor {
    /// What the cursor was saved for: the network and the watched accounts.
    pub binding: String,
    pub last: BTreeMap<String, u64>,
}

impl Cursor {
    /// A garbled file is an error: guessing a start would either replay history or skip a gap.
    pub fn parse(text: &str) -> Result<Self> {
        let bad = || {
            LogHavenError::Config("stellar.cursor is not a saved cursor; fix or delete it".into())
        };
        let mut lines = text.lines();
        let binding = lines
            .next()
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .ok_or_else(bad)?
            .to_string();
        let mut last = BTreeMap::new();
        for line in lines.filter(|l| !l.trim().is_empty()) {
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next(), parts.next()) {
                (Some(a), Some(t), None) if crate::config::is_stellar_account(a) => {
                    last.insert(a.to_string(), t.parse().map_err(|_| bad())?);
                }
                _ => return Err(bad()),
            }
        }
        Ok(Self { binding, last })
    }

    pub fn render(&self) -> String {
        let mut text = format!("{}\n", self.binding);
        for (account, token) in &self.last {
            text.push_str(&format!("{account} {token}\n"));
        }
        text
    }
}

/// Identifies what a cursor was saved for: the network (by passphrase) and the watched accounts.
pub async fn binding(get: &impl Get, cfg: &StellarConfig) -> Result<String> {
    use sha2::{Digest, Sha256};
    let root = get
        .get("/")
        .await?
        .ok_or_else(|| LogHavenError::Daemon("horizon: no root document".into()))?;
    let passphrase = root
        .get("network_passphrase")
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty() && p.len() < 200)
        .ok_or_else(|| LogHavenError::Daemon("horizon: root has no network_passphrase".into()))?;
    let accounts: BTreeSet<&String> = cfg.accounts.iter().collect();
    let text = json!([passphrase, accounts]).to_string();
    Ok(hex::encode(Sha256::digest(text.as_bytes())))
}

pub fn check_binding(cursor: &Cursor, current: &str) -> Result<()> {
    if cursor.binding == current {
        return Ok(());
    }
    Err(LogHavenError::Config(
        "stellar.cursor was saved for another network or other accounts; \
         delete it to start from the current tip, or restore the old settings"
            .into(),
    ))
}

pub struct StellarWatcher<G> {
    get: G,
    cfg: StellarConfig,
    bound: Option<String>,
}

impl StellarWatcher<HttpGet> {
    pub fn new(cfg: StellarConfig) -> Result<Self> {
        Ok(Self::with_get(HttpGet::new(&cfg.horizon_url)?, cfg))
    }
}

impl<G: Get> StellarWatcher<G> {
    pub fn with_get(get: G, cfg: StellarConfig) -> Self {
        Self {
            get,
            cfg,
            bound: None,
        }
    }
}

impl<G: Get> Watcher for StellarWatcher<G> {
    fn app(&self) -> &str {
        &self.cfg.app
    }

    fn poll_interval_secs(&self) -> u64 {
        self.cfg.poll_interval_secs
    }

    fn describe(&self) -> String {
        format!(
            "{} account(s) as app '{}'",
            self.cfg.accounts.len(),
            self.cfg.app
        )
    }

    async fn step(&mut self, cursor: Option<&str>) -> Result<Step> {
        let saved = cursor.map(Cursor::parse).transpose()?;
        let current = match &self.bound {
            Some(b) => b.clone(),
            None => binding(&self.get, &self.cfg).await?,
        };
        if let Some(c) = &saved {
            check_binding(c, &current)?;
        }
        let batch = poll(&self.get, &self.cfg, saved.as_ref()).await?;
        self.bound = Some(current.clone());
        let cursor = Cursor {
            binding: current,
            last: batch.last,
        };
        Ok(Step {
            records: batch.records,
            cursor: cursor.render(),
            more: batch.more,
            notice: None,
        })
    }
}

#[cfg(test)]
mod tests;
