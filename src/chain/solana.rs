//! Solana adapter: lists the finalized transactions that touch each watched
//! address (`getSignaturesForAddress`), reads their program logs
//! (`getTransaction`), and records one entry per transaction with `chain = "solana"`
//! and the signature as `tx_hash`, so `loghaven trace <signature>` can line it up
//! with off-chain logs.
//!
//! Only finalized transactions are read, so there is no reorg window to guard.
//! The record timestamp is the ingest time; the block time is in
//! `metadata.block_time`. The cursor holds the newest recorded signature per address.

use crate::chain::rpc::{HttpRpc, Rpc};
use crate::chain::watch::{Step, Watcher};
use crate::client::ClientRecord;
use crate::config::{SolanaConfig, is_base58};
use crate::error::{LogHavenError, Result};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Signatures asked for per `getSignaturesForAddress` call (the node's own maximum).
const PAGE: usize = 1_000;
/// Backlog read per address and poll; an older part is skipped and reported, so a
/// long outage cannot make one poll unbounded.
const MAX_BACKLOG: usize = 10_000;
/// Characters of one program log line kept in a record.
const MAX_LOG_LINE: usize = 512;
/// JSON-escaped bytes of program logs kept in a record, so one transaction cannot oversize a datagram.
const MAX_LOG_BYTES: usize = 4096;
const MAX_ERR_CHARS: usize = 200;
/// Reads of one transaction tried (one per poll) before it is recorded without logs.
const MAX_TX_ATTEMPTS: u32 = 5;
/// A transaction this young that the node reports as unknown is probably not loaded
/// there yet; an older one is pruned history.
const RECENT_SECS: i64 = 600;

/// A recorded transaction the cursor points at. The slot lets a node that does not
/// know the signature (lagging, or pruned past it) be told from a real backlog.
#[derive(Clone, Debug, PartialEq)]
pub struct Mark {
    pub signature: String,
    pub slot: u64,
}

#[derive(Clone)]
struct SigInfo {
    signature: String,
    slot: u64,
    block_time: Option<i64>,
    err: Option<String>,
}

fn parse_sig(v: &Value) -> Result<SigInfo> {
    let bad = |what: &str| LogHavenError::Daemon(format!("rpc: signature entry has no {what}"));
    let signature = v
        .get("signature")
        .and_then(Value::as_str)
        .filter(|s| is_base58(s, 64, 90))
        .ok_or_else(|| bad("valid signature"))?
        .to_string();
    let err = v
        .get("err")
        .filter(|e| !e.is_null())
        .map(|e| e.to_string().chars().take(MAX_ERR_CHARS).collect());
    Ok(SigInfo {
        signature,
        slot: v
            .get("slot")
            .and_then(Value::as_u64)
            .ok_or_else(|| bad("slot"))?,
        block_time: v.get("blockTime").and_then(Value::as_i64),
        err,
    })
}

async fn signatures(
    rpc: &impl Rpc,
    address: &str,
    before: Option<&str>,
    until: Option<&str>,
    limit: usize,
) -> Result<Vec<SigInfo>> {
    let mut opts = json!({"limit": limit, "commitment": "finalized"});
    if let Some(b) = before {
        opts["before"] = json!(b);
    }
    if let Some(u) = until {
        opts["until"] = json!(u);
    }
    let reply = rpc
        .call("getSignaturesForAddress", json!([address, opts]))
        .await?;
    reply
        .as_array()
        .ok_or_else(|| {
            LogHavenError::Daemon("rpc getSignaturesForAddress: result is not a list".into())
        })?
        .iter()
        .map(parse_sig)
        .collect()
}

/// Transactions newer than `until`, oldest first. The flag is true when more than
/// `MAX_BACKLOG` were waiting and the oldest ones were left out.
///
/// A node that does not know `until` ignores it and lists the newest history, so
/// an entry older than the cursor's slot is an error to retry, not a backlog.
/// Paging stops on an empty page, because a provider may return fewer than `PAGE`.
async fn backlog(
    rpc: &impl Rpc,
    address: &str,
    until: Option<&Mark>,
) -> Result<(Vec<SigInfo>, bool)> {
    let until_sig = until.map(|m| m.signature.as_str());
    let mut out: Vec<SigInfo> = vec![];
    let mut skipped = false;
    loop {
        let before = out.last().map(|s| s.signature.clone());
        let page = signatures(rpc, address, before.as_deref(), until_sig, PAGE).await?;
        if page.is_empty() {
            break;
        }
        if until.is_some_and(|m| page.iter().any(|i| i.slot < m.slot)) {
            return Err(LogHavenError::Daemon(format!(
                "rpc does not know the saved signature for {address} yet; retrying"
            )));
        }
        out.extend(page);
        if out.len() >= MAX_BACKLOG {
            let before = out.last().map(|s| s.signature.clone());
            skipped = !signatures(rpc, address, before.as_deref(), until_sig, 1)
                .await?
                .is_empty();
            break;
        }
    }
    out.reverse();
    Ok((out, skipped))
}

async fn transaction(rpc: &impl Rpc, signature: &str) -> Result<Option<Value>> {
    let tx = rpc
        .call(
            "getTransaction",
            json!([signature, {
                "encoding": "json",
                "commitment": "finalized",
                "maxSupportedTransactionVersion": 0,
            }]),
        )
        .await?;
    Ok((!tx.is_null()).then_some(tx))
}

/// What a watcher remembers between polls. All of it is a cache or a retry counter:
/// losing it (a restart) costs calls, never correctness.
#[derive(Default)]
pub struct Memory {
    queues: BTreeMap<String, Queue>,
    failures: BTreeMap<String, u32>,
}

/// An address's listed backlog, oldest first, so a catch-up lists it once instead of every poll.
struct Queue {
    /// Signature the listing was taken after.
    base: Option<String>,
    skipped: bool,
    pending: Vec<SigInfo>,
}

/// The backlog after `until`, from the cached listing when the cursor still sits inside it.
/// The flag reports a skipped part, once: until something from the listing was sent.
async fn pending_for(
    rpc: &impl Rpc,
    address: &str,
    until: Option<&Mark>,
    mem: &mut Memory,
) -> Result<(Vec<SigInfo>, bool)> {
    let until_sig = until.map(|m| m.signature.as_str());
    if let Some(q) = mem.queues.get(address) {
        let offset = if q.base.as_deref() == until_sig {
            Some(0)
        } else {
            until_sig.and_then(|u| {
                q.pending
                    .iter()
                    .position(|i| i.signature == u)
                    .map(|i| i + 1)
            })
        };
        if let Some(o) = offset.filter(|o| *o < q.pending.len()) {
            return Ok((q.pending[o..].to_vec(), q.skipped && o == 0));
        }
    }
    let (pending, skipped) = backlog(rpc, address, until).await?;
    mem.queues.insert(
        address.to_string(),
        Queue {
            base: until_sig.map(str::to_string),
            skipped,
            pending: pending.clone(),
        },
    );
    Ok((pending, skipped))
}

fn is_recent(block_time: Option<i64>) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    block_time.is_some_and(|t| now - t < RECENT_SECS)
}

/// The transaction and, when it was given up on, why it has no logs. A failed or
/// not-yet-known read is an error for the first attempts so the poll is retried.
async fn fetch(
    rpc: &impl Rpc,
    info: &SigInfo,
    mem: &mut Memory,
) -> Result<(Option<Value>, Option<String>)> {
    let failure = match transaction(rpc, &info.signature).await {
        Ok(Some(tx)) => {
            mem.failures.remove(&info.signature);
            return Ok((Some(tx), None));
        }
        Ok(None) if !is_recent(info.block_time) => return Ok((None, None)),
        Ok(None) => LogHavenError::Daemon(format!(
            "rpc does not have transaction {} yet; retrying",
            info.signature
        )),
        Err(e) => e,
    };
    if mem.failures.len() > MAX_BACKLOG {
        mem.failures.clear();
    }
    let attempts = mem.failures.entry(info.signature.clone()).or_default();
    *attempts += 1;
    if *attempts < MAX_TX_ATTEMPTS {
        return Err(failure);
    }
    // the entry stays, so a poll that fails on a later transaction gives up on this one at once
    Ok((
        None,
        Some(failure.to_string().chars().take(MAX_ERR_CHARS).collect()),
    ))
}

/// How many entries of each backlog to take. At most `max` each, and never past
/// the slot where the busiest capped address stops, so a transaction that touches
/// several addresses is taken together in one poll. A slot split by the cap is left
/// for the next poll, unless it is the only slot there is.
fn take_counts(pendings: &[&[SigInfo]], max: usize) -> Vec<usize> {
    if pendings.len() < 2 {
        return pendings.iter().map(|p| p.len().min(max)).collect();
    }
    let mut horizon: Option<u64> = None;
    for p in pendings.iter().filter(|p| p.len() > max) {
        let last = p[max - 1].slot;
        let own = p[..max]
            .iter()
            .rposition(|i| i.slot != last)
            .map_or(last, |k| p[k].slot);
        horizon = Some(horizon.map_or(own, |h| h.min(own)));
    }
    pendings
        .iter()
        .map(|p| match horizon {
            Some(h) => p.partition_point(|i| i.slot <= h).min(max),
            None => p.len(),
        })
        .collect()
}

/// Program logs cut to line and total budgets; the flag says whether anything was dropped.
fn bounded_logs(meta: &Value) -> (Vec<String>, usize, bool) {
    let lines: Vec<&str> = meta
        .get("logMessages")
        .and_then(Value::as_array)
        .map(|l| l.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut kept = vec![];
    let mut used = 0;
    let mut truncated = false;
    for line in &lines {
        let cut: String = line.chars().take(MAX_LOG_LINE).collect();
        truncated |= cut.len() < line.len();
        let cost = serde_json::to_string(&cut).map_or(usize::MAX, |s| s.len() + 1);
        if used + cost > MAX_LOG_BYTES {
            truncated = true;
            break;
        }
        used += cost;
        kept.push(cut);
    }
    (kept, lines.len(), truncated)
}

fn to_record(
    address: &str,
    info: &SigInfo,
    tx: Option<&Value>,
    logs_error: Option<&str>,
) -> ClientRecord {
    let meta = tx.and_then(|t| t.get("meta")).filter(|m| !m.is_null());
    let (logs, log_count, logs_truncated) = meta.map_or((vec![], 0, false), bounded_logs);
    let outcome = if info.err.is_some() { "failed" } else { "ok" };
    ClientRecord {
        level: if info.err.is_some() { "error" } else { "info" }.into(),
        source: "solana".into(),
        message: format!("tx {} slot {} {outcome}", info.signature, info.slot),
        chain: Some("solana".into()),
        tx_hash: Some(info.signature.clone()),
        metadata: Some(json!({
            "address": address,
            "slot": info.slot,
            "block_time": info.block_time,
            "err": info.err,
            "fee": meta.and_then(|m| m.get("fee")).and_then(Value::as_u64),
            "compute_units": meta
                .and_then(|m| m.get("computeUnitsConsumed"))
                .and_then(Value::as_u64),
            "logs_available": meta.is_some(),
            "logs_error": logs_error,
            "logs": logs,
            "log_count": log_count,
            "logs_truncated": logs_truncated,
        })),
        ..Default::default()
    }
}

fn gap_record(address: &str) -> ClientRecord {
    ClientRecord {
        level: "warn".into(),
        source: "solana".into(),
        message: format!(
            "more than {MAX_BACKLOG} transactions behind for {address}; the oldest were skipped"
        ),
        chain: Some("solana".into()),
        metadata: Some(json!({"address": address})),
        ..Default::default()
    }
}

pub struct Batch {
    pub records: Vec<ClientRecord>,
    /// Newest recorded signature per address, to save once `records` are sent.
    pub last: BTreeMap<String, Mark>,
    /// True when an address has more waiting than `max_signatures`.
    pub more: bool,
}

/// Reads the next transactions after `saved`. The caller owns the cursor.
/// Without a cursor it starts at the current tip and records nothing.
pub async fn poll(rpc: &impl Rpc, cfg: &SolanaConfig, saved: Option<&Cursor>) -> Result<Batch> {
    poll_with(rpc, cfg, saved, &mut Memory::default()).await
}

pub async fn poll_with(
    rpc: &impl Rpc,
    cfg: &SolanaConfig,
    saved: Option<&Cursor>,
    mem: &mut Memory,
) -> Result<Batch> {
    let addresses: BTreeSet<&String> = cfg.addresses.iter().collect();
    let max = cfg.max_signatures.max(1) as usize;
    let mut last = BTreeMap::new();
    let Some(saved) = saved else {
        for address in addresses {
            if let Some(tip) = signatures(rpc, address, None, None, 1).await?.pop() {
                last.insert(
                    address.clone(),
                    Mark {
                        signature: tip.signature,
                        slot: tip.slot,
                    },
                );
            }
        }
        return Ok(Batch {
            records: vec![],
            last,
            more: false,
        });
    };

    let mut records = vec![];
    let mut listed = vec![];
    for address in addresses {
        let until = saved.last.get(address);
        let (pending, skipped) = pending_for(rpc, address, until, mem).await?;
        if skipped {
            records.push(gap_record(address));
        }
        listed.push((address, until, pending));
    }
    let slices: Vec<&[SigInfo]> = listed.iter().map(|(_, _, p)| p.as_slice()).collect();
    let counts = take_counts(&slices, max);

    let mut seen = BTreeSet::new();
    let mut more = false;
    for ((address, until, pending), count) in listed.iter().zip(counts) {
        more |= count < pending.len();
        let take = &pending[..count];
        for info in take {
            // a transaction touching several watched addresses is recorded once
            if seen.insert(info.signature.clone()) {
                let (tx, logs_error) = fetch(rpc, info, mem).await?;
                records.push(to_record(address, info, tx.as_ref(), logs_error.as_deref()));
            }
        }
        let newest = take
            .last()
            .map(|i| Mark {
                signature: i.signature.clone(),
                slot: i.slot,
            })
            .or_else(|| (*until).cloned());
        if let Some(newest) = newest {
            last.insert((*address).clone(), newest);
        }
    }
    Ok(Batch {
        records,
        last,
        more,
    })
}

pub struct Cursor {
    /// What the cursor was saved for: the cluster and the watched addresses.
    pub binding: String,
    pub last: BTreeMap<String, Mark>,
}

impl Cursor {
    /// A garbled file is an error: guessing a start would either replay history or skip a gap.
    pub fn parse(text: &str) -> Result<Self> {
        let bad = || {
            LogHavenError::Config("solana.cursor is not a saved cursor; fix or delete it".into())
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
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some(a), Some(sig), Some(slot), None)
                    if is_base58(a, 32, 44) && is_base58(sig, 64, 90) =>
                {
                    let slot = slot.parse().map_err(|_| bad())?;
                    last.insert(
                        a.to_string(),
                        Mark {
                            signature: sig.to_string(),
                            slot,
                        },
                    );
                }
                _ => return Err(bad()),
            }
        }
        Ok(Self { binding, last })
    }

    pub fn render(&self) -> String {
        let mut text = format!("{}\n", self.binding);
        for (address, mark) in &self.last {
            text.push_str(&format!("{address} {} {}\n", mark.signature, mark.slot));
        }
        text
    }
}

/// Identifies what a cursor was saved for: the cluster (by genesis hash) and the watched addresses.
pub async fn binding(rpc: &impl Rpc, cfg: &SolanaConfig) -> Result<String> {
    use sha2::{Digest, Sha256};
    let genesis = rpc.call("getGenesisHash", json!([])).await?;
    let genesis = genesis
        .as_str()
        .filter(|g| is_base58(g, 32, 44))
        .ok_or_else(|| LogHavenError::Daemon("rpc getGenesisHash: result is not a hash".into()))?;
    let addresses: BTreeSet<&String> = cfg.addresses.iter().collect();
    let text = json!([genesis, addresses]).to_string();
    Ok(hex::encode(Sha256::digest(text.as_bytes())))
}

pub fn check_binding(cursor: &Cursor, current: &str) -> Result<()> {
    if cursor.binding == current {
        return Ok(());
    }
    Err(LogHavenError::Config(
        "solana.cursor was saved for another cluster or other addresses; \
         delete it to start from the current tip, or restore the old settings"
            .into(),
    ))
}

pub struct SolanaWatcher<R> {
    rpc: R,
    cfg: SolanaConfig,
    bound: Option<String>,
    memory: Memory,
}

impl SolanaWatcher<HttpRpc> {
    pub fn new(cfg: SolanaConfig) -> Result<Self> {
        Ok(Self::with_rpc(HttpRpc::new(&cfg.rpc_url)?, cfg))
    }
}

impl<R: Rpc> SolanaWatcher<R> {
    pub fn with_rpc(rpc: R, cfg: SolanaConfig) -> Self {
        Self {
            rpc,
            cfg,
            bound: None,
            memory: Memory::default(),
        }
    }
}

impl<R: Rpc> Watcher for SolanaWatcher<R> {
    fn app(&self) -> &str {
        &self.cfg.app
    }

    fn poll_interval_secs(&self) -> u64 {
        self.cfg.poll_interval_secs
    }

    fn describe(&self) -> String {
        format!(
            "{} address(es) as app '{}'",
            self.cfg.addresses.len(),
            self.cfg.app
        )
    }

    async fn step(&mut self, cursor: Option<&str>) -> Result<Step> {
        let saved = cursor.map(Cursor::parse).transpose()?;
        let current = match &self.bound {
            Some(b) => b.clone(),
            None => binding(&self.rpc, &self.cfg).await?,
        };
        if let Some(c) = &saved {
            check_binding(c, &current)?;
        }
        let batch = poll_with(&self.rpc, &self.cfg, saved.as_ref(), &mut self.memory).await?;
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
