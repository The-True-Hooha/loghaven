//! Ethereum adapter: polls `eth_getLogs` over JSON-RPC and normalizes each log
//! into a record with `chain = "ethereum"` and the transaction hash set, so
//! `loghaven trace <tx>` can line it up with off-chain logs.
//!
//! Only blocks at least `confirmations` deep are read, which is the reorg
//! protection. The record timestamp is the ingest time; the block time is in
//! `metadata.block_timestamp`.

use crate::client::ClientRecord;
use crate::config::EthereumConfig;
use crate::error::{LogHavenError, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// A cursor further ahead of the chain than this is treated as a mistake, not as waiting.
const MAX_AHEAD: u64 = 1_000;
/// Hex characters of event `data` kept in a record; longer data is cut so one log cannot oversize a datagram.
const MAX_DATA_HEX: usize = 4096;

pub trait Rpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value>;
}

pub struct HttpRpc {
    url: String,
    client: reqwest::Client,
}

impl HttpRpc {
    pub fn new(url: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| LogHavenError::Daemon(e.to_string()))?;
        Ok(Self {
            url: url.to_string(),
            client,
        })
    }
}

impl Rpc for HttpRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        // RPC URLs often embed an API key, so the URL is kept out of every error.
        let fail = |e: reqwest::Error| {
            LogHavenError::Daemon(format!("rpc {method} failed: {}", e.without_url()))
        };
        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(fail)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(LogHavenError::Daemon(format!(
                "rpc {method} failed: HTTP {status}"
            )));
        }
        let mut reply: Value = resp.json().await.map_err(fail)?;
        if let Some(err) = reply.get("error") {
            return Err(LogHavenError::Daemon(format!(
                "rpc {method} error: {}",
                err.get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            )));
        }
        reply
            .get_mut("result")
            .map(Value::take)
            .ok_or_else(|| LogHavenError::Daemon(format!("rpc {method}: reply has no result")))
    }
}

pub struct Batch {
    pub records: Vec<ClientRecord>,
    /// First block not yet covered; persist it once `records` are sent.
    pub next_block: u64,
    /// True when the poll stopped at `max_block_range` before reaching the safe head.
    pub more: bool,
}

fn quantity(v: &Value, what: &str) -> Result<u64> {
    v.as_str()
        .and_then(|s| s.strip_prefix("0x"))
        .and_then(|h| u64::from_str_radix(h, 16).ok())
        .ok_or_else(|| {
            let shown: String = v.to_string().chars().take(100).collect();
            LogHavenError::Daemon(format!("rpc: bad {what} quantity: {shown}"))
        })
}

/// Reads one range of confirmed blocks. Stateless: the caller owns the cursor.
pub async fn poll(rpc: &impl Rpc, cfg: &EthereumConfig, cursor: Option<u64>) -> Result<Batch> {
    let head = quantity(
        &rpc.call("eth_blockNumber", json!([])).await?,
        "block number",
    )?;
    let safe = head.saturating_sub(cfg.confirmations);
    let from = cursor.or(cfg.start_block).unwrap_or(safe);
    if from > safe.saturating_add(MAX_AHEAD) {
        return Err(LogHavenError::Daemon(format!(
            "next block {from} is far ahead of the chain head {head}; \
             delete ethereum.cursor or lower start_block (wrong chain or node?)"
        )));
    }
    if from > safe {
        return Ok(Batch {
            records: vec![],
            next_block: from,
            more: false,
        });
    }
    let mut to = safe.min(from.saturating_add(cfg.max_block_range.max(1) - 1));

    // Nodes reject ranges that match too many logs, so a failed call retries on half the span.
    let logs = loop {
        let mut filter = json!({
            "fromBlock": format!("{from:#x}"),
            "toBlock": format!("{to:#x}"),
            "address": cfg.addresses,
        });
        if !cfg.topics.is_empty() {
            filter["topics"] = json!([cfg.topics]);
        }
        match rpc.call("eth_getLogs", json!([filter])).await {
            Ok(v) => break v,
            Err(_) if to > from => to = from + (to - from) / 2,
            Err(e) => {
                return Err(LogHavenError::Daemon(format!(
                    "block {from} cannot be read even alone: {e}"
                )));
            }
        }
    };
    let logs = logs
        .as_array()
        .ok_or_else(|| LogHavenError::Daemon("rpc eth_getLogs: result is not a list".into()))?;

    let mut parsed = logs.iter().map(parse_log).collect::<Result<Vec<_>>>()?;
    // nodes report only canonical logs over HTTP, but a removed log must never be recorded
    parsed.retain(|l| !l.removed);
    parsed.sort_by_key(|l| (l.block, l.log_index));

    let mut times: BTreeMap<u64, u64> = BTreeMap::new();
    for l in &parsed {
        if let std::collections::btree_map::Entry::Vacant(slot) = times.entry(l.block) {
            let block = rpc
                .call(
                    "eth_getBlockByNumber",
                    json!([format!("{:#x}", l.block), false]),
                )
                .await?;
            let ts = block
                .get("timestamp")
                .ok_or_else(|| LogHavenError::Daemon("rpc: block has no timestamp".into()))?;
            slot.insert(quantity(ts, "timestamp")?);
        }
    }

    let records = parsed
        .iter()
        .map(|l| to_record(l, times[&l.block]))
        .collect();
    Ok(Batch {
        records,
        next_block: to.saturating_add(1),
        more: to < safe,
    })
}

struct RawLog {
    address: String,
    topics: Vec<String>,
    data: String,
    block: u64,
    log_index: u64,
    tx_hash: String,
    removed: bool,
}

fn parse_log(v: &Value) -> Result<RawLog> {
    let text = |key: &str| {
        v.get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| LogHavenError::Daemon(format!("rpc log has no {key}")))
    };
    let num = |key: &str| {
        v.get(key)
            .ok_or_else(|| LogHavenError::Daemon(format!("rpc log has no {key}")))
            .and_then(|q| quantity(q, key))
    };
    Ok(RawLog {
        address: text("address")?,
        topics: v
            .get("topics")
            .and_then(Value::as_array)
            .map(|t| {
                t.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        data: text("data")?,
        block: num("blockNumber")?,
        log_index: num("logIndex")?,
        tx_hash: text("transactionHash")?,
        removed: v.get("removed").and_then(Value::as_bool).unwrap_or(false),
    })
}

fn to_record(l: &RawLog, block_timestamp: u64) -> ClientRecord {
    let data_len = l.data.len().min(MAX_DATA_HEX);
    let topic0 = l.topics.first().map_or("none", String::as_str);
    ClientRecord {
        level: "info".into(),
        source: "ethereum".into(),
        message: format!("log {} topic0={topic0} block {}", l.address, l.block),
        chain: Some("ethereum".into()),
        tx_hash: Some(l.tx_hash.clone()),
        metadata: Some(json!({
            "address": l.address,
            "topics": l.topics,
            "data": &l.data[..data_len],
            "data_len": l.data.len(),
            "data_truncated": data_len < l.data.len(),
            "block_number": l.block,
            "block_timestamp": block_timestamp,
            "log_index": l.log_index,
            "removed": l.removed,
        })),
        ..Default::default()
    }
}

pub struct Cursor {
    pub next_block: u64,
    /// Absent in cursor files written by 0.2.0, which are adopted on the next save.
    pub binding: Option<String>,
}

/// Identifies what a cursor was saved for: the chain and the watched addresses and topics.
pub async fn binding(rpc: &impl Rpc, cfg: &EthereumConfig) -> Result<String> {
    use sha2::{Digest, Sha256};
    let chain_id = quantity(&rpc.call("eth_chainId", json!([])).await?, "chain id")?;
    let sorted = |v: &[String]| {
        let mut v: Vec<String> = v.iter().map(|s| s.to_ascii_lowercase()).collect();
        v.sort();
        v.dedup();
        v
    };
    let text = json!([chain_id, sorted(&cfg.addresses), sorted(&cfg.topics)]).to_string();
    Ok(hex::encode(Sha256::digest(text.as_bytes())))
}

pub fn check_binding(cursor: &Cursor, current: &str) -> Result<()> {
    match &cursor.binding {
        Some(saved) if saved != current => Err(LogHavenError::Config(
            "ethereum.cursor was saved for another chain or other addresses/topics; \
             delete it and set start_block to re-read history, or restore the old settings"
                .into(),
        )),
        _ => Ok(()),
    }
}

/// `Ok(None)` only when the file does not exist. A garbled file is an error:
/// guessing a start block would either replay history or skip a gap.
pub fn read_cursor(path: &Path) -> Result<Option<Cursor>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut lines = text.lines();
    let next_block = lines
        .next()
        .and_then(|l| l.trim().parse().ok())
        .ok_or_else(|| {
            LogHavenError::Config(format!(
                "{} is not a block number; fix or delete it",
                path.display()
            ))
        })?;
    let binding = lines
        .next()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .map(str::to_string);
    Ok(Some(Cursor {
        next_block,
        binding,
    }))
}

pub fn write_cursor(path: &Path, next_block: u64, binding: &str) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("cursor.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(format!("{next_block}\n{binding}\n").as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests;
