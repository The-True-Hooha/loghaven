use super::*;
use std::cell::RefCell;

const ADDR: &str = "So11111111111111111111111111111111111111112";
const ADDR2: &str = "Vote111111111111111111111111111111111111111";
const GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

/// Valid base58 signature for transaction number `n` (digits map to letters).
fn sig(n: u64) -> String {
    let tail: String = n
        .to_string()
        .bytes()
        .map(|d| (b'a' + d - b'0') as char)
        .collect();
    format!("{}{tail}", "S".repeat(64))
}

struct Mock {
    /// Per address, newest first, as the real node lists them.
    sigs: BTreeMap<String, Vec<(u64, Option<Value>)>>,
    txs: BTreeMap<u64, Value>,
    genesis: String,
    fail_tx: bool,
    max_page: usize,
    recent: bool,
    calls: RefCell<Vec<(String, Value)>>,
}

impl Mock {
    fn new() -> Self {
        Self {
            sigs: BTreeMap::new(),
            txs: BTreeMap::new(),
            genesis: GENESIS.into(),
            fail_tx: false,
            max_page: usize::MAX,
            recent: false,
            calls: RefCell::new(vec![]),
        }
    }

    /// Transactions `range` touch `addr`; higher numbers are newer.
    fn with(mut self, addr: &str, range: std::ops::RangeInclusive<u64>) -> Self {
        let list = self.sigs.entry(addr.into()).or_default();
        list.extend(range.rev().map(|n| (n, None)));
        list.sort_by_key(|(n, _)| std::cmp::Reverse(*n));
        list.dedup_by_key(|(n, _)| *n);
        self
    }

    fn with_tx(mut self, n: u64, logs: Vec<&str>) -> Self {
        self.txs.insert(
            n,
            json!({"slot": 1000 + n, "meta": {
                "err": null, "fee": 5000, "computeUnitsConsumed": 1200, "logMessages": logs,
            }}),
        );
        self
    }

    fn failed(mut self, addr: &str, n: u64) -> Self {
        for e in self.sigs.get_mut(addr).unwrap() {
            if e.0 == n {
                e.1 = Some(json!({"InstructionError": [0, "Custom"]}));
            }
        }
        self
    }

    fn number(s: &str) -> u64 {
        s.strip_prefix(&"S".repeat(64))
            .unwrap()
            .bytes()
            .fold(0, |acc, c| acc * 10 + (c - b'a') as u64)
    }

    fn count(&self, method: &str) -> usize {
        self.calls
            .borrow()
            .iter()
            .filter(|(m, _)| m == method)
            .count()
    }
}

impl Rpc for Mock {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.calls
            .borrow_mut()
            .push((method.into(), params.clone()));
        match method {
            "getGenesisHash" => Ok(json!(self.genesis)),
            "getSignaturesForAddress" => {
                let addr = params[0].as_str().unwrap();
                let opts = &params[1];
                assert_eq!(opts["commitment"], "finalized");
                let list = self.sigs.get(addr).cloned().unwrap_or_default();
                let start = opts["before"].as_str().map_or(0, |b| {
                    list.iter()
                        .position(|(n, _)| *n == Self::number(b))
                        .unwrap()
                        + 1
                });
                let end = opts["until"].as_str().map_or(list.len(), |u| {
                    list.iter()
                        .position(|(n, _)| *n == Self::number(u))
                        .unwrap_or(list.len())
                });
                let limit = (opts["limit"].as_u64().unwrap() as usize).min(self.max_page);
                let page: Vec<Value> = list[start.min(end)..end]
                    .iter()
                    .take(limit)
                    .map(|(n, err)| {
                        json!({"signature": sig(*n), "slot": 1000 + n, "err": err,
                               "blockTime": if self.recent { now() as u64 } else { 1_700_000_000 + n }, "confirmationStatus": "finalized"})
                    })
                    .collect();
                Ok(Value::Array(page))
            }
            "getTransaction" if self.fail_tx => Err(LogHavenError::Daemon(
                "rpc getTransaction error: boom".into(),
            )),
            "getTransaction" => {
                assert_eq!(params[1]["commitment"], "finalized");
                Ok(self
                    .txs
                    .get(&Self::number(params[0].as_str().unwrap()))
                    .cloned()
                    .unwrap_or(Value::Null))
            }
            other => panic!("unexpected rpc {other}"),
        }
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn cfg() -> SolanaConfig {
    SolanaConfig {
        rpc_url: "http://127.0.0.1:1".into(),
        ws_url: None,
        app: "solana".into(),
        addresses: vec![ADDR.into()],
        poll_interval_secs: 5,
        max_signatures: 100,
    }
}

fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

fn mark(n: u64) -> Mark {
    Mark {
        signature: sig(n),
        slot: 1000 + n,
    }
}

fn cursor_at(addr: &str, n: u64) -> Cursor {
    Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(addr.to_string(), mark(n))]),
    }
}

fn hashes(b: &Batch) -> Vec<String> {
    b.records.iter().filter_map(|r| r.tx_hash.clone()).collect()
}

#[test]
fn first_run_starts_at_the_tip_and_records_nothing() {
    let rpc = Mock::new().with(ADDR, 1..=50);
    let b = block_on(poll(&rpc, &cfg(), None)).unwrap();
    assert!(b.records.is_empty());
    assert_eq!(b.last[ADDR].signature, sig(50));
    assert_eq!(rpc.count("getTransaction"), 0);
}

#[test]
fn first_run_on_an_address_without_history_saves_no_entry() {
    let rpc = Mock::new();
    let b = block_on(poll(&rpc, &cfg(), None)).unwrap();
    assert!(b.records.is_empty() && b.last.is_empty());
}

#[test]
fn new_transactions_are_recorded_oldest_first_with_the_signature_as_tx_hash() {
    let rpc = Mock::new()
        .with(ADDR, 1..=53)
        .with_tx(51, vec!["Program X invoke [1]", "Program log: hi"])
        .failed(ADDR, 52);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 50)))).unwrap();
    assert_eq!(hashes(&b), [sig(51), sig(52), sig(53)]);
    assert_eq!(b.last[ADDR].signature, sig(53));
    assert!(!b.more);

    let r = &b.records[0];
    assert_eq!((r.level.as_str(), r.source.as_str()), ("info", "solana"));
    assert_eq!(r.chain.as_deref(), Some("solana"));
    assert_eq!(r.message, format!("tx {} slot 1051 ok", sig(51)));
    let m = r.metadata.as_ref().unwrap();
    assert_eq!(m["address"], ADDR);
    assert_eq!(m["block_time"], 1_700_000_051u64);
    assert_eq!(m["fee"], 5000);
    assert_eq!(m["compute_units"], 1200);
    assert_eq!(
        m["logs"],
        json!(["Program X invoke [1]", "Program log: hi"])
    );
    assert_eq!(m["logs_available"], true);

    let failed = &b.records[1];
    assert_eq!(failed.level, "error");
    assert!(failed.message.ends_with("failed"));
    assert!(
        failed.metadata.as_ref().unwrap()["err"]
            .as_str()
            .unwrap()
            .contains("Custom")
    );
}

#[test]
fn caught_up_records_nothing_and_keeps_the_cursor() {
    let rpc = Mock::new().with(ADDR, 1..=50);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 50)))).unwrap();
    assert!(b.records.is_empty() && !b.more);
    assert_eq!(b.last[ADDR].signature, sig(50));
}

#[test]
fn a_backlog_beyond_max_signatures_is_taken_in_order_over_several_polls() {
    let rpc = Mock::new().with(ADDR, 1..=25);
    let c = SolanaConfig {
        max_signatures: 10,
        ..cfg()
    };
    let b1 = block_on(poll(&rpc, &c, Some(&cursor_at(ADDR, 0)))).unwrap();
    assert_eq!(hashes(&b1), (1..=10).map(sig).collect::<Vec<_>>());
    assert!(b1.more);
    let cur = Cursor {
        binding: "b".into(),
        last: b1.last,
    };
    let b2 = block_on(poll(&rpc, &c, Some(&cur))).unwrap();
    assert_eq!(hashes(&b2), (11..=20).map(sig).collect::<Vec<_>>());
    assert!(b2.more);
    let cur = Cursor {
        binding: "b".into(),
        last: b2.last,
    };
    let b3 = block_on(poll(&rpc, &c, Some(&cur))).unwrap();
    assert_eq!(hashes(&b3), (21..=25).map(sig).collect::<Vec<_>>());
    assert!(!b3.more);
}

#[test]
fn a_backlog_longer_than_one_page_is_read_across_pages_without_gaps() {
    let rpc = Mock::new().with(ADDR, 1..=2_500);
    let c = SolanaConfig {
        max_signatures: 1_000,
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, Some(&cursor_at(ADDR, 0)))).unwrap();
    assert_eq!(b.records.len(), 1_000);
    assert_eq!(hashes(&b).first(), Some(&sig(1)));
    assert_eq!(hashes(&b).last(), Some(&sig(1_000)));
    assert!(b.more);
    assert!(rpc.count("getSignaturesForAddress") >= 3);
}

#[test]
fn an_oversized_backlog_skips_the_oldest_and_says_so_once() {
    let rpc = Mock::new().with(ADDR, 1..=11_000);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0)))).unwrap();
    let gap = &b.records[0];
    assert_eq!((gap.level.as_str(), gap.tx_hash.as_deref()), ("warn", None));
    assert!(gap.message.contains("oldest were skipped"));
    // the newest 10000 were read: 1001..=11000, oldest of them first
    assert_eq!(hashes(&b).first(), Some(&sig(1_001)));
    let cur = Cursor {
        binding: "b".into(),
        last: b.last,
    };
    let again = block_on(poll(&rpc, &cfg(), Some(&cur))).unwrap();
    assert!(again.records.iter().all(|r| r.level != "warn"));
}

#[test]
fn a_backlog_of_exactly_the_limit_is_not_reported_as_a_gap() {
    let rpc = Mock::new().with(ADDR, 1..=10_000);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0)))).unwrap();
    assert!(b.records.iter().all(|r| r.level != "warn"));
}

#[test]
fn a_pruned_transaction_is_still_recorded_without_logs() {
    let rpc = Mock::new().with(ADDR, 1..=1);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0)))).unwrap();
    let m = b.records[0].metadata.as_ref().unwrap();
    assert_eq!(m["logs_available"], false);
    assert_eq!(m["logs"], json!([]));
}

#[test]
fn a_failing_transaction_read_fails_the_poll_so_the_cursor_does_not_move() {
    let mut rpc = Mock::new().with(ADDR, 1..=3);
    rpc.fail_tx = true;
    let e = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0))))
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("boom"), "{e}");
}

#[test]
fn program_logs_are_cut_to_a_budget_that_keeps_the_datagram_small() {
    let long = "x".repeat(5_000);
    let escaped = "\u{1}".repeat(500);
    let many: Vec<String> = (0..200).map(|i| format!("Program log: line {i}")).collect();
    let mut logs: Vec<&str> = vec![&long, &escaped];
    logs.extend(many.iter().map(String::as_str));
    let rpc = Mock::new().with(ADDR, 1..=1).with_tx(1, logs);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0)))).unwrap();
    let m = b.records[0].metadata.as_ref().unwrap();
    assert_eq!(m["logs_truncated"], true);
    assert_eq!(m["log_count"], 202);
    assert_eq!(m["logs"][0].as_str().unwrap().len(), MAX_LOG_LINE);
    assert!(serde_json::to_vec(&b.records[0]).unwrap().len() < 9_000 - 1_000);
}

#[test]
fn a_transaction_touching_two_watched_addresses_is_recorded_once() {
    let rpc = Mock::new().with(ADDR, 1..=2).with(ADDR2, 2..=3);
    let c = SolanaConfig {
        addresses: vec![ADDR.into(), ADDR2.into()],
        ..cfg()
    };
    let cur = Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(ADDR.into(), mark(0)), (ADDR2.into(), mark(1))]),
    };
    let b = block_on(poll(&rpc, &c, Some(&cur))).unwrap();
    let mut got = hashes(&b);
    got.sort();
    assert_eq!(got, [sig(1), sig(2), sig(3)]);
    assert_eq!(b.last[ADDR].signature, sig(2));
    assert_eq!(b.last[ADDR2].signature, sig(3));
}

#[test]
fn an_address_missing_from_the_cursor_is_read_from_the_start() {
    let rpc = Mock::new().with(ADDR, 1..=2);
    let cur = Cursor {
        binding: "b".into(),
        last: BTreeMap::new(),
    };
    let b = block_on(poll(&rpc, &cfg(), Some(&cur))).unwrap();
    assert_eq!(hashes(&b), [sig(1), sig(2)]);
}

#[test]
fn a_malformed_signature_from_the_node_is_an_error_not_a_cursor() {
    for bad in [json!("not base58 !"), json!(""), json!(5), json!(null)] {
        let v = json!({"signature": bad, "slot": 1});
        assert!(parse_sig(&v).is_err(), "{bad}");
    }
    assert!(parse_sig(&json!({"signature": sig(1)})).is_err(), "no slot");
    let ok = parse_sig(&json!({"signature": sig(1), "slot": 9, "blockTime": null})).unwrap();
    assert_eq!((ok.slot, ok.block_time, ok.err), (9, None, None));
}

#[test]
fn cursor_text_round_trips_and_garbage_is_an_error() {
    let c = Cursor {
        binding: "abc".into(),
        last: BTreeMap::from([(ADDR.into(), mark(7)), (ADDR2.into(), mark(8))]),
    };
    let back = Cursor::parse(&c.render()).unwrap();
    assert_eq!((back.binding.as_str(), back.last), ("abc", c.last));
    assert!(Cursor::parse("abc\n").unwrap().last.is_empty());
    let short_sig = format!("abc\n{ADDR} abc 5\n");
    let no_slot = format!("abc\n{ADDR} {}\n", sig(1));
    let bad_slot = format!("abc\n{ADDR} {} x\n", sig(1));
    let three = format!("abc\n{ADDR} {} 5 extra\n", sig(1));
    let bad_addr = format!("abc\n0OIl {} 5\n", sig(1));
    for bad in [
        "",
        "\n",
        short_sig.as_str(),
        no_slot.as_str(),
        bad_slot.as_str(),
        three.as_str(),
        bad_addr.as_str(),
    ] {
        assert!(Cursor::parse(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn binding_changes_with_cluster_and_addresses_but_not_order() {
    let rpc = Mock::new();
    let b = |c: &SolanaConfig| block_on(binding(&rpc, c)).unwrap();
    let base = b(&cfg());
    assert_eq!(base, b(&cfg()));
    let two = SolanaConfig {
        addresses: vec![ADDR.into(), ADDR2.into()],
        ..cfg()
    };
    assert_ne!(base, b(&two));
    let swapped = SolanaConfig {
        addresses: vec![ADDR2.into(), ADDR.into(), ADDR.into()],
        ..cfg()
    };
    assert_eq!(b(&two), b(&swapped));
    let mut other = Mock::new();
    other.genesis = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG".into();
    assert_ne!(base, block_on(binding(&other, &cfg())).unwrap());
    let lowered = SolanaConfig {
        addresses: vec![ADDR.to_lowercase()],
        ..cfg()
    };
    assert_ne!(base, b(&lowered), "base58 is case sensitive");
}

#[test]
fn a_cursor_saved_for_other_settings_is_refused() {
    let saved = Cursor {
        binding: "old".into(),
        last: BTreeMap::new(),
    };
    let e = check_binding(&saved, "new").unwrap_err().to_string();
    assert!(e.contains("another cluster"), "{e}");
    assert!(check_binding(&saved, "old").is_ok());
}

#[test]
fn watcher_starts_at_the_tip_then_follows_with_a_cursor_it_wrote() {
    let mut w = SolanaWatcher::with_rpc(Mock::new().with(ADDR, 1..=5), cfg());
    let first = block_on(w.step(None)).unwrap();
    assert!(first.records.is_empty());
    let saved = Cursor::parse(&first.cursor).unwrap();
    assert_eq!(saved.last[ADDR].signature, sig(5));

    let mut w = SolanaWatcher::with_rpc(Mock::new().with(ADDR, 1..=7), cfg());
    let next = block_on(w.step(Some(&first.cursor))).unwrap();
    assert_eq!(next.records.len(), 2);
    assert_eq!(
        Cursor::parse(&next.cursor).unwrap().last[ADDR].signature,
        sig(7)
    );
}

#[test]
fn watcher_refuses_a_foreign_cursor_and_garbage_with_config_errors() {
    let mut w = SolanaWatcher::with_rpc(Mock::new(), cfg());
    let foreign = Cursor {
        binding: "someone-else".into(),
        last: BTreeMap::new(),
    }
    .render();
    for text in [foreign.as_str(), "\n"] {
        let e = block_on(w.step(Some(text))).err().unwrap();
        assert!(matches!(e, LogHavenError::Config(_)), "{e}");
    }
}

#[test]
fn validation_rejects_bad_settings() {
    assert!(crate::config::validate_solana(&cfg()).is_ok());
    let c = |f: &dyn Fn(&mut SolanaConfig)| {
        let mut s = cfg();
        f(&mut s);
        crate::config::validate_solana(&s).is_err()
    };
    assert!(c(&|s| s.rpc_url = "ftp://x".into()));
    assert!(c(&|s| s.addresses.clear()));
    assert!(c(&|s| s.addresses = vec!["0x1234".into()]));
    assert!(c(&|s| s.addresses = vec!["short".into()]));
    assert!(c(&|s| s.poll_interval_secs = 0));
    assert!(c(&|s| s.max_signatures = 0));
    assert!(c(&|s| s.max_signatures = 1_001));
    assert!(c(&|s| s.app = "../x".into()));
}

#[test]
fn a_node_that_clamps_the_page_size_loses_nothing() {
    let mut rpc = Mock::new().with(ADDR, 1..=250);
    rpc.max_page = 100;
    let c = SolanaConfig {
        max_signatures: 1_000,
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, Some(&cursor_at(ADDR, 0)))).unwrap();
    assert_eq!(hashes(&b), (1..=250).map(sig).collect::<Vec<_>>());
    assert!(b.records.iter().all(|r| r.level != "warn"));
}

#[test]
fn a_saved_signature_the_node_does_not_know_is_retried_not_replayed() {
    // the node lists history older than the cursor because it never heard of the signature
    let rpc = Mock::new().with(ADDR, 1..=60);
    let ahead = Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(
            ADDR.to_string(),
            Mark {
                signature: sig(9_999),
                slot: 1000 + 9_999,
            },
        )]),
    };
    let e = block_on(poll(&rpc, &cfg(), Some(&ahead))).err().unwrap();
    assert!(matches!(e, LogHavenError::Daemon(_)), "retryable: {e}");
    assert!(e.to_string().contains("does not know"), "{e}");
    assert_eq!(rpc.count("getTransaction"), 0);
}

#[test]
fn a_null_meta_counts_as_no_logs() {
    let mut rpc = Mock::new().with(ADDR, 1..=1);
    rpc.txs.insert(1, json!({"slot": 1001, "meta": null}));
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(ADDR, 0)))).unwrap();
    let m = b.records[0].metadata.as_ref().unwrap();
    assert_eq!(m["logs_available"], false);
}

fn cursor_from(b: Batch) -> Cursor {
    Cursor {
        binding: "b".into(),
        last: b.last,
    }
}

fn info(n: u64, slot: u64) -> SigInfo {
    SigInfo {
        signature: sig(n),
        slot,
        block_time: None,
        err: None,
    }
}

#[test]
fn a_transaction_that_always_errors_is_recorded_without_logs_and_the_watcher_moves_on() {
    let mut rpc = Mock::new().with(ADDR, 1..=3);
    rpc.fail_tx = true;
    let mut mem = Memory::default();
    let mut cur = cursor_at(ADDR, 0);
    let mut recorded = vec![];
    let mut errors = 0;
    for _ in 0..40 {
        match block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)) {
            Ok(b) => {
                recorded.extend(b.records.iter().filter_map(|r| r.metadata.clone()));
                cur = cursor_from(b);
            }
            Err(_) => errors += 1,
        }
        if cur.last[ADDR].signature == sig(3) {
            break;
        }
    }
    assert_eq!(
        cur.last[ADDR].signature,
        sig(3),
        "stalled after {errors} errors"
    );
    assert_eq!(recorded.len(), 3);
    assert!(recorded.iter().all(|m| m["logs_available"] == false));
    assert!(recorded[0]["logs_error"].as_str().unwrap().contains("boom"));
    assert!(errors >= MAX_TX_ATTEMPTS as usize - 1);
}

#[test]
fn a_recent_transaction_the_node_does_not_have_yet_is_retried_then_given_up() {
    let mut rpc = Mock::new().with(ADDR, 1..=1);
    rpc.recent = true;
    let mut mem = Memory::default();
    let cur = cursor_at(ADDR, 0);
    for _ in 0..MAX_TX_ATTEMPTS - 1 {
        let e = block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem))
            .err()
            .unwrap();
        assert!(e.to_string().contains("yet"), "{e}");
    }
    let b = block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)).unwrap();
    assert_eq!(
        b.records[0].metadata.as_ref().unwrap()["logs_available"],
        false
    );
}

#[test]
fn a_recent_transaction_that_appears_later_is_recorded_with_its_logs() {
    let mut rpc = Mock::new().with(ADDR, 1..=1);
    rpc.recent = true;
    let mut mem = Memory::default();
    let cur = cursor_at(ADDR, 0);
    assert!(block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)).is_err());
    let rpc = Mock::new()
        .with(ADDR, 1..=1)
        .with_tx(1, vec!["Program log: ok"]);
    let b = block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)).unwrap();
    assert_eq!(
        b.records[0].metadata.as_ref().unwrap()["logs"],
        json!(["Program log: ok"])
    );
    assert!(mem.failures.is_empty());
}

#[test]
fn a_catch_up_lists_the_backlog_once_and_loses_nothing_if_a_send_fails() {
    let rpc = Mock::new().with(ADDR, 1..=250);
    let c = SolanaConfig {
        max_signatures: 100,
        ..cfg()
    };
    let mut mem = Memory::default();
    let cur = cursor_at(ADDR, 0);
    let first = block_on(poll_with(&rpc, &c, Some(&cur), &mut mem)).unwrap();
    let listings = rpc.count("getSignaturesForAddress");
    // the send failed: the same cursor is polled again and must give the same transactions
    let again = block_on(poll_with(&rpc, &c, Some(&cur), &mut mem)).unwrap();
    assert_eq!(hashes(&first), hashes(&again));
    let second = block_on(poll_with(&rpc, &c, Some(&cursor_from(first)), &mut mem)).unwrap();
    assert_eq!(hashes(&second), (101..=200).map(sig).collect::<Vec<_>>());
    let third = block_on(poll_with(&rpc, &c, Some(&cursor_from(second)), &mut mem)).unwrap();
    assert_eq!(hashes(&third), (201..=250).map(sig).collect::<Vec<_>>());
    assert!(!third.more);
    assert_eq!(rpc.count("getSignaturesForAddress"), listings);
}

#[test]
fn the_skipped_gap_is_reported_again_until_something_is_sent() {
    let rpc = Mock::new().with(ADDR, 1..=11_000);
    let mut mem = Memory::default();
    let cur = cursor_at(ADDR, 0);
    for _ in 0..2 {
        let b = block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)).unwrap();
        assert_eq!(b.records.iter().filter(|r| r.level == "warn").count(), 1);
    }
    let b = block_on(poll_with(&rpc, &cfg(), Some(&cur), &mut mem)).unwrap();
    let next = block_on(poll_with(&rpc, &cfg(), Some(&cursor_from(b)), &mut mem)).unwrap();
    assert!(next.records.iter().all(|r| r.level != "warn"));
}

#[test]
fn a_new_transaction_is_seen_once_the_cached_backlog_is_used_up() {
    let mut mem = Memory::default();
    let c = SolanaConfig {
        max_signatures: 100,
        ..cfg()
    };
    let rpc = Mock::new().with(ADDR, 1..=150);
    let b1 = block_on(poll_with(&rpc, &c, Some(&cursor_at(ADDR, 0)), &mut mem)).unwrap();
    let rpc = Mock::new().with(ADDR, 1..=160);
    let b2 = block_on(poll_with(&rpc, &c, Some(&cursor_from(b1)), &mut mem)).unwrap();
    assert_eq!(b2.records.len(), 50);
    let b3 = block_on(poll_with(&rpc, &c, Some(&cursor_from(b2)), &mut mem)).unwrap();
    assert_eq!(hashes(&b3), (151..=160).map(sig).collect::<Vec<_>>());
}

#[test]
fn a_transaction_on_two_addresses_is_recorded_once_even_when_one_is_capped() {
    let rpc = Mock::new().with(ADDR, 1..=5).with(ADDR2, 5..=5);
    let c = SolanaConfig {
        addresses: vec![ADDR.into(), ADDR2.into()],
        max_signatures: 2,
        ..cfg()
    };
    let mut mem = Memory::default();
    let mut cur = Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(ADDR.into(), mark(0)), (ADDR2.into(), mark(4))]),
    };
    let mut all = vec![];
    for _ in 0..10 {
        let b = block_on(poll_with(&rpc, &c, Some(&cur), &mut mem)).unwrap();
        all.extend(hashes(&b));
        let more = b.more;
        cur = cursor_from(b);
        if !more {
            break;
        }
    }
    let mut sorted = all.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(all.len(), 5, "{all:?}");
    assert_eq!(sorted, (1..=5).map(sig).collect::<Vec<_>>());
}
