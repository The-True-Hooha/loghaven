use super::*;
use std::cell::RefCell;

const A: &str = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7";
const B: &str = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H";
const PASSPHRASE: &str = "Test SDF Network ; September 2015";

fn hash(n: u64) -> String {
    format!("{n:064x}")
}

fn token(n: u64) -> u64 {
    n * 4096
}

struct Mock {
    /// Per account, the transaction numbers it has, ascending.
    txs: BTreeMap<String, Vec<u64>>,
    ledger_of: BTreeMap<u64, u64>,
    failed: BTreeSet<u64>,
    passphrase: String,
    max_page: usize,
    unordered: bool,
    calls: RefCell<Vec<String>>,
}

impl Mock {
    fn new() -> Self {
        Self {
            txs: BTreeMap::new(),
            ledger_of: BTreeMap::new(),
            failed: BTreeSet::new(),
            passphrase: PASSPHRASE.into(),
            max_page: usize::MAX,
            unordered: false,
            calls: RefCell::new(vec![]),
        }
    }

    fn with(mut self, account: &str, range: std::ops::RangeInclusive<u64>) -> Self {
        let list = self.txs.entry(account.into()).or_default();
        list.extend(range);
        list.sort();
        list.dedup();
        self
    }

    fn ledger(&self, n: u64) -> u64 {
        self.ledger_of.get(&n).copied().unwrap_or(100 + n)
    }

    fn count(&self, needle: &str) -> usize {
        self.calls
            .borrow()
            .iter()
            .filter(|c| c.contains(needle))
            .count()
    }

    fn json(&self, n: u64) -> Value {
        json!({
            "hash": hash(n),
            "paging_token": token(n).to_string(),
            "ledger": self.ledger(n),
            "created_at": "2024-01-01T00:00:00Z",
            "successful": !self.failed.contains(&n),
            "fee_charged": "100",
            "operation_count": 2,
            "source_account": A,
            "memo_type": "text",
            "memo": format!("memo {n}"),
        })
    }
}

fn query(path: &str, key: &str) -> Option<String> {
    path.split_once('?')?
        .1
        .split('&')
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")).map(str::to_string))
}

impl Get for Mock {
    async fn get(&self, path: &str) -> Result<Option<Value>> {
        self.calls.borrow_mut().push(path.into());
        if path == "/" {
            return Ok(Some(json!({"network_passphrase": self.passphrase})));
        }
        let account = path
            .strip_prefix("/accounts/")
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        let Some(list) = self.txs.get(account) else {
            return Ok(None);
        };
        assert_eq!(query(path, "include_failed").as_deref(), Some("true"));
        let limit: usize = query(path, "limit").unwrap().parse().unwrap();
        assert!(limit <= PAGE);
        let limit = limit.min(self.max_page);
        let mut ns: Vec<u64> = match query(path, "cursor") {
            Some(c) => list
                .iter()
                .copied()
                .filter(|n| token(*n) > c.parse().unwrap())
                .collect(),
            None => list.clone(),
        };
        if query(path, "order").as_deref() == Some("desc") {
            ns.reverse();
        }
        ns.truncate(limit);
        if self.unordered {
            ns.reverse();
        }
        let records: Vec<Value> = ns.iter().map(|n| self.json(*n)).collect();
        Ok(Some(json!({"_embedded": {"records": records}})))
    }
}

fn cfg() -> StellarConfig {
    StellarConfig {
        horizon_url: "http://127.0.0.1:1".into(),
        app: "stellar".into(),
        accounts: vec![A.into()],
        poll_interval_secs: 5,
        max_records: 100,
    }
}

fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

fn cursor_at(account: &str, n: u64) -> Cursor {
    Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(account.to_string(), token(n))]),
    }
}

fn cursor_from(b: Batch) -> Cursor {
    Cursor {
        binding: "b".into(),
        last: b.last,
    }
}

fn hashes(b: &Batch) -> Vec<String> {
    b.records.iter().filter_map(|r| r.tx_hash.clone()).collect()
}

#[test]
fn first_run_starts_at_the_newest_transaction_and_records_nothing() {
    let rpc = Mock::new().with(A, 1..=50);
    let b = block_on(poll(&rpc, &cfg(), None)).unwrap();
    assert!(b.records.is_empty());
    assert_eq!(b.last[A], token(50));
}

#[test]
fn an_unknown_or_empty_account_saves_no_entry_and_is_read_from_the_start_later() {
    let rpc = Mock::new();
    let b = block_on(poll(&rpc, &cfg(), None)).unwrap();
    assert!(b.records.is_empty() && b.last.is_empty());
    let rpc = Mock::new().with(A, 1..=2);
    let empty = Cursor {
        binding: "b".into(),
        last: BTreeMap::new(),
    };
    let b = block_on(poll(&rpc, &cfg(), Some(&empty))).unwrap();
    assert_eq!(hashes(&b), [hash(1), hash(2)]);
}

#[test]
fn new_transactions_are_recorded_oldest_first_with_the_hash_as_tx_hash() {
    let mut rpc = Mock::new().with(A, 1..=53);
    rpc.failed.insert(52);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(A, 50)))).unwrap();
    assert_eq!(hashes(&b), [hash(51), hash(52), hash(53)]);
    assert_eq!(b.last[A], token(53));
    assert!(!b.more);

    let r = &b.records[0];
    assert_eq!((r.level.as_str(), r.source.as_str()), ("info", "stellar"));
    assert_eq!(r.chain.as_deref(), Some("stellar"));
    assert_eq!(r.message, format!("tx {} ledger 151 ok", hash(51)));
    let m = r.metadata.as_ref().unwrap();
    assert_eq!(m["account"], A);
    assert_eq!(m["ledger"], 151);
    assert_eq!(m["fee_charged"], 100);
    assert_eq!(m["operation_count"], 2);
    assert_eq!(m["memo"], "memo 51");
    assert_eq!(m["successful"], true);

    let failed = &b.records[1];
    assert_eq!(failed.level, "error");
    assert!(failed.message.ends_with("failed"));
    assert_eq!(failed.metadata.as_ref().unwrap()["successful"], false);
}

#[test]
fn caught_up_records_nothing_and_keeps_the_cursor() {
    let rpc = Mock::new().with(A, 1..=50);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(A, 50)))).unwrap();
    assert!(b.records.is_empty() && !b.more);
    assert_eq!(b.last[A], token(50));
}

#[test]
fn a_backlog_is_taken_in_order_over_several_polls_reading_only_what_it_records() {
    let rpc = Mock::new().with(A, 1..=250);
    let c = StellarConfig {
        max_records: 100,
        ..cfg()
    };
    let b1 = block_on(poll(&rpc, &c, Some(&cursor_at(A, 0)))).unwrap();
    assert_eq!(hashes(&b1), (1..=100).map(hash).collect::<Vec<_>>());
    assert!(b1.more);
    let b2 = block_on(poll(&rpc, &c, Some(&cursor_from(b1)))).unwrap();
    assert_eq!(hashes(&b2), (101..=200).map(hash).collect::<Vec<_>>());
    assert!(b2.more);
    let b3 = block_on(poll(&rpc, &c, Some(&cursor_from(b2)))).unwrap();
    assert_eq!(hashes(&b3), (201..=250).map(hash).collect::<Vec<_>>());
    assert!(!b3.more);
}

#[test]
fn a_backlog_of_exactly_max_records_is_not_reported_as_more() {
    let rpc = Mock::new().with(A, 1..=100);
    let b = block_on(poll(&rpc, &cfg(), Some(&cursor_at(A, 0)))).unwrap();
    assert_eq!(b.records.len(), 100);
    assert!(!b.more);
}

#[test]
fn pages_longer_than_horizons_maximum_are_read_across_calls_without_gaps() {
    let rpc = Mock::new().with(A, 1..=700);
    let c = StellarConfig {
        max_records: 600,
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, Some(&cursor_at(A, 0)))).unwrap();
    assert_eq!(hashes(&b), (1..=600).map(hash).collect::<Vec<_>>());
    assert!(b.more);
    assert!(rpc.count("order=asc") >= 4);
}

#[test]
fn a_node_that_returns_smaller_pages_loses_nothing() {
    let mut rpc = Mock::new().with(A, 1..=250);
    rpc.max_page = 30;
    let c = StellarConfig {
        max_records: 100,
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, Some(&cursor_at(A, 0)))).unwrap();
    assert_eq!(hashes(&b), (1..=100).map(hash).collect::<Vec<_>>());
    assert!(b.more);
}

#[test]
fn transactions_out_of_order_are_an_error_not_a_cursor() {
    let mut rpc = Mock::new().with(A, 1..=5);
    rpc.unordered = true;
    let e = block_on(poll(&rpc, &cfg(), Some(&cursor_at(A, 0))))
        .err()
        .unwrap();
    assert!(e.to_string().contains("ascending"), "{e}");
}

#[test]
fn a_transaction_on_two_accounts_is_recorded_once_even_when_one_is_capped() {
    let mut rpc = Mock::new().with(A, 1..=5).with(B, 5..=5);
    rpc.ledger_of.insert(5, 105);
    let c = StellarConfig {
        accounts: vec![A.into(), B.into()],
        max_records: 2,
        ..cfg()
    };
    let mut cur = Cursor {
        binding: "b".into(),
        last: BTreeMap::from([(A.into(), token(0)), (B.into(), token(4))]),
    };
    let mut all = vec![];
    for _ in 0..10 {
        let b = block_on(poll(&rpc, &c, Some(&cur))).unwrap();
        all.extend(hashes(&b));
        let more = b.more;
        cur = cursor_from(b);
        if !more {
            break;
        }
    }
    assert_eq!(all, (1..=5).map(hash).collect::<Vec<_>>());
}

#[test]
fn malformed_transactions_from_the_node_are_errors() {
    let ok = Mock::new().with(A, 1..=1).json(1);
    assert!(parse_tx(&ok).is_ok());
    for (key, bad) in [
        ("hash", json!("xyz")),
        ("hash", json!(5)),
        ("paging_token", json!("abc")),
        ("paging_token", json!(null)),
        ("ledger", json!("x")),
    ] {
        let mut v = ok.clone();
        v[key] = bad.clone();
        assert!(parse_tx(&v).is_err(), "{key}={bad}");
    }
    let mut v = ok.clone();
    v["hash"] = json!("AB".repeat(32));
    assert_eq!(
        parse_tx(&v).unwrap().hash,
        "ab".repeat(32),
        "stored lowercase"
    );
}

#[test]
fn memo_is_cut_and_cleaned_of_control_characters() {
    let mut v = Mock::new().json(1);
    v["memo"] = json!(format!("\u{1b}[31m{}", "m".repeat(1_000)));
    let tx = parse_tx(&v).unwrap();
    let memo = tx.memo.unwrap();
    assert_eq!(memo.len(), MAX_MEMO_CHARS);
    assert!(!memo.contains('\u{1b}'));
}

#[test]
fn cursor_text_round_trips_and_garbage_is_an_error() {
    let c = Cursor {
        binding: "abc".into(),
        last: BTreeMap::from([(A.into(), 7), (B.into(), 9)]),
    };
    let back = Cursor::parse(&c.render()).unwrap();
    assert_eq!((back.binding.as_str(), back.last), ("abc", c.last));
    assert!(Cursor::parse("abc\n").unwrap().last.is_empty());
    let no_token = format!("abc\n{A}\n");
    let bad_token = format!("abc\n{A} x\n");
    let extra = format!("abc\n{A} 5 extra\n");
    let bad_account = "abc\nGSHORT 5\n".to_string();
    for bad in [
        "",
        "\n",
        no_token.as_str(),
        bad_token.as_str(),
        extra.as_str(),
        bad_account.as_str(),
    ] {
        assert!(Cursor::parse(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn binding_changes_with_network_and_accounts_but_not_order() {
    let rpc = Mock::new();
    let b = |c: &StellarConfig| block_on(binding(&rpc, c)).unwrap();
    let base = b(&cfg());
    assert_eq!(base, b(&cfg()));
    let two = StellarConfig {
        accounts: vec![A.into(), B.into()],
        ..cfg()
    };
    assert_ne!(base, b(&two));
    let swapped = StellarConfig {
        accounts: vec![B.into(), A.into(), A.into()],
        ..cfg()
    };
    assert_eq!(b(&two), b(&swapped));
    let mut other = Mock::new();
    other.passphrase = "Public Global Stellar Network ; September 2015".into();
    assert_ne!(base, block_on(binding(&other, &cfg())).unwrap());
}

#[test]
fn a_cursor_saved_for_other_settings_is_refused() {
    let saved = Cursor {
        binding: "old".into(),
        last: BTreeMap::new(),
    };
    let e = check_binding(&saved, "new").unwrap_err().to_string();
    assert!(e.contains("another network"), "{e}");
    assert!(check_binding(&saved, "old").is_ok());
}

#[test]
fn watcher_starts_at_the_tip_then_follows_with_a_cursor_it_wrote() {
    let mut w = StellarWatcher::with_get(Mock::new().with(A, 1..=5), cfg());
    let first = block_on(w.step(None)).unwrap();
    assert!(first.records.is_empty());
    assert_eq!(Cursor::parse(&first.cursor).unwrap().last[A], token(5));

    let mut w = StellarWatcher::with_get(Mock::new().with(A, 1..=7), cfg());
    let next = block_on(w.step(Some(&first.cursor))).unwrap();
    assert_eq!(next.records.len(), 2);
    assert_eq!(Cursor::parse(&next.cursor).unwrap().last[A], token(7));
}

#[test]
fn watcher_refuses_a_foreign_cursor_and_garbage_with_config_errors() {
    let mut w = StellarWatcher::with_get(Mock::new(), cfg());
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
    assert!(crate::config::validate_stellar(&cfg()).is_ok());
    let c = |f: &dyn Fn(&mut StellarConfig)| {
        let mut s = cfg();
        f(&mut s);
        crate::config::validate_stellar(&s).is_err()
    };
    assert!(c(&|s| s.horizon_url = "ftp://x".into()));
    assert!(c(&|s| s.accounts.clear()));
    assert!(c(&|s| s.accounts = vec!["GSHORT".into()]));
    assert!(c(&|s| s.accounts = vec![A.to_lowercase()]));
    assert!(c(&|s| s.accounts = vec![format!("M{}", &A[1..])]));
    assert!(c(&|s| s.poll_interval_secs = 0));
    assert!(c(&|s| s.max_records = 0));
    assert!(c(&|s| s.max_records = 1_001));
    assert!(c(&|s| s.app = "../x".into()));
}
