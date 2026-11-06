use super::*;
use std::cell::RefCell;

const ADDR: &str = "0x00000000000000000000000000000000000000aa";
const TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

struct Mock {
    head: u64,
    logs: Vec<Value>,
    calls: RefCell<Vec<(String, Value)>>,
    fail_blocks: bool,
    max_span: Option<u64>,
}

impl Mock {
    fn new(head: u64, logs: Vec<Value>) -> Self {
        Self {
            head,
            logs,
            calls: RefCell::new(vec![]),
            fail_blocks: false,
            max_span: None,
        }
    }
}

impl Rpc for Mock {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.calls
            .borrow_mut()
            .push((method.into(), params.clone()));
        match method {
            "eth_blockNumber" => Ok(json!(format!("{:#x}", self.head))),
            "eth_getLogs" => {
                let from = quantity(&params[0]["fromBlock"], "from")?;
                let to = quantity(&params[0]["toBlock"], "to")?;
                if self.max_span.is_some_and(|m| to - from >= m) {
                    return Err(LogHavenError::Daemon(
                        "rpc eth_getLogs error: query returned more than 10000 results".into(),
                    ));
                }
                let hit: Vec<_> = self
                    .logs
                    .iter()
                    .filter(|l| {
                        let b = quantity(&l["blockNumber"], "b").unwrap();
                        (from..=to).contains(&b)
                    })
                    .cloned()
                    .collect();
                Ok(Value::Array(hit))
            }
            "eth_getBlockByNumber" if self.fail_blocks => Err(LogHavenError::Daemon(
                "rpc eth_getBlockByNumber error: boom".into(),
            )),
            "eth_getBlockByNumber" => {
                let n = quantity(&params[0], "n")?;
                Ok(json!({"timestamp": format!("{:#x}", 1_700_000_000 + n)}))
            }
            other => panic!("unexpected rpc {other}"),
        }
    }
}

fn log(block: u64, index: u64, tx: &str) -> Value {
    json!({
        "address": ADDR,
        "topics": [TOPIC],
        "data": "0x01",
        "blockNumber": format!("{block:#x}"),
        "logIndex": format!("{index:#x}"),
        "transactionHash": tx,
        "removed": false,
    })
}

fn cfg() -> EthereumConfig {
    EthereumConfig {
        rpc_url: "http://127.0.0.1:1".into(),
        ws_url: None,
        app: "ethereum".into(),
        addresses: vec![ADDR.into()],
        topics: vec![],
        poll_interval_secs: 12,
        confirmations: 6,
        max_block_range: 1000,
        start_block: None,
    }
}

fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

#[test]
fn records_are_normalized_and_carry_tx_hash() {
    let rpc = Mock::new(120, vec![log(100, 3, "0xabc")]);
    let c = EthereumConfig {
        start_block: Some(90),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    assert_eq!(b.records.len(), 1);
    let r = &b.records[0];
    assert_eq!(r.tx_hash.as_deref(), Some("0xabc"));
    assert_eq!(r.chain.as_deref(), Some("ethereum"));
    assert_eq!(r.source, "ethereum");
    assert!(r.message.contains(ADDR) && r.message.contains(TOPIC));
    let m = r.metadata.as_ref().unwrap();
    assert_eq!(m["block_number"], 100);
    assert_eq!(m["block_timestamp"], 1_700_000_100u64);
    assert_eq!(m["log_index"], 3);
    assert_eq!(m["data"], "0x01");
    assert_eq!(b.next_block, 115);
    assert!(!b.more);
}

#[test]
fn only_confirmed_blocks_are_read() {
    // head 120, 6 confirmations: block 114 is the newest allowed, 115 is not
    let rpc = Mock::new(120, vec![log(114, 0, "0xa"), log(115, 0, "0xb")]);
    let c = EthereumConfig {
        start_block: Some(110),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    let txs: Vec<_> = b
        .records
        .iter()
        .map(|r| r.tx_hash.clone().unwrap())
        .collect();
    assert_eq!(txs, ["0xa"]);
    assert_eq!(b.next_block, 115);
}

#[test]
fn caught_up_cursor_reads_nothing_and_keeps_cursor() {
    let rpc = Mock::new(120, vec![log(114, 0, "0xa")]);
    let b = block_on(poll(&rpc, &cfg(), Some(115))).unwrap();
    assert!(b.records.is_empty());
    assert_eq!(b.next_block, 115);
    assert!(
        rpc.calls
            .borrow()
            .iter()
            .all(|(m, _)| m == "eth_blockNumber")
    );
}

#[test]
fn first_run_without_start_block_begins_at_newest_confirmed_block() {
    let rpc = Mock::new(
        120,
        vec![
            log(50, 0, "0xold"),
            log(113, 0, "0xnot"),
            log(114, 0, "0xnew"),
        ],
    );
    let b = block_on(poll(&rpc, &cfg(), None)).unwrap();
    let txs: Vec<_> = b
        .records
        .iter()
        .map(|r| r.tx_hash.clone().unwrap())
        .collect();
    assert_eq!(txs, ["0xnew"]);
}

#[test]
fn large_gap_is_read_in_ranges_without_gaps_or_repeats() {
    let rpc = Mock::new(
        1_000,
        vec![log(10, 0, "0xa"), log(25, 0, "0xb"), log(30, 0, "0xc")],
    );
    let c = EthereumConfig {
        max_block_range: 20,
        ..cfg()
    };
    let mut cursor = Some(10);
    let mut seen = vec![];
    for _ in 0..3 {
        let b = block_on(poll(&rpc, &c, cursor)).unwrap();
        assert!(b.more);
        seen.extend(b.records.iter().map(|r| r.tx_hash.clone().unwrap()));
        cursor = Some(b.next_block);
    }
    assert_eq!(seen, ["0xa", "0xb", "0xc"]);
    assert_eq!(cursor, Some(70));
}

#[test]
fn logs_are_ordered_by_block_then_index() {
    let rpc = Mock::new(
        200,
        vec![log(20, 2, "0xc"), log(10, 5, "0xb"), log(10, 1, "0xa")],
    );
    let c = EthereumConfig {
        start_block: Some(0),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    let txs: Vec<_> = b
        .records
        .iter()
        .map(|r| r.tx_hash.clone().unwrap())
        .collect();
    assert_eq!(txs, ["0xa", "0xb", "0xc"]);
}

#[test]
fn filter_carries_addresses_and_topic_alternatives() {
    let rpc = Mock::new(120, vec![]);
    let c = EthereumConfig {
        topics: vec![TOPIC.into()],
        start_block: Some(100),
        ..cfg()
    };
    block_on(poll(&rpc, &c, None)).unwrap();
    let calls = rpc.calls.borrow();
    let (_, params) = calls.iter().find(|(m, _)| m == "eth_getLogs").unwrap();
    assert_eq!(params[0]["address"], json!([ADDR]));
    assert_eq!(params[0]["topics"], json!([[TOPIC]]));
    assert_eq!(params[0]["fromBlock"], "0x64");
}

#[test]
fn rpc_failure_for_block_time_fails_the_poll() {
    let mut rpc = Mock::new(120, vec![log(100, 0, "0xa")]);
    rpc.fail_blocks = true;
    let c = EthereumConfig {
        start_block: Some(90),
        ..cfg()
    };
    assert!(block_on(poll(&rpc, &c, None)).is_err());
}

#[test]
fn malformed_log_is_an_error_not_a_skip() {
    let mut bad = log(100, 0, "0xa");
    bad.as_object_mut().unwrap().remove("transactionHash");
    let rpc = Mock::new(120, vec![bad]);
    let c = EthereumConfig {
        start_block: Some(90),
        ..cfg()
    };
    assert!(block_on(poll(&rpc, &c, None)).is_err());
}

#[test]
fn cursor_file_round_trips_and_garbage_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("ethereum.cursor");
    assert_eq!(read_cursor(&p).unwrap(), None);
    write_cursor(&p, 42).unwrap();
    assert_eq!(read_cursor(&p).unwrap(), Some(42));
    write_cursor(&p, 43).unwrap();
    assert_eq!(read_cursor(&p).unwrap(), Some(43));
    for bad in ["not a number", ""] {
        std::fs::write(&p, bad).unwrap();
        assert!(read_cursor(&p).is_err(), "{bad:?}");
    }
}

#[test]
fn quantity_rejects_non_hex() {
    assert_eq!(quantity(&json!("0x10"), "q").unwrap(), 16);
    for bad in [json!("16"), json!(16), json!("0xzz"), json!(null)] {
        assert!(quantity(&bad, "q").is_err(), "{bad}");
    }
}

#[test]
fn validation_rejects_bad_settings() {
    let ok = cfg();
    assert!(crate::config::validate_ethereum(&ok).is_ok());
    let c = |f: &dyn Fn(&mut EthereumConfig)| {
        let mut e = cfg();
        f(&mut e);
        crate::config::validate_ethereum(&e).is_err()
    };
    assert!(c(&|e| e.rpc_url = "ftp://x".into()));
    assert!(c(&|e| e.addresses.clear()));
    assert!(c(&|e| e.addresses = vec!["0x12".into()]));
    assert!(c(&|e| e.topics = vec!["0xabc".into()]));
    assert!(c(&|e| e.poll_interval_secs = 0));
    assert!(c(&|e| e.max_block_range = 0));
    assert!(c(&|e| e.max_block_range = 10_001));
    assert!(c(&|e| e.app = "../x".into()));
}

mod http {
    use super::*;
    use axum::{Json, Router, routing::post};

    async fn serve(handler: Router) -> String {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/secret-key", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, handler).await.unwrap() });
        url
    }

    #[tokio::test]
    async fn http_rpc_returns_result_and_surfaces_rpc_errors() {
        let app = Router::new().route(
            "/secret-key",
            post(|Json(req): Json<Value>| async move {
                if req["method"] == "eth_blockNumber" {
                    Json(json!({"jsonrpc":"2.0","id":1,"result":"0x2a"}))
                } else {
                    Json(json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"range too large"}}))
                }
            }),
        );
        let rpc = HttpRpc::new(&serve(app).await).unwrap();
        assert_eq!(
            rpc.call("eth_blockNumber", json!([])).await.unwrap(),
            json!("0x2a")
        );
        let e = rpc
            .call("eth_getLogs", json!([]))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("range too large"), "{e}");
    }

    #[tokio::test]
    async fn http_rpc_errors_never_contain_the_url() {
        let app = Router::new().route(
            "/secret-key",
            post(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no") }),
        );
        let url = serve(app).await;
        let rpc = HttpRpc::new(&url).unwrap();
        let e = rpc
            .call("eth_blockNumber", json!([]))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("500") && !e.contains("secret-key"), "{e}");

        let dead = HttpRpc::new("http://127.0.0.1:1/secret-key").unwrap();
        let e = dead
            .call("eth_blockNumber", json!([]))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            !e.contains("secret-key") && !e.contains("127.0.0.1:1/"),
            "{e}"
        );
    }
}

#[test]
fn oversized_event_data_is_cut_and_flagged() {
    let mut big = log(100, 0, "0xa");
    big["data"] = json!(format!("0x{}", "ab".repeat(40_000)));
    let rpc = Mock::new(120, vec![big]);
    let c = EthereumConfig {
        start_block: Some(90),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    let m = b.records[0].metadata.as_ref().unwrap();
    assert_eq!(m["data"].as_str().unwrap().len(), MAX_DATA_HEX);
    assert_eq!(m["data_len"], 80_002);
    assert_eq!(m["data_truncated"], true);
    assert!(serde_json::to_vec(&b.records[0]).unwrap().len() < 10_000);
}

#[test]
fn removed_logs_are_not_recorded() {
    let mut gone = log(100, 0, "0xgone");
    gone["removed"] = json!(true);
    let rpc = Mock::new(120, vec![gone, log(100, 1, "0xkept")]);
    let c = EthereumConfig {
        start_block: Some(90),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    assert_eq!(b.records.len(), 1);
    assert_eq!(b.records[0].tx_hash.as_deref(), Some("0xkept"));
}

#[test]
fn cursor_far_ahead_of_the_chain_is_an_error() {
    let rpc = Mock::new(120, vec![]);
    let e = block_on(poll(&rpc, &cfg(), Some(50_000_000)))
        .err()
        .unwrap()
        .to_string();
    assert!(e.contains("far ahead"), "{e}");
    // a few blocks ahead is normal (caught up, or a node slightly behind)
    assert!(block_on(poll(&rpc, &cfg(), Some(120))).is_ok());
}

#[test]
fn hostile_head_does_not_overflow() {
    let rpc = Mock::new(u64::MAX, vec![]);
    let c = EthereumConfig {
        confirmations: 0,
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, Some(u64::MAX - 5))).unwrap();
    assert_eq!(b.next_block, u64::MAX);
}

#[test]
fn too_many_results_halves_the_range_until_it_fits() {
    let mut rpc = Mock::new(1_000, vec![log(10, 0, "0xa")]);
    rpc.max_span = Some(300);
    let c = EthereumConfig {
        start_block: Some(0),
        ..cfg()
    };
    let b = block_on(poll(&rpc, &c, None)).unwrap();
    assert_eq!(b.records.len(), 1);
    assert!(b.more);
    assert!(b.next_block > 1 && b.next_block <= 300, "{}", b.next_block);
}

#[test]
fn a_single_unreadable_block_is_a_clear_error() {
    let mut rpc = Mock::new(1_000, vec![]);
    rpc.max_span = Some(0);
    let c = EthereumConfig {
        start_block: Some(5),
        ..cfg()
    };
    let e = block_on(poll(&rpc, &c, None)).err().unwrap().to_string();
    assert!(e.contains("block 5 cannot be read"), "{e}");
}

#[test]
fn quantity_error_quotes_at_most_a_short_prefix() {
    let long = json!("x".repeat(10_000));
    let e = quantity(&long, "q").unwrap_err().to_string();
    assert!(e.len() < 300, "{}", e.len());
}
