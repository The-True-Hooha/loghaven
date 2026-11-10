use super::*;
use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::unbounded_channel;

fn record(app: &str, id: &str, ts: i64, tx: Option<&str>) -> Value {
    json!({"id": id, "app": app, "timestamp_ms": ts, "level": "info", "source": "s",
           "message": id, "trace_id": null, "span_id": null, "chain": null,
           "tx_hash": tx, "tags": null, "metadata": null})
}

type Seen = Arc<Mutex<Vec<HashMap<String, String>>>>;

/// A daemon stand-in with auth off. `fail_app` answers 500 for that app.
async fn daemon(fail_app: Option<&'static str>) -> (u16, Seen) {
    let seen: Seen = Arc::default();
    let log = seen.clone();
    let app = Router::new().route(
        "/logs",
        get(move |Query(q): Query<HashMap<String, String>>| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(q.clone());
                let app = q["app"].clone();
                if fail_app == Some(app.as_str()) {
                    return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                }
                let recs = match q.get("tx_hash").map(String::as_str) {
                    Some("0xabc") => vec![record(&app, &format!("{app}-tx"), 50, Some("0xabc"))],
                    Some(_) => vec![],
                    None => vec![
                        record(&app, &format!("{app}-2"), 20, None),
                        record(&app, &format!("{app}-1"), 10, None),
                    ],
                };
                let from: i64 = q.get("from_ms").map_or(i64::MIN, |v| v.parse().unwrap());
                let recs: Vec<Value> = recs
                    .into_iter()
                    .filter(|r| r["timestamp_ms"].as_i64().unwrap() >= from)
                    .collect();
                Ok(Json(json!({ "records": recs })))
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (port, seen)
}

fn cfg(port: u16) -> Config {
    let mut c = crate::config::default_config();
    c.auth.require_auth = false;
    c.auth.query_port = port;
    c
}

async fn next(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Msg>) -> Msg {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .expect("closed")
}

#[tokio::test]
async fn the_first_poll_loads_each_apps_recent_records_and_later_polls_do_not_repeat_them() {
    let (port, seen) = daemon(None).await;
    let (tx, mut rx) = unbounded_channel();
    let (_req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(
        cfg(port),
        vec!["web".into(), "api".into()],
        None,
        tx,
        req_rx,
    ));
    let mut first = vec![];
    while first.len() < 4 {
        if let Msg::Records(r) = next(&mut rx).await {
            first.extend(r);
        }
    }
    let mut ids: Vec<_> = first.iter().map(|r| r.id.clone()).collect();
    ids.sort();
    assert_eq!(ids, ["api-1", "api-2", "web-1", "web-2"]);
    tokio::time::sleep(Duration::from_millis(2300)).await;
    while let Ok(m) = rx.try_recv() {
        assert!(!matches!(m, Msg::Records(_)), "records were repeated");
    }
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0]["limit"], INITIAL.to_string());
    assert!(!seen[0].contains_key("from_ms"));
    assert!(
        seen.iter().skip(2).all(|q| q.contains_key("from_ms")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_trace_request_gathers_the_hash_from_every_app_and_a_dead_app_does_not_hide_the_rest() {
    let (port, _) = daemon(Some("api")).await;
    let (tx, mut rx) = unbounded_channel();
    let (req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(
        cfg(port),
        vec!["web".into(), "api".into()],
        None,
        tx,
        req_rx,
    ));
    req_tx.send(Request::Trace(7, "0xabc".into())).unwrap();
    loop {
        if let Msg::Trace(id, result) = next(&mut rx).await {
            assert_eq!(id, 7);
            let (recs, note) = result.unwrap();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].id, "web-tx");
            assert!(
                note.unwrap().contains("incomplete: api"),
                "the dead app is named"
            );
            break;
        }
    }
}

#[tokio::test]
async fn a_trace_where_every_app_fails_reports_the_error() {
    let (port, _) = daemon(Some("web")).await;
    let (tx, mut rx) = unbounded_channel();
    let (req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(cfg(port), vec!["web".into()], None, tx, req_rx));
    req_tx.send(Request::Trace(7, "0xabc".into())).unwrap();
    loop {
        if let Msg::Trace(_, result) = next(&mut rx).await {
            assert!(result.unwrap_err().contains("web"));
            break;
        }
    }
}

#[tokio::test]
async fn an_unreachable_daemon_is_reported_once_and_cleared_when_it_returns() {
    let (tx, mut rx) = unbounded_channel();
    let (_req_tx, req_rx) = unbounded_channel();
    // nothing listens on port 1
    tokio::spawn(run(cfg(1), vec!["web".into()], None, tx, req_rx));
    let Msg::Status(s) = next(&mut rx).await else {
        panic!("expected a status")
    };
    assert!(s.contains("web") && s.contains("cannot reach"), "{s}");
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert!(rx.try_recv().is_err(), "the same error is not repeated");
}

#[tokio::test]
async fn the_fetcher_stops_when_the_ui_goes_away() {
    let (port, _) = daemon(None).await;
    let (tx, rx) = unbounded_channel();
    let (_req_tx, req_rx) = unbounded_channel();
    let task = tokio::spawn(run(cfg(port), vec!["web".into()], None, tx, req_rx));
    drop(rx);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
}

/// A daemon whose app `big` has `count` records, all newer than any lookback window,
/// honoring `limit`, so a poll that asks for too little loses the older ones.
async fn busy_daemon(count: usize) -> (u16, Seen) {
    let seen: Seen = Arc::default();
    let log = seen.clone();
    let now = now_ms();
    let app = Router::new().route(
        "/logs",
        get(move |Query(q): Query<HashMap<String, String>>| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(q.clone());
                let limit: usize = q["limit"].parse().unwrap();
                let recs: Vec<Value> = (0..count)
                    .rev()
                    .take(limit)
                    .map(|i| record("big", &format!("b{i:05}"), now + i as i64, None))
                    .collect();
                Json(json!({ "records": recs }))
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (port, seen)
}

async fn drain(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Msg>,
    secs: u64,
) -> (Vec<LogRecord>, Vec<String>) {
    let mut recs = vec![];
    let mut statuses = vec![];
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    while let Ok(Some(m)) = tokio::time::timeout_at(end, rx.recv()).await {
        match m {
            Msg::Records(r) => recs.extend(r),
            Msg::Status(s) => statuses.push(s),
            _ => {}
        }
    }
    (recs, statuses)
}

#[tokio::test]
async fn a_poll_that_fills_a_page_is_repeated_with_a_bigger_page_so_nothing_is_lost() {
    let (port, seen) = busy_daemon(INITIAL + 1_500).await;
    let (tx, mut rx) = unbounded_channel();
    let (_req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(cfg(port), vec!["big".into()], None, tx, req_rx));
    let (recs, statuses) = drain(&mut rx, 3).await;
    // first poll: newest 200; second poll: page full, so asked again with the larger size
    let limits: Vec<usize> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|q| q["limit"].parse().unwrap())
        .collect();
    assert_eq!(limits[0], INITIAL);
    assert!(
        limits.contains(&PAGE) && limits.contains(&MAX_PAGE),
        "{limits:?}"
    );
    assert_eq!(recs.len(), INITIAL + 1_500, "every record arrived once");
    assert!(statuses.iter().all(|s| s.is_empty()), "{statuses:?}");
}

#[tokio::test]
async fn a_page_that_is_full_even_at_the_largest_size_is_reported() {
    let (port, _) = busy_daemon(MAX_PAGE + 500).await;
    let (tx, mut rx) = unbounded_channel();
    let (_req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(cfg(port), vec!["big".into()], None, tx, req_rx));
    let (_, statuses) = drain(&mut rx, 3).await;
    assert!(
        statuses.iter().any(|s| s.contains("some may be missing")),
        "{statuses:?}"
    );
}

#[tokio::test]
async fn a_hung_app_does_not_delay_the_others_and_a_trace_does_not_stop_polling() {
    let slow = Router::new().route(
        "/logs",
        get(|Query(q): Query<HashMap<String, String>>| async move {
            if q["app"] == "slow" || q.contains_key("tx_hash") {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Json(json!({"records": [record(&q["app"], &format!("{}-1", q["app"]), 10, None)]}))
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(l, slow).await.unwrap() });
    let (tx, mut rx) = unbounded_channel();
    let (req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(
        cfg(port),
        vec!["slow".into(), "fast".into()],
        None,
        tx,
        req_rx,
    ));
    req_tx.send(Request::Trace(1, "0xabc".into())).unwrap();
    // "fast" arrives although "slow" and the trace both hang
    let first = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Msg::Records(r) = rx.recv().await.unwrap() {
                break r;
            }
        }
    })
    .await;
    assert!(
        first.is_ok(),
        "polling was blocked by a slow app or a trace"
    );
}

#[tokio::test]
async fn polls_and_traces_share_one_session_per_app() {
    use axum::routing::post;
    let sessions = Arc::new(Mutex::new(0usize));
    let counter = sessions.clone();
    let app = Router::new()
        .route(
            "/auth",
            post(move || {
                let counter = counter.clone();
                async move {
                    *counter.lock().unwrap() += 1;
                    Json(json!({"token_id": "t".repeat(64), "secret_hex": "00".repeat(32), "expires_in_secs": 3600}))
                }
            }),
        )
        .route("/logs", get(|| async { Json(json!({"records": []})) }));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(port);
    c.auth.require_auth = true;
    c.auth.keys_dir = dir.path().to_path_buf();
    crate::auth::keys::generate_keypair(
        &c.auth.keys_dir.join("private.pem"),
        &c.auth.keys_dir.join("public.pem"),
    )
    .unwrap();
    let (tx, mut rx) = unbounded_channel();
    let (req_tx, req_rx) = unbounded_channel();
    tokio::spawn(run(c, vec!["web".into()], None, tx, req_rx));
    tokio::time::sleep(Duration::from_millis(1300)).await;
    for id in 1..=3 {
        req_tx.send(Request::Trace(id, "0xabc".into())).unwrap();
    }
    let mut traces = 0;
    while traces < 3 {
        if let Msg::Trace(..) = next(&mut rx).await {
            traces += 1;
        }
    }
    assert_eq!(
        *sessions.lock().unwrap(),
        1,
        "one session reused by polling and three traces"
    );
}
