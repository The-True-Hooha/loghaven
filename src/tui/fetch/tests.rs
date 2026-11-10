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
    let Msg::Records(first) = next(&mut rx).await else {
        panic!("expected records")
    };
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
    req_tx.send(Request::Trace("0xabc".into())).unwrap();
    loop {
        if let Msg::Trace(hash, result) = next(&mut rx).await {
            assert_eq!(hash, "0xabc");
            let recs = result.unwrap();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].id, "web-tx");
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
    req_tx.send(Request::Trace("0xabc".into())).unwrap();
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
