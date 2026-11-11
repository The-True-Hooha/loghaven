//! JSON-RPC over HTTP, shared by the chain adapters.

use crate::error::{LogHavenError, Result};
use serde_json::{Value, json};
use std::time::Duration;

/// Largest reply body read from a node; a hostile or broken node cannot make the watcher buffer more.
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// Characters of a node's own error text shown in ours.
const MAX_NODE_TEXT: usize = 200;

/// A node's text, safe to print: control characters (terminal escapes) removed, length bounded.
fn node_text(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control())
        .take(MAX_NODE_TEXT)
        .collect()
}

fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| LogHavenError::Daemon(e.to_string()))
}

async fn body_json(mut resp: reqwest::Response, what: &str) -> Result<Value> {
    let mut body: Vec<u8> = vec![];
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| LogHavenError::Daemon(format!("{what} failed: {}", e.without_url())))?
    {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(LogHavenError::Daemon(format!(
                "{what}: reply is larger than {} MiB",
                MAX_BODY_BYTES >> 20
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|_| LogHavenError::Daemon(format!("{what}: reply is not JSON")))
}

fn http_failure(what: &str, status: reqwest::StatusCode) -> LogHavenError {
    let msg = format!("{what} failed: HTTP {status}");
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        LogHavenError::RateLimited(msg)
    } else {
        LogHavenError::Daemon(msg)
    }
}

pub trait Rpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value>;
}

pub struct HttpRpc {
    url: String,
    client: reqwest::Client,
}

impl HttpRpc {
    pub fn new(url: &str) -> Result<Self> {
        Ok(Self {
            url: url.to_string(),
            client: client()?,
        })
    }
}

impl Rpc for HttpRpc {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let what = format!("rpc {method}");
        // RPC URLs often embed an API key, so the URL is kept out of every error.
        let resp = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| LogHavenError::Daemon(format!("{what} failed: {}", e.without_url())))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(http_failure(&what, status));
        }
        let mut reply = body_json(resp, &what).await?;
        if let Some(err) = reply.get("error") {
            return Err(LogHavenError::Daemon(format!(
                "{what} error: {}",
                node_text(
                    err.get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                )
            )));
        }
        reply
            .get_mut("result")
            .map(Value::take)
            .ok_or_else(|| LogHavenError::Daemon(format!("{what}: reply has no result")))
    }
}

/// Plain `GET` returning JSON, for REST nodes such as Horizon.
pub trait Get {
    /// `None` when the node answers 404.
    async fn get(&self, path: &str) -> Result<Option<Value>>;
}

pub struct HttpGet {
    base: String,
    client: reqwest::Client,
}

impl HttpGet {
    pub fn new(base: &str) -> Result<Self> {
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            client: client()?,
        })
    }
}

impl Get for HttpGet {
    async fn get(&self, path: &str) -> Result<Option<Value>> {
        let what = format!("GET {}", path.split('?').next().unwrap_or(path));
        let resp = self
            .client
            .get(format!("{}{path}", self.base))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| LogHavenError::Daemon(format!("{what} failed: {}", e.without_url())))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(http_failure(&what, status));
        }
        body_json(resp, &what).await.map(Some)
    }
}

#[cfg(test)]
mod tests {
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

    #[tokio::test]
    async fn node_error_text_is_bounded_and_has_no_control_characters() {
        let long = format!("\u{1b}[31m{}", "x".repeat(5_000));
        let app = Router::new().route(
            "/secret-key",
            post(move || {
                let long = long.clone();
                async move { Json(json!({"error": {"message": long}})) }
            }),
        );
        let rpc = HttpRpc::new(&serve(app).await).unwrap();
        let e = rpc.call("m", json!([])).await.unwrap_err().to_string();
        assert!(!e.contains('\u{1b}') && e.len() < 400, "{}", e.len());
    }

    #[tokio::test]
    async fn http_429_is_a_rate_limit_not_a_failure() {
        use axum::routing::get;
        let app = Router::new()
            .route(
                "/secret-key",
                post(|| async { axum::http::StatusCode::TOO_MANY_REQUESTS }),
            )
            .route(
                "/secret-key/x",
                get(|| async { axum::http::StatusCode::TOO_MANY_REQUESTS }),
            );
        let base = serve(app).await;
        let e = HttpRpc::new(&base).unwrap().call("m", json!([])).await;
        assert!(matches!(e, Err(LogHavenError::RateLimited(_))), "{e:?}");
        let e = HttpGet::new(&base).unwrap().get("/x").await;
        assert!(matches!(e, Err(LogHavenError::RateLimited(_))), "{e:?}");
    }

    #[tokio::test]
    async fn an_oversized_reply_is_refused() {
        let app = Router::new().route(
            "/secret-key",
            post(|| async { "a".repeat(MAX_BODY_BYTES + 1) }),
        );
        let rpc = HttpRpc::new(&serve(app).await).unwrap();
        let e = rpc.call("m", json!([])).await.unwrap_err().to_string();
        assert!(e.contains("larger than"), "{e}");
    }

    #[tokio::test]
    async fn http_get_maps_404_to_none_other_errors_to_status_and_hides_the_url() {
        use axum::routing::get;
        let app = Router::new()
            .route("/secret-key/ok", get(|| async { Json(json!({"a": 1})) }))
            .route(
                "/secret-key/bad",
                get(|| async { axum::http::StatusCode::BAD_GATEWAY }),
            );
        let base = serve(app).await;
        let g = HttpGet::new(&format!("{base}/")).unwrap();
        assert_eq!(g.get("/ok?x=1").await.unwrap(), Some(json!({"a": 1})));
        assert_eq!(g.get("/missing").await.unwrap(), None);
        let e = g.get("/bad").await.unwrap_err().to_string();
        assert!(e.contains("502") && !e.contains("secret-key/"), "{e}");
        let dead = HttpGet::new("http://127.0.0.1:1/secret-key").unwrap();
        let e = dead.get("/x").await.unwrap_err().to_string();
        assert!(
            !e.contains("secret-key") && !e.contains("127.0.0.1:1/"),
            "{e}"
        );
    }
}
