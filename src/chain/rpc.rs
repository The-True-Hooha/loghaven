//! JSON-RPC over HTTP, shared by the chain adapters.

use crate::error::{LogHavenError, Result};
use serde_json::{Value, json};
use std::time::Duration;

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
}
