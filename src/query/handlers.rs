use crate::auth::token::verify_jwt;
use crate::ipc::protocol::SessionData;
use crate::query::QueryState;
use crate::storage::query::{QueryFilter, query};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

pub fn routes() -> Router<QueryState> {
    Router::new()
        .route("/auth", post(post_auth))
        .route("/logs", get(get_logs))
}

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

// POST /auth — exchange RS256 JWT for session token
#[derive(Deserialize)]
pub struct AuthRequest {
    pub jwt: String,
}

pub async fn post_auth(
    State(state): State<QueryState>,
    Json(body): Json<AuthRequest>,
) -> Result<Json<SessionData>, StatusCode> {
    let pem = state
        .public_key_pem
        .as_deref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let claims = verify_jwt(&body.jwt, pem).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let remaining = claims.exp.saturating_sub(now);

    let (token_id, secret) = state
        .sessions
        .issue(&claims.sub)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(SessionData {
        token_id,
        secret_hex: hex::encode(secret),
        expires_in_secs: remaining,
    }))
}

// GET /logs?app=&level=&source=&from_ms=&to_ms=&text=&limit=
#[derive(Deserialize)]
pub struct LogsParams {
    pub app: String,
    pub level: Option<String>,
    pub source: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub text: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct LogsResponse {
    pub count: usize,
    pub records: Vec<crate::storage::record::LogRecord>,
}

pub async fn get_logs(
    State(state): State<QueryState>,
    headers: HeaderMap,
    Query(params): Query<LogsParams>,
) -> Result<Json<LogsResponse>, (StatusCode, String)> {
    if state.require_auth {
        let token = extract_bearer(&headers)
            .ok_or((StatusCode::UNAUTHORIZED, "missing Bearer token".into()))?;
        state
            .sessions
            .verify_session_token(&token)
            .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    }

    if let Err(e) = crate::storage::validate_app_name(&params.app) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid app: {}", e)));
    }

    let filter = QueryFilter {
        app: params.app,
        level: params.level,
        source: params.source,
        from_ms: params.from_ms,
        to_ms: params.to_ms,
        text: params.text,
        limit: params.limit.unwrap_or(1000),
    };

    let data_path = state.data_path.clone();
    let records = tokio::task::spawn_blocking(move || query(&data_path, &filter, None))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| match e {
            crate::error::LogHavenError::InvalidApp(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;

    Ok(Json(LogsResponse {
        count: records.len(),
        records,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::session::SessionStore;
    use std::sync::Arc;

    fn state() -> QueryState {
        QueryState {
            sessions: Arc::new(SessionStore::new(60)),
            public_key_pem: None,
            data_path: std::env::temp_dir().join(format!("lh-h-{}", std::process::id())),
            require_auth: false,
        }
    }

    fn params(app: &str) -> LogsParams {
        LogsParams {
            app: app.into(),
            level: None,
            source: None,
            from_ms: None,
            to_ms: None,
            text: None,
            limit: None,
        }
    }

    #[tokio::test]
    async fn logs_rejects_invalid_app_with_400() {
        for app in ["../etc", "", "a/b", "Web"] {
            let r = get_logs(State(state()), HeaderMap::new(), Query(params(app))).await;
            assert_eq!(r.err().unwrap().0, StatusCode::BAD_REQUEST, "{app:?}");
        }
    }

    #[tokio::test]
    async fn logs_unknown_valid_app_is_empty_200() {
        let r = get_logs(State(state()), HeaderMap::new(), Query(params("nope")))
            .await
            .unwrap();
        assert_eq!(r.0.count, 0);
    }
}
