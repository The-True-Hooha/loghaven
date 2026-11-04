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

const DEFAULT_LIMIT: usize = 1000;
const MAX_LIMIT: usize = 10_000;
const QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
    let (token_id, secret, expires_in_secs) = state
        .sessions
        .issue_capped(&claims.sub, claims.exp.saturating_sub(now))
        .map_err(|e| match e {
            crate::error::LogHavenError::InvalidApp(_) => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    Ok(Json(SessionData {
        token_id,
        secret_hex: hex::encode(secret),
        expires_in_secs,
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
    let session_app = if state.require_auth {
        let token = extract_bearer(&headers)
            .ok_or((StatusCode::UNAUTHORIZED, "missing Bearer token".into()))?;
        Some(
            state
                .sessions
                .verify_session_token(&token)
                .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?,
        )
    } else {
        None
    };

    if let Err(e) = crate::storage::validate_app_name(&params.app) {
        return Err((StatusCode::BAD_REQUEST, format!("invalid app: {}", e)));
    }

    if session_app.is_some_and(|app| app != params.app) {
        return Err((
            StatusCode::FORBIDDEN,
            "token is not authorized for this app".into(),
        ));
    }

    let limit = match params.limit {
        None => DEFAULT_LIMIT,
        Some(n) if (1..=MAX_LIMIT).contains(&n) => n,
        Some(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("limit must be between 1 and {MAX_LIMIT}"),
            ));
        }
    };
    if let (Some(from), Some(to)) = (params.from_ms, params.to_ms)
        && from > to
    {
        return Err((StatusCode::BAD_REQUEST, "from_ms is after to_ms".into()));
    }

    let filter = QueryFilter {
        app: params.app,
        level: params.level,
        source: params.source,
        from_ms: params.from_ms,
        to_ms: params.to_ms,
        text: params.text,
        limit,
    };

    let permit = state.query_slots.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "too many concurrent queries".into(),
        )
    })?;

    let data_path = state.data_path.clone();
    // the permit moves into the blocking job, so it is held until the scan really ends
    let job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        query(&data_path, &filter)
    });
    let records = tokio::time::timeout(QUERY_TIMEOUT, job)
        .await
        .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "query timed out".into()))?
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
        state_at("empty")
    }

    fn state_at(name: &str) -> QueryState {
        QueryState {
            sessions: Arc::new(SessionStore::new(60)),
            public_key_pem: None,
            data_path: std::env::temp_dir().join(format!("lh-h-{}-{}", name, std::process::id())),
            require_auth: false,
            query_slots: Arc::new(tokio::sync::Semaphore::new(
                crate::query::MAX_CONCURRENT_QUERIES,
            )),
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

    fn auth_state() -> (QueryState, String, String) {
        let mut st = state();
        st.require_auth = true;
        let (web, _, _) = st.sessions.issue("web").unwrap();
        let (api, _, _) = st.sessions.issue("api").unwrap();
        (st, web, api)
    }

    fn bearer(tok: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("authorization", format!("Bearer {tok}").parse().unwrap());
        h
    }

    #[tokio::test]
    async fn logs_requires_token_when_auth_enabled() {
        let (st, _, _) = auth_state();
        let r = get_logs(State(st.clone()), HeaderMap::new(), Query(params("web"))).await;
        assert_eq!(r.err().unwrap().0, StatusCode::UNAUTHORIZED);
        let r = get_logs(State(st), bearer("bogus"), Query(params("web"))).await;
        assert_eq!(r.err().unwrap().0, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logs_token_is_scoped_to_its_app() {
        let (st, web, _api) = auth_state();
        let r = get_logs(State(st.clone()), bearer(&web), Query(params("api"))).await;
        assert_eq!(r.err().unwrap().0, StatusCode::FORBIDDEN);
        let ok = get_logs(State(st), bearer(&web), Query(params("web")))
            .await
            .unwrap();
        assert_eq!(ok.0.count, 0);
    }

    #[tokio::test]
    async fn logs_unknown_valid_app_is_empty_200() {
        let r = get_logs(State(state()), HeaderMap::new(), Query(params("nope")))
            .await
            .unwrap();
        assert_eq!(r.0.count, 0);
    }

    fn seed(st: &QueryState, app: &str, recs: &[(i64, &str)]) {
        let _ = std::fs::remove_dir_all(&st.data_path);
        let records: Vec<_> = recs
            .iter()
            .map(|(ts, m)| {
                let mut r = crate::storage::record::LogRecord::new(app, "info", "src", *m);
                r.timestamp_ms = *ts;
                r
            })
            .collect();
        let dir = st
            .data_path
            .join(app)
            .join(crate::storage::date::date_from_ms(recs[0].0));
        crate::storage::writer::write_chunk(&dir.join("0000000000001-00001.parquet"), &records)
            .unwrap();
    }

    #[tokio::test]
    async fn logs_validates_limit_and_range() {
        let r = |limit, from, to| {
            let mut p = params("web");
            p.limit = limit;
            p.from_ms = from;
            p.to_ms = to;
            get_logs(State(state()), HeaderMap::new(), Query(p))
        };
        assert_eq!(
            r(Some(0), None, None).await.err().unwrap().0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            r(None, Some(10), Some(9)).await.err().unwrap().0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            r(Some(MAX_LIMIT + 1), None, None).await.err().unwrap().0,
            StatusCode::BAD_REQUEST
        );
        assert!(r(Some(MAX_LIMIT), None, None).await.is_ok());
        assert!(r(None, Some(9), Some(9)).await.is_ok());
    }

    #[tokio::test]
    async fn logs_returns_filtered_newest_first() {
        let st = state_at("e2e");
        seed(
            &st,
            "web",
            &[
                (1_700_000_000_000, "old error"),
                (1_700_000_000_500, "new ERROR"),
                (1_700_000_000_900, "ok"),
            ],
        );
        let mut p = params("web");
        p.text = Some("error".into());
        let r = get_logs(State(st.clone()), HeaderMap::new(), Query(p))
            .await
            .unwrap();
        assert_eq!(r.0.count, 2);
        assert_eq!(r.0.records[0].message, "new ERROR");
        assert_eq!(r.0.records[1].message, "old error");

        let mut p = params("web");
        p.limit = Some(1);
        let r = get_logs(State(st.clone()), HeaderMap::new(), Query(p))
            .await
            .unwrap();
        assert_eq!(r.0.records.len(), 1);
        assert_eq!(r.0.records[0].message, "ok");
        let _ = std::fs::remove_dir_all(&st.data_path);
    }

    #[tokio::test]
    async fn logs_sheds_load_when_all_query_slots_are_busy() {
        let mut st = state();
        st.query_slots = Arc::new(tokio::sync::Semaphore::new(0));
        let r = get_logs(State(st), HeaderMap::new(), Query(params("web"))).await;
        assert_eq!(r.err().unwrap().0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn logs_releases_its_slot_after_each_query() {
        let mut st = state();
        st.query_slots = Arc::new(tokio::sync::Semaphore::new(1));
        for _ in 0..3 {
            let _ = get_logs(State(st.clone()), HeaderMap::new(), Query(params("web")))
                .await
                .unwrap();
        }
        assert_eq!(st.query_slots.available_permits(), 1);
    }
}
