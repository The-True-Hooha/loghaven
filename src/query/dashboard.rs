//! The web dashboard: a static page served by the query server. It holds no secrets and no
//! data; the page reads the session token from the URL fragment and calls `/logs` itself.

use crate::query::QueryState;
use axum::{
    Router,
    extract::Request,
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};

const INDEX: &str = include_str!("dashboard/index.html");
const SCRIPT: &str = include_str!("dashboard/app.js");
const STYLE: &str = include_str!("dashboard/app.css");

pub fn routes() -> Router<QueryState> {
    Router::new()
        .route("/dashboard", get(|| asset("text/html; charset=utf-8", INDEX)))
        .route(
            "/dashboard/app.js",
            get(|| asset("text/javascript; charset=utf-8", SCRIPT)),
        )
        .route(
            "/dashboard/app.css",
            get(|| asset("text/css; charset=utf-8", STYLE)),
        )
}

async fn asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
            ),
        ],
        body,
    )
}

/// Refuses requests whose Host is not the loopback address, so a page on another site that
/// resolves its own name to 127.0.0.1 (DNS rebinding) cannot read the daemon.
pub(super) async fn local_host_only(req: Request, next: Next) -> Response {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h: &HeaderValue| h.to_str().ok())
        .unwrap_or("");
    let host = host.to_ascii_lowercase();
    let (name, port) = match host.find(']') {
        Some(end) => (&host[..=end], &host[end + 1..]),
        None => host.split_at(host.rfind(':').unwrap_or(host.len())),
    };
    let port_ok = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    if port_ok && matches!(name, "127.0.0.1" | "localhost" | "[::1]") {
        next.run(req).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    fn state() -> QueryState {
        QueryState {
            sessions: std::sync::Arc::new(crate::auth::session::SessionStore::new(60)),
            public_key_pem: None,
            data_path: std::env::temp_dir(),
            require_auth: true,
            query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    async fn get_with(path: &str, host: &str) -> Response {
        crate::query::router(state())
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::HOST, host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn serves_page_script_and_style_with_a_strict_policy() {
        for (path, kind) in [
            ("/dashboard", "text/html"),
            ("/dashboard/app.js", "text/javascript"),
            ("/dashboard/app.css", "text/css"),
        ] {
            let r = get_with(path, "127.0.0.1:9200").await;
            assert_eq!(r.status(), StatusCode::OK, "{path}");
            let h = r.headers();
            assert!(h[header::CONTENT_TYPE].to_str().unwrap().starts_with(kind));
            let csp = h[header::CONTENT_SECURITY_POLICY].to_str().unwrap();
            assert!(csp.contains("default-src 'none'") && !csp.contains("unsafe"), "{csp}");
            assert_eq!(h[header::REFERRER_POLICY], "no-referrer");
            assert_eq!(h[header::CACHE_CONTROL], "no-store");
        }
    }

    #[tokio::test]
    async fn refuses_a_host_that_is_not_loopback() {
        for host in [
            "evil.example:9200",
            "evil.example",
            "127.0.0.1.evil.example:9200",
            "",
            "[::1]garbage",
            "localhost:evil",
        ] {
            for path in ["/dashboard", "/logs?app=web"] {
                let r = get_with(path, host).await;
                assert_eq!(r.status(), StatusCode::FORBIDDEN, "{path} {host:?}");
            }
        }
        let r = get_with("/auth", "evil.example:9200").await;
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        for host in ["localhost:9200", "127.0.0.1", "[::1]:9200", "LOCALHOST:9200"] {
            let r = get_with("/dashboard/app.js", host).await;
            assert_eq!(r.status(), StatusCode::OK, "{host:?}");
        }
    }

    #[test]
    fn page_never_writes_untrusted_text_as_html() {
        for banned in ["innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("] {
            assert!(!SCRIPT.contains(banned), "{banned}");
        }
        assert!(!INDEX.contains("<script>") && !INDEX.contains("onclick"));
    }
}
