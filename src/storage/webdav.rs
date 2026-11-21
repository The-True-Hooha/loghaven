use crate::error::{LogHavenError, Result};
use crate::storage::cloud::{
    BoxFuture, MAX_LISTED, ObjectStore, STALL_TIMEOUT, Sink, upload_budget,
};
use reqwest::{Method, StatusCode, Url};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

/// A WebDAV server as an object store: keys are paths under `base`, and the collections along a
/// key are created before it is written.
pub(crate) struct WebDavStore {
    client: reqwest::Client,
    base: Url,
    login: Option<(String, String)>,
    made: Mutex<HashSet<String>>,
}

const MAX_LISTING_BYTES: usize = 16 << 20;
const MAX_DEPTH: usize = 32;

/// The path of a URL as the server means it: percent-decoded, no trailing slash.
fn decoded_path(url: &Url) -> String {
    percent_encoding::percent_decode_str(url.path())
        .decode_utf8_lossy()
        .trim_end_matches('/')
        .to_string()
}

fn fail(what: &str, detail: impl std::fmt::Display) -> LogHavenError {
    LogHavenError::Storage(format!("WebDAV {what} failed: {detail}"))
}

fn transport(what: &str, e: reqwest::Error) -> LogHavenError {
    fail(what, e.without_url())
}

fn status(what: &str, s: StatusCode) -> LogHavenError {
    fail(what, format!("HTTP {s}"))
}

impl WebDavStore {
    pub(crate) fn new(url: &str, login: Option<(String, String)>) -> Result<Self> {
        let mut base = Url::parse(url)
            .map_err(|_| LogHavenError::Config("WebDAV url is not a valid URL".into()))?;
        if !base.username().is_empty() || base.password().is_some() {
            return Err(LogHavenError::Config(
                "WebDAV url must not carry a user name or password; use username and password"
                    .into(),
            ));
        }
        base.set_query(None);
        base.set_fragment(None);
        // a redirect would resend the login (and cannot replay a streamed upload)
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| LogHavenError::Config(format!("cannot set up WebDAV client: {e}")))?;
        Ok(Self {
            client,
            base,
            login,
            made: Mutex::new(HashSet::new()),
        })
    }

    /// The URL of `key` (a `/`-separated path below the base); `collection` ends it with a slash.
    fn url(&self, key: &str, collection: bool) -> Result<Url> {
        let mut url = self.base.clone();
        {
            let mut segs = url
                .path_segments_mut()
                .map_err(|_| LogHavenError::Config("WebDAV url cannot carry a path".into()))?;
            segs.pop_if_empty();
            segs.extend(key.split('/').filter(|s| !s.is_empty()));
            if collection {
                segs.push("");
            }
        }
        Ok(url)
    }

    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let r = self.client.request(method, url);
        match &self.login {
            Some((user, pass)) => r.basic_auth(user, Some(pass)),
            None => r,
        }
    }

    async fn make_collections(&self, key: &str) -> Result<()> {
        let parts: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
        let mut path = String::new();
        for part in &parts[..parts.len().saturating_sub(1)] {
            path.push_str(part);
            path.push('/');
            if self.made.lock().unwrap().contains(&path) {
                continue;
            }
            let method = Method::from_bytes(b"MKCOL").expect("valid method");
            let resp = self
                .request(method, self.url(&path, true)?)
                .send()
                .await
                .map_err(|e| transport("MKCOL", e))?;
            // 405: the collection is already there
            if !(resp.status().is_success() || resp.status() == StatusCode::METHOD_NOT_ALLOWED) {
                return Err(status("MKCOL", resp.status()));
            }
            self.made.lock().unwrap().insert(path.clone());
        }
        Ok(())
    }

    async fn send_file(&self, key: &str, file: &Path, len: u64) -> Result<()> {
        self.make_collections(key).await?;
        let input = tokio::fs::File::open(file)
            .await
            .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(input));
        let resp = self
            .request(Method::PUT, self.url(key, false)?)
            .header(reqwest::header::CONTENT_LENGTH, len)
            .body(body)
            .send()
            .await
            .map_err(|e| transport("PUT", e))?;
        if resp.status().is_success() {
            return Ok(());
        }
        if matches!(resp.status(), StatusCode::NOT_FOUND | StatusCode::CONFLICT) {
            // a collection made earlier may have been removed since
            self.made.lock().unwrap().clear();
        }
        Err(status("PUT", resp.status()))
    }

    /// Entries one level below a collection: key (relative to the base) and whether it is a
    /// collection. A collection that does not exist has none. Anything the server lists that is
    /// not below the base, or is the collection itself, is ignored.
    async fn children(&self, collection: &str) -> Result<Vec<(String, bool)>> {
        let method = Method::from_bytes(b"PROPFIND").expect("valid method");
        let url = self.url(collection, true)?;
        let mut resp = self
            .request(method, url.clone())
            .header("Depth", "1")
            .header(reqwest::header::CONTENT_TYPE, "application/xml")
            .body(
                r#"<?xml version="1.0"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>"#,
            )
            .send()
            .await
            .map_err(|e| transport("PROPFIND", e))?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(vec![]);
        }
        if !resp.status().is_success() {
            return Err(status("PROPFIND", resp.status()));
        }
        let mut raw = Vec::new();
        while let Some(part) = resp.chunk().await.map_err(|e| transport("PROPFIND", e))? {
            if raw.len() + part.len() > MAX_LISTING_BYTES {
                return Err(fail("PROPFIND", "the answer is too large"));
            }
            raw.extend_from_slice(&part);
        }
        let text = String::from_utf8_lossy(&raw);
        let base_path = decoded_path(&self.base);
        let me = decoded_path(&url);
        let mut out = Vec::new();
        for (href, is_collection) in parse_multistatus(&text)? {
            let Ok(full) = url.join(&href) else { continue };
            if full.scheme() != url.scheme()
                || full.host_str() != url.host_str()
                || full.port_or_known_default() != url.port_or_known_default()
            {
                continue;
            }
            let path = decoded_path(&full);
            if path == me {
                continue;
            }
            let Some(rel) = path
                .strip_prefix(&base_path)
                .and_then(|r| r.strip_prefix('/'))
            else {
                continue;
            };
            if !rel.is_empty() && !rel.split('/').any(|s| s == "..") {
                out.push((rel.to_string(), is_collection));
            }
        }
        Ok(out)
    }

    async fn walk(&self, collection: &str) -> Result<Vec<String>> {
        let mut files = Vec::new();
        let mut seen = HashSet::new();
        let mut todo = vec![(collection.trim_matches('/').to_string(), 0usize)];
        while let Some((dir, depth)) = todo.pop() {
            if !seen.insert(dir.clone()) {
                continue;
            }
            if depth > MAX_DEPTH || files.len() > MAX_LISTED {
                return Err(fail("PROPFIND", "the listing is too deep or too large"));
            }
            let answer = tokio::time::timeout(STALL_TIMEOUT, self.children(&dir))
                .await
                .map_err(|_| LogHavenError::Storage("listing the store timed out".into()))??;
            for (key, is_collection) in answer {
                if is_collection {
                    todo.push((key, depth + 1));
                } else {
                    files.push(key);
                }
            }
        }
        files.sort();
        files.dedup();
        Ok(files)
    }

    async fn read(&self, key: &str, dest: &Path, max_bytes: u64) -> Result<u64> {
        let mut sink = Sink::create(dest, max_bytes).await?;
        let stalled = || LogHavenError::Storage("download from cloud timed out".into());
        let mut resp = tokio::time::timeout(
            STALL_TIMEOUT,
            self.request(Method::GET, self.url(key, false)?).send(),
        )
        .await
        .map_err(|_| stalled())?
        .map_err(|e| transport("GET", e))?;
        if !resp.status().is_success() {
            return Err(status("GET", resp.status()));
        }
        if let Some(n) = resp.content_length() {
            sink.refuse_if_larger(n)?;
        }
        let budget = upload_budget(resp.content_length().unwrap_or(max_bytes));
        let all = async {
            while let Some(part) = tokio::time::timeout(STALL_TIMEOUT, resp.chunk())
                .await
                .map_err(|_| stalled())?
                .map_err(|e| transport("GET", e))?
            {
                sink.push(&part).await?;
            }
            sink.finish().await
        };
        tokio::time::timeout(budget, all)
            .await
            .map_err(|_| stalled())?
    }
}

/// `(href, is_collection)` of every `response` in a PROPFIND answer; namespace prefixes vary
/// between servers, so only local names are compared. Entities (`&amp;`) inside an href are
/// resolved: the reader delivers them as events of their own.
fn parse_multistatus(xml: &str) -> Result<Vec<(String, bool)>> {
    use quick_xml::{Reader, escape::resolve_predefined_entity, events::Event};
    let bad = |e: &dyn std::fmt::Display| fail("PROPFIND", format!("bad XML: {e}"));
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    let mut href = String::new();
    let mut seen_href = false;
    let mut collection = false;
    let mut in_href = false;
    loop {
        match reader.read_event().map_err(|e| bad(&e))? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"response" => {
                    href.clear();
                    seen_href = false;
                    collection = false;
                }
                b"href" => {
                    in_href = true;
                    seen_href = true;
                    href.clear();
                }
                b"collection" => collection = true,
                _ => {}
            },
            Event::Empty(e) if e.local_name().as_ref() == b"collection" => collection = true,
            Event::Text(t) if in_href => href.push_str(&t.decode().map_err(|e| bad(&e))?),
            Event::GeneralRef(r) if in_href => {
                if let Some(c) = r.resolve_char_ref().map_err(|e| bad(&e))? {
                    href.push(c);
                } else if let Some(text) =
                    resolve_predefined_entity(&r.decode().map_err(|e| bad(&e))?)
                {
                    href.push_str(text);
                }
            }
            Event::End(e) => match e.local_name().as_ref() {
                b"href" => in_href = false,
                b"response" if seen_href => out.push((std::mem::take(&mut href), collection)),
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(out)
}

impl ObjectStore for WebDavStore {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let len = std::fs::metadata(file)
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?
                .len();
            tokio::time::timeout(upload_budget(len), self.send_file(key, file, len))
                .await
                .map_err(|_| LogHavenError::Storage("upload to cloud timed out".into()))?
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(self.walk(prefix))
    }

    fn download<'a>(
        &'a self,
        key: &'a str,
        dest: &'a Path,
        max_bytes: u64,
    ) -> BoxFuture<'a, Result<u64>> {
        Box::pin(self.read(key, dest, max_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::Target;
    use axum::{
        Router,
        body::Bytes,
        extract::{Request, State},
        http::StatusCode as S,
        response::{IntoResponse, Response},
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    #[derive(Default)]
    struct Dav {
        files: BTreeMap<String, Vec<u8>>,
        dirs: BTreeSet<String>,
        log: Vec<(String, String, Option<String>)>,
    }

    type Shared = Arc<Mutex<Dav>>;

    fn multistatus(base: &str, dir: &str, d: &Dav) -> String {
        let mut body = String::from(r#"<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">"#);
        let entry = |href: String, col: bool| {
            format!(
                "<D:response><D:href>{href}</D:href><D:propstat><D:prop><D:resourcetype>{}</D:resourcetype></D:prop></D:propstat></D:response>",
                if col { "<D:collection/>" } else { "" }
            )
        };
        body.push_str(&entry(format!("{base}/{dir}/").replace("//", "/"), true));
        let under = |p: &str| {
            let rest = p.strip_prefix(&format!("{dir}/"))?;
            (!rest.is_empty() && !rest.contains('/')).then_some(rest.to_string())
        };
        for sub in d.dirs.iter().filter_map(|p| under(p)) {
            body.push_str(&entry(
                format!("{base}/{dir}/{sub}/").replace("//", "/"),
                true,
            ));
        }
        for f in d.files.keys().filter_map(|p| under(p)) {
            let enc = f.replace(' ', "%20");
            body.push_str(&entry(
                format!("{base}/{dir}/{enc}").replace("//", "/"),
                false,
            ));
        }
        body.push_str("</D:multistatus>");
        body
    }

    async fn serve(State(st): State<Shared>, req: Request) -> Response {
        let (parts, body) = req.into_parts();
        let bytes: Bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let path = percent_encoding::percent_decode_str(parts.uri.path())
            .decode_utf8_lossy()
            .into_owned();
        let auth = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let mut d = st.lock().unwrap();
        d.log
            .push((parts.method.to_string(), path.clone(), auth.clone()));
        if auth.as_deref() != Some("Basic dXNlcjpwdw==") {
            return S::UNAUTHORIZED.into_response();
        }
        let key = path
            .trim_matches('/')
            .strip_prefix("dav")
            .unwrap_or("")
            .trim_matches('/')
            .to_string();
        match parts.method.as_str() {
            "MKCOL" => {
                if !d.dirs.insert(key.clone()) {
                    return S::METHOD_NOT_ALLOWED.into_response();
                }
                let parent = key.rsplit_once('/').map_or("", |(p, _)| p);
                if !parent.is_empty() && !d.dirs.contains(parent) {
                    d.dirs.remove(&key);
                    return S::CONFLICT.into_response();
                }
                S::CREATED.into_response()
            }
            "PUT" => {
                let parent = key.rsplit_once('/').map_or("", |(p, _)| p);
                if !parent.is_empty() && !d.dirs.contains(parent) {
                    return S::CONFLICT.into_response();
                }
                d.files.insert(key, bytes.to_vec());
                S::CREATED.into_response()
            }
            "GET" => match d.files.get(&key) {
                Some(b) => b.clone().into_response(),
                None => S::NOT_FOUND.into_response(),
            },
            "PROPFIND" => {
                if !key.is_empty() && !d.dirs.contains(&key) {
                    return S::NOT_FOUND.into_response();
                }
                (S::MULTI_STATUS, multistatus("/dav", &key, &d)).into_response()
            }
            _ => S::METHOD_NOT_ALLOWED.into_response(),
        }
    }

    async fn dav_server() -> (String, Shared) {
        let st: Shared = Arc::new(Mutex::new(Dav::default()));
        let app = Router::new().fallback(serve).with_state(st.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}/dav"), st)
    }

    fn target(url: &str, prefix: Option<&str>) -> Target {
        Target::new(
            "dav",
            Box::new(WebDavStore::new(url, Some(("user".into(), "pw".into()))).unwrap()),
            prefix.map(String::from),
        )
    }

    fn file(dir: &Path, body: &[u8]) -> std::path::PathBuf {
        let p = dir.join("chunk.parquet");
        std::fs::write(&p, body).unwrap();
        p
    }

    #[tokio::test]
    async fn objects_go_up_under_their_collections_and_come_back_by_listing_and_download() {
        let (url, st) = dav_server().await;
        let tmp = tempfile::tempdir().unwrap();
        let src = file(tmp.path(), b"chunk bytes");
        let t = target(&url, Some("backup"));
        for key in ["web/2024-01-01/a b.parquet", "web/2024-01-02/c.parquet"] {
            t.put_for_test(key, &src).await.unwrap();
        }
        {
            let d = st.lock().unwrap();
            let mk: Vec<_> = d
                .log
                .iter()
                .filter(|l| l.0 == "MKCOL")
                .map(|l| l.1.as_str())
                .collect();
            assert_eq!(
                mk,
                [
                    "/dav/backup/",
                    "/dav/backup/web/",
                    "/dav/backup/web/2024-01-01/",
                    "/dav/backup/web/2024-01-02/"
                ],
                "each collection is made once, parents first"
            );
        }
        let listed = t.list_app("web/").await.unwrap();
        assert_eq!(
            listed,
            ["web/2024-01-01/a b.parquet", "web/2024-01-02/c.parquet"]
        );
        let dest = tmp.path().join("down");
        let n = t
            .download_named("web/2024-01-01/a b.parquet", &dest, 1 << 20)
            .await
            .unwrap();
        assert_eq!(n, 11);
        assert_eq!(std::fs::read(&dest).unwrap(), b"chunk bytes");
    }

    /// A server that answers every request the same way.
    async fn canned(status: S, body: String, location: Option<&'static str>) -> String {
        let app = Router::new().fallback(move || {
            let body = body.clone();
            async move {
                let mut r = (status, body).into_response();
                if let Some(l) = location {
                    r.headers_mut().insert("location", l.parse().unwrap());
                }
                r
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("127.0.0.1:{}", addr.port())
    }

    #[tokio::test]
    async fn a_body_with_no_length_is_cut_off_at_the_size_a_chunk_may_have() {
        let app = Router::new().fallback(|| async {
            let parts = futures_util::stream::iter(
                (0..100).map(|_| Ok::<_, std::io::Error>(axum::body::Bytes::from(vec![7u8; 1000]))),
            );
            axum::body::Body::from_stream(parts)
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let t = target(&format!("http://127.0.0.1:{}", addr.port()), None);
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("down");
        let e = t
            .download_named("web/2024-01-01/a.parquet", &dest, 5000)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("more than the 5000"), "{e}");
        assert!(std::fs::metadata(&dest).unwrap().len() <= 5000);
    }

    fn multistatus_of(entries: &[(&str, bool)]) -> String {
        let mut x = String::from(r#"<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">"#);
        for (href, col) in entries {
            x.push_str(&format!(
                "<d:response><d:href>{href}</d:href><d:propstat><d:prop><d:resourcetype>{}</d:resourcetype></d:prop></d:propstat></d:response>",
                if *col { "<d:collection/>" } else { "" }
            ));
        }
        x.push_str("</d:multistatus>");
        x
    }

    async fn list_canned(base_path: &str, entries: &[(&str, bool)]) -> Result<Vec<String>> {
        let addr = canned(S::MULTI_STATUS, multistatus_of(entries), None).await;
        let t = target(&format!("http://{addr}{base_path}"), None);
        t.list_app("web/").await
    }

    #[tokio::test]
    async fn names_with_xml_entities_are_listed_whole() {
        let got = list_canned(
            "/dav",
            &[
                ("/dav/web/", true),
                ("/dav/web/a&amp;b.parquet", false),
                ("/dav/web/it&apos;s%20x.parquet", false),
            ],
        )
        .await
        .unwrap();
        assert_eq!(got, ["web/a&b.parquet", "web/it's x.parquet"]);
    }

    #[tokio::test]
    async fn a_base_url_with_encoded_characters_still_gives_the_right_keys() {
        let got = list_canned(
            "/my%20dir/me%40x",
            &[
                ("/my%20dir/me%40x/web/", true),
                ("/my%20dir/me%40x/web/f.parquet", false),
            ],
        )
        .await
        .unwrap();
        assert_eq!(got, ["web/f.parquet"]);
    }

    #[tokio::test]
    async fn the_collection_itself_is_not_taken_for_a_child_with_or_without_the_slash() {
        let got = list_canned("/dav", &[("/dav/web", true), ("/dav/web/f.parquet", false)])
            .await
            .unwrap();
        assert_eq!(got, ["web/f.parquet"]);
    }

    #[tokio::test]
    async fn entries_outside_the_base_or_on_another_host_are_ignored() {
        let got = list_canned(
            "/dav",
            &[
                ("/dav/web/", true),
                ("/etc/passwd", false),
                ("../../etc/x.parquet", false),
                ("/dav/../etc/y.parquet", false),
                ("http://evil.example/dav/web/z.parquet", false),
                ("/dav/web/ok.parquet", false),
            ],
        )
        .await
        .unwrap();
        assert_eq!(got, ["web/ok.parquet"]);
    }

    #[tokio::test]
    async fn a_server_that_lists_collections_in_a_cycle_does_not_loop_forever() {
        // every PROPFIND answers with the same two collections, so web lists sub and sub lists web
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            list_canned(
                "/dav",
                &[
                    ("/dav/web/", true),
                    ("/dav/web/sub/", true),
                    ("/dav/web/f.parquet", false),
                ],
            ),
        )
        .await
        .expect("the walk ended")
        .unwrap();
        assert!(got.iter().all(|k| k.ends_with("f.parquet")), "{got:?}");
    }

    #[tokio::test]
    async fn an_endless_listing_is_cut_off() {
        let addr = canned(S::MULTI_STATUS, " ".repeat(MAX_LISTING_BYTES + 1), None).await;
        let e = target(&format!("http://{addr}/dav"), None)
            .list_app("web/")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("too large"), "{e}");
    }

    #[tokio::test]
    async fn a_redirect_is_an_error_and_is_not_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let addr = canned(
            S::TEMPORARY_REDIRECT,
            String::new(),
            Some("http://127.0.0.1:1/x"),
        )
        .await;
        let e = target(&format!("http://{addr}/dav"), None)
            .put_for_test("web/2024-01-01/a.parquet", &file(tmp.path(), b"x"))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("307"), "{e}");
    }

    #[tokio::test]
    async fn a_collection_removed_after_it_was_made_is_made_again_on_the_next_try() {
        let (url, st) = dav_server().await;
        let tmp = tempfile::tempdir().unwrap();
        let src = file(tmp.path(), b"x");
        let t = target(&url, None);
        t.put_for_test("web/2024-01-01/a.parquet", &src)
            .await
            .unwrap();
        {
            let mut d = st.lock().unwrap();
            d.dirs.clear();
            d.files.clear();
        }
        assert!(
            t.put_for_test("web/2024-01-01/b.parquet", &src)
                .await
                .is_err()
        );
        t.put_for_test("web/2024-01-01/b.parquet", &src)
            .await
            .unwrap();
        assert!(
            st.lock()
                .unwrap()
                .files
                .contains_key("web/2024-01-01/b.parquet")
        );
    }

    #[tokio::test]
    async fn collections_that_already_exist_are_not_an_error() {
        let (url, _st) = dav_server().await;
        let tmp = tempfile::tempdir().unwrap();
        let src = file(tmp.path(), b"x");
        target(&url, None)
            .put_for_test("web/2024-01-01/a.parquet", &src)
            .await
            .unwrap();
        // a fresh client has no memory of the collections made before
        target(&url, None)
            .put_for_test("web/2024-01-01/b.parquet", &src)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_missing_collection_lists_as_empty_and_a_big_object_is_refused() {
        let (url, _st) = dav_server().await;
        let tmp = tempfile::tempdir().unwrap();
        let t = target(&url, None);
        assert!(t.list_app("nothing/").await.unwrap().is_empty());
        t.put_for_test("web/2024-01-01/a.parquet", &file(tmp.path(), &[1u8; 100]))
            .await
            .unwrap();
        let dest = tmp.path().join("down");
        let e = t
            .download_named("web/2024-01-01/a.parquet", &dest, 10)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("more than the 10"), "{e}");
    }

    #[tokio::test]
    async fn the_login_is_sent_and_a_refusal_names_the_status_not_the_address() {
        let (url, st) = dav_server().await;
        let tmp = tempfile::tempdir().unwrap();
        let wrong = Target::new(
            "dav",
            Box::new(WebDavStore::new(&url, Some(("user".into(), "nope".into()))).unwrap()),
            None,
        );
        let e = wrong
            .put_for_test("web/2024-01-01/a.parquet", &file(tmp.path(), b"x"))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("401"), "{e}");
        assert!(!e.contains("127.0.0.1"), "{e}");
        assert!(st.lock().unwrap().files.is_empty());
    }

    #[test]
    fn an_address_that_carries_a_password_is_refused() {
        let e = WebDavStore::new("https://me:secret@host/dav", None)
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("must not carry"), "{e}");
        assert!(!e.contains("secret"), "{e}");
    }

    #[test]
    fn a_propfind_answer_is_read_with_any_namespace_prefix() {
        let xml = r#"<multistatus xmlns="DAV:"><response><href>/a/</href><propstat><prop><resourcetype><collection/></resourcetype></prop></propstat></response><response><href>/a/f.parquet</href><propstat><prop><resourcetype/></prop></propstat></response></multistatus>"#;
        assert_eq!(
            parse_multistatus(xml).unwrap(),
            [
                ("/a/".to_string(), true),
                ("/a/f.parquet".to_string(), false)
            ]
        );
        assert!(parse_multistatus("<a><b></a>").is_err());
    }
}
