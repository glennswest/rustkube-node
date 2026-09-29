//! The node's stormblock engine API, with the engine's token (#66).
//!
//! The engine's management API (`:9090`) requires `Authorization: Bearer
//! <token>` on every call from stormblock 17 on (stormblock#107). Without it
//! every call is a 401: claims do not provision, pod volumes and VM disks do
//! not attach, and pulled images have nowhere to go. The token is not the
//! apiserver's. The engine mints one per node and writes it to a file, and a
//! client on the node reads that file.
//!
//! One [`EngineClient`] is built at startup and shared by everything in the
//! kubelet that calls the engine: the pod manager (claims), `system_claims`,
//! the VM manager and the stormpump image service.
//!
//! **Where the token is found.** This is the lookup stormblock's own CLI uses:
//! `$STORMBLOCK_API_TOKEN`, then the file at `$STORMBLOCK_TOKEN_FILE` (default
//! `/run/stormblock/engine/api_token`, which stormcos gives the kubelet), then
//! `/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`.
//!
//! **When it is read.** Not only once at startup. The engine mints the token
//! when it starts, and the kubelet may start first, so while no token is found
//! it is looked for again on every call. Once found it is cached, and a 401
//! makes the client read it again and, if it changed, retry once. The engine
//! keeps its token across restarts, so a 401 with an unchanged token is
//! returned to the caller as it is.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use reqwest::{Method, Response, StatusCode};
use serde_json::Value;
use tracing::{info, warn};

/// Where the engine listens on a node.
pub const DEFAULT_URL: &str = "http://127.0.0.1:9090";
/// The token file stormcos gives the kubelet (stormcos#104).
pub const DEFAULT_TOKEN_FILE: &str = "/run/stormblock/engine/api_token";
/// Where stormblock's CLI looks after `$STORMBLOCK_TOKEN_FILE`.
/// How long one ordinary engine call may take, connect to last byte (#99).
/// The engine is on loopback and answers in milliseconds; this bounds an
/// engine that has stopped answering, so a worker is freed with an error.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// How long to wait for the engine to accept a connection.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// A blank's mint answers when its format is done, which for a large class is
/// minutes. stormblock finishes the format on its own task if the caller goes
/// (stormblock#141) and refuses a second template of the same name, so a mint
/// that outlives this is found again by name, not made twice.
pub const MINT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

const FALLBACK_TOKEN_FILES: [&str; 2] = ["/etc/stormblock/api_token", "/var/lib/stormblock/api_token"];

/// Where the engine's token can come from, in order. Not `Debug`: it can
/// hold the token itself.
#[derive(Clone, Default)]
pub struct TokenSource {
    /// `$STORMBLOCK_API_TOKEN`: named explicitly, and used as it is.
    explicit: Option<String>,
    /// Files, first readable non-empty one wins.
    files: Vec<PathBuf>,
}

impl TokenSource {
    /// The lookup stormblock's CLI does, from this process's environment.
    pub fn from_env() -> TokenSource {
        let explicit = std::env::var("STORMBLOCK_API_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let first = std::env::var("STORMBLOCK_TOKEN_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_TOKEN_FILE.to_string());
        let mut files = vec![PathBuf::from(first)];
        files.extend(FALLBACK_TOKEN_FILES.iter().map(PathBuf::from));
        TokenSource { explicit, files }
    }

    /// Only these files, and no environment. For tests.
    pub fn files(files: impl IntoIterator<Item = impl Into<PathBuf>>) -> TokenSource {
        TokenSource { explicit: None, files: files.into_iter().map(Into::into).collect() }
    }

    /// No token at all: for an engine that runs with `require_auth = false`,
    /// and for tests of other things.
    pub fn none() -> TokenSource {
        TokenSource::default()
    }

    /// The token as it is now, or `None` when none is found yet.
    fn read(&self) -> Option<String> {
        if let Some(t) = &self.explicit {
            return Some(t.clone());
        }
        self.files.iter().find_map(|p| {
            let t = std::fs::read_to_string(p).ok()?;
            let t = t.trim();
            (!t.is_empty()).then(|| t.to_string())
        })
    }

    /// The places searched, for a message that says where to put it.
    fn describe(&self) -> String {
        let mut places = Vec::new();
        if self.explicit.is_some() {
            places.push("$STORMBLOCK_API_TOKEN".to_string());
        }
        places.extend(self.files.iter().map(|p| p.display().to_string()));
        places.join(", ")
    }
}

struct Inner {
    http: reqwest::Client,
    url: String,
    source: TokenSource,
    /// The token last read. `None` until one is found.
    token: Mutex<Option<String>>,
    /// Whether "no token found" has been logged, so a node whose engine has
    /// not minted yet says so once rather than on every sync tick.
    said_none: AtomicBool,
}

/// A client for this node's stormblock engine that carries its token.
///
/// Cheap to clone: every clone shares the connection pool and the cached
/// token, so a token re-read after a 401 is seen by every caller.
#[derive(Clone)]
pub struct EngineClient {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for EngineClient {
    // Not derived: the token must not reach a log through `{:?}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineClient")
            .field("url", &self.inner.url)
            .field("token_from", &self.inner.source.describe())
            .finish()
    }
}

impl Default for EngineClient {
    fn default() -> Self {
        EngineClient::from_env(DEFAULT_URL)
    }
}

impl EngineClient {
    /// The engine at `url`, with the token found the way stormblock's CLI
    /// finds it.
    pub fn from_env(url: &str) -> EngineClient {
        EngineClient::new(url, TokenSource::from_env())
    }

    pub fn new(url: &str, source: TokenSource) -> EngineClient {
        EngineClient {
            inner: Arc::new(Inner {
                // No client-wide timeout: each request carries its own, and
                // the volume watch carries none (#99).
                http: reqwest::Client::builder()
                    .connect_timeout(CONNECT_TIMEOUT)
                    .build()
                    .unwrap_or_default(),
                url: url.trim_end_matches('/').to_string(),
                source,
                token: Mutex::new(None),
                said_none: AtomicBool::new(false),
            }),
        }
    }

    /// The engine's base URL, e.g. `http://127.0.0.1:9090`.
    pub fn url(&self) -> &str {
        &self.inner.url
    }

    pub async fn get(&self, url: &str) -> reqwest::Result<Response> {
        self.send(Method::GET, url, None).await
    }

    /// A POST bounded by `timeout` rather than [`REQUEST_TIMEOUT`]: one the
    /// engine answers only when a long operation is done (a mint).
    pub async fn post_within(&self, url: &str, body: &Value, timeout: std::time::Duration) -> reqwest::Result<Response> {
        self.send_within(Method::POST, url, Some(body), Some(timeout)).await
    }

    /// A GET with no overall bound, for a stream that is meant to stay open
    /// (the volume watch). Connecting is still bounded.
    async fn get_stream(&self, url: &str) -> reqwest::Result<Response> {
        self.send_within(Method::GET, url, None, None).await
    }

    pub async fn post(&self, url: &str, body: &Value) -> reqwest::Result<Response> {
        self.send(Method::POST, url, Some(body)).await
    }

    pub async fn put(&self, url: &str, body: &Value) -> reqwest::Result<Response> {
        self.send(Method::PUT, url, Some(body)).await
    }

    pub async fn delete(&self, url: &str) -> reqwest::Result<Response> {
        self.send(Method::DELETE, url, None).await
    }

    /// Send one request with the token, and on a 401 read the token again
    /// and retry once if it changed.
    ///
    /// `url` is absolute. The callers already build their URLs from the
    /// engine's base, and several are pointed at a fake in tests.
    pub async fn send(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
    ) -> reqwest::Result<Response> {
        self.send_within(method, url, body, Some(REQUEST_TIMEOUT)).await
    }

    /// [`Self::send`], bounded by `timeout` (`None`: only the connect is).
    async fn send_within(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
        timeout: Option<std::time::Duration>,
    ) -> reqwest::Result<Response> {
        let used = self.token();
        let resp = self.once(method.clone(), url, body, used.as_deref(), timeout).await?;
        if resp.status() != StatusCode::UNAUTHORIZED {
            return Ok(resp);
        }
        let fresh = self.reload();
        if fresh.is_some() && fresh != used {
            info!("stormblock refused the engine token; retrying with the one now on disk");
            return self.once(method, url, body, fresh.as_deref(), timeout).await;
        }
        if fresh.is_none() {
            self.say_none();
        }
        Ok(resp)
    }

    async fn once(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
        token: Option<&str>,
        timeout: Option<std::time::Duration>,
    ) -> reqwest::Result<Response> {
        let mut req = self.inner.http.request(method, url);
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        req.send().await
    }

    /// The cached token, or a fresh read while there is none.
    fn token(&self) -> Option<String> {
        let mut cached = self.inner.token.lock().unwrap_or_else(|e| e.into_inner());
        if cached.is_none() {
            *cached = self.inner.source.read();
            if cached.is_some() && self.inner.said_none.swap(false, Ordering::Relaxed) {
                info!("stormblock engine token found");
            }
        }
        cached.clone()
    }

    /// Read the token again, whatever is cached.
    fn reload(&self) -> Option<String> {
        let fresh = self.inner.source.read();
        *self.inner.token.lock().unwrap_or_else(|e| e.into_inner()) = fresh.clone();
        fresh
    }

    fn say_none(&self) {
        if !self.inner.said_none.swap(true, Ordering::Relaxed) {
            warn!(
                "stormblock requires a token and none was found (looked in {}); \
                 engine calls are refused until the engine has minted one",
                self.inner.source.describe()
            );
        }
    }
}

/// stormblock's watch on its volumes (stormblock#80): newline-delimited
/// `{type, object}` events for every volume change.
pub const VOLUME_WATCH: &str = "/apis/storage.storm.io/v1/volumes?watch=1";

/// How often an engine without the watch is asked instead (#101). Counted as
/// a fallback, so a node running an older engine is visible in `/metrics`.
pub const VOLUME_POLL: std::time::Duration = std::time::Duration::from_secs(30);

impl EngineClient {
    /// Call `changed` whenever the engine's volumes change, for ever (#101).
    ///
    /// Follows [`VOLUME_WATCH`]. Once on every (re)connect, because what
    /// changed while disconnected is not replayed, then once per event line.
    /// A dropped stream reconnects with a backoff (1 s doubling to 30 s). An
    /// engine that has no watch (404) is asked every [`VOLUME_POLL`] instead,
    /// counted in `kubelet_timed_reconciles_total{worker="engine-volumes",
    /// cause="fallback"}`.
    pub async fn follow_volumes(&self, changed: impl Fn() + Send + Sync) {
        let url = format!("{}{VOLUME_WATCH}", self.url());
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            match self.get_stream(&url).await {
                Ok(r) if r.status() == StatusCode::NOT_FOUND => {
                    crate::metrics::observe_timed("engine-volumes", "fallback");
                    changed();
                    tokio::time::sleep(VOLUME_POLL).await;
                    continue;
                }
                Ok(mut r) if r.status().is_success() => {
                    backoff = std::time::Duration::from_secs(1);
                    changed();
                    let mut partial = Vec::new();
                    while let Ok(Some(chunk)) = r.chunk().await {
                        partial.extend_from_slice(&chunk);
                        let lines = partial.iter().filter(|b| **b == b'\n').count();
                        if lines > 0 {
                            // Everything up to the last newline is whole
                            // events; any tail is the start of the next.
                            let cut = partial.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
                            partial.drain(..cut);
                            changed();
                        }
                    }
                    tracing::debug!("stormblock volume watch ended; reconnecting");
                }
                Ok(r) => tracing::debug!("stormblock volume watch refused: {}", r.status()),
                Err(e) => tracing::debug!("stormblock volume watch: {e}"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
        }
    }
}

#[cfg(test)]
mod tests {

    /// An engine that accepts and never answers frees the caller at the
    /// bound, with an error (#99).
    #[tokio::test]
    async fn a_silent_engine_is_a_timeout_not_a_hang() {
        let app = axum::Router::new().route(
            "/api/v1/volumes",
            axum::routing::get(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                "late"
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let engine = EngineClient::new(&url, TokenSource::none());
        let started = std::time::Instant::now();
        let e = engine
            .send_within(Method::GET, &format!("{url}/api/v1/volumes"), None, Some(std::time::Duration::from_millis(200)))
            .await
            .unwrap_err();
        assert!(e.is_timeout(), "{e}");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }

    /// Every event line is a change; a connect is one too, and the stream
    /// ending reconnects (#101).
    #[tokio::test]
    async fn volume_events_are_followed_and_reconnected() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let app = axum::Router::new().route(
            "/apis/storage.storm.io/v1/volumes",
            axum::routing::get(|| async {
                "{\"type\":\"ADDED\",\"object\":{}}\n{\"type\":\"MODIFIED\",\"object\":{}}\n"
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let engine = EngineClient::new(&url, TokenSource::none());
        let seen = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        let follow = tokio::spawn(async move {
            engine.follow_volumes(move || { s.fetch_add(1, Ordering::SeqCst); }).await
        });
        // Connect + the events, then again after the reconnect (1 s).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while seen.load(Ordering::SeqCst) < 3 {
            assert!(tokio::time::Instant::now() < deadline, "saw {}", seen.load(Ordering::SeqCst));
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let first = seen.load(Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert!(seen.load(Ordering::SeqCst) > first, "reconnected after the stream ended");
        follow.abort();
    }
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// An engine that answers `/api/v1/volumes` only to `Bearer <want>`, and
    /// counts the requests it saw.
    async fn engine(want: Arc<Mutex<String>>) -> (String, Arc<AtomicUsize>) {
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        let app = axum::Router::new().route(
            "/api/v1/volumes",
            axum::routing::any(move |headers: axum::http::HeaderMap| {
                let (want, count) = (want.clone(), count.clone());
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let expected = format!("Bearer {}", want.lock().unwrap());
                    let got = headers.get("authorization").and_then(|v| v.to_str().ok());
                    if got == Some(expected.as_str()) {
                        (axum::http::StatusCode::OK, axum::Json(serde_json::json!({"items": []})))
                    } else {
                        (axum::http::StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({})))
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn the_token_on_disk_is_presented() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        std::fs::write(&file, "t1\n").unwrap();
        let (url, _) = engine(Arc::new(Mutex::new("t1".into()))).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let r = c.get(&format!("{url}/api/v1/volumes")).await.unwrap();
        assert_eq!(r.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_token_minted_after_the_kubelet_started_is_picked_up() {
        // The engine mints at start, and the kubelet may be first.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        let (url, _) = engine(Arc::new(Mutex::new("minted".into()))).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let path = format!("{url}/api/v1/volumes");

        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        std::fs::write(&file, "minted").unwrap();
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_401_rereads_the_token_and_retries_once() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        std::fs::write(&file, "old").unwrap();
        let want = Arc::new(Mutex::new("old".to_string()));
        let (url, seen) = engine(want.clone()).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let path = format!("{url}/api/v1/volumes");
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);

        // The token changes under a running kubelet.
        *want.lock().unwrap() = "new".into();
        std::fs::write(&file, "new").unwrap();
        seen.store(0, Ordering::SeqCst);
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(seen.load(Ordering::SeqCst), 2, "one refused, one retried");

        // A 401 with nothing new on disk is returned, not looped on.
        *want.lock().unwrap() = "other".into();
        seen.store(0, Ordering::SeqCst);
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn clones_share_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        std::fs::write(&file, "a").unwrap();
        let want = Arc::new(Mutex::new("a".to_string()));
        let (url, _) = engine(want.clone()).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let other = c.clone();
        let path = format!("{url}/api/v1/volumes");
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);

        *want.lock().unwrap() = "b".into();
        std::fs::write(&file, "b").unwrap();
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        // The clone already has the re-read token, so its first call is not refused.
        assert_eq!(other.token().as_deref(), Some("b"));
    }

    #[test]
    fn the_first_readable_file_wins_and_blank_files_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, c) = (dir.path().join("a"), dir.path().join("b"), dir.path().join("c"));
        std::fs::write(&b, "  \n").unwrap();
        std::fs::write(&c, "tok\n").unwrap();
        assert_eq!(TokenSource::files([&a, &b, &c]).read().as_deref(), Some("tok"));
        assert_eq!(TokenSource::none().read(), None);
    }

    #[test]
    fn debug_does_not_print_the_token() {
        let src = TokenSource { explicit: Some("secret".into()), files: vec![] };
        let c = EngineClient::new(DEFAULT_URL, src);
        let _ = c.token();
        assert!(!format!("{c:?}").contains("secret"));
    }
}
