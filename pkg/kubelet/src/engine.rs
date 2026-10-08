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
//!
//! **The admin token (#105).** Since stormblock#274 (`admin_gate = enforce`)
//! the node token no longer covers destructive verbs: deleting a template
//! (`DELETE /api/v1/fstemplates/{t}`, #140's rebuild of a broken blank), a
//! sealed volume, and the rest stormblock classifies so. Those need the
//! engine's admin token. It is found as stormblock finds it:
//! `$STORMBLOCK_ADMIN_TOKEN`, then the file at `$STORMBLOCK_ADMIN_TOKEN_FILE`
//! (default `/run/stormblock-admin/admin_token`). The kubelet presents the
//! node token first, always; only a call that changes something (not a GET)
//! and is still refused with 401 after the node token's own re-read is sent
//! once more with the admin token. What is destructive stays the engine's
//! decision (a volume delete is, or is not, by whether the volume is sealed).
//! The admin token is read again at every such call, so a rotated one is used
//! at once, and is never cached, logged or shown by `{:?}`. With no admin
//! token, or on an engine with one token for everything, nothing changes: the
//! 401 goes back to the caller, which reports it and retries on its next pass.

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

/// Where stormblock keeps its admin token unless told otherwise (stormblock
/// `mgmt::auth::admin_token_file`).
pub const DEFAULT_ADMIN_TOKEN_FILE: &str = "/run/stormblock-admin/admin_token";

const FALLBACK_TOKEN_FILES: [&str; 2] = ["/etc/stormblock/api_token", "/var/lib/stormblock/api_token"];

/// Where the engine's token can come from, in order. Not `Debug`: it can
/// hold the token itself.
#[derive(Clone, Default)]
pub struct TokenSource {
    /// `$STORMBLOCK_API_TOKEN`: named explicitly, and used as it is.
    explicit: Option<String>,
    /// Files, first readable non-empty one wins.
    files: Vec<PathBuf>,
    /// The variable `explicit` came from, for messages.
    env: &'static str,
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
        TokenSource { explicit, files, env: "$STORMBLOCK_API_TOKEN" }
    }

    /// The admin token's lookup (#105), as stormblock resolves it:
    /// `$STORMBLOCK_ADMIN_TOKEN`, then `$STORMBLOCK_ADMIN_TOKEN_FILE`, else
    /// [`DEFAULT_ADMIN_TOKEN_FILE`].
    pub fn admin_from_env() -> TokenSource {
        let explicit = std::env::var("STORMBLOCK_ADMIN_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let file = std::env::var("STORMBLOCK_ADMIN_TOKEN_FILE")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ADMIN_TOKEN_FILE.to_string());
        TokenSource { explicit, files: vec![PathBuf::from(file)], env: "$STORMBLOCK_ADMIN_TOKEN" }
    }

    /// Only these files, and no environment. For tests.
    pub fn files(files: impl IntoIterator<Item = impl Into<PathBuf>>) -> TokenSource {
        TokenSource { explicit: None, files: files.into_iter().map(Into::into).collect(), env: "" }
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
            places.push(self.env.to_string());
        }
        places.extend(self.files.iter().map(|p| p.display().to_string()));
        places.join(", ")
    }
}

struct Inner {
    http: reqwest::Client,
    url: String,
    source: TokenSource,
    /// The admin token's source (#105). Read at each use, never cached.
    admin: TokenSource,
    /// Whether "no admin token" has been logged.
    said_no_admin: AtomicBool,
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
            .field("admin_token_from", &self.inner.admin.describe())
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
        EngineClient::with_admin(url, TokenSource::from_env(), TokenSource::admin_from_env())
    }

    /// The node token only: destructive verbs are refused on an enforcing
    /// engine.
    pub fn new(url: &str, source: TokenSource) -> EngineClient {
        EngineClient::with_admin(url, source, TokenSource::none())
    }

    /// The node token, and the admin token for what the node token is
    /// refused (#105).
    pub fn with_admin(url: &str, source: TokenSource, admin: TokenSource) -> EngineClient {
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
                admin,
                said_no_admin: AtomicBool::new(false),
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
        let mut resp = resp;
        if fresh.is_some() && fresh != used {
            info!("stormblock refused the engine token; retrying with the one now on disk");
            resp = self.once(method.clone(), url, body, fresh.as_deref(), timeout).await?;
            if resp.status() != StatusCode::UNAUTHORIZED {
                return Ok(resp);
            }
        }
        if fresh.is_none() {
            self.say_none();
            return Ok(resp);
        }
        // The node token is current and still refused: a destructive verb
        // on an engine that gates them (stormblock#274). Reads never are.
        if method == Method::GET {
            return Ok(resp);
        }
        match self.inner.admin.read() {
            Some(admin) if Some(&admin) != fresh.as_ref() => {
                info!(%method, url = %path_of(url), "stormblock wants the admin token for this call; presenting it");
                self.inner.said_no_admin.store(false, Ordering::Relaxed);
                self.once(method, url, body, Some(&admin), timeout).await
            }
            Some(_) => Ok(resp),
            None => {
                if !self.inner.said_no_admin.swap(true, Ordering::Relaxed) {
                    warn!(
                        %method, url = %path_of(url),
                        "stormblock refused this call to the node token and no admin token was found \
                         (looked in {}); it is refused until one is provided, and retried by its caller",
                        self.inner.admin.describe()
                    );
                }
                Ok(resp)
            }
        }
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

/// A URL without its query, for a log line.
fn path_of(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
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

    /// stormblock#274's gate, in small: GET and POST take the node token (or
    /// the admin's); a template DELETE takes only the admin token. Records the
    /// bearer each request carried.
    async fn gated(node: &str, admin: Option<Arc<Mutex<String>>>) -> (String, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let log = seen.clone();
        let node = node.to_string();
        let app = axum::Router::new().route(
            "/api/v1/fstemplates/{t}",
            axum::routing::any(move |method: axum::http::Method, headers: axum::http::HeaderMap| {
                let (node, admin, log) = (node.clone(), admin.clone(), log.clone());
                async move {
                    let got = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.strip_prefix("Bearer "))
                        .unwrap_or("")
                        .to_string();
                    log.lock().unwrap().push(got.clone());
                    let is_admin = admin.as_ref().is_some_and(|a| *a.lock().unwrap() == got);
                    let is_node = got == node;
                    // No admin token configured: one token covers everything.
                    let ok = if method == axum::http::Method::DELETE && admin.is_some() {
                        is_admin
                    } else {
                        is_node || is_admin
                    };
                    if ok {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::UNAUTHORIZED
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    /// Distinct tokens (#105): the node token first; the delete it is refused
    /// is sent again with the admin token; a read never carries the admin's.
    #[tokio::test]
    async fn a_destructive_call_refused_to_the_node_token_presents_the_admin_token() {
        let dir = tempfile::tempdir().unwrap();
        let (nf, af) = (dir.path().join("api_token"), dir.path().join("admin_token"));
        std::fs::write(&nf, "node").unwrap();
        std::fs::write(&af, "admin1\n").unwrap();
        let admin = Arc::new(Mutex::new("admin1".to_string()));
        let (url, seen) = gated("node", Some(admin.clone())).await;
        let c = EngineClient::with_admin(&url, TokenSource::files([&nf]), TokenSource::files([&af]));
        let path = format!("{url}/api/v1/fstemplates/pvc-ext4j-64m");

        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(*seen.lock().unwrap(), ["node", "node", "admin1"]);

        // Rotation: the new admin token is read at the next such call.
        *admin.lock().unwrap() = "admin2".into();
        std::fs::write(&af, "admin2").unwrap();
        seen.lock().unwrap().clear();
        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(*seen.lock().unwrap(), ["node", "admin2"]);

        // A read refused to the node token is not escalated.
        let (url2, seen2) = gated("other", Some(admin)).await;
        let c2 = EngineClient::with_admin(&url2, TokenSource::files([&nf]), TokenSource::files([&af]));
        let p2 = format!("{url2}/api/v1/fstemplates/x");
        assert_eq!(c2.get(&p2).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert_eq!(*seen2.lock().unwrap(), ["node"]);
    }

    /// No admin token: the refusal goes back to the caller as it is (it
    /// reports it and retries on its next pass), once the token appears the
    /// same call succeeds.
    #[tokio::test]
    async fn a_missing_admin_token_leaves_the_refusal_visible_and_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let (nf, af) = (dir.path().join("api_token"), dir.path().join("admin_token"));
        std::fs::write(&nf, "node").unwrap();
        let (url, seen) = gated("node", Some(Arc::new(Mutex::new("adm".into())))).await;
        let c = EngineClient::with_admin(&url, TokenSource::files([&nf]), TokenSource::files([&af]));
        let path = format!("{url}/api/v1/fstemplates/t");

        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert_eq!(*seen.lock().unwrap(), ["node"]);

        std::fs::write(&af, "adm").unwrap();
        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::OK);

        // A wrong admin token is refused once and returned, not looped on.
        std::fs::write(&af, "stale").unwrap();
        seen.lock().unwrap().clear();
        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert_eq!(*seen.lock().unwrap(), ["node", "stale"]);
    }

    /// One token for everything (an engine before stormblock#274, or
    /// `admin_gate = audit`): the admin token is never presented.
    #[tokio::test]
    async fn a_single_token_engine_never_sees_the_admin_token() {
        let dir = tempfile::tempdir().unwrap();
        let (nf, af) = (dir.path().join("api_token"), dir.path().join("admin_token"));
        std::fs::write(&nf, "node").unwrap();
        std::fs::write(&af, "adm").unwrap();
        let (url, seen) = gated("node", None).await;
        let c = EngineClient::with_admin(&url, TokenSource::files([&nf]), TokenSource::files([&af]));
        let path = format!("{url}/api/v1/fstemplates/t");
        assert_eq!(c.delete(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.post(&path, &serde_json::json!({})).await.unwrap().status(), StatusCode::OK);
        assert_eq!(*seen.lock().unwrap(), ["node", "node"]);
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
        let src = TokenSource { explicit: Some("secret".into()), files: vec![], env: "$X" };
        let admin = TokenSource { explicit: Some("root-secret".into()), files: vec![], env: "$Y" };
        let c = EngineClient::with_admin(DEFAULT_URL, src, admin);
        let _ = c.token();
        let shown = format!("{c:?}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
