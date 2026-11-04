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
//!
//! **TLS (#312, stormblock#203, stormcos#374).** The URL's scheme decides. An
//! `https://` engine is verified against the CA file named by
//! `--stormblock-ca`, and only that: no public roots, and no way to skip the
//! check. [`EngineTrust`] reads the file at each new connection, so a CA that
//! stormcert writes after the kubelet starts, or renews, is used without a
//! restart. Until there is one every connection is refused before a request
//! (and so the token) is sent. `http://` keeps working until the engine's
//! `tls_mode = "required"`. The token is still what identifies the kubelet:
//! it presents no client pair.

use std::path::{Path, PathBuf};
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

/// How long a refused call waits for a new token on disk before any token
/// has been accepted (#290): the engine can answer before it has written this
/// boot's token, and the kubelet then holds last boot's.
pub const TOKEN_WAIT: std::time::Duration = std::time::Duration::from_secs(3);
const TOKEN_POLL: std::time::Duration = std::time::Duration::from_millis(50);

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

    /// Only these files, and no environment: stormstorage's token (#40), and
    /// tests.
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
    /// Whether the engine has answered anything but a 401 (#290). Until it
    /// has, a refusal waits [`TOKEN_WAIT`] for a new token on disk.
    accepted: AtomicBool,
    /// The token a wait gave up on: not waited for again until it changes.
    gave_up_on: Mutex<Option<Option<String>>>,
    /// The CA an `https://` engine is verified against, for `{:?}`.
    ca: Option<PathBuf>,
}

/// What an `https://` engine's certificate is checked against (#312): the CA
/// file, read at each new connection.
///
/// A file that is not there yet, or has never held a usable CA, refuses the
/// connection. Once one has been used, a file that goes or stops parsing (a
/// half-written renewal) keeps the last good CA, said in the log: it was
/// trusted a moment ago, and losing the engine would not make anything safer.
#[derive(Debug)]
pub struct EngineTrust {
    ca: Option<PathBuf>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    /// The CA's bytes as last used, and the verifier made from them.
    current: Mutex<Option<(Vec<u8>, Arc<rustls::client::WebPkiServerVerifier>)>>,
    /// The last problem logged: said once, not on every connection.
    said: Mutex<Option<String>>,
}

impl EngineTrust {
    pub fn new(ca: Option<&Path>) -> EngineTrust {
        EngineTrust {
            ca: ca.map(Path::to_path_buf),
            provider: Arc::new(rustls::crypto::ring::default_provider()),
            current: Mutex::new(None),
            said: Mutex::new(None),
        }
    }

    fn say(&self, what: String) {
        let mut said = self.said.lock().unwrap_or_else(|e| e.into_inner());
        if said.as_deref() != Some(what.as_str()) {
            warn!("stormblock engine TLS: {what}");
            *said = Some(what);
        }
    }

    /// A verifier for what the file holds now.
    fn build(&self, pem: &[u8]) -> Result<Arc<rustls::client::WebPkiServerVerifier>, String> {
        use rustls::pki_types::pem::PemObject;
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
            roots.add(c.map_err(|e| format!("{e:?}"))?).map_err(|e| e.to_string())?;
        }
        if roots.is_empty() {
            return Err("no certificate in it".to_string());
        }
        rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), self.provider.clone())
            .build()
            .map_err(|e| e.to_string())
    }

    /// The verifier to use for a connection being made now.
    fn verifier(&self) -> Result<Arc<rustls::client::WebPkiServerVerifier>, String> {
        let Some(path) = &self.ca else {
            return Err("an https engine URL needs a CA, and no --stormblock-ca was given".to_string());
        };
        let read = std::fs::read(path);
        let mut current = self.current.lock().unwrap_or_else(|e| e.into_inner());
        let problem = match read {
            Ok(pem) => {
                if let Some((_, verifier)) = current.as_ref().filter(|(held, _)| *held == pem) {
                    return Ok(verifier.clone());
                }
                match self.build(&pem) {
                    Ok(verifier) => {
                        info!("stormblock engine TLS: verifying against {}", path.display());
                        *current = Some((pem, verifier.clone()));
                        return Ok(verifier);
                    }
                    Err(e) => format!("CA file {} is not usable: {e}", path.display()),
                }
            }
            Err(e) => format!("CA file {} cannot be read: {e}", path.display()),
        };
        match current.as_ref() {
            Some((_, verifier)) => {
                self.say(format!("{problem}; keeping the CA last read from it"));
                Ok(verifier.clone())
            }
            None => Err(problem),
        }
    }

    /// The client TLS config: this verifier, no client certificate.
    fn client_config(self) -> rustls::ClientConfig {
        rustls::ClientConfig::builder_with_provider(self.provider.clone())
            .with_safe_default_protocol_versions()
            .expect("ring's provider supports the default TLS versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(self))
            .with_no_client_auth()
    }
}

impl rustls::client::danger::ServerCertVerifier for EngineTrust {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let verifier = self.verifier().map_err(|e| {
            self.say(format!("connection refused: {e}"));
            rustls::Error::General(e)
        })?;
        let checked = verifier.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now);
        match &checked {
            // reqwest's error says only "error sending request": the reason is here.
            Err(e) => self.say(format!("the engine's certificate was refused: {e}")),
            Ok(_) => *self.said.lock().unwrap_or_else(|e| e.into_inner()) = None,
        }
        checked
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
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
            .field("ca", &self.inner.ca)
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

    /// [`Self::from_env`], with the CA an `https://` engine is verified
    /// against (#312).
    pub fn from_env_with_ca(url: &str, ca: Option<&Path>) -> EngineClient {
        EngineClient::with_tls(url, TokenSource::from_env(), TokenSource::admin_from_env(), ca)
    }

    /// The node token only: destructive verbs are refused on an enforcing
    /// engine.
    pub fn new(url: &str, source: TokenSource) -> EngineClient {
        EngineClient::with_admin(url, source, TokenSource::none())
    }

    /// The node token, and the admin token for what the node token is
    /// refused (#105).
    pub fn with_admin(url: &str, source: TokenSource, admin: TokenSource) -> EngineClient {
        EngineClient::with_tls(url, source, admin, None)
    }

    /// Both tokens, and the CA file an `https://` engine is verified against.
    /// With no CA an `https://` engine is refused: public roots never apply.
    pub fn with_tls(url: &str, source: TokenSource, admin: TokenSource, ca: Option<&Path>) -> EngineClient {
        // Timed on its own (#262): a boot spent 3 s between the line before
        // this client and the line after it, with nothing logged.
        let building = std::time::Instant::now();
        // No client-wide timeout: each request carries its own, and the
        // volume watch carries none (#99).
        // Built or nothing: `unwrap_or_default` here would be a client that
        // trusts the public roots.
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .use_preconfigured_tls(EngineTrust::new(ca).client_config())
            .build()
            .expect("the engine's HTTP client builds with its own rustls config");
        if building.elapsed() > std::time::Duration::from_millis(100) {
            tracing::warn!("building the stormblock engine's HTTP client took {} ms", building.elapsed().as_millis());
        }
        EngineClient {
            inner: Arc::new(Inner {
                http,
                url: url.trim_end_matches('/').to_string(),
                source,
                admin,
                said_no_admin: AtomicBool::new(false),
                token: Mutex::new(None),
                said_none: AtomicBool::new(false),
                accepted: AtomicBool::new(false),
                gave_up_on: Mutex::new(None),
                ca: ca.map(Path::to_path_buf),
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
        let began = std::time::Instant::now();
        let answered = self.send_authorised(method.clone(), url, body, timeout).await;
        // A slow engine, seen from the kubelet's side (#285): which call, how
        // long (a clone stormblock took 10–20 s to make held a pod's start).
        // The volume watch (no timeout) is long by design and not said.
        let took = began.elapsed();
        if timeout.is_some() && took > crate::stormpump_ring::SLOW_CALL {
            warn!(
                %method, path = %path_of(url), ms = took.as_millis() as u64,
                status = answered.as_ref().map(|r| r.status().as_u16()).unwrap_or(0),
                "stormblock took {} ms to answer {method} {}", took.as_millis(), path_of(url)
            );
        }
        answered
    }

    /// [`Self::send_within`]'s call, with the token re-read or the admin
    /// credential on a refusal.
    async fn send_authorised(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
        timeout: Option<std::time::Duration>,
    ) -> reqwest::Result<Response> {
        let used = self.token();
        let resp = self.once(method.clone(), url, body, used.as_deref(), timeout).await?;
        if resp.status() != StatusCode::UNAUTHORIZED {
            self.inner.accepted.store(true, Ordering::Relaxed);
            return Ok(resp);
        }
        let mut fresh = self.reload();
        if fresh == used {
            fresh = self.await_new_token(&used).await;
        }
        let mut resp = resp;
        if fresh.is_some() && fresh != used {
            info!("stormblock refused the engine token; retrying with the one now on disk");
            resp = self.once(method.clone(), url, body, fresh.as_deref(), timeout).await?;
            if resp.status() != StatusCode::UNAUTHORIZED {
                self.inner.accepted.store(true, Ordering::Relaxed);
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
        // Retried on a transient failure (#211): the engine is away only
        // while it (re)starts. A POST is repeated only when it never reached
        // the engine; one that may have been applied (a clone, an attach, a
        // mint) is checked by its caller, which finds it again by name.
        retry::send(req, retry::Policy::ENGINE).await
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

    /// At boot (#290): the engine refused `used` and nothing newer is on
    /// disk. Before it has accepted any token, that is this boot's engine
    /// answering before it wrote its token, or the kubelet holding last boot's
    /// (a fallback file outlives the reboot): the file is read every
    /// [`TOKEN_POLL`] for up to [`TOKEN_WAIT`], instead of the refusal going
    /// back to a caller that retries seconds later. Once a token has been
    /// accepted, or a wait for this one gave up, `used` is returned at once.
    async fn await_new_token(&self, used: &Option<String>) -> Option<String> {
        if self.inner.accepted.load(Ordering::Relaxed) {
            return used.clone();
        }
        if self.inner.gave_up_on.lock().unwrap_or_else(|e| e.into_inner()).as_ref() == Some(used) {
            return used.clone();
        }
        let began = std::time::Instant::now();
        while began.elapsed() < TOKEN_WAIT {
            tokio::time::sleep(TOKEN_POLL).await;
            let fresh = self.reload();
            if fresh.is_some() && fresh != *used {
                info!(ms = began.elapsed().as_millis() as u64, "stormblock engine token rewritten; waited for it");
                return fresh;
            }
        }
        warn!(
            "stormblock refused the engine token and no other appeared in {} ms (looked in {})",
            TOKEN_WAIT.as_millis(),
            self.inner.source.describe()
        );
        *self.inner.gave_up_on.lock().unwrap_or_else(|e| e.into_inner()) = Some(used.clone());
        used.clone()
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
        // Each attempt is bounded; the retries (#211) are too: five attempts
        // and their backoff, well under the engine policy's deadline.
        assert!(started.elapsed() < std::time::Duration::from_secs(15));
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

        // Written while the first call is refused: that call waits for it.
        let writer = {
            let file = file.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                std::fs::write(&file, "minted").unwrap();
            })
        };
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        writer.await.unwrap();
    }

    /// #290: at boot the kubelet reads last boot's token (or the engine
    /// listens before it writes this boot's). The first refused call waits
    /// for the new one and succeeds, instead of going back to its caller.
    #[tokio::test]
    async fn a_stale_token_at_boot_waits_for_the_new_one() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        std::fs::write(&file, "last-boot").unwrap();
        let (url, seen) = engine(Arc::new(Mutex::new("this-boot".into()))).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let path = format!("{url}/api/v1/volumes");
        let writer = {
            let file = file.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                std::fs::write(&file, "this-boot\n").unwrap();
            })
        };
        let began = std::time::Instant::now();
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        let took = began.elapsed();
        assert!(took >= std::time::Duration::from_millis(300) && took < std::time::Duration::from_secs(1), "{took:?}");
        assert_eq!(seen.load(Ordering::SeqCst), 2, "refused once, then the new token");
        writer.await.unwrap();
    }

    /// A token that is never replaced is waited for once, not on every call.
    #[tokio::test]
    async fn a_wait_that_gave_up_is_not_repeated_for_the_same_token() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        std::fs::write(&file, "wrong").unwrap();
        let (url, seen) = engine(Arc::new(Mutex::new("right".into()))).await;
        let c = EngineClient::new(&url, TokenSource::files([&file]));
        let path = format!("{url}/api/v1/volumes");
        let began = std::time::Instant::now();
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert!(began.elapsed() >= TOKEN_WAIT);
        let began = std::time::Instant::now();
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::UNAUTHORIZED);
        assert!(began.elapsed() < std::time::Duration::from_millis(500), "{:?}", began.elapsed());
        assert_eq!(seen.load(Ordering::SeqCst), 2);
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
        // The node token accepted first (as the kubelet's first listing is):
        // the delete's refusal is the gate's, not a stale token's (#290).
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        seen.lock().unwrap().clear();

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

    /// A CA for the TLS tests, and a server certificate from it.
    struct TestCa {
        cert: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    impl TestCa {
        fn new() -> TestCa {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            TestCa { cert: params.self_signed(&key).unwrap(), key }
        }

        fn pem(&self) -> String {
            self.cert.pem()
        }

        /// A TLS stand-in for the engine with a certificate from this CA for
        /// `name`: its URL, and the tokens of the requests that reached it.
        async fn engine(&self, name: &str) -> (String, Arc<Mutex<Vec<String>>>) {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = rcgen::CertificateParams::new(vec![name.to_string()])
                .unwrap()
                .signed_by(&key, &self.cert, &self.key)
                .unwrap();
            let seen = Arc::new(Mutex::new(Vec::<String>::new()));
            let log = seen.clone();
            let app = axum::Router::new().route(
                "/api/v1/volumes",
                axum::routing::any(move |headers: axum::http::HeaderMap| {
                    let log = log.clone();
                    async move {
                        let got = headers.get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
                        log.lock().unwrap().push(got.to_string());
                        "[]"
                    }
                }),
            );
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
                cert.pem().into_bytes(),
                key.serialize_pem().into_bytes(),
            )
            .await
            .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(axum_server::from_tcp_rustls(listener, tls).serve(app.into_make_service()));
            (format!("https://127.0.0.1:{port}"), seen)
        }
    }

    fn tls_client(url: &str, ca: Option<&Path>) -> EngineClient {
        let token = TokenSource { explicit: Some("node".into()), files: vec![], env: "$X" };
        EngineClient::with_tls(url, token, TokenSource::none(), ca)
    }

    /// An engine serving TLS from the configured CA answers, GET and POST, and
    /// the token goes with them (#312's done-when, first half).
    #[tokio::test]
    async fn an_https_engine_from_the_configured_ca_is_reached_with_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new();
        let file = dir.path().join("ca.crt");
        std::fs::write(&file, ca.pem()).unwrap();
        let (url, seen) = ca.engine("127.0.0.1").await;
        let c = tls_client(&url, Some(&file));
        let path = format!("{url}/api/v1/volumes");
        assert_eq!(c.get(&path).await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.post(&path, &serde_json::json!({})).await.unwrap().status(), StatusCode::OK);
        assert_eq!(*seen.lock().unwrap(), ["Bearer node", "Bearer node"]);
    }

    /// The other half: another CA's certificate fails the handshake, so
    /// nothing, the token included, reaches the server.
    #[tokio::test]
    async fn a_certificate_from_another_ca_is_refused_and_no_token_is_sent() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ca.crt");
        std::fs::write(&file, TestCa::new().pem()).unwrap();
        let (url, seen) = TestCa::new().engine("127.0.0.1").await;
        let c = tls_client(&url, Some(&file));
        assert!(c.get(&format!("{url}/api/v1/volumes")).await.is_err());
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_certificate_for_another_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let ca = TestCa::new();
        let file = dir.path().join("ca.crt");
        std::fs::write(&file, ca.pem()).unwrap();
        let (url, seen) = ca.engine("other.example").await;
        let c = tls_client(&url, Some(&file));
        assert!(c.get(&format!("{url}/api/v1/volumes")).await.is_err());
        assert!(seen.lock().unwrap().is_empty());
    }

    /// No CA configured: refused, never verified against anything else.
    #[tokio::test]
    async fn an_https_engine_with_no_ca_configured_is_refused() {
        let (url, seen) = TestCa::new().engine("127.0.0.1").await;
        let c = tls_client(&url, None);
        assert!(c.get(&format!("{url}/api/v1/volumes")).await.is_err());
        assert!(seen.lock().unwrap().is_empty());
    }

    /// stormcert may write the CA after the kubelet starts, and renews it: the
    /// next connection reads the file, with no restart. The old CA's
    /// certificates are refused once it has been replaced.
    #[tokio::test]
    async fn the_ca_file_is_read_again_for_each_new_connection() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ca.crt");
        let (old, new) = (TestCa::new(), TestCa::new());
        let (old_url, _) = old.engine("127.0.0.1").await;
        let c = tls_client(&old_url, Some(&file));
        let path = |u: &str| format!("{u}/api/v1/volumes");

        assert!(c.get(&path(&old_url)).await.is_err(), "the CA is not there yet");
        std::fs::write(&file, old.pem()).unwrap();
        assert_eq!(c.get(&path(&old_url)).await.unwrap().status(), StatusCode::OK);

        std::fs::write(&file, new.pem()).unwrap();
        let (new_url, _) = new.engine("127.0.0.1").await;
        assert_eq!(c.get(&path(&new_url)).await.unwrap().status(), StatusCode::OK);
        let (old_again, _) = old.engine("127.0.0.1").await;
        assert!(c.get(&path(&old_again)).await.is_err());
    }

    #[test]
    fn a_file_that_goes_bad_keeps_the_last_good_ca_and_one_never_good_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ca.crt");
        let trust = EngineTrust::new(Some(&file));
        assert!(trust.verifier().unwrap_err().contains("cannot be read"));
        std::fs::write(&file, "half a renewal").unwrap();
        assert!(trust.verifier().unwrap_err().contains("not usable"));
        std::fs::write(&file, TestCa::new().pem()).unwrap();
        let good = trust.verifier().unwrap();
        std::fs::write(&file, "half a renewal").unwrap();
        assert!(Arc::ptr_eq(&trust.verifier().unwrap(), &good));
        std::fs::remove_file(&file).unwrap();
        assert!(Arc::ptr_eq(&trust.verifier().unwrap(), &good));
        assert!(EngineTrust::new(None).verifier().unwrap_err().contains("--stormblock-ca"));
    }

    /// `http://` is untouched by a CA being configured: stormcos can set the
    /// CA before it moves the URL.
    #[tokio::test]
    async fn a_plain_engine_still_works_with_a_ca_configured() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("ca.crt");
        std::fs::write(&file, TestCa::new().pem()).unwrap();
        let (url, seen) = gated("node", None).await;
        let c = tls_client(&url, Some(&file));
        assert_eq!(c.get(&format!("{url}/api/v1/fstemplates/t")).await.unwrap().status(), StatusCode::OK);
        assert_eq!(*seen.lock().unwrap(), ["node"]);
    }
}
