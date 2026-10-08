//! The kubelet's inbound HTTP server (upstream `:10250`).
//!
//! Liveness, a Prometheus `/metrics` endpoint, `/stats/summary`, `/pods` (the
//! pods this kubelet manages) and `/containerLogs` — the endpoint the
//! apiserver proxies `kubectl logs` to (rustkube-node#34). `/portForward`
//! speaks SPDY/3.1 (`spdy.rs`) or a WebSocket tunnel of it (#56); `/exec` and
//! `/attach` answer 501 until the engine can run a process inside a running
//! container (stormpump#103). Served over HTTPS with bearer-token auth
//! (rustkube-node#9).
//!
//! Two routes are here because the thing they reach is on the node and the
//! control plane cannot get to it: `/vmConsole` (stormvm's console doors) and
//! `DELETE /volumes` (stormblock). Both keep the blast radius at one node and
//! reuse a hop the apiserver already authenticates, rather than giving a
//! controller credentials to every node's engine.
//!
//! The console doors are **mounted, not dialled** (rustkube-node#43):
//! `stormvm-console` exposes its router as a library, so the last hop is a
//! function call rather than a socket to a second process. There is no
//! standalone node — every node runs rustkube, so every node with a VM on it
//! has a kubelet, and a daemon whose only job was to serve consoles was a
//! process that never needed to exist.

use retry::RetryExt;
use crate::pod_manager::{PodManager, VolumeRelease};
use axum::extract::{ConnectInfo, FromRef, Path, Query, Request, State};
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing::{delete, get, put}, Json, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

/// TLS + auth configuration for the inbound `:10250` server (rustkube-node#9).
#[derive(Clone)]
pub struct ServerConfig {
    /// PEM serving cert + key. When either is absent, a self-signed cert is
    /// generated at startup (SANs = node name + node IP).
    pub tls_cert: Option<Vec<u8>>,
    pub tls_key: Option<Vec<u8>>,
    /// The files the pair was read from: watched, and the pair reloaded when
    /// they change (#89, stormcert renews it at boot).
    pub tls_cert_path: Option<std::path::PathBuf>,
    pub tls_key_path: Option<std::path::PathBuf>,
    pub node_name: String,
    pub node_ip: String,
    /// Static bearer token accepted for inbound auth (e.g. a monitoring scraper).
    pub auth_token: Option<String>,
    /// Authenticated apiserver client + URL, used to validate bearer tokens via
    /// TokenReview.
    pub api_client: reqwest::Client,
    pub api_url: String,
    /// Serve all routes unauthenticated (dev only).
    pub anonymous: bool,
    /// This node's stormblock engine (`--stormblock`), for stormvm's
    /// `snapshot` verb (#83). The console finds the engine token itself, in
    /// the same places `engine.rs` looks.
    pub stormblock_url: String,
}

#[derive(Clone)]
struct AuthState {
    auth_token: Option<String>,
    api_client: reqwest::Client,
    api_url: String,
    anonymous: bool,
}

/// Everything the routes need: the pods this kubelet manages, and stormvm's
/// console doors as a mounted router.
///
/// A struct rather than a bare `Arc<PodManager>` only so the console can come
/// along; [`FromRef`] keeps every existing `State<Arc<PodManager>>` handler
/// working unchanged.
#[derive(Clone)]
struct AppState {
    pods: Arc<PodManager>,
    /// Virtual machines, when this node has an engine to run them.
    ///
    /// Held so the kubelet can answer "who is at this address" from the one
    /// place that knows — rather than pushing a copy into a metadata service
    /// and keeping two records of one fact in step by hand.
    vms: Option<Arc<crate::vm_manager::VmManager>>,
    /// stormvm's console router, held so `/vmConsole` can hand a request
    /// straight to it. Cloning shares its sessions and tokens — they live
    /// behind an `Arc` inside — so a clone per request is not a second
    /// console service.
    console: Router,
}

impl FromRef<AppState> for Arc<PodManager> {
    fn from_ref(state: &AppState) -> Arc<PodManager> {
        state.pods.clone()
    }
}

impl FromRef<AppState> for Option<Arc<crate::vm_manager::VmManager>> {
    fn from_ref(state: &AppState) -> Option<Arc<crate::vm_manager::VmManager>> {
        state.vms.clone()
    }
}

impl FromRef<AppState> for Router {
    fn from_ref(state: &AppState) -> Router {
        state.console.clone()
    }
}

/// Build the (unauthenticated) router — exposed for tests.
pub fn router(pod_manager: Arc<PodManager>) -> Router {
    router_with_console(
        pod_manager,
        None,
        console(crate::vm_manager::RUN_ROOT, crate::engine::DEFAULT_URL),
    )
}

/// stormvm's console router over `run_dir`, told where this node's stormblock
/// engine is.
///
/// Without `stormblock` the router still serves the doors and every other
/// verb, but `snapshot` answers 409 "this service was not told where
/// stormblock is" (#83) — so both mounts go through here.
fn console(run_dir: &str, stormblock: &str) -> Router {
    stormvm_console::router(stormvm_console::Config {
        run_dir: run_dir.to_string(),
        stormblock: Some(stormblock.to_string()),
        ..Default::default()
    })
}

/// The router, over a given console. Tests build one against a run directory
/// of their own; nothing else needs this.
fn router_with_console(
    pod_manager: Arc<PodManager>,
    vms: Option<Arc<crate::vm_manager::VmManager>>,
    console: Router,
) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(healthz))
        .route("/readyz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/metrics/cadvisor", get(metrics_cadvisor))
        .route("/stats/summary", get(stats_summary))
        .route("/pods", get(pods))
        // What `kubectl logs` reads, by way of the apiserver proxy.
        .route("/containerLogs/{namespace}/{pod}/{container}", get(container_logs))
        // A VM's console — what the apiserver's subresources.kubevirt.io
        // handler proxies to (rustkube#61), answered by stormvm's own router
        // mounted below (rustkube-node#43).
        .route("/vmConsole/{namespace}/{name}/{door}", get(vm_console))
        // A VM's verbs (#94): what the apiserver's subresources.kubevirt.io
        // verb handlers proxy to (rustkube#141), answered by the same router.
        .route("/vmVerb/{namespace}/{name}/{verb}", put(vm_verb))
        // The streaming subresources the apiserver splices through (#56):
        // `kubectl port-forward`, `exec`, `attach`. GET for a WebSocket
        // handshake, POST for SPDY.
        .route("/portForward/{namespace}/{pod}", get(port_forward).post(port_forward))
        .route("/exec/{namespace}/{pod}/{container}", get(exec_refused).post(exec_refused))
        .route("/attach/{namespace}/{pod}/{container}", get(attach_refused).post(attach_refused))
        // Who is at this address.
        //
        // Meant for the metadata service. The kubelet already holds the VMI,
        // the MAC it generated, the addresses the guest was given and the
        // whole lifecycle, so a second copy anywhere else is a copy that can
        // be wrong. stormimds does keep its own store today
        // (`/admin/instances`); which design wins is undecided
        // (stormimds#12).
        .route("/vmInstance/{address}", get(vm_instance))
        // Release the stormblock clone behind a claim, so a `Delete` reclaim
        // policy finishes (rustkube-node#46). Same shape as the VM console:
        // control plane → kubelet → a service the control plane cannot reach.
        // `DELETE` rather than a verb under some other path because it
        // destroys data, and should read that way in an audit log.
        .route("/volumes/{namespace}/{claim}", delete(release_volume))
        .with_state(AppState { pods: pod_manager, console, vms })
}

/// Serve the kubelet API over HTTPS on `0.0.0.0:<port>` with bearer-token auth
/// on everything except the health endpoints. Runs until the process exits.
pub async fn serve(
    port: u16,
    pod_manager: Arc<PodManager>,
    vms: Option<Arc<crate::vm_manager::VmManager>>,
    config: ServerConfig,
) {
    // rustls needs a process-wide crypto provider; installing is idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();

    if config.anonymous {
        warn!("kubelet server: anonymous auth enabled — :{port} endpoints are unauthenticated");
    }
    let auth = AuthState {
        auth_token: config.auth_token.clone(),
        api_client: config.api_client.clone(),
        api_url: config.api_url.clone(),
        anonymous: config.anonymous,
    };
    let app = router_with_console(
        pod_manager,
        vms,
        console(crate::vm_manager::RUN_ROOT, &config.stormblock_url),
    )
    .layer(middleware::from_fn_with_state(auth, auth_mw));

    // Serving cert: use the provided pair, else self-sign, and say which
    // (#89). A pair that was named and is missing never gets here: main waits
    // for it and then exits (#69).
    let (cert_pem, key_pem) = match (&config.tls_cert, &config.tls_key) {
        (Some(c), Some(k)) => {
            info!(
                "kubelet server: serving the configured pair ({})",
                config.tls_cert_path.as_ref().map_or("given".into(), |p| p.display().to_string())
            );
            (c.clone(), k.clone())
        }
        _ => {
            warn!("kubelet server: no --tls-cert-file: serving a self-signed certificate for {}", config.node_name);
            match self_signed_cert(&config.node_name, &config.node_ip) {
                Ok(pair) => pair,
                Err(e) => {
                    warn!("kubelet server: self-signed cert generation failed: {e}");
                    return;
                }
            }
        }
    };
    let tls = match axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem.clone(), key_pem.clone()).await {
        Ok(t) => t,
        Err(e) => {
            warn!("kubelet server: TLS config failed: {e}");
            return;
        }
    };
    if let (Some(cert), Some(key)) = (config.tls_cert_path.clone(), config.tls_key_path.clone()) {
        tokio::spawn(reload_on_change(tls.clone(), cert, key, (cert_pem, key_pem)));
    }

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    info!(
        "kubelet server listening on https://0.0.0.0:{port} (auth: {})",
        if config.anonymous { "anonymous" } else { "bearer-token" }
    );
    if let Err(e) = axum_server::bind_rustls(addr, tls)
        .serve(app.into_make_service())
        .await
    {
        warn!("kubelet server exited: {e}");
    }
}

/// Auth gate: health/liveness/readiness are always open; everything else needs a
/// valid bearer token (a configured static token, or one the apiserver accepts
/// via TokenReview).
async fn auth_mw(State(auth): State<AuthState>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    let exempt = matches!(path, "/healthz" | "/livez" | "/readyz");
    if exempt || auth.anonymous {
        return next.run(req).await;
    }
    let token = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::to_string);
    match token {
        Some(t) if authorize(&auth, &t).await => next.run(req).await,
        _ => (StatusCode::UNAUTHORIZED, "Unauthorized\n").into_response(),
    }
}

async fn authorize(auth: &AuthState, token: &str) -> bool {
    if let Some(expected) = &auth.auth_token {
        if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
            return true;
        }
    }
    token_review(auth, token).await
}

/// Validate a token via the apiserver's TokenReview API (best-effort — succeeds
/// only if the apiserver implements it and the token authenticates).
async fn token_review(auth: &AuthState, token: &str) -> bool {
    let url = format!(
        "{}/apis/authentication.k8s.io/v1/tokenreviews",
        auth.api_url.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": { "token": token }
    });
    match auth.api_client.post(&url).json(&body).send_repeatable(retry::Policy::API).await {
        Ok(resp) => resp
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|v| v["status"]["authenticated"].as_bool())
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Length-checked constant-time byte comparison (avoids token timing leaks).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Reload the serving pair whenever its files change (#89): stormcert renews
/// it at boot (stormcert#14), and a kubelet that read it once served the old
/// one until it restarted. Watches the pair's directory; reloads only when
/// the bytes differ from what is served.
async fn reload_on_change(
    tls: axum_server::tls_rustls::RustlsConfig,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    mut served: (Vec<u8>, Vec<u8>),
) {
    let changed = Arc::new(tokio::sync::Notify::new());
    let dir = cert.parent().map(|d| d.to_path_buf()).unwrap_or_else(|| cert.clone());
    let notify = changed.clone();
    tokio::spawn(async move { crate::fs_watch::watch(dir, move || notify.notify_one()).await });
    loop {
        changed.notified().await;
        // A writer that replaces both files fires twice; the second look sees both.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if let Some(now) = reload_pair(&tls, &cert, &key, &served).await {
            served = now;
        }
    }
}

/// Read the pair; when it differs from `served`, load it into `tls`. The new
/// pair when it was loaded; `None` when unchanged, unreadable or refused (a
/// half-written pair: the old one stays until the next change).
async fn reload_pair(
    tls: &axum_server::tls_rustls::RustlsConfig,
    cert: &std::path::Path,
    key: &std::path::Path,
    served: &(Vec<u8>, Vec<u8>),
) -> Option<(Vec<u8>, Vec<u8>)> {
    let (Ok(c), Ok(k)) = (std::fs::read(cert), std::fs::read(key)) else { return None };
    if c.is_empty() || k.is_empty() || (c.as_slice(), k.as_slice()) == (served.0.as_slice(), served.1.as_slice()) {
        return None;
    }
    match tls.reload_from_pem(c.clone(), k.clone()).await {
        Ok(()) => {
            info!("kubelet server: serving pair reloaded from {}", cert.display());
            Some((c, k))
        }
        Err(e) => {
            warn!("kubelet server: {} changed but does not load yet ({e}); still serving the previous pair", cert.display());
            None
        }
    }
}

/// Generate a self-signed serving cert (PEM cert, PEM key) for the node.
fn self_signed_cert(node_name: &str, node_ip: &str) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let mut sans = vec![node_name.to_string(), "localhost".to_string()];
    if !node_ip.is_empty() {
        sans.push(node_ip.to_string());
    }
    let key = rcgen::generate_simple_self_signed(sans)?;
    Ok((
        key.cert.pem().into_bytes(),
        key.key_pair.serialize_pem().into_bytes(),
    ))
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

/// The kubelet's own metrics, under upstream's names (#36; `metrics.rs`).
async fn metrics(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    let snap = pm.metrics_snapshot().await;
    ([("content-type", "text/plain; version=0.0.4")], crate::metrics::render_kubelet(&snap))
}

/// The header a metadata service forwards a host-network workload's
/// ServiceAccount token in (#122).
pub const WORKLOAD_TOKEN: &str = "x-storm-workload-token";

/// `GET /vmInstance/{address}` — the instance metadata for whoever holds it.
///
/// 404 for an address this node is not running a machine for, which is the
/// honest answer and the one a metadata service should pass on: a guest that
/// is not here is not this node's to describe.
async fn vm_instance(
    State(vms): State<Option<Arc<crate::vm_manager::VmManager>>>,
    Path(address): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    // A host-network workload's own ServiceAccount token, forwarded by the
    // metadata service (#122). Not `Authorization`: that is the caller's
    // credential to this kubelet, and `auth_mw` has spent it.
    let workload_token = headers
        .get(WORKLOAD_TOKEN)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().trim_start_matches("Bearer ").to_string());
    let Some(vms) = vms else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "this node runs no machines"})))
            .into_response();
    };
    match vms.instance_for(&address, workload_token.as_deref()).await {
        // A cold cache is 503, not 404.
        //
        // "I have not synced" and "no such machine" are different answers and
        // only one is safe to act on: a guest told the second at boot
        // configures itself as nobody and does not ask again. 503 with
        // Retry-After is what a client already knows to wait on.
        Some(v) if v.get("storm.io/cold").is_some() => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "2")],
            Json(serde_json::json!({"error": "this node has not synced yet"})),
        )
            .into_response(),
        // A cache the apiserver has not confirmed within the bound (#156):
        // the machine may have moved while this node was cut off.
        Some(v) if v.get("storm.io/stale").is_some() => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "5")],
            Json(serde_json::json!({"error": format!(
                "this node has not heard from the apiserver for {}s",
                v["storm.io/stale"].as_u64().unwrap_or_default()
            )})),
        )
            .into_response(),
        Some(v) => Json(v).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("no machine at {address} on this node")})),
        )
            .into_response(),
    }
}

/// The subprotocol `kubectl port-forward` asks for over SPDY.
const PORT_FORWARD_PROTOCOL: &str = "portforward.k8s.io";
/// ... and over a WebSocket: SPDY/3.1 tunnelled in binary messages (KEP-4006).
const PORT_FORWARD_TUNNEL: &str = "SPDY/3.1+portforward.k8s.io";

/// `/portForward/{namespace}/{pod}` (#56): upgrade to SPDY/3.1, or to a
/// WebSocket carrying SPDY, and forward each requested connection into the
/// pod's network namespace (`portforward.rs`).
///
/// A request this cannot serve is answered without a 101, which is what makes
/// client-go fall back from its WebSocket attempt to plain SPDY.
async fn port_forward(
    State(pm): State<Arc<PodManager>>,
    Path((namespace, pod)): Path<(String, String)>,
    mut req: Request,
) -> Response {
    let target = match pm.pod_network(&namespace, &pod).await {
        None => {
            return (StatusCode::NOT_FOUND, format!("pod {namespace}/{pod} not found on this node\n"))
                .into_response()
        }
        Some(Err(e)) => return (StatusCode::SERVICE_UNAVAILABLE, format!("{e}\n")).into_response(),
        Some(Ok((uid, netns))) => crate::portforward::Target { pod: format!("{namespace}/{pod}"), uid, netns },
    };
    let header = |name: &str| -> Vec<String> {
        req.headers()
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect()
    };
    let upgrade = header("upgrade");
    let websocket = upgrade.iter().any(|u| u.eq_ignore_ascii_case("websocket"));
    let spdy = upgrade.iter().any(|u| u.eq_ignore_ascii_case("SPDY/3.1"));
    let protocols = header(if websocket { "sec-websocket-protocol" } else { "x-stream-protocol-version" });
    let key = header("sec-websocket-key").into_iter().next();
    let wanted = if websocket { PORT_FORWARD_TUNNEL } else { PORT_FORWARD_PROTOCOL };
    if !websocket && !spdy {
        return (StatusCode::BAD_REQUEST, "port-forward needs an upgrade to SPDY/3.1 or a WebSocket\n").into_response();
    }
    if !protocols.iter().any(|p| p == wanted) {
        return (
            StatusCode::FORBIDDEN,
            format!("unable to upgrade: port-forward speaks {wanted}; the client offered {protocols:?}\n"),
        )
            .into_response();
    }
    let Some(on_upgrade) = req.extensions_mut().remove::<hyper::upgrade::OnUpgrade>() else {
        return (StatusCode::BAD_REQUEST, "the connection cannot be upgraded\n").into_response();
    };
    let mut resp = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "Upgrade");
    if websocket {
        let Some(key) = key else {
            return (StatusCode::BAD_REQUEST, "a WebSocket handshake needs Sec-WebSocket-Key\n").into_response();
        };
        resp = resp
            .header("upgrade", "websocket")
            .header("sec-websocket-accept", websocket_accept(&key))
            .header("sec-websocket-protocol", PORT_FORWARD_TUNNEL);
    } else {
        resp = resp.header("upgrade", "SPDY/3.1").header("x-stream-protocol-version", PORT_FORWARD_PROTOCOL);
    }
    tokio::spawn(async move {
        let io = match on_upgrade.await {
            Ok(u) => hyper_util::rt::TokioIo::new(u),
            Err(e) => {
                warn!("port-forward {}: upgrade failed: {e}", target.pod);
                return;
            }
        };
        let streams = if websocket {
            use tokio_tungstenite::tungstenite::protocol::Role;
            let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(io, Role::Server, None).await;
            crate::spdy::serve(websocket_bytes(ws))
        } else {
            crate::spdy::serve(io)
        };
        crate::portforward::serve(streams, target).await;
    });
    resp.body(axum::body::Body::empty()).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `Sec-WebSocket-Accept` for a key (RFC 6455).
fn websocket_accept(key: &str) -> String {
    use base64::Engine;
    use sha1::Digest;
    let mut h = sha1::Sha1::new();
    h.update(key.as_bytes());
    h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// A WebSocket's binary messages as one byte stream, both ways: what a
/// SPDY tunnel is (client-go's `TunnelingConnection`).
fn websocket_bytes<S>(ws: tokio_tungstenite::WebSocketStream<S>) -> tokio::io::DuplexStream
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::Message;
    let (ours, theirs) = tokio::io::duplex(256 * 1024);
    let (mut from_spdy, mut to_spdy) = tokio::io::split(theirs);
    let (mut sink, mut stream) = ws.split();
    tokio::spawn(async move {
        while let Some(Ok(m)) = stream.next().await {
            match m {
                Message::Binary(d) => {
                    if to_spdy.write_all(&d).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = to_spdy.shutdown().await;
    });
    tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match from_spdy.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if sink.send(Message::Binary(buf[..n].to_vec().into())).await.is_err() {
                        return;
                    }
                }
            }
        }
        let _ = sink.send(Message::Close(None)).await;
    });
    ours
}

/// `kubectl exec` (#56). Not served yet on stormpump: the engine has no way
/// to run a process inside a running container (stormpump#103). Refused with
/// the reason, before any upgrade, so `kubectl` prints it.
async fn exec_refused(Path((namespace, pod, container)): Path<(String, String, String)>) -> Response {
    streaming_refused("exec", &namespace, &pod, &container)
}

/// `kubectl attach` (#56): as exec, a container's stdin needs the engine (stormpump#103).
async fn attach_refused(Path((namespace, pod, container)): Path<(String, String, String)>) -> Response {
    streaming_refused("attach", &namespace, &pod, &container)
}

fn streaming_refused(verb: &str, namespace: &str, pod: &str, container: &str) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "{verb} into {namespace}/{pod}/{container} is not supported on this node yet: its runtime \
             (stormpump) cannot run a process inside a running container (stormpump#103). \
             `kubectl port-forward` and `kubectl logs` work.\n"
        ),
    )
        .into_response()
}

async fn pods(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    Json(pm.pods_json().await)
}

/// cAdvisor-shaped container and pod metrics, from the runtime at scrape time
/// (#36; `metrics::render_cadvisor`).
async fn metrics_cadvisor(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    let containers = pm.container_stats().await;
    let pods = pm.pod_network_stats().await;
    (
        [("content-type", "text/plain; version=0.0.4")],
        crate::metrics::render_cadvisor(&containers, &pods),
    )
}

/// Minimal Summary API (metrics-server / `kubectl top`) — node + per-pod
/// container CPU/memory grouped from the CRI container stats.
async fn stats_summary(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    use std::collections::BTreeMap;
    let stats = pm.container_stats().await;
    // Group containers by (namespace, pod).
    let mut by_pod: BTreeMap<(String, String), Vec<serde_json::Value>> = BTreeMap::new();
    let mut node_cpu = 0u64;
    let mut node_mem = 0u64;
    for s in &stats {
        // A container the runtime reported no number for adds nothing, and
        // its own entry leaves the field out rather than claiming 0.
        node_cpu += s.cpu_usage_core_nanos.unwrap_or(0);
        node_mem += s.memory_working_set_bytes.unwrap_or(0);
        by_pod
            .entry((s.namespace.clone(), s.pod.clone()))
            .or_default()
            .push(serde_json::json!({
                "name": s.name,
                "cpu": s.cpu_usage_core_nanos.map(|n| serde_json::json!({"usageCoreNanoSeconds": n})),
                "memory": s.memory_working_set_bytes.map(|b| serde_json::json!({"workingSetBytes": b})),
            }));
    }
    // Each pod's network (#131), upstream's `NetworkStats`: the default
    // interface's counters at the top, every interface listed.
    let mut networks: BTreeMap<(String, String), serde_json::Value> = BTreeMap::new();
    for p in pm.pod_network_stats().await {
        by_pod.entry((p.namespace.clone(), p.pod.clone())).or_default();
        networks.insert((p.namespace.clone(), p.pod.clone()), network_summary(&p.interfaces));
    }
    let pods: Vec<serde_json::Value> = by_pod
        .into_iter()
        .map(|(key, containers)| {
            let mut pod = serde_json::json!({
                "podRef": {"name": key.1, "namespace": key.0},
                "containers": containers,
            });
            if let Some(net) = networks.remove(&key) {
                pod["network"] = net;
            }
            pod
        })
        .collect();
    // Real node filesystem stats (ephemeral storage) for eviction/monitoring.
    let node_fs = crate::node_status::ephemeral_fs_stats().map(|(total, avail)| {
        serde_json::json!({
            "capacityBytes": total,
            "availableBytes": avail,
            "usedBytes": total.saturating_sub(avail),
        })
    });
    Json(serde_json::json!({
        "node": {
            "cpu": {"usageCoreNanoSeconds": node_cpu},
            "memory": {"workingSetBytes": node_mem},
            "fs": node_fs,
        },
        "pods": pods,
    }))
}

/// A pod's `network` in `/stats/summary` (#131): upstream's shape (`name`,
/// `rxBytes`, `rxErrors`, `txBytes`, `txErrors` of the default interface, `eth0`
/// when there is one, and `interfaces`), with packets and drops beside them.
fn network_summary(interfaces: &[crate::cri::InterfaceStats]) -> serde_json::Value {
    let one = |i: &crate::cri::InterfaceStats| {
        serde_json::json!({
            "name": i.name,
            "rxBytes": i.rx_bytes, "rxErrors": i.rx_errors,
            "rxPackets": i.rx_packets, "rxDropped": i.rx_dropped,
            "txBytes": i.tx_bytes, "txErrors": i.tx_errors,
            "txPackets": i.tx_packets, "txDropped": i.tx_dropped,
        })
    };
    let default = interfaces.iter().find(|i| i.name == "eth0").or(interfaces.first());
    let mut net = default.map(one).unwrap_or_else(|| serde_json::json!({}));
    net["interfaces"] = interfaces.iter().map(one).collect();
    net
}

/// `DELETE /volumes/{namespace}/{claim}` — delete the stormblock clone behind
/// a released claim (rustkube-node#46).
///
/// The control plane's provisioner creates and binds the PV but cannot honour
/// `reclaimPolicy: Delete`, because stormblock's management API is loopback
/// and only the node can reach it. Leaving the PV `Released` was the
/// deliberate stand-in: deleting the object without deleting the clone turns
/// a visible leak into an invisible one, and because the volume name is
/// derived from the claim's, a later unrelated claim of that name in that
/// namespace would silently adopt the previous tenant's data.
///
/// - `204` — the clone is gone, or there was none (a retry is not an error).
/// - `409` — a pod on this node still has it. Refused, not queued: a delete
///   that races a running pod pulls a filesystem away mid-write.
/// - `503` — the node could not establish that it is unused, or stormblock
///   refused. Fails closed, because "I could not check" is not "nothing is
///   using it" when the answer destroys data.
///
/// The release runs in a task of its own (#100). A client that disconnects
/// drops the handler's future, and with it the claim's reservation, while a
/// detach or delete may still be in flight at stormblock: a pod admitted in
/// that moment could mount a volume being deleted. The task keeps the
/// reservation until stormblock has answered, whoever is still listening.
async fn release_volume(
    State(pod_manager): State<Arc<PodManager>>,
    Path((namespace, claim)): Path<(String, String)>,
) -> Response {
    let released = {
        let (namespace, claim) = (namespace.clone(), claim.clone());
        tokio::spawn(async move { pod_manager.release_claim_volume(&namespace, &claim).await })
            .await
            .unwrap_or_else(|e| Err(format!("release task: {e}")))
    };
    match released {
        Ok(VolumeRelease::Released) => {
            info!("released the volume for claim {namespace}/{claim}");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(VolumeRelease::Absent) => StatusCode::NO_CONTENT.into_response(),
        Ok(VolumeRelease::InUse(holder)) => (
            StatusCode::CONFLICT,
            format!(
                "claim {namespace}/{claim} is still mounted by pod {namespace}/{holder} \
                 on this node\n"
            ),
        )
            .into_response(),
        Err(why) => {
            warn!("release of {namespace}/{claim} refused: {why}");
            (StatusCode::SERVICE_UNAVAILABLE, format!("{why}\n")).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cri::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // oneshot

    // A do-nothing runtime/image service so we can build a PodManager.
    pub(super) struct NoopRt;
    #[async_trait::async_trait]
    impl RuntimeService for NoopRt {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok(("n".into(), "0".into(), "v1".into()))
        }
        async fn run_pod_sandbox(&self, _: &PodSandboxConfig) -> Result<String, CriError> {
            Ok("sb".into())
        }
        async fn stop_pod_sandbox(&self, _: &str) -> Result<(), CriError> {
            Ok(())
        }
        async fn remove_pod_sandbox(&self, _: &str) -> Result<(), CriError> {
            Ok(())
        }
        async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatusInfo, CriError> {
            Ok(PodSandboxStatusInfo {
                id: id.into(),
                state: PodSandboxState::Ready,
                created_at: 0,
                ip: String::new(),
                additional_ips: vec![],
                netns_path: None,
                made: None,
            })
        }
        async fn list_pod_sandbox(&self) -> Result<Vec<PodSandboxSummary>, CriError> {
            Ok(vec![])
        }
        async fn create_container(
            &self,
            _: &str,
            _: &ContainerConfig,
            _: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            Ok("c".into())
        }
        async fn start_container(&self, _: &str) -> Result<(), CriError> {
            Ok(())
        }
        async fn stop_container(&self, _: &str, _: i64) -> Result<(), CriError> {
            Ok(())
        }
        async fn remove_container(&self, _: &str) -> Result<(), CriError> {
            Ok(())
        }
        async fn container_status(&self, id: &str) -> Result<ContainerStatusInfo, CriError> {
            Ok(ContainerStatusInfo {
                id: id.into(),
                name: id.into(),
                state: ContainerState::Running,
                created_at: 0,
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
                image: String::new(),
                image_ref: String::new(),
                reason: String::new(),
                message: String::new(),
            })
        }
        async fn list_containers(
            &self,
            _: Option<&str>,
        ) -> Result<Vec<ContainerStatusInfo>, CriError> {
            Ok(vec![])
        }
        async fn exec_sync(
            &self,
            _: &str,
            _: &[String],
            _: i64,
        ) -> Result<ExecSyncResult, CriError> {
            Ok(ExecSyncResult {
                stdout: vec![],
                stderr: vec![],
                exit_code: 0,
            })
        }
    }
    #[async_trait::async_trait]
    impl ImageService for NoopRt {
        async fn pull_image(&self, i: &str) -> Result<String, CriError> {
            Ok(i.into())
        }
        async fn image_status(&self, _: &str) -> Result<Option<ImageInfo>, CriError> {
            Ok(None)
        }
        async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
            Ok(vec![])
        }
        async fn remove_image(&self, _: &str) -> Result<(), CriError> {
            Ok(())
        }
    }

    /// #89: a renewed serving pair is loaded in place; an unchanged one is
    /// left alone, and a half-written one does not replace what is served.
    #[tokio::test]
    async fn a_renewed_serving_pair_is_reloaded_and_a_half_written_one_is_not() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = std::env::temp_dir().join(format!("kubelet-serving-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key) = (dir.join("kubelet-serving.crt"), dir.join("kubelet-serving.key"));
        let a = self_signed_cert("node-a", "10.0.0.1").unwrap();
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem(a.0.clone(), a.1.clone()).await.unwrap();
        let before = tls.get_inner();

        std::fs::write(&cert, &a.0).unwrap();
        std::fs::write(&key, &a.1).unwrap();
        assert!(reload_pair(&tls, &cert, &key, &a).await.is_none(), "unchanged: not reloaded");
        assert!(Arc::ptr_eq(&before, &tls.get_inner()));

        // Renewed: a new pair is served.
        let b = self_signed_cert("node-a", "10.0.0.1").unwrap();
        std::fs::write(&cert, &b.0).unwrap();
        std::fs::write(&key, &b.1).unwrap();
        let served = reload_pair(&tls, &cert, &key, &a).await.expect("reloaded");
        assert_eq!(served, b);
        let after = tls.get_inner();
        assert!(!Arc::ptr_eq(&before, &after));

        // Half-written (a key that does not parse yet): the old pair stays.
        std::fs::write(&key, b"-----BEGIN PRIVATE KEY-----\npartial").unwrap();
        assert!(reload_pair(&tls, &cert, &key, &b).await.is_none());
        assert!(Arc::ptr_eq(&after, &tls.get_inner()), "still the previous pair");
        std::fs::remove_dir_all(&dir).ok();
    }

    pub(super) fn app() -> Router {
        let rt = Arc::new(NoopRt);
        let pm = Arc::new(PodManager::new(rt.clone(), rt, "test-node"));
        router(pm)
    }

    /// #56: the router on a real socket (an upgrade needs one), with a
    /// hostNetwork pod `default/web` running, and a TCP echo server standing
    /// in for its port.
    async fn streaming_world() -> (std::net::SocketAddr, u16) {
        let rt = Arc::new(NoopRt);
        let pm = Arc::new(PodManager::new(rt.clone(), rt, "test-node"));
        let pod = serde_json::json!({
            "metadata": {"name": "web", "namespace": "default", "uid": "u-web"},
            "spec": {"hostNetwork": true, "nodeName": "test-node",
                     "containers": [{"name": "app", "image": "busybox", "command": ["/bin/sleep", "1d"]}]}
        });
        pm.record_running_for_test(&pod, "sb").await;
        assert!(matches!(pm.pod_network("default", "web").await, Some(Ok((_, None)))), "hostNetwork: the node's");
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, router(pm)).await.unwrap() });

        let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = echo.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut c, _)) = echo.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = c.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });
        (addr, port)
    }

    /// Send a request head and read the response head.
    async fn upgrade(addr: std::net::SocketAddr, head: &str) -> (tokio::net::TcpStream, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(head.as_bytes()).await.unwrap();
        let mut got = Vec::new();
        let mut b = [0u8; 1];
        while !got.ends_with(b"\r\n\r\n") {
            c.read_exact(&mut b).await.unwrap();
            got.push(b[0]);
        }
        (c, String::from_utf8(got).unwrap())
    }

    /// Forward one connection over a SPDY byte stream and check the echo.
    async fn forward_once<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(mut io: S, port: u16) {
        use crate::spdy::client::{read_frame, Client, Frame};
        use tokio::io::AsyncWriteExt;
        let port = port.to_string();
        let mut c = Client::new();
        let h = |t: &'static str| [("streamType", t), ("port", port.as_str()), ("requestID", "0")];
        io.write_all(&c.syn_stream(1, &h("error"), true)).await.unwrap();
        io.write_all(&c.syn_stream(3, &h("data"), false)).await.unwrap();
        io.write_all(&Client::data(3, b"through the kubelet", false)).await.unwrap();
        loop {
            match read_frame(&mut io).await.expect("session ended before the echo") {
                Frame::Data(3, _, b) => {
                    assert_eq!(b, b"through the kubelet");
                    break;
                }
                Frame::Data(1, _, b) if !b.is_empty() => panic!("{}", String::from_utf8_lossy(&b)),
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn port_forward_over_spdy_reaches_the_pods_port() {
        let (addr, port) = streaming_world().await;
        let (io, head) = upgrade(
            addr,
            "POST /portForward/default/web HTTP/1.1\r\nHost: n\r\nConnection: Upgrade\r\n\
             Upgrade: SPDY/3.1\r\nX-Stream-Protocol-Version: portforward.k8s.io\r\n\r\n",
        )
        .await;
        assert!(head.starts_with("HTTP/1.1 101"), "{head}");
        let lower = head.to_ascii_lowercase();
        assert!(lower.contains("upgrade: spdy/3.1") && lower.contains("x-stream-protocol-version: portforward.k8s.io"), "{head}");
        forward_once(io, port).await;
    }

    #[tokio::test]
    async fn port_forward_over_a_websocket_tunnel_reaches_the_pods_port() {
        use tokio_tungstenite::tungstenite::protocol::Role;
        let (addr, port) = streaming_world().await;
        let (io, head) = upgrade(
            addr,
            "GET /portForward/default/web HTTP/1.1\r\nHost: n\r\nConnection: Upgrade\r\n\
             Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Protocol: SPDY/3.1+portforward.k8s.io\r\n\r\n",
        )
        .await;
        assert!(head.starts_with("HTTP/1.1 101"), "{head}");
        // RFC 6455's own example key and answer.
        assert!(head.to_ascii_lowercase().contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="), "{head}");
        let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(io, Role::Client, None).await;
        forward_once(websocket_bytes(ws), port).await;
    }

    #[tokio::test]
    async fn port_forward_refusals_say_why_and_never_upgrade() {
        let (addr, _) = streaming_world().await;
        let (_, head) = upgrade(addr, "POST /portForward/default/nope HTTP/1.1\r\nHost: n\r\nUpgrade: SPDY/3.1\r\nConnection: Upgrade\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.1 404"), "{head}");
        let (_, head) = upgrade(addr, "POST /portForward/default/web HTTP/1.1\r\nHost: n\r\nUpgrade: SPDY/3.1\r\nConnection: Upgrade\r\nX-Stream-Protocol-Version: v4.channel.k8s.io\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.1 403"), "{head}");
        // A WebSocket without the tunnel subprotocol: refused, so client-go
        // falls back to SPDY.
        let (_, head) = upgrade(addr, "GET /portForward/default/web HTTP/1.1\r\nHost: n\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: v5.channel.k8s.io\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.1 403"), "{head}");
    }

    #[tokio::test]
    async fn exec_and_attach_answer_why_not_rather_than_404() {
        for path in ["/exec/default/web/app?command=sh&input=1&output=1&tty=1", "/attach/default/web/app?output=1"] {
            let resp = app()
                .oneshot(Request::builder().method("POST").uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
            let body = axum::body::to_bytes(resp.into_body(), 1 << 16).await.unwrap();
            assert!(String::from_utf8_lossy(&body).contains("stormpump#103"));
        }
    }

    #[tokio::test]
    async fn healthz_ok() {
        let resp = app()
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_exposes_gauges() {
        let resp = app()
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        // Values are process-wide and other tests set them too, so this
        // checks the names; metrics.rs checks the numbers.
        assert!(text.contains("kubelet_running_pods "), "{text}");
        assert!(text.contains(r#"kubelet_running_containers{container_state="running"}"#), "{text}");
    }

    #[tokio::test]
    async fn cadvisor_and_summary_ok() {
        for uri in ["/metrics/cadvisor", "/stats/summary"] {
            let resp = app()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        }
    }

    #[tokio::test]
    async fn pods_returns_podlist() {
        let resp = app()
            .oneshot(Request::builder().uri("/pods").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["kind"], "PodList");
    }

    /// `kubectl logs -n kube-system fastetcd-<node>` reads fastetcd's stormd
    /// log volume, found through the boot unit that mounts it (#72).
    #[tokio::test]
    async fn a_node_service_mirror_pod_reads_its_stormd_log_volume() {
        let root = tempfile::tempdir().unwrap();
        let bd = root.path().join("etc/stormpump/boot.d");
        std::fs::create_dir_all(&bd).unwrap();
        std::fs::write(
            bd.join("30-kube"),
            "volume felogs /logs/fastetcd\nspec fastetcd\n  mount felogs /var/log/stormd\n\
             volume reg /pallets/registry\nspec registry\n  root reg\n",
        )
        .unwrap();
        let logs = root.path().join("logs/fastetcd");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(
            logs.join("fastetcd.log"),
            "2026-09-28T10:00:00.000Z stdout info serving\n2026-09-28T10:00:01.000Z stderr warn slow\n",
        )
        .unwrap();
        std::fs::write(logs.join("fastetcd.20260827T000000.failed.log"), "2026-09-27T00:00:00.000Z stderr error boom\n")
            .unwrap();

        let rt = Arc::new(NoopRt);
        let pm = Arc::new(
            PodManager::new(rt.clone(), rt, "n1")
                .with_host_root(root.path())
                .with_assets_json(root.path().join("run/stormpump/assets.json")),
        );
        let get = |uri: &str| {
            let app = router(pm.clone());
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            async move {
                let resp = app.oneshot(req).await.unwrap();
                let status = resp.status();
                let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        };

        let (st, body) = get("/containerLogs/kube-system/fastetcd-n1/fastetcd").await;
        assert_eq!((st, body.as_str()), (StatusCode::OK, "serving\nslow\n"));
        let (_, body) = get("/containerLogs/kube-system/fastetcd-n1/fastetcd?tailLines=1&timestamps=true").await;
        assert_eq!(body, "2026-09-28T10:00:01.000Z slow\n");
        let (st, body) = get("/containerLogs/kube-system/fastetcd-n1/fastetcd?previous=true").await;
        assert_eq!((st, body.as_str()), (StatusCode::OK, "boom\n"));

        // Not run by stormd, another node's mirror, or the wrong container:
        // not this node's to answer.
        for uri in [
            "/containerLogs/kube-system/registry-n1/registry",
            "/containerLogs/kube-system/fastetcd-n2/fastetcd",
            "/containerLogs/kube-system/fastetcd-n1/other",
            "/containerLogs/default/fastetcd-n1/fastetcd",
        ] {
            assert_eq!(get(uri).await.0, StatusCode::NOT_FOUND, "{uri}");
        }
    }

    /// #87 (stormpump#90): a running node service that stormd does not run
    /// (the registry) is read from its own `w<id>.log`, which assets.json now
    /// names; `--previous` from the run before it.
    #[tokio::test]
    async fn a_running_service_not_run_by_stormd_is_read_from_its_own_run() {
        let root = tempfile::tempdir().unwrap();
        let run = root.path().join("run/stormpump");
        std::fs::create_dir_all(run.join("logs")).unwrap();
        std::fs::write(
            run.join("assets.json"),
            r#"{"assets":[{"name":"registry","running":true,"restarts":1,"age_secs":30,"domain":1,
               "last_exit":"exited 2","last_output":["old tail"],
               "runs":[{"started_at":10,"ended_at":20,"exit_code":2,"exit":"exited 2","log":"w3.log"},
                       {"started_at":21,"log":"w8.log","log_rotated":"w8.1.log"}]}]}"#,
        )
        .unwrap();
        std::fs::write(run.join("logs/w3.log"), "registry: bad config\n").unwrap();
        std::fs::write(run.join("logs/w8.1.log"), "registry: starting\n").unwrap();
        std::fs::write(run.join("logs/w8.log"), "registry: serving :5100\nregistry: GET /v2/\n").unwrap();

        let rt = Arc::new(NoopRt);
        let pm = Arc::new(
            PodManager::new(rt.clone(), rt, "n1")
                .with_host_root(root.path())
                .with_assets_json(run.join("assets.json")),
        );
        let get = |uri: &str| {
            let app = router(pm.clone());
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            async move {
                let resp = app.oneshot(req).await.unwrap();
                let status = resp.status();
                let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        };
        assert_eq!(
            get("/containerLogs/kube-system/registry-n1/registry").await,
            (StatusCode::OK, "registry: starting\nregistry: serving :5100\nregistry: GET /v2/\n".to_string()),
            "the running run, rotated part first, not the last exit's tail"
        );
        assert_eq!(
            get("/containerLogs/kube-system/registry-n1/registry?previous=true").await,
            (StatusCode::OK, "registry: bad config\n".to_string())
        );
        let (_, body) = get("/containerLogs/kube-system/registry-n1/registry?tailLines=1").await;
        assert_eq!(body, "registry: GET /v2/\n");
    }

    /// A node service that died before stormd wrote its volume (stormcluster
    /// and stormrdp on 11.61) answers with what PID 1 kept of its last exit,
    /// not "not found on this node" (#124). So does one not run by stormd.
    #[tokio::test]
    async fn a_dead_node_service_answers_with_stormpumps_last_output() {
        let root = tempfile::tempdir().unwrap();
        let bd = root.path().join("etc/stormpump/boot.d");
        std::fs::create_dir_all(&bd).unwrap();
        std::fs::write(
            bd.join("40-services"),
            "volume sclogs /logs/stormcluster\nspec stormcluster\n  mount sclogs /var/log/stormd\n\
             volume rdlogs /logs/stormrdp\nspec stormrdp\n  mount rdlogs /var/log/stormd\n\
             volume fd /logs/stormdrive\nspec stormdrive\n  mount fd /var/log/stormd\n",
        )
        .unwrap();
        // The volume exists and is empty: stormd never got that far.
        std::fs::create_dir_all(root.path().join("logs/stormcluster")).unwrap();
        let run = root.path().join("run/stormpump");
        std::fs::create_dir_all(&run).unwrap();
        std::fs::write(
            run.join("assets.json"),
            r#"{"assets":[
              {"name":"stormcluster","running":false,"restarts":1,"age_secs":3,"domain":2,
               "last_exit_code":1,"last_exit":"exited 1",
               "last_output":["stormd: starting stormcluster","stormcluster: /etc/stormcos/release: is a directory"]},
              {"name":"stormrdp","running":false,"restarts":1,"age_secs":3,"domain":2,"last_exit":"exited 78"},
              {"name":"stormdrive","running":true,"restarts":0,"age_secs":3,"domain":2},
              {"name":"registry","running":false,"restarts":4,"age_secs":3,"domain":1,
               "last_exit":"killed by signal 9","last_output":["registry: out of memory"]}]}"#,
        )
        .unwrap();

        let rt = Arc::new(NoopRt);
        let pm = Arc::new(
            PodManager::new(rt.clone(), rt, "n1")
                .with_host_root(root.path())
                .with_assets_json(run.join("assets.json")),
        );
        let get = |uri: &str| {
            let app = router(pm.clone());
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            async move {
                let resp = app.oneshot(req).await.unwrap();
                let status = resp.status();
                let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        };

        let why = "stormd: starting stormcluster\nstormcluster: /etc/stormcos/release: is a directory\n";
        for uri in [
            "/containerLogs/kube-system/stormcluster-n1/stormcluster",
            "/containerLogs/kube-system/stormcluster-n1/stormcluster?previous=true",
            // Nothing to follow: the record is all there is.
            "/containerLogs/kube-system/stormcluster-n1/stormcluster?follow=true",
        ] {
            assert_eq!(get(uri).await, (StatusCode::OK, why.to_string()), "{uri}");
        }
        let (_, body) = get("/containerLogs/kube-system/stormcluster-n1/stormcluster?tailLines=1").await;
        assert_eq!(body, "stormcluster: /etc/stormcos/release: is a directory\n");
        let (_, body) = get("/containerLogs/kube-system/stormcluster-n1/stormcluster?limitBytes=7").await;
        assert_eq!(body, "stormd:");

        // Not run by stormd: the record is its only log.
        let (st, body) = get("/containerLogs/kube-system/registry-n1/registry").await;
        assert_eq!((st, body.as_str()), (StatusCode::OK, "registry: out of memory\n"));

        // Nothing recorded: a 404 that says what was looked at and how it
        // ended, never "not found on this node".
        let (st, body) = get("/containerLogs/kube-system/stormrdp-n1/stormrdp").await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        assert!(body.contains("logs/stormrdp") && body.contains("last exit: exited 78"), "{body}");
        assert!(!body.contains("not found on this node"), "{body}");
        let (st, body) = get("/containerLogs/kube-system/stormrdp-n1/stormrdp?previous=true").await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
        assert!(body.contains("exited 78"), "{body}");

        // Running with an empty volume: the record is an earlier run's, not
        // this one's, so it is not served as the current log.
        let (st, body) = get("/containerLogs/kube-system/stormdrive-n1/stormdrive").await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        assert!(body.contains("stormpump: running"), "{body}");

        // Not a node service of this node.
        let (_, body) = get("/containerLogs/kube-system/stormcluster-n2/stormcluster").await;
        assert!(body.contains("not found on this node"), "{body}");
    }
}


/// `GET /containerLogs/{namespace}/{pod}/{container}` — what `kubectl logs`
/// ultimately reads.
///
/// The apiserver proxies the user's request here; this is the only place that
/// knows where a container's output actually is. The path is the CRI one the
/// kubelet itself told the runtime to write:
///
///   /var/log/pods/<namespace>_<pod>_<uid>/<container>/<restart>.log
///
/// The pod's UID is not in the URL, so it is resolved from the kubelet's own
/// view of its pods — which is also what makes a request for a pod this node
/// does not have a 404 rather than an empty body.
///
/// `follow=true` streams: the file as it stands is sent first, then whatever is
/// appended to it, until the container is gone from this node or the client
/// hangs up. That is the half of `kubectl logs -f` that lives here — the
/// apiserver already passes the body through chunk by chunk (rustkube#55), so
/// buffering on this side was what made `-f` print once and stop.
async fn container_logs(
    State(pm): State<Arc<PodManager>>,
    Path((namespace, pod, container)): Path<(String, String, String)>,
    Query(opts): Query<LogOptions>,
) -> Response {
    if let Err(why) = opts.runs_back() {
        return (StatusCode::BAD_REQUEST, why).into_response();
    }
    // A node service's mirror pod is no pod this kubelet runs (#72): its log
    // is on the service's stormd log volume, or in PID 1's record of its last
    // exit (#124).
    if pm.pod_uid(&namespace, &pod).await.is_none() && pm.waiting_reason(&namespace, &pod).is_none() {
        if let Some(svc) = pm.node_service(&namespace, &pod, &container) {
            return node_service_logs(svc, &namespace, &pod, &container, opts).await;
        }
    }
    let path = match log_file(&pm, &namespace, &pod, &container, &opts).await {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    // Read what is there now. Only up to the last newline: the runtime may be
    // mid-line, and half a line followed by the rest of it arriving as a
    // separate chunk would be indistinguishable from two lines.
    let raw = match std::fs::read_to_string(&path) {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::NOT_FOUND, format!("cannot read {path}: {e}\n")).into_response()
        }
    };
    let mut budget = opts.limit_bytes;
    // Not following: everything there is, a last line with no newline
    // included (#136: a test that crashed mid-line kept that line from us).
    if !opts.follow.unwrap_or(false) {
        return (StatusCode::OK, cap(filter_log(&raw, &opts), &mut budget)).into_response();
    }
    let consumed = raw.rfind('\n').map(|i| i + 1).unwrap_or(0);
    let head = cap(filter_log(&raw[..consumed], &opts), &mut budget);
    if budget == Some(0) {
        return (StatusCode::OK, head).into_response();
    }

    // Follow. A channel rather than a self-referential stream: the reader is a
    // blocking file poll, and pushing into a bounded channel gives it
    // backpressure from the client for free — a slow reader stalls the poll
    // instead of growing a buffer.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    if !head.is_empty() && tx.send(Ok(head.into_bytes())).await.is_err() {
        return (StatusCode::OK, String::new()).into_response();
    }
    // `tailLines` applies to the log as it stood, not to each new line.
    let opts = LogOptions { tail_lines: None, ..opts };
    tokio::spawn(async move {
        let mut offset = consumed as u64;
        loop {
            // The container going away is what ends `-f` upstream; without
            // this the stream would hold open forever on a finished pod.
            if pm.pod_uid(&namespace, &pod).await.is_none() {
                // Its last line, if the container ended mid-line (#136).
                if let Ok(rest) = rest_from(&path, offset) {
                    let out = cap(filter_log(&rest, &opts), &mut budget);
                    if !out.is_empty() {
                        let _ = tx.send(Ok(out.into_bytes())).await;
                    }
                }
                return;
            }
            match tail_from(&path, &mut offset) {
                Ok(chunk) if !chunk.is_empty() => {
                    let out = cap(filter_log(&chunk, &opts), &mut budget);
                    if !out.is_empty() && tx.send(Ok(out.into_bytes())).await.is_err() {
                        return; // client hung up
                    }
                    if budget == Some(0) {
                        return;
                    }
                }
                Ok(_) => {}
                // A read error mid-follow (the file rotated out from under us,
                // the mount went away) ends the stream rather than spinning.
                Err(_) => return,
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .unwrap_or_else(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response()
        })
}

/// `containerLogs` for a node service's mirror pod: its stormd log volume
/// ([`crate::node_logs`]), else what PID 1 kept of its last exit (#124).
///
/// The current run is every process stormd runs there, rotations included,
/// merged in time order. `previous` is the newest finished run, failed or
/// exited, as upstream (#216). `follow` polls
/// the live files, as for a pod, until the directory goes or the client hangs
/// up; a line written in the instant between a poll and a rotation is missed.
///
/// A service that died before stormd wrote anything (a bad config, a missing
/// root: stormcluster and stormrdp on 11.61), or one not run by stormd at all,
/// has only stormpump's `last_output`: the last lines its incarnation wrote to
/// PID 1's log. It is the current log of a service that is not running, and
/// the previous one when stormd kept no failed run. It has no timestamps, so
/// only `tailLines` and `limitBytes` apply, and there is nothing to follow.
async fn node_service_logs(
    svc: crate::pod_manager::NodeService,
    namespace: &str,
    pod: &str,
    container: &str,
    opts: LogOptions,
) -> Response {
    use crate::node_logs;
    let mut budget = opts.limit_bytes;
    let crate::pod_manager::NodeService { log_dir, record, runs_dir } = svc;
    let last_output = record.as_ref().map(|r| r.last_output.as_slice()).filter(|o| !o.is_empty());

    let back = opts.runs_back().unwrap_or(0);
    if back > 0 {
        // stormd's failed runs, newest first (#131: N back).
        if let Some(file) = log_dir.as_deref().and_then(|d| node_logs::finished_run(d, back)) {
            return match std::fs::read_to_string(&file) {
                Ok(t) => (StatusCode::OK, cap(filter_log(&t, &opts), &mut budget)).into_response(),
                Err(e) => (StatusCode::NOT_FOUND, format!("cannot read {}: {e}\n", file.display()))
                    .into_response(),
            };
        }
        // A service stormd does not run: its previous incarnation's own file
        // (#87, stormpump#90).
        if let Some(run) = record.as_ref().and_then(|r| r.ended_run(back)) {
            let files = node_logs::run_files(&runs_dir, run);
            if !files.is_empty() {
                let lines = node_logs::read_run(&files);
                return (StatusCode::OK, cap(plain_log(&lines, &opts), &mut budget)).into_response();
            }
        }
        // PID 1 keeps only the last exit's tail: one run back.
        if let (Some(lines), 1) = (last_output, back) {
            return (StatusCode::OK, cap(plain_log(lines, &opts), &mut budget)).into_response();
        }
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "container {container} has no previous run: {}\n",
                node_service_sources(log_dir.as_deref(), record.as_ref())
            ),
        )
            .into_response();
    }

    let dir = match log_dir {
        Some(d) if !node_logs::current_files(&d).is_empty() => d,
        log_dir => {
            // Not run by stormd (stormblock, the registry, timesync): the
            // running incarnation's own file, named in assets.json (#87).
            let live = record
                .as_ref()
                .and_then(|r| r.current_run())
                .map(|run| node_logs::run_files(&runs_dir, run))
                .filter(|f| !f.is_empty());
            if let Some(files) = live {
                return engine_run_logs(files, opts, budget).await;
            }
            if let (Some(lines), Some(false)) = (last_output, record.as_ref().map(|r| r.running)) {
                return (StatusCode::OK, cap(plain_log(lines, &opts), &mut budget)).into_response();
            }
            // Named, because the likely cause is the volume not being visible
            // here rather than the service writing nothing.
            return (
                StatusCode::NOT_FOUND,
                format!(
                    "no logs for container {container} in {namespace}/{pod}: {}\n",
                    node_service_sources(log_dir.as_deref(), record.as_ref())
                ),
            )
                .into_response();
        }
    };
    let (text, mut offsets) = node_logs::read_current(&dir);
    let head = cap(filter_log(&text, &opts), &mut budget);
    if !opts.follow.unwrap_or(false) || budget == Some(0) {
        return (StatusCode::OK, head).into_response();
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    if !head.is_empty() && tx.send(Ok(head.into_bytes())).await.is_err() {
        return (StatusCode::OK, String::new()).into_response();
    }
    let opts = LogOptions { tail_lines: None, ..opts };
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if !dir.is_dir() {
                return;
            }
            let mut chunks = Vec::new();
            for (proc_name, live) in node_logs::live_files(&dir) {
                let offset = offsets.entry(live.clone()).or_insert(0);
                let chunk = tail_from(&live.to_string_lossy(), offset).unwrap_or_default();
                chunks.push((proc_name, chunk));
            }
            let out = cap(filter_log(&node_logs::merge(&chunks), &opts), &mut budget);
            if !out.is_empty() && tx.send(Ok(out.into_bytes())).await.is_err() {
                return; // client hung up
            }
            if budget == Some(0) {
                return;
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .unwrap_or_else(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response()
        })
}

/// A node service's running incarnation, from PID 1's own files for it (#87):
/// the rotated part, then the live file; `follow` polls the live one. Its
/// lines carry no timestamps, so only `tailLines` and `limitBytes` apply.
async fn engine_run_logs(files: Vec<std::path::PathBuf>, opts: LogOptions, mut budget: Option<usize>) -> Response {
    let lines = crate::node_logs::read_run(&files);
    let head = cap(plain_log(&lines, &opts), &mut budget);
    if !opts.follow.unwrap_or(false) || budget == Some(0) {
        return (StatusCode::OK, head).into_response();
    }
    let Some(live) = files.last().cloned() else {
        return (StatusCode::OK, head).into_response();
    };
    let mut offset = std::fs::metadata(&live).map(|m| m.len()).unwrap_or(0);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(16);
    if !head.is_empty() && tx.send(Ok(head.into_bytes())).await.is_err() {
        return (StatusCode::OK, String::new()).into_response();
    }
    let opts = LogOptions { tail_lines: None, ..opts };
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let Ok(chunk) = tail_from(&live.to_string_lossy(), &mut offset) else {
                return; // the run ended and its file went
            };
            let lines: Vec<String> = chunk.lines().map(str::to_string).collect();
            let out = cap(plain_log(&lines, &opts), &mut budget);
            if !out.is_empty() && tx.send(Ok(out.into_bytes())).await.is_err() {
                return;
            }
            if budget == Some(0) {
                return;
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(axum::body::Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)))
        .unwrap_or_else(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response())
}

/// What was looked at for a node service's log and found empty, and what PID
/// 1 says about it: the body of a 404 or 400 that has to say why (#124).
fn node_service_sources(
    log_dir: Option<&std::path::Path>,
    record: Option<&crate::node_logs::Record>,
) -> String {
    let mut why = vec![match log_dir {
        Some(d) => format!("nothing in {}", d.display()),
        None => "no stormd log volume in its boot unit".to_string(),
    }];
    match record {
        None => why.push("not in PID 1's asset table".to_string()),
        Some(r) => {
            why.push(format!(
                "stormpump: {}{}",
                if r.running { "running" } else { "not running" },
                if r.last_output.is_empty() { ", no output recorded" } else { "" }
            ));
            if let Some(e) = &r.last_exit {
                why.push(format!("last exit: {e}"));
            }
            if let Some(e) = &r.last_error {
                why.push(format!("last start refused: {e}"));
            }
        }
    }
    why.join("; ")
}

/// Lines with no CRI prefix (stormpump's `last_output`): `tailLines` applies;
/// the time filters and `timestamps` have no timestamp to act on.
fn plain_log(lines: &[String], opts: &LogOptions) -> String {
    let from = opts.tail_lines.map_or(0, |n| lines.len().saturating_sub(n));
    lines[from..].iter().map(|l| format!("{l}\n")).collect()
}

/// Upstream's answer to `logs` on a container that has not started.
fn waiting_to_start(container: &str, pod: &str, kind: &str, why: &str) -> String {
    // A container that failed to start has nothing to read either, and says
    // why rather than "not found" (#133).
    if kind == "StartError" {
        return format!("container \"{container}\" in pod \"{pod}\" failed to start: StartError ({why})\n");
    }
    format!("container \"{container}\" in pod \"{pod}\" is waiting to start: {kind} ({why})\n")
}

/// Which file holds the run the caller asked for.
///
/// Restarts are numbered from 0, so the current run is the highest-numbered
/// file and `previous` is the one below it — absent for a container that has
/// never restarted, which is a 400 upstream rather than an empty success.
async fn log_file(
    pm: &PodManager,
    namespace: &str,
    pod: &str,
    container: &str,
    opts: &LogOptions,
) -> Result<String, Response> {
    let Some(uid) = pm.pod_uid(namespace, pod).await else {
        // A pod this node admitted and has not started is not "not found"
        // (#63): upstream answers 400 with why it is waiting.
        if let Some((kind, why)) = pm.waiting_state(namespace, pod) {
            return Err((StatusCode::BAD_REQUEST, waiting_to_start(container, pod, &kind, &why))
                .into_response());
        }
        return Err((
            StatusCode::NOT_FOUND,
            format!("pod {namespace}/{pod} not found on this node\n"),
        )
            .into_response());
    };
    let dir = format!("/var/log/pods/{namespace}_{pod}_{uid}/{container}");
    let none = || {
        (
            StatusCode::NOT_FOUND,
            format!("no logs for container {container} in {namespace}/{pod}\n"),
        )
            .into_response()
    };

    let mut runs: Vec<u32> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".log"))
                    .and_then(|n| n.parse().ok())
            })
            .collect(),
        Err(_) => return Err(none()),
    };
    runs.sort_unstable();
    if runs.is_empty() {
        return Err(none());
    }
    match pick_run(&runs, opts.runs_back().unwrap_or(0)) {
        Ok(want) => Ok(format!("{dir}/{want}.log")),
        Err(why) => Err((StatusCode::BAD_REQUEST, format!("container {container} {why}\n")).into_response()),
    }
}

/// Which `<restartCount>.log` to read (#131): of the runs on disk (ascending,
/// not empty), the one `back` runs before the newest.
fn pick_run(runs: &[u32], back: usize) -> Result<u32, String> {
    match runs.len().checked_sub(1 + back).and_then(|i| runs.get(i)) {
        Some(r) => Ok(*r),
        None if back == 1 => Err("has no previous run".into()),
        None => Err(format!(
            "has no run {back} back (it has {} before the current one)",
            runs.len().saturating_sub(1)
        )),
    }
}

/// Whole lines appended to `path` since `offset`, advancing `offset` past them.
///
/// A file shorter than the offset was truncated or replaced, so reading resumes
/// at the start rather than returning nothing forever.
fn tail_from(path: &str, offset: &mut u64) -> std::io::Result<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    if len < *offset {
        *offset = 0;
    }
    if len == *offset {
        return Ok(String::new());
    }
    f.seek(SeekFrom::Start(*offset))?;
    let mut buf = Vec::with_capacity((len - *offset) as usize);
    f.take(len - *offset).read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    // Stop at the last newline; a partial line stays for the next poll.
    let whole = match text.rfind('\n') {
        Some(i) => i + 1,
        None => return Ok(String::new()),
    };
    *offset += whole as u64;
    Ok(text[..whole].to_string())
}

/// Everything in `path` from `offset` on, a last line with no newline included:
/// what a follow that is ending still owes the reader (#136).
fn rest_from(path: &str, offset: u64) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    let from = (offset as usize).min(bytes.len());
    Ok(String::from_utf8_lossy(&bytes[from..]).into_owned())
}

/// Trim `s` to what is left of the `limitBytes` budget, spending it.
///
/// `None` is no limit. The cut is at a character boundary because the response
/// is UTF-8 text and half a codepoint is not; upstream cuts at a byte and lets
/// the client cope, which is worse for no gain.
fn cap(s: String, budget: &mut Option<usize>) -> String {
    let Some(left) = budget.as_mut() else {
        return s;
    };
    if s.len() <= *left {
        *left -= s.len();
        return s;
    }
    let mut end = *left;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    *left = 0;
    s[..end].to_string()
}

/// The query parameters `kubectl logs` sends.
#[derive(Debug, Default, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogOptions {
    /// Only the last N lines.
    tail_lines: Option<usize>,
    /// Only entries newer than this many seconds.
    since_seconds: Option<i64>,
    /// Only entries at or after this RFC3339 time.
    since_time: Option<String>,
    /// Prefix each line with its timestamp.
    timestamps: Option<bool>,
    /// The run before the current one: `true`/`1`, as upstream parses it; `N`
    /// for the run N back (#131: the console reads the last five).
    previous: Option<String>,
    /// Keep the response open and send what is appended.
    follow: Option<bool>,
    /// A byte cap on the response, counted after filtering.
    limit_bytes: Option<usize>,
}

impl LogOptions {
    /// How many runs back `previous` asks for (#131): 0 the current run, 1 the
    /// one before (`true`, upstream's), N the run N back. Booleans as Go's
    /// `ParseBool` reads them, so what upstream accepts means the same here.
    fn runs_back(&self) -> Result<usize, String> {
        let Some(v) = self.previous.as_deref() else { return Ok(0) };
        match v {
            "" | "false" | "f" | "F" | "FALSE" | "False" => Ok(0),
            "true" | "t" | "T" | "TRUE" | "True" => Ok(1),
            n => n.parse().map_err(|_| format!("previous={v}: true, false or a number of runs back\n")),
        }
    }
}

/// Apply the CRI log-format options to a chunk of a log file.
///
/// The CRI format is `<rfc3339nano> <stdout|stderr> <F|P> <line>`, and where a
/// runtime writes it this strips the prefix unless `timestamps` was asked for.
///
/// **stormpump does not write it.** The engine gives the container a descriptor
/// pointing straight at the log file and never sees a log byte — that is what
/// makes logging zero-copy, and it is deliberate. The consequence is that a
/// stormpump container's log has no per-line metadata, so `timestamps`,
/// `sinceSeconds` and `sinceTime` have nothing to work from and are inert.
/// `tailLines` is line-based and works regardless.
///
/// Only a line in CRI format is taken apart (#136); every other line is the
/// container's output as written. Lines are passed through rather than dropped
/// when they are not CRI format:
/// a log the reader cannot see is worse than one without a timestamp. The
/// alternative — refusing `--timestamps` outright — would break the common
/// invocation to signal something the caller cannot act on anyway. If per-line
/// timestamps become worth having, they cost the zero-copy property, and that
/// is the trade to weigh rather than a bug to fix.
/// One line with per-line metadata: `<RFC 3339 time> <stream> <tag> <message>`.
struct CriLine<'a> {
    ts: &'a str,
    full: bool,
    msg: &'a str,
}

/// The line's time and message, when it carries them (#136). Two formats do:
/// - CRI: `<time> <stdout|stderr> <P|F>[:flags] <message>`, `P` a partial
///   line continued by the next;
/// - stormd's log volume (a node service's mirror pod, #72): `<time>
///   <stdout|stderr|syslog|ingest> <emerg|alert|crit|error|warn|notice|info|debug>
///   <message>` (stormlog's `LogStream` and `Severity`).
///
/// The first field must parse as RFC 3339. Anything else is a plain line,
/// every stormpump container's among them. Any four-way split used to count,
/// so every plain line with three spaces lost its first three words.
fn cri_line(line: &str) -> Option<CriLine<'_>> {
    let mut it = line.splitn(4, ' ');
    let (ts, stream, tag) = (it.next()?, it.next()?, it.next()?);
    let msg = it.next().unwrap_or("");
    let full = match (stream, tag.split(':').next()?) {
        ("stdout" | "stderr", "F") => true,
        ("stdout" | "stderr", "P") => false,
        (
            "stdout" | "stderr" | "syslog" | "ingest",
            "emerg" | "alert" | "crit" | "error" | "warn" | "notice" | "info" | "debug",
        ) => true,
        _ => return None,
    };
    chrono::DateTime::parse_from_rfc3339(ts).ok()?;
    Some(CriLine { ts, full, msg })
}

fn filter_log(body: &str, opts: &LogOptions) -> String {
    let cutoff: Option<chrono::DateTime<chrono::Utc>> = opts
        .since_time
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
        .or_else(|| {
            opts.since_seconds
                .map(|s| chrono::Utc::now() - chrono::Duration::seconds(s))
        });

    let mut out: Vec<String> = Vec::new();
    // A CRI partial (`P`) line's text, waiting for the line that ends it.
    let mut partial: Option<(String, String)> = None;
    for line in body.lines() {
        let Some(cri) = cri_line(line) else {
            // Not CRI format (#136): every stormpump line, which has no
            // per-line metadata. Passed through whole, spaces and all.
            if let Some((_, text)) = partial.take() {
                out.push(text);
            }
            out.push(line.to_string());
            continue;
        };
        // A partial line is joined to the rest of it, as upstream reads them.
        let (ts, msg) = match partial.take() {
            Some((ts, mut text)) => {
                text.push_str(cri.msg);
                (ts, text)
            }
            None => (cri.ts.to_string(), cri.msg.to_string()),
        };
        if !cri.full {
            partial = Some((ts, msg));
            continue;
        }
        if let Some(cut) = cutoff {
            match chrono::DateTime::parse_from_rfc3339(&ts) {
                Ok(t) if t.with_timezone(&chrono::Utc) < cut => continue,
                _ => {}
            }
        }
        if opts.timestamps.unwrap_or(false) {
            out.push(format!("{ts} {msg}"));
        } else {
            out.push(msg);
        }
    }
    // A partial line the log ends on is still the container's output.
    if let Some((ts, text)) = partial {
        out.push(if opts.timestamps.unwrap_or(false) { format!("{ts} {text}") } else { text });
    }
    if let Some(n) = opts.tail_lines {
        if out.len() > n {
            out.drain(..out.len() - n);
        }
    }
    let mut s = out.join("\n");
    if !s.is_empty() {
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod log_tests {
    use super::*;

    fn cri(lines: &[(&str, &str)]) -> String {
        lines
            .iter()
            .map(|(ts, msg)| format!("{ts} stdout F {msg}\n"))
            .collect()
    }

    #[test]
    fn strips_the_cri_prefix_unless_timestamps_asked() {
        let body = cri(&[("2026-09-09T00:00:01Z", "one"), ("2026-09-09T00:00:02Z", "two")]);
        assert_eq!(filter_log(&body, &LogOptions::default()), "one\ntwo\n");
        let with_ts = LogOptions { timestamps: Some(true), ..Default::default() };
        assert_eq!(
            filter_log(&body, &with_ts),
            "2026-09-09T00:00:01Z one\n2026-09-09T00:00:02Z two\n"
        );
    }

    #[test]
    fn non_cri_lines_pass_through() {
        // stormpump writes the container's bytes verbatim; dropping them
        // because they have no timestamp would empty the log.
        let opts = LogOptions { timestamps: Some(true), ..Default::default() };
        assert_eq!(filter_log("raw line\n", &opts), "raw line\n");
    }

    /// #136: a plain line with spaces comes back whole; only a real CRI
    /// line (time, stdout|stderr, P|F) loses its three fields.
    #[test]
    fn plain_lines_with_spaces_come_back_whole() {
        let body = concat!(
            "stormpump: /proc could not be mounted (is the directory in the image?)\n",
            "{\"test\": \"start\", \"status\": \"pass\", \"ms\": 3}\n",
            "not-a-time stdout F looks almost like CRI\n",
            "2026-10-06T10:00:00Z stdin F wrong stream\n",
            "2026-10-06T10:00:00Z stdout X wrong tag\n",
            "2026-10-06T10:00:00Z stdout F a real CRI line\n",
            "2026-10-06T10:00:01.5Z stderr warn a stormd line\n",
        );
        assert_eq!(
            filter_log(body, &LogOptions::default()),
            concat!(
                "stormpump: /proc could not be mounted (is the directory in the image?)\n",
                "{\"test\": \"start\", \"status\": \"pass\", \"ms\": 3}\n",
                "not-a-time stdout F looks almost like CRI\n",
                "2026-10-06T10:00:00Z stdin F wrong stream\n",
                "2026-10-06T10:00:00Z stdout X wrong tag\n",
                "a real CRI line\n",
                "a stormd line\n",
            )
        );
    }

    /// CRI partial lines are joined to the line that ends them; a log that
    /// ends on a partial, or on a plain line with no newline, still shows it.
    #[test]
    fn partial_and_unterminated_lines_are_kept() {
        let body = "2026-10-06T10:00:00Z stdout P {\"test\": \n2026-10-06T10:00:00Z stderr F \"x\"}\n";
        assert_eq!(filter_log(body, &LogOptions::default()), "{\"test\": \"x\"}\n");
        let cut = "2026-10-06T10:00:00Z stdout P half a li";
        assert_eq!(filter_log(cut, &LogOptions::default()), "half a li\n");
        assert_eq!(filter_log("done\ncrashed mid li", &LogOptions::default()), "done\ncrashed mid li\n");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        std::fs::write(&path, "one\ncrashed mid li").unwrap();
        let p = path.to_str().unwrap();
        let mut offset = 0u64;
        assert_eq!(tail_from(p, &mut offset).unwrap(), "one\n");
        assert_eq!(rest_from(p, offset).unwrap(), "crashed mid li");
    }

    /// #131: `previous` as upstream parses it, and N runs back.
    #[test]
    fn previous_reads_true_false_or_n_runs_back() {
        let back = |v: Option<&str>| LogOptions { previous: v.map(String::from), ..Default::default() }.runs_back();
        assert_eq!(back(None), Ok(0));
        assert_eq!(back(Some("false")), Ok(0));
        assert_eq!(back(Some("true")), Ok(1));
        assert_eq!(back(Some("1")), Ok(1));
        assert_eq!(back(Some("3")), Ok(3));
        assert!(back(Some("yes")).is_err());

        let runs = [0, 1, 2, 3];
        assert_eq!(pick_run(&runs, 0), Ok(3));
        assert_eq!(pick_run(&runs, 1), Ok(2));
        assert_eq!(pick_run(&runs, 3), Ok(0));
        assert_eq!(pick_run(&runs, 4).unwrap_err(), "has no run 4 back (it has 3 before the current one)");
        assert_eq!(pick_run(&[0], 1).unwrap_err(), "has no previous run");
    }

    /// #131: a pod's network in /stats/summary, upstream's shape plus
    /// packets and drops; eth0 is the default.
    #[test]
    fn a_pods_network_summary_names_eth0_and_lists_every_interface() {
        use crate::cri::InterfaceStats;
        let net = network_summary(&[
            InterfaceStats { name: "net1".into(), rx_bytes: 1, ..Default::default() },
            InterfaceStats { name: "eth0".into(), rx_bytes: 10, rx_errors: 2, tx_dropped: 3, ..Default::default() },
        ]);
        assert_eq!(net["name"], "eth0");
        assert_eq!((net["rxBytes"].as_u64(), net["rxErrors"].as_u64(), net["txDropped"].as_u64()), (Some(10), Some(2), Some(3)));
        assert_eq!(net["interfaces"].as_array().unwrap().len(), 2);
        assert_eq!(network_summary(&[])["interfaces"], serde_json::json!([]));
    }

    #[test]
    fn tail_lines_keeps_the_last_n() {
        let body = "a\nb\nc\nd\n";
        let opts = LogOptions { tail_lines: Some(2), ..Default::default() };
        assert_eq!(filter_log(body, &opts), "c\nd\n");
    }

    #[test]
    fn limit_bytes_spends_a_budget_across_chunks() {
        let mut budget = Some(5);
        assert_eq!(cap("abc".to_string(), &mut budget), "abc");
        assert_eq!(budget, Some(2));
        // The second chunk is cut short and the budget is now spent, which is
        // what stops the follow loop.
        assert_eq!(cap("defgh".to_string(), &mut budget), "de");
        assert_eq!(budget, Some(0));
    }

    #[test]
    fn limit_bytes_cuts_on_a_char_boundary() {
        let mut budget = Some(2);
        // "é" is two bytes: a one-byte budget must yield nothing, not half of it.
        let mut one = Some(1);
        assert_eq!(cap("é".to_string(), &mut one), "");
        assert_eq!(cap("é".to_string(), &mut budget), "é");
    }

    #[test]
    fn no_limit_passes_everything() {
        let mut budget = None;
        assert_eq!(cap("anything at all".to_string(), &mut budget), "anything at all");
    }

    #[test]
    fn tail_from_returns_only_whole_lines_and_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        let p = path.to_str().unwrap().to_string();
        std::fs::write(&path, "one\ntwo\npart").unwrap();

        let mut offset = 0u64;
        assert_eq!(tail_from(&p, &mut offset).unwrap(), "one\ntwo\n");
        assert_eq!(offset, 8);
        // The partial line is not emitted until its newline arrives.
        assert_eq!(tail_from(&p, &mut offset).unwrap(), "");
        std::fs::write(&path, "one\ntwo\npartial\n").unwrap();
        assert_eq!(tail_from(&p, &mut offset).unwrap(), "partial\n");
    }

    #[test]
    fn tail_from_restarts_when_the_file_is_truncated() {
        // A rotated or recreated log is shorter than where we were reading;
        // holding the old offset would go silent for the life of the stream.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0.log");
        let p = path.to_str().unwrap().to_string();
        std::fs::write(&path, "long first line\n").unwrap();
        let mut offset = 0u64;
        assert_eq!(tail_from(&p, &mut offset).unwrap(), "long first line\n");
        std::fs::write(&path, "new\n").unwrap();
        assert_eq!(tail_from(&p, &mut offset).unwrap(), "new\n");
    }
}

/// `GET /vmConsole/{namespace}/{name}/{door}` — a VM's console.
///
/// The apiserver serves `subresources.kubevirt.io` so `virtctl console` has
/// something to resolve (rustkube#61). It cannot reach the console doors
/// itself — they read unix sockets under `/run/stormvm` on the node — and
/// this kubelet is already an authenticated hop (the apiserver's bearer
/// token, validated by TokenReview), so the console takes the route that
/// already exists rather than a second auth scheme.
///
/// The door used to be a second process on `:9095` that this handler dialled
/// and spliced. It is now `stormvm-console`'s own router, mounted in this
/// server and handed the request directly (rustkube-node#43). What that
/// deletes is worth naming: a TCP connect per session, ~150 lines that
/// re-implemented an HTTP client badly (parsing a response head by hand,
/// carrying the bytes that arrived after it), stormvm's weaker
/// "loopback, or a token" auth rule, and a long-lived process on every node
/// that runs a VM.
///
/// The only translation left is the path. The apiserver proxies to
/// `/vmConsole/{ns}/{name}/{door}` and the console router serves
/// `/api/v1/vms/{ns}/{name}/console/{door}`; rewriting the URI is cheaper
/// than changing a published subresource path, and the request — its
/// upgrade extension included, which is what makes the WebSocket work —
/// travels on untouched.
async fn vm_console(
    State(console): State<Router>,
    Path((namespace, name, door)): Path<(String, String, String)>,
    req: Request,
) -> Response {
    // stormvm's own spelling: `serial` and `vnc` are the doors it serves.
    // Refused here rather than forwarded, so a typo reads as a bad request
    // rather than as a 404 that could equally mean "no such VM".
    if door != "serial" && door != "vnc" {
        return (
            StatusCode::BAD_REQUEST,
            format!("no console door {door}: expected serial or vnc\n"),
        )
            .into_response();
    }

    let path = format!("/api/v1/vms/{namespace}/{name}/console/{door}");
    let uri = match path.parse::<axum::http::Uri>() {
        Ok(u) => u,
        Err(e) => {
            // Only reachable via a namespace or name that is not a legal path
            // segment, which the apiserver would not have accepted.
            return (StatusCode::BAD_REQUEST, format!("bad console path: {e}\n")).into_response();
        }
    };

    // **Hand the console the request its own listener would have built, not
    // this one with the URI swapped.** Three things have to be right, and
    // every one of them is a runtime failure that no type catches:
    //
    // 1. *The path parameters must not travel.* axum keeps the segments it
    //    captured in a request extension, and a router called with that
    //    extension still set appends its own to them. This route captures
    //    three and the console's captures two, so the door's `Path<(ns,
    //    name)>` was handed five and every console request failed as
    //    `Wrong number of path arguments`. Clearing the extensions is what
    //    makes this a handover rather than a nesting.
    //
    // 2. *`ConnectInfo` must be there.* The doors extract it, and axum
    //    inserts it only for a server built with
    //    `into_make_service_with_connect_info`. This one is not.
    //
    //    Loopback, and that is not a fiction: `is_local` asks whether the
    //    caller is already inside the node's boundary, and mounted here the
    //    caller *is* this process, on the node. The request also came through
    //    `auth_mw`, which is strictly stronger than the door's own rule —
    //    TLS and a TokenReview-validated bearer token, against
    //    "loopback, or a token" for an unauthenticated node-local port. That
    //    is the trade rustkube-node#43 describes: behind this server's auth,
    //    the console's token path is a fallback for nothing.
    //
    // 3. *The upgrade handle must survive the clear*, or the WebSocket the
    //    whole route exists for cannot be completed.
    //
    // The query goes with the path, which is also deliberate: the only
    // console query parameter is `token`, and a token must not reach the
    // doors from here for the same reason the `Authorization` header must
    // not — see below. (`to` belongs to the migrate and receive verbs, which
    // are not mounted.)
    to_console(console, uri, req).await
}

/// `PUT /vmVerb/{namespace}/{name}/{verb}`: a VM's verb (#94), answered by
/// stormvm's router (`PUT /api/v1/vms/{ns}/{name}/{verb}`) behind this
/// server's auth, as the console doors are. KubeVirt's `unfreeze` is stormvm's
/// `thaw`. `migrate` and `receive` are not offered: migration is driven by the
/// VMI's status (#40), never by a verb from outside.
async fn vm_verb(
    State(console): State<Router>,
    Path((namespace, name, verb)): Path<(String, String, String)>,
    req: Request,
) -> Response {
    const VERBS: &[&str] = &["pause", "unpause", "softreboot", "reset", "status", "freeze", "thaw", "snapshot"];
    let verb = if verb == "unfreeze" { "thaw".to_string() } else { verb };
    if !VERBS.contains(&verb.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            format!("no VM verb {verb}: expected one of {}, or unfreeze\n", VERBS.join(", ")),
        )
            .into_response();
    }
    // The verb's own parameters go with it (snapshot's `name`, `quiesce`); a
    // `token` does not, for the reason `to_console` drops `Authorization`.
    let query: String = req
        .uri()
        .query()
        .map(|q| {
            q.split('&')
                .filter(|kv| !kv.is_empty() && kv.split('=').next() != Some("token"))
                .collect::<Vec<_>>()
                .join("&")
        })
        .unwrap_or_default();
    let path = format!(
        "/api/v1/vms/{namespace}/{name}/{verb}{}{query}",
        if query.is_empty() { "" } else { "?" }
    );
    let uri = match path.parse::<axum::http::Uri>() {
        Ok(u) => u,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad verb path: {e}\n")).into_response(),
    };
    to_console(console, uri, req).await
}

/// Hand `req` to the console router at `uri`, as its own listener would have
/// built it (the reasons are on [`vm_console`]): the path captures cleared,
/// `ConnectInfo` loopback, the upgrade handle kept, `Authorization` dropped.
async fn to_console(console: Router, uri: axum::http::Uri, req: Request) -> Response {
    use tower::ServiceExt;
    let (mut parts, body) = req.into_parts();
    parts.uri = uri;

    // The apiserver's bearer token is already spent by `auth_mw` and is not a
    // console token. The door checks a presented token *before* it considers
    // loopback and refuses when it does not redeem, so forwarding this would
    // turn every authenticated console request into a 403 — including from
    // loopback, where it would otherwise be admitted outright.
    parts.headers.remove(AUTHORIZATION);

    let upgrade = parts.extensions.remove::<hyper::upgrade::OnUpgrade>();
    parts.extensions.clear();
    parts
        .extensions
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
    if let Some(upgrade) = upgrade {
        parts.extensions.insert(upgrade);
    }
    let req = Request::from_parts(parts, body);

    match console.oneshot(req).await {
        Ok(resp) => resp,
        // The router's error type is Infallible, so this arm is unreachable.
        Err(e) => {
            warn!("vm console: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "console failed\n").into_response()
        }
    }
}

#[cfg(test)]
mod volume_release_tests {
    use super::tests::app;
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    async fn release(uri: &str) -> StatusCode {
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        app().oneshot(req).await.unwrap().status()
    }

    /// The route exists and is a DELETE. `app()`'s PodManager points at the
    /// default loopback stormblock, which nothing is serving in a test, so
    /// the answer is the fail-closed one — which is the assertion worth
    /// making: an unreachable engine must not read as "released".
    #[tokio::test]
    async fn an_unreachable_engine_answers_503_not_204() {
        assert_eq!(release("/volumes/default/data").await, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A client that goes away mid-release does not take the claim's
    /// reservation with it (#100): the detach and delete finish, and until
    /// they have, no workload can be admitted to the claim.
    #[tokio::test]
    async fn a_dropped_request_keeps_the_claim_reserved_until_stormblock_answers() {
        use axum::routing::{delete as http_delete, get as http_get};
        use std::sync::atomic::{AtomicBool, Ordering};
        let deleted = Arc::new(AtomicBool::new(false));
        let d = deleted.clone();
        let engine = axum::Router::new()
            .route("/api/v1/volumes", http_get(|| async {
                axum::Json(serde_json::json!({"items": [{"id": "vol-1", "name": "pvc-default-data"}]}))
            }))
            .route("/api/v1/volumes/{id}/attach", http_delete(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                StatusCode::NO_CONTENT
            }))
            .route("/api/v1/volumes/{id}", http_delete(move || {
                let d = d.clone();
                async move { d.store(true, Ordering::SeqCst); StatusCode::NO_CONTENT }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, engine).await.unwrap() });

        let executor = crate::workload::Executor::new();
        let rt = Arc::new(super::tests::NoopRt);
        let pm = PodManager::new(rt.clone(), rt, "test-node")
            .with_engine(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()))
            .with_admission(executor.reservations.clone());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/volumes/default/data")
            .body(Body::empty())
            .unwrap();
        // The client gives up while the detach is in flight.
        let gave_up = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            router(Arc::new(pm)).oneshot(req),
        )
        .await;
        assert!(gave_up.is_err(), "the detach is slower than the client");
        let claim = crate::workload::Resource::Claim("default".into(), "data".into());
        assert!(executor.reservations.holder(&claim).is_some(), "released while stormblock was still working");

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while executor.reservations.holder(&claim).is_some() {
            assert!(tokio::time::Instant::now() < deadline, "the release never finished");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(deleted.load(Ordering::SeqCst), "the reservation went before the delete");
    }

    /// A GET on the release path is not a release. Destroying data through a
    /// safe method is the mistake this asserts against.
    #[tokio::test]
    async fn only_delete_releases() {
        let req = HttpRequest::builder()
            .uri("/volumes/default/data")
            .body(Body::empty())
            .unwrap();
        let status = app().oneshot(req).await.unwrap().status();
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }
}

#[cfg(test)]
mod console_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    /// The kubelet's server over a console router pointed at `run_dir`.
    fn app_with_console(run_dir: &str) -> Router {
        let rt = Arc::new(super::tests::NoopRt);
        let pm = Arc::new(PodManager::new(rt.clone(), rt, "test-node"));
        router_with_console(pm, None, console(run_dir, crate::engine::DEFAULT_URL))
    }

    async fn get(app: Router, uri: &str) -> axum::http::Response<Body> {
        let req = HttpRequest::builder().uri(uri).body(Body::empty()).unwrap();
        app.oneshot(req).await.unwrap()
    }

    /// Status and body together — a console failure is only diagnosable from
    /// the body, and an assertion on the status alone prints neither.
    async fn status_and_body(resp: axum::http::Response<Body>) -> (StatusCode, String) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    /// A door stormvm does not serve is refused here, so a typo reads as a
    /// bad request rather than as a 404 that could equally mean "no such VM".
    #[tokio::test]
    async fn an_unknown_door_is_refused_before_the_console_sees_it() {
        let dir = tempfile::tempdir().unwrap();
        let resp = get(
            app_with_console(dir.path().to_str().unwrap()),
            "/vmConsole/default/web-1/nonsense",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Write a console registration into `run_dir`, the way `vm_manager`
    /// does when it starts a machine.
    ///
    /// Built through serde rather than as a struct literal: `Registration`
    /// has no `Default` and gains fields, and a test that named them all
    /// would break on every one. Only namespace and name are required.
    fn register(run_dir: &str, namespace: &str, name: &str) {
        let reg: stormvm_node::console::Registration = serde_json::from_value(serde_json::json!({
            "namespace": namespace,
            "name": name,
            "serial_socket": format!("{run_dir}/{namespace}/{name}/serial.sock"),
        }))
        .unwrap();
        stormvm_node::console::write(run_dir, &reg).unwrap();
    }

    /// Serve the kubelet's router on a real socket and return its address.
    ///
    /// A real connection, not `oneshot`, because the doors cannot be reached
    /// any other way: `WebSocketUpgrade` is an *extractor*, so it runs before
    /// the handler body and rejects anything that is not a handshake — which
    /// means a request built in memory never reaches the VM lookup or the
    /// admission check at all. Only a real one carries the upgrade handle
    /// hyper puts in the extensions.
    async fn serve_console(run_dir: &str) -> SocketAddr {
        let app = app_with_console(run_dir);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    /// Open a console with a real WebSocket handshake, carrying the
    /// apiserver's bearer token exactly as `auth_mw` would have seen it.
    /// Returns the status line and the rest of the response.
    async fn handshake(addr: SocketAddr, path: &str) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\n\
             Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Authorization: Bearer an-apiserver-token\r\n\r\n"
        );
        sock.write_all(req.as_bytes()).await.unwrap();

        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]).to_string();
        let status: u16 = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        (status, text)
    }

    /// An unregistered VM gets the console's own answer — "no vm here" —
    /// and not a 502 from a socket nothing is listening on, which is what
    /// the old splice returned whenever stormvm was not running.
    #[tokio::test]
    async fn an_unregistered_vm_gets_the_consoles_own_answer() {
        let dir = tempfile::tempdir().unwrap();
        let addr = serve_console(dir.path().to_str().unwrap()).await;
        let (status, body) = handshake(addr, "/vmConsole/default/web-1/serial").await;
        assert_eq!(status, 404, "{body}");
        assert!(body.contains("no vm default/web-1"), "{body}");
    }

    /// **The test that proves the mount.** A registered VM is admitted and
    /// the console answers `101`, which means the request reached the door,
    /// resolved the VM out of the run directory, and passed admission.
    ///
    /// Two failures this rules out, both invisible to the type system and
    /// both real before they were fixed:
    ///
    /// - the door extracts `ConnectInfo`, which axum inserts only for a
    ///   server built with `into_make_service_with_connect_info` — this one
    ///   is not, so a missing injection is a 500;
    /// - the door checks a presented bearer token before it considers
    ///   loopback, and the apiserver's token does not redeem — so the
    ///   `Authorization` header this handshake carries would be a 403 if it
    ///   were forwarded rather than stripped.
    #[tokio::test]
    async fn a_registered_vm_is_admitted_and_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "default", "web-1");
        let addr = serve_console(run_dir).await;

        let (status, body) = handshake(addr, "/vmConsole/default/web-1/serial").await;
        assert_ne!(status, 500, "ConnectInfo was not injected: {body}");
        assert_ne!(status, 403, "the apiserver's token reached the door: {body}");
        assert_ne!(status, 404, "the registration was not found: {body}");
        assert_eq!(status, 101, "an admitted console must upgrade: {body}");
    }

    /// The kubelet's own `/healthz` still answers. The console router serves
    /// one too, so this is the collision that mounting has to avoid: the
    /// doors are reached by rewriting `/vmConsole/...`, never by merging the
    /// console's paths into this server's namespace.
    #[tokio::test]
    async fn mounting_the_console_does_not_take_over_healthz() {
        let dir = tempfile::tempdir().unwrap();
        let app = app_with_console(dir.path().to_str().unwrap());
        assert_eq!(get(app.clone(), "/healthz").await.status(), StatusCode::OK);
        // And the console's own paths are not served here: this server's API
        // surface is the kubelet's, not stormvm's.
        assert_eq!(get(app, "/api/v1/vms").await.status(), StatusCode::NOT_FOUND);
    }

    async fn put_verb(app: Router, uri: &str) -> (StatusCode, String) {
        let req = HttpRequest::builder().method("PUT").uri(uri).body(Body::empty()).unwrap();
        status_and_body(app.oneshot(req).await.unwrap()).await
    }

    /// #94: the verbs reach stormvm's router through the kubelet: `snapshot`
    /// with its own parameters gets as far as the machine (no hypervisor
    /// listens here), so the route, the loopback admission and the stripped
    /// `token` all held; an unregistered VM gets the router's own answer.
    #[tokio::test]
    async fn a_vm_verb_reaches_stormvms_router_through_the_kubelet() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "default", "web-1");
        let app = app_with_console(run_dir);

        let (status, body) =
            put_verb(app.clone(), "/vmVerb/default/web-1/snapshot?name=before&quiesce=never&token=nope").await;
        assert_ne!(status, StatusCode::FORBIDDEN, "a token reached the router: {body}");
        assert!(body.contains("hypervisor"), "{status}: {body}");

        let (status, body) = put_verb(app.clone(), "/vmVerb/default/ghost/pause").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("no vm default/ghost"), "the router's answer, not a missing route: {body}");

        // KubeVirt's spelling of thaw is accepted and forwarded.
        let (status, body) = put_verb(app.clone(), "/vmVerb/default/web-1/unfreeze").await;
        assert_ne!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(!body.contains("no VM verb"), "{body}");
    }

    #[tokio::test]
    async fn a_verb_that_is_not_offered_is_refused_here() {
        let dir = tempfile::tempdir().unwrap();
        let app = app_with_console(dir.path().to_str().unwrap());
        for verb in ["migrate", "receive", "nonsense"] {
            let (status, body) = put_verb(app.clone(), &format!("/vmVerb/default/web-1/{verb}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{verb}: {body}");
            assert!(body.contains("no VM verb"), "{body}");
        }
        assert_eq!(get(app, "/vmVerb/default/web-1/pause").await.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// #83: the console is told where stormblock is, so `snapshot` gets past
    /// "this service was not told where stormblock is" to the machine itself.
    ///
    /// Asked of the console router directly (through the kubelet it is
    /// `a_vm_verb_reaches_stormvms_router_through_the_kubelet`, #94). No hypervisor listens here, so the answer
    /// is the machine's silence — which is only reached once stormblock is set.
    #[tokio::test]
    async fn the_snapshot_verb_knows_where_stormblock_is() {
        use axum::extract::ConnectInfo;

        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "default", "web-1");

        let mut req = HttpRequest::builder()
            .method("PUT")
            .uri("/api/v1/vms/default/web-1/snapshot?name=before&quiesce=never")
            .body(Body::empty())
            .unwrap();
        // The verb admits loopback, as the doors do.
        req.extensions_mut().insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40000))));
        let resp = console(run_dir, "http://127.0.0.1:9").oneshot(req).await.unwrap();
        let (status, body) = status_and_body(resp).await;

        assert!(!body.contains("not told where stormblock is"), "{status}: {body}");
        assert!(body.contains("hypervisor"), "{status}: {body}");
    }
}
