//! The kubelet's inbound HTTP server (upstream `:10250`).
//!
//! Liveness, a Prometheus `/metrics` endpoint, `/stats/summary`, `/pods` (the
//! pods this kubelet manages) and `/containerLogs` — the endpoint the
//! apiserver proxies `kubectl logs` to (rustkube-node#34). Exec, attach and
//! portforward are follow-ups (rustkube-node#7). Served over HTTPS with
//! bearer-token auth (rustkube-node#9).
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

use crate::pod_manager::{PodManager, VolumeRelease};
use axum::extract::{ConnectInfo, FromRef, Path, Query, Request, State};
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing::{delete, get}, Json, Router};
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

impl FromRef<AppState> for Router {
    fn from_ref(state: &AppState) -> Router {
        state.console.clone()
    }
}

/// Build the (unauthenticated) router — exposed for tests.
pub fn router(pod_manager: Arc<PodManager>) -> Router {
    router_with_console(
        pod_manager,
        stormvm_console::router(stormvm_console::Config {
            run_dir: crate::vm_manager::RUN_ROOT.into(),
            ..Default::default()
        }),
    )
}

/// The router, over a given console. Tests build one against a run directory
/// of their own; nothing else needs this.
fn router_with_console(pod_manager: Arc<PodManager>, console: Router) -> Router {
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
        // Release the stormblock clone behind a claim, so a `Delete` reclaim
        // policy finishes (rustkube-node#46). Same shape as the VM console:
        // control plane → kubelet → a service the control plane cannot reach.
        // `DELETE` rather than a verb under some other path because it
        // destroys data, and should read that way in an audit log.
        .route("/volumes/{namespace}/{claim}", delete(release_volume))
        .with_state(AppState { pods: pod_manager, console })
}

/// Serve the kubelet API over HTTPS on `0.0.0.0:<port>` with bearer-token auth
/// on everything except the health endpoints. Runs until the process exits.
pub async fn serve(port: u16, pod_manager: Arc<PodManager>, config: ServerConfig) {
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
    let app = router(pod_manager).layer(middleware::from_fn_with_state(auth, auth_mw));

    // Serving cert: use the provided pair, else self-sign.
    let (cert_pem, key_pem) = match (&config.tls_cert, &config.tls_key) {
        (Some(c), Some(k)) => (c.clone(), k.clone()),
        _ => match self_signed_cert(&config.node_name, &config.node_ip) {
            Ok(pair) => pair,
            Err(e) => {
                warn!("kubelet server: self-signed cert generation failed: {e}");
                return;
            }
        },
    };
    let tls = match axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem, key_pem).await {
        Ok(t) => t,
        Err(e) => {
            warn!("kubelet server: TLS config failed: {e}");
            return;
        }
    };

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
    match auth.api_client.post(&url).json(&body).send().await {
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

async fn metrics(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    let (pods, containers) = pm.metrics_snapshot().await;
    let body = format!(
        "# HELP kubelet_running_pods Number of pods managed by this kubelet.\n\
         # TYPE kubelet_running_pods gauge\n\
         kubelet_running_pods {pods}\n\
         # HELP kubelet_running_containers Number of containers managed by this kubelet.\n\
         # TYPE kubelet_running_containers gauge\n\
         kubelet_running_containers {containers}\n"
    );
    ([("content-type", "text/plain; version=0.0.4")], body)
}

async fn pods(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    Json(pm.pods_json().await)
}

/// cAdvisor-style container metrics scraped by Prometheus.
async fn metrics_cadvisor(State(pm): State<Arc<PodManager>>) -> impl IntoResponse {
    let stats = pm.container_stats().await;
    let mut body = String::new();
    body.push_str("# HELP container_cpu_usage_seconds_total Cumulative CPU time consumed (seconds).\n");
    body.push_str("# TYPE container_cpu_usage_seconds_total counter\n");
    for s in &stats {
        let labels = format!(
            "container=\"{}\",pod=\"{}\",namespace=\"{}\"",
            s.name, s.pod, s.namespace
        );
        let secs = s.cpu_usage_core_nanos as f64 / 1e9;
        body.push_str(&format!("container_cpu_usage_seconds_total{{{labels}}} {secs}\n"));
    }
    body.push_str("# HELP container_memory_working_set_bytes Current working set (bytes).\n");
    body.push_str("# TYPE container_memory_working_set_bytes gauge\n");
    for s in &stats {
        let labels = format!(
            "container=\"{}\",pod=\"{}\",namespace=\"{}\"",
            s.name, s.pod, s.namespace
        );
        body.push_str(&format!(
            "container_memory_working_set_bytes{{{labels}}} {}\n",
            s.memory_working_set_bytes
        ));
    }
    ([("content-type", "text/plain; version=0.0.4")], body)
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
        node_cpu += s.cpu_usage_core_nanos;
        node_mem += s.memory_working_set_bytes;
        by_pod
            .entry((s.namespace.clone(), s.pod.clone()))
            .or_default()
            .push(serde_json::json!({
                "name": s.name,
                "cpu": {"usageCoreNanoSeconds": s.cpu_usage_core_nanos},
                "memory": {"workingSetBytes": s.memory_working_set_bytes},
            }));
    }
    let pods: Vec<serde_json::Value> = by_pod
        .into_iter()
        .map(|((ns, name), containers)| {
            serde_json::json!({
                "podRef": {"name": name, "namespace": ns},
                "containers": containers,
            })
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
async fn release_volume(
    State(pod_manager): State<Arc<PodManager>>,
    Path((namespace, claim)): Path<(String, String)>,
) -> Response {
    match pod_manager.release_claim_volume(&namespace, &claim).await {
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

    pub(super) fn app() -> Router {
        let rt = Arc::new(NoopRt);
        let pm = Arc::new(PodManager::new(rt.clone(), rt, "test-node"));
        router(pm)
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
        assert!(text.contains("kubelet_running_pods 0"));
        assert!(text.contains("kubelet_running_containers 0"));
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
    let consumed = raw.rfind('\n').map(|i| i + 1).unwrap_or(0);
    let mut budget = opts.limit_bytes;
    let head = cap(filter_log(&raw[..consumed], &opts), &mut budget);

    if !opts.follow.unwrap_or(false) {
        return (StatusCode::OK, head).into_response();
    }
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
    let want = if opts.previous.unwrap_or(false) {
        match runs.len().checked_sub(2).and_then(|i| runs.get(i)) {
            Some(r) => *r,
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("container {container} has no previous run\n"),
                )
                    .into_response())
            }
        }
    } else {
        match runs.last() {
            Some(r) => *r,
            None => return Err(none()),
        }
    };
    Ok(format!("{dir}/{want}.log"))
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
    /// The run before the current one.
    previous: Option<bool>,
    /// Keep the response open and send what is appended.
    follow: Option<bool>,
    /// A byte cap on the response, counted after filtering.
    limit_bytes: Option<usize>,
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
/// Lines are passed through rather than dropped when they are not CRI format:
/// a log the reader cannot see is worse than one without a timestamp. The
/// alternative — refusing `--timestamps` outright — would break the common
/// invocation to signal something the caller cannot act on anyway. If per-line
/// timestamps become worth having, they cost the zero-copy property, and that
/// is the trade to weigh rather than a bug to fix.
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
    for line in body.lines() {
        // `<ts> <stream> <tag> <message>` — split off exactly three fields, so
        // a message containing spaces survives intact.
        let mut it = line.splitn(4, ' ');
        let (ts, _stream, _tag, msg) = match (it.next(), it.next(), it.next(), it.next()) {
            (Some(a), Some(b), Some(c), Some(d)) => (a, b, c, d),
            // Not CRI format: pass it through rather than drop it. A log the
            // reader cannot see is worse than one with an odd prefix.
            _ => {
                out.push(line.to_string());
                continue;
            }
        };
        if let Some(cut) = cutoff {
            match chrono::DateTime::parse_from_rfc3339(ts) {
                Ok(t) if t.with_timezone(&chrono::Utc) < cut => continue,
                _ => {}
            }
        }
        if opts.timestamps.unwrap_or(false) {
            out.push(format!("{ts} {msg}"));
        } else {
            out.push(msg.to_string());
        }
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
    mut req: Request,
) -> Response {
    use tower::ServiceExt;

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
        router_with_console(
            pm,
            stormvm_console::router(stormvm_console::Config {
                run_dir: run_dir.to_string(),
                ..Default::default()
            }),
        )
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

    /// An unregistered VM gets the console's own "no vm here" answer — 404
    /// with its message, not a 502 from a socket nothing is listening on,
    /// which is what the old splice returned whenever stormvm was not running.
    #[tokio::test]
    async fn an_unregistered_vm_gets_the_consoles_own_answer() {
        let dir = tempfile::tempdir().unwrap();
        let resp = get(
            app_with_console(dir.path().to_str().unwrap()),
            "/vmConsole/default/web-1/serial",
        )
        .await;
        let (status, body) = status_and_body(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("no vm default/web-1"), "{body}");
    }

    /// **The test that proves the mount.** A registered VM must get *past*
    /// admission, and the two ways it would not are both invisible to the
    /// type system:
    ///
    /// - the door extracts `ConnectInfo`, which axum inserts only for a
    ///   server built with `into_make_service_with_connect_info` — this one
    ///   is not, so a missing injection is a 500;
    /// - the door checks a presented bearer token before it considers
    ///   loopback, and the apiserver's token does not redeem — so a
    ///   forwarded `Authorization` header is a 403.
    ///
    /// Sent without the WebSocket handshake headers, an admitted request
    /// reaches `WebSocketUpgrade` and is rejected there — 426, or 400. That
    /// is the pass: it means the request got all the way to the upgrade.
    #[tokio::test]
    async fn a_registered_vm_is_admitted_through_the_mount() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        // Built through serde rather than as a literal: `Registration` has no
        // `Default` and gains fields, and a test that named them all would
        // break on every one. Only namespace and name are required.
        let reg: stormvm_node::console::Registration = serde_json::from_value(serde_json::json!({
            "namespace": "default",
            "name": "web-1",
            "serial_socket": format!("{run_dir}/default/web-1/serial.sock"),
        }))
        .unwrap();
        stormvm_node::console::write(run_dir, &reg).unwrap();

        // With the apiserver's credential attached, exactly as auth_mw saw it.
        let req = HttpRequest::builder()
            .uri("/vmConsole/default/web-1/serial")
            .header("authorization", "Bearer an-apiserver-token")
            .body(Body::empty())
            .unwrap();
        let resp = app_with_console(run_dir).oneshot(req).await.unwrap();
        let (status, body) = status_and_body(resp).await;

        assert_ne!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "ConnectInfo was not injected — the door could not read its peer: {body}"
        );
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "the apiserver's bearer token reached the door and was refused: {body}"
        );
        assert_ne!(status, StatusCode::NOT_FOUND, "the registration was not found: {body}");
        assert!(
            status == StatusCode::UPGRADE_REQUIRED || status == StatusCode::BAD_REQUEST,
            "an admitted request should reach the WebSocket upgrade: got {status} — {body}"
        );
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
}
