//! The kubelet's inbound HTTP server (upstream `:10250`).
//!
//! Liveness, a Prometheus `/metrics` endpoint, `/stats/summary`, `/pods` (the
//! pods this kubelet manages) and `/containerLogs` — the endpoint the
//! apiserver proxies `kubectl logs` to (rustkube-node#34). Exec, attach and
//! portforward are follow-ups (rustkube-node#7). Served over HTTPS with
//! bearer-token auth (rustkube-node#9).

use crate::pod_manager::PodManager;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing::get, Json, Router};
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

/// Build the (unauthenticated) router — exposed for tests.
pub fn router(pod_manager: Arc<PodManager>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/livez", get(healthz))
        .route("/readyz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/metrics/cadvisor", get(metrics_cadvisor))
        .route("/stats/summary", get(stats_summary))
        .route("/pods", get(pods))
        // What `kubectl logs` reads, by way of the apiserver proxy.
        // `:param`, not `{param}` — this crate is on axum 0.7, where braces are
        // a *literal* segment. Written the 0.8 way the route matched nothing
        // and axum answered its own empty 404, which reads as "no such pod"
        // rather than "no such route". The apiserver is on 0.8 and its routes
        // use braces, so the two spellings coexist and neither is a typo.
        .route("/containerLogs/:namespace/:pod/:container", get(container_logs))
        .with_state(pod_manager)
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

    let addr: std::net::SocketAddr = ([0, 0, 0, 0], port).into();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cri::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // oneshot

    // A do-nothing runtime/image service so we can build a PodManager.
    struct NoopRt;
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

    fn app() -> Router {
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
