//! The kubelet's inbound HTTP server (upstream `:10250`).
//!
//! Minimal today: liveness, a Prometheus `/metrics` endpoint, and `/pods`
//! (the pods this kubelet manages). Exec/attach/portforward and
//! `/stats/summary` are follow-ups (rustkube-node#7). Plain HTTP for now;
//! TLS + bearer-token auth is the TLS phase (rustkube-node#9).

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
        .route(
            "/containerLogs/{namespace}/{pod}/{container}",
            get(container_logs),
        )
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
async fn container_logs(
    State(pm): State<Arc<PodManager>>,
    Path((namespace, pod, container)): Path<(String, String, String)>,
    Query(opts): Query<LogOptions>,
) -> impl IntoResponse {
    let Some(uid) = pm.pod_uid(&namespace, &pod).await else {
        return (
            StatusCode::NOT_FOUND,
            format!("pod {namespace}/{pod} not found on this node\n"),
        );
    };
    let dir = format!("/var/log/pods/{namespace}_{pod}_{uid}/{container}");

    // `previous` asks for the run before the current one. Restarts are numbered
    // from 0, so the current file is the highest and "previous" is the one
    // below it — absent for a container that has never restarted, which is a
    // 400 upstream rather than an empty success.
    let mut runs: Vec<u32> = match std::fs::read_dir(&dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                e.file_name().to_str().and_then(|n| n.strip_suffix(".log")).and_then(|n| n.parse().ok())
            })
            .collect(),
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                format!("no logs for container {container} in {namespace}/{pod}\n"),
            )
        }
    };
    runs.sort_unstable();
    let want = if opts.previous.unwrap_or(false) {
        match runs.len().checked_sub(2).and_then(|i| runs.get(i)) {
            Some(r) => *r,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("container {container} has no previous run\n"),
                )
            }
        }
    } else {
        match runs.last() {
            Some(r) => *r,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("no logs for container {container} in {namespace}/{pod}\n"),
                )
            }
        }
    };

    let path = format!("{dir}/{want}.log");
    let body = match std::fs::read_to_string(&path) {
        Ok(b) => b,
        Err(e) => return (StatusCode::NOT_FOUND, format!("cannot read {path}: {e}\n")),
    };
    (StatusCode::OK, filter_log(&body, &opts))
}

/// The query parameters `kubectl logs` sends.
#[derive(Debug, Default, serde::Deserialize)]
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
    /// Accepted and ignored: streaming is a separate change, and a client that
    /// asks for it should get the log it asked for rather than an error.
    #[allow(dead_code)]
    follow: Option<bool>,
    /// Accepted and ignored — a byte cap on the response.
    #[allow(dead_code)]
    limit_bytes: Option<usize>,
}

/// Apply the CRI log-format options to a file's contents.
///
/// The runtime writes CRI format: `<rfc3339nano> <stdout|stderr> <F|P> <line>`.
/// `kubectl logs` shows only the message unless `timestamps` is set, so the
/// prefix is stripped here rather than left for the client to be surprised by.
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
