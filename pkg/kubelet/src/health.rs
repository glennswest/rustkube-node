//! Health probes — liveness, readiness, startup.
//!
//! Implements HTTP GET, TCP connect, gRPC, and exec probes. HTTP/TCP/gRPC
//! probes are run **inside the pod's network namespace** when the runtime
//! reports it, so a probe targeting `127.0.0.1` (or any loopback-bound health
//! server, e.g. cilium-operator's `localhost:9234`) actually reaches the
//! container rather than the host. Named ports (`port: "health"`) are resolved
//! from the container's `ports[]`. (rustkube-node#15)
//!
//! As upstream's prober (#443):
//! - an `httpGet` probe sends its `httpHeaders`, a `Host` among them replacing
//!   the URL's, plus `User-Agent: kube-probe/<major.minor>` and `Accept: */*`
//!   unless the probe sets them;
//! - a `grpc` probe calls `grpc.health.v1.Health/Check` with its `service`
//!   (empty when unset) and passes only on `SERVING`. Any other status, an RPC
//!   error (`UNIMPLEMENTED`: no health service) or a listener that does not
//!   speak gRPC is a failure.
//!
//! An `https` httpGet probe in the pod netns (#452) is a full HTTPS GET over a
//! connection made in the netns: TLS without certificate verification (as
//! upstream's prober and the host path), the probe's headers, 200–399 passes.

use crate::cri::RuntimeService;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::debug;

/// Result of a health probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeResult {
    Success,
    Failure(String),
    Unknown,
}

/// Resolve an httpGet/tcpSocket/grpc `port` value to a number. Accepts a numeric
/// port, a numeric string, or a named port resolved against the container
/// spec's `ports[].name → containerPort`.
fn resolve_port(port_field: &Value, container_spec: &Value) -> Option<u16> {
    if let Some(n) = port_field.as_u64() {
        return u16::try_from(n).ok();
    }
    let name = port_field.as_str()?;
    if let Ok(n) = name.parse::<u16>() {
        return Some(n);
    }
    let ports = container_spec["ports"].as_array()?;
    for p in ports {
        if p["name"].as_str() == Some(name) {
            return p["containerPort"].as_u64().and_then(|n| u16::try_from(n).ok());
        }
    }
    None
}

/// The generated gRPC health protocol (`proto/grpc/health/v1/health.proto`).
pub mod proto {
    tonic::include_proto!("grpc.health.v1");
}

/// `User-Agent` of a probe that sets none: upstream's `kube-probe/<major>.<minor>`,
/// from the Kubernetes version this kubelet reports.
fn probe_user_agent() -> String {
    let v = crate::node_status::KUBERNETES_POSTURE.trim_start_matches('v');
    let mut parts = v.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("kube-probe/{major}.{minor}"),
        _ => "kube-probe".to_string(),
    }
}

/// An `httpGet` probe's request headers, in the order given: its
/// `httpHeaders`, then `User-Agent` and `Accept` unless it set them (#443).
/// A name or value that cannot be sent (a CR or LF in it) is an error, so the
/// probe fails naming it rather than going out without it.
fn probe_headers(http: &Value) -> Result<Vec<(HeaderName, HeaderValue)>, String> {
    let mut out = Vec::new();
    for h in http["httpHeaders"].as_array().into_iter().flatten() {
        let name = h["name"].as_str().unwrap_or("");
        if name.is_empty() {
            continue;
        }
        let value = h["value"].as_str().unwrap_or("");
        let n = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("httpGet: header name {name:?}: {e}"))?;
        let v = HeaderValue::from_str(value)
            .map_err(|e| format!("httpGet: header {name} value: {e}"))?;
        out.push((n, v));
    }
    let has = |out: &[(HeaderName, HeaderValue)], n: &HeaderName| out.iter().any(|(k, _)| k == n);
    let (has_ua, has_accept) = (
        has(&out, &reqwest::header::USER_AGENT),
        has(&out, &reqwest::header::ACCEPT),
    );
    if !has_ua {
        let ua = HeaderValue::from_str(&probe_user_agent()).map_err(|e| e.to_string())?;
        out.push((reqwest::header::USER_AGENT, ua));
    }
    if !has_accept {
        out.push((reqwest::header::ACCEPT, HeaderValue::from_static("*/*")));
    }
    Ok(out)
}

/// `host:port`, bracketing an IPv6 address.
fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Run a health probe based on the probe spec.
///
/// `container_spec` is used to resolve named ports; `netns_path`, when present,
/// is the pod network namespace (`/proc/<pid>/ns/net`) that http/tcp probes are
/// executed inside.
pub async fn run_probe(
    probe_spec: &Value,
    container_spec: &Value,
    container_id: &str,
    pod_ip: &str,
    netns_path: Option<&str>,
    runtime: &Arc<dyn RuntimeService>,
) -> ProbeResult {
    let timeout_secs = probe_spec["timeoutSeconds"].as_u64().unwrap_or(1).max(1);
    let timeout = Duration::from_secs(timeout_secs);

    // HTTP GET probe
    if !probe_spec["httpGet"].is_null() {
        let http = &probe_spec["httpGet"];
        let port = match resolve_port(&http["port"], container_spec) {
            Some(p) => p,
            None => {
                return ProbeResult::Failure(format!(
                    "httpGet: could not resolve port {}",
                    http["port"]
                ))
            }
        };
        let path = http["path"].as_str().unwrap_or("/");
        let scheme = http["scheme"].as_str().unwrap_or("HTTP").to_lowercase();
        // An empty host means "the pod IP" (or loopback, when run in the netns).
        let host = http["host"].as_str().filter(|h| !h.is_empty()).unwrap_or(pod_ip);
        let headers = match probe_headers(http) {
            Ok(h) => h,
            Err(e) => return ProbeResult::Failure(e),
        };
        return http_probe(&scheme, host, port, path, &headers, netns_path, timeout).await;
    }

    // TCP socket probe
    if !probe_spec["tcpSocket"].is_null() {
        let tcp = &probe_spec["tcpSocket"];
        let port = match resolve_port(&tcp["port"], container_spec) {
            Some(p) => p,
            None => {
                return ProbeResult::Failure(format!(
                    "tcpSocket: could not resolve port {}",
                    tcp["port"]
                ))
            }
        };
        let host = tcp["host"].as_str().filter(|h| !h.is_empty()).unwrap_or(pod_ip);
        return tcp_probe(host, port, netns_path, timeout).await;
    }

    // Exec probe (already runs inside the container's namespaces via the runtime)
    if !probe_spec["exec"].is_null() {
        let command: Vec<String> = probe_spec["exec"]["command"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        if command.is_empty() {
            return ProbeResult::Failure("exec probe has no command".into());
        }

        return exec_probe(container_id, &command, timeout_secs as i64, runtime).await;
    }

    // gRPC probe: grpc.health.v1.Health/Check (in the pod netns when known)
    if !probe_spec["grpc"].is_null() {
        let grpc = &probe_spec["grpc"];
        let port = match resolve_port(&grpc["port"], container_spec) {
            Some(p) => p,
            None => return ProbeResult::Failure("grpc: could not resolve port".into()),
        };
        let service = grpc["service"].as_str().unwrap_or("");
        return grpc_probe(pod_ip, port, service, netns_path, timeout).await;
    }

    ProbeResult::Success // No probe configured = always succeeds
}

/// HTTP GET probe — inside the pod netns when known, else from the host.
async fn http_probe(
    scheme: &str,
    host: &str,
    port: u16,
    path: &str,
    headers: &[(HeaderName, HeaderValue)],
    netns_path: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    // In the pod netns, HTTPS takes the gRPC probe's route: the connection is
    // made there and TLS runs over it here (#452).
    if scheme == "https" && netns_path.is_some() {
        return https_get_probe(host, port, path, headers, netns_path, timeout).await;
    }
    #[cfg(target_os = "linux")]
    if let Some(ns) = netns_path {
        return dial_in_netns(
            ns.to_string(),
            NetDial::Http {
                host: host.to_string(),
                port,
                path: path.to_string(),
                headers: headers.to_vec(),
            },
            timeout,
        )
        .await;
    }
    let _ = netns_path; // only consulted on Linux
    http_get_probe(scheme, host, port, path, headers, timeout).await
}

/// A TCP connection to `host:port`, made inside the pod netns when known. A
/// socket keeps the namespace it was made in, so the connection is made on a
/// thread that entered the netns and then used from here.
async fn connect_probe(
    host: &str,
    port: u16,
    netns_path: Option<&str>,
    timeout: Duration,
) -> Result<TcpStream, String> {
    #[cfg(target_os = "linux")]
    if let Some(ns) = netns_path {
        let (ns, host) = (ns.to_string(), host.to_string());
        let (tx, rx) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = tx.send(enter_netns(&ns).and_then(|()| {
                let sa = resolve_one_addr(&host, port)?;
                std::net::TcpStream::connect_timeout(&sa, timeout)
                    .map_err(|e| format!("tcp connect {}: {e}", host_port(&host, port)))
            }));
        });
        let std_stream = match tokio::time::timeout(timeout + Duration::from_secs(2), rx).await {
            Ok(Ok(r)) => r?,
            Ok(Err(_)) => return Err("probe thread canceled".into()),
            Err(_) => return Err("probe timed out".into()),
        };
        std_stream
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking: {e}"))?;
        return TcpStream::from_std(std_stream).map_err(|e| format!("tcp stream: {e}"));
    }
    let _ = netns_path;
    let addr = host_port(host, port);
    match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(format!("tcp connect {addr}: {e}")),
        Err(_) => Err(format!("tcp connect {addr}: timed out")),
    }
}

/// An `https` httpGet probe over a connection made by `connect_probe` (in the
/// pod netns when known, #452). The whole call, connection included, is
/// bounded by `timeout`.
async fn https_get_probe(
    host: &str,
    port: u16,
    path: &str,
    headers: &[(HeaderName, HeaderValue)],
    netns_path: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    debug!("HTTPS probe: {}{path}", host_port(host, port));
    let call = async {
        let stream = connect_probe(host, port, netns_path, timeout).await?;
        https_get_over(stream, host, port, path, headers).await
    };
    match tokio::time::timeout(timeout, call).await {
        Ok(Ok(())) => ProbeResult::Success,
        Ok(Err(e)) => ProbeResult::Failure(e),
        Err(_) => ProbeResult::Failure("https probe timed out".into()),
    }
}

/// TLS over `stream`, then the probe's GET and its status line: 200–399 passes.
async fn https_get_over(
    stream: TcpStream,
    host: &str,
    port: u16,
    path: &str,
    headers: &[(HeaderName, HeaderValue)],
) -> Result<(), String> {
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // A host that is no valid DNS name or IP (the server name is only SNI here,
    // nothing is verified against it) is named by the address it reached.
    let name = match ServerName::try_from(host.to_string()) {
        Ok(n) => n,
        Err(_) => ServerName::IpAddress(
            stream.peer_addr().map_err(|e| format!("peer address: {e}"))?.ip().into(),
        ),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(probe_tls_config()));
    let mut tls = connector
        .connect(name, stream)
        .await
        .map_err(|e| format!("tls handshake {}: {e}", host_port(host, port)))?;
    tls.write_all(&probe_request(host, port, path, headers))
        .await
        .map_err(|e| format!("write: {e}"))?;
    tls.flush().await.map_err(|e| format!("write: {e}"))?;
    let mut head = Vec::new();
    let mut chunk = [0u8; 512];
    while head.len() < 512 && !head.windows(2).any(|w| w == b"\r\n") {
        let n = tls.read(&mut chunk).await.map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..n]);
    }
    probe_status(&head)
}

/// The TLS client of an `https` probe in the pod netns: any certificate is
/// accepted, as upstream's prober (`InsecureSkipVerify`) and the host path
/// (`danger_accept_invalid_certs`) do. The handshake's signatures are still
/// checked, so the peer holds the key of the certificate it shows.
fn probe_tls_config() -> rustls::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring's provider supports the default TLS versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
        .with_no_client_auth()
}

#[derive(Debug)]
struct AnyCertificate(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// The request a probe sends in the pod netns, plain or over TLS: HTTP/1.0,
/// the probe's own `Host` replacing the URL's, every other header as given
/// (names and values were checked to hold no CR or LF).
fn probe_request(host: &str, port: u16, path: &str, headers: &[(HeaderName, HeaderValue)]) -> Vec<u8> {
    let path = if path.is_empty() { "/" } else { path };
    let mut req = format!("GET {path} HTTP/1.0\r\n").into_bytes();
    if !headers.iter().any(|(n, _)| n == reqwest::header::HOST) {
        req.extend_from_slice(format!("Host: {}\r\n", host_port(host, port)).as_bytes());
    }
    for (n, v) in headers {
        req.extend_from_slice(n.as_str().as_bytes());
        req.extend_from_slice(b": ");
        req.extend_from_slice(v.as_bytes());
        req.extend_from_slice(b"\r\n");
    }
    req.extend_from_slice(b"Connection: close\r\n\r\n");
    req
}

/// A response's first bytes: its status line decides, 200–399 passes.
fn probe_status(head: &[u8]) -> Result<(), String> {
    if head.is_empty() {
        return Err("empty response".into());
    }
    let text = String::from_utf8_lossy(head);
    let status_line = text.lines().next().unwrap_or("");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| format!("bad status line: {status_line}"))?;
    if (200..400).contains(&code) {
        Ok(())
    } else {
        Err(format!("HTTP {code}"))
    }
}

/// gRPC probe: `grpc.health.v1.Health/Check` for `service`, `SERVING` only
/// (#443). The whole call, connection included, is bounded by `timeout`.
async fn grpc_probe(
    host: &str,
    port: u16,
    service: &str,
    netns_path: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    use proto::health_check_response::ServingStatus;
    use proto::health_client::HealthClient;
    use proto::HealthCheckRequest;

    debug!("gRPC probe: {}/{service:?}", host_port(host, port));
    let call = async {
        let stream = connect_probe(host, port, netns_path, timeout).await?;
        // tonic dials through its connector; hand it the connection made above,
        // once (a probe is one call on one connection).
        let slot = std::sync::Arc::new(std::sync::Mutex::new(Some(stream)));
        let endpoint = tonic::transport::Endpoint::from_shared(format!(
            "http://{}",
            host_port(host, port)
        ))
        .map_err(|e| format!("grpc endpoint: {e}"))?
        .timeout(timeout);
        let channel = endpoint
            .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                let taken = slot.lock().ok().and_then(|mut s| s.take());
                async move {
                    taken
                        .map(hyper_util::rt::TokioIo::new)
                        .ok_or_else(|| std::io::Error::other("probe connection already used"))
                }
            }))
            .await
            .map_err(|e| format!("grpc connect: {e}"))?;
        let resp = HealthClient::new(channel)
            .check(HealthCheckRequest {
                service: service.to_string(),
            })
            .await
            .map_err(|s| format!("grpc health check: {:?}: {}", s.code(), s.message()))?;
        Ok::<i32, String>(resp.into_inner().status)
    };
    match tokio::time::timeout(timeout, call).await {
        Ok(Ok(status)) if status == ServingStatus::Serving as i32 => ProbeResult::Success,
        Ok(Ok(status)) => {
            let name = ServingStatus::try_from(status)
                .map(|s| s.as_str_name().to_string())
                .unwrap_or_else(|_| status.to_string());
            ProbeResult::Failure(format!("service unhealthy (responded with {name:?})"))
        }
        Ok(Err(e)) => ProbeResult::Failure(e),
        Err(_) => ProbeResult::Failure("grpc health check timed out".into()),
    }
}

/// TCP connect probe — inside the pod netns when known, else from the host.
async fn tcp_probe(
    host: &str,
    port: u16,
    netns_path: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    #[cfg(target_os = "linux")]
    if let Some(ns) = netns_path {
        return dial_in_netns(
            ns.to_string(),
            NetDial::Tcp {
                host: host.to_string(),
                port,
            },
            timeout,
        )
        .await;
    }
    let _ = netns_path;
    tcp_socket_probe(host, port, timeout).await
}

async fn http_get_probe(
    scheme: &str,
    host: &str,
    port: u16,
    path: &str,
    headers: &[(HeaderName, HeaderValue)],
    timeout: Duration,
) -> ProbeResult {
    let url = format!("{scheme}://{}{path}", host_port(host, port));
    debug!("HTTP probe: {url}");

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .danger_accept_invalid_certs(true)
        .build();

    let client = match client {
        Ok(c) => c,
        Err(e) => return ProbeResult::Failure(e.to_string()),
    };

    // Not retried (#211): a probe is asked once per period, and its
    // failureThreshold is the retry. Retrying here would hide a failing
    // container behind the policy's backoff.
    // Appended, so a name given twice is sent twice; a `Host` replaces the
    // URL's (the client sets one only when none is given).
    let mut map = reqwest::header::HeaderMap::new();
    for (n, v) in headers {
        map.append(n.clone(), v.clone());
    }
    match client.get(&url).headers(map).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            if (200..400).contains(&status) {
                ProbeResult::Success
            } else {
                ProbeResult::Failure(format!("HTTP {status}"))
            }
        }
        Err(e) => ProbeResult::Failure(e.to_string()),
    }
}

async fn tcp_socket_probe(host: &str, port: u16, timeout: Duration) -> ProbeResult {
    debug!("TCP probe: {host}:{port}");

    match tokio::time::timeout(timeout, TcpStream::connect(host_port(host, port))).await {
        Ok(Ok(_)) => ProbeResult::Success,
        Ok(Err(e)) => ProbeResult::Failure(format!("TCP connect failed: {e}")),
        Err(_) => ProbeResult::Failure("TCP connect timed out".into()),
    }
}

async fn exec_probe(
    container_id: &str,
    cmd: &[String],
    timeout: i64,
    runtime: &Arc<dyn RuntimeService>,
) -> ProbeResult {
    debug!("Exec probe in {container_id}: {:?}", cmd);

    match runtime.exec_sync(container_id, cmd, timeout).await {
        Ok(result) => {
            if result.exit_code == 0 {
                ProbeResult::Success
            } else {
                ProbeResult::Failure(format!("exec exited with code {}", result.exit_code))
            }
        }
        Err(e) => ProbeResult::Failure(e.to_string()),
    }
}

// ---- pod-netns HTTP/TCP dialing (Linux) ------------------------------------

#[cfg(target_os = "linux")]
enum NetDial {
    Http {
        host: String,
        port: u16,
        path: String,
        headers: Vec<(HeaderName, HeaderValue)>,
    },
    Tcp {
        host: String,
        port: u16,
    },
}

/// Perform a dial inside the pod's network namespace. The `setns(2)` call
/// changes only the calling thread's netns, so we run it on a dedicated OS
/// thread that exits immediately after (never a tokio worker or the blocking
/// pool, which are reused). The result comes back over a oneshot.
#[cfg(target_os = "linux")]
async fn dial_in_netns(netns_path: String, dial: NetDial, timeout: Duration) -> ProbeResult {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let res = enter_netns(&netns_path).and_then(|()| match dial {
            NetDial::Http {
                host,
                port,
                path,
                headers,
            } => http_get_blocking(&host, port, &path, &headers, timeout),
            NetDial::Tcp { host, port } => tcp_connect_blocking(&host, port, timeout),
        });
        let _ = tx.send(res);
    });

    match tokio::time::timeout(timeout + Duration::from_secs(2), rx).await {
        Ok(Ok(Ok(()))) => ProbeResult::Success,
        Ok(Ok(Err(e))) => ProbeResult::Failure(e),
        Ok(Err(_)) => ProbeResult::Failure("probe thread canceled".into()),
        Err(_) => ProbeResult::Failure("probe timed out".into()),
    }
}

/// Move the calling thread into the network namespace at `netns_path`.
/// `setns(2)` changes only this thread's netns: callers run it on a dedicated
/// thread that exits right after.
#[cfg(target_os = "linux")]
fn enter_netns(netns_path: &str) -> Result<(), String> {
    use std::os::fd::AsRawFd;
    let file =
        std::fs::File::open(netns_path).map_err(|e| format!("open netns {netns_path}: {e}"))?;
    // SAFETY: setns only affects this thread, which exits right after.
    let rc = unsafe { libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) };
    if rc != 0 {
        return Err(format!(
            "setns({netns_path}): {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn resolve_one_addr(host: &str, port: u16) -> Result<std::net::SocketAddr, String> {
    use std::net::ToSocketAddrs;
    let addr = host_port(host, port);
    addr.to_socket_addrs()
        .map_err(|e| format!("resolve {addr}: {e}"))?
        .next()
        .ok_or_else(|| format!("no address for {addr}"))
}

#[cfg(target_os = "linux")]
fn tcp_connect_blocking(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let sa = resolve_one_addr(host, port)?;
    std::net::TcpStream::connect_timeout(&sa, timeout)
        .map(|_| ())
        .map_err(|e| format!("tcp connect {host}:{port}: {e}"))
}

/// A plain `http` GET in the pod netns, on the thread that entered it. An
/// `https` probe never comes here: it runs TLS over a connection made in the
/// netns (`https_get_probe`, #452).
#[cfg(target_os = "linux")]
fn http_get_blocking(
    host: &str,
    port: u16,
    path: &str,
    headers: &[(HeaderName, HeaderValue)],
    timeout: Duration,
) -> Result<(), String> {
    use std::io::Write;
    let deadline = std::time::Instant::now() + timeout;
    let sa = resolve_one_addr(host, port)?;
    let mut sock = std::net::TcpStream::connect_timeout(&sa, timeout)
        .map_err(|e| format!("connect {host}:{port}: {e}"))?;
    sock.set_write_timeout(Some(timeout)).ok();
    sock.write_all(&probe_request(host, port, path, headers))
        .map_err(|e| format!("write: {e}"))?;
    let head = read_status_line(&mut sock, deadline)?;
    probe_status(&head)
}

/// The longest status line a probe reads; the https path keeps the same bound.
#[cfg(target_os = "linux")]
const STATUS_LINE_MAX: usize = 512;

/// A response's bytes up to the end of its status line (the first CRLF), EOF,
/// or `STATUS_LINE_MAX` bytes, all before `deadline` (#479). TCP may hand the
/// line over in pieces (`HT`, then the rest), so one read is not the line.
/// Each read waits only the time left, so a server sending a byte at a time
/// cannot hold the probe past its timeout.
#[cfg(target_os = "linux")]
fn read_status_line(
    sock: &mut std::net::TcpStream,
    deadline: std::time::Instant,
) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut head = Vec::new();
    let mut chunk = [0u8; STATUS_LINE_MAX];
    while head.len() < STATUS_LINE_MAX && !head.windows(2).any(|w| w == b"\r\n") {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return Err("read: timed out before the status line ended".into());
        }
        sock.set_read_timeout(Some(left)).map_err(|e| format!("read: {e}"))?;
        let n = match sock.read(&mut chunk[..STATUS_LINE_MAX - head.len()]) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err("read: timed out before the status line ended".into())
            }
            Err(e) => return Err(format!("read: {e}")),
        };
        if n == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..n]);
    }
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_numeric_named_and_string_ports() {
        let spec = json!({
            "ports": [
                {"name": "health", "containerPort": 9234},
                {"name": "metrics", "containerPort": 9963}
            ]
        });
        // numeric
        assert_eq!(resolve_port(&json!(8080), &spec), Some(8080));
        // numeric string
        assert_eq!(resolve_port(&json!("8443"), &spec), Some(8443));
        // named
        assert_eq!(resolve_port(&json!("health"), &spec), Some(9234));
        assert_eq!(resolve_port(&json!("metrics"), &spec), Some(9963));
        // unknown name → None (probe reports an explicit failure instead of
        // silently hitting port 80)
        assert_eq!(resolve_port(&json!("nope"), &spec), None);
    }

    // ---- #443: httpGet headers ----

    /// A one-shot HTTP server on loopback: answers `status` and hands back the
    /// request head it read.
    fn one_shot_http(status: u16) -> (u16, std::sync::mpsc::Receiver<String>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut chunk).unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
            let _ = s.write_all(
                format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            );
        });
        (port, rx)
    }

    /// The request's header lines, lower-cased names, in order.
    fn header_lines(req: &str) -> Vec<(String, String)> {
        req.split("\r\n")
            .skip(1)
            .filter(|l| !l.is_empty())
            .filter_map(|l| l.split_once(':'))
            .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect()
    }

    fn values(req: &str, name: &str) -> Vec<String> {
        header_lines(req).into_iter().filter(|(n, _)| n == name).map(|(_, v)| v).collect()
    }

    fn http_spec() -> Value {
        json!({
            "path": "/healthz",
            "httpHeaders": [
                {"name": "Authorization", "value": "Bearer s3cret"},
                {"name": "Host", "value": "app.example"},
                {"name": "X-Twice", "value": "a"},
                {"name": "X-Twice", "value": "b"}
            ]
        })
    }

    #[test]
    fn probe_headers_keep_the_probes_and_add_upstreams_defaults() {
        let h = probe_headers(&http_spec()).unwrap();
        let pairs: Vec<(String, String)> = h
            .iter()
            .map(|(n, v)| (n.as_str().to_string(), v.to_str().unwrap().to_string()))
            .collect();
        assert_eq!(pairs[0], ("authorization".into(), "Bearer s3cret".into()));
        assert_eq!(pairs[1], ("host".into(), "app.example".into()));
        assert_eq!(pairs[2], ("x-twice".into(), "a".into()));
        assert_eq!(pairs[3], ("x-twice".into(), "b".into()));
        assert!(pairs.contains(&("user-agent".into(), "kube-probe/1.36".into())), "{pairs:?}");
        assert!(pairs.contains(&("accept".into(), "*/*".into())), "{pairs:?}");

        // The probe's own User-Agent and Accept win; nothing is added twice.
        let own = probe_headers(&json!({"httpHeaders": [
            {"name": "user-agent", "value": "mine"},
            {"name": "Accept", "value": "application/json"}
        ]}))
        .unwrap();
        assert_eq!(own.len(), 2, "{own:?}");

        // No httpHeaders: the defaults alone.
        assert_eq!(probe_headers(&json!({})).unwrap().len(), 2);
    }

    #[test]
    fn a_header_that_cannot_be_sent_fails_the_probe_naming_it() {
        let e = probe_headers(&json!({"httpHeaders": [
            {"name": "X-Evil", "value": "a\r\nInjected: 1"}
        ]}))
        .unwrap_err();
        assert!(e.contains("X-Evil"), "{e}");
        let e = probe_headers(&json!({"httpHeaders": [{"name": "Bad Name", "value": "x"}]}))
            .unwrap_err();
        assert!(e.contains("Bad Name"), "{e}");
    }

    #[tokio::test]
    async fn host_path_sends_the_probes_headers_and_its_host() {
        let (port, rx) = one_shot_http(200);
        let h = probe_headers(&http_spec()).unwrap();
        let r = http_probe("http", "127.0.0.1", port, "/healthz", &h, None, Duration::from_secs(5)).await;
        assert_eq!(r, ProbeResult::Success);
        let req = rx.recv().unwrap();
        assert!(req.starts_with("GET /healthz "), "{req}");
        assert_eq!(values(&req, "authorization"), ["Bearer s3cret"], "{req}");
        assert_eq!(values(&req, "host"), ["app.example"], "{req}");
        assert_eq!(values(&req, "x-twice"), ["a", "b"], "{req}");
        assert_eq!(values(&req, "user-agent"), ["kube-probe/1.36"], "{req}");
        assert_eq!(values(&req, "accept"), ["*/*"], "{req}");
    }

    #[tokio::test]
    async fn host_path_without_a_host_header_names_the_target() {
        let (port, rx) = one_shot_http(204);
        let h = probe_headers(&json!({})).unwrap();
        let r = http_probe("http", "127.0.0.1", port, "/", &h, None, Duration::from_secs(5)).await;
        assert_eq!(r, ProbeResult::Success);
        let req = rx.recv().unwrap();
        assert_eq!(values(&req, "host"), [format!("127.0.0.1:{port}")], "{req}");
    }

    #[tokio::test]
    async fn host_path_fails_on_the_status_the_headers_earn() {
        let (port, _rx) = one_shot_http(401);
        let h = probe_headers(&json!({})).unwrap();
        let r = http_probe("http", "127.0.0.1", port, "/", &h, None, Duration::from_secs(5)).await;
        assert_eq!(r, ProbeResult::Failure("HTTP 401".into()));
    }

    /// The pod-netns path's request writer (run here without entering a netns).
    #[cfg(target_os = "linux")]
    #[test]
    fn netns_path_sends_the_probes_headers_and_its_host() {
        let (port, rx) = one_shot_http(200);
        let h = probe_headers(&http_spec()).unwrap();
        http_get_blocking("127.0.0.1", port, "/healthz", &h, Duration::from_secs(5)).unwrap();
        let req = rx.recv().unwrap();
        assert!(req.starts_with("GET /healthz HTTP/1.0\r\n"), "{req}");
        assert_eq!(values(&req, "authorization"), ["Bearer s3cret"], "{req}");
        assert_eq!(values(&req, "host"), ["app.example"], "{req}");
        assert_eq!(values(&req, "x-twice"), ["a", "b"], "{req}");
        assert_eq!(values(&req, "user-agent"), ["kube-probe/1.36"], "{req}");
        assert_eq!(values(&req, "accept"), ["*/*"], "{req}");

        let (port, rx) = one_shot_http(200);
        http_get_blocking("127.0.0.1", port, "/", &probe_headers(&json!({})).unwrap(), Duration::from_secs(5))
            .unwrap();
        let req = rx.recv().unwrap();
        assert_eq!(values(&req, "host"), [format!("127.0.0.1:{port}")], "{req}");
    }

    /// A one-shot server on loopback that reads the request head, then sends
    /// each of `parts` as its own TCP segment, `gap` apart, and keeps the
    /// connection open `hold` after the last.
    #[cfg(target_os = "linux")]
    fn split_http(parts: Vec<&'static [u8]>, gap: Duration, hold: Duration) -> u16 {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.set_nodelay(true).unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut chunk).unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    std::thread::sleep(gap);
                }
                if s.write_all(p).is_err() {
                    return;
                }
            }
            std::thread::sleep(hold);
        });
        port
    }

    /// #479 (e353605dcd69): the status line in two writes is still one line.
    #[cfg(target_os = "linux")]
    #[test]
    fn netns_path_reads_a_status_line_sent_in_pieces() {
        let h = probe_headers(&json!({})).unwrap();
        let port = split_http(
            vec![b"HT", b"TP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"],
            Duration::from_millis(200),
            Duration::ZERO,
        );
        assert_eq!(http_get_blocking("127.0.0.1", port, "/", &h, Duration::from_secs(5)), Ok(()));

        // The code comes in the second piece, and decides.
        let port = split_http(
            vec![b"HTTP/1.1 5", b"03 Unavailable\r\n\r\n"],
            Duration::from_millis(200),
            Duration::ZERO,
        );
        assert_eq!(
            http_get_blocking("127.0.0.1", port, "/", &h, Duration::from_secs(5)),
            Err("HTTP 503".into())
        );
    }

    /// #479: a status line that never ends fails at the probe's timeout, not later.
    #[cfg(target_os = "linux")]
    #[test]
    fn netns_path_fails_when_the_status_line_does_not_end_in_time() {
        let h = probe_headers(&json!({})).unwrap();
        // A byte every 300 ms: each read returns before its own timeout, so only
        // the probe's deadline stops it.
        let port = split_http(
            vec![b"H", b"T", b"T", b"P", b"/", b"1", b".", b"1"],
            Duration::from_millis(300),
            Duration::from_secs(5),
        );
        let start = std::time::Instant::now();
        let r = http_get_blocking("127.0.0.1", port, "/", &h, Duration::from_secs(1));
        let took = start.elapsed();
        assert_eq!(r, Err("read: timed out before the status line ended".into()));
        assert!(took < Duration::from_millis(1800), "took {took:?}");
    }

    /// #479: a response that ends (EOF) after its status line without a CRLF
    /// is read as it is.
    #[cfg(target_os = "linux")]
    #[test]
    fn netns_path_takes_the_status_line_at_eof() {
        let h = probe_headers(&json!({})).unwrap();
        let port = split_http(vec![b"HTTP/1.0 ", b"204"], Duration::from_millis(100), Duration::ZERO);
        assert_eq!(http_get_blocking("127.0.0.1", port, "/", &h, Duration::from_secs(5)), Ok(()));
    }

    // ---- #452: https in the pod netns ----

    /// A TLS server on loopback with a self-signed certificate (nothing the
    /// probe could verify): answers `status` and keeps the request headers.
    async fn tls_http(status: u16) -> (u16, std::sync::Arc<std::sync::Mutex<Vec<axum::http::HeaderMap>>>) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let key = rcgen::generate_simple_self_signed(vec!["probe.invalid".to_string()]).unwrap();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
            let log = log.clone();
            async move {
                log.lock().unwrap().push(headers);
                axum::http::StatusCode::from_u16(status).unwrap()
            }
        });
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
            key.cert.pem().into_bytes(),
            key.key_pair.serialize_pem().into_bytes(),
        )
        .await
        .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(axum_server::from_tcp_rustls(listener, tls).serve(app.into_make_service()));
        (port, seen)
    }

    fn all(h: &axum::http::HeaderMap, name: &str) -> Vec<String> {
        h.get_all(name).iter().map(|v| v.to_str().unwrap().to_string()).collect()
    }

    /// The netns route's TLS half, over a connection `connect_probe` made (here
    /// without a netns: entering one needs root). A 200 passes and the probe's
    /// headers arrive; the certificate is not one anything could verify.
    #[tokio::test]
    async fn https_probe_does_tls_sends_the_headers_and_passes_on_200() {
        let (port, seen) = tls_http(200).await;
        let h = probe_headers(&http_spec()).unwrap();
        let r = https_get_probe("127.0.0.1", port, "/healthz", &h, None, Duration::from_secs(5)).await;
        assert_eq!(r, ProbeResult::Success);
        let got = seen.lock().unwrap();
        assert_eq!(got.len(), 1);
        let req = &got[0];
        assert_eq!(all(req, "authorization"), ["Bearer s3cret"]);
        assert_eq!(all(req, "host"), ["app.example"]);
        assert_eq!(all(req, "x-twice"), ["a", "b"]);
        assert_eq!(all(req, "user-agent"), ["kube-probe/1.36"]);
        assert_eq!(all(req, "accept"), ["*/*"]);
    }

    #[tokio::test]
    async fn https_probe_fails_on_a_500() {
        let (port, seen) = tls_http(500).await;
        let h = probe_headers(&json!({})).unwrap();
        let r = https_get_probe("127.0.0.1", port, "/", &h, None, Duration::from_secs(5)).await;
        assert_eq!(r, ProbeResult::Failure("HTTP 500".into()));
        assert_eq!(seen.lock().unwrap().len(), 1, "the request reached the server");
    }

    /// A port with no TLS on it fails (it passed as a connect, #452).
    #[tokio::test]
    async fn https_probe_fails_on_a_port_without_tls() {
        // A plain HTTP server that answers on accept: one that first waited for
        // a request would wait on the ClientHello as the client waits on it.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            use std::io::Write;
            if let Ok((mut s, _)) = listener.accept() {
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            }
        });
        let h = probe_headers(&json!({})).unwrap();
        let r = https_get_probe("127.0.0.1", port, "/", &h, None, Duration::from_secs(5)).await;
        match r {
            ProbeResult::Failure(e) => assert!(e.starts_with("tls handshake"), "{e}"),
            other => panic!("passed without TLS: {other:?}"),
        }
    }

    /// `http_probe` with a netns sends `https` to the TLS route, not to a
    /// connect: with a netns path that does not exist the connect itself fails
    /// naming it (the route a netns probe takes, short of entering one).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn an_https_probe_with_a_netns_takes_the_tls_route() {
        let h = probe_headers(&json!({})).unwrap();
        let r = http_probe("https", "127.0.0.1", 1, "/", &h, Some("/nonexistent/netns"), Duration::from_secs(2)).await;
        match r {
            ProbeResult::Failure(e) => assert!(e.contains("open netns /nonexistent/netns"), "{e}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn probe_status_reads_the_status_line() {
        assert_eq!(probe_status(b"HTTP/1.1 302 Found\r\n"), Ok(()));
        assert_eq!(probe_status(b"HTTP/1.0 404 Not Found\r\n"), Err("HTTP 404".into()));
        assert_eq!(probe_status(b""), Err("empty response".into()));
        assert!(probe_status(b"\x16\x03\x01garbage").is_err());
    }

    // ---- #443: gRPC health ----

    use proto::health_check_response::ServingStatus;
    use proto::health_server::{Health, HealthServer};
    use proto::{HealthCheckRequest, HealthCheckResponse};

    /// A health service answering `status` for the service named `serves`, and
    /// NOT_FOUND for any other, as grpc-go's health server does.
    struct FakeHealth {
        serves: &'static str,
        status: Result<ServingStatus, tonic::Code>,
    }

    #[tonic::async_trait]
    impl Health for FakeHealth {
        async fn check(
            &self,
            req: tonic::Request<HealthCheckRequest>,
        ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
            if req.get_ref().service != self.serves {
                return Err(tonic::Status::not_found("unknown service"));
            }
            match self.status {
                Ok(s) => Ok(tonic::Response::new(HealthCheckResponse { status: s as i32 })),
                Err(code) => Err(tonic::Status::new(code, "fake")),
            }
        }

        type WatchStream = tokio_stream::Empty<Result<HealthCheckResponse, tonic::Status>>;

        async fn watch(
            &self,
            _: tonic::Request<HealthCheckRequest>,
        ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
            Ok(tonic::Response::new(tokio_stream::empty()))
        }
    }

    async fn serve_health(h: FakeHealth) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(HealthServer::new(h))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        port
    }

    fn grpc_spec(port: u16, service: Option<&str>) -> Value {
        let mut g = json!({"port": port});
        if let Some(s) = service {
            g["service"] = json!(s);
        }
        json!({"grpc": g, "timeoutSeconds": 3})
    }

    async fn run_grpc(spec: &Value) -> ProbeResult {
        // A gRPC probe never asks the runtime; a lazy client to no socket.
        let rt: Arc<dyn RuntimeService> = Arc::new(crate::cri_grpc::CriGrpcClient::new("/nonexistent/cri.sock"));
        run_probe(spec, &json!({}), "c", "127.0.0.1", None, &rt).await
    }

    #[tokio::test]
    async fn grpc_probe_passes_only_on_serving() {
        let port = serve_health(FakeHealth { serves: "", status: Ok(ServingStatus::Serving) }).await;
        assert_eq!(run_grpc(&grpc_spec(port, None)).await, ProbeResult::Success);

        let port = serve_health(FakeHealth { serves: "", status: Ok(ServingStatus::NotServing) }).await;
        match run_grpc(&grpc_spec(port, None)).await {
            ProbeResult::Failure(m) => assert!(m.contains("NOT_SERVING"), "{m}"),
            r => panic!("NOT_SERVING passed: {r:?}"),
        }

        let port = serve_health(FakeHealth { serves: "", status: Ok(ServingStatus::Unknown) }).await;
        assert!(matches!(run_grpc(&grpc_spec(port, None)).await, ProbeResult::Failure(_)));
    }

    #[tokio::test]
    async fn grpc_probe_asks_for_its_service() {
        let port = serve_health(FakeHealth { serves: "app.v1", status: Ok(ServingStatus::Serving) }).await;
        assert_eq!(run_grpc(&grpc_spec(port, Some("app.v1"))).await, ProbeResult::Success);
        // The empty (whole-server) service is not what this server serves.
        match run_grpc(&grpc_spec(port, None)).await {
            ProbeResult::Failure(m) => assert!(m.contains("NotFound"), "{m}"),
            r => panic!("wrong service passed: {r:?}"),
        }
    }

    #[tokio::test]
    async fn grpc_probe_fails_on_an_rpc_error() {
        let port = serve_health(FakeHealth { serves: "", status: Err(tonic::Code::Unimplemented) }).await;
        match run_grpc(&grpc_spec(port, None)).await {
            ProbeResult::Failure(m) => assert!(m.contains("Unimplemented"), "{m}"),
            r => panic!("unimplemented health passed: {r:?}"),
        }
    }

    #[tokio::test]
    async fn grpc_probe_fails_on_a_listener_that_is_not_grpc() {
        // Accepts and answers HTTP/1.1: a TCP connect alone would have passed.
        let (port, _rx) = one_shot_http(200);
        assert!(matches!(run_grpc(&grpc_spec(port, None)).await, ProbeResult::Failure(_)));
        // Nothing listening.
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        assert!(matches!(run_grpc(&grpc_spec(free, None)).await, ProbeResult::Failure(_)));
    }
}
