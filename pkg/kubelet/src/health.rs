//! Health probes — liveness, readiness, startup.
//!
//! Implements HTTP GET, TCP connect, gRPC, and exec probes. HTTP/TCP probes are
//! run **inside the pod's network namespace** when the runtime reports it, so a
//! probe targeting `127.0.0.1` (or any loopback-bound health server, e.g.
//! cilium-operator's `localhost:9234`) actually reaches the container rather
//! than the host. Named ports (`port: "health"`) are resolved from the
//! container's `ports[]`. (rustkube-node#15)

use crate::cri::RuntimeService;
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
        return http_probe(&scheme, host, port, path, netns_path, timeout).await;
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

    // gRPC probe — simplified to a TCP connect (in the pod netns when available)
    if !probe_spec["grpc"].is_null() {
        let port = match resolve_port(&probe_spec["grpc"]["port"], container_spec) {
            Some(p) => p,
            None => return ProbeResult::Failure("grpc: could not resolve port".into()),
        };
        return tcp_probe(pod_ip, port, netns_path, timeout).await;
    }

    ProbeResult::Success // No probe configured = always succeeds
}

/// HTTP GET probe — inside the pod netns when known, else from the host.
async fn http_probe(
    scheme: &str,
    host: &str,
    port: u16,
    path: &str,
    netns_path: Option<&str>,
    timeout: Duration,
) -> ProbeResult {
    #[cfg(target_os = "linux")]
    if let Some(ns) = netns_path {
        return dial_in_netns(
            ns.to_string(),
            NetDial::Http {
                scheme: scheme.to_string(),
                host: host.to_string(),
                port,
                path: path.to_string(),
            },
            timeout,
        )
        .await;
    }
    let _ = netns_path; // only consulted on Linux
    http_get_probe(scheme, host, port, path, timeout).await
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
    timeout: Duration,
) -> ProbeResult {
    let url = format!("{scheme}://{host}:{port}{path}");
    debug!("HTTP probe: {url}");

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .danger_accept_invalid_certs(true)
        .build();

    let client = match client {
        Ok(c) => c,
        Err(e) => return ProbeResult::Failure(e.to_string()),
    };

    match client.get(&url).send().await {
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

    match tokio::time::timeout(timeout, TcpStream::connect(format!("{host}:{port}"))).await {
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
        scheme: String,
        host: String,
        port: u16,
        path: String,
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
        use std::os::fd::AsRawFd;
        let res = (|| -> Result<(), String> {
            let file = std::fs::File::open(&netns_path)
                .map_err(|e| format!("open netns {netns_path}: {e}"))?;
            // SAFETY: setns only affects this thread, which exits right after.
            let rc = unsafe { libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) };
            if rc != 0 {
                return Err(format!(
                    "setns({netns_path}): {}",
                    std::io::Error::last_os_error()
                ));
            }
            match dial {
                NetDial::Http {
                    scheme,
                    host,
                    port,
                    path,
                } => http_get_blocking(&scheme, &host, port, &path, timeout),
                NetDial::Tcp { host, port } => tcp_connect_blocking(&host, port, timeout),
            }
        })();
        let _ = tx.send(res);
    });

    match tokio::time::timeout(timeout + Duration::from_secs(2), rx).await {
        Ok(Ok(Ok(()))) => ProbeResult::Success,
        Ok(Ok(Err(e))) => ProbeResult::Failure(e),
        Ok(Err(_)) => ProbeResult::Failure("probe thread canceled".into()),
        Err(_) => ProbeResult::Failure("probe timed out".into()),
    }
}

#[cfg(target_os = "linux")]
fn resolve_one_addr(host: &str, port: u16) -> Result<std::net::SocketAddr, String> {
    use std::net::ToSocketAddrs;
    let addr = format!("{host}:{port}");
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

#[cfg(target_os = "linux")]
fn http_get_blocking(
    scheme: &str,
    host: &str,
    port: u16,
    path: &str,
    timeout: Duration,
) -> Result<(), String> {
    // No in-netns TLS client; for HTTPS a successful connect is the best signal.
    if scheme == "https" {
        return tcp_connect_blocking(host, port, timeout);
    }
    use std::io::{Read, Write};
    let sa = resolve_one_addr(host, port)?;
    let mut sock = std::net::TcpStream::connect_timeout(&sa, timeout)
        .map_err(|e| format!("connect {host}:{port}: {e}"))?;
    sock.set_read_timeout(Some(timeout)).ok();
    sock.set_write_timeout(Some(timeout)).ok();
    let path = if path.is_empty() { "/" } else { path };
    let req = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    sock.write_all(req.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    let mut head = [0u8; 512];
    let n = sock.read(&mut head).map_err(|e| format!("read: {e}"))?;
    if n == 0 {
        return Err("empty response".into());
    }
    let text = String::from_utf8_lossy(&head[..n]);
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
}
