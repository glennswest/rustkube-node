//! Health probes — liveness, readiness, startup.
//!
//! Implements HTTP GET, TCP connect, gRPC, and exec probes.

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

/// Run a health probe based on the probe spec.
pub async fn run_probe(
    probe_spec: &Value,
    container_id: &str,
    pod_ip: &str,
    runtime: &Arc<dyn RuntimeService>,
) -> ProbeResult {
    let timeout_secs = probe_spec["timeoutSeconds"].as_u64().unwrap_or(1);
    let timeout = Duration::from_secs(timeout_secs);

    // HTTP GET probe
    if !probe_spec["httpGet"].is_null() {
        let http = &probe_spec["httpGet"];
        let port = http["port"].as_u64().unwrap_or(80) as u16;
        let path = http["path"].as_str().unwrap_or("/");
        let scheme = http["scheme"].as_str().unwrap_or("HTTP").to_lowercase();
        let host = http["host"].as_str().unwrap_or(pod_ip);

        return http_get_probe(&scheme, host, port, path, timeout).await;
    }

    // TCP socket probe
    if !probe_spec["tcpSocket"].is_null() {
        let port = probe_spec["tcpSocket"]["port"].as_u64().unwrap_or(0) as u16;
        let host = probe_spec["tcpSocket"]["host"]
            .as_str()
            .unwrap_or(pod_ip);

        return tcp_socket_probe(host, port, timeout).await;
    }

    // Exec probe
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

    // gRPC probe
    if !probe_spec["grpc"].is_null() {
        let port = probe_spec["grpc"]["port"].as_u64().unwrap_or(0) as u16;
        // Simplified — just do a TCP connect for now
        return tcp_socket_probe(pod_ip, port, timeout).await;
    }

    ProbeResult::Success // No probe configured = always succeeds
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
