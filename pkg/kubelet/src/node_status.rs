//! Node status reporting.
//!
//! Reports node capacity, allocatable resources, and conditions.
//! Sends heartbeats via Lease objects in kube-node-lease.

use serde_json::{json, Value};
use tracing::info;

/// API client for node status reporting.
pub struct NodeReporter {
    api_url: String,
    node_name: String,
    /// Pod CIDR assigned to this node, written to `spec.podCIDR` when set.
    pod_cidr: Option<String>,
    /// Container runtime version string (e.g. `cri-o://1.32.0`) for nodeInfo.
    runtime_version: String,
    /// Kubelet server port, reported in daemonEndpoints.kubeletEndpoint.
    kubelet_port: u16,
    client: reqwest::Client,
}

impl NodeReporter {
    pub fn new(api_url: &str, node_name: &str) -> Self {
        Self::with_pod_cidr(api_url, node_name, None)
    }

    pub fn with_pod_cidr(api_url: &str, node_name: &str, pod_cidr: Option<String>) -> Self {
        Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            node_name: node_name.to_string(),
            pod_cidr,
            runtime_version: "cri-o://unknown".to_string(),
            kubelet_port: 10250,
            client: reqwest::Client::new(),
        }
    }

    /// Set the container runtime version reported in nodeInfo.
    pub fn with_runtime_version(mut self, v: String) -> Self {
        if !v.is_empty() {
            self.runtime_version = v;
        }
        self
    }

    /// Set the kubelet server port reported in daemonEndpoints.
    pub fn with_kubelet_port(mut self, port: u16) -> Self {
        self.kubelet_port = port;
        self
    }

    /// The Node object metadata (name + labels) reported to the API server.
    fn node_metadata(&self) -> Value {
        json!({
            "name": &self.node_name,
            "labels": {
                "kubernetes.io/hostname": &self.node_name,
                "kubernetes.io/os": go_os(),
                "kubernetes.io/arch": go_arch(),
                "node.kubernetes.io/instance-type": "rustkube"
            }
        })
    }

    /// The Node spec (podCIDR/podCIDRs when configured).
    fn node_spec(&self) -> Value {
        match &self.pod_cidr {
            Some(cidr) => json!({ "podCIDR": cidr, "podCIDRs": [cidr] }),
            None => json!({}),
        }
    }

    /// Register this node with the API server. Creates the Node via POST; if it
    /// already exists (409) the status subresource is updated so the node still
    /// reports Ready on a kubelet restart.
    pub async fn register(&self) -> anyhow::Result<()> {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": self.node_metadata(),
            "spec": self.node_spec(),
            "status": self.build_status()
        });

        let resp = self
            .client
            .post(format!("{}/api/v1/nodes", self.api_url))
            .json(&node)
            .send()
            .await?;

        let code = resp.status().as_u16();
        if resp.status().is_success() {
            info!("Node {} registered", self.node_name);
            Ok(())
        } else if code == 409 {
            // Already exists — refresh status so we report Ready immediately.
            info!("Node {} already exists, updating status", self.node_name);
            self.update_node_status().await?;
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to register node: {body}");
        }
    }

    /// PUT the node status via the `/status` subresource (preserves metadata/spec).
    async fn update_node_status(&self) -> anyhow::Result<()> {
        let node_update = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": &self.node_name },
            "status": self.build_status()
        });

        let resp = self
            .client
            .put(format!("{}/api/v1/nodes/{}/status", self.api_url, self.node_name))
            .json(&node_update)
            .send()
            .await?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to update node status: {body}");
        }
        Ok(())
    }

    /// Send a heartbeat via Lease object.
    pub async fn heartbeat(&self) -> anyhow::Result<()> {
        let now = chrono::Utc::now();
        let lease = json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": &self.node_name,
                "namespace": "kube-node-lease"
            },
            "spec": {
                "holderIdentity": &self.node_name,
                "leaseDurationSeconds": 40,
                "renewTime": now.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
                "acquireTime": now.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
            }
        });

        // Try update first, then create
        let path = format!(
            "{}/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases/{}",
            self.api_url, self.node_name
        );

        let resp = self.client.put(&path).json(&lease).send().await;
        match resp {
            Ok(r) if r.status().is_success() => {}
            _ => {
                // Create it
                let create_path = format!(
                    "{}/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases",
                    self.api_url
                );
                let _ = self.client.post(&create_path).json(&lease).send().await;
            }
        }

        // Refresh node status conditions via the /status subresource so that
        // metadata (labels) and spec (podCIDR) are preserved across heartbeats.
        if let Err(e) = self.update_node_status().await {
            tracing::warn!("Heartbeat: node status update failed: {e}");
        }

        Ok(())
    }

    fn build_status(&self) -> Value {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Get system info
        let (total_mem_ki, cpu_count) = get_system_resources();

        json!({
            "capacity": {
                "cpu": cpu_count.to_string(),
                "memory": format!("{total_mem_ki}Ki"),
                "pods": "110",
                "ephemeral-storage": "50Gi"
            },
            "allocatable": {
                "cpu": cpu_count.to_string(),
                "memory": format!("{}Ki", total_mem_ki.saturating_sub(256 * 1024)),
                "pods": "110",
                "ephemeral-storage": "45Gi"
            },
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "rustkube kubelet is ready",
                    "lastHeartbeatTime": &now,
                    "lastTransitionTime": &now
                },
                {
                    "type": "MemoryPressure",
                    "status": "False",
                    "reason": "KubeletHasSufficientMemory",
                    "lastHeartbeatTime": &now,
                    "lastTransitionTime": &now
                },
                {
                    "type": "DiskPressure",
                    "status": "False",
                    "reason": "KubeletHasNoDiskPressure",
                    "lastHeartbeatTime": &now,
                    "lastTransitionTime": &now
                },
                {
                    "type": "PIDPressure",
                    "status": "False",
                    "reason": "KubeletHasSufficientPID",
                    "lastHeartbeatTime": &now,
                    "lastTransitionTime": &now
                }
            ],
            "nodeInfo": {
                "machineID": "",
                "systemUUID": "",
                "bootID": "",
                "kernelVersion": "",
                "osImage": format!("rustkube ({} {})", std::env::consts::OS, std::env::consts::ARCH),
                "containerRuntimeVersion": &self.runtime_version,
                "kubeletVersion": format!("v1.32.0-rustkube+{}", apimachinery::VERSION),
                "kubeProxyVersion": format!("v1.32.0-rustkube+{}", apimachinery::VERSION),
                "operatingSystem": go_os(),
                "architecture": go_arch()
            },
            "daemonEndpoints": {
                "kubeletEndpoint": { "Port": self.kubelet_port }
            },
            "addresses": build_addresses(&self.node_name)
        })
    }
}

/// Map Rust's `std::env::consts::ARCH` to the Go `GOARCH` string Kubernetes uses.
pub fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "arm" => "arm",
        "powerpc64" => "ppc64le",
        "s390x" => "s390x",
        "riscv64" => "riscv64",
        other => other,
    }
}

/// Map Rust's `std::env::consts::OS` to the Go `GOOS` string Kubernetes uses.
pub fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

fn build_addresses(node_name: &str) -> Value {
    let mut addresses = vec![json!({
        "type": "Hostname",
        "address": node_name
    })];
    if let Some(ip) = detect_node_ip() {
        addresses.insert(0, json!({
            "type": "InternalIP",
            "address": ip.to_string()
        }));
    }
    Value::Array(addresses)
}

/// Detect this node's primary IP: the local address a UDP socket would use
/// to reach the outside world. No packet is actually sent.
pub fn detect_node_ip() -> Option<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:53").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    if ip.is_unspecified() {
        None
    } else {
        Some(ip)
    }
}

/// Get system total memory in KiB and CPU count.
fn get_system_resources() -> (u64, u64) {
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1);

    let total_mem_ki = detect_total_memory_ki().unwrap_or(8 * 1024 * 1024); // 8Gi fallback

    (total_mem_ki, cpu_count)
}

/// Total physical memory in KiB.
#[cfg(target_os = "linux")]
fn detect_total_memory_ki() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn detect_total_memory_ki() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(bytes / 1024)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_total_memory_ki() -> Option<u64> {
    None
}
