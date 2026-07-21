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

    /// Use a specific (e.g. authenticated HTTPS) client for apiserver calls.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
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
            // Fresh node: no existing conditions to preserve.
            "status": self.build_status(&[])
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

    /// GET the node's current status conditions, so a heartbeat can merge its
    /// own conditions in without wiping ones set by other components
    /// (rustkube-node#29). Returns an empty list if the node/status can't be
    /// read — a heartbeat then just re-asserts the kubelet-owned conditions.
    async fn current_conditions(&self) -> Vec<Value> {
        let url = format!("{}/api/v1/nodes/{}", self.api_url, self.node_name);
        match self.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp
                .json::<Value>()
                .await
                .ok()
                .and_then(|node| node["status"]["conditions"].as_array().cloned())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// PUT the node status via the `/status` subresource (preserves metadata/spec).
    async fn update_node_status(&self) -> anyhow::Result<()> {
        let existing = self.current_conditions().await;
        let node_update = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": &self.node_name },
            "status": self.build_status(&existing)
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

    /// Build the node status, merging the kubelet-owned conditions into
    /// `existing` (the conditions currently on the object) so third-party
    /// conditions — Cilium's `NetworkUnavailable`, NPD/operator conditions — are
    /// preserved rather than clobbered on every heartbeat (rustkube-node#29).
    fn build_status(&self, existing: &[Value]) -> Value {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Real system + filesystem stats (no longer faked).
        let (total_mem_ki, cpu_count) = get_system_resources();
        let (fs_total, fs_avail) = ephemeral_fs_stats().unwrap_or((0, 0));
        // Available memory drives MemoryPressure; fall back to "plenty" if unknown.
        let avail_mem_ki = available_memory_ki().unwrap_or(total_mem_ki);

        // Eviction-style pressure signals (kubelet defaults: memory.available<100Mi,
        // nodefs.available<10%). PIDPressure only when very few PIDs remain.
        let mem_pressure = avail_mem_ki < 100 * 1024;
        let disk_pressure = fs_total > 0 && (fs_avail as f64 / fs_total as f64) < 0.10;
        let pid_pressure = pid_stats()
            .map(|(used, max)| max.saturating_sub(used) < 2000)
            .unwrap_or(false);

        // ephemeral-storage capacity from the container/pod-storage filesystem;
        // allocatable reserves the 10% eviction headroom.
        let eph_cap_ki = fs_total / 1024;
        let eph_alloc_ki = (fs_total / 1024) * 9 / 10;

        json!({
            "capacity": {
                "cpu": cpu_count.to_string(),
                "memory": format!("{total_mem_ki}Ki"),
                "pods": "110",
                "ephemeral-storage": format!("{eph_cap_ki}Ki")
            },
            "allocatable": {
                "cpu": cpu_count.to_string(),
                "memory": format!("{}Ki", total_mem_ki.saturating_sub(256 * 1024)),
                "pods": "110",
                "ephemeral-storage": format!("{eph_alloc_ki}Ki")
            },
            "conditions": merge_owned_conditions(
                existing, mem_pressure, disk_pressure, pid_pressure, &now,
            ),
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

/// The condition types the kubelet owns and refreshes each heartbeat. Every
/// other condition on the node (e.g. Cilium's `NetworkUnavailable`, or custom
/// operator/NPD conditions) is left untouched.
const KUBELET_OWNED_CONDITIONS: [&str; 4] =
    ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure"];

/// Merge the kubelet-owned conditions into `existing`, keyed by `type`
/// (rustkube-node#29):
///   * foreign conditions (not in `KUBELET_OWNED_CONDITIONS`) are carried
///     through verbatim, so a heartbeat never clobbers them;
///   * each owned condition's `lastTransitionTime` is carried forward from the
///     previous value when its `status` is unchanged, and only stamped `now` on
///     an actual flip — `lastHeartbeatTime` always advances.
fn merge_owned_conditions(
    existing: &[Value],
    mem_pressure: bool,
    disk_pressure: bool,
    pid_pressure: bool,
    now: &str,
) -> Value {
    // (type, status, reason, message) for the kubelet-owned conditions.
    let owned = [
        ("Ready", "True", "KubeletReady", "rustkube kubelet is ready"),
        (
            "MemoryPressure",
            if mem_pressure { "True" } else { "False" },
            if mem_pressure { "KubeletHasInsufficientMemory" } else { "KubeletHasSufficientMemory" },
            "",
        ),
        (
            "DiskPressure",
            if disk_pressure { "True" } else { "False" },
            if disk_pressure { "KubeletHasDiskPressure" } else { "KubeletHasNoDiskPressure" },
            "",
        ),
        (
            "PIDPressure",
            if pid_pressure { "True" } else { "False" },
            if pid_pressure { "KubeletHasInsufficientPID" } else { "KubeletHasSufficientPID" },
            "",
        ),
    ];

    // Preserve every condition the kubelet does not own.
    let mut out: Vec<Value> = existing
        .iter()
        .filter(|c| !KUBELET_OWNED_CONDITIONS.contains(&c["type"].as_str().unwrap_or("")))
        .cloned()
        .collect();

    for (typ, status, reason, message) in owned {
        let prev = existing.iter().find(|c| c["type"] == typ);
        // Keep the prior transition time unless the status actually changed.
        let transition = match prev {
            Some(p) if p["status"].as_str() == Some(status) => {
                p["lastTransitionTime"].as_str().unwrap_or(now).to_string()
            }
            _ => now.to_string(),
        };
        let mut cond = json!({
            "type": typ,
            "status": status,
            "reason": reason,
            "lastHeartbeatTime": now,
            "lastTransitionTime": transition,
        });
        if !message.is_empty() {
            cond["message"] = json!(message);
        }
        out.push(cond);
    }
    Value::Array(out)
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

/// (total, available) bytes of the filesystem backing container/pod storage —
/// used for `ephemeral-storage` capacity and DiskPressure. Tries the CRI-O /
/// kubelet storage roots, then falls back to `/`.
pub(crate) fn ephemeral_fs_stats() -> Option<(u64, u64)> {
    for p in ["/var/lib/containers", "/var/lib/kubelet", "/var", "/"] {
        if std::path::Path::new(p).exists() {
            if let Some(s) = statvfs_bytes(p) {
                return Some(s);
            }
        }
    }
    None
}

/// Currently-available memory in KiB (MemAvailable), for MemoryPressure.
#[cfg(target_os = "linux")]
fn available_memory_ki() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn available_memory_ki() -> Option<u64> {
    None
}

/// (used, max) process IDs, for PIDPressure.
#[cfg(target_os = "linux")]
fn pid_stats() -> Option<(u64, u64)> {
    let max: u64 = std::fs::read_to_string("/proc/sys/kernel/pid_max")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let used = std::fs::read_dir("/proc")
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false)
        })
        .count() as u64;
    Some((used, max))
}

#[cfg(not(target_os = "linux"))]
fn pid_stats() -> Option<(u64, u64)> {
    None
}

/// (total, available) bytes of the filesystem containing `path`, via statvfs(3).
#[cfg(target_os = "linux")]
fn statvfs_bytes(path: &str) -> Option<(u64, u64)> {
    use std::ffi::CString;
    let c = CString::new(path).ok()?;
    // SAFETY: statvfs fills a zeroed buffer; the return code is checked.
    let mut buf: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut buf) } != 0 {
        return None;
    }
    let frsize = buf.f_frsize as u64;
    Some((buf.f_blocks as u64 * frsize, buf.f_bavail as u64 * frsize))
}

#[cfg(not(target_os = "linux"))]
fn statvfs_bytes(_path: &str) -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types(v: &Value) -> Vec<String> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|c| c["type"].as_str().unwrap().to_string())
            .collect()
    }
    fn cond<'a>(v: &'a Value, t: &str) -> &'a Value {
        v.as_array().unwrap().iter().find(|c| c["type"] == t).unwrap()
    }

    #[test]
    fn preserves_foreign_conditions() {
        // The Cilium case: NetworkUnavailable must survive a heartbeat.
        let existing = json!([
            {"type": "NetworkUnavailable", "status": "False", "reason": "CiliumIsUp",
             "lastHeartbeatTime": "t0", "lastTransitionTime": "t0"}
        ]);
        let merged = merge_owned_conditions(existing.as_array().unwrap(), false, false, false, "t1");
        let ts = types(&merged);
        assert!(ts.contains(&"NetworkUnavailable".to_string()));
        for owned in ["Ready", "MemoryPressure", "DiskPressure", "PIDPressure"] {
            assert!(ts.contains(&owned.to_string()), "missing {owned}");
        }
        // Foreign condition carried through untouched.
        assert_eq!(cond(&merged, "NetworkUnavailable")["reason"], "CiliumIsUp");
        assert_eq!(cond(&merged, "NetworkUnavailable")["lastTransitionTime"], "t0");
        assert_eq!(cond(&merged, "Ready")["status"], "True");
    }

    #[test]
    fn carries_transition_time_when_status_unchanged() {
        // Ready was already True at t0; a heartbeat at t1 keeps the transition
        // time but advances the heartbeat time.
        let existing = json!([
            {"type": "Ready", "status": "True", "lastHeartbeatTime": "t0", "lastTransitionTime": "t0"}
        ]);
        let merged = merge_owned_conditions(existing.as_array().unwrap(), false, false, false, "t1");
        let ready = cond(&merged, "Ready");
        assert_eq!(ready["lastTransitionTime"], "t0", "transition time must not churn");
        assert_eq!(ready["lastHeartbeatTime"], "t1", "heartbeat time must advance");
    }

    #[test]
    fn stamps_transition_time_on_flip() {
        // DiskPressure flips False -> True: transition time updates to now.
        let existing = json!([
            {"type": "DiskPressure", "status": "False", "lastHeartbeatTime": "t0", "lastTransitionTime": "t0"}
        ]);
        let merged = merge_owned_conditions(existing.as_array().unwrap(), false, true, false, "t1");
        let dp = cond(&merged, "DiskPressure");
        assert_eq!(dp["status"], "True");
        assert_eq!(dp["lastTransitionTime"], "t1", "flip must stamp now");
    }

    #[test]
    fn fresh_node_gets_all_owned_conditions() {
        let merged = merge_owned_conditions(&[], false, false, false, "t1");
        assert_eq!(types(&merged).len(), 4);
        assert_eq!(cond(&merged, "Ready")["lastTransitionTime"], "t1");
    }
}
