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
    client: reqwest::Client,
}

impl NodeReporter {
    pub fn new(api_url: &str, node_name: &str) -> Self {
        Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            node_name: node_name.to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Register this node with the API server.
    pub async fn register(&self) -> anyhow::Result<()> {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": &self.node_name,
                "labels": {
                    "kubernetes.io/hostname": &self.node_name,
                    "kubernetes.io/os": std::env::consts::OS,
                    "kubernetes.io/arch": std::env::consts::ARCH,
                    "node.kubernetes.io/instance-type": "rustkube"
                }
            },
            "spec": {},
            "status": self.build_status()
        });

        let resp = self
            .client
            .post(format!("{}/api/v1/nodes", self.api_url))
            .json(&node)
            .send()
            .await?;

        if resp.status().is_success() || resp.status().as_u16() == 409 {
            info!("Node {} registered", self.node_name);
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Failed to register node: {body}");
        }
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

        // Update node status
        let status = self.build_status();
        let node_update = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": &self.node_name
            },
            "status": status
        });

        let _ = self
            .client
            .put(format!("{}/api/v1/nodes/{}", self.api_url, self.node_name))
            .json(&node_update)
            .send()
            .await;

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
                "osImage": format!("{} {}", std::env::consts::OS, std::env::consts::ARCH),
                "containerRuntimeVersion": "containerd://unknown",
                "kubeletVersion": format!("v1.32.0-rustkube+{}", apimachinery::VERSION),
                "kubeProxyVersion": format!("v1.32.0-rustkube+{}", apimachinery::VERSION),
                "operatingSystem": std::env::consts::OS,
                "architecture": std::env::consts::ARCH
            },
            "addresses": [{
                "type": "Hostname",
                "address": &self.node_name
            }]
        })
    }
}

/// Get system CPU count and total memory in KiB.
fn get_system_resources() -> (u64, u64) {
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1);

    // Default memory — platform-specific detection would go here
    let total_mem_ki = 8 * 1024 * 1024; // 8Gi default

    (total_mem_ki, cpu_count)
}
