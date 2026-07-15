//! Pod lifecycle manager.
//!
//! Manages the lifecycle of pods on this node. Watches for pods scheduled
//! to this node and drives them through: Pending → Running → Succeeded/Failed.

use crate::cri::{
    ContainerConfig, ContainerState, CriError, ImageService, Mount, PodSandboxConfig,
    RuntimeService,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// State of a managed pod on this node.
#[derive(Debug, Clone)]
pub struct PodState {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub sandbox_id: Option<String>,
    pub container_ids: HashMap<String, String>, // container name → container ID
    pub phase: String,
}

/// Manages pod lifecycle on a single node.
pub struct PodManager {
    runtime: Arc<dyn RuntimeService>,
    images: Arc<dyn ImageService>,
    pods: RwLock<HashMap<String, PodState>>, // uid → PodState
    node_name: String,
}

impl PodManager {
    pub fn new(
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        node_name: &str,
    ) -> Self {
        Self {
            runtime,
            images,
            pods: RwLock::new(HashMap::new()),
            node_name: node_name.to_string(),
        }
    }

    /// Sync desired pods (from API server) with actual running pods.
    pub async fn sync_pods(&self, desired_pods: &[Value]) -> Vec<PodStatusUpdate> {
        let mut updates = Vec::new();

        for pod in desired_pods {
            let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
            let name = pod["metadata"]["name"].as_str().unwrap_or("");
            let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
            let node_name = pod["spec"]["nodeName"].as_str().unwrap_or("");

            // Only manage pods scheduled to this node
            if node_name != self.node_name {
                continue;
            }

            let phase = pod["status"]["phase"].as_str().unwrap_or("Pending");

            // Skip terminated pods
            if phase == "Succeeded" || phase == "Failed" {
                continue;
            }

            let is_known = {
                let pods = self.pods.read().await;
                pods.contains_key(uid)
            };

            if !is_known {
                // New pod — start it
                match self.start_pod(pod).await {
                    Ok(status) => updates.push(status),
                    Err(e) => {
                        error!("Failed to start pod {namespace}/{name}: {e}");
                        updates.push(PodStatusUpdate {
                            namespace: namespace.to_string(),
                            name: name.to_string(),
                            phase: "Failed".to_string(),
                            message: e.to_string(),
                            container_statuses: vec![],
                            pod_ip: None,
                        });
                    }
                }
            } else {
                // Existing pod — check status
                match self.check_pod_status(uid).await {
                    Ok(status) => updates.push(status),
                    Err(e) => {
                        warn!("Failed to check pod {namespace}/{name} status: {e}");
                    }
                }
            }
        }

        updates
    }

    /// Start a new pod: create sandbox, pull images, create+start containers.
    async fn start_pod(&self, pod: &Value) -> Result<PodStatusUpdate, CriError> {
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

        info!("Starting pod {namespace}/{name}");

        // Build sandbox config
        let sandbox_config = PodSandboxConfig {
            name: name.to_string(),
            uid: uid.to_string(),
            namespace: namespace.to_string(),
            attempt: 0,
            hostname: pod["spec"]["hostname"]
                .as_str()
                .unwrap_or(name)
                .to_string(),
            log_directory: format!("/var/log/pods/{namespace}_{name}_{uid}"),
            dns_servers: pod["spec"]["dnsConfig"]["nameservers"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_else(|| vec!["10.96.0.10".to_string()]),
            dns_searches: vec![
                format!("{namespace}.svc.cluster.local"),
                "svc.cluster.local".to_string(),
                "cluster.local".to_string(),
            ],
            labels: extract_labels(pod),
            annotations: HashMap::new(),
            port_mappings: vec![],
        };

        // Create pod sandbox
        let sandbox_id = self.runtime.run_pod_sandbox(&sandbox_config).await?;
        info!("Created sandbox {sandbox_id} for {namespace}/{name}");

        // Get sandbox IP
        let sandbox_status = self.runtime.pod_sandbox_status(&sandbox_id).await?;
        let pod_ip = if sandbox_status.ip.is_empty() {
            None
        } else {
            Some(sandbox_status.ip.clone())
        };

        // Process containers
        let containers = pod["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut container_ids = HashMap::new();
        let mut container_statuses = Vec::new();

        for container_spec in &containers {
            let container_name = container_spec["name"].as_str().unwrap_or("unnamed");
            let image = container_spec["image"].as_str().unwrap_or("");

            // Pull image
            info!("Pulling image {image} for {namespace}/{name}/{container_name}");
            let image_ref = self.images.pull_image(image).await?;

            // Build container config
            let container_config = build_container_config(container_spec, &image_ref);

            // Create container
            let container_id = self
                .runtime
                .create_container(&sandbox_id, &container_config, &sandbox_config)
                .await?;

            // Start container
            self.runtime.start_container(&container_id).await?;
            info!("Started container {container_name} ({container_id}) in {namespace}/{name}");

            container_ids.insert(container_name.to_string(), container_id.clone());
            container_statuses.push(ContainerStatusReport {
                name: container_name.to_string(),
                container_id: container_id.clone(),
                state: "running".to_string(),
                ready: true,
                restart_count: 0,
                image: image.to_string(),
                image_ref: image_ref.to_string(),
            });
        }

        // Track the pod
        {
            let mut pods = self.pods.write().await;
            pods.insert(
                uid.to_string(),
                PodState {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    uid: uid.to_string(),
                    sandbox_id: Some(sandbox_id),
                    container_ids,
                    phase: "Running".to_string(),
                },
            );
        }

        Ok(PodStatusUpdate {
            namespace: namespace.to_string(),
            name: name.to_string(),
            phase: "Running".to_string(),
            message: String::new(),
            container_statuses,
            pod_ip,
        })
    }

    /// Check the status of a running pod.
    async fn check_pod_status(&self, uid: &str) -> Result<PodStatusUpdate, CriError> {
        let pod_state = {
            let pods = self.pods.read().await;
            pods.get(uid).cloned()
        };

        let state = pod_state.ok_or_else(|| CriError::NotFound(uid.to_string()))?;
        let mut container_statuses = Vec::new();
        let mut all_running = true;

        for (name, cid) in &state.container_ids {
            match self.runtime.container_status(cid).await {
                Ok(status) => {
                    let (state_str, ready) = match status.state {
                        ContainerState::Running => ("running", true),
                        ContainerState::Exited => {
                            all_running = false;
                            ("terminated", false)
                        }
                        ContainerState::Created => ("waiting", false),
                        ContainerState::Unknown => ("waiting", false),
                    };
                    container_statuses.push(ContainerStatusReport {
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: state_str.to_string(),
                        ready,
                        restart_count: 0,
                        image: status.image.clone(),
                        image_ref: status.image_ref,
                    });
                }
                Err(e) => {
                    all_running = false;
                    warn!("Failed to get status for container {name}: {e}");
                }
            }
        }

        let phase = if all_running { "Running" } else { "Failed" };

        Ok(PodStatusUpdate {
            namespace: state.namespace,
            name: state.name,
            phase: phase.to_string(),
            message: String::new(),
            container_statuses,
            pod_ip: None,
        })
    }

    /// Get the sandbox ID for a pod by UID.
    pub async fn get_sandbox_id(&self, uid: &str) -> Option<String> {
        let pods = self.pods.read().await;
        pods.get(uid).and_then(|s| s.sandbox_id.clone())
    }

    /// Register a pod that was restored from a checkpoint/migration.
    pub async fn register_restored_pod(
        &self,
        uid: &str,
        namespace: &str,
        name: &str,
        sandbox_id: &str,
    ) {
        let mut pods = self.pods.write().await;
        pods.insert(
            uid.to_string(),
            PodState {
                namespace: namespace.to_string(),
                name: name.to_string(),
                uid: uid.to_string(),
                sandbox_id: Some(sandbox_id.to_string()),
                container_ids: HashMap::new(),
                phase: "Running".to_string(),
            },
        );
        info!("Registered restored pod {namespace}/{name} with sandbox {sandbox_id}");
    }

    /// Stop and remove a pod.
    pub async fn stop_pod(&self, uid: &str) -> Result<(), CriError> {
        let state = {
            let mut pods = self.pods.write().await;
            pods.remove(uid)
        };

        if let Some(state) = state {
            // Stop containers
            for (name, cid) in &state.container_ids {
                info!("Stopping container {name} ({cid})");
                let _ = self.runtime.stop_container(cid, 30).await;
                let _ = self.runtime.remove_container(cid).await;
            }

            // Remove sandbox
            if let Some(sandbox_id) = &state.sandbox_id {
                info!("Removing sandbox {sandbox_id}");
                let _ = self.runtime.stop_pod_sandbox(sandbox_id).await;
                let _ = self.runtime.remove_pod_sandbox(sandbox_id).await;
            }
        }

        Ok(())
    }
}

/// Status update to send back to the API server.
#[derive(Debug)]
pub struct PodStatusUpdate {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub message: String,
    pub container_statuses: Vec<ContainerStatusReport>,
    pub pod_ip: Option<String>,
}

/// Container status for API server reporting.
#[derive(Debug)]
pub struct ContainerStatusReport {
    pub name: String,
    pub container_id: String,
    pub state: String,
    pub ready: bool,
    pub restart_count: u32,
    pub image: String,
    pub image_ref: String,
}

fn extract_labels(pod: &Value) -> HashMap<String, String> {
    pod["metadata"]["labels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn build_container_config(spec: &Value, image_ref: &str) -> ContainerConfig {
    let name = spec["name"].as_str().unwrap_or("unnamed").to_string();

    let command: Vec<String> = spec["command"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let args: Vec<String> = spec["args"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let envs: Vec<(String, String)> = spec["env"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| {
                    (
                        e["name"].as_str().unwrap_or("").to_string(),
                        e["value"].as_str().unwrap_or("").to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    let mounts: Vec<Mount> = spec["volumeMounts"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|m| Mount {
                    container_path: m["mountPath"].as_str().unwrap_or("").to_string(),
                    host_path: String::new(), // resolved later from volumes
                    readonly: m["readOnly"].as_bool().unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default();

    let cpu_request = spec["resources"]["requests"]["cpu"]
        .as_str()
        .unwrap_or("0");
    let mem_request = spec["resources"]["requests"]["memory"]
        .as_str()
        .unwrap_or("0");

    ContainerConfig {
        name,
        attempt: 0,
        image: image_ref.to_string(),
        command,
        args,
        working_dir: spec["workingDir"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        envs,
        mounts,
        labels: HashMap::new(),
        annotations: HashMap::new(),
        log_path: String::new(),
        stdin: spec["stdin"].as_bool().unwrap_or(false),
        tty: spec["tty"].as_bool().unwrap_or(false),
        cpu_period: 100_000,
        cpu_quota: parse_cpu_quota(cpu_request),
        cpu_shares: parse_cpu_shares(cpu_request),
        memory_limit_bytes: parse_memory_bytes(mem_request),
    }
}

fn parse_cpu_quota(s: &str) -> i64 {
    if let Some(stripped) = s.strip_suffix('m') {
        let millis: i64 = stripped.parse().unwrap_or(0);
        millis * 100 // 100m = 10000 quota (with 100000 period)
    } else {
        let cores: f64 = s.parse().unwrap_or(0.0);
        (cores * 100_000.0) as i64
    }
}

fn parse_cpu_shares(s: &str) -> i64 {
    if let Some(stripped) = s.strip_suffix('m') {
        let millis: i64 = stripped.parse().unwrap_or(0);
        (millis * 1024 / 1000).max(2) // 1 core = 1024 shares
    } else {
        let cores: f64 = s.parse().unwrap_or(0.0);
        ((cores * 1024.0) as i64).max(2)
    }
}

fn parse_memory_bytes(s: &str) -> i64 {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix("Ki") {
        stripped.parse::<i64>().unwrap_or(0) * 1024
    } else if let Some(stripped) = s.strip_suffix("Mi") {
        stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024
    } else if let Some(stripped) = s.strip_suffix("Gi") {
        stripped.parse::<i64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else {
        s.parse().unwrap_or(0)
    }
}
