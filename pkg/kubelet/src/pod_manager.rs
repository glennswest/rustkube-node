//! Pod lifecycle manager.
//!
//! Manages the lifecycle of pods on this node. Watches for pods scheduled
//! to this node and drives them through: Pending → Running → Succeeded/Failed.
//! Reconciliation is two-way: pods deleted from the API server (or marked
//! with a deletionTimestamp) are stopped and torn down, exited containers
//! are restarted per the pod restartPolicy, and liveness/readiness probes
//! drive container restarts and readiness.

use crate::cri::{
    ContainerConfig, ContainerState, CriError, ImageService, Mount, MountPropagation,
    PodSandboxConfig, RuntimeService,
};
use crate::health::{run_probe, ProbeResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
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
    /// Full pod object as last seen — needed to restart containers and run probes.
    pub pod: Value,
    pub pod_ip: Option<String>,
    /// Restart count per container name.
    pub restart_counts: HashMap<String, u32>,
    /// Readiness (probe result) per container name.
    pub ready: HashMap<String, bool>,
    /// Consecutive liveness probe failures per container name.
    pub liveness_failures: HashMap<String, u32>,
    /// When each container was last (re)started, for initialDelaySeconds.
    pub started: HashMap<String, Instant>,
    /// Containers that terminated for good (no restart) → exit code.
    pub terminated: HashMap<String, i32>,
}

/// Why a pod was removed from this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalReason {
    /// The pod has a deletionTimestamp — kubelet must confirm deletion.
    Deleting,
    /// The pod vanished from the API server's desired set.
    Orphaned,
}

/// A pod that was stopped and removed during sync.
#[derive(Debug)]
pub struct RemovedPod {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub reason: RemovalReason,
}

/// Result of a sync pass.
#[derive(Debug, Default)]
pub struct SyncOutcome {
    pub updates: Vec<PodStatusUpdate>,
    pub removed: Vec<RemovedPod>,
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
    ///
    /// Starts new pods, checks running ones (probes, restarts), and stops pods
    /// that carry a deletionTimestamp or disappeared from the desired set.
    pub async fn sync_pods(&self, desired_pods: &[Value]) -> SyncOutcome {
        let mut outcome = SyncOutcome::default();
        let mut desired_uids: Vec<String> = Vec::new();

        for pod in desired_pods {
            let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
            let name = pod["metadata"]["name"].as_str().unwrap_or("");
            let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
            let node_name = pod["spec"]["nodeName"].as_str().unwrap_or("");

            // Only manage pods scheduled to this node
            if node_name != self.node_name {
                continue;
            }
            desired_uids.push(uid.to_string());

            // Pod is being deleted — tear it down and confirm.
            if !pod["metadata"]["deletionTimestamp"].is_null() {
                let is_known = self.pods.read().await.contains_key(uid);
                if is_known {
                    info!("Pod {namespace}/{name} is terminating — stopping");
                    if let Err(e) = self.stop_pod(uid).await {
                        error!("Failed to stop terminating pod {namespace}/{name}: {e}");
                    }
                }
                outcome.removed.push(RemovedPod {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    uid: uid.to_string(),
                    reason: RemovalReason::Deleting,
                });
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
                    Ok(status) => outcome.updates.push(status),
                    Err(e) => {
                        error!("Failed to start pod {namespace}/{name}: {e}");
                        outcome.updates.push(PodStatusUpdate {
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
                // Existing pod — refresh spec, check status, run probes/restarts
                {
                    let mut pods = self.pods.write().await;
                    if let Some(state) = pods.get_mut(uid) {
                        state.pod = pod.clone();
                    }
                }
                match self.check_pod_status(uid).await {
                    Ok(status) => outcome.updates.push(status),
                    Err(e) => {
                        warn!("Failed to check pod {namespace}/{name} status: {e}");
                    }
                }
            }
        }

        // Pods we track that are no longer desired — stop them.
        let orphaned: Vec<PodState> = {
            let pods = self.pods.read().await;
            pods.values()
                .filter(|s| !desired_uids.contains(&s.uid))
                .cloned()
                .collect()
        };
        for state in orphaned {
            info!(
                "Pod {}/{} no longer desired — stopping",
                state.namespace, state.name
            );
            if let Err(e) = self.stop_pod(&state.uid).await {
                error!(
                    "Failed to stop orphaned pod {}/{}: {e}",
                    state.namespace, state.name
                );
            }
            outcome.removed.push(RemovedPod {
                namespace: state.namespace,
                name: state.name,
                uid: state.uid,
                reason: RemovalReason::Orphaned,
            });
        }

        outcome
    }

    /// Start a new pod: create sandbox, pull images, create+start containers.
    async fn start_pod(&self, pod: &Value) -> Result<PodStatusUpdate, CriError> {
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

        info!("Starting pod {namespace}/{name}");

        let sandbox_config = build_sandbox_config(pod);
        let volumes = resolve_volumes(pod);

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
        let mut ready_map = HashMap::new();
        let mut started_map = HashMap::new();

        for container_spec in &containers {
            let container_name = container_spec["name"].as_str().unwrap_or("unnamed");
            let image = container_spec["image"].as_str().unwrap_or("");

            // Pull image
            info!("Pulling image {image} for {namespace}/{name}/{container_name}");
            let image_ref = self.images.pull_image(image).await?;

            // Build container config
            let container_config = build_container_config(container_spec, &image_ref, &volumes);

            // Create container
            let container_id = self
                .runtime
                .create_container(&sandbox_id, &container_config, &sandbox_config)
                .await?;

            // Start container
            self.runtime.start_container(&container_id).await?;
            info!("Started container {container_name} ({container_id}) in {namespace}/{name}");

            // A container with a readiness probe starts not-ready until the
            // first probe succeeds; without one it is ready immediately.
            let ready = container_spec["readinessProbe"].is_null();
            ready_map.insert(container_name.to_string(), ready);
            started_map.insert(container_name.to_string(), Instant::now());

            container_ids.insert(container_name.to_string(), container_id.clone());
            container_statuses.push(ContainerStatusReport {
                name: container_name.to_string(),
                container_id: container_id.clone(),
                state: "running".to_string(),
                ready,
                restart_count: 0,
                exit_code: 0,
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
                    pod: pod.clone(),
                    pod_ip: pod_ip.clone(),
                    restart_counts: HashMap::new(),
                    ready: ready_map,
                    liveness_failures: HashMap::new(),
                    started: started_map,
                    terminated: HashMap::new(),
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

    /// Check the status of a running pod: query containers, run probes,
    /// restart per restartPolicy, and compute the pod phase.
    async fn check_pod_status(&self, uid: &str) -> Result<PodStatusUpdate, CriError> {
        let mut state = {
            let pods = self.pods.read().await;
            pods.get(uid).cloned()
        }
        .ok_or_else(|| CriError::NotFound(uid.to_string()))?;

        let restart_policy = state.pod["spec"]["restartPolicy"]
            .as_str()
            .unwrap_or("Always")
            .to_string();
        let container_specs: HashMap<String, Value> = state.pod["spec"]["containers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|c| (c["name"].as_str().unwrap_or("unnamed").to_string(), c.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let sandbox_config = build_sandbox_config(&state.pod);
        let pod_ip = state.pod_ip.clone().unwrap_or_default();

        let mut container_statuses = Vec::new();

        let container_ids: Vec<(String, String)> = state
            .container_ids
            .iter()
            .map(|(n, c)| (n.clone(), c.clone()))
            .collect();

        for (name, cid) in container_ids {
            let spec = container_specs.get(&name).cloned().unwrap_or(Value::Null);
            let restart_count = *state.restart_counts.get(&name).unwrap_or(&0);

            // Already terminated for good — report and move on.
            if let Some(exit_code) = state.terminated.get(&name) {
                container_statuses.push(ContainerStatusReport {
                    name: name.clone(),
                    container_id: cid.clone(),
                    state: "terminated".to_string(),
                    ready: false,
                    restart_count,
                    exit_code: *exit_code,
                    image: spec["image"].as_str().unwrap_or("").to_string(),
                    image_ref: String::new(),
                });
                continue;
            }

            let status = match self.runtime.container_status(&cid).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "Failed to get status for container {name} in {}/{}: {e}",
                        state.namespace, state.name
                    );
                    state.ready.insert(name.clone(), false);
                    container_statuses.push(ContainerStatusReport {
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "waiting".to_string(),
                        ready: false,
                        restart_count,
                        exit_code: 0,
                        image: spec["image"].as_str().unwrap_or("").to_string(),
                        image_ref: String::new(),
                    });
                    continue;
                }
            };

            match status.state {
                ContainerState::Running => {
                    let elapsed = state
                        .started
                        .get(&name)
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(u64::MAX);

                    // Liveness probe: consecutive failures past the threshold
                    // kill and restart the container.
                    let liveness = &spec["livenessProbe"];
                    let mut restarted = false;
                    if !liveness.is_null() && elapsed >= probe_initial_delay(liveness) {
                        match run_probe(liveness, &cid, &pod_ip, &self.runtime).await {
                            ProbeResult::Failure(reason) => {
                                let failures =
                                    state.liveness_failures.entry(name.clone()).or_insert(0);
                                *failures += 1;
                                let threshold = probe_failure_threshold(liveness);
                                warn!(
                                    "Liveness probe failed for {}/{}/{name} ({}/{threshold}): {reason}",
                                    state.namespace, state.name, *failures
                                );
                                if *failures >= threshold {
                                    info!(
                                        "Liveness threshold reached — restarting {}/{}/{name}",
                                        state.namespace, state.name
                                    );
                                    restarted = self
                                        .restart_container(
                                            &mut state,
                                            &name,
                                            &cid,
                                            &spec,
                                            &sandbox_config,
                                            &mut container_statuses,
                                        )
                                        .await;
                                }
                            }
                            _ => {
                                state.liveness_failures.insert(name.clone(), 0);
                            }
                        }
                    }
                    if restarted {
                        continue;
                    }

                    // Readiness probe drives the ready flag.
                    let readiness = &spec["readinessProbe"];
                    let ready = if readiness.is_null() {
                        true
                    } else if elapsed < probe_initial_delay(readiness) {
                        false
                    } else {
                        matches!(
                            run_probe(readiness, &cid, &pod_ip, &self.runtime).await,
                            ProbeResult::Success
                        )
                    };
                    state.ready.insert(name.clone(), ready);

                    container_statuses.push(ContainerStatusReport {
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "running".to_string(),
                        ready,
                        restart_count,
                        exit_code: 0,
                        image: status.image.clone(),
                        image_ref: status.image_ref,
                    });
                }
                ContainerState::Exited => {
                    let should_restart = match restart_policy.as_str() {
                        "Always" => true,
                        "OnFailure" => status.exit_code != 0,
                        _ => false,
                    };

                    if should_restart {
                        info!(
                            "Container {}/{}/{name} exited (code {}) — restarting per policy {restart_policy}",
                            state.namespace, state.name, status.exit_code
                        );
                        self.restart_container(
                            &mut state,
                            &name,
                            &cid,
                            &spec,
                            &sandbox_config,
                            &mut container_statuses,
                        )
                        .await;
                    } else {
                        info!(
                            "Container {}/{}/{name} exited (code {}) — not restarting (policy {restart_policy})",
                            state.namespace, state.name, status.exit_code
                        );
                        state.terminated.insert(name.clone(), status.exit_code);
                        state.ready.insert(name.clone(), false);
                        container_statuses.push(ContainerStatusReport {
                            name: name.clone(),
                            container_id: cid.clone(),
                            state: "terminated".to_string(),
                            ready: false,
                            restart_count,
                            exit_code: status.exit_code,
                            image: status.image.clone(),
                            image_ref: status.image_ref,
                        });
                    }
                }
                ContainerState::Created | ContainerState::Unknown => {
                    state.ready.insert(name.clone(), false);
                    container_statuses.push(ContainerStatusReport {
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "waiting".to_string(),
                        ready: false,
                        restart_count,
                        exit_code: 0,
                        image: status.image.clone(),
                        image_ref: status.image_ref,
                    });
                }
            }
        }

        // Pod phase: Succeeded/Failed only when every container has terminated
        // for good; otherwise it is still Running.
        let total = state.container_ids.len();
        let phase = if total > 0 && state.terminated.len() == total {
            if state.terminated.values().all(|&code| code == 0) {
                "Succeeded"
            } else {
                "Failed"
            }
        } else {
            "Running"
        };
        state.phase = phase.to_string();

        let update = PodStatusUpdate {
            namespace: state.namespace.clone(),
            name: state.name.clone(),
            phase: phase.to_string(),
            message: String::new(),
            container_statuses,
            pod_ip: state.pod_ip.clone(),
        };

        // Persist mutated state.
        {
            let mut pods = self.pods.write().await;
            pods.insert(uid.to_string(), state);
        }

        Ok(update)
    }

    /// Stop, remove, and recreate a container. Returns true on success;
    /// on failure the container is reported as waiting and retried next sync.
    async fn restart_container(
        &self,
        state: &mut PodState,
        name: &str,
        old_cid: &str,
        spec: &Value,
        sandbox_config: &PodSandboxConfig,
        container_statuses: &mut Vec<ContainerStatusReport>,
    ) -> bool {
        let _ = self.runtime.stop_container(old_cid, 5).await;
        let _ = self.runtime.remove_container(old_cid).await;

        let restart_count = state
            .restart_counts
            .entry(name.to_string())
            .and_modify(|c| *c += 1)
            .or_insert(1);
        let restart_count = *restart_count;
        state.liveness_failures.insert(name.to_string(), 0);

        let sandbox_id = match &state.sandbox_id {
            Some(id) => id.clone(),
            None => {
                error!(
                    "Cannot restart {}/{}/{name}: no sandbox",
                    state.namespace, state.name
                );
                return false;
            }
        };

        let image = spec["image"].as_str().unwrap_or("");
        let volumes = resolve_volumes(&state.pod);
        let result = async {
            let image_ref = self.images.pull_image(image).await?;
            let mut config = build_container_config(spec, &image_ref, &volumes);
            config.attempt = restart_count;
            let cid = self
                .runtime
                .create_container(&sandbox_id, &config, sandbox_config)
                .await?;
            self.runtime.start_container(&cid).await?;
            Ok::<(String, String), CriError>((cid, image_ref))
        }
        .await;

        match result {
            Ok((new_cid, image_ref)) => {
                state.container_ids.insert(name.to_string(), new_cid.clone());
                state.started.insert(name.to_string(), Instant::now());
                let ready = spec["readinessProbe"].is_null();
                state.ready.insert(name.to_string(), ready);
                container_statuses.push(ContainerStatusReport {
                    name: name.to_string(),
                    container_id: new_cid,
                    state: "running".to_string(),
                    ready,
                    restart_count,
                    exit_code: 0,
                    image: image.to_string(),
                    image_ref,
                });
                true
            }
            Err(e) => {
                error!(
                    "Failed to restart container {}/{}/{name}: {e}",
                    state.namespace, state.name
                );
                state.ready.insert(name.to_string(), false);
                container_statuses.push(ContainerStatusReport {
                    name: name.to_string(),
                    container_id: String::new(),
                    state: "waiting".to_string(),
                    ready: false,
                    restart_count,
                    exit_code: 0,
                    image: image.to_string(),
                    image_ref: String::new(),
                });
                false
            }
        }
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
                pod: Value::Null,
                pod_ip: None,
                restart_counts: HashMap::new(),
                ready: HashMap::new(),
                liveness_failures: HashMap::new(),
                started: HashMap::new(),
                terminated: HashMap::new(),
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
    pub exit_code: i32,
    pub image: String,
    pub image_ref: String,
}

/// Build the sandbox config for a pod object.
fn build_sandbox_config(pod: &Value) -> PodSandboxConfig {
    let name = pod["metadata"]["name"].as_str().unwrap_or("");
    let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
    let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

    PodSandboxConfig {
        name: name.to_string(),
        uid: uid.to_string(),
        namespace: namespace.to_string(),
        attempt: 0,
        hostname: pod["spec"]["hostname"].as_str().unwrap_or(name).to_string(),
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
        host_network: pod["spec"]["hostNetwork"].as_bool().unwrap_or(false),
        host_pid: pod["spec"]["hostPID"].as_bool().unwrap_or(false),
        host_ipc: pod["spec"]["hostIPC"].as_bool().unwrap_or(false),
    }
}

/// Resolve a pod's `spec.volumes` to host paths keyed by volume name, for the
/// volume types we can back with a host directory today:
///   - `hostPath`   → the given host path (created if `type` requests it)
///   - `emptyDir`   → a per-pod scratch dir under /var/lib/kubelet
/// configMap/secret/projected are not materialized yet (they need apiserver
/// reads) — a mount referencing them resolves to an empty per-pod dir so the
/// container still starts. Returns name → host_path.
fn resolve_volumes(pod: &Value) -> HashMap<String, String> {
    let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
    let mut map = HashMap::new();
    let volumes = match pod["spec"]["volumes"].as_array() {
        Some(v) => v,
        None => return map,
    };
    for vol in volumes {
        let name = match vol["name"].as_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let host_path = if let Some(hp) = vol["hostPath"]["path"].as_str() {
            // hostPath.type: DirectoryOrCreate/Directory create the dir.
            let typ = vol["hostPath"]["type"].as_str().unwrap_or("");
            if matches!(typ, "DirectoryOrCreate" | "Directory" | "") {
                let _ = std::fs::create_dir_all(hp);
            }
            hp.to_string()
        } else {
            // emptyDir (and, for now, configMap/secret/projected fallback):
            // a per-pod scratch directory.
            let dir = format!(
                "/var/lib/kubelet/pods/{uid}/volumes/kubernetes.io~empty-dir/{name}"
            );
            let _ = std::fs::create_dir_all(&dir);
            dir
        };
        map.insert(name, host_path);
    }
    map
}

fn probe_initial_delay(probe: &Value) -> u64 {
    probe["initialDelaySeconds"].as_u64().unwrap_or(0)
}

fn probe_failure_threshold(probe: &Value) -> u32 {
    probe["failureThreshold"].as_u64().unwrap_or(3) as u32
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

fn build_container_config(
    spec: &Value,
    image_ref: &str,
    volumes: &HashMap<String, String>,
) -> ContainerConfig {
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
                .filter_map(|m| {
                    let vol_name = m["name"].as_str().unwrap_or("");
                    // Skip mounts whose volume we couldn't resolve to a host
                    // path rather than bind-mounting an empty string.
                    let host_path = volumes.get(vol_name)?.clone();
                    Some(Mount {
                        container_path: m["mountPath"].as_str().unwrap_or("").to_string(),
                        host_path,
                        readonly: m["readOnly"].as_bool().unwrap_or(false),
                        propagation: match m["mountPropagation"].as_str() {
                            Some("Bidirectional") => MountPropagation::Bidirectional,
                            Some("HostToContainer") => MountPropagation::HostToContainer,
                            _ => MountPropagation::Private,
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let sc = &spec["securityContext"];
    let add_capabilities: Vec<String> = sc["capabilities"]["add"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
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
        privileged: sc["privileged"].as_bool().unwrap_or(false),
        readonly_rootfs: sc["readOnlyRootFilesystem"].as_bool().unwrap_or(false),
        add_capabilities,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cri::{
        ContainerStatusInfo, ExecSyncResult, ImageInfo, PodSandboxState, PodSandboxStatusInfo,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Clone)]
    struct FakeContainer {
        name: String,
        state: ContainerState,
        exit_code: i32,
        image: String,
    }

    /// In-memory runtime for pod lifecycle tests.
    #[derive(Default)]
    struct FakeRuntime {
        sandboxes: Mutex<HashMap<String, PodSandboxState>>,
        containers: Mutex<HashMap<String, FakeContainer>>,
        next_id: AtomicU32,
        /// Exit code exec_sync returns (probes).
        exec_exit_code: Mutex<i32>,
        removed_sandboxes: Mutex<Vec<String>>,
        removed_containers: Mutex<Vec<String>>,
    }

    impl FakeRuntime {
        fn set_container_state(&self, cid: &str, state: ContainerState, exit_code: i32) {
            let mut containers = self.containers.lock().unwrap();
            let c = containers.get_mut(cid).expect("container exists");
            c.state = state;
            c.exit_code = exit_code;
        }

        fn set_exec_exit_code(&self, code: i32) {
            *self.exec_exit_code.lock().unwrap() = code;
        }

        fn container_ids(&self) -> Vec<String> {
            self.containers.lock().unwrap().keys().cloned().collect()
        }

        fn live_sandbox_count(&self) -> usize {
            self.sandboxes.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl RuntimeService for FakeRuntime {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok(("fake".into(), "0.1".into(), "v1".into()))
        }

        async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
            let id = format!("sb-{}-{}", config.uid, self.next_id.fetch_add(1, Ordering::SeqCst));
            self.sandboxes
                .lock()
                .unwrap()
                .insert(id.clone(), PodSandboxState::Ready);
            Ok(id)
        }

        async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            self.sandboxes
                .lock()
                .unwrap()
                .insert(sandbox_id.to_string(), PodSandboxState::NotReady);
            Ok(())
        }

        async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            self.sandboxes.lock().unwrap().remove(sandbox_id);
            self.removed_sandboxes
                .lock()
                .unwrap()
                .push(sandbox_id.to_string());
            Ok(())
        }

        async fn pod_sandbox_status(
            &self,
            sandbox_id: &str,
        ) -> Result<PodSandboxStatusInfo, CriError> {
            Ok(PodSandboxStatusInfo {
                id: sandbox_id.to_string(),
                state: PodSandboxState::Ready,
                created_at: 0,
                ip: "10.88.0.5".to_string(),
                additional_ips: vec![],
            })
        }

        async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> {
            Ok(self
                .sandboxes
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect())
        }

        async fn create_container(
            &self,
            _sandbox_id: &str,
            config: &ContainerConfig,
            _sandbox_config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            let id = format!("c-{}-{}", config.name, self.next_id.fetch_add(1, Ordering::SeqCst));
            self.containers.lock().unwrap().insert(
                id.clone(),
                FakeContainer {
                    name: config.name.clone(),
                    state: ContainerState::Created,
                    exit_code: 0,
                    image: config.image.clone(),
                },
            );
            Ok(id)
        }

        async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
            let mut containers = self.containers.lock().unwrap();
            let c = containers
                .get_mut(container_id)
                .ok_or_else(|| CriError::NotFound(container_id.to_string()))?;
            c.state = ContainerState::Running;
            Ok(())
        }

        async fn stop_container(&self, container_id: &str, _timeout: i64) -> Result<(), CriError> {
            if let Some(c) = self.containers.lock().unwrap().get_mut(container_id) {
                c.state = ContainerState::Exited;
            }
            Ok(())
        }

        async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
            self.containers.lock().unwrap().remove(container_id);
            self.removed_containers
                .lock()
                .unwrap()
                .push(container_id.to_string());
            Ok(())
        }

        async fn container_status(
            &self,
            container_id: &str,
        ) -> Result<ContainerStatusInfo, CriError> {
            let containers = self.containers.lock().unwrap();
            let c = containers
                .get(container_id)
                .ok_or_else(|| CriError::NotFound(container_id.to_string()))?;
            Ok(ContainerStatusInfo {
                id: container_id.to_string(),
                name: c.name.clone(),
                state: c.state,
                created_at: 0,
                started_at: 0,
                finished_at: 0,
                exit_code: c.exit_code,
                image: c.image.clone(),
                image_ref: format!("{}@sha256:fake", c.image),
                reason: String::new(),
                message: String::new(),
            })
        }

        async fn list_containers(
            &self,
            _sandbox_id: Option<&str>,
        ) -> Result<Vec<ContainerStatusInfo>, CriError> {
            Ok(vec![])
        }

        async fn exec_sync(
            &self,
            _container_id: &str,
            _cmd: &[String],
            _timeout: i64,
        ) -> Result<ExecSyncResult, CriError> {
            Ok(ExecSyncResult {
                stdout: vec![],
                stderr: vec![],
                exit_code: *self.exec_exit_code.lock().unwrap(),
            })
        }
    }

    #[async_trait]
    impl ImageService for FakeRuntime {
        async fn pull_image(&self, image: &str) -> Result<String, CriError> {
            Ok(format!("{image}@sha256:fake"))
        }

        async fn image_status(&self, _image: &str) -> Result<Option<ImageInfo>, CriError> {
            Ok(None)
        }

        async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
            Ok(vec![])
        }

        async fn remove_image(&self, _image: &str) -> Result<(), CriError> {
            Ok(())
        }
    }

    const NODE: &str = "test-node";

    fn manager() -> (Arc<FakeRuntime>, PodManager) {
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::new(rt.clone(), rt.clone(), NODE);
        (rt, mgr)
    }

    fn pod(uid: &str, name: &str, restart_policy: &str, container: Value) -> Value {
        json!({
            "metadata": {"name": name, "namespace": "default", "uid": uid},
            "spec": {
                "nodeName": NODE,
                "restartPolicy": restart_policy,
                "containers": [container]
            },
            "status": {"phase": "Pending"}
        })
    }

    fn simple_container() -> Value {
        json!({"name": "app", "image": "busybox:latest"})
    }

    #[test]
    fn sandbox_config_reads_host_namespaces() {
        let mut p = pod("uid-1", "web", "Always", simple_container());
        p["spec"]["hostNetwork"] = json!(true);
        p["spec"]["hostPID"] = json!(true);
        let sc = build_sandbox_config(&p);
        assert!(sc.host_network);
        assert!(sc.host_pid);
        assert!(!sc.host_ipc);
    }

    #[test]
    fn resolve_volumes_maps_hostpath_and_emptydir() {
        let p = json!({
            "metadata": {"uid": "uid-9"},
            "spec": {"volumes": [
                {"name": "bpf", "hostPath": {"path": "/sys/fs/bpf", "type": "DirectoryOrCreate"}},
                {"name": "scratch", "emptyDir": {}}
            ]}
        });
        let v = resolve_volumes(&p);
        assert_eq!(v.get("bpf").map(String::as_str), Some("/sys/fs/bpf"));
        assert_eq!(
            v.get("scratch").map(String::as_str),
            Some("/var/lib/kubelet/pods/uid-9/volumes/kubernetes.io~empty-dir/scratch")
        );
    }

    #[test]
    fn container_config_resolves_mounts_and_security_context() {
        let mut volumes = HashMap::new();
        volumes.insert("bpf".to_string(), "/sys/fs/bpf".to_string());
        let spec = json!({
            "name": "cilium-agent",
            "image": "cilium:latest",
            "securityContext": {
                "privileged": true,
                "readOnlyRootFilesystem": true,
                "capabilities": {"add": ["NET_ADMIN", "SYS_MODULE"]}
            },
            "volumeMounts": [
                {"name": "bpf", "mountPath": "/sys/fs/bpf", "mountPropagation": "Bidirectional"},
                {"name": "missing", "mountPath": "/nope"}
            ]
        });
        let c = build_container_config(&spec, "cilium@sha", &volumes);
        assert!(c.privileged);
        assert!(c.readonly_rootfs);
        assert_eq!(c.add_capabilities, vec!["NET_ADMIN", "SYS_MODULE"]);
        // Only the resolvable mount is included; the unresolved one is dropped.
        assert_eq!(c.mounts.len(), 1);
        assert_eq!(c.mounts[0].host_path, "/sys/fs/bpf");
        assert_eq!(c.mounts[0].container_path, "/sys/fs/bpf");
        assert_eq!(c.mounts[0].propagation, MountPropagation::Bidirectional);
    }

    #[test]
    fn container_config_defaults_are_unprivileged() {
        let c = build_container_config(&simple_container(), "img", &HashMap::new());
        assert!(!c.privileged);
        assert!(c.add_capabilities.is_empty());
        assert!(c.mounts.is_empty());
    }

    #[tokio::test]
    async fn starts_new_pod() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());

        let outcome = mgr.sync_pods(&[p]).await;

        assert_eq!(outcome.updates.len(), 1);
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Running");
        assert_eq!(u.pod_ip.as_deref(), Some("10.88.0.5"));
        assert_eq!(u.container_statuses.len(), 1);
        assert!(u.container_statuses[0].ready);
        assert_eq!(rt.live_sandbox_count(), 1);
        assert_eq!(rt.container_ids().len(), 1);
    }

    #[tokio::test]
    async fn stops_orphaned_pod() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p]).await;

        // Pod vanished from the API server.
        let outcome = mgr.sync_pods(&[]).await;

        assert_eq!(outcome.removed.len(), 1);
        assert_eq!(outcome.removed[0].reason, RemovalReason::Orphaned);
        assert_eq!(rt.live_sandbox_count(), 0);
        assert!(rt.container_ids().is_empty());
    }

    #[tokio::test]
    async fn stops_terminating_pod() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        let mut deleting = p;
        deleting["metadata"]["deletionTimestamp"] = json!("2026-07-15T00:00:00Z");
        let outcome = mgr.sync_pods(&[deleting]).await;

        assert_eq!(outcome.removed.len(), 1);
        assert_eq!(outcome.removed[0].reason, RemovalReason::Deleting);
        assert_eq!(rt.live_sandbox_count(), 0);
        assert!(rt.container_ids().is_empty());
    }

    #[tokio::test]
    async fn restarts_exited_container_policy_always() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        let old_cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&old_cid, ContainerState::Exited, 1);

        let outcome = mgr.sync_pods(&[p]).await;

        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Running");
        assert_eq!(u.container_statuses[0].restart_count, 1);
        assert_eq!(u.container_statuses[0].state, "running");
        let new_cid = rt.container_ids().pop().unwrap();
        assert_ne!(new_cid, old_cid);
        assert!(rt.removed_containers.lock().unwrap().contains(&old_cid));
    }

    #[tokio::test]
    async fn pod_succeeds_policy_never_exit_zero() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "job", "Never", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 0);

        let outcome = mgr.sync_pods(&[p.clone()]).await;
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Succeeded");
        assert_eq!(u.container_statuses[0].state, "terminated");
        assert_eq!(u.container_statuses[0].exit_code, 0);
        // No restart happened.
        assert_eq!(u.container_statuses[0].restart_count, 0);
    }

    #[tokio::test]
    async fn pod_fails_policy_never_nonzero_exit() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "job", "Never", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 2);

        let outcome = mgr.sync_pods(&[p]).await;
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Failed");
        assert_eq!(u.container_statuses[0].exit_code, 2);
    }

    #[tokio::test]
    async fn policy_onfailure_restarts_only_on_failure() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "job", "OnFailure", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        // Exit 1 → restart
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(outcome.updates[0].container_statuses[0].restart_count, 1);
        assert_eq!(outcome.updates[0].phase, "Running");

        // Exit 0 → done, Succeeded
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 0);
        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].phase, "Succeeded");
    }

    #[tokio::test]
    async fn readiness_probe_drives_ready_flag() {
        let (rt, mgr) = manager();
        let container = json!({
            "name": "app",
            "image": "busybox:latest",
            "readinessProbe": {"exec": {"command": ["check"]}}
        });
        let p = pod("uid-1", "web", "Always", container);

        // With a readiness probe the container starts not-ready.
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert!(!outcome.updates[0].container_statuses[0].ready);

        // Probe failing → still not ready.
        rt.set_exec_exit_code(1);
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert!(!outcome.updates[0].container_statuses[0].ready);

        // Probe succeeding → ready.
        rt.set_exec_exit_code(0);
        let outcome = mgr.sync_pods(&[p]).await;
        assert!(outcome.updates[0].container_statuses[0].ready);
    }

    #[tokio::test]
    async fn liveness_probe_failure_restarts_container() {
        let (rt, mgr) = manager();
        let container = json!({
            "name": "app",
            "image": "busybox:latest",
            "livenessProbe": {"exec": {"command": ["check"]}, "failureThreshold": 2}
        });
        let p = pod("uid-1", "web", "Always", container);
        mgr.sync_pods(&[p.clone()]).await;
        let old_cid = rt.container_ids().pop().unwrap();

        rt.set_exec_exit_code(1);

        // First failure — under threshold, no restart.
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(outcome.updates[0].container_statuses[0].restart_count, 0);

        // Second failure — threshold reached, restart.
        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].container_statuses[0].restart_count, 1);
        let new_cid = rt.container_ids().pop().unwrap();
        assert_ne!(new_cid, old_cid);
    }

    #[tokio::test]
    async fn ignores_pods_for_other_nodes() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-1", "web", "Always", simple_container());
        p["spec"]["nodeName"] = json!("other-node");

        let outcome = mgr.sync_pods(&[p]).await;

        assert!(outcome.updates.is_empty());
        assert!(outcome.removed.is_empty());
        assert_eq!(rt.live_sandbox_count(), 0);
    }
}
