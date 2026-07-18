//! Pod lifecycle manager.
//!
//! Manages the lifecycle of pods on this node. Watches for pods scheduled
//! to this node and drives them through: Pending → Running → Succeeded/Failed.
//! Reconciliation is two-way: pods deleted from the API server (or marked
//! with a deletionTimestamp) are stopped and torn down, exited containers
//! are restarted per the pod restartPolicy, and liveness/readiness probes
//! drive container restarts and readiness.

use crate::cri::{
    ContainerConfig, ContainerState, CriError, ImageInfo, ImageService, Mount, MountPropagation,
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
    /// API server base URL, for reading ConfigMaps/Secrets referenced by env
    /// `valueFrom` and configMap/secret volumes. Empty → those reads are skipped.
    api_url: String,
    api_client: reqwest::Client,
    /// This node's IP, for the downward API `status.hostIP`.
    node_ip: String,
    /// Kubelet state root; per-pod volume dirs live under `<state_root>/pods`.
    /// Overridable in tests. Default `/var/lib/kubelet`.
    state_root: String,
}

impl PodManager {
    pub fn new(
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        node_name: &str,
    ) -> Self {
        Self::with_api(runtime, images, node_name, "", "127.0.0.1")
    }

    pub fn with_api(
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        node_name: &str,
        api_url: &str,
        node_ip: &str,
    ) -> Self {
        Self {
            runtime,
            images,
            pods: RwLock::new(HashMap::new()),
            node_name: node_name.to_string(),
            api_url: api_url.trim_end_matches('/').to_string(),
            api_client: reqwest::Client::new(),
            node_ip: node_ip.to_string(),
            state_root: "/var/lib/kubelet".to_string(),
        }
    }

    /// Fetch a ConfigMap's `data` map (namespaced). None on any failure.
    async fn fetch_configmap(&self, namespace: &str, name: &str) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!(
            "{}/api/v1/namespaces/{namespace}/configmaps/{name}",
            self.api_url
        );
        let resp = self.api_client.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Fetch a Secret and return its `data` with values base64-decoded to strings.
    async fn fetch_secret_decoded(
        &self,
        namespace: &str,
        name: &str,
    ) -> Option<HashMap<String, String>> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!(
            "{}/api/v1/namespaces/{namespace}/secrets/{name}",
            self.api_url
        );
        let resp = self.api_client.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let obj: Value = resp.json().await.ok()?;
        let data = obj["data"].as_object()?;
        let mut out = HashMap::new();
        for (k, v) in data {
            if let Some(b64) = v.as_str() {
                if let Ok(bytes) = base64_decode(b64) {
                    out.insert(k.clone(), String::from_utf8_lossy(&bytes).into_owned());
                }
            }
        }
        Some(out)
    }

    /// Resolve a container's env: literal `value` plus `valueFrom`
    /// (fieldRef downward API, configMapKeyRef, secretKeyRef).
    async fn resolve_env(
        &self,
        pod: &Value,
        container_spec: &Value,
        pod_ip: Option<&str>,
    ) -> Vec<(String, String)> {
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let mut out = Vec::new();
        let env = match container_spec["env"].as_array() {
            Some(e) => e,
            None => return out,
        };
        for e in env {
            let name = match e["name"].as_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            if let Some(v) = e["value"].as_str() {
                out.push((name, v.to_string()));
                continue;
            }
            let vf = &e["valueFrom"];
            if let Some(path) = vf["fieldRef"]["fieldPath"].as_str() {
                if let Some(v) = self.downward_field(pod, path, pod_ip) {
                    out.push((name, v));
                }
            } else if !vf["configMapKeyRef"].is_null() {
                let cm = vf["configMapKeyRef"]["name"].as_str().unwrap_or("");
                let key = vf["configMapKeyRef"]["key"].as_str().unwrap_or("");
                if let Some(obj) = self.fetch_configmap(namespace, cm).await {
                    if let Some(v) = obj["data"][key].as_str() {
                        out.push((name, v.to_string()));
                    }
                }
            } else if !vf["secretKeyRef"].is_null() {
                let sec = vf["secretKeyRef"]["name"].as_str().unwrap_or("");
                let key = vf["secretKeyRef"]["key"].as_str().unwrap_or("");
                if let Some(data) = self.fetch_secret_decoded(namespace, sec).await {
                    if let Some(v) = data.get(key) {
                        out.push((name, v.clone()));
                    }
                }
            }
        }
        out
    }

    /// Resolve a downward-API `fieldRef.fieldPath` against the pod + node context.
    fn downward_field(&self, pod: &Value, path: &str, pod_ip: Option<&str>) -> Option<String> {
        // metadata.labels['x'] / metadata.annotations['x']
        if let Some(rest) = path.strip_prefix("metadata.labels['") {
            let key = rest.strip_suffix("']")?;
            return Some(pod["metadata"]["labels"][key].as_str().unwrap_or("").to_string());
        }
        if let Some(rest) = path.strip_prefix("metadata.annotations['") {
            let key = rest.strip_suffix("']")?;
            return Some(
                pod["metadata"]["annotations"][key]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            );
        }
        let v = match path {
            "metadata.name" => pod["metadata"]["name"].as_str().unwrap_or("").to_string(),
            "metadata.namespace" => pod["metadata"]["namespace"].as_str().unwrap_or("default").to_string(),
            "metadata.uid" => pod["metadata"]["uid"].as_str().unwrap_or("").to_string(),
            "spec.nodeName" => self.node_name.clone(),
            "spec.serviceAccountName" => pod["spec"]["serviceAccountName"]
                .as_str()
                .unwrap_or("default")
                .to_string(),
            "status.hostIP" => self.node_ip.clone(),
            "status.podIP" => pod_ip.unwrap_or("").to_string(),
            _ => return None,
        };
        Some(v)
    }

    /// Resolve `spec.volumes` to host paths, materializing configMap/secret
    /// volumes to files under the pod dir. hostPath/emptyDir handled inline.
    async fn resolve_volumes(&self, pod: &Value) -> HashMap<String, String> {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
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
                let typ = vol["hostPath"]["type"].as_str().unwrap_or("");
                if matches!(typ, "DirectoryOrCreate" | "Directory" | "") {
                    let _ = std::fs::create_dir_all(hp);
                }
                hp.to_string()
            } else if !vol["configMap"].is_null() {
                let cm = vol["configMap"]["name"].as_str().unwrap_or("");
                let dir = pod_volume_dir(&self.state_root, uid, "configmap", &name);
                let data = self
                    .fetch_configmap(namespace, cm)
                    .await
                    .and_then(|o| o["data"].as_object().cloned());
                materialize_files(&dir, data.as_ref(), false);
                dir
            } else if !vol["secret"].is_null() {
                let sec = vol["secret"]["secretName"].as_str().unwrap_or("");
                let dir = pod_volume_dir(&self.state_root, uid, "secret", &name);
                let decoded = self.fetch_secret_decoded(namespace, sec).await;
                materialize_secret_files(&dir, decoded.as_ref());
                dir
            } else if let Some(sources) = vol["projected"]["sources"].as_array() {
                // Projected volume (e.g. kube-api-access: SA token + CA + downward API).
                let dir = pod_volume_dir(&self.state_root, uid, "projected", &name);
                let _ = std::fs::create_dir_all(&dir);
                self.materialize_projected(pod, namespace, &dir, sources).await;
                dir
            } else {
                // emptyDir (and other unhandled types): per-pod scratch dir.
                let dir = pod_volume_dir(&self.state_root, uid, "empty-dir", &name);
                let _ = std::fs::create_dir_all(&dir);
                dir
            };
            map.insert(name, host_path);
        }
        map
    }

    /// Materialize a projected volume's sources into `dir`: serviceAccountToken
    /// (via TokenRequest, best-effort), configMap, secret, and downwardAPI.
    async fn materialize_projected(
        &self,
        pod: &Value,
        namespace: &str,
        dir: &str,
        sources: &[Value],
    ) {
        for src in sources {
            if let Some(sat) = src.get("serviceAccountToken").filter(|v| !v.is_null()) {
                let path = sat["path"].as_str().unwrap_or("token");
                let sa = pod["spec"]["serviceAccountName"].as_str().unwrap_or("default");
                let aud = sat["audience"].as_str();
                if let Some(token) = self.request_sa_token(namespace, sa, aud).await {
                    let _ = std::fs::write(format!("{dir}/{path}"), token);
                }
            } else if let Some(cm) = src.get("configMap").filter(|v| !v.is_null()) {
                let name = cm["name"].as_str().unwrap_or("");
                let data = self
                    .fetch_configmap(namespace, name)
                    .await
                    .and_then(|o| o["data"].as_object().cloned());
                write_projected_items(dir, cm["items"].as_array(), data.as_ref());
            } else if let Some(sec) = src.get("secret").filter(|v| !v.is_null()) {
                let name = sec["name"].as_str().unwrap_or("");
                let decoded = self.fetch_secret_decoded(namespace, name).await;
                if let Some(data) = decoded {
                    let asmap: serde_json::Map<String, Value> =
                        data.into_iter().map(|(k, v)| (k, Value::String(v))).collect();
                    write_projected_items(dir, sec["items"].as_array(), Some(&asmap));
                }
            } else if let Some(dw) = src.get("downwardAPI").filter(|v| !v.is_null()) {
                if let Some(items) = dw["items"].as_array() {
                    for it in items {
                        let path = it["path"].as_str().unwrap_or("");
                        if let Some(fp) = it["fieldRef"]["fieldPath"].as_str() {
                            if let Some(val) = self.downward_field(pod, fp, None) {
                                let _ = std::fs::write(format!("{dir}/{path}"), val);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Request a ServiceAccount token via the TokenRequest API (best-effort;
    /// None if the apiserver doesn't support it or the call fails).
    async fn request_sa_token(
        &self,
        namespace: &str,
        sa: &str,
        audience: Option<&str>,
    ) -> Option<String> {
        if self.api_url.is_empty() {
            return None;
        }
        let body = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": { "audiences": audience.map(|a| vec![a]).unwrap_or_default() }
        });
        let url = format!(
            "{}/api/v1/namespaces/{namespace}/serviceaccounts/{sa}/token",
            self.api_url
        );
        let resp = self.api_client.post(&url).json(&body).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let v: Value = resp.json().await.ok()?;
        v["status"]["token"].as_str().map(String::from)
    }

    /// Standard in-cluster apiserver-discovery env vars. Points at the
    /// apiserver the kubelet uses (there is no Service routing yet, so the
    /// upstream 10.96.0.1 ClusterIP would be unreachable).
    fn service_account_env(&self) -> Vec<(String, String)> {
        let (host, port) = apiserver_host_port(&self.api_url);
        vec![
            ("KUBERNETES_SERVICE_HOST".into(), host),
            ("KUBERNETES_SERVICE_PORT".into(), port.clone()),
            ("KUBERNETES_SERVICE_PORT_HTTPS".into(), port),
        ]
    }

    /// Default ServiceAccount credential mount at
    /// `/var/run/secrets/kubernetes.io/serviceaccount` (token/ca.crt/namespace),
    /// used when the apiserver's SA admission did NOT already inject a
    /// projected `kube-api-access` volume and automount isn't disabled.
    async fn service_account_mount(&self, pod: &Value) -> Option<Mount> {
        if pod["spec"]["automountServiceAccountToken"].as_bool() == Some(false) {
            return None;
        }
        if pod_mounts_sa_path(pod) {
            return None; // SA admission already provided it — don't double-mount.
        }
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let sa = pod["spec"]["serviceAccountName"].as_str().unwrap_or("default");

        let dir = pod_volume_dir(&self.state_root, uid, "secret", "kube-api-access");
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(format!("{dir}/namespace"), namespace);
        if let Some(token) = self.request_sa_token(namespace, sa, None).await {
            let _ = std::fs::write(format!("{dir}/token"), token);
        }
        // ca.crt: the cluster CA would go here once the apiserver serves TLS.

        Some(Mount {
            container_path: SA_MOUNT_PATH.to_string(),
            host_path: dir,
            readonly: true,
            propagation: MountPropagation::Private,
        })
    }

    /// Reconcile the in-memory pod map with sandboxes already running in the
    /// container runtime. Called once at startup so a kubelet restart adopts
    /// the pods it was already running instead of creating duplicate sandboxes.
    pub async fn recover_state(&self) {
        let sandboxes = match self.runtime.list_pod_sandbox().await {
            Ok(s) => s,
            Err(e) => {
                warn!("state recovery: listing sandboxes failed: {e}");
                return;
            }
        };
        let mut recovered = 0;
        for sb in sandboxes {
            if sb.uid.is_empty() {
                continue;
            }
            // Map container name → id for the containers in this sandbox.
            let mut container_ids = HashMap::new();
            if let Ok(cs) = self.runtime.list_containers(Some(&sb.id)).await {
                for c in cs {
                    if !c.name.is_empty() {
                        container_ids.insert(c.name, c.id);
                    }
                }
            }
            let pod_ip = self
                .runtime
                .pod_sandbox_status(&sb.id)
                .await
                .ok()
                .map(|s| s.ip)
                .filter(|ip| !ip.is_empty());

            let mut pods = self.pods.write().await;
            if pods.contains_key(&sb.uid) {
                continue;
            }
            pods.insert(
                sb.uid.clone(),
                PodState {
                    namespace: sb.namespace.clone(),
                    name: sb.name.clone(),
                    uid: sb.uid.clone(),
                    sandbox_id: Some(sb.id.clone()),
                    container_ids,
                    phase: "Running".to_string(),
                    pod: Value::Null, // refreshed on the next sync
                    pod_ip,
                    restart_counts: HashMap::new(),
                    ready: HashMap::new(),
                    liveness_failures: HashMap::new(),
                    started: HashMap::new(),
                    terminated: HashMap::new(),
                },
            );
            recovered += 1;
            info!(
                "state recovery: adopted running pod {}/{} (sandbox {})",
                sb.namespace, sb.name, sb.id
            );
        }
        if recovered > 0 {
            info!("state recovery: adopted {recovered} running pod sandbox(es)");
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
    /// Run `spec.initContainers` in order, each to a successful (exit 0)
    /// completion, before the app containers start. A non-zero exit or a
    /// runtime error aborts pod start (the caller reports Failed; the
    /// controller/next sync retries).
    #[allow(clippy::too_many_arguments)]
    async fn run_init_containers(
        &self,
        pod: &Value,
        sandbox_id: &str,
        sandbox_config: &PodSandboxConfig,
        volumes: &HashMap<String, String>,
        pod_ip: Option<&str>,
        sa_mount: &Option<Mount>,
    ) -> Result<(), CriError> {
        let inits = match pod["spec"]["initContainers"].as_array() {
            Some(a) if !a.is_empty() => a.clone(),
            _ => return Ok(()),
        };
        let ns = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pod["metadata"]["name"].as_str().unwrap_or("");

        for spec in &inits {
            let cname = spec["name"].as_str().unwrap_or("init");
            let image = spec["image"].as_str().unwrap_or("");
            info!("Init container {ns}/{name}/{cname}: ensuring image {image}");
            let image_ref = self.ensure_image(image, spec).await?;
            let mut envs = self.resolve_env(pod, spec, pod_ip).await;
            merge_env(&mut envs, self.service_account_env());
            let mut mounts = resolve_mounts(spec, volumes);
            push_mount(&mut mounts, sa_mount.clone());
            let config = build_container_config(spec, &image_ref, envs, mounts);
            let cid = self
                .runtime
                .create_container(sandbox_id, &config, sandbox_config)
                .await?;
            self.runtime.start_container(&cid).await?;
            info!("Init container {ns}/{name}/{cname} started, waiting for completion");

            // Poll until the init container exits (bounded).
            let mut waited = 0u64;
            const POLL_MS: u64 = 500;
            const MAX_WAIT_MS: u64 = 120_000;
            loop {
                let status = self.runtime.container_status(&cid).await?;
                match status.state {
                    ContainerState::Exited => {
                        if status.exit_code != 0 {
                            let _ = self.runtime.remove_container(&cid).await;
                            return Err(CriError::Runtime(format!(
                                "init container {cname} exited with code {}",
                                status.exit_code
                            )));
                        }
                        info!("Init container {ns}/{name}/{cname} completed");
                        let _ = self.runtime.remove_container(&cid).await;
                        break;
                    }
                    _ => {
                        if waited >= MAX_WAIT_MS {
                            let _ = self.runtime.stop_container(&cid, 5).await;
                            let _ = self.runtime.remove_container(&cid).await;
                            return Err(CriError::Timeout);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(POLL_MS)).await;
                        waited += POLL_MS;
                    }
                }
            }
        }
        Ok(())
    }

    /// Ensure a container image is available per its `imagePullPolicy`:
    /// Always pulls; IfNotPresent pulls only if absent; Never fails if absent.
    async fn ensure_image(&self, image: &str, spec: &Value) -> Result<String, CriError> {
        match effective_pull_policy(spec, image) {
            "Never" => match self.images.image_status(image).await? {
                Some(info) => Ok(image_present_ref(&info, image)),
                None => Err(CriError::ImagePull(format!(
                    "image {image} not present and imagePullPolicy is Never"
                ))),
            },
            "IfNotPresent" => match self.images.image_status(image).await {
                Ok(Some(info)) => Ok(image_present_ref(&info, image)),
                _ => self.images.pull_image(image).await,
            },
            _ => self.images.pull_image(image).await, // Always
        }
    }

    async fn start_pod(&self, pod: &Value) -> Result<PodStatusUpdate, CriError> {
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

        info!("Starting pod {namespace}/{name}");

        let sandbox_config = build_sandbox_config(pod);
        let volumes = self.resolve_volumes(pod).await;

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

        // Default ServiceAccount credential mount (token/ca/namespace),
        // computed once per pod and injected into every container.
        let sa_mount = self.service_account_mount(pod).await;

        // Run init containers to completion (in order) before the app
        // containers — each must exit 0. A failure aborts pod start.
        self.run_init_containers(
            pod,
            &sandbox_id,
            &sandbox_config,
            &volumes,
            pod_ip.as_deref(),
            &sa_mount,
        )
        .await?;

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

            // Ensure image per imagePullPolicy
            info!("Ensuring image {image} for {namespace}/{name}/{container_name}");
            let image_ref = self.ensure_image(image, container_spec).await?;

            // Build container config (resolve env valueFrom + mounts, then
            // inject the SA credential mount + KUBERNETES_SERVICE_* env).
            let mut envs = self.resolve_env(pod, container_spec, pod_ip.as_deref()).await;
            merge_env(&mut envs, self.service_account_env());
            let mut mounts = resolve_mounts(container_spec, &volumes);
            push_mount(&mut mounts, sa_mount.clone());
            let container_config =
                build_container_config(container_spec, &image_ref, envs, mounts);

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
        let volumes = self.resolve_volumes(&state.pod).await;
        let mut envs = self
            .resolve_env(&state.pod, spec, state.pod_ip.as_deref())
            .await;
        merge_env(&mut envs, self.service_account_env());
        let mut mounts = resolve_mounts(spec, &volumes);
        push_mount(&mut mounts, self.service_account_mount(&state.pod).await);
        let result = async move {
            let image_ref = self.ensure_image(image, spec).await?;
            let mut config = build_container_config(spec, &image_ref, envs, mounts);
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
    /// Per-container resource stats from the runtime (for /metrics/cadvisor
    /// and /stats/summary).
    pub async fn container_stats(&self) -> Vec<crate::cri::ContainerStatsInfo> {
        self.runtime.list_container_stats().await.unwrap_or_default()
    }

    /// (managed pod count, total container count) for the /metrics endpoint.
    pub async fn metrics_snapshot(&self) -> (usize, usize) {
        let pods = self.pods.read().await;
        let containers = pods.values().map(|p| p.container_ids.len()).sum();
        (pods.len(), containers)
    }

    /// A v1 PodList of the pods this kubelet manages (for the /pods endpoint).
    pub async fn pods_json(&self) -> Value {
        let pods = self.pods.read().await;
        let items: Vec<Value> = pods
            .values()
            .map(|p| {
                serde_json::json!({
                    "metadata": {"name": p.name, "namespace": p.namespace, "uid": p.uid},
                    "status": {
                        "phase": p.phase,
                        "podIP": p.pod_ip,
                    }
                })
            })
            .collect();
        serde_json::json!({"kind": "PodList", "apiVersion": "v1", "items": items})
    }

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

/// Per-pod volume directory: <state_root>/pods/<uid>/volumes/kubernetes.io~<kind>/<name>.
fn pod_volume_dir(state_root: &str, uid: &str, kind: &str, name: &str) -> String {
    format!("{state_root}/pods/{uid}/volumes/kubernetes.io~{kind}/{name}")
}

/// Standard in-cluster ServiceAccount credential mount path.
const SA_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Effective imagePullPolicy for a container: explicit value, else the K8s
/// default (Always for an untagged or `:latest` image, IfNotPresent otherwise).
fn effective_pull_policy(spec: &Value, image: &str) -> &'static str {
    match spec["imagePullPolicy"].as_str() {
        Some("Always") => "Always",
        Some("Never") => "Never",
        Some("IfNotPresent") => "IfNotPresent",
        _ => {
            let after_host = image.rsplit_once('/').map(|(_, r)| r).unwrap_or(image);
            if !after_host.contains(':') || after_host.ends_with(":latest") {
                "Always"
            } else {
                "IfNotPresent"
            }
        }
    }
}

/// A reference for an already-present image: prefer a repo digest, then the
/// image id, then the requested name.
fn image_present_ref(info: &ImageInfo, image: &str) -> String {
    info.repo_digests
        .first()
        .cloned()
        .filter(|s| !s.is_empty())
        .or_else(|| (!info.id.is_empty()).then(|| info.id.clone()))
        .unwrap_or_else(|| image.to_string())
}

/// Whether any container already mounts the SA credential path (i.e. the
/// apiserver's SA admission injected a `kube-api-access` projected volume).
fn pod_mounts_sa_path(pod: &Value) -> bool {
    for key in ["containers", "initContainers"] {
        if let Some(cs) = pod["spec"][key].as_array() {
            for c in cs {
                if let Some(vm) = c["volumeMounts"].as_array() {
                    if vm
                        .iter()
                        .any(|m| m["mountPath"].as_str() == Some(SA_MOUNT_PATH))
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Parse host + port from the apiserver URL (`http://host:port` → ("host","port")).
fn apiserver_host_port(api_url: &str) -> (String, String) {
    let rest = api_url
        .strip_prefix("https://")
        .or_else(|| api_url.strip_prefix("http://"))
        .unwrap_or(api_url);
    let authority = rest.split('/').next().unwrap_or(rest);
    match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.to_string()),
        None => (authority.to_string(), "6443".to_string()),
    }
}

/// Append `additions` env pairs whose keys aren't already present in `envs`.
fn merge_env(envs: &mut Vec<(String, String)>, additions: Vec<(String, String)>) {
    for (k, v) in additions {
        if !envs.iter().any(|(ek, _)| ek == &k) {
            envs.push((k, v));
        }
    }
}

/// Append `mount` unless a mount for the same container_path already exists.
fn push_mount(mounts: &mut Vec<Mount>, mount: Option<Mount>) {
    if let Some(m) = mount {
        if !mounts.iter().any(|x| x.container_path == m.container_path) {
            mounts.push(m);
        }
    }
}

/// Write projected configMap/secret source data into `dir`. With `items`,
/// only the listed keys are written at their `path`; without, every key is
/// written at its own name.
fn write_projected_items(
    dir: &str,
    items: Option<&Vec<Value>>,
    data: Option<&serde_json::Map<String, Value>>,
) {
    let data = match data {
        Some(d) => d,
        None => return,
    };
    match items {
        Some(items) => {
            for it in items {
                let key = it["key"].as_str().unwrap_or("");
                let path = it["path"].as_str().unwrap_or(key);
                if let Some(s) = data.get(key).and_then(|v| v.as_str()) {
                    let _ = std::fs::write(format!("{dir}/{path}"), s);
                }
            }
        }
        None => {
            for (key, val) in data {
                if let Some(s) = val.as_str() {
                    let _ = std::fs::write(format!("{dir}/{key}"), s);
                }
            }
        }
    }
}

/// Write each ConfigMap data entry as a file `<dir>/<key>` (0644).
fn materialize_files(dir: &str, data: Option<&serde_json::Map<String, Value>>, _binary: bool) {
    let _ = std::fs::create_dir_all(dir);
    if let Some(data) = data {
        for (key, val) in data {
            if let Some(s) = val.as_str() {
                let _ = std::fs::write(format!("{dir}/{key}"), s);
            }
        }
    }
}

/// Write decoded Secret entries as files `<dir>/<key>` (0600 best-effort).
fn materialize_secret_files(dir: &str, data: Option<&HashMap<String, String>>) {
    let _ = std::fs::create_dir_all(dir);
    if let Some(data) = data {
        for (key, val) in data {
            let path = format!("{dir}/{key}");
            let _ = std::fs::write(&path, val);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
}

/// Minimal standard base64 decode (Secret data is standard-alphabet base64).
fn base64_decode(s: &str) -> Result<Vec<u8>, ()> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0u8;
    for &c in s.as_bytes() {
        if c == b'=' || c == b'\n' || c == b'\r' {
            continue;
        }
        let v = val(c).ok_or(())?;
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

/// Resolve a container's `volumeMounts` to CRI mounts using the pod's resolved
/// volumes. Mounts whose volume didn't resolve are dropped.
fn resolve_mounts(spec: &Value, volumes: &HashMap<String, String>) -> Vec<Mount> {
    spec["volumeMounts"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| {
                    let vol_name = m["name"].as_str().unwrap_or("");
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
        .unwrap_or_default()
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

/// Build the CRI container config from the spec, with env and mounts already
/// resolved (env `valueFrom` + configMap/secret volumes need async apiserver
/// reads, done by the caller).
fn build_container_config(
    spec: &Value,
    image_ref: &str,
    envs: Vec<(String, String)>,
    mounts: Vec<Mount>,
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

    let sc = &spec["securityContext"];
    let add_capabilities: Vec<String> = sc["capabilities"]["add"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    // cgroup semantics: cpu.shares from the CPU *request* (relative weight);
    // the cpu quota (hard cap) and memory limit from *limits* (0 = unlimited).
    let cpu_request = spec["resources"]["requests"]["cpu"].as_str().unwrap_or("0");
    let cpu_limit = spec["resources"]["limits"]["cpu"].as_str().unwrap_or("0");
    let mem_limit = spec["resources"]["limits"]["memory"].as_str().unwrap_or("0");

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
        cpu_quota: parse_cpu_quota(cpu_limit),
        cpu_shares: parse_cpu_shares(cpu_request),
        memory_limit_bytes: parse_memory_bytes(mem_limit),
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
        PodSandboxSummary,
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
        sandboxes: Mutex<HashMap<String, (PodSandboxState, PodSandboxConfig)>>,
        containers: Mutex<HashMap<String, FakeContainer>>,
        next_id: AtomicU32,
        /// Exit code exec_sync returns (probes).
        exec_exit_code: Mutex<i32>,
        /// Container names that exit immediately on start (init containers) → code.
        exit_on_start: Mutex<HashMap<String, i32>>,
        /// The most recent ContainerConfig passed to create_container.
        last_container_config: Mutex<Option<ContainerConfig>>,
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

        /// Make a container (by name) exit with `code` immediately when started.
        fn set_exit_on_start(&self, name: &str, code: i32) {
            self.exit_on_start.lock().unwrap().insert(name.to_string(), code);
        }

        fn created_names(&self) -> Vec<String> {
            let mut v: Vec<String> = self
                .containers
                .lock()
                .unwrap()
                .values()
                .map(|c| c.name.clone())
                .collect();
            v.sort();
            v
        }

        fn container_ids(&self) -> Vec<String> {
            self.containers.lock().unwrap().keys().cloned().collect()
        }

        fn last_container_config(&self) -> Option<ContainerConfig> {
            self.last_container_config.lock().unwrap().clone()
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
                .insert(id.clone(), (PodSandboxState::Ready, config.clone()));
            Ok(id)
        }

        async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            if let Some(entry) = self.sandboxes.lock().unwrap().get_mut(sandbox_id) {
                entry.0 = PodSandboxState::NotReady;
            }
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

        async fn list_pod_sandbox(&self) -> Result<Vec<PodSandboxSummary>, CriError> {
            Ok(self
                .sandboxes
                .lock()
                .unwrap()
                .iter()
                .map(|(k, (state, cfg))| PodSandboxSummary {
                    id: k.clone(),
                    state: *state,
                    uid: cfg.uid.clone(),
                    name: cfg.name.clone(),
                    namespace: cfg.namespace.clone(),
                })
                .collect())
        }

        async fn create_container(
            &self,
            _sandbox_id: &str,
            config: &ContainerConfig,
            _sandbox_config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            *self.last_container_config.lock().unwrap() = Some(config.clone());
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
            // Init containers exit immediately on start when configured to.
            if let Some(&code) = self.exit_on_start.lock().unwrap().get(&c.name) {
                c.state = ContainerState::Exited;
                c.exit_code = code;
            } else {
                c.state = ContainerState::Running;
            }
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
        let mut mgr = PodManager::new(rt.clone(), rt.clone(), NODE);
        // Write pod volume dirs under a writable temp root in tests.
        let tmp = std::env::temp_dir().join(format!("rk-kubelet-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        mgr.state_root = tmp.to_string_lossy().into_owned();
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

    #[tokio::test]
    async fn init_container_runs_before_app_and_success_starts_pod() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("setup", 0); // init container exits 0
        let mut p = pod("uid-1", "web", "Always", simple_container());
        p["spec"]["initContainers"] = json!([{"name": "setup", "image": "busybox:latest"}]);

        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates.len(), 1);
        assert_eq!(outcome.updates[0].phase, "Running");
        // App container is created; init container was removed after completing.
        assert_eq!(rt.created_names(), vec!["app".to_string()]);
        assert!(rt.removed_containers.lock().unwrap().iter().any(|_| true));
    }

    #[tokio::test]
    async fn init_container_failure_fails_pod() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("setup", 1); // init container exits non-zero
        let mut p = pod("uid-1", "web", "Always", simple_container());
        p["spec"]["initContainers"] = json!([{"name": "setup", "image": "busybox:latest"}]);

        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].phase, "Failed");
        // App container never created because init failed.
        assert!(!rt.created_names().contains(&"app".to_string()));
    }

    #[tokio::test]
    async fn projected_volume_writes_configmap_and_downward_files() {
        // No apiserver → SA token/configMap fetches are skipped, but the
        // downwardAPI source is materialized from pod context.
        let (_rt, mgr) = manager();
        let mut p = pod("uid-5", "web", "Always", simple_container());
        p["spec"]["volumes"] = json!([{
            "name": "kube-api-access",
            "projected": {"sources": [
                {"downwardAPI": {"items": [
                    {"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}
                ]}}
            ]}
        }]);
        let v = mgr.resolve_volumes(&p).await;
        let dir = v.get("kube-api-access").expect("projected volume resolved");
        assert!(dir.contains("kubernetes.io~projected"));
        // The downward file was written with the namespace.
        let content = std::fs::read_to_string(format!("{dir}/namespace")).unwrap_or_default();
        assert_eq!(content, "default");
    }

    #[test]
    fn pull_policy_defaults_and_explicit() {
        // Explicit wins.
        assert_eq!(effective_pull_policy(&json!({"imagePullPolicy": "Never"}), "x:1"), "Never");
        // Default: :latest / untagged → Always; pinned tag → IfNotPresent.
        assert_eq!(effective_pull_policy(&json!({}), "busybox:latest"), "Always");
        assert_eq!(effective_pull_policy(&json!({}), "busybox"), "Always");
        assert_eq!(effective_pull_policy(&json!({}), "busybox:1.36"), "IfNotPresent");
        // A port in the registry host must not be mistaken for a tag.
        assert_eq!(effective_pull_policy(&json!({}), "reg:5000/busybox:1.36"), "IfNotPresent");
        assert_eq!(effective_pull_policy(&json!({}), "reg:5000/busybox"), "Always");
    }

    #[test]
    fn apiserver_host_port_parsing() {
        assert_eq!(
            apiserver_host_port("http://192.168.8.98:6443"),
            ("192.168.8.98".to_string(), "6443".to_string())
        );
        assert_eq!(
            apiserver_host_port("https://api.example.com:443/foo"),
            ("api.example.com".to_string(), "443".to_string())
        );
    }

    #[test]
    fn pod_mounts_sa_path_detects_projected_mount() {
        let with = json!({"spec": {"containers": [
            {"name": "c", "volumeMounts": [{"name": "kube-api-access", "mountPath": SA_MOUNT_PATH}]}
        ]}});
        let without = json!({"spec": {"containers": [
            {"name": "c", "volumeMounts": [{"name": "data", "mountPath": "/data"}]}
        ]}});
        assert!(pod_mounts_sa_path(&with));
        assert!(!pod_mounts_sa_path(&without));
    }

    #[tokio::test]
    async fn service_account_mount_default_and_opt_out() {
        let (_rt, mgr) = manager(); // no apiserver → token skipped, but ns written
        // Default pod: gets an SA mount at the standard path.
        let p = pod("uid-2", "web", "Always", simple_container());
        let m = mgr.service_account_mount(&p).await.expect("sa mount");
        assert_eq!(m.container_path, SA_MOUNT_PATH);
        assert!(m.readonly);
        assert_eq!(
            std::fs::read_to_string(format!("{}/namespace", m.host_path)).unwrap_or_default(),
            "default"
        );
        // automountServiceAccountToken: false → no mount.
        let mut off = pod("uid-3", "web", "Always", simple_container());
        off["spec"]["automountServiceAccountToken"] = json!(false);
        assert!(mgr.service_account_mount(&off).await.is_none());
    }

    #[tokio::test]
    async fn service_account_env_injected_into_container() {
        let (rt, mgr) = manager();
        let p = pod("uid-4", "web", "Always", simple_container());
        mgr.sync_pods(&[p]).await;
        // The created app container has the KUBERNETES_SERVICE_* env + SA mount.
        let cfg = rt.last_container_config().expect("a container was created");
        assert!(cfg
            .envs
            .iter()
            .any(|(k, _)| k == "KUBERNETES_SERVICE_HOST"));
        assert!(cfg.mounts.iter().any(|m| m.container_path == SA_MOUNT_PATH));
    }

    #[test]
    fn detect_node_name_prefers_env() {
        // NODE_NAME wins when set; the function never returns empty.
        std::env::set_var("NODE_NAME", "explicit-node");
        assert_eq!(crate::kubelet::detect_node_name(), "explicit-node");
        std::env::remove_var("NODE_NAME");
        assert!(!crate::kubelet::detect_node_name().is_empty());
    }

    #[tokio::test]
    async fn resolve_volumes_maps_hostpath_and_emptydir() {
        let (_rt, mgr) = manager();
        let p = json!({
            "metadata": {"uid": "uid-9"},
            "spec": {"volumes": [
                {"name": "bpf", "hostPath": {"path": "/sys/fs/bpf", "type": "DirectoryOrCreate"}},
                {"name": "scratch", "emptyDir": {}}
            ]}
        });
        let v = mgr.resolve_volumes(&p).await;
        assert_eq!(v.get("bpf").map(String::as_str), Some("/sys/fs/bpf"));
        assert!(v
            .get("scratch")
            .unwrap()
            .ends_with("/pods/uid-9/volumes/kubernetes.io~empty-dir/scratch"));
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
        let mounts = resolve_mounts(&spec, &volumes);
        let c = build_container_config(&spec, "cilium@sha", vec![], mounts);
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
    fn resources_shares_from_request_quota_and_memory_from_limits() {
        let spec = json!({
            "name": "app", "image": "x",
            "resources": {
                "requests": {"cpu": "250m", "memory": "64Mi"},
                "limits": {"cpu": "500m", "memory": "128Mi"}
            }
        });
        let c = build_container_config(&spec, "x", vec![], vec![]);
        // shares from request (250m → ~256), quota from limit (500m → 50000
        // with 100000 period), memory limit from limit (128Mi).
        assert_eq!(c.cpu_shares, 256);
        assert_eq!(c.cpu_quota, 50_000);
        assert_eq!(c.memory_limit_bytes, 128 * 1024 * 1024);
        // No limits → unlimited (quota 0, memory 0), shares still from request.
        let spec2 = json!({"name": "app", "image": "x",
            "resources": {"requests": {"cpu": "100m"}}});
        let c2 = build_container_config(&spec2, "x", vec![], vec![]);
        assert_eq!(c2.cpu_quota, 0);
        assert_eq!(c2.memory_limit_bytes, 0);
    }

    #[test]
    fn container_config_defaults_are_unprivileged() {
        let c = build_container_config(&simple_container(), "img", vec![], vec![]);
        assert!(!c.privileged);
        assert!(c.add_capabilities.is_empty());
        assert!(c.mounts.is_empty());
    }

    #[tokio::test]
    async fn resolve_env_literal_and_downward_api() {
        let (_rt, mgr) = manager(); // node_name = "test-node", no apiserver
        let mut p = pod("uid-7", "web", "Always", simple_container());
        p["metadata"]["labels"]["tier"] = json!("frontend");
        let container = json!({
            "name": "app",
            "env": [
                {"name": "LITERAL", "value": "hi"},
                {"name": "NODE", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
                {"name": "NS", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
                {"name": "TIER", "valueFrom": {"fieldRef": {"fieldPath": "metadata.labels['tier']"}}}
            ]
        });
        let env = mgr.resolve_env(&p, &container, Some("10.1.2.3")).await;
        let m: HashMap<_, _> = env.into_iter().collect();
        assert_eq!(m.get("LITERAL").map(String::as_str), Some("hi"));
        assert_eq!(m.get("NODE").map(String::as_str), Some(NODE));
        assert_eq!(m.get("NS").map(String::as_str), Some("default"));
        assert_eq!(m.get("POD_IP").map(String::as_str), Some("10.1.2.3"));
        assert_eq!(m.get("TIER").map(String::as_str), Some("frontend"));
    }

    #[test]
    fn base64_decode_roundtrip() {
        // "hunter2" base64 == "aHVudGVyMg=="
        assert_eq!(base64_decode("aHVudGVyMg==").unwrap(), b"hunter2");
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
    async fn recover_state_adopts_running_sandbox_no_double_create() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        // First "boot": start the pod — one sandbox created.
        mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(rt.live_sandbox_count(), 1);

        // Simulate a kubelet restart: fresh manager, same runtime with the
        // sandbox still running.
        let mgr2 = PodManager::new(rt.clone(), rt.clone(), NODE);
        mgr2.recover_state().await;
        // The running pod is adopted, so a re-sync does NOT create a 2nd sandbox.
        let outcome = mgr2.sync_pods(&[p]).await;
        assert_eq!(rt.live_sandbox_count(), 1, "must not double-create sandbox");
        assert_eq!(outcome.updates[0].phase, "Running");
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
