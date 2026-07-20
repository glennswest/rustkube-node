//! Kubelet — the main node agent loop.
//!
//! Registers the node, sends heartbeats, syncs pods, runs probes.

use crate::cri::{ImageService, MigrationService, RuntimeService};
use crate::node_status::NodeReporter;
use crate::pod_manager::{PodManager, RemovalReason};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

/// Kubelet configuration.
#[derive(Debug, Clone)]
pub struct KubeletConfig {
    pub node_name: String,
    pub api_server_url: String,
    /// Pod CIDR for this node, written to `spec.podCIDR` when set.
    pub pod_cidr: Option<String>,
    pub heartbeat_interval: Duration,
    pub sync_interval: Duration,
    /// Port for the kubelet's inbound HTTP server (upstream 10250).
    pub kubelet_port: u16,
    /// Cluster CA (PEM) to trust for an HTTPS apiserver. None → no custom root.
    pub apiserver_ca: Option<Vec<u8>>,
    /// Bearer token for authenticating to the apiserver (SA/JWT). None → none.
    pub bearer_token: Option<String>,
    /// Client certificate chain (PEM) for mutual-TLS node auth. None → none.
    pub client_cert: Option<Vec<u8>>,
    /// Private key (PEM) for `client_cert`. None → none.
    pub client_key: Option<Vec<u8>>,
    /// Skip apiserver cert verification (dev only).
    pub insecure_skip_tls_verify: bool,
    /// Inbound `:10250` serving cert + key (PEM). None → self-signed at startup.
    pub serving_cert: Option<Vec<u8>>,
    pub serving_key: Option<Vec<u8>>,
    /// Static bearer token accepted by the inbound server (e.g. for monitoring).
    pub server_auth_token: Option<String>,
    /// Serve the inbound `:10250` endpoints unauthenticated (dev only).
    pub anonymous_auth: bool,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: hostname(),
            api_server_url: "http://localhost:6443".into(),
            pod_cidr: None,
            heartbeat_interval: Duration::from_secs(10),
            sync_interval: Duration::from_secs(2),
            kubelet_port: 10250,
            apiserver_ca: None,
            bearer_token: None,
            client_cert: None,
            client_key: None,
            insecure_skip_tls_verify: false,
            serving_cert: None,
            serving_key: None,
            server_auth_token: None,
            anonymous_auth: false,
        }
    }
}

/// The kubelet node agent.
pub struct Kubelet {
    config: KubeletConfig,
    pod_manager: Arc<PodManager>,
    migration: Arc<dyn MigrationService>,
    runtime: Arc<dyn RuntimeService>,
    api_client: reqwest::Client,
    node_ip: String,
}

impl Kubelet {
    pub fn new(
        config: KubeletConfig,
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        migration: Arc<dyn MigrationService>,
    ) -> anyhow::Result<Self> {
        let node_ip = crate::node_status::detect_node_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string());

        // One authenticated apiserver client (HTTPS CA + bearer token when
        // configured), shared by the pod manager, node reporter, and kubelet.
        // A build failure is fatal — proceeding with a silently-degraded client
        // would fail every apiserver call with an opaque transport error
        // (rustkube-node#16).
        let api_client = crate::client::build_authed_client(&crate::client::ClientAuth {
            ca_pem: config.apiserver_ca.as_deref(),
            token: config.bearer_token.as_deref(),
            client_cert_pem: config.client_cert.as_deref(),
            client_key_pem: config.client_key.as_deref(),
            insecure_skip_tls_verify: config.insecure_skip_tls_verify,
        })?;

        let pod_manager = Arc::new(
            PodManager::with_api(
                runtime.clone(),
                images,
                &config.node_name,
                &config.api_server_url,
                &node_ip,
                api_client.clone(),
            )
            .with_ca_pem(config.apiserver_ca.clone()),
        );

        Ok(Self {
            config,
            pod_manager,
            migration,
            runtime,
            api_client,
            node_ip,
        })
    }

    /// Run the kubelet. Blocks forever.
    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Kubelet starting for node {}", self.config.node_name);

        // Query the container runtime version for nodeInfo (e.g. cri-o://1.32.0).
        let runtime_version = match self.runtime.version().await {
            Ok((name, version, _)) => format!("{name}://{version}"),
            Err(e) => {
                warn!("Could not get runtime version: {e}");
                "cri-o://unknown".to_string()
            }
        };

        // Register node
        let reporter = NodeReporter::with_pod_cidr(
            &self.config.api_server_url,
            &self.config.node_name,
            self.config.pod_cidr.clone(),
        )
        .with_runtime_version(runtime_version.clone())
        .with_kubelet_port(self.config.kubelet_port)
        .with_client(self.api_client.clone());
        // Retry registration rather than exiting — the apiserver may be
        // briefly unreachable or the node's RBAC not yet in place. The kubelet
        // must not crash-loop out of the cluster over a transient failure.
        let mut backoff = Duration::from_secs(1);
        loop {
            match reporter.register().await {
                Ok(()) => break,
                Err(e) => {
                    warn!("Node registration failed ({e}); retrying in {backoff:?}");
                    time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }

        // Adopt pods already running in the runtime (e.g. after a kubelet
        // restart) so we reconcile rather than double-create sandboxes.
        self.pod_manager.recover_state().await;

        // Inbound kubelet HTTPS server (:10250) — /healthz, /metrics, /pods.
        {
            let pm = self.pod_manager.clone();
            let port = self.config.kubelet_port;
            let server_config = crate::server::ServerConfig {
                tls_cert: self.config.serving_cert.clone(),
                tls_key: self.config.serving_key.clone(),
                node_name: self.config.node_name.clone(),
                node_ip: self.node_ip.clone(),
                auth_token: self.config.server_auth_token.clone(),
                api_client: self.api_client.clone(),
                api_url: self.config.api_server_url.clone(),
                anonymous: self.config.anonymous_auth,
            };
            tokio::spawn(async move { crate::server::serve(port, pm, server_config).await });
        }

        // Spawn heartbeat task
        let reporter_url = self.config.api_server_url.clone();
        let node_name = self.config.node_name.clone();
        let pod_cidr = self.config.pod_cidr.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let kubelet_port = self.config.kubelet_port;
        let hb_client = self.api_client.clone();
        tokio::spawn(async move {
            let reporter = NodeReporter::with_pod_cidr(&reporter_url, &node_name, pod_cidr)
                .with_runtime_version(runtime_version)
                .with_kubelet_port(kubelet_port)
                .with_client(hb_client);
            let mut interval = time::interval(heartbeat_interval);
            loop {
                interval.tick().await;
                if let Err(e) = reporter.heartbeat().await {
                    error!("Heartbeat failed: {e}");
                }
            }
        });

        // Main sync loop
        let mut interval = time::interval(self.config.sync_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.sync().await {
                error!("Pod sync failed: {e}");
            }
        }
    }

    /// Sync pods: fetch desired pods from API server, reconcile with actual.
    async fn sync(&self) -> anyhow::Result<()> {
        // List all pods across all namespaces
        let resp: Value = self
            .api_client
            .get(format!("{}/api/v1/pods", self.config.api_server_url))
            .send()
            .await?
            .json()
            .await?;

        let pods = resp["items"].as_array().cloned().unwrap_or_default();

        // Filter to pods scheduled to this node
        let my_pods: Vec<Value> = pods
            .into_iter()
            .filter(|p| {
                p["spec"]["nodeName"].as_str() == Some(&self.config.node_name)
            })
            .collect();

        // Sync pod states
        let outcome = self.pod_manager.sync_pods(&my_pods).await;

        // Report status updates back to API server
        for update in &outcome.updates {
            if let Err(e) = self.report_pod_status(update).await {
                error!(
                    "Failed to report status for {}/{}: {e}",
                    update.namespace, update.name
                );
            }
        }

        // Confirm terminating pods: containers are down, so force-delete the
        // pod object to finish the API-side deletion. Orphaned pods are already
        // gone from the API server — nothing to confirm.
        for removed in &outcome.removed {
            if removed.reason != RemovalReason::Deleting {
                continue;
            }
            let path = format!(
                "{}/api/v1/namespaces/{}/pods/{}?gracePeriodSeconds=0",
                self.config.api_server_url, removed.namespace, removed.name
            );
            match self.api_client.delete(&path).send().await {
                Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 404 => {
                    info!(
                        "Confirmed deletion of pod {}/{}",
                        removed.namespace, removed.name
                    );
                }
                Ok(resp) => {
                    warn!(
                        "Failed to confirm deletion of {}/{}: HTTP {}",
                        removed.namespace,
                        removed.name,
                        resp.status()
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to confirm deletion of {}/{}: {e}",
                        removed.namespace, removed.name
                    );
                }
            }
        }

        // Handle migration annotations on pods assigned to this node
        for pod in &my_pods {
            if let Err(e) = self.handle_migration_annotations(pod).await {
                let name = pod["metadata"]["name"].as_str().unwrap_or("?");
                let ns = pod["metadata"]["namespace"].as_str().unwrap_or("?");
                warn!("Migration annotation handling failed for {ns}/{name}: {e}");
            }
        }

        Ok(())
    }

    /// Handle migration-related annotations on pods.
    async fn handle_migration_annotations(&self, pod: &Value) -> anyhow::Result<()> {
        let action = match pod["metadata"]["annotations"]["rustkube.io/migrate-action"].as_str() {
            Some(a) => a.to_string(),
            None => return Ok(()), // No migration action
        };

        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

        // Get sandbox ID from pod manager
        let sandbox_id = self.pod_manager.get_sandbox_id(uid).await;

        match action.as_str() {
            "checkpoint" => {
                let sandbox_id = sandbox_id
                    .ok_or_else(|| anyhow::anyhow!("no sandbox for pod {namespace}/{name}"))?;

                info!("Migration: checkpointing pod {namespace}/{name} (sandbox={sandbox_id})");
                match self.migration.checkpoint_pod(&sandbox_id).await {
                    Ok(checkpoint_ref) => {
                        let ref_json = serde_json::to_string(&checkpoint_ref)?;
                        // Write checkpoint ref back as annotation
                        let _ = self
                            .api_client
                            .patch(format!(
                                "{}/api/v1/namespaces/{namespace}/pods/{name}",
                                self.config.api_server_url
                            ))
                            .header("content-type", "application/strategic-merge-patch+json")
                            .json(&serde_json::json!({
                                "metadata": {
                                    "annotations": {
                                        "rustkube.io/checkpoint-ref": ref_json,
                                        "rustkube.io/migrate-action": "checkpoint-done",
                                    }
                                }
                            }))
                            .send()
                            .await;
                        info!("Migration: checkpoint complete for {namespace}/{name}");
                    }
                    Err(e) => {
                        warn!("Migration: checkpoint failed for {namespace}/{name}: {e}");
                        let _ = self
                            .api_client
                            .patch(format!(
                                "{}/api/v1/namespaces/{namespace}/pods/{name}",
                                self.config.api_server_url
                            ))
                            .header("content-type", "application/strategic-merge-patch+json")
                            .json(&serde_json::json!({
                                "metadata": {
                                    "annotations": {
                                        "rustkube.io/migrate-action": "checkpoint-failed",
                                        "rustkube.io/migrate-error": e.to_string(),
                                    }
                                }
                            }))
                            .send()
                            .await;
                    }
                }
            }
            "prepare-target" => {
                // This node is the target — prepare to receive a migration
                info!("Migration: preparing target for pod {namespace}/{name}");
                let config = crate::cri::PodSandboxConfig {
                    name: name.to_string(),
                    uid: uid.to_string(),
                    namespace: namespace.to_string(),
                    attempt: 0,
                    hostname: name.to_string(),
                    log_directory: format!("/var/log/pods/{namespace}_{name}_{uid}"),
                    dns_servers: vec!["10.96.0.10".to_string()],
                    dns_searches: vec![
                        format!("{namespace}.svc.cluster.local"),
                        "svc.cluster.local".to_string(),
                        "cluster.local".to_string(),
                    ],
                    labels: std::collections::HashMap::new(),
                    annotations: std::collections::HashMap::new(),
                    port_mappings: vec![],
                    ..Default::default()
                };

                match self.migration.prepare_migration_target(&config).await {
                    Ok(endpoint) => {
                        let _ = self
                            .api_client
                            .patch(format!(
                                "{}/api/v1/namespaces/{namespace}/pods/{name}",
                                self.config.api_server_url
                            ))
                            .header("content-type", "application/strategic-merge-patch+json")
                            .json(&serde_json::json!({
                                "metadata": {
                                    "annotations": {
                                        "rustkube.io/migration-endpoint": endpoint,
                                        "rustkube.io/migrate-action": "target-ready",
                                    }
                                }
                            }))
                            .send()
                            .await;
                        info!("Migration: target ready at {endpoint}");
                    }
                    Err(e) => {
                        warn!("Migration: prepare target failed: {e}");
                    }
                }
            }
            "live-migrate" => {
                let sandbox_id = sandbox_id
                    .ok_or_else(|| anyhow::anyhow!("no sandbox for pod {namespace}/{name}"))?;

                let target_endpoint = pod["metadata"]["annotations"]
                    ["rustkube.io/migration-target-endpoint"]
                    .as_str()
                    .unwrap_or("");

                info!(
                    "Migration: live-migrating pod {namespace}/{name} to {target_endpoint}"
                );
                match self
                    .migration
                    .live_migrate(&sandbox_id, target_endpoint)
                    .await
                {
                    Ok(()) => {
                        let _ = self
                            .api_client
                            .patch(format!(
                                "{}/api/v1/namespaces/{namespace}/pods/{name}",
                                self.config.api_server_url
                            ))
                            .header("content-type", "application/strategic-merge-patch+json")
                            .json(&serde_json::json!({
                                "metadata": {
                                    "annotations": {
                                        "rustkube.io/migrate-action": "migrate-done",
                                    }
                                }
                            }))
                            .send()
                            .await;
                        info!("Migration: live migration complete for {namespace}/{name}");
                    }
                    Err(e) => {
                        warn!("Migration: live migration failed: {e}");
                    }
                }
            }
            "restore-from" => {
                // Restore pod from checkpoint on this node
                let checkpoint_ref_json = pod["metadata"]["annotations"]
                    ["rustkube.io/checkpoint-ref"]
                    .as_str()
                    .unwrap_or("{}");

                info!("Migration: restoring pod {namespace}/{name} from checkpoint");
                if let Ok(checkpoint_ref) = serde_json::from_str(checkpoint_ref_json) {
                    let config = crate::cri::PodSandboxConfig {
                        name: name.to_string(),
                        uid: uid.to_string(),
                        namespace: namespace.to_string(),
                        attempt: 0,
                        hostname: name.to_string(),
                        log_directory: format!("/var/log/pods/{namespace}_{name}_{uid}"),
                        dns_servers: vec!["10.96.0.10".to_string()],
                        dns_searches: vec![
                            format!("{namespace}.svc.cluster.local"),
                            "svc.cluster.local".to_string(),
                            "cluster.local".to_string(),
                        ],
                        labels: std::collections::HashMap::new(),
                        annotations: std::collections::HashMap::new(),
                        port_mappings: vec![],
                        ..Default::default()
                    };

                    match self.migration.restore_pod(&checkpoint_ref, &config).await {
                        Ok(sandbox_id) => {
                            self.pod_manager.register_restored_pod(
                                uid,
                                namespace,
                                name,
                                &sandbox_id,
                            ).await;
                            info!(
                                "Migration: pod {namespace}/{name} restored (sandbox={sandbox_id})"
                            );
                        }
                        Err(e) => {
                            warn!("Migration: restore failed for {namespace}/{name}: {e}");
                        }
                    }
                }
            }
            // checkpoint-done, target-ready, migrate-done, transfer-complete,
            // checkpoint-failed — handled by migration controller, no kubelet action
            _ => {
                debug!("Migration: no-op for action '{action}' on {namespace}/{name}");
            }
        }

        Ok(())
    }

    async fn report_pod_status(
        &self,
        update: &crate::pod_manager::PodStatusUpdate,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let container_statuses: Vec<Value> = update
            .container_statuses
            .iter()
            .map(|cs| {
                let state_obj = match cs.state.as_str() {
                    "running" => serde_json::json!({
                        "running": {"startedAt": &now}
                    }),
                    "terminated" => serde_json::json!({
                        "terminated": {"exitCode": cs.exit_code, "finishedAt": &now}
                    }),
                    _ => serde_json::json!({
                        "waiting": {"reason": "ContainerCreating"}
                    }),
                };

                serde_json::json!({
                    "name": cs.name,
                    "state": state_obj,
                    "ready": cs.ready,
                    "restartCount": cs.restart_count,
                    "image": cs.image,
                    "imageID": cs.image_ref,
                    "containerID": format!("containerd://{}", cs.container_id)
                })
            })
            .collect();

        let mut conditions = vec![
            serde_json::json!({
                "type": "PodScheduled",
                "status": "True"
            }),
            serde_json::json!({
                "type": "Initialized",
                "status": "True"
            }),
        ];

        let all_ready = update.container_statuses.iter().all(|cs| cs.ready);
        conditions.push(serde_json::json!({
            "type": "ContainersReady",
            "status": if all_ready { "True" } else { "False" }
        }));
        conditions.push(serde_json::json!({
            "type": "Ready",
            "status": if all_ready { "True" } else { "False" }
        }));

        let mut status = serde_json::json!({
            "phase": &update.phase,
            "conditions": conditions,
            "containerStatuses": container_statuses,
            "hostIP": &self.node_ip,
            "startTime": &now
        });

        if let Some(ref ip) = update.pod_ip {
            status["podIP"] = serde_json::json!(ip);
            status["podIPs"] = serde_json::json!([{"ip": ip}]);
        }

        // Fetch current pod, merge status, update
        let path = format!(
            "{}/api/v1/namespaces/{}/pods/{}",
            self.config.api_server_url, update.namespace, update.name
        );

        if let Ok(resp) = self.api_client.get(&path).send().await {
            if resp.status().is_success() {
                if let Ok(mut pod) = resp.json::<Value>().await {
                    pod["status"] = status;
                    let _ = self.api_client.put(&path).json(&pod).send().await;
                }
            }
        }

        Ok(())
    }
}

fn hostname() -> String {
    detect_node_name()
}

/// Determine this node's name: NODE_NAME/HOSTNAME env (systemd doesn't export
/// HOSTNAME to services, so this often misses), then the real system hostname,
/// then "localhost" as a last resort.
pub fn detect_node_name() -> String {
    std::env::var("NODE_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .or_else(system_hostname)
        .unwrap_or_else(|| "localhost".to_string())
}

/// The kernel/system hostname, independent of the (often-unset-under-systemd)
/// HOSTNAME env var. Reads /proc on Linux, falls back to the `hostname` command.
fn system_hostname() -> Option<String> {
    #[cfg(target_os = "linux")]
    if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let h = h.trim();
        if !h.is_empty() {
            return Some(h.to_string());
        }
    }
    let out = std::process::Command::new("hostname").output().ok()?;
    let h = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if h.is_empty() {
        None
    } else {
        Some(h)
    }
}
