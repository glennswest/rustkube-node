//! Kubelet — the main node agent loop.
//!
//! Registers the node, sends heartbeats, syncs pods, runs probes.

use crate::cri::{ImageService, MigrationService, RuntimeService};
use crate::node_status::NodeReporter;
use crate::pod_manager::PodManager;
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn};

/// Kubelet configuration.
#[derive(Debug, Clone)]
pub struct KubeletConfig {
    pub node_name: String,
    pub api_server_url: String,
    pub heartbeat_interval: Duration,
    pub sync_interval: Duration,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: hostname(),
            api_server_url: "http://localhost:6443".into(),
            heartbeat_interval: Duration::from_secs(10),
            sync_interval: Duration::from_secs(2),
        }
    }
}

/// The kubelet node agent.
pub struct Kubelet {
    config: KubeletConfig,
    pod_manager: Arc<PodManager>,
    migration: Arc<dyn MigrationService>,
    reporter: NodeReporter,
    api_client: reqwest::Client,
}

impl Kubelet {
    pub fn new(
        config: KubeletConfig,
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        migration: Arc<dyn MigrationService>,
    ) -> Self {
        let reporter = NodeReporter::new(&config.api_server_url, &config.node_name);
        let pod_manager = Arc::new(PodManager::new(runtime, images, &config.node_name));

        Self {
            config,
            pod_manager,
            migration,
            reporter,
            api_client: reqwest::Client::new(),
        }
    }

    /// Run the kubelet. Blocks forever.
    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Kubelet starting for node {}", self.config.node_name);

        // Register node
        self.reporter.register().await?;

        // Spawn heartbeat task
        let reporter_url = self.config.api_server_url.clone();
        let node_name = self.config.node_name.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        tokio::spawn(async move {
            let reporter = NodeReporter::new(&reporter_url, &node_name);
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
        let updates = self.pod_manager.sync_pods(&my_pods).await;

        // Report status updates back to API server
        for update in &updates {
            if let Err(e) = self.report_pod_status(update).await {
                error!(
                    "Failed to report status for {}/{}: {e}",
                    update.namespace, update.name
                );
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
                        "terminated": {"exitCode": 0, "finishedAt": &now}
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
            "hostIP": "127.0.0.1",
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
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("NODE_NAME"))
        .unwrap_or_else(|_| "localhost".to_string())
}
