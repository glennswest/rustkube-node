//! Kubelet — the main node agent loop.
//!
//! Registers the node, sends heartbeats, syncs pods, runs probes.

use crate::cri::{ImageService, MigrationService, RuntimeService};
use crate::node_status::NodeReporter;
use crate::workload::{self, Access, Dependency, Executor, Key, Kind, Next, Resource};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::pod_manager::{InitContainerStatusReport, PodManager};
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
    /// Extra node labels applied at registration (`--node-labels`).
    pub node_labels: Vec<(String, String)>,
    /// Node annotations applied at registration (`--node-annotations`).
    pub node_annotations: Vec<(String, String)>,
    /// Taints applied at registration (`--register-with-taints`), and **only**
    /// at registration: a taint removed by an operator or by the component
    /// that made the node usable must not come back on the next restart.
    pub register_with_taints: Vec<serde_json::Value>,
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
    /// Directory of static-pod manifests (e.g. /etc/kubernetes/manifests). These
    /// pods run locally, independent of the apiserver — this is how the control
    /// plane (apiserver, etcd) bootstraps. `None` disables static pods.
    pub pod_manifest_path: Option<std::path::PathBuf>,
    /// This node's stormblock engine, with its token (#66). One client for
    /// every engine call the kubelet makes.
    pub engine: crate::engine::EngineClient,
}

impl Default for KubeletConfig {
    fn default() -> Self {
        Self {
            node_name: hostname(),
            api_server_url: "http://localhost:6443".into(),
            pod_cidr: None,
            node_labels: Vec::new(),
            node_annotations: Vec::new(),
            register_with_taints: Vec::new(),
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
            pod_manifest_path: Some(std::path::PathBuf::from("/etc/kubernetes/manifests")),
            engine: crate::engine::EngineClient::default(),
        }
    }
}

/// The kubelet node agent.
pub struct Kubelet {
    config: KubeletConfig,
    pod_manager: Arc<PodManager>,
    /// Virtual machines, when this node has an engine to start them with.
    ///
    /// `None` on any runtime but stormpump: a VM here is a workload in a
    /// machine domain, and nothing else on this node can start one. A kubelet
    /// without it simply does not list VMIs — it does not fail, and it does not
    /// pretend.
    vms: Option<Arc<crate::vm_manager::VmManager>>,
    /// VirtualMachineSnapshots of the machines this node runs (#53). Present
    /// exactly when `vms` is.
    snapshots: Option<Arc<crate::vm_snapshot::Snapshots>>,
    migration: Arc<dyn MigrationService>,
    runtime: Arc<dyn RuntimeService>,
    api_client: reqwest::Client,
    node_ip: String,
    /// Whether the missing static-pod directory has already been mentioned.
    ///
    /// It is read once per sync, so on any node that does not use static pods
    /// — every stormcos node, where `stormpump`'s boot.d units are the
    /// mechanism — the same line was emitted every few seconds forever. A
    /// directory that is not there is a fact about the configuration, not an
    /// event, and repeating it only buries the lines that are events.
    said_no_static_dir: std::sync::atomic::AtomicBool,
    /// The external CSI drivers on this node, shared with the pod manager
    /// and kept current by the registration loop (#52).
    csi: Arc<crate::csi_plugins::CsiPlugins>,
    watches: apimachinery::reactor::WatchHub,
    last_claims: std::sync::Mutex<crate::workload::VolumeIndex>,
    static_read_complete: std::sync::atomic::AtomicBool,
    runtime_changes: Option<tokio::sync::watch::Receiver<u64>>,
    workloads: Arc<Executor>,
    pods_synced: AtomicBool,
    vmis_synced: AtomicBool,
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

        let csi = Arc::new(
            crate::csi_plugins::CsiPlugins::new(&config.node_name)
                .with_api(api_client.clone(), &config.api_server_url),
        );
        let workloads = Executor::new();
        let pod_manager = Arc::new(
            PodManager::with_api(
                runtime.clone(),
                images,
                &config.node_name,
                &config.api_server_url,
                &node_ip,
                api_client.clone(),
            )
            .with_ca_pem(config.apiserver_ca.clone())
            .with_engine(config.engine.clone())
            .with_csi(csi.clone())
            .with_admission(workloads.reservations.clone()),
        );

        Ok(Self {
            config,
            pod_manager,
            vms: None,
            snapshots: None,
            migration,
            runtime,
            api_client,
            node_ip,
            said_no_static_dir: std::sync::atomic::AtomicBool::new(false),
            watches: Default::default(),
            last_claims: Default::default(),
            static_read_complete: std::sync::atomic::AtomicBool::new(true),
            runtime_changes: None,
            workloads,
            pods_synced: AtomicBool::new(false),
            vmis_synced: AtomicBool::new(false),
            csi,
        })
    }

    /// Reconcile virtual machines too, on the ring the containers use.
    ///
    /// A builder rather than a constructor argument because every runtime but
    /// stormpump would pass nothing. It takes the ring rather than a built
    /// manager so the manager gets *this* kubelet's authenticated apiserver
    /// client — a status written with an unauthenticated one is a status
    /// nobody ever sees.
    pub fn with_engine(mut self, ring: Arc<crate::stormpump_ring::RingClient>) -> Self {
        self.runtime_changes = Some(ring.subscribe_exits());
        self.vms = Some(Arc::new(
            crate::vm_manager::VmManager::new(
                Some(ring),
                &self.config.node_name,
                self.api_client.clone(),
                &self.config.api_server_url,
            )
            .with_storage(self.config.engine.clone())
            .with_claims(self.pod_manager.clone()),
        ));
        self.snapshots = Some(Arc::new(crate::vm_snapshot::Snapshots::new(
            self.api_client.clone(),
            &self.config.api_server_url,
            &self.config.node_name,
            crate::vm_manager::RUN_ROOT,
            crate::vm_snapshot::stormvm_take(self.config.engine.url().to_string()),
        )));
        self
    }

    /// Run the kubelet. Blocks forever.
    pub async fn run(self) -> anyhow::Result<()> {
        Arc::new(self).run_inner().await
    }

    async fn run_inner(self: &Arc<Self>) -> anyhow::Result<()> {
        info!("Kubelet starting for node {}", self.config.node_name);

        // Query the container runtime version for nodeInfo (e.g. cri-o://1.32.0).
        let runtime_version = match self.runtime.version().await {
            Ok((name, version, _)) => format!("{name}://{version}"),
            Err(e) => {
                warn!("Could not get runtime version: {e}");
                "cri-o://unknown".to_string()
            }
        };

        // Recover before any start. Reserve every observed name, even when
        // the API is unavailable and the old claim set is not yet known.
        loop {
            match self.pod_manager.recover_state().await {
                Ok(()) => break,
                Err(error) => {
                    warn!(%error, "runtime recovery incomplete; startup admission remains closed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        if let Some(vms) = &self.vms { vms.adopt_registered().await; }
        else { self.vmis_synced.store(true, Ordering::Release); }
        self.seed_names().await?;

        // Register the node in the BACKGROUND with backoff — never block the pod
        // loops on it. Static pods keep running (and get retried in the sync
        // loop) even while the apiserver is still coming up or RBAC isn't in
        // place yet. The kubelet must not crash-loop out over a transient
        // registration failure, nor stall the control-plane bootstrap on it.
        {
            let url = self.config.api_server_url.clone();
            let node_name = self.config.node_name.clone();
            let pod_cidr = self.config.pod_cidr.clone();
            let rv = runtime_version.clone();
            let port = self.config.kubelet_port;
            let client = self.api_client.clone();
            let labels = self.config.node_labels.clone();
            let annotations = self.config.node_annotations.clone();
            let taints = self.config.register_with_taints.clone();
            tokio::spawn(async move {
                let reporter = NodeReporter::with_pod_cidr(&url, &node_name, pod_cidr)
                    .with_runtime_version(rv)
                    .with_kubelet_port(port)
                    // Registration only. The heartbeat reporter below is
                    // deliberately built without these: it writes status
                    // through the /status subresource, and re-asserting taints
                    // every few seconds would undo every removal.
                    .with_registration(labels, annotations, taints)
                    .with_client(client);
                let mut backoff = Duration::from_secs(1);
                loop {
                    match reporter.register().await {
                        Ok(()) => {
                            info!("node registered");
                            break;
                        }
                        Err(e) => {
                            warn!("Node registration failed ({e}); retrying in {backoff:?}");
                            time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(30));
                        }
                    }
                }
            });
        }

        // Adopt pods already running in the runtime (e.g. after a kubelet
        // restart) so we reconcile rather than double-create sandboxes.
        // Recovery above precedes all admission.

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
                stormblock_url: self.config.engine.url().to_string(),
            };
            let vms = self.vms.clone();
            tokio::spawn(async move { crate::server::serve(port, pm, vms, server_config).await });
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

        // Mirror the node's own services into the API: when PID 1's asset
        // table changes, and when a mirror pod is edited or deleted (#101).
        tokio::spawn(self.clone().service_mirror_loop());

        // List the node's own data containers as PVCs (rustkube-node#49): the
        // same slow cadence as the services mirror, for the same reason.
        {
            let url = self.config.api_server_url.clone();
            let node = self.config.node_name.clone();
            let client = self.api_client.clone();
            let engine = self.config.engine.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    crate::system_claims::mirror(&client, &url, &engine, &node).await;
                }
            });
        }

        // Reclaim this node's released claims (reclaimPolicy: Delete).
        {
            let pm = self.pod_manager.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    pm.reclaim_released().await;
                }
            });
        }

        // External CSI drivers (#52): register what appears in
        // plugins_registry, and undo the volumes of pods that are gone
        // (deleted while the kubelet was down, or a teardown due a retry).
        tokio::spawn(self.csi.clone().run());
        {
            let pm = self.pod_manager.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    pm.sweep_csi_volumes().await;
                }
            });
        }

        if let Some(vms) = &self.vms {
            vms.spawn_address_pump();
        }

        // One bounded executor for both kinds; producers never perform runtime I/O.
        tokio::try_join!(self.pod_loop(), self.vm_loop(), self.vm_maintenance_loop(), async {
            self.workloads.run(self.clone(), 8).await;
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }

    async fn pod_loop(&self) -> anyhow::Result<()> {
        let worker = self.watches.worker("kubelet-pods");
        let mut exits = self.runtime_changes.clone();
        let mut volumes = Some(self.pod_manager.subscribe_volume_changes());
        let mut drivers = Some(self.csi.subscribe_changes());
        let mut images = self.pod_manager.subscribe_image_changes();
        let mut files = tokio::task::JoinSet::new();
        if let Some(path) = self.config.pod_manifest_path.clone() {
            let changed = worker.clone();
            files.spawn(
                async move { crate::fs_watch::watch(path, move || changed.enqueue()).await },
            );
        }
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                image = images.recv() => {
                    match image {
                        Ok(image)=>self.workloads.wake_dependency(&Dependency::Image(image)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>self.workloads.wake_kind(Kind::Pod),
                        Err(_)=>{}
                    }
                    continue;
                },
                _ = runtime_changed(&mut volumes) => { self.workloads.wake_kind(Kind::Pod); worker.enqueue(); continue; },
                _ = runtime_changed(&mut drivers) => { self.workloads.wake_kind(Kind::Pod); worker.enqueue(); continue; },
                _ = runtime_changed(&mut exits) => { self.workloads.wake_kind(Kind::Pod); worker.enqueue(); continue; },
            };
            worker
                .run(async {
                    if let Err(error) = self.sync().await {
                        apimachinery::reactor::failed();
                        error!("Pod sync failed: {error}");
                    }
                })
                .await;
            drop(work);
        }
    }

    // Keep main's snapshot and owner-sweep services independent of UID work.
    // Snapshot watches wake promptly; the deadline retains completion/recovery
    // and disk-owner checks until #101 supplies all dependency events.
    async fn vm_maintenance_loop(&self) -> anyhow::Result<()> {
        let Some(vms) = &self.vms else { return std::future::pending().await; };
        let worker = self.watches.worker("kubelet-vm-maintenance");
        let mut deadline = time::interval(self.config.sync_interval.max(Duration::from_millis(100)));
        loop {
            let work = tokio::select! {
                work = worker.next() => Some(work),
                _ = deadline.tick() => None,
            };
            worker.run(async {
                self.watches.observe(&self.api_client, format!(
                    "{}/apis/snapshot.kubevirt.io/v1beta1/virtualmachinesnapshots",
                    self.config.api_server_url));
                if let Some(snapshots) = &self.snapshots { snapshots.sync().await; }
                vms.sweep_orphans().await;
            }).await;
            drop(work);
        }
    }

    async fn vm_loop(&self) -> anyhow::Result<()> {
        let Some(vms) = &self.vms else {
            return std::future::pending().await;
        };
        let worker = self.watches.worker("kubelet-vmis");
        let mut exits = self.runtime_changes.clone();
        let mut volumes = Some(self.pod_manager.subscribe_volume_changes());
        let mut url = reqwest::Url::parse(&format!(
            "{}/apis/kubevirt.io/v1/virtualmachineinstances",
            self.config.api_server_url
        ))?;
        url.query_pairs_mut().append_pair(
            "fieldSelector",
            &format!("status.nodeName={}", self.config.node_name),
        );
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                _ = runtime_changed(&mut volumes) => { self.workloads.wake_kind(Kind::VirtualMachine); worker.enqueue(); continue; },
                _ = runtime_changed(&mut exits) => { self.workloads.wake_kind(Kind::VirtualMachine); worker.enqueue(); continue; },
            };
            worker
                .run(async {
                    self.watches.observe(&self.api_client, url.to_string());
                    match apimachinery::reactor::check(
                        apimachinery::reflector::list(&self.api_client, url.as_str()).await,
                    ) {
                        Ok(list) => {
                            let want: Vec<_> = list["items"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .filter(|v| {
                                    v["status"]["nodeName"].as_str() == Some(&self.config.node_name)
                                })
                                .cloned()
                                .collect();
                            self.observe_volume_dependencies();
                            vms.cache_specs(&want).await;
                            match self.seed_claims(Kind::VirtualMachine, &want).await {
                                Ok(()) => {
                                    if self.workloads.replace_source("api-vmis",Kind::VirtualMachine,&want).is_ok() {
                                        self.vmis_synced.store(true,Ordering::Release);
                                    }
                                }
                                Err(error) => { apimachinery::reactor::failed(); warn!(%error,"VM admission recovery pending"); }
                            }
                        }
                        Err(error) => {
                            debug!("VMI desired state unavailable: {error}");
                            // No CRD is a supported Pod-only cluster. Never
                            // treat this as deletion of registered machines.
                            if vms.running().await.is_empty() {
                                if let Ok(response)=self.api_client.get(url.clone()).send().await {
                                    if response.status().as_u16()==404 {
                                        self.vmis_synced.store(true,Ordering::Release);
                                    }
                                }
                            }
                        },
                    }
                })
                .await;
            drop(work);
        }
    }

    fn observe_volume_dependencies(&self) {
        for path in [
            "/api/v1/persistentvolumeclaims",
            "/api/v1/persistentvolumes",
            "/apis/storage.k8s.io/v1/volumeattachments",
        ] {
            self.watches.observe(
                &self.api_client,
                format!("{}{}", self.config.api_server_url, path),
            );
        }
    }

    /// Load static-pod manifests from `pod_manifest_path` and normalize them for
    /// local reconcile. Static pods run without the apiserver, keyed by a stable
    /// uid derived from name+node so re-reads don't recreate them. Best-effort:
    /// unreadable/invalid files are skipped with a warning.
    fn load_static_pods(&self) -> Vec<Value> {
        self.static_read_complete
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut pods = Vec::new();
        let Some(dir) = self.config.pod_manifest_path.as_ref() else {
            return pods;
        };
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Once. A node that does not use static pods reads this
                // directory on every sync, and saying so every time is how a
                // debug log stops being readable.
                if !self
                    .said_no_static_dir
                    .swap(true, std::sync::atomic::Ordering::Relaxed)
                {
                    debug!("no static pod dir {} — none will be loaded", dir.display());
                }
                return pods;
            }
            Err(e) => {
                self.static_read_complete
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                // Anything else is actionable — a permission problem, a path
                // that is not a directory — so it keeps saying so.
                warn!("static pod dir {}: {e}", dir.display());
                return pods;
            }
        };
        for entry in entries {
            let ent = match entry {
                Ok(ent) => ent,
                Err(error) => {
                    self.static_read_complete
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    warn!("static pod directory entry: {error}");
                    continue;
                }
            };
            let path = ent.path();
            match path.extension().and_then(|s| s.to_str()) {
                Some("yaml") | Some("yml") | Some("json") => {}
                _ => continue,
            }
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(e) => {
                    self.static_read_complete
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    warn!("read static pod {}: {e}", path.display());
                    continue;
                }
            };
            let mut pod: Value = match serde_yaml::from_str(&text) {
                Ok(v) => v,
                Err(e) => {
                    self.static_read_complete
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                    warn!("parse static pod {}: {e}", path.display());
                    continue;
                }
            };
            if pod.get("kind").and_then(|k| k.as_str()) != Some("Pod") {
                continue;
            }
            let name = pod["metadata"]["name"]
                .as_str()
                .map(str::to_string)
                .or_else(|| {
                    path.file_stem()
                        .and_then(|s| s.to_str())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| "static".into());
            let ns = pod["metadata"]["namespace"]
                .as_str()
                .unwrap_or("kube-system")
                .to_string();
            // Stable, node-scoped identity so reconciles don't recreate the pod
            // (matches Kubernetes' mirror-pod `<name>-<node>` convention).
            let uid = format!("static-{name}-{}", self.config.node_name);
            pod["metadata"]["name"] = Value::String(name);
            pod["metadata"]["namespace"] = Value::String(ns);
            pod["metadata"]["uid"] = Value::String(uid);
            pod["spec"]["nodeName"] = Value::String(self.config.node_name.clone());
            pods.push(pod);
        }
        pods
    }

    /// Publish independent snapshots; unavailable sources never imply deletion.
    async fn sync(&self) -> anyhow::Result<()> {
        let static_pods = self.load_static_pods();
        if self.static_read_complete.load(Ordering::Acquire) {
            self.pod_manager.cache_specs(&static_pods).await;
            self.seed_claims(Kind::Pod, &static_pods).await?;
            self.workloads.replace_source("static",Kind::Pod,&static_pods)?;
        } else { apimachinery::reactor::failed(); }
        let mut url = reqwest::Url::parse(&format!("{}/api/v1/pods",self.config.api_server_url))?;
        url.query_pairs_mut().append_pair("fieldSelector",&format!("spec.nodeName={}",self.config.node_name));
        self.watches.observe(&self.api_client,url.to_string());
        let list = apimachinery::reactor::check(apimachinery::reflector::list(&self.api_client,url.as_str()).await)?;
        let want: Vec<_> = list["items"].as_array().unwrap().iter().filter(|p|
            p["spec"]["nodeName"].as_str()==Some(&self.config.node_name)
            && p["metadata"]["annotations"]["kubernetes.io/config.source"].as_str()!=Some("stormpump")
            && !p["metadata"]["uid"].as_str().unwrap_or("").starts_with("static-"))
            .cloned().collect();
        self.pod_manager.cache_specs(&want).await;
        self.seed_claims(Kind::Pod,&want).await?;
        self.workloads.replace_source("api-pods",Kind::Pod,&want)?;
        self.pods_synced.store(true,Ordering::Release);
        self.observe_volume_dependencies();
        // Dependency collection changes wake only affected claim users.
        self.refresh_claim_dependencies().await;
        Ok(())
    }

    async fn seed_names(&self) -> anyhow::Result<()> {
        let mut api = Vec::new(); let mut statics = Vec::new();
        for pod in self.pod_manager.known_pods().await {
            let object = serde_json::json!({"metadata":{"namespace":pod.namespace,"name":pod.name,"uid":pod.uid}});
            let key=Key::of(Kind::Pod,&object)?;
            self.workloads.reservations.seed(&key,&[]);
            if key.uid.starts_with("static-") { statics.push(object); } else { api.push(object); }
        }
        self.workloads.replace_source("api-pods",Kind::Pod,&api)?;
        self.workloads.replace_source("static",Kind::Pod,&statics)?;
        if let Some(vms)=&self.vms {
            let mut objects=Vec::new();
            for vm in vms.running().await {
                let object=serde_json::json!({"metadata":{"namespace":vm.namespace,"name":vm.name,"uid":vm.uid}});
                self.workloads.reservations.seed(&Key::of(Kind::VirtualMachine,&object)?,&[]);
                objects.push(object);
            }
            self.workloads.replace_source("api-vmis",Kind::VirtualMachine,&objects)?;
        }
        Ok(())
    }

    async fn claims_for(&self,key:&Key,object:&Value) -> anyhow::Result<Vec<(Resource,Access)>> {
        let mut claims=Vec::new();
        for dependency in workload::dependencies(key,object) {
            let Dependency::Claim(ns,name)=dependency else {continue};
            let response=self.api_client.get(format!("{}/api/v1/namespaces/{ns}/persistentvolumeclaims/{name}",self.config.api_server_url)).send().await?;
            let pvc:Value=response.error_for_status()?.json().await?;
            let mode=if key.kind==Kind::VirtualMachine || crate::storage::is_rwop(&pvc) {Access::Exclusive} else {Access::SharedFilesystem};
            claims.push((Resource::Claim(ns,name),mode));
        }
        Ok(claims)
    }

    async fn seed_claims(&self,kind:Kind,objects:&[Value]) -> anyhow::Result<()> {
        let known:std::collections::HashSet<String>=match kind {
            Kind::Pod=>self.pod_manager.known_pods().await.into_iter().map(|p|p.uid).collect(),
            Kind::VirtualMachine=>match &self.vms {Some(v)=>v.running().await.into_iter().map(|v|v.uid).collect(),None=>Default::default()},
        };
        for object in objects {
            let key=Key::of(kind,object)?;
            if known.contains(&key.uid) {
                let claims=self.claims_for(&key,object).await?;
                self.workloads.reservations.seed(&key,&claims);
            }
        }
        Ok(())
    }

    async fn refresh_claim_dependencies(&self) {
        let base = &self.config.api_server_url;
        let claim_url = format!("{base}/api/v1/persistentvolumeclaims");
        let volume_url = format!("{base}/api/v1/persistentvolumes");
        let attachment_url = format!("{base}/apis/storage.k8s.io/v1/volumeattachments");
        let (claims, volumes, attachments) = tokio::join!(
            apimachinery::reflector::list(&self.api_client,&claim_url),
            apimachinery::reflector::list(&self.api_client,&volume_url),
            apimachinery::reflector::list(&self.api_client,&attachment_url),
        );
        if let (Ok(claims),Ok(volumes),Ok(attachments)) = (claims,volumes,attachments) {
            if let (Some(claims),Some(volumes),Some(attachments)) =
                (claims["items"].as_array(),volumes["items"].as_array(),attachments["items"].as_array()) {
                let changed = self.last_claims.lock().unwrap().update(claims,volumes,attachments);
                for dependency in changed {self.workloads.wake_dependency(&dependency);}
            }
        }
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
                                    "uid": uid,
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
                                    "uid": uid,
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
                                    "uid": uid,
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

                info!("Migration: live-migrating pod {namespace}/{name} to {target_endpoint}");
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
                                    "uid": uid,
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
                            self.pod_manager
                                .register_restored_pod(uid, namespace, name, &sandbox_id)
                                .await;
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
        source: &Value,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let container_statuses: Vec<Value> = update
            .container_statuses
            .iter()
            .map(|cs| {
                // The container's own times, not this moment.
                //
                // These were stamped with `now()` — the instant the status was
                // *reported*. Every container therefore claimed to have
                // started seconds ago, on every poll, so one that had been up
                // for an hour looked exactly like one that had just come back;
                // reading a restart into that is the natural mistake, and
                // somebody did. A time the runtime did not give renders as
                // null rather than as a plausible wrong answer.
                let state_obj = match cs.state.as_str() {
                    "running" => serde_json::json!({
                        "running": {"startedAt": nanos_to_rfc3339(cs.started_at)}
                    }),
                    "terminated" => serde_json::json!({
                        "terminated": {
                            "exitCode": cs.exit_code,
                            "startedAt": nanos_to_rfc3339(cs.started_at),
                            "finishedAt": nanos_to_rfc3339(cs.finished_at)
                        }
                    }),
                    // The reason is what `kubectl get pod` prints in the
                    // STATUS column, so "CrashLoopBackOff" here is the
                    // difference between a reader seeing a container that is
                    // starting and one that has been failing for ten minutes.
                    // Empty still means ContainerCreating — the ordinary case.
                    _ => {
                        let reason = if cs.reason.is_empty() {
                            "ContainerCreating"
                        } else {
                            cs.reason.as_str()
                        };
                        let mut waiting = serde_json::json!({"reason": reason});
                        if !cs.message.is_empty() {
                            waiting["message"] = serde_json::json!(cs.message);
                        }
                        serde_json::json!({"waiting": waiting})
                    }
                };

                let mut status = serde_json::json!({
                    "name": cs.name,
                    "state": state_obj,
                    "ready": cs.ready,
                    "restartCount": cs.restart_count,
                    "image": cs.image,
                    "imageID": cs.image_ref,
                });
                // A container not created yet has no id, and upstream leaves
                // the field out rather than naming `containerd://` (#63).
                if !cs.container_id.is_empty() {
                    status["containerID"] =
                        serde_json::json!(format!("containerd://{}", cs.container_id));
                }
                status
            })
            .collect();

        // Init containers, reported as Kubernetes reports them.
        //
        // These were invisible: the kubelet ran them, failed the pod if one
        // exited non-zero, and then said nothing — so cilium's six showed in a
        // console as six components of unknown health, with no way to tell
        // "ran and succeeded" from "never ran" (#47).
        let init_container_statuses: Vec<serde_json::Value> = update
            .init_container_statuses
            .iter()
            .map(|cs| {
                let state = if cs.state == "terminated" {
                    serde_json::json!({"terminated": {
                        "exitCode": cs.exit_code,
                        "reason": cs.reason,
                        "message": cs.message,
                        "startedAt": nanos_to_rfc3339(cs.started_at),
                        "finishedAt": nanos_to_rfc3339(cs.finished_at),
                        "containerID": format!("containerd://{}", cs.container_id),
                    }})
                } else {
                    serde_json::json!({"running": {
                        "startedAt": nanos_to_rfc3339(cs.started_at)
                    }})
                };
                serde_json::json!({
                    "name": cs.name,
                    "state": state,
                    // An init container is never "ready" — it is finished or
                    // it is not. Kubernetes reports ready=true for a
                    // successfully completed one, which is what lets a reader
                    // tell it apart from one still going.
                    "ready": cs.succeeded(),
                    "restartCount": 0,
                    "image": cs.image,
                    "imageID": cs.image_ref,
                    "containerID": format!("containerd://{}", cs.container_id)
                })
            })
            .collect();

        // `Initialized` was hardcoded True — before the init containers ran,
        // while they were running, and after one had failed. A pod wedged in
        // init reported Initialized=True with no containers, which is worse
        // than Unknown because it is confidently wrong, and it is the
        // condition anything waiting on init progress would read.
        //
        // Driven by the pod's *spec* rather than by the report being
        // non-empty: a pod with no init containers is initialized, and so is
        // one whose inits all succeeded. Anything else is not.
        let initialized = pod_initialized(
            update.declared_init_containers,
            &update.init_container_statuses,
        );

        let mut conditions = vec![
            serde_json::json!({
                "type": "PodScheduled",
                "status": "True"
            }),
            serde_json::json!({
                "type": "Initialized",
                "status": if initialized { "True" } else { "False" },
                "reason": if initialized { serde_json::Value::Null }
                          else { serde_json::json!("ContainersNotInitialized") }
            }),
        ];

        // A pod with no containers running is **not** Ready. `.all()` on an
        // empty list is true, so a pod that failed before any container
        // started reported Ready=True with no containers — which is how a
        // Failed pod came back looking healthy to everything that reads
        // conditions.
        let all_ready = !update.container_statuses.is_empty()
            && update.container_statuses.iter().all(|cs| cs.ready);
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
            "initContainerStatuses": init_container_statuses,
            "hostIP": &self.node_ip,
            "startTime": source["status"]["startTime"].as_str().unwrap_or(&now)
        });

        // **Publish why.** The kubelet already knows: `start_pod` returns the
        // error and it is carried here in `update.message` — and it was
        // dropped on the floor, so a pod that would not start reported
        // `phase: Failed` and nothing else. On a node with no shell that is
        // unrecoverable; the reason exists and nobody can read it.
        if !update.message.is_empty() {
            status["message"] = serde_json::json!(&update.message);
            status["reason"] = serde_json::json!(match update.phase.as_str() {
                "Failed" => "StartFailed",
                _ => "Kubelet",
            });
        }

        if let Some(ref ip) = update.pod_ip {
            status["podIP"] = serde_json::json!(ip);
            status["podIPs"] = serde_json::json!([{"ip": ip}]);
        }

        // Fetch current pod, merge status, update
        let path = format!(
            "{}/api/v1/namespaces/{}/pods/{}",
            self.config.api_server_url, update.namespace, update.name
        );

        // Preserve status fields owned by admission/scheduling; use the
        // source revision so a slow start cannot overwrite a recreated Pod.
        let mut merged = source["status"].as_object().cloned().unwrap_or_default();
        if status.get("message").is_none() {
            merged.remove("message");
            merged.remove("reason");
        }
        merged.extend(status.as_object().unwrap().clone());
        let status = Value::Object(merged);
        if status == source["status"] {
            return Ok(());
        }
        // Static manifests have no API identity; their mirror publication is
        // separate from managing their local runtime state.
        if source["metadata"]["resourceVersion"].as_str().is_none() {
            return Ok(());
        }
        self.api_client.put(format!("{path}/status"))
            .timeout(Duration::from_secs(10))
            .json(&serde_json::json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": &update.name, "namespace": &update.namespace,
                    "uid": source["metadata"]["uid"], "resourceVersion": source["metadata"]["resourceVersion"]},
                "status": status
            })).send().await?.error_for_status()?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl workload::Adapter for Kubelet {
    async fn reconcile(&self,key:&Key,desired:Option<Value>) -> anyhow::Result<Next> {
        let retry=self.config.sync_interval.max(Duration::from_millis(100));
        let deleting=desired.as_ref().map_or(true,|o| !o["metadata"]["deletionTimestamp"].is_null());
        if deleting {
            let claims=self.workloads.reservations.claims(key);
            let Some(_operations)=self.workloads.reservations.try_operations(&claims) else {return Ok(Next::After(Duration::from_millis(100)))};
            match key.kind {
                Kind::Pod=> {
                    self.pod_manager.stop_pod(&key.uid).await?;
                    if let Some(object)=&desired {
                        // Never acknowledge deletion of a same-name successor.
                        let response=self.api_client.delete(format!("{}/api/v1/namespaces/{}/pods/{}",self.config.api_server_url,key.namespace,key.name))
                            .json(&serde_json::json!({"apiVersion":"v1","kind":"DeleteOptions","gracePeriodSeconds":0,
                                "preconditions":{"uid":object["metadata"]["uid"]}})).send().await?;
                        anyhow::ensure!(response.status().is_success() || matches!(response.status().as_u16(),404|409),"Pod deletion acknowledgement failed: {}",response.status());
                    }
                }
                Kind::VirtualMachine=> {
                    if let Some(vms)=&self.vms { vms.reconcile_one(&key.uid,desired.as_ref()).await?; }
                }
            }
            self.workloads.reservations.release(key);
            return Ok(Next::AwaitEvent);
        }
        let object=desired.unwrap();
        // Recovery placeholder objects are never launch specifications.
        if object["spec"].is_null() {return Ok(Next::After(retry))}
        let static_without_claims=key.kind==Kind::Pod && key.uid.starts_with("static-")
            && !workload::dependencies(key,&object).iter().any(|d| matches!(d,Dependency::Claim(..)));
        if !static_without_claims {
            if !self.pods_synced.load(Ordering::Acquire) || !self.vmis_synced.load(Ordering::Acquire)
                || self.pod_manager.known_pods().await.iter().any(|p|p.pod.is_null())
                || match &self.vms {Some(v)=>v.has_unknown_claims().await,None=>false} {
                return Ok(Next::After(retry));
            }
        }
        let claims=self.claims_for(key,&object).await?;
        if !self.workloads.reservations.acquire(key,&claims) {return Ok(Next::AwaitEvent)}
        let Some(_operations)=self.workloads.reservations.try_operations(&claims) else {return Ok(Next::After(Duration::from_millis(100)))};
        match key.kind {
            Kind::Pod=> {
                // Incomplete collection semantics deliberately suppress orphan
                // sweeping: absence is handled by the deleted UID's own worker.
                let outcome=self.pod_manager.sync_pods_observed(&[object.clone()],false).await;
                for update in &outcome.updates {self.report_pod_status(update,&object).await?;}
                self.handle_migration_annotations(&object).await?;
            }
            Kind::VirtualMachine=> {
                if let Some(vms)=&self.vms {vms.reconcile_one(&key.uid,Some(&object)).await?;}
            }
        }
        // The pass set the deadlines it still needs (probes, backoffs,
        // pending retries); everything else is an event (#101).
        let due = match key.kind {
            Kind::Pod => self.pod_manager.take_due(&key.uid),
            Kind::VirtualMachine => self.vms.as_ref().and_then(|v| v.take_due(&key.uid)),
        };
        Ok(self.next_look(key.kind, due))
    }
}

impl Kubelet {
    /// The node's services as mirror pods, on events only (#101).
    ///
    /// Two sources. PID 1's asset table, through inotify on its directory,
    /// gated on the parsed table: PID 1 rewrites the file every pass with
    /// fresh ages (stormpump#67), so a read is taken at most once a second and
    /// only a changed name set, start, stop or restart goes further. And the
    /// mirror pods themselves, watched, so one deleted or edited is put back.
    /// The mirror writes nothing that is already current, so its own writes
    /// settle after one pass. A failed pass retries on the reactor's backoff.
    async fn service_mirror_loop(self: Arc<Self>) {
        const RUN_DIR: &str = "/run/stormpump";
        let worker = self.watches.worker("kubelet-service-mirror");
        let changed = Arc::new(tokio::sync::Notify::new());
        {
            let changed = changed.clone();
            tokio::spawn(crate::fs_watch::watch(RUN_DIR.into(), move || changed.notify_one()));
        }
        {
            let worker = worker.clone();
            tokio::spawn(async move {
                let mut last = None;
                loop {
                    changed.notified().await;
                    let text = std::fs::read_to_string(format!("{RUN_DIR}/assets.json")).unwrap_or_default();
                    let key = crate::mirror::table_key(&crate::mirror::parse_assets(&text));
                    if last.as_ref() != Some(&key) {
                        last = Some(key);
                        worker.enqueue();
                    }
                    // A floor, not a clock: nothing is read while the file is
                    // quiet. Needed while PID 1 rewrites it every pass.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        }
        let url = self.config.api_server_url.clone();
        let node = self.config.node_name.clone();
        loop {
            let work = worker.next().await;
            worker
                .run(async {
                    if !url.is_empty() {
                        self.watches.observe(&self.api_client, format!(
                            "{url}/api/v1/namespaces/kube-system/pods?labelSelector=storm.io%2Fcomponent%3Dnode-service"));
                    }
                    mirror_node_services(&self.api_client, &url, &node).await;
                })
                .await;
            drop(work);
        }
    }

    /// When a live workload is next looked at with no event (#101): its own
    /// deadline, else only an event. A runtime that reports no exits (a CRI
    /// runtime, not stormpump) cannot say a container ended, so its workloads
    /// keep `sync_interval` as a counted fallback.
    fn next_look(&self, kind: Kind, due: Option<Duration>) -> Next {
        let worker = match kind { Kind::Pod => "pod", Kind::VirtualMachine => "vmi" };
        if self.runtime_changes.is_none() {
            let retry = self.config.sync_interval.max(Duration::from_millis(100));
            crate::metrics::observe_timed(worker, "fallback");
            return Next::After(due.map_or(retry, |d| d.min(retry)));
        }
        match due {
            Some(d) => {
                crate::metrics::observe_timed(worker, "deadline");
                Next::After(d)
            }
            None => Next::AwaitEvent,
        }
    }
}

async fn runtime_changed(receiver: &mut Option<tokio::sync::watch::Receiver<u64>>) {
    if let Some(rx) = receiver {
        if rx.changed().await.is_ok() {
            return;
        }
    }
    std::future::pending::<()>().await;
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

/// Publish a mirror pod for each service PID 1 supervises.
///
/// Reads what stormpump wrote and reflects it. **Nothing is invented**: an
/// asset absent from the file is not mirrored, and a mirror whose asset has
/// gone is deleted — the file is the truth, and a stale pod claiming a service
/// is running is worse than no pod at all.
///
/// Best effort throughout. A node whose services cannot be *seen* still works;
/// failing the kubelet over a cosmetic write would trade a real capability for
/// a convenience.
/// What each node service looked like last time, so a change can be reported.
///
/// A crash-looping service produced nothing in Kubernetes: `stormlb` restarted
/// eight times in four minutes and the console showed a pod that was simply
/// not Running, with no event saying why or that anything had happened. Every
/// other pod on the node has a lifecycle; these had a state that silently
/// differed from the last time you looked.
static LAST_SEEN: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, (bool, u32)>>,
> = std::sync::OnceLock::new();

async fn mirror_node_services(client: &reqwest::Client, api_url: &str, node: &str) {
    // No cluster: nothing to mirror into, and nothing to retry.
    if api_url.is_empty() {
        return;
    }
    // Its own recorder: this runs on a worker of its own, not through the pod
    // manager, and building one per pass is a struct with a cloned client.
    let events = (!api_url.is_empty())
        .then(|| crate::events::EventRecorder::new(client.clone(), api_url, node));
    const ASSET_STATUS: &str = "/run/stormpump/assets.json";
    let text = match std::fs::read_to_string(ASSET_STATUS) {
        Ok(t) => t,
        Err(e) => {
            // Not an error — a node not run by stormpump has no such file, and
            // neither does one whose PID 1 has not written it yet.
            //
            // **But say so once.** This returned in silence, so when the node's
            // services stopped appearing as pods there was no line anywhere
            // saying why, and nothing distinguished "no file" from "cannot
            // read it" from "the loop is not running". Once, at debug, is
            // enough to answer that without a line every pass forever on a
            // node that will never have one.
            static SAID: std::sync::Once = std::sync::Once::new();
            SAID.call_once(|| {
                debug!("not mirroring node services: cannot read {ASSET_STATUS}: {e}");
            });
            return;
        }
    };
    let assets = crate::mirror::parse_assets(&text);
    if assets.is_empty() {
        return;
    }

    // What changed since the last pass, as events.
    //
    // The mirror sees every transition — it reads the table whenever it
    // changes — and reported none of them. A service that stopped, or
    // started, or has been restarting for four minutes is exactly what an
    // event is for, and rustkube-node#50 is this.
    {
        let seen =
            LAST_SEEN.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut changes: Vec<(String, &'static str, String)> = Vec::new();
        {
            let mut last = match seen.lock() {
                Ok(l) => l,
                Err(e) => e.into_inner(),
            };
            for a in &assets {
                match last.get(&a.name).copied() {
                    None => {
                        // First sight is not an event: every service would
                        // announce itself on every kubelet restart.
                    }
                    Some((was_running, was_restarts)) => {
                        if a.restarts > was_restarts {
                            changes.push((
                                a.name.clone(),
                                "BackOff",
                                format!(
                                    "restarted {} time(s); PID 1 has restarted it {} times",
                                    a.restarts - was_restarts,
                                    a.restarts
                                ),
                            ));
                        } else if was_running && !a.running {
                            changes.push((
                                a.name.clone(),
                                "Stopped",
                                "the service is no longer running".into(),
                            ));
                        } else if !was_running && a.running {
                            changes.push((
                                a.name.clone(),
                                "Started",
                                "the service is running".into(),
                            ));
                        }
                    }
                }
                last.insert(a.name.clone(), (a.running, a.restarts));
            }
        }
        for (name, reason, message) in changes {
            let etype = if reason == "Started" {
                "Normal"
            } else {
                "Warning"
            };
            let pod = serde_json::json!({
                "metadata": { "name": format!("{name}-{node}"), "namespace": "kube-system", "uid": "" }
            });
            if let Some(r) = &events {
                r.pod_event(&pod, etype, reason, &message).await;
            }
        }
    }

    // Mirrors of assets PID 1 did not list on this boot (#87): not running,
    // said once, never deleted.
    // The mirrors there are, read once: what exists is compared rather than
    // written blind (#101). Unreadable: nothing is written, and the pass is
    // retried on the reactor's backoff.
    let list_url = format!(
        "{api_url}/api/v1/namespaces/kube-system/pods?labelSelector=storm.io%2Fcomponent%3Dnode-service"
    );
    let listed = match client.get(&list_url).send().await {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
        _ => None,
    };
    let Some(list) = listed else {
        apimachinery::reactor::failed();
        return;
    };
    for pod in crate::mirror::stale_mirrors(&list, node, &assets) {
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let marked = crate::mirror::not_started(pod);
        let put = client
            .put(format!("{api_url}/api/v1/namespaces/kube-system/pods/{name}/status"))
            .json(&marked)
            .send()
            .await;
        match put {
            Ok(r) if r.status().is_success() => {
                let msg = marked["status"]["message"].as_str().unwrap_or("").to_string();
                if let Some(ev) = &events {
                    ev.pod_event(&marked, "Warning", crate::mirror::NOT_STARTED, &msg).await;
                }
            }
            Ok(r) => debug!("mirror {name}: not-started status -> {}", r.status()),
            Err(e) => debug!("mirror {name}: not-started status: {e}"),
        }
    }

    // The node's uid, so new mirrors are owned by it and collected with it.
    // Asked only when there is one to create.
    let mut node_uid: Option<String> = None;
    let now = chrono::Utc::now();
    for a in &assets {
        // startTime from the age PID 1 reported: the two ends share no clock,
        // so an age is portable where an instant is not.
        let started = (now - chrono::Duration::seconds(a.age_secs as i64))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let name = crate::mirror::mirror_name(&a.name, node);
        let base = format!("{api_url}/api/v1/namespaces/kube-system/pods");
        let existing = list["items"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|p| p["metadata"]["name"].as_str() == Some(name.as_str()));

        // The object is long-lived and only its status moves; a status that
        // already says it is not written (#101).
        match existing {
            Some(existing) => {
                let pod = crate::mirror::mirror_pod(a, node, "", &started);
                if crate::mirror::status_current(existing, &pod) {
                    continue;
                }
                let mut pod = pod;
                pod["metadata"] = existing["metadata"].clone();
                let put = client.put(format!("{base}/{name}/status")).json(&pod).send().await;
                if !matches!(put, Ok(ref r) if r.status().is_success() || r.status() == 409) {
                    apimachinery::reactor::failed();
                }
            }
            None => {
                if node_uid.is_none() {
                    node_uid = match client.get(format!("{api_url}/api/v1/nodes/{node}")).send().await {
                        Ok(r) if r.status().is_success() => r
                            .json::<Value>()
                            .await
                            .ok()
                            .and_then(|n| n["metadata"]["uid"].as_str().map(str::to_owned))
                            .filter(|u| !u.is_empty()),
                        _ => None,
                    };
                }
                let Some(uid) = &node_uid else {
                    // Not registered yet: its registration is an API change
                    // the watch does not see, so the pass is retried.
                    apimachinery::reactor::failed();
                    return;
                };
                let pod = crate::mirror::mirror_pod(a, node, uid, &started);
                let created = client.post(&base).json(&pod).send().await;
                if !matches!(created, Ok(ref r) if r.status().is_success() || r.status() == 409) {
                    apimachinery::reactor::failed();
                }
            }
        }
    }
}

/// Epoch nanoseconds, as CRI reports them, to the RFC 3339 the API expects.
///
/// Zero means the runtime did not say — a container that never started, or a
/// finish time asked for before there was one. That becomes `null` rather
/// than 1970, because a timestamp at the epoch sorts first and looks like a
/// fact.
fn nanos_to_rfc3339(nanos: i64) -> serde_json::Value {
    if nanos <= 0 {
        return serde_json::Value::Null;
    }
    match chrono::DateTime::from_timestamp(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32) {
        Some(t) => serde_json::json!(t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        None => serde_json::Value::Null,
    }
}

/// Is the pod past its init containers?
///
/// Computed against the *declared* count, not against the reports being
/// non-empty, because those two differ exactly where it matters: a pod that
/// has not reached its init containers yet reports none, and "none reported"
/// must not read the same as "none declared".
fn pod_initialized(declared: usize, reports: &[InitContainerStatusReport]) -> bool {
    if declared == 0 {
        return true;
    }
    reports.len() == declared && reports.iter().all(|cs| cs.succeeded())
}

#[cfg(test)]
mod init_condition_tests {
    use super::*;

    fn report(name: &str, exit: i32) -> InitContainerStatusReport {
        InitContainerStatusReport {
            name: name.into(),
            container_id: "abc".into(),
            state: "terminated".into(),
            exit_code: exit,
            reason: if exit == 0 {
                "Completed".into()
            } else {
                "Error".into()
            },
            message: String::new(),
            image: "busybox".into(),
            image_ref: "sha256:x".into(),
            started_at: 1,
            finished_at: 2,
        }
    }

    #[test]
    fn a_pod_with_no_init_containers_is_initialized() {
        assert!(pod_initialized(0, &[]));
    }

    #[test]
    fn a_pod_that_has_not_reached_its_inits_is_not_initialized() {
        // The bug this replaces: Initialized was hardcoded True, so a pod
        // wedged here reported True with no containers running.
        assert!(!pod_initialized(6, &[]));
    }

    #[test]
    fn a_pod_part_way_through_is_not_initialized() {
        assert!(!pod_initialized(
            6,
            &[report("config", 0), report("mount-cgroup", 0)]
        ));
    }

    #[test]
    fn all_succeeded_is_initialized() {
        let all: Vec<_> = [
            "config",
            "mount-cgroup",
            "apply-sysctl-overwrites",
            "mount-bpf-fs",
            "clean-cilium-state",
            "install-cni-binaries",
        ]
        .iter()
        .map(|n| report(n, 0))
        .collect();
        assert!(pod_initialized(6, &all));
    }

    #[test]
    fn one_failure_means_not_initialized() {
        let mut all: Vec<_> = (0..6).map(|i| report(&format!("i{i}"), 0)).collect();
        all[3] = report("i3", 1);
        assert!(!pod_initialized(6, &all));
    }

    #[test]
    fn a_running_init_has_not_succeeded() {
        let mut r = report("config", 0);
        r.state = "running".into();
        assert!(!r.succeeded());
        assert!(!pod_initialized(1, &[r]));
    }

    #[test]
    fn an_epoch_timestamp_is_null_rather_than_1970() {
        // A finish time asked for before there was one sorts first and looks
        // like a fact if it renders as 1970.
        assert_eq!(nanos_to_rfc3339(0), serde_json::Value::Null);
        assert_eq!(nanos_to_rfc3339(-1), serde_json::Value::Null);
        assert!(nanos_to_rfc3339(1_789_000_000_000_000_000).is_string());
    }
}
