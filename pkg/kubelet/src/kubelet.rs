//! Kubelet — the main node agent loop.
//!
//! Registers the node, sends heartbeats, syncs pods, runs probes.

use retry::RetryExt;
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
    /// Pod/VMI passes run at once, in the one executor (`--pod-workers`, #138).
    pub pod_workers: usize,
    /// Port for the kubelet's inbound HTTP server (upstream 10250).
    pub kubelet_port: u16,
    /// Pods this node takes, reported as capacity and allocatable `pods`
    /// (`--max-pods`, #165).
    pub max_pods: u32,
    /// `--system-reserved` + `--kube-reserved` (#24), held back from allocatable.
    pub reserved: crate::node_status::Reserved,
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
    /// Where the client pair came from (`--client-certificate`/`--client-key`),
    /// to present a renewed one without a restart (#77).
    pub client_cert_path: Option<std::path::PathBuf>,
    pub client_key_path: Option<std::path::PathBuf>,
    /// Where the serving pair came from, to reload it when it is renewed (#89).
    pub serving_cert_path: Option<std::path::PathBuf>,
    pub serving_key_path: Option<std::path::PathBuf>,
    /// Static bearer token accepted by the inbound server (e.g. for monitoring).
    pub server_auth_token: Option<String>,
    /// Serve the inbound `:10250` endpoints unauthenticated (dev only).
    pub anonymous_auth: bool,
    /// Directory of static-pod manifests (e.g. /etc/kubernetes/manifests). These
    /// pods run locally, independent of the apiserver — this is how the control
    /// plane (apiserver, etcd) bootstraps. `None` disables static pods.
    pub pod_manifest_path: Option<std::path::PathBuf>,
    /// The CNI config directory (`--cni-conf-dir`), watched so a pod waiting
    /// for the network starts when its config appears (#148). `None`: not
    /// watched; such a pod is looked at every 10 s.
    pub cni_conf_dir: Option<std::path::PathBuf>,
    /// This node's stormblock engine, with its token (#66). One client for
    /// every engine call the kubelet makes.
    pub engine: crate::engine::EngineClient,
    /// How claims are charged against the data slabs, and when to warn (#62).
    pub storage: crate::capacity::Policy,
    /// How long `/vmInstance` answers from the VMI cache without word from
    /// the apiserver (`--metadata-max-staleness`, #156). Zero: unbounded.
    pub metadata_max_staleness: Duration,
    /// Container log rotation (`--container-log-max-size`,
    /// `--container-log-max-files`, #216).
    pub container_log: crate::container_logs::Rotation,
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
            pod_workers: crate::workload::default_workers(),
            kubelet_port: 10250,
            max_pods: crate::node_status::DEFAULT_MAX_PODS,
            reserved: Default::default(),
            apiserver_ca: None,
            bearer_token: None,
            client_cert: None,
            client_key: None,
            insecure_skip_tls_verify: false,
            serving_cert: None,
            serving_key: None,
            serving_cert_path: None,
            serving_key_path: None,
            client_cert_path: None,
            client_key_path: None,
            server_auth_token: None,
            anonymous_auth: false,
            pod_manifest_path: Some(std::path::PathBuf::from("/etc/kubernetes/manifests")),
            cni_conf_dir: None,
            engine: crate::engine::EngineClient::default(),
            storage: crate::capacity::Policy::default(),
            metadata_max_staleness: crate::vm_manager::METADATA_MAX_STALENESS,
            container_log: Default::default(),
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
    /// Per pod UID, the status this kubelet last had acknowledged (#141).
    acked_status: std::sync::Mutex<std::collections::HashMap<String, AckedStatus>>,
    /// The client certificate, swapped in place when it is renewed (#77).
    client_cert: Option<Arc<crate::client::ReloadingClientCert>>,
    /// What each node service's health endpoint says (#96), by asset name.
    service_health: Arc<std::sync::Mutex<std::collections::HashMap<String, crate::node_health::ServiceHealth>>>,
    watches: apimachinery::reactor::WatchHub,
    last_claims: std::sync::Mutex<crate::workload::VolumeIndex>,
    static_read_complete: std::sync::atomic::AtomicBool,
    runtime_changes: Option<tokio::sync::watch::Receiver<u64>>,
    /// A CRI runtime's container-event stream is open (#116): its Pods wait
    /// on events, not on `sync_interval`.
    runtime_events_live: AtomicBool,
    /// The engine's exits by workload handle (#115), taken by the router.
    exit_routes: std::sync::Mutex<Option<tokio::sync::broadcast::Receiver<u64>>>,
    workloads: Arc<Executor>,
    pods_synced: AtomicBool,
    vmis_synced: AtomicBool,
    /// Moves whenever stormblock's volumes change (#101): the claims mirror
    /// and the VM disk-owner sweep follow it instead of a clock.
    engine_volumes: tokio::sync::watch::Sender<u64>,
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
        let (api_client, client_cert) = crate::client::build_authed_client_reloadable(&crate::client::ClientAuth {
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
            .with_storage_policy(config.storage)
            .with_csi(csi.clone())
            .with_admission(workloads.reservations.clone())
            .with_load(workloads.load.clone()),
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
            runtime_events_live: AtomicBool::new(false),
            exit_routes: std::sync::Mutex::new(None),
            workloads,
            pods_synced: AtomicBool::new(false),
            vmis_synced: AtomicBool::new(false),
            engine_volumes: tokio::sync::watch::channel(0).0,
            csi,
            acked_status: Default::default(),
            service_health: Default::default(),
            client_cert,
        })
    }

    /// Reconcile virtual machines too, on the ring the containers use.
    ///
    /// A builder rather than a constructor argument because every runtime but
    /// stormpump would pass nothing. It takes the ring rather than a built
    /// manager so the manager gets *this* kubelet's authenticated apiserver
    /// client — a status written with an unauthenticated one is a status
    /// nobody ever sees.
    ///
    /// `cni` is the node's CNI, for a VMI on the pod network (#88): its own
    /// sandbox is filled by the same plugins a pod's is.
    pub fn with_engine(
        mut self,
        ring: Arc<crate::stormpump_ring::RingClient>,
        cni: Option<cni::CniInvoker>,
    ) -> Self {
        self.runtime_changes = Some(ring.subscribe_exits());
        *self.exit_routes.lock().unwrap_or_else(|e| e.into_inner()) = Some(ring.subscribe_exit_handles());
        self.vms = Some(Arc::new(
            crate::vm_manager::VmManager::new(
                Some(ring),
                &self.config.node_name,
                self.api_client.clone(),
                &self.config.api_server_url,
            )
            .with_storage(self.config.engine.clone())
            .with_claims(self.pod_manager.clone())
            .with_cni(cni)
            .with_max_staleness(self.config.metadata_max_staleness)
            // Each machine's hypervisor identity for cadvisor (#84).
            .with_identities(crate::workload_identity::Publisher::default()),
        ));
        self.snapshots = Some(Arc::new(crate::vm_snapshot::Snapshots::new(
            self.api_client.clone(),
            &self.config.api_server_url,
            &self.config.node_name,
            crate::vm_manager::RUN_ROOT,
            crate::vm_snapshot::stormvm_take(self.config.engine.url().to_string()),
        )
        .with_restore_engine(Arc::new(crate::vm_restore::Stormblock(self.config.engine.url().to_string())))));
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
            Ok((name, version, _)) => {
                if let Some(why) = unsupported_runtime(&name, &version) {
                    anyhow::bail!(why);
                }
                format!("{name}://{version}")
            }
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
            let max_pods = self.config.max_pods;
            let reserved = self.config.reserved.clone();
            let client = self.api_client.clone();
            let labels = self.config.node_labels.clone();
            let annotations = self.config.node_annotations.clone();
            let taints = self.config.register_with_taints.clone();
            let runs_vms = self.vms.is_some();
            tokio::spawn(async move {
                let mut reporter = NodeReporter::with_pod_cidr(&url, &node_name, pod_cidr)
                    .with_runtime_version(rv)
                    .with_kubelet_port(port)
                    .with_max_pods(max_pods)
                    .with_reserved(reserved)
                    // Registration only. The heartbeat reporter below is
                    // deliberately built without these: it writes status
                    // through the /status subresource, and re-asserting taints
                    // every few seconds would undo every removal.
                    .with_registration(labels, annotations, taints)
                    .with_client(client);
                if runs_vms {
                    // KVM on the Node (#65): only where VMs can run at all.
                    reporter = reporter.with_kvm(Arc::new(crate::node_status::kvm_available));
                }
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
                tls_cert_path: self.config.serving_cert_path.clone(),
                tls_key_path: self.config.serving_key_path.clone(),
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
        let max_pods = self.config.max_pods;
        let hb_reserved = self.config.reserved.clone();
        let hb_client = self.api_client.clone();
        let hb_vms = self.vms.clone();
        tokio::spawn(async move {
            let mut reporter = NodeReporter::with_pod_cidr(&reporter_url, &node_name, pod_cidr)
                .with_runtime_version(runtime_version)
                .with_kubelet_port(kubelet_port)
                .with_max_pods(max_pods)
                .with_reserved(hb_reserved)
                .with_client(hb_client);
            if hb_vms.is_some() {
                reporter = reporter.with_kvm(Arc::new(crate::node_status::kvm_available));
            }
            let mut interval = time::interval(heartbeat_interval);
            loop {
                interval.tick().await;
                match reporter.heartbeat().await {
                    // Heard from: guest metadata may keep answering from
                    // the cache (#156).
                    Ok(true) => {
                        if let Some(vms) = &hb_vms {
                            vms.note_apiserver_contact();
                        }
                    }
                    Ok(false) => debug!("Heartbeat: the Lease was not renewed"),
                    Err(e) => error!("Heartbeat failed: {e}"),
                }
            }
        });

        // Mirror the node's own services into the API: when PID 1's asset
        // table changes, and when a mirror pod is edited or deleted (#101).
        tokio::spawn(self.clone().service_mirror_loop());

        // Each engine exit wakes its own Pod or VMI worker (#115).
        tokio::spawn(self.clone().exit_router());
        // A CRI runtime's container events, the same way (#116).
        if self.runtime_changes.is_none() {
            tokio::spawn(self.clone().runtime_event_router());
        }

        // A renewed client certificate is presented without a restart (#77).
        match (&self.client_cert, &self.config.client_cert_path, &self.config.client_key_path) {
            (Some(resolver), Some(cert), Some(key)) => {
                info!("apiserver client certificate {} is reloaded when it changes", cert.display());
                tokio::spawn(reload_client_cert(resolver.clone(), cert.clone(), key.clone()));
            }
            (None, Some(cert), _) => {
                warn!(
                    "apiserver client certificate {} is read once: reloading it needs --apiserver-ca with \
                     verification on; a renewed one is used after a restart",
                    cert.display()
                );
            }
            _ => {}
        }

        // stormblock's volume changes, for the workers below (#101).
        {
            let engine = self.config.engine.clone();
            let changes = self.engine_volumes.clone();
            tokio::spawn(async move {
                engine.follow_volumes(move || changes.send_modify(|v| *v = v.wrapping_add(1))).await
            });
        }

        // List the node's own data containers as PVCs (rustkube-node#49), on
        // engine volume and PV/PVC events (#101).
        tokio::spawn(self.clone().system_claims_loop());

        // The data slabs' room for claims, published and watched (#62).
        tokio::spawn(self.clone().capacity_loop());
        tokio::spawn(self.clone().placement_loop());

        // Containers' logs rotated as upstream's ContainerLogManager does
        // (#216): every 10 s, past --container-log-max-size.
        tokio::spawn(crate::container_logs::monitor("/var/log/pods".into(), self.config.container_log));

        // Pods' ServiceAccount tokens, written again at 80% of their life
        // (#122): on the earliest one's deadline, at most a minute apart so
        // a token written since is not missed.
        {
            let pm = self.pod_manager.clone();
            tokio::spawn(async move {
                loop {
                    let next = pm.refresh_tokens().await.unwrap_or(Duration::from_secs(60));
                    time::sleep(next.clamp(Duration::from_secs(1), Duration::from_secs(60))).await;
                    crate::metrics::observe_timed("tokens", "deadline");
                }
            });
        }

        // Reclaim this node's released claims (reclaimPolicy: Delete), on PV
        // events (#101).
        tokio::spawn(self.clone().reclaim_loop());

        // External CSI drivers (#52): register what appears in
        // plugins_registry, and undo the volumes of pods that are gone
        // (deleted while the kubelet was down, or a teardown due a retry),
        // on Pod events and pending-teardown deadlines (#101).
        tokio::spawn(self.csi.clone().run());
        tokio::spawn(self.clone().csi_sweep_loop());

        if let Some(vms) = &self.vms {
            vms.spawn_address_pump();
        }

        // One bounded executor for both kinds; producers never perform runtime I/O.
        tokio::try_join!(self.pod_loop(), self.vm_loop(), self.vm_maintenance_loop(), async {
            self.workloads.run(self.clone(), self.config.pod_workers).await;
            Ok::<(), anyhow::Error>(())
        })?;
        Ok(())
    }

    /// Route each engine exit to the one worker it concerns (#115). It used
    /// to wake every Pod and VMI worker and re-sync every pod's list, so the
    /// work grew with the node's workloads rather than with what changed.
    async fn exit_router(self: Arc<Self>) {
        let Some(mut exits) = self.exit_routes.lock().unwrap_or_else(|e| e.into_inner()).take() else {
            return;
        };
        loop {
            let wake = match exits.recv().await {
                Ok(handle) => {
                    let pod = self.pod_manager.pod_of_workload(handle).await;
                    let vm = match &self.vms {
                        Some(v) => v.uid_of_handle(handle).await,
                        None => None,
                    };
                    exit_wake(pod, vm)
                }
                // Missed some: nobody knows whose, so everyone looks.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => ExitWake::Everyone,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            };
            match wake {
                ExitWake::Pod(uid) => self.workloads.wake_where(Kind::Pod, |k| k.uid == uid),
                ExitWake::Vm(uid) => self.workloads.wake_where(Kind::VirtualMachine, |k| k.uid == uid),
                ExitWake::Everyone => {
                    debug!("an engine exit no workload here claims: waking every Pod and VMI");
                    self.workloads.wake_kind(Kind::Pod);
                    self.workloads.wake_kind(Kind::VirtualMachine);
                }
            }
        }
    }

    /// Follow a CRI runtime's container events (#116) and wake the Pod each
    /// names. While the stream is open the Pods wait on events instead of
    /// `sync_interval`; when it opens (again), every Pod looks once, because
    /// what happened while it was closed was not reported (upstream's evented
    /// PLEG relists the same way). A runtime without the stream keeps the
    /// timed fallback; a stream that drops is reopened on a backoff.
    async fn runtime_event_router(self: Arc<Self>) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = self.runtime.clone();
        let follow = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                let sent = tx.clone();
                let ended = runtime
                    .follow_container_events(&move |e: crate::cri::RuntimeEvent| {
                        let _ = sent.send(Some(e));
                    })
                    .await;
                // `None`: the stream is closed, the Pods go back to the clock.
                let _ = tx.send(None);
                match ended {
                    crate::cri::EventStream::Unsupported => {
                        info!("the container runtime has no event stream (GetContainerEvents); Pods are looked at every sync_interval");
                        return;
                    }
                    crate::cri::EventStream::Ended(why) => {
                        warn!("container runtime event stream ended ({why}); reopening in {backoff:?}");
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });
        while let Some(event) = rx.recv().await {
            match event {
                Some(crate::cri::RuntimeEvent::Connected) => {
                    info!("following the container runtime's events (GetContainerEvents)");
                    self.runtime_events_live.store(true, Ordering::Release);
                    self.workloads.wake_kind(Kind::Pod);
                }
                Some(crate::cri::RuntimeEvent::Container { pod_uid, container_id }) => {
                    let uid = match pod_uid {
                        Some(uid) => Some(uid),
                        None => self.pod_manager.pod_of_container(&container_id).await,
                    };
                    match uid {
                        Some(uid) => self.workloads.wake_where(Kind::Pod, |k| k.uid == uid),
                        None => debug!("a runtime event for container {container_id}, which no pod here names"),
                    }
                }
                None => {
                    if self.runtime_events_live.swap(false, Ordering::AcqRel) {
                        // Back on the clock: each Pod's next pass sets it.
                        self.workloads.wake_kind(Kind::Pod);
                    }
                }
            }
        }
        let _ = follow.await;
    }

    async fn pod_loop(&self) -> anyhow::Result<()> {
        let worker = self.watches.worker("kubelet-pods");
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
        // The CNI config directory (#148): a pod with no network config yet
        // is woken when it changes, not on a backoff.
        let cni_changed = Arc::new(tokio::sync::Notify::new());
        if let Some(path) = self.config.cni_conf_dir.clone() {
            let changed = cni_changed.clone();
            files.spawn(
                async move { crate::fs_watch::watch(path, move || changed.notify_one()).await },
            );
        }
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                _ = cni_changed.notified() => {
                    let waiting = self.pod_manager.waiting_for_network_config();
                    if !waiting.is_empty() {
                        info!(pods = waiting.len(), "CNI config directory changed: waking the pods waiting for the network");
                        self.workloads.wake_where(Kind::Pod, |k| waiting.contains(&k.uid));
                    }
                    continue;
                },
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
                // An engine exit is routed to its own worker (`exit_router`, #115).
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

    // Snapshots and the VM disk-owner sweep, independent of UID work, on
    // events only (#101): snapshot watches and take completions; VM/VMI
    // watches (an owner going) and engine volume changes (a disk let go).
    async fn vm_maintenance_loop(&self) -> anyhow::Result<()> {
        let Some(vms) = &self.vms else { return std::future::pending().await; };
        let worker = self.watches.worker("kubelet-vm-maintenance");
        let completed = self.snapshots.as_ref().map(|s| s.completions());
        let mut volumes = Some(self.engine_volumes.subscribe());
        let api = &self.config.api_server_url;
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                _ = notified(&completed) => { worker.enqueue(); continue; },
                _ = runtime_changed(&mut volumes) => { worker.enqueue(); continue; },
            };
            worker.run(async {
                if !api.is_empty() {
                    for path in [
                        "/apis/snapshot.kubevirt.io/v1beta1/virtualmachinesnapshots",
                        "/apis/kubevirt.io/v1/virtualmachines",
                        "/apis/kubevirt.io/v1/virtualmachineinstances",
                    ] {
                        self.watches.observe(&self.api_client, format!("{api}{path}"));
                    }
                }
                if let Some(snapshots) = &self.snapshots { snapshots.sync().await; }
                if let Some(after) = vms.sweep_orphans().await {
                    apimachinery::reactor::requeue_after(after);
                }
            }).await;
            drop(work);
        }
    }

    /// The node's volumes as PVs and PVCs (#49), on engine volume changes and
    /// PV/PVC events (#101). The mirror writes only what differs, so its own
    /// writes settle after a pass.
    async fn system_claims_loop(self: Arc<Self>) {
        let worker = self.watches.worker("kubelet-system-claims");
        let mut volumes = Some(self.engine_volumes.subscribe());
        let api = self.config.api_server_url.clone();
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                _ = runtime_changed(&mut volumes) => { worker.enqueue(); continue; },
            };
            worker.run(async {
                if !api.is_empty() {
                    self.watches.observe(&self.api_client, format!(
                        "{api}/api/v1/namespaces/{}/persistentvolumeclaims", crate::system_claims::NAMESPACE));
                    self.watches.observe(&self.api_client, format!("{api}/api/v1/persistentvolumes"));
                }
                crate::system_claims::mirror(&self.api_client, &api, &self.config.engine, &self.config.node_name).await;
            }).await;
            drop(work);
        }
    }

    /// The node's CSIStorageCapacity and slab gauges (#62), on engine volume
    /// changes and every minute: written bytes change free space with no
    /// volume event (counted as a fallback).
    async fn capacity_loop(self: Arc<Self>) {
        const PERIOD: Duration = Duration::from_secs(60);
        let mut volumes = Some(self.engine_volumes.subscribe());
        let events = (!self.config.api_server_url.is_empty()).then(|| {
            crate::events::EventRecorder::new(self.api_client.clone(), &self.config.api_server_url, &self.config.node_name)
        });
        let mut alerted = false;
        loop {
            match crate::capacity::publish(
                &self.api_client,
                &self.config.api_server_url,
                &self.config.engine,
                &self.config.node_name,
                &self.config.storage,
                events.as_ref(),
                alerted,
            )
            .await
            {
                Ok(past) => alerted = past,
                Err(e) => debug!("storage capacity not published: {e}"),
            }
            tokio::select! {
                _ = runtime_changed(&mut volumes) => {}
                _ = tokio::time::sleep(PERIOD) => crate::metrics::observe_timed("capacity", "fallback"),
            }
        }
    }

    /// Each stormblock PV's placement (#60): drives, shelf/bay, RAID partners,
    /// on engine volume changes and every minute (a rebuild's progress and a
    /// drive's health change with no volume event). stormdrive is asked on
    /// the node's address over TLS (`STORMDRIVE_URL` overrides).
    async fn placement_loop(self: Arc<Self>) {
        const PERIOD: Duration = Duration::from_secs(60);
        let mut volumes = Some(self.engine_volumes.subscribe());
        let events = (!self.config.api_server_url.is_empty()).then(|| {
            crate::events::EventRecorder::new(self.api_client.clone(), &self.config.api_server_url, &self.config.node_name)
        });
        // TLS first (stormdrive#19); a stormdrive from before it speaks plain
        // HTTP, and a newer one refuses plain reads, so the order is safe.
        let stormdrive: Vec<String> = match std::env::var("STORMDRIVE_URL").ok().filter(|u| !u.is_empty()) {
            Some(u) => vec![u],
            None => vec![format!("https://{}:9092", self.node_ip), format!("http://{}:9092", self.node_ip)],
        };
        loop {
            match crate::pv_placement::pass(
                &self.api_client,
                &self.config.api_server_url,
                &self.config.engine,
                &stormdrive,
                &self.config.node_name,
                events.as_ref(),
            )
            .await
            {
                Ok(n) if n > 0 => debug!(written = n, "PV placement brought up to date"),
                Ok(_) => {}
                Err(e) => debug!("PV placement not published: {e}"),
            }
            tokio::select! {
                _ = runtime_changed(&mut volumes) => {}
                _ = tokio::time::sleep(PERIOD) => crate::metrics::observe_timed("placement", "fallback"),
            }
        }
    }

    /// `reclaimPolicy: Delete` for this node's released claims, on PV events
    /// (#101). A claim still in use here is looked at again shortly: the pod
    /// holding it going is local, not an API change.
    async fn reclaim_loop(self: Arc<Self>) {
        let worker = self.watches.worker("kubelet-reclaim");
        let api = self.config.api_server_url.clone();
        loop {
            let work = worker.next().await;
            worker.run(async {
                if api.is_empty() { return; }
                self.watches.observe(&self.api_client, format!("{api}/api/v1/persistentvolumes"));
                if self.pod_manager.reclaim_released().await {
                    crate::metrics::observe_timed("reclaim", "deadline");
                    apimachinery::reactor::requeue_after(RECLAIM_PENDING);
                }
            }).await;
            drop(work);
        }
    }

    /// External CSI volumes of pods that are gone, on this node's Pod events
    /// (#101); a teardown that failed is retried on a deadline.
    async fn csi_sweep_loop(self: Arc<Self>) {
        let worker = self.watches.worker("kubelet-csi-sweep");
        let api = self.config.api_server_url.clone();
        loop {
            let work = worker.next().await;
            worker.run(async {
                if api.is_empty() { return; }
                self.watches.observe(&self.api_client, format!(
                    "{api}/api/v1/pods?fieldSelector=spec.nodeName%3D{}", self.config.node_name));
                // A claim grown by the control plane (#42) is a claim event.
                self.watches.observe(&self.api_client, format!("{api}/api/v1/persistentvolumeclaims"));
                let teardown = self.pod_manager.sweep_csi_volumes().await;
                let expansion = self.pod_manager.expand_csi_volumes().await;
                if teardown || expansion {
                    crate::metrics::observe_timed("csi-sweep", "deadline");
                    apimachinery::reactor::requeue_after(CSI_TEARDOWN_PENDING);
                }
            }).await;
            drop(work);
        }
    }

    async fn vm_loop(&self) -> anyhow::Result<()> {
        let Some(vms) = &self.vms else {
            return std::future::pending().await;
        };
        let worker = self.watches.worker("kubelet-vmis");
        let mut volumes = Some(self.pod_manager.subscribe_volume_changes());
        // Two lists, as rustkube's scheduler places a VMI (#85): by
        // `status.nodeName` (what it writes), or, for one placed by hand, by
        // `spec.nodeName`, which it then leaves alone. Listing the first alone
        // never saw a hand-placed VMI at all.
        let urls = crate::vm_manager::placement_urls(&self.config.api_server_url, &self.config.node_name)?;
        let url = urls[0].clone();
        loop {
            let work = tokio::select! {
                work = worker.next() => work,
                _ = runtime_changed(&mut volumes) => { self.workloads.wake_kind(Kind::VirtualMachine); worker.enqueue(); continue; },
            };
            worker
                .run(async {
                    for url in &urls {
                        self.watches.observe(&self.api_client, url.to_string());
                    }
                    let lists = futures::future::join_all(urls.iter().map(|url| async {
                        apimachinery::reactor::check(
                            apimachinery::reflector::list(&self.api_client, url.as_str()).await,
                        )
                    }))
                    .await;
                    // Both, or neither: a list that failed is not an empty one (#35).
                    match lists.into_iter().collect::<Result<Vec<_>, _>>() {
                        Ok(lists) => {
                            let want = crate::vm_manager::placed_on(&lists, &self.config.node_name);
                            self.take_hand_placed(&want);
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
                                if let Ok(response)=self.api_client.get(url.clone()).send_retrying(retry::Policy::API).await {
                                    if response.status().as_u16()==404 {
                                        // No VMIs can exist: metadata answers
                                        // "no such machine", not "cold" (#119).
                                        vms.cache_specs(&[]).await;
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

    /// Write `status.nodeName` on each VMI placed here by hand (#85), as the
    /// scheduler would have, so everything that reads the placement from the
    /// status (the metadata service, a console, migration) sees this node.
    /// Off the worker; a write that fails is tried again on the next pass,
    /// which still lists it by `spec.nodeName`.
    fn take_hand_placed(&self, want: &[Value]) {
        let node = &self.config.node_name;
        for vmi in want.iter().filter(|v| crate::vm_manager::hand_placed_here(v, node)) {
            let (Some(ns), Some(name), Some(uid)) = (
                vmi["metadata"]["namespace"].as_str(),
                vmi["metadata"]["name"].as_str(),
                vmi["metadata"]["uid"].as_str(),
            ) else {
                continue;
            };
            let url = format!(
                "{}/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{name}/status",
                self.config.api_server_url
            );
            let body = crate::vm_manager::take_body(uid, node);
            let (api, name) = (self.api_client.clone(), format!("{ns}/{name}"));
            tokio::spawn(async move {
                match api
                    .patch(&url)
                    .header("content-type", "application/merge-patch+json")
                    .json(&body)
                    .send_retrying(retry::Policy::API)
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        info!(vmi = %name, "placed here by spec.nodeName: status.nodeName written")
                    }
                    Ok(r) => warn!(vmi = %name, status = %r.status(), "could not write status.nodeName"),
                    Err(error) => warn!(vmi = %name, %error, "could not write status.nodeName"),
                }
            });
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
            self.pod_manager.note_seen(&static_pods).await;
            self.seed_claims(Kind::Pod, &static_pods).await?;
            self.workloads.replace_source("static",Kind::Pod,&static_pods)?;
        } else { apimachinery::reactor::failed(); }
        let mut url = reqwest::Url::parse(&format!("{}/api/v1/pods",self.config.api_server_url))?;
        url.query_pairs_mut().append_pair("fieldSelector",&format!("spec.nodeName={}",self.config.node_name));
        self.watches.observe(&self.api_client,url.to_string());
        let list = apimachinery::reactor::check(apimachinery::reflector::list(&self.api_client,url.as_str()).await)?;
        // A VMI's launcher Pod (rustkube#203) is the VM manager's, never run
        // as a pod: its VMI is woken when it appears, and a terminating one
        // is let go once no machine of its VMI runs here (#88).
        self.adopt_launchers(list["items"].as_array().map(|a| a.as_slice()).unwrap_or(&[])).await;
        let want: Vec<_> = list["items"].as_array().unwrap().iter().filter(|p|
            p["spec"]["nodeName"].as_str()==Some(&self.config.node_name)
            && p["metadata"]["annotations"]["kubernetes.io/config.source"].as_str()!=Some("stormpump")
            && !crate::vm_network::is_launcher(p)
            && !p["metadata"]["uid"].as_str().unwrap_or("").starts_with("static-"))
            .cloned().collect();
        self.pod_manager.cache_specs(&want).await;
        // Seen here, as the watch delivers it: not when a worker reaches it (#132).
        self.pod_manager.note_seen(&want).await;
        self.seed_claims(Kind::Pod,&want).await?;
        self.workloads.replace_source("api-pods",Kind::Pod,&want)?;
        self.pods_synced.store(true,Ordering::Release);
        self.observe_volume_dependencies();
        // Dependency collection changes wake only affected claim users.
        self.refresh_claim_dependencies().await;
        Ok(())
    }

    /// This node's launcher Pods (#88, rustkube#203).
    async fn adopt_launchers(&self, pods: &[Value]) {
        for p in pods.iter().filter(|p| p["spec"]["nodeName"].as_str() == Some(&self.config.node_name)) {
            let Some(vmi) = crate::vm_network::launcher_of(p) else { continue };
            if p["metadata"]["deletionTimestamp"].is_null() {
                // A pod-network VMI waits for this Pod: start it now.
                self.workloads.wake_where(Kind::VirtualMachine, |k| k.uid == vmi);
                continue;
            }
            if let Some(vms) = &self.vms {
                if vms.runs(vmi).await {
                    continue; // the machine goes with its VMI; the Pod waits for that
                }
            }
            let (ns, name) = (p["metadata"]["namespace"].as_str().unwrap_or(""), p["metadata"]["name"].as_str().unwrap_or(""));
            let body = serde_json::json!({"apiVersion":"v1","kind":"DeleteOptions","gracePeriodSeconds":0,
                "preconditions":{"uid":p["metadata"]["uid"]}});
            match self.api_client.delete(format!("{}/api/v1/namespaces/{ns}/pods/{name}", self.config.api_server_url))
                .json(&body).send_retrying(retry::Policy::API).await
            {
                Ok(r) if r.status().is_success() || matches!(r.status().as_u16(), 404 | 409) => {
                    info!(pod = %name, namespace = %ns, "launcher Pod let go: no machine of its VMI runs here")
                }
                Ok(r) => debug!(pod = %name, "launcher Pod deletion not confirmed: {}", r.status()),
                Err(e) => debug!(pod = %name, "launcher Pod deletion not confirmed: {e}"),
            }
        }
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
            let response=self.api_client.get(format!("{}/api/v1/namespaces/{ns}/persistentvolumeclaims/{name}",self.config.api_server_url)).send_retrying(retry::Policy::API).await?;
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
                            .send_retrying(retry::Policy::API)
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
                            .send_retrying(retry::Policy::API)
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
                            .send_retrying(retry::Policy::API)
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
                            .send_retrying(retry::Policy::API)
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
        // Each container's previous run (#130).
        let last_runs = self
            .pod_manager
            .last_terminated(source["metadata"]["uid"].as_str().unwrap_or(""));

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
                    "terminated" => {
                        // Upstream's reason (#130): the runtime's, else
                        // Completed / Error.
                        let mut t = serde_json::json!({
                            "exitCode": cs.exit_code,
                            "reason": crate::pod_manager::terminated_reason(&cs.reason, cs.exit_code),
                            "startedAt": nanos_to_rfc3339(cs.started_at),
                            "finishedAt": nanos_to_rfc3339(cs.finished_at)
                        });
                        if !cs.message.is_empty() {
                            t["message"] = serde_json::json!(cs.message);
                        }
                        serde_json::json!({ "terminated": t })
                    }
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
                // Why the last run died, once the next has started (#130).
                if let Some(last) = last_runs.get(&cs.name) {
                    status["lastState"] = last_state(last);
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
                let state = if cs.state == "waiting" {
                    // A sidecar between restarts (#111).
                    serde_json::json!({"waiting": {"reason": cs.reason, "message": cs.message}})
                } else if cs.state == "terminated" {
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
                let mut s = serde_json::json!({
                    "name": cs.name,
                    "state": state,
                    // An ordinary init container is never "ready": it is
                    // finished or it is not. Kubernetes reports ready=true for
                    // a successfully completed one, which is what lets a reader
                    // tell it apart from one still going. A sidecar is ready by
                    // its probe, like an app container (#111).
                    "ready": if cs.restartable { cs.ready } else { cs.succeeded() },
                    "restartCount": cs.restart_count,
                    "image": cs.image,
                    "imageID": cs.image_ref,
                    "containerID": format!("containerd://{}", cs.container_id)
                });
                if cs.restartable {
                    s["started"] = serde_json::json!(cs.started);
                }
                s
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
        // Sidecars' readiness counts too, as upstream's does (#111).
        let all_ready = !update.container_statuses.is_empty()
            && update.container_statuses.iter().all(|cs| cs.ready)
            && update.init_container_statuses.iter().filter(|cs| cs.restartable).all(|cs| cs.ready);
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
                _ if !update.reason.is_empty() => update.reason.as_str(),
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
        // While the watch has not yet delivered this kubelet's own last write,
        // that write is the status to compare with and its revision the one to
        // write on (#141): the probe pass a second after a start found the
        // pre-Running status here and wrote an unchanged one again.
        let uid = source["metadata"]["uid"].as_str().unwrap_or("").to_string();
        let acked = self.acked_status.lock().unwrap_or_else(|e| e.into_inner()).get(&uid).cloned();
        let (base, revision) = status_base(source, acked.as_ref());
        let mut merged = base.as_object().cloned().unwrap_or_default();
        if status.get("message").is_none() {
            merged.remove("message");
            merged.remove("reason");
        }
        merged.extend(status.as_object().unwrap().clone());
        let status = Value::Object(merged);
        if status == base {
            crate::metrics::observe_status_write("skipped");
            return Ok(());
        }
        // Static manifests have no API identity; their mirror publication is
        // separate from managing their local runtime state.
        let Some(revision) = revision else {
            return Ok(());
        };
        let written: Value = self.api_client.put(format!("{path}/status"))
            .timeout(Duration::from_secs(10))
            .json(&serde_json::json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": &update.name, "namespace": &update.namespace,
                    "uid": source["metadata"]["uid"], "resourceVersion": revision},
                "status": status
            })).send_retrying(retry::Policy::API).await?.error_for_status()?.json().await.unwrap_or(Value::Null);
        crate::metrics::observe_status_write("written");
        // What the apiserver now holds, and at which revision, for the next
        // pass that runs before the watch delivers it.
        if let Some(now) = written["metadata"]["resourceVersion"].as_str() {
            self.acked_status.lock().unwrap_or_else(|e| e.into_inner()).insert(
                uid,
                AckedStatus { written_on: revision, now: now.to_string(), status },
            );
        }

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
                    self.acked_status.lock().unwrap_or_else(|e| e.into_inner()).remove(&key.uid);
                    if let Some(object)=&desired {
                        // Never acknowledge deletion of a same-name successor.
                        let response=self.api_client.delete(format!("{}/api/v1/namespaces/{}/pods/{}",self.config.api_server_url,key.namespace,key.name))
                            .json(&serde_json::json!({"apiVersion":"v1","kind":"DeleteOptions","gracePeriodSeconds":0,
                                "preconditions":{"uid":object["metadata"]["uid"]}})).send_retrying(retry::Policy::API).await?;
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
                || self.pod_manager.has_unspecified().await
                || match &self.vms {Some(v)=>v.has_unknown_claims().await,None=>false} {
                return Ok(Next::After(retry));
            }
        }
        let claims=self.claims_for(key,&object).await?;
        if !self.workloads.reservations.acquire(key,&claims) {
            // Refused over a claim another workload here holds (a VM's disk,
            // a ReadWriteOncePod user): say so on the Pod (#80). Released, the
            // holder wakes it.
            if key.kind==Kind::Pod {
                if let Some((claim,holder))=self.workloads.reservations.blockers(key,&claims).into_iter().next() {
                    let why=workload::claim_held_message(&claim,&holder);
                    let update=self.pod_manager.claim_held(&object,why).await;
                    self.report_pod_status(&update,&object).await?;
                }
            }
            return Ok(Next::AwaitEvent)
        }
        let Some(_operations)=self.workloads.reservations.try_operations(&claims) else {return Ok(Next::After(Duration::from_millis(100)))};
        match key.kind {
            Kind::Pod=> {
                // Incomplete collection semantics deliberately suppress orphan
                // sweeping: absence is handled by the deleted UID's own worker.
                let outcome=self.pod_manager.sync_pods_observed(&[object.clone()],false).await;
                for update in &outcome.updates {
                    let sent=std::time::Instant::now();
                    self.report_pod_status(update,&object).await?;
                    if update.phase=="Running" {self.pod_manager.start_reported(&object,sent.elapsed()).await;}
                }
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
        // Each running service's stormd (#215, stormd#48): its processes'
        // state, restarts and readiness for the mirror's container status, and
        // its Kubernetes events onto the mirror pod. A change is a mirror pass.
        let stormd: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<crate::stormd_api::Process>>>> =
            Arc::default();
        {
            let worker = worker.clone();
            let stormd = stormd.clone();
            let (api, url, node) = (self.api_client.clone(), self.config.api_server_url.clone(), self.config.node_name.clone());
            tokio::spawn(stormd_poll(stormd, worker, api, url, node));
        }
        // Each running service's own health endpoint, on its own clock (#96):
        // a flip in readiness is a mirror pass. Only for a service whose
        // stormd does not answer: one that does reports its readiness itself.
        {
            let worker = worker.clone();
            let health = self.service_health.clone();
            let stormd = stormd.clone();
            tokio::spawn(async move {
                let probes = reqwest::Client::new();
                loop {
                    tokio::time::sleep(crate::node_health::PERIOD).await;
                    let urls = crate::node_health::health_urls(std::path::Path::new(crate::node_logs::HOST_ROOT));
                    let text = std::fs::read_to_string(format!("{RUN_DIR}/assets.json")).unwrap_or_default();
                    let running: Vec<String> = crate::mirror::parse_assets(&text)
                        .into_iter()
                        .filter(|a| a.running)
                        .map(|a| a.name)
                        .collect();
                    let mut flipped = false;
                    let answered: Vec<String> = stormd.lock().unwrap_or_else(|e| e.into_inner()).keys().cloned().collect();
                    for name in &running {
                        if answered.contains(name) {
                            continue;
                        }
                        let Some(url) = urls.get(name) else { continue };
                        let result = crate::node_health::probe(&probes, url).await;
                        let mut map = health.lock().unwrap_or_else(|e| e.into_inner());
                        let h = map.entry(name.clone()).or_insert(crate::node_health::ServiceHealth { ready: true, ..Default::default() });
                        if crate::node_health::observe(h, result) {
                            if h.ready {
                                info!("node service {name}: its health endpoint answers again");
                            } else {
                                warn!("node service {name}: not ready: {}", h.reason);
                            }
                            flipped = true;
                        }
                    }
                    // A stopped or unlisted service starts again from ready.
                    health.lock().unwrap_or_else(|e| e.into_inner()).retain(|n, _| running.contains(n));
                    if flipped {
                        worker.enqueue();
                    }
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
                    let health = self.service_health.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    let procs = stormd.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    mirror_node_services(&self.api_client, &url, &node, &self.node_ip, &health, &procs).await;
                })
                .await;
            drop(work);
        }
    }

    /// When a live workload is next looked at with no event (#101): its own
    /// deadline, else only an event. A runtime that reports no exits keeps
    /// `sync_interval` as a counted fallback: a CRI runtime without an open
    /// `GetContainerEvents` stream (#116), or one that has none.
    fn next_look(&self, kind: Kind, due: Option<Duration>) -> Next {
        let worker = match kind { Kind::Pod => "pod", Kind::VirtualMachine => "vmi" };
        if !evented(kind, self.runtime_changes.is_some(), self.runtime_events_live.load(Ordering::Acquire)) {
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

/// Watch the client pair's directory (and look every hour in case an event is
/// missed) and present a renewed pair from the next handshake on (#77). A pair
/// that does not load (half-written, the key not the certificate's) leaves
/// the current one, and is tried again on the next change.
async fn reload_client_cert(
    resolver: Arc<crate::client::ReloadingClientCert>,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
) {
    let changed = Arc::new(tokio::sync::Notify::new());
    let dir = cert.parent().map(|d| d.to_path_buf()).unwrap_or_else(|| cert.clone());
    let notify = changed.clone();
    tokio::spawn(async move { crate::fs_watch::watch(dir, move || notify.notify_one()).await });
    loop {
        let _ = tokio::time::timeout(Duration::from_secs(3600), changed.notified()).await;
        // A writer replacing both files fires twice; the second look sees both.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (Ok(c), Ok(k)) = (std::fs::read(&cert), std::fs::read(&key)) else { continue };
        match resolver.replace(&c, &k) {
            Ok(true) => info!("apiserver client certificate reloaded from {}", cert.display()),
            Ok(false) => {}
            Err(e) => warn!("apiserver client certificate {} changed but does not load yet ({e}); still presenting the previous one", cert.display()),
        }
    }
}

/// Whether a workload of `kind` waits on events rather than the clock (#101,
/// #116): every kind on an engine that reports exits (stormpump); Pods while a
/// CRI runtime's `GetContainerEvents` stream is open. VMIs run only on the
/// engine.
fn evented(kind: Kind, engine_exits: bool, cri_events_live: bool) -> bool {
    engine_exits || (kind == Kind::Pod && cri_events_live)
}

/// `lastState` for a container's previous run (#130).
fn last_state(last: &crate::pod_manager::LastTerminated) -> Value {
    let mut t = serde_json::json!({
        "exitCode": last.exit_code,
        "reason": last.reason,
        "startedAt": nanos_to_rfc3339(last.started_at),
        "finishedAt": nanos_to_rfc3339(last.finished_at),
    });
    if !last.message.is_empty() {
        t["message"] = serde_json::json!(last.message);
    }
    serde_json::json!({ "terminated": t })
}

/// Whom an engine exit wakes (#115).
#[derive(Debug, PartialEq, Eq)]
enum ExitWake {
    Pod(String),
    Vm(String),
    /// A handle no record here names (an adopted workload, a race with its
    /// removal): every Pod and VMI looks, as every exit used to make them.
    Everyone,
}

/// Its own pod, else its own VMI, else everyone.
fn exit_wake(pod: Option<String>, vm: Option<String>) -> ExitWake {
    match (pod, vm) {
        (Some(uid), _) => ExitWake::Pod(uid),
        (None, Some(uid)) => ExitWake::Vm(uid),
        (None, None) => ExitWake::Everyone,
    }
}

/// A container runtime the 1.36 posture no longer supports (#23): containerd
/// before 2.0, which Kubernetes 1.36 dropped. The kubelet refuses to start on
/// it rather than run against a runtime nothing tests it with. Anything else
/// (CRI-O, stormpump, the native runtime, a version that does not parse) is
/// accepted.
fn unsupported_runtime(name: &str, version: &str) -> Option<String> {
    if !name.eq_ignore_ascii_case("containerd") {
        return None;
    }
    let major: u32 = version.trim().trim_start_matches('v').split('.').next()?.parse().ok()?;
    (major < 2).then(|| {
        format!(
            "containerd {version} is not supported: Kubernetes 1.36 requires containerd 2.0 or later \
             (this kubelet reports the 1.36 posture, rustkube-node#23)"
        )
    })
}

#[cfg(test)]
mod runtime_support_tests {
    use super::*;

    /// #116: a CRI runtime's Pods leave the clock only while its event
    /// stream is open; the engine's exits cover every kind.
    #[test]
    fn pods_wait_on_events_only_while_a_stream_reports_them() {
        assert!(evented(Kind::Pod, true, false));
        assert!(evented(Kind::VirtualMachine, true, false));
        assert!(evented(Kind::Pod, false, true));
        assert!(!evented(Kind::Pod, false, false), "no stream: sync_interval");
        assert!(!evented(Kind::VirtualMachine, false, true));
    }

    /// #130: a container's previous run as upstream's lastState.
    #[test]
    fn last_state_is_upstreams_terminated() {
        let last = crate::pod_manager::LastTerminated {
            exit_code: 137,
            reason: "OOMKilled".into(),
            message: String::new(),
            started_at: 1_759_881_600_000_000_000,
            finished_at: 0,
        };
        let v = last_state(&last);
        assert_eq!(v["terminated"]["exitCode"], 137);
        assert_eq!(v["terminated"]["reason"], "OOMKilled");
        assert_eq!(v["terminated"]["startedAt"], "2025-10-08T00:00:00Z");
        assert!(v["terminated"]["finishedAt"].is_null(), "unknown is null, not 1970");
        assert!(v["terminated"].get("message").is_none());
    }

    /// #23: containerd 1.x is refused; 2.x, CRI-O and stormpump are not.
    #[test]
    fn containerd_before_2_is_refused() {
        assert!(unsupported_runtime("containerd", "v1.7.22").unwrap().contains("2.0 or later"));
        assert!(unsupported_runtime("containerd", "1.6.0").is_some());
        assert_eq!(unsupported_runtime("containerd", "v2.0.1"), None);
        assert_eq!(unsupported_runtime("containerd", "2.1.0"), None);
        assert_eq!(unsupported_runtime("cri-o", "1.30.0"), None);
        assert_eq!(unsupported_runtime("stormpump", "0.1.0"), None);
        assert_eq!(unsupported_runtime("containerd", "unknown"), None);
    }
}

#[cfg(test)]
mod exit_wake_tests {
    use super::*;

    /// #115: one exit wakes its own workload's worker, and only it.
    #[test]
    fn one_exit_wakes_its_own_uid_only() {
        assert_eq!(exit_wake(Some("pod-a".into()), None), ExitWake::Pod("pod-a".into()));
        assert_eq!(exit_wake(None, Some("vmi-b".into())), ExitWake::Vm("vmi-b".into()));
        assert_eq!(exit_wake(None, None), ExitWake::Everyone, "unknown: the old behaviour");
    }
    // That `wake_where` queues the picked key alone is `workload::tests::
    // wake_where_wakes_only_the_picked_keys`.
}

/// A claim still mounted here, or a stormblock that refused, is looked at
/// again after this (#101).
const RECLAIM_PENDING: Duration = Duration::from_secs(5);
/// A CSI teardown that did not complete is retried after this (#101).
const CSI_TEARDOWN_PENDING: Duration = Duration::from_secs(10);

/// A notification, or never when there is nothing to be notified by.
async fn notified(notify: &Option<Arc<tokio::sync::Notify>>) {
    match notify {
        Some(n) => n.notified().await,
        None => std::future::pending().await,
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

async fn mirror_node_services(
    client: &reqwest::Client,
    api_url: &str,
    node: &str,
    host_ip: &str,
    health: &std::collections::HashMap<String, crate::node_health::ServiceHealth>,
    stormd: &std::collections::HashMap<String, Vec<crate::stormd_api::Process>>,
) {
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
    // Mirrors of assets PID 1 did not list on this boot (#87): not running,
    // said once, never deleted.
    // The mirrors there are, read once: what exists is compared rather than
    // written blind (#101). Unreadable: nothing is written, and the pass is
    // retried on the reactor's backoff.
    let list_url = format!(
        "{api_url}/api/v1/namespaces/kube-system/pods?labelSelector=storm.io%2Fcomponent%3Dnode-service"
    );
    let listed = match client.get(&list_url).send_retrying(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
        _ => None,
    };
    let Some(list) = listed else {
        apimachinery::reactor::failed();
        return;
    };

    // What changed since the last pass, as events on the mirror pods (#50):
    // Started (with when), Stopped, Failed (a non-zero exit, with its tail),
    // BackOff. First sight is judged against the mirror pod the API has, so a
    // kubelet restart announces nothing that already happened.
    {
        let seen =
            LAST_SEEN.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let now = chrono::Utc::now();
        let uptime = crate::mirror::node_uptime();
        let mut changes = Vec::new();
        {
            let mut last = seen.lock().unwrap_or_else(|e| e.into_inner());
            for a in &assets {
                let started = (now - chrono::Duration::seconds(a.age(uptime) as i64))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string();
                let name = crate::mirror::mirror_name(&a.name, node);
                let existing = list["items"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|p| p["metadata"]["name"].as_str() == Some(name.as_str()));
                if let Some(t) = crate::mirror::transition(last.get(&a.name).copied(), a, existing, &started) {
                    changes.push((existing.cloned(), name, t));
                }
                last.insert(a.name.clone(), (a.running, a.restarts));
            }
        }
        for (existing, name, (etype, reason, message)) in changes {
            let pod = existing.unwrap_or_else(|| serde_json::json!({
                "metadata": { "name": name, "namespace": "kube-system", "uid": "" }
            }));
            if let Some(r) = &events {
                r.pod_event(&pod, etype, reason, &message).await;
            }
        }
    }
    for pod in crate::mirror::stale_mirrors(&list, node, &assets) {
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let marked = crate::mirror::not_started(pod);
        let put = client
            .put(format!("{api_url}/api/v1/namespaces/kube-system/pods/{name}/status"))
            .json(&marked)
            .send_retrying(retry::Policy::API)
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
    let uptime = crate::mirror::node_uptime();
    // Each service's golden, from the release the node booted (#130).
    let release = crate::image_config::RELEASE_MANIFESTS
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok())
        .unwrap_or_default();
    for a in &assets {
        let golden = crate::image_config::release_golden_in(&release, &a.name);
        // startTime from the service's age (#193): the node's uptime less PID
        // 1's `started_secs` (one clock, CLOCK_BOOTTIME), which stays right
        // between writes of assets.json; else the `age_secs` the file says.
        let started = (now - chrono::Duration::seconds(a.age(uptime) as i64))
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
                let mut pod = crate::mirror::mirror_pod_with(a, node, "", &started, health.get(&a.name));
                crate::mirror::with_provenance(&mut pod, golden.as_ref(), host_ip);
                let from_stormd = apply_stormd(&mut pod, a, stormd);
                // The golden's annotations (#130) are metadata, which the
                // status write below does not carry: patched when they differ.
                let want = golden.as_ref().map(crate::mirror::golden_annotations).unwrap_or_default();
                if want.iter().any(|(k, v)| existing["metadata"]["annotations"][k] != *v) {
                    let patched = client
                        .patch(format!("{base}/{name}"))
                        .header("content-type", "application/merge-patch+json")
                        .json(&serde_json::json!({"metadata": {"annotations": want}}))
                        .send_retrying(retry::Policy::API)
                        .await;
                    if !matches!(patched, Ok(ref r) if r.status().is_success()) {
                        apimachinery::reactor::failed();
                    }
                }
                if crate::mirror::status_current(existing, &pod) {
                    continue;
                }
                // Ready to not ready on its health endpoint: say so (#96).
                let was_ready = existing["status"]["containerStatuses"][0]["ready"] == true;
                // (stormd's own Unhealthy events say it when stormd answers.)
                if let (true, false, Some(h), Some(r)) =
                    (was_ready, from_stormd, health.get(&a.name).filter(|h| a.running && !h.ready), &events)
                {
                    r.pod_event(existing, "Warning", "Unhealthy", &format!("Readiness probe failed: {}", h.reason)).await;
                }
                let mut pod = pod;
                pod["metadata"] = existing["metadata"].clone();
                let put = client.put(format!("{base}/{name}/status")).json(&pod).send_retrying(retry::Policy::API).await;
                if !matches!(put, Ok(ref r) if r.status().is_success() || r.status() == 409) {
                    apimachinery::reactor::failed();
                }
            }
            None => {
                if node_uid.is_none() {
                    node_uid = match client.get(format!("{api_url}/api/v1/nodes/{node}")).send_retrying(retry::Policy::API).await {
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
                let mut pod = crate::mirror::mirror_pod_with(a, node, uid, &started, health.get(&a.name));
                crate::mirror::with_provenance(&mut pod, golden.as_ref(), host_ip);
                apply_stormd(&mut pod, a, stormd);
                let created = client.post(&base).json(&pod).send_repeatable(retry::Policy::API).await;
                if !matches!(created, Ok(ref r) if r.status().is_success() || r.status() == 409) {
                    apimachinery::reactor::failed();
                }
            }
        }
    }
}

/// A running service's container status from its stormd (#215), when stormd
/// answered. Returns whether it did.
fn apply_stormd(
    pod: &mut Value,
    a: &crate::mirror::Asset,
    stormd: &std::collections::HashMap<String, Vec<crate::stormd_api::Process>>,
) -> bool {
    if !a.running {
        return false;
    }
    match stormd.get(&a.name).and_then(|procs| crate::stormd_api::representative(&a.name, procs)) {
        Some(p) => {
            crate::mirror::with_stormd(pod, &a.name, &p, a.restarts);
            true
        }
        None => false,
    }
}

/// Ask every running host-network service's stormd, every
/// [`crate::stormd_api::PERIOD`], for its processes and its events (#215).
///
/// Processes: kept in `stormd` by service, a change enqueues a mirror pass. A
/// stormd that does not answer is dropped from the map (its mirror falls back
/// to PID 1's view and the #96 health probe). Events: read from the last
/// `seq` written (0 after a kubelet restart, or when PID 1 has restarted the
/// service, whose new stormd counts from 1), each written as an Event on the
/// mirror pod; the seq moves past an event only once it is written.
///
/// Not retried within a pass: this is a poll, and the next one is the retry.
async fn stormd_poll(
    stormd: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<crate::stormd_api::Process>>>>,
    worker: apimachinery::reactor::Worker,
    api: reqwest::Client,
    api_url: String,
    node: String,
) {
    use crate::stormd_api::{self, Endpoint};
    let http = reqwest::Client::builder().timeout(stormd_api::TIMEOUT).build().unwrap_or_default();
    let mut endpoints = std::collections::HashMap::new();
    let mut read_at: Option<std::time::Instant> = None;
    let mut said: std::collections::HashSet<String> = Default::default();
    // Per service: the last event seq written, and the service's run (PID 1's
    // restarts, started_secs) it belongs to.
    let mut seqs: std::collections::HashMap<String, (u64, (u32, Option<u64>))> = Default::default();
    loop {
        tokio::time::sleep(stormd_api::PERIOD).await;
        if read_at.is_none_or(|t| t.elapsed() > Duration::from_secs(60)) {
            endpoints = stormd_api::endpoints(std::path::Path::new(crate::node_logs::HOST_ROOT));
            read_at = Some(std::time::Instant::now());
        }
        let text = std::fs::read_to_string("/run/stormpump/assets.json").unwrap_or_default();
        let assets: Vec<_> = crate::mirror::parse_assets(&text).into_iter().filter(|a| a.running).collect();
        let mut now: std::collections::HashMap<String, Vec<stormd_api::Process>> = Default::default();
        for a in &assets {
            let base = match endpoints.get(&a.name) {
                Some(Endpoint::Plain(u)) => u.clone(),
                Some(Endpoint::Guarded(why)) => {
                    if said.insert(a.name.clone()) {
                        info!("node service {}: its stormd API is not read ({why}): its mirror shows PID 1's view", a.name);
                    }
                    continue;
                }
                None => continue,
            };
            let procs = match http.get(format!("{base}/api/v1/processes")).send().await {
                Ok(r) if r.status().is_success() => r.json::<Value>().await.ok().map(|v| stormd_api::parse_processes(&v)),
                _ => None,
            };
            let Some(procs) = procs.filter(|p| !p.is_empty()) else { continue };
            now.insert(a.name.clone(), procs);
            if api_url.is_empty() {
                continue;
            }
            let run = (a.restarts, a.started_secs.map(|s| s as u64));
            let entry = seqs.entry(a.name.clone()).or_insert((0, run));
            if entry.1 != run {
                *entry = (0, run);
            }
            let events = match http.get(format!("{base}/api/v1/events?since={}", entry.0)).send().await {
                Ok(r) if r.status().is_success() => r.json::<Value>().await.map(|v| stormd_api::parse_events(&v)).unwrap_or_default(),
                // An older stormd (no events route): nothing to publish.
                _ => Vec::new(),
            };
            if events.is_empty() {
                continue;
            }
            let name = crate::mirror::mirror_name(&a.name, &node);
            let pod_url = format!("{api_url}/api/v1/namespaces/kube-system/pods/{name}");
            let pod = match api.get(&pod_url).send_retrying(retry::Policy::API).await {
                Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
                _ => None,
            };
            // No mirror yet: its events wait for it.
            let Some(pod) = pod else { continue };
            for e in events {
                let obj = stormd_api::event_object(&pod, &a.name, &node, &e);
                if write_event(&api, &api_url, &obj).await {
                    entry.0 = entry.0.max(e.seq);
                } else {
                    break;
                }
            }
        }
        let changed = {
            let mut map = stormd.lock().unwrap_or_else(|e| e.into_inner());
            let changed = *map != now;
            *map = now;
            changed
        };
        if changed {
            worker.enqueue();
        }
    }
}

/// Create an Event, or bring an existing one's count and times up to date.
async fn write_event(api: &reqwest::Client, api_url: &str, obj: &Value) -> bool {
    let base = format!("{api_url}/api/v1/namespaces/kube-system/events");
    match api.post(&base).json(obj).send_repeatable(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => true,
        Ok(r) if r.status() == reqwest::StatusCode::CONFLICT => {
            let name = obj["metadata"]["name"].as_str().unwrap_or("");
            let patch = serde_json::json!({
                "count": obj["count"], "lastTimestamp": obj["lastTimestamp"], "eventTime": obj["eventTime"],
            });
            matches!(
                api.patch(format!("{base}/{name}"))
                    .header("content-type", "application/merge-patch+json")
                    .json(&patch)
                    .send_retrying(retry::Policy::API)
                    .await,
                Ok(r) if r.status().is_success()
            )
        }
        Ok(r) => {
            warn!("apiserver refused a node service's event {}: {}", obj["reason"], r.status());
            // A refusal of this event is not retried for ever.
            true
        }
        Err(e) => {
            debug!("node service event not written: {e}");
            false
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
/// A pod status this kubelet wrote and the apiserver acknowledged (#141).
#[derive(Debug, Clone)]
struct AckedStatus {
    /// The revision the write was made on.
    written_on: String,
    /// The revision the apiserver answered with.
    now: String,
    status: Value,
}

/// The status to compare a new one with, and the revision to write it on
/// (#141). The watch's copy (`source`), unless it is still at the revision
/// this kubelet's last acknowledged write was made on: then that write is
/// newer than the copy, and is what the apiserver holds. `None` revision: an
/// object with none (a static manifest's), which is not written.
fn status_base(source: &Value, acked: Option<&AckedStatus>) -> (Value, Option<String>) {
    let rv = source["metadata"]["resourceVersion"].as_str();
    match acked {
        Some(a) if rv == Some(a.written_on.as_str()) => (a.status.clone(), Some(a.now.clone())),
        _ => (source["status"].clone(), rv.map(String::from)),
    }
}

#[cfg(test)]
mod status_base_tests {
    use super::*;

    fn pod(rv: &str, phase: &str) -> Value {
        serde_json::json!({"metadata": {"uid": "u", "resourceVersion": rv}, "status": {"phase": phase}})
    }

    fn acked() -> AckedStatus {
        AckedStatus { written_on: "10".into(), now: "11".into(), status: serde_json::json!({"phase": "Running"}) }
    }

    #[test]
    fn the_watch_behind_our_own_write_compares_with_the_write_and_writes_on_its_revision() {
        // The watch still shows the pre-Running object our PUT was made on.
        let (base, rv) = status_base(&pod("10", "Pending"), Some(&acked()));
        assert_eq!((base, rv.as_deref()), (serde_json::json!({"phase": "Running"}), Some("11")));
    }

    #[test]
    fn the_watch_caught_up_or_moved_on_is_the_base() {
        let (base, rv) = status_base(&pod("11", "Running"), Some(&acked()));
        assert_eq!((base["phase"].as_str(), rv.as_deref()), (Some("Running"), Some("11")));
        // Someone else wrote after us: their object is the truth.
        let (base, rv) = status_base(&pod("15", "Failed"), Some(&acked()));
        assert_eq!((base["phase"].as_str(), rv.as_deref()), (Some("Failed"), Some("15")));
        // Nothing acknowledged yet.
        let (base, rv) = status_base(&pod("3", "Pending"), None);
        assert_eq!((base["phase"].as_str(), rv.as_deref()), (Some("Pending"), Some("3")));
        // A static manifest has no revision and is never written.
        let (_, rv) = status_base(&serde_json::json!({"metadata": {}, "status": {}}), None);
        assert!(rv.is_none());
    }
}

fn pod_initialized(declared: usize, reports: &[InitContainerStatusReport]) -> bool {
    if declared == 0 {
        return true;
    }
    // A sidecar counts once it has started (#111).
    reports.len() == declared && reports.iter().all(|cs| cs.initialized())
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
            ..Default::default()
        }
    }

    /// #111: a sidecar is initialized once it has started, not once it exits.
    #[test]
    fn a_started_sidecar_counts_as_initialized() {
        let mut sc = report("proxy", 0);
        sc.state = "running".into();
        sc.restartable = true;
        assert!(!pod_initialized(2, &[report("setup", 0), sc.clone()]), "not started yet");
        sc.started = true;
        assert!(pod_initialized(2, &[report("setup", 0), sc]));
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
