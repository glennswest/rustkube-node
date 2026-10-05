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
    PodSandboxConfig, RuntimeService, SeLinuxOptions, SeccompProfile,
};
use crate::health::{run_probe, ProbeResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

mod csi_volumes;
mod deadlines;

/// An image's resolution and when it finished (#132).
type ImageResult = Arc<std::sync::Mutex<Option<(Result<String, String>, Instant)>>>;
type ImageKey = (String, String);

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
    /// Per-container: has the startupProbe passed yet? Until it does (or when
    /// there is no startupProbe), liveness/readiness are suppressed so a slow-
    /// starting container (e.g. cilium-agent) isn't killed before it's up.
    pub startup_passed: HashMap<String, bool>,
    /// When each container was last (re)started, for initialDelaySeconds.
    pub started: HashMap<String, Instant>,
    /// Containers that terminated for good (no restart) → exit code.
    pub terminated: HashMap<String, i32>,
    /// What each init container did, kept for the life of the pod.
    ///
    /// Held here rather than recomputed because init containers are removed
    /// once they exit: the runtime cannot be asked afterwards, so if this is
    /// not recorded at the moment it happens it is gone. It also has to
    /// survive every later sync — `check_pod_status` builds a fresh status
    /// each cycle, and an init report that existed only at start would appear
    /// once and then vanish, which is worse than never reporting it.
    pub init_statuses: Vec<InitContainerStatusReport>,
    /// The sandbox was stopped once the pod finished (#137): its network
    /// (and pod IP) given back, its containers' records and logs kept until
    /// the Pod object goes.
    pub sandbox_stopped: bool,
}

/// What one init container did.
///
/// Separate from [`ContainerStatusReport`] because the questions are not the
/// same. An app container is asked whether it is ready and how often it has
/// restarted; an init container runs once, and what is wanted is whether it
/// finished, with what code, and when.
#[derive(Debug, Clone)]
pub struct InitContainerStatusReport {
    pub name: String,
    pub container_id: String,
    /// `terminated`, `running` or `waiting`.
    pub state: String,
    pub exit_code: i32,
    /// `Completed`, `Error`, or empty while it is still going.
    pub reason: String,
    pub message: String,
    pub image: String,
    pub image_ref: String,
    /// Epoch nanoseconds, as the runtime reports them. Zero when unknown.
    pub started_at: i64,
    pub finished_at: i64,
}

impl InitContainerStatusReport {
    /// Did this one finish successfully?
    pub fn succeeded(&self) -> bool {
        self.state == "terminated" && self.exit_code == 0
    }
}

/// What [`PodManager::release_claim_volume`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeRelease {
    /// The clone was deleted.
    Released,
    /// There was no such volume on this node — already released, or never
    /// provisioned here. Success, so a retrying controller settles.
    Absent,
    /// Refused: the named pod on this node still has the claim.
    InUse(String),
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
/// A pod volume, resolved to something the runtime can mount.
///
/// Make a `hostPath` source exist, if its declared type says the kubelet
/// should.
///
/// **`FileOrCreate` was not handled at all**, and that is what stopped
/// Cilium's agent: `/run/xtables.lock` is declared `FileOrCreate`, was never
/// created, and the spawn failed with `No such file or directory (attaching
/// mounts)` — an ENOENT naming no path, for a file the kubelet was supposed to
/// have made.
///
/// The types, as Kubernetes defines them:
///
/// | type | meaning |
/// |---|---|
/// | `DirectoryOrCreate` | create the directory if absent |
/// | `FileOrCreate` | create an empty file if absent (and its parent) |
/// | `Directory`, `File`, `Socket`, `CharDevice`, `BlockDevice` | **must already exist** |
/// | unset | no checks; the mount simply uses the path |
///
/// The "must already exist" types are deliberately not created: `Directory`
/// means "I expect this to be here", and silently creating an empty one turns
/// a misconfiguration into a container that starts and behaves strangely. What
/// this does instead is say so, because the alternative is the ENOENT above.
/// Where the host's filesystem is reachable from inside the kubelet.
///
/// **The kubelet runs in a container and the engine mounts in the host's
/// namespace.** A `create_dir_all` on this side makes the directory here,
/// where nothing will look for it; the mount then fails with an ENOENT naming
/// no path. Cilium's `/opt/cni/bin` failed exactly that way.
///
/// Absent on a kubelet that is not containerised, in which case the paths are
/// already the host's and no prefix is needed.
const HOST_ROOT: &str = "/hostroot";

/// The path as the *host* sees it, for creating something the engine will
/// later mount.
fn on_host(path: &str) -> std::path::PathBuf {
    let root = std::path::Path::new(HOST_ROOT);
    if root.is_dir() {
        root.join(path.trim_start_matches('/'))
    } else {
        std::path::PathBuf::from(path)
    }
}

fn ensure_host_path(path: &str, typ: &str) {
    match typ {
        "DirectoryOrCreate" => {
            let target = on_host(path);
            if let Err(e) = std::fs::create_dir_all(&target) {
                warn!(
                    "hostPath {path}: could not create {} : {e}",
                    target.display()
                );
            }
        }
        "FileOrCreate" => {
            let target = on_host(path);
            if target.exists() {
                return;
            }
            if let Some(parent) = target.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    warn!("hostPath {path}: could not create parent directory: {e}");
                    return;
                }
            }
            // create_new so a race with another pod does not truncate a file
            // something is already holding — xtables.lock is a lock file, and
            // truncating one under its holder is worse than losing the race.
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)
            {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => warn!("hostPath {path}: could not create file: {e}"),
            }
        }
        "" => {}
        // Must already exist. Say so rather than leaving the spawn to fail
        // with an errno and no path.
        other => {
            if !on_host(path).exists() {
                warn!(
                    "hostPath {path} has type {other}, which requires it to exist \
                     already, and it does not — the container will fail to start"
                );
            }
        }
    }
}

/// `fstype` is `None` for everything that is a directory to bind — hostPath,
/// configMap, secret, projected, emptyDir. A PersistentVolumeClaim resolves to
/// a **block device** and carries the filesystem on it, because the runtime
/// mounts it inside the container rather than binding a path the host already
/// has. Keeping that explicit rather than sniffing a `/dev/` prefix: the
/// difference between a bind and a mount decides whether a wrong answer is a
/// missing file or a corrupted one.
///
/// `block` is a claim of `volumeMode: Block` (#67): `path` is its device, bound
/// as it is at a container's `volumeDevices[].devicePath`, with no filesystem.
#[derive(Debug, Clone, Default)]
pub struct ResolvedVolume {
    pub path: String,
    pub fstype: Option<String>,
    pub block: bool,
}

/// Where a node service's logs are (#72, #124): its stormd log volume, and
/// what PID 1 recorded about its last exit. At least one is present.
#[derive(Debug, Clone)]
pub struct NodeService {
    pub log_dir: Option<std::path::PathBuf>,
    pub record: Option<crate::node_logs::Record>,
}

pub struct PodManager {
    runtime: Arc<dyn RuntimeService>,
    images: Arc<dyn ImageService>,
    pods: RwLock<HashMap<String, PodState>>, // uid → PodState
    node_name: String,
    /// API server base URL, for reading ConfigMaps/Secrets referenced by env
    /// `valueFrom` and configMap/secret volumes. Empty → those reads are skipped.
    api_url: String,
    api_client: reqwest::Client,
    /// Emits pod events, so a failure reaches `oc describe` rather than only a
    /// log on a node with no shell.
    events: Option<crate::events::EventRecorder>,
    /// This node's IP, for the downward API `status.hostIP`.
    node_ip: String,
    /// stormblock's management API on this node. Volumes for
    /// PersistentVolumeClaims are created and attached through it.
    storage_url: String,
    /// The client for it, carrying the engine's token (#66). The apiserver
    /// client's token means nothing to the engine.
    engine: crate::engine::EngineClient,
    /// Kubelet state root; per-pod volume dirs live under `<state_root>/pods`.
    /// Overridable in tests. Default `/var/lib/kubelet`.
    state_root: String,
    /// The cluster DNS service address(es) a pod's resolv.conf points at.
    cluster_dns: Vec<String>,
    /// The cluster domain the search path is built from.
    cluster_domain: String,
    /// Cluster CA (PEM), written into ServiceAccount `ca.crt` so in-cluster
    /// clients can verify a TLS apiserver.
    ca_pem: Option<Vec<u8>>,
    /// Per-container restart backoff, so a container that keeps dying is not
    /// recreated on every sync tick (#25).
    backoff: crate::crashloop::CrashLoopBackoff,
    /// The external CSI drivers registered on this node (#52). Empty until
    /// the kubelet's registration loop finds one, and a claim of an
    /// unregistered driver waits.
    csi: Arc<crate::csi_plugins::CsiPlugins>,
    /// PID 1's mountinfo, where a published CSI volume must appear before a
    /// pod is given it (`csi::is_mount_point`). Overridable in tests.
    csi_mountinfo: String,
    /// When each pod (by uid) was first seen and not yet started, for
    /// `kubelet_pod_start_duration_seconds` (#36).
    first_seen: std::sync::Mutex<HashMap<String, Instant>>,
    /// Pods (by uid) waiting on the pod network (#148): `None` for no CNI
    /// config yet (woken by the config directory changing), `Some(at)` for a
    /// config whose ADD has failed since `at` (retried on a backoff from then).
    network_waits: std::sync::Mutex<HashMap<String, Option<Instant>>>,
    /// Each starting pod's phases (by uid), from seen to `Running`
    /// acknowledged (#132).
    timings: std::sync::Mutex<HashMap<String, crate::start_timing::StartTiming>>,
    /// Pods (by uid) that are this node's but not started, and what they wait
    /// on (#63). Without this a pod waiting on its claim was known to nobody:
    /// `logs` said "not found on this node" and it had no container statuses.
    waiting: std::sync::Mutex<HashMap<String, WaitingPod>>,
    /// Blanks being minted in the background, by name (#63). A 1 TiB blank
    /// takes minutes to format, and the sync loop must not wait for it.
    minting: Arc<std::sync::Mutex<HashMap<String, Mint>>>,
    volume_changes: tokio::sync::watch::Sender<u64>,
    /// Where the host's root is seen, for the node services' logs (#72).
    host_root: std::path::PathBuf,
    /// PID 1's asset table, for a node service's last exit and output (#124).
    assets_json: std::path::PathBuf,
    admission: Option<Arc<crate::workload::Reservations>>,
    /// The executor's pool, for the start-timing annotation (#138).
    load: Option<Arc<crate::workload::Load>>,
    start_images: std::sync::Mutex<HashMap<String,HashMap<ImageKey,ImageResult>>>,
    image_inflight: Arc<std::sync::Mutex<HashMap<ImageKey,ImageResult>>>,
    image_slots: Arc<tokio::sync::Semaphore>,
    image_changes: tokio::sync::broadcast::Sender<String>,
    csi_operations: std::sync::Mutex<HashMap<(String,String),Arc<tokio::sync::Mutex<()>>>>,
    /// Probe runs and each pod's next event-less look (#101).
    deadlines: std::sync::Mutex<deadlines::Deadlines>,
    /// Pods whose start waits only on an event (an init container's exit):
    /// no retry backoff for them, only their own deadline (#101).
    event_waits: std::sync::Mutex<std::collections::HashSet<String>>,
}

/// A pod this node has admitted and not started, and why.
#[derive(Debug, Clone)]
pub struct WaitingPod {
    pub namespace: String,
    pub name: String,
    /// What it waits on, as `describe` shows it.
    pub reason: String,
}

/// A background mint of a size-class blank.
#[derive(Debug, Clone)]
enum Mint {
    InFlight,
    /// The engine refused it. Reported once, then the next claim tries again.
    Failed(String),
}

/// How long a pod waits on its volumes before the reason says it timed out.
///
/// The pod keeps waiting and retrying past it, as upstream's does: a mount
/// timeout is an Event and a message, not a Failed pod, because the volume may
/// still come (a 1 TiB blank formatting, an engine restarting).
pub const VOLUME_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);


/// How long a start waits for an image it has just asked for before it
/// yields the worker (#134): far above a local resolution (0.1 ms on
/// pvetest1), far below a registry pull.
const IMAGE_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

impl PodManager {
    pub fn subscribe_volume_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.volume_changes.subscribe()
    }

    pub fn new(
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        node_name: &str,
    ) -> Self {
        Self::with_api(
            runtime,
            images,
            node_name,
            "",
            "127.0.0.1",
            reqwest::Client::new(),
        )
    }

    pub fn with_api(
        runtime: Arc<dyn RuntimeService>,
        images: Arc<dyn ImageService>,
        node_name: &str,
        api_url: &str,
        node_ip: &str,
        api_client: reqwest::Client,
    ) -> Self {
        Self {
            runtime,
            images,
            pods: RwLock::new(HashMap::new()),
            admission: None,
            load: None,
            start_images: Default::default(),
            image_inflight: Default::default(),
            image_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            image_changes: tokio::sync::broadcast::channel(128).0,
            csi_operations: Default::default(),
            node_name: node_name.to_string(),
            events: (!api_url.is_empty())
                .then(|| crate::events::EventRecorder::new(api_client.clone(), api_url, node_name)),
            api_url: api_url.trim_end_matches('/').to_string(),
            api_client,
            node_ip: node_ip.to_string(),
            storage_url: crate::engine::DEFAULT_URL.to_string(),
            engine: crate::engine::EngineClient::default(),
            state_root: "/var/lib/kubelet".to_string(),
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            ca_pem: None,
            backoff: crate::crashloop::CrashLoopBackoff::new(),
            csi: Arc::new(crate::csi_plugins::CsiPlugins::new(node_name)),
            csi_mountinfo: "/proc/1/mountinfo".to_string(),
            first_seen: std::sync::Mutex::new(HashMap::new()),
            network_waits: Default::default(),
            timings: Default::default(),
            deadlines: Default::default(),
            event_waits: Default::default(),
            waiting: std::sync::Mutex::new(HashMap::new()),
            minting: Arc::new(std::sync::Mutex::new(HashMap::new())),
            volume_changes: tokio::sync::watch::channel(0).0,
            host_root: crate::node_logs::HOST_ROOT.into(),
            assets_json: crate::node_logs::ASSETS_JSON.into(),
        }
    }

    /// See the host's root here rather than at `/hostroot`: for tests.
    pub fn subscribe_image_changes(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.image_changes.subscribe()
    }

    /// Prepare all startup images off the worker pool, coalescing concurrent
    /// pulls of the same image/policy. Completed results belong to this startup,
    /// so a later Always request resolves its tag again.
    fn prepare_images(&self,pod:&Value)->Result<(),CriError> {
        let uid=pod["metadata"]["uid"].as_str().unwrap_or("");
        let mut starts=self.start_images.lock().unwrap();
        if !starts.contains_key(uid) {self.timing(pod,|t|t.image_asked(Instant::now()));}
        let results=starts.entry(uid.into()).or_default();
        for spec in ["initContainers","containers"].iter().flat_map(|field|
            pod["spec"][*field].as_array().into_iter().flatten()) {
            let image=spec["image"].as_str().unwrap_or("");
            let key=(image.to_string(),effective_pull_policy(spec,image).to_string());
            results.entry(key.clone()).or_insert_with(|| {
                let mut inflight=self.image_inflight.lock().unwrap();
                if let Some(result)=inflight.get(&key) {return result.clone();}
                let result:ImageResult=Arc::new(std::sync::Mutex::new(None));
                inflight.insert(key.clone(),result.clone());
                let state=result.clone(); let images=self.images.clone();
                let all=self.image_inflight.clone(); let slots=self.image_slots.clone();
                let changed=self.image_changes.clone();
                tokio::spawn(async move {
                    let _slot=slots.acquire_owned().await.unwrap();
                    let (image,policy)=&key;
                    let answer=match policy.as_str() {
                        "Never"=>match images.image_status(image).await {
                            Ok(Some(info))=>Ok(image_present_ref(&info,image)),
                            Ok(None)=>Err(CriError::ImagePull(format!("image {image} not present and imagePullPolicy is Never"))),
                            Err(error)=>Err(error),
                        },
                        "IfNotPresent"=>match images.image_status(image).await {
                            Ok(Some(info))=>Ok(image_present_ref(&info,image)),
                            _=>images.pull_image(image).await,
                        },
                        _=>images.pull_image(image).await,
                    };
                    *state.lock().unwrap()=Some((answer.map_err(|e|e.to_string()),Instant::now()));
                    all.lock().unwrap().remove(&key);
                    let _=changed.send(image.clone());
                });
                result
            });
        }
        let mut resolved=None::<Instant>;
        for (key,result) in results.iter() {
            match result.lock().unwrap().as_ref() {
                None=>return Err(CriError::Pending(format!("waiting for image {}",key.0))),
                Some((Err(error),_))=>return Err(CriError::ImagePull(error.clone())),
                Some((Ok(_),done))=>resolved=Some(resolved.map_or(*done,|r|r.max(*done))),
            }
        }
        drop(starts);
        if let Some(done)=resolved {self.timing(pod,|t|t.image_resolved(done));}
        Ok(())
    }

    /// [`Self::prepare_images`], waiting up to [`IMAGE_GRACE`] for an image
    /// it has only just asked for (#134). The resolution runs in a task, so
    /// the first look always found it unanswered: every pod's first attempt
    /// returned "waiting for image", wrote a ContainerCreating status, and was
    /// started by a second attempt, for a golden that resolves in 0.1 ms. A
    /// pull that takes longer than the grace still yields the worker, and the
    /// image's completion wakes the pod as before.
    async fn ready_images(&self,pod:&Value)->Result<(),CriError> {
        // Subscribed before the first look: a resolution that finishes
        // between the look and the wait is not missed.
        let mut changed=self.image_changes.subscribe();
        let deadline=tokio::time::Instant::now()+IMAGE_GRACE;
        loop {
            match self.prepare_images(pod) {
                Err(CriError::Pending(what))=>{
                    match tokio::time::timeout_at(deadline,changed.recv()).await {
                        Err(_)|Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))=>return Err(CriError::Pending(what)),
                        Ok(_)=>{}
                    }
                }
                other=>return other,
            }
        }
    }

    async fn startup_image(&self,pod:&Value,image:&str,spec:&Value)->Result<String,CriError> {
        if self.admission.is_none() {return self.ensure_image(image,spec).await;}
        let key=(image.to_string(),effective_pull_policy(spec,image).to_string());
        let starts=self.start_images.lock().unwrap();
        let result=starts.get(pod["metadata"]["uid"].as_str().unwrap_or(""))
            .and_then(|images|images.get(&key)).ok_or_else(||CriError::Pending(format!("waiting for image {image}")))?;
        let value=result.lock().unwrap().clone().map(|(r,_)|r);
        match value {
            Some(Ok(reference))=>Ok(reference),
            Some(Err(error))=>Err(CriError::ImagePull(error)),
            None=>Err(CriError::Pending(format!("waiting for image {image}"))),
        }
    }

    pub fn with_admission(mut self, admission: Arc<crate::workload::Reservations>) -> Self {
        self.admission = Some(admission);
        self
    }

    pub fn with_load(mut self, load: Arc<crate::workload::Load>) -> Self {
        self.load = Some(load);
        self
    }

    /// A pod adopted from the runtime whose spec no list has given yet. Read
    /// under the lock: every pass asks, and cloning every pod's state for it
    /// was O(pods) per pass (#138).
    pub async fn has_unspecified(&self) -> bool {
        self.pods.read().await.values().any(|p| p.pod.is_null())
    }

    pub async fn known_pods(&self) -> Vec<PodState> {
        self.pods.read().await.values().cloned().collect()
    }

    pub async fn cache_specs(&self, desired: &[Value]) {
        let mut pods = self.pods.write().await;
        for object in desired {
            if let Some(state) = object["metadata"]["uid"].as_str().and_then(|uid| pods.get_mut(uid)) {
                state.pod = object.clone();
            }
        }
    }

    pub fn with_host_root(mut self, root: impl Into<std::path::PathBuf>) -> Self {
        self.host_root = root.into();
        self
    }

    /// Read PID 1's asset table from here rather than the host's `/run`: for tests.
    pub fn with_assets_json(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.assets_json = path.into();
        self
    }

    /// The node service behind a mirror pod (#72, #124):
    /// `kube-system/<asset>-<node>`, container `<asset>`, and either a boot
    /// unit that mounts a volume at the asset's `/var/log/stormd` or an entry
    /// in PID 1's asset table. `None` for anything else.
    pub fn node_service(&self, namespace: &str, pod: &str, container: &str) -> Option<NodeService> {
        if namespace != "kube-system" {
            return None;
        }
        let asset = pod.strip_suffix(&format!("-{}", self.node_name))?;
        if asset.is_empty() || asset != container {
            return None;
        }
        let log_dir = crate::node_logs::log_dir(&self.host_root, asset);
        let record = std::fs::read_to_string(&self.assets_json)
            .ok()
            .and_then(|t| crate::node_logs::record(&t, asset));
        if log_dir.is_none() && record.is_none() {
            return None;
        }
        Some(NodeService { log_dir, record })
    }

    /// Use this registry of CSI drivers: the one the kubelet's registration
    /// loop fills.
    /// The node's engine, shared with the rest of the kubelet.
    pub fn with_engine(mut self, engine: crate::engine::EngineClient) -> Self {
        self.storage_url = engine.url().to_string();
        self.engine = engine;
        self
    }

    pub fn with_csi(mut self, csi: Arc<crate::csi_plugins::CsiPlugins>) -> Self {
        self.csi = csi;
        self
    }

    /// Cluster CA (PEM) to write into ServiceAccount `ca.crt` for pods.
    pub fn with_ca_pem(mut self, ca: Option<Vec<u8>>) -> Self {
        self.ca_pem = ca;
        self
    }

    /// Fetch a ConfigMap's `data` map (namespaced). None on any failure.
    /// GET any object from the apiserver.
    async fn api_get(&self, path: &str) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let resp = self
            .api_client
            .get(format!("{}{path}", self.api_url))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Emit a pod event, if there is a recorder.
    ///
    /// The recorder was built, stored, and never called: `pod_event` had zero
    /// call sites, so `describe pod` showed `Events: <none>` for exactly the
    /// failures somebody opens it for. A helper rather than `if let` at every
    /// site, because the sites are what was missing and they should be one
    /// line each.
    async fn event(&self, pod: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.pod_event(pod, etype, reason, message).await;
        }
    }

    /// An ordinary lifecycle event, queued rather than awaited (#134): see
    /// [`crate::events::EventRecorder::pod_event_later`].
    async fn event_later(&self, pod: &Value, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.pod_event_later(pod, "Normal", reason, message).await;
        }
    }

    /// POST an object to the apiserver. `None` on any failure, including a
    /// 409 — an object that already exists is not a failure to the callers
    /// here, which are all idempotent.
    async fn api_post(&self, path: &str, body: &Value) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let resp = self
            .api_client
            .post(format!("{}{path}", self.api_url))
            .json(body)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// PUT an object back to the apiserver.
    async fn api_put(&self, path: &str, body: &Value) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let resp = self
            .api_client
            .put(format!("{}{path}", self.api_url))
            .json(body)
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Publish the PersistentVolume behind a provisioned claim, and bind it.
    ///
    /// **Without this a claim works and reads Pending for ever.** The clone
    /// is made, attached and mounted, the pod runs on it and keeps its data
    /// across restarts — and the PVC object is never touched, so `kubectl get
    /// pvc` says Pending and a console renders that as degraded. Somebody
    /// looking at the cluster sees broken storage that is in fact working,
    /// which is the worst of both: nothing to fix, and no way to tell.
    ///
    /// Written after the clone exists rather than before, so the object never
    /// claims a volume that was not made.
    ///
    /// Best effort throughout. The pod has its storage either way, and
    /// failing a running workload because a status write did not land would
    /// be the reporting path breaking the thing it reports on.
    async fn bind_claim(&self, namespace: &str, claim: &str, volume: &str, bytes: u64) {
        // **The PV is named after the volume**, which is the contract with the
        // control plane's provisioner (rustkube `stormblock.rs`): both derive
        // `pvc-<ns>-<claim>`. This wrote `pvc-` + that, so every claim got two
        // PVs, one from each side, and the claim was repointed away from the
        // one the binder had matched.
        let pv_name = volume.to_string();

        // What the engine knows of the clone: its filesystem and uuid, and
        // what it was cloned from, for the PV's CSI source (#59). The size is
        // the class's, which is what was provisioned.
        let list = self
            .storage_get("/api/v1/volumes")
            .await
            .unwrap_or_default();
        let items = list["items"].as_array().cloned().unwrap_or_default();
        let names: std::collections::HashMap<String, String> = items
            .iter()
            .filter_map(|v| {
                Some((
                    v["id"].as_str()?.to_string(),
                    v["name"].as_str()?.to_string(),
                ))
            })
            .collect();
        let mut facts = items
            .iter()
            .find(|v| v["name"].as_str() == Some(volume))
            .map(|v| crate::system_claims::VolumeFacts::of(v, &names))
            .unwrap_or_else(|| crate::system_claims::VolumeFacts {
                name: volume.to_string(),
                ..Default::default()
            });
        facts.bytes = bytes;

        // The claim first, so the volume's claimRef can carry its uid: a PV
        // naming a claim by name alone reads as bound to whichever claim of
        // that name exists, including one made again after a delete.
        let path = format!("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{claim}");
        let Some(mut pvc) = self.api_get(&path).await else {
            return;
        };
        facts.block = crate::storage::is_block(&pvc);

        let mut pv = crate::system_claims::stormblock_pv(
            &facts,
            &pv_name,
            &self.node_name,
            crate::system_claims::claim_ref(&pvc),
            "Delete",
        );
        pv["status"] = serde_json::json!({ "phase": "Bound" });
        if self
            .api_post("/api/v1/persistentvolumes", &pv)
            .await
            .is_none()
        {
            // **It exists: bring it up to what was provisioned** (#64). The
            // control plane's provisioner (rustkube `stormblock.rs`) writes
            // the PV once the scheduler picks a node, before the pod starts
            // here, with the claim's *request* as its capacity: it leaves the
            // class to the node. This did nothing when the PV was there, so a
            // 3.5Gi claim on a 4 GiB volume said 3.5Gi for good.
            let pv_path = format!("/api/v1/persistentvolumes/{pv_name}");
            if let Some(existing) = self.api_get(&pv_path).await {
                if let Some(updated) =
                    crate::system_claims::reconcile_pv(&existing, &pv, Some(&pvc))
                {
                    if self.api_put(&pv_path, &updated).await.is_none() {
                        warn!("PV {pv_name}: could not record its capacity and source");
                    }
                }
            }
        }

        // Bind the claim to it. Read-modify-write rather than a patch,
        // because the claim carries a resourceVersion and losing a concurrent
        // edit here would be a claim pointing at the wrong volume.
        let before = pvc.clone();
        crate::system_claims::bind_pvc(&mut pvc, &pv_name, &self.node_name, volume);
        let capacity = serde_json::json!({ "storage": crate::system_claims::quantity(bytes) });
        if pvc["status"]["phase"].as_str() != Some("Bound") {
            pvc["status"] = serde_json::json!({
                "phase": "Bound",
                "accessModes": ["ReadWriteOnce"],
                "capacity": capacity,
            });
        } else {
            // Bound by the binder to the request-sized PV: the capacity is
            // the class's, which is what `df` in the pod shows.
            pvc["status"]["capacity"] = capacity;
        }
        if pvc == before {
            return;
        }
        if self.api_put(&path, &pvc).await.is_some() {
            info!("PVC {namespace}/{claim} bound to {pv_name}");
        }
    }

    /// POST to stormblock's management API on this node.
    ///
    /// Loopback by default: the storage engine runs on the node whose volumes
    /// it serves, and a kubelet asking another node's stormblock for a local
    /// device would get an answer that is true somewhere else.
    ///
    /// `Err` is stormblock's own answer — its status and body — or why there
    /// was none, never a bare "would not" (#140): a claim that waits on a
    /// refusal has to say which one, because the engine's API needs the
    /// node's token and nobody debugging the claim from outside has it.
    async fn storage_post(&self, path: &str, body: &Value) -> Result<Value, EngineRefusal> {
        let resp = self
            .engine
            .post(&format!("{}{path}", self.storage_url), body)
            .await
            .map_err(|e| EngineRefusal::unanswered(path, &e))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| EngineRefusal::unanswered(path, &e))?;
        if !status.is_success() {
            let r = EngineRefusal::refused(path, status.as_u16(), &text);
            warn!("{r}");
            return Err(r);
        }
        serde_json::from_str(&text).map_err(|e| EngineRefusal {
            status: Some(status.as_u16()),
            body: text.clone(),
            message: format!(
                "stormblock POST {path} -> {status} but not JSON ({e}): {}",
                excerpt(&text)
            ),
        })
    }

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
            return Some(
                pod["metadata"]["labels"][key]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            );
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
            "metadata.namespace" => pod["metadata"]["namespace"]
                .as_str()
                .unwrap_or("default")
                .to_string(),
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
    ///
    /// `Err` only for a claim this node must not provision: everything else
    /// degrades to scratch, loudly. See [`ClaimError`].
    async fn resolve_volumes(
        &self,
        pod: &Value,
    ) -> Result<HashMap<String, ResolvedVolume>, CriError> {
        self.resolve_volumes_timed(pod, &mut Vec::new()).await
    }

    /// [`resolve_volumes`](Self::resolve_volumes), with how long each volume
    /// took (#132).
    async fn resolve_volumes_timed(
        &self,
        pod: &Value,
        times: &mut Vec<(String, std::time::Duration)>,
    ) -> Result<HashMap<String, ResolvedVolume>, CriError> {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let mut map = HashMap::new();
        let mut current: Option<(String, Instant)> = None;
        let volumes = match pod["spec"]["volumes"].as_array() {
            Some(v) => v,
            None => return Ok(map),
        };
        for vol in volumes {
            let name = match vol["name"].as_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            // Each volume's time ends where the next begins (its arms
            // `continue` from many places).
            if let Some((n, t)) = current.take() {
                times.push((n, t.elapsed()));
            }
            current = Some((name.clone(), Instant::now()));
            let host_path = if let Some(hp) = vol["hostPath"]["path"].as_str() {
                ensure_host_path(hp, vol["hostPath"]["type"].as_str().unwrap_or(""));
                hp.to_string()
            } else if !vol["configMap"].is_null() {
                let cm = vol["configMap"]["name"].as_str().unwrap_or("");
                let dir = pod_volume_dir(&self.state_root, uid, "configmap", &name);
                let data = self
                    .fetch_configmap(namespace, cm)
                    .await
                    .and_then(|o| o["data"].as_object().cloned());
                materialize_files(&dir, data.as_ref(), false)
                    .map_err(|e| pod_dir_error(&name, &dir, e))?;
                dir
            } else if !vol["secret"].is_null() {
                let sec = vol["secret"]["secretName"].as_str().unwrap_or("");
                let dir = pod_volume_dir(&self.state_root, uid, "secret", &name);
                let decoded = self.fetch_secret_decoded(namespace, sec).await;
                materialize_secret_files(&dir, decoded.as_ref())
                    .map_err(|e| pod_dir_error(&name, &dir, e))?;
                dir
            } else if let Some(sources) = vol["projected"]["sources"].as_array() {
                // Projected volume (e.g. kube-api-access: SA token + CA + downward API).
                let dir = pod_volume_dir(&self.state_root, uid, "projected", &name);
                std::fs::create_dir_all(&dir).map_err(|e| pod_dir_error(&name, &dir, e))?;
                self.materialize_projected(pod, namespace, &dir, sources)
                    .await
                    .map_err(|e| pod_dir_error(&name, &dir, e))?;
                // Ensure the SA volume has a usable ca.crt even if the cluster
                // has no kube-root-ca.crt configMap to source it from.
                if let Some(ca) = &self.ca_pem {
                    let has_sat = sources.iter().any(|s| !s["serviceAccountToken"].is_null());
                    if has_sat {
                        std::fs::write(format!("{dir}/ca.crt"), ca)
                            .map_err(|e| pod_dir_error(&name, &dir, e))?;
                    }
                }
                dir
            } else if let Some(claim) = claim_of(pod, vol) {
                let claim = claim.as_str();
                // Another driver's volume: a claim bound to a PV whose
                // `csi.driver` is not ours. Its driver mounts it (#52).
                let pvc_path =
                    format!("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{claim}");
                match self.api_get(&pvc_path).await {
                    Some(pvc) => {
                        // A raw device where a filesystem is mounted, or the
                        // other way round, is refused as upstream refuses it:
                        // the pod waits with the reason (#67).
                        if let Some(why) =
                            volume_mode_misuse(pod, &name, crate::storage::is_block(&pvc))
                        {
                            return Err(CriError::VolumeNotReady(format!(
                                "PVC {namespace}/{claim}: {why}"
                            )));
                        }
                        if let Some(pv) = self.external_csi_pv(&pvc).await {
                            let readonly = vol["persistentVolumeClaim"]["readOnly"]
                                .as_bool()
                                .unwrap_or(false);
                            match self.mount_csi_claim(pod, &name, &pvc, &pv, readonly).await {
                                Ok(dir) => {
                                    map.insert(
                                        name,
                                        ResolvedVolume {
                                            path: dir,
                                            fstype: None,
                                            block: false,
                                        },
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    return Err(CriError::VolumeNotReady(format!(
                                        "PVC {namespace}/{claim}: {e}"
                                    )))
                                }
                            }
                        }
                    }
                    // A generic ephemeral volume's claim is made for the pod
                    // by a controller, and until it exists there is nothing
                    // to mount. Say which claim, and why it may never appear.
                    None if vol.get("ephemeral").is_some() => {
                        return Err(CriError::VolumeNotReady(format!(
                            "generic ephemeral volume {name}: claim {namespace}/{claim} does not \
                             exist yet (it is created from the pod's volumeClaimTemplate by the \
                             ephemeral-volume controller)"
                        )));
                    }
                    None => {}
                }
                // A real volume on stormblock, not a directory.
                //
                // The claim is cloned from a blank filesystem that was made
                // once at image-build time, attached as a block device on this
                // node, and mounted *by the container* — see storage.rs. A
                // failure here falls back to a scratch directory rather than
                // failing the pod, and says so: a pod that starts with its data
                // in the wrong place is bad, but so is a pod that will not start
                // on a node whose storage is briefly unreachable. The fallback
                // is loud, and `kubectl describe` shows the reason.
                match self.provision_claim(namespace, claim, uid).await {
                    Ok((device, fstype)) => {
                        map.insert(
                            name,
                            ResolvedVolume {
                                path: device,
                                block: fstype.is_none(),
                                fstype: fstype.map(String::from),
                            },
                        );
                        continue;
                    }
                    // Someone else's class. Permanent and deterministic, so
                    // the scratch fallback is exactly wrong here: its
                    // rationale is about a node whose storage is *briefly*
                    // unreachable, and silently giving a pod scratch storage
                    // forever because its claim belongs to another driver is
                    // the bad half of that trade with none of the good half.
                    // The pod waits instead, with the reason in `describe`.
                    Err(ClaimError::NotOurs(why)) => {
                        return Err(CriError::VolumeNotReady(why));
                    }
                    // ReadWriteOncePod, already held here. Also permanent as
                    // far as this pod is concerned until the holder goes
                    // away, and the scratch fallback would be worse than
                    // wrong: a pod that asked for an exclusive volume and
                    // silently got an empty directory has been told its
                    // guarantee held when it did not.
                    Err(ClaimError::InUse(why)) => {
                        return Err(CriError::VolumeNotReady(why));
                    }
                    // **Never scratch for a claim.** This fell back to a
                    // per-pod directory and started the pod — a database on a
                    // claim ran happily and lost everything at its next
                    // restart, with a warning in a log nobody reads as the
                    // only trace. A claim that cannot be provisioned yet is a
                    // pod that waits, retried on every sync, with the reason
                    // in `describe`.
                    Err(ClaimError::Failed(e)) => {
                        return Err(CriError::VolumeNotReady(format!(
                            "PVC {namespace}/{claim}: {e}"
                        )));
                    }
                }
            } else if vol.get("csi").is_some() {
                // An inline CSI volume, which lives and dies with the pod.
                match self.mount_csi_inline(pod, vol).await {
                    Ok(dir) => {
                        map.insert(
                            name,
                            ResolvedVolume {
                                path: dir,
                                fstype: None,
                                block: false,
                            },
                        );
                        continue;
                    }
                    Err(e) => return Err(CriError::VolumeNotReady(format!("volume {name}: {e}"))),
                }
            } else if vol.get("emptyDir").is_some() {
                // What emptyDir means: per-pod scratch.
                let dir = pod_volume_dir(&self.state_root, uid, "empty-dir", &name);
                std::fs::create_dir_all(&dir).map_err(|e| pod_dir_error(&name, &dir, e))?;
                dir
            } else {
                // Any other volume type (in-tree nfs, iscsi, and the rest)
                // is one this node cannot provide yet. A CSI driver for it can. Turning it into an empty
                // directory gave the pod a volume that looked right and held
                // nothing; waiting says what is missing.
                let kind = vol
                    .as_object()
                    .and_then(|o| o.keys().find(|k| k.as_str() != "name").cloned())
                    .unwrap_or_else(|| "unknown".into());
                return Err(CriError::VolumeNotReady(format!(
                    "volume {name} is of type {kind}, which this node does not provide; \
                     use a CSI driver for it (docs/csi.md)"
                )));
            };
            // **Check it is there, and say which one is not.**
            //
            // The engine mounts by path and reports ENOENT with no path, so a
            // single missing source presents as
            // `Spawn: No such file or directory (attaching mounts)` for a pod
            // with thirteen volumes — and finding which took reading the spec
            // and reasoning about the node. The kubelet knows every path it is
            // about to hand over; checking here costs a stat and turns
            // deduction into a sentence.
            // Checked where the *host* sees it, not where this process does.
            //
            // The kubelet runs in a container, so `/lib/modules` here is its
            // own golden's — which has none — while the engine mounts the
            // node's, where it is a symlink into the modules volume and
            // resolves perfectly. So this reported FailedMount on every boot
            // for a container that then ran fine, on a node that was 19/19
            // healthy. `on_host` is the mapping the rest of this file already
            // uses for exactly this reason; the check simply did not.
            let checked = on_host(&host_path);
            let p = checked.as_path();
            if !p.exists() {
                // A dangling symlink is not a missing path, and saying so
                // matters.
                //
                // `exists()` follows symlinks, so a link whose target has not
                // been mounted *yet* reads as absent. On this node /lib/modules
                // is exactly that — a symlink into the modules volume — and
                // every boot produced
                //
                // ```text
                // volume lib-modules: /lib/modules does not exist on this node
                //   — the container will fail to start with ENOENT
                // ```
                //
                // for a container that then started perfectly, because the
                // volume was mounted by the time it did. A warning that
                // predicts a failure which does not happen is worse than no
                // warning: it is read once, disbelieved, and then the real
                // ones are disbelieved too.
                match std::fs::symlink_metadata(p) {
                    Ok(_) => {
                        let target = std::fs::read_link(p)
                            .map(|t| t.display().to_string())
                            .unwrap_or_else(|_| "?".into());
                        warn!(
                            "volume {name}: {host_path} is a symlink to {target}, which is not \
                             there yet — if that is a volume still being mounted this resolves \
                             itself; if not, the container will fail with ENOENT"
                        );
                    }
                    Err(_) => {
                        warn!(
                            "volume {name}: {host_path} does not exist on this node — the \
                             container will fail to start with ENOENT attaching mounts"
                        );
                        // FailedMount, which is where somebody looks first.
                        //
                        // Not emitted for the dangling-symlink case above: a
                        // volume that is about to be mounted is not a failure,
                        // and an event saying it is would be read as one long
                        // after it resolved.
                        self.event(
                            pod,
                            "Warning",
                            "FailedMount",
                            &crate::events::failed_mount_message(&name, &host_path, ""),
                        )
                        .await;
                    }
                }
            }
            map.insert(
                name,
                ResolvedVolume {
                    path: host_path,
                    fstype: None,
                    block: false,
                },
            );
        }
        if let Some((n, t)) = current.take() {
            times.push((n, t.elapsed()));
        }
        Ok(map)
    }

    /// The name of another pod on this node that already has `claim` mounted,
    /// if there is one.
    ///
    /// "Already has it mounted" is read from the pods this manager is running:
    /// a pod is a holder while it is non-terminal, because a Succeeded or
    /// Failed pod's containers are gone and nothing of it is writing. The
    /// pod's own UID is excluded so a restart, which re-resolves its volumes,
    /// does not find itself and refuse to come back up.
    async fn other_pod_holding(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
    ) -> Option<String> {
        let pods = self.pods.read().await;
        pods.values()
            .find(|st| {
                st.uid != pod_uid
                    && st.namespace == namespace
                    && st.phase != "Succeeded"
                    && st.phase != "Failed"
                    && pod_claims(&st.pod).any(|c| c == claim)
            })
            .map(|st| st.name.clone())
    }

    /// Honour `reclaimPolicy: Delete` for the volumes this node holds.
    ///
    /// **The node reclaims its own.** A stormblock clone can only be deleted by
    /// the node that holds it — stormblock's API is loopback — so this pass
    /// finds this node's stormblock PVs that the binder has moved to
    /// `Released` with policy `Delete`, releases the clone through
    /// [`Self::release_claim_volume`] (which refuses while a pod here still has
    /// it), and then deletes the PV. The control plane's provisioner used to
    /// leave them `Released` with a warning for ever (rustkube#71).
    ///
    /// Never a system volume: those are Retain, and carry the system label.
    ///
    /// `true` when a reclaim is still pending (the claim is in use here, or a
    /// delete was refused): the caller looks again shortly (#101). An
    /// unreadable PV list is reported to the reactor, which retries.
    pub async fn reclaim_released(&self) -> bool {
        let Some(pvs) = self.api_get("/api/v1/persistentvolumes").await else {
            apimachinery::reactor::failed();
            return false;
        };
        let mut pending = false;
        for pv in pvs["items"].as_array().cloned().unwrap_or_default() {
            if pv["spec"]["csi"]["driver"].as_str() != Some("stormblock.storm.io")
                || pv["status"]["phase"].as_str() != Some("Released")
                || pv["spec"]["persistentVolumeReclaimPolicy"].as_str() != Some("Delete")
                || pv["metadata"]["labels"][crate::system_claims::LABEL].as_str() == Some("true")
            {
                continue;
            }
            let here = pv["metadata"]["annotations"]["storm.io/node"].as_str()
                == Some(&self.node_name)
                || pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(|t| {
                        t["matchExpressions"]
                            .as_array()
                            .cloned()
                            .unwrap_or_default()
                    })
                    .any(|e| {
                        e["values"]
                            .as_array()
                            .is_some_and(|v| v.iter().any(|n| n.as_str() == Some(&self.node_name)))
                    });
            if !here {
                continue;
            }
            let (Some(ns), Some(claim), Some(pv_name)) = (
                pv["spec"]["claimRef"]["namespace"].as_str(),
                pv["spec"]["claimRef"]["name"].as_str(),
                pv["metadata"]["name"].as_str(),
            ) else {
                continue;
            };
            let Some(pv_uid) = pv["metadata"]["uid"].as_str() else {continue};
            match self.release_claim_volume(ns, claim).await {
                Ok(VolumeRelease::Released) | Ok(VolumeRelease::Absent) => {
                    let url = format!("{}/api/v1/persistentvolumes/{pv_name}", self.api_url);
                    match self.api_client.delete(url).json(&serde_json::json!({
                        "apiVersion":"v1", "kind":"DeleteOptions",
                        "preconditions":{"uid":pv_uid,"resourceVersion":pv["metadata"]["resourceVersion"]}
                    })).send().await {
                        Ok(r) if r.status().is_success() || r.status().as_u16() == 404 => {
                            info!("reclaimed {pv_name}: the volume for {ns}/{claim} is deleted")
                        }
                        // A changed PV is a watch event of its own.
                        Ok(r) if r.status().as_u16() == 409 => {}
                        Ok(r) => { pending = true; debug!("PV {pv_name} not deleted: {}", r.status()) }
                        Err(e) => { pending = true; debug!("PV {pv_name} not deleted: {e}") }
                    }
                }
                Ok(other) => { pending = true; debug!("PV {pv_name} not reclaimed yet: {other:?}") }
                Err(e) => { pending = true; debug!("PV {pv_name} not reclaimed: {e}") }
            }
        }
        pending
    }

    /// Delete the stormblock clone behind a claim, so a `Delete` reclaim
    /// policy finishes instead of leaking (rustkube-node#46).
    ///
    /// **Why the node does this and not a controller.** stormblock's
    /// management API is loopback: the engine that holds the volume is on the
    /// node, and only the node can reach it. The alternative — an off-node
    /// path to every node's engine — is a credential that can destroy any
    /// volume in the cluster, reachable from wherever the controller runs.
    /// This route keeps the blast radius at one node and reuses a hop the
    /// apiserver already authenticates.
    ///
    /// The name comes from [`crate::storage::volume_name`], the same function
    /// that created it, so the two cannot disagree about which volume this is.
    pub async fn release_claim_volume(
        &self,
        namespace: &str,
        claim: &str,
    ) -> Result<VolumeRelease, String> {
        // Refuse while a pod here still has it. A delete that races a running
        // pod pulls a filesystem out from under a process mid-write, and the
        // pod finds out as EIO on a device that no longer exists.
        let _reclaim=if let Some(admission)=&self.admission {
            match admission.reclaim(namespace,claim) {
                Some(guard)=>Some(guard),
                None=>return Ok(VolumeRelease::InUse("claim has an active workload or cleanup reservation".into())),
            }
        } else {None};
        if let Some(holder) = self.claim_holder_here(namespace, claim).await? {
            return Ok(VolumeRelease::InUse(holder));
        }

        // Absent is success: a controller retrying a delete it already
        // completed is not an error, and answering 404 would make it one.
        //
        // This asks through `storage_volume_checked`, not the `Option`-shaped
        // lookup the provisioning path uses, and the difference is the whole
        // point: that one answers `None` both for "there is no such volume"
        // and for "stormblock did not answer", and here those are opposite
        // answers. Reporting the second as Absent would tell the controller
        // the clone was deleted when the node could not even ask — which
        // deletes the PV and turns the leak this endpoint exists to stop into
        // the invisible kind.
        let name = crate::storage::volume_name(namespace, claim);
        let Some((vol_id, _sealed)) = self.storage_volume_checked(&name).await? else {
            return Ok(VolumeRelease::Absent);
        };

        // Detach before deleting. The attach outlives the pod that needed it,
        // and stormblock refuses to delete a volume it is still serving — so
        // without this the delete comes back 409 for every volume that was
        // ever mounted, which is all of them.
        self.storage_delete(&format!("/api/v1/volumes/{vol_id}/attach"))
            .await?;
        self.storage_delete(&format!("/api/v1/volumes/{vol_id}"))
            .await?;
        info!("released volume {name} ({vol_id}) for claim {namespace}/{claim}");
        Ok(VolumeRelease::Released)
    }

    /// The name of a pod on this node that still uses `claim`, if there is one.
    ///
    /// Asks twice, because neither source is sufficient alone. Local state
    /// knows about static pods, which the apiserver has never heard of, and
    /// is stale for a pod adopted after a kubelet restart — state recovery
    /// enters those with a null spec and the claim list is only filled in on
    /// the next sync. The apiserver knows the specs but not the static pods.
    ///
    /// `Err` when the apiserver cannot be asked at all, and the caller must
    /// treat that as a refusal: this is a data-destroying operation, and "I
    /// could not check" is not "nothing is using it".
    async fn claim_holder_here(
        &self,
        namespace: &str,
        claim: &str,
    ) -> Result<Option<String>, String> {
        // The uid filter excludes nothing here: every pod is somebody else.
        if let Some(holder) = self.other_pod_holding(namespace, claim, "").await {
            return Ok(Some(holder));
        }
        if self.api_url.is_empty() {
            return Ok(None);
        }
        let pods = self.api_get("/api/v1/pods").await.ok_or_else(|| {
            "cannot confirm the volume is unused: the apiserver pod list is unavailable".to_string()
        })?;
        let holder = pods["items"]
            .as_array()
            .map(|a| a.as_slice())
            .unwrap_or(&[])
            .iter()
            .find(|p| {
                p["spec"]["nodeName"].as_str() == Some(&self.node_name)
                    && p["metadata"]["namespace"].as_str() == Some(namespace)
                    && !matches!(
                        p["status"]["phase"].as_str(),
                        Some("Succeeded") | Some("Failed")
                    )
                    && pod_claims(p).any(|c| c == claim)
            })
            .and_then(|p| p["metadata"]["name"].as_str())
            .map(String::from);
        Ok(holder)
    }

    /// Turn a PersistentVolumeClaim into a block device on this node.
    ///
    /// **Redundancy is not decided here.** stormblock and stormdrive are the
    /// redundancy controller: stormdrive knows the drives and their failure
    /// domains, stormblock places a volume's legs across them. The kubelet asks
    /// for a volume of a size and is told a device; how many copies it has and
    /// which drives they are on is not the kubelet's business and must not
    /// become it — a scheduler or a node agent that starts making placement
    /// decisions is how two components end up disagreeing about where data is.
    ///
    /// The volume name is derived from the namespace and claim, and creation is
    /// name-idempotent, so a restarted pod is reunited with its data rather than
    /// given a fresh volume. That is the whole difference between a claim and a
    /// scratch directory, and it is why the name cannot include the pod UID.
    async fn provision_claim(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
    ) -> Result<(String, Option<&'static str>), ClaimError> {
        self.provision_claim_volume(namespace, claim, pod_uid)
            .await
            .map(|(_, dev, fs)| (dev, fs))
    }

    /// A claim as a block device on this node, for a virtual machine's disk
    /// (#74). The same path a pod's claim takes: a bound claim gets its own
    /// volume, and an unbound claim of the built-in class is provisioned.
    ///
    /// Returns the volume id and the attached device. The VM manager records
    /// the id, to detach it when the machine stops. The claim outlives the
    /// machine, so the id is never the VM's to delete.
    ///
    /// Refused while a pod on this node uses the claim. A pod mounts the
    /// claim's filesystem and a VM writes the raw device, and two writers on
    /// one ext4 is corruption, whatever the access mode says.
    pub(crate) async fn claim_for_vm(
        &self,
        namespace: &str,
        claim: &str,
    ) -> Result<(String, String), String> {
        match self.claim_holder_here(namespace, claim).await {
            Ok(Some(pod)) => {
                return Err(format!(
                    "claim {namespace}/{claim} is in use by pod {namespace}/{pod} on this node"
                ))
            }
            Ok(None) => {}
            Err(e) => return Err(format!("claim {namespace}/{claim}: {e}")),
        }
        self.provision_claim_volume(namespace, claim, "")
            .await
            .map(|(id, dev, _)| (id, dev))
            .map_err(|e| match e {
                ClaimError::NotOurs(why) => format!("waiting for claim {claim} to bind: {why}"),
                ClaimError::InUse(why) => why,
                ClaimError::Failed(why) => format!("claim {namespace}/{claim}: {why}"),
            })
    }

    /// [`Self::provision_claim`], answering the volume id too, and the
    /// filesystem the device carries: `None` for a raw block claim (#67).
    async fn provision_claim_volume(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
    ) -> Result<(String, String, Option<&'static str>), ClaimError> {
        let name = crate::storage::volume_name(namespace, claim);

        // What the claim asked for, rounded up to a class. The class is also
        // the ceiling: a claim gets the blank that holds it and no more.
        let pvc: Value = self
            .api_get(&format!(
                "/api/v1/namespaces/{namespace}/persistentvolumeclaims/{claim}"
            ))
            .await
            .ok_or_else(|| ClaimError::Failed(format!("claim {claim} not found")))?;

        // Whose claim is this? Someone else's class is not ours to clone, and
        // getting that wrong provisions a second volume for a claim that is
        // already bound to somebody's PV (#44).
        if !crate::storage::provisioned_here(&pvc) {
            let class = pvc["spec"]["storageClassName"].as_str().unwrap_or("");
            return Err(ClaimError::NotOurs(if class.is_empty() {
                format!(
                    "claim {namespace}/{claim} sets storageClassName: \"\", which asks to bind \
                     an existing volume rather than provision one"
                )
            } else {
                format!(
                    "claim {namespace}/{claim} belongs to StorageClass {class}, not {}",
                    crate::storage::STORAGE_CLASS
                )
            }));
        }
        // `ReadWriteOncePod` means one *pod*, where `ReadWriteOnce` means one
        // *node* and lets every pod on that node share the volume. The
        // scheduler filter (rustkube#65) is what keeps a second pod Pending
        // with a readable reason, and it is not enough on its own: a static
        // pod, or one written straight onto `spec.nodeName`, never passes a
        // scheduler filter at all. Upstream refuses the mount for exactly
        // that reason, and so does this — a guarantee that holds for
        // scheduled pods and quietly does not for the two ways around the
        // scheduler is worse than not offering the mode (rustkube-node#42).
        if crate::storage::is_rwop(&pvc) {
            if let Some(holder) = self.other_pod_holding(namespace, claim, pod_uid).await {
                return Err(ClaimError::InUse(format!(
                    "claim {namespace}/{claim} is ReadWriteOncePod and is already mounted by \
                     pod {namespace}/{holder} on this node"
                )));
            }
        }

        let want = crate::storage::claim_bytes(&pvc);
        let (class, class_bytes) = crate::storage::class_for(want).ok_or_else(|| {
            ClaimError::Failed(format!(
                "claim asks for {want} bytes, larger than the largest size class"
            ))
        })?;
        // A raw block claim has no filesystem; a filesystem claim has its
        // class's (#67).
        let block = crate::storage::is_block(&pvc);
        let fstype = if block { None } else { crate::storage::class_fs(class).ok() };

        // Already provisioned? A claim is keyed on namespace and name, so a
        // restarted pod is reunited with its data rather than given a fresh
        // volume — which is the whole difference between a claim and a scratch
        // directory.
        // The node's own data containers are listed as claims, and they are
        // mounted by the services that own them. A second mount of the same
        // ext4 from a pod would corrupt it, so they are cloned, never mounted:
        // a claim with `dataSource: {kind: PersistentVolumeClaim, name: <it>}`
        // in this namespace gets a copy-on-write copy.
        if let Some(pv) = pvc["spec"]["volumeName"].as_str().filter(|v| !v.is_empty()) {
            if let Some(pv) = self
                .api_get(&format!("/api/v1/persistentvolumes/{pv}"))
                .await
            {
                if pv["metadata"]["labels"][crate::system_claims::LABEL].as_str() == Some("true") {
                    return Err(ClaimError::InUse(format!(
                        "{namespace}/{claim} is a node service's live data volume; mount a clone \
                         of it (a claim with dataSource naming it) rather than the volume itself"
                    )));
                }
            }
        }

        // A claim already bound to a stormblock PV mounts *that* volume. The
        // node's own data containers are claims like this (system_claims.rs),
        // and so is any volume an administrator publishes by hand: deriving a
        // name here instead would clone a new, empty volume beside the real one.
        let bound = self.bound_volume(&pvc).await;
        let existing = match bound {
            Some(id) => Some(id),
            None => self.storage_volume_id(&name).await,
        };
        let vol_id = match existing {
            Some(id) => id,
            // **A claim with a source is a clone of it.** Cloning data volumes
            // is what a claim *is* on this platform; a blank is only the case
            // where the source is an empty filesystem.
            None if crate::storage::claim_source(&pvc).is_some() => {
                self.clone_claim_source(namespace, &pvc, &name).await?
            }
            // **A raw block claim is a plain volume** (#67): nothing to
            // format and nothing to clone, so every class is offered, up to
            // the pebibyte classes no filesystem blank exists for yet.
            None if block => self.raw_volume(&name, class, class_bytes).await?,
            None => {
                // A class with no filesystem blank yet: the claim waits, and
                // says what would serve it now.
                if let Err(why) = crate::storage::class_fs(class) {
                    return Err(ClaimError::Failed(format!(
                        "claim {namespace}/{claim} rounds up to the {class} class, which has no \
                         filesystem yet: {why}"
                    )));
                }
                // Clone the blank for this class. `clone` is the one door:
                // it descends from a sealed volume, records lineage, and
                // stamps the clone with its own filesystem UUID — two live
                // filesystems must never claim one identity (stormblock#76).
                self.clone_blank(class, &name).await?
            }
        };

        // Attach it here, as a block device the container will mount.
        //
        // On the volume, not on /v1 (stormblock#78, v12.1.0): a volume is the
        // thing that has a device, and this one was cloned through /api/v1 so
        // it has no /v1 record. `ublk` explicitly rather than by preference —
        // asking for it where it cannot be offered is a 409 rather than a
        // silent downgrade to nvme-tcp, and a downgrade is not something to
        // discover from a mount that behaves oddly later.
        let attach = serde_json::json!({ "node": self.node_name, "transport": "ublk" });
        let info: Value = self
            .storage_post(&format!("/api/v1/volumes/{vol_id}/attach"), &attach)
            .await
            .map_err(|r| {
                ClaimError::Failed(format!(
                    "stormblock would not attach {name} as a local device: {r}"
                ))
            })?;
        if let Some(dev) = info["device_hint"].as_str() {
            info!("PVC {namespace}/{claim} -> {name} ({class}) at {dev}");
            // Say so on the claim, now that there is something to point at.
            //
            // Here rather than earlier so the object never names a volume
            // that was not made, and after the attach so a bound claim means
            // storage a pod can actually use.
            self.bind_claim(namespace, claim, &name, class_bytes).await;
            return Ok((vol_id, dev.to_string(), fstype));
        }
        Err(ClaimError::Failed(format!(
            "volume {name} did not attach locally: {info} — an NVMe-oF attach needs a \
             connect this node does not do yet"
        )))
    }

    /// A raw block claim's volume (#67): a plain stormblock volume of the
    /// class's size, in the data half like every claim, with no filesystem.
    /// Thin, so a pebibyte class costs nothing until it is written.
    async fn raw_volume(&self, name: &str, class: &str, bytes: u64) -> Result<String, ClaimError> {
        let body = serde_json::json!({
            "name": name,
            "size": crate::storage::engine_size(bytes),
            "role": "data",
        });
        let created = self
            .storage_post("/api/v1/volumes", &body)
            .await
            .map_err(|r| {
                ClaimError::Failed(format!(
                    "stormblock would not create the raw volume {name} ({class}): {r}"
                ))
            })?;
        created["id"]
            .as_str()
            .or_else(|| created["volume_id"].as_str())
            .map(String::from)
            .ok_or_else(|| {
                ClaimError::Failed(format!("stormblock created {name} but named no volume: {created}"))
            })
    }

    /// Clone the blank of a size class into the claim's volume `name`,
    /// minting the blank first when this node has none.
    ///
    /// A clone stormblock refuses because the template itself is broken
    /// ([`EngineRefusal::template_broken`]) deletes the template and mints it
    /// again: the refusal is the same on every retry, so a claim that only
    /// retried the clone waited forever (#140). Any other refusal is the
    /// claim's wait, with stormblock's answer in it.
    async fn clone_blank(&self, class: &str, name: &str) -> Result<String, ClaimError> {
        let blank = crate::storage::template_name(class);
        let template = match self.storage_template_state(&blank).await {
            // A blank not sealed yet cannot be cloned. Formatting a
            // 1 TiB class takes minutes (stormblock#141), and the claim
            // waits with the state named rather than failing a clone.
            Some((id, state)) if state == "ready" => id,
            Some((_, state)) => {
                return Err(ClaimError::Failed(format!(
                    "waiting for volume {name}: template {blank} {state}"
                )))
            }
            // Mint it, rather than refusing the claim (#45).
            //
            // The alternative — which this replaces — capped the class
            // ladder at whatever the image happened to carry: a claim
            // above the largest shipped blank was refused outright,
            // and adding a class meant rebuilding an image. Baking
            // them in also decides at build time a question only run
            // time can answer, which is which sizes are actually
            // claimed, and spends image space on classes a node may
            // never use.
            //
            // One `mkfs` ever, per class, per node: the first claim of
            // a class pays for it and every claim after is a clone.
            //
            // In the background (#63): the POST answers when the
            // format is done, and a 1 TiB class held the whole sync
            // loop for it. Every pod on the node stopped being
            // reconciled, and the waiting pod was nowhere.
            None => self.mint_template(&blank, class).await.map_err(|e| {
                ClaimError::Failed(format!("waiting for volume {name}: {e}"))
            })?,
        };

        // Through the template, not the volume: `fstemplates/{id}/clone`
        // gives the clone its own filesystem UUID and verifies it, so
        // two claims never present one identity (stormblock#76).
        let body = serde_json::json!({ "name": name, "verify": true });
        let created = match self
            .storage_post(&format!("/api/v1/fstemplates/{template}/clone"), &body)
            .await
        {
            Ok(v) => v,
            Err(r) if r.template_broken() => {
                warn!("template {blank} is broken ({r}); deleting it to mint it again");
                let path = format!("/api/v1/fstemplates/{template}");
                let again = match self.storage_delete(&path).await {
                    Ok(()) => match self.mint_template(&blank, class).await {
                        Ok(_) => "deleted; minted again, cloning on the next try".to_string(),
                        Err(e) => format!("deleted; {e}"),
                    },
                    Err(e) => format!("could not delete it: {e}"),
                };
                return Err(ClaimError::Failed(format!(
                    "waiting for volume {name}: template {blank} is broken ({r}); {again}"
                )));
            }
            Err(r) => {
                return Err(ClaimError::Failed(format!(
                    "stormblock would not clone {blank} to {name}: {r}"
                )))
            }
        };
        created["volume_id"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| {
                ClaimError::Failed(format!("clone of {blank} returned no volume: {created}"))
            })
    }

    /// A volume's id by name, or `None` when this node has no such volume.
    async fn storage_volume_id(&self, name: &str) -> Option<String> {
        self.storage_volume(name).await.map(|(id, _)| id)
    }

    /// A volume's id and whether it is sealed.
    async fn storage_volume(&self, name: &str) -> Option<(String, bool)> {
        let list: Value = self.storage_get("/api/v1/volumes").await?;
        let v = list["items"]
            .as_array()?
            .iter()
            .find(|v| v["name"].as_str() == Some(name))?;
        Some((
            v["id"].as_str()?.to_string(),
            v["sealed"].as_bool().unwrap_or(false),
        ))
    }

    /// A volume's id and seal state by name, telling "no such volume" apart
    /// from "stormblock did not answer".
    ///
    /// [`Self::storage_volume`] collapses the two into `None`, which is right
    /// for provisioning — either way there is nothing to clone from and the
    /// next step handles it — and wrong for anything that destroys data.
    async fn storage_volume_checked(&self, name: &str) -> Result<Option<(String, bool)>, String> {
        let resp = self
            .engine
            .get(&format!("{}/api/v1/volumes", self.storage_url))
            .await
            .map_err(|e| format!("stormblock is not answering on this node: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("stormblock volume list -> {}", resp.status()));
        }
        let list: Value = resp
            .json()
            .await
            .map_err(|e| format!("stormblock volume list is not JSON: {e}"))?;
        let items = list["items"]
            .as_array()
            .ok_or_else(|| "stormblock volume list has no items".to_string())?;
        Ok(items
            .iter()
            .find(|v| v["name"].as_str() == Some(name))
            .and_then(|v| {
                Some((
                    v["id"].as_str()?.to_string(),
                    v["sealed"].as_bool().unwrap_or(false),
                ))
            }))
    }

    /// Mint the blank for a size class ([`mint_blank`]) in the background,
    /// one at a time per blank, and answer its id once it is `ready`.
    ///
    /// `Err` is what the claim waits on: the mint in flight, the template's
    /// state while it formats, or the engine's refusal (once; the next claim
    /// tries again). Completion wakes waiting workers; reconciliation never
    /// waits inline for formatting to finish.
    async fn mint_template(&self, blank: &str, class: &str) -> Result<String, String> {
        let started = {
            let mut m = self.minting.lock().unwrap_or_else(|e| e.into_inner());
            match m.get(blank).cloned() {
                Some(Mint::InFlight) => None,
                Some(Mint::Failed(e)) => {
                    m.remove(blank);
                    return Err(e);
                }
                None => {
                    m.insert(blank.to_string(), Mint::InFlight);
                    Some(())
                }
            }
        };
        if started.is_some() {
            info!("no blank {blank} on this node — minting it (one mkfs, ever, for class {class})");
            let engine = self.engine.clone();
            let url = format!("{}/api/v1/fstemplates", self.storage_url);
            let minting = self.minting.clone();
            let changed = self.volume_changes.clone();
            let (blank, class) = (blank.to_string(), class.to_string());
            tokio::spawn(async move {
                let failed = mint_blank(&engine, &url, &blank, &class).await.err();
                let mut m = minting.lock().unwrap_or_else(|e| e.into_inner());
                match failed {
                    Some(e) => {
                        warn!("{e}");
                        m.insert(blank, Mint::Failed(e));
                    }
                    None => {
                        m.remove(&blank);
                    }
                }
                drop(m);
                changed.send_modify(|generation| *generation = generation.wrapping_add(1));
            });
        }
        match self.storage_template_state(blank).await {
            Some((id, state)) if state == "ready" => Ok(id),
            Some((_, state)) => Err(format!("template {blank} {state}")),
            None => match self
                .minting
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(blank)
            {
                Some(Mint::Failed(e)) => Err(e.clone()),
                _ => Err(format!(
                    "minting template {blank} (one mkfs for class {class})"
                )),
            },
        }
    }

    /// Clone what a claim's `dataSource` names into the claim's own volume.
    ///
    /// - `PersistentVolumeClaim` (same namespace): the volume behind that
    ///   claim's bound PV, which is how the system's own data containers in
    ///   `kube-system` are cloned too. A claim not bound yet falls back to the
    ///   name this node would have given it.
    /// - `Golden` in `storm.io`: a golden by name, `<name>.golden` first.
    ///
    /// Through `volumes/snapshots`, which takes a live source — a claim that is
    /// mounted and being written is not sealed, and the clone route refuses
    /// anything that is not. The copy is copy-on-write and gets its own
    /// filesystem UUID.
    async fn clone_claim_source(
        &self,
        namespace: &str,
        pvc: &Value,
        name: &str,
    ) -> Result<String, ClaimError> {
        let source = crate::storage::claim_source(pvc).expect("checked by the caller");
        let candidates: Vec<String> = match &source {
            crate::storage::ClaimSource::Claim(src_ns, src) => {
                let src_ns = src_ns.as_deref().unwrap_or(namespace);
                let mut c = Vec::new();
                let path = format!("/api/v1/namespaces/{src_ns}/persistentvolumeclaims/{src}");
                if let Some(src_pvc) = self.api_get(&path).await {
                    if let Some(pv) = src_pvc["spec"]["volumeName"]
                        .as_str()
                        .filter(|v| !v.is_empty())
                    {
                        if let Some(pv) = self
                            .api_get(&format!("/api/v1/persistentvolumes/{pv}"))
                            .await
                        {
                            if let Some(h) = pv["spec"]["csi"]["volumeHandle"].as_str() {
                                c.push(h.to_string());
                            }
                        }
                    }
                }
                c.push(crate::storage::volume_name(src_ns, src));
                c
            }
            crate::storage::ClaimSource::Golden(g) => vec![format!("{g}.golden"), g.clone()],
        };
        let mut src_id = None;
        for cand in &candidates {
            if let Some(id) = self.storage_volume_id(cand).await {
                src_id = Some((cand.clone(), id));
                break;
            }
        }
        let Some((src_name, src_id)) = src_id else {
            return Err(ClaimError::Failed(format!(
                "the claim's source {source:?} has no volume on this node (looked for {})",
                candidates.join(", ")
            )));
        };
        // **Flush first.** The snapshot is block-level, and a claim being
        // written has its newest data in the page cache of whichever mount
        // holds it — here, or a service's mount in PID 1's namespace. Tested:
        // a file written a moment before the clone was missing from it, and
        // present in a clone taken after writeback. `sync(2)` writes back every
        // filesystem's dirty data and journal, whichever namespace mounted it,
        // so the clone holds what the writer had written. (Consistency a
        // running application needs — a freeze — is the snapshot work,
        // stormvm#28.)
        let _ = tokio::task::spawn_blocking(|| {
            // SAFETY: sync(2) takes no arguments and cannot fail.
            unsafe { libc::sync() }
        })
        .await;
        let body = serde_json::json!({ "name": name, "source_volume_id": src_id });
        let made: Value = self
            .storage_post("/api/v1/volumes/snapshots", &body)
            .await
            .map_err(|r| {
                ClaimError::Failed(format!("stormblock would not clone {src_name} to {name}: {r}"))
            })?;
        info!("claim {namespace}/{name}: cloned from {src_name}");
        made["id"].as_str().map(String::from).ok_or_else(|| {
            ClaimError::Failed(format!("clone of {src_name} returned no id: {made}"))
        })
    }

    /// The stormblock volume behind a claim's bound PV, when it is ours.
    async fn bound_volume(&self, pvc: &Value) -> Option<String> {
        let pv_name = pvc["spec"]["volumeName"]
            .as_str()
            .filter(|v| !v.is_empty())?;
        let pv = self
            .api_get(&format!("/api/v1/persistentvolumes/{pv_name}"))
            .await?;
        if pv["spec"]["csi"]["driver"].as_str() != Some("stormblock.storm.io") {
            return None;
        }
        let handle = pv["spec"]["csi"]["volumeHandle"].as_str()?;
        self.storage_volume_id(handle).await
    }

    /// A filesystem template's id and state (`ready`, `awaiting_format`,
    /// `awaiting_seed`), by name.
    /// An engine that reports no state is taken as ready, which is what every
    /// template it answered for was before states existed.
    async fn storage_template_state(&self, name: &str) -> Option<(String, String)> {
        let t: Value = self
            .storage_get(&format!("/api/v1/fstemplates/{name}"))
            .await?;
        let id = t["id"].as_str()?.to_string();
        Some((id, t["state"].as_str().unwrap_or("ready").to_string()))
    }

    /// DELETE on stormblock's management API on this node.
    ///
    /// Unlike the GET and POST helpers this reports *why* it failed rather
    /// than answering `None`. It is only used on the release path, where the
    /// reason travels back to the control plane and is the whole content of
    /// the answer: "it is still served by ublk" and "there is no such volume"
    /// are different facts and a controller does different things with them.
    async fn storage_delete(&self, path: &str) -> Result<(), String> {
        let resp = self
            .engine
            .delete(&format!("{}{path}", self.storage_url))
            .await
            .map_err(|e| format!("stormblock is not answering on this node: {e}"))?;
        let status = resp.status();
        if status.is_success() || status.as_u16() == 404 {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(format!(
            "stormblock DELETE {path} -> {status}: {}",
            body.chars().take(200).collect::<String>()
        ))
    }

    /// GET from stormblock's management API on this node.
    async fn storage_get(&self, path: &str) -> Option<Value> {
        let resp = self
            .engine
            .get(&format!("{}{path}", self.storage_url))
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Materialize a projected volume's sources into `dir`: serviceAccountToken
    /// (via TokenRequest, best-effort), configMap, secret, and downwardAPI.
    async fn materialize_projected(
        &self,
        pod: &Value,
        namespace: &str,
        dir: &str,
        sources: &[Value],
    ) -> std::io::Result<()> {
        for src in sources {
            if let Some(sat) = src.get("serviceAccountToken").filter(|v| !v.is_null()) {
                let path = sat["path"].as_str().unwrap_or("token");
                let sa = pod["spec"]["serviceAccountName"]
                    .as_str()
                    .unwrap_or("default");
                let aud = sat["audience"].as_str();
                if let Some(token) = self.request_sa_token(namespace, sa, aud).await {
                    std::fs::write(format!("{dir}/{path}"), token)?;
                }
            } else if let Some(cm) = src.get("configMap").filter(|v| !v.is_null()) {
                let name = cm["name"].as_str().unwrap_or("");
                let data = self
                    .fetch_configmap(namespace, name)
                    .await
                    .and_then(|o| o["data"].as_object().cloned());
                write_projected_items(dir, cm["items"].as_array(), data.as_ref())?;
            } else if let Some(sec) = src.get("secret").filter(|v| !v.is_null()) {
                let name = sec["name"].as_str().unwrap_or("");
                let decoded = self.fetch_secret_decoded(namespace, name).await;
                if let Some(data) = decoded {
                    let asmap: serde_json::Map<String, Value> = data
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect();
                    write_projected_items(dir, sec["items"].as_array(), Some(&asmap))?;
                }
            } else if let Some(dw) = src.get("downwardAPI").filter(|v| !v.is_null()) {
                if let Some(items) = dw["items"].as_array() {
                    for it in items {
                        let path = it["path"].as_str().unwrap_or("");
                        if let Some(fp) = it["fieldRef"]["fieldPath"].as_str() {
                            if let Some(val) = self.downward_field(pod, fp, None) {
                                std::fs::write(format!("{dir}/{path}"), val)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
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
    /// The pod's `/etc/resolv.conf`.
    ///
    /// **No pod had one at all** — not empty, absent — so every lookup inside
    /// every container fell through to 127.0.0.1 and reported "no servers
    /// could be reached", while cluster DNS was up and reachable the whole
    /// time. Nothing ever told a pod where the resolver was.
    ///
    /// Written under the pod's own directory and bind-mounted, the same shape
    /// as the ServiceAccount token: one path, visible to the kubelet that
    /// writes it and to the engine that mounts it.
    ///
    /// `Err` when the file cannot be written (a full node, #129): starting the
    /// pod with the image's resolver instead would be a silent wrong answer.
    fn resolv_conf_mount(&self, pod: &Value) -> Result<Option<Mount>, CriError> {
        // A pod that mounts its own resolv.conf means it.
        if pod["spec"]["containers"].as_array().is_some_and(|cs| {
            cs.iter().any(|c| {
                c["volumeMounts"].as_array().is_some_and(|ms| {
                    ms.iter()
                        .any(|m| m["mountPath"].as_str() == Some("/etc/resolv.conf"))
                })
            })
        }) {
            return Ok(None);
        }

        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let host_network = pod["spec"]["hostNetwork"].as_bool().unwrap_or(false);
        let policy = crate::dns::DnsPolicy::parse(pod["spec"]["dnsPolicy"].as_str(), host_network);
        let node_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default();
        let content = crate::dns::resolv_conf(
            policy,
            namespace,
            &self.cluster_dns,
            &self.cluster_domain,
            pod["spec"].get("dnsConfig"),
            &node_resolv,
        );
        if content.is_empty() {
            return Ok(None);
        }

        let dir = format!("{}/pods/{uid}/etc", self.state_root);
        let path = format!("{dir}/resolv.conf");
        std::fs::create_dir_all(&dir)
            .and_then(|()| std::fs::write(&path, &content))
            .map_err(|e| pod_dir_error("/etc/resolv.conf", &path, e))?;
        Ok(Some(Mount {
            container_path: "/etc/resolv.conf".to_string(),
            host_path: path,
            readonly: true,
            propagation: MountPropagation::Private,
            selinux_relabel: true,
            fstype: None,
        }))
    }

    /// Point pods at a different cluster DNS, or a different domain.
    pub fn with_cluster_dns(mut self, servers: Vec<String>, domain: String) -> Self {
        if !servers.is_empty() {
            self.cluster_dns = servers;
        }
        if !domain.is_empty() {
            self.cluster_domain = domain;
        }
        self
    }

    /// `Err` when the directory or its files cannot be written (a full node,
    /// #129): binding a missing or half-written token is a container that
    /// fails at spawn or cannot reach the apiserver.
    async fn service_account_mount(&self, pod: &Value) -> Result<Option<Mount>, CriError> {
        if pod["spec"]["automountServiceAccountToken"].as_bool() == Some(false) {
            return Ok(None);
        }
        if pod_mounts_sa_path(pod) {
            return Ok(None); // SA admission already provided it — don't double-mount.
        }
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let sa = pod["spec"]["serviceAccountName"]
            .as_str()
            .unwrap_or("default");

        let dir = pod_volume_dir(&self.state_root, uid, "secret", "kube-api-access");
        let token = self.request_sa_token(namespace, sa, None).await;
        let written = (|| -> std::io::Result<()> {
            std::fs::create_dir_all(&dir)?;
            std::fs::write(format!("{dir}/namespace"), namespace)?;
            if let Some(token) = &token {
                std::fs::write(format!("{dir}/token"), token)?;
            }
            if let Some(ca) = &self.ca_pem {
                std::fs::write(format!("{dir}/ca.crt"), ca)?;
            }
            Ok(())
        })();
        written.map_err(|e| pod_dir_error("kube-api-access", &dir, e))?;

        Ok(Some(Mount {
            container_path: SA_MOUNT_PATH.to_string(),
            host_path: dir,
            readonly: true,
            propagation: MountPropagation::Private,
            selinux_relabel: true, // kubelet-materialized — must relabel for SELinux
            fstype: None,          // a directory to bind, not a device
        }))
    }

    /// Reconcile the in-memory pod map with sandboxes already running in the
    /// container runtime. Called once at startup so a kubelet restart adopts
    /// the pods it was already running instead of creating duplicate sandboxes.
    pub async fn recover_state(&self) -> Result<(), CriError> {
        let sandboxes = match self.runtime.list_pod_sandbox().await {
            Ok(s) => s,
            Err(e) => {
                warn!("state recovery: listing sandboxes failed: {e}");
                return Err(e);
            }
        };
        let mut recovered = 0;
        for sb in sandboxes {
            if sb.uid.is_empty() {
                continue;
            }
            // Map container name → id for the containers in this sandbox.
            let mut container_ids = HashMap::new();
            {
                let cs = self.runtime.list_containers(Some(&sb.id)).await?;
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
                    startup_passed: HashMap::new(),
                    started: HashMap::new(),
                    terminated: HashMap::new(),
                    init_statuses: Vec::new(),
                    sandbox_stopped: false,
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
        Ok(())
    }

    /// Sync desired pods (from API server) with actual running pods.
    ///
    /// Starts new pods, checks running ones (probes, restarts), and stops pods
    /// that carry a deletionTimestamp or disappeared from the desired set.
    pub async fn sync_pods(&self, desired_pods: &[Value]) -> SyncOutcome {
        self.sync_pods_observed(desired_pods, true).await
    }

    /// Incomplete API or manifest reads may reconcile known work, but cannot
    /// prove a Pod is absent. Keep live Pods and pending volume state intact.
    pub async fn sync_pods_observed(&self, desired_pods: &[Value], complete: bool) -> SyncOutcome {
        let mut outcome = SyncOutcome::default();
        let mut desired_uids: Vec<String> = Vec::new();
        let mut relist = std::time::Duration::ZERO;
        let mut relisted = false;

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
                        apimachinery::reactor::failed();
                        error!("Failed to stop terminating pod {namespace}/{name}: {e}");
                        continue;
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

            // Skip pods that are terminal *and meant to be*.
            //
            // An `Always` pod is never terminal, so a stored Succeeded or
            // Failed on one is a phase this kubelet should not have written
            // (or an older one did) — and skipping it here is what turned that
            // mistake into a pod nothing would ever restart. Reconciling it
            // is the recovery: the pod is started or re-checked like any
            // other, and its phase is corrected on the next status write.
            let restart_policy = pod["spec"]["restartPolicy"].as_str().unwrap_or("Always");
            if (phase == "Succeeded" || phase == "Failed") && restart_policy != "Always" {
                // Its sandbox may still hold the network: the stop at the
                // terminal pass failed, or this is that pass's own write
                // coming back (#137). Retried until it is given back.
                self.stop_finished_sandbox(uid).await;
                continue;
            }

            let partial = self.pods.read().await.get(uid).is_some_and(|p| p.phase=="Cleanup");
            if partial {
                if let Err(error)=self.stop_pod(uid).await {
                    warn!(%error, %uid, "partial Pod start cleanup pending");
                    self.due_in(uid, deadlines::RECHECK);
                    continue;
                }
            }

            let is_known = {
                let pods = self.pods.read().await;
                pods.get(uid).is_some_and(|p| p.phase != "Starting")
            };

            if !is_known {
                // New pod — start it
                let seen = *self
                    .first_seen
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(uid.to_string())
                    .or_insert_with(Instant::now);
                let started = self.start_pod(pod).await;
                if started.is_err() {
                    self.timing(pod, |t| t.attempt_failed());
                }
                match started {
                    Ok(status) => {
                        self.first_seen
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(uid);
                        self.network_waits
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(uid);
                        self.waiting
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(uid);
                        crate::metrics::observe_pod_start(seen.elapsed().as_secs_f64());
                        // One look once it is up: that pass schedules its
                        // probes (#101).
                        self.due_in(uid, deadlines::RECHECK);
                        outcome.updates.push(status)
                    }
                    // A volume that is not there yet is **Pending**, not
                    // Failed. Upstream retries a hostPath that does not exist
                    // because it may appear — another component creates it, a
                    // disk mounts — and marking the pod Failed ends it for a
                    // condition that has not been established as permanent.
                    // The same shape one layer over: a pod that cannot get
                    // an address waits for one. Marking it Failed left CoreDNS
                    // dead on a node whose network came up ten seconds later,
                    // and nothing retried it.
                    Err(CriError::Pending(what)) => {
                        self.retry_wait(uid, seen.elapsed());
                        outcome.updates.push(self.waiting_pod(pod,what));
                    }
                    // No CNI config yet (#148): woken when the config
                    // directory changes, so the pod starts as soon as the
                    // agent writes its conflist instead of up to ten seconds
                    // later on a backoff. The deadline is only the fallback
                    // for a node where the directory cannot be watched.
                    Err(CriError::NetworkNotConfigured(what)) => {
                        info!("Pod {namespace}/{name} waiting for a CNI network config: {what}");
                        self.network_waits
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(uid.to_string(), None);
                        self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
                        self.due_in(uid, deadlines::NETWORK_FALLBACK);
                        let message = format!("network is not ready: {what}");
                        outcome.updates.push(self.waiting_pod(pod, message));
                    }
                    // A config, and its ADD failed (the agent is not serving
                    // yet): retried on a backoff from the first ADD failure,
                    // not from when the pod was first seen, so a pod that
                    // waited 20 s for the config is not then asked about
                    // every 5 s.
                    Err(CriError::NetworkNotReady(what)) => {
                        warn!("Pod {namespace}/{name} waiting on the pod network: {what}");
                        let since = *self
                            .network_waits
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .entry(uid.to_string())
                            .and_modify(|at| {
                                at.get_or_insert_with(Instant::now);
                            })
                            .or_insert_with(|| Some(Instant::now()));
                        let failing = since.map(|at| at.elapsed()).unwrap_or_default();
                        self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
                        self.due_in(uid, deadlines::wait_backoff(failing));
                        let message = format!("network is not ready: {what}");
                        outcome.updates.push(self.waiting_pod(pod, message));
                    }
                    // Admitted and waiting, not missing (#63): the pod is
                    // recorded, its containers are `ContainerCreating` with
                    // the reason, and `describe` has a FailedMount Event.
                    // A full node (#129): waiting, with the errno, until
                    // something frees space. Failed would hand the pod back to
                    // its controller, which recreates it into the same disk.
                    Err(CriError::NodeStorage(what)) => {
                        warn!("Pod {namespace}/{name} waiting on node storage: {what}");
                        self.retry_wait(uid, seen.elapsed());
                        self.event(pod, "Warning", "Failed", &what).await;
                        outcome.updates.push(self.waiting_pod(pod, what));
                    }
                    Err(CriError::VolumeNotReady(what)) => {
                        warn!("Pod {namespace}/{name} waiting on volumes: {what}");
                        self.retry_wait(uid, seen.elapsed());
                        let message = volume_wait_message(&what, seen.elapsed());
                        self.event(pod, "Warning", "FailedMount", &message).await;
                        outcome.updates.push(self.waiting_pod(pod, message));
                    }
                    Err(e) => {
                        self.start_images.lock().unwrap().remove(uid);
                        if let Some(state)=self.pods.write().await.get_mut(uid) { state.phase="Cleanup".into(); }
                        apimachinery::reactor::failed();
                        self.retry_wait(uid, seen.elapsed());
                        error!("Failed to start pod {namespace}/{name}: {e}");
                        self.waiting
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(uid);
                        outcome.updates.push(PodStatusUpdate {
                            namespace: namespace.to_string(),
                            name: name.to_string(),
                            phase: "Failed".to_string(),
                            message: e.to_string(),
                            container_statuses: vec![],
                            init_container_statuses: vec![],
                            declared_init_containers: declared_init_containers(pod),
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
                let t = Instant::now();
                let checked = self.check_pod_status(uid).await;
                relist += t.elapsed();
                relisted = true;
                match checked {
                    Ok(status) => outcome.updates.push(status),
                    Err(e) => {
                        apimachinery::reactor::failed();
                        self.due_in(uid, deadlines::RECHECK);
                        warn!("Failed to check pod {namespace}/{name} status: {e}");
                    }
                }
            }
        }
        // The pass over known pods is this kubelet's relist: there is no
        // separate PLEG, and this is where their state is read back from the
        // runtime (probes run in it too).
        if relisted {
            crate::metrics::observe_relist(relist.as_secs_f64());
        }
        if !complete {
            return outcome;
        }
        self.first_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|uid, _| desired_uids.contains(uid));
        self.network_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|uid, _| desired_uids.contains(uid));
        self.timings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|uid, _| desired_uids.contains(uid));
        self.waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|uid, _| desired_uids.contains(uid));

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
                apimachinery::reactor::failed();
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
        volumes: &HashMap<String, ResolvedVolume>,
        pod_ip: Option<&str>,
        sa_mount: &Option<Mount>,
        dns_mount: &Option<Mount>,
        out: &mut Vec<InitContainerStatusReport>,
    ) -> Result<(), CriError> {
        let inits = match pod["spec"]["initContainers"].as_array() {
            Some(a) if !a.is_empty() => a.clone(),
            _ => return Ok(()),
        };
        let ns = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pod["metadata"]["name"].as_str().unwrap_or("");

        let uid=pod["metadata"]["uid"].as_str().unwrap_or("");
        for spec in &inits {
            let cname = spec["name"].as_str().unwrap_or("init");
            if out.iter().any(|r| r.name==cname && r.succeeded()) {continue;}
            let existing=self.pods.read().await.get(uid).and_then(|p|p.container_ids.get(cname).cloned());
            let image = spec["image"].as_str().unwrap_or("");
            info!("Init container {ns}/{name}/{cname}: ensuring image {image}");
            let image_ref = self.startup_image(pod,image, spec).await?;
            let mut envs = self.resolve_env(pod, spec, pod_ip).await;
            merge_env(&mut envs, self.service_account_env());
            let mut mounts = resolve_mounts(spec, volumes);
            push_mount(&mut mounts, sa_mount.clone());
            push_mount(&mut mounts, dns_mount.clone());
            let mut config = build_container_config(spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut config, pod);
            ensure_container_log_dir(&sandbox_config.log_directory, &config.name);
            let cid = if let Some(cid)=existing {cid} else {
            let cid = self.runtime.create_container(sandbox_id, &config, sandbox_config).await?;
            if let Some(state)=self.pods.write().await.get_mut(pod["metadata"]["uid"].as_str().unwrap_or("")) {
                state.container_ids.insert(cname.into(),cid.clone());
            }
            self.runtime.start_container(&cid).await?;
            if let Some(state)=self.pods.write().await.get_mut(uid) {state.started.insert(cname.into(),Instant::now());}
            cid
            };
            info!("Init container {ns}/{name}/{cname} started, waiting for completion");

            // Poll until the init container exits (bounded).
            let mut waited = self.pods.read().await.get(uid).and_then(|p|p.started.get(cname))
                .map(|t|t.elapsed().as_millis() as u64).unwrap_or(0);
            const POLL_MS: u64 = 500;
            const MAX_WAIT_MS: u64 = 120_000;
            loop {
                let status = self.runtime.container_status(&cid).await?;
                match status.state {
                    ContainerState::Exited => {
                        if status.exit_code != 0 {
                            // The failing one is the whole answer to "why is
                            // this pod not starting", so it is reported rather
                            // than only being turned into an error string.
                            out.push(InitContainerStatusReport {
                                name: cname.to_string(),
                                container_id: cid.clone(),
                                state: "terminated".into(),
                                exit_code: status.exit_code,
                                reason: "Error".into(),
                                message: status.message.clone(),
                                image: image.to_string(),
                                image_ref: image_ref.clone(),
                                started_at: status.started_at,
                                finished_at: status.finished_at,
                            });
                            let _ = self.runtime.remove_container(&cid).await;
                            return Err(CriError::Runtime(format!(
                                "init container {cname} exited with code {}",
                                status.exit_code
                            )));
                        }
                        info!("Init container {ns}/{name}/{cname} completed");
                        // Recorded *before* the removal below, which is the
                        // only chance: once the container is gone the runtime
                        // cannot be asked what it did, and this is what a pod
                        // reports as initContainerStatuses for the rest of its
                        // life.
                        out.push(InitContainerStatusReport {
                            name: cname.to_string(),
                            container_id: cid.clone(),
                            state: "terminated".into(),
                            exit_code: 0,
                            reason: "Completed".into(),
                            message: String::new(),
                            image: image.to_string(),
                            image_ref: image_ref.clone(),
                            started_at: status.started_at,
                            finished_at: status.finished_at,
                        });
                        if let Some(state)=self.pods.write().await.get_mut(uid) {state.init_statuses=out.clone();}
                        let _ = self.runtime.remove_container(&cid).await;
                        break;
                    }
                    _ => {
                        if waited >= MAX_WAIT_MS {
                            out.push(InitContainerStatusReport {
                                name: cname.to_string(),
                                container_id: cid.clone(),
                                state: "terminated".into(),
                                exit_code: -1,
                                reason: "DeadlineExceeded".into(),
                                message: format!(
                                    "init container did not exit within {}s",
                                    MAX_WAIT_MS / 1000
                                ),
                                image: image.to_string(),
                                image_ref: image_ref.clone(),
                                started_at: status.started_at,
                                finished_at: 0,
                            });
                            let _ = self.runtime.stop_container(&cid, 5).await;
                            let _ = self.runtime.remove_container(&cid).await;
                            return Err(CriError::Timeout);
                        }
                        if self.admission.is_some() {
                            // Its exit is an event; only the deadline is not.
                            let left = std::time::Duration::from_millis(MAX_WAIT_MS.saturating_sub(waited));
                            self.due_in(uid, left);
                            self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).insert(uid.to_string());
                            return Err(CriError::Pending(format!("init container {cname} is running")));
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
        let mut attempt = crate::start_timing::Attempt::begin();
        let pending = self
            .timings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|t| !t.is_started())
            .count()
            .max(1);
        attempt.queue(self.load.as_ref().map(|l| l.text()), pending);
        if self.admission.is_some() {self.ready_images(pod).await?;}

        let sandbox_config = build_sandbox_config(pod);
        let step = Instant::now();
        let mut volume_times = Vec::new();
        let volumes = self.resolve_volumes_timed(pod, &mut volume_times).await?;
        // Everything the kubelet writes for the pod is written before the
        // sandbox exists, so a node that cannot hold it (a full disk, #129)
        // leaves the pod waiting with the errno and nothing to undo.
        // Default ServiceAccount credential mount (token/ca/namespace),
        // computed once per pod and injected into every container.
        let token = Instant::now();
        let sa_mount = self.service_account_mount(pod).await?;
        if sa_mount.is_some() {
            volume_times.push(("(serviceaccount)".into(), token.elapsed()));
        }
        let dns_mount = self.resolv_conf_mount(pod)?;
        prepare_log_dirs(pod, &sandbox_config.log_directory)?;
        for (volume, took) in &volume_times {
            attempt.volume(volume, *took);
        }
        attempt.volumes(step.elapsed());

        // **The kubelet cannot check host paths from here.** It runs in a
        // container; the mounts happen in the engine's namespace, which is the
        // host's. A `Path::exists()` on this side asks the kubelet's own
        // filesystem and answers about the wrong machine — an earlier version
        // did exactly that and refused pods whose hostPaths were present on
        // the host and absent in this container.
        //
        // The engine does the mount and knows which one failed, so naming it
        // is the engine's job. See `ExecStep::Mounts` and the mount index it
        // carries.

        // Create pod sandbox
        let step = Instant::now();
        let existing=self.pods.read().await.get(uid).and_then(|p|p.sandbox_id.clone());
        let sandbox_id=if let Some(existing)=existing {existing} else {
        let sandbox_id = self.runtime.run_pod_sandbox(&sandbox_config).await?;
        // Persist each side effect before the next await. A failed or cancelled
        // start is cleaned by this UID before another sandbox can be created.
        self.pods.write().await.insert(uid.into(), PodState {
            namespace:namespace.into(),name:name.into(),uid:uid.into(),
            sandbox_id:Some(sandbox_id.clone()),container_ids:HashMap::new(),
            phase:"Starting".into(),pod:pod.clone(),pod_ip:None,
            restart_counts:HashMap::new(),ready:HashMap::new(),liveness_failures:HashMap::new(),
            startup_passed:HashMap::new(),started:HashMap::new(),terminated:HashMap::new(),init_statuses:Vec::new(),sandbox_stopped:false,
        });
        sandbox_id
        };
        info!("Created sandbox {sandbox_id} for {namespace}/{name}");

        // Get sandbox IP
        let sandbox_status = self.runtime.pod_sandbox_status(&sandbox_id).await?;
        let pod_ip = if sandbox_status.ip.is_empty() {
            None
        } else {
            Some(sandbox_status.ip.clone())
        };
        attempt.sandbox(step.elapsed());

        // Run init containers to completion (in order) before the app
        // containers — each must exit 0. A failure aborts pod start.
        // The out-param is why this is not a plain `?` on a returned Vec: when
        // an init container fails, the reports gathered so far are exactly
        // what says *which* one failed and how far the pod got, and a Result
        // that carried only the error would throw that away at the moment it
        // became useful.
        let mut init_statuses=self.pods.read().await.get(uid).map(|p|p.init_statuses.clone()).unwrap_or_default();
        let step = Instant::now();
        let init_outcome = self
            .run_init_containers(
                pod,
                &sandbox_id,
                &sandbox_config,
                &volumes,
                pod_ip.as_deref(),
                &sa_mount,
                &dns_mount,
                &mut init_statuses,
            )
            .await;
        if let Err(e) = init_outcome {
            if let Some(failed) = init_statuses.iter().find(|s| !s.succeeded()) {
                warn!(
                    "Pod {namespace}/{name}: init container {} {} (exit {})",
                    failed.name, failed.reason, failed.exit_code
                );
            }
            return Err(e);
        }
        attempt.init(step.elapsed());

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
            let step = Instant::now();

            // Ensure image per imagePullPolicy
            info!("Ensuring image {image} for {namespace}/{name}/{container_name}");
            // **Say what happened, not what upstream would have done.**
            //
            // This emitted `Pulling` before asking and `Successfully pulled`
            // after, unconditionally — so a container that resolved a local
            // golden in microseconds reported pulling from quay.io. On a node
            // whose entire design is that nothing is fetched at boot, that is
            // not a cosmetic inaccuracy: it is evidence of a fault that does
            // not exist, and it sent somebody looking for one.
            //
            // The image is checked first, so the event says which of the two
            // actually happened.
            let present = self
                .images
                .image_status(image)
                .await
                .ok()
                .flatten()
                .is_some();
            if !present {
                self.event_later(
                    pod,
                    "Pulling",
                    &format!("Pulling image \"{image}\""),
                )
                .await;
            }
            let image_ref = match self.startup_image(pod,image,container_spec).await {
                Ok(r) => {
                    if present {
                        // Upstream's word for an image that was already
                        // there, and the one `describe` readers expect.
                        self.event_later(
                            pod,
                            "Pulled",
                            &format!("Container image \"{image}\" already present on machine"),
                        )
                        .await;
                    } else {
                        self.event_later(
                            pod,
                            "Pulled",
                            &format!("Successfully pulled image \"{image}\""),
                        )
                        .await;
                    }
                    r
                }
                Err(e) => {
                    // The reason an image did not resolve is the whole
                    // diagnosis, and it lived only in a log on a node with no
                    // shell.
                    self.event(
                        pod,
                        "Warning",
                        "Failed",
                        &format!("Failed to pull image \"{image}\": {e}"),
                    )
                    .await;
                    return Err(e);
                }
            };

            // Build container config (resolve env valueFrom + mounts, then
            // inject the SA credential mount + KUBERNETES_SERVICE_* env).
            let mut envs = self
                .resolve_env(pod, container_spec, pod_ip.as_deref())
                .await;
            merge_env(&mut envs, self.service_account_env());
            let mut mounts = resolve_mounts(container_spec, &volumes);
            push_mount(&mut mounts, sa_mount.clone());
            push_mount(&mut mounts, dns_mount.clone());
            let mut container_config =
                build_container_config(container_spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut container_config, pod);
            ensure_container_log_dir(&sandbox_config.log_directory, &container_config.name);

            // Create container
            let container_id = match self
                .runtime
                .create_container(&sandbox_id, &container_config, &sandbox_config)
                .await
            {
                Ok(id) => {
                    self.event_later(
                        pod,
                        "Created",
                        &format!("Created container {container_name}"),
                    )
                    .await;
                    id
                }
                Err(e) => {
                    self.event(
                        pod,
                        "Warning",
                        "Failed",
                        &format!("Error creating container {container_name}: {e}"),
                    )
                    .await;
                    return Err(e);
                }
            };

            if let Some(state)=self.pods.write().await.get_mut(uid) {
                state.container_ids.insert(container_name.into(),container_id.clone());
            }
            // Start container
            if let Err(e) = self.runtime.start_container(&container_id).await {
                self.event(
                    pod,
                    "Warning",
                    "Failed",
                    &format!("Error starting container {container_name}: {e}"),
                )
                .await;
                return Err(e);
            }
            self.event_later(
                pod,
                "Started",
                &format!("Started container {container_name}"),
            )
            .await;
            info!("Started container {container_name} ({container_id}) in {namespace}/{name}");
            attempt.container(container_name, step.elapsed());

            // A container with a readiness probe starts not-ready until the
            // first probe succeeds; without one it is ready immediately.
            let ready = container_spec["readinessProbe"].is_null();
            ready_map.insert(container_name.to_string(), ready);
            started_map.insert(container_name.to_string(), Instant::now());

            container_ids.insert(container_name.to_string(), container_id.clone());
            container_statuses.push(ContainerStatusReport {
                started_at: 0,
                finished_at: 0,
                name: container_name.to_string(),
                container_id: container_id.clone(),
                state: "running".to_string(),
                ready,
                restart_count: 0,
                exit_code: 0,
                image: image.to_string(),
                image_ref: image_ref.to_string(),
                reason: String::new(),
                message: String::new(),
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
                    startup_passed: HashMap::new(),
                    started: started_map,
                    terminated: HashMap::new(),
                    init_statuses: init_statuses.clone(),
                    sandbox_stopped: false,
                },
            );
        }

        self.timing(pod, |t| t.started(attempt));
        Ok(PodStatusUpdate {
            namespace: namespace.to_string(),
            name: name.to_string(),
            phase: "Running".to_string(),
            message: String::new(),
            container_statuses,
            init_container_statuses: init_statuses,
            declared_init_containers: declared_init_containers(pod),
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
                    .map(|c| {
                        (
                            c["name"].as_str().unwrap_or("unnamed").to_string(),
                            c.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let sandbox_config = build_sandbox_config(&state.pod);
        let pod_ip = state.pod_ip.clone().unwrap_or_default();
        // The pod's network namespace, so http/tcp probes run inside it and can
        // reach loopback-bound health servers (e.g. cilium-operator on
        // 127.0.0.1:9234). Best-effort — falls back to the host netns if unknown.
        let pod_netns: Option<String> = match &state.sandbox_id {
            Some(sid) => self
                .runtime
                .pod_sandbox_status(sid)
                .await
                .ok()
                .and_then(|s| s.netns_path),
            None => None,
        };

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
                    started_at: 0,
                    finished_at: 0,
                    name: name.clone(),
                    container_id: cid.clone(),
                    state: "terminated".to_string(),
                    ready: false,
                    restart_count,
                    exit_code: *exit_code,
                    image: spec["image"].as_str().unwrap_or("").to_string(),
                    image_ref: String::new(),
                    reason: String::new(),
                    message: String::new(),
                });
                continue;
            }

            let status = match self.runtime.container_status(&cid).await {
                Ok(s) => s,
                Err(CriError::NotFound(_)) => {
                    // The container record is gone from the runtime — CRI-O
                    // pruned a crashed container, or the kubelet restarted after
                    // the container died. Recreate it per restart policy instead
                    // of reporting "waiting" forever (which would strand the pod;
                    // e.g. an adopted cilium-operator whose container had exited).
                    let should_recreate = matches!(restart_policy.as_str(), "Always" | "OnFailure");
                    let key = crate::crashloop::CrashLoopBackoff::key(uid, &name);
                    let backing_off = self.backoff.wait(&key);
                    if should_recreate && !spec.is_null() && backing_off.is_none() {
                        info!(
                            "Container {}/{}/{name} missing from runtime — recreating per policy {restart_policy}",
                            state.namespace, state.name
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
                        self.restarted_backoff(uid, &key);
                    } else if let Some(left) = backing_off.filter(|_| should_recreate) {
                        self.due_in(uid, left);
                        // A pruned record is the commonest way a crash loop
                        // looks from here, so this path needs the same gate as
                        // a plain exit — it is the one the cilium-operator
                        // runaway came through.
                        state.ready.insert(name.clone(), false);
                        container_statuses.push(ContainerStatusReport {
                            started_at: 0,
                            finished_at: 0,
                            name: name.clone(),
                            container_id: cid.clone(),
                            state: "waiting".to_string(),
                            ready: false,
                            restart_count,
                            exit_code: 0,
                            image: spec["image"].as_str().unwrap_or("").to_string(),
                            image_ref: String::new(),
                            reason: "CrashLoopBackOff".to_string(),
                            message: format!(
                                "back-off {}s restarting failed container {name}",
                                left.as_secs()
                            ),
                        });
                    } else {
                        state.terminated.insert(name.clone(), 0);
                        state.ready.insert(name.clone(), false);
                        container_statuses.push(ContainerStatusReport {
                            started_at: 0,
                            finished_at: 0,
                            name: name.clone(),
                            container_id: cid.clone(),
                            state: "terminated".to_string(),
                            ready: false,
                            restart_count,
                            exit_code: 0,
                            image: spec["image"].as_str().unwrap_or("").to_string(),
                            image_ref: String::new(),
                            reason: String::new(),
                            message: String::new(),
                        });
                    }
                    continue;
                }
                Err(e) => {
                    self.due_in(uid, deadlines::RECHECK);
                    // Transient error (e.g. RPC timeout): keep waiting rather than
                    // disturbing a container that may still be alive.
                    warn!(
                        "Failed to get status for container {name} in {}/{}: {e}",
                        state.namespace, state.name
                    );
                    state.ready.insert(name.clone(), false);
                    container_statuses.push(ContainerStatusReport {
                        started_at: 0,
                        finished_at: 0,
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "waiting".to_string(),
                        ready: false,
                        restart_count,
                        exit_code: 0,
                        image: spec["image"].as_str().unwrap_or("").to_string(),
                        image_ref: String::new(),
                        reason: String::new(),
                        message: String::new(),
                    });
                    continue;
                }
            };

            match status.state {
                ContainerState::Running => {
                    // This start of the container, which its probe runs
                    // belong to (#101). `None` when adopted.
                    let started = state.started.get(&name).copied();
                    let elapsed = started
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(u64::MAX);

                    // Startup probe: until it passes, gate out liveness/readiness
                    // so a slow-starting container (e.g. cilium-agent bringing up
                    // its :9879 health server) isn't killed prematurely.
                    let startup = &spec["startupProbe"];
                    if !startup.is_null()
                        && !state.startup_passed.get(&name).copied().unwrap_or(false)
                    {
                        let not_ready = |statuses: &mut Vec<ContainerStatusReport>| {
                            statuses.push(ContainerStatusReport {
                                started_at: status.started_at,
                                finished_at: status.finished_at,
                                name: name.clone(),
                                container_id: cid.clone(),
                                state: "running".to_string(),
                                ready: false,
                                restart_count,
                                exit_code: 0,
                                image: status.image.clone(),
                                image_ref: status.image_ref.clone(),
                                reason: String::new(),
                                message: String::new(),
                            });
                        };
                        // Not due: inside its initial delay, or run less
                        // than a period ago. Its deadline is set (#101).
                        if !self.probe_due(uid, &name, deadlines::STARTUP, startup, started) {
                            state.ready.insert(name.clone(), false);
                            not_ready(&mut container_statuses);
                            continue;
                        }
                        let result = run_probe(
                            startup,
                            &spec,
                            &cid,
                            &pod_ip,
                            pod_netns.as_deref(),
                            &self.runtime,
                        )
                        .await;
                        self.probed(uid, &name, deadlines::STARTUP, startup, started);
                        match result {
                            ProbeResult::Success => {
                                info!(
                                    "Startup probe passed for {}/{}/{name}",
                                    state.namespace, state.name
                                );
                                state.startup_passed.insert(name.clone(), true);
                                state.liveness_failures.insert(name.clone(), 0);
                                // fall through to liveness/readiness this cycle
                            }
                            ProbeResult::Failure(reason) => {
                                let failures =
                                    state.liveness_failures.entry(name.clone()).or_insert(0);
                                *failures += 1;
                                let threshold = probe_failure_threshold(startup);
                                warn!(
                                    "Startup probe failed for {}/{}/{name} ({}/{threshold}): {reason}",
                                    state.namespace, state.name, *failures
                                );
                                if *failures >= threshold {
                                    info!(
                                        "Startup threshold reached — restarting {}/{}/{name}",
                                        state.namespace, state.name
                                    );
                                    if self
                                        .restart_container(
                                            &mut state,
                                            &name,
                                            &cid,
                                            &spec,
                                            &sandbox_config,
                                            &mut container_statuses,
                                        )
                                        .await
                                    {
                                        continue;
                                    }
                                }
                                state.ready.insert(name.clone(), false);
                                not_ready(&mut container_statuses);
                                continue;
                            }
                            ProbeResult::Unknown => {
                                state.ready.insert(name.clone(), false);
                                not_ready(&mut container_statuses);
                                continue;
                            }
                        }
                    }

                    // Liveness probe: consecutive failures past the threshold
                    // kill and restart the container.
                    let liveness = &spec["livenessProbe"];
                    let mut restarted = false;
                    if !liveness.is_null()
                        && self.probe_due(uid, &name, deadlines::LIVENESS, liveness, started)
                    {
                        let result = run_probe(
                            liveness,
                            &spec,
                            &cid,
                            &pod_ip,
                            pod_netns.as_deref(),
                            &self.runtime,
                        )
                        .await;
                        self.probed(uid, &name, deadlines::LIVENESS, liveness, started);
                        match result {
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
                        // Not yet due: its deadline is recorded.
                        self.probe_due(uid, &name, deadlines::READINESS, readiness, started);
                        false
                    } else if !self.probe_due(uid, &name, deadlines::READINESS, readiness, started) {
                        // Ran less than a period ago: what it said then.
                        state.ready.get(&name).copied().unwrap_or(false)
                    } else {
                        let ok = matches!(
                            run_probe(
                                readiness,
                                &spec,
                                &cid,
                                &pod_ip,
                                pod_netns.as_deref(),
                                &self.runtime
                            )
                            .await,
                            ProbeResult::Success
                        );
                        self.probed(uid, &name, deadlines::READINESS, readiness, started);
                        ok
                    };
                    state.ready.insert(name.clone(), ready);
                    // Up, and forgiven once it has been up long enough.
                    self.backoff
                        .running(&crate::crashloop::CrashLoopBackoff::key(uid, &name));

                    container_statuses.push(ContainerStatusReport {
                        started_at: status.started_at,
                        finished_at: status.finished_at,
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "running".to_string(),
                        ready,
                        restart_count,
                        exit_code: 0,
                        image: status.image.clone(),
                        image_ref: status.image_ref,
                        reason: String::new(),
                        message: String::new(),
                    });
                }
                ContainerState::Exited => {
                    let should_restart = match restart_policy.as_str() {
                        "Always" => true,
                        "OnFailure" => status.exit_code != 0,
                        _ => false,
                    };

                    if should_restart {
                        let key = crate::crashloop::CrashLoopBackoff::key(uid, &name);
                        if let Some(left) = self.backoff.wait(&key) {
                            self.due_in(uid, left);
                            // Backing off. Reported as waiting with the reason
                            // a reader expects, rather than recreated now: a
                            // container recreated every sync tick is a crash
                            // loop at thirty restarts a minute, and the first
                            // failure — the one that says why — scrolls away.
                            state.ready.insert(name.clone(), false);
                            container_statuses.push(ContainerStatusReport {
                                started_at: status.started_at,
                                finished_at: status.finished_at,
                                name: name.clone(),
                                container_id: cid.clone(),
                                state: "waiting".to_string(),
                                ready: false,
                                restart_count,
                                exit_code: status.exit_code,
                                image: status.image.clone(),
                                image_ref: status.image_ref,
                                reason: "CrashLoopBackOff".to_string(),
                                message: format!(
                                    "back-off {}s restarting failed container {name}",
                                    left.as_secs()
                                ),
                            });
                        } else {
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
                            self.restarted_backoff(uid, &key);
                        }
                    } else {
                        info!(
                            "Container {}/{}/{name} exited (code {}) — not restarting (policy {restart_policy})",
                            state.namespace, state.name, status.exit_code
                        );
                        state.terminated.insert(name.clone(), status.exit_code);
                        state.ready.insert(name.clone(), false);
                        container_statuses.push(ContainerStatusReport {
                            started_at: status.started_at,
                            finished_at: status.finished_at,
                            name: name.clone(),
                            container_id: cid.clone(),
                            state: "terminated".to_string(),
                            ready: false,
                            restart_count,
                            exit_code: status.exit_code,
                            image: status.image.clone(),
                            image_ref: status.image_ref,
                            reason: String::new(),
                            message: String::new(),
                        });
                    }
                }
                ContainerState::Created | ContainerState::Unknown => {
                    self.due_in(uid, deadlines::RECHECK);
                    state.ready.insert(name.clone(), false);
                    container_statuses.push(ContainerStatusReport {
                        started_at: status.started_at,
                        finished_at: status.finished_at,
                        name: name.clone(),
                        container_id: cid.clone(),
                        state: "waiting".to_string(),
                        ready: false,
                        restart_count,
                        exit_code: 0,
                        image: status.image.clone(),
                        image_ref: status.image_ref,
                        reason: String::new(),
                        message: String::new(),
                    });
                }
            }
        }

        // Reconcile: create any app container declared in the spec that has no
        // live record and has not terminated for good. Covers an adopted pod
        // whose container had already exited (its record pruned by the runtime)
        // and partial starts. Initial creation is not gated by restartPolicy.
        let missing: Vec<String> = container_specs
            .keys()
            .filter(|n| !state.container_ids.contains_key(*n) && !state.terminated.contains_key(*n))
            .cloned()
            .collect();
        for name in missing {
            let spec = match container_specs.get(&name) {
                Some(s) if !s.is_null() => s.clone(),
                _ => continue,
            };
            // Gated like the other two recreate paths: this is where a
            // container whose record the runtime pruned comes back round, and
            // an ungated reconcile recreates it every sync tick.
            let key = crate::crashloop::CrashLoopBackoff::key(uid, &name);
            if let Some(left) = self.backoff.wait(&key) {
                self.due_in(uid, left);
                state.ready.insert(name.clone(), false);
                container_statuses.push(ContainerStatusReport {
                    started_at: 0,
                    finished_at: 0,
                    name: name.clone(),
                    container_id: String::new(),
                    state: "waiting".to_string(),
                    ready: false,
                    restart_count: 0,
                    exit_code: 0,
                    image: spec["image"].as_str().unwrap_or("").to_string(),
                    image_ref: String::new(),
                    reason: "CrashLoopBackOff".to_string(),
                    message: format!(
                        "back-off {}s restarting failed container {name}",
                        left.as_secs()
                    ),
                });
                continue;
            }
            info!(
                "Container {}/{}/{name} declared but not running — creating",
                state.namespace, state.name
            );
            self.restart_container(
                &mut state,
                &name,
                "", // no prior container record
                &spec,
                &sandbox_config,
                &mut container_statuses,
            )
            .await;
            self.restarted_backoff(uid, &key);
        }

        // Pod phase.
        //
        // A `restartPolicy: Always` pod is **never** terminal from a container
        // exiting: the contract of Always is that the container comes back, so
        // the pod stays Running and the container sits in waiting /
        // CrashLoopBackOff. Only Never and OnFailure reach Succeeded or Failed
        // this way (#25).
        //
        // Marking an Always pod Failed was not a cosmetic error. The sync loop
        // skips terminated pods, so the pod was never looked at again — the
        // cilium-agent DaemonSet pod sat Failed for hours and deleting it was
        // the only way out. A node that strands a recoverable pod has failed
        // at the one thing the kubelet is for.
        let total = state.container_ids.len();
        let phase = if restart_policy == "Always" {
            "Running"
        } else if total > 0 && state.terminated.len() == total {
            if state.terminated.values().all(|&code| code == 0) {
                "Succeeded"
            } else {
                "Failed"
            }
        } else {
            "Running"
        };
        state.phase = phase.to_string();
        let finished = phase != "Running";

        let update = PodStatusUpdate {
            namespace: state.namespace.clone(),
            name: state.name.clone(),
            phase: phase.to_string(),
            message: String::new(),
            container_statuses,
            // From the recorded state, not from the runtime: the init
            // containers were removed when they exited, so this is the only
            // surviving account of them. Without it every sync after the
            // first would report a pod with no init containers at all.
            init_container_statuses: state.init_statuses.clone(),
            declared_init_containers: declared_init_containers(&state.pod),
            pod_ip: state.pod_ip.clone(),
        };

        // Persist mutated state.
        {
            let mut pods = self.pods.write().await;
            pods.insert(uid.to_string(), state);
        }
        if finished {
            self.stop_finished_sandbox(uid).await;
        }

        Ok(update)
    }

    /// A finished pod (Succeeded or Failed: every container terminated for
    /// good) gives its sandbox's network back now, not when the Pod object is
    /// deleted (#137). Upstream's kubelet does the same: the sandbox of a pod
    /// that will not run again is stopped, CNI DEL frees its address, and the
    /// containers' records, status and logs stay until the Pod goes.
    ///
    /// Without this a `restartPolicy: Never` Job pod held its address until
    /// something deleted it: 1,000 sleep pods on pvetest1 filled Cilium's
    /// range at ~250 and the rest waited on "range is full".
    ///
    /// Once per pod; a failure is retried on RECHECK (the worker's next pass
    /// reaches the terminal branch of the sync, which calls this again).
    async fn stop_finished_sandbox(&self, uid: &str) {
        let (sandbox, namespace, name) = {
            let pods = self.pods.read().await;
            let Some(state) = pods.get(uid) else { return };
            if state.sandbox_stopped {
                return;
            }
            (state.sandbox_id.clone(), state.namespace.clone(), state.name.clone())
        };
        if let Some(sandbox) = &sandbox {
            match self.runtime.stop_pod_sandbox(sandbox).await {
                Ok(()) | Err(CriError::NotFound(_)) => {
                    info!("Pod {namespace}/{name} finished: sandbox {sandbox} stopped, network released");
                }
                Err(e) => {
                    apimachinery::reactor::failed();
                    warn!("Pod {namespace}/{name} finished but its sandbox did not stop (retried): {e}");
                    self.due_in(uid, deadlines::RECHECK);
                    return;
                }
            }
        }
        if let Some(state) = self.pods.write().await.get_mut(uid) {
            state.sandbox_stopped = true;
        }
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
                self.due_in(&state.uid, deadlines::RECHECK);
                return false;
            }
        };

        let image = spec["image"].as_str().unwrap_or("");
        // A volume this node must not provision stops the restart: the same
        // reason is on the pod's status from the start path, and retrying
        // every tick would not change it.
        let volumes = match self.resolve_volumes(&state.pod).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Container {}/{}/{name}: {e}", state.namespace, state.name);
                self.due_in(&state.uid, deadlines::RECHECK);
                return false;
            }
        };
        let mut envs = self
            .resolve_env(&state.pod, spec, state.pod_ip.as_deref())
            .await;
        merge_env(&mut envs, self.service_account_env());
        let mut mounts = resolve_mounts(spec, &volumes);
        match self.service_account_mount(&state.pod).await {
            Ok(m) => push_mount(&mut mounts, m),
            Err(e) => {
                warn!("Container {}/{}/{name}: {e}", state.namespace, state.name);
                self.due_in(&state.uid, deadlines::RECHECK);
                return false;
            }
        }
        // Capture the pod's namespace-sharing flags before `state` is borrowed
        // by the async block below (which also mutably borrows `state` later).
        let pod_for_ns = state.pod.clone();
        let result = async move {
            let image_ref = self.ensure_image(image, spec).await?;
            let mut config = build_container_config(spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut config, &pod_for_ns);
            config.attempt = restart_count;
            config.log_path = format!("{}/{restart_count}.log", config.name);
            ensure_container_log_dir(&sandbox_config.log_directory, &config.name);
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
                state
                    .container_ids
                    .insert(name.to_string(), new_cid.clone());
                state.started.insert(name.to_string(), Instant::now());
                let ready = spec["readinessProbe"].is_null();
                state.ready.insert(name.to_string(), ready);
                container_statuses.push(ContainerStatusReport {
                    started_at: 0,
                    finished_at: 0,
                    name: name.to_string(),
                    container_id: new_cid,
                    state: "running".to_string(),
                    ready,
                    restart_count,
                    exit_code: 0,
                    image: image.to_string(),
                    image_ref,
                    reason: String::new(),
                    message: String::new(),
                });
                true
            }
            Err(e) => {
                self.due_in(&state.uid, deadlines::RECHECK);
                error!(
                    "Failed to restart container {}/{}/{name}: {e}",
                    state.namespace, state.name
                );
                state.ready.insert(name.to_string(), false);
                container_statuses.push(ContainerStatusReport {
                    started_at: 0,
                    finished_at: 0,
                    name: name.to_string(),
                    container_id: String::new(),
                    state: "waiting".to_string(),
                    ready: false,
                    restart_count,
                    exit_code: 0,
                    image: image.to_string(),
                    image_ref: String::new(),
                    reason: String::new(),
                    message: String::new(),
                });
                false
            }
        }
    }

    /// Get the sandbox ID for a pod by UID.
    /// Per-container resource stats from the runtime (for /metrics/cadvisor
    /// and /stats/summary).
    pub async fn container_stats(&self) -> Vec<crate::cri::ContainerStatsInfo> {
        self.runtime
            .list_container_stats()
            .await
            .unwrap_or_default()
    }

    /// What `/metrics` reports, read at scrape time: pods with a sandbox, and
    /// every container the runtime lists, by state (#36).
    pub async fn metrics_snapshot(&self) -> crate::metrics::KubeletSnapshot {
        let running_pods = self
            .pods
            .read()
            .await
            .values()
            .filter(|p| p.sandbox_id.is_some())
            .count();
        let containers = self
            .runtime
            .list_containers(None)
            .await
            .map(|cs| cs.into_iter().map(|c| c.state).collect())
            .unwrap_or_default();
        crate::metrics::KubeletSnapshot {
            running_pods,
            containers,
        }
    }

    /// Per-pod network counters from the runtime (for /metrics/cadvisor).
    pub async fn pod_network_stats(&self) -> Vec<crate::cri::PodNetworkStats> {
        self.runtime
            .list_pod_network_stats()
            .await
            .unwrap_or_default()
    }

    /// A v1 PodList of the pods this kubelet manages (for the /pods endpoint).
    /// The UID of a pod this node is running, by namespace and name.
    ///
    /// Needed because a container's log path contains the UID
    /// (`/var/log/pods/<ns>_<pod>_<uid>/`) while the URL `kubectl logs` sends
    /// does not — and because a pod this node has never heard of should be a
    /// A restart was made (or tried): the next is gated by the backoff, and
    /// the pod is looked at again when it runs out (#101).
    fn restarted_backoff(&self, uid: &str, key: &str) {
        self.backoff.restarted(key);
        if let Some(left) = self.backoff.wait(key) {
            self.due_in(uid, left);
        }
    }

    /// A start that is waiting is tried again with a backoff growing with
    /// the wait, unless what it waits on is an event (#101).
    /// This pod's start record, made now if the pod list has not noted it
    /// (#132). Seen is the same instant as `first_seen`.
    fn timing(&self, pod: &Value, f: impl FnOnce(&mut crate::start_timing::StartTiming)) {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        if uid.is_empty() {
            return;
        }
        let seen = *self
            .first_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(uid.to_string())
            .or_insert_with(Instant::now);
        let mut timings = self.timings.lock().unwrap_or_else(|e| e.into_inner());
        f(timings
            .entry(uid.to_string())
            .or_insert_with(|| crate::start_timing::StartTiming::new(pod, seen)));
    }

    /// The pods a list or watch (or a manifest read) just delivered: a pod
    /// not started here is seen now, if it was not already (#132).
    pub async fn note_seen(&self, pods: &[Value]) {
        let known = self.pods.read().await;
        for pod in pods {
            let Some(uid) = pod["metadata"]["uid"].as_str() else { continue };
            let started = known.get(uid).is_some_and(|p| p.phase != "Starting" && p.phase != "Cleanup");
            let terminal = matches!(pod["status"]["phase"].as_str(), Some("Succeeded" | "Failed"));
            if started || terminal || !pod["metadata"]["deletionTimestamp"].is_null()
                || self.timings.lock().unwrap_or_else(|e| e.into_inner()).contains_key(uid)
            {
                continue;
            }
            self.timing(pod, |_| {});
        }
    }

    /// `Running` was acknowledged by the apiserver (or needed no write), the
    /// write taking `report`. The first time after a start, publish where the
    /// start went (#132): one INFO line, the histograms, and, for a pod the
    /// apiserver has, the `storm.io/start-timing` annotation and a
    /// `StartTiming` Event. Anything else: nothing.
    pub async fn start_reported(&self, pod: &Value, report: std::time::Duration) {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let acked = Instant::now();
        let finished = {
            let mut timings = self.timings.lock().unwrap_or_else(|e| e.into_inner());
            match timings.get(uid) {
                Some(t) if t.is_started() => {
                    let f = t.finish(report, acked);
                    timings.remove(uid);
                    f
                }
                _ => return,
            }
        };
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        info!("Pod {namespace}/{name} started: {}", finished.text);
        for &(phase, took) in &finished.phases {
            crate::metrics::observe_start_phase(phase, took.as_secs_f64());
        }
        // A static pod has no object to annotate.
        if self.api_url.is_empty() || pod["metadata"]["resourceVersion"].as_str().is_none() {
            return;
        }
        self.event_later(pod, crate::start_timing::REASON, &finished.text).await;
        let patch = serde_json::json!({"metadata": {"uid": uid, "annotations": {
            crate::start_timing::ANNOTATION: finished.text}}});
        // Off the worker (#138): the pod is running and reported, and its
        // pass held a worker another pod's start was queued for while this
        // write went to the apiserver.
        let request = self
            .api_client
            .patch(format!("{}/api/v1/namespaces/{namespace}/pods/{name}", self.api_url))
            .header("content-type", "application/merge-patch+json")
            .timeout(std::time::Duration::from_secs(10))
            .json(&patch);
        let pod_name = format!("{namespace}/{name}");
        tokio::spawn(async move {
            if let Err(error) = request.send().await.and_then(|r| r.error_for_status()) {
                warn!(%error, "Pod {pod_name}: start timing annotation not written");
            }
        });
    }

    /// The pods (by uid) waiting for a CNI network config to appear (#148):
    /// what a change in the config directory wakes.
    pub fn waiting_for_network_config(&self) -> std::collections::HashSet<String> {
        self.network_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, at)| at.is_none())
            .map(|(uid, _)| uid.clone())
            .collect()
    }

    fn retry_wait(&self, uid: &str, waited: std::time::Duration) {
        if self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid) {
            return;
        }
        self.due_in(uid, deadlines::wait_backoff(waited));
    }

    /// 404 rather than an empty log.
    /// Record a pod as admitted and waiting, and the status that says so:
    /// Pending, every container `waiting: ContainerCreating` with `message`.
    fn waiting_pod(&self, pod: &Value, message: String) -> PodStatusUpdate {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let name = pod["metadata"]["name"].as_str().unwrap_or("").to_string();
        let namespace = pod["metadata"]["namespace"]
            .as_str()
            .unwrap_or("default")
            .to_string();
        self.waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                uid.to_string(),
                WaitingPod {
                    namespace: namespace.clone(),
                    name: name.clone(),
                    reason: message.clone(),
                },
            );
        PodStatusUpdate {
            namespace,
            name,
            phase: "Pending".to_string(),
            container_statuses: creating_statuses(pod, &message),
            message,
            init_container_statuses: vec![],
            declared_init_containers: declared_init_containers(pod),
            pod_ip: None,
        }
    }

    /// Why a pod this node has admitted is not started yet, when it is
    /// waiting (#63). `logs` answers with it rather than "not found".
    pub fn waiting_reason(&self, namespace: &str, name: &str) -> Option<String> {
        self.waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|w| w.namespace == namespace && w.name == name)
            .map(|w| w.reason.clone())
    }

    pub async fn pod_uid(&self, namespace: &str, name: &str) -> Option<String> {
        let pods = self.pods.read().await;
        pods.values()
            .find(|p| p.namespace == namespace && p.name == name)
            .map(|p| p.uid.clone())
    }

    pub async fn pods_json(&self) -> Value {
        let pods = self.pods.read().await;
        let mut items: Vec<Value> = pods
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
        // Admitted and waiting pods are this node's too (#63).
        let waiting = self
            .waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        items.extend(
            waiting
                .iter()
                .filter(|(uid, _)| !pods.contains_key(*uid))
                .map(|(uid, w)| {
                    serde_json::json!({
                        "metadata": {"name": w.name, "namespace": w.namespace, "uid": uid},
                        "status": {"phase": "Pending", "message": w.reason}
                    })
                }),
        );
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
                startup_passed: HashMap::new(),
                started: HashMap::new(),
                terminated: HashMap::new(),
                init_statuses: Vec::new(),
                sandbox_stopped: false,
            },
        );
        info!("Registered restored pod {namespace}/{name} with sandbox {sandbox_id}");
    }

    /// Stop and remove a pod.
    pub async fn stop_pod(&self, uid: &str) -> Result<(), CriError> {
        fn stopped(result: Result<(), CriError>) -> Result<(), CriError> {
            match result {
                Err(CriError::NotFound(_)) => Ok(()),
                result => result,
            }
        }
        // Retain the record through every await. Cancellation or RPC failure
        // must leave enough state to retry rather than forgetting a live Pod.
        let state = self.pods.read().await.get(uid).cloned();
        if let Some(state) = state {
            for (name, cid) in &state.container_ids {
                info!("Stopping container {name} ({cid})");
                stopped(self.runtime.stop_container(cid, 30).await)?;
                stopped(self.runtime.remove_container(cid).await)?;
            }
            if let Some(sandbox) = &state.sandbox_id {
                stopped(self.runtime.stop_pod_sandbox(sandbox).await)?;
                stopped(self.runtime.remove_pod_sandbox(sandbox).await)?;
            }
        }
        if !self.teardown_csi_volumes(uid).await {
            return Err(CriError::VolumeNotReady(
                "Pod volume cleanup is still pending".into(),
            ));
        }
        self.pods.write().await.remove(uid);
        self.start_images.lock().unwrap().remove(uid);
        self.backoff.forget_pod(uid);
        self.forget_deadlines(uid);
        self.first_seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(uid);
        self.network_waits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(uid);
        self.timings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(uid);
        self.waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(uid);
        Ok(())
    }
}

/// Why a claim could not be turned into a device.
///
/// Split because the two halves deserve opposite treatment, and conflating
/// them is what made the scratch fallback dangerous (#44). `Failed` is
/// transient — the engine is unreachable, a clone failed, an attach 409'd —
/// and a pod that will not start on a node whose storage is briefly
/// unreachable is worse than one that starts with scratch and says so loudly.
/// `NotOurs` is permanent and deterministic, so the same fallback would give
/// a pod scratch storage *forever* because its claim belongs to another
/// driver, which is that trade's cost without its benefit.
/// Lay down a blank filesystem for a size class, once: `POST /api/v1/fstemplates`.
///
/// The name is `template_name`'s, and that matters more than it looks:
/// stormcos once had the registry looking a blank up as `pvc-ext4j-<mib>m`
/// while the image called it `pvc-1M`, and neither side could see the other's
/// name. Minting through the same function the lookup uses keeps that from
/// coming back from this side.
///
/// `role: data`: the blank, and so every claim cloned from it, lives in the
/// half no install formats — a claim shares its blank's unwritten extents, and
/// the system half is replaced by every install. stormblock formats and seals
/// it. A racing mint of the same class gets 409, which is not a failure: the
/// caller looks the template up either way.
async fn mint_blank(
    engine: &crate::engine::EngineClient,
    url: &str,
    blank: &str,
    class: &str,
) -> Result<(), String> {
    // The class's filesystem and its size in MiB (stormblock has no `P`).
    let fs = crate::storage::class_fs(class)?;
    let bytes = crate::storage::SIZE_CLASSES
        .iter()
        .find(|(c, _, _)| *c == class)
        .map(|(_, b, _)| *b)
        .ok_or_else(|| format!("no size class {class}"))?;
    let body = serde_json::json!({
        "name": blank,
        "size": crate::storage::engine_size(bytes),
        "fs": fs,
        "role": "data",
    });
    // Bounded by MINT_TIMEOUT, not the ordinary request bound: the answer
    // comes when the format is done (#99).
    let resp = engine
        .post_within(url, &body, crate::engine::MINT_TIMEOUT)
        .await
        .map_err(|e| format!("stormblock would not mint the blank {blank}: {e}"))?;
    let status = resp.status();
    if status.is_success() || status.as_u16() == 409 {
        return Ok(());
    }
    let text = resp.text().await.unwrap_or_default();
    Err(format!(
        "stormblock would not mint the blank {blank}: {status}: {}",
        text.chars().take(200).collect::<String>()
    ))
}

#[derive(Debug)]
enum ClaimError {
    /// Another provisioner's claim. Never falls back.
    NotOurs(String),
    /// A `ReadWriteOncePod` claim another pod on this node already holds.
    /// Never falls back either: the point of the mode is that the second
    /// mount does not happen.
    InUse(String),
    /// Something went wrong that may not be wrong next time.
    Failed(String),
}

impl std::fmt::Display for ClaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClaimError::NotOurs(m) | ClaimError::InUse(m) | ClaimError::Failed(m) => f.write_str(m),
        }
    }
}

/// What stormblock said instead of doing a POST, or why it said nothing
/// (#140). `status` is `None` when no answer came: the request bound ran out,
/// or the engine was not there.
#[derive(Debug, Clone)]
struct EngineRefusal {
    status: Option<u16>,
    /// The engine's body as sent: the `error` text is what tells a broken
    /// template from a refused clone.
    body: String,
    message: String,
}

impl EngineRefusal {
    fn refused(path: &str, status: u16, body: &str) -> EngineRefusal {
        let reason = serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|v| v["error"].as_str().map(String::from))
            .unwrap_or_else(|| body.to_string());
        EngineRefusal {
            status: Some(status),
            body: body.to_string(),
            message: format!("stormblock POST {path} -> {status}: {}", excerpt(&reason)),
        }
    }

    fn unanswered(path: &str, e: &reqwest::Error) -> EngineRefusal {
        // reqwest's own Display is "error sending request for url (…)"; the
        // cause is further down the chain.
        let mut why = e.to_string();
        let mut src = std::error::Error::source(e);
        while let Some(s) = src {
            why = format!("{why}: {s}");
            src = s.source();
        }
        let message = if e.is_timeout() {
            format!(
                "stormblock POST {path}: no answer within {} s ({why})",
                crate::engine::REQUEST_TIMEOUT.as_secs()
            )
        } else {
            format!("stormblock POST {path}: no answer ({why})")
        };
        EngineRefusal { status: None, body: String::new(), message }
    }

    /// A clone refused because of the *template*, not the clone: its sealed
    /// volume is gone, not sealed, or was never recorded. stormblock answers
    /// that the same way every time, so retrying the clone never gets past
    /// it; the template has to be made again. A template that is not `ready`
    /// yet ("fstemplate … is awaiting_format — seal it before cloning") is
    /// still being made, and is not broken.
    fn template_broken(&self) -> bool {
        let body = self.body.as_str();
        match self.status {
            // `volume <id> not found`: the template names a volume the engine
            // does not have. `fstemplate … not found` is the template itself
            // gone, which the next pass mints anyway.
            Some(404) => body.contains("volume") && !body.contains("fstemplate"),
            Some(409) => body.contains("is not sealed"),
            Some(500) => body.contains("has no sealed snapshot"),
            _ => false,
        }
    }
}

impl std::fmt::Display for EngineRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// The first 300 characters of an engine answer, for a log line or a status.
fn excerpt(text: &str) -> String {
    let t = text.trim();
    if t.chars().count() <= 300 {
        return t.to_string();
    }
    format!("{}…", t.chars().take(300).collect::<String>())
}

/// Status update to send back to the API server.
#[derive(Debug)]
pub struct PodStatusUpdate {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub message: String,
    pub container_statuses: Vec<ContainerStatusReport>,
    /// What the pod's init containers did. Empty for a pod that has none —
    /// which the emitter distinguishes from a pod whose inits are unreported,
    /// because `Initialized` is computed from the pod spec, not from this.
    pub init_container_statuses: Vec<InitContainerStatusReport>,
    /// How many init containers the pod *declares*.
    ///
    /// The `Initialized` condition is computed against this rather than
    /// against the reports being non-empty, because the two differ exactly
    /// when it matters: a pod that has not reached its init containers yet
    /// reports none, and "none reported" must not read the same as "none
    /// declared". A pod with no init containers is initialized; a pod with
    /// three and no reports is not.
    pub declared_init_containers: usize,
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
    /// The `waiting` reason, when the state is `waiting`. Empty means
    /// `ContainerCreating` — the ordinary case of a container on its way up.
    ///
    /// `CrashLoopBackOff` is the one that matters: it is how a person reading
    /// `kubectl get pod` learns the difference between a container that is
    /// starting and one that has been failing for ten minutes.
    pub reason: String,
    /// Free text under the reason — for a backoff, how much of it is left.
    pub message: String,
    /// When the container actually started and finished, epoch nanoseconds as
    /// the runtime reports them. **Zero means unknown**, and is rendered as
    /// null rather than as a time.
    ///
    /// These exist because the status reporter used to stamp `startedAt` and
    /// `finishedAt` with `now()` — the moment the status was *reported*, not
    /// the moment anything happened. Every container therefore claimed to
    /// have started seconds ago on every poll, so a container that had been
    /// up for an hour was indistinguishable from one that had just been
    /// restarted, and reading a restart into it was the natural mistake.
    pub started_at: i64,
    pub finished_at: i64,
}

/// How many init containers a pod declares.
/// Every container of a pod that has not started, as `ContainerCreating`
/// with why. An empty list read as "this pod has no containers", and
/// `kubectl get pod` showed Pending with nothing under it (#63).
fn creating_statuses(pod: &Value, message: &str) -> Vec<ContainerStatusReport> {
    pod["spec"]["containers"]
        .as_array()
        .map(|cs| {
            cs.iter()
                .map(|c| ContainerStatusReport {
                    name: c["name"].as_str().unwrap_or("").to_string(),
                    container_id: String::new(),
                    state: "waiting".to_string(),
                    ready: false,
                    restart_count: 0,
                    exit_code: 0,
                    image: c["image"].as_str().unwrap_or("").to_string(),
                    image_ref: String::new(),
                    reason: "ContainerCreating".to_string(),
                    message: message.to_string(),
                    started_at: 0,
                    finished_at: 0,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What a pod waiting on its volumes says, upstream's shape. Past
/// [`VOLUME_WAIT_TIMEOUT`] it says it timed out, and it keeps waiting.
///
/// The minutes are the timeout's, not the elapsed time, so the message is the
/// same on every sync and the Event aggregates instead of multiplying.
pub fn volume_wait_message(what: &str, waited: std::time::Duration) -> String {
    if waited >= VOLUME_WAIT_TIMEOUT {
        format!(
            "Unable to attach or mount volumes: unmounted volumes=[{what}]: timed out after {}m \
             waiting for the condition; still retrying",
            VOLUME_WAIT_TIMEOUT.as_secs() / 60
        )
    } else {
        format!("Unable to attach or mount volumes: unmounted volumes=[{what}]")
    }
}

pub fn declared_init_containers(pod: &Value) -> usize {
    pod["spec"]["initContainers"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

/// Build the sandbox config for a pod object.
fn build_sandbox_config(pod: &Value) -> PodSandboxConfig {
    let name = pod["metadata"]["name"].as_str().unwrap_or("");
    let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
    let uid = pod["metadata"]["uid"].as_str().unwrap_or("");

    // The sandbox must allow privileged containers if any container (init or
    // app) requests it, else the runtime rejects the privileged container.
    let any_privileged = ["containers", "initContainers"].iter().any(|k| {
        pod["spec"][k]
            .as_array()
            .map(|a| {
                a.iter()
                    .any(|c| c["securityContext"]["privileged"].as_bool() == Some(true))
            })
            .unwrap_or(false)
    });

    PodSandboxConfig {
        name: name.to_string(),
        uid: uid.to_string(),
        namespace: namespace.to_string(),
        attempt: 0,
        hostname: pod["spec"]["hostname"].as_str().unwrap_or(name).to_string(),
        log_directory: format!("/var/log/pods/{namespace}_{name}_{uid}"),
        dns_servers: pod["spec"]["dnsConfig"]["nameservers"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
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
        privileged: any_privileged,
        seccomp_profile: parse_seccomp(&pod["spec"]["securityContext"]),
        // Pod-level seLinuxOptions labels the sandbox as well as the
        // containers. The sandbox owns the namespaces the containers join, so
        // leaving it at the default type while the containers run as another
        // is the inconsistency that shows up as a denial on the shared
        // resource rather than on the container that was actually relabelled.
        selinux_options: parse_selinux_options(&pod["spec"]["securityContext"]),
    }
}

/// The PVC names a pod mounts, in spec order.
fn pod_claims(pod: &Value) -> impl Iterator<Item = &str> {
    pod["spec"]["volumes"]
        .as_array()
        .map(|v| v.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter_map(|vol| vol["persistentVolumeClaim"]["claimName"].as_str())
}

/// Per-pod volume directory: <state_root>/pods/<uid>/volumes/kubernetes.io~<kind>/<name>.
/// The claim a volume mounts: `persistentVolumeClaim.claimName`, or for a
/// generic ephemeral volume the claim made for it, `<pod>-<volume>` (the
/// name upstream's ephemeral-volume controller gives it, and the one
/// rustkube's attach/detach controller looks for).
fn claim_of(pod: &Value, vol: &Value) -> Option<String> {
    if let Some(c) = vol["persistentVolumeClaim"]["claimName"].as_str() {
        return Some(c.to_string());
    }
    vol.get("ephemeral")?;
    Some(format!(
        "{}-{}",
        pod["metadata"]["name"].as_str().unwrap_or(""),
        vol["name"].as_str().unwrap_or("")
    ))
}

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
) -> std::io::Result<()> {
    let data = match data {
        Some(d) => d,
        None => return Ok(()),
    };
    match items {
        Some(items) => {
            for it in items {
                let key = it["key"].as_str().unwrap_or("");
                let path = it["path"].as_str().unwrap_or(key);
                if let Some(s) = data.get(key).and_then(|v| v.as_str()) {
                    std::fs::write(format!("{dir}/{path}"), s)?;
                }
            }
        }
        None => {
            for (key, val) in data {
                if let Some(s) = val.as_str() {
                    std::fs::write(format!("{dir}/{key}"), s)?;
                }
            }
        }
    }
    Ok(())
}

/// Write each ConfigMap data entry as a file `<dir>/<key>` (0644).
fn materialize_files(
    dir: &str,
    data: Option<&serde_json::Map<String, Value>>,
    _binary: bool,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    if let Some(data) = data {
        for (key, val) in data {
            if let Some(s) = val.as_str() {
                std::fs::write(format!("{dir}/{key}"), s)?;
            }
        }
    }
    Ok(())
}

/// Write decoded Secret entries as files `<dir>/<key>` (0600 best-effort).
fn materialize_secret_files(dir: &str, data: Option<&HashMap<String, String>>) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    if let Some(data) = data {
        for (key, val) in data {
            let path = format!("{dir}/{key}");
            std::fs::write(&path, val)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
    Ok(())
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

/// Does the pod use volume `name` against its claim's mode (#67)? A Block
/// claim named in some container's `volumeMounts`, or a Filesystem one in
/// `volumeDevices`, is refused, as upstream refuses it.
fn volume_mode_misuse(pod: &Value, name: &str, block: bool) -> Option<String> {
    let (wrong, want) = if block {
        ("volumeMounts", "volumeDevices")
    } else {
        ("volumeDevices", "volumeMounts")
    };
    let mode = if block { "Block" } else { "Filesystem" };
    ["initContainers", "containers"]
        .iter()
        .filter_map(|k| pod["spec"][*k].as_array())
        .flatten()
        .find(|c| {
            c[wrong]
                .as_array()
                .is_some_and(|l| l.iter().any(|m| m["name"].as_str() == Some(name)))
        })
        .map(|c| {
            format!(
                "volume {name} is volumeMode: {mode}, and container {} names it in {wrong}; \
                 use {want}",
                c["name"].as_str().unwrap_or("?")
            )
        })
}

/// Resolve a container's `volumeMounts` (and `volumeDevices`, #67) to CRI
/// mounts using the pod's resolved volumes. Mounts whose volume didn't
/// resolve are dropped.
fn resolve_mounts(spec: &Value, volumes: &HashMap<String, ResolvedVolume>) -> Vec<Mount> {
    let mut mounts = resolve_volume_mounts(spec, volumes);
    mounts.extend(resolve_volume_devices(spec, volumes));
    mounts
}

/// A container's `volumeDevices`: a raw block claim's device, bound as it is
/// at `devicePath` (stormpump binds a device node onto a file placeholder).
/// No filesystem, no relabel, and no propagation: it is a device, not a tree.
fn resolve_volume_devices(spec: &Value, volumes: &HashMap<String, ResolvedVolume>) -> Vec<Mount> {
    spec["volumeDevices"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|d| {
                    let resolved = volumes.get(d["name"].as_str()?)?;
                    if !resolved.block {
                        return None;
                    }
                    Some(Mount {
                        container_path: d["devicePath"].as_str()?.to_string(),
                        host_path: resolved.path.clone(),
                        readonly: false,
                        propagation: MountPropagation::Private,
                        selinux_relabel: false,
                        fstype: None,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_volume_mounts(spec: &Value, volumes: &HashMap<String, ResolvedVolume>) -> Vec<Mount> {
    spec["volumeMounts"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| {
                    let vol_name = m["name"].as_str().unwrap_or("");
                    let resolved = volumes.get(vol_name)?;
                    let host_path = resolved.path.clone();
                    // Relabel kubelet-materialized volumes (configMap/secret/
                    // projected/emptyDir live under .../volumes/kubernetes.io~*)
                    // so the container can read them under enforcing SELinux;
                    // never relabel hostPath (arbitrary host system paths).
                    //
                    // Not a CSI volume: its filesystem is the driver's, which
                    // may not take labels at all (NFS), and relabelling a
                    // volume that is shared with other pods is not ours to do.
                    let selinux_relabel = host_path.contains("/volumes/kubernetes.io~")
                        && !host_path.contains("/volumes/kubernetes.io~csi/");
                    Some(Mount {
                        container_path: m["mountPath"].as_str().unwrap_or("").to_string(),
                        host_path,
                        readonly: m["readOnly"].as_bool().unwrap_or(false),
                        propagation: match m["mountPropagation"].as_str() {
                            // Only a privileged container may push mounts back
                            // to the node, as upstream rules. Anything else asking
                            // gets Private. That cannot hand a pod an empty volume:
                            // a CSI driver whose mounts do not reach the node is
                            // caught by the kubelet's mountinfo check, and the pod
                            // waits with the reason (`csi::is_mount_point`).
                            Some("Bidirectional")
                                if spec["securityContext"]["privileged"].as_bool()
                                    == Some(true) =>
                            {
                                MountPropagation::Bidirectional
                            }
                            Some("Bidirectional") => {
                                warn!(
                                    "container {}: mountPropagation Bidirectional on {vol_name} \
                                     needs a privileged container; mounted Private",
                                    spec["name"].as_str().unwrap_or("?")
                                );
                                MountPropagation::Private
                            }
                            Some("HostToContainer") => MountPropagation::HostToContainer,
                            _ => MountPropagation::Private,
                        },
                        selinux_relabel,
                        fstype: resolved.fstype.clone(),
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
/// conmon opens each container's log at `<sandbox log_directory>/<name>/<attempt>.log`
/// but does not create the `<name>` subdirectory — so container creation fails
/// with "conmon: Failed to open log file" unless the kubelet makes it first.
/// A per-pod directory the kubelet owns could not be made or written (#129).
///
/// The errno is the diagnosis: on server3 every one of these was ENOSPC, and
/// it reached `describe` as "does not exist on this node" because the error
/// was discarded and only the absence was checked afterwards. A wait, not a
/// failure: the pod is retried and starts once the node has room.
fn pod_dir_error(volume: &str, dir: &str, e: std::io::Error) -> CriError {
    CriError::VolumeNotReady(format!("{volume}: cannot write {dir}: {e}"))
}

/// The node is out of room for what it was asked to write: ENOSPC, EDQUOT,
/// or a read-only filesystem.
fn storage_refused(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOSPC) | Some(libc::EDQUOT) | Some(libc::EROFS)
    )
}

/// Create every container's log directory before the sandbox (#129).
///
/// A full node failed here as a generic start error after the sandbox and the
/// first container were made, so the pod went Failed. Out of space is a wait
/// ([`CriError::NodeStorage`]); anything else is left to the runtime, which
/// creates the directory again and names its own failure.
fn prepare_log_dirs(pod: &Value, log_directory: &str) -> Result<(), CriError> {
    for spec in ["initContainers", "containers"]
        .iter()
        .flat_map(|f| pod["spec"][*f].as_array().into_iter().flatten())
    {
        let dir = format!("{log_directory}/{}", spec["name"].as_str().unwrap_or("unnamed"));
        match std::fs::create_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if storage_refused(&e) => {
                return Err(CriError::NodeStorage(format!(
                    "cannot create the container log directory {dir}: {e}"
                )))
            }
            Err(e) => warn!("could not create container log dir {dir}: {e}"),
        }
    }
    Ok(())
}

fn ensure_container_log_dir(log_directory: &str, container_name: &str) {
    let dir = format!("{log_directory}/{container_name}");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        warn!("could not create container log dir {dir}: {e}");
    }
}

/// Expand Kubernetes `$(VAR)` references in a command/arg string against the
/// container's environment, mirroring upstream kubelet semantics:
///   - `$(NAME)` → the value of `NAME` if present, else the literal `$(NAME)`
///   - `$$`      → a literal `$`
///   - `$(`      with no closing `)` → left verbatim
///   - `$` followed by anything else → left verbatim (the `$` and next char)
fn expand_env_refs(input: &str, vars: &HashMap<&str, &str>) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' && i + 1 < chars.len() {
            let rest = &chars[i + 1..];
            match rest[0] {
                '$' => {
                    // Escaped operator: `$$` → `$`.
                    out.push('$');
                    i += 2;
                }
                '(' => {
                    if let Some(close) = rest.iter().position(|&c| c == ')') {
                        // rest[0] is '(', so any ')' is at index >= 1.
                        let name: String = rest[1..close].iter().collect();
                        match vars.get(name.as_str()) {
                            Some(v) => out.push_str(v),
                            None => {
                                // Unknown var: emit the reference verbatim.
                                out.push('$');
                                out.push('(');
                                out.push_str(&name);
                                out.push(')');
                            }
                        }
                        i += 1 + close + 1; // consume `$` `(` .. `)`
                    } else {
                        // Incomplete reference `$(...` with no closer.
                        out.push('$');
                        out.push('(');
                        i += 2;
                    }
                }
                other => {
                    // `$` not beginning an expression: emit both chars.
                    out.push('$');
                    out.push(other);
                    i += 2;
                }
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn build_container_config(
    spec: &Value,
    image_ref: &str,
    envs: Vec<(String, String)>,
    mounts: Vec<Mount>,
) -> ContainerConfig {
    let name = spec["name"].as_str().unwrap_or("unnamed").to_string();

    // Kubernetes expands `$(VAR)` references in command/args from the
    // container's resolved environment (`$$` escapes to a literal `$`;
    // unknown names are left verbatim). Required by many workloads —
    // e.g. cilium-operator passes `--debug=$(CILIUM_DEBUG)`.
    let env_map: HashMap<&str, &str> = envs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

    let command: Vec<String> = spec["command"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(|s| expand_env_refs(s, &env_map))
                .collect()
        })
        .unwrap_or_default();

    let args: Vec<String> = spec["args"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(|s| expand_env_refs(s, &env_map))
                .collect()
        })
        .unwrap_or_default();

    let sc = &spec["securityContext"];
    let add_capabilities: Vec<String> = sc["capabilities"]["add"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // securityContext.seLinuxOptions — pass the container's SELinux label to the
    // runtime (e.g. cilium's init containers request `type: spc_t`). A
    // container that sets none inherits the pod's; that fallback is applied by
    // `apply_pod_namespaces`, which is the only place holding the pod.
    let selinux_options = parse_selinux_options(sc);

    // cgroup semantics: cpu.shares from the CPU *request* (relative weight);
    // the cpu quota (hard cap) and memory limit from *limits* (0 = unlimited).
    let cpu_request = spec["resources"]["requests"]["cpu"].as_str().unwrap_or("0");
    let cpu_limit = spec["resources"]["limits"]["cpu"].as_str().unwrap_or("0");
    let mem_limit = spec["resources"]["limits"]["memory"]
        .as_str()
        .unwrap_or("0");

    // CRI log path, relative to the sandbox log_directory: <container>/<attempt>.log.
    // Enables `crictl logs` / `kubectl logs`.
    let log_path = format!("{name}/0.log");

    ContainerConfig {
        name,
        attempt: 0,
        image: image_ref.to_string(),
        command,
        args,
        working_dir: spec["workingDir"].as_str().unwrap_or("").to_string(),
        envs,
        mounts,
        labels: HashMap::new(),
        annotations: HashMap::new(),
        log_path,
        stdin: spec["stdin"].as_bool().unwrap_or(false),
        tty: spec["tty"].as_bool().unwrap_or(false),
        cpu_period: 100_000,
        cpu_quota: parse_cpu_quota(cpu_limit),
        cpu_shares: parse_cpu_shares(cpu_request),
        memory_limit_bytes: parse_memory_bytes(mem_limit),
        privileged: sc["privileged"].as_bool().unwrap_or(false),
        readonly_rootfs: sc["readOnlyRootFilesystem"].as_bool().unwrap_or(false),
        add_capabilities,
        selinux_options,
        host_network: false,
        host_pid: false,
        host_ipc: false,
        share_process_namespace: false,
        // Container-level seccompProfile; a pod-level one is filled in later by
        // apply_pod_namespaces when the container doesn't set its own.
        seccomp_profile: parse_seccomp(sc),
    }
}

/// Read a `securityContext.seccompProfile` (`{type, localhostProfile}`).
fn parse_seccomp(sc: &Value) -> Option<SeccompProfile> {
    let p = &sc["seccompProfile"];
    match p["type"].as_str()? {
        "Unconfined" => Some(SeccompProfile::Unconfined),
        "RuntimeDefault" => Some(SeccompProfile::RuntimeDefault),
        "Localhost" => Some(SeccompProfile::Localhost(
            p["localhostProfile"].as_str().unwrap_or("").to_string(),
        )),
        _ => None,
    }
}

/// securityContext.seLinuxOptions → the CRI SELinux label.
///
/// Absent means "no opinion", and the runtime picks the default type
/// (`container_t`). That is the right default and the wrong answer for a
/// container that asked to be super-privileged: Cilium's init containers set
/// `type: spc_t` precisely because they write host paths, and under enforcing
/// SELinux `container_t` is denied those writes (rustkube-node#26).
fn parse_selinux_options(security_context: &Value) -> Option<SeLinuxOptions> {
    let se = &security_context["seLinuxOptions"];
    if !se.is_object() {
        return None;
    }
    let field = |k: &str| se[k].as_str().unwrap_or("").to_string();
    Some(SeLinuxOptions {
        user: field("user"),
        role: field("role"),
        type_: field("type"),
        level: field("level"),
    })
}

/// Copy the pod-level namespace-sharing flags onto a container config. The
/// container's CRI namespace_options must be consistent with the sandbox's
/// (both derive from the pod), or the runtime refuses to start the container —
/// e.g. a hostPID pod's sandbox is pid=NODE, so its containers must be too.
fn apply_pod_namespaces(config: &mut ContainerConfig, pod: &Value) {
    let spec = &pod["spec"];
    config.host_network = spec["hostNetwork"].as_bool().unwrap_or(false);
    config.host_pid = spec["hostPID"].as_bool().unwrap_or(false);
    config.host_ipc = spec["hostIPC"].as_bool().unwrap_or(false);
    config.share_process_namespace = spec["shareProcessNamespace"].as_bool().unwrap_or(false);
    // A container without its own seccompProfile inherits the pod's.
    if config.seccomp_profile.is_none() {
        config.seccomp_profile = parse_seccomp(&spec["securityContext"]);
    }
    // Likewise seLinuxOptions: pod-level applies to every container, and a
    // container that sets its own overrides it (rustkube-node#26).
    if config.selinux_options.is_none() {
        config.selinux_options = parse_selinux_options(&spec["securityContext"]);
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
pub(crate) mod tests {
    use super::*;
    use crate::cri::{
        ContainerStatusInfo, ExecSyncResult, ImageInfo, PodSandboxState, PodSandboxStatusInfo,
        PodSandboxSummary,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
    pub(crate) struct FakeRuntime {
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
        fail_stop: AtomicBool,
        /// stop_pod_sandbox fails while set (#137).
        fail_sandbox_stop: AtomicBool,
        stopped_sandboxes: Mutex<Vec<String>>,
        slow_image: Mutex<Option<Arc<tokio::sync::Notify>>>,
        image_pulls: AtomicU32,
        /// run_pod_sandbox's network (#148): 0 ready, 1 no CNI config, 2 ADD fails.
        network: AtomicU32,
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
            self.exit_on_start
                .lock()
                .unwrap()
                .insert(name.to_string(), code);
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
            match self.network.load(Ordering::SeqCst) {
                1 => return Err(CriError::NetworkNotConfigured("no CNI network configured yet".into())),
                2 => return Err(CriError::NetworkNotReady("CNI ADD failed: agent not serving".into())),
                _ => {}
            }
            let id = format!(
                "sb-{}-{}",
                config.uid,
                self.next_id.fetch_add(1, Ordering::SeqCst)
            );
            self.sandboxes
                .lock()
                .unwrap()
                .insert(id.clone(), (PodSandboxState::Ready, config.clone()));
            Ok(id)
        }

        async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            if self.fail_sandbox_stop.load(Ordering::SeqCst) {
                return Err(CriError::NetworkNotReady("CNI DEL: injected".into()));
            }
            self.stopped_sandboxes.lock().unwrap().push(sandbox_id.to_string());
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
                netns_path: None,
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
            let id = format!(
                "c-{}-{}",
                config.name,
                self.next_id.fetch_add(1, Ordering::SeqCst)
            );
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
            if self.fail_stop.load(Ordering::SeqCst) {
                return Err(CriError::Connection("injected outage".into()));
            }
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
            self.image_pulls.fetch_add(1,Ordering::SeqCst);
            let gate=if image=="slow" {self.slow_image.lock().unwrap().clone()} else {None};
            if let Some(gate)=gate {gate.notified().await;}
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

    /// A pod state holding one claim, in the given phase.
    fn state_holding(uid: &str, name: &str, claim: &str, phase: &str) -> PodState {
        PodState {
            namespace: "default".to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
            sandbox_id: None,
            container_ids: HashMap::new(),
            phase: phase.to_string(),
            pod: json!({
                "metadata": {"name": name, "namespace": "default", "uid": uid},
                "spec": {"volumes": [{"name": "data",
                    "persistentVolumeClaim": {"claimName": claim}}]}
            }),
            pod_ip: None,
            restart_counts: HashMap::new(),
            ready: HashMap::new(),
            liveness_failures: HashMap::new(),
            startup_passed: HashMap::new(),
            started: HashMap::new(),
            terminated: HashMap::new(),
            init_statuses: Vec::new(),
            sandbox_stopped: false,
        }
    }

    #[tokio::test]
    async fn rwop_holder_is_found_only_among_live_pods_elsewhere() {
        let (_rt, mgr) = manager();
        {
            let mut pods = mgr.pods.write().await;
            pods.insert(
                "uid-1".into(),
                state_holding("uid-1", "db", "data", "Running"),
            );
        }

        // A second pod on this node wanting the same claim finds the holder —
        // this is the case the scheduler cannot catch, because a static pod or
        // one written straight onto spec.nodeName never passes a filter.
        assert_eq!(
            mgr.other_pod_holding("default", "data", "uid-2")
                .await
                .as_deref(),
            Some("db")
        );

        // The holder does not find itself: a restart re-resolves its own
        // volumes and must not refuse to come back up.
        assert!(mgr
            .other_pod_holding("default", "data", "uid-1")
            .await
            .is_none());

        // A different claim, and the same claim in another namespace, are
        // different volumes.
        assert!(mgr
            .other_pod_holding("default", "other", "uid-2")
            .await
            .is_none());
        assert!(mgr
            .other_pod_holding("prod", "data", "uid-2")
            .await
            .is_none());

        // A terminal pod holds nothing — its containers are gone, so nothing
        // of it is writing and the claim is free.
        {
            let mut pods = mgr.pods.write().await;
            pods.get_mut("uid-1").unwrap().phase = "Succeeded".into();
        }
        assert!(mgr
            .other_pod_holding("default", "data", "uid-2")
            .await
            .is_none());
    }

    /// A stand-in for stormblock's management API on loopback: it holds at
    /// most one volume and records what was done to it.
    struct FakeStormblock {
        detached: Arc<AtomicBool>,
        deleted: Arc<AtomicBool>,
        url: String,
    }

    /// Serve the three calls a release makes: list the volumes, detach one,
    /// delete one. A deleted volume drops out of the listing, so a second
    /// release of the same claim sees what a real one would.
    async fn fake_stormblock(volume: Option<&str>) -> FakeStormblock {
        use axum::routing::{delete as http_delete, get as http_get};

        let detached = Arc::new(AtomicBool::new(false));
        let deleted = Arc::new(AtomicBool::new(false));
        let name = volume.map(String::from);

        let (for_list, for_detach, for_delete) =
            (deleted.clone(), detached.clone(), deleted.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/volumes",
                http_get(move || {
                    let (name, deleted) = (name.clone(), for_list.clone());
                    async move {
                        let items = match (&name, deleted.load(Ordering::SeqCst)) {
                            (Some(n), false) => json!([{"id": "vol-1", "name": n}]),
                            _ => json!([]),
                        };
                        axum::Json(json!({ "items": items }))
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{id}/attach",
                http_delete(move || {
                    let d = for_detach.clone();
                    async move {
                        d.store(true, Ordering::SeqCst);
                        axum::http::StatusCode::NO_CONTENT
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{id}",
                http_delete(move || {
                    let d = for_delete.clone();
                    async move {
                        d.store(true, Ordering::SeqCst);
                        axum::http::StatusCode::NO_CONTENT
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeStormblock {
            detached,
            deleted,
            url: format!("http://{addr}"),
        }
    }

    #[tokio::test]
    async fn a_pod_waiting_on_a_volume_is_admitted_not_missing() {
        // #63: a pod whose volume is not ready was reported Pending and then
        // forgotten, so `logs` said "not found on this node" and it had no
        // container statuses.
        let (rt, mgr) = manager();
        let mut p = pod("uid-1", "probe", "Always", simple_container());
        p["spec"]["volumes"] = json!([{"name": "share", "nfs": {"server": "x", "path": "/"}}]);

        let outcome = mgr.sync_pods(&[p.clone()]).await;
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Pending");
        assert!(
            u.message.starts_with("Unable to attach or mount volumes"),
            "{}",
            u.message
        );
        assert_eq!(u.container_statuses.len(), 1);
        let c = &u.container_statuses[0];
        assert_eq!((c.name.as_str(), c.state.as_str()), ("app", "waiting"));
        assert_eq!(c.reason, "ContainerCreating");
        assert!(
            c.message.contains("volume share is of type nfs"),
            "{}",
            c.message
        );
        assert!(c.container_id.is_empty());
        assert!(rt.created_names().is_empty());

        // The node knows it, and says why.
        let why = mgr
            .waiting_reason("default", "probe")
            .expect("recorded as waiting");
        assert!(why.contains("nfs"));
        let listed = mgr.pods_json().await;
        assert_eq!(listed["items"][0]["metadata"]["uid"], "uid-1");
        assert_eq!(listed["items"][0]["status"]["phase"], "Pending");

        // Still waiting on the next pass, and gone once the pod is.
        mgr.sync_pods(&[p]).await;
        assert!(mgr.waiting_reason("default", "probe").is_some());
        mgr.sync_pods(&[]).await;
        assert!(mgr.waiting_reason("default", "probe").is_none());
        assert_eq!(mgr.pods_json().await["items"].as_array().unwrap().len(), 0);
    }

    /// A state root the kubelet cannot write under: a regular file, so every
    /// `create_dir_all` beneath it fails (ENOTDIR), as ENOSPC did on server3.
    fn unwritable_root(mgr: &mut PodManager, tag: &str) {
        let f = std::env::temp_dir().join(format!("rk-kubelet-full-{}-{tag}", std::process::id()));
        std::fs::write(&f, b"not a directory").unwrap();
        mgr.state_root = f.to_string_lossy().into_owned();
    }

    #[tokio::test]
    async fn a_pod_dir_that_cannot_be_written_waits_with_the_errno() {
        // #129: the emptyDir's create_dir_all failed (ENOSPC) and was
        // discarded, the check then said "does not exist on this node", and
        // the pod went Failed. It waits, says why, and has nothing to undo.
        let (rt, mut mgr) = manager();
        unwritable_root(&mut mgr, "emptydir");
        let mut p = pod("uid-full", "agent", "Always", simple_container());
        p["spec"]["volumes"] = json!([{"name": "tmp", "emptyDir": {}}]);

        let outcome = mgr.sync_pods(&[p.clone()]).await;
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Pending", "{}", u.message);
        let c = &u.container_statuses[0];
        assert_eq!(c.reason, "ContainerCreating");
        assert!(c.message.contains("tmp: cannot write"), "{}", c.message);
        assert!(c.message.contains("kubernetes.io~empty-dir/tmp"), "{}", c.message);
        assert!(!c.message.contains("does not exist"), "{}", c.message);
        assert!(rt.sandboxes.lock().unwrap().is_empty());
        assert!(rt.created_names().is_empty());

        // Space back: the next pass starts it.
        let (_, ok) = manager();
        mgr.state_root = ok.state_root.clone();
        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].phase, "Running", "{}", outcome.updates[0].message);
        assert_eq!(rt.created_names(), vec!["app".to_string()]);
    }

    #[tokio::test]
    async fn an_unwritable_service_account_token_waits_before_the_sandbox() {
        // No volumes of its own: the default kube-api-access dir is the one
        // that cannot be written. Before #129 it was written after the
        // sandbox, errors ignored, and the container bound a missing path.
        let (rt, mut mgr) = manager();
        unwritable_root(&mut mgr, "sa");
        let p = pod("uid-sa", "web", "Always", simple_container());
        let outcome = mgr.sync_pods(&[p]).await;
        let u = &outcome.updates[0];
        assert_eq!(u.phase, "Pending", "{}", u.message);
        assert!(
            u.container_statuses[0].message.contains("kube-api-access: cannot write"),
            "{}",
            u.container_statuses[0].message
        );
        assert!(rt.sandboxes.lock().unwrap().is_empty());
    }

    #[test]
    fn a_full_disk_is_a_storage_wait_and_log_dirs_are_made_per_container() {
        for errno in [libc::ENOSPC, libc::EDQUOT, libc::EROFS] {
            assert!(storage_refused(&std::io::Error::from_raw_os_error(errno)));
        }
        assert!(!storage_refused(&std::io::Error::from_raw_os_error(libc::EACCES)));

        let root = tempfile::tempdir().unwrap();
        let logs = root.path().join("default_web_u").to_string_lossy().into_owned();
        let p = json!({"spec": {
            "initContainers": [{"name": "init"}],
            "containers": [{"name": "app"}, {"name": "side"}]
        }});
        prepare_log_dirs(&p, &logs).unwrap();
        for c in ["init", "app", "side"] {
            assert!(std::path::Path::new(&logs).join(c).is_dir(), "{c}");
        }
    }

    #[test]
    fn a_long_volume_wait_says_it_timed_out_and_stays_stable() {
        let short = volume_wait_message(
            "v: template t awaiting_format",
            std::time::Duration::from_secs(10),
        );
        assert!(!short.contains("timed out"));
        let long = volume_wait_message("v: template t awaiting_format", VOLUME_WAIT_TIMEOUT);
        assert!(long.contains("timed out after 5m"), "{long}");
        assert!(long.contains("still retrying"));
        // The same text later, so the FailedMount Event aggregates.
        assert_eq!(
            long,
            volume_wait_message("v: template t awaiting_format", VOLUME_WAIT_TIMEOUT * 3)
        );
    }

    /// A stormblock whose `POST /api/v1/fstemplates` takes `format` to answer,
    /// like a 1 TiB blank being formatted, and whose template reads `state`
    /// once minted (none before). Counts the POSTs.
    async fn slow_minting_stormblock(
        format: std::time::Duration,
        state: &'static str,
    ) -> (String, Arc<AtomicU32>, Arc<tokio::sync::Notify>) {
        use axum::routing::{get as http_get, post as http_post};
        let posts = Arc::new(AtomicU32::new(0));
        let made = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = started.clone();
        let (p, m, m2) = (posts.clone(), made.clone(), made.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/fstemplates",
                http_post(move || {
                    let (p, m) = (p.clone(), m.clone());
                    let signal = signal.clone();
                    async move {
                        p.fetch_add(1, Ordering::SeqCst);
                        // Persisted before the format, as stormblock does.
                        m.store(true, Ordering::SeqCst);
                        signal.notify_one();
                        tokio::time::sleep(format).await;
                        (
                            axum::http::StatusCode::CREATED,
                            axum::Json(json!({"template": {}})),
                        )
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates/{name}",
                http_get(move || {
                    let m = m2.clone();
                    async move {
                        if m.load(Ordering::SeqCst) {
                            Ok(axum::Json(json!({"id": "tpl-1", "state": state})))
                        } else {
                            Err(axum::http::StatusCode::NOT_FOUND)
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), posts, started)
    }

    fn with_stormblock(mgr: PodManager, url: &str) -> PodManager {
        mgr.with_engine(crate::engine::EngineClient::new(
            url,
            crate::engine::TokenSource::none(),
        ))
    }

    #[tokio::test]
    async fn a_slow_mint_does_not_hold_the_sync() {
        // #63: a 1 TiB blank's format held the whole sync loop.
        let (url, posts, started) =
            slow_minting_stormblock(std::time::Duration::from_secs(30), "awaiting_format").await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);

        let t = Instant::now();
        let e = mgr
            .mint_template("pvc-ext4j-1048576m", "1T")
            .await
            .unwrap_err();
        assert!(
            t.elapsed() < std::time::Duration::from_secs(1),
            "waited {:?}",
            t.elapsed()
        );
        assert!(e.contains("pvc-ext4j-1048576m"));
        tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
            .await
            .unwrap();

        // The next claim waits on the same mint rather than starting another.
        let e = mgr
            .mint_template("pvc-ext4j-1048576m", "1T")
            .await
            .unwrap_err();
        assert_eq!(e, "template pvc-ext4j-1048576m awaiting_format");
        assert_eq!(posts.load(Ordering::SeqCst), 1);
    }

    /// A stormblock whose `pvc-ext4j-64m` template is `ready` until deleted
    /// and whose clone answers `refusal` (#140). Counts DELETEs and mints.
    async fn refusing_stormblock(
        refusal: (u16, &'static str),
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>, Arc<std::sync::atomic::AtomicUsize>) {
        use axum::routing::{get as http_get, post as http_post};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let deleted = Arc::new(AtomicBool::new(false));
        let deletes = Arc::new(AtomicUsize::new(0));
        let mints = Arc::new(AtomicUsize::new(0));
        let (d1, d2, dels, ms) = (deleted.clone(), deleted.clone(), deletes.clone(), mints.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/fstemplates/{name}",
                http_get(move || {
                    let d = d1.clone();
                    async move {
                        if d.load(Ordering::SeqCst) {
                            Err(axum::http::StatusCode::NOT_FOUND)
                        } else {
                            Ok(axum::Json(json!({"id": "tpl-1", "state": "ready"})))
                        }
                    }
                })
                .delete(move || {
                    let (d, n) = (d2.clone(), dels.clone());
                    async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        d.store(true, Ordering::SeqCst);
                        axum::Json(json!({"deleted": "tpl-1"}))
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates/{id}/clone",
                http_post(move || async move {
                    let (code, error) = refusal;
                    (
                        axum::http::StatusCode::from_u16(code).unwrap(),
                        axum::Json(json!({"error": error, "code": code})),
                    )
                }),
            )
            .route(
                "/api/v1/fstemplates",
                http_post(move || {
                    let n = ms.clone();
                    async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        (axum::http::StatusCode::CREATED, axum::Json(json!({})))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), deletes, mints)
    }

    #[tokio::test]
    async fn a_template_whose_volume_is_gone_is_minted_again() {
        // #140: a `ready` template whose sealed volume the engine no longer
        // has refuses every clone the same way; retrying the clone looped
        // forever on "would not clone".
        use std::sync::atomic::Ordering;
        let (url, deletes, mints) =
            refusing_stormblock((404, "volume 5c1e0b7a-0000-4000-8000-000000000001 not found")).await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let e = mgr.clone_blank("64M", "pvc-ns-d").await.unwrap_err().to_string();
        assert!(e.contains("template pvc-ext4j-64m is broken"), "{e}");
        assert!(e.contains("404"), "{e}");
        assert!(e.contains("volume 5c1e0b7a-0000-4000-8000-000000000001 not found"), "{e}");
        assert_eq!(deletes.load(Ordering::SeqCst), 1);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while mints.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the blank is minted again");
    }

    #[tokio::test]
    async fn a_refused_clone_names_stormblocks_reason_and_keeps_the_template() {
        use std::sync::atomic::Ordering;
        let (url, deletes, mints) =
            refusing_stormblock((500, "cloning volume: no free slots in the data half")).await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let e = mgr.clone_blank("64M", "pvc-ns-d").await.unwrap_err().to_string();
        assert!(e.starts_with("stormblock would not clone pvc-ext4j-64m to pvc-ns-d: "), "{e}");
        assert!(e.contains("-> 500: cloning volume: no free slots in the data half"), "{e}");
        assert_eq!(deletes.load(Ordering::SeqCst), 0);
        assert_eq!(mints.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn no_answer_from_stormblock_is_named_not_silent() {
        // Before #140 a transport error answered `None` with no log line.
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, "http://127.0.0.1:1");
        let e = mgr
            .storage_post("/api/v1/fstemplates/t/clone", &json!({}))
            .await
            .unwrap_err();
        assert_eq!(e.status, None);
        assert!(e.to_string().starts_with("stormblock POST /api/v1/fstemplates/t/clone: no answer"), "{e}");
    }

    #[test]
    fn which_clone_refusals_mean_a_broken_template() {
        let r = |code, error: &str| {
            EngineRefusal::refused("/p", code, &json!({"error": error, "code": code}).to_string())
        };
        assert!(r(404, "volume 1 not found").template_broken());
        assert!(r(409, "volume 1 is not sealed — seal it before cloning").template_broken());
        assert!(r(500, "fstemplate pvc-ext4j-64m has no sealed snapshot").template_broken());
        // The template itself gone: the next pass mints it, nothing to delete.
        assert!(!r(404, "fstemplate pvc-ext4j-64m not found").template_broken());
        // Still being made.
        assert!(!r(409, "fstemplate pvc-ext4j-64m is awaiting_format — seal it before cloning")
            .template_broken());
        assert!(!r(500, "cloning volume: out of space").template_broken());
        assert!(!r(401, "auth: required").template_broken());
        assert_eq!(r(401, "auth: required").to_string(), "stormblock POST /p -> 401: auth: required");
    }

    #[tokio::test]
    async fn mint_completion_notifies_without_an_api_edit_or_sync_tick() {
        let (url, posts, _) =
            slow_minting_stormblock(std::time::Duration::from_millis(20), "ready").await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let mut changes = mgr.subscribe_volume_changes();
        let _ = mgr.mint_template("pvc-ext4j-1m", "1M").await;
        tokio::time::timeout(std::time::Duration::from_secs(1), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mgr.storage_template_state("pvc-ext4j-1m").await,
            Some(("tpl-1".into(), "ready".into()))
        );
        assert_eq!(posts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn releasing_a_claim_detaches_then_deletes_the_clone() {
        let sb = fake_stormblock(Some("pvc-default-data")).await;
        let (_rt, mut mgr) = manager();
        mgr.storage_url = sb.url.clone();

        assert_eq!(
            mgr.release_claim_volume("default", "data").await.unwrap(),
            VolumeRelease::Released
        );
        // Detach first: the attach outlives the pod, and stormblock refuses to
        // delete a volume it is still serving.
        assert!(sb.detached.load(Ordering::SeqCst));
        assert!(sb.deleted.load(Ordering::SeqCst));

        // Releasing again is success, not a 404: a controller retrying a
        // delete it already completed must be able to settle.
        assert_eq!(
            mgr.release_claim_volume("default", "data").await.unwrap(),
            VolumeRelease::Absent
        );
    }

    #[tokio::test]
    async fn a_claim_a_pod_still_holds_is_refused_not_deleted() {
        let sb = fake_stormblock(Some("pvc-default-data")).await;
        let (_rt, mut mgr) = manager();
        mgr.storage_url = sb.url.clone();
        {
            let mut pods = mgr.pods.write().await;
            pods.insert(
                "uid-1".into(),
                state_holding("uid-1", "db", "data", "Running"),
            );
        }

        assert_eq!(
            mgr.release_claim_volume("default", "data").await.unwrap(),
            VolumeRelease::InUse("db".into())
        );
        // Refused, not queued, and above all nothing was touched: a delete
        // that races a running pod pulls a filesystem away mid-write.
        assert!(!sb.detached.load(Ordering::SeqCst));
        assert!(!sb.deleted.load(Ordering::SeqCst));
    }

    /// One loopback server standing in for both the apiserver and stormblock
    /// (#67): it serves the claim, records every request, keeps the volumes
    /// created, and attaches anything at `/dev/ublkb7`.
    struct ClaimWorld {
        url: String,
        calls: Arc<Mutex<Vec<(String, String, Value)>>>,
    }

    async fn claim_world(pvc: Value) -> ClaimWorld {
        let calls: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
        let volumes: Arc<Mutex<Vec<Value>>> = Arc::default();
        let log = calls.clone();
        let app = axum::Router::new().fallback(
            move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let (log, volumes, pvc) = (log.clone(), volumes.clone(), pvc.clone());
                async move {
                    use axum::http::StatusCode;
                    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let path = uri.path().to_string();
                    log.lock().unwrap().push((method.to_string(), path.clone(), body.clone()));
                    let ok = |v: Value| (StatusCode::OK, axum::Json(v));
                    match (method.as_str(), path.as_str()) {
                        ("GET", p) if p.contains("/persistentvolumeclaims/") => ok(pvc),
                        ("PUT", p) if p.contains("/persistentvolumeclaims/") => ok(body),
                        ("POST", "/api/v1/persistentvolumes") => (StatusCode::CREATED, axum::Json(body)),
                        ("GET", "/api/v1/volumes") => {
                            ok(json!({ "items": volumes.lock().unwrap().clone() }))
                        }
                        ("POST", "/api/v1/volumes") => {
                            let v = json!({ "id": "vol-raw", "name": body["name"] });
                            volumes.lock().unwrap().push(v.clone());
                            (StatusCode::CREATED, axum::Json(v))
                        }
                        ("POST", p) if p.ends_with("/attach") => {
                            ok(json!({ "device_hint": "/dev/ublkb7" }))
                        }
                        _ => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                    }
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        ClaimWorld { url, calls }
    }

    fn claim_manager(w: &ClaimWorld) -> PodManager {
        let rt = Arc::new(FakeRuntime::default());
        PodManager::with_api(rt.clone(), rt, NODE, &w.url, "127.0.0.1", reqwest::Client::new())
            .with_engine(crate::engine::EngineClient::new(&w.url, crate::engine::TokenSource::none()))
    }

    fn raw_claim(size: &str, mode: &str) -> Value {
        json!({
            "metadata": { "name": "raw", "namespace": "default", "uid": "c-1" },
            "spec": {
                "storageClassName": "stormblock",
                "volumeMode": mode,
                "accessModes": ["ReadWriteOnce"],
                "resources": { "requests": { "storage": size } },
            }
        })
    }

    #[tokio::test]
    async fn a_block_claim_is_a_raw_volume_up_to_a_pebibyte() {
        let w = claim_world(raw_claim("600Ti", "Block")).await;
        let mgr = claim_manager(&w);
        let (dev, fs) = mgr.provision_claim("default", "raw", "uid-1").await.unwrap();
        assert_eq!((dev.as_str(), fs), ("/dev/ublkb7", None));

        let calls = w.calls.lock().unwrap().clone();
        // A plain volume of the 1P class, said in MiB; no template, no mkfs.
        let made = calls.iter().find(|(m, p, _)| m == "POST" && p == "/api/v1/volumes").unwrap();
        assert_eq!(made.2["name"], "pvc-default-raw");
        assert_eq!(made.2["size"], "1073741824M");
        assert_eq!(made.2["role"], "data");
        assert!(!calls.iter().any(|(_, p, _)| p.contains("fstemplates")), "{calls:?}");
        // Its PV says Block, with the class's capacity.
        let pv = calls.iter().find(|(m, p, _)| m == "POST" && p == "/api/v1/persistentvolumes").unwrap();
        assert_eq!(pv.2["spec"]["volumeMode"], "Block");
        assert_eq!(pv.2["spec"]["capacity"]["storage"], "1Pi");
    }

    #[tokio::test]
    async fn a_filesystem_claim_past_the_formatted_classes_waits_and_says_use_block() {
        let w = claim_world(raw_claim("20Ti", "Filesystem")).await;
        let mgr = claim_manager(&w);
        match mgr.provision_claim("default", "raw", "uid-1").await {
            Err(ClaimError::Failed(why)) => {
                assert!(why.contains("64T class") && why.contains("volumeMode: Block"), "{why}")
            }
            other => panic!("{other:?}"),
        }
        let calls = w.calls.lock().unwrap().clone();
        assert!(!calls.iter().any(|(m, _, _)| m == "POST"), "nothing made: {calls:?}");
    }

    #[test]
    fn a_volumes_mode_must_match_how_the_pod_uses_it() {
        let pod = json!({"spec": {
            "initContainers": [{"name": "init", "volumeMounts": [{"name": "fs", "mountPath": "/d"}]}],
            "containers": [{"name": "db", "volumeDevices": [{"name": "raw", "devicePath": "/dev/xvda"}]}],
        }});
        assert!(volume_mode_misuse(&pod, "raw", true).is_none());
        assert!(volume_mode_misuse(&pod, "fs", false).is_none());
        let why = volume_mode_misuse(&pod, "raw", false).unwrap();
        assert!(why.contains("container db names it in volumeDevices; use volumeMounts"), "{why}");
        let why = volume_mode_misuse(&pod, "fs", true).unwrap();
        assert!(why.contains("Block") && why.contains("container init") && why.contains("use volumeDevices"), "{why}");
    }

    #[test]
    fn a_block_volume_is_bound_at_its_device_path_with_no_filesystem() {
        let mut vols = HashMap::new();
        vols.insert("raw".to_string(), ResolvedVolume { path: "/dev/ublkb7".into(), fstype: None, block: true });
        vols.insert("fs".to_string(), ResolvedVolume {
            path: "/dev/ublkb8".into(), fstype: Some("ext4".into()), block: false,
        });
        let spec = json!({
            "volumeMounts": [{"name": "fs", "mountPath": "/data"}],
            "volumeDevices": [{"name": "raw", "devicePath": "/dev/xvda"}, {"name": "fs", "devicePath": "/dev/no"}],
        });
        let m = resolve_mounts(&spec, &vols);
        assert_eq!(m.len(), 2, "a filesystem volume is never bound as a device");
        assert_eq!((m[0].container_path.as_str(), m[0].fstype.as_deref()), ("/data", Some("ext4")));
        assert_eq!((m[1].container_path.as_str(), m[1].host_path.as_str()), ("/dev/xvda", "/dev/ublkb7"));
        assert!(m[1].fstype.is_none() && !m[1].readonly && !m[1].selinux_relabel);
    }

    #[tokio::test]
    async fn claims_reach_the_engine_with_its_token_not_the_apiservers() {
        // The engine refuses anything but its own token (#66, stormblock#107).
        let app = axum::Router::new().route(
            "/api/v1/volumes",
            axum::routing::get(|h: axum::http::HeaderMap| async move {
                match h.get("authorization").and_then(|v| v.to_str().ok()) {
                    Some("Bearer engine-tok") => {
                        (axum::http::StatusCode::OK, axum::Json(json!({"items": []})))
                    }
                    _ => (axum::http::StatusCode::UNAUTHORIZED, axum::Json(json!({}))),
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("api_token");
        let (_rt, mgr) = manager();
        let mgr = mgr.with_engine(crate::engine::EngineClient::new(
            &url,
            crate::engine::TokenSource::files([&file]),
        ));
        // Not minted yet: refused, and said so rather than read as "no volume".
        assert!(mgr
            .storage_volume_checked("pvc-default-data")
            .await
            .is_err());
        std::fs::write(&file, "engine-tok\n").unwrap();
        assert_eq!(
            mgr.storage_volume_checked("pvc-default-data")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn an_unreachable_engine_is_an_error_not_a_silent_success() {
        // Nothing listening: reporting Absent here would tell the controller
        // the clone is gone when the node could not even ask, and the PV
        // would be deleted over a volume that is still allocated.
        let (_rt, mut mgr) = manager();
        mgr.storage_url = "http://127.0.0.1:1".to_string();
        assert!(mgr.release_claim_volume("default", "data").await.is_err());
    }

    #[test]
    fn pod_claims_lists_only_pvc_volumes() {
        let pod = json!({"spec": {"volumes": [
            {"name": "cfg", "configMap": {"name": "c"}},
            {"name": "data", "persistentVolumeClaim": {"claimName": "pgdata"}},
            {"name": "tmp", "emptyDir": {}},
            {"name": "more", "persistentVolumeClaim": {"claimName": "wal"}}
        ]}});
        assert_eq!(pod_claims(&pod).collect::<Vec<_>>(), vec!["pgdata", "wal"]);
        // A pod with no volumes at all is not a special case.
        assert_eq!(pod_claims(&json!({"spec": {}})).count(), 0);
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

    async fn prepared(mgr:&PodManager,p:&Value) {
        let mut changed=mgr.subscribe_image_changes();
        loop {
            match mgr.prepare_images(p) {
                Ok(())=>return,
                Err(CriError::Pending(_))=>{tokio::time::timeout(std::time::Duration::from_secs(1),changed.recv()).await.unwrap().unwrap();},
                Err(error)=>panic!("{error}"),
            }
        }
    }

    #[tokio::test]
    async fn slow_image_is_shared_and_does_not_hold_an_unrelated_start() {
        let (rt,mgr)=manager();let e=crate::workload::Executor::new();
        let mgr=mgr.with_admission(e.reservations.clone());
        let gate=Arc::new(tokio::sync::Notify::new());
        *rt.slow_image.lock().unwrap()=Some(gate.clone());
        let slow=pod("slow","slow","Always",json!({"name":"app","image":"slow"}));
        let mut other=slow.clone();other["metadata"]["uid"]=json!("other");
        assert!(matches!(mgr.start_pod(&slow).await,Err(CriError::Pending(_))));
        assert!(matches!(mgr.start_pod(&other).await,Err(CriError::Pending(_))));
        let fast=pod("fast","fast","Always",json!({"name":"app","image":"fast"}));
        prepared(&mgr,&fast).await;
        assert_eq!(mgr.start_pod(&fast).await.unwrap().phase,"Running");
        assert_eq!(rt.image_pulls.load(Ordering::SeqCst),2,"one slow pull shared, one fast");
        let mut changed=mgr.subscribe_image_changes();
        gate.notify_one();changed.recv().await.unwrap();
        assert_eq!(mgr.start_pod(&slow).await.unwrap().phase,"Running");
    }

    /// #134: an image resolved off the worker in well under the grace is
    /// waited for, so the first attempt starts the pod (it took two).
    #[tokio::test]
    async fn a_local_image_starts_the_pod_on_the_first_attempt() {
        let (rt,mgr)=manager();let e=crate::workload::Executor::new();
        let mgr=mgr.with_admission(e.reservations.clone());
        let p=pod("first","first","Always",json!({"name":"app","image":"fast"}));
        assert_eq!(mgr.start_pod(&p).await.unwrap().phase,"Running");
        assert_eq!(rt.image_pulls.load(Ordering::SeqCst),1);
        let t=mgr.timings.lock().unwrap()["first"].clone();
        let text=t.finish(std::time::Duration::ZERO,Instant::now()).text;
        assert!(text.contains("attempts=1"),"{text}");
    }

    #[tokio::test]
    async fn staged_init_yields_reuses_sandbox_and_deletion_cleans_partial_start() {
        let (rt,mgr)=manager();
        let executor=crate::workload::Executor::new();
        let mgr=mgr.with_admission(executor.reservations.clone());
        let mut p=pod("staged","staged","Always",json!({"name":"app","image":"test"}));
        p["spec"]["initContainers"]=json!([{"name":"init","image":"test"}]);
        prepared(&mgr,&p).await;
        let result=tokio::time::timeout(std::time::Duration::from_millis(200),mgr.start_pod(&p)).await.unwrap();
        assert!(matches!(result,Err(CriError::Pending(_))));
        assert_eq!(rt.sandboxes.lock().unwrap().len(),1);
        assert_eq!(rt.containers.lock().unwrap().len(),1);
        let first=mgr.pods.read().await["staged"].clone();
        let result=mgr.start_pod(&p).await;
        assert!(matches!(result,Err(CriError::Pending(_))));
        assert_eq!(mgr.pods.read().await["staged"].sandbox_id,first.sandbox_id);
        assert_eq!(rt.containers.lock().unwrap().len(),1);
        mgr.stop_pod("staged").await.unwrap();
        assert!(rt.sandboxes.lock().unwrap().is_empty());
        assert!(rt.containers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn staged_init_completion_advances_to_app_without_restarting_init() {
        let (rt,mgr)=manager();
        let executor=crate::workload::Executor::new();
        let mgr=mgr.with_admission(executor.reservations.clone());
        let mut p=pod("staged-done","staged-done","Always",json!({"name":"app","image":"test"}));
        p["spec"]["initContainers"]=json!([{"name":"init","image":"test"}]);
        prepared(&mgr,&p).await;
        assert!(matches!(mgr.start_pod(&p).await,Err(CriError::Pending(_))));
        let cid=mgr.pods.read().await["staged-done"].container_ids["init"].clone();
        rt.set_container_state(&cid,ContainerState::Exited,0);
        let result=mgr.start_pod(&p).await.unwrap();
        assert_eq!(result.phase,"Running");
        assert_eq!(result.init_container_statuses.len(),1);
        assert_eq!(rt.sandboxes.lock().unwrap().len(),1);
        assert_eq!(mgr.pods.read().await["staged-done"].container_ids.len(),1);
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
        let v = mgr.resolve_volumes(&p).await.unwrap();
        let dir = &v
            .get("kube-api-access")
            .expect("projected volume resolved")
            .path;
        assert!(dir.contains("kubernetes.io~projected"));
        // The downward file was written with the namespace.
        let content = std::fs::read_to_string(format!("{dir}/namespace")).unwrap_or_default();
        assert_eq!(content, "default");
    }

    #[test]
    fn pull_policy_defaults_and_explicit() {
        // Explicit wins.
        assert_eq!(
            effective_pull_policy(&json!({"imagePullPolicy": "Never"}), "x:1"),
            "Never"
        );
        // Default: :latest / untagged → Always; pinned tag → IfNotPresent.
        assert_eq!(
            effective_pull_policy(&json!({}), "busybox:latest"),
            "Always"
        );
        assert_eq!(effective_pull_policy(&json!({}), "busybox"), "Always");
        assert_eq!(
            effective_pull_policy(&json!({}), "busybox:1.36"),
            "IfNotPresent"
        );
        // A port in the registry host must not be mistaken for a tag.
        assert_eq!(
            effective_pull_policy(&json!({}), "reg:5000/busybox:1.36"),
            "IfNotPresent"
        );
        assert_eq!(
            effective_pull_policy(&json!({}), "reg:5000/busybox"),
            "Always"
        );
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
        let m = mgr.service_account_mount(&p).await.unwrap().expect("sa mount");
        assert_eq!(m.container_path, SA_MOUNT_PATH);
        assert!(m.readonly);
        assert_eq!(
            std::fs::read_to_string(format!("{}/namespace", m.host_path)).unwrap_or_default(),
            "default"
        );
        // automountServiceAccountToken: false → no mount.
        let mut off = pod("uid-3", "web", "Always", simple_container());
        off["spec"]["automountServiceAccountToken"] = json!(false);
        assert!(mgr.service_account_mount(&off).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn service_account_env_injected_into_container() {
        let (rt, mgr) = manager();
        let p = pod("uid-4", "web", "Always", simple_container());
        mgr.sync_pods(&[p]).await;
        // The created app container has the KUBERNETES_SERVICE_* env + SA mount.
        let cfg = rt.last_container_config().expect("a container was created");
        assert!(cfg.envs.iter().any(|(k, _)| k == "KUBERNETES_SERVICE_HOST"));
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
        let v = mgr.resolve_volumes(&p).await.unwrap();
        assert_eq!(v.get("bpf").map(|r| r.path.as_str()), Some("/sys/fs/bpf"));
        assert!(v
            .get("scratch")
            .unwrap()
            .path
            .ends_with("/pods/uid-9/volumes/kubernetes.io~empty-dir/scratch"));
        // A directory to bind carries no filesystem — only a claim does.
        assert!(v.get("bpf").unwrap().fstype.is_none());
    }

    #[test]
    fn container_config_resolves_mounts_and_security_context() {
        let mut volumes = HashMap::new();
        volumes.insert(
            "bpf".to_string(),
            ResolvedVolume {
                path: "/sys/fs/bpf".to_string(),
                fstype: None,
                block: false,
            },
        );
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
    fn only_a_privileged_container_gets_bidirectional_propagation() {
        let mut volumes = HashMap::new();
        volumes.insert(
            "kubelet".to_string(),
            ResolvedVolume {
                path: "/var/lib/kubelet".to_string(),
                fstype: None,
                block: false,
            },
        );
        let spec = |privileged: bool| {
            json!({
                "name": "csi-plugin",
                "securityContext": {"privileged": privileged},
                "volumeMounts": [{"name": "kubelet", "mountPath": "/var/lib/kubelet",
                                  "mountPropagation": "Bidirectional"}]
            })
        };
        assert_eq!(
            resolve_mounts(&spec(true), &volumes)[0].propagation,
            MountPropagation::Bidirectional
        );
        assert_eq!(
            resolve_mounts(&spec(false), &volumes)[0].propagation,
            MountPropagation::Private
        );
    }

    #[test]
    fn apply_pod_namespaces_propagates_host_flags() {
        // hostPID pod (the Cilium agent): the container must carry host_pid so
        // its CRI pid namespace becomes NODE and matches the hostPID sandbox —
        // otherwise the runtime rejects it with "pod level PID namespace
        // requested for the container, but pod sandbox was not similarly
        // configured".
        let mut c = build_container_config(&simple_container(), "img", vec![], vec![]);
        assert!(!c.host_pid && !c.host_network && !c.host_ipc);
        let pod = json!({"spec": {"hostPID": true, "hostNetwork": true}});
        apply_pod_namespaces(&mut c, &pod);
        assert!(c.host_pid);
        assert!(c.host_network);
        assert!(!c.host_ipc);
        assert!(!c.share_process_namespace);

        // A plain pod leaves every namespace flag off (container gets its own).
        let mut c2 = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut c2, &json!({"spec": {}}));
        assert!(!c2.host_pid && !c2.host_network && !c2.host_ipc && !c2.share_process_namespace);
    }

    #[test]
    fn seccomp_profile_parsed_and_inherited_from_pod() {
        // parse_seccomp maps each K8s profile type.
        assert_eq!(
            parse_seccomp(&json!({"seccompProfile": {"type": "Unconfined"}})),
            Some(SeccompProfile::Unconfined)
        );
        assert_eq!(
            parse_seccomp(&json!({"seccompProfile": {"type": "RuntimeDefault"}})),
            Some(SeccompProfile::RuntimeDefault)
        );
        assert_eq!(
            parse_seccomp(
                &json!({"seccompProfile": {"type": "Localhost", "localhostProfile": "p.json"}})
            ),
            Some(SeccompProfile::Localhost("p.json".into()))
        );
        assert_eq!(parse_seccomp(&json!({})), None);

        // The Cilium case: seccompProfile set only at the pod level must reach the
        // container, or the runtime default profile fails its syscalls with EPERM.
        let mut c = build_container_config(&simple_container(), "img", vec![], vec![]);
        assert_eq!(c.seccomp_profile, None);
        apply_pod_namespaces(
            &mut c,
            &json!({"spec": {"securityContext": {"seccompProfile": {"type": "Unconfined"}}}}),
        );
        assert_eq!(c.seccomp_profile, Some(SeccompProfile::Unconfined));

        // A container's own profile wins over the pod's.
        let own = json!({"name": "a", "image": "x", "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}}});
        let mut c2 = build_container_config(&own, "x", vec![], vec![]);
        apply_pod_namespaces(
            &mut c2,
            &json!({"spec": {"securityContext": {"seccompProfile": {"type": "Unconfined"}}}}),
        );
        assert_eq!(c2.seccomp_profile, Some(SeccompProfile::RuntimeDefault));
    }

    #[test]
    fn expands_env_refs_in_command_and_args() {
        // The exact cilium-operator case: `--debug=$(CILIUM_DEBUG)` must become
        // `--debug=false` from the resolved env, or the container crash-loops.
        let spec = json!({
            "name": "cilium-operator", "image": "x",
            "command": ["cilium-operator-generic"],
            "args": ["--config-dir=/tmp/cilium/config-map", "--debug=$(CILIUM_DEBUG)"]
        });
        let envs = vec![("CILIUM_DEBUG".to_string(), "false".to_string())];
        let c = build_container_config(&spec, "x", envs, vec![]);
        assert_eq!(c.command, vec!["cilium-operator-generic"]);
        assert_eq!(
            c.args,
            vec!["--config-dir=/tmp/cilium/config-map", "--debug=false"]
        );
    }

    #[test]
    fn expand_env_refs_semantics() {
        let mut m = HashMap::new();
        m.insert("FOO", "bar");
        m.insert("EMPTY", "");
        // basic substitution + surrounding text
        assert_eq!(expand_env_refs("x-$(FOO)-y", &m), "x-bar-y");
        // known-empty var expands to empty
        assert_eq!(expand_env_refs("[$(EMPTY)]", &m), "[]");
        // unknown var left verbatim
        assert_eq!(expand_env_refs("$(MISSING)", &m), "$(MISSING)");
        // `$$` escapes to a single literal `$`
        assert_eq!(expand_env_refs("cost is $$5", &m), "cost is $5");
        // incomplete reference left verbatim
        assert_eq!(expand_env_refs("$(FOO", &m), "$(FOO");
        // lone `$` and `$`+non-paren left verbatim
        assert_eq!(expand_env_refs("a$", &m), "a$");
        assert_eq!(expand_env_refs("$x", &m), "$x");
        // multiple refs in one string
        assert_eq!(expand_env_refs("$(FOO)/$(FOO)", &m), "bar/bar");
    }

    #[test]
    fn passes_selinux_options_to_cri() {
        // Cilium's init containers request `type: spc_t` so they can write host
        // paths under enforcing SELinux (rustkube-node#26).
        let spec = json!({
            "name": "mount-cgroup", "image": "x",
            "securityContext": {"seLinuxOptions": {"type": "spc_t", "level": "s0"}}
        });
        let c = build_container_config(&spec, "x", vec![], vec![]);
        let se = c.selinux_options.expect("seLinuxOptions must pass through");
        assert_eq!(se.type_, "spc_t");
        assert_eq!(se.level, "s0");
        assert_eq!(se.user, "");
        // A container without seLinuxOptions gets None (default label).
        let plain =
            build_container_config(&json!({"name": "a", "image": "x"}), "x", vec![], vec![]);
        assert!(plain.selinux_options.is_none());
    }

    #[test]
    fn selinux_options_inherited_from_pod_and_overridden_by_container() {
        // Pod-level seLinuxOptions applies to a container that sets none.
        let pod = json!({
            "spec": {"securityContext": {"seLinuxOptions": {"type": "spc_t", "level": "s0"}}}
        });
        let mut c = build_container_config(&simple_container(), "img", vec![], vec![]);
        assert!(c.selinux_options.is_none());
        apply_pod_namespaces(&mut c, &pod);
        assert_eq!(c.selinux_options.as_ref().unwrap().type_, "spc_t");

        // The container's own label wins over the pod's.
        let own = json!({
            "name": "app", "image": "x",
            "securityContext": {"seLinuxOptions": {"type": "container_t"}}
        });
        let mut c2 = build_container_config(&own, "x", vec![], vec![]);
        apply_pod_namespaces(&mut c2, &pod);
        assert_eq!(c2.selinux_options.as_ref().unwrap().type_, "container_t");

        // A pod with no label leaves the container unlabelled.
        let mut c3 = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut c3, &json!({"spec": {}}));
        assert!(c3.selinux_options.is_none());
    }

    #[test]
    fn sandbox_carries_the_pod_level_selinux_label() {
        // The sandbox owns the namespaces its containers join, so a pod-level
        // label has to reach it too (rustkube-node#26).
        let pod = json!({
            "metadata": {"name": "cilium", "namespace": "kube-system", "uid": "u1"},
            "spec": {"securityContext": {"seLinuxOptions": {"type": "spc_t", "level": "s0"}}}
        });
        let sb = build_sandbox_config(&pod);
        assert_eq!(sb.selinux_options.as_ref().unwrap().type_, "spc_t");

        // A container-level label alone does not relabel the sandbox.
        let pod2 = json!({
            "metadata": {"name": "web", "namespace": "default", "uid": "u2"},
            "spec": {"containers": [{
                "name": "app", "image": "x",
                "securityContext": {"seLinuxOptions": {"type": "spc_t"}}
            }]}
        });
        assert!(build_sandbox_config(&pod2).selinux_options.is_none());
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
    async fn failed_stop_keeps_runtime_and_local_record_for_retry() {
        let (rt, mgr) = manager();
        mgr.sync_pods(&[pod("uid-stop", "web", "Always", simple_container())])
            .await;
        rt.fail_stop.store(true, Ordering::SeqCst);
        assert!(mgr.stop_pod("uid-stop").await.is_err());
        assert!(mgr.pods.read().await.contains_key("uid-stop"));
        assert_eq!(rt.live_sandbox_count(), 1);
        rt.fail_stop.store(false, Ordering::SeqCst);
        mgr.stop_pod("uid-stop").await.unwrap();
        assert!(!mgr.pods.read().await.contains_key("uid-stop"));
        assert_eq!(rt.live_sandbox_count(), 0);
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
        mgr2.recover_state().await.unwrap();
        // The running pod is adopted, so a re-sync does NOT create a 2nd sandbox.
        let outcome = mgr2.sync_pods(&[p]).await;
        assert_eq!(rt.live_sandbox_count(), 1, "must not double-create sandbox");
        assert_eq!(outcome.updates[0].phase, "Running");
    }

    #[tokio::test]
    async fn incomplete_desired_state_never_stops_a_live_pod() {
        let (runtime, manager) = manager();
        let desired = pod("uid-preserve", "web", "Always", simple_container());
        manager.sync_pods(&[desired]).await;
        assert_eq!(runtime.live_sandbox_count(), 1);
        let partial = manager.sync_pods_observed(&[], false).await;
        assert!(partial.removed.is_empty());
        assert_eq!(runtime.live_sandbox_count(), 1);
        let complete = manager.sync_pods_observed(&[], true).await;
        assert_eq!(complete.removed.len(), 1);
        assert_eq!(runtime.live_sandbox_count(), 0);
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

    /// The reported failure: a `restartPolicy: Always` pod whose container
    /// crashed went `phase=Failed`, and the sync loop skips Failed pods, so
    /// nothing ever restarted it. The cilium-agent DaemonSet pod sat like that
    /// for hours and deleting it was the only way out (#25).
    #[tokio::test]
    async fn an_always_pod_is_not_failed_by_a_crashing_container() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "agent", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);

        let outcome = mgr.sync_pods(&[p]).await;
        let u = &outcome.updates[0];
        assert_eq!(
            u.phase, "Running",
            "an Always pod stays Running: the contract of Always is that the container comes back"
        );
    }

    /// The other half of the strand: a pod already carrying a stored `Failed`
    /// — written by an older kubelet, or by a start that failed — must be
    /// picked up again rather than skipped forever.
    #[tokio::test]
    async fn a_failed_always_pod_is_reconciled_not_skipped() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-1", "agent", "Always", simple_container());
        p["status"] = json!({"phase": "Failed"});

        let outcome = mgr.sync_pods(&[p]).await;
        assert!(
            !rt.container_ids().is_empty(),
            "the pod should have been started, not skipped"
        );
        assert_eq!(outcome.updates[0].phase, "Running");

        // A Never pod that has genuinely finished is still left alone.
        let (rt2, mgr2) = manager();
        let mut done = pod("uid-2", "job", "Never", simple_container());
        done["status"] = json!({"phase": "Succeeded"});
        let outcome = mgr2.sync_pods(&[done]).await;
        assert!(
            rt2.container_ids().is_empty(),
            "a finished job is not restarted"
        );
        assert!(outcome.updates.is_empty());
    }

    /// The second crash backs off instead of being recreated on the next tick,
    /// and says so in the words `kubectl get pod` prints.
    #[tokio::test]
    async fn a_second_crash_backs_off_rather_than_restarting_now() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "agent", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;

        // First crash: restarted immediately — most exits are not a loop.
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        let u = &mgr.sync_pods(&[p.clone()]).await.updates[0];
        assert_eq!(
            u.container_statuses[0].restart_count, 1,
            "the first restart is immediate"
        );

        // Second crash, straight away: held, not recreated.
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        let outcome = mgr.sync_pods(&[p]).await;
        let cs = &outcome.updates[0].container_statuses[0];
        assert_eq!(cs.state, "waiting");
        assert_eq!(cs.reason, "CrashLoopBackOff");
        assert!(cs.message.contains("back-off"), "{}", cs.message);
        assert!(!cs.ready);
        // And the pod is still Running, not Failed.
        assert_eq!(outcome.updates[0].phase, "Running");
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

    // #137: a finished pod's sandbox (and so its pod IP) is given back when
    // it finishes, not when the Pod object is deleted. Its container records
    // stay, for status and logs.
    #[tokio::test]
    async fn finished_never_pod_stops_its_sandbox_and_keeps_its_containers() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "job", "Never", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        let sandbox = mgr.get_sandbox_id("uid-1").await.unwrap();
        assert!(rt.stopped_sandboxes.lock().unwrap().is_empty(), "running: not stopped");

        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 0);
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(outcome.updates[0].phase, "Succeeded");
        assert_eq!(*rt.stopped_sandboxes.lock().unwrap(), vec![sandbox.clone()]);
        assert_eq!(rt.sandboxes.lock().unwrap()[&sandbox].0, PodSandboxState::NotReady);
        assert!(rt.removed_sandboxes.lock().unwrap().is_empty(), "removed only with the Pod");
        assert_eq!(rt.container_ids(), vec![cid], "container record kept for logs");

        // The apiserver now says Succeeded: nothing is stopped again.
        let mut done = p.clone();
        done["status"] = json!({"phase": "Succeeded"});
        mgr.sync_pods(&[done.clone()]).await;
        assert_eq!(rt.stopped_sandboxes.lock().unwrap().len(), 1);

        // Deleting the Pod removes the rest.
        mgr.sync_pods(&[]).await;
        assert_eq!(*rt.removed_sandboxes.lock().unwrap(), vec![sandbox]);
    }

    #[tokio::test]
    async fn a_failed_sandbox_stop_of_a_finished_pod_is_retried() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "job", "Never", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 2);
        rt.fail_sandbox_stop.store(true, Ordering::SeqCst);
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(outcome.updates[0].phase, "Failed", "status still reported");
        assert!(rt.stopped_sandboxes.lock().unwrap().is_empty());
        assert!(mgr.take_due("uid-1").is_some_and(|d| d <= deadlines::RECHECK), "retried");

        // The retry comes through the terminal branch: the pod reads Failed.
        rt.fail_sandbox_stop.store(false, Ordering::SeqCst);
        let mut done = p.clone();
        done["status"] = json!({"phase": "Failed"});
        let outcome = mgr.sync_pods(&[done.clone()]).await;
        assert!(outcome.updates.is_empty());
        assert_eq!(rt.stopped_sandboxes.lock().unwrap().len(), 1);
        mgr.sync_pods(&[done]).await;
        assert_eq!(rt.stopped_sandboxes.lock().unwrap().len(), 1, "once");
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

        // Inside its period the probe is not run again: what it said stands,
        // and the pod asks to be looked at when the period is up (#101).
        rt.set_exec_exit_code(0);
        mgr.take_due("uid-1");
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert!(!outcome.updates[0].container_statuses[0].ready);
        let due = mgr.take_due("uid-1").expect("the probe's next run");
        assert!(due <= std::time::Duration::from_secs(10) && due > std::time::Duration::from_secs(8), "{due:?}");

        // Probe succeeding, once due → ready.
        mgr.expire_probes();
        let outcome = mgr.sync_pods(&[p]).await;
        assert!(outcome.updates[0].container_statuses[0].ready);
    }

    /// A running pod with no probes and nothing pending asks for no timed
    /// look: only an event (an exit, an edit) brings its worker back (#101).
    #[tokio::test]
    async fn a_settled_pod_without_probes_waits_only_for_events() {
        let (_rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        assert!(mgr.take_due("uid-1").is_some(), "one look after the start");
        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].container_statuses[0].state, "running");
        assert_eq!(mgr.take_due("uid-1"), None);
    }

    /// A container in CrashLoopBackOff is looked at again when its backoff
    /// runs out, with no event and no tick (#101).
    #[tokio::test]
    async fn a_backoff_sets_the_pods_next_look() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        let key = crate::crashloop::CrashLoopBackoff::key("uid-1", "app");
        mgr.backoff.restarted(&key);
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        mgr.take_due("uid-1");
        let outcome = mgr.sync_pods(&[p]).await;
        assert_eq!(outcome.updates[0].container_statuses[0].reason, "CrashLoopBackOff");
        let due = mgr.take_due("uid-1").expect("the backoff's end");
        assert!(due <= std::time::Duration::from_secs(10) && due > std::time::Duration::from_secs(8), "{due:?}");
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

        // Second failure, a period later — threshold reached, restart.
        mgr.expire_probes();
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

    /// FileOrCreate was not handled at all, and that is what stopped Cilium's
    /// agent: /run/xtables.lock is declared FileOrCreate, was never created,
    /// and the spawn failed with ENOENT attaching mounts.
    #[test]
    fn host_path_types_create_only_what_they_should() {
        let root = std::env::temp_dir().join(format!("rk-hp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        // DirectoryOrCreate makes the directory, parents included.
        let d = root.join("a/b/c");
        ensure_host_path(d.to_str().unwrap(), "DirectoryOrCreate");
        assert!(d.is_dir(), "DirectoryOrCreate should create the directory");

        // FileOrCreate makes an empty file and its parent.
        let f = root.join("run/xtables.lock");
        ensure_host_path(f.to_str().unwrap(), "FileOrCreate");
        assert!(f.is_file(), "FileOrCreate should create the file");

        // And does not truncate one that exists — it may be held.
        std::fs::write(&f, b"held").unwrap();
        ensure_host_path(f.to_str().unwrap(), "FileOrCreate");
        assert_eq!(std::fs::read(&f).unwrap(), b"held", "must not truncate");

        // Directory means "must already exist": nothing is created.
        let must = root.join("absent");
        ensure_host_path(must.to_str().unwrap(), "Directory");
        assert!(!must.exists(), "Directory must not be created");

        // An unset type performs no checks and creates nothing.
        let none = root.join("untyped");
        ensure_host_path(none.to_str().unwrap(), "");
        assert!(!none.exists(), "an unset type creates nothing");

        std::fs::remove_dir_all(&root).ok();
    }

    /// #132: a pod's start is timed from when the list delivered it, its
    /// volumes and containers by name, and the record is published once.
    #[tokio::test]
    async fn a_start_is_timed_from_seen_and_published_once() {
        let (_rt, mgr) = manager();
        let mut p = pod("uid-timed", "timed", "Always", simple_container());
        p["spec"]["volumes"] = json!([{"name": "scratch", "emptyDir": {}}]);
        mgr.note_seen(&[p.clone()]).await;
        let seen = mgr.timings.lock().unwrap()["uid-timed"].clone();
        assert!(!seen.is_started());

        let outcome = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(outcome.updates[0].phase, "Running");
        let t = mgr.timings.lock().unwrap()["uid-timed"].clone();
        assert!(t.is_started());
        let text = t.finish(std::time::Duration::from_millis(3), Instant::now()).text;
        for part in ["wait=", "image=", "volumes=", "sandbox=", "init=", "containers=",
            "report=3.0ms", "total=", "attempts=1", "pending=1", "volume/scratch=", "container/app="] {
            assert!(text.contains(part), "{part} in {text}");
        }

        // Noted again by the next list: a started pod is not seen anew.
        mgr.note_seen(&[p.clone()]).await;
        mgr.start_reported(&p, std::time::Duration::from_millis(3)).await;
        assert!(!mgr.timings.lock().unwrap().contains_key("uid-timed"));
        mgr.note_seen(&[p.clone()]).await;
        assert!(!mgr.timings.lock().unwrap().contains_key("uid-timed"));
        // A later Running report (a check, not a start) publishes nothing.
        mgr.start_reported(&p, std::time::Duration::ZERO).await;
    }

    /// #138: a start says what it queued behind, the executor's pool and the
    /// pods seen here and not yet started.
    #[tokio::test]
    async fn a_start_says_what_it_queued_behind() {
        let (_rt, mgr) = manager();
        let load = Arc::new(crate::workload::Load::default());
        load.busy.store(5, Ordering::Relaxed);
        load.limit.store(32, Ordering::Relaxed);
        let mgr = mgr.with_load(load);
        let a = pod("uid-a", "a", "Always", simple_container());
        let b = pod("uid-b", "b", "Always", simple_container());
        mgr.note_seen(&[a.clone(), b.clone()]).await;
        mgr.sync_pods_observed(&[a], false).await;
        let text = mgr.timings.lock().unwrap()["uid-a"]
            .finish(std::time::Duration::ZERO, Instant::now())
            .text;
        assert!(text.contains("workers=5/32"), "{text}");
        assert!(text.contains("pending=2"), "{text}");
    }

    /// #132: a pod that waited counts its attempts, and the wait is the
    /// time before the attempt that started it.
    #[tokio::test]
    async fn a_start_after_a_wait_counts_its_attempts() {
        let (_rt, mgr) = manager();
        let mut p = pod("uid-waited", "waited", "Always", simple_container());
        p["spec"]["volumes"] = json!([{"name": "data", "nfs": {"server": "x", "path": "/"}}]);
        mgr.note_seen(&[p.clone()]).await;
        assert_eq!(mgr.sync_pods(&[p.clone()]).await.updates[0].phase, "Pending");
        assert!(!mgr.timings.lock().unwrap()["uid-waited"].is_started());

        p["spec"]["volumes"] = json!([]);
        assert_eq!(mgr.sync_pods(&[p.clone()]).await.updates[0].phase, "Running");
        let t = mgr.timings.lock().unwrap()["uid-waited"].clone();
        let text = t.finish(std::time::Duration::ZERO, Instant::now()).text;
        assert!(text.contains("attempts=2"), "{text}");

        // Gone before it was reported: the record goes with it.
        mgr.sync_pods(&[]).await;
        assert!(!mgr.timings.lock().unwrap().contains_key("uid-waited"));
    }

    /// #148: a pod with no CNI config waits for the config (woken by the
    /// directory, 10 s only as a fallback), not on a backoff from when it was
    /// seen; once the config is there, a failing ADD is retried from its own
    /// first failure (1 s), not from the pod's 60 s of waiting.
    #[tokio::test]
    async fn a_pod_waits_for_the_cni_config_then_retries_add_from_its_own_failure() {
        let (rt, mgr) = manager();
        let p = pod("uid-net", "coredns", "Always", simple_container());
        rt.network.store(1, Ordering::SeqCst);
        mgr.note_seen(&[p.clone()]).await;
        // As if it had been waiting a minute: the old backoff would be 10 s.
        mgr.first_seen.lock().unwrap().insert("uid-net".into(), Instant::now() - std::time::Duration::from_secs(60));
        let out = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(out.updates[0].phase, "Pending");
        assert!(out.updates[0].message.contains("network is not ready"), "{}", out.updates[0].message);
        assert!(mgr.waiting_for_network_config().contains("uid-net"));
        let due = mgr.take_due("uid-net").unwrap();
        assert!(due > std::time::Duration::from_secs(9), "fallback, not a poll: {due:?}");

        // The config appears; the agent is not serving yet.
        rt.network.store(2, Ordering::SeqCst);
        assert_eq!(mgr.sync_pods(&[p.clone()]).await.updates[0].phase, "Pending");
        assert!(mgr.waiting_for_network_config().is_empty(), "no longer waiting on the file");
        let due = mgr.take_due("uid-net").unwrap();
        assert!(due <= std::time::Duration::from_secs(1), "ADD retried at once: {due:?}");

        rt.network.store(0, Ordering::SeqCst);
        assert_eq!(mgr.sync_pods(&[p.clone()]).await.updates[0].phase, "Running");
        assert!(mgr.network_waits.lock().unwrap().is_empty());
    }

    /// #148: a waiting pod that goes is not left among the network waiters.
    #[tokio::test]
    async fn a_deleted_network_waiter_is_forgotten() {
        let (rt, mgr) = manager();
        let p = pod("uid-gone", "gone", "Always", simple_container());
        rt.network.store(1, Ordering::SeqCst);
        mgr.sync_pods(&[p.clone()]).await;
        assert!(mgr.waiting_for_network_config().contains("uid-gone"));
        mgr.sync_pods(&[]).await;
        assert!(mgr.waiting_for_network_config().is_empty());
    }
}
