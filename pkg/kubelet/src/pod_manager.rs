//! Pod lifecycle manager.
//!
//! Manages the lifecycle of pods on this node. Watches for pods scheduled
//! to this node and drives them through: Pending → Running → Succeeded/Failed.
//! Reconciliation is two-way: pods deleted from the API server (or marked
//! with a deletionTimestamp) are stopped and torn down, exited containers
//! are restarted per the pod restartPolicy, and liveness/readiness probes
//! drive container restarts and readiness.

use retry::RetryExt;
use crate::cri::{
    ContainerConfig, ContainerState, CriError, ImageInfo, ImageService, Mount, MountPropagation,
    PodSandboxConfig, RuntimeService, SeLinuxOptions, SeccompProfile,
};
use crate::health::{run_probe, ProbeResult};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

mod csi_volumes;
mod deadlines;

/// An image's resolution and when it finished (#132).
type ImageResult = Arc<std::sync::Mutex<Option<(Result<String, String>, Instant)>>>;
type ImageKey = (String, String);

/// How long a `postStart` hook may take (#374). Upstream sets none; a hook
/// that never ends would hold the pod's start for ever, so one is set.
pub const POST_START_LIMIT: Duration = Duration::from_secs(120);

/// A built-in claim volume's owner in the engine (#372): its claim.
pub fn claim_owner(namespace: &str, claim: &str, uid: &str) -> Value {
    let mut o = serde_json::json!({ "kind": "PersistentVolumeClaim", "namespace": namespace, "name": claim });
    if !uid.is_empty() {
        o["uid"] = serde_json::json!(uid);
    }
    o
}

/// A pod's `VolumeStats` (#372): one per volume `ids` names (pod volume
/// name → volume id), its claim from the pod's `spec.volumes`, its sizes from
/// the engine's `listing` when it has the volume.
pub fn volume_stats(
    namespace: &str,
    pod: &str,
    spec_volumes: &Value,
    ids: &std::collections::BTreeMap<String, String>,
    listing: &Value,
) -> Vec<Value> {
    let items = listing["items"].as_array();
    // A hostPath's volume (#395) is the volume it is on, not the pod's: its
    // sizes are the whole volume's, so it has no stats of its own.
    let host_path = |name: &str| {
        spec_volumes.as_array().is_some_and(|vs| {
            vs.iter().any(|v| v["name"].as_str() == Some(name) && v["hostPath"].is_object())
        })
    };
    ids.iter()
        .filter(|(name, _)| !host_path(name))
        .map(|(name, id)| {
            let mut v = serde_json::json!({ "name": name, "volumeId": id });
            let claim = spec_volumes.as_array().and_then(|vs| {
                vs.iter().find(|v| v["name"].as_str() == Some(name.as_str()))
            }).and_then(|v| match v["persistentVolumeClaim"]["claimName"].as_str() {
                Some(c) => Some(c.to_string()),
                // A generic ephemeral volume's claim is `<pod>-<volume>`.
                None => v.get("ephemeral").map(|_| format!("{pod}-{name}")),
            });
            if let Some(c) = claim {
                v["pvcRef"] = serde_json::json!({ "name": c, "namespace": namespace });
            }
            if let Some(e) = items.and_then(|i| i.iter().find(|e| e["id"].as_str() == Some(id.as_str()))) {
                if let Some(b) = e["virtual_size_bytes"].as_u64() {
                    v["capacityBytes"] = serde_json::json!(b);
                }
                if let Some(b) = e["allocated_bytes"].as_u64() {
                    v["usedBytes"] = serde_json::json!(b);
                }
            }
            v
        })
        .collect()
}

/// Where each container of a pod mounts pod volume `volume` (#469): its
/// `containers`, then its `initContainers` (a sidecar runs as one), as
/// (container name, `mountPath`). A `volumeDevices` entry is a raw block
/// device with no filesystem, so it is not one.
pub fn volume_mount_paths(spec: &Value, volume: &str) -> Vec<(String, String)> {
    ["containers", "initContainers"]
        .iter()
        .flat_map(|k| spec[*k].as_array().into_iter().flatten())
        .flat_map(|c| {
            let name = c["name"].as_str().unwrap_or("").to_string();
            c["volumeMounts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(move |m| m["name"].as_str() == Some(volume))
                .filter_map(|m| m["mountPath"].as_str().map(str::to_string))
                .map(move |p| (name.clone(), p))
        })
        .collect()
}

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
    /// The readiness probe's last result per container name and how many
    /// times in a row it has given it (#460, cd264eea6874): readiness changes
    /// only at `successThreshold` / `failureThreshold`. Forgotten with each
    /// new run of the container.
    pub readiness_run: HashMap<String, ReadinessRun>,
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
    /// Containers whose recorded replacement was created but did not start,
    /// and could not be removed then (#437, 12e774c42aec). The next pass
    /// removes and recreates it rather than waiting on a container that
    /// never runs.
    pub unstarted: std::collections::HashSet<String>,
}

/// What one init container did.
///
/// Separate from [`ContainerStatusReport`] because the questions are not the
/// same. An app container is asked whether it is ready and how often it has
/// restarted; an init container runs once, and what is wanted is whether it
/// finished, with what code, and when.
#[derive(Debug, Clone, Default)]
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
    /// A restartable init container (`restartPolicy: Always`, a native
    /// sidecar, #111): it runs for the pod's life rather than to completion.
    pub restartable: bool,
    /// A sidecar has started: running, and its startupProbe passed if it has
    /// one. What lets the next init container go.
    pub started: bool,
    /// A sidecar's readiness, which counts toward the pod's.
    pub ready: bool,
    pub restart_count: u32,
}

impl InitContainerStatusReport {
    /// Did this one finish successfully?
    pub fn succeeded(&self) -> bool {
        self.state == "terminated" && self.exit_code == 0
    }

    /// Has its init step been done: finished (an ordinary init) or started
    /// (a sidecar)?
    pub fn initialized(&self) -> bool {
        self.succeeded() || (self.restartable && self.started)
    }
}

/// What is left of the pod's `activeDeadlineSeconds` (#126), from its
/// `status.startTime`, else from when this kubelet first saw it. `None`: no
/// deadline. Zero: passed.
fn active_deadline_left(
    pod: &Value,
    seen: Option<Instant>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<std::time::Duration> {
    let deadline = std::time::Duration::from_secs(pod["spec"]["activeDeadlineSeconds"].as_u64()?);
    let active = pod["status"]["startTime"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .and_then(|t| (now - t.with_timezone(&chrono::Utc)).to_std().ok())
        .or_else(|| seen.map(|s| s.elapsed()))
        .unwrap_or_default();
    Some(deadline.saturating_sub(active))
}

/// Is this init container a native sidecar (#111)?
pub fn is_restartable_init(spec: &Value) -> bool {
    spec["restartPolicy"].as_str() == Some("Always")
}

/// The pod's sidecars' names, in declaration order.
fn sidecar_names(pod: &Value) -> Vec<String> {
    pod["spec"]["initContainers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|c| is_restartable_init(c))
                .filter_map(|c| c["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
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
/// The annotation upstream's kubelet marks a pod's source with: `file` for a
/// static pod's manifest, `api` for the apiserver's (#262).
pub const CONFIG_SOURCE: &str = "kubernetes.io/config.source";

/// A pod from this node's manifest directory (#262).
pub fn is_static(pod: &Value) -> bool {
    pod["metadata"]["annotations"][CONFIG_SOURCE].as_str() == Some("file")
}

/// The engine volume a static pod's `hostPath` lives on (#395), from the
/// engine's volume listing (`items`): the one mounted at `path` or at the
/// deepest directory above it (by path component), its own mount point or one
/// of its attachments'. A volume mounted at `/` names nothing, and a path on no
/// listed volume has none: never a guess.
pub fn host_path_volume<'a>(items: &'a [Value], path: &str) -> Option<&'a str> {
    let path = path.trim_end_matches('/');
    let under = |at: &str| {
        let at = at.trim_end_matches('/');
        !at.is_empty() && (path == at || path.strip_prefix(at).is_some_and(|rest| rest.starts_with('/')))
    };
    items
        .iter()
        .filter_map(|v| {
            let id = v["id"].as_str().filter(|i| !i.is_empty())?;
            let own = v["mounted_at"].as_str().into_iter();
            let attached = v["attachments"].as_array().into_iter().flatten().filter_map(|a| a["mounted_at"].as_str());
            let depth = own.chain(attached).filter(|at| under(at)).map(|at| at.trim_end_matches('/').len()).max()?;
            Some((depth, id))
        })
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, id)| id)
}

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

/// `path` as the host resolves it, every symlink followed (#395): stormcos's
/// `/data` is a link to `/d`, so etcd's `/data/fastetcd` is on the volume
/// mounted at `/d/fastetcd`. Links are read under `root` (the host's root as
/// this process sees it), an absolute target starting again at the host's `/`.
/// What does not exist is kept as written; past 40 links the path is as given.
fn host_real_path(root: &std::path::Path, path: &str) -> String {
    let mut rest: std::collections::VecDeque<String> =
        path.split('/').filter(|c| !c.is_empty()).map(String::from).collect();
    let mut done: Vec<String> = Vec::new();
    let mut links = 0;
    while let Some(c) = rest.pop_front() {
        match c.as_str() {
            "." => continue,
            ".." => {
                done.pop();
                continue;
            }
            _ => {}
        }
        done.push(c);
        let here = root.join(done.join("/"));
        let Ok(target) = std::fs::read_link(&here) else {
            if std::fs::symlink_metadata(&here).is_err() {
                // Absent: the rest as written.
                done.extend(rest.drain(..));
            }
            continue;
        };
        links += 1;
        if links > 40 {
            return path.to_string();
        }
        done.pop();
        let target = target.to_string_lossy().into_owned();
        if target.starts_with('/') {
            done.clear();
        }
        for t in target.split('/').filter(|c| !c.is_empty()).rev() {
            rest.push_front(t.to_string());
        }
    }
    format!("/{}", done.join("/"))
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
    /// A volume the kubelet writes from the API (configMap, secret,
    /// projected): mounted read-only whatever `volumeMounts.readOnly` says,
    /// as upstream does, so a container cannot plant a symlink where the
    /// kubelet writes next (rustkube-node#428, finding 551d194231b5).
    pub api: bool,
}

/// Where a node service's logs are (#72, #124): its stormd log volume, and
/// what PID 1 recorded about its last exit. At least one is present.
#[derive(Debug, Clone)]
pub struct NodeService {
    pub log_dir: Option<std::path::PathBuf>,
    pub record: Option<crate::node_logs::Record>,
    /// PID 1's workload log directory, where a run's files are (#87).
    pub runs_dir: std::path::PathBuf,
}

/// The claim's binding (its PV and the claim bound to it), with only what it
/// needs, so the pod path can run it after the attach without waiting for it
/// (#95: 150–220 ms of a claim's start on 11.91). The pod does not need it:
/// the control plane's binder has normally bound the claim already, and this
/// brings the PV to the class's capacity and source.
#[derive(Clone)]
struct ClaimBinder {
    api_url: String,
    api_client: reqwest::Client,
    storage_url: String,
    engine: crate::engine::EngineClient,
    node_name: String,
}

impl ClaimBinder {
    async fn api_get(&self, path: &str) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let r = self.api_client.get(format!("{}{path}", self.api_url)).send_retrying(retry::Policy::API).await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json().await.ok()
    }

    async fn api_post(&self, path: &str, body: &Value) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let r = self.api_client.post(format!("{}{path}", self.api_url)).json(body).send_repeatable(retry::Policy::API).await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json().await.ok()
    }

    async fn api_put(&self, path: &str, body: &Value) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let r = self.api_client.put(format!("{}{path}", self.api_url)).json(body).send_retrying(retry::Policy::API).await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json().await.ok()
    }

    async fn storage_get(&self, path: &str) -> Option<Value> {
        let r = self.engine.get(&format!("{}{path}", self.storage_url)).await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json::<Value>().await.ok()
    }

    async fn bind(&self, namespace: &str, claim: &str, volume: &str, bytes: u64) {
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
        // The engine's size when it says one (#379): a grown volume is past
        // its class, and a restore is its snapshot's size until grown to its
        // claim. The class's, which is what was made, otherwise.
        let bytes = if facts.bytes > 0 { facts.bytes } else { bytes };
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
        } else if pvc["status"]["allocatedResourceStatuses"].is_null() {
            // Bound by the binder to the request-sized PV: the capacity is
            // the class's, which is what `df` in the pod shows. Not while a
            // grow is under way (#379): the claim's capacity follows its
            // filesystem, and writing the volume's here would skip it.
            pvc["status"]["capacity"] = capacity;
        }
        if pvc == before {
            return;
        }
        if self.api_put(&path, &pvc).await.is_some() {
            info!("PVC {namespace}/{claim} bound to {pv_name}");
        }
    }
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
    /// How a claim is charged against the data slabs (#62).
    storage_policy: crate::capacity::Policy,
    /// stormstorage, which mirrors a claim's volume to another node (#338).
    stormstorage: Option<crate::engine::EngineClient>,
    /// Held from a new claim's capacity check to its volume's creation (#62),
    /// so two claims cannot both take the last of the room.
    capacity_lock: tokio::sync::Mutex<()>,
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
    /// Every projected ServiceAccount token this kubelet wrote, by pod uid,
    /// to be written again at 80% of its life (#122).
    tokens: std::sync::Mutex<HashMap<String, Vec<TokenFile>>>,
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
    /// RuntimeClass name → handler (#204). Upstream's `handler` is immutable,
    /// so an answer is kept for the kubelet's life.
    runtime_handlers: std::sync::Mutex<HashMap<String, String>>,
    /// Each starting pod's phases (by uid), from seen to `Running`
    /// acknowledged (#132).
    timings: std::sync::Mutex<HashMap<String, crate::start_timing::StartTiming>>,
    /// Pods (by uid) that are this node's but not started, and what they wait
    /// on (#63). Without this a pod waiting on its claim was known to nobody:
    /// `logs` said "not found on this node" and it had no container statuses.
    waiting: std::sync::Mutex<HashMap<String, WaitingPod>>,
    /// Not before (#133): a pod whose image would not pull, or whose
    /// container would not start, is not tried again before its back-off is
    /// up, whatever wakes it (an exit anywhere wakes every workload, #115).
    start_holdoff: std::sync::Mutex<HashMap<String, Instant>>,
    /// How many tries in a row a pod's container create or start was refused (#287, #289):
    /// those are retried on [`deadlines::create_backoff`]'s short schedule
    /// before the 10 s clock. Cleared when the pod starts.
    create_refusals: std::sync::Mutex<HashMap<String, u32>>,
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
    /// Completed init containers kept until their pod stops, by pod uid (#47),
    /// on a runtime whose removal takes the log with it.
    kept_inits: std::sync::Mutex<HashMap<String, Vec<String>>>,
    /// Each container's previous run, by pod uid then container (#130): what
    /// `lastState.terminated` says once the next run has started.
    last_terminated: std::sync::Mutex<HashMap<String, HashMap<String, LastTerminated>>>,
    /// The volume behind each of a pod's claims, by pod uid then the pod's
    /// volume name (#372): the engine's id for a built-in claim, the driver's
    /// handle for another's. `storm.io/volume.<name>` on the pod and the
    /// summary's `volume[]` say the same.
    pod_volumes: std::sync::Mutex<HashMap<String, std::collections::BTreeMap<String, String>>>,
}

/// A pod this node has admitted and not started, and why.
#[derive(Debug, Clone)]
pub struct WaitingPod {
    pub namespace: String,
    pub name: String,
    /// What it waits on, as `describe` shows it.
    pub reason: String,
    /// The containers' reason: `ContainerCreating`, `ErrImagePull`,
    /// `ImagePullBackOff`, `CreateContainerError`, `RunContainerError`, or
    /// `StartError` for one that failed for good (#133).
    pub kind: String,
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
            stormstorage: None,
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
            storage_policy: crate::capacity::Policy::default(),
            capacity_lock: tokio::sync::Mutex::new(()),
            state_root: "/var/lib/kubelet".to_string(),
            cluster_dns: vec!["10.96.0.10".to_string()],
            cluster_domain: "cluster.local".to_string(),
            ca_pem: None,
            tokens: Default::default(),
            backoff: crate::crashloop::CrashLoopBackoff::new(),
            csi: Arc::new(crate::csi_plugins::CsiPlugins::new(node_name)),
            csi_mountinfo: "/proc/1/mountinfo".to_string(),
            first_seen: std::sync::Mutex::new(HashMap::new()),
            network_waits: Default::default(),
            runtime_handlers: Default::default(),
            timings: Default::default(),
            deadlines: Default::default(),
            event_waits: Default::default(),
            kept_inits: Default::default(),
            last_terminated: Default::default(),
            pod_volumes: Default::default(),
            waiting: std::sync::Mutex::new(HashMap::new()),
            start_holdoff: std::sync::Mutex::new(HashMap::new()),
            create_refusals: std::sync::Mutex::new(HashMap::new()),
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

    /// Where this node's logs are, for `/logs/` (#198).
    pub fn host_logs(&self) -> crate::host_logs::Node {
        crate::host_logs::Node {
            host_root: self.host_root.clone(),
            assets_json: self.assets_json.clone(),
            runs_dir: self.runs_dir(),
            name: self.node_name.clone(),
        }
    }

    /// PID 1's per-run output files: beside its asset table.
    fn runs_dir(&self) -> std::path::PathBuf {
        std::path::Path::new(&self.assets_json)
            .parent()
            .map(|d| d.join("logs"))
            .unwrap_or_else(crate::node_logs::engine_log_dir)
    }

    /// The node service behind a mirror pod (#72, #124):
    /// `kube-system/<asset>-<node>`, container `<asset>`, and either a boot
    /// unit that mounts a volume at the asset's `/var/log/stormd` or an entry
    /// in PID 1's asset table. `None` for anything else.
    pub fn node_service(&self, namespace: &str, pod: &str, container: &str) -> Option<NodeService> {
        let asset = pod.strip_suffix(&format!("-{}", self.node_name))?;
        if asset.is_empty() || asset != container {
            return None;
        }
        // In its service's project (#418), as the mirror put it.
        if namespace != crate::mirror_namespaces::current().of(asset) {
            return None;
        }
        let log_dir = crate::node_logs::log_dir(&self.host_root, asset);
        let record = std::fs::read_to_string(&self.assets_json)
            .ok()
            .and_then(|t| crate::node_logs::record(&t, asset));
        if log_dir.is_none() && record.is_none() {
            return None;
        }
        Some(NodeService { log_dir, record, runs_dir: self.runs_dir() })
    }

    /// Use this registry of CSI drivers: the one the kubelet's registration
    /// loop fills.
    /// The node's engine, shared with the rest of the kubelet.
    /// How claims are charged against the data slabs (#62).
    pub fn with_storage_policy(mut self, policy: crate::capacity::Policy) -> Self {
        self.storage_policy = policy;
        self
    }

    pub fn with_engine(mut self, engine: crate::engine::EngineClient) -> Self {
        self.storage_url = engine.url().to_string();
        self.engine = engine;
        self
    }

    /// stormstorage's API, for moving a claim to another node (#338).
    pub fn with_stormstorage(mut self, stormstorage: Option<crate::engine::EngineClient>) -> Self {
        self.stormstorage = stormstorage;
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
            .send_retrying(retry::Policy::API)
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// GET an object, telling "not there" (`Ok(None)`, a 404) apart from "not
    /// answered" (`Err`): what decides a claim's policy must not read an
    /// unreachable StorageClass as one that names nothing (#71).
    /// The handler of the pod's RuntimeClass (#204): empty without one. A
    /// RuntimeClass that is not there is a wait with the reason, as upstream
    /// (the pod is never run on the default runtime instead); an apiserver
    /// that does not answer is a wait too.
    async fn runtime_handler(&self, pod: &Value) -> Result<String, CriError> {
        let Some(class) = pod["spec"]["runtimeClassName"].as_str().filter(|c| !c.is_empty()) else {
            return Ok(String::new());
        };
        if let Some(h) = self.runtime_handlers.lock().unwrap_or_else(|e| e.into_inner()).get(class) {
            return Ok(h.clone());
        }
        let not_found = || CriError::Refused {
            reason: "FailedCreatePodSandBox".into(),
            message: format!("RuntimeClass \"{class}\" not found"),
        };
        if self.api_url.is_empty() {
            return Err(not_found());
        }
        let rc = self
            .api_get_checked(&format!("/apis/node.k8s.io/v1/runtimeclasses/{class}"))
            .await
            .map_err(|e| CriError::Pending(format!("RuntimeClass {class}: {e}")))?
            .ok_or_else(not_found)?;
        let handler = rc["handler"].as_str().unwrap_or("").to_string();
        if handler.is_empty() {
            return Err(CriError::Refused {
                reason: "FailedCreatePodSandBox".into(),
                message: format!("RuntimeClass \"{class}\" has no handler"),
            });
        }
        self.runtime_handlers.lock().unwrap_or_else(|e| e.into_inner()).insert(class.to_string(), handler.clone());
        Ok(handler)
    }

    async fn api_get_checked(&self, path: &str) -> Result<Option<Value>, String> {
        if self.api_url.is_empty() {
            return Ok(None);
        }
        let resp = self
            .api_client
            .get(format!("{}{path}", self.api_url))
            .send_retrying(retry::Policy::API)
            .await
            .map_err(|e| format!("GET {path}: {e}"))?;
        match resp.status() {
            s if s.as_u16() == 404 => Ok(None),
            s if s.is_success() => resp.json::<Value>().await.map(Some).map_err(|e| format!("GET {path}: {e}")),
            s => Err(format!("GET {path}: {s}")),
        }
    }

    /// A pod's extra networks and replaced default network, resolved to their
    /// NetworkAttachmentDefinitions (#233). `Err` names what is missing.
    async fn resolve_networks(
        &self,
        pod: &Value,
    ) -> Result<(Vec<crate::cri::NetworkAttachment>, Option<crate::cri::NetworkAttachment>), String> {
        use crate::multus;
        let ns = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let annotations = &pod["metadata"]["annotations"];
        let mut networks = Vec::new();
        if let Some(a) = annotations[multus::NETWORKS].as_str() {
            for (i, sel) in multus::parse(a)?.iter().enumerate() {
                let nad = self.nad(sel, ns).await?;
                networks.push(multus::attachment(sel, ns, multus::ifname(sel, i), nad.as_ref())?);
            }
        }
        let default_network = match annotations[multus::DEFAULT_NETWORK].as_str().filter(|a| !a.trim().is_empty()) {
            None => None,
            Some(a) => {
                let mut sels = multus::parse(a)?;
                if sels.len() != 1 {
                    return Err(format!("{}: one network, not {}", multus::DEFAULT_NETWORK, sels.len()));
                }
                let sel = sels.remove(0);
                let nad = self.nad(&sel, ns).await?;
                Some(multus::attachment(&sel, ns, cni::invoker::DEFAULT_IFNAME.to_string(), nad.as_ref())?)
            }
        };
        Ok((networks, default_network))
    }

    /// A NAD as the apiserver has it; `Ok(None)` when it does not exist.
    async fn nad(&self, sel: &crate::multus::Selection, pod_namespace: &str) -> Result<Option<Value>, String> {
        let ns = sel.namespace.as_deref().unwrap_or(pod_namespace);
        self.api_get_checked(&crate::multus::nad_path(ns, &sel.name))
            .await
            .map_err(|e| format!("NetworkAttachmentDefinition {ns}/{}: {e}", sel.name))
    }

    /// The placement policy a new claim's volume gets: its StorageClass's
    /// `redundancy`, `spread` and `tier` (#71). A class that does not exist,
    /// or names nothing, is the default (one copy); one that cannot be read
    /// is a wait, never "none"; one that names a policy this driver does not
    /// take is refused on the claim.
    async fn claim_policy(&self, pvc: &Value) -> Result<crate::storage::ClaimPolicy, ClaimError> {
        let class = pvc["spec"]["storageClassName"].as_str().unwrap_or(crate::storage::STORAGE_CLASS);
        let sc = self
            .api_get_checked(&format!("/apis/storage.k8s.io/v1/storageclasses/{class}"))
            .await
            .map_err(|e| ClaimError::Failed(format!("waiting to read StorageClass {class}: {e}")))?;
        crate::storage::ClaimPolicy::from_class(sc.as_ref()).map_err(|e| {
            ClaimError::Failed(format!("StorageClass {class}: {e}"))
        })
    }

    /// Emit a pod event, if there is a recorder.
    ///
    /// The recorder was built, stored, and never called: `pod_event` had zero
    /// call sites, so `describe pod` showed `Events: <none>` for exactly the
    /// failures somebody opens it for. A helper rather than `if let` at every
    /// site, because the sites are what was missing and they should be one
    /// line each.
    /// Run one lifecycle handler of a container (#374): `exec` in the
    /// container, `httpGet` or `tcpSocket` from the pod's network namespace
    /// (the probe paths, `health.rs`), or `sleep`. Bounded by `within`.
    async fn run_hook(
        &self,
        handler: &Value,
        spec: &Value,
        cid: &str,
        sandbox_id: &str,
        pod_ip: &str,
        within: Duration,
    ) -> Result<(), String> {
        if let Some(secs) = handler["sleep"]["seconds"].as_u64() {
            tokio::time::sleep(Duration::from_secs(secs).min(within)).await;
            return Ok(());
        }
        let mut probe = handler.clone();
        probe["timeoutSeconds"] = serde_json::json!(within.as_secs().max(1));
        let netns = match handler["exec"].is_null() {
            true => self.runtime.pod_sandbox_status(sandbox_id).await.ok().and_then(|s| s.netns_path),
            false => None,
        };
        match crate::health::run_probe(&probe, spec, cid, pod_ip, netns.as_deref(), &self.runtime).await {
            crate::health::ProbeResult::Success => Ok(()),
            crate::health::ProbeResult::Failure(why) => Err(why),
            crate::health::ProbeResult::Unknown => Err("the handler's result is unknown".into()),
        }
    }

    /// A container's `lifecycle.postStart` (#374), run once it has started.
    /// As upstream: it fails the container, which is killed with a
    /// `FailedPostStartHook` event and restarted (or not) by its policy.
    /// Bounded by [`POST_START_LIMIT`].
    async fn post_start(&self, pod: &Value, spec: &Value, cid: &str, sandbox_id: &str, pod_ip: &str) {
        let handler = &spec["lifecycle"]["postStart"];
        if handler.is_null() {
            return;
        }
        let name = spec["name"].as_str().unwrap_or("");
        if let Err(why) = self.run_hook(handler, spec, cid, sandbox_id, pod_ip, POST_START_LIMIT).await {
            let msg = format!("PostStartHook failed for container {name}: {why}");
            warn!("{msg}");
            self.event(pod, "Warning", "FailedPostStartHook", &msg).await;
            if let Err(e) = self.runtime.stop_container(cid, 0).await {
                warn!("container {name}: not killed after its postStart hook failed: {e}");
            }
        }
    }

    /// A container's `lifecycle.preStop` (#374), run before its stop's
    /// SIGTERM, inside the grace; what it took is taken from the grace the
    /// stop has left, as upstream. A failure is a `FailedPreStopHook` event,
    /// and the stop goes on.
    async fn pre_stop(&self, pod: &Value, name: &str, cid: &str, grace: i64) -> i64 {
        let Some(spec) = ["containers", "initContainers"]
            .iter()
            .filter_map(|k| pod["spec"][k].as_array())
            .flatten()
            .find(|c| c["name"].as_str() == Some(name))
            .cloned()
        else {
            return grace;
        };
        let handler = &spec["lifecycle"]["preStop"];
        if handler.is_null() || grace <= 0 {
            return grace;
        }
        let (sandbox_id, pod_ip) = self
            .pods
            .read()
            .await
            .get(pod["metadata"]["uid"].as_str().unwrap_or(""))
            .map(|p| (p.sandbox_id.clone(), p.pod_ip.clone().unwrap_or_default()))
            .unwrap_or_default();
        let t = Instant::now();
        let within = Duration::from_secs(grace as u64);
        let done = self.run_hook(handler, &spec, cid, sandbox_id.as_deref().unwrap_or(""), &pod_ip, within);
        match tokio::time::timeout(within, done).await {
            Ok(Ok(())) => {}
            Ok(Err(why)) => {
                let msg = format!("PreStopHook failed for container {name}: {why}");
                warn!("{msg}");
                self.event(pod, "Warning", "FailedPreStopHook", &msg).await;
            }
            Err(_) => {
                let msg = format!("PreStopHook for container {name} did not finish within the grace ({grace}s)");
                warn!("{msg}");
                self.event(pod, "Warning", "FailedPreStopHook", &msg).await;
            }
        }
        (grace - t.elapsed().as_secs() as i64).max(0)
    }

    async fn event(&self, pod: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.pod_event(pod, etype, reason, message).await;
        }
    }

    /// An Event on a PersistentVolumeClaim (#70): its provisioning, as
    /// external-provisioner reports it (`Provisioning`, `ProvisioningFailed`,
    /// `ProvisioningSucceeded`). Repeats aggregate in the recorder.
    async fn claim_event(&self, claim: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event("v1", "PersistentVolumeClaim", claim, etype, reason, message).await;
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
            .send_repeatable(retry::Policy::API)
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
            .send_retrying(retry::Policy::API)
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
        self.binder().bind(namespace, claim, volume, bytes).await
    }

    /// What [`ClaimBinder`] needs, cloned out of this manager so a bind can
    /// run as a task of its own (#95).
    fn binder(&self) -> ClaimBinder {
        ClaimBinder {
            api_url: self.api_url.clone(),
            api_client: self.api_client.clone(),
            storage_url: self.storage_url.clone(),
            engine: self.engine.clone(),
            node_name: self.node_name.clone(),
        }
    }

    /// Say in the engine that a claim's volume is the claim's (#372):
    /// stormblock#115's owner, `PersistentVolumeClaim` with its namespace,
    /// name and uid, so the engine's listing and the console say whose it is
    /// and its cleanup never takes it for an orphan. Off the start path: the
    /// pod needs the device, not the record; a refusal is said and the next
    /// start asks again.
    fn own_claim_volume(&self, vol_id: &str, namespace: &str, claim: &str, uid: &str) {
        let owner = claim_owner(namespace, claim, uid);
        let url = format!("{}/api/v1/volumes/{vol_id}/owner", self.storage_url);
        let engine = self.engine.clone();
        let what = format!("{namespace}/{claim}");
        tokio::spawn(async move {
            match engine.put(&url, &crate::container_roots::owner_body(&owner)).await {
                Ok(r) if r.status().is_success() => debug!("claim {what}: its volume's owner recorded"),
                Ok(r) => warn!("claim {what}: its volume's owner not recorded: stormblock answered {}", r.status()),
                Err(e) => warn!("claim {what}: its volume's owner not recorded: {e}"),
            }
        });
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

    /// A ConfigMap's or Secret's keys and their values, as bytes
    /// (#437, dc6872a9854f: a Secret's value is any bytes, and a lossy UTF-8
    /// conversion changed a keystore's 0xff into U+FFFD).
    ///
    /// `Ok(None)`: the object does not exist (404), or there is no apiserver
    /// to hold it. `Err`: it could not be read (refused, unreachable, not
    /// decodable), which is never read as "absent" (#437, e635cb20b82e): a
    /// required credential that could not be fetched is a pod that waits,
    /// not a pod that starts without it.
    async fn fetch_keyed(
        &self,
        kind: KeyedKind,
        namespace: &str,
        name: &str,
    ) -> Result<Option<KeyedData>, String> {
        if self.api_url.is_empty() {
            return Ok(None);
        }
        let what = kind.describe(namespace, name);
        let url = format!("{}/api/v1/namespaces/{namespace}/{}/{name}", self.api_url, kind.resource());
        let resp = self
            .api_client
            .get(&url)
            .send_retrying(retry::Policy::API)
            .await
            .map_err(|e| format!("{what}: the apiserver did not answer: {e}"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("{what}: the apiserver answered {status}: {}", excerpt(&body)));
        }
        let obj: Value = resp.json().await.map_err(|e| format!("{what}: not JSON: {e}"))?;
        keyed_data(kind, &obj).map(Some).map_err(|e| format!("{what}: {e}"))
    }

    /// Resolve a container's env (#437, 699efd7029fe and e635cb20b82e):
    /// `envFrom` first, each source's keys under its `prefix`, a later
    /// source over an earlier one; then `env`, whose entries override what
    /// `envFrom` imported, a later one over an earlier one of the same name.
    /// `env` is a literal `value` or a `valueFrom` (downward `fieldRef`,
    /// `configMapKeyRef`, `secretKeyRef`).
    ///
    /// A ConfigMap, Secret or key that does not exist is skipped only when
    /// its reference is `optional`; otherwise, and whenever the object could
    /// not be read, the container is not created: `Err` is upstream's
    /// `CreateContainerConfigError`, which waits and is tried again.
    async fn resolve_env(
        &self,
        pod: &Value,
        container_spec: &Value,
        pod_ip: Option<&str>,
    ) -> Result<Vec<(String, String)>, CriError> {
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let container = container_spec["name"].as_str().unwrap_or("");
        let config_error = |message: String| CriError::Container {
            container: container.to_string(),
            reason: "CreateContainerConfigError".into(),
            message,
        };
        let mut cache: HashMap<(KeyedKind, String), Option<KeyedData>> = HashMap::new();
        let mut out: Vec<(String, String)> = Vec::new();
        for src in container_spec["envFrom"].as_array().into_iter().flatten() {
            let prefix = src["prefix"].as_str().unwrap_or("");
            let (kind, r) = if src["configMapRef"].is_object() {
                (KeyedKind::ConfigMap, &src["configMapRef"])
            } else if src["secretRef"].is_object() {
                (KeyedKind::Secret, &src["secretRef"])
            } else {
                continue;
            };
            let name = r["name"].as_str().unwrap_or("");
            let optional = r["optional"].as_bool().unwrap_or(false);
            let data = self.fetch_cached(&mut cache, kind, namespace, name).await.map_err(config_error)?;
            let Some(data) = data else {
                if optional {
                    continue;
                }
                return Err(config_error(format!("{} not found", kind.describe(namespace, name))));
            };
            for (k, v) in data {
                let var = format!("{prefix}{k}");
                if !valid_env_name(&var) {
                    warn!(
                        "container {container}: envFrom {}: {var:?} is not a valid variable name; skipped",
                        kind.describe(namespace, name)
                    );
                    continue;
                }
                set_env(&mut out, var, String::from_utf8_lossy(&v).into_owned());
            }
        }
        for e in container_spec["env"].as_array().into_iter().flatten() {
            let name = match e["name"].as_str() {
                Some(n) => n.to_string(),
                None => continue,
            };
            if let Some(v) = e["value"].as_str() {
                set_env(&mut out, name, v.to_string());
                continue;
            }
            let vf = &e["valueFrom"];
            if let Some(path) = vf["fieldRef"]["fieldPath"].as_str() {
                if let Some(v) = self.downward_field(pod, path, pod_ip) {
                    set_env(&mut out, name, v);
                }
                continue;
            }
            let (kind, r) = if vf["configMapKeyRef"].is_object() {
                (KeyedKind::ConfigMap, &vf["configMapKeyRef"])
            } else if vf["secretKeyRef"].is_object() {
                (KeyedKind::Secret, &vf["secretKeyRef"])
            } else if vf.is_null() {
                // Neither `value` nor `valueFrom`: the variable is set, empty.
                set_env(&mut out, name, String::new());
                continue;
            } else {
                // resourceFieldRef is #329.
                continue;
            };
            let obj = r["name"].as_str().unwrap_or("");
            let key = r["key"].as_str().unwrap_or("");
            let optional = r["optional"].as_bool().unwrap_or(false);
            let data = self.fetch_cached(&mut cache, kind, namespace, obj).await.map_err(config_error)?;
            match data.as_ref().map(|d| d.get(key)) {
                Some(Some(v)) => set_env(&mut out, name, String::from_utf8_lossy(v).into_owned()),
                _ if optional => {}
                None => {
                    return Err(config_error(format!(
                        "env {name}: {} not found",
                        kind.describe(namespace, obj)
                    )))
                }
                Some(None) => {
                    return Err(config_error(format!(
                        "env {name}: {} has no key {key:?}",
                        kind.describe(namespace, obj)
                    )))
                }
            }
        }
        Ok(out)
    }

    /// [`fetch_keyed`](Self::fetch_keyed) once per object for one container.
    async fn fetch_cached(
        &self,
        cache: &mut HashMap<(KeyedKind, String), Option<KeyedData>>,
        kind: KeyedKind,
        namespace: &str,
        name: &str,
    ) -> Result<Option<KeyedData>, String> {
        let k = (kind, name.to_string());
        if let Some(d) = cache.get(&k) {
            return Ok(d.clone());
        }
        let d = self.fetch_keyed(kind, namespace, name).await?;
        cache.insert(k, d.clone());
        Ok(d)
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
        self.resolve_volumes_timed(pod, &mut Vec::new(), &mut Vec::new()).await
    }

    /// [`resolve_volumes`](Self::resolve_volumes), with how long each volume
    /// took (#132) and each claim's steps (#95).
    async fn resolve_volumes_timed(
        &self,
        pod: &Value,
        times: &mut Vec<(String, std::time::Duration)>,
        claims: &mut Vec<(String, crate::start_timing::ClaimSteps)>,
    ) -> Result<HashMap<String, ResolvedVolume>, CriError> {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let mut map = HashMap::new();
        let mut current: Option<(String, Instant)> = None;
        // The engine's volumes, read once, for a static pod's hostPaths (#395).
        let mut engine_volumes: Option<Value> = None;
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
                // A static pod's data is a hostPath (etcd's `/data/fastetcd`):
                // its mirror names the engine volume it is on (#395), as a
                // claim's pod names its claim's (#372).
                if is_static(pod) {
                    if engine_volumes.is_none() {
                        engine_volumes = Some(self.storage_get("/api/v1/volumes").await.unwrap_or(Value::Null));
                    }
                    let items = engine_volumes.as_ref().and_then(|l| l["items"].as_array());
                    let root = std::path::Path::new(HOST_ROOT);
                    let real = host_real_path(if root.is_dir() { root } else { std::path::Path::new("/") }, hp);
                    if let Some(id) = items.and_then(|i| host_path_volume(i, &real)) {
                        self.record_pod_volume(uid, &name, id);
                    }
                }
                hp.to_string()
            } else if vol["configMap"].is_object() || vol["secret"].is_object() {
                // #437: the object's bytes, only the `items` it selects, and
                // a missing required object or key is a pod that waits.
                let (kind, src, obj, sub) = if vol["configMap"].is_object() {
                    let src = &vol["configMap"];
                    (KeyedKind::ConfigMap, src, src["name"].as_str().unwrap_or(""), "configmap")
                } else {
                    let src = &vol["secret"];
                    (KeyedKind::Secret, src, src["secretName"].as_str().unwrap_or(""), "secret")
                };
                let what = kind.describe(namespace, obj);
                let dir = pod_volume_dir(&self.state_root, uid, sub, &name);
                let data = self
                    .fetch_keyed(kind, namespace, obj)
                    .await
                    .map_err(|e| CriError::VolumeNotReady(format!("volume {name}: {e}")))?;
                let files = keyed_files(&what, data.as_ref(), src, kind.default_mode())
                    .map_err(|e| CriError::VolumeNotReady(format!("volume {name}: {e}")))?;
                write_keyed(&dir, &files, true).map_err(|e| pod_dir_error(&name, &dir, e))?;
                dir
            } else if let Some(sources) = vol["projected"]["sources"].as_array() {
                // Projected volume (e.g. kube-api-access: SA token + CA + downward API).
                let dir = pod_volume_dir(&self.state_root, uid, "projected", &name);
                std::fs::create_dir_all(&dir).map_err(|e| pod_dir_error(&name, &dir, e))?;
                self.materialize_projected(pod, namespace, &name, &dir, sources).await?;
                // Ensure the SA volume has a usable ca.crt even if the cluster
                // has no kube-root-ca.crt configMap to source it from.
                if let Some(ca) = &self.ca_pem {
                    let has_sat = sources.iter().any(|s| !s["serviceAccountToken"].is_null());
                    if has_sat {
                        crate::api_files::write_in(&dir, "ca.crt", ca, 0o644)
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
                                    if let Some(h) = pv["spec"]["csi"]["volumeHandle"].as_str() {
                                        self.record_pod_volume(uid, &name, h);
                                    }
                                    map.insert(
                                        name,
                                        ResolvedVolume {
                                            path: dir,
                                            fstype: None,
                                            block: false,
                                            api: false,
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
                let mut steps = crate::start_timing::ClaimSteps::default();
                let provisioned = self.provision_claim_timed(namespace, claim, uid, &mut steps).await;
                match provisioned {
                    Ok((vol_id, device, fstype)) => {
                        self.record_pod_volume(uid, &name, &vol_id);
                        claims.push((name.clone(), steps));
                        map.insert(
                            name,
                            ResolvedVolume {
                                path: device,
                                block: fstype.is_none(),
                                fstype: fstype.map(String::from),
                                api: false,
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
                                api: false,
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
                // 0777, as upstream: a container that runs as its image's
                // User or its runAsUser (#98) must be able to write it.
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777))
                        .map_err(|e| pod_dir_error(&name, &dir, e))?;
                }
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
                if kind == "gitRepo" {
                    // Removed upstream in 1.36 (#23); upstream's pod waits with
                    // FailedMount too.
                    return Err(CriError::VolumeNotReady(format!(
                        "volume {name} is of type gitRepo, which Kubernetes 1.36 removed; \
                         clone into an emptyDir from an init container instead"
                    )));
                }
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
            // What the kubelet writes from the API is never the container's
            // to write: see `ResolvedVolume::api`.
            let api = !vol["configMap"].is_null()
                || !vol["secret"].is_null()
                || vol["projected"]["sources"].is_array();
            map.insert(
                name,
                ResolvedVolume {
                    path: host_path,
                    fstype: None,
                    block: false,
                    api,
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
                    })).send_retrying(retry::Policy::API).await {
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

    /// Built-in claims asked onto another node (#338): see
    /// [`crate::claim_move`]. Each claim is moved by the node whose storage
    /// holds it. `Some(wait)` while a move is in flight.
    pub async fn move_claims(&self) -> Option<std::time::Duration> {
        use crate::claim_move as cm;
        if self.api_url.is_empty() {
            return None;
        }
        let list = self.api_get("/api/v1/persistentvolumeclaims").await?;
        let mut again = None;
        for pvc in list["items"].as_array().into_iter().flatten() {
            let m = cm::Move::of(pvc);
            if !m.any() {
                continue;
            }
            if self.move_claim(pvc, &m).await {
                again = Some(cm::POLL);
            }
        }
        again
    }

    /// One claim's move (#338). `true` while there is more to do.
    async fn move_claim(&self, pvc: &Value, m: &crate::claim_move::Move) -> bool {
        use crate::claim_move::{self as cm, Step};
        let here = self.node_name.as_str();
        let ns = pvc["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
        let claim = pvc["metadata"]["name"].as_str().unwrap_or("").to_string();
        let Some(pv_name) = pvc["spec"]["volumeName"].as_str().filter(|v| !v.is_empty()) else { return false };
        let Some(pv) = self.api_get(&format!("/api/v1/persistentvolumes/{pv_name}")).await else { return m.active() };
        if !cm::is_builtin(&pv) {
            return false;
        }
        // Whose is it: the node driving a move in flight, else the PV's.
        let mine = if m.active() { m.from == here } else { cm::pv_node(&pv).as_deref() == Some(here) };
        if !mine {
            return false;
        }
        match cm::step(m, here) {
            Step::Nothing => false,
            Step::Abort(why) => {
                self.abort_claim_move(pvc, m, cm::ABORTED, &why).await;
                false
            }
            Step::Start(to) => {
                self.start_claim_move(pvc, &pv, &ns, &claim, &to, m.set.as_deref()).await
            }
            Step::Carry => self.carry_claim_move(pvc, &pv, &ns, &claim, m).await,
        }
    }

    /// Write the PVC's move annotations.
    async fn mark_claim(&self, pvc: &Value, pairs: &[(&str, Option<&str>)]) -> bool {
        let ns = pvc["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pvc["metadata"]["name"].as_str().unwrap_or("");
        let url = format!("{}/api/v1/namespaces/{ns}/persistentvolumeclaims/{name}", self.api_url);
        let ok = matches!(
            self.api_client.patch(&url).header("content-type", "application/merge-patch+json")
                .json(&crate::claim_move::annotations(pairs)).send_retrying(retry::Policy::API).await,
            Ok(r) if r.status().is_success()
        );
        if !ok {
            warn!("PVC {ns}/{name}: the move's state was not written; retried");
        }
        ok
    }

    /// A move given up: the copy aborted (if there is one), the claim where
    /// it was, `state` and why on the PVC.
    async fn abort_claim_move(&self, pvc: &Value, m: &crate::claim_move::Move, state: &str, why: &str) -> bool {
        use crate::claim_move as cm;
        let ns = pvc["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pvc["metadata"]["name"].as_str().unwrap_or("");
        let mut reason = why.to_string();
        if !m.migration.is_empty() {
            if let Some(ss) = &self.stormstorage {
                if let Err(e) = crate::vm_migration::Stormstorage(ss).step(&m.migration, "abort").await {
                    if !e.contains("404") {
                        warn!("PVC {ns}/{name}: abort of {} refused, retried: {e}", m.migration);
                        return false;
                    }
                }
            }
            reason.push_str(&format!("; the copy is aborted, the claim stays on {}", self.node_name));
        }
        self.claim_event(pvc, "Warning", "MoveFailed", &reason).await;
        self.mark_claim(pvc, &[(cm::STATE, Some(state)), (cm::REASON, Some(&reason)), (cm::PROGRESS, None)]).await
    }

    async fn start_claim_move(&self, pvc: &Value, pv: &Value, ns: &str, claim: &str, to: &str, set: Option<&str>) -> bool {
        use crate::claim_move as cm;
        let here = self.node_name.as_str();
        let refuse = |why: String| async move {
            self.claim_event(pvc, "Warning", "MoveRefused", &why).await;
            self.mark_claim(pvc, &[
                (cm::STATE, Some(cm::REFUSED)), (cm::REASON, Some(&why)), (cm::PROGRESS, None),
                (cm::TARGET, Some(to)), (cm::FROM, Some(here)), (cm::MIGRATION, None),
            ]).await;
            false
        };
        // The facts the refusals need.
        let mut facts = cm::Facts {
            size: pv["spec"]["capacity"]["storage"].as_str().and_then(crate::storage::parse_quantity).unwrap_or(0),
            stormstorage: self.stormstorage.is_some(),
            ..Default::default()
        };
        let Some(pods) = self.api_get(&format!("/api/v1/namespaces/{ns}/pods")).await else { return true };
        let mut elsewhere: std::collections::BTreeSet<String> = Default::default();
        for p in pods["items"].as_array().into_iter().flatten() {
            let uses = p["spec"]["volumes"].as_array().into_iter().flatten()
                .any(|v| v["persistentVolumeClaim"]["claimName"].as_str() == Some(claim));
            let done = matches!(p["status"]["phase"].as_str(), Some("Succeeded" | "Failed"));
            if let Some(n) = p["spec"]["nodeName"].as_str().filter(|n| uses && !done && !n.is_empty() && *n != self.node_name) {
                elsewhere.insert(n.to_string());
            }
        }
        facts.used_elsewhere = elsewhere.into_iter().collect();
        facts.target_ready = match self.api_get_checked(&format!("/api/v1/nodes/{to}")).await {
            Ok(None) => None,
            Ok(Some(n)) => Some(n["status"]["conditions"].as_array().into_iter().flatten()
                .any(|c| c["type"] == "Ready" && c["status"] == "True")),
            Err(_) => return true,
        };
        facts.target_room = self
            .api_get(&format!("/apis/storage.k8s.io/v1/namespaces/kube-system/csistoragecapacities/stormblock-{to}"))
            .await
            .and_then(|c| {
                c["maximumVolumeSize"].as_str().or(c["capacity"].as_str()).and_then(crate::storage::parse_quantity)
            });
        if let Some(why) = cm::refusal(pvc, pv, to, &facts) {
            return refuse(why).await;
        }
        let handle = pv["spec"]["csi"]["volumeHandle"].as_str().unwrap_or("");
        let id = self.storage_get("/api/v1/volumes").await.and_then(|l| {
            l["items"].as_array()?.iter().find(|v| v["name"].as_str() == Some(handle))?["id"].as_str().map(String::from)
        });
        let Some(id) = id else {
            return refuse(format!("this node's storage has no volume {handle}")).await;
        };
        let ss = self.stormstorage.as_ref().expect("refused above without one");
        // A copy already under way for this volume is this move's, started by
        // a pass whose PVC write was lost (or before a kubelet restart): it is
        // recorded, never started twice (#437, 0417958a2e64). Not knowing is
        // asked again next pass, rather than a second start.
        let recorded = |mig: crate::vm_migration::DiskMigration| async move {
            let progress = cm::progress(mig.bytes_total, mig.bytes_remaining);
            // Until this is written, every pass finds the copy here again.
            if !self.mark_claim(pvc, &[
                (cm::STATE, Some(cm::COPYING)), (cm::REASON, None), (cm::PROGRESS, Some(&progress)),
                (cm::MIGRATION, Some(&mig.id)), (cm::TARGET, Some(to)), (cm::FROM, Some(here)),
            ]).await {
                warn!("PVC {ns}/{claim}: migration {} not recorded on the claim yet; it is found again next pass", mig.id);
            }
            true
        };
        match crate::vm_migration::Stormstorage(ss).live_for(here, &id).await {
            // A copy that failed still holds the volume in stormstorage, and
            // nothing will carry it: aborted (its target deleted, the source
            // as it was), then a new one starts.
            Ok(Some(mig)) if mig.state == "failed" => {
                if let Err(e) = crate::vm_migration::Stormstorage(ss).step(&mig.id, "abort").await {
                    warn!("PVC {ns}/{claim}: failed migration {} of {handle} not aborted, asked again: {e}", mig.id);
                    return true;
                }
                info!("PVC {ns}/{claim}: aborted failed migration {} of {handle} before a new one", mig.id);
            }
            Ok(Some(mig)) if mig.target_node == to => {
                info!("PVC {ns}/{claim}: migration {} of {handle} to {to} is already under way: recorded", mig.id);
                return recorded(mig).await;
            }
            Ok(Some(mig)) => {
                return refuse(format!(
                    "{handle} is already being copied to {} by migration {} ({}); abort it in stormstorage first",
                    mig.target_node, mig.id, mig.state
                ))
                .await;
            }
            Ok(None) => {}
            Err(e) => {
                warn!("PVC {ns}/{claim}: could not ask stormstorage for {handle}'s migrations, asked again: {e}");
                return true;
            }
        }
        let placement = set.map(|s| serde_json::json!({ "array": s }));
        match crate::vm_migration::Stormstorage(ss).start_on(here, &id, to, placement).await {
            Ok(mig) => {
                info!("PVC {ns}/{claim}: moving {handle} to {to}: migration {}", mig.id);
                self.claim_event(pvc, "Normal", "Moving", &format!("copying {handle} to {to}")).await;
                recorded(mig).await
            }
            Err(e) => {
                self.claim_event(pvc, "Warning", "MoveFailed", &e).await;
                self.mark_claim(pvc, &[
                    (cm::STATE, Some(cm::FAILED)), (cm::REASON, Some(&e)), (cm::PROGRESS, None),
                    (cm::TARGET, Some(to)), (cm::FROM, Some(here)), (cm::MIGRATION, None),
                ]).await;
                false
            }
        }
    }

    /// A move in flight, one step further: progress; `synced`; once no pod
    /// here holds the claim, the cutover, the source retired and the PV
    /// pointed at the target.
    async fn carry_claim_move(&self, pvc: &Value, pv: &Value, ns: &str, claim: &str, m: &crate::claim_move::Move) -> bool {
        use crate::claim_move as cm;
        use crate::vm_migration::Stormstorage;
        let Some(ss) = self.stormstorage.as_ref() else {
            return self.abort_claim_move(pvc, m, cm::FAILED, "this node has no stormstorage (--stormstorage) any more").await;
        };
        let progress_now = pvc["metadata"]["annotations"][cm::PROGRESS].as_str().unwrap_or("");
        let mig = match Stormstorage(ss).get(&m.migration).await {
            Ok(mig) => mig,
            Err(e) if e.contains("404") && m.state != cm::CUTTING_OVER => {
                let why = format!("stormstorage no longer has migration {}", m.migration);
                self.abort_claim_move(pvc, &cm::Move { migration: String::new(), ..m.clone() }, cm::FAILED, &why).await;
                return false;
            }
            Err(e) => {
                warn!("PVC {ns}/{claim}: move {}: {e}", m.migration);
                return true;
            }
        };
        match mig.state.as_str() {
            "failed" | "aborted" => {
                let why = format!("the copy {}: {}", mig.state, mig.message);
                self.abort_claim_move(pvc, &cm::Move { migration: String::new(), ..m.clone() }, cm::FAILED, &why).await;
                return false;
            }
            "synced" | "cut_over" | "completed" => {}
            _ => {
                let p = cm::progress(mig.bytes_total, mig.bytes_remaining);
                if p != progress_now {
                    self.mark_claim(pvc, &[(cm::PROGRESS, Some(&p))]).await;
                }
                return true;
            }
        }
        // Where the migration is: what was read, or what a cutover whose
        // answer was lost turned out to have done.
        let mut state = mig.state.clone();
        if mig.state == "synced" {
            if m.state == cm::COPYING {
                info!("PVC {ns}/{claim}: in sync with {}", m.target);
                self.claim_event(pvc, "Normal", "MoveSynced", &format!("in sync with {}: the pod may stop", m.target)).await;
                if !self.mark_claim(pvc, &[(cm::STATE, Some(cm::SYNCED)), (cm::PROGRESS, Some("in sync"))]).await {
                    return true;
                }
            }
            // The cutover waits for every pod here to let the claim go, and
            // holds its start out while it runs.
            match self.claim_running_here(ns, claim).await {
                Ok(Some(pod)) => {
                    let p = format!("in sync; the cutover waits for pod {ns}/{pod} to stop");
                    if p != progress_now {
                        self.mark_claim(pvc, &[(cm::PROGRESS, Some(&p))]).await;
                    }
                    return true;
                }
                Ok(None) => {}
                Err(e) => {
                    debug!("PVC {ns}/{claim}: holders not known, the cutover waits: {e}");
                    return true;
                }
            }
            let _guard = match &self.admission {
                Some(a) => match a.reclaim(ns, claim) {
                    Some(g) => Some(g),
                    None => return true,
                },
                None => None,
            };
            if m.state != cm::CUTTING_OVER
                && !self.mark_claim(pvc, &[(cm::STATE, Some(cm::CUTTING_OVER)), (cm::PROGRESS, Some("cutting over"))]).await
            {
                return true;
            }
            let handle = pv["spec"]["csi"]["volumeHandle"].as_str().unwrap_or("");
            let id = self.storage_get("/api/v1/volumes").await.and_then(|l| {
                l["items"].as_array()?.iter().find(|v| v["name"].as_str() == Some(handle))?["id"].as_str().map(String::from)
            });
            if let Some(id) = id {
                if let Err(e) = self.storage_delete(&format!("/api/v1/volumes/{id}/attach")).await {
                    warn!("PVC {ns}/{claim}: {handle} is still attached here, the cutover waits: {e}");
                    return true;
                }
            }
            match Stormstorage(ss).step(&m.migration, "cutover").await {
                Ok(c) if c.state == "cut_over" => {}
                Ok(c) => {
                    warn!("PVC {ns}/{claim}: the cutover left {} {} ({}), tried again", m.migration, c.state, c.message);
                    return true;
                }
                // The cutover is the point of no return, and an error says
                // nothing about which side of it the copy is on: the answer
                // may be what was lost (#428, finding e4aaa109678b). The
                // migration is read again and the move goes on from there.
                Err(e) => match cutover_outcome(&e, Stormstorage(ss).get(&m.migration).await) {
                    Cutover::Done(now) => {
                        info!("PVC {ns}/{claim}: the cutover's answer was lost ({e}); {} is {now}", m.migration);
                        state = now;
                    }
                    Cutover::Ask => {
                        warn!("PVC {ns}/{claim}: the cutover of {} is not known yet ({e}), asked again", m.migration);
                        return true;
                    }
                    Cutover::Refused(why) => {
                        self.abort_claim_move(pvc, m, cm::FAILED, &why).await;
                        return false;
                    }
                    Cutover::Ended(why) => {
                        self.abort_claim_move(pvc, &cm::Move { migration: String::new(), ..m.clone() }, cm::FAILED, &why).await;
                        return false;
                    }
                },
            }
        }
        // Cut over: the copy is the claim's. Retire the source, then point
        // the PV at the target, then say so.
        if state != "completed" {
            let done = match Stormstorage(ss).step(&m.migration, "complete").await {
                Ok(c) => c.state == "completed",
                Err(e) => {
                    warn!("PVC {ns}/{claim}: the source is not retired yet, retried: {e}");
                    false
                }
            };
            if !done {
                return true;
            }
        }
        let url = format!("{}/api/v1/persistentvolumes/{}", self.api_url, pv["metadata"]["name"].as_str().unwrap_or(""));
        let rebound = matches!(
            self.api_client.patch(&url).header("content-type", "application/merge-patch+json")
                .json(&cm::rebind_patch(pv, &m.target)).send_retrying(retry::Policy::API).await,
            Ok(r) if r.status().is_success()
        );
        if !rebound {
            warn!("PVC {ns}/{claim}: the PV was not pointed at {} yet, retried", m.target);
            return true;
        }
        info!("PVC {ns}/{claim}: on {}'s storage", m.target);
        self.claim_event(pvc, "Normal", "Moved", &format!("the claim is on {}'s storage", m.target)).await;
        !self.mark_claim(pvc, &[(cm::STATE, Some(cm::COMPLETE)), (cm::PROGRESS, None), (cm::REASON, None)]).await
    }

    /// A pod on this node that runs on `claim` (#338): one started here, or
    /// Running in the API (adopted after a restart). Unlike
    /// [`Self::claim_holder_here`], a Pending pod scheduled here is not one:
    /// a replacement waiting for the cutover must not hold it up. A pod part
    /// way through its start holds the claim's admission, which the cutover
    /// takes ([`crate::workload::Reservations::reclaim`]).
    async fn claim_running_here(&self, namespace: &str, claim: &str) -> Result<Option<String>, String> {
        if let Some(holder) = self.other_pod_holding(namespace, claim, "").await {
            return Ok(Some(holder));
        }
        if self.api_url.is_empty() {
            return Ok(None);
        }
        let pods = self
            .api_get(&format!("/api/v1/namespaces/{namespace}/pods"))
            .await
            .ok_or_else(|| "the apiserver pod list is unavailable".to_string())?;
        Ok(pods["items"].as_array().into_iter().flatten()
            .find(|p| {
                p["spec"]["nodeName"].as_str() == Some(&self.node_name)
                    && p["status"]["phase"].as_str() == Some("Running")
                    && pod_claims(p).any(|c| c == claim)
            })
            .and_then(|p| p["metadata"]["name"].as_str())
            .map(String::from))
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
    #[cfg(test)]
    async fn provision_claim(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
    ) -> Result<(String, Option<&'static str>), ClaimError> {
        self.provision_claim_volume(namespace, claim, pod_uid, &mut Default::default(), false)
            .await
            .map(|(_, dev, fs)| (dev, fs))
    }

    /// [`Self::provision_claim`], with its steps (#95).
    async fn provision_claim_timed(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
        steps: &mut crate::start_timing::ClaimSteps,
    ) -> Result<(String, String, Option<&'static str>), ClaimError> {
        self.provision_claim_volume(namespace, claim, pod_uid, steps, true).await
    }

    /// A claim as a block device on this node, for a virtual machine's disk
    /// (#74). The same path a pod's claim takes: a bound claim gets its own
    /// volume, and an unbound claim of the built-in class is provisioned.
    ///
    /// Returns the volume id and the attached device. The VM manager records
    /// the id, to detach it when the machine stops. The claim outlives the
    /// machine, so the id is never the VM's to delete.
    ///
    /// A claim bound to another CSI driver's PV (#157: a stormblock-csi claim,
    /// a RAID across servers) is staged and published by that driver as a raw
    /// block for VMI `vmi_uid`'s disk `disk`: no volume id (nothing of the
    /// engine's to detach), the published path as the device. The VM manager
    /// unpublishes it ([`Self::teardown_vm_csi`]) when the machine stops.
    ///
    /// Refused while a pod on this node uses the claim. A pod mounts the
    /// claim's filesystem and a VM writes the raw device, and two writers on
    /// one ext4 is corruption, whatever the access mode says.
    pub(crate) async fn claim_for_vm(
        &self,
        namespace: &str,
        claim: &str,
        vmi_uid: &str,
        disk: &str,
        readonly: bool,
    ) -> Result<(Option<String>, String), String> {
        match self.claim_holder_here(namespace, claim).await {
            Ok(Some(pod)) => {
                return Err(format!(
                    "claim {namespace}/{claim} is in use by pod {namespace}/{pod} on this node"
                ))
            }
            Ok(None) => {}
            Err(e) => return Err(format!("claim {namespace}/{claim}: {e}")),
        }
        let explain = |e: ClaimError| match e {
            ClaimError::NotOurs(why) => format!("waiting for claim {claim} to bind: {why}"),
            ClaimError::InUse(why) => why,
            ClaimError::Failed(why) => format!("claim {namespace}/{claim}: {why}"),
        };
        if let Some(pvc) = self
            .api_get(&format!("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{claim}"))
            .await
        {
            if let Some(pv) = self.external_csi_pv(&pvc).await {
                return self
                    .mount_csi_block_for_vm(namespace, vmi_uid, disk, &pvc, &pv, readonly)
                    .await
                    .map(|device| (None, device))
                    .map_err(|e| match e {
                        // Another driver's claim that is not a raw block is
                        // not a wait: it will not become one.
                        ClaimError::NotOurs(why) => format!("claim {namespace}/{claim}: {why}"),
                        e => explain(e),
                    });
            }
        }
        self.provision_claim_volume(namespace, claim, "", &mut Default::default(), false)
            .await
            .map(|(id, dev, _)| (Some(id), dev))
            .map_err(explain)
    }

    /// Unpublish (and unstage, when no one else here has it) VMI `vmi_uid`'s
    /// disks on other drivers' claims (#157). `true` when nothing is left.
    pub(crate) async fn teardown_vm_csi(&self, vmi_uid: &str) -> bool {
        self.teardown_csi_volumes(&csi_volumes::vm_holder(vmi_uid)).await
    }

    /// The one disk `disk` of VMI `vmi_uid` on another driver's claim,
    /// unpublished (and unstaged when no one else here has it): a disk taken
    /// out of a running VM (#247). `true` when nothing is left.
    pub(crate) async fn teardown_vm_csi_disk(&self, vmi_uid: &str, disk: &str) -> bool {
        self.teardown_csi_volume(&csi_volumes::vm_holder(vmi_uid), disk).await
    }

    /// The VMIs with CSI disks recorded on this node (#157): for the VM
    /// manager's sweep of machines that are gone. `Err` when the records
    /// cannot be read (nothing is torn down then).
    pub(crate) fn vm_csi_holders(&self) -> std::io::Result<Vec<String>> {
        csi_volumes::vm_holders(&self.state_root)
    }

    /// [`Self::provision_claim`], answering the volume id too, and the
    /// filesystem the device carries: `None` for a raw block claim (#67).
    async fn provision_claim_volume(
        &self,
        namespace: &str,
        claim: &str,
        pod_uid: &str,
        steps: &mut crate::start_timing::ClaimSteps,
        bind_later: bool,
    ) -> Result<(String, String, Option<&'static str>), ClaimError> {
        let name = crate::storage::volume_name(namespace, claim);
        // Each step timed for the pod's start timing (#95).
        let mut step = Instant::now();

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
        //
        // The PV and the engine's volume list are read once each, together
        // (#95): this was the PV twice and the whole list up to three times,
        // one after another, and under the Dell's I/O stress each took
        // seconds (claim/<v>/lookup 10.6 s on 12.37).
        let pv_name = pvc["spec"]["volumeName"].as_str().filter(|v| !v.is_empty());
        let (pv, volumes) = tokio::join!(
            async {
                match pv_name {
                    Some(pv) => self.api_get(&format!("/api/v1/persistentvolumes/{pv}")).await,
                    None => None,
                }
            },
            self.storage_get("/api/v1/volumes"),
        );
        // A claim being cut over, or moved to another node's storage
        // (#338): never a fresh volume here beside the real one.
        if let Some(why) = crate::claim_move::start_hold(&pvc, pv.as_ref(), &self.node_name) {
            return Err(ClaimError::InUse(why));
        }
        if let Some(pv) = &pv {
            if pv["metadata"]["labels"][crate::system_claims::LABEL].as_str() == Some("true") {
                return Err(ClaimError::InUse(format!(
                    "{namespace}/{claim} is a node service's live data volume; mount a clone \
                     of it (a claim with dataSource naming it) rather than the volume itself"
                )));
            }
        }
        let volume_id = |wanted: &str| -> Option<String> {
            volumes.as_ref()?["items"]
                .as_array()?
                .iter()
                .find(|v| v["name"].as_str() == Some(wanted))?["id"]
                .as_str()
                .map(String::from)
        };

        // A claim already bound to a stormblock PV mounts *that* volume. The
        // node's own data containers are claims like this (system_claims.rs),
        // and so is any volume an administrator publishes by hand: deriving a
        // name here instead would clone a new, empty volume beside the real one.
        let bound = pv
            .as_ref()
            .filter(|pv| pv["spec"]["csi"]["driver"].as_str() == Some("stormblock.storm.io"))
            .and_then(|pv| pv["spec"]["csi"]["volumeHandle"].as_str())
            .and_then(|handle| volume_id(handle));
        let existing = bound.or_else(|| volume_id(&name));
        // Whether the engine already says this claim owns it (#372): read
        // here, from the listing in hand, so a claim owned once costs no
        // write on every start.
        let claim_uid = pvc["metadata"]["uid"].as_str().unwrap_or("").to_string();
        let owned = existing.as_deref().is_some_and(|id| {
            volumes.as_ref().and_then(|l| l["items"].as_array()).is_some_and(|items| {
                items.iter().any(|v| {
                    v["id"].as_str() == Some(id)
                        && v["owner"]["kind"] == "PersistentVolumeClaim"
                        && v["owner"]["name"].as_str() == Some(claim)
                        && (claim_uid.is_empty() || v["owner"]["uid"].as_str() == Some(&claim_uid))
                })
            })
        });
        // **A new volume is charged its class** (#62, #108): refused here as
        // well as by the scheduler, which static pods and pods written onto
        // `spec.nodeName` never meet. Held until it is made. An engine that
        // cannot list its slabs (older than the slab API) is not checked.
        // The class's policy (#71), for a volume made here from nothing (a
        // clone of a source keeps its source's). Refused or unreadable: the
        // claim waits, and says so on itself.
        //
        // The class is read while the lock is waited for and the room read
        // (#95): neither needs the other.
        let room_step = Instant::now();
        let (policy, room) = if existing.is_none() {
            let wants_policy = crate::storage::claim_source(&pvc).is_none();
            let (policy, (guard, capacity)) = tokio::join!(
                async {
                    if wants_policy {
                        self.claim_policy(&pvc).await
                    } else {
                        Ok(crate::storage::ClaimPolicy::default())
                    }
                },
                async {
                    let guard = self.capacity_lock.lock().await;
                    let capacity = crate::capacity::read(&self.engine, &self.storage_policy).await;
                    (guard, capacity)
                },
            );
            let policy = match policy {
                Ok(p) => p,
                Err(e) => {
                    drop(guard);
                    self.claim_event(&pvc, "Warning", "ProvisioningFailed", &e.to_string()).await;
                    return Err(e);
                }
            };
            (policy, Some((guard, capacity)))
        } else {
            (crate::storage::ClaimPolicy::default(), None)
        };
        let _room = if let Some((guard, capacity)) = room {
            match capacity {
                Ok(c) if c.total > 0 => {
                    // Charged what its policy costs on the drives: a mirror
                    // twice its class (#71).
                    let cost = (class_bytes as f64 * policy.scheme.overhead()).ceil() as u64;
                    if let Some(why) = c.refusal(class, cost, &self.storage_policy) {
                        return Err(ClaimError::Failed(format!("claim {namespace}/{claim}: {why}")));
                    }
                }
                Ok(_) => debug!("no data slab reported; claim {namespace}/{claim} not checked for room"),
                Err(e) => debug!("claim {namespace}/{claim} not checked for room: {e}"),
            }
            Some(guard)
        } else {
            None
        };
        if existing.is_none() {
            steps.room = Some(room_step.elapsed());
        }
        steps.lookup = step.elapsed();
        step = Instant::now();
        let how = match &existing {
            None if crate::storage::claim_source(&pvc).is_some() => Some("clone"),
            None if block => Some("raw"),
            None => Some("blank"),
            Some(_) => None,
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
            None if block => self.raw_volume(&name, class, class_bytes, &policy).await?,
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
                // The class's tier goes on the clone (#400, stormblock#377):
                // the clone's own writes land there.
                self.clone_blank(class, &name, &pvc, &policy).await?
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
        steps.made = how.map(|h| (h, step.elapsed()));
        if !owned {
            self.own_claim_volume(&vol_id, namespace, claim, &claim_uid);
        }
        step = Instant::now();
        let attach = serde_json::json!({ "node": self.node_name, "transport": "ublk" });
        let info: Value = self
            .storage_post(&format!("/api/v1/volumes/{vol_id}/attach"), &attach)
            .await
            .map_err(|r| {
                ClaimError::Failed(format!(
                    "stormblock would not attach {name} as a local device: {r}"
                ))
            })?;
        steps.attach = step.elapsed();
        step = Instant::now();
        if let Some(dev) = info["device_hint"].as_str() {
            info!("PVC {namespace}/{claim} -> {name} ({class}) at {dev}");
            // Say so on the claim, now that there is something to point at.
            //
            // Here rather than earlier so the object never names a volume
            // that was not made, and after the attach so a bound claim means
            // storage a pod can actually use.
            if bind_later {
                // Off the start path (#95): the pod needs the device, not
                // the objects. Not timed; its own log line says when.
                let binder = self.binder();
                let (ns, c, v) = (namespace.to_string(), claim.to_string(), name.clone());
                tokio::spawn(async move { binder.bind(&ns, &c, &v, class_bytes).await });
                steps.bind = None;
            } else {
                self.bind_claim(namespace, claim, &name, class_bytes).await;
                steps.bind = Some(step.elapsed());
            }
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
    async fn raw_volume(
        &self,
        name: &str,
        class: &str,
        bytes: u64,
        policy: &crate::storage::ClaimPolicy,
    ) -> Result<String, ClaimError> {
        let mut body = serde_json::json!({
            "name": name,
            "size": crate::storage::engine_size(bytes),
            "role": "data",
        });
        // The class's policy (#71): redundancy and spread placed by the
        // engine (409 when it cannot), the tier as a preference.
        body.as_object_mut().expect("an object").extend(policy.engine_fields());
        if let Some(t) = &policy.tier {
            body["tier"] = serde_json::json!(t);
        }
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
    ///
    /// A template the engine already *lists* as `broken` takes the same path
    /// before any clone is tried (#383): only the formatting states wait, and
    /// a state this kubelet does not know is a failure named on the claim.
    async fn clone_blank(
        &self,
        class: &str,
        name: &str,
        claim: &Value,
        policy: &crate::storage::ClaimPolicy,
    ) -> Result<String, ClaimError> {
        // One blank per (size, fs, redundancy, spread) (#71): the clone
        // inherits the blank's policy.
        let blank = policy.blank_name(class);
        let template = match self.storage_template_status(&blank).await {
            Some(TemplateStatus::Ready(id)) => id,
            // A blank not sealed yet cannot be cloned. Formatting a
            // 1 TiB class takes minutes (stormblock#141), and the claim
            // waits with the state named rather than failing a clone.
            Some(TemplateStatus::Making(_, state)) => {
                self.claim_event(claim, "Normal", "Provisioning", &format!(
                    "waiting for the {class} blank {blank} to be formatted and sealed ({state})"
                )).await;
                return Err(ClaimError::Failed(format!(
                    "waiting for volume {name}: template {blank} {state}"
                )))
            }
            // Final (#383): nothing formats or seals a broken template, and
            // no clone is tried to be refused, so waiting was forever.
            Some(TemplateStatus::Broken(id, reason)) => {
                warn!("template {blank} is broken ({reason}); deleting it to mint it again");
                let again = self.remint(&blank, &id, class, policy).await;
                let why = format!("template {blank} is broken ({reason}); {again}");
                self.claim_event(claim, "Warning", "ProvisioningFailed", &why).await;
                return Err(ClaimError::Failed(format!("waiting for volume {name}: {why}")));
            }
            // Never a wait (#383): a state that means "never" would be
            // waited on forever, with nothing on the claim to say why.
            Some(TemplateStatus::Unknown(_, state)) => {
                let why = format!(
                    "template {blank} is in state {state:?}, which this kubelet does not know; \
                     not waiting on it (expected ready, {})",
                    TEMPLATE_MAKING.join(", ")
                );
                self.claim_event(claim, "Warning", "ProvisioningFailed", &why).await;
                return Err(ClaimError::Failed(format!("waiting for volume {name}: {why}")));
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
            None => match self.mint_template(&blank, class, policy).await {
                Ok(id) => id,
                Err(e) => {
                    // On the claim too (#70): `describe pvc` said nothing,
                    // and "a stormblock fault" in the console said less.
                    if mint_refused(&e) {
                        self.claim_event(claim, "Warning", "ProvisioningFailed", &e).await;
                    } else {
                        self.claim_event(claim, "Normal", "Provisioning", &format!(
                            "minting the {class} blank {blank}: one format per class on this node, \
                             then every claim of the class is a clone of it"
                        )).await;
                    }
                    return Err(ClaimError::Failed(format!("waiting for volume {name}: {e}")));
                }
            },
        };

        // Through the template, not the volume: `fstemplates/{id}/clone`
        // gives the clone its own filesystem UUID and verifies it, so
        // two claims never present one identity (stormblock#76).
        //
        // The class's tier rides on the clone (#400, stormblock#377, v24.30.0):
        // the clone's own writes go to that tier, while what it shares with
        // the blank stays where the blank is. The blank itself takes no tier,
        // so one blank serves every tier. An engine before v24.30.0 ignores
        // the field.
        let mut body = serde_json::json!({ "name": name, "verify": true });
        if let Some(t) = &policy.tier {
            body["tier"] = serde_json::json!(t);
        }
        let created = match self
            .storage_post(&format!("/api/v1/fstemplates/{template}/clone"), &body)
            .await
        {
            Ok(v) => v,
            // Gone between the listing and the clone (#383, after stormblock#663:
            // the engine keeps no broken templates, a template that is not whole is
            // deleted): mint it now, once, and clone when the mint wakes the claim.
            Err(r) if r.template_gone() => {
                warn!("template {blank} vanished before its clone ({r}); minting it again");
                let again = match self.mint_template(&blank, class, policy).await {
                    Ok(_) => "minted again, cloning on the next try".to_string(),
                    Err(e) => format!("minting it again ({e}), cloning when it is ready"),
                };
                let why = format!("template {blank} was gone when it was cloned; {again}");
                self.claim_event(claim, "Normal", "Provisioning", &why).await;
                return Err(ClaimError::Failed(format!("waiting for volume {name}: {why}")));
            }
            Err(r) if r.template_broken() => {
                warn!("template {blank} is broken ({r}); deleting it to mint it again");
                let again = self.remint(&blank, &template, class, policy).await;
                let why = format!("template {blank} is broken ({r}); {again}");
                self.claim_event(claim, "Warning", "ProvisioningFailed", &why).await;
                return Err(ClaimError::Failed(format!("waiting for volume {name}: {why}")));
            }
            Err(r) => {
                let why = format!("stormblock would not clone {blank} to {name}: {r}");
                self.claim_event(claim, "Warning", "ProvisioningFailed", &why).await;
                return Err(ClaimError::Failed(why))
            }
        };
        if created["volume_id"].is_string() {
            let tier = policy.tier.as_deref().map(|t| format!(", its writes on tier {t}")).unwrap_or_default();
            self.claim_event(claim, "Normal", "ProvisioningSucceeded", &format!(
                "volume {name} cloned from the {class} blank {blank}{tier}"
            )).await;
        }
        created["volume_id"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| {
                ClaimError::Failed(format!("clone of {blank} returned no volume: {created}"))
            })
    }

    /// Delete a broken template (by id) and mint it again (#140, #383).
    /// Answers what happened, for the claim's event: the mint runs in the
    /// background and its completion wakes the claim, which clones then.
    async fn remint(
        &self,
        blank: &str,
        id: &str,
        class: &str,
        policy: &crate::storage::ClaimPolicy,
    ) -> String {
        match self.storage_delete(&format!("/api/v1/fstemplates/{id}")).await {
            Ok(()) => match self.mint_template(blank, class, policy).await {
                Ok(_) => "deleted; minted again, cloning on the next try".to_string(),
                Err(e) => format!("deleted; minted again ({e}), cloning when it is ready"),
            },
            Err(e) => format!("could not delete it: {e}"),
        }
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
    async fn mint_template(
        &self,
        blank: &str,
        class: &str,
        policy: &crate::storage::ClaimPolicy,
    ) -> Result<String, String> {
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
            let (blank, class, policy) = (blank.to_string(), class.to_string(), policy.clone());
            tokio::spawn(async move {
                let failed = mint_blank(&engine, &url, &blank, &class, &policy).await.err();
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
        match self.storage_template_status(blank).await {
            Some(TemplateStatus::Ready(id)) => Ok(id),
            Some(TemplateStatus::Broken(_, reason)) => {
                Err(format!("template {blank} broken ({reason})"))
            }
            Some(TemplateStatus::Making(_, state) | TemplateStatus::Unknown(_, state)) => {
                Err(format!("template {blank} {state}"))
            }
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
        if let crate::storage::ClaimSource::Snapshot(snapshot) = &source {
            return self.restore_claim_snapshot(namespace, pvc, name, snapshot).await;
        }
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
            crate::storage::ClaimSource::Snapshot(_) => unreachable!("restored above"),
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

    /// A claim whose `dataSource` is a `VolumeSnapshot` of this class (#379):
    /// a new volume from the snapshot's group in this node's engine, at the
    /// snapshot's size (`pvc_resize` grows it to the claim's request once it
    /// is bound). A claim asking for less than the snapshot is refused, as
    /// upstream's provisioner refuses it.
    async fn restore_claim_snapshot(
        &self,
        namespace: &str,
        pvc: &Value,
        name: &str,
        snapshot: &str,
    ) -> Result<String, ClaimError> {
        let claim = pvc["metadata"]["name"].as_str().unwrap_or_default();
        let groups = crate::pvc_snapshot::Groups::new(&self.engine);
        let found = crate::pvc_snapshot::source(&self.api_client, &self.api_url, &groups, namespace, snapshot, &self.node_name)
            .await
            .map_err(ClaimError::Failed)?;
        let size: u64 = found.group.snapshots.iter().map(|s| s.size_bytes).sum();
        let want = crate::storage::claim_bytes(pvc);
        if want < size {
            let why = format!(
                "requested volume size {want} is less than the size {size} for the source snapshot {snapshot}"
            );
            self.claim_event(pvc, "Warning", "ProvisioningFailed", &why).await;
            return Err(ClaimError::Failed(why));
        }
        // An earlier pass that restored and then failed: the volume by name.
        if let Some(id) = self.storage_volume_id(name).await {
            return Ok(id);
        }
        let restore = || async {
            groups.restore(name, &found.group).await.map_err(|e| {
                ClaimError::Failed(format!("restoring VolumeSnapshot {namespace}/{snapshot} into {name}: {e}"))
            })
        };
        let v1 = restore().await?;
        // The restore answers `/v1`'s id; attach, owner and every later call
        // take the engine's (the #344 lesson): the volume by name. `/v1` is
        // idempotent by name, so a record left by an earlier claim of this
        // name whose volume was deleted under it is handed back empty (#344,
        // runs 2b38113e35 / 7cb87fb2e3): deleted, and restored once more.
        let id = match self.storage_volume_id(name).await {
            Some(id) => id,
            None => {
                warn!("claim {namespace}/{claim}: the restore answered {v1}, with no volume named {name}: a stale /v1 record; deleting it and restoring again");
                self.storage_delete(&format!("/v1/volumes/{v1}")).await.map_err(|e| {
                    ClaimError::Failed(format!("restoring into {name}: deleting the stale /v1 record {v1}: {e}"))
                })?;
                restore().await?;
                self.storage_volume_id(name).await.ok_or_else(|| {
                    ClaimError::Failed(format!("restored {name} from {snapshot}, but the engine does not list it by name"))
                })?
            }
        };
        info!("claim {namespace}/{claim}: restored from VolumeSnapshot {snapshot} (content {}, group {})", found.content, found.group.id);
        self.claim_event(pvc, "Normal", "Restored", &format!(
            "restored from VolumeSnapshot {snapshot} ({size} bytes; grown to the request once bound)"
        )).await;
        Ok(id)
    }

    /// A filesystem template by name, sorted into what a claim does with it
    /// ([`TemplateStatus`]). An engine that reports no state is taken as
    /// ready, which is what every template it answered for was before states
    /// existed.
    async fn storage_template_status(&self, name: &str) -> Option<TemplateStatus> {
        let t: Value = self
            .storage_get(&format!("/api/v1/fstemplates/{name}"))
            .await?;
        TemplateStatus::of(&t)
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
    /// The sources are fetched at once, not one after another (#340): each
    /// writes its own paths, and on a worker every fetch crosses to the
    /// apiserver, so kube-api-access's token and kube-root-ca.crt took two
    /// round trips in a row (328 ms of a worker's first pod).
    async fn materialize_projected(
        &self,
        pod: &Value,
        namespace: &str,
        volume: &str,
        dir: &str,
        sources: &[Value],
    ) -> Result<(), CriError> {
        futures::future::try_join_all(
            sources.iter().map(|src| self.materialize_projected_source(pod, namespace, volume, dir, src)),
        )
        .await?;
        Ok(())
    }

    /// One source of a projected volume (see `materialize_projected`). A
    /// configMap or secret source follows the volume's rules (#437,
    /// `keyed_files`): its bytes, its `items`, and a missing required object
    /// or key is a pod that waits.
    async fn materialize_projected_source(
        &self,
        pod: &Value,
        namespace: &str,
        volume: &str,
        dir: &str,
        src: &Value,
    ) -> Result<(), CriError> {
        let io = |e| pod_dir_error(volume, dir, e);
        if let Some(sat) = src.get("serviceAccountToken").filter(|v| !v.is_null()) {
            let path = sat["path"].as_str().unwrap_or("token");
            let aud = sat["audience"].as_str();
            // Bound to the pod, for the projection's own lifetime
            // (upstream's default 3600 s), refreshed at 80% (#122).
            let secs = sat["expirationSeconds"].as_i64().unwrap_or(3600).max(600);
            self.write_bound_token(pod, dir, path, aud, secs).await.map_err(io)?;
        } else if src["configMap"].is_object() || src["secret"].is_object() {
            let (kind, s) = if src["configMap"].is_object() {
                (KeyedKind::ConfigMap, &src["configMap"])
            } else {
                (KeyedKind::Secret, &src["secret"])
            };
            let obj = s["name"].as_str().unwrap_or("");
            let what = kind.describe(namespace, obj);
            let not_ready = |e: String| CriError::VolumeNotReady(format!("volume {volume}: {e}"));
            let mut data = self.fetch_keyed(kind, namespace, obj).await.map_err(not_ready)?;
            // kube-api-access names kube-root-ca.crt, which a cluster may not
            // have published yet: the kubelet's own CA is that ConfigMap's
            // content, so a pod does not wait for it.
            if data.is_none() && kind == KeyedKind::ConfigMap && obj == "kube-root-ca.crt" {
                if let Some(ca) = &self.ca_pem {
                    data = Some(KeyedData::from([("ca.crt".to_string(), ca.to_vec())]));
                }
            }
            // A projected volume's mode is the volume's defaultMode, 0644.
            let files = keyed_files(&what, data.as_ref(), s, 0o644).map_err(not_ready)?;
            write_keyed(dir, &files, false).map_err(io)?;
        } else if let Some(dw) = src.get("downwardAPI").filter(|v| !v.is_null()) {
            if let Some(items) = dw["items"].as_array() {
                for it in items {
                    let path = it["path"].as_str().unwrap_or("");
                    if let Some(fp) = it["fieldRef"]["fieldPath"].as_str() {
                        if let Some(val) = self.downward_field(pod, fp, None) {
                            crate::api_files::write_in(dir, path, val.as_bytes(), 0o644).map_err(io)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A ServiceAccount token via the TokenRequest API (best-effort; `None`
    /// if the apiserver doesn't support it or the call fails), bound to `pod`
    /// (`kind: Pod`, name, uid) for `expiration_secs` (#122, rustkube#182),
    /// and when it expires: what upstream's kubelet asks
    /// for, so the apiserver can tie the token to the pod's life and a
    /// TokenReview names the pod.
    async fn request_bound_token(
        &self,
        namespace: &str,
        sa: &str,
        audience: Option<&str>,
        pod: Option<(&str, &str)>,
        expiration_secs: Option<i64>,
    ) -> Option<(String, Option<chrono::DateTime<chrono::Utc>>)> {
        if self.api_url.is_empty() {
            return None;
        }
        let body = token_request(audience, pod, expiration_secs);
        let url = format!(
            "{}/api/v1/namespaces/{namespace}/serviceaccounts/{sa}/token",
            self.api_url
        );
        let resp = self.api_client.post(&url).json(&body).send_repeatable(retry::Policy::API).await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let v: Value = resp.json().await.ok()?;
        let token = v["status"]["token"].as_str()?.to_string();
        let expires = v["status"]["expirationTimestamp"]
            .as_str()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&chrono::Utc));
        Some((token, expires))
    }

    /// Write a bound token for `pod` into `<dir>/<path>` and remember it, so
    /// the refresher writes it again at 80% of its life (#122).
    async fn write_bound_token(
        &self,
        pod: &Value,
        dir: &str,
        path: &str,
        audience: Option<&str>,
        expiration_secs: i64,
    ) -> std::io::Result<bool> {
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let sa = pod["spec"]["serviceAccountName"].as_str().unwrap_or("default");
        let bound = (!name.is_empty() && !uid.is_empty()).then_some((name, uid));
        let Some((token, expires)) = self
            .request_bound_token(namespace, sa, audience, bound, Some(expiration_secs))
            .await
        else {
            return Ok(false);
        };
        crate::api_files::write_in(dir, path, token.as_bytes(), 0o644)?;
        let now = chrono::Utc::now();
        let f = TokenFile {
            file: format!("{dir}/{path}"),
            namespace: namespace.to_string(),
            sa: sa.to_string(),
            audience: audience.map(String::from),
            pod: bound.map(|(n, u)| (n.to_string(), u.to_string())),
            expiration_secs,
            refresh_at: refresh_at(now, expires, expiration_secs),
        };
        let mut t = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
        let list = t.entry(uid.to_string()).or_default();
        list.retain(|x| x.file != f.file);
        list.push(f);
        Ok(true)
    }

    /// Write again every token past 80% of its life (#122), and forget those
    /// of pods this kubelet no longer has. Answers when the next one is due.
    pub async fn refresh_tokens(&self) -> Option<std::time::Duration> {
        let known: std::collections::HashSet<String> = self.pods.read().await.keys().cloned().collect();
        let due: Vec<TokenFile> = {
            let mut t = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
            t.retain(|uid, _| known.contains(uid));
            let now = chrono::Utc::now();
            t.values().flatten().filter(|f| f.refresh_at <= now).cloned().collect()
        };
        for f in due {
            let bound = f.pod.as_ref().map(|(n, u)| (n.as_str(), u.as_str()));
            match self
                .request_bound_token(&f.namespace, &f.sa, f.audience.as_deref(), bound, Some(f.expiration_secs))
                .await
            {
                Some((token, expires)) => {
                    if let Err(e) =
                        crate::api_files::write_atomic(std::path::Path::new(&f.file), token.as_bytes(), 0o644)
                    {
                        warn!(file = %f.file, "ServiceAccount token not refreshed: {e}");
                        continue;
                    }
                    let next = refresh_at(chrono::Utc::now(), expires, f.expiration_secs);
                    let mut t = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
                    for x in t.values_mut().flatten().filter(|x| x.file == f.file) {
                        x.refresh_at = next;
                    }
                    debug!(file = %f.file, "ServiceAccount token refreshed");
                }
                // Tried again on the next pass; the old token is still good
                // for the remaining fifth of its life.
                None => warn!(file = %f.file, "ServiceAccount token refresh refused; retried"),
            }
        }
        let t = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now();
        t.values()
            .flatten()
            .map(|f| (f.refresh_at - now).to_std().unwrap_or_default())
            .min()
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

    /// The cluster DNS servers and domain a ClusterFirst pod gets, for a
    /// VM's DHCP lease on the pod network (#88).
    pub fn cluster_dns_config(&self) -> (Vec<String>, String) {
        (self.cluster_dns.clone(), self.cluster_domain.clone())
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
        // **A static pod gets no ServiceAccount token** (#262), as upstream: it
        // is not an API object a ServiceAccount admits. The control plane's
        // static pods start before the apiserver answers, and asking for a
        // token there cost every boot ~1.1 s of refused retries, then wrote none.
        if is_static(pod) {
            return Ok(None);
        }
        if pod_mounts_sa_path(pod) {
            return Ok(None); // SA admission already provided it — don't double-mount.
        }
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");

        let dir = pod_volume_dir(&self.state_root, uid, "secret", "kube-api-access");
        let written = (|| -> std::io::Result<()> {
            std::fs::create_dir_all(&dir)?;
            crate::api_files::write_in(&dir, "namespace", namespace.as_bytes(), 0o644)?;
            if let Some(ca) = &self.ca_pem {
                crate::api_files::write_in(&dir, "ca.crt", ca, 0o644)?;
            }
            Ok(())
        })();
        written.map_err(|e| pod_dir_error("kube-api-access", &dir, e))?;
        // Bound to the pod for 3607 s, as upstream's kubelet asks for
        // kube-api-access (#122); refreshed at 80% of its life.
        self.write_bound_token(pod, &dir, "token", None, 3607)
            .await
            .map_err(|e| pod_dir_error("kube-api-access", &dir, e))?;

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
        // The restart backoff an earlier kubelet left (#112): a crash-looping
        // container adopted below keeps its delay.
        self.backoff.persist_to(std::path::Path::new(&self.state_root).join("crashloop.json"));
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
                    liveness_failures: HashMap::new(), readiness_run: HashMap::new(),
                    startup_passed: HashMap::new(),
                    started: HashMap::new(),
                    terminated: HashMap::new(),
                    init_statuses: Vec::new(),
                    sandbox_stopped: false, unstarted: Default::default(),
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
                    // The deleting object's grace (#225).
                    if let Err(e) = self.stop_pod_within(uid, Some(termination_grace(pod))).await {
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
            // A pod this node refused at admission (#23) is terminal whatever
            // its policy, as upstream's: its controller replaces it.
            if phase == "Failed" && pod["status"]["reason"].as_str() == Some(APPARMOR_REASON) {
                continue;
            }
            if (phase == "Succeeded" || phase == "Failed") && restart_policy != "Always" {
                // Its sandbox may still hold the network: the stop at the
                // terminal pass failed, or this is that pass's own write
                // coming back (#137). Retried until it is given back.
                self.stop_finished_sandbox(uid).await;
                continue;
            }

            let partial = self.pods.read().await.get(uid).is_some_and(|p| p.phase=="Cleanup");
            if partial {
                // The teardown forgets the pod; when it was first seen and
                // why it waits are kept, so its back-off grows and its reason
                // still answers `logs` (#133).
                let seen = self.first_seen.lock().unwrap_or_else(|e| e.into_inner()).get(uid).copied();
                let waiting = self.waiting.lock().unwrap_or_else(|e| e.into_inner()).get(uid).cloned();
                if let Err(error)=self.stop_pod(uid).await {
                    warn!(%error, %uid, "partial Pod start cleanup pending");
                    self.due_in(uid, deadlines::RECHECK);
                    continue;
                }
                if let Some(seen) = seen {
                    self.first_seen.lock().unwrap_or_else(|e| e.into_inner()).insert(uid.to_string(), seen);
                }
                if let Some(w) = waiting {
                    self.waiting.lock().unwrap_or_else(|e| e.into_inner()).insert(uid.to_string(), w);
                }
            }

            let is_known = {
                let pods = self.pods.read().await;
                pods.get(uid).is_some_and(|p| p.phase != "Starting")
            };

            if !is_known {
                // Not before its back-off (#133).
                let holdoff = self.start_holdoff.lock().unwrap_or_else(|e| e.into_inner()).get(uid).copied();
                if let Some(at) = holdoff.filter(|at| Instant::now() < *at) {
                    self.due_at(uid, at);
                    continue;
                }
                // Refused at admission (#23): upstream's AppArmor check. No
                // runtime here applies a profile, so a pod that asks for one
                // is refused rather than run unconfined.
                if let Some(why) = apparmor_refusal(pod) {
                    warn!("Pod {namespace}/{name} refused: {why}");
                    self.event(pod, "Warning", APPARMOR_REASON, &why).await;
                    outcome.updates.push(PodStatusUpdate {
                        reason: APPARMOR_REASON.to_string(),
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        phase: "Failed".to_string(),
                        message: why,
                        container_statuses: vec![],
                        init_container_statuses: vec![],
                        declared_init_containers: declared_init_containers(pod),
                        pod_ip: None,
                    });
                    continue;
                }
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
                        self.start_holdoff.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
                        self.create_refusals.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
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
                        // Upstream's Event for it (#3); repeats aggregate.
                        self.event(pod, "Warning", "NetworkNotReady", &message).await;
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
                        // The plugin's own reason on the pod (#3): Cilium's
                        // "unable to connect to Cilium agent … Is the agent
                        // running?" was only in the kubelet's log.
                        self.event(pod, "Warning", "FailedCreatePodSandBox", &format!(
                            "Failed to create pod sandbox: {what}"
                        )).await;
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
                    // Not run as asked (#204, #430): waiting, with the reason
                    // as the Event's, retried on the wait's backoff.
                    Err(CriError::Refused { reason, message }) => {
                        warn!("Pod {namespace}/{name} refused ({reason}): {message}");
                        self.retry_wait(uid, seen.elapsed());
                        self.event(pod, "Warning", &reason, &message).await;
                        outcome.updates.push(self.waiting_pod(pod, message));
                    }
                    Err(CriError::VolumeNotReady(what)) => {
                        warn!("Pod {namespace}/{name} waiting on volumes: {what}");
                        self.retry_wait(uid, seen.elapsed());
                        let message = volume_wait_message(&what, seen.elapsed());
                        self.event(pod, "Warning", "FailedMount", &message).await;
                        outcome.updates.push(self.waiting_pod(pod, message));
                    }
                    // An image that would not pull is a wait, as upstream's
                    // ErrImagePull and then ImagePullBackOff (#133): it may be
                    // pushed, the registry may come back. Failing the pod
                    // ended it with no container state, and the failed pull
                    // was cached for the pod, so nothing asked again.
                    Err(CriError::ImagePull(what)) => {
                        self.start_images.lock().unwrap().remove(uid);
                        self.cleanup_partial_start(uid).await;
                        let again = self.waiting_state(namespace, name)
                            .is_some_and(|(k, _)| k == "ErrImagePull" || k == "ImagePullBackOff");
                        let kind = if again { "ImagePullBackOff" } else { "ErrImagePull" };
                        warn!("Pod {namespace}/{name}: {kind}: {what}");
                        if again {
                            self.event(pod, "Normal", "BackOff", &format!("Back-off pulling image: {what}")).await;
                        } else {
                            self.event(pod, "Warning", "Failed", &format!("Failed to pull image: {what}")).await;
                        }
                        self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
                        self.hold_off(uid, deadlines::start_backoff(seen.elapsed()));
                        outcome.updates.push(self.waiting_pod_as(pod, kind, what));
                    }
                    // One container would not be created or started (#133).
                    // Under restartPolicy Never a start failure ends the pod,
                    // with that container terminated StartError (exit 128),
                    // as upstream reports it; anything else waits and tries
                    // again on a back-off, with the container's reason.
                    Err(CriError::Container { container, reason, message }) => {
                        self.cleanup_partial_start(uid).await;
                        let never = pod["spec"]["restartPolicy"].as_str() == Some("Never");
                        if reason == "StartError" && never {
                            error!("Pod {namespace}/{name}: container {container} failed to start: {message}");
                            self.note_waiting(pod, "StartError", &message);
                            outcome.updates.push(PodStatusUpdate {
                                reason: String::new(),
                                namespace: namespace.to_string(),
                                name: name.to_string(),
                                phase: "Failed".to_string(),
                                message: format!("container {container} failed to start: {message}"),
                                container_statuses: failed_start_statuses(pod, &container, &message),
                                init_container_statuses: self.recorded_inits(uid).await,
                                declared_init_containers: declared_init_containers(pod),
                                pod_ip: None,
                            });
                        } else {
                            let kind = if reason == "StartError" { "RunContainerError".to_string() } else { reason };
                            // A refused create or start (#287, #289: the
                            // engine refusing a new root's attach or mount, or
                            // a claim's mount at start, once, after a boot) is
                            // tried again within a second, growing to the 10 s
                            // clock; the control plane's static pods are on
                            // that path at every boot. A container that ran
                            // and exited is CrashLoopBackOff's, not this.
                            let after = {
                                let mut refusals = self.create_refusals.lock().unwrap_or_else(|e| e.into_inner());
                                let n = refusals.entry(uid.to_string()).or_insert(0);
                                *n += 1;
                                deadlines::create_backoff(*n, seen.elapsed())
                            };
                            warn!("Pod {namespace}/{name}: {kind}: container {container}: {message} (next try in {} ms)", after.as_millis());
                            self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
                            self.hold_off(uid, after);
                            outcome.updates.push(self.waiting_pod_as(pod, &kind, format!("container {container}: {message}")));
                        }
                    }
                    // The runtime did not answer: it may be restarting. A
                    // wait, not the end of the pod.
                    Err(CriError::Connection(what)) => {
                        self.cleanup_partial_start(uid).await;
                        warn!("Pod {namespace}/{name}: container runtime unreachable: {what}");
                        self.retry_wait(uid, seen.elapsed());
                        outcome.updates.push(self.waiting_pod_as(pod, "ContainerCreating", format!("container runtime unreachable: {what}")));
                    }
                    Err(e) => {
                        self.start_images.lock().unwrap().remove(uid);
                        let inits = self.recorded_inits(uid).await;
                        if let Some(state)=self.pods.write().await.get_mut(uid) { state.phase="Cleanup".into(); }
                        apimachinery::reactor::failed();
                        self.retry_wait(uid, seen.elapsed());
                        error!("Failed to start pod {namespace}/{name}: {e}");
                        // Never terminal with no container state (#133): an
                        // init container that failed is in the init statuses
                        // and the apps wait on it (PodInitializing); otherwise
                        // the apps are terminated StartError. `logs` answers
                        // why, not "not found".
                        let message = e.to_string();
                        let init_failed = inits.iter().any(|r| r.state == "terminated" && r.exit_code != 0);
                        let container_statuses = if init_failed {
                            creating_statuses_as(pod, "PodInitializing", &message)
                        } else {
                            failed_start_statuses(pod, "", &message)
                        };
                        self.note_waiting(pod, "StartError", &message);
                        outcome.updates.push(PodStatusUpdate {
                            reason: String::new(),
                            namespace: namespace.to_string(),
                            name: name.to_string(),
                            phase: "Failed".to_string(),
                            message,
                            container_statuses,
                            init_container_statuses: inits,
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
        self.start_holdoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|uid, _| desired_uids.contains(uid));
        self.create_refusals
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
            // A native sidecar (#111) is looked at on every pass, started or
            // not: one that died while a later init runs is restarted here.
            let restartable = is_restartable_init(spec);
            let existing=self.pods.read().await.get(uid).and_then(|p|p.container_ids.get(cname).cloned());
            if restartable && existing.is_none() {
                let key = crate::crashloop::CrashLoopBackoff::key(uid, cname);
                if let Some(left) = self.backoff.wait(&key) {
                    self.due_in(uid, left);
                    return Err(CriError::Pending(format!(
                        "sidecar {cname}: back-off {}s before restarting it",
                        left.as_secs()
                    )));
                }
            }
            let image = spec["image"].as_str().unwrap_or("");
            info!("Init container {ns}/{name}/{cname}: ensuring image {image}");
            let image_ref = self.startup_image(pod,image, spec).await?;
            let mut envs = self.resolve_env(pod, spec, pod_ip).await?;
            merge_env(&mut envs, self.service_account_env());
            let mounts = container_mounts(spec, volumes, &envs, sa_mount, dns_mount).map_err(|message| CriError::Container {
                container: cname.to_string(),
                reason: "CreateContainerConfigError".into(),
                message,
            })?;
            let mut config = build_container_config(spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut config, pod);
            config.realtime = realtime_of(pod, &sandbox_config.runtime_handler, &config.name);
            ensure_container_log_dir(&sandbox_config.log_directory, &config.name);
            let cid = if let Some(cid)=existing {cid} else {
            // A retried init or a restarted sidecar is a new run with its own
            // file, so `--previous` reads the one before (#216).
            new_log_run(&mut config, &sandbox_config.log_directory, 0);
            let cid = self.runtime.create_container(sandbox_id, &config, sandbox_config).await?;
            if let Some(state)=self.pods.write().await.get_mut(pod["metadata"]["uid"].as_str().unwrap_or("")) {
                state.container_ids.insert(cname.into(),cid.clone());
            }
            self.runtime.start_container(&cid).await?;
            self.post_start(pod, spec, &cid, sandbox_id, pod_ip.unwrap_or("")).await;
            if let Some(state)=self.pods.write().await.get_mut(uid) {state.started.insert(cname.into(),Instant::now());}
            cid
            };
            if restartable {
                self.sidecar_step(pod, uid, sandbox_id, spec, &cid, &image_ref, pod_ip, out).await?;
                continue;
            }
            info!("Init container {ns}/{name}/{cname} started, waiting for completion");

            // It runs until it exits (#126): no fixed bound, as upstream. The
            // pod's activeDeadlineSeconds, when set, is the only deadline.
            // Read once (#375, clippy never_loop): exited goes on to the next init container, anything else is pending.
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
                                ..Default::default()
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
                            ..Default::default()
                        });
                        if let Some(state)=self.pods.write().await.get_mut(uid) {state.init_statuses=out.clone();}
                        // Its log must still answer `kubectl logs -c` (#47):
                        // removed now only where removal keeps the log.
                        if self.runtime.logs_survive_removal() {
                            let _ = self.runtime.remove_container(&cid).await;
                        } else {
                            self.kept_inits.lock().unwrap_or_else(|e| e.into_inner()).entry(uid.to_string()).or_default().push(cid.clone());
                        }
                    }
                    _ => {
                        let seen = self.first_seen.lock().unwrap_or_else(|e| e.into_inner()).get(uid).copied();
                        let left = active_deadline_left(pod, seen, chrono::Utc::now());
                        if left.is_some_and(|l| l.is_zero()) {
                            let deadline = pod["spec"]["activeDeadlineSeconds"].as_u64().unwrap_or(0);
                            warn!("Pod {ns}/{name}: activeDeadlineSeconds ({deadline}s) passed with init container {cname} running");
                            out.push(InitContainerStatusReport {
                                name: cname.to_string(),
                                container_id: cid.clone(),
                                state: "terminated".into(),
                                exit_code: -1,
                                reason: "DeadlineExceeded".into(),
                                message: format!(
                                    "Pod was active on the node longer than the specified deadline (activeDeadlineSeconds {deadline})"
                                ),
                                image: image.to_string(),
                                image_ref: image_ref.clone(),
                                started_at: status.started_at,
                                finished_at: 0,
                                ..Default::default()
                            });
                            let grace = pod["spec"]["terminationGracePeriodSeconds"].as_i64().unwrap_or(30);
                            let _ = self.runtime.stop_container(&cid, grace).await;
                            let _ = self.runtime.remove_container(&cid).await;
                            return Err(CriError::Runtime(format!(
                                "DeadlineExceeded: Pod was active on the node longer than the specified deadline (activeDeadlineSeconds {deadline})"
                            )));
                        }
                        // Its exit is an event, so nothing waits on a worker
                        // (#126); the deadline, when there is one, is the due
                        // time. A runtime with no exit events is looked at
                        // again on the recheck.
                        if self.admission.is_some() {
                            self.event_waits.lock().unwrap_or_else(|e| e.into_inner()).insert(uid.to_string());
                            if let Some(left) = left {
                                self.due_in(uid, left);
                            }
                        } else {
                            self.due_in(uid, left.map_or(deadlines::RECHECK, |l| l.min(deadlines::RECHECK)));
                        }
                        return Err(CriError::Pending(format!("init container {cname} is running")));
                    }
                }
        }
        Ok(())
    }

    /// One pass over a native sidecar (#111): started and kept, the next init
    /// container allowed once it has *started* (running, and its startupProbe
    /// passed if it has one). No completion and no deadline: it runs for the
    /// pod's life. An exit before it started is a restart with back-off,
    /// never a failed pod, because its restartPolicy is Always whatever the
    /// pod's is.
    #[allow(clippy::too_many_arguments)]
    async fn sidecar_step(
        &self,
        pod: &Value,
        uid: &str,
        sandbox_id: &str,
        spec: &Value,
        cid: &str,
        image_ref: &str,
        pod_ip: Option<&str>,
        out: &mut Vec<InitContainerStatusReport>,
    ) -> Result<(), CriError> {
        let cname = spec["name"].as_str().unwrap_or("init");
        let (ns, name) = (
            pod["metadata"]["namespace"].as_str().unwrap_or("default"),
            pod["metadata"]["name"].as_str().unwrap_or(""),
        );
        let restart_count = self.pods.read().await.get(uid)
            .and_then(|p| p.restart_counts.get(cname).copied()).unwrap_or(0);
        let mut report = InitContainerStatusReport {
            name: cname.to_string(),
            container_id: cid.to_string(),
            image: spec["image"].as_str().unwrap_or("").to_string(),
            image_ref: image_ref.to_string(),
            restartable: true,
            restart_count,
            ..Default::default()
        };
        let status = self.runtime.container_status(cid).await?;
        report.started_at = status.started_at;
        match status.state {
            ContainerState::Exited => {
                let _ = self.runtime.remove_container(cid).await;
                let restarts = {
                    let mut pods = self.pods.write().await;
                    match pods.get_mut(uid) {
                        Some(st) => {
                            st.container_ids.remove(cname);
                            st.started.remove(cname);
                            st.startup_passed.remove(cname);
                            st.readiness_run.remove(cname);
                            let c = st.restart_counts.entry(cname.to_string()).or_insert(0);
                            *c += 1;
                            *c
                        }
                        None => restart_count + 1,
                    }
                };
                self.restarted_backoff(uid, &crate::crashloop::CrashLoopBackoff::key(uid, cname));
                warn!("Sidecar {ns}/{name}/{cname} exited with code {}; restarting it", status.exit_code);
                report.state = "waiting".into();
                report.reason = "CrashLoopBackOff".into();
                report.exit_code = status.exit_code;
                report.message = format!("exited with code {}; restarting", status.exit_code);
                report.restart_count = restarts;
                self.record_init(uid, out, report).await;
                Err(CriError::Pending(format!(
                    "sidecar {cname} exited with code {}; restarting it",
                    status.exit_code
                )))
            }
            ContainerState::Running => {
                let startup = &spec["startupProbe"];
                let passed = self.pods.read().await.get(uid)
                    .is_some_and(|p| p.startup_passed.get(cname).copied().unwrap_or(false));
                if !startup.is_null() && !passed {
                    let netns = self.runtime.pod_sandbox_status(sandbox_id).await.ok().and_then(|s| s.netns_path);
                    let result = run_probe(startup, spec, cid, pod_ip.unwrap_or(""), netns.as_deref(), &self.runtime).await;
                    if !matches!(result, ProbeResult::Success) {
                        report.state = "running".into();
                        self.record_init(uid, out, report).await;
                        let period = startup["periodSeconds"].as_u64().unwrap_or(10).max(1);
                        self.due_in(uid, std::time::Duration::from_secs(period));
                        return Err(CriError::Pending(format!("sidecar {cname}: waiting for its startupProbe")));
                    }
                    if let Some(st) = self.pods.write().await.get_mut(uid) {
                        st.startup_passed.insert(cname.to_string(), true);
                    }
                }
                let already = out.iter().any(|r| r.name == cname && r.started);
                if !already {
                    info!("Sidecar {ns}/{name}/{cname} started");
                }
                report.state = "running".into();
                report.started = true;
                // Ready at once without a readinessProbe, as an app container.
                report.ready = spec["readinessProbe"].is_null()
                    || out.iter().any(|r| r.name == cname && r.ready);
                self.record_init(uid, out, report).await;
                Ok(())
            }
            _ => {
                report.state = "running".into();
                self.record_init(uid, out, report).await;
                self.due_in(uid, deadlines::RECHECK);
                Err(CriError::Pending(format!("sidecar {cname} is starting")))
            }
        }
    }

    /// Put `report` in `out` (replacing the one of its name) and in the pod's
    /// record, which is what survives later passes.
    async fn record_init(&self, uid: &str, out: &mut Vec<InitContainerStatusReport>, report: InitContainerStatusReport) {
        match out.iter_mut().find(|r| r.name == report.name) {
            Some(r) => *r = report,
            None => out.push(report),
        }
        if let Some(state) = self.pods.write().await.get_mut(uid) {
            state.init_statuses = out.clone();
        }
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

        let mut sandbox_config = build_sandbox_config(pod);
        // The pod's RuntimeClass (#204) and its real time (#430), checked
        // before anything is made: a refusal leaves nothing to undo.
        sandbox_config.runtime_handler = self.runtime_handler(pod).await?;
        realtime_admission(pod, &sandbox_config.runtime_handler, crate::realtime::Preempt::read())?;
        let step = Instant::now();
        let mut volume_times = Vec::new();
        let mut claim_steps = Vec::new();
        let volumes = self.resolve_volumes_timed(pod, &mut volume_times, &mut claim_steps).await?;
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
        for (volume, steps) in claim_steps {
            attempt.claim(&volume, steps);
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
        // Made by this attempt: its steps are this start's to report (#139).
        let made_here=existing.is_none();
        let sandbox_id=if let Some(existing)=existing {existing} else {
        // The pod's extra networks (#233), resolved before the sandbox: a
        // NAD that is not there is a wait naming it.
        let mut sandbox_config = sandbox_config.clone();
        if !sandbox_config.host_network {
            let (networks, default_network) = self.resolve_networks(pod).await.map_err(CriError::NetworkNotReady)?;
            sandbox_config.networks = networks;
            sandbox_config.default_network = default_network;
        }
        let sandbox_id = self.runtime.run_pod_sandbox(&sandbox_config).await?;
        // Persist each side effect before the next await. A failed or cancelled
        // start is cleaned by this UID before another sandbox can be created.
        self.pods.write().await.insert(uid.into(), PodState {
            namespace:namespace.into(),name:name.into(),uid:uid.into(),
            sandbox_id:Some(sandbox_id.clone()),container_ids:HashMap::new(),
            phase:"Starting".into(),pod:pod.clone(),pod_ip:None,
            restart_counts:HashMap::new(),ready:HashMap::new(),liveness_failures:HashMap::new(),readiness_run:HashMap::new(),
            startup_passed:HashMap::new(),started:HashMap::new(),terminated:HashMap::new(),init_statuses:Vec::new(),sandbox_stopped:false,unstarted:Default::default(),
        });
        sandbox_id
        };
        info!("Created sandbox {sandbox_id} for {namespace}/{name}");

        // Get sandbox IP
        let status_step = Instant::now();
        let sandbox_status = self.runtime.pod_sandbox_status(&sandbox_id).await?;
        let pod_ip = if sandbox_status.ip.is_empty() {
            None
        } else {
            Some(sandbox_status.ip.clone())
        };
        attempt.sandbox(step.elapsed());
        if made_here {
            attempt.sandbox_steps(sandbox_status.made, status_step.elapsed());
        }

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
            // Kept on the record (#133): the pod's status says which init
            // container failed and how, however the error is reported.
            if let Some(state) = self.pods.write().await.get_mut(uid) {
                state.init_statuses = init_statuses.clone();
            }
            if let Some(failed) = init_statuses.iter().find(|s| !s.initialized()) {
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
                .await?;
            merge_env(&mut envs, self.service_account_env());
            let mounts = container_mounts(container_spec, &volumes, &envs, &sa_mount, &dns_mount).map_err(|message| CriError::Container {
                container: container_name.to_string(),
                reason: "CreateContainerConfigError".into(),
                message,
            })?;
            let mut container_config =
                build_container_config(container_spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut container_config, pod);
            container_config.realtime = realtime_of(pod, &sandbox_config.runtime_handler, &container_config.name);
            ensure_container_log_dir(&sandbox_config.log_directory, &container_config.name);
            new_log_run(&mut container_config, &sandbox_config.log_directory, 0);

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
                    return Err(container_error(e, container_name, "CreateContainerError"));
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
                return Err(container_error(e, container_name, "StartError"));
            }
            self.event_later(
                pod,
                "Started",
                &format!("Started container {container_name}"),
            )
            .await;
            self.post_start(pod, container_spec, &container_id, &sandbox_id, pod_ip.as_deref().unwrap_or("")).await;
            info!("Started container {container_name} ({container_id}) in {namespace}/{name}");
            let took = step.elapsed();
            attempt.container(container_name, took, self.runtime.container_steps(&container_id).await);

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

        // The sidecars started during init keep running with the apps (#111):
        // their containers, start times, restarts and startup state carry
        // into the pod's record, and their readiness counts.
        let mut restart_counts = HashMap::new();
        let mut startup_passed = HashMap::new();
        {
            let pods = self.pods.read().await;
            if let Some(prev) = pods.get(uid) {
                for n in sidecar_names(pod) {
                    if let Some(c) = prev.container_ids.get(&n) {
                        container_ids.insert(n.clone(), c.clone());
                    }
                    if let Some(t) = prev.started.get(&n) {
                        started_map.insert(n.clone(), *t);
                    }
                    if let Some(r) = prev.restart_counts.get(&n) {
                        restart_counts.insert(n.clone(), *r);
                    }
                    if let Some(p) = prev.startup_passed.get(&n) {
                        startup_passed.insert(n.clone(), *p);
                    }
                }
            }
        }
        for r in init_statuses.iter().filter(|r| r.restartable) {
            ready_map.insert(r.name.clone(), r.ready);
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
                    restart_counts,
                    ready: ready_map,
                    liveness_failures: HashMap::new(), readiness_run: HashMap::new(),
                    startup_passed,
                    started: started_map,
                    terminated: HashMap::new(),
                    init_statuses: init_statuses.clone(),
                    sandbox_stopped: false, unstarted: Default::default(),
                },
            );
        }

        self.timing(pod, |t| t.started(attempt));
        Ok(PodStatusUpdate {
            reason: String::new(),
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

        let pod_restart_policy = state.pod["spec"]["restartPolicy"]
            .as_str()
            .unwrap_or("Always")
            .to_string();
        let mut container_specs: HashMap<String, Value> = state.pod["spec"]["containers"]
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
        // Native sidecars (#111) are probed, restarted and reported like app
        // containers once the pod is running, with their own policy (Always).
        let sidecars = sidecar_names(&state.pod);
        for c in state.pod["spec"]["initContainers"].as_array().into_iter().flatten() {
            if let Some(n) = c["name"].as_str().filter(|n| sidecars.iter().any(|s| s == n)) {
                container_specs.insert(n.to_string(), c.clone());
            }
        }
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
            // A sidecar comes back whatever the pod's policy (#111).
            let restart_policy = if sidecars.contains(&name) {
                "Always".to_string()
            } else {
                pod_restart_policy.clone()
            };
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
                                        "Startup threshold reached — stopping {}/{}/{name}",
                                        state.namespace, state.name
                                    );
                                    if self
                                        .probe_gave_up(
                                            &mut state,
                                            &name,
                                            &cid,
                                            &spec,
                                            &sandbox_config,
                                            &restart_policy,
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
                                        "Liveness threshold reached — stopping {}/{}/{name}",
                                        state.namespace, state.name
                                    );
                                    restarted = self
                                        .probe_gave_up(
                                            &mut state,
                                            &name,
                                            &cid,
                                            &spec,
                                            &sandbox_config,
                                            &restart_policy,
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
                        // Only a run of the threshold's length changes it (#460).
                        let was = state.ready.get(&name).copied().unwrap_or(false);
                        let ready = state.readiness_run.entry(name.clone()).or_default().after(
                            was,
                            ok,
                            probe_success_threshold(readiness),
                            probe_failure_threshold(readiness),
                        );
                        if ready != was {
                            info!(
                                "Readiness of {}/{}/{name}: {} after {} {} in a row",
                                state.namespace,
                                state.name,
                                if ready { "ready" } else { "not ready" },
                                state.readiness_run[&name].count,
                                if ok { "successes" } else { "failures" },
                            );
                        }
                        ready
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
                        // The run that ended, for `lastState` while it waits
                        // and once the next one runs (#130).
                        self.last_terminated
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .entry(uid.to_string())
                            .or_default()
                            .insert(name.clone(), LastTerminated::of(&status));
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
                            // The runtime's word (OOMKilled), else upstream's
                            // Completed / Error (#130).
                            reason: terminated_reason(&status.reason, status.exit_code).to_string(),
                            message: status.message.clone(),
                        });
                    }
                }
                // A replacement that was created but did not start, and was
                // not removed then (#437, 12e774c42aec): removed and made
                // again, on the crash-loop clock, rather than waited on.
                ContainerState::Created if state.unstarted.contains(&name) && !spec.is_null() => {
                    let key = crate::crashloop::CrashLoopBackoff::key(uid, &name);
                    if let Some(left) = self.backoff.wait(&key) {
                        self.due_in(uid, left);
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
                            image: status.image.clone(),
                            image_ref: status.image_ref,
                            reason: "RunContainerError".to_string(),
                            message: format!("back-off {}s starting container {name} again", left.as_secs()),
                        });
                    } else {
                        self.restart_container(&mut state, &name, &cid, &spec, &sandbox_config, &mut container_statuses)
                            .await;
                        self.restarted_backoff(uid, &key);
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
        // Sidecars are reported among the init containers (#111), with what
        // the loop above found for them.
        let (side, apps): (Vec<_>, Vec<_>) =
            container_statuses.into_iter().partition(|cs| sidecars.contains(&cs.name));
        let container_statuses = apps;
        for cs in side {
            let passed = state.pod["spec"]["initContainers"].as_array().into_iter().flatten()
                .find(|c| c["name"].as_str() == Some(cs.name.as_str()))
                .is_some_and(|c| c["startupProbe"].is_null())
                || state.startup_passed.get(&cs.name).copied().unwrap_or(false);
            if let Some(r) = state.init_statuses.iter_mut().find(|r| r.name == cs.name) {
                r.container_id = cs.container_id;
                r.state = cs.state;
                r.ready = cs.ready;
                r.restart_count = cs.restart_count;
                r.exit_code = cs.exit_code;
                r.reason = cs.reason;
                r.message = cs.message;
                r.started = r.state == "running" && passed;
                if cs.started_at != 0 {
                    r.started_at = cs.started_at;
                }
                r.finished_at = cs.finished_at;
            }
        }

        // The phase is the app containers' (#111): a sidecar running on does
        // not keep a Never/OnFailure pod from finishing.
        let app_names: Vec<&String> =
            state.container_ids.keys().filter(|n| !sidecars.contains(*n)).collect();
        let total = app_names.len();
        let app_codes: Vec<i32> =
            app_names.iter().filter_map(|n| state.terminated.get(*n).copied()).collect();
        let phase = if pod_restart_policy == "Always" {
            "Running"
        } else if total > 0 && app_codes.len() == total {
            if app_codes.iter().all(|&code| code == 0) {
                "Succeeded"
            } else {
                "Failed"
            }
        } else {
            "Running"
        };
        state.phase = phase.to_string();
        let finished = phase != "Running";
        if finished {
            // The apps are done: the sidecars are stopped, last declared
            // first, as upstream does (#111).
            for n in sidecars.iter().rev() {
                let Some(cid) = state.container_ids.get(n).cloned() else { continue };
                if state.terminated.contains_key(n) {
                    continue;
                }
                let grace = state.pod["spec"]["terminationGracePeriodSeconds"].as_i64().unwrap_or(30);
                match self.runtime.stop_container(&cid, grace).await {
                    Ok(()) | Err(CriError::NotFound(_)) => {
                        info!("Sidecar {}/{}/{n} stopped: the pod's containers finished", state.namespace, state.name);
                        state.terminated.insert(n.clone(), 0);
                        state.ready.insert(n.clone(), false);
                        if let Some(r) = state.init_statuses.iter_mut().find(|r| &r.name == n) {
                            r.state = "terminated".into();
                            r.reason = "Completed".into();
                            r.ready = false;
                            r.started = false;
                        }
                    }
                    Err(e) => warn!("Sidecar {}/{}/{n} did not stop (retried): {e}", state.namespace, state.name),
                }
            }
        }

        let update = PodStatusUpdate {
            reason: String::new(),
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
    ///
    /// #437:
    /// - The old container must be gone before its replacement is made
    ///   (1e8f97747676): a stop or remove that fails is a restart that waits,
    ///   with the old ID kept, never two of one container in the sandbox at
    ///   once with the same volumes and ports. Only the runtime saying it does
    ///   not have the container counts as gone.
    /// - The new ID is recorded as soon as it is created (12e774c42aec). A
    ///   start that fails or times out stops and removes it; if that fails
    ///   too, the next restart finds it recorded and removes it first.
    /// - A new run's probes start over (6735237ca645): its startup probe
    ///   runs again, and it is not ready until it passes.
    /// - Its mounts are the start path's (`container_mounts`), resolv.conf
    ///   included (6c156be85e7f).
    async fn restart_container(
        &self,
        state: &mut PodState,
        name: &str,
        old_cid: &str,
        spec: &Value,
        sandbox_config: &PodSandboxConfig,
        container_statuses: &mut Vec<ContainerStatusReport>,
    ) -> bool {
        let image = spec["image"].as_str().unwrap_or("");
        let restart_count = state.restart_counts.get(name).copied().unwrap_or(0);
        let wait = |state: &mut PodState,
                    statuses: &mut Vec<ContainerStatusReport>,
                    cid: &str,
                    restart_count: u32,
                    message: String| {
            warn!("Container {}/{}/{name}: {message}", state.namespace, state.name);
            self.due_in(&state.uid, deadlines::RECHECK);
            state.ready.insert(name.to_string(), false);
            statuses.push(ContainerStatusReport {
                started_at: 0,
                finished_at: 0,
                name: name.to_string(),
                container_id: cid.to_string(),
                state: "waiting".to_string(),
                ready: false,
                restart_count,
                exit_code: 0,
                image: image.to_string(),
                image_ref: String::new(),
                reason: String::new(),
                message,
            });
            false
        };

        let sandbox_id = match &state.sandbox_id {
            Some(id) => id.clone(),
            None => {
                return wait(state, container_statuses, old_cid, restart_count, "cannot restart: no sandbox".into());
            }
        };
        if !old_cid.is_empty() {
            if let Err(e) = self.confirm_gone(old_cid).await {
                return wait(
                    state,
                    container_statuses,
                    old_cid,
                    restart_count,
                    format!("previous container {old_cid} not confirmed gone, so no replacement yet: {e}"),
                );
            }
        }

        let restart_count = {
            let c = state.restart_counts.entry(name.to_string()).or_insert(0);
            *c += 1;
            *c
        };
        // A new run: every probe outcome of the last one is forgotten.
        state.liveness_failures.insert(name.to_string(), 0);
        state.startup_passed.remove(name);
        state.readiness_run.remove(name);
        state.started.remove(name);
        state.ready.insert(name.to_string(), false);

        // A volume this node must not provision stops the restart: the same
        // reason is on the pod's status from the start path, and retrying
        // every tick would not change it.
        let volumes = match self.resolve_volumes(&state.pod).await {
            Ok(v) => v,
            Err(e) => return wait(state, container_statuses, old_cid, restart_count, e.to_string()),
        };
        let mut envs = match self.resolve_env(&state.pod, spec, state.pod_ip.as_deref()).await {
            Ok(e) => e,
            Err(e) => return wait(state, container_statuses, old_cid, restart_count, e.to_string()),
        };
        merge_env(&mut envs, self.service_account_env());
        let sa_mount = match self.service_account_mount(&state.pod).await {
            Ok(m) => m,
            Err(e) => return wait(state, container_statuses, old_cid, restart_count, e.to_string()),
        };
        let dns_mount = match self.resolv_conf_mount(&state.pod) {
            Ok(m) => m,
            Err(e) => return wait(state, container_statuses, old_cid, restart_count, e.to_string()),
        };
        let mounts = match container_mounts(spec, &volumes, &envs, &sa_mount, &dns_mount) {
            Ok(m) => m,
            Err(e) => return wait(state, container_statuses, old_cid, restart_count, e),
        };

        let pod = state.pod.clone();
        let created = async {
            let image_ref = self.ensure_image(image, spec).await?;
            // Its RuntimeClass again (#430): a real-time container restarts on
            // its isolated cores, never on the shared pool. Not known now is
            // a failed restart, retried.
            let handler = self.runtime_handler(&pod).await?;
            let mut config = build_container_config(spec, &image_ref, envs, mounts);
            apply_pod_namespaces(&mut config, &pod);
            config.realtime = realtime_of(&pod, &handler, &config.name);
            config.attempt = restart_count;
            ensure_container_log_dir(&sandbox_config.log_directory, &config.name);
            // After every run on disk, not only the restart count, which a
            // kubelet restart resets (#216).
            new_log_run(&mut config, &sandbox_config.log_directory, restart_count);
            let cid = self.runtime.create_container(&sandbox_id, &config, sandbox_config).await?;
            Ok::<(String, String), CriError>((cid, image_ref))
        }
        .await;
        let (new_cid, image_ref) = match created {
            Ok(c) => c,
            Err(e) => {
                return wait(state, container_statuses, old_cid, restart_count, format!("not recreated: {e}"))
            }
        };
        // Recorded before it is started: a start that fails or times out
        // leaves a container the next pass must find.
        state.container_ids.insert(name.to_string(), new_cid.clone());

        if let Err(e) = self.runtime.start_container(&new_cid).await {
            // A start that timed out may have started it: stop and remove it
            // now, so nothing runs that the kubelet does not track. If that
            // fails, it stays recorded, and the next restart removes it first.
            state.unstarted.insert(name.to_string());
            let cleanup = match self.confirm_gone(&new_cid).await {
                Ok(()) => "removed".to_string(),
                Err(c) => format!("kept for the next try to remove ({c})"),
            };
            return wait(
                state,
                container_statuses,
                &new_cid,
                restart_count,
                format!("restarted container {new_cid} did not start: {e}; {cleanup}"),
            );
        }
        state.unstarted.remove(name);
        self.post_start(&pod, spec, &new_cid, &sandbox_id, state.pod_ip.as_deref().unwrap_or("")).await;
        state.started.insert(name.to_string(), Instant::now());
        // Not ready until its startup probe passes, if it has one.
        let ready = spec["readinessProbe"].is_null() && spec["startupProbe"].is_null();
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
        // Its new run is on a new clone (#130).
        self.annotate_containers(&state.pod, state.container_ids.clone()).await;
        true
    }

    /// A startup or liveness probe reached its failure threshold (#437,
    /// c57f2ce7c38d). The container is killed, and what follows is its
    /// restart policy's (a native sidecar's is Always): Always and OnFailure
    /// restart it (a killed container exits non-zero); Never leaves it
    /// terminated, as upstream does, instead of running a one-shot again.
    ///
    /// `true` when the container's status was reported here.
    #[allow(clippy::too_many_arguments)]
    async fn probe_gave_up(
        &self,
        state: &mut PodState,
        name: &str,
        cid: &str,
        spec: &Value,
        sandbox_config: &PodSandboxConfig,
        restart_policy: &str,
        container_statuses: &mut Vec<ContainerStatusReport>,
    ) -> bool {
        if matches!(restart_policy, "Always" | "OnFailure") {
            return self
                .restart_container(state, name, cid, spec, sandbox_config, container_statuses)
                .await;
        }
        match self.runtime.stop_container(cid, RESTART_STOP_GRACE_SECS).await {
            Ok(()) | Err(CriError::NotFound(_)) => {}
            Err(e) => {
                warn!(
                    "Container {}/{}/{name}: its probe gave up, and it could not be stopped: {e}",
                    state.namespace, state.name
                );
                self.due_in(&state.uid, deadlines::RECHECK);
                return false;
            }
        }
        let (exit_code, reason, message, started_at, finished_at, image_ref) =
            match self.runtime.container_status(cid).await {
                Ok(s) if s.state == ContainerState::Exited => (
                    s.exit_code,
                    terminated_reason(&s.reason, s.exit_code).to_string(),
                    s.message,
                    s.started_at,
                    s.finished_at,
                    s.image_ref,
                ),
                // Killed: what a SIGKILLed process exits with.
                _ => (137, "Error".to_string(), String::new(), 0, 0, String::new()),
            };
        info!(
            "Container {}/{}/{name}: its probe gave up — not restarting (policy {restart_policy}), exit {exit_code}",
            state.namespace, state.name
        );
        state.terminated.insert(name.to_string(), exit_code);
        state.ready.insert(name.to_string(), false);
        container_statuses.push(ContainerStatusReport {
            started_at,
            finished_at,
            name: name.to_string(),
            container_id: cid.to_string(),
            state: "terminated".to_string(),
            ready: false,
            restart_count: state.restart_counts.get(name).copied().unwrap_or(0),
            exit_code,
            image: spec["image"].as_str().unwrap_or("").to_string(),
            image_ref,
            reason,
            message,
        });
        true
    }

    /// Stop and remove a container, and say whether it is gone: `Ok` only
    /// when both succeeded or the runtime does not have it (#437,
    /// 1e8f97747676).
    async fn confirm_gone(&self, cid: &str) -> Result<(), CriError> {
        match self.runtime.stop_container(cid, RESTART_STOP_GRACE_SECS).await {
            Ok(()) | Err(CriError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        match self.runtime.remove_container(cid).await {
            Ok(()) | Err(CriError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
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
    /// Each pod's volumes for `/stats/summary` (#372), by (namespace, pod):
    /// upstream's `VolumeStats` (`name`, `pvcRef`), sized from the engine
    /// (`capacityBytes` its virtual size, `usedBytes` the bytes it holds of
    /// its own), with `volumeId`, the id `storm.io/volume.<name>` names.
    ///
    /// A volume a running container mounts also has its filesystem's
    /// `availableBytes` and `inodes*` (#469), from a statfs of its mount path
    /// in that container's mount namespace (`volume_fs`).
    pub async fn pod_volume_stats(&self) -> Vec<((String, String), Vec<Value>)> {
        type Held = (String, String, Value, HashMap<String, String>);
        let pods: Vec<(String, Held)> = self
            .pods
            .read()
            .await
            .values()
            .map(|p| {
                (p.uid.clone(), (p.namespace.clone(), p.name.clone(), p.pod["spec"].clone(), p.container_ids.clone()))
            })
            .collect();
        let held: Vec<_> = pods
            .into_iter()
            .map(|(uid, held)| (held, self.pod_volume_ids(&uid)))
            .filter(|(_, ids)| !ids.is_empty())
            .collect();
        if held.is_empty() {
            return Vec::new();
        }
        let listing = self.storage_get("/api/v1/volumes").await.unwrap_or(Value::Null);
        let mut out = Vec::new();
        for ((ns, name, spec, containers), ids) in held {
            let mut stats = volume_stats(&ns, &name, &spec["volumes"], &ids, &listing);
            for v in stats.iter_mut() {
                let Some(volume) = v["name"].as_str().map(str::to_string) else { continue };
                if let Some(fs) = self.volume_fs_stats(&spec, &containers, &volume).await {
                    fs.fill(v);
                }
            }
            out.push(((ns, name), stats));
        }
        out
    }

    /// The filesystem of pod volume `volume` as the first running container
    /// that mounts it sees it (#469). `None` when no running container mounts
    /// it, the runtime cannot name its process, or nothing is mounted there.
    async fn volume_fs_stats(
        &self,
        spec: &Value,
        containers: &HashMap<String, String>,
        volume: &str,
    ) -> Option<crate::volume_fs::FsStats> {
        for (container, mount_path) in volume_mount_paths(spec, volume) {
            let Some(id) = containers.get(&container) else { continue };
            let Some(pid) = self.runtime.container_pid(id).await else { continue };
            #[cfg(target_os = "linux")]
            {
                let asked = tokio::task::spawn_blocking(move || crate::volume_fs::statfs_in(pid, &mount_path)).await;
                match asked {
                    Ok(Ok(fs)) => return Some(fs),
                    Ok(Err(e)) => debug!(volume = %volume, container = %container, "volume statfs: {e}"),
                    Err(e) => debug!(volume = %volume, container = %container, "volume statfs: {e}"),
                }
            }
            #[cfg(not(target_os = "linux"))]
            let _ = (pid, mount_path);
        }
        None
    }

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
    /// Record that a pod's volume `name` is the volume `id` (#372).
    fn record_pod_volume(&self, uid: &str, name: &str, id: &str) {
        self.pod_volumes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(uid.to_string())
            .or_default()
            .insert(name.to_string(), id.to_string());
    }

    /// A pod's volumes and the volume behind each (#372), by pod volume name.
    pub fn pod_volume_ids(&self, uid: &str) -> std::collections::BTreeMap<String, String> {
        self.pod_volumes.lock().unwrap_or_else(|e| e.into_inner()).get(uid).cloned().unwrap_or_default()
    }

    /// Each container's previous run in a pod (#130), for `lastState`.
    pub fn last_terminated(&self, uid: &str) -> HashMap<String, LastTerminated> {
        self.last_terminated.lock().unwrap_or_else(|e| e.into_inner()).get(uid).cloned().unwrap_or_default()
    }

    /// Each container's image provenance and instance, as pod annotations
    /// (#130): `storm.io/image-resolved.<c>` (when its image's golden was last
    /// checked and cloned for it), `storm.io/instance.<c>` (the engine volume
    /// that is its own CoW root), `storm.io/golden.<c>` (what that was cloned
    /// from) and `storm.io/image-build.<c>` (OCI build info, JSON). A key whose
    /// name part would pass 63 characters is left out.
    async fn container_annotations(&self, uid: &str) -> serde_json::Map<String, Value> {
        let ids = match self.pods.read().await.get(uid) {
            Some(p) => p.container_ids.clone(),
            None => return Default::default(),
        };
        self.annotations_for(uid, ids).await
    }

    /// A pod's volumes as `storm.io/volume.<name>` (#372), what a static
    /// pod's mirror carries (#395). A key past 63 characters is left out.
    pub fn volume_annotations(&self, uid: &str) -> serde_json::Map<String, Value> {
        self.pod_volume_ids(uid)
            .into_iter()
            .filter(|(name, _)| format!("volume.{name}").len() <= 63)
            .map(|(name, id)| (format!("storm.io/volume.{name}"), serde_json::json!(id)))
            .collect()
    }

    /// [`Self::container_annotations`] for these containers (name → id), and
    /// the pod's claims as `storm.io/volume.<name>` (#372).
    async fn annotations_for(&self, uid: &str, ids: HashMap<String, String>) -> serde_json::Map<String, Value> {
        let mut out = self.volume_annotations(uid);
        for (name, cid) in ids {
            let Some(info) = self.runtime.container_image_info(&cid).await else { continue };
            let mut put = |what: &str, v: Value| {
                let key = format!("{what}.{name}");
                if key.len() <= 63 {
                    out.insert(format!("storm.io/{key}"), v);
                }
            };
            if let Some(t) = info.resolved_at {
                put("image-resolved", serde_json::json!(t));
            }
            if let Some(v) = info.instance {
                put("instance", serde_json::json!(v));
            }
            // The image's digest beside its root and golden (#372), so the
            // chain image → golden → root holds on the object itself.
            if let Some(d) = info.image_id.filter(|d| !d.is_empty()) {
                put("image-digest", serde_json::json!(d));
            }
            if let Some(g) = info.golden {
                put("golden", serde_json::json!(g));
            }
            if !info.build.is_empty() {
                put("image-build", serde_json::json!(Value::Object(info.build).to_string()));
            }
        }
        out
    }

    /// Write [`Self::container_annotations`] now, off the worker: after a
    /// restart, whose new run has its own clone (#130).
    async fn annotate_containers(&self, pod: &Value, ids: HashMap<String, String>) {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        if self.api_url.is_empty() || pod["metadata"]["resourceVersion"].as_str().is_none() {
            return;
        }
        let annotations = self.annotations_for(uid, ids).await;
        if annotations.is_empty() {
            return;
        }
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = pod["metadata"]["name"].as_str().unwrap_or("");
        let request = self
            .api_client
            .patch(format!("{}/api/v1/namespaces/{namespace}/pods/{name}", self.api_url))
            .header("content-type", "application/merge-patch+json")
            .timeout(std::time::Duration::from_secs(10))
            .json(&serde_json::json!({"metadata": {"uid": uid, "annotations": annotations}}));
        let pod_name = format!("{namespace}/{name}");
        tokio::spawn(async move {
            if let Err(error) = request.send_retrying(retry::Policy::API).await.and_then(|r| r.error_for_status()) {
                warn!(%error, "Pod {pod_name}: container annotations not written");
            }
        });
    }

    /// The pod's `network-status` entries, from its sandbox (#131).
    async fn network_status_of(&self, uid: &str) -> Option<Value> {
        let sandbox = self.pods.read().await.get(uid).and_then(|p| p.sandbox_id.clone())?;
        self.runtime.pod_network_status(&sandbox).await
    }

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
        if is_static(pod) {
            crate::startup::static_pod_started(&format!("{namespace}/{name}"));
        }
        for &(phase, took) in &finished.phases {
            crate::metrics::observe_start_phase(phase, took.as_secs_f64());
        }
        // A static pod has no object to annotate.
        if self.api_url.is_empty() || pod["metadata"]["resourceVersion"].as_str().is_none() {
            return;
        }
        self.event_later(pod, crate::start_timing::REASON, &finished.text).await;
        let mut patch = serde_json::json!({"metadata": {"uid": uid, "annotations": {
            crate::start_timing::ANNOTATION: finished.text}}});
        // Each container's image and its own clone (#130).
        for (k, v) in self.container_annotations(uid).await {
            patch["metadata"]["annotations"][k] = v;
        }
        // What the CNI wired (#131), in the same write: Multus's annotation,
        // a JSON string as Multus writes it.
        if let Some(status) = self.network_status_of(uid).await {
            patch["metadata"]["annotations"][crate::network_status::ANNOTATION] =
                serde_json::json!(serde_json::to_string_pretty(&status).unwrap_or_default());
        }
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
            if let Err(error) = request.send_retrying(retry::Policy::API).await.and_then(|r| r.error_for_status()) {
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
        self.waiting_pod_as(pod, "ContainerCreating", message)
    }

    /// [`Self::waiting_pod`], with the containers' reason (#133).
    fn waiting_pod_as(&self, pod: &Value, kind: &str, message: String) -> PodStatusUpdate {
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
                    kind: kind.to_string(),
                },
            );
        PodStatusUpdate {
            reason: String::new(),
            namespace,
            name,
            phase: "Pending".to_string(),
            container_statuses: creating_statuses_as(pod, kind, &message),
            message,
            init_container_statuses: vec![],
            declared_init_containers: declared_init_containers(pod),
            pod_ip: None,
        }
    }

    /// A pod admission refused over a claim another workload here holds
    /// (#80): Pending, its containers `ContainerCreating` with `why`, and a
    /// FailedMount Event, as for any volume it waits on. The holder's release
    /// wakes it.
    pub async fn claim_held(&self, pod: &Value, why: String) -> PodStatusUpdate {
        self.event(pod, "Warning", "FailedMount", &why).await;
        self.waiting_pod(pod, why)
    }

    /// Try this pod's start again after `after`, and not before (#133).
    fn hold_off(&self, uid: &str, after: std::time::Duration) {
        self.start_holdoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(uid.to_string(), Instant::now() + after);
        self.due_in(uid, after);
    }

    /// A start that got as far as a sandbox and then failed is torn down on
    /// the next pass (`Cleanup`), so the try after it begins clean (#133).
    async fn cleanup_partial_start(&self, uid: &str) {
        if let Some(state) = self.pods.write().await.get_mut(uid) {
            if state.sandbox_id.is_some() || !state.container_ids.is_empty() {
                state.phase = "Cleanup".into();
            }
        }
    }

    /// The init containers' reports recorded for a pod so far.
    async fn recorded_inits(&self, uid: &str) -> Vec<InitContainerStatusReport> {
        self.pods.read().await.get(uid).map(|p| p.init_statuses.clone()).unwrap_or_default()
    }

    /// Record why a pod is not running, for `logs` (#133).
    fn note_waiting(&self, pod: &Value, kind: &str, message: &str) {
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        self.waiting.lock().unwrap_or_else(|e| e.into_inner()).insert(
            uid.to_string(),
            WaitingPod {
                namespace: pod["metadata"]["namespace"].as_str().unwrap_or("default").to_string(),
                name: pod["metadata"]["name"].as_str().unwrap_or("").to_string(),
                reason: message.to_string(),
                kind: kind.to_string(),
            },
        );
    }

    /// Why a pod this node has admitted is not started yet, when it is
    /// waiting (#63). `logs` answers with it rather than "not found".
    pub fn waiting_reason(&self, namespace: &str, name: &str) -> Option<String> {
        self.waiting_state(namespace, name).map(|(_, why)| why)
    }

    /// The containers' reason and why, for a pod admitted here and not
    /// running (#133): what `logs` answers instead of "not found".
    pub fn waiting_state(&self, namespace: &str, name: &str) -> Option<(String, String)> {
        self.waiting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|w| w.namespace == namespace && w.name == name)
            .map(|w| (w.kind.clone(), w.reason.clone()))
    }

    /// Where to reach a pod's ports from (#56): its uid and the sandbox's
    /// network namespace, `None` for a hostNetwork pod (the node's). `None`
    /// overall when no pod of that name runs here; an error when it does but
    /// its namespace cannot be found, never the node's network in its place.
    pub async fn pod_network(
        &self,
        namespace: &str,
        name: &str,
    ) -> Option<Result<(String, Option<String>), String>> {
        let (uid, host, sandbox) = {
            let pods = self.pods.read().await;
            let p = pods.values().find(|p| p.namespace == namespace && p.name == name)?;
            (p.uid.clone(), p.pod["spec"]["hostNetwork"].as_bool() == Some(true), p.sandbox_id.clone())
        };
        if host {
            return Some(Ok((uid, None)));
        }
        let Some(sid) = sandbox else {
            return Some(Err(format!("pod {namespace}/{name} has no sandbox yet")));
        };
        Some(match self.runtime.pod_sandbox_status(&sid).await {
            Ok(st) => st
                .netns_path
                .map(|n| (uid, Some(n)))
                .ok_or_else(|| format!("pod {namespace}/{name}: the runtime reports no network namespace")),
            Err(e) => Err(format!("pod {namespace}/{name}: {e}")),
        })
    }

    /// The runtime's id for `container` of pod `namespace/name` on this node,
    /// and the runtime (#56: `kubectl exec`, `attach`). `None`: no such pod
    /// here; `Some(Err)`: the pod has no such container running.
    pub async fn exec_target(
        &self,
        namespace: &str,
        name: &str,
        container: &str,
    ) -> Option<Result<(String, Arc<dyn RuntimeService>), String>> {
        let pods = self.pods.read().await;
        let p = pods.values().find(|p| p.namespace == namespace && p.name == name)?;
        Some(match p.container_ids.get(container) {
            Some(id) => Ok((id.clone(), self.runtime.clone())),
            None => Err(format!(
                "container {container} is not valid for pod {name}, or it is not running on this node yet"
            )),
        })
    }

    /// Record `pod` as running in `sandbox`, for tests of what reads the
    /// record (the server's streaming routes) without a start.
    #[cfg(test)]
    pub(crate) async fn record_running_for_test(&self, pod: &Value, sandbox: &str) {
        let m = &pod["metadata"];
        let s = |k: &str| m[k].as_str().unwrap_or("").to_string();
        self.pods.write().await.insert(s("uid"), PodState {
            namespace: s("namespace"), name: s("name"), uid: s("uid"),
            sandbox_id: Some(sandbox.into()),
            container_ids: pod["spec"]["containers"].as_array().into_iter().flatten()
                .filter_map(|c| c["name"].as_str())
                .map(|n| (n.to_string(), format!("ctr-{n}"))).collect(),
            phase: "Running".into(), pod: pod.clone(), pod_ip: None,
            restart_counts: HashMap::new(), ready: HashMap::new(), liveness_failures: HashMap::new(), readiness_run: HashMap::new(),
            startup_passed: HashMap::new(), started: HashMap::new(), terminated: HashMap::new(),
            init_statuses: Vec::new(), sandbox_stopped: false, unstarted: Default::default(),
        });
    }

    /// The pod whose container ran as engine workload `handle` (#115).
    pub async fn pod_of_workload(&self, handle: u64) -> Option<String> {
        self.runtime.pod_of_workload(handle).await
    }

    /// The pod one of whose containers (or whose sandbox) has this id (#116).
    pub async fn pod_of_container(&self, id: &str) -> Option<String> {
        let pods = self.pods.read().await;
        pods.values()
            .find(|p| p.sandbox_id.as_deref() == Some(id) || p.container_ids.values().any(|c| c == id))
            .map(|p| p.uid.clone())
    }

    pub async fn pod_uid(&self, namespace: &str, name: &str) -> Option<String> {
        let pods = self.pods.read().await;
        pods.values()
            .find(|p| p.namespace == namespace && p.name == name)
            .map(|p| p.uid.clone())
    }

    /// Has the current run of `container` ended (#416)? What ends a followed
    /// log (`kubectl logs -f`), as upstream: the pod gone from this node, the
    /// container terminated for good or the pod finished, an init container
    /// done, or the runtime saying the container has exited or is gone.
    /// A container not started yet (no id) has not ended. A runtime that does
    /// not answer is not taken as an exit: the stream would end too soon.
    pub async fn container_run_ended(&self, namespace: &str, name: &str, container: &str) -> bool {
        let id = {
            let pods = self.pods.read().await;
            let Some(p) = pods.values().find(|p| p.namespace == namespace && p.name == name) else {
                return true;
            };
            if p.terminated.contains_key(container)
                || p.sandbox_stopped
                || matches!(p.phase.as_str(), "Succeeded" | "Failed")
                || p.init_statuses.iter().any(|i| i.name == container && i.state == "terminated")
            {
                return true;
            }
            match p.container_ids.get(container) {
                Some(id) => id.clone(),
                None => return false,
            }
        };
        match self.runtime.container_status(&id).await {
            Ok(s) => s.state == ContainerState::Exited,
            Err(CriError::NotFound(_)) => true,
            Err(_) => false,
        }
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
                liveness_failures: HashMap::new(), readiness_run: HashMap::new(),
                startup_passed: HashMap::new(),
                started: HashMap::new(),
                terminated: HashMap::new(),
                init_statuses: Vec::new(),
                sandbox_stopped: false, unstarted: Default::default(),
            },
        );
        info!("Registered restored pod {namespace}/{name} with sandbox {sandbox_id}");
    }

    /// Stop and remove a pod, within its own grace ([`termination_grace`]).
    pub async fn stop_pod(&self, uid: &str) -> Result<(), CriError> {
        self.stop_pod_within(uid, None).await
    }

    /// [`Self::stop_pod`], with the grace a delete gave (`None`: the pod's own).
    ///
    /// The app containers are stopped together, so the pod's grace is one
    /// budget as upstream gives it, not one per container; then the sidecars,
    /// last declared first (#111). Before #225 every container got a literal
    /// 30 s, whatever the pod said: a 5 s pod whose process ignores SIGTERM
    /// was killed 30 s after its delete.
    pub async fn stop_pod_within(&self, uid: &str, grace: Option<i64>) -> Result<(), CriError> {
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
            // The app containers first, then the sidecars, last declared
            // first (#111): a sidecar (a proxy, a log shipper) outlives what
            // it serves, as upstream orders it.
            let sidecars = sidecar_names(&state.pod);
            let grace = grace.map(|g| g.max(0)).unwrap_or_else(|| termination_grace(&state.pod));
            let apps: Vec<(&String, &String)> =
                state.container_ids.iter().filter(|(n, _)| !sidecars.contains(*n)).collect();
            let pod = &state.pod;
            let stops = apps.iter().map(|(name, cid)| async move {
                // Its preStop first, inside the grace (#374).
                let grace = self.pre_stop(pod, name, cid, grace).await;
                info!("Stopping container {name} ({cid}), grace {grace}s");
                stopped(self.runtime.stop_container(cid, grace).await)
            });
            for r in futures::future::join_all(stops).await {
                r?;
            }
            for (_, cid) in &apps {
                stopped(self.runtime.remove_container(cid).await)?;
            }
            for n in sidecars.iter().rev() {
                if let Some((name, cid)) = state.container_ids.get_key_value(n) {
                    let grace = self.pre_stop(&state.pod, name, cid, grace).await;
                    info!("Stopping sidecar {name} ({cid}), grace {grace}s");
                    stopped(self.runtime.stop_container(cid, grace).await)?;
                    stopped(self.runtime.remove_container(cid).await)?;
                }
            }
            // The completed init containers kept for their logs (#47).
            let kept = self.kept_inits.lock().unwrap_or_else(|e| e.into_inner()).get(uid).cloned().unwrap_or_default();
            for cid in kept {
                stopped(self.runtime.remove_container(&cid).await)?;
                if let Some(v) = self.kept_inits.lock().unwrap_or_else(|e| e.into_inner()).get_mut(uid) {
                    v.retain(|c| c != &cid);
                }
            }
            self.kept_inits.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
            self.last_terminated.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
            self.pod_volumes.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
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
/// The states of a template still being made, which a claim waits on: the
/// engine's own (`awaiting_format`, `awaiting_seed`, stormblock
/// `fs/template.rs`) and the steps named in #383 (`formatting`, `sealing`).
const TEMPLATE_MAKING: &[&str] = &["awaiting_format", "formatting", "sealing", "awaiting_seed"];

/// What a claim does with a filesystem template the engine lists (#383).
#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplateStatus {
    /// Sealed: clone it. Also an engine that names no state (before states).
    Ready(String),
    /// Still being made ([`TEMPLATE_MAKING`]): wait, with the state named.
    Making(String, String),
    /// Final: the engine's reason. Delete it and mint it again.
    Broken(String, String),
    /// A state this kubelet does not know: a failure, never a wait.
    Unknown(String, String),
}

impl TemplateStatus {
    /// From the engine's template object: `None` without an id.
    ///
    /// stormblock lists a broken template as `"state": "broken"` with the
    /// reason in `"broken"`; either one alone is taken as broken too.
    fn of(t: &Value) -> Option<Self> {
        let id = t["id"].as_str()?.to_string();
        let state = t["state"].as_str().unwrap_or("ready");
        let reason = t["broken"].as_str().filter(|r| !r.is_empty());
        Some(match (state, reason) {
            (_, Some(r)) => Self::Broken(id, r.to_string()),
            ("broken", None) => Self::Broken(id, "the engine gave no reason".into()),
            ("ready", None) => Self::Ready(id),
            (s, None) if TEMPLATE_MAKING.contains(&s) => Self::Making(id, s.to_string()),
            (s, None) => Self::Unknown(id, s.to_string()),
        })
    }
}

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
/// Whether a [`PodManager::mint_template`] error is the engine refusing the
/// mint ([`mint_blank`]'s words), not a mint still under way (#70).
fn mint_refused(e: &str) -> bool {
    e.starts_with("stormblock would not mint")
}

async fn mint_blank(
    engine: &crate::engine::EngineClient,
    url: &str,
    blank: &str,
    class: &str,
    policy: &crate::storage::ClaimPolicy,
) -> Result<(), String> {
    // The class's filesystem and its size in MiB (stormblock has no `P`).
    let fs = crate::storage::class_fs(class)?;
    let bytes = crate::storage::SIZE_CLASSES
        .iter()
        .find(|(c, _, _)| *c == class)
        .map(|(_, b, _)| *b)
        .ok_or_else(|| format!("no size class {class}"))?;
    let mut body = serde_json::json!({
        "name": blank,
        "size": crate::storage::engine_size(bytes),
        "fs": fs,
        "role": "data",
    });
    // Its policy (#71): every claim cloned from it inherits it.
    body.as_object_mut().expect("an object").extend(policy.engine_fields());
    // Bounded by MINT_TIMEOUT, not the ordinary request bound: the answer
    // comes when the format is done (#99).
    let resp = engine
        .post_within(url, &body, crate::engine::MINT_TIMEOUT)
        .await
        .map_err(|e| format!("stormblock would not mint the blank {blank}: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let text = resp.text().await.unwrap_or_default();
    // A 409 is "it exists already" (a mint that outlived its caller) or a
    // policy the node cannot place on distinct domains (stormblock#151):
    // only the first is a success (#71).
    if status.as_u16() == 409 && text.to_ascii_lowercase().contains("exist") {
        return Ok(());
    }
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
    /// The template itself is gone (`fstemplate … not found`): made again by
    /// a mint, as one never made is (#383).
    fn template_gone(&self) -> bool {
        self.status == Some(404) && self.body.contains("fstemplate") && self.body.contains("not found")
    }

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

/// `status.reason` of a pod refused for an AppArmor profile (upstream's).
pub const APPARMOR_REASON: &str = "AppArmor";

/// Why this pod cannot run here, when it asks for AppArmor (#23).
///
/// Upstream's admission: a pod that asks for any profile other than
/// `Unconfined` (pod or container `securityContext.appArmorProfile`, or the
/// deprecated `container.apparmor.security.beta.kubernetes.io/<container>`
/// annotation, which 1.34 deprecated for the field) is refused on a node that
/// cannot enforce it. No runtime here applies one (stormpump has no LSM
/// profile and the CRI path does not forward it), so
/// such a pod is refused rather than run unconfined in silence.
pub fn apparmor_refusal(pod: &Value) -> Option<String> {
    let asks = |sc: &Value| {
        sc["appArmorProfile"]["type"]
            .as_str()
            .is_some_and(|t| t != "Unconfined")
    };
    let mut wants = Vec::new();
    if asks(&pod["spec"]["securityContext"]) {
        wants.push("the pod".to_string());
    }
    for list in ["initContainers", "containers", "ephemeralContainers"] {
        for c in pod["spec"][list].as_array().into_iter().flatten() {
            if asks(&c["securityContext"]) {
                wants.push(format!("container {}", c["name"].as_str().unwrap_or("?")));
            }
        }
    }
    const PREFIX: &str = "container.apparmor.security.beta.kubernetes.io/";
    if let Some(a) = pod["metadata"]["annotations"].as_object() {
        let mut keys: Vec<_> = a.iter().collect();
        keys.sort_by(|x, y| x.0.cmp(y.0));
        for (k, v) in keys {
            if let Some(c) = k.strip_prefix(PREFIX) {
                if v.as_str().is_some_and(|v| v != "unconfined") {
                    let who = format!("container {c}");
                    if !wants.contains(&who) {
                        wants.push(who);
                    }
                }
            }
        }
    }
    (!wants.is_empty()).then(|| {
        format!(
            "Cannot enforce AppArmor: this node's runtime applies no AppArmor profile, and {} asks for one",
            wants.join(", ")
        )
    })
}


/// Status update to send back to the API server.
#[derive(Debug)]
pub struct PodStatusUpdate {
    pub namespace: String,
    pub name: String,
    pub phase: String,
    pub message: String,
    /// `status.reason` when it is upstream's own word for the outcome (an
    /// admission refusal: `AppArmor`, #23); empty: the kubelet's default.
    pub reason: String,
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

/// A container's previous run (#130): `lastState.terminated`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastTerminated {
    pub exit_code: i32,
    pub reason: String,
    pub message: String,
    /// Epoch nanoseconds; 0 unknown.
    pub started_at: i64,
    pub finished_at: i64,
}

impl LastTerminated {
    fn of(status: &crate::cri::ContainerStatusInfo) -> LastTerminated {
        LastTerminated {
            exit_code: status.exit_code,
            reason: terminated_reason(&status.reason, status.exit_code).to_string(),
            message: status.message.clone(),
            started_at: status.started_at,
            finished_at: status.finished_at,
        }
    }
}

/// A terminated container's `reason` (#130): the runtime's (`OOMKilled`,
/// `StartError`), else upstream's `Completed` for exit 0 and `Error` for the
/// rest.
pub fn terminated_reason(runtime: &str, exit_code: i32) -> &str {
    if !runtime.is_empty() {
        runtime
    } else if exit_code == 0 {
        "Completed"
    } else {
        "Error"
    }
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
/// A container's create or start failure, with upstream's reason for it
/// (#133). Waits stay waits.
fn container_error(e: CriError, container: &str, reason: &str) -> CriError {
    match e {
        CriError::Runtime(_) | CriError::NotFound(_) | CriError::Timeout | CriError::ImagePull(_) | CriError::Migration(_) => {
            CriError::Container { container: container.into(), reason: reason.into(), message: e.to_string() }
        }
        other => other,
    }
}

fn creating_statuses(pod: &Value, message: &str) -> Vec<ContainerStatusReport> {
    creating_statuses_as(pod, "ContainerCreating", message)
}

/// A pod that did not start, as upstream reports it (#133): `failed` (every
/// app container when empty) terminated `StartError`, exit 128, with why; the
/// others never ran and say so.
fn failed_start_statuses(pod: &Value, failed: &str, message: &str) -> Vec<ContainerStatusReport> {
    let mut out = creating_statuses_as(pod, "ContainerCreating", message);
    for cs in &mut out {
        if failed.is_empty() || cs.name == failed {
            cs.state = "terminated".into();
            cs.reason = "StartError".into();
            cs.exit_code = 128;
        } else {
            cs.message = format!("not started: container {failed} failed to start");
        }
    }
    out
}

/// Every app container waiting with `kind` as its reason (#133).
fn creating_statuses_as(pod: &Value, kind: &str, message: &str) -> Vec<ContainerStatusReport> {
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
                    reason: kind.to_string(),
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
/// The real-time admission (#430), before anything is made: `realtime-hard`
/// without `PREEMPT_RT`, and any mistake in what the pod asked (the field
/// named), refused with a Warning `RealtimeRefused` and a waiting reason.
fn realtime_admission(pod: &Value, handler: &str, preempt: crate::realtime::Preempt) -> Result<(), CriError> {
    let refused = |message: String| CriError::Refused { reason: crate::realtime::REFUSED.into(), message };
    if let Some(why) = crate::realtime::hard_refusal(handler, preempt) {
        return Err(refused(why));
    }
    crate::realtime::for_pod(pod, handler).map(|_| ()).map_err(refused)
}

/// One container's real time (#430), as the admission checked it.
fn realtime_of(pod: &Value, handler: &str, container: &str) -> Option<crate::realtime::Realtime> {
    crate::realtime::for_pod(pod, handler).ok()?.into_iter().find(|(n, _)| n == container).map(|(_, rt)| rt)
}

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
        port_mappings: host_port_mappings(pod),
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
        qos_class: qos_class(pod).to_string(),
        // Resolved against the apiserver before the sandbox (#233).
        networks: Vec::new(),
        default_network: None,
        // From the pod's RuntimeClass, before the sandbox (#204).
        runtime_handler: String::new(),
    }
}

/// The node ports a pod's containers ask for (#437, 2eee3c8b1cb1): every
/// `ports[]` entry with a `hostPort`, its protocol (TCP unless said) and
/// `hostIP`. A hostNetwork pod's ports are the node's already, so it has none.
fn host_port_mappings(pod: &Value) -> Vec<crate::cri::PortMapping> {
    if pod["spec"]["hostNetwork"].as_bool().unwrap_or(false) {
        return Vec::new();
    }
    let in_range = |v: &Value| v.as_i64().filter(|p| (1..=65535).contains(p)).map(|p| p as i32);
    ["initContainers", "containers"]
        .iter()
        .flat_map(|k| pod["spec"][*k].as_array().into_iter().flatten())
        .flat_map(|c| c["ports"].as_array().into_iter().flatten())
        .filter_map(|p| {
            Some(crate::cri::PortMapping {
                host_port: in_range(&p["hostPort"])?,
                container_port: in_range(&p["containerPort"])?,
                protocol: p["protocol"].as_str().unwrap_or("TCP").to_string(),
                host_ip: p["hostIP"].as_str().unwrap_or("").to_string(),
            })
        })
        .collect()
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

/// The grace a container gets when it is stopped to be restarted or after
/// a probe gave up on it.
const RESTART_STOP_GRACE_SECS: i64 = 5;

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

/// A container's mounts, the same on its first start and every restart
/// (#437, 6c156be85e7f: a restart rebuilt them without the generated
/// `/etc/resolv.conf`, so a restarted container lost cluster DNS): its
/// volumes and devices, then the ServiceAccount credentials and the
/// kubelet's resolv.conf where the container does not mount its own.
fn container_mounts(
    spec: &Value,
    volumes: &HashMap<String, ResolvedVolume>,
    env: &[(String, String)],
    sa_mount: &Option<Mount>,
    dns_mount: &Option<Mount>,
) -> Result<Vec<Mount>, String> {
    let mut mounts = resolve_mounts(spec, volumes, env)?;
    push_mount(&mut mounts, sa_mount.clone());
    push_mount(&mut mounts, dns_mount.clone());
    Ok(mounts)
}

/// The two API objects whose keys the kubelet writes as files or env.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum KeyedKind {
    ConfigMap,
    Secret,
}

impl KeyedKind {
    fn resource(self) -> &'static str {
        match self {
            KeyedKind::ConfigMap => "configmaps",
            KeyedKind::Secret => "secrets",
        }
    }

    fn describe(self, namespace: &str, name: &str) -> String {
        match self {
            KeyedKind::ConfigMap => format!("configmap {namespace}/{name}"),
            KeyedKind::Secret => format!("secret {namespace}/{name}"),
        }
    }

    /// The mode a key's file gets when neither the item nor the volume says.
    fn default_mode(self) -> u32 {
        match self {
            KeyedKind::ConfigMap => 0o644,
            KeyedKind::Secret => 0o600,
        }
    }
}

/// A ConfigMap's or Secret's keys and their values as bytes (#437).
type KeyedData = std::collections::BTreeMap<String, Vec<u8>>;

/// The keys of a fetched ConfigMap (`data` as text, `binaryData` base64) or
/// Secret (`data` base64), byte for byte.
fn keyed_data(kind: KeyedKind, obj: &Value) -> Result<KeyedData, String> {
    let mut out = KeyedData::new();
    let decode = |field: &str, out: &mut KeyedData| -> Result<(), String> {
        for (k, v) in obj[field].as_object().into_iter().flatten() {
            let b64 = v.as_str().unwrap_or("");
            let bytes = base64_decode(b64).map_err(|()| format!("{field}.{k} is not base64"))?;
            out.insert(k.clone(), bytes);
        }
        Ok(())
    };
    match kind {
        KeyedKind::ConfigMap => {
            for (k, v) in obj["data"].as_object().into_iter().flatten() {
                out.insert(k.clone(), v.as_str().unwrap_or("").as_bytes().to_vec());
            }
            decode("binaryData", &mut out)?;
        }
        KeyedKind::Secret => decode("data", &mut out)?,
    }
    Ok(out)
}

/// One file a configMap or secret source writes: its path in the volume,
/// its bytes and its mode.
#[derive(Debug, PartialEq)]
struct KeyedFile {
    path: String,
    bytes: Vec<u8>,
    mode: u32,
}

/// The files a configMap or secret source (a volume, or a projected
/// volume's source) writes (#437, 1f318a543243 and e635cb20b82e).
///
/// With `items`, only the listed keys, each at its `path` (its own name when
/// the item has none) and its `mode`; without, every key at its own name.
/// The volume's `defaultMode`, else `default_mode`, for the rest.
///
/// `data` is `None` when the object does not exist. A missing object, or a
/// listed key it does not have, is skipped only when the source is
/// `optional`; otherwise `Err` says which, and the pod waits for it.
fn keyed_files(
    what: &str,
    data: Option<&KeyedData>,
    src: &Value,
    default_mode: u32,
) -> Result<Vec<KeyedFile>, String> {
    let optional = src["optional"].as_bool().unwrap_or(false);
    let Some(data) = data else {
        return if optional { Ok(Vec::new()) } else { Err(format!("{what} not found")) };
    };
    let mode_of = |v: &Value, or: u32| v.as_u64().map(|m| m as u32 & 0o777).unwrap_or(or);
    let default_mode = mode_of(&src["defaultMode"], default_mode);
    let mut out = Vec::new();
    match src["items"].as_array() {
        Some(items) => {
            for it in items {
                let key = it["key"].as_str().unwrap_or("");
                let Some(bytes) = data.get(key) else {
                    if optional {
                        continue;
                    }
                    return Err(format!("{what} has no key {key:?}"));
                };
                let path = it["path"].as_str().filter(|p| !p.is_empty()).unwrap_or(key);
                if crate::api_files::contained(path).is_none() {
                    return Err(format!("{what}: item path {path:?} is not a path inside the volume"));
                }
                out.push(KeyedFile {
                    path: path.to_string(),
                    bytes: bytes.clone(),
                    mode: mode_of(&it["mode"], default_mode),
                });
            }
        }
        None => {
            for (k, bytes) in data {
                out.push(KeyedFile { path: k.clone(), bytes: bytes.clone(), mode: default_mode });
            }
        }
    }
    Ok(out)
}

/// Write `files` into `dir` (symlink-safe, `api_files`). With `prune`, also
/// remove what `dir` holds that `files` does not name, so a key dropped from
/// the object or the `items` list is gone at the next write (#437,
/// 1f318a543243). A projected volume shares its directory between sources
/// (and its token), so it does not prune.
fn write_keyed(dir: &str, files: &[KeyedFile], prune: bool) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    for f in files {
        crate::api_files::write_in(dir, &f.path, &f.bytes, f.mode)?;
    }
    if prune {
        let keep: std::collections::HashSet<std::path::PathBuf> = files
            .iter()
            .filter_map(|f| crate::api_files::contained(&f.path))
            .collect();
        let parents: std::collections::HashSet<std::path::PathBuf> = keep
            .iter()
            .flat_map(|p| p.ancestors().skip(1).map(std::path::Path::to_path_buf))
            .filter(|p| !p.as_os_str().is_empty())
            .collect();
        prune_dir(std::path::Path::new(dir), std::path::Path::new(""), &keep, &parents)?;
    }
    Ok(())
}

/// Remove everything under `root/rel` that is neither in `keep` nor a
/// directory on the way to something in it. Symlinks are removed, never followed.
fn prune_dir(
    root: &std::path::Path,
    rel: &std::path::Path,
    keep: &std::collections::HashSet<std::path::PathBuf>,
    parents: &std::collections::HashSet<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root.join(rel))? {
        let entry = entry?;
        let r = rel.join(entry.file_name());
        if keep.contains(&r) {
            continue;
        }
        let ft = entry.file_type()?;
        if ft.is_dir() && parents.contains(&r) {
            prune_dir(root, &r, keep, parents)?;
        } else if ft.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// A name `envFrom` may set: not empty, no `=`, printable ASCII (what
/// upstream accepts since RelaxedEnvironmentVariableValidation).
fn valid_env_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| (0x20..0x7f).contains(&b) && b != b'=')
}

/// Set `name` to `value`, replacing an earlier value of the same name in
/// place (a later env entry, or `env` over `envFrom`, wins).
fn set_env(env: &mut Vec<(String, String)>, name: String, value: String) {
    match env.iter_mut().find(|(k, _)| *k == name) {
        Some(e) => e.1 = value,
        None => env.push((name, value)),
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

/// Where a claim move's cutover stands when its step answered an error
/// (#428, finding e4aaa109678b).
#[derive(Debug, PartialEq, Eq)]
enum Cutover {
    /// The copy is cut over (or completed, the state it is in): go on to
    /// retire the source and point the PV at the target.
    Done(String),
    /// Not known, or the step failed on the way (a timeout, a 5xx): ask
    /// again next pass, the claim untouched.
    Ask,
    /// Still before the cutover and stormstorage said no: abort the copy.
    Refused(String),
    /// The copy failed or was aborted on stormstorage's side.
    Ended(String),
}

/// What a cutover error means, from the migration read again after it.
fn cutover_outcome(err: &str, now: Result<crate::vm_migration::DiskMigration, String>) -> Cutover {
    let Ok(mig) = now else { return Cutover::Ask };
    match mig.state.as_str() {
        "cut_over" | "completed" => Cutover::Done(mig.state.clone()),
        "failed" | "aborted" => Cutover::Ended(format!("the copy {}: {}", mig.state, mig.message)),
        "synced" if retry::class_of_message(err) == retry::Class::Real => {
            Cutover::Refused(format!("the cutover was refused: {err}"))
        }
        _ => Cutover::Ask,
    }
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

/// One ServiceAccount token file this kubelet keeps fresh (#122).
#[derive(Debug, Clone, PartialEq)]
struct TokenFile {
    file: String,
    namespace: String,
    sa: String,
    audience: Option<String>,
    /// The pod it is bound to (name, uid).
    pod: Option<(String, String)>,
    expiration_secs: i64,
    refresh_at: chrono::DateTime<chrono::Utc>,
}

/// The `TokenRequest` body: audiences, and (#122) the lifetime and the pod
/// the token is bound to.
fn token_request(audience: Option<&str>, pod: Option<(&str, &str)>, expiration_secs: Option<i64>) -> Value {
    let mut spec = serde_json::json!({ "audiences": audience.map(|a| vec![a]).unwrap_or_default() });
    if let Some(secs) = expiration_secs {
        spec["expirationSeconds"] = serde_json::json!(secs);
    }
    if let Some((name, uid)) = pod {
        spec["boundObjectRef"] = serde_json::json!({ "kind": "Pod", "apiVersion": "v1", "name": name, "uid": uid });
    }
    serde_json::json!({ "apiVersion": "authentication.k8s.io/v1", "kind": "TokenRequest", "spec": spec })
}

/// When a token issued `now` is written again: at 80% of its life, from the
/// apiserver's `expirationTimestamp` when it gave one (it may grant longer
/// than asked), else from what was asked (#122).
fn refresh_at(
    now: chrono::DateTime<chrono::Utc>,
    expires: Option<chrono::DateTime<chrono::Utc>>,
    asked_secs: i64,
) -> chrono::DateTime<chrono::Utc> {
    let life = expires.map(|e| (e - now).num_seconds()).filter(|s| *s > 0).unwrap_or(asked_secs);
    now + chrono::Duration::seconds(life * 4 / 5)
}

/// Resolve a container's `volumeMounts` (and `volumeDevices`, #67) to CRI
/// mounts using the pod's resolved volumes. Mounts whose volume didn't
/// resolve are dropped.
/// A container's mounts and devices. `env` is the container's resolved env,
/// which a `subPathExpr` expands. `Err` is why a mount cannot be made as
/// asked (a `subPath` that leaves its volume, goes through a symlink, or is
/// on a volume this node cannot take a subpath of): the container is not
/// created, as upstream's `CreateContainerConfigError`.
fn resolve_mounts(
    spec: &Value,
    volumes: &HashMap<String, ResolvedVolume>,
    env: &[(String, String)],
) -> Result<Vec<Mount>, String> {
    let mut mounts = resolve_volume_mounts(spec, volumes, env)?;
    mounts.extend(resolve_volume_devices(spec, volumes));
    Ok(mounts)
}

/// The host path of `sub` inside the directory volume at `base` (#437,
/// 0f6ec9a72d6c), checked where the host sees it (`on_host`).
///
/// `sub` is relative and plain (no `..`, no absolute path), and nothing on
/// the way from `base` to it may be a symlink: a container that can write
/// the volume could otherwise point a subpath anywhere on the node. Each
/// directory on the way that is not there is made (as upstream does) with
/// the volume's own mode when `create`; the last component may be a file,
/// which binds that one file (a ConfigMap key at `/etc/app.conf`).
fn subpath_on_host(base: &str, sub: &str, create: bool) -> Result<String, String> {
    let rel = crate::api_files::contained(sub)
        .ok_or_else(|| format!("subPath {sub:?} is not a path inside the volume"))?;
    let base_view = on_host(base);
    let base_meta = std::fs::metadata(&base_view)
        .map_err(|e| format!("subPath {sub:?}: volume directory {base}: {e}"))?;
    if !base_meta.is_dir() {
        return Err(format!("subPath {sub:?}: volume {base} is not a directory"));
    }
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        base_meta.permissions().mode() & 0o7777
    };
    let parts: Vec<_> = rel.components().collect();
    let mut cur = base_view;
    for (i, part) in parts.iter().enumerate() {
        cur.push(part);
        let last = i + 1 == parts.len();
        match std::fs::symlink_metadata(&cur) {
            Ok(m) if m.file_type().is_symlink() => {
                return Err(format!(
                    "subPath {sub:?}: {} is a symlink; a subpath may not go through one",
                    cur.display()
                ))
            }
            Ok(m) if m.is_dir() || last => {}
            Ok(_) => return Err(format!("subPath {sub:?}: {} is not a directory", cur.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && create => {
                match std::fs::create_dir(&cur) {
                    Ok(()) => {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(&cur, std::fs::Permissions::from_mode(mode))
                            .map_err(|e| format!("subPath {sub:?}: {}: {e}", cur.display()))?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(format!("subPath {sub:?}: cannot make {}: {e}", cur.display())),
                }
                // Made by us or by someone at the same moment: either way
                // it must be a real directory now.
                let m = std::fs::symlink_metadata(&cur)
                    .map_err(|e| format!("subPath {sub:?}: {}: {e}", cur.display()))?;
                if !m.is_dir() {
                    return Err(format!("subPath {sub:?}: {} is not a directory", cur.display()));
                }
            }
            Err(e) => return Err(format!("subPath {sub:?}: {}: {e}", cur.display())),
        }
    }
    Ok(format!("{}/{}", base.trim_end_matches('/'), rel.display()))
}

/// Where a volume mount binds from: the volume, or its `subPath` /
/// `subPathExpr` inside it (#437, 0f6ec9a72d6c). Never the volume's root
/// for a mount that asked for a path inside it.
fn mount_source(
    m: &Value,
    resolved: &ResolvedVolume,
    env: &[(String, String)],
) -> Result<String, String> {
    let vol = m["name"].as_str().unwrap_or("");
    let sub = match (
        m["subPath"].as_str().filter(|s| !s.is_empty()),
        m["subPathExpr"].as_str().filter(|s| !s.is_empty()),
    ) {
        (None, None) => return Ok(resolved.path.clone()),
        (Some(_), Some(_)) => {
            return Err(format!("volume {vol}: subPath and subPathExpr are mutually exclusive"))
        }
        (Some(s), None) => s.to_string(),
        (None, Some(e)) => {
            // `$(NAME)` from the container's env, as its command's are.
            let vars: HashMap<&str, &str> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            expand_env_refs(e, &vars)
        }
    };
    if resolved.fstype.is_some() || resolved.block {
        // The engine mounts a claim's filesystem from its device inside the
        // container, whole; it has no way to bind a directory of it yet
        // (stormpump#274). Binding the whole volume would expose what the
        // mount meant to keep out.
        return Err(format!(
            "volume {vol}: subPath {sub:?} on a claim this node mounts from its device is not \
             available (stormpump#274)"
        ));
    }
    // The kubelet's API volumes are written whole on every pass; a subpath
    // in one must name what is there, never make a directory in it.
    subpath_on_host(&resolved.path, &sub, !resolved.api).map_err(|e| format!("volume {vol}: {e}"))
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

fn resolve_volume_mounts(
    spec: &Value,
    volumes: &HashMap<String, ResolvedVolume>,
    env: &[(String, String)],
) -> Result<Vec<Mount>, String> {
    spec["volumeMounts"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| {
                    let vol_name = m["name"].as_str().unwrap_or("");
                    let resolved = volumes.get(vol_name)?;
                    let host_path = match mount_source(m, resolved, env) {
                        Ok(p) => p,
                        Err(e) => return Some(Err(e)),
                    };
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
                    Some(Ok(Mount {
                        container_path: m["mountPath"].as_str().unwrap_or("").to_string(),
                        host_path,
                        readonly: resolved.api || m["readOnly"].as_bool().unwrap_or(false),
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
                    }))
                })
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn probe_initial_delay(probe: &Value) -> u64 {
    probe["initialDelaySeconds"].as_u64().unwrap_or(0)
}

fn probe_failure_threshold(probe: &Value) -> u32 {
    probe["failureThreshold"].as_u64().unwrap_or(3) as u32
}

/// A probe's `successThreshold` (default 1, at least 1, as the API's).
fn probe_success_threshold(probe: &Value) -> u32 {
    u32::try_from(probe["successThreshold"].as_u64().unwrap_or(1)).unwrap_or(u32::MAX).max(1)
}

/// A readiness probe's last result and how many times in a row it has given
/// it (#460, cd264eea6874).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadinessRun {
    /// The last result: passed.
    pub ok: bool,
    /// How many times in a row; 0 before the first.
    pub count: u32,
}

impl ReadinessRun {
    /// The readiness after one more result, from `was`: it changes only once
    /// `successThreshold` successes (to ready) or `failureThreshold` failures
    /// (to not ready) have come in a row, as upstream's prober worker. A
    /// container starts not ready, so a first success with
    /// `successThreshold: 3` is not yet ready, and one failure of a ready
    /// container with `failureThreshold: 3` leaves it ready.
    pub fn after(&mut self, was: bool, ok: bool, success_threshold: u32, failure_threshold: u32) -> bool {
        if self.count > 0 && self.ok == ok {
            self.count = self.count.saturating_add(1);
        } else {
            *self = ReadinessRun { ok, count: 1 };
        }
        let threshold = if ok { success_threshold } else { failure_threshold }.max(1);
        if self.count >= threshold { ok } else { was }
    }
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

/// Give a container about to be created its run's own log file (#216): the
/// next `<N>.log` in its directory, old runs pruned
/// ([`crate::container_logs::new_run`]).
fn new_log_run(config: &mut ContainerConfig, log_directory: &str, at_least: u32) {
    let n = crate::container_logs::new_run(log_directory, &config.name, at_least);
    config.log_path = format!("{}/{n}.log", config.name);
}

/// How long a pod's containers get between SIGTERM and SIGKILL (#225): the
/// delete's `metadata.deletionGracePeriodSeconds` when the object carries it
/// (a `--grace-period` that shortens it), else `spec.terminationGracePeriodSeconds`,
/// else upstream's 30. Never negative.
pub fn termination_grace(pod: &Value) -> i64 {
    pod["metadata"]["deletionGracePeriodSeconds"]
        .as_i64()
        .or_else(|| pod["spec"]["terminationGracePeriodSeconds"].as_i64())
        .unwrap_or(30)
        .max(0)
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
    let capabilities = |which: &str| -> Vec<String> {
        sc["capabilities"][which]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let add_capabilities = capabilities("add");
    // Dropped too (#118): a pod that drops NET_RAW must not keep it.
    let drop_capabilities = capabilities("drop");

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
        stdin_once: spec["stdinOnce"].as_bool().unwrap_or(false),
        tty: spec["tty"].as_bool().unwrap_or(false),
        cpu_period: 100_000,
        cpu_quota: parse_cpu_quota(cpu_limit),
        cpu_shares: parse_cpu_shares(cpu_request),
        memory_limit_bytes: parse_memory_bytes(mem_limit),
        // The pod's values fill in what the container leaves unset
        // (`apply_pod_namespaces`), and the image's User what both do (#98).
        run_as_user: sc["runAsUser"].as_i64(),
        run_as_group: sc["runAsGroup"].as_i64(),
        run_as_non_root: sc["runAsNonRoot"].as_bool(),
        privileged: sc["privileged"].as_bool().unwrap_or(false),
        readonly_rootfs: sc["readOnlyRootFilesystem"].as_bool().unwrap_or(false),
        add_capabilities,
        drop_capabilities,
        no_new_privs: sc["allowPrivilegeEscalation"].as_bool() == Some(false),
        selinux_options,
        host_network: false,
        host_pid: false,
        host_ipc: false,
        share_process_namespace: false,
        // Container-level seccompProfile; a pod-level one is filled in later by
        // apply_pod_namespaces when the container doesn't set its own.
        seccomp_profile: parse_seccomp(sc),
        // The pod's, set by apply_pod_namespaces (#57).
        qos_class: String::new(),
        // The pod's, set by apply_pod_namespaces (#171).
        fs_group: None,
        fs_group_change_policy: None,
        supplemental_groups: Vec::new(),
        // The node's RDMA verbs devices, when asked for (#348).
        rdma: crate::rdma::requested(spec),
        // Set by the caller from the pod's RuntimeClass (#430, `realtime_of`).
        realtime: None,
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
/// A pod's QoS class, by upstream's rule (#57): `BestEffort` when no container
/// asks for or limits CPU or memory; `Guaranteed` when every container limits
/// both and asks for exactly that (a request left unset is its limit);
/// `Burstable` otherwise. Init containers count as containers do.
pub fn qos_class(pod: &Value) -> &'static str {
    let containers: Vec<&Value> = ["initContainers", "containers"]
        .iter()
        .flat_map(|k| pod["spec"][*k].as_array().into_iter().flatten())
        .collect();
    let res = |c: &Value, side: &str, r: &str| c["resources"][side][r].as_str().filter(|v| !v.is_empty()).map(str::to_string);
    let any = containers.iter().any(|c| {
        ["requests", "limits"].iter().any(|side| ["cpu", "memory"].iter().any(|r| res(c, side, r).is_some()))
    });
    if !any {
        return "BestEffort";
    }
    let guaranteed = !containers.is_empty()
        && containers.iter().all(|c| {
            ["cpu", "memory"].iter().all(|r| match res(c, "limits", r) {
                Some(limit) => res(c, "requests", r).map_or(true, |req| {
                    if *r == "cpu" {
                        parse_cpu_shares(&req) == parse_cpu_shares(&limit)
                    } else {
                        crate::storage::parse_quantity(&req) == crate::storage::parse_quantity(&limit)
                    }
                }),
                None => false,
            })
        });
    if guaranteed { "Guaranteed" } else { "Burstable" }
}

fn apply_pod_namespaces(config: &mut ContainerConfig, pod: &Value) {
    let spec = &pod["spec"];
    config.qos_class = qos_class(pod).to_string();
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
    // runAsUser / runAsGroup / runAsNonRoot: the container's own, else the
    // pod's (#98). An explicit `runAsNonRoot: false` on the container wins.
    let pod_sc = &spec["securityContext"];
    if config.run_as_user.is_none() {
        config.run_as_user = pod_sc["runAsUser"].as_i64();
    }
    if config.run_as_group.is_none() {
        config.run_as_group = pod_sc["runAsGroup"].as_i64();
    }
    if config.run_as_non_root.is_none() {
        config.run_as_non_root = pod_sc["runAsNonRoot"].as_bool();
    }
    // fsGroup, its change policy and supplementalGroups are the pod's only
    // (#171): a container's securityContext has none of them.
    config.fs_group = pod_sc["fsGroup"].as_u64().and_then(|g| u32::try_from(g).ok());
    config.fs_group_change_policy = pod_sc["fsGroupChangePolicy"].as_str().map(String::from);
    config.supplemental_groups = pod_sc["supplementalGroups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|g| g.as_u64().and_then(|g| u32::try_from(g).ok()))
        .collect();
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

    /// #469: where each container mounts a volume, init (sidecar) ones too;
    /// a raw block device is not a mount.
    #[test]
    fn volume_mount_paths_lists_each_containers_mount() {
        let spec = serde_json::json!({
            "containers": [
                {"name": "app", "volumeMounts": [{"name": "data", "mountPath": "/var/lib/pg"},
                                                  {"name": "cfg", "mountPath": "/etc/app"}]},
                {"name": "raw", "volumeDevices": [{"name": "data", "devicePath": "/dev/xvda"}]}
            ],
            "initContainers": [
                {"name": "backup", "restartPolicy": "Always",
                 "volumeMounts": [{"name": "data", "mountPath": "/src", "readOnly": true}]}
            ]
        });
        assert_eq!(
            volume_mount_paths(&spec, "data"),
            [("app".to_string(), "/var/lib/pg".to_string()), ("backup".to_string(), "/src".to_string())]
        );
        assert!(volume_mount_paths(&spec, "none").is_empty());
    }
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
        /// Images that do not pull, and container names whose create or
        /// start fails (#133).
        missing_images: Mutex<std::collections::HashSet<String>>,
        fail_create: Mutex<std::collections::HashSet<String>>,
        fail_start: Mutex<std::collections::HashSet<String>>,
        /// Container ids whose remove fails (#437).
        fail_remove: Mutex<std::collections::HashSet<String>>,
        /// Every stop_container: the container and its grace (#225).
        stop_graces: Mutex<Vec<(String, i64)>>,
        /// Every exec_sync: the container and its command (#374), and the
        /// stops among them, in order: `exec <cmd>` / `stop <cid>`.
        execs: Mutex<Vec<(String, Vec<String>)>>,
        order: Mutex<Vec<String>>,
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
        async fn container_image_info(&self, container_id: &str) -> Option<crate::cri::ContainerImageInfo> {
            let mut build = serde_json::Map::new();
            build.insert("version".into(), json!("1.0"));
            Some(crate::cri::ContainerImageInfo {
                image_id: Some("sha256:00".into()),
                golden: Some("img-0".into()),
                instance: Some(format!("vol-{container_id}")),
                resolved_at: Some("2026-10-08T00:00:00Z".into()),
                build,
            })
        }

        async fn pod_network_status(&self, sandbox_id: &str) -> Option<Value> {
            Some(json!([{"name": "fake", "interface": "eth0", "sandbox": sandbox_id}]))
        }

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
                // As the stormpump runtime reports it (#139).
                made: Some(crate::cri::SandboxSteps {
                    acquire: std::time::Duration::from_millis(1),
                    cni: std::time::Duration::from_millis(2),
                }),
            })
        }

        async fn container_steps(&self, _id: &str) -> Option<crate::cri::ContainerSteps> {
            // As the stormpump runtime reports them (#311).
            Some(crate::cri::ContainerSteps {
                clone: std::time::Duration::from_millis(3),
                spawn: std::time::Duration::from_millis(4),
                ..Default::default()
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
            if self.fail_create.lock().unwrap().contains(&config.name) {
                return Err(CriError::Runtime(format!("{}: injected create failure", config.name)));
            }
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
            if self.fail_start.lock().unwrap().contains(&c.name) {
                return Err(CriError::Runtime(format!("{}: injected start failure (exec: no such file)", c.name)));
            }
            // Init containers exit immediately on start when configured to.
            if let Some(&code) = self.exit_on_start.lock().unwrap().get(&c.name) {
                c.state = ContainerState::Exited;
                c.exit_code = code;
            } else {
                c.state = ContainerState::Running;
            }
            Ok(())
        }

        async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError> {
            self.stop_graces.lock().unwrap().push((container_id.to_string(), timeout));
            self.order.lock().unwrap().push(format!("stop {container_id}"));
            if self.fail_stop.load(Ordering::SeqCst) {
                return Err(CriError::Connection("injected outage".into()));
            }
            if let Some(c) = self.containers.lock().unwrap().get_mut(container_id) {
                c.state = ContainerState::Exited;
            }
            Ok(())
        }

        async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
            if self.fail_remove.lock().unwrap().contains(container_id) {
                return Err(CriError::Runtime(format!("{container_id}: injected remove failure")));
            }
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
            container_id: &str,
            cmd: &[String],
            _timeout: i64,
        ) -> Result<ExecSyncResult, CriError> {
            self.execs.lock().unwrap().push((container_id.to_string(), cmd.to_vec()));
            self.order.lock().unwrap().push(format!("exec {}", cmd.join(" ")));
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
            if self.missing_images.lock().unwrap().contains(image) {
                return Err(CriError::ImagePull(format!("{image}: manifest unknown")));
            }
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
            liveness_failures: HashMap::new(), readiness_run: HashMap::new(),
            startup_passed: HashMap::new(),
            started: HashMap::new(),
            terminated: HashMap::new(),
            init_statuses: Vec::new(),
            sandbox_stopped: false, unstarted: Default::default(),
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

    /// #70: the claim says what its blank is doing. A mint under way is a
    /// Normal `Provisioning`, a refused mint a Warning `ProvisioningFailed`
    /// with the engine's words, and the clone `ProvisioningSucceeded`, all on
    /// the PersistentVolumeClaim (the pod's FailedMount was all there was).
    #[tokio::test]
    async fn the_claim_says_what_its_blank_is_doing() {
        use axum::routing::{get as http_get, post as http_post};
        let events = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let ready = Arc::new(AtomicBool::new(false));
        let (ev, r1, r2) = (events.clone(), ready.clone(), ready.clone());
        let app = axum::Router::new()
            .route("/api/v1/namespaces/{ns}/events", http_post(move |axum::Json(b): axum::Json<Value>| {
                ev.lock().unwrap().push(b);
                async { (axum::http::StatusCode::CREATED, axum::Json(json!({}))) }
            }))
            // No room for the blank: the engine refuses the mint.
            .route("/api/v1/fstemplates", http_post(|| async {
                // Not at once: the first ask sees the mint under way.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                (axum::http::StatusCode::INSUFFICIENT_STORAGE, "no data slab has 64 MiB free")
            }))
            .route("/api/v1/fstemplates/{name}", http_get(move || {
                let ready = r1.load(Ordering::SeqCst);
                async move {
                    if ready { Ok(axum::Json(json!({"id": "tpl-64", "state": "ready"}))) }
                    else { Err(axum::http::StatusCode::NOT_FOUND) }
                }
            }))
            .route("/api/v1/fstemplates/{id}/clone", http_post(move || {
                let _ = &r2;
                async { axum::Json(json!({"volume_id": "vol-d"})) }
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new());
        let mgr = with_stormblock(mgr, &url);
        let claim = json!({"metadata": {"namespace": "ns", "name": "d", "uid": "pvc-uid"}});
        let said = |reason: &str| {
            events.lock().unwrap().iter().filter(|e| e["reason"] == reason).cloned().collect::<Vec<_>>()
        };

        // First ask: the mint starts in the background; the claim says so.
        assert!(mgr.clone_blank("64M", "pvc-ns-d", &claim, &Default::default()).await.is_err());
        let minting = said("Provisioning");
        assert_eq!(minting.len(), 1, "{:?}", events.lock().unwrap());
        assert_eq!(minting[0]["type"], "Normal");
        assert_eq!(minting[0]["involvedObject"]["kind"], "PersistentVolumeClaim");
        assert_eq!(minting[0]["involvedObject"]["name"], "d");
        assert!(minting[0]["message"].as_str().unwrap().contains("minting the 64M blank pvc-ext4j-64m"));

        // The engine refused it: the next ask says why, on the claim.
        let t = Instant::now();
        while said("ProvisioningFailed").is_empty() {
            assert!(t.elapsed() < std::time::Duration::from_secs(5), "{:?}", events.lock().unwrap());
            let _ = mgr.clone_blank("64M", "pvc-ns-d", &claim, &Default::default()).await;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let failed = said("ProvisioningFailed");
        assert_eq!(failed[0]["type"], "Warning");
        let m = failed[0]["message"].as_str().unwrap();
        assert!(m.contains("would not mint the blank pvc-ext4j-64m") && m.contains("no data slab has 64 MiB free"), "{m}");

        // A ready blank: cloned, and the claim says so.
        ready.store(true, Ordering::SeqCst);
        assert_eq!(mgr.clone_blank("64M", "pvc-ns-d", &claim, &Default::default()).await.unwrap(), "vol-d");
        let ok = said("ProvisioningSucceeded");
        assert_eq!(ok.len(), 1);
        assert!(ok[0]["message"].as_str().unwrap().contains("volume pvc-ns-d cloned from the 64M blank pvc-ext4j-64m"));
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
            .mint_template("pvc-ext4j-1048576m", "1T", &Default::default())
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
            .mint_template("pvc-ext4j-1048576m", "1T", &Default::default())
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
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
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
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
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

    /// A stormblock that lists `pvc-ext4j-64m` as `state` (with `broken`'s
    /// reason) until it is deleted, then 404 until it is minted, then `ready`
    /// as `tpl-2`; only `tpl-2` clones (#383). Counts DELETEs and mints.
    async fn listing_stormblock(
        state: &'static str,
        broken: Option<&'static str>,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>, Arc<std::sync::atomic::AtomicUsize>) {
        use axum::routing::{get as http_get, post as http_post};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let deletes = Arc::new(AtomicUsize::new(0));
        let mints = Arc::new(AtomicUsize::new(0));
        let (d1, d2, m1, m2) = (deletes.clone(), deletes.clone(), mints.clone(), mints.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/fstemplates/{name}",
                http_get(move || {
                    let (d, m) = (d1.clone(), m1.clone());
                    async move {
                        if m.load(Ordering::SeqCst) > 0 {
                            Ok(axum::Json(json!({"id": "tpl-2", "state": "ready"})))
                        } else if d.load(Ordering::SeqCst) > 0 {
                            Err(axum::http::StatusCode::NOT_FOUND)
                        } else {
                            Ok(axum::Json(json!({"id": "tpl-1", "state": state, "broken": broken})))
                        }
                    }
                })
                .delete(move || {
                    let d = d2.clone();
                    async move {
                        d.fetch_add(1, Ordering::SeqCst);
                        axum::Json(json!({"deleted": "tpl-1"}))
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates/{id}/clone",
                http_post(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                    if id == "tpl-2" {
                        (axum::http::StatusCode::CREATED, axum::Json(json!({"volume_id": "vol-1"})))
                    } else {
                        (axum::http::StatusCode::CONFLICT, axum::Json(json!({"error": "not this one", "code": 409})))
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates",
                http_post(move || {
                    let n = m2.clone();
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
    async fn a_template_listed_broken_is_deleted_minted_again_and_cloned() {
        // #383: server8's kept pvc-ext4j-64m, listed `broken`, was waited on
        // for 25 minutes as if it were still being formatted.
        use std::sync::atomic::Ordering;
        let (url, deletes, mints) = listing_stormblock(
            "broken",
            Some("its sealed volume 5c1e0b7a is gone"),
        )
        .await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let mut changes = mgr.subscribe_volume_changes();
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
        assert!(
            e.contains("template pvc-ext4j-64m is broken (its sealed volume 5c1e0b7a is gone); deleted; minted again"),
            "{e}"
        );
        assert_eq!(deletes.load(Ordering::SeqCst), 1);
        // The mint's completion wakes the claim, and the next pass clones.
        tokio::time::timeout(std::time::Duration::from_secs(2), changes.changed())
            .await
            .expect("the mint finishes")
            .unwrap();
        assert_eq!(mints.load(Ordering::SeqCst), 1);
        let vol = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap();
        assert_eq!(vol, "vol-1");
        assert_eq!(deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_template_gone_before_its_clone_is_minted_once_and_cloned() {
        // #383 after stormblock#663: listed ready, deleted by the engine before
        // the clone. The clone's 404 mints it (once), the mint wakes the claim,
        // and the next pass clones the new one.
        use axum::routing::{get as http_get, post as http_post};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mints = Arc::new(AtomicUsize::new(0));
        let (m1, m2) = (mints.clone(), mints.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/fstemplates/{name}",
                http_get(move || {
                    let m = m1.clone();
                    async move {
                        let id = if m.load(Ordering::SeqCst) > 0 { "tpl-2" } else { "tpl-1" };
                        axum::Json(json!({"id": id, "state": "ready"}))
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates/{id}/clone",
                http_post(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                    if id == "tpl-2" {
                        (axum::http::StatusCode::CREATED, axum::Json(json!({"volume_id": "vol-1"})))
                    } else {
                        (axum::http::StatusCode::NOT_FOUND, axum::Json(json!({"error": format!("fstemplate {id} not found"), "code": 404})))
                    }
                }),
            )
            .route(
                "/api/v1/fstemplates",
                http_post(move || {
                    let n = m2.clone();
                    async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        (axum::http::StatusCode::CREATED, axum::Json(json!({})))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let mut changes = mgr.subscribe_volume_changes();
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
        assert!(e.contains("template pvc-ext4j-64m was gone when it was cloned; minted again"), "{e}");
        tokio::time::timeout(std::time::Duration::from_secs(2), changes.changed())
            .await
            .expect("the mint finishes")
            .unwrap();
        assert_eq!(mints.load(Ordering::SeqCst), 1);
        assert_eq!(mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap(), "vol-1");
        assert_eq!(mints.load(Ordering::SeqCst), 1, "minted once");
    }

    #[tokio::test]
    async fn a_state_the_kubelet_does_not_know_is_a_failure_not_a_wait() {
        use std::sync::atomic::Ordering;
        let (url, deletes, mints) = listing_stormblock("quarantined", None).await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
        assert!(e.contains("template pvc-ext4j-64m is in state \"quarantined\", which this kubelet does not know"), "{e}");
        assert!(!e.contains("formatted and sealed"), "{e}");
        assert_eq!(deletes.load(Ordering::SeqCst), 0);
        assert_eq!(mints.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_template_still_formatting_is_waited_on_and_kept() {
        use std::sync::atomic::Ordering;
        let (url, deletes, mints) = listing_stormblock("formatting", None).await;
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let e = mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &Default::default()).await.unwrap_err().to_string();
        assert_eq!(e, "waiting for volume pvc-ns-d: template pvc-ext4j-64m formatting");
        assert_eq!(deletes.load(Ordering::SeqCst), 0);
        assert_eq!(mints.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn template_states_sort_into_what_a_claim_does() {
        let of = |t: Value| TemplateStatus::of(&t).unwrap();
        let id = || "t".to_string();
        assert_eq!(of(json!({"id": "t"})), TemplateStatus::Ready(id()));
        assert_eq!(of(json!({"id": "t", "state": "ready"})), TemplateStatus::Ready(id()));
        for s in ["awaiting_format", "formatting", "sealing", "awaiting_seed"] {
            assert_eq!(of(json!({"id": "t", "state": s})), TemplateStatus::Making(id(), s.into()));
        }
        assert_eq!(
            of(json!({"id": "t", "state": "broken", "broken": "gone"})),
            TemplateStatus::Broken(id(), "gone".into())
        );
        assert_eq!(
            of(json!({"id": "t", "state": "broken"})),
            TemplateStatus::Broken(id(), "the engine gave no reason".into())
        );
        // An engine that set the reason but kept the old state.
        assert_eq!(
            of(json!({"id": "t", "state": "ready", "broken": "gone"})),
            TemplateStatus::Broken(id(), "gone".into())
        );
        assert_eq!(of(json!({"id": "t", "state": "odd"})), TemplateStatus::Unknown(id(), "odd".into()));
        assert_eq!(TemplateStatus::of(&json!({"state": "ready"})), None);
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
        let _ = mgr.mint_template("pvc-ext4j-1m", "1M", &Default::default()).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mgr.storage_template_status("pvc-ext4j-1m").await,
            Some(TemplateStatus::Ready("tpl-1".into()))
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
        claim_world_with(pvc, None, vec![]).await
    }

    /// [`claim_world`] whose engine also lists `slabs` (#62) and starts with
    /// `volumes`.
    async fn claim_world_with(pvc: Value, slabs: Option<Value>, volumes: Vec<Value>) -> ClaimWorld {
        let calls: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
        let volumes: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(volumes));
        let log = calls.clone();
        let app = axum::Router::new().fallback(
            move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let (log, volumes, pvc, slabs) = (log.clone(), volumes.clone(), pvc.clone(), slabs.clone());
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
                        ("GET", "/api/v1/slabs") if slabs.is_some() => ok(json!({ "items": slabs })),
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

    /// A [`ClaimWorld`] with a StorageClass (#71): `class` served at its path
    /// (else 404), no blanks yet, and every mint answered `mint`.
    async fn policy_world(pvc: Value, class: Option<Value>, mint: (u16, Value)) -> ClaimWorld {
        let calls: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
        let log = calls.clone();
        let app = axum::Router::new().fallback(
            move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let (log, pvc, class, mint) = (log.clone(), pvc.clone(), class.clone(), mint.clone());
                async move {
                    use axum::http::StatusCode;
                    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let path = uri.path().to_string();
                    log.lock().unwrap().push((method.to_string(), path.clone(), body.clone()));
                    let ok = |v: Value| (StatusCode::OK, axum::Json(v));
                    match (method.as_str(), path.as_str()) {
                        ("GET", p) if p.contains("/persistentvolumeclaims/") => ok(pvc),
                        ("GET", p) if p.starts_with("/apis/storage.k8s.io/v1/storageclasses/") => match class {
                            Some(c) => ok(c),
                            None => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                        },
                        ("GET", p) if p.starts_with("/api/v1/fstemplates/") => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                        ("POST", "/api/v1/fstemplates") => (StatusCode::from_u16(mint.0).unwrap(), axum::Json(mint.1)),
                        ("GET", "/api/v1/volumes") => ok(json!({ "items": [] })),
                        ("POST", "/api/v1/volumes") => (StatusCode::CREATED, axum::Json(json!({ "id": "vol-raw" }))),
                        ("POST", p) if p.ends_with("/attach") => ok(json!({ "device_hint": "/dev/ublkb7" })),
                        _ => ok(json!({})),
                    }
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        ClaimWorld { url, calls }
    }

    fn mirror_class() -> Value {
        json!({ "metadata": { "name": "stormblock" }, "provisioner": "stormblock.storm.io",
                "parameters": { "redundancy": "mirror", "spread": "shelf", "tier": "cold" } })
    }

    /// Wait for a request matching `pred` to reach the fake (a mint runs in
    /// the background).
    async fn seen(w: &ClaimWorld, pred: impl Fn(&(String, String, Value)) -> bool) -> Option<(String, String, Value)> {
        for _ in 0..300 {
            if let Some(c) = w.calls.lock().unwrap().iter().find(|c| pred(c)).cloned() {
                return Some(c);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        None
    }

    /// #233: the networks annotation resolves to its NADs (the pod's namespace
    /// unless named), ifnames by position unless given; a missing NAD names
    /// itself; default-network replaces eth0's.
    #[tokio::test]
    async fn a_pods_networks_resolve_to_their_nads() {
        let app = axum::Router::new().fallback(|uri: axum::http::Uri| async move {
            use axum::http::StatusCode;
            let p = uri.path().to_string();
            let nad = |cfg: &str| (StatusCode::OK, axum::Json(json!({"spec": {"config": cfg}})));
            match p.as_str() {
                "/apis/k8s.cni.cncf.io/v1/namespaces/default/network-attachment-definitions/lan" => nad(r#"{"cniVersion":"1.0.0","name":"lan","type":"bridge"}"#),
                "/apis/k8s.cni.cncf.io/v1/namespaces/infra/network-attachment-definitions/storage" => nad(r#"{"cniVersion":"1.0.0","name":"storage","type":"macvlan"}"#),
                "/apis/k8s.cni.cncf.io/v1/namespaces/kube-system/network-attachment-definitions/vlan248" => nad(r#"{"cniVersion":"1.0.0","name":"vlan248","type":"macvlan"}"#),
                _ => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new());

        let pod = |ann: Value| json!({"metadata": {"namespace": "default", "name": "p", "annotations": ann}});
        let (nets, def) = mgr
            .resolve_networks(&pod(json!({
                "k8s.v1.cni.cncf.io/networks": "lan, infra/storage@stor0",
                "v1.multus-cni.io/default-network": "kube-system/vlan248"})))
            .await
            .unwrap();
        assert_eq!(nets.iter().map(|n| (n.name.as_str(), n.ifname.as_str())).collect::<Vec<_>>(),
                   [("default/lan", "net1"), ("infra/storage", "stor0")]);
        assert!(nets[1].config.contains("macvlan"));
        let def = def.unwrap();
        assert_eq!((def.name.as_str(), def.ifname.as_str()), ("kube-system/vlan248", "eth0"));

        let e = mgr.resolve_networks(&pod(json!({"k8s.v1.cni.cncf.io/networks": "lan,ghost"}))).await.unwrap_err();
        assert_eq!(e, "NetworkAttachmentDefinition default/ghost not found");
        let (none, d) = mgr.resolve_networks(&pod(json!({}))).await.unwrap();
        assert!(none.is_empty() && d.is_none(), "no annotation: no extra networks");
    }

    /// #71: a filesystem claim of a mirrored class mints the blank for its
    /// size and policy, with the policy; the blank takes no tier (#400: the
    /// clone does).
    #[tokio::test]
    async fn a_class_policy_mints_its_own_blank() {
        let w = policy_world(raw_claim("16Mi", "Filesystem"), Some(mirror_class()), (201, json!({"template": {}}))).await;
        let mgr = claim_manager(&w);
        assert!(mgr.provision_claim("default", "raw", "uid-1").await.is_err(), "waits on the mint");
        let mint = seen(&w, |(m, p, _)| m == "POST" && p == "/api/v1/fstemplates").await.expect("minted");
        assert_eq!(mint.2["name"], "pvc-ext4j-16m-mirror2-shelf");
        assert_eq!(mint.2["redundancy"], "mirror:2");
        assert_eq!(mint.2["spread"], "shelf");
        assert!(mint.2.get("tier").is_none(), "fstemplates take no tier (stormblock#377)");
        let looked = w.calls.lock().unwrap().iter().any(|(m, p, _)| m == "GET" && p == "/api/v1/fstemplates/pvc-ext4j-16m-mirror2-shelf");
        assert!(looked, "the policy's blank is the one looked for");
        assert!(w.calls.lock().unwrap().iter().all(|(_, _, b)| b["reason"] != "TierNotApplied"), "#400: no TierNotApplied");
    }

    /// #400: a filesystem claim's class tier is sent on the blank's clone
    /// (stormblock#377); a class with no tier sends none.
    #[tokio::test]
    async fn a_filesystem_claims_tier_rides_on_the_clone() {
        let calls: Arc<Mutex<Vec<(String, String, Value)>>> = Arc::default();
        let log = calls.clone();
        let app = axum::Router::new().fallback(
            move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let log = log.clone();
                async move {
                    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let path = uri.path().to_string();
                    log.lock().unwrap().push((method.to_string(), path.clone(), body));
                    match (method.as_str(), path.as_str()) {
                        ("GET", "/api/v1/fstemplates/pvc-ext4j-64m") => axum::Json(json!({"id": "t1", "state": "ready"})),
                        ("POST", "/api/v1/fstemplates/t1/clone") => axum::Json(json!({"volume_id": "vol-c"})),
                        _ => axum::Json(json!({})),
                    }
                }
            },
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let clone_body = || {
            calls.lock().unwrap().iter().rev()
                .find(|(m, p, _)| m == "POST" && p == "/api/v1/fstemplates/t1/clone")
                .map(|c| c.2.clone()).expect("cloned")
        };

        let fast = crate::storage::ClaimPolicy { tier: Some("fast".into()), ..Default::default() };
        assert_eq!(mgr.clone_blank("64M", "pvc-ns-d", &Value::Null, &fast).await.unwrap(), "vol-c");
        let b = clone_body();
        assert_eq!((b["name"].clone(), b["verify"].clone(), b["tier"].clone()), (json!("pvc-ns-d"), json!(true), json!("fast")));

        assert_eq!(mgr.clone_blank("64M", "pvc-ns-e", &Value::Null, &Default::default()).await.unwrap(), "vol-c");
        let b = clone_body();
        assert_eq!(b["name"], "pvc-ns-e");
        assert!(b.get("tier").is_none(), "no tier in the class, none on the clone: {b}");
        assert!(calls.lock().unwrap().iter().all(|(_, _, b)| b["reason"] != "TierNotApplied"));
    }

    /// #71: a raw Block claim carries the policy and the tier itself.
    #[tokio::test]
    async fn a_block_claim_carries_its_class_policy_and_tier() {
        let w = policy_world(raw_claim("16Mi", "Block"), Some(mirror_class()), (500, json!({}))).await;
        let mgr = claim_manager(&w);
        mgr.provision_claim("default", "raw", "uid-1").await.unwrap();
        let made = seen(&w, |(m, p, _)| m == "POST" && p == "/api/v1/volumes").await.unwrap();
        assert_eq!((made.2["redundancy"].clone(), made.2["spread"].clone(), made.2["tier"].clone()),
                   (json!("mirror:2"), json!("shelf"), json!("cold")));
    }

    /// #71: a policy the node cannot place is refused by the engine with a 409
    /// that is not "exists"; it is the claim's ProvisioningFailed, not a mint
    /// taken as done. A class naming nothing keeps the shipped blank.
    #[tokio::test]
    async fn an_unplaceable_policy_is_said_on_the_claim() {
        let refused = (409, json!({"error": "mirror:2 needs 2 distinct shelf domains; this node has 1", "code": "conflict"}));
        let w = policy_world(raw_claim("16Mi", "Filesystem"), Some(mirror_class()), refused).await;
        let mgr = claim_manager(&w);
        assert!(mgr.provision_claim("default", "raw", "uid-1").await.is_err());
        seen(&w, |(m, p, _)| m == "POST" && p == "/api/v1/fstemplates").await.expect("minted");
        // The next look finds the mint failed, and says so on the claim.
        let mut said = None;
        for _ in 0..50 {
            let _ = mgr.provision_claim("default", "raw", "uid-1").await;
            said = w.calls.lock().unwrap().iter().find(|(m, p, b)| {
                m == "POST" && p.ends_with("/events") && b["reason"] == "ProvisioningFailed"
                    && b["message"].as_str().unwrap_or("").contains("distinct shelf domains")
            }).cloned();
            if said.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(said.is_some(), "{:?}", w.calls.lock().unwrap());

        let w = policy_world(raw_claim("16Mi", "Filesystem"), None, (201, json!({}))).await;
        let mgr = claim_manager(&w);
        let _ = mgr.provision_claim("default", "raw", "uid-1").await;
        let mint = seen(&w, |(m, p, _)| m == "POST" && p == "/api/v1/fstemplates").await.unwrap();
        assert_eq!(mint.2["name"], "pvc-ext4j-16m");
        assert!(mint.2.get("redundancy").is_none());

        // A parameter this driver does not take: refused on the claim, nothing made.
        let bad = json!({ "metadata": { "name": "stormblock" }, "parameters": { "replicas": "3" } });
        let w = policy_world(raw_claim("16Mi", "Filesystem"), Some(bad), (201, json!({}))).await;
        let mgr = claim_manager(&w);
        match mgr.provision_claim("default", "raw", "uid-1").await {
            Err(ClaimError::Failed(why)) => assert!(why.contains("replicas"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(!w.calls.lock().unwrap().iter().any(|(m, p, _)| m == "POST" && p.starts_with("/api/v1/fstemplates")));
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

    /// #372: the summary's volumes name the ids the annotations do, their
    /// claims and the engine's sizes.
    #[test]
    fn a_pods_volume_stats_name_their_volume_ids() {
        let spec = json!([{"name": "data", "persistentVolumeClaim": {"claimName": "db"}}, {"name": "x", "csi": {}},
            {"name": "scratch", "ephemeral": {"volumeClaimTemplate": {}}}]);
        let ids = std::collections::BTreeMap::from([("data".to_string(), "v-1".to_string()), ("x".to_string(), "h-2".to_string()),
            ("scratch".to_string(), "v-3".to_string())]);
        let listing = json!({"items": [{"id": "v-1", "virtual_size_bytes": 1024, "allocated_bytes": 10}]});
        let s = volume_stats("ns", "web", &spec, &ids, &listing);
        assert_eq!(s[1]["pvcRef"], json!({"name": "web-scratch", "namespace": "ns"}), "an ephemeral volume's claim");
        assert_eq!(s[0], json!({"name": "data", "volumeId": "v-1", "pvcRef": {"name": "db", "namespace": "ns"},
            "capacityBytes": 1024, "usedBytes": 10}));
        assert_eq!(s[2], json!({"name": "x", "volumeId": "h-2"}), "another driver's: its handle, no engine sizes");
    }

    /// #372: a claim's volume is said to be the claim's in the engine, as
    /// `{"owner": …}`, and once it is, a later start writes nothing.
    #[tokio::test]
    async fn a_claims_volume_is_owned_by_its_claim() {
        let w = claim_world(raw_claim("16Mi", "Block")).await;
        let mgr = claim_manager(&w);
        let mut steps = crate::start_timing::ClaimSteps::default();
        let (vol, _, _) = mgr.provision_claim_timed("default", "raw", "uid-1", &mut steps).await.unwrap();
        assert_eq!(vol, "vol-raw");
        let put = seen(&w, |(m, p, _)| m == "PUT" && p == "/api/v1/volumes/vol-raw/owner").await.expect("the owner was written");
        assert_eq!(put.2, json!({"owner": {"kind": "PersistentVolumeClaim", "namespace": "default", "name": "raw", "uid": "c-1"}}));

        let owned = json!({"id": "vol-raw", "name": crate::storage::volume_name("default", "raw"),
            "owner": {"kind": "PersistentVolumeClaim", "namespace": "default", "name": "raw", "uid": "c-1"}});
        let w = claim_world_with(raw_claim("16Mi", "Block"), None, vec![owned]).await;
        let mgr = claim_manager(&w);
        mgr.provision_claim_timed("default", "raw", "uid-1", &mut Default::default()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!w.calls.lock().unwrap().iter().any(|(m, p, _)| m == "PUT" && p.ends_with("/owner")), "already its claim's");
    }

    /// #95: a claim's steps, as the start timing reports them. Each one here is
    /// at least one request to the fake apiserver or stormblock.
    #[tokio::test]
    async fn a_claims_steps_are_timed_lookup_make_attach_bind() {
        let w = claim_world(raw_claim("16Mi", "Block")).await;
        let mgr = claim_manager(&w);
        let mut steps = crate::start_timing::ClaimSteps::default();
        mgr.provision_claim_timed("default", "raw", "uid-1", &mut steps).await.unwrap();
        let (how, made) = steps.made.expect("a new volume was made");
        assert_eq!(how, "raw");
        for (step, d) in [("lookup", steps.lookup), ("raw", made), ("attach", steps.attach)] {
            assert!(d > std::time::Duration::ZERO, "{step} was not timed: {steps:?}");
        }
        // The room check is a part of the lookup, timed on its own for a new volume.
        let room = steps.room.expect("a new volume's room was checked");
        assert!(room > std::time::Duration::ZERO && room <= steps.lookup, "{steps:?}");
        // The engine's volume list is read once for the lookup and once for
        // the room, not once per question (#95).
        let listings = w.calls.lock().unwrap().iter().filter(|(m, p, _)| m == "GET" && p == "/api/v1/volumes").count();
        assert_eq!(listings, 2, "{:?}", w.calls.lock().unwrap());
        // The bind runs after the start path returns, not in it, and still
        // happens: the claim's PV reaches the apiserver.
        assert_eq!(steps.bind, None, "the pod path does not wait for the bind");
        let posted = || w.calls.lock().unwrap().iter().any(|(m, p, _)| m == "POST" && p == "/api/v1/persistentvolumes");
        for _ in 0..200 {
            if posted() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(posted(), "the bind ran in the background");
    }

    /// #149: a filesystem claim past 16 TiB rounds up to an ext4 class blank
    /// like any other (stormblock carries mkfs.ext4.rs#9/#10), minted with
    /// its size in MiB, not refused as Block-only.
    #[tokio::test]
    async fn a_filesystem_claim_of_a_pebibyte_class_mints_its_ext4_blank() {
        for (want, mib) in [("20Ti", 64u64 << 20), ("300Ti", 1u64 << 30)] {
            let w = policy_world(raw_claim(want, "Filesystem"), None, (201, json!({"template": {}}))).await;
            let mgr = claim_manager(&w);
            match mgr.provision_claim("default", "raw", "uid-1").await {
                Err(ClaimError::Failed(why)) => assert!(!why.contains("no filesystem"), "{want}: {why}"),
                Err(_) => {}
                Ok(_) => panic!("{want}: the blank is still being minted"),
            }
            let mint = seen(&w, |(m, p, _)| m == "POST" && p == "/api/v1/fstemplates").await.expect("minted");
            assert_eq!(mint.2["name"], format!("pvc-ext4j-{mib}m"), "{want}");
            assert_eq!(mint.2["size"], format!("{mib}M"), "{want}");
            assert_eq!(mint.2["fs"], "ext4", "{want}");
        }
    }

    #[tokio::test]
    async fn a_claim_whose_class_does_not_fit_waits_and_nothing_is_made() {
        // #62: a 1.7 TB data slab with one 1 TiB claim on it has no room for a
        // second 600Gi claim (its class is 1 TiB), at ratio 1.0.
        let gi = 1u64 << 30;
        let slabs = json!([{ "role": "data", "total_bytes": 1700 * gi, "free_bytes": 1690 * gi }]);
        let first = json!({ "id": "v-1", "name": "pvc-default-first", "role": "data",
                            "virtual_size_bytes": 1024 * gi, "sealed": false });
        let w = claim_world_with(raw_claim("600Gi", "Block"), Some(slabs.clone()), vec![first]).await;
        let mgr = claim_manager(&w);
        match mgr.provision_claim("default", "raw", "uid-1").await {
            Err(ClaimError::Failed(why)) => {
                assert!(why.contains("not enough room") && why.contains("1T class"), "{why}")
            }
            other => panic!("{other:?}"),
        }
        let calls = w.calls.lock().unwrap().clone();
        assert!(!calls.iter().any(|(m, _, _)| m == "POST"), "nothing made: {calls:?}");

        // A 256Gi claim fits.
        let w = claim_world_with(raw_claim("200Gi", "Block"), Some(slabs), vec![]).await;
        let (dev, _) = claim_manager(&w).provision_claim("default", "raw", "uid-1").await.unwrap();
        assert_eq!(dev, "/dev/ublkb7");
    }

    #[test]
    fn a_token_is_asked_for_bound_to_its_pod_with_a_lifetime() {
        let b = token_request(None, Some(("web-1", "u-1")), Some(3607));
        assert_eq!(b["spec"]["expirationSeconds"], 3607);
        assert_eq!(b["spec"]["boundObjectRef"], json!({ "kind": "Pod", "apiVersion": "v1", "name": "web-1", "uid": "u-1" }));
        assert_eq!(b["spec"]["audiences"], json!([]));
        let plain = token_request(Some("vault"), None, None);
        assert_eq!(plain["spec"], json!({ "audiences": ["vault"] }));
    }

    #[test]
    fn a_token_is_refreshed_at_four_fifths_of_its_life() {
        let now = chrono::Utc::now();
        // The apiserver granted a year (warnafter): 80% of that.
        let year = now + chrono::Duration::days(365);
        assert_eq!((refresh_at(now, Some(year), 3607) - now).num_days(), 292);
        // No expiry said: 80% of what was asked.
        assert_eq!((refresh_at(now, None, 3600) - now).num_seconds(), 2880);
    }

    #[tokio::test]
    async fn a_pods_token_is_bound_written_and_refreshed_when_due() {
        let asked: Arc<Mutex<Vec<Value>>> = Arc::default();
        let log = asked.clone();
        let app = axum::Router::new().route(
            "/api/v1/namespaces/default/serviceaccounts/default/token",
            axum::routing::post(move |axum::Json(b): axum::Json<Value>| {
                let log = log.clone();
                async move {
                    let n = { let mut l = log.lock().unwrap(); l.push(b); l.len() };
                    axum::Json(json!({ "status": { "token": format!("tok-{n}"),
                        "expirationTimestamp": (chrono::Utc::now() + chrono::Duration::seconds(3600)).to_rfc3339() } }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new());
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        let p = json!({ "metadata": { "name": "web-1", "namespace": "default", "uid": "u-1" }, "spec": {} });

        assert!(mgr.write_bound_token(&p, d, "token", None, 3607).await.unwrap());
        assert_eq!(std::fs::read_to_string(dir.path().join("token")).unwrap(), "tok-1");
        assert_eq!(asked.lock().unwrap()[0]["spec"]["boundObjectRef"]["uid"], "u-1");
        assert_eq!(asked.lock().unwrap()[0]["spec"]["expirationSeconds"], 3607);

        // Not due: nothing asked. A pod the kubelet does not have: forgotten.
        mgr.pods.write().await.insert("u-1".into(), PodState {
            namespace: "default".into(), name: "web-1".into(), uid: "u-1".into(),
            sandbox_id: None, container_ids: HashMap::new(), phase: "Running".into(), pod: p.clone(),
            pod_ip: None, restart_counts: HashMap::new(), ready: HashMap::new(),
            liveness_failures: HashMap::new(), readiness_run: HashMap::new(), startup_passed: HashMap::new(), started: HashMap::new(),
            terminated: HashMap::new(), init_statuses: Vec::new(), sandbox_stopped: false, unstarted: Default::default(),
        });
        let next = mgr.refresh_tokens().await.unwrap();
        assert!(next > std::time::Duration::from_secs(2800), "{next:?}");
        assert_eq!(asked.lock().unwrap().len(), 1);
        // Due: written again.
        for f in mgr.tokens.lock().unwrap().values_mut().flatten() {
            f.refresh_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        }
        mgr.refresh_tokens().await;
        assert_eq!(std::fs::read_to_string(dir.path().join("token")).unwrap(), "tok-2");
        mgr.pods.write().await.remove("u-1");
        assert!(mgr.refresh_tokens().await.is_none(), "a gone pod's tokens are forgotten");
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
        vols.insert("raw".to_string(), ResolvedVolume { path: "/dev/ublkb7".into(), fstype: None, block: true, api: false });
        vols.insert("fs".to_string(), ResolvedVolume {
            path: "/dev/ublkb8".into(), fstype: Some("ext4".into()), block: false,
            api: false,
        });
        let spec = json!({
            "volumeMounts": [{"name": "fs", "mountPath": "/data"}],
            "volumeDevices": [{"name": "raw", "devicePath": "/dev/xvda"}, {"name": "fs", "devicePath": "/dev/no"}],
        });
        let m = resolve_mounts(&spec, &vols, &[]).unwrap();
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

    /// #111: a pod with two native sidecars around an ordinary init.
    fn sidecar_pod(uid: &str, restart_policy: &str) -> Value {
        let mut p = pod(uid, uid, restart_policy, simple_container());
        p["spec"]["initContainers"] = json!([
            {"name": "proxy", "image": "busybox:latest", "restartPolicy": "Always"},
            {"name": "setup", "image": "busybox:latest"},
            {"name": "logs", "image": "busybox:latest", "restartPolicy": "Always"}
        ]);
        p
    }

    fn serial(cid: &str) -> u32 {
        cid.rsplit('-').next().unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn a_sidecar_starts_before_the_next_init_and_runs_with_the_app() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("setup", 0);
        let p = sidecar_pod("uid-sc", "Always");
        let result = mgr.start_pod(&p).await.unwrap();
        assert_eq!(result.phase, "Running");

        let init: Vec<_> = result.init_container_statuses.iter()
            .map(|r| (r.name.as_str(), r.state.as_str(), r.restartable, r.started, r.initialized()))
            .collect();
        assert_eq!(init, [
            ("proxy", "running", true, true, true),
            ("setup", "terminated", false, false, true),
            ("logs", "running", true, true, true),
        ]);
        assert!(result.init_container_statuses.iter().filter(|r| r.restartable).all(|r| r.ready), "no readinessProbe: ready");

        let ids = mgr.pods.read().await["uid-sc"].container_ids.clone();
        assert!(ids.contains_key("proxy") && ids.contains_key("logs") && ids.contains_key("app"), "{ids:?}");
        assert!(!ids.contains_key("setup"), "an ordinary init is removed once done");
        // Order: proxy, then setup (once proxy started), then logs, then the app.
        let removed = rt.removed_containers.lock().unwrap().clone();
        // Completed, and kept for its log until the pod stops (#47).
        let kept = mgr.kept_inits.lock().unwrap()["uid-sc"].clone();
        let setup = kept.iter().find(|c| c.contains("-setup-")).expect("setup ran and was kept");
        assert!(!removed.contains(setup), "not removed while the pod runs");
        assert!(serial(&ids["proxy"]) < serial(setup) && serial(setup) < serial(&ids["logs"]));
        assert!(serial(&ids["logs"]) < serial(&ids["app"]));
        assert!(!removed.iter().any(|c| c == &ids["proxy"] || c == &ids["logs"]), "sidecars keep running");
    }

    #[tokio::test]
    async fn a_sidecar_that_exits_before_it_started_is_restarted_not_failed() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("proxy", 1);
        let p = sidecar_pod("uid-sc-crash", "Never");
        match mgr.start_pod(&p).await {
            Err(CriError::Pending(m)) => assert!(m.contains("sidecar proxy exited with code 1"), "{m}"),
            other => panic!("a sidecar crash is a restart, not a failure: {other:?}"),
        }
        let state = mgr.pods.read().await["uid-sc-crash"].clone();
        assert_eq!(state.restart_counts.get("proxy"), Some(&1));
        assert!(!state.container_ids.contains_key("proxy"), "made again on the next try");
        let r = &state.init_statuses[0];
        assert_eq!((r.state.as_str(), r.reason.as_str(), r.restart_count), ("waiting", "CrashLoopBackOff", 1));
        // Inside its back-off the next try waits rather than recreating it.
        assert!(matches!(mgr.start_pod(&p).await, Err(CriError::Pending(m)) if m.contains("back-off")));
    }

    #[tokio::test]
    async fn a_containers_run_ends_when_it_exits_is_gone_or_its_pod_is() {
        // #416: what ends `kubectl logs -f`.
        let (rt, mgr) = manager();
        let p = pod("uid-run", "run", "Always", simple_container());
        mgr.start_pod(&p).await.unwrap();
        assert!(!mgr.container_run_ended("default", "run", "app").await, "running");
        assert!(!mgr.container_run_ended("default", "run", "other").await, "not started");
        let cid = mgr.pods.read().await["uid-run"].container_ids["app"].clone();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        assert!(mgr.container_run_ended("default", "run", "app").await, "exited, before any restart");
        rt.containers.lock().unwrap().remove(&cid);
        assert!(mgr.container_run_ended("default", "run", "app").await, "gone from the runtime");
        assert!(mgr.container_run_ended("default", "gone", "app").await, "no such pod here");

        // restartPolicy Never: recorded as terminated for good, and Succeeded.
        let p = pod("uid-once", "once", "Never", simple_container());
        mgr.start_pod(&p).await.unwrap();
        let cid = mgr.pods.read().await["uid-once"].container_ids["app"].clone();
        rt.set_container_state(&cid, ContainerState::Exited, 0);
        assert_eq!(mgr.check_pod_status("uid-once").await.unwrap().phase, "Succeeded");
        assert!(mgr.container_run_ended("default", "once", "app").await, "Completed");
    }

    #[tokio::test]
    async fn a_sidecar_restarts_under_restart_policy_never_and_does_not_hold_the_phase() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("setup", 0);
        let p = sidecar_pod("uid-sc-never", "Never");
        mgr.start_pod(&p).await.unwrap();
        let before = mgr.pods.read().await["uid-sc-never"].container_ids["proxy"].clone();

        // The sidecar dies: restarted, whatever the pod's policy says.
        rt.set_container_state(&before, ContainerState::Exited, 2);
        let u = mgr.check_pod_status("uid-sc-never").await.unwrap();
        assert_eq!(u.phase, "Running");
        let after = mgr.pods.read().await["uid-sc-never"].container_ids["proxy"].clone();
        assert_ne!(before, after, "a new proxy container");
        assert_eq!(mgr.pods.read().await["uid-sc-never"].restart_counts.get("proxy"), Some(&1));
        assert!(u.container_statuses.iter().all(|cs| cs.name == "app"), "sidecars are not app containers");
        assert!(u.init_container_statuses.iter().any(|r| r.name == "proxy" && r.restart_count == 1));

        // The app finishes: the pod is Succeeded with its sidecars still
        // running, and they are stopped (last declared first).
        let app = mgr.pods.read().await["uid-sc-never"].container_ids["app"].clone();
        rt.set_container_state(&app, ContainerState::Exited, 0);
        let u = mgr.check_pod_status("uid-sc-never").await.unwrap();
        assert_eq!(u.phase, "Succeeded");
        for n in ["proxy", "logs"] {
            let r = u.init_container_statuses.iter().find(|r| r.name == n).unwrap();
            assert_eq!((r.state.as_str(), r.ready), ("terminated", false), "{n}");
        }
        let containers = rt.containers.lock().unwrap();
        let ids = mgr.pods.read().await["uid-sc-never"].container_ids.clone();
        assert!(matches!(containers[&ids["proxy"]].state, ContainerState::Exited));
        assert!(matches!(containers[&ids["logs"]].state, ContainerState::Exited));
    }

    #[tokio::test]
    async fn a_pod_is_torn_down_apps_first_then_sidecars_last_declared_first() {
        let (rt, mgr) = manager();
        rt.set_exit_on_start("setup", 0);
        let p = sidecar_pod("uid-sc-down", "Always");
        mgr.start_pod(&p).await.unwrap();
        let ids = mgr.pods.read().await["uid-sc-down"].container_ids.clone();
        let setup = mgr.kept_inits.lock().unwrap()["uid-sc-down"][0].clone();
        rt.removed_containers.lock().unwrap().clear();
        mgr.stop_pod("uid-sc-down").await.unwrap();
        let removed = rt.removed_containers.lock().unwrap().clone();
        // Then the completed init kept for its log (#47).
        assert_eq!(removed, vec![ids["app"].clone(), ids["logs"].clone(), ids["proxy"].clone(), setup]);
    }

    /// #23: a pod that asks for an AppArmor profile is refused, upstream's
    /// way (Failed, reason AppArmor), and nothing is started; Unconfined runs.
    #[tokio::test]
    async fn a_pod_asking_for_apparmor_is_refused_and_never_started() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-aa", "confined", "Always", simple_container());
        p["spec"]["containers"][0]["securityContext"] = json!({"appArmorProfile": {"type": "RuntimeDefault"}});
        let outcome = mgr.sync_pods(&[p.clone()]).await;
        let u = &outcome.updates[0];
        assert_eq!((u.phase.as_str(), u.reason.as_str()), ("Failed", APPARMOR_REASON));
        assert!(u.message.contains("container app asks for one"), "{}", u.message);
        assert!(rt.created_names().is_empty(), "nothing started");

        // Its own Failed write coming back: terminal, even under Always.
        p["status"] = json!({"phase": "Failed", "reason": "AppArmor"});
        assert!(mgr.sync_pods(&[p]).await.updates.is_empty());
        assert!(rt.created_names().is_empty());

        // Cilium's agent shape: Unconfined is admitted.
        let mut ok = pod("uid-un", "agent", "Always", simple_container());
        ok["spec"]["securityContext"] = json!({"appArmorProfile": {"type": "Unconfined"}});
        assert_eq!(mgr.sync_pods(&[ok]).await.updates[0].phase, "Running");
    }

    #[test]
    fn apparmor_profiles_are_read_from_the_field_and_the_old_annotation() {
        let base = json!({"metadata": {}, "spec": {"containers": [{"name": "a"}, {"name": "b"}]}});
        assert_eq!(apparmor_refusal(&base), None);
        let mut p = base.clone();
        p["spec"]["securityContext"] = json!({"appArmorProfile": {"type": "Localhost", "localhostProfile": "k8s-x"}});
        assert!(apparmor_refusal(&p).unwrap().contains("the pod asks"));
        let mut p = base.clone();
        p["metadata"]["annotations"] = json!({
            "container.apparmor.security.beta.kubernetes.io/a": "unconfined",
            "container.apparmor.security.beta.kubernetes.io/b": "runtime/default"
        });
        let why = apparmor_refusal(&p).unwrap();
        assert!(why.contains("container b") && !why.contains("container a"), "{why}");
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
        // App container is created. The completed init container is kept, so
        // its log still answers `kubectl logs -c setup` (#47): this runtime's
        // removal would take the log with it.
        assert_eq!(rt.created_names(), vec!["app".to_string(), "setup".to_string()]);
        assert!(rt.removed_containers.lock().unwrap().is_empty(), "kept while the pod runs");
        // The pod stops: the init container goes with it.
        mgr.stop_pod("uid-1").await.unwrap();
        let removed = rt.removed_containers.lock().unwrap().clone();
        assert_eq!(removed.len(), 2, "the app and the kept init container: {removed:?}");
        assert!(mgr.kept_inits.lock().unwrap().is_empty());
    }

    /// #126: an init container runs until it exits; past two minutes it is
    /// still waited for, not killed DeadlineExceeded.
    #[tokio::test]
    async fn an_init_container_still_running_after_two_minutes_is_waited_for() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-mig", "mig", "Never", simple_container());
        p["spec"]["initContainers"] = json!([{"name": "migrate", "image": "busybox:latest"}]);
        match mgr.start_pod(&p).await {
            Err(CriError::Pending(m)) => assert!(m.contains("init container migrate is running"), "{m}"),
            other => panic!("{other:?}"),
        }
        // Long past the old 120 s bound.
        if let Some(earlier) = Instant::now().checked_sub(std::time::Duration::from_secs(600)) {
            mgr.first_seen.lock().unwrap().insert("uid-mig".into(), earlier);
            mgr.pods.write().await.get_mut("uid-mig").unwrap().started.insert("migrate".into(), earlier);
        }
        assert!(matches!(mgr.start_pod(&p).await, Err(CriError::Pending(_))), "still waited for");
        let cid = mgr.pods.read().await["uid-mig"].container_ids["migrate"].clone();
        rt.set_container_state(&cid, ContainerState::Exited, 0);
        assert_eq!(mgr.start_pod(&p).await.unwrap().phase, "Running");
    }

    /// #126: the pod's activeDeadlineSeconds is the one deadline an init
    /// container meets.
    #[tokio::test]
    async fn active_deadline_seconds_ends_a_running_init_container() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-ads", "ads", "Never", simple_container());
        p["spec"]["initContainers"] = json!([{"name": "migrate", "image": "busybox:latest"}]);
        p["spec"]["activeDeadlineSeconds"] = json!(60);
        p["status"]["startTime"] = json!((chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339());
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.phase, "Failed");
        assert!(u.message.contains("DeadlineExceeded"), "{}", u.message);
        let r = u.init_container_statuses.iter().find(|r| r.name == "migrate").expect("reported");
        assert_eq!((r.reason.as_str(), r.exit_code), ("DeadlineExceeded", -1));
        assert_eq!(u.container_statuses[0].reason, "PodInitializing");
        assert!(rt.removed_containers.lock().unwrap().iter().any(|c| c.contains("-migrate-")), "stopped and removed");
    }

    #[test]
    fn the_active_deadline_counts_from_start_time_else_first_seen() {
        let now = chrono::Utc::now();
        let mut p = json!({"spec": {}, "status": {}});
        assert_eq!(active_deadline_left(&p, None, now), None, "no deadline");
        p["spec"]["activeDeadlineSeconds"] = json!(100);
        p["status"]["startTime"] = json!((now - chrono::Duration::seconds(30)).to_rfc3339());
        let left = active_deadline_left(&p, None, now).unwrap().as_secs();
        assert!((69..=70).contains(&left), "{left}");
        p["status"]["startTime"] = json!((now - chrono::Duration::seconds(300)).to_rfc3339());
        assert!(active_deadline_left(&p, None, now).unwrap().is_zero(), "passed");
        // No startTime: from when the kubelet first saw it.
        p["status"] = json!({});
        assert!(active_deadline_left(&p, Some(Instant::now()), now).unwrap().as_secs() >= 99);
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
        // #133: never terminal with no container state. The failed init is
        // reported, and the app waits on it.
        let u = &outcome.updates[0];
        assert!(u.init_container_statuses.iter().any(|r| r.name == "setup" && r.exit_code == 1), "{:?}", u.init_container_statuses);
        assert_eq!(u.container_statuses.len(), 1);
        assert_eq!((u.container_statuses[0].state.as_str(), u.container_statuses[0].reason.as_str()), ("waiting", "PodInitializing"));
        assert_eq!(mgr.waiting_state("default", "web").map(|(k, _)| k).as_deref(), Some("StartError"), "logs say why");
    }

    /// #133: an image that will not pull is a wait (ErrImagePull, then
    /// ImagePullBackOff), not a pod ended with no container state; the next
    /// try waits its back-off, and the partial start is torn down.
    #[tokio::test]
    async fn an_image_that_will_not_pull_waits_with_errimagepull_then_backoff() {
        let (rt, mgr) = manager();
        rt.missing_images.lock().unwrap().insert("nope:1".into());
        let p = pod("uid-img", "img", "Never", json!({"name": "app", "image": "nope:1"}));
        let u = mgr.sync_pods(&[p.clone()]).await.updates.remove(0);
        assert_eq!(u.phase, "Pending");
        let cs = &u.container_statuses[0];
        assert_eq!((cs.state.as_str(), cs.reason.as_str()), ("waiting", "ErrImagePull"));
        assert!(cs.message.contains("manifest unknown"), "{}", cs.message);
        assert_eq!(mgr.waiting_state("default", "img").unwrap().0, "ErrImagePull");

        // Woken early: held off, and the partial start is gone.
        let pulls = rt.image_pulls.load(Ordering::SeqCst);
        let again = mgr.sync_pods(&[p.clone()]).await;
        assert!(again.updates.is_empty(), "not tried before its back-off");
        assert_eq!(rt.image_pulls.load(Ordering::SeqCst), pulls);
        assert!(rt.sandboxes.lock().unwrap().is_empty(), "the partial start was torn down");

        // Its back-off up: tried again, now ImagePullBackOff.
        mgr.start_holdoff.lock().unwrap().clear();
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!((u.phase.as_str(), u.container_statuses[0].reason.as_str()), ("Pending", "ImagePullBackOff"));
        assert!(rt.image_pulls.load(Ordering::SeqCst) > pulls, "asked the registry again");
    }

    #[tokio::test]
    async fn a_container_that_will_not_be_created_waits_with_its_reason() {
        let (rt, mgr) = manager();
        rt.fail_create.lock().unwrap().insert("app".into());
        let p = pod("uid-cc", "cc", "Always", simple_container());
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.phase, "Pending");
        assert_eq!(u.container_statuses[0].reason, "CreateContainerError");
        assert!(u.container_statuses[0].message.contains("injected create failure"));
    }

    /// #287: a refused create is tried again in 0.5 s, then 1 s, not after
    /// the 10 s start back-off; once it starts, the count is gone.
    #[tokio::test]
    async fn a_refused_create_is_retried_within_a_second() {
        use std::time::Duration;
        let (rt, mgr) = manager();
        rt.fail_create.lock().unwrap().insert("app".into());
        let p = pod("uid-cr", "cr", "Always", simple_container());
        let holdoff = |mgr: &PodManager| {
            mgr.start_holdoff.lock().unwrap()["uid-cr"].saturating_duration_since(Instant::now())
        };
        let u = mgr.sync_pods(&[p.clone()]).await.updates.remove(0);
        assert_eq!(u.container_statuses[0].reason, "CreateContainerError");
        let first = holdoff(&mgr);
        assert!(first <= Duration::from_millis(500) && first > Duration::from_millis(300), "{first:?}");

        mgr.start_holdoff.lock().unwrap().clear();
        mgr.sync_pods(&[p.clone()]).await;
        let second = holdoff(&mgr);
        assert!(second <= Duration::from_secs(1) && second > Duration::from_millis(800), "{second:?}");

        // The runtime takes it now: started, and the count forgotten.
        rt.fail_create.lock().unwrap().clear();
        mgr.start_holdoff.lock().unwrap().clear();
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.phase, "Running");
        assert!(mgr.create_refusals.lock().unwrap().is_empty());
    }

    /// Under restartPolicy Never a start failure ends the pod, with the
    /// container terminated StartError, exit 128, as upstream reports it;
    /// under Always it waits and tries again.
    #[tokio::test]
    async fn a_start_failure_is_starterror_under_never_and_a_wait_otherwise() {
        let (rt, mgr) = manager();
        rt.fail_start.lock().unwrap().insert("app".into());
        let p = pod("uid-se", "se", "Never", simple_container());
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.phase, "Failed");
        let cs = &u.container_statuses[0];
        assert_eq!((cs.state.as_str(), cs.reason.as_str(), cs.exit_code), ("terminated", "StartError", 128));
        assert!(cs.message.contains("no such file"), "{}", cs.message);
        assert_eq!(mgr.waiting_state("default", "se").unwrap().0, "StartError");

        let p = pod("uid-se2", "se2", "Always", simple_container());
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!((u.phase.as_str(), u.container_statuses[0].reason.as_str()), ("Pending", "RunContainerError"));
        // #289: a refused start (a claim's mount the engine refused once) is
        // tried again in half a second, not after the 10 s start back-off.
        let next = mgr.start_holdoff.lock().unwrap()["uid-se2"].saturating_duration_since(Instant::now());
        assert!(next <= std::time::Duration::from_millis(500), "{next:?}");
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

    /// #262: a static pod (its manifest marked `config.source: file`) gets no
    /// ServiceAccount mount and asks no apiserver for a token: the control
    /// plane's static pods start before there is one to ask.
    #[tokio::test]
    async fn a_static_pod_asks_for_no_service_account_token() {
        let (_rt, mgr) = manager();
        let mut p = pod("static-etcd-n1", "etcd", "Always", simple_container());
        p["metadata"]["annotations"] = json!({CONFIG_SOURCE: "file"});
        assert!(is_static(&p));
        let started = std::time::Instant::now();
        assert!(mgr.service_account_mount(&p).await.unwrap().is_none());
        assert!(started.elapsed() < std::time::Duration::from_millis(50), "no request, no retries");
        assert!(!is_static(&pod("uid-4", "web", "Always", simple_container())));
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
        // Writable by any uid (#98), as upstream makes it.
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&v.get("scratch").unwrap().path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o777, "{mode:o}");
    }

    /// rustkube-node#428 (finding 551d194231b5): configMap, secret and
    /// projected volumes are the kubelet's to write. They mount read-only
    /// even when `volumeMounts.readOnly` is false or absent, so a container
    /// cannot replace a key with a symlink; an emptyDir stays writable.
    #[tokio::test]
    async fn api_volumes_mount_read_only_whatever_the_mount_says() {
        let (_rt, mgr) = manager();
        let p = json!({
            "metadata": {"uid": "uid-428", "namespace": "default"},
            "spec": {"volumes": [
                {"name": "cm", "configMap": {"name": "c", "optional": true}},
                {"name": "sec", "secret": {"secretName": "s", "optional": true}},
                {"name": "proj", "projected": {"sources": []}},
                {"name": "scratch", "emptyDir": {}}
            ]}
        });
        let v = mgr.resolve_volumes(&p).await.unwrap();
        assert!(v["cm"].api && v["sec"].api && v["proj"].api && !v["scratch"].api);
        let spec = json!({"volumeMounts": [
            {"name": "cm", "mountPath": "/cm", "readOnly": false},
            {"name": "sec", "mountPath": "/sec"},
            {"name": "proj", "mountPath": "/proj", "readOnly": false},
            {"name": "scratch", "mountPath": "/scratch"}
        ]});
        let ro: Vec<(String, bool)> =
            resolve_mounts(&spec, &v, &[]).unwrap().into_iter().map(|m| (m.container_path, m.readonly)).collect();
        assert_eq!(
            ro,
            vec![("/cm".into(), true), ("/sec".into(), true), ("/proj".into(), true), ("/scratch".into(), false)]
        );
    }

    /// #98: runAsUser / runAsGroup / runAsNonRoot from the container, else the pod.
    #[test]
    fn run_as_comes_from_the_container_else_the_pod() {
        let mut c = simple_container();
        c["securityContext"] = json!({"runAsUser": 1000, "runAsNonRoot": false});
        let mut cfg = build_container_config(&c, "img", vec![], vec![]);
        let pod = json!({"spec": {"securityContext": {"runAsUser": 2000, "runAsGroup": 3000, "runAsNonRoot": true}}});
        apply_pod_namespaces(&mut cfg, &pod);
        assert_eq!((cfg.run_as_user, cfg.run_as_group, cfg.run_as_non_root), (Some(1000), Some(3000), Some(false)));
        let mut bare = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut bare, &pod);
        assert_eq!((bare.run_as_user, bare.run_as_non_root), (Some(2000), Some(true)));
        let mut none = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut none, &json!({"spec": {}}));
        assert_eq!((none.run_as_user, none.run_as_group, none.run_as_non_root), (None, None, None));
    }

    /// #348: a container asking for `rdma.storm.io/hca` is marked for the
    /// runtime; one that does not is not.
    #[test]
    fn a_container_asking_for_rdma_is_marked() {
        let mut c = simple_container();
        c["resources"] = json!({"limits": {"rdma.storm.io/hca": "1"}, "requests": {"rdma.storm.io/hca": "1"}});
        assert!(build_container_config(&c, "img", vec![], vec![]).rdma);
        assert!(!build_container_config(&simple_container(), "img", vec![], vec![]).rdma);
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
                api: false,
            },
        );
        let spec = json!({
            "name": "cilium-agent",
            "image": "cilium:latest",
            "securityContext": {
                "privileged": true,
                "readOnlyRootFilesystem": true,
                "capabilities": {"add": ["NET_ADMIN", "SYS_MODULE"], "drop": ["NET_RAW"]}
            },
            "volumeMounts": [
                {"name": "bpf", "mountPath": "/sys/fs/bpf", "mountPropagation": "Bidirectional"},
                {"name": "missing", "mountPath": "/nope"}
            ]
        });
        let mounts = resolve_mounts(&spec, &volumes, &[]).unwrap();
        let c = build_container_config(&spec, "cilium@sha", vec![], mounts);
        assert!(c.privileged);
        assert!(c.readonly_rootfs);
        assert_eq!(c.add_capabilities, vec!["NET_ADMIN", "SYS_MODULE"]);
        assert_eq!(c.drop_capabilities, vec!["NET_RAW"]);
        assert!(!c.no_new_privs, "allowPrivilegeEscalation unset is not no_new_privs");
        let no_escalation = build_container_config(
            &json!({"name": "x", "securityContext": {"allowPrivilegeEscalation": false}}),
            "x",
            vec![],
            vec![],
        );
        assert!(no_escalation.no_new_privs);
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
                api: false,
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
            resolve_mounts(&spec(true), &volumes, &[]).unwrap()[0].propagation,
            MountPropagation::Bidirectional
        );
        assert_eq!(
            resolve_mounts(&spec(false), &volumes, &[]).unwrap()[0].propagation,
            MountPropagation::Private
        );
    }

    #[test]
    fn qos_class_is_upstreams() {
        let pod = |cs: Value| json!({"spec": {"containers": cs}});
        assert_eq!(qos_class(&pod(json!([{"name": "a"}]))), "BestEffort");
        assert_eq!(qos_class(&pod(json!([{"name": "a", "resources": {"limits": {"cpu": "1", "memory": "1Gi"}}}]))), "Guaranteed", "a request left unset is its limit");
        assert_eq!(qos_class(&pod(json!([{"name": "a", "resources": {"requests": {"cpu": "1000m", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "1Gi"}}}]))), "Guaranteed");
        assert_eq!(qos_class(&pod(json!([{"name": "a", "resources": {"requests": {"cpu": "500m"}, "limits": {"cpu": "1", "memory": "1Gi"}}}]))), "Burstable");
        assert_eq!(qos_class(&pod(json!([{"name": "a", "resources": {"requests": {"memory": "64Mi"}}}]))), "Burstable");
        assert_eq!(qos_class(&pod(json!([{"name": "a", "resources": {"limits": {"cpu": "1", "memory": "1Gi"}}}, {"name": "b"}]))), "Burstable", "every container must be");
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

    /// #171: fsGroup, its policy and supplementalGroups come from the pod.
    #[test]
    fn fs_group_and_supplemental_groups_are_the_pods() {
        let mut c = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut c, &json!({"spec": {"securityContext": {
            "fsGroup": 2000, "fsGroupChangePolicy": "OnRootMismatch", "supplementalGroups": [3000, 4000]
        }}}));
        assert_eq!(c.fs_group, Some(2000));
        assert_eq!(c.fs_group_change_policy.as_deref(), Some("OnRootMismatch"));
        assert_eq!(c.supplemental_groups, [3000, 4000]);
        let mut plain = build_container_config(&simple_container(), "img", vec![], vec![]);
        apply_pod_namespaces(&mut plain, &json!({"spec": {}}));
        assert_eq!((plain.fs_group, plain.supplemental_groups.len()), (None, 0));
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

    /// #414: `stdinOnce` reaches the runtime, off by default.
    #[test]
    fn container_config_carries_stdin_once() {
        assert!(!build_container_config(&simple_container(), "img", vec![], vec![]).stdin_once);
        let spec = json!({"name": "app", "image": "busybox", "stdin": true, "stdinOnce": true});
        let c = build_container_config(&spec, "img", vec![], vec![]);
        assert!(c.stdin && c.stdin_once);
    }

    #[test]
    fn container_config_defaults_are_unprivileged() {
        let c = build_container_config(&simple_container(), "img", vec![], vec![]);
        assert!(!c.privileged);
        assert!(c.add_capabilities.is_empty());
        assert!(c.drop_capabilities.is_empty());
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
        let env = mgr.resolve_env(&p, &container, Some("10.1.2.3")).await.unwrap();
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

    /// #225: a deleted pod's containers get its terminationGracePeriodSeconds,
    /// or the delete's shorter deletionGracePeriodSeconds, never a fixed 30.
    #[tokio::test]
    async fn a_deleted_pod_is_stopped_within_its_own_grace() {
        let (rt, mgr) = manager();
        let mut p = pod("uid-1", "web", "Never", simple_container());
        p["spec"]["terminationGracePeriodSeconds"] = json!(5);
        mgr.sync_pods(&[p.clone()]).await;
        let mut deleting = p.clone();
        deleting["metadata"]["deletionTimestamp"] = json!("2026-10-09T00:00:00Z");
        mgr.sync_pods(&[deleting]).await;
        let graces: Vec<i64> = rt.stop_graces.lock().unwrap().iter().map(|(_, g)| *g).collect();
        assert_eq!(graces, [5]);

        assert_eq!(termination_grace(&json!({"spec": {}})), 30, "upstream's default");
        assert_eq!(termination_grace(&json!({"spec": {"terminationGracePeriodSeconds": 0}})), 0);
        assert_eq!(
            termination_grace(&json!({"metadata": {"deletionGracePeriodSeconds": 2}, "spec": {"terminationGracePeriodSeconds": 60}})),
            2,
            "the delete's own grace"
        );
        assert_eq!(termination_grace(&json!({"spec": {"terminationGracePeriodSeconds": -1}})), 0);

        // The reconciler's path, with the grace the deleting object gave.
        let (rt, mgr) = manager();
        mgr.sync_pods(&[pod("uid-2", "web", "Never", simple_container())]).await;
        mgr.stop_pod_within("uid-2", Some(1)).await.unwrap();
        assert_eq!(rt.stop_graces.lock().unwrap().iter().map(|(_, g)| *g).collect::<Vec<_>>(), [1]);
    }

    /// #374: a postStart hook runs once the container has started; one that
    /// fails kills the container (grace 0), as upstream.
    #[tokio::test]
    async fn a_post_start_hook_runs_and_its_failure_kills_the_container() {
        let mut c = simple_container();
        c["lifecycle"] = json!({"postStart": {"exec": {"command": ["warm", "up"]}}});
        let (rt, mgr) = manager();
        mgr.sync_pods(&[pod("uid-ps", "web", "Always", c.clone())]).await;
        let cid = mgr.pods.read().await["uid-ps"].container_ids["app"].clone();
        assert_eq!(*rt.execs.lock().unwrap(), vec![(cid.clone(), vec!["warm".to_string(), "up".to_string()])]);
        assert!(rt.stop_graces.lock().unwrap().is_empty(), "a hook that passed kills nothing");

        let (rt, mgr) = manager();
        rt.set_exec_exit_code(1);
        mgr.sync_pods(&[pod("uid-ps2", "web", "Always", c)]).await;
        let cid = mgr.pods.read().await["uid-ps2"].container_ids["app"].clone();
        assert_eq!(*rt.stop_graces.lock().unwrap(), vec![(cid, 0)], "a failed hook kills its container at once");

        // `sleep` needs no runtime.
        let (rt, mgr) = manager();
        let mut c = simple_container();
        c["lifecycle"] = json!({"postStart": {"sleep": {"seconds": 0}}});
        mgr.sync_pods(&[pod("uid-ps3", "web", "Always", c)]).await;
        assert!(rt.execs.lock().unwrap().is_empty() && rt.stop_graces.lock().unwrap().is_empty());
    }

    /// #374: a preStop hook runs before the container's stop, in the grace;
    /// a sidecar's runs after every app container has stopped.
    #[tokio::test]
    async fn a_pre_stop_hook_runs_before_the_stop_and_sidecars_last() {
        let (rt, mgr) = manager();
        let mut app = simple_container();
        app["lifecycle"] = json!({"preStop": {"exec": {"command": ["drain-app"]}}});
        let mut p = pod("uid-pre", "web", "Always", app);
        p["spec"]["terminationGracePeriodSeconds"] = json!(5);
        p["spec"]["initContainers"] = json!([{"name": "proxy", "image": "busybox:latest", "restartPolicy": "Always",
            "lifecycle": {"preStop": {"exec": {"command": ["drain-proxy"]}}}}]);
        mgr.sync_pods(&[p]).await;
        let ids = mgr.pods.read().await["uid-pre"].container_ids.clone();
        rt.order.lock().unwrap().clear();
        mgr.stop_pod("uid-pre").await.unwrap();
        let order = rt.order.lock().unwrap().clone();
        assert_eq!(order, vec![
            "exec drain-app".to_string(), format!("stop {}", ids["app"]),
            "exec drain-proxy".to_string(), format!("stop {}", ids["proxy"]),
        ]);
        assert!(rt.stop_graces.lock().unwrap().iter().all(|(_, g)| (4..=5).contains(g)), "{:?}", rt.stop_graces.lock().unwrap());

        // A failed hook: the stop goes on.
        let (rt, mgr) = manager();
        let mut app = simple_container();
        app["lifecycle"] = json!({"preStop": {"exec": {"command": ["drain"]}}});
        mgr.sync_pods(&[pod("uid-pre2", "web", "Always", app)]).await;
        rt.set_exec_exit_code(1);
        mgr.stop_pod("uid-pre2").await.unwrap();
        assert_eq!(rt.stop_graces.lock().unwrap().len(), 1);
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

    /// #130: the run that ended is the container's lastState (exit code and
    /// upstream's reason); a run that ends for good says why in its state.
    #[tokio::test]
    async fn the_previous_run_is_kept_for_last_state() {
        let (rt, mgr) = manager();
        let p = pod("uid-1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        assert!(mgr.last_terminated("uid-1").is_empty());
        let old_cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&old_cid, ContainerState::Exited, 137);
        mgr.sync_pods(&[p]).await;
        let last = &mgr.last_terminated("uid-1")["app"];
        assert_eq!((last.exit_code, last.reason.as_str()), (137, "Error"));
        mgr.stop_pod("uid-1").await.unwrap();
        assert!(mgr.last_terminated("uid-1").is_empty(), "forgotten with the pod");

        let (rt, mgr) = manager();
        let job = pod("uid-2", "job", "Never", simple_container());
        mgr.sync_pods(&[job.clone()]).await;
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 0);
        let u = &mgr.sync_pods(&[job]).await.updates[0];
        assert_eq!(u.container_statuses[0].reason, "Completed");
        assert_eq!(terminated_reason("OOMKilled", 137), "OOMKilled");
        assert_eq!(terminated_reason("", 2), "Error");
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

    /// #460 (cd264eea6874): readiness changes only after `successThreshold`
    /// successes or `failureThreshold` failures in a row; a different result
    /// starts the run again.
    #[test]
    fn readiness_changes_only_at_its_thresholds() {
        let mut run = ReadinessRun::default();
        // successThreshold 3: two successes are not enough, the third is.
        assert!(!run.after(false, true, 3, 3));
        assert!(!run.after(false, true, 3, 3));
        assert!(run.after(false, true, 3, 3));
        // failureThreshold 3: a ready container stays ready through two.
        assert!(run.after(true, false, 3, 3));
        assert!(run.after(true, false, 3, 3));
        // A success between breaks the run of failures.
        assert!(run.after(true, true, 3, 3));
        assert!(run.after(true, false, 3, 3));
        assert!(run.after(true, false, 3, 3));
        assert!(!run.after(true, false, 3, 3));
        assert_eq!(run, ReadinessRun { ok: false, count: 3 });
        // Thresholds of 1 (and a 0, taken as 1) follow every result.
        let mut run = ReadinessRun::default();
        assert!(run.after(false, true, 1, 1));
        assert!(!run.after(true, false, 0, 1));
        assert_eq!(probe_success_threshold(&json!({})), 1);
        assert_eq!(probe_success_threshold(&json!({"successThreshold": 0})), 1);
        assert_eq!(probe_success_threshold(&json!({"successThreshold": 4})), 4);
    }

    /// #460 through the pod loop: `successThreshold: 2` needs two passing
    /// runs to be ready, `failureThreshold: 3` three failing ones to stop
    /// being ready, and a restart starts the count again.
    #[tokio::test]
    async fn readiness_probe_honours_its_thresholds() {
        let (rt, mgr) = manager();
        let container = json!({
            "name": "app",
            "image": "busybox:latest",
            "readinessProbe": {"exec": {"command": ["check"]}, "successThreshold": 2, "failureThreshold": 3}
        });
        let p = pod("uid-1", "web", "Always", container);
        let ready = |o: &SyncOutcome| o.updates[0].container_statuses[0].ready;
        // Each sync after the first, a period on, probes once.
        async fn probed(mgr: &PodManager, p: &Value) -> SyncOutcome {
            mgr.expire_probes();
            mgr.sync_pods(std::slice::from_ref(p)).await
        }
        rt.set_exec_exit_code(0);
        mgr.sync_pods(&[p.clone()]).await;
        let probe = || probed(&mgr, &p);
        let o = probe().await;
        assert!(!ready(&o), "one success of two");
        let o = probe().await;
        assert!(ready(&o), "two successes");
        rt.set_exec_exit_code(1);
        let o = probe().await;
        assert!(ready(&o), "one failure of three");
        let o = probe().await;
        assert!(ready(&o), "two failures of three");
        rt.set_exec_exit_code(0);
        let o = probe().await;
        assert!(ready(&o), "a success between");
        rt.set_exec_exit_code(1);
        for _ in 0..2 {
            assert!(ready(&probe().await), "the failures start again");
        }
        let o = probe().await;
        assert!(!ready(&o), "three failures in a row");
        rt.set_exec_exit_code(0);
        let o = probe().await;
        assert!(!ready(&o), "one success of two again");
        // A new run of the container forgets the old one's count.
        let cid = rt.container_ids().pop().unwrap();
        rt.set_container_state(&cid, ContainerState::Exited, 1);
        let o = mgr.sync_pods(&[p.clone()]).await;
        assert_eq!(o.updates[0].container_statuses[0].restart_count, 1, "restarted");
        assert_eq!(tracked(&mgr, "uid-1").await.readiness_run.get("app"), None);
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
            "report=3.0ms", "total=", "attempts=1", "pending=1", "volume/scratch=", "container/app=",
            // #139: the runtime's steps, the status read and the rest.
            "sandbox/acquire=1.0ms", "sandbox/cni=2.0ms", "sandbox/status=", "sandbox/other=",
            // #311: the runtime's steps and the rest.
            "container/app/clone=3.0ms", "container/app/spawn=4.0ms", "container/app/other="] {
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

    /// #395: a hostPath names the volume mounted at it or at the deepest
    /// directory above it, by component; `/` and an unlisted path name none.
    #[test]
    fn a_host_path_names_the_volume_it_is_on() {
        let items = vec![
            json!({"id": "root", "name": "node", "mounted_at": "/"}),
            json!({"id": "data", "name": "data", "mounted_at": "/data"}),
            json!({"id": "etcd", "name": "fastetcd-data", "attachments": [{"mounted_at": "/data/fastetcd/"}]}),
            json!({"id": "", "name": "no-id", "mounted_at": "/data/fastetcd/member"}),
        ];
        assert_eq!(host_path_volume(&items, "/data/fastetcd"), Some("etcd"));
        assert_eq!(host_path_volume(&items, "/data/fastetcd/member/wal"), Some("etcd"));
        assert_eq!(host_path_volume(&items, "/data/fastetcd2"), Some("data"), "by component, not by prefix");
        assert_eq!(host_path_volume(&items, "/data/"), Some("data"));
        assert_eq!(host_path_volume(&items, "/etc/kubernetes"), None, "`/` names nothing");
        assert_eq!(host_path_volume(&[], "/data"), None);
    }

    /// #395: a hostPath is matched where the host resolves it: `/data` a
    /// link to `/d` (stormcos), relative links, `..`, and a missing tail.
    #[test]
    fn a_host_path_is_resolved_through_the_hosts_links() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        std::fs::create_dir_all(r.join("d/fastetcd")).unwrap();
        std::os::unix::fs::symlink("/d", r.join("data")).unwrap();
        std::fs::create_dir_all(r.join("etc")).unwrap();
        std::os::unix::fs::symlink("../d/fastetcd", r.join("etc/etcd")).unwrap();
        std::os::unix::fs::symlink("loop", r.join("loop")).unwrap();
        assert_eq!(host_real_path(r, "/data/fastetcd"), "/d/fastetcd");
        assert_eq!(host_real_path(r, "/data/fastetcd/member/"), "/d/fastetcd/member");
        assert_eq!(host_real_path(r, "/etc/etcd"), "/d/fastetcd");
        assert_eq!(host_real_path(r, "/data/../etc"), "/etc");
        assert_eq!(host_real_path(r, "/nowhere/x"), "/nowhere/x");
        assert_eq!(host_real_path(r, "/loop"), "/loop", "a loop is left as given");
        let items = vec![json!({"id": "etcd", "mounted_at": "/d/fastetcd"})];
        assert_eq!(host_path_volume(&items, &host_real_path(r, "/data/fastetcd")), Some("etcd"));
    }

    /// #395: a static pod's hostPaths are recorded as the engine volumes
    /// they are on, as `storm.io/volume.<name>`; an API pod's are not, and
    /// a hostPath has no `VolumeStats` (its sizes are the whole volume's).
    #[tokio::test]
    async fn a_static_pods_host_paths_are_its_volumes() {
        let app = axum::Router::new().route(
            "/api/v1/volumes",
            axum::routing::get(|| async {
                axum::Json(json!({"items": [
                    {"id": "vol-data", "name": "data", "mounted_at": "/data", "virtual_size_bytes": 9},
                ]}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_rt, mgr) = manager();
        let mgr = with_stormblock(mgr, &url);
        let volumes = json!([
            {"name": "etcd-data", "hostPath": {"path": "/data/fastetcd"}},
            {"name": "certs", "hostPath": {"path": "/etc/kubernetes/pki"}},
        ]);
        let mut api = pod("uid-api", "api", "Always", simple_container());
        api["spec"]["volumes"] = volumes.clone();
        mgr.resolve_volumes(&api).await.unwrap();
        assert!(mgr.volume_annotations("uid-api").is_empty(), "an API pod's hostPaths are not looked up");
        let mut st = pod("uid-etcd", "etcd-n1", "Always", simple_container());
        st["metadata"]["annotations"] = json!({CONFIG_SOURCE: "file"});
        st["spec"]["volumes"] = volumes.clone();
        mgr.resolve_volumes(&st).await.unwrap();
        let a = mgr.volume_annotations("uid-etcd");
        assert_eq!(a.len(), 1, "a path on no volume is not named: {a:?}");
        assert_eq!(a["storm.io/volume.etcd-data"], "vol-data");
        let listing = json!({"items": [{"id": "vol-data", "virtual_size_bytes": 9}]});
        assert!(volume_stats("kube-system", "etcd-n1", &volumes, &mgr.pod_volume_ids("uid-etcd"), &listing).is_empty());
    }

    /// #130: each container's image and its own clone, as annotations; a
    /// name too long for an annotation key is left out.
    #[tokio::test]
    async fn containers_are_annotated_with_their_image_and_instance() {
        let (_rt, mgr) = manager();
        let p = pod("uid-ann", "ann", "Always", simple_container());
        assert!(mgr.container_annotations("uid-ann").await.is_empty());
        mgr.sync_pods(&[p]).await;
        let a = mgr.container_annotations("uid-ann").await;
        let cid = mgr.pods.read().await["uid-ann"].container_ids["app"].clone();
        assert_eq!(a["storm.io/instance.app"], json!(format!("vol-{cid}")));
        assert_eq!(a["storm.io/image-resolved.app"], "2026-10-08T00:00:00Z");
        assert_eq!(a["storm.io/golden.app"], "img-0");
        assert_eq!(a["storm.io/image-build.app"], json!(r#"{"version":"1.0"}"#));
        // #372: the image's digest, and each claim's volume by the pod's name for it.
        assert_eq!(a["storm.io/image-digest.app"], "sha256:00");
        assert!(a.keys().all(|k| !k.starts_with("storm.io/volume.")), "no claims, no volume keys");
        mgr.record_pod_volume("uid-ann", "data", "6f1c-vol");
        let a = mgr.container_annotations("uid-ann").await;
        assert_eq!(a["storm.io/volume.data"], "6f1c-vol");
        assert_eq!(mgr.pod_volume_ids("uid-ann").get("data").map(String::as_str), Some("6f1c-vol"));
        let long = HashMap::from([("c".repeat(60), "x".to_string())]);
        assert!(mgr.annotations_for("uid-none", long).await.is_empty());
    }

    /// #131: a started pod's network-status entries come from its sandbox,
    /// for the annotation written with its start timing.
    #[tokio::test]
    async fn a_started_pods_network_status_is_its_sandboxs() {
        let (_rt, mgr) = manager();
        let p = pod("uid-net", "net", "Always", simple_container());
        assert_eq!(mgr.network_status_of("uid-net").await, None, "no sandbox yet");
        assert_eq!(mgr.sync_pods(&[p]).await.updates[0].phase, "Running");
        let status = mgr.network_status_of("uid-net").await.unwrap();
        let sandbox = mgr.pods.read().await["uid-net"].sandbox_id.clone().unwrap();
        assert_eq!(status[0]["sandbox"], json!(sandbox));
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

    /// #3: a pod waiting on the network says why in Events too, as upstream:
    /// `NetworkNotReady` with no CNI config, `FailedCreatePodSandBox` with the
    /// plugin's own words when ADD fails (seen live: Cilium's agent gone).
    #[tokio::test]
    async fn a_pod_waiting_on_the_network_has_upstreams_events() {
        let events = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let ev = events.clone();
        let app = axum::Router::new().route(
            "/api/v1/namespaces/{ns}/events",
            axum::routing::post(move |axum::Json(b): axum::Json<Value>| {
                ev.lock().unwrap().push(b);
                async { (axum::http::StatusCode::CREATED, axum::Json(json!({}))) }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mut mgr = PodManager::with_api(rt.clone(), rt.clone(), NODE, &url, "127.0.0.1", reqwest::Client::new());
        let tmp = tempfile::tempdir().unwrap();
        mgr.state_root = tmp.path().to_string_lossy().into_owned();
        let p = pod("uid-ev", "web", "Always", simple_container());
        let reasons = || events.lock().unwrap().iter().map(|e| (e["type"].as_str().unwrap_or("").to_string(), e["reason"].as_str().unwrap_or("").to_string(), e["message"].as_str().unwrap_or("").to_string())).collect::<Vec<_>>();

        rt.network.store(1, Ordering::SeqCst);
        mgr.sync_pods(&[p.clone()]).await;
        let r = reasons();
        assert!(r.iter().any(|(t, why, m)| t == "Warning" && why == "NetworkNotReady" && m.contains("no CNI network configured yet")), "{r:?}");

        rt.network.store(2, Ordering::SeqCst);
        mgr.sync_pods(&[p.clone()]).await;
        let r = reasons();
        assert!(r.iter().any(|(t, why, m)| t == "Warning" && why == "FailedCreatePodSandBox" && m.contains("CNI ADD failed: agent not serving")), "{r:?}");
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

    /// The apiserver, stormblock and stormstorage of a claim move (#338),
    /// on one URL: one PVC `db/data` bound to PV `pvc-db-data` on
    /// [`NODE`], its volume `v-1` in the engine, node `b` Ready.
    #[derive(Default)]
    struct MoveFake {
        pvc: Value,
        pv: Value,
        pods: Vec<Value>,
        /// stormstorage: id → state.
        migrations: HashMap<String, String>,
        /// Everything asked of stormstorage and the engine, in order.
        asked: Vec<String>,
        /// The next cutover answers this status instead of 200 (#428):
        /// with `cutover_done`, after cutting over (its answer lost);
        /// without, having done nothing.
        cutover_error: Option<u16>,
        cutover_done: bool,
    }

    fn merge(target: &mut Value, patch: &Value) {
        let Some(p) = patch.as_object() else { *target = patch.clone(); return };
        if !target.is_object() {
            *target = json!({});
        }
        let t = target.as_object_mut().unwrap();
        for (k, v) in p {
            if v.is_null() {
                t.remove(k);
            } else {
                merge(t.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
    }

    async fn move_fake(ann: Value) -> (PodManager, Arc<std::sync::Mutex<MoveFake>>) {
        use axum::extract::{Path, State};
        use axum::response::IntoResponse as _;
        use axum::routing::{get as http_get, patch as http_patch, post as http_post, delete as http_delete};
        type S = Arc<std::sync::Mutex<MoveFake>>;
        let state: S = Arc::new(std::sync::Mutex::new(MoveFake {
            pvc: json!({"metadata": {"namespace": "db", "name": "data", "uid": "pvc-1", "annotations": ann},
                        "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": "pvc-db-data", "storageClassName": "stormblock"}}),
            pv: json!({"metadata": {"name": "pvc-db-data", "uid": "pv-1", "annotations": {"storm.io/node": NODE}},
                       "spec": {"csi": {"driver": "stormblock.storm.io", "volumeHandle": "pvc-db-data"},
                                "capacity": {"storage": "1Gi"},
                                "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                                    {"key": "kubernetes.io/hostname", "operator": "In", "values": [NODE]}]}]}}}}),
            ..Default::default()
        }));
        let app = axum::Router::new()
            .route("/api/v1/persistentvolumeclaims", http_get(|State(s): State<S>| async move {
                axum::Json(json!({"items": [s.lock().unwrap().pvc.clone()]}))
            }))
            .route("/api/v1/namespaces/{ns}/persistentvolumeclaims/{n}",
                http_get(|State(s): State<S>| async move { axum::Json(s.lock().unwrap().pvc.clone()) })
                .patch(|State(s): State<S>, axum::Json(b): axum::Json<Value>| async move {
                    let mut s = s.lock().unwrap();
                    merge(&mut s.pvc, &b);
                    axum::Json(s.pvc.clone())
                }))
            .route("/api/v1/persistentvolumes/{n}",
                http_get(|State(s): State<S>| async move { axum::Json(s.lock().unwrap().pv.clone()) })
                .merge(http_patch(|State(s): State<S>, axum::Json(b): axum::Json<Value>| async move {
                    let mut s = s.lock().unwrap();
                    s.asked.push("rebind".into());
                    merge(&mut s.pv, &b);
                    axum::Json(s.pv.clone())
                })))
            .route("/api/v1/namespaces/{ns}/pods", http_get(|State(s): State<S>| async move {
                axum::Json(json!({"items": s.lock().unwrap().pods.clone()}))
            }))
            .route("/api/v1/pods", http_get(|State(s): State<S>| async move {
                axum::Json(json!({"items": s.lock().unwrap().pods.clone()}))
            }))
            .route("/api/v1/nodes/{n}", http_get(|Path(n): Path<String>| async move {
                if n == "b" {
                    axum::Json(json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}})).into_response()
                } else {
                    axum::http::StatusCode::NOT_FOUND.into_response()
                }
            }))
            .route("/api/v1/namespaces/{ns}/events", http_post(|| async { (axum::http::StatusCode::CREATED, axum::Json(json!({}))) }))
            .route("/api/v1/volumes", http_get(|| async { axum::Json(json!({"items": [{"id": "v-1", "name": "pvc-db-data"}]})) }))
            .route("/api/v1/volumes/{id}/attach", http_delete(|State(s): State<S>, Path(id): Path<String>| async move {
                s.lock().unwrap().asked.push(format!("detach {id}"));
                axum::Json(json!({}))
            }))
            .route("/api/v1/nodes/{n}/volumes/{v}/migrate", http_post(
                |State(s): State<S>, Path((n, v)): Path<(String, String)>, axum::Json(b): axum::Json<Value>| async move {
                    let mut s = s.lock().unwrap();
                    s.asked.push(format!("start {n}/{v}→{} {}", b["to"].as_str().unwrap_or(""), b["placement"]));
                    s.migrations.insert("m-1".into(), "copying".into());
                    axum::Json(json!({"id": "m-1", "state": "starting", "bytes_total": 1024, "bytes_remaining": 1024}))
                }))
            .route("/api/v1/migrations", http_get(|State(s): State<S>| async move {
                let all: Vec<Value> = s.lock().unwrap().migrations.iter().map(|(id, st)| json!({
                    "id": id, "state": st, "source_node": NODE, "source_volume_id": "v-1",
                    "target": {"node": "b"}, "bytes_total": 1024, "bytes_remaining": 512
                })).collect();
                axum::Json(json!({"migrations": all}))
            }))
            .route("/api/v1/migrations/{id}", http_get(|State(s): State<S>, Path(id): Path<String>| async move {
                match s.lock().unwrap().migrations.get(&id) {
                    Some(st) => axum::Json(json!({"id": id, "state": st, "bytes_total": 1024, "bytes_remaining": 256})).into_response(),
                    None => axum::http::StatusCode::NOT_FOUND.into_response(),
                }
            }))
            .route("/api/v1/migrations/{id}/{step}", http_post(
                |State(s): State<S>, Path((id, step)): Path<(String, String)>| async move {
                    let mut s = s.lock().unwrap();
                    s.asked.push(format!("{step} {id}"));
                    let next = match step.as_str() { "cutover" => "cut_over", "complete" => "completed", "abort" => "aborted", _ => "copying" };
                    if step == "cutover" {
                        if let Some(code) = s.cutover_error.take() {
                            if s.cutover_done {
                                s.migrations.insert(id.clone(), next.into());
                            }
                            let code = axum::http::StatusCode::from_u16(code).unwrap();
                            return (code, "cutover: no answer").into_response();
                        }
                    }
                    // After the cutover, an abort is refused: the target is the claim's.
                    if step == "abort" && matches!(s.migrations.get(&id).map(String::as_str), Some("cut_over" | "completed")) {
                        return (axum::http::StatusCode::CONFLICT, "cut over: not aborted").into_response();
                    }
                    s.migrations.insert(id.clone(), next.into());
                    axum::Json(json!({"id": id, "state": next})).into_response()
                }))
            .with_state(state.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new());
        let mgr = with_stormblock(mgr, &url)
            .with_stormstorage(Some(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none())));
        (mgr, state)
    }

    fn annotation(s: &Arc<std::sync::Mutex<MoveFake>>, key: &str) -> String {
        s.lock().unwrap().pvc["metadata"]["annotations"][key].as_str().unwrap_or("").to_string()
    }

    fn asked(s: &Arc<std::sync::Mutex<MoveFake>>) -> Vec<String> {
        s.lock().unwrap().asked.clone()
    }

    /// **#338.** A claim asked onto b: mirrored there (onto the set asked
    /// for), `synced`; the cutover waits for the pod running on it here;
    /// once it is gone, detached, cut over, the source retired, the PV
    /// pointed at b, `complete`. A pod here then waits rather than getting a
    /// fresh volume.
    #[tokio::test]
    async fn a_claim_moves_to_another_node_once_its_pod_stops() {
        use crate::claim_move as cm;
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b", cm::MOVE_SET: "r1"})).await;
        assert_eq!(mgr.move_claims().await, Some(cm::POLL));
        assert_eq!(asked(&s), [r#"start test-node/v-1→b {"array":"r1"}"#]);
        assert_eq!(annotation(&s, cm::STATE), cm::COPYING);
        assert_eq!((annotation(&s, cm::MIGRATION), annotation(&s, cm::TARGET), annotation(&s, cm::FROM)),
                   ("m-1".into(), "b".into(), NODE.into()));

        // Copying: progress.
        mgr.move_claims().await;
        assert!(annotation(&s, cm::PROGRESS).starts_with("75%"), "{}", annotation(&s, cm::PROGRESS));

        // In sync, with a pod running on it here: synced, and it waits.
        {
            let mut f = s.lock().unwrap();
            f.migrations.insert("m-1".into(), "synced".into());
            f.pods = vec![json!({"metadata": {"name": "db-0", "namespace": "db"}, "spec": {"nodeName": NODE,
                "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "data"}}]}, "status": {"phase": "Running"}})];
        }
        assert_eq!(mgr.move_claims().await, Some(cm::POLL));
        assert_eq!(annotation(&s, cm::STATE), cm::SYNCED);
        assert!(annotation(&s, cm::PROGRESS).contains("waits for pod db/db-0"));
        assert_eq!(asked(&s).len(), 1, "no cutover while it runs");

        // Its replacement, Pending here, does not hold it up.
        s.lock().unwrap().pods[0]["status"]["phase"] = json!("Pending");
        assert_eq!(mgr.move_claims().await, None, "done");
        assert_eq!(asked(&s), [r#"start test-node/v-1→b {"array":"r1"}"#, "detach v-1", "cutover m-1", "complete m-1", "rebind"]);
        assert_eq!(annotation(&s, cm::STATE), cm::COMPLETE);
        let pv = s.lock().unwrap().pv.clone();
        assert_eq!(cm::pv_node(&pv).as_deref(), Some("b"), "the PV names b");
        assert_eq!(pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0], "b");
        assert_eq!(pv["spec"]["csi"]["volumeHandle"], "pvc-db-data", "b's copy has the same name");

        // A pod still here waits; nothing more is asked of anyone.
        let pvc = s.lock().unwrap().pvc.clone();
        assert!(cm::start_hold(&pvc, Some(&pv), NODE).unwrap().contains("on b's storage"));
        assert_eq!(cm::start_hold(&pvc, Some(&pv), "b"), None, "b starts it");
        let n = asked(&s).len();
        assert_eq!(mgr.move_claims().await, None);
        assert_eq!(asked(&s).len(), n);
    }

    /// #338: what is not moved is refused, with the reason on the PVC; a
    /// failed copy and a request taken off mid-copy leave the claim here.
    #[tokio::test]
    async fn a_claim_move_refused_failed_or_taken_back_leaves_it_here() {
        use crate::claim_move as cm;
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "c"})).await;
        mgr.move_claims().await;
        assert_eq!(annotation(&s, cm::STATE), cm::REFUSED);
        assert!(annotation(&s, cm::REASON).contains("node c does not exist"));
        assert!(asked(&s).is_empty());
        mgr.move_claims().await;
        assert!(asked(&s).is_empty(), "not retried until asked again");

        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        s.lock().unwrap().pvc["spec"]["accessModes"] = json!(["ReadWriteMany"]);
        mgr.move_claims().await;
        assert!(annotation(&s, cm::REASON).contains("ReadWriteMany"));

        // The copy fails: failed, the reason stormstorage gave.
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        mgr.move_claims().await;
        s.lock().unwrap().migrations.insert("m-1".into(), "failed".into());
        assert_eq!(mgr.move_claims().await, None);
        assert_eq!(annotation(&s, cm::STATE), cm::FAILED);
        assert!(annotation(&s, cm::REASON).contains("the copy failed"));
        assert_eq!(cm::pv_node(&s.lock().unwrap().pv).as_deref(), Some(NODE), "the claim stays here");

        // Taken off mid-copy: aborted.
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        mgr.move_claims().await;
        s.lock().unwrap().pvc["metadata"]["annotations"][cm::MOVE_TO] = Value::Null;
        mgr.move_claims().await;
        assert_eq!(asked(&s).last().map(String::as_str), Some("abort m-1"));
        assert_eq!(annotation(&s, cm::STATE), cm::ABORTED);
        assert!(annotation(&s, cm::REASON).contains("taken off"));
    }

    /// A claim move brought to its cutover, with no pod holding it: the
    /// next `move_claims` pass cuts over.
    async fn move_at_cutover(err: u16, done: bool) -> (PodManager, Arc<std::sync::Mutex<MoveFake>>) {
        use crate::claim_move as cm;
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        mgr.move_claims().await;
        let mut f = s.lock().unwrap();
        f.migrations.insert("m-1".into(), "synced".into());
        f.cutover_error = Some(err);
        f.cutover_done = done;
        drop(f);
        (mgr, s)
    }

    /// #428 (finding e4aaa109678b): stormstorage cut over, but its answer
    /// was lost (a 504). The migration is read again: the move goes on to
    /// retire the source and point the PV at b. It is never aborted, and the
    /// claim is never said to have stayed here.
    #[tokio::test]
    async fn a_cutover_whose_answer_is_lost_still_moves_the_claim() {
        use crate::claim_move as cm;
        let (mgr, s) = move_at_cutover(504, true).await;
        assert_eq!(mgr.move_claims().await, None, "done");
        let asked = asked(&s);
        assert!(!asked.iter().any(|a| a.starts_with("abort")), "{asked:?}");
        assert_eq!(&asked[asked.len() - 3..], ["cutover m-1", "complete m-1", "rebind"]);
        assert_eq!(annotation(&s, cm::STATE), cm::COMPLETE);
        assert_eq!(cm::pv_node(&s.lock().unwrap().pv).as_deref(), Some("b"));
    }

    /// The cutover failed on the way (a 504) and did nothing: still synced,
    /// nothing aborted; the next pass cuts over.
    #[tokio::test]
    async fn a_cutover_that_did_not_happen_is_asked_again_not_aborted() {
        use crate::claim_move as cm;
        let (mgr, s) = move_at_cutover(504, false).await;
        assert_eq!(mgr.move_claims().await, Some(cm::POLL));
        assert!(!asked(&s).iter().any(|a| a.starts_with("abort")));
        assert_eq!(annotation(&s, cm::STATE), cm::CUTTING_OVER);
        assert_eq!(cm::pv_node(&s.lock().unwrap().pv).as_deref(), Some(NODE), "still here");
        assert_eq!(mgr.move_claims().await, None, "cut over on the next pass");
        assert_eq!(annotation(&s, cm::STATE), cm::COMPLETE);
        assert_eq!(cm::pv_node(&s.lock().unwrap().pv).as_deref(), Some("b"));
    }

    /// Refused while still synced (a 409): the copy is aborted and the
    /// claim stays here, with the reason.
    #[tokio::test]
    async fn a_cutover_refused_before_it_happens_aborts_the_move() {
        use crate::claim_move as cm;
        let (mgr, s) = move_at_cutover(409, false).await;
        assert_eq!(mgr.move_claims().await, None);
        assert_eq!(asked(&s).last().map(String::as_str), Some("abort m-1"));
        assert_eq!(annotation(&s, cm::STATE), cm::FAILED);
        assert!(annotation(&s, cm::REASON).contains("the cutover was refused"), "{}", annotation(&s, cm::REASON));
        assert_eq!(cm::pv_node(&s.lock().unwrap().pv).as_deref(), Some(NODE));
    }

    #[test]
    fn a_cutover_error_is_read_from_the_migration_after_it() {
        use crate::vm_migration::DiskMigration;
        let at = |st: &str| Ok(DiskMigration { id: "m".into(), state: st.into(), ..Default::default() });
        let lost = "stormstorage 504 Gateway Timeout: cutover";
        let no = "stormstorage 409 Conflict: not in sync";
        assert_eq!(cutover_outcome(lost, at("cut_over")), Cutover::Done("cut_over".into()));
        assert_eq!(cutover_outcome(no, at("completed")), Cutover::Done("completed".into()));
        assert_eq!(cutover_outcome(lost, at("synced")), Cutover::Ask);
        assert!(matches!(cutover_outcome(no, at("synced")), Cutover::Refused(_)));
        assert!(matches!(cutover_outcome(lost, at("failed")), Cutover::Ended(_)));
        assert_eq!(cutover_outcome(lost, Err("stormstorage: connection refused".into())), Cutover::Ask);
    }

    /// #204: `runtimeClassName` resolves to the RuntimeClass's handler, asked
    /// once; a RuntimeClass that is not there refuses the pod with upstream's
    /// words; no class is the default runtime.
    #[tokio::test]
    async fn a_runtime_class_resolves_to_its_handler_and_a_missing_one_waits() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let asked = Arc::new(AtomicUsize::new(0));
        let n = asked.clone();
        let app = axum::Router::new().route(
            "/apis/node.k8s.io/v1/runtimeclasses/{name}",
            axum::routing::get(move |axum::extract::Path(name): axum::extract::Path<String>| {
                let n = n.clone();
                async move {
                    use axum::response::IntoResponse as _;
                    n.fetch_add(1, Ordering::SeqCst);
                    if name == "rt" {
                        axum::Json(json!({"metadata": {"name": "rt"}, "handler": "realtime-soft"})).into_response()
                    } else {
                        axum::http::StatusCode::NOT_FOUND.into_response()
                    }
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new());
        let with = |class: Option<&str>| {
            let mut p = pod("u", "p", "Always", simple_container());
            if let Some(c) = class {
                p["spec"]["runtimeClassName"] = json!(c);
            }
            p
        };
        assert_eq!(mgr.runtime_handler(&with(None)).await.unwrap(), "");
        assert_eq!(mgr.runtime_handler(&with(Some("rt"))).await.unwrap(), "realtime-soft");
        assert_eq!(mgr.runtime_handler(&with(Some("rt"))).await.unwrap(), "realtime-soft");
        assert_eq!(asked.load(Ordering::SeqCst), 1, "asked once: a handler never changes");
        match mgr.runtime_handler(&with(Some("gvisor"))).await {
            Err(CriError::Refused { reason, message }) => {
                assert_eq!((reason.as_str(), message.as_str()), ("FailedCreatePodSandBox", "RuntimeClass \"gvisor\" not found"));
            }
            other => panic!("{other:?}"),
        }
    }

    /// #430: `realtime-hard` without PREEMPT_RT is refused, never run as
    /// soft; `realtime-soft` is admitted; a mistake in the annotation is a
    /// refusal naming the field; each container gets its own real time.
    #[test]
    fn real_time_admission_refuses_hard_without_preempt_rt() {
        use crate::realtime::{Preempt, HANDLER_HARD, HANDLER_SOFT, REFUSED, RESOURCE};
        let mut c = json!({"name": "loop", "image": "i"});
        c["resources"]["requests"][RESOURCE] = json!("2");
        c["resources"]["limits"][RESOURCE] = json!("2");
        let mut p = pod("u", "rt", "Always", c);
        p["metadata"]["annotations"] = json!({"stormpump.io/realtime": "policy=fifo,priority=80"});
        match realtime_admission(&p, HANDLER_HARD, Preempt::Dynamic) {
            Err(CriError::Refused { reason, message }) => {
                assert_eq!(reason, REFUSED);
                assert!(message.contains("PREEMPT_RT") && message.contains("not run as soft"), "{message}");
            }
            other => panic!("{other:?}"),
        }
        assert!(realtime_admission(&p, HANDLER_HARD, Preempt::Rt).is_ok());
        assert!(realtime_admission(&p, HANDLER_SOFT, Preempt::Dynamic).is_ok());
        let rt = realtime_of(&p, HANDLER_SOFT, "loop").expect("real time");
        assert_eq!((rt.cores, rt.shape.priority), (2, 80));
        assert_eq!(realtime_of(&p, HANDLER_SOFT, "other"), None);
        p["metadata"]["annotations"]["stormpump.io/realtime"] = json!("policy=fifo,priority=200");
        match realtime_admission(&p, HANDLER_SOFT, Preempt::Dynamic) {
            Err(CriError::Refused { message, .. }) => assert!(message.contains("priority=\"200\""), "{message}"),
            other => panic!("{other:?}"),
        }
        // Neither asked: admitted, nothing real time.
        let plain = pod("u", "p", "Always", simple_container());
        assert!(realtime_admission(&plain, "", Preempt::Dynamic).is_ok());
        assert_eq!(realtime_of(&plain, "", "app"), None);
    }

    // ---- #437: ConfigMaps and Secrets as volumes and env ----

    fn b64(bytes: &[u8]) -> String {
        const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
            let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(A[(n >> (18 - 6 * i)) as usize & 63] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    /// A Secret's binary value: what `keyed_data` and the volume write.
    const KEYSTORE: &[u8] = &[0xff, 0xfe, 0x00, 0x80, b'k'];

    /// An apiserver with configmap `app`, secret `creds` and a secret
    /// `broken` that answers 500; everything else is 404.
    async fn keyed_api() -> (Arc<FakeRuntime>, PodManager) {
        use axum::routing::get as http_get;
        let app = axum::Router::new().route(
            "/api/v1/namespaces/{ns}/{res}/{name}",
            http_get(|axum::extract::Path((_ns, res, name)): axum::extract::Path<(String, String, String)>| async move {
                match (res.as_str(), name.as_str()) {
                    ("configmaps", "app") => Ok(axum::Json(json!({
                        "data": {"a.conf": "alpha", "b": "beta"},
                        "binaryData": {"bin": b64(&[0, 255])}
                    }))),
                    ("secrets", "creds") => Ok(axum::Json(json!({
                        "data": {"user": b64(b"admin"), "key": b64(KEYSTORE)}
                    }))),
                    ("secrets", "broken") => Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
                    _ => Err(axum::http::StatusCode::NOT_FOUND),
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let rt = Arc::new(FakeRuntime::default());
        let mut mgr = PodManager::with_api(rt.clone(), rt.clone(), NODE, &url, "127.0.0.1", reqwest::Client::new());
        let tmp = std::env::temp_dir().join(format!("rk-kubelet-437-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        mgr.state_root = tmp.to_string_lossy().into_owned();
        (rt, mgr)
    }

    fn volume_pod(uid: &str, volumes: Value) -> Value {
        let mut p = pod(uid, "web", "Always", simple_container());
        p["spec"]["volumes"] = volumes;
        p
    }

    fn listing(dir: &str) -> Vec<String> {
        fn walk(root: &std::path::Path, rel: &std::path::Path, out: &mut Vec<String>) {
            for e in std::fs::read_dir(root.join(rel)).unwrap() {
                let e = e.unwrap();
                let r = rel.join(e.file_name());
                if e.file_type().unwrap().is_dir() {
                    walk(root, &r, out);
                } else {
                    out.push(r.to_string_lossy().into_owned());
                }
            }
        }
        let mut out = Vec::new();
        walk(std::path::Path::new(dir), std::path::Path::new(""), &mut out);
        out.sort();
        out
    }

    /// #437 dc6872a9854f: a Secret's value is bytes, not lossy UTF-8.
    #[test]
    fn secret_data_is_kept_as_bytes() {
        let obj = json!({"data": {"key": b64(KEYSTORE)}});
        assert_eq!(keyed_data(KeyedKind::Secret, &obj).unwrap()["key"], KEYSTORE);
        let bad = json!({"data": {"key": "not base64!"}});
        assert!(keyed_data(KeyedKind::Secret, &bad).is_err());
        let cm = json!({"data": {"t": "text"}, "binaryData": {"b": b64(&[0, 255])}});
        let d = keyed_data(KeyedKind::ConfigMap, &cm).unwrap();
        assert_eq!((d["t"].as_slice(), d["b"].as_slice()), (&b"text"[..], &[0u8, 255][..]));
    }

    /// #437 dc6872a9854f and 1f318a543243: a secret volume writes the
    /// selected key's exact bytes at its item path and nothing else; a
    /// changed selection removes the file it no longer names.
    #[tokio::test]
    async fn secret_volume_writes_selected_items_byte_for_byte() {
        let (_rt, mgr) = keyed_api().await;
        let p = volume_pod("uid-437-sec", json!([{"name": "s", "secret": {
            "secretName": "creds", "items": [{"key": "key", "path": "keystore/ks.bin", "mode": 0o400}]
        }}]));
        let v = mgr.resolve_volumes(&p).await.unwrap();
        let dir = v["s"].path.clone();
        assert_eq!(listing(&dir), vec!["keystore/ks.bin"]);
        assert_eq!(std::fs::read(format!("{dir}/keystore/ks.bin")).unwrap(), KEYSTORE);
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(format!("{dir}/keystore/ks.bin")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o400);

        let p = volume_pod("uid-437-sec", json!([{"name": "s", "secret": {
            "secretName": "creds", "items": [{"key": "user"}]
        }}]));
        mgr.resolve_volumes(&p).await.unwrap();
        assert_eq!(listing(&dir), vec!["user"]);
        assert_eq!(std::fs::read(format!("{dir}/user")).unwrap(), b"admin");
    }

    /// #437 1f318a543243: a configMap volume's items rename and select;
    /// without items every key is written, binaryData included.
    #[tokio::test]
    async fn configmap_volume_applies_items() {
        let (_rt, mgr) = keyed_api().await;
        let p = volume_pod("uid-437-cm", json!([
            {"name": "one", "configMap": {"name": "app", "items": [{"key": "a.conf", "path": "app.conf"}]}},
            {"name": "all", "configMap": {"name": "app"}}
        ]));
        let v = mgr.resolve_volumes(&p).await.unwrap();
        assert_eq!(listing(&v["one"].path), vec!["app.conf"]);
        assert_eq!(std::fs::read(format!("{}/app.conf", v["one"].path)).unwrap(), b"alpha");
        assert_eq!(listing(&v["all"].path), vec!["a.conf", "b", "bin"]);
        assert_eq!(std::fs::read(format!("{}/bin", v["all"].path)).unwrap(), [0, 255]);
        // An item path that leaves the volume is refused.
        let p = volume_pod("uid-437-cm2", json!([
            {"name": "x", "configMap": {"name": "app", "items": [{"key": "b", "path": "../escape"}]}}
        ]));
        assert!(matches!(mgr.resolve_volumes(&p).await, Err(CriError::VolumeNotReady(_))));
    }

    /// #437 e635cb20b82e: a missing required object or key, or one the
    /// apiserver would not give, is a pod that waits; optional is empty.
    #[tokio::test]
    async fn missing_required_keyed_volumes_wait() {
        let (_rt, mgr) = keyed_api().await;
        let waits = |vols: Value| {
            let p = volume_pod("uid-437-miss", vols);
            let mgr = &mgr;
            async move {
                match mgr.resolve_volumes(&p).await {
                    Err(CriError::VolumeNotReady(m)) => m,
                    other => panic!("expected VolumeNotReady, got {:?}", other.map(|m| m.len())),
                }
            }
        };
        let m = waits(json!([{"name": "c", "configMap": {"name": "nope"}}])).await;
        assert!(m.contains("configmap default/nope not found"), "{m}");
        let m = waits(json!([{"name": "s", "secret": {"secretName": "creds", "items": [{"key": "gone"}]}}])).await;
        assert!(m.contains("has no key \"gone\""), "{m}");
        // An API failure is never read as "absent", optional or not.
        let m = waits(json!([{"name": "s", "secret": {"secretName": "broken", "optional": true}}])).await;
        assert!(m.contains("500"), "{m}");
        let m = waits(json!([{"name": "p", "projected": {"sources": [{"secret": {"name": "nope"}}]}}])).await;
        assert!(m.contains("secret default/nope not found"), "{m}");

        let p = volume_pod("uid-437-opt", json!([
            {"name": "c", "configMap": {"name": "nope", "optional": true}},
            {"name": "s", "secret": {"secretName": "creds", "optional": true, "items": [{"key": "gone"}, {"key": "user"}]}}
        ]));
        let v = mgr.resolve_volumes(&p).await.unwrap();
        assert!(listing(&v["c"].path).is_empty());
        assert_eq!(listing(&v["s"].path), vec!["user"]);
    }

    /// kube-api-access names kube-root-ca.crt; where the cluster has not
    /// published it, the kubelet's CA stands in, and the pod does not wait.
    #[tokio::test]
    async fn kube_root_ca_falls_back_to_the_kubelets_ca() {
        let (_rt, mut mgr) = keyed_api().await;
        mgr.ca_pem = Some(b"CA PEM".to_vec());
        let p = volume_pod("uid-437-ca", json!([{"name": "kube-api-access", "projected": {"sources": [
            {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}
        ]}}]));
        let v = mgr.resolve_volumes(&p).await.unwrap();
        assert_eq!(std::fs::read(format!("{}/ca.crt", v["kube-api-access"].path)).unwrap(), b"CA PEM");
    }

    /// #437 699efd7029fe: envFrom imports every key under its prefix, and
    /// env overrides it; a later entry of one name wins.
    #[tokio::test]
    async fn env_from_imports_keys_and_env_overrides() {
        let (_rt, mgr) = keyed_api().await;
        let p = pod("uid-437-env", "web", "Always", simple_container());
        let c = json!({"name": "app",
            "envFrom": [
                {"configMapRef": {"name": "app"}, "prefix": "CM_"},
                {"secretRef": {"name": "creds"}},
                {"secretRef": {"name": "nope", "optional": true}}
            ],
            "env": [
                {"name": "user", "value": "root"},
                {"name": "PASS", "valueFrom": {"secretKeyRef": {"name": "creds", "key": "user"}}},
                {"name": "MAYBE", "valueFrom": {"configMapKeyRef": {"name": "app", "key": "zz", "optional": true}}},
                {"name": "EMPTY"}
            ]});
        let env = mgr.resolve_env(&p, &c, None).await.unwrap();
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, vec!["CM_a.conf", "CM_b", "CM_bin", "key", "user", "PASS", "EMPTY"]);
        let m: HashMap<_, _> = env.into_iter().collect();
        assert_eq!(m["CM_a.conf"], "alpha");
        assert_eq!(m["user"], "root");
        assert_eq!(m["PASS"], "admin");
        assert_eq!(m["EMPTY"], "");
    }

    /// #437 e635cb20b82e: a missing required env source is
    /// CreateContainerConfigError, never a container without the variable.
    #[tokio::test]
    async fn missing_required_env_is_a_config_error() {
        let (_rt, mgr) = keyed_api().await;
        let p = pod("uid-437-enverr", "web", "Always", simple_container());
        for (c, want) in [
            (json!({"name": "app", "envFrom": [{"configMapRef": {"name": "nope"}}]}), "configmap default/nope not found"),
            (json!({"name": "app", "env": [{"name": "X", "valueFrom": {"secretKeyRef": {"name": "creds", "key": "gone"}}}]}),
             "has no key \"gone\""),
            (json!({"name": "app", "env": [{"name": "X", "valueFrom": {"secretKeyRef": {"name": "broken", "key": "k", "optional": true}}}]}),
             "500"),
        ] {
            match mgr.resolve_env(&p, &c, None).await {
                Err(CriError::Container { container, reason, message }) => {
                    assert_eq!((container.as_str(), reason.as_str()), ("app", "CreateContainerConfigError"));
                    assert!(message.contains(want), "{message}");
                }
                other => panic!("{c}: expected a config error, got {other:?}"),
            }
        }
    }

    // ---- #437 0f6ec9a72d6c: subPath ----

    fn dir_volume(path: &std::path::Path, api: bool) -> ResolvedVolume {
        ResolvedVolume { path: path.to_string_lossy().into_owned(), fstype: None, block: false, api }
    }

    /// A subPath binds the path inside the volume: one file of a ConfigMap
    /// volume, or a directory of an emptyDir made on the way; never the root.
    #[test]
    fn sub_path_binds_inside_the_volume() {
        let cm = tempfile::tempdir().unwrap();
        std::fs::write(cm.path().join("app.conf"), "x").unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let vols = HashMap::from([
            ("cfg".to_string(), dir_volume(cm.path(), true)),
            ("data".to_string(), dir_volume(scratch.path(), false)),
        ]);
        let spec = json!({"name": "app", "volumeMounts": [
            {"name": "cfg", "mountPath": "/etc/app.conf", "subPath": "app.conf"},
            {"name": "data", "mountPath": "/var/lib/a", "subPath": "a/db"},
            {"name": "data", "mountPath": "/var/lib/p", "subPathExpr": "pods/$(POD_NAME)"}
        ]});
        let env = vec![("POD_NAME".to_string(), "web-0".to_string())];
        let m = resolve_mounts(&spec, &vols, &env).unwrap();
        let src: Vec<&str> = m.iter().map(|m| m.host_path.as_str()).collect();
        let cmp = cm.path().display().to_string();
        let sp = scratch.path().display().to_string();
        assert_eq!(src, vec![format!("{cmp}/app.conf"), format!("{sp}/a/db"), format!("{sp}/pods/web-0")]);
        assert!(scratch.path().join("a/db").is_dir() && scratch.path().join("pods/web-0").is_dir());
    }

    /// A subPath that leaves the volume, goes through a symlink, is missing
    /// from an API volume, or is on a claim filesystem is refused, and the
    /// volume's root is never bound in its place.
    #[test]
    fn sub_path_refusals() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
        let vols = HashMap::from([
            ("v".to_string(), dir_volume(root.path(), false)),
            ("api".to_string(), dir_volume(root.path(), true)),
            ("claim".to_string(), ResolvedVolume { path: "/dev/ublkb9".into(), fstype: Some("ext4".into()), block: false, api: false }),
        ]);
        for (mount, want) in [
            (json!({"name": "v", "mountPath": "/m", "subPath": "../up"}), "not a path inside the volume"),
            (json!({"name": "v", "mountPath": "/m", "subPath": "/abs"}), "not a path inside the volume"),
            (json!({"name": "v", "mountPath": "/m", "subPath": "link/x"}), "is a symlink"),
            (json!({"name": "v", "mountPath": "/m", "subPathExpr": "$(UNSET)/../x"}), "not a path inside the volume"),
            (json!({"name": "api", "mountPath": "/m", "subPath": "absent"}), "No such file"),
            (json!({"name": "claim", "mountPath": "/m", "subPath": "pgdata"}), "stormpump#274"),
            (json!({"name": "v", "mountPath": "/m", "subPath": "a", "subPathExpr": "b"}), "mutually exclusive"),
        ] {
            let spec = json!({"name": "app", "volumeMounts": [mount.clone()]});
            let e = resolve_mounts(&spec, &vols, &[]).unwrap_err();
            assert!(e.contains(want), "{mount}: {e}");
        }
        assert!(!outside.path().join("x").exists());
    }


    /// A subPath mount on the start path: a refusal is
    /// CreateContainerConfigError, and no container is created.
    #[tokio::test]
    async fn a_refused_sub_path_creates_no_container() {
        let (rt, mgr) = manager();
        let mut c = simple_container();
        c["volumeMounts"] = json!([{"name": "scratch", "mountPath": "/m", "subPath": "../up"}]);
        let mut p = pod("uid-437-sub", "web", "Always", c);
        p["spec"]["volumes"] = json!([{"name": "scratch", "emptyDir": {}}]);
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.phase, "Pending");
        assert_eq!(u.container_statuses[0].reason, "CreateContainerConfigError");
        assert!(rt.last_container_config.lock().unwrap().is_none());
    }

    // ---- #437: restarts and probes ----

    async fn tracked(mgr: &PodManager, uid: &str) -> PodState {
        mgr.pods.read().await.get(uid).cloned().unwrap()
    }

    /// #437 1e8f97747676: a restart whose stop of the old container fails
    /// makes no replacement and keeps the old ID.
    #[tokio::test]
    async fn a_restart_waits_until_the_old_container_is_gone() {
        let (rt, mgr) = manager();
        let p = pod("uid-437-r1", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        let old = rt.container_ids().pop().unwrap();
        rt.set_container_state(&old, ContainerState::Exited, 1);
        rt.fail_stop.store(true, Ordering::SeqCst);

        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(rt.container_ids(), vec![old.clone()], "no replacement while the old one may run");
        assert!(!rt.removed_containers.lock().unwrap().contains(&old));
        let cs = &u.container_statuses[0];
        assert_eq!((cs.state.as_str(), cs.container_id.as_str(), cs.restart_count), ("waiting", old.as_str(), 0));
        assert_eq!(tracked(&mgr, "uid-437-r1").await.container_ids["app"], old);
    }

    /// #437 12e774c42aec: the replacement is recorded before it is started;
    /// a failed start removes it, and one that cannot be removed stays
    /// recorded, so it is removed before anything else is made.
    #[tokio::test]
    async fn a_failed_restart_start_keeps_track_of_the_new_container() {
        let (rt, mgr) = manager();
        let p = pod("uid-437-r2", "web", "Always", simple_container());
        mgr.sync_pods(&[p.clone()]).await;
        let old = rt.container_ids().pop().unwrap();
        rt.set_container_state(&old, ContainerState::Exited, 1);
        rt.fail_start.lock().unwrap().insert("app".into());
        let new = format!("c-app-{}", rt.next_id.load(Ordering::SeqCst));
        rt.fail_remove.lock().unwrap().insert(new.clone());

        let u = mgr.sync_pods(&[p.clone()]).await.updates.remove(0);
        let st = tracked(&mgr, "uid-437-r2").await;
        assert_eq!(st.container_ids["app"], new, "recorded though its start failed");
        assert!(st.unstarted.contains("app"));
        assert_eq!(u.container_statuses[0].container_id, new);
        assert_eq!(rt.container_ids(), vec![new.clone()], "the old one went first");

        // The next try (past its back-off) removes it before making another.
        rt.fail_start.lock().unwrap().clear();
        rt.fail_remove.lock().unwrap().clear();
        mgr.backoff.forget_pod("uid-437-r2");
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert!(rt.removed_containers.lock().unwrap().contains(&new));
        assert_eq!(u.container_statuses[0].state, "running");
        let st = tracked(&mgr, "uid-437-r2").await;
        assert!(!st.unstarted.contains("app"));
        assert_eq!(rt.container_ids(), vec![st.container_ids["app"].clone()]);
    }

    /// #437 6735237ca645 and 6c156be85e7f: a restarted container's startup
    /// probe runs again (it is not ready before it passes), and it keeps the
    /// kubelet's resolv.conf.
    #[tokio::test]
    async fn a_restarted_container_starts_its_probes_over_and_keeps_resolv_conf() {
        let (rt, mgr) = manager();
        let c = json!({
            "name": "app", "image": "busybox:latest",
            "startupProbe": {"exec": {"command": ["up"]}, "failureThreshold": 30}
        });
        let p = pod("uid-437-r3", "web", "Always", c);
        mgr.sync_pods(&[p.clone()]).await;
        let has_resolv = |cfg: &ContainerConfig| cfg.mounts.iter().any(|m| m.container_path == "/etc/resolv.conf");
        assert!(has_resolv(&rt.last_container_config().unwrap()));
        rt.set_exec_exit_code(0);
        mgr.expire_probes();
        let u = mgr.sync_pods(&[p.clone()]).await.updates.remove(0);
        assert!(u.container_statuses[0].ready, "startup probe passed");
        assert_eq!(tracked(&mgr, "uid-437-r3").await.startup_passed.get("app"), Some(&true));

        let old = rt.container_ids().pop().unwrap();
        rt.set_container_state(&old, ContainerState::Exited, 1);
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        assert_eq!(u.container_statuses[0].restart_count, 1);
        assert!(!u.container_statuses[0].ready, "not ready before its startup probe passes again");
        let st = tracked(&mgr, "uid-437-r3").await;
        assert_eq!(st.startup_passed.get("app"), None);
        assert_eq!(st.ready.get("app"), Some(&false));
        assert!(has_resolv(&rt.last_container_config().unwrap()), "the restart kept resolv.conf");
    }

    /// #437 c57f2ce7c38d: a liveness probe that gives up on a Never pod's
    /// container stops it, and it is not made again.
    #[tokio::test]
    async fn a_probe_that_gives_up_under_never_does_not_restart() {
        let (rt, mgr) = manager();
        let c = json!({
            "name": "app", "image": "busybox:latest",
            "livenessProbe": {"exec": {"command": ["check"]}, "failureThreshold": 1}
        });
        let p = pod("uid-437-r4", "job", "Never", c);
        mgr.sync_pods(&[p.clone()]).await;
        let cid = rt.container_ids().pop().unwrap();
        rt.set_exec_exit_code(1);
        mgr.expire_probes();
        let u = mgr.sync_pods(&[p]).await.updates.remove(0);
        let cs = &u.container_statuses[0];
        assert_eq!((cs.state.as_str(), cs.restart_count), ("terminated", 0));
        assert!(rt.stop_graces.lock().unwrap().iter().any(|(c, _)| *c == cid));
        assert_eq!(rt.container_ids(), vec![cid], "no second run");
        assert!(tracked(&mgr, "uid-437-r4").await.terminated.contains_key("app"));
    }

    /// #437 (2eee3c8b1cb1): a container's hostPorts reach the sandbox; a
    /// port with no hostPort, and a hostNetwork pod's, do not.
    #[test]
    fn host_ports_reach_the_sandbox_config() {
        let mut c = simple_container();
        c["ports"] = json!([
            {"containerPort": 80, "hostPort": 8080},
            {"containerPort": 53, "hostPort": 5353, "protocol": "UDP", "hostIP": "10.0.0.1"},
            {"containerPort": 9090}
        ]);
        let mut p = pod("uid-437-hp", "web", "Always", c);
        let got: Vec<(String, i32, i32, String)> = build_sandbox_config(&p)
            .port_mappings
            .into_iter()
            .map(|m| (m.protocol, m.container_port, m.host_port, m.host_ip))
            .collect();
        assert_eq!(
            got,
            vec![
                ("TCP".into(), 80, 8080, String::new()),
                ("UDP".into(), 53, 5353, "10.0.0.1".into())
            ]
        );
        p["spec"]["hostNetwork"] = json!(true);
        assert!(build_sandbox_config(&p).port_mappings.is_empty());
    }

    /// #437 0417958a2e64: a copy started by a pass whose PVC write was lost
    /// (or before a kubelet restart) is found and recorded, never started a
    /// second time; a failed one is aborted before a new one starts.
    #[tokio::test]
    async fn a_claim_move_whose_record_was_lost_adopts_its_copy() {
        use crate::claim_move as cm;
        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        mgr.move_claims().await;
        assert_eq!(annotation(&s, cm::MIGRATION), "m-1");
        // The write is lost: the PVC says nothing of the copy.
        for k in [cm::MIGRATION, cm::STATE, cm::TARGET, cm::FROM, cm::PROGRESS] {
            s.lock().unwrap().pvc["metadata"]["annotations"][k] = Value::Null;
        }
        assert_eq!(mgr.move_claims().await, Some(cm::POLL));
        assert_eq!(asked(&s).iter().filter(|a| a.starts_with("start ")).count(), 1, "{:?}", asked(&s));
        assert_eq!((annotation(&s, cm::MIGRATION), annotation(&s, cm::STATE)), ("m-1".into(), cm::COPYING.into()));

        let (mgr, s) = move_fake(json!({cm::MOVE_TO: "b"})).await;
        s.lock().unwrap().migrations.insert("m-0".into(), "failed".into());
        mgr.move_claims().await;
        assert_eq!(asked(&s), ["abort m-0", "start test-node/v-1→b null"]);
        assert_eq!(annotation(&s, cm::MIGRATION), "m-1");
    }
}
