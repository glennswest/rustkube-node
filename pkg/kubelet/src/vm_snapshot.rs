//! `VirtualMachineSnapshot` (`snapshot.kubevirt.io/v1beta1`) for the VMs this
//! node runs (#53).
//!
//! stormvm reads the object (`stormvm_spec::snapshot`) and takes the snapshot
//! (`stormvm_console::snapshot::take`: freeze → pause → one stormblock group
//! snapshot of every volume → unpause → thaw). This module decides which
//! objects are this node's, and writes what happened onto them.
//!
//! **Whose it is.** The VM a snapshot names is this node's when its
//! registration is here (`/run/stormvm/<ns>/<vm>`): that is the node whose
//! stormblock holds the VM's volumes, and so the only one that can take them.
//! The node that starts a snapshot marks it (`storm.io/snapshot-node`, written
//! against the object's resourceVersion), so exactly one node takes it and a
//! restarted kubelet knows which in-progress snapshots were its own.
//!
//! **A restart mid-take** finds its snapshot `InProgress` and not in flight,
//! and takes it again. That is safe: stormblock is idempotent by name, and the
//! group is named for the VM and the snapshot (`<ns>.<vm>.<snapshot>`), so the
//! second take answers with what the first made — at the cost of the guest
//! held still once more.
//!
//! **A disk on another driver's claim** (#157: a stormblock-csi claim, a RAID
//! across servers) is not in this node's stormblock. It is taken KubeVirt's
//! way, in the same hold of the guest: one `VolumeSnapshot` per such disk
//! (`vmsnapshot-<snapshot uid>-volume-<disk>`, the driver's default class,
//! owned by the VirtualMachineSnapshot), waited for until its driver has cut
//! it (`status.creationTime`). Which disks, which claims, and what a restore
//! needs to make a claim like them is recorded at the claim
//! (`storm.io/snapshot-volumesnapshots`). A restore of those disks makes
//! claims from the VolumeSnapshots, which the driver serves from wherever its
//! data is: a surviving leg on another node included (`vm_restore.rs`).
//!
//! `VirtualMachineRestore` is `vm_restore.rs` (#53, option A of #109).

use retry::RetryExt;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use stormvm_node::console::Registration;
use stormvm_spec::snapshot::{self as spec, Phase, SnapshotResult};
use tracing::{debug, info, warn};

/// Which node took (or is taking) a snapshot.
pub const NODE_ANNOTATION: &str = "storm.io/snapshot-node";

/// The VM's disks when the snapshot was taken, as `{"<disk>": "<volume id>"}`
/// (#53): what a restore matches the group's members to. Written with the
/// claim, from the registration, which is gone once the VM stops.
pub const DISKS_ANNOTATION: &str = "storm.io/snapshot-disks";

/// The disks on other drivers' claims (#157), as
/// `{"<disk>": {"volumeSnapshot": "<name>", "claim": {name, storageClassName,
/// accessModes, volumeMode, storage}}}`: what was taken, and what a restore
/// makes a claim like. Written with the claim of the snapshot.
pub const VOLUME_SNAPSHOTS_ANNOTATION: &str = "storm.io/snapshot-volumesnapshots";

/// How long the guest is held for a driver to cut its VolumeSnapshots.
const CUT_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Is this registered disk on another driver's claim (#157)? Published by
/// that driver for the VMI (`csi_volumes::vm_holder`), with no engine volume.
pub fn is_csi_disk(d: &stormvm_node::console::RegisteredDisk) -> bool {
    d.volume_id.is_none() && d.device.contains("/volumes/kubernetes.io~csi/")
}

/// The VolumeSnapshot of disk `disk` for snapshot `snap` (KubeVirt's name).
pub fn volume_snapshot_name(snap: &Value, disk: &str) -> String {
    format!("vmsnapshot-{}-volume-{disk}", snap["metadata"]["uid"].as_str().unwrap_or(""))
}

/// The VolumeSnapshot object for one recorded disk: of its claim, owned by
/// the VirtualMachineSnapshot (deleting it deletes them), the driver's
/// default VolumeSnapshotClass.
pub fn volume_snapshot(snap: &Value, entry: &Value) -> Value {
    let (ns, name) = ns_name(snap);
    json!({
        "apiVersion": "snapshot.storage.k8s.io/v1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": entry["volumeSnapshot"],
            "namespace": ns,
            "labels": { "snapshot.kubevirt.io/source-vm-name": snap["spec"]["source"]["name"] },
            "ownerReferences": [{
                "apiVersion": API, "kind": "VirtualMachineSnapshot", "name": name,
                "uid": snap["metadata"]["uid"], "controller": true, "blockOwnerDeletion": false,
            }],
        },
        "spec": { "source": { "persistentVolumeClaimName": entry["claim"]["name"] } },
    })
}

/// The disk map of a registration: every disk with a volume behind it.
pub fn disk_map(reg: &Registration) -> Value {
    Value::Object(
        reg.disks
            .iter()
            .filter_map(|d| Some((d.name.clone(), json!(d.volume_id.clone()?))))
            .collect(),
    )
}

/// KubeVirt's default `failureDeadline`.
const DEFAULT_DEADLINE_SECS: u64 = 300;

const API: &str = "snapshot.kubevirt.io/v1beta1";

/// What a take came to, as the status needs it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Taken {
    /// stormblock's group-snapshot id, when the disks were taken.
    pub group: Option<String>,
    /// The VolumeSnapshots of disks on other drivers' claims (#157).
    pub volume_snapshots: Vec<String>,
    pub indications: Vec<String>,
    /// Every problem, including a guest left paused or frozen. `None` is a
    /// success.
    pub error: Option<String>,
}

impl Taken {
    fn failed(why: impl Into<String>) -> Taken {
        Taken { error: Some(why.into()), ..Default::default() }
    }
}

/// Takes a registered VM's snapshot under a name, with the VolumeSnapshots
/// to make in the same hold (#157). A seam for the tests: the real one needs
/// a hypervisor and a stormblock.
pub type TakeFn =
    Arc<dyn Fn(Registration, String, Vec<Value>) -> Pin<Box<dyn Future<Output = Taken> + Send>> + Send + Sync>;

/// The real take, against this node's stormblock, and the apiserver for the
/// VolumeSnapshots.
pub fn stormvm_take(stormblock: String, api: reqwest::Client, api_url: String) -> TakeFn {
    Arc::new(move |reg: Registration, name: String, volume_snapshots: Vec<Value>| {
        let stormblock = stormblock.clone();
        let (api, api_url) = (api.clone(), api_url.clone());
        Box::pin(async move {
            if !volume_snapshots.is_empty() {
                return take_with_volume_snapshots(&reg, &stormblock, &name, volume_snapshots, &api, &api_url).await;
            }
            // Taken again while stormblock is away (#211): stormvm's take is
            // idempotent by name, and only a take whose disks were not taken
            // for a transport reason is repeated (a refusal, or a guest left
            // paused or frozen, is reported as it is).
            let what = format!("snapshot {name}");
            let (reg, stormblock, name) = (&reg, &stormblock, &name);
            let taken = retry::with_backoff(retry::Policy::ENGINE.named("snapshot"), &what, |_| retry::Class::Infra, || async move {
                let opts = stormvm_control::snapshot::Options::default();
                let out = stormvm_console::snapshot::take(reg, stormblock, name, opts).await;
                let taken = Taken {
                    group: out.disks.as_ref().ok().map(|g| g.id.clone()),
                    volume_snapshots: Vec::new(),
                    indications: out.indications.iter().map(|s| s.to_string()).collect(),
                    error: out.error(),
                };
                match (&out.disks, &taken.error) {
                    (Err(_), Some(e)) if retry::class_of_message(e) == retry::Class::Infra => Err(Unreached(taken)),
                    _ => Ok(taken),
                }
            })
            .await;
            taken.unwrap_or_else(|Unreached(t)| t)
        })
    })
}

/// A take with disks on other drivers' claims (#157): the guest frozen and
/// paused once (stormvm's hold), then this node's group of its stormblock
/// volumes, if it has any, and each VolumeSnapshot made and cut.
async fn take_with_volume_snapshots(
    reg: &Registration,
    stormblock: &str,
    name: &str,
    volume_snapshots: Vec<Value>,
    api: &reqwest::Client,
    api_url: &str,
) -> Taken {
    let machine = stormvm_control::Machine {
        kind: stormvm_control::Kind::parse(&reg.vmm),
        control: reg.control_socket.clone(),
        agent: reg.agent_socket.clone(),
    };
    let volumes = stormvm_console::snapshot::volumes(reg);
    let group_name = stormvm_console::snapshot::group_name(reg, name);
    let base = stormblock.to_string();
    let opts = stormvm_control::snapshot::Options::default();
    let out = stormvm_control::snapshot::take(&machine, opts, move || async move {
        let group = if volumes.is_empty() {
            None
        } else {
            let g = tokio::task::spawn_blocking(move || {
                stormvm_block::Client::new(base).group_snapshot(&group_name, &volumes).map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| format!("the snapshot call did not finish: {e}"))??;
            Some(g.id)
        };
        let mut made = Vec::new();
        for vs in &volume_snapshots {
            made.push(cut(api, api_url, vs).await?);
        }
        Ok((group, made))
    })
    .await;
    let (group, volume_snapshots) = out.disks.as_ref().ok().cloned().unwrap_or_default();
    Taken {
        group,
        volume_snapshots,
        indications: out.indications.iter().map(|s| s.to_string()).collect(),
        error: out.error(),
    }
}

/// Make one VolumeSnapshot (an existing one of the name is this take's,
/// asked again after a restart) and wait for its driver to cut it.
async fn cut(api: &reqwest::Client, api_url: &str, vs: &Value) -> Result<String, String> {
    let (ns, name) = ns_name(vs);
    let base = format!("{api_url}/apis/snapshot.storage.k8s.io/v1/namespaces/{ns}/volumesnapshots");
    let r = api
        .post(&base)
        .json(vs)
        .send_repeatable(retry::Policy::API)
        .await
        .map_err(|e| format!("VolumeSnapshot {name}: {e}"))?;
    if !r.status().is_success() && r.status().as_u16() != 409 {
        return Err(format!("VolumeSnapshot {name} not made: {}", r.status()));
    }
    let started = std::time::Instant::now();
    loop {
        if let Ok(r) = api.get(format!("{base}/{name}")).send_retrying(retry::Policy::API).await {
            if let Ok(v) = r.json::<Value>().await {
                if let Some(e) = v["status"]["error"]["message"].as_str() {
                    return Err(format!("VolumeSnapshot {name}: {e}"));
                }
                if !v["status"]["creationTime"].is_null() || v["status"]["readyToUse"].as_bool() == Some(true) {
                    return Ok(name.to_string());
                }
            }
        }
        if started.elapsed() >= CUT_WAIT {
            return Err(format!(
                "VolumeSnapshot {name} was not cut within {}s: does its driver take snapshots (csi-snapshotter, \
                 CreateSnapshot)?",
                CUT_WAIT.as_secs()
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// A take that failed before its disks were taken, for a transport reason.
struct Unreached(Taken);

impl std::fmt::Display for Unreached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.error.as_deref().unwrap_or("not taken"))
    }
}

/// What to do with one snapshot object this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    /// Not this node's, finished, or already being taken.
    Skip,
    /// Unclaimed and its VM is here: claim it, then take it.
    Claim,
    /// Claimed by this node, `InProgress`, and not in flight: take it again.
    Resume,
    /// This node's, and it cannot be taken: `Failed`, with why.
    Fail(String),
}

/// The VM a snapshot names, read loosely: a malformed object whose VM is here
/// is still this node's to fail, with every problem stormvm finds.
fn source_vm(obj: &Value) -> &str {
    obj["spec"]["source"]["name"].as_str().unwrap_or("")
}

fn decide(
    obj: &Value,
    node: &str,
    registered: bool,
    in_flight: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Action {
    match obj["status"]["phase"].as_str() {
        Some("Succeeded") | Some("Failed") => return Action::Skip,
        _ => {}
    }
    if in_flight {
        return Action::Skip;
    }
    let owner = obj["metadata"]["annotations"][NODE_ANNOTATION].as_str();
    let mine = match owner {
        Some(n) => n == node,
        None => registered,
    };
    if !mine {
        return Action::Skip;
    }
    let req = match spec::snapshot_request(obj) {
        Ok(r) => r,
        Err(rejected) => return Action::Fail(rejected.to_string()),
    };
    let deadline = req.failure_deadline_secs.unwrap_or(DEFAULT_DEADLINE_SECS);
    let created = obj["metadata"]["creationTimestamp"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
    if let Some(created) = created {
        if (now - created.with_timezone(&chrono::Utc)).num_seconds() > deadline as i64 {
            return Action::Fail(format!("not taken within its failureDeadline ({deadline}s)"));
        }
    }
    if !registered {
        return Action::Fail(format!("{} is no longer running on {node}", req.vm));
    }
    if owner.is_some() {
        Action::Resume
    } else {
        Action::Claim
    }
}

pub struct Snapshots {
    api: reqwest::Client,
    api_url: String,
    node: String,
    run_dir: String,
    take: TakeFn,
    events: Option<crate::events::EventRecorder>,
    /// `<ns>/<name>` of the snapshots being taken now.
    in_flight: Mutex<HashSet<String>>,
    /// Said once: a cluster without the CRDs is ordinary (stormcos#170).
    absent_said: std::sync::atomic::AtomicBool,
    /// Told when a take finishes (#101): its status write is an API event
    /// too, but one that failed is not, and the next pass must still run.
    completed: Arc<tokio::sync::Notify>,
    /// Makes restored volumes (#53). `None`: restores are not served.
    pub(crate) restore_engine: Option<Arc<dyn crate::vm_restore::RestoreEngine>>,
    /// Restores already written complete or failing the same way, so an
    /// unchanged one is not written again.
    pub(crate) restore_said: Mutex<std::collections::HashMap<String, String>>,
}

impl Snapshots {
    pub fn new(
        api: reqwest::Client,
        api_url: &str,
        node: &str,
        run_dir: &str,
        take: TakeFn,
    ) -> Snapshots {
        let api_url = api_url.trim_end_matches('/').to_string();
        let events = (!api_url.is_empty())
            .then(|| crate::events::EventRecorder::new(api.clone(), &api_url, node));
        Snapshots {
            api,
            api_url,
            node: node.to_string(),
            run_dir: run_dir.to_string(),
            take,
            events,
            in_flight: Mutex::new(HashSet::new()),
            absent_said: std::sync::atomic::AtomicBool::new(false),
            completed: Arc::new(tokio::sync::Notify::new()),
            restore_engine: None,
            restore_said: Mutex::new(Default::default()),
        }
    }

    /// Serve `VirtualMachineRestore` with this engine (#53).
    pub fn with_restore_engine(mut self, engine: Arc<dyn crate::vm_restore::RestoreEngine>) -> Snapshots {
        self.restore_engine = Some(engine);
        self
    }

    pub(crate) fn api(&self) -> (&reqwest::Client, &str, &str) {
        (&self.api, &self.api_url, &self.node)
    }

    pub(crate) async fn object_event(&self, kind: &str, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event(API, kind, obj, etype, reason, message).await;
        }
    }

    /// Notified each time a take finishes (#101).
    pub fn completions(&self) -> Arc<tokio::sync::Notify> {
        self.completed.clone()
    }

    /// One pass over every snapshot in the cluster.
    ///
    /// A take runs in the background: the guest is held only for one
    /// stormblock call, but freezing waits on the guest agent, and the tick
    /// that syncs every pod on the node must not wait with it.
    pub async fn sync(self: &Arc<Self>) {
        // Restores first: they read snapshots as they stand, and a pass with
        // no snapshot CRD has no restores either.
        self.restores().await;
        let Some(items) = self.list().await else { return };
        let now = chrono::Utc::now();
        for obj in items {
            let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
            let name = obj["metadata"]["name"].as_str().unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            let vm = source_vm(&obj);
            let reg = (!vm.is_empty())
                .then(|| stormvm_node::console::find(&self.run_dir, &ns, vm))
                .flatten();
            let key = format!("{ns}/{name}");
            let in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).contains(&key);
            match decide(&obj, &self.node, reg.is_some(), in_flight, now) {
                Action::Skip => {}
                Action::Fail(why) => {
                    self.finish(&obj, String::new(), Taken::failed(why)).await;
                }
                Action::Claim => {
                    let reg = reg.expect("registered");
                    // Its disks on other drivers' claims (#157), read now: a
                    // claim that cannot be read is a retry, not a snapshot
                    // that leaves the disk out.
                    let csi = match self.csi_disks(&obj, &reg).await {
                        Ok(c) => c,
                        Err(why) => {
                            debug!("{key}: not claimed yet: {why}");
                            apimachinery::reactor::failed();
                            continue;
                        }
                    };
                    let disks = disk_map(&reg);
                    if self.claim(&obj, &disks, &csi).await {
                        let mut obj = obj;
                        obj["metadata"]["annotations"][VOLUME_SNAPSHOTS_ANNOTATION] = json!(csi.to_string());
                        self.start(obj, reg, key);
                    }
                }
                Action::Resume => self.start(obj, reg.expect("registered"), key),
            }
        }
    }

    /// Every snapshot, or `None` when there is no answer (or no CRD).
    async fn list(&self) -> Option<Vec<Value>> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!("{}/apis/{API}/virtualmachinesnapshots", self.api_url);
        let r = match self.api.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) => r,
            Err(e) => {
                debug!("virtualmachinesnapshots not listed: {e}");
                apimachinery::reactor::failed();
                return None;
            }
        };
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            if !self.absent_said.swap(true, std::sync::atomic::Ordering::Relaxed) {
                info!("no {API} virtualmachinesnapshots on this cluster: snapshots are not served");
            }
            return None;
        }
        if !r.status().is_success() {
            debug!("virtualmachinesnapshots not listed: {}", r.status());
            apimachinery::reactor::failed();
            return None;
        }
        let list: Value = r.json().await.ok()?;
        Some(list["items"].as_array().cloned().unwrap_or_default())
    }

    /// Mark the snapshot as this node's, against the version read. A node
    /// that loses the race gets a conflict and leaves it.
    async fn claim(&self, obj: &Value, disks: &Value, csi: &Value) -> bool {
        let (ns, name) = ns_name(obj);
        let mut meta = json!({ "annotations": {
            NODE_ANNOTATION: self.node,
            DISKS_ANNOTATION: disks.to_string(),
        } });
        if csi.as_object().is_some_and(|m| !m.is_empty()) {
            meta["annotations"][VOLUME_SNAPSHOTS_ANNOTATION] = json!(csi.to_string());
        }
        if let Some(rv) = obj["metadata"]["resourceVersion"].as_str() {
            meta["resourceVersion"] = json!(rv);
        }
        let url = format!("{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}", self.api_url);
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "metadata": meta }))
            .send_retrying(retry::Policy::API)
            .await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                debug!("{ns}/{name}: snapshot not claimed: {}", r.status());
                false
            }
            Err(e) => {
                debug!("{ns}/{name}: snapshot not claimed: {e}");
                // Not an answer: no event will follow, so the pass retries.
                apimachinery::reactor::failed();
                false
            }
        }
    }

    fn start(self: &Arc<Self>, obj: Value, reg: Registration, key: String) {
        self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone());
        let me = self.clone();
        tokio::spawn(async move {
            let source_uid = me.source_uid(&obj, &reg).await;
            me.write_status(
                &obj,
                &SnapshotResult {
                    phase: Some(Phase::InProgress),
                    time: now_rfc3339(),
                    source_uid: source_uid.clone(),
                    ..Default::default()
                },
            )
            .await;
            let (ns, name) = ns_name(&obj);
            info!("{ns}/{name}: taking a snapshot of {}", reg.name);
            // The VolumeSnapshots recorded at the claim (#157), also after a
            // restart: the same names, so a second take finds the first's.
            let recorded: serde_json::Map<String, Value> = obj["metadata"]["annotations"][VOLUME_SNAPSHOTS_ANNOTATION]
                .as_str()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            let volume_snapshots = recorded.values().map(|e| volume_snapshot(&obj, e)).collect();
            let taken = (me.take)(reg, name.to_string(), volume_snapshots).await;
            me.finish(&obj, source_uid, taken).await;
            me.in_flight.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
            me.completed.notify_one();
        });
    }

    /// The VMI's disks on other drivers' claims (#157), each with its
    /// VolumeSnapshot's name and what its claim was: `{}` when it has none.
    /// `Err` when the VMI or a claim cannot be read.
    async fn csi_disks(&self, obj: &Value, reg: &Registration) -> Result<Value, String> {
        let disks: Vec<&str> = reg.disks.iter().filter(|d| is_csi_disk(d)).map(|d| d.name.as_str()).collect();
        let mut out = serde_json::Map::new();
        if disks.is_empty() {
            return Ok(Value::Object(out));
        }
        let ns = reg.namespace.as_str();
        let vmi = self
            .get(&format!("/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{}", reg.name))
            .await?;
        for disk in disks {
            let vol = vmi["spec"]["volumes"]
                .as_array()
                .and_then(|v| v.iter().find(|v| v["name"] == disk))
                .ok_or_else(|| format!("disk {disk} is not among VMI {}'s volumes", reg.name))?;
            let claim = vol["persistentVolumeClaim"]["claimName"]
                .as_str()
                .or_else(|| vol["dataVolume"]["name"].as_str())
                .ok_or_else(|| format!("disk {disk} is not a claim"))?;
            let pvc = self.get(&format!("/api/v1/namespaces/{ns}/persistentvolumeclaims/{claim}")).await?;
            let storage = pvc["status"]["capacity"]["storage"]
                .as_str()
                .or_else(|| pvc["spec"]["resources"]["requests"]["storage"].as_str())
                .unwrap_or("");
            out.insert(
                disk.to_string(),
                json!({
                    "volumeSnapshot": volume_snapshot_name(obj, disk),
                    "claim": {
                        "name": claim,
                        "storageClassName": pvc["spec"]["storageClassName"],
                        "accessModes": pvc["spec"]["accessModes"],
                        "volumeMode": pvc["spec"]["volumeMode"],
                        "storage": storage,
                    },
                }),
            );
        }
        Ok(Value::Object(out))
    }

    async fn get(&self, path: &str) -> Result<Value, String> {
        let r = self
            .api
            .get(format!("{}{path}", self.api_url))
            .send_retrying(retry::Policy::API)
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("{path}: {}", r.status()));
        }
        r.json().await.map_err(|e| format!("{path}: {e}"))
    }

    /// The uid of what was snapshotted: the VirtualMachine's when the source
    /// is one, else the VMI's (its registration has it).
    async fn source_uid(&self, obj: &Value, reg: &Registration) -> String {
        if obj["spec"]["source"]["kind"].as_str() != Some("VirtualMachine") {
            return reg.uid.clone();
        }
        let url = format!(
            "{}/apis/kubevirt.io/v1/namespaces/{}/virtualmachines/{}",
            self.api_url, reg.namespace, reg.name
        );
        let vm: Option<Value> = match self.api.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        };
        vm.and_then(|v| v["metadata"]["uid"].as_str().map(String::from)).unwrap_or_default()
    }

    async fn finish(&self, obj: &Value, source_uid: String, taken: Taken) {
        let (ns, name) = ns_name(obj);
        let ok = taken.error.is_none();
        let result = SnapshotResult {
            phase: Some(if ok { Phase::Succeeded } else { Phase::Failed }),
            time: now_rfc3339(),
            source_uid,
            group_snapshot: taken.group.clone(),
            indications: taken.indications,
            error: taken.error.clone(),
        };
        self.write_status(obj, &result).await;
        match &taken.error {
            None => {
                let mut what = Vec::new();
                if let Some(g) = &taken.group {
                    what.push(format!("group snapshot {g}"));
                }
                if !taken.volume_snapshots.is_empty() {
                    what.push(format!("VolumeSnapshots {}", taken.volume_snapshots.join(", ")));
                }
                let what = what.join("; ");
                info!("{ns}/{name}: snapshot taken ({what})");
                self.event(obj, "Normal", "SnapshotSucceeded", &what).await;
            }
            Some(e) => {
                warn!("{ns}/{name}: snapshot failed: {e}");
                self.event(obj, "Warning", "SnapshotFailed", e).await;
            }
        }
    }

    async fn write_status(&self, obj: &Value, r: &SnapshotResult) {
        let (ns, name) = ns_name(obj);
        let url = format!(
            "{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}/status",
            self.api_url
        );
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "status": spec::snapshot_status(r) }))
            .send_retrying(retry::Policy::API)
            .await
        {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => warn!("{ns}/{name}: snapshot status not written: {}", resp.status()),
            Err(e) => warn!("{ns}/{name}: snapshot status not written: {e}"),
        }
    }

    async fn event(&self, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event(API, "VirtualMachineSnapshot", obj, etype, reason, message).await;
        }
    }
}

fn ns_name(obj: &Value) -> (&str, &str) {
    (
        obj["metadata"]["namespace"].as_str().unwrap_or("default"),
        obj["metadata"]["name"].as_str().unwrap_or(""),
    )
}

fn now_rfc3339() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Path;
    use axum::routing::{get, patch};

    fn snap(name: &str, vm: &str) -> Value {
        json!({
            "apiVersion": API,
            "kind": "VirtualMachineSnapshot",
            "metadata": {
                "name": name, "namespace": "web", "uid": format!("u-{name}"),
                "resourceVersion": "7",
                "creationTimestamp": now_rfc3339(),
            },
            "spec": { "source": { "apiGroup": "kubevirt.io", "kind": "VirtualMachine", "name": vm } },
        })
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    #[test]
    fn an_unclaimed_snapshot_of_a_vm_here_is_claimed() {
        assert_eq!(decide(&snap("s", "web-1"), "n1", true, false, now()), Action::Claim);
    }

    #[test]
    fn a_vm_elsewhere_is_not_this_nodes_business() {
        assert_eq!(decide(&snap("s", "web-1"), "n1", false, false, now()), Action::Skip);
        let mut o = snap("s", "web-1");
        o["metadata"]["annotations"] = json!({ NODE_ANNOTATION: "n2" });
        o["status"] = json!({ "phase": "InProgress" });
        assert_eq!(decide(&o, "n1", true, false, now()), Action::Skip);
    }

    #[test]
    fn a_finished_or_running_take_is_left_alone() {
        for phase in ["Succeeded", "Failed"] {
            let mut o = snap("s", "web-1");
            o["status"] = json!({ "phase": phase });
            assert_eq!(decide(&o, "n1", true, false, now()), Action::Skip, "{phase}");
        }
        assert_eq!(decide(&snap("s", "web-1"), "n1", true, true, now()), Action::Skip);
    }

    #[test]
    fn a_claimed_take_that_a_restart_interrupted_is_taken_again() {
        let mut o = snap("s", "web-1");
        o["metadata"]["annotations"] = json!({ NODE_ANNOTATION: "n1" });
        o["status"] = json!({ "phase": "InProgress" });
        assert_eq!(decide(&o, "n1", true, false, now()), Action::Resume);
        // Its VM has gone from here meanwhile: nobody else can take it.
        match decide(&o, "n1", false, false, now()) {
            Action::Fail(why) => assert!(why.contains("no longer running on n1"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn past_its_failure_deadline_it_fails() {
        let mut o = snap("s", "web-1");
        o["spec"]["failureDeadline"] = json!("1m");
        let later = now() + chrono::Duration::seconds(61);
        match decide(&o, "n1", true, false, later) {
            Action::Fail(why) => assert!(why.contains("failureDeadline (60s)"), "{why}"),
            other => panic!("{other:?}"),
        }
        // The default is five minutes.
        let o = snap("s", "web-1");
        assert_eq!(decide(&o, "n1", true, false, now() + chrono::Duration::seconds(299)), Action::Claim);
        assert!(matches!(decide(&o, "n1", true, false, now() + chrono::Duration::seconds(301)), Action::Fail(_)));
    }

    #[test]
    fn a_malformed_snapshot_of_a_vm_here_fails_with_every_problem() {
        let mut o = snap("s", "web-1");
        o["spec"]["failureDeadline"] = json!("soon");
        match decide(&o, "n1", true, false, now()) {
            Action::Fail(why) => assert!(why.contains("failureDeadline"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    type Log = Arc<Mutex<Vec<(String, Value)>>>;

    /// An apiserver holding snapshots, recording every write, with one
    /// VirtualMachine (`web/web-1`, uid `vm-uid`).
    async fn apiserver(items: Vec<Value>, claim_status: u16) -> (String, Log) {
        let log: Log = Arc::default();
        let (l1, l2, l3) = (log.clone(), log.clone(), log.clone());
        let app = axum::Router::new()
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/virtualmachinesnapshots",
                get(move || {
                    let items = items.clone();
                    async move { axum::Json(json!({ "items": items })) }
                }),
            )
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/namespaces/{ns}/virtualmachinesnapshots/{name}",
                patch(move |Path((_, name)): Path<(String, String)>, axum::Json(b): axum::Json<Value>| {
                    let l = l1.clone();
                    async move {
                        l.lock().unwrap().push((format!("claim {name}"), b));
                        (axum::http::StatusCode::from_u16(claim_status).unwrap(), axum::Json(json!({})))
                    }
                }),
            )
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/namespaces/{ns}/virtualmachinesnapshots/{name}/status",
                patch(move |Path((_, name)): Path<(String, String)>, axum::Json(b): axum::Json<Value>| {
                    let l = l2.clone();
                    async move {
                        l.lock().unwrap().push((format!("status {name}"), b));
                        axum::Json(json!({}))
                    }
                }),
            )
            .route(
                "/apis/kubevirt.io/v1/namespaces/web/virtualmachines/web-1",
                get(|| async { axum::Json(json!({ "metadata": { "uid": "vm-uid" } })) }),
            )
            .route(
                "/apis/kubevirt.io/v1/namespaces/web/virtualmachineinstances/web-1",
                get(|| async { axum::Json(json!({ "spec": { "volumes": [
                    { "name": "rootdisk", "dataVolume": { "name": "fedora" } },
                    { "name": "data", "persistentVolumeClaim": { "claimName": "data-claim" } },
                ] } })) }),
            )
            .route(
                "/api/v1/namespaces/web/persistentvolumeclaims/data-claim",
                get(|| async { axum::Json(json!({
                    "spec": { "storageClassName": "stormblock-csi", "accessModes": ["ReadWriteOnce"], "volumeMode": "Block",
                              "resources": { "requests": { "storage": "16Gi" } } },
                    "status": { "capacity": { "storage": "20Gi" } } })) }),
            )
            .route(
                "/api/v1/namespaces/web/events",
                axum::routing::post(move |axum::Json(b): axum::Json<Value>| {
                    let l = l3.clone();
                    async move {
                        l.lock().unwrap().push(("event".into(), b));
                        axum::Json(json!({}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, log)
    }

    #[test]
    fn the_disk_map_names_every_disk_with_a_volume() {
        let reg: Registration = serde_json::from_value(json!({
            "namespace": "web", "name": "web-1", "uid": "u",
            "disks": [
                { "name": "rootdisk", "volume_id": "v-root", "owned": true, "device": "/dev/ublkb1" },
                { "name": "data", "volume_id": "v-data", "owned": false, "device": "/dev/ublkb2" },
                { "name": "scratch", "device": "/dev/x" },
            ],
        }))
        .unwrap();
        assert_eq!(disk_map(&reg), json!({ "rootdisk": "v-root", "data": "v-data" }));
    }

    fn register(run_dir: &str, ns: &str, name: &str) {
        let reg: Registration = serde_json::from_value(json!({
            "namespace": ns, "name": name, "uid": "vmi-uid",
        }))
        .unwrap();
        stormvm_node::console::write(run_dir, &reg).unwrap();
    }

    /// A take that records what it was asked and answers `taken`.
    fn fake_take(taken: Taken, asked: Arc<Mutex<Vec<(String, String)>>>) -> TakeFn {
        Arc::new(move |reg: Registration, name: String, _vs: Vec<Value>| {
            asked.lock().unwrap().push((format!("{}/{}", reg.namespace, reg.name), name));
            let t = taken.clone();
            Box::pin(async move { t })
        })
    }

    async fn settled(log: &Log, writes: usize) -> Vec<(String, Value)> {
        for _ in 0..200 {
            if log.lock().unwrap().len() >= writes {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        log.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_snapshot_of_a_vm_here_is_claimed_taken_and_written_succeeded() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) =
            apiserver(vec![snap("before", "web-1"), snap("other", "db-1")], 200).await;
        let asked = Arc::default();
        let taken = Taken {
            group: Some("gs-1".into()),
            indications: vec!["Online".into(), "GuestAgent".into()],
            ..Default::default()
        };
        let s = Arc::new(Snapshots::new(
            reqwest::Client::new(), &url, "n1", run_dir, fake_take(taken, Arc::clone(&asked)),
        ));
        s.sync().await;
        // claim, InProgress, Succeeded, event.
        let log = settled(&log, 4).await;

        assert_eq!(*asked.lock().unwrap(), [("web/web-1".to_string(), "before".to_string())]);
        assert_eq!(log[0].0, "claim before");
        assert_eq!(log[0].1["metadata"]["annotations"][NODE_ANNOTATION], "n1");
        assert_eq!(log[0].1["metadata"]["resourceVersion"], "7");
        assert_eq!(log[1].0, "status before");
        assert_eq!(log[1].1["status"]["phase"], "InProgress");
        let done = &log.iter().find(|(k, b)| k == "status before" && b["status"]["phase"] != "InProgress").unwrap().1;
        assert_eq!(done["status"]["phase"], "Succeeded");
        assert_eq!(done["status"]["readyToUse"], true);
        assert_eq!(done["status"]["virtualMachineSnapshotContentName"], "gs-1");
        assert_eq!(done["status"]["sourceUID"], "vm-uid");
        assert_eq!(done["status"]["indications"], json!(["Online", "GuestAgent"]));
        let ev = &log.iter().find(|(k, _)| k == "event").unwrap().1;
        assert_eq!(ev["involvedObject"]["kind"], "VirtualMachineSnapshot");
        assert_eq!(ev["reason"], "SnapshotSucceeded");
        // `db-1` is not here: nothing was written about `other`.
        assert!(log.iter().all(|(k, _)| !k.ends_with("other")), "{log:?}");
    }

    #[tokio::test]
    async fn a_failed_take_is_written_failed_with_why() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) = apiserver(vec![snap("before", "web-1")], 200).await;
        let taken = Taken {
            indications: vec!["Online".into()],
            error: Some("404 volume vol-1".into()),
            ..Default::default()
        };
        let s = Arc::new(Snapshots::new(reqwest::Client::new(), &url, "n1", run_dir, fake_take(taken, Arc::default())));
        s.sync().await;
        let log = settled(&log, 4).await;
        let done = &log.iter().find(|(k, b)| k == "status before" && b["status"]["phase"] == "Failed").unwrap().1;
        assert_eq!(done["status"]["readyToUse"], false);
        assert!(done["status"]["error"]["message"].as_str().unwrap().contains("404 volume vol-1"));
        let ev = &log.iter().find(|(k, _)| k == "event").unwrap().1;
        assert_eq!(ev["type"], "Warning");
    }

    /// #157: a disk on another driver's claim is recorded at the claim (its
    /// VolumeSnapshot's name, and the claim it is of) and handed to the take
    /// as a VolumeSnapshot owned by the VirtualMachineSnapshot.
    #[tokio::test]
    async fn a_disk_on_another_drivers_claim_is_taken_as_a_volume_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        let reg: Registration = serde_json::from_value(json!({
            "namespace": "web", "name": "web-1", "uid": "vmi-uid",
            "disks": [
                { "name": "rootdisk", "volume_id": "v-root", "owned": true, "device": "/dev/ublkb1" },
                { "name": "data", "device": "/var/lib/kubelet/pods/vm-vmi-uid/volumes/kubernetes.io~csi/data/mount" },
            ],
        }))
        .unwrap();
        assert!(!is_csi_disk(&reg.disks[0]) && is_csi_disk(&reg.disks[1]));
        stormvm_node::console::write(run_dir, &reg).unwrap();
        let (url, log) = apiserver(vec![snap("before", "web-1")], 200).await;
        let given: Arc<Mutex<Vec<Value>>> = Arc::default();
        let g = given.clone();
        let take: TakeFn = Arc::new(move |_reg: Registration, _name: String, vs: Vec<Value>| {
            g.lock().unwrap().extend(vs);
            Box::pin(async {
                Taken { group: Some("gs-1".into()), volume_snapshots: vec!["vmsnapshot-u-before-volume-data".into()], ..Default::default() }
            })
        });
        let s = Arc::new(Snapshots::new(reqwest::Client::new(), &url, "n1", run_dir, take));
        s.sync().await;
        let log = settled(&log, 4).await;

        let claim = &log.iter().find(|(k, _)| k == "claim before").unwrap().1;
        assert_eq!(claim["metadata"]["annotations"][DISKS_ANNOTATION], json!(r#"{"rootdisk":"v-root"}"#));
        let csi: Value = serde_json::from_str(claim["metadata"]["annotations"][VOLUME_SNAPSHOTS_ANNOTATION].as_str().unwrap()).unwrap();
        assert_eq!(csi["data"]["volumeSnapshot"], "vmsnapshot-u-before-volume-data");
        assert_eq!(csi["data"]["claim"], json!({ "name": "data-claim", "storageClassName": "stormblock-csi",
            "accessModes": ["ReadWriteOnce"], "volumeMode": "Block", "storage": "20Gi" }));
        let vs = given.lock().unwrap().clone();
        assert_eq!(vs.len(), 1);
        assert_eq!(vs[0]["metadata"]["name"], "vmsnapshot-u-before-volume-data");
        assert_eq!(vs[0]["metadata"]["namespace"], "web");
        assert_eq!(vs[0]["spec"]["source"]["persistentVolumeClaimName"], "data-claim");
        assert_eq!(vs[0]["metadata"]["ownerReferences"][0]["uid"], "u-before");
        let ev = &log.iter().find(|(k, _)| k == "event").unwrap().1;
        assert!(ev["message"].as_str().unwrap().contains("VolumeSnapshots vmsnapshot-u-before-volume-data"), "{ev}");
    }

    /// A VolumeSnapshot is made (an existing one is this take's) and waited
    /// for until its driver cut it; the driver's error is the take's.
    #[tokio::test]
    async fn a_volume_snapshot_is_made_and_waited_for_until_cut() {
        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let g = gets.clone();
        let app = axum::Router::new()
            .route("/apis/snapshot.storage.k8s.io/v1/namespaces/web/volumesnapshots",
                   axum::routing::post(|| async { (axum::http::StatusCode::CONFLICT, axum::Json(json!({}))) }))
            .route("/apis/snapshot.storage.k8s.io/v1/namespaces/web/volumesnapshots/{name}",
                   get(move |Path(name): Path<String>| {
                       let n = g.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                       async move {
                           axum::Json(match name.as_str() {
                               "ok" if n == 0 => json!({ "status": { "readyToUse": false } }),
                               "ok" => json!({ "status": { "creationTime": "2026-10-10T20:00:00Z", "readyToUse": false } }),
                               _ => json!({ "status": { "error": { "message": "driver does not support snapshots" } } }),
                           })
                       }
                   }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let api = reqwest::Client::new();
        let vs = |n: &str| json!({ "metadata": { "name": n, "namespace": "web" } });
        assert_eq!(cut(&api, &url, &vs("ok")).await.unwrap(), "ok");
        assert!(gets.load(std::sync::atomic::Ordering::SeqCst) >= 2, "waited for the cut");
        let e = cut(&api, &url, &vs("bad")).await.unwrap_err();
        assert!(e.contains("driver does not support snapshots"), "{e}");
    }

    /// Another node claimed it first: the conflict is a loss, and nothing is
    /// taken here.
    #[tokio::test]
    async fn a_lost_claim_takes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) = apiserver(vec![snap("before", "web-1")], 409).await;
        let asked: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let s = Arc::new(Snapshots::new(
            reqwest::Client::new(), &url, "n1", run_dir, fake_take(Taken::default(), Arc::clone(&asked)),
        ));
        s.sync().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(asked.lock().unwrap().is_empty());
        assert_eq!(log.lock().unwrap().len(), 1, "only the claim");
    }

    /// No CRD: a 404 list is nothing to do, not an error.
    #[tokio::test]
    async fn no_crd_is_nothing_to_do() {
        let app = axum::Router::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let s = Snapshots::new(reqwest::Client::new(), &url, "n1", "/nonexistent", fake_take(Taken::default(), Arc::default()));
        assert!(s.list().await.is_none());
    }
}
