//! `VirtualMachineGolden` (`storm.io/v1alpha1`): a golden made from a running
//! VM (#344, vmcloud-image-operator#50, stormvm#17).
//!
//! vmimages takes a person's request, checks they may have it, and writes a
//! work order naming the VMI (`spec.vm`) and the name of the golden
//! (`spec.golden`). The kubelet of the node the VM runs on does the work,
//! because it owns the machine, its disks and its guest agent. vmimages
//! stays unprivileged.
//!
//! The steps are stormvm's (`stormvm_node::golden`, the order of its
//! `stormvm golden`):
//!
//! 1. **Snapshot** the root disk alone (frozen and paused for an instant,
//!    the #28 path). The source keeps running and is otherwise untouched.
//! 2. **Copy** it: a volume made from the snapshot, under `spec.golden`.
//! 3. **Boot the copy** as a short-lived *sealer* (`sealer_spec`: the
//!    source's machine, the copy as its only disk, no NIC, no seed, the agent
//!    on), and wait for its agent.
//! 4. **De-identify** it through the agent (`DEIDENTIFY`), and check the
//!    script got to its end (`DEIDENTIFIED`); it powers off and the copy is
//!    detached.
//! 5. **Seal** the copy, delete the group snapshot, and write `Sealed` with
//!    the volume's id, digest, firmware and provenance.
//!
//! On any failure the request is `Failed` with the reason, and the copy and
//! the group are deleted. A guest with no agent is `Failed`: a golden that
//! was not de-identified is worse than none. A request runs once: one already
//! `Sealed` or `Failed` is left alone.
//!
//! **No owner on the golden.** stormvm's CLI gives the copy its VM as owner.
//! Here that would make it the VM's disk to #75's sweep, deleted once the VM
//! goes, while the registry has adopted it and other machines boot it. So the
//! golden is owned by nobody, like any other golden.
//!
//! **A name already taken is refused.** stormblock's create is idempotent by
//! name: a copy asked for under the name of a golden already there would
//! answer *that* golden, and the sealer would boot and rewrite it.
//!
//! **A kubelet restart mid-way** finds the request `Snapshotting` or
//! `Sealing`, claimed by this node and not in flight. It is not run again
//! (once only): it is `Failed`, and what the status recorded is removed (the
//! sealer stopped, the copy by its recorded id, the group by its name).
//! A copy made in the instant before its id was written is left behind.

use async_trait::async_trait;
use retry::RetryExt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use stormvm_node::console::Registration;
use stormvm_node::golden as g;
use stormvm_spec::VmSpec;
use tracing::{debug, info, warn};

pub const API: &str = "storm.io/v1alpha1";
pub const KIND: &str = "VirtualMachineGolden";
const RESOURCE: &str = "virtualmachinegoldens";

pub const PENDING: &str = "Pending";
pub const SNAPSHOTTING: &str = "Snapshotting";
pub const SEALING: &str = "Sealing";
pub const SEALED: &str = "Sealed";
pub const FAILED: &str = "Failed";

/// The sealer's uid, as the kubelet registers it: `vmgolden-<request uid>`.
/// The VM manager leaves a registration with this prefix alone: it is no
/// VMI's machine, and stopping it as an orphan would fail the golden.
pub const SEALER_UID_PREFIX: &str = "vmgolden-";

/// How long the copy is given to boot and answer through its agent.
pub const BOOT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

pub fn is_sealer(uid: &str) -> bool {
    uid.starts_with(SEALER_UID_PREFIX)
}

pub fn sealer_uid(request: &Value) -> String {
    format!("{SEALER_UID_PREFIX}{}", request["metadata"]["uid"].as_str().unwrap_or(""))
}

/// The group snapshot's name: stormvm's (`<ns>.<vm>.golden-<golden>`), one
/// per golden, so a restarted kubelet finds it by name.
pub fn group_name(namespace: &str, vm: &str, golden: &str) -> String {
    format!("{namespace}.{vm}.golden-{golden}")
}

/// What a pass does with one request.
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Skip,
    /// Its VM runs here and nobody took it: take it.
    Claim,
    /// Refused before anything is made.
    Fail(String),
    /// Taken by this node, not running now (a kubelet restart): failed, and
    /// what it made removed.
    Abandon,
}

/// Decide a request. `source_here`: the VM it names is registered on this
/// node (a VMI's machine, not a sealer).
pub fn decide(obj: &Value, node: &str, source_here: bool, in_flight: bool) -> Action {
    if !obj["metadata"]["deletionTimestamp"].is_null() || in_flight {
        return Action::Skip;
    }
    let status = &obj["status"];
    let phase = status["phase"].as_str().unwrap_or("");
    if phase == SEALED || phase == FAILED {
        return Action::Skip;
    }
    let owner = status["node"].as_str().unwrap_or("");
    if !owner.is_empty() {
        if owner != node {
            return Action::Skip;
        }
        if phase == SNAPSHOTTING || phase == SEALING {
            return Action::Abandon;
        }
    }
    if !source_here {
        return Action::Skip;
    }
    if obj["spec"]["golden"].as_str().unwrap_or("").is_empty() {
        return Action::Fail("spec.golden is empty: the golden needs a name".into());
    }
    Action::Claim
}

/// The engine calls a golden needs. A seam for the tests.
#[async_trait]
pub trait Engine: Send + Sync {
    /// The id of the volume named `name`, if there is one.
    async fn volume_named(&self, name: &str) -> Result<Option<String>, String>;
    /// A new volume named `name` holding `snapshot`; its id.
    async fn volume_from_snapshot(&self, name: &str, snapshot: &stormvm_block::Snapshot) -> Result<String, String>;
    async fn seal(&self, id: &str) -> Result<(), String>;
    /// The sealed volume's sha256, when the engine has it.
    async fn digest(&self, id: &str) -> Option<String>;
    /// Gone already is done.
    async fn delete_volume(&self, id: &str) -> Result<(), String>;
    /// Every group snapshot named `name` deleted. None is done.
    async fn delete_groups_named(&self, name: &str) -> Result<(), String>;
    async fn delete_group(&self, id: &str) -> Result<(), String>;
}

/// The machine calls a golden needs: the VM manager's. A seam for the tests.
#[async_trait]
pub trait Machines: Send + Sync {
    /// The root disk of the registered VM, at one instant, as a group.
    async fn snapshot_root(&self, reg: &Registration, root_id: &str, group: &str)
        -> Result<stormvm_block::GroupSnapshot, String>;
    /// The sealer started, registered under `uid`.
    async fn start_sealer(&self, uid: &str, spec: &VmSpec) -> Result<Registration, String>;
    /// Wait for its agent, run [`g::DEIDENTIFY`] and check it got to its
    /// end, then power it off.
    async fn deidentify(&self, reg: &Registration, within: std::time::Duration) -> Result<(), String>;
    /// Stopped (if it still runs), its disk detached, its registration gone.
    async fn stop_sealer(&self, reg: &Registration) -> Result<(), String>;
}

/// What a run has made, for a failure to remove.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Made {
    pub group: Option<String>,
    pub copy: Option<String>,
    pub sealer: Option<Registration>,
}

pub struct Goldens {
    api: reqwest::Client,
    api_url: String,
    node: String,
    run_dir: String,
    engine: Arc<dyn Engine>,
    machines: Arc<dyn Machines>,
    events: Option<crate::events::EventRecorder>,
    /// `<ns>/<name>` of the requests running now.
    in_flight: Mutex<HashSet<String>>,
    absent_said: std::sync::atomic::AtomicBool,
    /// Told when a run ends, so the next pass sees what it wrote.
    completed: Arc<tokio::sync::Notify>,
}

impl Goldens {
    pub fn new(
        api: reqwest::Client,
        api_url: &str,
        node: &str,
        run_dir: &str,
        engine: Arc<dyn Engine>,
        machines: Arc<dyn Machines>,
    ) -> Goldens {
        let api_url = api_url.trim_end_matches('/').to_string();
        let events = (!api_url.is_empty()).then(|| crate::events::EventRecorder::new(api.clone(), &api_url, node));
        Goldens {
            api,
            api_url,
            node: node.to_string(),
            run_dir: run_dir.to_string(),
            engine,
            machines,
            events,
            in_flight: Mutex::new(HashSet::new()),
            absent_said: std::sync::atomic::AtomicBool::new(false),
            completed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn completions(&self) -> Arc<tokio::sync::Notify> {
        self.completed.clone()
    }

    /// The registration of the VM a request names, when it is a VMI's
    /// machine on this node.
    fn source(&self, obj: &Value) -> Option<Registration> {
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
        let vm = obj["spec"]["vm"].as_str().unwrap_or("");
        if vm.is_empty() {
            return None;
        }
        stormvm_node::console::find(&self.run_dir, ns, vm).filter(|r| !r.uid.is_empty() && !is_sealer(&r.uid))
    }

    pub async fn sync(self: &Arc<Self>) {
        let Some(items) = self.list().await else { return };
        for obj in items {
            let (ns, name) = ns_name(&obj);
            if name.is_empty() {
                continue;
            }
            let key = format!("{ns}/{name}");
            let in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).contains(&key);
            let reg = self.source(&obj);
            match decide(&obj, &self.node, reg.is_some(), in_flight) {
                Action::Skip => {}
                Action::Fail(why) => self.fail(&obj, &why).await,
                Action::Claim => {
                    if self.claim(&obj).await {
                        self.start(obj, reg.expect("registered"), key);
                    }
                }
                Action::Abandon => {
                    self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone());
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.abandon(&obj).await;
                        this.in_flight.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
                        this.completed.notify_one();
                    });
                }
            }
        }
    }

    fn start(self: &Arc<Self>, obj: Value, reg: Registration, key: String) {
        self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone());
        let this = self.clone();
        tokio::spawn(async move {
            this.run(&obj, &reg).await;
            this.in_flight.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
            this.completed.notify_one();
        });
    }

    /// One request, start to end: `Sealed`, or `Failed` with nothing left.
    pub async fn run(&self, obj: &Value, reg: &Registration) {
        let (ns, name) = ns_name(obj);
        let mut made = Made::default();
        let outcome = match self.vmi(obj).await {
            Ok(vmi) => self.make(obj, &vmi, reg, &mut made).await,
            Err(e) => Err(e),
        };
        match outcome {
            Ok(status) => {
                info!("{ns}/{name}: golden {} sealed ({})", obj["spec"]["golden"].as_str().unwrap_or(""),
                      status["volumeId"].as_str().unwrap_or(""));
                self.patch_status(obj, status).await;
                self.event(obj, "Normal", "Sealed", &format!(
                    "golden {} made from VM {}", obj["spec"]["golden"].as_str().unwrap_or(""),
                    obj["spec"]["vm"].as_str().unwrap_or(""))).await;
            }
            Err(why) => {
                let left = self.remove(&made).await;
                let why = if left.is_empty() { why } else { format!("{why}; not removed: {}", left.join("; ")) };
                warn!("{ns}/{name}: golden not made: {why}");
                self.fail(obj, &why).await;
            }
        }
    }

    /// The steps. What is made is recorded in `made` as it is made, so a
    /// failure at any step removes exactly that.
    pub async fn make(&self, obj: &Value, vmi: &Value, reg: &Registration, made: &mut Made) -> Result<Value, String> {
        let golden = obj["spec"]["golden"].as_str().unwrap_or("").to_string();
        let vm: VmSpec = stormvm_spec::kube::from_kube(vmi).map_err(|e| format!("reading VMI {}: {e}", reg.name))?;
        if vmi["metadata"]["uid"].as_str() != Some(reg.uid.as_str()) {
            return Err(format!("VMI {}/{} is not the machine running here (another uid)", reg.namespace, reg.name));
        }
        let root = g::root_disk(&vm).ok_or_else(|| format!("VM {} has no disk of its own to make a golden of", vm.name))?;
        let root_id = reg
            .disks
            .iter()
            .find(|d| d.name == root.name)
            .and_then(|d| d.volume_id.clone())
            .ok_or_else(|| format!("disk {} of VM {} has no volume on this node's engine", root.name, vm.name))?;
        if let Some(id) = self.engine.volume_named(&golden).await? {
            return Err(format!(
                "a volume named {golden} ({id}) is already on this node's engine: a golden is never made over another"
            ));
        }
        let sealer = g::sealer_spec(&vm, "-").ok_or_else(|| format!("no sealer for VM {}", vm.name))?;
        if let Some(other) = stormvm_node::console::find(&self.run_dir, &sealer.namespace, &sealer.name) {
            return Err(format!(
                "a machine named {}/{} already runs here (uid {}): the sealer needs that name",
                sealer.namespace, sealer.name, other.uid
            ));
        }

        // 1. The root disk, at one instant.
        let group_name = group_name(&reg.namespace, &reg.name, &golden);
        let group = self.machines.snapshot_root(reg, &root_id, &group_name).await
            .map_err(|e| format!("snapshotting {}'s root disk {}: {e}", vm.name, root.name))?;
        made.group = Some(group.id.clone());
        let snap = group.of(&root_id).cloned()
            .ok_or_else(|| format!("the snapshot {} has no member for {root_id}", group.id))?;

        // 2. The copy, under the golden's name.
        let copy = self.engine.volume_from_snapshot(&golden, &snap).await
            .map_err(|e| format!("copying {}'s root disk as {golden}: {e}", vm.name))?;
        made.copy = Some(copy.clone());
        self.patch_status(obj, json!({
            "phase": SEALING,
            "node": self.node,
            "volumeId": copy,
            "message": format!("booting the copy of {}'s root disk as a sealer", vm.name),
        })).await;

        // 3. The sealer: the copy, booted once, with nothing to reach.
        let sealer = g::sealer_spec(&vm, &copy).ok_or_else(|| format!("no sealer for VM {}", vm.name))?;
        let sreg = self.machines.start_sealer(&sealer_uid(obj), &sealer).await
            .map_err(|e| format!("starting the sealer: {e}"))?;
        made.sealer = Some(sreg.clone());

        // 4. De-identified by its own agent, then stopped and let go.
        self.machines.deidentify(&sreg, BOOT_TIMEOUT).await?;
        self.machines.stop_sealer(&sreg).await.map_err(|e| format!("stopping the sealer: {e}"))?;
        made.sealer = None;

        // 5. Sealed. From here it is a golden: nothing below removes it.
        self.engine.seal(&copy).await.map_err(|e| format!("sealing {golden}: {e}"))?;
        made.copy = None;
        if let Err(e) = self.engine.delete_group(&group.id).await {
            warn!("golden {golden}: the snapshot group {} was not deleted: {e}", group.id);
        }
        made.group = None;
        let sha256 = self.engine.digest(&copy).await;
        let now = chrono::Utc::now();
        let firmware = match vm.firmware.kind {
            stormvm_spec::FirmwareType::Bios => "bios",
            _ => "uefi",
        };
        let mut status = json!({
            "phase": SEALED,
            "message": format!("sealed from VM {}'s root disk {}", vm.name, root.name),
            "node": self.node,
            "volumeId": copy,
            "volume": golden,
            "arch": std::env::consts::ARCH,
            "firmware": firmware,
            "provenance": g::provenance(&reg.namespace, &reg.name, now.timestamp().max(0) as u64),
            "sealedAt": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        });
        if let Some(sha) = sha256 {
            status["sha256"] = json!(sha);
        }
        Ok(status)
    }

    /// Remove what a failed run made: the sealer, the copy, the group. What
    /// could not be removed, said.
    pub async fn remove(&self, made: &Made) -> Vec<String> {
        let mut left = Vec::new();
        if let Some(sreg) = &made.sealer {
            if let Err(e) = self.machines.stop_sealer(sreg).await {
                left.push(format!("the sealer {}/{}: {e}", sreg.namespace, sreg.name));
            }
        }
        if let Some(copy) = &made.copy {
            if let Err(e) = self.engine.delete_volume(copy).await {
                left.push(format!("the copy {copy}: {e}"));
            }
        }
        if let Some(group) = &made.group {
            if let Err(e) = self.engine.delete_group(group).await {
                left.push(format!("the snapshot group {group}: {e}"));
            }
        }
        left
    }

    /// A request this node took and is not running (a kubelet restart):
    /// what its status says it made is removed, and it is `Failed`.
    pub async fn abandon(&self, obj: &Value) {
        let (ns, _) = ns_name(obj);
        let vm = obj["spec"]["vm"].as_str().unwrap_or("");
        let golden = obj["spec"]["golden"].as_str().unwrap_or("");
        let uid = sealer_uid(obj);
        let sealer = stormvm_node::console::find(&self.run_dir, ns, &format!("{vm}-sealer")).filter(|r| r.uid == uid);
        let made = Made {
            sealer,
            copy: obj["status"]["volumeId"].as_str().filter(|s| !s.is_empty()).map(str::to_string),
            group: None,
        };
        let mut left = self.remove(&made).await;
        if !golden.is_empty() {
            if let Err(e) = self.engine.delete_groups_named(&group_name(ns, vm, golden)).await {
                left.push(format!("the snapshot group {}: {e}", group_name(ns, vm, golden)));
            }
        }
        let mut why = "the kubelet restarted while making this golden, and a request runs once: \
                       what it had made is removed; ask again"
            .to_string();
        if !left.is_empty() {
            why = format!("{why} (not removed: {})", left.join("; "));
        }
        self.fail(obj, &why).await;
    }

    async fn fail(&self, obj: &Value, why: &str) {
        self.patch_status(obj, json!({ "phase": FAILED, "node": self.node, "message": why })).await;
        self.event(obj, "Warning", "Failed", why).await;
    }

    /// Taken: `Snapshotting` and this node, guarded by the version read.
    async fn claim(&self, obj: &Value) -> bool {
        if self.api_url.is_empty() {
            return true;
        }
        let (ns, name) = ns_name(obj);
        let mut body = json!({ "status": {
            "phase": SNAPSHOTTING,
            "node": self.node,
            "message": format!("snapshotting VM {}'s root disk", obj["spec"]["vm"].as_str().unwrap_or("")),
        }});
        if let Some(rv) = obj["metadata"]["resourceVersion"].as_str() {
            body["metadata"] = json!({ "resourceVersion": rv });
        }
        let url = format!("{}/apis/{API}/namespaces/{ns}/{RESOURCE}/{name}/status", self.api_url);
        match self.api.patch(&url).header("content-type", "application/merge-patch+json").json(&body)
            .send_retrying(retry::Policy::API).await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                debug!("{ns}/{name}: golden request not taken: {}", r.status());
                false
            }
            Err(e) => {
                debug!("{ns}/{name}: golden request not taken: {e}");
                apimachinery::reactor::failed();
                false
            }
        }
    }

    async fn vmi(&self, obj: &Value) -> Result<Value, String> {
        let (ns, _) = ns_name(obj);
        let vm = obj["spec"]["vm"].as_str().unwrap_or("");
        let url = format!("{}/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{vm}", self.api_url);
        let r = self.api.get(&url).send_retrying(retry::Policy::API).await.map_err(|e| format!("reading VMI {ns}/{vm}: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("reading VMI {ns}/{vm}: {}", r.status()));
        }
        r.json().await.map_err(|e| format!("reading VMI {ns}/{vm}: {e}"))
    }

    /// Every request, or `None` when there is no answer (or no CRD).
    async fn list(&self) -> Option<Vec<Value>> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!("{}/apis/{API}/{RESOURCE}", self.api_url);
        let r = match self.api.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) => r,
            Err(e) => {
                debug!("{RESOURCE} not listed: {e}");
                apimachinery::reactor::failed();
                return None;
            }
        };
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            if !self.absent_said.swap(true, std::sync::atomic::Ordering::Relaxed) {
                info!("no {API} {RESOURCE} on this cluster: goldens from VMs are not made");
            }
            return None;
        }
        if !r.status().is_success() {
            debug!("{RESOURCE} not listed: {}", r.status());
            apimachinery::reactor::failed();
            return None;
        }
        let list: Value = r.json().await.ok()?;
        Some(list["items"].as_array().cloned().unwrap_or_default())
    }

    async fn patch_status(&self, obj: &Value, status: Value) {
        if self.api_url.is_empty() {
            return;
        }
        let (ns, name) = ns_name(obj);
        let url = format!("{}/apis/{API}/namespaces/{ns}/{RESOURCE}/{name}/status", self.api_url);
        match self.api.patch(&url).header("content-type", "application/merge-patch+json")
            .json(&json!({ "status": status })).send_retrying(retry::Policy::API).await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => warn!("{ns}/{name}: golden status not written: {}", r.status()),
            Err(e) => warn!("{ns}/{name}: golden status not written: {e}"),
        }
    }

    async fn event(&self, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event(API, KIND, obj, etype, reason, message).await;
        }
    }
}

fn ns_name(obj: &Value) -> (&str, &str) {
    (
        obj["metadata"]["namespace"].as_str().unwrap_or("default"),
        obj["metadata"]["name"].as_str().unwrap_or(""),
    )
}

/// This node's engine: stormvm's client for the `/v1` calls (blocking, so
/// off the async threads), the kubelet's engine client for the listing and
/// the digest.
pub struct Stormblock {
    pub client: crate::engine::EngineClient,
}

impl Stormblock {
    fn block(&self) -> stormvm_block::Client {
        stormvm_block::Client::new(self.client.url())
    }

    async fn blocking<T: Send + 'static>(
        &self,
        what: String,
        f: impl Fn(&stormvm_block::Client) -> Result<T, String> + Send + Sync + 'static,
    ) -> Result<T, String> {
        let c = self.block();
        tokio::task::spawn_blocking(move || {
            retry::blocking(retry::Policy::ENGINE, &what, |e: &String| retry::class_of_message(e), || f(&c))
        })
        .await
        .map_err(|e| format!("{e}"))?
    }
}

/// A delete answered "not found" is done.
fn gone_is_done(r: Result<(), String>) -> Result<(), String> {
    match r {
        Err(e) if e.contains("404") || e.to_lowercase().contains("not found") => Ok(()),
        other => other,
    }
}

#[async_trait]
impl Engine for Stormblock {
    async fn volume_named(&self, name: &str) -> Result<Option<String>, String> {
        let url = format!("{}/api/v1/volumes", self.client.url());
        let r = self.client.get(&url).await.map_err(|e| format!("listing volumes: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("listing volumes: {}", r.status()));
        }
        let list: Value = r.json().await.map_err(|e| format!("listing volumes: {e}"))?;
        Ok(list["items"].as_array().into_iter().flatten()
            .find(|v| v["name"].as_str() == Some(name))
            .and_then(|v| v["id"].as_str().map(str::to_string)))
    }
    async fn volume_from_snapshot(&self, name: &str, snapshot: &stormvm_block::Snapshot) -> Result<String, String> {
        let (n, s) = (name.to_string(), snapshot.clone());
        self.blocking(format!("volume {name} from snapshot"), move |c| {
            c.volume_from_snapshot(&n, &s).map(|v| v.id).map_err(|e| e.to_string())
        }).await
    }
    async fn seal(&self, id: &str) -> Result<(), String> {
        let id = id.to_string();
        self.blocking(format!("seal {id}"), move |c| c.seal(&id, false).map_err(|e| e.to_string())).await
    }
    async fn digest(&self, id: &str) -> Option<String> {
        let url = format!("{}/api/v1/volumes/{id}/digest", self.client.url());
        let r = self.client.get(&url).await.ok()?;
        if !r.status().is_success() {
            debug!("volume {id}: no digest: {}", r.status());
            return None;
        }
        let v: Value = r.json().await.ok()?;
        v["sha256"].as_str().map(str::to_string)
    }
    async fn delete_volume(&self, id: &str) -> Result<(), String> {
        let id = id.to_string();
        gone_is_done(self.blocking(format!("delete volume {id}"), move |c| c.delete_volume(&id).map_err(|e| e.to_string())).await)
    }
    async fn delete_groups_named(&self, name: &str) -> Result<(), String> {
        let n = name.to_string();
        let groups = self.blocking(format!("group snapshots {name}"), move |c| {
            c.group_snapshots(Some(&n)).map_err(|e| e.to_string())
        }).await?;
        for g in groups.into_iter().filter(|g| g.name == name) {
            self.delete_group(&g.id).await?;
        }
        Ok(())
    }
    async fn delete_group(&self, id: &str) -> Result<(), String> {
        let id = id.to_string();
        gone_is_done(self.blocking(format!("delete group snapshot {id}"), move |c| {
            c.delete_group_snapshot(&id).map_err(|e| e.to_string())
        }).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn request(phase: Option<&str>, node: Option<&str>) -> Value {
        let mut o = json!({
            "apiVersion": API, "kind": KIND,
            "metadata": { "name": "company-base", "namespace": "ci", "uid": "r-1", "resourceVersion": "3" },
            "spec": { "vm": "base", "golden": "company-base", "image": "company-base" },
        });
        if let Some(p) = phase { o["status"]["phase"] = json!(p); }
        if let Some(n) = node { o["status"]["node"] = json!(n); }
        o
    }

    #[test]
    fn a_request_is_taken_once_by_the_node_its_vm_runs_on() {
        assert_eq!(decide(&request(None, None), "n1", true, false), Action::Claim);
        assert_eq!(decide(&request(Some(PENDING), None), "n1", true, false), Action::Claim);
        assert_eq!(decide(&request(None, None), "n1", false, false), Action::Skip, "the VM is not here");
        assert_eq!(decide(&request(None, None), "n1", true, true), Action::Skip, "running now");
        for done in [SEALED, FAILED] {
            assert_eq!(decide(&request(Some(done), Some("n1")), "n1", true, false), Action::Skip, "{done} runs once");
        }
        assert_eq!(decide(&request(Some(SEALING), Some("n2")), "n1", true, false), Action::Skip, "another node's");
        for mid in [SNAPSHOTTING, SEALING] {
            assert_eq!(decide(&request(Some(mid), Some("n1")), "n1", true, false), Action::Abandon, "{mid} after a restart");
        }
        let mut deleted = request(None, None);
        deleted["metadata"]["deletionTimestamp"] = json!("2026-11-10T00:00:00Z");
        assert_eq!(decide(&deleted, "n1", true, false), Action::Skip);
        let mut nameless = request(None, None);
        nameless["spec"]["golden"] = json!("");
        assert!(matches!(decide(&nameless, "n1", true, false), Action::Fail(_)));
    }

    #[test]
    fn sealer_registrations_are_told_apart() {
        let r = request(None, None);
        assert_eq!(sealer_uid(&r), "vmgolden-r-1");
        assert!(is_sealer(&sealer_uid(&r)));
        assert!(!is_sealer("r-1"));
        assert_eq!(group_name("ci", "base", "company-base"), "ci.base.golden-company-base");
    }

    /// What the fakes were asked, in order.
    type Log = Arc<StdMutex<Vec<String>>>;

    #[derive(Default)]
    struct FakeEngine {
        log: Log,
        existing: Option<String>,
        fail_seal: bool,
    }

    #[async_trait]
    impl Engine for FakeEngine {
        async fn volume_named(&self, name: &str) -> Result<Option<String>, String> {
            self.log.lock().unwrap().push(format!("named {name}"));
            Ok(self.existing.clone())
        }
        async fn volume_from_snapshot(&self, name: &str, s: &stormvm_block::Snapshot) -> Result<String, String> {
            self.log.lock().unwrap().push(format!("copy {name} from {}", s.id));
            Ok("v-copy".into())
        }
        async fn seal(&self, id: &str) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("seal {id}"));
            if self.fail_seal { Err("refused".into()) } else { Ok(()) }
        }
        async fn digest(&self, id: &str) -> Option<String> {
            self.log.lock().unwrap().push(format!("digest {id}"));
            Some("abc123".into())
        }
        async fn delete_volume(&self, id: &str) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("delete volume {id}"));
            Ok(())
        }
        async fn delete_groups_named(&self, name: &str) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("delete groups {name}"));
            Ok(())
        }
        async fn delete_group(&self, id: &str) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("delete group {id}"));
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeMachines {
        log: Log,
        /// The sealer's agent never answers.
        no_agent: bool,
        run_dir: String,
    }

    #[async_trait]
    impl Machines for FakeMachines {
        async fn snapshot_root(&self, _reg: &Registration, root_id: &str, group: &str)
            -> Result<stormvm_block::GroupSnapshot, String> {
            self.log.lock().unwrap().push(format!("snapshot {root_id} as {group}"));
            Ok(stormvm_block::GroupSnapshot {
                id: "g-1".into(),
                name: group.into(),
                snapshots: vec![stormvm_block::Snapshot {
                    id: "s-1".into(), name: "s".into(), source_volume_id: root_id.into(), size_bytes: 1 << 30,
                    ready: true, created_at_ms: 0, group_snapshot_id: Some("g-1".into()),
                }],
                ready: true,
                created_at_ms: 0,
            })
        }
        async fn start_sealer(&self, uid: &str, spec: &VmSpec) -> Result<Registration, String> {
            assert_eq!(spec.disks.len(), 1, "the copy alone");
            assert!(spec.interfaces.is_empty() && spec.guest_agent);
            self.log.lock().unwrap().push(format!("start {} as {uid} on {:?}", spec.name, spec.disks[0].from));
            let logging = stormvm_node::plan::Logging::pod("/var/log/pods/x", 0);
            Ok(Registration::of(spec, uid, &logging, &self.run_dir))
        }
        async fn deidentify(&self, reg: &Registration, _within: std::time::Duration) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("deidentify {}", reg.name));
            if self.no_agent { Err("the copy's guest agent did not answer within 300s".into()) } else { Ok(()) }
        }
        async fn stop_sealer(&self, reg: &Registration) -> Result<(), String> {
            self.log.lock().unwrap().push(format!("stop {}", reg.name));
            Ok(())
        }
    }

    fn vmi() -> Value {
        json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": { "name": "base", "namespace": "ci", "uid": "u-base" },
            "spec": {
                "domain": {
                    "memory": { "guest": "2Gi" },
                    "devices": { "disks": [ { "name": "root", "disk": { "bus": "virtio" } } ] },
                },
                "volumes": [ { "name": "root", "containerDisk": { "image": "fedora-44-x86_64" } } ],
            },
        })
    }

    fn source(run_dir: &str) -> Registration {
        let vm = stormvm_spec::kube::from_kube(&vmi()).unwrap();
        let logging = stormvm_node::plan::Logging::pod("/var/log/pods/ci_base_u-base/base", 0);
        let mut reg = Registration::of(&vm, "u-base", &logging, run_dir);
        reg.disks = vec![stormvm_node::console::RegisteredDisk {
            name: "root".into(), volume_id: Some("v-root".into()), owned: true, device: "/dev/ublkb1".into(),
        }];
        reg
    }

    fn goldens(engine: FakeEngine, machines: FakeMachines, run_dir: &str) -> Goldens {
        Goldens::new(reqwest::Client::new(), "", "n1", run_dir, Arc::new(engine), Arc::new(machines))
    }

    #[tokio::test]
    async fn a_golden_is_snapshotted_copied_deidentified_and_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let log: Log = Default::default();
        let gs = goldens(FakeEngine { log: log.clone(), ..Default::default() },
                         FakeMachines { log: log.clone(), run_dir: run.into(), ..Default::default() }, run);
        let mut made = Made::default();
        let status = gs.make(&request(None, None), &vmi(), &source(run), &mut made).await.expect("sealed");
        assert_eq!(*log.lock().unwrap(), vec![
            "named company-base",
            "snapshot v-root as ci.base.golden-company-base",
            "copy company-base from s-1",
            "start base-sealer as vmgolden-r-1 on Volume(\"v-copy\")",
            "deidentify base-sealer",
            "stop base-sealer",
            "seal v-copy",
            "delete group g-1",
            "digest v-copy",
        ]);
        assert_eq!(made, Made::default(), "nothing left to remove");
        assert_eq!(status["phase"], SEALED);
        assert_eq!(status["volumeId"], "v-copy");
        assert_eq!(status["volume"], "company-base");
        assert_eq!(status["sha256"], "abc123");
        assert_eq!(status["firmware"], "uefi");
        assert_eq!(status["node"], "n1");
        assert!(status["provenance"].as_str().unwrap().starts_with("vm:ci/base@"));
        assert!(status["sealedAt"].as_str().unwrap().ends_with('Z'));
    }

    /// No agent in the guest: Failed, and the sealer, the copy and the
    /// group all removed.
    #[tokio::test]
    async fn a_guest_with_no_agent_fails_and_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let log: Log = Default::default();
        let gs = goldens(FakeEngine { log: log.clone(), ..Default::default() },
                         FakeMachines { log: log.clone(), run_dir: run.into(), no_agent: true }, run);
        let mut made = Made::default();
        let err = gs.make(&request(None, None), &vmi(), &source(run), &mut made).await.unwrap_err();
        assert!(err.contains("guest agent"), "{err}");
        assert_eq!(made.copy.as_deref(), Some("v-copy"));
        assert_eq!(made.group.as_deref(), Some("g-1"));
        assert!(made.sealer.is_some());
        log.lock().unwrap().clear();
        assert!(gs.remove(&made).await.is_empty());
        assert_eq!(*log.lock().unwrap(), vec!["stop base-sealer", "delete volume v-copy", "delete group g-1"]);
    }

    /// A seal refused: the stopped copy and the group are removed.
    #[tokio::test]
    async fn a_refused_seal_removes_the_copy() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let log: Log = Default::default();
        let gs = goldens(FakeEngine { log: log.clone(), fail_seal: true, ..Default::default() },
                         FakeMachines { log: log.clone(), run_dir: run.into(), ..Default::default() }, run);
        let mut made = Made::default();
        assert!(gs.make(&request(None, None), &vmi(), &source(run), &mut made).await.is_err());
        assert_eq!(made, Made { group: Some("g-1".into()), copy: Some("v-copy".into()), sealer: None });
    }

    /// A golden of that name already there: refused before anything is made.
    #[tokio::test]
    async fn a_name_already_taken_is_refused_before_anything_is_made() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let log: Log = Default::default();
        let gs = goldens(FakeEngine { log: log.clone(), existing: Some("v-old".into()), ..Default::default() },
                         FakeMachines { log: log.clone(), run_dir: run.into(), ..Default::default() }, run);
        let mut made = Made::default();
        let err = gs.make(&request(None, None), &vmi(), &source(run), &mut made).await.unwrap_err();
        assert!(err.contains("already on this node's engine"), "{err}");
        assert_eq!(made, Made::default());
        assert_eq!(*log.lock().unwrap(), vec!["named company-base"]);
    }

    /// A VMI of the same name but another uid (the one registered here was
    /// replaced) is refused.
    #[tokio::test]
    async fn another_vmi_of_the_name_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let gs = goldens(FakeEngine::default(), FakeMachines { run_dir: run.into(), ..Default::default() }, run);
        let mut other = vmi();
        other["metadata"]["uid"] = json!("u-new");
        let err = gs.make(&request(None, None), &other, &source(run), &mut Made::default()).await.unwrap_err();
        assert!(err.contains("another uid"), "{err}");
    }

    /// After a restart: the recorded copy and the group by name removed,
    /// and the sealer this request registered stopped.
    #[tokio::test]
    async fn a_request_left_by_a_restart_is_failed_and_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let run = dir.path().to_str().unwrap();
        let log: Log = Default::default();
        let machines = FakeMachines { log: log.clone(), run_dir: run.into(), ..Default::default() };
        let vm = stormvm_spec::kube::from_kube(&vmi()).unwrap();
        let sealer = g::sealer_spec(&vm, "v-copy").unwrap();
        let sreg = machines.start_sealer("vmgolden-r-1", &sealer).await.unwrap();
        stormvm_node::console::write(run, &sreg).unwrap();
        log.lock().unwrap().clear();
        let gs = goldens(FakeEngine { log: log.clone(), ..Default::default() }, machines, run);
        let mut obj = request(Some(SEALING), Some("n1"));
        obj["status"]["volumeId"] = json!("v-copy");
        gs.abandon(&obj).await;
        assert_eq!(*log.lock().unwrap(), vec![
            "stop base-sealer",
            "delete volume v-copy",
            "delete groups ci.base.golden-company-base",
        ]);
    }
}
