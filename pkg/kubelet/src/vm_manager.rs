//! Virtual machines, on the loop this kubelet already runs.
//!
//! **A VM is a workload, not a special case.** It is scheduled to a node like
//! anything else, started through the ring this kubelet already owns, and its
//! output lands where `kubectl logs` already looks. What makes it a VM rather
//! than a container is the stormpump *domain* — 3, 4 or 5 — and the fact that
//! its disks are block devices named on a hypervisor's command line instead of
//! filesystems bound into a root.
//!
//! There is no virt-launcher pod and no libvirt. The object is KubeVirt's
//! (`VirtualMachineInstance`), because that vocabulary is the one people
//! already know; everything under it is storm's own. See stormvm's
//! `docs/kube.md`.
//!
//! # Why here and not in a daemon of its own
//!
//! The ring connection is per client, and a workload belongs to the client
//! that started it: a second daemon holding a second connection means its
//! death is its VMs' death. This kubelet already holds the connection, already
//! absorbs exits, already owns the restart decision and already serves the pod
//! log path. A VM manager beside `PodManager` reuses all of it.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};
use stormpump_abi::handle::Handle;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::stormpump_ring::{RingClient, RingError};
use stormvm_node::plan::{self, Logging, ResolvedDisk};
use stormvm_spec::{DiskSource, VmSpec};

/// Where pod logs live, and therefore where a VM's console has to go for
/// `kubectl logs` to find it.
const LOG_ROOT: &str = "/var/log/pods";
/// Per-VM sockets: the serial console and the hypervisor's control socket.
const RUN_ROOT: &str = "/run/stormvm";

/// One VM this node is running, or has run.
#[derive(Debug, Clone)]
pub struct Vm {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// Where this machine's output goes, so a failure can be quoted back.
    pub log_dir: String,
    pub handle: Handle,
    /// What was cloned or attached for it, so it can be given back.
    pub disks: Vec<ResolvedDisk>,
    pub phase: Phase,
    /// Filled in when it ends.
    pub exit_code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Pending => "Pending",
            Phase::Running => "Running",
            Phase::Succeeded => "Succeeded",
            Phase::Failed => "Failed",
        }
    }

    fn terminal(self) -> bool {
        matches!(self, Phase::Succeeded | Phase::Failed)
    }
}

pub struct VmManager {
    ring: Option<Arc<RingClient>>,
    /// stormblock's management API on this node.
    storage: String,
    node_name: String,
    api: reqwest::Client,
    api_url: String,
    http: reqwest::Client,
    /// Keyed by uid: a VM deleted and recreated under one name is two
    /// different machines, and treating them as one is how the second finds
    /// the first's disks.
    vms: Mutex<HashMap<String, Vm>>,
}

impl VmManager {
    pub fn new(
        ring: Option<Arc<RingClient>>,
        node_name: impl Into<String>,
        api: reqwest::Client,
        api_url: impl Into<String>,
    ) -> VmManager {
        VmManager {
            ring,
            storage: "http://127.0.0.1:9090".into(),
            node_name: node_name.into(),
            api,
            api_url: api_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
            vms: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_storage(mut self, storage: impl Into<String>) -> VmManager {
        self.storage = storage.into();
        self
    }

    /// Reconcile: start what is assigned here and not running, stop what is
    /// running here and no longer assigned.
    ///
    /// `desired` is what the apiserver says belongs on this node. A VM whose
    /// object has gone is stopped; a VM that has ended is left in its terminal
    /// phase and its disks given back once — not restarted, because a
    /// `restartPolicy` for VMs is the controller's decision and this node does
    /// not have one yet.
    pub async fn sync(&self, desired: &[Value]) {
        self.absorb_ends().await;

        let mut want: Vec<(String, Value)> = Vec::new();
        for obj in desired {
            let uid = obj["metadata"]["uid"].as_str().unwrap_or("").to_string();
            if uid.is_empty() {
                warn!("a VirtualMachineInstance with no uid was skipped");
                continue;
            }
            want.push((uid, obj.clone()));
        }

        for (uid, obj) in &want {
            if self.vms.lock().await.contains_key(uid) {
                continue;
            }
            if let Err(e) = self.start(uid, obj).await {
                let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
                let name = obj["metadata"]["name"].as_str().unwrap_or("");
                warn!("{ns}/{name}: {e}");
                self.record_failure(uid, obj, &e).await;
            }
        }

        let keep: Vec<String> = want.iter().map(|(u, _)| u.clone()).collect();
        let gone: Vec<Vm> = {
            let vms = self.vms.lock().await;
            vms.values().filter(|v| !keep.contains(&v.uid)).cloned().collect()
        };
        for vm in gone {
            self.stop(&vm).await;
            self.vms.lock().await.remove(&vm.uid);
        }
    }

    /// What this node is running, for `/pods`-style introspection and tests.
    pub async fn running(&self) -> Vec<Vm> {
        self.vms.lock().await.values().cloned().collect()
    }

    async fn start(&self, uid: &str, obj: &Value) -> Result<(), String> {
        let vm: VmSpec = stormvm_spec::kube::from_kube(obj).map_err(|e| e.to_string())?;
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
        let ring = self.ring.clone().ok_or_else(|| {
            "no ring to stormpump: only the engine starts a machine".to_string()
        })?;

        // Storage first. Nothing has been asked of the engine yet, so a golden
        // that does not exist costs a failed status and no cleanup.
        let disks = self.resolve_disks(&vm).await?;

        // The pod log directory, because that is where `kubectl logs` looks.
        // The container name is the VM's, so the path is the one the kubelet's
        // own log handler builds for a container of the same name.
        let dir = format!("{LOG_ROOT}/{ns}_{}_{uid}/{}", vm.name, vm.name);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            self.release(&disks).await;
            return Err(format!("could not make {dir}: {e}"));
        }
        let run = format!("{RUN_ROOT}/{}", vm.name);
        let _ = std::fs::create_dir_all(&run);

        let logging = Logging::pod(&dir, 0);
        let built = match plan::build(&vm, &disks, &logging, RUN_ROOT) {
            Ok(p) => p,
            Err(e) => {
                self.release(&disks).await;
                return Err(format!("{e:#}"));
            }
        };

        // The ring is blocking and owns its own thread; the async side reaches
        // it through `spawn_blocking`, as the container path does.
        let domain = built.domain;
        let volumes = built.volumes.clone();
        let spec_bytes = built.spec.clone();
        let started = tokio::task::spawn_blocking(move || -> Result<Handle, RingError> {
            let mut handles = Vec::with_capacity(volumes.len());
            for path in &volumes {
                handles.push(ring.volume_register(path)?);
            }
            let spec = ring.spec_define(spec_bytes)?;
            // No root and no sandbox: a machine's root is a disk on the
            // hypervisor's command line, and a VM is not put in a pod's
            // network namespace by the engine — its NIC is a tap, which is a
            // descriptor rather than a namespace.
            ring.spawn(spec, Handle::NONE, handles[0], Handle::NONE, &handles[1..], domain)
        })
        .await
        .map_err(|e| format!("ring task: {e}"))?;

        let handle = match started {
            Ok(h) => h,
            Err(e) => {
                self.release(&disks).await;
                return Err(format!("stormpump refused: {e:?}"));
            }
        };

        info!(vm = %vm.name, namespace = %ns, ?handle, "vm started");
        let rec = Vm {
            namespace: ns,
            name: vm.name.clone(),
            uid: uid.to_string(),
            log_dir: dir.clone(),
            handle,
            disks,
            phase: Phase::Running,
            exit_code: 0,
            message: String::new(),
        };
        self.vms.lock().await.insert(uid.to_string(), rec.clone());
        self.patch_status(&rec).await;
        Ok(())
    }

    /// Ask the engine about every running VM, because an exit that nothing
    /// notices leaves a dead machine looking like a live one.
    ///
    /// Asked rather than subscribed to: the unsolicited exit channel is
    /// drained by the container runtime, and a shared channel has exactly one
    /// reader. A query per VM per sync is a handful of round trips at ~200 µs
    /// each, which is not worth a second mechanism to avoid.
    async fn absorb_ends(&self) {
        let Some(ring) = self.ring.clone() else { return };
        let live: Vec<Vm> = {
            let vms = self.vms.lock().await;
            vms.values().filter(|v| !v.phase.terminal()).cloned().collect()
        };
        for vm in live {
            let r = ring.clone();
            let handle = vm.handle;
            let answer = tokio::task::spawn_blocking(move || r.query(handle)).await;
            let Ok(Ok(cqe)) = answer else { continue };
            // aux: 0 running, 1 parked, 2 | (wait status << 8).
            if cqe.aux & 0xff != 2 {
                continue;
            }
            let status = (cqe.aux >> 8) as i32;
            let signal = status & 0x7f;
            let code = if signal != 0 { 128 + signal } else { (status >> 8) & 0xff };
            let mut done = vm.clone();
            done.phase = if code == 0 { Phase::Succeeded } else { Phase::Failed };
            done.exit_code = code;
            done.message = match (code, hypervisor_said(&vm.log_dir)) {
                // **What it said, not that it failed.** "the hypervisor exited
                // with 1" is true and useless: the reason is in a file on a
                // node with no shell, and reading it has cost this stack whole
                // rebuild-and-boot cycles more than once.
                (c, Some(said)) if c != 0 => format!("the hypervisor exited with {c}: {said}"),
                (c, _) => format!("the hypervisor exited with {c}"),
            };
            info!(vm = %done.name, code, "vm ended");
            self.release(&done.disks).await;
            if let Some(r) = self.ring.clone() {
                let h = done.handle;
                let _ = tokio::task::spawn_blocking(move || r.workload_release(h)).await;
            }
            self.vms.lock().await.insert(done.uid.clone(), done.clone());
            self.patch_status(&done).await;
        }
    }

    async fn stop(&self, vm: &Vm) {
        if let Some(ring) = self.ring.clone() {
            let handle = vm.handle;
            // A machine gets a real grace period: ACPI shutdown, then a kill.
            // Thirty seconds is what a guest needs to flush and unmount, and
            // the timer is the engine's so it survives this process.
            let _ = tokio::task::spawn_blocking(move || ring.stop(handle, 30)).await;
        }
        self.release(&vm.disks).await;
        info!(vm = %vm.name, "vm stopped");
    }

    /// Clone or attach every disk. Failure gives back what it already took —
    /// an attachment left behind is a device nobody will ever release.
    async fn resolve_disks(&self, vm: &VmSpec) -> Result<Vec<ResolvedDisk>, String> {
        let mut done: Vec<ResolvedDisk> = Vec::new();
        for d in &vm.disks {
            let volume_id = match &d.from {
                DiskSource::Golden(g) => {
                    let body = json!({
                        "name": format!("{}-{}", vm.name, d.name),
                        "size": d.size,
                        "label": format!("storm.io/vm={}", vm.name),
                        "verify": true,
                    });
                    match self
                        .post(&format!("{}/api/v1/volumes/{g}/clone", self.storage), &body)
                        .await
                    {
                        Ok(v) => v["id"].as_str().unwrap_or_default().to_string(),
                        Err(e) => {
                            self.release(&done).await;
                            return Err(format!("cloning golden {g} for disk {}: {e}", d.name));
                        }
                    }
                }
                DiskSource::Volume(v) => v.clone(),
                DiskSource::CloudInit => match self.seed_volume(vm).await {
                    Ok(id) => id,
                    Err(e) => {
                        self.release(&done).await;
                        return Err(format!("disk {}: {e}", d.name));
                    }
                },
            };
            // No node in the request. This kubelet is asking the stormblock
            // *on this node* to attach a volume *here*; naming the node adds a
            // way for the two to disagree and no information — and they did
            // disagree, because a stormblock started by an init system has no
            // HOSTNAME in its environment and called itself "localhost" while
            // the attach named the node. The storage knows which machine it
            // is running on.
            let body = json!({ "transport": "ublk" });
            let info = match self
                .post(&format!("{}/api/v1/volumes/{volume_id}/attach", self.storage), &body)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    self.release(&done).await;
                    return Err(format!("attaching {volume_id} for disk {}: {e}", d.name));
                }
            };
            let Some(device) = info["device_hint"].as_str() else {
                self.release(&done).await;
                return Err(format!(
                    "disk {} did not attach locally: {info} — an NVMe-oF attach needs a connect \
                     this node does not do yet",
                    d.name
                ));
            };
            done.push(ResolvedDisk {
                name: d.name.clone(),
                device: device.to_string(),
                volume_id: Some(volume_id),
                readonly: d.readonly,
                bus: d.bus,
            });
        }
        Ok(done)
    }

    /// Build this VM's cloud-init seed.
    ///
    /// **A cloud image has no password and no keys**, by design: it becomes
    /// *this* machine by reading a seed. The seed is a small filesystem
    /// labelled `cidata` holding `meta-data`, `user-data` and (when the
    /// network is not DHCP) `network-config` — cloud-init finds it by that
    /// label and by nothing else.
    ///
    /// It is built the way everything else here is: a template formatted once
    /// per node, a copy-on-write clone per VM, and the files written through
    /// stormblock, which writes into an ext4 image directly with no mount and
    /// no loop device. Writing an ISO 9660 here would mean a filesystem writer
    /// in the kubelet, which is the thing this ecosystem has learned not to
    /// duplicate.
    ///
    /// The hostname comes from the **definition** — the VM's `hostname` or its
    /// name — never from a lease. A guest that takes its name from DHCP is a
    /// guest whose identity changes when the network does, and its
    /// certificates and logs change with it.
    async fn seed_volume(&self, vm: &VmSpec) -> Result<String, String> {
        let seed = stormvm_cloudinit::Seed::for_vm(vm);
        let template = self.ensure_seed_template().await?;

        // A clone per VM, named after it, so a start that is retried finds its
        // own seed rather than making a second.
        let body = json!({ "name": format!("{}-seed", vm.name), "verify": true });
        let clone = self
            .post(&format!("{}/api/v1/volumes/{template}/clone", self.storage), &body)
            .await
            .map_err(|e| format!("cloning the cidata template: {e}"))?;
        let id = clone["id"]
            .as_str()
            .ok_or_else(|| format!("stormblock returned no volume for the seed: {clone}"))?
            .to_string();

        let files: Vec<Value> = seed
            .files()
            .into_iter()
            .map(|(name, content)| json!({ "path": format!("/{name}"), "content": content }))
            .collect();
        self.post(
            &format!("{}/api/v1/volumes/{id}/files", self.storage),
            &json!({ "files": files }),
        )
        .await
        .map_err(|e| format!("writing the seed: {e}"))?;
        Ok(id)
    }

    /// The template every seed is cloned from, made once per node.
    ///
    /// Idempotent by name: two VMs starting at once both ask, and the second
    /// must find the first's rather than build a second template.
    async fn ensure_seed_template(&self) -> Result<String, String> {
        let url = format!("{}/api/v1/fstemplates", self.storage);
        if let Ok(r) = self.http.get(&format!("{url}/cidata")).send().await {
            if r.status().is_success() {
                if let Ok(v) = r.json::<Value>().await {
                    if let Some(id) = v["id"].as_str() {
                        return Ok(id.to_string());
                    }
                }
            }
        }
        // 8 MiB: the three files are a few hundred bytes and the smallest ext4
        // mke2fs will make is already larger than they need.
        let body = json!({ "name": "cidata", "size": "8M", "label": "cidata" });
        let v = self
            .post(&url, &body)
            .await
            .map_err(|e| format!("creating the cidata template: {e}"))?;
        v["id"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| format!("stormblock returned no template: {v}"))
    }

    /// Best effort: a start that has already gone wrong must not be made worse
    /// by refusing to clean up.
    async fn release(&self, disks: &[ResolvedDisk]) {
        for d in disks {
            let Some(id) = &d.volume_id else { continue };
            let url = format!("{}/api/v1/volumes/{id}/attach", self.storage);
            if let Err(e) = self.http.delete(&url).send().await {
                warn!("could not detach {id} ({}): {e}", d.name);
            }
        }
    }

    async fn record_failure(&self, uid: &str, obj: &Value, why: &str) {
        let rec = Vm {
            namespace: obj["metadata"]["namespace"].as_str().unwrap_or("default").into(),
            name: obj["metadata"]["name"].as_str().unwrap_or_default().into(),
            uid: uid.to_string(),
            log_dir: String::new(),
            handle: Handle::NONE,
            disks: Vec::new(),
            phase: Phase::Failed,
            exit_code: 0,
            message: why.to_string(),
        };
        self.vms.lock().await.insert(uid.to_string(), rec.clone());
        self.patch_status(&rec).await;
    }

    /// Say what happened, in the object.
    ///
    /// The reason a VM did not start belongs where somebody can read it. The
    /// alternative is a log on a node with no shell, which is the failure the
    /// kubelet's own event work exists to fix.
    async fn patch_status(&self, vm: &Vm) {
        let url = format!(
            "{}/apis/kubevirt.io/v1/namespaces/{}/virtualmachineinstances/{}/status",
            self.api_url, vm.namespace, vm.name
        );
        let mut status = json!({
            "phase": vm.phase.as_str(),
            "nodeName": self.node_name,
        });
        if !vm.message.is_empty() {
            status["reason"] = json!(if vm.phase == Phase::Failed { "Failed" } else { "Ended" });
            status["message"] = json!(vm.message);
        }
        let body = json!({ "status": status });
        if let Err(e) = self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&body)
            .send()
            .await
        {
            warn!("could not report {}/{}: {e}", vm.namespace, vm.name);
        }
    }

    async fn post(&self, url: &str, body: &Value) -> Result<Value, String> {
        let resp = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|e| format!("{e}: {text}"))
    }
}

/// The last thing the hypervisor printed before it went.
///
/// Bounded and best-effort: a log that cannot be read is not a reason to lose
/// the exit code as well, and a status message is not the place for a
/// megabyte. The tail rather than the head — a hypervisor that refuses
/// something says so last, after whatever it managed first.
fn hypervisor_said(log_dir: &str) -> Option<String> {
    const KEEP: usize = 400;
    if log_dir.is_empty() {
        return None;
    }
    let text = std::fs::read_to_string(format!("{log_dir}/hypervisor.log")).ok()?;
    let tail: String = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("; ");
    if tail.is_empty() {
        return None;
    }
    Some(tail.chars().take(KEEP).collect())
}

/// The VMIs the apiserver says belong on this node.
///
/// Best-effort, like the pod list: a cluster with no such CRD is the ordinary
/// case on a node that runs no VMs, and it must not abort a sync.
pub async fn list_for_node(api: &reqwest::Client, api_url: &str, node: &str) -> Vec<Value> {
    let url = format!("{}/apis/kubevirt.io/v1/virtualmachineinstances", api_url.trim_end_matches('/'));
    let Ok(resp) = api.get(&url).send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let Ok(v) = resp.json::<Value>().await else {
        return Vec::new();
    };
    v["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter(|o| assigned_to(o, node))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Whether a VMI is this node's.
///
/// `status.nodeName` first, because that is what the scheduler writes, then
/// `spec.nodeName` for one placed by hand. Anything unassigned is not this
/// node's business — a kubelet that started unscheduled work would start it on
/// every node at once.
fn assigned_to(obj: &Value, node: &str) -> bool {
    obj["status"]["nodeName"].as_str() == Some(node) || obj["spec"]["nodeName"].as_str() == Some(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vmi(node: &str) -> Value {
        json!({
            "kind": "VirtualMachineInstance",
            "metadata": { "name": "web-1", "namespace": "default", "uid": "u-1" },
            "spec": { "nodeName": node, "domain": { "memory": { "guest": "1Gi" } } }
        })
    }

    /// A kubelet that started unscheduled work would start it on every node at
    /// once.
    #[test]
    fn only_this_nodes_machines_are_this_nodes_business() {
        assert!(assigned_to(&vmi("n1"), "n1"));
        assert!(!assigned_to(&vmi("n2"), "n1"));
        let mut scheduled = vmi("");
        scheduled["spec"]["nodeName"] = json!(null);
        scheduled["status"] = json!({ "nodeName": "n1" });
        assert!(assigned_to(&scheduled, "n1"), "status.nodeName is what the scheduler writes");
        let unassigned = json!({ "kind": "VirtualMachineInstance", "spec": {} });
        assert!(!assigned_to(&unassigned, "n1"));
    }

    /// The exit status packing the engine uses: `aux & 0xff == 2` means
    /// exited, and the wait status is in the bits above it. A signalled guest
    /// reports 128+signal, which is what a shell does and what Kubernetes
    /// shows for a container.
    #[test]
    fn an_exit_is_decoded_the_way_the_engine_packs_it() {
        let decode = |aux: u32| -> Option<i32> {
            if aux & 0xff != 2 {
                return None;
            }
            let status = (aux >> 8) as i32;
            let signal = status & 0x7f;
            Some(if signal != 0 { 128 + signal } else { (status >> 8) & 0xff })
        };
        assert_eq!(decode(0), None, "still running");
        assert_eq!(decode(1), None, "parked is not ended");
        // exit(0) is wait status 0; exit(3) is 3 << 8.
        assert_eq!(decode(2 | (0 << 8)), Some(0));
        assert_eq!(decode(2 | ((3 << 8) << 8)), Some(3));
        // SIGKILL is 9 in the low seven bits.
        assert_eq!(decode(2 | (9 << 8)), Some(137));
    }

    /// The reason a hypervisor gave, quoted back — the whole point being that
    /// "exited with 1" is true and useless when the explanation is in a file
    /// on a node with no shell.
    #[test]
    fn what_the_hypervisor_said_reaches_the_status() {
        let dir = std::env::temp_dir().join(format!("vmtest-log-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("hypervisor.log");
        std::fs::write(
            &path,
            "qemu-system-x86_64: warning: something\n\n             qemu-system-x86_64: -blockdev: could not open '/dev/ublkb9': No such file\n",
        )
        .unwrap();
        let said = hypervisor_said(dir.to_str().unwrap()).unwrap();
        assert!(said.contains("could not open"), "{said}");
        assert!(!said.contains("\n\n"), "blank lines are not information: {said}");

        // A log that cannot be read must not cost the exit code as well.
        assert_eq!(hypervisor_said("/nonexistent/dir"), None);
        assert_eq!(hypervisor_said(""), None);

        // An empty log says nothing rather than an empty quote.
        std::fs::write(&path, "\n\n").unwrap();
        assert_eq!(hypervisor_said(dir.to_str().unwrap()), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A VM with no uid cannot be told apart from the next one of the same
    /// name, and the second would find the first's disks.
    #[tokio::test]
    async fn a_vmi_with_no_uid_is_skipped_rather_than_started() {
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "http://127.0.0.1:1");
        let mut no_uid = vmi("n1");
        no_uid["metadata"]["uid"] = json!(null);
        m.sync(&[no_uid]).await;
        assert!(m.running().await.is_empty());
    }

    /// Without a ring there is no engine, and a VM that "started" without one
    /// would be a status nobody can act on.
    #[tokio::test]
    async fn no_ring_is_a_failed_vm_with_a_reason_not_a_silent_nothing() {
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "http://127.0.0.1:1");
        m.sync(&[vmi("n1")]).await;
        let all = m.running().await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].phase, Phase::Failed);
        assert!(all[0].message.contains("no ring"), "{}", all[0].message);
    }
}
