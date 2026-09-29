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
///
/// Public because the console doors mounted in `server.rs` read the same
/// directory this writes (rustkube-node#43). One constant, so the side that
/// registers a VM and the side that opens its console cannot look in two
/// different places.
pub const RUN_ROOT: &str = "/run/stormvm";

/// Drop a machine's console registration.
///
/// Absent is success in `console::remove`, so a stop racing a failed start is
/// not an error and this needs no "was it written?" bookkeeping.
fn deregister(namespace: &str, name: &str) {
    if let Err(e) = stormvm_node::console::remove(RUN_ROOT, namespace, name) {
        warn!(vm = %name, namespace = %namespace, "could not drop the console registration: {e}");
    }
}
/// Held on a VMI while this node runs its machine, so the object's deletion
/// completes only once the machine is gone (#35). Without it the object went
/// at once, and a kubelet that missed the moment left the hypervisor running
/// with nothing in the cluster that showed or controlled it.
pub const FINALIZER: &str = "storm.io/vm";

/// QUERY reports a wait status only when the low byte is the exited state.
fn exited(aux: u32) -> bool {
    aux & 0xff == 2
}

/// `"true"` on a VMI or its VirtualMachine: its disks get no owner, so the
/// orphan sweep never deletes them — they outlive even the VM (#75).
pub const RETAIN_ANNOTATION: &str = "storm.io/retain-disks";

/// How often, at most, the orphan sweep asks the engine and the apiserver.
const SWEEP_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// Who a machine's disks belong to, as stormblock records it
/// (`owner {kind, namespace, name, uid}`, stormblock#115).
///
/// **The VirtualMachine, not the VMI** (#75): a restart deletes the VMI and
/// makes a new one, and the disks are the VM's to keep across that. A VMI with
/// no VirtualMachine owns its own. `Null` when either carries
/// [`RETAIN_ANNOTATION`]: nothing owns them, so nothing deletes them.
fn disk_owner(vmi: &Value, vm: Option<&Value>) -> Value {
    let retained = |o: &Value| o["metadata"]["annotations"][RETAIN_ANNOTATION].as_str() == Some("true");
    if retained(vmi) || vm.is_some_and(retained) {
        return Value::Null;
    }
    let ns = vmi["metadata"]["namespace"].as_str().unwrap_or("default");
    let by_vm = vmi["metadata"]["ownerReferences"]
        .as_array()
        .and_then(|refs| refs.iter().find(|o| o["kind"] == "VirtualMachine"));
    match by_vm {
        Some(r) => json!({
            "kind": "VirtualMachine",
            "namespace": ns,
            "name": r["name"].as_str().unwrap_or(""),
            "uid": r["uid"].as_str().unwrap_or(""),
        }),
        None => json!({
            "kind": "VirtualMachineInstance",
            "namespace": ns,
            "name": vmi["metadata"]["name"].as_str().unwrap_or(""),
            "uid": vmi["metadata"]["uid"].as_str().unwrap_or(""),
        }),
    }
}

/// Is this volume, found under the name a disk would have, left by an
/// earlier object of the same name? A VM deleted and made again under one
/// name is a new machine, and must not boot the old one's root.
fn left_by_another(found: &Value, owner: &Value) -> bool {
    let theirs = found["owner"]["uid"].as_str().unwrap_or("");
    let ours = owner["uid"].as_str().unwrap_or("");
    !theirs.is_empty() && !ours.is_empty() && theirs != ours
}

/// The apiserver path of a disk owner, for the sweep. `None` for an owner
/// this kubelet did not write.
fn owner_path(owner: &Value) -> Option<String> {
    let plural = match owner["kind"].as_str()? {
        "VirtualMachine" => "virtualmachines",
        "VirtualMachineInstance" => "virtualmachineinstances",
        _ => return None,
    };
    let ns = owner["namespace"].as_str().filter(|n| !n.is_empty())?;
    let name = owner["name"].as_str().filter(|n| !n.is_empty())?;
    Some(format!("/apis/kubevirt.io/v1/namespaces/{ns}/{plural}/{name}"))
}

/// Has a disk's owner gone for good, from the apiserver's answer to a GET of
/// it? Only a 404, another uid (a new object under the old name) or a
/// deletionTimestamp say so. Anything else — a 5xx, a 403, no answer — keeps
/// the disk: deleting on "could not ask" is how data goes.
fn owner_gone(status: u16, obj: Option<&Value>, owner: &Value) -> bool {
    match status {
        404 => true,
        200..=299 => {
            let Some(obj) = obj else { return false };
            let uid = owner["uid"].as_str().unwrap_or("");
            let now = obj["metadata"]["uid"].as_str().unwrap_or("");
            (!uid.is_empty() && !now.is_empty() && uid != now)
                || !obj["metadata"]["deletionTimestamp"].is_null()
        }
        _ => false,
    }
}

/// Is this VMI being deleted?
fn terminating(obj: &Value) -> bool {
    !obj["metadata"]["deletionTimestamp"].is_null()
}

/// The registration's disks, from what a start resolved: every attached
/// volume, and whether this machine made it. An owned volume that is not a
/// disk is recorded too, with no device, so a stop can still delete it.
fn registered_disks(disks: &[ResolvedDisk], owned: &[String]) -> Vec<stormvm_node::console::RegisteredDisk> {
    let mut out: Vec<_> = disks
        .iter()
        .map(|d| stormvm_node::console::RegisteredDisk {
            name: d.name.clone(),
            volume_id: d.volume_id.clone(),
            owned: d.volume_id.as_ref().is_some_and(|id| owned.contains(id)),
            device: d.device.clone(),
        })
        .collect();
    for id in owned {
        if !disks.iter().any(|d| d.volume_id.as_ref() == Some(id)) {
            out.push(stormvm_node::console::RegisteredDisk {
                name: String::new(),
                volume_id: Some(id.clone()),
                owned: true,
                device: String::new(),
            });
        }
    }
    out
}

/// A machine as its registration describes it: what a kubelet that did not
/// start it (this one, before a restart) knows about it.
fn vm_of(reg: &stormvm_node::console::Registration) -> Vm {
    let disks = reg
        .disks
        .iter()
        .filter(|d| !d.device.is_empty())
        .map(|d| ResolvedDisk {
            name: d.name.clone(),
            device: d.device.clone(),
            volume_id: d.volume_id.clone(),
            readonly: false,
            bus: Default::default(),
        })
        .collect();
    Vm {
        namespace: reg.namespace.clone(),
        name: reg.name.clone(),
        uid: reg.uid.clone(),
        log_dir: format!("{LOG_ROOT}/{}_{}_{}/{}", reg.namespace, reg.name, reg.uid, reg.name),
        handle: reg.workload.map(Handle).unwrap_or(Handle::NONE),
        disks,
        phase: Phase::Running,
        exit_code: 0,
        message: String::new(),
        started_unix: reg.started,
        ready_unix: None,
        owned_volumes: reg.disks.iter().filter(|d| d.owned).filter_map(|d| d.volume_id.clone()).collect(),
        nics: Vec::new(),
        // Whatever reached its seed was decided by the kubelet that started
        // it; the agent's keys are applied again from the Secret.
        access: Access::default(),
    }
}

/// Is a hypervisor listening on its control socket? For a machine with no
/// engine handle, the only way left to tell a live one from a dead one.
///
/// A connect, not a command: QMP serves one client at a time, and a console
/// holding it must not make the machine look dead. A dead hypervisor leaves
/// its socket file behind, and a connect to it is refused.
fn control_alive(reg: &stormvm_node::console::Registration) -> bool {
    reg.control_socket
        .as_deref()
        .is_some_and(|p| std::os::unix::net::UnixStream::connect(p).is_ok())
}

/// The merge patch that adds or removes [`FINALIZER`], or `None` when the
/// object already is that way. Carries the object's resourceVersion, so it
/// applies only to the version it was computed from.
pub fn finalizer_patch(obj: &Value, present: bool) -> Option<Value> {
    let mut list: Vec<Value> = obj["metadata"]["finalizers"].as_array().cloned().unwrap_or_default();
    if list.iter().any(|f| f == FINALIZER) == present {
        return None;
    }
    if present {
        list.push(json!(FINALIZER));
    } else {
        list.retain(|f| f != FINALIZER);
    }
    let mut meta = json!({ "finalizers": list, "uid": obj["metadata"]["uid"] });
    if let Some(rv) = obj["metadata"]["resourceVersion"].as_str() {
        meta["resourceVersion"] = json!(rv);
    }
    Some(json!({ "metadata": meta }))
}

/// The bridge a VM lands on when its network does not name one.
///
/// A bridge of the node's own rather than the node's uplink: attaching taps
/// directly to the interface carrying the node's address would put the node's
/// own connectivity in the blast radius of a VM start.
const DEFAULT_BRIDGE: &str = "stormbr0";

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
    /// When the hypervisor was started, and when the guest first answered.
    ///
    /// The difference is the number people actually want — not "how long
    /// since the object was created" but "how long until this machine was
    /// usable". A VM that takes forty seconds to reach a login prompt and
    /// one that takes four are different machines, and nothing recorded it.
    pub started_unix: u64,
    pub ready_unix: Option<u64>,
    /// Volumes this machine created and therefore owns.
    ///
    /// Its root clone and its cloud-init seed. A `volume:<id>` it was handed
    /// is not here, because that one belongs to whoever made it and outlives
    /// the machine on purpose.
    pub owned_volumes: Vec<String>,
    /// The interfaces this machine was actually given.
    ///
    /// Resolved at start — name, MAC, and the binding that produced it — and
    /// then thrown away, so a running VM reported `phase` and `nodeName` and
    /// nothing else. A console could not show what network a guest was on,
    /// and neither could anyone debugging why it could not be reached.
    pub nics: Vec<NicReport>,
    /// How its SSH keys (`spec.accessCredentials`) have fared (#92).
    pub access: Access,
}

/// What became of a machine's `accessCredentials` (#92), for the
/// `AccessCredentialsSynchronized` condition and for applying agent keys again
/// only when a Secret changes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Access {
    /// Why the boot-time (`noCloud`) keys did not all reach the seed. Empty
    /// when they did, or there were none.
    pub boot: Vec<String>,
    /// Per (Secret, user), the keys the guest agent last accepted.
    pub applied: Vec<(String, String, Vec<String>)>,
    /// Why the agent's keys did not all arrive, as of the last try. `None`:
    /// the agent has not answered yet.
    pub agent: Option<Vec<String>>,
    /// The condition as last reported.
    pub condition: Option<Value>,
}

/// Whether every set of keys arrived, or everything that did not. `None` for
/// a machine that asked for none.
fn access_outcome(
    creds: &[stormvm_spec::access::AccessCredential],
    access: &Access,
) -> Option<Result<(), String>> {
    if creds.is_empty() {
        return None;
    }
    let mut problems = access.boot.clone();
    if creds.iter().any(|c| !c.at_boot()) {
        match &access.agent {
            None => problems.push("waiting for the guest agent to answer".to_string()),
            Some(p) => problems.extend(p.iter().cloned()),
        }
    }
    Some(if problems.is_empty() { Ok(()) } else { Err(problems.join("; ")) })
}

/// The condition for `outcome`, keeping `prev`'s `lastTransitionTime` when the
/// status has not changed: the time is when it became true or false, not
/// when it was last looked at.
fn access_condition(prev: Option<&Value>, outcome: &Result<(), String>, now: &str) -> Value {
    let mut c = stormvm_spec::access::condition(outcome, now);
    if let Some(p) = prev.filter(|p| p["status"] == c["status"]) {
        c["lastTransitionTime"] = p["lastTransitionTime"].clone();
    }
    c
}

/// `existing` conditions with the access one replaced (or added). A merge
/// patch replaces a list whole, so the others are carried over rather than
/// lost.
fn with_condition(existing: &Value, cond: &Value) -> Value {
    let mut out: Vec<Value> = existing
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|c| c["type"] != stormvm_spec::access::CONDITION)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    out.push(cond.clone());
    Value::Array(out)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}


/// Addresses the node has seen each of a machine's NICs use, by MAC.
///
/// Read from `/proc/net/arp`, which is the node's IPv4 neighbour table. This
/// is deliberately a weaker source than the guest agent: it only knows what
/// has been seen from this node, it is IPv4 only, and an entry can be stale.
/// It exists because the alternative for an agentless guest was showing
/// nothing, and "no address" reads as a machine with no network rather than
/// as a machine nobody has asked.
///
/// `None` when the table says nothing about any of them, so the caller can
/// fall back rather than overwrite a good answer with an empty one.
fn neighbour_addresses(vm: &Vm) -> Option<Vec<Vec<String>>> {
    let table = std::fs::read_to_string("/proc/net/arp").ok()?;
    // IP address, HW type, flags, HW address, mask, device
    let mut by_mac: std::collections::HashMap<String, Vec<String>> = Default::default();
    for line in table.lines().skip(1) {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        // Flags 0x0 is an incomplete entry: the kernel asked and nobody
        // answered, so the address it names is a guess.
        if f[2] == "0x0" {
            continue;
        }
        by_mac.entry(f[3].to_ascii_lowercase()).or_default().push(f[0].to_string());
    }
    let out: Vec<Vec<String>> = vm
        .nics
        .iter()
        .map(|n| by_mac.get(&n.mac.to_ascii_lowercase()).cloned().unwrap_or_default())
        .collect();
    out.iter().any(|v| !v.is_empty()).then_some(out)
}

/// The addresses the guest holds, per interface, from the QEMU guest agent.
///
/// `guest-network-get-interfaces` is the only thing that knows: with a bridge
/// the address comes from a DHCP server the node does not run, and with
/// masquerade it is inside the hypervisor's own stack. The socket has been
/// wired at `/run/stormvm/<ns>/<name>/agent.sock` since machines started and
/// nothing has ever read it.
///
/// `None` when there is no agent, no answer, or the guest has not got that
/// far — all of which are ordinary and none of which are worth logging on
/// every sync of every machine.
async fn guest_addresses(vm: &Vm) -> Option<Vec<Vec<String>>> {
    let sock = format!("{RUN_ROOT}/{}/{}/agent.sock", vm.namespace, vm.name);
    if !std::path::Path::new(&sock).exists() {
        return None;
    }
    let v = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stormvm_control::qga::execute(&sock, "guest-network-get-interfaces"),
    )
    .await
    .ok()?
    .ok()?;
    // Matched on MAC, not on position.
    //
    // The guest lists its interfaces in its own order, which is the kernel's
    // enumeration and not the order the spec declares them. With one NIC
    // those always agree; with two they agree until they do not, and the
    // failure is an address attributed to the wrong interface — which is
    // worse than no address, because it looks like an answer.
    //
    // Both sides have the MAC: the node generated it, the guest reports it.
    // That is an identity, so it is what is used.
    let mut by_mac: std::collections::HashMap<String, Vec<String>> = Default::default();
    for iface in v.get("return")?.as_array()? {
        let mac = iface
            .get("hardware-address")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if mac.is_empty() || mac == "00:00:00:00:00:00" {
            continue;
        }
        let addrs: Vec<String> = iface
            .get("ip-addresses")
            .and_then(|a| a.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.get("ip-address").and_then(|s| s.as_str()))
                    // Link-local tells nobody anything they can reach.
                    .filter(|s| !s.starts_with("fe80:") && !s.starts_with("169.254."))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        by_mac.insert(mac, addrs);
    }
    // In the spec's order, so the result lines up with `vm.nics`.
    Some(
        vm.nics
            .iter()
            .map(|n| by_mac.get(&n.mac.to_ascii_lowercase()).cloned().unwrap_or_default())
            .collect(),
    )
}

/// How the address reaches the guest, in one word.
///
/// The difference that matters to somebody who cannot reach their VM:
/// `bridge` puts the guest on a real network, `user` is a NAT inside the
/// hypervisor process that nothing outside it can route to, and `passt` is
/// the same shape in a userspace device. A VMI that reported only `Running`
/// gave no way to tell those apart.
fn binding_of(t: &stormvm_vmm::NicTransport) -> String {
    match t {
        stormvm_vmm::NicTransport::Tap(_) => "bridge".into(),
        stormvm_vmm::NicTransport::User { .. } => "user".into(),
        stormvm_vmm::NicTransport::Stream(_) => "passt".into(),
    }
}

/// Why a machine did not start, and whether asking again would help.
///
/// The distinction the kubelet did not have: a spec that cannot work and a
/// resource that has not arrived look identical at the call site and are not
/// remotely the same thing. One is over; the other is a pod waiting for an
/// image pull.
#[derive(Debug)]
pub enum StartFail {
    /// Something is missing that is expected to arrive. The machine stays
    /// Pending and the next sync tries again.
    Waiting(String),
    /// It did not work. Retried with backoff, Pending with the reason, unless
    /// the VM's run strategy (`Once`, `Manual`) says to give up (#76).
    Failed(String),
}

impl StartFail {
    fn message(&self) -> &str {
        match self {
            StartFail::Waiting(m) | StartFail::Failed(m) => m,
        }
    }
}

impl From<String> for StartFail {
    /// Everything that has not been classified is a failure: retried with
    /// backoff and reported each time, so a real fault is never silent.
    fn from(s: String) -> Self {
        StartFail::Failed(s)
    }
}

/// One interface, as the object should describe it.
#[derive(Debug, Clone)]
pub struct NicReport {
    pub name: String,
    pub mac: String,
    /// What the guest actually holds, asked of the guest agent.
    ///
    /// Not derivable from anything the node knows: with a bridge the address
    /// comes from a DHCP server the node does not run, and with masquerade
    /// it is inside the hypervisor's own stack. The agent is the only thing
    /// that can answer, and its socket has been wired since the machine
    /// started — nothing read it.
    pub addresses: Vec<String>,
    /// `bridge`, `user`, `passt`, … — how the address reaches the guest,
    /// which is the difference between a VM the cluster can route to and one
    /// only its own hypervisor can see.
    pub binding: String,
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
    /// The engine client, with the engine's token (#66).
    engine: crate::engine::EngineClient,
    /// The pod manager, which resolves a disk that names a claim the way it
    /// resolves a pod's (#74). `None` in tests and wherever there is no
    /// apiserver, and a claim disk then fails and says why.
    claims: Option<Arc<crate::pod_manager::PodManager>>,
    /// Events about virtual machines.
    ///
    /// A VM that will not start failed in this file, and the reason — a
    /// golden that is not on this node, a disk that would not attach —
    /// reached a log on a node with no shell and nowhere else. `describe vmi`
    /// showed nothing, which is the surface built for exactly this.
    events: Option<crate::events::EventRecorder>,
    /// Keyed by uid: a VM deleted and recreated under one name is two
    /// different machines, and treating them as one is how the second finds
    /// the first's disks.
    vms: Mutex<HashMap<String, Vm>>,
    stopping: Mutex<std::collections::HashSet<String>>,
    /// The VMIs the apiserver last gave this node, by uid.
    ///
    /// **The object is the truth; this is a cache of it.** Metadata is
    /// answered from here rather than from the local `Vm` record, because the
    /// two can diverge and the local one has no claim to be right when they
    /// do: it is derived from an object that may since have changed, on a
    /// node that may since have stopped running the machine.
    ///
    /// Replaced wholesale on every sync rather than updated by events — a
    /// cache rebuilt from the object is correct after any restart, move or
    /// partition, where one maintained by events is correct only if every
    /// event landed.
    desired: Mutex<HashMap<String, Value>>,
    disk_lifecycle: tokio::sync::RwLock<()>,
    /// What the watch last saw, when there is one.
    ///
    /// The watch maintains this and the reconcile loop reads it, so the two
    /// stay decoupled: a watch that reconnects does not disturb a
    /// reconciliation in flight, and a reconcile that takes a moment does not
    /// hold up an event.
    watched: Mutex<Option<Vec<Value>>>,
    /// Whether a sync has ever completed.
    ///
    /// A cold cache must say so. "I have not synced" and "no such machine"
    /// are different answers and only one of them is safe to act on: a guest
    /// told the second at boot configures itself as nobody.
    synced: std::sync::atomic::AtomicBool,
    /// What each running machine's bridged taps have shown the guest take
    /// (#91), by uid: the NIC's index and its watcher. A guest on a node
    /// bridge gets its address from a DHCP server the node does not run, and
    /// the tap is the one place the node sees the lease go by. Not in [`Vm`],
    /// which is cloned freely and a watcher is not.
    snoopers: std::sync::Mutex<HashMap<String, Vec<(usize, stormvm_net::Snooper)>>>,
    /// Where the watchers report a change, for [`Self::spawn_address_pump`]
    /// to write it to the VMI at once rather than on the next sync.
    snoop_tx: tokio::sync::mpsc::UnboundedSender<Snooped>,
    snoop_rx: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<Snooped>>>,
    /// Failed starts waiting to be tried again, by uid (#76).
    retries: std::sync::Mutex<HashMap<String, Retry>>,
    /// When the orphan sweep last ran (#75).
    swept: std::sync::Mutex<Option<std::time::Instant>>,
}

/// A failed start waiting for its next try (#76).
#[derive(Debug, Clone, PartialEq)]
struct Retry {
    /// The spec it failed with: a new one is tried at once.
    generation: i64,
    attempts: u32,
    next: std::time::Instant,
}

/// The wait before the next try after `attempts` failures: 10 s, doubling,
/// at most 5 min.
fn retry_delay(attempts: u32) -> std::time::Duration {
    let secs = 10u64.saturating_mul(1u64 << attempts.saturating_sub(1).min(16));
    std::time::Duration::from_secs(secs.min(300))
}

/// Does the machine behind this VMI want a failed start given up on?
///
/// Only its VirtualMachine's run strategy says so: `Once` runs once, and
/// `Manual` restarts only when asked. Anything else (`Always`,
/// `RerunOnFailure`, `running: true`), and a VMI with no VirtualMachine, is
/// retried: "Nothing should be perm." (#76). The VM is `None` when there is
/// none or it could not be read, and that retries too.
fn gives_up(vm: Option<&Value>) -> bool {
    let Some(vm) = vm else { return false };
    matches!(vm["spec"]["runStrategy"].as_str(), Some("Once") | Some("Manual"))
}

/// The VirtualMachine that owns this VMI, by name, if any.
fn owner_vm(obj: &Value) -> Option<&str> {
    obj["metadata"]["ownerReferences"]
        .as_array()?
        .iter()
        .find(|o| o["kind"] == "VirtualMachine")?["name"]
        .as_str()
}

/// A tap watcher's news: the guest behind NIC `nic` of machine `uid` now
/// holds `addresses`.
#[derive(Debug)]
struct Snooped {
    uid: String,
    nic: usize,
    addresses: Vec<String>,
}

/// Per NIC, what the tap watcher saw, over what the agent or the neighbour
/// table said (#91).
///
/// The watcher saw the lease itself, so it is right where the others are
/// stale or silent; a NIC it has seen nothing on keeps the other answer.
/// `None` only when nobody has an answer for any NIC.
fn prefer_snooped(
    other: Option<Vec<Vec<String>>>,
    snooped: Vec<Vec<String>>,
) -> Option<Vec<Vec<String>>> {
    if snooped.iter().all(|s| s.is_empty()) {
        return other;
    }
    let mut out = other.unwrap_or_default();
    if out.len() < snooped.len() {
        out.resize(snooped.len(), Vec::new());
    }
    for (slot, s) in out.iter_mut().zip(snooped) {
        if !s.is_empty() {
            *slot = s;
        }
    }
    Some(out)
}

impl VmManager {
    pub fn new(
        ring: Option<Arc<RingClient>>,
        node_name: impl Into<String>,
        api: reqwest::Client,
        api_url: impl Into<String>,
    ) -> VmManager {
        let node_name = node_name.into();
        let api_url = api_url.into().trim_end_matches('/').to_string();
        let events = (!api_url.is_empty())
            .then(|| crate::events::EventRecorder::new(api.clone(), &api_url, &node_name));
        let (snoop_tx, snoop_rx) = tokio::sync::mpsc::unbounded_channel();
        VmManager {
            ring,
            storage: crate::engine::DEFAULT_URL.into(),
            node_name,
            api,
            api_url,
            engine: crate::engine::EngineClient::default(),
            claims: None,
            events,
            vms: Mutex::new(HashMap::new()),
            stopping: Mutex::new(Default::default()),
            desired: Mutex::new(HashMap::new()),
            disk_lifecycle: tokio::sync::RwLock::new(()),
            watched: Mutex::new(None),
            synced: std::sync::atomic::AtomicBool::new(false),
            snoopers: std::sync::Mutex::new(HashMap::new()),
            snoop_tx,
            snoop_rx: std::sync::Mutex::new(Some(snoop_rx)),
            retries: std::sync::Mutex::new(HashMap::new()),
            swept: std::sync::Mutex::new(None),
        }
    }

    /// Write a tap watcher's news to the VMI as it arrives (#91): "within
    /// seconds of the guest's DHCP", not on the next sync. Once; later calls
    /// do nothing.
    pub fn spawn_address_pump(self: &Arc<Self>) {
        let rx = self.snoop_rx.lock().unwrap_or_else(|e| e.into_inner()).take();
        let Some(mut rx) = rx else { return };
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            while let Some(news) = rx.recv().await {
                let Some(me) = me.upgrade() else { return };
                me.snooped(news).await;
            }
        });
    }

    /// One NIC's addresses changed on the tap. An empty list (a release, an
    /// expiry) is left to the sync, which falls back to the other sources.
    /// News for a machine not recorded yet (a lease in the instant before
    /// `start` records it) is not lost: the sync reads the watcher too.
    async fn snooped(&self, news: Snooped) {
        if news.addresses.is_empty() {
            return;
        }
        let updated = {
            let mut vms = self.vms.lock().await;
            let Some(vm) = vms.get_mut(&news.uid) else { return };
            if vm.phase.terminal() {
                return;
            }
            let Some(nic) = vm.nics.get_mut(news.nic) else { return };
            if nic.addresses == news.addresses {
                return;
            }
            info!(vm = %vm.name, nic = %nic.name, addresses = ?news.addresses, "guest address seen on its tap");
            nic.addresses = news.addresses;
            if vm.ready_unix.is_none() {
                vm.ready_unix = Some(now_unix());
            }
            vm.clone()
        };
        self.patch_status(&updated).await;
    }

    /// What each NIC's tap watcher has seen, empty where there is none.
    fn snooped_addresses(&self, uid: &str, nics: usize) -> Vec<Vec<String>> {
        let mut out = vec![Vec::new(); nics];
        let map = self.snoopers.lock().unwrap_or_else(|e| e.into_inner());
        for (i, s) in map.get(uid).into_iter().flatten() {
            if let Some(slot) = out.get_mut(*i) {
                *slot = s.addresses();
            }
        }
        out
    }

    /// A Secret's SSH public keys, in the machine's namespace (#92).
    async fn secret_keys(&self, ns: &str, name: &str) -> Result<Vec<String>, String> {
        if self.api_url.is_empty() {
            return Err(format!("Secret {name}: no apiserver to read it from"));
        }
        let url = format!("{}/api/v1/namespaces/{ns}/secrets/{name}", self.api_url);
        let r = self.api.get(&url).send().await.map_err(|e| format!("Secret {name}: {e}"))?;
        match r.status().as_u16() {
            200 => {}
            404 => return Err(format!("Secret {ns}/{name} not found")),
            code => return Err(format!("Secret {ns}/{name}: {code}")),
        }
        let secret: Value = r.json().await.map_err(|e| format!("Secret {name}: {e}"))?;
        Ok(stormvm_spec::access::keys_in_secret(&secret))
    }

    /// The keys for the seed, from every `noCloud` / `configDrive`
    /// credential, and why any set could not be had.
    async fn boot_keys(&self, vm: &VmSpec) -> (Vec<String>, Vec<String>) {
        let (mut keys, mut problems) = (Vec::new(), Vec::new());
        let has_seed = vm.disks.iter().any(|d| d.from == DiskSource::CloudInit);
        for c in vm.access_credentials.iter().filter(|c| c.at_boot()) {
            if !has_seed {
                // KubeVirt refuses this shape; here the machine still runs,
                // and the condition says why the key is not in it.
                problems.push(format!(
                    "Secret {}: noCloud keys ride the cloud-init seed, and this VMI has no \
                     cloudInitNoCloud volume",
                    c.secret
                ));
                continue;
            }
            match self.secret_keys(&vm.namespace, &c.secret).await {
                Ok(k) if k.is_empty() => problems.push(format!("Secret {} holds no keys", c.secret)),
                Ok(k) => {
                    for key in k {
                        if !keys.contains(&key) {
                            keys.push(key);
                        }
                    }
                }
                Err(e) => problems.push(e),
            }
        }
        (keys, problems)
    }

    /// Keep a running machine's agent-delivered keys current, and its
    /// condition with them (#92).
    ///
    /// Read from the VMI as it is now, so a credential added to a running
    /// machine ("Add my keys") and a machine adopted after a restart are both
    /// covered. Each Secret is read on every sync, and a user's keys are sent
    /// to the agent only when they differ from what it last accepted, with
    /// `reset`: the Secret is the truth, and a key taken out of it leaves the
    /// guest. A Secret that is missing, or holds no keys, is reported and
    /// leaves the guest's keys as they are: emptying a machine's
    /// `authorized_keys` because a Secret went missing is a lock-out.
    async fn sync_access(&self, uid: &str, agent_up: bool) {
        let Some(obj) = self.desired.lock().await.get(uid).cloned() else { return };
        // Refused credentials refused the machine at start.
        let Ok(creds) = stormvm_spec::access::from_kube(&obj["spec"]) else { return };
        let Some(vm) = self.vms.lock().await.get(uid).cloned() else { return };
        let mut access = vm.access.clone();

        let agent_creds: Vec<(&str, &[String])> = creds
            .iter()
            .filter_map(|c| match &c.propagation {
                stormvm_spec::access::Propagation::GuestAgent { users } => {
                    Some((c.secret.as_str(), users.as_slice()))
                }
                _ => None,
            })
            .collect();
        if agent_up && !agent_creds.is_empty() {
            let sock = format!("{RUN_ROOT}/{}/{}/agent.sock", vm.namespace, vm.name);
            let (mut applied, mut problems) = (Vec::new(), Vec::new());
            for (secret, users) in agent_creds {
                let keep = |applied: &mut Vec<(String, String, Vec<String>)>| {
                    applied.extend(vm.access.applied.iter().filter(|a| a.0 == secret).cloned());
                };
                let keys = match self.secret_keys(&vm.namespace, secret).await {
                    Ok(k) if k.is_empty() => {
                        problems.push(format!("Secret {secret} holds no keys; the guest's were left as they are"));
                        keep(&mut applied);
                        continue;
                    }
                    Ok(k) => k,
                    Err(e) => {
                        problems.push(e);
                        keep(&mut applied);
                        continue;
                    }
                };
                for user in users {
                    let sent = vm.access.applied.iter().any(|(s, u, k)| s == secret && u == user && *k == keys);
                    if sent {
                        applied.push((secret.to_string(), user.clone(), keys.clone()));
                        continue;
                    }
                    match stormvm_control::qga::set_authorized_keys(&sock, user, &keys).await {
                        Ok(()) => {
                            info!(vm = %vm.name, user = %user, secret = %secret, keys = keys.len(),
                                  "authorized keys set through the guest agent");
                            applied.push((secret.to_string(), user.clone(), keys.clone()));
                        }
                        Err(e) => problems.push(format!("user {user} (Secret {secret}): {e}")),
                    }
                }
            }
            access.applied = applied;
            access.agent = Some(problems);
        }
        let outcome = access_outcome(&creds, &access);
        access.condition = outcome.map(|o| access_condition(access.condition.as_ref(), &o, &now_rfc3339()));
        if access == vm.access {
            return;
        }
        let updated = {
            let mut vms = self.vms.lock().await;
            let Some(cur) = vms.get_mut(uid) else { return };
            cur.access = access;
            cur.clone()
        };
        self.patch_status(&updated).await;
    }

    /// Stop watching a machine's taps. Off the async threads: dropping a
    /// watcher joins its thread, which wakes at most a second later.
    fn drop_snoopers(&self, uid: &str) {
        let gone = self.snoopers.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
        if let Some(gone) = gone {
            tokio::task::spawn_blocking(move || drop(gone));
        }
    }

    /// Emit an event about a machine, if there is a recorder.
    async fn event(&self, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.pod_event(obj, etype, reason, message).await;
        }
    }

    /// The same, for a machine known only by its record.
    ///
    /// The end-of-life path holds a `Vm` rather than the object it came from,
    /// and that is the event most worth having: a machine that exits is the
    /// one somebody is asking about. The recorder needs namespace, name and
    /// uid, which the record has.
    async fn event_of(&self, vm: &Vm, etype: &str, reason: &str, message: &str) {
        let obj = serde_json::json!({
            "metadata": {
                "namespace": vm.namespace,
                "name": vm.name,
                "uid": vm.uid,
            }
        });
        self.event(&obj, etype, reason, message).await;
    }

    /// Resolve claim disks through the pod manager (#74).
    pub fn with_claims(mut self, pods: Arc<crate::pod_manager::PodManager>) -> VmManager {
        self.claims = Some(pods);
        self
    }

    /// The node's engine, shared with the rest of the kubelet.
    pub fn with_storage(mut self, engine: crate::engine::EngineClient) -> VmManager {
        self.storage = engine.url().to_string();
        self.engine = engine;
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
    /// What the watch has, if a watch is running.
    ///
    /// `None` means no watch has delivered anything yet and the caller should
    /// list instead — a fresh kubelet must not sit idle waiting for an event
    /// that only fires when something *changes*.
    pub async fn watched(&self) -> Option<Vec<Value>> {
        self.watched.lock().await.clone()
    }

    /// The watch's view, from the watch task.
    pub async fn set_watched(&self, objs: Vec<Value>) {
        *self.watched.lock().await = Some(objs);
    }

    /// Startup adoption records facts only; cleanup belongs to the UID worker.
    pub async fn adopt_registered(&self) {
        let mut records = self.vms.lock().await;
        for reg in stormvm_node::console::list(RUN_ROOT) {
            if !reg.uid.is_empty() {
                records.entry(reg.uid.clone()).or_insert_with(|| vm_of(&reg));
            }
        }
    }

    pub async fn cache_specs(&self, objects: &[Value]) {
        *self.desired.lock().await = objects.iter().filter_map(|o|
            o["metadata"]["uid"].as_str().map(|uid| (uid.into(), o.clone()))).collect();
        self.synced.store(true, std::sync::atomic::Ordering::Release);
    }

    pub async fn has_unknown_claims(&self) -> bool {
        let keys: Vec<_> = self.vms.lock().await.keys().cloned().collect();
        let desired = self.desired.lock().await;
        keys.iter().any(|uid| !desired.contains_key(uid))
    }

    /// Only this UID is observed or mutated. The common executor protects its
    /// name/claims, including while a terminating predecessor retains disks.
    pub async fn reconcile_one(&self, uid: &str, object: Option<&Value>) -> anyhow::Result<bool> {
        if object.map_or(true, terminating) {
            let vm = self.vms.lock().await.get(uid).cloned();
            if let Some(vm) = vm {
                anyhow::ensure!(self.stop(&vm).await, "VM cleanup pending for {uid}");
                self.vms.lock().await.remove(uid);
            }
            self.retries.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
            if let Some(object) = object { anyhow::ensure!(self.set_finalizer(object, false).await,"VM finalizer cleanup pending"); }
            return Ok(true);
        }
        let object = object.unwrap();
        self.absorb_ends_for(Some(uid)).await;
        if !self.vms.lock().await.contains_key(uid) {
            self.start_with_retry(uid, object).await;
        }
        let running = self.vms.lock().await.get(uid).is_some_and(|vm| !vm.phase.terminal());
        if !running { return Ok(false); }
        self.set_finalizer(object, true).await;
        Ok(false)
    }

    pub async fn sync(&self, desired: &[Value]) {
        self.absorb_ends().await;

        let mut want: Vec<(String, Value)> = Vec::new();
        let mut going: Vec<(String, Value)> = Vec::new();
        for obj in desired {
            let uid = obj["metadata"]["uid"].as_str().unwrap_or("").to_string();
            if uid.is_empty() {
                warn!("a VirtualMachineInstance with no uid was skipped");
                continue;
            }
            // Being deleted is not wanted: its machine stops now, and the
            // object goes once it has (the finalizer).
            if terminating(obj) {
                going.push((uid, obj.clone()));
            } else {
                want.push((uid, obj.clone()));
            }
        }
        // The cache of record, replaced rather than merged.
        {
            let mut d = self.desired.lock().await;
            *d = want.iter().map(|(u, o)| (u.clone(), o.clone())).collect();
        }
        self.synced.store(true, std::sync::atomic::Ordering::Relaxed);

        // Machines running here that this process does not know: started
        // before a restart, or by a kubelet that kept no record. Adopted when
        // their object still wants them, stopped when it does not.
        self.reconcile_registered(&want).await;

        // Stops before starts. A deleted VMI's replacement derives the same
        // tap name, and a tap exists while any process holds its descriptor —
        // so starting first raced the old hypervisor's exit and failed with
        // EBUSY on a tap that was already dying.
        let keep: Vec<String> = want.iter().map(|(u, _)| u.clone()).collect();
        let gone: Vec<Vm> = {
            let vms = self.vms.lock().await;
            vms.values().filter(|v| !keep.contains(&v.uid)).cloned().collect()
        };
        for vm in gone {
            if self.stop(&vm).await {
                self.vms.lock().await.remove(&vm.uid);
            }
        }
        // The machine is gone, so its object may go too.
        for (uid, obj) in &going {
            if !self.vms.lock().await.contains_key(uid) {
                self.set_finalizer(obj, false).await;
            }
        }

        // Disks whose VirtualMachine (or VMI) has gone for good (#75). After
        // the stops, so a machine just stopped no longer holds them; before
        // the starts, so a disk left by an earlier VM of the same name is
        // gone before its successor looks for it.
        self.sweep_orphans().await;

        // A retry for a machine no longer wanted is forgotten.
        self.retries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|u, _| want.iter().any(|(w, _)| w == u));

        for (uid, obj) in &want {
            if self.vms.lock().await.contains_key(uid) {
                continue;
            }
            self.start_with_retry(uid, obj).await;
        }

        // Every machine running here holds its object until it is stopped.
        // Each pass rather than once at start: a status write moves the
        // resourceVersion, and the add is guarded by it.
        let running: std::collections::HashSet<String> = {
            let vms = self.vms.lock().await;
            vms.values().filter(|v| !v.phase.terminal()).map(|v| v.uid.clone()).collect()
        };
        for (uid, obj) in &want {
            if running.contains(uid) {
                self.set_finalizer(obj, true).await;
            }
        }
    }

    /// Main's failed-start policy shared by the UID adapter and legacy tests.
    async fn start_with_retry(&self, uid: &str, obj: &Value) {
        // Backing off after a failed start (#76), unless the spec changed.
        let generation = obj["metadata"]["generation"].as_i64().unwrap_or(0);
        {
            let mut retries = self.retries.lock().unwrap_or_else(|e| e.into_inner());
            match retries.get(uid) {
                Some(r) if r.generation != generation => {
                    retries.remove(uid);
                }
                Some(r) if std::time::Instant::now() < r.next => return,
                _ => {}
            }
        }
        let result = self.start(uid, obj).await;
        if result.is_ok() {
            self.retries.lock().unwrap_or_else(|e| e.into_inner()).remove(uid);
        }
        if let Err(e) = result {
            let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
            let name = obj["metadata"]["name"].as_str().unwrap_or("");
            if let StartFail::Waiting(why) = &e {
                // Pending, and nothing is recorded in `vms` — which is
                // what lets the next sync try again. Recording it is what
                // made a missing golden permanent: sync saw the uid and
                // skipped it for ever.
                info!("{ns}/{name}: {why}");
                self.event(obj, "Normal", "Waiting", why).await;
                self.patch_pending(ns, name, uid, why).await;
                return;
            }
            let e = e.message().to_string();
            warn!("{ns}/{name}: {e}");
            // Given up on only when its VirtualMachine says so; otherwise
            // Pending, with the reason and when it will be tried again.
            // A failure recorded here used to be skipped for good, so a
            // stormblock that was down for a moment left the machine dead
            // until somebody recreated it (#76).
            let owner = match owner_vm(obj) {
                Some(vm) => self.get_vm(ns, vm).await,
                None => None,
            };
            if gives_up(owner.as_ref()) {
                // The reason, where somebody will look for it.
                //
                // This is the message that said `cloning golden
                // fedora-43-x86_64 for disk root: 404 no volume` and went
                // only to a log on a node with no shell.
                self.event(obj, "Warning", "FailedStart", &e).await;
                self.record_failure(uid, obj, &e).await;
                return;
            }
            let (attempts, wait) = {
                let mut retries = self.retries.lock().unwrap_or_else(|e| e.into_inner());
                let r = retries.entry(uid.to_string()).or_insert(Retry {
                    generation,
                    attempts: 0,
                    next: std::time::Instant::now(),
                });
                r.attempts += 1;
                let wait = retry_delay(r.attempts);
                r.next = std::time::Instant::now() + wait;
                (r.attempts, wait)
            };
            let why = format!(
                "start failed (attempt {attempts}), retrying in {}s: {e}",
                wait.as_secs()
            );
            self.event(obj, "Warning", "FailedStart", &why).await;
            self.patch_retrying(ns, name, uid, &why).await;
        }
    }

    /// What this node is running, for `/pods`-style introspection and tests.
    pub async fn running(&self) -> Vec<Vm> {
        self.vms.lock().await.values().cloned().collect()
    }


    /// A copy of the machine with any `userDataSecretRef` turned into inline
    /// `userData`.
    ///
    /// Returns the object unchanged when there is no reference, when the
    /// secret cannot be read, or when it holds nothing usable -- a machine
    /// that starts without its seed is bad, and a machine that does not start
    /// at all because a secret was briefly unavailable is worse. The failure
    /// is logged rather than swallowed, because "no login" needs a reason
    /// somewhere.
    ///
    /// `stringData` as well as `data`: the apiserver is supposed to fold the
    /// first into the second on write, and rustkube does not, so a secret
    /// created the way Kubernetes documents comes back as it was written.
    async fn with_seed(&self, obj: &Value) -> Value {
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
        let Some(vols) = obj.pointer("/spec/volumes").and_then(Value::as_array) else {
            return obj.clone();
        };
        let refs: Vec<(usize, String)> = vols
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                v.pointer("/cloudInitNoCloud/userDataSecretRef/name")
                    .and_then(Value::as_str)
                    .map(|n| (i, n.to_string()))
            })
            .collect();
        if refs.is_empty() || self.api_url.is_empty() {
            return obj.clone();
        }
        let mut out = obj.clone();
        for (i, name) in refs {
            let url = format!(
                "{}/api/v1/namespaces/{ns}/secrets/{name}",
                self.api_url.trim_end_matches('/')
            );
            let secret = match self.api.get(&url).send().await {
                Ok(r) => match r.json::<Value>().await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(secret = %name, error = %e, "cloud-init seed: unreadable secret");
                        continue;
                    }
                },
                Err(e) => {
                    warn!(secret = %name, error = %e, "cloud-init seed: secret not fetched");
                    continue;
                }
            };
            let Some(text) = seed_text(&secret) else {
                warn!(secret = %name, "cloud-init seed: secret has no userdata");
                continue;
            };
            out["spec"]["volumes"][i]["cloudInitNoCloud"]["userData"] = Value::String(text);
            // The reference has been honoured; leaving it would let a later
            // reader resolve it a second time.
            if let Some(m) = out["spec"]["volumes"][i]["cloudInitNoCloud"].as_object_mut() {
                m.remove("userDataSecretRef");
            }
        }
        out
    }

    /// Start a machine; a start that fails deletes the volumes it created.
    ///
    /// A golden's clone and a cloud-init seed are made new on every start,
    /// and a failed one used to only detach them. Once a failed start is
    /// retried (#76), that is two volumes left behind per attempt. What it
    /// found and reused (an `emptyDisk`, a claim, a `volume:`) is not
    /// deleted: that may be the guest's data.
    async fn start(&self, uid: &str, obj: &Value) -> Result<(), StartFail> {
        let _disks = self.disk_lifecycle.read().await;
        let mut fresh = Vec::new();
        let result = self.start_attempt(uid, obj, &mut fresh).await;
        if result.is_err() {
            self.delete_volumes(obj["metadata"]["name"].as_str().unwrap_or(""), &fresh).await;
        }
        result
    }

    async fn start_attempt(&self, uid: &str, obj: &Value, fresh: &mut Vec<String>) -> Result<(), StartFail> {
        // Resolve the cloud-init secret before anything reads the spec.
        //
        // A seed may be referenced rather than inlined -- `userDataSecretRef`
        // -- because the payload is where SSH keys and passwords live and a
        // VMI spec is readable by anyone with get on virtualmachineinstances.
        // Nothing resolved it: stormvm reads `cloudInitNoCloud.userData` and
        // only that, so a machine whose seed was a reference booted with **no
        // cloud-init at all** -- no key, no user, no hostname -- and the only
        // symptom was a guest nobody could log into, which reads as a broken
        // image rather than a missing indirection.
        //
        // Resolved here because this is the last place that has both an
        // apiserver client and the object: the secret is fetched by the
        // kubelet and inlined into the spec it hands to the engine, so the
        // payload still never travels in the VMI.
        let obj = &self.with_seed(obj).await;
        let vm: VmSpec = stormvm_spec::kube::from_kube(obj).map_err(|e| e.to_string())?;
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
        let ring = self.ring.clone().ok_or_else(|| {
            "no ring to stormpump: only the engine starts a machine".to_string()
        })?;

        // SSH keys that ride the seed (#92), fetched before the seed is made.
        // A Secret that cannot be read does not stop the machine: the key may
        // be in its user-data too, and the condition says what is missing.
        let (boot_keys, boot_problems) = self.boot_keys(&vm).await;
        // Storage first. Nothing has been asked of the engine yet, so a golden
        // that does not exist costs a failed status and no cleanup.
        let owner = self.owner_of(obj).await;
        let (disks, owned_volumes) = self.resolve_disks_with_keys(&vm, &boot_keys, &owner, fresh).await?;

        // The pod log directory, because that is where `kubectl logs` looks.
        // The container name is the VM's, so the path is the one the kubelet's
        // own log handler builds for a container of the same name.
        let dir = format!("{LOG_ROOT}/{ns}_{}_{uid}/{}", vm.name, vm.name);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            self.release(&disks).await;
            return Err(StartFail::Failed(format!("could not make {dir}: {e}")));
        }
        // NICs, before the plan: the tap has to exist so its descriptor can
        // be named, and it has to be deposited so the engine can find it by
        // that name.
        // The tap watchers come back too, held here until the machine is
        // recorded: a start that fails below drops them with it.
        let (nics, nic_reports, snoopers) = match self.resolve_nics(uid, &ns, &vm, &ring).await {
            Ok(n) => n,
            Err(e) => {
                self.release(&disks).await;
                return Err(StartFail::Failed(e));
            }
        };

        let logging = Logging::pod(&dir, 0);

        // Register the machine for the console doors, before the spawn
        // (rustkube-node#38).
        //
        // stormvm's console daemon resolves a VM out of the run directory
        // rather than out of an apiserver — it is a different process from
        // whatever started the machine and has to answer "where is
        // default/web-1's serial socket" across its own restart, with no
        // cluster to ask. `stormvm start` writes this file; the kubelet did
        // not, so a VM the kubelet started was invisible: `/api/v1/vms` empty
        // and every attach a 404.
        //
        // Before the spawn, and from the same `logging` and run dir the plan
        // is built from, so it describes *this* machine rather than a second
        // reading of the spec. The console reports a door open only when the
        // socket is actually there, so a registration that precedes the
        // hypervisor is right: it says what was asked for, and the socket says
        // whether the machine got that far.
        let registration = stormvm_node::console::Registration::of(&vm, uid, &logging, RUN_ROOT);
        if let Err(e) = stormvm_node::console::write(RUN_ROOT, &registration) {
            // Not fatal: a machine that runs without a console door is worse
            // than one that does not run at all only to whoever wanted the
            // door. Warned rather than swallowed, because the symptom at the
            // console end is a 404 with nothing to say why.
            warn!(vm = %vm.name, "could not register for the console doors: {e}");
        }

        let built = match plan::build_with_nics(&vm, &disks, &nics, &logging, RUN_ROOT) {
            Ok(p) => p,
            Err(e) => {
                deregister(&vm.namespace, &vm.name);
                self.release(&disks).await;
                return Err(StartFail::Failed(format!("{e:#}")));
            }
        };

        // The run directory, from the plan rather than rebuilt here.
        //
        // It is `<RUN_ROOT>/<ns>/<name>` now that a VM is qualified by its
        // namespace (stormvm#6), and the plan has already told the hypervisor
        // to bind its sockets there. A second `format!` that disagreed would
        // leave qemu binding into a directory nobody made, and the failure
        // names neither the path nor the reason — which is why the plan
        // carries it out rather than expecting it to be derived twice.
        //
        // After the plan, therefore, not before.
        if let Err(e) = std::fs::create_dir_all(&built.run_dir) {
            deregister(&vm.namespace, &vm.name);
            self.release(&disks).await;
            return Err(StartFail::Failed(format!("could not make {}: {e}", built.run_dir)));
        }

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
                deregister(&vm.namespace, &vm.name);
                self.release(&disks).await;
                return Err(StartFail::Failed(format!("stormpump refused: {e:?}")));
            }
        };

        info!(vm = %vm.name, namespace = %ns, ?handle, "vm started");
        // The record a restarted kubelet finds it by (#35): the engine keeps
        // the machine across a restart, and this process's memory does not.
        // Without the handle, deleting its object stopped nothing.
        let mut reg = registration.clone().running_as(handle.0);
        reg.disks = registered_disks(&disks, &owned_volumes);
        if let Err(e) = stormvm_node::console::write(RUN_ROOT, &reg) {
            warn!(vm = %vm.name, "could not record the running machine's handle: {e}");
        }
        self.event(obj, "Normal", "Started",
                   &format!("Started virtual machine {}", vm.name)).await;
        let mut rec = Vm {
            namespace: ns,
            name: vm.name.clone(),
            uid: uid.to_string(),
            log_dir: dir.clone(),
            handle,
            disks,
            phase: Phase::Running,
            exit_code: 0,
            started_unix: now_unix(),
            ready_unix: None,
            owned_volumes,
            nics: nic_reports,
            message: String::new(),
            access: Access { boot: boot_problems, ..Access::default() },
        };
        if let Some(outcome) = access_outcome(&vm.access_credentials, &rec.access) {
            rec.access.condition = Some(access_condition(None, &outcome, &now_rfc3339()));
        }
        self.vms.lock().await.insert(uid.to_string(), rec.clone());
        if !snoopers.is_empty() {
            self.snoopers.lock().unwrap_or_else(|e| e.into_inner()).insert(uid.to_string(), snoopers);
        }
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
    async fn absorb_ends(&self) { self.absorb_ends_for(None).await; }

    async fn absorb_ends_for(&self, uid: Option<&str>) {
        let Some(ring) = self.ring.clone() else { return };
        let live: Vec<Vm> = {
            let vms = self.vms.lock().await;
            vms.values().filter(|v| !v.phase.terminal() && uid.map_or(true, |uid| uid == v.uid)).cloned().collect()
        };
        for vm in live {
            // Ask the guest what address it has, and report it when it
            // changes.
            //
            // Best-effort on purpose: a guest with no agent, or one still
            // booting, simply has no answer, and that is not a fault to
            // report. A short timeout because this runs on every sync and a
            // hung agent must not hold the loop.
            // The guest agent first, the node's own neighbour table second.
            //
            // A guest with no agent has no answer about itself, and for a
            // bridged machine that left the row with no address at all -- the
            // one field somebody actually wants, missing on every guest
            // without qemu-guest-agent, which includes anything mid-install
            // and most images that are not cloud images.
            //
            // The node can answer instead: the machine is on a bridge it
            // owns, so once the guest has spoken to anything its MAC is in
            // the neighbour table with the address it took. Second, not
            // first: the agent knows every address on every interface, and
            // the neighbour table knows only what has been seen from here.
            let agent = guest_addresses(&vm).await;
            // An answer, even one with no addresses, is an agent that can
            // take keys (#92).
            let agent_up = agent.is_some();
            let addrs = match agent {
                Some(a) if a.iter().any(|v| !v.is_empty()) => Some(a),
                other => neighbour_addresses(&vm).or(other),
            };
            // The tap watcher first of all (#91): it saw the lease, on a
            // bridge where the agent may be absent and the neighbour table
            // knows only what the node has talked to.
            let addrs = prefer_snooped(addrs, self.snooped_addresses(&vm.uid, vm.nics.len()));
            if let Some(addrs) = addrs {
                let changed = vm.nics.iter().map(|n| &n.addresses).ne(addrs.iter());
                // The agent answering *is* the readiness signal: it runs in
                // the guest, so a reply means the guest booted far enough to
                // start it. Recorded once — the first answer is the boot, and
                // every one after it is just the machine still running.
                let first_answer = vm.ready_unix.is_none();
                if changed || first_answer {
                    let mut with = vm.clone();
                    if first_answer {
                        with.ready_unix = Some(now_unix());
                    }
                    for (n, a) in with.nics.iter_mut().zip(addrs.into_iter()) {
                        n.addresses = a;
                    }
                    self.vms.lock().await.insert(with.uid.clone(), with.clone());
                    self.patch_status(&with).await;
                }
            }
            // Its SSH keys through the agent, and the condition (#92).
            self.sync_access(&vm.uid, agent_up).await;
            let code = if vm.handle.is_none() {
                // Adopted without a handle: its control socket is the only
                // sign of life, and there is no exit status to read.
                match stormvm_node::console::find(RUN_ROOT, &vm.namespace, &vm.name) {
                    Some(reg) if control_alive(&reg) => continue,
                    _ => continue,
                }
            } else {
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
                if signal != 0 { 128 + signal } else { (status >> 8) & 0xff }
            };
            let mut done = vm.clone();
            done.phase = if code == 0 { Phase::Succeeded } else { Phase::Failed };
            done.exit_code = code;
            done.message = match (code, hypervisor_said(&vm.log_dir)) {
                (-1, _) => "the hypervisor ended; its exit status is unknown (it was started by an \
                            earlier kubelet, which kept no handle)"
                    .to_string(),
                // **What it said, not that it failed.** "the hypervisor exited
                // with 1" is true and useless: the reason is in a file on a
                // node with no shell, and reading it has cost this stack whole
                // rebuild-and-boot cycles more than once.
                (c, Some(said)) if c != 0 => format!("the hypervisor exited with {c}: {said}"),
                (c, _) => format!("the hypervisor exited with {c}"),
            };
            info!(vm = %done.name, code, "vm ended");
            // A machine that exits 0 asked to; anything else did not.
            if code == 0 {
                self.event_of(&done, "Normal", "Stopped",
                              &format!("Virtual machine {} stopped", done.name)).await;
            } else {
                self.event_of(&done, "Warning", "Failed", &done.message).await;
            }
            self.drop_snoopers(&done.uid);
            // Retain the handle and disks for checked teardown. Releasing the
            // handle here loses the authoritative exit record before cleanup.
            self.vms.lock().await.insert(done.uid.clone(), done.clone());
            self.patch_status(&done).await;
        }
    }

    /// Keep the record and finalizer until exit and every cleanup operation
    /// are confirmed. An unavailable query is not evidence of an exit.
    async fn stop(&self, vm: &Vm) -> bool {
        if vm.handle.is_none() && !vm.phase.terminal() {
            // Old registrations have no authoritative engine identity. A
            // missing/unreachable control socket alone cannot prove exit.
            self.stop_by_control(vm).await;
            warn!(vm = %vm.name, "cannot confirm exit without an engine handle; retaining disks and registration");
            return false;
        }
        if !vm.handle.is_none() && !vm.phase.terminal() {
            let Some(ring) = self.ring.clone() else { return false };
            let handle = vm.handle;
            let r = ring.clone();
            let observed = tokio::task::spawn_blocking(move || r.query(handle)).await;
            let Ok(Ok(cqe)) = observed else { return false };
            if !exited(cqe.aux) {
                if self.stopping.lock().await.insert(vm.uid.clone()) {
                    let r = ring.clone();
                    if !matches!(tokio::task::spawn_blocking(move || r.stop(handle, 30)).await, Ok(Ok(_))) {
                        self.stopping.lock().await.remove(&vm.uid);
                    }
                }
                // The engine owns the grace/kill deadline. Yield this worker
                // while it runs so eight stopping guests do not occupy the pool.
                return false;
            }
        }

        // Do not forget a refused detach (including HTTP 409), or delete its
        // volume while the engine still holds it. Every operation is retryable.
        for disk in &vm.disks {
            let Some(id) = &disk.volume_id else { continue };
            let url = format!("{}/api/v1/volumes/{id}/attach", self.storage);
            match self.engine.delete(&url).await {
                Ok(r) if r.status().is_success() || r.status().as_u16() == 404 => {}
                _ => return false,
            }
        }
        // Disk lifetime follows the VM owner (#75), not this VMI. The orphan
        // sweep deletes them only after the owner is confirmed gone.
        self.drop_snoopers(&vm.uid);
        if let Some(ring) = self.ring.clone().filter(|_| !vm.handle.is_none()) {
            let handle = vm.handle;
            if !matches!(tokio::task::spawn_blocking(move || ring.workload_release(handle)).await, Ok(Ok(_))) {
                return false;
            }
        }
        deregister(&vm.namespace, &vm.name);
        self.stopping.lock().await.remove(&vm.uid);
        info!(vm = %vm.name, "vm stopped");
        true
    }

    /// [`Self::resolve_disks_with_keys`] with no keys: for tests.
    #[cfg(test)]
    async fn resolve_disks(&self, vm: &VmSpec) -> Result<(Vec<ResolvedDisk>, Vec<String>), StartFail> {
        self.resolve_disks_with_keys(vm, &[], &Value::Null, &mut Vec::new()).await
    }

    /// Clone or attach every disk. Failure gives back what it already took —
    /// an attachment left behind is a device nobody will ever release.
    /// `keys` go into a cloud-init seed's `public-keys` (#92). Every volume
    /// made new here is added to `fresh`, as it is made, so a start that
    /// fails later can delete it (#76).
    ///
    /// A disk the machine owns is found by name before it is made (#75): a
    /// restart attaches the root the guest left, and the golden is needed
    /// only the first time. `owner` goes on each ([`disk_owner`]).
    async fn resolve_disks_with_keys(
        &self,
        vm: &VmSpec,
        keys: &[String],
        owner: &Value,
        fresh: &mut Vec<String>,
    ) -> Result<(Vec<ResolvedDisk>, Vec<String>), StartFail> {
        let mut done: Vec<ResolvedDisk> = Vec::new();
        let mut owned: Vec<String> = Vec::new();
        for d in &vm.disks {
            let volume_id = match &d.from {
                DiskSource::Golden(g) => {
                    // Both qualified by namespace, through stormvm's own
                    // definitions so the standalone and cluster paths cannot
                    // disagree. stormblock's namespace is flat and a VM's is
                    // not: `default/web-1` and `staging/web-1` both used to
                    // ask for a volume called `web-1-root`, and
                    // `clone_volume` does not check uniqueness — so there
                    // were two distinct volumes under one name and
                    // `volume_by_name` returned whichever the map iterated
                    // first.
                    let name = stormvm_node::start::volume_name(vm, &d.name);
                    match self.reuse(&d.name, &name, owner).await {
                        Ok(Some(id)) => id,
                        Ok(None) => {
                    let mut body = json!({
                        "name": name,
                        "size": d.size,
                        "label": format!("storm.io/vm={}", vm.id()),
                        "verify": true,
                    });
                    if !owner.is_null() {
                        body["owner"] = owner.clone();
                    }
                    match self
                        .post(&format!("{}/api/v1/volumes/{g}/clone", self.storage), &body)
                        .await
                    {
                        Ok(v) => {
                            let id = v["id"].as_str().unwrap_or_default().to_string();
                            if !id.is_empty() {
                                fresh.push(id.clone());
                            }
                            id
                        }
                        Err(e) => {
                            self.release(&done).await;
                            // A golden that is not here *yet* is not a
                            // failure.
                            //
                            // Goldening a cloud image is minutes of
                            // downloading and sealing, and a machine asked
                            // for before that finishes used to fail
                            // permanently: the record went Failed, sync saw
                            // it in `vms` and skipped it for ever, and the
                            // only recovery was deleting the VM and creating
                            // it again once the image had landed.
                            //
                            // A pod scheduled before its image is pulled
                            // waits. So does this.
                            if e.starts_with("404") {
                                return Err(StartFail::Waiting(format!(
                                    "waiting for golden {g}"
                                )));
                            }
                            return Err(StartFail::Failed(format!(
                                "cloning golden {g} for disk {}: {e}",
                                d.name
                            )));
                        }
                    }
                        }
                        Err(e) => {
                            self.release(&done).await;
                            return Err(e);
                        }
                    }
                }
                DiskSource::Volume(v) => v.clone(),
                // A PersistentVolumeClaim in the VM's namespace (#74). The
                // pod manager resolves it and attaches it, exactly as for a
                // pod, so the device is taken from that and the attach below
                // is skipped. Anything that stops it resolving (unbound,
                // another class, a pod using it here) is something that can
                // change, so the machine waits rather than fails.
                DiskSource::Claim(c) => {
                    let Some(pods) = &self.claims else {
                        self.release(&done).await;
                        return Err(StartFail::Failed(format!(
                            "disk {}: claim {c} needs an apiserver to resolve, and this kubelet \
                             has none",
                            d.name
                        )));
                    };
                    match pods.claim_for_vm(&vm.namespace, c).await {
                        Ok((volume_id, device)) => {
                            done.push(ResolvedDisk {
                                name: d.name.clone(),
                                device,
                                volume_id: Some(volume_id),
                                readonly: d.readonly,
                                bus: d.bus,
                            });
                            continue;
                        }
                        Err(why) => {
                            self.release(&done).await;
                            return Err(StartFail::Waiting(format!("disk {}: {why}", d.name)));
                        }
                    }
                }
                // A blank disk the VM owns, KubeVirt's `emptyDisk` (#73). Made
                // once and found by name on every later start, as the
                // standalone path does: a data disk that came back blank after
                // a restart would lose everything the guest wrote to it.
                DiskSource::Empty => {
                    let name = stormvm_node::start::volume_name(vm, &d.name);
                    let found = match self.reuse(&d.name, &name, owner).await {
                        Ok(found) => found,
                        Err(e) => {
                            self.release(&done).await;
                            return Err(e);
                        }
                    };
                    match found {
                        Some(id) => id,
                        None => match self.empty_volume(vm, &d.name, d.size.as_deref(), owner).await {
                            Ok(id) => {
                                fresh.push(id.clone());
                                id
                            }
                            Err(e) => {
                                self.release(&done).await;
                                return Err(StartFail::Failed(format!("disk {}: {e}", d.name)));
                            }
                        },
                    }
                }
                DiskSource::CloudInit => match self.seed_volume(vm, &d.name, keys, owner).await {
                    Ok(id) => {
                        fresh.push(id.clone());
                        id
                    }
                    Err(e) => {
                        self.release(&done).await;
                        return Err(StartFail::Failed(format!("disk {}: {e}", d.name)));
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
                    return Err(StartFail::Failed(format!("attaching {volume_id} for disk {}: {e}", d.name)));
                }
            };
            let Some(device) = info["device_hint"].as_str() else {
                self.release(&done).await;
                return Err(StartFail::Failed(format!(
                    "disk {} did not attach locally: {info} — an NVMe-oF attach needs a connect \
                     this node does not do yet",
                    d.name
                )));
            };
            // Whose disk is this?
            //
            // A clone of a golden, a cloud-init seed and an empty disk were
            // made *for* this machine and go with it. A `volume:<id>` or a
            // claim was handed to it and is somebody else's — deleting that
            // is deleting data the machine was only borrowing. stormvm's own
            // `delete` states the rule. Listed rather than excluded, so a
            // source stormvm adds later is kept until someone decides.
            let ours = matches!(
                d.from,
                DiskSource::Golden(_) | DiskSource::CloudInit | DiskSource::Empty
            );
            if ours {
                owned.push(volume_id.clone());
            }
            done.push(ResolvedDisk {
                name: d.name.clone(),
                device: device.to_string(),
                volume_id: Some(volume_id),
                readonly: d.readonly,
                bus: d.bus,
            });
        }
        Ok((done, owned))
    }

    /// Make this VM's NICs and hand their descriptors to the engine.
    ///
    /// **The node makes the tap; the hypervisor inherits it.** Creating one
    /// needs CAP_NET_ADMIN, and a VMM that could do it would hold that
    /// capability for the whole life of a guest running a foreign operating
    /// system.
    ///
    /// One tap per declared interface, in order — a VM with three NICs gets
    /// three, each on the bridge its network names, which is what makes a
    /// machine on the storage network and a machine on the pod network differ
    /// only in a bridge name.
    /// Make every NIC the spec asks for, and deposit its descriptor.
    ///
    /// The naming and the ioctls are `stormvm-net`'s, not this file's
    /// (rustkube-node#39). Two reasons, and the second is the one that
    /// matters: the standalone `stormvm start` path and the kubelet must not
    /// derive *different* names for one VM, and the derivation this file used
    /// to carry ignored the namespace — so `default/web-1` and
    /// `staging/web-1` got the same tap name and, worse, the same MAC. Two
    /// guests with one address on a shared segment presents as intermittent
    /// connectivity for both, with the ARP table the only place it is visible.
    ///
    /// The local ioctl helpers went with it. They read `ifr_ifindex` through
    /// the `flags` arm of `ifreq` — an `int` read as a `short`, which is the
    /// whole value below 32767 and a truncation above it. Interface indices
    /// are monotonic and never reused, and every container veth consumes one,
    /// so a node that has churned enough pods enslaves the wrong interface or
    /// none, silently: the tap exists, the guest has a NIC, and no frame ever
    /// reaches the bridge.
    async fn resolve_nics(
        &self,
        uid: &str,
        namespace: &str,
        vm: &VmSpec,
        ring: &Arc<RingClient>,
    ) -> Result<(Vec<plan::ResolvedNic>, Vec<NicReport>, Vec<(usize, stormvm_net::Snooper)>), String> {
        let defaults = stormvm_net::Defaults { uplink_bridge: DEFAULT_BRIDGE.to_string() };
        // Pure: every decision that could be wrong is made here, with no
        // privilege and nothing created yet.
        let plans = stormvm_net::plan(namespace, &vm.name, &vm.interfaces, &defaults)?;

        let mut out = Vec::with_capacity(plans.len());
        let mut reports = Vec::with_capacity(plans.len());
        let mut snoopers = Vec::new();
        for p in &plans {
            // No sandbox: a VM on the pod network wants a namespace this
            // kubelet does not pop here yet, and `realise` refuses that
            // binding by name rather than landing the guest on the node's own
            // network.
            let made = stormvm_net::realise(p, None, RUN_ROOT)?;
            if let Some(fd) = made.fd {
                // Before the spawn: the engine resolves `Spec.fds` against
                // what was already deposited, so a descriptor that arrives
                // later is a name nobody deposited and the guest starts with
                // no NIC — which it reports as a network that is simply down.
                // The ring takes ownership and holds the tap up for the run.
                let r = ring.clone();
                let slot = p.slot();
                let nic = p.nic.clone();
                tokio::task::spawn_blocking(move || r.deposit_fd(&slot, fd))
                    .await
                    .map_err(|e| format!("deposit task: {e}"))?
                    .map_err(|e| format!("interface {nic}: {e:?}"))?;
            }
            let mac = made.address.as_ref().map(|a| a.mac.clone()).unwrap_or_else(|| p.mac.clone());
            // A tap on one of the node's bridges: the guest's address comes
            // from the segment's DHCP server, and the tap is where the node
            // sees it (#91). Watched before the spawn, because a guest that
            // DHCPs in its first second would otherwise do it unwatched. A
            // watcher that cannot open is a warning: the machine still runs,
            // and the agent and the neighbour table still answer.
            if made.binding == "host-bridge" {
                let (tx, id, i) = (self.snoop_tx.clone(), uid.to_string(), reports.len());
                let seen = move |addresses: Vec<String>| {
                    let _ = tx.send(Snooped { uid: id.clone(), nic: i, addresses });
                };
                match stormvm_net::snoop_tap(&p.tap, &mac, seen) {
                    Ok(s) => snoopers.push((i, s)),
                    Err(e) => warn!(vm = %vm.name, nic = %p.nic, "cannot watch {} for the guest's address: {e}", p.tap),
                }
            }
            reports.push(NicReport {
                name: p.nic.clone(),
                mac: mac.clone(),
                binding: binding_of(&made.transport),
                // Empty until the guest has booted far enough to have one.
                addresses: vec![],
            });
            out.push(plan::ResolvedNic {
                name: p.nic.clone(),
                // Where a binding produced an address, the MAC is that
                // address's; otherwise the derived one.
                mac,
                transport: made.transport,
            });
        }
        Ok((out, reports, snoopers))
    }


    /// Build this VM's cloud-init seed.
    ///
    /// **A cloud image has no password and no keys**, by design: it becomes
    /// *this* machine by reading a seed. The seed is a small **vfat** volume
    /// labelled `CIDATA` holding `meta-data`, `user-data` and (when the
    /// network is not DHCP) `network-config`.
    ///
    /// vfat, not ext4. NoCloud is documented as vfat or ISO 9660 and in
    /// practice honours only those: an ext4 volume carrying the same label and
    /// the same files produced `Did not find any data source, searched
    /// classes: ()` inside an Alpine guest with the disk visible as `vdb`. The
    /// label was genuinely on the disk — `blkid` said so — and it still was
    /// not read.
    ///
    /// stormblock makes it in one call, so there is no template, no clone and
    /// no filesystem writer here.
    ///
    /// The hostname comes from the **definition** — the VM's `hostname` or its
    /// name — never from a lease. A guest that takes its name from DHCP is a
    /// guest whose identity changes when the network does, and its
    /// certificates and logs change with it.
    /// The seed is made again on every start — the keys in it can change —
    /// under the disk's namespaced name. The one a previous start left is
    /// deleted first: a stop only detaches (#75), and two volumes under one
    /// name are two answers to a lookup by name.
    async fn seed_volume(&self, vm: &VmSpec, disk: &str, keys: &[String], owner: &Value) -> Result<String, String> {
        let mut seed = stormvm_cloudinit::Seed::for_vm(vm);
        // `accessCredentials` keys go in meta-data `public-keys`, never
        // user-data, where a second `ssh_authorized_keys:` would replace the
        // VM's own (#92).
        for k in keys {
            if !seed.public_keys.contains(k) {
                seed.public_keys.push(k.clone());
            }
        }

        // 16 MiB: the files are a few hundred bytes, and FAT16 needs enough
        // clusters to be FAT16 at all. Thin, so it costs what it holds.
        let name = stormvm_node::start::volume_name(vm, disk);
        if let Some(old) = self.volume_named(&name).await? {
            if let Some(id) = old["id"].as_str() {
                self.delete_volumes(&vm.name, &[id.to_string()]).await;
            }
        }
        let mut body = json!({
            "name": name,
            "size": "16M",
            "redundancy": "none",
        });
        if !owner.is_null() {
            body["owner"] = owner.clone();
        }
        let v = self
            .post(&format!("{}/api/v1/volumes", self.storage), &body)
            .await
            .map_err(|e| format!("creating the seed volume: {e}"))?;
        let id = v["id"]
            .as_str()
            .ok_or_else(|| format!("stormblock returned no volume: {v}"))?
            .to_string();

        let files: Vec<Value> = seed
            .files()
            .into_iter()
            .map(|(name, contents)| json!({ "path": name, "contents": contents }))
            .collect();
        if let Err(e) = self
            .post(
                &format!("{}/api/v1/volumes/{id}/cidata", self.storage),
                &json!({ "files": files, "label": "CIDATA" }),
            )
            .await
        {
            // Made here and unusable: not left behind for the next try (#76).
            self.delete_volumes(&vm.name, std::slice::from_ref(&id)).await;
            return Err(format!("writing the seed: {e}"));
        }
        Ok(id)
    }

    /// A new blank volume of `size` for the VM's empty disk. The caller has
    /// already looked for the one a previous start made ([`Self::reuse`]).
    ///
    /// No `redundancy`: the engine's default, not the seed's `none`, because
    /// this is the guest's data. No filesystem either: what goes on the disk
    /// is the guest's business.
    async fn empty_volume(
        &self,
        vm: &VmSpec,
        disk: &str,
        size: Option<&str>,
        owner: &Value,
    ) -> Result<String, String> {
        let name = stormvm_node::start::volume_name(vm, disk);
        let size = size.ok_or_else(|| "an empty disk needs a size".to_string())?;
        let mut body = json!({
            "name": name,
            "size": size,
            "label": format!("storm.io/vm={}", vm.id()),
        });
        if !owner.is_null() {
            body["owner"] = owner.clone();
        }
        let v = self
            .post(&format!("{}/api/v1/volumes", self.storage), &body)
            .await
            .map_err(|e| format!("creating empty disk {name}: {e}"))?;
        v["id"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| format!("stormblock returned no volume for {name}: {v}"))
    }

    /// A volume's id by name. `Ok(None)` only when the engine answered and
    /// has no such volume: "could not ask" must not become "make a new one",
    /// or a restart during an engine hiccup gives the guest a blank disk.
    async fn volume_named(&self, name: &str) -> Result<Option<Value>, String> {
        Ok(self.volumes().await?.into_iter().find(|v| v["name"].as_str() == Some(name)))
    }

    /// Every volume on this node's engine, as it describes them.
    async fn volumes(&self) -> Result<Vec<Value>, String> {
        let url = format!("{}/api/v1/volumes", self.storage);
        let resp = self.engine.get(&url).await.map_err(|e| format!("listing volumes: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("listing volumes: {}", resp.status()));
        }
        let list: Value = resp.json().await.map_err(|e| format!("listing volumes: {e}"))?;
        Ok(list["items"].as_array().cloned().unwrap_or_default())
    }

    /// The volume a previous start made for disk `disk` (named `name`), if
    /// there is one, with its owner brought up to `owner` (#75).
    ///
    /// A listing that fails is a failed start, not "make a new one": that
    /// would give a restarted guest a fresh root and leave its own beside it.
    /// One left by an earlier object of the same name waits for the sweep.
    async fn reuse(&self, disk: &str, name: &str, owner: &Value) -> Result<Option<String>, StartFail> {
        let found = self
            .volume_named(name)
            .await
            .map_err(|e| StartFail::Failed(format!("disk {disk}: {e}")))?;
        let Some(v) = found else { return Ok(None) };
        let Some(id) = v["id"].as_str().map(String::from) else { return Ok(None) };
        if left_by_another(&v, owner) {
            return Err(StartFail::Waiting(format!(
                "disk {disk}: volume {name} was left by an earlier {} of this name; waiting for it to be removed",
                v["owner"]["kind"].as_str().unwrap_or("owner")
            )));
        }
        if v["owner"] != *owner {
            self.set_volume_owner(&id, owner).await;
        }
        info!(volume = %id, "reusing {name} for disk {disk}");
        Ok(Some(id))
    }

    /// Record a volume's owner (`Null`: none). Best effort: a disk left with
    /// an old owner is kept by the sweep, never deleted early.
    async fn set_volume_owner(&self, id: &str, owner: &Value) {
        let url = format!("{}/api/v1/volumes/{id}/owner", self.storage);
        match self.engine.put(&url, &json!({ "owner": owner })).await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => warn!(volume = %id, "owner not recorded: {}", r.status()),
            Err(e) => warn!(volume = %id, "owner not recorded: {e}"),
        }
    }

    /// The owner for this VMI's disks: [`disk_owner`], with its
    /// VirtualMachine read for the retain annotation.
    async fn owner_of(&self, obj: &Value) -> Value {
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
        let vm = match owner_vm(obj) {
            Some(name) => self.get_vm(ns, name).await,
            None => None,
        };
        disk_owner(obj, vm.as_ref())
    }

    /// Delete the disks whose owner has gone for good (#75): its
    /// VirtualMachine deleted (or replaced under the same name), or a VMI
    /// with no VirtualMachine deleted. At most once a [`SWEEP_EVERY`].
    ///
    /// Only volumes this kubelet gave an owner to, not in use, and not held
    /// by a machine it knows. An owner that cannot be read keeps its disks.
    pub(crate) async fn sweep_orphans(&self) {
        if self.api_url.is_empty() {
            return;
        }
        {
            let mut swept = self.swept.lock().unwrap_or_else(|e| e.into_inner());
            if swept.is_some_and(|t| t.elapsed() < SWEEP_EVERY) {
                return;
            }
            *swept = Some(std::time::Instant::now());
        }
        let _disks = self.disk_lifecycle.write().await;
        let volumes = match self.volumes().await {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("orphan sweep skipped: {e}");
                return;
            }
        };
        let held: std::collections::HashSet<String> = {
            let vms = self.vms.lock().await;
            vms.values().flat_map(|v| v.disks.iter().filter_map(|d| d.volume_id.clone())).collect()
        };
        for v in volumes {
            let Some(id) = v["id"].as_str() else { continue };
            let owner = &v["owner"];
            let Some(path) = owner_path(owner) else { continue };
            if held.contains(id) || v["in_use"].as_bool() == Some(true) {
                continue;
            }
            let (status, obj) = match self.api.get(format!("{}{path}", self.api_url)).send().await {
                Ok(r) => {
                    let status = r.status().as_u16();
                    (status, r.json::<Value>().await.ok())
                }
                Err(_) => continue,
            };
            if !owner_gone(status, obj.as_ref(), owner) {
                continue;
            }
            info!(volume = %id, "deleting {}: its {} {}/{} is gone",
                  v["name"].as_str().unwrap_or(""),
                  owner["kind"].as_str().unwrap_or(""),
                  owner["namespace"].as_str().unwrap_or(""),
                  owner["name"].as_str().unwrap_or(""));
            self.delete_volumes(owner["name"].as_str().unwrap_or(""), &[id.to_string()]).await;
        }
    }

    /// Best effort: a start that has already gone wrong must not be made worse
    /// by refusing to clean up.
    async fn release(&self, disks: &[ResolvedDisk]) {
        for d in disks {
            let Some(id) = &d.volume_id else { continue };
            let url = format!("{}/api/v1/volumes/{id}/attach", self.storage);
            if let Err(e) = self.engine.delete(&url).await {
                warn!("could not detach {id} ({}): {e}", d.name);
            }
        }
    }

    /// Is this one of the node's own addresses?
    ///
    /// Read from the interfaces rather than compared against a configured
    /// node IP: a node has several, and the one a host-network container
    /// happens to source from is whichever the route chose.
    async fn node_ip_holds(&self, ip: &str) -> bool {
        let Ok(out) = tokio::process::Command::new("ip")
            .args(["-o", "addr", "show"])
            .output()
            .await
        else {
            // Cannot tell, so do not guess. Refusing costs a guest its
            // metadata; guessing gives it somebody else's.
            return true;
        };
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.split_whitespace().nth(3))
            .any(|cidr| cidr.split('/').next() == Some(ip))
    }

    /// Who is at this address, as instance metadata.
    ///
    /// **The kubelet is the source of truth and this is how it is asked.**
    ///
    /// The first version of this pushed a copy into the metadata service at
    /// start and deleted it at stop, which is two records of one fact kept in
    /// step by hand. Every way that goes wrong is a guest being told
    /// something false: a machine that moved, a registration that failed
    /// while the machine started anyway, an address handed to its next
    /// occupant before the delete landed. The deregistration existed
    /// precisely to paper over that, which is the sign it was the wrong
    /// shape.
    ///
    /// So nothing is stored twice. The kubelet already holds every fact —
    /// the VMI, the MAC it generated, the addresses the guest was given, and
    /// the machine's whole lifecycle — and answers from that. A record it
    /// does not have is a machine that is not running here, which is exactly
    /// the answer a metadata service should give.
    pub async fn instance_at(&self, ip: &str) -> Option<Value> {
        // An address the node itself holds identifies nobody.
        //
        // A container on the host network shares the node's address, and so
        // does every other one — and so does the node. Source IP cannot tell
        // them apart, so there is no "the instance at this address" to
        // return, and answering with *a* machine would hand one workload
        // another's identity, keys and userdata.
        //
        // Refusing is the only correct answer. A host-network workload that
        // needs an identity needs a different mechanism than an address, and
        // inventing one here quietly would be the worst version of that.
        if self.node_ip_holds(ip).await {
            return None;
        }
        if !self.synced.load(std::sync::atomic::Ordering::Relaxed) {
            // Cold. Saying "no such machine" here would have a guest
            // configure itself as nobody on a kubelet that simply has not
            // caught up yet; the caller turns this into "ask again".
            return Some(json!({ "storm.io/cold": true }));
        }
        let vms = self.vms.lock().await;
        // Several machines can share a pod, so this matches the *machine* by
        // its own address rather than resolving a pod and assuming one.
        let vm = vms.values().find(|v| {
            !v.phase.terminal() && v.nics.iter().any(|n| n.addresses.iter().any(|a| a == ip))
        })?;
        // The object, which is the truth. The local record only said which
        // machine holds this address; everything a guest is told about itself
        // comes from what the apiserver says it should be.
        let obj = self.desired.lock().await.get(&vm.uid).cloned();
        let interfaces: Vec<Value> = vm
            .nics
            .iter()
            .enumerate()
            .map(|(i, n)| {
                json!({
                    "device_index": i as u32,
                    "mac": n.mac,
                    "ipv4_addresses": n.addresses.iter().filter(|a| a.contains('.')).collect::<Vec<_>>(),
                    "ipv6_addresses": n.addresses.iter().filter(|a| a.contains(':')).collect::<Vec<_>>(),
                })
            })
            .collect();
        let ann = |k: &str| -> Option<String> {
            obj.as_ref()?
                .pointer(&format!("/metadata/annotations/{}", k.replace('/', "~1")))?
                .as_str()
                .map(str::to_string)
        };
        Some(json!({
            "instance_id": vm.uid,
            // The name the object gives it, falling back to the machine's
            // own — an annotation somebody set is a deliberate answer and
            // beats one derived from the object's name.
            "hostname": ann("storm.io/hostname").unwrap_or_else(|| vm.name.clone()),
            "local_ipv4": ip,
            "region": "storm",
            // The node, because that is the failure domain a guest is in.
            "zone": self.node_name,
            "tags": { "namespace": vm.namespace, "name": vm.name },
            // Labels from the object, so a guest can see what it was
            // deployed as. Only labels: annotations carry the SSH key and
            // the bridge and are not a guest's business.
            "labels": obj
                .as_ref()
                .and_then(|o| o.pointer("/metadata/labels").cloned())
                .unwrap_or_else(|| json!({})),
            // What the node gave it, which the object cannot know: the
            // addresses came from a DHCP server the node does not run, and
            // the MAC was generated here.
            "network": { "interfaces": interfaces },
            "launched_at": vm.started_unix,
        }))
    }

    /// Delete the volumes this machine made for itself.
    ///
    /// `release` only *detached* them, so a VM's root clone and its seed
    /// outlived it for ever. One machine leaves two orphans; a build fleet
    /// creating and destroying a hundred a day leaves two hundred, and
    /// nothing can tell them from volumes something still needs — which is
    /// the orphan problem stormblock#115 exists for, manufactured daily.
    ///
    /// Only what this machine created. A `volume:<id>` it was handed belongs
    /// to whoever made it and is meant to outlive the machine; deleting that
    /// is deleting somebody's data because a VM that borrowed it went away.
    /// stormvm's own `delete` states this rule and the kubelet did not
    /// implement it.
    ///
    /// After the detach, and best-effort: a volume that will not delete is
    /// worth a line, not a failed teardown. The machine is already gone, and
    /// refusing to finish would leave the *registration* behind too.
    /// Every registered machine this process does not know, reconciled
    /// against what is wanted (#35).
    ///
    /// The engine keeps a machine running across a kubelet restart, and this
    /// process's `vms` does not survive one. So a machine can be running here
    /// that nothing tracks: deleting its object stopped nothing, and the
    /// hypervisor went on with an address and a disk and no object anywhere.
    ///
    /// - running, and its object wants it: adopted, as though started here;
    /// - running, and nothing wants it: stopped, disks given back;
    /// - gone, and its object wants it: recorded as ended, so it is not
    ///   started again behind the VM controller's back;
    /// - gone, and nothing wants it: its leftovers released.
    ///
    /// Only on a sync, which has a list that answered: an empty `want` from an
    /// apiserver that did not answer never reaches here.
    async fn reconcile_registered(&self, want: &[(String, Value)]) {
        let Some(ring) = self.ring.clone() else { return };
        let known: std::collections::HashSet<String> = self.vms.lock().await.keys().cloned().collect();
        for reg in stormvm_node::console::list(RUN_ROOT) {
            if reg.uid.is_empty() || known.contains(&reg.uid) {
                continue;
            }
            let obj = want.iter().find(|(u, _)| *u == reg.uid).map(|(_, o)| o);
            let vm = vm_of(&reg);
            // Some(true) running, Some(false) exited with a status the engine
            // still holds, None gone or unknowable.
            let state = match reg.workload {
                Some(h) => {
                    let r = ring.clone();
                    match tokio::task::spawn_blocking(move || r.query(Handle(h))).await {
                        Ok(Ok(cqe)) => Some(cqe.aux & 0xff != 2),
                        _ => None,
                    }
                }
                None => control_alive(&reg).then_some(true),
            };
            let id = format!("{}/{}", vm.namespace, vm.name);
            match (state, obj) {
                (Some(true), Some(obj)) => {
                    info!(vm = %id, handle = ?vm.handle, "adopted a running machine");
                    // Started before disks had owners (#75): give them one
                    // now, or the sweep could never delete them.
                    let owner = self.owner_of(obj).await;
                    for d in reg.disks.iter().filter(|d| d.owned) {
                        if let Some(v) = &d.volume_id {
                            self.set_volume_owner(v, &owner).await;
                        }
                    }
                    self.event(obj, "Normal", "Adopted",
                               &format!("Virtual machine {} was already running on {}", vm.name, self.node_name))
                        .await;
                    self.vms.lock().await.insert(vm.uid.clone(), vm.clone());
                    self.patch_status(&vm).await;
                }
                // Its end is the engine's to report: `absorb_ends` reads it.
                (Some(false), Some(_)) => {
                    self.vms.lock().await.insert(vm.uid.clone(), vm);
                }
                (None, Some(_)) => {
                    warn!(vm = %id, "engine state unknown; retaining machine and disks");
                    self.vms.lock().await.insert(vm.uid.clone(), vm);
                }
                (running, None) => {
                    if running == Some(true) {
                        warn!(vm = %id, uid = %vm.uid, "running with no VirtualMachineInstance; stopping it");
                    }
                    if !self.stop(&vm).await {
                        self.vms.lock().await.insert(vm.uid.clone(), vm);
                    }
                }
            }
        }
    }

    /// Stop a machine there is no engine handle for, through its own control
    /// socket: ACPI first, with the same grace as the engine's stop, then the
    /// hypervisor told to quit.
    ///
    /// Only for a machine started by a kubelet that kept no handle. Every
    /// start now records one, and the engine's stop is the one to use.
    async fn stop_by_control(&self, vm: &Vm) {
        use stormvm_control::{Kind, Machine};
        let Some(reg) = stormvm_node::console::find(RUN_ROOT, &vm.namespace, &vm.name) else { return };
        if !control_alive(&reg) {
            return;
        }
        let kind = Kind::parse(&reg.vmm);
        let Some(sock) = reg.control_socket.clone() else { return };
        let m = Machine { kind, control: Some(sock.clone()), agent: None };
        let short = std::time::Duration::from_secs(5);
        if let Ok(Err(e)) = tokio::time::timeout(short, m.softreboot()).await {
            warn!(vm = %vm.name, "ACPI shutdown not delivered: {e}");
        }
        let gone_within = |secs: u64| {
            let reg = reg.clone();
            async move {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
                while control_alive(&reg) {
                    if std::time::Instant::now() >= deadline {
                        return false;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                true
            }
        };
        if gone_within(30).await {
            return;
        }
        warn!(vm = %vm.name, "did not shut down within its grace period; telling the hypervisor to quit");
        let forced = match kind {
            Kind::Qemu => tokio::time::timeout(short, stormvm_control::qmp::command(&sock, "quit", None))
                .await
                .map(|r| r.map(|_| ())),
            Kind::CloudHypervisor => {
                tokio::time::timeout(short, stormvm_control::chv::put(&sock, "vmm.shutdown")).await
            }
        };
        if let Ok(Err(e)) = forced {
            warn!(vm = %vm.name, "quit not delivered: {e}");
        }
        if !gone_within(5).await {
            warn!(vm = %vm.name, "the hypervisor is still running; it has no engine handle to kill");
        }
    }

    /// Add or remove this node's finalizer on a VMI. Guarded by the object's
    /// resourceVersion, so a stale copy loses rather than overwrites; the
    /// next sync has a fresher one.
    async fn set_finalizer(&self, obj: &Value, present: bool) -> bool {
        if self.api_url.is_empty() { return true; }
        let Some(body) = finalizer_patch(obj, present) else { return true };
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = obj["metadata"]["name"].as_str().unwrap_or("");
        let url = format!("{}/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{name}", self.api_url);
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&body)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                info!("{ns}/{name}: finalizer {FINALIZER} {}", if present { "added" } else { "removed" });
                true
            }
            Ok(r) => { tracing::debug!("{ns}/{name}: finalizer not updated: {}", r.status()); !present && r.status().as_u16()==404 },
            Err(e) => { tracing::debug!("{ns}/{name}: finalizer not updated: {e}"); false },
        }
    }

    /// Delete these volumes. Best effort, each failure logged.
    async fn delete_volumes(&self, vm: &str, ids: &[String]) {
        for id in ids {
            let url = format!("{}/api/v1/volumes/{id}", self.storage);
            match self.engine.delete(&url).await {
                Ok(r) if r.status().is_success() => {
                    info!(vm = %vm, volume = %id, "deleted the machine's own volume");
                }
                Ok(r) => {
                    let code = r.status();
                    let body = r.text().await.unwrap_or_default();
                    warn!(vm = %vm, volume = %id,
                          "could not delete: {code} {}", body.trim());
                }
                Err(e) => warn!(vm = %vm, volume = %id, "could not delete: {e}"),
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
            started_unix: now_unix(),
            // It never became ready, and that is the point of recording it.
            ready_unix: None,
            owned_volumes: Vec::new(),
            // A machine that never started has no interfaces to report, and
            // saying so is different from not knowing.
            nics: Vec::new(),
            message: why.to_string(),
            access: Access::default(),
        };
        self.vms.lock().await.insert(uid.to_string(), rec.clone());
        self.patch_status(&rec).await;
    }

    /// A machine that is waiting for something, said on the object.
    ///
    /// Pending rather than Failed, with the reason — so a console shows
    /// "waiting for golden fedora-43" instead of a machine that looks broken
    /// and a person who deletes it and tries again.
    /// A VirtualMachine by name, or `None` when there is none or it could not
    /// be read.
    async fn get_vm(&self, ns: &str, name: &str) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!("{}/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachines/{name}", self.api_url);
        let r = self.api.get(&url).send().await.ok()?;
        if !r.status().is_success() {
            return None;
        }
        r.json().await.ok()
    }

    /// Pending after a failed start that will be tried again (#76).
    async fn patch_retrying(&self, ns: &str, name: &str, uid: &str, why: &str) {
        self.patch_pending_as(ns, name, uid, "FailedStart", why).await;
    }

    async fn patch_pending(&self, ns: &str, name: &str, uid: &str, why: &str) {
        self.patch_pending_as(ns, name, uid, "Waiting", why).await;
    }

    async fn patch_pending_as(&self, ns: &str, name: &str, uid: &str, reason: &str, why: &str) {
        let url = format!(
            "{}/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{name}/status",
            self.api_url
        );
        let body = json!({ "metadata": {"uid": uid}, "status": {
            "phase": "Pending",
            "reason": reason,
            "message": why,
            "nodeName": self.node_name,
        }});
        if let Err(e) = self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&body)
            .send()
            .await
        {
            warn!("could not report {ns}/{name} as pending: {e}");
        }
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
        // What the machine was actually given.
        //
        // A running VMI reported `phase` and `nodeName` and nothing else, so
        // a console had nothing to show and anyone asking "why can I not
        // reach this guest" had nowhere to look. The MAC was generated at
        // start and discarded; the binding — the thing that decides whether
        // an address is routable at all — was never written down anywhere.
        //
        // Upstream's field, so a console that knows KubeVirt knows this.
        if vm.started_unix > 0 {
            status["storm.io/startedUnix"] = json!(vm.started_unix);
        }
        if let Some(r) = vm.ready_unix {
            status["storm.io/readyUnix"] = json!(r);
            // Precomputed, because every consumer would otherwise subtract
            // two numbers and one of them would get it wrong.
            status["storm.io/bootSeconds"] = json!(r.saturating_sub(vm.started_unix));
        }
        if !vm.nics.is_empty() {
            status["interfaces"] = json!(vm
                .nics
                .iter()
                .map(|n| json!({
                    "name": n.name,
                    "mac": n.mac,
                    // Upstream's field, singular, plus every address when
                    // there is more than one — a guest with v4 and v6 has
                    // two and neither is "the" address.
                    "ipAddress": n.addresses.first().cloned().unwrap_or_default(),
                    "ipAddresses": n.addresses,
                    // Not upstream's, and named so it cannot be mistaken for
                    // one: `user` means the guest is behind a NAT inside the
                    // hypervisor process, which is a very different thing
                    // from an address the cluster routes to.
                    "storm.io/binding": n.binding,
                }))
                .collect::<Vec<_>>());
        }
        if !vm.message.is_empty() {
            status["reason"] = json!(if vm.phase == Phase::Failed { "Failed" } else { "Ended" });
            status["message"] = json!(vm.message);
        }
        // `AccessCredentialsSynchronized` (#92), with whatever conditions the
        // object already carries: a merge patch replaces the list whole.
        if let Some(cond) = &vm.access.condition {
            let existing = self
                .desired
                .lock()
                .await
                .get(&vm.uid)
                .map(|o| o["status"]["conditions"].clone())
                .unwrap_or(Value::Null);
            status["conditions"] = with_condition(&existing, cond);
        }
        let body = json!({ "metadata": {"uid": vm.uid}, "status": status });
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
        let resp = self.engine.post(url, body).await.map_err(|e| e.to_string())?;
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
/// Follow this node's machines, and keep following them.
///
/// A poll asks "what is it now" every two seconds whether or not anything
/// changed; a watch is told. That matters most for the case it was hardest to
/// reason about: a machine that moves. With a poll the old node keeps
/// answering for it for up to a tick after it is gone, and the new one does
/// not answer for up to a tick after it arrives. With a watch both learn at
/// the moment the object changes.
///
/// The shape is the one every Kubernetes client uses, and each part of it is
/// there for a failure that happens:
///
/// 1. **LIST** for the current state and the `resourceVersion` it is current
///    as of. Starting a watch without one asks for "everything from now",
///    which silently misses whatever exists already.
/// 2. **WATCH** from that version. The body never ends; events arrive as
///    newline-delimited JSON.
/// 3. **410 Gone** means the server has discarded the history this version
///    needed — ordinary after a compaction or a long disconnect, and the only
///    correct response is to LIST again rather than to retry the watch.
/// 4. **Any disconnect** is normal. An apiserver rotating, a load balancer
///    idling out, a network blink: reconnect from the last version seen.
///
/// `on_set` is called with the full set after each change, so the caller sees
/// the same shape a poll gave it and nothing downstream has to understand
/// events.
pub async fn watch_for_node<F>(
    api: reqwest::Client,
    api_url: String,
    node: String,
    on_set: F,
) where
    F: Fn(Vec<Value>) + Send + Sync + 'static,
{
    use futures::StreamExt;
    let base = api_url.trim_end_matches('/').to_string();
    let selector = format!("fieldSelector=status.nodeName%3D{node}");
    // What this node believes it should be running, by uid — the set the
    // watch maintains and hands back whole.
    let mut have: HashMap<String, Value> = HashMap::new();

    loop {
        // 1. LIST.
        let list_url =
            format!("{base}/apis/kubevirt.io/v1/virtualmachineinstances?{selector}");
        let Ok(resp) = api.get(&list_url).send().await else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };
        // An error answer is not an empty list (#35): a Status body has no
        // items, and handing that on stopped every machine on the node.
        let v = match resp.status().is_success() {
            true => resp.json::<Value>().await.ok().filter(|v| v["items"].is_array()),
            false => None,
        };
        let Some(v) = v else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        };
        have.clear();
        for o in v["items"].as_array().unwrap_or(&Vec::new()) {
            // The same local check the poll kept, for the same reason: an
            // apiserver that does not implement the selector answers with
            // everything, and running the cluster's machines on one node is
            // worse than a slow list.
            if !assigned_to(o, &node) {
                continue;
            }
            if let Some(uid) = o["metadata"]["uid"].as_str() {
                have.insert(uid.to_string(), o.clone());
            }
        }
        let mut version = v["metadata"]["resourceVersion"]
            .as_str()
            .unwrap_or("")
            .to_string();
        on_set(have.values().cloned().collect());

        // 2. WATCH.
        loop {
            let watch_url = format!(
                "{base}/apis/kubevirt.io/v1/virtualmachineinstances                 ?watch=true&{selector}&resourceVersion={version}&timeoutSeconds=300"
            );
            let Ok(resp) = api.get(&watch_url).send().await else {
                break;
            };
            if resp.status().as_u16() == 410 {
                // Too old. Re-list; do not retry this version.
                break;
            }
            if !resp.status().is_success() {
                break;
            }
            let mut stream = resp.bytes_stream();
            // Events are newline-delimited and a chunk is not a line: one
            // chunk can hold several events or half of one.
            let mut buf = Vec::new();
            let mut gone = false;
            while let Some(Ok(chunk)) = stream.next().await {
                buf.extend_from_slice(&chunk);
                while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=nl).collect();
                    let Ok(ev) = serde_json::from_slice::<Value>(&line[..line.len() - 1]) else {
                        continue;
                    };
                    let kind = ev["type"].as_str().unwrap_or("");
                    let obj = &ev["object"];
                    if kind == "ERROR" {
                        gone = true;
                        break;
                    }
                    if let Some(rv) = obj["metadata"]["resourceVersion"].as_str() {
                        version = rv.to_string();
                    }
                    let Some(uid) = obj["metadata"]["uid"].as_str() else { continue };
                    match kind {
                        "ADDED" | "MODIFIED" => {
                            if assigned_to(obj, &node) {
                                have.insert(uid.to_string(), obj.clone());
                            } else {
                                // Reassigned away from here — which is a
                                // machine that moved, and the moment this
                                // node must stop answering for it.
                                have.remove(uid);
                            }
                        }
                        "DELETED" => {
                            have.remove(uid);
                        }
                        _ => continue,
                    }
                    on_set(have.values().cloned().collect());
                }
                if gone {
                    break;
                }
            }
            if gone {
                break;
            }
            // The body ended: a timeout, a rotation, a blink. Reconnect from
            // where we are rather than re-listing, which is the whole point
            // of keeping the version.
        }
        // Fell out to re-list. A moment's pause so a persistently broken
        // apiserver is not hammered.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

///
/// `None` when the list failed. **A failed list is not an empty one**: read as
/// "no machines here", it stopped every VM on the node and deleted the roots
/// they owned, over an apiserver that did not answer for a moment (#35).
pub async fn list_for_node(api: &reqwest::Client, api_url: &str, node: &str) -> Option<Vec<Value>> {
    // Ask for this node's machines, not the cluster's.
    //
    // This listed **every VMI in the cluster** and filtered locally, every
    // two seconds, on every node. On a small cluster that is invisible. At a
    // thousand nodes running a thousand machines each it is a million objects
    // fetched five hundred times a second, and the apiserver spends its life
    // serializing a list that each caller throws away 99.9% of.
    //
    // The field selector is the same one upstream's kubelet uses, and the
    // reason it does: a node's business is its own machines.
    //
    // Still a poll. A watch is the right answer and a larger change — it
    // needs resourceVersion tracking and re-establishment on disconnect —
    // but a filtered poll is correct now and is the difference between
    // O(cluster) and O(node) per tick.
    let url = format!(
        "{}/apis/kubevirt.io/v1/virtualmachineinstances?fieldSelector=status.nodeName%3D{node}",
        api_url.trim_end_matches('/')
    );
    let resp = api.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v = resp.json::<Value>().await.ok()?;
    if !v["items"].is_array() {
        return None;
    }
    // Filtered again locally, deliberately.
    //
    // An apiserver that does not implement this field selector answers with
    // everything rather than an error, and silently running every machine in
    // the cluster on one node is a worse failure than a slow list. The check
    // is cheap and it is the one that decides.
    v["items"]
        .as_array()
        .map(|items| items.iter().filter(|o| assigned_to(o, node)).cloned().collect())
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

    #[tokio::test]
    async fn unknown_vm_exit_retains_record_and_finalizer() {
        let f = fake().await;
        let m = manager_for(&f, false);
        let mut obj = vmi("n1");
        m.record_failure("u-1", &obj, "fixture").await;
        {
            let mut records = m.vms.lock().await;
            let vm = records.get_mut("u-1").unwrap();
            vm.phase = Phase::Running;
            vm.handle = Handle(123);
        }
        obj["metadata"]["deletionTimestamp"] = json!("2026-09-29T00:00:00Z");
        m.sync(&[obj]).await;
        assert_eq!(m.running().await.len(), 1, "no engine is not proof of exit");
    }

    #[tokio::test]
    async fn refused_detach_keeps_vm_cleanup_record() {
        use axum::{routing::delete, http::StatusCode};
        let app = axum::Router::new().route("/api/v1/volumes/disk/attach",
            delete(|| async { StatusCode::CONFLICT }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "")
            .with_storage(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()));
        m.record_failure("u-1", &vmi("n1"), "fixture").await;
        m.vms.lock().await.get_mut("u-1").unwrap().disks.push(ResolvedDisk {
            name: "root".into(), device: "/dev/test".into(), volume_id: Some("disk".into()),
            readonly: false, bus: Default::default(),
        });
        m.sync(&[]).await;
        assert_eq!(m.running().await.len(), 1, "HTTP 409 must remain retryable");
        server.abort();
}

    /// `AccessCredentialsSynchronized`: every problem named, an agent not yet
    /// heard from is a reason, and the transition time holds while the status
    /// does (#92).
    #[test]
    fn the_access_condition_says_what_did_not_arrive() {
        use stormvm_spec::access::{AccessCredential, Propagation};
        let boot = AccessCredential { secret: "k".into(), propagation: Propagation::NoCloud };
        let agent = AccessCredential {
            secret: "k".into(),
            propagation: Propagation::GuestAgent { users: vec!["root".into()] },
        };
        assert_eq!(access_outcome(&[], &Access::default()), None);
        assert_eq!(access_outcome(&[boot.clone()], &Access::default()), Some(Ok(())));
        let a = Access { boot: vec!["Secret ns/k not found".into()], ..Access::default() };
        assert_eq!(access_outcome(&[boot.clone()], &a), Some(Err("Secret ns/k not found".into())));
        assert_eq!(
            access_outcome(&[boot.clone(), agent.clone()], &Access::default()),
            Some(Err("waiting for the guest agent to answer".into()))
        );
        let a = Access { agent: Some(vec![]), ..Access::default() };
        assert_eq!(access_outcome(&[agent], &a), Some(Ok(())));

        let t0 = access_condition(None, &Err("x".into()), "2026-09-28T10:00:00Z");
        assert_eq!(t0["status"], "False");
        assert_eq!(t0["message"], "x");
        let t1 = access_condition(Some(&t0), &Err("y".into()), "2026-09-28T10:05:00Z");
        assert_eq!(t1["lastTransitionTime"], "2026-09-28T10:00:00Z", "still False: same transition");
        assert_eq!(t1["message"], "y");
        let t2 = access_condition(Some(&t1), &Ok(()), "2026-09-28T10:06:00Z");
        assert_eq!((t2["status"].as_str(), t2["lastTransitionTime"].as_str()), (Some("True"), Some("2026-09-28T10:06:00Z")));

        // The others survive; the old access condition is replaced, not doubled.
        let existing = json!([{"type": "Ready", "status": "True"}, t0]);
        let merged = with_condition(&existing, &t2);
        assert_eq!(merged, json!([{"type": "Ready", "status": "True"}, t2]));
        assert_eq!(with_condition(&Value::Null, &t2), json!([t2]));
    }

    /// Boot keys come from the Secrets, `stringData` included; a missing one,
    /// or a machine with no seed to carry them, is a reason, not a failed
    /// start (#92).
    #[tokio::test]
    async fn boot_keys_are_read_from_the_secrets() {
        use axum::routing::get;
        let app = axum::Router::new()
            .route(
                "/api/v1/namespaces/default/secrets/owner-keys",
                get(|| async {
                    axum::Json(json!({"stringData": {"key": "ssh-ed25519 AAAAC3Nza owner@laptop\n"}}))
                }),
            )
            .fallback(|| async { (axum::http::StatusCode::NOT_FOUND, "no") });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let m = VmManager::new(None, "n1", reqwest::Client::new(), &url);

        let creds = json!([
            {"sshPublicKey": {"source": {"secret": {"secretName": "owner-keys"}},
                              "propagationMethod": {"noCloud": {}}}},
            {"sshPublicKey": {"source": {"secret": {"secretName": "gone"}},
                              "propagationMethod": {"noCloud": {}}}},
        ]);
        let mut obj = vmi("n1");
        obj["spec"]["accessCredentials"] = creds;
        obj["spec"]["domain"]["devices"] = json!({"disks": [{"name": "ci", "disk": {"bus": "virtio"}}]});
        obj["spec"]["volumes"] = json!([{"name": "ci", "cloudInitNoCloud": {"userData": "#cloud-config\n"}}]);
        let vm: VmSpec = stormvm_spec::kube::from_kube(&obj).unwrap();
        let (keys, problems) = m.boot_keys(&vm).await;
        assert_eq!(keys, vec!["ssh-ed25519 AAAAC3Nza owner@laptop".to_string()]);
        assert_eq!(problems, vec!["Secret default/gone not found".to_string()]);

        // No cloudInitNoCloud volume: nowhere for the keys to go, and it says so.
        let mut bare = vmi("n1");
        bare["spec"]["accessCredentials"] = json!([
            {"sshPublicKey": {"source": {"secret": {"secretName": "owner-keys"}},
                              "propagationMethod": {"noCloud": {}}}}]);
        let vm: VmSpec = stormvm_spec::kube::from_kube(&bare).unwrap();
        let (keys, problems) = m.boot_keys(&vm).await;
        assert!(keys.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("no cloudInitNoCloud volume"), "{problems:?}");
    }

    /// The tap watcher wins per NIC where it has seen something; elsewhere
    /// the agent's or the neighbour table's answer stands (#91).
    #[test]
    fn a_snooped_address_is_preferred_per_nic() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        // Nothing seen on any tap: the other answer, even none.
        assert_eq!(prefer_snooped(None, vec![vec![], vec![]]), None);
        assert_eq!(
            prefer_snooped(Some(vec![s(&["10.0.0.9"])]), vec![vec![]]),
            Some(vec![s(&["10.0.0.9"])])
        );
        // The lease beats a stale neighbour entry on net0; net1 keeps the agent's.
        assert_eq!(
            prefer_snooped(
                Some(vec![s(&["192.168.30.99"]), s(&["fd00::5"])]),
                vec![s(&["192.168.30.4"]), vec![]]
            ),
            Some(vec![s(&["192.168.30.4"]), s(&["fd00::5"])])
        );
        // No agent, no neighbour entry: the tap alone answers.
        assert_eq!(
            prefer_snooped(None, vec![vec![], s(&["192.168.30.4", "fe80::1"])]),
            Some(vec![vec![], s(&["192.168.30.4", "fe80::1"])])
        );
    }

    /// News from a watcher lands on its NIC and the machine counts as up;
    /// news for an unknown machine, or an empty list, changes nothing.
    #[tokio::test]
    async fn snooped_news_updates_the_nic_it_names() {
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "");
        let vm = Vm {
            namespace: "default".into(),
            name: "test2".into(),
            uid: "u-2".into(),
            log_dir: String::new(),
            handle: Handle::NONE,
            disks: vec![],
            phase: Phase::Running,
            exit_code: 0,
            message: String::new(),
            started_unix: 1,
            ready_unix: None,
            owned_volumes: vec![],
            nics: vec![
                NicReport { name: "net0".into(), mac: "02:00:00:00:00:01".into(), addresses: vec![], binding: "bridge".into() },
                NicReport { name: "net1".into(), mac: "02:00:00:00:00:02".into(), addresses: vec![], binding: "bridge".into() },
            ],
            access: Access::default(),
        };
        m.vms.lock().await.insert("u-2".into(), vm);

        m.snooped(Snooped { uid: "other".into(), nic: 0, addresses: vec!["1.2.3.4".into()] }).await;
        m.snooped(Snooped { uid: "u-2".into(), nic: 1, addresses: vec![] }).await;
        assert!(m.vms.lock().await["u-2"].nics.iter().all(|n| n.addresses.is_empty()));

        m.snooped(Snooped { uid: "u-2".into(), nic: 1, addresses: vec!["192.168.30.4".into()] }).await;
        let vm = m.vms.lock().await["u-2"].clone();
        assert!(vm.nics[0].addresses.is_empty());
        assert_eq!(vm.nics[1].addresses, vec!["192.168.30.4".to_string()]);
        assert!(vm.ready_unix.is_some());
    }

    fn vmi(node: &str) -> Value {
        json!({
            "kind": "VirtualMachineInstance",
            "metadata": { "name": "web-1", "namespace": "default", "uid": "u-1" },
            "spec": { "nodeName": node, "domain": { "memory": { "guest": "1Gi" } } }
        })
    }

    #[test]
    fn the_finalizer_is_added_once_and_removed_once() {
        let mut obj = vmi("n1");
        obj["metadata"]["resourceVersion"] = json!("7");
        obj["metadata"]["finalizers"] = json!(["other"]);
        let add = finalizer_patch(&obj, true).expect("added");
        assert_eq!(add["metadata"]["finalizers"], json!(["other", FINALIZER]));
        assert_eq!(add["metadata"]["resourceVersion"], "7", "guarded by the version it was read at");
        assert!(finalizer_patch(&obj, false).is_none(), "not there, nothing to remove");

        obj["metadata"]["finalizers"] = json!(["other", FINALIZER]);
        assert!(finalizer_patch(&obj, true).is_none(), "there already");
        let rm = finalizer_patch(&obj, false).expect("removed");
        assert_eq!(rm["metadata"]["finalizers"], json!(["other"]), "another controller's is kept");
    }

    #[test]
    fn a_registration_carries_what_a_restarted_kubelet_needs() {
        let disks = vec![
            ResolvedDisk { name: "root".into(), device: "/dev/ublkb1".into(), volume_id: Some("v-root".into()),
                           readonly: false, bus: Default::default() },
            ResolvedDisk { name: "data".into(), device: "/dev/ublkb2".into(), volume_id: Some("v-claim".into()),
                           readonly: false, bus: Default::default() },
        ];
        let owned = vec!["v-root".to_string(), "v-seed".to_string()];
        let reg = stormvm_node::console::Registration {
            namespace: "default".into(),
            name: "web-1".into(),
            uid: "u-1".into(),
            serial_socket: None,
            vnc_socket: None,
            serial_log: None,
            hypervisor_log: None,
            control_socket: None,
            agent_socket: None,
            vmm: "qemu".into(),
            started: 100,
            workload: Some(42),
            disks: registered_disks(&disks, &owned),
        };
        // Through the file format, as a restarted kubelet reads it.
        let reg: stormvm_node::console::Registration =
            serde_json::from_str(&serde_json::to_string(&reg).unwrap()).unwrap();
        let vm = vm_of(&reg);
        assert_eq!(vm.handle, Handle(42));
        assert_eq!(vm.uid, "u-1");
        assert_eq!(vm.started_unix, 100);
        assert_eq!(vm.log_dir, format!("{LOG_ROOT}/default_web-1_u-1/web-1"));
        // Both disks are detached on a stop; only what it made is deleted.
        let attached: Vec<_> = vm.disks.iter().filter_map(|d| d.volume_id.clone()).collect();
        assert_eq!(attached, vec!["v-root", "v-claim"]);
        assert_eq!(vm.owned_volumes, vec!["v-root", "v-seed"]);
        assert_eq!(vm_of(&stormvm_node::console::Registration { workload: None, ..reg }).handle, Handle::NONE);
    }

    #[test]
    fn a_hypervisor_is_alive_while_its_control_socket_answers() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("control.sock");
        let reg = |p: Option<String>| stormvm_node::console::Registration {
            namespace: "default".into(),
            name: "web-1".into(),
            uid: "u-1".into(),
            serial_socket: None,
            vnc_socket: None,
            serial_log: None,
            hypervisor_log: None,
            control_socket: p,
            agent_socket: None,
            vmm: "qemu".into(),
            started: 0,
            workload: None,
            disks: Vec::new(),
        };
        let r = reg(Some(sock.to_string_lossy().into()));
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        assert!(control_alive(&r));
        // A dead hypervisor leaves its socket file behind.
        drop(listener);
        assert!(sock.exists());
        assert!(!control_alive(&r));
        assert!(!control_alive(&reg(None)));
    }

    /// A VMI being deleted is not started, and its finalizer comes off once
    /// nothing runs here for it, which is what lets the delete complete.
    #[tokio::test]
    async fn a_deleted_vmi_is_let_go_once_its_machine_is_stopped() {
        use axum::routing::patch;
        let patches: Arc<std::sync::Mutex<Vec<(String, Value)>>> = Arc::default();
        let p = patches.clone();
        let app = axum::Router::new()
            .route(
                "/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{name}",
                patch(move |uri: axum::http::Uri, axum::Json(body): axum::Json<Value>| {
                    let p = p.clone();
                    async move {
                        p.lock().unwrap().push((uri.path().to_string(), body));
                        axum::Json(json!({}))
                    }
                }),
            )
            .fallback(|| async { axum::Json(json!({})) });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let m = VmManager::new(None, "n1", reqwest::Client::new(), &url);
        let mut obj = vmi("n1");
        obj["metadata"]["deletionTimestamp"] = json!("2026-09-27T00:00:00Z");
        obj["metadata"]["finalizers"] = json!([FINALIZER]);
        obj["metadata"]["resourceVersion"] = json!("9");
        m.sync(&[obj]).await;

        assert!(m.running().await.is_empty(), "a VMI being deleted is not started");
        let got = patches.lock().unwrap().clone();
        let rm: Vec<_> = got.iter().filter(|(path, _)| !path.ends_with("/status")).collect();
        assert_eq!(rm.len(), 1, "{got:?}");
        assert_eq!(rm[0].0, "/apis/kubevirt.io/v1/namespaces/default/virtualmachineinstances/web-1");
        assert_eq!(rm[0].1["metadata"]["finalizers"], json!([]));
        assert_eq!(rm[0].1["metadata"]["resourceVersion"], "9");
    }

    /// The console doors resolve a VM out of the run directory, so a machine
    /// this kubelet started has to leave a registration there — and take it
    /// away again (rustkube-node#38).
    #[test]
    fn a_machine_is_registered_for_the_console_and_deregistered_with_it() {
        let run = tempfile::tempdir().unwrap();
        let root = run.path().to_str().unwrap();
        let vm: VmSpec = stormvm_spec::kube::from_kube(&vmi("n1")).unwrap();
        let logging = Logging::pod("/var/log/pods/default_web-1_u-1/web-1", 0);

        // The namespace comes from the spec now, not from a separate
        // argument — a VM is qualified by it everywhere (stormvm#6).
        let reg = stormvm_node::console::Registration::of(&vm, "u-1", &logging, root);
        stormvm_node::console::write(root, &reg).unwrap();

        // Found by namespace *and* name: two `web-1`s in two namespaces must
        // not put a terminal on the wrong guest.
        let found = stormvm_node::console::find(root, "default", "web-1");
        assert!(found.is_some(), "the door should be registered");
        assert_eq!(found.unwrap().uid, "u-1");
        assert!(stormvm_node::console::find(root, "staging", "web-1").is_none());

        stormvm_node::console::remove(root, "default", "web-1").unwrap();
        assert!(
            stormvm_node::console::find(root, "default", "web-1").is_none(),
            "a registration that outlives its machine is a door onto a socket \
             nothing is bound to"
        );
        // Absent is success, so a stop racing a failed start is not an error.
        assert!(stormvm_node::console::remove(root, "default", "web-1").is_ok());
    }

    /// Volume names carry the namespace, or two VMs share one volume.
    ///
    /// The silent half of #41: stormblock's namespace is flat and a VM's is
    /// not, and `clone_volume` does not check uniqueness — so `default/web-1`
    /// and `staging/web-1` both asked for `web-1-root`, and `volume_by_name`
    /// returned whichever the map happened to iterate first.
    #[test]
    fn two_namespaces_do_not_share_a_volume_name() {
        let a: VmSpec = stormvm_spec::kube::from_kube(&vmi("n1")).unwrap();
        let mut other = vmi("n1");
        other["metadata"]["namespace"] = json!("staging");
        let b: VmSpec = stormvm_spec::kube::from_kube(&other).unwrap();

        let an = stormvm_node::start::volume_name(&a, "root");
        let bn = stormvm_node::start::volume_name(&b, "root");
        assert_ne!(an, bn, "two namespaces, one volume name");
        assert!(an.starts_with("default."), "{an}");
        assert!(bn.starts_with("staging."), "{bn}");
        // And the label that groups a VM's volumes is qualified too.
        assert_eq!(a.id(), "default/web-1");
        assert_ne!(a.id(), b.id());
    }

    /// The namespace is part of what a NIC is named after (rustkube-node#39).
    ///
    /// Asserted here, not only in `stormvm-net`, because the bug this replaced
    /// lived in this crate: two VMs of one name in two namespaces derived the
    /// same tap name and the same MAC, which presents as intermittent
    /// connectivity for both with the ARP table the only evidence.
    #[test]
    fn two_namespaces_do_not_share_a_tap_or_a_mac() {
        let a_tap = stormvm_net::tap_name("default", "web-1", "net0");
        let b_tap = stormvm_net::tap_name("staging", "web-1", "net0");
        assert_ne!(a_tap, b_tap);
        assert_ne!(
            stormvm_net::mac_for("default", "web-1", "net0"),
            stormvm_net::mac_for("staging", "web-1", "net0")
        );
        // Still inside the kernel's 15-byte interface-name limit, which is
        // what the hash is for.
        assert!(a_tap.len() <= 15, "{a_tap}");
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
    ///
    /// Retried, not recorded Failed for good (#76): a standalone VMI has no
    /// run strategy saying to give up. Not again before its backoff, and at
    /// once when its spec changes.
    #[tokio::test]
    async fn a_failed_start_is_retried_with_backoff_not_given_up() {
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "http://127.0.0.1:1");
        let attempts = |m: &VmManager| m.retries.lock().unwrap().get("u-1").map(|r| (r.attempts, r.generation));
        m.sync(&[vmi("n1")]).await;
        assert!(m.running().await.is_empty(), "not recorded: it will be tried again");
        assert_eq!(attempts(&m), Some((1, 0)));

        // Inside the backoff: not tried.
        m.sync(&[vmi("n1")]).await;
        assert_eq!(attempts(&m), Some((1, 0)));

        // A new spec is tried at once, and counts from one.
        let mut changed = vmi("n1");
        changed["metadata"]["generation"] = json!(2);
        m.sync(&[changed]).await;
        assert_eq!(attempts(&m), Some((1, 2)));

        // No longer wanted: forgotten.
        m.sync(&[]).await;
        assert_eq!(attempts(&m), None);
    }

    #[tokio::test]
    async fn uid_adapter_preserves_start_backoff_and_forgets_deleted_uid() {
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "http://127.0.0.1:1");
        let mut obj = vmi("n1");
        m.reconcile_one("u-1", Some(&obj)).await.unwrap();
        m.reconcile_one("u-1", Some(&obj)).await.unwrap();
        assert_eq!(m.retries.lock().unwrap()["u-1"].attempts, 1);
        obj["metadata"]["generation"] = json!(2);
        m.reconcile_one("u-1", Some(&obj)).await.unwrap();
        assert_eq!(m.retries.lock().unwrap()["u-1"].generation, 2);
        assert_eq!(m.retries.lock().unwrap()["u-1"].attempts, 1);
        m.reconcile_one("u-1", None).await.unwrap();
        assert!(!m.retries.lock().unwrap().contains_key("u-1"));
    }

    #[tokio::test]
    async fn uid_teardown_detaches_but_keeps_vm_owned_disks() {
        use axum::{routing::delete, http::StatusCode};
        let deleted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = deleted.clone();
        let app = axum::Router::new()
            .route("/api/v1/volumes/disk/attach", delete(|| async { StatusCode::OK }))
            .route("/api/v1/volumes/disk", delete(move || {
                let seen = seen.clone();
                async move { seen.store(true, std::sync::atomic::Ordering::SeqCst); StatusCode::OK }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "")
            .with_storage(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()));
        m.record_failure("u-1", &vmi("n1"), "fixture").await;
        {
            let mut records = m.vms.lock().await;
            let vm = records.get_mut("u-1").unwrap();
            vm.disks.push(ResolvedDisk { name: "root".into(), device: "/dev/test".into(),
                volume_id: Some("disk".into()), readonly: false, bus: Default::default() });
            vm.owned_volumes.push("disk".into());
        }
        m.reconcile_one("u-1", None).await.unwrap();
        assert!(m.running().await.is_empty());
        assert!(!deleted.load(std::sync::atomic::Ordering::SeqCst));
        server.abort();
    }

    /// A VirtualMachine with `runStrategy: Once` asked for no second try: that
    /// one is recorded Failed with the reason, as before (#76).
    #[tokio::test]
    async fn a_failed_start_under_run_strategy_once_is_failed_with_a_reason() {
        use axum::routing::get;
        let app = axum::Router::new()
            .route(
                "/apis/kubevirt.io/v1/namespaces/default/virtualmachines/web",
                get(|| async { axum::Json(json!({"spec": {"runStrategy": "Once"}})) }),
            )
            .fallback(|| async { axum::Json(json!({})) });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let m = VmManager::new(None, "n1", reqwest::Client::new(), &url);
        let mut obj = vmi("n1");
        obj["metadata"]["ownerReferences"] = json!([{"kind": "VirtualMachine", "name": "web"}]);
        m.sync(&[obj]).await;
        let all = m.running().await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].phase, Phase::Failed);
        assert!(all[0].message.contains("no ring"), "{}", all[0].message);
    }

    /// A clone made for a start is known as fresh before anything else can
    /// fail, and is deleted with it, so a retried start leaves nothing
    /// behind (#76).
    #[tokio::test]
    async fn a_failed_start_deletes_the_clone_it_made() {
        use axum::routing::{delete, post};
        let deleted: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let d = deleted.clone();
        let app = axum::Router::new()
            .route(
                "/api/v1/volumes/{g}/clone",
                post(|| async { axum::Json(json!({"id": "clone-1"})) }),
            )
            // Nothing made before: the root is cloned (#75 looks first).
            .route(
                "/api/v1/volumes",
                axum::routing::get(|| async { axum::Json(json!({"items": []})) }),
            )
            // The attach fails, after the clone was made.
            .route(
                "/api/v1/volumes/{id}/attach",
                post(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no ublk") }),
            )
            .route(
                "/api/v1/volumes/{id}",
                delete(move |axum::extract::Path(id): axum::extract::Path<String>| {
                    let d = d.clone();
                    async move {
                        d.lock().unwrap().push(id);
                        axum::Json(json!({}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "")
            .with_storage(crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none()));

        let mut obj = vmi("n1");
        obj["spec"]["domain"]["devices"] = json!({"disks": [{"name": "root", "disk": {"bus": "virtio"}}]});
        obj["spec"]["volumes"] = json!([{"name": "root", "containerDisk": {"image": "fedora-43"}}]);
        let vm: VmSpec = stormvm_spec::kube::from_kube(&obj).unwrap();
        let mut fresh = Vec::new();
        let r = m.resolve_disks_with_keys(&vm, &[], &Value::Null, &mut fresh).await;
        assert!(matches!(r, Err(StartFail::Failed(_))), "{r:?}");
        assert_eq!(fresh, vec!["clone-1".to_string()]);
        m.delete_volumes("web-1", &fresh).await;
        assert_eq!(*deleted.lock().unwrap(), vec!["clone-1".to_string()]);
    }

    #[test]
    fn retries_back_off_from_ten_seconds_to_five_minutes() {
        let d = |n| retry_delay(n).as_secs();
        assert_eq!((d(1), d(2), d(3), d(4), d(5), d(6), d(7)), (10, 20, 40, 80, 160, 300, 300));
        assert_eq!(d(1000), 300);
        assert!(!gives_up(None));
        assert!(!gives_up(Some(&json!({"spec": {"running": true}}))));
        assert!(!gives_up(Some(&json!({"spec": {"runStrategy": "RerunOnFailure"}}))));
        assert!(gives_up(Some(&json!({"spec": {"runStrategy": "Manual"}}))));
        let owned = json!({"metadata": {"ownerReferences": [{"kind": "Pod", "name": "p"}, {"kind": "VirtualMachine", "name": "vm"}]}});
        assert_eq!(owner_vm(&owned), Some("vm"));
        assert_eq!(owner_vm(&json!({"metadata": {}})), None);
    }

    /// An apiserver and an engine on one port (their paths do not overlap):
    /// a claim `default/data` bound to the stormblock volume `vol-claim`, the
    /// pods `pods` lists, and an engine whose volumes are `vols`. Creates are
    /// recorded, attaches answer a device, and any write to the apiserver
    /// (the claim's binding) is accepted.
    // ---- #75: a VM's disks outlive its VMI ----

    #[test]
    fn a_vms_disks_belong_to_the_virtual_machine_not_the_vmi() {
        let mut obj = vmi("n1");
        obj["metadata"]["ownerReferences"] =
            json!([{"kind": "VirtualMachine", "name": "web", "uid": "vm-uid"}]);
        assert_eq!(
            disk_owner(&obj, None),
            json!({"kind": "VirtualMachine", "namespace": "default", "name": "web", "uid": "vm-uid"})
        );
        // No VirtualMachine: the VMI owns its own.
        assert_eq!(
            disk_owner(&vmi("n1"), None),
            json!({"kind": "VirtualMachineInstance", "namespace": "default", "name": "web-1", "uid": "u-1"})
        );
        // Retained, on the VMI or on its VM: no owner, so nothing deletes them.
        let mut kept = obj.clone();
        kept["metadata"]["annotations"] = json!({ RETAIN_ANNOTATION: "true" });
        assert!(disk_owner(&kept, None).is_null());
        let vm = json!({"metadata": {"annotations": { RETAIN_ANNOTATION: "true" }}});
        assert!(disk_owner(&obj, Some(&vm)).is_null());
    }

    #[test]
    fn only_a_404_another_uid_or_a_deletion_says_the_owner_is_gone() {
        let owner = json!({"kind": "VirtualMachine", "namespace": "web", "name": "a", "uid": "u1"});
        assert_eq!(owner_path(&owner).as_deref(), Some("/apis/kubevirt.io/v1/namespaces/web/virtualmachines/a"));
        assert_eq!(owner_path(&json!({"kind": "PersistentVolumeClaim", "namespace": "x", "name": "y"})), None);
        assert!(owner_gone(404, None, &owner));
        assert!(!owner_gone(200, Some(&json!({"metadata": {"uid": "u1"}})), &owner));
        assert!(owner_gone(200, Some(&json!({"metadata": {"uid": "u2"}})), &owner));
        assert!(owner_gone(200, Some(&json!({"metadata": {"uid": "u1", "deletionTimestamp": "t"}})), &owner));
        // Could not ask is not gone.
        for code in [500, 503, 403, 401] {
            assert!(!owner_gone(code, None, &owner), "{code}");
        }
        assert!(!owner_gone(200, None, &owner));
    }

    /// An engine for the #75 tests: volumes with owners, clone/create/delete,
    /// owner updates; and an apiserver answering for owners by path.
    struct DiskEngine {
        url: String,
        vols: Arc<std::sync::Mutex<Vec<Value>>>,
        cloned: Arc<std::sync::Mutex<Vec<Value>>>,
        deleted: Arc<std::sync::Mutex<Vec<String>>>,
    }

    async fn disk_engine(vols: Vec<Value>, owners: Vec<(&'static str, u16, Value)>) -> DiskEngine {
        use axum::extract::Path;
        use axum::routing::{get, post, put};
        type Shared<T> = Arc<std::sync::Mutex<T>>;
        let vols: Shared<Vec<Value>> = Arc::new(std::sync::Mutex::new(vols));
        let cloned: Shared<Vec<Value>> = Arc::default();
        let deleted: Shared<Vec<String>> = Arc::default();
        let (v1, v2, v3, v4, v5) = (vols.clone(), vols.clone(), vols.clone(), vols.clone(), vols.clone());
        let (c1, d1) = (cloned.clone(), deleted.clone());
        let mut app = axum::Router::new()
            .route(
                "/api/v1/volumes",
                get(move || {
                    let v = v1.clone();
                    async move { axum::Json(json!({ "items": v.lock().unwrap().clone() })) }
                })
                .post(move |axum::Json(b): axum::Json<Value>| {
                    let v = v2.clone();
                    async move {
                        let mut v = v.lock().unwrap();
                        // Never reused, as the engine's are not.
                        let id = format!("vol-{}", uuid::Uuid::new_v4().simple());
                        v.push(json!({"id": id, "name": b["name"], "owner": b["owner"]}));
                        axum::Json(json!({ "id": id }))
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{g}/clone",
                post(move |axum::Json(b): axum::Json<Value>| {
                    let (v, c) = (v3.clone(), c1.clone());
                    async move {
                        let mut v = v.lock().unwrap();
                        // Never reused, as the engine's are not.
                        let id = format!("vol-{}", uuid::Uuid::new_v4().simple());
                        v.push(json!({"id": id, "name": b["name"], "owner": b["owner"]}));
                        c.lock().unwrap().push(b);
                        axum::Json(json!({ "id": id }))
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{id}/attach",
                post(|Path(id): Path<String>| async move {
                    axum::Json(json!({ "device_hint": format!("/dev/ublk-{id}") }))
                })
                .delete(|| async { axum::Json(json!({})) }),
            )
            .route(
                "/api/v1/volumes/{id}/owner",
                put(move |Path(id): Path<String>, axum::Json(b): axum::Json<Value>| {
                    let v = v4.clone();
                    async move {
                        for x in v.lock().unwrap().iter_mut() {
                            if x["id"] == id {
                                x["owner"] = b["owner"].clone();
                            }
                        }
                        axum::Json(json!({}))
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{id}",
                axum::routing::delete(move |Path(id): Path<String>| {
                    let (v, d) = (v5.clone(), d1.clone());
                    async move {
                        v.lock().unwrap().retain(|x| x["id"] != id);
                        d.lock().unwrap().push(id);
                        axum::Json(json!({}))
                    }
                }),
            )
            .route("/api/v1/volumes/{id}/cidata", post(|| async { axum::Json(json!({})) }));
        for (path, code, body) in owners {
            app = app.route(
                path,
                get(move || {
                    let body = body.clone();
                    async move { (axum::http::StatusCode::from_u16(code).unwrap(), axum::Json(body)) }
                }),
            );
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        DiskEngine { url, vols, cloned, deleted }
    }

    fn disk_manager(e: &DiskEngine, api: &str) -> VmManager {
        VmManager::new(None, "n1", reqwest::Client::new(), api)
            .with_storage(crate::engine::EngineClient::new(&e.url, crate::engine::TokenSource::none()))
    }

    /// A VMI of VirtualMachine `web` (uid `vm-uid`): a golden root and a seed.
    fn vm_owned_vmi(uid: &str) -> Value {
        let mut obj = vmi("n1");
        obj["metadata"]["uid"] = json!(uid);
        obj["metadata"]["ownerReferences"] =
            json!([{"kind": "VirtualMachine", "name": "web", "uid": "vm-uid"}]);
        obj["spec"]["domain"]["devices"] = json!({"disks": [
            {"name": "root", "disk": {"bus": "virtio"}},
            {"name": "cloudinit", "disk": {"bus": "virtio"}},
        ]});
        obj["spec"]["volumes"] = json!([
            {"name": "root", "containerDisk": {"image": "fedora-43"}},
            {"name": "cloudinit", "cloudInitNoCloud": {"userData": "#cloud-config\n"}},
        ]);
        obj
    }

    /// **The issue.** A VirtualMachine restart is a new VMI: its root is the
    /// one the first start cloned, found by name, and the golden is cloned
    /// once. The seed is made again (its keys can change) and the old one
    /// goes, so there is never a second volume under its name.
    #[tokio::test]
    async fn a_restart_reattaches_the_root_it_left_and_clones_once() {
        let e = disk_engine(vec![], vec![]).await;
        let m = disk_manager(&e, "");
        let first = vm_owned_vmi("vmi-1");
        let owner = disk_owner(&first, None);
        let spec: VmSpec = stormvm_spec::kube::from_kube(&first).unwrap();
        let (disks, owned) = m.resolve_disks_with_keys(&spec, &[], &owner, &mut Vec::new()).await.unwrap();
        let root = disks[0].volume_id.clone().unwrap();
        let seed = disks[1].volume_id.clone().unwrap();
        assert_eq!(owned, vec![root.clone(), seed.clone()]);
        assert_eq!(e.cloned.lock().unwrap().len(), 1);
        assert_eq!(e.cloned.lock().unwrap()[0]["owner"], owner);
        assert_eq!(e.cloned.lock().unwrap()[0]["name"], json!("default.web-1-root"));

        // Stopped: detached, nothing deleted.
        let vm = Vm {
            namespace: "default".into(),
            name: "web-1".into(),
            uid: "vmi-1".into(),
            log_dir: String::new(),
            handle: Handle::NONE,
            disks,
            phase: Phase::Succeeded,
            exit_code: 0,
            message: String::new(),
            started_unix: 1,
            ready_unix: None,
            owned_volumes: owned,
            nics: vec![],
            access: Access::default(),
        };
        m.stop(&vm).await;
        assert!(e.deleted.lock().unwrap().is_empty(), "a stop deletes nothing");

        // The restart: a new VMI of the same VirtualMachine.
        let second = vm_owned_vmi("vmi-2");
        let spec: VmSpec = stormvm_spec::kube::from_kube(&second).unwrap();
        let (again, _) = m
            .resolve_disks_with_keys(&spec, &[], &disk_owner(&second, None), &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(again[0].volume_id.as_deref(), Some(root.as_str()), "the same root");
        assert_eq!(e.cloned.lock().unwrap().len(), 1, "not cloned again");
        assert_eq!(*e.deleted.lock().unwrap(), vec![seed.clone()], "only the old seed goes");
        assert_ne!(again[1].volume_id.as_deref(), Some(seed.as_str()));
        let names: Vec<Value> = e.vols.lock().unwrap().iter().map(|v| v["name"].clone()).collect();
        assert_eq!(names.iter().filter(|n| **n == json!("default.web-1-cloudinit")).count(), 1);
    }

    /// A VM deleted and made again under its name is a new machine: the old
    /// one's root is not booted, and the start waits for the sweep.
    #[tokio::test]
    async fn a_root_left_by_an_earlier_vm_of_the_name_is_not_reused() {
        let old = json!({"id": "old-root", "name": "default.web-1-root",
                         "owner": {"kind": "VirtualMachine", "namespace": "default", "name": "web", "uid": "earlier"}});
        let e = disk_engine(vec![old], vec![]).await;
        let m = disk_manager(&e, "");
        let obj = vm_owned_vmi("vmi-1");
        let spec: VmSpec = stormvm_spec::kube::from_kube(&obj).unwrap();
        let r = m.resolve_disks_with_keys(&spec, &[], &disk_owner(&obj, None), &mut Vec::new()).await;
        match r {
            Err(StartFail::Waiting(why)) => assert!(why.contains("earlier VirtualMachine"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(e.cloned.lock().unwrap().is_empty());
    }

    /// A disk made before owners existed gets one when it is found, so the
    /// sweep can find it later.
    #[tokio::test]
    async fn a_found_disk_is_given_its_owner() {
        let e = disk_engine(vec![json!({"id": "r", "name": "default.web-1-root"})], vec![]).await;
        let m = disk_manager(&e, "");
        let obj = vm_owned_vmi("vmi-1");
        let spec: VmSpec = stormvm_spec::kube::from_kube(&obj).unwrap();
        let owner = disk_owner(&obj, None);
        m.resolve_disks_with_keys(&spec, &[], &owner, &mut Vec::new()).await.unwrap();
        let root = e.vols.lock().unwrap().iter().find(|v| v["id"] == "r").cloned().unwrap();
        assert_eq!(root["owner"], owner);
    }

    /// The sweep deletes exactly the disks whose owner is gone for good.
    #[tokio::test]
    async fn the_sweep_deletes_only_what_a_gone_owner_left() {
        let owned = |id: &str, kind: &str, name: &str, uid: &str| {
            json!({"id": id, "name": id, "owner": {"kind": kind, "namespace": "default", "name": name, "uid": uid}})
        };
        let vols = vec![
            owned("deleted-vm", "VirtualMachine", "a", "ua"),
            owned("live-vm", "VirtualMachine", "b", "ub"),
            owned("replaced-vm", "VirtualMachine", "c", "uc"),
            owned("unanswered", "VirtualMachine", "d", "ud"),
            owned("deleting-vmi", "VirtualMachineInstance", "e", "ue"),
            {
                let mut v = owned("in-use", "VirtualMachine", "a", "ua");
                v["in_use"] = json!(true);
                v
            },
            owned("held-here", "VirtualMachine", "a", "ua"),
            json!({"id": "no-owner", "name": "no-owner"}),
            json!({"id": "a-claim", "name": "a-claim",
                   "owner": {"kind": "PersistentVolumeClaim", "namespace": "default", "name": "a"}}),
        ];
        let p = |plural: &str, n: &str| format!("/apis/kubevirt.io/v1/namespaces/default/{plural}/{n}");
        let owners: Vec<(&'static str, u16, Value)> = vec![
            (Box::leak(p("virtualmachines", "b").into_boxed_str()), 200, json!({"metadata": {"uid": "ub"}})),
            (Box::leak(p("virtualmachines", "c").into_boxed_str()), 200, json!({"metadata": {"uid": "new"}})),
            (Box::leak(p("virtualmachines", "d").into_boxed_str()), 500, json!({})),
            (Box::leak(p("virtualmachineinstances", "e").into_boxed_str()), 200,
             json!({"metadata": {"uid": "ue", "deletionTimestamp": "2026-09-29T00:00:00Z"}})),
        ];
        // `a` has no route: 404.
        let e = disk_engine(vols, owners).await;
        let m = disk_manager(&e, &e.url);
        m.vms.lock().await.insert("x".into(), Vm {
            namespace: "default".into(),
            name: "x".into(),
            uid: "x".into(),
            log_dir: String::new(),
            handle: Handle::NONE,
            disks: vec![ResolvedDisk {
                name: "root".into(),
                device: "/dev/x".into(),
                volume_id: Some("held-here".into()),
                readonly: false,
                bus: stormvm_spec::DiskBus::Virtio,
            }],
            phase: Phase::Running,
            exit_code: 0,
            message: String::new(),
            started_unix: 1,
            ready_unix: None,
            owned_volumes: vec![],
            nics: vec![],
            access: Access::default(),
        });

        m.sweep_orphans().await;
        let mut gone = e.deleted.lock().unwrap().clone();
        gone.sort();
        assert_eq!(gone, vec!["deleted-vm", "deleting-vmi", "replaced-vm"]);

        // Not again within the minute.
        e.vols.lock().unwrap().push(owned("later", "VirtualMachine", "a", "ua"));
        m.sweep_orphans().await;
        assert_eq!(e.deleted.lock().unwrap().len(), 3);
    }

    struct Fake {
        url: String,
        vols: Arc<std::sync::Mutex<Vec<(String, String)>>>,
        created: Arc<std::sync::Mutex<Vec<Value>>>,
        pods: Arc<std::sync::Mutex<Value>>,
        list_fails: Arc<std::sync::atomic::AtomicBool>,
    }

    async fn fake() -> Fake {
        use axum::routing::{get, post};
        type Shared<T> = Arc<std::sync::Mutex<T>>;
        let vols: Shared<Vec<(String, String)>> =
            Arc::new(std::sync::Mutex::new(vec![("vol-claim".into(), "pvc-default-data".into())]));
        let created: Shared<Vec<Value>> = Arc::default();
        let pods: Shared<Value> = Arc::new(std::sync::Mutex::new(json!({"items": []})));
        let list_fails = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let (v1, v2, c1, p1, f1) =
            (vols.clone(), vols.clone(), created.clone(), pods.clone(), list_fails.clone());
        let app = axum::Router::new()
            .route(
                "/api/v1/volumes",
                get(move || {
                    let (v, f) = (v1.clone(), f1.clone());
                    async move {
                        if f.load(std::sync::atomic::Ordering::SeqCst) {
                            return (axum::http::StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({})));
                        }
                        let items: Vec<Value> = v
                            .lock()
                            .unwrap()
                            .iter()
                            .map(|(id, name)| json!({"id": id, "name": name}))
                            .collect();
                        (axum::http::StatusCode::OK, axum::Json(json!({ "items": items })))
                    }
                })
                .post(move |axum::Json(body): axum::Json<Value>| {
                    let (v, c) = (v2.clone(), c1.clone());
                    async move {
                        let id = format!("vol-{}", v.lock().unwrap().len());
                        v.lock().unwrap().push((id.clone(), body["name"].as_str().unwrap().into()));
                        c.lock().unwrap().push(body);
                        axum::Json(json!({ "id": id }))
                    }
                }),
            )
            .route(
                "/api/v1/volumes/{id}/attach",
                post(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                    axum::Json(json!({ "device_hint": format!("/dev/ublk-{id}") }))
                }),
            )
            .route(
                "/api/v1/namespaces/default/persistentvolumeclaims/data",
                get(|| async {
                    axum::Json(json!({
                        "metadata": {"name": "data", "namespace": "default"},
                        "spec": {
                            "storageClassName": "stormblock",
                            "volumeName": "pvc-default-data",
                            "resources": {"requests": {"storage": "1Gi"}}
                        }
                    }))
                }),
            )
            .route(
                "/api/v1/persistentvolumes/pvc-default-data",
                get(|| async {
                    axum::Json(json!({
                        "metadata": {"name": "pvc-default-data"},
                        "spec": {"csi": {"driver": "stormblock.storm.io", "volumeHandle": "pvc-default-data"}}
                    }))
                }),
            )
            .route(
                "/api/v1/pods",
                get(move || {
                    let p = p1.clone();
                    async move { axum::Json(p.lock().unwrap().clone()) }
                }),
            )
            .fallback(|| async { axum::Json(json!({})) });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Fake { url, vols, created, pods, list_fails }
    }

    fn manager_for(f: &Fake, with_claims: bool) -> VmManager {
        let engine =
            crate::engine::EngineClient::new(&f.url, crate::engine::TokenSource::none());
        let m = VmManager::new(None, "n1", reqwest::Client::new(), "").with_storage(engine.clone());
        if !with_claims {
            return m;
        }
        let rt = Arc::new(crate::pod_manager::tests::FakeRuntime::default());
        let pods = crate::pod_manager::PodManager::with_api(
            rt.clone(),
            rt,
            "n1",
            &f.url,
            "127.0.0.1",
            reqwest::Client::new(),
        )
        .with_engine(engine);
        m.with_claims(Arc::new(pods))
    }

    /// A VMI with one disk, `data`, from the given volume source.
    fn vm_with(volume: Value) -> VmSpec {
        let mut obj = vmi("n1");
        obj["spec"]["domain"]["devices"] = json!({ "disks": [{ "name": "data", "disk": { "bus": "virtio" } }] });
        let mut v = volume;
        v["name"] = json!("data");
        obj["spec"]["volumes"] = json!([v]);
        stormvm_spec::kube::from_kube(&obj).unwrap()
    }

    /// An emptyDisk is made blank once, owned by the VM, and found again on
    /// the next start rather than made again (#73).
    #[tokio::test]
    async fn an_empty_disk_is_created_once_and_reused() {
        let f = fake().await;
        let m = manager_for(&f, false);
        let vm = vm_with(json!({ "emptyDisk": { "capacity": "2Gi" } }));
        assert_eq!(vm.disks[0].from, DiskSource::Empty);

        let (disks, owned) = m.resolve_disks(&vm).await.unwrap();
        let made = f.created.lock().unwrap().clone();
        assert_eq!(made.len(), 1);
        assert_eq!(made[0]["name"], json!(stormvm_node::start::volume_name(&vm, "data")));
        assert_eq!(made[0]["size"], json!("2Gi"));
        assert_eq!(made[0]["label"], json!("storm.io/vm=default/web-1"));
        // The engine's default redundancy, not the seed's `none`: this is data.
        assert!(made[0].get("redundancy").is_none(), "{}", made[0]);
        let id = disks[0].volume_id.clone().unwrap();
        assert_eq!(owned, vec![id.clone()], "the VM owns its empty disk");

        // Started again: the same volume, nothing new made.
        let (again, _) = m.resolve_disks(&vm).await.unwrap();
        assert_eq!(again[0].volume_id.as_deref(), Some(id.as_str()));
        assert_eq!(f.created.lock().unwrap().len(), 1);
    }

    /// An engine that cannot list is not an engine with no such volume. A
    /// blank made then would replace the guest's data after a restart.
    #[tokio::test]
    async fn an_empty_disk_is_not_made_when_the_engine_cannot_say_it_exists() {
        let f = fake().await;
        f.list_fails.store(true, std::sync::atomic::Ordering::SeqCst);
        let m = manager_for(&f, false);
        let vm = vm_with(json!({ "emptyDisk": { "capacity": "2Gi" } }));
        assert!(matches!(m.resolve_disks(&vm).await, Err(StartFail::Failed(_))));
        assert!(f.created.lock().unwrap().is_empty());
    }

    /// A claim is resolved to its bound volume, attached, and not owned: the
    /// claim outlives the VM (#74).
    #[tokio::test]
    async fn a_claim_disk_is_its_bound_volume_and_not_the_vms() {
        let f = fake().await;
        let m = manager_for(&f, true);
        let vm = vm_with(json!({ "persistentVolumeClaim": { "claimName": "data" } }));
        assert_eq!(vm.disks[0].from, DiskSource::Claim("data".into()));

        let (disks, owned) = m.resolve_disks(&vm).await.unwrap();
        assert_eq!(disks[0].volume_id.as_deref(), Some("vol-claim"));
        assert_eq!(disks[0].device, "/dev/ublk-vol-claim");
        assert!(owned.is_empty(), "deleting the VM must not delete the claim's volume");
        assert!(f.created.lock().unwrap().is_empty());
        assert_eq!(f.vols.lock().unwrap().len(), 1);
    }

    /// A pod on this node using the claim: the VM waits, and says which pod.
    #[tokio::test]
    async fn a_claim_a_pod_here_uses_makes_the_vm_wait() {
        let f = fake().await;
        *f.pods.lock().unwrap() = json!({"items": [{
            "metadata": {"name": "db", "namespace": "default"},
            "spec": {"nodeName": "n1", "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "data"}}]},
            "status": {"phase": "Running"}
        }]});
        let m = manager_for(&f, true);
        let vm = vm_with(json!({ "persistentVolumeClaim": { "claimName": "data" } }));
        match m.resolve_disks(&vm).await {
            Err(StartFail::Waiting(why)) => assert!(why.contains("pod default/db"), "{why}"),
            other => panic!("expected Waiting, got {other:?}"),
        }
    }

    /// No apiserver to ask: a claim cannot be resolved, and it says so.
    #[tokio::test]
    async fn a_claim_disk_without_an_apiserver_fails_and_says_why() {
        let f = fake().await;
        let m = manager_for(&f, false);
        let vm = vm_with(json!({ "persistentVolumeClaim": { "claimName": "data" } }));
        match m.resolve_disks(&vm).await {
            Err(StartFail::Failed(why)) => assert!(why.contains("apiserver"), "{why}"),
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}

/// The cloud-init document inside a Secret, from `data` or `stringData`.
///
/// `data` is base64 and `stringData` is not. Both are checked because the
/// apiserver is supposed to fold the second into the first on write and
/// rustkube does not, so a secret written the documented way stays in
/// `stringData` and a reader that only knows `data` finds nothing.
///
/// The key is `userdata` by convention, but any single entry is taken when
/// that name is absent: a seed with one field and the wrong name is obviously
/// the seed, and failing there would mean a guest with no login over a
/// spelling.
fn seed_text(secret: &Value) -> Option<String> {
    use base64::Engine as _;
    let pick = |m: &Value| -> Option<(String, String)> {
        let obj = m.as_object()?;
        let (k, v) = obj
            .get_key_value("userdata")
            .or_else(|| obj.get_key_value("userData"))
            .or_else(|| if obj.len() == 1 { obj.iter().next() } else { None })?;
        Some((k.clone(), v.as_str()?.to_string()))
    };
    if let Some((_, raw)) = secret.get("stringData").and_then(|m| pick(m)) {
        if !raw.trim().is_empty() {
            return Some(raw);
        }
    }
    if let Some((_, b64)) = secret.get("data").and_then(|m| pick(m)) {
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64.trim()).ok()?;
        let text = String::from_utf8(bytes).ok()?;
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    None
}

#[cfg(test)]
mod seed_tests {
    use super::*;

    /// A seed written as `stringData` is still a seed.
    ///
    /// The apiserver is supposed to fold `stringData` into `data` on write.
    /// rustkube does not, so a secret created the documented way comes back
    /// exactly as written -- and a reader that only knows `data` finds
    /// nothing, which presents as a guest with no login and no reason.
    #[test]
    fn a_seed_is_read_from_string_data() {
        let s = serde_json::json!({
            "stringData": {"userdata": "#cloud-config\nhostname: web-1\n"}
        });
        assert!(seed_text(&s).unwrap().contains("hostname: web-1"));
    }

    /// And from `data`, which is base64.
    #[test]
    fn a_seed_is_read_from_base64_data() {
        use base64::Engine as _;
        let b = base64::engine::general_purpose::STANDARD.encode("#cloud-config\nssh_authorized_keys:\n");
        let s = serde_json::json!({"data": {"userdata": b}});
        assert!(seed_text(&s).unwrap().contains("ssh_authorized_keys"));
    }

    /// One field under another name is obviously the seed.
    ///
    /// Failing over a spelling would mean a guest nobody can log into.
    #[test]
    fn a_single_oddly_named_field_is_taken_as_the_seed() {
        let s = serde_json::json!({"stringData": {"user-data": "#cloud-config\n"}});
        assert_eq!(seed_text(&s).as_deref(), Some("#cloud-config\n"));
    }

    /// Nothing usable is None, not an empty document.
    #[test]
    fn an_empty_secret_is_no_seed_at_all() {
        assert!(seed_text(&serde_json::json!({})).is_none());
        assert!(seed_text(&serde_json::json!({"stringData": {"userdata": "  "}})).is_none());
        // Two fields, neither named: ambiguous, so not guessed.
        assert!(seed_text(&serde_json::json!({"stringData": {"a": "x", "b": "y"}})).is_none());
    }

}
