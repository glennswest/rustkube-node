//! A running VMI's cpu, memory and interface changes (#246, stormvm#24).
//!
//! stormvm renders the headroom a machine may grow into when it starts
//! (`cpu.maxSockets` → `maxcpus`, `memory.maxGuest` → a virtio-mem device,
//! spare PCIe root ports on qemu) and offers the verbs that use it
//! (`stormvm_control::hotplug`). This module is the kubelet's half that does
//! not touch the machine: what changed between the spec and what the machine
//! was last given, the record of that kept beside the machine's registration
//! (so a restarted kubelet knows it), and the [`Hotplug`] seam the verbs are
//! called through (a fake in tests).
//!
//! **The record, not the machine, is the baseline.** A change is a spec value
//! that differs from what the kubelet last gave the machine: at start, the
//! spec it was started from; after that, each change applied. A machine with
//! no headroom whose spec never changes is never asked anything, and one
//! started by a stormvm that counted vCPUs differently (stormvm#92) is not
//! "changed" by the count alone.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The condition a hotplug outcome is reported as on the VMI.
pub const CONDITION: &str = "HotplugSynchronized";

/// The record's file name, in the machine's run directory
/// (`<run root>/<namespace>/<name>/`).
pub const RECORD_FILE: &str = "kubelet-hotplug.json";

/// What the kubelet last gave a running machine.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// The vCPU total (`cpu.cores` in stormvm's spec: the product of
    /// KubeVirt's cores, sockets and threads).
    pub vcpus: u32,
    /// The guest memory in bytes (`memory.guest`), when it parsed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<u64>,
    /// Its NICs, in the order they were given (boot ones first).
    #[serde(default)]
    pub nics: Vec<NicRecord>,
}

/// One NIC the machine has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicRecord {
    /// The spec's name for it; qemu's device is `nic.<name>`.
    pub name: String,
    pub mac: String,
    /// The binding stormvm chose (`host-bridge`, `bridge`, `multus`, …).
    pub binding: String,
    /// For a secondary multus NIC: the interface its NAD's CNI ADD made in the
    /// VMI's sandbox (`net1`, …), DEL'd when the NIC goes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ifname: Option<String>,
}

impl Record {
    /// The record a machine started from `spec` has: what the spec says.
    pub fn from_spec(spec: &stormvm_spec::VmSpec, nics: Vec<NicRecord>) -> Record {
        Record { vcpus: want_vcpus(spec), memory: want_memory(spec), nics }
    }

    pub fn has_nic(&self, name: &str) -> bool {
        self.nics.iter().any(|n| n.name == name)
    }
}

/// The vCPU total a spec asks for.
pub fn want_vcpus(spec: &stormvm_spec::VmSpec) -> u32 {
    spec.cpu.cores.max(1)
}

/// The guest memory a spec asks for, in bytes.
pub fn want_memory(spec: &stormvm_spec::VmSpec) -> Option<u64> {
    stormvm_spec::parse_size(&spec.memory.size)
}

/// Where a machine's record lives.
pub fn record_path(run_root: &Path, namespace: &str, name: &str) -> PathBuf {
    run_root.join(namespace).join(name).join(RECORD_FILE)
}

/// Read a machine's record. `None` when there is none (a machine started by a
/// kubelet before #246) or it does not parse.
pub fn load(run_root: &Path, namespace: &str, name: &str) -> Option<Record> {
    let text = std::fs::read(record_path(run_root, namespace, name)).ok()?;
    serde_json::from_slice(&text).ok()
}

/// Write it whole and rename it into place: a crash leaves the old record or
/// the new one.
pub fn save(run_root: &Path, namespace: &str, name: &str, r: &Record) -> Result<(), String> {
    let path = record_path(run_root, namespace, name);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec(r).map_err(|e| e.to_string())?;
    std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
}

pub fn remove(run_root: &Path, namespace: &str, name: &str) {
    let _ = std::fs::remove_file(record_path(run_root, namespace, name));
}

/// What to do to bring a machine from `have` to `spec`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes {
    pub vcpus: Option<u32>,
    pub memory: Option<u64>,
    /// NICs the spec no longer names, last given first (so the newest ports
    /// free first).
    pub remove: Vec<String>,
    /// NICs the spec names that the machine lacks, in spec order. After the
    /// removals, which free their ports.
    pub add: Vec<String>,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        *self == Changes::default()
    }
}

pub fn changes(spec: &stormvm_spec::VmSpec, have: &Record) -> Changes {
    let vcpus = Some(want_vcpus(spec)).filter(|n| *n != have.vcpus);
    let memory = want_memory(spec).filter(|m| Some(*m) != have.memory);
    let remove = have
        .nics
        .iter()
        .rev()
        .filter(|n| !spec.interfaces.iter().any(|i| i.name == n.name))
        .map(|n| n.name.clone())
        .collect();
    let add = spec
        .interfaces
        .iter()
        .filter(|i| !have.has_nic(&i.name))
        .map(|i| i.name.clone())
        .collect();
    Changes { vcpus, memory, remove, add }
}

/// A machine's memory as the verbs report it, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryNow {
    pub boot: u64,
    /// What the guest has now (boot + plugged).
    pub current: u64,
    /// What was last asked for (boot + requested).
    pub requested: u64,
}

impl MemoryNow {
    /// The guest has not taken in (or given back) all it was asked to.
    pub fn settling(&self) -> bool {
        self.current != self.requested
    }
}

/// The hotplug verbs of one running machine, as the kubelet calls them.
/// `stormvm_control::Machine` in production; a fake in tests. Errors are the
/// verb's own words: they go on the VMI as they are.
#[async_trait::async_trait]
pub trait Hotplug: Send + Sync {
    /// Set the vCPU total. Returns how many are present now.
    async fn set_vcpus(&self, n: u32) -> Result<u32, String>;
    /// Ask for `bytes` of guest memory in all.
    async fn set_memory(&self, bytes: u64) -> Result<MemoryNow, String>;
    /// The memory now (how far a change has got).
    async fn memory(&self) -> Result<MemoryNow, String>;
    /// Plug a NIC in. Returns the port.
    async fn add_nic(&self, nic: stormvm_control::hotplug::HotNic) -> Result<String, String>;
    /// Take a NIC out; the guest has to release it.
    async fn remove_nic(&self, name: &str) -> Result<(), String>;
}

fn memory_now(m: stormvm_control::hotplug::MemorySize) -> MemoryNow {
    MemoryNow { boot: m.boot, current: m.total(), requested: m.boot + m.requested }
}

#[async_trait::async_trait]
impl Hotplug for stormvm_control::Machine {
    async fn set_vcpus(&self, n: u32) -> Result<u32, String> {
        stormvm_control::Machine::set_vcpus(self, n).await.map(|v| v.present).map_err(|e| e.to_string())
    }
    async fn set_memory(&self, bytes: u64) -> Result<MemoryNow, String> {
        stormvm_control::Machine::set_memory(self, bytes).await.map(memory_now).map_err(|e| e.to_string())
    }
    async fn memory(&self) -> Result<MemoryNow, String> {
        stormvm_control::Machine::memory(self).await.map(memory_now).map_err(|e| e.to_string())
    }
    async fn add_nic(&self, nic: stormvm_control::hotplug::HotNic) -> Result<String, String> {
        stormvm_control::Machine::add_nic(self, nic).await.map_err(|e| e.to_string())
    }
    async fn remove_nic(&self, name: &str) -> Result<(), String> {
        stormvm_control::Machine::remove_nic(self, name).await.map_err(|e| e.to_string())
    }
}

/// What the VMI's status says about a machine's size (KubeVirt's fields).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Size {
    /// `status.currentCPUTopology`: cores per socket, sockets, threads.
    pub topology: Option<(u32, u32, u32)>,
    pub memory: Option<MemoryNow>,
    /// `HotplugSynchronized`, once a change has been tried.
    pub condition: Option<Value>,
}

/// The topology `vcpus` makes in `spec`'s shape: the cores and threads per
/// socket are fixed at boot, and a change grows or shrinks sockets (KubeVirt's
/// live update). A total the shape does not divide is reported as one socket
/// of that many cores, which is what it is to the guest's count.
pub fn topology(spec: &stormvm_spec::VmSpec, vcpus: u32) -> (u32, u32, u32) {
    let cps = spec.cpu.cores_per_socket();
    let threads = spec.cpu.threads.max(1);
    let per_socket = cps * threads;
    if per_socket > 0 && vcpus % per_socket == 0 {
        (cps, vcpus / per_socket, threads)
    } else {
        (vcpus, 1, 1)
    }
}

/// KubeVirt's Quantity for a byte count: `Mi` when whole, else bytes.
pub fn quantity(bytes: u64) -> String {
    const MI: u64 = 1 << 20;
    if bytes % MI == 0 {
        format!("{}Mi", bytes / MI)
    } else {
        bytes.to_string()
    }
}

/// The status fields for `size`, merged into `status`.
pub fn write_status(status: &mut Value, size: &Size) {
    if let Some((cores, sockets, threads)) = size.topology {
        status["currentCPUTopology"] = json!({ "cores": cores, "sockets": sockets, "threads": threads });
    }
    if let Some(m) = size.memory {
        status["memory"] = json!({
            "guestAtBoot": quantity(m.boot),
            "guestCurrent": quantity(m.current),
            "guestRequested": quantity(m.requested),
        });
    }
}

/// The `HotplugSynchronized` condition: True when every change was applied,
/// False with each refusal (a verb's own words) when one was not. The
/// transition time moves only when the status does.
pub fn condition(prev: Option<&Value>, problems: &[String], now: &str) -> Value {
    let status = if problems.is_empty() { "True" } else { "False" };
    let since = prev
        .filter(|p| p["status"] == status)
        .and_then(|p| p["lastTransitionTime"].as_str())
        .unwrap_or(now)
        .to_string();
    let (reason, message) = if problems.is_empty() {
        ("Synchronized", "every cpu, memory and interface change reached the machine".to_string())
    } else {
        ("HotplugRefused", problems.join("; "))
    };
    json!({
        "type": CONDITION,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": since,
        "lastProbeTime": Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(obj: &Value) -> stormvm_spec::VmSpec {
        stormvm_spec::kube::from_kube(obj).unwrap()
    }

    fn vmi(cores: u64, sockets: u64, guest: &str, nics: &[&str]) -> Value {
        let interfaces: Vec<Value> = nics.iter().map(|n| json!({"name": n, "bridge": {}})).collect();
        // The first on the pod network, the rest on NADs of their names.
        let networks: Vec<Value> = nics
            .iter()
            .enumerate()
            .map(|(i, n)| if i == 0 { json!({"name": n, "pod": {}}) } else { json!({"name": n, "multus": {"networkName": n}}) })
            .collect();
        json!({
            "kind": "VirtualMachineInstance",
            "metadata": {"name": "web-1", "namespace": "default", "uid": "u-1"},
            "spec": {
                "domain": {
                    "cpu": {"cores": cores, "sockets": sockets, "maxSockets": 8},
                    "memory": {"guest": guest, "maxGuest": "8Gi"},
                    "devices": {"interfaces": interfaces},
                },
                "networks": networks,
            }
        })
    }

    fn nic(name: &str) -> NicRecord {
        NicRecord { name: name.into(), mac: format!("52:54:00:00:00:{:02x}", name.len()), binding: "host-bridge".into(), ifname: None }
    }

    /// The spec the machine was started from is no change; a grown socket
    /// count and memory are; NICs are removed newest first, added in spec
    /// order.
    #[test]
    fn changes_are_the_spec_against_what_the_machine_was_given() {
        let start = spec(&vmi(2, 2, "2Gi", &["default"]));
        let had = Record::from_spec(&start, vec![nic("default")]);
        assert_eq!(had.vcpus, 4, "2 cores × 2 sockets");
        assert_eq!(had.memory, Some(2 << 30));
        assert!(changes(&start, &had).is_empty());

        let grown = spec(&vmi(2, 3, "4Gi", &["default"]));
        let c = changes(&grown, &had);
        assert_eq!((c.vcpus, c.memory), (Some(6), Some(4 << 30)));
        assert!(c.add.is_empty() && c.remove.is_empty());

        let mut have = had.clone();
        have.nics = vec![nic("default"), nic("a"), nic("b")];
        let swapped = spec(&vmi(2, 2, "2Gi", &["default", "c", "d"]));
        let c = changes(&swapped, &have);
        assert_eq!(c.remove, vec!["b".to_string(), "a".to_string()]);
        assert_eq!(c.add, vec!["c".to_string(), "d".to_string()]);
        assert_eq!((c.vcpus, c.memory), (None, None));
    }

    #[test]
    fn the_record_round_trips_beside_the_machine() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path(), "default", "web-1"), None);
        let mut r = Record::from_spec(&spec(&vmi(1, 1, "1Gi", &["default"])), vec![nic("default")]);
        r.nics.push(NicRecord { ifname: Some("net1".into()), binding: "multus".into(), ..nic("vlan") });
        save(dir.path(), "default", "web-1", &r).unwrap();
        assert!(dir.path().join("default/web-1").join(RECORD_FILE).exists());
        assert_eq!(load(dir.path(), "default", "web-1"), Some(r));
        remove(dir.path(), "default", "web-1");
        assert_eq!(load(dir.path(), "default", "web-1"), None);
    }

    #[test]
    fn status_says_the_topology_memory_and_the_outcome() {
        let s = spec(&vmi(2, 2, "2Gi", &["default"]));
        assert_eq!(topology(&s, 6), (2, 3, 1), "three sockets of two");
        assert_eq!(topology(&s, 5), (5, 1, 1), "a total the shape does not divide");
        let mut status = json!({"phase": "Running"});
        write_status(&mut status, &Size {
            topology: Some((2, 3, 1)),
            memory: Some(MemoryNow { boot: 2 << 30, current: 3 << 30, requested: 4 << 30 }),
            condition: None,
        });
        assert_eq!(status["currentCPUTopology"], json!({"cores": 2, "sockets": 3, "threads": 1}));
        assert_eq!(status["memory"], json!({"guestAtBoot": "2048Mi", "guestCurrent": "3072Mi", "guestRequested": "4096Mi"}));

        let ok = condition(None, &[], "t1");
        assert_eq!((ok["status"].as_str(), ok["reason"].as_str()), (Some("True"), Some("Synchronized")));
        let bad = condition(Some(&ok), &["vCPUs 4 → 32: past maxcpus".into()], "t2");
        assert_eq!(bad["status"], "False");
        assert_eq!(bad["lastTransitionTime"], "t2", "the status moved");
        assert!(bad["message"].as_str().unwrap().contains("past maxcpus"));
        let still = condition(Some(&bad), &["again".into()], "t3");
        assert_eq!(still["lastTransitionTime"], "t2", "held while the status holds");
    }
}
