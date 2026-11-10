//! RDMA verbs for a pod that asks (#348, stormcos#914).
//!
//! Where the node has RDMA verbs devices (`/dev/infiniband/uverbsN`, one per
//! HCA port device the verbs modules registered, and `/dev/infiniband/rdma_cm`)
//! the Node advertises the extended resource [`RESOURCE`] with one unit per
//! uverbs device. A container that asks for it (`resources.limits` or
//! `requests`) gets every one of those device nodes and `CAP_IPC_LOCK`; a
//! container that does not ask gets neither. No device plugin process: the
//! kubelet reads the kernel's own record, as it does for KVM (#65).
//!
//! - **Devices** come from sysfs, not `/dev`: the kubelet's own container has
//!   no node `/dev`, and `/sys/class/infiniband_verbs/uverbsN` and
//!   `/sys/class/misc/rdma_cm` are there exactly while the kernel has the
//!   device registered. The binds name the node's `/dev/infiniband/<name>`,
//!   which the engine resolves in the node's mount view (as it does a raw
//!   block claim's device, #67).
//! - **Cgroup device access**: stormpump attaches no device filter to a
//!   workload's cgroup, so a node bound into the container is usable as is.
//! - **Memory registration**: the kernel pins a memory region against
//!   `RLIMIT_MEMLOCK` unless the task has `CAP_IPC_LOCK` (`ib_umem_get`), and
//!   stormpump's spec has no rlimit to raise. So the container gets
//!   `CAP_IPC_LOCK` added: registration is then bounded by memory, not the
//!   rlimit. A privileged container has it already. Raising the rlimit itself
//!   waits on stormpump (an rlimit in its spec).
//! - **All or nothing**: one container asking gets all of the node's verbs
//!   devices (a shared HCA, as the shared-device RDMA plugins give). The count
//!   on the Node is what the scheduler places by.

use std::path::Path;

use serde_json::Value;

use crate::cri::{ContainerConfig, Mount, MountPropagation};

/// The extended resource a pod asks for.
pub const RESOURCE: &str = "rdma.storm.io/hca";

/// The capability that lets a task pin memory past `RLIMIT_MEMLOCK`.
pub const IPC_LOCK: &str = "IPC_LOCK";

/// The node's RDMA verbs devices, by their names under `/dev/infiniband`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Devices {
    /// `uverbs0`, `uverbs1`, … in number order.
    pub uverbs: Vec<String>,
    /// Whether `rdma_cm` is registered.
    pub rdma_cm: bool,
}

impl Devices {
    /// The node's, from the kernel's record under `sys` (`/sys` outside tests).
    pub fn in_sys(sys: &Path) -> Devices {
        let mut uverbs: Vec<(u32, String)> = std::fs::read_dir(sys.join("class/infiniband_verbs"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let n = name.strip_prefix("uverbs")?.parse().ok()?;
                Some((n, name))
            })
            .collect();
        uverbs.sort();
        Devices {
            uverbs: uverbs.into_iter().map(|(_, n)| n).collect(),
            rdma_cm: sys.join("class/misc/rdma_cm").exists(),
        }
    }

    /// The node's now.
    pub fn now() -> Devices {
        Devices::in_sys(Path::new("/sys"))
    }

    /// How many units the Node advertises: one per uverbs device, and none
    /// without `rdma_cm` (the issue's "where both exist": SMB Direct and
    /// NFS/RDMA connect through the CM).
    pub fn units(&self) -> u32 {
        if self.rdma_cm {
            self.uverbs.len() as u32
        } else {
            0
        }
    }

    /// The device nodes a requesting container gets, as node paths.
    pub fn nodes(&self) -> Vec<String> {
        let mut out: Vec<String> = self.uverbs.iter().map(|n| format!("/dev/infiniband/{n}")).collect();
        if self.rdma_cm {
            out.push("/dev/infiniband/rdma_cm".into());
        }
        out
    }
}

/// The node's unit count now ([`Devices::units`]); the Node status's probe.
pub fn units_now() -> u32 {
    Devices::now().units()
}

/// Whether a container spec asks for [`RESOURCE`] (a positive amount in its
/// limits or requests; upstream requires the two to be equal for an extended
/// resource, and either alone is taken).
pub fn requested(container: &Value) -> bool {
    ["limits", "requests"].iter().any(|k| {
        let q = &container["resources"][*k][RESOURCE];
        q.as_str().and_then(|s| s.trim().parse::<f64>().ok()).or_else(|| q.as_f64()).is_some_and(|n| n > 0.0)
    })
}

/// Give a requesting container the devices and `CAP_IPC_LOCK`, or say why
/// it cannot have them (the node has none now).
pub fn apply(config: &mut ContainerConfig, devices: &Devices) -> Result<(), String> {
    if devices.units() == 0 {
        return Err(format!(
            "container {} asks for {RESOURCE}, but this node has no RDMA verbs devices now \
             (uverbs: {}, rdma_cm: {}): are the verbs modules loaded (stormcos#914)?",
            config.name,
            if devices.uverbs.is_empty() { "none".to_string() } else { devices.uverbs.join(", ") },
            if devices.rdma_cm { "yes" } else { "no" },
        ));
    }
    for node in devices.nodes() {
        if config.mounts.iter().any(|m| m.container_path == node) {
            continue;
        }
        config.mounts.push(Mount {
            container_path: node.clone(),
            host_path: node,
            readonly: false,
            propagation: MountPropagation::Private,
            selinux_relabel: false,
            fstype: None,
        });
    }
    if !config.privileged && !config.add_capabilities.iter().any(|c| cap_is(c, IPC_LOCK)) {
        config.add_capabilities.push(IPC_LOCK.into());
    }
    Ok(())
}

/// `IPC_LOCK` and `CAP_IPC_LOCK` are one capability, in any case.
fn cap_is(name: &str, cap: &str) -> bool {
    let n = name.trim().to_ascii_uppercase();
    n.strip_prefix("CAP_").unwrap_or(&n) == cap
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sys(uverbs: &[&str], rdma_cm: bool) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("class/infiniband_verbs")).unwrap();
        for u in uverbs {
            std::fs::create_dir_all(d.path().join("class/infiniband_verbs").join(u)).unwrap();
        }
        if rdma_cm {
            std::fs::create_dir_all(d.path().join("class/misc/rdma_cm")).unwrap();
        }
        d
    }

    #[test]
    fn devices_come_from_sysfs_in_number_order() {
        let d = sys(&["uverbs10", "uverbs2", "abi_version", "uverbs0"], true);
        let dev = Devices::in_sys(d.path());
        assert_eq!(dev.uverbs, vec!["uverbs0", "uverbs2", "uverbs10"]);
        assert_eq!(dev.units(), 3);
        assert_eq!(
            dev.nodes(),
            vec!["/dev/infiniband/uverbs0", "/dev/infiniband/uverbs2", "/dev/infiniband/uverbs10", "/dev/infiniband/rdma_cm"]
        );
        // No rdma_cm: nothing advertised.
        assert_eq!(Devices::in_sys(sys(&["uverbs0"], false).path()).units(), 0);
        // No verbs at all (no class directory).
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(Devices::in_sys(empty.path()), Devices::default());
    }

    #[test]
    fn a_container_asks_by_limits_or_requests() {
        assert!(requested(&json!({"resources": {"limits": {RESOURCE: "1"}}})));
        assert!(requested(&json!({"resources": {"requests": {RESOURCE: 2}}})));
        assert!(!requested(&json!({"resources": {"limits": {RESOURCE: "0"}}})));
        assert!(!requested(&json!({"resources": {"limits": {"cpu": "1"}}})));
        assert!(!requested(&json!({"name": "c"})));
    }

    #[test]
    fn a_requesting_container_gets_the_nodes_and_ipc_lock() {
        let dev = Devices { uverbs: vec!["uverbs0".into()], rdma_cm: true };
        let mut c = ContainerConfig { name: "smb".into(), ..Default::default() };
        apply(&mut c, &dev).unwrap();
        let binds: Vec<(&str, &str, Option<&String>)> =
            c.mounts.iter().map(|m| (m.container_path.as_str(), m.host_path.as_str(), m.fstype.as_ref())).collect();
        assert_eq!(
            binds,
            vec![
                ("/dev/infiniband/uverbs0", "/dev/infiniband/uverbs0", None),
                ("/dev/infiniband/rdma_cm", "/dev/infiniband/rdma_cm", None),
            ]
        );
        assert_eq!(c.add_capabilities, vec![IPC_LOCK]);
        // Again: nothing doubled.
        apply(&mut c, &dev).unwrap();
        assert_eq!((c.mounts.len(), c.add_capabilities.len()), (2, 1));

        // Asked for already, in another spelling; privileged has it.
        let mut asked = ContainerConfig { add_capabilities: vec!["CAP_IPC_LOCK".into()], ..Default::default() };
        apply(&mut asked, &dev).unwrap();
        assert_eq!(asked.add_capabilities, vec!["CAP_IPC_LOCK"]);
        let mut privileged = ContainerConfig { privileged: true, ..Default::default() };
        apply(&mut privileged, &dev).unwrap();
        assert!(privileged.add_capabilities.is_empty());
    }

    #[test]
    fn no_devices_is_a_refusal_naming_what_is_missing() {
        let mut c = ContainerConfig { name: "smb".into(), ..Default::default() };
        let e = apply(&mut c, &Devices { uverbs: vec!["uverbs0".into()], rdma_cm: false }).unwrap_err();
        assert!(e.contains("rdma.storm.io/hca") && e.contains("rdma_cm: no") && e.contains("uverbs: uverbs0"), "{e}");
        assert!(c.mounts.is_empty() && c.add_capabilities.is_empty());
    }
}
