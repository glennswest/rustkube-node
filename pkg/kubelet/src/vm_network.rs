//! A VMI on the pod network gets a sandbox of its own (#88, stormvm#16).
//!
//! A VM on `networks: [{pod: {}}]` has to *be* on the pod network: a pod IP
//! the CNI allocated, reachable cluster-wide, with the CNI's policy applied.
//! So each such VMI gets what a pod sandbox gets, from the same engine and the
//! same CNI:
//!
//! 1. a network namespace held by stormpump (`sandbox_acquire`, the isolated
//!    profile a pod sandbox uses);
//! 2. CNI ADD into it, as container `vm-<uid>` for the VMI's namespace, name
//!    and uid;
//! 3. the NICs realised in it (`stormvm_net::realise(p, Some(netns), …)`): the
//!    bridge binding moves the pod's address off the CNI's interface onto a
//!    bridge with the VM's tap, so the guest holds the pod IP and MAC;
//! 4. the hypervisor spawned **in** the sandbox (the engine joins a machine to
//!    a client's sandbox exactly as it joins a container), so masquerade, when
//!    asked for, NATs out of the pod rather than the node;
//! 5. a DHCP responder (`stormvm_net::serve_dhcp`) answering the guest's MAC
//!    with the pod IP, gateway, the cluster DNS, the ClusterFirst search list
//!    and its hostname, for the machine's life.
//!
//! **The record outlives the kubelet.** The engine keeps the machine (and so
//! the sandbox it occupies) across a kubelet restart, and this process's
//! memory does not. What teardown and a restarted kubelet need — the sandbox
//! handle, the namespace path, the CNI identity, the pod IP and each lease —
//! is written to `<dir>/<uid>.json` before the CNI is asked for anything, so a
//! start interrupted half way is still found and undone. Under `/run`: a
//! sandbox does not outlive a reboot, and neither should its record.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Where the records live.
pub const STATE_DIR: &str = "/run/rustkube-node/vm-network";

/// The sandbox profile a pod gets: its own network namespace, filled by the
/// CNI (`stormpump_runtime`'s `PROFILE_ISOLATED`).
pub const PROFILE_ISOLATED: u8 = 4;

/// What one guest NIC is told by DHCP, kept so a restarted kubelet can answer
/// it again (the guest renews).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// The NIC's index in the VMI's interfaces.
    pub nic: usize,
    pub mac: String,
    pub ip: Ipv4Addr,
    pub prefix: u8,
    pub gateway: Ipv4Addr,
    pub dns: Vec<Ipv4Addr>,
    pub search: Vec<String>,
    pub hostname: Option<String>,
    pub mtu: Option<u32>,
}

impl LeaseRecord {
    pub fn of(nic: usize, mac: &str, l: &stormvm_net::dhcp::Lease) -> LeaseRecord {
        LeaseRecord {
            nic,
            mac: mac.to_string(),
            ip: l.ip,
            prefix: l.prefix,
            gateway: l.gateway,
            dns: l.dns.clone(),
            search: l.search.clone(),
            hostname: l.hostname.clone(),
            mtu: l.mtu,
        }
    }

    pub fn lease(&self) -> Result<stormvm_net::dhcp::Lease, String> {
        Ok(stormvm_net::dhcp::Lease {
            mac: stormvm_net::dhcp::parse_mac(&self.mac)?,
            ip: self.ip,
            prefix: self.prefix,
            gateway: self.gateway,
            dns: self.dns.clone(),
            search: self.search.clone(),
            hostname: self.hostname.clone(),
            mtu: self.mtu,
        })
    }
}

/// One VMI's sandbox on the pod network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodNet {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    /// The engine's sandbox handle (raw).
    pub sandbox: u64,
    /// `/proc/<holder>/ns/net`, what the CNI and stormvm are given.
    pub netns: String,
    /// What the CNI allocated; empty until ADD succeeded.
    pub ip: String,
    pub leases: Vec<LeaseRecord>,
    /// The VMI's launcher Pod (rustkube#203), whose identity the CNI was
    /// given and whose status this kubelet writes. Empty on a node with no
    /// apiserver, where the VMI's own identity is used.
    #[serde(default)]
    pub pod_name: String,
    #[serde(default)]
    pub pod_uid: String,
}

impl PodNet {
    /// The CNI's container id for this VMI: stable, so a DEL after a restart
    /// names what the ADD made.
    pub fn cni_id(&self) -> String {
        format!("vm-{}", self.uid)
    }

    /// The CNI's view: the launcher Pod's name and uid when there is one, so
    /// Cilium labels the endpoint from that Pod (the VMI's labels).
    pub fn cni_pod(&self) -> cni::PodNetwork {
        let (name, uid) = if self.pod_name.is_empty() {
            (self.name.as_str(), self.uid.as_str())
        } else {
            (self.pod_name.as_str(), self.pod_uid.as_str())
        };
        cni::PodNetwork::new(&self.cni_id(), &self.netns, &self.namespace, name, uid)
    }
}

/// The servers and search list a ClusterFirst pod in `namespace` gets
/// (`dns.rs`), as DHCP carries them. Servers that are not IPv4 are left out:
/// the lease is IPv4.
pub fn cluster_first(namespace: &str, servers: &[String], domain: &str) -> (Vec<Ipv4Addr>, Vec<String>) {
    let dns = servers.iter().filter_map(|s| s.parse().ok()).collect();
    let search = if domain.is_empty() {
        Vec::new()
    } else {
        vec![format!("{namespace}.svc.{domain}"), format!("svc.{domain}"), domain.to_string()]
    };
    (dns, search)
}

/// The label every launcher Pod carries (KubeVirt's), and its value.
pub const LAUNCHER_LABEL: &str = "kubevirt.io";
pub const LAUNCHER: &str = "virt-launcher";
/// The label naming the VMI (by uid) a launcher Pod is for.
pub const CREATED_BY: &str = "kubevirt.io/created-by";

/// Is this Pod a VMI's launcher (rustkube#203)? Not the pod manager's to run:
/// the VM manager adopts it.
pub fn is_launcher(pod: &serde_json::Value) -> bool {
    pod["metadata"]["labels"][LAUNCHER_LABEL].as_str() == Some(LAUNCHER)
}

/// The VMI uid a launcher Pod is for.
pub fn launcher_of(pod: &serde_json::Value) -> Option<&str> {
    is_launcher(pod).then(|| pod["metadata"]["labels"][CREATED_BY].as_str()).flatten().filter(|u| !u.is_empty())
}

/// The live launcher Pod for VMI `uid` among `pods`: one created by it and
/// owned by it, not being deleted.
pub fn launcher_for<'a>(pods: &'a [serde_json::Value], uid: &str) -> Option<&'a serde_json::Value> {
    pods.iter().find(|p| {
        launcher_of(p) == Some(uid)
            && p["metadata"]["deletionTimestamp"].is_null()
            && p["metadata"]["ownerReferences"]
                .as_array()
                .is_some_and(|o| o.iter().any(|r| r["uid"].as_str() == Some(uid)))
    })
}

/// The launcher Pod's status for a machine: `Running` with the pod IP and
/// Ready while it runs; `Succeeded`/`Failed` and not Ready once it ended.
/// Merged over what the Pod has, so fields others own are kept.
pub fn launcher_status(existing: &serde_json::Value, ip: &str, phase: &str, now: &str) -> serde_json::Value {
    let running = phase == "Running";
    let ready = if running { "True" } else { "False" };
    let cond = |t: &str, s: &str| {
        let since = existing["conditions"]
            .as_array()
            .and_then(|c| c.iter().find(|c| c["type"] == t && c["status"] == s))
            .and_then(|c| c["lastTransitionTime"].as_str())
            .unwrap_or(now)
            .to_string();
        serde_json::json!({ "type": t, "status": s, "lastTransitionTime": since })
    };
    let mut st = existing.as_object().cloned().unwrap_or_default();
    st.insert("phase".into(), serde_json::json!(phase));
    st.insert(
        "conditions".into(),
        serde_json::json!([
            cond("PodScheduled", "True"),
            cond("Initialized", "True"),
            cond("ContainersReady", ready),
            cond("Ready", ready),
        ]),
    );
    if !ip.is_empty() {
        st.insert("podIP".into(), serde_json::json!(ip));
        st.insert("podIPs".into(), serde_json::json!([{ "ip": ip }]));
    }
    st.entry("startTime").or_insert_with(|| serde_json::json!(now));
    serde_json::Value::Object(st)
}

/// The pod-network NICs of a plan: the ones that need a sandbox.
pub fn wants_sandbox(plans: &[stormvm_net::NicPlan]) -> bool {
    plans.iter().any(|p| matches!(p.attach, stormvm_net::Attach::Pod(_)))
}

/// Is this a binding on the pod network (the machine's address is the pod's)?
pub fn on_pod_network(binding: &str) -> bool {
    matches!(binding, "bridge" | "masquerade" | "passt")
}

/// The records, one file per VMI.
#[derive(Debug, Clone)]
pub struct Store {
    dir: PathBuf,
}

impl Default for Store {
    fn default() -> Self {
        Store { dir: PathBuf::from(STATE_DIR) }
    }
}

impl Store {
    pub fn at(dir: impl Into<PathBuf>) -> Store {
        Store { dir: dir.into() }
    }

    fn path(&self, uid: &str) -> PathBuf {
        self.dir.join(format!("{uid}.json"))
    }

    /// Written whole and renamed into place, so a crash leaves the old record
    /// or the new one, never half of either.
    pub fn save(&self, p: &PodNet) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir).map_err(|e| format!("{}: {e}", self.dir.display()))?;
        let path = self.path(&p.uid);
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_vec(p).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
    }

    pub fn remove(&self, uid: &str) {
        let _ = std::fs::remove_file(self.path(uid));
    }

    /// Every record there is. One that does not parse is skipped with a
    /// warning: it names nothing that can be undone.
    pub fn load_all(&self) -> Vec<PodNet> {
        let Ok(dir) = std::fs::read_dir(&self.dir) else { return Vec::new() };
        dir.flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .filter_map(|e| {
                let text = std::fs::read(e.path()).ok()?;
                match serde_json::from_slice(&text) {
                    Ok(p) => Some(p),
                    Err(err) => {
                        tracing::warn!(path = %e.path().display(), "unreadable VM network record: {err}");
                        None
                    }
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> PodNet {
        PodNet {
            uid: "u-1".into(),
            namespace: "default".into(),
            name: "web-1".into(),
            sandbox: 42,
            netns: "/proc/77/ns/net".into(),
            ip: "10.0.1.5".into(),
            pod_name: String::new(),
            pod_uid: String::new(),
            leases: vec![LeaseRecord {
                nic: 0,
                mac: "0a:58:0a:00:01:05".into(),
                ip: "10.0.1.5".parse().unwrap(),
                prefix: 32,
                gateway: "10.0.1.1".parse().unwrap(),
                dns: vec!["10.96.0.10".parse().unwrap()],
                search: vec!["default.svc.cluster.local".into()],
                hostname: Some("web-1".into()),
                mtu: Some(1450),
            }],
        }
    }

    #[test]
    fn a_record_survives_the_disk_and_names_its_cni_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let r = record();
        store.save(&r).unwrap();
        assert_eq!(store.load_all(), vec![r.clone()]);
        let pod = r.cni_pod();
        assert_eq!(pod.container_id, "vm-u-1");
        assert_eq!(pod.netns_path, "/proc/77/ns/net");
        assert_eq!((pod.pod_namespace.as_str(), pod.pod_name.as_str(), pod.pod_uid.as_str()), ("default", "web-1", "u-1"));
        store.remove("u-1");
        assert!(store.load_all().is_empty());
        // A missing directory is no records, not an error.
        assert!(Store::at(dir.path().join("none")).load_all().is_empty());
    }

    fn launcher(uid: &str, owner: &str) -> serde_json::Value {
        serde_json::json!({ "metadata": {
            "name": "virt-launcher-web-1-abcde", "namespace": "default", "uid": "p-1",
            "labels": { "kubevirt.io": "virt-launcher", "kubevirt.io/created-by": uid, "app": "web" },
            "ownerReferences": [{ "kind": "VirtualMachineInstance", "uid": owner, "controller": true }] } })
    }

    #[test]
    fn the_launcher_pod_is_found_by_its_vmi_and_owner() {
        let pods = vec![launcher("u-2", "u-2"), launcher("u-1", "u-1")];
        assert_eq!(launcher_for(&pods, "u-1").unwrap()["metadata"]["uid"], "p-1");
        assert!(launcher_for(&[launcher("u-1", "other")], "u-1").is_none(), "owned by another");
        let mut going = launcher("u-1", "u-1");
        going["metadata"]["deletionTimestamp"] = serde_json::json!("2026-10-05T00:00:00Z");
        assert!(launcher_for(&[going.clone()], "u-1").is_none());
        assert_eq!(launcher_of(&going), Some("u-1"));
        assert!(!is_launcher(&serde_json::json!({ "metadata": { "labels": { "app": "x" } } })));
    }

    #[test]
    fn the_cni_names_the_launcher_pod_when_there_is_one() {
        let mut r = record();
        r.pod_name = "virt-launcher-web-1-abcde".into();
        r.pod_uid = "p-1".into();
        let pod = r.cni_pod();
        assert_eq!((pod.pod_name.as_str(), pod.pod_uid.as_str()), ("virt-launcher-web-1-abcde", "p-1"));
        assert_eq!(pod.container_id, "vm-u-1", "the sandbox is still the VMI's");
        // An old record (no launcher fields) still parses.
        let old: PodNet = serde_json::from_str(r#"{"uid":"u","namespace":"n","name":"v","sandbox":1,"netns":"/x","ip":"","leases":[]}"#).unwrap();
        assert_eq!(old.cni_pod().pod_name, "v");
    }

    #[test]
    fn the_launcher_status_says_running_with_the_pod_ip_then_ended() {
        let now = "2026-10-05T20:00:00Z";
        let st = launcher_status(&serde_json::json!({ "qosClass": "Burstable" }), "10.0.1.5", "Running", now);
        assert_eq!(st["phase"], "Running");
        assert_eq!(st["podIP"], "10.0.1.5");
        assert_eq!(st["podIPs"][0]["ip"], "10.0.1.5");
        assert_eq!(st["qosClass"], "Burstable", "others' fields kept");
        let ready = st["conditions"].as_array().unwrap().iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], "True");
        let later = launcher_status(&st, "10.0.1.5", "Running", "2026-10-05T21:00:00Z");
        assert_eq!(later, st, "an unchanged status is the same object (no write)");
        let ended = launcher_status(&st, "", "Failed", "2026-10-05T21:00:00Z");
        assert_eq!(ended["phase"], "Failed");
        let ready = ended["conditions"].as_array().unwrap().iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!((ready["status"].as_str(), ready["lastTransitionTime"].as_str()), (Some("False"), Some("2026-10-05T21:00:00Z")));
    }

    #[test]
    fn a_lease_round_trips_through_its_record() {
        let l = record().leases[0].lease().unwrap();
        assert_eq!(l.mac, [0x0a, 0x58, 0x0a, 0, 1, 5]);
        assert_eq!(LeaseRecord::of(0, "0a:58:0a:00:01:05", &l), record().leases[0]);
    }

    #[test]
    fn the_guest_is_told_what_a_cluster_first_pod_is_told() {
        let (dns, search) = cluster_first("shop", &["10.96.0.10".into(), "fd00::a".into()], "cluster.local");
        assert_eq!(dns, vec!["10.96.0.10".parse::<Ipv4Addr>().unwrap()]);
        assert_eq!(search, vec!["shop.svc.cluster.local", "svc.cluster.local", "cluster.local"]);
        assert!(cluster_first("shop", &[], "").1.is_empty());
    }

    #[test]
    fn only_pod_bindings_want_a_sandbox() {
        let nic = |attach| stormvm_net::NicPlan {
            nic: "net0".into(),
            tap: "t0".into(),
            mac: "02:00:00:00:00:02".into(),
            attach,
        };
        let host = nic(stormvm_net::Attach::Bridge("stormbr0".into()));
        let pod = nic(stormvm_net::Attach::Pod(stormvm_spec::PodBinding::Bridge));
        assert!(!wants_sandbox(&[host.clone()]));
        assert!(wants_sandbox(&[host, pod]));
        assert!(on_pod_network("bridge") && on_pod_network("masquerade") && on_pod_network("passt"));
        assert!(!on_pod_network("host-bridge"));
    }
}
