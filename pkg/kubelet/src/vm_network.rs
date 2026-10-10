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
//! A VMI's `networks: [{multus: {networkName}}]` (stormvm#85; the Multus
//! standard, owner on stormcos#249) are more NICs in the same sandbox, the
//! way virt-launcher wires them: each NetworkAttachmentDefinition's config is
//! ADDed into the sandbox on its own interface (`net1`, `net2`, … by order),
//! after the default network, and stormvm bridges that interface to the VM's
//! tap on `vmnetN`. `multus: {default: true}` replaces the cluster's network:
//! its NAD is ADDed on `eth0` instead. A secondary NIC whose NAD's IPAM gave
//! an address is told it by DHCP on its own bridge, with no router (the
//! default route stays the pod network's); one with no IPAM has its guest's
//! address watched on the tap.
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
    /// The sandbox bridge it is answered on: `vmbr0` (none recorded, as
    /// before stormvm#85) or a multus NIC's `vmnetN`.
    #[serde(default)]
    pub bridge: Option<String>,
}

impl LeaseRecord {
    pub fn of(nic: usize, mac: &str, l: &stormvm_net::dhcp::Lease, bridge: Option<&str>) -> LeaseRecord {
        LeaseRecord {
            bridge: bridge.filter(|b| *b != stormvm_net::SANDBOX_BRIDGE).map(str::to_string),
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

/// A NetworkAttachmentDefinition ADDed into a VMI's sandbox (stormvm#85):
/// what its DEL needs after a restart, when the NAD itself may be gone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// `namespace/name` of the NAD: the network-status entry's name.
    pub name: String,
    /// The interface in the sandbox.
    pub ifname: String,
    /// The NAD's `spec.config`, as it was ADDed.
    pub config: String,
}

impl Attachment {
    pub fn config(&self) -> Result<cni::NetworkConfigList, String> {
        cni::NetworkConfigList::from_json(&self.config, &self.name).map_err(|e| format!("network {} ({}): its config: {e}", self.name, self.ifname))
    }
}

/// One VMI's sandbox on the pod network.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The NAD that replaces the cluster's network on `eth0`
    /// (`multus: {default: true}`), when the VMI names one.
    #[serde(default)]
    pub default_network: Option<Attachment>,
    /// The secondary NADs, in the order they were ADDed (recorded before
    /// each ADD, so an interrupted start's DEL covers it).
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// `k8s.v1.cni.cncf.io/network-status` for the launcher Pod: one entry
    /// per network, the default first.
    #[serde(default)]
    pub network_status: Option<serde_json::Value>,
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

/// The live launcher Pod for VMI `uid` on `node` among `pods`: one created
/// by it and owned by it, placed on this node, not being deleted. During a
/// live migration the target node has a launcher of its own (rustkube#203,
/// `kubevirt.io/migrationJobUID`), so a VMI can have two; each node takes
/// its own (#152).
pub fn launcher_for<'a>(pods: &'a [serde_json::Value], uid: &str, node: &str) -> Option<&'a serde_json::Value> {
    pods.iter().find(|p| {
        launcher_of(p) == Some(uid)
            && p["spec"]["nodeName"].as_str() == Some(node)
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

/// The pod a reviewed ServiceAccount token is bound to (#122).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPod {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// The node the apiserver says the pod is on, when it says.
    pub node: Option<String>,
}

/// From a TokenReview answer: authenticated, a ServiceAccount
/// (`system:serviceaccount:<ns>:<sa>`), and bound to a pod
/// (`authentication.kubernetes.io/pod-name`/`pod-uid` in `status.user.extra`).
pub fn bound_pod(review: &serde_json::Value) -> Option<BoundPod> {
    let st = &review["status"];
    if st["authenticated"].as_bool() != Some(true) {
        return None;
    }
    let namespace = st["user"]["username"].as_str()?.strip_prefix("system:serviceaccount:")?.split(':').next()?.to_string();
    let extra = |k: &str| -> Option<String> {
        let v = &st["user"]["extra"][format!("authentication.kubernetes.io/{k}")];
        v.as_array().and_then(|a| a.first()).or(Some(v)).and_then(|x| x.as_str()).filter(|x| !x.is_empty()).map(String::from)
    };
    Some(BoundPod { namespace, name: extra("pod-name")?, uid: extra("pod-uid")?, node: extra("node-name") })
}

/// Does this pod object answer for a token bound to `uid` on `node`: the same
/// pod (a recreated one of the name is not), placed here, not ending?
pub fn pod_answers(pod: &serde_json::Value, uid: &str, node: &str) -> bool {
    pod["metadata"]["uid"].as_str() == Some(uid)
        && pod["spec"]["nodeName"].as_str() == Some(node)
        && pod["metadata"]["deletionTimestamp"].is_null()
        && !matches!(pod["status"]["phase"].as_str(), Some("Succeeded") | Some("Failed"))
}

/// A pod's instance metadata (#122), shaped as a machine's: the pod's own
/// identity, from its object.
pub fn pod_metadata(pod: &serde_json::Value, ip: &str, node: &str) -> serde_json::Value {
    let m = &pod["metadata"];
    let name = m["name"].as_str().unwrap_or("");
    serde_json::json!({
        "instance_id": m["uid"],
        "storm.io/kind": "Pod",
        "hostname": pod["spec"]["hostname"].as_str().filter(|h| !h.is_empty()).unwrap_or(name),
        "local_ipv4": ip,
        "region": "storm",
        "zone": node,
        "tags": { "namespace": m["namespace"], "name": name },
        "labels": m["labels"].as_object().map(|l| serde_json::Value::Object(l.clone())).unwrap_or_else(|| serde_json::json!({})),
        "service_account": pod["spec"]["serviceAccountName"].as_str().unwrap_or("default"),
        "launched_at": pod["status"]["startTime"],
    })
}

/// The NICs of a plan that need a sandbox: the pod network's and every
/// multus one (stormvm#85).
pub fn wants_sandbox(plans: &[stormvm_net::NicPlan]) -> bool {
    plans.iter().any(|p| matches!(p.attach, stormvm_net::Attach::Pod(_) | stormvm_net::Attach::Multus { .. }))
}

/// A plan's multus networks: the one replacing the pod network (if any),
/// and the secondary ones in order, each as `(network, ifname)`.
pub fn multus_networks(plans: &[stormvm_net::NicPlan]) -> (Option<(String, String)>, Vec<(String, String)>) {
    let mut default = None;
    let mut secondary = Vec::new();
    for p in plans {
        if let stormvm_net::Attach::Multus { network, ifname, default: d } = &p.attach {
            if *d {
                default = Some((network.clone(), ifname.clone()));
            } else {
                secondary.push((network.clone(), ifname.clone()));
            }
        }
    }
    (default, secondary)
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
                bridge: None,
            }],
            ..Default::default()
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
            "ownerReferences": [{ "kind": "VirtualMachineInstance", "uid": owner, "controller": true }] },
            "spec": { "nodeName": "n1" } })
    }

    #[test]
    fn the_launcher_pod_is_found_by_its_vmi_and_owner() {
        let pods = vec![launcher("u-2", "u-2"), launcher("u-1", "u-1")];
        assert_eq!(launcher_for(&pods, "u-1", "n1").unwrap()["metadata"]["uid"], "p-1");
        assert!(launcher_for(&[launcher("u-1", "other")], "u-1", "n1").is_none(), "owned by another");
        // A migration's target launcher (rustkube#203) is the target node's;
        // each node takes its own (#152).
        let mut target = launcher("u-1", "u-1");
        target["metadata"]["uid"] = serde_json::json!("p-target");
        target["metadata"]["labels"]["kubevirt.io/migrationJobUID"] = serde_json::json!("m-1");
        target["spec"]["nodeName"] = serde_json::json!("n2");
        let both = vec![target, launcher("u-1", "u-1")];
        assert_eq!(launcher_for(&both, "u-1", "n1").unwrap()["metadata"]["uid"], "p-1");
        assert_eq!(launcher_for(&both, "u-1", "n2").unwrap()["metadata"]["uid"], "p-target");
        assert!(launcher_for(&both, "u-1", "n3").is_none());
        let mut going = launcher("u-1", "u-1");
        going["metadata"]["deletionTimestamp"] = serde_json::json!("2026-10-05T00:00:00Z");
        assert!(launcher_for(&[going.clone()], "u-1", "n1").is_none());
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

    fn review(user: &str, name: &str, uid: &str, node: Option<&str>) -> serde_json::Value {
        let mut extra = serde_json::json!({
            "authentication.kubernetes.io/pod-name": [name],
            "authentication.kubernetes.io/pod-uid": [uid],
        });
        if let Some(n) = node {
            extra["authentication.kubernetes.io/node-name"] = serde_json::json!([n]);
        }
        serde_json::json!({ "status": { "authenticated": true, "user": { "username": user, "extra": extra } } })
    }

    #[test]
    fn a_reviewed_token_names_its_pod() {
        let b = bound_pod(&review("system:serviceaccount:ops:agent", "agent-x", "p-1", Some("n1"))).unwrap();
        assert_eq!(b, BoundPod { namespace: "ops".into(), name: "agent-x".into(), uid: "p-1".into(), node: Some("n1".into()) });
        assert!(bound_pod(&review("alice", "agent-x", "p-1", None)).is_none(), "not a ServiceAccount");
        let mut unbound = review("system:serviceaccount:ops:agent", "", "", None);
        unbound["status"]["user"]["extra"] = serde_json::json!({});
        assert!(bound_pod(&unbound).is_none(), "a token bound to no pod");
        let mut no = review("system:serviceaccount:ops:agent", "a", "p", None);
        no["status"]["authenticated"] = serde_json::json!(false);
        assert!(bound_pod(&no).is_none());
    }

    #[test]
    fn only_the_same_pod_placed_here_and_running_answers() {
        let pod = serde_json::json!({
            "metadata": { "name": "agent-x", "namespace": "ops", "uid": "p-1", "labels": { "app": "agent" } },
            "spec": { "nodeName": "n1", "hostNetwork": true, "serviceAccountName": "agent" },
            "status": { "phase": "Running", "startTime": "2026-10-05T00:00:00Z" },
        });
        assert!(pod_answers(&pod, "p-1", "n1"));
        assert!(!pod_answers(&pod, "p-2", "n1"), "a recreated pod of the name");
        assert!(!pod_answers(&pod, "p-1", "n2"), "placed elsewhere");
        let mut ending = pod.clone();
        ending["metadata"]["deletionTimestamp"] = serde_json::json!("2026-10-05T01:00:00Z");
        assert!(!pod_answers(&ending, "p-1", "n1"));
        let md = pod_metadata(&pod, "192.168.30.2", "n1");
        assert_eq!(md["instance_id"], "p-1");
        assert_eq!(md["storm.io/kind"], "Pod");
        assert_eq!(md["hostname"], "agent-x");
        assert_eq!(md["labels"]["app"], "agent");
        assert_eq!(md["tags"]["namespace"], "ops");
        assert_eq!(md["service_account"], "agent");
    }

    #[test]
    fn a_lease_round_trips_through_its_record() {
        let l = record().leases[0].lease().unwrap();
        assert_eq!(l.mac, [0x0a, 0x58, 0x0a, 0, 1, 5]);
        assert_eq!(LeaseRecord::of(0, "0a:58:0a:00:01:05", &l, Some("vmbr0")), record().leases[0], "vmbr0 is the default, not recorded");
        assert_eq!(LeaseRecord::of(1, "02:00:00:00:00:07", &l, Some("vmnet1")).bridge.as_deref(), Some("vmnet1"));
    }

    /// stormvm#85: the multus networks and what DEL needs survive the disk; a
    /// record written before them still reads.
    #[test]
    fn multus_networks_are_recorded_and_an_older_record_still_reads() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::at(dir.path());
        let mut r = record();
        r.default_network = Some(Attachment { name: "default/flat".into(), ifname: "eth0".into(), config: "{}".into() });
        r.attachments = vec![Attachment { name: "vlans/v30".into(), ifname: "net1".into(), config: r#"{"type":"bridge"}"#.into() }];
        r.network_status = Some(serde_json::json!([{ "name": "default/flat", "default": true }]));
        store.save(&r).unwrap();
        assert_eq!(store.load_all(), vec![r]);
        let old = r#"{"uid":"u-2","namespace":"default","name":"old","sandbox":1,"netns":"/proc/9/ns/net","ip":"10.0.1.9",
            "leases":[{"nic":0,"mac":"0a:58:0a:00:01:09","ip":"10.0.1.9","prefix":32,"gateway":"10.0.1.1","dns":[],"search":[],
            "hostname":null,"mtu":null}]}"#;
        std::fs::write(dir.path().join("u-2.json"), old).unwrap();
        let read = store.load_all().into_iter().find(|p| p.uid == "u-2").unwrap();
        assert!(read.attachments.is_empty() && read.default_network.is_none() && read.leases[0].bridge.is_none());
    }

    #[test]
    fn a_vmis_multus_networks_want_a_sandbox_in_order() {
        let obj = serde_json::json!({
            "kind": "VirtualMachineInstance",
            "metadata": { "name": "web-1", "namespace": "default", "uid": "u-1" },
            "spec": {
                "domain": { "memory": { "guest": "1Gi" }, "devices": { "interfaces": [
                    { "name": "lan", "bridge": {} }, { "name": "v30", "bridge": {} }, { "name": "v40", "bridge": {} }
                ] } },
                "networks": [
                    { "name": "lan", "multus": { "networkName": "flat", "default": true } },
                    { "name": "v30", "multus": { "networkName": "vlans/v30" } },
                    { "name": "v40", "multus": { "networkName": "v40" } }
                ]
            }
        });
        let vm = stormvm_spec::kube::from_kube(&obj).unwrap();
        let plans = stormvm_net::plan("default", &vm.name, &vm.interfaces, &Default::default()).unwrap();
        assert!(wants_sandbox(&plans));
        let (default, secondary) = multus_networks(&plans);
        assert_eq!(default, Some(("flat".to_string(), "eth0".to_string())));
        assert_eq!(secondary, vec![("vlans/v30".to_string(), "net1".to_string()), ("v40".to_string(), "net2".to_string())]);
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
