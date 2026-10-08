//! The node's own volumes, as complete, current PV + PVC sets (#49, #59).
//!
//! Every service on a stormcos node keeps its state in stormblock volumes:
//! `fastetcd-data`, `stormcert-data`, `stormcos-state`, and a `<component>-logs`
//! volume per component. Each is a copy-on-write clone of a golden, mounted at
//! boot by the initramfs. That *is* this platform's storage class at work: a
//! claim is a clone of a data volume. So each one is represented the way any
//! Kubernetes claim is: **a PV and its bound PVC, always together**.
//!
//! - namespace `kube-system`, beside the service's own mirrored pod
//!   (`mirror.rs`): `kube-system/fastetcd-data-<node>` next to
//!   `kube-system/fastetcd-<node>`
//! - a `PersistentVolume` named `storm-<volume>-<node>`, class `stormblock`, the
//!   stormblock volume as its CSI handle with its filesystem and golden, pinned
//!   to this node, reclaim **Retain**: deleting the object must never delete a
//!   service's data
//! - a `PersistentVolumeClaim` named `<volume>-<node>`, bound to it, with the
//!   golden it was cloned from as its `dataSourceRef`
//!
//! **Node-qualified names** (owner, #107): every node has a `fastetcd-data`, and
//! a claim or PV name exists once per cluster, so unqualified names represented
//! only the first node to write them. A service volume is one node's (no
//! cross-node RAID for system services), so the node in the name is the whole
//! identity. No migration: objects written under the old unqualified names are
//! left as they are; the mirror lets go of a claim only when the volume its
//! `storm.io/volume` names is gone from the node.
//! - on both, `storm.io/volume-kind` (`data`, `state` or `logs`) and
//!   `storm.io/component` (`fastetcd`), so `kubectl get pvc -l
//!   storm.io/volume-kind=logs` lists the log volumes
//!
//! Written as a dynamically provisioned pair of any driver reads: the PVC
//! carries `pv.kubernetes.io/bind-completed`, `bound-by-controller`,
//! `volume.kubernetes.io/storage-provisioner` and `selected-node`, and the
//! PV's `claimRef` carries the claim's uid.
//!
//! **A reconciler, not a one-shot.** stormblock is the source of truth, and
//! every pass (30 s) brings the API up to it: a missing object is created again
//! (etcd wiped, namespace deleted, a claim deleted by hand), a volume that grew
//! grows its objects, health and access are kept current, and a volume created
//! after boot gets the same set. Phases and protection finalizers are the
//! binder's (rustkube's `persistentvolume.rs`), so this writes objects and
//! bindings, never phases, except the claim's first status, which saves it
//! reading as Unknown until the binder's next pass.
//!
//! **Only this node's objects.** An object is this node's when it carries the
//! mirror's label and `storm.io/node` names this node. Anything else under the
//! same name is left alone, never overwritten.

use retry::RetryExt;
use std::collections::HashMap;

use serde_json::{json, Map, Value};
use tracing::{debug, info, warn};

/// Where the node's own claims live: with the node's services.
pub const NAMESPACE: &str = "kube-system";

/// Marks the objects this mirror owns — and marks a volume a node service has
/// mounted, which a pod may clone but must not mount.
pub const LABEL: &str = "storm.io/system-volume";
/// What the volume holds: `data`, `state` or `logs`.
pub const KIND_LABEL: &str = "storm.io/volume-kind";
/// The component it belongs to: `fastetcd` for `fastetcd-logs`.
pub const COMPONENT_LABEL: &str = "storm.io/component";

/// The CSI driver name the class's PVs carry, and the provisioner the claims name.
pub const DRIVER: &str = "stormblock.storm.io";

/// What a node volume holds and whose it is, from its name, or `None` when it
/// is not one of the node's service volumes.
///
/// Writable, and named `<component>-data`, `-state` or `-logs`. Goldens are
/// sealed and are what these are cloned *from*; `pvc-*` are claims already
/// (the built-in driver's, `bind_claim`); `standby-*` are pre-minted clones
/// that are nobody's until a claim takes them. The engine marks nothing as
/// logs, and `role` is only the slab half (`system` or `data`), so the name is
/// what says.
pub fn kind_of(v: &Value) -> Option<(&'static str, String)> {
    let name = v["name"].as_str()?;
    if v["sealed"].as_bool().unwrap_or(false)
        || name.ends_with(".golden")
        || name.starts_with("pvc-")
        || name.starts_with("standby-")
    {
        return None;
    }
    for (suffix, kind) in [("-data", "data"), ("-state", "state"), ("-logs", "logs")] {
        if let Some(component) = name.strip_suffix(suffix).filter(|c| !c.is_empty()) {
            return Some((kind, component.to_string()));
        }
    }
    None
}

/// Kept for callers and tests that ask the old question.
pub fn is_data_container(v: &Value) -> bool {
    matches!(kind_of(v), Some(("data" | "state", _)))
}

/// Bytes as a Kubernetes quantity: `1Gi` rather than `1073741824`.
pub fn quantity(bytes: u64) -> String {
    const UNITS: [(&str, u64); 5] =
        [("Pi", 1 << 50), ("Ti", 1 << 40), ("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)];
    for (u, n) in UNITS {
        if bytes >= n && bytes % n == 0 {
            return format!("{}{u}", bytes / n);
        }
    }
    bytes.to_string()
}

/// The PV name for a node volume on `node`: `storm-<volume>-<node>` (#107).
pub fn pv_name(volume: &str, node: &str) -> String {
    format!("storm-{volume}-{node}")
}

/// The claim name for a node volume on `node`: `<volume>-<node>` (#107), in
/// [`NAMESPACE`].
pub fn claim_name(volume: &str, node: &str) -> String {
    format!("{volume}-{node}")
}

/// What the engine says about one volume, as the objects need it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VolumeFacts {
    pub name: String,
    pub bytes: u64,
    /// `fs.kind`: `ext4`, `xfs`, …
    pub fs_type: Option<String>,
    pub fs_uuid: Option<String>,
    /// The golden it was cloned from, without `.golden`.
    pub golden: Option<String>,
    pub health: Option<String>,
    pub access: Option<String>,
    /// The slab half: `system` or `data`.
    pub role: Option<String>,
    /// A raw block volume (`volumeMode: Block`, #67): what its claim asked
    /// for, since the engine's record of a plain volume and of an unformatted
    /// one look alike. The node's own volumes are filesystems.
    pub block: bool,
}

impl VolumeFacts {
    /// From one entry of the engine's volume list. `names` maps every volume's
    /// id to its name, for the parent.
    pub fn of(v: &Value, names: &HashMap<String, String>) -> VolumeFacts {
        let s = |k: &str| v[k].as_str().filter(|x| !x.is_empty()).map(String::from);
        VolumeFacts {
            name: s("name").unwrap_or_default(),
            bytes: v["virtual_size_bytes"].as_u64().unwrap_or(0),
            fs_type: v["fs"]["kind"].as_str().filter(|x| !x.is_empty()).map(String::from),
            fs_uuid: s("fs_uuid"),
            golden: v["parent"]
                .as_str()
                .and_then(|p| names.get(p))
                .map(|g| g.strip_suffix(".golden").unwrap_or(g).to_string()),
            health: s("health"),
            access: s("access"),
            role: s("role"),
            block: false,
        }
    }
}

/// The CSI source of a stormblock PV: the handle, and what is known about the
/// filesystem and where it came from.
pub fn csi_source(f: &VolumeFacts) -> Value {
    let mut csi = json!({ "driver": DRIVER, "volumeHandle": f.name });
    if let Some(t) = &f.fs_type {
        csi["fsType"] = json!(t);
    }
    let mut attrs = Map::new();
    if let Some(g) = &f.golden {
        attrs.insert("storm.io/golden".into(), json!(g));
    }
    if let Some(u) = &f.fs_uuid {
        attrs.insert("storm.io/fs-uuid".into(), json!(u));
    }
    if !attrs.is_empty() {
        csi["volumeAttributes"] = Value::Object(attrs);
    }
    csi
}

/// The annotations a PV carries about its volume's current state.
pub fn pv_state_annotations(f: &VolumeFacts, node: &str) -> Map<String, Value> {
    let mut a = Map::new();
    a.insert("storm.io/node".into(), json!(node));
    a.insert("storm.io/volume".into(), json!(f.name));
    a.insert("pv.kubernetes.io/provisioned-by".into(), json!(DRIVER));
    for (k, v) in [("storm.io/health", &f.health), ("storm.io/access", &f.access), ("storm.io/role", &f.role)]
    {
        if let Some(v) = v {
            a.insert(k.into(), json!(v));
        }
    }
    a
}

/// The annotations a bound, provisioned claim carries, as upstream's binder
/// and scheduler leave them.
pub fn pvc_bound_annotations(node: &str, volume: &str) -> Map<String, Value> {
    let mut a = Map::new();
    a.insert("pv.kubernetes.io/bind-completed".into(), json!("yes"));
    a.insert("pv.kubernetes.io/bound-by-controller".into(), json!("yes"));
    a.insert("volume.kubernetes.io/storage-provisioner".into(), json!(DRIVER));
    a.insert("volume.beta.kubernetes.io/storage-provisioner".into(), json!(DRIVER));
    a.insert("volume.kubernetes.io/selected-node".into(), json!(node));
    a.insert("storm.io/node".into(), json!(node));
    a.insert("storm.io/volume".into(), json!(volume));
    a
}

/// The PV's `nodeAffinity`: a stormblock clone lives on the node that made it.
pub fn node_affinity(node: &str) -> Value {
    json!({ "required": { "nodeSelectorTerms": [{
        "matchExpressions": [{
            "key": "kubernetes.io/hostname",
            "operator": "In",
            "values": [node],
        }],
    }]}})
}

fn labels(kind: &str, component: &str) -> Value {
    json!({ LABEL: "true", KIND_LABEL: kind, COMPONENT_LABEL: component })
}

/// A stormblock PV for volume `f` on `node`, naming `claim_ref` as its claim.
///
/// The one builder for every stormblock PV this kubelet writes: the node's own
/// volumes (Retain) and the built-in driver's claims (`bind_claim`, Delete).
/// Labels are the caller's.
pub fn stormblock_pv(f: &VolumeFacts, pv_name: &str, node: &str, claim_ref: Value, reclaim: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": pv_name,
            "annotations": pv_state_annotations(f, node),
        },
        "spec": {
            "capacity": { "storage": quantity(f.bytes) },
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": reclaim,
            "storageClassName": crate::storage::STORAGE_CLASS,
            "volumeMode": if f.block { "Block" } else { "Filesystem" },
            "claimRef": claim_ref,
            "csi": csi_source(f),
            "nodeAffinity": node_affinity(node),
        },
    })
}

/// Point `pvc` at `pv_name` and give it the annotations a bound, provisioned
/// claim carries. Everything else on it is kept.
pub fn bind_pvc(pvc: &mut Value, pv_name: &str, node: &str, volume: &str) {
    pvc["spec"]["volumeName"] = json!(pv_name);
    if pvc["spec"]["storageClassName"].as_str().is_none() {
        pvc["spec"]["storageClassName"] = json!(crate::storage::STORAGE_CLASS);
    }
    merge_map(pvc, "annotations", &Value::Object(pvc_bound_annotations(node, volume)));
}

/// The PV and PVC for one node volume, as they should be. The PV's `claimRef`
/// has no uid until the claim exists ([`reconcile_pv`] adds it).
pub fn objects(f: &VolumeFacts, kind: &str, component: &str, node: &str) -> (Value, Value) {
    let labels = labels(kind, component);
    let claim_ref = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": NAMESPACE,
        "name": claim_name(&f.name, node),
    });
    let mut pv = stormblock_pv(f, &pv_name(&f.name, node), node, claim_ref, "Retain");
    pv["metadata"]["labels"] = labels.clone();
    let mut pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": claim_name(&f.name, node),
            "namespace": NAMESPACE,
            "labels": labels,
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": crate::storage::STORAGE_CLASS,
            "volumeMode": "Filesystem",
            "resources": { "requests": { "storage": quantity(f.bytes) } },
        },
    });
    if let Some(g) = &f.golden {
        pvc["spec"]["dataSourceRef"] = json!({ "apiGroup": "storm.io", "kind": "Golden", "name": g });
    }
    bind_pvc(&mut pvc, &pv_name(&f.name, node), node, &f.name);
    (pv, pvc)
}

/// Is this object the mirror's, for this node? Only those are ever written.
pub fn is_ours(obj: &Value, node: &str) -> bool {
    obj["metadata"]["labels"][LABEL].as_str() == Some("true")
        && obj["metadata"]["annotations"]["storm.io/node"].as_str() == Some(node)
}

/// Merge `want`'s entries into `obj[section]`, keeping everything else there.
fn merge_map(obj: &mut Value, section: &str, want: &Value) {
    let Some(want) = want.as_object() else { return };
    if !obj["metadata"][section].is_object() {
        obj["metadata"][section] = json!({});
    }
    for (k, v) in want {
        obj["metadata"][section][k] = v.clone();
    }
}

/// An existing claim brought up to date, or `None` when it already is.
///
/// Labels and annotations are merged, not replaced: another tool's are kept.
/// The request only grows, as a claim's may: a volume that was expanded is a
/// claim that asked for more, and one that reads smaller than its request is
/// left alone rather than shrunk.
pub fn reconcile_pvc(existing: &Value, want: &Value) -> Option<Value> {
    let mut obj = existing.clone();
    merge_map(&mut obj, "labels", &want["metadata"]["labels"]);
    merge_map(&mut obj, "annotations", &want["metadata"]["annotations"]);
    let want_req = &want["spec"]["resources"]["requests"]["storage"];
    let have = obj["spec"]["resources"]["requests"]["storage"]
        .as_str()
        .and_then(crate::storage::parse_quantity)
        .unwrap_or(0);
    let wanted = want_req.as_str().and_then(crate::storage::parse_quantity).unwrap_or(0);
    if wanted > have {
        obj["spec"]["resources"]["requests"]["storage"] = want_req.clone();
    }
    (obj != *existing).then_some(obj)
}

/// An existing volume brought up to date, or `None` when it already is.
///
/// Capacity and the CSI source follow the engine; the `claimRef` follows the
/// claim's uid, so a claim that was deleted and made again is the one the
/// volume names.
pub fn reconcile_pv(existing: &Value, want: &Value, claim: Option<&Value>) -> Option<Value> {
    let mut obj = existing.clone();
    merge_map(&mut obj, "labels", &want["metadata"]["labels"]);
    merge_map(&mut obj, "annotations", &want["metadata"]["annotations"]);
    obj["spec"]["capacity"] = want["spec"]["capacity"].clone();
    obj["spec"]["csi"] = want["spec"]["csi"].clone();
    // Only a different claim moves the reference. Its resourceVersion changes
    // every time the binder touches it, and following that would rewrite the
    // volume on every pass; upstream records it once, at binding.
    if let Some(c) = claim {
        if obj["spec"]["claimRef"]["uid"] != c["metadata"]["uid"] {
            obj["spec"]["claimRef"] = claim_ref(c);
        }
    }
    (obj != *existing).then_some(obj)
}

/// A `claimRef` naming this claim exactly: namespace, name, uid, resourceVersion.
pub fn claim_ref(pvc: &Value) -> Value {
    let m = &pvc["metadata"];
    let mut r = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": m["namespace"],
        "name": m["name"],
    });
    if let Some(uid) = m["uid"].as_str() {
        r["uid"] = json!(uid);
    }
    if let Some(rv) = m["resourceVersion"].as_str() {
        r["resourceVersion"] = json!(rv);
    }
    r
}

/// Objects of a list, by name.
fn by_name(list: &Value) -> HashMap<String, Value> {
    list["items"]
        .as_array()
        .map(|a| a.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter_map(|o| Some((o["metadata"]["name"].as_str()?.to_string(), o.clone())))
        .collect()
}

async fn get_json(client: &reqwest::Client, url: &str) -> Option<Value> {
    match client.get(url).send_retrying(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => r.json().await.ok(),
        _ => None,
    }
}

/// POST, and the object as the apiserver stored it.
async fn create(client: &reqwest::Client, url: &str, obj: &Value) -> Option<Value> {
    match client.post(url).json(obj).send_repeatable(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => r.json().await.ok(),
        Ok(r) => {
            debug!("create {url}: {}", r.status());
            None
        }
        Err(e) => {
            debug!("create {url}: {e}");
            None
        }
    }
}

async fn replace(client: &reqwest::Client, url: &str, obj: &Value) -> Option<Value> {
    match client.put(url).json(obj).send_retrying(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => r.json().await.ok(),
        Ok(r) => {
            debug!("update {url}: {}", r.status());
            None
        }
        Err(e) => {
            debug!("update {url}: {e}");
            None
        }
    }
}

/// One pass: every node volume has its complete, current PV and bound PVC.
///
/// `client` is the apiserver's, `engine` the node's stormblock (with its own
/// token, #66). Nothing is written unless it differs from what is there.
pub async fn mirror(
    client: &reqwest::Client,
    api_url: &str,
    engine: &crate::engine::EngineClient,
    node: &str,
) {
    if api_url.is_empty() {
        return;
    }
    // Failures are reported to the reactor, which retries the pass (#101):
    // nothing else would bring it back.
    let vols: Value = match engine.get(&format!("{}/api/v1/volumes", engine.url())).await {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => v,
            Err(_) => return apimachinery::reactor::failed(),
        },
        _ => return apimachinery::reactor::failed(),
    };
    let items = vols["items"].as_array().cloned().unwrap_or_default();
    let names: HashMap<String, String> = items
        .iter()
        .filter_map(|v| Some((v["id"].as_str()?.to_string(), v["name"].as_str()?.to_string())))
        .collect();
    let mirrored: Vec<(VolumeFacts, &'static str, String)> = items
        .iter()
        .filter_map(|v| {
            let (kind, component) = kind_of(v)?;
            Some((VolumeFacts::of(v, &names), kind, component))
        })
        .collect();
    if mirrored.is_empty() {
        return;
    }

    let ns_path = format!("{api_url}/api/v1/namespaces/{NAMESPACE}");
    if get_json(client, &ns_path).await.is_none() {
        let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}});
        let _ = client.post(format!("{api_url}/api/v1/namespaces")).json(&ns).send_repeatable(retry::Policy::API).await;
    }

    // Everything there is, read once: two lists a pass rather than two GETs a
    // volume.
    let pvc_base = format!("{api_url}/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
    let pv_base = format!("{api_url}/api/v1/persistentvolumes");
    let Some(pvcs) = get_json(client, &pvc_base).await.map(|l| by_name(&l)) else { return apimachinery::reactor::failed() };
    let Some(pvs) = get_json(client, &pv_base).await.map(|l| by_name(&l)) else { return apimachinery::reactor::failed() };

    for (f, kind, component) in &mirrored {
        let (want_pv, want_pvc) = objects(f, kind, component, node);
        let pvname = pv_name(&f.name, node);
        let cname = claim_name(&f.name, node);

        // The claim first: the volume's claimRef names the claim's uid, which
        // exists only once the claim does.
        let claim = match pvcs.get(&cname) {
            Some(c) if !is_ours(c, node) => {
                debug!("PVC {NAMESPACE}/{cname} belongs to another node or tool; left alone");
                continue;
            }
            Some(c) if !c["metadata"]["deletionTimestamp"].is_null() => {
                // Being deleted: the binder lets it go once nothing mounts it,
                // and the next pass makes it again.
                continue;
            }
            Some(c) => match reconcile_pvc(c, &want_pvc) {
                Some(updated) => {
                    let got = replace(client, &format!("{pvc_base}/{cname}"), &updated).await;
                    if got.is_some() {
                        info!("PVC {NAMESPACE}/{cname} brought up to date");
                    }
                    got.or_else(|| Some(c.clone()))
                }
                None => Some(c.clone()),
            },
            None => {
                let made = create(client, &pvc_base, &want_pvc).await;
                if let Some(mut made) = made.clone() {
                    info!("listed {kind} volume {} as PVC {NAMESPACE}/{cname}", f.name);
                    // Bound from the first moment it is visible: a claim with
                    // no status reads as Unknown until the binder's next pass.
                    made["status"] = json!({
                        "phase": "Bound",
                        "accessModes": ["ReadWriteOnce"],
                        "capacity": want_pvc["spec"]["resources"]["requests"].clone(),
                    });
                    let _ = replace(client, &format!("{pvc_base}/{cname}"), &made).await;
                }
                made
            }
        };

        match pvs.get(&pvname) {
            Some(pv) if !is_ours(pv, node) => {
                warn!(
                    "PV {pvname} is not this node's ({}); this node's {} has no volume object",
                    pv["metadata"]["annotations"]["storm.io/node"].as_str().unwrap_or("?"),
                    f.name
                );
            }
            Some(pv) if !pv["metadata"]["deletionTimestamp"].is_null() => {}
            Some(pv) => {
                if let Some(updated) = reconcile_pv(pv, &want_pv, claim.as_ref()) {
                    if replace(client, &format!("{pv_base}/{pvname}"), &updated).await.is_some() {
                        info!("PV {pvname} brought up to date");
                    }
                }
            }
            None => {
                let mut pv = want_pv;
                if let Some(c) = &claim {
                    pv["spec"]["claimRef"] = claim_ref(c);
                }
                if create(client, &pv_base, &pv).await.is_some() {
                    info!("listed {kind} volume {} as PV {pvname}", f.name);
                }
            }
        }
    }

    // A volume that went away: its claim goes, and the binder makes the PV
    // Released. The PV itself is never deleted here (Retain): it is the record
    // of where the data was, for an administrator. Only on a listing that
    // named at least one of this node's volumes (above), so an engine that
    // answers with nothing yet does not let go of every claim at once.
    //
    // By the volume a claim names (`storm.io/volume`), not by its name: a
    // claim written under the old unqualified name (`fastetcd-data`, before
    // #107) still names a volume that is here, and is left as it is rather
    // than read as vanished (no migration, owner).
    let present: std::collections::HashSet<&str> = mirrored.iter().map(|(f, _, _)| f.name.as_str()).collect();
    for (name, c) in &pvcs {
        let volume = c["metadata"]["annotations"]["storm.io/volume"].as_str().unwrap_or(name);
        if !is_ours(c, node) || !c["metadata"]["deletionTimestamp"].is_null() || present.contains(volume) {
            continue;
        }
        match client.delete(format!("{pvc_base}/{name}")).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => {
                info!("volume {volume} is gone from this node: claim {NAMESPACE}/{name} deleted, its PV kept")
            }
            Ok(r) => debug!("PVC {NAMESPACE}/{name} not deleted: {}", r.status()),
            Err(e) => debug!("PVC {NAMESPACE}/{name} not deleted: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol(name: &str, role: &str, sealed: bool) -> Value {
        json!({"id": format!("id-{name}"), "name": name, "role": role, "sealed": sealed,
               "virtual_size_bytes": 1u64 << 30})
    }

    fn facts(name: &str) -> VolumeFacts {
        VolumeFacts {
            name: name.into(),
            bytes: 1 << 30,
            fs_type: Some("ext4".into()),
            fs_uuid: Some("u-1".into()),
            golden: Some("fastetcd-data".into()),
            health: Some("healthy".into()),
            access: Some("rw".into()),
            role: Some("data".into()),
            block: false,
        }
    }

    #[test]
    fn the_node_volumes_are_data_state_and_logs() {
        assert_eq!(kind_of(&vol("fastetcd-data", "data", false)), Some(("data", "fastetcd".into())));
        assert_eq!(kind_of(&vol("stormcos-state", "data", false)), Some(("state", "stormcos".into())));
        // Logs are claims too, whichever slab half they are on.
        assert_eq!(
            kind_of(&vol("rustkube-apiserver-logs", "system", false)),
            Some(("logs", "rustkube-apiserver".into()))
        );
        // What they are cloned from, a claim already, a standby, a root.
        assert_eq!(kind_of(&vol("fastetcd-data.golden", "data", true)), None);
        assert_eq!(kind_of(&vol("fastetcd-logs", "data", true)), None);
        assert_eq!(kind_of(&vol("pvc-default-db", "data", false)), None);
        assert_eq!(kind_of(&vol("standby-pvc-ext4j-1m-12345678", "data", false)), None);
        assert_eq!(kind_of(&vol("fedora-44-x86_64", "data", false)), None);
        assert_eq!(kind_of(&vol("stormpump", "system", false)), None);
        assert_eq!(kind_of(&vol("-logs", "system", false)), None);
    }

    #[test]
    fn facts_come_from_the_engines_listing() {
        let golden = json!({"id": "g", "name": "fastetcd-data.golden"});
        let v = json!({"id": "v", "name": "fastetcd-data", "virtual_size_bytes": 1u64 << 30,
                       "fs": {"kind": "ext4"}, "fs_uuid": "u-1", "parent": "g",
                       "health": "healthy", "access": "rw", "role": "data"});
        let names = HashMap::from([("g".to_string(), "fastetcd-data.golden".to_string())]);
        let _ = golden;
        assert_eq!(VolumeFacts::of(&v, &names), facts("fastetcd-data"));
    }

    #[test]
    fn a_node_volume_is_a_complete_bound_pair() {
        let (pv, pvc) = objects(&facts("fastetcd-logs"), "logs", "fastetcd", "node1");
        // They name each other.
        assert_eq!(pv["metadata"]["name"], "storm-fastetcd-logs-node1");
        assert_eq!(pvc["metadata"]["name"], "fastetcd-logs-node1");
        assert_eq!(pv["spec"]["claimRef"]["name"], "fastetcd-logs-node1");
        assert_eq!(pv["spec"]["claimRef"]["namespace"], NAMESPACE);
        assert_eq!(pvc["spec"]["volumeName"], "storm-fastetcd-logs-node1");
        assert_eq!(pvc["metadata"]["annotations"]["storm.io/volume"], "fastetcd-logs");
        // The same kind and component on both.
        for o in [&pv, &pvc] {
            assert_eq!(o["metadata"]["labels"][KIND_LABEL], "logs");
            assert_eq!(o["metadata"]["labels"][COMPONENT_LABEL], "fastetcd");
            assert_eq!(o["metadata"]["labels"][LABEL], "true");
            assert!(is_ours(o, "node1"));
            assert!(!is_ours(o, "node2"));
        }
        // Keeps its data, and says what is on it and where it came from.
        assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Retain");
        assert_eq!(pv["spec"]["csi"]["driver"], DRIVER);
        assert_eq!(pv["spec"]["csi"]["volumeHandle"], "fastetcd-logs");
        assert_eq!(pv["spec"]["csi"]["fsType"], "ext4");
        assert_eq!(pv["spec"]["csi"]["volumeAttributes"]["storm.io/golden"], "fastetcd-data");
        assert_eq!(pv["spec"]["csi"]["volumeAttributes"]["storm.io/fs-uuid"], "u-1");
        let a = &pv["metadata"]["annotations"];
        assert_eq!(a["pv.kubernetes.io/provisioned-by"], DRIVER);
        assert_eq!(a["storm.io/health"], "healthy");
        assert_eq!(a["storm.io/role"], "data");
        // Reads like any dynamically provisioned, bound claim.
        let a = &pvc["metadata"]["annotations"];
        assert_eq!(a["pv.kubernetes.io/bind-completed"], "yes");
        assert_eq!(a["pv.kubernetes.io/bound-by-controller"], "yes");
        assert_eq!(a["volume.kubernetes.io/storage-provisioner"], DRIVER);
        assert_eq!(a["volume.beta.kubernetes.io/storage-provisioner"], DRIVER);
        assert_eq!(a["volume.kubernetes.io/selected-node"], "node1");
        assert_eq!(pvc["spec"]["dataSourceRef"]["kind"], "Golden");
        assert_eq!(pvc["spec"]["resources"]["requests"]["storage"], "1Gi");
    }

    #[test]
    fn a_claim_ref_carries_the_claims_uid() {
        let pvc = json!({"metadata": {"name": "c", "namespace": "kube-system", "uid": "u-9",
                                      "resourceVersion": "42"}});
        let r = claim_ref(&pvc);
        assert_eq!(r["uid"], "u-9");
        assert_eq!(r["resourceVersion"], "42");
        assert_eq!(r["kind"], "PersistentVolumeClaim");
    }

    #[test]
    fn an_up_to_date_pair_is_not_rewritten() {
        let (pv, pvc) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        assert!(reconcile_pvc(&pvc, &pvc).is_none());
        assert!(reconcile_pv(&pv, &pv, None).is_none());
    }

    #[test]
    fn a_grown_volume_grows_its_objects_and_a_claim_never_shrinks() {
        let (pv, pvc) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        let mut bigger = facts("fastetcd-data");
        bigger.bytes = 2 << 30;
        bigger.health = Some("degraded".into());
        let (want_pv, want_pvc) = objects(&bigger, "data", "fastetcd", "node1");

        let pvc2 = reconcile_pvc(&pvc, &want_pvc).expect("request grows");
        assert_eq!(pvc2["spec"]["resources"]["requests"]["storage"], "2Gi");
        let pv2 = reconcile_pv(&pv, &want_pv, None).expect("capacity and health follow");
        assert_eq!(pv2["spec"]["capacity"]["storage"], "2Gi");
        assert_eq!(pv2["metadata"]["annotations"]["storm.io/health"], "degraded");

        // Smaller than the request: the request stays.
        let (_, smaller) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        assert!(reconcile_pvc(&pvc2, &smaller).is_none());
    }

    #[test]
    fn another_tools_labels_and_annotations_are_kept() {
        let (_, mut pvc) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        pvc["metadata"]["labels"]["team"] = json!("db");
        pvc["metadata"]["annotations"]["note"] = json!("keep");
        let (_, want) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        assert!(reconcile_pvc(&pvc, &want).is_none(), "nothing of ours changed");
    }

    #[test]
    fn a_claim_made_again_is_the_one_the_volume_names() {
        let (mut pv, _) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        pv["spec"]["claimRef"]["uid"] = json!("old");
        let claim = json!({"metadata": {"name": "fastetcd-data", "namespace": NAMESPACE, "uid": "new",
                                        "resourceVersion": "7"}});
        let got = reconcile_pv(&pv, &pv.clone(), Some(&claim)).expect("uid follows the claim");
        assert_eq!(got["spec"]["claimRef"]["uid"], "new");
    }

    #[test]
    fn sizes_read_as_quantities() {
        assert_eq!(quantity(1 << 30), "1Gi");
        assert_eq!(quantity(64 << 20), "64Mi");
        assert_eq!(quantity(3 << 40), "3Ti");
        assert_eq!(quantity(1 << 50), "1Pi");
        assert_eq!(quantity(4 << 40), "4Ti");
        assert_eq!(quantity(1000), "1000");
    }

    /// The whole pass against a fake apiserver and engine: every volume gets
    /// exactly one PV and one PVC that name each other, a deleted claim comes
    /// back, two nodes with the same volume each get their own pair (#107),
    /// and an old unqualified pair is left alone.
    #[tokio::test]
    async fn every_node_volume_gets_its_pair_and_a_deleted_claim_comes_back() {
        let api = fake::Api::serve().await;
        let engine = fake::engine(json!({"items": [
            {"id": "g", "name": "fastetcd-data.golden", "sealed": true, "role": "data", "virtual_size_bytes": 1u64 << 30},
            {"id": "a", "name": "fastetcd-data", "parent": "g", "role": "data", "virtual_size_bytes": 1u64 << 30,
             "fs": {"kind": "ext4"}, "fs_uuid": "u-a", "health": "healthy", "access": "rw"},
            {"id": "b", "name": "fastetcd-logs", "role": "system", "virtual_size_bytes": 64u64 << 20,
             "fs": {"kind": "ext4"}},
            {"id": "c", "name": "pvc-default-db", "role": "data", "virtual_size_bytes": 1u64 << 30},
        ]}))
        .await;
        let client = reqwest::Client::new();

        let claim = |v: &str, n: &str| format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{v}-{n}");
        let pv_at = |v: &str, n: &str| format!("/api/v1/persistentvolumes/storm-{v}-{n}");

        // A pair written under the old unqualified names (before #107): left
        // as it is, never read as a vanished volume (no migration).
        let (mut old_pv, mut old_pvc) = objects(&facts("fastetcd-data"), "data", "fastetcd", "node1");
        old_pvc["metadata"]["name"] = json!("fastetcd-data");
        old_pv["metadata"]["name"] = json!("storm-fastetcd-data");
        api.put(&format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/fastetcd-data"), old_pvc.clone());
        api.put("/api/v1/persistentvolumes/storm-fastetcd-data", old_pv.clone());

        mirror(&client, &api.url, &engine, "node1").await;
        for name in ["fastetcd-data", "fastetcd-logs"] {
            let pvc = api.get(&claim(name, "node1")).unwrap();
            let pv = api.get(&pv_at(name, "node1")).unwrap();
            assert_eq!(pvc["spec"]["volumeName"], json!(format!("storm-{name}-node1")));
            assert_eq!(pv["spec"]["claimRef"]["name"], json!(format!("{name}-node1")));
            assert_eq!(pv["spec"]["claimRef"]["uid"], pvc["metadata"]["uid"], "{name}");
            assert_eq!(pv["metadata"]["labels"][KIND_LABEL], pvc["metadata"]["labels"][KIND_LABEL]);
            assert_eq!(pvc["status"]["phase"], "Bound");
        }
        assert_eq!(api.count("persistentvolumeclaims"), 3, "pvc-* is the built-in driver's; the old one stays");
        assert_eq!(api.count("persistentvolumes"), 3);
        assert_eq!(
            api.get(&format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/fastetcd-data")),
            Some(old_pvc),
            "the old unqualified claim is left as it was"
        );

        // A second pass changes nothing.
        let writes = api.writes();
        mirror(&client, &api.url, &engine, "node1").await;
        assert_eq!(api.writes(), writes, "an up-to-date pair is not rewritten");

        // Deleted by hand: made again, and the volume names the new claim.
        api.delete(&claim("fastetcd-data", "node1"));
        mirror(&client, &api.url, &engine, "node1").await;
        let pvc = api.get(&claim("fastetcd-data", "node1")).unwrap();
        let pv = api.get(&pv_at("fastetcd-data", "node1")).unwrap();
        assert_eq!(pv["spec"]["claimRef"]["uid"], pvc["metadata"]["uid"]);

        // Another node with the same volumes gets its own pair (#107), and
        // leaves this node's as they are.
        let node1 = api.get(&claim("fastetcd-data", "node1"));
        mirror(&client, &api.url, &engine, "node2").await;
        assert_eq!(api.get(&claim("fastetcd-data", "node1")), node1);
        for name in ["fastetcd-data", "fastetcd-logs"] {
            let pvc = api.get(&claim(name, "node2")).expect("node2's claim");
            let pv = api.get(&pv_at(name, "node2")).expect("node2's PV");
            assert_eq!(pv["spec"]["claimRef"]["uid"], pvc["metadata"]["uid"]);
            assert_eq!(pvc["metadata"]["annotations"]["storm.io/node"], "node2");
            assert_eq!(pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0], "node2");
        }

        // A volume that went away: its claim goes, its PV stays (Retain), and
        // the other volume's pair is untouched.
        let fewer = fake::engine(json!({"items": [
            {"id": "a", "name": "fastetcd-data", "role": "data", "virtual_size_bytes": 1u64 << 30,
             "fs": {"kind": "ext4"}, "fs_uuid": "u-a", "health": "healthy", "access": "rw"},
        ]}))
        .await;
        mirror(&client, &api.url, &fewer, "node1").await;
        assert!(api.get(&claim("fastetcd-logs", "node1")).is_none());
        assert!(api.get(&pv_at("fastetcd-logs", "node1")).is_some(), "the PV is never deleted");
        assert!(api.get(&claim("fastetcd-data", "node1")).is_some());
        assert!(api.get(&claim("fastetcd-logs", "node2")).is_some(), "node2's volume is node2's business");

        // An engine that lists none of the node's volumes lets go of nothing.
        let empty = fake::engine(json!({"items": []})).await;
        mirror(&client, &api.url, &empty, "node1").await;
        assert!(api.get(&claim("fastetcd-data", "node1")).is_some());
    }

    #[test]
    fn a_driver_claim_is_bound_the_same_way() {
        let mut pvc = json!({"metadata": {"name": "db", "namespace": "default", "uid": "u-1",
                                          "annotations": {"keep": "me"}},
                             "spec": {"resources": {"requests": {"storage": "1Gi"}}}});
        bind_pvc(&mut pvc, "pvc-default-db", "node1", "pvc-default-db");
        assert_eq!(pvc["spec"]["volumeName"], "pvc-default-db");
        assert_eq!(pvc["spec"]["storageClassName"], crate::storage::STORAGE_CLASS);
        let a = &pvc["metadata"]["annotations"];
        assert_eq!(a["keep"], "me");
        assert_eq!(a["pv.kubernetes.io/bind-completed"], "yes");
        assert_eq!(a["volume.kubernetes.io/selected-node"], "node1");

        let f = VolumeFacts { name: "pvc-default-db".into(), bytes: 1 << 30, fs_type: Some("ext4".into()),
                              ..Default::default() };
        let pv = stormblock_pv(&f, "pvc-default-db", "node1", claim_ref(&pvc), "Delete");
        assert_eq!(pv["spec"]["claimRef"]["uid"], "u-1");
        assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Delete");
        assert_eq!(pv["spec"]["csi"]["fsType"], "ext4");
        assert!(pv["metadata"]["labels"].is_null(), "a driver claim is not a system volume");
    }

    /// A small in-memory apiserver: GET, list, POST (uid and resourceVersion
    /// assigned), PUT and DELETE by path.
    mod fake {
        use super::*;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        pub struct Api {
            pub url: String,
            store: Arc<Mutex<HashMap<String, Value>>>,
            writes: Arc<Mutex<usize>>,
        }

        impl Api {
            pub async fn serve() -> Api {
                let store: Arc<Mutex<HashMap<String, Value>>> = Arc::default();
                let writes: Arc<Mutex<usize>> = Arc::default();
                let (s, w) = (store.clone(), writes.clone());
                let app = axum::Router::new().fallback(
                    move |method: axum::http::Method,
                          uri: axum::http::Uri,
                          body: axum::body::Bytes| {
                        let (s, w) = (s.clone(), w.clone());
                        async move { handle(&s, &w, method, uri.path(), &body) }
                    },
                );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
                Api { url, store, writes }
            }
            pub fn get(&self, path: &str) -> Option<Value> {
                self.store.lock().unwrap().get(path).cloned()
            }
            pub fn delete(&self, path: &str) {
                self.store.lock().unwrap().remove(path);
            }
            /// Store an object as though another writer had made it.
            pub fn put(&self, path: &str, obj: Value) {
                self.store.lock().unwrap().insert(path.to_string(), obj);
            }
            pub fn count(&self, resource: &str) -> usize {
                self.store.lock().unwrap().keys().filter(|k| k.contains(&format!("/{resource}/"))).count()
            }
            pub fn writes(&self) -> usize {
                *self.writes.lock().unwrap()
            }
        }

        fn handle(
            store: &Mutex<HashMap<String, Value>>,
            writes: &Mutex<usize>,
            method: axum::http::Method,
            path: &str,
            body: &[u8],
        ) -> (axum::http::StatusCode, axum::Json<Value>) {
            use axum::http::{Method, StatusCode};
            let mut s = store.lock().unwrap();
            let ok = |v: Value| (StatusCode::OK, axum::Json(v));
            match method {
                Method::GET => {
                    if let Some(v) = s.get(path) {
                        return ok(v.clone());
                    }
                    let prefix = format!("{path}/");
                    let items: Vec<Value> =
                        s.iter().filter(|(k, _)| k.starts_with(&prefix)).map(|(_, v)| v.clone()).collect();
                    if path.ends_with("persistentvolumes") || path.ends_with("persistentvolumeclaims") {
                        return ok(json!({ "items": items }));
                    }
                    (StatusCode::NOT_FOUND, axum::Json(json!({})))
                }
                Method::POST => {
                    let mut obj: Value = serde_json::from_slice(body).unwrap();
                    let name = obj["metadata"]["name"].as_str().unwrap().to_string();
                    let key = format!("{path}/{name}");
                    if s.contains_key(&key) {
                        return (StatusCode::CONFLICT, axum::Json(json!({})));
                    }
                    *writes.lock().unwrap() += 1;
                    let n = *writes.lock().unwrap();
                    obj["metadata"]["uid"] = json!(format!("uid-{n}"));
                    obj["metadata"]["resourceVersion"] = json!(n.to_string());
                    s.insert(key, obj.clone());
                    ok(obj)
                }
                Method::PUT => {
                    let mut obj: Value = serde_json::from_slice(body).unwrap();
                    *writes.lock().unwrap() += 1;
                    let n = *writes.lock().unwrap();
                    obj["metadata"]["resourceVersion"] = json!(n.to_string());
                    s.insert(path.to_string(), obj.clone());
                    ok(obj)
                }
                Method::DELETE => match s.remove(path) {
                    Some(v) => {
                        *writes.lock().unwrap() += 1;
                        ok(v)
                    }
                    None => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                },
                _ => (StatusCode::METHOD_NOT_ALLOWED, axum::Json(json!({}))),
            }
        }

        pub async fn engine(volumes: Value) -> crate::engine::EngineClient {
            let app = axum::Router::new().route(
                "/api/v1/volumes",
                axum::routing::get(move || {
                    let v = volumes.clone();
                    async move { axum::Json(v) }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none())
        }
    }

    #[test]
    fn a_block_claims_volume_is_a_block_pv() {
        let mut f = VolumeFacts { name: "pvc-ns-raw".into(), bytes: 1 << 40, ..Default::default() };
        let pv = stormblock_pv(&f, "pvc-ns-raw", "n1", json!({}), "Delete");
        assert_eq!(pv["spec"]["volumeMode"], "Filesystem");
        f.block = true;
        let pv = stormblock_pv(&f, "pvc-ns-raw", "n1", json!({}), "Delete");
        assert_eq!(pv["spec"]["volumeMode"], "Block");
        assert!(pv["spec"]["csi"].get("fsType").is_none());
    }
}
