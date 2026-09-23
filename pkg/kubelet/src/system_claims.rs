//! The node's own data containers, listed as PersistentVolumeClaims.
//!
//! Every service on a stormcos node keeps its state in a stormblock data
//! volume — `fastetcd-data`, `stormcert-data`, `registry-data`, `stormcos-state`
//! — a copy-on-write clone of a golden, mounted at boot by the initramfs. That
//! *is* this platform's storage class at work: a claim is a clone of a data
//! volume. But none of them appeared in the API, so the cluster showed zero
//! claims on a node running two dozen, and nothing a Kubernetes tool could see
//! said where a service's data lives or how big it is (rustkube-node#49).
//!
//! This mirrors them, the way `mirror_node_services` mirrors the services:
//!
//! - namespace `kube-system`, beside the service's own pod: the node's services
//!   are mirrored there (`mirror.rs`), and a service's data belongs with it —
//!   `kube-system/fastetcd-data` next to `kube-system/fastetcd-<node>`
//! - a `PersistentVolume` named `storm-<volume>`, class `stormblock`, the
//!   stormblock volume as its CSI handle, pinned to this node, reclaim
//!   **Retain**: deleting the object must never delete a service's data.
//! - a `PersistentVolumeClaim` named `<volume>`, already bound to it, with the
//!   golden it was cloned from as its `dataSourceRef`.
//!
//! Created bound, so neither provisioner acts on them: the control plane's
//! skips claims with a `volumeName`, and the kubelet mounts a bound claim's own
//! volume rather than deriving one. A claim can name one of these as its
//! `dataSource` to get a clone of a service's data.
//!
//! stormblock is the source of truth. This only ever creates; a volume that
//! goes away leaves its objects for an administrator, which is what Retain
//! means everywhere else too.

use serde_json::{json, Value};
use tracing::{debug, info};

/// Where the node's own claims live: with the node's services.
pub const NAMESPACE: &str = "kube-system";

/// Marks the objects this mirror owns — and marks a volume a node service has
/// mounted, which a pod may clone but must not mount.
pub const LABEL: &str = "storm.io/system-volume";

/// Is this stormblock volume one of the node's data containers?
///
/// A writable data-role volume named `*-data` or `*-state`. Goldens are sealed
/// and are what these are cloned *from*; `pvc-*` are claims already; `standby-*`
/// are pre-minted clones waiting for a claim.
pub fn is_data_container(v: &Value) -> bool {
    let name = v["name"].as_str().unwrap_or("");
    v["role"].as_str() == Some("data")
        && !v["sealed"].as_bool().unwrap_or(false)
        && !name.ends_with(".golden")
        && !name.starts_with("pvc-")
        && !name.starts_with("standby-")
        && (name.ends_with("-data") || name.ends_with("-state"))
}

/// Bytes as a Kubernetes quantity: `1Gi` rather than `1073741824`.
pub fn quantity(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] =
        [("Ti", 1 << 40), ("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)];
    for (u, n) in UNITS {
        if bytes >= n && bytes % n == 0 {
            return format!("{}{u}", bytes / n);
        }
    }
    bytes.to_string()
}

/// The PV name for a data container.
pub fn pv_name(volume: &str) -> String {
    format!("storm-{volume}")
}

/// The PV and PVC for one data container.
pub fn objects(v: &Value, node: &str, golden: Option<&str>) -> (Value, Value) {
    let name = v["name"].as_str().unwrap_or("");
    let bytes = v["virtual_size_bytes"].as_u64().unwrap_or(0);
    let capacity = json!({ "storage": quantity(bytes) });
    let labels = json!({ LABEL: "true" });
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": pv_name(name),
            "labels": labels,
            "annotations": {
                "storm.io/node": node,
                "storm.io/volume": name,
                "pv.kubernetes.io/provisioned-by": "stormblock.storm.io/system",
            },
        },
        "spec": {
            "capacity": capacity,
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": crate::storage::STORAGE_CLASS,
            "volumeMode": "Filesystem",
            "claimRef": {
                "kind": "PersistentVolumeClaim",
                "namespace": NAMESPACE,
                "name": name,
            },
            "csi": { "driver": "stormblock.storm.io", "volumeHandle": name },
            "nodeAffinity": { "required": { "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": "kubernetes.io/hostname",
                    "operator": "In",
                    "values": [node],
                }],
            }]}},
        },
    });
    let mut spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "storageClassName": crate::storage::STORAGE_CLASS,
        "volumeName": pv_name(name),
        "volumeMode": "Filesystem",
        "resources": { "requests": capacity },
    });
    if let Some(g) = golden {
        spec["dataSourceRef"] = json!({ "apiGroup": "storm.io", "kind": "Golden", "name": g });
    }
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": labels,
            "annotations": { "storm.io/node": node, "storm.io/volume": name },
        },
        "spec": spec,
    });
    (pv, pvc)
}

/// One pass: every data container on this node has its PV and bound PVC.
pub async fn mirror(client: &reqwest::Client, api_url: &str, storage_url: &str, node: &str) {
    if api_url.is_empty() {
        return;
    }
    let vols: Value = match client.get(format!("{storage_url}/api/v1/volumes")).send().await {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => v,
            Err(_) => return,
        },
        _ => return,
    };
    let items = vols["items"].as_array().cloned().unwrap_or_default();
    let by_id: std::collections::HashMap<&str, &str> = items
        .iter()
        .filter_map(|v| Some((v["id"].as_str()?, v["name"].as_str()?)))
        .collect();
    let containers: Vec<&Value> = items.iter().filter(|v| is_data_container(v)).collect();
    if containers.is_empty() {
        return;
    }

    let ns_path = format!("{api_url}/api/v1/namespaces/{NAMESPACE}");
    if !matches!(client.get(&ns_path).send().await, Ok(r) if r.status().is_success()) {
        let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}});
        let _ = client.post(format!("{api_url}/api/v1/namespaces")).json(&ns).send().await;
    }

    for v in containers {
        let name = v["name"].as_str().unwrap_or("");
        let golden = v["parent"]
            .as_str()
            .and_then(|p| by_id.get(p).copied())
            .map(|g| g.strip_suffix(".golden").unwrap_or(g).to_string());
        let (pv, pvc) = objects(v, node, golden.as_deref());

        let pv_path = format!("{api_url}/api/v1/persistentvolumes/{}", pv_name(name));
        if !matches!(client.get(&pv_path).send().await, Ok(r) if r.status().is_success()) {
            match client.post(format!("{api_url}/api/v1/persistentvolumes")).json(&pv).send().await {
                Ok(r) if r.status().is_success() => info!("listed data container {name} as PV {}", pv_name(name)),
                Ok(r) => debug!("PV {} not created: {}", pv_name(name), r.status()),
                Err(e) => debug!("PV {} not created: {e}", pv_name(name)),
            }
        }

        let pvc_path = format!("{api_url}/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{name}");
        if !matches!(client.get(&pvc_path).send().await, Ok(r) if r.status().is_success()) {
            let url = format!("{api_url}/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims");
            match client.post(url).json(&pvc).send().await {
                Ok(r) if r.status().is_success() => {
                    info!("listed data container {name} as PVC {NAMESPACE}/{name}");
                    // Bound from the first moment it is visible: a claim with
                    // no status reads as Unknown until the binder's next pass,
                    // and a console shows every service's data as unhealthy
                    // for that window.
                    if let Ok(mut made) = r.json::<Value>().await {
                        made["status"] = json!({
                            "phase": "Bound",
                            "accessModes": ["ReadWriteOnce"],
                            "capacity": pvc["spec"]["resources"]["requests"].clone(),
                        });
                        let _ = client.put(&pvc_path).json(&made).send().await;
                    }
                }
                Ok(r) => debug!("PVC {NAMESPACE}/{name} not created: {}", r.status()),
                Err(e) => debug!("PVC {NAMESPACE}/{name} not created: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol(name: &str, role: &str, sealed: bool) -> Value {
        json!({"name": name, "role": role, "sealed": sealed, "virtual_size_bytes": 1u64 << 30})
    }

    #[test]
    fn the_data_containers_are_the_ones_listed() {
        assert!(is_data_container(&vol("fastetcd-data", "data", false)));
        assert!(is_data_container(&vol("stormcos-state", "data", false)));
        // What they are cloned from, not a container.
        assert!(!is_data_container(&vol("fastetcd-data.golden", "data", true)));
        // Already a claim, or waiting to become one.
        assert!(!is_data_container(&vol("pvc-default-db", "data", false)));
        assert!(!is_data_container(&vol("standby-pvc-ext4j-1m-12345678", "data", false)));
        // A cloud image, a log, a system volume.
        assert!(!is_data_container(&vol("fedora-44-x86_64", "data", false)));
        assert!(!is_data_container(&vol("fastetcd-logs", "system", false)));
        assert!(!is_data_container(&vol("stormpump", "system", false)));
    }

    #[test]
    fn a_data_container_is_a_bound_claim_that_keeps_its_data() {
        let (pv, pvc) = objects(&vol("fastetcd-data", "data", false), "node1", Some("fastetcd-data"));
        assert_eq!(pv["metadata"]["name"], "storm-fastetcd-data");
        assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Retain");
        assert_eq!(pv["spec"]["csi"]["volumeHandle"], "fastetcd-data");
        assert_eq!(pv["spec"]["claimRef"]["namespace"], NAMESPACE);
        assert_eq!(pvc["spec"]["volumeName"], "storm-fastetcd-data");
        assert_eq!(pvc["spec"]["storageClassName"], "stormblock");
        assert_eq!(pvc["spec"]["dataSourceRef"]["kind"], "Golden");
        assert_eq!(pvc["spec"]["resources"]["requests"]["storage"], "1Gi");
    }

    #[test]
    fn sizes_read_as_quantities() {
        assert_eq!(quantity(1 << 30), "1Gi");
        assert_eq!(quantity(64 << 20), "64Mi");
        assert_eq!(quantity(3 << 40), "3Ti");
        assert_eq!(quantity(1000), "1000");
    }
}
