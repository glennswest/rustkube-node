//! Mirror pods for static pods (#258), as upstream's kubelet publishes them.
//!
//! A static pod runs from a manifest in `--pod-manifest-path`; the apiserver
//! does not know it unless the kubelet tells it. Since stormcos#559 the
//! control plane (etcd, kube-apiserver, kube-controller-manager,
//! kube-scheduler) is static pods, so without a mirror `get pods`, the console
//! and stormcentral's checks see none of it, and `pods/log` has no pod to ask.
//!
//! Upstream's shape: the static pod is named `<name>-<node>` (here too, from
//! the manifest's load), and its mirror is the same pod in the API, annotated
//! `kubernetes.io/config.mirror: <hash>` (and `config.hash`, `config.source:
//! file`), owned by the Node. The kubelet writes the mirror's status from the
//! static pod's containers; it never runs a mirror pod. A mirror is recreated
//! when it is deleted or its hash is not the manifest's (the manifest
//! changed), and deleted when its manifest is gone.

use serde_json::{json, Value};
use std::collections::HashMap;

/// Upstream's annotation on a mirror pod: the static pod's hash.
pub const CONFIG_MIRROR: &str = "kubernetes.io/config.mirror";
/// The static pod's hash, on the static pod and its mirror.
pub const CONFIG_HASH: &str = "kubernetes.io/config.hash";

/// A mirror pod (any kubelet's): never run as a pod.
pub fn is_mirror(pod: &Value) -> bool {
    pod["metadata"]["annotations"][CONFIG_MIRROR].is_string()
}

/// A manifest's hash: what its mirror carries, so a changed manifest gets a
/// new mirror. The first 16 hex digits of its text's SHA-256.
pub fn manifest_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// `<namespace>/<name>`.
pub fn key(pod: &Value) -> String {
    format!(
        "{}/{}",
        pod["metadata"]["namespace"].as_str().unwrap_or(""),
        pod["metadata"]["name"].as_str().unwrap_or("")
    )
}

/// The mirror of `static_pod` to create: its spec, labels and annotations,
/// the mirror annotation, owned by the Node (`node_uid`).
pub fn mirror_of(static_pod: &Value, node: &str, node_uid: &str) -> Value {
    let meta = &static_pod["metadata"];
    let hash = meta["annotations"][CONFIG_HASH].as_str().unwrap_or("");
    let mut annotations = meta["annotations"].as_object().cloned().unwrap_or_default();
    annotations.insert(CONFIG_MIRROR.into(), json!(hash));
    annotations.insert(CONFIG_HASH.into(), json!(hash));
    annotations.insert(crate::pod_manager::CONFIG_SOURCE.into(), json!("file"));
    let mut metadata = json!({
        "name": meta["name"],
        "namespace": meta["namespace"],
        "annotations": annotations,
        "ownerReferences": [{
            "apiVersion": "v1", "kind": "Node", "name": node, "uid": node_uid, "controller": true,
        }],
    });
    if meta["labels"].is_object() {
        metadata["labels"] = meta["labels"].clone();
    }
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": static_pod["spec"],
    })
}

/// What one pass does to make the API's mirrors match the static pods.
#[derive(Debug, Default, PartialEq)]
pub struct Plan {
    /// Static pods (by key) with no live mirror: create one.
    pub create: Vec<String>,
    /// Mirrors to delete (`namespace`, `name`, `uid`): an orphan, or one whose
    /// hash is not its manifest's (recreated once it is gone).
    pub delete: Vec<(String, String, String)>,
    /// The live mirrors that match, by key: the objects status is written to.
    pub current: HashMap<String, Value>,
}

/// Compare this node's static pods with the mirror pods the API has on it.
/// A mirror being deleted is neither current nor deleted again: it is
/// recreated once the API has let it go.
pub fn plan(statics: &[Value], api: &[Value], node: &str) -> Plan {
    let mirrors: HashMap<String, &Value> = api
        .iter()
        .filter(|p| is_mirror(p) && p["spec"]["nodeName"].as_str() == Some(node))
        .map(|p| (key(p), p))
        .collect();
    let mut out = Plan::default();
    for s in statics {
        let k = key(s);
        let hash = s["metadata"]["annotations"][CONFIG_HASH].as_str().unwrap_or("");
        match mirrors.get(&k) {
            None => out.create.push(k),
            Some(m) if !m["metadata"]["deletionTimestamp"].is_null() => {}
            Some(m) if m["metadata"]["annotations"][CONFIG_MIRROR].as_str() != Some(hash) => {
                out.delete.push(identity(m));
            }
            Some(m) => {
                out.current.insert(k, (*m).clone());
            }
        }
    }
    let wanted: std::collections::HashSet<String> = statics.iter().map(key).collect();
    for (k, m) in &mirrors {
        if !wanted.contains(k) && m["metadata"]["deletionTimestamp"].is_null() {
            out.delete.push(identity(m));
        }
    }
    out.delete.sort();
    out
}

fn identity(m: &Value) -> (String, String, String) {
    let meta = &m["metadata"];
    (
        meta["namespace"].as_str().unwrap_or("").to_string(),
        meta["name"].as_str().unwrap_or("").to_string(),
        meta["uid"].as_str().unwrap_or("").to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn static_pod(name: &str, hash: &str) -> Value {
        json!({
            "kind": "Pod",
            "metadata": {
                "name": format!("{name}-n1"), "namespace": "kube-system", "uid": format!("static-{name}-n1"),
                "labels": {"component": name},
                "annotations": {CONFIG_HASH: hash, "kubernetes.io/config.source": "file"},
            },
            "spec": {"nodeName": "n1", "containers": [{"name": name, "image": format!("/pallets/{name}")}]},
        })
    }

    fn mirror(name: &str, hash: &str, uid: &str) -> Value {
        json!({
            "metadata": {
                "name": format!("{name}-n1"), "namespace": "kube-system", "uid": uid,
                "annotations": {CONFIG_MIRROR: hash},
            },
            "spec": {"nodeName": "n1"},
        })
    }

    #[test]
    fn the_mirror_is_the_static_pod_named_owned_and_annotated() {
        let m = mirror_of(&static_pod("etcd", "abc"), "n1", "node-uid");
        assert_eq!(m["metadata"]["name"], "etcd-n1");
        assert_eq!(m["metadata"]["namespace"], "kube-system");
        assert!(m["metadata"]["uid"].is_null(), "the API gives the mirror its own uid");
        assert_eq!(m["metadata"]["annotations"][CONFIG_MIRROR], "abc");
        assert_eq!(m["metadata"]["annotations"][CONFIG_HASH], "abc");
        assert_eq!(m["metadata"]["annotations"]["kubernetes.io/config.source"], "file");
        assert_eq!(m["metadata"]["labels"]["component"], "etcd");
        assert_eq!(m["metadata"]["ownerReferences"][0]["kind"], "Node");
        assert_eq!(m["metadata"]["ownerReferences"][0]["uid"], "node-uid");
        assert_eq!(m["spec"]["containers"][0]["image"], "/pallets/etcd");
        assert!(m["status"].is_null());
        assert!(is_mirror(&m));
        assert!(!is_mirror(&static_pod("etcd", "abc")));
    }

    #[test]
    fn missing_mirrors_are_made_stale_and_orphaned_ones_deleted() {
        let statics = [static_pod("etcd", "h1"), static_pod("kube-apiserver", "h2"), static_pod("kube-scheduler", "h3")];
        let mut going = mirror("kube-scheduler", "h3", "u-s");
        going["metadata"]["deletionTimestamp"] = json!("2026-10-21T00:00:00Z");
        let mut other_node = mirror("old", "h9", "u-o2");
        other_node["spec"]["nodeName"] = json!("n2");
        let not_a_mirror = json!({"metadata": {"name": "web", "namespace": "default", "uid": "u-w"}, "spec": {"nodeName": "n1"}});
        let api = [
            mirror("etcd", "h1", "u-e"),
            mirror("kube-apiserver", "stale", "u-a"),
            going,
            mirror("gone", "h4", "u-g"),
            other_node,
            not_a_mirror,
        ];
        let p = plan(&statics, &api, "n1");
        assert!(p.create.is_empty(), "{:?}", p.create);
        assert_eq!(
            p.delete,
            vec![
                ("kube-system".to_string(), "gone-n1".to_string(), "u-g".to_string()),
                ("kube-system".to_string(), "kube-apiserver-n1".to_string(), "u-a".to_string()),
            ]
        );
        assert_eq!(p.current.keys().collect::<Vec<_>>(), ["kube-system/etcd-n1"]);

        // Once the deleted ones are gone, they are made.
        let p = plan(&statics, &[mirror("etcd", "h1", "u-e")], "n1");
        assert_eq!(p.create, ["kube-system/kube-apiserver-n1", "kube-system/kube-scheduler-n1"]);
        assert!(p.delete.is_empty());
    }

    #[test]
    fn a_manifests_hash_follows_its_text() {
        assert_eq!(manifest_hash("a"), manifest_hash("a"));
        assert_ne!(manifest_hash("a"), manifest_hash("b"));
        assert_eq!(manifest_hash("a").len(), 16);
    }
}
