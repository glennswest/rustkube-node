//! The node's own services, as pods.
//!
//! **stormpump runs the things that make a node a node** — the storage engine,
//! the registry, the control plane itself — and none of it is visible to
//! anyone holding `oc`. They are not pods. Nothing ever created an API object
//! for them. Their logs are on volumes a person cannot reach, and their
//! restart counts live in PID 1's memory.
//!
//! That gap cost most of a day: a workload that would not start was diagnosed
//! from a single status string, while the supervisor knew the state, the
//! restart count and the exit code the whole time.
//!
//! This is Kubernetes' own answer to the same problem. A kubelet that runs
//! something directly creates a **mirror pod**: a read-only object in the API
//! that says "this is running here", so `get`, `describe` and `logs` work on
//! it like anything else. Upstream does it for static pods; this does it for
//! whatever PID 1 reports.
//!
//! What a mirror pod is *not* is a scheduling decision. Nothing acts on these:
//! the scheduler does not place them, deleting one does not stop anything, and
//! the annotation says where they came from. They exist to be read.

use serde_json::{json, Value};

/// One asset, as PID 1 reports it in `/run/stormpump/assets.json`.
#[derive(Debug, Clone, PartialEq)]
pub struct Asset {
    pub name: String,
    pub running: bool,
    pub restarts: u32,
    pub age_secs: u64,
}

/// Parse the asset table.
///
/// Hand-parsed rather than pulled through a schema: the file is written by
/// PID 1 with `write!` and no serialiser, so the two ends are deliberately
/// small and the shape is three fields. A file that cannot be read at all is
/// an empty list — the node still works, it just cannot be seen, and that is
/// not a reason to fail anything.
pub fn parse_assets(text: &str) -> Vec<Asset> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let Some(items) = v["assets"].as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|a| {
            let name = a["name"].as_str()?.to_string();
            if name.is_empty() {
                return None;
            }
            Some(Asset {
                name,
                running: a["running"].as_bool().unwrap_or(false),
                restarts: a["restarts"].as_u64().unwrap_or(0) as u32,
                age_secs: a["age_secs"].as_u64().unwrap_or(0),
            })
        })
        .collect()
}

/// The pod name for an asset.
///
/// `<asset>-<node>`, which is upstream's convention for a static pod's mirror:
/// the name has to be unique across the cluster, and two nodes both running a
/// storage engine is the normal case rather than a collision.
pub fn mirror_name(asset: &str, node: &str) -> String {
    format!("{asset}-{node}")
}

/// Build the mirror pod for an asset.
///
/// `config.source: stormpump` is the annotation that says this object
/// describes something the API did not schedule — upstream writes `file` or
/// `http` there for the same reason. The owner reference is the Node, so the
/// object is collected when the node goes away and nothing has to remember to
/// clean it up.
pub fn mirror_pod(asset: &Asset, node: &str, node_uid: &str, started: &str) -> Value {
    let phase = if asset.running { "Running" } else { "Failed" };
    let state = if asset.running {
        json!({ "running": { "startedAt": started } })
    } else {
        // No exit code: PID 1 reports whether it is up, not how it died. A
        // fabricated 0 would read as a clean exit.
        json!({ "terminated": { "reason": "Error", "finishedAt": started } })
    };

    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": mirror_name(&asset.name, node),
            "namespace": "kube-system",
            "annotations": {
                "kubernetes.io/config.source": "stormpump",
                // Says plainly that editing it changes nothing, because a
                // read-only object that looks writable invites someone to try.
                "storm.io/mirror": "true",
            },
            "labels": {
                "storm.io/asset": asset.name,
                "storm.io/component": "node-service",
            },
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Node",
                "name": node,
                "uid": node_uid,
                "controller": true,
            }],
        },
        "spec": {
            "nodeName": node,
            "hostNetwork": true,
            // Never evicted and never rescheduled: this describes something
            // PID 1 is already running, and moving it is not a thing anyone
            // can do from here.
            "priorityClassName": "system-node-critical",
            "tolerations": [{ "operator": "Exists" }],
            "containers": [{
                "name": asset.name,
                "image": format!("stormpump://{}", asset.name),
            }],
        },
        "status": {
            "phase": phase,
            "hostIP": "",
            "startTime": started,
            "conditions": [
                { "type": "PodScheduled", "status": "True" },
                { "type": "Initialized", "status": "True" },
                { "type": "ContainersReady",
                  "status": if asset.running { "True" } else { "False" } },
                { "type": "Ready",
                  "status": if asset.running { "True" } else { "False" } },
            ],
            "containerStatuses": [{
                "name": asset.name,
                "image": format!("stormpump://{}", asset.name),
                "ready": asset.running,
                "restartCount": asset.restarts,
                "state": state,
            }],
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assets_parse_and_a_bad_file_is_empty_not_fatal() {
        let text = r#"{"assets":[
            {"name":"stormblock","running":true,"restarts":0,"age_secs":120,"domain":1},
            {"name":"registry","running":false,"restarts":7,"age_secs":3,"domain":1}]}"#;
        let a = parse_assets(text);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], Asset { name: "stormblock".into(), running: true, restarts: 0, age_secs: 120 });
        assert_eq!(a[1].restarts, 7);
        assert!(!a[1].running);

        // A node that cannot be read is a node that cannot be seen, which is
        // not a reason to fail anything.
        assert!(parse_assets("").is_empty());
        assert!(parse_assets("{").is_empty());
        assert!(parse_assets(r#"{"assets":"nonsense"}"#).is_empty());
        // An entry with no name is skipped rather than named "".
        assert!(parse_assets(r#"{"assets":[{"running":true}]}"#).is_empty());
    }

    /// `<asset>-<node>`, because two nodes both running a storage engine is
    /// the normal case and not a collision.
    #[test]
    fn the_mirror_name_is_scoped_to_the_node() {
        assert_eq!(mirror_name("stormblock", "storm-2c91b3"), "stormblock-storm-2c91b3");
        assert_ne!(
            mirror_name("stormblock", "node-a"),
            mirror_name("stormblock", "node-b")
        );
    }

    #[test]
    fn a_running_asset_mirrors_as_a_ready_pod() {
        let a = Asset { name: "stormblock".into(), running: true, restarts: 2, age_secs: 60 };
        let p = mirror_pod(&a, "n1", "uid-1", "2026-08-29T00:00:00Z");
        assert_eq!(p["status"]["phase"], "Running");
        assert_eq!(p["status"]["containerStatuses"][0]["restartCount"], 2);
        assert_eq!(p["status"]["containerStatuses"][0]["ready"], true);
        assert!(p["status"]["containerStatuses"][0]["state"]["running"].is_object());
        // The annotation is what says the API did not schedule this.
        assert_eq!(p["metadata"]["annotations"]["kubernetes.io/config.source"], "stormpump");
        // Owned by the Node, so it is collected with it.
        assert_eq!(p["metadata"]["ownerReferences"][0]["kind"], "Node");
        assert_eq!(p["metadata"]["ownerReferences"][0]["uid"], "uid-1");
    }

    /// A stopped asset is not Ready, and does not claim an exit code PID 1
    /// never reported — a fabricated 0 would read as a clean exit.
    #[test]
    fn a_stopped_asset_is_not_ready_and_invents_no_exit_code() {
        let a = Asset { name: "registry".into(), running: false, restarts: 9, age_secs: 1 };
        let p = mirror_pod(&a, "n1", "uid-1", "2026-08-29T00:00:00Z");
        assert_eq!(p["status"]["phase"], "Failed");
        assert_eq!(p["status"]["containerStatuses"][0]["ready"], false);
        let term = &p["status"]["containerStatuses"][0]["state"]["terminated"];
        assert!(term.is_object());
        assert!(term.get("exitCode").is_none(), "must not invent an exit code");
        for c in p["status"]["conditions"].as_array().unwrap() {
            if c["type"] == "Ready" {
                assert_eq!(c["status"], "False");
            }
        }
    }
}
