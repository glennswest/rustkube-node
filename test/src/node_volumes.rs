//! `medium`: the node's own volumes as PV + PVC sets (#59), checked through
//! the API on the test node.
//!
//! 1. **Complete pairs.** Every claim the kubelet mirrors for `STORM_NODE`
//!    (label `storm.io/system-volume`, annotation `storm.io/node`) is named
//!    `<volume>-<node>` in `kube-system`, Bound to PV `storm-<volume>-<node>`,
//!    and that PV's `claimRef` names it by namespace, name and uid. Both carry
//!    the same `storm.io/volume-kind` and `storm.io/component`, and every such
//!    PV of the node has its claim. There is at least one (every stormcos node
//!    has `fastetcd-data`).
//! 2. **Restored.** One claim (a `logs` one when there is one) is deleted;
//!    the kubelet makes it again, and its PV names the new claim's uid.
//!
//! Pairs under the old unqualified names (before #107) are not this check's:
//! they are left as they were (no migration) and are skipped here.

use std::time::{Duration, Instant};

use serde_json::Value;

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::Outcome;

pub const NAMESPACE: &str = "kube-system";
const LABEL: &str = "storm.io/system-volume";

fn ann<'a>(o: &'a Value, k: &str) -> &'a str {
    o["metadata"]["annotations"][k].as_str().unwrap_or("")
}
fn label<'a>(o: &'a Value, k: &str) -> &'a str {
    o["metadata"]["labels"][k].as_str().unwrap_or("")
}

/// This node's node-qualified objects: claims named `<volume>-<node>`, PVs
/// named `storm-<volume>-<node>`.
fn ours<'a>(objs: &'a [Value], node: &str, pv: bool) -> Vec<&'a Value> {
    objs.iter()
        .filter(|o| ann(o, "storm.io/node") == node && label(o, LABEL) == "true")
        .filter(|o| {
            let v = ann(o, "storm.io/volume");
            let want = if pv { format!("storm-{v}-{node}") } else { format!("{v}-{node}") };
            !v.is_empty() && s(o, "/metadata/name") == want
        })
        .collect()
}

/// What is wrong with this node's pairs, or nothing. `pvcs` are
/// `kube-system`'s claims, `pvs` the cluster's volumes.
pub fn problems(node: &str, pvcs: &[Value], pvs: &[Value]) -> Vec<String> {
    let claims = ours(pvcs, node, false);
    let volumes = ours(pvs, node, true);
    let mut wrong = Vec::new();
    if claims.is_empty() {
        wrong.push(format!("no node volume of {node} is represented as a claim"));
    }
    for c in &claims {
        let name = s(c, "/metadata/name");
        let volume = ann(c, "storm.io/volume");
        let pv_name = format!("storm-{volume}-{node}");
        if s(c, "/spec/volumeName") != pv_name {
            wrong.push(format!("claim {name} names PV {:?}, not {pv_name}", s(c, "/spec/volumeName")));
        }
        if s(c, "/status/phase") != "Bound" {
            wrong.push(format!("claim {name} is {:?}, not Bound", s(c, "/status/phase")));
        }
        let Some(pv) = volumes.iter().find(|p| s(p, "/metadata/name") == pv_name) else {
            wrong.push(format!("claim {name} has no PV {pv_name}"));
            continue;
        };
        let r = &pv["spec"]["claimRef"];
        if r["namespace"].as_str() != Some(NAMESPACE) || r["name"].as_str() != Some(name) {
            wrong.push(format!("PV {pv_name} claimRef names {}/{}", s(r, "/namespace"), s(r, "/name")));
        }
        if r["uid"] != c["metadata"]["uid"] {
            wrong.push(format!("PV {pv_name} claimRef uid {} is not claim {name}'s {}", r["uid"], c["metadata"]["uid"]));
        }
        for k in ["storm.io/volume-kind", "storm.io/component"] {
            if label(c, k).is_empty() || label(c, k) != label(pv, k) {
                wrong.push(format!("{k}: claim {name} {:?}, PV {pv_name} {:?}", label(c, k), label(pv, k)));
            }
        }
    }
    for pv in &volumes {
        let want = s(&pv["spec"]["claimRef"], "/name");
        if !claims.iter().any(|c| s(c, "/metadata/name") == want) {
            wrong.push(format!("PV {} has no claim {NAMESPACE}/{want}", s(pv, "/metadata/name")));
        }
    }
    wrong
}

async fn lists(api: &Api) -> Result<(Vec<Value>, Vec<Value>), String> {
    let pvcs = api
        .list(&format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims"))
        .await?
        .ok_or("kube-system claims are not served")?;
    let pvs = api.list(k8s::PVS).await?.ok_or("PVs are not served")?;
    Ok((pvcs, pvs))
}

/// Check 1: the node's pairs are complete.
pub async fn pairs(env: &Env, api: &Api) -> Outcome {
    let (pvcs, pvs) = match lists(api).await {
        Ok(l) => l,
        Err(e) => return Outcome::Infra(e),
    };
    let wrong = problems(&env.node, &pvcs, &pvs);
    let n = ours(&pvcs, &env.node, false).len();
    if wrong.is_empty() {
        Outcome::Pass(format!("{n} node volumes of {} are complete PV + PVC pairs", env.node))
    } else {
        Outcome::Fail(wrong.join("; "))
    }
}

/// Check 2: a deleted claim is made again and its PV names the new uid.
pub async fn restored(env: &Env, api: &Api, within: Duration) -> Outcome {
    let (pvcs, _) = match lists(api).await {
        Ok(l) => l,
        Err(e) => return Outcome::Infra(e),
    };
    let claims = ours(&pvcs, &env.node, false);
    let Some(c) = claims
        .iter()
        .find(|c| label(c, "storm.io/volume-kind") == "logs")
        .or_else(|| claims.first())
    else {
        return Outcome::Fail(format!("no node volume of {} to delete", env.node));
    };
    let (name, old_uid) = (s(c, "/metadata/name").to_string(), c["metadata"]["uid"].clone());
    let pv_name = s(c, "/spec/volumeName").to_string();
    let path = format!("/api/v1/namespaces/{NAMESPACE}/persistentvolumeclaims/{name}");
    if let Err(e) = api.delete(&path).await {
        return Outcome::Infra(e);
    }
    let t = Instant::now();
    let (path, pv_path) = (path.as_str(), format!("{}/{pv_name}", k8s::PVS));
    let pv_path = pv_path.as_str();
    let old = &old_uid;
    let back = k8s::until(within, || async move {
        let Some(c) = api.get(path).await? else { return Err("not made again yet".to_string()) };
        if c["metadata"]["uid"] == *old || !c["metadata"]["deletionTimestamp"].is_null() {
            return Err("the old claim is still going".to_string());
        }
        let pv = api.get(pv_path).await?.ok_or("its PV is gone")?;
        if pv["spec"]["claimRef"]["uid"] != c["metadata"]["uid"] {
            return Err(format!("PV {pv_path} still names uid {}", pv["spec"]["claimRef"]["uid"]));
        }
        Ok(Ok(()))
    })
    .await;
    match back {
        Ok(Ok(())) => Outcome::Pass(format!(
            "claim {NAMESPACE}/{name} deleted and made again in {} ms; PV {pv_name} names the new uid",
            t.elapsed().as_millis()
        )),
        Ok(Err(e)) => Outcome::Fail(e),
        Err(e) => Outcome::Fail(format!("claim {NAMESPACE}/{name} was not restored: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pair(volume: &str, node: &str, kind: &str) -> (Value, Value) {
        let labels = json!({ LABEL: "true", "storm.io/volume-kind": kind, "storm.io/component": "fastetcd" });
        let ann = json!({ "storm.io/node": node, "storm.io/volume": volume });
        let pvc = json!({
            "metadata": { "name": format!("{volume}-{node}"), "namespace": NAMESPACE, "uid": format!("u-{volume}"),
                          "labels": labels, "annotations": ann },
            "spec": { "volumeName": format!("storm-{volume}-{node}") },
            "status": { "phase": "Bound" },
        });
        let pv = json!({
            "metadata": { "name": format!("storm-{volume}-{node}"), "labels": labels, "annotations": ann },
            "spec": { "claimRef": { "namespace": NAMESPACE, "name": format!("{volume}-{node}"),
                                    "uid": format!("u-{volume}") } },
        });
        (pvc, pv)
    }

    #[test]
    fn complete_pairs_have_no_problems_and_other_nodes_and_old_names_are_not_ours() {
        let (c1, v1) = pair("fastetcd-data", "n1", "data");
        let (c2, v2) = pair("fastetcd-logs", "n1", "logs");
        let (c3, v3) = pair("fastetcd-data", "n2", "data");
        // An old unqualified pair (before #107).
        let (mut c4, mut v4) = pair("fastetcd-data", "n1", "data");
        c4["metadata"]["name"] = json!("fastetcd-data");
        v4["metadata"]["name"] = json!("storm-fastetcd-data");
        v4["spec"]["claimRef"]["uid"] = json!("stale");
        let pvcs = vec![c1, c2, c3, c4];
        let pvs = vec![v1, v2, v3, v4];
        assert_eq!(problems("n1", &pvcs, &pvs), Vec::<String>::new());
        assert_eq!(ours(&pvcs, "n1", false).len(), 2);
    }

    #[test]
    fn a_broken_pair_says_what_is_wrong() {
        let (mut c, mut v) = pair("fastetcd-data", "n1", "data");
        v["spec"]["claimRef"]["uid"] = json!("other");
        v["metadata"]["labels"]["storm.io/volume-kind"] = json!("logs");
        c["status"]["phase"] = json!("Pending");
        let (_, orphan) = pair("stormcert-data", "n1", "data");
        let p = problems("n1", &[c], &[v, orphan]).join("; ");
        assert!(p.contains("uid"), "{p}");
        assert!(p.contains("storm.io/volume-kind"), "{p}");
        assert!(p.contains("not Bound"), "{p}");
        assert!(p.contains("PV storm-stormcert-data-n1 has no claim"), "{p}");
        assert!(problems("n1", &[], &[]).join("").contains("no node volume"));
    }
}
