//! The `realtime` suite (#430, stormpump#252): a real-time pod on stormpump,
//! read back from the node rather than from what was asked.
//!
//! - `realtime-node`: the Node says what it can give: the label
//!   `stormpump.io/realtime` (`hard`, `soft` or `none`) and
//!   `kernel.stormpump.io/preempt`, and, with isolated cores, allocatable
//!   `stormpump.io/isolated-cores` matching the label.
//! - `realtime-soft-fifo`: a pod with RuntimeClass `realtime-soft`, two
//!   isolated cores and `policy=fifo,priority=80` runs; from inside it the
//!   kernel says `SCHED_FIFO` priority 80, its allowed CPUs are exactly two
//!   whole cores, and its allowed memory nodes are the ones those cores are
//!   on (`cpuset.mems` follows `cpuset.cpus`).
//! - `realtime-hard-refused`: on a node whose kernel is not `PREEMPT_RT`, a
//!   `realtime-hard` pod is not run; its `RealtimeRefused` Event says why.
//!
//! The pod cases need a node with an isolated set and the RuntimeClasses
//! (stormcos#1477); without them they are skipped and say what is missing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

pub const RESOURCE: &str = "stormpump.io/isolated-cores";
pub const ANNOTATION: &str = "stormpump.io/realtime";
pub const LABEL: &str = "stormpump.io/realtime";
pub const PREEMPT_LABEL: &str = "kernel.stormpump.io/preempt";

/// What the `rt-view` workload printed, as fields.
pub fn view_fields(detail: &str) -> std::collections::BTreeMap<String, String> {
    detail
        .split_whitespace()
        .filter_map(|w| w.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Whether `mems` (a list) names exactly the NUMA nodes in `nodes`
/// (comma-separated, `-` for none known).
pub fn mems_follow(mems: &str, nodes: &str) -> bool {
    if nodes == "-" {
        return !mems.is_empty();
    }
    let a = crate::workload::expand_cpu_list(mems);
    let b: Vec<u32> = nodes.split(',').filter_map(|n| n.parse().ok()).collect();
    a == b
}

/// A pod of this image reporting what the kernel gives it, with RuntimeClass
/// `class`, `cores` isolated cores and the annotation `shape`.
pub fn rt_pod(env: &Env, name: &str, class: &str, cores: u32, shape: &str) -> Value {
    let mut p = k8s::pod(env, name, &["rt-view"]);
    p["metadata"]["annotations"] = json!({ ANNOTATION: shape });
    p["spec"]["runtimeClassName"] = json!(class);
    let n = cores.to_string();
    let mut res = json!({ "requests": {}, "limits": {} });
    res["requests"][RESOURCE] = json!(n);
    res["limits"][RESOURCE] = json!(n);
    p["spec"]["containers"][0]["resources"] = res;
    p
}

async fn runtime_class(api: &Api, name: &str) -> Result<bool, String> {
    Ok(api.get(&format!("/apis/node.k8s.io/v1/runtimeclasses/{name}")).await?.is_some())
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let t = Instant::now();
    let node = match api.get(&format!("/api/v1/nodes/{}", env.node_name)).await {
        Ok(Some(n)) => n,
        Ok(None) => return r.record("realtime-node", Outcome::Infra(format!("Node {} not found", env.node_name)), 0, None),
        Err(e) => return r.record("realtime-node", Outcome::Infra(e), 0, None),
    };
    let label = s(&node, &format!("/metadata/labels/{}", LABEL.replace('/', "~1"))).to_string();
    let preempt = s(&node, &format!("/metadata/labels/{}", PREEMPT_LABEL.replace('/', "~1"))).to_string();
    let isolated: u32 = s(&node, &format!("/status/allocatable/{}", RESOURCE.replace('/', "~1"))).parse().unwrap_or(0);
    if label.is_empty() || preempt.is_empty() {
        let why = format!("Node {} has no {LABEL}/{PREEMPT_LABEL} labels: its kubelet predates #430", env.node_name);
        r.record("realtime-node", Outcome::Skip(why.clone()), 0, None);
        r.record("realtime-soft-fifo", Outcome::Skip(why.clone()), 0, None);
        r.record("realtime-hard-refused", Outcome::Skip(why), 0, None);
        return;
    }
    let consistent = match label.as_str() {
        "none" => isolated == 0,
        "soft" => isolated > 0 && preempt != "rt",
        "hard" => isolated > 0 && preempt == "rt",
        _ => false,
    };
    let facts = format!("{LABEL}={label} {PREEMPT_LABEL}={preempt} allocatable {RESOURCE}={isolated}");
    r.record(
        "realtime-node",
        if consistent { Outcome::Pass(facts) } else { Outcome::Fail(format!("the labels and allocatable disagree: {facts}")) },
        t.elapsed().as_millis(),
        None,
    );

    // A soft FIFO pod on two isolated cores.
    let t = Instant::now();
    let soft = if isolated < 2 {
        Outcome::Skip(format!("Node {} has {isolated} isolated cores; the case needs 2 (stormcos#1477)", env.node_name))
    } else {
        match runtime_class(&api, "realtime-soft").await {
            Err(e) => Outcome::Infra(e),
            Ok(false) => Outcome::Skip("no RuntimeClass realtime-soft on the cluster (stormcos#1477)".into()),
            Ok(true) => soft_fifo(&env, &api).await,
        }
    };
    r.record("realtime-soft-fifo", soft, t.elapsed().as_millis(), None);

    // realtime-hard on a kernel that is not PREEMPT_RT.
    let t = Instant::now();
    let hard = if preempt == "rt" {
        Outcome::Skip(format!("Node {} runs a PREEMPT_RT kernel: realtime-hard is admitted there", env.node_name))
    } else if isolated < 1 {
        Outcome::Skip(format!("Node {} has no isolated cores (stormcos#1477)", env.node_name))
    } else {
        match runtime_class(&api, "realtime-hard").await {
            Err(e) => Outcome::Infra(e),
            Ok(false) => Outcome::Skip("no RuntimeClass realtime-hard on the cluster (stormcos#1477)".into()),
            Ok(true) => hard_refused(&env, &api).await,
        }
    };
    r.record("realtime-hard-refused", hard, t.elapsed().as_millis(), None);

    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(30)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

async fn soft_fifo(env: &Env, api: &Api) -> Outcome {
    let name = "rt-soft";
    if let Err(e) = api.create(&k8s::pods(env), &rt_pod(env, name, "realtime-soft", 2, "policy=fifo,priority=80")).await {
        return Outcome::Infra(format!("creating pod {name}: {e}"));
    }
    let done = match k8s::pod_done(env, api, name, Duration::from_secs(120)).await {
        Ok(d) => d,
        Err(e) => {
            let events = k8s::events_for(env, api, "Pod", name).await.unwrap_or_default();
            return Outcome::Fail(format!("{e}; events: {}", events.join(" | ")));
        }
    };
    let said = k8s::said(env, api, name, &done).await;
    if done.phase != "Succeeded" {
        return Outcome::Fail(format!("pod {name} ended {}: {said}", done.phase));
    }
    let detail = serde_json::from_str::<Value>(&said)
        .ok()
        .and_then(|v| v["detail"].as_str().map(str::to_string))
        .unwrap_or(said);
    let f = view_fields(&detail);
    let get = |k: &str| f.get(k).map(String::as_str).unwrap_or("");
    let mut wrong = Vec::new();
    if get("policy") != "1" || get("priority") != "80" {
        wrong.push(format!("scheduled policy {} priority {} (wanted SCHED_FIFO=1, 80)", get("policy"), get("priority")));
    }
    if get("cores") != "2" {
        wrong.push(format!("{} whole cores in cpus {} (wanted 2)", get("cores"), get("cpus")));
    }
    if !mems_follow(get("mems"), get("nodes")) {
        wrong.push(format!("mems {} are not the cores' NUMA nodes {}", get("mems"), get("nodes")));
    }
    if wrong.is_empty() {
        Outcome::Pass(format!("read back from the node: {detail}"))
    } else {
        Outcome::Fail(format!("{} ({detail})", wrong.join("; ")))
    }
}

async fn hard_refused(env: &Env, api: &Api) -> Outcome {
    let name = "rt-hard";
    if let Err(e) = api.create(&k8s::pods(env), &rt_pod(env, name, "realtime-hard", 1, "policy=fifo,priority=80")).await {
        return Outcome::Infra(format!("creating pod {name}: {e}"));
    }
    let seen = k8s::until(Duration::from_secs(90), || async {
        let events = k8s::events_for(env, api, "Pod", name).await?;
        events
            .into_iter()
            .find(|e| e.starts_with("RealtimeRefused:"))
            .ok_or_else(|| format!("no RealtimeRefused Event on pod {name} yet"))
    })
    .await;
    let pod = api.get(&format!("{}/{name}", k8s::pods(env))).await.ok().flatten().unwrap_or(Value::Null);
    let phase = s(&pod, "/status/phase").to_string();
    match seen {
        Err(e) => Outcome::Fail(format!("{e}; the pod is {phase:?}{}", k8s::waiting(&pod))),
        Ok(event) if !event.contains("PREEMPT_RT") => Outcome::Fail(format!("the refusal does not name PREEMPT_RT: {event}")),
        Ok(_) if phase == "Running" || phase == "Succeeded" => Outcome::Fail(format!("refused, and it ran anyway: {phase}")),
        Ok(event) => Outcome::Pass(format!("not run ({phase}); {event}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_view_is_read_and_mems_must_follow_the_cores() {
        let f = view_fields("cpus=4-5,12-13 mems=0 cores=2 nodes=0 policy=1 priority=80");
        assert_eq!((f["cpus"].as_str(), f["cores"].as_str(), f["policy"].as_str()), ("4-5,12-13", "2", "1"));
        assert!(mems_follow("0", "0"));
        assert!(mems_follow("0-1", "0,1"));
        assert!(!mems_follow("0-1", "0"), "memory from a node the cores are not on");
        assert!(mems_follow("0", "-"));
    }

    #[test]
    fn a_real_time_pod_asks_for_its_class_cores_and_shape() {
        let env = crate::k8s::tests::env();
        let p = rt_pod(&env, "rt", "realtime-soft", 2, "policy=fifo,priority=80");
        assert_eq!(p["spec"]["runtimeClassName"], "realtime-soft");
        assert_eq!(p["metadata"]["annotations"][ANNOTATION], "policy=fifo,priority=80");
        let res = &p["spec"]["containers"][0]["resources"];
        assert_eq!((res["requests"][RESOURCE].as_str(), res["limits"][RESOURCE].as_str()), (Some("2"), Some("2")));
        assert_eq!(p["spec"]["containers"][0]["args"][0], "rt-view");
    }
}
