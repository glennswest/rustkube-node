//! `short` (< 2 min, #61): the kubelet is up on the node and does its main
//! job. The gate every OS release passes on every test machine, so it needs
//! nothing a machine may lack: no storage class, no hardware, only this image.
//!
//! - `node-ready`: the Node is Ready, its heartbeat is recent, its kubelet
//!   says its version, and no pressure condition is True.
//! - `pod-runs`: a pod on the node runs to Succeeded with exit code 0 and a
//!   pod IP, and its log reads back through the apiserver (which asks this
//!   kubelet).
//! - `pod-exit-code`: a container that exits 3 leaves its pod Failed with
//!   exit code 3 (the kubelet reports what happened, not just that it ended).
//! - `pod-delete`: a running pod, deleted, is gone well within its grace
//!   period (the kubelet stops it and confirms).
//!
//! The pods are pinned with `spec.nodeName`, and run in parallel.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::api::{now_secs, rfc3339_secs, s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

/// Kept for cleanup and the summary.
const MARGIN: Duration = Duration::from_secs(15);
/// How old the Node's last heartbeat may be. The kubelet reports every 10 s
/// by default; this leaves room for clock skew between the test pod and the
/// apiserver.
const HEARTBEAT: i64 = 120;

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    r.run("node-ready", node_ready(&env, &api)).await;
    let spawn = |name: &'static str, f: fn(Arc<Env>, Api) -> std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + Send>>| {
        let (env, api) = (env.clone(), api.clone());
        tokio::spawn(async move {
            let t = Instant::now();
            let o = f(env, api).await;
            (name, o, t.elapsed().as_millis())
        })
    };
    let tasks = vec![
        spawn("pod-runs", |e, a| Box::pin(async move { pod_runs(&e, &a).await })),
        spawn("pod-exit-code", |e, a| Box::pin(async move { pod_exit_code(&e, &a).await })),
        spawn("pod-delete", |e, a| Box::pin(async move { pod_delete(&e, &a).await })),
    ];
    for t in tasks {
        match t.await {
            Ok((name, o, ms)) => {
                r.record(name, o, ms, None);
            }
            Err(e) => {
                r.record("short-task", Outcome::Infra(format!("a case panicked: {e}")), 0, None);
            }
        }
    }
    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(5)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

/// What is wrong with a Node for running pods, or `None`.
pub fn node_problem(node: &Value, now: i64) -> Option<String> {
    let conds = node["status"]["conditions"].as_array().cloned().unwrap_or_default();
    let cond = |t: &str| conds.iter().find(|c| c["type"] == t);
    let Some(ready) = cond("Ready") else { return Some("the Node has no Ready condition".into()) };
    if ready["status"] != "True" {
        return Some(format!("Ready is {}: {} {}", ready["status"], s(ready, "/reason"), s(ready, "/message")));
    }
    match rfc3339_secs(s(ready, "/lastHeartbeatTime")) {
        Some(t) if now - t <= HEARTBEAT => {}
        Some(t) => return Some(format!("the last heartbeat is {} s old", now - t)),
        None => return Some("Ready has no lastHeartbeatTime".into()),
    }
    for p in ["MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"] {
        if let Some(c) = cond(p).filter(|c| c["status"] == "True") {
            return Some(format!("{p} is True: {}", s(c, "/message")));
        }
    }
    if s(node, "/status/nodeInfo/kubeletVersion").is_empty() {
        return Some("status.nodeInfo.kubeletVersion is empty".into());
    }
    None
}

async fn node_ready(env: &Env, api: &Api) -> Outcome {
    let node = match api.get(&format!("/api/v1/nodes/{}", env.node_name)).await {
        Ok(Some(n)) => n,
        Ok(None) => return Outcome::Fail(format!("Node {} is gone", env.node_name)),
        Err(e) => return Outcome::Infra(e),
    };
    match node_problem(&node, now_secs()) {
        Some(why) => Outcome::Fail(format!("Node {}: {why}", env.node_name)),
        None => Outcome::Pass(format!(
            "Node {} Ready, kubelet {}",
            env.node_name,
            s(&node, "/status/nodeInfo/kubeletVersion")
        )),
    }
}

fn within(env: &Env) -> Duration {
    env.budget(Duration::from_secs(90), MARGIN)
}

async fn pod_runs(env: &Env, api: &Api) -> Outcome {
    let token = format!("rustkube-node-short-{}", env.run_id);
    let name = "short-runs";
    if let Err(e) = api.create(&k8s::pods(env), &k8s::pod(env, name, &["echo", &token])).await {
        return Outcome::Infra(e);
    }
    let done = match k8s::pod_done(env, api, name, within(env)).await {
        Ok(d) => d,
        Err(e) => return Outcome::Fail(e),
    };
    let p = match api.get(&format!("{}/{name}", k8s::pods(env))).await {
        Ok(Some(p)) => p,
        Ok(None) => return Outcome::Fail(format!("pod {name} vanished once done")),
        Err(e) => return Outcome::Infra(e),
    };
    if done.phase != "Succeeded" {
        return Outcome::Fail(format!("pod ended {}: {}", done.phase, done.message));
    }
    let code = p.pointer("/status/containerStatuses/0/state/terminated/exitCode").and_then(Value::as_i64);
    if code != Some(0) {
        return Outcome::Fail(format!("Succeeded, but the container's exit code is {code:?}"));
    }
    let ip = s(&p, "/status/podIP").to_string();
    if ip.is_empty() {
        return Outcome::Fail("Succeeded with no status.podIP".into());
    }
    match k8s::log(env, api, name, false).await {
        Ok(l) if l.contains(&token) => Outcome::Pass(format!("Succeeded on {}, pod IP {ip}, log reads back", done.node)),
        Ok(l) => Outcome::Fail(format!("the log does not hold {token:?}: {}", crate::api::short(&Value::String(l)))),
        Err(e) => Outcome::Fail(format!("the log could not be read: {e}")),
    }
}

async fn pod_exit_code(env: &Env, api: &Api) -> Outcome {
    let name = "short-exit-code";
    if let Err(e) = api.create(&k8s::pods(env), &k8s::pod(env, name, &["exit", "3"])).await {
        return Outcome::Infra(e);
    }
    let done = match k8s::pod_done(env, api, name, within(env)).await {
        Ok(d) => d,
        Err(e) => return Outcome::Fail(e),
    };
    let p = match api.get(&format!("{}/{name}", k8s::pods(env))).await {
        Ok(Some(p)) => p,
        Ok(None) => return Outcome::Fail(format!("pod {name} vanished once done")),
        Err(e) => return Outcome::Infra(e),
    };
    let t = p.pointer("/status/containerStatuses/0/state/terminated").cloned().unwrap_or(Value::Null);
    let code = t["exitCode"].as_i64();
    if done.phase == "Failed" && code == Some(3) {
        Outcome::Pass(format!("Failed, exit code 3, reason {:?}", s(&t, "/reason")))
    } else {
        Outcome::Fail(format!("phase {}, exit code {code:?}, wanted Failed and 3", done.phase))
    }
}

async fn pod_delete(env: &Env, api: &Api) -> Outcome {
    let name = "short-delete";
    let path = format!("{}/{name}", k8s::pods(env));
    if let Err(e) = api.create(&k8s::pods(env), &k8s::pod(env, name, &["sleep", "600"])).await {
        return Outcome::Infra(e);
    }
    if let Err(e) = k8s::pod_running(env, api, name, within(env)).await {
        return Outcome::Fail(e);
    }
    let t = Instant::now();
    if let Err(e) = api.delete(&path).await {
        return Outcome::Infra(e);
    }
    // The grace period is 10 s and the workload exits on SIGTERM at once.
    match k8s::gone(api, &path, env.budget(Duration::from_secs(30), MARGIN)).await {
        Ok(()) => Outcome::Pass(format!("Running, deleted, gone in {} ms", t.elapsed().as_millis())),
        Err(e) => Outcome::Fail(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(ready: &str, beat: &str, extra: Value) -> Value {
        let mut conds = vec![json!({"type": "Ready", "status": ready, "lastHeartbeatTime": beat})];
        if let Value::Array(e) = extra {
            conds.extend(e);
        }
        json!({"status": {"conditions": conds, "nodeInfo": {"kubeletVersion": "v1.32.0"}}})
    }

    #[test]
    fn a_ready_node_with_a_recent_heartbeat_has_no_problem() {
        let now = rfc3339_secs("2026-10-06T12:00:00Z").unwrap();
        let fresh = node("True", "2026-10-06T11:59:50Z", json!([{"type": "DiskPressure", "status": "False"}]));
        assert_eq!(node_problem(&fresh, now), None);
        let stale = node("True", "2026-10-06T11:50:00Z", json!([]));
        assert!(node_problem(&stale, now).unwrap().contains("600 s old"));
        let not_ready = node("False", "2026-10-06T11:59:50Z", json!([]));
        assert!(node_problem(&not_ready, now).unwrap().starts_with("Ready is"));
        let full = node("True", "2026-10-06T11:59:50Z", json!([{"type": "DiskPressure", "status": "True", "message": "m"}]));
        assert_eq!(node_problem(&full, now).unwrap(), "DiskPressure is True: m");
        assert!(node_problem(&json!({}), now).is_some());
    }
}
