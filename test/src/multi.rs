//! `medium`'s multi-container pod (#374): one pod, `medium-multi`, with an
//! init container, a sidecar (`restartPolicy: Always` init container) and two
//! app containers `a` and `b`, sharing an `emptyDir` (`/scratch`), a claim of
//! the built-in class (`/data`) and the pod's network and IPC. Each container
//! runs the workload `member` (`workload.rs`): it reaches its peers on
//! `127.0.0.1` before it listens, so a container is ready only once the pod's
//! network joined it to the others.
//!
//! | container | listens | reaches | probes, hooks, resources |
//! |---|---|---|---|
//! | `init` | | | writes `/scratch/from-init` |
//! | `sidecar` | 8081 | | startup tcpSocket; preStop writes `/data/prestop-sidecar` |
//! | `a` | 8080 | sidecar | readiness httpGet; postStart (`rkn-test`, by the image's PATH alone, #393) writes `/scratch/poststart-a`, preStop `/data/prestop-a`; 16Mi/64Mi |
//! | `b` | 8082 | a, sidecar | readiness and liveness tcpSocket; 32Mi/96Mi |
//!
//! The cases, read back through the apiserver:
//!
//! - `pod-multi-ready`: Running; Initialized, ContainersReady, Ready; `init`
//!   Completed, the sidecar running and started, `a` and `b` ready, each with
//!   its own resources.
//! - `pod-multi-localhost`: `b`'s log says it reached `a` and the sidecar on
//!   127.0.0.1 (one network namespace).
//! - `pod-multi-logs`: `log?container=` of each says that container's own lines.
//! - `pod-multi-exec`: `exec -c b` finds the init container's file in the
//!   shared emptyDir; `exec -c a` finds its postStart hook's file; `exec -c
//!   sidecar` runs (a sidecar's preStop is an exec in it).
//! - `pod-multi-ipc`: `exec -c b` reads `/dev/shm/multi-a`, written by `a`
//!   (one IPC namespace and `/dev/shm`, as upstream's pod sandbox shares).
//! - `pod-multi-restart`: `a` crashes once (a marker `b` writes): `a`'s
//!   restartCount is 1 with `lastState.terminated` exit 1 and it is ready
//!   again; `b` and the sidecar keep their container ids and count 0.
//! - `pod-multi-stop-order` (needs the built-in class): the pod deleted, a
//!   reader pod mounts the claim: both preStop hooks wrote their files, and the
//!   sidecar's SIGTERM came after `a`'s and `b`'s (sidecars stop last). A
//!   failure names every file read and the pod's PreStop/Killing events.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::Outcome;

pub const POD: &str = "medium-multi";
pub const READER: &str = "medium-multi-read";
pub const CLAIM: &str = "medium-multi-data";
/// The image's own binary, for exec, hooks and probes (`Containerfile`).
pub const BIN: &str = "/test";

fn mounts() -> Value {
    json!([{ "name": "scratch", "mountPath": "/scratch" }, { "name": "data", "mountPath": "/data" }])
}

fn hook(path: &str, text: &str) -> Value {
    json!({ "exec": { "command": [BIN, "write-file", path, text] } })
}

/// The image's copy of this binary that only its own `PATH` reaches
/// (`/opt/rkn/bin`, test/Containerfile), by bare name (#393).
pub const PATH_ONLY_BIN: &str = "rkn-test";

/// [`hook`], calling [`PATH_ONLY_BIN`]: it runs only if exec resolves the
/// command with the image's environment, as cilium's `bash` hook needs.
fn path_hook(path: &str, text: &str) -> Value {
    json!({ "exec": { "command": [PATH_ONLY_BIN, "write-file", path, text] } })
}

/// The pod. `claim`: the data volume is [`CLAIM`] (else an emptyDir, and the
/// stop order cannot be read after the pod).
pub fn pod(env: &Env, claim: bool) -> Value {
    let member = |name: &str, port: &str, peers: &[&str]| {
        let mut args = vec!["member", name, port, "/scratch", "/data"];
        args.extend_from_slice(peers);
        let mut c = k8s::container(env, name, &args);
        c["volumeMounts"] = mounts();
        c
    };
    let mut init = k8s::container(env, "init", &["write-file", "/scratch/from-init", "init ran"]);
    init["volumeMounts"] = mounts();

    let mut sidecar = member("sidecar", "8081", &[]);
    sidecar["restartPolicy"] = json!("Always");
    sidecar["startupProbe"] = json!({ "tcpSocket": { "port": 8081 }, "periodSeconds": 1, "failureThreshold": 120 });
    sidecar["lifecycle"] = json!({ "preStop": hook("/data/prestop-sidecar", "sidecar") });

    let mut a = member("a", "8080", &["8081=sidecar"]);
    a["ports"] = json!([{ "name": "http", "containerPort": 8080 }]);
    a["readinessProbe"] = json!({ "httpGet": { "port": "http", "path": "/" }, "periodSeconds": 2 });
    a["lifecycle"] = json!({
        "postStart": path_hook("/scratch/poststart-a", "a"),
        "preStop": hook("/data/prestop-a", "a"),
    });
    a["resources"] = json!({ "requests": { "cpu": "10m", "memory": "16Mi" }, "limits": { "memory": "64Mi" } });

    let mut b = member("b", "8082", &["8080=a", "8081=sidecar"]);
    b["readinessProbe"] = json!({ "tcpSocket": { "port": 8082 }, "periodSeconds": 2 });
    b["livenessProbe"] = json!({ "tcpSocket": { "port": 8082 }, "initialDelaySeconds": 60, "periodSeconds": 10 });
    b["resources"] = json!({ "requests": { "cpu": "20m", "memory": "32Mi" }, "limits": { "memory": "96Mi" } });

    let mut p = k8s::pod(env, POD, &[]);
    p["spec"]["restartPolicy"] = json!("Always");
    p["spec"]["terminationGracePeriodSeconds"] = json!(20);
    p["spec"]["initContainers"] = json!([init, sidecar]);
    p["spec"]["containers"] = json!([a, b]);
    p["spec"]["volumes"] = json!([
        { "name": "scratch", "emptyDir": {} },
        if claim {
            json!({ "name": "data", "persistentVolumeClaim": { "claimName": CLAIM } })
        } else {
            json!({ "name": "data", "emptyDir": {} })
        },
    ]);
    p
}

/// The reader for `pod-multi-stop-order`: one container per file, each
/// `read-file` of it, so each answer is its own container's log.
pub const READ: &[(&str, &str)] = &[
    ("stop-a", "/data/stop-a"),
    ("stop-b", "/data/stop-b"),
    ("stop-sidecar", "/data/stop-sidecar"),
    ("prestop-a", "/data/prestop-a"),
    ("prestop-sidecar", "/data/prestop-sidecar"),
];

pub fn reader(env: &Env) -> Value {
    let mut p = k8s::pod(env, READER, &[]);
    let cs: Vec<Value> = READ
        .iter()
        .map(|(name, path)| {
            let mut c = k8s::container(env, name, &["read-file", path]);
            c["volumeMounts"] = json!([{ "name": "data", "mountPath": "/data" }]);
            c
        })
        .collect();
    p["spec"]["containers"] = json!(cs);
    p["spec"]["volumes"] = json!([{ "name": "data", "persistentVolumeClaim": { "claimName": CLAIM } }]);
    p
}

fn status_of<'a>(p: &'a Value, list: &str, name: &str) -> Option<&'a Value> {
    p["status"][list].as_array()?.iter().find(|c| c["name"] == name)
}

fn condition(p: &Value, kind: &str) -> String {
    p["status"]["conditions"]
        .as_array()
        .and_then(|cs| cs.iter().find(|c| c["type"] == kind))
        .map(|c| s(c, "/status").to_string())
        .unwrap_or_else(|| "absent".into())
}

/// `pod-multi-ready`'s judgement of the pod as the apiserver has it.
pub fn ready_check(p: &Value) -> Result<String, String> {
    let mut why = Vec::new();
    if s(p, "/status/phase") != "Running" {
        why.push(format!("phase {:?}", s(p, "/status/phase")));
    }
    for kind in ["Initialized", "ContainersReady", "Ready"] {
        let c = condition(p, kind);
        if c != "True" {
            why.push(format!("{kind} {c}"));
        }
    }
    match status_of(p, "initContainerStatuses", "init") {
        Some(c) if s(c, "/state/terminated/reason") == "Completed" => {}
        c => why.push(format!("init is not Completed: {}", c.map_or("absent".into(), |c| c["state"].to_string()))),
    }
    match status_of(p, "initContainerStatuses", "sidecar") {
        Some(c) if c["state"]["running"].is_object() && c["started"] == true => {}
        c => why.push(format!("the sidecar is not running and started: {}", c.map_or("absent".into(), |c| c.to_string()))),
    }
    let names: Vec<&str> = p["status"]["containerStatuses"]
        .as_array()
        .map(|cs| cs.iter().map(|c| s(c, "/name")).collect())
        .unwrap_or_default();
    if names != ["a", "b"] {
        why.push(format!("containerStatuses are {names:?}, not [a, b]"));
    }
    for (name, memory) in [("a", "16Mi"), ("b", "32Mi")] {
        match status_of(p, "containerStatuses", name) {
            Some(c) if c["ready"] == true && c["state"]["running"].is_object() => {
                if s(c, "/resources/requests/memory") != memory {
                    why.push(format!("{name}'s status resources are {}, not its own (requests.memory {memory})", c["resources"]));
                }
            }
            c => why.push(format!("{name} is not running and ready: {}", c.map_or("absent".into(), |c| c["state"].to_string()))),
        }
    }
    if why.is_empty() {
        Ok("Running and Ready; init Completed, the sidecar started, a and b ready with their own resources".into())
    } else {
        Err(why.join("; "))
    }
}

/// A container's id and restart count.
pub fn run_of(p: &Value, list: &str, name: &str) -> (String, i64) {
    status_of(p, list, name)
        .map(|c| (s(c, "/containerID").to_string(), c["restartCount"].as_i64().unwrap_or(0)))
        .unwrap_or_default()
}

/// `pod-multi-restart`'s judgement: `a` restarted once from exit 1 and is
/// ready; `b` and the sidecar are the runs they were (`before`).
pub fn restart_check(p: &Value, before_b: &(String, i64), before_sidecar: &(String, i64)) -> Result<String, String> {
    let a = status_of(p, "containerStatuses", "a").ok_or("no status for a")?;
    if a["restartCount"].as_i64() != Some(1) || a["ready"] != true {
        return Err(format!("a: restartCount {}, ready {}", a["restartCount"], a["ready"]));
    }
    if a["lastState"]["terminated"]["exitCode"].as_i64() != Some(1) {
        return Err(format!("a's lastState is not its exit 1: {}", a["lastState"]));
    }
    let (b, sidecar) = (run_of(p, "containerStatuses", "b"), run_of(p, "initContainerStatuses", "sidecar"));
    if &b != before_b {
        return Err(format!("b changed with a's restart: {before_b:?} -> {b:?}"));
    }
    if &sidecar != before_sidecar {
        return Err(format!("the sidecar changed with a's restart: {before_sidecar:?} -> {sidecar:?}"));
    }
    Ok("a restarted alone (restartCount 1, lastState exit 1, ready again); b and the sidecar untouched".into())
}

/// The `detail` of a workload's JSON line in `log`.
pub fn detail(log: &str) -> Option<String> {
    log.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .find_map(|v| v["detail"].as_str().map(str::to_string))
}

/// `pod-multi-stop-order`'s judgement from what the reader read: each
/// container's file by name.
pub fn stop_order_check(read: &[(String, String)]) -> Result<String, String> {
    let get = |n: &str| read.iter().find(|(k, _)| k == n).map(|(_, v)| v.as_str()).unwrap_or("(absent)");
    let time = |n: &str| get(n).trim().parse::<u128>().map_err(|_| format!("{n}: {:?}", get(n)));
    if get("prestop-a") != "a" || get("prestop-sidecar") != "sidecar" {
        return Err(format!("preStop hooks: a {:?}, sidecar {:?}", get("prestop-a"), get("prestop-sidecar")));
    }
    let (a, b, sc) = (time("stop-a")?, time("stop-b")?, time("stop-sidecar")?);
    if sc < a.max(b) {
        return Err(format!("the sidecar was stopped before the app containers: a {a}, b {b}, sidecar {sc} (ns)"));
    }
    Ok(format!("both preStop hooks ran; the sidecar stopped {} ms after the last app container", (sc - a.max(b)) / 1_000_000))
}

fn within(env: &Env) -> Duration {
    env.budget(Duration::from_secs(300), Duration::from_secs(120))
}

/// A container's log, through the apiserver.
pub async fn log(env: &Env, api: &Api, pod: &str, container: &str, previous: bool) -> Result<String, String> {
    let path = format!("{}/{pod}/log?container={container}{}", k8s::pods(env), if previous { "&previous=true" } else { "" });
    match api.get(&path).await? {
        Some(Value::String(t)) => Ok(t),
        Some(v) => Ok(v.to_string()),
        None => Err(format!("{path}: not found")),
    }
}

async fn exec(env: &Env, api: &Api, container: &str, args: &[&str]) -> Result<crate::api::Exec, String> {
    let mut cmd = vec![BIN];
    cmd.extend_from_slice(args);
    api.exec(&env.namespace, POD, container, &cmd, Duration::from_secs(30)).await
}

/// Every case, as (name, outcome, ms). One pod; the cases run in order on it.
pub async fn run(env: &Env, api: &Api) -> Vec<(String, Outcome, u128)> {
    fn put(out: &mut Vec<(String, Outcome, u128)>, name: &str, o: Outcome, t: &mut Instant) {
        out.push((name.to_string(), o, t.elapsed().as_millis()));
        *t = Instant::now();
    }
    let mut out = Vec::new();
    let mut t = Instant::now();
    let claim = match k8s::no_builtin_class(api).await {
        Ok(None) => true,
        Ok(Some(_)) => false,
        Err(e) => {
            put(&mut out, "pod-multi-ready", Outcome::Infra(e), &mut t);
            return out;
        }
    };
    if claim {
        if let Err(e) = api.create(&k8s::pvcs(env), &k8s::claim(env, CLAIM, "1Mi")).await {
            put(&mut out, "pod-multi-ready", Outcome::Infra(e), &mut t);
            return out;
        }
    }
    if let Err(e) = api.create(&k8s::pods(env), &pod(env, claim)).await {
        put(&mut out, "pod-multi-ready", Outcome::Infra(e), &mut t);
        return out;
    }
    let path = format!("{}/{POD}", k8s::pods(env));
    let ready = k8s::until(within(env), || {
        let path = path.clone();
        async move {
            let p = api.get(&path).await?.ok_or("the pod is gone")?;
            ready_check(&p).map(|d| (d, p))
        }
    })
    .await;
    let p = match ready {
        Ok((d, p)) => {
            put(&mut out, "pod-multi-ready", Outcome::Pass(d), &mut t);
            p
        }
        Err(e) => {
            put(&mut out, "pod-multi-ready", Outcome::Fail(e), &mut t);
            // Running all the same: the other cases still say what they can.
            match api.get(&path).await {
                Ok(Some(p)) if s(&p, "/status/phase") == "Running" => p,
                _ => return out,
            }
        }
    };

    let o = match log(env, api, POD, "b", false).await {
        Ok(l) if l.contains("b: reached a on 127.0.0.1:8080") && l.contains("b: reached sidecar on 127.0.0.1:8081") => {
            Outcome::Pass("b reached a and the sidecar on 127.0.0.1: one network namespace".into())
        }
        Ok(l) => Outcome::Fail(format!("b's log does not say it reached a and the sidecar on 127.0.0.1: {}", crate::api::short(&Value::String(l)))),
        Err(e) => Outcome::Fail(e),
    };
    put(&mut out, "pod-multi-localhost", o, &mut t);

    let mut bad = Vec::new();
    for (c, want) in [("init", "init ran"), ("sidecar", "sidecar: serving on 8081"), ("a", "a: serving on 8080"), ("b", "b: serving on 8082")] {
        match log(env, api, POD, c, false).await {
            Ok(l) if l.contains(want) => {}
            Ok(l) => bad.push(format!("{c}: {:?} not in {}", want, crate::api::short(&Value::String(l)))),
            Err(e) => bad.push(format!("{c}: {e}")),
        }
    }
    let o = if bad.is_empty() { Outcome::Pass("each container's log is its own".into()) } else { Outcome::Fail(bad.join("; ")) };
    put(&mut out, "pod-multi-logs", o, &mut t);

    let o = match (
        exec(env, api, "b", &["expect-file", "/scratch/from-init", "init ran"]).await,
        exec(env, api, "a", &["read-file", "/scratch/poststart-a"]).await,
        exec(env, api, "sidecar", &["expect-file", "/scratch/from-init", "init ran"]).await,
    ) {
        (Ok(init), Ok(hook), Ok(sc)) if init.ok() && detail(&hook.stdout).as_deref() == Some("a") && sc.ok() => Outcome::Pass(
            "exec -c b found init's file in the shared emptyDir; exec -c a found its postStart hook's file; exec -c sidecar ran".into(),
        ),
        (Ok(init), Ok(hook), Ok(sc)) => Outcome::Fail(format!(
            "exec -c b expect-file: status {} stdout {}; exec -c a read-file poststart-a: {}; exec -c sidecar expect-file: status {} stdout {}",
            init.status, init.stdout.trim(), hook.stdout.trim(), sc.status, sc.stdout.trim()
        )),
        (Err(e), _, _) | (_, Err(e), _) => Outcome::Fail(e),
        (_, _, Err(e)) => Outcome::Fail(format!("exec -c sidecar: {e}")),
    };
    put(&mut out, "pod-multi-exec", o, &mut t);

    let o = match exec(env, api, "b", &["read-file", "/dev/shm/multi-a"]).await {
        Ok(r) if detail(&r.stdout).as_deref() == Some("a") => Outcome::Pass("b reads a's /dev/shm: one IPC namespace".into()),
        Ok(r) => Outcome::Fail(format!(
            "b does not see a's /dev/shm/multi-a ({}): the pod's containers do not share IPC and /dev/shm",
            detail(&r.stdout).unwrap_or_else(|| r.stdout.trim().to_string())
        )),
        Err(e) => Outcome::Fail(e),
    };
    put(&mut out, "pod-multi-ipc", o, &mut t);

    let (before_b, before_sidecar) = (run_of(&p, "containerStatuses", "b"), run_of(&p, "initContainerStatuses", "sidecar"));
    let o = match exec(env, api, "b", &["write-file", "/scratch/crash-a", "now"]).await {
        Err(e) => Outcome::Fail(format!("the crash marker: {e}")),
        Ok(_) => {
            let path = path.clone();
            let (bb, bs) = (&before_b, &before_sidecar);
            match k8s::until(within(env), || {
                let path = path.clone();
                async move {
                    let p = api.get(&path).await?.ok_or("the pod is gone")?;
                    restart_check(&p, bb, bs)
                }
            })
            .await
            {
                Ok(d) => match log(env, api, POD, "a", true).await {
                    Ok(l) if l.contains("a: crashing as asked") => Outcome::Pass(format!("{d}; log -c a --previous is the crashed run")),
                    Ok(l) => Outcome::Fail(format!("{d}; but a's previous log is not the crashed run: {}", crate::api::short(&Value::String(l)))),
                    Err(e) => Outcome::Fail(format!("{d}; a's previous log: {e}")),
                },
                Err(e) => Outcome::Fail(e),
            }
        }
    };
    put(&mut out, "pod-multi-restart", o, &mut t);

    let o = if !claim {
        let _ = api.delete(&path).await;
        Outcome::Skip("no built-in class: the stop times cannot outlive the pod".into())
    } else {
        stop_order(env, api, &path).await
    };
    put(&mut out, "pod-multi-stop-order", o, &mut t);
    out
}

async fn stop_order(env: &Env, api: &Api, path: &str) -> Outcome {
    if let Err(e) = api.delete(path).await {
        return Outcome::Infra(e);
    }
    if let Err(e) = k8s::gone(api, path, within(env)).await {
        return Outcome::Fail(format!("the pod was not gone: {e}"));
    }
    if let Err(e) = api.create(&k8s::pods(env), &reader(env)).await {
        return Outcome::Infra(e);
    }
    match k8s::pod_done(env, api, READER, within(env)).await {
        Ok(d) if d.phase == "Succeeded" => {}
        Ok(d) => return Outcome::Fail(format!("the reader ended {}: {}", d.phase, d.message)),
        Err(e) => return Outcome::Fail(e),
    }
    let mut read = Vec::new();
    for (name, _) in READ {
        match log(env, api, READER, name, false).await {
            Ok(l) => read.push((name.to_string(), detail(&l).unwrap_or_default())),
            Err(e) => return Outcome::Fail(format!("reader -c {name}: {e}")),
        }
    }
    match stop_order_check(&read) {
        Ok(d) => Outcome::Pass(d),
        Err(e) => {
            // What would say why: every file the reader read, and the
            // kubelet's hook and stop events for the pod.
            let files: Vec<String> = read.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
            let events = match k8s::events_for(env, api, "Pod", POD).await {
                Ok(ev) => ev.into_iter().filter(|e| e.contains("PreStop") || e.starts_with("Killing")).collect::<Vec<_>>().join(" | "),
                Err(e) => format!("(events: {e})"),
            };
            Outcome::Fail(format!("{e}; read: {}; events: {}", files.join(", "), if events.is_empty() { "none" } else { &events }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(name: &str, ready: bool, mem: &str, id: &str, restarts: i64) -> Value {
        json!({"name": name, "ready": ready, "state": {"running": {}}, "containerID": id, "restartCount": restarts,
               "resources": {"requests": {"memory": mem}}})
    }

    fn good() -> Value {
        json!({"status": {
            "phase": "Running",
            "conditions": [{"type": "Initialized", "status": "True"}, {"type": "ContainersReady", "status": "True"}, {"type": "Ready", "status": "True"}],
            "initContainerStatuses": [
                {"name": "init", "state": {"terminated": {"reason": "Completed", "exitCode": 0}}},
                {"name": "sidecar", "state": {"running": {}}, "started": true, "containerID": "s1", "restartCount": 0},
            ],
            "containerStatuses": [running("a", true, "16Mi", "a1", 0), running("b", true, "32Mi", "b1", 0)],
        }})
    }

    #[test]
    fn the_ready_check_wants_every_container() {
        assert!(ready_check(&good()).is_ok());
        let mut p = good();
        p["status"]["containerStatuses"][1]["ready"] = json!(false);
        assert!(ready_check(&p).unwrap_err().contains("b is not running and ready"));
        let mut p = good();
        p["status"]["initContainerStatuses"][1]["started"] = json!(false);
        assert!(ready_check(&p).unwrap_err().contains("sidecar"));
        let mut p = good();
        p["status"]["containerStatuses"][0]["resources"] = json!({});
        assert!(ready_check(&p).unwrap_err().contains("a's status resources"));
    }

    #[test]
    fn a_restart_of_a_leaves_the_others() {
        let (b, sc) = (("b1".to_string(), 0), ("s1".to_string(), 0));
        let mut p = good();
        p["status"]["containerStatuses"][0] = running("a", true, "16Mi", "a2", 1);
        p["status"]["containerStatuses"][0]["lastState"] = json!({"terminated": {"exitCode": 1}});
        assert!(restart_check(&p, &b, &sc).is_ok());
        let mut q = p.clone();
        q["status"]["containerStatuses"][1]["containerID"] = json!("b2");
        assert!(restart_check(&q, &b, &sc).unwrap_err().contains("b changed"));
        assert!(restart_check(&good(), &b, &sc).is_err(), "no restart yet");
    }

    #[test]
    fn the_sidecar_stops_last() {
        let read = |sc: &str| -> Vec<(String, String)> {
            [("stop-a", "100"), ("stop-b", "200"), ("stop-sidecar", sc), ("prestop-a", "a"), ("prestop-sidecar", "sidecar")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(stop_order_check(&read("5000300")).is_ok());
        assert!(stop_order_check(&read("150")).unwrap_err().contains("before the app containers"));
        assert!(stop_order_check(&read("(absent)")).is_err());
        assert_eq!(detail("x\n{\"workload\": \"read-file\", \"ok\": true, \"detail\": \"a\"}\n").as_deref(), Some("a"));
    }

    #[test]
    fn the_pod_declares_what_the_cases_read() {
        let env = crate::k8s::tests::env();
        let p = pod(&env, true);
        assert_eq!(p["spec"]["initContainers"][1]["restartPolicy"], "Always");
        assert_eq!(p["spec"]["containers"][1]["args"], json!(["member", "b", "8082", "/scratch", "/data", "8080=a", "8081=sidecar"]));
        assert_eq!(p["spec"]["volumes"][1]["persistentVolumeClaim"]["claimName"], CLAIM);
        assert_eq!(pod(&env, false)["spec"]["volumes"][1]["emptyDir"], json!({}));
        assert_eq!(reader(&env)["spec"]["containers"].as_array().map(Vec::len), Some(READ.len()));
        // #393: a's postStart names its command bare, for the image's PATH to find.
        let post = &p["spec"]["containers"][0]["lifecycle"]["postStart"]["exec"]["command"];
        assert_eq!(post[0], PATH_ONLY_BIN);
        assert!(!PATH_ONLY_BIN.contains('/'));
    }
}
