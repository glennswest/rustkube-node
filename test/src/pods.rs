//! `medium`'s pod cases (#61): what the kubelet does for a pod beyond
//! running it, and how it fails, end to end through the API. They need only
//! this image, so they run with or without the built-in storage class.
//!
//! - `pod-restart-on-failure`: `restartPolicy: OnFailure`, a container that
//!   fails once (its marker in an emptyDir, which outlives the container)
//!   and then succeeds: the pod Succeeds with restartCount 1, and
//!   `log?previous=true` is the failed run's output.
//! - `pod-init-first`: an init container writes a file into an emptyDir, the
//!   main container finds it: init runs first, to completion, sharing volumes.
//! - `pod-config`: a ConfigMap as a volume and as `configMapKeyRef`, and the
//!   downward API (`metadata.name`, `spec.nodeName`) as env: each seen as set.
//! - `pod-missing-image`: an image no registry has waits as ErrImagePull or
//!   ImagePullBackOff with a message, and never runs.
//! - `pod-carried-image` (#332, #294): a pod of a golden the release carries
//!   with nothing mounted at boot (`/etc/stormblock/carried`) runs to Succeeded
//!   on its own `ctr-…` root. A release with no list, or one that mounts the
//!   image anyway, cannot show that, so the case skips and says which.
//! - `pod-image-on-no-list` (#332): a bare image name on neither list gets the
//!   registry's 404 and never runs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::Outcome;

const MARGIN: Duration = Duration::from_secs(120);

/// Each case, run in parallel by `medium::run`: (test name, outcome, ms).
pub async fn run(env: Arc<Env>, api: Api) -> Vec<(String, Outcome, u128)> {
    type Case = fn(Arc<Env>, Api) -> std::pin::Pin<Box<dyn std::future::Future<Output = Outcome> + Send>>;
    let cases: [(&str, Case); 6] = [
        ("pod-restart-on-failure", |e, a| Box::pin(async move { restart_on_failure(&e, &a).await })),
        ("pod-init-first", |e, a| Box::pin(async move { init_first(&e, &a).await })),
        ("pod-config", |e, a| Box::pin(async move { config(&e, &a).await })),
        ("pod-missing-image", |e, a| Box::pin(async move { missing_image(&e, &a).await })),
        ("pod-carried-image", |e, a| Box::pin(async move { carried_image(&e, &a).await })),
        ("pod-image-on-no-list", |e, a| Box::pin(async move { image_on_no_list(&e, &a).await })),
    ];
    let tasks: Vec<_> = cases
        .into_iter()
        .map(|(name, f)| {
            let (env, api) = (env.clone(), api.clone());
            tokio::spawn(async move {
                let t = Instant::now();
                let o = f(env, api).await;
                (name.to_string(), o, t.elapsed().as_millis())
            })
        })
        .collect();
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await.unwrap_or_else(|e| ("pod-case".into(), Outcome::Infra(format!("panicked: {e}")), 0)));
    }
    out
}

fn within(env: &Env) -> Duration {
    env.budget(Duration::from_secs(300), MARGIN)
}

/// Create `pod`, wait for it to finish, and return it as it ended.
async fn finished(env: &Env, api: &Api, pod: &Value) -> Result<Value, Outcome> {
    let name = s(pod, "/metadata/name").to_string();
    api.create(&k8s::pods(env), pod).await.map_err(Outcome::Infra)?;
    k8s::pod_done(env, api, &name, within(env)).await.map_err(Outcome::Fail)?;
    api.get(&format!("{}/{name}", k8s::pods(env)))
        .await
        .map_err(Outcome::Infra)?
        .ok_or_else(|| Outcome::Fail(format!("pod {name} vanished once done")))
}

fn scratch() -> Value {
    json!([{ "name": "scratch", "emptyDir": {} }])
}

/// The pod for `pod-restart-on-failure`.
pub fn restart_pod(env: &Env) -> Value {
    let mut p = k8s::pod(env, "medium-restart", &["fail-once", "/scratch"]);
    p["spec"]["restartPolicy"] = json!("OnFailure");
    p["spec"]["volumes"] = scratch();
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "scratch", "mountPath": "/scratch" }]);
    p
}

async fn restart_on_failure(env: &Env, api: &Api) -> Outcome {
    let p = match finished(env, api, &restart_pod(env)).await {
        Ok(p) => p,
        Err(o) => return o,
    };
    let phase = s(&p, "/status/phase");
    let restarts = p.pointer("/status/containerStatuses/0/restartCount").and_then(Value::as_i64);
    if phase != "Succeeded" || restarts != Some(1) {
        return Outcome::Fail(format!("phase {phase}, restartCount {restarts:?}; wanted Succeeded after 1 restart"));
    }
    match k8s::log(env, api, "medium-restart", true).await {
        Ok(l) if l.contains("first run") => Outcome::Pass("Succeeded after 1 restart; the previous log is the failed run".into()),
        Ok(l) => Outcome::Fail(format!("the previous log is not the failed run: {}", crate::api::short(&Value::String(l)))),
        Err(e) => Outcome::Fail(format!("the previous log could not be read: {e}")),
    }
}

/// The pod for `pod-init-first`.
pub fn init_pod(env: &Env) -> Value {
    let mut p = k8s::pod(env, "medium-init", &["expect-file", "/scratch/from-init", "written by init"]);
    let mut init = k8s::container(env, "init", &["write-file", "/scratch/from-init", "written by init"]);
    init["volumeMounts"] = json!([{ "name": "scratch", "mountPath": "/scratch" }]);
    p["spec"]["initContainers"] = json!([init]);
    p["spec"]["volumes"] = scratch();
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "scratch", "mountPath": "/scratch" }]);
    p
}

async fn init_first(env: &Env, api: &Api) -> Outcome {
    match finished(env, api, &init_pod(env)).await {
        Ok(p) if s(&p, "/status/phase") == "Succeeded" => Outcome::Pass("the main container found the init container's file".into()),
        Ok(p) => Outcome::Fail(format!(
            "phase {}; init: {}; main: {}",
            s(&p, "/status/phase"),
            p.pointer("/status/initContainerStatuses/0/state").cloned().unwrap_or(Value::Null),
            p.pointer("/status/containerStatuses/0/state").cloned().unwrap_or(Value::Null)
        )),
        Err(o) => o,
    }
}

/// The ConfigMap and the pod for `pod-config`: a volume checked by an init
/// container, then env from the ConfigMap and the downward API checked by
/// the main containers' successive inits (one check per container: the image
/// has no shell to chain them).
pub fn config_objects(env: &Env) -> (Value, Value) {
    let cm = json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": { "name": "medium-config", "namespace": env.namespace, "labels": k8s::labels(env) },
        "data": { "greeting": "hello from a configmap" },
    });
    let mut p = k8s::pod(env, "medium-config", &["expect-env", "NODE", &env.node_name]);
    let mut vol = k8s::container(env, "volume", &["expect-file", "/config/greeting", "hello from a configmap"]);
    vol["volumeMounts"] = json!([{ "name": "config", "mountPath": "/config" }]);
    let mut key = k8s::container(env, "key", &["expect-env", "GREETING", "hello from a configmap"]);
    key["env"] = json!([{ "name": "GREETING", "valueFrom": { "configMapKeyRef": { "name": "medium-config", "key": "greeting" } } }]);
    let mut me = k8s::container(env, "name", &["expect-env", "POD", "medium-config"]);
    me["env"] = json!([{ "name": "POD", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }]);
    p["spec"]["initContainers"] = json!([vol, key, me]);
    p["spec"]["containers"][0]["env"] = json!([{ "name": "NODE", "valueFrom": { "fieldRef": { "fieldPath": "spec.nodeName" } } }]);
    p["spec"]["volumes"] = json!([{ "name": "config", "configMap": { "name": "medium-config" } }]);
    (cm, p)
}

async fn config(env: &Env, api: &Api) -> Outcome {
    let (cm, p) = config_objects(env);
    if let Err(e) = api.create(&k8s::configmaps(env), &cm).await {
        return Outcome::Infra(e);
    }
    match finished(env, api, &p).await {
        Ok(p) if s(&p, "/status/phase") == "Succeeded" => {
            Outcome::Pass("configMap volume, configMapKeyRef and fieldRef env all seen".into())
        }
        Ok(p) => {
            // Which check failed: the first init container not terminated 0.
            let failed = p["status"]["initContainerStatuses"]
                .as_array()
                .and_then(|a| a.iter().find(|c| c.pointer("/state/terminated/exitCode").and_then(Value::as_i64) != Some(0)))
                .map(|c| s(c, "/name").to_string())
                .unwrap_or_else(|| "main".into());
            Outcome::Fail(format!("phase {}; the {failed} check failed", s(&p, "/status/phase")))
        }
        Err(o) => o,
    }
}

/// An image name no registry serves: `.invalid` never resolves (RFC 2606).
pub const MISSING_IMAGE: &str = "registry.invalid/rustkube-node-test/missing:1";

async fn missing_image(env: &Env, api: &Api) -> Outcome {
    waits_for_image(env, api, "medium-missing-image", MISSING_IMAGE).await
}

/// The carried image the case runs, and a program in it that exits 0.
const CARRIED_IMAGE: &str = "nextnfs";
const CARRIED_COMMAND: &[&str] = &["/usr/sbin/nextnfs", "--help"];
/// A bare name, as a pallet is named, that no list holds and no registry has.
const NO_LIST_IMAGE: &str = "rknt-on-no-list";

/// The engine volume `list` (the grammar of `/etc/stormblock/mounts`, one
/// `<volume>:/p/<image>` per line) names for `image`.
pub fn listed_volume<'a>(list: &'a str, image: &str) -> Option<&'a str> {
    list.lines().find_map(|l| {
        let (vol, path) = l.trim().split_once(':')?;
        (path == format!("/p/{image}")).then_some(vol)
    })
}

/// A file of the node's `/etc/stormblock`, read by a pod of this image.
async fn node_stormblock_file(env: &Env, api: &Api, pod: &str, file: &str) -> Result<String, Outcome> {
    let path = format!("/stormblock/{file}");
    let mut p = k8s::pod(env, pod, &["read-file", &path]);
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "etc", "mountPath": "/stormblock", "readOnly": true }]);
    p["spec"]["volumes"] = json!([{ "name": "etc", "hostPath": { "path": "/etc/stormblock" } }]);
    let done = finished(env, api, &p).await?;
    let log = k8s::log(env, api, pod, false).await.map_err(Outcome::Infra)?;
    if s(&done, "/status/phase") != "Succeeded" {
        return Err(Outcome::Infra(format!("could not read the node's {path}: {log}")));
    }
    let line: Value = log
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str(l).ok())
        .ok_or_else(|| Outcome::Infra(format!("no result line reading {path}: {log}")))?;
    Ok(s(&line, "/detail").to_string())
}

async fn carried_image(env: &Env, api: &Api) -> Outcome {
    let carried = match node_stormblock_file(env, api, "medium-read-carried", "carried").await {
        Ok(t) => t,
        Err(o) => return o,
    };
    let Some(volume) = listed_volume(&carried, CARRIED_IMAGE) else {
        return Outcome::Skip(format!("this release does not list {CARRIED_IMAGE} in /etc/stormblock/carried"));
    };
    let mounts = match node_stormblock_file(env, api, "medium-read-mounts", "mounts").await {
        Ok(t) => t,
        Err(o) => return o,
    };
    if listed_volume(&mounts, CARRIED_IMAGE).is_some() {
        return Outcome::Skip(format!("this release also mounts {CARRIED_IMAGE} at boot, so it is no test of the list"));
    }
    let name = "medium-carried";
    let mut p = k8s::pod(env, name, &[]);
    let c = &mut p["spec"]["containers"][0];
    c["image"] = json!(CARRIED_IMAGE);
    c["command"] = json!(CARRIED_COMMAND);
    c.as_object_mut().map(|o| o.remove("args"));
    let done = match finished(env, api, &p).await {
        Ok(d) => d,
        Err(o) => return o,
    };
    if s(&done, "/status/phase") != "Succeeded" {
        return Outcome::Fail(format!(
            "phase {}: {}{}",
            s(&done, "/status/phase"),
            s(&done, "/status/message"),
            k8s::waiting(&done)
        ));
    }
    // Written soon after the start, off the pod's worker: it may trail the end.
    let path = format!("{}/{name}", k8s::pods(env));
    let root = k8s::until(Duration::from_secs(30), || {
        let path = path.clone();
        async move {
            let p = api.get(&path).await?.ok_or("the pod is gone")?;
            let root = s(&p, "/metadata/annotations/storm.io~1instance.main").to_string();
            if root.is_empty() {
                return Err("no storm.io/instance.main annotation".to_string());
            }
            Ok((root, s(&p, "/metadata/annotations/storm.io~1golden.main").to_string()))
        }
    })
    .await;
    match root {
        Ok((root, _)) if !root.starts_with("ctr-") => Outcome::Fail(format!("its root is {root:?}, not a ctr-… clone")),
        Ok((root, golden)) => Outcome::Pass(format!(
            "{CARRIED_IMAGE} (volume {volume}, listed, not mounted at boot) Succeeded on root {root}; golden {golden:?}"
        )),
        Err(e) => Outcome::Fail(format!("Succeeded, but its root is not known: {e}")),
    }
}

async fn image_on_no_list(env: &Env, api: &Api) -> Outcome {
    match waits_for_image(env, api, "medium-no-list", NO_LIST_IMAGE).await {
        Outcome::Pass(d) if !d.contains("404") => Outcome::Fail(format!("not the registry's 404: {d}")),
        other => other,
    }
}

/// A pod of `image` waits as ErrImagePull or ImagePullBackOff and never runs.
async fn waits_for_image(env: &Env, api: &Api, name: &str, image: &str) -> Outcome {
    let mut p = k8s::pod(env, name, &["echo", "never"]);
    p["spec"]["containers"][0]["image"] = json!(image);
    if let Err(e) = api.create(&k8s::pods(env), &p).await {
        return Outcome::Infra(e);
    }
    let path = format!("{}/{name}", k8s::pods(env));
    let seen = k8s::until(env.budget(Duration::from_secs(120), MARGIN), || {
        let path = path.clone();
        async move {
            let p = api.get(&path).await?.ok_or("the pod is gone")?;
            let phase = s(&p, "/status/phase");
            if phase != "Pending" {
                return Ok(Err(format!("phase {phase}: a pod with no image left Pending")));
            }
            let reason = s(&p, "/status/containerStatuses/0/state/waiting/reason");
            if matches!(reason, "ErrImagePull" | "ImagePullBackOff") {
                return Ok(Ok(format!("waiting {reason}: {}", s(&p, "/status/containerStatuses/0/state/waiting/message"))));
            }
            Err(format!("phase {phase}{}", k8s::waiting(&p)))
        }
    })
    .await;
    match seen {
        Ok(Ok(d)) => Outcome::Pass(d),
        Ok(Err(e)) => Outcome::Fail(e),
        Err(e) => Outcome::Fail(format!("never ErrImagePull or ImagePullBackOff: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::tests::env;

    #[test]
    fn a_listed_image_names_its_volume() {
        let list = "cilium-operator:/p/operator-generic\nnextnfs:/p/nextnfs\n";
        assert_eq!(listed_volume(list, "nextnfs"), Some("nextnfs"));
        assert_eq!(listed_volume(list, "operator-generic"), Some("cilium-operator"));
        assert_eq!(listed_volume(list, "next"), None);
        assert_eq!(listed_volume("(absent)", "nextnfs"), None);
    }

    #[test]
    fn the_restart_pod_keeps_its_marker_across_restarts() {
        let p = restart_pod(&env());
        assert_eq!(p["spec"]["restartPolicy"], "OnFailure");
        assert_eq!(p["spec"]["nodeName"], "node1");
        assert_eq!(p["spec"]["containers"][0]["args"], json!(["fail-once", "/scratch"]));
        assert_eq!(p["spec"]["volumes"][0]["emptyDir"], json!({}));
    }

    #[test]
    fn the_init_container_writes_what_the_main_one_reads() {
        let p = init_pod(&env());
        let init = &p["spec"]["initContainers"][0];
        assert_eq!(init["args"][0], "write-file");
        assert_eq!(init["args"][1], p["spec"]["containers"][0]["args"][1]);
        assert_eq!(init["args"][2], p["spec"]["containers"][0]["args"][2]);
    }

    #[test]
    fn the_config_pod_checks_each_source_once() {
        let (cm, p) = config_objects(&env());
        assert_eq!(cm["metadata"]["labels"]["storm.io/test-run"], "r1");
        let inits: Vec<&str> = p["spec"]["initContainers"].as_array().unwrap().iter().map(|c| c["name"].as_str().unwrap()).collect();
        assert_eq!(inits, ["volume", "key", "name"]);
        assert_eq!(p["spec"]["containers"][0]["args"], json!(["expect-env", "NODE", "node1"]));
    }
}
