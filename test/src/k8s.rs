//! The objects the suites make, and the waits and checks they share. Every
//! namespaced object goes in the run's namespace; every object carries
//! `storm.io/test-run`.

use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::{Env, CLASS, PROVISIONER};

pub const MIB: u64 = 1 << 20;

/// Where a workload pod sees its claim.
pub const FS_PATH: &str = "/data";
/// Where a workload pod sees a raw block claim (`volumeDevices`, #67).
pub const DEV_PATH: &str = "/dev/xvda";

pub fn pvcs(env: &Env) -> String {
    format!("/api/v1/namespaces/{}/persistentvolumeclaims", env.namespace)
}
pub fn pods(env: &Env) -> String {
    format!("/api/v1/namespaces/{}/pods", env.namespace)
}
pub fn events(env: &Env) -> String {
    format!("/api/v1/namespaces/{}/events", env.namespace)
}
pub const PVS: &str = "/api/v1/persistentvolumes";

fn labels(env: &Env) -> Value {
    json!({ "storm.io/test-run": env.run_id, "storm.io/test-of": "rustkube-node" })
}

/// A claim of the built-in class asking for `request`, verbatim (a quantity
/// string, so `3.5Gi` and `1500M` reach the kubelet as written).
pub fn claim(env: &Env, name: &str, request: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": { "name": name, "namespace": env.namespace, "labels": labels(env) },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": CLASS,
            "resources": { "requests": { "storage": request } },
        },
    })
}

/// A claim like [`claim`] of `volumeMode: Block`: a raw device, no filesystem.
pub fn block_claim(env: &Env, name: &str, request: &str) -> Value {
    let mut c = claim(env, name, request);
    c["spec"]["volumeMode"] = json!("Block");
    c
}

/// [`work`], with the claim a raw block device at [`DEV_PATH`]
/// (`volumeDevices`) and that path given to the workload.
pub fn work_device(env: &Env, name: &str, claim: &str, mode: &str, args: &[String]) -> Value {
    let mut p = work(env, name, claim, mode, args);
    let c = &mut p["spec"]["containers"][0];
    c["args"][1] = json!(DEV_PATH);
    c.as_object_mut().unwrap().remove("volumeMounts");
    c["volumeDevices"] = json!([{ "name": "claim", "devicePath": DEV_PATH }]);
    p
}

/// A pod running this image's workload `args` (after the mode, the claim's
/// mount path is inserted) with claim `claim` at [`FS_PATH`].
pub fn work(env: &Env, name: &str, claim: &str, mode: &str, args: &[String]) -> Value {
    let mut a = vec![mode.to_string(), FS_PATH.to_string()];
    a.extend(args.iter().cloned());
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": { "name": name, "namespace": env.namespace, "labels": labels(env) },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": false,
            "containers": [{
                "name": "work",
                "image": env.image,
                "imagePullPolicy": "IfNotPresent",
                "args": a,
                // root owns a fresh filesystem's top directory; no capability
                // is needed beyond that ownership.
                "securityContext": { "runAsUser": 0, "allowPrivilegeEscalation": false, "capabilities": { "drop": ["ALL"] } },
                "volumeMounts": [{ "name": "claim", "mountPath": FS_PATH }],
            }],
            "volumes": [{ "name": "claim", "persistentVolumeClaim": { "claimName": claim } }],
        },
    })
}

/// Whether the built-in class is here. `Ok(None)` when it is; `Ok(Some(why))`
/// when it is not (the suite then reports one skip); `Err` when the API could
/// not be asked.
pub async fn no_builtin_class(api: &Api) -> Result<Option<String>, String> {
    match api.get(&format!("/apis/storage.k8s.io/v1/storageclasses/{CLASS}")).await? {
        None => Ok(Some(format!("no StorageClass {CLASS}: the node's built-in claims are not offered here"))),
        Some(c) if s(&c, "/provisioner") != PROVISIONER => Ok(Some(format!(
            "StorageClass {CLASS} has provisioner {:?}, not {PROVISIONER}",
            s(&c, "/provisioner")
        ))),
        Some(_) => Ok(None),
    }
}

/// How a workload pod ended.
pub struct Done {
    pub phase: String,
    pub node: String,
    pub message: String,
}

/// Wait for pod `name` to reach Succeeded or Failed.
pub async fn pod_done(env: &Env, api: &Api, name: &str, within: Duration) -> Result<Done, String> {
    let path = format!("{}/{name}", pods(env));
    let end = Instant::now() + within;
    let mut last = String::from("never seen");
    loop {
        if let Some(p) = api.get(&path).await? {
            let phase = s(&p, "/status/phase").to_string();
            let message = p
                .pointer("/status/containerStatuses/0/state/terminated/message")
                .and_then(Value::as_str)
                .unwrap_or_else(|| s(&p, "/status/message"))
                .to_string();
            if phase == "Succeeded" || phase == "Failed" {
                return Ok(Done { phase, node: s(&p, "/spec/nodeName").to_string(), message });
            }
            last = format!("phase {phase:?} on {:?}{}", s(&p, "/spec/nodeName"), waiting(&p));
        }
        if Instant::now() >= end {
            return Err(format!("pod {name} did not finish within {} s ({last})", within.as_secs()));
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// `, waiting: <reason>: <message>` for a pod whose first container waits.
pub fn waiting(p: &Value) -> String {
    let reason = s(p, "/status/containerStatuses/0/state/waiting/reason");
    if reason.is_empty() {
        return String::new();
    }
    let msg = s(p, "/status/containerStatuses/0/state/waiting/message");
    format!(", waiting: {reason}{}", if msg.is_empty() { String::new() } else { format!(": {msg}") })
}

/// The messages of the events about `kind`/`name` in the run's namespace.
pub async fn events_for(env: &Env, api: &Api, kind: &str, name: &str) -> Result<Vec<String>, String> {
    let items = api.list(&events(env)).await?.unwrap_or_default();
    Ok(items
        .iter()
        .filter(|e| s(e, "/involvedObject/kind") == kind && s(e, "/involvedObject/name") == name)
        .map(|e| format!("{}: {}", s(e, "/reason"), s(e, "/message")))
        .collect())
}

/// Poll `check` every 3 s until it returns Ok, or `within` passes (then the
/// last error).
pub async fn until<T, F, Fut>(within: Duration, mut check: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let end = Instant::now() + within;
    loop {
        let r = check().await;
        if r.is_ok() || Instant::now() >= end {
            return r;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Wait until `path` no longer exists.
pub async fn gone(api: &Api, path: &str, within: Duration) -> Result<(), String> {
    until(within, || async move {
        match api.get(path).await? {
            None => Ok(()),
            Some(_) => Err(format!("{path} still exists after {} s", within.as_secs())),
        }
    })
    .await
}

/// Delete every pod and claim this run made in its namespace (best effort:
/// the runner deletes the namespace after, and this is what keeps a failed
/// case from leaving a clone behind it). Returns the PVs still present after
/// `within`.
pub async fn drain(env: &Env, api: &Api, within: Duration) -> Result<Vec<String>, String> {
    let sel = format!("?labelSelector=storm.io/test-run%3D{}", env.run_id);
    let claims = api.list(&format!("{}{sel}", pvcs(env))).await?.unwrap_or_default();
    let mut left: Vec<String> = claims.iter().map(|c| s(c, "/spec/volumeName").to_string()).filter(|v| !v.is_empty()).collect();
    for p in api.list(&format!("{}{sel}", pods(env))).await?.unwrap_or_default() {
        api.delete(&format!("{}/{}", pods(env), s(&p, "/metadata/name"))).await?;
    }
    for c in &claims {
        api.delete(&format!("{}/{}", pvcs(env), s(c, "/metadata/name"))).await?;
    }
    let end = Instant::now() + within;
    while !left.is_empty() && Instant::now() < end {
        let mut still = Vec::new();
        for pv in &left {
            if api.get(&format!("{PVS}/{pv}")).await?.is_some() {
                still.push(pv.clone());
            }
        }
        left = still;
        if !left.is_empty() {
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
    Ok(left)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn env() -> Env {
        Env {
            suite: "medium".into(),
            run_id: "r1".into(),
            namespace: "test-ns".into(),
            api: "https://127.0.0.1:6443".into(),
            node: String::new(),
            image: "img:1".into(),
            mint_budget: Duration::from_secs(1),
            token: None,
            ca: None,
            timeout: Duration::from_secs(1),
            started: Instant::now(),
        }
    }

    #[test]
    fn claims_ask_verbatim_in_the_builtin_class() {
        let b = claim(&env(), "c", "3.5Gi");
        assert_eq!(b["metadata"]["namespace"], "test-ns");
        assert_eq!(b["metadata"]["labels"]["storm.io/test-run"], "r1");
        assert_eq!(b["spec"]["storageClassName"], "stormblock");
        assert_eq!(b["spec"]["resources"]["requests"]["storage"], "3.5Gi");
    }

    #[test]
    fn workload_pods_use_this_image_and_mount_the_claim() {
        let b = work(&env(), "p", "c", "sized", &["1".into(), "2".into()]);
        assert_eq!(b["spec"]["containers"][0]["image"], "img:1");
        assert_eq!(b["spec"]["containers"][0]["args"], json!(["sized", FS_PATH, "1", "2"]));
        assert_eq!(b["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"], "c");
    }

    #[test]
    fn a_waiting_container_says_why() {
        let p = json!({"status": {"containerStatuses": [{"state": {"waiting": {"reason": "ContainerCreating", "message": "m"}}}]}});
        assert_eq!(waiting(&p), ", waiting: ContainerCreating: m");
        assert_eq!(waiting(&json!({})), "");
    }
}
