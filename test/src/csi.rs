//! The `csi` suite (#52, master's option A): the kubelet's CSI node side
//! against a real driver process on the node, this image's `csi-driver` mode
//! (`csi_driver.rs`). The PV is made by hand (no external-provisioner), so the
//! subject is the node half alone: registration, NodePublish, the mount
//! reaching the pod through mount propagation, a kubelet restart, and
//! NodeUnpublish.
//!
//! Objects: a CSIDriver named for the run (`attachRequired: false`), a
//! privileged driver pod pinned to the node with `/var/lib/kubelet` as a
//! Bidirectional hostPath, a Retain PV of that driver pre-bound to a claim in
//! the run's namespace, and consumer pods of this image. Cluster-scoped ones
//! carry the run label (cleanup deletes what is left; `test/requires.toml`
//! `[csi]` declares the writes).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

pub const CSIDRIVERS: &str = "/apis/storage.k8s.io/v1/csidrivers";
pub const CSINODES: &str = "/apis/storage.k8s.io/v1/csinodes";

const DRIVER_POD: &str = "csi-driver";
const CLAIM: &str = "csi-claim";
/// Where the consumers see the claim, and the file they write and read.
const FILE: &str = "/data/rknt-csi";

/// The run's driver name: a DNS subdomain, unique to the run so a leftover
/// from an earlier run is never mistaken for this one.
pub fn driver_name(run_id: &str) -> String {
    let id: String = run_id.to_ascii_lowercase().chars().filter(|c| c.is_ascii_alphanumeric()).take(24).collect();
    format!("rknt-{id}.csi.storm.io")
}

fn pv_name(env: &Env) -> String {
    format!("rknt-csi-{}", env.run_id.to_ascii_lowercase())
}
fn handle(env: &Env) -> String {
    format!("vol-{}", env.run_id.to_ascii_lowercase())
}

pub fn csidriver(env: &Env, driver: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
        "metadata": { "name": driver, "labels": k8s::labels(env) },
        "spec": {
            "attachRequired": false,
            "podInfoOnMount": false,
            "volumeLifecycleModes": ["Persistent"],
        },
    })
}

/// A Retain PV of the run's driver, pre-bound to [`CLAIM`].
pub fn pv(env: &Env, driver: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": { "name": pv_name(env), "labels": k8s::labels(env) },
        "spec": {
            "capacity": { "storage": "16Mi" },
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "",
            "volumeMode": "Filesystem",
            "claimRef": { "apiVersion": "v1", "kind": "PersistentVolumeClaim", "namespace": env.namespace, "name": CLAIM },
            "csi": { "driver": driver, "volumeHandle": handle(env) },
        },
    })
}

pub fn claim(env: &Env) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": { "name": CLAIM, "namespace": env.namespace, "labels": k8s::labels(env) },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "",
            "volumeName": pv_name(env),
            "resources": { "requests": { "storage": "16Mi" } },
        },
    })
}

/// The driver's pod: privileged, `/var/lib/kubelet` shared both ways, as an
/// upstream driver's DaemonSet has it (docs/csi.md).
pub fn driver_pod(env: &Env, driver: &str) -> Value {
    let mut p = k8s::pod(env, DRIVER_POD, &["csi-driver", driver, &env.node_name]);
    p["spec"]["restartPolicy"] = json!("Always");
    let c = &mut p["spec"]["containers"][0];
    c["name"] = json!("driver");
    c["securityContext"] = json!({ "privileged": true, "runAsUser": 0 });
    c["volumeMounts"] = json!([{ "name": "kubelet-dir", "mountPath": "/var/lib/kubelet", "mountPropagation": "Bidirectional" }]);
    p["spec"]["volumes"] = json!([{ "name": "kubelet-dir", "hostPath": { "path": "/var/lib/kubelet", "type": "Directory" } }]);
    p
}

/// A pod of this image with the claim at `/data`, running `args`.
pub fn consumer(env: &Env, name: &str, args: &[&str]) -> Value {
    let mut p = k8s::pod(env, name, args);
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "claim", "mountPath": "/data" }]);
    p["spec"]["volumes"] = json!([{ "name": "claim", "persistentVolumeClaim": { "claimName": CLAIM } }]);
    p
}

/// The pod that restarts the kubelet: privileged, in the host's PID namespace.
pub fn restart_pod(env: &Env) -> Value {
    let mut p = k8s::pod(env, "csi-restart-kubelet", &["restart-kubelet"]);
    p["spec"]["hostPID"] = json!(true);
    p["spec"]["containers"][0]["securityContext"] = json!({ "privileged": true, "runAsUser": 0 });
    p
}

/// Whether the driver's log has a `call` line for a target of pod `uid`.
pub fn called(log: &str, call: &str, uid: &str) -> bool {
    log.lines().any(|l| {
        l.contains(&format!("csi-driver: {call} "))
            && l.contains(&format!("/pods/{uid}/"))
            && !l.contains(" failed: ")
    })
}

/// The driver as CSINode lists it on this node, if it does.
pub fn csinode_driver<'a>(csinode: &'a Value, driver: &str) -> Option<&'a Value> {
    csinode["spec"]["drivers"].as_array()?.iter().find(|d| d["name"] == driver)
}

struct Suite {
    env: Arc<Env>,
    api: Api,
    driver: String,
    token: String,
}

impl Suite {
    async fn driver_log(&self) -> Result<String, String> {
        k8s::log(&self.env, &self.api, DRIVER_POD, false).await
    }

    /// Wait for the driver's log to show `call` for pod `uid`.
    async fn wait_call(&self, call: &str, uid: &str, within: Duration) -> Result<(), String> {
        k8s::until(within, || async move {
            let log = self.driver_log().await?;
            if called(&log, call, uid) {
                Ok(())
            } else {
                let tail: Vec<&str> = log.lines().rev().take(3).collect();
                Err(format!("no {call} for pod {uid} in the driver's log within {} s (last: {tail:?})", within.as_secs()))
            }
        })
        .await
    }

    async fn create_pod(&self, body: &Value) -> Result<String, String> {
        let p = self.api.create(&k8s::pods(&self.env), body).await?;
        Ok(s(&p, "/metadata/uid").to_string())
    }

    /// Run a consumer to completion; Ok when it Succeeded.
    async fn run_consumer(&self, name: &str, args: &[&str], within: Duration) -> Result<String, String> {
        let uid = self.create_pod(&consumer(&self.env, name, args)).await?;
        let d = k8s::pod_done(&self.env, &self.api, name, within).await?;
        if d.phase != "Succeeded" {
            return Err(format!("pod {name} {}: {}", d.phase, d.message));
        }
        Ok(uid)
    }

    async fn delete_pod(&self, name: &str, within: Duration) -> Result<(), String> {
        let path = format!("{}/{name}", k8s::pods(&self.env));
        self.api.delete(&path).await?;
        k8s::gone(&self.api, &path, within).await
    }

    async fn registered(&self, within: Duration) -> Result<String, String> {
        let path = format!("{CSINODES}/{}", self.env.node_name);
        let driver = self.driver.clone();
        k8s::until(within, || {
            let (path, driver) = (path.clone(), driver.clone());
            async move {
                let n = self.api.get(&path).await?.ok_or_else(|| format!("no CSINode {path}"))?;
                match csinode_driver(&n, &driver) {
                    Some(d) if d["nodeID"] == self.env.node_name.as_str() => Ok(format!("nodeID {}", d["nodeID"])),
                    Some(d) => Err(format!("CSINode lists {driver} with nodeID {}, not {}", d["nodeID"], self.env.node_name)),
                    None => Err(format!("CSINode {} does not list {driver}", self.env.node_name)),
                }
            }
        })
        .await
    }

    async fn setup(&self) -> Result<(), String> {
        self.api.create(CSIDRIVERS, &csidriver(&self.env, &self.driver)).await?;
        self.api.create(k8s::PVS, &pv(&self.env, &self.driver)).await?;
        self.api.create(&k8s::pvcs(&self.env), &claim(&self.env)).await?;
        self.api.create(&k8s::pods(&self.env), &driver_pod(&self.env, &self.driver)).await?;
        Ok(())
    }

    async fn case_registered(&self) -> Outcome {
        let t = Instant::now();
        if let Err(e) = k8s::pod_running(&self.env, &self.api, DRIVER_POD, Duration::from_secs(180)).await {
            return Outcome::Fail(format!("the driver pod never ran: {e}"));
        }
        match self.registered(Duration::from_secs(120)).await {
            Ok(id) => Outcome::Pass(format!("{} registered: CSINode {} lists it ({id}) {} s after its pod was made",
                self.driver, self.env.node_name, t.elapsed().as_secs())),
            Err(e) => Outcome::Fail(format!("{e}; driver log: {}", self.driver_log().await.unwrap_or_else(|e| e))),
        }
    }

    async fn case_bound(&self) -> Outcome {
        let path = format!("{}/{CLAIM}", k8s::pvcs(&self.env));
        let r = k8s::until(Duration::from_secs(60), || {
            let path = path.clone();
            async move {
                let c = self.api.get(&path).await?.ok_or("the claim is gone")?;
                if s(&c, "/status/phase") == "Bound" { Ok(()) } else { Err(format!("claim {CLAIM} is {:?}", s(&c, "/status/phase"))) }
            }
        })
        .await;
        match r {
            Ok(()) => Outcome::Pass(format!("claim {CLAIM} Bound to {}", pv_name(&self.env))),
            Err(e) => Outcome::Fail(e),
        }
    }

    /// A pod writes through the published mount; the driver logs its publish.
    async fn case_publish(&self) -> Result<String, String> {
        let uid = match self.run_consumer("csi-writer", &["write-file", FILE, &self.token], Duration::from_secs(180)).await {
            Ok(u) => u,
            Err(e) => return Err(format!("{e}{}", self.events("csi-writer").await)),
        };
        self.wait_call("NodePublishVolume", &uid, Duration::from_secs(30)).await?;
        Ok(uid)
    }

    async fn events(&self, pod: &str) -> String {
        match k8s::events_for(&self.env, &self.api, "Pod", pod).await {
            Ok(e) if !e.is_empty() => format!("; events: {}", e.join(" | ")),
            _ => String::new(),
        }
    }

    /// The writer deleted: NodeUnpublish; a new pod reads what it wrote, from
    /// the driver's store through a new publish.
    async fn case_unpublish(&self, writer: &str) -> Outcome {
        if let Err(e) = self.delete_pod("csi-writer", Duration::from_secs(60)).await {
            return Outcome::Fail(e);
        }
        if let Err(e) = self.wait_call("NodeUnpublishVolume", writer, Duration::from_secs(60)).await {
            return Outcome::Fail(e);
        }
        match self.run_consumer("csi-reader", &["expect-file", FILE, &self.token], Duration::from_secs(180)).await {
            Ok(_) => Outcome::Pass("NodeUnpublishVolume on the writer's delete; a new pod read its data back".into()),
            Err(e) => Outcome::Fail(format!("after the unpublish: {e}")),
        }
    }

    /// A pod holding the volume across a kubelet restart keeps it; the
    /// restarted kubelet publishes for a new pod and unpublishes the holder.
    async fn case_restart(&self) -> Outcome {
        let pods = k8s::pods(&self.env);
        let holder = match self.create_pod(&consumer(&self.env, "csi-holder", &["sleep", "3600"])).await {
            Ok(u) => u,
            Err(e) => return Outcome::Infra(e),
        };
        let before = match k8s::pod_running(&self.env, &self.api, "csi-holder", Duration::from_secs(180)).await {
            Ok(p) => p,
            Err(e) => return Outcome::Fail(format!("the holder never ran: {e}")),
        };
        let cid = s(&before, "/status/containerStatuses/0/containerID").to_string();
        if let Err(e) = self.create_pod(&restart_pod(&self.env)).await {
            return Outcome::Infra(e);
        }
        let restarted = match k8s::pod_done(&self.env, &self.api, "csi-restart-kubelet", Duration::from_secs(300)).await {
            Ok(d) if d.phase == "Succeeded" => k8s::log(&self.env, &self.api, "csi-restart-kubelet", false).await.unwrap_or_default(),
            Ok(d) => return Outcome::Fail(format!("the kubelet restart did not happen: {}", d.message)),
            Err(e) => return Outcome::Fail(format!("the kubelet did not come back: {e}")),
        };
        // The restarted kubelet finds the driver again and publishes for a
        // new pod.
        if let Err(e) = self.registered(Duration::from_secs(120)).await {
            return Outcome::Fail(format!("after the restart: {e}"));
        }
        if let Err(e) = self.run_consumer("csi-reader2", &["expect-file", FILE, &self.token], Duration::from_secs(180)).await {
            return Outcome::Fail(format!("after the restart: {e}"));
        }
        let after = match self.api.get(&format!("{pods}/csi-holder")).await {
            Ok(Some(p)) => p,
            Ok(None) => return Outcome::Fail("the holder is gone after the restart".into()),
            Err(e) => return Outcome::Infra(e),
        };
        let restarts = after.pointer("/status/containerStatuses/0/restartCount").and_then(Value::as_u64).unwrap_or(0);
        let now = s(&after, "/status/containerStatuses/0/containerID");
        if s(&after, "/status/phase") != "Running" || now != cid || restarts != 0 {
            return Outcome::Fail(format!(
                "the holder did not keep running through the restart: phase {:?}, container {now:?} (was {cid:?}), restarts {restarts}",
                s(&after, "/status/phase")
            ));
        }
        // Its teardown is the restarted kubelet's, from the record on disk.
        if let Err(e) = self.delete_pod("csi-holder", Duration::from_secs(90)).await {
            return Outcome::Fail(e);
        }
        if let Err(e) = self.wait_call("NodeUnpublishVolume", &holder, Duration::from_secs(90)).await {
            return Outcome::Fail(format!("after the restart: {e}"));
        }
        Outcome::Pass(format!(
            "{}; the holder kept its container, a new pod read the data, the holder's delete unpublished",
            restarted.trim().trim_start_matches("restart-kubelet: ")
        ))
    }

    async fn case_deregistered(&self) -> Outcome {
        if let Err(e) = self.delete_pod(DRIVER_POD, Duration::from_secs(90)).await {
            return Outcome::Fail(e);
        }
        let path = format!("{CSINODES}/{}", self.env.node_name);
        let r = k8s::until(Duration::from_secs(60), || {
            let path = path.clone();
            async move {
                match self.api.get(&path).await? {
                    Some(n) if csinode_driver(&n, &self.driver).is_some() => Err(format!("CSINode still lists {}", self.driver)),
                    _ => Ok(()),
                }
            }
        })
        .await;
        match r {
            Ok(()) => Outcome::Pass(format!("driver pod deleted: CSINode no longer lists {}", self.driver)),
            Err(e) => Outcome::Fail(e),
        }
    }

    /// Consumers first (their unpublish needs the driver), then the driver,
    /// then the claim, the PV and the CSIDriver.
    async fn cleanup(&self) -> Result<(), String> {
        let pods = k8s::pods(&self.env);
        for p in ["csi-writer", "csi-reader", "csi-holder", "csi-reader2", "csi-restart-kubelet"] {
            self.api.delete(&format!("{pods}/{p}")).await?;
        }
        for p in ["csi-writer", "csi-reader", "csi-holder", "csi-reader2", "csi-restart-kubelet"] {
            k8s::gone(&self.api, &format!("{pods}/{p}"), Duration::from_secs(60)).await?;
        }
        k8s::drain(&self.env, &self.api, Duration::from_secs(5)).await?;
        self.api.delete(&format!("{}/{}", k8s::PVS, pv_name(&self.env))).await?;
        self.api.delete(&format!("{CSIDRIVERS}/{}", self.driver)).await
    }
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let suite = Suite { driver: driver_name(&env.run_id), token: format!("rknt-csi-{}", env.run_id), env, api };
    let s = &suite;
    if let Err(e) = s.setup().await {
        r.record("csi-setup", Outcome::Infra(e), 0, None);
    } else if r.run("csi-registered", s.case_registered()).await {
        r.run("csi-claim-bound", s.case_bound()).await;
        let t = Instant::now();
        match s.case_publish().await {
            Err(e) => {
                r.record("csi-publish", Outcome::Fail(e), t.elapsed().as_millis(), None);
            }
            Ok(writer) => {
                r.record("csi-publish", Outcome::Pass("a pod wrote through the driver's NodePublishVolume mount".into()), t.elapsed().as_millis(), None);
                if r.run("csi-unpublish", s.case_unpublish(&writer)).await {
                    r.run("csi-kubelet-restart", s.case_restart()).await;
                }
            }
        }
        r.run("csi-deregistered", s.case_deregistered()).await;
    }
    if let Err(e) = s.cleanup().await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::tests::env;

    #[test]
    fn the_driver_is_named_for_the_run() {
        assert_eq!(driver_name("94CAeb79d0"), "rknt-94caeb79d0.csi.storm.io");
        assert_eq!(driver_name("a_b-c"), "rknt-abc.csi.storm.io");
    }

    #[test]
    fn the_pv_is_the_drivers_and_prebound_to_the_claim() {
        let e = env();
        let v = pv(&e, "d.csi.storm.io");
        assert_eq!(v["spec"]["csi"]["driver"], "d.csi.storm.io");
        assert_eq!(v["spec"]["claimRef"]["namespace"], "test-ns");
        assert_eq!(v["spec"]["claimRef"]["name"], CLAIM);
        assert_eq!(v["spec"]["persistentVolumeReclaimPolicy"], "Retain");
        assert_eq!(v["metadata"]["labels"]["storm.io/test-run"], "r1");
        let c = claim(&e);
        assert_eq!(c["spec"]["volumeName"], v["metadata"]["name"]);
        assert_eq!(c["spec"]["storageClassName"], "");
        assert_eq!(csidriver(&e, "d")["spec"]["attachRequired"], false);
    }

    #[test]
    fn the_driver_pod_shares_the_kubelet_dir_both_ways() {
        let p = driver_pod(&env(), "d.csi.storm.io");
        let c = &p["spec"]["containers"][0];
        assert_eq!(c["args"], json!(["csi-driver", "d.csi.storm.io", "node1"]));
        assert_eq!(c["securityContext"]["privileged"], true);
        assert_eq!(c["volumeMounts"][0]["mountPropagation"], "Bidirectional");
        assert_eq!(p["spec"]["volumes"][0]["hostPath"]["path"], "/var/lib/kubelet");
        assert_eq!(p["spec"]["nodeName"], "node1");
        let r = restart_pod(&env());
        assert_eq!(r["spec"]["hostPID"], true);
        let w = consumer(&env(), "w", &["write-file", FILE, "x"]);
        assert_eq!(w["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"], CLAIM);
    }

    #[test]
    fn calls_are_read_from_the_drivers_log() {
        let log = "csi-driver: d serving on /var/lib/kubelet/plugins/d/csi.sock for node node1\n\
                   csi-driver: NodePublishVolume volume=vol-r1 target=/var/lib/kubelet/pods/u1/volumes/kubernetes.io~csi/pv/mount readonly=false\n\
                   csi-driver: NodeUnpublishVolume volume=vol-r1 target=/var/lib/kubelet/pods/u2/volumes/kubernetes.io~csi/pv/mount failed: busy\n";
        assert!(called(log, "NodePublishVolume", "u1"));
        assert!(!called(log, "NodePublishVolume", "u2"));
        assert!(!called(log, "NodeUnpublishVolume", "u2"), "a failed call is not the call");
    }

    #[test]
    fn csinode_lists_the_driver() {
        let n = json!({"spec": {"drivers": [{"name": "a", "nodeID": "node1"}]}});
        assert!(csinode_driver(&n, "a").is_some());
        assert!(csinode_driver(&n, "b").is_none());
        assert!(csinode_driver(&json!({"spec": {}}), "a").is_none());
    }
}
