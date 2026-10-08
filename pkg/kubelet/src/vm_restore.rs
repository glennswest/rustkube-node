//! `VirtualMachineRestore` (`snapshot.kubevirt.io/v1beta1`) for snapshots this
//! node took (#53; the owner's option A on #109).
//!
//! **Whose it is.** A snapshot is one stormblock group snapshot, and on
//! stormcos stormblock is per node, so the group and every volume made from
//! it live on the node that took it (`storm.io/snapshot-node`). That node
//! serves the restore.
//!
//! **What a restore does** (option A: the VirtualMachine's disks become the
//! restored claims):
//!
//! 1. waits for the snapshot to be `Succeeded`, and for the VM to be stopped
//!    (no VMI): a running guest's disks are not swapped under it;
//! 2. for every disk in the snapshot's disk map (`storm.io/snapshot-disks`,
//!    written when it was taken) except a cloud-init one, makes a **new**
//!    volume from that disk's member, `<ns>.<vm>-<disk>-restore-<restore>`:
//!    the disk set is restored together, and the old disks are left as they
//!    were;
//! 3. gives each a bound pair: PVC `<vm>-<disk>-restore-<restore>`
//!    (`volumeMode: Block`, a VM disk) and its PV (class `stormblock`, the
//!    volume as its handle, pinned to this node, reclaim Delete). The VM's
//!    placement follows the PV's `nodeAffinity`;
//! 4. points the VirtualMachine's `spec.template.spec.volumes[<disk>]` at that
//!    claim;
//! 5. writes `status.complete` with `restores`, and an Event.
//!
//! A cloud-init seed is not restored: the kubelet makes it again at every
//! start (#75), and a VM with no cloud-init has none.
//!
//! Every step is find-or-create (stormblock is idempotent by name, objects
//! are looked up first), so a pass interrupted anywhere finishes on the next.

use retry::RetryExt;
use serde_json::{json, Value};
use stormvm_spec::snapshot::{self as spec, Restored};
use tracing::{debug, info, warn};

use crate::vm_snapshot::{Snapshots, DISKS_ANNOTATION, NODE_ANNOTATION};

const API: &str = "snapshot.kubevirt.io/v1beta1";

/// Who marks the claims a restore made.
pub const RESTORED_FROM: &str = "storm.io/restored-from";

/// The stormblock calls a restore needs. A seam for the tests; the real one
/// is stormvm's client (blocking, so it is called off the async threads).
pub trait RestoreEngine: Send + Sync {
    /// A group snapshot by id, or `None` when there is no such group.
    fn group(&self, id: &str) -> Result<Option<stormvm_block::GroupSnapshot>, String>;
    /// A new volume named `name` holding `member`; asking again under the
    /// same name answers the one already made.
    fn from_snapshot(&self, name: &str, member: &stormvm_block::Snapshot) -> Result<String, String>;
}

/// stormvm's client against this node's stormblock, with the engine's token.
pub struct Stormblock(pub String);

// Both retried while the engine is away (#211): a read, and a create by name
// that answers the one already made when asked again.
impl RestoreEngine for Stormblock {
    fn group(&self, id: &str) -> Result<Option<stormvm_block::GroupSnapshot>, String> {
        retry::blocking(retry::Policy::ENGINE, &format!("group snapshot {id}"), |e: &String| retry::class_of_message(e), || {
            stormvm_block::Client::new(&self.0).group_snapshot_by_id(id).map_err(|e| e.to_string())
        })
    }
    fn from_snapshot(&self, name: &str, member: &stormvm_block::Snapshot) -> Result<String, String> {
        retry::blocking(retry::Policy::ENGINE, &format!("volume {name} from snapshot"), |e: &String| retry::class_of_message(e), || {
            stormvm_block::Client::new(&self.0)
                .volume_from_snapshot(name, member)
                .map(|v| if v.name.is_empty() { name.to_string() } else { v.name })
                .map_err(|e| e.to_string())
        })
    }
}

/// The names a restore gives disk `disk` of `vm`: the stormblock volume (and
/// PV), and the claim.
pub fn names(ns: &str, vm: &str, disk: &str, restore: &str) -> (String, String) {
    (format!("{ns}.{vm}-{disk}-restore-{restore}"), format!("{vm}-{disk}-restore-{restore}"))
}

/// Is `disk` a cloud-init volume in this VM's template (not restored)?
pub fn is_cloud_init(vm: &Value, disk: &str) -> bool {
    vm["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .and_then(|v| v.iter().find(|v| v["name"] == disk))
        .is_some_and(|v| v.get("cloudInitNoCloud").is_some() || v.get("cloudInitConfigDrive").is_some())
}

/// The VirtualMachine with each restored disk pointing at its claim, or
/// `None` when it already does.
pub fn rewired(vm: &Value, claims: &[(String, String)]) -> Option<Value> {
    let mut out = vm.clone();
    let vols = out["spec"]["template"]["spec"]["volumes"].as_array_mut()?;
    for (disk, claim) in claims {
        let want = json!({ "name": disk, "persistentVolumeClaim": { "claimName": claim } });
        match vols.iter_mut().find(|v| v["name"] == disk.as_str()) {
            Some(v) => *v = want,
            None => vols.push(want),
        }
    }
    (out != *vm).then_some(out)
}

/// What one pass decided for a restore that is not finished.
enum Step {
    /// Waiting (the snapshot, the VM to stop), with why: Progressing.
    Wait(String),
    /// Cannot be done as asked: not complete, Ready False with why.
    Error(String),
}

impl Snapshots {
    /// Every restore whose snapshot this node took, one pass.
    pub(crate) async fn restores(&self) {
        let Some(engine) = self.restore_engine.clone() else { return };
        let (api, url, _) = self.api();
        if url.is_empty() {
            return;
        }
        let list = match api.get(format!("{url}/apis/{API}/virtualmachinerestores")).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
            Ok(_) => None, // no CRD, or no answer: nothing to do this pass
            Err(e) => {
                debug!("virtualmachinerestores not listed: {e}");
                None
            }
        };
        for obj in list.and_then(|l| l["items"].as_array().cloned()).unwrap_or_default() {
            if obj["status"]["complete"].as_bool() == Some(true) {
                continue;
            }
            self.restore_one(&obj, engine.as_ref()).await;
        }
    }

    async fn restore_one(&self, obj: &Value, engine: &dyn RestoreEngine) {
        let Ok(req) = spec::restore_request(obj) else {
            return; // whose it is cannot be told without its snapshot
        };
        let Some(snap) = self.api_object(&format!("/apis/{API}/namespaces/{}/virtualmachinesnapshots/{}", req.namespace, req.snapshot)).await else {
            return;
        };
        let (_, _, node) = self.api();
        if snap["metadata"]["annotations"][NODE_ANNOTATION].as_str() != Some(node) {
            return; // another node's snapshot, and so its restore
        }
        match self.restore_steps(obj, &req, &snap, engine).await {
            Ok(restores) => {
                let st = spec::restore_status(true, &now(), &restores, None);
                if self.write_restore_status(obj, st).await {
                    info!("{}/{}: restored {} from {}", req.namespace, req.vm, restores.len(), req.snapshot);
                    let msg = format!(
                        "restored {} disk(s) of {} from {}: {}",
                        restores.len(),
                        req.vm,
                        req.snapshot,
                        restores.iter().map(|r| format!("{} → {}", r.disk, r.volume)).collect::<Vec<_>>().join(", ")
                    );
                    self.object_event("VirtualMachineRestore", obj, "Normal", "VirtualMachineRestoreComplete", &msg).await;
                }
            }
            Err(step) => {
                let (progressing, why) = match &step {
                    Step::Wait(w) => (true, w.clone()),
                    Step::Error(e) => (false, e.clone()),
                };
                let key = format!("{}/{}", req.namespace, req.name);
                let said = self.restore_said.lock().unwrap_or_else(|e| e.into_inner()).insert(key, why.clone());
                if said.as_deref() == Some(why.as_str()) {
                    return; // already says so
                }
                let mut st = spec::restore_status(false, &now(), &[], (!progressing).then_some(why.as_str()));
                if progressing {
                    st["conditions"][0]["reason"] = json!(why);
                }
                self.write_restore_status(obj, st).await;
                if !progressing {
                    warn!("{}/{}: restore failed: {why}", req.namespace, req.name);
                    self.object_event("VirtualMachineRestore", obj, "Warning", "VirtualMachineRestoreError", &why).await;
                }
            }
        }
    }

    async fn restore_steps(
        &self,
        obj: &Value,
        req: &spec::RestoreRequest,
        snap: &Value,
        engine: &dyn RestoreEngine,
    ) -> Result<Vec<Restored>, Step> {
        let ns = req.namespace.as_str();
        match snap["status"]["phase"].as_str() {
            Some("Succeeded") => {}
            Some("Failed") => return Err(Step::Error(format!("snapshot {} failed", req.snapshot))),
            _ => return Err(Step::Wait(format!("waiting for snapshot {} to succeed", req.snapshot))),
        }
        let group_id = snap["status"]["virtualMachineSnapshotContentName"].as_str().unwrap_or("");
        let disks: serde_json::Map<String, Value> = snap["metadata"]["annotations"][DISKS_ANNOTATION]
            .as_str()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        if group_id.is_empty() || disks.is_empty() {
            return Err(Step::Error(format!(
                "snapshot {} records no disks (taken before {DISKS_ANNOTATION} was written): take a new one",
                req.snapshot
            )));
        }
        // Stopped: a running guest's disks are not swapped under it.
        if self.api_object(&format!("/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachineinstances/{}", req.vm)).await.is_some() {
            return Err(Step::Wait(format!("VirtualMachine {} is running: stop it to restore", req.vm)));
        }
        let vm_path = format!("/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachines/{}", req.vm);
        let Some(vm) = self.api_object(&vm_path).await else {
            return Err(Step::Error(format!("VirtualMachine {} not found", req.vm)));
        };
        let group = engine
            .group(group_id)
            .map_err(|e| Step::Wait(format!("stormblock: {e}")))?
            .ok_or_else(|| Step::Error(format!("group snapshot {group_id} is gone from this node")))?;

        let mut restores = Vec::new();
        let mut claims = Vec::new();
        for (disk, vol) in &disks {
            if is_cloud_init(&vm, disk) {
                continue; // made again at the next start
            }
            let member = group
                .of(vol.as_str().unwrap_or(""))
                .ok_or_else(|| Step::Error(format!("group {group_id} has no member for disk {disk}")))?;
            let (volume, claim) = names(ns, &req.vm, disk, &req.name);
            let made = engine
                .from_snapshot(&volume, member)
                .map_err(|e| Step::Wait(format!("stormblock would not restore disk {disk}: {e}")))?;
            self.bind_restored(ns, &claim, &made, member.size_bytes, obj).await.map_err(Step::Wait)?;
            restores.push(Restored { disk: disk.clone(), snapshot: req.snapshot.clone(), volume: claim.clone() });
            claims.push((disk.clone(), claim));
        }
        // The VM's disks are the restored claims (option A).
        if let Some(updated) = rewired(&vm, &claims) {
            let (api, url, _) = self.api();
            let r = api.put(format!("{url}{vm_path}")).json(&updated).send_retrying(retry::Policy::API).await;
            match r {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => return Err(Step::Wait(format!("VirtualMachine {} not updated: {}", req.vm, r.status()))),
                Err(e) => return Err(Step::Wait(format!("VirtualMachine {} not updated: {e}", req.vm))),
            }
        }
        Ok(restores)
    }

    /// The bound PVC and PV for a restored volume, made when missing.
    async fn bind_restored(&self, ns: &str, claim: &str, volume: &str, bytes: u64, restore: &Value) -> Result<(), String> {
        let (api, url, node) = self.api();
        let pvc_path = format!("/api/v1/namespaces/{ns}/persistentvolumeclaims/{claim}");
        let pvc = match self.api_object(&pvc_path).await {
            Some(p) => p,
            None => {
                let mut pvc = json!({
                    "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": claim, "namespace": ns,
                        "labels": { RESTORED_FROM: restore["metadata"]["name"] },
                    },
                    "spec": {
                        "accessModes": ["ReadWriteOnce"],
                        "storageClassName": crate::storage::STORAGE_CLASS,
                        "volumeMode": "Block",
                        "resources": { "requests": { "storage": crate::system_claims::quantity(bytes) } },
                    },
                });
                crate::system_claims::bind_pvc(&mut pvc, volume, node, volume);
                let r = api
                    .post(format!("{url}/api/v1/namespaces/{ns}/persistentvolumeclaims"))
                    .json(&pvc)
                    .send_repeatable(retry::Policy::API)
                    .await
                    .map_err(|e| format!("claim {claim}: {e}"))?;
                if !r.status().is_success() {
                    return Err(format!("claim {claim} not made: {}", r.status()));
                }
                r.json().await.map_err(|e| format!("claim {claim}: {e}"))?
            }
        };
        if self.api_object(&format!("/api/v1/persistentvolumes/{volume}")).await.is_none() {
            let facts = crate::system_claims::VolumeFacts { name: volume.to_string(), bytes, block: true, ..Default::default() };
            let mut pv = crate::system_claims::stormblock_pv(&facts, volume, node, crate::system_claims::claim_ref(&pvc), "Delete");
            pv["metadata"]["labels"] = json!({ RESTORED_FROM: restore["metadata"]["name"] });
            let r = api
                .post(format!("{url}/api/v1/persistentvolumes"))
                .json(&pv)
                .send_repeatable(retry::Policy::API)
                .await
                .map_err(|e| format!("PV {volume}: {e}"))?;
            if !r.status().is_success() && r.status().as_u16() != 409 {
                return Err(format!("PV {volume} not made: {}", r.status()));
            }
        }
        Ok(())
    }

    async fn api_object(&self, path: &str) -> Option<Value> {
        let (api, url, _) = self.api();
        match api.get(format!("{url}{path}")).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        }
    }

    async fn write_restore_status(&self, obj: &Value, status: Value) -> bool {
        let (api, url, _) = self.api();
        let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = obj["metadata"]["name"].as_str().unwrap_or("");
        let path = format!("{url}/apis/{API}/namespaces/{ns}/virtualmachinerestores/{name}/status");
        match api
            .patch(&path)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "status": status }))
            .send_retrying(retry::Policy::API)
            .await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                warn!("{ns}/{name}: restore status not written: {}", r.status());
                false
            }
            Err(e) => {
                warn!("{ns}/{name}: restore status not written: {e}");
                false
            }
        }
    }
}

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    type Store = Arc<Mutex<HashMap<String, Value>>>;

    /// An in-memory apiserver: objects by path; the cluster-wide restore list;
    /// POST adds (uid assigned), PUT replaces, a status PATCH merges `status`.
    async fn api(objects: Vec<(&str, Value)>) -> (String, Store, Arc<Mutex<Vec<String>>>) {
        let store: Store = Arc::new(Mutex::new(objects.into_iter().map(|(k, v)| (k.to_string(), v)).collect()));
        let writes: Arc<Mutex<Vec<String>>> = Arc::default();
        let (s, w) = (store.clone(), writes.clone());
        let app = axum::Router::new().fallback(
            move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let (s, w) = (s.clone(), w.clone());
                async move {
                    use axum::http::StatusCode;
                    let path = uri.path().to_string();
                    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let mut s = s.lock().unwrap();
                    match method.as_str() {
                        "GET" if path.ends_with("/virtualmachinerestores") => {
                            let items: Vec<Value> = s.iter().filter(|(k, _)| k.contains("/virtualmachinerestores/")).map(|(_, v)| v.clone()).collect();
                            (StatusCode::OK, axum::Json(json!({ "items": items })))
                        }
                        "GET" => match s.get(&path) {
                            Some(v) => (StatusCode::OK, axum::Json(v.clone())),
                            None => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                        },
                        "POST" => {
                            w.lock().unwrap().push(format!("POST {path}"));
                            let mut o = body;
                            let key = format!("{path}/{}", o["metadata"]["name"].as_str().unwrap());
                            o["metadata"]["uid"] = json!(format!("uid-{}", s.len()));
                            s.insert(key, o.clone());
                            (StatusCode::CREATED, axum::Json(o))
                        }
                        "PUT" => {
                            w.lock().unwrap().push(format!("PUT {path}"));
                            s.insert(path, body.clone());
                            (StatusCode::OK, axum::Json(body))
                        }
                        "PATCH" if path.ends_with("/status") => {
                            w.lock().unwrap().push(format!("STATUS {path}"));
                            let key = path.trim_end_matches("/status").to_string();
                            if let Some(o) = s.get_mut(&key) {
                                o["status"] = body["status"].clone();
                            }
                            (StatusCode::OK, axum::Json(json!({})))
                        }
                        "PATCH" => (StatusCode::OK, axum::Json(json!({}))),
                        _ => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                    }
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, store, writes)
    }

    /// A stormblock with one group of members of `v-root`, `v-data`, `v-seed`.
    #[derive(Default)]
    struct Engine {
        made: Mutex<Vec<String>>,
    }

    impl RestoreEngine for Engine {
        fn group(&self, id: &str) -> Result<Option<stormvm_block::GroupSnapshot>, String> {
            let member = |v: &str| stormvm_block::Snapshot {
                id: format!("s-{v}"),
                name: format!("s-{v}"),
                source_volume_id: v.into(),
                size_bytes: 10 << 30,
                ready: true,
                created_at_ms: 0,
                group_snapshot_id: Some(id.into()),
            };
            Ok((id == "g-1").then(|| stormvm_block::GroupSnapshot {
                id: id.into(),
                name: "web.web-1.snap".into(),
                snapshots: vec![member("v-root"), member("v-data"), member("v-seed")],
                ready: true,
                created_at_ms: 0,
            }))
        }
        fn from_snapshot(&self, name: &str, member: &stormvm_block::Snapshot) -> Result<String, String> {
            self.made.lock().unwrap().push(format!("{name} <- {}", member.id));
            Ok(name.to_string())
        }
    }

    const SNAP: &str = "/apis/snapshot.kubevirt.io/v1beta1/namespaces/web/virtualmachinesnapshots/snap";
    const RESTORE: &str = "/apis/snapshot.kubevirt.io/v1beta1/namespaces/web/virtualmachinerestores/r1";
    const VM: &str = "/apis/kubevirt.io/v1/namespaces/web/virtualmachines/web-1";
    const VMI: &str = "/apis/kubevirt.io/v1/namespaces/web/virtualmachineinstances/web-1";

    fn snapshot(node: &str, disks: Option<&str>) -> Value {
        let mut ann = json!({ NODE_ANNOTATION: node });
        if let Some(d) = disks {
            ann[DISKS_ANNOTATION] = json!(d);
        }
        json!({ "metadata": { "name": "snap", "namespace": "web", "annotations": ann },
                "status": { "phase": "Succeeded", "virtualMachineSnapshotContentName": "g-1" } })
    }

    fn restore() -> Value {
        json!({ "apiVersion": "snapshot.kubevirt.io/v1beta1", "kind": "VirtualMachineRestore",
                "metadata": { "name": "r1", "namespace": "web" },
                "spec": { "target": { "apiGroup": "kubevirt.io", "kind": "VirtualMachine", "name": "web-1" },
                          "virtualMachineSnapshotName": "snap" } })
    }

    fn vm() -> Value {
        json!({ "metadata": { "name": "web-1", "namespace": "web", "resourceVersion": "5" },
                "spec": { "template": { "spec": { "volumes": [
                    { "name": "rootdisk", "dataVolume": { "name": "fedora" } },
                    { "name": "data", "emptyDisk": { "capacity": "10Gi" } },
                    { "name": "cloudinitdisk", "cloudInitNoCloud": { "userData": "#cloud-config" } },
                ] } } } })
    }

    const DISKS: &str = r#"{"rootdisk":"v-root","data":"v-data","cloudinitdisk":"v-seed"}"#;

    fn snapshots(url: &str, engine: Arc<Engine>) -> Snapshots {
        let take: crate::vm_snapshot::TakeFn =
            Arc::new(|_, _| Box::pin(async { crate::vm_snapshot::Taken::default() }));
        Snapshots::new(reqwest::Client::new(), url, "n1", "/nonexistent", take).with_restore_engine(engine)
    }

    #[tokio::test]
    async fn a_stopped_vm_is_restored_to_claims_of_its_whole_disk_set() {
        let (url, store, writes) = api(vec![(SNAP, snapshot("n1", Some(DISKS))), (RESTORE, restore()), (VM, vm())]).await;
        let engine = Arc::new(Engine::default());
        snapshots(&url, engine.clone()).restores().await;

        // Root and data from their members; the seed is made again at start.
        let mut made = engine.made.lock().unwrap().clone();
        made.sort();
        assert_eq!(made, vec!["web.web-1-data-restore-r1 <- s-v-data", "web.web-1-rootdisk-restore-r1 <- s-v-root"]);
        let s = store.lock().unwrap().clone();
        for (disk, claim) in [("rootdisk", "web-1-rootdisk-restore-r1"), ("data", "web-1-data-restore-r1")] {
            let pvc = &s[&format!("/api/v1/namespaces/web/persistentvolumeclaims/{claim}")];
            let volume = format!("web.web-1-{disk}-restore-r1");
            assert_eq!(pvc["spec"]["volumeMode"], "Block");
            assert_eq!(pvc["spec"]["volumeName"], json!(volume));
            assert_eq!(pvc["spec"]["resources"]["requests"]["storage"], "10Gi");
            assert_eq!(pvc["metadata"]["labels"][RESTORED_FROM], "r1");
            let pv = &s[&format!("/api/v1/persistentvolumes/{volume}")];
            assert_eq!(pv["spec"]["claimRef"]["uid"], pvc["metadata"]["uid"]);
            assert_eq!(pv["spec"]["volumeMode"], "Block");
            assert_eq!(pv["spec"]["csi"]["volumeHandle"], json!(volume));
            assert_eq!(pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0], "n1");
            assert_eq!(pv["spec"]["persistentVolumeReclaimPolicy"], "Delete");
        }
        // The VM's disks are the claims now; the seed is untouched.
        let vols = s[VM]["spec"]["template"]["spec"]["volumes"].as_array().unwrap().clone();
        let by = |n: &str| vols.iter().find(|v| v["name"] == n).unwrap().clone();
        assert_eq!(by("rootdisk")["persistentVolumeClaim"]["claimName"], "web-1-rootdisk-restore-r1");
        assert_eq!(by("data")["persistentVolumeClaim"]["claimName"], "web-1-data-restore-r1");
        assert!(by("cloudinitdisk").get("cloudInitNoCloud").is_some());
        // Complete, saying what went where.
        let st = &s[RESTORE]["status"];
        assert_eq!(st["complete"], true);
        assert_eq!(st["restores"].as_array().unwrap().len(), 2);

        // A complete restore is left alone.
        let n = writes.lock().unwrap().len();
        snapshots(&url, engine.clone()).restores().await;
        assert_eq!(writes.lock().unwrap().len(), n);
    }

    #[tokio::test]
    async fn a_running_vm_waits_and_another_nodes_snapshot_is_not_ours() {
        let (url, store, _) = api(vec![(SNAP, snapshot("n1", Some(DISKS))), (RESTORE, restore()), (VM, vm()),
                                       (VMI, json!({ "metadata": { "name": "web-1" } }))]).await;
        let engine = Arc::new(Engine::default());
        let s = snapshots(&url, engine.clone());
        s.restores().await;
        assert!(engine.made.lock().unwrap().is_empty(), "nothing made under a running guest");
        let st = store.lock().unwrap()[RESTORE]["status"].clone();
        assert_eq!(st["complete"], false);
        assert!(st["conditions"][0]["reason"].as_str().unwrap().contains("stop it to restore"), "{st}");

        let (url, store, writes) = api(vec![(SNAP, snapshot("n2", Some(DISKS))), (RESTORE, restore()), (VM, vm())]).await;
        snapshots(&url, engine.clone()).restores().await;
        assert!(writes.lock().unwrap().is_empty());
        assert!(store.lock().unwrap()[RESTORE].get("status").is_none());
    }

    #[tokio::test]
    async fn a_snapshot_with_no_disk_map_is_an_error_said_once() {
        let (url, store, writes) = api(vec![(SNAP, snapshot("n1", None)), (RESTORE, restore()), (VM, vm())]).await;
        let s = snapshots(&url, Arc::new(Engine::default()));
        s.restores().await;
        let st = store.lock().unwrap()[RESTORE]["status"].clone();
        assert_eq!(st["complete"], false);
        assert!(st["conditions"][1]["reason"].as_str().unwrap().contains("take a new one"), "{st}");
        let n = writes.lock().unwrap().len();
        s.restores().await;
        assert_eq!(writes.lock().unwrap().len(), n, "the same error is not written again");
    }

    #[test]
    fn rewiring_points_disks_at_claims_and_is_a_no_op_once_done() {
        let claims = vec![("rootdisk".to_string(), "c-root".to_string())];
        let once = rewired(&vm(), &claims).unwrap();
        assert!(rewired(&once, &claims).is_none());
        assert!(is_cloud_init(&vm(), "cloudinitdisk") && !is_cloud_init(&vm(), "rootdisk"));
        assert_eq!(names("web", "web-1", "rootdisk", "r1"), ("web.web-1-rootdisk-restore-r1".into(), "web-1-rootdisk-restore-r1".into()));
    }
}
