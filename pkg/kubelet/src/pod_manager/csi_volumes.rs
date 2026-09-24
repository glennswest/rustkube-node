//! Volumes of external CSI drivers: mount at pod start, unmount when it goes.
//!
//! The built-in class (`stormblock.storm.io`) is not a CSI driver at all: the
//! node clones and attaches it itself (`provision_claim`). This is the path
//! for everyone else's: a claim bound to a PV whose `csi.driver` is another
//! driver, and inline `csi:` volumes. Registration is `csi_plugins.rs`, the
//! protocol is `csi.rs`, and the design, including what mount propagation
//! requires, is `docs/csi.md`.
//!
//! **State is on disk, not in memory.** Every published volume has a
//! `vol_data.json` beside its mount directory, written *before* the driver
//! is called, as upstream does. Teardown reads those files, so a volume
//! published by a kubelet that has since restarted, or by a pod start that
//! failed half-way, is still unpublished, and unstaged when the last one on
//! the node goes.

use super::{ClaimError, PodManager};
use crate::csi::{self, VolumeSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// The driver this node provisions itself. Its PVs are not CSI volumes.
pub const BUILTIN_DRIVER: &str = "stormblock.storm.io";

/// What is recorded beside each published volume. The field names are
/// upstream's `vol_data.json`, plus the staging path, so teardown does not
/// need to recompute it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VolData {
    pub driver_name: String,
    pub volume_handle: String,
    pub spec_vol_id: String,
    pub node_name: String,
    /// `Persistent` or `Ephemeral` (an inline `csi:` volume).
    pub volume_lifecycle_mode: String,
    /// Set when the volume was staged, so it has to be unstaged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_path: Option<String>,
}

/// What the CSIDriver object says. A driver with no CSIDriver object gets
/// upstream's defaults: attach required, no pod info, persistent only.
struct DriverPolicy {
    attach_required: bool,
    pod_info_on_mount: bool,
    ephemeral_allowed: bool,
}

impl PodManager {
    /// The PV behind `pvc`, when it belongs to an external CSI driver.
    ///
    /// `None` for an unbound claim, for the built-in class, and for a PV that
    /// is not CSI. Those go the way they always went.
    pub(super) async fn external_csi_pv(&self, pvc: &Value) -> Option<Value> {
        let pv_name = pvc["spec"]["volumeName"].as_str().filter(|v| !v.is_empty())?;
        let pv = self.api_get(&format!("/api/v1/persistentvolumes/{pv_name}")).await?;
        let driver = pv["spec"]["csi"]["driver"].as_str()?;
        (driver != BUILTIN_DRIVER).then_some(pv)
    }

    async fn driver_policy(&self, driver: &str) -> DriverPolicy {
        let d = self
            .api_get(&format!("/apis/storage.k8s.io/v1/csidrivers/{driver}"))
            .await
            .unwrap_or(Value::Null);
        let spec = &d["spec"];
        DriverPolicy {
            attach_required: spec["attachRequired"].as_bool().unwrap_or(true),
            pod_info_on_mount: spec["podInfoOnMount"].as_bool().unwrap_or(false),
            ephemeral_allowed: spec["volumeLifecycleModes"]
                .as_array()
                .is_some_and(|m| m.iter().any(|m| m.as_str() == Some("Ephemeral"))),
        }
    }

    /// A Secret's data, for a `*SecretRef`. An absent ref is no secrets; a
    /// ref to a Secret that cannot be read is an error, because a driver
    /// called without its credentials fails in a way that names neither.
    async fn csi_secrets(&self, r: &Value, default_ns: &str) -> Result<HashMap<String, String>, ClaimError> {
        let Some(name) = r["name"].as_str().filter(|n| !n.is_empty()) else {
            return Ok(HashMap::new());
        };
        let ns = r["namespace"].as_str().filter(|n| !n.is_empty()).unwrap_or(default_ns);
        self.fetch_secret_decoded(ns, name)
            .await
            .ok_or_else(|| ClaimError::Failed(format!("secret {ns}/{name} for the CSI driver cannot be read")))
    }

    /// Mount a claim bound to an external driver's PV: wait for the attach,
    /// stage, publish, and confirm the node can see it. The published
    /// directory is returned, for the engine to bind.
    pub(super) async fn mount_csi_claim(
        &self,
        pod: &Value,
        vol_name: &str,
        pvc: &Value,
        pv: &Value,
        readonly: bool,
    ) -> Result<String, ClaimError> {
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let claim = pvc["metadata"]["name"].as_str().unwrap_or("");
        let src = &pv["spec"]["csi"];
        let driver = src["driver"].as_str().unwrap_or("");
        let handle = src["volumeHandle"].as_str().unwrap_or("");
        let pv_name = pv["metadata"]["name"].as_str().unwrap_or("");
        if handle.is_empty() {
            return Err(ClaimError::Failed(format!("PV {pv_name} has no csi.volumeHandle")));
        }
        if pv["spec"]["volumeMode"].as_str() == Some("Block") {
            return Err(ClaimError::NotOurs(format!(
                "PV {pv_name} is a raw block volume (volumeMode: Block), which this node does not \
                 publish yet; only Filesystem"
            )));
        }
        // ReadWriteOncePod holds on this node whoever the driver is, for the
        // same reason as for the built-in class (#42).
        if crate::storage::is_rwop(pvc) {
            if let Some(holder) = self.other_pod_holding(namespace, claim, uid).await {
                return Err(ClaimError::InUse(format!(
                    "claim {namespace}/{claim} is ReadWriteOncePod and is already mounted by pod \
                     {namespace}/{holder} on this node"
                )));
            }
        }

        let reg = self.csi.get(driver).await.ok_or_else(|| {
            ClaimError::Failed(format!(
                "CSI driver {driver} is not registered on this node (no registrar socket in \
                 {}; is its node plugin running here?)",
                crate::csi_plugins::PLUGINS_REGISTRY
            ))
        })?;
        let policy = self.driver_policy(driver).await;

        // Attached first, when the driver attaches. The external-attacher
        // does ControllerPublishVolume and records the result on the
        // VolumeAttachment, and NodeStage may need its metadata (a device
        // path, a LUN) to find the volume at all.
        let mut publish_context = HashMap::new();
        if policy.attach_required {
            let va_name = csi::attachment_name(handle, driver, &self.node_name);
            let va = self
                .api_get(&format!("/apis/storage.k8s.io/v1/volumeattachments/{va_name}"))
                .await
                .ok_or_else(|| {
                    ClaimError::Failed(format!(
                        "waiting for VolumeAttachment {va_name} ({driver} volume {handle} to \
                         {}): not created yet",
                        self.node_name
                    ))
                })?;
            if va["status"]["attached"].as_bool() != Some(true) {
                let why = va["status"]["attachError"]["message"]
                    .as_str()
                    .map(|m| format!(": {m}"))
                    .unwrap_or_else(|| ": the driver's attacher has not attached it yet".into());
                return Err(ClaimError::Failed(format!("waiting for VolumeAttachment {va_name}{why}")));
            }
            if let Some(m) = va["status"]["attachmentMetadata"].as_object() {
                for (k, v) in m {
                    if let Some(v) = v.as_str() {
                        publish_context.insert(k.clone(), v.to_string());
                    }
                }
            }
        }

        let mut volume_context: HashMap<String, String> = src["volumeAttributes"]
            .as_object()
            .map(|m| m.iter().filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string()))).collect())
            .unwrap_or_default();
        if policy.pod_info_on_mount {
            volume_context.extend(pod_info(pod, false));
        }
        let first_mode = pv["spec"]["accessModes"][0].as_str().unwrap_or("ReadWriteOnce");
        let spec = VolumeSpec {
            volume_id: handle.to_string(),
            fs_type: src["fsType"].as_str().unwrap_or("").to_string(),
            mount_flags: pv["spec"]["mountOptions"]
                .as_array()
                .map(|a| a.iter().filter_map(|o| o.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            access_mode: csi::access_mode(first_mode, reg.caps) as i32,
            readonly: readonly || src["readOnly"].as_bool().unwrap_or(false),
            volume_context,
            publish_context,
            stage_secrets: self.csi_secrets(&src["nodeStageSecretRef"], namespace).await?,
            publish_secrets: self.csi_secrets(&src["nodePublishSecretRef"], namespace).await?,
        };
        let staging = reg
            .caps
            .stage_unstage
            .then(|| csi::staging_path(&self.state_root, driver, handle));
        let data = VolData {
            driver_name: driver.to_string(),
            volume_handle: handle.to_string(),
            spec_vol_id: pv_name.to_string(),
            node_name: self.node_name.clone(),
            volume_lifecycle_mode: "Persistent".into(),
            staging_path: staging.clone(),
        };
        self.publish_csi(&reg, uid, vol_name, &spec, &data).await
    }

    /// Mount an inline `csi:` volume: a volume that lives and dies with the
    /// pod, published without stage or attach, as upstream does.
    pub(super) async fn mount_csi_inline(&self, pod: &Value, vol: &Value) -> Result<String, ClaimError> {
        let namespace = pod["metadata"]["namespace"].as_str().unwrap_or("default");
        let uid = pod["metadata"]["uid"].as_str().unwrap_or("");
        let vol_name = vol["name"].as_str().unwrap_or("");
        let src = &vol["csi"];
        let driver = src["driver"].as_str().unwrap_or("");
        let reg = self.csi.get(driver).await.ok_or_else(|| {
            ClaimError::Failed(format!("CSI driver {driver} is not registered on this node"))
        })?;
        let policy = self.driver_policy(driver).await;
        if !policy.ephemeral_allowed {
            return Err(ClaimError::NotOurs(format!(
                "volume {vol_name}: CSI driver {driver} does not list Ephemeral in its \
                 CSIDriver volumeLifecycleModes, so it cannot be used inline"
            )));
        }
        let mut volume_context: HashMap<String, String> = src["volumeAttributes"]
            .as_object()
            .map(|m| m.iter().filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string()))).collect())
            .unwrap_or_default();
        // Upstream always says "ephemeral" for an inline volume, and adds the
        // pod's identity when the driver asks for it.
        volume_context.insert("csi.storage.k8s.io/ephemeral".into(), "true".into());
        if policy.pod_info_on_mount {
            volume_context.extend(pod_info(pod, true));
        }
        // Upstream's handle for an inline volume: unique to the pod and the
        // volume, and stable across the pod's restarts.
        let handle = format!("csi-{}", csi::sha256_hex(&format!("{uid}{vol_name}")));
        let spec = VolumeSpec {
            volume_id: handle.clone(),
            fs_type: src["fsType"].as_str().unwrap_or("").to_string(),
            mount_flags: vec![],
            access_mode: csi::access_mode("ReadWriteOnce", reg.caps) as i32,
            readonly: src["readOnly"].as_bool().unwrap_or(false),
            volume_context,
            publish_context: HashMap::new(),
            stage_secrets: HashMap::new(),
            publish_secrets: self.csi_secrets(&src["nodePublishSecretRef"], namespace).await?,
        };
        let data = VolData {
            driver_name: driver.to_string(),
            volume_handle: handle,
            spec_vol_id: vol_name.to_string(),
            node_name: self.node_name.clone(),
            volume_lifecycle_mode: "Ephemeral".into(),
            staging_path: None,
        };
        self.publish_csi(&reg, uid, vol_name, &spec, &data).await
    }

    /// Record, stage, publish, and verify.
    async fn publish_csi(
        &self,
        reg: &crate::csi_plugins::Registered,
        uid: &str,
        vol_name: &str,
        spec: &VolumeSpec,
        data: &VolData,
    ) -> Result<String, ClaimError> {
        let target = csi::publish_path(&self.state_root, uid, vol_name);
        let dir = Path::new(&target).parent().map(Path::to_path_buf).unwrap_or_default();
        // The record first, so a publish that happens and is then forgotten
        // (the kubelet dies before the pod starts) is still undone.
        std::fs::create_dir_all(&dir)
            .map_err(|e| ClaimError::Failed(format!("cannot create {}: {e}", dir.display())))?;
        let json = serde_json::to_vec_pretty(data).unwrap_or_default();
        std::fs::write(dir.join("vol_data.json"), json)
            .map_err(|e| ClaimError::Failed(format!("cannot record {}: {e}", dir.display())))?;
        // The CO makes the staging directory. The target's parent is ours,
        // and the target itself is the driver's to create.
        if let Some(staging) = &data.staging_path {
            std::fs::create_dir_all(staging)
                .map_err(|e| ClaimError::Failed(format!("cannot create {staging}: {e}")))?;
        }

        csi::setup_volume(&reg.client, spec, data.staging_path.as_deref(), &target)
            .await
            .map_err(|e| ClaimError::Failed(format!("{e:#}")))?;

        // **Is it really there, where the engine will look?** See
        // `csi::is_mount_point`. A driver whose mounts do not propagate to the
        // node has published into its own namespace, and binding the
        // directory would give the pod an empty one on the node's root.
        let mountinfo = std::fs::read_to_string(&self.csi_mountinfo).map_err(|e| {
            ClaimError::Failed(format!(
                "published {} but cannot confirm the node sees the mount ({}: {e})",
                data.driver_name, self.csi_mountinfo
            ))
        })?;
        if !csi::is_mount_point(&mountinfo, &target) {
            return Err(ClaimError::Failed(format!(
                "CSI driver {} published volume {} at {target}, but the mount is not visible on \
                 the node, so the pod would get an empty directory. The driver pod has to mount \
                 /var/lib/kubelet with mountPropagation: Bidirectional, and the engine has to \
                 honour it (stormpump#35; see docs/csi.md)",
                data.driver_name, data.volume_handle
            )));
        }
        info!(
            "CSI {} volume {} published for pod {uid} at {target}",
            data.driver_name, data.volume_handle
        );
        Ok(target)
    }

    /// Unpublish every CSI volume of pod `uid`, and unstage the ones no other
    /// pod on this node still has. `true` when nothing is left.
    ///
    /// Called after the pod's containers are gone, so the engine's binds of
    /// the published directories have gone with them. A volume whose driver
    /// is not registered right now, or that the driver refuses to unpublish,
    /// keeps its record and is retried by [`Self::sweep_csi_volumes`].
    pub async fn teardown_csi_volumes(&self, uid: &str) -> bool {
        let base = PathBuf::from(&self.state_root).join("pods").join(uid).join("volumes/kubernetes.io~csi");
        let Ok(rd) = std::fs::read_dir(&base) else { return true };
        let mut clean = true;
        for entry in rd.flatten() {
            let dir = entry.path();
            let Some(data) = read_vol_data(&dir) else {
                // No record means nothing was ever asked of a driver here.
                let _ = std::fs::remove_dir(dir.join("mount"));
                let _ = std::fs::remove_dir(&dir);
                continue;
            };
            let Some(reg) = self.csi.get(&data.driver_name).await else {
                debug!("CSI {}: not registered, {} stays published for now", data.driver_name, dir.display());
                clean = false;
                continue;
            };
            let target = dir.join("mount").to_string_lossy().into_owned();
            if let Err(e) = reg.client.unpublish(&data.volume_handle, &target).await {
                warn!("CSI {}: unpublish of {target} failed, will retry: {e:#}", data.driver_name);
                clean = false;
                continue;
            }
            // Only empty directories are removed. A mount point that is somehow
            // still mounted is not empty, and remove_dir refuses it rather than
            // deleting what is in the volume.
            if std::fs::remove_dir(&target).is_err() && Path::new(&target).exists() {
                warn!("CSI {}: {target} is not empty after unpublish; left in place", data.driver_name);
                clean = false;
                continue;
            }
            let _ = std::fs::remove_file(dir.join("vol_data.json"));
            let _ = std::fs::remove_dir(&dir);
            info!("CSI {} volume {} unpublished from pod {uid}", data.driver_name, data.volume_handle);

            if let Some(staging) = &data.staging_path {
                if self.csi_volume_in_use(&data.driver_name, &data.volume_handle) {
                    continue;
                }
                match reg.client.unstage(&data.volume_handle, staging).await {
                    Ok(()) => info!("CSI {} volume {} unstaged", data.driver_name, data.volume_handle),
                    // Not retried from here: the record that would drive a
                    // retry is gone. The driver treats a later NodeStage as
                    // idempotent, so a volume left staged is untidy, not wrong.
                    Err(e) => warn!("CSI {}: unstage of {staging} failed: {e:#}", data.driver_name),
                }
            }
        }
        if clean {
            let _ = std::fs::remove_dir(&base);
        }
        clean
    }

    /// Does any pod on this node still have this volume published?
    fn csi_volume_in_use(&self, driver: &str, handle: &str) -> bool {
        csi_records(&self.state_root)
            .into_iter()
            .any(|(_, d)| d.driver_name == driver && d.volume_handle == handle)
    }

    /// Tear down the CSI volumes of pods this node is no longer running.
    ///
    /// Covers what `stop_pod` cannot: a pod deleted while the kubelet was
    /// down, one whose start failed after its volumes were published, and a
    /// teardown that failed and is due a retry. A pod counts as running if
    /// this manager has it or the apiserver says it is bound here and not
    /// finished. The second is what keeps this away from a pod half-way
    /// through its start, which is not in the manager yet. If the apiserver
    /// cannot be asked, nothing is touched.
    pub async fn sweep_csi_volumes(&self) {
        let uids: std::collections::BTreeSet<String> =
            csi_records(&self.state_root).into_iter().map(|(uid, _)| uid).collect();
        if uids.is_empty() {
            return;
        }
        let Some(pods) = self.api_get("/api/v1/pods").await else { return };
        let live: std::collections::HashSet<&str> = pods["items"]
            .as_array()
            .map(|a| a.as_slice())
            .unwrap_or(&[])
            .iter()
            .filter(|p| p["spec"]["nodeName"].as_str() == Some(&self.node_name))
            .filter(|p| !matches!(p["status"]["phase"].as_str(), Some("Succeeded") | Some("Failed")))
            .filter_map(|p| p["metadata"]["uid"].as_str())
            .collect();
        for uid in uids {
            if live.contains(uid.as_str()) || self.pods.read().await.contains_key(&uid) {
                continue;
            }
            self.teardown_csi_volumes(&uid).await;
        }
    }
}

/// The pod's identity, for a driver that asked for it (`podInfoOnMount`).
fn pod_info(pod: &Value, ephemeral: bool) -> HashMap<String, String> {
    let m = &pod["metadata"];
    HashMap::from([
        ("csi.storage.k8s.io/pod.name".into(), m["name"].as_str().unwrap_or("").into()),
        ("csi.storage.k8s.io/pod.namespace".into(), m["namespace"].as_str().unwrap_or("").into()),
        ("csi.storage.k8s.io/pod.uid".into(), m["uid"].as_str().unwrap_or("").into()),
        (
            "csi.storage.k8s.io/serviceAccount.name".into(),
            pod["spec"]["serviceAccountName"].as_str().unwrap_or("default").into(),
        ),
        ("csi.storage.k8s.io/ephemeral".into(), ephemeral.to_string()),
    ])
}

fn read_vol_data(dir: &Path) -> Option<VolData> {
    let bytes = std::fs::read(dir.join("vol_data.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Every CSI volume record on this node, with the pod it belongs to.
pub(super) fn csi_records(state_root: &str) -> Vec<(String, VolData)> {
    let mut out = Vec::new();
    let Ok(pods) = std::fs::read_dir(Path::new(state_root).join("pods")) else { return out };
    for pod in pods.flatten() {
        let uid = pod.file_name().to_string_lossy().into_owned();
        let Ok(vols) = std::fs::read_dir(pod.path().join("volumes/kubernetes.io~csi")) else { continue };
        for v in vols.flatten() {
            if let Some(d) = read_vol_data(&v.path()) {
                out.push((uid.clone(), d));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    //! The whole node side against a real gRPC driver on a Unix socket: a mock
    //! registrar and node plugin, a fake apiserver, and PID 1's mountinfo as a
    //! file the mock driver writes to when it "mounts".

    use super::*;
    use crate::csi::proto;
    use crate::csi_plugins::{registration_proto as reg, CsiPlugins};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tonic::{Request, Response, Status};

    const NODE: &str = "test-node";
    const DRIVER: &str = "test.csi.io";

    type R<T> = Result<Response<T>, Status>;

    /// A node plugin that records its calls and "mounts" by creating the
    /// target and listing it in the mountinfo file, when `propagates`.
    #[derive(Clone)]
    struct MockDriver {
        calls: Arc<Mutex<Vec<String>>>,
        stage: Arc<Mutex<Option<proto::NodeStageVolumeRequest>>>,
        publish: Arc<Mutex<Option<proto::NodePublishVolumeRequest>>>,
        mountinfo: PathBuf,
        propagates: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockDriver {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn record(&self, c: &str) {
            self.calls.lock().unwrap().push(c.to_string());
        }
    }

    #[tonic::async_trait]
    impl proto::identity_server::Identity for MockDriver {
        async fn get_plugin_info(&self, _: Request<proto::GetPluginInfoRequest>) -> R<proto::GetPluginInfoResponse> {
            Ok(Response::new(proto::GetPluginInfoResponse {
                name: DRIVER.into(),
                vendor_version: "1.0".into(),
                manifest: Default::default(),
            }))
        }
        async fn get_plugin_capabilities(
            &self,
            _: Request<proto::GetPluginCapabilitiesRequest>,
        ) -> R<proto::GetPluginCapabilitiesResponse> {
            Ok(Response::new(proto::GetPluginCapabilitiesResponse { capabilities: vec![] }))
        }
        async fn probe(&self, _: Request<proto::ProbeRequest>) -> R<proto::ProbeResponse> {
            Ok(Response::new(proto::ProbeResponse { ready: Some(true) }))
        }
    }

    #[tonic::async_trait]
    impl proto::node_server::Node for MockDriver {
        async fn node_stage_volume(&self, r: Request<proto::NodeStageVolumeRequest>) -> R<proto::NodeStageVolumeResponse> {
            self.record("stage");
            *self.stage.lock().unwrap() = Some(r.into_inner());
            Ok(Response::new(proto::NodeStageVolumeResponse {}))
        }
        async fn node_unstage_volume(&self, _: Request<proto::NodeUnstageVolumeRequest>) -> R<proto::NodeUnstageVolumeResponse> {
            self.record("unstage");
            Ok(Response::new(proto::NodeUnstageVolumeResponse {}))
        }
        async fn node_publish_volume(&self, r: Request<proto::NodePublishVolumeRequest>) -> R<proto::NodePublishVolumeResponse> {
            self.record("publish");
            let r = r.into_inner();
            std::fs::create_dir_all(&r.target_path).unwrap();
            if self.propagates.load(std::sync::atomic::Ordering::SeqCst) {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new().append(true).create(true).open(&self.mountinfo).unwrap();
                writeln!(f, "140 22 0:52 / {} rw shared:70 - tmpfs tmpfs rw", r.target_path).unwrap();
            }
            *self.publish.lock().unwrap() = Some(r);
            Ok(Response::new(proto::NodePublishVolumeResponse {}))
        }
        async fn node_unpublish_volume(&self, r: Request<proto::NodeUnpublishVolumeRequest>) -> R<proto::NodeUnpublishVolumeResponse> {
            self.record("unpublish");
            let _ = std::fs::remove_dir(r.into_inner().target_path);
            Ok(Response::new(proto::NodeUnpublishVolumeResponse {}))
        }
        async fn node_get_volume_stats(&self, _: Request<proto::NodeGetVolumeStatsRequest>) -> R<proto::NodeGetVolumeStatsResponse> {
            Err(Status::unimplemented("stats"))
        }
        async fn node_expand_volume(&self, _: Request<proto::NodeExpandVolumeRequest>) -> R<proto::NodeExpandVolumeResponse> {
            Err(Status::unimplemented("expand"))
        }
        async fn node_get_capabilities(
            &self,
            _: Request<proto::NodeGetCapabilitiesRequest>,
        ) -> R<proto::NodeGetCapabilitiesResponse> {
            use proto::node_service_capability::{rpc::Type, Rpc, Type as Cap};
            Ok(Response::new(proto::NodeGetCapabilitiesResponse {
                capabilities: vec![proto::NodeServiceCapability {
                    r#type: Some(Cap::Rpc(Rpc { r#type: Type::StageUnstageVolume as i32 })),
                }],
            }))
        }
        async fn node_get_info(&self, _: Request<proto::NodeGetInfoRequest>) -> R<proto::NodeGetInfoResponse> {
            Ok(Response::new(proto::NodeGetInfoResponse {
                node_id: "driver-id-of-test-node".into(),
                max_volumes_per_node: 0,
                accessible_topology: Some(proto::Topology {
                    segments: HashMap::from([("topology.test.csi.io/node".into(), NODE.into())]),
                }),
            }))
        }
    }

    /// The registrar sidecar: points the kubelet at the driver's socket.
    #[derive(Clone)]
    struct MockRegistrar {
        endpoint: String,
        notified: Arc<Mutex<Option<reg::RegistrationStatus>>>,
    }

    #[tonic::async_trait]
    impl reg::registration_server::Registration for MockRegistrar {
        async fn get_info(&self, _: Request<reg::InfoRequest>) -> R<reg::PluginInfo> {
            Ok(Response::new(reg::PluginInfo {
                r#type: "CSIPlugin".into(),
                name: DRIVER.into(),
                endpoint: self.endpoint.clone(),
                supported_versions: vec!["1.0.0".into()],
            }))
        }
        async fn notify_registration_status(
            &self,
            r: Request<reg::RegistrationStatus>,
        ) -> R<reg::RegistrationStatusResponse> {
            *self.notified.lock().unwrap() = Some(r.into_inner());
            Ok(Response::new(reg::RegistrationStatusResponse {}))
        }
    }

    /// A fake apiserver: GET what is stored, 404 otherwise; POST and PUT
    /// store; PATCH is recorded.
    #[derive(Clone, Default)]
    struct FakeApi {
        objects: Arc<Mutex<HashMap<String, Value>>>,
        patches: Arc<Mutex<Vec<(String, Value)>>>,
    }

    impl FakeApi {
        fn put(&self, path: &str, v: Value) {
            self.objects.lock().unwrap().insert(path.to_string(), v);
        }
        fn get(&self, path: &str) -> Option<Value> {
            self.objects.lock().unwrap().get(path).cloned()
        }
        async fn serve(&self) -> String {
            use axum::http::{Method, StatusCode};
            let api = self.clone();
            let app = axum::Router::new().fallback(
                move |method: Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                    let api = api.clone();
                    async move {
                        let path = uri.path().to_string();
                        let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                        match method {
                            Method::GET => match api.get(&path) {
                                Some(v) => (StatusCode::OK, axum::Json(v)),
                                None => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                            },
                            Method::POST => {
                                let name = body["metadata"]["name"].as_str().unwrap_or("").to_string();
                                api.put(&format!("{path}/{name}"), body.clone());
                                (StatusCode::CREATED, axum::Json(body))
                            }
                            Method::PUT => {
                                api.put(&path, body.clone());
                                (StatusCode::OK, axum::Json(body))
                            }
                            Method::PATCH => {
                                api.patches.lock().unwrap().push((path, body.clone()));
                                (StatusCode::OK, axum::Json(body))
                            }
                            _ => (StatusCode::METHOD_NOT_ALLOWED, axum::Json(json!({}))),
                        }
                    }
                },
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            format!("http://{addr}")
        }
    }

    struct Rig {
        _dir: tempfile::TempDir,
        root: PathBuf,
        driver: MockDriver,
        registrar: MockRegistrar,
        api: FakeApi,
        csi: Arc<CsiPlugins>,
        mgr: PodManager,
    }

    async fn rig() -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let registry = root.join("plugins_registry");
        std::fs::create_dir_all(&registry).unwrap();
        std::fs::create_dir_all(root.join("plugins/test")).unwrap();
        let mountinfo = root.join("mountinfo");
        std::fs::write(&mountinfo, "22 1 0:21 / / rw - erofs /dev/vda ro\n").unwrap();

        let driver = MockDriver {
            calls: Default::default(),
            stage: Default::default(),
            publish: Default::default(),
            mountinfo: mountinfo.clone(),
            propagates: Arc::new(true.into()),
        };
        let sock = root.join("plugins/test/csi.sock");
        let identity = proto::identity_server::IdentityServer::new(driver.clone());
        let node = proto::node_server::NodeServer::new(driver.clone());
        let listener = tokio::net::UnixListener::bind(&sock).unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(identity)
                .add_service(node)
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await
                .unwrap()
        });

        let registrar = MockRegistrar {
            endpoint: format!("unix://{}", sock.display()),
            notified: Default::default(),
        };
        let reg_svc = reg::registration_server::RegistrationServer::new(registrar.clone());
        let listener = tokio::net::UnixListener::bind(registry.join(format!("{DRIVER}-reg.sock"))).unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(reg_svc)
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await
                .unwrap()
        });

        let api = FakeApi::default();
        let url = api.serve().await;
        api.put(&format!("/api/v1/nodes/{NODE}"), json!({"metadata": {"name": NODE, "uid": "node-uid"}}));

        let csi = Arc::new(
            CsiPlugins::new(NODE)
                .with_api(reqwest::Client::new(), &url)
                .with_registry_dir(&registry),
        );
        let rt = Arc::new(crate::pod_manager::tests::FakeRuntime::default());
        let mut mgr = PodManager::with_api(rt.clone(), rt, NODE, &url, "127.0.0.1", reqwest::Client::new())
            .with_csi(csi.clone());
        mgr.state_root = root.join("kubelet").to_string_lossy().into_owned();
        mgr.csi_mountinfo = mountinfo.to_string_lossy().into_owned();
        Rig { _dir: dir, root, driver, registrar, api, csi, mgr }
    }

    fn claim_pod(uid: &str, name: &str) -> Value {
        json!({
            "metadata": {"name": name, "namespace": "default", "uid": uid},
            "spec": {
                "nodeName": NODE,
                "serviceAccountName": "app",
                "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "data"}}],
                "containers": [{"name": "c", "image": "busybox"}],
            },
        })
    }

    fn store_claim(api: &FakeApi) {
        api.put(
            "/api/v1/namespaces/default/persistentvolumeclaims/data",
            json!({"metadata": {"name": "data", "namespace": "default"},
                   "spec": {"storageClassName": "test", "volumeName": "pv-data",
                            "accessModes": ["ReadWriteOnce"]}}),
        );
        api.put(
            "/api/v1/persistentvolumes/pv-data",
            json!({"metadata": {"name": "pv-data"},
                   "spec": {"accessModes": ["ReadWriteOnce"],
                            "mountOptions": ["noatime"],
                            "csi": {"driver": DRIVER, "volumeHandle": "vol-1", "fsType": "ext4",
                                    "volumeAttributes": {"share": "a"}}}}),
        );
        api.put(
            &format!("/apis/storage.k8s.io/v1/csidrivers/{DRIVER}"),
            json!({"metadata": {"name": DRIVER}, "spec": {"attachRequired": true, "podInfoOnMount": true}}),
        );
    }

    fn attach(api: &FakeApi) {
        let va = csi::attachment_name("vol-1", DRIVER, NODE);
        api.put(
            &format!("/apis/storage.k8s.io/v1/volumeattachments/{va}"),
            json!({"metadata": {"name": va},
                   "status": {"attached": true, "attachmentMetadata": {"devicePath": "/dev/sdx"}}}),
        );
    }

    fn not_ready(r: Result<HashMap<String, super::super::ResolvedVolume>, crate::cri::CriError>) -> String {
        match r {
            Err(crate::cri::CriError::VolumeNotReady(m)) => m,
            other => panic!("expected VolumeNotReady, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_driver_registers_and_is_recorded_in_csinode() {
        let rig = rig().await;
        rig.csi.scan_once().await;
        assert_eq!(rig.csi.names().await, vec![DRIVER.to_string()]);
        let reg = rig.csi.get(DRIVER).await.unwrap();
        assert_eq!(reg.node.node_id, "driver-id-of-test-node");
        assert!(reg.caps.stage_unstage);
        // The registrar was told.
        let n = rig.registrar.notified.lock().unwrap().clone().unwrap();
        assert!(n.plugin_registered, "{}", n.error);

        // CSINode lists it, with the driver's node ID, owned by the Node.
        let cn = rig.api.get(&format!("/apis/storage.k8s.io/v1/csinodes/{NODE}")).unwrap();
        assert_eq!(cn["spec"]["drivers"][0]["name"], DRIVER);
        assert_eq!(cn["spec"]["drivers"][0]["nodeID"], "driver-id-of-test-node");
        assert_eq!(cn["spec"]["drivers"][0]["topologyKeys"], json!(["topology.test.csi.io/node"]));
        assert_eq!(cn["metadata"]["ownerReferences"][0]["uid"], "node-uid");
        // And the topology is on the node.
        let patches = rig.api.patches.lock().unwrap().clone();
        assert_eq!(patches[0].1["metadata"]["labels"]["topology.test.csi.io/node"], NODE);

        // The registrar goes: so does the driver, from here and CSINode.
        std::fs::remove_file(rig.root.join(format!("plugins_registry/{DRIVER}-reg.sock"))).unwrap();
        rig.csi.scan_once().await;
        assert!(rig.csi.names().await.is_empty());
        let cn = rig.api.get(&format!("/apis/storage.k8s.io/v1/csinodes/{NODE}")).unwrap();
        assert_eq!(cn["spec"]["drivers"], json!([]));
    }

    #[tokio::test]
    async fn a_claim_of_another_driver_is_staged_published_and_torn_down() {
        let rig = rig().await;
        store_claim(&rig.api);
        let pod = claim_pod("uid-1", "app-1");

        // Not registered yet: the pod waits, and says which driver.
        let m = not_ready(rig.mgr.resolve_volumes(&pod).await);
        assert!(m.contains("not registered"), "{m}");

        rig.csi.scan_once().await;
        // Registered, not attached: waits on the VolumeAttachment by name.
        let m = not_ready(rig.mgr.resolve_volumes(&pod).await);
        assert!(m.contains(&csi::attachment_name("vol-1", DRIVER, NODE)), "{m}");
        assert!(rig.driver.calls().is_empty(), "nothing is staged before the attach");

        attach(&rig.api);
        let vols = rig.mgr.resolve_volumes(&pod).await.unwrap();
        let target = csi::publish_path(&rig.mgr.state_root, "uid-1", "data");
        assert_eq!(vols["data"].path, target);
        assert_eq!(vols["data"].fstype, None, "the engine binds a published directory");
        assert_eq!(rig.driver.calls(), vec!["stage", "publish"]);

        let stage = rig.driver.stage.lock().unwrap().clone().unwrap();
        assert_eq!(stage.volume_id, "vol-1");
        assert_eq!(stage.staging_target_path, csi::staging_path(&rig.mgr.state_root, DRIVER, "vol-1"));
        assert_eq!(stage.publish_context["devicePath"], "/dev/sdx");
        let publish = rig.driver.publish.lock().unwrap().clone().unwrap();
        assert_eq!(publish.target_path, target);
        assert_eq!(publish.staging_target_path, stage.staging_target_path);
        assert_eq!(publish.volume_context["share"], "a");
        assert_eq!(publish.volume_context["csi.storage.k8s.io/pod.name"], "app-1");
        assert_eq!(publish.volume_context["csi.storage.k8s.io/serviceAccount.name"], "app");
        let cap = publish.volume_capability.unwrap();
        match cap.access_type.unwrap() {
            proto::volume_capability::AccessType::Mount(m) => {
                assert_eq!(m.fs_type, "ext4");
                assert_eq!(m.mount_flags, vec!["noatime".to_string()]);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            cap.access_mode.unwrap().mode,
            proto::volume_capability::access_mode::Mode::SingleNodeWriter as i32
        );

        // A second pod on the node shares the staged volume.
        let pod2 = claim_pod("uid-2", "app-2");
        rig.mgr.resolve_volumes(&pod2).await.unwrap();

        // The first pod goes: unpublished, and not unstaged, because the
        // second still has it.
        rig.mgr.stop_pod("uid-1").await.unwrap();
        assert_eq!(rig.driver.calls(), vec!["stage", "publish", "stage", "publish", "unpublish"]);
        assert!(!Path::new(&target).exists());
        // The last one goes: unstaged.
        rig.mgr.stop_pod("uid-2").await.unwrap();
        assert_eq!(
            rig.driver.calls(),
            vec!["stage", "publish", "stage", "publish", "unpublish", "unpublish", "unstage"]
        );
        assert!(csi_records(&rig.mgr.state_root).is_empty());
    }

    #[tokio::test]
    async fn a_mount_the_node_cannot_see_is_refused_not_given_as_scratch() {
        let rig = rig().await;
        store_claim(&rig.api);
        attach(&rig.api);
        rig.csi.scan_once().await;
        // The driver mounts in its own namespace and nothing propagates.
        rig.driver.propagates.store(false, std::sync::atomic::Ordering::SeqCst);
        let m = not_ready(rig.mgr.resolve_volumes(&claim_pod("uid-1", "app-1")).await);
        assert!(m.contains("not visible on the node"), "{m}");
        assert!(m.contains("Bidirectional"), "{m}");
        // The record is there, so the sweep can undo the publish once the pod is gone.
        assert_eq!(csi_records(&rig.mgr.state_root).len(), 1);
        rig.api.put("/api/v1/pods", json!({"items": []}));
        rig.mgr.sweep_csi_volumes().await;
        assert!(rig.driver.calls().ends_with(&["unpublish".to_string(), "unstage".to_string()]));
        assert!(csi_records(&rig.mgr.state_root).is_empty());
    }

    #[tokio::test]
    async fn the_sweep_leaves_a_live_pods_volumes_alone() {
        let rig = rig().await;
        store_claim(&rig.api);
        attach(&rig.api);
        rig.csi.scan_once().await;
        let pod = claim_pod("uid-1", "app-1");
        rig.mgr.resolve_volumes(&pod).await.unwrap();
        // Bound here and not finished, per the apiserver: a pod mid-start.
        rig.api.put("/api/v1/pods", json!({"items": [pod]}));
        rig.mgr.sweep_csi_volumes().await;
        assert_eq!(rig.driver.calls(), vec!["stage", "publish"]);
        assert_eq!(csi_records(&rig.mgr.state_root).len(), 1);
    }

    #[tokio::test]
    async fn an_inline_volume_is_published_only_for_an_ephemeral_capable_driver() {
        let rig = rig().await;
        rig.csi.scan_once().await;
        let pod = json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "uid-9"},
            "spec": {"nodeName": NODE, "volumes": [
                {"name": "scratch", "csi": {"driver": DRIVER, "volumeAttributes": {"size": "1Gi"}}}
            ]},
        });
        rig.api.put(
            &format!("/apis/storage.k8s.io/v1/csidrivers/{DRIVER}"),
            json!({"spec": {"attachRequired": false, "volumeLifecycleModes": ["Persistent"]}}),
        );
        let m = not_ready(rig.mgr.resolve_volumes(&pod).await);
        assert!(m.contains("Ephemeral"), "{m}");

        rig.api.put(
            &format!("/apis/storage.k8s.io/v1/csidrivers/{DRIVER}"),
            json!({"spec": {"attachRequired": false, "volumeLifecycleModes": ["Ephemeral"]}}),
        );
        rig.mgr.resolve_volumes(&pod).await.unwrap();
        // Publish only: an inline volume is neither attached nor staged.
        assert_eq!(rig.driver.calls(), vec!["publish"]);
        let publish = rig.driver.publish.lock().unwrap().clone().unwrap();
        assert_eq!(publish.volume_context["csi.storage.k8s.io/ephemeral"], "true");
        assert_eq!(publish.volume_id, format!("csi-{}", csi::sha256_hex("uid-9scratch")));
        rig.mgr.stop_pod("uid-9").await.unwrap();
        assert_eq!(rig.driver.calls(), vec!["publish", "unpublish"]);
    }

    #[tokio::test]
    async fn a_generic_ephemeral_volume_waits_for_its_claim_by_name() {
        let rig = rig().await;
        let pod = json!({
            "metadata": {"name": "web", "namespace": "default", "uid": "uid-e"},
            "spec": {"nodeName": NODE, "volumes": [
                {"name": "cache", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}
            ]},
        });
        let m = not_ready(rig.mgr.resolve_volumes(&pod).await);
        assert!(m.contains("default/web-cache"), "{m}");
    }
}
