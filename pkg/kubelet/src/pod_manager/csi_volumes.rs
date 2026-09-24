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
