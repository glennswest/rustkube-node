//! The CSI node client: how the kubelet talks to another driver's node plugin.
//!
//! **This was a stub** that logged each call and created the directories, and
//! it never spoke to a driver. A claim of another StorageClass therefore could
//! not mount, and the module read as though it could (#52). It is now the real
//! thing: gRPC over the driver's Unix socket, speaking the vendored CSI v1.9
//! protocol (`proto/csi/csi.proto`).
//!
//! Only the Identity and Node services are here. The Controller service
//! (CreateVolume, ControllerPublish) is not the kubelet's to call. A driver's
//! external-provisioner and external-attacher sidecars call it, prompted by
//! the PVC and by the `VolumeAttachment` that rustkube's attach/detach
//! controller writes.
//!
//! # The node side of a volume
//!
//! 1. **NodeStageVolume**, once per volume per node, when the driver advertises
//!    `STAGE_UNSTAGE_VOLUME`: the driver mounts the device at a global staging
//!    path, `<root>/plugins/kubernetes.io/csi/<driver>/<sha256(handle)>/globalmount`.
//! 2. **NodePublishVolume**, once per pod: the driver makes the volume appear at
//!    `<root>/pods/<uid>/volumes/kubernetes.io~csi/<volume>/mount`.
//! 3. NodeUnpublishVolume when the pod goes, and NodeUnstageVolume when the
//!    last pod on the node lets go.
//!
//! Where the driver's mounts land (its namespace or the node's) is the part
//! that decides whether this works at all. See `docs/csi.md`.

use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};

/// The generated CSI v1 messages and services.
pub mod proto {
    tonic::include_proto!("csi.v1");
}

use proto::identity_client::IdentityClient;
use proto::node_client::NodeClient;
use proto::node_service_capability::rpc::Type as NodeRpc;
use proto::volume_capability::access_mode::Mode;

/// How long one call to a driver may take.
///
/// NodeStage of a network volume can legitimately take a while: a connect
/// and a filesystem check. A call that never returns would stall the pod
/// sync loop that made it. Two minutes matches upstream's `csiTimeout`.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// A gRPC channel over a Unix socket.
///
/// Lazy, so a driver that is restarting is an error on the call that needs it,
/// not at construction. tonic wants a URI, and the connector ignores it.
pub fn unix_channel(path: &Path, timeout: Duration) -> Channel {
    let path = path.to_path_buf();
    Endpoint::from_static("http://[::1]:50051")
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(5))
        .connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
}

/// What a driver says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginInfo {
    pub name: String,
    pub vendor_version: String,
}

/// What the node plugin says about this node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeInfo {
    /// The driver's own name for this node. This is what the attacher passes to
    /// ControllerPublishVolume, via `CSINode`, and it is often not the
    /// Kubernetes node name.
    pub node_id: String,
    /// 0 means the driver sets no limit.
    pub max_volumes_per_node: i64,
    /// Topology segments (e.g. `topology.hostpath.csi/node: n1`). They become
    /// node labels, and their keys go in `CSINode`, so a topology-aware
    /// provisioner can place a volume where this node can reach it.
    pub topology: HashMap<String, String>,
}

/// The node capabilities the kubelet acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NodeCapabilities {
    /// NodeStage/NodeUnstage are implemented, so a volume is staged once per
    /// node before it is published per pod. Without it, publish is the only
    /// step.
    pub stage_unstage: bool,
    /// The driver understands SINGLE_NODE_SINGLE_WRITER and
    /// SINGLE_NODE_MULTI_WRITER, so ReadWriteOncePod can be said precisely.
    pub single_node_multi_writer: bool,
}

/// A PersistentVolume access mode, in CSI terms.
///
/// Upstream maps the PV's *first* access mode and so does this, because a
/// driver given a mode its volume was not provisioned for may refuse the
/// publish.
pub fn access_mode(pv_mode: &str, caps: NodeCapabilities) -> Mode {
    match pv_mode {
        "ReadOnlyMany" => Mode::MultiNodeReaderOnly,
        "ReadWriteMany" => Mode::MultiNodeMultiWriter,
        "ReadWriteOncePod" if caps.single_node_multi_writer => Mode::SingleNodeSingleWriter,
        "ReadWriteOnce" if caps.single_node_multi_writer => Mode::SingleNodeMultiWriter,
        _ => Mode::SingleNodeWriter,
    }
}

/// Everything the node plugin needs to know about one volume.
#[derive(Debug, Clone, Default)]
pub struct VolumeSpec {
    pub volume_id: String,
    /// `""` lets the driver choose.
    pub fs_type: String,
    pub mount_flags: Vec<String>,
    pub access_mode: i32,
    pub readonly: bool,
    /// `PV.spec.csi.volumeAttributes`, plus the pod's identity when the
    /// CSIDriver asks for it (`podInfoOnMount`).
    pub volume_context: HashMap<String, String>,
    /// `VolumeAttachment.status.attachmentMetadata`: whatever the
    /// controller's publish told the node (a device path or a LUN).
    pub publish_context: HashMap<String, String>,
    pub stage_secrets: HashMap<String, String>,
    pub publish_secrets: HashMap<String, String>,
}

impl VolumeSpec {
    fn capability(&self) -> proto::VolumeCapability {
        proto::VolumeCapability {
            access_type: Some(proto::volume_capability::AccessType::Mount(
                proto::volume_capability::MountVolume {
                    fs_type: self.fs_type.clone(),
                    mount_flags: self.mount_flags.clone(),
                    volume_mount_group: String::new(),
                },
            )),
            access_mode: Some(proto::volume_capability::AccessMode { mode: self.access_mode }),
        }
    }
}

/// A connection to one driver's node plugin.
#[derive(Clone)]
pub struct CsiDriverClient {
    socket: PathBuf,
    channel: Channel,
}

impl CsiDriverClient {
    pub fn new(socket: &Path) -> Self {
        Self { socket: socket.to_path_buf(), channel: unix_channel(socket, CALL_TIMEOUT) }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    fn identity(&self) -> IdentityClient<Channel> {
        IdentityClient::new(self.channel.clone())
    }

    fn node(&self) -> NodeClient<Channel> {
        NodeClient::new(self.channel.clone())
    }

    fn at(&self) -> String {
        self.socket.display().to_string()
    }

    pub async fn plugin_info(&self) -> Result<PluginInfo> {
        let r = self
            .identity()
            .get_plugin_info(proto::GetPluginInfoRequest {})
            .await
            .with_context(|| format!("GetPluginInfo on {}", self.at()))?
            .into_inner();
        Ok(PluginInfo { name: r.name, vendor_version: r.vendor_version })
    }

    /// Whether the plugin says it is ready. An unset `ready` means ready,
    /// per the spec.
    pub async fn probe(&self) -> Result<bool> {
        let r = self
            .identity()
            .probe(proto::ProbeRequest {})
            .await
            .with_context(|| format!("Probe on {}", self.at()))?
            .into_inner();
        Ok(r.ready.unwrap_or(true))
    }

    pub async fn node_info(&self) -> Result<NodeInfo> {
        let r = self
            .node()
            .node_get_info(proto::NodeGetInfoRequest {})
            .await
            .with_context(|| format!("NodeGetInfo on {}", self.at()))?
            .into_inner();
        if r.node_id.is_empty() {
            return Err(anyhow!("NodeGetInfo on {} returned an empty node_id", self.at()));
        }
        Ok(NodeInfo {
            node_id: r.node_id,
            max_volumes_per_node: r.max_volumes_per_node,
            topology: r.accessible_topology.map(|t| t.segments).unwrap_or_default(),
        })
    }

    pub async fn node_capabilities(&self) -> Result<NodeCapabilities> {
        let r = self
            .node()
            .node_get_capabilities(proto::NodeGetCapabilitiesRequest {})
            .await
            .with_context(|| format!("NodeGetCapabilities on {}", self.at()))?
            .into_inner();
        let mut caps = NodeCapabilities::default();
        for c in r.capabilities {
            if let Some(proto::node_service_capability::Type::Rpc(rpc)) = c.r#type {
                match NodeRpc::try_from(rpc.r#type) {
                    Ok(NodeRpc::StageUnstageVolume) => caps.stage_unstage = true,
                    Ok(NodeRpc::SingleNodeMultiWriter) => caps.single_node_multi_writer = true,
                    _ => {}
                }
            }
        }
        Ok(caps)
    }

    pub async fn stage(&self, v: &VolumeSpec, staging: &str) -> Result<()> {
        self.node()
            .node_stage_volume(proto::NodeStageVolumeRequest {
                volume_id: v.volume_id.clone(),
                publish_context: v.publish_context.clone(),
                staging_target_path: staging.to_string(),
                volume_capability: Some(v.capability()),
                secrets: v.stage_secrets.clone(),
                volume_context: v.volume_context.clone(),
            })
            .await
            .map_err(|s| status_error("NodeStageVolume", &v.volume_id, s))?;
        Ok(())
    }

    pub async fn unstage(&self, volume_id: &str, staging: &str) -> Result<()> {
        self.node()
            .node_unstage_volume(proto::NodeUnstageVolumeRequest {
                volume_id: volume_id.to_string(),
                staging_target_path: staging.to_string(),
            })
            .await
            .map_err(|s| status_error("NodeUnstageVolume", volume_id, s))?;
        Ok(())
    }

    /// `staging` is `None` for a driver without STAGE_UNSTAGE_VOLUME.
    pub async fn publish(&self, v: &VolumeSpec, staging: Option<&str>, target: &str) -> Result<()> {
        self.node()
            .node_publish_volume(proto::NodePublishVolumeRequest {
                volume_id: v.volume_id.clone(),
                publish_context: v.publish_context.clone(),
                staging_target_path: staging.unwrap_or("").to_string(),
                target_path: target.to_string(),
                volume_capability: Some(v.capability()),
                readonly: v.readonly,
                secrets: v.publish_secrets.clone(),
                volume_context: v.volume_context.clone(),
            })
            .await
            .map_err(|s| status_error("NodePublishVolume", &v.volume_id, s))?;
        Ok(())
    }

    pub async fn unpublish(&self, volume_id: &str, target: &str) -> Result<()> {
        self.node()
            .node_unpublish_volume(proto::NodeUnpublishVolumeRequest {
                volume_id: volume_id.to_string(),
                target_path: target.to_string(),
            })
            .await
            .map_err(|s| status_error("NodeUnpublishVolume", volume_id, s))?;
        Ok(())
    }
}

/// A driver's refusal, readable in `describe`: the call, the volume, the
/// gRPC code and the driver's own message. A raw `Status` debug-prints its
/// metadata and buries the one sentence that matters.
fn status_error(call: &str, volume_id: &str, s: tonic::Status) -> anyhow::Error {
    anyhow!("{call} {volume_id}: {:?}: {}", s.code(), s.message())
}

/// Stage (when the driver stages) and then publish.
///
/// Both calls are idempotent by the CSI contract, so this is safe to repeat.
/// A pod that retries its start after one volume failed calls it again for
/// the ones that succeeded.
pub async fn setup_volume(
    client: &CsiDriverClient,
    v: &VolumeSpec,
    staging: Option<&str>,
    target: &str,
) -> Result<()> {
    if let Some(staging) = staging {
        client.stage(v, staging).await?;
    }
    client.publish(v, staging, target).await
}

/// `sha256(input)`, lower-case hex.
pub fn sha256_hex(input: &str) -> String {
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

/// The VolumeAttachment name for a volume on a node: `csi-` + sha256 of
/// handle, driver and node, concatenated.
///
/// **It has to match what the attach/detach controller wrote**
/// (rustkube `attachdetach.rs::attachment_name`, which is upstream's
/// `getAttachmentName`), because the kubelet does not list attachments. It
/// GETs this one by name.
pub fn attachment_name(volume_handle: &str, driver: &str, node: &str) -> String {
    format!("csi-{}", sha256_hex(&format!("{volume_handle}{driver}{node}")))
}

/// The global staging path for a volume, upstream's layout:
/// `<root>/plugins/kubernetes.io/csi/<driver>/<sha256(handle)>/globalmount`.
///
/// Hashed because a volume handle is the driver's string and may hold
/// anything, `/` included.
pub fn staging_path(state_root: &str, driver: &str, volume_handle: &str) -> String {
    format!(
        "{}/plugins/kubernetes.io/csi/{driver}/{}/globalmount",
        state_root.trim_end_matches('/'),
        sha256_hex(volume_handle)
    )
}

/// Where a pod's CSI volume is published:
/// `<root>/pods/<uid>/volumes/kubernetes.io~csi/<volume>/mount`.
pub fn publish_path(state_root: &str, pod_uid: &str, volume: &str) -> String {
    format!(
        "{}/pods/{pod_uid}/volumes/kubernetes.io~csi/{volume}/mount",
        state_root.trim_end_matches('/')
    )
}

/// Is `path` a mount point in PID 1's mount namespace?
///
/// **This is the check that stops a CSI volume from becoming scratch.** The
/// driver mounts in its own namespace. If the mount does not propagate to
/// the node's namespace, where stormpump resolves binds, the directory still
/// exists (the driver created it on the shared filesystem) and the pod's
/// bind finds it empty. The pod would then write its data onto the node's
/// root and lose it with the pod, which is exactly what 5236dbe stopped for
/// stormblock claims. So the kubelet looks where the engine will look.
///
/// `mountinfo` is PID 1's `/proc/1/mountinfo`, read through the host PID
/// namespace the kubelet shares.
pub fn is_mount_point(mountinfo: &str, path: &str) -> bool {
    let path = path.trim_end_matches('/');
    mountinfo.lines().any(|l| {
        // Field 5 is the mount point, with spaces and friends octal-escaped.
        l.split(' ').nth(4).map(unescape_mountinfo).as_deref() == Some(path)
    })
}

fn unescape_mountinfo(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
            let v = (b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0');
            out.push(v);
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_name_matches_the_controllers() {
        // The same vector rustkube's attachdetach.rs tests: sha256 of
        // "volhandle" + "csi.example.com" + "node1".
        let a = attachment_name("volhandle", "csi.example.com", "node1");
        assert!(a.starts_with("csi-") && a.len() == 68);
        assert_eq!(a, format!("csi-{}", sha256_hex("volhandlecsi.example.comnode1")));
        assert_eq!(
            sha256_hex(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn paths_follow_upstreams_layout() {
        assert_eq!(
            staging_path("/var/lib/kubelet/", "hostpath.csi.k8s.io", ""),
            "/var/lib/kubelet/plugins/kubernetes.io/csi/hostpath.csi.k8s.io/\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/globalmount"
        );
        assert_eq!(
            publish_path("/var/lib/kubelet", "u1", "data"),
            "/var/lib/kubelet/pods/u1/volumes/kubernetes.io~csi/data/mount"
        );
    }

    #[test]
    fn access_modes_map_as_upstream_does() {
        let plain = NodeCapabilities::default();
        let multi = NodeCapabilities { single_node_multi_writer: true, ..plain };
        assert_eq!(access_mode("ReadWriteOnce", plain), Mode::SingleNodeWriter);
        assert_eq!(access_mode("ReadWriteOnce", multi), Mode::SingleNodeMultiWriter);
        // Without the capability the driver cannot be told "one pod", so it
        // is told "one node", and the kubelet's own RWOP check holds the rest.
        assert_eq!(access_mode("ReadWriteOncePod", plain), Mode::SingleNodeWriter);
        assert_eq!(access_mode("ReadWriteOncePod", multi), Mode::SingleNodeSingleWriter);
        assert_eq!(access_mode("ReadOnlyMany", plain), Mode::MultiNodeReaderOnly);
        assert_eq!(access_mode("ReadWriteMany", plain), Mode::MultiNodeMultiWriter);
    }

    #[test]
    fn a_mount_point_is_found_in_pid_1s_mountinfo() {
        let mi = "\
22 1 0:21 / / rw,relatime shared:1 - erofs /dev/vda ro
140 22 0:52 / /var/lib/kubelet/pods/u1/volumes/kubernetes.io~csi/data/mount rw shared:70 - tmpfs tmpfs rw
141 22 0:53 / /mnt/with\\040space rw - tmpfs tmpfs rw
";
        assert!(is_mount_point(mi, "/var/lib/kubelet/pods/u1/volumes/kubernetes.io~csi/data/mount"));
        assert!(is_mount_point(mi, "/var/lib/kubelet/pods/u1/volumes/kubernetes.io~csi/data/mount/"));
        assert!(is_mount_point(mi, "/mnt/with space"));
        // The directory exists, and nothing is mounted on it: the case that
        // would hand a pod scratch storage.
        assert!(!is_mount_point(mi, "/var/lib/kubelet/pods/u2/volumes/kubernetes.io~csi/data/mount"));
        assert!(!is_mount_point(mi, "/var/lib/kubelet"));
    }
}
