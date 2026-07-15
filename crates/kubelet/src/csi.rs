//! CSI (Container Storage Interface) client for volume management.
//!
//! Provides trait-based abstractions for CSI Identity, Node, and Controller
//! services. The kubelet primarily uses the Node service for staging and
//! publishing volumes to pods.
//!
//! # Architecture
//!
//! CSI drivers expose three gRPC services:
//!
//! - **Identity**: Plugin metadata and health checks
//! - **Node**: Volume staging and publishing (kubelet side)
//! - **Controller**: Volume creation, deletion, and attachment (control plane side)
//!
//! # Volume Lifecycle
//!
//! When a pod needs a volume:
//!
//! 1. **CreateVolume** (Controller) — Provision storage on backend
//! 2. **ControllerPublishVolume** (Controller) — Attach volume to node
//! 3. **NodeStageVolume** (Node) — Mount volume to global staging directory
//! 4. **NodePublishVolume** (Node) — Bind mount to pod directory
//!
//! Teardown is the reverse:
//!
//! 1. **NodeUnpublishVolume** (Node) — Unmount from pod directory
//! 2. **NodeUnstageVolume** (Node) — Unmount from global staging directory
//! 3. **ControllerUnpublishVolume** (Controller) — Detach from node
//! 4. **DeleteVolume** (Controller) — Delete storage
//!
//! # Example Usage
//!
//! ```no_run
//! use rk_kubelet::csi::{UnixCsiClient, setup_volume, teardown_volume};
//! use std::path::PathBuf;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Connect to CSI driver socket
//! let client = UnixCsiClient::new(
//!     PathBuf::from("/var/lib/kubelet/plugins/csi-driver/csi.sock"),
//!     "driver.example.com".to_string(),
//!     "node-1".to_string(),
//! );
//!
//! // Setup volume for pod
//! setup_volume(
//!     &client,
//!     "vol-12345",
//!     "/var/lib/kubelet/plugins/kubernetes.io/csi/vol-12345/globalmount",
//!     "/var/lib/kubelet/pods/abc-123/volumes/csi/vol-12345",
//!     "ext4",
//!     false,
//! )
//! .await?;
//!
//! // Pod uses volume...
//!
//! // Teardown when pod terminates
//! teardown_volume(
//!     &client,
//!     "vol-12345",
//!     "/var/lib/kubelet/plugins/kubernetes.io/csi/vol-12345/globalmount",
//!     "/var/lib/kubelet/pods/abc-123/volumes/csi/vol-12345",
//! )
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Current Implementation
//!
//! This module provides trait-based abstractions and a **stub implementation**
//! (`UnixCsiClient`) that logs operations but does not communicate with real
//! CSI drivers. A production implementation would:
//!
//! - Use tonic/gRPC to communicate over the Unix domain socket
//! - Parse CSI protobuf messages (from csi.proto)
//! - Handle CSI error codes and retries
//! - Implement actual mount/unmount operations
//!
//! The stub is sufficient for testing and development of the kubelet's volume
//! management logic.
//!
//! # References
//!
//! - CSI specification: https://github.com/container-storage-interface/spec
//! - Kubernetes CSI documentation: https://kubernetes-csi.github.io/docs/

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{info, warn};

// ============================================================================
// Identity Service
// ============================================================================

/// CSI plugin metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsiPluginInfo {
    pub name: String,
    pub vendor_version: String,
    pub manifest: HashMap<String, String>,
}

/// CSI plugin capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CsiCapability {
    ControllerService,
    VolumeAccessibilityConstraints,
    OnlineExpansion,
    OfflineExpansion,
}

/// CSI Identity service — plugin metadata and health.
#[async_trait]
pub trait CsiIdentity: Send + Sync {
    /// Get plugin name and version.
    async fn get_plugin_info(&self) -> Result<CsiPluginInfo>;

    /// Get plugin capabilities.
    async fn get_plugin_capabilities(&self) -> Result<Vec<CsiCapability>>;

    /// Health check — returns true if plugin is ready.
    async fn probe(&self) -> Result<bool>;
}

// ============================================================================
// Node Service
// ============================================================================

/// Request to stage a volume on the node (first mount step).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStageVolumeRequest {
    pub volume_id: String,
    pub publish_context: HashMap<String, String>,
    pub staging_target_path: String,
    pub volume_capability: VolumeCapability,
    pub secrets: HashMap<String, String>,
    pub volume_context: HashMap<String, String>,
}

/// Request to unstage a volume from the node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUnstageVolumeRequest {
    pub volume_id: String,
    pub staging_target_path: String,
}

/// Request to publish a volume to a pod (second mount step).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePublishVolumeRequest {
    pub volume_id: String,
    pub publish_context: HashMap<String, String>,
    pub staging_target_path: String,
    pub target_path: String,
    pub volume_capability: VolumeCapability,
    pub readonly: bool,
    pub secrets: HashMap<String, String>,
    pub volume_context: HashMap<String, String>,
}

/// Request to unpublish a volume from a pod.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUnpublishVolumeRequest {
    pub volume_id: String,
    pub target_path: String,
}

/// Volume access capability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeCapability {
    pub access_type: AccessType,
    pub access_mode: AccessMode,
}

/// How the volume is accessed (block vs filesystem).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessType {
    Block,
    Mount { fs_type: String, mount_flags: Vec<String> },
}

/// Volume access mode (RWO, ROX, RWX, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AccessMode {
    SingleNodeWriter,
    SingleNodeReaderOnly,
    MultiNodeReaderOnly,
    MultiNodeSingleWriter,
    MultiNodeMultiWriter,
}

/// Node service capability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeCapability {
    StageUnstageVolume,
    GetVolumeStats,
    VolumeCondition,
    SingleNodeMultiWriter,
}

/// Node topology and resource info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub max_volumes_per_node: u64,
    pub accessible_topology: HashMap<String, String>,
}

/// CSI Node service — volume staging and publishing.
#[async_trait]
pub trait CsiNode: Send + Sync {
    /// Stage a volume to a global staging directory on the node.
    /// Called once per volume, before any pod mounts.
    async fn node_stage_volume(&self, req: NodeStageVolumeRequest) -> Result<()>;

    /// Unstage a volume from the global staging directory.
    /// Called after all pods have unmounted the volume.
    async fn node_unstage_volume(&self, req: NodeUnstageVolumeRequest) -> Result<()>;

    /// Publish (mount) a volume into a pod's directory.
    /// Called once per pod using the volume.
    async fn node_publish_volume(&self, req: NodePublishVolumeRequest) -> Result<()>;

    /// Unpublish (unmount) a volume from a pod's directory.
    async fn node_unpublish_volume(&self, req: NodeUnpublishVolumeRequest) -> Result<()>;

    /// Get node service capabilities.
    async fn node_get_capabilities(&self) -> Result<Vec<NodeCapability>>;

    /// Get node ID and topology info.
    async fn node_get_info(&self) -> Result<NodeInfo>;
}

// ============================================================================
// Controller Service
// ============================================================================

/// Request to create a volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeRequest {
    pub name: String,
    pub capacity_bytes: u64,
    pub volume_capabilities: Vec<VolumeCapability>,
    pub parameters: HashMap<String, String>,
    pub secrets: HashMap<String, String>,
    pub volume_content_source: Option<VolumeContentSource>,
    pub accessibility_requirements: Option<TopologyRequirement>,
}

/// Volume metadata returned by CreateVolume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    pub volume_id: String,
    pub capacity_bytes: u64,
    pub volume_context: HashMap<String, String>,
    pub content_source: Option<VolumeContentSource>,
    pub accessible_topology: Vec<HashMap<String, String>>,
}

/// Source for volume content (snapshot, clone, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VolumeContentSource {
    Snapshot { snapshot_id: String },
    Volume { volume_id: String },
}

/// Topology requirement for volume placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyRequirement {
    pub requisite: Vec<HashMap<String, String>>,
    pub preferred: Vec<HashMap<String, String>>,
}

/// Request to publish a volume to a node (attach).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerPublishRequest {
    pub volume_id: String,
    pub node_id: String,
    pub volume_capability: VolumeCapability,
    pub readonly: bool,
    pub secrets: HashMap<String, String>,
    pub volume_context: HashMap<String, String>,
}

/// Publish context returned by ControllerPublishVolume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishInfo {
    pub publish_context: HashMap<String, String>,
}

/// Request to unpublish a volume from a node (detach).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerUnpublishRequest {
    pub volume_id: String,
    pub node_id: String,
    pub secrets: HashMap<String, String>,
}

/// Request to validate volume capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateCapabilitiesRequest {
    pub volume_id: String,
    pub volume_context: HashMap<String, String>,
    pub volume_capabilities: Vec<VolumeCapability>,
    pub parameters: HashMap<String, String>,
    pub secrets: HashMap<String, String>,
}

/// CSI Controller service — volume lifecycle and attachment.
#[async_trait]
pub trait CsiController: Send + Sync {
    /// Create a new volume.
    async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume>;

    /// Delete a volume.
    async fn delete_volume(&self, volume_id: &str) -> Result<()>;

    /// Publish (attach) a volume to a node.
    async fn controller_publish_volume(&self, req: ControllerPublishRequest) -> Result<PublishInfo>;

    /// Unpublish (detach) a volume from a node.
    async fn controller_unpublish_volume(&self, req: ControllerUnpublishRequest) -> Result<()>;

    /// Validate that a volume supports the requested capabilities.
    async fn validate_volume_capabilities(&self, req: ValidateCapabilitiesRequest) -> Result<bool>;
}

// ============================================================================
// Unix Domain Socket Client (Stub Implementation)
// ============================================================================

/// CSI client that connects to a driver via Unix domain socket.
///
/// This is a stub implementation that logs operations. In a production
/// implementation, this would use gRPC to communicate with the CSI driver
/// over the socket (typically /var/lib/kubelet/plugins/<driver>/csi.sock).
pub struct UnixCsiClient {
    socket_path: PathBuf,
    plugin_name: String,
    node_id: String,
}

impl UnixCsiClient {
    /// Create a new CSI client connected to the given Unix socket.
    pub fn new(socket_path: PathBuf, plugin_name: String, node_id: String) -> Self {
        Self {
            socket_path,
            plugin_name,
            node_id,
        }
    }

    /// Get the socket path.
    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }
}

#[async_trait]
impl CsiIdentity for UnixCsiClient {
    async fn get_plugin_info(&self) -> Result<CsiPluginInfo> {
        info!("CSI GetPluginInfo: socket={}", self.socket_path.display());

        // Stub: return synthetic plugin info
        Ok(CsiPluginInfo {
            name: self.plugin_name.clone(),
            vendor_version: "0.1.0".to_string(),
            manifest: HashMap::new(),
        })
    }

    async fn get_plugin_capabilities(&self) -> Result<Vec<CsiCapability>> {
        info!("CSI GetPluginCapabilities: socket={}", self.socket_path.display());

        // Stub: assume controller service is available
        Ok(vec![CsiCapability::ControllerService])
    }

    async fn probe(&self) -> Result<bool> {
        info!("CSI Probe: socket={}", self.socket_path.display());

        // Stub: assume plugin is ready if socket exists
        Ok(self.socket_path.exists())
    }
}

#[async_trait]
impl CsiNode for UnixCsiClient {
    async fn node_stage_volume(&self, req: NodeStageVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path,
            "CSI NodeStageVolume"
        );

        // Stub: In production, this would:
        // 1. Call the CSI driver via gRPC
        // 2. The driver would attach the volume to the node
        // 3. Mount it to the staging path

        // For now, just ensure the staging directory exists
        let staging_path = PathBuf::from(&req.staging_target_path);
        if !staging_path.exists() {
            std::fs::create_dir_all(&staging_path)
                .with_context(|| format!("Failed to create staging path: {}", req.staging_target_path))?;
        }

        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path,
            "CSI NodeStageVolume: staged (stub)"
        );

        Ok(())
    }

    async fn node_unstage_volume(&self, req: NodeUnstageVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path,
            "CSI NodeUnstageVolume"
        );

        // Stub: In production, this would:
        // 1. Unmount the volume from the staging path
        // 2. Call the CSI driver to detach/cleanup

        warn!(
            volume_id = %req.volume_id,
            "CSI NodeUnstageVolume: unstaged (stub)"
        );

        Ok(())
    }

    async fn node_publish_volume(&self, req: NodePublishVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path,
            readonly = req.readonly,
            "CSI NodePublishVolume"
        );

        // Stub: In production, this would:
        // 1. Bind-mount from staging_target_path to target_path
        // 2. Apply readonly flag if needed

        let target_path = PathBuf::from(&req.target_path);
        if !target_path.exists() {
            std::fs::create_dir_all(&target_path)
                .with_context(|| format!("Failed to create target path: {}", req.target_path))?;
        }

        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path,
            "CSI NodePublishVolume: published (stub)"
        );

        Ok(())
    }

    async fn node_unpublish_volume(&self, req: NodeUnpublishVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path,
            "CSI NodeUnpublishVolume"
        );

        // Stub: In production, this would unmount the bind mount

        warn!(
            volume_id = %req.volume_id,
            "CSI NodeUnpublishVolume: unpublished (stub)"
        );

        Ok(())
    }

    async fn node_get_capabilities(&self) -> Result<Vec<NodeCapability>> {
        info!("CSI NodeGetCapabilities");

        // Stub: advertise staging support
        Ok(vec![NodeCapability::StageUnstageVolume])
    }

    async fn node_get_info(&self) -> Result<NodeInfo> {
        info!("CSI NodeGetInfo");

        Ok(NodeInfo {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 256,
            accessible_topology: HashMap::new(),
        })
    }
}

#[async_trait]
impl CsiController for UnixCsiClient {
    async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume> {
        info!(
            name = %req.name,
            capacity_bytes = req.capacity_bytes,
            "CSI CreateVolume"
        );

        // Stub: return a synthetic volume ID
        let volume_id = format!("vol-{}", uuid::Uuid::new_v4());

        warn!(
            volume_id = %volume_id,
            name = %req.name,
            "CSI CreateVolume: created (stub)"
        );

        Ok(Volume {
            volume_id,
            capacity_bytes: req.capacity_bytes,
            volume_context: req.parameters,
            content_source: req.volume_content_source,
            accessible_topology: vec![],
        })
    }

    async fn delete_volume(&self, volume_id: &str) -> Result<()> {
        info!(volume_id = %volume_id, "CSI DeleteVolume");

        warn!(volume_id = %volume_id, "CSI DeleteVolume: deleted (stub)");

        Ok(())
    }

    async fn controller_publish_volume(&self, req: ControllerPublishRequest) -> Result<PublishInfo> {
        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            readonly = req.readonly,
            "CSI ControllerPublishVolume"
        );

        // Stub: return empty publish context
        warn!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "CSI ControllerPublishVolume: published (stub)"
        );

        Ok(PublishInfo {
            publish_context: HashMap::new(),
        })
    }

    async fn controller_unpublish_volume(&self, req: ControllerUnpublishRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "CSI ControllerUnpublishVolume"
        );

        warn!(
            volume_id = %req.volume_id,
            "CSI ControllerUnpublishVolume: unpublished (stub)"
        );

        Ok(())
    }

    async fn validate_volume_capabilities(&self, req: ValidateCapabilitiesRequest) -> Result<bool> {
        info!(
            volume_id = %req.volume_id,
            num_capabilities = req.volume_capabilities.len(),
            "CSI ValidateVolumeCapabilities"
        );

        // Stub: assume all capabilities are valid
        Ok(true)
    }
}

// ============================================================================
// Volume Lifecycle Helpers
// ============================================================================

/// Setup a volume for use by a pod.
///
/// This performs the full CSI node workflow:
/// 1. NodeStageVolume (global mount)
/// 2. NodePublishVolume (bind mount to pod)
pub async fn setup_volume(
    csi: &dyn CsiNode,
    volume_id: &str,
    staging_path: &str,
    target_path: &str,
    fs_type: &str,
    readonly: bool,
) -> Result<()> {
    info!(
        volume_id = %volume_id,
        staging_path = %staging_path,
        target_path = %target_path,
        fs_type = %fs_type,
        readonly = readonly,
        "Setting up CSI volume"
    );

    // Stage the volume (global mount)
    csi.node_stage_volume(NodeStageVolumeRequest {
        volume_id: volume_id.to_string(),
        publish_context: HashMap::new(),
        staging_target_path: staging_path.to_string(),
        volume_capability: VolumeCapability {
            access_type: AccessType::Mount {
                fs_type: fs_type.to_string(),
                mount_flags: vec![],
            },
            access_mode: AccessMode::SingleNodeWriter,
        },
        secrets: HashMap::new(),
        volume_context: HashMap::new(),
    })
    .await
    .with_context(|| format!("Failed to stage volume {}", volume_id))?;

    // Publish the volume (bind mount to pod)
    csi.node_publish_volume(NodePublishVolumeRequest {
        volume_id: volume_id.to_string(),
        publish_context: HashMap::new(),
        staging_target_path: staging_path.to_string(),
        target_path: target_path.to_string(),
        volume_capability: VolumeCapability {
            access_type: AccessType::Mount {
                fs_type: fs_type.to_string(),
                mount_flags: vec![],
            },
            access_mode: AccessMode::SingleNodeWriter,
        },
        readonly,
        secrets: HashMap::new(),
        volume_context: HashMap::new(),
    })
    .await
    .with_context(|| format!("Failed to publish volume {} to {}", volume_id, target_path))?;

    info!(
        volume_id = %volume_id,
        target_path = %target_path,
        "CSI volume setup complete"
    );

    Ok(())
}

/// Teardown a volume after pod termination.
///
/// This performs the full CSI node cleanup:
/// 1. NodeUnpublishVolume (remove bind mount)
/// 2. NodeUnstageVolume (unmount global mount)
pub async fn teardown_volume(
    csi: &dyn CsiNode,
    volume_id: &str,
    staging_path: &str,
    target_path: &str,
) -> Result<()> {
    info!(
        volume_id = %volume_id,
        staging_path = %staging_path,
        target_path = %target_path,
        "Tearing down CSI volume"
    );

    // Unpublish the volume (remove bind mount)
    if let Err(e) = csi
        .node_unpublish_volume(NodeUnpublishVolumeRequest {
            volume_id: volume_id.to_string(),
            target_path: target_path.to_string(),
        })
        .await
    {
        warn!(
            volume_id = %volume_id,
            target_path = %target_path,
            error = %e,
            "Failed to unpublish volume (continuing)"
        );
    }

    // Unstage the volume (unmount global mount)
    if let Err(e) = csi
        .node_unstage_volume(NodeUnstageVolumeRequest {
            volume_id: volume_id.to_string(),
            staging_target_path: staging_path.to_string(),
        })
        .await
    {
        warn!(
            volume_id = %volume_id,
            staging_path = %staging_path,
            error = %e,
            "Failed to unstage volume (continuing)"
        );
    }

    info!(
        volume_id = %volume_id,
        "CSI volume teardown complete"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unix_csi_client_identity() {
        let client = UnixCsiClient::new(
            PathBuf::from("/tmp/csi.sock"),
            "test-driver".to_string(),
            "node-1".to_string(),
        );

        let info = client.get_plugin_info().await.unwrap();
        assert_eq!(info.name, "test-driver");

        let caps = client.get_plugin_capabilities().await.unwrap();
        assert!(caps.contains(&CsiCapability::ControllerService));
    }

    #[tokio::test]
    async fn test_unix_csi_client_node() {
        let client = UnixCsiClient::new(
            PathBuf::from("/tmp/csi.sock"),
            "test-driver".to_string(),
            "node-1".to_string(),
        );

        let info = client.node_get_info().await.unwrap();
        assert_eq!(info.node_id, "node-1");
        assert_eq!(info.max_volumes_per_node, 256);

        let caps = client.node_get_capabilities().await.unwrap();
        assert!(caps.contains(&NodeCapability::StageUnstageVolume));
    }

    #[tokio::test]
    async fn test_volume_lifecycle() {
        let client = UnixCsiClient::new(
            PathBuf::from("/tmp/csi.sock"),
            "test-driver".to_string(),
            "node-1".to_string(),
        );

        let tempdir = tempfile::tempdir().unwrap();
        let staging_path = tempdir.path().join("staging");
        let target_path = tempdir.path().join("target");

        // Setup should succeed (stub creates directories)
        setup_volume(
            &client,
            "vol-123",
            staging_path.to_str().unwrap(),
            target_path.to_str().unwrap(),
            "ext4",
            false,
        )
        .await
        .unwrap();

        assert!(staging_path.exists());
        assert!(target_path.exists());

        // Teardown should succeed
        teardown_volume(
            &client,
            "vol-123",
            staging_path.to_str().unwrap(),
            target_path.to_str().unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_controller_operations() {
        let client = UnixCsiClient::new(
            PathBuf::from("/tmp/csi.sock"),
            "test-driver".to_string(),
            "node-1".to_string(),
        );

        // Create volume
        let vol = client
            .create_volume(CreateVolumeRequest {
                name: "test-vol".to_string(),
                capacity_bytes: 1024 * 1024 * 1024, // 1 GiB
                volume_capabilities: vec![VolumeCapability {
                    access_type: AccessType::Mount {
                        fs_type: "ext4".to_string(),
                        mount_flags: vec![],
                    },
                    access_mode: AccessMode::SingleNodeWriter,
                }],
                parameters: HashMap::new(),
                secrets: HashMap::new(),
                volume_content_source: None,
                accessibility_requirements: None,
            })
            .await
            .unwrap();

        assert!(vol.volume_id.starts_with("vol-"));
        assert_eq!(vol.capacity_bytes, 1024 * 1024 * 1024);

        // Publish volume
        let publish_info = client
            .controller_publish_volume(ControllerPublishRequest {
                volume_id: vol.volume_id.clone(),
                node_id: "node-1".to_string(),
                volume_capability: VolumeCapability {
                    access_type: AccessType::Mount {
                        fs_type: "ext4".to_string(),
                        mount_flags: vec![],
                    },
                    access_mode: AccessMode::SingleNodeWriter,
                },
                readonly: false,
                secrets: HashMap::new(),
                volume_context: HashMap::new(),
            })
            .await
            .unwrap();

        assert!(publish_info.publish_context.is_empty());

        // Unpublish volume
        client
            .controller_unpublish_volume(ControllerUnpublishRequest {
                volume_id: vol.volume_id.clone(),
                node_id: "node-1".to_string(),
                secrets: HashMap::new(),
            })
            .await
            .unwrap();

        // Delete volume
        client.delete_volume(&vol.volume_id).await.unwrap();
    }
}
