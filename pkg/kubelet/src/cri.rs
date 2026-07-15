//! CRI (Container Runtime Interface) client.
//!
//! Defines the CRI gRPC client types matching the K8s CRI v1 API.
//! Connects to containerd or CRI-O via Unix socket.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CRI container state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerState {
    Created = 0,
    Running = 1,
    Exited = 2,
    Unknown = 3,
}

/// CRI pod sandbox state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodSandboxState {
    Ready = 0,
    NotReady = 1,
}

/// Pod sandbox configuration.
#[derive(Debug, Clone)]
pub struct PodSandboxConfig {
    pub name: String,
    pub uid: String,
    pub namespace: String,
    pub attempt: u32,
    pub hostname: String,
    pub log_directory: String,
    pub dns_servers: Vec<String>,
    pub dns_searches: Vec<String>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub port_mappings: Vec<PortMapping>,
}

/// Port mapping for a pod sandbox.
#[derive(Debug, Clone)]
pub struct PortMapping {
    pub protocol: String,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

/// Container configuration.
#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub name: String,
    pub attempt: u32,
    pub image: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: String,
    pub envs: Vec<(String, String)>,
    pub mounts: Vec<Mount>,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub log_path: String,
    pub stdin: bool,
    pub tty: bool,
    pub cpu_period: i64,
    pub cpu_quota: i64,
    pub cpu_shares: i64,
    pub memory_limit_bytes: i64,
}

/// Mount specification.
#[derive(Debug, Clone)]
pub struct Mount {
    pub container_path: String,
    pub host_path: String,
    pub readonly: bool,
}

/// Container status information.
#[derive(Debug, Clone)]
pub struct ContainerStatusInfo {
    pub id: String,
    pub name: String,
    pub state: ContainerState,
    pub created_at: i64,
    pub started_at: i64,
    pub finished_at: i64,
    pub exit_code: i32,
    pub image: String,
    pub image_ref: String,
    pub reason: String,
    pub message: String,
}

/// Pod sandbox status.
#[derive(Debug, Clone)]
pub struct PodSandboxStatusInfo {
    pub id: String,
    pub state: PodSandboxState,
    pub created_at: i64,
    pub ip: String,
    pub additional_ips: Vec<String>,
}

/// Image information.
#[derive(Debug, Clone)]
pub struct ImageInfo {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size: u64,
}

/// Exec sync result.
#[derive(Debug, Clone)]
pub struct ExecSyncResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

/// CRI RuntimeService trait.
#[async_trait]
pub trait RuntimeService: Send + Sync + 'static {
    /// Get runtime version info.
    async fn version(&self) -> Result<(String, String, String), CriError>;

    /// Create and start a pod sandbox. Returns the sandbox ID.
    async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError>;

    /// Stop a pod sandbox.
    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError>;

    /// Remove a pod sandbox.
    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError>;

    /// Get pod sandbox status.
    async fn pod_sandbox_status(
        &self,
        sandbox_id: &str,
    ) -> Result<PodSandboxStatusInfo, CriError>;

    /// List pod sandboxes.
    async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError>;

    /// Create a container in a sandbox. Returns container ID.
    async fn create_container(
        &self,
        sandbox_id: &str,
        config: &ContainerConfig,
        sandbox_config: &PodSandboxConfig,
    ) -> Result<String, CriError>;

    /// Start a container.
    async fn start_container(&self, container_id: &str) -> Result<(), CriError>;

    /// Stop a container.
    async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError>;

    /// Remove a container.
    async fn remove_container(&self, container_id: &str) -> Result<(), CriError>;

    /// Get container status.
    async fn container_status(
        &self,
        container_id: &str,
    ) -> Result<ContainerStatusInfo, CriError>;

    /// List containers.
    async fn list_containers(
        &self,
        sandbox_id: Option<&str>,
    ) -> Result<Vec<ContainerStatusInfo>, CriError>;

    /// Execute a command synchronously in a container.
    async fn exec_sync(
        &self,
        container_id: &str,
        cmd: &[String],
        timeout: i64,
    ) -> Result<ExecSyncResult, CriError>;
}

/// CRI ImageService trait.
#[async_trait]
pub trait ImageService: Send + Sync + 'static {
    /// Pull an image.
    async fn pull_image(&self, image: &str) -> Result<String, CriError>;

    /// Get image status.
    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError>;

    /// List images.
    async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError>;

    /// Remove an image.
    async fn remove_image(&self, image: &str) -> Result<(), CriError>;
}

/// Migration strategy for a pod sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStrategy {
    /// CRIU checkpoint/restore for native containers.
    Checkpoint,
    /// VM live migration (cloud-hypervisor/QEMU).
    LiveMigrate,
    /// Firecracker snapshot + restore.
    Snapshot,
    /// Kill + reschedule (CRI or unsupported runtimes).
    Evacuate,
}

/// Reference to a checkpoint artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub path: String,
    pub size: u64,
    pub is_stream: bool,
    pub stream_endpoint: Option<String>,
}

/// Progress of an ongoing migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub phase: String,
    pub percent: u8,
    pub bytes_transferred: u64,
    pub elapsed_ms: u64,
    pub message: String,
}

/// Migration service trait — implemented per runtime.
#[async_trait]
pub trait MigrationService: Send + Sync + 'static {
    /// Determine the migration strategy for a sandbox.
    fn migration_strategy(&self, sandbox_id: &str) -> MigrationStrategy;

    /// Checkpoint a pod sandbox (freeze state to disk/stream).
    async fn checkpoint_pod(&self, sandbox_id: &str) -> Result<CheckpointRef, CriError>;

    /// Restore a pod from a checkpoint.
    async fn restore_pod(
        &self,
        checkpoint: &CheckpointRef,
        config: &PodSandboxConfig,
    ) -> Result<String, CriError>;

    /// Prepare target node to receive a live migration.
    async fn prepare_migration_target(
        &self,
        config: &PodSandboxConfig,
    ) -> Result<String, CriError>;

    /// Live-migrate a sandbox to a target endpoint.
    async fn live_migrate(
        &self,
        sandbox_id: &str,
        target_endpoint: &str,
    ) -> Result<(), CriError>;

    /// Query migration progress.
    async fn migration_progress(&self, sandbox_id: &str) -> Result<MigrationProgress, CriError>;
}

/// CRI error type.
#[derive(Debug, thiserror::Error)]
pub enum CriError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("runtime error: {0}")]
    Runtime(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("image pull error: {0}")]
    ImagePull(String),

    #[error("timeout")]
    Timeout,

    #[error("migration error: {0}")]
    Migration(String),
}
