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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PodSandboxState {
    Ready = 0,
    #[default]
    NotReady = 1,
}

/// Summary of a pod sandbox from a list call, including the pod identity
/// (metadata) needed to reconcile the kubelet's state with the runtime.
#[derive(Debug, Clone, Default)]
pub struct PodSandboxSummary {
    pub id: String,
    pub state: PodSandboxState,
    pub uid: String,
    pub name: String,
    pub namespace: String,
}

/// Pod sandbox configuration.
#[derive(Debug, Clone, Default)]
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
    /// Share the host network namespace (pod.spec.hostNetwork).
    pub host_network: bool,
    /// Share the host PID namespace (pod.spec.hostPID).
    pub host_pid: bool,
    /// Share the host IPC namespace (pod.spec.hostIPC).
    pub host_ipc: bool,
    /// Allow privileged containers in this sandbox. Required when any container
    /// in the pod sets `securityContext.privileged` — otherwise the runtime
    /// rejects it with "no privileged container allowed in sandbox"
    /// (e.g. Cilium's mount-bpf-fs init container). (rustkube-node#26)
    pub privileged: bool,
    /// pod.spec.securityContext.seccompProfile — applied to the sandbox.
    pub seccomp_profile: Option<SeccompProfile>,
    /// pod.spec.securityContext.seLinuxOptions — the sandbox's SELinux label.
    /// The sandbox holds the namespaces its containers join, so a pod-level
    /// label has to reach it as well as them (rustkube-node#26).
    pub selinux_options: Option<SeLinuxOptions>,
    /// The pod's QoS class (`Guaranteed`, `Burstable`, `BestEffort`), for its
    /// cgroup parent on a CRI runtime (#24). Empty: the runtime's default.
    pub qos_class: String,
    /// The pod's extra networks (`k8s.v1.cni.cncf.io/networks`, #233),
    /// resolved to their NetworkAttachmentDefinitions, in order. A runtime
    /// that does its own CNI (stormpump) runs one ADD per entry after the
    /// default network.
    pub networks: Vec<NetworkAttachment>,
    /// The pod's default network, replaced (`v1.multus-cni.io/default-network`).
    pub default_network: Option<NetworkAttachment>,
}

/// One network a pod attaches to (#233), resolved from its
/// NetworkAttachmentDefinition.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NetworkAttachment {
    /// `namespace/name` of the NetworkAttachmentDefinition: the
    /// `network-status` entry's name.
    pub name: String,
    /// The interface in the pod (`net1`, … unless the pod named it).
    pub ifname: String,
    /// The NAD's `spec.config`: a CNI conflist or one plugin's config.
    pub config: String,
    /// Addresses the pod asked for (`ips`), for plugins that take them.
    pub ips: Vec<String>,
    /// A MAC address the pod asked for, likewise.
    pub mac: Option<String>,
}

/// Port mapping for a pod sandbox.
#[derive(Debug, Clone, Default)]
pub struct PortMapping {
    pub protocol: String,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

/// Container configuration.
#[derive(Debug, Clone, Default)]
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
    /// The pod's QoS class (`Guaranteed`, `Burstable`, `BestEffort`), which
    /// picks its cgroup group on stormpump (#57, stormpump#68). Empty: unknown.
    pub qos_class: String,
    /// securityContext.runAsUser (the container's, else the pod's; #98). `None`:
    /// the image's `User`, else root.
    pub run_as_user: Option<i64>,
    /// securityContext.runAsGroup, likewise.
    pub run_as_group: Option<i64>,
    /// securityContext.runAsNonRoot: refuse to start as uid 0.
    pub run_as_non_root: Option<bool>,
    /// securityContext.privileged — full host access (Cilium agent needs this).
    pub privileged: bool,
    /// securityContext.readOnlyRootFilesystem.
    pub readonly_rootfs: bool,
    /// securityContext.capabilities.add (Linux capability names, e.g. NET_ADMIN).
    pub add_capabilities: Vec<String>,
    /// securityContext.capabilities.drop (#118). `ALL` is a name here as in
    /// the pod spec; the runtime resolves it.
    pub drop_capabilities: Vec<String>,
    /// securityContext.seLinuxOptions — the container's SELinux label. Cilium's
    /// init containers request `type: spc_t` so they can write host paths under
    /// enforcing SELinux; without passing this the runtime uses `container_t`
    /// and those writes are denied (rustkube-node#26).
    pub selinux_options: Option<SeLinuxOptions>,
    /// pod.spec.hostNetwork — the container joins the host network namespace.
    pub host_network: bool,
    /// pod.spec.hostPID — the container joins the host PID namespace. Must match
    /// the sandbox: a hostPID pod builds its sandbox with pid=NODE, so the
    /// container's namespace_options.pid must also be NODE or the runtime rejects
    /// it ("pod level PID namespace requested ... but pod sandbox was not
    /// similarly configured") — this is what blocks the hostPID Cilium agent.
    pub host_pid: bool,
    /// pod.spec.hostIPC — the container joins the host IPC namespace.
    pub host_ipc: bool,
    /// pod.spec.shareProcessNamespace — containers share the pod's PID namespace
    /// (pid=POD) instead of each getting their own (pid=CONTAINER).
    pub share_process_namespace: bool,
    /// securityContext.seccompProfile (container-level, else the pod's). Cilium
    /// needs `Unconfined`, or the runtime's default profile fails its syscalls
    /// with EPERM — stalling even the `config` init container on an HTTPS call.
    pub seccomp_profile: Option<SeccompProfile>,
}

/// SELinux label parts (user/role/type/level) for a container.
#[derive(Debug, Clone, Default)]
pub struct SeLinuxOptions {
    pub user: String,
    pub role: String,
    pub type_: String,
    pub level: String,
}

/// securityContext.seccompProfile — which seccomp profile the runtime applies.
/// Unset means the runtime uses its default profile, which answers blocked
/// syscalls with EPERM; workloads that program the datapath (Cilium) must run
/// `Unconfined` or even a plain apiserver call inside an init container fails.
#[derive(Debug, Clone, PartialEq)]
pub enum SeccompProfile {
    /// No seccomp filtering.
    Unconfined,
    /// The runtime's default profile.
    RuntimeDefault,
    /// A profile file on the node (absolute path in the ref).
    Localhost(String),
}

/// Mount propagation mode (matches CRI MountPropagation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MountPropagation {
    /// No propagation (default).
    #[default]
    Private,
    /// Host → container only.
    HostToContainer,
    /// Bidirectional (host ↔ container) — needed for e.g. the bpf fs mount.
    Bidirectional,
}

/// Mount specification.
#[derive(Debug, Clone, Default)]
pub struct Mount {
    pub container_path: String,
    pub host_path: String,
    pub readonly: bool,
    pub propagation: MountPropagation,
    /// Ask the runtime to relabel the host path for the container's SELinux
    /// context. Required on enforcing SELinux hosts for kubelet-materialized
    /// content (SA token, configMap/secret/projected/emptyDir); without it the
    /// container is denied access even when DAC perms allow. Never set for
    /// hostPath — relabeling host system paths (e.g. /sys, /proc) is harmful.
    pub selinux_relabel: bool,
    /// A filesystem to mount, when `host_path` names a **block device** rather
    /// than a directory to bind.
    ///
    /// `None` is every mount that existed before block-backed claims: bind the
    /// directory. `Some("ext4")` is a PersistentVolumeClaim, whose backing is a
    /// device the runtime mounts inside the container — which is what keeps the
    /// mount out of the host's namespace and away from mount propagation.
    pub fstype: Option<String>,
}

/// Container resource-usage stats (subset of CRI ContainerStats).
#[derive(Debug, Clone, Default)]
pub struct ContainerStatsInfo {
    pub container_id: String,
    pub name: String,
    /// Pod name/namespace, from the sandbox labels (io.kubernetes.pod.*).
    pub pod: String,
    pub namespace: String,
    /// Cumulative CPU usage in nanoseconds (for the cadvisor counter).
    /// `None` when the runtime did not report it: absent is not zero (#36).
    pub cpu_usage_core_nanos: Option<u64>,
    pub memory_working_set_bytes: Option<u64>,
    /// Bytes the container's writable layer uses.
    pub fs_usage_bytes: Option<u64>,
}

/// One network interface's counters, inside a pod.
#[derive(Debug, Clone, Default)]
pub struct InterfaceStats {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    /// Packets, errors and drops each way (#131), the same `/proc/net/dev` line.
    pub rx_packets: u64,
    pub rx_errors: u64,
    pub rx_dropped: u64,
    pub tx_packets: u64,
    pub tx_errors: u64,
    pub tx_dropped: u64,
}

/// A pod's network counters. Network belongs to the pod's sandbox, not to any
/// one container, as in cAdvisor.
#[derive(Debug, Clone, Default)]
pub struct PodNetworkStats {
    pub sandbox_id: String,
    pub pod: String,
    pub namespace: String,
    pub interfaces: Vec<InterfaceStats>,
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
    /// Path to the sandbox's network namespace (`/proc/<pid>/ns/net`), when the
    /// runtime reports it. Used to run http/tcp health probes inside the pod's
    /// netns so `127.0.0.1`/loopback-bound health servers are reachable.
    pub netns_path: Option<String>,
    /// How the runtime spent making it, when it times that (#139): the
    /// stormpump runtime does, a CRI runtime does not.
    pub made: Option<SandboxSteps>,
}

/// The runtime's own steps in making a sandbox (#139), for the start timing's
/// `sandbox/acquire` and `sandbox/cni`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SandboxSteps {
    /// stormpump `SandboxAcquire` (a warm namespace holder); zero on the host network.
    pub acquire: std::time::Duration,
    /// The CNI ADD, the plugin's exec included; zero when none ran.
    pub cni: std::time::Duration,
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
    async fn list_pod_sandbox(&self) -> Result<Vec<PodSandboxSummary>, CriError>;

    /// Resource-usage stats for all containers. Default: none (runtimes that
    /// don't implement stats return an empty list).
    async fn list_container_stats(&self) -> Result<Vec<ContainerStatsInfo>, CriError> {
        Ok(vec![])
    }

    /// Whether a container's log file outlives `remove_container` (#47).
    /// When it does not (a CRI runtime may take it with the container), a
    /// completed init container is kept until its pod stops, as upstream
    /// keeps it, so `kubectl logs -c <init>` still answers.
    fn logs_survive_removal(&self) -> bool {
        false
    }

    /// The pod (its uid) whose container ran as engine workload `handle`
    /// (#115), to wake that pod's worker alone when it exits. Default: not
    /// known, and the exit wakes every pod.
    async fn pod_of_workload(&self, _handle: u64) -> Option<String> {
        None
    }

    /// A container's image provenance and its own root (#130). Default: not known.
    async fn container_image_info(&self, _container_id: &str) -> Option<ContainerImageInfo> {
        None
    }

    /// What the CNI wired into this sandbox, as `network-status` entries
    /// (#131). Default: not known.
    async fn pod_network_status(&self, _sandbox_id: &str) -> Option<serde_json::Value> {
        None
    }

    /// Follow the runtime's container events (#116, CRI `GetContainerEvents`,
    /// upstream's evented PLEG), calling `on` for each, until the stream ends.
    /// `Connected` comes first, once the runtime has accepted the stream:
    /// what changed before it was not reported. Default: [`EventStream::Unsupported`].
    async fn follow_container_events(&self, _on: &(dyn Fn(RuntimeEvent) + Send + Sync)) -> EventStream {
        EventStream::Unsupported
    }

    /// Network counters per pod sandbox. Default: none.
    async fn list_pod_network_stats(&self) -> Result<Vec<PodNetworkStats>, CriError> {
        Ok(vec![])
    }

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
/// What a container runs from (#130), for its pod's annotations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerImageInfo {
    /// `sha256:<hex>` of the image (manifest digest, or a pallet golden's).
    pub image_id: Option<String>,
    /// The golden it was cloned from.
    pub golden: Option<String>,
    /// The engine volume that is its own copy-on-write root.
    pub instance: Option<String>,
    /// When its image was last resolved for it, RFC 3339.
    pub resolved_at: Option<String>,
    /// OCI build info / release provenance.
    pub build: serde_json::Map<String, serde_json::Value>,
}

/// One of the runtime's container events (#116).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeEvent {
    /// The stream is open; nothing before this was reported.
    Connected,
    /// A container was created, started, stopped or deleted: its pod's uid
    /// (from the event's sandbox status, when it carries one) and its id.
    Container { pod_uid: Option<String>, container_id: String },
}

/// How following the runtime's events ended (#116).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventStream {
    /// The runtime has no event stream (`Unimplemented`, or not a CRI runtime).
    Unsupported,
    /// It had one and it ended (or could not be opened): try again.
    Ended(String),
}

#[derive(Debug, thiserror::Error)]
pub enum CriError {
    /// A staged operation yielded; its recorded resources remain owned.
    #[error("startup pending: {0}")]
    Pending(String),
    #[error("connection error: {0}")]
    Connection(String),

    #[error("runtime error: {0}")]
    Runtime(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("image pull error: {0}")]
    ImagePull(String),

    /// One container could not be created or started (#133): `reason` is
    /// upstream's word for it (`CreateContainerError`, `StartError`), so the
    /// pod reports that container's state rather than none at all.
    #[error("{reason}: container {container}: {message}")]
    Container { container: String, reason: String, message: String },

    /// A volume is not ready yet — the pod stays Pending and is retried,
    /// which is what upstream does for a hostPath that is not there. Failing
    /// the pod would be wrong: the path may appear.
    #[error("volume not ready: {0}")]
    VolumeNotReady(String),
    /// The pod network is not available yet.
    ///
    /// Retryable for the same reason as a missing volume: the CNI appears when
    /// its agent comes up, and marking the pod Failed ends it for a condition
    /// that has not been established as permanent. This is the ordinary state
    /// of every pod scheduled in the window before the network agent is ready.
    #[error("network not ready: {0}")]
    NetworkNotReady(String),
    /// No CNI network config exists yet (#148): the pod waits for the file,
    /// and is woken when the config directory changes rather than polled.
    /// A config that exists but whose ADD fails is [`CriError::NetworkNotReady`].
    #[error("network not ready: {0}")]
    NetworkNotConfigured(String),
    /// The node's filesystem refused something the pod needs before it can
    /// start (ENOSPC, EDQUOT, EROFS on the container log directory), #129.
    ///
    /// Retryable: space comes back when something is cleaned up, and a pod
    /// marked Failed for a full disk is deleted and recreated by its
    /// controller into the same full disk, burning its backoff.
    #[error("node storage: {0}")]
    NodeStorage(String),

    #[error("timeout")]
    Timeout,

    #[error("migration error: {0}")]
    Migration(String),
}
