//! CRI v1 gRPC client — the real thing.
//!
//! Speaks the Kubernetes CRI v1 protocol over a Unix socket to CRI-O or
//! containerd, exactly as upstream kubelet (and OpenShift) do. Generated
//! from the vendored kubernetes/cri-api proto (release-1.32) by build.rs.
//!
//! This supersedes the Phase-1 crictl bridge in `cri_client.rs`.

use crate::cri::{
    CheckpointRef, ContainerConfig, ContainerState, ContainerStatusInfo, CriError,
    ExecSyncResult, ImageInfo, ImageService, MigrationProgress, MigrationService,
    MigrationStrategy, PodSandboxConfig, PodSandboxState, PodSandboxStatusInfo, PodSandboxSummary,
    RuntimeService,
};
use async_trait::async_trait;
use tonic::transport::{Channel, Endpoint, Uri};
use tracing::info;

/// Generated CRI v1 types (`package runtime.v1` in the proto).
pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("runtime.v1");
}

use proto::image_service_client::ImageServiceClient;
use proto::runtime_service_client::RuntimeServiceClient;

/// gRPC CRI client over a Unix domain socket.
pub struct CriGrpcClient {
    runtime: RuntimeServiceClient<Channel>,
    images: ImageServiceClient<Channel>,
    socket_path: String,
}

impl CriGrpcClient {
    /// Create a client for a CRI socket (e.g. `/run/crio/crio.sock` or
    /// `unix:///run/containerd/containerd.sock`). Connects lazily — the
    /// socket is dialed on first RPC and redialed as needed.
    pub fn new(socket: &str) -> Self {
        let path = socket.strip_prefix("unix://").unwrap_or(socket).to_string();
        info!("CRI gRPC client for {path}");

        // The URI is a placeholder — the connector ignores it and dials the
        // Unix socket.
        let channel = Endpoint::try_from("http://[::1]:50051")
            .expect("static endpoint URI")
            // Default per-request deadline: bounds any hung CRI call (e.g. a
            // RunPodSandbox blocked on CNI) so it can't stall the sync loop.
            // Image pulls and exec override this with their own longer/explicit
            // deadlines below.
            .timeout(std::time::Duration::from_secs(120))
            .connect_with_connector_lazy(tower::service_fn({
                let path = path.clone();
                move |_: Uri| {
                    let path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }
            }));

        Self {
            runtime: RuntimeServiceClient::new(channel.clone()),
            images: ImageServiceClient::new(channel),
            socket_path: path,
        }
    }
}

/// Wrap a message in a tonic Request with an explicit per-request deadline,
/// overriding the channel default (used where the default is wrong — long
/// image pulls, and exec honoring the caller's probe timeout).
fn timed<T>(msg: T, secs: u64) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.set_timeout(std::time::Duration::from_secs(secs));
    r
}

fn rpc_err(e: tonic::Status) -> CriError {
    match e.code() {
        tonic::Code::NotFound => CriError::NotFound(e.message().to_string()),
        tonic::Code::Unavailable => CriError::Connection(e.message().to_string()),
        tonic::Code::DeadlineExceeded => CriError::Timeout,
        _ => CriError::Runtime(format!("{}: {}", e.code(), e.message())),
    }
}

fn to_proto_sandbox_config(config: &PodSandboxConfig) -> proto::PodSandboxConfig {
    proto::PodSandboxConfig {
        metadata: Some(proto::PodSandboxMetadata {
            name: config.name.clone(),
            uid: config.uid.clone(),
            namespace: config.namespace.clone(),
            attempt: config.attempt,
        }),
        hostname: config.hostname.clone(),
        log_directory: config.log_directory.clone(),
        dns_config: Some(proto::DnsConfig {
            servers: config.dns_servers.clone(),
            searches: config.dns_searches.clone(),
            options: vec![],
        }),
        port_mappings: config
            .port_mappings
            .iter()
            .map(|pm| proto::PortMapping {
                protocol: match pm.protocol.to_uppercase().as_str() {
                    "UDP" => proto::Protocol::Udp as i32,
                    "SCTP" => proto::Protocol::Sctp as i32,
                    _ => proto::Protocol::Tcp as i32,
                },
                container_port: pm.container_port,
                host_port: pm.host_port,
                host_ip: pm.host_ip.clone(),
            })
            .collect(),
        labels: config.labels.clone().into_iter().collect(),
        annotations: config.annotations.clone().into_iter().collect(),
        linux: Some(proto::LinuxPodSandboxConfig {
            security_context: Some(proto::LinuxSandboxSecurityContext {
                namespace_options: Some(namespace_options(
                    config.host_network,
                    config.host_pid,
                    config.host_ipc,
                )),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build CRI NamespaceOption from host-namespace-sharing flags. NODE = share
/// the host namespace; POD = the sandbox's own (default).
fn namespace_options(host_network: bool, host_pid: bool, host_ipc: bool) -> proto::NamespaceOption {
    let mode = |host: bool| {
        if host {
            proto::NamespaceMode::Node as i32
        } else {
            proto::NamespaceMode::Pod as i32
        }
    };
    proto::NamespaceOption {
        network: mode(host_network),
        pid: mode(host_pid),
        ipc: mode(host_ipc),
        ..Default::default()
    }
}

fn to_proto_propagation(p: crate::cri::MountPropagation) -> i32 {
    use crate::cri::MountPropagation::*;
    match p {
        Private => proto::MountPropagation::PropagationPrivate as i32,
        HostToContainer => proto::MountPropagation::PropagationHostToContainer as i32,
        Bidirectional => proto::MountPropagation::PropagationBidirectional as i32,
    }
}

fn to_proto_container_config(config: &ContainerConfig) -> proto::ContainerConfig {
    proto::ContainerConfig {
        metadata: Some(proto::ContainerMetadata {
            name: config.name.clone(),
            attempt: config.attempt,
        }),
        image: Some(proto::ImageSpec {
            image: config.image.clone(),
            ..Default::default()
        }),
        command: config.command.clone(),
        args: config.args.clone(),
        working_dir: config.working_dir.clone(),
        envs: config
            .envs
            .iter()
            .map(|(k, v)| proto::KeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect(),
        mounts: config
            .mounts
            .iter()
            .map(|m| proto::Mount {
                container_path: m.container_path.clone(),
                host_path: m.host_path.clone(),
                readonly: m.readonly,
                propagation: to_proto_propagation(m.propagation),
                ..Default::default()
            })
            .collect(),
        labels: config.labels.clone().into_iter().collect(),
        annotations: config.annotations.clone().into_iter().collect(),
        log_path: config.log_path.clone(),
        stdin: config.stdin,
        tty: config.tty,
        linux: Some(proto::LinuxContainerConfig {
            resources: Some(proto::LinuxContainerResources {
                cpu_period: config.cpu_period,
                cpu_quota: config.cpu_quota,
                cpu_shares: config.cpu_shares,
                memory_limit_in_bytes: config.memory_limit_bytes,
                ..Default::default()
            }),
            security_context: Some(proto::LinuxContainerSecurityContext {
                privileged: config.privileged,
                readonly_rootfs: config.readonly_rootfs,
                capabilities: Some(proto::Capability {
                    add_capabilities: config.add_capabilities.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}

fn from_proto_container_state(state: i32) -> ContainerState {
    match proto::ContainerState::try_from(state) {
        Ok(proto::ContainerState::ContainerCreated) => ContainerState::Created,
        Ok(proto::ContainerState::ContainerRunning) => ContainerState::Running,
        Ok(proto::ContainerState::ContainerExited) => ContainerState::Exited,
        _ => ContainerState::Unknown,
    }
}

fn from_proto_container_status(s: proto::ContainerStatus) -> ContainerStatusInfo {
    ContainerStatusInfo {
        id: s.id,
        name: s.metadata.map(|m| m.name).unwrap_or_default(),
        state: from_proto_container_state(s.state),
        created_at: s.created_at,
        started_at: s.started_at,
        finished_at: s.finished_at,
        exit_code: s.exit_code,
        image: s.image.map(|i| i.image).unwrap_or_default(),
        image_ref: s.image_ref,
        reason: s.reason,
        message: s.message,
    }
}

#[async_trait]
impl RuntimeService for CriGrpcClient {
    async fn version(&self) -> Result<(String, String, String), CriError> {
        let resp = self
            .runtime
            .clone()
            .version(proto::VersionRequest {
                version: "v1".to_string(),
            })
            .await
            .map_err(rpc_err)?
            .into_inner();
        Ok((resp.runtime_name, resp.runtime_version, resp.runtime_api_version))
    }

    async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
        let resp = self
            .runtime
            .clone()
            .run_pod_sandbox(proto::RunPodSandboxRequest {
                config: Some(to_proto_sandbox_config(config)),
                runtime_handler: String::new(),
            })
            .await
            .map_err(rpc_err)?
            .into_inner();
        Ok(resp.pod_sandbox_id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        self.runtime
            .clone()
            .stop_pod_sandbox(proto::StopPodSandboxRequest {
                pod_sandbox_id: sandbox_id.to_string(),
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        self.runtime
            .clone()
            .remove_pod_sandbox(proto::RemovePodSandboxRequest {
                pod_sandbox_id: sandbox_id.to_string(),
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    async fn pod_sandbox_status(
        &self,
        sandbox_id: &str,
    ) -> Result<PodSandboxStatusInfo, CriError> {
        let resp = self
            .runtime
            .clone()
            .pod_sandbox_status(proto::PodSandboxStatusRequest {
                pod_sandbox_id: sandbox_id.to_string(),
                verbose: false,
            })
            .await
            .map_err(rpc_err)?
            .into_inner();

        let status = resp
            .status
            .ok_or_else(|| CriError::Runtime("empty sandbox status".into()))?;
        let network = status.network.unwrap_or_default();

        Ok(PodSandboxStatusInfo {
            id: status.id,
            state: if status.state == proto::PodSandboxState::SandboxReady as i32 {
                PodSandboxState::Ready
            } else {
                PodSandboxState::NotReady
            },
            created_at: status.created_at,
            ip: network.ip,
            additional_ips: network.additional_ips.into_iter().map(|i| i.ip).collect(),
        })
    }

    async fn list_pod_sandbox(&self) -> Result<Vec<PodSandboxSummary>, CriError> {
        let resp = self
            .runtime
            .clone()
            .list_pod_sandbox(proto::ListPodSandboxRequest { filter: None })
            .await
            .map_err(rpc_err)?
            .into_inner();

        Ok(resp
            .items
            .into_iter()
            .map(|s| {
                let state = if s.state == proto::PodSandboxState::SandboxReady as i32 {
                    PodSandboxState::Ready
                } else {
                    PodSandboxState::NotReady
                };
                let m = s.metadata.unwrap_or_default();
                PodSandboxSummary {
                    id: s.id,
                    state,
                    uid: m.uid,
                    name: m.name,
                    namespace: m.namespace,
                }
            })
            .collect())
    }

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: &ContainerConfig,
        sandbox_config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        let resp = self
            .runtime
            .clone()
            .create_container(proto::CreateContainerRequest {
                pod_sandbox_id: sandbox_id.to_string(),
                config: Some(to_proto_container_config(config)),
                sandbox_config: Some(to_proto_sandbox_config(sandbox_config)),
            })
            .await
            .map_err(rpc_err)?
            .into_inner();
        Ok(resp.container_id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
        self.runtime
            .clone()
            .start_container(proto::StartContainerRequest {
                container_id: container_id.to_string(),
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError> {
        self.runtime
            .clone()
            .stop_container(proto::StopContainerRequest {
                container_id: container_id.to_string(),
                timeout,
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
        self.runtime
            .clone()
            .remove_container(proto::RemoveContainerRequest {
                container_id: container_id.to_string(),
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> Result<ContainerStatusInfo, CriError> {
        let resp = self
            .runtime
            .clone()
            .container_status(proto::ContainerStatusRequest {
                container_id: container_id.to_string(),
                verbose: false,
            })
            .await
            .map_err(rpc_err)?
            .into_inner();

        let status = resp
            .status
            .ok_or_else(|| CriError::NotFound(container_id.to_string()))?;
        Ok(from_proto_container_status(status))
    }

    async fn list_containers(
        &self,
        sandbox_id: Option<&str>,
    ) -> Result<Vec<ContainerStatusInfo>, CriError> {
        let filter = sandbox_id.map(|id| proto::ContainerFilter {
            pod_sandbox_id: id.to_string(),
            ..Default::default()
        });

        let resp = self
            .runtime
            .clone()
            .list_containers(proto::ListContainersRequest { filter })
            .await
            .map_err(rpc_err)?
            .into_inner();

        Ok(resp
            .containers
            .into_iter()
            .map(|c| ContainerStatusInfo {
                id: c.id,
                name: c.metadata.map(|m| m.name).unwrap_or_default(),
                state: from_proto_container_state(c.state),
                created_at: c.created_at,
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
                image: c.image.map(|i| i.image).unwrap_or_default(),
                image_ref: c.image_ref,
                reason: String::new(),
                message: String::new(),
            })
            .collect())
    }

    async fn exec_sync(
        &self,
        container_id: &str,
        cmd: &[String],
        timeout: i64,
    ) -> Result<ExecSyncResult, CriError> {
        // Honor the caller's exec/probe timeout as the RPC deadline (+5s slack
        // so the runtime returns its own timeout rather than us cancelling).
        let deadline = timeout.max(0) as u64 + 5;
        let resp = self
            .runtime
            .clone()
            .exec_sync(timed(
                proto::ExecSyncRequest {
                    container_id: container_id.to_string(),
                    cmd: cmd.to_vec(),
                    timeout,
                },
                deadline,
            ))
            .await
            .map_err(rpc_err)?
            .into_inner();

        Ok(ExecSyncResult {
            stdout: resp.stdout,
            stderr: resp.stderr,
            exit_code: resp.exit_code,
        })
    }
}

#[async_trait]
impl ImageService for CriGrpcClient {
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        // Image pulls can be large — allow well beyond the 120s channel default.
        let resp = self
            .images
            .clone()
            .pull_image(timed(
                proto::PullImageRequest {
                    image: Some(proto::ImageSpec {
                        image: image.to_string(),
                        ..Default::default()
                    }),
                    auth: None,
                    sandbox_config: None,
                },
                600,
            ))
            .await
            .map_err(|e| CriError::ImagePull(format!("{image}: {}", e.message())))?
            .into_inner();
        Ok(resp.image_ref)
    }

    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
        let resp = self
            .images
            .clone()
            .image_status(proto::ImageStatusRequest {
                image: Some(proto::ImageSpec {
                    image: image.to_string(),
                    ..Default::default()
                }),
                verbose: false,
            })
            .await
            .map_err(rpc_err)?
            .into_inner();

        Ok(resp.image.map(|i| ImageInfo {
            id: i.id,
            repo_tags: i.repo_tags,
            repo_digests: i.repo_digests,
            size: i.size,
        }))
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
        let resp = self
            .images
            .clone()
            .list_images(proto::ListImagesRequest { filter: None })
            .await
            .map_err(rpc_err)?
            .into_inner();

        Ok(resp
            .images
            .into_iter()
            .map(|i| ImageInfo {
                id: i.id,
                repo_tags: i.repo_tags,
                repo_digests: i.repo_digests,
                size: i.size,
            })
            .collect())
    }

    async fn remove_image(&self, image: &str) -> Result<(), CriError> {
        self.images
            .clone()
            .remove_image(proto::RemoveImageRequest {
                image: Some(proto::ImageSpec {
                    image: image.to_string(),
                    ..Default::default()
                }),
            })
            .await
            .map_err(rpc_err)?;
        Ok(())
    }
}

/// External CRI runtimes don't expose checkpoint/migrate through CRI —
/// migration for CRI pods is evacuate (kill + reschedule).
#[async_trait]
impl MigrationService for CriGrpcClient {
    fn migration_strategy(&self, _sandbox_id: &str) -> MigrationStrategy {
        MigrationStrategy::Evacuate
    }

    async fn checkpoint_pod(&self, _sandbox_id: &str) -> Result<CheckpointRef, CriError> {
        Err(CriError::Migration(format!(
            "CRI runtime at {} does not support checkpoint via CRI",
            self.socket_path
        )))
    }

    async fn restore_pod(
        &self,
        _checkpoint: &CheckpointRef,
        _config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        Err(CriError::Migration("restore not supported via CRI".into()))
    }

    async fn prepare_migration_target(
        &self,
        _config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        Err(CriError::Migration("live migration not supported via CRI".into()))
    }

    async fn live_migrate(
        &self,
        _sandbox_id: &str,
        _target_endpoint: &str,
    ) -> Result<(), CriError> {
        Err(CriError::Migration("live migration not supported via CRI".into()))
    }

    async fn migration_progress(&self, _sandbox_id: &str) -> Result<MigrationProgress, CriError> {
        Err(CriError::Migration("no migration in progress".into()))
    }
}
