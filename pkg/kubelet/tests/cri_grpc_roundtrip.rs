//! Round-trip test for the CRI gRPC client: a mock CRI runtime (the server
//! side of the same generated protocol) listens on a real Unix socket in a
//! temp dir, and `CriGrpcClient` talks to it exactly as it would to CRI-O.

use kubelet::cri::{
    ContainerConfig, ContainerState, ImageService, PodSandboxConfig, RuntimeService,
};
use kubelet::cri_grpc::{proto, CriGrpcClient};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

type Result<T> = std::result::Result<Response<T>, Status>;

/// Records the requests the "runtime" saw so the test can assert on the
/// proto encoding the client produced.
#[derive(Default)]
struct Recorded {
    sandbox_config: Option<proto::PodSandboxConfig>,
    container_config: Option<proto::ContainerConfig>,
    pulled_image: Option<String>,
}

#[derive(Default, Clone)]
struct MockCri {
    recorded: Arc<Mutex<Recorded>>,
}

fn unimplemented<T>() -> Result<T> {
    Err(Status::unimplemented("not needed by this test"))
}

#[tonic::async_trait]
impl proto::runtime_service_server::RuntimeService for MockCri {
    async fn version(&self, _: Request<proto::VersionRequest>) -> Result<proto::VersionResponse> {
        Ok(Response::new(proto::VersionResponse {
            version: "0.1.0".into(),
            runtime_name: "cri-o".into(),
            runtime_version: "1.32.0".into(),
            runtime_api_version: "v1".into(),
        }))
    }

    async fn run_pod_sandbox(
        &self,
        request: Request<proto::RunPodSandboxRequest>,
    ) -> Result<proto::RunPodSandboxResponse> {
        self.recorded.lock().unwrap().sandbox_config = request.into_inner().config;
        Ok(Response::new(proto::RunPodSandboxResponse {
            pod_sandbox_id: "sb-abc123".into(),
        }))
    }

    async fn pod_sandbox_status(
        &self,
        _: Request<proto::PodSandboxStatusRequest>,
    ) -> Result<proto::PodSandboxStatusResponse> {
        Ok(Response::new(proto::PodSandboxStatusResponse {
            status: Some(proto::PodSandboxStatus {
                id: "sb-abc123".into(),
                state: proto::PodSandboxState::SandboxReady as i32,
                network: Some(proto::PodSandboxNetworkStatus {
                    ip: "10.0.42.9".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn create_container(
        &self,
        request: Request<proto::CreateContainerRequest>,
    ) -> Result<proto::CreateContainerResponse> {
        self.recorded.lock().unwrap().container_config = request.into_inner().config;
        Ok(Response::new(proto::CreateContainerResponse {
            container_id: "ctr-def456".into(),
        }))
    }

    async fn start_container(
        &self,
        _: Request<proto::StartContainerRequest>,
    ) -> Result<proto::StartContainerResponse> {
        Ok(Response::new(proto::StartContainerResponse {}))
    }

    async fn container_status(
        &self,
        request: Request<proto::ContainerStatusRequest>,
    ) -> Result<proto::ContainerStatusResponse> {
        Ok(Response::new(proto::ContainerStatusResponse {
            status: Some(proto::ContainerStatus {
                id: request.into_inner().container_id,
                metadata: Some(proto::ContainerMetadata {
                    name: "app".into(),
                    attempt: 0,
                }),
                state: proto::ContainerState::ContainerRunning as i32,
                image: Some(proto::ImageSpec {
                    image: "busybox:latest".into(),
                    ..Default::default()
                }),
                image_ref: "sha256:feedface".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn exec_sync(
        &self,
        _: Request<proto::ExecSyncRequest>,
    ) -> Result<proto::ExecSyncResponse> {
        Ok(Response::new(proto::ExecSyncResponse {
            stdout: b"ok".to_vec(),
            stderr: vec![],
            exit_code: 0,
        }))
    }

    async fn stop_pod_sandbox(
        &self,
        _: Request<proto::StopPodSandboxRequest>,
    ) -> Result<proto::StopPodSandboxResponse> {
        unimplemented()
    }
    async fn remove_pod_sandbox(
        &self,
        _: Request<proto::RemovePodSandboxRequest>,
    ) -> Result<proto::RemovePodSandboxResponse> {
        unimplemented()
    }
    async fn list_pod_sandbox(
        &self,
        _: Request<proto::ListPodSandboxRequest>,
    ) -> Result<proto::ListPodSandboxResponse> {
        unimplemented()
    }
    async fn stop_container(
        &self,
        _: Request<proto::StopContainerRequest>,
    ) -> Result<proto::StopContainerResponse> {
        unimplemented()
    }
    async fn remove_container(
        &self,
        _: Request<proto::RemoveContainerRequest>,
    ) -> Result<proto::RemoveContainerResponse> {
        unimplemented()
    }
    async fn list_containers(
        &self,
        _: Request<proto::ListContainersRequest>,
    ) -> Result<proto::ListContainersResponse> {
        unimplemented()
    }
    async fn update_container_resources(
        &self,
        _: Request<proto::UpdateContainerResourcesRequest>,
    ) -> Result<proto::UpdateContainerResourcesResponse> {
        unimplemented()
    }
    async fn reopen_container_log(
        &self,
        _: Request<proto::ReopenContainerLogRequest>,
    ) -> Result<proto::ReopenContainerLogResponse> {
        unimplemented()
    }
    async fn exec(&self, _: Request<proto::ExecRequest>) -> Result<proto::ExecResponse> {
        unimplemented()
    }
    async fn attach(&self, _: Request<proto::AttachRequest>) -> Result<proto::AttachResponse> {
        unimplemented()
    }
    async fn port_forward(
        &self,
        _: Request<proto::PortForwardRequest>,
    ) -> Result<proto::PortForwardResponse> {
        unimplemented()
    }
    async fn container_stats(
        &self,
        _: Request<proto::ContainerStatsRequest>,
    ) -> Result<proto::ContainerStatsResponse> {
        unimplemented()
    }
    async fn list_container_stats(
        &self,
        _: Request<proto::ListContainerStatsRequest>,
    ) -> Result<proto::ListContainerStatsResponse> {
        unimplemented()
    }
    async fn pod_sandbox_stats(
        &self,
        _: Request<proto::PodSandboxStatsRequest>,
    ) -> Result<proto::PodSandboxStatsResponse> {
        unimplemented()
    }
    async fn list_pod_sandbox_stats(
        &self,
        _: Request<proto::ListPodSandboxStatsRequest>,
    ) -> Result<proto::ListPodSandboxStatsResponse> {
        unimplemented()
    }
    async fn update_runtime_config(
        &self,
        _: Request<proto::UpdateRuntimeConfigRequest>,
    ) -> Result<proto::UpdateRuntimeConfigResponse> {
        unimplemented()
    }
    async fn status(&self, _: Request<proto::StatusRequest>) -> Result<proto::StatusResponse> {
        unimplemented()
    }
    async fn checkpoint_container(
        &self,
        _: Request<proto::CheckpointContainerRequest>,
    ) -> Result<proto::CheckpointContainerResponse> {
        unimplemented()
    }
    type GetContainerEventsStream = std::pin::Pin<
        Box<
            dyn tonic::codegen::tokio_stream::Stream<
                    Item = std::result::Result<proto::ContainerEventResponse, Status>,
                > + Send,
        >,
    >;
    async fn get_container_events(
        &self,
        _: Request<proto::GetEventsRequest>,
    ) -> Result<Self::GetContainerEventsStream> {
        unimplemented()
    }
    async fn list_metric_descriptors(
        &self,
        _: Request<proto::ListMetricDescriptorsRequest>,
    ) -> Result<proto::ListMetricDescriptorsResponse> {
        unimplemented()
    }
    async fn list_pod_sandbox_metrics(
        &self,
        _: Request<proto::ListPodSandboxMetricsRequest>,
    ) -> Result<proto::ListPodSandboxMetricsResponse> {
        unimplemented()
    }
    async fn runtime_config(
        &self,
        _: Request<proto::RuntimeConfigRequest>,
    ) -> Result<proto::RuntimeConfigResponse> {
        unimplemented()
    }
}

#[tonic::async_trait]
impl proto::image_service_server::ImageService for MockCri {
    async fn pull_image(
        &self,
        request: Request<proto::PullImageRequest>,
    ) -> Result<proto::PullImageResponse> {
        let image = request
            .into_inner()
            .image
            .map(|i| i.image)
            .unwrap_or_default();
        self.recorded.lock().unwrap().pulled_image = Some(image);
        Ok(Response::new(proto::PullImageResponse {
            image_ref: "sha256:cafed00d".into(),
        }))
    }

    async fn list_images(
        &self,
        _: Request<proto::ListImagesRequest>,
    ) -> Result<proto::ListImagesResponse> {
        unimplemented()
    }
    async fn image_status(
        &self,
        _: Request<proto::ImageStatusRequest>,
    ) -> Result<proto::ImageStatusResponse> {
        unimplemented()
    }
    async fn remove_image(
        &self,
        _: Request<proto::RemoveImageRequest>,
    ) -> Result<proto::RemoveImageResponse> {
        unimplemented()
    }
    async fn image_fs_info(
        &self,
        _: Request<proto::ImageFsInfoRequest>,
    ) -> Result<proto::ImageFsInfoResponse> {
        unimplemented()
    }
}

/// Start the mock runtime on a Unix socket; returns the socket path and the
/// recorder handle.
async fn start_mock(dir: &std::path::Path) -> (String, Arc<Mutex<Recorded>>) {
    let socket_path = dir.join("crio.sock");
    let mock = MockCri::default();
    let recorded = mock.recorded.clone();

    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proto::runtime_service_server::RuntimeServiceServer::new(
                mock.clone(),
            ))
            .add_service(proto::image_service_server::ImageServiceServer::new(mock))
            .serve_with_incoming(incoming),
    );

    (socket_path.to_string_lossy().into_owned(), recorded)
}

fn sandbox_config() -> PodSandboxConfig {
    PodSandboxConfig {
        name: "web".into(),
        uid: "uid-1".into(),
        namespace: "default".into(),
        attempt: 0,
        hostname: "web".into(),
        log_directory: "/var/log/pods/default_web_uid-1".into(),
        dns_servers: vec!["10.96.0.10".into()],
        dns_searches: vec!["default.svc.cluster.local".into()],
        labels: HashMap::new(),
        annotations: HashMap::new(),
        port_mappings: vec![],
        host_network: true,
        host_pid: false,
        host_ipc: false,
        privileged: true,
    }
}

#[tokio::test]
async fn full_pod_flow_over_unix_socket() {
    let dir = tempfile::tempdir().unwrap();
    let (socket, recorded) = start_mock(dir.path()).await;

    let client = CriGrpcClient::new(&socket);

    // Version
    let (name, version, api) = client.version().await.unwrap();
    assert_eq!(name, "cri-o");
    assert_eq!(version, "1.32.0");
    assert_eq!(api, "v1");

    // Sandbox
    let config = sandbox_config();
    let sandbox_id = client.run_pod_sandbox(&config).await.unwrap();
    assert_eq!(sandbox_id, "sb-abc123");

    let sent = recorded.lock().unwrap().sandbox_config.clone().unwrap();
    let meta = sent.metadata.unwrap();
    assert_eq!(meta.name, "web");
    assert_eq!(meta.namespace, "default");
    assert_eq!(meta.uid, "uid-1");
    assert_eq!(sent.dns_config.unwrap().servers, vec!["10.96.0.10"]);
    // hostNetwork=true → sandbox network namespace mode NODE.
    let ns = sent.linux.unwrap().security_context.unwrap().namespace_options.unwrap();
    assert_eq!(ns.network, proto::NamespaceMode::Node as i32);
    assert_eq!(ns.pid, proto::NamespaceMode::Pod as i32);

    let status = client.pod_sandbox_status(&sandbox_id).await.unwrap();
    assert_eq!(status.ip, "10.0.42.9");

    // Image pull
    let image_ref = client.pull_image("busybox:latest").await.unwrap();
    assert_eq!(image_ref, "sha256:cafed00d");
    assert_eq!(
        recorded.lock().unwrap().pulled_image.as_deref(),
        Some("busybox:latest")
    );

    // Container
    let container = ContainerConfig {
        name: "app".into(),
        attempt: 0,
        image: image_ref,
        command: vec!["sleep".into()],
        args: vec!["3600".into()],
        working_dir: String::new(),
        envs: vec![("FOO".into(), "bar".into())],
        mounts: vec![],
        labels: HashMap::new(),
        annotations: HashMap::new(),
        log_path: String::new(),
        stdin: false,
        tty: false,
        cpu_period: 100_000,
        cpu_quota: 20_000,
        cpu_shares: 204,
        memory_limit_bytes: 64 * 1024 * 1024,
        privileged: true,
        readonly_rootfs: false,
        add_capabilities: vec!["NET_ADMIN".into()],
        selinux_options: Some(kubelet::cri::SeLinuxOptions {
            type_: "spc_t".into(),
            level: "s0".into(),
            ..Default::default()
        }),
    };
    let container_id = client
        .create_container(&sandbox_id, &container, &config)
        .await
        .unwrap();
    assert_eq!(container_id, "ctr-def456");

    let sent = recorded.lock().unwrap().container_config.clone().unwrap();
    assert_eq!(sent.metadata.unwrap().name, "app");
    assert_eq!(sent.command, vec!["sleep"]);
    assert_eq!(sent.envs[0].key, "FOO");
    assert_eq!(sent.envs[0].value, "bar");
    let linux = sent.linux.unwrap();
    let resources = linux.resources.unwrap();
    assert_eq!(resources.cpu_quota, 20_000);
    assert_eq!(resources.memory_limit_in_bytes, 64 * 1024 * 1024);
    // securityContext: privileged + NET_ADMIN reach the wire.
    let sc = linux.security_context.unwrap();
    assert!(sc.privileged);
    assert_eq!(sc.capabilities.unwrap().add_capabilities, vec!["NET_ADMIN"]);

    client.start_container(&container_id).await.unwrap();

    let status = client.container_status(&container_id).await.unwrap();
    assert_eq!(status.state, ContainerState::Running);
    assert_eq!(status.image_ref, "sha256:feedface");

    // Exec (probe path)
    let result = client
        .exec_sync(&container_id, &["true".to_string()], 5)
        .await
        .unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout, b"ok");
}

#[tokio::test]
async fn connection_error_when_socket_missing() {
    let client = CriGrpcClient::new("/nonexistent/cri.sock");
    let err = client.version().await.unwrap_err();
    // Must surface as an error, not hang or panic.
    assert!(!err.to_string().is_empty());
}
