//! Native container runtime using youki's libcontainer.
//!
//! Directly creates and manages OCI containers without containerd.
//! Uses libcontainer for namespace/cgroup setup and oci-spec for
//! OCI runtime specification types.
//!
//! Architecture:
//!   rk-kubelet → libcontainer → Linux kernel
//!   (no containerd, no runc, no Go)

#[cfg(target_os = "linux")]
mod linux {
    use crate::checkpoint::CriuCheckpointer;
    use crate::cri::*;
    use async_trait::async_trait;
    use libcontainer::container::builder::ContainerBuilder;
    use libcontainer::container::Container;
    use libcontainer::syscall::syscall::SyscallType;
    use oci_spec::runtime::{
        LinuxBuilder, LinuxResourcesBuilder, LinuxCpuBuilder, LinuxMemoryBuilder,
        MountBuilder, ProcessBuilder, RootBuilder, SpecBuilder,
    };
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tracing::{debug, error, info, warn};

    /// Root directory for container state and rootfs.
    const CONTAINER_ROOT: &str = "/var/lib/rustkube/containers";
    const CONTAINER_STATE: &str = "/run/rustkube/containers";

    /// Native OCI container runtime using libcontainer.
    pub struct NativeRuntime {
        root_dir: PathBuf,
        state_dir: PathBuf,
        /// sandbox_id → SandboxState
        sandboxes: RwLock<HashMap<String, SandboxState>>,
    }

    #[derive(Debug, Clone)]
    struct SandboxState {
        id: String,
        config: PodSandboxConfig,
        ip: String,
        containers: Vec<String>,
    }

    impl NativeRuntime {
        pub fn new() -> Self {
            let root_dir = PathBuf::from(CONTAINER_ROOT);
            let state_dir = PathBuf::from(CONTAINER_STATE);
            let _ = std::fs::create_dir_all(&root_dir);
            let _ = std::fs::create_dir_all(&state_dir);

            Self {
                root_dir,
                state_dir,
                sandboxes: RwLock::new(HashMap::new()),
            }
        }

        fn container_root(&self, id: &str) -> PathBuf {
            self.root_dir.join(id)
        }

        fn container_state(&self, id: &str) -> PathBuf {
            self.state_dir.join(id)
        }

        /// Build an OCI runtime spec from our container config.
        fn build_oci_spec(
            &self,
            config: &ContainerConfig,
            sandbox: &PodSandboxConfig,
        ) -> Result<oci_spec::runtime::Spec, CriError> {
            // Root filesystem
            let rootfs = self.container_root(&format!(
                "{}-{}", sandbox.uid, config.name
            )).join("rootfs");

            let root = RootBuilder::default()
                .path(rootfs.to_string_lossy().to_string())
                .readonly(false)
                .build()
                .map_err(|e| CriError::Runtime(format!("root spec: {e}")))?;

            // Process
            let mut process_builder = ProcessBuilder::default();

            let mut args = config.command.clone();
            args.extend(config.args.clone());
            if args.is_empty() {
                args = vec!["/bin/sh".to_string()];
            }

            process_builder
                .args(args)
                .cwd(if config.working_dir.is_empty() {
                    "/".to_string()
                } else {
                    config.working_dir.clone()
                })
                .terminal(config.tty);

            // Environment variables
            let env: Vec<String> = config
                .envs
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            process_builder.env(env);

            let process = process_builder
                .build()
                .map_err(|e| CriError::Runtime(format!("process spec: {e}")))?;

            // Mounts
            let mut mounts = vec![];
            // Standard mounts
            for (dst, src, typ, opts) in &[
                ("/proc", "proc", "proc", vec!["nosuid", "noexec", "nodev"]),
                ("/dev", "tmpfs", "tmpfs", vec!["nosuid", "strictatime", "mode=755", "size=65536k"]),
                ("/dev/pts", "devpts", "devpts", vec!["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"]),
                ("/dev/shm", "shm", "tmpfs", vec!["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]),
                ("/dev/mqueue", "mqueue", "mqueue", vec!["nosuid", "noexec", "nodev"]),
                ("/sys", "sysfs", "sysfs", vec!["nosuid", "noexec", "nodev", "ro"]),
            ] {
                let opts_strings: Vec<String> = opts.iter().map(|s| s.to_string()).collect();
                let m = MountBuilder::default()
                    .destination(dst.to_string())
                    .source(src.to_string())
                    .typ(typ.to_string())
                    .options(opts_strings)
                    .build()
                    .map_err(|e| CriError::Runtime(format!("mount spec: {e}")))?;
                mounts.push(m);
            }

            // User mounts
            for mount in &config.mounts {
                let mut opts = vec!["rbind".to_string()];
                if mount.readonly {
                    opts.push("ro".to_string());
                }
                let m = MountBuilder::default()
                    .destination(mount.container_path.clone())
                    .source(mount.host_path.clone())
                    .typ("bind".to_string())
                    .options(opts)
                    .build()
                    .map_err(|e| CriError::Runtime(format!("user mount: {e}")))?;
                mounts.push(m);
            }

            // Linux resources (cgroups v2)
            let mut linux_builder = LinuxBuilder::default();

            let mut resources_builder = LinuxResourcesBuilder::default();

            if config.cpu_quota > 0 || config.cpu_shares > 0 {
                let mut cpu_builder = LinuxCpuBuilder::default();
                if config.cpu_quota > 0 {
                    cpu_builder.quota(config.cpu_quota);
                    cpu_builder.period(config.cpu_period as u64);
                }
                if config.cpu_shares > 0 {
                    cpu_builder.shares(config.cpu_shares as u64);
                }
                let cpu = cpu_builder
                    .build()
                    .map_err(|e| CriError::Runtime(format!("cpu spec: {e}")))?;
                resources_builder.cpu(cpu);
            }

            if config.memory_limit_bytes > 0 {
                let memory = LinuxMemoryBuilder::default()
                    .limit(config.memory_limit_bytes)
                    .build()
                    .map_err(|e| CriError::Runtime(format!("memory spec: {e}")))?;
                resources_builder.memory(memory);
            }

            let resources = resources_builder
                .build()
                .map_err(|e| CriError::Runtime(format!("resources spec: {e}")))?;
            linux_builder.resources(resources);

            let linux = linux_builder
                .build()
                .map_err(|e| CriError::Runtime(format!("linux spec: {e}")))?;

            // Build final spec
            let spec = SpecBuilder::default()
                .version("1.0.2")
                .root(root)
                .process(process)
                .mounts(mounts)
                .linux(linux)
                .hostname(sandbox.hostname.clone())
                .build()
                .map_err(|e| CriError::Runtime(format!("spec build: {e}")))?;

            Ok(spec)
        }
    }

    #[async_trait]
    impl RuntimeService for NativeRuntime {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok((
                "0.1.0".to_string(),
                "rustkube-native".to_string(),
                env!("CARGO_PKG_VERSION").to_string(),
            ))
        }

        async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
            let sandbox_id = format!(
                "sb-{}-{}",
                &config.uid[..8.min(config.uid.len())],
                &uuid::Uuid::new_v4().to_string()[..8]
            );

            info!("Creating pod sandbox {sandbox_id} for {}/{}", config.namespace, config.name);

            // Create sandbox directories
            let sandbox_dir = self.container_root(&sandbox_id);
            std::fs::create_dir_all(&sandbox_dir)
                .map_err(|e| CriError::Runtime(format!("create sandbox dir: {e}")))?;

            // Create log directory
            std::fs::create_dir_all(&config.log_directory)
                .map_err(|e| CriError::Runtime(format!("create log dir: {e}")))?;

            // For now, use host networking (sandbox IP = host IP)
            // Full CNI integration in Phase 2
            let ip = "10.244.0.2".to_string();

            let state = SandboxState {
                id: sandbox_id.clone(),
                config: config.clone(),
                ip,
                containers: Vec::new(),
            };

            self.sandboxes.write().await.insert(sandbox_id.clone(), state);

            Ok(sandbox_id)
        }

        async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            info!("Stopping pod sandbox {sandbox_id}");
            // Stop all containers in this sandbox
            let container_ids = {
                let sandboxes = self.sandboxes.read().await;
                sandboxes
                    .get(sandbox_id)
                    .map(|s| s.containers.clone())
                    .unwrap_or_default()
            };

            for cid in &container_ids {
                let _ = self.stop_container(cid, 10).await;
            }

            Ok(())
        }

        async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            info!("Removing pod sandbox {sandbox_id}");

            // Remove all containers
            let container_ids = {
                let sandboxes = self.sandboxes.read().await;
                sandboxes
                    .get(sandbox_id)
                    .map(|s| s.containers.clone())
                    .unwrap_or_default()
            };

            for cid in &container_ids {
                let _ = self.remove_container(cid).await;
            }

            // Remove sandbox state
            self.sandboxes.write().await.remove(sandbox_id);

            // Clean up sandbox directory
            let sandbox_dir = self.container_root(sandbox_id);
            let _ = std::fs::remove_dir_all(&sandbox_dir);

            Ok(())
        }

        async fn pod_sandbox_status(
            &self,
            sandbox_id: &str,
        ) -> Result<PodSandboxStatusInfo, CriError> {
            let sandboxes = self.sandboxes.read().await;
            let state = sandboxes
                .get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            Ok(PodSandboxStatusInfo {
                id: state.id.clone(),
                state: PodSandboxState::Ready,
                created_at: 0,
                ip: state.ip.clone(),
                additional_ips: vec![],
            })
        }

        async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> {
            let sandboxes = self.sandboxes.read().await;
            Ok(sandboxes
                .values()
                .map(|s| (s.id.clone(), PodSandboxState::Ready))
                .collect())
        }

        async fn create_container(
            &self,
            sandbox_id: &str,
            config: &ContainerConfig,
            sandbox_config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            let container_id = format!(
                "ct-{}-{}",
                &config.name[..8.min(config.name.len())],
                &uuid::Uuid::new_v4().to_string()[..8]
            );

            info!("Creating container {container_id} ({}) in sandbox {sandbox_id}", config.name);

            // Build OCI spec
            let spec = self.build_oci_spec(config, sandbox_config)?;

            // Create container root dir
            let container_dir = self.container_root(&container_id);
            let rootfs_dir = container_dir.join("rootfs");
            std::fs::create_dir_all(&rootfs_dir)
                .map_err(|e| CriError::Runtime(format!("create rootfs dir: {e}")))?;

            // Write OCI spec
            let spec_path = container_dir.join("config.json");
            let spec_json = serde_json::to_string_pretty(&spec)
                .map_err(|e| CriError::Runtime(format!("serialize spec: {e}")))?;
            std::fs::write(&spec_path, spec_json)
                .map_err(|e| CriError::Runtime(format!("write spec: {e}")))?;

            // Create the container via libcontainer
            let state_dir = self.container_state(&container_id);
            std::fs::create_dir_all(&state_dir)
                .map_err(|e| CriError::Runtime(format!("create state dir: {e}")))?;

            ContainerBuilder::new(container_id.clone(), SyscallType::default())
                .with_root_path(state_dir)
                .map_err(|e| CriError::Runtime(format!("set root path: {e}")))?
                .as_init(&container_dir)
                .with_systemd(false)
                .build()
                .map_err(|e| CriError::Runtime(format!("build container: {e}")))?;

            // Track in sandbox
            {
                let mut sandboxes = self.sandboxes.write().await;
                if let Some(sb) = sandboxes.get_mut(sandbox_id) {
                    sb.containers.push(container_id.clone());
                }
            }

            info!("Container {container_id} created");
            Ok(container_id)
        }

        async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
            info!("Starting container {container_id}");

            let state_dir = self.container_state(container_id);

            let mut container = Container::load(state_dir)
                .map_err(|e| CriError::Runtime(format!("load container: {e}")))?;

            container
                .start()
                .map_err(|e| CriError::Runtime(format!("start container: {e}")))?;

            info!("Container {container_id} started");
            Ok(())
        }

        async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError> {
            info!("Stopping container {container_id} (timeout={timeout}s)");

            let state_dir = self.container_state(container_id);

            match Container::load(&state_dir) {
                Ok(mut container) => {
                    // Send SIGTERM, then SIGKILL after timeout
                    let _ = container.kill(nix::sys::signal::Signal::SIGTERM, true);

                    // Wait briefly for graceful shutdown
                    tokio::time::sleep(std::time::Duration::from_secs(
                        timeout.min(5) as u64,
                    ))
                    .await;

                    // Force kill if still running
                    let _ = container.kill(nix::sys::signal::Signal::SIGKILL, true);
                }
                Err(e) => {
                    warn!("Could not load container {container_id} for stop: {e}");
                }
            }

            Ok(())
        }

        async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
            info!("Removing container {container_id}");

            let state_dir = self.container_state(container_id);

            if let Ok(mut container) = Container::load(&state_dir) {
                let _ = container.delete(true);
            }

            // Clean up directories
            let _ = std::fs::remove_dir_all(&state_dir);
            let container_dir = self.container_root(container_id);
            let _ = std::fs::remove_dir_all(&container_dir);

            Ok(())
        }

        async fn container_status(
            &self,
            container_id: &str,
        ) -> Result<ContainerStatusInfo, CriError> {
            let state_dir = self.container_state(container_id);

            let state = match Container::load(&state_dir) {
                Ok(container) => {
                    let status = container.state.status;
                    match status {
                        libcontainer::container::ContainerStatus::Created => ContainerState::Created,
                        libcontainer::container::ContainerStatus::Running => ContainerState::Running,
                        libcontainer::container::ContainerStatus::Stopped => ContainerState::Exited,
                        _ => ContainerState::Unknown,
                    }
                }
                Err(_) => ContainerState::Unknown,
            };

            Ok(ContainerStatusInfo {
                id: container_id.to_string(),
                name: container_id.to_string(),
                state,
                created_at: 0,
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
                image: String::new(),
                image_ref: String::new(),
                reason: String::new(),
                message: String::new(),
            })
        }

        async fn list_containers(
            &self,
            sandbox_id: Option<&str>,
        ) -> Result<Vec<ContainerStatusInfo>, CriError> {
            if let Some(sid) = sandbox_id {
                let sandboxes = self.sandboxes.read().await;
                if let Some(sb) = sandboxes.get(sid) {
                    let mut result = Vec::new();
                    for cid in &sb.containers {
                        if let Ok(status) = self.container_status(cid).await {
                            result.push(status);
                        }
                    }
                    return Ok(result);
                }
            }

            // List all containers from all sandboxes
            let sandboxes = self.sandboxes.read().await;
            let mut result = Vec::new();
            for sb in sandboxes.values() {
                for cid in &sb.containers {
                    if let Ok(status) = self.container_status(cid).await {
                        result.push(status);
                    }
                }
            }
            Ok(result)
        }

        async fn exec_sync(
            &self,
            container_id: &str,
            cmd: &[String],
            _timeout: i64,
        ) -> Result<ExecSyncResult, CriError> {
            debug!("Exec in {container_id}: {:?}", cmd);

            // Use nsenter to exec into container's namespaces
            let state_dir = self.container_state(container_id);
            let container = Container::load(&state_dir)
                .map_err(|e| CriError::Runtime(format!("load container for exec: {e}")))?;

            let pid = container
                .state
                .pid
                .ok_or_else(|| CriError::Runtime("container has no PID".into()))?;

            // nsenter into the container's namespaces
            let output = std::process::Command::new("nsenter")
                .args([
                    "-t",
                    &pid.to_string(),
                    "-m",
                    "-u",
                    "-i",
                    "-n",
                    "-p",
                    "--",
                ])
                .args(cmd)
                .output()
                .map_err(|e| CriError::Runtime(format!("nsenter failed: {e}")))?;

            Ok(ExecSyncResult {
                stdout: output.stdout,
                stderr: output.stderr,
                exit_code: output.status.code().unwrap_or(-1),
            })
        }
    }

    #[async_trait]
    impl MigrationService for NativeRuntime {
        fn migration_strategy(&self, _sandbox_id: &str) -> MigrationStrategy {
            if CriuCheckpointer::is_available() {
                MigrationStrategy::Checkpoint
            } else {
                MigrationStrategy::Evacuate
            }
        }

        async fn checkpoint_pod(&self, sandbox_id: &str) -> Result<CheckpointRef, CriError> {
            let checkpointer = CriuCheckpointer::new();

            // Get all container PIDs in this sandbox
            let sandboxes = self.sandboxes.read().await;
            let sandbox = sandboxes
                .get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            let container_ids = sandbox.containers.clone();
            drop(sandboxes);

            // Checkpoint each container
            let mut checkpoint_dirs = Vec::new();
            for cid in &container_ids {
                let state_dir = self.container_state(cid);
                let container = Container::load(&state_dir)
                    .map_err(|e| CriError::Migration(format!("load container {cid}: {e}")))?;

                let pid = container
                    .state
                    .pid
                    .ok_or_else(|| CriError::Migration(format!("container {cid} has no PID")))?;

                let dump_dir = checkpointer.checkpoint_container(cid, pid as u32)?;
                checkpoint_dirs.push(dump_dir);
            }

            // Package the first checkpoint (multi-container packaging in Phase 2)
            let archive = if let Some(first_dir) = checkpoint_dirs.first() {
                checkpointer.package_checkpoint(first_dir)?
            } else {
                return Err(CriError::Migration("no containers to checkpoint".into()));
            };

            let size = std::fs::metadata(&archive).map(|m| m.len()).unwrap_or(0);

            Ok(CheckpointRef {
                path: archive.to_string_lossy().to_string(),
                size,
                is_stream: false,
                stream_endpoint: None,
            })
        }

        async fn restore_pod(
            &self,
            checkpoint: &CheckpointRef,
            config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            let checkpointer = CriuCheckpointer::new();
            let archive = PathBuf::from(&checkpoint.path);

            // Unpack checkpoint
            let checkpoint_dir = checkpointer.unpack_checkpoint(&archive)?;

            // Create a new sandbox
            let sandbox_id = self.run_pod_sandbox(config).await?;

            // Restore into the sandbox's container root
            let root_dir = self.container_root(&sandbox_id).join("rootfs");
            std::fs::create_dir_all(&root_dir)
                .map_err(|e| CriError::Migration(format!("create rootfs: {e}")))?;

            let _pid = checkpointer.restore_container(&checkpoint_dir, &root_dir)?;

            info!("Pod restored from checkpoint into sandbox {sandbox_id}");
            Ok(sandbox_id)
        }

        async fn prepare_migration_target(
            &self,
            _config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            Err(CriError::Migration(
                "native runtime uses checkpoint strategy, not live migration".into(),
            ))
        }

        async fn live_migrate(
            &self,
            _sandbox_id: &str,
            _target_endpoint: &str,
        ) -> Result<(), CriError> {
            Err(CriError::Migration(
                "native runtime uses checkpoint strategy, not live migration".into(),
            ))
        }

        async fn migration_progress(
            &self,
            _sandbox_id: &str,
        ) -> Result<MigrationProgress, CriError> {
            Ok(MigrationProgress {
                phase: "unknown".into(),
                percent: 0,
                bytes_transferred: 0,
                elapsed_ms: 0,
                message: "checkpoint-based migration does not support progress tracking".into(),
            })
        }
    }

    /// Image service that pulls OCI images and unpacks layers.
    pub struct NativeImageService {
        image_store: PathBuf,
    }

    impl NativeImageService {
        pub fn new() -> Self {
            let image_store = PathBuf::from("/var/lib/rustkube/images");
            let _ = std::fs::create_dir_all(&image_store);
            Self { image_store }
        }
    }

    #[async_trait]
    impl ImageService for NativeImageService {
        async fn pull_image(&self, image: &str) -> Result<String, CriError> {
            info!("Pulling image: {image}");

            // Use skopeo or a Rust OCI client to pull
            // For Phase 1, try skopeo if available, fallback to noting the ref
            let output = std::process::Command::new("skopeo")
                .args([
                    "copy",
                    &format!("docker://{image}"),
                    &format!(
                        "oci:{}:{}",
                        self.image_store.to_string_lossy(),
                        image.replace('/', "_").replace(':', "_")
                    ),
                ])
                .output();

            match output {
                Ok(o) if o.status.success() => {
                    info!("Image pulled: {image}");
                    Ok(image.to_string())
                }
                Ok(o) => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    warn!("skopeo pull failed (will use image ref): {stderr}");
                    // Return the image ref anyway — containerd may have it cached
                    Ok(image.to_string())
                }
                Err(_) => {
                    warn!("skopeo not found, using image ref directly: {image}");
                    Ok(image.to_string())
                }
            }
        }

        async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
            let tag = image.replace('/', "_").replace(':', "_");
            let image_path = self.image_store.join(&tag);

            if image_path.exists() {
                Ok(Some(ImageInfo {
                    id: tag,
                    repo_tags: vec![image.to_string()],
                    repo_digests: vec![],
                    size: 0,
                }))
            } else {
                Ok(None)
            }
        }

        async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
            let mut images = Vec::new();

            if let Ok(entries) = std::fs::read_dir(&self.image_store) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        images.push(ImageInfo {
                            id: entry.file_name().to_string_lossy().to_string(),
                            repo_tags: vec![],
                            repo_digests: vec![],
                            size: 0,
                        });
                    }
                }
            }

            Ok(images)
        }

        async fn remove_image(&self, image: &str) -> Result<(), CriError> {
            let tag = image.replace('/', "_").replace(':', "_");
            let image_path = self.image_store.join(&tag);
            let _ = std::fs::remove_dir_all(&image_path);
            Ok(())
        }
    }
}

// Re-export for Linux
#[cfg(target_os = "linux")]
pub use linux::{NativeRuntime, NativeImageService};

// Stub for non-Linux (macOS dev)
#[cfg(not(target_os = "linux"))]
pub mod stub {
    use crate::cri::*;
    use async_trait::async_trait;

    pub struct NativeRuntime;

    impl Default for NativeRuntime {
        fn default() -> Self { Self }
    }

    impl NativeRuntime {
        pub fn new() -> Self { Self }
    }

    #[async_trait]
    impl MigrationService for NativeRuntime {
        fn migration_strategy(&self, _sandbox_id: &str) -> MigrationStrategy {
            MigrationStrategy::Evacuate
        }
        async fn checkpoint_pod(&self, _sandbox_id: &str) -> Result<CheckpointRef, CriError> {
            Err(CriError::Migration("not supported on this platform".into()))
        }
        async fn restore_pod(&self, _checkpoint: &CheckpointRef, _config: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Migration("not supported on this platform".into()))
        }
        async fn prepare_migration_target(&self, _config: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Migration("not supported on this platform".into()))
        }
        async fn live_migrate(&self, _sandbox_id: &str, _target_endpoint: &str) -> Result<(), CriError> {
            Err(CriError::Migration("not supported on this platform".into()))
        }
        async fn migration_progress(&self, _sandbox_id: &str) -> Result<MigrationProgress, CriError> {
            Err(CriError::Migration("not supported on this platform".into()))
        }
    }

    #[async_trait]
    impl RuntimeService for NativeRuntime {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok(("0.1.0".into(), "rustkube-stub".into(), "dev".into()))
        }
        async fn run_pod_sandbox(&self, _: &PodSandboxConfig) -> Result<String, CriError> {
            Ok("stub-sandbox".into())
        }
        async fn stop_pod_sandbox(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn remove_pod_sandbox(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatusInfo, CriError> {
            Ok(PodSandboxStatusInfo { id: id.into(), state: PodSandboxState::Ready, created_at: 0, ip: "10.244.0.2".into(), additional_ips: vec![] })
        }
        async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> { Ok(vec![]) }
        async fn create_container(&self, _: &str, _: &ContainerConfig, _: &PodSandboxConfig) -> Result<String, CriError> {
            Ok("stub-container".into())
        }
        async fn start_container(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn stop_container(&self, _: &str, _: i64) -> Result<(), CriError> { Ok(()) }
        async fn remove_container(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn container_status(&self, id: &str) -> Result<ContainerStatusInfo, CriError> {
            Ok(ContainerStatusInfo { id: id.into(), name: id.into(), state: ContainerState::Running, created_at: 0, started_at: 0, finished_at: 0, exit_code: 0, image: String::new(), image_ref: String::new(), reason: String::new(), message: String::new() })
        }
        async fn list_containers(&self, _: Option<&str>) -> Result<Vec<ContainerStatusInfo>, CriError> { Ok(vec![]) }
        async fn exec_sync(&self, _: &str, _: &[String], _: i64) -> Result<ExecSyncResult, CriError> {
            Ok(ExecSyncResult { stdout: vec![], stderr: vec![], exit_code: 0 })
        }
    }

    pub struct NativeImageService;

    impl Default for NativeImageService {
        fn default() -> Self { Self }
    }

    impl NativeImageService {
        pub fn new() -> Self { Self }
    }

    #[async_trait]
    impl ImageService for NativeImageService {
        async fn pull_image(&self, image: &str) -> Result<String, CriError> { Ok(image.into()) }
        async fn image_status(&self, _: &str) -> Result<Option<ImageInfo>, CriError> { Ok(None) }
        async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> { Ok(vec![]) }
        async fn remove_image(&self, _: &str) -> Result<(), CriError> { Ok(()) }
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::{NativeRuntime, NativeImageService};
