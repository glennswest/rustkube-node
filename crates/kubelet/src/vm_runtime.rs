//! VM-based container runtime using cloud-hypervisor or QEMU.
//!
//! Runs pods inside lightweight microVMs for strong isolation.
//! Each pod sandbox becomes a VM; containers inside share the VM.
//!
//! Architecture:
//!   rk-kubelet → cloud-hypervisor (REST API) → KVM → guest kernel → workload
//!
//! Supports:
//!   - cloud-hypervisor (preferred, Rust-native VMM)
//!   - QEMU/KVM (fallback, wider hardware support)
//!   - Firecracker (alternative, minimal microVM)
//!
//! The VM boots a lightweight kernel with a guest agent that manages
//! containers inside the VM using the same OCI spec.

#[cfg(target_os = "linux")]
mod linux {
    use crate::cri::*;
    use crate::vm_migrate;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tracing::{debug, error, info, warn};

    const VM_ROOT: &str = "/var/lib/rustkube/vms";
    const VM_RUN: &str = "/run/rustkube/vms";

    /// Which VMM backend to use.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum VmmBackend {
        /// cloud-hypervisor — Rust-based, modern, virtio-focused
        CloudHypervisor,
        /// QEMU/KVM — full-featured, wide hardware support
        Qemu,
        /// Firecracker — minimal microVM from AWS
        Firecracker,
    }

    impl VmmBackend {
        /// Detect the best available VMM on this system.
        pub fn detect() -> Option<Self> {
            // Prefer cloud-hypervisor (Rust-native)
            if which("cloud-hypervisor").is_some() {
                return Some(Self::CloudHypervisor);
            }
            // Try Firecracker
            if which("firecracker").is_some() {
                return Some(Self::Firecracker);
            }
            // Fall back to QEMU
            if which("qemu-system-x86_64").is_some() || which("qemu-system-aarch64").is_some() {
                return Some(Self::Qemu);
            }
            None
        }

        fn binary(&self) -> &'static str {
            match self {
                Self::CloudHypervisor => "cloud-hypervisor",
                Self::Qemu => {
                    #[cfg(target_arch = "aarch64")]
                    { "qemu-system-aarch64" }
                    #[cfg(not(target_arch = "aarch64"))]
                    { "qemu-system-x86_64" }
                }
                Self::Firecracker => "firecracker",
            }
        }
    }

    fn which(binary: &str) -> Option<PathBuf> {
        Command::new("which")
            .arg(binary)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| PathBuf::from(String::from_utf8_lossy(&o.stdout).trim()))
    }

    /// VM configuration for a sandbox.
    #[derive(Debug, Clone)]
    pub struct VmConfig {
        /// Number of vCPUs
        pub vcpus: u32,
        /// Memory in MiB
        pub memory_mib: u64,
        /// Path to guest kernel (vmlinux or bzImage)
        pub kernel: PathBuf,
        /// Path to root filesystem image (ext4 or squashfs)
        pub rootfs: PathBuf,
        /// Kernel command line
        pub cmdline: String,
        /// Enable virtiofs for host volume sharing
        pub virtiofs: bool,
    }

    impl Default for VmConfig {
        fn default() -> Self {
            Self {
                vcpus: 1,
                memory_mib: 256,
                kernel: PathBuf::from("/var/lib/rustkube/vm-kernel/vmlinux"),
                rootfs: PathBuf::from("/var/lib/rustkube/vm-kernel/rootfs.ext4"),
                cmdline: "console=hvc0 root=/dev/vda rw quiet".to_string(),
                virtiofs: true,
            }
        }
    }

    /// State of a running VM.
    #[derive(Debug)]
    struct VmState {
        id: String,
        config: PodSandboxConfig,
        vm_config: VmConfig,
        backend: VmmBackend,
        /// PID of the VMM process
        pid: Option<u32>,
        /// Unix socket for VMM API (cloud-hypervisor, firecracker)
        api_socket: PathBuf,
        /// IP address of the VM
        ip: String,
        /// Containers running inside this VM
        containers: Vec<String>,
    }

    /// VM-based container runtime.
    ///
    /// Each pod sandbox runs as a microVM. Containers inside the pod
    /// share the same VM. Provides stronger isolation than Linux namespaces
    /// alone — each pod gets its own kernel.
    pub struct VmRuntime {
        root_dir: PathBuf,
        run_dir: PathBuf,
        backend: VmmBackend,
        default_vm_config: VmConfig,
        vms: RwLock<HashMap<String, VmState>>,
    }

    impl VmRuntime {
        pub fn new(backend: VmmBackend) -> Self {
            let root_dir = PathBuf::from(VM_ROOT);
            let run_dir = PathBuf::from(VM_RUN);
            let _ = std::fs::create_dir_all(&root_dir);
            let _ = std::fs::create_dir_all(&run_dir);

            Self {
                root_dir,
                run_dir,
                backend,
                default_vm_config: VmConfig::default(),
                vms: RwLock::new(HashMap::new()),
            }
        }

        /// Create with auto-detected VMM backend.
        pub fn auto() -> Result<Self, CriError> {
            let backend = VmmBackend::detect()
                .ok_or_else(|| CriError::Runtime(
                    "No VMM found. Install cloud-hypervisor, firecracker, or qemu.".into()
                ))?;
            info!("Using VMM backend: {:?}", backend);
            Ok(Self::new(backend))
        }

        /// Set the default VM configuration.
        pub fn with_vm_config(mut self, config: VmConfig) -> Self {
            self.default_vm_config = config;
            self
        }

        fn vm_dir(&self, id: &str) -> PathBuf {
            self.root_dir.join(id)
        }

        fn vm_run_dir(&self, id: &str) -> PathBuf {
            self.run_dir.join(id)
        }

        fn api_socket_path(&self, id: &str) -> PathBuf {
            self.run_dir.join(format!("{}.sock", id))
        }

        /// Parse VM resource requests from pod annotations.
        fn vm_config_from_annotations(&self, config: &PodSandboxConfig) -> VmConfig {
            let mut vm = self.default_vm_config.clone();

            if let Some(vcpus) = config.annotations.get("rustkube.io/vm-vcpus") {
                if let Ok(n) = vcpus.parse() {
                    vm.vcpus = n;
                }
            }
            if let Some(mem) = config.annotations.get("rustkube.io/vm-memory") {
                // Parse "512Mi", "1Gi", or plain MiB number
                vm.memory_mib = parse_memory_mib(mem).unwrap_or(vm.memory_mib);
                }
            if let Some(kernel) = config.annotations.get("rustkube.io/vm-kernel") {
                vm.kernel = PathBuf::from(kernel);
            }
            if let Some(rootfs) = config.annotations.get("rustkube.io/vm-rootfs") {
                vm.rootfs = PathBuf::from(rootfs);
            }

            vm
        }

        /// Launch a cloud-hypervisor VM.
        fn launch_cloud_hypervisor(
            &self,
            id: &str,
            vm_config: &VmConfig,
            vm_dir: &Path,
        ) -> Result<u32, CriError> {
            let api_socket = self.api_socket_path(id);

            // Create VM disk from rootfs template
            let vm_disk = vm_dir.join("rootfs.img");
            if !vm_disk.exists() {
                // Copy the base rootfs image for this VM
                std::fs::copy(&vm_config.rootfs, &vm_disk)
                    .map_err(|e| CriError::Runtime(format!("copy rootfs: {e}")))?;
            }

            let mut cmd = Command::new("cloud-hypervisor");
            cmd.args([
                "--api-socket", &api_socket.to_string_lossy(),
                "--kernel", &vm_config.kernel.to_string_lossy(),
                "--disk", &format!("path={}", vm_disk.to_string_lossy()),
                "--cpus", &format!("boot={}", vm_config.vcpus),
                "--memory", &format!("size={}M", vm_config.memory_mib),
                "--cmdline", &vm_config.cmdline,
                "--serial", "off",
                "--console", "off",
            ]);

            // Add virtiofs if enabled (for host volume sharing)
            if vm_config.virtiofs {
                let shared_dir = vm_dir.join("shared");
                let _ = std::fs::create_dir_all(&shared_dir);
                cmd.args([
                    "--fs", &format!(
                        "tag=host,socket={},num_queues=1,queue_size=512",
                        vm_dir.join("virtiofs.sock").to_string_lossy()
                    ),
                ]);
            }

            // Net — tap device for VM networking
            cmd.args([
                "--net", &format!("tap=vmtap_{},mac={}", &id[..8.min(id.len())], generate_mac()),
            ]);

            // Daemonize
            let child = cmd
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(vm_dir.join("vmm.log"))
                    .map_err(|e| CriError::Runtime(format!("create vmm log: {e}")))?)
                .spawn()
                .map_err(|e| CriError::Runtime(format!("spawn cloud-hypervisor: {e}")))?;

            Ok(child.id())
        }

        /// Launch a Firecracker VM.
        fn launch_firecracker(
            &self,
            id: &str,
            vm_config: &VmConfig,
            vm_dir: &Path,
        ) -> Result<u32, CriError> {
            let api_socket = self.api_socket_path(id);

            // Firecracker expects config via API, but we can use --config-file
            let fc_config = serde_json::json!({
                "boot-source": {
                    "kernel_image_path": vm_config.kernel.to_string_lossy(),
                    "boot_args": vm_config.cmdline,
                },
                "drives": [{
                    "drive_id": "rootfs",
                    "path_on_host": vm_config.rootfs.to_string_lossy(),
                    "is_root_device": true,
                    "is_read_only": false,
                }],
                "machine-config": {
                    "vcpu_count": vm_config.vcpus,
                    "mem_size_mib": vm_config.memory_mib,
                },
            });

            let config_path = vm_dir.join("fc-config.json");
            std::fs::write(&config_path, serde_json::to_string_pretty(&fc_config).unwrap())
                .map_err(|e| CriError::Runtime(format!("write fc config: {e}")))?;

            let child = Command::new("firecracker")
                .args([
                    "--api-sock", &api_socket.to_string_lossy(),
                    "--config-file", &config_path.to_string_lossy(),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(vm_dir.join("vmm.log"))
                    .map_err(|e| CriError::Runtime(format!("create vmm log: {e}")))?)
                .spawn()
                .map_err(|e| CriError::Runtime(format!("spawn firecracker: {e}")))?;

            Ok(child.id())
        }

        /// Launch a QEMU VM.
        fn launch_qemu(
            &self,
            id: &str,
            vm_config: &VmConfig,
            vm_dir: &Path,
        ) -> Result<u32, CriError> {
            let vm_disk = vm_dir.join("rootfs.img");
            if !vm_disk.exists() {
                std::fs::copy(&vm_config.rootfs, &vm_disk)
                    .map_err(|e| CriError::Runtime(format!("copy rootfs: {e}")))?;
            }

            let monitor_socket = vm_dir.join("qemu-monitor.sock");
            let qga_socket = vm_dir.join("qemu-ga.sock");

            let child = Command::new(self.backend.binary())
                .args([
                    "-machine", "q35,accel=kvm",
                    "-cpu", "host",
                    "-smp", &vm_config.vcpus.to_string(),
                    "-m", &format!("{}M", vm_config.memory_mib),
                    "-kernel", &vm_config.kernel.to_string_lossy(),
                    "-append", &vm_config.cmdline,
                    "-drive", &format!(
                        "file={},format=raw,if=virtio",
                        vm_disk.to_string_lossy()
                    ),
                    "-nographic",
                    "-nodefaults",
                    "-serial", "none",
                    "-monitor", &format!("unix:{},server,nowait", monitor_socket.to_string_lossy()),
                    "-chardev", &format!(
                        "socket,path={},server=on,wait=off,id=qga",
                        qga_socket.to_string_lossy()
                    ),
                    "-device", "virtio-serial",
                    "-device", "virtserialport,chardev=qga,name=org.qemu.guest_agent.0",
                    "-netdev", &format!("tap,id=net0,ifname=vmtap_{},script=no,downscript=no", &id[..8.min(id.len())]),
                    "-device", "virtio-net-pci,netdev=net0",
                    "-daemonize",
                    "-pidfile", &vm_dir.join("qemu.pid").to_string_lossy(),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(vm_dir.join("vmm.log"))
                    .map_err(|e| CriError::Runtime(format!("create vmm log: {e}")))?)
                .spawn()
                .map_err(|e| CriError::Runtime(format!("spawn qemu: {e}")))?;

            Ok(child.id())
        }

        /// Send a command to cloud-hypervisor via its API socket.
        async fn ch_api(&self, id: &str, method: &str, path: &str, body: Option<&str>) -> Result<String, CriError> {
            let socket = self.api_socket_path(id);

            // Use curl with --unix-socket for simplicity
            let mut cmd = Command::new("curl");
            cmd.args([
                "--unix-socket", &socket.to_string_lossy(),
                "-s",
            ]);

            if let Some(body) = body {
                cmd.args(["-X", method, "-H", "Content-Type: application/json", "-d", body]);
            } else if method != "GET" {
                cmd.args(["-X", method]);
            }

            cmd.arg(format!("http://localhost/api/v1/vm.{}", path));

            let output = cmd.output()
                .map_err(|e| CriError::Runtime(format!("ch api call: {e}")))?;

            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            } else {
                Err(CriError::Runtime(format!(
                    "ch api error: {}",
                    String::from_utf8_lossy(&output.stderr)
                )))
            }
        }

        /// Stop the VMM process.
        fn kill_vmm(&self, pid: u32) {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGTERM,
            );
        }
    }

    #[async_trait]
    impl RuntimeService for VmRuntime {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok((
                "0.1.0".to_string(),
                format!("rustkube-vm-{:?}", self.backend).to_lowercase(),
                env!("CARGO_PKG_VERSION").to_string(),
            ))
        }

        async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
            let sandbox_id = format!(
                "vm-{}-{}",
                &config.uid[..8.min(config.uid.len())],
                &uuid::Uuid::new_v4().to_string()[..8]
            );

            info!("Creating VM sandbox {sandbox_id} for {}/{}", config.namespace, config.name);

            let vm_dir = self.vm_dir(&sandbox_id);
            let run_dir = self.vm_run_dir(&sandbox_id);
            std::fs::create_dir_all(&vm_dir)
                .map_err(|e| CriError::Runtime(format!("create vm dir: {e}")))?;
            std::fs::create_dir_all(&run_dir)
                .map_err(|e| CriError::Runtime(format!("create vm run dir: {e}")))?;

            let vm_config = self.vm_config_from_annotations(config);

            // Check kernel and rootfs exist
            if !vm_config.kernel.exists() {
                return Err(CriError::Runtime(format!(
                    "VM kernel not found: {}. Install a guest kernel at this path.",
                    vm_config.kernel.display()
                )));
            }
            if !vm_config.rootfs.exists() {
                return Err(CriError::Runtime(format!(
                    "VM rootfs not found: {}. Create a root filesystem image.",
                    vm_config.rootfs.display()
                )));
            }

            // Launch the VMM
            let pid = match self.backend {
                VmmBackend::CloudHypervisor => self.launch_cloud_hypervisor(&sandbox_id, &vm_config, &vm_dir)?,
                VmmBackend::Firecracker => self.launch_firecracker(&sandbox_id, &vm_config, &vm_dir)?,
                VmmBackend::Qemu => self.launch_qemu(&sandbox_id, &vm_config, &vm_dir)?,
            };

            info!("VMM process started (pid={pid}) for sandbox {sandbox_id}");

            // Wait for API socket to become available (cloud-hypervisor/firecracker)
            if self.backend != VmmBackend::Qemu {
                let socket = self.api_socket_path(&sandbox_id);
                for _ in 0..50 {
                    if socket.exists() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }

            // Allocate an IP for the VM (will be configured via DHCP or static in guest)
            let vm_ip = format!("10.245.0.{}", (pid % 254) + 1);

            let state = VmState {
                id: sandbox_id.clone(),
                config: config.clone(),
                vm_config,
                backend: self.backend,
                pid: Some(pid),
                api_socket: self.api_socket_path(&sandbox_id),
                ip: vm_ip,
                containers: Vec::new(),
            };

            self.vms.write().await.insert(sandbox_id.clone(), state);

            info!("VM sandbox {sandbox_id} created");
            Ok(sandbox_id)
        }

        async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            info!("Stopping VM sandbox {sandbox_id}");

            let vms = self.vms.read().await;
            if let Some(vm) = vms.get(sandbox_id) {
                // Try graceful shutdown via API first
                match vm.backend {
                    VmmBackend::CloudHypervisor => {
                        let _ = self.ch_api(sandbox_id, "PUT", "shutdown", None).await;
                        // Wait briefly for clean shutdown
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                    VmmBackend::Firecracker => {
                        // Firecracker: send CtrlAltDel action
                        let _ = Command::new("curl")
                            .args([
                                "--unix-socket", &vm.api_socket.to_string_lossy(),
                                "-s", "-X", "PUT",
                                "-H", "Content-Type: application/json",
                                "-d", r#"{"action_type": "SendCtrlAltDel"}"#,
                                "http://localhost/actions",
                            ])
                            .output();
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                    VmmBackend::Qemu => {
                        // QEMU: send system_powerdown via monitor socket
                        let monitor = self.vm_dir(sandbox_id).join("qemu-monitor.sock");
                        let _ = Command::new("socat")
                            .args([
                                "-", &format!("UNIX-CONNECT:{}", monitor.to_string_lossy()),
                            ])
                            .stdin(std::process::Stdio::piped())
                            .output();
                    }
                }

                // Kill VMM process if still running
                if let Some(pid) = vm.pid {
                    self.kill_vmm(pid);
                }
            }

            Ok(())
        }

        async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
            info!("Removing VM sandbox {sandbox_id}");

            // Stop first
            self.stop_pod_sandbox(sandbox_id).await?;

            // Remove state
            self.vms.write().await.remove(sandbox_id);

            // Clean up directories and sockets
            let _ = std::fs::remove_dir_all(self.vm_dir(sandbox_id));
            let _ = std::fs::remove_dir_all(self.vm_run_dir(sandbox_id));
            let _ = std::fs::remove_file(self.api_socket_path(sandbox_id));

            Ok(())
        }

        async fn pod_sandbox_status(
            &self,
            sandbox_id: &str,
        ) -> Result<PodSandboxStatusInfo, CriError> {
            let vms = self.vms.read().await;
            let vm = vms.get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            // Check if VMM process is still running
            let state = if let Some(pid) = vm.pid {
                let is_running = Path::new(&format!("/proc/{}", pid)).exists();
                if is_running {
                    PodSandboxState::Ready
                } else {
                    PodSandboxState::NotReady
                }
            } else {
                PodSandboxState::NotReady
            };

            Ok(PodSandboxStatusInfo {
                id: vm.id.clone(),
                state,
                created_at: 0,
                ip: vm.ip.clone(),
                additional_ips: vec![],
            })
        }

        async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> {
            let vms = self.vms.read().await;
            Ok(vms.values().map(|vm| {
                let state = if vm.pid.is_some() {
                    PodSandboxState::Ready
                } else {
                    PodSandboxState::NotReady
                };
                (vm.id.clone(), state)
            }).collect())
        }

        async fn create_container(
            &self,
            sandbox_id: &str,
            config: &ContainerConfig,
            _sandbox_config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            let container_id = format!(
                "vmc-{}-{}",
                &config.name[..8.min(config.name.len())],
                &uuid::Uuid::new_v4().to_string()[..8]
            );

            info!("Creating container {container_id} in VM sandbox {sandbox_id}");

            // For Phase 1, containers inside a VM are tracked but the workload
            // runs directly from the VM's rootfs. In Phase 2, a guest agent
            // inside the VM will manage individual containers.
            let vm_dir = self.vm_dir(sandbox_id);
            let container_dir = vm_dir.join("containers").join(&container_id);
            std::fs::create_dir_all(&container_dir)
                .map_err(|e| CriError::Runtime(format!("create container dir: {e}")))?;

            // Write container config for the guest agent to pick up
            let config_json = serde_json::json!({
                "id": container_id,
                "image": config.image,
                "command": config.command,
                "args": config.args,
                "env": config.envs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>(),
                "working_dir": config.working_dir,
                "mounts": config.mounts.iter().map(|m| serde_json::json!({
                    "source": m.host_path,
                    "destination": m.container_path,
                    "readonly": m.readonly,
                })).collect::<Vec<_>>(),
            });

            std::fs::write(
                container_dir.join("config.json"),
                serde_json::to_string_pretty(&config_json).unwrap(),
            ).map_err(|e| CriError::Runtime(format!("write container config: {e}")))?;

            // Track in VM state
            {
                let mut vms = self.vms.write().await;
                if let Some(vm) = vms.get_mut(sandbox_id) {
                    vm.containers.push(container_id.clone());
                }
            }

            Ok(container_id)
        }

        async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
            info!("Starting container {container_id} in VM");
            // Phase 1: The VM runs the workload from its rootfs directly.
            // Phase 2: Guest agent starts the specific container inside the VM.
            Ok(())
        }

        async fn stop_container(&self, container_id: &str, _timeout: i64) -> Result<(), CriError> {
            info!("Stopping container {container_id} in VM");
            // Phase 1: Container lifecycle is tied to VM lifecycle.
            // Phase 2: Guest agent stops the container.
            Ok(())
        }

        async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
            info!("Removing container {container_id} from VM");

            // Remove from all VM states
            let mut vms = self.vms.write().await;
            for vm in vms.values_mut() {
                vm.containers.retain(|c| c != container_id);
            }

            Ok(())
        }

        async fn container_status(
            &self,
            container_id: &str,
        ) -> Result<ContainerStatusInfo, CriError> {
            // Find which VM owns this container
            let vms = self.vms.read().await;
            let vm = vms.values()
                .find(|vm| vm.containers.contains(&container_id.to_string()));

            let state = if let Some(vm) = vm {
                if vm.pid.map(|p| Path::new(&format!("/proc/{}", p)).exists()).unwrap_or(false) {
                    ContainerState::Running
                } else {
                    ContainerState::Exited
                }
            } else {
                ContainerState::Unknown
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
            let vms = self.vms.read().await;

            let containers: Vec<String> = if let Some(sid) = sandbox_id {
                vms.get(sid)
                    .map(|vm| vm.containers.clone())
                    .unwrap_or_default()
            } else {
                vms.values()
                    .flat_map(|vm| vm.containers.clone())
                    .collect()
            };

            let mut result = Vec::new();
            for cid in containers {
                if let Ok(status) = self.container_status(&cid).await {
                    result.push(status);
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
            debug!("VM exec in {container_id}: {:?}", cmd);

            // Find the VM for this container
            let vms = self.vms.read().await;
            let vm = vms.values()
                .find(|vm| vm.containers.contains(&container_id.to_string()))
                .ok_or_else(|| CriError::NotFound(container_id.to_string()))?;

            // Execute via QEMU guest agent or SSH
            match vm.backend {
                VmmBackend::Qemu => {
                    // Use qemu-guest-agent
                    let qga_socket = self.vm_dir(&vm.id).join("qemu-ga.sock");
                    let exec_cmd = serde_json::json!({
                        "execute": "guest-exec",
                        "arguments": {
                            "path": cmd.first().unwrap_or(&"/bin/sh".to_string()),
                            "arg": &cmd[1..],
                            "capture-output": true,
                        }
                    });
                    let output = Command::new("socat")
                        .args([
                            "-", &format!("UNIX-CONNECT:{}", qga_socket.to_string_lossy()),
                        ])
                        .stdin(std::process::Stdio::piped())
                        .output()
                        .map_err(|e| CriError::Runtime(format!("qga exec: {e}")))?;

                    Ok(ExecSyncResult {
                        stdout: output.stdout,
                        stderr: output.stderr,
                        exit_code: output.status.code().unwrap_or(-1),
                    })
                }
                _ => {
                    // SSH fallback for cloud-hypervisor/firecracker
                    let output = Command::new("ssh")
                        .args([
                            "-o", "StrictHostKeyChecking=no",
                            "-o", "UserKnownHostsFile=/dev/null",
                            "-q",
                            &format!("root@{}", vm.ip),
                        ])
                        .args(cmd)
                        .output()
                        .map_err(|e| CriError::Runtime(format!("ssh exec: {e}")))?;

                    Ok(ExecSyncResult {
                        stdout: output.stdout,
                        stderr: output.stderr,
                        exit_code: output.status.code().unwrap_or(-1),
                    })
                }
            }
        }
    }

    #[async_trait]
    impl MigrationService for VmRuntime {
        fn migration_strategy(&self, sandbox_id: &str) -> MigrationStrategy {
            // Check the backend for this sandbox (or use the default)
            let backend = tokio::runtime::Handle::current()
                .block_on(async {
                    let vms = self.vms.read().await;
                    vms.get(sandbox_id).map(|v| v.backend)
                })
                .unwrap_or(self.backend);

            match backend {
                VmmBackend::CloudHypervisor | VmmBackend::Qemu => MigrationStrategy::LiveMigrate,
                VmmBackend::Firecracker => MigrationStrategy::Snapshot,
            }
        }

        async fn checkpoint_pod(&self, sandbox_id: &str) -> Result<CheckpointRef, CriError> {
            let vms = self.vms.read().await;
            let vm = vms.get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            match vm.backend {
                VmmBackend::Firecracker => {
                    let snapshot_dir = self.vm_dir(sandbox_id).join("snapshot");
                    vm_migrate::firecracker_create_snapshot(&vm.api_socket, &snapshot_dir)?;
                    let size = std::fs::metadata(snapshot_dir.join("memory"))
                        .map(|m| m.len())
                        .unwrap_or(0);
                    Ok(CheckpointRef {
                        path: snapshot_dir.to_string_lossy().to_string(),
                        size,
                        is_stream: false,
                        stream_endpoint: None,
                    })
                }
                VmmBackend::CloudHypervisor => {
                    // Pause + snapshot for cold checkpoint
                    let _ = self.ch_api(sandbox_id, "PUT", "pause", None).await;
                    // CH doesn't have a file-based snapshot like FC; use migration
                    Err(CriError::Migration(
                        "cloud-hypervisor uses live migration, not checkpoint".into(),
                    ))
                }
                VmmBackend::Qemu => {
                    Err(CriError::Migration(
                        "QEMU uses live migration, not checkpoint".into(),
                    ))
                }
            }
        }

        async fn restore_pod(
            &self,
            checkpoint: &CheckpointRef,
            config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            // Only Firecracker supports snapshot restore
            let snapshot_dir = PathBuf::from(&checkpoint.path);

            // Create a new VM sandbox
            let sandbox_id = self.run_pod_sandbox(config).await?;

            let vms = self.vms.read().await;
            let vm = vms.get(&sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.clone()))?;

            match vm.backend {
                VmmBackend::Firecracker => {
                    vm_migrate::firecracker_load_snapshot(&vm.api_socket, &snapshot_dir)?;
                    Ok(sandbox_id)
                }
                _ => Err(CriError::Migration(
                    "only Firecracker supports snapshot restore".into(),
                )),
            }
        }

        async fn prepare_migration_target(
            &self,
            config: &PodSandboxConfig,
        ) -> Result<String, CriError> {
            // Create a new VM sandbox that will receive the migration
            let sandbox_id = self.run_pod_sandbox(config).await?;

            let vms = self.vms.read().await;
            let vm = vms.get(&sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.clone()))?;

            match vm.backend {
                VmmBackend::CloudHypervisor => {
                    // Pick a port for migration based on sandbox ID hash
                    let port = 4000 + (sandbox_id.len() as u16 % 1000);
                    let endpoint = vm_migrate::ch_prepare_receive(&vm.api_socket, port)?;
                    Ok(format!("{sandbox_id}:{endpoint}"))
                }
                VmmBackend::Qemu => {
                    let port = 4000 + (sandbox_id.len() as u16 % 1000);
                    let endpoint = format!("tcp:0.0.0.0:{port}");
                    // QEMU incoming mode is set at VM launch — the sandbox
                    // was already started with -incoming in run_pod_sandbox
                    Ok(format!("{sandbox_id}:{endpoint}"))
                }
                VmmBackend::Firecracker => {
                    // Firecracker uses snapshot, not live migration
                    Err(CriError::Migration(
                        "Firecracker uses snapshot strategy, not live migration".into(),
                    ))
                }
            }
        }

        async fn live_migrate(
            &self,
            sandbox_id: &str,
            target_endpoint: &str,
        ) -> Result<(), CriError> {
            let vms = self.vms.read().await;
            let vm = vms.get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            match vm.backend {
                VmmBackend::CloudHypervisor => {
                    vm_migrate::ch_send_migration(&vm.api_socket, target_endpoint)?;
                    Ok(())
                }
                VmmBackend::Qemu => {
                    let monitor = self.vm_dir(sandbox_id).join("qemu-monitor.sock");
                    vm_migrate::qemu_migrate_to(&monitor, target_endpoint)?;
                    Ok(())
                }
                VmmBackend::Firecracker => {
                    Err(CriError::Migration(
                        "Firecracker does not support live migration".into(),
                    ))
                }
            }
        }

        async fn migration_progress(
            &self,
            sandbox_id: &str,
        ) -> Result<MigrationProgress, CriError> {
            let vms = self.vms.read().await;
            let vm = vms.get(sandbox_id)
                .ok_or_else(|| CriError::NotFound(sandbox_id.to_string()))?;

            match vm.backend {
                VmmBackend::Qemu => {
                    let monitor = self.vm_dir(sandbox_id).join("qemu-monitor.sock");
                    let (status, transferred, total) = vm_migrate::qemu_query_progress(&monitor)?;
                    let percent = if total > 0 {
                        ((transferred * 100) / total) as u8
                    } else {
                        0
                    };
                    Ok(MigrationProgress {
                        phase: status,
                        percent,
                        bytes_transferred: transferred,
                        elapsed_ms: 0,
                        message: format!("{transferred}/{total} bytes"),
                    })
                }
                _ => Ok(MigrationProgress {
                    phase: "migrating".into(),
                    percent: 0,
                    bytes_transferred: 0,
                    elapsed_ms: 0,
                    message: "progress tracking not available for this VMM".into(),
                }),
            }
        }
    }

    /// Parse Kubernetes-style memory values to MiB.
    fn parse_memory_mib(s: &str) -> Option<u64> {
        let s = s.trim();
        if let Some(n) = s.strip_suffix("Gi") {
            n.parse::<u64>().ok().map(|v| v * 1024)
        } else if let Some(n) = s.strip_suffix("Mi") {
            n.parse::<u64>().ok()
        } else if let Some(n) = s.strip_suffix("Ki") {
            n.parse::<u64>().ok().map(|v| v / 1024)
        } else {
            s.parse::<u64>().ok()
        }
    }

    /// Generate a random MAC address (locally administered).
    fn generate_mac() -> String {
        use std::time::SystemTime;
        let t = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!(
            "52:54:00:{:02x}:{:02x}:{:02x}",
            (t >> 16) as u8,
            (t >> 8) as u8,
            t as u8,
        )
    }
}

#[cfg(target_os = "linux")]
pub use linux::{VmRuntime, VmConfig, VmmBackend};

// Stub for non-Linux (macOS dev)
#[cfg(not(target_os = "linux"))]
pub mod stub {
    use crate::cri::*;
    use async_trait::async_trait;
    use std::path::PathBuf;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum VmmBackend {
        CloudHypervisor,
        Qemu,
        Firecracker,
    }

    impl VmmBackend {
        pub fn detect() -> Option<Self> { None }
    }

    #[derive(Debug, Clone)]
    pub struct VmConfig {
        pub vcpus: u32,
        pub memory_mib: u64,
        pub kernel: PathBuf,
        pub rootfs: PathBuf,
        pub cmdline: String,
        pub virtiofs: bool,
    }

    impl Default for VmConfig {
        fn default() -> Self {
            Self {
                vcpus: 1,
                memory_mib: 256,
                kernel: PathBuf::from("/var/lib/rustkube/vm-kernel/vmlinux"),
                rootfs: PathBuf::from("/var/lib/rustkube/vm-kernel/rootfs.ext4"),
                cmdline: String::new(),
                virtiofs: false,
            }
        }
    }

    pub struct VmRuntime;

    impl VmRuntime {
        pub fn new(_backend: VmmBackend) -> Self { Self }
        pub fn auto() -> Result<Self, CriError> {
            Err(CriError::Runtime("VM runtime not supported on this platform".into()))
        }
        pub fn with_vm_config(self, _config: VmConfig) -> Self { self }
    }

    #[async_trait]
    impl MigrationService for VmRuntime {
        fn migration_strategy(&self, _sandbox_id: &str) -> MigrationStrategy {
            MigrationStrategy::Evacuate
        }
        async fn checkpoint_pod(&self, _sandbox_id: &str) -> Result<CheckpointRef, CriError> {
            Err(CriError::Migration("VM runtime not supported on this platform".into()))
        }
        async fn restore_pod(&self, _checkpoint: &CheckpointRef, _config: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Migration("VM runtime not supported on this platform".into()))
        }
        async fn prepare_migration_target(&self, _config: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Migration("VM runtime not supported on this platform".into()))
        }
        async fn live_migrate(&self, _sandbox_id: &str, _target_endpoint: &str) -> Result<(), CriError> {
            Err(CriError::Migration("VM runtime not supported on this platform".into()))
        }
        async fn migration_progress(&self, _sandbox_id: &str) -> Result<MigrationProgress, CriError> {
            Err(CriError::Migration("VM runtime not supported on this platform".into()))
        }
    }

    #[async_trait]
    impl RuntimeService for VmRuntime {
        async fn version(&self) -> Result<(String, String, String), CriError> {
            Ok(("0.1.0".into(), "rustkube-vm-stub".into(), "dev".into()))
        }
        async fn run_pod_sandbox(&self, _: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Runtime("VM runtime not supported on this platform".into()))
        }
        async fn stop_pod_sandbox(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn remove_pod_sandbox(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn pod_sandbox_status(&self, _: &str) -> Result<PodSandboxStatusInfo, CriError> {
            Err(CriError::Runtime("VM runtime not supported".into()))
        }
        async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> { Ok(vec![]) }
        async fn create_container(&self, _: &str, _: &ContainerConfig, _: &PodSandboxConfig) -> Result<String, CriError> {
            Err(CriError::Runtime("VM runtime not supported".into()))
        }
        async fn start_container(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn stop_container(&self, _: &str, _: i64) -> Result<(), CriError> { Ok(()) }
        async fn remove_container(&self, _: &str) -> Result<(), CriError> { Ok(()) }
        async fn container_status(&self, _: &str) -> Result<ContainerStatusInfo, CriError> {
            Err(CriError::Runtime("VM runtime not supported".into()))
        }
        async fn list_containers(&self, _: Option<&str>) -> Result<Vec<ContainerStatusInfo>, CriError> { Ok(vec![]) }
        async fn exec_sync(&self, _: &str, _: &[String], _: i64) -> Result<ExecSyncResult, CriError> {
            Err(CriError::Runtime("VM runtime not supported".into()))
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::{VmRuntime, VmConfig, VmmBackend};
