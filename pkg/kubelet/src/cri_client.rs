//! CRI gRPC client.
//!
//! Connects to containerd or CRI-O via Unix socket and implements
//! the RuntimeService and ImageService traits from cri.rs.
//! Uses JSON over HTTP to the CRI runtime's API rather than raw gRPC
//! proto generation, for simplicity in Phase 1.

use crate::cri::*;
use async_trait::async_trait;
use std::process::Command;
use tracing::info;

/// CRI client connecting to a container runtime via its socket.
///
/// Phase 1 implementation uses `crictl` CLI as a bridge to the CRI socket.
/// Phase 2 will use direct tonic gRPC over Unix socket.
pub struct CriClient {
    socket_path: String,
    crictl_path: String,
}

impl CriClient {
    pub fn new(socket_path: &str) -> Self {
        let crictl_path = which_crictl().unwrap_or_else(|| "crictl".to_string());
        Self {
            socket_path: socket_path.to_string(),
            crictl_path,
        }
    }

    fn crictl(&self, args: &[&str]) -> Result<String, CriError> {
        let output = Command::new(&self.crictl_path)
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .args(args)
            .output()
            .map_err(|e| CriError::Connection(format!("failed to run crictl: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Runtime(format!(
                "crictl {} failed: {stderr}",
                args.join(" ")
            )));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn crictl_json(&self, args: &[&str]) -> Result<serde_json::Value, CriError> {
        let mut full_args = vec!["-o", "json"];
        full_args.extend_from_slice(args);
        let output = self.crictl(&full_args)?;
        serde_json::from_str(&output)
            .map_err(|e| CriError::Runtime(format!("failed to parse crictl JSON: {e}")))
    }
}

#[async_trait]
impl RuntimeService for CriClient {
    async fn version(&self) -> Result<(String, String, String), CriError> {
        let output = self.crictl(&["version"])?;
        // Parse version output
        let mut version = String::new();
        let mut runtime_name = String::new();
        let mut runtime_version = String::new();
        for line in output.lines() {
            if let Some(v) = line.strip_prefix("Version: ") {
                version = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("RuntimeName: ") {
                runtime_name = v.trim().to_string();
            } else if let Some(v) = line.strip_prefix("RuntimeVersion: ") {
                runtime_version = v.trim().to_string();
            }
        }
        Ok((version, runtime_name, runtime_version))
    }

    async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
        // Write sandbox config to temp file
        let sandbox_json = serde_json::json!({
            "metadata": {
                "name": config.name,
                "uid": config.uid,
                "namespace": config.namespace,
                "attempt": config.attempt
            },
            "hostname": config.hostname,
            "log_directory": config.log_directory,
            "dns_config": {
                "servers": config.dns_servers,
                "searches": config.dns_searches
            },
            "labels": config.labels,
            "annotations": config.annotations,
            "linux": {
                "security_context": {
                    "namespace_options": {
                        "network": 0
                    }
                }
            }
        });

        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| CriError::Runtime(format!("temp file: {e}")))?;
        std::fs::write(tmp.path(), sandbox_json.to_string())
            .map_err(|e| CriError::Runtime(format!("write config: {e}")))?;

        let path_str = tmp.path().to_string_lossy().to_string();
        let output = self.crictl(&["runp", &path_str])?;
        let sandbox_id = output.trim().to_string();
        info!("Created sandbox: {sandbox_id}");
        Ok(sandbox_id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        self.crictl(&["stopp", sandbox_id])?;
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        self.crictl(&["rmp", sandbox_id])?;
        Ok(())
    }

    async fn pod_sandbox_status(
        &self,
        sandbox_id: &str,
    ) -> Result<PodSandboxStatusInfo, CriError> {
        let json = self.crictl_json(&["inspectp", sandbox_id])?;

        let status = &json["status"];
        let state = if status["state"].as_str() == Some("SANDBOX_READY") {
            PodSandboxState::Ready
        } else {
            PodSandboxState::NotReady
        };

        let ip = status["network"]["ip"]
            .as_str()
            .unwrap_or("")
            .to_string();

        let additional_ips = status["network"]["additionalIps"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v["ip"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let created_at = status["createdAt"]
            .as_i64()
            .unwrap_or(0);

        Ok(PodSandboxStatusInfo {
            id: sandbox_id.to_string(),
            state,
            created_at,
            ip,
            additional_ips,
        })
    }

    async fn list_pod_sandbox(&self) -> Result<Vec<(String, PodSandboxState)>, CriError> {
        let json = self.crictl_json(&["pods"])?;
        let items = json["items"].as_array().cloned().unwrap_or_default();

        let mut result = Vec::new();
        for item in &items {
            let id = item["id"].as_str().unwrap_or("").to_string();
            let state = if item["state"].as_str() == Some("SANDBOX_READY") {
                PodSandboxState::Ready
            } else {
                PodSandboxState::NotReady
            };
            result.push((id, state));
        }

        Ok(result)
    }

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: &ContainerConfig,
        sandbox_config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        // Write container config
        let env_list: Vec<serde_json::Value> = config
            .envs
            .iter()
            .map(|(k, v)| {
                serde_json::json!({"key": k, "value": v})
            })
            .collect();

        let mounts_list: Vec<serde_json::Value> = config
            .mounts
            .iter()
            .map(|m| {
                serde_json::json!({
                    "container_path": m.container_path,
                    "host_path": m.host_path,
                    "readonly": m.readonly
                })
            })
            .collect();

        let container_json = serde_json::json!({
            "metadata": {
                "name": config.name,
                "attempt": config.attempt
            },
            "image": {
                "image": config.image
            },
            "command": config.command,
            "args": config.args,
            "working_dir": config.working_dir,
            "envs": env_list,
            "mounts": mounts_list,
            "labels": config.labels,
            "annotations": config.annotations,
            "log_path": config.log_path,
            "stdin": config.stdin,
            "tty": config.tty,
            "linux": {
                "resources": {
                    "cpu_period": config.cpu_period,
                    "cpu_quota": config.cpu_quota,
                    "cpu_shares": config.cpu_shares,
                    "memory_limit_in_bytes": config.memory_limit_bytes
                }
            }
        });

        let sandbox_json = serde_json::json!({
            "metadata": {
                "name": sandbox_config.name,
                "uid": sandbox_config.uid,
                "namespace": sandbox_config.namespace,
                "attempt": sandbox_config.attempt
            }
        });

        let container_tmp = tempfile::NamedTempFile::new()
            .map_err(|e| CriError::Runtime(format!("temp file: {e}")))?;
        std::fs::write(container_tmp.path(), container_json.to_string())
            .map_err(|e| CriError::Runtime(format!("write container config: {e}")))?;

        let sandbox_tmp = tempfile::NamedTempFile::new()
            .map_err(|e| CriError::Runtime(format!("temp file: {e}")))?;
        std::fs::write(sandbox_tmp.path(), sandbox_json.to_string())
            .map_err(|e| CriError::Runtime(format!("write sandbox config: {e}")))?;

        let container_path = container_tmp.path().to_string_lossy().to_string();
        let sandbox_path = sandbox_tmp.path().to_string_lossy().to_string();

        let output = self.crictl(&[
            "create",
            sandbox_id,
            &container_path,
            &sandbox_path,
        ])?;

        let container_id = output.trim().to_string();
        info!("Created container: {container_id}");
        Ok(container_id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
        self.crictl(&["start", container_id])?;
        Ok(())
    }

    async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError> {
        let timeout_str = timeout.to_string();
        self.crictl(&["stop", "--timeout", &timeout_str, container_id])?;
        Ok(())
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
        self.crictl(&["rm", container_id])?;
        Ok(())
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> Result<ContainerStatusInfo, CriError> {
        let json = self.crictl_json(&["inspect", container_id])?;
        let status = &json["status"];

        let state = match status["state"].as_str() {
            Some("CONTAINER_RUNNING") => ContainerState::Running,
            Some("CONTAINER_EXITED") => ContainerState::Exited,
            Some("CONTAINER_CREATED") => ContainerState::Created,
            _ => ContainerState::Unknown,
        };

        Ok(ContainerStatusInfo {
            id: container_id.to_string(),
            name: status["metadata"]["name"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            state,
            created_at: status["createdAt"].as_i64().unwrap_or(0),
            started_at: status["startedAt"].as_i64().unwrap_or(0),
            finished_at: status["finishedAt"].as_i64().unwrap_or(0),
            exit_code: status["exitCode"].as_i64().unwrap_or(0) as i32,
            image: status["image"]["image"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            image_ref: status["imageRef"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            reason: status["reason"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            message: status["message"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        })
    }

    async fn list_containers(
        &self,
        sandbox_id: Option<&str>,
    ) -> Result<Vec<ContainerStatusInfo>, CriError> {
        let args = if let Some(sid) = sandbox_id {
            vec!["ps", "-a", "--pod", sid]
        } else {
            vec!["ps", "-a"]
        };

        let json = self.crictl_json(&args)?;
        let containers = json["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let mut result = Vec::new();
        for c in &containers {
            let state = match c["state"].as_str() {
                Some("CONTAINER_RUNNING") => ContainerState::Running,
                Some("CONTAINER_EXITED") => ContainerState::Exited,
                Some("CONTAINER_CREATED") => ContainerState::Created,
                _ => ContainerState::Unknown,
            };

            result.push(ContainerStatusInfo {
                id: c["id"].as_str().unwrap_or("").to_string(),
                name: c["metadata"]["name"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                state,
                created_at: c["createdAt"].as_i64().unwrap_or(0),
                started_at: 0,
                finished_at: 0,
                exit_code: 0,
                image: c["image"]["image"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                image_ref: c["imageRef"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                reason: String::new(),
                message: String::new(),
            });
        }

        Ok(result)
    }

    async fn exec_sync(
        &self,
        container_id: &str,
        cmd: &[String],
        timeout: i64,
    ) -> Result<ExecSyncResult, CriError> {
        let timeout_str = timeout.to_string();
        let mut args = vec!["exec", "--timeout", &timeout_str, container_id];
        let cmd_strs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
        args.extend_from_slice(&cmd_strs);

        let output = Command::new(&self.crictl_path)
            .env("CONTAINER_RUNTIME_ENDPOINT", &self.socket_path)
            .args(&args)
            .output()
            .map_err(|e| CriError::Runtime(format!("exec failed: {e}")))?;

        Ok(ExecSyncResult {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

#[async_trait]
impl MigrationService for CriClient {
    fn migration_strategy(&self, _sandbox_id: &str) -> MigrationStrategy {
        MigrationStrategy::Evacuate
    }

    async fn checkpoint_pod(&self, _sandbox_id: &str) -> Result<CheckpointRef, CriError> {
        Err(CriError::Migration(
            "CRI runtime uses evacuate strategy — checkpoint not supported".into(),
        ))
    }

    async fn restore_pod(
        &self,
        _checkpoint: &CheckpointRef,
        _config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        Err(CriError::Migration(
            "CRI runtime uses evacuate strategy — restore not supported".into(),
        ))
    }

    async fn prepare_migration_target(
        &self,
        _config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        Err(CriError::Migration(
            "CRI runtime uses evacuate strategy — live migration not supported".into(),
        ))
    }

    async fn live_migrate(
        &self,
        _sandbox_id: &str,
        _target_endpoint: &str,
    ) -> Result<(), CriError> {
        Err(CriError::Migration(
            "CRI runtime uses evacuate strategy — live migration not supported".into(),
        ))
    }

    async fn migration_progress(
        &self,
        _sandbox_id: &str,
    ) -> Result<MigrationProgress, CriError> {
        Err(CriError::Migration(
            "CRI runtime uses evacuate strategy — no migration in progress".into(),
        ))
    }
}

#[async_trait]
impl ImageService for CriClient {
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        let output = self.crictl(&["pull", image])?;
        // crictl pull returns the image ref
        let image_ref = output.trim().to_string();
        if image_ref.is_empty() {
            Ok(image.to_string())
        } else {
            // Last line is the image ref
            Ok(image_ref
                .lines()
                .last()
                .unwrap_or(image)
                .trim()
                .to_string())
        }
    }

    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
        match self.crictl_json(&["inspecti", image]) {
            Ok(json) => {
                let status = &json["status"];
                Ok(Some(ImageInfo {
                    id: status["id"].as_str().unwrap_or("").to_string(),
                    repo_tags: status["repoTags"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    repo_digests: status["repoDigests"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    size: status["size"].as_u64().unwrap_or(0),
                }))
            }
            Err(_) => Ok(None),
        }
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
        let json = self.crictl_json(&["images"])?;
        let images = json["images"].as_array().cloned().unwrap_or_default();

        let mut result = Vec::new();
        for img in &images {
            result.push(ImageInfo {
                id: img["id"].as_str().unwrap_or("").to_string(),
                repo_tags: img["repoTags"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                repo_digests: img["repoDigests"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
                size: img["size"].as_u64().unwrap_or(0),
            });
        }

        Ok(result)
    }

    async fn remove_image(&self, image: &str) -> Result<(), CriError> {
        self.crictl(&["rmi", image])?;
        Ok(())
    }
}

/// Find crictl binary.
fn which_crictl() -> Option<String> {
    let paths = [
        "/usr/local/bin/crictl",
        "/usr/bin/crictl",
        "/opt/bin/crictl",
    ];
    for p in &paths {
        if std::path::Path::new(p).exists() {
            return Some(p.to_string());
        }
    }
    // Try PATH
    Command::new("which")
        .arg("crictl")
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
}

/// Default CRI socket paths to try.
pub fn detect_cri_socket() -> String {
    let candidates = [
        "/run/containerd/containerd.sock",
        "/run/crio/crio.sock",
        "/var/run/containerd/containerd.sock",
        "/var/run/crio/crio.sock",
        "/var/run/dockershim.sock",
    ];

    for path in &candidates {
        if std::path::Path::new(path).exists() {
            return format!("unix://{path}");
        }
    }

    "unix:///run/containerd/containerd.sock".to_string()
}
