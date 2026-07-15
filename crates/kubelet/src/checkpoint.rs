//! CRIU checkpoint/restore for native container migration.
//!
//! Uses CRIU (Checkpoint/Restore In Userspace) to freeze a running
//! container's process tree and memory state to disk, then restore it
//! on the same or a different node.
//!
//! Architecture:
//!   kubelet → CriuCheckpointer → criu CLI → kernel (ptrace, /proc)

#[cfg(target_os = "linux")]
mod linux {
    use crate::cri::CriError;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use tracing::{debug, info, warn};

    const CHECKPOINT_ROOT: &str = "/var/lib/rustkube/checkpoints";

    /// CRIU-based checkpoint/restore engine.
    pub struct CriuCheckpointer {
        checkpoint_dir: PathBuf,
    }

    impl CriuCheckpointer {
        pub fn new() -> Self {
            let checkpoint_dir = PathBuf::from(CHECKPOINT_ROOT);
            let _ = std::fs::create_dir_all(&checkpoint_dir);
            Self { checkpoint_dir }
        }

        /// Check if CRIU is available on this system.
        pub fn is_available() -> bool {
            Command::new("criu")
                .arg("check")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        }

        /// Checkpoint a single container by PID.
        pub fn checkpoint_container(
            &self,
            container_id: &str,
            pid: u32,
        ) -> Result<PathBuf, CriError> {
            let dump_dir = self.checkpoint_dir.join(container_id);
            std::fs::create_dir_all(&dump_dir)
                .map_err(|e| CriError::Migration(format!("create checkpoint dir: {e}")))?;

            info!("CRIU checkpoint: container={container_id} pid={pid}");

            let output = Command::new("criu")
                .args([
                    "dump",
                    "-t", &pid.to_string(),
                    "-D", &dump_dir.to_string_lossy(),
                    "--leave-stopped",
                    "--shell-job",
                    "--tcp-established",
                    "--ext-unix-sk",
                    "--file-locks",
                    "-v4",
                    "-o", &dump_dir.join("dump.log").to_string_lossy(),
                ])
                .output()
                .map_err(|e| CriError::Migration(format!("criu dump failed to execute: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CriError::Migration(format!(
                    "criu dump failed (exit {}): {stderr}",
                    output.status.code().unwrap_or(-1)
                )));
            }

            info!("CRIU checkpoint complete: {}", dump_dir.display());
            Ok(dump_dir)
        }

        /// Restore a container from a checkpoint directory.
        pub fn restore_container(
            &self,
            checkpoint_dir: &Path,
            root_dir: &Path,
        ) -> Result<u32, CriError> {
            info!(
                "CRIU restore: checkpoint={} root={}",
                checkpoint_dir.display(),
                root_dir.display()
            );

            let output = Command::new("criu")
                .args([
                    "restore",
                    "-D", &checkpoint_dir.to_string_lossy(),
                    "--root", &root_dir.to_string_lossy(),
                    "--shell-job",
                    "--tcp-established",
                    "--ext-unix-sk",
                    "--file-locks",
                    "-d",
                    "--pidfile", &checkpoint_dir.join("restore.pid").to_string_lossy(),
                    "-v4",
                    "-o", &checkpoint_dir.join("restore.log").to_string_lossy(),
                ])
                .output()
                .map_err(|e| CriError::Migration(format!("criu restore failed to execute: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CriError::Migration(format!(
                    "criu restore failed (exit {}): {stderr}",
                    output.status.code().unwrap_or(-1)
                )));
            }

            // Read restored PID
            let pid_file = checkpoint_dir.join("restore.pid");
            let pid_str = std::fs::read_to_string(&pid_file)
                .map_err(|e| CriError::Migration(format!("read restore pid: {e}")))?;
            let pid: u32 = pid_str
                .trim()
                .parse()
                .map_err(|e| CriError::Migration(format!("parse restore pid: {e}")))?;

            info!("CRIU restore complete: pid={pid}");
            Ok(pid)
        }

        /// Package a checkpoint directory into a tar.zst archive for transfer.
        pub fn package_checkpoint(
            &self,
            checkpoint_dir: &Path,
        ) -> Result<PathBuf, CriError> {
            let archive = checkpoint_dir.with_extension("tar.zst");

            debug!("Packaging checkpoint: {} → {}", checkpoint_dir.display(), archive.display());

            let output = Command::new("tar")
                .args([
                    "--zstd",
                    "-cf", &archive.to_string_lossy(),
                    "-C", &checkpoint_dir.parent().unwrap_or(Path::new("/")).to_string_lossy(),
                    &checkpoint_dir.file_name().unwrap_or_default().to_string_lossy(),
                ])
                .output()
                .map_err(|e| CriError::Migration(format!("tar checkpoint: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CriError::Migration(format!("tar checkpoint failed: {stderr}")));
            }

            let size = std::fs::metadata(&archive)
                .map(|m| m.len())
                .unwrap_or(0);
            info!("Checkpoint packaged: {} ({} bytes)", archive.display(), size);

            Ok(archive)
        }

        /// Unpack a checkpoint archive.
        pub fn unpack_checkpoint(
            &self,
            archive: &Path,
        ) -> Result<PathBuf, CriError> {
            let dest = self.checkpoint_dir.join(
                archive
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_suffix(".tar"))
                    .unwrap_or("restored"),
            );
            std::fs::create_dir_all(&dest)
                .map_err(|e| CriError::Migration(format!("create unpack dir: {e}")))?;

            debug!("Unpacking checkpoint: {} → {}", archive.display(), dest.display());

            let output = Command::new("tar")
                .args([
                    "--zstd",
                    "-xf", &archive.to_string_lossy(),
                    "-C", &dest.to_string_lossy(),
                ])
                .output()
                .map_err(|e| CriError::Migration(format!("untar checkpoint: {e}")))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CriError::Migration(format!("untar checkpoint failed: {stderr}")));
            }

            Ok(dest)
        }

        /// Clean up checkpoint data for a container.
        pub fn cleanup(&self, container_id: &str) {
            let dir = self.checkpoint_dir.join(container_id);
            let archive = dir.with_extension("tar.zst");
            if dir.exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
            if archive.exists() {
                let _ = std::fs::remove_file(&archive);
            }
            debug!("Cleaned up checkpoint for {container_id}");
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::CriuCheckpointer;

#[cfg(not(target_os = "linux"))]
pub mod stub {
    use crate::cri::CriError;
    use std::path::{Path, PathBuf};

    pub struct CriuCheckpointer;

    impl Default for CriuCheckpointer {
        fn default() -> Self { Self }
    }

    impl CriuCheckpointer {
        pub fn new() -> Self { Self }

        pub fn is_available() -> bool { false }

        pub fn checkpoint_container(&self, _container_id: &str, _pid: u32) -> Result<PathBuf, CriError> {
            Err(CriError::Migration("CRIU not supported on this platform".into()))
        }

        pub fn restore_container(&self, _checkpoint_dir: &Path, _root_dir: &Path) -> Result<u32, CriError> {
            Err(CriError::Migration("CRIU not supported on this platform".into()))
        }

        pub fn package_checkpoint(&self, _checkpoint_dir: &Path) -> Result<PathBuf, CriError> {
            Err(CriError::Migration("CRIU not supported on this platform".into()))
        }

        pub fn unpack_checkpoint(&self, _archive: &Path) -> Result<PathBuf, CriError> {
            Err(CriError::Migration("CRIU not supported on this platform".into()))
        }

        pub fn cleanup(&self, _container_id: &str) {}
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::CriuCheckpointer;
