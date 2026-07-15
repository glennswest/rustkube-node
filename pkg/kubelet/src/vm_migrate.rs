//! VM live migration helpers per VMM backend.
//!
//! Provides migration primitives for each supported hypervisor:
//! - cloud-hypervisor: REST API (`/api/v1/vm.send-migration`, `vm.receive-migration`)
//! - QEMU: QMP protocol over Unix socket (`migrate`, `query-migrate`)
//! - Firecracker: Snapshot API (`/snapshot/create`, `/snapshot/load`)

#[cfg(target_os = "linux")]
mod linux {
    use crate::cri::CriError;
    use std::path::Path;
    use std::process::Command;
    use tracing::{debug, info};

    // ── cloud-hypervisor ─────────────────────────────────────────

    /// Prepare a cloud-hypervisor VM to receive a live migration.
    ///
    /// Starts the receiver side listening on a TCP endpoint.
    /// Returns the endpoint URI to pass to the sender.
    pub fn ch_prepare_receive(api_socket: &Path, port: u16) -> Result<String, CriError> {
        let endpoint = format!("tcp:0.0.0.0:{port}");
        let body = serde_json::json!({
            "receiver_url": &endpoint,
        });

        info!("CH: preparing migration receiver on {endpoint}");

        let output = Command::new("curl")
            .args([
                "--unix-socket", &api_socket.to_string_lossy(),
                "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", &body.to_string(),
                "http://localhost/api/v1/vm.receive-migration",
            ])
            .output()
            .map_err(|e| CriError::Migration(format!("ch receive-migration: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Migration(format!("ch receive-migration failed: {stderr}")));
        }

        Ok(endpoint)
    }

    /// Send a cloud-hypervisor VM to a target migration endpoint.
    pub fn ch_send_migration(api_socket: &Path, target_uri: &str) -> Result<(), CriError> {
        let body = serde_json::json!({
            "destination_url": target_uri,
        });

        info!("CH: sending migration to {target_uri}");

        let output = Command::new("curl")
            .args([
                "--unix-socket", &api_socket.to_string_lossy(),
                "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", &body.to_string(),
                "http://localhost/api/v1/vm.send-migration",
            ])
            .output()
            .map_err(|e| CriError::Migration(format!("ch send-migration: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Migration(format!("ch send-migration failed: {stderr}")));
        }

        info!("CH: migration sent successfully");
        Ok(())
    }

    // ── QEMU ─────────────────────────────────────────────────────

    /// Send a QMP command to QEMU via its monitor socket.
    fn qmp_command(monitor_socket: &Path, command: &str) -> Result<String, CriError> {
        debug!("QMP: sending to {}: {command}", monitor_socket.display());

        // Use socat to talk to the QEMU monitor
        let output = Command::new("socat")
            .args([
                "-", &format!("UNIX-CONNECT:{}", monitor_socket.to_string_lossy()),
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| CriError::Migration(format!("qmp command: {e}")))?;

        // Note: QMP requires a capabilities negotiation first; this is simplified.
        // In production, use a proper QMP client crate.
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Start a QEMU VM in incoming migration mode.
    pub fn qemu_start_incoming(
        qemu_binary: &str,
        port: u16,
        vm_args: &[String],
    ) -> Result<u32, CriError> {
        info!("QEMU: starting in incoming mode on port {port}");

        let mut cmd = Command::new(qemu_binary);
        cmd.args(vm_args);
        cmd.args(["-incoming", &format!("tcp:0:{port}")]);

        let child = cmd
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| CriError::Migration(format!("qemu incoming spawn: {e}")))?;

        Ok(child.id())
    }

    /// Trigger QEMU live migration to a target.
    pub fn qemu_migrate_to(monitor_socket: &Path, target_uri: &str) -> Result<(), CriError> {
        info!("QEMU: migrating to {target_uri}");

        let cmd = serde_json::json!({
            "execute": "migrate",
            "arguments": {
                "uri": target_uri
            }
        });

        let _ = qmp_command(monitor_socket, &cmd.to_string())?;
        Ok(())
    }

    /// Query QEMU migration progress.
    pub fn qemu_query_progress(monitor_socket: &Path) -> Result<(String, u64, u64), CriError> {
        let cmd = serde_json::json!({
            "execute": "query-migrate"
        });

        let response = qmp_command(monitor_socket, &cmd.to_string())?;

        // Parse response — simplified; real QMP returns structured JSON
        let parsed: serde_json::Value = serde_json::from_str(&response).unwrap_or_default();
        let status = parsed["return"]["status"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let transferred = parsed["return"]["ram"]["transferred"]
            .as_u64()
            .unwrap_or(0);
        let total = parsed["return"]["ram"]["total"].as_u64().unwrap_or(0);

        Ok((status, transferred, total))
    }

    // ── Firecracker ──────────────────────────────────────────────

    /// Create a Firecracker snapshot.
    pub fn firecracker_create_snapshot(
        api_socket: &Path,
        snapshot_dir: &Path,
    ) -> Result<(), CriError> {
        let vmstate_path = snapshot_dir.join("vmstate");
        let memory_path = snapshot_dir.join("memory");

        std::fs::create_dir_all(snapshot_dir)
            .map_err(|e| CriError::Migration(format!("create snapshot dir: {e}")))?;

        info!("Firecracker: creating snapshot at {}", snapshot_dir.display());

        // Pause first
        let output = Command::new("curl")
            .args([
                "--unix-socket", &api_socket.to_string_lossy(),
                "-s", "-X", "PATCH",
                "-H", "Content-Type: application/json",
                "-d", r#"{"state":"Paused"}"#,
                "http://localhost/vm",
            ])
            .output()
            .map_err(|e| CriError::Migration(format!("fc pause: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Migration(format!("fc pause failed: {stderr}")));
        }

        // Create snapshot
        let body = serde_json::json!({
            "snapshot_type": "Full",
            "snapshot_path": vmstate_path.to_string_lossy(),
            "mem_file_path": memory_path.to_string_lossy(),
        });

        let output = Command::new("curl")
            .args([
                "--unix-socket", &api_socket.to_string_lossy(),
                "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", &body.to_string(),
                "http://localhost/snapshot/create",
            ])
            .output()
            .map_err(|e| CriError::Migration(format!("fc create snapshot: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Migration(format!("fc snapshot create failed: {stderr}")));
        }

        info!("Firecracker: snapshot created");
        Ok(())
    }

    /// Load a Firecracker snapshot.
    pub fn firecracker_load_snapshot(
        api_socket: &Path,
        snapshot_dir: &Path,
    ) -> Result<(), CriError> {
        let vmstate_path = snapshot_dir.join("vmstate");
        let memory_path = snapshot_dir.join("memory");

        info!("Firecracker: loading snapshot from {}", snapshot_dir.display());

        let body = serde_json::json!({
            "snapshot_path": vmstate_path.to_string_lossy(),
            "mem_file_path": memory_path.to_string_lossy(),
            "enable_diff_snapshots": false,
            "resume_vm": true,
        });

        let output = Command::new("curl")
            .args([
                "--unix-socket", &api_socket.to_string_lossy(),
                "-s", "-X", "PUT",
                "-H", "Content-Type: application/json",
                "-d", &body.to_string(),
                "http://localhost/snapshot/load",
            ])
            .output()
            .map_err(|e| CriError::Migration(format!("fc load snapshot: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CriError::Migration(format!("fc snapshot load failed: {stderr}")));
        }

        info!("Firecracker: snapshot loaded and resumed");
        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(not(target_os = "linux"))]
pub mod stub {
    use crate::cri::CriError;
    use std::path::Path;

    pub fn ch_prepare_receive(_api_socket: &Path, _port: u16) -> Result<String, CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn ch_send_migration(_api_socket: &Path, _target_uri: &str) -> Result<(), CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn qemu_start_incoming(_binary: &str, _port: u16, _args: &[String]) -> Result<u32, CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn qemu_migrate_to(_monitor_socket: &Path, _target_uri: &str) -> Result<(), CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn qemu_query_progress(_monitor_socket: &Path) -> Result<(String, u64, u64), CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn firecracker_create_snapshot(_api_socket: &Path, _snapshot_dir: &Path) -> Result<(), CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
    pub fn firecracker_load_snapshot(_api_socket: &Path, _snapshot_dir: &Path) -> Result<(), CriError> {
        Err(CriError::Migration("VM migration not supported on this platform".into()))
    }
}

#[cfg(not(target_os = "linux"))]
pub use stub::*;
