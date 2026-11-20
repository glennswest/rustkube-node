//! The stormpump runtime's records on disk (#334).
//!
//! The engine keeps a kubelet's workloads and their handles across a
//! reattach with the same [`crate::stormpump_ring::TOKEN`], and hands it the
//! exits it missed. What the kubelet lost on a restart was its own side: the
//! sandbox and container maps lived only in memory. So a restarted kubelet
//! listed no pods, started every one a second time, and left the first set
//! running with nothing supervising it.
//!
//! Every sandbox and container is now written to [`STATE_FILE`] with its
//! engine handles, whole, after each change. A restarted kubelet reads it
//! back before anything else asks ([`StormpumpRuntime::with_state`]), asks
//! the engine about each recorded workload ([`StormpumpRuntime::adopt`]), and
//! `recover_state` adopts the pods from the listing as on any runtime that
//! keeps its own records.
//!
//! **Handles are a boot's.** A record from another boot names handles a new
//! engine may have given to other workloads, so it is never read: the file
//! carries the boot id.
//!
//! **Not in the file:** a container's console (`tty: true` / `stdin: true`),
//! a descriptor (its PTY master or stdin's write end). The engine keeps a
//! copy of it instead (#429, stormpump#260), and [`StormpumpRuntime::adopt`]
//! takes it back. An engine that kept none (older than stormpump#260) leaves
//! `kubectl attach` refused saying so; logs and exec work.

use super::*;
use serde::{Deserialize, Serialize};

/// Where the records are kept on a node.
pub const STATE_FILE: &str = "/var/lib/kubelet/stormpump-runtime.json";

/// This boot's id, from the kernel.
pub(super) fn boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map(|s| s.trim().to_string()).unwrap_or_default()
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct File {
    pub boot_id: String,
    pub sandboxes: Vec<SandboxRecord>,
    pub containers: Vec<ContainerRecord>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct SandboxRecord {
    id: String,
    handle: Option<u64>,
    config: PodSandboxConfig,
    state: PodSandboxState,
    created_at: i64,
    netns: Option<String>,
    ip: String,
    identity_file: Option<std::path::PathBuf>,
    network_status: Option<serde_json::Value>,
}

/// [`ExecBase`], with its capabilities spelled out.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct ExecRecord {
    env: Vec<String>,
    cwd: String,
    uid: u32,
    gid: u32,
    /// `CapBase::Standard` when true, else `Full`.
    standard_caps: bool,
    add: Vec<String>,
    drop: Vec<String>,
    no_new_privs: bool,
    groups: Vec<u32>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct StdioRecord {
    stdin: bool,
    tty: bool,
    slots: Vec<(String, i32)>,
    log_file: std::path::PathBuf,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct ContainerRecord {
    id: String,
    sandbox_id: String,
    name: String,
    namespace: String,
    pod: String,
    image: String,
    spec_handle: Option<u64>,
    workload_handle: Option<u64>,
    volume_handles: Vec<u64>,
    root_handle: Option<u64>,
    root_volume: Option<String>,
    provenance: Option<crate::image_config::Provenance>,
    resolved_at: Option<String>,
    identity_file: Option<std::path::PathBuf>,
    log_dir: String,
    mount_sources: Vec<(String, Option<String>)>,
    root_path: Option<String>,
    read_root: Option<String>,
    /// `None` reads back as `Unknown`.
    state: Option<ContainerState>,
    created_at: i64,
    started_at: i64,
    finished_at: i64,
    exit_code: i32,
    privileged: bool,
    host_network: bool,
    host_pid: bool,
    /// Its removal was asked for and not finished: retried after the restart.
    removing: bool,
    exec: Option<ExecRecord>,
    stdio: Option<StdioRecord>,
    log_file: std::path::PathBuf,
}

impl SandboxRecord {
    pub(super) fn of(sb: &Sandbox) -> SandboxRecord {
        SandboxRecord {
            id: sb.id.clone(),
            handle: sb.handle.map(|h| h.0),
            config: sb.config.clone(),
            state: sb.state,
            created_at: sb.created_at,
            netns: sb.netns.clone(),
            ip: sb.ip.clone(),
            identity_file: sb.identity_file.clone(),
            network_status: sb.network_status.clone(),
        }
    }

    pub(super) fn into_sandbox(self) -> Sandbox {
        Sandbox {
            id: self.id,
            handle: self.handle.map(Handle),
            config: self.config,
            state: self.state,
            created_at: self.created_at,
            netns: self.netns,
            ip: self.ip,
            made: None,
            identity_file: self.identity_file,
            network_status: self.network_status,
        }
    }
}

impl ContainerRecord {
    pub(super) fn of(c: &Container) -> ContainerRecord {
        ContainerRecord {
            id: c.id.clone(),
            sandbox_id: c.sandbox_id.clone(),
            name: c.name.clone(),
            namespace: c.namespace.clone(),
            pod: c.pod.clone(),
            image: c.image.clone(),
            spec_handle: c.spec_handle.map(|h| h.0),
            workload_handle: c.workload_handle.map(|h| h.0),
            volume_handles: c.volume_handles.iter().map(|h| h.0).collect(),
            root_handle: c.root_handle.map(|h| h.0),
            root_volume: c.root_volume.clone(),
            provenance: c.provenance.clone(),
            resolved_at: c.resolved_at.clone(),
            identity_file: c.identity_file.clone(),
            log_dir: c.log_dir.clone(),
            mount_sources: c.mount_sources.clone(),
            root_path: c.root_path.clone(),
            read_root: c.read_root.clone(),
            state: Some(c.state),
            created_at: c.created_at,
            started_at: c.started_at,
            finished_at: c.finished_at,
            exit_code: c.exit_code,
            privileged: c.privileged,
            host_network: c.host_network,
            host_pid: c.host_pid,
            removing: c.removing.is_some(),
            exec: c.exec_base.as_ref().map(|b| ExecRecord {
                env: b.env.clone(),
                cwd: b.cwd.clone(),
                uid: b.uid,
                gid: b.gid,
                standard_caps: b.caps.base == stormpump::spec::CapBase::Standard,
                add: b.caps.add.clone(),
                drop: b.caps.drop.clone(),
                no_new_privs: b.caps.no_new_privs,
                groups: b.groups.clone(),
            }),
            stdio: c.stdio.as_ref().map(|s| StdioRecord {
                stdin: s.stdin,
                tty: s.tty,
                slots: s.slots.clone(),
                log_file: s.log_file.clone(),
            }),
            log_file: c.log_file.clone(),
        }
    }

    pub(super) fn into_container(self) -> Container {
        Container {
            steps: Default::default(),
            id: self.id,
            sandbox_id: self.sandbox_id,
            name: self.name,
            namespace: self.namespace,
            pod: self.pod,
            image: self.image,
            spec_handle: self.spec_handle.map(Handle),
            workload_handle: self.workload_handle.map(Handle),
            volume_handles: self.volume_handles.into_iter().map(Handle).collect(),
            root_handle: self.root_handle.map(Handle),
            root_volume: self.root_volume,
            provenance: self.provenance,
            resolved_at: self.resolved_at,
            // Published at its start; the file it was published as is kept,
            // so its removal withdraws it.
            identity: None,
            identity_file: self.identity_file,
            log_dir: self.log_dir,
            mount_sources: self.mount_sources,
            root_path: self.root_path,
            read_root: self.read_root,
            state: self.state.unwrap_or(ContainerState::Unknown),
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            exit_code: self.exit_code,
            privileged: self.privileged,
            host_network: self.host_network,
            host_pid: self.host_pid,
            removing: self.removing.then(std::time::Instant::now),
            exec_base: self.exec.map(|e| ExecBase {
                env: e.env,
                cwd: e.cwd,
                uid: e.uid,
                gid: e.gid,
                caps: stormpump::spec::Caps {
                    base: if e.standard_caps {
                        stormpump::spec::CapBase::Standard
                    } else {
                        stormpump::spec::CapBase::Full
                    },
                    add: e.add,
                    drop: e.drop,
                    no_new_privs: e.no_new_privs,
                },
                groups: e.groups,
            }),
            stdio: self.stdio.map(|s| StdioWanted { stdin: s.stdin, tty: s.tty, slots: s.slots, log_file: s.log_file }),
            // A PTY this process never held (see the module comment).
            console: None,
            log_file: self.log_file,
            // Applied before its start, which is behind it.
            ownership: None,
        }
    }
}

/// The records at `path`, if they are this boot's. Anything else (no file,
/// another boot's, unreadable) is nothing to adopt, said once.
pub(super) fn load(path: &std::path::Path, boot: &str) -> Option<File> {
    let text = match std::fs::read(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(path = %path.display(), "stormpump: runtime records not read, nothing adopted: {e}");
            return None;
        }
    };
    let file: File = match serde_json::from_slice(&text) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(path = %path.display(), "stormpump: runtime records unreadable, nothing adopted: {e}");
            return None;
        }
    };
    if file.boot_id != boot || boot.is_empty() {
        tracing::info!(
            path = %path.display(), recorded = %file.boot_id, now = %boot,
            "stormpump: runtime records are another boot's: nothing to adopt"
        );
        return None;
    }
    Some(file)
}

/// Write the records whole (a new file renamed over the old).
pub(super) fn write(path: &std::path::Path, file: &File) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let bytes = serde_json::to_vec(file).map_err(std::io::Error::other)?;
    crate::api_files::write_atomic(path, &bytes, 0o600)
}
