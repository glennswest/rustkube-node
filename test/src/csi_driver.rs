//! The csi suite's own CSI driver (#52, master's option A): a real driver
//! process on the node, so the suite sees the kubelet's CSI node side work
//! against real sockets, real kernel mounts and real mount propagation,
//! without a third-party image.
//!
//! Two workload modes of this image:
//!
//! - `csi-driver <name> <node>`: the node plugin. It serves CSI Identity and
//!   Node on `/var/lib/kubelet/plugins/<name>/csi.sock`, and registers itself
//!   (the registrar's part) on `/var/lib/kubelet/plugins_registry/<name>-reg.sock`.
//!   No staging. NodePublish bind-mounts the volume's own directory
//!   `/var/lib/kubelet/plugins/<name>/data/<volume id>` at the target, so the
//!   data outlives each publish. NodeUnpublish unmounts the target and removes
//!   it. Each call is one `csi-driver: …` line on stdout, which the suite reads
//!   back through the pod's log. SIGTERM removes the sockets (the kubelet then
//!   deregisters it) and the data, unless a volume is still published.
//!   Its pod is privileged and mounts `/var/lib/kubelet` with
//!   `mountPropagation: Bidirectional`, like any upstream driver's DaemonSet.
//! - `restart-kubelet`: in a privileged hostPID pod, SIGTERM the node's
//!   `kubelet` process (a restart, not a node shutdown: rustkube-node#181) and
//!   wait for PID 1 to start a new one.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("csi.v1");
}
pub mod reg {
    #![allow(clippy::all)]
    tonic::include_proto!("pluginregistration");
}

/// Where the kubelet keeps pods' volumes and plugins, on the node and in the
/// driver's pod (the hostPath).
pub const KUBELET_ROOT: &str = "/var/lib/kubelet";

type R<T> = Result<Response<T>, Status>;

/// Whether `args` is one of this module's modes.
pub fn is_mode(args: &[String]) -> bool {
    matches!(args.first().map(String::as_str), Some("csi-driver" | "restart-kubelet"))
}

/// Run a mode; returns the exit code.
pub fn main(args: &[String]) -> i32 {
    if args[0] == "restart-kubelet" {
        return match restart_kubelet(Path::new("/proc"), Duration::from_secs(180)) {
            Ok(d) => {
                println!("restart-kubelet: {d}");
                0
            }
            Err(e) => {
                println!("restart-kubelet: {e}");
                let _ = std::fs::write("/dev/termination-log", &e);
                1
            }
        };
    }
    let (Some(name), Some(node)) = (args.get(1), args.get(2)) else {
        println!("csi-driver: usage: csi-driver <name> <node>");
        return 2;
    };
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            println!("csi-driver: tokio runtime: {e}");
            return 1;
        }
    };
    let d = Driver::new(name, node, PathBuf::from(KUBELET_ROOT), Mounts::Kernel);
    match rt.block_on(serve(d)) {
        Ok(()) => 0,
        Err(e) => {
            println!("csi-driver: {e}");
            1
        }
    }
}

/// How the driver mounts: the kernel's, or (in unit tests) a record.
#[derive(Clone)]
pub enum Mounts {
    Kernel,
    Fake(Arc<Mutex<BTreeSet<PathBuf>>>),
}

impl Mounts {
    fn is_mounted(&self, target: &Path) -> Result<bool, String> {
        match self {
            Mounts::Fake(m) => Ok(m.lock().unwrap().contains(target)),
            Mounts::Kernel => {
                let info = std::fs::read_to_string("/proc/self/mountinfo")
                    .map_err(|e| format!("read /proc/self/mountinfo: {e}"))?;
                let t = target.to_string_lossy();
                Ok(mount_points(&info).iter().any(|m| *m == t))
            }
        }
    }

    fn bind(&self, source: &Path, target: &Path, readonly: bool) -> Result<(), String> {
        match self {
            Mounts::Fake(m) => {
                m.lock().unwrap().insert(target.to_path_buf());
                Ok(())
            }
            Mounts::Kernel => {
                let (s, t) = (cstr(source)?, cstr(target)?);
                // SAFETY: both are valid NUL-terminated paths; no data argument.
                if unsafe { libc::mount(s.as_ptr(), t.as_ptr(), std::ptr::null(), libc::MS_BIND, std::ptr::null()) } != 0 {
                    return Err(format!("bind {} at {}: {}", source.display(), target.display(), std::io::Error::last_os_error()));
                }
                if readonly {
                    let flags = libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY;
                    // SAFETY: as above.
                    if unsafe { libc::mount(s.as_ptr(), t.as_ptr(), std::ptr::null(), flags, std::ptr::null()) } != 0 {
                        let e = std::io::Error::last_os_error();
                        let _ = self.unmount(target);
                        return Err(format!("remount {} read-only: {e}", target.display()));
                    }
                }
                Ok(())
            }
        }
    }

    fn unmount(&self, target: &Path) -> Result<(), String> {
        match self {
            Mounts::Fake(m) => {
                m.lock().unwrap().remove(target);
                Ok(())
            }
            Mounts::Kernel => {
                let t = cstr(target)?;
                // SAFETY: a valid NUL-terminated path.
                if unsafe { libc::umount2(t.as_ptr(), 0) } != 0 {
                    let e = std::io::Error::last_os_error();
                    // Not a mount point (any more): already unpublished.
                    if e.raw_os_error() != Some(libc::EINVAL) {
                        return Err(format!("unmount {}: {e}", target.display()));
                    }
                }
                Ok(())
            }
        }
    }
}

fn cstr(p: &Path) -> Result<std::ffi::CString, String> {
    std::ffi::CString::new(p.as_os_str().to_string_lossy().as_bytes()).map_err(|e| format!("{}: {e}", p.display()))
}

/// The mount points of a mountinfo text (field 5, octal escapes undone).
pub fn mount_points(mountinfo: &str) -> Vec<String> {
    mountinfo.lines().filter_map(|l| l.split(' ').nth(4)).map(unescape).collect()
}

fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c)) {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The driver: its name, the node it says it is, where the kubelet's tree is,
/// and what it has published.
#[derive(Clone)]
pub struct Driver {
    pub name: String,
    node: String,
    root: PathBuf,
    mounts: Mounts,
    /// Target → volume id, of the publishes this process made.
    published: Arc<Mutex<BTreeMap<PathBuf, String>>>,
}

impl Driver {
    pub fn new(name: &str, node: &str, root: PathBuf, mounts: Mounts) -> Driver {
        Driver { name: name.into(), node: node.into(), root, mounts, published: Default::default() }
    }

    pub fn plugin_dir(&self) -> PathBuf {
        self.root.join("plugins").join(&self.name)
    }
    pub fn csi_socket(&self) -> PathBuf {
        self.plugin_dir().join("csi.sock")
    }
    pub fn registration_socket(&self) -> PathBuf {
        self.root.join("plugins_registry").join(format!("{}-reg.sock", self.name))
    }
    /// Where a volume's data lives between publishes.
    pub fn data_dir(&self, volume: &str) -> PathBuf {
        self.plugin_dir().join("data").join(volume)
    }

    /// A volume id is a path component here: refused unless it is a plain one.
    fn volume_id(&self, id: &str) -> Result<(), Status> {
        let ok = !id.is_empty()
            && id.len() <= 128
            && !id.starts_with('.')
            && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
        if ok { Ok(()) } else { Err(Status::invalid_argument(format!("volume id {id:?} is not a plain name"))) }
    }

    /// A target must be a pod volume's directory under the kubelet's tree.
    fn target(&self, t: &str) -> Result<PathBuf, Status> {
        let p = PathBuf::from(t);
        let under = p.starts_with(self.root.join("pods")) && !t.contains("/../") && !t.ends_with("/..");
        if under { Ok(p) } else { Err(Status::invalid_argument(format!("target {t:?} is not under {}/pods", self.root.display()))) }
    }
}

fn say(line: String) {
    println!("csi-driver: {line}");
}

#[tonic::async_trait]
impl proto::identity_server::Identity for Driver {
    async fn get_plugin_info(&self, _: Request<proto::GetPluginInfoRequest>) -> R<proto::GetPluginInfoResponse> {
        Ok(Response::new(proto::GetPluginInfoResponse {
            name: self.name.clone(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            manifest: Default::default(),
        }))
    }
    async fn get_plugin_capabilities(
        &self,
        _: Request<proto::GetPluginCapabilitiesRequest>,
    ) -> R<proto::GetPluginCapabilitiesResponse> {
        // No controller service: the suite's PV is made by hand.
        Ok(Response::new(proto::GetPluginCapabilitiesResponse { capabilities: vec![] }))
    }
    async fn probe(&self, _: Request<proto::ProbeRequest>) -> R<proto::ProbeResponse> {
        Ok(Response::new(proto::ProbeResponse { ready: Some(true) }))
    }
}

#[tonic::async_trait]
impl proto::node_server::Node for Driver {
    async fn node_stage_volume(&self, _: Request<proto::NodeStageVolumeRequest>) -> R<proto::NodeStageVolumeResponse> {
        Err(Status::unimplemented("this driver does not stage"))
    }
    async fn node_unstage_volume(
        &self,
        _: Request<proto::NodeUnstageVolumeRequest>,
    ) -> R<proto::NodeUnstageVolumeResponse> {
        Err(Status::unimplemented("this driver does not stage"))
    }

    async fn node_publish_volume(
        &self,
        r: Request<proto::NodePublishVolumeRequest>,
    ) -> R<proto::NodePublishVolumeResponse> {
        let r = r.into_inner();
        self.volume_id(&r.volume_id)?;
        let target = self.target(&r.target_path)?;
        match r.volume_capability.as_ref().and_then(|c| c.access_type.as_ref()) {
            Some(proto::volume_capability::AccessType::Mount(_)) => {}
            Some(proto::volume_capability::AccessType::Block(_)) => {
                return Err(Status::invalid_argument("this driver has no raw block volumes"))
            }
            None => return Err(Status::invalid_argument("no volume capability")),
        }
        let data = self.data_dir(&r.volume_id);
        let fail = |e: String| {
            say(format!("NodePublishVolume volume={} target={} failed: {e}", r.volume_id, target.display()));
            Status::internal(e)
        };
        std::fs::create_dir_all(&data).map_err(|e| fail(format!("create {}: {e}", data.display())))?;
        std::fs::create_dir_all(&target).map_err(|e| fail(format!("create {}: {e}", target.display())))?;
        if self.mounts.is_mounted(&target).map_err(fail)? {
            say(format!("NodePublishVolume volume={} target={} already published", r.volume_id, target.display()));
        } else {
            self.mounts.bind(&data, &target, r.readonly).map_err(fail)?;
            say(format!("NodePublishVolume volume={} target={} readonly={}", r.volume_id, target.display(), r.readonly));
        }
        self.published.lock().unwrap().insert(target, r.volume_id);
        Ok(Response::new(proto::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        r: Request<proto::NodeUnpublishVolumeRequest>,
    ) -> R<proto::NodeUnpublishVolumeResponse> {
        let r = r.into_inner();
        self.volume_id(&r.volume_id)?;
        let target = self.target(&r.target_path)?;
        let fail = |e: String| {
            say(format!("NodeUnpublishVolume volume={} target={} failed: {e}", r.volume_id, target.display()));
            Status::internal(e)
        };
        if self.mounts.is_mounted(&target).map_err(fail)? {
            self.mounts.unmount(&target).map_err(fail)?;
        }
        match std::fs::remove_dir(&target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(fail(format!("remove {}: {e}", target.display()))),
        }
        self.published.lock().unwrap().remove(&target);
        say(format!("NodeUnpublishVolume volume={} target={}", r.volume_id, target.display()));
        Ok(Response::new(proto::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        _: Request<proto::NodeGetVolumeStatsRequest>,
    ) -> R<proto::NodeGetVolumeStatsResponse> {
        Err(Status::unimplemented("no volume stats"))
    }
    async fn node_expand_volume(&self, _: Request<proto::NodeExpandVolumeRequest>) -> R<proto::NodeExpandVolumeResponse> {
        Err(Status::unimplemented("no expansion"))
    }
    async fn node_get_capabilities(
        &self,
        _: Request<proto::NodeGetCapabilitiesRequest>,
    ) -> R<proto::NodeGetCapabilitiesResponse> {
        Ok(Response::new(proto::NodeGetCapabilitiesResponse { capabilities: vec![] }))
    }
    async fn node_get_info(&self, _: Request<proto::NodeGetInfoRequest>) -> R<proto::NodeGetInfoResponse> {
        Ok(Response::new(proto::NodeGetInfoResponse {
            node_id: self.node.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }
}

#[tonic::async_trait]
impl reg::registration_server::Registration for Driver {
    async fn get_info(&self, _: Request<reg::InfoRequest>) -> R<reg::PluginInfo> {
        Ok(Response::new(reg::PluginInfo {
            r#type: "CSIPlugin".into(),
            name: self.name.clone(),
            endpoint: format!("unix://{}", self.csi_socket().display()),
            supported_versions: vec!["1.0.0".into()],
        }))
    }
    async fn notify_registration_status(
        &self,
        r: Request<reg::RegistrationStatus>,
    ) -> R<reg::RegistrationStatusResponse> {
        let r = r.into_inner();
        if r.plugin_registered {
            say("registered with the kubelet".into());
        } else {
            say(format!("the kubelet refused the registration: {}", r.error));
        }
        Ok(Response::new(reg::RegistrationStatusResponse {}))
    }
}

/// Bind a fresh Unix socket at `path` (a stale one from an earlier run removed).
fn listen(path: &Path) -> Result<tokio_stream::wrappers::UnixListenerStream, String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("remove stale {}: {e}", path.display())),
    }
    let l = tokio::net::UnixListener::bind(path).map_err(|e| format!("bind {}: {e}", path.display()))?;
    Ok(tokio_stream::wrappers::UnixListenerStream::new(l))
}

/// Serve until SIGTERM (or SIGINT), then withdraw.
pub async fn serve(d: Driver) -> Result<(), String> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).map_err(|e| format!("SIGTERM handler: {e}"))?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| format!("SIGINT handler: {e}"))?;

    // The driver's own socket first, so the kubelet finds it serving as soon
    // as the registration socket appears.
    let csi = listen(&d.csi_socket())?;
    let node = tonic::transport::Server::builder()
        .add_service(proto::identity_server::IdentityServer::new(d.clone()))
        .add_service(proto::node_server::NodeServer::new(d.clone()))
        .serve_with_incoming(csi);
    let mut node = tokio::spawn(node);
    let registration = listen(&d.registration_socket())?;
    let registrar = tonic::transport::Server::builder()
        .add_service(reg::registration_server::RegistrationServer::new(d.clone()))
        .serve_with_incoming(registration);
    let mut registrar = tokio::spawn(registrar);
    say(format!("{} serving on {} for node {}", d.name, d.csi_socket().display(), d.node));

    let ended = tokio::select! {
        _ = term.recv() => Ok("SIGTERM"),
        _ = int.recv() => Ok("SIGINT"),
        r = &mut node => Err(format!("the CSI server ended: {r:?}")),
        r = &mut registrar => Err(format!("the registration server ended: {r:?}")),
    };
    // Withdraw: the registration socket first (the kubelet deregisters on its
    // removal), then the driver's.
    node.abort();
    registrar.abort();
    let _ = std::fs::remove_file(d.registration_socket());
    let _ = std::fs::remove_file(d.csi_socket());
    let left = d.published.lock().unwrap().len();
    if left == 0 {
        let _ = std::fs::remove_dir_all(d.plugin_dir());
        say("stopped; sockets and data removed".into());
    } else {
        // The data dirs are the sources of live binds: never deleted under one.
        say(format!("stopped; sockets removed, data kept ({left} volume(s) still published)"));
    }
    ended.map(|_| ())
}

/// The pids whose command is `kubelet`, under `proc` (the host's, in a
/// hostPID pod), other than this process.
pub fn kubelet_pids(proc: &Path) -> Vec<u32> {
    let me = std::process::id();
    let Ok(dir) = std::fs::read_dir(proc) else { return vec![] };
    let mut pids: Vec<u32> = dir
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .filter(|&p| p != me)
        .filter(|p| std::fs::read_to_string(proc.join(p.to_string()).join("comm")).is_ok_and(|c| c.trim() == "kubelet"))
        .collect();
    pids.sort_unstable();
    pids
}

/// SIGTERM the kubelet and wait for a new one.
pub fn restart_kubelet(proc: &Path, within: Duration) -> Result<String, String> {
    let old = kubelet_pids(proc);
    if old.is_empty() {
        return Err(format!("no kubelet process under {} (is the pod hostPID?)", proc.display()));
    }
    for &p in &old {
        // SAFETY: kill(2) with a pid and a signal number.
        if unsafe { libc::kill(p as libc::pid_t, libc::SIGTERM) } != 0 {
            return Err(format!("SIGTERM kubelet {p}: {}", std::io::Error::last_os_error()));
        }
    }
    let t = Instant::now();
    while t.elapsed() < within {
        std::thread::sleep(Duration::from_millis(500));
        let new: Vec<u32> = kubelet_pids(proc).into_iter().filter(|p| !old.contains(p)).collect();
        if !new.is_empty() {
            return Ok(format!("kubelet {old:?} → {new:?} after {} ms", t.elapsed().as_millis()));
        }
    }
    Err(format!("kubelet {old:?} sent SIGTERM; no new kubelet within {} s", within.as_secs()))
}

#[cfg(test)]
mod tests {
    use super::proto::identity_server::Identity;
    use super::proto::node_server::Node;
    use super::reg::registration_server::Registration;
    use super::*;

    fn rig(name: &str) -> (Driver, PathBuf, Arc<Mutex<BTreeSet<PathBuf>>>) {
        let root = std::env::temp_dir().join(format!("rknt-csi-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let m: Arc<Mutex<BTreeSet<PathBuf>>> = Default::default();
        (Driver::new("t.csi.storm.io", "node1", root.clone(), Mounts::Fake(m.clone())), root, m)
    }

    fn publish(target: &Path, volume: &str) -> proto::NodePublishVolumeRequest {
        proto::NodePublishVolumeRequest {
            volume_id: volume.into(),
            target_path: target.to_string_lossy().into(),
            volume_capability: Some(proto::VolumeCapability {
                access_type: Some(proto::volume_capability::AccessType::Mount(Default::default())),
                access_mode: None,
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn it_says_who_it_is_and_where_it_serves() {
        let (d, root, _) = rig("info");
        let i = d.get_plugin_info(Request::new(Default::default())).await.unwrap().into_inner();
        assert_eq!(i.name, "t.csi.storm.io");
        assert_eq!(d.probe(Request::new(Default::default())).await.unwrap().into_inner().ready, Some(true));
        let n = d.node_get_info(Request::new(Default::default())).await.unwrap().into_inner();
        assert_eq!(n.node_id, "node1");
        let caps = d.node_get_capabilities(Request::new(Default::default())).await.unwrap().into_inner();
        assert!(caps.capabilities.is_empty(), "no staging");
        let r = d.get_info(Request::new(reg::InfoRequest {})).await.unwrap().into_inner();
        assert_eq!(r.r#type, "CSIPlugin");
        assert_eq!(r.endpoint, format!("unix://{}/plugins/t.csi.storm.io/csi.sock", root.display()));
        assert!(d.registration_socket().ends_with("plugins_registry/t.csi.storm.io-reg.sock"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn publish_binds_the_volumes_data_and_unpublish_undoes_it() {
        let (d, root, mounts) = rig("publish");
        let target = root.join("pods/uid1/volumes/kubernetes.io~csi/pv1/mount");
        d.node_publish_volume(Request::new(publish(&target, "vol-1"))).await.unwrap();
        assert!(mounts.lock().unwrap().contains(&target));
        assert!(d.data_dir("vol-1").is_dir());
        assert!(target.is_dir());
        // Again: idempotent, still one mount.
        d.node_publish_volume(Request::new(publish(&target, "vol-1"))).await.unwrap();
        assert_eq!(mounts.lock().unwrap().len(), 1);
        let un = proto::NodeUnpublishVolumeRequest { volume_id: "vol-1".into(), target_path: target.to_string_lossy().into() };
        d.node_unpublish_volume(Request::new(un.clone())).await.unwrap();
        assert!(mounts.lock().unwrap().is_empty());
        assert!(!target.exists());
        assert!(d.data_dir("vol-1").is_dir(), "the data outlives the publish");
        // Again: already gone is fine.
        d.node_unpublish_volume(Request::new(un)).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn it_refuses_what_is_not_a_pod_volume() {
        let (d, root, mounts) = rig("refuse");
        let outside = root.join("elsewhere/mount");
        let e = d.node_publish_volume(Request::new(publish(&outside, "vol-1"))).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        let target = root.join("pods/uid1/volumes/kubernetes.io~csi/pv1/mount");
        for bad in ["", "../x", "a/b", ".hidden"] {
            let e = d.node_publish_volume(Request::new(publish(&target, bad))).await.unwrap_err();
            assert_eq!(e.code(), tonic::Code::InvalidArgument, "{bad:?}");
        }
        let mut block = publish(&target, "vol-1");
        block.volume_capability.as_mut().unwrap().access_type =
            Some(proto::volume_capability::AccessType::Block(Default::default()));
        let e = d.node_publish_volume(Request::new(block)).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert!(mounts.lock().unwrap().is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mountinfo_mount_points() {
        let info = "22 1 0:21 / / rw - erofs /dev/vda ro\n\
                    140 22 0:52 /data/v /var/lib/kubelet/pods/u/volumes/kubernetes.io~csi/pv/mount rw shared:70 - ext4 /dev/x rw\n\
                    141 22 0:53 / /a\\040b rw - tmpfs tmpfs rw\n";
        let m = mount_points(info);
        assert_eq!(m[0], "/");
        assert_eq!(m[1], "/var/lib/kubelet/pods/u/volumes/kubernetes.io~csi/pv/mount");
        assert_eq!(m[2], "/a b");
    }

    #[test]
    fn the_kubelet_is_found_by_its_command() {
        let proc = std::env::temp_dir().join(format!("rknt-proc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&proc);
        for (pid, comm) in [("10", "kubelet\n"), ("11", "stormd\n"), ("12", "kubelet-helper\n")] {
            std::fs::create_dir_all(proc.join(pid)).unwrap();
            std::fs::write(proc.join(pid).join("comm"), comm).unwrap();
        }
        std::fs::create_dir_all(proc.join("self")).unwrap();
        assert_eq!(kubelet_pids(&proc), vec![10]);
        std::fs::remove_dir_all(&proc).unwrap();
        assert!(restart_kubelet(&proc, Duration::from_secs(1)).is_err(), "no proc, no kubelet");
    }

    #[test]
    fn mode_dispatch() {
        assert!(is_mode(&["csi-driver".into()]));
        assert!(is_mode(&["restart-kubelet".into()]));
        assert!(!is_mode(&["csi".into()]));
    }
}
