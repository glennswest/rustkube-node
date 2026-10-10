//! The kubelet driving stormpump directly, over its ring.
//!
//! # Why there is no shim
//!
//! The obvious way to reach a runtime that is not containerd is to write a CRI
//! shim: a process that listens on a Unix socket, speaks the CRI protobuf
//! dialect, and translates. That is what every runtime outside the big two
//! does, and it is the right answer when the two sides are strangers.
//!
//! These two are not strangers. [`super::cri`] defines `RuntimeService` and
//! `ImageService` as *Rust traits*, and gRPC is one implementation of them
//! rather than the interface itself. stormpump, for its part, is driven by a
//! shared-memory ring rather than by a socket protocol. So a shim would add a
//! process, a socket, and two protobuf transcodes per call in order to connect
//! two Rust programs that can already call each other.
//!
//! What is here instead: the traits, implemented over the ring. A CRI call
//! becomes a submission-queue entry in shared memory and an eventfd wake.
//! Nothing is serialised except the spec itself, which is a payload written
//! once into an arena the engine already has mapped.
//!
//! That is worth something on its own, but it is not the main saving. The main
//! saving is `SandboxAcquire`: stormpump keeps *warm* sandboxes, with their
//! namespaces already created, and creating namespaces is most of what a
//! container start costs. A shim cannot expose that, because CRI has no way to
//! say "give me one you prepared earlier".
//!
//! # What a pod is here
//!
//! | CRI | stormpump |
//! |---|---|
//! | pod sandbox | a sandbox from the warm pool, holding the pod's namespaces |
//! | container | a spec, defined once, spawned into that sandbox |
//! | image | a copy-on-write clone minted by the registry, registered as a volume |
//!
//! The mapping is close because both were designed around the same shape: a
//! group of processes sharing namespaces, each with its own root filesystem.
//!
//! # What this does not do yet
//!
//! Capabilities, seccomp and SELinux are absent from stormpump's spec — it does
//! not drop anything, so a workload keeps what PID 1 had. That is permissive
//! rather than restrictive: a privileged container works, and an unprivileged
//! one is over-privileged. Cilium runs; a hostile workload is not contained.
//! `ContainerConfig` carries the pod's `capabilities.add`/`drop` and
//! `privileged`, but `spec_for` has nowhere to put them until stormpump#47 gives
//! `Spec` a capability set (#118). It must map them before stormpump's default
//! narrows to the runtime's standard set, or every pod that adds one (Cilium's
//! NET_ADMIN, BPF, …) loses it as an EPERM rather than a refused start.

use retry::RetryExt;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::stormpump_ring::{RingClient, RingError};
use stormpump::sandbox::Profile;
use stormpump_abi::handle::Handle;
use stormpump_abi::Domain;

use crate::cri::{
    ContainerConfig, ContainerState, ContainerStatusInfo, CriError, ImageInfo, ImageService,
    MountPropagation, PodSandboxConfig, PodSandboxState, PodSandboxStatusInfo, PodSandboxSummary,
    RuntimeService,
};

/// Where stormpump listens for clients on a stormcos node.
pub const DEFAULT_SOCKET: &str = "/run/stormpump.sock";

/// What one spec can carry, from `stormpump::spec::MAX_MOUNTS`.
const MAX_MOUNTS: usize = 16;

/// `sandbox::Profile::Isolated` — a network namespace with nothing in it but
/// loopback.
///
/// The right profile whenever a CNI owns pod networking, which is the case this
/// is built for: the plugin creates the veth and anything the runtime plumbed
/// first would have to be undone. It is also what makes a pod's containers
/// share `localhost`, since loopback is there whether or not anything else is.
///
/// A node with no CNI installed gets pods that can reach each other inside a
/// pod and nothing outside it — which is honest, and better than a pod that
/// fails to start because the node has no bridge to wire to.
///
/// Named here rather than imported so the number this puts on the wire is
/// visible where it is chosen.
const PROFILE_ISOLATED: u8 = 4;

/// One container the kubelet has asked for.
struct Container {
    id: String,
    sandbox_id: String,
    name: String,
    /// The pod and namespace this container belongs to.
    ///
    /// A container's identity is `namespace/pod/container`, never the bare
    /// name: two namespaces may each have an `app`, and they are different
    /// containers. Kept here so a log line, a status and a workload name all
    /// say which one — the bare name is ambiguous exactly when someone is
    /// trying to tell two of them apart.
    namespace: String,
    pod: String,
    image: String,
    /// The handle stormpump returned for the spec. `None` until created.
    spec_handle: Option<Handle>,
    /// The handle for the running workload. `None` until started.
    workload_handle: Option<Handle>,
    /// The volumes registered for this container at start — its root, its
    /// logs and its mounts — released when it is removed. They were never
    /// released, so a claim's device mount outlived every pod that used it
    /// and the volume was later detached under a live filesystem.
    volume_handles: Vec<Handle>,
    /// This container's own root, registered with the engine (PID 1 mounted
    /// it) at create, released at removal (#104).
    root_handle: Option<Handle>,
    /// The engine volume that is this container's own root (#104): one CoW
    /// clone of its image's sealed golden, deleted with the container.
    root_volume: Option<String>,
    /// Where its image came from (#130): the registry's record for a pulled
    /// image, the release manifest for a pallet. `None` when neither says.
    provenance: Option<crate::image_config::Provenance>,
    /// When its image's golden was last resolved for it (#130): its create,
    /// which checks the golden and clones it. RFC 3339.
    resolved_at: Option<String>,
    /// Who this container is, for cadvisor (#84): filled with its pid and
    /// cgroup at start, published as a file, withdrawn at removal.
    identity: Option<crate::workload_identity::Record>,
    identity_file: Option<std::path::PathBuf>,
    /// The directory this container's log file is opened in:
    /// `<sandbox log_directory>/<container>`. Kubernetes reads
    /// `<that>/<restart>.log` and nowhere else.
    log_dir: String,
    /// The host paths for this container's mounts, in the order the spec
    /// declares their destinations, with the filesystem when the source is a
    /// block device (a PersistentVolumeClaim). Registered as volumes at spawn
    /// and paired with those destinations by position.
    mount_sources: Vec<(String, Option<String>)>,
    /// Where the image's filesystem is mounted on this node.
    ///
    /// Registered with the engine at start rather than at create: a volume
    /// handle is a resource the engine holds, and holding one for a container
    /// that may never start is a leak for as long as the pod is pending.
    root_path: Option<String>,
    state: ContainerState,
    created_at: i64,
    started_at: i64,
    finished_at: i64,
    exit_code: i32,
    /// Recorded, not enforced. See the module comment: stormpump drops nothing
    /// today, so these describe what was *asked for* rather than what is true.
    privileged: bool,
    host_network: bool,
    host_pid: bool,
    /// Set when the kubelet asked for the removal, to when it was last tried;
    /// the record stays until every release it holds is done (#90). The
    /// engine refuses `WorkloadRelease` while the workload runs, and a stop
    /// only signals: a removal right after a stop (a restart, an init past
    /// its deadline) is refused, and the callers there do not retry. So the
    /// runtime does: when the workload's exit arrives, at least every
    /// [`REMOVAL_RETRY`], and before its sandbox is removed. A removed
    /// container is gone to the kubelet (not listed, NotFound) at once.
    removing: Option<std::time::Instant>,
}

/// How often a removal the engine refused is tried again with no exit to
/// prompt it (#90).
const REMOVAL_RETRY: std::time::Duration = std::time::Duration::from_secs(10);

/// The removals to try again now (#90): a refused one whose workload has
/// just exited (the release the engine refused while it ran can succeed), or
/// one last tried at least [`REMOVAL_RETRY`] ago (a refused volume release,
/// or an exit drained before the removal was asked).
fn removals_due(
    containers: &HashMap<String, Container>,
    exits: &[crate::stormpump_ring::Exited],
) -> Vec<String> {
    containers
        .values()
        .filter(|c| match c.removing {
            None => false,
            Some(t) => {
                t.elapsed() >= REMOVAL_RETRY
                    || exits.iter().any(|e| c.workload_handle == Some(e.handle))
            }
        })
        .map(|c| c.id.clone())
        .collect()
}

/// One pod sandbox.
#[derive(Clone)]
struct Sandbox {
    id: String,
    handle: Option<Handle>,
    config: PodSandboxConfig,
    state: PodSandboxState,
    created_at: i64,
    /// `/proc/<holder>/ns/net`, the only name a CNI plugin understands.
    netns: Option<String>,
    /// What the CNI gave this pod, empty when none ran.
    ip: String,
    /// The acquire and the CNI ADD that made it (#139); `None` for one adopted
    /// rather than made by this process.
    made: Option<crate::cri::SandboxSteps>,
    /// Its namespace holder's identity file (#84): the workload that reports
    /// the pod's network.
    identity_file: Option<std::path::PathBuf>,
    /// The CNI ADD's result as `network-status` entries (#131); `None` when
    /// no CNI ran (host network, no invoker) or for an adopted sandbox.
    network_status: Option<serde_json::Value>,
}

/// A container's `imageID` (#130): its image's digest when one is known,
/// else the reference it was created from.
fn image_id(c: &Container) -> String {
    c.provenance
        .as_ref()
        .and_then(|p| p.digest.clone())
        .unwrap_or_else(|| c.image.clone())
}

/// The pod as one extra network sees it (#233): its interface, and the
/// addresses / MAC the pod asked for, offered as `runtimeConfig`.
fn attachment_pod(base: &cni::PodNetwork, a: &crate::cri::NetworkAttachment) -> cni::PodNetwork {
    let mut p = base.clone().on_interface(&a.ifname);
    if !a.ips.is_empty() {
        p.runtime_config.insert("ips".into(), serde_json::json!(a.ips));
    }
    if let Some(m) = &a.mac {
        p.runtime_config.insert("mac".into(), serde_json::json!(m));
    }
    p
}

/// A sandbox whose CNI ADD failed and whose DEL has not succeeded yet (#100).
///
/// A plugin chain can fail half way through ADD with an address allocated or
/// an endpoint made. The CNI contract is that the runtime then calls DEL; the
/// namespace holder is kept until it succeeds, because DEL finds the
/// interface through that namespace.
struct FailedNetwork {
    id: String,
    handle: Option<Handle>,
    netns: String,
    config: PodSandboxConfig,
}

/// The kubelet's view of stormpump.
pub struct StormpumpRuntime {
    socket: String,
    /// The ring, once attached. `None` on a node where stormpump is not PID 1,
    /// which is every development box — the runtime is constructible there so
    /// its bookkeeping can be tested, and every operation that needs the engine
    /// says so rather than pretending.
    ring: Option<Arc<RingClient>>,
    /// The CNI, when one is installed.
    ///
    /// Held rather than resolved once at startup: Cilium writes its conflist
    /// only after its agent is up, which is minutes after the kubelet decided
    /// anything. `CniInvoker` reloads the config on every call for exactly
    /// this reason, so the question "is there a network" is asked per pod.
    cni: Option<cni::CniInvoker>,
    sandboxes: Mutex<HashMap<String, Sandbox>>,
    containers: Mutex<HashMap<String, Container>>,
    /// Failed ADDs still to be undone, retried before each new sandbox.
    failed_networks: Mutex<Vec<FailedNetwork>>,
    /// Monotonic, so two containers created in the same millisecond do not
    /// collide the way a timestamp-derived id would.
    next_id: std::sync::atomic::AtomicU64,
    /// Image configs by image root, filled by [`StormpumpImages`] (#98).
    image_configs: Arc<crate::image_config::ImageConfigs>,
    /// Makes and deletes each container's own root (#104).
    roots: Option<Arc<crate::container_roots::Roots>>,
    /// Root volumes being made, which the orphan sweep must not take.
    making: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Publishes each workload's identity for cadvisor (#84); `None` in tests.
    identities: Option<crate::workload_identity::Publisher>,
}

impl StormpumpRuntime {
    /// Attach to stormpump. Fails if the engine is not there, because a
    /// kubelet that starts without its runtime and discovers it pod by pod
    /// reports a dozen unrelated failures for one cause.
    /// The ring, for the image service — which needs the engine for the one
    /// thing only the engine can do: mount a volume in the node's namespace.
    pub fn ring_client(&self) -> Option<Arc<RingClient>> {
        self.ring.clone()
    }

    pub fn connect(socket: impl Into<String>) -> Result<StormpumpRuntime, RingError> {
        let socket = socket.into();
        let ring = Arc::new(RingClient::attach(&socket)?);
        tracing::info!(socket = %socket, "kubelet attached to stormpump");
        Ok(StormpumpRuntime {
            socket,
            ring: Some(ring),
            cni: None,
            sandboxes: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
            failed_networks: Mutex::new(Vec::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            image_configs: Arc::default(),
            roots: None,
            making: Default::default(),
            identities: None,
        })
    }

    /// Hand a sandbox back after a failed start.
    ///
    /// Without this every retry of a pod that cannot get a network leaks a
    /// namespace holder, and a pod retrying on a backoff would exhaust the
    /// node while looking like it was merely waiting.
    async fn release_sandbox(&self, id: &str, handle: Option<Handle>) {
        self.sandboxes.lock().await.remove(id);
        if let Some(h) = handle {
            let _ = self.on_ring(move |r| r.sandbox_release(h)).await;
        }
    }

    /// DEL every network a sandbox has (#233): its extra networks, last first,
    /// then the default (the replaced one, else the cluster's). Every DEL is
    /// tried; the first error is answered. DEL of a network never added is
    /// allowed by the CNI contract, so a half-made sandbox is undone whole.
    async fn del_networks(&self, invoker: &cni::CniInvoker, id: &str, netns: &str, config: &PodSandboxConfig) -> Result<(), String> {
        let base = cni::PodNetwork::new(id, netns, &config.namespace, &config.name, &config.uid);
        let mut first: Option<String> = None;
        for a in config.networks.iter().rev() {
            let r = match cni::NetworkConfigList::from_json(&a.config, &a.name) {
                Ok(c) => invoker.del_network(&c, &attachment_pod(&base, a)).await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            if let Err(e) = r {
                first.get_or_insert(format!("{} ({}): {e}", a.name, a.ifname));
            }
        }
        let r = match &config.default_network {
            Some(d) => match cni::NetworkConfigList::from_json(&d.config, &d.name) {
                Ok(c) => invoker.del_network(&c, &base).await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            },
            None => invoker.del(&base).await.map_err(|e| e.to_string()),
        };
        if let Err(e) = r {
            first.get_or_insert(e);
        }
        first.map_or(Ok(()), Err)
    }

    /// DEL, then give the sandbox back. `false` when DEL failed: the caller
    /// keeps the record, and the namespace with it.
    async fn unwind_network(&self, failed: &FailedNetwork) -> bool {
        if let Some(invoker) = &self.cni {
            if let Err(e) = self.del_networks(invoker, &failed.id, &failed.netns, &failed.config).await {
                tracing::warn!(sandbox = %failed.id, pod = %failed.config.name,
                    "CNI DEL after a failed ADD did not succeed, retried before the next sandbox: {e}");
                return false;
            }
        }
        self.release_sandbox(&failed.id, failed.handle).await;
        true
    }

    /// CNI ADD of each extra network (#233), in order, each on its own
    /// interface. `Err` names the network that failed; the caller DELs every
    /// network and fails the sandbox.
    async fn add_attachments(&self, invoker: &cni::CniInvoker, base: &cni::PodNetwork, config: &PodSandboxConfig) -> Result<Vec<cni::CniResult>, String> {
        let mut out = Vec::new();
        for a in &config.networks {
            let c = cni::NetworkConfigList::from_json(&a.config, &a.name)
                .map_err(|e| format!("network {} ({}): its config: {e}", a.name, a.ifname))?;
            let r = invoker
                .add_network(&c, &attachment_pod(base, a))
                .await
                .map_err(|e| format!("network {} ({}): {e}", a.name, a.ifname))?;
            out.push(r);
        }
        Ok(out)
    }

    /// Retry every failed ADD's DEL. One at a time, under the lock, so two
    /// sandboxes starting at once do not both run one.
    async fn retry_failed_networks(&self) {
        let mut failed = self.failed_networks.lock().await;
        let mut kept = Vec::new();
        for f in failed.drain(..) {
            if !self.unwind_network(&f).await {
                kept.push(f);
            }
        }
        *failed = kept;
    }

    /// Give this runtime a CNI to call.
    ///
    /// Optional because a node with no pod network is a real configuration —
    /// containers then share loopback with the rest of their pod and reach
    /// nothing else, which is what a pod on a network-less node honestly is.
    pub fn with_cni(mut self, invoker: Option<cni::CniInvoker>) -> Self {
        self.cni = invoker;
        self
    }

    pub fn new(socket: impl Into<String>) -> StormpumpRuntime {
        StormpumpRuntime {
            socket: socket.into(),
            ring: None,
            cni: None,
            sandboxes: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
            failed_networks: Mutex::new(Vec::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            image_configs: Arc::default(),
            roots: None,
            making: Default::default(),
            identities: None,
        }
    }

    /// Publish each workload's cgroup → pod/container identity (#84), and
    /// sweep the records whose workload is gone.
    pub fn with_identities(mut self, publisher: crate::workload_identity::Publisher) -> Self {
        let gone = publisher.sweep(crate::workload_identity::cgroup_of);
        if gone > 0 {
            tracing::info!(gone, "workload identities of workloads no longer running removed");
        }
        self.identities = Some(publisher);
        self
    }

    /// Publish `record` for the workload whose init process is `pid`.
    fn publish_identity(&self, mut record: crate::workload_identity::Record, pid: i32) -> Option<std::path::PathBuf> {
        let publisher = self.identities.as_ref()?;
        record.cgroup = crate::workload_identity::cgroup_of(pid)?;
        record.pid = pid;
        publisher.publish(&record)
    }

    fn withdraw_identity(&self, path: Option<std::path::PathBuf>) {
        if let (Some(p), Some(publisher)) = (path, &self.identities) {
            publisher.withdraw(&p);
        }
    }

    /// The engine that makes each container's own root (#104).
    pub fn with_roots(mut self, roots: crate::container_roots::Roots) -> Self {
        self.roots = Some(Arc::new(roots));
        self
    }

    /// Size the Pod groups (#57, stormpump#68): `pods` as `allocatable_cpus`
    /// cores of shares (OpenShift's `kubepods`), `pods/besteffort` the floor
    /// (upstream's 2 shares). An engine without groups refuses, and Pods keep
    /// the engine's defaults.
    pub async fn size_pod_groups(&self, allocatable_cpus: u32) {
        let pods = shares_to_weight(i64::from(allocatable_cpus) * 1024);
        for (group, weight) in [(stormpump::spec::Group::Pods, pods), (stormpump::spec::Group::PodsBestEffort, 1)] {
            match self.on_ring(move |r| r.group_set(group as u8, Some(weight), None)).await {
                Ok(()) => tracing::info!(group = group.dir(), weight, "stormpump: Pod group sized"),
                Err(e) => tracing::warn!(group = group.dir(), "stormpump: Pod group not sized (an engine before stormpump#68?): {e}"),
            }
        }
    }

    /// Delete the container roots nothing holds: what a kubelet that died
    /// between a create and a removal left (#104). Run once at start; a root
    /// still attached or mounted is never touched.
    pub async fn sweep_roots(&self) {
        let Some(roots) = self.roots.clone() else { return };
        let mut known: std::collections::HashSet<String> =
            self.making.lock().unwrap_or_else(|e| e.into_inner()).clone();
        known.extend(
            self.containers.lock().await.keys().map(|id| crate::container_roots::volume_name(id)),
        );
        let gone = roots.sweep(&known).await;
        if gone > 0 {
            tracing::info!(gone, "stormpump: orphan container roots deleted");
        }
    }

    /// The image configs the image service finds (#98): the same `Arc` both
    /// are given.
    pub fn with_image_configs(mut self, configs: Arc<crate::image_config::ImageConfigs>) -> Self {
        self.image_configs = configs;
        self
    }

    /// The ring, or an error naming what is missing.
    fn ring(&self) -> Result<Arc<RingClient>, CriError> {
        self.ring.clone().ok_or_else(|| {
            CriError::Connection(format!(
                "not attached to stormpump at {} — is it PID 1 on this node?",
                self.socket
            ))
        })
    }

    /// Run one ring call off the async runtime.
    ///
    /// `submit` blocks until the engine answers. That is microseconds in the
    /// ordinary case, but a blocking call on a runtime worker is a blocking
    /// call however short it usually is, and the case that matters is the
    /// engine that has stopped answering.
    async fn on_ring<T, F>(&self, f: F) -> Result<T, CriError>
    where
        F: FnOnce(&RingClient) -> Result<T, RingError> + Send + 'static,
        T: Send + 'static,
    {
        let ring = self.ring()?;
        tokio::task::spawn_blocking(move || f(&ring))
            .await
            .map_err(|e| CriError::Runtime(format!("ring call did not run: {e}")))?
            .map_err(|e| CriError::Runtime(e.to_string()))
    }

    /// Everything else about a sandbox going away.
    ///
    /// Split out from the ring call so the invariant it carries can be tested
    /// on a box with no engine: a container whose sandbox is gone has no
    /// namespaces to live in, and leaving it listed has the kubelet trying to
    /// reconcile something that cannot exist.
    async fn forget_containers_of(&self, sandbox_id: &str) {
        self.containers
            .lock()
            .await
            .retain(|_, c| c.sandbox_id != sandbox_id);
    }

    /// Take note of anything the engine says has ended.
    ///
    /// An exit arrives unsolicited, so this is how a crashed container stops
    /// being `Running` without the kubelet polling for it.
    async fn absorb_exits(&self) {
        let Ok(ring) = self.ring() else { return };
        let exits = tokio::task::spawn_blocking(move || ring.drain_exits())
            .await
            .unwrap_or_default();
        self.note_exits(exits).await;
    }

    /// [`Self::absorb_exits`] without the ring: record the exits, then try
    /// again every removal one of them (or the retry interval) unblocks.
    async fn note_exits(&self, exits: Vec<crate::stormpump_ring::Exited>) {
        let retry;
        {
        let mut containers = self.containers.lock().await;
        retry = removals_due(&containers, &exits);
        for e in exits {
            for c in containers.values_mut() {
                if c.workload_handle == Some(e.handle) {
                    c.state = ContainerState::Exited;
                    c.finished_at = now_nanos();
                    // A wait status: the low byte is the signal, the next is
                    // the exit code. Kubernetes reports 128+signal for a
                    // signalled container, which is what a shell does too.
                    let sig = (e.status & 0x7f) as i32;
                    c.exit_code = if sig != 0 {
                        128 + sig
                    } else {
                        ((e.status >> 8) & 0xff) as i32
                    };
                    tracing::info!(
                        container = %c.id,
                        name = %format!("{}/{}/{}", c.namespace, c.pod, c.name),
                        code = c.exit_code, "stormpump: container exited"
                    );
                }
            }
        }
        }
        self.retry_removals(retry).await;
    }

    /// Release what a container holds in the engine (its workload, then its
    /// volumes) and forget it once all of it is released. Each release that
    /// succeeds is dropped from the record, so a retry does only what is left.
    async fn finish_removal(&self, container_id: &str) -> Result<(), CriError> {
        let (workload, volumes) = {
            let containers = self.containers.lock().await;
            let Some(c) = containers.get(container_id) else { return Ok(()) };
            (c.workload_handle, c.volume_handles.clone())
        };
        if let Some(c) = self.containers.lock().await.get_mut(container_id) {
            c.removing = Some(std::time::Instant::now());
        }
        if let Some(workload) = workload {
            // Busy/timeout is pending cleanup, never permission to forget it.
            self.on_ring(move |r| r.workload_release(workload)).await?;
            if let Some(c) = self.containers.lock().await.get_mut(container_id) {
                c.workload_handle = None;
            }
        }
        for volume in volumes {
            self.on_ring(move |r| r.volume_release(volume)).await?;
            if let Some(c) = self.containers.lock().await.get_mut(container_id) {
                c.volume_handles.retain(|held| *held != volume);
            }
        }
        // Its own root last (#104): the last release unmounts it, then the
        // clone is detached and deleted. A refusal keeps the record, retried.
        let (root, volume) = {
            let containers = self.containers.lock().await;
            containers.get(container_id).map(|c| (c.root_handle, c.root_volume.clone())).unwrap_or_default()
        };
        if let Some(root) = root {
            self.on_ring(move |r| r.volume_release(root)).await?;
            if let Some(c) = self.containers.lock().await.get_mut(container_id) {
                c.root_handle = None;
            }
        }
        if let (Some(volume), Some(roots)) = (volume, self.roots.clone()) {
            roots.destroy(&volume).await.map_err(CriError::Runtime)?;
            if let Some(c) = self.containers.lock().await.get_mut(container_id) {
                c.root_volume = None;
            }
        }
        if let Some(c) = self.containers.lock().await.remove(container_id) {
            self.withdraw_identity(c.identity_file);
        }
        Ok(())
    }

    /// Undo a root made for a container that will not be created (#104).
    /// Where `image` (what `pull_image` returned) came from (#130): the
    /// registry's record for a pulled image's golden, else, for a pallet, its
    /// golden volume's entry in the node's release manifest.
    fn provenance_for(&self, image: &str) -> Option<crate::image_config::Provenance> {
        if let Some(p) = self.image_configs.provenance(image) {
            return Some(p);
        }
        // The volume the engine said is mounted at the pallet's path (#231),
        // else the command line's list, else the path's own name.
        if let Some(vol) = crate::container_roots::pallet_path(image)
            .and_then(|p| self.roots.as_ref().and_then(|r| r.pallet_volume_of(p)))
        {
            return crate::image_config::release_golden(&vol);
        }
        let cmdline = crate::container_roots::node_mount_list();
        match crate::container_roots::golden_of(image, &cmdline)? {
            crate::container_roots::Golden::Pallet(vol) => crate::image_config::release_golden(&vol),
            crate::container_roots::Golden::Template(_) => None,
        }
    }

    async fn discard_root(&self, handle: Option<Handle>, volume: &str) {
        if let Some(h) = handle {
            if let Err(e) = self.on_ring(move |r| r.volume_release(h)).await {
                tracing::warn!(volume = %volume, "a discarded root was not released: {e}");
            }
        }
        if let Some(roots) = &self.roots {
            if let Err(e) = roots.destroy(volume).await {
                tracing::warn!(volume = %volume, "a discarded root was not deleted (the sweep retries): {e}");
            }
        }
    }

    /// Finish the removals in `ids` (#90). A refusal keeps the record and is
    /// tried again later; nothing is forgotten here.
    async fn retry_removals(&self, ids: Vec<String>) {
        for id in ids {
            match self.finish_removal(&id).await {
                Ok(()) => tracing::info!(container = %id, "stormpump: deferred removal done, engine resources released"),
                Err(e) => tracing::debug!(container = %id, "stormpump: removal still refused, retried: {e}"),
            }
        }
    }

    fn mint_id(&self, prefix: &str) -> String {
        let n = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}-{n:08x}")
    }

    /// Whether stormpump is reachable at all.
    ///
    /// Checked rather than assumed, because the failure a kubelet gives when
    /// its runtime is absent should say which runtime and where — not surface
    /// as every pod failing to start for no stated reason.
    pub fn probe(&self) -> Result<(), CriError> {
        if std::path::Path::new(&self.socket).exists() {
            Ok(())
        } else {
            Err(CriError::Connection(format!(
                "no stormpump socket at {} — is stormpump PID 1 on this node?",
                self.socket
            )))
        }
    }
}

/// A CRI container config as a stormpump spec.
///
/// The two models line up more than they differ, and where they differ the
/// comment says which way it went and why.
/// The container's argv, env, working directory and user: the pod spec over
/// the image's own config (#98, `image_config::compose`), with `HOSTNAME`.
fn compose_for(
    config: &ContainerConfig,
    image: Option<&crate::image_config::ImageConfig>,
    image_name: Option<&str>,
    read_root: Option<&std::path::Path>,
) -> Result<crate::image_config::Composed, String> {
    let env: Vec<String> = config.envs.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let root = read_root.map(std::path::Path::to_path_buf);
    let pod = crate::image_config::PodSide {
        // The reference the pod wrote, when known, rather than its mount.
        image: image_name.unwrap_or(&config.image),
        command: &config.command,
        args: &config.args,
        env: &env,
        working_dir: &config.working_dir,
        run_as_user: config.run_as_user,
        run_as_group: config.run_as_group,
        run_as_non_root: config.run_as_non_root.unwrap_or(false),
    };
    let mut run = crate::image_config::compose(&pod, image, root.as_deref())?;
    // Upstream's kubelet sets HOSTNAME to the pod name, and plenty of software
    // reads it rather than calling uname.
    if !config.name.is_empty() && !run.env.iter().any(|e| e.starts_with("HOSTNAME=")) {
        run.env.push(format!("HOSTNAME={}", config.name));
    }
    Ok(run)
}

fn spec_for(
    config: &ContainerConfig,
    sandbox: &PodSandboxConfig,
    run: crate::image_config::Composed,
    read_root: Option<&std::path::Path>,
) -> stormpump::spec::Spec {
    use stormpump::spec::{Logs, Root, Share, Spec};

    let crate::image_config::Composed { mut argv, env, cwd, uid, gid } = run;

    // **argv[0] is resolved here, because the engine will not do it.**
    //
    // stormpump refuses a relative argv[0] on purpose — "resolving PATH is a
    // lookup, and lookups do not belong on a start path" — and that is the
    // right invariant for an engine. But Kubernetes says `command` is what the
    // *runtime* execs, and every real manifest writes it the way a shell would:
    // Cilium's containers say `cilium-agent`, `cilium-dbg`, `sh`. Passing those
    // through unchanged made every Cilium pod fail at SpecDefine.
    //
    // So the lookup happens on this side, where the image root is known, and
    // the engine still receives an absolute path.
    if let Some(first) = argv.first().cloned() {
        if !first.starts_with('/') {
            match read_root {
                Some(root) => match resolve_in_image(root, &first, &crate::image_config::path_of(&env)) {
                    Some(abs) => argv[0] = abs,
                    None => tracing::warn!(
                        image = %config.image, argv0 = %first,
                        "argv[0] is not on PATH inside the image; the engine will \
                         refuse the spec"
                    ),
                },
                None => tracing::warn!(
                    image = %config.image, argv0 = %first,
                    "cannot resolve argv[0]: the image is not on this node yet"
                ),
            }
        }
    }

    Spec {
        domain: Domain::Container,
        // The root arrives as a registered volume handle at spawn, not here:
        // the container's own clone of its image's golden (#104), which PID 1
        // mounted. `Chroot` is "enter the volume's mount view", which is what
        // a container root is.
        root: Root::Chroot,
        // Its own file, in the directory Kubernetes will look in.
        //
        // The spawn carries the log *volume* — the container's directory under
        // `/var/log/pods/<ns>_<pod>_<uid>/<container>/` — and the spec carries
        // the file's *name*, `<restart>.log`. Both are needed: the engine
        // opens the name with `openat` against the volume, and would otherwise
        // name the file after an id the client never sees, where nothing looks
        // for it.
        logs: Logs::Combined,
        // `<name>/<restart>.log` is what CRI hands us; the directory half is
        // the volume, so what is left is the file.
        log_name: config
            .log_path
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("0.log")
            .to_string(),
        // What the pod asked for, in a fixed order. The volumes themselves are
        // handles supplied at spawn, paired with these by position — so this
        // order and that order are the same order, and the engine refuses a
        // count that does not match.
        //
        // Capped at what a spec can carry. A pod past the cap is refused at
        // define time with the count in the message, rather than starting with
        // some of its volumes.
        mounts: config
            .mounts
            .iter()
            .take(MAX_MOUNTS)
            .map(|m| stormpump::spec::Mount {
                dst: m.container_path.clone(),
                readonly: m.readonly,
                // The pod's mountPropagation (#81, stormpump#35): rslave for
                // HostToContainer (Cilium's agent watching /sys/fs/bpf and
                // pod netns), rshared for Bidirectional (a CSI node plugin's
                // /var/lib/kubelet, whose mounts must reach the node). The pod
                // manager gives Bidirectional only to privileged containers,
                // as upstream does; the engine refuses it on a filesystem
                // mount, and these are all binds.
                propagation: match m.propagation {
                    MountPropagation::Private => stormpump::spec::Propagation::Private,
                    MountPropagation::HostToContainer => stormpump::spec::Propagation::HostToContainer,
                    MountPropagation::Bidirectional => stormpump::spec::Propagation::Bidirectional,
                },
                // Always a bind. A PersistentVolumeClaim resolves to a block
                // device, and the *engine* mounts it at registration (see the
                // spawn below) — the same path an image pull takes. Registering
                // the device node itself as a directory to bind failed every
                // claim with `ENOTDIR (attaching mounts)`.
                fstype: None,
            })
            .collect(),
        // hostNetwork is the node's namespace; anything else is the pod's,
        // which the sandbox already holds. `Profile::Host` is "no namespace at
        // all", which is exactly what hostNetwork means.
        profile: if config.host_network || sandbox.host_network {
            Profile::Host
        } else {
            Profile::Routed
        },
        argv,
        env,
        cwd,
        uid,
        gid,
        share: Share {
            // A container asking for hostPID in a sandbox not built for it is
            // the mismatch every runtime rejects, so the sandbox's answer wins
            // and the container's is folded into it.
            pid: config.host_pid || sandbox.host_pid,
            ipc: config.host_ipc || sandbox.host_ipc,
            uts: false,
        },
        tty: config.tty,
        // The pod's QoS group (#57, the owner's choice on #106: OpenShift's
        // shape, stormpump#68): Pods under `pods/`, beside the node services
        // rather than among them, so upstream's shares → weight conversion
        // makes a small request small relative to other Pods only.
        group: group_for(&config.qos_class),
        cpu_weight: weight_for(config),
        limits: limits_for(config),
        ..Spec::default()
    }
}

/// The cgroup group a pod's containers run in, by its QoS class (#57).
/// Unknown (a CRI caller that set none): the node group, as before.
fn group_for(qos: &str) -> stormpump::spec::Group {
    use stormpump::spec::Group;
    match qos {
        "Guaranteed" => Group::Pods,
        "Burstable" => Group::PodsBurstable,
        "BestEffort" => Group::PodsBestEffort,
        _ => Group::Node,
    }
}

/// Upstream's `cpuSharesToCPUWeight`: shares 2..=262144 onto weight 1..=10000.
pub fn shares_to_weight(shares: i64) -> u32 {
    let shares = shares.clamp(2, 262_144) as u64;
    (1 + (shares - 2) * 9999 / 262_142) as u32
}

/// A container's `cpu.weight`: its request's, once it is in a Pod group;
/// the engine's default otherwise (a weight among the node services would
/// starve it, which is why #106 put Pods under their own parent).
fn weight_for(config: &ContainerConfig) -> u32 {
    if group_for(&config.qos_class) == stormpump::spec::Group::Node {
        return stormpump::spec::Spec::default().cpu_weight;
    }
    shares_to_weight(config.cpu_shares)
}

/// A container's stats from the engine's block. `u64::MAX` is the engine's
/// "the kernel did not say", and becomes `None`, never a number.
fn stats_info(
    mut info: crate::cri::ContainerStatsInfo,
    st: &stormpump_abi::query::Stats,
    memory: Option<&stormpump_abi::query::Memory>,
) -> crate::cri::ContainerStatsInfo {
    let known = |v: u64| (v != stormpump_abi::query::UNKNOWN).then_some(v);
    info.cpu_usage_core_nanos = known(st.cpu_usage_usec).map(|us| us.saturating_mul(1000));
    // The working set when the engine gives it (stormpump#64), else
    // memory.current, which counts page cache and reads high.
    info.memory_working_set_bytes =
        memory.and_then(|m| known(m.working_set)).or_else(|| known(st.memory_current));
    info
}

/// What the container's `resources` ask of the engine (#57): the CRI numbers
/// the pod manager derived from the pod spec, onto stormpump's `Limits`.
///
/// - `memory_limit_bytes` → `memory.max`, and `memory.swap.max = 0`: the
///   kubelet runs with swap off, and upstream gives a limited container no
///   swap, where the engine's default would let it spill past its limit.
/// - `cpu_quota` / `cpu_period` → `cpu.max`.
///
/// Zero means "not set" in CRI, and becomes "not declared" here: a declared
/// limit is applied or the spawn is refused, so only what was asked for goes
/// in.
fn limits_for(config: &ContainerConfig) -> stormpump::spec::Limits {
    let mut l = stormpump::spec::Limits::default();
    if config.memory_limit_bytes > 0 {
        l.memory_max = Some(config.memory_limit_bytes as u64);
        l.swap_max = Some(0);
    }
    if config.cpu_quota > 0 {
        l.cpu_max = Some(stormpump::spec::CpuMax {
            quota_us: config.cpu_quota as u64,
            period_us: if config.cpu_period > 0 {
                config.cpu_period as u64
            } else {
                stormpump::spec::CpuMax::DEFAULT_PERIOD_US
            },
        });
    }
    l
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl RuntimeService for StormpumpRuntime {
    async fn version(&self) -> Result<(String, String, String), CriError> {
        self.probe()?;
        Ok((
            "0.1.0".to_string(),
            "stormpump".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ))
    }

    async fn run_pod_sandbox(&self, config: &PodSandboxConfig) -> Result<String, CriError> {
        self.probe()?;
        self.retry_failed_networks().await;
        let id = self.mint_id("sb");
        // The namespaces the sandbox is built with come from the pod, and they
        // have to be decided here rather than per container: a container asking
        // for hostPID in a sandbox that was not built for it is the mismatch
        // every runtime rejects, and the reason is that the sandbox's
        // namespaces already exist by the time the container is created.
        // Acquired, so the pod's containers share it.
        //
        // This is the difference between a pod and a bag of containers: they
        // land in namespaces that already exist rather than each making its
        // own, so they share a network and reach each other on localhost.
        //
        // Not for host networking — there is no namespace to hold, because the
        // containers are already in the same one, which is the node's.
        //
        // Everything else gets an isolated namespace: a CNI fills it when one
        // is installed, and until then its containers share loopback with each
        // other and reach nothing outside, which is what a pod on a node with
        // no pod network honestly is.
        // No config, no sandbox (#148): a pod scheduled before the CNI agent
        // is up used to acquire a namespace, find no conflist and release it
        // on every retry. Asked first, it waits for nothing but the file.
        if !config.host_network {
            if let Some(invoker) = &self.cni {
                if let Err(e) = invoker.network_ready() {
                    return Err(CriError::NetworkNotConfigured(format!(
                        "no CNI network configured yet ({e})"
                    )));
                }
            }
        }
        // Each step timed for the start timing (#139): on a slow disk the sum
        // was read as a stormblock clone, and no clone is made here.
        let mut steps = crate::cri::SandboxSteps::default();
        let acquiring = std::time::Instant::now();
        let (handle, netns) = if config.host_network {
            (None, None)
        } else {
            let (h, pid) = self.on_ring(|r| r.sandbox_acquire(PROFILE_ISOLATED)).await?;
            steps.acquire = acquiring.elapsed();
            // A pid of 0 means the engine reported no holder; `/proc/0/ns/net`
            // is not a path and handing it to a plugin would fail a long way
            // from here.
            let netns = (pid > 0).then(|| format!("/proc/{pid}/ns/net"));
            (Some(h), netns)
        };

        // Pod networking.
        //
        // **Nothing invoked a CNI here before**, so an isolated namespace was
        // created and then left empty: pods ran, reported no address, and had
        // no interface but loopback. Cilium was running and healthy the whole
        // time, because a plugin does nothing until something execs it.
        //
        // Asked per pod rather than at startup — the config appears when the
        // agent that writes it comes up, which is after the kubelet decided
        // anything it decided once.
        // One line saying what the two inputs actually are.
        //
        // Both have to be present for a plugin to run, and when nothing
        // happened there was no way to tell which was missing: a silent `if
        // let` on two options looks identical whichever half is None, and two
        // build-and-boot cycles went into guessing.
        tracing::info!(
            sandbox = %id, cni = self.cni.is_some(), netns = ?netns,
            "pod networking inputs"
        );
        let mut ip = String::new();
        let mut network_status = None;
        if let (Some(invoker), Some(ns)) = (&self.cni, &netns) {
            // A replaced default network (#233) needs no cluster config.
            let ready = match &config.default_network {
                Some(_) => Ok(String::new()),
                None => invoker.network_ready(),
            };
            match ready {
                Ok(_) => {
                    let pod = cni::PodNetwork::new(
                        &id,
                        ns,
                        &config.namespace,
                        &config.name,
                        &config.uid,
                    );
                    let adding = std::time::Instant::now();
                    let added = match &config.default_network {
                        Some(d) => match cni::NetworkConfigList::from_json(&d.config, &d.name) {
                            Ok(c) => invoker.add_network(&c, &pod).await,
                            Err(e) => Err(e),
                        },
                        None => invoker.add(&pod).await,
                    };
                    // Then each extra network, in order (#233).
                    let added = match added {
                        Ok(result) => match self.add_attachments(invoker, &pod, &config).await {
                            Ok(extra) => Ok((result, extra)),
                            Err(e) => Err(cni::CniError::Attachment(e)),
                        },
                        Err(e) => Err(e),
                    };
                    steps.cni = adding.elapsed();
                    match added {
                        Ok((result, extra)) => {
                            ip = result
                                .ips
                                .first()
                                .map(|i| i.address.split('/').next().unwrap_or("").to_string())
                                .unwrap_or_default();
                            tracing::info!(
                                sandbox = %id, pod = %config.name, %ip,
                                "CNI attached the pod network"
                            );
                            // What it wired, for the pod's annotation (#131).
                            let netns_path = ns.clone();
                            let mtu = |ifname: &str| crate::network_status::mtu_in_netns(&netns_path, ifname);
                            let mut status = crate::network_status::entries(&result, &mtu);
                            if let Some(d) = &config.default_network {
                                crate::network_status::rename(&mut status, &d.name);
                            }
                            for (a, r) in config.networks.iter().zip(&extra) {
                                crate::network_status::append(&mut status, crate::network_status::attachment_entries(r, &a.name, &a.ifname, &mtu));
                            }
                            network_status = Some(status);
                        }
                        // Same reasoning as a missing config: a pod that
                        // asked for a network and did not get one must not
                        // come up looking healthy.
                        //
                        // DEL first (#100): a chain that failed half way may
                        // have allocated an address or made an endpoint, and
                        // the CNI contract is that the runtime undoes it.
                        // Kept, namespace and all, until DEL succeeds.
                        Err(e) => {
                            let failed = FailedNetwork {
                                id: id.clone(),
                                handle,
                                netns: ns.clone(),
                                config: config.clone(),
                            };
                            if !self.unwind_network(&failed).await {
                                self.failed_networks.lock().await.push(failed);
                            }
                            return Err(CriError::NetworkNotReady(format!(
                                "CNI ADD failed: {e}"
                            )));
                        }
                    }
                }
                Err(e) => {
                    // **Wait, rather than run without a network.**
                    //
                    // This used to warn and carry on, so a pod created in the
                    // window before Cilium writes its conflist got loopback
                    // and nothing ever re-attached it: CoreDNS came up
                    // Running, with no address, and its Service had no
                    // endpoints. Running-but-unreachable is the worst of the
                    // three outcomes — it looks healthy.
                    //
                    // Failing here leaves the pod to be retried, and the
                    // retry succeeds as soon as the agent is up. The agent
                    // itself is hostNetwork, so it is not waiting on this.
                    // Checked before the acquire too: this is the config
                    // going between the two.
                    self.release_sandbox(&id, handle).await;
                    return Err(CriError::NetworkNotConfigured(format!(
                        "no CNI network configured yet ({e})"
                    )));
                }
            }
        }

        // Its namespace holder reports the pod's network to cadvisor (#84).
        let holder = netns.as_deref().and_then(|n| n.strip_prefix("/proc/")?.split('/').next()?.parse::<i32>().ok());
        let identity_file = holder.and_then(|pid| {
            self.publish_identity(
                crate::workload_identity::Record {
                    kind: "sandbox".into(),
                    reports_network: true,
                    namespace: config.namespace.clone(),
                    pod: config.name.clone(),
                    pod_uid: config.uid.clone(),
                    container_id: id.clone(),
                    labels: config.labels.clone().into_iter().collect(),
                    annotations: config.annotations.clone().into_iter().collect(),
                    ..Default::default()
                }
                .with_kubernetes_labels(),
                pid,
            )
        });
        let sb = Sandbox {
            id: id.clone(),
            handle,
            config: config.clone(),
            state: PodSandboxState::Ready,
            created_at: now_nanos(),
            netns,
            ip,
            made: Some(steps),
            identity_file,
            network_status,
        };
        self.sandboxes.lock().await.insert(id.clone(), sb);
        tracing::info!(
            sandbox = %id, pod = %config.name, ns = %config.namespace,
            host_network = config.host_network, host_pid = config.host_pid,
            "stormpump: pod sandbox created"
        );
        Ok(id)
    }

    /// The network goes at stop, as CRI has it (#137): CNI DEL gives the
    /// pod's address back and the namespace's holder is released. A finished
    /// Pod is stopped long before it is removed (its container records and
    /// logs stay until the Pod object goes), and DEL only at removal kept
    /// every finished Pod's address until then: Cilium's range filled at
    /// ~250 and every later Pod waited on "range is full".
    ///
    /// Idempotent. A failed DEL leaves the sandbox as it was, so the next
    /// stop (or the removal) runs it again; a failed release keeps the handle
    /// for the removal to retry.
    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        let existing = self.sandboxes.lock().await.get(sandbox_id).cloned();
        let Some(sb) = existing else { return Ok(()) };
        if let (Some(invoker), Some(ns)) = (&self.cni, &sb.netns) {
            // Every network it has, extra ones last-first (#233).
            self.del_networks(invoker, sandbox_id, ns, &sb.config)
                .await
                .map_err(|e| CriError::NetworkNotReady(format!("CNI DEL: {e}")))?;
            tracing::info!(sandbox = %sandbox_id, pod = %sb.config.name, "CNI released the pod network");
        }
        let released = match sb.handle {
            Some(h) => match self.on_ring(move |r| r.sandbox_release(h)).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(sandbox = %sandbox_id, "sandbox release at stop refused, retried at removal: {e}");
                    false
                }
            },
            None => true,
        };
        let mut withdrawn = None;
        if let Some(sb) = self.sandboxes.lock().await.get_mut(sandbox_id) {
            sb.state = PodSandboxState::NotReady;
            sb.netns = None;
            if released {
                sb.handle = None;
                withdrawn = sb.identity_file.take();
            }
        }
        self.withdraw_identity(withdrawn);
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        // Removals the engine refused before (a restart's old container,
        // #90) are tried again now, since they are what this waits on.
        let pending: Vec<String> = self.containers.lock().await.values()
            .filter(|c| c.sandbox_id == sandbox_id && c.removing.is_some())
            .map(|c| c.id.clone())
            .collect();
        self.retry_removals(pending).await;
        if self.containers.lock().await.values().any(|c|c.sandbox_id==sandbox_id) {
            return Err(CriError::Pending("sandbox still has container cleanup records".into()));
        }
        // Whatever stop did not finish (a sandbox never stopped, a refused
        // release) is done here; stop is idempotent.
        self.stop_pod_sandbox(sandbox_id).await?;
        let existing=self.sandboxes.lock().await.get(sandbox_id).cloned();
        if let Some(sb)=existing {
            if let Some(handle)=sb.handle {self.on_ring(move |r|r.sandbox_release(handle)).await?;}
        }
        if let Some(sb) = self.sandboxes.lock().await.remove(sandbox_id) {
            self.withdraw_identity(sb.identity_file);
        }
        Ok(())
    }

    async fn pod_sandbox_status(
        &self,
        sandbox_id: &str,
    ) -> Result<PodSandboxStatusInfo, CriError> {
        let sandboxes = self.sandboxes.lock().await;
        let sb = sandboxes
            .get(sandbox_id)
            .ok_or_else(|| CriError::NotFound(format!("sandbox {sandbox_id}")))?;
        Ok(PodSandboxStatusInfo {
            id: sb.id.clone(),
            state: sb.state,
            created_at: sb.created_at,
            // A host-network pod has the node's address; anything else has
            // whatever the CNI gave it, and an empty string where none ran —
            // reporting an address we were not given would make a pod look
            // reachable when it is not.
            ip: sb.ip.clone(),
            additional_ips: Vec::new(),
            netns_path: sb.netns.clone(),
            made: sb.made,
        })
    }

    async fn list_pod_sandbox(&self) -> Result<Vec<PodSandboxSummary>, CriError> {
        let sandboxes = self.sandboxes.lock().await;
        Ok(sandboxes
            .values()
            .map(|sb| PodSandboxSummary {
                id: sb.id.clone(),
                state: sb.state,
                uid: sb.config.uid.clone(),
                name: sb.config.name.clone(),
                namespace: sb.config.namespace.clone(),
            })
            .collect())
    }

    /// Each ready pod's interfaces, from its network namespace (#36).
    ///
    /// The sandbox's netns is named by its holder process
    /// (`/proc/<holder>/ns/net`), and `/proc/<holder>/net/dev` is that
    /// namespace's counters, which is where cAdvisor reads them too. No engine
    /// call is needed. A pod on the host network has no namespace of its own,
    /// and so no series, as upstream.
    async fn list_pod_network_stats(
        &self,
    ) -> Result<Vec<crate::cri::PodNetworkStats>, CriError> {
        let sandboxes = self.sandboxes.lock().await;
        Ok(sandboxes
            .values()
            .filter(|sb| sb.state == PodSandboxState::Ready)
            .filter_map(|sb| {
                let dev = sb.netns.as_deref()?.strip_suffix("/ns/net")?.to_string() + "/net/dev";
                let text = std::fs::read_to_string(dev).ok()?;
                Some(crate::cri::PodNetworkStats {
                    sandbox_id: sb.id.clone(),
                    pod: sb.config.name.clone(),
                    namespace: sb.config.namespace.clone(),
                    interfaces: crate::metrics::parse_net_dev(&text),
                })
            })
            .collect())
    }

    async fn create_container(
        &self,
        sandbox_id: &str,
        config: &ContainerConfig,
        _sandbox_config: &PodSandboxConfig,
    ) -> Result<String, CriError> {
        // The sandbox first, the engine second. A request naming a sandbox
        // that does not exist is wrong whether or not stormpump is running,
        // and answering it with "no stormpump on this node" sends the reader
        // to the wrong component — the kubelet's own bookkeeping is what has
        // the answer, and it needs nothing to give it.
        {
            let sandboxes = self.sandboxes.lock().await;
            if !sandboxes.contains_key(sandbox_id) {
                return Err(CriError::NotFound(format!("sandbox {sandbox_id}")));
            }
        }
        self.probe()?;
        let (namespace, pod, pod_uid) = {
            let sandboxes = self.sandboxes.lock().await;
            sandboxes
                .get(sandbox_id)
                .map(|sb| (sb.config.namespace.clone(), sb.config.name.clone(), sb.config.uid.clone()))
                .unwrap_or_default()
        };
        let id = self.mint_id("ct");

        // **Its own root** (#104, the owner's rule): one CoW clone of the
        // image's sealed golden, attached here and mounted by PID 1, never a
        // directory another container also runs on.
        let roots = self.roots.clone().ok_or_else(|| {
            CriError::Runtime(format!(
                "container {}: no engine to make its own root from image {}",
                config.name, config.image
            ))
        })?;
        let volume = crate::container_roots::volume_name(&id);
        self.making.lock().unwrap_or_else(|e| e.into_inner()).insert(volume.clone());
        let owner = serde_json::json!({ "kind": "Pod", "namespace": namespace, "name": pod, "uid": pod_uid });
        let made = roots.make(&volume, &config.image, &owner).await;
        let made = match made {
            Ok(m) => m,
            Err(e) => {
                self.making.lock().unwrap_or_else(|e| e.into_inner()).remove(&volume);
                return Err(CriError::Runtime(format!("container {}: its root: {e}", config.name)));
            }
        };
        let mount = crate::container_roots::mount_point(&id);
        let (at, device) = (mount.clone(), made.device.clone());
        let root = self.on_ring(move |r| r.volume_register_device(&at, &device, "ext4")).await;
        self.making.lock().unwrap_or_else(|e| e.into_inner()).remove(&volume);
        let root = match root {
            Ok(h) => h,
            Err(e) => {
                self.discard_root(None, &made.volume_id).await;
                return Err(CriError::Runtime(format!(
                    "container {}: stormpump would not mount its root {} at {mount}: {e}",
                    config.name, made.device
                )));
            }
        };
        if config.readonly_rootfs {
            tracing::warn!(
                container = %config.name, pod = %pod,
                "readOnlyRootFilesystem: the container's own root is mounted writable until the \
                 engine can mount it read-only (stormpump#108)"
            );
        }

        // User names and argv[0] are read from the image: a pallet's mount,
        // or this container's root for a pulled image.
        let read_root = image_root(&config.image).unwrap_or_else(|| std::path::PathBuf::from(&mount));
        // The image's own config under the pod spec (#98). Known when the
        // image service found it for this image; a refusal names the image.
        let image = self.image_configs.get(&config.image);
        let name = self.image_configs.image_of(&config.image);
        // The image's declared volumes (#172), as CRI-O's default
        // `image_volumes = "mkdir"`: a directory in this container's own root
        // (private and writable since #104, so its image content stays and it
        // goes with the container) for each path no pod mount covers.
        if let Some(img) = image.as_ref() {
            let covered: Vec<&str> = config.mounts.iter().map(|m| m.container_path.as_str()).collect();
            for path in crate::image_config::declared_volumes(img, &covered) {
                match crate::image_config::make_in_root(std::path::Path::new(&mount), &path) {
                    Ok(true) => {}
                    Ok(false) => tracing::warn!(
                        container = %config.name, %path,
                        "image volume not made: a symlink or a file is on its path in the image"
                    ),
                    Err(e) => tracing::warn!(container = %config.name, %path, "image volume not made: {e}"),
                }
            }
        }
        let defined = match compose_for(config, image.as_ref(), name.as_deref(), Some(&read_root)) {
            Ok(run) => {
                let encoded = spec_for(config, _sandbox_config, run, Some(&read_root)).encode();
                self.on_ring(move |r| r.spec_define(encoded)).await
            }
            Err(e) => Err(CriError::Runtime(format!("container {}: {e}", config.name))),
        };
        let spec = match defined {
            Ok(s) => s,
            Err(e) => {
                self.discard_root(Some(root), &made.volume_id).await;
                return Err(e);
            }
        };

        let c = Container {
            id: id.clone(),
            sandbox_id: sandbox_id.to_string(),
            name: config.name.clone(),
            namespace: namespace.clone(),
            pod: pod.clone(),
            mount_sources: config
                .mounts
                .iter()
                .take(MAX_MOUNTS)
                .map(|m| (m.host_path.clone(), m.fstype.clone()))
                .collect(),
            log_dir: format!(
                "{}/{}",
                _sandbox_config.log_directory.trim_end_matches('/'),
                config.name
            ),
            image: config.image.clone(),
            spec_handle: Some(spec),
            workload_handle: None,
            volume_handles: Vec::new(),
            root_handle: Some(root),
            root_volume: Some(made.volume_id.clone()),
            provenance: self.provenance_for(&config.image),
            resolved_at: Some(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
            identity: Some(crate::workload_identity::Record {
                kind: "container".into(),
                namespace: namespace.clone(),
                pod: pod.clone(),
                pod_uid: pod_uid.clone(),
                container: config.name.clone(),
                container_id: id.clone(),
                // The reference the pod wrote, not its golden's.
                image: self.image_configs.image_of(&config.image).unwrap_or_else(|| config.image.clone()),
                labels: config.labels.clone().into_iter().collect(),
                annotations: config.annotations.clone().into_iter().collect(),
                ..Default::default()
            }.with_kubernetes_labels()),
            identity_file: None,
            root_path: Some(mount.clone()),
            state: ContainerState::Created,
            created_at: now_nanos(),
            started_at: 0,
            finished_at: 0,
            exit_code: 0,
            privileged: config.privileged,
            host_network: config.host_network,
            host_pid: config.host_pid,
            removing: None,
        };
        let qualified = format!("{}/{}/{}", c.namespace, c.pod, c.name);
        self.containers.lock().await.insert(id.clone(), c);
        tracing::info!(
            container = %id, name = %qualified, image = %config.image, root = %volume,
            "stormpump: container created on its own root"
        );
        Ok(id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
        self.probe()?;
        let (spec, path, log_dir, sandbox_id, mount_sources) = {
            let containers = self.containers.lock().await;
            let c = containers
                .get(container_id)
                .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
            let spec = c.spec_handle.ok_or_else(|| {
                CriError::Runtime(format!("container {container_id} has no spec"))
            })?;
            // The root is the image's filesystem. Without one there is nothing
            // to run, and saying so is better than spawning a container onto
            // nothing and reporting that it started.
            let root = c.root_handle.ok_or_else(|| {
                CriError::Runtime(format!(
                    "container {container_id} has no root of its own (image {})",
                    c.image
                ))
            })?;
            let path = c.root_path.clone().unwrap_or_default();
            (spec, (root, path), c.log_dir.clone(), c.sandbox_id.clone(), c.mount_sources.clone())
        };
        let (root, path) = path;

        // The pod's sandbox, if it has one. A host-network pod has none and
        // each container is simply in the node's namespaces.
        let sandbox = {
            let sandboxes = self.sandboxes.lock().await;
            sandboxes.get(&sandbox_id).and_then(|sb| sb.handle).unwrap_or(Handle::NONE)
        };

        // Registered now, not at create: a volume handle is a resource the
        // engine holds, and holding one for a container that may never start
        // is a leak for as long as its pod is pending.
        // The directory the engine opens the log file in. Created here as well
        // as by the kubelet's own bookkeeping, because the engine resolves it
        // in the *host's* mount namespace and a missing directory is an EINVAL
        // at spawn rather than a missing log.
        //
        // **Failing here beats failing at spawn.** This used to warn and carry
        // on, and the engine then refused the spawn with `EINVAL (opening the
        // log file)` — a message that names neither the directory nor the
        // reason. When the node's root filesystem filled, every pod on the
        // node reported that, and the cause was an ENOSPC discarded right
        // here.
        if let Err(e) = std::fs::create_dir_all(&log_dir) {
            return Err(CriError::Runtime(format!(
                "cannot create the container log directory {log_dir}: {e}"
            )));
        }

        let (workload, held) = self
            .on_ring(move |r| {
                let mut held = Vec::new();
                let result = (|| {
                // Its own root, registered at create and released at removal.
                let logs = r.volume_register(&log_dir)?;
                held.push(logs);
                // One per mount point, in the spec's order.
                let mut mounts = Vec::with_capacity(mount_sources.len());
                for (src, fstype) in &mount_sources {
                    match fstype {
                        // A block device: PID 1 mounts it on the node, and the
                        // container binds that directory. Idempotent, so two
                        // pods sharing a ReadWriteOnce claim on this node get
                        // the one mount.
                        Some(fs) => {
                            let dev = src.rsplit('/').next().unwrap_or(src);
                            let host = format!("{PVC_ROOT}/{dev}");
                            let handle = r.volume_register_device(&host, src, fs)?;
                            held.push(handle);
                            mounts.push(handle);
                        }
                        None => {
                            let handle = r.volume_register(src)?;
                            held.push(handle);
                            mounts.push(handle);
                        },
                    }
                }
                // The mount *sources* by name, not only their count. A start
                // that fails at the mount step is otherwise a step with no
                // subject: "attaching mounts, ENOENT" does not say which of
                // them, and the destinations are inside the spec where this
                // log cannot see them.
                tracing::debug!(
                    ?spec, ?root, ?logs, ?sandbox, path = %path, logs_dir = %log_dir,
                    mounts = %mount_sources.iter().map(|(s, _)| s.as_str()).collect::<Vec<_>>().join(","),
                    "stormpump: spawning"
                );
                // **Name the mount, here.** The engine reports which one
                // failed as an index — it is the only side that can, doing the
                // mount in the host's namespace while this process runs in a
                // container — and the index only means something where the
                // list that was sent still exists, which is inside this
                // closure.
                r.spawn(spec, root, logs, sandbox, &mounts, Domain::Container as u8)
                    .map_err(|e| {
                        let named = match &e {
                            RingError::Failed { step, .. } => {
                                crate::stormpump_ring::failed_mount_index(*step)
                                    .and_then(|i| mount_sources.get(i).map(|s| (i, s.0.clone())))
                            }
                            _ => None,
                        };
                        match named {
                            Some((i, src)) => {
                                RingError::Detail(format!("{e}: mount {i} is {src}"))
                            }
                            None => e,
                        }
                    })
                })();
                // Preserve partial registrations even when a later operation fails.
                Ok((result, held))
            })
            .await?;

        let mut containers = self.containers.lock().await;
        let c = containers
            .get_mut(container_id)
            .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
        c.volume_handles = held;
        let workload = workload.map_err(|e| CriError::Runtime(e.to_string()))?;
        c.workload_handle = Some(workload);
        c.state = ContainerState::Running;
        c.started_at = now_nanos();
        let identity = c.identity.clone();
        drop(containers);
        // Who it is, for cadvisor (#84), from its pid's cgroup.
        if let (Some(record), true) = (identity, self.identities.is_some()) {
            let pid = self.on_ring(move |r| r.query_info(workload)).await.ok().flatten().map(|i| i.pid).unwrap_or(0);
            let file = self.publish_identity(record, pid);
            if let Some(c) = self.containers.lock().await.get_mut(container_id) {
                c.identity_file = file;
            }
        }
        let containers = self.containers.lock().await;
        let c = containers
            .get(container_id)
            .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
        tracing::info!(
            container = %c.id, name = %format!("{}/{}/{}", c.namespace, c.pod, c.name),
            workload = ?workload, "stormpump: container started"
        );
        Ok(())
    }

    async fn stop_container(&self, container_id: &str, timeout: i64) -> Result<(), CriError> {
        let workload = {
            let containers = self.containers.lock().await;
            containers
                .get(container_id)
                .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?
                .workload_handle
        };
        if let Some(w) = workload {
            // Signal, grace, kill is one op: the policy timer lives in the
            // engine rather than in every client that wants to stop something.
            let grace = timeout.max(0) as u64;
            let already_stopping=self.containers.lock().await.get(container_id).is_some_and(|c|c.finished_at!=0);
            if !already_stopping {self.on_ring(move |r| r.stop(w, grace)).await?;}
        }
        let mut containers = self.containers.lock().await;
        if let Some(c) = containers.get_mut(container_id) {
            c.state = ContainerState::Exited;
            c.finished_at = now_nanos();
        }
        Ok(())
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
        // Asked for from here on, whatever the engine answers: the runtime
        // finishes it if this attempt is refused (#90).
        {
            let mut containers = self.containers.lock().await;
            let Some(c) = containers.get_mut(container_id) else { return Ok(()) };
            c.removing.get_or_insert_with(std::time::Instant::now);
        }
        self.finish_removal(container_id).await
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> Result<ContainerStatusInfo, CriError> {
        self.absorb_exits().await;
        let containers = self.containers.lock().await;
        let c = containers
            .get(container_id)
            .filter(|c| c.removing.is_none())
            .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
        Ok(ContainerStatusInfo {
            id: c.id.clone(),
            name: c.name.clone(),
            state: c.state,
            created_at: c.created_at,
            started_at: c.started_at,
            finished_at: c.finished_at,
            exit_code: c.exit_code,
            image: c.image.clone(),
            image_ref: image_id(c),
            reason: String::new(),
            message: String::new(),
        })
    }

    /// What each running container has consumed, from the engine's `QUERY`
    /// stats block (#57).
    ///
    /// CPU is exact (`cpu_usage_usec`). Memory is `memory.current`, which
    /// counts the page cache that upstream's working set subtracts
    /// (`inactive_file`); the stats block has nothing to subtract it with, so
    /// this reads high for a container that does a lot of file I/O. A field
    /// the kernel did not provide stays `None`, and a container the engine has
    /// no stats for is left out.
    async fn list_container_stats(
        &self,
    ) -> Result<Vec<crate::cri::ContainerStatsInfo>, CriError> {
        self.absorb_exits().await;
        let Some(ring) = self.ring.clone() else {
            return Ok(vec![]);
        };
        let running: Vec<(Handle, crate::cri::ContainerStatsInfo)> = {
            let containers = self.containers.lock().await;
            containers
                .values()
                .filter(|c| c.state == ContainerState::Running && c.removing.is_none())
                .filter_map(|c| {
                    Some((
                        c.workload_handle?,
                        crate::cri::ContainerStatsInfo {
                            container_id: c.id.clone(),
                            name: c.name.clone(),
                            pod: c.pod.clone(),
                            namespace: c.namespace.clone(),
                            ..Default::default()
                        },
                    ))
                })
                .collect()
        };
        // One blocking task for the lot: each query is a ring round trip of
        // microseconds, and a task per container would cost more than the
        // queries.
        let stats = tokio::task::spawn_blocking(move || {
            running
                .into_iter()
                .filter_map(|(h, info)| {
                    let (st, memory) = ring.query_usage(h).ok()?;
                    Some(stats_info(info, &st?, memory.as_ref()))
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| CriError::Runtime(format!("stats: {e}")))?;
        Ok(stats)
    }

    async fn list_containers(
        &self,
        sandbox_id: Option<&str>,
    ) -> Result<Vec<ContainerStatusInfo>, CriError> {
        self.absorb_exits().await;
        let containers = self.containers.lock().await;
        Ok(containers
            .values()
            .filter(|c| sandbox_id.is_none_or(|s| c.sandbox_id == s))
            .filter(|c| c.removing.is_none())
            .map(|c| ContainerStatusInfo {
                id: c.id.clone(),
                name: c.name.clone(),
                state: c.state,
                created_at: c.created_at,
                started_at: c.started_at,
                finished_at: c.finished_at,
                exit_code: c.exit_code,
                image: c.image.clone(),
                image_ref: image_id(c),
                reason: String::new(),
                message: String::new(),
            })
            .collect())
    }

    /// The engine writes a container's log into the pod's log directory and
    /// a removal releases the workload and its root, never the file (#47):
    /// an init container is removed at once, its log kept.
    fn logs_survive_removal(&self) -> bool {
        true
    }

    async fn container_image_info(&self, container_id: &str) -> Option<crate::cri::ContainerImageInfo> {
        let containers = self.containers.lock().await;
        let c = containers.get(container_id)?;
        let p = c.provenance.clone().unwrap_or_default();
        Some(crate::cri::ContainerImageInfo {
            image_id: p.digest.clone(),
            golden: p.golden,
            instance: c.root_volume.clone(),
            resolved_at: c.resolved_at.clone(),
            build: p.build,
        })
    }

    async fn pod_network_status(&self, sandbox_id: &str) -> Option<serde_json::Value> {
        self.sandboxes.lock().await.get(sandbox_id).and_then(|s| s.network_status.clone())
    }

    async fn pod_of_workload(&self, handle: u64) -> Option<String> {
        let sandbox = {
            let containers = self.containers.lock().await;
            containers.values().find(|c| c.workload_handle.map(|h| h.0) == Some(handle))?.sandbox_id.clone()
        };
        let sandboxes = self.sandboxes.lock().await;
        Some(sandboxes.get(&sandbox)?.config.uid.clone()).filter(|u| !u.is_empty())
    }

    async fn exec_sync(
        &self,
        _container_id: &str,
        _cmd: &[String],
        _timeout: i64,
    ) -> Result<crate::cri::ExecSyncResult, CriError> {
        // A Task spawned into the container's sandbox is exactly this, and the
        // ring already has the op. Not wired yet, and an empty success would be
        // worse than an error: a readiness probe that "succeeds" without
        // running anything reports every container healthy.
        Err(CriError::Runtime(
            "exec_sync is not wired to the stormpump ring yet".to_string(),
        ))
    }
}

/// Images, from the registry next door.
///
/// The registry mints a copy-on-write clone of the image's golden and hands
/// back a volume. That volume *is* the container's root — there is no unpacking
/// step at container start, because the unpacking happened once when the image
/// was first seen. It is the same mechanism the node's own goldens use.
pub struct StormpumpImages {
    /// The registry's base URL, e.g. `http://127.0.0.1:5100`.
    registry: String,
    http: reqwest::Client,
    /// image ref -> the reference a container is created from
    /// (`template:<name>`, the image's sealed golden here), for images whose
    /// golden this kubelet has already found. Keyed on the ref as written: a
    /// tag that moves is a different image, and `imagePullPolicy` is what
    /// exists to ask again.
    pulled: Mutex<HashMap<String, String>>,
    /// Each image's config, from the registry's golden record (#98).
    configs: Arc<crate::image_config::ImageConfigs>,
    /// The release manifest's text, when given rather than read from the
    /// node (`RELEASE_MANIFESTS`): for tests of golden versions (#86).
    release_manifest: Option<String>,
    /// This node's NVMe host NQN (#236), sent as `host_nqn` on a clone
    /// request so the clone's export admits this host alone (stormblock#212).
    host_nqn: Option<String>,
}

/// Find `argv0` on the standard PATH *inside* an image root.
///
/// Returns the path as the container will see it (absolute, image-relative),
/// not the host path — the container is chrooted into the image, so
/// `/pallets/cilium/usr/bin/cilium-agent` on this side is `/usr/bin/cilium-agent`
/// on that one.
///
/// The directories are the container's `PATH` (the image's `Env` under the
/// pod's, #98), else the conventional one. A miss is reported, not guessed at.
fn resolve_in_image(root: &std::path::Path, argv0: &str, path: &[String]) -> Option<String> {
    // A command with a slash in it is a path already, just not an absolute
    // one — `./foo` or `bin/foo`. PATH is not consulted for those, the same as
    // a shell.
    if argv0.contains('/') {
        return None;
    }
    for dir in path {
        let candidate = root.join(dir.trim_start_matches('/')).join(argv0);
        if candidate.is_file() {
            return Some(format!("{dir}/{argv0}"));
        }
    }
    None
}

/// How long the registry is given to answer for an image's config (#98).
const CONFIG_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Where the engine mounts a claim's block device for its pods to bind.
const PVC_ROOT: &str = "/run/stormpump/pvc";

/// A pallet image's mounted path, for reading the image (user names, argv[0]),
/// or `None` for anything else: a pulled image (`template:<name>`) is read
/// from the container's own root once it is mounted (#104).
fn image_root(image: &str) -> Option<std::path::PathBuf> {
    image_root_in(image, std::path::Path::new(PALLET_ROOT))
}

fn image_root_in(image: &str, pallets: &std::path::Path) -> Option<std::path::PathBuf> {
    if image.starts_with(crate::container_roots::TEMPLATE_PREFIX) {
        return None;
    }
    StormpumpImages::local_path_in(image, pallets)
}

impl StormpumpImages {
    pub fn new(registry: impl Into<String>) -> StormpumpImages {
        StormpumpImages {
            registry: registry.into(),
            http: reqwest::Client::new(),
            pulled: Mutex::new(HashMap::new()),
            configs: Arc::default(),
            release_manifest: None,
            host_nqn: node_host_nqn(),
        }
    }

    /// Ask for clones as this host (#236), not the node's own NQN: for tests.
    pub fn with_host_nqn(mut self, nqn: Option<String>) -> StormpumpImages {
        self.host_nqn = nqn;
        self
    }

    /// Read golden versions (#86) against this manifest, not the node's.
    pub fn with_release_manifest(mut self, manifest: impl Into<String>) -> StormpumpImages {
        self.release_manifest = Some(manifest.into());
        self
    }

    /// Where the image configs it finds go (#98): the runtime reads them.
    pub fn with_image_configs(mut self, configs: Arc<crate::image_config::ImageConfigs>) -> StormpumpImages {
        self.configs = configs;
        self
    }

    /// Learn the config of `image`, rooted at `root`, from the registry's
    /// golden record (#98), unless it is known (or a miss is recent). Bounded:
    /// a pallet starts without the registry, and a slow answer must not hold
    /// it. Anything but a record with a config is a miss.
    async fn learn_config(&self, image: &str, root: &str) {
        if !self.configs.needs_lookup(root) {
            return;
        }
        let Ok(mut url) = reqwest::Url::parse(&self.registry) else {
            self.configs.put(root, image, None);
            return;
        };
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(["v1", "goldens", image]);
        }
        let config = match self.http.get(url.clone()).timeout(CONFIG_LOOKUP_TIMEOUT).send_retrying(retry::Policy::REGISTRY).await {
            Ok(r) if r.status().is_success() => r
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| crate::image_config::from_golden(&v)),
            Ok(r) => {
                tracing::debug!(image = %image, status = %r.status(), "registry has no golden record for the image");
                None
            }
            Err(e) => {
                tracing::debug!(image = %image, "registry not asked for the image's config: {e}");
                None
            }
        };
        tracing::info!(image = %image, root = %root, known = config.is_some(), "image config");
        self.configs.put(root, image, config);
    }

    /// Ask sbregistry's cluster for a golden this node has none of (#79), by
    /// the clone route, the one that starts the fetch. `Err` is the pull's
    /// failure with the registry's words; `Ok(Some(record))` the golden that
    /// turned ready meanwhile, its stray clone deleted.
    async fn demand(&self, image: &str) -> Result<Option<serde_json::Value>, String> {
        let url = format!("{}/v1/clones", self.registry.trim_end_matches('/'));
        let resp = self
            .http
            .post(&url)
            .json(&clone_request(image, self.host_nqn.as_deref()))
            .send_retrying(retry::Policy::REGISTRY)
            .await
            .map_err(|e| format!("registry {url} did not answer for {image}: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let answer: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        if !status.is_success() {
            let said = answer["error"].as_str().map(str::to_string).unwrap_or_else(|| text.chars().take(300).collect());
            return Err(format!("{image} is not on this node ({status}): {said}"));
        }
        // Ready in the meantime: the clone is not wanted (#104).
        if let Some(id) = answer["id"].as_str() {
            let gone = self.http.delete(format!("{url}/{id}")).send_retrying(retry::Policy::REGISTRY).await;
            if !matches!(gone, Ok(ref r) if r.status().is_success() || r.status().as_u16() == 404) {
                tracing::warn!(image = %image, clone = %id, "a registry clone minted by the demand was not deleted (the registry reaps it)");
            }
        }
        let mut again = reqwest::Url::parse(&self.registry).map_err(|e| format!("registry: {e}"))?;
        if let Ok(mut path) = again.path_segments_mut() {
            path.pop_if_empty().extend(["v1", "goldens", image]);
        }
        let record = match self.http.get(again).send_retrying(retry::Policy::REGISTRY).await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        };
        Ok(record)
    }

    pub fn registry(&self) -> &str {
        &self.registry
    }
}

/// Where a node's own images live once the initramfs has mounted them.
///
/// A golden *is* an image: a sealed filesystem, cloned copy-on-write, mounted
/// read-only. The ones a node ships with are already mounted here by the time
/// anything runs, so an image named after one needs no pull at all — the
/// filesystem is already on the node and the "pull" is a lookup.
const PALLET_ROOT: &str = "/pallets";

impl StormpumpImages {
    /// The mounted path for an image, if this node ships it as a golden.
    ///
    /// `docker.io/library/busybox:latest` -> `busybox`, so a pod can name an
    /// image the ordinary way and get the node's copy. A tag is ignored,
    /// deliberately: a golden is one sealed filesystem and its version is the
    /// pallet's, not a string in a pod spec.
    fn local_path(image: &str) -> Option<std::path::PathBuf> {
        Self::local_path_in(image, std::path::Path::new(PALLET_ROOT))
    }

    fn local_path_in(image: &str, pallets: &std::path::Path) -> Option<std::path::PathBuf> {
        let last = image.rsplit('/').next().unwrap_or(image);
        let name = last.split(['@', ':']).next().unwrap_or(last);
        if name.is_empty() {
            return None;
        }
        let p = pallets.join(name);
        // A directory that exists but is not a mount is an empty mount point —
        // the initramfs makes those for volumes it could not attach. Running a
        // container on one gives an empty root and a confusing failure, so it
        // is treated as absent.
        let has_content = std::fs::read_dir(&p).map(|mut d| d.next().is_some()).unwrap_or(false);
        has_content.then_some(p)
    }
}

#[async_trait]
impl ImageService for StormpumpImages {
    /// Make sure an image's **sealed golden** is on this node, and answer the
    /// reference a container is created from (#104).
    ///
    /// A pull clones nothing and mounts nothing: each container's root is its
    /// own clone of the golden, made at create (`container_roots`). The image
    /// was cloned once per image here before, and every container of it ran on
    /// that one clone; a container cloning *that* would have been a clone of a
    /// clone.
    ///
    /// 1. **A pallet.** The image shipped with the node: its golden is in the
    ///    slab. The reference is the pallet's path (`/pallets/<x>`).
    /// 2. **Already found.** A previous container of this image asked.
    /// 3. **The registry's.** sbregistry's golden record for the image names
    ///    the fstemplate it sealed in this node's engine: `template:<name>`.
    ///    A golden still building or failed is a pull that failed for now,
    ///    with the registry's answer (ErrImagePull, then back-off).
    /// 4. **Not on this node** (404): the cluster is asked for it (#79). Only
    ///    the clone route starts sbregistry's cluster fetch, so it is posted
    ///    as the demand: 503 "fetching it from the cluster" or 404 "push it"
    ///    is the pull's failure, retried on the back-off. A clone it minted
    ///    because the golden turned ready meanwhile is deleted at once: the
    ///    container's root is its own clone, never the registry's
    ///    (stormblock-registry#98 asks for a demand that mints nothing).
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        // A golden version other than the release's (#86): through the
        // registry, which fetches it from forge on demand, never the pallet.
        if let Some(v) = self.version_of(image) {
            if let Some(found) = self.pulled.lock().await.get(image).cloned() {
                return Ok(found);
            }
            tracing::info!(image = %image, golden = %v.golden, reference = %v.reference,
                "image asks for a golden version the release does not run; asking the registry");
            let reference = self.registry_golden(&v.reference).await?;
            self.pulled.lock().await.insert(image.to_string(), reference.clone());
            return Ok(reference);
        }
        if let Some(path) = Self::local_path(image) {
            tracing::info!(image = %image, path = %path.display(), "image is a golden on this node");
            let path = path.to_string_lossy().into_owned();
            self.learn_config(image, &path).await;
            return Ok(path);
        }
        if let Some(found) = self.pulled.lock().await.get(image).cloned() {
            return Ok(found);
        }
        let reference = self.registry_golden(image).await?;
        self.pulled.lock().await.insert(image.to_string(), reference.clone());
        Ok(reference)
    }

    /// Whether the image is on this node — as a golden, or already pulled.
    ///
    /// A pulled image has to count, or `imagePullPolicy: IfNotPresent` pulls
    /// every time and the cache above never gets consulted. A golden version
    /// (#86) counts only once pulled: the pallet is another version.
    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
        let path = match Self::local_path(image).filter(|_| self.version_of(image).is_none()) {
            Some(p) => Some(p.to_string_lossy().into_owned()),
            None => self.pulled.lock().await.get(image).cloned(),
        };
        Ok(path.map(|id| ImageInfo {
            id,
            repo_tags: vec![image.to_string()],
            repo_digests: Vec::new(),
            size: 0,
        }))
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(PALLET_ROOT) else { return Ok(out) };
        for e in entries.flatten() {
            let Some(name) = e.file_name().to_str().map(str::to_owned) else { continue };
            if Self::local_path(&name).is_some() {
                out.push(ImageInfo {
                    id: e.path().to_string_lossy().into_owned(),
                    repo_tags: vec![name],
                    repo_digests: Vec::new(),
                    size: 0,
                });
            }
        }
        Ok(out)
    }

    async fn remove_image(&self, _image: &str) -> Result<(), CriError> {
        // A golden is not this node's to delete: it is a pallet member, and
        // what runs is a clone of it. Removing images is the pallet's business.
        Ok(())
    }
}

impl StormpumpImages {
    /// The golden version `image` asks for, against the node's release
    /// manifest and its pallets (#86).
    fn version_of(&self, image: &str) -> Option<crate::image_config::GoldenVersion> {
        let manifest = self.release_manifest.clone().unwrap_or_else(|| {
            crate::image_config::RELEASE_MANIFESTS
                .iter()
                .find_map(|p| std::fs::read_to_string(p).ok())
                .unwrap_or_default()
        });
        crate::image_config::golden_version(image, &manifest, |n| Self::local_path(n).is_some())
    }

    /// The registry's ready golden for `image`, as the reference a container
    /// is created from (`template:<name>`), asking the cluster for it on a
    /// miss (steps 3 and 4 of [`ImageService::pull_image`]).
    async fn registry_golden(&self, image: &str) -> Result<String, CriError> {
        let mut url = reqwest::Url::parse(&self.registry)
            .map_err(|e| CriError::ImagePull(format!("registry {}: {e}", self.registry)))?;
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(["v1", "goldens", image]);
        }
        let resp = self
            .http
            .get(url.clone())
            .send_retrying(retry::Policy::REGISTRY)
            .await
            .map_err(|e| CriError::ImagePull(format!("registry {url} did not answer for {image}: {e}")))?;
        let status = resp.status();
        let mut text = resp.text().await.unwrap_or_default();
        let mut record: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        if status == reqwest::StatusCode::NOT_FOUND {
            match self.demand(image).await {
                Ok(Some(found)) => record = found,
                Ok(None) => {}
                Err(why) => return Err(CriError::ImagePull(why)),
            }
            text = record.to_string();
        }
        let reference = golden_reference(&record).ok_or_else(|| {
            CriError::ImagePull(format!(
                "registry has no ready golden for {image} ({status}): {}",
                text.chars().take(300).collect::<String>()
            ))
        })?;
        tracing::info!(image = %image, golden = %reference, "image's golden is on this node");
        self.configs.put(&reference, image, crate::image_config::from_golden(&record));
        // What it is and where it came from (#130).
        self.configs.put_provenance(&reference, crate::image_config::provenance_of_record(&record));
        Ok(reference)
    }
}

/// Where a node keeps its NVMe host NQN, as the kubelet sees it: the node's
/// own (`/hostroot`) first, then its own view.
pub const HOST_NQN_FILES: [&str; 2] = ["/hostroot/etc/nvme/hostnqn", "/etc/nvme/hostnqn"];

/// This node's NVMe host NQN, if it has one (#236): the first readable
/// `nqn.`… line of [`HOST_NQN_FILES`].
fn node_host_nqn() -> Option<String> {
    HOST_NQN_FILES.iter().find_map(|p| host_nqn_in(&std::fs::read_to_string(p).ok()?))
}

/// The NQN in a `hostnqn` file's text.
fn host_nqn_in(text: &str) -> Option<String> {
    text.lines().map(str::trim).find(|l| l.starts_with("nqn.")).map(str::to_string)
}

/// A clone request to sbregistry (#236): `host_nqn` when the node has one,
/// so the clone's export admits this host alone (stormblock-registry#102,
/// stormblock#212). Without it the engine admits any host while
/// `allow_any_host` is on, its default.
fn clone_request(golden: &str, host_nqn: Option<&str>) -> serde_json::Value {
    let mut body = serde_json::json!({ "golden": golden });
    if let Some(n) = host_nqn {
        body["host_nqn"] = serde_json::json!(n);
    }
    body
}

/// The reference a container is created from, for a registry golden record
/// that is ready: `template:<template_name>`, the fstemplate the registry
/// sealed in this node's engine (#104). `None` for anything not ready.
fn golden_reference(record: &serde_json::Value) -> Option<String> {
    if record["status"].as_str() != Some("ready") {
        return None;
    }
    let t = record["template_name"].as_str().filter(|t| !t.is_empty())?;
    Some(format!("{}{t}", crate::container_roots::TEMPLATE_PREFIX))
}

#[cfg(test)]
mod tests {

    /// #233: extra networks are added in order, each on its interface with
    /// what the pod asked for; DEL takes them last-first, then the default.
    #[tokio::test]
    async fn extra_networks_are_added_in_order_and_deleted_in_reverse() {
        use std::os::unix::fs::PermissionsExt;
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(conf.path().join("05-default.conflist"), r#"{"cniVersion":"1.0.0","name":"podnet","plugins":[{"type":"podnet"}]}"#).unwrap();
        // Each plugin logs its command, interface and runtimeConfig.
        for t in ["podnet", "bridge", "macvlan"] {
            let script = format!(
                "#!/bin/sh\nin=$(cat)\nrc=$(printf '%s' \"$in\" | grep -o '\"runtimeConfig\":{{[^}}]*}}' || true)\necho \"{t} $CNI_COMMAND $CNI_IFNAME $rc\" >> {dir}/calls\nprintf '%s' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.9.0.2/24\"}}]}}'\n",
                dir = bin.path().display()
            );
            let p = bin.path().join(t);
            std::fs::write(&p, script).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let invoker = cni::CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        let rt = StormpumpRuntime::new("/nonexistent");
        let att = |name: &str, ifname: &str, t: &str| crate::cri::NetworkAttachment {
            name: name.into(),
            ifname: ifname.into(),
            config: format!(r#"{{"cniVersion":"1.0.0","name":"{name}","plugins":[{{"type":"{t}","capabilities":{{"mac":true}}}}]}}"#),
            ips: vec![],
            mac: (t == "macvlan").then(|| "02:00:00:00:00:07".to_string()),
        };
        let config = PodSandboxConfig {
            name: "p".into(), namespace: "ns".into(), uid: "u".into(),
            networks: vec![att("ns/lan", "net1", "bridge"), att("other/storage", "stor0", "macvlan")],
            ..Default::default()
        };
        let base = cni::PodNetwork::new("sb-1", "/proc/1/ns/net", "ns", "p", "u");
        let results = rt.add_attachments(&invoker, &base, &config).await.unwrap();
        assert_eq!(results.len(), 2);
        rt.del_networks(&invoker, "sb-1", "/proc/1/ns/net", &config).await.unwrap();
        let calls = std::fs::read_to_string(bin.path().join("calls")).unwrap();
        let lines: Vec<&str> = calls.lines().map(str::trim).collect();
        assert_eq!(lines, [
            "bridge ADD net1",
            r#"macvlan ADD stor0 "runtimeConfig":{"mac":"02:00:00:00:00:07"}"#,
            r#"macvlan DEL stor0 "runtimeConfig":{"mac":"02:00:00:00:00:07"}"#,
            "bridge DEL net1",
            "podnet DEL eth0",
        ]);

        // A NAD whose config is not one names itself.
        let bad = PodSandboxConfig { networks: vec![crate::cri::NetworkAttachment { name: "ns/broken".into(), ifname: "net1".into(), config: "{}".into(), ..Default::default() }], ..config };
        let e = rt.add_attachments(&invoker, &base, &bad).await.unwrap_err();
        assert!(e.contains("ns/broken") && e.contains("net1"), "{e}");
    }

    /// A failed ADD is followed by DEL, and a DEL that fails keeps the
    /// sandbox until a later one succeeds (#100).
    #[tokio::test]
    async fn a_failed_cni_add_is_deleted_and_kept_until_del_succeeds() {
        use std::os::unix::fs::PermissionsExt;
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-net.conflist"),
            r#"{"cniVersion":"1.0.0","name":"net","plugins":[{"type":"fake"}]}"#,
        )
        .unwrap();
        // DEL fails until `ok` exists; every call is counted.
        let script = format!(
            "#!/bin/sh\ncat >/dev/null\necho \"$CNI_COMMAND\" >> {dir}/calls\n[ -e {dir}/ok ] && exit 0\necho '{{\"code\":11,\"msg\":\"busy\"}}'\nexit 1\n",
            dir = bin.path().display()
        );
        let plugin = bin.path().join("fake");
        std::fs::write(&plugin, script).unwrap();
        std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o755)).unwrap();
        let rt = StormpumpRuntime::new("/nonexistent").with_cni(Some(cni::CniInvoker::new(
            conf.path(),
            vec![bin.path().to_path_buf()],
        )));
        let calls = || std::fs::read_to_string(bin.path().join("calls")).unwrap_or_default();

        let failed = FailedNetwork {
            id: "sb-1".into(),
            handle: None,
            netns: "/proc/1/ns/net".into(),
            config: PodSandboxConfig { name: "p".into(), namespace: "ns".into(), uid: "u".into(), ..Default::default() },
        };
        assert!(!rt.unwind_network(&failed).await, "DEL refused: kept");
        rt.failed_networks.lock().await.push(failed);
        rt.retry_failed_networks().await;
        assert_eq!(rt.failed_networks.lock().await.len(), 1);
        assert_eq!(calls(), "DEL\nDEL\n");

        std::fs::write(bin.path().join("ok"), "").unwrap();
        rt.retry_failed_networks().await;
        assert!(rt.failed_networks.lock().await.is_empty());
        assert_eq!(calls(), "DEL\nDEL\nDEL\n");
    }
    use super::*;

    fn std_path() -> Vec<String> {
        crate::image_config::path_of(&[])
    }

    /// `spec_for` with the pod's own argv (no image config), and an empty one
    /// allowed: these tests are about the spec's other fields.
    fn spec_of(cc: &ContainerConfig, sb: &PodSandboxConfig) -> stormpump::spec::Spec {
        let run = compose_for(cc, None, None, None).unwrap_or_else(|_| crate::image_config::Composed {
            argv: Vec::new(),
            env: Vec::new(),
            cwd: "/".into(),
            uid: 0,
            gid: 0,
        });
        spec_for(cc, sb, run, None)
    }

    fn rt() -> StormpumpRuntime {
        StormpumpRuntime::new("/nonexistent/stormpump.sock")
    }

    /// A pod's limits reach the engine (#57): memory.max with no swap, and
    /// cpu.max from the quota and period. Unset (0) is not declared.
    #[test]
    fn resource_limits_become_the_specs_limits() {
        let cc = ContainerConfig {
            name: "app".into(),
            command: vec!["/bin/app".into()],
            memory_limit_bytes: 512 * 1024 * 1024,
            cpu_quota: 50_000,
            cpu_period: 100_000,
            cpu_shares: 512,
            ..Default::default()
        };
        let spec = spec_of(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.limits.memory_max, Some(512 * 1024 * 1024));
        assert_eq!(spec.limits.swap_max, Some(0));
        assert_eq!(
            spec.limits.cpu_max,
            Some(stormpump::spec::CpuMax { quota_us: 50_000, period_us: 100_000 })
        );
        assert_eq!(spec.limits.pids_max, None);
        // No QoS class (a CRI caller that set none): the node group, the
        // engine's default weight, as before.
        assert_eq!(spec.cpu_weight, stormpump::spec::Spec::default().cpu_weight);
        assert_eq!(spec.group, stormpump::spec::Group::Node);

        // What the engine receives is what was set: through the wire and back.
        let back = stormpump::spec::Spec::decode(&spec.encode()).unwrap();
        assert_eq!(back.limits, spec.limits);
    }

    /// #57 step 4 (#106, stormpump#68): a Pod's containers run in its QoS
    /// group, and the CPU request becomes a weight by upstream's conversion.
    #[test]
    fn a_pods_request_is_its_weight_inside_its_qos_group() {
        use stormpump::spec::Group;
        assert_eq!(shares_to_weight(2), 1, "no request: the floor");
        assert_eq!(shares_to_weight(1024), 39, "one CPU, as upstream");
        assert_eq!(shares_to_weight(262_144), 10_000);
        assert_eq!(shares_to_weight(0), 1, "clamped");
        let cc = |qos: &str, shares| ContainerConfig { name: "c".into(), command: vec!["/c".into()], cpu_shares: shares, qos_class: qos.into(), ..Default::default() };
        let s = spec_of(&cc("Guaranteed", 2048), &PodSandboxConfig::default());
        assert_eq!((s.group, s.cpu_weight), (Group::Pods, shares_to_weight(2048)));
        let s = spec_of(&cc("Burstable", 512), &PodSandboxConfig::default());
        assert_eq!((s.group, s.cpu_weight), (Group::PodsBurstable, shares_to_weight(512)));
        let s = spec_of(&cc("BestEffort", 2), &PodSandboxConfig::default());
        assert_eq!((s.group, s.cpu_weight), (Group::PodsBestEffort, 1));
        // Through the wire and back: the engine gets the group.
        let back = stormpump::spec::Spec::decode(&s.encode()).unwrap();
        assert_eq!((back.group, back.cpu_weight), (Group::PodsBestEffort, 1));
    }

    #[test]
    fn a_container_without_limits_declares_none() {
        let cc = ContainerConfig {
            name: "app".into(),
            command: vec!["/bin/app".into()],
            cpu_period: 100_000,
            ..Default::default()
        };
        assert!(spec_of(&cc, &PodSandboxConfig::default()).limits.is_empty());
        // A quota with no period gets the kernel's.
        let cc = ContainerConfig { cpu_quota: 25_000, cpu_period: 0, ..cc };
        assert_eq!(
            limits_for(&cc).cpu_max,
            Some(stormpump::spec::CpuMax { quota_us: 25_000, period_us: 100_000 })
        );
    }

    /// The engine's "unknown" is no number, not u64::MAX and not 0.
    #[test]
    fn stats_map_onto_cri_and_unknown_stays_unknown() {
        let st = stormpump_abi::query::Stats {
            cpu_usage_usec: 2_500_000,
            memory_current: 4096,
            ..Default::default()
        };
        let info = stats_info(crate::cri::ContainerStatsInfo::default(), &st, None);
        assert_eq!(info.cpu_usage_core_nanos, Some(2_500_000_000));
        assert_eq!(info.memory_working_set_bytes, Some(4096), "no memory block: memory.current");
        // stormpump#64's block: the real working set, page cache left out.
        let m = stormpump_abi::query::Memory { working_set: 1024, ..Default::default() };
        let info = stats_info(crate::cri::ContainerStatsInfo::default(), &st, Some(&m));
        assert_eq!(info.memory_working_set_bytes, Some(1024));
        let unknown_ws = stormpump_abi::query::Memory::default();
        let info = stats_info(crate::cri::ContainerStatsInfo::default(), &st, Some(&unknown_ws));
        assert_eq!(info.memory_working_set_bytes, Some(4096), "an unknown working set falls back");

        let unknown = stats_info(crate::cri::ContainerStatsInfo::default(), &Default::default(), None);
        assert_eq!(unknown.cpu_usage_core_nanos, None);
        assert_eq!(unknown.memory_working_set_bytes, None);
    }

    #[tokio::test]
    async fn no_engine_is_no_stats_not_an_error() {
        assert!(rt().list_container_stats().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_absent_engine_is_named_rather_than_guessed_at() {
        // The failure a kubelet gives when its runtime is missing should say
        // which runtime and where. "Pod failed to start" does not.
        let e = rt().probe().unwrap_err();
        let text = format!("{e:?}");
        assert!(text.contains("stormpump"), "{text}");
        assert!(text.contains("/nonexistent/stormpump.sock"), "{text}");
    }

    #[tokio::test]
    async fn ids_do_not_collide_within_a_millisecond() {
        // A timestamp-derived id would; two containers of a pod are created
        // back to back.
        let r = rt();
        let a = r.mint_id("ct");
        let b = r.mint_id("ct");
        assert_ne!(a, b);
        assert!(a.starts_with("ct-") && b.starts_with("ct-"));
    }

    /// Everything below runs without an engine. The lifecycle itself needs
    /// one — a sandbox is acquired from stormpump's pool and a container is a
    /// spawn — so what is tested here is the bookkeeping either side of the
    /// ring, and the failures a node without stormpump should give.

    /// #115: an exiting workload's handle names its pod, through its
    /// container and sandbox; an unknown one names none.
    #[tokio::test]
    async fn an_exiting_workload_names_its_pod() {
        let r = StormpumpRuntime::new("/run/stormpump.sock");
        r.sandboxes.lock().await.insert(
            "sb-1".into(),
            Sandbox {
                id: "sb-1".into(),
                handle: None,
                config: PodSandboxConfig { uid: "pod-uid-1".into(), ..Default::default() },
                state: PodSandboxState::Ready,
                created_at: 0,
                netns: None,
                ip: String::new(),
                made: None,
                identity_file: None,
                network_status: None,
            },
        );
        let mut c = bare("ct-1", "sb-1");
        c.workload_handle = Some(Handle(77));
        r.containers.lock().await.insert("ct-1".into(), c);
        assert_eq!(r.pod_of_workload(77).await.as_deref(), Some("pod-uid-1"));
        assert_eq!(r.pod_of_workload(78).await, None);
    }

    #[tokio::test]
    async fn sandbox_cleanup_waits_for_container_records() {
        let r = StormpumpRuntime::new("/run/stormpump.sock");
        // Placed directly: acquiring one needs the engine, and the invariant
        // under test is about the maps rather than about the acquisition.
        r.sandboxes.lock().await.insert(
            "sb-1".into(),
            Sandbox {
                id: "sb-1".into(),
                handle: None,
                config: PodSandboxConfig::default(),
                state: PodSandboxState::Ready,
                created_at: 0,
                netns: None,
                ip: String::new(),
                made: None,
                identity_file: None,
                network_status: None,
            },
        );
        for (id, sb) in [("ct-1", "sb-1"), ("ct-2", "sb-1"), ("ct-3", "sb-other")] {
            r.containers.lock().await.insert(
                id.into(),
                Container {
                    id: id.into(),
                    sandbox_id: sb.into(),
                    name: id.into(),
                    image: "i".into(),
                    spec_handle: None,
                    namespace: "default".into(),
                    pod: "p".into(),
                    log_dir: String::new(),
                    mount_sources: Vec::new(),
                    workload_handle: None,
            volume_handles: Vec::new(),
                    root_handle: None,
                    root_volume: None,
                    provenance: None,
                    resolved_at: None,
                    identity: None,
                    identity_file: None,
                    root_path: None,
                    state: ContainerState::Created,
                    created_at: 0,
                    started_at: 0,
                    finished_at: 0,
                    exit_code: 0,
                    privileged: false,
                    host_network: false,
                    host_pid: false,
                    removing: None,
                },
            );
        }

        assert!(r.remove_pod_sandbox("sb-1").await.is_err());
        assert!(r.sandboxes.lock().await.contains_key("sb-1"));
        // A refused engine release must retain the container and its volumes.
        r.containers.lock().await.get_mut("ct-1").unwrap().volume_handles.push(Handle::NONE);
        assert!(r.remove_container("ct-1").await.is_err());
        assert_eq!(r.containers.lock().await["ct-1"].volume_handles.len(), 1);
        r.containers.lock().await.get_mut("ct-1").unwrap().volume_handles.clear();
        r.remove_container("ct-1").await.unwrap();
        r.remove_container("ct-2").await.unwrap();
        r.remove_pod_sandbox("sb-1").await.unwrap();
        assert!(!r.sandboxes.lock().await.contains_key("sb-1"));

        // The two in that sandbox are gone; the one in another is not.
        let left = r.list_containers(None).await.unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, "ct-3");
    }

    #[tokio::test]
    async fn a_container_needs_a_sandbox_that_exists() {
        let r = StormpumpRuntime::new("/run/stormpump.sock");
        let cfg = PodSandboxConfig::default();
        let cc = ContainerConfig { name: "c".into(), ..Default::default() };
        let e = r.create_container("sb-nope", &cc, &cfg).await.unwrap_err();
        // The sandbox is checked before the engine is reached, so this says
        // "not found" rather than "no stormpump" even on a box without one.
        assert!(matches!(e, CriError::NotFound(_)), "{e:?}");
    }

    #[tokio::test]
    async fn a_node_without_stormpump_says_so() {
        let r = StormpumpRuntime::new("/nonexistent/stormpump.sock");
        let e = r.run_pod_sandbox(&PodSandboxConfig::default()).await.unwrap_err();
        let text = format!("{e}");
        assert!(text.contains("stormpump"), "{text}");
        assert!(text.contains("/nonexistent/stormpump.sock"), "{text}");
    }

    #[tokio::test]
    async fn exec_sync_refuses_rather_than_reporting_a_success_it_did_not_have() {
        let r = StormpumpRuntime::new("/run/stormpump.sock");
        let e = r.exec_sync("ct-1", &["true".into()], 1).await;
        assert!(e.is_err(), "an empty success would mark every probe healthy");
    }

    #[test]
    fn a_host_network_container_gets_no_namespace_at_all() {
        // `Profile::Host` is "no network namespace", which is exactly what
        // hostNetwork means — and what Cilium's agent runs with.
        let sandbox = PodSandboxConfig { host_network: true, ..Default::default() };
        let cc = ContainerConfig { name: "cilium".into(), ..Default::default() };
        let spec = spec_of(&cc, &sandbox);
        assert_eq!(spec.profile, Profile::Host);

        // And an ordinary pod is routed: east-west plus a default route.
        let spec = spec_of(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.profile, Profile::Routed);
    }

    /// #3 item 5: a host-network pod needs no CNI at all, which is what keeps the
    /// control plane and the CNI's own agent up while the pod network is broken.
    /// The same node's pod-network pod waits for the config instead of starting
    /// without an address.
    #[tokio::test]
    async fn a_host_network_pod_needs_no_cni_and_a_pod_network_one_waits_for_it() {
        let dir = std::env::temp_dir().join(format!("sp-hostnet-{}", std::process::id()));
        let conf = dir.join("net.d");
        std::fs::create_dir_all(&conf).unwrap();
        // `probe` asks only that the socket's path exists; neither path below
        // reaches the ring.
        let socket = dir.join("stormpump.sock");
        std::fs::write(&socket, b"").unwrap();
        let r = StormpumpRuntime::new(socket.to_str().unwrap())
            .with_cni(Some(cni::CniInvoker::new(&conf, vec![dir.join("bin")])));

        let host = PodSandboxConfig { name: "cilium".into(), host_network: true, ..Default::default() };
        let id = r.run_pod_sandbox(&host).await.expect("no CNI config is no reason to refuse the host network");
        let st = r.pod_sandbox_status(&id).await.unwrap();
        assert!(st.netns_path.is_none() && st.ip.is_empty(), "no namespace, no CNI address");
        assert_eq!(st.made, Some(crate::cri::SandboxSteps::default()), "no acquire, no ADD");

        let pod = PodSandboxConfig { name: "coredns".into(), ..Default::default() };
        match r.run_pod_sandbox(&pod).await {
            Err(CriError::NetworkNotConfigured(m)) => assert!(m.contains("no CNI network configured"), "{m}"),
            other => panic!("a pod-network pod must wait for the config, got {other:?}"),
        }
        assert_eq!(r.sandboxes.lock().await.len(), 1, "nothing made for the waiting pod");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_sandbox_decides_the_namespaces() {
        // A container asking for hostPID in a sandbox not built for it is the
        // mismatch every runtime rejects, because the sandbox's namespaces
        // already exist by the time the container is created. So the two are
        // folded together rather than allowed to disagree.
        let sandbox = PodSandboxConfig { host_pid: true, ..Default::default() };
        let cc = ContainerConfig { name: "c".into(), ..Default::default() };
        assert!(spec_of(&cc, &sandbox).share.pid);

        let cc = ContainerConfig { host_pid: true, ..Default::default() };
        assert!(spec_of(&cc, &PodSandboxConfig::default()).share.pid);
    }

    #[tokio::test]
    async fn two_namespaces_may_each_have_a_container_called_app() {
        // A container is namespace/pod/container, never the bare name. Two
        // namespaces each having an `app` is ordinary, and they are different
        // containers — anything that keys on the name alone merges them.
        let r = StormpumpRuntime::new("/run/stormpump.sock");
        for (id, ns) in [("sb-a", "alpha"), ("sb-b", "beta")] {
            r.sandboxes.lock().await.insert(
                id.into(),
                Sandbox {
                    id: id.into(),
                    handle: None,
                    config: PodSandboxConfig {
                        name: "web".into(),
                        namespace: ns.into(),
                        ..Default::default()
                    },
                    state: PodSandboxState::Ready,
                    created_at: 0,
                    netns: None,
                    ip: String::new(),
                    made: None,
                    identity_file: None,
                    network_status: None,
                },
            );
        }
        // Both are called `app`; both must exist, distinctly.
        let containers = r.containers.lock().await.len();
        assert_eq!(containers, 0);
        drop(containers);

        let mut ids = Vec::new();
        for sb in ["sb-a", "sb-b"] {
            let mut c = r.containers.lock().await;
            let id = r.mint_id("ct");
            let ns = r.sandboxes.lock().await[sb].config.namespace.clone();
            c.insert(
                id.clone(),
                Container {
                    id: id.clone(),
                    sandbox_id: sb.into(),
                    name: "app".into(),
                    namespace: ns,
                    pod: "web".into(),
                    log_dir: String::new(),
                    mount_sources: Vec::new(),
                    image: "busybox".into(),
                    spec_handle: None,
                    workload_handle: None,
            volume_handles: Vec::new(),
                    root_handle: None,
                    root_volume: None,
                    provenance: None,
                    resolved_at: None,
                    identity: None,
                    identity_file: None,
                    root_path: None,
                    state: ContainerState::Created,
                    created_at: 0,
                    started_at: 0,
                    finished_at: 0,
                    exit_code: 0,
                    privileged: false,
                    host_network: false,
                    host_pid: false,
                    removing: None,
                },
            );
            ids.push(id);
        }
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert_eq!(r.list_containers(None).await.unwrap().len(), 2);
        // And each is reachable in its own sandbox.
        assert_eq!(r.list_containers(Some("sb-a")).await.unwrap().len(), 1);
        assert_eq!(r.list_containers(Some("sb-b")).await.unwrap().len(), 1);
    }

    #[test]
    fn mounts_keep_the_order_the_spec_declares() {
        // The engine pairs the nth volume handle with the nth destination, so
        // these two lists are the same list seen twice. A mismatch does not
        // fail — it mounts the wrong volume at the right path, which is the
        // kind of bug that is found much later and by something else.
        use crate::cri::Mount;
        let cc = ContainerConfig {
            mounts: vec![
                Mount { container_path: "/data".into(), host_path: "/host/a".into(), ..Default::default() },
                Mount { container_path: "/cfg".into(), host_path: "/host/b".into(), readonly: true, ..Default::default() },
            ],
            ..Default::default()
        };
        let spec = spec_of(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.mounts.len(), 2);
        assert!(spec.mounts.iter().all(|m| m.propagation == stormpump::spec::Propagation::Private), "the default");
        assert_eq!(spec.mounts[0].dst, "/data");
        assert!(!spec.mounts[0].readonly);
        assert_eq!(spec.mounts[1].dst, "/cfg");
        assert!(spec.mounts[1].readonly, "a read-only mount stays read-only");

        // And the sources this records are in that same order.
        let sources: Vec<String> =
            cc.mounts.iter().take(MAX_MOUNTS).map(|m| m.host_path.clone()).collect();
        assert_eq!(sources, vec!["/host/a".to_string(), "/host/b".to_string()]);
    }

    /// With no image config, HOME and PATH arrive unset unless
    /// the runtime supplies them. Cilium's operator died on an empty $HOME
    /// after getting all the way to starting its hive.
    #[test]
    fn the_container_gets_the_environment_an_image_would_have_given_it() {
        let cfg = ContainerConfig {
            name: "cilium-operator".into(),
            command: vec!["/usr/bin/cilium-operator".into()],
            ..Default::default()
        };
        let env = compose_for(&cfg, None, None, None).unwrap().env;
        assert!(env.iter().any(|e| e == "HOME=/root"), "{env:?}");
        assert!(env.iter().any(|e| e.starts_with("PATH=/usr/local/sbin:")), "{env:?}");
        assert!(env.iter().any(|e| e == "HOSTNAME=cilium-operator"), "{env:?}");
    }

    /// Defaults, not overrides — a pod that sets HOME means it.
    #[test]
    fn the_pod_environment_wins_over_the_defaults() {
        let cfg = ContainerConfig {
            name: "c".into(),
            command: vec!["/bin/app".into()],
            envs: vec![
                ("HOME".to_string(), "/home/app".to_string()),
                ("PATH".to_string(), "/opt/bin".to_string()),
            ],
            ..Default::default()
        };
        let env = compose_for(&cfg, None, None, None).unwrap().env;
        assert!(env.iter().any(|e| e == "HOME=/home/app"), "{env:?}");
        assert!(env.iter().any(|e| e == "PATH=/opt/bin"), "{env:?}");
        // And exactly once each — a duplicate would leave which one wins to
        // whatever execve does with it.
        assert_eq!(env.iter().filter(|e| e.starts_with("HOME=")).count(), 1);
        assert_eq!(env.iter().filter(|e| e.starts_with("PATH=")).count(), 1);
    }

    /// #103: what a pull returns is what create_container is handed, and it
    /// must resolve to that mount — not to `/pallets/<volume>`. The volume is
    /// the one from the live repro on C2NR0Q2.
    #[test]
    fn a_pulled_images_reference_is_its_golden_not_a_shared_mount() {
        let pallets = tempfile::tempdir().unwrap();
        // A pull answers the golden's reference; there is no image-level mount
        // to read from (or run on): each container reads its own root (#104).
        assert_eq!(image_root_in("template:sbr-quay.io-a-b-1", pallets.path()), None);
        assert_eq!(
            golden_reference(&serde_json::json!({"status": "ready", "template_name": "sbr-a-1"})).as_deref(),
            Some("template:sbr-a-1")
        );
        for not_ready in [
            serde_json::json!({"status": "building", "template_name": "sbr-a-1"}),
            serde_json::json!({"status": "ready", "template_name": ""}),
            serde_json::json!({"error": "no golden a"}),
        ] {
            assert_eq!(golden_reference(&not_ready), None, "{not_ready}");
        }
    }

    /// A golden still resolves, by its ref or by the pallet path a pull of it
    /// returns; a name with no pallet behind it (or an empty one) does not.
    #[test]
    fn a_golden_resolves_by_ref_or_by_pallet_path() {
        let pallets = tempfile::tempdir().unwrap();
        let busybox = pallets.path().join("busybox");
        std::fs::create_dir_all(busybox.join("bin")).unwrap();
        std::fs::create_dir_all(pallets.path().join("empty")).unwrap();
        for image in ["busybox", "docker.io/library/busybox:latest", busybox.to_str().unwrap()] {
            assert_eq!(image_root_in(image, pallets.path()), Some(busybox.clone()), "{image}");
        }
        assert_eq!(image_root_in("empty", pallets.path()), None);
        assert_eq!(image_root_in("quay.io/x/nonesuch:1", pallets.path()), None);
    }

    /// The engine refuses a relative argv[0], and every real manifest writes
    /// one — Cilium says `cilium-agent`, `cilium-dbg`, `sh`. Resolving it here
    /// is what lets those run.
    #[test]
    fn argv0_resolves_against_the_image_path() {
        let root = std::env::temp_dir().join(format!("rk-argv-{}", std::process::id()));
        let bin = root.join("usr/bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("cilium-agent"), b"#!/bin/true\n").unwrap();

        // Found on PATH, and reported as the *container* sees it.
        assert_eq!(
            resolve_in_image(&root, "cilium-agent", &std_path()),
            Some("/usr/bin/cilium-agent".to_string())
        );
        // Not there at all.
        assert_eq!(resolve_in_image(&root, "nonesuch", &std_path()), None);
        // A command containing a slash is a path, not a PATH lookup — same as
        // a shell.
        assert_eq!(resolve_in_image(&root, "./cilium-agent", &std_path()), None);
        assert_eq!(resolve_in_image(&root, "usr/bin/cilium-agent", &std_path()), None);

        // Order matters: /usr/local/bin wins over /usr/bin.
        let local = root.join("usr/local/bin");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("cilium-agent"), b"x").unwrap();
        assert_eq!(
            resolve_in_image(&root, "cilium-agent", &std_path()),
            Some("/usr/local/bin/cilium-agent".to_string())
        );

        // A directory of the right name is not a command.
        std::fs::create_dir_all(root.join("usr/bin/adir")).unwrap();
        assert_eq!(resolve_in_image(&root, "adir", &std_path()), None);

        std::fs::remove_dir_all(&root).ok();
    }

    /// #81: each mount's propagation reaches the engine's spec as CRI's mode.
    #[test]
    fn mount_propagation_reaches_the_spec() {
        use stormpump::spec::Propagation;
        let m = |dst: &str, p: MountPropagation| crate::cri::Mount {
            container_path: dst.into(),
            host_path: format!("/host{dst}"),
            propagation: p,
            ..Default::default()
        };
        let cc = ContainerConfig {
            command: vec!["/bin/agent".into()],
            privileged: true,
            mounts: vec![
                m("/etc/cfg", MountPropagation::Private),
                m("/sys/fs/bpf", MountPropagation::HostToContainer),
                m("/var/lib/kubelet", MountPropagation::Bidirectional),
            ],
            ..Default::default()
        };
        let spec = spec_of(&cc, &PodSandboxConfig::default());
        let got: Vec<_> = spec.mounts.iter().map(|m| (m.dst.as_str(), m.propagation)).collect();
        assert_eq!(
            got,
            [
                ("/etc/cfg", Propagation::Private),
                ("/sys/fs/bpf", Propagation::HostToContainer),
                ("/var/lib/kubelet", Propagation::Bidirectional)
            ]
        );
        // Every mount stays a bind: the engine refuses propagation on a filesystem mount.
        assert!(spec.mounts.iter().all(|m| m.fstype.is_none()));
        // And it survives the encoding the engine is handed.
        assert!(!spec.encode().is_empty());
    }

    #[test]
    fn command_and_args_become_one_argv() {
        let cc = ContainerConfig {
            command: vec!["/usr/bin/cilium-agent".into()],
            args: vec!["--config-dir".into(), "/tmp/cilium".into()],
            ..Default::default()
        };
        let spec = spec_of(&cc, &PodSandboxConfig::default());
        assert_eq!(
            spec.argv,
            vec!["/usr/bin/cilium-agent", "--config-dir", "/tmp/cilium"]
        );
    }

    /// A fake sbregistry: clones by id, with the list filter, mint and bind
    /// (#143). `refuse_bind` makes a bind answer 500; `refuse_list` the list.
    #[derive(Default)]
    struct FakeRegistry {
        clones: Vec<serde_json::Value>,
        minted: usize,
        refuse_bind: bool,
        refuse_list: bool,
        /// Golden records asked for, by name as the registry decoded it (#98).
        golden_lookups: Vec<String>,
        /// A golden that became ready while the demand was posted (#79).
        arrived: bool,
        /// Clones deleted.
        deleted: Vec<String>,
    }

    async fn fake_registry() -> (String, Arc<std::sync::Mutex<FakeRegistry>>) {
        use axum::extract::{Path, Query, State};
        use axum::http::StatusCode;
        use axum::Json;
        type S = Arc<std::sync::Mutex<FakeRegistry>>;
        let state: S = Default::default();
        let app = axum::Router::new()
            .route(
                "/v1/clones",
                axum::routing::get(
                    |State(s): State<S>, Query(q): Query<HashMap<String, String>>| async move {
                        let r = s.lock().unwrap();
                        if r.refuse_list {
                            return Err(StatusCode::SERVICE_UNAVAILABLE);
                        }
                        Ok(Json(serde_json::Value::Array(
                            r.clones
                                .iter()
                                .filter(|c| q.get("consumer").map_or(true, |w| c["consumer"] == **w))
                                .filter(|c| q.get("state").map_or(true, |w| c["state"] == **w))
                                .cloned()
                                .collect(),
                        )))
                    },
                )
                .post(|State(s): State<S>, Json(b): Json<serde_json::Value>| async move {
                    let mut r = s.lock().unwrap();
                    // The cluster demand (#79): elsewhere in the cluster, or nowhere.
                    match b["golden"].as_str() {
                        Some("quay.io/a/elsewhere:1") => return Err((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                            "error": "quay.io/a/elsewhere:1 is not on this node: fetching it from the cluster (job j1, queued); retry shortly"})))),
                        Some("quay.io/a/nonesuch:1") => return Err((StatusCode::NOT_FOUND, Json(serde_json::json!({
                            "error": "no ready golden quay.io/a/nonesuch:1 — push the image, or POST /v1/goldens to build it"})))),
                        Some("quay.io/a/arrived:1") => r.arrived = true,
                        // #86: a version the node does not hold yet: fetched from forge.
                        Some("registry/nextnfs:5555cccc6666") => return Err((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
                            "error": "registry/nextnfs:5555cccc6666 is not on this node: fetching it from forge (job j2, queued); retry shortly"})))),
                        _ => {}
                    }
                    r.minted += 1;
                    let n = r.minted;
                    let c = serde_json::json!({
                        "id": format!("c{n}"), "volume_name": format!("img-clone-{n}"),
                        "state": "claimed",
                    });
                    r.clones.push(c.clone());
                    Ok(Json(c))
                }),
            )
            .route(
                "/v1/clones/{id}",
                axum::routing::delete(|State(s): State<S>, Path(id): Path<String>| async move {
                    s.lock().unwrap().deleted.push(id);
                    Json(serde_json::json!({}))
                }),
            )
            .route(
                "/v1/clones/{id}/bind",
                axum::routing::post(
                    |State(s): State<S>, Path(id): Path<String>, Json(b): Json<serde_json::Value>| async move {
                        let mut r = s.lock().unwrap();
                        if r.refuse_bind {
                            return Err((StatusCode::INTERNAL_SERVER_ERROR, "state not saved".to_string()));
                        }
                        let c = r.clones.iter_mut().find(|c| c["id"] == *id)
                            .ok_or((StatusCode::NOT_FOUND, format!("no clone {id}")))?;
                        c["consumer"] = b["consumer"].clone();
                        c["state"] = "bound".into();
                        Ok(Json(c.clone()))
                    },
                ),
            )
            // A golden record (#98): only the coredns image has a config.
            .route(
                "/v1/goldens/{name}",
                axum::routing::get(|State(s): State<S>, Path(name): Path<String>| async move {
                    s.lock().unwrap().golden_lookups.push(name.clone());
                    if name == "registry.k8s.io/coredns/coredns:v1.11.1" {
                        Ok(Json(serde_json::json!({"name": name, "status": "ready",
                            "template_name": "sbr-coredns-1", "config": {
                            "Entrypoint": ["/coredns"], "User": "65532:65532", "WorkingDir": "/"}})))
                    } else if name == "quay.io/a/arrived:1" && s.lock().unwrap().arrived {
                        Ok(Json(serde_json::json!({"name": name, "status": "ready", "template_name": "sbr-arrived-1"})))
                    } else if name == "registry/nextnfs:3333bbbb4444" {
                        // #86: a version this node holds now (fetched from forge).
                        Ok(Json(serde_json::json!({"name": name, "status": "ready", "template_name": "golden-nextnfs-3333bbbb4444"})))
                    } else if name == "quay.io/a/building:1" {
                        Ok(Json(serde_json::json!({"name": name, "status": "building", "template_name": "sbr-b-1"})))
                    } else {
                        Err((StatusCode::NOT_FOUND, format!("no golden {name}")))
                    }
                }),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, state)
    }

    /// #236: a clone request names this host when it has an NQN, and the
    /// file's NQN is its first `nqn.` line.
    #[test]
    fn a_clone_request_names_this_host() {
        let n = "nqn.2014-08.org.nvmexpress:uuid:3f1c0a5e-1111-2222-3333-444455556666";
        assert_eq!(clone_request("registry/x:1", Some(n)), serde_json::json!({"golden": "registry/x:1", "host_nqn": n}));
        assert_eq!(clone_request("registry/x:1", None), serde_json::json!({"golden": "registry/x:1"}));
        assert_eq!(host_nqn_in(&format!("{n}\n")).as_deref(), Some(n));
        assert_eq!(host_nqn_in("\n# comment\n"), None);
    }

    /// #86: a tag naming a golden version other than the release's is pulled
    /// from the registry (and fetched on demand), never the pallet; the
    /// release's own version and an untagged image stay the pallet.
    #[tokio::test]
    async fn a_golden_version_is_pulled_through_the_registry() {
        let (url, reg) = fake_registry().await;
        let manifest = r#"{"assets":[{"kind":"golden","name":"nextnfs","digest":"ab","provenance":"golden-nextnfs-1111aaaa2222"}]}"#;
        let img = StormpumpImages::new(&url).with_release_manifest(manifest);
        // Held by the registry: a template the container root is cloned from.
        let r = img.pull_image("nextnfs:3333bbbb4444").await.unwrap();
        assert_eq!(r, "template:golden-nextnfs-3333bbbb4444");
        assert!(img.image_status("nextnfs:3333bbbb4444").await.unwrap().is_some(), "pulled: present");
        assert_eq!(reg.lock().unwrap().golden_lookups, vec!["registry/nextnfs:3333bbbb4444".to_string()]);
        // Not held: the demand, and the registry's words as the pull failure.
        match img.pull_image("nextnfs:5555cccc6666").await {
            Err(CriError::ImagePull(why)) => assert!(why.contains("fetching it from forge"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(img.image_status("nextnfs:5555cccc6666").await.unwrap().is_none());
        // The release's own version is not a registry question.
        let before = reg.lock().unwrap().golden_lookups.len();
        assert!(img.version_of("nextnfs:1111aaaa2222").is_none());
        assert!(img.version_of("nextnfs").is_none());
        assert_eq!(reg.lock().unwrap().golden_lookups.len(), before);
    }

    /// #98: the image service learns an image's config from its golden record
    /// once per root (a miss is believed for a while), and the runtime applies
    /// it under the pod spec: CoreDNS's args run after its Entrypoint, as its
    /// image's User.
    #[tokio::test]
    async fn the_images_config_is_learned_once_and_applied_under_the_pod_spec() {
        let (url, reg) = fake_registry().await;
        let configs = Arc::new(crate::image_config::ImageConfigs::default());
        let img = StormpumpImages::new(&url).with_image_configs(configs.clone());
        let coredns = "registry.k8s.io/coredns/coredns:v1.11.1";
        let root = "/run/stormpump/images/img-clone-1";
        img.learn_config(coredns, root).await;
        img.learn_config(coredns, root).await;
        img.learn_config("busybox", "/pallets/busybox").await;
        img.learn_config("busybox", "/pallets/busybox").await;
        assert_eq!(
            reg.lock().unwrap().golden_lookups,
            vec![coredns.to_string(), "busybox".to_string()],
            "the name reaches the registry whole (slashes and all), once per root"
        );

        let cc = ContainerConfig {
            name: "coredns".into(),
            image: root.into(),
            args: vec!["-conf".into(), "/etc/coredns/Corefile".into()],
            ..Default::default()
        };
        let run = compose_for(&cc, configs.get(root).as_ref(), configs.image_of(root).as_deref(), None).unwrap();
        let spec = spec_for(&cc, &PodSandboxConfig::default(), run, None);
        assert_eq!(spec.argv, vec!["/coredns", "-conf", "/etc/coredns/Corefile"]);
        assert_eq!((spec.uid, spec.gid), (65532, 65532));

        // The pallet has no config: the same args-only container is refused,
        // naming the image, instead of exec'ing `-conf`.
        let cc = ContainerConfig { image: "/pallets/busybox".into(), ..cc };
        let e = compose_for(&cc, configs.get("/pallets/busybox").as_ref(), configs.image_of("/pallets/busybox").as_deref(), None)
            .unwrap_err();
        assert!(e.contains("image busybox ") && e.contains("-conf"), "{e}");
    }

    /// #104: a pull makes sure the image's sealed golden is here and answers
    /// its reference. It clones nothing: the registry mints no clone, and a
    /// second container of the image asks nothing again.
    #[tokio::test]
    async fn a_pull_finds_the_golden_and_clones_nothing() {
        let (url, reg) = fake_registry().await;
        let configs = Arc::new(crate::image_config::ImageConfigs::default());
        let img = StormpumpImages::new(&url).with_image_configs(configs.clone());
        let coredns = "registry.k8s.io/coredns/coredns:v1.11.1";
        let reference = img.pull_image(coredns).await.unwrap();
        assert_eq!(reference, "template:sbr-coredns-1");
        assert_eq!(img.pull_image(coredns).await.unwrap(), reference);
        let r = reg.lock().unwrap();
        assert_eq!(r.minted, 0, "no clone at pull time: the container's root is its own clone");
        assert_eq!(r.golden_lookups, vec![coredns.to_string()], "asked once");
        drop(r);
        // The config came with the record, keyed by the reference.
        assert_eq!(configs.get(&reference).unwrap().entrypoint, vec!["/coredns".to_string()]);
        assert_eq!(configs.image_of(&reference).as_deref(), Some(coredns));
        // Still building, or unknown: a failed pull, with the registry's answer.
        let e = img.pull_image("quay.io/a/building:1").await.unwrap_err().to_string();
        assert!(e.contains("no ready golden") && e.contains("building"), "{e}");
        let e = img.pull_image("quay.io/a/nonesuch:1").await.unwrap_err().to_string();
        assert!(e.contains("not on this node (404") && e.contains("push the image"), "{e}");
        assert_eq!(reg.lock().unwrap().minted, 0);
    }

    /// #79: an image this node's registry has no golden of is asked of the
    /// cluster (the clone route is the one that starts the fetch); its 503 is
    /// the pull's failure, retried. A golden that turns ready meanwhile is
    /// used, and the clone the demand minted is deleted (#104).
    #[tokio::test]
    async fn an_image_not_on_this_node_is_asked_of_the_cluster() {
        let (url, reg) = fake_registry().await;
        let img = StormpumpImages::new(&url);
        let e = img.pull_image("quay.io/a/elsewhere:1").await.unwrap_err();
        assert!(matches!(e, CriError::ImagePull(_)), "{e}");
        let e = e.to_string();
        assert!(e.contains("503") && e.contains("fetching it from the cluster"), "{e}");

        assert_eq!(img.pull_image("quay.io/a/arrived:1").await.unwrap(), "template:sbr-arrived-1");
        let r = reg.lock().unwrap();
        assert_eq!(r.minted, 1, "the demand minted one, because the golden was ready by then");
        assert_eq!(r.deleted, vec!["c1".to_string()], "and it was deleted: the container makes its own root");
    }

    fn bare(id: &str, sandbox: &str) -> Container {
        Container {
            id: id.into(),
            sandbox_id: sandbox.into(),
            name: id.into(),
            namespace: "default".into(),
            pod: "p".into(),
            image: "i".into(),
            spec_handle: None,
            workload_handle: None,
            volume_handles: Vec::new(),
            root_handle: None,
            root_volume: None,
            provenance: None,
            resolved_at: None,
            identity: None,
            identity_file: None,
            log_dir: String::new(),
            mount_sources: Vec::new(),
            root_path: None,
            state: ContainerState::Exited,
            created_at: 0,
            started_at: 0,
            finished_at: 0,
            exit_code: 0,
            privileged: false,
            host_network: false,
            host_pid: false,
            removing: None,
        }
    }

    /// A removal the engine refuses is not dropped (#90): the record stays,
    /// hidden from the kubelet, and the runtime finishes it on the workload's
    /// exit, on the retry interval, or before its sandbox goes.
    /// #47: on stormpump a removed container's log stays where `kubectl logs`
    /// reads it, which is why an init container is removed as soon as it
    /// completes.
    #[tokio::test]
    async fn a_removed_containers_log_stays_readable() {
        let rt = rt();
        assert!(rt.logs_survive_removal());
        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("setup");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(log_dir.join("0.log"), "2026-10-08T00:00:00Z stdout F done\n").unwrap();
        let mut c = bare("ct-init", "sb-1");
        c.log_dir = log_dir.to_string_lossy().into_owned();
        rt.containers.lock().await.insert("ct-init".into(), c);
        rt.remove_container("ct-init").await.unwrap();
        assert!(rt.containers.lock().await.get("ct-init").is_none(), "the record is gone");
        assert!(log_dir.join("0.log").exists(), "the log is not");
    }

    #[tokio::test]
    async fn a_refused_removal_is_kept_hidden_and_finished_later() {
        let r = rt();
        let w = Handle(7);
        let mut c = bare("ct-1", "sb-1");
        c.workload_handle = Some(w);
        r.containers.lock().await.insert("ct-1".into(), c);
        r.containers.lock().await.insert("ct-2".into(), bare("ct-2", "sb-1"));

        // Refused (no engine here stands in for EBUSY right after a stop).
        assert!(r.remove_container("ct-1").await.is_err());
        assert!(r.containers.lock().await["ct-1"].removing.is_some());
        assert!(matches!(r.container_status("ct-1").await, Err(CriError::NotFound(_))));
        let listed: Vec<String> = r.list_containers(None).await.unwrap().into_iter().map(|c| c.id).collect();
        assert_eq!(listed, vec!["ct-2".to_string()]);

        // Due on its workload's exit, not on another's; not yet by time.
        let exit = |h| crate::stormpump_ring::Exited { handle: h, status: 0 };
        {
            let cs = r.containers.lock().await;
            assert_eq!(removals_due(&cs, &[exit(w)]), vec!["ct-1".to_string()]);
            assert!(removals_due(&cs, &[exit(Handle(8))]).is_empty());
            assert!(removals_due(&cs, &[]).is_empty());
        }
        // Due by time once the interval has passed since the last try.
        r.containers.lock().await.get_mut("ct-1").unwrap().removing =
            std::time::Instant::now().checked_sub(REMOVAL_RETRY);
        assert_eq!(removals_due(&*r.containers.lock().await, &[]), vec!["ct-1".to_string()]);

        // The engine has let go (the workload is released): the next exit
        // pass finishes the removal.
        r.containers.lock().await.get_mut("ct-1").unwrap().workload_handle = None;
        r.note_exits(Vec::new()).await;
        assert!(!r.containers.lock().await.contains_key("ct-1"));

        // A pending one in a sandbox being removed is tried first, so the
        // sandbox is not held by a record the kubelet no longer names.
        r.containers.lock().await.get_mut("ct-2").unwrap().removing = Some(std::time::Instant::now());
        r.sandboxes.lock().await.insert(
            "sb-1".into(),
            Sandbox {
                id: "sb-1".into(),
                handle: None,
                config: PodSandboxConfig::default(),
                state: PodSandboxState::Ready,
                created_at: 0,
                netns: None,
                ip: String::new(),
                made: None,
                identity_file: None,
                network_status: None,
            },
        );
        r.remove_pod_sandbox("sb-1").await.unwrap();
        assert!(r.containers.lock().await.is_empty());
    }
}
