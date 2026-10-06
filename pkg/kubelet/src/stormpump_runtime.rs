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
//! The fields are accepted here and recorded, so the day stormpump learns to
//! drop capabilities this file needs no rewriting — but nothing enforces them
//! today and this comment is the only honest place to say so.

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
    PodSandboxConfig, PodSandboxState, PodSandboxStatusInfo, PodSandboxSummary, RuntimeService,
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
    /// The image's volume, registered with the engine. `None` until started.
    root_handle: Option<Handle>,
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

    /// DEL, then give the sandbox back. `false` when DEL failed: the caller
    /// keeps the record, and the namespace with it.
    async fn unwind_network(&self, failed: &FailedNetwork) -> bool {
        if let Some(invoker) = &self.cni {
            let pod = cni::PodNetwork::new(
                &failed.id,
                &failed.netns,
                &failed.config.namespace,
                &failed.config.name,
                &failed.config.uid,
            );
            if let Err(e) = invoker.del(&pod).await {
                tracing::warn!(sandbox = %failed.id, pod = %failed.config.name,
                    "CNI DEL after a failed ADD did not succeed, retried before the next sandbox: {e}");
                return false;
            }
        }
        self.release_sandbox(&failed.id, failed.handle).await;
        true
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
        }
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
        self.containers.lock().await.remove(container_id);
        Ok(())
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
fn spec_for(config: &ContainerConfig, sandbox: &PodSandboxConfig) -> stormpump::spec::Spec {
    use stormpump::spec::{Logs, Root, Share, Spec};

    let mut argv: Vec<String> = config.command.clone();
    argv.extend(config.args.iter().cloned());
    // A spec with nothing to run is refused at define time (`EmptyArgv`), and
    // an image's entrypoint is not something this side knows. Falling back to a
    // shell would run the wrong thing silently; an empty argv fails loudly at
    // the moment the spec is defined, naming the container.

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
            match image_root(&config.image) {
                Some(root) => match resolve_in_image(&root, &first) {
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
        // the spec is defined once and can be spawned many times, and the
        // image a container runs is a property of the spawn. `Chroot` is
        // "enter the volume's mount view", which is what a container root is.
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
            // `m.propagation` has nowhere to go yet: the engine's mounts are
            // all private, so a Bidirectional one (a CSI node plugin's
            // /var/lib/kubelet) does not push its mounts back to the node.
            // The kubelet catches the result before a pod is given an empty
            // volume (`csi::is_mount_point`). stormpump#35 adds the field.
            .map(|m| stormpump::spec::Mount {
                dst: m.container_path.clone(),
                readonly: m.readonly,
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
        env: container_env(config),
        cwd: if config.working_dir.is_empty() {
            "/".to_string()
        } else {
            config.working_dir.clone()
        },
        share: Share {
            // A container asking for hostPID in a sandbox not built for it is
            // the mismatch every runtime rejects, so the sandbox's answer wins
            // and the container's is folded into it.
            pid: config.host_pid || sandbox.host_pid,
            ipc: config.host_ipc || sandbox.host_ipc,
            uts: false,
        },
        tty: config.tty,
        // `cpu_shares` (the CPU *request*) is not mapped onto `cpu_weight`
        // yet. Every stormpump workload is a sibling under one cgroup parent,
        // node services included, and upstream's conversion gives a pod
        // weights far below the engine's default of 100 (1 CPU → 39, no
        // request → 1, stormpump's filler weight). Upstream's pods compete
        // only inside `kubepods`. Which way this node goes is open on #57.
        limits: limits_for(config),
        ..Spec::default()
    }
}

/// A container's stats from the engine's block. `u64::MAX` is the engine's
/// "the kernel did not say", and becomes `None`, never a number.
fn stats_info(
    mut info: crate::cri::ContainerStatsInfo,
    st: &stormpump_abi::query::Stats,
) -> crate::cri::ContainerStatsInfo {
    let known = |v: u64| (v != stormpump_abi::query::UNKNOWN).then_some(v);
    info.cpu_usage_core_nanos = known(st.cpu_usage_usec).map(|us| us.saturating_mul(1000));
    info.memory_working_set_bytes = known(st.memory_current);
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
        let (handle, netns) = if config.host_network {
            (None, None)
        } else {
            let (h, pid) = self.on_ring(|r| r.sandbox_acquire(PROFILE_ISOLATED)).await?;
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
        if let (Some(invoker), Some(ns)) = (&self.cni, &netns) {
            match invoker.network_ready() {
                Ok(_) => {
                    let pod = cni::PodNetwork::new(
                        &id,
                        ns,
                        &config.namespace,
                        &config.name,
                        &config.uid,
                    );
                    match invoker.add(&pod).await {
                        Ok(result) => {
                            ip = result
                                .ips
                                .first()
                                .map(|i| i.address.split('/').next().unwrap_or("").to_string())
                                .unwrap_or_default();
                            tracing::info!(
                                sandbox = %id, pod = %config.name, %ip,
                                "CNI attached the pod network"
                            );
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

        let sb = Sandbox {
            id: id.clone(),
            handle,
            config: config.clone(),
            state: PodSandboxState::Ready,
            created_at: now_nanos(),
            netns,
            ip,
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
            let pod = cni::PodNetwork::new(
                sandbox_id,
                ns,
                &sb.config.namespace,
                &sb.config.name,
                &sb.config.uid,
            );
            invoker
                .del(&pod)
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
        if let Some(sb) = self.sandboxes.lock().await.get_mut(sandbox_id) {
            sb.state = PodSandboxState::NotReady;
            sb.netns = None;
            if released {
                sb.handle = None;
            }
        }
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
        self.sandboxes.lock().await.remove(sandbox_id);
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
        let encoded = spec_for(config, _sandbox_config).encode();
        let spec = self.on_ring(move |r| r.spec_define(encoded)).await?;

        let (namespace, pod) = {
            let sandboxes = self.sandboxes.lock().await;
            sandboxes
                .get(sandbox_id)
                .map(|sb| (sb.config.namespace.clone(), sb.config.name.clone()))
                .unwrap_or_default()
        };

        let id = self.mint_id("ct");
        let c = Container {
            id: id.clone(),
            sandbox_id: sandbox_id.to_string(),
            name: config.name.clone(),
            namespace,
            pod,
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
            root_handle: None,
            // The image ref is the mounted path, which is what pull_image
            // returned: `/pallets/<name>` for a golden, `/run/stormpump/
            // images/<volume>` for a pull (#103). A pod whose image was never
            // pulled has none, and start_container says so rather than
            // spawning onto nothing.
            root_path: image_root(&config.image)
                .map(|p| p.to_string_lossy().into_owned()),
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
            container = %id, name = %qualified, image = %config.image,
            "stormpump: container created"
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
            let path = c.root_path.clone().ok_or_else(|| {
                CriError::Runtime(format!(
                    "container {container_id} has no root — image {} was never pulled",
                    c.image
                ))
            })?;
            (spec, path, c.log_dir.clone(), c.sandbox_id.clone(), c.mount_sources.clone())
        };

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
                let root = r.volume_register(&path)?;
                held.push(root);
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
            image_ref: c.image.clone(),
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
                    let st = ring.query_stats(h).ok().flatten()?;
                    Some(stats_info(info, &st))
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
                image_ref: c.image.clone(),
                reason: String::new(),
                message: String::new(),
            })
            .collect())
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
    /// This node's stormblock, which attaches a clone as a block device.
    storage: String,
    /// Its client, with the engine's token (#66). `http` is for the registry,
    /// which must not be sent the engine's token.
    engine: crate::engine::EngineClient,
    /// Passed to the attach, because a volume is attached *somewhere*.
    node_name: String,
    http: reqwest::Client,
    /// The engine, for the one thing only the engine can do: mount.
    ring: Option<Arc<RingClient>>,
    /// image ref -> mountpoint, for images already pulled.
    ///
    /// A pull is expensive (fetch, unpack, seal, clone, attach, mount) and the
    /// kubelet pulls per container, so the second container of an image must
    /// not repeat it. Keyed on the ref as written: a tag that moves is a
    /// different image, but resolving that on every start would mean a
    /// registry round trip per container start, and `imagePullPolicy` is what
    /// exists to ask for it.
    pulled: Mutex<HashMap<String, String>>,
    /// image ref -> registry clone id, for a pulled image whose bind the
    /// registry refused (#143). The image is mounted and used; the bind is
    /// asked again on the next pull of it, because a clone left `claimed` is
    /// reaped by the registry after its grace period.
    unbound: Mutex<HashMap<String, String>>,
    /// One pull of an image at a time (#143): two concurrent pulls would mint
    /// two clones, and a bound clone is never reaped, so the loser would leak.
    pulling: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

/// The container's environment: what the pod asked for, plus the defaults an
/// OCI runtime would have taken from the image config.
///
/// **A golden has no image config**, so `HOME`, `PATH` and `HOSTNAME` — which
/// every other runtime derives from the image or the pod — arrive unset unless
/// something puts them there. Real programs assume them: Cilium's operator got
/// as far as starting its hive and then died on
/// `unable to get current user home directory: os/user lookup failed; $HOME is
/// empty`, which is a missing environment variable wearing the costume of a
/// user-lookup failure.
///
/// The pod always wins. These are defaults, not overrides: a spec that sets
/// `HOME` means it, and this must never quietly replace it.
fn container_env(config: &ContainerConfig) -> Vec<String> {
    let mut env: Vec<String> = config
        .envs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();

    let has = |env: &Vec<String>, key: &str| {
        let prefix = format!("{key}=");
        env.iter().any(|e| e.starts_with(&prefix))
    };

    // HOME=/root because a container without a user database runs as root,
    // and that is the home an image would declare for it.
    if !has(&env, "HOME") {
        env.push("HOME=/root".to_string());
    }
    if !has(&env, "PATH") {
        env.push(
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
        );
    }
    // Upstream's kubelet sets HOSTNAME to the pod name, and plenty of software
    // reads it rather than calling uname.
    if !has(&env, "HOSTNAME") && !config.name.is_empty() {
        env.push(format!("HOSTNAME={}", config.name));
    }
    env
}

/// Find `argv0` on the standard PATH *inside* an image root.
///
/// Returns the path as the container will see it (absolute, image-relative),
/// not the host path — the container is chrooted into the image, so
/// `/pallets/cilium/usr/bin/cilium-agent` on this side is `/usr/bin/cilium-agent`
/// on that one.
///
/// The directory list is the conventional PATH, in the conventional order. An
/// image that puts its binary somewhere else and relies on an `ENV PATH` is not
/// handled: a golden is a filesystem and carries no image config, so there is
/// no PATH to read. That case is a miss, and a miss is reported rather than
/// guessed at.
fn resolve_in_image(root: &std::path::Path, argv0: &str) -> Option<String> {
    // A command with a slash in it is a path already, just not an absolute
    // one — `./foo` or `bin/foo`. PATH is not consulted for those, the same as
    // a shell.
    if argv0.contains('/') {
        return None;
    }
    for dir in [
        "/usr/local/sbin",
        "/usr/local/bin",
        "/usr/sbin",
        "/usr/bin",
        "/sbin",
        "/bin",
    ] {
        let candidate = root.join(dir.trim_start_matches('/')).join(argv0);
        if candidate.is_file() {
            return Some(format!("{dir}/{argv0}"));
        }
    }
    None
}

/// Where a pulled image is mounted. Under `/run` because it does not survive a
/// reboot: the clone does, and is found again by name.
const IMAGE_ROOT: &str = "/run/stormpump/images";
/// Where the engine mounts a claim's block device for its pods to bind.
const PVC_ROOT: &str = "/run/stormpump/pvc";

/// The root a container of `image` runs on, or `None` when this node has none.
///
/// `image` is what the kubelet hands `create_container`: the reference
/// [`StormpumpImages::pull_image`] returned, which is the mounted path. A
/// golden's is `/pallets/<name>`; a pull's is `/run/stormpump/images/<volume>`,
/// and that one is taken as given (#103). Mapping it through
/// [`StormpumpImages::local_path`] as well made every pulled image look for
/// `/pallets/<volume>`, and fail at start as "never pulled".
///
/// The pulled path is not checked for content from here: PID 1 mounted it in
/// the node's namespace, which is where the engine resolves the root at spawn,
/// and this process may not see that mount. A path that is not mounted fails
/// there, with the engine's own error.
/// Where [`StormpumpImages::pull_image`] mounts a pulled clone, and so the
/// reference it returns for it.
fn pulled_mount(volume: &str) -> String {
    format!("{IMAGE_ROOT}/{volume}")
}

fn image_root(image: &str) -> Option<std::path::PathBuf> {
    image_root_in(image, std::path::Path::new(PALLET_ROOT))
}

fn image_root_in(image: &str, pallets: &std::path::Path) -> Option<std::path::PathBuf> {
    if let Some(rest) = image.strip_prefix(IMAGE_ROOT) {
        // Exactly one component below the image root, which is what a pull
        // makes: nothing above it, nothing beside it.
        let volume = rest.strip_prefix('/')?;
        let plain = !volume.is_empty() && !volume.contains('/') && volume != "." && volume != "..";
        return plain.then(|| std::path::PathBuf::from(image));
    }
    StormpumpImages::local_path_in(image, pallets)
}

impl StormpumpImages {
    pub fn new(registry: impl Into<String>) -> StormpumpImages {
        StormpumpImages {
            registry: registry.into(),
            storage: crate::engine::DEFAULT_URL.to_string(),
            engine: crate::engine::EngineClient::default(),
            node_name: String::new(),
            http: reqwest::Client::new(),
            ring: None,
            pulled: Mutex::new(HashMap::new()),
            unbound: Mutex::new(HashMap::new()),
            pulling: Mutex::new(HashMap::new()),
        }
    }

    /// The engine and the node identity, without which a pull can get as far
    /// as a clone and no further.
    pub fn with_engine(
        mut self,
        ring: Option<Arc<RingClient>>,
        node_name: impl Into<String>,
    ) -> StormpumpImages {
        self.ring = ring;
        self.node_name = node_name.into();
        self
    }

    /// The node's engine, shared with the rest of the kubelet.
    pub fn with_storage(mut self, engine: crate::engine::EngineClient) -> StormpumpImages {
        self.storage = engine.url().to_string();
        self.engine = engine;
        self
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
    /// POST JSON and read JSON back, or say why not.
    ///
    /// A non-2xx carries the body: the registry and the engine both explain
    /// themselves in it, and "HTTP 409" on its own has sent people to the
    /// wrong component more than once.
    async fn post(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let resp =
            self.http.post(url).json(body).send().await.map_err(|e| format!("{url}: {e}"))?;
        Self::json_answer(url, resp).await
    }

    /// [`Self::post`] to the engine, with its token.
    async fn post_engine(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let resp = self.engine.post(url, body).await.map_err(|e| format!("{url}: {e}"))?;
        Self::json_answer(url, resp).await
    }

    async fn json_answer(url: &str, resp: reqwest::Response) -> Result<serde_json::Value, String> {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{url}: {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|e| format!("{url}: not JSON: {e}: {text}"))
    }

    /// The mount of an image this kubelet pulled, with its bind asked again
    /// first if the registry refused it before (#143).
    async fn pulled_path(&self, image: &str) -> Option<String> {
        let path = self.pulled.lock().await.get(image).cloned()?;
        let pending = self.unbound.lock().await.get(image).cloned();
        if let Some(id) = pending {
            self.bind_or_note(image, &id).await;
        }
        Some(path)
    }

    /// What the registry records as holding a pulled image's clone (#143):
    /// this kubelet, for this image. One clone per image per node, shared by
    /// every container of it, so the image is the holder, not a container.
    fn consumer(&self, image: &str) -> String {
        format!("kubelet/{}/{image}", self.node_name)
    }

    /// The clone already bound to `consumer`, found by asking the registry
    /// (#143, sbregistry#19). This is how a pull after a kubelet restart (or a
    /// reboot) gets the clone it bound before instead of minting another: a
    /// bound clone is never reaped, so minting again would leak one per
    /// restart. An error is an error, not "none": minting on a failed lookup
    /// is the same leak.
    async fn bound_clone(&self, consumer: &str) -> Result<Option<serde_json::Value>, String> {
        let url = format!("{}/v1/clones", self.registry);
        let resp = self
            .http
            .get(&url)
            .query(&[("consumer", consumer), ("state", "bound")])
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let list = Self::json_answer(&url, resp).await?;
        let list = list.as_array().ok_or_else(|| format!("{url}: not a list: {list}"))?;
        // The registry filters; checked here as well, because an older
        // registry that ignores the query would hand back every clone.
        Ok(list
            .iter()
            .find(|c| {
                c["consumer"].as_str() == Some(consumer)
                    && c["state"].as_str() == Some("bound")
                    && c["id"].is_string()
                    && c["volume_name"].is_string()
            })
            .cloned())
    }

    /// Tell the registry what holds clone `id` (#143). Until this, the clone
    /// is `claimed`, and the registry reaps a claim older than its grace
    /// period (900 s) as abandoned, under the mounted image.
    async fn bind(&self, id: &str, consumer: &str) -> Result<(), String> {
        let url = format!("{}/v1/clones/{id}/bind", self.registry);
        let rec = self.post(&url, &serde_json::json!({ "consumer": consumer })).await?;
        if rec["state"].as_str() != Some("bound") {
            return Err(format!("{url}: the clone is {} after the bind", rec["state"]));
        }
        Ok(())
    }

    /// The registry half of a pull: the clone this node already bound for
    /// `image`, else a new one. Returns (clone id, volume name, bound).
    async fn clone_for(&self, image: &str) -> Result<(String, String, bool), CriError> {
        let consumer = self.consumer(image);
        let found = self.bound_clone(&consumer).await.map_err(|e| {
            CriError::ImagePull(format!("registry could not say which clone holds {image}: {e}"))
        })?;
        let (clone, bound) = match found {
            Some(c) => (c, true),
            None => {
                // `remote_image` is what lets the registry build the golden
                // on demand when it has never seen this image.
                let body = serde_json::json!({ "golden": image, "remote_image": image });
                let c = self
                    .post(&format!("{}/v1/clones", self.registry), &body)
                    .await
                    .map_err(|e| {
                        CriError::ImagePull(format!("registry could not clone {image}: {e}"))
                    })?;
                (c, false)
            }
        };
        let field = |k: &str| {
            clone[k].as_str().map(str::to_owned).ok_or_else(|| {
                CriError::ImagePull(format!("registry returned no clone {k} for {image}: {clone}"))
            })
        };
        Ok((field("id")?, field("volume_name")?, bound))
    }

    /// Bind `id` for `image`, or note it for the next pull (#143). A refused
    /// bind does not fail the pull: the image is mounted, and pulling again
    /// would mint a second clone while the first one is in use.
    async fn bind_or_note(&self, image: &str, id: &str) {
        match self.bind(id, &self.consumer(image)).await {
            Ok(()) => {
                self.unbound.lock().await.remove(image);
            }
            Err(e) => {
                tracing::warn!(
                    image = %image, clone = %id,
                    "registry would not bind the image's clone (retried on the next pull; \
                     an unbound clone is reaped after the registry's grace period): {e}"
                );
                self.unbound.lock().await.insert(image.to_string(), id.to_string());
            }
        }
    }

    /// A volume's id by name, from this node's stormblock.
    async fn volume_id(&self, name: &str) -> Option<String> {
        let resp = self.engine.get(&format!("{}/api/v1/volumes", self.storage)).await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let list: serde_json::Value = resp.json().await.ok()?;
        let v = list["items"].as_array()?.iter().find(|v| v["name"].as_str() == Some(name))?;
        Some(v["id"].as_str()?.to_string())
    }

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
    /// Make an image available on this node, as a path a container can be
    /// rooted at.
    ///
    /// Three cases, in order of cost:
    ///
    /// 1. **A golden.** The image shipped in a pallet and is already mounted.
    ///    Free, and the case every standard component takes.
    /// 2. **Already pulled.** A previous container of this image did the work.
    /// 3. **A pull.** The registry turns the image into a sealed golden
    ///    volume, mints a copy-on-write clone of it, stormblock attaches the
    ///    clone as a block device, and the engine mounts it. Then the clone
    ///    is bound to `kubelet/<node>/<image>` (#143): a clone left `claimed`
    ///    is reaped by the registry after its grace period, under the mount.
    ///    A pull after a kubelet restart asks for that bound clone first and
    ///    reattaches it rather than minting another. Nothing releases a
    ///    pulled image yet: that needs an image GC (#161).
    ///
    /// Each step is somebody else's job and is idempotent, which is what makes
    /// a half-finished pull safe to retry: the registry reuses a sealed
    /// template for a digest it already has, the attach returns the device it
    /// already made, and the mount treats "already mounted there" as success.
    ///
    /// **One clone per image, not per container.** Containers of the same
    /// image share the mount, which is what goldens already do — `/pallets/
    /// busybox` is one clone however many pods name busybox. A writable layer
    /// per container is the next step and a real one; until then an image
    /// whose containers write to their own root will have them write to each
    /// other's.
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        if let Some(path) = Self::local_path(image) {
            tracing::info!(image = %image, path = %path.display(), "image is a golden on this node");
            return Ok(path.to_string_lossy().into_owned());
        }
        if let Some(path) = self.pulled_path(image).await {
            return Ok(path);
        }
        // One pull of this image at a time; the one that waited finds the
        // other's result.
        let gate = self.pulling.lock().await.entry(image.to_string()).or_default().clone();
        let _gate = gate.lock().await;
        if let Some(path) = self.pulled_path(image).await {
            return Ok(path);
        }

        // The engine is required, not optional: without it the pull can reach
        // a clone and an attached device and then have nowhere to put it.
        // Saying so here beats a mount that silently went nowhere.
        let ring = self.ring.as_ref().ok_or_else(|| {
            CriError::ImagePull(format!(
                "{image} is not a golden on this node and cannot be pulled: the kubelet \
                 has no ring to stormpump, and only the engine can mount a volume"
            ))
        })?;

        // 1. The registry turns a reference into a sealed golden and hands
        //    back a clone of it, or the one this node bound before (#143).
        let (clone_id, volume, bound) = self.clone_for(image).await?;
        let volume = volume.as_str();

        // 2. Attach the clone here, as a block device.
        let vol_id = self.volume_id(volume).await.ok_or_else(|| {
            CriError::ImagePull(format!("stormblock has no volume {volume} for {image}"))
        })?;
        let attach = serde_json::json!({ "node": self.node_name, "transport": "ublk" });
        let info: serde_json::Value = self
            .post_engine(&format!("{}/api/v1/volumes/{vol_id}/attach", self.storage), &attach)
            .await
            .map_err(|e| CriError::ImagePull(format!("could not attach {volume}: {e}")))?;
        let device = info["device_hint"].as_str().ok_or_else(|| {
            CriError::ImagePull(format!(
                "{volume} did not attach locally: {info} — an NVMe-oF attach needs a connect \
                 this node does not do yet"
            ))
        })?;

        // 3. The engine mounts it, in the node's mount namespace rather than
        //    this container's.
        let mount = pulled_mount(volume);
        ring.volume_register_device(&mount, device, "ext4").map_err(|e| {
            CriError::ImagePull(format!("stormpump would not mount {device} at {mount}: {e}"))
        })?;

        // 4. Say what holds it, now that something does (#143).
        if !bound {
            self.bind_or_note(image, &clone_id).await;
        }

        tracing::info!(image = %image, %device, %mount, clone = %clone_id, found_again = bound, "pulled");
        self.pulled.lock().await.insert(image.to_string(), mount.clone());
        Ok(mount)
    }

    /// Whether the image is on this node — as a golden, or already pulled.
    ///
    /// A pulled image has to count, or `imagePullPolicy: IfNotPresent` pulls
    /// every time and the cache above never gets consulted.
    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
        let path = match Self::local_path(image) {
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

#[cfg(test)]
mod tests {

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
        let spec = spec_for(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.limits.memory_max, Some(512 * 1024 * 1024));
        assert_eq!(spec.limits.swap_max, Some(0));
        assert_eq!(
            spec.limits.cpu_max,
            Some(stormpump::spec::CpuMax { quota_us: 50_000, period_us: 100_000 })
        );
        assert_eq!(spec.limits.pids_max, None);
        // The request is not a weight yet (open on #57).
        assert_eq!(spec.cpu_weight, stormpump::spec::Spec::default().cpu_weight);

        // What the engine receives is what was set: through the wire and back.
        let back = stormpump::spec::Spec::decode(&spec.encode()).unwrap();
        assert_eq!(back.limits, spec.limits);
    }

    #[test]
    fn a_container_without_limits_declares_none() {
        let cc = ContainerConfig {
            name: "app".into(),
            command: vec!["/bin/app".into()],
            cpu_period: 100_000,
            ..Default::default()
        };
        assert!(spec_for(&cc, &PodSandboxConfig::default()).limits.is_empty());
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
        let info = stats_info(crate::cri::ContainerStatsInfo::default(), &st);
        assert_eq!(info.cpu_usage_core_nanos, Some(2_500_000_000));
        assert_eq!(info.memory_working_set_bytes, Some(4096));

        let unknown = stats_info(crate::cri::ContainerStatsInfo::default(), &Default::default());
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
        let spec = spec_for(&cc, &sandbox);
        assert_eq!(spec.profile, Profile::Host);

        // And an ordinary pod is routed: east-west plus a default route.
        let spec = spec_for(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.profile, Profile::Routed);
    }

    #[test]
    fn the_sandbox_decides_the_namespaces() {
        // A container asking for hostPID in a sandbox not built for it is the
        // mismatch every runtime rejects, because the sandbox's namespaces
        // already exist by the time the container is created. So the two are
        // folded together rather than allowed to disagree.
        let sandbox = PodSandboxConfig { host_pid: true, ..Default::default() };
        let cc = ContainerConfig { name: "c".into(), ..Default::default() };
        assert!(spec_for(&cc, &sandbox).share.pid);

        let cc = ContainerConfig { host_pid: true, ..Default::default() };
        assert!(spec_for(&cc, &PodSandboxConfig::default()).share.pid);
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
        let spec = spec_for(&cc, &PodSandboxConfig::default());
        assert_eq!(spec.mounts.len(), 2);
        assert_eq!(spec.mounts[0].dst, "/data");
        assert!(!spec.mounts[0].readonly);
        assert_eq!(spec.mounts[1].dst, "/cfg");
        assert!(spec.mounts[1].readonly, "a read-only mount stays read-only");

        // And the sources this records are in that same order.
        let sources: Vec<String> =
            cc.mounts.iter().take(MAX_MOUNTS).map(|m| m.host_path.clone()).collect();
        assert_eq!(sources, vec!["/host/a".to_string(), "/host/b".to_string()]);
    }

    #[test]
    /// A golden carries no image config, so HOME and PATH arrive unset unless
    /// the runtime supplies them. Cilium's operator died on an empty $HOME
    /// after getting all the way to starting its hive.
    #[test]
    fn the_container_gets_the_environment_an_image_would_have_given_it() {
        let cfg = ContainerConfig { name: "cilium-operator".into(), ..Default::default() };
        let env = container_env(&cfg);
        assert!(env.iter().any(|e| e == "HOME=/root"), "{env:?}");
        assert!(env.iter().any(|e| e.starts_with("PATH=/usr/local/sbin:")), "{env:?}");
        assert!(env.iter().any(|e| e == "HOSTNAME=cilium-operator"), "{env:?}");
    }

    /// Defaults, not overrides — a pod that sets HOME means it.
    #[test]
    fn the_pod_environment_wins_over_the_defaults() {
        let cfg = ContainerConfig {
            name: "c".into(),
            envs: vec![
                ("HOME".to_string(), "/home/app".to_string()),
                ("PATH".to_string(), "/opt/bin".to_string()),
            ],
            ..Default::default()
        };
        let env = container_env(&cfg);
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
    fn a_pulled_image_resolves_to_its_mount() {
        let pallets = tempfile::tempdir().unwrap();
        let pulled = pulled_mount("clone-test-stormcos-qa-short-bf7012f9fe4b-18dad12e8a816346");
        assert_eq!(
            image_root_in(&pulled, pallets.path()),
            Some(std::path::PathBuf::from(&pulled))
        );
        // The same reference again (a retry, or the second container of the
        // image) resolves the same way: nothing is remembered between calls.
        assert_eq!(image_root(&pulled), Some(std::path::PathBuf::from(&pulled)));
        // Only a single volume directly below the image root.
        for bad in [
            IMAGE_ROOT.to_string(),
            format!("{IMAGE_ROOT}/"),
            format!("{IMAGE_ROOT}/.."),
            format!("{IMAGE_ROOT}/a/b"),
            format!("{IMAGE_ROOT}x/v"),
        ] {
            assert_eq!(image_root_in(&bad, pallets.path()), None, "{bad}");
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
            resolve_in_image(&root, "cilium-agent"),
            Some("/usr/bin/cilium-agent".to_string())
        );
        // Not there at all.
        assert_eq!(resolve_in_image(&root, "nonesuch"), None);
        // A command containing a slash is a path, not a PATH lookup — same as
        // a shell.
        assert_eq!(resolve_in_image(&root, "./cilium-agent"), None);
        assert_eq!(resolve_in_image(&root, "usr/bin/cilium-agent"), None);

        // Order matters: /usr/local/bin wins over /usr/bin.
        let local = root.join("usr/local/bin");
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("cilium-agent"), b"x").unwrap();
        assert_eq!(
            resolve_in_image(&root, "cilium-agent"),
            Some("/usr/local/bin/cilium-agent".to_string())
        );

        // A directory of the right name is not a command.
        std::fs::create_dir_all(root.join("usr/bin/adir")).unwrap();
        assert_eq!(resolve_in_image(&root, "adir"), None);

        std::fs::remove_dir_all(&root).ok();
    }

    fn command_and_args_become_one_argv() {
        let cc = ContainerConfig {
            command: vec!["/usr/bin/cilium-agent".into()],
            args: vec!["--config-dir".into(), "/tmp/cilium".into()],
            ..Default::default()
        };
        let spec = spec_for(&cc, &PodSandboxConfig::default());
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
                .post(|State(s): State<S>| async move {
                    let mut r = s.lock().unwrap();
                    r.minted += 1;
                    let n = r.minted;
                    let c = serde_json::json!({
                        "id": format!("c{n}"), "volume_name": format!("img-clone-{n}"),
                        "state": "claimed",
                    });
                    r.clones.push(c.clone());
                    Json(c)
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
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, state)
    }

    /// A pull's clone is bound to this node and image, and a kubelet that
    /// restarts finds it again instead of minting a second one (#143).
    #[tokio::test]
    async fn a_pulled_clone_is_bound_and_found_again_after_a_restart() {
        let (url, reg) = fake_registry().await;
        let img = StormpumpImages::new(&url).with_engine(None, "node1");
        let (id, volume, bound) = img.clone_for("quay.io/a/b:1").await.unwrap();
        assert_eq!((id.as_str(), volume.as_str(), bound), ("c1", "img-clone-1", false));
        img.bind_or_note("quay.io/a/b:1", &id).await;
        {
            let r = reg.lock().unwrap();
            assert_eq!(r.clones[0]["state"], "bound");
            assert_eq!(r.clones[0]["consumer"], "kubelet/node1/quay.io/a/b:1");
        }
        assert!(img.unbound.lock().await.is_empty());

        // A new kubelet (restart): same clone, already bound, nothing minted.
        let again = StormpumpImages::new(&url).with_engine(None, "node1");
        assert_eq!(
            again.clone_for("quay.io/a/b:1").await.unwrap(),
            ("c1".to_string(), "img-clone-1".to_string(), true)
        );
        // Another image, or the same image on another node, is its own clone.
        assert_eq!(again.clone_for("quay.io/a/b:2").await.unwrap().0, "c2");
        let other = StormpumpImages::new(&url).with_engine(None, "node2");
        assert_eq!(other.clone_for("quay.io/a/b:1").await.unwrap().0, "c3");
        assert_eq!(reg.lock().unwrap().minted, 3);
    }

    /// A refused bind leaves the image usable and is asked again on the next
    /// pull of it (#143).
    #[tokio::test]
    async fn a_refused_bind_is_retried_on_the_next_pull() {
        let (url, reg) = fake_registry().await;
        let img = StormpumpImages::new(&url).with_engine(None, "node1");
        let (id, volume, _) = img.clone_for("busybox:1").await.unwrap();
        reg.lock().unwrap().refuse_bind = true;
        img.bind_or_note("busybox:1", &id).await;
        assert_eq!(img.unbound.lock().await.get("busybox:1"), Some(&id));
        assert_eq!(reg.lock().unwrap().clones[0]["state"], "claimed");
        img.pulled.lock().await.insert("busybox:1".into(), pulled_mount(&volume));

        // Still refused: the path is answered, the bind stays pending.
        assert_eq!(img.pull_image("busybox:1").await.unwrap(), pulled_mount(&volume));
        assert!(img.unbound.lock().await.contains_key("busybox:1"));

        reg.lock().unwrap().refuse_bind = false;
        assert_eq!(img.pull_image("busybox:1").await.unwrap(), pulled_mount(&volume));
        assert!(img.unbound.lock().await.is_empty());
        let r = reg.lock().unwrap();
        assert_eq!(r.clones[0]["state"], "bound");
        assert_eq!(r.minted, 1);
    }

    /// A registry that cannot say which clone holds an image is a failed
    /// pull, never a second mint (#143).
    #[tokio::test]
    async fn a_failed_lookup_mints_nothing() {
        let (url, reg) = fake_registry().await;
        reg.lock().unwrap().refuse_list = true;
        let img = StormpumpImages::new(&url).with_engine(None, "node1");
        let e = img.clone_for("busybox:1").await.unwrap_err();
        assert!(e.to_string().contains("which clone holds busybox:1"), "{e}");
        assert_eq!(reg.lock().unwrap().minted, 0);
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
            },
        );
        r.remove_pod_sandbox("sb-1").await.unwrap();
        assert!(r.containers.lock().await.is_empty());
    }
}
