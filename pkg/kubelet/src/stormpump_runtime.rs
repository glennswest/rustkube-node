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
    /// The image's volume, registered with the engine. `None` until started.
    root_handle: Option<Handle>,
    /// The directory this container's log file is opened in:
    /// `<sandbox log_directory>/<container>`. Kubernetes reads
    /// `<that>/<restart>.log` and nowhere else.
    log_dir: String,
    /// The host paths for this container's mounts, in the order the spec
    /// declares their destinations. Registered as volumes at spawn and paired
    /// with those destinations by position.
    mount_sources: Vec<String>,
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
}

/// One pod sandbox.
struct Sandbox {
    id: String,
    handle: Option<Handle>,
    config: PodSandboxConfig,
    state: PodSandboxState,
    created_at: i64,
}

/// The kubelet's view of stormpump.
pub struct StormpumpRuntime {
    socket: String,
    /// The ring, once attached. `None` on a node where stormpump is not PID 1,
    /// which is every development box — the runtime is constructible there so
    /// its bookkeeping can be tested, and every operation that needs the engine
    /// says so rather than pretending.
    ring: Option<Arc<RingClient>>,
    sandboxes: Mutex<HashMap<String, Sandbox>>,
    containers: Mutex<HashMap<String, Container>>,
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
            sandboxes: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub fn new(socket: impl Into<String>) -> StormpumpRuntime {
        StormpumpRuntime {
            socket: socket.into(),
            ring: None,
            sandboxes: Mutex::new(HashMap::new()),
            containers: Mutex::new(HashMap::new()),
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
        if exits.is_empty() {
            return;
        }
        let mut containers = self.containers.lock().await;
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
            match StormpumpImages::local_path(&config.image) {
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
            .map(|m| stormpump::spec::Mount {
                dst: m.container_path.clone(),
                readonly: m.readonly,
                // A PersistentVolumeClaim resolves to a block device, and the
                // container's own child mounts it — which is why a real volume
                // needs no mount propagation and no host-side mount at all.
                fstype: m.fstype.clone(),
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
        ..Spec::default()
    }
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
        let handle = if config.host_network {
            None
        } else {
            Some(self.on_ring(|r| r.sandbox_acquire(PROFILE_ISOLATED)).await?)
        };

        let sb = Sandbox {
            id: id.clone(),
            handle,
            config: config.clone(),
            state: PodSandboxState::Ready,
            created_at: now_nanos(),
        };
        self.sandboxes.lock().await.insert(id.clone(), sb);
        tracing::info!(
            sandbox = %id, pod = %config.name, ns = %config.namespace,
            host_network = config.host_network, host_pid = config.host_pid,
            "stormpump: pod sandbox created"
        );
        Ok(id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        let mut sandboxes = self.sandboxes.lock().await;
        if let Some(sb) = sandboxes.get_mut(sandbox_id) {
            sb.state = PodSandboxState::NotReady;
        }
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> Result<(), CriError> {
        let handle = self
            .sandboxes
            .lock()
            .await
            .remove(sandbox_id)
            .and_then(|sb| sb.handle);
        if let Some(h) = handle {
            // Back to the pool, which is what makes the next start cheap. The
            // engine defers it until the last container has left, so this may
            // be called while they are still stopping.
            let _ = self.on_ring(move |r| r.sandbox_release(h)).await;
        }
        self.forget_containers_of(sandbox_id).await;
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
            // A host-network pod has the node's address; anything else needs
            // the CNI to have run, and reporting an address we have not been
            // given would make a pod look reachable when it is not.
            ip: String::new(),
            additional_ips: Vec::new(),
            netns_path: None,
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
                .map(|m| m.host_path.clone())
                .collect(),
            log_dir: format!(
                "{}/{}",
                _sandbox_config.log_directory.trim_end_matches('/'),
                config.name
            ),
            image: config.image.clone(),
            spec_handle: Some(spec),
            workload_handle: None,
            root_handle: None,
            // The image ref is the mounted path, which is what pull_image
            // returned. A pod whose image was never pulled has none, and
            // start_container says so rather than spawning onto nothing.
            root_path: StormpumpImages::local_path(&config.image)
                .map(|p| p.to_string_lossy().into_owned()),
            state: ContainerState::Created,
            created_at: now_nanos(),
            started_at: 0,
            finished_at: 0,
            exit_code: 0,
            privileged: config.privileged,
            host_network: config.host_network,
            host_pid: config.host_pid,
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
        let _ = std::fs::create_dir_all(&log_dir);

        let workload = self
            .on_ring(move |r| {
                let root = r.volume_register(&path)?;
                let logs = r.volume_register(&log_dir)?;
                // One per mount point, in the spec's order.
                let mut mounts = Vec::with_capacity(mount_sources.len());
                for src in &mount_sources {
                    mounts.push(r.volume_register(src)?);
                }
                // The mount *sources* by name, not only their count. A start
                // that fails at the mount step is otherwise a step with no
                // subject: "attaching mounts, ENOENT" does not say which of
                // them, and the destinations are inside the spec where this
                // log cannot see them.
                tracing::debug!(
                    ?spec, ?root, ?logs, ?sandbox, path = %path, logs_dir = %log_dir,
                    mounts = %mount_sources.join(","),
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
                                    .and_then(|i| mount_sources.get(i).map(|s| (i, s.clone())))
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
            })
            .await?;


        let mut containers = self.containers.lock().await;
        let c = containers
            .get_mut(container_id)
            .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
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
            self.on_ring(move |r| r.stop(w, grace)).await?;
        }
        let mut containers = self.containers.lock().await;
        if let Some(c) = containers.get_mut(container_id) {
            c.state = ContainerState::Exited;
            c.finished_at = now_nanos();
        }
        Ok(())
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), CriError> {
        let workload = self
            .containers
            .lock()
            .await
            .remove(container_id)
            .and_then(|c| c.workload_handle);
        if let Some(w) = workload {
            // Frees the pidfd and the cgroup, which the process dying does not.
            // Refused while it is still running, so this follows a stop.
            let _ = self
                .on_ring(move |r| r.workload_release(w))
                .await;
        }
        Ok(())
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> Result<ContainerStatusInfo, CriError> {
        self.absorb_exits().await;
        let containers = self.containers.lock().await;
        let c = containers
            .get(container_id)
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

    async fn list_containers(
        &self,
        sandbox_id: Option<&str>,
    ) -> Result<Vec<ContainerStatusInfo>, CriError> {
        self.absorb_exits().await;
        let containers = self.containers.lock().await;
        Ok(containers
            .values()
            .filter(|c| sandbox_id.is_none_or(|s| c.sandbox_id == s))
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

impl StormpumpImages {
    pub fn new(registry: impl Into<String>) -> StormpumpImages {
        StormpumpImages {
            registry: registry.into(),
            storage: DEFAULT_STORAGE_URL.to_string(),
            node_name: String::new(),
            http: reqwest::Client::new(),
            ring: None,
            pulled: Mutex::new(HashMap::new()),
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

    pub fn with_storage(mut self, storage: impl Into<String>) -> StormpumpImages {
        self.storage = storage.into();
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

/// This node's stormblock. The engine is local by construction: a volume is
/// attached to the node that will use it.
const DEFAULT_STORAGE_URL: &str = "http://127.0.0.1:9090";

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
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{url}: {status}: {text}"));
        }
        serde_json::from_str(&text).map_err(|e| format!("{url}: not JSON: {e}: {text}"))
    }

    /// A volume's id by name, from this node's stormblock.
    async fn volume_id(&self, name: &str) -> Option<String> {
        let resp =
            self.http.get(format!("{}/api/v1/volumes", self.storage)).send().await.ok()?;
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
        let last = image.rsplit('/').next().unwrap_or(image);
        let name = last.split(['@', ':']).next().unwrap_or(last);
        if name.is_empty() {
            return None;
        }
        let p = std::path::Path::new(PALLET_ROOT).join(name);
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
    ///    clone as a block device, and the engine mounts it.
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
        if let Some(path) = self.pulled.lock().await.get(image) {
            return Ok(path.clone());
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
        //    back a clone of it. `remote_image` is what lets it build the
        //    golden on demand when it has never seen this image.
        let body = serde_json::json!({ "golden": image, "remote_image": image });
        let clone: serde_json::Value = self
            .post(&format!("{}/v1/clones", self.registry), &body)
            .await
            .map_err(|e| CriError::ImagePull(format!("registry could not clone {image}: {e}")))?;
        let volume = clone["volume_name"].as_str().ok_or_else(|| {
            CriError::ImagePull(format!("registry returned no volume for {image}: {clone}"))
        })?;

        // 2. Attach the clone here, as a block device.
        let vol_id = self.volume_id(volume).await.ok_or_else(|| {
            CriError::ImagePull(format!("stormblock has no volume {volume} for {image}"))
        })?;
        let attach = serde_json::json!({ "node": self.node_name, "transport": "ublk" });
        let info: serde_json::Value = self
            .post(&format!("{}/api/v1/volumes/{vol_id}/attach", self.storage), &attach)
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
        let mount = format!("{IMAGE_ROOT}/{volume}");
        ring.volume_register_device(&mount, device, "ext4").map_err(|e| {
            CriError::ImagePull(format!("stormpump would not mount {device} at {mount}: {e}"))
        })?;

        tracing::info!(image = %image, %device, %mount, "pulled");
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
    use super::*;

    fn rt() -> StormpumpRuntime {
        StormpumpRuntime::new("/nonexistent/stormpump.sock")
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
    async fn removing_a_sandbox_takes_its_containers_with_it() {
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
                },
            );
        }

        r.forget_containers_of("sb-1").await;

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
}
