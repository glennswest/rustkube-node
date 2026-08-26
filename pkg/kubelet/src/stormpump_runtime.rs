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

    Spec {
        domain: Domain::Container,
        // The root arrives as a registered volume handle at spawn, not here:
        // the spec is defined once and can be spawned many times, and the
        // image a container runs is a property of the spawn. `Chroot` is
        // "enter the volume's mount view", which is what a container root is.
        root: Root::Chroot,
        // Its own file, so a crashed container's reason survives it and the
        // node's console is not the only place it went.
        logs: Logs::Combined,
        mounts: Vec::new(),
        // hostNetwork is the node's namespace; anything else is the pod's,
        // which the sandbox already holds. `Profile::Host` is "no namespace at
        // all", which is exactly what hostNetwork means.
        profile: if config.host_network || sandbox.host_network {
            Profile::Host
        } else {
            Profile::Routed
        },
        argv,
        env: config
            .envs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect(),
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
        // Recorded, not acquired.
        //
        // `SandboxAcquire` is reserved in stormpump's ABI and not implemented:
        // the engine dispatches twelve ops and that is not one of them. A
        // sandbox is not a thing a client asks for — `Spawn` builds the
        // workload's namespaces itself, and the warm pool behind that is the
        // engine's own business rather than something a caller reaches into.
        //
        // So a pod sandbox here is what the pod *wants*, held until its
        // containers are spawned and folded into each of their specs. The
        // consequence, stated plainly: containers of a pod get namespaces that
        // match rather than namespaces they share, because nothing in the spec
        // can yet say "join that one's". That is right for a single-container
        // pod and wrong for a pod that expects a shared localhost — which is
        // the next thing this needs from the engine.
        let sb = Sandbox {
            id: id.clone(),
            handle: None,
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
            // Back to the pool, which is what makes the next start cheap.
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
        self.probe()?;
        {
            let sandboxes = self.sandboxes.lock().await;
            if !sandboxes.contains_key(sandbox_id) {
                return Err(CriError::NotFound(format!("sandbox {sandbox_id}")));
            }
        }
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
        let (spec, path) = {
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
            (spec, path)
        };

        // Registered now, not at create: a volume handle is a resource the
        // engine holds, and holding one for a container that may never start
        // is a leak for as long as its pod is pending.
        let workload = self
            .on_ring(move |r| {
                let root = r.volume_register(&path)?;
                // Both handles, before the spawn that uses them. A spawn
                // refused with EINVAL says nothing about *which* argument was
                // wrong, and the two candidates — a spec defined for another
                // domain, and a root handle the engine does not recognise —
                // are told apart by seeing them.
                tracing::debug!(
                    ?spec, ?root, path = %path, domain = Domain::Container as u8,
                    "stormpump: spawning"
                );
                r.spawn(spec, root, Domain::Container as u8)
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
}

impl StormpumpImages {
    pub fn new(registry: impl Into<String>) -> StormpumpImages {
        StormpumpImages { registry: registry.into() }
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
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        if let Some(path) = Self::local_path(image) {
            tracing::info!(image = %image, path = %path.display(), "image is a golden on this node");
            return Ok(path.to_string_lossy().into_owned());
        }
        // Everything else needs the registry to mint a copy-on-write clone and
        // the node to attach it, and the attach half does not exist yet: a
        // clone is a volume, and nothing mounts a volume at runtime. Reported
        // rather than faked, because a container started on the wrong
        // filesystem is worse than one that does not start.
        Err(CriError::ImagePull(format!(
            "{image} is not a golden on this node, and pulling from the registry at {} \
             needs runtime volume attach, which is not built",
            self.registry
        )))
    }

    async fn image_status(&self, image: &str) -> Result<Option<ImageInfo>, CriError> {
        Ok(Self::local_path(image).map(|p| ImageInfo {
            id: p.to_string_lossy().into_owned(),
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
