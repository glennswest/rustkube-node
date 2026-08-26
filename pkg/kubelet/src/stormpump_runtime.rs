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
    image: String,
    /// The handle stormpump returned for the spec. `None` until created.
    spec_handle: Option<u32>,
    /// The handle for the running workload. `None` until started.
    workload_handle: Option<u32>,
    /// The image's volume, registered with the engine. `None` until pulled.
    root_handle: Option<u32>,
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
    handle: Option<u32>,
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
            .map_err(|e| CriError::Other(format!("ring call did not run: {e}")))?
            .map_err(|e| CriError::Other(e.to_string()))
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
                        container = %c.id, name = %c.name, code = c.exit_code,
                        "stormpump: container exited"
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
    use stormpump::spec::{Logs, Profile, Root, Share, Spec};

    let mut argv: Vec<String> = config.command.clone();
    argv.extend(config.args.iter().cloned());

    Spec {
        domain: Domain::Container,
        // The root arrives as a registered volume handle at spawn, not here:
        // the spec is defined once and can be spawned many times, and the
        // image a container runs is a property of the spawn.
        root: Root::Volume,
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
        // The sandbox spec: what namespaces this pod gets. Defined and
        // acquired now rather than at the first container, because the
        // containers of a pod join namespaces that must already exist — and
        // because acquiring is where the warm pool pays off.
        use stormpump::spec::{Profile, Share, Spec};
        let sandbox_spec = Spec {
            domain: Domain::Container,
            profile: if config.host_network { Profile::Host } else { Profile::Routed },
            share: Share {
                pid: config.host_pid,
                ipc: config.host_ipc,
                uts: false,
            },
            ..Spec::default()
        };
        let encoded = sandbox_spec.encode();
        let handle = self
            .on_ring(move |r| {
                let spec = r.spec_define(encoded)?;
                r.sandbox_acquire(spec)
            })
            .await?;

        let sb = Sandbox {
            id: id.clone(),
            handle: Some(handle.raw()),
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
            let _ = self
                .on_ring(move |r| r.sandbox_release(Handle::from_raw(h)))
                .await;
        }
        // Its containers go with it. A container whose sandbox is gone has no
        // namespaces to live in, and leaving it listed would have the kubelet
        // trying to reconcile something that cannot exist.
        self.containers
            .lock()
            .await
            .retain(|_, c| c.sandbox_id != sandbox_id);
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

        let id = self.mint_id("ct");
        let c = Container {
            id: id.clone(),
            sandbox_id: sandbox_id.to_string(),
            name: config.name.clone(),
            image: config.image.clone(),
            spec_handle: Some(spec.raw()),
            workload_handle: None,
            root_handle: None,
            state: ContainerState::Created,
            created_at: now_nanos(),
            started_at: 0,
            finished_at: 0,
            exit_code: 0,
            privileged: config.privileged,
            host_network: config.host_network,
            host_pid: config.host_pid,
        };
        self.containers.lock().await.insert(id.clone(), c);
        tracing::info!(
            container = %id, name = %config.name, image = %config.image,
            "stormpump: container created"
        );
        Ok(id)
    }

    async fn start_container(&self, container_id: &str) -> Result<(), CriError> {
        self.probe()?;
        let (spec, root) = {
            let containers = self.containers.lock().await;
            let c = containers
                .get(container_id)
                .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
            let spec = c.spec_handle.ok_or_else(|| {
                CriError::Other(format!("container {container_id} has no spec"))
            })?;
            // The root is the image's volume. Until pull_image mints one there
            // is nothing to run, and saying so is better than spawning a
            // container with no filesystem and reporting it started.
            let root = c.root_handle.ok_or_else(|| {
                CriError::Other(format!(
                    "container {container_id} has no root volume — its image was never pulled"
                ))
            })?;
            (spec, root)
        };

        let workload = self
            .on_ring(move |r| {
                r.spawn(Handle::from_raw(spec), Handle::from_raw(root), Domain::Container as u8)
            })
            .await?;

        let mut containers = self.containers.lock().await;
        let c = containers
            .get_mut(container_id)
            .ok_or_else(|| CriError::NotFound(format!("container {container_id}")))?;
        c.workload_handle = Some(workload.raw());
        c.state = ContainerState::Running;
        c.started_at = now_nanos();
        tracing::info!(
            container = %c.id, name = %c.name, workload = workload.raw(),
            "stormpump: container started"
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
            self.on_ring(move |r| r.stop(Handle::from_raw(w), grace)).await?;
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
                .on_ring(move |r| r.workload_release(Handle::from_raw(w)))
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
        Err(CriError::Unsupported(
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

#[async_trait]
impl ImageService for StormpumpImages {
    async fn pull_image(&self, image: &str) -> Result<String, CriError> {
        Err(CriError::Unsupported(format!(
            "pulling {image} through the registry at {} is not wired yet",
            self.registry
        )))
    }

    async fn image_status(&self, _image: &str) -> Result<Option<ImageInfo>, CriError> {
        Ok(None)
    }

    async fn list_images(&self) -> Result<Vec<ImageInfo>, CriError> {
        Ok(Vec::new())
    }

    async fn remove_image(&self, _image: &str) -> Result<(), CriError> {
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

    #[tokio::test]
    async fn removing_a_sandbox_takes_its_containers_with_it() {
        let r = StormpumpRuntime::new("/dev/null"); // exists, so probe passes
        let cfg = PodSandboxConfig { name: "p".into(), ..Default::default() };
        let sb = r.run_pod_sandbox(&cfg).await.unwrap();

        let cc = ContainerConfig { name: "c".into(), ..Default::default() };
        let ct = r.create_container(&sb, &cc, &cfg).await.unwrap();
        assert_eq!(r.list_containers(None).await.unwrap().len(), 1);

        r.remove_pod_sandbox(&sb).await.unwrap();
        // A container whose sandbox is gone has no namespaces to live in;
        // leaving it listed has the kubelet reconciling something that cannot
        // exist.
        assert!(r.list_containers(None).await.unwrap().is_empty());
        assert!(r.container_status(&ct).await.is_err());
    }

    #[tokio::test]
    async fn a_container_needs_a_sandbox_that_exists() {
        let r = StormpumpRuntime::new("/dev/null");
        let cfg = PodSandboxConfig::default();
        let cc = ContainerConfig { name: "c".into(), ..Default::default() };
        assert!(r.create_container("sb-nope", &cc, &cfg).await.is_err());
    }

    #[tokio::test]
    async fn exec_sync_refuses_rather_than_reporting_a_success_it_did_not_have() {
        let r = StormpumpRuntime::new("/dev/null");
        let e = r.exec_sync("ct-1", &["true".into()], 1).await;
        assert!(e.is_err(), "an empty success would mark every probe healthy");
    }
}
