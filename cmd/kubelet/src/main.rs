//! kubelet — Kubernetes node agent: registers the node, runs the pod lifecycle
//! against a container runtime, and reports status/heartbeats.
//!
//! The runtime is `--runtime`: `native` (the default), `stormpump` (what
//! stormcos selects: PID 1's engine over its ring, plus stormvm VMIs and
//! snapshots), `cri` (an external CRI v1 runtime) or `vm` (the legacy microVM
//! path). What is implemented and what is not is in the repo's
//! `docs/status.md`; every flag and default is in `docs/configuration.md`.

use clap::Parser;
use kubelet::{
    detect_cri_socket, CriGrpcClient, Kubelet, KubeletConfig, NativeImageService, NativeRuntime,
    VmRuntime, VmmBackend,
};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "kubelet", about = "Kubernetes node agent (Rust)")]
struct Cli {
    /// API server URL to register with.
    #[arg(long, env = "APISERVER_URL", default_value = "http://127.0.0.1:6443")]
    apiserver: String,

    /// Node name (defaults to hostname).
    #[arg(long, env = "NODE_NAME")]
    node_name: Option<String>,

    /// Pod CIDR for this node, written to the Node's spec.podCIDR.
    #[arg(long, env = "POD_CIDR")]
    pod_cidr: Option<String>,

    /// Extra labels to register this node with: `key=value,key=value`.
    #[arg(long = "node-labels", env = "NODE_LABELS", default_value = "")]
    node_labels: String,

    /// Annotations to register this node with: `key=value,key=value`.
    ///
    /// Not an upstream kubelet flag. It is here because a node's placement
    /// facts — rack, shelf, failure domain — have to reach the API from
    /// somewhere, and the node is the only thing that knows them at boot.
    #[arg(long = "node-annotations", env = "NODE_ANNOTATIONS", default_value = "")]
    node_annotations: String,

    /// Taints to register this node with: `key=value:Effect` or `key:Effect`.
    ///
    /// Applied only when the Node object is created. This is how a node keeps
    /// workloads off itself until something makes it usable — a node with no
    /// pod network is Ready by every measure the kubelet can take, and pods
    /// scheduled onto it get no address.
    #[arg(long = "register-with-taints", env = "REGISTER_WITH_TAINTS", default_value = "")]
    register_with_taints: String,

    /// Static-pod manifest directory. Pods here run locally (no apiserver) —
    /// how the control plane bootstraps. Empty string disables static pods.
    #[arg(long, env = "POD_MANIFEST_PATH", default_value = "/etc/kubernetes/manifests")]
    pod_manifest_path: String,

    /// Where the registry that mints image clones lives (--runtime=stormpump).
    #[arg(long, default_value = "http://127.0.0.1:5100")]
    registry: String,

    /// This node's stormblock engine API: claims, VM disks and pulled images
    /// are cloned and attached through it. Its token is read from
    /// $STORMBLOCK_API_TOKEN or $STORMBLOCK_TOKEN_FILE (default
    /// /run/stormblock/engine/api_token), as stormblock's CLI does (#66).
    #[arg(long, env = "STORMBLOCK_URL", default_value = "http://127.0.0.1:9090")]
    stormblock: String,

    /// CRI socket path (only used with --runtime=cri).
    #[arg(long, env = "CRI_SOCKET")]
    cri_socket: Option<String>,

    /// Container runtime: native (libcontainer), vm (microVM), cri (external CRI).
    #[arg(long, default_value = "native", value_parser = ["native", "vm", "cri", "stormpump"])]
    runtime: String,

    /// VMM backend for --runtime=vm.
    #[arg(long, default_value = "auto", value_parser = ["auto", "cloud-hypervisor", "qemu", "firecracker"])]
    vmm: String,

    /// CNI network config directory (Cilium writes 05-cilium.conflist here).
    #[arg(long, env = "CNI_CONF_DIR", default_value = "/etc/cni/net.d")]
    cni_conf_dir: String,

    /// CNI plugin binary directory.
    #[arg(long, env = "CNI_BIN_DIR", default_value = "/opt/cni/bin")]
    cni_bin_dir: String,

    /// Run no CNI plugin (dev only). Pods do not get host networking: a
    /// pod that is not hostNetwork still gets its own network namespace, with
    /// loopback only and no address.
    #[arg(long, default_value_t = false)]
    no_cni: bool,

    /// Pod and VMI passes (starts, status checks, teardowns) run at once.
    /// Default: 16 per CPU, at least 32, at most 256.
    #[arg(long, env = "POD_WORKERS")]
    pod_workers: Option<usize>,

    /// Committed bytes allowed per byte of the stormblock data slabs: a claim
    /// is charged its full size class (#62, #108). 1.0 is no overcommit.
    #[arg(long, env = "STORAGE_OVERCOMMIT", default_value_t = 1.0)]
    storage_overcommit: f64,

    /// Percent of the stormblock data slabs kept back from claims (#62).
    #[arg(long, env = "STORAGE_RESERVE_PERCENT", default_value_t = 5.0)]
    storage_reserve_percent: f64,

    /// Percent of the stormblock data slabs written past which the node warns:
    /// a SlabFilling Event on its stormblock PVs (#62).
    #[arg(long, env = "STORAGE_ALERT_PERCENT", default_value_t = 85.0)]
    storage_alert_percent: f64,

    /// Seconds `/vmInstance` answers guest metadata from the VMI cache without
    /// word from the apiserver (a renewed node Lease or a VMI list); past it,
    /// 503 + Retry-After (#156). Default: the Lease duration. 0: unbounded.
    #[arg(long, env = "METADATA_MAX_STALENESS", default_value_t = 40)]
    metadata_max_staleness: u64,

    /// Pods this node takes: its capacity and allocatable `pods`, which the
    /// scheduler holds it to (#165). Upstream's default; stormcos sets 250.
    #[arg(long, env = "MAX_PODS", default_value_t = kubelet::node_status::DEFAULT_MAX_PODS)]
    max_pods: u32,

    /// Port for the kubelet's inbound HTTP server (/healthz, /metrics, /pods).
    #[arg(long, env = "KUBELET_PORT", default_value_t = 10250)]
    kubelet_port: u16,

    /// Cluster CA cert (PEM) to trust for an HTTPS apiserver.
    #[arg(long, env = "APISERVER_CA")]
    apiserver_ca: Option<String>,

    /// File containing a bearer token to authenticate to the apiserver.
    #[arg(long, env = "KUBELET_TOKEN_FILE")]
    token_file: Option<String>,

    /// Kubeconfig file providing apiserver URL, CA, and client cert/key or token.
    /// Explicit flags below override the matching kubeconfig values.
    #[arg(long, env = "KUBECONFIG")]
    kubeconfig: Option<String>,

    /// Client certificate (PEM) for mutual-TLS node auth (system:node:<name>).
    #[arg(long, env = "KUBELET_CLIENT_CERT")]
    client_certificate: Option<String>,

    /// Private key (PEM) for --client-certificate.
    #[arg(long, env = "KUBELET_CLIENT_KEY")]
    client_key: Option<String>,

    /// Skip apiserver certificate verification (dev only — do not use in prod).
    #[arg(long, default_value_t = false)]
    insecure_skip_tls_verify: bool,

    /// Serving certificate (PEM) for the inbound :10250 server. Self-signed if unset.
    #[arg(long, env = "KUBELET_TLS_CERT_FILE")]
    tls_cert_file: Option<String>,

    /// Serving private key (PEM) for --tls-cert-file.
    #[arg(long, env = "KUBELET_TLS_PRIVATE_KEY_FILE")]
    tls_private_key_file: Option<String>,

    /// File with a static bearer token accepted by the inbound :10250 server
    /// (e.g. for a metrics scraper). Tokens are also validated via TokenReview.
    #[arg(long, env = "KUBELET_SERVER_TOKEN_FILE")]
    server_token_file: Option<String>,

    /// Serve the inbound :10250 endpoints unauthenticated (dev only).
    #[arg(long, default_value_t = false)]
    anonymous_auth: bool,
}

/// How long a credential file named on the command line may take to appear.
const CREDENTIAL_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Read a credential file the boot may not have written yet: the apiserver CA,
/// the client pair, a token, the serving pair (#69).
///
/// Bounded, because a wait that never ends is a node that never says why it is
/// not up; and fatal at the end of it, because a credential that was named on
/// the command line and is not on disk is a misconfiguration this process
/// cannot work around. Answering "none" instead is a downgrade: a missing
/// client pair or token made this kubelet `system:anonymous` (cluster-admin on
/// sno, a stream of 403s that read as RBAC elsewhere), a missing CA a client
/// that can never verify the apiserver, a missing serving pair a self-signed
/// one.
async fn wait_for_file(flag: &str, path: &str, limit: std::time::Duration) -> anyhow::Result<Vec<u8>> {
    let started = std::time::Instant::now();
    let mut said = false;
    loop {
        match std::fs::read(path) {
            // An empty file is a write in progress, not a credential.
            Ok(b) if !b.is_empty() => {
                if said {
                    tracing::info!("{flag} {path} appeared after {:.1}s", started.elapsed().as_secs_f64());
                }
                return Ok(b);
            }
            _ => {}
        }
        if started.elapsed() >= limit {
            anyhow::bail!(
                "{flag} {path} was named but is still not readable after {}s. \
                 A credential that was asked for is never replaced by none",
                limit.as_secs()
            );
        }
        if !said {
            tracing::warn!("{flag} {path} is not there yet — waiting for it");
            said = true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// [`wait_for_file`] for an optional flag.
async fn named(flag: &str, path: Option<&str>) -> anyhow::Result<Option<Vec<u8>>> {
    match path {
        Some(p) => Ok(Some(wait_for_file(flag, p, CREDENTIAL_WAIT).await?)),
        None => Ok(None),
    }
}

/// A token file's contents, trimmed.
fn token_text(bytes: Vec<u8>) -> anyhow::Result<String> {
    Ok(String::from_utf8(bytes)?.trim().to_string())
}

const DEFAULT_APISERVER: &str = "http://127.0.0.1:6443";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let node_name = cli
        .node_name
        .clone()
        .unwrap_or_else(kubelet::detect_node_name);
    tracing::info!(
        "kubelet starting — node={node_name} runtime={} apiserver={}",
        cli.runtime,
        cli.apiserver
    );

    // Standard CNI for the native/VM-fallback runtimes. Cilium is the expected
    // default plugin; anything spec-compliant in the conf dir works.
    //
    // Runtimes that do their own networking are exempt, and that exemption has
    // to be real rather than a comment. `cri` hands the job to the external
    // runtime; `stormpump` does it in the engine, which owns the network
    // namespace and attaches each workload by profile (routed, private,
    // macvlan, host, isolated) with no plugin involved.
    //
    // Without this the kubelet gates sandbox creation on a CNI config it will
    // never use, and an empty /etc/cni/net.d makes every pod fail as
    // `No such file or directory` — an ENOENT that points at the container
    // image, the volume, or the log path, none of which are the cause. That
    // cost a day here.
    // A runtime that does its own networking is exempt — **unless a CNI
    // network is actually configured**, because then someone has installed a
    // plugin and means it.
    //
    // The first version of this exempted by runtime alone, which is wrong the
    // moment Cilium is the plan: Cilium *is* a CNI plugin, something has to
    // invoke it, and a blanket exemption means nothing ever does. The runtime
    // owning networking is the *default*, not a veto over a network config
    // that exists on disk.
    let runtime_owns_networking = matches!(cli.runtime.as_str(), "cri" | "stormpump");
    let cni_configured = cni::CniInvoker::new(
        cli.cni_conf_dir.clone(),
        vec![std::path::PathBuf::from(&cli.cni_bin_dir)],
    )
    .network_ready()
    .is_ok();
    // **A runtime that re-checks per pod must not be gated on a one-time
    // look.** Cilium writes its conflist when its agent comes up, which is
    // minutes after this line runs, and the answer here used to be permanent
    // for the life of the process: the node logged "no network config is
    // present", never looked again, and every pod got an empty namespace while
    // a healthy Cilium sat beside it. `CniInvoker` reloads its config on every
    // call precisely so this decision does not have to be final.
    let cni_invoker = if cli.no_cni {
        tracing::warn!("CNI disabled (--no-cni) — pods get an isolated namespace with loopback only and no address");
        None
    } else if runtime_owns_networking && !cni_configured {
        tracing::info!(
            "no CNI network in {} yet; the {} runtime will use its own networking until \
             one appears",
            cli.cni_conf_dir,
            cli.runtime
        );
        Some(cni::CniInvoker::new(
            cli.cni_conf_dir.clone(),
            vec![std::path::PathBuf::from(&cli.cni_bin_dir)],
        ))
    } else {
        let invoker = cni::CniInvoker::new(
            cli.cni_conf_dir.clone(),
            vec![std::path::PathBuf::from(&cli.cni_bin_dir)],
        );
        match invoker.network_ready() {
            Ok(name) => tracing::info!("CNI network '{name}' configured ({})", cli.cni_conf_dir),
            Err(e) => tracing::warn!(
                "CNI not ready yet ({e}) — pod sandbox creation will fail until a \
                 network config appears in {} (e.g. install Cilium)",
                cli.cni_conf_dir
            ),
        }
        Some(invoker)
    };

    // The ring, when there is one. A VM is a workload in a machine domain and
    // only the engine starts one, so every other runtime leaves this None and
    // the kubelet simply does not reconcile VMs.
    let mut engine_ring: Option<std::sync::Arc<kubelet::stormpump_ring::RingClient>> = None;
    // The node's stormblock engine, with its token (#66): one client, shared
    // by claims, VM disks and image pulls. The token may not exist yet (the
    // engine mints it at start), and the client keeps looking for it.
    let engine = kubelet::engine::EngineClient::from_env(&cli.stormblock);
    tracing::info!("stormblock engine: {engine:?}");

    let (runtime, images, migration): (
        Arc<dyn kubelet::cri::RuntimeService>,
        Arc<dyn kubelet::cri::ImageService>,
        Arc<dyn kubelet::cri::MigrationService>,
    ) = match cli.runtime.as_str() {
        "vm" => {
            let backend = match cli.vmm.as_str() {
                "cloud-hypervisor" => Some(VmmBackend::CloudHypervisor),
                "qemu" => Some(VmmBackend::Qemu),
                "firecracker" => Some(VmmBackend::Firecracker),
                _ => VmmBackend::detect(),
            };
            if let Some(backend) = backend {
                tracing::info!("kubelet using VM runtime ({:?})", backend);
                let rt = Arc::new(VmRuntime::new(backend));
                let img = Arc::new(NativeImageService::new());
                let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
                (rt as _, img as _, mig)
            } else {
                tracing::error!("no VMM found, falling back to native runtime");
                let rt = Arc::new(NativeRuntime::new().with_cni(cni_invoker));
                let img = Arc::new(NativeImageService::new());
                let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
                (rt as _, img as _, mig)
            }
        }
        "stormpump" => {
            // The engine's ring, not a socket protocol. See
            // kubelet::stormpump_runtime for why there is no shim.
            let socket = cli
                .cri_socket
                .clone()
                .unwrap_or_else(|| kubelet::stormpump_runtime::DEFAULT_SOCKET.to_string());
            match kubelet::stormpump_runtime::StormpumpRuntime::connect(&socket) {
                Ok(rt) => {
                    tracing::info!("kubelet using stormpump runtime (ring at {socket})");
                    // The runtime asks per pod whether a network exists, so it
                    // is given the invoker even when none is configured yet.
                    // The images found and the containers created share each
                    // image's config (#98).
                    let configs = Arc::new(kubelet::image_config::ImageConfigs::default());
                    // Each container's own root is cloned from its image's
                    // golden through the node's engine (#104).
                    let rt = Arc::new(
                        rt.with_cni(cni_invoker)
                            .with_image_configs(configs.clone())
                            .with_roots(kubelet::container_roots::Roots::new(engine.clone(), node_name.clone())),
                    );
                    engine_ring = rt.ring_client();
                    // Roots a kubelet that died mid-create or mid-removal left.
                    {
                        let rt = rt.clone();
                        tokio::spawn(async move { rt.sweep_roots().await });
                    }
                    // A pull only finds the image's golden (#104).
                    let img = Arc::new(
                        kubelet::stormpump_runtime::StormpumpImages::new(
                            cli.registry.clone(),
                        )
                        .with_image_configs(configs),
                    );
                    let mig = Arc::new(NativeRuntime::new())
                        as Arc<dyn kubelet::cri::MigrationService>;
                    (rt as _, img as _, mig)
                }
                Err(e) => {
                    // Refused rather than fallen back from. A kubelet that
                    // silently runs a different runtime than it was told to
                    // is a node whose pods are not where anyone thinks.
                    tracing::error!("cannot use the stormpump runtime: {e}");
                    std::process::exit(1);
                }
            }
        }
        "cri" => {
            let socket = cli.cri_socket.clone().unwrap_or_else(detect_cri_socket);
            tracing::info!("kubelet using CRI runtime via gRPC ({})", socket);
            let rt = Arc::new(CriGrpcClient::new(&socket));
            let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
            (rt.clone() as _, rt as _, mig)
        }
        _ => {
            tracing::info!("kubelet using native runtime (libcontainer)");
            let rt = Arc::new(NativeRuntime::new().with_cni(cni_invoker));
            let img = Arc::new(NativeImageService::new());
            let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
            (rt as _, img as _, mig)
        }
    };

    // A kubeconfig supplies defaults for the apiserver URL, CA, client cert/key,
    // and token; explicit --* flags override the matching kubeconfig field.
    // Written by the same boot as the credentials it names: waited for too.
    named("--kubeconfig", cli.kubeconfig.as_deref()).await?;
    let kubeconfig = match cli.kubeconfig.as_deref() {
        Some(p) => match kubelet::kubeconfig::load(p) {
            Ok(kc) => {
                tracing::info!("loaded kubeconfig {p}");
                Some(kc)
            }
            Err(e) => anyhow::bail!("kubeconfig {p}: {e}"),
        },
        None => None,
    };

    // A trust anchor that was asked for and is not there **yet**.
    //
    // A best-effort read answered `None` for a missing file, and `None` here
    // means "no CA", so an explicitly configured anchor that had not been
    // written yet turned into a client that trusts only the public roots and
    // can therefore never verify this cluster's apiserver. Nothing rebuilds
    // that client, so the node retried registration every thirty seconds for
    // the life of the boot:
    //
    //   WARN kubelet: Node registration failed (error sending request for url
    //        (https://192.168.30.2:6443/api/v1/nodes)); retrying in 30s
    //
    // and nothing ever scheduled, because the apiserver had no node. On the
    // machine this was found on the margin was two seconds — the kubelet
    // started at 20:25:01.81 and `stormcert-init` wrote `ca.crt` at 20:25:03 —
    // and both are started by the same boot, so the order is not something
    // either one controls.
    //
    // The message made it worse: `reqwest` reports a TLS trust failure as
    // "error sending request for url", naming neither TLS nor the
    // certificate, so it reads exactly like the network being down. `curl`
    // with the same CA, the same URL and the same network namespace answered
    // HTTP 200 the whole time.
    //
    // So an anchor that was named is waited for, briefly, and its continued
    // absence is fatal rather than silent. A path that was given and cannot
    // be read is a misconfiguration; only the delay is transient. The same
    // holds for every credential named below (#69): a missing client pair or
    // token was skipped, and the kubelet ran as `system:anonymous`.
    let apiserver_ca = named("--apiserver-ca", cli.apiserver_ca.as_deref())
        .await?
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.ca_pem.clone()));
    let client_cert = named("--client-certificate", cli.client_certificate.as_deref())
        .await?
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.client_cert_pem.clone()));
    let client_key = named("--client-key", cli.client_key.as_deref())
        .await?
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.client_key_pem.clone()));
    let bearer_token = match named("--token-file", cli.token_file.as_deref()).await? {
        Some(b) => Some(token_text(b)?),
        None => kubeconfig.as_ref().and_then(|k| k.token.clone()),
    };

    // Explicit --apiserver wins over kubeconfig's server, which wins over the default.
    let api_server_url = if cli.apiserver != DEFAULT_APISERVER {
        cli.apiserver
    } else {
        kubeconfig
            .as_ref()
            .and_then(|k| k.server.clone())
            .unwrap_or(cli.apiserver)
    };
    let insecure_skip_tls_verify = cli.insecure_skip_tls_verify
        || kubeconfig
            .as_ref()
            .map(|k| k.insecure_skip_tls_verify)
            .unwrap_or(false);

    // Inbound :10250 server TLS + auth (rustkube-node#9).
    // Named serving files too (#69): a missing pair silently became a
    // self-signed one, a missing token no static token.
    let serving_cert = named("--tls-cert-file", cli.tls_cert_file.as_deref()).await?;
    let serving_key = named("--tls-private-key-file", cli.tls_private_key_file.as_deref()).await?;
    let server_auth_token = match named("--server-token-file", cli.server_token_file.as_deref()).await? {
        Some(b) => Some(token_text(b)?),
        None => None,
    };

    let config = KubeletConfig {
        node_name,
        api_server_url,
        pod_cidr: cli.pod_cidr,
        node_labels: kubelet::node_status::parse_key_values(&cli.node_labels),
        node_annotations: kubelet::node_status::parse_key_values(&cli.node_annotations),
        register_with_taints: kubelet::node_status::parse_taints(&cli.register_with_taints),
        pod_manifest_path: if cli.pod_manifest_path.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(&cli.pod_manifest_path))
        },
        kubelet_port: cli.kubelet_port,
        max_pods: cli.max_pods,
        apiserver_ca,
        bearer_token,
        client_cert,
        client_key,
        insecure_skip_tls_verify,
        serving_cert,
        serving_key,
        serving_cert_path: cli.tls_cert_file.as_ref().map(std::path::PathBuf::from),
        client_cert_path: cli.client_certificate.as_ref().map(std::path::PathBuf::from),
        client_key_path: cli.client_key.as_ref().map(std::path::PathBuf::from),
        serving_key_path: cli.tls_private_key_file.as_ref().map(std::path::PathBuf::from),
        server_auth_token,
        anonymous_auth: cli.anonymous_auth,
        cni_conf_dir: (!cli.no_cni).then(|| std::path::PathBuf::from(&cli.cni_conf_dir)),
        engine: engine.clone(),
        pod_workers: cli.pod_workers.unwrap_or_else(kubelet::workload::default_workers).max(1),
        storage: kubelet::capacity::Policy {
            overcommit: cli.storage_overcommit,
            reserve_percent: cli.storage_reserve_percent,
            alert_percent: cli.storage_alert_percent,
        },
        metadata_max_staleness: std::time::Duration::from_secs(cli.metadata_max_staleness),
        ..Default::default()
    };
    let mut kubelet = Kubelet::new(config, runtime, images, migration)?;
    if let Some(ring) = engine_ring {
        // The same connection the containers are started on, deliberately: a
        // workload belongs to the client that started it, so a second ring
        // would mean a second thing whose death is a VM's death.
        // The VM manager's own invoker on the same directories (#88): a VMI
        // on the pod network gets its sandbox from the same plugins.
        let vm_cni = (!cli.no_cni).then(|| {
            cni::CniInvoker::new(cli.cni_conf_dir.clone(), vec![std::path::PathBuf::from(&cli.cni_bin_dir)])
        });
        kubelet = kubelet.with_engine(ring, vm_cni);
    }
    if let Err(e) = kubelet.run().await {
        anyhow::bail!("kubelet failed: {e}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("kubelet-main-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn a_named_credential_written_late_is_waited_for() {
        let f = scratch("late").join("kubelet.crt");
        let g = f.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            // An empty file first: a write in progress, not the credential.
            std::fs::write(&g, b"").unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            std::fs::write(&g, b"PEM").unwrap();
        });
        let got = wait_for_file("--client-certificate", f.to_str().unwrap(), Duration::from_secs(10)).await;
        assert_eq!(got.unwrap(), b"PEM");
    }

    #[tokio::test]
    async fn a_named_credential_that_never_appears_is_fatal_naming_the_flag() {
        let f = scratch("never").join("token");
        let e = wait_for_file("--token-file", f.to_str().unwrap(), Duration::from_millis(600))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("--token-file") && e.contains(f.to_str().unwrap()), "{e}");
    }

    #[tokio::test]
    async fn an_unnamed_credential_is_none_at_once() {
        assert!(named("--client-key", None).await.unwrap().is_none());
    }

    #[test]
    fn a_token_file_is_trimmed() {
        assert_eq!(token_text(b"abc.def\n".to_vec()).unwrap(), "abc.def");
    }
}
