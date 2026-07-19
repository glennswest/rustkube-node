//! kubelet — Kubernetes node agent: registers the node, runs the pod lifecycle
//! against a container runtime (native/VM/CRI), and reports status/heartbeats.
//!
//! NOTE: this wires the existing `kubelet` library entrypoints. The node level
//! is early — CRI runtime integration, node registration, and networking still
//! need real work (see the repo README + issues).

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

    /// CRI socket path (only used with --runtime=cri).
    #[arg(long, env = "CRI_SOCKET")]
    cri_socket: Option<String>,

    /// Container runtime: native (libcontainer), vm (microVM), cri (external CRI).
    #[arg(long, default_value = "native", value_parser = ["native", "vm", "cri"])]
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

    /// Disable CNI networking (pods use host networking). For dev only.
    #[arg(long, default_value_t = false)]
    no_cni: bool,

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
}

/// Read a file to bytes, warning (not failing) if it can't be read — matches
/// the best-effort handling of the other credential files.
fn read_file_bytes(path: &str) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::warn!("cannot read {path}: {e}");
            None
        }
    }
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

    // Standard CNI for the native/VM-fallback runtimes (with --runtime=cri
    // the external runtime invokes CNI itself). Cilium is the expected
    // default plugin; anything spec-compliant in the conf dir works.
    let cni_invoker = if cli.no_cni {
        tracing::warn!("CNI disabled (--no-cni) — pods will use host networking");
        None
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

    let apiserver_ca = cli
        .apiserver_ca
        .as_deref()
        .and_then(read_file_bytes)
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.ca_pem.clone()));
    let client_cert = cli
        .client_certificate
        .as_deref()
        .and_then(read_file_bytes)
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.client_cert_pem.clone()));
    let client_key = cli
        .client_key
        .as_deref()
        .and_then(read_file_bytes)
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.client_key_pem.clone()));
    let bearer_token = cli
        .token_file
        .as_ref()
        .and_then(|p| match std::fs::read_to_string(p) {
            Ok(s) => Some(s.trim().to_string()),
            Err(e) => {
                tracing::warn!("cannot read token file {p}: {e}");
                None
            }
        })
        .or_else(|| kubeconfig.as_ref().and_then(|k| k.token.clone()));

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

    let config = KubeletConfig {
        node_name,
        api_server_url,
        pod_cidr: cli.pod_cidr,
        kubelet_port: cli.kubelet_port,
        apiserver_ca,
        bearer_token,
        client_cert,
        client_key,
        insecure_skip_tls_verify,
        ..Default::default()
    };
    let kubelet = Kubelet::new(config, runtime, images, migration)?;
    if let Err(e) = kubelet.run().await {
        anyhow::bail!("kubelet failed: {e}");
    }
    Ok(())
}
