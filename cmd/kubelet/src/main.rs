//! kubelet — Kubernetes node agent: registers the node, runs the pod lifecycle
//! against a container runtime (native/VM/CRI), and reports status/heartbeats.
//!
//! NOTE: this wires the existing `kubelet` library entrypoints. The node level
//! is early — CRI runtime integration, node registration, and networking still
//! need real work (see the repo README + issues).

use clap::Parser;
use kubelet::{
    detect_cri_socket, CriClient, Kubelet, KubeletConfig, NativeImageService, NativeRuntime,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let node_name = cli.node_name.clone().unwrap_or_else(|| {
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("NODE_NAME"))
            .unwrap_or_else(|_| "localhost".to_string())
    });
    tracing::info!(
        "kubelet starting — node={node_name} runtime={} apiserver={}",
        cli.runtime,
        cli.apiserver
    );

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
                let rt = Arc::new(NativeRuntime::new());
                let img = Arc::new(NativeImageService::new());
                let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
                (rt as _, img as _, mig)
            }
        }
        "cri" => {
            let socket = cli.cri_socket.clone().unwrap_or_else(detect_cri_socket);
            tracing::info!("kubelet using CRI runtime ({})", socket);
            let rt = Arc::new(CriClient::new(&socket));
            let img = Arc::new(CriClient::new(&socket));
            let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
            (rt as _, img as _, mig)
        }
        _ => {
            tracing::info!("kubelet using native runtime (libcontainer)");
            let rt = Arc::new(NativeRuntime::new());
            let img = Arc::new(NativeImageService::new());
            let mig = rt.clone() as Arc<dyn kubelet::cri::MigrationService>;
            (rt as _, img as _, mig)
        }
    };

    let config = KubeletConfig {
        node_name,
        api_server_url: cli.apiserver,
        pod_cidr: cli.pod_cidr,
        ..Default::default()
    };
    let kubelet = Kubelet::new(config, runtime, images, migration);
    if let Err(e) = kubelet.run().await {
        anyhow::bail!("kubelet failed: {e}");
    }
    Ok(())
}
