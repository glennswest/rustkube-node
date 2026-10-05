//! kube-proxy — programs the node's service dataplane (iptables today, eBPF
//! planned) for ClusterIP/NodePort, watching Services + Endpoints from the API.
//!
//! On a TLS apiserver it trusts `--ca-file` and sends the bearer token from
//! `--token-file`; both default to the pod's ServiceAccount when it has one
//! (rustkube-node#145).

use clap::Parser;
use proxy::client::ApiAuth;
use proxy::{ProxyConfig, ServiceProxy};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "kube-proxy", about = "Kubernetes service proxy (Rust)")]
struct Cli {
    /// API server URL to watch Services/Endpoints from. In a DaemonSet,
    /// `https://$(NODE_IP):6443`, not the `kubernetes` ClusterIP (which is
    /// what kube-proxy itself makes routable).
    #[arg(long, env = "APISERVER_URL", default_value = "http://127.0.0.1:6443")]
    apiserver: String,

    /// PEM CA to trust for the apiserver's certificate.
    /// [default: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt, if present]
    #[arg(long, env = "KUBE_PROXY_CA_FILE")]
    ca_file: Option<PathBuf>,

    /// File with the bearer token sent to the apiserver, re-read on every request.
    /// [default: /var/run/secrets/kubernetes.io/serviceaccount/token, if present]
    #[arg(long, env = "KUBE_PROXY_TOKEN_FILE")]
    token_file: Option<PathBuf>,

    /// Pod CIDR. ClusterIP traffic from outside it is masqueraded, so the
    /// backend's reply returns through this node. Unset: no such rule.
    #[arg(long, env = "KUBE_PROXY_CLUSTER_CIDR")]
    cluster_cidr: Option<String>,

    /// Node name (defaults to hostname).
    #[arg(long, env = "NODE_NAME")]
    node_name: Option<String>,
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
    tracing::info!("kube-proxy starting — node={node_name} apiserver={}", cli.apiserver);

    let config = ProxyConfig {
        api_server_url: cli.apiserver,
        auth: ApiAuth::resolve(cli.ca_file, cli.token_file)?,
        cluster_cidr: cli.cluster_cidr,
        node_name,
        ..Default::default()
    };
    let proxy = ServiceProxy::new(config)?;
    if let Err(e) = proxy.run().await {
        anyhow::bail!("kube-proxy failed: {e}");
    }
    Ok(())
}
