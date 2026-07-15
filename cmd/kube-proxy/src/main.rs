//! kube-proxy — programs the node's service dataplane (iptables today, eBPF
//! planned) for ClusterIP/NodePort, watching Services + Endpoints from the API.

use clap::Parser;
use proxy::{ProxyConfig, ServiceProxy};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "kube-proxy", about = "Kubernetes service proxy (Rust)")]
struct Cli {
    /// API server URL to watch Services/Endpoints from.
    #[arg(long, env = "APISERVER_URL", default_value = "http://127.0.0.1:6443")]
    apiserver: String,

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
        node_name,
        ..Default::default()
    };
    let proxy = ServiceProxy::new(config);
    if let Err(e) = proxy.run().await {
        anyhow::bail!("kube-proxy failed: {e}");
    }
    Ok(())
}
