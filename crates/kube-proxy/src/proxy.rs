//! Service proxy main loop.
//!
//! Watches Services and Endpoints, generates iptables rules,
//! and applies them to the node.

use crate::endpoints::sync_services_and_endpoints;
use crate::iptables;
use crate::service_map::ServiceMap;
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

/// Service proxy configuration.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub api_server_url: String,
    pub sync_interval: Duration,
    pub node_name: String,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            api_server_url: "http://localhost:6443".into(),
            sync_interval: Duration::from_secs(5),
            node_name: "localhost".into(),
        }
    }
}

/// The service proxy (kube-proxy equivalent).
pub struct ServiceProxy {
    config: ProxyConfig,
    service_map: ServiceMap,
    client: reqwest::Client,
}

impl ServiceProxy {
    pub fn new(config: ProxyConfig) -> Self {
        Self {
            config,
            service_map: ServiceMap::new(),
            client: reqwest::Client::new(),
        }
    }

    /// Run the proxy. Blocks forever.
    pub async fn run(&self) -> anyhow::Result<()> {
        info!(
            "Service proxy starting, watching {}",
            self.config.api_server_url
        );

        let mut interval = time::interval(self.config.sync_interval);

        loop {
            interval.tick().await;

            match sync_services_and_endpoints(
                &self.config.api_server_url,
                &self.service_map,
                &self.client,
            )
            .await
            {
                Ok(changed) => {
                    if changed {
                        self.apply_rules().await;
                    }
                }
                Err(e) => {
                    error!("Failed to sync services/endpoints: {e}");
                }
            }
        }
    }

    async fn apply_rules(&self) {
        let services = self.service_map.get_all();
        let rules = iptables::generate_rules(&services);

        if let Err(e) = iptables::apply_rules(&rules).await {
            error!("Failed to apply iptables rules: {e}");
        }
    }

    /// Get the service map (for health checks / debugging).
    pub fn service_map(&self) -> &ServiceMap {
        &self.service_map
    }
}
