//! Service proxy main loop.
//!
//! Lists Services and Endpoints, generates iptables rules, and applies them
//! to the node when they differ from what was last applied (or once a
//! minute, so rules removed behind its back come back).

use crate::client::{ApiAuth, ApiClient};
use crate::endpoints::sync_services_and_endpoints;
use crate::iptables::{self, IptablesRules, RuleApplier, RuleOptions};
use crate::service_map::ServiceMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time;
use tracing::{error, info};

/// Service proxy configuration.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub api_server_url: String,
    /// CA and bearer token for the apiserver (rustkube-node#145).
    pub auth: ApiAuth,
    pub sync_interval: Duration,
    /// Rules are applied again this long after the last apply, even unchanged.
    pub resync_interval: Duration,
    pub node_name: String,
    /// Pod CIDR; ClusterIP traffic from outside it is masqueraded.
    pub cluster_cidr: Option<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            api_server_url: "http://localhost:6443".into(),
            auth: ApiAuth::default(),
            sync_interval: Duration::from_secs(5),
            resync_interval: Duration::from_secs(60),
            node_name: "localhost".into(),
            cluster_cidr: None,
        }
    }
}

/// The service proxy (kube-proxy equivalent).
pub struct ServiceProxy {
    config: ProxyConfig,
    service_map: ServiceMap,
    client: ApiClient,
    /// Dataplane seam: real `iptables-restore` on Linux, no-op elsewhere.
    applier: Box<dyn RuleApplier>,
    /// The rules last applied successfully, and when.
    applied: Mutex<Option<(IptablesRules, Instant)>>,
}

impl ServiceProxy {
    /// Construct a proxy using the platform-default rule applier
    /// (`iptables-restore` on Linux, a no-op applier on other platforms).
    /// Fails when the CA or token file is not usable.
    pub fn new(config: ProxyConfig) -> anyhow::Result<Self> {
        Self::with_applier(config, iptables::default_applier())
    }

    /// Construct a proxy with an explicit [`RuleApplier`], allowing tests (and
    /// non-Linux hosts) to inject a mock/no-op dataplane.
    pub fn with_applier(config: ProxyConfig, applier: Box<dyn RuleApplier>) -> anyhow::Result<Self> {
        let client = ApiClient::new(&config.api_server_url, &config.auth)?;
        Ok(Self {
            config,
            service_map: ServiceMap::new(),
            client,
            applier,
            applied: Mutex::new(None),
        })
    }

    /// Run the proxy. Blocks forever.
    pub async fn run(&self) -> anyhow::Result<()> {
        info!(
            "Service proxy starting, watching {} (CA {}, token {})",
            self.client.base(),
            describe(&self.config.auth.ca_file),
            describe(&self.config.auth.token_file),
        );

        let mut interval = time::interval(self.config.sync_interval);

        loop {
            interval.tick().await;
            self.sync_once().await;
        }
    }

    /// One pass: list, and apply the rules if they changed or are due.
    /// A failed list changes nothing on the node.
    pub async fn sync_once(&self) {
        if let Err(e) = sync_services_and_endpoints(&self.client, &self.service_map).await {
            error!("Failed to sync services/endpoints: {e}");
            return;
        }
        let opts = RuleOptions {
            cluster_cidr: self.config.cluster_cidr.clone(),
        };
        let rules = iptables::generate_rules_with(&self.service_map.get_all(), &opts);
        let mut applied = self.applied.lock().await;
        let due = match applied.as_ref() {
            None => true,
            Some((last, at)) => *last != rules || at.elapsed() >= self.config.resync_interval,
        };
        if !due {
            return;
        }
        if applied.as_ref().map_or(true, |(last, _)| *last != rules) {
            info!(
                "Service map changed: {} service ports, {} rules",
                self.service_map.get_all().len(),
                rules.nat_rules.len()
            );
        }
        match self.applier.apply(&rules).await {
            Ok(()) => *applied = Some((rules, Instant::now())),
            // Not recorded: the next pass tries again.
            Err(e) => error!("Failed to apply iptables rules: {e}"),
        }
    }

    /// Get the service map (for health checks / debugging).
    pub fn service_map(&self) -> &ServiceMap {
        &self.service_map
    }
}

fn describe(p: &Option<std::path::PathBuf>) -> String {
    p.as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "none".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    /// Answers each GET from `answer(path)` → (status line, body), forever.
    fn fake_apiserver(
        answer: impl Fn(&str) -> (String, String) + Send + Sync + 'static,
    ) -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        std::thread::spawn(move || {
            for s in l.incoming() {
                let mut s = s.unwrap();
                let mut r = BufReader::new(s.try_clone().unwrap());
                let mut first = String::new();
                r.read_line(&mut first).unwrap();
                loop {
                    let mut line = String::new();
                    if r.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                        break;
                    }
                }
                let path = first.split_whitespace().nth(1).unwrap_or("").to_string();
                let (status, body) = answer(&path);
                let _ = write!(
                    s,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        url
    }

    #[derive(Clone, Default)]
    struct Counting(Arc<std::sync::Mutex<Vec<IptablesRules>>>);

    #[async_trait::async_trait]
    impl RuleApplier for Counting {
        async fn apply(&self, rules: &IptablesRules) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(rules.clone());
            Ok(())
        }
    }

    const SERVICES: &str = r#"{"items":[{"metadata":{"name":"web","namespace":"ns"},
        "spec":{"clusterIP":"10.96.1.1","ports":[{"name":"http","port":80,"protocol":"TCP"}]}}]}"#;

    fn endpoints(ip: &str) -> String {
        format!(
            r#"{{"items":[{{"metadata":{{"name":"web","namespace":"ns"}},
            "subsets":[{{"addresses":[{{"ip":"{ip}"}}],"ports":[{{"name":"http","port":8080,"protocol":"TCP"}}]}}]}}]}}"#
        )
    }

    #[tokio::test]
    async fn applies_once_then_again_when_a_backend_moves_at_the_same_count() {
        let ip = Arc::new(std::sync::Mutex::new("10.244.0.3".to_string()));
        let ip2 = ip.clone();
        let url = fake_apiserver(move |path| {
            let body = if path.starts_with("/api/v1/services") {
                SERVICES.to_string()
            } else {
                endpoints(&ip2.lock().unwrap())
            };
            ("200 OK".into(), body)
        });
        let applier = Counting::default();
        let p = ServiceProxy::with_applier(
            ProxyConfig { api_server_url: url, ..Default::default() },
            Box::new(applier.clone()),
        )
        .unwrap();
        p.sync_once().await;
        p.sync_once().await;
        assert_eq!(applier.0.lock().unwrap().len(), 1, "unchanged rules applied twice");
        *ip.lock().unwrap() = "10.244.0.9".into();
        p.sync_once().await;
        let applied = applier.0.lock().unwrap();
        assert_eq!(applied.len(), 2);
        assert!(applied[1].nat_rules.iter().any(|r| r.contains("10.244.0.9:8080")));
        assert!(!applied[1].nat_rules.iter().any(|r| r.contains("10.244.0.3")));
    }

    #[tokio::test]
    async fn a_refused_list_leaves_the_rules_alone() {
        let refuse = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let r2 = refuse.clone();
        let url = fake_apiserver(move |path| {
            if r2.load(std::sync::atomic::Ordering::SeqCst) {
                return (
                    "401 Unauthorized".into(),
                    r#"{"kind":"Status","code":401}"#.into(),
                );
            }
            let body = if path.starts_with("/api/v1/services") {
                SERVICES.to_string()
            } else {
                endpoints("10.244.0.3")
            };
            ("200 OK".into(), body)
        });
        let applier = Counting::default();
        let p = ServiceProxy::with_applier(
            ProxyConfig {
                api_server_url: url,
                resync_interval: Duration::ZERO,
                ..Default::default()
            },
            Box::new(applier.clone()),
        )
        .unwrap();
        p.sync_once().await;
        refuse.store(true, std::sync::atomic::Ordering::SeqCst);
        p.sync_once().await;
        let applied = applier.0.lock().unwrap();
        // One apply: the 401 pass neither applied an empty set nor resynced.
        assert_eq!(applied.len(), 1);
        assert!(applied[0].has_dnat());
        assert_eq!(p.service_map().get_all()[0].endpoints.len(), 1);
    }
}
