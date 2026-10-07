//! Stale UDP conntrack entries (rustkube-node#147).
//!
//! The rules send a new UDP flow to a live backend, but an existing flow keeps
//! the NAT conntrack recorded for it: a client socket that talks to kube-dns
//! (`10.96.0.10:53`) keeps reaching the CoreDNS pod that was replaced, until
//! the entry ages out (30 s unreplied, 120 s assured). TCP has no such problem
//! in practice (the old connection fails and a new one is made), so upstream,
//! and this, clear UDP only:
//!
//! - an endpoint that left a UDP Service port: its entries by ClusterIP
//!   (`--orig-dst <clusterIP> --dst-nat <endpoint>`) and by NodePort
//!   (`--dport <nodePort> --dst-nat <endpoint>`);
//! - a UDP Service port that gains its first endpoint: every entry to its
//!   ClusterIP (`--orig-dst <clusterIP>`), since packets sent while it had none
//!   left entries that would keep them going nowhere.
//!
//! Done after the new rules are in place, so nothing re-creates an entry to
//! the old backend in between.

use crate::service_map::ServiceInfo;
use std::collections::{BTreeSet, HashMap};

/// One `conntrack -D` to run, as its arguments.
pub type Delete = Vec<String>;

/// What the rules were built from, for UDP: per Service port, its ClusterIP,
/// NodePort and ready endpoint IPs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UdpBackends(HashMap<String, (String, Option<u16>, BTreeSet<String>)>);

impl UdpBackends {
    pub fn of(services: &[ServiceInfo]) -> Self {
        let mut map = HashMap::new();
        for s in services.iter().filter(|s| s.key.protocol.eq_ignore_ascii_case("UDP")) {
            let key = format!("{}/{}:{}", s.key.namespace, s.key.name, s.key.port);
            let ready = s.endpoints.iter().filter(|e| e.ready).map(|e| e.ip.clone()).collect();
            map.insert(key, (s.key.cluster_ip.clone(), s.key.node_port, ready));
        }
        UdpBackends(map)
    }
}

/// The deletes that going from `before` to `after` calls for (module doc).
/// Sorted and without duplicates, so a pass is deterministic.
pub fn stale(before: &UdpBackends, after: &UdpBackends) -> Vec<Delete> {
    let mut out: BTreeSet<Delete> = BTreeSet::new();
    let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    for (key, (cluster_ip, node_port, now)) in &after.0 {
        let had = before.0.get(key).map(|(_, _, ips)| ips.clone()).unwrap_or_default();
        if had.is_empty() && !now.is_empty() {
            out.insert(args(&["-D", "-p", "udp", "--orig-dst", cluster_ip]));
        }
        for gone in had.difference(now) {
            out.insert(args(&["-D", "-p", "udp", "--orig-dst", cluster_ip, "--dst-nat", gone]));
            if let Some(np) = node_port {
                out.insert(args(&["-D", "-p", "udp", "--dport", &np.to_string(), "--dst-nat", gone]));
            }
        }
    }
    // A Service port that went away entirely: its endpoints are gone too.
    for (key, (cluster_ip, node_port, had)) in &before.0 {
        if after.0.contains_key(key) {
            continue;
        }
        for gone in had {
            out.insert(args(&["-D", "-p", "udp", "--orig-dst", cluster_ip, "--dst-nat", gone]));
            if let Some(np) = node_port {
                out.insert(args(&["-D", "-p", "udp", "--dport", &np.to_string(), "--dst-nat", gone]));
            }
        }
    }
    out.into_iter().collect()
}

/// The dataplane seam for conntrack, as [`crate::iptables::RuleApplier`] is
/// for the rules.
#[async_trait::async_trait]
pub trait Conntrack: Send + Sync {
    async fn delete(&self, args: &Delete);
}

/// The `conntrack` binary (conntrack-tools). "0 flow entries have been
/// deleted" exits 1 and is not a failure; a missing binary is said once and
/// then nothing, as upstream treats a node without it.
#[derive(Default)]
pub struct ConntrackTool {
    missing_said: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl Conntrack for ConntrackTool {
    async fn delete(&self, args: &Delete) {
        let a = args.clone();
        let run = tokio::task::spawn_blocking(move || std::process::Command::new("conntrack").args(&a).output()).await;
        match run {
            Ok(Ok(out)) if out.status.success() => {
                tracing::info!("conntrack {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim());
            }
            Ok(Ok(out)) => {
                let err = String::from_utf8_lossy(&out.stderr);
                if !err.contains("0 flow entries") {
                    tracing::warn!("conntrack {}: {}", args.join(" "), err.trim());
                }
            }
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                if !self.missing_said.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    tracing::warn!(
                        "no conntrack binary on this node: stale UDP flows to a replaced Service backend \
                         are not cleared and age out (30-120 s)"
                    );
                }
            }
            Ok(Err(e)) => tracing::warn!("conntrack {}: {e}", args.join(" ")),
            Err(e) => tracing::warn!("conntrack {}: {e}", args.join(" ")),
        }
    }
}

/// The platform default: the binary on Linux, nothing elsewhere.
pub fn default_conntrack() -> Box<dyn Conntrack> {
    #[cfg(target_os = "linux")]
    {
        Box::new(ConntrackTool::default())
    }
    #[cfg(not(target_os = "linux"))]
    {
        struct Nothing;
        #[async_trait::async_trait]
        impl Conntrack for Nothing {
            async fn delete(&self, _: &Delete) {}
        }
        Box::new(Nothing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_map::{Endpoint, ServiceKey};

    fn svc(proto: &str, node_port: Option<u16>, ips: &[&str]) -> ServiceInfo {
        ServiceInfo {
            key: ServiceKey {
                namespace: "kube-system".into(),
                name: "kube-dns".into(),
                cluster_ip: "10.96.0.10".into(),
                port: 53,
                protocol: proto.into(),
                node_port,
            },
            port_name: "dns".into(),
            endpoints: ips.iter().map(|ip| Endpoint { ip: ip.to_string(), port: 53, ready: true }).collect(),
            session_affinity: false,
        }
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn a_replaced_udp_backend_is_cleared_by_cluster_ip_and_node_port() {
        let before = UdpBackends::of(&[svc("UDP", Some(30053), &["10.0.1.5", "10.0.1.6"])]);
        let after = UdpBackends::of(&[svc("UDP", Some(30053), &["10.0.1.6", "10.0.1.9"])]);
        assert_eq!(
            stale(&before, &after),
            vec![
                s(&["-D", "-p", "udp", "--dport", "30053", "--dst-nat", "10.0.1.5"]),
                s(&["-D", "-p", "udp", "--orig-dst", "10.96.0.10", "--dst-nat", "10.0.1.5"]),
            ]
        );
    }

    #[test]
    fn a_first_endpoint_clears_the_cluster_ip_and_nothing_else_is_touched() {
        let before = UdpBackends::of(&[svc("UDP", None, &[])]);
        let after = UdpBackends::of(&[svc("UDP", None, &["10.0.1.5"])]);
        assert_eq!(stale(&before, &after), vec![s(&["-D", "-p", "udp", "--orig-dst", "10.96.0.10"])]);
        // Unchanged: nothing. TCP: never.
        assert!(stale(&after, &after).is_empty());
        let tcp_before = UdpBackends::of(&[svc("TCP", None, &["10.0.1.5"])]);
        let tcp_after = UdpBackends::of(&[svc("TCP", None, &["10.0.1.6"])]);
        assert!(stale(&tcp_before, &tcp_after).is_empty());
        // The first pass after a start knows nothing before: only "first endpoint" clears.
        assert_eq!(stale(&UdpBackends::default(), &after), vec![s(&["-D", "-p", "udp", "--orig-dst", "10.96.0.10"])]);
    }

    #[test]
    fn a_removed_udp_service_clears_its_backends() {
        let before = UdpBackends::of(&[svc("UDP", None, &["10.0.1.5"])]);
        assert_eq!(
            stale(&before, &UdpBackends::default()),
            vec![s(&["-D", "-p", "udp", "--orig-dst", "10.96.0.10", "--dst-nat", "10.0.1.5"])]
        );
    }
}
