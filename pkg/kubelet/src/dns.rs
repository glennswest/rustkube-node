//! The pod's resolver configuration.
//!
//! **No pod had an `/etc/resolv.conf`.** Not an empty one — the file did not
//! exist, so every name lookup inside every container fell back to
//! `127.0.0.1`, got connection refused, and reported "no servers could be
//! reached". Cluster DNS was running and reachable the whole time; nothing
//! ever told a pod where it was.
//!
//! Upstream generates this content and hands it to the runtime through the
//! CRI sandbox config. Here it is written to a file under the pod's directory
//! and bind-mounted, because that is the mechanism this runtime has, and the
//! path is one both the kubelet and the engine can see.

/// What a pod's `dnsPolicy` asks for.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DnsPolicy {
    /// Cluster DNS first, with the search path that makes short names work.
    ClusterFirst,
    /// The node's own resolver, unchanged.
    Default,
    /// Only what `dnsConfig` says.
    None,
}

impl DnsPolicy {
    pub fn parse(policy: Option<&str>, host_network: bool) -> Self {
        match policy {
            Some("Default") => DnsPolicy::Default,
            Some("None") => DnsPolicy::None,
            Some("ClusterFirstWithHostNet") => DnsPolicy::ClusterFirst,
            // The default is ClusterFirst — except on host networking, where
            // upstream downgrades it to Default unless the pod explicitly
            // asked for ClusterFirstWithHostNet. A host-network pod sharing
            // the node's resolver is what "Default" means.
            Some("ClusterFirst") => DnsPolicy::ClusterFirst,
            _ if host_network => DnsPolicy::Default,
            _ => DnsPolicy::ClusterFirst,
        }
    }
}

/// Build a resolv.conf for a pod.
///
/// `ndots:5` is not arbitrary: it is what makes `neta`, `neta.default` and
/// `neta.default.svc` all resolve, by forcing the search path to be tried
/// before the name is treated as absolute. It is also why `autopath` exists —
/// the cost is up to five queries for every name that is *not* cluster-local.
pub fn resolv_conf(
    policy: DnsPolicy,
    namespace: &str,
    cluster_dns: &[String],
    cluster_domain: &str,
    dns_config: Option<&serde_json::Value>,
    node_resolv: &str,
) -> String {
    let mut out = String::new();
    let (mut servers, mut searches, mut options): (Vec<String>, Vec<String>, Vec<String>) =
        (Vec::new(), Vec::new(), Vec::new());

    match policy {
        DnsPolicy::ClusterFirst => {
            servers.extend(cluster_dns.iter().cloned());
            searches.push(format!("{namespace}.svc.{cluster_domain}"));
            searches.push(format!("svc.{cluster_domain}"));
            searches.push(cluster_domain.to_string());
            options.push("ndots:5".to_string());
        }
        // The node's file, as it stands. Returned whole so that anything the
        // node's resolver was told — a search domain from DHCP, an option —
        // reaches the pod too.
        DnsPolicy::Default => return node_resolv.to_string(),
        DnsPolicy::None => {}
    }

    // dnsConfig adds to ClusterFirst and *is* the configuration for None.
    if let Some(cfg) = dns_config {
        if let Some(ns) = cfg["nameservers"].as_array() {
            servers.extend(ns.iter().filter_map(|v| v.as_str()).map(str::to_string));
        }
        if let Some(se) = cfg["searches"].as_array() {
            searches.extend(se.iter().filter_map(|v| v.as_str()).map(str::to_string));
        }
        if let Some(op) = cfg["options"].as_array() {
            for o in op {
                let name = o["name"].as_str().unwrap_or("");
                if name.is_empty() {
                    continue;
                }
                match o["value"].as_str() {
                    Some(v) => options.push(format!("{name}:{v}")),
                    None => options.push(name.to_string()),
                }
            }
        }
    }

    for s in &servers {
        out.push_str(&format!("nameserver {s}\n"));
    }
    if !searches.is_empty() {
        out.push_str(&format!("search {}\n", searches.join(" ")));
    }
    if !options.is_empty() {
        out.push_str(&format!("options {}\n", options.join(" ")));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cluster_first_points_at_cluster_dns_with_the_search_path() {
        let c = resolv_conf(
            DnsPolicy::ClusterFirst,
            "default",
            &["10.96.0.10".to_string()],
            "cluster.local",
            None,
            "nameserver 192.168.8.252\n",
        );
        assert!(c.contains("nameserver 10.96.0.10"), "{c}");
        // The order matters: the pod's own namespace is tried first, which is
        // what makes a bare Service name resolve.
        assert!(
            c.contains("search default.svc.cluster.local svc.cluster.local cluster.local"),
            "{c}"
        );
        assert!(c.contains("options ndots:5"), "{c}");
    }

    /// Default hands back the node's file unchanged — including whatever the
    /// node was told by DHCP, which a reconstructed file would lose.
    #[test]
    fn default_policy_is_the_nodes_own_file() {
        let node = "nameserver 192.168.8.252\nsearch g8.lo\n";
        let c = resolv_conf(DnsPolicy::Default, "default", &[], "cluster.local", None, node);
        assert_eq!(c, node);
    }

    #[test]
    fn none_takes_only_what_dns_config_says() {
        let cfg = json!({
            "nameservers": ["1.1.1.1"],
            "searches": ["example.com"],
            "options": [{"name": "ndots", "value": "2"}, {"name": "edns0"}]
        });
        let c = resolv_conf(
            DnsPolicy::None, "default", &["10.96.0.10".to_string()],
            "cluster.local", Some(&cfg), "",
        );
        assert!(c.contains("nameserver 1.1.1.1"));
        // Cluster DNS is not added under None, which is the whole point.
        assert!(!c.contains("10.96.0.10"), "{c}");
        assert!(c.contains("search example.com"));
        assert!(c.contains("options ndots:2 edns0"), "{c}");
    }

    /// A host-network pod uses the node's resolver unless it says otherwise,
    /// because it *is* on the node's network.
    #[test]
    fn host_network_downgrades_to_default_unless_asked() {
        assert_eq!(DnsPolicy::parse(None, true), DnsPolicy::Default);
        assert_eq!(DnsPolicy::parse(None, false), DnsPolicy::ClusterFirst);
        assert_eq!(
            DnsPolicy::parse(Some("ClusterFirstWithHostNet"), true),
            DnsPolicy::ClusterFirst
        );
    }
}
