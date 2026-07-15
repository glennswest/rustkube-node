// NetworkPolicy enforcement for RustKube proxy
//
// Implements Kubernetes NetworkPolicy filtering with iptables rule generation.
// Default allow behavior unless a policy selects the pod, then default deny + explicit allow rules.

use serde_json::Value;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Direction of network traffic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDirection {
    Ingress,
    Egress,
}

/// Peer selector in a network policy rule
#[derive(Debug, Clone)]
pub struct PeerSelector {
    /// Pod label selector (matches pods in the same namespace unless namespace_selector is set)
    pub pod_selector: Option<HashMap<String, String>>,
    /// Namespace label selector
    pub namespace_selector: Option<HashMap<String, String>>,
    /// IP CIDR block
    pub ip_block: Option<IpBlock>,
}

/// IP CIDR block with optional exceptions
#[derive(Debug, Clone)]
pub struct IpBlock {
    pub cidr: String,
    pub except: Vec<String>,
}

/// Port and protocol specification
#[derive(Debug, Clone)]
pub struct PortRule {
    pub protocol: String, // TCP, UDP, SCTP
    pub port: Option<u16>,
    pub end_port: Option<u16>, // For port ranges (K8s 1.25+)
}

/// Parsed network policy rule
#[derive(Debug, Clone)]
pub struct NetworkPolicyRule {
    pub direction: PolicyDirection,
    pub peers: Vec<PeerSelector>,
    pub ports: Vec<PortRule>,
}

/// Fully parsed NetworkPolicy
#[derive(Debug, Clone)]
pub struct ParsedPolicy {
    pub name: String,
    pub namespace: String,
    pub pod_selector: HashMap<String, String>,
    pub policy_types: Vec<PolicyDirection>,
    pub ingress_rules: Vec<NetworkPolicyRule>,
    pub egress_rules: Vec<NetworkPolicyRule>,
}

/// NetworkPolicy enforcement engine
pub struct NetworkPolicyEngine {
    policies: Arc<RwLock<Vec<ParsedPolicy>>>,
}

impl NetworkPolicyEngine {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Load NetworkPolicies from K8s JSON format
    pub async fn load_policies(&self, policies: &[Value]) {
        let mut parsed = Vec::new();
        for policy in policies {
            if let Some(p) = parse_policy(policy) {
                parsed.push(p);
            }
        }
        let mut lock = self.policies.write().await;
        *lock = parsed;
        debug!("Loaded {} network policies", lock.len());
    }

    /// Check if traffic is allowed by network policies
    ///
    /// Returns true if traffic is allowed, false if denied.
    /// Default-allow if no policies select the destination pod.
    #[allow(clippy::too_many_arguments)]
    pub async fn is_traffic_allowed(
        &self,
        src_ip: &str,
        src_labels: &HashMap<String, String>,
        src_namespace: &str,
        dst_ip: &str,
        dst_labels: &HashMap<String, String>,
        dst_namespace: &str,
        dst_port: u16,
        protocol: &str,
    ) -> bool {
        let policies = self.policies.read().await;

        // Find policies that select the destination pod
        let applicable: Vec<&ParsedPolicy> = policies
            .iter()
            .filter(|p| {
                p.namespace == dst_namespace && matches_selector(dst_labels, &p.pod_selector)
            })
            .collect();

        if applicable.is_empty() {
            // No policy selects this pod → default allow
            return true;
        }

        // At least one policy selects this pod → default deny, check rules
        for policy in applicable {
            // Check if this policy has ingress rules
            if policy
                .policy_types
                .contains(&PolicyDirection::Ingress)
            {
                for rule in &policy.ingress_rules {
                    if self.rule_allows_traffic(
                        rule,
                        src_ip,
                        src_labels,
                        src_namespace,
                        dst_port,
                        protocol,
                    ) {
                        debug!(
                            "Traffic allowed by policy {}/{}: {}:{} → {}:{}",
                            policy.namespace, policy.name, src_ip, src_namespace, dst_ip, dst_port
                        );
                        return true;
                    }
                }
            }
        }

        debug!(
            "Traffic denied: {}:{} → {}:{} (port {}, proto {})",
            src_ip, src_namespace, dst_ip, dst_namespace, dst_port, protocol
        );
        false
    }

    /// Check if a single rule allows the traffic
    fn rule_allows_traffic(
        &self,
        rule: &NetworkPolicyRule,
        src_ip: &str,
        src_labels: &HashMap<String, String>,
        src_namespace: &str,
        dst_port: u16,
        protocol: &str,
    ) -> bool {
        // Check peer match (source)
        let peer_match = if rule.peers.is_empty() {
            // Empty peers → allow from anywhere
            true
        } else {
            rule.peers.iter().any(|peer| {
                self.peer_matches(peer, src_ip, src_labels, src_namespace)
            })
        };

        if !peer_match {
            return false;
        }

        // Check port match
        if rule.ports.is_empty() {
            // Empty ports → allow all ports/protocols
            return true;
        }

        rule.ports.iter().any(|port_rule| {
            let proto_match = port_rule.protocol.eq_ignore_ascii_case(protocol);
            let port_match = if let Some(port) = port_rule.port {
                if let Some(end_port) = port_rule.end_port {
                    dst_port >= port && dst_port <= end_port
                } else {
                    dst_port == port
                }
            } else {
                true // No port specified → all ports
            };
            proto_match && port_match
        })
    }

    /// Check if a peer selector matches the source
    fn peer_matches(
        &self,
        peer: &PeerSelector,
        src_ip: &str,
        src_labels: &HashMap<String, String>,
        src_namespace: &str,
    ) -> bool {
        // IP block match
        if let Some(ref ip_block) = peer.ip_block {
            if cidr_contains(&ip_block.cidr, src_ip) {
                // Check exceptions
                for except in &ip_block.except {
                    if cidr_contains(except, src_ip) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        // Pod/namespace selector match
        if let Some(ref pod_selector) = peer.pod_selector {
            if !matches_selector(src_labels, pod_selector) {
                return false;
            }
        }

        // Namespace selector (if present)
        if let Some(ref ns_selector) = peer.namespace_selector {
            // For simplicity, we'd need namespace labels here.
            // In real implementation, query API server for namespace labels.
            // For now, assume namespace name equals label "kubernetes.io/metadata.name"
            let mut ns_labels = HashMap::new();
            ns_labels.insert("kubernetes.io/metadata.name".to_string(), src_namespace.to_string());
            if !matches_selector(&ns_labels, ns_selector) {
                return false;
            }
        }

        true
    }

    /// Generate iptables rules for current policies
    ///
    /// Creates a RK-NETPOL chain with DROP default + ACCEPT rules for allowed traffic.
    pub async fn generate_iptables_rules(&self, pods: &[Value]) -> Vec<String> {
        let policies = self.policies.read().await;
        let mut rules = Vec::new();

        // Create RK-NETPOL chain
        rules.push("-N RK-NETPOL".to_string());
        rules.push("-F RK-NETPOL".to_string());

        // Jump to RK-NETPOL from FORWARD
        rules.push("-A FORWARD -j RK-NETPOL".to_string());

        for policy in policies.iter() {
            // Find pods selected by this policy
            for pod_val in pods {
                let pod_labels = extract_labels(pod_val);
                let pod_ns = extract_namespace(pod_val);
                let pod_ip = extract_pod_ip(pod_val);

                if pod_ns != policy.namespace {
                    continue;
                }
                if !matches_selector(&pod_labels, &policy.pod_selector) {
                    continue;
                }

                // This pod is selected by the policy
                if let Some(ip) = pod_ip {
                    // Default deny for this pod
                    if policy.policy_types.contains(&PolicyDirection::Ingress) {
                        rules.push(format!("-A RK-NETPOL -d {} -j DROP", ip));
                    }

                    // Add ACCEPT rules for each ingress rule
                    for rule in &policy.ingress_rules {
                        for peer in &rule.peers {
                            let src_spec = if let Some(ref ip_block) = peer.ip_block {
                                format!("-s {}", ip_block.cidr)
                            } else {
                                // Pod selector → would need pod IP lookup (simplified: allow all)
                                "".to_string()
                            };

                            for port_rule in &rule.ports {
                                let proto = port_rule.protocol.to_lowercase();
                                let port_spec = if let Some(port) = port_rule.port {
                                    format!("--dport {}", port)
                                } else {
                                    "".to_string()
                                };

                                rules.push(format!(
                                    "-A RK-NETPOL {} -d {} -p {} {} -j ACCEPT",
                                    src_spec, ip, proto, port_spec
                                ));
                            }

                            // If no ports specified, allow all
                            if rule.ports.is_empty() {
                                rules.push(format!(
                                    "-A RK-NETPOL {} -d {} -j ACCEPT",
                                    src_spec, ip
                                ));
                            }
                        }
                    }
                }
            }
        }

        rules
    }
}

impl Default for NetworkPolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse K8s NetworkPolicy JSON into ParsedPolicy
fn parse_policy(policy: &Value) -> Option<ParsedPolicy> {
    let metadata = policy.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    let spec = policy.get("spec")?;
    let pod_selector = spec.get("podSelector")?;
    let pod_selector_labels = extract_match_labels(pod_selector);

    let policy_types: Vec<PolicyDirection> = spec
        .get("policyTypes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| match v.as_str()? {
                    "Ingress" => Some(PolicyDirection::Ingress),
                    "Egress" => Some(PolicyDirection::Egress),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_else(|| vec![PolicyDirection::Ingress]); // Default to Ingress if not specified

    let ingress_rules = spec
        .get("ingress")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(parse_ingress_rule)
                .collect()
        })
        .unwrap_or_default();

    let egress_rules = spec
        .get("egress")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(parse_egress_rule)
                .collect()
        })
        .unwrap_or_default();

    Some(ParsedPolicy {
        name,
        namespace,
        pod_selector: pod_selector_labels,
        policy_types,
        ingress_rules,
        egress_rules,
    })
}

/// Parse an ingress rule
fn parse_ingress_rule(rule: &Value) -> Option<NetworkPolicyRule> {
    let peers = rule
        .get("from")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_peer).collect())
        .unwrap_or_default();

    let ports = rule
        .get("ports")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_port_rule).collect())
        .unwrap_or_default();

    Some(NetworkPolicyRule {
        direction: PolicyDirection::Ingress,
        peers,
        ports,
    })
}

/// Parse an egress rule
fn parse_egress_rule(rule: &Value) -> Option<NetworkPolicyRule> {
    let peers = rule
        .get("to")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_peer).collect())
        .unwrap_or_default();

    let ports = rule
        .get("ports")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_port_rule).collect())
        .unwrap_or_default();

    Some(NetworkPolicyRule {
        direction: PolicyDirection::Egress,
        peers,
        ports,
    })
}

/// Parse a peer selector (from/to)
fn parse_peer(peer: &Value) -> Option<PeerSelector> {
    let pod_selector = peer
        .get("podSelector")
        .map(extract_match_labels);

    let namespace_selector = peer
        .get("namespaceSelector")
        .map(extract_match_labels);

    let ip_block = peer.get("ipBlock").and_then(|block| {
        let cidr = block.get("cidr")?.as_str()?.to_string();
        let except = block
            .get("except")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Some(IpBlock { cidr, except })
    });

    Some(PeerSelector {
        pod_selector,
        namespace_selector,
        ip_block,
    })
}

/// Parse a port rule
fn parse_port_rule(port: &Value) -> Option<PortRule> {
    let protocol = port
        .get("protocol")
        .and_then(|v| v.as_str())
        .unwrap_or("TCP")
        .to_string();

    let port_num = port
        .get("port")
        .and_then(|v| v.as_u64())
        .map(|p| p as u16);

    let end_port = port
        .get("endPort")
        .and_then(|v| v.as_u64())
        .map(|p| p as u16);

    Some(PortRule {
        protocol,
        port: port_num,
        end_port,
    })
}

/// Extract matchLabels from a label selector
fn extract_match_labels(selector: &Value) -> HashMap<String, String> {
    selector
        .get("matchLabels")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    v.as_str().map(|s| (k.clone(), s.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Check if labels match a selector
fn matches_selector(labels: &HashMap<String, String>, selector: &HashMap<String, String>) -> bool {
    if selector.is_empty() {
        // Empty selector matches all
        return true;
    }

    for (key, value) in selector {
        if labels.get(key) != Some(value) {
            return false;
        }
    }
    true
}

/// Check if an IP is contained in a CIDR range
fn cidr_contains(cidr: &str, ip: &str) -> bool {
    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        warn!("Invalid CIDR format: {}", cidr);
        return false;
    }

    let network_addr = match parts[0].parse::<Ipv4Addr>() {
        Ok(addr) => addr,
        Err(_) => {
            warn!("Invalid network address in CIDR: {}", parts[0]);
            return false;
        }
    };

    let prefix_len: u8 = match parts[1].parse() {
        Ok(len) if len <= 32 => len,
        _ => {
            warn!("Invalid prefix length in CIDR: {}", parts[1]);
            return false;
        }
    };

    let test_addr = match ip.parse::<Ipv4Addr>() {
        Ok(addr) => addr,
        Err(_) => {
            warn!("Invalid IP address: {}", ip);
            return false;
        }
    };

    // Convert to u32 for bitwise operations
    let network = u32::from(network_addr);
    let test = u32::from(test_addr);

    // Create mask
    let mask = if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    };

    (network & mask) == (test & mask)
}

/// Extract labels from pod JSON
fn extract_labels(pod: &Value) -> HashMap<String, String> {
    pod.get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    v.as_str().map(|s| (k.clone(), s.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract namespace from pod JSON
fn extract_namespace(pod: &Value) -> String {
    pod.get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string()
}

/// Extract pod IP from pod JSON
fn extract_pod_ip(pod: &Value) -> Option<String> {
    pod.get("status")
        .and_then(|s| s.get("podIP"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cidr_contains() {
        assert!(cidr_contains("192.168.1.0/24", "192.168.1.100"));
        assert!(cidr_contains("192.168.1.0/24", "192.168.1.1"));
        assert!(cidr_contains("192.168.1.0/24", "192.168.1.254"));
        assert!(!cidr_contains("192.168.1.0/24", "192.168.2.1"));
        assert!(!cidr_contains("10.0.0.0/8", "11.0.0.1"));
        assert!(cidr_contains("10.0.0.0/8", "10.255.255.255"));
        assert!(cidr_contains("0.0.0.0/0", "1.2.3.4"));
        assert!(cidr_contains("172.16.0.0/12", "172.31.255.255"));
        assert!(!cidr_contains("172.16.0.0/12", "172.32.0.1"));
    }

    #[test]
    fn test_matches_selector() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "nginx".to_string());
        assert!(matches_selector(&labels, &selector));

        selector.insert("tier".to_string(), "backend".to_string());
        assert!(!matches_selector(&labels, &selector));

        let empty_selector = HashMap::new();
        assert!(matches_selector(&labels, &empty_selector));
    }

    #[tokio::test]
    async fn test_default_allow() {
        let engine = NetworkPolicyEngine::new();
        let mut src_labels = HashMap::new();
        src_labels.insert("app".to_string(), "client".to_string());
        let mut dst_labels = HashMap::new();
        dst_labels.insert("app".to_string(), "server".to_string());

        // No policies → default allow
        assert!(
            engine
                .is_traffic_allowed(
                    "10.0.0.1",
                    &src_labels,
                    "default",
                    "10.0.0.2",
                    &dst_labels,
                    "default",
                    80,
                    "TCP"
                )
                .await
        );
    }

    #[test]
    fn test_parse_peer() {
        let json = serde_json::json!({
            "ipBlock": {
                "cidr": "192.168.1.0/24",
                "except": ["192.168.1.100/32"]
            }
        });

        let peer = parse_peer(&json).unwrap();
        assert!(peer.ip_block.is_some());
        let ip_block = peer.ip_block.unwrap();
        assert_eq!(ip_block.cidr, "192.168.1.0/24");
        assert_eq!(ip_block.except, vec!["192.168.1.100/32"]);
    }

    #[test]
    fn test_parse_port_rule() {
        let json = serde_json::json!({
            "protocol": "TCP",
            "port": 80
        });

        let port = parse_port_rule(&json).unwrap();
        assert_eq!(port.protocol, "TCP");
        assert_eq!(port.port, Some(80));
        assert_eq!(port.end_port, None);
    }
}
