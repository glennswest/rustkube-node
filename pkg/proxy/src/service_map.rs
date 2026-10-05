//! Service → Endpoints mapping.
//!
//! Maintains the mapping from Service ClusterIP:port to backend pod endpoints.
//! Updated by watching Services and Endpoints from the API server.

use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;

/// A service's virtual IP and port.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ServiceKey {
    pub namespace: String,
    pub name: String,
    pub cluster_ip: String,
    pub port: u16,
    pub protocol: String,
    pub node_port: Option<u16>,
}

/// A backend endpoint (pod IP:port).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Endpoint {
    pub ip: String,
    pub port: u16,
    pub ready: bool,
}

/// Service info with its backends.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub key: ServiceKey,
    /// `spec.ports[].name`: an Endpoints port is matched to its Service port
    /// by name (and protocol), as upstream does; empty for a lone unnamed port.
    pub port_name: String,
    pub endpoints: Vec<Endpoint>,
    pub session_affinity: bool,
}

/// Thread-safe service map.
#[derive(Debug, Clone)]
pub struct ServiceMap {
    /// key: "namespace/name:port/protocol" → ServiceInfo
    services: Arc<DashMap<String, ServiceInfo>>,
}

impl Default for ServiceMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceMap {
    pub fn new() -> Self {
        Self {
            services: Arc::new(DashMap::new()),
        }
    }

    /// Update services from API server data.
    pub fn update_services(&self, services: &[Value]) {
        let mut seen = std::collections::HashSet::new();

        for svc in services {
            let name = svc["metadata"]["name"].as_str().unwrap_or("");
            let namespace = svc["metadata"]["namespace"].as_str().unwrap_or("default");
            let cluster_ip = svc["spec"]["clusterIP"].as_str().unwrap_or("");

            // Skip headless services (ClusterIP: None)
            if cluster_ip.is_empty() || cluster_ip == "None" {
                continue;
            }

            let svc_type = svc["spec"]["type"].as_str().unwrap_or("ClusterIP");
            let session_affinity = svc["spec"]["sessionAffinity"].as_str() == Some("ClientIP");

            let ports = svc["spec"]["ports"].as_array().cloned().unwrap_or_default();

            for port_spec in &ports {
                let port = port_spec["port"].as_u64().unwrap_or(0) as u16;
                let protocol = port_spec["protocol"].as_str().unwrap_or("TCP").to_string();
                let port_name = port_spec["name"].as_str().unwrap_or("").to_string();
                let node_port = if svc_type == "NodePort" || svc_type == "LoadBalancer" {
                    port_spec["nodePort"].as_u64().map(|p| p as u16)
                } else {
                    None
                };

                // Protocol is part of the key: kube-dns serves 53/UDP and 53/TCP.
                let map_key = format!("{namespace}/{name}:{port}/{protocol}");
                seen.insert(map_key.clone());

                let key = ServiceKey {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    cluster_ip: cluster_ip.to_string(),
                    port,
                    protocol,
                    node_port,
                };

                // Preserve existing endpoints if we already have them
                let existing_endpoints = self
                    .services
                    .get(&map_key)
                    .map(|s| s.endpoints.clone())
                    .unwrap_or_default();

                self.services.insert(
                    map_key,
                    ServiceInfo {
                        key,
                        port_name,
                        endpoints: existing_endpoints,
                        session_affinity,
                    },
                );
            }
        }

        // Remove services that no longer exist
        self.services.retain(|k, _| seen.contains(k));
    }

    /// Set every Service port's backends from the Endpoints list, as it is
    /// now: a subset port is matched to the Service port of the same name and
    /// protocol (its number is the target port, not the Service port), every
    /// subset contributes, and a port with no Endpoints left has none.
    pub fn update_endpoints(&self, endpoints_list: &[Value]) {
        use std::collections::HashMap;
        // (namespace, name, port name, protocol) → backends
        let mut found: HashMap<(String, String, String, String), Vec<Endpoint>> = HashMap::new();
        for ep in endpoints_list {
            let name = ep["metadata"]["name"].as_str().unwrap_or("");
            let namespace = ep["metadata"]["namespace"].as_str().unwrap_or("default");
            for subset in ep["subsets"].as_array().map(Vec::as_slice).unwrap_or_default() {
                let addresses = subset["addresses"].as_array().map(Vec::as_slice).unwrap_or_default();
                for port_spec in subset["ports"].as_array().map(Vec::as_slice).unwrap_or_default() {
                    let Some(port) = port_spec["port"].as_u64().map(|p| p as u16) else {
                        continue;
                    };
                    let key = (
                        namespace.to_string(),
                        name.to_string(),
                        port_spec["name"].as_str().unwrap_or("").to_string(),
                        port_spec["protocol"].as_str().unwrap_or("TCP").to_string(),
                    );
                    let list = found.entry(key).or_default();
                    for addr in addresses {
                        let Some(ip) = addr["ip"].as_str() else { continue };
                        let e = Endpoint { ip: ip.to_string(), port, ready: true };
                        if !list.contains(&e) {
                            list.push(e);
                        }
                    }
                }
            }
        }
        for mut entry in self.services.iter_mut() {
            let key = (
                entry.key.namespace.clone(),
                entry.key.name.clone(),
                entry.port_name.clone(),
                entry.key.protocol.clone(),
            );
            entry.endpoints = found.get(&key).cloned().unwrap_or_default();
        }
    }

    /// Get all service infos for generating proxy rules.
    pub fn get_all(&self) -> Vec<ServiceInfo> {
        self.services.iter().map(|e| e.value().clone()).collect()
    }

    /// Pick a random backend for a service.
    pub fn pick_endpoint(&self, cluster_ip: &str, port: u16) -> Option<Endpoint> {
        for entry in self.services.iter() {
            let info = entry.value();
            if info.key.cluster_ip == cluster_ip && info.key.port == port {
                let ready: Vec<_> = info.endpoints.iter().filter(|e| e.ready).collect();
                if ready.is_empty() {
                    return None;
                }
                let idx = rand::random::<usize>() % ready.len();
                return Some(ready[idx].clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn by_port(m: &ServiceMap, port: u16, proto: &str) -> ServiceInfo {
        m.get_all()
            .into_iter()
            .find(|s| s.key.port == port && s.key.protocol == proto)
            .unwrap()
    }

    #[test]
    fn kube_dns_udp_and_tcp_both_kept_and_matched_by_name() {
        let m = ServiceMap::new();
        m.update_services(&[json!({
            "metadata": {"name": "kube-dns", "namespace": "kube-system"},
            "spec": {"clusterIP": "10.96.0.10", "ports": [
                {"name": "dns", "port": 53, "protocol": "UDP", "targetPort": 53},
                {"name": "dns-tcp", "port": 53, "protocol": "TCP", "targetPort": 53},
                {"name": "metrics", "port": 9153, "protocol": "TCP", "targetPort": 9153}
            ]}
        })]);
        assert_eq!(m.get_all().len(), 3);
        m.update_endpoints(&[json!({
            "metadata": {"name": "kube-dns", "namespace": "kube-system"},
            "subsets": [{"addresses": [{"ip": "10.244.0.7"}], "ports": [
                {"name": "dns", "port": 53, "protocol": "UDP"},
                {"name": "dns-tcp", "port": 53, "protocol": "TCP"},
                {"name": "metrics", "port": 9153, "protocol": "TCP"}
            ]}]
        })]);
        for (port, proto) in [(53, "UDP"), (53, "TCP"), (9153, "TCP")] {
            let s = by_port(&m, port, proto);
            assert_eq!(s.endpoints, vec![Endpoint { ip: "10.244.0.7".into(), port, ready: true }]);
        }
    }

    #[test]
    fn target_port_differs_from_service_port() {
        let m = ServiceMap::new();
        m.update_services(&[json!({
            "metadata": {"name": "kubernetes", "namespace": "default"},
            "spec": {"clusterIP": "10.96.0.1", "ports": [
                {"name": "https", "port": 443, "protocol": "TCP", "targetPort": 6443}
            ]}
        })]);
        m.update_endpoints(&[json!({
            "metadata": {"name": "kubernetes", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "192.168.11.5"}],
                         "ports": [{"name": "https", "port": 6443, "protocol": "TCP"}]}]
        })]);
        let s = by_port(&m, 443, "TCP");
        assert_eq!(s.endpoints[0].ip, "192.168.11.5");
        assert_eq!(s.endpoints[0].port, 6443);
    }

    #[test]
    fn multi_port_targets_are_not_crossed_and_vanished_endpoints_clear() {
        let m = ServiceMap::new();
        let svc = json!({
            "metadata": {"name": "web", "namespace": "ns"},
            "spec": {"clusterIP": "10.96.1.1", "ports": [
                {"name": "http", "port": 80, "protocol": "TCP", "targetPort": 8080},
                {"name": "https", "port": 443, "protocol": "TCP", "targetPort": 8443}
            ]}
        });
        m.update_services(std::slice::from_ref(&svc));
        m.update_endpoints(&[json!({
            "metadata": {"name": "web", "namespace": "ns"},
            "subsets": [{"addresses": [{"ip": "10.244.0.3"}, {"ip": "10.244.0.4"}], "ports": [
                {"name": "http", "port": 8080, "protocol": "TCP"},
                {"name": "https", "port": 8443, "protocol": "TCP"}
            ]}]
        })]);
        assert!(by_port(&m, 80, "TCP").endpoints.iter().all(|e| e.port == 8080));
        assert!(by_port(&m, 443, "TCP").endpoints.iter().all(|e| e.port == 8443));
        assert_eq!(by_port(&m, 80, "TCP").endpoints.len(), 2);
        // The Endpoints object goes: the Service keeps no stale backends.
        m.update_services(std::slice::from_ref(&svc));
        m.update_endpoints(&[]);
        assert!(by_port(&m, 80, "TCP").endpoints.is_empty());
    }
}
