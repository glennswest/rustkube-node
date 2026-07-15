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
    pub endpoints: Vec<Endpoint>,
    pub session_affinity: bool,
}

/// Thread-safe service map.
#[derive(Debug, Clone)]
pub struct ServiceMap {
    /// key: "namespace/name:port" → ServiceInfo
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
                let node_port = if svc_type == "NodePort" || svc_type == "LoadBalancer" {
                    port_spec["nodePort"].as_u64().map(|p| p as u16)
                } else {
                    None
                };

                let map_key = format!("{namespace}/{name}:{port}");
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
                        endpoints: existing_endpoints,
                        session_affinity,
                    },
                );
            }
        }

        // Remove services that no longer exist
        self.services.retain(|k, _| seen.contains(k));
    }

    /// Update endpoints from API server data.
    pub fn update_endpoints(&self, endpoints_list: &[Value]) {
        for ep in endpoints_list {
            let name = ep["metadata"]["name"].as_str().unwrap_or("");
            let namespace = ep["metadata"]["namespace"].as_str().unwrap_or("default");

            let subsets = ep["subsets"].as_array().cloned().unwrap_or_default();

            for subset in &subsets {
                let addresses = subset["addresses"].as_array().cloned().unwrap_or_default();
                let ports = subset["ports"].as_array().cloned().unwrap_or_default();

                for port_spec in &ports {
                    let port = port_spec["port"].as_u64().unwrap_or(0) as u16;
                    let svc_port = port_spec["port"].as_u64().unwrap_or(0) as u16;

                    // Find matching service entry
                    // Services use spec.ports[].port, endpoints use subsets[].ports[].port (target port)
                    let map_key = format!("{namespace}/{name}:{svc_port}");

                    // Also try with the service port (may differ from target port)
                    if let Some(mut svc_info) = self.services.get_mut(&map_key) {
                        svc_info.endpoints = addresses
                            .iter()
                            .map(|addr| Endpoint {
                                ip: addr["ip"].as_str().unwrap_or("").to_string(),
                                port,
                                ready: true,
                            })
                            .collect();
                    } else {
                        // Try to find by scanning all service ports for this namespace/name
                        for mut entry in self.services.iter_mut() {
                            if entry.key.namespace == namespace && entry.key.name == name {
                                entry.endpoints = addresses
                                    .iter()
                                    .map(|addr| Endpoint {
                                        ip: addr["ip"].as_str().unwrap_or("").to_string(),
                                        port,
                                        ready: true,
                                    })
                                    .collect();
                            }
                        }
                    }
                }
            }
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
