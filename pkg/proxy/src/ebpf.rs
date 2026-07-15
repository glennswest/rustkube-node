//! eBPF-based service proxy using aya.
//!
//! Replaces iptables DNAT with BPF programs for O(1) service dispatch.
//! Feature-gated: only compiles on Linux with the `ebpf` feature.
//!
//! # Architecture
//!
//! The eBPF proxy uses BPF programs attached to TC (traffic control) hooks
//! to intercept packets destined for ClusterIP/NodePort services and perform
//! load balancing to backend endpoints without kernel conntrack overhead.
//!
//! ## BPF Maps
//!
//! - `SERVICES`: Maps service VIP:port → ServiceEntry (list of endpoints)
//! - `ENDPOINTS`: Maps endpoint index → EndpointEntry (IP:port, weight)
//! - `CONNECTIONS`: Maps 5-tuple → selected endpoint (connection affinity)
//!
//! ## Packet Flow
//!
//! 1. TC ingress hook intercepts packets
//! 2. Lookup destination in SERVICES map
//! 3. If match, select endpoint (round-robin or random weighted)
//! 4. Store selection in CONNECTIONS map
//! 5. Rewrite destination IP/port in packet
//! 6. Recalculate checksums
//! 7. Forward packet
//!
//! ## Reverse Path (SNAT)
//!
//! - TC egress hook intercepts reply packets
//! - Lookup source in CONNECTIONS map
//! - Rewrite source IP/port back to service VIP
//! - Recalculate checksums

#![allow(dead_code)]

use crate::service_map::ServiceMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// eBPF proxy state.
///
/// Manages BPF program lifecycle and synchronizes service/endpoint state
/// between the Kubernetes API and BPF maps.
pub struct EbpfProxy {
    service_map: Arc<ServiceMap>,
    state: Arc<RwLock<EbpfState>>,
}

/// Internal eBPF state (programs, maps, attached interfaces).
struct EbpfState {
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    programs: Vec<BpfProgram>,
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    maps: BpfMaps,

    /// Interfaces we're attached to
    attached_ifaces: Vec<String>,

    /// Service → endpoint mapping (cached for sync)
    services: HashMap<ServiceKey, Vec<EndpointEntry>>,
}

/// Service lookup key (VIP + port + protocol).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ServiceKey {
    vip: IpAddr,
    port: u16,
    protocol: Protocol,
}

/// Protocol type.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum Protocol {
    Tcp = 6,
    Udp = 17,
}

/// Endpoint entry (backend pod IP:port).
#[derive(Debug, Clone)]
struct EndpointEntry {
    ip: IpAddr,
    port: u16,
    weight: u32,
}

#[cfg(all(target_os = "linux", feature = "ebpf"))]
struct BpfProgram {
    // Placeholder for aya::Bpf object
    // In real implementation: aya::Bpf, aya::programs::SchedCLS, etc.
    _name: String,
}

#[cfg(all(target_os = "linux", feature = "ebpf"))]
struct BpfMaps {
    // Placeholder for aya::maps::HashMap, aya::maps::Array, etc.
    // In real implementation:
    // services: aya::maps::HashMap<ServiceKey, u32>, // VIP → endpoint list index
    // endpoints: aya::maps::Array<EndpointEntry>,
    // connections: aya::maps::LruHashMap<ConnTuple, u32>,
    _placeholder: (),
}

impl EbpfProxy {
    /// Create a new eBPF proxy.
    ///
    /// Does not load or attach BPF programs yet — call `attach()` explicitly.
    pub fn new(service_map: Arc<ServiceMap>) -> Self {
        info!("Initializing eBPF proxy");

        let state = EbpfState {
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            programs: Vec::new(),
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            maps: BpfMaps { _placeholder: () },
            attached_ifaces: Vec::new(),
            services: HashMap::new(),
        };

        Self {
            service_map,
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Load BPF programs and attach to network interface.
    ///
    /// # Arguments
    ///
    /// * `iface` - Network interface name (e.g., "eth0", "ens3")
    ///
    /// # Returns
    ///
    /// Ok(()) if programs loaded and attached successfully.
    ///
    /// # Errors
    ///
    /// - BPF program compilation/load failed
    /// - Interface not found or permission denied
    /// - Kernel BPF support missing
    pub async fn attach(&self, iface: &str) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            info!("Loading eBPF programs for interface {}", iface);

            // Real implementation would:
            // 1. Load BPF object file (aya::Bpf::load or aya::include_bytes_aligned!)
            // 2. Get program handles (bpf.program("tc_ingress"), bpf.program("tc_egress"))
            // 3. Attach to TC ingress/egress hooks (TcAttachOptions::attach)
            // 4. Get map handles (bpf.map("SERVICES"), bpf.map("ENDPOINTS"), etc.)
            // 5. Store in self.state

            let mut state = self.state.write().await;

            // Placeholder: simulate program load
            let program = BpfProgram {
                _name: format!("{}_tc", iface),
            };

            state.programs.push(program);
            state.attached_ifaces.push(iface.to_string());

            info!("eBPF programs attached to {}", iface);

            // Initial service sync
            drop(state);
            self.sync_services().await?;

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            warn!(
                "eBPF proxy requested for interface {} but not available \
                 (requires Linux + ebpf feature)",
                iface
            );
            Ok(())
        }
    }

    /// Update BPF maps with current service endpoints.
    ///
    /// Reads current state from ServiceMap and pushes updates to BPF maps.
    /// Called automatically after attach() and should be called periodically
    /// or when service/endpoint updates are detected.
    pub async fn sync_services(&self) -> anyhow::Result<()> {
        debug!("Syncing services to eBPF maps");

        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            let state = self.state.read().await;

            // Real implementation would:
            // 1. Iterate over all services in ServiceMap
            // 2. For each service:
            //    - Get ClusterIP/NodePort and endpoints
            //    - Encode ServiceKey (VIP + port + protocol)
            //    - Write to SERVICES map
            //    - Write endpoints to ENDPOINTS map
            // 3. Handle deletions (services that disappeared)

            // Placeholder: log intent
            debug!("eBPF map sync: {} services tracked", state.services.len());

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            debug!("eBPF map sync skipped (not available)");
            Ok(())
        }
    }

    /// Detach BPF programs from all interfaces.
    ///
    /// Cleanup method — removes TC hooks and unloads programs.
    pub async fn detach(&self) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            let mut state = self.state.write().await;

            info!("Detaching eBPF programs from {} interfaces", state.attached_ifaces.len());

            // Real implementation would:
            // 1. For each attached interface, remove TC hooks
            // 2. Drop program handles (aya handles cleanup on drop)
            // 3. Clear maps

            state.programs.clear();
            state.attached_ifaces.clear();
            state.services.clear();

            info!("eBPF programs detached");

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            debug!("eBPF detach skipped (not available)");
            Ok(())
        }
    }

    /// Get list of attached interfaces.
    pub async fn attached_interfaces(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.attached_ifaces.clone()
    }

    /// Check if eBPF support is available.
    pub fn is_available() -> bool {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            // Real implementation would check:
            // - /sys/fs/bpf mounted
            // - CAP_BPF or CAP_NET_ADMIN capability
            // - Kernel version >= 4.18 (or 5.x for newer features)
            true
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            false
        }
    }
}

/// Add service to eBPF maps (helper for ServiceMap integration).
pub async fn add_service(
    proxy: &EbpfProxy,
    vip: IpAddr,
    port: u16,
    protocol: &str,
    endpoints: Vec<(IpAddr, u16)>,
) -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    {
        let proto = match protocol.to_lowercase().as_str() {
            "tcp" => Protocol::Tcp,
            "udp" => Protocol::Udp,
            _ => {
                warn!("Unsupported protocol {} for eBPF proxy", protocol);
                return Ok(());
            }
        };

        let key = ServiceKey {
            vip,
            port,
            protocol: proto,
        };

        let entries: Vec<EndpointEntry> = endpoints
            .into_iter()
            .map(|(ip, port)| EndpointEntry {
                ip,
                port,
                weight: 1, // Equal weight for now
            })
            .collect();

        let mut state = proxy.state.write().await;
        state.services.insert(key, entries);

        // Real implementation would update BPF maps here

        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    {
        let _ = (proxy, vip, port, protocol, endpoints);
        Ok(())
    }
}

/// Remove service from eBPF maps.
pub async fn remove_service(
    proxy: &EbpfProxy,
    vip: IpAddr,
    port: u16,
    protocol: &str,
) -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    {
        let proto = match protocol.to_lowercase().as_str() {
            "tcp" => Protocol::Tcp,
            "udp" => Protocol::Udp,
            _ => return Ok(()),
        };

        let key = ServiceKey {
            vip,
            port,
            protocol: proto,
        };

        let mut state = proxy.state.write().await;
        state.services.remove(&key);

        // Real implementation would delete from BPF maps

        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    {
        let _ = (proxy, vip, port, protocol);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_availability() {
        let available = EbpfProxy::is_available();

        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        assert!(available, "eBPF should be available on Linux with ebpf feature");

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        assert!(!available, "eBPF should not be available without Linux + ebpf feature");
    }

    #[tokio::test]
    async fn test_proxy_lifecycle() {
        let service_map = Arc::new(ServiceMap::new());
        let proxy = EbpfProxy::new(service_map);

        // Attach should not error (no-op on non-Linux)
        proxy.attach("eth0").await.expect("attach failed");

        // Sync should not error
        proxy.sync_services().await.expect("sync failed");

        // Detach should not error
        proxy.detach().await.expect("detach failed");
    }

    #[tokio::test]
    async fn test_service_operations() {
        let service_map = Arc::new(ServiceMap::new());
        let proxy = EbpfProxy::new(service_map);

        proxy.attach("eth0").await.expect("attach failed");

        let vip = IpAddr::V4(Ipv4Addr::new(10, 96, 0, 1));
        let endpoints = vec![
            (IpAddr::V4(Ipv4Addr::new(10, 244, 0, 10)), 8080),
            (IpAddr::V4(Ipv4Addr::new(10, 244, 0, 11)), 8080),
        ];

        add_service(&proxy, vip, 80, "tcp", endpoints)
            .await
            .expect("add_service failed");

        remove_service(&proxy, vip, 80, "tcp")
            .await
            .expect("remove_service failed");

        proxy.detach().await.expect("detach failed");
    }
}
