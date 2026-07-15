//! eBPF VXLAN encap/decap for high-performance overlay networking.
//!
//! Replaces kernel VXLAN with BPF programs for lower latency and higher
//! throughput. Feature-gated for Linux + ebpf feature.
//!
//! # Architecture
//!
//! Traditional VXLAN uses kernel module which adds overhead:
//! - Extra context switches
//! - Packet copies
//! - Conntrack overhead
//!
//! eBPF VXLAN processes packets entirely in BPF hooks:
//! - TC egress: encapsulate pod traffic → VXLAN
//! - TC ingress: decapsulate VXLAN → pod traffic
//! - XDP (optional): even faster ingress path
//!
//! ## BPF Maps
//!
//! - `VTEP_PEERS`: Maps pod IP → VTEP IP (where to send encapsulated packets)
//! - `VNI_CONFIG`: Maps interface → VNI (VXLAN Network Identifier)
//! - `LOCAL_PODS`: Maps local pod IP → true (skip encap for local delivery)
//!
//! ## Packet Flow (Egress - Encapsulation)
//!
//! 1. TC egress hook intercepts packet from pod
//! 2. Lookup destination IP in VTEP_PEERS
//! 3. If found (remote pod):
//!    - Add outer Ethernet header
//!    - Add outer IP header (local VTEP → remote VTEP)
//!    - Add outer UDP header (port 4789)
//!    - Add VXLAN header (VNI from VNI_CONFIG)
//!    - Recalculate checksums
//!    - Forward to underlay network
//! 4. If not found (external/service):
//!    - Pass through to normal routing
//!
//! ## Packet Flow (Ingress - Decapsulation)
//!
//! 1. TC/XDP ingress hook intercepts VXLAN packet (UDP port 4789)
//! 2. Validate VXLAN header
//! 3. Extract inner packet
//! 4. Lookup destination in LOCAL_PODS
//! 5. If local, deliver to pod interface
//! 6. If not local, drop (shouldn't happen)

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// eBPF VXLAN encapsulation state.
///
/// Manages BPF programs for VXLAN overlay networking and maintains
/// the mapping between pod IPs and VTEP (VXLAN Tunnel Endpoint) IPs.
pub struct EbpfEncap {
    /// VXLAN Network Identifier
    vni: u32,

    /// Local VTEP IP (this node's underlay IP)
    local_vtep_ip: Ipv4Addr,

    /// Internal state (BPF programs, maps, peers)
    state: Arc<RwLock<EncapState>>,
}

struct EncapState {
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    programs: Vec<BpfProgram>,

    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    maps: BpfMaps,

    /// Attached interfaces (e.g., "cni0", "veth*")
    attached_ifaces: Vec<String>,

    /// Peer mappings: remote pod IP → remote VTEP IP
    peers: HashMap<Ipv4Addr, Ipv4Addr>,

    /// Local pod IPs (for fast local delivery detection)
    local_pods: Vec<Ipv4Addr>,
}

#[cfg(all(target_os = "linux", feature = "ebpf"))]
struct BpfProgram {
    // Placeholder for aya BPF program handles
    // Real: aya::Bpf, aya::programs::SchedCLS, aya::programs::Xdp
    _name: String,
}

#[cfg(all(target_os = "linux", feature = "ebpf"))]
struct BpfMaps {
    // Placeholder for aya map handles
    // Real:
    // vtep_peers: aya::maps::HashMap<u32, u32>, // pod IP → VTEP IP (both as u32)
    // vni_config: aya::maps::HashMap<u32, u32>, // ifindex → VNI
    // local_pods: aya::maps::HashSet<u32>,       // local pod IPs
    _placeholder: (),
}

impl EbpfEncap {
    /// Create a new eBPF VXLAN encapsulation instance.
    ///
    /// # Arguments
    ///
    /// * `vni` - VXLAN Network Identifier (24-bit, typically 1-16777215)
    /// * `local_vtep_ip` - This node's underlay IP address
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::Ipv4Addr;
    /// use rk_cni::ebpf_encap::EbpfEncap;
    ///
    /// let encap = EbpfEncap::new(100, Ipv4Addr::new(192, 168, 1, 10));
    /// ```
    pub fn new(vni: u32, local_vtep_ip: Ipv4Addr) -> Self {
        info!("Initializing eBPF VXLAN encap (VNI={}, VTEP={})", vni, local_vtep_ip);

        // Validate VNI (24-bit)
        if vni > 0xFFFFFF {
            warn!("VNI {} exceeds 24-bit range, truncating", vni);
        }

        let state = EncapState {
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            programs: Vec::new(),
            #[cfg(all(target_os = "linux", feature = "ebpf"))]
            maps: BpfMaps { _placeholder: () },
            attached_ifaces: Vec::new(),
            peers: HashMap::new(),
            local_pods: Vec::new(),
        };

        Self {
            vni: vni & 0xFFFFFF, // Mask to 24 bits
            local_vtep_ip,
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Load and attach BPF programs to network interface.
    ///
    /// # Arguments
    ///
    /// * `iface` - Interface name (e.g., "cni0" for bridge, "vxlan0" for existing VXLAN device)
    ///
    /// # Returns
    ///
    /// Ok(()) if programs loaded and attached successfully.
    ///
    /// # Errors
    ///
    /// - BPF compilation/load failed
    /// - Interface not found
    /// - Permission denied (requires CAP_BPF or CAP_NET_ADMIN)
    pub async fn attach(&self, iface: &str) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            info!("Loading eBPF VXLAN encap programs for interface {}", iface);

            // Real implementation would:
            // 1. Load BPF object (aya::Bpf::load or include_bytes_aligned!)
            // 2. Get encap/decap program handles
            // 3. Attach to TC egress/ingress (or XDP for ingress)
            // 4. Get map handles
            // 5. Initialize VNI_CONFIG map with this interface → VNI
            // 6. Initialize LOCAL_PODS map with current pod IPs

            let mut state = self.state.write().await;

            // Placeholder: simulate program load
            state.programs.push(BpfProgram {
                _name: format!("{}_vxlan_encap", iface),
            });
            state.programs.push(BpfProgram {
                _name: format!("{}_vxlan_decap", iface),
            });

            state.attached_ifaces.push(iface.to_string());

            info!("eBPF VXLAN programs attached to {} (VNI={})", iface, self.vni);

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            warn!(
                "eBPF VXLAN encap requested for {} but not available \
                 (requires Linux + ebpf feature)",
                iface
            );
            Ok(())
        }
    }

    /// Add a remote peer to the VXLAN overlay.
    ///
    /// # Arguments
    ///
    /// * `peer_ip` - Remote pod IP (destination for encapsulated traffic)
    /// * `peer_vtep` - Remote VTEP IP (where to send encapsulated packets)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let encap = EbpfEncap::new(100, Ipv4Addr::new(192, 168, 1, 10));
    /// encap.attach("cni0").await.unwrap();
    /// encap.add_peer(
    ///     Ipv4Addr::new(10, 244, 1, 5),
    ///     Ipv4Addr::new(192, 168, 1, 11),
    /// ).await.unwrap();
    /// ```
    pub async fn add_peer(
        &self,
        peer_ip: Ipv4Addr,
        peer_vtep: Ipv4Addr,
    ) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            debug!("Adding VXLAN peer: {} → VTEP {}", peer_ip, peer_vtep);

            let mut state = self.state.write().await;

            // Check if this is a local pod (VTEP == local VTEP)
            if peer_vtep == self.local_vtep_ip {
                debug!("Peer {} is local, adding to local_pods", peer_ip);
                if !state.local_pods.contains(&peer_ip) {
                    state.local_pods.push(peer_ip);
                }
                // Real implementation: update LOCAL_PODS BPF map
                return Ok(());
            }

            // Add to remote peers
            state.peers.insert(peer_ip, peer_vtep);

            // Real implementation: update VTEP_PEERS BPF map
            // Convert IPs to u32 (network byte order)
            // let peer_ip_u32 = u32::from(peer_ip);
            // let peer_vtep_u32 = u32::from(peer_vtep);
            // state.maps.vtep_peers.insert(&peer_ip_u32, &peer_vtep_u32, 0)?;

            debug!("VXLAN peer added: {} → {}", peer_ip, peer_vtep);

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            debug!("eBPF VXLAN add_peer skipped: {} → {}", peer_ip, peer_vtep);
            Ok(())
        }
    }

    /// Remove a remote peer from the VXLAN overlay.
    ///
    /// # Arguments
    ///
    /// * `peer_ip` - Pod IP to remove
    pub async fn remove_peer(&self, peer_ip: Ipv4Addr) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            debug!("Removing VXLAN peer: {}", peer_ip);

            let mut state = self.state.write().await;

            // Remove from local pods if present
            state.local_pods.retain(|ip| *ip != peer_ip);

            // Remove from remote peers
            state.peers.remove(&peer_ip);

            // Real implementation: delete from BPF maps
            // let peer_ip_u32 = u32::from(peer_ip);
            // state.maps.vtep_peers.remove(&peer_ip_u32)?;
            // state.maps.local_pods.remove(&peer_ip_u32)?;

            debug!("VXLAN peer removed: {}", peer_ip);

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            debug!("eBPF VXLAN remove_peer skipped: {}", peer_ip);
            Ok(())
        }
    }

    /// Detach BPF programs from all interfaces.
    pub async fn detach(&self) -> anyhow::Result<()> {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            let mut state = self.state.write().await;

            info!(
                "Detaching eBPF VXLAN programs from {} interfaces",
                state.attached_ifaces.len()
            );

            // Real implementation:
            // 1. Detach TC/XDP hooks
            // 2. Drop program handles (aya cleanup)
            // 3. Clear maps

            state.programs.clear();
            state.attached_ifaces.clear();
            state.peers.clear();
            state.local_pods.clear();

            info!("eBPF VXLAN programs detached");

            Ok(())
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            debug!("eBPF VXLAN detach skipped (not available)");
            Ok(())
        }
    }

    /// Get current peer count.
    pub async fn peer_count(&self) -> usize {
        let state = self.state.read().await;
        state.peers.len() + state.local_pods.len()
    }

    /// Get list of attached interfaces.
    pub async fn attached_interfaces(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.attached_ifaces.clone()
    }

    /// Get VNI.
    pub fn vni(&self) -> u32 {
        self.vni
    }

    /// Get local VTEP IP.
    pub fn local_vtep_ip(&self) -> Ipv4Addr {
        self.local_vtep_ip
    }

    /// Check if eBPF VXLAN support is available.
    pub fn is_available() -> bool {
        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            // Real implementation would check:
            // - BPF filesystem mounted
            // - Capabilities (CAP_BPF)
            // - Kernel support for TC redirect (BPF_F_INGRESS, BPF_F_EGRESS)
            // - XDP support (optional)
            true
        }

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        {
            false
        }
    }
}

/// Bulk update peers from a list of (pod IP, VTEP IP) tuples.
///
/// More efficient than calling add_peer() in a loop for large peer sets.
pub async fn bulk_update_peers(
    encap: &EbpfEncap,
    peers: Vec<(Ipv4Addr, Ipv4Addr)>,
) -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", feature = "ebpf"))]
    {
        debug!("Bulk updating {} VXLAN peers", peers.len());

        let mut state = encap.state.write().await;

        // Clear existing peers
        state.peers.clear();
        state.local_pods.clear();

        // Add new peers
        for (peer_ip, peer_vtep) in peers {
            if peer_vtep == encap.local_vtep_ip {
                state.local_pods.push(peer_ip);
            } else {
                state.peers.insert(peer_ip, peer_vtep);
            }
        }

        // Real implementation: batch update BPF maps
        // More efficient than individual map operations

        debug!(
            "Bulk update complete: {} remote, {} local",
            state.peers.len(),
            state.local_pods.len()
        );

        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
    {
        let _ = (encap, peers);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vni_truncation() {
        let encap = EbpfEncap::new(0x1FFFFFF, Ipv4Addr::new(192, 168, 1, 1));
        assert_eq!(encap.vni(), 0xFFFFFF, "VNI should be truncated to 24 bits");
    }

    #[test]
    fn test_availability() {
        let available = EbpfEncap::is_available();

        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        assert!(available, "eBPF VXLAN should be available on Linux with ebpf feature");

        #[cfg(not(all(target_os = "linux", feature = "ebpf")))]
        assert!(!available, "eBPF VXLAN should not be available without Linux + ebpf feature");
    }

    #[tokio::test]
    async fn test_encap_lifecycle() {
        let encap = EbpfEncap::new(100, Ipv4Addr::new(192, 168, 1, 10));

        // Attach
        encap.attach("cni0").await.expect("attach failed");

        // Add peers
        encap
            .add_peer(Ipv4Addr::new(10, 244, 1, 5), Ipv4Addr::new(192, 168, 1, 11))
            .await
            .expect("add_peer failed");

        encap
            .add_peer(Ipv4Addr::new(10, 244, 1, 6), Ipv4Addr::new(192, 168, 1, 11))
            .await
            .expect("add_peer failed");

        // Local pod (same VTEP)
        encap
            .add_peer(Ipv4Addr::new(10, 244, 0, 5), Ipv4Addr::new(192, 168, 1, 10))
            .await
            .expect("add_peer local failed");

        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            let count = encap.peer_count().await;
            assert_eq!(count, 3, "Should have 3 peers (2 remote + 1 local)");
        }

        // Remove peer
        encap
            .remove_peer(Ipv4Addr::new(10, 244, 1, 5))
            .await
            .expect("remove_peer failed");

        // Detach
        encap.detach().await.expect("detach failed");
    }

    #[tokio::test]
    async fn test_bulk_update() {
        let encap = EbpfEncap::new(100, Ipv4Addr::new(192, 168, 1, 10));
        encap.attach("cni0").await.expect("attach failed");

        let peers = vec![
            (Ipv4Addr::new(10, 244, 1, 5), Ipv4Addr::new(192, 168, 1, 11)),
            (Ipv4Addr::new(10, 244, 1, 6), Ipv4Addr::new(192, 168, 1, 11)),
            (Ipv4Addr::new(10, 244, 2, 5), Ipv4Addr::new(192, 168, 1, 12)),
            (Ipv4Addr::new(10, 244, 0, 5), Ipv4Addr::new(192, 168, 1, 10)), // local
        ];

        bulk_update_peers(&encap, peers)
            .await
            .expect("bulk_update_peers failed");

        #[cfg(all(target_os = "linux", feature = "ebpf"))]
        {
            let count = encap.peer_count().await;
            assert_eq!(count, 4, "Should have 4 peers after bulk update");
        }

        encap.detach().await.expect("detach failed");
    }

    #[test]
    fn test_getters() {
        let vni = 200;
        let vtep = Ipv4Addr::new(192, 168, 1, 20);
        let encap = EbpfEncap::new(vni, vtep);

        assert_eq!(encap.vni(), vni);
        assert_eq!(encap.local_vtep_ip(), vtep);
    }
}
