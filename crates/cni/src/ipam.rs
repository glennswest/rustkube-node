//! Host-local IPAM plugin.
//!
//! Allocates IPs from a configured subnet range, persisting
//! allocations to disk for crash recovery.

use crate::cni_types::{CniError, IpamConfig};
use ipnet::Ipv4Net;
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use tracing::info;

/// Default data directory for IP allocations.
const DEFAULT_DATA_DIR: &str = "/var/lib/rk-cni/ipam";

/// Host-local IPAM allocator.
pub struct HostLocalIpam {
    subnet: Ipv4Net,
    range_start: Ipv4Addr,
    range_end: Ipv4Addr,
    gateway: Ipv4Addr,
    data_dir: PathBuf,
    network_name: String,
}

impl HostLocalIpam {
    /// Create a new IPAM from config.
    pub fn new(config: &IpamConfig, network_name: &str) -> Result<Self, CniError> {
        let subnet: Ipv4Net = config
            .subnet
            .parse()
            .map_err(|e| CniError::IpamError(format!("invalid subnet '{}': {e}", config.subnet)))?;

        let network = subnet.network();
        let broadcast = subnet.broadcast();

        // Default range: first usable to last usable
        let range_start = if config.range_start.is_empty() {
            next_ip(network)
        } else {
            config
                .range_start
                .parse()
                .map_err(|e| CniError::IpamError(format!("invalid range_start: {e}")))?
        };

        let range_end = if config.range_end.is_empty() {
            prev_ip(broadcast)
        } else {
            config
                .range_end
                .parse()
                .map_err(|e| CniError::IpamError(format!("invalid range_end: {e}")))?
        };

        let gateway = if config.gateway.is_empty() {
            next_ip(network) // First IP is gateway by default
        } else {
            config
                .gateway
                .parse()
                .map_err(|e| CniError::IpamError(format!("invalid gateway: {e}")))?
        };

        let data_dir = if config.data_dir.is_empty() {
            PathBuf::from(DEFAULT_DATA_DIR)
        } else {
            PathBuf::from(&config.data_dir)
        };

        Ok(Self {
            subnet,
            range_start,
            range_end,
            gateway,
            data_dir,
            network_name: network_name.to_string(),
        })
    }

    /// Allocate an IP for a container.
    pub fn allocate(&self, container_id: &str) -> Result<(Ipv4Addr, Ipv4Addr, u8), CniError> {
        let allocated = self.load_allocations()?;

        let mut ip = self.range_start;
        loop {
            // Skip gateway
            if ip != self.gateway && !allocated.contains(&ip) {
                // Found a free IP
                self.save_allocation(container_id, ip)?;
                info!(
                    "IPAM allocated {}/{} (gateway {}) for container {container_id}",
                    ip,
                    self.subnet.prefix_len(),
                    self.gateway
                );
                return Ok((ip, self.gateway, self.subnet.prefix_len()));
            }

            if ip == self.range_end {
                break;
            }
            ip = next_ip(ip);
        }

        Err(CniError::IpamError(format!(
            "no free IPs in range {}-{}",
            self.range_start, self.range_end
        )))
    }

    /// Release an IP for a container.
    pub fn release(&self, container_id: &str) -> Result<(), CniError> {
        let alloc_dir = self.alloc_dir();
        let alloc_file = alloc_dir.join(container_id);

        if alloc_file.exists() {
            std::fs::remove_file(&alloc_file)
                .map_err(|e| CniError::IpamError(format!("failed to remove allocation: {e}")))?;
            info!("IPAM released IP for container {container_id}");
        }

        Ok(())
    }

    /// Get the gateway IP.
    pub fn gateway(&self) -> Ipv4Addr {
        self.gateway
    }

    /// Get the subnet prefix length.
    pub fn prefix_len(&self) -> u8 {
        self.subnet.prefix_len()
    }

    fn alloc_dir(&self) -> PathBuf {
        self.data_dir.join(&self.network_name)
    }

    fn load_allocations(&self) -> Result<HashSet<Ipv4Addr>, CniError> {
        let alloc_dir = self.alloc_dir();
        let mut allocated = HashSet::new();

        if !alloc_dir.exists() {
            return Ok(allocated);
        }

        let entries = std::fs::read_dir(&alloc_dir)
            .map_err(|e| CniError::IpamError(format!("failed to read alloc dir: {e}")))?;

        for entry in entries.flatten() {
            if let Ok(contents) = std::fs::read_to_string(entry.path()) {
                if let Ok(ip) = contents.trim().parse::<Ipv4Addr>() {
                    allocated.insert(ip);
                }
            }
        }

        Ok(allocated)
    }

    fn save_allocation(&self, container_id: &str, ip: Ipv4Addr) -> Result<(), CniError> {
        let alloc_dir = self.alloc_dir();
        std::fs::create_dir_all(&alloc_dir)
            .map_err(|e| CniError::IpamError(format!("failed to create alloc dir: {e}")))?;

        let alloc_file = alloc_dir.join(container_id);
        std::fs::write(&alloc_file, ip.to_string())
            .map_err(|e| CniError::IpamError(format!("failed to write allocation: {e}")))?;

        Ok(())
    }
}

/// Increment an IPv4 address by 1.
fn next_ip(ip: Ipv4Addr) -> Ipv4Addr {
    let n: u32 = ip.into();
    Ipv4Addr::from(n + 1)
}

/// Decrement an IPv4 address by 1.
fn prev_ip(ip: Ipv4Addr) -> Ipv4Addr {
    let n: u32 = ip.into();
    Ipv4Addr::from(n - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_prev_ip() {
        let ip: Ipv4Addr = "10.244.0.1".parse().unwrap();
        assert_eq!(next_ip(ip), "10.244.0.2".parse::<Ipv4Addr>().unwrap());
        assert_eq!(prev_ip(ip), "10.244.0.0".parse::<Ipv4Addr>().unwrap());
    }

    #[test]
    fn test_ipam_allocate() {
        let dir = tempfile::tempdir().unwrap();
        let config = IpamConfig {
            ipam_type: "host-local".into(),
            subnet: "10.244.1.0/24".into(),
            range_start: "10.244.1.2".into(),
            range_end: "10.244.1.254".into(),
            gateway: "10.244.1.1".into(),
            routes: vec![],
            data_dir: dir.path().to_string_lossy().to_string(),
        };

        let ipam = HostLocalIpam::new(&config, "test-net").unwrap();

        let (ip1, gw, prefix) = ipam.allocate("container-1").unwrap();
        assert_eq!(ip1, "10.244.1.2".parse::<Ipv4Addr>().unwrap());
        assert_eq!(gw, "10.244.1.1".parse::<Ipv4Addr>().unwrap());
        assert_eq!(prefix, 24);

        let (ip2, _, _) = ipam.allocate("container-2").unwrap();
        assert_eq!(ip2, "10.244.1.3".parse::<Ipv4Addr>().unwrap());

        ipam.release("container-1").unwrap();

        let (ip3, _, _) = ipam.allocate("container-3").unwrap();
        assert_eq!(ip3, "10.244.1.2".parse::<Ipv4Addr>().unwrap()); // Reuses freed IP
    }
}
