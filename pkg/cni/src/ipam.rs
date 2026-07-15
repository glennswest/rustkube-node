//! Host-local IPAM plugin.
//!
//! Allocates IPs from a configured subnet range. Allocation state is held
//! in memory as the source of truth and mirrored to a pluggable persistence
//! backend (the [`AllocStore`] seam) so state can survive a restart.
//!
//! The allocation logic is pure and deterministic — it never touches the
//! kernel — so it is fully unit-testable on any platform (including macOS)
//! by swapping the disk backing store for the in-memory [`MemStore`].

use crate::cni_types::{CniError, IpamConfig};
use ipnet::Ipv4Net;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::info;

/// Default data directory for IP allocations.
const DEFAULT_DATA_DIR: &str = "/var/lib/rk-cni/ipam";

/// Persistence seam for IPAM allocations.
///
/// The allocator keeps its authoritative state in memory; this trait lets the
/// same allocation logic persist to disk in production or to nothing at all in
/// tests. Implementations must be safe to share across threads.
pub trait AllocStore: Send + Sync {
    /// Load all previously persisted `container_id -> IP` allocations.
    fn load(&self) -> Result<HashMap<String, Ipv4Addr>, CniError>;
    /// Persist a single allocation.
    fn persist(&self, container_id: &str, ip: Ipv4Addr) -> Result<(), CniError>;
    /// Remove a persisted allocation (idempotent).
    fn remove(&self, container_id: &str) -> Result<(), CniError>;
}

/// In-memory persistence backend — persistence is a no-op.
///
/// Used for unit tests and any caller that does not need crash recovery.
#[derive(Debug, Default)]
pub struct MemStore;

impl AllocStore for MemStore {
    fn load(&self) -> Result<HashMap<String, Ipv4Addr>, CniError> {
        Ok(HashMap::new())
    }
    fn persist(&self, _container_id: &str, _ip: Ipv4Addr) -> Result<(), CniError> {
        Ok(())
    }
    fn remove(&self, _container_id: &str) -> Result<(), CniError> {
        Ok(())
    }
}

/// Disk-backed persistence backend.
///
/// Stores one file per container under `dir`, whose contents are the
/// allocated IP in dotted-decimal form. This matches the layout used by the
/// upstream host-local plugin closely enough for crash recovery.
#[derive(Debug)]
pub struct DiskStore {
    dir: PathBuf,
}

impl DiskStore {
    /// Create a disk store rooted at `<data_dir>/<network_name>`.
    pub fn new(data_dir: PathBuf, network_name: &str) -> Self {
        Self {
            dir: data_dir.join(network_name),
        }
    }
}

impl AllocStore for DiskStore {
    fn load(&self) -> Result<HashMap<String, Ipv4Addr>, CniError> {
        let mut out = HashMap::new();
        if !self.dir.exists() {
            return Ok(out);
        }
        let entries = std::fs::read_dir(&self.dir)
            .map_err(|e| CniError::IpamError(format!("failed to read alloc dir: {e}")))?;
        for entry in entries.flatten() {
            let path = entry.path();
            if let (Some(name), Ok(contents)) =
                (path.file_name().and_then(|n| n.to_str()), std::fs::read_to_string(&path))
            {
                if let Ok(ip) = contents.trim().parse::<Ipv4Addr>() {
                    out.insert(name.to_string(), ip);
                }
            }
        }
        Ok(out)
    }

    fn persist(&self, container_id: &str, ip: Ipv4Addr) -> Result<(), CniError> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| CniError::IpamError(format!("failed to create alloc dir: {e}")))?;
        std::fs::write(self.dir.join(container_id), ip.to_string())
            .map_err(|e| CniError::IpamError(format!("failed to write allocation: {e}")))?;
        Ok(())
    }

    fn remove(&self, container_id: &str) -> Result<(), CniError> {
        let file = self.dir.join(container_id);
        if file.exists() {
            std::fs::remove_file(&file)
                .map_err(|e| CniError::IpamError(format!("failed to remove allocation: {e}")))?;
        }
        Ok(())
    }
}

/// Host-local IPAM allocator.
///
/// Hands out the lowest free address in `[range_start, range_end]`, always
/// skipping the network address, the broadcast address and the gateway.
pub struct HostLocalIpam {
    subnet: Ipv4Net,
    range_start: Ipv4Addr,
    range_end: Ipv4Addr,
    gateway: Ipv4Addr,
    store: Box<dyn AllocStore>,
    /// In-memory source of truth: `container_id -> allocated IP`.
    allocations: Mutex<HashMap<String, Ipv4Addr>>,
}

impl HostLocalIpam {
    /// Create a disk-backed IPAM from config (production path).
    pub fn new(config: &IpamConfig, network_name: &str) -> Result<Self, CniError> {
        let data_dir = if config.data_dir.is_empty() {
            PathBuf::from(DEFAULT_DATA_DIR)
        } else {
            PathBuf::from(&config.data_dir)
        };
        let store = DiskStore::new(data_dir, network_name);
        Self::with_store(config, Box::new(store))
    }

    /// Create an IPAM with an explicit persistence backend.
    ///
    /// Pass a [`MemStore`] for pure, host-independent unit tests.
    pub fn with_store(config: &IpamConfig, store: Box<dyn AllocStore>) -> Result<Self, CniError> {
        let subnet: Ipv4Net = config
            .subnet
            .parse()
            .map_err(|e| CniError::IpamError(format!("invalid subnet '{}': {e}", config.subnet)))?;

        let network = subnet.network();
        let broadcast = subnet.broadcast();

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
            next_ip(network) // First usable IP is the gateway by default.
        } else {
            config
                .gateway
                .parse()
                .map_err(|e| CniError::IpamError(format!("invalid gateway: {e}")))?
        };

        // Seed the in-memory state from whatever the store already knows.
        let allocations = store.load()?;

        Ok(Self {
            subnet,
            range_start,
            range_end,
            gateway,
            store,
            allocations: Mutex::new(allocations),
        })
    }

    /// Allocate an IP for a container.
    ///
    /// Returns `(pod_ip, gateway, prefix_len)`. If the container already holds
    /// an allocation the existing IP is returned (idempotent ADD). Returns
    /// [`CniError::IpamError`] when the range is exhausted.
    pub fn allocate(&self, container_id: &str) -> Result<(Ipv4Addr, Ipv4Addr, u8), CniError> {
        let mut allocations = self.allocations.lock().unwrap();

        // Idempotency: re-allocating for the same container returns its IP.
        if let Some(&ip) = allocations.get(container_id) {
            return Ok((ip, self.gateway, self.subnet.prefix_len()));
        }

        let taken: std::collections::HashSet<Ipv4Addr> = allocations.values().copied().collect();

        let mut ip = self.range_start;
        loop {
            if self.is_assignable(ip) && !taken.contains(&ip) {
                allocations.insert(container_id.to_string(), ip);
                self.store.persist(container_id, ip)?;
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

    /// Release a container's IP (idempotent).
    pub fn release(&self, container_id: &str) -> Result<(), CniError> {
        let mut allocations = self.allocations.lock().unwrap();
        if allocations.remove(container_id).is_some() {
            self.store.remove(container_id)?;
            info!("IPAM released IP for container {container_id}");
        }
        Ok(())
    }

    /// Look up the current allocation for a container, if any.
    pub fn allocated_ip(&self, container_id: &str) -> Option<Ipv4Addr> {
        self.allocations.lock().unwrap().get(container_id).copied()
    }

    /// Get the gateway IP.
    pub fn gateway(&self) -> Ipv4Addr {
        self.gateway
    }

    /// Get the subnet prefix length.
    pub fn prefix_len(&self) -> u8 {
        self.subnet.prefix_len()
    }

    /// Whether `ip` may be handed to a pod: inside the subnet and not the
    /// network, broadcast or gateway address.
    fn is_assignable(&self, ip: Ipv4Addr) -> bool {
        ip != self.subnet.network()
            && ip != self.subnet.broadcast()
            && ip != self.gateway
            && self.subnet.contains(&ip)
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

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    /// Build a mem-backed IPAM over `subnet` using only defaults, so the
    /// allocator derives gateway (.1), start (.1) and end (broadcast-1).
    fn ipam(subnet: &str) -> HostLocalIpam {
        let config = IpamConfig {
            ipam_type: "host-local".into(),
            subnet: subnet.into(),
            range_start: String::new(),
            range_end: String::new(),
            gateway: String::new(),
            routes: vec![],
            data_dir: String::new(),
        };
        HostLocalIpam::with_store(&config, Box::new(MemStore)).unwrap()
    }

    #[test]
    fn test_next_prev_ip() {
        assert_eq!(next_ip(ip("10.244.0.1")), ip("10.244.0.2"));
        assert_eq!(prev_ip(ip("10.244.0.1")), ip("10.244.0.0"));
    }

    #[test]
    fn test_sequential_allocation_skips_reserved() {
        // Defaults: network .0, gateway .1, broadcast .255 all skipped.
        let ipam = ipam("10.244.1.0/24");
        assert_eq!(ipam.allocate("c1").unwrap().0, ip("10.244.1.2"));
        assert_eq!(ipam.allocate("c2").unwrap().0, ip("10.244.1.3"));
        assert_eq!(ipam.allocate("c3").unwrap().0, ip("10.244.1.4"));

        // The gateway comes back with each allocation, prefix is /24.
        let (_, gw, prefix) = ipam.allocate("c4").unwrap();
        assert_eq!(gw, ip("10.244.1.1"));
        assert_eq!(prefix, 24);
        assert_eq!(ipam.allocated_ip("c4"), Some(ip("10.244.1.5")));
    }

    #[test]
    fn test_release_then_reallocate_reuses_ip() {
        let ipam = ipam("10.244.1.0/24");
        assert_eq!(ipam.allocate("c1").unwrap().0, ip("10.244.1.2"));
        assert_eq!(ipam.allocate("c2").unwrap().0, ip("10.244.1.3"));

        ipam.release("c1").unwrap();
        assert_eq!(ipam.allocated_ip("c1"), None);

        // Lowest free address is the just-freed .2.
        assert_eq!(ipam.allocate("c3").unwrap().0, ip("10.244.1.2"));
    }

    #[test]
    fn test_idempotent_allocation() {
        let ipam = ipam("10.244.1.0/24");
        let first = ipam.allocate("c1").unwrap().0;
        let again = ipam.allocate("c1").unwrap().0;
        assert_eq!(first, again);
        // A second, distinct container must not collide.
        assert_ne!(ipam.allocate("c2").unwrap().0, first);
    }

    #[test]
    fn test_exhaustion() {
        // /30: network .0, broadcast .3, usable .1/.2; .1 is the gateway,
        // leaving exactly one assignable address (.2).
        let ipam = ipam("10.244.1.0/30");
        assert_eq!(ipam.allocate("c1").unwrap().0, ip("10.244.1.2"));
        let err = ipam.allocate("c2").unwrap_err();
        assert!(matches!(err, CniError::IpamError(_)), "expected exhaustion, got {err:?}");
    }

    #[test]
    fn test_disk_store_roundtrip_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let config = IpamConfig {
            ipam_type: "host-local".into(),
            subnet: "10.244.1.0/24".into(),
            range_start: String::new(),
            range_end: String::new(),
            gateway: String::new(),
            routes: vec![],
            data_dir: dir.path().to_string_lossy().to_string(),
        };

        // First allocator writes to disk.
        let a = HostLocalIpam::new(&config, "net").unwrap();
        let ip1 = a.allocate("c1").unwrap().0;
        assert_eq!(ip1, ip("10.244.1.2"));
        drop(a);

        // A fresh allocator recovers the allocation from disk and hands the
        // next container the following address rather than reusing .2.
        let b = HostLocalIpam::new(&config, "net").unwrap();
        assert_eq!(b.allocated_ip("c1"), Some(ip("10.244.1.2")));
        assert_eq!(b.allocate("c2").unwrap().0, ip("10.244.1.3"));
    }
}
