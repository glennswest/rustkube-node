//! VXLAN overlay networking.
//!
//! Creates VXLAN tunnels between nodes for cross-node pod traffic.
//! Each node gets a VTEP (VXLAN Tunnel EndPoint) device.

use crate::cni_types::CniError;
use std::net::Ipv4Addr;
use tracing::info;

/// VXLAN tunnel configuration per node.
#[derive(Debug, Clone)]
pub struct VxlanTunnel {
    pub vni: u32,
    pub port: u16,
    pub vtep_name: String,
    pub local_ip: Ipv4Addr,
    pub pod_cidr: String,
}

impl VxlanTunnel {
    pub fn new(vni: u32, port: u16, local_ip: Ipv4Addr, pod_cidr: &str) -> Self {
        Self {
            vni,
            port,
            vtep_name: format!("rk-vxlan{vni}"),
            local_ip,
            pod_cidr: pod_cidr.to_string(),
        }
    }

    /// Create the VXLAN device. Linux only.
    #[cfg(target_os = "linux")]
    pub fn create_vtep(&self) -> Result<(), CniError> {
        use std::process::Command;

        let vni_str = self.vni.to_string();
        let port_str = self.port.to_string();
        let local_str = self.local_ip.to_string();

        // Create VXLAN device
        let output = Command::new("ip")
            .args([
                "link", "add", &self.vtep_name,
                "type", "vxlan",
                "id", &vni_str,
                "dstport", &port_str,
                "local", &local_str,
                "nolearning",
            ])
            .output()
            .map_err(|e| CniError::VxlanError(format!("failed to create VXLAN device: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ignore "already exists" errors
            if !stderr.contains("File exists") {
                return Err(CniError::VxlanError(format!(
                    "ip link add vxlan failed: {stderr}"
                )));
            }
        }

        // Bring up
        Command::new("ip")
            .args(["link", "set", &self.vtep_name, "up"])
            .output()
            .map_err(|e| CniError::VxlanError(format!("failed to bring up VTEP: {e}")))?;

        info!(
            "VXLAN VTEP {} created (VNI={}, port={}, local={})",
            self.vtep_name, self.vni, self.port, self.local_ip
        );

        Ok(())
    }

    /// No-op on non-Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn create_vtep(&self) -> Result<(), CniError> {
        info!(
            "Skipping VXLAN VTEP creation on non-Linux (VNI={}, local={})",
            self.vni, self.local_ip
        );
        Ok(())
    }

    /// Add a remote node's FDB and route entries. Linux only.
    #[cfg(target_os = "linux")]
    pub fn add_peer(
        &self,
        peer_ip: Ipv4Addr,
        peer_pod_cidr: &str,
        peer_vtep_mac: &str,
    ) -> Result<(), CniError> {
        use std::process::Command;

        let peer_ip_str = peer_ip.to_string();

        // Add FDB entry: forward to remote VTEP
        Command::new("bridge")
            .args([
                "fdb", "append", peer_vtep_mac,
                "dev", &self.vtep_name,
                "dst", &peer_ip_str,
            ])
            .output()
            .map_err(|e| CniError::VxlanError(format!("failed to add FDB entry: {e}")))?;

        // Add route to remote pod CIDR via VXLAN
        let _ = Command::new("ip")
            .args([
                "route", "add", peer_pod_cidr,
                "via", &peer_ip_str,
                "dev", &self.vtep_name,
                "onlink",
            ])
            .output();

        info!(
            "Added VXLAN peer: {} → {} (pod CIDR {})",
            peer_ip, self.vtep_name, peer_pod_cidr
        );

        Ok(())
    }

    /// No-op on non-Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn add_peer(
        &self,
        peer_ip: Ipv4Addr,
        peer_pod_cidr: &str,
        _peer_vtep_mac: &str,
    ) -> Result<(), CniError> {
        info!(
            "Skipping VXLAN peer add on non-Linux (peer={}, cidr={})",
            peer_ip, peer_pod_cidr
        );
        Ok(())
    }

    /// Remove the VXLAN device. Linux only.
    #[cfg(target_os = "linux")]
    pub fn destroy(&self) -> Result<(), CniError> {
        use std::process::Command;

        let _ = Command::new("ip")
            .args(["link", "del", &self.vtep_name])
            .output();

        info!("VXLAN VTEP {} destroyed", self.vtep_name);
        Ok(())
    }

    /// No-op on non-Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn destroy(&self) -> Result<(), CniError> {
        Ok(())
    }
}
