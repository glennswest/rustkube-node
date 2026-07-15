//! Bridge CNI plugin.
//!
//! Creates a Linux bridge, a veth pair connecting the pod network
//! namespace to the bridge, and configures IP routing.

use crate::cni_types::{CniConfig, CniDns, CniError, CniInterface, CniIpConfig, CniResult, RouteConfig};
use crate::ipam::HostLocalIpam;
use tracing::info;

/// Execute the bridge CNI ADD command.
pub fn cmd_add(
    config: &CniConfig,
    container_id: &str,
    netns: &str,
    ifname: &str,
) -> Result<CniResult, CniError> {
    let bridge_name = if config.bridge.is_empty() {
        "rk-br0"
    } else {
        &config.bridge
    };

    // Allocate IP via IPAM
    let ipam = HostLocalIpam::new(&config.ipam, &config.name)?;
    let (pod_ip, gateway, prefix_len) = ipam.allocate(container_id)?;

    info!(
        "Bridge ADD: container={container_id} bridge={bridge_name} ip={pod_ip}/{prefix_len} gw={gateway}"
    );

    // On Linux, we would:
    // 1. Create bridge if not exists
    // 2. Create veth pair
    // 3. Move one end into pod netns
    // 4. Assign IP to pod veth
    // 5. Set up routes
    // 6. Optionally set up IP masquerading
    #[cfg(target_os = "linux")]
    {
        setup_bridge_linux(bridge_name, container_id, netns, ifname, pod_ip, gateway, prefix_len, config)?;
    }

    // Build CNI result
    let result = CniResult {
        cni_version: config.cni_version.clone(),
        interfaces: vec![
            CniInterface {
                name: bridge_name.to_string(),
                mac: String::new(),
                sandbox: String::new(),
            },
            CniInterface {
                name: ifname.to_string(),
                mac: String::new(),
                sandbox: netns.to_string(),
            },
        ],
        ips: vec![CniIpConfig {
            address: format!("{pod_ip}/{prefix_len}"),
            gateway: Some(gateway.to_string()),
            interface: Some(1), // pod interface
        }],
        routes: vec![RouteConfig {
            dst: "0.0.0.0/0".to_string(),
            gw: gateway.to_string(),
        }],
        dns: CniDns {
            nameservers: vec!["10.96.0.10".to_string()],
            domain: String::new(),
            search: vec![
                "default.svc.cluster.local".to_string(),
                "svc.cluster.local".to_string(),
                "cluster.local".to_string(),
            ],
            options: vec!["ndots:5".to_string()],
        },
    };

    Ok(result)
}

/// Execute the bridge CNI DEL command.
pub fn cmd_del(
    config: &CniConfig,
    container_id: &str,
    _netns: &str,
    _ifname: &str,
) -> Result<(), CniError> {
    info!("Bridge DEL: container={container_id}");

    // Release IP via IPAM
    let ipam = HostLocalIpam::new(&config.ipam, &config.name)?;
    ipam.release(container_id)?;

    // On Linux, clean up veth and netns interface
    #[cfg(target_os = "linux")]
    {
        cleanup_bridge_linux(container_id, netns, ifname)?;
    }

    Ok(())
}

/// Linux-specific bridge setup using netlink.
#[cfg(target_os = "linux")]
fn setup_bridge_linux(
    bridge_name: &str,
    container_id: &str,
    netns: &str,
    ifname: &str,
    pod_ip: std::net::Ipv4Addr,
    gateway: std::net::Ipv4Addr,
    prefix_len: u8,
    config: &CniConfig,
) -> Result<(), CniError> {
    use std::process::Command;

    // Create bridge if it doesn't exist
    let _ = Command::new("ip")
        .args(["link", "add", bridge_name, "type", "bridge"])
        .output();

    Command::new("ip")
        .args(["link", "set", bridge_name, "up"])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to set bridge up: {e}")))?;

    // Assign gateway IP to bridge if configured
    if config.is_gateway {
        let gw_cidr = format!("{gateway}/{prefix_len}");
        let _ = Command::new("ip")
            .args(["addr", "add", &gw_cidr, "dev", bridge_name])
            .output();
    }

    // Create veth pair
    let host_veth = format!("veth{}", &container_id[..8.min(container_id.len())]);
    Command::new("ip")
        .args([
            "link", "add", &host_veth, "type", "veth", "peer", "name", ifname,
        ])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to create veth: {e}")))?;

    // Attach host end to bridge
    Command::new("ip")
        .args(["link", "set", &host_veth, "master", bridge_name])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to attach veth to bridge: {e}")))?;

    Command::new("ip")
        .args(["link", "set", &host_veth, "up"])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to set host veth up: {e}")))?;

    // Move pod end into netns
    Command::new("ip")
        .args(["link", "set", ifname, "netns", netns])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to move veth to netns: {e}")))?;

    // Configure pod interface inside netns
    let pod_cidr = format!("{pod_ip}/{prefix_len}");
    Command::new("nsenter")
        .args([
            "--net", netns, "--", "ip", "addr", "add", &pod_cidr, "dev", ifname,
        ])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to assign IP in netns: {e}")))?;

    Command::new("nsenter")
        .args(["--net", netns, "--", "ip", "link", "set", ifname, "up"])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to set pod interface up: {e}")))?;

    // Set up loopback
    Command::new("nsenter")
        .args(["--net", netns, "--", "ip", "link", "set", "lo", "up"])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to set lo up: {e}")))?;

    // Add default route via gateway
    let gw_str = gateway.to_string();
    Command::new("nsenter")
        .args([
            "--net", netns, "--", "ip", "route", "add", "default", "via", &gw_str,
        ])
        .output()
        .map_err(|e| CniError::BridgeError(format!("failed to add default route: {e}")))?;

    // Enable IP masquerading if configured
    if config.ip_masq {
        let subnet = format!("{}", config.ipam.subnet);
        let _ = Command::new("iptables")
            .args([
                "-t", "nat", "-A", "POSTROUTING",
                "-s", &subnet,
                "!", "-d", &subnet,
                "-j", "MASQUERADE",
            ])
            .output();
    }

    info!(
        "Bridge setup complete: {host_veth} → {bridge_name}, pod {ifname}={pod_ip}/{prefix_len}"
    );

    Ok(())
}

/// Linux-specific cleanup.
#[cfg(target_os = "linux")]
fn cleanup_bridge_linux(
    container_id: &str,
    netns: &str,
    ifname: &str,
) -> Result<(), CniError> {
    use std::process::Command;

    // Delete the pod interface from the netns (this also removes the veth pair)
    let _ = Command::new("nsenter")
        .args(["--net", netns, "--", "ip", "link", "del", ifname])
        .output();

    Ok(())
}
