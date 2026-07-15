//! Bridge CNI plugin.
//!
//! Creates a Linux bridge, a veth pair connecting the pod network namespace
//! to the bridge, and configures IP routing.
//!
//! The kernel-touching work is isolated behind the [`NetlinkOps`] trait so the
//! pure planning logic ([`BridgePlan`]) and the CNI result construction can be
//! unit-tested on any platform. On Linux the default backend shells out to
//! `ip`/`nsenter`/`iptables`; everywhere else (and in tests) a no-op recording
//! backend is used.

use crate::cni_types::{
    CniConfig, CniDns, CniError, CniInterface, CniIpConfig, CniResult, RouteConfig,
};
use crate::ipam::HostLocalIpam;
use std::net::Ipv4Addr;
use std::sync::Mutex;
use tracing::info;

/// Default bridge name when none is configured.
const DEFAULT_BRIDGE: &str = "rk-br0";

/// A fully-resolved, pure description of the network plumbing required to
/// attach one pod to the bridge. Contains no side effects — it is produced
/// from config + the IPAM result and then handed to a [`NetlinkOps`] backend.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgePlan {
    pub bridge_name: String,
    /// Host-side veth name (attached to the bridge).
    pub host_veth: String,
    /// Pod-side interface name (moved into the netns).
    pub pod_ifname: String,
    pub netns: String,
    pub pod_ip: Ipv4Addr,
    pub gateway: Ipv4Addr,
    pub prefix_len: u8,
    /// Routes to install inside the pod netns.
    pub routes: Vec<RouteConfig>,
    /// Whether the bridge should own the gateway address.
    pub is_gateway: bool,
    pub ip_masq: bool,
    /// Subnet used for the masquerade rule.
    pub subnet: String,
}

impl BridgePlan {
    /// Build the plan for a container from resolved inputs. Pure/deterministic.
    pub fn new(
        config: &CniConfig,
        container_id: &str,
        netns: &str,
        ifname: &str,
        pod_ip: Ipv4Addr,
        gateway: Ipv4Addr,
        prefix_len: u8,
    ) -> Self {
        let bridge_name = if config.bridge.is_empty() {
            DEFAULT_BRIDGE.to_string()
        } else {
            config.bridge.clone()
        };

        Self {
            bridge_name,
            host_veth: host_veth_name(container_id),
            pod_ifname: ifname.to_string(),
            netns: netns.to_string(),
            pod_ip,
            gateway,
            prefix_len,
            routes: vec![RouteConfig {
                dst: "0.0.0.0/0".to_string(),
                gw: gateway.to_string(),
            }],
            is_gateway: config.is_gateway,
            ip_masq: config.ip_masq,
            subnet: config.ipam.subnet.clone(),
        }
    }

    /// Pod address in CIDR form, e.g. `10.244.1.2/24`.
    pub fn pod_cidr(&self) -> String {
        format!("{}/{}", self.pod_ip, self.prefix_len)
    }

    /// Gateway address in CIDR form, e.g. `10.244.1.1/24`.
    pub fn gateway_cidr(&self) -> String {
        format!("{}/{}", self.gateway, self.prefix_len)
    }
}

/// Derive a deterministic, kernel-legal (<=15 char) host veth name.
fn host_veth_name(container_id: &str) -> String {
    let short = &container_id[..8.min(container_id.len())];
    format!("veth{short}")
}

/// Kernel/netns operations required to realise a [`BridgePlan`].
///
/// This is the mockable seam: the real Linux backend performs netlink/`ip`
/// operations, while tests and non-Linux builds use [`RecordingNetlinkOps`].
pub trait NetlinkOps {
    fn ensure_bridge(&self, name: &str) -> Result<(), CniError>;
    fn set_bridge_gateway(&self, bridge: &str, gw_cidr: &str) -> Result<(), CniError>;
    fn create_veth_pair(&self, host: &str, pod: &str) -> Result<(), CniError>;
    fn attach_to_bridge(&self, veth: &str, bridge: &str) -> Result<(), CniError>;
    fn move_to_netns(&self, ifname: &str, netns: &str) -> Result<(), CniError>;
    fn assign_pod_addr(&self, netns: &str, ifname: &str, cidr: &str) -> Result<(), CniError>;
    fn add_default_route(&self, netns: &str, gw: &str) -> Result<(), CniError>;
    fn enable_masquerade(&self, subnet: &str) -> Result<(), CniError>;
    fn delete_pod_link(&self, netns: &str, ifname: &str) -> Result<(), CniError>;
}

/// Apply a plan through a backend. This is the single ordered sequence of
/// side-effecting steps; both the Linux and no-op backends run through it.
pub fn apply_plan(plan: &BridgePlan, ops: &dyn NetlinkOps) -> Result<(), CniError> {
    ops.ensure_bridge(&plan.bridge_name)?;
    if plan.is_gateway {
        ops.set_bridge_gateway(&plan.bridge_name, &plan.gateway_cidr())?;
    }
    ops.create_veth_pair(&plan.host_veth, &plan.pod_ifname)?;
    ops.attach_to_bridge(&plan.host_veth, &plan.bridge_name)?;
    ops.move_to_netns(&plan.pod_ifname, &plan.netns)?;
    ops.assign_pod_addr(&plan.netns, &plan.pod_ifname, &plan.pod_cidr())?;
    ops.add_default_route(&plan.netns, &plan.gateway.to_string())?;
    if plan.ip_masq && !plan.subnet.is_empty() {
        ops.enable_masquerade(&plan.subnet)?;
    }
    Ok(())
}

/// Build the CNI Result returned by a successful ADD.
fn build_result(config: &CniConfig, plan: &BridgePlan) -> CniResult {
    CniResult {
        cni_version: config.cni_version.clone(),
        interfaces: vec![
            CniInterface {
                name: plan.bridge_name.clone(),
                mac: String::new(),
                sandbox: String::new(),
            },
            CniInterface {
                name: plan.pod_ifname.clone(),
                mac: String::new(),
                sandbox: plan.netns.clone(),
            },
        ],
        ips: vec![CniIpConfig {
            address: plan.pod_cidr(),
            gateway: Some(plan.gateway.to_string()),
            interface: Some(1), // pod interface
        }],
        routes: plan.routes.clone(),
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
    }
}

/// Execute the bridge CNI ADD command using the platform-default backend.
pub fn cmd_add(
    config: &CniConfig,
    container_id: &str,
    netns: &str,
    ifname: &str,
) -> Result<CniResult, CniError> {
    cmd_add_with_ops(config, container_id, netns, ifname, &default_ops())
}

/// Execute the bridge CNI ADD command against an explicit backend.
///
/// This is the testable entry point: allocate an IP, plan the plumbing, apply
/// it through `ops`, and return the CNI result.
pub fn cmd_add_with_ops(
    config: &CniConfig,
    container_id: &str,
    netns: &str,
    ifname: &str,
    ops: &dyn NetlinkOps,
) -> Result<CniResult, CniError> {
    let ipam = HostLocalIpam::new(&config.ipam, &config.name)?;
    let (pod_ip, gateway, prefix_len) = ipam.allocate(container_id)?;

    let plan = BridgePlan::new(config, container_id, netns, ifname, pod_ip, gateway, prefix_len);

    info!(
        "Bridge ADD: container={container_id} bridge={} veth={} pod={} gw={}",
        plan.bridge_name,
        plan.host_veth,
        plan.pod_cidr(),
        plan.gateway
    );

    if let Err(e) = apply_plan(&plan, ops) {
        // Roll back the IP so a failed ADD does not leak an allocation.
        let _ = ipam.release(container_id);
        return Err(e);
    }

    Ok(build_result(config, &plan))
}

/// Execute the bridge CNI DEL command using the platform-default backend.
pub fn cmd_del(
    config: &CniConfig,
    container_id: &str,
    netns: &str,
    ifname: &str,
) -> Result<(), CniError> {
    cmd_del_with_ops(config, container_id, netns, ifname, &default_ops())
}

/// Execute the bridge CNI DEL command against an explicit backend.
pub fn cmd_del_with_ops(
    config: &CniConfig,
    container_id: &str,
    netns: &str,
    ifname: &str,
    ops: &dyn NetlinkOps,
) -> Result<(), CniError> {
    info!("Bridge DEL: container={container_id}");

    // Best-effort teardown of the pod link, then release the IP.
    let _ = ops.delete_pod_link(netns, ifname);

    let ipam = HostLocalIpam::new(&config.ipam, &config.name)?;
    ipam.release(container_id)?;
    Ok(())
}

/// The platform-default backend: real netlink ops on Linux, no-op elsewhere.
#[cfg(target_os = "linux")]
fn default_ops() -> LinuxNetlinkOps {
    LinuxNetlinkOps
}

#[cfg(not(target_os = "linux"))]
fn default_ops() -> RecordingNetlinkOps {
    RecordingNetlinkOps::default()
}

/// A no-op backend that records the sequence of operations it was asked to
/// perform. Used on non-Linux hosts and in unit tests to assert on the plan
/// without touching the kernel.
#[derive(Debug, Default)]
pub struct RecordingNetlinkOps {
    ops: Mutex<Vec<String>>,
}

impl RecordingNetlinkOps {
    /// Snapshot the recorded operations, in order.
    pub fn recorded(&self) -> Vec<String> {
        self.ops.lock().unwrap().clone()
    }
    fn record(&self, s: String) -> Result<(), CniError> {
        self.ops.lock().unwrap().push(s);
        Ok(())
    }
}

impl NetlinkOps for RecordingNetlinkOps {
    fn ensure_bridge(&self, name: &str) -> Result<(), CniError> {
        self.record(format!("ensure_bridge {name}"))
    }
    fn set_bridge_gateway(&self, bridge: &str, gw_cidr: &str) -> Result<(), CniError> {
        self.record(format!("set_bridge_gateway {bridge} {gw_cidr}"))
    }
    fn create_veth_pair(&self, host: &str, pod: &str) -> Result<(), CniError> {
        self.record(format!("create_veth_pair {host} {pod}"))
    }
    fn attach_to_bridge(&self, veth: &str, bridge: &str) -> Result<(), CniError> {
        self.record(format!("attach_to_bridge {veth} {bridge}"))
    }
    fn move_to_netns(&self, ifname: &str, netns: &str) -> Result<(), CniError> {
        self.record(format!("move_to_netns {ifname} {netns}"))
    }
    fn assign_pod_addr(&self, netns: &str, ifname: &str, cidr: &str) -> Result<(), CniError> {
        self.record(format!("assign_pod_addr {netns} {ifname} {cidr}"))
    }
    fn add_default_route(&self, netns: &str, gw: &str) -> Result<(), CniError> {
        self.record(format!("add_default_route {netns} {gw}"))
    }
    fn enable_masquerade(&self, subnet: &str) -> Result<(), CniError> {
        self.record(format!("enable_masquerade {subnet}"))
    }
    fn delete_pod_link(&self, netns: &str, ifname: &str) -> Result<(), CniError> {
        self.record(format!("delete_pod_link {netns} {ifname}"))
    }
}

/// Real Linux backend — shells out to `ip`/`nsenter`/`iptables`.
#[cfg(target_os = "linux")]
pub struct LinuxNetlinkOps;

#[cfg(target_os = "linux")]
impl LinuxNetlinkOps {
    fn run(cmd: &str, args: &[&str], ctx: &str) -> Result<(), CniError> {
        use std::process::Command;
        Command::new(cmd)
            .args(args)
            .output()
            .map_err(|e| CniError::BridgeError(format!("{ctx}: {e}")))?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl NetlinkOps for LinuxNetlinkOps {
    fn ensure_bridge(&self, name: &str) -> Result<(), CniError> {
        // Ignore "already exists" from the add.
        let _ = Self::run("ip", &["link", "add", name, "type", "bridge"], "add bridge");
        Self::run("ip", &["link", "set", name, "up"], "set bridge up")
    }
    fn set_bridge_gateway(&self, bridge: &str, gw_cidr: &str) -> Result<(), CniError> {
        let _ = Self::run("ip", &["addr", "add", gw_cidr, "dev", bridge], "add bridge addr");
        Ok(())
    }
    fn create_veth_pair(&self, host: &str, pod: &str) -> Result<(), CniError> {
        Self::run(
            "ip",
            &["link", "add", host, "type", "veth", "peer", "name", pod],
            "create veth",
        )?;
        Self::run("ip", &["link", "set", host, "up"], "set host veth up")
    }
    fn attach_to_bridge(&self, veth: &str, bridge: &str) -> Result<(), CniError> {
        Self::run("ip", &["link", "set", veth, "master", bridge], "attach veth")
    }
    fn move_to_netns(&self, ifname: &str, netns: &str) -> Result<(), CniError> {
        Self::run("ip", &["link", "set", ifname, "netns", netns], "move to netns")
    }
    fn assign_pod_addr(&self, netns: &str, ifname: &str, cidr: &str) -> Result<(), CniError> {
        Self::run(
            "nsenter",
            &["--net", netns, "--", "ip", "addr", "add", cidr, "dev", ifname],
            "assign pod addr",
        )?;
        Self::run(
            "nsenter",
            &["--net", netns, "--", "ip", "link", "set", ifname, "up"],
            "set pod link up",
        )?;
        Self::run(
            "nsenter",
            &["--net", netns, "--", "ip", "link", "set", "lo", "up"],
            "set lo up",
        )
    }
    fn add_default_route(&self, netns: &str, gw: &str) -> Result<(), CniError> {
        Self::run(
            "nsenter",
            &["--net", netns, "--", "ip", "route", "add", "default", "via", gw],
            "add default route",
        )
    }
    fn enable_masquerade(&self, subnet: &str) -> Result<(), CniError> {
        let _ = Self::run(
            "iptables",
            &[
                "-t", "nat", "-A", "POSTROUTING", "-s", subnet, "!", "-d", subnet, "-j",
                "MASQUERADE",
            ],
            "enable masquerade",
        );
        Ok(())
    }
    fn delete_pod_link(&self, netns: &str, ifname: &str) -> Result<(), CniError> {
        let _ = Self::run(
            "nsenter",
            &["--net", netns, "--", "ip", "link", "del", ifname],
            "delete pod link",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cni_types::IpamConfig;

    fn test_config() -> CniConfig {
        CniConfig {
            cni_version: "1.0.0".into(),
            name: "rk-net".into(),
            plugin_type: "bridge".into(),
            bridge: "rk-br0".into(),
            is_gateway: true,
            ip_masq: true,
            hairpin_mode: false,
            mtu: 1500,
            ipam: IpamConfig {
                ipam_type: "host-local".into(),
                subnet: "10.244.1.0/24".into(),
                range_start: String::new(),
                range_end: String::new(),
                gateway: String::new(),
                routes: vec![],
                // Persist under a temp dir so the test never writes to /var.
                data_dir: std::env::temp_dir()
                    .join(format!("rk-cni-test-{}", std::process::id()))
                    .to_string_lossy()
                    .into(),
            },
            vxlan: None,
        }
    }

    #[test]
    fn test_bridge_plan_contents() {
        let cfg = test_config();
        let plan = BridgePlan::new(
            &cfg,
            "abcdef0123456789",
            "/var/run/netns/pod1",
            "eth0",
            "10.244.1.2".parse().unwrap(),
            "10.244.1.1".parse().unwrap(),
            24,
        );

        // veth pair: deterministic host name + pod ifname.
        assert_eq!(plan.host_veth, "vethabcdef01");
        assert_eq!(plan.pod_ifname, "eth0");
        // allocated pod IP carried through.
        assert_eq!(plan.pod_cidr(), "10.244.1.2/24");
        // default route via the bridge gateway.
        assert_eq!(plan.routes.len(), 1);
        assert_eq!(plan.routes[0].dst, "0.0.0.0/0");
        assert_eq!(plan.routes[0].gw, "10.244.1.1");
    }

    #[test]
    fn test_cmd_add_applies_plan_and_builds_result() {
        let cfg = test_config();
        let ops = RecordingNetlinkOps::default();

        let result =
            cmd_add_with_ops(&cfg, "container-add-1", "/var/run/netns/p", "eth0", &ops).unwrap();

        // Result carries the allocated IP, gateway and default route.
        assert_eq!(result.ips.len(), 1);
        assert_eq!(result.ips[0].address, "10.244.1.2/24");
        assert_eq!(result.ips[0].gateway.as_deref(), Some("10.244.1.1"));
        assert_eq!(result.routes[0].dst, "0.0.0.0/0");
        assert_eq!(result.routes[0].gw, "10.244.1.1");
        assert_eq!(result.interfaces.len(), 2);
        assert_eq!(result.interfaces[1].sandbox, "/var/run/netns/p");

        // The backend saw the veth pair created and the default route added.
        let rec = ops.recorded();
        assert!(rec.iter().any(|s| s == "create_veth_pair vethcontaine eth0"), "{rec:?}");
        assert!(rec.iter().any(|s| s.starts_with("add_default_route")), "{rec:?}");
        assert!(rec.iter().any(|s| s == "assign_pod_addr /var/run/netns/p eth0 10.244.1.2/24"), "{rec:?}");

        // Clean up so re-runs start from a fresh IPAM state on disk.
        cmd_del_with_ops(&cfg, "container-add-1", "/var/run/netns/p", "eth0", &ops).unwrap();
    }
}
