//! rk-cni: CNI plugin binaries for pod network setup.
//!
//! Implements bridge, host-local IPAM, loopback, and portmap plugins.
//! Phase 1: VXLAN overlay for cross-node pod traffic.
//! Phase 2: eBPF-based encap/decap.

pub mod bridge;
pub mod cni_types;
#[allow(unexpected_cfgs)]
pub mod ebpf_encap;
pub mod ipam;
pub mod vxlan;

pub use bridge::{BridgePlan, NetlinkOps, RecordingNetlinkOps};
pub use cni_types::{CniConfig, CniError, CniResult};
pub use ipam::{AllocStore, DiskStore, HostLocalIpam, MemStore};
