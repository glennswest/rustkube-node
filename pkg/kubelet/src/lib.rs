//! rk-kubelet: Node agent managing pod lifecycle via CRI.
//!
//! Connects to container runtimes (containerd, CRI-O) via gRPC,
//! manages pod state machines, health probes, volumes, image pulls,
//! and reports node status via Lease heartbeats.

pub mod checkpoint;
pub mod cri;
pub mod cri_client;
pub mod csi;
pub mod health;
pub mod kubelet;
pub mod node_status;
pub mod pod_manager;
pub mod runtime;
pub mod vm_migrate;
pub mod vm_runtime;

pub use checkpoint::CriuCheckpointer;
pub use cri_client::{CriClient, detect_cri_socket};
pub use kubelet::{Kubelet, KubeletConfig};
pub use runtime::{NativeRuntime, NativeImageService};
pub use vm_runtime::{VmRuntime, VmConfig, VmmBackend};
