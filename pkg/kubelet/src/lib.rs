//! rk-kubelet: Node agent managing pod lifecycle via CRI.
//!
//! Connects to container runtimes (containerd, CRI-O) via gRPC,
//! manages pod state machines, health probes, volumes, image pulls,
//! and reports node status via Lease heartbeats.

pub mod capacity;
pub mod container_roots;
pub mod dns;
pub mod engine;
pub mod events;
pub mod metrics;
pub mod mirror;
pub mod node_health;
pub mod node_logs;
pub mod start_timing;
pub mod storage;
pub mod system_claims;
pub mod stormpump_ring;
pub mod stormpump_runtime;
pub mod checkpoint;
pub mod client;
pub mod crashloop;
pub mod cri;
pub mod cri_client;
pub mod cri_grpc;
pub mod csi;
pub mod csi_plugins;
pub mod health;
pub mod image_config;
pub mod kubeconfig;
pub mod kubelet;
pub mod node_status;
pub mod pod_manager;
pub mod portforward;
pub mod runtime;
pub mod server;
pub mod spdy;
pub mod vm_manager;
pub mod vm_network;
pub mod vm_restore;
pub mod vm_migrate;
pub mod vm_runtime;
pub mod vm_snapshot;

pub use checkpoint::CriuCheckpointer;
pub use cri_client::{CriClient, detect_cri_socket};
pub use cri_grpc::CriGrpcClient;
pub use kubelet::{detect_node_name, Kubelet, KubeletConfig};
pub use runtime::{NativeRuntime, NativeImageService};
pub use vm_runtime::{VmRuntime, VmConfig, VmmBackend};

mod fs_watch;

pub mod workload;
