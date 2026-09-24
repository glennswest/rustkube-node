//! Generate the gRPC clients the kubelet speaks, from vendored protos.
//! Requires `protoc` on the build host.
//!
//! - `proto/api.proto`: CRI v1 (kubernetes/cri-api, release-1.32)
//! - `proto/csi/csi.proto`: CSI spec v1.9.0, for the node plugins of external
//!   storage drivers (`csi.rs`)
//! - `proto/pluginregistration/api.proto`: the kubelet plugin-registration API
//!   (k8s.io/kubelet v0.32.0), which a driver's registrar serves on its socket in
//!   `/var/lib/kubelet/plugins_registry` (`csi_plugins.rs`)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Server codegen is for tests only (a mock CRI runtime, a mock CSI driver
    // and its registrar).
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/api.proto",
                "proto/csi/csi.proto",
                "proto/pluginregistration/api.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
