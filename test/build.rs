//! The CSI driver's gRPC servers (`src/csi_driver.rs`, #52), from the protos
//! the kubelet vendors: CSI v1.9.0 and the kubelet plugin-registration API.
//! Requires `protoc` on the build host, as the kubelet's own build does.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &[
                "../pkg/kubelet/proto/csi/csi.proto",
                "../pkg/kubelet/proto/pluginregistration/api.proto",
            ],
            &["../pkg/kubelet/proto"],
        )?;
    println!("cargo:rerun-if-changed=../pkg/kubelet/proto");
    Ok(())
}
