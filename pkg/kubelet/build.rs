//! Generate the CRI v1 gRPC client from the vendored kubernetes/cri-api
//! proto (release-1.32). Requires `protoc` on the build host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Server codegen is for tests only (mock CRI runtime).
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/api.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/api.proto");
    Ok(())
}
