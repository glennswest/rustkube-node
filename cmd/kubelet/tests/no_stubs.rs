//! No stubs (stormcentral#1455, #410): `cargo test` runs `deploy/check-stubs.sh`,
//! the same sweep stormcentral runs on main each night, so a stub marker fails
//! the build instead of being found afterwards.

use std::path::Path;
use std::process::Command;

#[test]
fn checkout_has_no_stub_markers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let out = Command::new("bash")
        .arg(root.join("deploy/check-stubs.sh"))
        .arg(&root)
        .output()
        .expect("run deploy/check-stubs.sh");
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
