#!/bin/sh
# Build rustkube-node's test binary for the commit checked out, and stage it
# where test/Containerfile copies it from (test/.stage/test).
#
# stormcentral runs this on the build box before `podman build -f
# test/Containerfile <repo root>` (stormcentral docs/test-standard.md). By hand:
#
#   test/build.sh && podman build -f test/Containerfile -t rustkube-node-test .
#
# The crate is its own workspace (test/Cargo.toml); the binary is static
# (musl), so the image is scratch.
set -eu
target=${1:-x86_64-unknown-linux-musl}
root=$(cd "$(dirname "$0")/.." && pwd)
manifest="$root/test/Cargo.toml"

cargo build --release --locked --target "$target" --manifest-path "$manifest"
tdir=$(cargo metadata --format-version 1 --no-deps --manifest-path "$manifest" |
    sed 's/.*"target_directory":"\([^"]*\)".*/\1/')
mkdir -p "$root/test/.stage"
cp "$tdir/$target/release/rustkube-node-test" "$root/test/.stage/test"
echo "staged $root/test/.stage/test"
