#!/usr/bin/env bash
# Build release binaries and package them as rpm and deb.
# Run on a Linux build host (e.g. dev.g8.lo). Output: target/packages/.
#
# usage: packaging/build-packages.sh [version]
set -euo pipefail

VERSION="${1:-0.1.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/target/packages"
mkdir -p "$OUT"

echo "==> cargo build --release"
cargo build --release --manifest-path "$ROOT/Cargo.toml"

BINARIES=(kubelet kube-proxy)
UNITS=("$ROOT/deploy/systemd/kubelet.service" "$ROOT/deploy/systemd/kube-proxy.service")
ENVS=("$ROOT/packaging/env/kubelet.env" "$ROOT/packaging/env/kube-proxy.env")

# ---------------------------------------------------------------- rpm
if command -v rpmbuild >/dev/null 2>&1; then
    echo "==> building rpm"
    TOP="$(mktemp -d)"
    mkdir -p "$TOP"/{SOURCES,SPECS,RPMS,BUILD,BUILDROOT}
    for b in "${BINARIES[@]}"; do cp "$ROOT/target/release/$b" "$TOP/SOURCES/"; done
    cp "${UNITS[@]}" "$TOP/SOURCES/"
    cp "${ENVS[@]}" "$TOP/SOURCES/"
    rpmbuild -bb \
        --define "_topdir $TOP" \
        --define "pkg_version $VERSION" \
        "$ROOT/packaging/rpm/rustkube-node.spec"
    cp "$TOP"/RPMS/*/*.rpm "$OUT/"
    rm -rf "$TOP"
else
    echo "==> rpmbuild not found, skipping rpm" >&2
fi

# ---------------------------------------------------------------- deb
if command -v dpkg-deb >/dev/null 2>&1; then
    echo "==> building deb"
    ARCH="$(dpkg --print-architecture 2>/dev/null || echo amd64)"
    STAGE="$(mktemp -d)"
    PKG="$STAGE/rustkube-node_${VERSION}_${ARCH}"
    mkdir -p "$PKG/DEBIAN" "$PKG/usr/bin" "$PKG/lib/systemd/system" "$PKG/etc/kubernetes"

    for b in "${BINARIES[@]}"; do install -m0755 "$ROOT/target/release/$b" "$PKG/usr/bin/"; done
    install -m0644 "${UNITS[@]}" "$PKG/lib/systemd/system/"
    install -m0644 "$ROOT/packaging/env/kubelet.env" "$PKG/etc/kubernetes/kubelet"
    install -m0644 "$ROOT/packaging/env/kube-proxy.env" "$PKG/etc/kubernetes/kube-proxy"

    sed -e "s/@VERSION@/$VERSION/" -e "s/@ARCH@/$ARCH/" \
        "$ROOT/packaging/deb/control.in" > "$PKG/DEBIAN/control"
    printf '/etc/kubernetes/kubelet\n/etc/kubernetes/kube-proxy\n' > "$PKG/DEBIAN/conffiles"
    cat > "$PKG/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
systemctl daemon-reload >/dev/null 2>&1 || true
EOF
    cat > "$PKG/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
if [ "$1" = remove ]; then
    systemctl stop kubelet.service kube-proxy.service >/dev/null 2>&1 || true
fi
EOF
    chmod 0755 "$PKG/DEBIAN/postinst" "$PKG/DEBIAN/prerm"

    dpkg-deb --build --root-owner-group "$PKG" "$OUT/"
    rm -rf "$STAGE"
else
    echo "==> dpkg-deb not found, skipping deb" >&2
fi

echo "==> packages in $OUT:"
ls -la "$OUT"
