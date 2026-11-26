#!/usr/bin/env bash
# The `rustkube-node` golden — the node agent's image: the kubelet and
# kube-proxy on a stormd base — built here, from this checkout, and nowhere
# else (#480, stormcentral#1836: "do repos, repos are not expensive";
# stormcos's stage mode is retired).
#
#   deploy/build-image.sh rustkube-node OUT
#
# stormcentral's component build runs it for the `rustkube-node` component
# (kind `tree`: `deploy/build-golden.sh rustkube-node OUT`, which execs this)
# in a checkout fetched at the exact commit, on a fresh build VM, as root of
# the build's own user namespace. OUT is copied into the sealed read-only
# golden `golden-rustkube-node-<build>`, sized from the content. It is the
# `rustkube-node` member of the system pallet, laid at /pallets/rustkube-node
# and run by stormpump's node-agent unit, whose PID 1 is /stormd.
#
# The tree, as stormcos's deploy/build-goldens.sh staged it until 2026-11-26
# (f7f7ccb: its "rustkube-node — the kubelet and kube-proxy" section and the
# stormdbase helpers it calls, moved here with their content unchanged):
#
#   stormd                  the stormd binary, from the `stormd` input image
#   bin/<a> usr/bin/<a>     every applet stormd lists (`--list-commands`),
#                           links to ../stormd and ../../stormd
#   usr/bin/kubelet         static x86_64 musl, the release profile
#   usr/bin/kube-proxy      the same (not run: Cilium replaces it)
#   etc/stormd/config.toml  stormd's config: cni-stale-config, the kubelet and
#                           its API health (below)
#   .stormd/                empty: the release's one stormd, bound read-only
#                           (stormcos#822, #465)
#   var/log/stormd/         the log volume's mount point
#   var/run -> ../run       so cilium-cni, exec'd in this mount namespace,
#                           finds /var/run/cilium/cilium.sock on the host's /run
#   etc/kubernetes/         empty: the mount point for what the node gives it
#   hostrun pallets var/log/pods var/lib/kubelet etc/cni/net.d opt/cni/bin
#   run/cilium run/stormpump hostroot data dev proc sys run tmp sbin usr/sbin
#                           empty mount points (a bind onto a missing target
#                           fails with ENOENT and the container never starts)
#
# One change from the stage, by the master's ruling for this move (rustkube#403,
# option A: "one repo, one image, nothing of another component's inside it"):
# the stage also wrote etc/kubernetes/mirror-namespaces.json from stormcos's
# deploy/node/projects.toml. That map is stormcos's content, so stormcos
# supplies it (the kubelet reads `--mirror-namespaces`/`MIRROR_NAMESPACES` and
# watches its directory, #418); this image does not carry it.
#
# stormd: `GOLDEN_INPUT_STORMD` (the mounted input image's directory, which
# holds bin/stormd) or `STORMD_STATIC` (the binary). Neither is an error:
# stormd is never compiled per image. `STORMD_API_TLS=1` adds TLS to stormd's
# [api] (off by default, as in the stage).
#
# x86_64 only, as in stage mode: any other GOLDEN_ARCH is refused.
set -euo pipefail
name=${1:?usage: deploy/build-image.sh rustkube-node OUT}
out=${2:?usage: deploy/build-image.sh rustkube-node OUT}
[ "$name" = rustkube-node ] || { echo "build-image: this repo builds only 'rustkube-node', not '$name'" >&2; exit 2; }
arch=${GOLDEN_ARCH:-x86_64}
[ "$arch" = x86_64 ] || { echo "build-image: rustkube-node is built for x86_64 only (GOLDEN_ARCH=$arch)" >&2; exit 2; }
cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true

# stormd, from its input image: never compiled here.
STORMD=${STORMD_STATIC:-}
if [ -z "$STORMD" ] && [ -n "${GOLDEN_INPUT_STORMD:-}" ]; then STORMD="$GOLDEN_INPUT_STORMD/bin/stormd"; fi
[ -n "$STORMD" ] || { echo "build-image: no stormd: set GOLDEN_INPUT_STORMD (the stormd input image) or STORMD_STATIC" >&2; exit 1; }
[ -f "$STORMD" ] && [ -x "$STORMD" ] || { echo "build-image: stormd $STORMD is not an executable file" >&2; exit 1; }
applets=$("$STORMD" --list-commands) || { echo "build-image: $STORMD --list-commands failed: cannot tell which applets to link" >&2; exit 1; }
[ -n "$applets" ] || { echo "build-image: $STORMD lists no applets" >&2; exit 1; }
# The kubelet's apiserver comes from env.d (#282, stormcluster#21), which needs
# a stormd with env_default (stormd#37). The stage kept the flag in args for an
# older one; no stormd image that old is left, so one is refused rather than
# shipped with a kubelet that cannot be pointed at a cluster.
grep -aq 'env_default' "$STORMD" || { echo "build-image: $STORMD has no env_default (stormd#37): the kubelet's APISERVER_URL could not be set" >&2; exit 1; }

TARGET=x86_64-unknown-linux-musl
command -v rustup >/dev/null 2>&1 && rustup target add "$TARGET" >/dev/null 2>&1 || true
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
commit=$(git rev-parse HEAD)
# The profile a golden ships: `dist` when the workspace defines one, else
# `release` (the stage's rule).
profile=release; grep -q '^\[profile\.dist\]' Cargo.toml && profile=dist

# The whole workspace, as the stage built it (`cargo build` at the root): a
# narrower build can unify features differently and give other bytes.
echo "==> cargo build --profile $profile --locked --target $TARGET ($commit)"
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)" \
    cargo build --profile "$profile" --locked --target "$TARGET" --quiet
NB="$CARGO_TARGET_DIR/$TARGET/$profile"
for b in kubelet kube-proxy; do
    [ -x "$NB/$b" ] || { echo "build-image: cargo produced no $NB/$b" >&2; exit 1; }
    # Static: the golden has no loader and no libc.
    if command -v file >/dev/null 2>&1; then
        file "$NB/$b" | grep -Eq 'static-pie linked|statically linked' || { echo "build-image: $b is not static: $(file "$NB/$b")" >&2; exit 1; }
    fi
done

# The stormdbase root (the stage's stormdbase_stage).
mkdir -p "$out"/{bin,usr/bin,sbin,usr/sbin,etc/stormd,data,dev,proc,sys,run,tmp}
mkdir -p "$out/var/log/stormd" "$out/.stormd"
install -m 0755 "$STORMD" "$out/stormd"
for cmd in $applets; do
    ln -sfn ../stormd    "$out/bin/$cmd"
    ln -sfn ../../stormd "$out/usr/bin/$cmd"
done

install -m 0755 "$NB/kubelet" "$NB/kube-proxy" "$out/usr/bin/"
mkdir -p "$out/hostrun" "$out/pallets" "$out/var/log/pods" \
         "$out/var/lib/kubelet" \
         "$out/etc/cni/net.d" "$out/opt/cni/bin" \
         "$out/run/cilium" "$out/run/stormpump" "$out/hostroot" \
         "$out/etc/kubernetes"
rm -rf "$out/var/run"
ln -sfn ../run "$out/var/run"

cat > "$out/etc/stormd/config.toml" << 'CONF'
[general]
name = "rustkube-node"
log_dir = "/var/log/stormd"

[api_health]
# This container's API health for PID 1's node summary (#458, stormd#52):
# stormd writes its probes' states here, and the unit binds health.d in.
state_file = "/run/stormpump/health.d/rustkube-node.json"

[api]
bind = "0.0.0.0:9085"

# Last boot's Cilium CNI config goes before the kubelet starts (#646). Cilium
# writes /etc/cni/net.d/05-cilium.conflist only once its agent is ready
# (write-cni-conf-when-ready, 50-cilium-config.yaml), but /etc/cni/net.d is the
# cni-netd volume (#470) and keeps the file across a reboot or a power cut. So
# the kubelet found a network config at once and sent every pod to cilium-cni
# before there was an agent: the ADD made the host veth, failed, the DEL could
# not remove it (the agent never had the endpoint), and each retry on that
# sandbox met "unable to create veth pair: file exists" (×17 on pvetest1,
# coredns +18 s). Without the file the kubelet waits for the agent as upstream
# does. stormd's own rm applet (this golden has no shell, #579); -f, so a
# first boot or a flowsdn node with no such file is a clean exit.
[[process]]
name = "cni-stale-config"
command = "/usr/bin/rm"
args = ["-f", "/etc/cni/net.d/05-cilium.conflist"]
on_failure = "restart"
on_exit = "stop"

[[process]]
name = "kubelet"
command = "/usr/bin/kubelet"
# The node agent is the newest thing here and the one still being brought up.
env = { RUST_LOG = "info,kubelet=debug" }
args = [
  "--runtime", "stormpump",
  # The engine's socket, on the host's /run bound in at /hostrun. Not a CRI
  # socket — the flag is shared with the CRI runtime, and what is behind it
  # here is stormpump's ring.
  "--cri-socket", "/hostrun/stormpump.sock",
  # The CA this node minted for itself, so the kubelet can verify the
  # apiserver — and so that every pod's ca.crt is the same anchor rather than
  # a copy that can drift from it.
  "--apiserver-ca", "/data/stormcert/ca.crt",
  # Who it is (#76): system:node:<name> in system:nodes, minted by the
  # apiserver golden for this hostname.
  "--client-certificate", "/data/stormcert/kubelet.crt",
  "--client-key", "/data/stormcert/kubelet.key",
  # What it serves :10250 with (#81): from the node CA, minted by the
  # apiserver golden. Not waited for: stormd's `stat` and friends exit 0 on a
  # missing file (stormd#31), so there is no wait to write. A first
  # boot where the kubelet starts before the mint gets the self-signed
  # fallback until its next restart (rustkube-node#89), and
  # deploy/test/check-security.sh reports it.
  "--tls-cert-file", "/data/stormcert/kubelet-serving.crt",
  "--tls-private-key-file", "/data/stormcert/kubelet-serving.key",
  "--node-name", "${NODE_NAME}",
  # The node registry over TLS against the node CA (#355, rustkube-node#167);
  # it presents the engine token, which the registry takes as admin.
  "--registry", "https://127.0.0.1:5100",
  "--registry-ca", "/data/stormcert/ca.crt",
  # The engine over https, against the node CA (#374, rustkube-node#312); its
  # stormvm-block paths read the same file by default (stormvm#110).
  "--stormblock", "https://127.0.0.1:9090",
  "--stormblock-ca", "/data/stormcert/ca.crt",
  # Live migration's TLS (#682, rustkube-node#40, stormvm#30): qemu's memory
  # stream between two kubelets, each checking the other's certificate against
  # its node name. The kubelet-serving pair is that certificate: a join's
  # `stormcert-agent adopt --install` makes it the cluster-issued node
  # certificate (stormcluster ADOPTED_PAIRS; CN system:node:<node>, the
  # node's names, serverAuth and clientAuth) and ca.crt the cluster's anchors,
  # so two joined nodes trust each other. Read only when a migration starts and
  # copied into the machine's run directory then. An SNO never migrates; its
  # pair is the node CA's. The other half, --stormstorage, comes from env.d
  # (STORMSTORAGE_URL) once the cluster's stormstorage exists (#354).
  "--migration-tls-cert", "/data/stormcert/kubelet-serving.crt",
  "--migration-tls-key", "/data/stormcert/kubelet-serving.key",
  "--migration-tls-ca", "/data/stormcert/ca.crt",
  # Its credential for the cluster stormstorage's disk-migration verbs
  # (stormstorage#117): kube-system/vm-migration's token, read at each call.
  "--stormstorage-token-file", "/data/stormcert/vm-migration.token",
  # Pods per node: OpenShift's 250, not upstream's 110 (owner 2026-10-06,
  # rustkube#205, #329). A 100-Pod burst on pvetest1 waited ~60 s for slots at
  # 110. The kubelet writes it to the Node's capacity and allocatable pods
  # (rustkube-node#165); the pod range per node follows (stormcos-cilium, /23).
  "--max-pods", "250",
  # Static pods (#559, owner #556 (C)): the control plane runs here, so its
  # probes are the kubelet's. boot.d 30-kube's tasks place each pod's manifest
  # on the host's /run (bound in at /run), only on sno and master nodes; the
  # kubelet watches the directory, before it exists too.
  "--pod-manifest-path", "/run/kubernetes/manifests",
]
# The apiserver URL from env (#282, stormcluster#21): a worker's kubelet, or a
# demoted master's, points at the cluster's endpoint, which stormcluster writes
# into env.d/rustkube-node. stormd sets an env_default key only when it did not
# inherit it, and args always win over env, so the flag is not in args.
env_default = { APISERVER_URL = "https://${NODE_IP}:6443" }
on_failure = "restart"
on_exit = "restart"
depends_on = ["cni-stale-config"]
# Ready when the apiserver answers. A ready_probe gates only this stormd's
# dependents (stormd README): the kubelet itself starts at once, as it must to
# run static pods before there is an apiserver (#559).
ready_probe = { type = "http", url = "https://127.0.0.1:6443/healthz", interval_secs = 2 }

# API health (#458): the pods it runs, a real read of its own state. The
# kubelet authenticates the bearer and authorises it through the apiserver
# (TokenReview, SubjectAccessReview), so an apiserver stall shows here too;
# the apiserver's own probe says which. node-admin's token, from tier-0.
[[process.api]]
name = "pods"
url = "https://127.0.0.1:10250/pods"
p99_ms = 300
timeout_secs = 10
token_file = "/data/stormcert/node-admin.token"
restart_after_stalled_secs = 600

# kube-proxy is not run: Cilium replaces it.
#
# `kube-proxy-replacement: "true"` puts service load balancing in eBPF, which
# removes the whole iptables service dataplane — and with it the module hunt
# that cost several boots, since every xt_ extension kube-proxy reached for had
# to exist on a node that could not load one. Two implementations of the same
# thing, one of them programming rules the other would fight over, is worse
# than either.

# The log volume is 64 MiB, so the defaults — rotate at 100 MiB, keep 10
# generations — would never rotate and would simply fill it. Sized to what is
# actually there: 8 MiB a file, three generations, five finished runs, which is
# ~40 MiB worst case and leaves room.
[stormlog.file]
max_size_bytes = 8388608
max_files = 3
max_runs = 5
CONF
# TLS on stormd's process API (#371, stormd#32): off until the console reads
# over TLS, as in the stage; the platform's [build] stormd_api_tls turns it on.
if [ "${STORMD_API_TLS:-0}" = 1 ]; then
    sed -i '/^\[api\]/,/^bind = /{/^bind = /a\
tls_cert_file = "/data/stormcert/stormd.crt"\
tls_key_file = "/data/stormcert/stormd.key"\
client_ca_file = ["/data/stormcert/ca.crt", "/run/stormblock/forge/ca.crt"]
}' "$out/etc/stormd/config.toml"
    grep -q '^tls_cert_file = "/data/stormcert/stormd.crt"$' "$out/etc/stormd/config.toml" || { echo "build-image: no [api] bind to add TLS to" >&2; exit 1; }
fi

# Modes as the stage's tree had them: directories 0755, /tmp 1777.
find "$out" -type d -exec chmod 0755 {} +
chmod 1777 "$out/tmp"
chmod 0644 "$out/etc/stormd/config.toml"

# Read back what was copied: a short copy is never sealed (stormcentral#716).
for b in kubelet kube-proxy; do
    [ "$(sha256sum < "$out/usr/bin/$b")" = "$(sha256sum < "$NB/$b")" ] || { echo "build-image: usr/bin/$b does not read back as built" >&2; exit 1; }
done
[ "$(sha256sum < "$out/stormd")" = "$(sha256sum < "$STORMD")" ] || { echo "build-image: stormd does not read back as given" >&2; exit 1; }

# What it ships with (#668), as the stage declared it: the engine over https
# (#374) and a registry that serves TLS and starts before its pair exists
# (#355, stormblock-registry#253).
echo "@@SHIPS_WITH rustkube-node stormblock@>=golden-stormblock-9e36b0662d66"
echo "@@SHIPS_WITH rustkube-node registry@>=golden-registry-8ba6d9542049"

echo "==> rustkube-node golden tree in $out: kubelet and kube-proxy at $commit, on $("$STORMD" --version 2>/dev/null | head -1)"
(cd "$out" && find . -mindepth 1 -not -path './bin/*' -not -path './usr/bin/*' -printf '%M %8s %P\n' | LC_ALL=C sort -k3)
echo "    + $(echo "$applets" | wc -w) applets linked in bin/ and usr/bin/"
sha256sum "$out/stormd" "$out/usr/bin/kubelet" "$out/usr/bin/kube-proxy" | sed "s|$out/||"
