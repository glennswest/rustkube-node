#!/usr/bin/env bash
# Builds the rustkube-node golden tree with deploy/build-image.sh and checks it
# against what stormcos's stage made (#480). On a build VM:
#
#   SC_TOOLS=stormd sc-build 'bash deploy/test-build-image.sh'
#
# stormd comes from its newest golden ($STORMD_BIN, sc-build's SC_TOOLS), the
# way a component build is given the stormd input image; STORMD_STATIC works too.
set -euo pipefail
cd "$(dirname "$0")/.."
STORMD=${STORMD_STATIC:-${STORMD_BIN:-}}
[ -n "$STORMD" ] || { echo "test-build-image: no stormd (SC_TOOLS=stormd, or STORMD_STATIC)" >&2; exit 2; }
w=$(mktemp -d); trap 'rm -rf "$w"' EXIT
fails=0
ok()   { echo "ok   $*"; }
fail() { echo "FAIL $*"; fails=$((fails + 1)); }
check() { local what=$1; shift; if "$@"; then ok "$what"; else fail "$what"; fi; }

# The refusals first: they exit before any build.
rc=0; bash deploy/build-image.sh rustkube "$w/x" >/dev/null 2>&1 || rc=$?
check "another name exits 2 ($rc)" test "$rc" = 2
rc=0; GOLDEN_ARCH=aarch64 bash deploy/build-image.sh rustkube-node "$w/x" >/dev/null 2>&1 || rc=$?
check "GOLDEN_ARCH=aarch64 exits 2 ($rc)" test "$rc" = 2
rc=0; env -u STORMD_STATIC -u GOLDEN_INPUT_STORMD bash deploy/build-image.sh rustkube-node "$w/x" >/dev/null 2>&1 || rc=$?
check "no stormd exits 1 ($rc)" test "$rc" = 1

# The build, through the contract's entry point, stormd as the input image's
# directory (bin/stormd), as a component build would mount it.
mkdir -p "$w/in/stormd/bin"; cp "$STORMD" "$w/in/stormd/bin/stormd"
out=$w/tree
env -u STORMD_STATIC GOLDEN_INPUT_STORMD="$w/in/stormd" bash deploy/build-golden.sh rustkube-node "$out" | tee "$w/log"
T=$out

for b in kubelet kube-proxy; do
    check "usr/bin/$b is an executable file" test -f "$T/usr/bin/$b" -a -x "$T/usr/bin/$b" -a ! -L "$T/usr/bin/$b"
    check "usr/bin/$b is static" bash -c "file '$T/usr/bin/$b' | grep -Eq 'static-pie linked|statically linked'"
    check "usr/bin/$b is the one cargo built" cmp -s "$T/usr/bin/$b" "target/x86_64-unknown-linux-musl/release/$b"
done
check "usr/bin/kubelet runs (--help)" bash -c "'$T/usr/bin/kubelet' --help >/dev/null 2>&1"
check "/stormd is the given stormd" cmp -s "$T/stormd" "$STORMD"
check "/stormd is 0755" test "$(stat -c %a "$T/stormd")" = 755
applets=$("$STORMD" --list-commands)
n=0; bad=""
for a in $applets; do
    n=$((n + 1))
    [ "$(readlink "$T/bin/$a")" = ../stormd ] || bad="$bad bin/$a"
    [ "$(readlink "$T/usr/bin/$a")" = ../../stormd ] || bad="$bad usr/bin/$a"
done
check "all $n applets linked in bin/ and usr/bin/${bad:+ (not:$bad)}" test -z "$bad"
check "usr/bin/rm (cni-stale-config's command) is an applet" test "$(readlink "$T/usr/bin/rm")" = ../../stormd
bad=""
for d in bin sbin usr/bin usr/sbin etc/stormd data dev proc sys run tmp var/log/stormd .stormd \
         hostrun pallets var/log/pods var/lib/kubelet etc/cni/net.d opt/cni/bin run/cilium run/stormpump hostroot etc/kubernetes; do
    [ -d "$T/$d" ] && [ ! -L "$T/$d" ] || bad="$bad $d"
done
check "every mount point is a directory${bad:+ (not:$bad)}" test -z "$bad"
check ".stormd/ is empty" test -z "$(ls -A "$T/.stormd")"
check "var/run -> ../run" test "$(readlink "$T/var/run")" = ../run
check "tmp/ is 1777" test "$(stat -c %a "$T/tmp")" = 1777
check "no mirror-namespaces.json (stormcos's content, rustkube#403 A)" test ! -e "$T/etc/kubernetes/mirror-namespaces.json"
check "etc/kubernetes/ is empty" test -z "$(ls -A "$T/etc/kubernetes")"

C=$T/etc/stormd/config.toml
check "config.toml is 0644" test "$(stat -c %a "$C")" = 644
if python3 -c 'import tomllib' 2>/dev/null; then
    check "config.toml parses, as the stage's processes" python3 - "$C" << 'PY'
import sys, tomllib
c = tomllib.load(open(sys.argv[1], "rb"))
assert c["general"]["name"] == "rustkube-node", c["general"]
assert c["api"] == {"bind": "0.0.0.0:9085"}, c["api"]
assert c["api_health"]["state_file"] == "/run/stormpump/health.d/rustkube-node.json"
p = {x["name"]: x for x in c["process"]}
assert list(p) == ["cni-stale-config", "kubelet"], list(p)
k = p["kubelet"]
assert k["command"] == "/usr/bin/kubelet"
assert "--apiserver" not in k["args"], "the apiserver flag stays in args"
assert k["env_default"] == {"APISERVER_URL": "https://${NODE_IP}:6443"}, k.get("env_default")
a = k["args"]
for f, v in [("--runtime", "stormpump"), ("--cri-socket", "/hostrun/stormpump.sock"), ("--max-pods", "250"),
             ("--node-name", "${NODE_NAME}"), ("--pod-manifest-path", "/run/kubernetes/manifests"),
             ("--stormblock", "https://127.0.0.1:9090"), ("--registry", "https://127.0.0.1:5100")]:
    assert a[a.index(f) + 1] == v, (f, v)
assert k["depends_on"] == ["cni-stale-config"]
assert k["api"][0]["url"] == "https://127.0.0.1:10250/pods"
assert c["stormlog"]["file"] == {"max_size_bytes": 8388608, "max_files": 3, "max_runs": 5}
PY
else
    echo "skip config.toml parse: no python3 tomllib"
fi
check "the two @@SHIPS_WITH lines" test "$(grep -c '^@@SHIPS_WITH rustkube-node ' "$w/log")" = 2

# STORMD_API_TLS=1 adds stormd's API TLS, as the stage's stormd_api_tls.
env -u STORMD_STATIC GOLDEN_INPUT_STORMD="$w/in/stormd" STORMD_API_TLS=1 bash deploy/build-image.sh rustkube-node "$w/tls" > /dev/null
check "STORMD_API_TLS=1: [api] has the stormd pair" grep -qx 'tls_cert_file = "/data/stormcert/stormd.crt"' "$w/tls/etc/stormd/config.toml"
check "STORMD_API_TLS unset: no TLS" bash -c "! grep -q tls_cert_file '$C'"

echo "== $fails failed"
[ "$fails" = 0 ]
