#!/usr/bin/env bash
# Build the rustkube-node golden: kubelet and kube-proxy, in a sealed
# filesystem on the forge, ready for a release to compose over.
#
# Run on the build box (dev.g8.lo). Needs `nvme-cli`, `mkfs.ext4` and root —
# it attaches a volume from the forge and mounts it.
#
# usage: scripts/build-golden.sh [size]
#
# ---------------------------------------------------------------------------
# Why there is no tar, and no image file
#
# A golden is a filesystem. The forge can hand this machine an NVMe namespace
# over TCP, so the filesystem can be made *where it will live* and written
# with `cp` — no archive to serialise into and back out of, no image file to
# build and then copy into a volume, no loop device, and nothing clever.
#
# The two paths this replaces both moved every byte an extra time:
#
#   * `packaging/build-packages.sh` builds an rpm and a deb. They are the
#     wrong shape for this platform — a node installs nothing — and they are
#     built without `--target`, so they carry glibc binaries a node cannot
#     run.
#   * The stormcos image build reads `target/x86_64-unknown-linux-musl/
#     release/kubelet` straight off this box's filesystem. That is not an
#     artifact, it is whatever happened to be compiled here last, and it has
#     already shipped a stale binary while the log named the right commit.
#
# What comes out of this is a named, sealed volume on the forge with a content
# digest, which a release composes over by *mapping* rather than copying.
# ---------------------------------------------------------------------------
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SIZE="${1:-${GOLDEN_SIZE:-96M}}"
NAME="${GOLDEN_NAME:-rustkube-node}"
ENGINE="${STORMBLOCK_ENGINE:-http://forge.g16.lo:9090}"
NQN="${GOLDEN_NQN:-nqn.2026-09.lo.g16:stormcos}"
TRIPLE="${MUSL_TRIPLE:-x86_64-unknown-linux-musl}"
BINARIES=(kubelet kube-proxy)

say() { printf '   %s\n' "$*" >&2; }
api() {
    local method="$1" path="$2" body="${3:-}"
    if [ -n "$body" ]; then
        curl -fsS -X "$method" "$ENGINE/api/v1$path" \
            -H 'Content-Type: application/json' -d "$body"
    else
        curl -fsS -X "$method" "$ENGINE/api/v1$path"
    fi
}
jfield() { python3 -c 'import sys,json;print(json.load(sys.stdin).get(sys.argv[1],""))' "$1"; }

for t in nvme mkfs.ext4 curl python3; do
    command -v "$t" >/dev/null || { echo "ERROR: $t is not installed" >&2; exit 2; }
done
[ "$(id -u)" = 0 ] || { echo "ERROR: needs root — this mounts a filesystem" >&2; exit 2; }

# ---------------------------------------------------------------- the binaries
#
# musl, statically linked. A node has no shared libraries to speak of, and a
# glibc binary there fails at exec with a message about an interpreter.
say "building $TRIPLE"
cargo build --release --target "$TRIPLE"

for b in "${BINARIES[@]}"; do
    [ -x "target/$TRIPLE/release/$b" ] || {
        echo "ERROR: target/$TRIPLE/release/$b was not built" >&2; exit 1; }
done

# The provenance this golden will carry. A digest says the bytes are these
# bytes; it does not say what produced them, and that is the question asked
# when something is wrong.
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
DIRTY=""
[ -n "$(git status --porcelain 2>/dev/null)" ] && DIRTY="+dirty"
PROV="rustkube-node@${COMMIT}${DIRTY}"
for b in "${BINARIES[@]}"; do
    PROV="$PROV $b:$(sha256sum "target/$TRIPLE/release/$b" | cut -c1-16)"
done
say "provenance: $PROV"

# ---------------------------------------------------------------- the volume
VOL="golden-$NAME"
say "asking $ENGINE for $VOL ($SIZE)"
ID="$(api GET "/volumes" | python3 -c '
import sys,json
want=sys.argv[1]
for v in json.load(sys.stdin).get("items",[]):
    if v.get("name")==want: print(v["id"]); break
' "$VOL")"

if [ -n "$ID" ]; then
    # Sealed means immutable between builds. A rebuild is the one thing
    # entitled to lift that.
    [ "$(api GET "/volumes/$ID" | jfield sealed)" = "True" ] \
        && api DELETE "/volumes/$ID/seal" >/dev/null
else
    ID="$(api POST /volumes \
        "{\"name\":\"$VOL\",\"size\":\"$SIZE\",\"tier\":\"hot\",\"role\":\"data\"}" | jfield id)"
    [ -n "$ID" ] || { echo "ERROR: the engine refused a $SIZE volume" >&2; exit 1; }
fi
say "volume $ID"

EXPORT="$(api POST /exports "{\"volume_id\":\"$ID\",\"protocol\":\"nvmeof\"}")"
EXID="$(printf '%s' "$EXPORT" | jfield id)"
NSID="$(printf '%s' "$EXPORT" | jfield nsid)"
[ -n "$EXID" ] || { echo "ERROR: the engine would not export $VOL" >&2; exit 1; }

cleanup() {
    [ -n "${MNT:-}" ] && mountpoint -q "$MNT" && { sync; umount "$MNT"; }
    [ -n "${MNT:-}" ] && [ -d "$MNT" ] && rmdir "$MNT" 2>/dev/null || true
    # Never leave an export standing for a device that never appeared: the
    # volume then cannot be deleted or reopened, and the next build fails
    # somewhere else entirely.
    [ -n "${EXID:-}" ] && api DELETE "/exports/$EXID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

HOST="${ENGINE#*://}"; HOST="${HOST%%:*}"
nvme connect -t tcp -a "$HOST" -s 4420 -n "$NQN" >/dev/null 2>&1 || true

DEV=""
for _ in $(seq 40); do
    for blk in /sys/block/nvme*; do
        [ -r "$blk/nsid" ] || continue
        if [ "$(cat "$blk/nsid")" = "$NSID" ]; then DEV="/dev/$(basename "$blk")"; break 2; fi
    done
    sleep 0.5
done
[ -n "$DEV" ] || { echo "ERROR: no device appeared for nsid $NSID on $NQN" >&2; exit 1; }
say "attached $DEV"

# ---------------------------------------------------------------- the content
#
# Read-only: nothing ever writes to a content golden, so it carries neither a
# journal nor the 5% ext4 reserves for root to recover a full filesystem. Both
# would be inherited by every clone on every node, for ever.
mkfs.ext4 -q -F -b 4096 -O ^has_journal -m 0 -L "${NAME:0:16}" "$DEV"
MNT="$(mktemp -d)"
mount "$DEV" "$MNT"

install -d -m 0755 "$MNT/usr/bin"
for b in "${BINARIES[@]}"; do
    install -m 0755 "target/$TRIPLE/release/$b" "$MNT/usr/bin/$b"
    say "$(printf '%-12s %s' "$b" "$(du -h "$MNT/usr/bin/$b" | cut -f1)")"
done

# The mount points a container runtime expects to find. `podman export` does
# not emit the empty directories an image declares, and a golden with no
# /proc, /sys or /dev has them mounted silently and unsuccessfully, which
# surfaces inside the container as something else entirely.
install -d -m 0555 "$MNT/proc" "$MNT/sys"
install -d -m 0755 "$MNT/dev" "$MNT/etc"
install -d -m 1777 "$MNT/tmp"

printf '%s\n' "$PROV" > "$MNT/etc/rustkube-node.provenance"

sync
umount "$MNT"; rmdir "$MNT"; MNT=""

# ---------------------------------------------------------------- seal
api POST "/volumes/$ID/seal" >/dev/null
DIGEST="$(api GET "/volumes/$ID" | jfield content_digest)"
say "sealed $VOL${DIGEST:+ — $DIGEST}"

cleanup; trap - EXIT
nvme disconnect -n "$NQN" >/dev/null 2>&1 || true

printf '%s\t%s\t%s\n' "$NAME" "$VOL" "$PROV"
