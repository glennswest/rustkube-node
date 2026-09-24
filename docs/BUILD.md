# Building rustkube-node

What this repository produces, and why it is a golden rather than a package.

## The short version

```bash
# on the build box, as root
scripts/build-golden.sh
```

That builds `kubelet` and `kube-proxy` as static musl binaries, asks the forge
for a volume, attaches it over NVMe/TCP, makes a filesystem on it, copies the
binaries in, and seals it. What comes out is a named, sealed volume with a
content digest — the thing a stormcos release composes over.

For a plain compile, without touching the forge:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

This needs `../rustkube` checked out as a sibling and `protoc` on the host, for
the gRPC codegen (CRI, CSI, and plugin registration).

On the build box, use `sc-build scripts/sc-build.sh` (optionally followed by
a command, which defaults to `cargo build && cargo test`). sc-build builds in a
scratch directory with no sibling, so the script clones rustkube inside the
scratch tree (`RUSTKUBE_REF`, default `main`) and points the path dependency
there.

## Why a golden

A stormcos node installs nothing. Everything it runs is a copy-on-write clone
of a sealed filesystem that is already on its disk, started by stormpump in
milliseconds. There is no package manager on a node, no `rpm -i`, and no step
where a binary is unpacked into a root filesystem — so a `.rpm` or a `.deb` is
the wrong shape for this platform however correct it is for another.

The packages this replaces were wrong twice over: `packaging/build-packages.sh`
runs `cargo build --release` **without** `--target`, so the binaries it packages
are glibc-linked and a node cannot exec them at all.

## Why there is no tar, and no image file

A golden is a filesystem. The forge can hand the build box an NVMe namespace
over TCP, so the filesystem is made **where it is going to live** and written
with `cp`. Nothing is serialised into an archive and read back out; no image
file is built and then copied into a volume; no loop device is involved.

That matters because the alternative moves every byte an extra time, and this
platform's whole storage argument is that a release *maps* content rather than
copying it. A build that starts by making a second copy of everything is
arguing against the thing it feeds.

It is also just ordinary Linux: `nvme connect`, `mkfs.ext4`, `mount`, `install`,
`umount`. There is nothing here that needs a special tool to understand.

## Why it stops reading this box's `target/` directory

Today the stormcos image build takes the binary straight off the build box:

```sh
musl_bin() { echo "$(target_dir "$1")/$MUSL_TRIPLE/release/$2"; }
```

That is not an artifact. It is whatever was compiled here last, and it has
already cost a day: on 2026-08-27 a golden was assembled around a binary from
an earlier build — the compile had been failing for hours behind a misread exit
status — and every log line named the right commit throughout. The only way to
tell was to grep the binary for a string the new code contained.

A sealed volume with a content digest makes that a lookup instead of an
investigation. This is `stormcos#16`.

## What the golden contains

```
/usr/bin/kubelet
/usr/bin/kube-proxy
/etc/rustkube-node.provenance     what built it
/proc  /sys  /dev  /etc  /tmp     mount points a runtime expects
```

It is formatted **read-only** — `-O ^has_journal -m 0`. Nothing ever writes to
a content golden: a journal exists to make a write survivable and there are no
writes, and the 5% of blocks ext4 reserves for root to recover a full
filesystem is pure overhead in every clone, on every node, for ever. A workload
that needs to write takes a PVC.

## Provenance

The golden carries a line naming what produced it:

```
rustkube-node@b1aa3dc kubelet:3f9a1c2e8d4b5a60 kube-proxy:7c1d0e5f2a9b3846
```

A digest says the bytes are these bytes; it does not say what made them, and
that is the question asked when something is wrong. A golden that recorded only
a tarball hash once sent an issue to the wrong project entirely.

A dirty tree is recorded as `@<commit>+dirty`, because an image built from
uncommitted work cannot be reproduced, rolled back to, or described by the
commit its manifest records.

## Environment

| Variable | Default | What it is |
|---|---|---|
| `STORMBLOCK_ENGINE` | `http://forge.g16.lo:9090` | the forge's management API |
| `GOLDEN_NQN` | `nqn.2026-09.lo.g16:stormcos` | the NVMe subsystem to attach from |
| `GOLDEN_NAME` | `rustkube-node` | the golden's name; the volume is `golden-<name>` |
| `GOLDEN_SIZE` | `96M` | argument 1 overrides |
| `MUSL_TRIPLE` | `x86_64-unknown-linux-musl` | the target |

`MUSL_TRIPLE` is the one to change for another architecture — and it is worth
saying that the fleet is x86_64 today. An ARM default that outlived its
hardware has already shipped goldens full of binaries no node could exec, and
nothing caught it, because a filesystem of ARM binaries seals and verifies
exactly as well as one of x86.

## Requirements on the build box

`nvme-cli`, `mkfs.ext4`, `curl`, `python3`, a Rust toolchain with the musl
target, and root — it mounts a filesystem. It is meant to run on `dev.g8.lo`.

## What is still duplicated

The volume-open, export, attach and seal sequence here is a leaner copy of what
`stormpump/deploy/build-goldens.sh` does for every other component. That is
worth extracting into one place both can call; until then, the hard-won parts
of that script — redialling a connection that outlived an engine restart,
never leaving an export standing for a device that never appeared — are
reproduced here deliberately rather than rediscovered later.
