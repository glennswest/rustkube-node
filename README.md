# rustkube-node

The **node level** of [rustkube](https://github.com/glennswest/rustkube) — the
Kubernetes worker components, in Rust. Split into its own repo for parallel
development; the code stays upstream-shaped and monorepo-mergeable.

> **Status: early / greenfield.** The libraries exist (ported from rustkube),
> the binaries build, but a node does not yet fully join a cluster or run pods.
> See the tracking issues.

## Components

Upstream-shaped: thin `cmd/<component>` binaries over `pkg/<lib>` libraries
(same layout as [rustkube](https://github.com/glennswest/rustkube)).

| Binary | cmd → pkg | Role |
|--------|-----------|------|
| `kubelet` | `cmd/kubelet` → `pkg/kubelet` | Node agent — registration, pod lifecycle, health probes, CRI/native/VM runtime |
| `kube-proxy` | `cmd/kube-proxy` → `pkg/proxy` | Service dataplane — iptables (today) / eBPF (planned) for ClusterIP/NodePort |
| — | `pkg/cni` | Standard CNI invoker (libcni-style) + built-in plugins (bridge, host-local IPAM, VXLAN) |

Binaries and systemd units use **exact upstream names** (`kubelet`,
`kube-proxy`, `kubelet.service`, `kube-proxy.service`), config under
`/etc/kubernetes/` — so this is a drop-in node.

## Runtime & networking defaults

- **Container runtime: CRI-O over gRPC** (`--runtime=cri`). The kubelet speaks
  the CRI v1 protocol over the Unix socket (`/run/crio/crio.sock`) — the same
  protocol OpenShift uses — via a tonic client generated from the vendored
  kubernetes/cri-api proto (K8s 1.32, `pkg/kubelet/proto/api.proto`). This
  gets full OCI image ecosystem compatibility for free. containerd works too.
- **CNI: standard plugins, default Cilium.** With CRI-O, the runtime invokes
  CNI itself from `/etc/cni/net.d` (Cilium writes `05-cilium.conflist`).
  For the native/VM runtimes, the kubelet invokes the standard CNI protocol
  directly (`pkg/cni/src/invoker.rs`) — any spec-compliant plugin works.
- The **native runtime** (`--runtime=native`, youki libcontainer, no
  containerd) and **VM runtime** (`--runtime=vm`) are experimental paths.
- Test target: **x86_64 Linux**.

## The kubelet API (`:10250`)

HTTPS, bearer-token auth (a static token, or one the apiserver accepts via
TokenReview); `/healthz`, `/livez` and `/readyz` are open.

| Route | What it is |
|---|---|
| `GET /metrics`, `/metrics/cadvisor`, `/stats/summary` | Node and pod metrics |
| `GET /pods` | The pods this kubelet manages |
| `GET /containerLogs/{ns}/{pod}/{container}` | What `kubectl logs` reads, by way of the apiserver proxy |
| `GET /vmConsole/{ns}/{name}/{door}` | A VM's `serial` or `vnc` console, answered by stormvm's console router mounted here |
| `DELETE /volumes/{ns}/{claim}` | Delete the stormblock clone behind a released claim |

The last two exist here because what they reach is on the node and the
control plane cannot get to it. Routing through the kubelet keeps the blast
radius at one node and reuses a hop the apiserver already authenticates,
rather than handing a controller credentials to every node's engine.

They reach it two different ways, and the difference is worth knowing.
`DELETE /volumes` **dials** stormblock, which serves its management API on
`127.0.0.1:9090` and is a separate engine with its own lifecycle. The console
is **mounted**: `stormvm-console` is a library that hands back an
`axum::Router`, so the doors run inside this process and the last hop is a
function call. There is no standalone node — every node runs rustkube, so
every node with a VM on it already has a kubelet, and a second long-lived
process whose only job was to serve consoles was one that never needed to
exist. Mounted here the doors also inherit this server's TLS and bearer auth
instead of stormvm's weaker "loopback, or a token" rule for an
unauthenticated node-local port.

`stormvm serve` still mounts the same router on `:9095` for a developer at a
terminal. That is a convenience for debugging a guest that will not boot, not
a deployment shape, and nothing in a cluster depends on it.

`DELETE /volumes` is what makes `reclaimPolicy: Delete` finish instead of
leaking: `204` when the clone is gone or was never there, `409` while a pod
on this node still has the claim, `503` when the node cannot establish that
it is unused. It never answers `204` on a guess — the volume name is derived
from the claim's, so a PV deleted over a surviving clone would let a later
claim of the same name in the same namespace adopt the previous tenant's
data.

## Storage

Claims of the built-in `stormblock` class are cloned and attached by the
node itself (`pkg/kubelet/src/storage.rs`). Every other StorageClass goes
through its CSI driver. The kubelet registers node plugins from
`/var/lib/kubelet/plugins_registry`, writes `CSINode`, and stages and
publishes volumes. It will not give a pod a volume whose mount has not reached
the node. See [docs/csi.md](docs/csi.md). The mounts of external drivers need
Bidirectional propagation in the engine (stormpump#35), and until that lands
pods on such claims wait with that reason.

## Relationship to rustkube

- **Control plane** (kube-apiserver, controller-manager, scheduler, fastetcd)
  lives in [rustkube](https://github.com/glennswest/rustkube).
- **DNS** is external (see [microdns](https://github.com/glennswest/microdns) —
  the K8s DNS source runs there).
- Shared types come from rustkube's `apimachinery` crate via a **sibling path
  dependency**:
  ```toml
  apimachinery = { path = "../rustkube/pkg/apimachinery" }
  ```
  So check out `rustkube` as a sibling directory:
  ```
  projects/
    rustkube/        # control plane (has pkg/apimachinery)
    rustkube-node/   # this repo
  ```

## Build

What ships is a **golden** — a sealed filesystem on the forge that a stormcos
release composes over. A node installs nothing, so there is no package to
build and no image file to copy:

```bash
scripts/build-golden.sh          # on the build box, as root
```

It builds the static binaries, attaches a volume from the forge over NVMe/TCP,
makes a filesystem on it, copies the binaries in with `install`, and seals it.
No tar, no loop device, no second copy of anything. See [docs/BUILD.md](docs/BUILD.md).

To compile without touching the forge:

```bash
# requires ../rustkube checked out as a sibling, and `protoc` on the build
# host (CRI gRPC codegen)
cargo build --release            # produces target/release/{kubelet,kube-proxy}
cargo build --release --target x86_64-unknown-linux-musl   # static
```

`packaging/build-packages.sh` still makes an rpm and a deb. They are kept for
hosts that are not stormcos nodes, and they are **not** what a node runs — note
that they package a glibc build, which a node cannot exec.

## The work (greenfield)

The node level is genuinely not finished. Priorities:

1. **kubelet ↔ CRI**: real containerd/CRI-O integration (or the native/VM
   runtimes), node registration + Lease heartbeats, pod sandbox lifecycle,
   volume mounts, probes end-to-end so a node goes `Ready` and runs a pod.
2. **kube-proxy**: iptables service/endpoint programming verified against a live
   apiserver; eBPF path behind a feature.
3. **CNI**: pod networking on a real node (bridge + IPAM + overlay), wired to the
   kubelet pod sandbox.
4. **Schedulable masters + workers**: once the above works, both a `worker1.g8.lo`
   node and schedulable masters can run app loads.

## License

Apache-2.0
