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
| — | `pkg/cni` | CNI plugins (bridge, host-local IPAM, VXLAN) |

Binaries and systemd units use **exact upstream names** (`kubelet`,
`kube-proxy`, `kubelet.service`, `kube-proxy.service`), config under
`/etc/kubernetes/` — so this is a drop-in node.

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

```bash
# requires ../rustkube checked out as a sibling
cargo build --release            # produces target/release/{kubelet,kube-proxy}
cargo build --release --target x86_64-unknown-linux-musl   # static
```

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
