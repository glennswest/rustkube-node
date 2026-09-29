# rustkube-node

The **node level** of [rustkube](https://github.com/glennswest/rustkube) — the
Kubernetes worker components, in Rust. Split into its own repo for parallel
development; the code stays upstream-shaped and monorepo-mergeable.

> **Status: early / greenfield.** The libraries exist (ported from rustkube),
> the binaries build, but a node does not yet fully join a cluster or run pods.
> See the tracking issues.

## Event-driven branch

`turbomode` pairs with rustkube's branch of the same name. Pod and VMI
assignment/volume watches enqueue coalesced reconciliation work; Pod and VM
subscriptions run independently; runtime work now uses one eight-worker Pod/VMI executor with name/claim
reservations and recovery barriers (#100, validation in progress). Stormpump exits and Linux static-manifest changes
also wake workers. CSI registrar sockets use filesystem notifications;
successful registrations wake Pod workers, with deadlines for pending failures. Failed or incomplete Pod/manifest reads cannot stop live
Pods by treating an unknown desired set as empty. Status publication retains
startTime, skips unchanged status and uses the observed resourceVersion.
Pod teardown retains its runtime record until stopping and volume cleanup succeed.
CSI teardown preserves its retry record through failed unstage calls and refuses
cleanup from unreadable records or incomplete Pod lists. Pod and VMI adapters share that executor. Failed startup/cleanup records are
retained, and same-name successors wait for the previous UID to release its resources.

The full workspace build and tests passed on dev at `55d458c`, including
224 kubelet unit and four integration tests. Cleanup retains refused engine releases, CSI publication and teardown
serialize by driver/handle, and failed runtime recovery keeps admission closed.
PV and VolumeAttachment changes route through an inverse claim index; failed
collection reads retain the index. VM status and migration writes carry UID
guards. VM cancellation remains blocked on
[stormpump#63](https://github.com/glennswest/stormpump/issues/63): the shared
engine client cannot withdraw a deposited tap after a pre-spawn failure.
Every-boundary cancellation coverage and VM partial-start cleanup are unfinished;
this branch is not ready to merge or release. This is not a measured subsecond release.
Active workloads still use an explicit runtime/probe/volume observation
fallback, and service/volume mirrors and CSI cleanup tasks retain their existing schedules.
Per-UID concurrency and complete local event sources are tracked in
[#100](https://github.com/glennswest/rustkube-node/issues/100) and
[#101](https://github.com/glennswest/rustkube-node/issues/101).
See [the design and baseline](docs/event-driven-design.md) and
[#102](https://github.com/glennswest/rustkube-node/issues/102) for validation.
Builds run on dev only, after 10:00 America/Chicago on 2026-09-29.

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
| `GET /metrics`, `/metrics/cadvisor` | Prometheus metrics under upstream's names: the kubelet's own, and cAdvisor-shaped container and pod usage. See [docs/metrics.md](docs/metrics.md) |
| `GET /stats/summary` | The Summary API (`kubectl top`, metrics-server) |
| `GET /pods` | The pods this kubelet manages, including admitted pods still waiting to start (`Pending`, with the reason) |
| `GET /containerLogs/{ns}/{pod}/{container}` | What `kubectl logs` reads, by way of the apiserver proxy. A pod waiting to start answers `400 … is waiting to start: ContainerCreating (<reason>)`, as upstream does. A node service's mirror pod (`kube-system/<asset>-<node>`, container `<asset>`) reads the service's stormd log volume, found through the boot unit that mounts it at `/var/log/stormd` and seen under `/hostroot`. The current log covers every process stormd runs there, rotations included, merged in time order and marked `[<proc>]` when there is more than one. `--previous` is the newest `.failed.log`, and tail, since, timestamps, limit and follow all apply. A service not run by stormd (stormblock, registry) has no such volume and answers 404 |
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
node itself (`pkg/kubelet/src/storage.rs`), through the node's stormblock
engine (`--stormblock`, default `http://127.0.0.1:9090`). The engine requires
its own token, and the kubelet presents it on every call
(`pkg/kubelet/src/engine.rs`). It reads the token from `$STORMBLOCK_API_TOKEN`,
or else from the file at `$STORMBLOCK_TOKEN_FILE` (default
`/run/stormblock/engine/api_token`, set by stormcos), then
`/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`. The engine
mints the token when it starts, and the kubelet may start first, so while no
token is found the kubelet looks again on every call. After a 401 it reads the
token again and retries once. Every other StorageClass goes
through its CSI driver. The kubelet registers node plugins from
`/var/lib/kubelet/plugins_registry`, writes `CSINode`, and stages and
publishes volumes. It will not give a pod a volume whose mount has not reached
the node. See [docs/csi.md](docs/csi.md). The mounts of external drivers need
Bidirectional propagation in the engine (stormpump#35), and until that lands
pods on such claims wait with that reason.

**A pod whose volumes are not ready waits, and says why.** It is `Pending`,
every container is `waiting: ContainerCreating` with the reason (for example
`waiting for volume pvc-default-data: template pvc-ext4j-1048576m
awaiting_format`), and `describe` shows a `FailedMount` Event. After 5 minutes
the reason says it timed out, and the kubelet keeps retrying. As upstream, a
mount timeout does not fail the pod, because the volume may still come.

The first claim of a size class mints its blank (one `mkfs`). The mint runs in
the background without an inline wait. Completion signals the Pod and VM
queues immediately; waiting claims check the template's state and proceed
when it is `ready`. A large format does not hold the reconciliation pass.

### Virtual machine disks

With `--runtime=stormpump` the kubelet also runs the VirtualMachineInstances
assigned to its node (`pkg/kubelet/src/vm_manager.rs`). Each volume of a VMI
becomes a stormblock volume attached here:

| VMI volume | Disk | Deleted with the VM |
|---|---|---|
| `dataVolume` / `containerDisk` | a clone of the named golden | yes |
| `cloudInitNoCloud` | a generated `cidata` seed | yes |
| `emptyDisk: {capacity}` | a blank volume `<ns>.<vm>-<disk>`, reused if it already exists | yes |
| `persistentVolumeClaim: {claimName}` | the claim's volume, resolved exactly as for a pod (a bound claim uses its volume, an unbound `stormblock` claim is provisioned) | no, it belongs to the claim |

Today "deleted with the VM" also happens when the VM stops, so golden clones
and empty disks come back fresh after a stop (#75). A claim's disk is never
deleted.

A claim's disk waits, with the reason on the VMI, while the claim is unbound,
belongs to another StorageClass, or is in use by a pod on this node. A pod
mounts the filesystem and a VM writes the raw device, so the two must not
share it.

### Virtual machine lifecycle

- **A VMI being deleted stops its machine.** While a machine runs, its VMI
  carries the finalizer `storm.io/vm`, so the deletion completes only once the
  machine is gone. The stop is ACPI with a 30 s grace, then a kill, then the
  disks are detached.
- **A VM outlives a kubelet restart** (the engine supervises it), so the kubelet
  records each one where a restarted kubelet finds it: the machine's
  registration, `/run/stormvm/<ns>/<name>/vm.json`, with the engine's workload
  handle and its disks. On every sync, a registered machine the kubelet does
  not know is adopted when its VMI still wants it, and stopped when not.
- A machine started by an older kubelet has no handle recorded. If its VMI is
  gone, it is stopped through its own control socket: ACPI, then `quit`.
- A failed VMI list is skipped, not read as "no machines". Reading it that way
  stopped every VM on the node.
- Restarting a `running: true` VM whose instance ended is the VM controller's
  job (rustkube#104).

## Tests on a node

`test/` is the test container, per stormcentral's `docs/test-standard.md`. It
is a standalone crate (its own workspace and `Cargo.lock`). `test/build.sh`
builds its static binary on the build box, and `test/Containerfile` copies it
into a scratch image. The image is started as `/test <suite>` and run by
stormcentral as a Job (`test/rustkube-node-test.yaml`):

    stormcentral test run rustkube-node medium --url http://stormcentral.g8.lo

- **medium** (< 30 min): claims of the built-in `stormblock` class (#64).
  - It makes a claim at every size class (1Mi … 1Ti) and at arbitrary sizes:
    1 byte, 1Mi+1, 17Mi, 1500M, 3.5Gi and 600Gi. The claims run in parallel.
  - For each one, a pod writes and reads back 64 KiB and checks that `df` is
    within the class the request rounds to. The test then checks that the claim
    is Bound with that class as its status capacity, and deletes the pod and
    the claim. The PV must be reclaimed.
  - A 2Ti claim must be refused with the reason, and never bound.
  - Every case has `RUSTKUBE_NODE_TEST_MINT_BUDGET` (default 1200 s), because a
    class may be minted on first use.
  - Overcommit is reported skip until #62.
- **short**, **long**: not written yet (#61). Each reports one skip.

The same image is the workload pods' program (`/test sized <path> <seed>
<bytes> <lo> <hi>`), so a run pulls nothing else. Check it builds with
`sc-build 'cd test && cargo test --locked && cargo build --release --locked'`.

## Relationship to rustkube

- **Control plane** (kube-apiserver, controller-manager, scheduler, fastetcd)
  lives in [rustkube](https://github.com/glennswest/rustkube).
- **DNS** is external (see [microdns](https://github.com/glennswest/microdns) —
  the K8s DNS source runs there).
- Shared types come from rustkube's `apimachinery` crate, as a **git
  dependency pinned to a commit** (`Cargo.toml`, `[workspace.dependencies]`):
  ```toml
  apimachinery = { git = "https://github.com/glennswest/rustkube", rev = "<commit>" }
  ```
  This checkout builds on its own. Moving to a newer rustkube means changing
  `rev` and running `cargo update -p apimachinery`.

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
# requires `protoc` on the build host (CRI/CSI gRPC codegen)
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
