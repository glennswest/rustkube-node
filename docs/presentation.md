---
marp: true
title: rustkube-node
description: The node half of rustkube, the Kubernetes worker for stormcos
paginate: true
---

<!--
Render: npx @marp-team/marp-cli docs/presentation.md          (HTML)
        npx @marp-team/marp-cli docs/presentation.md --pdf    (PDF)
Written 2026-10-05, refreshed 2026-10-09 from main 06b91b5 (workspace 0.13.0;
latest stage golden golden-rustkube-node-2eabd6fc174b) and the current docs: README.md,
docs/api.md, docs/configuration.md, docs/status.md. Every claim names where it
can be checked. Planned work is on its own slide and says so (#55).
-->

# rustkube-node

**The node half of rustkube: the Kubernetes worker for stormcos, in Rust**

kubelet · kube-proxy · CNI helpers

`github.com/glennswest/rustkube-node` · workspace 0.13.0 · shipped as a stormcos stage golden

---

## What it is, and the problem it solves

A stormcos node has no container runtime daemon, no systemd and no libvirt. PID 1 is **stormpump** (an
engine reached over a shared-memory ring), storage is **stormblock** (copy-on-write volumes and goldens),
and VMs are **stormvm** plans.

rustkube-node is the piece that makes such a node **a Kubernetes node**:

- registers the Node and keeps its heartbeat and conditions
- runs the Pods and KubeVirt `VirtualMachineInstance`s the scheduler puts on it
- turns `PersistentVolumeClaim`s into stormblock clones, and the node's own volumes into PV/PVC objects
- shows the node's own services (PID 1's assets) as read-only Pods with their logs

It speaks the upstream API shapes (`kubectl logs`, `describe`, metrics under upstream names). It is a
**partial** node implementation; the gaps are tracked in `docs/status.md`.

---

## Where it sits in stormcos

```
        stormcos (the product)        flowsdn (its edition's CNI and Services)
                    \                    /
                     v                  v
 rustkube  ----->  [ rustkube-node: kubelet ]
 apiserver,          |      |       |       |        |
 scheduler,          v      v       v       v        v
 controllers    stormpump stormvm stormblock sbregistry CNI
                (PID 1,  (plans,  (volumes, (image    (Cilium
                 ring)   console)  goldens)  clones)  or flowsdn)
```

stormcentral's relationships: **rustkube-node → rustkube, stormpump, stormvm**; depended on by
**stormcos** and **flowsdn**. Shared types come from rustkube's `apimachinery` crate, pinned by `rev`
(`Cargo.toml`). Ports it talks to: `docs/api.md`.

---

## How it works

```
 API watches (Pods, VMIs, PV/PVC, Secrets) + engine volume watch + CNI conf inotify
                                   |
                                   v
            workload executor: one worker per UID, name/claim reservations
                          (--pod-workers, 32-256)
                     |                                  |
                     v                                  v
      pod manager: sandbox, volumes,      VM manager: disks, NICs, pod-network
      containers, probes                  sandbox, spawn
                     |                                  |
                     +------> stormpump ring <----------+
                     +------> stormblock API <----------+
                     +------> CNI ADD / DEL  <----------+

 mirrors -> apiserver: node services as Pods, node volumes as PV+PVC, CSIStorageCapacity
```

- **Event-driven:** no sync tick. A worker runs on a change or on its own deadline (a probe period, a
  backoff); what still polls is counted in `kubelet_timed_reconciles_total` (#101).
- **Per-UID workers** with partial-start unwind: a failed start gives back every tap, handle and CNI ADD
  it took (#100). Calls into the ring, CNI and stormblock are all bounded (#99).

---

## Today: Pods

From the code (`pkg/kubelet/src/pod_manager.rs`, `stormpump_runtime.rs`):

- **Runtimes:** `--runtime stormpump` on stormcos (the ring at `/hostrun/stormpump.sock`); `cri` for
  CRI-O/containerd; `native` is the CLI default
- **Network:** a stormpump sandbox per Pod, CNI ADD into it; a Pod waits, with no sandbox, until the CNI
  config appears (inotify on `--cni-conf-dir`, #148); a finished Pod gives its IP back at once (#137)
- **Images and roots:** every container gets its own CoW clone of the image's sealed golden, mounted by
  PID 1 and deleted with it (#104); the image's config applies under the pod spec (#98); `port-forward`
  is served, exec/attach not yet (#56)
- **Lifecycle:** init containers and native sidecars (#111), restart policies, CrashLoopBackOff, liveness/readiness/startup probes,
  Events, ServiceAccount tokens **bound to the Pod** and refreshed at 80% of their life (#122)
- **Start timing:** every start's phases in `storm.io/start-timing`, a `StartTiming` Event and
  `kubelet_pod_start_phase_duration_seconds{phase}` (#132)
- **Full-disk safe:** per-pod dirs are written before the sandbox; ENOSPC is a wait with the errno, not a
  `Failed` pod (#129)

---

## Today: storage

From the code (`storage.rs`, `capacity.rs`, `system_claims.rs`, `csi*.rs`):

- **Built-in class `stormblock`:** a claim rounds up to a size class (1Mi … **1Pi**, ×4 steps) and is a
  CoW clone of that class's sealed ext4 blank, attached over ublk; blanks are minted on first use (#67)
- **`volumeMode: Block`:** a plain thin volume, bound at `volumeDevices[].devicePath` (#67)
- **Room is enforced:** a claim is charged its full class; the node publishes `CSIStorageCapacity`
  `kube-system/stormblock-<node>` and refuses a claim that does not fit; `SlabFilling` warnings (#62)
- **Node volumes as objects:** every `*-data`/`*-state`/`*-logs` volume is a bound PV+PVC,
  `kube-system/<volume>-<node>`, kept current by a reconciler (#59)
- **Third-party CSI drivers:** plugin registration, NodeStage/NodePublish/teardown and NodeExpandVolume
  over gRPC, mount propagation onto the engine (#81, #42; `docs/csi.md`)

---

## Today: virtual machines

KubeVirt `VirtualMachineInstance`s, run through stormvm and the ring (`vm_manager.rs`):

- **Disks:** golden clones, cloud-init seeds, emptyDisks and claims; disks outlive the VMI and belong to
  the VirtualMachine (#75); failed starts retried with backoff (#76)
- **NICs:** node bridges with the guest's address read off its tap (#91); **pod network** with its own
  sandbox, CNI ADD, the hypervisor in it, DHCP for the guest and the pod IP in status, under the
  VMI's launcher Pod from rustkube's VM controller, so Cilium policy and Services apply (#88, #152)
- **Adoption:** machines survive a kubelet restart and are adopted; deletion stops them (#35)
- **`accessCredentials`:** keys into the seed and through the guest agent (#92)
- **Snapshots and restores:** `VirtualMachineSnapshot` (one stormblock group snapshot) and
  `VirtualMachineRestore` (the disk set restored as claims, the VM rewired to them) (#53)
- **Consoles and metadata:** `/vmConsole/{ns}/{name}/{serial|vnc}`; `/vmInstance/{address}` answers only
  where the VMI places the machine (#119), and a host-network Pod by its token (#122)

---

## Today: the node's own services

PID 1 runs the node's services (fastetcd, stormblock, the registry …) from its asset table. The kubelet:

- mirrors each asset as a read-only Pod `kube-system/<asset>-<node>`: state, readiness, restarts and
  Events from the service's stormd API (#215), or Pending `NotStarted` when not started this boot
- answers `kubectl logs` on it from the service's stormd log volume, or PID 1's last 20 lines of a dead
  incarnation (#72, #124)
- mirrors their volumes as PV+PVC (previous slide)

So `kubectl get pods -n kube-system` shows the whole node, not only what Kubernetes started.

---

## Interfaces

| | |
|---|---|
| **Listener** | `:10250` HTTPS (`--kubelet-port`), bearer token (static or TokenReview); `/healthz` `/livez` `/readyz` open |
| **Routes** | `/pods`, `/containerLogs/…`, `/metrics`, `/metrics/cadvisor`, `/stats/summary`, `/portForward/…`, `/vmConsole/…`, `/vmVerb/…`, `/vmInstance/…`, `DELETE /volumes/{ns}/{claim}`; `/exec`, `/attach` answer 501 |
| **Talks to** | apiserver (`--apiserver`), stormpump ring (`--cri-socket`), stormblock `:9090` (`--stormblock`), sbregistry `:5100` (`--registry`), CNI (`--cni-conf-dir`, `--cni-bin-dir`) |
| **Tuning** | `--pod-workers`, `--max-pods` (110), `--system-reserved`/`--kube-reserved`, `--container-log-max-size`/`-files`, `--storage-overcommit` (1.0), `--storage-reserve-percent` (5), `--storage-alert-percent` (85) |
| **Metrics** | upstream names (`kubelet_running_pods`, `kubelet_pod_start_duration_seconds` …), cAdvisor series, slab gauges |

Full lists: `docs/api.md`, `docs/configuration.md`, `docs/metrics.md`.

---

## How it ships and runs

- **Built** with `sc-build` on the build box from a pushed commit (`cargo build --locked && cargo test
  --locked`), nothing kept
- **Shipped** as a **stage golden** (stormd, its config and the binaries):
  `stormcentral component stage rustkube-node`; the release train collects it into a stormcos release.
  A bin-only golden cannot start the service (`docs/BUILD.md`)
- **Started** by stormd on every node, `--runtime stormpump`; the kubelet attaches to PID 1's ring with a
  stable client token, so a restart gets its workloads back and adopts running Pods and VMs
- **Updated** by installing a new stormcos release: the image carries the golden; nothing is installed by
  hand on a node
- **Tested:** unit tests on every build (465 kubelet tests at 009d006); the `test/` container
  (`short`: Node Ready and a pod's life; `medium`: pod features, PVC ladder, raw block, overcommit, node
  volumes; `long`: night waves) runs as a Job through stormcentral

---

## Planned (not in the code yet)

- **exec / attach** and exec probes through stormpump's op (#56); capabilities and fsGroup onto the
  engine's `Spec` (#118, #171)
- **Live migration** of stormvm machines: a RAID leg on the destination first (owner, #159), behind
  stormstorage#44 (#40); graceful VM stop (#181)
- **Pod-network VMs on a live node:** the proof is stormcos_qa#18's namespace-isolation run (#88)
- **ext4 for the 64Ti–1Pi classes** once stormblock carries the fixed formatter (#149, stormblock#289);
  class blanks from a thin library model instead of a format on the node (#214)
- **Node stats from cAdvisor's library** (#21); **live measurement** of warm subsecond starts on the Dell (#102)

---

## Status and the open issues that matter

- **Verified by build and unit tests**, and for some work by a live run (the short suite passed on the
  Dell at 9229603, #217); each issue says what ran (`docs/status.md` is the audit)
- **P0:** #214 (class blanks formatted on the node at runtime)
- **P1:** #56 (exec/attach), #95/#99/#102 (startup speed), #184 (VMI waiting on its golden), #210 (pinned
  git dependencies), #61 (test suites live), #88/#91/#92 (VM checks on a live node)
- **Waiting on the owner:** #13/#221 (microVM Pods), #86 (versioned goldens)

`gh issue list --state open` for the rest.
