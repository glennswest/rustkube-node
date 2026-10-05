# Implementation and documentation audit

Reviewed 2026-10-02 against main **fecb331** and
`git log --since=2026-09-25` (the previous audit, 2026-09-29, covered from
September 18 at 5bb1a38). Version 0.13.0 is the workspace version; changes
after its tag (e8211b6) are unreleased as a version, though stage goldens of
main have been requested since (latest golden-rustkube-node-e5db6ac32831 at
9c2f738, release request stormcos#164). This document describes source and
recorded `sc-build` verification, not a newly tested live node: no change
below has been measured on a node yet (#102, target C2NR0Q2 per #110).

## Changes since September 25

| Area | Current code and representative commits |
|---|---|
| Standalone builds | Pinned git apimachinery dependency replaces sibling path; Cargo.lock updated (`d3005a9`, `6741e93`) |
| Engine authentication | Shared EngineClient with the engine's own token (`52726fb`, `ca6dd06`); v0.11.0 |
| VM disks from stormvm v0.10.0 | emptyDisk and PVC disks (`6aca609`); v0.12.0 |
| Metrics/resources | Prometheus metric families under upstream names; stormpump QUERY CPU/memory; memory/CPU limits mapped to engine Spec (`f239350`, `4245b8f`) |
| Service volumes | Complete PV/PVC object builder and reconciliation, including logs volumes (`30bb887`) |
| VM lifecycle | Registered VM adoption/deletion/finalizers, failed VMI list is not empty (`334f912`, `50c9865`); v0.13.0. Startup retry and failed-start cleanup (`b362777`); durable disks and owner sweep (`5c70a96`); a golden still importing (409 not sealed) waits (`c74b589`, #117) |
| Built-in PVCs | Fractional quantities and class-sized capacity (`c5187a6`, `30ce985`); waiting Pods recorded Pending/ContainerCreating with FailedMount Events, blanks minted off the reconcile path (`c78baf0`) |
| VM integration | Bridged tap address reporting (`3fe045b`), accessCredentials (`16fd7c6`), snapshot reconciler (`347655a`), console router told the stormblock URL (`52d7d43`) |
| Node-service mirrors | stormd mirror logs (`ec32b92`) and "not started on this boot" status (`7f4f3d1`) |
| UID workers (turbomode, #114) | Shared Pod/VMI workers (eight then; `--pod-workers`, 16 per CPU within 32–256, since #138) with name/claim reservations, shared four-slot image pulls, staged init and VM shutdown waits, retained failed teardown, CSI mutation serialization, inverse claim index, UID-guarded writes (`cdb0a4b`…`55d458c`, merged `600b58a`) |
| Partial-start unwind (#100) | VM taps withdrawn with stormpump `DEPOSIT_WITHDRAW` and handles released after a failed start (`6c49fbc`); CNI DEL after a failed ADD; reclaim outlives its HTTP client (`077ad5c`) |
| No sync tick (#101) | Per-probe and backoff deadlines (`4320c20`); service mirror on `/run/stormpump` inotify (`08fc437`); claims mirror, reclaim, CSI sweep and VM maintenance on watches (`82f00c3`); `kubelet_timed_reconciles_total` |
| Bounded calls (#99) | Ring requests 30 s from enqueue, CNI plugin exec 60 s, engine 5 s connect / 60 s request / 1 h mint (`db9b783`) |
| Full node filesystem (#129) | Per-pod dirs, ServiceAccount token, resolv.conf and container log dirs written before the sandbox; a failed write (ENOSPC…) keeps the pod Pending with the errno instead of "does not exist" and `Failed` |
| Pod start timing (#132) | Phases of each start (seen → images → volumes → sandbox → init → containers → Running acknowledged) as `storm.io/start-timing`, a `StartTiming` Event, `kubelet_pod_start_phase_duration_seconds{phase}` and one INFO line (`8009bd4`); not yet read on a node |
| First-attempt starts (#134) | A start waits up to 100 ms for an image it just asked for, so a local image starts on attempt 1 with no `ContainerCreating` status write first (every start took 2 on pvetest1); lifecycle Normal Events are queued, written in order off the start path (`5f6af4f`); not yet measured on a node |
| Finished Pods release their IP (#137) | A Succeeded/Failed Pod's sandbox is stopped when it finishes: CNI DEL (stormpump runtime, at stop rather than removal) and the netns holder released; records and logs kept until deletion; a failed DEL retried (`227fbfe`). 1,000 Never pods on pvetest1 filled Cilium's range at ~250 before; not yet measured on a node |
| Burst starts (#138) | 8 fixed workers queued 173 pods on pvetest1 (wait p50 2.4 s, max 9.3 s): `--pod-workers` (16 per CPU, 32–256), the start-timing PATCH off the worker, `workers=`/`pending=` in the annotation, no PodState clone per pass (`20e95fc`); not yet measured on a node |
| Clone refusals (#140) | Dell C2NR0Q2 on 11.76: every claim waited on "stormblock would not clone pvc-ext4j-64m". Engine POSTs (template clone, attach, snapshot) now carry stormblock's status and `error`, or the transport error, into the claim's wait; a template refused as broken (sealed volume 404 / not sealed / no sealed snapshot) is deleted and minted again; not yet run on a node |
| Pods wait for the CNI config (#148) | Dell 11.79: coredns took 16 sandbox attempts over 27 s before the network. No conflist is now a wait without a sandbox, woken by an inotify watch on `--cni-conf-dir` (10 s fallback); a failing ADD backs off from its own first failure (`f1a0c38`); not yet measured on a node |
| kube-proxy on a TLS apiserver (#145) | `--ca-file`/`--token-file` defaulting to the ServiceAccount, token re-read per request; refused lists are errors, not empty; jumps into `KUBE-SERVICES`/`KUBE-POSTROUTING` ensured; rules rewritten on any change (not counts), resynced every 60 s; ports keyed by protocol and matched by name; optional `--cluster-cidr` (`3243b77`). For stormcos#265's flowsdn edition; not yet run on a node |
| Tests | PVC medium-suite container and remote static-binary staging (`f63d8c2`, `38dba5a`); latest full sc-build at `c74b589`: 270 kubelet unit, 4 integration, 25 CNI, 17 proxy tests; live acceptance remains open |

## Owner decisions recorded, implementation pending

These were open questions in the previous audit. The owner has answered each;
what is left is implementation, tracked on the issue named.

| Decision | Answer | Implementation |
|---|---|---|
| [#106](https://github.com/glennswest/rustkube-node/issues/106) CPU request weights | OpenShift's shape: Pods under one parent cgroup, upstream weights within it | engine side [stormpump#68](https://github.com/glennswest/stormpump/issues/68), then [#57](https://github.com/glennswest/rustkube-node/issues/57) |
| [#107](https://github.com/glennswest/rustkube-node/issues/107) service PV/PVC names | `<volume>-<node>` (e.g. `fastetcd-data-<node>`), no migration | [#59](https://github.com/glennswest/rustkube-node/issues/59) |
| [#108](https://github.com/glennswest/rustkube-node/issues/108) capacity accounting | Class size counted at bind, overcommit ratio 1.0, class-sized clones | [#62](https://github.com/glennswest/rustkube-node/issues/62) |
| [#109](https://github.com/glennswest/rustkube-node/issues/109) restored VM disks | Option A: a restore rewrites the VirtualMachine's disks to the restored PVCs | [#53](https://github.com/glennswest/rustkube-node/issues/53) |
| [#110](https://github.com/glennswest/rustkube-node/issues/110) live validation target | C2NR0Q2 (the Dell R230) first | [#102](https://github.com/glennswest/rustkube-node/issues/102) |

Source entry points: `cmd/kubelet/src/main.rs`, `pkg/kubelet/src/kubelet.rs`,
`pod_manager.rs`, `stormpump_runtime.rs`, `vm_manager.rs`, `vm_snapshot.rs`,
`server.rs`, `system_claims.rs`, and `test/src/medium.rs`.

## Promises the current code does not fulfill

Each gap has an owning issue. These are limitations, not supported features.

| Unsupported or incomplete claim | Tracking |
|---|---|
| Full upstream/drop-in Kubernetes node compatibility; API types still use v1_32 | [#23](https://github.com/glennswest/rustkube-node/issues/23) |
| Interactive exec, attach, port-forward and the streaming path used by kubectl cp | [#56](https://github.com/glennswest/rustkube-node/issues/56) |
| Full upstream cAdvisor dashboards, actual node CPU/memory, PSI/imagefs-aware eviction and CRI pod-network stats | [#21](https://github.com/glennswest/rustkube-node/issues/21) |
| External CSI propagation and real-driver acceptance | [#81](https://github.com/glennswest/rustkube-node/issues/81), [#52](https://github.com/glennswest/rustkube-node/issues/52), [stormvm#65](https://github.com/glennswest/stormvm/issues/65) |
| NodeExpandVolume and completion of filesystem-resize status | [#42](https://github.com/glennswest/rustkube-node/issues/42) |
| Generic ephemeral claim creation | [rustkube#94](https://github.com/glennswest/rustkube/issues/94) |
| ext4 blanks for the 64Ti, 256Ti and 1Pi classes (raw block only until stormblock carries mkfs.ext4.rs#9/#10); a Block claim's PV written by the control plane says Filesystem | [stormblock#289](https://github.com/glennswest/stormblock/issues/289), [rustkube#201](https://github.com/glennswest/rustkube/issues/201) |
| StorageClass placement policy and cross-node replication | [#71](https://github.com/glennswest/rustkube-node/issues/71), [#68](https://github.com/glennswest/rustkube-node/issues/68) |
| Every node's service volume represented (`<volume>-<node>` names decided in #107, not implemented); placement metadata join | [#59](https://github.com/glennswest/rustkube-node/issues/59), [#60](https://github.com/glennswest/rustkube-node/issues/60) |
| Mutual Pod/VM claim exclusion on main | [#80](https://github.com/glennswest/rustkube-node/issues/80) |
| Arbitrary pulled images, image metadata defaults and versioned golden selection (a pulled image's mount is the container root since #103) | [#79](https://github.com/glennswest/rustkube-node/issues/79), [#98](https://github.com/glennswest/rustkube-node/issues/98), [#86](https://github.com/glennswest/rustkube-node/issues/86) |
| Private writable container roots and their filesystem accounting | [#104](https://github.com/glennswest/rustkube-node/issues/104) |
| CPU-request weights (`cpu_shares` → `cpu_weight`) under a Pod parent cgroup, as decided in #106 | [#57](https://github.com/glennswest/rustkube-node/issues/57), [stormpump#68](https://github.com/glennswest/stormpump/issues/68) |
| Real service readiness, complete lifecycle failure Events and live non-stormd logs (a dead service's last 20 lines are served since #124) | [#96](https://github.com/glennswest/rustkube-node/issues/96), [#50](https://github.com/glennswest/rustkube-node/issues/50), [#82](https://github.com/glennswest/rustkube-node/issues/82), [#87](https://github.com/glennswest/rustkube-node/issues/87) |
| Node Ready gated by CNI, and no-cni help promising host networking | [#3](https://github.com/glennswest/rustkube-node/issues/3), [#32](https://github.com/glennswest/rustkube-node/issues/32) |
| VMI pod-network launcher Pod from rustkube's VM controller (rustkube#203; the kubelet adopts it, not run on a node), spec.nodeName-only assignment, migration and control-verb routes | [#88](https://github.com/glennswest/rustkube-node/issues/88), [#85](https://github.com/glennswest/rustkube-node/issues/85), [#40](https://github.com/glennswest/rustkube-node/issues/40), [#94](https://github.com/glennswest/rustkube-node/issues/94) |
| VirtualMachineRestore (design decided in #109: rewrite the VM's disks to restored PVCs) | [#53](https://github.com/glennswest/rustkube-node/issues/53) |
| Pod `securityContext.capabilities` on the stormpump ring path (add only parsed, drop ignored) | [#118](https://github.com/glennswest/rustkube-node/issues/118) |
| Guest metadata (`/vmInstance`) as the metadata service's single source (stormimds keeps its own store; undecided), and for host-network callers. (Indexed by address and gated on the object's placement since #119; a node partitioned from the apiserver still answers from its last cache.) | [stormimds#12](https://github.com/glennswest/stormimds/issues/12), [#122](https://github.com/glennswest/rustkube-node/issues/122) |
| End-to-end legacy microVM Pods | [#13](https://github.com/glennswest/rustkube-node/issues/13) |
| Successful init-container log retention; restartable init sidecars | [#47](https://github.com/glennswest/rustkube-node/issues/47), [#111](https://github.com/glennswest/rustkube-node/issues/111) |
| Restart backoff persistence across kubelet restart | [#112](https://github.com/glennswest/rustkube-node/issues/112) |
| Missing configured credentials fail closed; client certificate reload | [#69](https://github.com/glennswest/rustkube-node/issues/69), [#89](https://github.com/glennswest/rustkube-node/issues/89), [#77](https://github.com/glennswest/rustkube-node/issues/77) |
| Explicit default-valued apiserver flag overriding kubeconfig | [#113](https://github.com/glennswest/rustkube-node/issues/113) |
| Destructive engine calls with a separate admin token | [#105](https://github.com/glennswest/rustkube-node/issues/105) |
| Tunable max-pods/reservations/cgroup-driver | [#24](https://github.com/glennswest/rustkube-node/issues/24) |
| Subsecond startup measured on a node; a stormpump exit routed to its own UID; CRI container events instead of the `sync_interval` fallback | [#95](https://github.com/glennswest/rustkube-node/issues/95), [#99](https://github.com/glennswest/rustkube-node/issues/99), [#115](https://github.com/glennswest/rustkube-node/issues/115), [#116](https://github.com/glennswest/rustkube-node/issues/116) |
| Complete short/medium/long live acceptance and runner image injection | [#61](https://github.com/glennswest/rustkube-node/issues/61), [#64](https://github.com/glennswest/rustkube-node/issues/64), [#97](https://github.com/glennswest/rustkube-node/issues/97), [#102](https://github.com/glennswest/rustkube-node/issues/102) |
| Legacy bin-only builder as a supported release path | [#51](https://github.com/glennswest/rustkube-node/issues/51) |

The former CRI-O/crun/conmon-rs default-stack roadmap (#22/#28) is superseded
by the platform's stormpump selection. An eBPF kube-proxy in this repository
is not a shipping commitment: Cilium owns the service dataplane (#2).
