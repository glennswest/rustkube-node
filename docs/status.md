# Implementation and documentation audit

Reviewed 2026-09-29 against main **5bb1a38** and
`git log --since=2026-09-18`. Version 0.13.0 is the workspace version;
changes after its tag remain unreleased. This document describes source and
recorded verification, not a newly tested live node. The #114 merge integrates turbomode UID workers with the main behaviors
listed below. VM partial-start unwind, CNI DEL after a failed ADD and
reclaim-handler cancellation are done (#100, unit-tested only). Complete event
sources and live acceptance remain open (#99, #101, #102); no release or golden
is part of this merge.

## Changes since September 18

| Area | Current code and representative commits |
|---|---|
| Standalone builds | Pinned git apimachinery dependency replaces sibling path; Cargo.lock updated (`d3005a9`, `6741e93`) |
| Pod status | Actual container timestamps, retained init outcome/status and computed Initialized (`8db31ff`, `4fefda1`, `ece4211`); successful init logs are still removed |
| Built-in PVCs | Size-class blank clones through ublk, data-source clones, binding/reclaim, no scratch fallback (`596fb8e`, `32641e4`, `3f1f7d2`, `5236dbe`); fractional quantities and rounded capacity (`c5187a6`, `30ce985`) |
| Waiting volumes | Recorded Pending/ContainerCreating state and FailedMount Events; background blank minting (`c78baf0`) |
| Engine authentication | Shared EngineClient with the engine's own token (`52726fb`, `ca6dd06`) |
| Service volumes | Complete PV/PVC object builder and reconciliation, including logs volumes (`30bb887`) |
| External CSI | Real Unix gRPC calls, registration, CSINode/topology, stage/publish and cleanup (`086d819`, `dafb56f`, `f0310e5`); propagation integration remains incomplete |
| Metrics/resources | Prometheus metric families and stormpump QUERY CPU/memory; memory/CPU limits mapped to engine Spec (`f239350`, `4245b8f`) |
| VM lifecycle | Registered VM adoption/deletion/finalizers (`50c9865`); startup retry and failed-start cleanup (`b362777`); durable disks and owner sweep (`5c70a96`) |
| VM integration | Bridged tap address reporting (`3fe045b`), accessCredentials (`16fd7c6`), snapshot reconciler (`347655a`) |
| Consoles/logs | Mounted stormvm console router; engine configured for snapshots (`66a04f6`, `52d7d43`); stormd mirror logs and stale mirror status (`ec32b92`, `7f4f3d1`) |
| Tests | PVC medium-suite container and remote static-binary staging (`f63d8c2`, `38dba5a`); live acceptance remains open |

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
| Built-in classes beyond 1TiB, raw block and per-class filesystem selection | [#67](https://github.com/glennswest/rustkube-node/issues/67) |
| Slab capacity reservation/overcommit refusal; policy undecided | [#62](https://github.com/glennswest/rustkube-node/issues/62), [#108](https://github.com/glennswest/rustkube-node/issues/108) |
| StorageClass placement policy and cross-node replication | [#71](https://github.com/glennswest/rustkube-node/issues/71), [#68](https://github.com/glennswest/rustkube-node/issues/68) |
| Every node's service volume represented despite shared names; placement metadata join | [#59](https://github.com/glennswest/rustkube-node/issues/59), [#107](https://github.com/glennswest/rustkube-node/issues/107), [#60](https://github.com/glennswest/rustkube-node/issues/60) |
| Mutual Pod/VM claim exclusion on main | [#80](https://github.com/glennswest/rustkube-node/issues/80) |
| Arbitrary pulled images, image metadata defaults and versioned golden selection | [#79](https://github.com/glennswest/rustkube-node/issues/79), [#103](https://github.com/glennswest/rustkube-node/issues/103), [#98](https://github.com/glennswest/rustkube-node/issues/98), [#86](https://github.com/glennswest/rustkube-node/issues/86) |
| Private writable container roots and their filesystem accounting | [#104](https://github.com/glennswest/rustkube-node/issues/104) |
| Proportional CPU-request weights under stormpump's flat hierarchy | [#57](https://github.com/glennswest/rustkube-node/issues/57), [#106](https://github.com/glennswest/rustkube-node/issues/106) |
| Real service readiness, complete lifecycle failure Events and non-stormd logs | [#96](https://github.com/glennswest/rustkube-node/issues/96), [#50](https://github.com/glennswest/rustkube-node/issues/50), [#82](https://github.com/glennswest/rustkube-node/issues/82), [#87](https://github.com/glennswest/rustkube-node/issues/87) |
| Node Ready gated by CNI, and no-cni help promising host networking | [#3](https://github.com/glennswest/rustkube-node/issues/3), [#32](https://github.com/glennswest/rustkube-node/issues/32) |
| VMI pod-network sandbox, spec.nodeName-only assignment, migration and control-verb routes | [#88](https://github.com/glennswest/rustkube-node/issues/88), [#85](https://github.com/glennswest/rustkube-node/issues/85), [#40](https://github.com/glennswest/rustkube-node/issues/40), [#94](https://github.com/glennswest/rustkube-node/issues/94) |
| VirtualMachineRestore | [#53](https://github.com/glennswest/rustkube-node/issues/53), owner decision [#109](https://github.com/glennswest/rustkube-node/issues/109) |
| End-to-end legacy microVM Pods | [#13](https://github.com/glennswest/rustkube-node/issues/13) |
| Successful init-container log retention; restartable init sidecars | [#47](https://github.com/glennswest/rustkube-node/issues/47), [#111](https://github.com/glennswest/rustkube-node/issues/111) |
| Restart backoff persistence across kubelet restart | [#112](https://github.com/glennswest/rustkube-node/issues/112) |
| Missing configured credentials fail closed; client certificate reload | [#69](https://github.com/glennswest/rustkube-node/issues/69), [#89](https://github.com/glennswest/rustkube-node/issues/89), [#77](https://github.com/glennswest/rustkube-node/issues/77) |
| Explicit default-valued apiserver flag overriding kubeconfig | [#113](https://github.com/glennswest/rustkube-node/issues/113) |
| Destructive engine calls with a separate admin token | [#105](https://github.com/glennswest/rustkube-node/issues/105) |
| Tunable max-pods/reservations/cgroup-driver | [#24](https://github.com/glennswest/rustkube-node/issues/24) |
| Subsecond startup, fully bounded I/O/cancellation and event-driven workers on main | [#95](https://github.com/glennswest/rustkube-node/issues/95), [#99](https://github.com/glennswest/rustkube-node/issues/99), [#101](https://github.com/glennswest/rustkube-node/issues/101) |
| Complete short/medium/long live acceptance and runner image injection | [#61](https://github.com/glennswest/rustkube-node/issues/61), [#64](https://github.com/glennswest/rustkube-node/issues/64), [#97](https://github.com/glennswest/rustkube-node/issues/97), [#102](https://github.com/glennswest/rustkube-node/issues/102) |
| Legacy bin-only builder as a supported release path | [#51](https://github.com/glennswest/rustkube-node/issues/51) |

The former CRI-O/crun/conmon-rs default-stack roadmap (#22/#28) is superseded
by the platform's stormpump selection. An eBPF kube-proxy in this repository
is not a shipping commitment: Cilium owns the service dataplane (#2).
