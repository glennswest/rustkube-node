# Implementation and documentation audit

Reviewed 2026-10-09 against main **06b91b5** and `git log --since=2026-10-02`
(the previous audit, 2026-10-02, covered from September 25 at fecb331).
Version 0.13.0 is the workspace version; changes after its tag (e8211b6) are
unreleased as a version, though stage goldens of main are requested as issues
finish (latest golden-rustkube-node-2eabd6fc174b at 9229603, release request
stormcos#424). This document describes source and recorded `sc-build`
verification (465 kubelet unit tests at 009d006), not a tested live node unless
a row says so.

## Changes since October 2

| Area | Current code and issues |
|---|---|
| Each container its own root (#104, owner's option A) | `create_container` clones the image's sealed golden (a pallet's `<volume>.golden`, a pull's sbregistry fstemplate) into engine volume `ctr-<container>`, attached over ublk and mounted by PID 1 at `/run/stormpump/roots/<container>`; deleted with the container, a restart gets a fresh one; orphans swept at start. A pull clones nothing any more (#143's image-level clone and #161 are superseded). `readOnlyRootFilesystem` still mounts it writable (stormpump#108) |
| Image config (#98, #172) | Entrypoint/Cmd/Env/WorkingDir/User from sbregistry's golden record under the pod spec; `runAsUser`/`runAsGroup`/`runAsNonRoot` applied; declared `Volumes` made as directories in the container's root. Pallets get no config yet (#208) |
| Unknown images (#79) | A golden the node's registry lacks is demanded through sbregistry's clone route (cluster fetch); 503/404 become ErrImagePull on the pull back-off |
| Starts and errors (#133, #126, #111, #47) | Image/create errors are waits with the container's reason (ErrImagePull → ImagePullBackOff, CreateContainerError); Never + start failure is `StartError`; no fixed init deadline (`activeDeadlineSeconds` bounds init only, #185); native sidecars (restartPolicy Always); a completed init's log kept |
| Restarts (#112, #174) | CrashLoopBackOff persisted to `<state root>/crashloop.json` across a kubelet restart; a stop's grace goes where STOP reads it (it waited 30 s every time) |
| Pod status writes (#141, #217) | Skipped when unchanged against the last acknowledged status; a 409 re-reads the pod (uid checked), merges and writes again (≤ 5 tries), `kubelet_pod_status_writes_total{result="conflict"}` |
| Retries (#211) | `pkg/retry`: every remote call retries infrastructure failures with bounded, jittered backoff; POST only when never sent or 429. Inventory in [retries.md](retries.md) |
| Node reporting (#24, #65, #78, #165, #23) | `--max-pods`, `--system-reserved`/`--kube-reserved` (allocatable = capacity − reserved − eviction line; not enforced as a cgroup, #205), `--cgroup-driver` (CRI); KVM advertised (`devices.kubevirt.io/kvm`, `storm.io/kvm`); `nodeInfo` kernel/boot/machine/system IDs; kubeletVersion v1.36, AppArmor profiles refused (#197), containerd < 2.0 refused |
| Credentials (#69, #77, #89, #105, #113) | A named credential file is waited for 60 s, then fatal; client and serving pairs reloaded when stormcert renews them; engine admin credential for destructive verbs; a given `--apiserver` always wins over the kubeconfig |
| Pod groups (#57, #106) | Pods under stormpump's `pods` group by QoS (`Spec.group`), CPU requests as `cpu.weight`, the group's weight from allocatable CPU; working set from QUERY MEMORY |
| Node services' mirror pods (#50, #82, #87, #96, #193, #215) | Container state, readiness, restarts and Events from each service's stormd API (`/api/v1/processes`, `/api/v1/events`), health probe fallback where stormd does not answer (#219); last exit and output; PID 1's run files for non-stormd services; startTime from `started_secs` |
| Logs (#124, #131, #136, #216) | One directory per container and a file per run (`<N>.log`, `previous=N`), current + 5 kept; rotation (`--container-log-max-size`, `--container-log-max-files`); plain lines no longer stripped |
| Storage (#59, #60, #62, #67, #70, #80, #209, #42) | Node volumes as `<volume>-<node>` PV/PVC pairs; PV placement annotations from stormblock + stormdrive; CSIStorageCapacity, refusal and `SlabFilling`; ladder to 1Pi (64Ti+ raw block only, #149); Provisioning Events on the claim; a Pod blocked on a VM's claim says why; minting templates not charged; NodeExpandVolume for CSI claims |
| Networking (#3, #131, #137, #147, #148) | Pod network counters and Multus-style `network-status`; network waits have Events; kube-proxy clears stale UDP conntrack (no stormcos edition runs kube-proxy); the CNI contract in [networking.md](networking.md) |
| Streaming (#56) | `portForward` over SPDY/3.1 and its WebSocket tunnel; exec/attach still answer 501 (#56 item 5) |
| VMs (#53, #75, #80, #85, #88, #92, #94, #119, #122, #152, #156) | Restore (option A of #109); `/vmVerb`; hand-placed VMIs (spec.nodeName); pod-network VMIs under rustkube's launcher Pod; `/vmInstance` indexed, placement-gated, staleness-bounded, and answering host-network Pods by their token |
| Workload identity (#84) | `/run/rustkube/workloads/<cgroup>.json` per stormpump workload (pods, sandboxes, VMs) for cadvisor |
| CRI runtime (#116) | `GetContainerEvents` followed; the 2 s fallback only without it |
| Tests (#61, #64, #97, #209, #217) | short, medium and long suites in `test/`; the runner's own pod is never cleaned up. Live: short 4fe3fb250e passed on C2NR0Q2 (4/0/0) at 9229603 |
| Docs | [retries.md](retries.md), [networking.md](networking.md), [presentation.md](presentation.md) |

## Owner decisions recorded

| Decision | Answer | State |
|---|---|---|
| [#106](https://github.com/glennswest/rustkube-node/issues/106) CPU weights vs node services | Pods under one parent cgroup (OpenShift's shape) | Done (#57, stormpump#68) |
| [#107](https://github.com/glennswest/rustkube-node/issues/107) service PV/PVC names | `<volume>-<node>`, no migration | Done (#59) |
| [#108](https://github.com/glennswest/rustkube-node/issues/108) capacity accounting | Class size at bind, ratio 1.0, class-sized clones | Done (#62) |
| [#109](https://github.com/glennswest/rustkube-node/issues/109) restored VM disks | A: restore rewrites the VM's disks to restored PVCs | Done (#53); needs the CRDs (stormcos#170) |
| [#110](https://github.com/glennswest/rustkube-node/issues/110), [#158](https://github.com/glennswest/rustkube-node/issues/158) live target | C2NR0Q2 first; the day suite there is the acceptance, full scale at night on a pve VM | [#102](https://github.com/glennswest/rustkube-node/issues/102) open |
| [#104](https://github.com/glennswest/rustkube-node/issues/104) container roots | A: every container its own CoW clone of the sealed golden, no exceptions | Done |
| [#3](https://github.com/glennswest/rustkube-node/issues/3) NotReady gating | No gating ("get working early"); per-pod waits instead | Done; #201 follows |
| [#68](https://github.com/glennswest/rustkube-node/issues/68) replicated claims | stormblock-csi owns them; the built-in class stays node-local | Closed |
| [#159](https://github.com/glennswest/rustkube-node/issues/159) migration disks | A RAID leg on the destination, then memory | [#40](https://github.com/glennswest/rustkube-node/issues/40) after stormstorage#44 |
| [#216](https://github.com/glennswest/rustkube-node/issues/216) `--previous` on a mirror pod | The newest finished run, failed or exited | Done |
| stormimds#12 guest metadata | stormimds asks `/vmInstance` on every request (single source) | Built in stormimds; node credential is stormcos#330 / #180 |

Decided 2026-10-09: microVM Pods are stormvisor's ([#13](https://github.com/glennswest/rustkube-node/issues/13), `--runtime vm` retired;
dispatch by RuntimeClass is [#204](https://github.com/glennswest/rustkube-node/issues/204)), and versioned goldens are copied on demand
([#86](https://github.com/glennswest/rustkube-node/issues/86), the fetch is stormblock-registry#116).

Source entry points: `cmd/kubelet/src/main.rs`, `pkg/kubelet/src/kubelet.rs`,
`pod_manager.rs`, `stormpump_runtime.rs`, `container_roots.rs`, `vm_manager.rs`,
`mirror.rs`, `stormd_api.rs`, `server.rs`, `system_claims.rs`, `capacity.rs`,
`pkg/retry`, and `test/src/{short,medium,long}.rs`.

## Promises the current code does not fulfill

Each gap has an owning issue. These are limitations, not supported features.

| Unsupported or incomplete claim | Tracking |
|---|---|
| exec, attach and exec probes (port-forward is served). stormpump's op is in the lock (13cf2c9, stormpump#103 done); the kubelet does not use it yet | [#56](https://github.com/glennswest/rustkube-node/issues/56) |
| Pod `securityContext.capabilities` on stormpump (`Spec.caps` is in the lock; not mapped) and `fsGroup`/`supplementalGroups` (`Spec.groups`; a claim is root's) | [#118](https://github.com/glennswest/rustkube-node/issues/118), [#171](https://github.com/glennswest/rustkube-node/issues/171) |
| OOMKilled, termination messages, hugepages, in-place resize, `activeDeadlineSeconds` on a running pod | [#194](https://github.com/glennswest/rustkube-node/issues/194), [#162](https://github.com/glennswest/rustkube-node/issues/162), [#195](https://github.com/glennswest/rustkube-node/issues/195), [#192](https://github.com/glennswest/rustkube-node/issues/192), [#185](https://github.com/glennswest/rustkube-node/issues/185) |
| `runtimeClassName` (microVM Pods go to stormvisor through it) and AppArmor profiles applied | [#204](https://github.com/glennswest/rustkube-node/issues/204), [#197](https://github.com/glennswest/rustkube-node/issues/197) |
| Reservations enforced as a cgroup limit (they are advertised only) | [#205](https://github.com/glennswest/rustkube-node/issues/205) |
| Actual node CPU/memory, machine info and eviction from cAdvisor's library (node figures are container sums); a stormpump container's filesystem usage; VM tap counters | [#21](https://github.com/glennswest/rustkube-node/issues/21), [#222](https://github.com/glennswest/rustkube-node/issues/222), [#206](https://github.com/glennswest/rustkube-node/issues/206) |
| `readOnlyRootFilesystem` (the container's own root is mounted writable until the engine can mount it read-only) | [stormpump#108](https://github.com/glennswest/stormpump/issues/108) |
| Pallet images' config; versioned goldens; building an image from upstream on demand | [#208](https://github.com/glennswest/rustkube-node/issues/208), [#86](https://github.com/glennswest/rustkube-node/issues/86), stormblock-registry#50 |
| The registry over TLS with the engine token | [#167](https://github.com/glennswest/rustkube-node/issues/167) |
| Class blanks formatted at runtime (`mint_template`) instead of a thin library model | [#214](https://github.com/glennswest/rustkube-node/issues/214) |
| StorageClass redundancy/spread/tier; the class matched by provisioner, not name | [#71](https://github.com/glennswest/rustkube-node/issues/71), [#200](https://github.com/glennswest/rustkube-node/issues/200) |
| ext4 for the 64Ti, 256Ti and 1Pi classes (raw block only); a Block claim's control-plane PV | [#149](https://github.com/glennswest/rustkube-node/issues/149), [rustkube#201](https://github.com/glennswest/rustkube/issues/201) |
| External CSI with a real driver; CSI raw block and volume stats; generic ephemeral claims | [#52](https://github.com/glennswest/rustkube-node/issues/52), [#223](https://github.com/glennswest/rustkube-node/issues/223), [rustkube#94](https://github.com/glennswest/rustkube/issues/94) |
| Broken templates matched by stormblock's own state (today by the compatibility text) | [#203](https://github.com/glennswest/rustkube-node/issues/203) |
| Mirror pods: stormd's API over TLS; the readiness fallback reading stormd's current probe keys; units the release does not carry | [#218](https://github.com/glennswest/rustkube-node/issues/218), [#219](https://github.com/glennswest/rustkube-node/issues/219), [#202](https://github.com/glennswest/rustkube-node/issues/202) |
| `/logs/` (node log files, `oc adm node-logs`); request authorization by SubjectAccessReview | [#198](https://github.com/glennswest/rustkube-node/issues/198), [#180](https://github.com/glennswest/rustkube-node/issues/180) |
| Per-pod waits woken by events and `storm.io/wait-for` | [#201](https://github.com/glennswest/rustkube-node/issues/201) |
| VMs: graceful stop (`Machine::shut_down`), live migration, waiting-for-golden progress, snapshot status detail, SSH keys/user-data in `/vmInstance`, a restore from a RAID twin | [#181](https://github.com/glennswest/rustkube-node/issues/181), [#40](https://github.com/glennswest/rustkube-node/issues/40), [#184](https://github.com/glennswest/rustkube-node/issues/184), [#187](https://github.com/glennswest/rustkube-node/issues/187), [#168](https://github.com/glennswest/rustkube-node/issues/168), [#157](https://github.com/glennswest/rustkube-node/issues/157) |
| VirtualMachineSnapshot/Restore on a node (needs the CRDs); pod-network VMIs live; accessCredentials and tap addresses live | [stormcos#170](https://github.com/glennswest/stormcos/issues/170), [#88](https://github.com/glennswest/rustkube-node/issues/88), [#92](https://github.com/glennswest/rustkube-node/issues/92), [#91](https://github.com/glennswest/rustkube-node/issues/91) |
| Git dependencies pinned by rev (stormvm by rev c7a7859 since #240; stormpump follows `branch = "main"` and stormvm pins an older stormpump, f466116, so two copies are built until they name one rev: stormvm#82) | [#210](https://github.com/glennswest/rustkube-node/issues/210) |
| Subsecond starts measured; regressions seen on 11.91 | [#95](https://github.com/glennswest/rustkube-node/issues/95), [#99](https://github.com/glennswest/rustkube-node/issues/99), [#102](https://github.com/glennswest/rustkube-node/issues/102), [#160](https://github.com/glennswest/rustkube-node/issues/160), [#190](https://github.com/glennswest/rustkube-node/issues/190), [#191](https://github.com/glennswest/rustkube-node/issues/191), [#207](https://github.com/glennswest/rustkube-node/issues/207) |
| Complete live acceptance of the medium and long suites; cases that need node access | [#61](https://github.com/glennswest/rustkube-node/issues/61), [#64](https://github.com/glennswest/rustkube-node/issues/64), [#163](https://github.com/glennswest/rustkube-node/issues/163), [#186](https://github.com/glennswest/rustkube-node/issues/186) |

The former CRI-O/crun/conmon-rs default-stack roadmap (#22/#28) is superseded
by the platform's stormpump selection. An eBPF kube-proxy in this repository
is not a shipping commitment: Cilium owns the service dataplane in the Cilium
edition and flowsdn in the flowsdn edition (#2, #145).
