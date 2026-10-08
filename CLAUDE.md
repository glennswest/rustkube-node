# CLAUDE.md — rustkube-node

The node half of rustkube: the kubelet (`pkg/kubelet`, `cmd/kubelet`), kube-proxy
(`pkg/proxy`, `cmd/kube-proxy`) and the CNI helpers (`pkg/cni`). It ships as a
stage golden through `stormcentral component stage rustkube-node`, not a package
(the legacy bin-only `scripts/build-golden.sh` was removed, #51). The cross-project rules are in
`../CLAUDE.md`; this file is the project's own context and work plan.

## Version

`0.13.0`. There is one version location: `[workspace.package] version` in
`Cargo.toml` (every crate uses `version.workspace = true`).

## Current implementation reference (2026-10-02)

Main baseline: fecb331. See `docs/status.md` for changes since September 25,
the owner's recorded decisions and issue-backed limitations, `docs/configuration.md` for every CLI/env/default,
and `docs/api.md` for ports and actual routes. CLI runtime defaults to native;
stormcos explicitly chooses stormpump. Cilium (or, in the flowsdn edition, flowsdn) owns
Services; the packaged kube-proxy is not started in either edition (#145, #155). PVCs use the built-in stormblock driver and sealed
size-class blanks over ublk, with CSI only for third-party drivers.

Main has VMI adoption, persistent VM-owned disks, accessCredentials, tap address
reporting and snapshot reconciliation; restore is #53. The #114 merge added turbomode's
UID workers; #100 (partial-start unwind), #101 (no sync tick) and #99 (bounded calls)
followed. Historical work-plan entries below record what was true at each checkpoint;
they are not current deployment guarantees.
Owner decisions (#106–#110) are answered: #106 Pods under one parent cgroup, OpenShift's
shape (stormpump#68, then #57); #107 `<volume>-<node>` names, no migration (#59); #108
class size at bind, ratio 1.0, class-sized clones (#62); #109 option A, restore rewrites the
VM's disks to restored PVCs (#53); #110 C2NR0Q2 first (#102).

## Build and test

`sc-build` from this checkout, after `git push` (see `../CLAUDE.md`). The
checkout builds on its own: `apimachinery` is a git dependency on rustkube,
pinned by `rev` in `Cargo.toml`. It needs `protoc` on the build box, for the vendored protos in `pkg/kubelet/proto`:

- `api.proto`: CRI v1 (kubernetes/cri-api, release-1.32)
- `csi/csi.proto`: CSI spec v1.9.0
- `pluginregistration/api.proto`: the kubelet plugin-registration API (k8s.io/kubelet v0.32.0)

## Release goldens: `component stage`, never `component build`

The stormcos image runs rustkube-node as a **stage golden**: stormd, its config
and the kubelet. Build a release golden with

    stormcentral component stage rustkube-node --url http://stormcentral.g8.lo

`stormcentral component build rustkube-node` makes a bin-only golden (the
binaries alone). 11.49 shipped one and the kubelet could not start.
stormcentral now refuses that mix-up. The goldens requested with `build` for
v0.10.0–v0.12.0 (ca8caa9be480, fdc1c7497472, 2e39fa8daf0a) are bin-only. The
owner built the stage golden for v0.12.0: golden-rustkube-node-ce66e97945ad
(image 11.50). `scripts/build-golden.sh` (another bin-only builder) was removed
by #51.

## Work plan

### In progress: #112 (P3), restart backoff across a kubelet restart

2026-10-08. `crashloop.rs` was memory only. Done in code: `persist_to(<state root>/crashloop.json)` from
`recover_state` (loads, drops entries past STABLE), every change saved atomically in wall-clock seconds; keyed by pod
uid, so a recreated Pod inherits nothing. 1 test (restart carries the delay and its doubling, new uid nothing, forget
on disk, forgiven not carried, garbage file). Next: sc-build, golden, close.

### Done: #47 (P3), the last part: an init container's log after it completes

2026-10-08. Status half done (4fefda1, ece4211); the fixed init deadline went with #126. Left: `run_init_containers`
removes a completed init container at once. The native runtime writes no logs; stormpump's removal never deletes the
log file (the engine writes it in the pod's log dir), so `kubectl logs -c <init>` already answers there; a CRI runtime
may take the log with the container. Done in code: `RuntimeService::logs_survive_removal` (false; stormpump true); a
completed init kept in `PodManager::kept_inits` until `stop_pod` where removal would lose the log. Tests (1 changed,
1 new). sc-build 90c291c0ec: 419 pass (after #196's test updates). Golden e3415aaa02e2. Closed.

### Waiting on stormstorage#49: #157 (P3), VM restore from a RAID twin on another node

2026-10-08. #68 superseded (owner: replicated volumes are stormblock-csi's; stormstorage/stormblock/stormdrive do the
work; the built-in class stays node-local). A twin exists only for VM disks that are stormblock-csi claims, so: CSI
group snapshots of them (stormstorage#49, needs-owner), a VM disk on another driver's claim (NVMe/TCP, #142), then
#53's restore from VolumeSnapshots (dataSource). Commented; proposed after stormstorage#49. No code.

### Waiting on stormpump#56 + a build: #181 (P2), a VM stopped gracefully: Machine::shut_down(grace) before the engine's stop

2026-10-08. `stop` sends the engine's stop (SIGTERM to the hypervisor = a power cut) at once when there is a handle;
only `stop_by_control` asks the guest. stormvm main (409e0fc, has 94004b7) offers `Machine::shut_down(grace) ->
PoweredOff | AlreadyGone | StillRunning(why)`; the lock has stormvm 180fa13 (without it).
1. [ ] Lock: stormvm 180fa13 → 409e0fc (all its crates), on branch wip/181-graceful-stop, built on the build box
       against stormpump 13cf2c9.
2. [ ] `stop`: shut_down(grace) in the background (grace = the VMI's terminationGracePeriodSeconds, default 30),
       the engine's stop only on StillRunning; `stop_by_control` uses shut_down too.
3. [ ] Node shutdown (item 3) waits on stormpump#56 (the engine stops the kubelet before the hypervisors; the guest's
       grace is an owner decision there). Tests, docs, CHANGELOG; sc-build; golden; leave #181 open on item 3.
       2026-10-08: items 1+2 coded on branch wip/181-graceful-stop (37a5843): `graceful_then_forced` (shut_down → force
       only on StillRunning, or on PoweredOff/AlreadyGone when the engine still sees it running 5 s later), `grace_from`
       (terminationGracePeriodSeconds, default 30), `stopping` an Arc, `stop_by_control` uses shut_down; 1 test. NOT
       built: the lock job (`sc-build 'cargo update -p stormvm-control --precise 409e0fc…; git diff Cargo.lock; cargo
       build'` on the branch) waited out its hour behind the master's build drain for the 11.95 test (owner: builds
       off). Resume: rerun it on the branch, apply the diff, build/test the branch, docs + CHANGELOG, merge, golden.
       Item 3 (node shutdown) after stormpump#56's grace decision. #181 proposed after stormpump#56.

### Done from this side: #57 step 4, CPU requests as Pod cgroup weights (stormpump#68's Pod group, OpenShift's shape)

2026-10-08. stormpump#68 done (golden-stormpump-accd1a3e8e61, stormpump@13cf2c9, release request stormcos#309):
`Spec.group` (node | pods | pods/burstable | pods/besteffort, payload v8) and `GROUP_SET` (a Pod group's cpu.weight
/ memory.max). Also QUERY `MEMORY` (anon, file, inactive_file, working_set; stormpump#64).
1. [x] Lock: stormpump 795b92e → 13cf2c9 (`--precise`), stormvm stays 180fa13; build on the build box.
2. [x] `spec_for`: group by the pod's QoS class; `cpu_weight` from `cpu_shares` by upstream's conversion;
       `GROUP_SET pods` weight from allocatable CPU at start. Working set from QUERY MEMORY when the engine has it.
3. [x] Tests, docs, CHANGELOG; sc-build; golden (ships with stormpump's accd1a3e8e61: an older engine refuses a v8
       spec, so the release must carry both); close.
       Done on wip/57-pod-groups (f6312e9 code, f2163fd lock; lock diff from build job 15d6107177, whole workspace built
       against stormvm 180fa13), fast-forwarded to main. sc-build fe6f90d674: 418 kubelet unit pass (3 new/changed).
       Stage golden golden-rustkube-node-2842aefdb2cf (a87d82e), stormcos#366; must ship with golden-stormpump-accd1a3e8e61
       (stormcos#309), said on both. #57 closed.

### Done: #50 (P2), node services' lifecycle Events

2026-10-08. Done before: Started/Stopped/BackOff on transitions (7e6f4b0), NotStarted (7f4f3d1), exit and tail on
Stopped/BackOff (#82). Left: first sight said nothing, events carried observation time, a failure read as Stopped.
1. [x] `mirror::transition`: first sight judged against the API's mirror pod (same startedAt ±5 s and restartCount =
       the same run, nothing; else Started with the real start time; down while the API says running = ended);
       non-zero exit / signal = Warning Failed. mirror_node_services reads the mirror list first, then the events.
2. [x] Test, README, CHANGELOG (700d134). sc-build job 5246ba2d95: 416 pass. In golden c2f201fd4cb5 (stage: unchanged).
       Closed.

### Done: #42 (P2), the node half of volume expansion (ReadWriteOncePod done in v0.8.0)

2026-10-08. rustkube#63 done: a grown claim has `allocatedResourceStatuses.storage: NodeResizePending`,
`Resizing` + `FileSystemResizePending`, PV capacity = new size, claim capacity old. Upstream then: NodeExpandVolume,
capacity = new, both conditions removed, allocatedResourceStatuses dropped.
1. [x] `csi.rs`: `expand_volume` capability, `node_expand`. `csi_volumes.rs`: `resize_due`, `resized_status`,
       `expand_csi_volumes` (records → PV → claim; driver call; status merge patch; Events). csi sweep loop watches PVCs.
2. [x] Tests (2), csi.md, status.md, CHANGELOG (1af5ece). sc-build job 8d5ba26c64: 415 kubelet unit pass. Stage golden
       golden-rustkube-node-0ee0277c7dba (stormcos#366). Closed. Live with #52's driver (stormblock-registry#99).

### Paused (P0 #104 first): #21 (P2), cadvisor's library for node / machine / filesystem stats and eviction signals

2026-10-08. Work is on branch `wip/21-cadvisor-node` (5f81dec), reverted on main (290938e) so main keeps building:
the dependency needs Cargo.lock entries made on the build box. Scoping answered on the issue (owner: glennswest/cadvisor;
upstream split cadvisor into a kubelet library: node/fs/machine there, containers on CRI). On the branch:
- `cadvisor-host` + `cadvisor-model` (git, rev afb46a0) in Cargo.toml; **lock not made yet**: `cargo update -p` cannot
  name a new package, so run `sc-build 'cargo metadata --format-version 1 >/dev/null; git diff Cargo.lock'` on the
  branch and apply the printed diff (that job was started at 5f81dec; read it or rerun).
- `node_stats.rs`: root cgroup CPU (`CgroupReader::read_stats("/")`), root memory as cAdvisor computes it (usage =
  MemTotal − MemFree, working set = usage − inactive_file, available = total − working set), machine info once;
  `render` (machine_*, root memory usage/rss); 2 tests.
- `metrics::render_cadvisor_with_node` (root `id="/"` series in the cpu/working-set families); `/stats/summary`
  node cpu/memory from the root (container sums only as fallback); MemoryPressure on capacity − working set.
Resume: lock, merge the branch to main, sc-build, live check (/stats/summary, /metrics/cadvisor on a node), close.
Not here: PSI, imagefs (images are stormblock volumes on stormcos).

### Paused (P0 #104 first): #60 (P2), each PV's storage placement: volume, drives, shelf/bay, RAID partners

2026-10-08. Both sources exist: stormblock `GET /api/v1/volumes?placement=true` (`placement.drives[]` serial/wwn,
`legs` policy/health/missing, `rebuild`, `arrays[].members[]` state/drive/node = RAID partners; stormblock#136) and
stormdrive `GET https://<node>:9092/api/v1/placement` (drives with wwn/serial, `shelf.key`, `labels.shelf`, `bay`,
health, designation; TLS with the node CA, a node-CA client cert reads it, stormdrive#19). The kubelet's apiserver
client is that CA + `kubelet.crt`. The node-volume mirror covers only `-data/-state/-logs`; built-in claim PVs are
never refreshed, so placement is its own pass over every PV of this node's stormblock volumes.
1. [x] `pv_placement.rs`: drive index (wwn, else serial); annotations `storm.io/volume-id`, `storm.io/golden`,
       `storm.io/redundancy`, `storm.io/health`, `storm.io/rebuild`, `storm.io/drives` (JSON: wwn, serial, model,
       node, shelf, bay), `storm.io/raid-partners` (JSON: array, level, index, drive, node, state); labels
       `storm.io/shelf` (one shelf), `storm.io/redundancy`, `storm.io/health` (label-safe); Events on change
       (VolumeDegraded / VolumeHealthy, RebuildStarted / RebuildFinished, VolumeMoved, RaidPartnerChanged).
2. [x] `placement_loop`: engine volume changes + 60 s; merge-patch only what changed. stormdrive unreachable:
       stormblock's half alone, said once.
3. [ ] Tests, docs (node-volumes.md, README), CHANGELOG; sc-build (stormcentral#544); close.
       Done: 6610901, b1e91d6 (TLS then plain HTTP: 11.91's stormdrive is plain; its /api/v1/placement shape checked
       live on server3, read-only: wwn `naa.…`, no shelf/bay on a blade). 4 tests. Paused for P0 #104; the sc-build of
       b1e91d6 (which carries #104) is running. Resume: read that build, then a live PV check, close.

### Shipped (golden 0fc1a74fff24): #84 (P2), each stormpump workload's cgroup → pod/container identity, for cadvisor

2026-10-08. cadvisor (cadvisor#3, #15) waits on this repo to pick the shape. Found: stormpump does not report a
workload's cgroup name, but QUERY's info block gives its pid (`stormpump_abi::query::info`) and the kubelet shares the
host pid namespace, so `/proc/<pid>/cgroup` names it; a sandbox's holder pid comes from SANDBOX_ACQUIRE. cadvisor
(stormcos boot unit) mounts only its data and logs, so either shape needs a stormcos mount.
Shape chosen: one JSON file per workload, `/run/rustkube/workloads/<cgroup basename>.json` (host /run, the kubelet's
own /run): cgroup, pid, kind (container | sandbox), reports_network (the pod-network sandbox only), namespace, pod,
pod_uid, container, container_id, image, labels (`io.kubernetes.*` + the CRI labels), annotations. Written atomically at
container start / sandbox made, removed at removal; files whose pid no longer has that cgroup swept at start. No
auth, inotify-watchable, survives a kubelet restart.
1. [x] `workload_identity.rs` (record, cgroup_of, publish/withdraw/sweep); `RingClient::query_info`.
2. [x] stormpump runtime: publish at start/sandbox, withdraw at removal; sweep at connect.
3. [ ] Tests, docs (README, api.md), CHANGELOG; file stormcos (cadvisor mounts host /run ro) and comment cadvisor#3/#15;
       sc-build (stormcentral#544); close. VMs (cadvisor#15) next: same record, kind vm, from the VM manager.
       Done: dcdc68c (pods), 728c185 (VMs: kind vm, withdraw by id); stormcos#391 filed (cadvisor mounts host /run);
       cadvisor#3/#15 told. sc-build refused (stormcentral#544); #84 proposed after it. Then golden, live check, close.
       2026-10-08: built at c8256ac (job 61569b65c2), 413 kubelet unit pass incl. the 3 identity tests. Stage golden
       aborts (stormcentral#362); #84 proposed after it. Live check then: files in /run/rustkube/workloads on a node.

### Done: #70 (P2), Events on the claim while its blank mints, and when a mint or clone fails

2026-10-08. Done before: a blank not `ready` is a wait (c78baf0, #63), the mint is background and bounded by
`MINT_TIMEOUT` and found again by name (db9b783, #99); the pod gets FailedMount. Left: Events on the **PVC**.
1. [x] `clone_blank(class, name, claim)`: Normal `Provisioning` (minting / waiting for the blank's format and
       seal), Warning `ProvisioningFailed` (a refused mint or clone, the engine's words), Normal
       `ProvisioningSucceeded` (cloned). External-provisioner's reasons; the recorder aggregates repeats.
2. [ ] Tests (fake apiserver + stormblock: the claim's Events), README, CHANGELOG; sc-build (stormcentral#544); close.
       Done: ad70ee9 (1 test). sc-build refused (409 build VMs are off, stormcentral#544); #70 proposed after it.

### Done: #79 (P2), an image the node's registry has no golden of: ask the cluster, else a pull failure

2026-10-08. The kubelet sent `POST /v1/clones {golden, remote_image}`; sbregistry dropped `remote_image` (f9bcfdd), so
an unknown image 404'd. Since #104 (0241211) a pull is `GET /v1/goldens/{image}` and a miss is ErrImagePull with
the registry's answer, option (b). Found: that GET does not start sbregistry's cluster fetch (`repljobs::demand`
runs only on the clone routes, 503 "fetching it from the cluster … retry shortly"; `/v1/replicate` is admin), so
#104 lost it. Option (a) (build from upstream, `POST /v1/goldens` pull-through) waits on stormblock-registry#50.
1. [x] A 404 golden: `POST /v1/clones {golden}` as the demand; 503/404 → ErrImagePull with its message (retried on
       the pull back-off); a clone minted because the golden turned ready meanwhile is deleted and the record read.
2. [x] File sbregistry (stormblock-registry#98): a demand that mints nothing (`GET /v1/goldens/{name}?demand=true`).
3. [ ] Tests (fake registry: demand 503, 404, ready-in-between deletes the clone), README, CHANGELOG; sc-build
       (stormcentral#544); close.
       Done: 3822c45 (1 new test, 1 extended). sc-build refused (409 build VMs are off, stormcentral#544); #79
       proposed after it.

### Shipped (golden 0fc1a74fff24): #104 (P0), every container its own CoW root (owner: A, no exceptions)

2026-10-08. Owner (#104, 10-07): every container's root is its own CoW clone of the image's **sealed golden**,
writable, deleted with the container; a restart gets a fresh clone; never shared; exactly one layer between
golden and what the workload writes (no image-level clone, no clone of a clone). `readOnlyRootFilesystem` mounts
the container's own clone read-only. Found:
- Today: `create_container` roots every container at the image *path* (`/pallets/<x>`, or the pull's shared
  mount `/run/stormpump/images/<clone>`), and `start_container` registers that directory: shared roots.
- Pallets: the slab holds `<vol>.golden` (sealed) and its first clone `<vol>`, mounted at `/p/<path>` by
  `rd.stormblock.mount=<vol>:/p/<path>` on the kernel command line (`/pallets` → `/p`; e.g.
  `cilium-operator:/p/operator-generic`). Engine listing: `parent`, `sealed`, `in_use`, `owner`.
- Pulls: sbregistry's golden record (`GET /v1/goldens/{image}`) names `template_name`, an fstemplate in this
  node's engine; `POST /v1/clones` minted the per-image clone that every container shared (#143's binding).
- stormpump (lock 795b92e) has no read-only root: VOLUME_REGISTER takes no flag, `Root` is Inherit|Chroot.
Steps:
1. [x] Image service: a pull only makes sure the golden is ready (registry record → `template:<name>`); no clone,
       no attach, no mount, no bind. Pallets resolve as before (`/pallets/<x>`).
2. [x] Runtime (engine client + node): `create_container` clones the golden (`template:` → fstemplate clone;
       pallet → `<vol>.golden` via /proc/cmdline, else `<vol>`'s sealed parent), attaches (ublk), has PID 1 mount
       it at `/run/stormpump/roots/<cid>` (the root handle); argv[0] and user names are read from that mount.
       `start_container` spawns on that handle. Removal (and a failed create) releases the handle, detaches and
       deletes the volume; a refusal is retried (#90). Orphan sweep: `ctr-*` volumes not in use and not ours.
3. [x] readOnlyRootFilesystem: file stormpump (read-only device mount / root); until then the clone is private
       and writable, said in the log.
4. [ ] Tests (one clone per container start, two containers of one image get two roots, removal deletes it,
       failed create cleans up), docs (README, status.md, configuration.md), CHANGELOG; sc-build; golden; close.
       Supersedes #143's image-level clone and #161 (image GC: nothing image-level is held any more).
       Done: container_roots.rs (d6e0aaa), wiring 0241211, docs c393372; stormpump#108 filed (read-only device
       mount). Tests (8 new). NOT built: sc-build refused `409 build VMs are off: set [buildvms] pve_host`
       (stormcentral#544, commented: holds this P0). #104 proposed after it. Then build, golden, live check
       (two pods of one image write the same root path, read back different), close; close #161 as superseded.
       2026-10-08: built at c8256ac (job 61569b65c2, after #189's two compile errors): 413 kubelet unit pass, every #104
       test by name. `component stage` aborted twice (`[platform in] …/stormd`, `[platform abort]`, exit 255) =
       stormcentral#362 (stormd golden lacks bin/stormd), commented; #104 proposed after it. Then stage, shipped, live check.
       #362 fixed: stage golden golden-rustkube-node-0fc1a74fff24 (7df677b = c8256ac code), stormcos#366 (asked the gate for
       the isolation / one-clone / pods-start checks); `stormcentral shipped`. Close #161 as superseded once released.

### Paused (P0 #104 first): #71 (P2), built-in driver: redundancy / spread / tier from the StorageClass

2026-10-08. Stopped before code at stormcentral's word (P0 #104 waited 40 h). Findings:
- Engine: `POST /api/v1/fstemplates` and `/api/v1/volumes` take `redundancy`, spelled with the spread:
  `mirror:2@shelf` (stormblock `RedundancyPolicy::parse`, `@rung` suffix); a clone inherits its template's
  policy. **No `tier` field** on fstemplates: that is stormblock#151 (open; also /v1 for stormblock-csi).
- The default for a class naming no policy is stormblock's design "owner decision 2" (`none` vs `mirror`),
  unanswered: keep today's `none`, and today's blank names, so shipped blanks still hit.
- Both halves match the class *name* `stormblock`: `storage::provisioned_here` here, rustkube
  `controller-manager/src/stormblock.rs` STORAGE_CLASS. stormcos's class: provisioner `stormblock.storm.io`,
  WFFC. A second class (e.g. `stormblock-mirror`) needs rustkube to match by provisioner too: file there.
Design (resume here):
1. [ ] storage.rs `Policy::from_class(sc)`: parameters `redundancy` (none|mirror|mirror:N|raid5:D+1|raid6:D+2),
       `spread` (rung), `tier` (absent/`hot` only, else refused naming stormblock#151); unknown keys refused.
       `engine()` → `<redundancy>[@<spread>]` or None; blank name `template_name(class)` + `-<slug>-<spread>`
       when not none (`pvc-ext4j-1048576m-mirror2-shelf`).
2. [ ] pod_manager `provision_claim_volume`: resolve the class (GET storageclasses/{name}; provisioner
       `stormblock.storm.io` = ours; unset = ours, default policy; `""` = not ours; unreadable = wait, never
       "none"); pass the policy to `clone_blank`/`mint_template`/`mint_blank` (`redundancy` in the body) and
       `raw_volume`. A refused mint (InsufficientDomains) also as an Event on the PVC (`events.object_event`).
3. [ ] Tests (policy parse, names, mint body against the fake stormblock), README/csi.md, CHANGELOG; sc-build
       (submit when the slot is free: stormcentral#541); file rustkube (provisioner match); golden; close.

### Shipped (golden 670219fd054b): #82 (P2), a node service's last exit and output on its mirror pod

2026-10-08. stormpump#51 (adc64f8) writes `last_exit_code` | `last_exit_signal`, `last_exit` and `last_output` (20
lines) per asset in assets.json, once it has exited, kept across the restart. fc867c1 (#124) reads `last_exit` /
`last_output` for pods/log only (`node_logs.rs`). `container_failing` is stormd's own event (stormd README,
"Events"), not the kubelet's: the kubelet's counterpart is the mirror's BackOff/Stopped Event.
1. [x] `mirror::Asset.last_exit` (code, signal, text, output); `mirror_pod_with`: `lastState.terminated`
       (exitCode, or 128+signal, signal, reason Error, message = output tail ≤ 80 lines / 4 KiB) and the same on a
       stopped service's `state.terminated`; `status_current` and `table_key` see a new exit.
2. [x] BackOff/Stopped Events carry the exit and the tail.
3. [x] Tests (2), README, CHANGELOG (8a8007e). sc-build 8a8007e (job 2eb0ae2979, submitted once the slot was free:
       queued jobs are cancelled, stormcentral#541): 400 kubelet unit pass. Stage golden
       golden-rustkube-node-670219fd054b (stormcos#366; the first stage try hit a transient GitHub fetch error),
       `stormcentral shipped`.

### 2026-10-08: the build backlog ran

The build-VM slot came free; sc-build 134fd60 (job 9161dda65c) `cargo build --locked && cargo test --locked`: 398 kubelet unit pass, plus 34 proxy, 25 CNI and the integration tests; every waiting issue's own
tests by name. First run 924aaaa811 found two #85 compile errors (#188, fixed 134fd60). Closed: #89, #147, #87, #94,
#96, #77, #85, #65, #80, #188. Stage golden golden-rustkube-node-6894f5af0f45 (134fd60, 3m42s), release request
stormcos#366: `stormcentral shipped` for #126, #115, #165, #174, #111, #133 (told stormcos#329 about --max-pods).
#141 proposed after stormcentral#526 (live burst), #95 after stormpump#107 (claim target).

### Waiting on the owner: #68 (P2), replicated claims (a StorageClass asks for replicas on other servers)

2026-10-08. Since the issue (09-25) replicated claims went to stormblock-csi (owner, stormblock-csi#29, 10-06):
class `stormblock-csi`, params `replicaSlaves`/`pool`/`tier`/`spread`, a stormstorage distributed volume served
over NVMe/TCP, operator failover. stormstorage has `prefer_node` (#50) and the RAID shape is decided (#41, option
1). The kubelet already runs third-party CSI (#52, propagation #81). Multi-node blockers either way: one cluster
stormstorage (stormstorage#56, stormcos#354), NVMe/TCP attach from a non-head node (#142). Asked on #68: A
stormblock-csi owns it (recommended; built-in stays node-local, #71 for in-node redundancy), B the built-in driver
too (head-local only), C the built-in class sends replicas>1 to stormblock-csi. `wait-owner` run. No code.

### Done: #80 (P2), a Pod blocked on a claim a VM here holds says why

2026-10-08. The two-writer case is closed by admission since #100/#114: a VMI's claims are reserved Exclusive
(`claims_for`), so a Pod wanting the same claim is not admitted (`reconcile` → `AwaitEvent`). Left (owner's recheck
on #80): the blocked Pod gets no reason (no waiting status, no Event), no test, README/status.md say the check is
missing.
1. [x] `Reservations::blockers(key, claims)`: each claim held against this request and who holds it.
2. [x] `reconcile`: a Pod refused on a claim gets Pending + `ContainerCreating` "claim ns/c is in use by
       VirtualMachineInstance ns/vm on this node" (or by Pod … for RWOP) and a FailedMount Event
       (`PodManager::claim_held`); still AwaitEvent (the release wakes it).
3. [ ] Tests (blockers; a Pod after a VM), README/status.md, CHANGELOG; sc-build; close.
       Done: 43ab0e9 (1 test). Job 560e7c6206 cancelled while queued (stormcentral#541); #80 proposed after it.

### Done: #65 (P2), the Node advertises KVM

2026-10-08. `node_status.rs` reports no KVM, so a test Job (no /dev/kvm in its container) cannot tell a VM-capable
node (`requires: [kvm]`, stormcos_qa vm-lifecycle). VMs run only with the stormpump engine (`Kubelet.vms`). The
kubelet's own container has no /dev/kvm (the engine spawns the VMM in the node's mount view), so detection reads
the kernel: `/sys/class/misc/kvm` (the kernel registered the device), or /dev/kvm (or /hostroot/dev/kvm) opens.
1. [x] `kvm_available()`; NodeReporter `with_kvm` (only with the VM manager); capacity/allocatable
       `devices.kubevirt.io/kvm: 1k` (KubeVirt's), every heartbeat; labels `storm.io/kvm=true`,
       `kubevirt.io/schedulable=true` at registration and patched on an existing node whenever they differ
       (`false`/removed when KVM is gone).
   Done: 8fb1461 (2 tests).
2. [ ] Tests, docs (README, configuration.md), CHANGELOG (8fb1461); sc-build; close (tell stormcos_qa#16 how to
       check: `-l storm.io/kvm=true`). Job d23dc7b9a5 cancelled while queued (stormcentral#541); #65 proposed after it.

### Done: #85, a VMI placed with spec.nodeName is never started

2026-10-07. Found: vm_loop lists VMIs with `fieldSelector=status.nodeName=<node>` and filters `want` on
status.nodeName, so a hand-placed VMI (spec.nodeName only) never reaches `assigned_to`. rustkube's scheduler
(`virtualmachine.rs`) places by status.nodeName, else spec.nodeName, and skips a VMI that has either.
1. [x] One rule (`assigned_to`, rustkube's): status.nodeName when set, else spec.nodeName.
2. [x] vm_loop and `list_for_node`/`watch_for_node`: a second list (and watch) by `spec.nodeName`, merged by
       uid, narrowed by `assigned_to` (`placement_urls`, `placed_on`).
3. [x] A hand-placed VMI taken here gets `status.nodeName` written (`take_hand_placed`, uid-guarded).
4. [ ] Tests (2), docs, CHANGELOG (c71b858); sc-build at c71b858 (stormcentral#536 closed, so this one build
       also covers #126, #115, #147, #89, #87, #94, #96, #77); close.
       Job a8e2791d84 cancelled while queued: the master's deliberate drain (1 slot, queued jobs cancelled until
       stormcentral's bootstrap job 03e9cbfde9 installs; said on stormcentral#536). `tmp/wait-drain.sh` reruns
       the build once that job is done or the slots come back.
       The bootstrap job was cancelled too; 5fd1caed32 (ee08d48) cancelled the same way. Filed stormcentral#541,
       #85 proposed after it. Rerun `SC_BUILD_VM=1 sc-build 'cargo build --locked && cargo test --locked'`, close.

### Done: #77, the client certificate reloaded when stormcert renews it

2026-10-08. The apiserver client (one `reqwest::Client`, cloned into every module) takes the client pair once.
Rather than rebuild it everywhere: a rustls `ClientConfig` (CA roots + a `ResolvesClientCert` whose pair can be
swapped) via `use_preconfigured_tls`; every clone's next handshake presents the current pair. The pair's directory
is watched (+ hourly); a new pair that does not parse or whose key does not match is refused, the old one kept.
Only with a CA and verification on (stormcos); insecure / no-CA keep the static identity, said in the log.
1. [x] client.rs `ReloadingClientCert` + `build_authed_client_reloadable`; KubeletConfig cert/key paths; reload
       task; tests (rcgen pairs: swap, unchanged, mismatched key refused); docs, CHANGELOG; sc-build.
       Done: d1ad901 (2 tests). NOT yet built: the SC_BUILD_VM job was cancelled while queued. Rerun, close.

### Done: #96, a node service's mirror pod is Ready only while it answers

2026-10-08. Each service golden's stormd config declares its liveness (`[process.liveness] type = "http"`, `url =
http://127.0.0.1:<port><health>`, stormcos `service_golden`); its boot unit names the golden (`root <volume>`) and
`profile host`. `node_health.rs`: the URLs from boot.d + the goldens under `/hostroot`; probed every 10 s (2 s);
3 failures → not ready, one answer → ready; a flip queues a mirror pass. `mirror_pod_with`: Running, ready false,
Ready/ContainersReady False, reason Unhealthy; a Warning `Unhealthy` Event on the flip.
1. [ ] Code + tests (5) + README, CHANGELOG (ff7000c; the push needed 11 tries, GitHub 500s). NOT yet built: the
       SC_BUILD_VM job was cancelled while queued (stormcentral#536). Rerun, close; live check on a node.

### Done: #94, the VMI verbs on :10250 (`PUT /vmVerb/{ns}/{name}/{verb}`)

2026-10-08. stormvm's router (lock 180fa13) serves `PUT /api/v1/vms/{ns}/{name}/{verb}`: pause, unpause,
softreboot, reset, status, freeze, thaw, snapshot (and migrate/receive, not for here: migration is #40's, by the
VMI's status). rustkube#141 (the apiserver's verb proxy) is open and has not fixed a path: this route is the one #94
proposed, said on rustkube#141. KubeVirt says `unfreeze` where stormvm says `thaw`: both accepted.
1. [x] `vm_verb`: verb allow-list, query forwarded without `token`, the console's handover (`to_console`, shared
       with `/vmConsole`). Tests through the kubelet router; docs (api.md, README), CHANGELOG; sc-build.
       Done: 8674302 (2 tests; contract on rustkube#141). NOT yet built: the SC_BUILD_VM job was cancelled while
       queued. Rerun, close; real-node snapshot check after rustkube#141.

### Done: #89, the serving pair: never a silent self-signed fallback; reloaded when renewed

2026-10-08. The wait-then-fatal half was #69 (5d869b9): `--tls-cert-file`/`--tls-private-key-file` wait 60 s, then
exit naming the flag. Left: say which pair :10250 serves, and reload it when stormcert renews it (at boot,
stormcert#14) instead of serving the old one until the kubelet restarts.
1. [x] Paths into `ServerConfig`; log "configured pair <path>" or "self-signed (no --tls-cert-file)"; watch the
       pair's directory (fs_watch) and `reload_from_pem` when its bytes change; a half-written pair fails and the
       old one stays. Test; docs (configuration.md), CHANGELOG; sc-build (stormcentral#536).
       Done: dd3df0b (1 test). NOT yet built: the SC_BUILD_VM job was cancelled while queued. Rerun, close.

### Waiting on the owner: #104 (P2), private writable container roots

2026-10-08. stormpump's rule ("one storage primitive … no overlay, no tmpfs layering", spec.rs) makes a private
root a CoW clone per container (clone the golden, attach, PID 1 mount as root, delete on removal/restart), the path
claims and pulls already use. Cost on 11.91 warm: ~0.4–1.3 s per start (clone, attach, mount: #95, stormblock#327,
stormpump#107) and a volume per container. Asked on #104: A private by default + readOnlyRootFilesystem keeps the
shared root (recommended), B private only when asked, C A with pre-minted clones.

### Waiting on the owner: #86 (P2), versioned goldens for pods

2026-10-08. Kubelet half clear: the release manifest (`/etc/stormcos/release/manifest.json`, `assets[kind=golden]`
name → digest) says each pallet's golden; a tag/digest naming another version selects `golden-<name>-<sha12>`,
cloned CoW and mounted like a pulled image. Missing: the source when the node lacks the version. sbregistry's
catalog lists forge goldens but "nothing is copied" (#43), and cross-engine clones need NVMe/TCP (#142).
Asked on #86: A copy on demand (recommended), B remote attach, C node-local only. A tag/digest matching no known
golden version keeps running the pallet (upstream OCI tags/digests, Cilium's `@sha256:`).

### Done: #147 (P2), kube-proxy clears stale UDP conntrack entries

2026-10-08. kube-proxy rewrites the DNAT when an endpoint goes but leaves conntrack: a UDP flow to a ClusterIP
(kube-dns) keeps its NAT to the old backend for 30–120 s. (No stormcos edition runs kube-proxy since #145/#155;
the binary ships and this is its correctness.) Upstream: `conntrack -D -p udp --orig-dst <svc> --dst-nat <ep>`
per removed UDP endpoint, `--dport <nodePort>` for NodePorts, and `--orig-dst <svc>` when a UDP Service gains its
first endpoint.
1. [x] `conntrack.rs`: the stale set from the last applied and new UDP backends; a `Conntrack` seam (the
       `conntrack` binary: exit 1 with "0 flow entries" is fine, a missing binary warns once). Deletes after a
       successful apply. Tests (fake apiserver: endpoint replaced, NodePort, first endpoint, TCP untouched); docs
       (configuration.md, README), CHANGELOG; sc-build (blocked: stormcentral#536).
       Done: 033a05a (4 tests). NOT yet built: the SC_BUILD_VM job was cancelled while queued. Rerun, close.
       2026-10-08: e58d5ed7fc (97801bc) cancelled the same way; #147 proposed after stormcentral#541.

### Shipped (golden 6894f5af0f45): #115 (P2), a stormpump exit wakes its own workload only

2026-10-08. An unsolicited exit bumps the ring's counter; `pod_loop`/`vm_loop` answer with `wake_kind` **and**
`worker.enqueue()` (a full list sync), so one exit wakes every Pod and VMI worker and re-syncs every pod.
1. [x] Ring: a broadcast of exiting handles beside the drained queue. Router task: the Pod runtime maps handle →
       container → sandbox → pod uid (`RuntimeService::pod_of_workload`), the VM manager handle → VMI uid; wake that
       uid only (`wake_where`). Unknown handle or a lagged channel: wake both kinds (the old behaviour). The loops'
       exit arms go (the counter stays: it says the runtime has exit events).
2. [ ] Tests (3), README, event-driven-design.md, CHANGELOG (92cd9ef). NOT yet built: the SC_BUILD_VM job was
       cancelled while queued (stormcentral#535/#536). Rerun, then golden (stormcentral#527) and close.
       2026-10-08: e58d5ed7fc (97801bc) cancelled the same way; #115 proposed after stormcentral#541.

### Shipped (golden 6894f5af0f45): #126 (P2), no fixed init deadline; activeDeadlineSeconds bounds it

2026-10-08. `run_init_containers` stops an init still running after 120 s (`DeadlineExceeded`, exit -1). Upstream:
an init runs until it exits; only the pod's `activeDeadlineSeconds` (from its startTime) limits it. The admission
path (production) already waits on the exit event; the non-admission path slept 500 ms on the worker.
1. [x] Drop MAX_WAIT; `active_deadline_left` (startTime, else first seen); past it the init is stopped and reported
       DeadlineExceeded ("Pod was active on the node longer than the specified deadline"); both paths Pending
       (deadline as the due time when set). Tests; README/status.md/CHANGELOG; sc-build. File: activeDeadlineSeconds
       for a running pod is enforced nowhere (follow-up).
       Done: 9a39a40 (3 tests), #185 filed. NOT yet built: both SC_BUILD_VM runs cancelled while queued (platform-wide,
       stormcentral#535/#536). Rerun `SC_BUILD_VM=1 sc-build 'cargo build --locked && cargo test --locked'`, then
       golden (stormcentral#527) and close.
       2026-10-08: e58d5ed7fc (97801bc) cancelled too; #126 proposed after stormcentral#541.

### Waiting on stormcentral#526 (live run; golden 6894f5af0f45 built): #141 (P2), the probe pass's status PUT skipped when nothing changed

2026-10-08. `report_pod_status` skips when the merged status equals `source["status"]`, but `source` is the
watch's copy, which can still be the version before this kubelet's own last PUT: the comparison runs against the
pre-Running status and an unchanged status is written again (and with a stale resourceVersion).
1. [x] Keep, per UID, what this kubelet last had acknowledged (the RV it wrote on, the RV the PUT returned, the
       status). `status_base`: while the watch's copy is still at the RV our write was based on, compare and write
       against the acknowledged status and RV; otherwise the watch's copy. Skip when unchanged. Forgotten on delete.
2. [x] `kubelet_pod_status_writes_total{result="written|skipped"}`; tests (2); metrics.md, CHANGELOG (743c0f6).
       SC_BUILD_VM sc-build: 376 kubelet unit pass.
3. [ ] Golden (stormcentral#527), then a turbomode burst on pvetest1 (stormcentral#526): no second PUT, wait p90
       < 100 ms. #141 proposed after stormcentral#527.

### Shipped (golden 6894f5af0f45): #133 (P2), a start error is never "terminal with no container state"

2026-10-08. The 10-02 trigger was #103 (a pulled image's root unresolved → start_container failed; fixed 4263d80,
shipped). The behaviour it exposed stands: every start error not matched as a wait (image pull, create, start,
runtime unreachable, init failure) made the pod **Failed with no container statuses**, and `logs` 404'd. A failed
pull was also cached for the pod for good. Upstream: image/create errors are waits with the container's reason
(ErrImagePull → ImagePullBackOff, CreateContainerError), a start failure under restartPolicy Never is Failed with
the container terminated `StartError` (exit 128).
1. [x] `CriError::Container{container, reason, message}` from start_pod's create/start; sync arms: ImagePull (forget
       the cached failure; ErrImagePull → ImagePullBackOff; 10 s–5 min back-off), Container (Never + StartError:
       Failed with the terminated status; else Pending with the reason, partial start torn down, first_seen kept so
       the back-off grows), Connection (a wait); the rest Failed *with* init statuses and app containers
       PodInitializing/StartError. `WaitingPod.kind`; logs answer 400 with it (never 404 for an admitted pod).
2. [x] Tests (3 new, 1 extended), README, CHANGELOG (dc3eed0, 2910b5a: a failed init's report kept, #183).
       SC_BUILD_VM sc-build: 374 kubelet unit pass. Golden after stormcentral#527 (with #165, #95, #174, #111).

### Shipped (golden 6894f5af0f45): #111 (P2), restartable init containers (native sidecars, restartPolicy: Always)

2026-10-08. `run_init_containers` runs each init to exit 0 (120 s deadline) and removes it; a sidecar blocks the
start and is killed. Upstream (KEP-753): a sidecar is started in its init slot, the next init waits only until it
has *started* (running; its startupProbe passed, if it has one), it runs and restarts (Always, whatever the pod's
policy) for the pod's life, its readiness counts toward Ready, it does not count toward the phase, it is stopped
after the app containers finish (Never/OnFailure) and torn down after them, in reverse order.
1. [x] `InitContainerStatusReport` + restartable/started/ready/restart_count, `initialized()`; status JSON (ready,
       started, restartCount, waiting); `pod_initialized` and ContainersReady/Ready count sidecars.
2. [x] `run_init_containers`: a sidecar is started and kept (no removal, no deadline); exit before started =
       restart with backoff (Pending), never Failed; startupProbe run from the init path.
3. [x] `start_pod` carries sidecar records into the tracked state; `check_pod_status` probes/restarts sidecars
       (Always), reports them as init statuses, leaves them out of the phase, stops them once the apps finished.
4. [x] `stop_pod`: apps first, then sidecars in reverse declaration order.
5. [x] Tests (5 new), docs (README, status.md, planning/container-startup.md), CHANGELOG (d5c3d85). SC_BUILD_VM
       sc-build: 371 kubelet unit pass. Golden after stormcentral#527 (with #165, #95, #174). Not run on a node.

### Shipped (golden 6894f5af0f45): #174, a stop's grace goes where STOP reads it

2026-10-08. `RingClient::stop` put the grace in `inline_a` (seconds); STOP reads `inline_b` in ms (0 = 30 s) and
`flags::FORCE` for "now" (stormpump docs/ABI.md op 20, same at the lock's 795b92e). Every stop waited 30 s.
1. [x] `stop_sqe`: `inline_b = grace_secs * 1000`, grace 0 → FORCE; unit test; CHANGELOG; sc-build (build VM);
       golden when component stage works (stormcentral#527); close.
       Done: a6b3155; SC_BUILD_VM sc-build: 366 kubelet unit pass. One golden after #527 carries #165, #95, #174.

### Shipped (golden 6894f5af0f45): #165 (P1), --max-pods (owner: option A on rustkube#205; stormcos sets 250)

2026-10-08. `node_status.rs` reports a fixed `pods: "110"` in capacity and allocatable. The kubelet reads no
KubeletConfiguration file and admits Pods against no count of its own, so the flag and the status are all.
1. [x] `--max-pods` / `MAX_PODS` (default 110) → `KubeletConfig.max_pods` → `NodeReporter::with_max_pods` →
       capacity and allocatable `pods`; test; docs (configuration.md, README, status.md), CHANGELOG; sc-build;
       golden; close; tell stormcos it can set 250 (its /24 pod range note stays theirs).
       Done: c635c02; sc-build `cargo build --locked && cargo test --locked`: 365 kubelet unit pass (1 new);
       `kubelet --help` lists `--max-pods <MAX_PODS>` [env: MAX_PODS]. (#179 was my bad extra command, closed.)
       Rerun on a build VM (dev.g8.lo is retired): `SC_BUILD_VM=1 sc-build …` at fb94c9a, exit 0, same results.
2. [ ] Stage golden: `component stage` is blocked until stormcentral#521. #165 proposed after it. Then close #165
       and tell stormcos#329 (which sets 250).

### Done: #81, pod mount propagation onto stormpump's `Mount.propagation`

2026-10-08. Unblocked: stormvm#65 closed (stormvm main builds against stormpump main f466116, stormcast bba68c9).
The lock pins stormpump 30a76d3, stormvm dc1b7ea, stormcast 801f822; all three move together.
1. [x] Lock: `cargo update -p stormpump -p stormvm-node -p stormcast` on dev (CARGO_NET_GIT_FETCH_WITH_CLI), diff
       applied here; fix what the newer stormvm/stormpump APIs break.
2. [x] `spec_for` maps it (599a86e; lock as cargo resolved it f4de851; tests 4545e23, which also covers #164's
       byte-sized emptyDisk). sc-build 4545e23: 364 kubelet unit pass. Golden golden-rustkube-node-a97563b0a5c7
       (stormcos#366). #81 closed, #164 shipped. #52's step 6 is done; its end-to-end with a real driver stays.

### Waiting on stormpump#103: #56 (P1), exec, attach and portForward on :10250

2026-10-08. rustkube's apiserver splices the upgrade transparently (streaming.rs: headers and bytes as they are,
query translated to `input/output/error/tty/command`, `port=`), so the kubelet speaks SPDY/3.1 itself, and
WebSocket for a newer kubectl (client-go falls back to SPDY when the WebSocket upgrade is refused).
Found: stormpump has no op that runs a process inside a running workload, and deposited fds reach only VM domains,
so exec/attach (and `exec_sync`, exec probes) need the engine: filed stormpump#103. Port-forward needs nothing from
it: the kubelet connects inside the sandbox's netns (as health.rs dials) and splices.
1. [x] `spdy.rs`: SPDY/3.1 server session (frames, zlib header blocks with the SPDY/3 dictionary: inflate via
       miniz_oxide's core with the window preloaded, deflate as stored blocks; PING, WINDOW_UPDATE sent as data
       is read, RST, GOAWAY).
2. [x] `portforward.rs` + `/portForward/{ns}/{pod}`: streams paired by requestID (error + data), connect to the
       port in the pod's netns (host network: the node's), splice; errors on the error stream.
3. [x] WebSocket: port-forward's `SPDY/3.1+portforward.k8s.io` tunnel; exec/attach answer 501 naming stormpump#103.
4. [x] Tests (10 new), docs (README, api.md, status.md), CHANGELOG (886ce15, 82769d2; lock 8224957; #173/#175
       were the build/test failures on the way). sc-build 82769d2: 363 kubelet unit pass. Golden
       golden-rustkube-node-ff480857443c (stormcos#366). #127 closed. Not run with a real kubectl/node.
5. [ ] After stormpump#103: exec (and exec_sync) through the engine's op; SPDY `v4.channel.k8s.io` streams
       (stdin/stdout/stderr/error/resize) and WebSocket `v5.channel.k8s.io`; attach on a container's stdin; live
       `kubectl exec -it` / `port-forward` on a node; close #56. #56 proposed after stormpump#103.

### Done: #98 (P1), the image config applied under the pod spec

2026-10-08. sbregistry serves a pushed image's OCI config (`Entrypoint`, `Cmd`, `Env`, `WorkingDir`, `User`) as
`config` on `GET /v1/goldens/{name}`; boot pallets have none until stormblock-registry#58 (open; `Volumes` too).
stormpump's Spec has `uid`/`gid` and the engine drops to them (exec.rs). Found: `runAsUser`/`runAsGroup`/
`runAsNonRoot` are read nowhere, so every container is root; no stormcos manifest sets them or `fsGroup`.
1. [x] `image_config.rs`: the config, a cache by image root (a miss re-asked after 5 min), `compose` (CRI rules:
       command replaces Entrypoint, args replaces Cmd, args alone after Entrypoint; image Env under the pod's;
       WorkingDir/User when the pod leaves them unset; user names via the image's /etc/passwd and /etc/group).
       No config and an empty argv or one starting with `-`: an error naming the image.
2. [x] StormpumpImages fetches the config when it resolves a root (pallet or pull); the runtime composes in
       `create_container`. ContainerConfig `run_as_user`/`run_as_group`/`run_as_non_root` (container, else pod),
       forwarded on CRI gRPC too. emptyDir 0777 as upstream (a non-root user must write it).
3. [x] Tests (8 new; `command_and_args_become_one_argv` had lost its #[test]), docs (README, status.md), CHANGELOG
       (bbbb96a; build fix 01361c6 = #170). sc-build 31e53c6 `cargo build --locked && cargo test --locked`: 353
       kubelet unit pass. Golden golden-rustkube-node-04134493c42a (stormcos#366). Follow-ups #171 (fsGroup), #172
       (image Volumes, after stormblock-registry#58). Closed. Not run on a node.

### Waiting on stormpump#107 (claim target; golden 6894f5af0f45 built): #95 (P1), pod/claim start toward ≤2 s / ≤1 s: the claim's own steps timed

2026-10-08. Done elsewhere: phases on the pod (#132), `sandbox` split (#139); the traced causes (2 s pass, serial
starts, unbounded CNI, inline events, per-pass PUTs) by #99/#100/#101/#134/#138. Still undone from item 1: "the same
for a claim". A claim's volume is one `volume/<name>`; its steps are lookup (PVC, PV, existing volume), make (clone
of the class blank / of a source / raw volume; none when reused), attach (ublk), bind (PV/PVC writes). The
filesystem mount is PID 1's, at container create (in `containers`).
1. [x] `ClaimSteps` from `provision_claim_volume`; `claim/<volume>/{lookup,blank|clone|raw,attach,bind}` in the
       annotation, Event and log line. Tests, docs (README, status.md), CHANGELOG; sc-build; golden.
       Done: 9d7b454; sc-build `cargo build --locked && cargo test --locked`: 345 kubelet unit pass (2 new). Stage
       golden golden-rustkube-node-a259ec6b4c3d (stormcos#366). Status commented on #95.
2. [x] Measured (2026-10-08, 11.91 = ff480857443c; stormcentral `/api/v1/testhosts/<tag>` `last.log`, warm probes):
       container start 0.2–1.0 s (met, pvetest1 + server3); claim 0.8–4.0 s (missed). Kubelet claim total 0.5–0.7 s
       cold / 1.5–1.7 s warm: bind 150–220 ms (ours), PID 1's ext4 mount 0.55–0.88 s warm (stormpump#107), blank
       clone 0.3–0.37 s warm (stormblock#327); control plane 0.3–3.1 s (rustkube#147, commented). The Dell's 11.91
       failed its fresh-slab gate: no samples there.
3. [x] Bind off a pod's start path (d758167): `ClaimBinder` task after the attach; VM path still inline; `ClaimSteps.bind`
       optional. SC_BUILD_VM sc-build: 365 kubelet unit pass.
4. [ ] Golden (stormcentral#527); then the claim target behind stormpump#107, stormblock#327, rustkube#147. #95
       proposed after stormcentral#527.

### Done: #3 (P1), the node side Cilium needs

2026-10-08. Already on main (validated 09-29): CNI ADD with the sandbox's netns, `K8S_POD_*` CNI_ARGS, the IP into
status, a failed ADD fails the sandbox (DEL first), DEL at stop (#137), host network skips it. Item 1 holds: stormcos
mounts the host's /etc/cni/net.d and /opt/cni/bin at the kubelet's defaults (build-goldens.sh:2590).
**Item 2 needs the owner:** rustkube's scheduler refuses every pod on a node whose Ready is not True
(filter.rs:65, no toleration helps), so a kubelet that posts Ready=False without a CNI config would keep Cilium's
own agent off the node: never Ready. And rustkube's node controller removes `not-ready` taints while the Lease is
healthy. stormcos registers no `node.cilium.io/agent-not-ready` taint. Options posted on #3.
1. [x] Test: run_pod_sandbox for a host-network pod with a CNI but no config succeeds with no address; a pod-network
       one is NetworkNotConfigured. `--no-cni` help: pods get an isolated namespace with loopback only (#54 audit).
       Done: 2a2b463; sc-build `cargo build --locked && cargo test --locked`: 343 kubelet unit pass.
2. [x] Question on #3 (A upstream shape + rustkube scheduler/taint change, recommended; B stormcos registers
       Cilium's agent-not-ready taint; C kubelet-managed network-unavailable taint), `stormcentral wait-owner`. Live checks (pod CIDR address, coredns leaves Pending, endpoint
       released, agent stopped → sandbox fails) wait on a test machine (stormcos#337). Resume from the answer.

**Owner (2026-10-07): no NotReady gating** ("we want to get working early"). 2026-10-08: verified live on server3
(11.91, lease c5ccab77ea): pod-to-pod, endpoint released on delete, sandbox fails with the agent gone and starts by
itself when it is back; coredns on the Dell left Pending by itself. Gap found and fixed: network waits had no Event
(02a5b2e, NetworkNotReady / FailedCreatePodSandBox; build waits on stormcentral#544 with #104/#79/#70). Closed.
### Done: #69, a named credential that is missing is waited for, then fatal (never anonymous)

2026-10-08. `main.rs` waited (60 s, then fatal) only for `--apiserver-ca`; `--client-certificate`, `--client-key`,
`--token-file` were read once and skipped when missing, so the kubelet ran as `system:anonymous` (cluster-admin on
sno). The serving pair (`--tls-cert-file`/`--tls-private-key-file`, else self-signed) and `--server-token-file`
had the same silent fallback; stormcos names the client and serving pairs under /data/stormcert, all minted by
the boot (build-goldens.sh:5353), so a wait is safe for them.
1. [x] `wait_for_ca` → `wait_for_file(flag, path, limit)` for every named credential file and `--kubeconfig`;
       absent after 60 s: exit naming the flag. Unit tests (appears late, never appears, empty = not yet).
2. [x] Docs (configuration.md), CHANGELOG (5d869b9; 0f4cc7d restores `DEFAULT_APISERVER`, which the edit dropped:
       build-failure #169, closed). sc-build 0f4cc7d `cargo build --locked && cargo test --locked`: exit 0, 4 new
       main.rs tests pass. Stage golden golden-rustkube-node-236e69973cf6 (stormcos#366). Closed. Not run on a node.

### Done: #139, start timing: `sandbox` split into acquire / cni / status

2026-10-08. `start_pod` times `sandbox` around `run_pod_sandbox` + `pod_sandbox_status`; on stormpump that is
SandboxAcquire, network_ready and the CNI ADD (no stormblock call). Wanted: `acquire`, `cni`, `status` sub-phases
in `storm.io/start-timing`, so the blade's next measurement names the slow step.
1. [x] Runtime reports the sandbox's own split; start timing records it; annotation/Event/histogram/log.
2. [x] Tests (1 new, the end-to-end timing test extended), docs (README, metrics.md, status.md), CHANGELOG (fa18eaf).
       sc-build 0f4cc7d: 342 kubelet unit pass. Golden golden-rustkube-node-236e69973cf6 (stormcos#366). Closed.
       Live check after the release: busybox pods on server3 show `sandbox/acquire|cni|status|other`.

### Done: #51, retire `scripts/build-golden.sh` (the second golden builder)

2026-10-07. Authority is decided: `stormcentral component stage rustkube-node` (stormcos `deploy/build-goldens.sh`
in stage mode). README/BUILD.md already say so (ad43ee9); the script remains. Nothing calls it: stormcentral runs
stormcos's stage recipe or a repo's `deploy/build-golden.sh` (none here); no stormcos/stormpump/master reference.
1. [x] Deleted `scripts/build-golden.sh`; README, BUILD.md, CLAUDE.md, CHANGELOG (10d859c). sc-build `cargo build
       --locked && cargo test --locked`: exit 0, 341 kubelet unit pass. No golden (nothing shipped changed). Closed.

### Done: #156 (P3), /vmInstance refuses metadata from a cache past a staleness bound

2026-10-07. A VMI cache answer is fresh only while the apiserver has been heard from within a bound: the node
Lease renewed (heartbeat, 10 s) or a VMI LIST (`cache_specs`, a live LIST). Past `--metadata-max-staleness`
(default 40 s, the Lease duration; 0 disables) a machine found here answers 503 + Retry-After, not its identity.
An address with no machine here stays 404 (the local record is the truth for "not here"); the token path (#122)
is live GETs already.
1. [x] `heartbeat` returns whether the Lease was renewed; VmManager `note_apiserver_contact`, bound, stale → 503.
2. [x] Flag/env/config, tests (2 new), docs (README, api.md, configuration.md, status.md), CHANGELOG (5ae5c2e). sc-build
       `cargo build --locked && cargo test --locked`: 341 kubelet unit, 3 node_reregister pass. Stage golden
       golden-rustkube-node-c539820ae4ec, release request stormcos#366. Closed. Not run on a node (no live partition).

### Done: #155, docs: no stormcos edition runs kube-proxy

2026-10-07. de51950: README, BUILD.md, status.md, presentation, CLAUDE.md (flowsdn owns Services in its edition,
#145; checked against stormcos main). sc-build: deck renders (12 slides), `cargo build --locked` ok. Closed.

### Waiting on stormcentral#526: #61, short, medium and long test suites (stormcentral docs/test-standard.md)

2026-10-06. `test/` has medium's storage cases (#64, #59, #62, #67); short and long are one skip each. The
runner's namespace Role is `*`; cluster reads come from `test/requires.toml` (only [medium] declares `nodes`, which
every suite needs to resolve STORM_NODE). Pods are pinned with `spec.nodeName` (the kubelet, not the scheduler).
1. [x] Workload modes (scratch image, no shell): `echo`, `exit <code>`, `sleep` (exits 0 on SIGTERM),
       `fail-once <dir>`, `write-file`, `expect-file`, `expect-env`.
2. [x] short (< 2 min): node Ready + heartbeat + no pressure; a pod runs, has a pod IP, its log reads back through
       the apiserver; a pod exiting 3 is Failed with exit code 3; a running pod deleted is gone within its grace.
3. [x] medium pod cases (with or without the storage class): OnFailure restart (restartCount 1, `previous` log),
       init container before the main one (emptyDir), configMap volume + configMapKeyRef + fieldRef env, a missing
       image waits with ErrImagePull/ImagePullBackOff and never runs.
4. [x] long (night, pve VM): waves of pods sized from the node's allocatable pods (every 4th with a 16Mi built-in
       claim when the class is there); per wave p50/p95 start, drain, leftover pods/PVs; a wave > 2× the first's
       p95 (+2 s) or any residue is a failure. VM waves are stormcos_qa's vm-waves.
5. [ ] requires.toml [short]/[long], Job yaml notes, docs, CHANGELOG; sc-build `cd test && cargo test && build`;
       runs on C2NR0Q2 (short, medium) when the queue moves. Gaps that need node access (#35 restart adoption,
       #75 guest persistence, #72 mirror-pod logs, #83 snapshot) filed as a follow-up.
       #97: short 89e5de132e (pvetest1) dropped by a stormcentral restart (stormcentral#466), bb31b533fe: no VM 3101.
       #97 proposed after stormcos#337; any suite's run past `environment` closes it (a blade after 06:00 Chicago too).
       Done: d20ef52; sc-build `cd test && cargo test --locked; cargo build --release --locked`: 28 pass, release
       builds. Filed #162 (terminationMessagePath never read), #163 (node-access medium gaps). Live: short
       c822f4832d, medium 5d9d765385 dropped by a stormcentral restart; requeued short e4bbc4ea0f, medium
       2434af9cf2 (C2NR0Q2; pvetest1 erroring: no VM 3101). Earlier medium fff1f4d9d9 (65e3c3c): the Dell's
       apiserver stopped answering mid-run (no results; 6443 refused after). If 2434af9cf2 does it again, suspect
       the storage cases' load and file it. Close #61 on their results.
       Both errored: the Dell's apiserver is down (stormcos#337: engine :9090 and :6443 gone since 20:55Z, reinstall
       queued; commented that fff1f4d9d9 already lost the apiserver at 17:35Z). Now queued: short 6ab7e3709e
       (pvetest2), medium 1538b1fa13 (pvetest2), short 48f97966b0 (server1).
       All three errored on the machines (pvetest2: no VM 3102; server1: a stormcentral restart). No test machine
       works (pvetest1/2 VMs missing, the Dell down; blades are off 19:00–06:00 Chicago). #61 proposed after
       stormcos#337. Resume: `stormcentral test run rustkube-node short|medium --tag C2NR0Q2` once it is reinstalled.
       2026-10-08: the Dell is back (11.89) but test images are still built on the retired dev.g8.lo (stormcentral#521;
       stormcos_qa turbomode 630379c31b failed there). Test crate passes on a build VM (28 tests). #97 proposed after
       stormcentral#521; fff1f4d9d9 reached `running` but its log was lost with the Dell's apiserver.
       #521 closed early; #61 and #97 proposed after stormcentral#526 (test-image builds still go to dev).

### Waiting on stormpump#47: #118 (P2), pod capabilities onto the ring

2026-10-06. stormpump main (819b55d) has no capability field in `Spec`; #47 (open) is the engine side (owner on
stormpump#59: pods get the runtime's 14 + their `add`, boot.d services the full set). The lock's stormpump
(30a76d3) cannot move until stormvm#65 either.
1. [x] Now: `securityContext.capabilities.drop` parsed (`ContainerConfig.drop_capabilities`) and forwarded on the
       CRI gRPC path (`Capability.drop_capabilities`), which also ignored it. Test, docs, CHANGELOG. c934c5f; sc-build
       `cargo build --locked && cargo test --locked`: 340 kubelet unit + CRI round trip pass. No golden (CRI path only).
2. [ ] After stormpump#47 (and stormvm#65 for the lock): map add/drop/privileged into `Spec`'s field in `spec_for`;
       test `CapEff` (NET_ADMIN added, NET_RAW dropped). #118 proposed after stormpump#47 (commented there: names
       add/drop + privileged, or a resolved mask if the engine prefers).

### Done from this side: #90 (P1), descriptors in PID 1: refused workload release after a stop

2026-10-06. Half 1 (a failed start's volumes) was af1704b: handles kept on the record, the Cleanup pass's
`stop_pod` releases them. Half 2: `restart_container` and the init deadline path stop then remove with `let _`;
the engine refuses `WorkloadRelease` (EBUSY) until the exit, the kubelet forgets the old cid, nothing retries,
and `remove_pod_sandbox` then waits on that record forever (pod deletion stuck too).
1. [x] Runtime-owned: `Container.removing` (asked at / last tried); a removed record is hidden (list, status,
       stats); `removals_due` = its workload's exit or ≥ 10 s since the last try, from `note_exits` (every
       absorb_exits); `remove_pod_sandbox` retries its sandbox's pending removals first.
2. [x] Test (1 new, no engine), CHANGELOG (1afb7e4). sc-build 1afb7e4 `cargo test --locked`: 340 kubelet unit,
       25 CNI, 30 proxy, integration and doc-tests pass. Not run on a node.
3. [x] Stage golden golden-rustkube-node-ddcdf8b8bb73 (8664370); stage filed no release request (commented on
       stormcentral#117); `stormcentral shipped`. #143's golden 65e3c3c shipped in 11.88 (stormcos#305).

### Done from this side: #143 (P1), pull_image binds its registry clone (stormblock#267)

2026-10-06. Found: `pull_image` mints (`POST /v1/clones`, state claimed), attaches and mounts, never binds; the
registry reaps a claimed clone after 900 s. sbregistry 0.26: `POST /v1/clones/{id}/bind {consumer}`,
`POST /v1/clones/{id}/release`, `GET /v1/clones?consumer=&state=` ("recovered by asking", #19). Nothing in the
kubelet unmounts/detaches a pulled image (no image GC; `remove_image` is a no-op), and a kubelet restart forgets
`pulled`, so the next pull mints a second clone: once clones are bound that would leak one per restart.
1. [x] Consumer `kubelet/<node>/<image>`. A pull first asks for a clone bound to it (found again after a restart:
       attach and mount are idempotent); else mints. After the mount, bind; a refused bind keeps the image
       usable and is retried on the next pull of it (warned). A failed lookup fails the pull (no duplicate mint).
2. [x] Tests (3 new, fake registry), docs (configuration.md, status.md), CHANGELOG (38bf04e). sc-build 38bf04e
       `cargo build --locked && cargo test --locked`: exit 0; 339 kubelet unit pass. Not run on a node.
3. [x] Release = an image GC (unmount, detach, release when no container uses it): filed as #161.

### Done: #62, slab capacity: publish, refuse, alert (#108 decided)

2026-10-05. Owner (#108): a claim's full class size counts at bind, overcommit ratio 1.0, clones keep the class
size. Engine: `/api/v1/slabs` items `role`, `total_bytes`, `free_bytes`; volumes `role`, `sealed`,
`virtual_size_bytes`. rustkube's scheduler (`capacity_fits`) reads `maximumVolumeSize` (else `capacity`) of a
CSIStorageCapacity matching the class and node topology, against the raw request.
1. [x] `capacity.rs`: `Capacity::of` (data slabs; committed = writable data volumes' virtual size, not
       sealed/goldens/class blanks/standbys), available = min(total×ratio − committed, free) − reserve,
       `largest_class`, `refusal`. Flags `--storage-overcommit` 1.0, `--storage-reserve-percent` 5,
       `--storage-alert-percent` 85 (`KubeletConfig.storage`).
2. [x] `capacity_loop`: CSIStorageCapacity `kube-system/stormblock-<node>` on engine volume changes + 60 s.
3. [x] Refusal in `provision_claim_volume` for a new volume, under `capacity_lock`; no slab API = unchecked.
4. [x] Gauges `kubelet_stormblock_data_bytes{kind}`, `_used_percent`; `SlabFilling` per crossing on PVs.
5. [x] Tests (5 new), docs, CHANGELOG (700483a, 1c5db42). sc-build 700483a `cargo build --locked && cargo
       test --locked`: 322 kubelet unit pass; `cd test && cargo test --locked && cargo build --release
       --locked` at 1c5db42: 17 pass. Medium `pvc-overcommit-refused` enabled. Not run on a node.
       stormcos#151 (storageCapacity: true) flips once every node runs this release.

### In progress: #88 (P1), a VMI on the pod network gets its own sandbox (stormvm#16)

2026-10-05. stormvm dc1b7ea (in the lock) has `realise(p, Some(netns), …)` (bridge binding: pod IP/MAC off the
CNI's interface onto `vmbr0` with the tap), `serve_dhcp(netns, lease)` and `Made.binding`. stormpump 30a76d3
joins a machine-domain spawn to a client-held sandbox (`inline_a`, `ns_fds`), as for a container.
1. [x] `vm_network.rs` (record, store, lease round trip, ClusterFirst DNS); `acquire_pod_network`
       (sandbox_acquire + CNI ADD `vm-<uid>`, recorded first; failed ADD → DEL; no CNI → Waiting) before the
       deposit window; `release_pod_network` (responders, DEL, sandbox_release, ESTALE ok, record).
2. [x] `resolve_nics(.., pod)` realises pod NICs in the netns, starts `serve_dhcp` per bridged NIC;
       `launch` spawns with the sandbox handle (`inline_a`).
3. [x] Status: `made.binding` (`binding_of` removed); pod NICs report the pod IP from the start, and
       `absorb_ends` does not replace it.
4. [x] Released at machine end, failed start, deletion; `adopt_registered` → `restore_pod_networks`.
       `Kubelet::with_engine(ring, cni)`; main passes an invoker on the same dirs.
5. [x] Tests (7 new), README, status.md, CHANGELOG (b177a89, the brace fix, 7558936). sc-build of the
       fix commit `cargo build --locked && cargo test --locked`: 317 kubelet unit pass. Not run on a node.
6. [x] **Owner (2026-10-05): B**, rustkube's VM controller creates a virt-launcher Pod, the kubelet adopts it;
       "can we have multiple vm in a namespace" (yes: a sandbox and a Pod each). Filed rustkube#203 (contract:
       labels `kubevirt.io: virt-launcher`, `kubevirt.io/created-by: <vmi uid>`, owner ref, nodeName set).
7. [x] Kubelet adopts it (3d9d26e, test 1 more, docs 523e978): launcher Pods kept out of the pod manager (and their VMI woken when one appears);
       the pod-network VMI waits for its launcher Pod; CNI ADD with the Pod's ns/name/uid (recorded); the Pod's
       status written (Running + podIP + Ready at start, Succeeded/Failed at end); a terminating launcher Pod
       with no machine here is confirmed deleted (grace 0, uid precondition).
8. [x] Tests (4 new), docs, CHANGELOG. sc-build `cargo build --locked && cargo test --locked`: 326 kubelet unit pass.
9. [x] rustkube#203 done (golden-rustkube-3397b0cb2d2d). #152 fixed (a318daf: launcher on this node; migration
       target launchers), golden-rustkube-node-cce2c30dd5c8. Kubelet side complete.
10. [ ] Live done-when = stormcos_qa#18 (5 pod-network VMs, same-namespace NetworkPolicy), behind
       stormcentral#55, the Fedora golden, stormcentral#376. `virtctl ssh` via port-forward needs #56. #88 proposed
       after stormcos_qa#18.

### Done from this side: #67 (P1), PVC ladder to PiB, raw block volumes, per-class filesystem

2026-10-05. Owner (#67, 09-25): large classes stay **ext4** unless stormcos#91 says otherwise; Block for single
objects past 16 TiB. stormcos#91's table: nothing forces XFS. Found:
- stormblock main pins mkfs-ext4 **v3.0.0**: format RAM ~8 KiB/group (16 TiB 1.3 GiB, 64 TiB 5.1 GiB, 256 TiB
  17.6 GiB, 1 PiB OOM; the fix, mkfs.ext4.rs#10, is untagged), and the default inode count wraps at 256 TiB
  (mkfs.ext4.rs#9, open: the filesystem is not clean). The fstemplate API exposes no inode ratio.
- stormblock `size` strings parse K/M/G/T only (no P): sizes are sent as `<MiB>M`.
- stormpump binds a non-directory source onto a file placeholder, so a raw device can be bound at a
  `volumeDevices[].devicePath` with no stormpump change (`fstype: None`, source = the device).
- rustkube's provisioner (`stormblock.rs`) writes the PV without `volumeMode` (admission → Filesystem), so a
  Block claim's control-plane PV mismatches: rustkube issue.
1. [x] storage.rs: `SIZE_CLASSES` (name, bytes, `ClassFs`); 4T, 16T ext4; 64T, 256T, 1P `BlockOnly` (reason
       names mkfs.ext4.rs#10/#9 and volumeMode: Block). Mint `fs` from the class, `size: <MiB>M`. d917f26.
2. [x] Block claims: raw volume (`POST /api/v1/volumes`, role data, class size), `ResolvedVolume.block`,
       `volumeDevices` bound at devicePath, `volume_mode_misuse` wait, PV `volumeMode` from the claim, `Pi`
       quantities. Third-party CSI Block was already refused (csi_volumes.rs).
3. [x] Tests: 9 new unit (ladder, fs per class, MiB sizes, raw volume end to end against one fake
       apiserver+stormblock, 20Ti filesystem refused, mode misuse, device mounts, Block PV, Pi). `test/` medium:
       4Ti/16Ti, Block 1Mi/20Ti/1Pi via volumeDevices, 20Ti filesystem refused, above-ladder 2Pi (312e9ff).
       Docs README, csi.md, status.md, CHANGELOG (04a6fdc).
4. [x] Filed rustkube#201 (provisioner volumeMode), stormblock#289 (carry mkfs.ext4.rs#9/#10).
5. [x] sc-build 1e65251 `cargo build --locked && cargo test --locked`: 310 kubelet unit pass, exit 0 in 150 s;
       `cd test && cargo test --locked` at 04a6fdc: 15 pass. Stage golden golden-rustkube-node-a0b9cebe68a0,
       release request stormcos#164; #67 closed, shipped. Follow-up #149 (flip 64T+ to ext4) proposed after
       stormblock#289. Not run on a node: medium Job waits on the test pipeline (#64), stormcos#92.

### Done: #119 (P3), metadata at scale (stormcos#54)

2026-10-05. Found: `instance_at` scans every VM and NIC under the `vms` lock and answers from the local record
even when the cached VMI is missing or places the machine elsewhere. Per-machine reconciliation is already the
UID executor's (`replace_source` enqueues only UIDs whose intent changed); `desired` is still replaced only by
the full LIST, and a cluster without the VMI CRD never leaves "cold".
1. [x] `Machines`: `vms` keeps an address → uids index, updated on insert/remove/update (only non-terminal
       machines; no `DerefMut`); `instance_at` answers from it, and refuses an address two machines here claim.
2. [x] `reconcile_one` refreshes that UID's cached object (removes it when gone); CRD 404 marks synced.
3. [x] `placed_here`: cached object exists, not terminating, uid matches, `status.nodeName` is this node, no
       completed (not failed) `migrationState` to another node. Covers address reuse. Not covered: a node
       partitioned from the apiserver answers from its last cache (no lease; noted in status.md). Bounded by #156.
4. [x] Tests (5 new), docs (README, api.md, status.md), CHANGELOG (3fe7496). sc-build 3fe7496 `cargo build
       --locked && cargo test --locked`: 301 kubelet unit, 30 proxy, 25 CNI, 1 doc-test pass; exit 0 in 147 s.
       Not run on a node.
5. [x] Stage golden golden-rustkube-node-f254f18e7c47 (ed55857), release request stormcos#164; #119 closed, shipped.

### Done: #148 (P1), pods wait for the CNI instead of retrying into backoff

2026-10-05. Dell 11.79: cilium agent `attempts=10` over 10.5 s, coredns 16 over 27 s; CNI → all pods ~20–27 s.
Found: the cilium agent is hostNetwork and never meets the CNI check; its attempts are its staged init containers
(each a Pending pass woken by the exit, no backoff). A network pod with no CNI config acquires a stormpump
sandbox, finds no conflist, releases it and retries on `wait_backoff` (a quarter of the wait, 1–10 s), so once
the config appears it starts up to 10 s later. A CNI ADD that fails (agent not serving yet) has the same backoff.
The executor's queue is apimachinery's FIFO with ≥32 workers: a network pod no longer holds a worker long
enough to delay the agent, so no reorder.
1. [x] stormpump runtime: no conflist → `CriError::NetworkNotConfigured`, checked before the sandbox is acquired.
2. [x] Pod manager: a pod waiting on the config is woken by the config (event wait, 10 s fallback); an ADD failure
       retries on a backoff from its first ADD failure, not from when the pod was seen.
3. [x] Kubelet: `--cni-conf-dir` watched (inotify, fs_watch); a change wakes only the pods waiting on the network.
4. [x] Tests, docs (README, configuration.md, status.md), CHANGELOG. f1a0c38; sc-build f1a0c38 `cargo build --locked &&
       cargo test --locked`: 296 kubelet unit (3 new), 4 integration, 25 CNI, 30 proxy pass. Not run on a node.
5. [x] Stage golden golden-rustkube-node-61af4712bf0d (409a838), release request stormcos#164; #148 closed,
       `stormcentral shipped`. Live check: coredns's start-timing `wait` ends ~1 s after Cilium's conflist.

### Done: #145 (P0), kube-proxy to a TLS apiserver with a token (stormcos#265, flowsdn edition)

2026-10-05. Found: kube-proxy builds `reqwest::Client::new()` (no CA, no token) and parses any answer as a list,
so a 401 reads as "no Services" and wipes every rule. Its rules are never entered: nothing jumps from
PREROUTING/OUTPUT to `KUBE-SERVICES` or from POSTROUTING to `KUBE-POSTROUTING`. A change is detected only by
counts, so a pod replaced at a new IP keeps the old DNAT.
1. [x] `--ca-file`/`--token-file` (`KUBE_PROXY_CA_FILE`/`KUBE_PROXY_TOKEN_FILE`), defaulting to the in-cluster
       ServiceAccount paths when present; token re-read each pass (projected tokens rotate). Non-2xx is an error.
2. [x] Jumps ensured (`-C`, else `-I`) for PREROUTING, OUTPUT → KUBE-SERVICES, POSTROUTING → KUBE-POSTROUTING.
3. [x] Apply when the generated rules differ from the last applied set, or 60 s after it (resync).
4. [x] `--cluster-cidr` (optional): ClusterIP traffic from outside it is marked for masquerade. Also found and
       fixed: service ports keyed without protocol (kube-dns 53/UDP vs 53/TCP collided), Endpoints matched by
       number not name, stale backends never cleared. 3243b77; sc-build eb0afdb: 30 proxy tests (13 new) pass.
5. [x] Tests, docs (configuration.md, README, status.md), CHANGELOG (cbfaec0). sc-build cbfaec0 `cargo build --locked
       && cargo test --locked`: 293 kubelet unit, 4 integration, 25 CNI, 30 proxy pass. Stage golden
       golden-rustkube-node-ed553922ee44, release request stormcos#164; node requirements answered on #145; closed,
       `stormcentral shipped`. Follow-up #147 (UDP conntrack cleanup). Not run on a node: live check is stormcos#265.

### Done from this side: #140 (P0), every new claim on the Dell (11.76): "stormblock would not clone pvc-ext4j-64m"

2026-10-03. C2NR0Q2 (Dell R230, 11.76: this repo at ac07686, stormblock v20.0.0); pvetest1 fine with the same
release. The node's stormblock needs its token, so the refusal cannot be read from here. Found: `storage_post`
answers `None` for a transport error (60 s request bound, no log at all) and for a 2xx that is not JSON, and the
claim message never carries stormblock's answer. stormblock refuses a clone of a `ready` template whose sealed
volume is missing (404 "volume … not found"), not sealed (409) or absent (500 "has no sealed snapshot"); nothing
here rebuilds such a template, so every retry meets the same refusal.
1. [x] Engine POSTs (template clone, attach, snapshot clone) answer `Result` with stormblock's status and body, or
       the transport error (timeout named); the claim's waiting message and FailedMount carry it.
2. [x] A clone refused because the template itself is broken: DELETE the template and mint it again (one mkfs);
       the claim waits on the mint. A template still formatting, or a refusal about the clone, is not "broken".
3. [x] Tests (fake stormblock), docs (README, status.md), CHANGELOG (e3ca68d, de10176). sc-build de10176
       `cargo build --locked && cargo test --locked`: 293 kubelet unit (4 new) and the other suites pass; exit 0
       in 82 s. Not run on a node.
4. [x] Stage golden golden-rustkube-node-945e83c78007 (c248063), release request stormcos#164; `stormcentral
       shipped`. Live check after the release: the Dell's claim heals (broken template) or its FailedMount names
       stormblock's refusal; a refusal that is stormblock's goes to stormblock as its own issue.

### Done: #138, starts queue under a burst (wait p50 2.4 s, max 9.3 s over 173 pods)

2026-10-03. pvetest1 (11.72), stormcos_qa turbomode. Found: every Pod/VMI pass runs on one executor with a
fixed 8 workers (`workloads.run(self, 8)`). A start pass holds its worker through the sandbox (~200 ms), the
status PUT and the start-timing annotation PATCH; each pod also gets a second pass 1 s later (its probes), with
another status PUT. 173 pods in ~9.3 s is ~18/s, i.e. ~430 ms per pass across 8 workers: the pool is the
queue. The ring (non-payload requests pipelined), CNI (no lock) and image pool are not.
1. [x] Worker pool: `--pod-workers` / `POD_WORKERS`, default 16 × CPUs clamped to [32, 256].
2. [x] The start-timing annotation PATCH off the worker (spawned, like the queued Events).
3. [x] Annotation: `workers=<busy>/<limit>` and `pending=<pods seen here, not yet started>` at the attempt's begin.
4. [x] The pass's "any adopted pod without its spec" guard reads under the lock instead of cloning every PodState.
5. [x] Tests, docs (README, configuration.md, status.md), CHANGELOG (20e95fc, b5f21c5). sc-build 20e95fc
       `cargo build --locked && cargo test --locked`: 289 kubelet unit (2 new), 4 integration, 25 CNI, 17 proxy,
       1 doc-test pass (1 ignored); exit 0 in 122 s. Not run on a node.
6. [x] Stage golden golden-rustkube-node-317e2624c20e (ac07686), release request stormcos#164; #138 closed.
       Live check after the release: turbomode on pvetest1, 50 pods,
       wait p90 < 100 ms, total p90 < 500 ms; `workers=` says whether the pool is still the limit.

### Done: #137 (P1), a finished Pod keeps its pod IP (Cilium range full at ~250)

2026-10-03. pvetest1 (11.72): 1,000 `restartPolicy: Never` sleep pods, 250 reach Succeeded, the rest wait on
`range is full`. Found: nothing stops a terminal Pod's sandbox until the Pod object is deleted (the sync skips
Succeeded/Failed pods), and the stormpump runtime's `stop_pod_sandbox` only flipped a state: CNI DEL ran in
`remove_pod_sandbox`, which waits for every container record to go.
1. [x] stormpump `stop_pod_sandbox`: CNI DEL (an error keeps it Ready, retried) and the netns holder released,
       idempotent; `remove_pod_sandbox` does not DEL again.
2. [x] Pod manager: the pass that makes a pod Succeeded/Failed stops its sandbox (containers, status, logs kept);
       a failed stop is retried on RECHECK from the terminal-skip branch (`PodState.sandbox_stopped`).
3. [x] Tests, docs (README, status.md), CHANGELOG (227fbfe, e9bbcb9). sc-build 227fbfe `cargo build --locked &&
       cargo test --locked`: 287 kubelet unit (2 new), 4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored);
       exit 0 in 123 s. Not run on a node.
4. [x] Stage golden golden-rustkube-node-9c5a14b04400 (3e4446b), release request stormcos#164; #137 closed.
       Live check after the release: stormcos_qa turbomode on pvetest1 reaches 1,000 Succeeded.

### Done: #134 (P1), every start attempts=2; report outlier; serial work in sandbox/containers

2026-10-02. pvetest1 (11.71): five busybox pods, all `attempts=2`, one `report=729ms`.
Found: `prepare_images` spawns the image resolution and reads its result in the same call, before the
task has run, so attempt 1 always returns `Pending("waiting for image …")` (a golden resolves in 0.1 ms) and
the pod writes a ContainerCreating status PUT before the real one. `report` is exactly one status PUT (no GET,
no tick): 729 ms is the apiserver's write. In `containers`, each container awaits three Event POSTs
(Pulled, Created, Started) to the apiserver, serially, inside the timed step.
1. [x] Image grace: a just-asked image is waited for up to 100 ms (subscribed before the spawn), so a local
       image starts on attempt 1 with no Pending write; a real pull still yields the worker.
2. [x] Events: Normal lifecycle Events on the start path go through one ordered background sender
       (timestamps taken when recorded); Warnings stay inline.
3. [x] Tests, docs (README, status.md), CHANGELOG (5f6af4f, 23d33f3). sc-build 5f6af4f `cargo build --locked &&
       cargo test --locked`: 285 kubelet unit (2 new), 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0.
       Not run on a node. PUT outlier filed as rustkube#191. Remaining sandbox/containers time is stormpump work.
4. [x] Stage golden golden-rustkube-node-31c042c9e59a (1d2d711), release request stormcos#164; #134 closed.
       Live check after the release: five busybox pods on pvetest1 read `attempts=1`.

### Done: #132 (P1), per-pod start timing (annotation, Event, histograms, one INFO line)

2026-10-02. Owner: "Why 1 second? What's holding us up?" Nothing times the phases (#95 item 1).
One `StartTiming` per pod UID, kept across Pending retries, published once the apiserver acknowledges Running.
Phases: `scheduled` (PodScheduled/creation → seen, wall clock), `wait` (seen → the attempt that started it),
`image` (images asked for → resolved; a pull is the registry clone + attach + mount), `volumes` (+ each volume),
`sandbox` (incl. CNI), `init`, `containers` (+ each create+start), `report` (status PUT sent → acknowledged), `total`.
1. [x] `start_timing.rs`; seen noted when the pod list/watch delivers it; start_pod times its steps.
2. [x] Publish (8009bd4): `storm.io/start-timing` annotation (merge patch), `StartTiming` Event, histogram
       `kubelet_pod_start_phase_duration_seconds{phase}`, one INFO log line.
3. [x] Tests, docs (README, metrics.md, status.md), CHANGELOG. sc-build f10e8f3 `cargo build --locked && cargo test --locked`:
       283 kubelet unit (6 new), 4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0 in 106 s. Not run on a node.
4. [x] Stage golden golden-rustkube-node-70fe65e86c27 (59f0a05), release request stormcos#164; #132 closed.
       Live check after the release: a pod on a blade shows the annotation; stormconsole#69 and stormcentral#301 read it.

### Done: #129 (P0), after a reboot every new pod fails: its own dirs "do not exist"

2026-10-02. Found on server3 (11.65, storm-8486f0) from the live kubelet log: not a namespace mismatch. The
filesystem under `/var/lib/kubelet` and `/var/log/pods` is full after the reboot (ephemeral-storage capacity
61396Ki, DiskPressure True since 22:52:35, `could not create container log dir …: No space left on device`).
The kubelet discarded the emptyDir/projected/SA `create_dir_all` errors (so ENOSPC read as "does not exist") and
the log-dir ENOSPC was a generic start error, so the pod went `Failed` and the DaemonSet burned its backoff.
1. [x] Per-pod dirs (emptyDir, configMap, secret, projected, SA token, resolv.conf) and the container log dirs
       created before the sandbox; a failure is a wait (Pending, FailedMount / Failed Event naming the errno),
       retried, never `Failed`. a72d4a1.
2. [x] Tests, docs (README, status.md), CHANGELOG. sc-build a72d4a1 `cargo build --locked && cargo test --locked`:
       277 kubelet unit, 4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0 in 97 s. Not run on a node.
3. [x] Full filesystem filed as stormcos#231 (P0): server3 runs no new pod until the host has room.
4. [x] Stage golden golden-rustkube-node-cb302b196e29 (644d4ca), release request stormcos#164; #129 closed.
       Live check after stormcos#231 and the release: the issue's SNO hard-power-off test on server3.

### Done: #103 (P0), a pulled non-golden image cannot be resolved by create_container

2026-10-02. The pod manager passes `create_container` the reference `pull_image` returned: for a pull that is the
mount `/run/stormpump/images/<volume>`. `create_container` and `spec_for` resolved it with `local_path`, which maps
the last path component to `/pallets/<name>`, so every pulled image failed at start "was never pulled" (C2NR0Q2,
11.56, stormcos_qa test Jobs).
1. [x] `image_root`: a path one volume below `/run/stormpump/images/` (what a pull returns) is the root as given
       (not checked from here: PID 1 mounted it in the node's namespace); otherwise `local_path` as before. Used by
       `create_container` and `spec_for`. 7d28509.
2. [x] Tests: the live repro's volume resolves to its mount, repeatably; malformed paths refused; goldens by ref and
       by pallet path. create/start themselves need an engine (not unit-testable here).
3. [x] Docs (configuration.md, status.md), CHANGELOG. sc-build 4263d80 `cargo build --locked && cargo test --locked`:
       274 kubelet unit, 4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0 in 94 s. The first run
       hit an ETXTBSY CNI test race, filed by sc-build as #128 and fixed in 4263d80 (retry, as libcni).
4. [x] Stage golden golden-rustkube-node-b064bfdcc9fb (4263d80), release request stormcos#164. Live check after the
       release: `stormcentral test run stormcos_qa short` reaches the pod log.

### Done: #124, `logs` on a stormpump:// mirror pod answers "not found on this node"

2026-10-02. Found: 11.61 (server1, stormcos#217) carries rustkube-node d5c0d2c (v0.12.0, base 11.50), which
predates #72's stormd-volume path, so every mirror pod answered "not found". On main, #72 serves the stormd
volume, but a service that dies before stormd writes it (stormcluster, stormrdp on 11.61), `--previous` with no
`.failed.log`, and a service not run by stormd still had nothing. stormpump (#51, in 11.61's 95dbea4) records
the dead incarnation's last 20 lines of its `w<id>.log` as `last_output` in assets.json.
1. [x] `node_logs::Record` from assets.json (`last_output`, `last_exit`, `last_error`); the mirror path resolves
       the asset when assets.json lists it or a boot unit gives it a stormd volume.
2. [x] Current: stormd volume; else, asset not running, stormpump's `last_output`. Previous: newest `.failed.log`,
       else `last_output`. Nothing: 404/400 naming what was looked at (and the exit/refusal), never "not found".
3. [x] Tests, docs (README, api.md, status.md), CHANGELOG. fc867c1; sc-build `cargo build --locked && cargo test --locked`:
       272 kubelet unit, 4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0 in 89 s. Not run on a node.
4. [x] Stage golden golden-rustkube-node-58e64be5aba3 (46ee39f), release request stormcos#164; #124 closed.
       The live check is the next release on server1 (stormcos#217).

### Done: documentation refresh from code since 2026-09-25 (#54, #120, #121)

2026-10-02, on main at fecb331.
1. [x] Audit README, docs/, CLAUDE.md against code and `git log --since=2026-09-25`.
2. [x] status.md: changes since 09-25, decided-not-implemented table (#106–#110), new gap rows
   (#115, #116, #118, #119, #122, stormimds#12). README, api.md, configuration.md, BUILD.md,
   csi.md (inotify registry, 1 s retry), node-volumes.md updated. :5100 is sbregistry (#120).
3. [x] Module comments (#54): cmd/kubelet main.rs, storage.rs (PID 1 mounts, container binds),
   server.rs `/vmInstance` (stormimds#12 undecided).
4. [x] CHANGELOG; pushed 13517fb; sc-build `cargo build --locked && cargo test --locked` exit 0 in 91 s
   (270 kubelet unit pass). Comments on #54/#120/#121; #106–#109 closed with pointers. Docs/comments only: no golden.

### Done: #117, a golden still importing (409 not sealed) waits, not a failed start

2026-09-30, on main. `resolve_disks` treated only a 404 from `POST /volumes/{g}/clone` as Waiting;
stormblock's `409 … is not sealed` (vmimages still importing) counted as failed starts with backoff.
1. [x] `golden_wait(e)`: 404 → "waiting for golden {g}", 409 + "not sealed" → "… (importing)"; unit test. c74b589.
2. [x] README, CHANGELOG. sc-build c74b589 `cargo build --locked && cargo test --locked`: 270 kubelet unit,
   4 integration, 25 CNI, 17 proxy, 1 doc-test pass (1 ignored); exit 0 in 106 s. Not run on a node.
3. [x] #117 closed with evidence. Stage golden golden-rustkube-node-e5db6ac32831 (9c2f738), release request stormcos#164.

### Done: #114, turbomode merged into main

2026-09-29: owner instruction in #114 supersedes the earlier no-merge hold.
Preserve main's #91 address pump, #35 failed-list protection, #53 snapshots
and #75 VM disk ownership in the UID worker design. Open #100/#101/#102
work continues on main afterwards; no golden or release is requested here.

1. [x] Read #114, #100, project rules and open issues; checkout is clean.
2. [x] Merge origin/main into turbomode; preserve address pump, failed-list
   protection, snapshot maintenance, durable disks, retry policy and credentials.
3. [x] ec39c2a pushed; full sc-build `cargo build --locked && cargo test --locked`
   passed: 251 kubelet unit, four integration, 24 CNI, 17 proxy and one doc-test;
   one doc-test ignored. Remote exit 0 in 107s; drive deleted. Local statistics
   append was read-only (remote result unaffected).
4. [x] Main merge 600b58a pushed (--no-ff); full sc-build with the same command
   passed in 79s, remote exit 0, identical test counts, drive deleted. The local
   statistics append again reported read-only; no remote build/test failure.
5. [x] Posted merge/test evidence and remaining #100/#101/#102 work on #114;
   issue closed. No golden, release or live deployment requested. Continue
   unfinished work on main; live target remains #110. This final checkpoint
   changes documentation only after the verified main merge.


### Parked on #102: #99, event-driven node and subsecond warm startup

2026-09-29, on main. #99 is the umbrella; #100/#101 are done, rustkube#143,
#145, #146 and #148 are closed, and rustkube#144/#147/#149 are open there.
Its acceptance measures real nodes (C2NR0Q2, owner's choice on #110), which
waits on the master installing the release (stormcos#164) = #102.
1. [x] Ring: `DEADLINE` from enqueue; abandoned requests keep the arena until
   they complete; a late success is undone. db9b783.
2. [x] CNI: plugin exec bounded (60 s), killed and reaped on timeout. db9b783.
3. [x] EngineClient: 5 s connect, 60 s request, 1 h mint, unbounded watch.
   A timed-out mint is found again by name (stormblock refuses duplicates).
4. [x] sc-build db9b783: 269 kubelet unit, 4 integration, 25 CNI, 17 proxy,
   1 doc-test pass (1 ignored). Acceptance table in docs/event-driven-design.md.
5. [ ] Real-node measurements = #102; proposed #99 after it (moved behind #102).
       #102 (2026-10-05): pvetest1 day turbomode run 7277704177: sleep request→running p50 4.05 s, p95 61.1 s,
       of which scheduling p95 60.86 s (filed rustkube#205); PVC p95 10.85 s. C2NR0Q2 blocked by registry 507
       (stormcentral#376). Owner (#158): **C**, the day suite on the Dell is the acceptance; full scale scheduled
       separately at night on a pve VM (reported, not gating). #102 proposed after stormcentral#376.
       2026-10-07: #376 closed, 11.88 has the 4 GiB registry; the Dell is down (stormcos#337), pve VMs missing.
       #102 proposed after stormcos#337; then `stormcentral test run stormcos_qa turbomode --tag C2NR0Q2`.
       2026-10-08: stormcos#337 closed (11.89). Queued turbomode 630379c31b (stormcos_qa b9ec446519df, C2NR0Q2),
       behind short 7f911d561b (at "wait for the node to settle", media import: stormblock-registry#95 may hold it).
       vm-waves 845557b202 queued too (#91/#92's check). Read with `stormcentral test show <id>`; compare with the
       11.50/11.51 baseline (container start 20.8/21.2 s, claim 75.5/67.2 s) and post on #102.
       630379c31b: error at the test-image build on dev.g8.lo (retired; stormcentral#521), after the node settled.
       #102 proposed after stormcentral#521; rerun then. #521 closed before the build-VM stormcentral installed (plain
       sc-build still went to dev at ~13:45Z): #102 and #97 re-proposed after stormcentral#526 (test-image builds),
       #165 after stormcentral#527 (component stage).
   Stage golden golden-rustkube-node-e8bca700a793 (bba7d54), release request stormcos#164.

### Done: #101, events and explicit deadlines instead of sync ticks

2026-09-29, on main. Heartbeat is the only fixed schedule left.
1. [x] Pod adapter deadlines (probe periods, CrashLoopBackOff, waiting retry,
   init limit); AwaitEvent otherwise; CRI runtime without exits keeps
   `sync_interval`, counted (`kubelet_timed_reconciles_total`). 4320c20.
2. [x] VMI deadlines: start backoff, waiting retry, guest-agent poll. 4320c20.
3. [x] Service mirror: inotify on /run/stormpump gated on the parsed table
   (≤1 read/s while PID 1 rewrites every pass: filed stormpump#67) + mirror-pod
   watch; skip-when-current writes. 08fc437.
4. [x] stormblock volume watch (`follow_volumes`, 30 s counted poll without it)
   → system claims + disk-owner sweep; PV/PVC and VM/VMI watches. 82f00c3.
5. [x] Reclaim on PV watch (5 s pending retry); CSI sweep on node Pod watch
   (10 s pending retry); snapshot take completions notify. 82f00c3.
6. [x] Tests, docs, CHANGELOG. sc-build fbb9676 `cargo build --locked && cargo
   test --locked`: 266 kubelet unit, 4 integration, 24 CNI, 17 proxy, 1 doc-test
   pass (1 ignored). Not measured on a node (#102). Follow-up: #115 (route an
   exit to its own UID instead of waking every workload).
7. [x] Stage golden golden-rustkube-node-2f39a07e3c07 (4b648f4), release request stormcos#164.

### Done: #100 on main, VM partial-start unwind (stormpump#63 closed)

2026-09-29: master moved #100 to main after the #114 merge; stormpump#63's
`DEPOSIT_WITHDRAW` (op 9) is on stormpump main. The lock's stormpump (30a76d3)
predates the enum variant and cannot move until stormvm#65, so the op is sent by
number (`stormpump_ring::OP_DEPOSIT_WITHDRAW`).
1. [x] `RingClient::deposit_withdraw` (op 9) and `spec_release`.
2. [x] VmManager ledger by uid (deposit noted before sending, handles as
   registered, cleared inside the spawn closure on success). Unwound in
   `launch`'s window after a failure, before the next start (Waiting while a
   deposit is held) and before a deletion is acknowledged. Found: stormvm names
   deposits `tap-<nic>` per client, so concurrent starts could swap taps; one
   kubelet-wide deposit window now covers deposit→spawn and every withdraw.
3. [x] Tests (injected undo), docs, CHANGELOG. sc-build 6c49fbc: 255 kubelet unit pass.
4. [x] CNI: DEL after a failed ADD before the sandbox is released, retained and
   retried if DEL fails. Reclaim handler runs the release in its own task.
   sc-build 077ad5c `cargo build --locked && cargo test --locked`: 257 kubelet
   unit, 4 integration, 24 CNI, 17 proxy, 1 doc-test pass (1 ignored). Not run
   on a node (live validation is #102/#110).
5. [x] Issue closed. Stage golden golden-rustkube-node-2fb5a1e7ab0d (ac738df),
   release request stormcos#164.

### Done (history): #100, common bounded per-UID Pod/VMI workers

Earlier checkpoint: #100 continued at turbomode 9b46886 (211 tests passed).
#114 now authorizes merging; unfinished cancellation work continues on main.

1. [x] Wire Pod/VMI adapters to one eight-worker executor with name/claim
   reservations; seed adopted workloads before admission. Static manifests have
   an independent desired source, so unavailable API reads do not remove them.
2. [x] Stage image pulls (four-slot shared pull pool), init waits and VM shutdown
   grace periods. Retain partial Pod starts, refused runtime releases and CSI
   teardown records. Serialize CSI mutations by driver/handle. Reject unknown
   runtime recovery and VM exit as proof that cleanup is safe.
3. [x] Add inverse claim/image/driver indexes and PV/attachment-to-claim routing;
   UID guards on Pod deletion, VM status/finalizers, migration writes and PV deletion.
4. [x] Push incremental checkpoints and run sc-build. af1704b passed 223 unit +
   four integration tests. e504f58 full workspace build passed; test compilation
   found a missing json macro import. Fixed in 55d458c: full sc-build
   `cargo build --locked && cargo test --locked` passed (224 kubelet unit,
   four integration, 24 CNI, 17 proxy; one doc-test passed, one ignored).
5. [ ] Finish cancellation at every side-effect boundary. External blocker:
   stormpump#63 has no acknowledged withdrawal for deposited VM tap FDs before
   spawn. Partial NIC/plan/registration failures can keep a tap alive in the
   shared engine connection and block same-name recreation. #100 moved behind
   that issue with stormcentral propose; do not close or release it yet.
   Resume: wire deposit withdrawal and retain VM partial-start ownership (disk,
   volume/spec handles, registration, NIC deposits) until each cleanup succeeds;
   add real adapter cancellation/failure-injection coverage. Audit CNI partial
   sandbox failures and cancellation of the claim-reclaim HTTP handler too.
   Existing tests cover shared admission, slow-image/fast-start, staged init
   resume/delete, failed teardown, same-name replacement and dirty work during
   an active operation; they do not establish every-boundary cancellation yet.
6. [x] Main's #91/#35/#53/#75 behavior is integrated under #114. Live target
   remains the owner's decision under #102/#110. Version stays 0.13.0: this
   merges unfinished work, without cutting a feature release or golden.


### Done: documentation refresh from code (#54), 2026-09-29

Scope: current main at 5bb1a38 and `git log --since=2026-09-18`.
Work on docs/code-refresh-20260929; preserve turbomode separately, with no merge
or golden from that experimental branch.

1. [x] Audit README, docs and current configuration/API/runtime code; distinguish
   implementation from plans and live acceptance.
2. [x] Correct shipping instructions, add complete CLI/default/port references,
   and link every unsupported promise to its owning issue (file missing ones).
3. [x] Update CHANGELOG; review for secrets; commit and push the documentation.
   New P2 follow-ups: #111 restartable init sidecars, #112 backoff persistence,
   #113 explicit default-valued apiserver precedence. Existing gaps are linked
   in docs/status.md; #3 now records the no-cni help/behavior mismatch.
4. [x] ad43ee9 passed remote sc-build `cargo build --locked && cargo test --locked`:
   227 kubelet unit tests, four integration tests and the CNI/proxy suites passed;
   one doc-test ignored. Remote exit 0 in 74 seconds; scratch drive deleted.
   The local runs.jsonl append failed (read-only filesystem); remote verification
   completed. All 26 CLI flags, 11 routes and local doc links were checked.
5. [x] Documentation only: no version bump or runtime change. Fast-forwarded
   onto main at 9c1fe94; turbomode preserved. Requested the standard stage once.
6. [ ] Artifact follow-up only: stage 131edf1ad026 failed before compilation
   fetching private stormcos be718e09b5f2 (GitHub username unavailable).
   Added evidence to stormcentral#161 and proposed #54 after it (proposal
   663d4471d6a0 awaits approval). No golden or
   release request was produced. Documentation itself is published and verified;
   resume the stage request after the platform authentication fix.



### Done: #35, VMs reconciled against VMIs: deletion stops them, orphans found

Found, 2026-09-27 (test2's QEMU outlived its VMI on C2NR0Q2): a VM outlives a kubelet restart (the engine
supervises it; the token reclaims ownership), but its handle lived only in `vms`, in memory. A restarted
kubelet knew nothing of it, so deleting its VMI stopped nothing. Also: a failed VMI list read as "no VMIs" and
stopped every VM (and deleted its root); a terminating VMI (deletionTimestamp) was still desired.
test1 not restarted is the VM controller's: rustkube#104.

Steps:
1. [x] A failed list is not an empty one: `list_for_node` → Option, the watch's LIST checks status; no sync on failure.
2. [x] The registration (`/run/stormvm/<ns>/<name>/vm.json`) is the record: after the spawn it is rewritten with
       `running_as(handle)` and the disks (volume ids, owned).
3. [x] `reconcile_registered` each sync: running+wanted → adopt; running+unwanted → stop; gone+wanted → Failed
       record (not restarted behind the VM controller); gone+unwanted → leftovers released. Handle-less:
       liveness is a connect to the control socket; stop is ACPI, 30 s, then QMP `quit` / chv `vmm.shutdown`.
4. [x] A terminating VMI is not desired. Finalizer `storm.io/vm` ensured each sync while running (rv-guarded),
       removed once stopped.
5. [x] Tests, docs (README), CHANGELOG. sc-build at 2331af9: all pass (kubelet 189). Not run on a node.
6. [x] Released v0.13.0 (e8211b6). Release sc-build at cf6aee2 (`--release --locked --target
       x86_64-unknown-linux-musl`, then `cargo test --locked`): passed (kubelet 189). Stage golden
       golden-rustkube-node-d9a108728be0 (at ccb7bfc; the first stage try died on dev: no NVMe device). No
       release request was filed by stage: stormcentral#117. #35 closed (sc-build only, not on a node).

### Done: #75, a VM's disks outlive its VMI: restart does not re-clone

Found, 2026-09-29: `stop` → `destroy_owned` deleted every golden clone, seed and emptyDisk, and the golden clone was
unconditional, so each VirtualMachine restart (a new VMI) re-cloned root. stormblock#115 (closed) gives a volume an
`owner {kind, namespace, name, uid}`, set on create/clone or `PUT /volumes/{id}/owner`, returned by the listing.
The VMI's ownerReferences carry its VirtualMachine's uid (rustkube virtualmachine.rs).

Steps:
1. [x] Find before create: a golden disk or emptyDisk named `volume_name(vm, disk)` is reused, cloned/created only
       when missing (a failed listing is a failed start, never "make a new one"). Owner on each: the VMI's
       VirtualMachine (kind, ns, name, uid), else the VMI itself. A found disk owned by an earlier object of the
       same name (other uid) waits for the sweep. Seed: namespaced name, the old one replaced each start.
2. [x] Stop detaches only. `destroy_owned` gone.
3. [x] Orphan sweep (≤ once a minute): engine volumes owned by a VirtualMachine/VMI, not in use and not held by a
       machine here, whose owner is 404 / another uid / being deleted → deleted. Any other answer keeps them.
4. [x] `storm.io/retain-disks: "true"` (VMI or VM annotation): disks get no owner, never swept. Owners set on the
       disks of adopted machines too.
5. [x] Tests, docs (README), CHANGELOG. sc-build at 9149ea1: all pass (kubelet 227). Not run on a node
       (C2NR0Q2). Unreleased. Closed.

### Historical checkpoint: #100 before #114 superseded the merge hold

At this checkpoint work was on `turbomode`; #114 supersedes that hold. Read rustkube's `docs/turbomode-handoff.md` (turbomode branch).
2026-09-29: handoff step 2 done, `sc-build 'cargo test --locked -p kubelet'` at turbomode 9b46886: 211 pass.
Merging origin/main conflicts in `kubelet.rs` (turbomode's `pod_loop`/`vm_loop` vs main's #91 address pump,
#35 `watch_for_node`/`list_for_node`, #53 `snapshots.sync()`): aborted, owner asked on #100 (resolve, or leave).

### Done from this side: #53 snapshot + restore (option A, #109), VirtualMachineSnapshot / VirtualMachineRestore

2026-09-30: owner chose A (a restore rewrites the VirtualMachine's disks to the restored PVCs).
The text below predates the answer.

2026-09-29 recheck: read #53 and #109, including comments. Snapshot support is
already on main; #109 remains unanswered and labeled needs-owner. Stop restore
implementation until the owner chooses disk references and node placement.
Propose #53 after #109; then resume step 3 below, update docs/tests, push and
verify with sc-build before completion. This recheck changes documentation only;
no new build, live validation, release or issue closure is claimed.

stormvm (in the dc1b7ea lock): `stormvm_spec::snapshot::{snapshot_request, restore_request, snapshot_status,
restore_status}`, `stormvm_console::snapshot::take(reg, stormblock, name, Options)` (freeze → pause → one /v1
group snapshot, named `<ns>.<vm>.<snap>`, idempotent by name → unpause → thaw). stormblock#130 is closed.
The CRDs are stormcos#170 (open): until they are installed the LIST 404s and the kubelet does nothing.
Found: stormblock on stormcos is single-node (no cluster), so a group snapshot and any volume restored from it
live only in the engine of the node that took it. The VMI shape has no raw-volume disk (only dataVolume,
containerDisk → golden, PVC, cloudInit, emptyDisk), so "rewire the disks" needs a decision: owner.

Steps:
1. [x] `vm_snapshot.rs`, each tick after the VMIs: list snapshots (404 → CRD absent, nothing). One whose source
       is registered here (`/run/stormvm/<ns>/<vm>`) and has no phase: claim it (annotation
       `storm.io/snapshot-node`, rv-guarded), InProgress, `take` in the background, then Succeeded/Failed +
       group id, sourceUID, indications, Event. failureDeadline (default 5 min) from creationTimestamp. An
       InProgress one of ours not in flight (kubelet restart) is taken again: idempotent by name.
2. [x] Tests (fake apiserver, injected take), docs (README), CHANGELOG. sc-build at 2b88465: all pass (kubelet 221).
       Not run on a node (CRDs: stormcos#170; needs a running VM). Unreleased.
3. [x] Restore: owner chose **A** (#109): the restore rewrites the VirtualMachine's disks to restored PVCs; the
       whole disk set together; seed regenerated only when the VM has cloud-init (it is, each start, #75);
       placement follows the PVs' nodeAffinity.
4. [x] (2026-10-05) The claim records `storm.io/snapshot-disks` (disk → volume id, `disk_map(reg)`).
5. [x] `vm_restore.rs`: by the snapshot's node; waits (snapshot Succeeded, VM stopped); per non-cloud-init disk
       `from_snapshot` + bound Block PVC/PV (Delete, this node); VM template volume → PVC; status + Events;
       errors said once. `RestoreEngine` seam (`Stormblock` = stormvm_block::Client). 5c4edf2 (+ fix for #153).
6. [x] Tests (5 new), docs (README "Restores", api.md, status.md), CHANGELOG. sc-build `cargo build --locked
       && cargo test --locked`: 331 kubelet unit pass. Not run on a node: the CRDs are stormcos#170.

### Done: #83, the console router is told where stormblock is (the snapshot verb)

stormvm's `Config.stormblock` was left `None` in both mounts of `stormvm_console::router`, so its `snapshot`
verb answered 409 on every node. stormvm_block::Client reads the engine token itself (`Token::from_env`, same
order as engine.rs). The verbs are not routed onto :10250 yet: that is #94 (snapshot may now go with them).

Steps:
1. [x] `server.rs`: one `console(run_dir, stormblock)` builder; `ServerConfig.stormblock_url` from the kubelet's
       engine URL (`--stormblock`); `router()` (tests) uses `engine::DEFAULT_URL`. Test: snapshot gets past the
       409 to the hypervisor.
2. [x] Docs (README), CHANGELOG. sc-build at 09cc138: all pass (kubelet 211). Closed. Not run on a node. Unreleased.

### Done: #76, a failed VM start is retried with backoff, not recorded Failed for good

Owner: "Nothing should be perm." rustkube#104 (v0.16.0) makes the VM controller recreate a *Failed* VMI under
Always / RerunOnFailure / running:true and leave it under Once / Manual.

Found, 2026-09-28: every start clones a new root from its golden and makes a new seed, and a failed start
only detaches (`release`), so each retry would leak two volumes.

Steps:
1. [x] A failed start deletes the volumes it created in that attempt (golden clones, seeds; never a reused
       emptyDisk), in `resolve_disks` and after it.
2. [x] `StartFail::Failed`: retry with backoff 10 s doubling to 5 min, keyed on uid + generation (a spec change
       retries at once). Pending with the reason + attempt + next try, Warning Event each attempt.
       Failed for good only when the owning VM's runStrategy is Once or Manual. Standalone VMI: retried.
3. [x] Tests, docs (README), CHANGELOG. sc-build at 20036bf: all pass (kubelet 210). Not run on a node. Unreleased.

### Waiting on stormblock-registry#95 / stormcentral#521 / stormcos_qa#52: #92, a VMI's accessCredentials (keys into the seed and through the agent)

stormvm e5b4d16 (in the dc1b7ea lock): `VmSpec.access_credentials`, `access::keys_in_secret`, `Seed.public_keys`,
`qga::set_authorized_keys` (reset: true), `access::condition`.

Steps:
1. [x] Start: `noCloud` Secrets read before the disks, keys into the seed's meta-data `public-keys`. A missing
       Secret, an empty one, or no cloudInitNoCloud volume: start anyway, reason in the condition.
2. [x] Each sync, from the VMI as it is now: agent credentials applied once the agent answers, re-sent only
       when the Secret's keys differ from what the agent last accepted. A missing/empty Secret leaves the
       guest's keys alone (no lock-out). `Vm.access` holds it.
3. [x] `AccessCredentialsSynchronized` merged into the VMI's existing conditions; transition time held while
       the status holds.
4. [x] Tests, docs (README), CHANGELOG. sc-build at 1f5409a: build clean, all pass (kubelet 207).
5. [ ] The issue's done-when on the test host: console-created VM, ssh in with the key; "Add my keys" live.
       2026-10-05: half 1 is stormcos_qa `vm-waves` (noCloud key, ssh); half 2 filed as stormcos_qa#52. Runs on
       C2NR0Q2 get 507 on the image push (stormcentral#376); #92 proposed after it. Then run vm-waves, close.
       2026-10-07: #376 closed; vm-waves 18d8dc0653 died on the Dell's apiserver (stormcos#337). #92 proposed after
       stormcos#337; half 2 still needs stormcos_qa#52.
       2026-10-08: stormcos#337 closed; vm-waves still can't run (test images: stormcentral#521; fresh-node media
       import: stormblock-registry#95). Tests pass on a build VM (fb94c9a). #92 proposed after stormblock-registry#95.

### Done: #55, a presentation of rustkube-node (`docs/presentation.md`, Marp)

2026-10-05. 8–15 slides from current code/docs: purpose; place in stormcos (stormcentral: depends on rustkube,
stormpump, stormvm; depended on by stormcos, flowsdn); moving parts (diagram); features today (Pods, storage,
VMs, node services); interfaces; shipping; planned (own slide); status and open issues. Check claims by grep;
render with marp-cli via sc-build if dev can.
Done: docs/presentation.md, 12 slides, ASCII diagrams (Marp shows Mermaid as source). sc-build `npx
@marp-team/marp-cli docs/presentation.md -o …html`: renders, 12 sections. PDF needs a browser dev lacks (#154).

### Done: #122, host-network metadata by ServiceAccount token; pod-bound tokens

2026-10-05. rustkube#182 (in golden-rustkube-152acbd2a1f0): TokenRequest honours `expirationSeconds` and
`boundObjectRef` (Pod name + uid); TokenReview returns `authentication.kubernetes.io/pod-name`/`pod-uid`/
`node-name` in `status.user.extra`. The kubelet's `Authorization` header on :10250 is the caller's own credential
(auth_mw), so the workload's token comes in `X-Storm-Workload-Token`.
1. [x] Tokens (d25f05c): kube-api-access asks `expirationSeconds: 3607` + `boundObjectRef` (the Pod); a projected
       `serviceAccountToken` passes its own `expirationSeconds` (default 3600) + the Pod. Each file recorded;
       a refresher rewrites it (tmp + rename) at 80% of its life (`status.expirationTimestamp`); dropped with the pod.
2. [x] (0cdc142) `/vmInstance/{address}` from a node address with `X-Storm-Workload-Token`: TokenReview; the pod in
       `status.user.extra` must be on this node (node-name), and its object (GET) must match uid, nodeName,
       not terminating, not terminal; the answer is that pod's metadata (`storm.io/kind: Pod`). Anything else:
       today's refusal (404).
3. [x] Tests (5 new), docs (c79e92c), CHANGELOG. sc-build `cargo build --locked && cargo test --locked`: 336
       kubelet unit pass. Golden, stormimds#12 note, close.

### Waiting on stormstorage#44: #40, live migration of stormvm-started VMIs

2026-10-05. Both halves exist: stormvm dc1b7ea `plan::build_receiving`, `Machine::{migrate, receive_migration,
migration}`; rustkube#184 drives VMI `status.migrationState` (controller: migrationUid/sourceNode; scheduler:
targetNode; target kubelet: receiving start + `targetNodeAddress`; source kubelet: send + completed/failed).
CRD stormcos#288 (open). **Blocker: the disks.** Migration moves memory only; every VM disk is a volume in the
source node's stormblock, the target cannot attach it (#142, no NVMe/TCP connect) and stormvm copies no disks.
A target started normally would clone a fresh root: guest RAM and disk would disagree. Asked on #40: A shared over
NVMe/TCP (#142; recommended), B copy during migration (blockdev-mirror), C replicas only (#68). No code yet.
**Owner (#159, 2026-10-05): add a RAID leg on the destination, let it catch up, then move memory.** Needs a storage
primitive for in-use local volumes (migrate → synced → cut over / abort): filed stormstorage#44; #40 proposed after
it. Kubelet side then: target waits all disks synced → receiving start on its copies → targetNodeAddress; source
sends → completed/failed; cut over or abort each disk.

### Waiting on stormblock-registry#95 / stormcentral#521: #91, a bridged VM's IP from its tap (stormvm_net::snoop_tap)

stormvm 2be5900 (lock moves 1b0d941 → dc1b7ea, `cargo update -p stormvm-net` on dev with
`CARGO_NET_GIT_FETCH_WITH_CLI=true`; the lock diff applied here). `snoop_tap(tap, mac, changed)` → `Snooper`
(`addresses()`, drop joins its thread, ≤1 s RCVTIMEO). `Made.binding == "host-bridge"` marks a tap on a node bridge.

Steps:
1. [x] Lock bump; `resolve_nics` takes the uid and starts a snooper per host-bridge NIC before the spawn (a failure
       to open it is a warning, not a failed start); `start` keeps them, into `snoopers[uid]` once running.
2. [x] Callback → unbounded channel → pump task (spawned in `run`): that NIC's addresses + `patch_status` at once.
3. [x] `absorb_ends`: snooper per NIC first, then guest agent, then neighbour table. Snoopers dropped (in
       spawn_blocking) when the VM stops or ends.
4. [x] Tests, docs (README), CHANGELOG. sc-build at 3ad9ea8: build clean, all pass (kubelet 205).
       The bump had also moved stormpump (30a76d3 → e8ccef9) and stormcast; pinned back with `--precise`, because
       stormvm-node doesn't build against stormpump's `Mount.propagation` (filed stormvm#65, which also blocks #81).
5. [ ] On the test host (the issue's done-when): a new bridged VM shows its lease in `status.interfaces[].ipAddress`
       within seconds. 2026-10-05: C2NR0Q2 is up on 11.80 (carries 3ad9ea8). stormcos_qa's `vm-waves` suite is
       exactly this (bridged `storm.io/bridge: stormbr0` Fedora VMs, "Running with an address in
       status.interfaces[]"); running it there. Found on the way: stormcentral builds the Job itself (no
       `test/rustkube-node-test.yaml`, no `RUSTKUBE_NODE_TEST_IMAGE` = #97), grants cluster reads only through
       `test/requires.toml` (none here), and `STORM_NODE` is an address: the #59/#62 medium cases need fixing.
       Run a844d807fe: C2NR0Q2's sbregistry answers 507 on the push (stormcentral#376, commented). #91 proposed
       after stormcentral#376; rerun `stormcentral test run stormcos_qa vm-waves --tag C2NR0Q2` then.
       Test container fixed for the real runner (fb7fc66, 8d65920: own image #97, node name, requires.toml,
       node-volumes restore → skip); sc-build `cd test && cargo test --locked && cargo build --release
       --locked`: 19 pass. #97 also proposed after stormcentral#376 for its live run.
       2026-10-07: stormcentral#376 closed; vm-waves 18d8dc0653 died on the Dell's apiserver (stormcos#337, down since
       20:55Z, reinstall queued). #91 proposed after stormcos#337.
       2026-10-08: stormcos#337 closed (C2NR0Q2 on 11.89). Still no live run: test images are built on the retired
       dev.g8.lo (stormcentral#521) and a fresh node's media import fails (stormblock-registry#95, P0), which the
       Fedora VMs need. Unit tests pass on a build VM (fb94c9a). #91 proposed after stormblock-registry#95.
       2026-10-06: 11.88 on C2NR0Q2 has the 4 GiB registry (stormcos#122, no more 507). Queued: vm-waves
       18d8dc0653 (#91, #92 half 1), rustkube-node medium fff1f4d9d9 (#64, #59, #62, #67) and short c7e24520ec,
       at 65e3c3c. Read with `stormcentral test show <id>`; close what passes.

### Done: #87, static (mirror) pods: logs and stale status

Logs: stormd services answered by #72. registry/stormblock/timesync wait on stormpump#55 (assets.json names no log).
Stale status, found 2026-09-28: stormpump lists every asset it tried to start (refused ones too, with
last_error), but one not started on this boot is not in the table, and the mirror writes only pods for listed
assets, so its pod keeps the last boot's Running and startTime (registry, stormstorage on C2NR0Q2 at 17:32).

Steps:
1. [x] Each pass: list this node's mirror pods (`storm.io/component=node-service`, spec.nodeName); one whose asset
       is not in the table gets status "not started on this boot" (phase Pending, waiting NotStarted, not
       Ready), written once (skipped when already so), with a Warning Event. Never deleted.
2. [x] Tests, docs (README), CHANGELOG. sc-build at 7f4f3d1: all pass (kubelet 203). Not run on a node.
3. [x] registry/stormblock/timesync logs: after stormpump#55 names each asset's `w<id>.log`, serve it (and the
       previous incarnation's for `--previous`) from `/hostrun/stormpump/logs`. Then close #87.
       2026-10-08: landed as stormpump#90 (17407fc, #55 left open): assets.json `runs` (last 5, oldest first, the
       running one last), each with `log` (`w<id>.log`) and `log_rotated`, or `stdout`/`stderr`. Doing: `Record.runs`;
       current = the running run's files (rotated first), `-f` follows the live one; `--previous` = stormd's
       `.failed.log`, else the newest ended run's files, else `last_output`. Tests, README/api.md, CHANGELOG.
       Done: 99c34e2 (2 tests). NOT yet built: the SC_BUILD_VM job was cancelled while queued. Rerun, close.

### Done: #72, `kubectl logs` on a node service's mirror pod reads its stormd log volume

Found, 2026-09-28: a mirror pod (`kube-system/<asset>-<node>`) is not in the pod manager, so `containerLogs`
answers "not found on this node" (#87 is the same miss). stormd writes `<proc>.log` (current),
`<proc>.N.log` (rotations, 1 newest) and `<proc>.<YYYYMMDDTHHMMSS>.{failed,exited}.log` into its
`/var/log/stormd`, which boot.d mounts from a host path (`volume felogs /logs/fastetcd` +
`mount felogs /var/log/stormd`). The kubelet sees the host at `/hostroot`. Lines are
`<rfc3339> <stream> <severity> <msg>`. Assets not run by stormd (stormblock, registry, timesync) write only to
stormpump's `/run/stormpump/logs/w<id>.log`, and assets.json does not name the id: stormpump issue, not here.

Steps:
1. [x] `node_logs.rs`: boot.d parse (spec → host path of its /var/log/stormd), current run (rotations + live, every
       process, merged by time, `[proc]` prefix when more than one), `--previous` = newest `.failed.log`.
2. [x] `containerLogs`: a pod this node does not run, in kube-system, named `<asset>-<node>`, container `<asset>` →
       node logs. tailLines, sinceSeconds/sinceTime, timestamps, limitBytes, follow (polls the live files).
3. [x] Tests, docs (README), CHANGELOG. sc-build at ec32b92: all pass (kubelet 202). Filed stormpump#55 (log id in
       assets.json) for stormblock/registry/timesync; #87 keeps those and the stale-status half. Not run on a node:
       whether `/hostroot/logs/<svc>` shows the mounted volume is unchecked (C2NR0Q2 down). Unreleased.

### Waiting on a test machine (stormblock#331 / morning blades): #64, PVC size test (the medium suite's first test)

2026-10-08: medium ran at c8256ac on server3 (63a3c5201b, 19:08–19:43Z): image built and pushed, the Job ran, and 90 s
into the size cases the node's apiserver stopped answering (delete of sz-class-1mi, then cleanup, timed out): the
second time after the Dell's fff1f4d9d9. The Dell's 4d4721131e: the Job never got a pod. All twenty size cases ran at
once (every class blank a format, up to 16Ti and 1Pi). Doing: `SIZE_CASES_AT_ONCE = 3`, largest first (the test is
not a stress test); file stormcos (the control plane starved by storage load) with the two runs; rerun medium.
2acfe02 done (28 pass); stormcos#400 filed. Rerun bfec2f964b (pvetest1): node never settled (flow-over, stormblock#331).
Blades off 19:00–06:00 Chicago. #64 proposed after stormblock#331; else run on server3 in the morning.

Started 2026-09-27. No `test/` existed (that is #61), so this adds the container, modelled on
stormblock-csi's `test/` (own workspace, static musl binary, scratch image, the image doubles as
the workload pods' program). Short and long report one skip pointing at #61.

Steps:
1. [x] Kubelet: `parse_quantity` reads fractions (`3.5Gi` parsed as nothing, and the claim got 1 MiB).
2. [x] `test/`: Cargo.toml (lock seeded from stormblock-csi/test, same deps), env, api, report, workload
       (`sized <path> <seed> <bytes> <lo> <hi>`: write, read back, statvfs total in (lo, hi]).
3. [x] medium `pvc-size-*`: every ladder class + 1, 1Mi+1, 17Mi, 1500M, 3.5Gi, 600Gi, in parallel; each bound,
       status capacity = its class, df in (previous class, class], deleted, PV gone. 2Ti: FailedMount /
       waiting reason "larger than the largest size class", never bound. 1Ti timed against a budget
       (`RUSTKUBE_NODE_TEST_MINT_BUDGET`, default 20 min). Overcommit: skip until #62 decides.
4. [x] Containerfile (repo root context), Job yaml, docs, CHANGELOG. Also found and fixed: a request-sized PV
       (control plane first) never got the class's capacity (30ce985). sc-build: root at 30ce985 all pass
       (kubelet 195); `cd test` at f63d8c2: 15 tests pass `--locked`, release binary runs (sized, short skip).
5. [ ] On a machine: run 831e049d94 (medium, e2ae178, C2NR0Q2) sat at "podman build" > 60 min while every
       other run there failed at push / sbregistry :5100 refused. Rerun when the test pipeline is healthy; the
       machine's kubelet predates c5187a6/30ce985, so 3.5gi and capacity cases should fail until a release.
       2026-09-28: 831e049d94 still at "podman build" (>1 day, no timeout; the queue behind it waits). :5100
       answers again (stormcentral#71). Doing: `test/build.sh` builds the static binary on dev, the
       Containerfile is `FROM scratch` + COPY (no rust:1-alpine pull, no compile in podman), as cadvisor's;
       file the hung run on stormcentral; rerun.
       Done: 38dba5a (build.sh verified on dev: static-pie, `/test short` exits 2 with no runner env). Hung run
       filed as stormcentral#139. Rerun 20d0bf509c (38dba5a) sat at "power" 15:56–16:51 UTC; C2NR0Q2 then
       unreachable (no ping, no route). **Waiting on the test machine**; rerun
       `stormcentral test run rustkube-node medium --commit <head>` when it answers. Overcommit case still
       waits on #62 (owner decision).

### Done: #63, a pod waiting on its claim stays visible (ContainerCreating + reason)

Found, 2026-09-27: two causes. (1) A pod whose volumes are not ready gets a Pending status and is never
recorded, so `pod_uid` misses it: `logs` says "not found on this node", no container statuses. (2) The
engine client has no timeout and `mint_template` is one synchronous POST, so a 1 TiB blank's format holds
the whole sync loop (stormblock#141 keeps formatting after a client leaves).

Steps:
1. [x] Mint in the background (one in flight per blank); a template not `ready` is a wait naming its state
       ("template pvc-ext4j-1048576m awaiting_format"), not a clone attempt.
2. [x] Waiting pods recorded (uid, reason, since): statuses report every container `waiting: ContainerCreating`
       with the reason; `logs` answers 400 "waiting to start: ContainerCreating" instead of 404.
3. [x] FailedMount Event on the pod; past 5 min the message says "timed out after Nm waiting for …" and the
       pod keeps retrying, Pending (upstream's behaviour: a mount timeout does not fail the pod).
4. [x] Tests, docs (README), CHANGELOG. sc-build at c78baf0: all pass (kubelet 193). Closed; noted on #70
       (wait-for-ready done here, Event on the claim still open). Not run on a node. Unreleased.

### Decided, not started: #62, CSIStorageCapacity for the built-in class

Recorded 2026-10-02: the owner answered on #108 ("take the recommendation"): the class size counts
at bind, overcommit ratio 1.0, clones sized to the class. Resume from the findings below.
2026-09-28: stopped before code. What a node publishes, and when the kubelet refuses a claim, both depend on
how a claim counts against the slab, and the issue leaves that open. Questions posted on #62; resume from the answer.

Found, 2026-09-27:
- Engine: `GET /api/v1/slabs` items have `role` (system|data), `total_bytes`, `free_bytes`. Claims live in
  the data half (blanks minted `role: data`). `/api/v1/volumes`: `virtual_size_bytes`, `allocated_bytes`.
  No overcommit guard in stormblock (clone never checks space; writes get NoSpace). Pool pressure gauges
  only, pool-wide, only with growth enabled; no Event.
- rustkube scheduler (`pkg/scheduler/src/volumebinding.rs` `capacity_fits`): applies to unbound claims whose
  class provisioner is a CSIDriver with `storageCapacity: true`; matches `storageClassName` + `nodeTopology`
  selector on node labels; compares `maximumVolumeSize` (else `capacity`) against the raw request (not the
  class). No object for a node → node rejected.
- stormcos `46-csidriver.yaml` (`stormblock.storm.io`) lacks `storageCapacity: true`: publishing does nothing
  until stormcos flips it, and it must flip only after every node publishes. Provisioner `stormblock.storm.io`, WFFC.
- Open for the owner: overcommit ratio default (class size counted at bind vs written bytes), and whether
  to size clones to the exact request.

### Done: #59, every node volume a complete, current PV + PVC set

Found, 2026-09-27: `system_claims.rs` creates once and never updates, skips `*-logs`, and leaves
`claimRef` without uid and the binding annotations off. rustkube's binder (persistentvolume.rs) owns PV
phases (Bound if the claim exists, Released if not) and the protection finalizers, so the mirror writes
objects and bindings, not phases. The engine marks nothing as logs: kind comes from the name suffix
(`-data`, `-state`, `-logs`), and `role` is only the slab half (system|data), recorded as an annotation.

Steps:
1. [x] Shared builder (`stormblock_pv`, `bind_pvc`, `objects`): labels `storm.io/volume-kind`, `storm.io/component`;
       PVC bind-completed, bound-by-controller, storage-provisioner (+beta), selected-node; PV provisioned-by,
       `claimRef` with uid+resourceVersion, `csi.fsType`, `volumeAttributes`, health/access/role annotations.
2. [x] Reconciler: two lists a pass; PVC first, then PV with its uid; size only grows; labels/annotations merged;
       claimRef follows the claim's uid. Only objects annotated with this node. Nothing written when current.
3. [x] `bind_claim` (built-in driver claims) uses the same builder.
4. [x] (b) A vanished volume: the mirror deletes its claim, the binder makes the PV Released, the PV is never
       deleted (the issue's stated outcome). Not on a listing with none of the node's volumes.
5. [x] **#107: `<volume>-<node>`, no migration** (5855ff8). PVC `kube-system/<volume>-<node>`, PV
       `storm-<volume>-<node>`; the vanished-volume branch keys on the claim's `storm.io/volume`, so old
       unqualified pairs are left as they are. E2E test: two nodes each get their pair, an old pair untouched.
       sc-build 5855ff8: 310 kubelet unit pass. `test/` medium `node-volumes-pairs` / `-restored` (82bc3ce;
       sc-build `cd test && cargo test --locked && cargo build --release --locked`: 17 pass). Live run waits on
       the release (the test machine's kubelet writes the old names) and the test pipeline (#64).
6. [x] Tests (fake apiserver + engine end to end), docs (`docs/node-volumes.md`), CHANGELOG. sc-build at 30bb887: all pass (kubelet 185).

### Done: #57, pod limits onto stormpump `Spec.limits`, container stats from `QUERY`

Steps:
1. [x] `spec_for`: `memory_limit_bytes` → `memory_max` (+ `swap_max = 0`), `cpu_quota`/`cpu_period` → `cpu_max`.
       Nothing sets `unified`/pids here, so those have no source yet.
2. [x] Ring client: one request owns the arena at a time (payloads all go at offset 0), and a request can
       have its region copied back after completion. `query_stats`.
3. [x] `list_container_stats`: `QUERY` stats → CPU (exact) and memory (`memory_current`, includes page cache).
4. [x] **Decided (#106): Pods under one parent cgroup (OpenShift), engine side stormpump#68 (done 2026-10-08, see above).** `cpu_shares` → `cpu_weight`. stormpump workloads are flat siblings, node services
       included (default weight 100). Upstream's conversion puts every pod below them (1 CPU → 39, no request → 1).
5. [x] Tests, docs, CHANGELOG. sc-build at 4245b8f: all pass (kubelet 177). Issue stays open on step 4.

### Done: #36, kubelet metrics under upstream's names

Found, 2026-09-27: `/metrics` and `/metrics/cadvisor` exist, but hand-written. `/metrics` has two gauges,
`kubelet_running_containers` has no `container_state`, there is no `process_*`, and the cadvisor side has
CPU and memory only, from CRI. The stormpump runtime reports no container stats at all: `QUERY` has a stats
block, but the ring client never reads a reply arena back. That is #57 (step 3), not this issue.

Steps:
1. [x] `metrics.rs`: the recorder (`kubernetes_build_info` with the kubelet's version), apimachinery's `process_*`
       collector, `kubelet_running_pods`, `kubelet_running_containers{container_state}`, and histograms
       `kubelet_pod_start_duration_seconds` (first seen → started) and `kubelet_pleg_relist_duration_seconds`
       (the sync pass over known pods; there is no separate PLEG). Upstream's buckets.
2. [x] cadvisor: `ContainerStatsInfo` fields optional (absent ≠ 0), plus `fs_usage_bytes` (CRI writable layer).
       Pod network from a new `list_pod_network_stats`. stormpump reads `/proc/<holder>/net/dev` of the sandbox.
       Series labelled `{container,id,namespace,pod}`, and network also `{interface}`, per pod.
3. [x] Tests, docs (`docs/metrics.md`), CHANGELOG. sc-build at f239350: all pass (kubelet 171). Closed.
   stormpump CPU/memory series wait on #57 (ring client must read QUERY's reply arena; noted there).

### Done: #73 + #74, stormvm v0.10.0's DiskSource (Empty, Claim)

stormvm v0.10.0 (1b0d941) adds `DiskSource::Empty` (emptyDisk) and `DiskSource::Claim`
(a PVC, which used to arrive as `Volume(<claim name>)` and fail with "invalid UUID").
`vm_manager::resolve_disks` matches exhaustively, so the lock bump and both arms land together.

Steps:
1. [x] Lock: `cargo update -p stormvm-spec` on dev (all stormvm crates move to 1b0d941).
2. [x] Empty (#73): find `<vm>-<disk>` by name, else create it blank with the disk's size (no redundancy), label `storm.io/vm`; owned by the VM.
3. [x] Claim (#74): the pod manager resolves it (`provision_claim`, shared): bound → its volume, unbound stormblock-class → provisioned,
       other class → Waiting. A pod on this node holding the claim → Waiting with its name. Not owned: deleting the VM keeps the volume.
4. [x] Ownership is an explicit list (Golden, CloudInit, Empty), so a new source is not owned by default.
5. [x] Tests, docs, CHANGELOG. sc-build at 6e4b48f: all pass (kubelet 166). Release v0.12.0, golden, close both.
   Follow-up filed: #80 (a pod can mount a claim a VM here is using).

### Done: #66 (P0, stormcos#104), the kubelet presents no stormblock engine token

The engine (stormblock ≥ 17) requires `Authorization: Bearer <token>` on its API.
Every engine call from the kubelet got 401, so claims and VM disks did not attach.

Steps:
1. [x] `engine.rs`: one engine client. The token comes from `$STORMBLOCK_API_TOKEN`, then the
       file at `$STORMBLOCK_TOKEN_FILE` (default `/run/stormblock/engine/api_token`), then
       `/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`. It is re-read on a 401, and on
       every call while none is found (the engine mints it at start). Tests.
2. [x] pod_manager, system_claims, vm_manager and stormpump_runtime use it instead of the apiserver client or a bare client.
3. [x] `scripts/build-golden.sh`: curl carries the token.
4. [x] Docs, CHANGELOG. sc-build at ca6dd06: build clean, all tests pass (kubelet 161, including engine::tests).
5. [x] Released v0.11.0 (036b976), golden-rustkube-node-fdc1c7497472, #66 closed.

### Done: #58, the golden build cannot load the workspace

The golden build fetches this repo alone, at one commit, and runs
`cargo build --release --locked`. `apimachinery` was `path = "../rustkube/..."`,
so there is no sibling there and the manifest does not load.

Steps:
1. [x] `apimachinery` becomes a git dependency on rustkube, pinned by `rev`
       (v0.15.2, e7f4fdb). A rustkube bump is an explicit change here.
2. [x] Regenerate `Cargo.lock` for it on dev (no cargo on this VM). `cargo update -p apimachinery` cannot
       match the old path entry, so use `cargo metadata`, which rewrites the lock minimally.
3. [x] Remove `scripts/sc-build.sh` and `.deps/`; docs (README, BUILD.md), CHANGELOG.
4. [x] `sc-build 'cargo build --release --locked --target x86_64-unknown-linux-musl'`, then `cargo test --locked`: passed at 6741e93.
5. [x] Close #58, request the golden.

### Waiting on stormblock-registry#99 (pushed images): #52, external StorageClasses (the CSI node side)

2026-09-29: stormpump#35's engine side is on stormpump main (a06ce4c..), and `/` is rshared on C2NR0Q2 (11.51).
What is left here is step 6 = #81 (`Mount.propagation` into `spec_for`), which needs stormpump ≥ a06ce4c in the
lock. The lock has one stormpump, shared with stormvm-node, and stormvm main (cf343c7) still builds
`stormpump::spec::Mount` without `propagation` (plan.rs:347): stormvm#65, open. Proposed #52 and #81 after it.
Then: bump stormpump, map propagation (#81), end to end with csi-driver-host-path on a node (step 9).

(Was: parked on stormpump#35 (P1).)

Findings, 2026-09-24:
- `csi.rs` was a stub. It logged calls and created directories, and it never
  spoke gRPC. The issue called it complete, and it was not.
- **Blocked outside this repo:** stormpump makes every container's mount namespace
  `MS_PRIVATE`, and its spec has no propagation field. So a CSI driver's
  NodePublish mount cannot reach PID 1's namespace, and the application
  container's bind would find an empty directory. It needs a stormpump issue
  for per-mount propagation, and possibly stormcos for `/var/lib/kubelet` as a
  shared mount on the host.

Steps:
1. [x] Vendor `csi.proto` and the plugin-registration proto; build.rs generates clients (and servers, for tests).
2. [x] `csi.rs`: a real gRPC client over the driver's Unix socket (Identity + Node).
3. [x] `csi_plugins.rs`: watch `/var/lib/kubelet/plugins_registry`, GetInfo, NodeGetInfo,
       write `CSINode` and the topology labels, NotifyRegistrationStatus, and deregister when the socket goes.
4. [x] Mount: a claim bound to a PV of another driver waits for its VolumeAttachment
       (when the CSIDriver has `attachRequired`), then NodeStage and NodePublish, and the published directory is bound in.
       Write `vol_data.json` beside the mount so teardown survives a kubelet restart.
5. [x] Unmount: NodeUnpublish when the pod goes, NodeUnstage when it is the last pod on the node.
6. [x] Pass `mountPropagation` through to stormpump (#81, 599a86e). Was stormpump#35. Until then the mountinfo check keeps pods waiting instead of giving them an empty directory.
7. [x] Tests: a mock driver and registrar on a real Unix socket, the full round trip.
8. [x] Docs (`docs/csi.md`), CHANGELOG.
9. [ ] End to end with csi-driver-host-path on a node (step 6 done by #81). 2026-10-08: no node runs #81 yet (goldens
       in the open release request stormcos#366; test machines on 11.91); the driver's images must be in the node's
       registry. #52 proposed after stormcos#366. Then: lease a machine, deploy the driver, pod on its claim, write/read,
       kubelet restart, delete → NodeUnpublish.
       2026-10-08: 11.93 (pvetest1) carries #81. Images copied there over /v2/ (tmp/regcopy.py: registry.k8s.io →
       :5100, podman is not usable here); every golden build fails at the seal (stormblock-registry#99, filed). Manifests
       in tmp/csi52/ (CSIDriver no-attach, one pinned plugin+registrar+provisioner pod, Immediate SC, 16Mi PVC, busybox).
       #52 proposed after stormblock-registry#99; lease released, nothing created on the node.
10. [x] Build verified: `sc-build scripts/sc-build.sh` at 004b2e7, build clean, all kubelet tests pass.

Related, filed elsewhere: rustkube#94 (no ephemeral-volume controller).
