# CLAUDE.md — rustkube-node

The node half of rustkube: the kubelet (`pkg/kubelet`, `cmd/kubelet`), kube-proxy
(`pkg/proxy`, `cmd/kube-proxy`) and the CNI helpers (`pkg/cni`). It ships as a
golden (`scripts/build-golden.sh`), not a package. The cross-project rules are in
`../CLAUDE.md`; this file is the project's own context and work plan.

## Version

`0.13.0`. There is one version location: `[workspace.package] version` in
`Cargo.toml` (every crate uses `version.workspace = true`).

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
(image 11.50). `scripts/build-golden.sh` also makes a bin-only golden, and is
not the release path.

## Work plan

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

### In progress: #92, a VMI's accessCredentials (keys into the seed and through the agent)

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
       Waits on C2NR0Q2 (unreachable) and a release. Then close.

### In progress: #91, a bridged VM's IP from its tap (stormvm_net::snoop_tap)

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
       within seconds. Waits on C2NR0Q2 (unreachable) and a release. Then close.

### In progress: #87, static (mirror) pods: logs and stale status

Logs: stormd services answered by #72. registry/stormblock/timesync wait on stormpump#55 (assets.json names no log).
Stale status, found 2026-09-28: stormpump lists every asset it tried to start (refused ones too, with
last_error), but one not started on this boot is not in the table, and the mirror writes only pods for listed
assets, so its pod keeps the last boot's Running and startTime (registry, stormstorage on C2NR0Q2 at 17:32).

Steps:
1. [x] Each pass: list this node's mirror pods (`storm.io/component=node-service`, spec.nodeName); one whose asset
       is not in the table gets status "not started on this boot" (phase Pending, waiting NotStarted, not
       Ready), written once (skipped when already so), with a Warning Event. Never deleted.
2. [x] Tests, docs (README), CHANGELOG. sc-build at 7f4f3d1: all pass (kubelet 203). Not run on a node.
3. [ ] registry/stormblock/timesync logs: after stormpump#55 names each asset's `w<id>.log`, serve it (and the
       previous incarnation's for `--previous`) from `/hostrun/stormpump/logs`. Then close #87.

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

### In progress: #64, PVC size test (the medium suite's first test)

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

### Blocked on owner decision: #62, CSIStorageCapacity for the built-in class (findings only, not started)

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

### In progress: #59, every node volume a complete, current PV + PVC set

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
5. [ ] **Owner decision (a), open:** names collide across nodes. `kube-system/fastetcd-data` and PV
       `storm-fastetcd-data` exist once per cluster, so only the first node's volumes are represented; the
       others log a warning. Asked on the issue.
6. [x] Tests (fake apiserver + engine end to end), docs (`docs/node-volumes.md`), CHANGELOG. sc-build at 30bb887: all pass (kubelet 185).

### In progress: #57, pod limits onto stormpump `Spec.limits`, container stats from `QUERY`

Steps:
1. [x] `spec_for`: `memory_limit_bytes` → `memory_max` (+ `swap_max = 0`), `cpu_quota`/`cpu_period` → `cpu_max`.
       Nothing sets `unified`/pids here, so those have no source yet.
2. [x] Ring client: one request owns the arena at a time (payloads all go at offset 0), and a request can
       have its region copied back after completion. `query_stats`.
3. [x] `list_container_stats`: `QUERY` stats → CPU (exact) and memory (`memory_current`, includes page cache).
4. [ ] **Decision (owner):** `cpu_shares` → `cpu_weight`. stormpump workloads are flat siblings, node services
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

### Parked on stormpump#35 (P1): #52, external StorageClasses (the CSI node side)

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
6. [ ] Pass `mountPropagation` through to stormpump once it has the field. Filed as stormpump#35. Until then the mountinfo check keeps pods waiting instead of giving them an empty directory.
7. [x] Tests: a mock driver and registrar on a real Unix socket, the full round trip.
8. [x] Docs (`docs/csi.md`), CHANGELOG.
9. [ ] End to end with csi-driver-host-path: blocked on step 6 (stormpump#35).
10. [x] Build verified: `sc-build scripts/sc-build.sh` at 004b2e7, build clean, all kubelet tests pass.

Related, filed elsewhere: rustkube#94 (no ephemeral-volume controller).
