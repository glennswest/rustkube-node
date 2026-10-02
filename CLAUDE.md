# CLAUDE.md — rustkube-node

The node half of rustkube: the kubelet (`pkg/kubelet`, `cmd/kubelet`), kube-proxy
(`pkg/proxy`, `cmd/kube-proxy`) and the CNI helpers (`pkg/cni`). It ships as a
stage golden through `stormcentral component stage rustkube-node`, not the
legacy bin-only `scripts/build-golden.sh` or a package. The cross-project rules are in
`../CLAUDE.md`; this file is the project's own context and work plan.

## Version

`0.13.0`. There is one version location: `[workspace.package] version` in
`Cargo.toml` (every crate uses `version.workspace = true`).

## Current implementation reference (2026-10-02)

Main baseline: fecb331. See `docs/status.md` for changes since September 25,
the owner's recorded decisions and issue-backed limitations, `docs/configuration.md` for every CLI/env/default,
and `docs/api.md` for ports and actual routes. CLI runtime defaults to native;
stormcos explicitly chooses stormpump. Cilium owns Services; the packaged
kube-proxy is not started. PVCs use the built-in stormblock driver and sealed
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
(image 11.50). `scripts/build-golden.sh` also makes a bin-only golden, and is
not the release path.

## Work plan

### Done: documentation refresh from code since 2026-09-25 (#54, #120, #121)

2026-10-02, on main at fecb331.
1. [x] Audit README, docs/, CLAUDE.md against code and `git log --since=2026-09-25`.
2. [x] status.md: changes since 09-25, decided-not-implemented table (#106–#110), new gap rows
   (#115, #116, #118, #119, #122, stormimds#12). README, api.md, configuration.md, BUILD.md,
   csi.md (inotify registry, 1 s retry), node-volumes.md updated. :5100 is sbregistry (#120).
3. [x] Module comments (#54): cmd/kubelet main.rs, storage.rs (PID 1 mounts, container binds),
   server.rs `/vmInstance` (stormimds#12 undecided).
4. [ ] CHANGELOG; commit, push, sc-build; comment on #54/#120/#121.

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

### Decided (#109, option A), restore not started: #53, VirtualMachineSnapshot / VirtualMachineRestore

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
3. [ ] Restore: **owner decision**, tracked on #109 (extracted from #53, 2026-09-29). A = restored volumes as PV + PVC, the kubelet
       patches the VM template's volume to the claim + node affinity (recommended); B = annotation
       `storm.io/restored-disks` copied by rustkube's VM controller to the VMI. Resume from the answer.

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
5. [ ] **Decided (#107): `<volume>-<node>`, no migration; not implemented.** Names collide across nodes. `kube-system/fastetcd-data` and PV
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
4. [ ] **Decided (#106): Pods under one parent cgroup (OpenShift), engine side stormpump#68.** `cpu_shares` → `cpu_weight`. stormpump workloads are flat siblings, node services
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

### Parked on stormvm#65: #52, external StorageClasses (the CSI node side)

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
6. [ ] Pass `mountPropagation` through to stormpump once it has the field. Filed as stormpump#35. Until then the mountinfo check keeps pods waiting instead of giving them an empty directory.
7. [x] Tests: a mock driver and registrar on a real Unix socket, the full round trip.
8. [x] Docs (`docs/csi.md`), CHANGELOG.
9. [ ] End to end with csi-driver-host-path: blocked on step 6 (stormpump#35).
10. [x] Build verified: `sc-build scripts/sc-build.sh` at 004b2e7, build clean, all kubelet tests pass.

Related, filed elsewhere: rustkube#94 (no ephemeral-volume controller).
