# Event-driven node (turbomode)

Design, 2026-09-29. Paired with rustkube's docs/event-driven-design.md and
issues rustkube#143–#147. Preserve the kubelet API, CRI, CSI, CNI, Pod/VMI
status and static-pod bootstrap. Build on dev only, after 10:00 America/Chicago
on 2026-09-29. No build or runtime-validation claims before that gate.

## Findings before turbomode

The pre-turbomode kubelet sync waited on sync_interval, lists all Pods, filters for the
node, then serially reconciles VMs and Pods. A VMI watch already exists but
only replaces desired state; a timer still gates execution. Reclaim and CSI
cleanup run every 30 seconds, plugin discovery every two seconds. Runtime
and volume operations can hold up unrelated work. A failed Pod LIST is
currently treated as an empty desired set, which risks stopping live Pods.

## Target

Revisioned Pod/VMI and volume dependency watches update desired state, then
queue affected UIDs. One active worker per UID, a dirty bit for changes during
execution, FIFO ready queues and bounded parallelism across independent UIDs.
No mutex held over network, image, volume, probe or runtime I/O. Desired state
and observed runtime state are separate; absence is authoritative only after
a complete synchronized LIST/WATCH. Retain the last desired set through API
outages. A recreated name has a new UID and cannot inherit old cleanup work.

Pod stages are explicit: admitted → waiting for image/volume/network →
creating sandbox → starting init/app containers → running → terminating →
cleanup. Async completion moves work to ready immediately. Independent image
pulls and volume preparation can overlap, with per-image/per-volume deduplication
and bounded concurrency. Partial failure records completed stages for retry;
cancellation must not leak a mount, sandbox, or container. Teardown and Pod
admission need per-claim serialization to preserve ReadWriteOncePod.

A Pod watch uses spec.nodeName; VMIs are watched twice, by status.nodeName and by
spec.nodeName (a VMI placed by hand, #85), merged by uid, with local
assignment checks as defense against a server ignoring selectors. Page every
LIST. Watch from its revision, frame bounded NDJSON, resume on disconnect,
relist on 410, back off failures. Apply observed state before waking work.
Never let independently spawned watch callbacks reorder snapshots. Filter
unchanged input/status echoes so a status PUT does not loop forever.

Sources of local changes:

* Runtime exits and lifecycle events: CRI event stream where supported,
  stormpump completion/event ring for its backend. A runtime lacking events
  gets an explicitly identified, measured observation fallback, not a hidden
  blanket Pod sync timer.
* Static manifests and CSI registration sockets: filesystem notifications,
  initial directory scan, overflow-triggered rescan. Static Pods must start
  without any API connection. An unreadable directory is not empty state.
* Volumes: PVC/PV/VolumeAttachment watches plus CSI RPC and stormblock mint/
  attach completion. If the engine provides no completion stream, back off
  only the outstanding volume operation and file the engine contract gap.
* Probes: schedule each container's actual periodSeconds/initialDelaySeconds;
  readiness transitions enqueue status work immediately.
* Heartbeat, graceful shutdown, restart backoff and request timeout are real
  deadlines. Service/volume mirrors should follow engine state changes.

Use one workload queue and worker framework for Pods and VMIs. Runtime adapters
implement their different lifecycle operations; admission, dependency indexes,
volume reservations, retries, cancellation and status delivery are shared.
Key work by (kind, namespace, name, UID), with one active worker per key and
bounded concurrency across both kinds. A slow workload must not delay an
unrelated workload of either kind. Coalesce status publication by UID and
avoid unchanged writes; preserve resourceVersion and conflict retries. A
slow pull or a VM's 30-second graceful shutdown must not delay another Pod.
The compatibility step wakes current reconcile passes from events and fixes
failed-LIST handling first; it does not claim to solve per-object concurrency.
Replace that adapter with indexed per-UID state machines before claiming the
full architecture or scale target is complete.

## Acceptance

Tests: list/watch race, pagination, split frames, 410/reconnect, error Status
vs empty list, deletion/recreation, event during worker execution, no-op
status echoes, cancellation during runtime/CSI I/O, static-pod API outage,
container exit without API change, delayed volume readiness without Pod edit,
and a slow Pod/VM not blocking a fast one. Use fake runtime/clock tests first.

On real nodes measure accepted Pod write → bind → observed assignment →
sandbox → container process start → Ready, p50/p95/p99/max. Warm cached image
and already-ready storage target subsecond create-to-process-start; separately
report cold images, format time, deliberate grace periods and probes. Test
idle CPU/requests, burst load and API/runtime disconnect recovery. Do not
claim subsecond behavior based only on deleting sleeps.


## Current migration and remaining work

The first change reuses rustkube's shared reactor/reflector/work queue with a
pinned git dependency. It separates Pod and VM passes, observes assignment
and volume collections, wakes on stormpump exits and Linux filesystem events,
and suppresses destructive cleanup on partial desired-state reads. It does
not yet add per-UID runtime concurrency or eliminate all observation fallbacks.

- [#99: architecture](https://github.com/glennswest/rustkube-node/issues/99)
- [#100: per-UID workers](https://github.com/glennswest/rustkube-node/issues/100)
- [#101: remaining local event sources](https://github.com/glennswest/rustkube-node/issues/101)
- [#102: real-node performance](https://github.com/glennswest/rustkube-node/issues/102)

No builds/tests have run for these changes; only static review and formatting.
Do not deploy or close performance issues on that evidence.

## Owner-supplied release baseline: C2NR0Q2

Measured on each OS-release install; provided 2026-09-29. Empty entries and
em dashes below both mean no reported measurement, not zero. Instrumentation
boundaries, sample counts, image warmth and polling granularity are not yet
verified. These are observations, not percentiles.

| Metric | 11.44 | 11.45 | 11.46 | 11.48 | 11.49 | 11.50 | 11.51 |
|---|---:|---:|---:|---:|---:|---:|---:|
| boot → SSH | 272s | 254s | 254s | 257s | 235s | 289s | 287s |
| boot → apiserver ready | 272s | 254s | 254s | 257s | 235s | 289s | 287s |
| boot → node Ready | 272s | 254s | 254s | 257s | — | 289s | 287s |
| boot → all pods running | 374s | 359s | — | 299s | — | 349s | 346s |
| container start | 12s | 12s | — | 12s | — | 21s | 21s |
| claim bind + mount | — | — | — | — | — | 76s | 67s |
| VM start | — | — | — | — | — | — | — |
| reboot → SSH | — | — | — | — | — | 183s | — |
| reboot → apiserver ready | — | — | — | — | — | 215s | — |
| reboot → node Ready | — | — | — | — | — | 215s | — |
| reboot → all pods running | — | — | — | — | — | 215s | — |

Container start regressed 12 → 21 seconds (+75%); claim bind/mount improved
76 → 67 seconds (~12%) but remains far from the target. Boot measurements
need separate firmware, OS, storage, service-start and readiness timestamps.
Identical SSH/API/Node times may be a sampling artifact; establish metric
implementation before assigning cause. Preserve the per-machine/per-release
history and add distributions and stage timestamps to it. VM startup needs a
measurement before any performance claim.


## Multi-master requirement

Traditional multi-master Kubernetes is required. The node targets the stable
control-plane VIP/load-balanced URL and must tolerate successive LIST, WATCH,
status and Lease calls reaching different API servers. Resume watch revisions
from the shared datastore history, handle 410 by complete relist, and retain
running work through API-server failover and quorum loss. Failed reads never
mean all assigned Pods/VMIs disappeared. UID/resourceVersion protects status
and cleanup from same-name replacements during a failover. No node queue
assumes a particular master or an in-process control-plane notification.

Acceptance includes three API servers, controller/scheduler leader changes,
watch reconnect to another master, minority partitions, old-leader resume and
node restart during failover. Paired control-plane HA gate: rustkube#149.
Normal subsecond startup and deliberate lease-expiry failover time are
measured separately. These scenarios have not yet run for this branch.

The initial global migration lock has been replaced by name/claim reservations
and per-claim mutation locks. Pod mounts and VM raw-disk use share admission;
unrelated claims can progress concurrently. Reservations survive failed cleanup.

CSI registration migration: socket-directory notifications now drive scans;
registration/publication failures get retry deadlines, and successful local
registration immediately wakes Pod reconciliation. Cleanup and mirrors remain
separate pending work under #101. Live-node validation remains under #102.

Owner clarification (2026-09-29): keep Pods and VMs as close as possible. The
existing separate loops are a temporary adapter, not the target design. Both
now use a common executor and admission/reservation table, with runtime-specific
operations behind adapters.

Cleanup checkpoint (#100): VM stop now retains records on query failures,
timeouts and refused detach/delete operations. Natural exits keep the engine
handle until teardown. Legacy registrations without an engine handle cannot
prove exit merely from an unavailable control socket; cleanup remains pending.
Pod deletion acknowledgement uses DeleteOptions UID preconditions.

The common executor now supports independent authoritative source snapshots,
so static-manifest replacement cannot imply API Pod deletion. Its inverse
claim/image/driver indexes target dependent UIDs, and status-only updates do
not enqueue runtime work. Recovery seeding retains all observed holders and
never downgrades an exclusive reservation. Both adapters are wired.

Adapter checkpoint (#100): Pod and VMI producers publish snapshots to one
executor (eight workers then; `--pod-workers` since #138). UID work owns name/claim reservations until confirmed
cleanup; shared filesystem users serialize claim mutations without serializing
unrelated claims. Recovery seeds names before admission and waits for complete
Pod/VMI views plus known claim ownership before admitting API workloads. Static
Pods without claims can bootstrap during an API outage. An intent change queues
a follow-up instead of aborting a runtime RPC; spawned operations retain their
state even if the supervisor future is dropped. Pod start records the sandbox
and each container before proceeding, so failed starts clean up before retry.
Per-UID observation deadlines remain pending #101. Adapter unit and integration
tests passed on dev; these are not live-node performance measurements or proof
of completion of the entire cancellation acceptance matrix.

Wait-state checkpoint: an init container still running leaves its sandbox and
container recorded and yields for the next UID observation. Completion resumes
startup without recreating the sandbox. VM shutdown sends the engine's grace
request once and yields until QUERY confirms exit. Finalizer cleanup failures
remain retryable. Deletion during a side effect waits for its completion before
the UID's cleanup pass; the same-name successor remains admission-blocked.

Image waiting is a separate stage: up to four pulls run concurrently outside
the eight runtime workers. Concurrent requests for the same image/policy share
a pull, while each startup retains its result. Later Always requests resolve
the tag again. Completion wakes the inverse image index. Claim reclamation
reserves exclusively in the admission table before checking holders/deleting,
so an in-flight start cannot slip between its check and delete.

PV/VolumeAttachment observations now join back to the claim dependency index.
The three collections are committed together; a failed collection read preserves
its previous dependencies. Runtime recovery must inventory sandboxes and
containers successfully before any new starts. CSI publication and teardown
share per-driver/handle mutation exclusion. Stormpump teardown retains records
on refused release and startup retains partially registered volume handles.

VM partial-start unwind (#100, on main after #114): [stormpump#63](https://github.com/glennswest/stormpump/issues/63)
added `DEPOSIT_WITHDRAW` (op 9), which closes a deposit no spawn consumed and
acknowledges it in the completion. The kubelet sends it by number
(`stormpump_ring::OP_DEPOSIT_WITHDRAW`): the stormpump-abi locked when this was
written predated the variant.

- **Ledger.** Per UID, outside the start's future: each `tap-<nic>` deposit is
  noted *before* it is sent, each volume/spec handle as the engine returns it,
  and the whole record is dropped inside the blocking spawn closure the moment
  the spawn succeeds. A start that fails, panics or is abandoned leaves an
  accurate record either way; withdrawing a deposit never sent is a no-op.
- **Deposit window.** stormvm names deposits `tap-<nic>` and the engine keys
  them by client and name, so two VMs with a NIC called `default` starting on
  two workers would replace each other's tap. One kubelet-wide lock covers
  the first deposit through the spawn's answer, and every withdraw; disk
  resolution stays outside it and concurrent.
- **Unwind points.** Inside the window after a failed launch; before the next
  start of the UID (Waiting while a deposit is still held, since the same tap
  name would meet EBUSY); and before a deletion is acknowledged, which keeps the
  name reservation so a same-name successor cannot start first. A handle that
  will not release is a leak, not a conflict: it is retried, and after the
  UID is gone it moves to an orphan bucket retried by later unwinds.
- **Engine answers.** EINVAL on withdraw means an engine older than op 9
  (warned and dropped: the engine closes a client's deposits when it goes);
  ESTALE on a release means already released. A kubelet restart needs no
  ledger: its reconnect drops the old connection's deposits.

Other side-effect boundaries closed at the same time: a failed CNI ADD is
followed by DEL before the sandbox is released (DEL needs the namespace), and a
DEL that fails keeps the sandbox, retried before the next sandbox. The
claim-reclaim handler runs the release in its own task, so a disconnecting
client cannot drop the claim's reservation while stormblock is still detaching
or deleting. Tests cover each with fakes (injected undo, a scripted CNI plugin,
a slow stormblock); none of this has run on a node (#102).

## Main integration (#114)

The UID executor preserves main's VM tap address pump and access-credential
reporting. A failed VMI LIST leaves desired state unchanged. Snapshot requests
have an independent watch worker and completion/recovery deadline; the owner
sweep retains its one-minute cadence. Starts share a read lock against the
sweep's exclusive disk-lifecycle lock, preserving sweep/start exclusion while
independent starts remain concurrent. VM stop checks every detach and runtime
release but never deletes VM-owned disks; only the owner sweep decides that.
Main's failed-start backoff and runStrategy policy also run through the UID
adapter. Complete event sources and cancellation coverage remain #100/#101.

## Events and deadlines instead of ticks (#101)

Implemented on main, 2026-09-29. The heartbeat (the node lease) is the only
fixed schedule left.

| Work | Was | Now |
|---|---|---|
| Live Pod | every `sync_interval` (2 s) | stormpump exits (or a CRI runtime's `GetContainerEvents`, #116), volume/image/API events; deadlines: each probe's `periodSeconds` from `initialDelaySeconds` (a probe not due keeps its last result), CrashLoopBackOff end, waiting-start retry (¼ of the wait, 1–10 s), `activeDeadlineSeconds` during init (no fixed init limit since #126) |
| Live VMI | every `sync_interval` | ring exits, API/volume events; deadlines: start backoff, waiting retry (1–30 s), guest-agent poll (2 s while booting → 30 s; 10 s handle-less). The agent has no push, so its poll is a per-machine probe period |
| Service mirror | 15 s | inotify on `/run/stormpump`, gated on the parsed table (read at most once a second while PID 1 rewrites it every pass: stormpump#67), plus the mirror pods' watch; writes only what differs |
| System claims | 30 s | stormblock volume watch + PV/PVC watches |
| Reclaim | 30 s | PV watch; 5 s retry while pending |
| CSI sweep | 30 s | this node's Pod watch; 10 s retry while a teardown is pending |
| Snapshots, disk-owner sweep | `sync_interval` | snapshot/VM/VMI watches, take completions, stormblock volume watch; the sweep's once-a-minute floor defers events |

Failures inside these workers call `apimachinery::reactor::failed()`, which
retries on the reactor's backoff (100 ms doubling to 30 s) rather than on a
clock. Fallbacks that remain are counted in
`kubelet_timed_reconciles_total{cause="fallback"}`: a CRI runtime without exit
events keeps `sync_interval` for its workloads, and an engine without the
volume watch is polled every 30 s. CrashLoopBackOff forgiveness is judged at
the next restart, since nothing looks at a healthy container on a clock.

Verified by unit tests only: probe and backoff deadlines, a settled pod with no
deadline, the engine watch's follow/reconnect, the sweep's deferral and VM
start retry deadlines. Not measured on a node (#102).

## Request deadlines (#99)

The ring, CNI and engine I/O named in #99's comments are bounded (see
[configuration](configuration.md#runtime-details)). The ring's deadline counts
from when a request is made, not from when it is sent. A timeout never unsends
an SQE: the request is kept as abandoned until the engine answers, so the
arena is not reused early, and whatever a late success made is undone by the
ring thread. Verified by unit tests (deadline bookkeeping, late-success undo,
a hung CNI plugin killed and reaped, a silent engine timing out), not on a
node.

### Acceptance status (2026-09-29)

| #99 acceptance item | Where it is covered |
|---|---|
| list/watch race, pagination, split frames, 410/reconnect, error Status vs empty list | rustkube `apimachinery::reflector` (rustkube#143, closed), which every kubelet watch and LIST uses |
| deletion/recreation, event during worker execution, slow Pod/VM not blocking a fast one | `workload.rs` tests (#100) |
| no-op status echoes | `workload::intent` and the reflector's semantic compare; the mirrors write only what differs (#101) |
| cancellation during runtime/CSI I/O | #100 (VM partial-start unwind, CNI DEL, reclaim handler, CSI teardown records) |
| static-pod API outage | `source_replacement_never_deletes_another_sources_work` |
| container exit / delayed volume readiness without API change | stormpump exit routed to its own Pod/VMI worker (#115; `one_exit_wakes_its_own_uid_only`) and deadlines (#101); `mint_completion_notifies_without_an_api_edit_or_sync_tick` |
| real-node latency (write → bind → sandbox → process → Ready, p50–max), idle CPU, burst, disconnect recovery | **not done**: #102 on C2NR0Q2 (owner's choice, #110), after the release with this code is installed |

Control-plane halves still open in rustkube: #144, #147 and #149.
