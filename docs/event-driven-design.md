# Event-driven node (turbomode)

Design, 2026-09-29. Paired with rustkube's docs/event-driven-design.md and
issues rustkube#143–#147. Preserve the kubelet API, CRI, CSI, CNI, Pod/VMI
status and static-pod bootstrap. Build on dev only, after 10:00 America/Chicago
on 2026-09-29. No build or runtime-validation claims before that gate.

## Findings

The main kubelet sync waits on sync_interval, lists all Pods, filters for the
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

A Pod watch uses spec.nodeName; a VMI watch uses status.nodeName, with local
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

The migration adapter serializes Pod/VM runtime mutations to preserve existing
claim admission assumptions. Removing this lock requires per-claim reservations
covering Pod mounts and VM raw-disk use, including rollback and restart recovery.
Independent subscriptions alone do not provide concurrent workload execution.

CSI registration migration: socket-directory notifications now drive scans;
registration/publication failures get retry deadlines, and successful local
registration immediately wakes Pod reconciliation. Cleanup and mirrors remain
separate pending work. No runtime validation has run yet.

Owner clarification (2026-09-29): keep Pods and VMs as close as possible. The
existing separate loops are a temporary adapter, not the target design. Both
will use a common executor and admission/reservation table, with runtime-specific
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
never downgrades an exclusive reservation. Adapter wiring remains in progress.

Adapter checkpoint (#100): Pod and VMI producers publish snapshots to one
eight-worker executor. UID work owns name/claim reservations until confirmed
cleanup; shared filesystem users serialize claim mutations without serializing
unrelated claims. Recovery seeds names before admission and waits for complete
Pod/VMI views plus known claim ownership before admitting API workloads. Static
Pods without claims can bootstrap during an API outage. An intent change queues
a follow-up instead of aborting a runtime RPC; spawned operations retain their
state even if the supervisor future is dropped. Pod start records the sandbox
and each container before proceeding, so failed starts clean up before retry.
Per-UID observation deadlines remain pending #101. This checkpoint is not yet
validated or a claim of completion of the staged/cancellation acceptance matrix.

Wait-state checkpoint: an init container still running leaves its sandbox and
container recorded and yields for the next UID observation. Completion resumes
startup without recreating the sandbox. VM shutdown sends the engine's grace
request once and yields until QUERY confirms exit. Finalizer cleanup failures
remain retryable. Deletion during a side effect waits for its completion before
the UID's cleanup pass; the same-name successor remains admission-blocked.
