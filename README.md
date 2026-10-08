# rustkube-node

The **node level** of [rustkube](https://github.com/glennswest/rustkube) — the
Kubernetes worker components, in Rust. Split into its own repo for parallel
development; the code stays upstream-shaped and monorepo-mergeable.

Current code: **main at fecb331, audited 2026-10-02**, workspace version
**0.13.0** plus unreleased changes (shipped to stormcos as stage goldens of
main; latest golden-rustkube-node-e5db6ac32831, release request stormcos#164). The kubelet registers Nodes, maintains
heartbeats, runs Pods, and reconciles stormvm VirtualMachineInstances through
stormpump. This is a partial Kubernetes node implementation; remaining gaps
and unsupported promises are tracked in [the capability audit](docs/status.md).
The event-driven UID worker implementation is merged (#114), with partial-start
unwind (#100), event-driven reconciliation (#101) and bounded runtime calls
(#99). Live measurement on a node is #102 (C2NR0Q2, owner's choice on #110).

## Event-driven reconciliation

The turbomode implementation uses rustkube's pinned reactor dependency. Pod and VMI
assignment/volume watches enqueue coalesced reconciliation work; Pod and VM
subscriptions run independently; runtime work uses one Pod/VMI executor (`--pod-workers` passes at once, default 16 per CPU within 32–256, #138) with name/claim
reservations and recovery barriers (#100). Stormpump exits and Linux static-manifest changes
also wake workers. CSI registrar sockets use filesystem notifications;
successful registrations wake Pod workers, with deadlines for pending failures. Failed or incomplete Pod/manifest reads cannot stop live
Pods by treating an unknown desired set as empty. Status publication retains
startTime, skips unchanged status and uses the observed resourceVersion.
Pod teardown retains its runtime record until stopping and volume cleanup succeed.
CSI teardown preserves its retry record through failed unstage calls and refuses
cleanup from unreadable records or incomplete Pod lists. Pod and VMI adapters share that executor. Failed startup/cleanup records are
retained, and same-name successors wait for the previous UID to release its resources.

The full workspace build and tests last passed on dev at `c74b589` (270
kubelet unit, four integration, 25 CNI and 17 proxy tests). Cleanup retains refused engine releases, CSI publication and teardown
serialize by driver/handle, and failed runtime recovery keeps admission closed.
PV and VolumeAttachment changes route through an inverse claim index; failed
collection reads retain the index. VM status and migration writes carry UID
guards. A VM start records what it puts in stormpump (each tap deposit before
it is sent, each volume and spec handle as it is registered) outside the
start's future; a failed, panicked or abandoned start is unwound with
stormpump's `DEPOSIT_WITHDRAW`
([stormpump#63](https://github.com/glennswest/stormpump/issues/63)) and handle
releases. Deposits are named `tap-<nic>` per engine client, so the window from
the first deposit to the spawn's answer, and every withdraw, is serialized
across VMs. The next start of that VMI waits while one of its taps is still
held, and its deletion (and the name) is held until the tap is withdrawn. A
failed CNI ADD is followed by DEL before the sandbox goes, and a claim reclaim
runs to completion even if its HTTP client disconnects (#100). Subsecond
startup has not been measured on a live node.
No global sync tick remains (#101): a live Pod or VMI is looked at again
only on an event or its own deadline (a probe period, a backoff, a pending
retry, a guest-agent poll), and the service mirror, system claims, reclaim,
CSI sweep and VM maintenance run on file, API-watch and stormblock volume-watch
events. What still polls is counted in `kubelet_timed_reconciles_total`
(see [configuration](docs/configuration.md)). A stormpump exit wakes its own
Pod or VMI worker only (#115): the exiting handle is mapped to its pod (container,
then sandbox) or its VMI, and only a handle no record names (or a lagged
channel) wakes every workload. With `--runtime cri` the kubelet follows the runtime's
`GetContainerEvents` stream (#116, upstream's evented PLEG): each event wakes the Pod its sandbox names (else
the Pod holding that container), every Pod looks once when the stream opens, and only while no stream is open
(a runtime without the RPC, CRI-O without `enable_pod_events`, or a dropped stream being reopened on a 1–30 s
backoff) do Pods keep the counted `sync_interval` fallback.
See [the design and baseline](docs/event-driven-design.md) and
[#102](https://github.com/glennswest/rustkube-node/issues/102) for validation.
Builds run on dev only after pushing. Main's tap address pump, snapshot
reconciler, VM startup backoff and disk-owner sweep remain active. A failed
VMI LIST retains the last desired set; stopping a VMI detaches its disks,
while the owner sweep decides when to delete them.

## Pod start timing

Every pod start says where its time went (#132). The kubelet keeps one record per pod
UID from the moment its pod list (a watch event, or a static manifest read) delivers the
pod, across every retry, to the moment the apiserver acknowledges `Running`, and then
writes it once, as the annotation `storm.io/start-timing`, a `StartTiming` Event, the
histogram `kubelet_pod_start_phase_duration_seconds{phase}` and one INFO log line:

    storm.io/start-timing: scheduled=850ms wait=1.2ms image=0.3ms volumes=4.4ms sandbox=40ms
      init=0.0ms containers=180ms report=8.1ms total=236ms attempts=1 workers=3/64 pending=1
      sandbox/acquire=6.2ms sandbox/cni=31ms sandbox/status=0.1ms sandbox/other=2.7ms volume/data=3.9ms volume/(serviceaccount)=0.4ms container/app=180ms

| phase | from → to |
|---|---|
| `scheduled` | the bind → seen here. The bind is rustkube's `storm.io/scheduled-at` annotation (microseconds, written with `spec.nodeName`, rustkube#190), else the pod's `PodScheduled` transition, else its `creationTimestamp`; those two are whole seconds and carry up to a second of truncation (#135). Wall clocks of two machines (exact when the scheduler is on this node): negative when they disagree by more than the gap |
| `wait` | seen → the start attempt that succeeded began: admission, image and volume waits, earlier attempts |
| `image` | the pod's images first asked for → the last resolved. A pallet is ~0; a pull is one registry lookup of the image's golden record (no clone: #104) |
| `volumes` | the pod's volumes in that attempt: claims cloned and attached, configMaps, secrets, projected, the ServiceAccount token, resolv.conf and log dirs. Each spec volume also as `volume/<name>` |
| `sandbox` | the sandbox made, its network (CNI) included, → its address read. Taken apart (#139) when the attempt made it: `sandbox/acquire` (stormpump `SandboxAcquire`, the warm namespace holder; 0 on the host network), `sandbox/cni` (the CNI ADD, the plugin's exec included), `sandbox/status` (the address read) and `sandbox/other` (the rest: the runtime's checks, retried DELs of earlier failed networks). A CRI runtime gives only status and other. No step is a stormblock call |
| `claim/<volume>/…` | a claim's steps (#95), after its `volume/<name>`: `lookup` (the PVC, its PV and an existing volume read, the room checked), how it was made with its time (`blank`: a clone of the class's sealed blank; `clone`: of a source; `raw`: a Block claim; absent when the volume already existed), `attach` (ublk) and `bind` (the PV written, the claim bound; only where the start waits for it, the VM path: a pod's start runs it in the background since #95, as the control plane has normally bound the claim already). The filesystem mount is PID 1's, when the container is created, so it is in `containers` |
| `init` | init containers run to completion |
| `containers` | every app container created and started; each also as `container/<name>`. Creating one includes making its own root (#104): the clone of the image's golden, its attach and PID 1's mount |
| `report` | the `Running` status write sent → acknowledged |
| `total` | seen → `Running` acknowledged |

`image` overlaps `wait` (images resolve off the worker while the pod waits). A static pod
has no API object, so it gets the log line and the histograms only. The annotation's merge
patch changes the pod's resourceVersion; a status write racing it gets a 409 and is retried
on the newer object.

`workers=<busy>/<limit>` is the executor's passes running when the start attempt began, this
one included, out of `--pod-workers`, and `pending=<n>` the pods seen here and not yet started,
this one included (#138). A large `wait` with `workers` at its limit is the pool; with
`workers` below it, the wait was the pod's own (an image, a volume, the network). The
annotation's patch is written off the worker, after `Running` is acknowledged.

`attempts` is 1 for a pod with nothing to wait for (#134). A start waits up to 100 ms for an
image it has just asked for, which covers a golden or a present image; a pull that takes
longer leaves the pod `ContainerCreating` ("waiting for image …") and its completion starts
the next attempt. More than one attempt means the pod waited on something: an image pull,
a volume, the network or storage, named in its waiting reason and Events. `report` is the
one status PUT and nothing else, so a long `report` is the apiserver's write. The ordinary
lifecycle Events (`Pulling`, `Pulled`, `Created`, `Started`, `StartTiming`) are queued and
written in order by one background sender, so `containers` does not include their POSTs;
Warnings are written before the start moves on.

## Components and runtime selection

| Component | Source | Current role |
|---|---|---|
| `kubelet` | `cmd/kubelet`, `pkg/kubelet` | Node registration, Pod lifecycle, probes, storage, logs, metrics and stormvm VMIs |
| `kube-proxy` | `cmd/kube-proxy`, `pkg/proxy` | iptables Services/Endpoints (polled every 5 s), TLS + ServiceAccount token to the apiserver (#145). No stormcos edition starts it: Cilium owns Services in the Cilium edition, flowsdn in the flowsdn edition (owner, #145; stormcos#265, flowsdn#292). It stays in the golden for anything that runs it later |
| CNI library | `pkg/cni` | Standard plugin invocation and networking helpers |

The executable defaults to **`--runtime native`**. The stormcos stage recipe
explicitly selects **`--runtime stormpump`**, connecting to the engine ring at
`/hostrun/stormpump.sock`. `--runtime cri` selects an external CRI v1 runtime
(CRI-O or containerd); it is optional. `--runtime vm` is the separate, incomplete
legacy microVM-Pod path, not the stormvm VMI manager.

For stormpump Pods, CNI configuration is checked per sandbox: the kubelet
passes the sandbox network namespace to CNI ADD and invokes DEL when the
sandbox stops. A Pod that finishes (`restartPolicy` Never or OnFailure, every
container terminated for good, Succeeded or Failed) has its sandbox stopped in
that pass, so its address goes back to the CNI at once; its container records,
status and logs stay until the Pod object is deleted (#137). What the CNI ADD returned is written on the Pod as
`k8s.v1.cni.cncf.io/network-status` (Multus's shape: `name` = the conflist, `interface`, `ips`, `mac`, `mtu`,
`default`, `dns`, `gateway`; plus `addresses` with prefixes, `routes`, `plugins`), with the start timing, once
Running is acknowledged (#131). An MTU the plugin does not report is read inside the pod's namespace. What the node owns for networking and what the CNI
(Cilium via network-operator) owns, and why a node with no gateway is never "the CNI is not up yet", are in
[node networking](docs/networking.md) (#32).
A Pod with no CNI configuration yet waits Pending ("network is not ready")
without a sandbox: the config is checked before one is acquired, and the
kubelet watches `--cni-conf-dir` (inotify), so the Pods waiting on it are
started as soon as the agent writes its conflist (10 s fallback where the
directory cannot be watched). A failed ADD (the agent not serving yet) is
retried on a backoff from its own first failure, 1 s at first (#148).
Host-network Pods, the CNI agent's own among them, bypass this. CRI delegates networking to the external runtime. **Node
Ready is not gated on the CNI** (owner, #3: "we want to get working early"): the node is Ready once the kubelet is up, a
pod-network pod waits for the config and then for a working ADD, and host-network pods start at once. A waiting pod says
why on its containers and in an Event: `NetworkNotReady` with no config, `FailedCreatePodSandBox` with the plugin's own
words when ADD fails (Cilium's "unable to connect to Cilium agent … Is the agent running?" with the agent gone). The
CNI ADD runs with the sandbox's netns and `K8S_POD_NAMESPACE`/`K8S_POD_NAME`/`K8S_POD_INFRA_CONTAINER_ID`, its address is
the pod's IP, a failed ADD is DEL'd and never leaves a pod addressless, and DEL runs at stop (verified live on server3,
11.91, #3).

**Every container runs on its own root** on stormpump (#104, the owner's rule): a copy-on-write clone of its
image's **sealed golden**, made when the container is created (engine volume `ctr-<container>`, attached over
ublk, mounted by PID 1 at `/run/stormpump/roots/<container>`), writable, and detached and deleted when the
container is removed; a restart is a new container with a fresh clone. Containers never share a root, and one
layer sits between the golden and what the workload writes. A pallet image's golden is the slab's
`<volume>.golden` (else the mounted volume's sealed parent; the volume is the one `rd.stormblock.mount=` mounts
at `/p/<path>`); a pulled image's is the fstemplate sbregistry sealed for it, so a pull clones nothing: it finds
the golden record (`GET /v1/goldens/{image}`, ready, `template_name`) and answers `template:<name>`. An image
this node's registry has no golden of is asked of the cluster (#79): the kubelet posts sbregistry's clone route,
the one that starts its cluster fetch, and its answer (503 "fetching it from the cluster", 404 "push the image")
is the pull's ErrImagePull, retried on the back-off; a clone it minted because the golden became ready meanwhile
is deleted (stormblock-registry#98 would make that a plain demand). Building an image from upstream on demand
(`POST /v1/goldens`) is not done: whether that endpoint stays is stormblock-registry#50. A failed
create or start removes the clone; roots no container holds (a kubelet that died mid-way) are deleted at
start, never one still attached. `readOnlyRootFilesystem` mounts the container's own clone read-only once the
engine can (stormpump#108); until then it is private and writable, and the kubelet says so. Per container
start this is a clone, an attach and an ext4 mount (~0.4–1.3 s on 11.91, faster as stormblock#327 and
stormpump#107 land).

**The image's config is applied under the pod spec** on stormpump (#98), as a CRI runtime does. When an image
resolves (a pallet or a pull), the kubelet asks sbregistry for its golden record (`GET /v1/goldens/{image}`,
2 s bound; a miss is asked again after 5 min) and keeps its `config`: `command` replaces the image's
`Entrypoint` and `args` its `Cmd` (`args` alone run after the `Entrypoint`); the image's `Env` is under the
pod's `env`; `WorkingDir` applies when `workingDir` is unset; `User` (a number or a name from the image's
`/etc/passwd`/`/etc/group`) when `runAsUser`/`runAsGroup` are unset. `runAsUser`, `runAsGroup` and
`runAsNonRoot` (container, else pod) are applied (every container ran as root before). `PATH` and `HOME`
default when neither side sets them; argv[0] is looked up on the container's `PATH`. The node's boot goldens
carry no config yet (stormblock-registry#58): a container of one whose argv would be empty or start with a flag
is refused, naming the image, instead of exec'ing the flag. emptyDir volumes are 0777, as upstream, so a
non-root uid can write them. Not yet: an image's declared `Volumes` (not recorded by the registry, #58) and
`fsGroup` (a claim's filesystem is root's).

**Kubernetes 1.36 posture** (#23, the control plane's since rustkube#37): `nodeInfo.kubeletVersion` is
`v1.36.0-rustkube+<apimachinery>` and `kubeProxyVersion` is not reported (1.33 removed it). A pod that asks for
an AppArmor profile other than `Unconfined` (pod or container `securityContext.appArmorProfile`, or the old
`container.apparmor.security.beta.kubernetes.io/<c>` annotation) is refused as upstream refuses it on a node that
cannot enforce one: `Failed`, reason `AppArmor`, a Warning Event, nothing started (no runtime here applies a
profile). A `gitRepo` volume waits with a message naming its 1.36 removal (clone from an init container into an
emptyDir). With `--runtime cri`, containerd before 2.0 stops the kubelet at startup (1.36 dropped containerd 1.7);
CRI-O is not checked. cgroups are v2 only; kube-proxy has no IPVS mode.

**Container status and image provenance** (#130). A container that ended carries upstream's
`state.terminated.reason` (the runtime's, e.g. `OOMKilled`, else `Completed` for exit 0, `Error` otherwise) and
`message`; once it is restarted (or while it backs off) its previous run is `lastState.terminated` (exit code,
reason, start and finish), forgotten with the pod. On stormpump a container's `imageID` is its image's
`sha256:`: the registry record's manifest digest for a pulled image, the release manifest's golden digest for a
pallet (else the reference, as before). Each container's provenance is on the pod as annotations, written with
the start timing and again after each restart: `storm.io/image-resolved.<c>` (when its golden was last checked
and cloned for it), `storm.io/instance.<c>` (the engine volume that is its own CoW root), `storm.io/golden.<c>`
and `storm.io/image-build.<c>` (JSON: OCI `created` and `org.opencontainers.image.*` labels once the registry
keeps them, stormblock-registry#100; a pallet's release `provenance`/`version`). A node service's mirror pod
carries its golden's digest as `imageID`, `storm.io/golden` and `storm.io/golden-provenance` from the release
manifest, and `hostIP`.

**Node allocatable and cgroups** (#24): allocatable is upstream's, capacity less `--system-reserved`,
`--kube-reserved` and the hard-eviction line (memory 100Mi, nodefs 10%); it was a fixed capacity − 256 MiB. With
`--runtime cri` each sandbox gets upstream's per-QoS `kubepods` cgroup parent, spelled for the runtime's cgroup driver
(its `RuntimeConfig`, else `--cgroup-driver`, else cgroupfs). stormpump keeps its own Pod groups (#57).

**An init container runs until it exits** (#126), as upstream: there is no fixed bound (it was 120 s, then
`DeadlineExceeded`), and its wait holds no worker (its exit is an event). The pod's `activeDeadlineSeconds`, counted
from its `startTime`, is the only deadline: past it the init container is stopped and reported `DeadlineExceeded`
and the pod is Failed. A running pod's `activeDeadlineSeconds` is not enforced yet (#185).

**Native sidecars** (`initContainers[].restartPolicy: Always`, #111) run as upstream runs them: started in their
init slot, the next init container goes once the sidecar has *started* (running, and its `startupProbe` passed if
it has one), not when it exits, and there is no init deadline for it. It then runs for the pod's life: probed like
an app container, restarted whatever the pod's `restartPolicy` (an exit before it started is a restart with
back-off, never a failed pod), reported in `initContainerStatuses` with `started`, `ready` and `restartCount`, and
counted in `Initialized` (once started) and `ContainersReady`/`Ready`. It does not count toward the phase: a
`Never`/`OnFailure` pod finishes with its app containers, and its sidecars are stopped then, last declared first.
On deletion the app containers stop first, then the sidecars in reverse order.

An overview deck is [docs/presentation.md](docs/presentation.md) (Marp).
See [configuration and defaults](docs/configuration.md), [ports and APIs](docs/api.md),
and [build and shipping](docs/BUILD.md). The binaries share upstream names;
this does **not** establish full upstream compatibility.

## The kubelet API (`:10250`)

HTTPS, bearer-token auth (a static token, or one the apiserver accepts via
TokenReview); `/healthz`, `/livez` and `/readyz` are open.

| Route | What it is |
|---|---|
| `GET /metrics`, `/metrics/cadvisor` | Prometheus metrics under upstream's names: the kubelet's own, and cAdvisor-shaped container and pod usage. See [docs/metrics.md](docs/metrics.md) |
| `GET /stats/summary` | Partial Summary API: container CPU/memory, their sums as node CPU/memory, node filesystem usage, and each pod's `network` (bytes, packets, errors, drops per interface, #131); not full metrics-server/HPA conformance |
| `GET /pods` | The pods this kubelet manages, including admitted pods still waiting to start (`Pending`, with the reason) |
| `GET /containerLogs/{ns}/{pod}/{container}` | What `kubectl logs` reads, by way of the apiserver proxy. A pod waiting to start answers `400 … is waiting to start: ContainerCreating (<reason>)`, as upstream does. A node service's mirror pod (`kube-system/<asset>-<node>`, container `<asset>`) reads the service's stormd log volume, found through the boot unit that mounts it at `/var/log/stormd` and seen under `/hostroot`. The current log covers every process stormd runs there, rotations included, merged in time order and marked `[<proc>]` when there is more than one. `--previous` is the newest `.failed.log`, and tail, since, timestamps, limit and follow all apply. A service not run by stormd (stormblock, registry, timesync) is read from PID 1's own file for its running incarnation: `/run/stormpump/assets.json` lists each asset's last five `runs` (stormpump#90), each naming its `w<id>.log` (and `w<id>.1.log`, the rotated start) under `/run/stormpump/logs`; follow polls the live file. `--previous` is then the newest ended run's file (#87). When there is no such file either (the service died before stormd wrote its volume, an older PID 1), the log is PID 1's record of its last exit: `last_output`, the last 20 lines its incarnation wrote (stormpump#51); `--previous` falls back to it too. PID 1's lines have no timestamps, so only tail and limit apply. With none of these, a 404 (400 for `--previous`) names the volume looked in and the last exit or refused start (#124) |
| `POST/GET /portForward/{ns}/{pod}` | `kubectl port-forward` (#56). Upgrades to SPDY/3.1 (`X-Stream-Protocol-Version: portforward.k8s.io`) or to a WebSocket tunnelling SPDY (`Sec-WebSocket-Protocol: SPDY/3.1+portforward.k8s.io`, what a newer kubectl tries first). Each forwarded connection is an `error` + `data` stream pair by `requestID`; the kubelet connects to `localhost:<port>` inside the pod's network namespace (IPv4, then IPv6; the node's for a hostNetwork pod) and splices; a failure is written on the error stream. Anything else is refused before the upgrade (404 unknown pod, 403 another protocol), which is what makes client-go fall back from WebSocket to SPDY |
| `POST/GET /exec/…`, `/attach/…` | 501 with the reason: stormpump cannot yet run a process inside a running container or give one a stdin (stormpump#103). Exec probes (`exec_sync`) wait on the same |
| `GET /vmConsole/{ns}/{name}/{door}` | A VM's `serial` or `vnc` console, answered by stormvm's console router mounted here |
| `GET /vmInstance/{address}` | VMI metadata by observed guest address (an index, not a scan); answered only when the cached VMI places the machine on this node (`status.nodeName`, a completed migration's target), so a moved machine or a reused address is 404 here (#119). A node address (host-network workload) answers only with the workload's ServiceAccount token in `X-Storm-Workload-Token`, for the pod it is bound to, if that pod runs here (#122). Cold cache returns 503 with Retry-After, and so does a machine here when the apiserver has not been heard from (renewed node Lease or VMI list) within `--metadata-max-staleness`, 40 s (#156); absent guest returns 404. stormimds keeps its own store today; which design wins is stormimds#12 |
| `DELETE /volumes/{ns}/{claim}` | Delete the stormblock clone behind a released claim |

The console and volume-release routes exist here because what they reach is on the node and the
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

The mounted router is told where this node's stormblock engine is (the
kubelet's `--stormblock`, default `http://127.0.0.1:9090`), so its `snapshot`
verb can take a VM's disks as one group snapshot; it finds the engine token
itself, in the same places the kubelet does. Its verbs are on `:10250` too (#94):
`PUT /vmVerb/{ns}/{name}/{verb}`, for `pause`, `unpause`, `softreboot`, `reset`,
`status`, `freeze`, `thaw` (KubeVirt's `unfreeze` is accepted for it) and
`snapshot`, its query (`name`, `quiesce`) forwarded without any `token`, behind this
server's auth exactly as the doors are. `migrate`/`receive` are not offered: a
migration is driven by the VMI's status (#40). The apiserver's
`subresources.kubevirt.io` verb proxy is rustkube#141.

`stormvm serve` mounts the same router on `127.0.0.1:9095`, and it ships: stormcos
runs it on every node as a service golden (`stormvm`, port 9095, loopback only;
stormcos `deploy/build-goldens.sh`, `serve --addr 127.0.0.1:9095 --run-dir
/run/stormvm`), and stormconsole's VM plugin reaches the serial and VNC consoles
through it (`[vm] url = http://127.0.0.1:9095`, stormconsole
`crates/plugins/vm/src/console.rs`). It reads the same `/run/stormvm`
registrations this kubelet writes, so both doors answer for the same machines.
From outside the node the apiserver's `vmConsole` proxy to `:10250` (above) is the
way in; `:9095` is the node-local one (#123, stormcos#65).

`DELETE /volumes` is what makes `reclaimPolicy: Delete` finish instead of
leaking: `204` when the clone is gone or was never there, `409` while a pod
on this node still has the claim, `503` when the node cannot establish that
it is unused. It never answers `204` on a guess — the volume name is derived
from the claim's, so a PV deleted over a surviving clone would let a later
claim of the same name in the same namespace adopt the previous tenant's
data.

## The node's services as pods

When PID 1's asset table (`/run/stormpump/assets.json`) changes, and when a mirror pod is edited or deleted,
the kubelet mirrors each asset as a
read-only pod, `kube-system/<asset>-<node>` (labels `storm.io/asset`, `storm.io/component=node-service`).
A running asset's pod is Running and Ready, and a stopped one is Failed. **Ready means it answers** (#96): for a
host-network service the kubelet reads the liveness URL its golden's stormd config declares
(`[process.liveness] type = "http"`, `url = …`; the golden is the boot unit's `root` volume, under `/hostroot`)
and asks it every 10 s (2 s timeout). Three failures in a row make the pod Running but not ready: container
`ready: false`, `Ready`/`ContainersReady` False, reason `Unhealthy` with the failure, and an `Unhealthy` Warning
Event; one answer makes it ready again. A service with no declared HTTP liveness, or not on the host network,
is Ready while running, as before. **A service that has exited says how** (#82): PID 1 keeps each asset's last
exit in assets.json (`last_exit_code` or `last_exit_signal`, `last_exit`, the last 20 lines of `last_output`;
stormpump#51), and the mirror's container carries it as `lastState.terminated` (`exitCode`, 128 + the signal
when a signal ended it, `signal`, reason `Error`, `message` = the output's tail within upstream's 80 lines /
4 KiB). A service that stays down has the same on `state.terminated`; one that never exited invents no exit
code. **Its lifecycle is Events on the mirror pod** (#50): `Started` (with the service's own start time), `Stopped`,
`Failed` (a non-zero exit or a signal, Warning), `BackOff` (PID 1 restarted it), the last three ending with the exit
and that tail. First sight is judged against the mirror pod the API has, so a service started since it was last
written is announced and a kubelet restart re-announces nothing. For a service run by stormd the
output is stormd's own until stormd#29. A mirror whose asset is not in the
table on this boot (its unit was not started) becomes Pending, with its container waiting `NotStarted`, no
`startTime`, and one Warning Event. It used to keep the previous boot's Running status. Mirrors are never
deleted by the kubelet, and `kubectl logs` on them is described above.

## Storage

The **built-in stormblock PVC driver** uses stormblock and sbregistry blanks.
A claim is a copy-on-write clone of a sealed, preformatted size-class blank:
no per-claim mkfs, data copy or CSI. Claims are cloned and attached over ublk by the
node itself (`pkg/kubelet/src/storage.rs`), through the node's stormblock
engine (`--stormblock`, default `http://127.0.0.1:9090`). The engine requires
its own token, and the kubelet presents it on every call
(`pkg/kubelet/src/engine.rs`). It reads the token from `$STORMBLOCK_API_TOKEN`,
or else from the file at `$STORMBLOCK_TOKEN_FILE` (default
`/run/stormblock/engine/api_token`, set by stormcos), then
`/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`. The engine
mints the token when it starts, and the kubelet may start first, so while no
token is found the kubelet looks again on every call. After a 401 it reads the
token again and retries once if the token changed. A change the engine refuses to the node token (a destructive verb under stormblock#274's admin gate, such as deleting a broken template) is sent once more with the admin credential from `$STORMBLOCK_ADMIN_TOKEN` or the file at `$STORMBLOCK_ADMIN_TOKEN_FILE` (the engine's admin token, or a ServiceAccount token its SubjectAccessReview allows), read at each use (#105). Every other StorageClass goes
through its CSI driver. The kubelet registers node plugins from
`/var/lib/kubelet/plugins_registry`, writes `CSINode`, and stages and
publishes volumes. It will not give a pod a volume whose mount has not reached
the node. See [docs/csi.md](docs/csi.md). Each pod mount's `mountPropagation` reaches stormpump's spec (#81,
stormpump#35): HostToContainer as `rslave`, Bidirectional (privileged containers only) as `rshared`.
Real-driver mount/restart/delete acceptance remains #52.

**A pod whose volumes are not ready waits, and says why.** It is `Pending`,
every container is `waiting: ContainerCreating` with the reason (for example
`waiting for volume pvc-default-data: template pvc-ext4j-1048576m
awaiting_format`), and `describe` shows a `FailedMount` Event. After 5 minutes
the reason says it timed out, and the kubelet keeps retrying. As upstream, a
mount timeout does not fail the pod, because the volume may still come.
The same holds for the files the kubelet writes for a pod (emptyDir, configMap,
secret and projected directories, the ServiceAccount token, resolv.conf and the
container log directories): they are written before the sandbox, and a write the
node refuses (a full disk, ENOSPC) leaves the pod waiting with the path and the
errno, retried until there is room (#129).

**A pod that cannot start says which container and why, as upstream does** (#133). An image that will not pull
is a wait: the containers are `waiting: ErrImagePull`, then `ImagePullBackOff`, and the pull is asked again after a
back-off of as long as it has waited (10 s to 5 min). A container that will not be created waits as
`CreateContainerError`; one that will not start waits as `RunContainerError`, except under `restartPolicy: Never`,
where the pod is Failed with that container terminated `StartError`, exit 128. A start that got past its sandbox
is torn down before the next try, and the try waits its back-off whatever wakes the pod. Anything else that ends a
start (an init container that failed) leaves the pod Failed with its init statuses and its containers
`PodInitializing`, never with no container state, and `logs` answers why (400), not "not found".

The image ships sealed blanks for the common classes (`pvc-ext4j-<MiB>m`,
sbregistry's naming). A class with no blank is minted on its first claim:
stormblock formats and seals it once (`role: data`). The mint runs in
the background without an inline wait. Completion signals the Pod and VM
queues immediately; waiting claims check the template's state and proceed
when it is `ready`. A large format does not hold the reconciliation pass.
A clone stormblock refuses leaves the claim waiting with stormblock's own
answer (status and `error`, or "no answer within 60 s") in the pod's
FailedMount Event and container message. A refusal that means the template
itself is broken (its sealed volume missing or not sealed, or no sealed
snapshot recorded) deletes the template and mints it again, so the next
claim clones a sound one rather than meeting the same refusal forever (#140).
**The claim itself says so** (#70), with external-provisioner's reasons: a Normal `Provisioning` while its
class's blank is minted or formats, a Warning `ProvisioningFailed` with stormblock's words when a mint or a
clone is refused, and a Normal `ProvisioningSucceeded` once it is cloned (`kubectl describe pvc`; repeats
aggregate).

The ladder is **1Mi, 16Mi, 64Mi, 256Mi, 1Gi, 4Gi, 16Gi, 64Gi, 256Gi, 1Ti,
4Ti, 16Ti, 64Ti, 256Ti, 1Pi** (#67); requests round up, and requests above 1Pi
are refused. The rounded class is the volume ceiling. Each class names its
filesystem (`storage.rs` `SIZE_CLASSES`); all are ext4 (owner, #67; stormcos#91
found nothing that forces XFS). **64Ti, 256Ti and 1Pi are raw block only** for
now: stormblock's formatter (mkfs-ext4 v3.0.0) needs ~5 GiB to format 64 TiB
(mkfs.ext4.rs#10) and wraps the inode count at 256 TiB (mkfs.ext4.rs#9), so a
filesystem claim rounding to them waits with that reason and the hint to use
`volumeMode: Block` (stormblock#289 tracks carrying the fixed formatter).

**Raw block claims** (`volumeMode: Block`): a plain thin stormblock volume of
the class's size (`POST /api/v1/volumes`, `role: data`; no template, no mkfs),
attached over ublk like any claim, and bound at the container's
`volumeDevices[].devicePath` as the device itself (no filesystem). A Block claim
named in `volumeMounts`, or a Filesystem claim in `volumeDevices`, keeps the pod
waiting with the reason. The PV's `volumeMode` follows the claim when the node writes it; the control
plane's provisioner writes Filesystem until rustkube#201. A VM disk
naming a Block claim gets the raw volume. Raw block from third-party CSI drivers
is refused (`docs/csi.md`). Sizes go to stormblock as `<MiB>M`, since it has no
`P` suffix.

**Room on the data slabs** (#62; policy #108, `pkg/kubelet/src/capacity.rs`).
A claim is a thin clone, so nothing physical stops a slab filling under many
large claims. So a claim is charged its **full class size** when its volume is
made (a 600Gi claim takes 1 TiB of room: the class is what it can write),
clones keep the class size, and the overcommit ratio is 1.0:

- **Room** = `min(data total × --storage-overcommit − committed, data free) −
  reserve`, where `committed` is the virtual size of every writable data-role
  volume (claims, node service volumes, VM disks; not goldens, sealed volumes or
  the class blanks) and `reserve` is `--storage-reserve-percent` (5) of the data
  slabs.
- **Published:** `CSIStorageCapacity` `kube-system/stormblock-<node>` (class
  `stormblock`, topology `kubernetes.io/hostname`), `capacity` = the room and
  `maximumVolumeSize` = the largest class that fits, on engine volume changes
  and every 60 s. rustkube's scheduler compares a claim's request with it once
  the CSIDriver says `storageCapacity: true` (stormcos#151).
- **Refused at provision** too (static pods and `spec.nodeName` pods meet no
  scheduler): a new volume whose class does not fit leaves the pod waiting with
  `not enough room on this node's data slabs for the <class> class …` and the
  numbers, and nothing is made. The check and the create are serialized. An
  engine with no slab API is not checked.
- **Alert:** gauges `kubelet_stormblock_data_bytes{kind}` and
  `kubelet_stormblock_data_used_percent`; past `--storage-alert-percent` (85)
  written, one Warning `SlabFilling` Event on each of this node's stormblock
  PVs per crossing, and a log line.
See [service volume objects](docs/node-volumes.md) for the PV/PVC mirror, and for each stormblock PV's placement
(drives, shelf/bay, RAID partners, health and rebuild, with Events on change; #60).

### Virtual machine disks

With `--runtime=stormpump` the kubelet also runs the VirtualMachineInstances
assigned to its node (`pkg/kubelet/src/vm_manager.rs`). Assigned is rustkube's
scheduler's rule: `status.nodeName` (what the scheduler writes) when it is set, else
`spec.nodeName` (a VMI placed by hand, which the scheduler leaves alone). The kubelet lists
and watches both, and writes `status.nodeName` on a hand-placed VMI when it takes it (#85).
Each volume of a VMI becomes a stormblock volume attached here:

| VMI volume | Disk | Deleted with the VM |
|---|---|---|
| `dataVolume` / `containerDisk` | `<ns>.<vmi>-<disk>`, a clone of the named golden made on the first start and reattached on every later one | yes |
| `cloudInitNoCloud` | `<ns>.<vmi>-<disk>`, a generated `cidata` seed, made again on each start (the old one is replaced) | yes |
| `emptyDisk: {capacity}` | a blank volume `<ns>.<vmi>-<disk>`, reused if it already exists | yes |
| `persistentVolumeClaim: {claimName}` | the claim's volume, resolved exactly as for a pod (a bound claim uses its volume, an unbound `stormblock` claim is provisioned) | no, it belongs to the claim |

**A stop only detaches (#75).** A VirtualMachine restart is a new VMI, and it
finds its disks by name: the root is what the guest wrote, and the golden is
needed only the first time. A listing that fails is a failed start, never
"make a new one".

**"The VM" is the VirtualMachine.** Each disk the machine makes carries a
stormblock owner (`owner {kind, namespace, name, uid}`, stormblock#115). The
owner is the VMI's VirtualMachine, or the VMI itself when there is none.
The kubelet sweeps the engine's volumes when stormblock's volume watch or a
VM/VMI watch reports a change, at most once a minute (events inside that are
deferred, not dropped). A disk is deleted when
its owner is gone for good: a 404, a new object under the same name (another
uid), or a `deletionTimestamp`. Any other answer keeps it, and so does a disk
in use or attached to a machine here.

**Same name, new VM:** a disk left by an earlier VM of the same name (another
owner uid) is not booted. The start waits, with the reason on the VMI, until
the sweep removes it.

**Keeping disks:** `storm.io/retain-disks: "true"` on the VMI or its
VirtualMachine gives its disks no owner. The sweep never deletes them, and a
VM made again under that name reattaches them. Disks of a machine adopted
after a kubelet restart are given their owner then.

Stopping the VM never deletes a claim's disk; its PVC reclaim policy owns deletion.

A claim's disk waits, with the reason on the VMI, while the claim is unbound,
belongs to another StorageClass, or is in use by a pod on this node. A pod
mounts the filesystem and a VM writes the raw device, so the two must not
share it. The reverse holds too: admission reserves a VMI's claims exclusively,
so a Pod wanting a claim a VM here holds is not started. It waits Pending, its
containers `ContainerCreating` with "claim ns/c is a disk of
VirtualMachineInstance ns/vm on this node" and a FailedMount Event, and
starts once the VM lets the claim go (#80).

### Virtual machine lifecycle

- **A VM-capable node says so (#65).** With the stormpump engine (the only
  runtime that starts VMs), the kubelet checks for KVM on each heartbeat: the
  kernel's `/sys/class/misc/kvm`, or a `/dev/kvm` (its own or `/hostroot`'s)
  that opens read-write. While it is there the Node carries
  `devices.kubevirt.io/kvm: 1k` in capacity and allocatable (KubeVirt's device
  plugin's resource) and the labels `storm.io/kvm=true` and
  `kubevirt.io/schedulable=true`. Without it the resource is gone, `storm.io/kvm`
  is removed and `kubevirt.io/schedulable` is `false`. A node with the native
  runtime reports none of these. A test that `requires: [kvm]` checks
  `kubectl get nodes -l storm.io/kvm=true`.
- **A VMI being deleted stops its machine.** While a machine runs, its VMI
  carries the finalizer `storm.io/vm`, so the deletion completes only once the
  machine is gone. The stop is ACPI with a 30 s grace, then a kill, then the
  disks are detached (never deleted: see above).
- **A VM outlives a kubelet restart** (the engine supervises it), so the kubelet
  records each one where a restarted kubelet finds it: the machine's
  registration, `/run/stormvm/<ns>/<name>/vm.json`, with the engine's workload
  handle and its disks. Whenever the VMI set is read (the watch's LIST and
  each change), a registered machine the kubelet does not know is adopted
  when its VMI still wants it, and stopped when not.
- A machine started by an older kubelet has no handle recorded. If its VMI is
  gone, it is stopped through its own control socket: ACPI, then `quit`.
- A failed VMI list is skipped, not read as "no machines". Reading it that way
  stopped every VM on the node.
- **A failed start is retried**, with backoff from 10 s doubling to 5 min.
  - The VMI stays Pending with reason `FailedStart` and a message giving the
    attempt, the wait and the error. Each attempt also has a Warning Event.
  - A new spec (`metadata.generation`) is tried at once.
  - A missing golden is not a failure: it waits, and is tried again on the
    waiting-start deadline (a quarter of the wait so far, 1–30 s).
    So does one still being imported (stormblock answers the clone `409 … not
    sealed`): "waiting for golden <g> (importing)", a Normal `Waiting` Event,
    no FailedStart backoff.
  - A start gives up, and the VMI goes Failed, only when its VirtualMachine's
    `runStrategy` is `Once` or `Manual`.
  - A failed start cleans up volumes created in that attempt; reused disks
    are kept.
- Restarting a `running: true` VM whose instance ended is the VM controller's
  job (rustkube#104).
- **The pod network: a sandbox per VMI** (#88, stormvm#16). A VMI with any NIC
  on `networks: [{pod: {}}]` gets what a pod sandbox gets
  (`pkg/kubelet/src/vm_network.rs`):
  - a network namespace from stormpump (`sandbox_acquire`, the pod profile)
    and a CNI ADD into it, as container `vm-<uid>` for the VMI's namespace,
    name and uid, before the deposit window;
  - its NICs realised there (`stormvm_net::realise(p, Some(netns), …)`): the
    bridge binding (`pod: {}` with `bridge: {}` or no binding named) moves the
    pod's address and MAC off the CNI's interface onto a bridge with the tap,
    so the guest holds the pod IP;
  - the hypervisor spawned in the sandbox (the engine joins a machine to it as
    it joins a container), so `masquerade: {}` NATs out of the pod;
  - a DHCP responder per bridged NIC (`serve_dhcp`), answering only the guest's
    MAC with the pod IP, gateway, the cluster DNS, the ClusterFirst search list
    and the VMI's hostname, held for the machine's life;
  - `status.interfaces[]` has the pod IP from the start and the binding
    stormvm chose (`bridge`, `masquerade`, `passt`, `host-bridge`); the agent
    and neighbour table do not replace a pod NIC's address.
  - The record (sandbox handle, netns, CNI identity, IP, leases) is written to
    `/run/rustkube-node/vm-network/<uid>.json` before the ADD. Teardown is CNI
    DEL and the sandbox release, retried until done: when the machine ends,
    when a start fails after the ADD, and at deletion. A restarted kubelet
    restarts DHCP for adopted machines and releases records with no machine.
  - No CNI configured yet (or `--no-cni`) keeps the VMI Pending with the reason,
    as for a pod; a failed ADD runs DEL and waits.
  - **Its launcher Pod** (owner's choice on #88; made by rustkube's VM
    controller, rustkube#203): a Pod labelled `kubevirt.io: virt-launcher` and
    `kubevirt.io/created-by: <vmi uid>`, owned by the VMI, on this node (during
    a migration the target node has its own; each node takes the one on it,
    #152). The
    VMI waits for it. The CNI ADD names that Pod (its namespace, name and uid),
    so Cilium labels the endpoint from it (the VMI's labels, so NetworkPolicy
    applies), and the kubelet writes its status: `Running` with `podIP` and
    Ready at start, `Succeeded`/`Failed` at the end, so Services select the VM.
    The pod manager never runs it. A terminating launcher Pod with no machine
    of its VMI here is confirmed deleted. Every VMI has its own sandbox and
    launcher Pod, so a namespace holds any number of VMs.
- **A guest's address** goes to `status.interfaces[].ipAddress` / `ipAddresses`
  from three sources, in this order:
  - **The tap watcher:** a NIC on one of the node's bridges (`host`,
    `bridged:<name>`, `storm.io/bridge`) is watched from before the spawn
    (stormvm-net `snoop_tap`). The guest's DHCP, DHCPv6 or SLAAC is seen on
    the tap and written to the VMI straight away, not on the next sync.
  - **The QEMU guest agent**, when the guest runs one.
  - **The node's neighbour table** (`/proc/net/arp`).
- **SSH keys: `spec.accessCredentials`.**
  - **`noCloud` / `configDrive` Secrets** are read at start, and their keys go
    into the seed's meta-data `public-keys`, never user-data. That needs a
    `cloudInitNoCloud` volume. A missing Secret doesn't stop the machine.
  - **`qemuGuestAgent: {users}` Secrets** are applied through the guest agent
    once it answers. They're applied again whenever the Secret's keys change,
    with `reset`, so the Secret is the truth. A missing or empty Secret leaves
    the guest's keys alone rather than locking it out.
  - **Status:** the VMI carries `AccessCredentialsSynchronized`, with every
    reason when it is False: a missing Secret, no seed, or an agent not
    answering yet.
- **Snapshots: `VirtualMachineSnapshot`** (`snapshot.kubevirt.io/v1beta1`).
  - **Whose:** the node whose stormblock holds the VM's volumes, the one it
    runs on (`spec.source` is a `VirtualMachine` or a `VirtualMachineInstance`).
    That node marks the object `storm.io/snapshot-node` (against its
    resourceVersion, so exactly one node takes it), sets `InProgress`, and
    takes it in the background.
  - **How:** stormvm freezes the guest (through its agent, when it has one),
    pauses it, takes one stormblock group snapshot of every volume, named
    `<ns>.<vm>.<snapshot>`, then unpauses and thaws.
  - **Status:** `Succeeded` + `readyToUse`, or `Failed` with the error.
    `virtualMachineSnapshotContentName` is the stormblock group id, and it
    also carries `sourceUID`, `indications` and an Event on the object.
    `failureDeadline` (default 5 min) counts from creation.
  - **Restart:** a kubelet restarted mid-take takes it again. stormblock
    answers a name it has seen with what it made then. A snapshot whose VM
    has left the node is Failed.
  - **Needs the CRDs** (stormcos#170): without them the list is a 404 and the
    kubelet does nothing.
  - A taken snapshot also records its disks (`storm.io/snapshot-disks`,
    `{"<disk>": "<volume id>"}`, from the registration, written with the claim).
- **Restores: `VirtualMachineRestore`** (#53; owner's option A, #109;
  `pkg/kubelet/src/vm_restore.rs`). Served by the node that took the snapshot
  (its stormblock holds the group):
  - waits for the snapshot to be `Succeeded` and the VM stopped (no VMI),
    saying so in the `Progressing` condition;
  - for every disk in the snapshot's disk map except a cloud-init one (made
    again at every start), a **new** volume from that disk's member,
    `<ns>.<vm>-<disk>-restore-<restore>`; the old disks are left as they are;
  - for each, a bound PVC `<vm>-<disk>-restore-<restore>` (`volumeMode:
    Block`, label `storm.io/restored-from`) and its PV (class `stormblock`,
    pinned to this node, reclaim Delete). The VM's placement follows the PV;
  - the VirtualMachine's `spec.template.spec.volumes[<disk>]` becomes that
    `persistentVolumeClaim`;
  - `status.complete` with `restores` (disk, claim, snapshot) and a
    `VirtualMachineRestoreComplete` Event. An error (snapshot Failed, no disk
    map, the group gone) is written once, Ready False, with a Warning Event.
  - Every step is find-or-create, so an interrupted restore finishes on the
    next pass. A snapshot taken before the disk map was recorded cannot be
    restored: take a new one.
  - A restored disk is made on this node; pulling a RAID twin from another
    node (the owner's note on #109) is not done.

## Tests on a node

`test/` is the test container, per stormcentral's `docs/test-standard.md`. It
is a standalone crate (its own workspace and `Cargo.lock`). `test/build.sh`
builds its static binary on the build box, and `test/Containerfile` copies it
into a scratch image. The image is started as `/test <suite>` and run by
stormcentral as a Job (`test/rustkube-node-test.yaml`):

    stormcentral test run rustkube-node short --tag <machine> --url http://stormcentral.g8.lo

Every pod a suite makes is pinned to the test node (`spec.nodeName`): the
kubelet is under test, not the scheduler.

- **short** (< 2 min, the release gate, #61): needs nothing a machine may lack.
  - `node-ready`: the Node is Ready, its heartbeat is under 120 s old, its
    kubelet reports a version, and no pressure condition is True.
  - `pod-runs`: a pod runs to Succeeded with exit code 0 and a pod IP, and its
    log reads back through the apiserver.
  - `pod-exit-code`: a container exiting 3 leaves its pod Failed with exit code 3.
  - `pod-delete`: a running pod, deleted, is gone within 30 s.
- **medium** (< 30 min): pod features and failure paths (#61), which run with
  or without the storage class:
  - `pod-restart-on-failure`: `OnFailure`, failing once: Succeeded with
    restartCount 1, and `log?previous=true` is the failed run.
  - `pod-init-first`: an init container's file (emptyDir) is there for the main one.
  - `pod-config`: a ConfigMap volume, `configMapKeyRef` and `fieldRef` env.
  - `pod-missing-image`: an image no registry has waits as ErrImagePull or
    ImagePullBackOff and never runs.

  And claims of the built-in `stormblock` class (#64):
  - It makes a claim at every filesystem size class (1Mi … 16Ti) and at
    arbitrary sizes: 1 byte, 1Mi+1, 17Mi, 1500M, 3.5Gi and 600Gi, and raw block
    claims of 1Mi, 20Ti and 1Pi (`volumeDevices`; the device must be exactly its
    class). The claims run in parallel.
  - For each one, a pod writes and reads back 64 KiB and checks that `df` is
    within the class the request rounds to. The test then checks that the claim
    is Bound with that class as its status capacity, and deletes the pod and
    the claim. The PV must be reclaimed.
  - A 2Pi claim must be refused with the reason, and never bound; a 20Ti
    filesystem claim (64Ti class, no ext4 blank yet) must wait naming
    `volumeMode: Block`.
  - Every case has `RUSTKUBE_NODE_TEST_MINT_BUDGET` (default 1200 s), because a
    class may be minted on first use.
  - Overcommit (#62): a Block claim one class above the test node's published
    `maximumVolumeSize`, its pod pinned to the node, must wait with "not enough
    room" (or the scheduler's capacity reason) and never bind; skip when the
    node can still take the largest class.
  - The node's own volumes (#59): every claim mirrored for the test node is
    `kube-system/<volume>-<node>`, Bound to `storm-<volume>-<node>`, whose
    `claimRef` names it by uid, with the same kind and component labels; one
    claim is deleted and must come back with its PV naming the new uid.
- **long** (the night window, on a pve VM): waves of pods, each 80% of the
  node's free pod slots (`RUSTKUBE_NODE_TEST_WAVE_MAX`, default 500), every
  fourth with a 16Mi built-in claim written by an init container. Per wave
  (`wave-<n>`, with `pods`, `p50_ms`, `p95_ms`, `drain_ms`, `left` as fields):
  each pod's start (create → seen Running, 1 s resolution), a 30 s hold and a
  log read, then the drain. A wave fails on anything left after its drain
  (pods, claims, PVs) or a p95 start over twice the first wave's plus 2 s.
  Waves repeat until the suite's time is nearly out (`RUSTKUBE_NODE_TEST_WAVES`
  caps them for a hand run). VM waves are stormcos_qa's `vm-waves`.

The same image is the workload pods' program (`/test sized <path> <seed>
<bytes> <lo> <hi>`, `/test echo …`, `sleep`, `exit`, `fail-once`,
`write-file`, `expect-file`, `expect-env`), so a run pulls nothing else. Check it builds with
`sc-build 'cd test && cargo test --locked && cargo build --release --locked'`.

Required test inputs are listed in [configuration](docs/configuration.md#test-container).
The image the workload pods run is the Job's own (#97), and `STORM_NODE` (an
address) is resolved to its Node. No successful live run of any suite is
claimed yet (#64, #61).

## Relationship to rustkube

- **Control plane** (kube-apiserver, controller-manager, scheduler, fastetcd)
  lives in [rustkube](https://github.com/glennswest/rustkube).
- Cluster DNS is a platform service. Pod DNS configuration is built in
  `pod_manager.rs`; this repository does not run a DNS server.
- Shared types come from rustkube's `apimachinery` crate, as a **git
  dependency pinned to a commit** (`Cargo.toml`, `[workspace.dependencies]`):
  ```toml
  apimachinery = { git = "https://github.com/glennswest/rustkube", rev = "<commit>" }
  ```
  This checkout builds on its own. Moving to a newer rustkube means changing
  `rev` and running `cargo update -p apimachinery`.

## Build and ship

From a clean checkout, commit and push first, then:

```bash
sc-build 'cargo build --locked && cargo test --locked'
```

The build service fetches the pushed commit into an isolated scratch build on
dev and deletes it afterwards. Do not build on the session VM or create a
persistent checkout on dev.

A stormcos release consumes a **stage golden**, containing stormd, its config
and the binaries. After completed release work passes validation, request it
through `stormcentral component stage rustkube-node --url http://stormcentral.g8.lo`.
Do not use `component build`: it produces the bin-only artifact that cannot
start this service. The repository's own golden builder (`scripts/build-golden.sh`)
was removed (#51). See [BUILD.md](docs/BUILD.md) for the complete workflow.

## License

Apache-2.0
