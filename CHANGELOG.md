# Changelog

## 2026-10-08

- **feat:** Per-container logs at upstream parity (#216). Every container run gets its own `<N>.log` (the next number
  on disk, at least the restart count): init containers and native sidecars no longer reuse `0.log`, so `--previous`
  works for a retried init, a restarted sidecar and across a kubelet restart. The current run and five previous are
  kept, older runs pruned when a new one starts. Rotation as upstream's ContainerLogManager: `--container-log-max-size`
  (10Mi) and `--container-log-max-files` (5), every 10 s, `<N>.log.<YYYYMMDD-HHMMSS>`, older rotations gzip'd, oldest
  deleted; copy-truncate until stormpump offers a reopen. Timestamps and stream per line need stormpump to write
  CRI-format logs: stormpump#129.
- **feat:** Every remote call retries (#211, code review). New `pkg/retry` crate, shared by the kubelet, kube-proxy
  and the test container: bounded retry with jittered exponential backoff and a whole-operation deadline per
  dependency (`API`, `ENGINE`, `REGISTRY`, `PEER`, `LOCAL`), `Retry-After` honoured, no retry on a real answer
  (4xx other than 408/429, 501), a POST repeated only when it never left or on 429 unless the caller marks it
  repeatable (named creates, TokenReview/TokenRequest), every retried call's attempts logged, failures classified
  infrastructure vs refused. Routed through it: every apiserver call (kubelet, kube-proxy, test container), the
  stormblock engine client, the registry, stormdrive, CSI node RPCs (idempotent by the spec), stormvm's restore
  client and the snapshot take. Probes, CRI, the ring and CNI exec say why they are not retried. Every call site
  and its before/after: docs/retries.md.
- **chore:** #210 (git dependencies pinned by `rev`, stormcentral#571) tried and reverted (8774f06, 4f3ef17).
  Cargo refuses a `[patch]` that points at a dependency's own URL, so the transitive stormpump/stormcast
  references can only be pinned in stormvm and stormpump (stormvm#82, stormpump#123). No change to the build.
- **fix:** A class blank still being minted is no longer charged against the data slabs (#209). stormblock formats
  a template on `fstemplate-<name>-raw` (unsealed until the seal) and seals it as `fstemplate-<fs>-<name>`; only the
  bare blank name and sealed volumes were treated as sources, so a 1 TiB blank being formatted counted 1 TiB and
  the 1Ti claim that asked for it was refused "not enough room" on a 1.8 TB slab. Every `fstemplate-` volume is
  now a source.
- **test:** `medium`: a size case whose class is above the node's published `maximumVolumeSize` is checked as a
  refusal ("not enough room", pod pinned to the node) instead of waiting the whole mint budget for room that cannot
  appear; four such cases on a 1.8 TB node held the slots for 20 minutes and the 1Ti mint started late (#209).
- **fix:** A node service's mirror pod takes its `startTime` from PID 1's `started_secs` (`CLOCK_BOOTTIME`) against
  `/proc/uptime`, not from `age_secs` (#193): since stormpump#67 assets.json is written only when the table
  changes, so `age_secs` goes stale between writes and the start time would drift and be rewritten every pass.
  `age_secs` stays the fallback for a PID 1 without `started_secs`.
- **feat:** An image's declared `Volumes` are made (#172): read from the registry's golden config, each path no pod
  mount covers becomes a directory in the container's own root, CRI-O's default `mkdir` behaviour (the root is the
  container's private writable clone since #104). Made component by component, never through a symlink.
- **feat:** Container status and image provenance for stormconsole#69/#75 (#130): `lastState.terminated` (each
  container's previous run, kept from its exit through its restart or back-off), `state.terminated.reason`
  (runtime's, else `Completed`/`Error`) and `message`; on stormpump a real `imageID` (`sha256:` from the registry
  record or the release manifest); per-container pod annotations `storm.io/image-resolved.<c>`,
  `storm.io/instance.<c>` (its CoW root volume), `storm.io/golden.<c>`, `storm.io/image-build.<c>`, written at start
  and after each restart; mirror pods get `imageID`, `storm.io/golden`, `storm.io/golden-provenance` and `hostIP`.
  OCI build labels need stormblock-registry#100 (the registry keeps no `created`/`Labels` yet).
- **feat:** Pod network detail for stormconsole#69 (#131). (1) Packets, errors and drops each way beside the bytes:
  `container_network_{receive,transmit}_{packets,errors,packets_dropped}_total` in `/metrics/cadvisor`, and each
  pod's `network` in `/stats/summary` (upstream's shape plus packets/drops). (2) The CNI ADD's result written as
  `k8s.v1.cni.cncf.io/network-status` (Multus's shape, plus prefixes, routes and the plugin chain; the MTU read in
  the pod's namespace when the plugin does not report it); the CNI invoker now records the network and plugins on
  its result. (3) `containerLogs?previous=N`, the run N back (`true`/`1` stays the previous one), for regular pods
  and node-service mirror pods (stormd's failed runs, PID 1's last five).
- **fix:** `pods/log` no longer drops the first three words of every plain line with three or more spaces (#136): a
  line is taken apart only when its first field is an RFC 3339 time and the next two are CRI's (`stdout|stderr`,
  `P|F`) or stormd's (stream, severity); stormpump's lines, which carry neither, come back whole, so test runners'
  spaced JSON results (stormcos_qa, stormlb, stormpump, stormraid) read again. CRI partial lines are joined to the
  rest, and a last line with no newline is returned (a plain read; `-f` once the pod is gone).
- **fix:** Start timing's `scheduled` phase is timed from rustkube's `storm.io/scheduled-at` annotation (the bind time
  in microseconds, rustkube#190) when the pod carries it, and kept in microseconds (`scheduled=4.3ms`), #135. The
  `PodScheduled` transition and `creationTimestamp` (whole seconds, up to 999 ms of truncation: pvetest1's
  "growing" 156→393 ms) remain the fallbacks.
- **feat:** `--system-reserved`, `--kube-reserved` and `--cgroup-driver`, upstream's flags (#24; `--max-pods` was #165,
  `--node-labels`/`--register-with-taints` earlier). Allocatable is now upstream's: capacity less both reservations
  and the hard-eviction line (memory 100Mi, nodefs 10%), CPU in millicores when fractional. **Changed default:** with
  no reservation, allocatable memory is capacity − 100Mi (it was a fixed capacity − 256Mi). With `--runtime cri`
  the cgroup driver is the runtime's (`RuntimeConfig`; a disagreeing flag is ignored with a warning), else the flag,
  else cgroupfs, and every sandbox carries upstream's per-QoS `kubepods` cgroup parent in that spelling (it sent none).
- **docs:** `docs/networking.md`, the node/CNI networking contract (#32): what the node's boot (`ip=dhcp`: address,
  default route, DNS) and the kubelet (registration, sandboxes, CNI ADD/DEL) own, what network-operator and Cilium own
  (install, conflist, Pod routes, masquerade, Services, `NetworkUnavailable`), the pre-CNI sequence under #3's
  decision (Ready at once, host-network Pods start, Pod-network Pods wait on the conflist), and how to tell pre-CNI
  from broken (a node without a gateway is a DHCP/lab matter, never the CNI). Linked from the README.
- **feat:** With `--runtime cri` the kubelet follows the runtime's `GetContainerEvents` stream (#116, upstream's
  evented PLEG) instead of looking at every Pod each `sync_interval`: an event wakes the Pod named by its sandbox
  status (else the Pod holding that container id), every Pod looks once whenever the stream opens (what happened
  while it was closed is not reported), and a dropped stream is reopened on a 1–30 s backoff. Only while no stream
  is open (the RPC `Unimplemented`, or between reconnects) do Pods keep the counted fallback.
- **feat:** The node side of the Kubernetes 1.36 posture (#23, rustkube#37): `nodeInfo.kubeletVersion` reports
  `v1.36.0-rustkube+…` and `kubeProxyVersion` is no longer written (removed in 1.33). A pod asking for an AppArmor
  profile other than `Unconfined` (field or the deprecated annotation) is refused at admission as upstream refuses it
  on a node that cannot enforce one: `Failed`, reason `AppArmor`, Warning Event, nothing started, terminal whatever
  its restartPolicy. A `gitRepo` volume's wait names its 1.36 removal. containerd before 2.0 is refused at startup.
  The unused workspace `k8s-openapi` (v1_32) dependency is dropped (apimachinery keeps its own).
- **docs:** status.md gap rows for #47, #69/#89/#77 and #105 removed (done).
- **fix:** A given `--apiserver` / `APISERVER_URL` wins over the kubeconfig's server even when it equals the default
  `http://127.0.0.1:6443` (#113). The flag has no clap default any more; precedence is decided by whether it was
  given (given → kubeconfig server → default), not by comparing its value with the default.
- **fix:** The Node's `nodeInfo` carries `kernelVersion` (`/proc/sys/kernel/osrelease`), `bootID`
  (`/proc/sys/kernel/random/boot_id`), `machineID` (the node's `/etc/machine-id` at `/hostroot` first) and
  `systemUUID` (`/sys/class/dmi/id/product_uuid`), each `""` only when its file is absent (#78). They were always
  empty, so `kubectl get nodes -o wide` and `sc -o wide` showed no kernel.
- **feat:** The stormblock engine client carries an admin credential for destructive verbs (#105). Since
  stormblock#274 the node token no longer covers deleting a template or a sealed volume; such a call (any non-GET)
  that is still refused with 401 after the node token's reread is sent once more with `$STORMBLOCK_ADMIN_TOKEN` or
  the file at `$STORMBLOCK_ADMIN_TOKEN_FILE` (default `/run/stormblock-admin/admin_token`): the engine's admin token
  or a ServiceAccount token its SubjectAccessReview allows. Read at each use (rotation), never cached or logged;
  absent, the refusal reaches the caller as before and is retried on its next pass. Single-token engines see no change.
- **fix:** A crash-looping container keeps its restart backoff across a kubelet restart (#112): CrashLoopBackOff is
  written to `<state root>/crashloop.json` (wall-clock seconds, atomically, on every change) and read back by the
  restarted kubelet's state recovery. An entry past the 10-minute stable window is forgiven, so not carried; a Pod
  recreated under the same name has a new uid and inherits nothing; a deleted pod's entries go with it.
- **fix:** An init container's log stays readable after it completes (#47): on a runtime whose `remove_container`
  takes the log with it (a CRI runtime), a completed init container is kept until its pod stops, as upstream keeps
  it; on stormpump, whose removal releases the workload and its root but never the log file, it is removed at once.
  `RuntimeService::logs_survive_removal`. (The status half of #47 is since 4fefda1/ece4211.)
- **test:** medium's size cases run three at a time, the largest first (#64): all twenty at once (every class blank
  a format, claims up to 16Ti and 1Pi) took the node's apiserver down on two runs (fff1f4d9d9, 63a3c5201b).
- **feat:** Pod CPU requests are cgroup weights among Pods (#57, the owner's choice on #106: OpenShift's shape):
  each container runs in its pod's QoS group (stormpump#68: `pods`, `pods/burstable`, `pods/besteffort`), its
  `cpu.weight` upstream's conversion of its request, and the kubelet sizes the `pods` group from the node's CPUs and
  `besteffort` at the floor at start. Container memory is the engine's working set (stormpump#64) when given.
- **build:** the lock moves stormpump 795b92e → 13cf2c9 and stormcast 2bcdafc → 3cec734 (stormvm unchanged). An
  engine before stormpump#68 refuses a spec with a group, so this ships with golden-stormpump-accd1a3e8e61.
- **feat:** A node service's lifecycle as Events, completed (#50): a service seen for the first time is `Started`
  (with its own start time) unless its mirror pod already shows that run, so one started since the pod was written
  is announced and a kubelet restart re-announces nothing; a non-zero exit or a signal is a Warning `Failed` with the
  exit and output tail (a clean one stays `Stopped`); down at first sight while the API says running is reported.
- **feat:** The node half of volume expansion (#42): a claim the control plane grew (`NodeResizePending` /
  `FileSystemResizePending`, PV past the claim's capacity) has its filesystem grown by `NodeExpandVolume` on the
  published path, then its status finished as upstream's kubelet does (new capacity, `Resizing` and
  `FileSystemResizePending` gone, `allocatedResourceStatuses` dropped) with a `FileSystemResizeSuccessful` Event;
  a refusal is `FileSystemResizeFailed`, retried. Driven by claim events. ReadWriteOncePod (the other half) since v0.8.0.
- **feat:** Each stormblock PV carries its storage placement (#60): `storm.io/volume-id`, `storm.io/golden`,
  `storm.io/redundancy`, `storm.io/health`, `storm.io/rebuild`, `storm.io/drives` (wwn, serial, model, node, shelf,
  bay, health) and `storm.io/raid-partners` (array members and their state), labels `storm.io/shelf`,
  `storm.io/redundancy`, `storm.io/health`; Events on change (VolumeDegraded/VolumeHealthy, RebuildStarted/Finished,
  VolumeMoved, RaidPartnerChanged). stormblock's per-volume placement joined with stormdrive's drive locations on
  WWN or serial; refreshed on engine volume changes and every minute; only changes written. `STORMDRIVE_URL`.
- **feat:** Each stormpump workload's cgroup → pod/container identity, for cadvisor (#84): one JSON file per workload in
  `/run/rustkube/workloads/<cgroup>.json` (cgroup from `/proc/<pid>/cgroup`, init pid from QUERY's info block, kind
  container | sandbox, `reports_network` on the pod-network sandbox, namespace/pod/uid/container/id/image, CRI labels
  with upstream's `io.kubernetes.*`, annotations), written at start, removed at removal, stale ones swept at start.
  cadvisor had only an opaque `id` for every stormcos pod. A VMI's hypervisor too (kind `vm`, `kubevirt.io/*` labels,
  network reported on the pod network). `RingClient::query_info`. cadvisor's mount of the host `/run` is stormcos#391.
- **feat:** A pod waiting on the network says why in an Event too (#3), as upstream: Warning `NetworkNotReady` with no
  CNI config, Warning `FailedCreatePodSandBox` with the plugin's words when CNI ADD fails. Only the container's waiting
  message (and the kubelet's log) said it before.
- **docs:** Node Ready is not gated on the CNI (owner's decision on #3); the node side Cilium needs verified live on
  server3 (11.91): pod-to-pod on one node, endpoint released on delete, a sandbox that fails rather than starts
  addressless with the agent gone, and the pod starting by itself once the agent is back.
- **feat:** Events on the claim while its size-class blank is made (#70): Normal `Provisioning` while the blank is
  minted or formats, Warning `ProvisioningFailed` with stormblock's words when a mint or clone is refused, Normal
  `ProvisioningSucceeded` once cloned. Only the pod's FailedMount said anything before; the console showed "a
  stormblock fault" and no reason. (The wait for a formatting blank and the bounded mint landed with #63 and #99.)
- **fix:** An image the node's registry has no golden of is asked of the cluster, else fails its pull with the
  registry's words (#79). The kubelet sent `remote_image`, which sbregistry dropped, so such an image 404'd; and
  #104's `GET /v1/goldens` did not start sbregistry's cluster fetch. A 404 golden now posts the clone route as the
  demand: 503 "fetching it from the cluster" / 404 "push the image" are ErrImagePull, retried on the back-off; a
  clone minted because the golden became ready meanwhile is deleted at once. stormblock-registry#98 asks for a
  demand that mints nothing.
- **feat:** Every container runs on its own root (#104, the owner's rule): a copy-on-write clone of its image's
  sealed golden, made at create (engine volume `ctr-<container>`, attached over ublk, mounted by PID 1 at
  `/run/stormpump/roots/<container>`), and detached and deleted with the container; a restart gets a fresh
  clone. Before, every container of an image ran on one directory (the pallet's mount, or the single clone a
  pull made per image), so containers wrote each other's roots. A pull now clones nothing: it finds the image's
  golden (a pallet: `<volume>.golden` in the slab, else the mounted volume's sealed parent; a pull: the
  fstemplate in sbregistry's golden record, `template:<name>`). The per-image registry clone and its binding
  (#143) are gone. A failed create removes its clone; roots nothing holds are deleted when the kubelet starts.
  `readOnlyRootFilesystem` waits on stormpump#108 (the root is private and writable until then, and logged).
- **feat:** A node service's last exit and output on its mirror pod (#82). PID 1 keeps them in assets.json
  (stormpump#51); the kubelet read them only for pods/log. The mirror's container now carries
  `lastState.terminated` (exit code, or 128 + the signal; `signal`; reason `Error`; `message` = the last output
  lines within 80 lines / 4 KiB), a stopped service the same on `state.terminated`, and the `BackOff`/`Stopped`
  Events end with the exit and the tail. A new exit is a change the mirror writes.
- **fix:** The #85 change compiles: `watch_for_node`'s per-placement closure no longer moves the node name it
  still passes on, and `assigned_to`'s helper is a function, not a closure returning a borrow (build-failure #188).
- **fix:** A Pod held back by a claim a VM on this node is using says why (#80). Admission already reserves a
  VMI's claims exclusively (since #100), so the Pod was never started against the guest's disk, but it sat with
  no status and no Event. It is now Pending with its containers `ContainerCreating` and the reason ("claim ns/c is
  a disk of VirtualMachineInstance ns/vm on this node"; a ReadWriteOncePod holder or a reclaim is named the same
  way), with a FailedMount Event; the VM's release wakes it.
- **feat:** The Node says whether it can run a VM (#65). A test in a Job has no /dev/kvm and could not tell a
  VM-capable node through the API. With the stormpump engine the kubelet checks KVM each heartbeat
  (`/sys/class/misc/kvm`, else a `/dev/kvm` or `/hostroot/dev/kvm` that opens read-write) and reports
  `devices.kubevirt.io/kvm: 1k` in capacity and allocatable, as KubeVirt's device plugin does, plus the labels
  `storm.io/kvm=true` and `kubevirt.io/schedulable=true`. The labels are written at registration and patched on an
  existing node whenever they change; without KVM the resource goes, `storm.io/kvm` is removed and
  `kubevirt.io/schedulable` is `false`.
- **fix:** A VMI placed by hand with `spec.nodeName` is started (#85). The kubelet listed and watched VMIs by
  `status.nodeName` only, and filtered what came back on it too, so the `spec.nodeName` half of its own
  assignment rule never saw anything. It now lists and watches both fields, merges them by uid and keeps what
  rustkube's scheduler places here (`status.nodeName` when set, else `spec.nodeName`: a stale `spec.nodeName` no
  longer claims a VMI the scheduler put elsewhere). A hand-placed VMI gets `status.nodeName` written when it is
  taken (status merge patch, uid-guarded). The unused `watch_for_node` built its watch URL with spaces in it.
- **fix:** A renewed client certificate is presented without a restart (#77). The apiserver client took
  `--client-certificate`/`--client-key` once, so a kubelet that ran past stormcert's renewal (80% of 365 days)
  kept presenting the old one until it expired. With a CA and verification on, the client's TLS now asks a
  resolver for the pair on each handshake, shared by every clone of the client; the pair's directory is watched
  (and looked at hourly) and the resolver swapped when the bytes change, after checking the key matches the
  certificate.
- **fix:** A node service's mirror pod is Ready only while the service answers (#96). It read Running and Ready
  whenever PID 1 had the process up, so stormstorage refusing `:9093` on 11.50 looked healthy to the console, test
  runs and release gates. The kubelet now reads each host-network service's own liveness URL from its golden's
  stormd config (the check stormd restarts it by) and asks it every 10 s; three failures in a row make the pod
  Running but not ready (`Ready=False`, reason `Unhealthy` with the failure, a Warning Event), one answer ready
  again.
- **feat:** A VM's verbs on `:10250` (#94): `PUT /vmVerb/{ns}/{name}/{verb}` hands `pause`, `unpause`, `softreboot`,
  `reset`, `status`, `freeze`, `thaw` (KubeVirt's `unfreeze` accepted) and `snapshot` to stormvm's mounted router,
  behind the server's auth with the same handover as the console doors (now one `to_console`), the verb's query
  forwarded without `token`. `migrate`/`receive` are refused (migration is the VMI status's, #40). For the
  apiserver's `subresources.kubevirt.io` verbs (rustkube#141).
- **feat:** `kubectl logs` on a node service not run by stormd (stormblock, the registry, timesync) reads its live log
  (#87). stormpump#90 lists each asset's last runs in `assets.json`, each naming its `w<id>.log`; the kubelet now
  serves the running run's files (rotated part first, `-f` follows the live one) and, for `--previous`, the newest
  ended run's. `last_output` stays the fallback for a PID 1 without runs.
- **fix:** The `:10250` serving pair is reloaded when stormcert renews it (#89). It was read once, so a pair renewed at
  boot after the kubelet started (stormcert#14) was not served until the next restart. `--tls-cert-file`'s directory
  is now watched and the pair reloaded in place when its bytes change (a half-written pair leaves the previous one
  serving). The log says which pair is served, configured or self-signed. A named pair that is missing already waits
  and then exits, never falling back (#69).
- **fix:** kube-proxy deletes stale UDP conntrack entries (#147). It rewrote the DNAT when a Service's backend went
  but left conntrack, so a long-lived UDP socket to a ClusterIP (a DNS client of kube-dns) kept reaching the
  replaced pod for 30–120 s. After an apply it now runs, as upstream, `conntrack -D -p udp --orig-dst <clusterIP>
  --dst-nat <old ip>` per removed UDP endpoint (and `--dport <nodePort>` for NodePorts), and clears a UDP
  ClusterIP when it gains its first endpoint. "0 flow entries" is not an error; a node without the `conntrack`
  binary warns once. (No stormcos edition runs kube-proxy, #145/#155; the binary ships.)
- **docs:** README: `stormvm serve` on `127.0.0.1:9095` is not a developer-only convenience (#123, stormcos#65). It
  ships on every node as a stormcos service golden, and stormconsole's VM plugin uses it for serial and VNC
  consoles; it reads the same `/run/stormvm` registrations as the kubelet's `vmConsole` route.
- **perf:** A stormpump exit wakes only its own workload (#115). Each exit woke every Pod and VMI worker and queued
  a full list sync of the node's pods, so the work grew with the workloads on the node rather than with the one that
  ended. The ring now broadcasts the exiting handle; a router maps it to its pod (container → sandbox → uid,
  `RuntimeService::pod_of_workload`) or its VMI (`VmManager::uid_of_handle`) and wakes that worker alone. A handle no
  record names (an adopted workload) or a lagged channel wakes every workload, as before.
- **fix:** An init container runs until it exits (#126). The kubelet stopped any init still running after a fixed
  120 s (`DeadlineExceeded`, exit -1), so a migration or a long copy could never finish. Now the pod's
  `activeDeadlineSeconds` (from its startTime) is the only deadline; past it the init is stopped and reported
  `DeadlineExceeded` and the pod is Failed. The wait holds no worker on either path (the non-admission one slept
  500 ms at a time). A running pod's `activeDeadlineSeconds` is #185.
- **perf:** A pod's probe pass no longer rewrites an unchanged status (#141, #138 follow-up). The skip compared with
  the watch's copy of the pod, which a second after a start was still the pre-Running object, so the unchanged
  Running status was written again, on a worker, on a stale revision. The kubelet now keeps per pod the status the
  apiserver acknowledged and the revision it answered with; while the watch's copy is still at the revision that
  write was made on, the write is what is compared with and its revision what the next write is made on. New
  counter `kubelet_pod_status_writes_total{result="written|skipped"}`.
- **fix:** A start error no longer ends a pod with no container state and a 404 log (#133, stormcos_qa short on
  C2NR0Q2 10-02, triggered then by #103). An image that will not pull waits as `ErrImagePull` →
  `ImagePullBackOff` and is pulled again after a 10 s–5 min back-off (the failed pull was cached for the pod for
  good); a container that will not be created waits as `CreateContainerError`, one that will not start as
  `RunContainerError`, or under `restartPolicy: Never` ends the pod with that container terminated `StartError`
  (exit 128). An unreachable runtime is a wait. Any other failure (an init container's) reports the init statuses
  and the apps `PodInitializing`. A partial start is torn down before the next try, which waits its back-off however
  the pod is woken. `logs` on such a pod answers 400 with the reason.
- **feat:** Native sidecars (#111): an init container with `restartPolicy: Always` is started in its slot and the
  next init goes once it has started (running, startupProbe passed), with no init deadline; it runs for the pod's
  life, probed and restarted (Always, whatever the pod's policy; an early exit is a restart with back-off, never a
  failed pod), and is reported in `initContainerStatuses` with `started`, `ready`, `restartCount` (and a `waiting`
  state). It counts in `Initialized` once started and in `ContainersReady`/`Ready`, but not in the phase: a
  Never/OnFailure pod finishes with its apps and its sidecars are then stopped, last declared first. Deletion
  stops the apps first, then the sidecars in reverse order. Before, a sidecar blocked the start until the 120 s
  init deadline killed it.
- **fix:** A container's stop grace reaches the engine (#174). `RingClient::stop` put it in `inline_a`, in seconds;
  stormpump's STOP reads `inline_b`, in milliseconds (0 = its 30 s default), so every stop waited 30 s before the
  SIGKILL whatever `terminationGracePeriodSeconds` said (seen in stormpump's medium suite: a 5 s grace killed at
  30.06 s). Now `inline_b = grace × 1000`, and a grace of 0 is `FORCE`.
- **perf:** A pod's claim no longer waits for its PV and binding to be written (#95). On 11.91 (pvetest1,
  server3) the kubelet's claim start was 0.5–0.7 s cold and 1.5–1.7 s warm, and its `bind` (an engine volume
  listing, the PVC read, the PV written, the claim written) was 150–220 ms of it. The pod needs the attached
  device, not the objects, and the control plane's binder has normally bound the claim already, so the bind now
  runs as its own task after the attach. The VM path still binds before it returns. `claim/<volume>/bind` is
  absent from a pod's start timing.
- **feat:** `--max-pods` / `MAX_PODS` (#165, owner's option A on rustkube#205): the node's `capacity.pods` and
  `allocatable.pods`, fixed at 110 before, are configurable (default 110, upstream's). stormcos sets 250. On
  pvetest1 a 100-Pod burst waited ~60 s on the 110 the scheduler held the node to.
- **feat:** A pod mount's `mountPropagation` reaches the engine (#81): `spec_for` maps `cri::MountPropagation`
  onto stormpump's `Mount.propagation` (stormpump#35), so Cilium's HostToContainer mounts are `rslave` and a CSI
  node plugin's Bidirectional `/var/lib/kubelet` is `rshared` (privileged only, as before). Lock: stormpump
  30a76d3 → 795b92e, stormvm dc1b7ea → 180fa13, stormcast 801f822 → 2bcdafc (stormvm#65 made them compatible).
- **fix:** An emptyDisk VMI starts again (#164): the stormvm bump brings b5979ef, so an `emptyDisk.capacity` (and a
  dataVolume's request) reaches stormblock as a byte count, which it reads, instead of `64Mi`, which it refused.
- **feat:** `kubectl port-forward` is served (#56): `/portForward/{ns}/{pod}` upgrades to SPDY/3.1
  (`portforward.k8s.io`) or to a WebSocket tunnelling SPDY (`SPDY/3.1+portforward.k8s.io`, a newer kubectl's
  first try), pairs each request's error and data streams, connects to `localhost:<port>` inside the pod's network
  namespace (the node's for hostNetwork) and splices. New `spdy.rs`: a SPDY/3.1 server session (header blocks
  inflated with the SPDY/3 dictionary through miniz_oxide's core, written as stored blocks; SYN_REPLY per stream,
  PING echo, RST, GOAWAY; no flow control, as spdystream does none). `/exec` and `/attach` now answer 501 naming
  stormpump#103 (the engine cannot run a process in a running container) instead of 404. server.rs's module doc
  no longer points them at #7 (#127).
- **fix:** The image's config is applied under the pod spec on stormpump (#98). argv was `command` + `args`
  only, so an args-only container (CoreDNS's `-conf …`) exec'd its first argument, a container with no `env`
  got no image `PATH`, and every container ran as root. Now, from sbregistry's golden record (`config`, asked
  once per image root): `command`/`args` over `Entrypoint`/`Cmd` by CRI's rules, the image's `Env` under the
  pod's, `WorkingDir` and `User` (numbers or names from the image's `/etc/passwd`/`/etc/group`) when the pod
  leaves them unset. `runAsUser`/`runAsGroup`/`runAsNonRoot` are read (container, else pod) and applied; the CRI
  gRPC path forwards the first two. An image with no known config (the boot goldens, until
  stormblock-registry#58) whose argv would be empty or start with a flag is refused, naming the image. emptyDir
  is created 0777, as upstream.
- **feat:** Start timing takes a claim apart (#95): `claim/<volume>/lookup` (PVC, PV, existing volume, room),
  `claim/<volume>/blank|clone|raw` (how its volume was made, absent when it existed), `claim/<volume>/attach`
  (ublk) and `claim/<volume>/bind` (PV written, claim bound), in the annotation, Event and log line, with
  histograms `claim/lookup`, `claim/make`, `claim/attach`, `claim/bind`. The 4–67 s claim bind+mount samples had
  only the one `volume/<name>` number.
- **docs:** `--no-cni`'s help no longer says pods use host networking (#3): a pod that is not hostNetwork still
  gets an isolated namespace, with loopback only and no address. The startup warning says the same.
- **test:** A host-network pod's sandbox is made with a CNI configured and no config present (no namespace, no
  ADD), and a pod-network pod on the same node waits with `NetworkNotConfigured`, making nothing (#3, item 5).
- **feat:** Start timing takes `sandbox` apart (#139): `sandbox/acquire` (stormpump `SandboxAcquire`),
  `sandbox/cni` (the CNI ADD with the plugin's exec), `sandbox/status` (the address read) and `sandbox/other`
  (the rest), in `storm.io/start-timing`, the Event, the log line and
  `kubelet_pod_start_phase_duration_seconds{phase}`. server3's 265 ms–2.3 s `sandbox` (stormblock#264) was read as
  a stormblock clone; the step makes none, and the next measurement names which part is slow. The stormpump
  runtime reports its steps in the sandbox status (`PodSandboxStatusInfo.made`); a CRI runtime gives status and
  other only. Written only when the attempt made the sandbox.
- **fix:** A credential file named on the command line is waited for (60 s) and its absence is fatal, never
  skipped (#69). `--client-certificate`, `--client-key` and `--token-file` were read once and dropped when
  missing, so a kubelet started before stormcert wrote its pair, or given a mistyped path, ran as
  `system:anonymous` (cluster-admin on sno, 403s elsewhere). Only `--apiserver-ca` was waited for. The same
  wait now covers `--kubeconfig`, `--tls-cert-file`/`--tls-private-key-file` (a missing pair became a
  self-signed one) and `--server-token-file`. An empty file counts as not written yet. The error names the flag.

## 2026-10-07

- **chore(build):** Removed `scripts/build-golden.sh` (#51). It was a second golden builder (root on dev,
  bin-only output) beside the platform's stage recipe, which is the only release path:
  `stormcentral component stage rustkube-node`. Nothing called it. README and BUILD.md updated.
- **fix:** `/vmInstance` no longer answers from a cache the apiserver has not confirmed (#156). A node cut off
  from the apiserver kept handing out the identity its last cache placed here, while the control plane could
  move the machine or give its address to another. The kubelet now records when it last heard from the
  apiserver (a renewed node Lease, or a VMI LIST); past `--metadata-max-staleness` (`METADATA_MAX_STALENESS`,
  default 40 s, the Lease duration; 0 is unbounded) a machine found here answers 503 + `Retry-After: 5`. An
  address with no machine here stays 404. `NodeReporter::heartbeat` returns whether the Lease was renewed.
- **docs:** No stormcos edition runs kube-proxy (#155): README, BUILD.md, status.md, the presentation and
  CLAUDE.md said the flowsdn edition ran it as a DaemonSet. The owner chose flowsdn's own Services (#145;
  stormcos#265 removed `65-kube-proxy.yaml`, flowsdn#292). kube-proxy stays in the golden.

## 2026-10-06

- **test:** short, medium pod cases and long (#61). short (< 2 min): Node Ready with a recent heartbeat and no
  pressure, a pod runs to Succeeded with a pod IP and its log reads back, a container's exit code 3 is
  reported, a running pod is deleted within 30 s. medium, with or without the storage class: OnFailure restart
  with the previous log, init container before the main one, ConfigMap volume + configMapKeyRef + fieldRef env,
  a missing image waits as ErrImagePull/ImagePullBackOff. long: waves of pods at 80% of the node's free pod
  slots (every fourth with a 16Mi claim), p50/p95 start, drain and residue per wave; a slowdown or residue
  fails the wave. Pods are pinned with `spec.nodeName`; new workload modes `echo`, `exit`, `sleep`,
  `fail-once`, `write-file`, `expect-file`, `expect-env`. `test/requires.toml` declares [short] and [long].
- **fix:** `securityContext.capabilities.drop` is parsed (`ContainerConfig.drop_capabilities`) and sent to a CRI
  runtime (`Capability.drop_capabilities`); only `add` was (#118). The stormpump ring path still forwards
  neither: its `Spec` has no capability field until stormpump#47.
- **fix:** A container removal the engine refuses is finished by the runtime (#90): `WorkloadRelease` is
  refused while the workload runs, and a stop only signals, so a removal right after a stop (a restart, an
  init container past its deadline) was refused and its caller dropped the error. The record (workload,
  cgroup, volume handles in PID 1; the sandbox it occupies) stayed for good, and the pod's sandbox removal
  waited on it. Now the record is kept, hidden from the kubelet (not listed, NotFound), and the removal is
  tried again on the workload's exit, at least every 10 s, and before its sandbox is removed.
- **docs:** presentation: #143 and #90 are no longer open P1s.
- **fix:** A pulled image's registry clone is bound (#143, stormblock#267): after the attach and mount,
  `POST /v1/clones/{id}/bind` with consumer `kubelet/<node>/<image>`. It stayed `claimed`, and sbregistry
  reaps a claim older than 900 s as abandoned, under the mounted image. A pull first asks the registry for
  the clone bound to that consumer (`GET /v1/clones?consumer=&state=bound`) and reattaches it, so a kubelet
  restart does not mint (and leak) a second one; a failed lookup fails the pull rather than minting. A
  refused bind keeps the image usable and is asked again on its next pull. Pulls of one image are
  serialised. Releasing a pulled image needs an image GC: #161.

## 2026-10-05

- **docs:** presentation: the launcher Pod (rustkube#203) moved from Planned to what works; pod-network VMs'
  live proof is stormcos_qa#18 (#55, #88).
- **fix:** A VMI's launcher Pod is the one on this node (#152): during a live migration rustkube gives the
  target node its own launcher (rustkube#203, `kubevirt.io/migrationJobUID`), and `launcher_for` took the
  first live one, so a target could have named the source's Pod in its CNI ADD and status.
- **docs:** `docs/presentation.md`, a 12-slide Marp deck on rustkube-node's purpose and functionality (#55):
  what it is, where it sits in stormcos (stormcentral's relationships), how it works, what it does today
  (Pods, storage, VMs, node services), interfaces, shipping, planned work on its own slide, and status.
- **feat:** ServiceAccount tokens are bound to their pod (#122, rustkube#182): kube-api-access asks
  `expirationSeconds: 3607` and `boundObjectRef` (the Pod's name and uid); a projected
  `serviceAccountToken` passes its own `expirationSeconds` (default 3600) and the Pod. Each token file is
  written again (atomically) at 80% of the life the apiserver granted, and forgotten with its pod. The
  unbound request is gone.
- **feat:** `/vmInstance/{address}` answers a host-network workload (#122): from a node address, with the
  workload's ServiceAccount token in `X-Storm-Workload-Token`, the kubelet TokenReviews it and answers for
  the pod the token is bound to (`storm.io/kind: Pod`) when that pod's object has the token's uid, is on
  this node and is not ending. No token, or any other answer, is the refusal as before.
- **feat:** `VirtualMachineRestore` (#53, owner's option A on #109): by the node that took the snapshot,
  once the snapshot is Succeeded and the VM stopped, every disk but cloud-init gets a new volume from its
  group member, a bound Block PVC/PV pair pinned to this node, and the VirtualMachine's template volume is
  rewritten to that claim; `status.complete` with `restores`, Events, errors written once. A taken
  snapshot records `storm.io/snapshot-disks` (disk → volume id) with its claim, which a restore needs.
  New direct dependency `stormvm-block` (already in the lock).
- **feat:** A pod-network VMI adopts its launcher Pod (#88, owner's choice B; the Pod is made by
  rustkube's VM controller, rustkube#203): the VMI waits for a Pod labelled `kubevirt.io: virt-launcher`
  and `kubevirt.io/created-by: <vmi uid>` and owned by it; the CNI ADD names that Pod, so Cilium labels
  the endpoint from it; the kubelet writes its status (`Running`, `podIP`, Ready; `Succeeded`/`Failed`
  at the end) so Services select the VM; such Pods never reach the pod manager, wake their VMI when
  they appear, and are confirmed deleted (grace 0, uid precondition) once no machine of their VMI runs.
- **fix:** The test container runs under stormcentral's real runner (#97; found checking #91). The runner
  builds the Job itself, so: the workload image is the run's own pod's (`test` container), else the
  standard's `test-rustkube-node-<suite>:<commit12>`, with `RUSTKUBE_NODE_TEST_IMAGE` only an override;
  `STORM_NODE` is an address, so the Node's name is resolved from the node list; `test/requires.toml`
  declares the medium suite's cluster reads (nodes, PVs, PVCs, StorageClasses, CSIStorageCapacities).
  `node-volumes-restored` reports skip: a test may not delete kube-system's claims (cluster reads only).
  `test/rustkube-node-test.yaml` is marked reference only.
- **feat:** Claims are charged their full size class against the node's stormblock data slabs (#62; owner
  policy #108: class size at bind, overcommit ratio 1.0, class-sized clones). The node publishes
  `CSIStorageCapacity` `kube-system/stormblock-<node>` (room left, `maximumVolumeSize` = the largest class
  that fits) on volume changes and every 60 s; a new claim volume that does not fit is refused at
  provision (the pod waits with the numbers; check and create serialized); gauges
  `kubelet_stormblock_data_bytes{kind}` / `kubelet_stormblock_data_used_percent`, and a `SlabFilling`
  Warning on this node's stormblock PVs past the alert percent. New flags `--storage-overcommit` (1.0),
  `--storage-reserve-percent` (5), `--storage-alert-percent` (85). The scheduler reads the object once
  stormcos#151 sets `storageCapacity: true`.
- **test:** medium `pvc-overcommit-refused` runs (#62): a Block claim one class above the test node's
  `maximumVolumeSize`, pinned to the node, must wait with the reason and never bind.
- **feat:** A VMI on the pod network gets its own sandbox (#88, stormvm#16): a stormpump network
  namespace and a CNI ADD (container `vm-<uid>`), its NICs realised in it (the bridge binding gives the
  guest the pod IP and MAC), the hypervisor spawned inside it, and a DHCP responder per bridged NIC
  (pod IP, gateway, cluster DNS, ClusterFirst search, hostname). `status.interfaces[]` reports the pod
  IP from the start and the binding stormvm chose (`made.binding`; `binding_of` is gone). The sandbox
  is recorded under `/run/rustkube-node/vm-network/` before the ADD, released (CNI DEL + sandbox
  release) when the machine ends, on a failed start and at deletion, and restored (DHCP) or released
  after a kubelet restart. No CNI yet is a wait, as for a pod.
- **fix:** Every node's service volumes are represented (#59, owner's naming on #107): the PVC is
  `kube-system/<volume>-<node>` and the PV `storm-<volume>-<node>`. Before, the names were the same on
  every node, so only the first node to write them had objects and the others logged a warning. No
  migration: a pair under the old unqualified names is left as it is; the mirror now decides a volume
  "went away" by the volume a claim names (`storm.io/volume`), not by the claim's name, so it does not
  delete those old claims either.
- **test:** medium suite `node-volumes-pairs` and `node-volumes-restored` (#59): every claim mirrored for
  the test node is a node-qualified, Bound pair whose PV names it by uid with matching kind/component
  labels, and a deleted claim comes back with its PV naming the new uid. The Job's ClusterRole may read
  PVCs and delete them (one mirrored claim; its PV is Retain).
- **feat:** The PVC ladder reaches 1 PiB (#67): classes 4Ti, 16Ti, 64Ti, 256Ti and 1Pi after 1Ti (x4
  steps, still the quota). Each class names its filesystem; all are ext4 (owner, #67; stormcos#91).
  64Ti, 256Ti and 1Pi are raw block only until stormblock carries a formatter that can lay them down
  (mkfs.ext4.rs#10 memory, #9 inode wrap; stormblock#289): a filesystem claim rounding to them waits
  with that reason and names `volumeMode: Block`. Blanks are minted with the class's filesystem and
  its size in MiB (stormblock's sizes have no `P`).
- **feat:** Raw block claims (`volumeMode: Block`, #67): a plain thin stormblock volume of the class's
  size (no template, no mkfs), attached over ublk, bound as the device at the container's
  `volumeDevices[].devicePath`. A Block claim in `volumeMounts` or a Filesystem claim in
  `volumeDevices` waits with the reason. The PV the node writes carries the claim's `volumeMode`
  (the control plane's provisioner does not yet: rustkube#201). PV capacities read `Pi`.
- **test:** medium suite: 4Ti and 16Ti classes, Block claims of 1Mi, 20Ti and 1Pi through
  `volumeDevices` (the device exactly its class), a 20Ti filesystem claim refused naming Block, and
  above-ladder moved to 2Pi.
- **fix:** Guest metadata (`/vmInstance/{address}`) answers only for a machine whose VMI places it on this
  node (#119, stormcos#54): the cached object must exist, not be terminating, carry the machine's uid, name
  this node in `status.nodeName`, and not have completed a migration to another node. Before, the local
  record alone decided, so a machine that moved, or a stale record whose object was gone, still answered,
  and an address reused elsewhere could resolve to its predecessor. Each machine's cached object is
  refreshed by its own reconcile, not only by the next full list. A cluster without the VMI CRD is no
  longer "cold" for ever (404, not 503).
- **perf:** `/vmInstance` finds the machine through an address → uid index kept with the machine records
  (re-indexed on every change to a machine), not by scanning every machine and NIC under the lock. An
  address two live machines here both claim answers 404 rather than either of them.
- **perf:** A pod scheduled before the CNI agent is up starts as soon as its config appears (#148, Dell
  on 11.79: coredns 16 attempts over 27 s, CNI → all pods 20–27 s per boot). With no conflist the stormpump
  runtime no longer acquires and releases a sandbox per attempt (the config is checked first), and the pod
  is no longer retried on a backoff that reached 10 s: the kubelet watches `--cni-conf-dir` (inotify) and
  wakes exactly the pods waiting for it, with a 10 s fallback. A CNI ADD that fails once the config is
  there (agent not serving yet) is retried on a backoff from its own first failure (1 s at first), not from
  when the pod was first seen. The cilium agent's `attempts=10` is its staged init containers (hostNetwork,
  woken by each exit), not a network wait.
- **feat:** kube-proxy talks to a TLS apiserver with a token (#145, for stormcos#265: the flowsdn
  edition has no Service datapath and runs this kube-proxy as a DaemonSet). `--ca-file` /
  `KUBE_PROXY_CA_FILE` and `--token-file` / `KUBE_PROXY_TOKEN_FILE` default to the pod's
  ServiceAccount (`/var/run/secrets/kubernetes.io/serviceaccount/{ca.crt,token}`) when present;
  the token is re-read on every request (projected tokens rotate). New optional `--cluster-cidr` /
  `KUBE_PROXY_CLUSTER_CIDR`: ClusterIP traffic from outside the pod CIDR is masqueraded.
- **fix:** kube-proxy's rules are entered: `PREROUTING` and `OUTPUT` jump to `KUBE-SERVICES` and
  `POSTROUTING` to `KUBE-POSTROUTING`, checked after every restore and inserted when missing
  (before, nothing reached the chains it wrote). `iptables`/`iptables-restore` wait for the
  xtables lock (`-w 5`).
- **fix:** kube-proxy no longer removes every rule when the apiserver refuses it: a non-2xx answer
  or a body with no `items` is an error, and the rules on the node stay as they were.
- **fix:** kube-proxy rewrites the rules when a backend changes at the same count (a replaced pod at
  a new IP kept the old DNAT), and applies them again every 60 s; unchanged rules are not rewritten
  every 5 s.
- **fix:** kube-proxy keeps both halves of a port served on two protocols (kube-dns 53/UDP and
  53/TCP collided, so one had no rules), matches Endpoints ports to Service ports by name (a
  multi-port Service got one target port for all), takes every subset, and drops backends whose
  Endpoints are gone.

## 2026-10-03

- **fix:** A claim stormblock will not clone says why (#140, Dell C2NR0Q2 on 11.76: every claim
  waited forever on "stormblock would not clone pvc-ext4j-64m to pvc-…", which could not be
  debugged without the node's engine token). The template clone, attach and snapshot-clone POSTs
  carry stormblock's status and `error`, or the transport error ("no answer within 60 s"), into the
  pod's wait and FailedMount Event; a transport error was not logged at all before. A clone refused
  because the template itself is broken (its sealed volume missing or not sealed, no sealed
  snapshot) deletes the template and mints it again instead of meeting the same refusal on every
  retry.
- **perf:** Pod starts no longer queue behind 8 workers (#138, pvetest1 on 11.72: over 173
  pods `wait` was 2.4 s p50, 3.6 s p90, 9.3 s max, for ~250 ms of work). The one Pod/VMI executor
  ran a fixed 8 passes at once, and a start pass held its worker through the sandbox, the status
  PUT and the start-timing annotation PATCH. New `--pod-workers` / `POD_WORKERS`, default 16 per
  CPU, at least 32 and at most 256. The annotation PATCH is written off the worker. Each pass no
  longer clones every pod's state to look for an adopted pod without its spec.
- **feat:** `storm.io/start-timing` adds `workers=<busy>/<limit>` (the executor's passes running
  when the start began) and `pending=<n>` (pods seen here and not yet started) (#138).
- **fix:** A finished Pod gives its pod IP back (#137, pvetest1 on 11.72: of 1,000
  `restartPolicy: Never` pods, 250 reached `Succeeded` and the rest waited on Cilium's
  "range is full"). Nothing stopped a Succeeded/Failed Pod's sandbox until the Pod object was
  deleted, and the stormpump runtime ran CNI DEL only at sandbox removal. The pass that makes a
  Pod Succeeded or Failed now stops its sandbox: CNI DEL and the netns holder released, the
  container records, status and logs kept until deletion. A failed DEL is retried.

## 2026-10-02

- **fix:** Every pod start took two attempts (#134, pvetest1 on 11.71: `attempts=2` on all five
  pods). `prepare_images` spawned the image resolution and read its result in the same call,
  before the task had run, so the first attempt always returned "waiting for image", wrote a
  `ContainerCreating` status to the apiserver, and a second attempt started the pod, for a
  golden that resolves in 0.1 ms. A start now waits up to 100 ms for an image it has just
  asked for; a longer pull still yields the worker and wakes the pod when it completes.
- **perf:** The ordinary lifecycle Events of a start (`Pulling`, `Pulled`, `Created`,
  `Started`, `StartTiming`) are queued and written in order by one background sender instead
  of being awaited: a start waited on three apiserver POSTs per container, between one
  container and the next. Warning Events are still written before the start moves on.

- **feat:** Per-pod start timing (#132). Each pod start is timed from the moment the pod list/watch
  delivers it to the moment the apiserver acknowledges `Running`, across retries: `scheduled`
  (PodScheduled → seen, wall clock), `wait`, `image` (a pull is the registry clone + attach + mount),
  `volumes` (and each volume, the ServiceAccount token as `(serviceaccount)`), `sandbox` (network
  included), `init`, `containers` (and each create+start), `report` (status PUT sent →
  acknowledged), `total`, `attempts`. Published once per start as the pod annotation
  `storm.io/start-timing`, a `StartTiming` Event, the histogram
  `kubelet_pod_start_phase_duration_seconds{phase}` and one INFO log line. A static pod gets the
  log line and histograms only. `kubelet_pod_start_duration_seconds` now counts from the same
  seen moment (the list delivering the pod), not from a worker reaching it.
- **fix:** A node whose filesystem is full no longer fails every new pod (#129). The kubelet
  discarded the errors from creating a pod's emptyDir, configMap, secret, projected and
  ServiceAccount-token directories and resolv.conf, so ENOSPC reached `describe` as "… does not
  exist on this node", and the ENOSPC creating the container log directory was a generic start
  error that marked the pod `Failed` (server3, 11.65: 32 cilium pods in 30 minutes, each deleted
  and recreated by the DaemonSet into the same full disk). All of these are now written before
  the sandbox is created; a write that fails keeps the pod Pending (`ContainerCreating`) with the
  path and errno in a FailedMount Event (a `Failed` Event for an out-of-space log directory), and
  it is retried until it succeeds.
- **fix:** CNI: a plugin exec refused with ETXTBSY ("Text file busy") is retried up to five
  times (40 ms doubling), as libcni does (#128). A binary just written can still be open for
  writing in a process forked before the writer closed it; `del_runs_chain_in_reverse` failed
  that way in sc-build.
- **fix:** A pulled (non-golden) image now starts under the stormpump runtime (#103).
  `create_container` is handed the path `pull_image` returned, `/run/stormpump/images/<volume>`,
  and resolved it as if it named a pallet (`/pallets/<volume>`), so every pulled image, every
  component `/test` image among them, failed at start: "has no root — image … was never pulled".
  That path is now the container's root as given (argv[0] lookup too); goldens resolve as before.
- **fix:** `kubectl logs` on a node service's mirror pod (`kube-system/<asset>-<node>`)
  no longer answers "pod not found on this node" when the service has nothing in its
  stormd log volume (#124). A service that died before stormd wrote anything (stormcluster
  and stormrdp on 11.61), or one not run by stormd (stormblock, registry), is served PID 1's
  record of its last exit: `last_output` from `/run/stormpump/assets.json` (stormpump#51).
  `--previous` falls back to it when stormd kept no `.failed.log`. With nothing recorded,
  the 404/400 names the volume looked in, the last exit and any refused start. (11.61
  itself carries the v0.12.0 kubelet, which predates #72's stormd-volume path.)
- **docs:** Documentation refreshed from the code for changes since
  2026-09-25 (#54, #120, #121). `docs/status.md` is re-audited at fecb331,
  with a table of everything since 09-25 (UID workers, #100 unwind, #101 no
  sync tick, #99 bounded calls, #117) and a table of the owner's answered
  decisions #106–#110 with the issue implementing each. Docs that still called
  them open (status, node-volumes, api, README, CLAUDE.md work plan) now give
  the decision and what is left. New limitation rows: #115, #116, #118,
  #119, #122, stormimds#12.
- **docs:** `--registry`/:5100 is sbregistry, not a stormpump image service
  (#120); `/vmInstance` notes that stormimds keeps its own store, which design
  wins is stormimds#12.
- **docs:** `docs/csi.md`: the plugin registry is followed by filesystem
  notifications (no 2 s scan), a failed registration is retried after 1 s (not
  30 s), each registration and `CSINode` write is bounded at 10 s.
- **docs:** README: the VM disk-owner sweep runs on volume/VM watch events, at
  most once a minute; a waiting golden is retried on its waiting deadline;
  registered machines are reconciled whenever the VMI set is read; blanks are
  shipped sealed and only a missing class is minted.
- **docs:** Module comments (#54): `cmd/kubelet` names the runtimes as they
  are (native default, stormpump on stormcos); `storage.rs` describes PID 1
  mounting a claim's device under `/run/stormpump/pvc` and the container
  binding it, not a child-side mount.

## 2026-09-30

- **chore:** Stage golden golden-rustkube-node-e5db6ac32831 at 9c2f738 (#117), release request stormcos#164.
- **fix:** A VM whose golden is still being imported (stormblock refuses the
  clone with `409 … is not sealed`) waits, "waiting for golden <g>
  (importing)", like a golden not yet created, instead of counting failed
  starts with backoff and FailedStart Warnings (#117).

## 2026-09-29

- **chore:** Stage golden golden-rustkube-node-e8bca700a793 at bba7d54 (#99), release request stormcos#164.
- **fix:** Bound every runtime-side request (#99). The stormpump ring's 30 s
  deadline, previously declared and never applied, now runs from enqueue: a
  request past it is answered `Timeout`, and its late completion still frees
  the arena and is undone if it made something. Each CNI plugin exec is limited
  to 60 s, then killed and reaped. Engine calls get a 5 s connect bound and a
  60 s request bound (1 h for a mint; none for the volume watch).
- **docs:** Runtime deadlines in configuration; #99 acceptance status in the
  event-driven design.
- **chore:** Stage golden golden-rustkube-node-2f39a07e3c07 at 4b648f4 (#101), release request stormcos#164.
- **feat:** No global sync tick (#101). Live Pods and VMIs come back only on
  events or their own deadlines: probe `periodSeconds`/`initialDelaySeconds`
  (upstream timing; previously every probe ran every 2 s), CrashLoopBackOff
  end, waiting-start backoff, init-container limit, VM guest-agent poll.
- **feat:** Service mirror on `/run/stormpump` inotify and mirror-pod watch,
  writing only what differs; system claims and the VM disk-owner sweep on
  stormblock's volume watch plus PV/PVC and VM/VMI watches; reclaim on the PV
  watch; CSI sweep on this node's Pod watch; snapshots on take completion (#101).
- **feat:** `kubelet_timed_reconciles_total{worker,cause}` counts deadline and
  polling-fallback work (#101).
- **fix:** CrashLoopBackOff forgiveness is judged at restart, not only when a
  running container happens to be observed (#101).
- **docs:** Configuration, README, node volumes, CSI, metrics and the
  event-driven design updated for #101. Filed stormpump#67 (assets.json is
  rewritten every supervision pass).
- **chore:** Stage golden golden-rustkube-node-2fb5a1e7ab0d at ac738df (#100), release request stormcos#164.
- **feat:** Unwind a failed, panicked or abandoned VM start: a per-UID ledger
  records each tap deposit before it is sent and each volume/spec handle as it
  is registered; stormpump's `DEPOSIT_WITHDRAW` (op 9, stormpump#63) and handle
  releases undo them. The next start waits while a tap is still held, and a
  deletion keeps its name reservation until the tap is withdrawn (#100).
- **fix:** Serialize the deposit→spawn window and every withdraw across VMs:
  deposits are named `tap-<nic>` per engine client, so two concurrent starts
  with the same NIC name could take each other's tap (#100).
- **fix:** Run CNI DEL after a failed ADD before releasing the sandbox; a DEL
  that fails keeps the sandbox and is retried before the next one (#100).
- **fix:** The claim-reclaim endpoint finishes the detach/delete in its own
  task, so a client disconnect no longer drops the claim's reservation while
  stormblock is still working (#100).
- **docs:** README, event-driven design and status updated for the above.

- **docs:** Record main merge 600b58a verification (297 tests passed, one ignored)
  and completion of #114; outstanding turbomode work continues on main.

- **docs:** Record successful full workspace verification of merged turbomode ec39c2a (#114).

- **feat:** Integrate main into turbomode (#114), preserving tap addresses,
  snapshots, failed-list safety, durable VM disks, startup retry policy and
  access credentials within UID reconciliation. Add adapter regression coverage.
- **docs:** Refresh current architecture and merge status; unfinished #100–#102
  work continues on main without a release or golden.

- **docs:** Record the owner-authorized #114 merge plan and two-branch workspace validation.

- **docs:** Reconcile the worker design with implemented adapters and record
  stormpump#63 as the remaining VM cancellation dependency for #100.

- **fix:** Import the JSON macro in the volume-index regression test; the full
  dev workspace build passed but test compilation exposed the missing import.

- **feat:** Index PV/VolumeAttachment changes back to claim users; retain the
  previous index when a collection cannot be read. Guard VM status, migration
  annotations and PV deletion by observed UID; retry panicked UID adapters (#100).

- **fix:** Retain stormpump container/sandbox records on refused cleanup and
  partial volume registrations on failed startup; serialize CSI publication and
  teardown by driver/handle. Require successful runtime recovery before starts (#100).

- **feat:** Prepare images in a separate four-slot pool, share in-flight pulls,
  and wake only image dependents on completion. Claim reclamation now takes an
  exclusive admission reservation against concurrent starts (#100).

- **feat:** Yield init-container and VM shutdown waits between UID passes;
  reuse partial startup state and retry finalizer cleanup with UID guards.
- **test:** Cover staged init resume/delete, deletion during an active operation,
  same-name UID replacement and per-claim mutation exclusion (#100).

- **feat:** Connect Pod/VMI UID adapters to one eight-worker executor with
  recovery barriers, shared claim admission, per-claim mutation exclusion and
  retained partial Pod-start records. Runtime/probe deadlines remain for #101.

- **feat:** Add independent desired-state sources, inverse claim/image/driver
  indexes and recovery admission seeding to the common executor (#100).

- **fix:** Match the QUERY exit helper to the ABI u32 completion field; dev
  compilation of 8ebb8b3 caught the mismatch.

- **fix:** Retain VM records/finalizers on unknown exit or refused disk cleanup;
  acknowledge Pod deletion with the observed UID (#100).

- **docs:** Record #100 restart plan and the decision to defer the main merge.

## Unreleased — turbomode (not yet built or measured)

- Remove the two-second inline template-mint wait; completion notifies Pod
  and VM queues directly. Completion and slow-mint tests await dev validation.

- CSI registration now wakes on filesystem changes and signals Pod workers;
  retry deadlines exist only for pending registration/publication failures.
  Failed directory scans retain registrations. Unbuilt; dev validation pending.

- Watch-driven Pod/VMI work, separate Pod/VM loops, stormpump exit and Linux
  manifest notifications. Preserve live Pods when desired-state reads fail.
- Stable startTime, unchanged-status suppression and revision-guarded status
  writes. Failed terminating-Pod teardown is not acknowledged as complete.
- Paired architecture/measurement plan in docs/event-driven-design.md;
  #99–#102 track remaining indexed workers, local events and real-node tests.


## [Unreleased]

### 2026-09-29
- **docs:** Recheck #53 and record the unanswered restore decision on #109 as
  its work-plan blocker; preserve existing snapshot verification separately.
- **docs:** Refresh README, build/storage/metrics references and planning docs
  against main 5bb1a38 and history since September 18 (#54). Add complete
  CLI/environment defaults, ports/routes, shipping workflow and an issue-linked
  capability audit; preserve the built-in stormblock PVC description. Separate
  experimental turbomode and unverified live acceptance from main behavior.
- **docs:** Record passing remote build/tests at ad43ee9 and the CLI/route/link audit;
  stage-golden follow-up is blocked by platform fetch authentication (stormcentral#161),
  with the scheduling dependency proposed and awaiting approval.
- **docs:** Track uncovered sidecar/backoff/endpoint-precedence gaps as
  #111/#112/#113; correct no-cni and legacy builder documentation without changing code.

<!-- New unreleased changes go here -->

### 2026-09-29 (a VM's disks outlive its VMI, #75)
- **fix(kubelet):** a VM stop deleted every disk the machine had made, and a
  golden was cloned on every start, so each VirtualMachine restart brought
  the guest back as a fresh image.
  - A golden clone or emptyDisk is found by name (`<ns>.<vmi>-<disk>`) and
    reattached. It is cloned or created only when missing, and a failed
    listing is a failed start.
  - A stop only detaches.
  - Each disk gets a stormblock owner: the VMI's VirtualMachine, or the VMI
    itself without one. A sweep, at most once a minute, deletes disks whose
    owner is gone for good (404, another uid, being deleted), never on an
    unanswered GET.
  - A disk left by an earlier VM of the same name is not reused: the start
    waits for the sweep.
  - `storm.io/retain-disks: "true"` keeps a machine's disks unowned, so they
    are never swept.
  - The seed is namespaced and replaced each start.
- **feat(kubelet):** `EngineClient::put`.

### 2026-09-29 (VirtualMachineSnapshot, #53)
- **feat(kubelet):** a `VirtualMachineSnapshot` of a VM this node runs is
  taken.
  - The node marks it `storm.io/snapshot-node` (rv-guarded) and sets it
    `InProgress`.
  - stormvm freezes, pauses, takes one stormblock group snapshot of every
    volume, then unpauses and thaws.
  - The object gets `Succeeded`/`Failed`, the group id as
    `virtualMachineSnapshotContentName`, `sourceUID`, `indications` and an
    Event. `failureDeadline` is honoured.
  - A take interrupted by a restart is taken again (idempotent by name).
  - Nothing happens until the CRDs are installed (stormcos#170).
  - `VirtualMachineRestore` waits on an owner decision (#53).
- **feat(kubelet):** Events can name any kind of object, not only a Pod.

### 2026-09-29 (the console router knows where stormblock is, #83)
- **fix(kubelet):** stormvm's console router was mounted without
  `Config.stormblock`, so its `snapshot` verb answered 409 "this service was
  not told where stormblock is" on every node. Both mounts now pass the
  kubelet's engine URL (`--stormblock`). The verbs are not yet routed onto
  `:10250` (#94).

### 2026-09-28 (a failed VM start is retried, #76)
- **fix(kubelet):** a VM start that failed was recorded Failed and never tried
  again, so a moment of stormblock being down left the machine dead until it
  was recreated.
  - It is now retried with backoff (10 s doubling to 5 min, and at once on a
    spec change), Pending with the reason and a Warning Event each attempt.
  - It is Failed only when the VirtualMachine's `runStrategy` is `Once` or
    `Manual`.
- **fix(kubelet):** a failed start deletes the golden clone and seed it made.
  It used to only detach them, which would leave two volumes behind per retry.

### 2026-09-28 (accessCredentials, #92)
- **feat(kubelet):** a VMI's `spec.accessCredentials` is honoured.
  - `noCloud` / `configDrive` keys are read from their Secrets into the
    seed's `public-keys` at start.
  - `qemuGuestAgent` keys are set through the agent once it answers, and again
    whenever the Secret changes, without a reboot.
  - `AccessCredentialsSynchronized` is reported in `status.conditions`, merged
    with any others.

### 2026-09-28 (a bridged VM's address from its tap, #91)
- **feat(kubelet):** a VM NIC on a node bridge is watched on its tap from
  before the spawn (stormvm-net `snoop_tap`, stormvm 2be5900). The address the
  guest leases from the segment's DHCP server reaches
  `status.interfaces[].ipAddress` as soon as it is seen. The tap watcher is
  preferred over the guest agent, and the agent over the neighbour table. A
  guest with no agent that the node had never talked to used to report no
  address at all.
- **chore(deps):** stormvm crates 1b0d941 → dc1b7ea. stormpump stays at 30a76d3:
  stormvm-node still builds `stormpump::spec::Mount` without `propagation`.

### 2026-09-28 (stale mirror pods, #87)
- **fix(kubelet):** a node service that PID 1 did not start on this boot kept
  its mirror pod's status from the previous boot (Running, the old
  `startTime`), because the mirror only wrote assets in the table. Such a
  mirror is now marked Pending, with its container waiting `NotStarted` and a
  Warning Event. It is written once and never deleted.

### 2026-09-28 (node service logs, #72)
- **feat(kubelet):** `kubectl logs -n kube-system <asset>-<node>` reads the
  service's stormd log volume: the volume its boot unit mounts at
  `/var/log/stormd`, seen under `/hostroot`. The current run covers every
  process with its rotations, merged in time order. `--previous` is the newest
  failed run. tail, since, timestamps, limit and follow work. It used to answer
  "not found on this node".

### 2026-09-28 (PVC size test, #64)
- **build(test):** `test/build.sh` builds the static test binary on the build
  box, and `test/Containerfile` only copies it into a scratch image. The image
  used to compile inside `podman build` from `rust:1-alpine`, with no cache.

### 2026-09-27 (PVC size test, #64)
- **fix(kubelet):** a claim for a fractional size (`3.5Gi`) did not parse, and
  a claim that does not parse was read as asking for nothing, so it got the
  1 MiB class. Fractions now parse and round up to a whole byte.
- **fix(kubelet):** when the control plane wrote a claim's PV first (with the
  claim's request as its capacity) and the binder bound it, the kubelet left
  both as they were. A 3.5Gi claim on a 4 GiB volume said 3.5Gi for good. The
  existing PV's capacity and CSI source are now brought up to what was
  provisioned, and so is the claim's status capacity.
- **test:** `test/`, the test container per stormcentral's test standard, with
  a medium suite of PVC sizes. It covers every class and arbitrary sizes, each
  bound, sized, written, read, deleted and reclaimed; 2Ti must be refused with
  the reason. Short and long report a skip until #61.

### 2026-09-27 (a pod waiting on its claim, #63)
- **fix(kubelet):** a pod whose volumes were not ready was reported Pending and
  then forgotten. `kubectl logs` said "not found on this node" and the pod had
  no container statuses. It is now recorded as waiting: every container reports
  `waiting: ContainerCreating` with the reason, `/pods` lists it, `logs` answers
  400 "waiting to start", and a `FailedMount` Event is written. After 5 minutes
  the reason says it timed out, and it keeps retrying.
- **fix(kubelet):** minting a size-class blank was one synchronous engine call,
  so a 1 TiB blank's format held the whole sync loop. It now runs in the
  background, one per blank, waited for inline for 2 s. A template that is not
  `ready` is a wait that names its state, not a clone attempt.
- **fix(kubelet):** a container status with no container id leaves out
  `containerID` instead of reporting `containerd://`.

## [v0.13.0] — 2026-09-27

### 2026-09-27 (VM lifecycle, #35)
- **fix(vm):** a failed VirtualMachineInstance list (an apiserver that did not
  answer, or an error status) was read as "no machines here". Every VM on the
  node was stopped and its root deleted. The pass is now skipped.
- **fix(vm):** a VMI being deleted (`deletionTimestamp`) was still treated as
  wanted, and its machine kept running. It is now stopped. While a machine
  runs, its VMI holds the finalizer `storm.io/vm`, which comes off once the
  machine is stopped, so the delete completes only after the VM is gone.
- **fix(vm):** a VM outlives a kubelet restart, but its handle lived only in
  memory. A restarted kubelet could not stop it, and deleting the VMI left the
  hypervisor running (test2 on C2NR0Q2). The registration
  (`/run/stormvm/<ns>/<name>/vm.json`) now records the workload handle and the
  disks. Each sync adopts a registered machine that its VMI still wants, and
  stops one that nothing wants.
- **fix(vm):** a machine with no recorded handle (started by an older kubelet)
  whose VMI is gone is stopped through its control socket: ACPI, a 30 s
  grace, then `quit`.
- **fix(vm):** a stopped machine's workload handle is released in the engine
  (its pidfd and cgroup), as an ended one's already was.

### 2026-09-27 (node volumes as PV + PVC sets, #59)
- **feat(kubelet):** every `<component>-data`, `-state` and `-logs` volume on
  the node is a PV and its bound PVC in `kube-system`, labelled
  `storm.io/volume-kind` (`data`, `state`, `logs`) and `storm.io/component`.
  The `-logs` volumes were not represented at all.
- **feat(kubelet):** the pair reads like a dynamically provisioned claim: the
  PV's `claimRef` carries the claim's uid, `csi.fsType` and `volumeAttributes`
  (golden, fs uuid), and health, access and role annotations. The PVC carries
  `bind-completed`, `bound-by-controller`, `storage-provisioner` and
  `selected-node`.
- **feat(kubelet):** a reconciler, not create-once. Every 30 s a missing
  object is made again, a grown volume grows its objects (a request never
  shrinks), and labels, annotations and the claim's uid are kept current.
  Nothing is written when nothing changed. Only objects annotated with this
  node are touched.
- **feat(kubelet):** a volume that goes away has its claim deleted, so the
  binder marks its PV Released. The PV is never deleted (Retain).
- **feat(kubelet):** the built-in driver's claims (`bind_claim`) use the same
  builder: the PV names the claim's uid and carries fsType and attributes, and
  the claim gets the same binding annotations.

### 2026-09-27 (stormpump limits and stats, #57)
- **feat(stormpump):** a container's resource limits reach the engine.
  `limits.memory` becomes `memory.max`, with `memory.swap.max = 0` as
  upstream gives a limited container with swap off. `limits.cpu` becomes
  `cpu.max`. A declared limit is applied or the spawn is refused by
  stormpump, so a limit is never silently dropped again. The CPU *request*
  is not mapped to `cpu.weight` yet (open on #57).
- **feat(stormpump):** container stats from the engine's `QUERY`: CPU and
  memory for `/metrics/cadvisor` and `/stats/summary`. Memory is
  `memory.current`, which includes page cache.
- **fix(stormpump):** the ring client gives the shared arena to one request at
  a time. Every payload was written at offset 0 while other requests could
  still be in flight. A request can also have its region read back after it
  completes.

### 2026-09-27 (metrics, #36)
- **feat(kubelet):** `/metrics` under upstream's names, through the Prometheus
  recorder rustkube's components use. It carries `kubelet_running_pods`,
  `kubelet_running_containers{container_state}` (it had no label),
  `kubelet_pod_start_duration_seconds` and
  `kubelet_pleg_relist_duration_seconds` (histograms, upstream's buckets),
  the `process_*` family (apimachinery's collector) and
  `kubernetes_build_info` with the kubelet's own version.
- **feat(kubelet):** `/metrics/cadvisor` adds `container_fs_usage_bytes` (CRI
  writable layer) and `container_network_{receive,transmit}_bytes_total`
  (per pod, per interface). Every series is labelled
  `{container,id,namespace,pod}`. A stat the runtime did not report is no
  series, where it used to be 0, and `/stats/summary` leaves it out the same
  way.
- **feat(stormpump):** pod network counters, read from the sandbox holder's
  `/proc/<pid>/net/dev`.
- **docs:** `docs/metrics.md`.

### 2026-09-27
- **docs(build):** a release golden is a stage golden
  (`stormcentral component stage rustkube-node`), not `component build` or
  `scripts/build-golden.sh`. Those make bin-only goldens, and 11.49 shipped
  one and the kubelet could not start.

## [v0.12.0] — 2026-09-27

### 2026-09-27 (VM disks, #73, #74)
- **feat(vm):** `emptyDisk` volumes (stormvm's `DiskSource::Empty`, #73). The
  VM's `<ns>.<vm>-<disk>` volume is found by name, or created blank with the
  disk's capacity, the engine's default redundancy and the `storm.io/vm`
  label. An existing one is reused, and it is owned by the VM, so until #75
  a stop deletes it like the VM's other disks.
  An engine that cannot list volumes fails the start rather than making a
  second blank.
- **fix(vm):** a `persistentVolumeClaim` disk is resolved as a claim
  (stormvm's `DiskSource::Claim`, #74). It used to reach stormblock as a
  volume id and fail with "invalid UUID". The pod manager resolves and
  attaches it exactly as for a pod. The VM waits while the claim is unbound,
  of another class, or used by a pod on this node, and deleting the VM keeps
  the claim's volume.
- **fix(vm):** which disks a VM owns (and deletes) is an explicit list:
  golden clones, the cloud-init seed, and empty disks.
- **chore(deps):** stormvm 1b0d941 (v0.10.0), with stormpump 30a76d3 and
  stormcast 801f822, as `cargo update -p stormvm-spec` resolved them.

## [v0.11.0] — 2026-09-27

### 2026-09-27 (engine token, #66, stormcos#104)
- **fix(kubelet):** every call to the node's stormblock engine carries the
  engine's token (`Authorization: Bearer`). stormblock 17 refuses its API
  without one, so claims, VM disks and image pulls stopped attaching. The pod
  manager sent the apiserver's token, and system_claims, the VM manager and
  the stormpump image service sent none. They now share one `EngineClient`
  (`engine.rs`) built from `--stormblock` and the token lookup stormblock's
  CLI uses: `$STORMBLOCK_API_TOKEN`, then `$STORMBLOCK_TOKEN_FILE` (default
  `/run/stormblock/engine/api_token`), then `/etc/stormblock/api_token`, then
  `/var/lib/stormblock/api_token`. While no token is found it is looked for on
  every call, because the engine mints it at start and the kubelet may be
  first. After a 401 it is read again and the call retried once.
- **fix(build):** `scripts/build-golden.sh` sends the engine's token, found the
  same way, on curl's stdin rather than its command line.
- **BREAKING (library):** `Kubelet::with_engine` takes only the ring (the engine
  is `KubeletConfig::engine`). `VmManager::with_storage` and
  `StormpumpImages::with_storage` take an `EngineClient`, and so does
  `system_claims::mirror`.

## [v0.10.0] — 2026-09-26

### 2026-09-26 (golden build, #58)
- **fix(build):** rustkube's `apimachinery` is a git dependency pinned by
  `rev` (rustkube v0.15.2, e7f4fdb), not `path = "../rustkube/..."`. The
  golden build fetches this repository alone and runs `--locked`, and it
  could not load the workspace without the sibling. `Cargo.lock` records the
  commit, so a rustkube bump is an explicit change here.
- **chore(build):** `scripts/sc-build.sh` and `.deps/` are gone. They cloned
  rustkube into sc-build's scratch tree, and plain `sc-build` now works.

### 2026-09-24 (external CSI drivers, #52)
- **feat(csi):** `csi.rs` is a real CSI node client: gRPC over the driver's
  Unix socket, from the vendored CSI v1.9.0 proto. It was a stub that logged
  each call and created the directories, and it never reached a driver.
- **feat(csi):** driver registration. Registrar sockets in
  `/var/lib/kubelet/plugins_registry` are found (GetInfo), the driver is
  asked about the node (NodeGetInfo, NodeGetCapabilities), and `CSINode`
  lists exactly the registered drivers with their node IDs and topology
  keys. Topology becomes node labels, and the registrar is told the outcome.
  A removed socket deregisters its driver.
- **feat(csi):** a claim bound to another driver's PV mounts. The kubelet
  waits for its VolumeAttachment when the CSIDriver requires attach, stages
  to upstream's `globalmount`, publishes to the pod's `kubernetes.io~csi`
  directory, and has the engine bind that directory. It unpublishes when the
  pod goes, and unstages when the last pod on the node lets go. Records
  (`vol_data.json`) are written before any driver call, and a 30 s sweep
  undoes what a restart or a failed start left behind.
- **feat(csi):** inline `csi:` volumes, for drivers that allow `Ephemeral`.
  Generic `ephemeral:` volumes resolve to their `<pod>-<volume>` claim and
  wait for it, with a reason (rustkube#94: nothing creates it yet).
- **fix(csi):** a published volume is given to a pod only when it is a mount
  point in PID 1's mountinfo. A driver's mount that stayed in its own
  namespace would otherwise hand the pod an empty directory. That is the
  state until the engine does Bidirectional propagation (stormpump#35).
- **fix(kubelet):** Bidirectional mount propagation is for privileged
  containers only, as upstream rules. An unprivileged container is mounted
  Private, with a warning.
- **fix(kubelet):** CSI volumes are not SELinux-relabelled.
- **build:** `scripts/sc-build.sh` builds on the build box without a sibling
  rustkube checkout, by cloning it inside the scratch tree.
- **docs:** `docs/csi.md`, including the mount-propagation decision.

### 2026-09-24
- **fix(runtime):** a container's volume registrations (root, logs, mounts) are
  released when it is removed. They never were, so a claim's device mount
  outlived its pods, the claim was detached and deleted under the live
  filesystem, and every `sync` on the node hung on the recoverable ublk device.
  The last release now unmounts (stormpump 23aaab7), and stormblock refuses a
  detach while mounted (0718bd1).
- **fix(runtime):** a volume release answered EBUSY (a mount still in use) is
  retried every 2 s for a minute in the background, not dropped.

### 2026-09-23 (PVCs)
- **fix(storage):** claims find the blanks the image ships. The kubelet looked
  for `pvc-1M`; the image and sbregistry name them `pvc-ext4j-<MiB>m`. Minting
  read a field stormblock does not return. So every claim fell back to a
  scratch directory that does not survive the pod. Templates are now found,
  minted and cloned through `/api/v1/fstemplates`, whose clone gives each
  claim its own filesystem UUID.
- **feat(storage):** the size ladder runs to 1 TiB in x4 steps (1M … 1T). It
  stopped at 1 GiB, so an ordinary application asking for 20Gi was refused.
- **feat(storage):** a claim with `dataSource`/`dataSourceRef` is a CoW clone
  of it: another claim (across namespaces with `dataSourceRef.namespace`), or
  a golden (`apiGroup: storm.io, kind: Golden`). Cloning data volumes is the
  built-in storage class.
- **fix(storage):** the PV a node publishes is named `pvc-<ns>-<claim>`, the
  contract with the control plane, not `pvc-pvc-<ns>-<claim>`, which gave
  every claim two PVs. It carries node affinity.
- **feat(storage):** a claim bound to a stormblock PV mounts that PV's volume.
- **feat(storage):** the node's own data containers (`*-data`, `*-state`) are
  listed as bound PVCs in `kube-system`, beside the services' own pods, with PVs `storm-<volume>` (reclaim
  Retain) and their golden as `dataSourceRef` (#49). They can be cloned; a pod
  mounting one directly is refused, because the service has it mounted.
- **feat(storage):** the node reclaims its own released claims. A stormblock PV
  pinned here that the binder has moved to Released with policy Delete has its
  clone deleted (refused while a pod here still has it) and then the PV. They
  sat Released for ever before (rustkube#71). System volumes are Retain.
- **fix(storage):** a claim's block device is mounted by the engine at
  registration (`/run/stormpump/pvc/<dev>`) and bound into the container, the
  same path an image pull takes. Registering the device node itself as a
  directory to bind failed every claim with `ENOTDIR (attaching mounts)`.
- **fix(storage):** blanks minted on demand ask stormblock for `role: data`, so
  claims live in the half no install formats.
- **fix(storage):** cloning a claim flushes first (`sync(2)`). The clone is a
  block snapshot, and a file written a moment before was still in the page
  cache and missing from the clone; verified on the R230.
- **fix(storage):** a claim that cannot be provisioned makes the pod wait
  (`VolumeNotReady`, retried every sync, reason in `describe`) instead of
  starting it on a scratch directory whose data would not persist. Only
  `emptyDir` is scratch now; any other unsupported volume type waits with a
  message naming it.

### 2026-09-20
- **feat(build):** `scripts/build-golden.sh` — what this repository ships is a
  golden, not a package. It builds the static musl binaries, asks the forge for
  a volume, attaches it over NVMe/TCP, makes a read-only filesystem on it,
  copies the binaries in with `install`, and seals it. No tar and no image
  file: a golden **is** a filesystem, and the forge can hand the build box the
  namespace it will live in, so it is written where it belongs with ordinary
  `cp` rather than serialised into an archive and read back out. The result is
  a named, sealed volume with a content digest, which a release composes over
  by mapping rather than copying.
- **docs:** `docs/BUILD.md` — the new build, and why. Includes why the rpm and
  deb are the wrong shape for a node that installs nothing, and why they could
  never have worked anyway: `build-packages.sh` runs `cargo build --release`
  without `--target`, so it packages glibc binaries a node cannot exec.
- **feat(build):** the golden records what produced it —
  `rustkube-node@<commit> kubelet:<digest> kube-proxy:<digest>` — at
  `/etc/rustkube-node.provenance`, with `+dirty` when the tree is not clean. A
  digest says the bytes are these bytes; it does not say what made them, and
  that is the question asked when something is wrong.

### 2026-09-20
- **fix(test-cluster):** stop keeping a second, wrong copy of the fixture's
  artifact pins in `config.sh` (#27). `terragrunt.hcl` is what cloud-init
  templates from, so it is the only thing that decides what a VM installs;
  the copy in `config.sh` was documentation of that and had drifted to name
  rustkube-node v0.1.0 and rustkube v0.7.1 while the rig actually installed
  v0.2.3 and v0.7.33. Nothing read it, so nothing caught it, and anyone
  reading it to find out what the fixture runs — which is the first thing
  #27 asks — was told the wrong answer.

## [v0.9.0] — 2026-09-20

### 2026-09-20
- **feat(kubelet):** mount stormvm's console router instead of splicing to a
  second process (#43). `/vmConsole/{ns}/{name}/{door}` is now answered by
  `stormvm-console`'s own `axum::Router`, running in this process. There is
  no standalone node — every node runs rustkube, so every node with a VM on
  it already has a kubelet, and a long-lived daemon whose only job was to
  serve consoles was one that never needed to exist. What goes with it: a TCP
  connect per session, ~200 lines that re-implemented an HTTP client by hand
  (parsing a response head, carrying the bytes that arrived after it), the
  `STORMVM_CONSOLE_ADDR` escape hatch, and stormvm's weaker
  "loopback, or a token" auth rule — the doors inherit this server's TLS and
  TokenReview-validated bearer auth instead. Closes stormcos#39 and makes
  rustkube-node#38 non-load-bearing.
- **BREAKING:** `pkg/kubelet` is on **axum 0.8**, matching stormvm-console
  and the apiserver — a 0.8 `Router` cannot nest in a 0.7 one, which is why
  the console was reached over a socket in the first place. All route
  patterns move from `:param` to `{param}`; the two spellings were previously
  a standing source of 404s that read as "no such pod" rather than "no such
  route". `axum-server` 0.7 serves an 0.8 router unchanged. `hyper` stays a
  direct dependency for `hyper::upgrade::OnUpgrade`.
- **fix(kubelet):** three things the mounted console needs that its own
  listener used to provide, each a runtime failure no type catches. The
  request is rebuilt rather than re-addressed, because axum keeps a route's
  captured segments in an extension and the inner router appends its own —
  three plus two meant every door saw `Wrong number of path arguments`.
  `ConnectInfo` is injected as loopback, since axum only inserts it for a
  server built with `into_make_service_with_connect_info` and, mounted, the
  caller genuinely is this process on the node. The `Authorization` header is
  stripped: it is the apiserver's token, already spent, and the door checks a
  presented token before it considers loopback, so forwarding it would refuse
  every authenticated console request. The upgrade handle is carried across
  the rebuild, or the WebSocket cannot complete.

## [v0.8.0] — 2026-09-20

### 2026-09-20
- **feat(kubelet):** `DELETE /volumes/{namespace}/{claim}` on `:10250`, so a
  `Delete` reclaim policy stops leaking (#46, filed from rustkube#71). The
  control plane provisions and binds the PV but cannot delete the clone:
  stormblock's management API is loopback, so only the node can reach the
  engine holding the volume. The controller was leaving the PV `Released` and
  emitting `VolumeNotDeleted` on every pass — deliberately, because deleting
  the object without the clone turns a visible leak into an invisible one,
  and the volume name is derived from the claim's, so a later unrelated claim
  of that name in that namespace would adopt the previous tenant's data. Same
  shape as the VM console (rustkube#61): no new auth and no new reachability
  assumption. The name is resolved through `storage::volume_name`, the same
  function that created it. It detaches before deleting, because the attach
  outlives the pod and stormblock refuses to delete a volume it still serves.
  A pod on this node still holding the claim is a `409`, refused rather than
  queued. Absent is `204`, so a retrying controller settles. An engine that
  cannot be reached, or an apiserver that cannot confirm the claim is unused,
  is a `503` — "I could not check" is not "nothing is using it" when the
  answer destroys data, and answering `204` there would delete the PV over a
  volume that is still allocated.
- **feat(kubelet):** enforce `ReadWriteOncePod` at the mount (#42). The
  scheduler filter is what keeps a second pod Pending with a readable reason,
  and it cannot see the two ways around it: a static pod, or one written
  straight onto `spec.nodeName`, never passes a scheduler filter at all. The
  kubelet now refuses the second mount of an RWOP claim while another
  non-terminal pod on this node holds it, as upstream does — a guarantee that
  holds for scheduled pods and quietly does not for unscheduled ones is worse
  than not offering the mode, because a database that asked for exclusivity
  gets a volume that only says it has it. The refusal leaves the pod Pending
  with the holder named, and deliberately does not take the scratch-directory
  fallback: handing a pod an empty directory here would report the guarantee
  as kept. The claim's own pod re-resolving its volumes on restart does not
  count as a second holder, nor does a Succeeded or Failed one.
- **fix(kubelet):** honour *pod-level* `securityContext.seLinuxOptions`, and
  label the sandbox with it (#26). The container-level passthrough landed
  already, with a comment saying the pod-level fallback was applied "via the
  caller" — no caller did: `apply_pod_namespaces` inherited the pod's seccomp
  profile and not its SELinux label, so a pod that set the label once for all
  its containers, rather than on each, got `container_t` and the denials the
  passthrough existed to prevent. The parse is now one shared helper used by
  the container, the pod fallback and the sandbox, so the three cannot
  disagree, and the sandbox — which owns the namespaces its containers join —
  carries the pod's label instead of being left at the default type.
- **fix(kubelet):** put the Node object back when it disappears underneath a
  running kubelet (#31). The heartbeat's status PUT 404s once the object is
  gone — an admin `kubectl delete node`, node GC, an etcd restore that rolled
  it back — and the old code logged a warning and retried the identical PUT
  every 10 s forever, so the node stayed absent until someone restarted the
  kubelet. That is not a transient failure with a retry: the object the PUT
  addresses does not exist, and only a POST can bring it back. A 404 is now
  told apart from every other status failure and answered with `register()`,
  as upstream's status manager does; other failures still just retry, and the
  409 path inside registration deliberately does not re-register, so the two
  cannot chase each other.
- **feat(kubelet):** mint a blank filesystem template on first use instead of
  requiring the image to carry one (#45). `storage.rs`'s module doc has always
  said the template is minted the first time a size class is asked for; the
  code looked it up and gave up, and a second doc comment then documented the
  workaround — so two comments in one file contradicted each other and the
  class ladder was capped at whatever the image happened to ship. A claim
  above the largest shipped class was refused outright and adding a class
  meant rebuilding an image. One `mkfs` ever, per class, per node.
- **fix(kubelet):** provision only claims that belong to this node's
  StorageClass (#44). Every PVC a pod mounted became a stormblock clone
  regardless of who else owned it — harmless while this is the only
  provisioner, and silent the moment it is not: a CSI driver binds the claim
  to its own PV while this node clones a second volume and the pod runs on
  that one, leaving a real, allocated volume mounted by nobody and a claim
  that looks healthy pointing at no data. `storageClassName: ""` is an
  explicit opt-out rather than "no opinion", matching the binder's
  `claim_class`, so a static PV is no longer provisioned over.
- **fix(kubelet):** a claim belonging to another provisioner no longer falls
  back to scratch. The fallback's rationale is about storage that is
  *briefly* unreachable; a class mismatch is permanent, so the same path gave
  a pod scratch storage forever. It now waits with the reason in
  `kubectl describe`, which is what upstream does and what can be diagnosed.
- **fix(kubelet):** build against stormvm main again, and stop deriving two of
  a VM's names without its namespace (#41). `Registration::of` takes the
  namespace from the spec now and `console::remove` needs it, which were the
  two compile errors; the two that compiled and were wrong mattered more. The
  run directory is `<RUN_ROOT>/<ns>/<name>`, taken from `MachinePlan::run_dir`
  rather than rebuilt — a second `format!` that disagreed left qemu binding
  sockets into a directory nobody made, with nothing in the failure naming the
  path. And volume names now come from `stormvm_node::start::volume_name`
  (`<ns>.<name>-<disk>`) with the label from `vm.id()`: stormblock's namespace
  is flat and `clone_volume` does not check uniqueness, so `default/web-1` and
  `staging/web-1` both asked for `web-1-root` and `volume_by_name` returned
  whichever the map iterated first.

## [v0.6.0] — 2026-09-09

### Added
- **feat(kubelet):** `GET /vmConsole/{ns}/{name}/{door}` on :10250 — a VM's
  serial or VNC console, spliced through to stormvm on loopback. This is the
  node half of rustkube#61: the apiserver serves
  `subresources.kubevirt.io/v1` so `virtctl console` has something to resolve,
  and it cannot reach stormvm itself, because stormvm is loopback-bound and
  mints its one-attach tokens only from loopback. The kubelet is on the node
  and is already an authenticated hop, so the console takes the route that
  already exists rather than a second auth scheme.
- The hop is transparent: nothing parses a WebSocket frame. The client's
  handshake headers go up verbatim (`Sec-WebSocket-Key` included, so the
  accept value stormvm computes is the one the client checks), stormvm's `101`
  comes back verbatim, and a refusal is passed through with its own status so
  "no such VM" reads as itself. `STORMVM_CONSOLE_ADDR` overrides the default
  `127.0.0.1:9095`.

## [v0.5.0] — 2026-09-09

### Fixed
- **fix(kubelet):** **self-healing** — a `restartPolicy: Always` pod is no
  longer marked `Failed` when a container exits (#25). It stays `Running` with
  the container in `waiting`, which is the contract of `Always`. The old
  behavior stranded pods: the sync loop skips terminated pods, so a
  cilium-agent DaemonSet pod that crashed sat `Failed` for hours and deleting
  it was the only way out.

### Added
- **feat(kubelet):** CrashLoopBackOff — a per-container exponential restart
  backoff, 10s doubling to a 300s cap, reset once a container has stayed up
  for ten minutes (#25). The first restart is still immediate; every recreate
  path is gated (container exit, a record the runtime pruned, and the
  missing-container reconcile), which is the same root as the 889-pod
  cilium-operator runaway that recreated on every 2s sync tick. The reason and
  the remaining wait reach the apiserver, so `kubectl get pod` prints
  `CrashLoopBackOff` rather than `ContainerCreating`.

### Fixed (cont.)
- **fix(kubelet):** the sync loop no longer skips a `Failed` pod whose
  `restartPolicy` is `Always` — such a phase should not exist, and skipping it
  is what turned the mistake into a pod nothing would ever restart. A
  genuinely finished `Never`/`OnFailure` pod is still left alone.

### Changed
- **build:** add `[profile.release]` — opt-level 3, thin LTO, one codegen unit,
  `strip = "debuginfo"` (#30). The workspace had no release profile at all, so
  binaries shipped into the read-only erofs root with Cargo's defaults and full
  debuginfo, and there is no package manager on an immutable node to slim them
  down later. Matches rustkube's profile: the two halves of one control plane
  should not be built to different settings. Measured on the kubelet: 16.6 MiB,
  against 14.0 MiB if the symbol table went too — the 2.6 MiB buys backtraces
  that name functions. LTO and codegen-units are the part likely to matter, not
  the size: kubelet and kube-proxy are on the node's hot path.

## [v0.4.0] — 2026-09-09

### Added
- **feat(kubelet):** a VM the kubelet starts is registered for stormvm's
  console doors — `vm.json` is written into the run directory beside the
  sockets the hypervisor binds, and removed on stop and on every failure path
  after the write (#38). Without it `/api/v1/vms` was empty and every attach
  404'd for machines the kubelet started, though `stormvm start` registered
  its own.

### Fixed
- **fix(kubelet):** NIC naming and the ioctls that make a tap are
  `stormvm-net`'s now, replacing the local `vm_net` module (#39). Two bugs go
  with it: `ifr_ifindex` was read through the `flags` arm of `ifreq` — an
  `int` read as a `short`, correct below 32767 and truncating above it, so a
  node that has churned enough pods enslaves the wrong interface or none, with
  the tap present and no frame reaching the bridge; and the derived tap name
  and **MAC** ignored the namespace, so `default/web-1` and `staging/web-1`
  collided — two guests with one address on a shared segment.

### Breaking
- **BREAKING:** derived MACs change, because the namespace now enters the
  hash. A guest keyed on its old address (a DHCP reservation, an ARP entry)
  sees a new one once.

## [v0.3.0] — 2026-09-09

### Added
- **feat:** kubelet `/containerLogs` honors `follow` — the log so far is sent,
  then whatever is appended to it, until the container leaves this node or the
  client hangs up. `follow` was accepted and ignored, so `kubectl logs -f`
  printed once and stopped (rustkube-node#34).
- **feat:** kubelet `/containerLogs` honors `limitBytes` — a budget spent across
  the initial read and every followed chunk, cut on a character boundary.

### Changed
- **perf:** a followed log streams through a bounded channel instead of being
  read whole into a `String`, so a large or open-ended log does not sit on the
  kubelet's heap.

### 2026-09-20
- **fix:** `initContainerStatuses` is reported (#47). The kubelet ran init
  containers and said nothing about them, so cilium's six appeared in a
  console as six components of unknown health with no way to distinguish
  "ran and succeeded" from "never ran". Each report carries
  `state.terminated` with the exit code, reason and start/finish times.
- **fix:** the `Initialized` condition is computed rather than hardcoded
  `True`. It was True before the init containers ran, while they were
  running, and after one had failed — a pod wedged in init reported
  `Initialized=True` with no containers, which is worse than Unknown because
  it is confidently wrong. It is driven by the declared count, because "none
  reported" and "none declared" are exactly the two cases that must not read
  the same.
- The report is taken at the moment each init container exits, before the
  `remove_container` that follows, and kept in `PodState`: once removed the
  runtime cannot be asked what it did, and `check_pod_status` rebuilds the
  status every cycle, so a report held anywhere else would appear once and
  vanish. A failing init container is now reported rather than only becoming
  an error string — which one failed is the whole answer to why the pod will
  not start.
- **fix:** `startedAt` and `finishedAt` were stamped with `now()` — the instant
  the status was *reported*, not the instant anything happened. Every
  container claimed to have started seconds ago on every poll, so one that had
  been up for an hour was indistinguishable from one that had just been
  restarted; an apiserver running since boot was read as having restarted. The
  CRI status already carried the real times and they were being discarded. A
  terminated container now also reports `startedAt`, which is what makes a
  duration computable. A time the runtime did not give renders as `null`
  rather than 1970, which sorts first and looks like a fact.
- **fix:** the kubelet said `no static pod dir /etc/kubernetes/manifests` on
  every sync forever. Static pods are read once per sync interval, and no
  stormcos node has that directory — `stormpump`'s boot.d units are the
  mechanism — so the line repeated every few seconds on every node. A missing
  directory is a fact about the configuration, not an event: it is now said
  once. A directory that exists but cannot be read is the opposite — that is
  actionable, so it became a warning that keeps repeating.
- **fix:** the event POST checked only for a transport error, so an apiserver
  that accepted the connection and rejected the object — a 422 on a field it
  did not like, a 403, a 404 on a namespace — recorded nothing and said
  nothing. "No events at all" then looks identical to a kubelet that never
  tried, which is the one explanation the logs could not distinguish it from.
  A rejected event is now a warning naming the status and the body.
- **chore:** update the pinned `stormvm-*` git dependencies to `abfaf73`. The
  lock pinned `7307ee4`, so the kubelet was built against a `stormvm-spec`
  three commits old and a fix made in stormvm did not reach a stormcos node —
  the release recorded `rustkube-node@<commit>` accurately while the
  stormvm commit inside it was invisible and stale.
- **fix:** a deleted VM takes its own volumes with it. `release` only
  *detached* them, so every machine's root clone and cloud-init seed outlived
  it for ever — one VM leaves two orphans, and a build fleet creating and
  destroying a hundred a day leaves two hundred that nothing can tell from
  volumes something still needs. Only what the machine created: a
  `volume:<id>` it was handed belongs to whoever made it and is meant to
  outlive it. stormvm's own `delete` already states this rule; the kubelet did
  not implement it.
- **feat(vm):** the kubelet registers each machine with the node's metadata
  service at start, and forgets it at stop. A guest asks `169.254.169.254` who
  it is, and the node running it is the only thing that can answer — it knows
  the VMI, the MAC it generated and the addresses the guest was given.
  Registered by the kubelet rather than by a watch on the apiserver, so a
  guest's identity does not wait on a control plane that may be starting,
  elsewhere or down: a node boots useful alone, and so do its guests.
  Deregistration matters as much — an address is handed to the next guest, and
  a metadata service answering for the previous occupant of an IP is worse
  than one that does not answer.
- **fix:** the kubelet asks for **its own** machines, not the cluster's. It
  listed every VMI cluster-wide every two seconds and filtered locally — on a
  small cluster invisible, at a thousand nodes running a thousand machines
  each it is a million objects fetched five hundred times a second, with the
  apiserver serializing a list each caller throws away 99.9% of. The field
  selector is the one upstream's kubelet uses. The local filter stays: an
  apiserver that does not implement the selector answers with everything
  rather than an error, and silently running every machine in the cluster on
  one node is a worse failure than a slow list.
- **feat:** the kubelet **watches** its machines rather than polling for them.
  A poll asks "what is it now" every two seconds whether or not anything
  changed; a watch is told. That matters most for the case hardest to reason
  about — a machine that moves: with a poll the old node keeps answering for
  it for up to a tick after it is gone and the new one stays silent for up to
  a tick after it arrives. LIST for the current state and its
  `resourceVersion`, WATCH from there, re-LIST on `410 Gone`, reconnect on any
  disconnect. The reconcile loop still runs on its interval, because a watch
  says what changed and reconciliation is what makes the node match it —
  including when nothing changed and something drifted.
- **feat(kubelet):** `nodeInfo.osImage` names the StormCOS release the node
  booted, read from the manifest volume the image carries at
  `/etc/stormcos/release/version`. Nothing surfaced it before: a node could not
  say which release it was running, and finding out meant asking the registry
  which release a boothost synonym pointed at — the build's record of what was
  *published*, not the node's record of what it *booted*, which differ for
  exactly as long as a node has not rebooted. Answered in `nodeInfo` because
  that is where Kubernetes already answers it, so one query returns the
  release, the kernel and the kubelet together and every existing reader gets
  it free. Falls back to naming the runtime outside a stormcos image.
- **fix(kubelet):** `userDataSecretRef` is resolved before the spec reaches
  the engine. A seed may be referenced rather than inlined, because the
  payload is where SSH keys live and a VMI spec is readable by anyone with
  `get` on virtualmachineinstances — but nothing resolved the reference, so a
  machine whose seed was a reference booted with **no cloud-init at all**: no
  key, no user, no hostname, and a guest nobody could log into. `stringData`
  is read as well as `data`, because the apiserver is supposed to fold the
  first into the second and rustkube does not.
- **feat(kubelet):** a machine with no guest agent still reports an address,
  read from the node's neighbour table by MAC. Agentless guests — anything
  mid-install, anything that is not a cloud image — showed no address at all,
  which reads as a machine with no network rather than one nobody has asked.
