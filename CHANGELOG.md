# Changelog

## [Unreleased]

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
