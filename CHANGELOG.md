# Changelog

## [Unreleased]

<!-- New unreleased changes go here -->

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
