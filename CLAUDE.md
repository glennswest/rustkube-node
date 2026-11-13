# CLAUDE.md — rustkube-node

The node half of rustkube: the kubelet (`pkg/kubelet`, `cmd/kubelet`), kube-proxy
(`pkg/proxy`, `cmd/kube-proxy`) and the CNI helpers (`pkg/cni`). It ships as a
stage golden through `stormcentral component stage rustkube-node`, not a package
(the legacy bin-only `scripts/build-golden.sh` was removed, #51). The cross-project rules are in
`../CLAUDE.md`; this file is the project's own context and work plan.

## Version

`0.13.0`. There is one version location: `[workspace.package] version` in
`Cargo.toml` (every crate uses `version.workspace = true`).

## Current implementation reference (2026-11-13)

Main baseline: 0c4a931 (#340 pod condition times, a8f0511; #344 golden from a running VM; #348 RDMA verbs; #338/#339 claim and stopped-VM moves to another node; #342 vmInstance refusals; #312 the engine client over https with `--stormblock-ca`, 55eb751; #311 `container/<name>` steps, 6a3b27a; #310 diagnosed, no code: Cilium's `endpoint-create` limiter; #303 VMI Running once its control socket answers; stormvm 0225218, #304 bridge keeps multus/SR-IOV NICs; #302 disk moves off the node; #301; #40 live migration kubelet half, #160 parked startup admission, #292 ring arena per request; every git dependency by rev, #210; stage builds are listed per issue in the work plan). See
`docs/status.md` for changes since October 2, the owner's recorded decisions and issue-backed limitations,
`docs/configuration.md` for every CLI/env/default, `docs/api.md` for ports and actual routes, `docs/retries.md`
for every remote call's retry policy. CLI runtime defaults to native; stormcos explicitly chooses stormpump.
Cilium (or, in the flowsdn edition, flowsdn) owns Services; the packaged kube-proxy is not started in either
edition (#145, #155). PVCs use the built-in stormblock driver and sealed size-class blanks over ublk, with CSI
only for third-party drivers. Every container's root is its own CoW clone of the image's sealed golden (#104,
owner's option A). Node services' mirror pods take their state and Events from each service's stormd API (#215).

Main has live migration of stormvm-started VMIs (#40: stormstorage disk mirror, qemu TLS; flags `--stormstorage`, `--migration-tls-*`, `--migration-port`), VMI adoption, persistent VM-owned disks, accessCredentials, tap address reporting, snapshots and
restores (#53), pod-network VMIs under rustkube's launcher Pod (#88), the VM verbs (#94). Exec/attach are served
on stormpump's exec op (#56). Historical work-plan entries below record what was true at each checkpoint; they are not
current deployment guarantees. Owner decisions answered: #106 (Pods under one parent cgroup, done), #107
(`<volume>-<node>`, done), #108 (class size at bind, done), #109 (option A, done), #110/#158 (C2NR0Q2's day
suite is the acceptance, #102), #104 (A), #3 (no NotReady gating; #201), #68 (stormblock-csi owns replicas),
#159 (RAID leg on the destination, #40 after stormstorage#44), stormimds#12 (stormimds asks `/vmInstance`).
Open: #334 (a kubelet restart adopts no pods on stormpump), #327-#330 (downward API, token automount, service env). Waiting on the owner: #102 (acceptance machine: the Dell's tests valve is closed).

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

The work plan and its session history are **[docs/work-plan.md](docs/work-plan.md)**. That file is not loaded by default. Find your section with `grep -n '^## ' docs/work-plan.md`, then read only that section. Write intentions there before starting a task, as before. Keep this CLAUDE.md short: it is loaded into every turn.
