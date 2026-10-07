# Configuration and defaults

Source: `cmd/kubelet/src/main.rs`, `pkg/kubelet/src/kubelet.rs`,
`engine.rs`, `cri_client.rs`, and `cmd/kube-proxy/src/main.rs`, main fecb331
(2026-10-02). These are executable defaults, not the stormcos launch arguments.
There is no general kubelet YAML/TOML config-file flag; kubeconfig supplies API
connection credentials. Flags take precedence over their Clap environment
variables. `RUST_LOG` configures tracing (default `info`).

## Kubelet flags

An em dash means there is no environment binding or the value is unset.

| Flag | Environment | Default / behavior |
|---|---|---|
| `--apiserver` | `APISERVER_URL` | `http://127.0.0.1:6443`; kubeconfig server may replace this default |
| `--node-name` | `NODE_NAME` | Detect NODE_NAME, HOSTNAME, system hostname, then `localhost` |
| `--pod-cidr` | `POD_CIDR` | Unset; when set writes Node spec.podCIDR |
| `--node-labels` | `NODE_LABELS` | Empty; comma-separated `key=value`. With the stormpump engine the kubelet also writes `storm.io/kvm` and `kubevirt.io/schedulable` itself, from whether KVM is there (#65) |
| `--node-annotations` | `NODE_ANNOTATIONS` | Empty; comma-separated `key=value` |
| `--register-with-taints` | `REGISTER_WITH_TAINTS` | Empty; `key=value:Effect` or `key:Effect`, on Node creation only |
| `--pod-manifest-path` | `POD_MANIFEST_PATH` | `/etc/kubernetes/manifests`; empty string disables static Pods |
| `--runtime` | — | `native`; choices `native`, `cri`, `vm`, `stormpump` |
| `--vmm` | — | `auto`; choices `auto`, `cloud-hypervisor`, `qemu`, `firecracker`; for legacy `vm` runtime |
| `--registry` | — | `http://127.0.0.1:5100`; sbregistry (stormblock-registry), which mints image clones, with `--runtime stormpump` |
| `--stormblock` | `STORMBLOCK_URL` | `http://127.0.0.1:9090`; engine for claims, VM disks and images |
| `--cri-socket` | `CRI_SOCKET` | CRI auto-detection, or `/run/stormpump.sock` for stormpump |
| `--cni-conf-dir` | `CNI_CONF_DIR` | `/etc/cni/net.d`; watched (inotify) unless `--no-cni`: a change wakes the Pods waiting for a network config (#148) |
| `--cni-bin-dir` | `CNI_BIN_DIR` | `/opt/cni/bin` |
| `--no-cni` | — | `false`; disables the kubelet CNI invoker; see runtime caveat below |
| `--storage-overcommit` | `STORAGE_OVERCOMMIT` | `1.0`: committed bytes allowed per byte of stormblock data slab; a claim is charged its full class (#62, #108) |
| `--storage-reserve-percent` | `STORAGE_RESERVE_PERCENT` | `5`: percent of the data slabs kept back from claims (#62) |
| `--metadata-max-staleness` | `METADATA_MAX_STALENESS` | `40` (seconds, the node Lease duration): `/vmInstance` answers a machine's metadata from the VMI cache only within this long of word from the apiserver (a renewed Lease or a VMI list); past it, 503 + `Retry-After: 5` (#156). `0`: unbounded |
| `--storage-alert-percent` | `STORAGE_ALERT_PERCENT` | `85`: percent of the data slabs written past which a `SlabFilling` Warning goes on this node's stormblock PVs (#62) |
| `--pod-workers` | `POD_WORKERS` | 16 per CPU, at least 32, at most 256: Pod/VMI passes (starts, checks, teardowns) run at once (#138) |
| `--kubelet-port` | `KUBELET_PORT` | `10250`; HTTPS on `0.0.0.0` |
| `--max-pods` | `MAX_PODS` | `110` (upstream's); the node's `capacity.pods` and `allocatable.pods`, which the scheduler holds it to (#165). stormcos sets 250 (rustkube#205). The kubelet itself admits by no count |
| `--apiserver-ca` | `APISERVER_CA` | Unset; PEM trust anchor, otherwise kubeconfig CA |
| `--token-file` | `KUBELET_TOKEN_FILE` | Unset; outbound bearer token file, otherwise kubeconfig token |
| `--kubeconfig` | `KUBECONFIG` | Unset; explicit file, not automatic `~/.kube/config` discovery |
| `--client-certificate` | `KUBELET_CLIENT_CERT` | Unset; outbound PEM client certificate, otherwise kubeconfig. With `--apiserver-ca` (and verification on), its directory is watched (and looked at hourly) and a renewed pair is presented from the next connection on, no restart (#77; stormcert renews it at 80% of its year). A pair that does not load or whose key is not its certificate's is refused; the previous one stays. Without a CA, or with `--insecure-skip-tls-verify`, it is read once |
| `--client-key` | `KUBELET_CLIENT_KEY` | Unset; outbound PEM key, otherwise kubeconfig |
| `--insecure-skip-tls-verify` | — | `false`; ORed with kubeconfig's skip-verification setting |
| `--tls-cert-file` | `KUBELET_TLS_CERT_FILE` | Unset; inbound serving PEM certificate (else a self-signed one, and the log says so). Its directory is watched, and the pair is reloaded in place when it changes, e.g. renewed by stormcert at boot (#89); a pair that does not load yet (half-written) leaves the previous one serving |
| `--tls-private-key-file` | `KUBELET_TLS_PRIVATE_KEY_FILE` | Unset; inbound serving PEM key |
| `--server-token-file` | `KUBELET_SERVER_TOKEN_FILE` | Unset; static token accepted by inbound server, alongside TokenReview |
| `--anonymous-auth` | — | `false`; when true disables inbound bearer authentication |

**A named credential file is waited for, then fatal** (#69): `--kubeconfig`, `--apiserver-ca`,
`--client-certificate`, `--client-key`, `--token-file`, `--tls-cert-file`, `--tls-private-key-file` and
`--server-token-file`. The boot may write them after the kubelet starts (stormcert), so a missing or empty
file is waited for up to 60 s, with one warning; if it is still absent the kubelet exits naming the flag
and path. It never falls back to the kubeconfig's value, anonymous access or a self-signed serving pair
for a file it was told to use. An unset flag still falls back as described above.

Clap also provides `--help`. No `--system-reserved`, `--kube-reserved` or
`--cgroup-driver` flag exists (#24), and no KubeletConfiguration file is read.
Node reporting uses a fixed 256 MiB memory reservation.

### Runtime details

CRI socket detection selects the first existing path, in this order:
`/run/crio/crio.sock`, `/var/run/crio/crio.sock`,
`/run/containerd/containerd.sock`, `/var/run/containerd/containerd.sock`,
`/var/run/dockershim.sock`; with none present it returns
`unix:///run/containerd/containerd.sock`. This is path detection, not a working
runtime probe. The gRPC client has a 120-second channel timeout and requests
600 seconds for image pull and the caller's timeout for exec; the synthetic
`http://[::1]:50051` URI is used with a Unix connector, not a TCP listener.

Every other runtime-side call is bounded too (#99):

- **stormpump ring:** 30 s per request, counted from when it was asked, so a
  request queued behind a wedged engine times out as well. A request dequeued
  after its deadline is never submitted. One already on the ring is answered
  `Timeout` and kept: its late completion still frees the shared arena, and a
  late success is undone (a spawn is stopped and released on exit; a volume,
  spec or sandbox is released).
- **CNI plugins:** 60 s per plugin exec, stdin to exit
  (`cni::PLUGIN_TIMEOUT`, `CniInvoker::with_timeout`). Past it the plugin is
  killed and reaped, and the call fails. A failed ADD is followed by DEL
  (#100).
- **stormblock engine:** 5 s to connect and 60 s per request. A blank's mint
  POST is allowed 1 h, since it answers when the format is done: stormblock
  finishes the format itself and refuses a duplicate name, so a mint that
  outlives the bound is found again by name. The volume watch stream has no
  overall bound.

`--runtime stormpump` fails startup if the ring connection fails. It also
activates the stormvm VMI and snapshot reconcilers. `--runtime vm` detects a
VMM and falls back to native if none is found; its Pod container lifecycle is
incomplete (#13). It does not activate the stormvm VMI reconciler.

`--no-cni` removes the kubelet's invoker. It does not rewrite Pod hostNetwork,
change CRI runtime configuration or force stormpump to use the host namespace.
A non-hostNetwork stormpump Pod can therefore get an isolated namespace with
no plugin wiring: loopback only, no address. The help text says so since #3; use
`spec.hostNetwork` when host networking is the intended workload contract.

### Credential precedence and failure behavior

Kubeconfig load errors are fatal. A non-default apiserver argument wins over
kubeconfig; an argument equal to `http://127.0.0.1:6443` is indistinguishable
from the default, so kubeconfig still wins in that case (#113). Readable explicit
credential files override kubeconfig values. Client cert/key/token read errors
warn and fall back to kubeconfig, or leave credentials absent (#69).
Client credentials are read at startup and not reloaded (#77).
An explicitly named CA is retried every 500 ms for up to 60 seconds; failure
is fatal. This wait does not apply to every credential file.

The server uses the serving cert/key pair if both were read; otherwise it
self-signs at startup, including when a configured file is missing (#89).
Outbound API credentials, inbound server tokens and stormblock credentials
are separate. The non-health inbound routes accept the configured static token
or a bearer token accepted by API TokenReview; see [API](api.md).

Engine token lookup: nonempty `STORMBLOCK_API_TOKEN`, then
`STORMBLOCK_TOKEN_FILE` (default `/run/stormblock/engine/api_token`), then
`/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`.
It retries discovery while absent, caches a found token, and rereads on 401,
retrying once only if changed. No separate admin-token source exists (#105).

### Library-only defaults and fixed paths

`KubeletConfig::default()` has `heartbeat_interval = 10s`, `sync_interval = 2s`,
and API URL `http://localhost:6443` (the CLI overrides that URL). These intervals
are not CLI keys. The heartbeat is the only fixed schedule. Pods and VMIs share
one pool of UID workers (`--pod-workers`) driven by watches and stormpump exits; a worker comes back
without an event only for its own deadlines (#101): a probe's `periodSeconds`
from its `initialDelaySeconds`, a CrashLoopBackOff's end, a waiting start
(backoff of a quarter of the wait, 1–10 s for Pods, 1–30 s for VMIs), an init
container's 120 s limit, and a VM guest-agent poll (2 s while booting, backing
off to 30 s; 10 s for a machine adopted without an engine handle).
`sync_interval` applies only to a runtime without exit events (CRI), as a
counted fallback. Image pulls use four separate slots. The service mirror
follows `/run/stormpump` (inotify, at most one read a second while PID 1
rewrites the file every pass, stormpump#67) and its own mirror pods' watch.
System claims and the disk-owner sweep follow stormblock's volume watch (30 s
poll, counted, on an engine without one) and PV/PVC or VM/VMI watches; the
sweep runs at most once a minute and defers, not drops, events inside that.
Reclaim follows the PV watch (5 s retry while a claim is in use); the CSI sweep
follows this node's Pod watch (10 s retry for a pending teardown). CSI
registration uses filesystem notifications with retry deadlines. Snapshots
follow their watch and take completions.

Fixed paths include `/var/lib/kubelet` for volume records,
`/var/log/pods` for container logs, `/run/stormvm/<namespace>/<name>/vm.json`
for VM registration, `/run/stormpump/assets.json` for service mirrors,
`/pallets` for shipped images and `/run/stormpump/images/<volume>` for pulled images
(the path a pull returns is the container's root as given, #103). A pulled image's
sbregistry clone is bound to `kubelet/<node>/<image>` after the mount, and a pull after a
kubelet restart reattaches that bound clone instead of minting another (#143); nothing
unmounts or releases a pulled image yet (#161).
The kubelet views host files under `/hostroot` in stormcos; the stage/boot
configuration must provide those mounts.

## Stormcos launch overrides

The authoritative stormcos stage recipe selects `--runtime stormpump`,
`--cri-socket /hostrun/stormpump.sock`, `--apiserver https://${NODE_IP}:6443`,
`--node-name ${NODE_NAME}`, the local sbregistry on port 5100, and these certificates:

- CA: `/data/stormcert/ca.crt`
- client: `/data/stormcert/kubelet.crt` and `kubelet.key`
- serving: `/data/stormcert/kubelet-serving.crt` and `kubelet-serving.key`

stormd expands the node variables; the kubelet itself does not expand shell
syntax in flag values. The generated stormd config sets
`RUST_LOG=info,kubelet=debug`. See [shipping](BUILD.md).

## Kube-proxy

Source: `cmd/kube-proxy/src/main.rs`, `pkg/proxy`.

| Flag | Env | Default / purpose |
|---|---|---|
| `--apiserver` | `APISERVER_URL` | `http://127.0.0.1:6443`. In a DaemonSet, `https://$(NODE_IP):6443`: not the `kubernetes` ClusterIP, which is what kube-proxy makes routable |
| `--ca-file` | `KUBE_PROXY_CA_FILE` | `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt` if present, else none. PEM CA trusted for the apiserver |
| `--token-file` | `KUBE_PROXY_TOKEN_FILE` | `/var/run/secrets/kubernetes.io/serviceaccount/token` if present, else none. Bearer token, re-read on every request |
| `--cluster-cidr` | `KUBE_PROXY_CLUSTER_CIDR` | None. Pod CIDR: ClusterIP traffic from outside it is marked for masquerade |
| `--node-name` | `NODE_NAME` | HOSTNAME, then NODE_NAME, then `localhost` |

An explicit CA or token file that does not exist, an unusable CA or an empty
token stops kube-proxy at start. Every five seconds it lists `/api/v1/services`
and `/api/v1/endpoints` (RBAC: get/list/watch on services and endpoints); a
refused or malformed answer leaves the node's rules alone. Rules go through
`iptables-restore -w 5 --noflush` (nat table) when they differ from the last
applied set, and again every 60 s; after each restore, the jumps
PREROUTING/OUTPUT → `KUBE-SERVICES` and POSTROUTING → `KUBE-POSTROUTING` are
checked (`iptables -C`) and inserted when missing. It needs `iptables` and
`iptables-restore` on PATH, hostNetwork and NET_ADMIN (privileged). After an apply
it deletes the UDP conntrack entries the change left stale (#147), as upstream:
for each endpoint that left a UDP Service port, `conntrack -D -p udp --orig-dst
<clusterIP> --dst-nat <ip>` (and `--dport <nodePort> --dst-nat <ip>` for a NodePort),
and `--orig-dst <clusterIP>` alone when the port gains its first endpoint. It needs
the `conntrack` binary (conntrack-tools) for that; without it a stale UDP flow (a
DNS client of a replaced CoreDNS) ages out in 30–120 s and the log says so once.
TCP entries are left alone. It leaves the chains of a deleted Service in place
(empty of jumps). No serving port, no kubeconfig flag, no EndpointSlices and no
eBPF backend.

## Test container

Sources: `test/src/env.rs`, `test/src/main.rs`. `/test <suite>` selects short,
medium or long. The process passes this to `STORM_SUITE` (default short).

| Variable | Default / purpose |
|---|---|
| `STORM_API` | Required API URL |
| `STORM_RUN_ID` | Required ownership/cleanup identifier |
| `STORM_NAMESPACE` | Required, or namespace from the mounted ServiceAccount |
| `STORM_NODE` | The test node's address (or name), resolved to its Node at the start of a run; the suites' pods are pinned to it |
| `RUSTKUBE_NODE_TEST_IMAGE` | The workload pods' image; empty: the Job's own pod's image, else `test-rustkube-node-<suite>:<commit12>` (#97) |
| `RUSTKUBE_NODE_TEST_WAVE_MAX` | long: the largest wave, default 500 pods (a wave is otherwise 80% of the node's free pod slots) |
| `RUSTKUBE_NODE_TEST_WAVES` | long: stop after this many waves (default: until the suite's time is nearly out) |
| `STORM_TIMEOUT` | Seconds: short 120, medium 1800, long 28800 |
| `RUSTKUBE_NODE_TEST_MINT_BUDGET` | 1200 seconds per PVC case, bounded by remaining suite time |

Token and CA come from `/var/run/secrets/kubernetes.io/serviceaccount`.
Missing required inputs exit 2. Short and long currently report skips; medium
covers PVC sizes, raw block, node volumes and overcommit refusal. No live pass is implied.
