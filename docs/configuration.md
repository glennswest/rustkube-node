# Configuration and defaults

Source: `cmd/kubelet/src/main.rs`, `pkg/kubelet/src/kubelet.rs`,
`engine.rs`, `cri_client.rs`, and `cmd/kube-proxy/src/main.rs`, main 06b91b5
(2026-10-09). These are executable defaults, not the stormcos launch arguments.
There is no general kubelet YAML/TOML config-file flag; kubeconfig supplies API
connection credentials. Flags take precedence over their Clap environment
variables. `RUST_LOG` configures tracing (default `info`).

## Kubelet flags

An em dash means there is no environment binding or the value is unset.

| Flag | Environment | Default / behavior |
|---|---|---|
| `--apiserver` | `APISERVER_URL` | Unset: the kubeconfig's server, else `http://127.0.0.1:6443`. Given, it wins over the kubeconfig, whatever its value (#113) |
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
| (env only) | `STORMDRIVE_URL` | `https://<node-ip>:9092`, then `http://<node-ip>:9092` (a stormdrive before stormdrive#19): the node's stormdrive, read for each PV's drive shelf/bay (#60) |
| `--storage-alert-percent` | `STORAGE_ALERT_PERCENT` | `85`: percent of the data slabs written past which a `SlabFilling` Warning goes on this node's stormblock PVs (#62) |
| `--pod-workers` | `POD_WORKERS` | 16 per CPU, at least 32, at most 256: Pod/VMI passes (starts, checks, teardowns) run at once (#138) |
| `--kubelet-port` | `KUBELET_PORT` | `10250`; HTTPS on `0.0.0.0` |
| `--container-log-max-size` | `CONTAINER_LOG_MAX_SIZE` | `10Mi` (upstream's `containerLogMaxSize`): a container's live log file larger than this is rotated, checked every 10 s (#216). A positive quantity |
| `--container-log-max-files` | `CONTAINER_LOG_MAX_FILES` | `5` (upstream's `containerLogMaxFiles`): files per container run, the live one included; older rotations are gzip'd, the oldest deleted. At least 2 (#216) |
| `--max-pods` | `MAX_PODS` | `110` (upstream's); the node's `capacity.pods` and `allocatable.pods`, which the scheduler holds it to (#165). stormcos sets 250 (rustkube#205). The kubelet itself admits by no count |
| `--system-reserved` | `SYSTEM_RESERVED` | Unset. `cpu=500m,memory=1Gi,ephemeral-storage=1Gi` (upstream's spelling) held back for the OS: allocatable is capacity less this, `--kube-reserved` and the hard-eviction line (memory 100Mi, nodefs 10%) (#24). `pid` and other resources refused at startup (no node-allocatable cgroup holds them) |
| `--kube-reserved` | `KUBE_RESERVED` | Unset. Held back for the node's own components; as `--system-reserved` (#24) |
| `--cgroup-driver` | `CGROUP_DRIVER` | Unset; `systemd` or `cgroupfs`. `--runtime cri` only: the runtime's own answer (CRI `RuntimeConfig`) wins, as since 1.36 (a disagreeing flag is ignored with a warning), then this, then `cgroupfs`. Each sandbox's `cgroup_parent` is upstream's per-QoS `kubepods` place in that spelling (`kubepods-burstable-pod<uid>.slice`, `/kubepods/burstable/pod<uid>`) (#24). stormpump owns its groups (#57) |
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

Environment keys with no flag: `STORMDRIVE_URL` (above), the engine credentials
`STORMBLOCK_API_TOKEN`/`STORMBLOCK_TOKEN_FILE` and `STORMBLOCK_ADMIN_TOKEN`/`STORMBLOCK_ADMIN_TOKEN_FILE`
(below), and `RUST_LOG`.

**A named credential file is waited for, then fatal** (#69): `--kubeconfig`, `--apiserver-ca`,
`--client-certificate`, `--client-key`, `--token-file`, `--tls-cert-file`, `--tls-private-key-file` and
`--server-token-file`. The boot may write them after the kubelet starts (stormcert), so a missing or empty
file is waited for up to 60 s, with one warning; if it is still absent the kubelet exits naming the flag
and path. It never falls back to the kubeconfig's value, anonymous access or a self-signed serving pair
for a file it was told to use. An unset flag still falls back as described above.

Clap also provides `--help`. No KubeletConfiguration file is read. Reservations
are advertised in allocatable only; nothing enforces them as a cgroup limit (#205).

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

Kubeconfig load errors are fatal. A given `--apiserver` (flag or
`APISERVER_URL`) wins over the kubeconfig's server, even when it is
`http://127.0.0.1:6443`; only when neither is given is that the default (#113).
Explicit credential files override kubeconfig values; a named one that is
missing is waited for, then fatal (#69, above). With a CA and verification on,
the client pair is reloaded when renewed (#77).
The server uses the serving cert/key pair when `--tls-cert-file` and
`--tls-private-key-file` are given (waited for, then fatal, as above) and says
so in the log; with neither given it self-signs at startup and says that (#89).
Outbound API credentials, inbound server tokens and stormblock credentials
are separate. The non-health inbound routes accept the configured static token
or a bearer token accepted by API TokenReview; see [API](api.md).

Engine token lookup: nonempty `STORMBLOCK_API_TOKEN`, then
`STORMBLOCK_TOKEN_FILE` (default `/run/stormblock/engine/api_token`), then
`/etc/stormblock/api_token`, then `/var/lib/stormblock/api_token`.
It retries discovery while absent, caches a found token, and rereads on 401,
retrying once only if changed.

Admin credential (#105), for what stormblock's admin gate (stormblock#274)
refuses to the node token: deleting a template (#140's rebuild of a broken
blank) or a sealed volume. Nonempty `STORMBLOCK_ADMIN_TOKEN`, then the file at
`STORMBLOCK_ADMIN_TOKEN_FILE` (default `/run/stormblock-admin/admin_token`,
stormblock's own default). The file can hold the engine's admin token or a
Kubernetes ServiceAccount token that the engine's SubjectAccessReview allows
(`storage.storm.io`, the stormcos#296 route). The node token is always sent
first; only a non-GET call still refused with 401 after the node token's reread
is sent once more with the admin credential. It is read at each such call
(rotation needs no restart), never cached or logged. Absent: the 401 reaches the
caller, which reports it (the claim's ProvisioningFailed Event) and retries on
its next pass; one warning names where it was looked for.

### Library-only defaults and fixed paths

The Node's `status.nodeInfo` reads fixed files, each trimmed, `""` when none
is there (#78): `kernelVersion` from `/proc/sys/kernel/osrelease`, `bootID`
from `/proc/sys/kernel/random/boot_id`, `machineID` from the node's
`/hostroot/etc/machine-id` (else `/hostroot/var/lib/dbus/machine-id`, then the
kubelet's own two), `systemUUID` from `/sys/class/dmi/id/product_uuid`, and
`osImage` from `/etc/stormcos/release/version` (or `/hostroot`'s).

`KubeletConfig::default()` has `heartbeat_interval = 10s`, `sync_interval = 2s`,
and API URL `http://localhost:6443` (the CLI overrides that URL). These intervals
are not CLI keys. The heartbeat is the only fixed schedule. Pods and VMIs share
one pool of UID workers (`--pod-workers`) driven by watches and stormpump exits; a worker comes back
without an event only for its own deadlines (#101): a probe's `periodSeconds`
from its `initialDelaySeconds`, a CrashLoopBackOff's end, a waiting start
(backoff of a quarter of the wait, 1–10 s for Pods, 1–30 s for VMIs), the pod's
`activeDeadlineSeconds` while an init container runs (there is no fixed init limit, #126), and a VM guest-agent poll (2 s while booting, backing
off to 30 s; 10 s for a machine adopted without an engine handle).
`sync_interval` applies only to Pods on a CRI runtime with no open
`GetContainerEvents` stream (#116), as a counted fallback. Image pulls use four separate slots. The service mirror
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
`/pallets` for shipped images (read for user names and argv[0]), and
`/run/stormpump/roots/<container>` where PID 1 mounts each container's own root: engine volume
`ctr-<container>`, a clone of the image's sealed golden (a pallet's `<volume>.golden`, a pull's
sbregistry fstemplate, referred to as `template:<name>`), deleted with the container (#104).
The kernel command line's `rd.stormblock.mount=` maps a pallet path to its slab volume.
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
`STORM_SUITE` and `STORM_COMMIT` (the image tag's commit) come from the runner too.
Missing required inputs exit 2. short: Node Ready and heartbeat, a pod's run, address, log, exit
code and delete; medium: pod features (restart, init, configMap/env, missing image) and the
storage cases (PVC sizes, raw block, node volumes, overcommit refusal; at most three size cases
at once); long: waves of pods at the node's capacity. The runner's own pod is never deleted by
the cleanup (#217). Live: short passed on C2NR0Q2 at 9229603; medium and long have no complete
live pass yet (#61, #64).
