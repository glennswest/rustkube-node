# Configuration and defaults

Source: `cmd/kubelet/src/main.rs`, `pkg/kubelet/src/kubelet.rs`,
`engine.rs`, `cri_client.rs`, and `cmd/kube-proxy/src/main.rs`, main 5bb1a38
(2026-09-29). These are executable defaults, not the stormcos launch arguments.
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
| `--node-labels` | `NODE_LABELS` | Empty; comma-separated `key=value` |
| `--node-annotations` | `NODE_ANNOTATIONS` | Empty; comma-separated `key=value` |
| `--register-with-taints` | `REGISTER_WITH_TAINTS` | Empty; `key=value:Effect` or `key:Effect`, on Node creation only |
| `--pod-manifest-path` | `POD_MANIFEST_PATH` | `/etc/kubernetes/manifests`; empty string disables static Pods |
| `--runtime` | — | `native`; choices `native`, `cri`, `vm`, `stormpump` |
| `--vmm` | — | `auto`; choices `auto`, `cloud-hypervisor`, `qemu`, `firecracker`; for legacy `vm` runtime |
| `--registry` | — | `http://127.0.0.1:5100`; stormpump image service |
| `--stormblock` | `STORMBLOCK_URL` | `http://127.0.0.1:9090`; engine for claims, VM disks and images |
| `--cri-socket` | `CRI_SOCKET` | CRI auto-detection, or `/run/stormpump.sock` for stormpump |
| `--cni-conf-dir` | `CNI_CONF_DIR` | `/etc/cni/net.d` |
| `--cni-bin-dir` | `CNI_BIN_DIR` | `/opt/cni/bin` |
| `--no-cni` | — | `false`; disables the kubelet CNI invoker; see runtime caveat below |
| `--kubelet-port` | `KUBELET_PORT` | `10250`; HTTPS on `0.0.0.0` |
| `--apiserver-ca` | `APISERVER_CA` | Unset; PEM trust anchor, otherwise kubeconfig CA |
| `--token-file` | `KUBELET_TOKEN_FILE` | Unset; outbound bearer token file, otherwise kubeconfig token |
| `--kubeconfig` | `KUBECONFIG` | Unset; explicit file, not automatic `~/.kube/config` discovery |
| `--client-certificate` | `KUBELET_CLIENT_CERT` | Unset; outbound PEM client certificate, otherwise kubeconfig |
| `--client-key` | `KUBELET_CLIENT_KEY` | Unset; outbound PEM key, otherwise kubeconfig |
| `--insecure-skip-tls-verify` | — | `false`; ORed with kubeconfig's skip-verification setting |
| `--tls-cert-file` | `KUBELET_TLS_CERT_FILE` | Unset; inbound serving PEM certificate |
| `--tls-private-key-file` | `KUBELET_TLS_PRIVATE_KEY_FILE` | Unset; inbound serving PEM key |
| `--server-token-file` | `KUBELET_SERVER_TOKEN_FILE` | Unset; static token accepted by inbound server, alongside TokenReview |
| `--anonymous-auth` | — | `false`; when true disables inbound bearer authentication |

Clap also provides `--help`. No `--max-pods`, `--system-reserved`,
`--kube-reserved` or `--cgroup-driver` flag exists (#24). Node reporting uses
110 Pods and a fixed 256 MiB memory reservation; these are not tunable flags.

### Runtime details

CRI socket detection selects the first existing path, in this order:
`/run/crio/crio.sock`, `/var/run/crio/crio.sock`,
`/run/containerd/containerd.sock`, `/var/run/containerd/containerd.sock`,
`/var/run/dockershim.sock`; with none present it returns
`unix:///run/containerd/containerd.sock`. This is path detection, not a working
runtime probe. The gRPC client has a 120-second channel timeout and requests
600 seconds for image pull and the caller's timeout for exec; the synthetic
`http://[::1]:50051` URI is used with a Unix connector, not a TCP listener.

`--runtime stormpump` fails startup if the ring connection fails. It also
activates the stormvm VMI and snapshot reconcilers. `--runtime vm` detects a
VMM and falls back to native if none is found; its Pod container lifecycle is
incomplete (#13). It does not activate the stormvm VMI reconciler.

`--no-cni` removes the kubelet's invoker. It does not rewrite Pod hostNetwork,
change CRI runtime configuration or force stormpump to use the host namespace.
A non-hostNetwork stormpump Pod can therefore get an isolated namespace with
no plugin wiring. Use `spec.hostNetwork` when that is the intended workload
contract; do not rely on the CLI help's host-network shorthand (#3).

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
are not CLI keys. Main still serially reconciles Pods, watches VMIs and
reconciles their current desired state. Service mirrors run every 15 seconds;
service-volume reconciliation and CSI cleanup every 30 seconds; CSI registration
polls every two seconds. The event-driven design is separate turbomode work.

Fixed paths include `/var/lib/kubelet` for volume records,
`/var/log/pods` for container logs, `/run/stormvm/<namespace>/<name>/vm.json`
for VM registration, `/run/stormpump/assets.json` for service mirrors,
`/pallets` for shipped images and `/run/stormpump/images` for pulled images.
The kubelet views host files under `/hostroot` in stormcos; the stage/boot
configuration must provide those mounts.

## Stormcos launch overrides

The authoritative stormcos stage recipe selects `--runtime stormpump`,
`--cri-socket /hostrun/stormpump.sock`, `--apiserver https://${NODE_IP}:6443`,
`--node-name ${NODE_NAME}`, local registry port 5100, and these certificates:

- CA: `/data/stormcert/ca.crt`
- client: `/data/stormcert/kubelet.crt` and `kubelet.key`
- serving: `/data/stormcert/kubelet-serving.crt` and `kubelet-serving.key`

stormd expands the node variables; the kubelet itself does not expand shell
syntax in flag values. The generated stormd config sets
`RUST_LOG=info,kubelet=debug`. See [shipping](BUILD.md).

## Kube-proxy

Only `--apiserver` (`APISERVER_URL`, `http://127.0.0.1:6443`) and `--node-name`
(`NODE_NAME`, then HOSTNAME/NODE_NAME/localhost fallback) are exposed, plus help.
The library polls Services/Endpoints every five seconds and invokes
iptables-restore on Linux. It has no serving port, kubeconfig flag or TLS/token
credential flags. No eBPF backend is implemented; stormcos uses Cilium instead.

## Test container

Sources: `test/src/env.rs`, `test/src/main.rs`. `/test <suite>` selects short,
medium or long. The process passes this to `STORM_SUITE` (default short).

| Variable | Default / purpose |
|---|---|
| `STORM_API` | Required API URL |
| `STORM_RUN_ID` | Required ownership/cleanup identifier |
| `STORM_NAMESPACE` | Required, or namespace from the mounted ServiceAccount |
| `STORM_NODE` | Empty; optional node selector input |
| `RUSTKUBE_NODE_TEST_IMAGE` | Required workload image; runner injection is missing (#97) |
| `STORM_TIMEOUT` | Seconds: short 120, medium 1800, long 28800 |
| `RUSTKUBE_NODE_TEST_MINT_BUDGET` | 1200 seconds per PVC case, bounded by remaining suite time |

Token and CA come from `/var/run/secrets/kubernetes.io/serviceaccount`.
Missing required inputs exit 2. Short and long currently report skips; medium
covers PVC sizes, with overcommit explicitly skipped. No live pass is implied.
