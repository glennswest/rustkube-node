# Ports and kubelet API

Audited from `pkg/kubelet/src/server.rs` and the CLI at main fecb331 (2026-10-02).

| Endpoint | Ownership / default |
|---|---|
| `0.0.0.0:10250` HTTPS | Kubelet listener, configurable with `--kubelet-port` |
| `127.0.0.1:6443` HTTP | CLI's outbound apiserver default; stormcos overrides to node HTTPS |
| `127.0.0.1:9090` HTTP | Outbound stormblock management API, `--stormblock` |
| `127.0.0.1:5100` HTTP | Outbound sbregistry (stormblock-registry, the registry that mints image clones), `--registry` |
| `/run/stormpump.sock` Unix socket | Default stormpump ring bootstrap; stormcos uses `/hostrun/stormpump.sock` |
| `/var/lib/kubelet/plugins_registry/*` Unix sockets | CSI registrar discovery, then node-plugin Unix endpoints |
| `0.0.0.0:9085` | stormd management listener from stormcos's stage config, not a kubelet route |

The kubelet does not listen on a separate read-only HTTP port or expose a CRI
server. Kube-proxy has no listener. VM console traffic is mounted into the
kubelet process; no separate console TCP service is required by this path.

## Routes

`/healthz`, `/livez`, `/readyz` are unauthenticated and return `200 ok` from the
same handler. They do not probe CNI, API connectivity or workload readiness.
Every other route requires a bearer token unless `--anonymous-auth` is set:
a configured static token is accepted, otherwise API TokenReview checks the
token. Missing/invalid credentials are rejected; TLS is still used with
anonymous auth. See [credential behavior](configuration.md).

| Method | Route | Behavior |
|---|---|---|
| GET | `/healthz`, `/livez`, `/readyz` | Process HTTP health only |
| GET | `/metrics` | Kubelet/process metric families |
| GET | `/metrics/cadvisor` | Runtime-supplied container/pod metric subset |
| GET | `/stats/summary` | Partial CPU/memory and filesystem summary; node CPU/memory are container sums; each pod's `network` (upstream's `name`/`rxBytes`/`rxErrors`/`txBytes`/`txErrors` of `eth0`, every interface in `interfaces`, plus `rxPackets`/`rxDropped`/`txPackets`/`txDropped`, #131) |
| GET | `/pods` | Locally managed Pods, including recorded waiting Pods |
| GET | `/containerLogs/{namespace}/{pod}/{container}` | Runtime logs; a node service's mirror pod reads its stormd log volume, else PID 1's `last_output` for it (#124) |
| GET | `/vmConsole/{namespace}/{name}/{door}` | `serial` or `vnc` through stormvm's router, with WebSocket upgrade |
| PUT | `/vmVerb/{namespace}/{name}/{verb}` | A VM's verb through stormvm's router (`PUT /api/v1/vms/{ns}/{name}/{verb}`, #94): `pause`, `unpause`, `softreboot`, `reset`, `status`, `freeze`, `thaw` (or `unfreeze`), `snapshot` (`?name=&quiesce=`). Query forwarded without `token`; 400 for any other verb (`migrate`/`receive` included); the router's own answers otherwise (404 for an unregistered VM) |
| GET | `/vmInstance/{address}` | Guest metadata: the machine found by address in this node's index, answered from its cached VMI only when that object places it here (uid matches, not terminating, `status.nodeName` is this node, no completed migration to another node; #119). **A node address** (a host-network workload) is refused unless the request carries the workload's own ServiceAccount token in `X-Storm-Workload-Token` (#122; not `Authorization`, which is the caller's credential to the kubelet): the kubelet TokenReviews it, takes the pod from `status.user.extra` (`authentication.kubernetes.io/pod-name`/`pod-uid`/`node-name`), and answers that pod's metadata (`storm.io/kind: Pod`) only when the pod's object has that uid, is placed on this node and is not ending. 404 when absent, placed elsewhere, or claimed by two machines here; 503 + `Retry-After: 2` while cold (until the first VMI list; a cluster without the VMI CRD is not cold); 503 + `Retry-After: 5` for a machine here when the apiserver has not been heard from (a renewed node Lease or a VMI list) within `--metadata-max-staleness` (40 s), since a partitioned node cannot see the machine move (#156). Whether the metadata service (stormimds) asks this or keeps its own store is undecided (stormimds#12) |
| DELETE | `/volumes/{namespace}/{claim}` | Built-in claim clone reclamation: 204 absent/deleted, 409 in use, 503 when safe release cannot be established |

`previous` is `true`/`1` (the run before, as upstream parses it) or `N`, the run N
back (#131): a regular pod's `<restartCount>.log` files, a stormd service's failed
runs newest first, PID 1's last five runs; PID 1's `last_output` is one run back
only. Past the oldest run, 400 saying how many there are. rustkube's `pods/log`
forwards the value as given.

Log query options: `follow`, `previous`, `tailLines`, `sinceSeconds`,
`sinceTime`, `timestamps`, `limitBytes`. Waiting containers return 400 with
their reason. stormd mirror logs are discovered through boot-unit log mounts;
`previous` selects the newest failed-run log; non-stormd services read PID 1's
run files (#87); a completed init container's log is kept (#47).

A line loses its time, stream and tag only when it carries them (#136): CRI
(`<RFC 3339> <stdout|stderr> <P|F> <msg>`, partial `P` lines joined to the rest)
or stormd's (`<RFC 3339> <stream> <severity> <msg>`). Every other line, which is
every stormpump container's (no per-line metadata), comes back as written, so
`timestamps`/`since*` have nothing to act on there. A last line with no newline
is returned by a plain read, and by `follow` once the pod is gone.

| POST, GET | `/portForward/{namespace}/{pod}` | `kubectl port-forward` (#56): SPDY/3.1 (`portforward.k8s.io`) or a WebSocket tunnel of it (`SPDY/3.1+portforward.k8s.io`); connects to `localhost:<port>` in the pod's network namespace (the node's for hostNetwork). 404 unknown pod, 403 another protocol, 503 no namespace found |
| POST, GET | `/exec/{namespace}/{pod}/{container}`, `/attach/…` | 501 naming stormpump#103: the engine cannot yet run a process in a running container. Exec probes wait on the same |

The SPDY session (`spdy.rs`) answers each stream with a SYN_REPLY, echoes
PINGs and does no flow control (client-go's spdystream does none either); the WebSocket channel protocols for
exec/attach (`v5.channel.k8s.io`) come with stormpump#103.
The console router knows the stormblock URL but its control verbs are not
exposed by the kubelet route table.

VirtualMachineSnapshot is reconciled as a Kubernetes resource through the
apiserver, not a new kubelet HTTP route. It requires the snapshot CRDs,
`--runtime stormpump`, a locally owned VM and its local storage.
VirtualMachineRestore is served the same way, by the node that took the
snapshot (#53, option A of #109: the VM's disks are rewritten to restored
PVCs; README, "Restores"). Stormvm VMI migration is also unimplemented (#40).
