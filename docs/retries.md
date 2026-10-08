# Retries: every call that leaves the process (#211)

Every call the kubelet, kube-proxy and the test container make to another
process goes through one helper, the `retry` crate (`pkg/retry`), or says
where it is made why it does not. Before #211 nothing retried at the call:
an apiserver call was sent once (a 30 s timeout), an engine call was sent
once (60 s; only a 401 was asked again, with a re-read token), and a single
timeout failed the whole pass, or the whole test.

## The rules

| | |
|---|---|
| **Retried** (infrastructure) | a timeout, a connection refused or reset, an error sending the request, HTTP 408, 429, 500, 502, 503, 504 |
| **Not retried** (a real answer) | any other 4xx or 5xx (400, 401, 403, 404, 409, 422, 501, …), a request that could not be built, a body that does not decode |
| **Backoff** | `first_delay × 2^(n−1)`, capped at `max_delay`, with equal jitter (half fixed, half random) |
| **Retry-After** | honoured (seconds form) up to 30 s |
| **Bound** | at most `attempts`, and no attempt starts past `deadline` from the first; each attempt keeps its own request timeout |
| **Idempotency** | GET, HEAD, OPTIONS, PUT, DELETE, PATCH repeat freely (the apiserver's PUT carries a resourceVersion, so a repeat after an applied write is a 409, a real answer). A POST repeats only when it never left (the connect failed) or the server said it did nothing (429), unless the caller marks it repeatable (`send_repeatable`): a create with a fixed name (a repeat is a 409 the caller reads as "exists"), a TokenReview or TokenRequest |
| **Logged** | `<call>: succeeded (200 OK) on attempt 3 after 4.1 s` (info), `<call>: gave up after 4 attempts / 6.2 s: <last error>` (warn, `class=infrastructure`), `<call>: not retried (HTTP 503): a POST may have been applied …` (warn). Target `retry`, fields `call` and `policy` |
| **Classified** | `retry::class_of_error`, `class_of_status`, `class_of_message`: `Infra` or `Real`. The test container reports what still fails after the retries as `Infra` ("could not run", exit 2), never as a failed test |

## Policies

| Policy | For | Attempts | First delay | Max delay | Deadline |
|---|---|---|---|---|---|
| `API` | the apiserver (kubelet, kube-proxy, test container) | 4 | 250 ms | 4 s | 60 s |
| `ENGINE` | the node's stormblock engine (loopback) | 5 | 250 ms | 5 s | 90 s |
| `REGISTRY` | the node's registry (sbregistry) | 4 | 500 ms | 5 s | 60 s |
| `PEER` | another node service read for information (stormdrive) | 3 | 500 ms | 4 s | 20 s |
| `LOCAL` | a gRPC plugin on a local socket (CSI drivers) | 4 | 200 ms | 2 s | 20 s |

Each policy has a test (`each_policy_survives_failures_then_succeeds`) with a
fake that fails `attempts − 1` times then answers.

## Call sites

Lines are as of the #211 change.

### The stormblock engine

One choke point: `EngineClient::once` (`pkg/kubelet/src/engine.rs:346`),
`retry::send(req, Policy::ENGINE)`, under the 401 token re-read and the admin
token (#105). Every engine call goes through it: claims and class blanks
(`pod_manager.rs`, 12 calls: template clone, mint, raw volume, attach,
detach, delete), container roots (`container_roots.rs`, 5), VM disks
(`vm_manager.rs`, 6: golden clone, emptyDisk, seed, attach, owner), system
claims (`system_claims.rs`), placement (`pv_placement.rs`), the volume watch
(`follow_volumes`, which also reconnects on its own backoff). Before: sent
once. Now: reads, deletes and PUTs retried; a POST (clone, attach, mint)
only when it never reached the engine, because it may have been applied:
its caller finds it again by name on the next pass (the mint already did,
#99).

The restore's stormvm client (`vm_restore.rs:62`, `:67`): blocking; a group
snapshot read and a volume-from-snapshot by name (stormvm answers the one
already made). `retry::blocking(Policy::ENGINE)`, classified by message.
Before: once.

The snapshot take (`vm_snapshot.rs:91`): stormvm's take is idempotent by
name; taken again (`Policy::ENGINE`) only when its disks were not taken for
a transport reason. A refusal, or a guest left paused or frozen, is reported
as it is. Before: once, and a transient failure made the snapshot Failed
for good.

### The registry (`stormpump_runtime.rs`, `Policy::REGISTRY`)

| Line | Call | Before | Now |
|---|---|---|---|
| 1819 | GET `/v1/goldens/{image}` (image config) | once | retried |
| 1848 | POST `/v1/clones` (the demand that starts a cluster fetch) | once | not repeated after it left: it may have minted a clone. Its 503 "fetching" is the registry's answer, retried on the pull back-off |
| 1860 | DELETE the clone minted meanwhile | once | retried |
| 1869 | GET the golden record again | once | retried |
| 1959 | GET `/v1/goldens/{image}` (pull) | once | retried |

### stormdrive

`pv_placement.rs:356`, GET `/api/v1/placement`, `Policy::PEER`. Before:
once; unreachable meant stormblock's half alone. Now: retried first.

### The apiserver (`Policy::API`)

All before: sent once, the next pass the only retry. Now: retried as the
rules say. `R` = POST marked repeatable (named create or review).

| File | Lines |
|---|---|
| `kubelet.rs` | 890 (VMI CRD probe), 933 (snapshot claim patch), 1107, 1668 (launcher / Pod delete, uid precondition), 1144 (PVC), 1226, 1248, 1293, 1333 (migration annotations), 1637 (Pod status PUT, RV), 2125 (mirror list), 2177, 2247 (mirror status PUT), 2231 (mirror annotations), 2254 (Node), 2272 R (mirror Pod create) |
| `pod_manager.rs` | 331, 342 R, 353 (ClaimBinder get/post/put), 872, 922 R, 940 (get/post/put), 1021 (ConfigMap), 1041 (Secret), 1559 (PV delete, uid + RV preconditions), 2418 R (TokenRequest), 4780, 4840 (annotation patches) |
| `pod_manager/csi_volumes.rs` | 657 (claim status patch) |
| `vm_manager.rs` | 1268, 1757 (Secrets), 2628, 2646 (launcher Pods), 2656 (launcher status), 3053, 3185, 3494, 3866 (reads), 3174 R (TokenReview), 3427, 3526, 3607 (VMI patches), 3737 (VMI list), 3775 (VMI watch: its loop relists too) |
| `vm_restore.rs` | 121, 305, 320 (reads), 243 (VM PUT), 277 R, 293 R (restored PVC, PV) |
| `vm_snapshot.rs` | 296, 387 (reads), 336, 431 (patches) |
| `node_status.rs` | 294 R (Node create), 318, 349, 465 (Node get/patch), 404 (Lease PUT), 414 R (Lease create) |
| `system_claims.rs` | 390, 412, 564 (get/put/delete), 398 R (PV/PVC create), 466 R (namespace) |
| `csi_plugins.rs` | 352, 416, 424, 439 (CSINode), 462 R (CSINode create) |
| `capacity.rs` | 223, 235, 270, 241 R (CSIStorageCapacity) |
| `pv_placement.rs` | 373, 393 (PVs) |
| `events.rs` | 233 (event patch), 252 R (event create, fixed name) |
| `server.rs` | 296 R (TokenReview for `/vmInstance`) |
| `pkg/proxy/src/client.rs` | 126 (kube-proxy's lists) |
| `test/src/api.rs` | 40 (every test call; POST not repeated unless it never left) |

### Not retried, and why

| Where | Why |
|---|---|
| `health.rs` (HTTP probes) | a probe is asked once per period; its failureThreshold is the retry |
| `node_health.rs` (node services' liveness) | three failures in a row are the threshold |
| `cri_grpc.rs` (CRI runtime) | RunPodSandbox / CreateContainer make a new object each time; Unavailable is `CriError::Connection`, a wait the pod worker retries on its backoff after reading back what exists |
| `stormpump_ring.rs` (PID 1's ring) | shared memory on this node, bounded by `DEADLINE`; SPAWN, CLONE, SANDBOX_ACQUIRE are not safe to repeat |
| `pkg/cni/src/invoker.rs` (CNI plugin exec) | bounded (60 s); an ADD allocates, a failed one is followed by DEL and the start retried on the CNI backoff (#148) |
| The kubelet's Pod watch | apimachinery's reflector (rustkube), with its own relist and backoff |

CSI node RPCs (`csi.rs`, `CsiDriverClient::call`) **are** retried
(`Policy::LOCAL`): every CSI node call is idempotent by the spec.
Unavailable, DeadlineExceeded, ResourceExhausted and Aborted (the spec's
"operation pending, retry") are infrastructure; anything else is the
driver's answer.
