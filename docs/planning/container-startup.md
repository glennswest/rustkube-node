# Container Startup — Analysis & Roadmap

Status: forward-looking planning doc. To be reviewed **after** the in-flight Cilium
bringup. Not urgent execution.

Scope: the container-startup path in the rustkube-node kubelet on a stock
**CRI-O 1.32 → crun 1.24 → conmon 2.1.13** node, `--runtime=cri` over
`unix:///run/crio/crio.sock`, with Cilium as the sole CNI. Project posture is an
**OCP-6-class stack** — track latest (k8s 1.36 / CRI-O 1.36 / crun), never pin
behind. crun is the default OCI runtime for fast startup. Container stats come from
CRI; node/fs/machine stats are owned by cadvisor (glennswest/cadvisor, see #21).

All file/function references are to `pkg/kubelet/src/`.

---

## 1. Current startup path, step by step

The desired-state loop lives in `kubelet.rs`: `sync_interval` is **2s**
(`kubelet.rs:43`), and `recover_state()` runs once before the loop
(`kubelet.rs:149`). Each tick calls `PodManager::sync_pods` (`pod_manager.rs:508`).

For a **new** pod, `sync_pods` → `start_pod` (`pod_manager.rs:709`) runs this
sequence, strictly ordered:

1. **`build_sandbox_config`** (`pod_manager.rs:1369`) — derives name/uid/namespace,
   `log_directory = /var/log/pods/<ns>_<name>_<uid>`, DNS (defaults to
   `10.96.0.10` + cluster search domains), labels, and `hostNetwork/hostPID/hostIPC`.
   Cheap, in-process.

2. **`resolve_volumes`** (`pod_manager.rs:256`) — materializes configMap/secret/
   projected/emptyDir volumes to per-pod dirs and, for projected SA volumes,
   requests a token via the TokenRequest API (`request_sa_token`,
   `pod_manager.rs:366`). **Latency:** one apiserver round-trip per configMap /
   secret / SA-token, all **serial** and **synchronous**, before the sandbox even
   exists. Cold apiserver or many volumes → measurable pre-sandbox delay.

3. **`run_pod_sandbox`** (CRI `RunPodSandbox`, `cri_grpc.rs:289`) — CRI-O creates
   the **pause** container **and invokes Cilium** (CNI ADD) to wire netns/IP. This
   is typically the single largest fixed cost of pod startup and is entirely inside
   CRI-O + the CNI. Note: unlike `pull_image` and `exec_sync`, `run_pod_sandbox` is
   **not** wrapped in the `timed()` per-RPC deadline helper (`cri_grpc.rs:74`), so a
   hung CNI ADD relies only on the channel default — worth confirming given #5
   ("a hung RunPodSandbox blocks the whole sync loop") was the motivating bug.

4. **`pod_sandbox_status`** — fetch the pod IP (needed for downward-API `status.podIP`).

5. **`service_account_mount`** (`pod_manager.rs:408`) — one more TokenRequest +
   file writes if SA admission didn't already inject a projected volume.

6. **`run_init_containers`** (`pod_manager.rs:623`) — **strictly serial**, each to
   exit 0 before the next. Per init container: `ensure_image` → resolve env/mounts →
   `build_container_config` → `ensure_container_log_dir` → `create_container` →
   `start_container` → **poll every 500ms up to 120s** (`POLL_MS`/`MAX_WAIT_MS`,
   `pod_manager.rs:659`) for exit. The 500ms poll granularity alone adds up to 0.5s
   of latency per init container even for instant-exit inits.

7. **App containers** — the loop at `pod_manager.rs:758`, also **serial** per
   container:
   - **`ensure_image`** (`pod_manager.rs:693`) honoring `imagePullPolicy`
     (`effective_pull_policy`, `pod_manager.rs:1409`): `Never` = status-only,
     `IfNotPresent` = status then pull-if-absent, `Always`/untagged/`:latest` =
     `pull_image` (CRI `PullImage`, **600s timeout**, `cri_grpc.rs:593`). Cold pull
     dominates first-start latency; cached path is one `image_status` RPC.
   - **`resolve_env`** (`pod_manager.rs:175`) + `merge_env(service_account_env)` —
     more apiserver reads for env `valueFrom` configMap/secret refs.
   - **`resolve_mounts`** + `push_mount(sa_mount)`.
   - **`build_container_config`** (`pod_manager.rs:1697`) — now does `$(VAR)`
     expansion in command/args (`expand_env_refs`, `pod_manager.rs:1647`), cgroup
     translation (`parse_cpu_*`, `parse_memory_bytes`), securityContext passthrough.
   - **`ensure_container_log_dir`** (`pod_manager.rs:1634`) — mkdir the `<name>`
     subdir conmon needs (otherwise "Failed to open log file").
   - **`create_container`** then **`start_container`** (CRI) — crun+conmon spawn.
   - Readiness seed: ready = true immediately **unless** a `readinessProbe` exists
     (`pod_manager.rs:788`).

8. Pod is recorded Running in the in-memory map (`pod_manager.rs:806`). **Ready**
   is gated later by `check_pod_status` running the readiness probe only after
   `initialDelaySeconds` (`probe_initial_delay`, `pod_manager.rs:1609`).

### Where latency lives (summary)

| Stage | Cost | Cold vs cached |
|---|---|---|
| Volume/env apiserver reads | serial RTTs, pre-sandbox | worsens with cold apiserver |
| Sandbox + CNI (Cilium) | fixed, largest single step | ~constant |
| Image pull | dominant on first start | cold: seconds–minutes; cached: 1 RPC |
| Init containers | serial + 500ms poll floor each | serial chain multiplies |
| create/start per container | crun ~fast; serial | crun ~50–100ms/container faster than runc |
| Probe initialDelay | delays Ready, not start | user-configured |

### Where failure modes live

- **Init container non-zero exit** aborts the whole pod start (`pod_manager.rs:665`);
  the caller reports `Failed`; next 2s sync retries from scratch — no backoff.
- **Init container exceeds 120s** → `CriError::Timeout`, pod start aborts.
- **Image pull failure / `Never` with absent image** → `CriError::ImagePull`, pod
  `Failed`.
- **create/start error** → pod `Failed`, retried next sync.

---

## 2. Known gaps / risks observed this project

- **No restart backoff / no CrashLoopBackOff.** A crashed or missing container is
  recreated on the very next sync. With `sync_interval = 2s`, a crash-looping
  container is recreated **roughly every 2s** indefinitely. `restart_container`
  (`pod_manager.rs:1157`) bumps `restart_counts` but never consults elapsed time or
  an exponential delay. This is the churn seen in the **889-pod cilium-operator**
  runaway during Cilium bringup — combined with the (now-fixed, #15) mis-probing, it
  hammered otherwise-healthy pods. **Highest-priority reliability gap** (the #15
  "add restart backoff" follow-up).

- **No `startupProbe`.** `check_pod_status` handles only liveness + readiness
  (`pod_manager.rs:972`, `:1012`); there is no `startupProbe` grep hit anywhere.
  Slow-starting containers can be liveness-killed before they finish booting; the
  only mitigation today is `initialDelaySeconds`.

- **Init containers strictly serial**, each with a 500ms poll floor
  (`run_init_containers`, `pod_manager.rs:623`). No sidecar/native-sidecar
  (`restartPolicy: Always` init) support — those would run-and-block forever here.

- **Image handling is serial with no parallelism / pre-pull / pinning.** Each
  container pulls in turn inside the start loop; there is a 600s timeout but no
  concurrent pulls across containers, no warm cache, no pinned-image guarantee.

- **Sandbox reuse only via `recover_state`.** Adoption happens **once at startup**
  (`pod_manager.rs:441`, adopts running sandboxes and maps container names→ids). No
  warm sandbox/pause pool; every fresh pod pays full sandbox+CNI cost.

- **`recover_state` sets `pod: Value::Null`** (`pod_manager.rs:484`) until the next
  sync refreshes spec — a brief window where an adopted pod has no spec to
  reconcile against.

- **Whole-pod-start abort semantics.** A single init/image/create failure fails the
  entire pod and restarts the sequence from step 1 next sync — no partial-progress
  memoization, so cold image pulls can be re-attempted after an unrelated later
  failure.

- **`check_pod_status` reconcile** (`pod_manager.rs:1092`) and the `NotFound`
  recreate path (`pod_manager.rs:903`) correctly re-adopt/recreate missing
  containers — but they too feed the no-backoff recreate loop.

---

## 3. Startup-latency levers

1. **crun as default OCI runtime — already in place.** crun trims roughly
   **~50–100ms per container** off create/start vs runc (no Go runtime, lower memory,
   faster cgroup setup). At pod scale (multi-container pods, DaemonSets across many
   nodes) this compounds. Keep crun the default; track crun releases with CRI-O.
   *Effort: done; keep current.*

2. **Image pre-pull / warm cache / pinned images.** Pre-pull critical images
   (pause, CNI, cluster-critical DaemonSets) at node boot; pin them so CRI-O GC
   can't evict. Biggest cold-start win. Hooks into `ImageService::pull_image` /
   `list_images` (`cri.rs:256`). *Effort: medium.*

3. **Parallel app-container create/start.** The `start_pod` container loop
   (`pod_manager.rs:758`) is serial; app containers within a pod have no ordering
   contract (unlike init containers) and could be created/started concurrently
   (`join_all` over the per-container future). Also enables **parallel image pulls**
   across containers of the same pod. *Effort: medium; moderate win on multi-container pods.*

4. **Sandbox / pause reuse (warm pool).** Pre-create a small pool of sandboxes (or
   at least keep pause images warm) so `run_pod_sandbox` + CNI ADD isn't fully cold
   per pod. Higher complexity because CNI wiring is pod-specific; realistically a
   later item and easier if/when we own the CRI (§5). *Effort: high.*

5. **Lazy / on-demand image pulling (stargz / nydus).** Start containers before the
   full image is present, faulting in layers on read. Large win for big images and
   scale-out, but requires a snapshotter CRI-O supports. **Future / OCP-6 horizon.**
   *Effort: high; external dependency.*

6. **conmon-rs (pod-level monitor) vs conmon.** conmon-rs is a single Rust monitor
   per pod (vs one conmon process per container), reducing per-container process
   overhead and giving a cleaner attach/log API. CRI-O already supports it. Modest
   direct startup win, but strategically aligned with an all-Rust stack (§5).
   *Effort: low (config flip on CRI-O) to evaluate.*

---

## 4. Reliability levers

1. **CrashLoopBackOff with exponential backoff — highest priority.** Add per-container
   backoff state (last-restart `Instant`, current delay) to `PodState`
   (`pod_manager.rs:24`) and gate `restart_container` (`pod_manager.rs:1157`) and the
   `Exited`/`NotFound`/reconcile recreate paths on it. Match upstream: start 10s,
   double to a 300s cap, reset after the container stays up long enough. Directly
   kills the ~2s recreate churn (the 889-pod runaway). This is the explicit #15
   follow-up. *Effort: medium; highest impact.*

2. **`startupProbe` support.** Add a third probe branch in `check_pod_status`: while
   a startupProbe is defined and not yet succeeded, suppress the liveness probe and
   hold Ready=false; on success, hand off to liveness/readiness. Prevents
   liveness-killing slow starters. `health.rs::run_probe` already runs in the pod
   netns and resolves named ports (#15 done), so this is mostly control-flow in
   `pod_manager.rs`. *Effort: low–medium.*

3. **Better failure surfacing in pod status / events.** Today failures collapse to
   `phase = Failed` + a message, or a `waiting` container status
   (`ContainerStatusReport`, `pod_manager.rs:1357`). Add structured reason/message
   (e.g. `CrashLoopBackOff`, `ImagePullBackOff`, `RunContainerError`) and, ideally,
   emit Events so `kubectl describe` is diagnosable. *Effort: medium.*

4. **Graceful restart-time adoption.** `recover_state` adopts running sandboxes but
   loses restart counts, backoff state, and readiness across a kubelet restart
   (`pod_manager.rs:441`, sets `pod: Null`, zeroed maps). Persist minimal per-pod
   state (restart counts, backoff, terminated) to disk so a kubelet bounce doesn't
   reset backoff and re-churn. *Effort: medium.*

---

## 5. The Rust-CRI question (#22)

**Question:** stay on stock CRI-O (Go) or grow the native youki/libcontainer path
(`runtime.rs` — the `--runtime=native` seed) into a full Rust CRI daemon
(youki + conmon-rs + a Rust CRI gRPC server + storage/overlay + image mgmt + CNI
invocation).

**Startup-specific upside of owning the CRI:**
- Full control of the create/start path — we could implement **warm sandbox/pause
  pools** (§3.4), **parallel create**, and **lazy image faulting** on our own terms
  instead of waiting on CRI-O feature flags.
- Tighter integration with our sync loop (no gRPC hop for hot paths; direct backoff
  and reconcile without CRI round-trips).
- Uniform all-Rust stack (crun→youki, conmon→conmon-rs) matching the OCP-6 posture.

**Cost / risk:**
- Reimplementing **image management + overlay/containers-storage** is the hard,
  high-risk part — it is most of what CRI-O actually is, and gets us little
  *startup* benefit over a warm cache on CRI-O.
- Loses OpenShift/CRI-O parity, SELinux/seccomp maturity, CVE response, and the
  large `ImageService`/`RuntimeService` surface CRI-O already satisfies
  (`cri.rs:186`, `:254`).
- Cilium already covers networking, so no netavark/aardvark rewrite is needed —
  that narrows scope, but storage/image remains the tall pole.

**Recommendation (matches #22):** **stay on CRI-O until node + Cilium bringup is
proven.** The startup wins we want most (backoff, startupProbe, pre-pull, parallel
create) are all achievable **on CRI-O today** and should land first. Then scope a
Rust CRI as a phased epic, not a big-bang rewrite:

- **Spike 0:** flip CRI-O to **conmon-rs** and measure — zero code, proves the Rust
  monitor path and buys a data point.
- **Spike 1:** exercise the existing `runtime.rs` youki path (`--runtime=native`)
  for single-container pods; measure create/start latency vs CRI-O+crun.
- **Phase A:** youki + conmon-rs behind a **Rust CRI gRPC server** that still
  delegates **image + storage to containers/storage** (don't rewrite overlay).
- **Phase B:** only if Phase A shows a decisive startup/ownership win, take on
  native image/storage — the point of no return.

Gate each phase on measured startup improvement over "CRI-O + warm cache + parallel
create + backoff." If those §3/§4 items close the gap, the CRI rewrite stays a
research track, not a delivery.

---

## 6. OCP-6 alignment (k8s 1.36 / CRI-O 1.36 / crun)

Track latest; do not pin behind. Startup-relevant changes to plan for:

- **Image volumes (`image` volume source, now GA-track).** Mount an OCI image
  read-only as a volume. Affects `resolve_volumes` (`pod_manager.rs:256`) and needs
  the CRI to support the image-volume mount — a new volume branch and a pull/mount
  step in the startup path.
- **User namespaces (`hostUsers: false`, maturing).** Per-pod userns changes sandbox
  creation and file ownership/relabel of materialized volumes; interacts with our
  `selinux_relabel` logic (`resolve_mounts`, `pod_manager.rs:1579`) and
  `build_sandbox_config`.
- **In-place pod resize (`resources` update without restart), GA in 1.33+.** Today a
  resources change would be seen as spec drift and could recreate the container. We
  should honor `UpdateContainerResources` (a CRI call not yet in our
  `RuntimeService` trait, `cri.rs:186`) rather than restart — relevant to *not*
  triggering needless startups.
- **Sidecar (native) containers — `initContainers` with `restartPolicy: Always`,
  GA.** Our `run_init_containers` (`pod_manager.rs:623`) polls each init to exit;
  a native sidecar never exits, so it would hang pod start today. Must start
  sidecars, wait for their *started/ready*, then proceed to app containers.
- **crun / CRI-O 1.36 cgroup v2 + faster startup paths.** Keep crun current; verify
  our `parse_cpu_*`/`parse_memory_bytes` cgroup translation (`pod_manager.rs:1775+`)
  stays correct under cgroup v2 semantics.

---

## 7. Phased roadmap (P0 / P1 / P2)

Ranked by startup/reliability impact vs effort. "Touches" lists the primary files.

### Already done (baseline — do not re-do)
- `$(VAR)` command/arg expansion — `expand_env_refs`, `build_container_config`
  (`pod_manager.rs:1647`, `:1697`).
- Container log-dir creation for conmon — `ensure_container_log_dir`
  (`pod_manager.rs:1634`).
- SELinux relabel of kubelet-materialized mounts — `resolve_mounts`
  (`pod_manager.rs:1591`).
- Missing-container reconcile + `NotFound` recreate — `check_pod_status`
  (`pod_manager.rs:903`, `:1092`).
- Probes run in the pod netns + named-port resolution (**#15**) — `health.rs`
  (`resolve_port`, `dial_in_netns`).
- Per-RPC CRI timeouts incl. `pull_image` 600s (**#5**) — `cri_grpc.rs:74`.
- crun as default OCI runtime (fast startup).
- Startup state recovery / sandbox adoption — `recover_state` (`pod_manager.rs:441`).

### P0 — reliability first, kill the churn (highest impact, low–medium effort)
1. **CrashLoopBackOff / exponential restart backoff.** Add backoff fields to
   `PodState`; gate all recreate paths. Touches `pod_manager.rs`
   (`PodState`, `restart_container`, `check_pod_status`). *#15 follow-up.*
2. **`startupProbe` support.** Third probe branch; suppress liveness until startup
   succeeds. Touches `pod_manager.rs` (`check_pod_status`); reuses `health.rs`.
3. **Structured failure reasons (CrashLoopBackOff / ImagePullBackOff /
   RunContainerError).** Touches `ContainerStatusReport` + status reporting in
   `pod_manager.rs`, `node_status.rs`/status push.
4. **Confirm `run_pod_sandbox` per-RPC deadline.** Wrap in `timed()` if missing so a
   hung CNI ADD can't stall the sync loop. Touches `cri_grpc.rs:289`.

### P1 — startup latency, moderate effort
5. **Image pre-pull + pinning of critical images** (pause, CNI, cluster-critical
   DaemonSets) at node boot. Touches `ImageService` usage; new pre-pull step in
   `kubelet.rs` startup / `pod_manager.rs`.
6. **Parallel app-container create/start + parallel per-pod image pulls.** Touches
   the `start_pod` container loop (`pod_manager.rs:758`).
7. **Native sidecar containers** (`initContainers` w/ `restartPolicy: Always`).
   Touches `run_init_containers` (`pod_manager.rs:623`).
8. **Persist per-pod restart/backoff state across kubelet restarts** so adoption
   doesn't reset backoff. Touches `recover_state` (`pod_manager.rs:441`) + a small
   on-disk store.
9. **Reduce init-container poll floor** (event/shorter poll instead of 500ms fixed).
   Touches `run_init_containers` (`pod_manager.rs:659`).

### P2 — strategic / horizon, high effort or external
10. **conmon-rs spike** (CRI-O config flip) + **youki `--runtime=native` latency
    benchmark** (`runtime.rs`) — data for #22.
11. **Warm sandbox / pause pool.** Touches sandbox creation; easier post-CRI-ownership.
12. **Lazy image pulling (stargz/nydus)** — external snapshotter dependency; OCP-6 horizon.
13. **OCP-6 features:** image volumes (`resolve_volumes`), user namespaces
    (`build_sandbox_config` + relabel), in-place resize
    (`UpdateContainerResources`, new `RuntimeService` method in `cri.rs`).
14. **Rust CRI epic (#22)** — phased per §5, gated on measured startup wins over the
    CRI-O baseline after P0/P1.

---

### One-line ranking rationale
P0 removes the observed failure amplifier (2s recreate churn / 889-pod runaway) at
low cost and unblocks safe Cilium operation. P1 attacks real cold-start latency
(pull + serial create) without leaving CRI-O. P2 is where the all-Rust CRI and
OCP-6 startup features live — pursued only after the cheaper wins are proven.
