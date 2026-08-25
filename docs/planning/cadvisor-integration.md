# Integrating cadvisor-rs into the kubelet

**Status: plan, 2026-08-25.** Tracking issue: [#21](https://github.com/glennswest/rustkube-node/issues/21).
Repos: [glennswest/cadvisor](https://github.com/glennswest/cadvisor) (cadvisor-rs),
this one, and [stormcos](https://github.com/glennswest/stormcos) (which packages it).

---

## 1. What actually changed upstream

The prompt for this was "cadvisor has been replaced upstream". Checked, because
the fix depends on which of several things that means:

| Claim | Verdict |
|---|---|
| google/cadvisor is archived/dead | **No.** Active: v0.60.5 on 2026-07-11, pushed 2026-07-20, 63 open issues. |
| The v1/v2 REST API and web UI were removed | **No.** Still shipped, under `cmd/internal/{api,http,pages}` — plus new `appmetrics` and `processlist`. |
| cAdvisor was replaced for container stats | **Yes, and years ago.** Since k8s 1.26, `/metrics/cadvisor` container/pod series come from the CRI (KEP-2371). |
| cAdvisor is gone from the kubelet | **No.** It still owns node, machine, filesystem and volume stats, which the CRI does not provide. |
| **Something was restructured in June 2026** | **Yes — and this is the real answer.** |

**v0.60.0 (2026-06-20) split the project into two Go modules.** `cache`,
`machine`, `manager`, `metrics`, `stats`, `storage`, `version` and `watcher`
moved out of the root module into `github.com/google/cadvisor/lib` — described
by upstream as "a lean, kubelet-focused library module", with its own `go.mod`
and a trimmed dependency set. The binary keeps the REST API, the web UI, OOM
watching and the app-metrics collector.

Upstream drew the line explicitly, in `lib/manager/manager.go`:

> *The methods below back the full cAdvisor binary's v1/v2 REST API and web UI.
> They are pure queries over the in-memory container registry and add no
> dependencies; **the kubelet does not call them**.*

So the kubelet-facing surface is now named and small: `Start`/`Stop`,
`GetContainerInfoV2`, `GetRequestedContainersInfo`, **`GetMachineInfo`**,
`GetVersionInfo`, **`GetDirFsInfo`**, **`GetFsInfo`**, `DebugInfo`.

Three follow-up commits in `lib/` say what "kubelet-focused" cost them, and
each one is a decision we would otherwise have to rediscover:

- `lib/manager: allow disabling container discovery` — an embedded cAdvisor
  must not go looking for containers. The CRI owns that list.
- `lib/model: make ContainerStats sub-stats pointers to convey collection
  presence` — nil means *not collected*, which a zero cannot say.
- `Move OOM watching out of the lib module into the binary` — a side effect,
  not a query, so it does not belong in something a kubelet links.

**Also relevant, and not cAdvisor's doing:**

- **PSI is GA in k8s 1.36**: the kubelet ingests CPU/memory/IO pressure from
  cAdvisor and runc and embeds it in the Summary API at node *and* pod level.
  That is new required surface, and it is what real pressure conditions want.
- **cgroup v1 is deprecated** (k8s 1.35; kubelet fails on cgroup v1 by default
  under KEP-5573, removal no earlier than 1.38). cadvisor-rs is cgroup-v2 only,
  so we are early rather than late here.

## 2. Where we actually stand

**cadvisor-rs is already shaped like the thing upstream just built.** Its
crates split along the same seam, and `cadvisor-manager::Manager` already
exposes the kubelet-facing set almost method for method:

| upstream `lib/manager` | `cadvisor-manager::Manager` |
|---|---|
| `GetMachineInfo` | `machine_info()` |
| `GetVersionInfo` | `version_info()` |
| `GetFsInfo` | `storage_info()` |
| `GetDirFsInfo` | `machine_fs_stats()` |
| `Start` | `start()` |
| the REST/UI-only queries | `container_info()`, `subcontainers_info()`, `docker_containers()`, `events()` |

`cadvisor-model` / `-host` / `-runtime` / `-manager` / `-metrics` are the
library half; `cadvisor-api` and the `cadvisor` binary are the daemon half.
**No restructuring is needed. The integration is a dependency edge we have not
drawn yet.**

**This kubelet is further along than #21 says.** The issue describes
`MemoryPressure`/`DiskPressure`/`PIDPressure` as hardcoded `False` and
`ephemeral-storage` as a made-up `50Gi`/`45Gi`. That is stale —
`node_status.rs:205-240` computes all four from real data now ("Real system +
filesystem stats (no longer faked)"): `available_memory_ki()`,
`ephemeral_fs_stats()`, `pid_stats()`. **#21's premise needs correcting before
anyone plans against it.**

What is genuinely still missing:

1. **`node_status.rs` re-implements cadvisor-host.** `get_system_resources()`,
   `ephemeral_fs_stats()`, `available_memory_ki()`, `pid_stats()` are a second
   procfs/statfs reader living beside a crate that exists to be one. Two
   readers is how two answers happen.
2. **Thresholds are approximations, not the eviction signal set.** `<100Mi`
   available and `<10%` nodefs are the right defaults, but they are hardcoded
   at the comparison rather than derived from configured eviction signals, and
   `imagefs` is not distinguished from `nodefs`.
3. **`/metrics/cadvisor` emits two series** — `container_cpu_usage_seconds_total`
   and `container_memory_working_set_bytes`, hand-rendered in `server.rs:202`.
   Dashboards expect the `container_*` set plus `machine_*`.
4. **`/stats/summary` has no real node section**, so `kubectl top node`, the HPA
   path and eviction all read from something thin.
5. **No PSI**, anywhere.

## 3. The plan

Ordered so each phase is useful alone and nothing is blocked on the phase after
it.

### Phase 1 — declare the library seam in cadvisor-rs *(cadvisor repo)*

Adopt upstream's split explicitly rather than by coincidence.

- Add `cadvisor-kubelet`: a thin facade crate re-exporting exactly the
  kubelet-facing surface (machine, fs, version, node stats), so the boundary is
  a compile error to cross rather than a convention. Upstream needed a second
  Go module for this; a facade crate is the Rust equivalent and costs less.
- Add **`discovery: bool`** to `ManagerConfig` (upstream's `allow disabling
  container discovery`). Embedded in a kubelet, cAdvisor must not enumerate
  containers — the CRI already did, and two enumerations disagree at exactly
  the wrong moment.
- Audit `cadvisor-model` for **nil-vs-zero**: sub-stats that were not collected
  must be `Option`, not `0`. A zero reported as fact is worse than a gap.
- Keep `cadvisor-api` + the binary exactly as they are. Standalone monitoring
  is still a real deployment (ironprom scrapes it), and this phase does not
  touch it.

**Deliverable:** rustkube-node can add one dependency and get node/fs/machine
without pulling in axum or the REST API.

### Phase 2 — consume it in the kubelet *(this repo, closes #21)*

- Depend on `cadvisor-kubelet` (git dependency, pinned; the two repos release
  independently and a path dep would couple them).
- **Replace** the four duplicated readers in `node_status.rs` with it. Delete
  them in the same commit — a fallback path that is never exercised is a second
  implementation that rots.
- Feed the real numbers into: node conditions and `ephemeral-storage`
  (already the shape, better source), the `/stats/summary` **node** section, and
  `/metrics/cadvisor`'s `machine_*` series.
- **Container stats stay on the CRI.** This is upstream's model and already
  ours; the library is configured with discovery off (Phase 1) so it cannot
  drift into that job.
- Answer #21's open question in the code: **node/fs/machine only**. Richer
  container metrics can come later from the CRI stats we already collect,
  rendered through `cadvisor-metrics` rather than by hand in `server.rs`.

**Deliverable:** real disk-pressure eviction, honest `kubectl top node`, and one
procfs reader in the tree instead of two.

### Phase 3 — close the upstream drift *(cadvisor repo)*

cadvisor-rs's conformance contract is pinned to **v0.49.2**; upstream is at
**v0.60.5** — eleven releases, including v0.53, v0.54, v0.55, v0.56, v0.57 and
the v0.60 split.

- Re-run `conformance/` against v0.60.5 and record what moved. The harness
  exists and diffs structurally, so this is a run-and-read, not a rewrite.
- Pick up **the additional cgroup v2 `memory.stat` metrics** from v0.60.0.
- Re-pin the stated contract, or state deliberately that we conform to v0.49.2
  *plus* a named delta. Either is defensible; silence is not.

### Phase 4 — PSI *(both repos)*

New surface, GA upstream in 1.36, and the thing that turns our pressure
conditions from thresholds into signals: read `cpu.pressure`, `memory.pressure`
and `io.pressure` from cgroup v2, expose them through the library, and put them
in `/stats/summary` at node and pod level.

### Phase 5 — packaging *(stormcos)*

stormcos ships cadvisor as a **standalone RPM with a systemd unit**
(`editions/kubernetes.toml`, `README.md`). Once the kubelet embeds the library,
that daemon is monitoring-only — still wanted where ironprom scrapes a node,
redundant where it does not. Decide per edition rather than by default, and say
which in the edition file.

## 4. Decisions this needs from a human

1. **Does the standalone daemon stay in the kubernetes edition?** It duplicates
   what the kubelet will serve. Keeping both means two producers of
   `machine_*` on one node.
2. **Git dependency or vendored crate?** Recommending a pinned git dependency:
   the two repos have their own release cadences, and vendoring makes upstream
   drift invisible — which is exactly the failure this whole plan is about.
3. **Conformance target.** Chase v0.60.5, or freeze at v0.49.2 + delta? Chasing
   costs work each release; freezing costs a growing lie in the README.

## 5. Risks worth naming

- **Two producers, one metric name.** If both the kubelet and the standalone
  daemon export `machine_*` from one node, ironprom sees duplicate series that
  differ only by port. Decide Phase 5 before Phase 2 lands anywhere real.
- **The library links into the kubelet's process.** A panic in a metrics path
  becomes a kubelet outage. Upstream moved OOM watching out of `lib` for a
  version of this reason; anything that is not a pure query deserves the same
  scrutiny before it crosses the seam.
- **cgroup v1.** Being v2-only is correct going forward and means cadvisor-rs
  cannot run on a cgroup v1 node at all. That is fine for stormcos and a hard
  stop anywhere else — worth stating in the README rather than discovering.
- **#21 is stale.** Anyone reading it today will plan to fix things that are
  already fixed.

## 6. What I would do first

Phase 1's `discovery: bool` and the `Option` audit, then Phase 2's reader
replacement. That is the smallest change that deletes a duplicate
implementation, and it makes eviction correct — which is the only item here
with a user-visible failure mode today.
