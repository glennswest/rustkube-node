# Container startup: current baseline and remaining work

Updated 2026-09-29 from main 5bb1a38. This replaces the old CRI-O/crun/conmon
roadmap, whose shipped-runtime premise no longer applies. stormcos selects
stormpump; the CLI defaults to native and optional CRI uses an external runtime.
See [configuration](../configuration.md) and [implementation audit](../status.md).

## Main today

`kubelet.rs` runs a two-second Pod sync and reconciles the VMI desired state.
`pod_manager.rs::start_pod` resolves volumes, creates a sandbox, resolves
service-account/DNS inputs, runs init containers to successful completion,
and creates/starts app containers. These operations are largely serial.
A volume that is not ready stays Pending/ContainerCreating with a reason and
FailedMount Event. Blank formatting runs in the background, with up to two
seconds of inline waiting. It does not allocate scratch storage for a PVC.

`check_pod_status` implements startup, liveness and readiness probes. Startup
probe success gates the other probes. CrashLoopBackoff exists: first restart
is immediate, subsequent waits begin at ten seconds and double to five minutes;
a stable ten-minute run resets it. Neither backoff nor startupProbe should be
planned as a missing feature. Init containers still use a 500 ms completion
poll with a 120-second limit and are removed after exit (#47).

CRI calls have a 120-second channel timeout; pull/exec carry explicit request
deadlines. This does not bound stormpump ring operations, engine HTTP requests
or local CNI subprocesses. Those gaps are recorded in #99.

## Remaining work, not measured guarantees

- #99/#100: per-UID workers, independent bounded image/volume work, recovery and
  partial-start cleanup. The turbomode branch contains work not merged to main.
- #101: probe period scheduling, runtime/volume notifications and elimination
  of global polling where a real event source exists.
- #102/#110: approved live target and startup/scale/storage acceptance. Measure
  accepted write → assignment → sandbox → process start → Ready, separating
  cold image/template work from warm startup. No subsecond claim is established.
- #103/#98/#86: image-root resolution, image config and version selection.
- #111: restartable init sidecars: implemented (started in their slot, kept, restarted, stopped after the apps).
- #112: persist/reconstruct backoff across kubelet restarts.

The existing metrics measure first observation → start and known-Pod sync
passes; they do not by themselves measure accepted API write → process start.
See [metrics](../metrics.md). Warm sandbox pools, lazy image pulling and an
alternative CRI daemon were exploratory ideas in the former roadmap, not
implemented features or current shipping commitments.
