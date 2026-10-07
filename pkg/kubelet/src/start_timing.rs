//! Where a pod's start goes, phase by phase (#132).
//!
//! One [`StartTiming`] per pod UID, from the moment the kubelet's pod list
//! (or a static manifest read) first delivers the pod to the moment the
//! apiserver acknowledges `Running`. It outlives retries: a pod that waits on
//! an image or a claim keeps one record, and the phases of the attempt that
//! started it are what is reported, with the time before that attempt as
//! `wait`.
//!
//! The phases, all from monotonic clocks except `scheduled`:
//!
//! | phase        | from → to |
//! |--------------|-----------|
//! | `scheduled`  | the pod's `PodScheduled` transition (else `creationTimestamp`) → seen here; wall clocks of two machines |
//! | `wait`       | seen → the start attempt that succeeded began (admission, image and volume waits, retries) |
//! | `image`      | this pod's images first asked for → the last of them resolved; a pull is the registry's clone, its attach and its mount |
//! | `volumes`    | every volume of the pod (each also as `volume/<name>`), claims cloned and attached included |
//! | `sandbox`    | the sandbox made (its network included) → its address read |
//! | `sandbox/acquire` | stormpump `SandboxAcquire`, the warm namespace holder (#139) |
//! | `sandbox/cni`     | the CNI ADD, the plugin's exec included |
//! | `sandbox/status`  | the sandbox's status read (its address) |
//! | `sandbox/other`   | the rest of `sandbox`: the runtime's checks, retried DELs of earlier failed networks, bookkeeping |
//! | `init`       | init containers, run to completion |
//! | `containers` | every app container created and started (each also as `container/<name>`) |
//! | `report`     | the `Running` status sent → acknowledged |
//! | `total`      | seen → `Running` acknowledged |
//!
//! Two counts say what the attempt queued behind (#138): `workers=<busy>/<limit>`,
//! the executor's passes running when it began (this one included) out of
//! `--pod-workers`, and `pending=<n>`, the pods seen here and not yet started
//! (this one included).
//!
//! The `sandbox/*` steps are written when this attempt made the sandbox: acquire
//! and cni only from a runtime that times them (stormpump; a CRI runtime gives
//! `sandbox/status` and `sandbox/other`). None of them is a stormblock call: a
//! pod's root is a golden mounted at boot (#139, stormblock#264).
//!
//! `image` overlaps `wait` (an image is resolved off the worker while the pod
//! waits); the rest follow one another, so `wait + volumes + sandbox + init +
//! containers + report` is `total` less the gaps between steps.

use std::time::{Duration, Instant};

use serde_json::Value;

/// The annotation the breakdown is written to.
pub const ANNOTATION: &str = "storm.io/start-timing";
/// The Event reason it is recorded under.
pub const REASON: &str = "StartTiming";

/// The phases with a histogram, in the order they are written.
pub const PHASES: &[&str] = &[
    "scheduled", "wait", "image", "volumes", "sandbox", "init", "containers", "report", "total",
];

/// `sandbox`'s steps (#139), with a histogram each when they were measured.
pub const SANDBOX_STEPS: &[&str] = &["sandbox/acquire", "sandbox/cni", "sandbox/status", "sandbox/other"];

/// One pod's start, as far as it has got.
#[derive(Debug, Clone)]
pub struct StartTiming {
    seen: Instant,
    /// Milliseconds from scheduling to seen, by wall clock. Negative when the
    /// two machines' clocks disagree by more than the gap; reported as is.
    scheduled_ms: Option<i64>,
    attempts: u32,
    image_asked: Option<Instant>,
    image: Option<Duration>,
    attempt: Option<Attempt>,
    /// `start_pod` returned the pod running: what is left is the report.
    started: bool,
}

/// `sandbox` taken apart (#139).
#[derive(Debug, Clone, Copy)]
struct SandboxSplit {
    /// The runtime's own steps, when it times them.
    made: Option<crate::cri::SandboxSteps>,
    status: Duration,
}

/// The steps of one start attempt.
#[derive(Debug, Clone)]
pub struct Attempt {
    began: Instant,
    volumes: Duration,
    per_volume: Vec<(String, Duration)>,
    sandbox: Duration,
    /// What making the sandbox was made of (#139), when this attempt made it.
    sandbox_steps: Option<SandboxSplit>,
    init: Duration,
    containers: Duration,
    per_container: Vec<(String, Duration)>,
    /// The executor's pool when the attempt began, `busy/limit` (#138).
    workers: Option<String>,
    /// Pods seen here and not yet started when it began, this one included.
    pending: Option<usize>,
}

impl Attempt {
    pub fn begin() -> Self {
        Self {
            workers: None,
            pending: None,
            began: Instant::now(),
            volumes: Duration::ZERO,
            per_volume: Vec::new(),
            sandbox: Duration::ZERO,
            sandbox_steps: None,
            init: Duration::ZERO,
            containers: Duration::ZERO,
            per_container: Vec::new(),
        }
    }
    /// What the attempt queued behind: the pool, and the pods still waiting.
    pub fn queue(&mut self, workers: Option<String>, pending: usize) {
        self.workers = workers;
        self.pending = Some(pending);
    }
    pub fn volume(&mut self, name: &str, took: Duration) {
        self.per_volume.push((name.to_string(), took));
    }
    pub fn volumes(&mut self, took: Duration) {
        self.volumes = took;
    }
    pub fn sandbox(&mut self, took: Duration) {
        self.sandbox = took;
    }
    /// The sandbox this attempt made: the runtime's steps (`None` from one that
    /// does not time them) and the status read after it (#139).
    pub fn sandbox_steps(&mut self, made: Option<crate::cri::SandboxSteps>, status: Duration) {
        self.sandbox_steps = Some(SandboxSplit { made, status });
    }
    pub fn init(&mut self, took: Duration) {
        self.init = took;
    }
    pub fn container(&mut self, name: &str, took: Duration) {
        self.containers += took;
        self.per_container.push((name.to_string(), took));
    }
}

impl StartTiming {
    /// Seen at `seen`. `pod` gives the scheduling time, when it has one.
    pub fn new(pod: &Value, seen: Instant) -> Self {
        Self::seen_at(pod, seen, chrono::Utc::now())
    }

    fn seen_at(pod: &Value, seen: Instant, now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            seen,
            scheduled_ms: scheduled_at(pod).map(|t| (now - t).num_milliseconds()),
            attempts: 0,
            image_asked: None,
            image: None,
            attempt: None,
            started: false,
        }
    }

    /// This pod's images were asked for (the first time only counts).
    pub fn image_asked(&mut self, at: Instant) {
        self.image_asked.get_or_insert(at);
    }

    /// Every image of the pod resolved, the last at `done`.
    pub fn image_resolved(&mut self, done: Instant) {
        if self.image.is_none() {
            let asked = self.image_asked.unwrap_or(done);
            self.image = Some(done.saturating_duration_since(asked));
        }
    }

    /// A start attempt ended without the pod running.
    pub fn attempt_failed(&mut self) {
        self.attempts += 1;
    }

    /// The attempt that started the pod.
    pub fn started(&mut self, attempt: Attempt) {
        self.attempts += 1;
        self.attempt = Some(attempt);
        self.started = true;
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    /// The breakdown once `Running` was acknowledged at `acked`, its PUT
    /// having taken `report`: the annotation's text and each phase with a
    /// histogram (`scheduled` only when it is not negative).
    pub fn finish(&self, report: Duration, acked: Instant) -> Finished {
        let total = acked.saturating_duration_since(self.seen);
        let mut phases: Vec<(&'static str, Duration)> = Vec::new();
        let mut text: Vec<String> = Vec::new();
        if let Some(ms) = self.scheduled_ms {
            text.push(format!("scheduled={ms}ms"));
            if ms >= 0 {
                phases.push(("scheduled", Duration::from_millis(ms as u64)));
            }
        }
        let mut put = |name: &'static str, d: Duration| {
            text.push(format!("{name}={}", ms(d)));
            phases.push((name, d));
        };
        let a = self.attempt.as_ref();
        put("wait", a.map_or(Duration::ZERO, |a| a.began.saturating_duration_since(self.seen)));
        put("image", self.image.unwrap_or(Duration::ZERO));
        put("volumes", a.map_or(Duration::ZERO, |a| a.volumes));
        put("sandbox", a.map_or(Duration::ZERO, |a| a.sandbox));
        put("init", a.map_or(Duration::ZERO, |a| a.init));
        put("containers", a.map_or(Duration::ZERO, |a| a.containers));
        put("report", report);
        put("total", total);
        text.push(format!("attempts={}", self.attempts.max(1)));
        if let Some(w) = a.and_then(|a| a.workers.as_deref()) {
            text.push(format!("workers={w}"));
        }
        if let Some(p) = a.and_then(|a| a.pending) {
            text.push(format!("pending={p}"));
        }
        if let Some((a, split)) = a.and_then(|a| Some((a, a.sandbox_steps?))) {
            let made = split.made.unwrap_or_default();
            let other = a.sandbox.saturating_sub(made.acquire + made.cni + split.status);
            let steps = [
                ("sandbox/acquire", split.made.map(|m| m.acquire)),
                ("sandbox/cni", split.made.map(|m| m.cni)),
                ("sandbox/status", Some(split.status)),
                ("sandbox/other", Some(other)),
            ];
            for (name, d) in steps {
                if let Some(d) = d {
                    text.push(format!("{name}={}", ms(d)));
                    phases.push((name, d));
                }
            }
        }
        if let Some(a) = a {
            for (name, d) in &a.per_volume {
                text.push(format!("volume/{name}={}", ms(*d)));
            }
            for (name, d) in &a.per_container {
                text.push(format!("container/{name}={}", ms(*d)));
            }
        }
        Finished { text: text.join(" "), phases }
    }
}

/// What a finished start reports.
#[derive(Debug, Clone)]
pub struct Finished {
    /// `scheduled=850ms wait=1.2ms image=0.3ms … total=212ms attempts=1 volume/data=4.1ms container/app=180ms`
    pub text: String,
    pub phases: Vec<(&'static str, Duration)>,
}

/// Milliseconds, with a tenth below ten: a subsecond start is made of
/// steps that round to nothing at whole milliseconds.
fn ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 10.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{}ms", ms.round() as u64)
    }
}

/// When the pod was bound to a node: its `PodScheduled` condition's
/// transition, else its creation (a pod made with `nodeName` set never
/// passes the scheduler).
fn scheduled_at(pod: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    let scheduled = pod["status"]["conditions"].as_array().and_then(|cs| {
        cs.iter()
            .find(|c| c["type"] == "PodScheduled" && c["status"] == "True")
            .and_then(|c| c["lastTransitionTime"].as_str())
    });
    scheduled
        .or_else(|| pod["metadata"]["creationTimestamp"].as_str())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&chrono::Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(t: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&chrono::Utc)
    }

    #[test]
    fn scheduled_from_the_condition_else_creation() {
        let pod = json!({"metadata": {"creationTimestamp": "2026-10-02T10:00:00Z"},
            "status": {"conditions": [{"type": "PodScheduled", "status": "True",
                "lastTransitionTime": "2026-10-02T10:00:01Z"}]}});
        let t = StartTiming::seen_at(&pod, Instant::now(), at("2026-10-02T10:00:01.250Z"));
        assert_eq!(t.scheduled_ms, Some(250));
        let bare = json!({"metadata": {"creationTimestamp": "2026-10-02T10:00:00Z"}});
        let t = StartTiming::seen_at(&bare, Instant::now(), at("2026-10-02T10:00:02Z"));
        assert_eq!(t.scheduled_ms, Some(2000));
        let t = StartTiming::seen_at(&json!({}), Instant::now(), at("2026-10-02T10:00:02Z"));
        assert_eq!(t.scheduled_ms, None);
    }

    #[test]
    fn breakdown_names_every_phase_and_each_volume_and_container() {
        let seen = Instant::now();
        let pod = json!({"metadata": {"creationTimestamp": "2026-10-02T10:00:00Z"}});
        let mut t = StartTiming::seen_at(&pod, seen, at("2026-10-02T10:00:00.040Z"));
        t.image_asked(seen);
        t.image_resolved(seen + Duration::from_millis(3));
        t.attempt_failed();
        let mut a = Attempt::begin();
        a.began = seen + Duration::from_millis(500);
        a.volume("data", Duration::from_micros(4_100));
        a.volumes(Duration::from_millis(5));
        a.sandbox(Duration::from_millis(40));
        a.container("app", Duration::from_millis(180));
        a.container("side", Duration::from_millis(20));
        t.started(a);
        assert!(t.is_started());
        let f = t.finish(Duration::from_millis(12), seen + Duration::from_millis(800));
        assert_eq!(
            f.text,
            "scheduled=40ms wait=500ms image=3.0ms volumes=5.0ms sandbox=40ms init=0.0ms \
             containers=200ms report=12ms total=800ms attempts=2 volume/data=4.1ms \
             container/app=180ms container/side=20ms"
        );
        let names: Vec<_> = f.phases.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, PHASES);
    }

    #[test]
    fn sandbox_is_taken_apart_into_acquire_cni_status_and_the_rest() {
        let seen = Instant::now();
        let mut t = StartTiming::seen_at(&json!({}), seen, at("2026-10-02T10:00:00Z"));
        let mut a = Attempt::begin();
        a.sandbox(Duration::from_millis(621));
        let made = crate::cri::SandboxSteps { acquire: Duration::from_millis(15), cni: Duration::from_millis(590) };
        a.sandbox_steps(Some(made), Duration::from_micros(400));
        t.started(a);
        let f = t.finish(Duration::ZERO, seen);
        assert!(
            f.text.contains(" sandbox=621ms ")
                && f.text.contains(" sandbox/acquire=15ms sandbox/cni=590ms sandbox/status=0.4ms sandbox/other=16ms"),
            "{}",
            f.text
        );
        let names: Vec<_> = f.phases.iter().map(|(n, _)| *n).filter(|n| n.starts_with("sandbox/")).collect();
        assert_eq!(names, SANDBOX_STEPS);

        // A runtime that does not time its steps: status and the rest only.
        let mut t = StartTiming::seen_at(&json!({}), seen, at("2026-10-02T10:00:00Z"));
        let mut a = Attempt::begin();
        a.sandbox(Duration::from_millis(50));
        a.sandbox_steps(None, Duration::from_millis(2));
        t.started(a);
        let f = t.finish(Duration::ZERO, seen);
        assert!(!f.text.contains("sandbox/acquire") && !f.text.contains("sandbox/cni"), "{}", f.text);
        assert!(f.text.contains("sandbox/status=2.0ms sandbox/other=48ms"), "{}", f.text);

        // A sandbox an earlier attempt made: nothing to take apart.
        let mut t = StartTiming::seen_at(&json!({}), seen, at("2026-10-02T10:00:00Z"));
        t.started(Attempt::begin());
        assert!(!t.finish(Duration::ZERO, seen).text.contains("sandbox/"));
    }

    #[test]
    fn a_clock_behind_the_scheduler_is_written_but_not_observed() {
        let pod = json!({"metadata": {"creationTimestamp": "2026-10-02T10:00:01Z"}});
        let seen = Instant::now();
        let mut t = StartTiming::seen_at(&pod, seen, at("2026-10-02T10:00:00Z"));
        t.started(Attempt::begin());
        let f = t.finish(Duration::ZERO, seen);
        assert!(f.text.starts_with("scheduled=-1000ms "), "{}", f.text);
        assert!(!f.phases.iter().any(|(n, _)| *n == "scheduled"));
    }

    #[test]
    fn the_first_image_request_and_resolution_count() {
        let seen = Instant::now();
        let mut t = StartTiming::seen_at(&json!({}), seen, chrono::Utc::now());
        t.image_asked(seen);
        t.image_asked(seen + Duration::from_secs(5));
        t.image_resolved(seen + Duration::from_millis(7));
        t.image_resolved(seen + Duration::from_secs(9));
        assert_eq!(t.image, Some(Duration::from_millis(7)));
    }
}
