//! Where a VMI's start goes, step by step (#259).
//!
//! The pod path has had this since #132 ([`crate::start_timing`]); a VMI's
//! start had none, so a VM that read `Running` six seconds late on pvetest1
//! (12.19 on, the install probe's second VM) could not say where the six
//! seconds went. The same annotation, the same `name=<ms>` text.
//!
//! | step       | from → to |
//! |------------|-----------|
//! | `created`  | the VMI's `creationTimestamp` → first seen here; wall clocks of two machines, whole seconds when the apiserver writes them so |
//! | `wait`     | seen → the start attempt that started it (waits and retries) |
//! | `unwind`   | an earlier failed start's taps withdrawn (#100) |
//! | `lock`     | waiting for the disk lock: the orphan sweep holds it (#75) |
//! | `seed`     | a referenced cloud-init Secret read |
//! | `keys`     | `accessCredentials` Secrets read (#92) |
//! | `owner`    | its VirtualMachine read (the disks' owner, #75) |
//! | `disks`    | every disk found, cloned or made, and attached (each also as `disk/<name>`) |
//! | `network`  | the pod network: sandbox, CNI ADD (#88); nothing for a VM on a node bridge |
//! | `resume`   | a restore's saved state prepared (#251) |
//! | `window`   | waiting for the deposit window (another start's taps, #100) |
//! | `launch`   | taps, plan and the engine's spawn |
//! | `record`   | identity published, the registration rewritten with the handle |
//! | `report`   | the `Running` status patch sent → answered |
//! | `total`    | seen → `Running` answered |
//!
//! `attempts=N` counts the start attempts (a `Waiting` one included). The
//! steps follow one another, so their sum is `total` less `created` and the
//! gaps between them.

use std::time::{Duration, Instant};

use serde_json::Value;

/// The annotation the breakdown is written to: the pod path's.
pub const ANNOTATION: &str = crate::start_timing::ANNOTATION;
/// The Event reason it is recorded under.
pub const REASON: &str = crate::start_timing::REASON;

/// One VMI's start, from first seen to the attempt that started it.
#[derive(Debug, Clone)]
pub struct Seen {
    pub at: Instant,
    /// Microseconds from the VMI's creation to `at`, by wall clock.
    created_us: Option<i64>,
    pub attempts: u32,
}

impl Seen {
    pub fn new(vmi: &Value) -> Self {
        Self::at(vmi, Instant::now(), chrono::Utc::now())
    }

    fn at(vmi: &Value, at: Instant, now: chrono::DateTime<chrono::Utc>) -> Self {
        let created = vmi["metadata"]["creationTimestamp"]
            .as_str()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&chrono::Utc));
        Self { at, created_us: created.and_then(|c| (now - c).num_microseconds()), attempts: 0 }
    }
}

/// The steps of one start attempt, in the order they ran.
#[derive(Debug, Clone)]
pub struct Steps {
    pub began: Instant,
    steps: Vec<(&'static str, Duration)>,
    disks: Vec<(String, Duration)>,
}

impl Steps {
    pub fn begin() -> Self {
        Self { began: Instant::now(), steps: Vec::new(), disks: Vec::new() }
    }

    /// `name` took `took`; a step met twice is added up.
    pub fn put(&mut self, name: &'static str, took: Duration) {
        match self.steps.iter_mut().find(|(n, _)| *n == name) {
            Some((_, d)) => *d += took,
            None => self.steps.push((name, took)),
        }
    }

    /// Each disk's part of `disks`, filled by the resolver as it goes.
    pub fn disks_mut(&mut self) -> &mut Vec<(String, Duration)> {
        &mut self.disks
    }

    /// `name` for how long since `from`, and now as the next step's start.
    pub fn since(&mut self, name: &'static str, from: Instant) -> Instant {
        let now = Instant::now();
        self.put(name, now.saturating_duration_since(from));
        now
    }

    /// The breakdown, once `Running` was answered at `acked`.
    pub fn finish(&self, seen: Option<&Seen>, acked: Instant) -> String {
        let mut text: Vec<String> = Vec::new();
        if let Some(us) = seen.and_then(|s| s.created_us) {
            let d = Duration::from_micros(us.unsigned_abs());
            text.push(format!("created={}{}", if us < 0 { "-" } else { "" }, ms(d)));
        }
        let from = seen.map_or(self.began, |s| s.at);
        text.push(format!("wait={}", ms(self.began.saturating_duration_since(from))));
        for (name, d) in &self.steps {
            text.push(format!("{name}={}", ms(*d)));
        }
        text.push(format!("total={}", ms(acked.saturating_duration_since(from))));
        text.push(format!("attempts={}", seen.map_or(1, |s| s.attempts.max(1))));
        for (name, d) in &self.disks {
            text.push(format!("disk/{name}={}", ms(*d)));
        }
        text.join(" ")
    }
}

/// Milliseconds, a tenth below ten (the pod path's rule).
fn ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 10.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{}ms", ms.round() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_vmi_start_is_written_step_by_step_with_its_disks_and_attempts() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-14T18:52:05.674Z").unwrap().with_timezone(&chrono::Utc);
        let vmi = json!({"metadata": {"creationTimestamp": "2026-10-14T18:52:05.174Z"}});
        let seen_at = Instant::now();
        let mut seen = Seen::at(&vmi, seen_at, now);
        seen.attempts = 2;
        let mut s = Steps { began: seen_at + Duration::from_millis(1000), steps: Vec::new(), disks: Vec::new() };
        s.put("lock", Duration::from_millis(6300));
        s.put("disks", Duration::from_micros(4200));
        s.put("disks", Duration::from_micros(800));
        s.disks_mut().push(("root".into(), Duration::from_millis(5)));
        s.put("report", Duration::from_millis(240));
        let text = s.finish(Some(&seen), seen_at + Duration::from_millis(7600));
        assert_eq!(
            text,
            "created=500ms wait=1000ms lock=6300ms disks=5.0ms report=240ms total=7600ms attempts=2 disk/root=5.0ms"
        );
        // A VMI with no creation time, first attempt: no `created`, attempts 1.
        let bare = Seen::at(&json!({}), seen_at, now);
        assert!(Steps { began: seen_at, steps: Vec::new(), disks: Vec::new() }
            .finish(Some(&bare), seen_at)
            .starts_with("wait=0.0ms total=0.0ms attempts=1"));
        // Clocks that disagree say so rather than hiding it.
        let ahead = Seen::at(&json!({"metadata": {"creationTimestamp": "2026-10-14T18:52:06.174Z"}}), seen_at, now);
        assert!(s.finish(Some(&ahead), seen_at).starts_with("created=-500ms "));
    }
}
