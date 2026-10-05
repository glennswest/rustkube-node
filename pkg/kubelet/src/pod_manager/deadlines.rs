//! When a pod must next be looked at with no event to say so (#101).
//!
//! The UID worker used to come back to every live pod every `sync_interval`,
//! whether or not anything could have changed. What can change without an
//! event is short and explicit, and each gets its own deadline here:
//!
//! - a probe, every `periodSeconds` from `initialDelaySeconds` (upstream's
//!   timing; the tick used to run every probe every two seconds);
//! - a CrashLoopBackOff running out;
//! - a start that is waiting (a volume, the network, a failed attempt), retried
//!   with a backoff that grows with the wait;
//! - an init container's deadline.
//!
//! Everything else is an event: a stormpump exit, a volume or image change,
//! an API edit. The worker asks [`PodManager::take_due`] after each pass and
//! sleeps until the earliest deadline, or until an event.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::PodManager;

/// A retry for something transiently wrong (a runtime status call that
/// failed, a container not yet running). Short: it is not a wait for
/// something slow, it is a second look.
pub(super) const RECHECK: Duration = Duration::from_secs(1);

/// The longest a waiting start goes between attempts.
const WAIT_CAP: Duration = Duration::from_secs(10);

/// A pod waiting for a CNI config is woken by the config directory (#148);
/// this is the look it gets anyway, where the directory cannot be watched.
pub(super) const NETWORK_FALLBACK: Duration = WAIT_CAP;

/// The probe kinds, as keys.
pub(super) const STARTUP: &str = "startup";
pub(super) const LIVENESS: &str = "liveness";
pub(super) const READINESS: &str = "readiness";

/// A probe's last run: for which start of the container, and when.
#[derive(Debug, Clone, Copy)]
pub(super) struct ProbeRun {
    started: Option<Instant>,
    at: Instant,
}

/// Probe runs and deadlines, by pod uid.
#[derive(Debug, Default)]
pub(super) struct Deadlines {
    probes: HashMap<(String, String, &'static str), ProbeRun>,
    due: HashMap<String, Instant>,
}

fn period(probe: &Value) -> Duration {
    Duration::from_secs(probe["periodSeconds"].as_u64().unwrap_or(10).max(1))
}

fn initial_delay(probe: &Value) -> Duration {
    Duration::from_secs(probe["initialDelaySeconds"].as_u64().unwrap_or(0))
}

/// How long a start that has waited `waited` waits before the next try: a
/// quarter of the wait so far, between one second and [`WAIT_CAP`]. A pod
/// whose network appears a second later starts a second later; one waiting
/// on a slow volume is not asked about every second for ten minutes.
pub(super) fn wait_backoff(waited: Duration) -> Duration {
    (waited / 4).clamp(RECHECK, WAIT_CAP)
}

impl Deadlines {
    fn due_at(&mut self, uid: &str, at: Instant) {
        self.due
            .entry(uid.to_string())
            .and_modify(|d| *d = (*d).min(at))
            .or_insert(at);
    }

    /// Is `kind`'s probe on `container` due now? When it is not, its due time
    /// is recorded as the pod's deadline. `started` is when this start of
    /// the container began (`None` for an adopted one): a restart is a new
    /// start, and its probes begin again from their initial delay.
    fn probe_due(&mut self, uid: &str, container: &str, kind: &'static str, probe: &Value, started: Option<Instant>) -> bool {
        let now = Instant::now();
        if let Some(start) = started {
            let first = start + initial_delay(probe);
            if now < first {
                self.due_at(uid, first);
                return false;
            }
        }
        let key = (uid.to_string(), container.to_string(), kind);
        match self.probes.get(&key) {
            Some(run) if run.started == started && now < run.at + period(probe) => {
                let next = run.at + period(probe);
                self.due_at(uid, next);
                false
            }
            _ => true,
        }
    }

    /// The probe ran now; it is next due a period from now.
    fn probed(&mut self, uid: &str, container: &str, kind: &'static str, probe: &Value, started: Option<Instant>) {
        let now = Instant::now();
        self.probes.insert(
            (uid.to_string(), container.to_string(), kind),
            ProbeRun { started, at: now },
        );
        self.due_at(uid, now + period(probe));
    }

    fn take(&mut self, uid: &str) -> Option<Duration> {
        self.due
            .remove(uid)
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    fn forget(&mut self, uid: &str) {
        self.due.remove(uid);
        self.probes.retain(|(u, _, _), _| u != uid);
    }
}

impl PodManager {
    fn deadlines(&self) -> std::sync::MutexGuard<'_, Deadlines> {
        self.deadlines.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Look at `uid` again by `at` at the latest.
    pub(super) fn due_at(&self, uid: &str, at: Instant) {
        self.deadlines().due_at(uid, at);
    }

    pub(super) fn due_in(&self, uid: &str, after: Duration) {
        self.due_at(uid, Instant::now() + after);
    }

    pub(super) fn probe_due(&self, uid: &str, container: &str, kind: &'static str, probe: &Value, started: Option<Instant>) -> bool {
        self.deadlines().probe_due(uid, container, kind, probe, started)
    }

    pub(super) fn probed(&self, uid: &str, container: &str, kind: &'static str, probe: &Value, started: Option<Instant>) {
        self.deadlines().probed(uid, container, kind, probe, started)
    }

    /// How long until this pod must be looked at again with no event, and
    /// forget it: each pass sets the deadlines it still needs. `None`: only
    /// an event.
    pub fn take_due(&self, uid: &str) -> Option<Duration> {
        self.deadlines().take(uid)
    }

    pub(super) fn forget_deadlines(&self, uid: &str) {
        self.deadlines().forget(uid);
    }

    /// Tests: a period has passed for every probe.
    #[cfg(test)]
    pub(super) fn expire_probes(&self) {
        self.deadlines().probes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_probe_runs_once_a_period_from_its_initial_delay() {
        let mut d = Deadlines::default();
        let probe = json!({"periodSeconds": 5, "initialDelaySeconds": 3});
        let started = Some(Instant::now());
        assert!(!d.probe_due("u", "c", LIVENESS, &probe, started), "inside the initial delay");
        let due = d.take("u").unwrap();
        assert!(due <= Duration::from_secs(3) && due > Duration::from_secs(2), "{due:?}");

        let adopted = None;
        assert!(d.probe_due("u", "c", LIVENESS, &probe, adopted), "no delay known: due");
        d.probed("u", "c", LIVENESS, &probe, adopted);
        assert!(!d.probe_due("u", "c", LIVENESS, &probe, adopted), "ran just now");
        let due = d.take("u").unwrap();
        assert!(due <= Duration::from_secs(5) && due > Duration::from_secs(4), "{due:?}");

        // A restart is a new start: the old run does not count for it.
        let later = Instant::now().checked_sub(Duration::from_secs(60));
        assert!(d.probe_due("u", "c", LIVENESS, &probe, later));
        // Kinds and containers are separate.
        assert!(d.probe_due("u", "c", READINESS, &probe, adopted));
        assert!(d.probe_due("u", "other", LIVENESS, &probe, adopted));
    }

    #[test]
    fn the_earliest_deadline_wins_and_is_taken_once() {
        let mut d = Deadlines::default();
        let now = Instant::now();
        d.due_at("u", now + Duration::from_secs(9));
        d.due_at("u", now + Duration::from_secs(2));
        d.due_at("u", now + Duration::from_secs(5));
        assert!(d.take("u").unwrap() <= Duration::from_secs(2));
        assert_eq!(d.take("u"), None, "a pass sets what it still needs");
        d.probed("u", "c", STARTUP, &json!({}), None);
        d.forget("u");
        assert_eq!(d.take("u"), None);
        assert!(d.probes.is_empty());
    }

    #[test]
    fn a_waiting_start_backs_off_with_the_wait() {
        assert_eq!(wait_backoff(Duration::ZERO), RECHECK);
        assert_eq!(wait_backoff(Duration::from_secs(20)), Duration::from_secs(5));
        assert_eq!(wait_backoff(Duration::from_secs(600)), WAIT_CAP);
    }
}
