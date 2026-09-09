//! CrashLoopBackOff — the restart backoff for a container that keeps dying.
//!
//! Without one, a container that exits is recreated on every sync tick. At a
//! two-second sync interval that is a crash loop running at thirty restarts a
//! minute: it burns the node's CPU on container creation, fills the runtime
//! with dead records, and floods the log so the *first* failure — the one that
//! says why — scrolls away. The same root produced the 889-pod cilium-operator
//! runaway (rustkube-node#25).
//!
//! Kubernetes' shape, followed here: the first restart is immediate, and each
//! one after it waits twice as long as the last, from ten seconds to a five
//! minute cap. A container that has stayed up for ten minutes is no longer
//! considered to be looping and starts again from ten seconds.
//!
//! Keyed by `<pod uid>/<container name>` rather than by container id, because
//! the id changes on every restart and the thing being backed off is the
//! container's *identity*, which does not.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The first wait, and the factor between waits.
const BASE: Duration = Duration::from_secs(10);
/// The longest wait. Upstream's cap, and for the same reason: past five
/// minutes a human is going to look at it anyway, and a longer wait only
/// delays the recovery that a fixed image would bring.
const CAP: Duration = Duration::from_secs(300);
/// How long a container has to stay up before it is forgiven. Shorter than
/// this and a container that crashes every minute would reset its backoff
/// every time and never back off at all.
const STABLE: Duration = Duration::from_secs(600);

#[derive(Debug)]
struct Entry {
    /// The wait that was applied to the restart just made.
    delay: Duration,
    /// When the next restart is allowed.
    ready_at: Instant,
    /// When the last restart happened, to judge whether it has held.
    restarted_at: Instant,
}

/// Per-container restart backoff.
#[derive(Debug, Default)]
pub struct CrashLoopBackoff {
    entries: Mutex<HashMap<String, Entry>>,
}

impl CrashLoopBackoff {
    pub fn new() -> Self {
        Self::default()
    }

    /// The key for one container of one pod.
    pub fn key(pod_uid: &str, container: &str) -> String {
        format!("{pod_uid}/{container}")
    }

    /// How long this container still has to wait, or `None` if it may restart
    /// now.
    ///
    /// `None` for a container with no history is deliberate: the first restart
    /// is immediate, because most container exits are not a crash loop and
    /// making every one of them wait ten seconds would slow every ordinary
    /// restart to punish the rare pathological one.
    pub fn wait(&self, key: &str) -> Option<Duration> {
        let entries = self.entries.lock().ok()?;
        let e = entries.get(key)?;
        let now = Instant::now();
        (e.ready_at > now).then(|| e.ready_at - now)
    }

    /// Record a restart, and set what the next one will have to wait.
    pub fn restarted(&self, key: &str) {
        let Ok(mut entries) = self.entries.lock() else { return };
        let now = Instant::now();
        let delay = match entries.get(key) {
            // Doubling from the last wait, capped. `min` rather than a
            // saturating multiply: the cap is the point, not overflow.
            Some(e) => (e.delay * 2).min(CAP),
            None => BASE,
        };
        entries.insert(key.to_string(), Entry { delay, ready_at: now + delay, restarted_at: now });
    }

    /// The container is up. Forget its backoff once it has been up long
    /// enough to count as recovered.
    pub fn running(&self, key: &str) {
        let Ok(mut entries) = self.entries.lock() else { return };
        if entries.get(key).is_some_and(|e| e.restarted_at.elapsed() >= STABLE) {
            entries.remove(key);
        }
    }

    /// Drop every container of a pod that is gone, so a node that churns pods
    /// does not accumulate an entry per container forever.
    pub fn forget_pod(&self, pod_uid: &str) {
        let Ok(mut entries) = self.entries.lock() else { return };
        let prefix = format!("{pod_uid}/");
        entries.retain(|k, _| !k.starts_with(&prefix));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_restart_is_immediate_and_the_rest_back_off() {
        let b = CrashLoopBackoff::new();
        let k = CrashLoopBackoff::key("u-1", "app");

        // Nothing known: restart now. Most exits are not a loop.
        assert!(b.wait(&k).is_none());

        b.restarted(&k);
        let first = b.wait(&k).expect("a second crash must wait");
        assert!(first <= BASE && first > BASE / 2, "{first:?}");

        b.restarted(&k);
        let second = b.wait(&k).expect("still waiting");
        assert!(second > BASE, "the wait must grow: {second:?}");
    }

    #[test]
    fn the_wait_is_capped() {
        let b = CrashLoopBackoff::new();
        let k = CrashLoopBackoff::key("u-1", "app");
        // Ten doublings from 10s would be 10240s without a cap.
        for _ in 0..10 {
            b.restarted(&k);
        }
        let left = b.wait(&k).expect("waiting");
        assert!(left <= CAP, "{left:?} exceeds the cap");
        assert!(left > CAP / 2, "{left:?} should be at the cap");
    }

    #[test]
    fn a_container_that_has_not_been_up_long_keeps_its_backoff() {
        // The reset is what makes a container that crashes every minute back
        // off at all — resetting on any sighting of a running container would
        // hand it a fresh ten seconds each time round.
        let b = CrashLoopBackoff::new();
        let k = CrashLoopBackoff::key("u-1", "app");
        b.restarted(&k);
        b.restarted(&k);
        let before = b.wait(&k).expect("waiting");
        b.running(&k);
        let after = b.wait(&k).expect("still waiting: it has only just restarted");
        assert!(after <= before);
    }

    #[test]
    fn a_pod_that_is_gone_takes_its_entries_with_it() {
        let b = CrashLoopBackoff::new();
        let a = CrashLoopBackoff::key("u-1", "app");
        let side = CrashLoopBackoff::key("u-1", "sidecar");
        let other = CrashLoopBackoff::key("u-2", "app");
        for k in [&a, &side, &other] {
            b.restarted(k);
        }
        b.forget_pod("u-1");
        assert!(b.wait(&a).is_none());
        assert!(b.wait(&side).is_none());
        assert!(b.wait(&other).is_some(), "another pod's backoff is not this pod's business");
    }

    #[test]
    fn the_key_is_the_containers_identity_not_its_id() {
        // A restart gives the container a new id; the backoff has to survive
        // that or it would reset on every restart and never grow.
        assert_eq!(CrashLoopBackoff::key("u-1", "app"), CrashLoopBackoff::key("u-1", "app"));
        assert_ne!(CrashLoopBackoff::key("u-1", "app"), CrashLoopBackoff::key("u-2", "app"));
    }
}
