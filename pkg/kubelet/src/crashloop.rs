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
//!
//! **Kept across a kubelet restart** (#112): with a file
//! ([`CrashLoopBackoff::persist_to`]), every change is written there in wall-
//! clock seconds, and a restarted kubelet reads it back, so a crash-looping
//! container it adopts keeps its delay instead of restarting at once. Entries
//! past the stable window are forgiven anyway and are not kept; a recreated
//! Pod has a new uid, so nothing transfers to it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// Where the entries are kept across a kubelet restart (#112).
    file: Mutex<Option<PathBuf>>,
}

/// One entry as written: wall-clock seconds, which survive the process.
#[derive(serde::Serialize, serde::Deserialize)]
struct Saved {
    delay_secs: u64,
    ready_at: u64,
    restarted_at: u64,
}

fn unix(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// The wall-clock time an `Instant` was, now.
fn wall(i: Instant, now: Instant, now_wall: u64) -> u64 {
    if i >= now {
        now_wall + (i - now).as_secs()
    } else {
        now_wall.saturating_sub((now - i).as_secs())
    }
}

/// The `Instant` a wall-clock time is, now.
fn instant(w: u64, now: Instant, now_wall: u64) -> Instant {
    if w >= now_wall {
        now + Duration::from_secs(w - now_wall)
    } else {
        now.checked_sub(Duration::from_secs(now_wall - w)).unwrap_or(now)
    }
}

impl CrashLoopBackoff {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep the entries in `path` from now on, and take back what an earlier
    /// kubelet left there (#112). Entries past the stable window are
    /// forgiven, so dropped. An unreadable file is a fresh start.
    pub fn persist_to(&self, path: PathBuf) {
        let now = Instant::now();
        let now_wall = unix(SystemTime::now());
        let saved: HashMap<String, Saved> = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let mut restored = 0;
        if let Ok(mut entries) = self.entries.lock() {
            for (k, s) in saved {
                if now_wall.saturating_sub(s.restarted_at) >= STABLE.as_secs() {
                    continue;
                }
                entries.entry(k).or_insert_with(|| {
                    restored += 1;
                    Entry {
                        delay: Duration::from_secs(s.delay_secs),
                        ready_at: instant(s.ready_at, now, now_wall),
                        restarted_at: instant(s.restarted_at, now, now_wall),
                    }
                });
            }
        }
        if restored > 0 {
            tracing::info!(restored, "crash-loop backoff carried over from the last kubelet");
        }
        if let Ok(mut f) = self.file.lock() {
            *f = Some(path);
        }
        self.save();
    }

    /// Write the entries, when there is a file (tmp + rename). A failure is
    /// logged, never fatal: the backoff still works, only in memory.
    fn save(&self) {
        let Some(path) = self.file.lock().ok().and_then(|f| f.clone()) else { return };
        let now = Instant::now();
        let now_wall = unix(SystemTime::now());
        let saved: HashMap<String, Saved> = match self.entries.lock() {
            Ok(entries) => entries
                .iter()
                .map(|(k, e)| {
                    (k.clone(), Saved {
                        delay_secs: e.delay.as_secs(),
                        ready_at: wall(e.ready_at, now, now_wall),
                        restarted_at: wall(e.restarted_at, now, now_wall),
                    })
                })
                .collect(),
            Err(_) => return,
        };
        let tmp = path.with_extension("json.tmp");
        let written = serde_json::to_vec(&saved)
            .map_err(std::io::Error::other)
            .and_then(|b| std::fs::write(&tmp, b))
            .and_then(|()| std::fs::rename(&tmp, &path));
        if let Err(e) = written {
            tracing::debug!(path = %path.display(), "crash-loop backoff not saved: {e}");
        }
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
            // Up long enough since the last restart: forgiven, here as well
            // as in `running`. Nothing looks at a healthy container on a
            // clock any more (#101), so `running` may never have been told.
            Some(e) if e.restarted_at.elapsed() >= STABLE => BASE,
            // Doubling from the last wait, capped. `min` rather than a
            // saturating multiply: the cap is the point, not overflow.
            Some(e) => (e.delay * 2).min(CAP),
            None => BASE,
        };
        entries.insert(key.to_string(), Entry { delay, ready_at: now + delay, restarted_at: now });
        drop(entries);
        self.save();
    }

    /// The container is up. Forget its backoff once it has been up long
    /// enough to count as recovered.
    pub fn running(&self, key: &str) {
        let Ok(mut entries) = self.entries.lock() else { return };
        if entries.get(key).is_some_and(|e| e.restarted_at.elapsed() >= STABLE) {
            entries.remove(key);
            drop(entries);
            self.save();
        }
    }

    /// Drop every container of a pod that is gone, so a node that churns pods
    /// does not accumulate an entry per container forever.
    pub fn forget_pod(&self, pod_uid: &str) {
        let Ok(mut entries) = self.entries.lock() else { return };
        let prefix = format!("{pod_uid}/");
        let before = entries.len();
        entries.retain(|k, _| !k.starts_with(&prefix));
        let changed = entries.len() != before;
        drop(entries);
        if changed {
            self.save();
        }
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

    /// #112: a restarted kubelet keeps a crash-looping container's delay, a
    /// recreated Pod (new uid) does not inherit it, and a forgiven entry is
    /// not carried over.
    #[test]
    fn the_backoff_survives_a_kubelet_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("crashloop.json");
        let k = CrashLoopBackoff::key("u-1", "app");

        let first = CrashLoopBackoff::new();
        first.persist_to(path.clone());
        first.restarted(&k);
        first.restarted(&k); // 20 s
        let waiting = first.wait(&k).unwrap();
        assert!(waiting > Duration::from_secs(18), "{waiting:?}");

        // A new kubelet, same file.
        let second = CrashLoopBackoff::new();
        second.persist_to(path.clone());
        let carried = second.wait(&k).expect("still backing off");
        assert!(carried <= waiting + Duration::from_secs(1) && carried > Duration::from_secs(15), "{carried:?}");
        // The doubling continues from where it was.
        second.restarted(&k);
        assert!(second.wait(&k).unwrap() > Duration::from_secs(35), "40 s next");
        // Another Pod of the same name has a new uid: nothing to inherit.
        assert!(second.wait(&CrashLoopBackoff::key("u-2", "app")).is_none());
        // Gone with its pod, on disk too.
        second.forget_pod("u-1");
        let third = CrashLoopBackoff::new();
        third.persist_to(path.clone());
        assert!(third.wait(&k).is_none());

        // An entry past the stable window is forgiven, not carried.
        let old = unix(SystemTime::now()) - STABLE.as_secs() - 5;
        std::fs::write(&path, serde_json::json!({"u-9/app": {"delay_secs": 300, "ready_at": old + 300, "restarted_at": old}}).to_string()).unwrap();
        let fourth = CrashLoopBackoff::new();
        fourth.persist_to(path.clone());
        assert!(fourth.wait("u-9/app").is_none());
        // An unreadable file is a fresh start.
        std::fs::write(&path, "garbage").unwrap();
        CrashLoopBackoff::new().persist_to(path);
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

    /// Forgiven at the next restart even if nobody saw it running (#101):
    /// no clock looks at a healthy container any more.
    #[test]
    fn a_restart_after_a_stable_run_starts_from_the_base() {
        let b = CrashLoopBackoff::new();
        let k = CrashLoopBackoff::key("u-1", "app");
        b.restarted(&k);
        b.restarted(&k);
        b.restarted(&k);
        let Some(long_ago) = Instant::now().checked_sub(STABLE) else { return };
        b.entries.lock().unwrap().get_mut(&k).unwrap().restarted_at = long_ago;
        b.restarted(&k);
        let left = b.wait(&k).unwrap();
        assert!(left <= BASE && left > BASE / 2, "{left:?}");
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
