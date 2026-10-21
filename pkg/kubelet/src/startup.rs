//! The kubelet's own startup, timed (#262).
//!
//! Since the control plane is static pods (stormcos#559), the time from the
//! kubelet's start to their containers is on every node's boot path. Each
//! step of `main` is recorded here; the summary is one INFO line when the
//! kubelet begins starting pods, and the first static pod's start is said
//! relative to the process's.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

struct Clock {
    start: Instant,
    last: Mutex<(Instant, Vec<(&'static str, Duration)>)>,
}

static CLOCK: OnceLock<Clock> = OnceLock::new();
static FIRST_STATIC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn clock() -> &'static Clock {
    CLOCK.get_or_init(|| {
        let now = Instant::now();
        Clock { start: now, last: Mutex::new((now, Vec::new())) }
    })
}

/// Start the clock: the first thing `main` does.
pub fn begin() {
    let _ = clock();
}

/// `step` has just finished: its time is since the previous one.
pub fn mark(step: &'static str) {
    let c = clock();
    let mut last = c.last.lock().unwrap_or_else(|e| e.into_inner());
    let now = Instant::now();
    let took = now - last.0;
    last.0 = now;
    last.1.push((step, took));
}

/// Since the process began.
pub fn elapsed() -> Duration {
    clock().start.elapsed()
}

fn ms(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

/// `total=…ms step=…ms …`, every step so far.
pub fn summary() -> String {
    let c = clock();
    let last = c.last.lock().unwrap_or_else(|e| e.into_inner());
    let mut text = vec![format!("total={}", ms(c.start.elapsed()))];
    text.extend(last.1.iter().map(|(s, d)| format!("{s}={}", ms(*d))));
    text.join(" ")
}

/// A static pod has started: the first one is said, once, with how long
/// after the kubelet's own start (#262: a regression shows in one line).
pub fn static_pod_started(name: &str) -> Option<Duration> {
    if FIRST_STATIC.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return None;
    }
    let since = elapsed();
    tracing::info!(
        "first static pod {name} running {} after the kubelet started ({})",
        ms(since),
        summary()
    );
    Some(since)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_are_timed_one_after_another_and_the_first_static_pod_is_said_once() {
        begin();
        mark("a");
        std::thread::sleep(Duration::from_millis(20));
        mark("b");
        let s = summary();
        assert!(s.starts_with("total=") && s.contains(" a=") && s.contains(" b="), "{s}");
        let b: u128 = s.split(" b=").nth(1).unwrap().trim_end_matches("ms").split(' ').next().unwrap().trim_end_matches("ms").parse().unwrap();
        assert!(b >= 20, "{s}");
        assert!(static_pod_started("kube-system/etcd").is_some());
        assert!(static_pod_started("kube-system/kube-apiserver").is_none(), "once");
    }
}
