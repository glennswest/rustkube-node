//! The kubelet's own startup, timed (#262).
//!
//! Since the control plane is static pods (stormcos#559), the time from the
//! kubelet's start to their containers is on every node's boot path. Each
//! step of `main` is recorded here; the summary is one INFO line when the
//! kubelet begins starting pods, and the first static pod's start is said
//! relative to the process's.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// What the process had used at a point: CPU (user + system), major page
/// faults, and voluntary (it blocked) and involuntary (it was preempted)
/// context switches, all threads together (`getrusage(RUSAGE_SELF)`).
#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub struct Usage {
    pub cpu: Duration,
    pub majflt: u64,
    pub waits: u64,
    pub preempted: u64,
}

impl Usage {
    #[cfg(target_os = "linux")]
    fn now() -> Usage {
        // SAFETY: getrusage fills the struct it is given.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) } != 0 {
            return Usage::default();
        }
        let tv = |t: libc::timeval| Duration::new(t.tv_sec.max(0) as u64, (t.tv_usec.max(0) as u32) * 1000);
        Usage {
            cpu: tv(ru.ru_utime) + tv(ru.ru_stime),
            majflt: ru.ru_majflt.max(0) as u64,
            waits: ru.ru_nvcsw.max(0) as u64,
            preempted: ru.ru_nivcsw.max(0) as u64,
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn now() -> Usage {
        Usage::default()
    }

    fn since(self, then: Usage) -> Usage {
        Usage {
            cpu: self.cpu.saturating_sub(then.cpu),
            majflt: self.majflt.saturating_sub(then.majflt),
            waits: self.waits.saturating_sub(then.waits),
            preempted: self.preempted.saturating_sub(then.preempted),
        }
    }
}

/// A step's time, and with it what the process did in it: a step whose wall
/// time is far above its CPU time waited, and the counts say on what (#262:
/// ~1.5 s per boot in steps that do almost no work).
#[derive(Clone, Copy, Debug)]
struct Step {
    name: &'static str,
    took: Duration,
    usage: Usage,
}

/// Steps under this are said by their time alone.
const DETAIL_FROM: Duration = Duration::from_millis(50);

struct Clock {
    start: Instant,
    /// Process start → [`begin`]: exec, loading and the runtime's start.
    exec: Option<Duration>,
    last: Mutex<(Instant, Usage, Vec<Step>)>,
}

static CLOCK: OnceLock<Clock> = OnceLock::new();
static FIRST_STATIC: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn clock() -> &'static Clock {
    CLOCK.get_or_init(|| {
        let now = Instant::now();
        Clock { start: now, exec: since_exec(), last: Mutex::new((now, Usage::now(), Vec::new())) }
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
    let (now, usage) = (Instant::now(), Usage::now());
    let took = now - last.0;
    let used = usage.since(last.1);
    last.0 = now;
    last.1 = usage;
    last.2.push(Step { name: step, took, usage: used });
}

/// How long this process ran before [`begin`]: its start time
/// (`/proc/self/stat`, clock ticks since boot) against `CLOCK_BOOTTIME`.
#[cfg(target_os = "linux")]
fn since_exec() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let ticks = start_ticks(&stat)?;
    // SAFETY: sysconf takes a constant; clock_gettime fills the timespec.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    if hz <= 0 || unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) } != 0 {
        return None;
    }
    let started = Duration::from_millis(ticks.saturating_mul(1000) / hz as u64);
    let now = Duration::new(ts.tv_sec.max(0) as u64, ts.tv_nsec.max(0) as u32);
    now.checked_sub(started)
}

#[cfg(not(target_os = "linux"))]
fn since_exec() -> Option<Duration> {
    None
}

/// `starttime` (field 22) of a `/proc/<pid>/stat` line. The command (field
/// 2) may hold spaces and parentheses, so fields are counted after its last `)`.
fn start_ticks(stat: &str) -> Option<u64> {
    let rest = &stat[stat.rfind(')')? + 1..];
    rest.split_whitespace().nth(19)?.parse().ok()
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
    if let Some(e) = c.exec {
        text.push(format!("exec={}", ms(e)));
    }
    text.extend(last.2.iter().map(step_text));
    text.join(" ")
}

/// `name=Nms`, and for a step of [`DETAIL_FROM`] or more what it did:
/// `name=1577ms(cpu 12ms, 340 faults, 3 waits, 1 preempted)`.
fn step_text(s: &Step) -> String {
    if s.took < DETAIL_FROM {
        return format!("{}={}", s.name, ms(s.took));
    }
    let u = s.usage;
    format!(
        "{}={}(cpu {}, {} faults, {} waits, {} preempted)",
        s.name,
        ms(s.took),
        ms(u.cpu),
        u.majflt,
        u.waits,
        u.preempted
    )
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
        let b: u128 = s.split(" b=").nth(1).unwrap().chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap();
        assert!(b >= 20, "{s}");
        // A step past DETAIL_FROM says what it did: a sleep is waiting, not CPU.
        assert!(s.contains(" b=") && s.split(" b=").nth(1).unwrap().contains("(cpu "), "{s}");
        assert!(static_pod_started("kube-system/etcd").is_some());
        assert!(static_pod_started("kube-system/kube-apiserver").is_none(), "once");
    }

    #[test]
    fn a_short_step_is_its_time_alone_and_a_long_one_says_what_it_did() {
        let quick = Step { name: "cni", took: Duration::from_millis(6), usage: Usage::default() };
        assert_eq!(step_text(&quick), "cni=6ms");
        let slow = Step {
            name: "flags",
            took: Duration::from_millis(1577),
            usage: Usage { cpu: Duration::from_millis(12), majflt: 340, waits: 3, preempted: 1 },
        };
        assert_eq!(step_text(&slow), "flags=1577ms(cpu 12ms, 340 faults, 3 waits, 1 preempted)");
        let later = Usage { cpu: Duration::from_millis(20), majflt: 5, waits: 9, preempted: 2 };
        let earlier = Usage { cpu: Duration::from_millis(5), majflt: 1, waits: 4, preempted: 2 };
        assert_eq!(later.since(earlier), Usage { cpu: Duration::from_millis(15), majflt: 4, waits: 5, preempted: 0 });
    }

    #[test]
    fn the_start_time_is_read_after_the_command() {
        // A command with a space and a parenthesis in it (field 2).
        let stat = "4321 (kube let) x) S 1 4321 4321 0 -1 4194560 120 0 0 0 3 1 0 0 20 0 9 0 98765 123456 789";
        assert_eq!(start_ticks(stat), Some(98765));
        assert_eq!(start_ticks("garbage"), None);
        #[cfg(target_os = "linux")]
        assert!(since_exec().is_some(), "this process has a start time");
    }
}
