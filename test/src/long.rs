//! `long` (the night window, #61): waves of pods at the node's capacity,
//! measuring how fast each wave starts and what it leaves behind
//! (stormcentral `docs/test-standard.md`, "Overnight soaks: waves").
//!
//! Each wave:
//!
//! 1. **Ramp.** Its size is 80% of the pod slots the node has free
//!    (`status.allocatable.pods` less the pods already on it), read from the
//!    API at the start of the run, never assumed; at most
//!    `RUSTKUBE_NODE_TEST_WAVE_MAX` (default 500). Every pod is pinned to the
//!    node and holds (`sleep`). Every fourth also gets a 16Mi claim of the
//!    built-in class, written by an init container, when the class is here.
//!    The pods go in steps of ten, each step waited on, and the ramp stops
//!    adding pods when the node's `MemAvailable` (its `/proc/meminfo`: the
//!    runner is on the node, and the file is not namespaced) is under a floor,
//!    20% of `MemTotal` and at least 1 GiB (`RUSTKUBE_NODE_TEST_MEM_FLOOR_PCT`).
//!    A wave of BestEffort pods at 80% of 110 slots ran a 16 GiB node out of
//!    memory and took its apiserver with it (ef37856d6b): the slots are not
//!    the node's capacity, its memory is. Later waves take the first wave's
//!    size, so they compare. When the Node's `capacity.memory` is not this
//!    `MemTotal` (the runner on another node), the floor is not used.
//! 2. **Measure** each pod's start: from its create to the first time the API
//!    shows it Running (polled every second, so the resolution is a second),
//!    or the pod's own `startedAt - creationTimestamp` plus a second, when
//!    that is less. Both upper-bound the start; the pod's own does not depend
//!    on the runner watching. The runner's client waits 30 s on a request, so
//!    one hung call made every pod of its step read 30 s late (7dae7cfca6,
//!    0e99f2d197: one wave each in a night, p95 ~31 s). Each wave also
//!    records its slowest API call and its failed ones, and a slow wave names
//!    its slowest pods.
//! 3. **Hold** 30 s, and read one pod's log back.
//! 4. **Drain**: delete every pod and claim of the wave, and check they are
//!    gone, and the claims' PVs reclaimed.
//!
//! Waves repeat until the suite's time is nearly out (or
//! `RUSTKUBE_NODE_TEST_WAVES` waves, for a hand run). Each wave is one result
//! line, `wave-<n>`, with its numbers as fields. A wave fails when anything
//! it made is left after its drain, or when its p95 start is more than twice
//! the first wave's plus 2 s: a slowdown is a failure even when every pod
//! started. VMs are stormcos_qa's `vm-waves`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{percentile, s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

/// A wave stops being started when less than this, plus three times the last
/// wave's length, is left.
const MARGIN: Duration = Duration::from_secs(600);
const HOLD: Duration = Duration::from_secs(30);
/// One pod in this many carries a claim.
const CLAIM_EVERY: usize = 4;
/// Pods created between two memory reads.
const STEP: usize = 10;
/// How long one step's pods are waited on before the next memory read.
const STEP_WAIT: Duration = Duration::from_secs(120);
const GIB: u64 = 1 << 30;
/// An API call this slow is named in the wave's line.
const SLOW_CALL: u128 = 5_000;
/// The slowest pods a slow wave names.
const SLOWEST: usize = 3;

/// Seconds since the epoch of an RFC 3339 UTC time (`2026-11-14T11:26:00Z`,
/// with or without a fraction); `None` for anything else.
pub fn epoch_secs(t: &str) -> Option<i64> {
    let t = t.strip_suffix('Z')?;
    let t = t.split('.').next()?;
    let (date, time) = t.split_once('T')?;
    let num = |s: &str| s.parse::<i64>().ok();
    let mut d = date.splitn(3, '-').map(num);
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let mut c = time.splitn(3, ':').map(num);
    let (h, min, sec) = (c.next()??, c.next()??, c.next()??);
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) || h > 23 || min > 59 || sec > 60 {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + h * 3600 + min * 60 + sec)
}

/// A Running pod's own start, ms: its container's `startedAt` less its
/// `creationTimestamp`, plus the second both are truncated to, so it is an
/// upper bound like the runner's.
pub fn own_start_ms(p: &Value) -> Option<u128> {
    let created = epoch_secs(s(p, "/metadata/creationTimestamp"))?;
    let started = epoch_secs(s(p, "/status/containerStatuses/0/state/running/startedAt"))?;
    u128::try_from(started.checked_sub(created)?).ok().map(|d| d * 1000 + 1000)
}

/// The start a pod is measured at: what the runner saw, or its own, if less.
pub fn start_ms(seen: u128, own: Option<u128>) -> u128 {
    own.map_or(seen, |o| o.min(seen))
}

/// A wave's API calls: the slowest, and how many failed.
#[derive(Default)]
pub struct Calls {
    pub slowest_ms: u128,
    pub slowest: String,
    pub errors: usize,
}

impl Calls {
    async fn time<T>(&mut self, what: &str, f: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
        let t = Instant::now();
        let r = f.await;
        let ms = t.elapsed().as_millis();
        if ms > self.slowest_ms {
            self.slowest_ms = ms;
            self.slowest = what.to_string();
        }
        if r.is_err() {
            self.errors += 1;
        }
        r
    }
}

/// `MemTotal` and `MemAvailable` of a `/proc/meminfo`, in bytes.
pub fn meminfo(text: &str) -> Option<(u64, u64)> {
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(':'))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
            .map(|kib| kib * 1024)
    };
    Some((field("MemTotal")?, field("MemAvailable")?))
}

/// The memory a wave leaves the node: `pct`% of its total, at least 1 GiB.
pub fn mem_floor(total: u64, pct: u64) -> u64 {
    (total / 100 * pct.min(90)).max(GIB)
}

/// Whether this `MemTotal` is the Node's `capacity.memory` (within 5%):
/// the runner reads the meminfo of the node it is on.
pub fn same_node_memory(capacity: u64, total: u64) -> bool {
    capacity > 0 && capacity.abs_diff(total) <= capacity / 20
}

/// The node's memory as the runner sees it: (total, available).
fn node_memory() -> Option<(u64, u64)> {
    meminfo(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// The wave size for a node: 80% of its free pod slots, at least 1, at most
/// `max`.
pub fn wave_size(allocatable_pods: u64, on_node: u64, max: u64) -> u64 {
    (allocatable_pods.saturating_sub(on_node) * 8 / 10).clamp(1, max.max(1))
}

/// Whether a wave's p95 start is a regression against the first wave's.
pub fn slowed(first_p95: u128, p95: u128) -> bool {
    p95 > first_p95 * 2 + 2000
}

/// One wave's numbers.
pub struct Wave {
    pub n: usize,
    pub pods: usize,
    pub claims: usize,
    pub p50_ms: u128,
    pub p95_ms: u128,
    pub max_ms: u128,
    pub ramp_ms: u128,
    pub drain_ms: u128,
    /// The node's `MemAvailable` before the wave and at its fullest, MiB.
    pub mem_before_mib: u64,
    pub mem_low_mib: u64,
    /// Whether the ramp stopped at the memory floor short of its size.
    pub mem_stopped: bool,
    /// What its drain left: pods, claims, PVs, by name.
    pub left: Vec<String>,
    /// Pods that never ran, with why.
    pub never_ran: Vec<String>,
    /// The slowest pods: name, measured start, and what the runner saw, ms.
    pub slowest: Vec<(String, u128, u128)>,
    /// The ramp's API calls.
    pub calls: Calls,
}

impl Wave {
    pub fn fields(&self) -> String {
        format!(
            "\"wave\": {}, \"pods\": {}, \"claims\": {}, \"p50_ms\": {}, \"p95_ms\": {}, \"max_ms\": {}, \"ramp_ms\": {}, \"drain_ms\": {}, \"left\": {}, \"mem_before_mib\": {}, \"mem_low_mib\": {}, \"mem_stopped\": {}, \"api_slowest_ms\": {}, \"api_errors\": {}",
            self.n, self.pods, self.claims, self.p50_ms, self.p95_ms, self.max_ms, self.ramp_ms, self.drain_ms, self.left.len(),
            self.mem_before_mib, self.mem_low_mib, self.mem_stopped, self.calls.slowest_ms, self.calls.errors
        )
    }

    /// The wave's verdict against the first wave's p95.
    pub fn outcome(&self, first_p95: Option<u128>) -> Outcome {
        let mut wrong = Vec::new();
        if !self.never_ran.is_empty() {
            wrong.push(format!("{} pods never ran ({})", self.never_ran.len(), self.never_ran.iter().take(3).cloned().collect::<Vec<_>>().join("; ")));
        }
        if self.pods == 0 && self.mem_stopped {
            wrong.push(format!("the node was under the memory floor before the wave ({} MiB available)", self.mem_low_mib));
        }
        if !self.left.is_empty() {
            wrong.push(format!("left after the drain: {}", self.left.iter().take(5).cloned().collect::<Vec<_>>().join(", ")));
        }
        if let Some(f) = first_p95.filter(|f| slowed(*f, self.p95_ms)) {
            let names: Vec<String> = self.slowest.iter().map(|(n, ms, seen)| format!("{n} {ms} ms (seen at {seen} ms)")).collect();
            wrong.push(format!("p95 start {} ms against the first wave's {f} ms (slowest: {})", self.p95_ms, names.join(", ")));
        }
        let mut summary = format!(
            "{} pods ({} with claims): start p50 {} ms, p95 {} ms; drained in {} ms{}",
            self.pods, self.claims, self.p50_ms, self.p95_ms, self.drain_ms,
            if self.mem_stopped { format!("; stopped at the memory floor ({} MiB available)", self.mem_low_mib) } else { String::new() }
        );
        if self.calls.slowest_ms >= SLOW_CALL {
            summary.push_str(&format!("; slowest API call {} ms ({})", self.calls.slowest_ms, self.calls.slowest));
        }
        if self.calls.errors > 0 {
            summary.push_str(&format!("; {} API call(s) failed", self.calls.errors));
        }
        if wrong.is_empty() { Outcome::Pass(summary) } else { Outcome::Fail(format!("{summary}; {}", wrong.join("; "))) }
    }
}

fn knob(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let node = match api.get(&format!("/api/v1/nodes/{}", env.node_name)).await {
        Ok(Some(n)) => n,
        Ok(None) => {
            r.record("capacity", Outcome::Infra(format!("Node {} is gone", env.node_name)), 0, None);
            return;
        }
        Err(e) => {
            r.record("capacity", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let allocatable: u64 = s(&node, "/status/allocatable/pods").parse().unwrap_or(0);
    let path = format!("/api/v1/pods?fieldSelector=spec.nodeName%3D{}", env.node_name);
    let on_node = match api.list(&path).await {
        Ok(Some(p)) => p.iter().filter(|p| !matches!(s(p, "/status/phase"), "Succeeded" | "Failed")).count() as u64,
        Ok(None) => 0,
        Err(e) => {
            r.record("capacity", Outcome::Infra(format!("pods on the node: {e}")), 0, None);
            return;
        }
    };
    if allocatable == 0 {
        r.record("capacity", Outcome::Fail(format!("Node {} reports no allocatable pods", env.node_name)), 0, None);
        return;
    }
    let mut size = wave_size(allocatable, on_node, knob("RUSTKUBE_NODE_TEST_WAVE_MAX").unwrap_or(500)) as usize;
    let claims = matches!(k8s::no_builtin_class(&api).await, Ok(None));
    let capacity = crate::api::quantity_bytes(s(&node, "/status/capacity/memory")).unwrap_or(0);
    let floor = match node_memory() {
        Some((total, _)) if same_node_memory(capacity, total) => Some(mem_floor(total, knob("RUSTKUBE_NODE_TEST_MEM_FLOOR_PCT").unwrap_or(20))),
        _ => None,
    };
    r.record(
        "capacity",
        Outcome::Pass(format!(
            "{allocatable} pod slots, {on_node} in use: waves of up to {size}{}; {}",
            if claims { "" } else { " (no built-in class: no claims)" },
            match floor {
                Some(f) => format!("ramp stops under {} MiB available", f >> 20),
                None => "no memory floor (this meminfo is not the Node's)".to_string(),
            }
        )),
        0,
        None,
    );

    let waves = knob("RUSTKUBE_NODE_TEST_WAVES").map(|w| w as usize);
    let mut first_p95 = None;
    let mut last = Duration::ZERO;
    for n in 1.. {
        if waves.is_some_and(|w| n > w) || env.remaining() < MARGIN + last * 3 {
            break;
        }
        let t = Instant::now();
        let w = wave(&env, &api, n, size, claims, floor).await;
        last = t.elapsed();
        if n == 1 && w.mem_stopped {
            size = w.pods.max(1);
        }
        let fields = w.fields();
        r.record(&format!("wave-{n}"), w.outcome(first_p95), last.as_millis(), Some(&fields));
        if first_p95.is_none() && w.never_ran.is_empty() {
            first_p95 = Some(w.p95_ms);
        }
    }
    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(60)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

/// The objects of pod `i` of wave `n`: the pod, and its claim if it has one.
pub fn wave_objects(env: &Env, n: usize, i: usize, claims: bool) -> (Value, Option<Value>) {
    let name = format!("w{n}-{i}");
    let mut p = k8s::pod(env, &name, &["sleep", "86400"]);
    p["metadata"]["labels"]["storm.io/wave"] = json!(n.to_string());
    p["spec"]["terminationGracePeriodSeconds"] = json!(5);
    if !claims || !i.is_multiple_of(CLAIM_EVERY) {
        return (p, None);
    }
    let mut c = k8s::claim(env, &name, "16Mi");
    c["metadata"]["labels"]["storm.io/wave"] = json!(n.to_string());
    let mut fill = k8s::container(env, "fill", &["fill", k8s::FS_PATH, &(n * 100_000 + i).to_string(), "65536"]);
    fill["volumeMounts"] = json!([{ "name": "claim", "mountPath": k8s::FS_PATH }]);
    p["spec"]["initContainers"] = json!([fill]);
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "claim", "mountPath": k8s::FS_PATH }]);
    p["spec"]["volumes"] = json!([{ "name": "claim", "persistentVolumeClaim": { "claimName": name } }]);
    (p, Some(c))
}

/// Poll a wave's pods until each created one has been seen Running, or `end`.
/// `started` holds each pod's (measured, seen) start.
async fn poll(
    api: &Api, sel: &str, created: &HashMap<String, Instant>, started: &mut HashMap<String, (u128, u128)>,
    last_seen: &mut HashMap<String, String>, calls: &mut Calls, end: Instant,
) {
    while started.len() < created.len() && Instant::now() < end {
        if let Ok(Some(items)) = calls.time("list the wave", api.list(sel)).await {
            for p in &items {
                let name = s(p, "/metadata/name").to_string();
                if started.contains_key(&name) {
                    continue;
                }
                if s(p, "/status/phase") == "Running" && p.pointer("/status/containerStatuses/0/state/running").is_some() {
                    if let Some(t) = created.get(&name) {
                        let seen = t.elapsed().as_millis();
                        started.insert(name, (start_ms(seen, own_start_ms(p)), seen));
                    }
                } else {
                    last_seen.insert(name, format!("{}{}", s(p, "/status/phase"), k8s::waiting(p)));
                }
            }
        }
        if started.len() < created.len() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn wave(env: &Env, api: &Api, n: usize, size: usize, claims: bool, floor: Option<u64>) -> Wave {
    let mut w = Wave {
        n, pods: 0, claims: 0, p50_ms: 0, p95_ms: 0, max_ms: 0, ramp_ms: 0, drain_ms: 0,
        mem_before_mib: 0, mem_low_mib: 0, mem_stopped: false, left: Vec::new(), never_ran: Vec::new(),
        slowest: Vec::new(), calls: Calls::default(),
    };
    let ramp = Instant::now();
    let sel = format!("{}?labelSelector=storm.io%2Fwave%3D{n},storm.io%2Ftest-run%3D{}", k8s::pods(env), env.run_id);
    let mut created: HashMap<String, Instant> = HashMap::new();
    let mut started: HashMap<String, (u128, u128)> = HashMap::new();
    let mut last_seen: HashMap<String, String> = HashMap::new();
    let mut claim_names = Vec::new();
    // The ramp's time: 10 minutes, or what the suite has left.
    let end = Instant::now() + env.budget(Duration::from_secs(600), Duration::from_secs(120));
    let mut low = u64::MAX;
    let mut i = 0;
    while i < size && Instant::now() < end {
        if let Some((_, avail)) = node_memory() {
            if i == 0 {
                w.mem_before_mib = avail >> 20;
            }
            low = low.min(avail);
            if floor.is_some_and(|f| avail < f) {
                w.mem_stopped = true;
                break;
            }
        }
        for j in i..(i + STEP).min(size) {
            let (p, c) = wave_objects(env, n, j, claims);
            let name = s(&p, "/metadata/name").to_string();
            if let Some(c) = c {
                match w.calls.time(&format!("create claim {name}"), api.create(&k8s::pvcs(env), &c)).await {
                    Ok(_) => claim_names.push(name.clone()),
                    Err(e) => {
                        w.never_ran.push(format!("{name}: claim: {e}"));
                        continue;
                    }
                }
            }
            match w.calls.time(&format!("create pod {name}"), api.create(&k8s::pods(env), &p)).await {
                Ok(_) => {
                    created.insert(name, Instant::now());
                }
                Err(e) => w.never_ran.push(format!("{name}: {e}")),
            }
        }
        i = (i + STEP).min(size);
        // Wait on this step before reading the memory again: a pod takes its
        // memory as it starts, not when it is made.
        poll(api, &sel, &created, &mut started, &mut last_seen, &mut w.calls, end.min(Instant::now() + STEP_WAIT)).await;
    }
    poll(api, &sel, &created, &mut started, &mut last_seen, &mut w.calls, end).await;
    if let Some((_, avail)) = node_memory() {
        low = low.min(avail);
    }
    w.mem_low_mib = if low == u64::MAX { 0 } else { low >> 20 };
    w.pods = created.len() + w.never_ran.len();
    w.claims = claim_names.len();
    w.ramp_ms = ramp.elapsed().as_millis();
    for name in created.keys().filter(|n| !started.contains_key(*n)) {
        w.never_ran.push(format!("{name}: {}", last_seen.get(name).map(String::as_str).unwrap_or("never listed")));
    }
    let times: Vec<u128> = started.values().map(|(ms, _)| *ms).collect();
    w.p50_ms = percentile(&times, 50);
    w.p95_ms = percentile(&times, 95);
    w.max_ms = times.iter().copied().max().unwrap_or(0);
    let mut slowest: Vec<(String, u128, u128)> = started.iter().map(|(n, (ms, seen))| (n.clone(), *ms, *seen)).collect();
    slowest.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
    slowest.truncate(SLOWEST);
    w.slowest = slowest;

    // Hold, and read one pod's log back: the kubelet still serves while full.
    tokio::time::sleep(HOLD.min(env.remaining().saturating_sub(MARGIN))).await;
    if let Some(name) = started.keys().next() {
        if let Err(e) = k8s::log(env, api, name, false).await {
            w.never_ran.push(format!("{name}: log while held: {e}"));
        }
    }

    // Drain, and see what is left.
    let drain = Instant::now();
    for name in created.keys() {
        let _ = api.delete(&format!("{}/{name}", k8s::pods(env))).await;
    }
    let mut pvs = Vec::new();
    for name in &claim_names {
        let path = format!("{}/{name}", k8s::pvcs(env));
        if let Ok(Some(c)) = api.get(&path).await {
            let pv = s(&c, "/spec/volumeName");
            if !pv.is_empty() {
                pvs.push(pv.to_string());
            }
        }
        let _ = api.delete(&path).await;
    }
    let end = Instant::now() + env.budget(Duration::from_secs(600), Duration::from_secs(60));
    loop {
        let mut left = Vec::new();
        if let Ok(Some(items)) = api.list(&sel).await {
            left.extend(items.iter().map(|p| format!("pod {}", s(p, "/metadata/name"))));
        }
        for name in &claim_names {
            if let Ok(Some(_)) = api.get(&format!("{}/{name}", k8s::pvcs(env))).await {
                left.push(format!("claim {name}"));
            }
        }
        for pv in &pvs {
            if let Ok(Some(_)) = api.get(&format!("{}/{pv}", k8s::PVS)).await {
                left.push(format!("PV {pv}"));
            }
        }
        if left.is_empty() || Instant::now() >= end {
            w.left = left;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    w.drain_ms = drain.elapsed().as_millis();
    w
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::k8s::tests::env;

    #[test]
    fn waves_are_sized_from_the_free_slots() {
        assert_eq!(wave_size(110, 10, 500), 80);
        assert_eq!(wave_size(110, 10, 50), 50);
        assert_eq!(wave_size(10, 10, 500), 1);
        assert_eq!(wave_size(0, 5, 500), 1);
    }

    #[test]
    fn a_slow_wave_is_twice_the_first_plus_two_seconds() {
        assert!(!slowed(1000, 4000));
        assert!(slowed(1000, 4001));
        assert!(!slowed(0, 2000));
    }

    #[test]
    fn every_fourth_pod_carries_a_claim_when_the_class_is_here() {
        let (p, c) = wave_objects(&env(), 2, 4, true);
        let c = c.unwrap();
        assert_eq!(c["spec"]["resources"]["requests"]["storage"], "16Mi");
        assert_eq!(c["metadata"]["labels"]["storm.io/wave"], "2");
        assert_eq!(p["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"], "w2-4");
        assert_eq!(p["spec"]["initContainers"][0]["args"][0], "fill");
        assert_eq!(p["metadata"]["labels"]["storm.io/test-run"], "r1");
        assert!(wave_objects(&env(), 2, 5, true).1.is_none());
        assert!(wave_objects(&env(), 2, 4, false).1.is_none());
    }

    #[test]
    fn a_wave_fails_on_residue_or_slowdown() {
        let w = |p95, left: Vec<String>| Wave {
            n: 2, pods: 10, claims: 2, p50_ms: 100, p95_ms: p95, max_ms: p95, ramp_ms: 0, drain_ms: 0,
            mem_before_mib: 4000, mem_low_mib: 3000, mem_stopped: false, left, never_ran: vec![],
            slowest: vec![("w2-3".into(), p95, p95 + 100)], calls: Calls::default(),
        };
        assert!(matches!(w(500, vec![]).outcome(Some(400)), Outcome::Pass(_)));
        assert!(matches!(w(5000, vec![]).outcome(Some(400)), Outcome::Fail(_)));
        assert!(matches!(w(500, vec!["PV x".into()]).outcome(Some(400)), Outcome::Fail(_)));
        assert!(w(500, vec![]).fields().contains("\"p95_ms\": 500"));
        assert!(w(500, vec![]).fields().contains("\"mem_low_mib\": 3000"));
        assert!(w(500, vec![]).fields().contains("\"api_slowest_ms\": 0, \"api_errors\": 0"));
        assert!(matches!(w(5000, vec![]).outcome(Some(400)), Outcome::Fail(s) if s.contains("slowest: w2-3 5000 ms (seen at 5100 ms)")));
        let mut hung = w(500, vec![]);
        hung.calls = Calls { slowest_ms: 30_012, slowest: "list the wave".into(), errors: 1 };
        assert!(matches!(hung.outcome(Some(400)), Outcome::Pass(s) if s.contains("slowest API call 30012 ms (list the wave)") && s.contains("1 API call(s) failed")));
        let mut full = w(500, vec![]);
        full.mem_stopped = true;
        assert!(matches!(full.outcome(Some(400)), Outcome::Pass(s) if s.contains("memory floor")));
        full.pods = 0;
        assert!(matches!(full.outcome(Some(400)), Outcome::Fail(_)));
    }

    #[test]
    fn meminfo_reads_total_and_available() {
        let text = "MemTotal:       16384000 kB\nMemFree:         1000 kB\nMemAvailable:    4096000 kB\n";
        assert_eq!(meminfo(text), Some((16384000 * 1024, 4096000 * 1024)));
        assert_eq!(meminfo("MemTotal: 10 kB\n"), None);
        assert_eq!(meminfo(""), None);
    }

    #[test]
    fn the_floor_is_a_share_of_the_node_and_at_least_a_gib() {
        assert_eq!(mem_floor(16 * GIB, 20), 16 * GIB / 100 * 20);
        assert_eq!(mem_floor(2 * GIB, 20), GIB);
        assert_eq!(mem_floor(100 * GIB, 500), 100 * GIB / 100 * 90);
    }

    #[test]
    fn the_floor_is_used_only_on_the_nodes_own_meminfo() {
        assert!(same_node_memory(16 * GIB, 16 * GIB - GIB / 2));
        assert!(!same_node_memory(16 * GIB, 64 * GIB));
        assert!(!same_node_memory(0, 16 * GIB));
    }

    #[test]
    fn rfc3339_times_read_as_epoch_seconds() {
        assert_eq!(epoch_secs("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(epoch_secs("2026-11-14T11:26:00Z"), Some(1_794_655_560));
        assert_eq!(epoch_secs("2000-03-01T00:00:01.250Z"), Some(951_868_801));
        assert_eq!(epoch_secs("2026-11-14T11:26:00+01:00"), None);
        assert_eq!(epoch_secs(""), None);
        assert_eq!(epoch_secs("2026-13-01T00:00:00Z"), None);
    }

    #[test]
    fn a_pods_own_start_bounds_what_the_runner_saw() {
        let p = json!({
            "metadata": { "creationTimestamp": "2026-11-14T11:26:00Z" },
            "status": { "containerStatuses": [{ "state": { "running": { "startedAt": "2026-11-14T11:26:01Z" } } }] }
        });
        assert_eq!(own_start_ms(&p), Some(2000));
        // The runner blind for 30 s behind a hung call: the pod's own wins.
        assert_eq!(start_ms(31_288, own_start_ms(&p)), 2000);
        // The runner saw it sooner than a second's truncation allows.
        assert_eq!(start_ms(1_380, own_start_ms(&p)), 1_380);
        assert_eq!(start_ms(1_380, None), 1_380);
        assert_eq!(own_start_ms(&json!({ "metadata": { "creationTimestamp": "2026-11-14T11:26:05Z" },
            "status": { "containerStatuses": [{ "state": { "running": { "startedAt": "2026-11-14T11:26:01Z" } } }] } })), None);
    }
}
