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
//! 2. **Measure** each pod's start: from its create to the first time the API
//!    shows it Running (polled every second, so the resolution is a second).
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
    /// What its drain left: pods, claims, PVs, by name.
    pub left: Vec<String>,
    /// Pods that never ran, with why.
    pub never_ran: Vec<String>,
}

impl Wave {
    pub fn fields(&self) -> String {
        format!(
            "\"wave\": {}, \"pods\": {}, \"claims\": {}, \"p50_ms\": {}, \"p95_ms\": {}, \"max_ms\": {}, \"ramp_ms\": {}, \"drain_ms\": {}, \"left\": {}",
            self.n, self.pods, self.claims, self.p50_ms, self.p95_ms, self.max_ms, self.ramp_ms, self.drain_ms, self.left.len()
        )
    }

    /// The wave's verdict against the first wave's p95.
    pub fn outcome(&self, first_p95: Option<u128>) -> Outcome {
        let mut wrong = Vec::new();
        if !self.never_ran.is_empty() {
            wrong.push(format!("{} pods never ran ({})", self.never_ran.len(), self.never_ran.iter().take(3).cloned().collect::<Vec<_>>().join("; ")));
        }
        if !self.left.is_empty() {
            wrong.push(format!("left after the drain: {}", self.left.iter().take(5).cloned().collect::<Vec<_>>().join(", ")));
        }
        if let Some(f) = first_p95.filter(|f| slowed(*f, self.p95_ms)) {
            wrong.push(format!("p95 start {} ms against the first wave's {f} ms", self.p95_ms));
        }
        let summary = format!(
            "{} pods ({} with claims): start p50 {} ms, p95 {} ms; drained in {} ms",
            self.pods, self.claims, self.p50_ms, self.p95_ms, self.drain_ms
        );
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
    let size = wave_size(allocatable, on_node, knob("RUSTKUBE_NODE_TEST_WAVE_MAX").unwrap_or(500)) as usize;
    let claims = matches!(k8s::no_builtin_class(&api).await, Ok(None));
    r.record(
        "capacity",
        Outcome::Pass(format!("{allocatable} pod slots, {on_node} in use: waves of {size}{}", if claims { "" } else { " (no built-in class: no claims)" })),
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
        let w = wave(&env, &api, n, size, claims).await;
        last = t.elapsed();
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
    if !claims || i % CLAIM_EVERY != 0 {
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

async fn wave(env: &Env, api: &Api, n: usize, size: usize, claims: bool) -> Wave {
    let mut w = Wave {
        n, pods: size, claims: 0, p50_ms: 0, p95_ms: 0, max_ms: 0, ramp_ms: 0, drain_ms: 0,
        left: Vec::new(), never_ran: Vec::new(),
    };
    let ramp = Instant::now();
    let mut created: HashMap<String, Instant> = HashMap::new();
    let mut claim_names = Vec::new();
    for i in 0..size {
        let (p, c) = wave_objects(env, n, i, claims);
        let name = s(&p, "/metadata/name").to_string();
        if let Some(c) = c {
            match api.create(&k8s::pvcs(env), &c).await {
                Ok(_) => claim_names.push(name.clone()),
                Err(e) => {
                    w.never_ran.push(format!("{name}: claim: {e}"));
                    continue;
                }
            }
        }
        match api.create(&k8s::pods(env), &p).await {
            Ok(_) => {
                created.insert(name, Instant::now());
            }
            Err(e) => w.never_ran.push(format!("{name}: {e}")),
        }
    }
    w.claims = claim_names.len();

    // Poll the wave's pods until each has been seen Running, or the ramp's
    // time is up (10 minutes, or what the suite has left).
    let sel = format!("{}?labelSelector=storm.io%2Fwave%3D{n},storm.io%2Ftest-run%3D{}", k8s::pods(env), env.run_id);
    let mut started: HashMap<String, u128> = HashMap::new();
    let mut last_seen: HashMap<String, String> = HashMap::new();
    let end = Instant::now() + env.budget(Duration::from_secs(600), Duration::from_secs(120));
    while started.len() < created.len() && Instant::now() < end {
        if let Ok(Some(items)) = api.list(&sel).await {
            for p in &items {
                let name = s(p, "/metadata/name").to_string();
                if started.contains_key(&name) {
                    continue;
                }
                if s(p, "/status/phase") == "Running" && p.pointer("/status/containerStatuses/0/state/running").is_some() {
                    if let Some(t) = created.get(&name) {
                        started.insert(name, t.elapsed().as_millis());
                    }
                } else {
                    last_seen.insert(name, format!("{}{}", s(p, "/status/phase"), k8s::waiting(p)));
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    w.ramp_ms = ramp.elapsed().as_millis();
    for name in created.keys().filter(|n| !started.contains_key(*n)) {
        w.never_ran.push(format!("{name}: {}", last_seen.get(name).map(String::as_str).unwrap_or("never listed")));
    }
    let times: Vec<u128> = started.values().copied().collect();
    w.p50_ms = percentile(&times, 50);
    w.p95_ms = percentile(&times, 95);
    w.max_ms = times.iter().copied().max().unwrap_or(0);

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
            n: 2, pods: 10, claims: 2, p50_ms: 100, p95_ms: p95, max_ms: p95, ramp_ms: 0, drain_ms: 0, left, never_ran: vec![],
        };
        assert!(matches!(w(500, vec![]).outcome(Some(400)), Outcome::Pass(_)));
        assert!(matches!(w(5000, vec![]).outcome(Some(400)), Outcome::Fail(_)));
        assert!(matches!(w(500, vec!["PV x".into()]).outcome(Some(400)), Outcome::Fail(_)));
        assert!(w(500, vec![]).fields().contains("\"p95_ms\": 500"));
    }
}
