//! `medium` (< 30 min): claims of the built-in `stormblock` class, end to
//! end through the API (#64).
//!
//! **Sizes.** One claim at every class of the ladder the kubelet offers
//! (`pkg/kubelet/src/storage.rs` `SIZE_CLASSES`), and arbitrary sizes: the
//! smallest request (1 byte), just over a class (1Mi+1, 17Mi), decimal and
//! fractional units (1500M, 3.5Gi), and 600Gi. Each, in parallel:
//!
//! 1. a claim and a pod that mounts it, writes 64 KiB, reads it back, and
//!    checks that the filesystem's size is in (the class below, the class the
//!    request rounds to];
//! 2. the claim is Bound and its status capacity is that class;
//! 3. pod and claim deleted, and the PV is gone (reclaimed: the clone deleted
//!    on the node).
//!
//! **Above the ladder** (2Ti): refused, not stuck. The pod waits with the
//! reason ("larger than the largest size class") on its status or in an
//! Event, and the claim is never Bound.
//!
//! **Minting.** A class with no blank on the node is minted on first use,
//! which for 1 TiB is minutes of formatting (stormblock#141). Every case has
//! [`Env::mint_budget`] to finish; the `ms` of the 1Ti case is that time.
//!
//! **Overcommit** (a no-overcommit drive refuses claims beyond its free space
//! at bind) is reported skip: the kubelet has no such setting yet
//! (rustkube-node#62).

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::{self, s, Api};
use crate::env::Env;
use crate::k8s::{self, MIB};
use crate::report::{Outcome, Report};

const MARGIN: Duration = Duration::from_secs(180);
const BYTES: u64 = 64 * 1024;

/// The kubelet's ladder, in bytes. The same list as `SIZE_CLASSES`; a class
/// added there and not here is a claim this suite rounds wrongly, and the
/// size check says so.
pub const LADDER: &[(&str, u64)] = &[
    ("1Mi", MIB),
    ("16Mi", 16 * MIB),
    ("64Mi", 64 * MIB),
    ("256Mi", 256 * MIB),
    ("1Gi", 1024 * MIB),
    ("4Gi", 4 * 1024 * MIB),
    ("16Gi", 16 * 1024 * MIB),
    ("64Gi", 64 * 1024 * MIB),
    ("256Gi", 256 * 1024 * MIB),
    ("1Ti", 1024 * 1024 * MIB),
];

/// One claim: a test name and the request, verbatim.
pub struct Case {
    pub name: String,
    pub request: String,
}

/// Every class, then the arbitrary sizes. 2Ti is separate (`above-ladder`).
pub fn cases() -> Vec<Case> {
    let mut v: Vec<Case> = LADDER
        .iter()
        .map(|(q, _)| Case { name: format!("class-{}", q.to_ascii_lowercase()), request: q.to_string() })
        .collect();
    for (name, request) in [
        ("smallest", "1"),
        ("1mi-plus-1", "1048577"),
        ("17mi", "17Mi"),
        ("1500m", "1500M"),
        ("3.5gi", "3.5Gi"),
        ("600gi", "600Gi"),
    ] {
        v.push(Case { name: name.into(), request: request.into() });
    }
    v
}

/// The class a request rounds to, and the class below it (0 for the first):
/// the filesystem must be larger than `lo` and at most `hi`. `None` above
/// the ladder.
pub fn class_range(request: &str) -> Option<(u64, u64)> {
    let want = api::quantity_bytes(request)?;
    let i = LADDER.iter().position(|(_, b)| *b >= want)?;
    Some((if i == 0 { 0 } else { LADDER[i - 1].1 }, LADDER[i].1))
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    // All at once: the budget is 30 minutes and a 1 TiB mint may take most of it.
    let mut tasks = Vec::new();
    for (i, c) in cases().into_iter().enumerate() {
        let (env, api) = (env.clone(), api.clone());
        tasks.push(tokio::spawn(async move {
            let t = Instant::now();
            let o = size_case(&env, &api, &c, i as u64 + 1).await;
            (format!("pvc-size-{}", c.name), o, t.elapsed().as_millis())
        }));
    }
    let above = {
        let (env, api) = (env.clone(), api.clone());
        tokio::spawn(async move {
            let t = Instant::now();
            let o = above_ladder(&env, &api).await;
            ("pvc-size-above-ladder".to_string(), o, t.elapsed().as_millis())
        })
    };
    tasks.push(above);
    for t in tasks {
        match t.await {
            Ok((name, o, ms)) => {
                r.record(&name, o, ms, None);
            }
            Err(e) => {
                r.record("pvc-size", Outcome::Fail(format!("a case panicked: {e}")), 0, None);
            }
        }
    }
    r.record(
        "pvc-overcommit-refused",
        Outcome::Skip("the kubelet has no overcommit setting to honour yet (rustkube-node#62)".into()),
        0,
        None,
    );
    let t = Instant::now();
    let o = match k8s::drain(&env, &api, env.budget(Duration::from_secs(300), Duration::from_secs(10))).await {
        Ok(left) if left.is_empty() => Outcome::Pass("nothing of this run is left: pods, claims and their PVs are gone".into()),
        Ok(left) => Outcome::Fail(format!("PVs not reclaimed: {}", left.join(", "))),
        Err(e) => Outcome::Infra(e),
    };
    r.record("reclaim-all", o, t.elapsed().as_millis(), None);
}

fn within(env: &Env, want: Duration) -> Duration {
    env.budget(want, MARGIN)
}

/// Claim `request`, mount, write, check the size, delete, check reclaimed.
async fn size_case(env: &Env, api: &Api, c: &Case, seed: u64) -> Outcome {
    let Some((lo, hi)) = class_range(&c.request) else {
        return Outcome::Infra(format!("{} does not round to a class here", c.request));
    };
    let name = format!("sz-{}", c.name.replace('.', "-"));
    if let Err(e) = api.create(&k8s::pvcs(env), &k8s::claim(env, &name, &c.request)).await {
        return Outcome::Infra(e);
    }
    let args = [seed.to_string(), BYTES.to_string(), lo.to_string(), hi.to_string()];
    if let Err(e) = api.create(&k8s::pods(env), &k8s::work(env, &name, &name, "sized", &args)).await {
        return Outcome::Infra(e);
    }
    let done = match k8s::pod_done(env, api, &name, within(env, env.mint_budget)).await {
        Ok(d) => d,
        Err(e) => return Outcome::Fail(e),
    };
    if done.phase != "Succeeded" {
        return Outcome::Fail(format!("request {}: pod failed on {}: {}", c.request, done.node, done.message));
    }
    let class = LADDER.iter().find(|(_, b)| *b == hi).map(|(q, _)| *q).unwrap_or("?");

    // Bound, and saying the class it rounded to.
    let claim_path = format!("{}/{name}", k8s::pvcs(env));
    let pvc = match api.get(&claim_path).await {
        Ok(Some(p)) => p,
        Ok(None) => return Outcome::Fail(format!("claim {name} is gone")),
        Err(e) => return Outcome::Infra(e),
    };
    let (phase, pv) = (s(&pvc, "/status/phase").to_string(), s(&pvc, "/spec/volumeName").to_string());
    let status = s(&pvc, "/status/capacity/storage").to_string();
    let mut wrong = Vec::new();
    if phase != "Bound" {
        wrong.push(format!("claim is {phase:?}, not Bound"));
    }
    if api::quantity_bytes(&status) != Some(hi) {
        wrong.push(format!("claim status capacity is {status:?}, not {class}"));
    }

    // Deleted, and reclaimed.
    let t = Instant::now();
    let pod_path = format!("{}/{name}", k8s::pods(env));
    if let Err(e) = api.delete(&pod_path).await {
        return Outcome::Infra(e);
    }
    if let Err(e) = k8s::gone(api, &pod_path, within(env, Duration::from_secs(120))).await {
        wrong.push(e);
    } else if let Err(e) = api.delete(&claim_path).await {
        return Outcome::Infra(e);
    } else if pv.is_empty() {
        wrong.push("the claim named no PV".into());
    } else if let Err(e) = k8s::gone(api, &format!("{}/{pv}", k8s::PVS), within(env, Duration::from_secs(300))).await {
        wrong.push(format!("not reclaimed: {e}"));
    }
    let detail = format!(
        "request {} -> {class} on {}: {}; claim status {status:?}; deleted and reclaimed in {} s",
        c.request,
        done.node,
        done.message,
        t.elapsed().as_secs()
    );
    if wrong.is_empty() {
        Outcome::Pass(detail)
    } else {
        Outcome::Fail(format!("{}; {detail}", wrong.join("; ")))
    }
}

/// 2Ti: the pod waits with the reason, and the claim is never Bound.
async fn above_ladder(env: &Env, api: &Api) -> Outcome {
    const WHY: &str = "larger than the largest size class";
    let name = "sz-above-ladder";
    if let Err(e) = api.create(&k8s::pvcs(env), &k8s::claim(env, name, "2Ti")).await {
        return Outcome::Infra(e);
    }
    let args = ["1".to_string(), BYTES.to_string(), "0".to_string(), u64::MAX.to_string()];
    if let Err(e) = api.create(&k8s::pods(env), &k8s::work(env, name, name, "sized", &args)).await {
        return Outcome::Infra(e);
    }
    let pod_path = format!("{}/{name}", k8s::pods(env));
    let path = pod_path.as_str();
    let seen = k8s::until(within(env, Duration::from_secs(180)), || async move {
        let p = api.get(path).await?.ok_or_else(|| "the pod is gone".to_string())?;
        let phase = s(&p, "/status/phase");
        if phase == "Succeeded" || phase == "Failed" || phase == "Running" {
            return Ok(Err(format!("the pod reached {phase}: a 2Ti claim was given a volume")));
        }
        let status = format!("{}{}", s(&p, "/status/message"), k8s::waiting(&p));
        if status.contains(WHY) {
            return Ok(Ok(format!("pod status: {status}")));
        }
        let mut evs = k8s::events_for(env, api, "Pod", name).await?;
        evs.extend(k8s::events_for(env, api, "PersistentVolumeClaim", name).await?);
        match evs.iter().find(|e| e.contains(WHY)) {
            Some(e) => Ok(Ok(format!("event: {e}"))),
            None => Err(format!("no reason naming the ladder yet (pod {phase:?}{}; events: {evs:?})", k8s::waiting(&p))),
        }
    })
    .await;
    let claim = api.get(&format!("{}/{name}", k8s::pvcs(env))).await.ok().flatten();
    let phase = claim.as_ref().map(|c| s(c, "/status/phase").to_string()).unwrap_or_default();
    let _ = api.delete(&pod_path).await;
    let _ = api.delete(&format!("{}/{name}", k8s::pvcs(env))).await;
    match seen {
        Ok(Ok(why)) if phase != "Bound" => Outcome::Pass(format!("refused: {why}; claim {phase:?}")),
        Ok(Ok(why)) => Outcome::Fail(format!("the reason is given ({why}) but the claim is Bound")),
        Ok(Err(e)) => Outcome::Fail(e),
        Err(e) => Outcome::Fail(format!("a 2Ti claim is stuck without saying why: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_to_the_class_above() {
        assert_eq!(class_range("1"), Some((0, MIB)));
        assert_eq!(class_range("1Mi"), Some((0, MIB)));
        assert_eq!(class_range("1048577"), Some((MIB, 16 * MIB)));
        assert_eq!(class_range("17Mi"), Some((16 * MIB, 64 * MIB)));
        assert_eq!(class_range("1500M"), Some((1024 * MIB, 4096 * MIB)));
        assert_eq!(class_range("3.5Gi"), Some((1024 * MIB, 4096 * MIB)));
        assert_eq!(class_range("600Gi"), Some((256 << 30, 1 << 40)));
        assert_eq!(class_range("1Ti"), Some((256 << 30, 1 << 40)));
        assert_eq!(class_range("2Ti"), None);
    }

    #[test]
    fn every_class_and_the_arbitrary_sizes_are_cases() {
        let c = cases();
        assert_eq!(c.len(), LADDER.len() + 6);
        assert!(c.iter().all(|c| class_range(&c.request).is_some()));
        // Names are unique, and make valid object names.
        let mut names: Vec<String> = c.iter().map(|c| format!("sz-{}", c.name.replace('.', "-"))).collect();
        assert!(names.iter().all(|n| n.chars().all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')));
        names.sort();
        names.dedup();
        assert_eq!(names.len(), c.len());
    }
}
