//! `medium` (< 30 min): claims of the built-in `stormblock` class, end to
//! end through the API (#64).
//!
//! **Sizes.** One claim at every class of the ladder the kubelet offers
//! (`pkg/kubelet/src/storage.rs` `SIZE_CLASSES`), and arbitrary sizes: the
//! smallest request (1 byte), just over a class (1Mi+1, 17Mi), decimal and
//! fractional units (1500M, 3.5Gi), and 600Gi. Each, [`SIZE_CASES_AT_ONCE`] at a time, the largest first:
//!
//! 1. a claim and a pod that mounts it, writes 64 KiB, reads it back, and
//!    checks that the filesystem's size is in (the class below, the class the
//!    request rounds to];
//! 2. the claim is Bound and its status capacity is that class;
//! 3. pod and claim deleted, and the PV is gone (reclaimed: the clone deleted
//!    on the node).
//!
//! **Raw block** (`volumeMode: Block`, #67): claims of 1Mi, 20Ti (the 64Ti
//! class) and 1Pi, each a device at `volumeDevices[].devicePath`; the pod
//! writes and reads back at offset 0 and the device is exactly its class.
//! A 20Ti *filesystem* claim rounds to 64Ti, which has no ext4 blank yet: it
//! waits with the reason, which names `volumeMode: Block`.
//!
//! **Above the ladder** (2Pi): refused, not stuck. The pod waits with the
//! reason ("larger than the largest size class") on its status or in an
//! Event, and the claim is never Bound.
//!
//! **Minting.** A class with no blank on the node is minted on first use,
//! which for 1 TiB is minutes of formatting (stormblock#141). Every case has
//! [`Env::mint_budget`] to finish; the `ms` of the 1Ti case is that time.
//!
//! **Overcommit refused** (#62): the test node's published CSIStorageCapacity
//! (`kube-system/stormblock-<node>`) says the largest class that still fits;
//! a claim one class above it, for a pod pinned to that node, must wait with
//! the reason ("not enough room", or the scheduler's "no published storage
//! capacity") and never bind. Skip when the node can still take the largest
//! class.

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
    ("4Ti", 4 * 1024 * 1024 * MIB),
    ("16Ti", 16 * 1024 * 1024 * MIB),
];

/// The classes past [`LADDER`]: raw block only, until stormblock can format
/// ext4 at these sizes (#67).
pub const BLOCK_ONLY: &[(&str, u64)] = &[
    ("64Ti", 64 * 1024 * 1024 * MIB),
    ("256Ti", 256 * 1024 * 1024 * MIB),
    ("1Pi", 1024 * 1024 * 1024 * MIB),
];

/// Raw block claims: a test name and the request.
pub const BLOCK_CASES: &[(&str, &str)] = &[("1mi", "1Mi"), ("20ti", "20Ti"), ("1pi", "1Pi")];

/// One claim: a test name and the request, verbatim, and whether it is a raw
/// block claim.
pub struct Case {
    pub name: String,
    pub request: String,
    pub block: bool,
}

/// Every class, then the arbitrary sizes. 2Ti is separate (`above-ladder`).
pub fn cases() -> Vec<Case> {
    let mut v: Vec<Case> = LADDER
        .iter()
        .map(|(q, _)| Case { name: format!("class-{}", q.to_ascii_lowercase()), request: q.to_string(), block: false })
        .collect();
    for (name, request) in [
        ("smallest", "1"),
        ("1mi-plus-1", "1048577"),
        ("17mi", "17Mi"),
        ("1500m", "1500M"),
        ("3.5gi", "3.5Gi"),
        ("600gi", "600Gi"),
    ] {
        v.push(Case { name: name.into(), request: request.into(), block: false });
    }
    for (name, request) in BLOCK_CASES {
        v.push(Case { name: format!("block-{name}"), request: request.to_string(), block: true });
    }
    v
}

/// The class a request rounds to, and the class below it (0 for the first):
/// the filesystem must be larger than `lo` and at most `hi`. `None` above
/// the ladder (for a filesystem, the ext4 classes; for a block claim, all).
pub fn class_range(request: &str, block: bool) -> Option<(u64, u64)> {
    let want = api::quantity_bytes(request)?;
    let all: Vec<(&str, u64)> = LADDER.iter().chain(if block { BLOCK_ONLY } else { &[] }).copied().collect();
    let i = all.iter().position(|(_, b)| *b >= want)?;
    Some((if i == 0 { 0 } else { all[i - 1].1 }, all[i].1))
}

/// The pod cases (`pods.rs`), started by the caller alongside these: run in
/// parallel with the storage cases, and awaited before the final drain,
/// which deletes every pod of the run.
pub type PodCases = tokio::task::JoinHandle<Vec<(String, Outcome, u128)>>;

/// Record the pod cases' results.
pub async fn record_pod_cases(cases: PodCases, r: &mut Report) {
    match cases.await {
        Ok(v) => {
            for (name, o, ms) in v {
                r.record(&name, o, ms, None);
            }
        }
        Err(e) => {
            r.record("pod-cases", Outcome::Infra(format!("panicked: {e}")), 0, None);
        }
    }
}

/// Size cases running at once (#64). All twenty at once (every class blank a
/// format, claims up to 16Ti and 1Pi) took the node's apiserver down twice
/// (fff1f4d9d9 on the Dell, 63a3c5201b on server3): this suite checks
/// behaviour, it is not a stress test. The `ms` of a case counts from when it
/// started, not from when it was queued.
pub const SIZE_CASES_AT_ONCE: usize = 3;

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report, pod_cases: PodCases) {
    // A few at a time, the largest first: a 1 TiB mint may take much of the
    // 30 minutes, and it should not wait behind the small classes.
    let gate = Arc::new(tokio::sync::Semaphore::new(SIZE_CASES_AT_ONCE));
    let mut ordered: Vec<(usize, Case)> = cases().into_iter().enumerate().collect();
    ordered.sort_by_key(|(_, c)| std::cmp::Reverse(api::quantity_bytes(&c.request).unwrap_or(0)));
    let mut tasks = Vec::new();
    for (i, c) in ordered {
        let (env, api, gate) = (env.clone(), api.clone(), gate.clone());
        tasks.push(tokio::spawn(async move {
            let _turn = gate.acquire_owned().await;
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
    let unformatted = {
        let (env, api) = (env.clone(), api.clone());
        tokio::spawn(async move {
            let t = Instant::now();
            let o = refused(&env, &api, "sz-fs-20ti", "20Ti", &["volumeMode: Block"], false).await;
            ("pvc-size-filesystem-past-ext4-classes".to_string(), o, t.elapsed().as_millis())
        })
    };
    tasks.push(unformatted);
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
    let t = Instant::now();
    let o = overcommit_refused(&env, &api).await;
    r.record("pvc-overcommit-refused", o, t.elapsed().as_millis(), None);
    // The node's own volumes (#59): complete pairs, and a deleted claim back.
    let t = Instant::now();
    let o = crate::node_volumes::pairs(&env, &api).await;
    r.record("node-volumes-pairs", o, t.elapsed().as_millis(), None);
    let t = Instant::now();
    let o = crate::node_volumes::restored(&env, &api, within(&env, Duration::from_secs(120))).await;
    r.record("node-volumes-restored", o, t.elapsed().as_millis(), None);
    record_pod_cases(pod_cases, r).await;

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
    let Some((lo, hi)) = class_range(&c.request, c.block) else {
        return Outcome::Infra(format!("{} does not round to a class here", c.request));
    };
    let name = format!("sz-{}", c.name.replace('.', "-"));
    let claim = if c.block { k8s::block_claim(env, &name, &c.request) } else { k8s::claim(env, &name, &c.request) };
    if let Err(e) = api.create(&k8s::pvcs(env), &claim).await {
        return Outcome::Infra(e);
    }
    // A raw device is exactly its class: (hi - 1, hi].
    let lo = if c.block { hi - 1 } else { lo };
    let args = [seed.to_string(), BYTES.to_string(), lo.to_string(), hi.to_string()];
    let pod = if c.block {
        k8s::work_device(env, &name, &name, "sized", &args)
    } else {
        k8s::work(env, &name, &name, "sized", &args)
    };
    if let Err(e) = api.create(&k8s::pods(env), &pod).await {
        return Outcome::Infra(e);
    }
    let done = match k8s::pod_done(env, api, &name, within(env, env.mint_budget)).await {
        Ok(d) => d,
        Err(e) => return Outcome::Fail(e),
    };
    if done.phase != "Succeeded" {
        return Outcome::Fail(format!("request {}: pod failed on {}: {}", c.request, done.node, done.message));
    }
    let class = LADDER.iter().chain(BLOCK_ONLY).find(|(_, b)| *b == hi).map(|(q, _)| *q).unwrap_or("?");

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

/// 2Pi: the pod waits with the reason, and the claim is never Bound.
async fn above_ladder(env: &Env, api: &Api) -> Outcome {
    refused(env, api, "sz-above-ladder", "2Pi", &["larger than the largest size class"], false).await
}

/// One class above what the test node says it can still take (#62).
async fn overcommit_refused(env: &Env, api: &Api) -> Outcome {
    let path = format!("/apis/storage.k8s.io/v1/namespaces/kube-system/csistoragecapacities/stormblock-{}", env.node_name);
    let cap = match api.get(&path).await {
        Ok(Some(c)) => c,
        Ok(None) => return Outcome::Fail(format!("node {} publishes no CSIStorageCapacity ({path})", env.node_name)),
        Err(e) => return Outcome::Infra(e),
    };
    let max = api::quantity_bytes(s(&cap, "/maximumVolumeSize")).unwrap_or(0);
    let all: Vec<(&str, u64)> = LADDER.iter().chain(BLOCK_ONLY).copied().collect();
    let Some((request, _)) = all.iter().find(|(_, b)| *b > max) else {
        return Outcome::Skip(format!("node {} can still take the largest class (maximumVolumeSize {max})", env.node_name));
    };
    // Block, so a class past the ext4 ones is refused for room, not for
    // having no filesystem.
    let why = ["not enough room", "no published storage capacity"];
    match refused(env, api, "sz-overcommit", request, &why, true).await {
        Outcome::Pass(d) => Outcome::Pass(format!("{request} with maximumVolumeSize {}: {d}", s(&cap, "/maximumVolumeSize"))),
        other => other,
    }
}

/// A claim of `request` that the node refuses: the pod waits with a reason
/// containing one of `why`, and the claim is never Bound. `pinned_block`: a
/// Block claim, its pod pinned to the test node (so a scheduler that does not
/// read capacity cannot place it elsewhere).
async fn refused(env: &Env, api: &Api, name: &str, request: &str, why: &[&str], pinned_block: bool) -> Outcome {
    let claim = if pinned_block { k8s::block_claim(env, name, request) } else { k8s::claim(env, name, request) };
    if let Err(e) = api.create(&k8s::pvcs(env), &claim).await {
        return Outcome::Infra(e);
    }
    let args = ["1".to_string(), BYTES.to_string(), "0".to_string(), u64::MAX.to_string()];
    let mut pod = if pinned_block {
        k8s::work_device(env, name, name, "sized", &args)
    } else {
        k8s::work(env, name, name, "sized", &args)
    };
    if pinned_block {
        pod["spec"]["nodeSelector"] = serde_json::json!({ "kubernetes.io/hostname": env.node_name });
    }
    if let Err(e) = api.create(&k8s::pods(env), &pod).await {
        return Outcome::Infra(e);
    }
    let pod_path = format!("{}/{name}", k8s::pods(env));
    let path = pod_path.as_str();
    let seen = k8s::until(within(env, Duration::from_secs(180)), || async move {
        let p = api.get(path).await?.ok_or_else(|| "the pod is gone".to_string())?;
        let phase = s(&p, "/status/phase");
        if phase == "Succeeded" || phase == "Failed" || phase == "Running" {
            return Ok(Err(format!("the pod reached {phase}: a {request} claim was given a volume")));
        }
        let status = format!("{}{}", s(&p, "/status/message"), k8s::waiting(&p));
        if why.iter().any(|w| status.contains(w)) {
            return Ok(Ok(format!("pod status: {status}")));
        }
        let mut evs = k8s::events_for(env, api, "Pod", name).await?;
        evs.extend(k8s::events_for(env, api, "PersistentVolumeClaim", name).await?);
        match evs.iter().find(|e| why.iter().any(|w| e.contains(w))) {
            Some(e) => Ok(Ok(format!("event: {e}"))),
            None => Err(format!("no reason containing any of {why:?} yet (pod {phase:?}{}; events: {evs:?})", k8s::waiting(&p))),
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
        Err(e) => Outcome::Fail(format!("a {request} claim is stuck without saying why: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_to_the_class_above() {
        let fs = |q| class_range(q, false);
        assert_eq!(fs("1"), Some((0, MIB)));
        assert_eq!(fs("1Mi"), Some((0, MIB)));
        assert_eq!(fs("1048577"), Some((MIB, 16 * MIB)));
        assert_eq!(fs("17Mi"), Some((16 * MIB, 64 * MIB)));
        assert_eq!(fs("1500M"), Some((1024 * MIB, 4096 * MIB)));
        assert_eq!(fs("3.5Gi"), Some((1024 * MIB, 4096 * MIB)));
        assert_eq!(fs("600Gi"), Some((256 << 30, 1 << 40)));
        assert_eq!(fs("1Ti"), Some((256 << 30, 1 << 40)));
        assert_eq!(fs("2Ti"), Some((1 << 40, 4 << 40)));
        assert_eq!(fs("16Ti"), Some((4 << 40, 16 << 40)));
        assert_eq!(fs("20Ti"), None, "no ext4 class past 16Ti yet");
        assert_eq!(class_range("20Ti", true), Some((16 << 40, 64 << 40)));
        assert_eq!(class_range("1Pi", true), Some((256 << 40, 1 << 50)));
        assert_eq!(class_range("2Pi", true), None);
    }

    #[test]
    fn every_class_and_the_arbitrary_sizes_are_cases() {
        let c = cases();
        assert_eq!(c.len(), LADDER.len() + 6 + BLOCK_CASES.len());
        assert!(c.iter().all(|c| class_range(&c.request, c.block).is_some()));
        // Names are unique, and make valid object names.
        let mut names: Vec<String> = c.iter().map(|c| format!("sz-{}", c.name.replace('.', "-"))).collect();
        assert!(names.iter().all(|n| n.chars().all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')));
        names.sort();
        names.dedup();
        assert_eq!(names.len(), c.len());
    }
}
