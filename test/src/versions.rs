//! The `versions` suite (#86): an image tag selects a golden version, so one
//! instance rolls to another golden and back with no node reboot.
//!
//! The component is nextnfs (a service golden every release lists, with a
//! program that exits 0: `/usr/sbin/nextnfs --help`). From the node's release
//! manifest (`/etc/stormcos/release/manifest.json`, read by a pod) the suite
//! knows the release's own build `R`; it picks a version `V` that is not `R`
//! (`RUSTKUBE_NODE_TEST_GOLDEN_VERSION`, else the first of [`CANDIDATES`]
//! that is not the release's).
//!
//! - `version-release`: the manifest lists nextnfs's golden as
//!   `golden-nextnfs-<R>`.
//! - `version-forward`: a pod of `nextnfs:<V>` Succeeds, and its root was
//!   cloned from the version's template `component-nextnfs-<V>`
//!   (`storm.io/golden.main`), not the pallet. Its Events say whether the
//!   node had to fetch it (the registry's "fetching" ErrImagePull) or already
//!   held it from an earlier run.
//! - `version-back`: a pod of `nextnfs:<R>` (the release's tag) Succeeds on
//!   the pallet: its root is not cloned from any `component-nextnfs-…`.
//! - `version-forward-again`: a second pod of `nextnfs:<V>` Succeeds from the
//!   same template with no ErrImagePull at all: the version stays on the node,
//!   so rolling forward again is a clone, not a fetch.
//!
//! On request (`stormcentral test run rustkube-node versions`), in no gate:
//! it needs forge's registry to hold `golden-nextnfs-<V>` (stormcentral#542)
//! and the node to name forge as a peer (stormcos#271).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

pub const COMPONENT: &str = "nextnfs";
const COMMAND: &[&str] = &["/usr/sbin/nextnfs", "--help"];
/// nextnfs builds forge holds as `golden-nextnfs-<sha12>` (0.19.0 and 0.18.0,
/// `stormcentral component images nextnfs`, 2026-11-24). The first that is
/// not the release's is the version rolled to.
pub const CANDIDATES: &[&str] = &["238b1f4020e1", "b3efc2fd7397", "20e09b52faaf"];
const MANIFEST_DIR: &str = "/etc/stormcos/release";
const MARGIN: Duration = Duration::from_secs(60);

/// The release's nextnfs build: the sha12 of its golden asset's provenance.
pub fn release_sha12(manifest: &str) -> Option<String> {
    let doc: Value = serde_json::from_str(manifest).ok()?;
    let prefix = format!("golden-{COMPONENT}-");
    doc["assets"].as_array()?.iter().find_map(|a| {
        (a["kind"] == "golden" && a["name"].as_str() == Some(COMPONENT))
            .then(|| a["provenance"].as_str()?.strip_prefix(&prefix).map(str::to_string))
            .flatten()
    })
}

/// The version to roll to: `wanted` if given, else the first candidate that
/// is not the release's. `None` when only the release's is known.
pub fn pick_version(release: &str, wanted: Option<&str>) -> Option<String> {
    match wanted {
        Some(w) if w != release => Some(w.to_string()),
        Some(_) => None,
        None => CANDIDATES.iter().find(|c| **c != release).map(|c| c.to_string()),
    }
}

/// The engine template a version's root is cloned from (stormblock-registry#116).
pub fn template_of(sha12: &str) -> String {
    format!("component-{COMPONENT}-{sha12}")
}

/// Whether a pod's Events show the registry fetching the version for it.
pub fn saw_fetch(events: &[String]) -> bool {
    events.iter().any(|e| e.to_lowercase().contains("fetch"))
}

/// Whether a pod's Events show any failed pull.
pub fn saw_pull_error(events: &[String]) -> bool {
    events.iter().any(|e| {
        e.contains("ErrImagePull") || e.contains("ImagePullBackOff") || e.starts_with("Failed:") || e.starts_with("BackOff: Back-off pulling")
    })
}

/// A pod of nextnfs at `tag`, running its `--help`.
pub fn version_pod(env: &Env, name: &str, tag: &str) -> Value {
    let mut p = k8s::pod(env, name, &[]);
    let c = &mut p["spec"]["containers"][0];
    c["image"] = json!(format!("{COMPONENT}:{tag}"));
    c["command"] = json!(COMMAND);
    c.as_object_mut().map(|o| o.remove("args"));
    p
}

async fn read_manifest(env: &Env, api: &Api) -> Result<String, Outcome> {
    let name = "version-read-manifest";
    let path = "/release/manifest.json";
    let mut p = k8s::pod(env, name, &["read-file", path]);
    p["spec"]["containers"][0]["volumeMounts"] = json!([{ "name": "release", "mountPath": "/release", "readOnly": true }]);
    p["spec"]["volumes"] = json!([{ "name": "release", "hostPath": { "path": MANIFEST_DIR } }]);
    api.create(&k8s::pods(env), &p).await.map_err(Outcome::Infra)?;
    let done = k8s::pod_done(env, api, name, env.budget(Duration::from_secs(120), MARGIN)).await.map_err(Outcome::Infra)?;
    let log = k8s::log(env, api, name, false).await.map_err(Outcome::Infra)?;
    if done.phase != "Succeeded" {
        return Err(Outcome::Infra(format!("could not read the node's {MANIFEST_DIR}/manifest.json: {log}")));
    }
    let line: Value = log
        .lines()
        .rev()
        .find_map(|l| serde_json::from_str(l).ok())
        .ok_or_else(|| Outcome::Infra(format!("no result line reading the manifest: {log}")))?;
    Ok(s(&line, "/detail").to_string())
}

/// Run a version pod to its end: its phase, its root's golden and its Events.
struct Ran {
    golden: String,
    events: Vec<String>,
    secs: u64,
}

async fn run_pod(env: &Env, api: &Api, name: &str, tag: &str, within: Duration) -> Result<Ran, Outcome> {
    let t = Instant::now();
    api.create(&k8s::pods(env), &version_pod(env, name, tag)).await.map_err(Outcome::Infra)?;
    let done = k8s::pod_done(env, api, name, within).await;
    let events = k8s::events_for(env, api, "Pod", name).await.unwrap_or_default();
    let done = done.map_err(|e| Outcome::Fail(format!("{e}; events: {}", events.join(" | "))))?;
    if done.phase != "Succeeded" {
        let said = k8s::said(env, api, name, &done).await;
        return Err(Outcome::Fail(format!("{COMPONENT}:{tag} ended {}: {said}; events: {}", done.phase, events.join(" | "))));
    }
    let secs = t.elapsed().as_secs();
    // Written soon after the start, off the pod's worker: it may trail the end.
    let path = format!("{}/{name}", k8s::pods(env));
    let golden = k8s::until(Duration::from_secs(30), || {
        let path = path.clone();
        async move {
            let p = api.get(&path).await?.ok_or("the pod is gone")?;
            let g = s(&p, "/metadata/annotations/storm.io~1golden.main").to_string();
            if g.is_empty() {
                return Err("no storm.io/golden.main annotation".to_string());
            }
            Ok(g)
        }
    })
    .await
    .map_err(|e| Outcome::Fail(format!("{COMPONENT}:{tag} Succeeded, but what its root was cloned from is not known: {e}")))?;
    Ok(Ran { golden, events, secs })
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let t = Instant::now();
    let names = ["version-release", "version-forward", "version-back", "version-forward-again"];
    let skip_rest = |r: &mut Report, from: usize, why: &str| {
        for n in &names[from..] {
            r.record(n, Outcome::Skip(why.to_string()), 0, None);
        }
    };
    let manifest = match read_manifest(&env, &api).await {
        Ok(m) => m,
        Err(o) => {
            r.record(names[0], o, t.elapsed().as_millis(), None);
            skip_rest(r, 1, "the release manifest could not be read");
            return cleanup(&env, &api, r).await;
        }
    };
    let Some(release) = release_sha12(&manifest) else {
        r.record(names[0], Outcome::Skip(format!("the release manifest lists no golden-{COMPONENT}-<sha12>")), 0, None);
        skip_rest(r, 1, "no release version of nextnfs to roll from");
        return cleanup(&env, &api, r).await;
    };
    r.record(names[0], Outcome::Pass(format!("the release's {COMPONENT} is golden-{COMPONENT}-{release}")), t.elapsed().as_millis(), None);

    let wanted = std::env::var("RUSTKUBE_NODE_TEST_GOLDEN_VERSION").ok().filter(|v| !v.is_empty());
    let Some(version) = pick_version(&release, wanted.as_deref()) else {
        skip_rest(r, 1, &format!("no {COMPONENT} version other than the release's ({release}) is known"));
        return cleanup(&env, &api, r).await;
    };
    let template = template_of(&version);

    // Forward: the version, fetched on demand (or held from an earlier run).
    let t = Instant::now();
    let forward = run_pod(&env, &api, "version-forward", &version, env.budget(Duration::from_secs(600), MARGIN)).await;
    let forward = match forward {
        Err(o) => o,
        Ok(ran) if !ran.golden.contains(&template) => {
            Outcome::Fail(format!("{COMPONENT}:{version} ran, but its root was cloned from {:?}, not {template}", ran.golden))
        }
        Ok(ran) => Outcome::Pass(format!(
            "{COMPONENT}:{version} Succeeded in {} s on a clone of {}; {}",
            ran.secs,
            ran.golden,
            if saw_fetch(&ran.events) { "fetched from a peer first" } else { "the node already held it (an earlier run fetched it)" }
        )),
    };
    let forward_ok = matches!(forward, Outcome::Pass(_));
    r.record(names[1], forward, t.elapsed().as_millis(), None);
    if !forward_ok {
        skip_rest(r, 2, "version-forward did not pass");
        return cleanup(&env, &api, r).await;
    }

    // Back: the release's tag runs the pallet.
    let t = Instant::now();
    let back = match run_pod(&env, &api, "version-back", &release, env.budget(Duration::from_secs(180), MARGIN)).await {
        Err(o) => o,
        Ok(ran) if ran.golden.contains(&format!("component-{COMPONENT}-")) => {
            Outcome::Fail(format!("{COMPONENT}:{release} (the release's) was cloned from {:?}, not the pallet", ran.golden))
        }
        Ok(ran) => Outcome::Pass(format!("{COMPONENT}:{release} Succeeded in {} s on the pallet ({})", ran.secs, ran.golden)),
    };
    r.record(names[2], back, t.elapsed().as_millis(), None);

    // Forward again: no fetch, no pull error.
    let t = Instant::now();
    let again = match run_pod(&env, &api, "version-forward-again", &version, env.budget(Duration::from_secs(180), MARGIN)).await {
        Err(o) => o,
        Ok(ran) if !ran.golden.contains(&template) => {
            Outcome::Fail(format!("{COMPONENT}:{version} again was cloned from {:?}, not {template}", ran.golden))
        }
        Ok(ran) if saw_pull_error(&ran.events) => Outcome::Fail(format!(
            "{COMPONENT}:{version} again had a failed pull, so the version was not kept: {}",
            ran.events.join(" | ")
        )),
        Ok(ran) => Outcome::Pass(format!("{COMPONENT}:{version} again Succeeded in {} s on {} with no fetch", ran.secs, ran.golden)),
    };
    r.record(names[3], again, t.elapsed().as_millis(), None);
    cleanup(&env, &api, r).await;
}

async fn cleanup(env: &Env, api: &Api, r: &mut Report) {
    if let Err(e) = k8s::drain(env, api, Duration::from_secs(30)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"{"assets":[
        {"kind":"golden","name":"stormd","provenance":"golden-stormd-aaaaaaaaaaaa"},
        {"kind":"golden","name":"nextnfs","digest":"sha256:00","provenance":"golden-nextnfs-20e09b52faaf"}]}"#;

    #[test]
    fn the_release_version_is_read_from_the_manifest() {
        assert_eq!(release_sha12(MANIFEST).as_deref(), Some("20e09b52faaf"));
        assert_eq!(release_sha12(r#"{"assets":[]}"#), None);
        assert_eq!(release_sha12("not json"), None);
    }

    #[test]
    fn the_version_rolled_to_is_never_the_releases() {
        assert_eq!(pick_version("20e09b52faaf", None).as_deref(), Some("238b1f4020e1"));
        assert_eq!(pick_version("238b1f4020e1", None).as_deref(), Some("b3efc2fd7397"));
        assert_eq!(pick_version("20e09b52faaf", Some("adde4303ad50")).as_deref(), Some("adde4303ad50"));
        assert_eq!(pick_version("adde4303ad50", Some("adde4303ad50")), None);
    }

    #[test]
    fn a_version_pod_runs_the_tag_and_the_fetch_is_seen_in_events() {
        let env = crate::k8s::tests::env();
        let p = version_pod(&env, "v", "238b1f4020e1");
        let c = &p["spec"]["containers"][0];
        assert_eq!(c["image"], "nextnfs:238b1f4020e1");
        assert_eq!(c["command"][0], "/usr/sbin/nextnfs");
        assert!(c.get("args").is_none());
        assert_eq!(template_of("238b1f4020e1"), "component-nextnfs-238b1f4020e1");
        let fetching = vec!["Failed: ErrImagePull: registry is fetching it from forge".to_string()];
        assert!(saw_fetch(&fetching) && saw_pull_error(&fetching));
        let clean = vec!["Pulled: image nextnfs:238b1f4020e1".to_string(), "Started: main".to_string()];
        assert!(!saw_fetch(&clean) && !saw_pull_error(&clean));
    }
}
