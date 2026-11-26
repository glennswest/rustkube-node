//! The `pvc` suite (#379): PVC management on the built-in class, as `oc`
//! does it — a claim grown online while a pod holds it, a `VolumeSnapshot` of
//! it, and that snapshot restored into a larger claim.
//!
//! - `pvc-classes`: StorageClass `stormblock` says `allowVolumeExpansion:
//!   true` and a `VolumeSnapshotClass` of driver `stormblock.storm.io` exists
//!   (stormcos#1032). Without them the rest is a skip, naming what is missing.
//! - `pvc-grow`: a 64Mi claim held by a pod that wrote a pattern to it is
//!   patched to 256Mi. The claim ends with `status.capacity.storage` 256Mi and
//!   no resize condition; the pod sees its mounted filesystem past 200 MiB
//!   (statvfs, as `df` reads it) and its pattern intact; the claim carries the
//!   `Resizing` and `FileSystemResizeSuccessful` events.
//! - `pvc-shrink-refused`: patching it back to 64Mi is refused by the
//!   apiserver.
//! - `pvc-snapshot`: a `VolumeSnapshot` of the grown claim is `readyToUse`
//!   with a `restoreSize` of at least 256Mi.
//! - `pvc-restore`: a 512Mi claim with that snapshot as its `dataSource` is
//!   restored: a pod reads the pattern back and sees its filesystem grow past
//!   400 MiB.
//! - `pvc-snapshot-deleted`: deleting the snapshot deletes its content
//!   (`deletionPolicy: Delete`).
//! - `pvc-tier-fast` (#400): where StorageClass `stormblock-fast` is (tier
//!   `fast`, stormcos#1039), a 64Mi claim of it binds, a pod writes and reads
//!   it, and its `ProvisioningSucceeded` event says `its writes on tier fast`
//!   (the kubelet sent the tier on the blank's clone). A skip where the class
//!   is not there (a node with no SSD beside a rotating system drive).
//! - `pvc-volume-stats` (#469): a pod holding a 64Mi claim reports its
//!   `statvfs` (`df -B1`, `df -i`); the node's `/stats/summary` (asked of its
//!   kubelet at the node's InternalIP with the run's ServiceAccount token, as
//!   `nodes/proxy`, a subresource, may not be granted to a suite) gives that
//!   claim's `availableBytes`, `inodes`
//!   and `inodesFree` for the same mount: inodes equal, available bytes within
//!   1 MiB. What `/metrics`' `kubelet_volume_stats_available_bytes` and
//!   `_inodes*` are rendered from.
//! - `pvc-volume-metrics` (#456, #469): the same claim, still held, on the
//!   kubelet's `/metrics` (asked the same way): all six
//!   `kubelet_volume_stats_*` families labelled `namespace` and
//!   `persistentvolumeclaim` (what `oc adm top persistentvolumeclaims`
//!   reads): capacity at least the claim's 64Mi, used at least the 4 MiB the
//!   pod wrote, available within 1 MiB of its `df`, inodes and inodes free
//!   equal to it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::{Env, CLASS, PROVISIONER};
use crate::k8s::{self, MIB};
use crate::report::{Outcome, Report};

const MARGIN: Duration = Duration::from_secs(20);
const SNAP_API: &str = "/apis/snapshot.storage.k8s.io/v1";
/// The fast tier's class (stormcos#1039) and the tier it names (#400).
pub const FAST_CLASS: &str = "stormblock-fast";
pub const FAST_TIER: &str = "fast";
const SEED: u64 = 379;
const BYTES: u64 = 8 * MIB;

fn snapshots(env: &Env) -> String {
    format!("{SNAP_API}/namespaces/{}/volumesnapshots", env.namespace)
}

/// A `VolumeSnapshot` of claim `claim` in class `class`.
pub fn snapshot(env: &Env, name: &str, claim: &str, class: &str) -> Value {
    json!({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
        "metadata": { "name": name, "namespace": env.namespace, "labels": k8s::labels(env) },
        "spec": { "volumeSnapshotClassName": class, "source": { "persistentVolumeClaimName": claim } },
    })
}

/// A claim restored from snapshot `snap`.
pub fn restored_claim(env: &Env, name: &str, request: &str, snap: &str) -> Value {
    let mut c = k8s::claim(env, name, request);
    c["spec"]["dataSource"] = json!({ "apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": snap });
    c
}

/// A 64Mi claim of [`FAST_CLASS`].
pub fn fast_claim(env: &Env, name: &str) -> Value {
    let mut c = k8s::claim(env, name, "64Mi");
    c["spec"]["storageClassName"] = json!(FAST_CLASS);
    c
}

/// Why StorageClass `sc` (the [`FAST_CLASS`] read) is no fast tier of the
/// built-in driver, or `None` when it is.
pub fn fast_class_missing(sc: Option<&Value>) -> Option<String> {
    match sc {
        None => Some(format!("no StorageClass {FAST_CLASS} (stormcos#1039: a node with an SSD beside a rotating system drive)")),
        Some(c) if s(c, "/provisioner") != PROVISIONER => Some(format!("StorageClass {FAST_CLASS} is {:?}'s", s(c, "/provisioner"))),
        Some(c) if s(c, "/parameters/tier") != FAST_TIER => {
            Some(format!("StorageClass {FAST_CLASS} names tier {:?}, not {FAST_TIER}", s(c, "/parameters/tier")))
        }
        Some(_) => None,
    }
}

/// The pod pinned to the node holding `claim`, running `grows` on it: the
/// pattern written (or read back), then its filesystem waited on until it
/// holds `want` bytes.
pub fn grows_pod(env: &Env, name: &str, claim: &str, want: u64) -> Value {
    let args = [SEED.to_string(), BYTES.to_string(), want.to_string(), "600".to_string()];
    let mut p = k8s::work(env, name, claim, "grows", &args);
    p["spec"]["nodeName"] = json!(env.node_name);
    p
}

/// Whether the classes #379 needs are here: `None` when they are, else why not.
pub fn classes_missing(sc: Option<&Value>, vscs: &[Value]) -> Option<String> {
    let mut why = Vec::new();
    match sc {
        None => why.push(format!("no StorageClass {CLASS}")),
        Some(c) if s(c, "/provisioner") != PROVISIONER => why.push(format!("StorageClass {CLASS} is {:?}'s", s(c, "/provisioner"))),
        Some(c) if c["allowVolumeExpansion"] != json!(true) => {
            why.push(format!("StorageClass {CLASS} does not allow volume expansion (stormcos#1032)"))
        }
        Some(_) => {}
    }
    if !vscs.iter().any(|v| s(v, "/driver") == PROVISIONER) {
        why.push(format!("no VolumeSnapshotClass of driver {PROVISIONER} (stormcos#1032)"));
    }
    (!why.is_empty()).then(|| why.join("; "))
}

/// The pod's `df-hold` line (#469): its `statvfs` of the claim.
pub fn df_said(log: &str) -> Option<Value> {
    log.lines().rev().find_map(|l| {
        let line: Value = serde_json::from_str(l.trim()).ok()?;
        if line["ok"] != json!(true) {
            return None;
        }
        serde_json::from_str(line["detail"].as_str()?).ok()
    })
}

/// Pod `pod`'s volume `volume` in a `/stats/summary`, once it carries
/// `availableBytes`.
pub fn summary_volume<'a>(summary: &'a Value, namespace: &str, pod: &str, volume: &str) -> Option<&'a Value> {
    summary["pods"].as_array()?.iter()
        .find(|p| s(p, "/podRef/namespace") == namespace && s(p, "/podRef/name") == pod)?["volume"]
        .as_array()?
        .iter()
        .find(|v| s(v, "/name") == volume && v["availableBytes"].is_u64())
}

/// The pod's `df` against the kubelet's numbers for the same mount: inodes
/// equal, available bytes within 1 MiB (the claim is idle between the two).
pub fn df_matches(df: &Value, vol: &Value) -> Result<String, String> {
    let n = |v: &Value, k: &str| v[k].as_u64().ok_or_else(|| format!("no {k} in {v}"));
    let (avail, kavail) = (n(df, "availableBytes")?, n(vol, "availableBytes")?);
    let (inodes, kinodes) = (n(df, "inodes")?, n(vol, "inodes")?);
    let (free, kfree) = (n(df, "inodesFree")?, n(vol, "inodesFree")?);
    let used = n(vol, "inodesUsed")?;
    let said = format!(
        "df: {avail} B available, {inodes} inodes, {free} free; kubelet: {kavail} B, {kinodes} inodes, {kfree} free, {used} used"
    );
    if inodes != kinodes || free != kfree || used != kinodes.saturating_sub(kfree) || avail.abs_diff(kavail) > MIB {
        return Err(format!("the kubelet's numbers are not the pod's: {said}"));
    }
    Ok(said)
}

/// The families `pvc-volume-metrics` reads (#456, #469).
pub const VOLUME_FAMILIES: [&str; 6] =
    ["capacity_bytes", "used_bytes", "available_bytes", "inodes", "inodes_free", "inodes_used"];

/// Claim `claim` (in `namespace`)'s `kubelet_volume_stats_<family>` values in
/// a Prometheus text exposition.
pub fn volume_series(text: &str, namespace: &str, claim: &str) -> std::collections::BTreeMap<String, f64> {
    let labels = |l: &str| {
        let has = |k: &str, v: &str| l.split(',').any(|kv| kv.trim() == format!("{k}=\"{v}\""));
        has("namespace", namespace) && has("persistentvolumeclaim", claim)
    };
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("kubelet_volume_stats_")?;
            let (family, rest) = rest.split_once('{')?;
            let (l, value) = rest.split_once('}')?;
            if !labels(l) {
                return None;
            }
            Some((family.to_string(), value.split_whitespace().next()?.parse().ok()?))
        })
        .collect()
}

/// The claim's series against the pod's `df` (#456, #469).
pub fn metrics_match(series: &std::collections::BTreeMap<String, f64>, df: &Value, claim_bytes: u64, written: u64) -> Result<String, String> {
    let missing: Vec<&str> = VOLUME_FAMILIES.iter().copied().filter(|f| !series.contains_key(*f)).collect();
    if !missing.is_empty() {
        return Err(format!("no kubelet_volume_stats_{{{}}} for the claim; it has {:?}", missing.join(","), series));
    }
    let v = |f: &str| series[f] as u64;
    let n = |k: &str| df[k].as_u64().ok_or_else(|| format!("no {k} in the pod's df {df}"));
    let said = format!("{series:?}; df {df}");
    if v("capacity_bytes") < claim_bytes {
        return Err(format!("capacity below the claim's {claim_bytes}: {said}"));
    }
    if v("used_bytes") < written {
        return Err(format!("used below the {written} bytes the pod wrote: {said}"));
    }
    if v("available_bytes").abs_diff(n("availableBytes")?) > MIB || v("inodes") != n("inodes")? || v("inodes_free") != n("inodesFree")? {
        return Err(format!("not the pod's df: {said}"));
    }
    Ok(said)
}

struct Suite {
    env: Arc<Env>,
    api: Api,
}

impl Suite {
    fn claim_path(&self, name: &str) -> String {
        format!("{}/{name}", k8s::pvcs(&self.env))
    }

    /// Set a claim's request (a read-modify-write: the test's Api has no patch).
    async fn request(&self, name: &str, request: &str) -> Result<(), String> {
        let path = self.claim_path(name);
        let mut c = self.api.get(&path).await?.ok_or_else(|| format!("claim {name} is gone"))?;
        c["spec"]["resources"]["requests"]["storage"] = json!(request);
        self.api.put(&path, &c).await.map(|_| ())
    }

    /// Wait for a claim's capacity to reach `bytes` with no resize pending.
    async fn grown(&self, name: &str, bytes: u64, within: Duration) -> Result<Value, String> {
        let path = self.claim_path(name);
        k8s::until(within, || {
            let path = path.clone();
            async move {
                let c = self.api.get(&path).await?.ok_or_else(|| format!("claim {name} is gone"))?;
                let have = crate::api::quantity_bytes(s(&c, "/status/capacity/storage")).unwrap_or(0);
                let pending: Vec<String> = c["status"]["conditions"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|x| x["status"] == "True")
                    .map(|x| format!("{} {}", s(x, "/type"), s(x, "/message")))
                    .collect();
                if have >= bytes && pending.is_empty() {
                    Ok(c)
                } else {
                    Err(format!(
                        "claim {name}: capacity {:?}, conditions {pending:?}, allocatedResourceStatuses {}",
                        s(&c, "/status/capacity/storage"),
                        c["status"]["allocatedResourceStatuses"]
                    ))
                }
            }
        })
        .await
    }

    /// The pod's result line: `grows`'s detail, or why it failed.
    async fn finished(&self, pod: &str, within: Duration) -> Result<String, String> {
        let done = k8s::pod_done(&self.env, &self.api, pod, within).await?;
        let message = if done.message.is_empty() {
            k8s::log(&self.env, &self.api, pod, false).await.unwrap_or_default()
        } else {
            done.message
        };
        let line: Value = serde_json::from_str(message.trim().lines().last().unwrap_or("")).unwrap_or(Value::Null);
        match (done.phase.as_str(), line["ok"].as_bool()) {
            ("Succeeded", Some(true)) => Ok(s(&line, "/detail").to_string()),
            _ => Err(format!("pod {pod} {}: {}", done.phase, message.trim())),
        }
    }

    async fn grow(&self) -> Outcome {
        let within = self.env.budget(Duration::from_secs(300), MARGIN);
        if let Err(e) = self.api.create(&k8s::pvcs(&self.env), &k8s::claim(&self.env, "grow", "64Mi")).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &grows_pod(&self.env, "grow-pod", "grow", 200 * MIB)).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = k8s::pod_running(&self.env, &self.api, "grow-pod", within).await {
            return Outcome::Fail(format!("the pod holding the claim did not start: {e}"));
        }
        let t = Instant::now();
        if let Err(e) = self.request("grow", "256Mi").await {
            return Outcome::Fail(format!("growing the claim to 256Mi was refused: {e}"));
        }
        let claim = match self.grown("grow", 256 * MIB, within).await {
            Ok(c) => c,
            Err(e) => {
                // What the node said, so a stuck claim names its step.
                let events = k8s::events_for(&self.env, &self.api, "PersistentVolumeClaim", "grow").await.unwrap_or_default();
                return Outcome::Fail(format!("{e}; events: {events:?}"));
            }
        };
        let took = t.elapsed().as_secs();
        let pod = match self.finished("grow-pod", within).await {
            Ok(d) => d,
            Err(e) => return Outcome::Fail(format!("the claim says 256Mi, but in the pod: {e}")),
        };
        let events = k8s::events_for(&self.env, &self.api, "PersistentVolumeClaim", "grow").await.unwrap_or_default();
        for want in ["Resizing", "FileSystemResizeSuccessful"] {
            if !events.iter().any(|e| e.starts_with(&format!("{want}:"))) {
                return Outcome::Fail(format!("no {want} event on the claim: {events:?}"));
            }
        }
        Outcome::Pass(format!(
            "64Mi → {} in {took} s; pod: {pod}; events: {}",
            s(&claim, "/status/capacity/storage"),
            events.join(" | ")
        ))
    }

    async fn shrink_refused(&self) -> Outcome {
        match self.request("grow", "64Mi").await {
            Ok(()) => Outcome::Fail("the apiserver took a shrink of claim grow to 64Mi".into()),
            Err(e) if e.contains("403") || e.contains("422") || e.to_lowercase().contains("forbidden") => Outcome::Pass(e),
            Err(e) => Outcome::Infra(e),
        }
    }

    async fn snapshot(&self, class: &str) -> Outcome {
        let within = self.env.budget(Duration::from_secs(180), MARGIN);
        if let Err(e) = self.api.create(&snapshots(&self.env), &snapshot(&self.env, "grow-snap", "grow", class)).await {
            return Outcome::Infra(e);
        }
        let path = format!("{}/grow-snap", snapshots(&self.env));
        let got = k8s::until(within, || {
            let path = path.clone();
            async move {
                let v = self.api.get(&path).await?.ok_or("VolumeSnapshot grow-snap is gone")?;
                if let Some(e) = v.pointer("/status/error/message").and_then(Value::as_str) {
                    return Ok(Err(e.to_string()));
                }
                if v["status"]["readyToUse"] == json!(true) {
                    Ok(Ok(v))
                } else {
                    Err(format!("VolumeSnapshot grow-snap: status {}", v["status"]))
                }
            }
        })
        .await;
        let v = match got {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Outcome::Fail(format!("the snapshot was refused: {e}")),
            Err(e) => return Outcome::Fail(e),
        };
        let size = match &v["status"]["restoreSize"] {
            Value::String(q) => crate::api::quantity_bytes(q).unwrap_or(0),
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        };
        if size < 256 * MIB {
            return Outcome::Fail(format!("readyToUse, but restoreSize is {} (< 256Mi)", v["status"]["restoreSize"]));
        }
        Outcome::Pass(format!(
            "readyToUse, restoreSize {}, content {}",
            v["status"]["restoreSize"],
            s(&v, "/status/boundVolumeSnapshotContentName")
        ))
    }

    async fn restore(&self) -> Outcome {
        let within = self.env.budget(Duration::from_secs(300), MARGIN);
        let c = restored_claim(&self.env, "restored", "512Mi", "grow-snap");
        if let Err(e) = self.api.create(&k8s::pvcs(&self.env), &c).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &grows_pod(&self.env, "restored-pod", "restored", 400 * MIB)).await {
            return Outcome::Infra(e);
        }
        let pod = match self.finished("restored-pod", within).await {
            Ok(d) => d,
            Err(e) => return Outcome::Fail(e),
        };
        if !pod.starts_with("read back") {
            return Outcome::Fail(format!("the restored claim did not hold the snapshot's data: {pod}"));
        }
        match self.grown("restored", 512 * MIB, Duration::from_secs(30)).await {
            Ok(c) => Outcome::Pass(format!("{}; claim {}", pod, s(&c, "/status/capacity/storage"))),
            Err(e) => Outcome::Fail(format!("the pod saw it, but {e}")),
        }
    }

    /// #400: a claim of the fast class binds, holds data, and the kubelet says
    /// its clone was sent tier fast.
    async fn tier_fast(&self) -> Outcome {
        let sc = match self.api.get(&format!("/apis/storage.k8s.io/v1/storageclasses/{FAST_CLASS}")).await {
            Ok(sc) => sc,
            Err(e) => return Outcome::Infra(e),
        };
        if let Some(why) = fast_class_missing(sc.as_ref()) {
            return Outcome::Skip(why);
        }
        let within = self.env.budget(Duration::from_secs(300), MARGIN);
        if let Err(e) = self.api.create(&k8s::pvcs(&self.env), &fast_claim(&self.env, "fast")).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &grows_pod(&self.env, "fast-pod", "fast", 32 * MIB)).await {
            return Outcome::Infra(e);
        }
        let pod = match self.finished("fast-pod", within).await {
            Ok(d) => d,
            Err(e) => return Outcome::Fail(format!("a pod on the fast claim: {e}")),
        };
        let events = k8s::events_for(&self.env, &self.api, "PersistentVolumeClaim", "fast").await.unwrap_or_default();
        let want = format!("its writes on tier {FAST_TIER}");
        match events.iter().find(|e| e.starts_with("ProvisioningSucceeded:")) {
            Some(e) if e.contains(&want) => Outcome::Pass(format!("pod: {pod}; {e}")),
            Some(e) => Outcome::Fail(format!("the claim's event does not say {want:?}: {e}")),
            None => Outcome::Fail(format!("no ProvisioningSucceeded event on the fast claim: {events:?}")),
        }
    }

    async fn volume_stats(&self) -> Outcome {
        let within = self.env.budget(Duration::from_secs(300), MARGIN);
        if let Err(e) = self.api.create(&k8s::pvcs(&self.env), &k8s::claim(&self.env, "stats", "64Mi")).await {
            return Outcome::Infra(e);
        }
        let mut pod = k8s::work(&self.env, "stats-pod", "stats", "df-hold", &["600".to_string()]);
        pod["spec"]["nodeName"] = json!(self.env.node_name);
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &pod).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = k8s::pod_running(&self.env, &self.api, "stats-pod", within).await {
            return Outcome::Fail(format!("the pod holding the claim did not start: {e}"));
        }
        let df = match k8s::until(within, || async {
            let log = k8s::log(&self.env, &self.api, "stats-pod", false).await?;
            df_said(&log).ok_or_else(|| format!("no df line yet: {}", log.trim()))
        })
        .await
        {
            Ok(d) => d,
            Err(e) => return Outcome::Fail(format!("the pod's df: {e}")),
        };
        let ip = match k8s::node_ip(&self.env, &self.api).await {
            Ok(ip) => ip,
            Err(e) => return Outcome::Infra(e),
        };
        let ns = self.env.namespace.clone();
        let vol = k8s::until(within, || async {
            let summary = k8s::kubelet_get(&self.env, &ip, "/stats/summary").await?;
            summary_volume(&summary, &ns, "stats-pod", "claim")
                .cloned()
                .ok_or_else(|| "the summary has no availableBytes for the claim".to_string())
        })
        .await;
        match vol {
            Ok(v) => match df_matches(&df, &v) {
                Ok(said) => Outcome::Pass(said),
                Err(e) => Outcome::Fail(e),
            },
            Err(e) if e.contains("error sending request") => Outcome::Infra(format!("the kubelet is not reachable: {e}")),
            Err(e) => Outcome::Fail(format!("{e} (the pod's df: {df})")),
        }
    }

    /// `pvc-volume-metrics` (#456, #469): `stats-pod`'s claim on /metrics.
    async fn volume_metrics(&self) -> Outcome {
        let within = self.env.budget(Duration::from_secs(120), MARGIN);
        let df = match k8s::log(&self.env, &self.api, "stats-pod", false).await.map(|l| df_said(&l)) {
            Ok(Some(d)) => d,
            Ok(None) => return Outcome::Skip("stats-pod gave no df: pvc-volume-stats says why".into()),
            Err(e) => return Outcome::Infra(e),
        };
        let ip = match k8s::node_ip(&self.env, &self.api).await {
            Ok(ip) => ip,
            Err(e) => return Outcome::Infra(e),
        };
        let ns = self.env.namespace.clone();
        let got = k8s::until(within, || async {
            let (_, text) = k8s::kubelet_text(&self.env, &ip, "/metrics").await?;
            metrics_match(&volume_series(&text, &ns, "stats"), &df, 64 * MIB, 4 * MIB)
        })
        .await;
        match got {
            Ok(said) => Outcome::Pass(said),
            Err(e) if e.contains("error sending request") => Outcome::Infra(format!("the kubelet is not reachable: {e}")),
            Err(e) => Outcome::Fail(e),
        }
    }

    async fn snapshot_deleted(&self) -> Outcome {
        let path = format!("{}/grow-snap", snapshots(&self.env));
        let content = match self.api.get(&path).await {
            Ok(Some(v)) => s(&v, "/status/boundVolumeSnapshotContentName").to_string(),
            Ok(None) => return Outcome::Skip("no snapshot to delete".into()),
            Err(e) => return Outcome::Infra(e),
        };
        if let Err(e) = self.api.delete(&path).await {
            return Outcome::Infra(e);
        }
        let within = self.env.budget(Duration::from_secs(120), MARGIN);
        if let Err(e) = k8s::gone(&self.api, &path, within).await {
            return Outcome::Fail(e);
        }
        if content.is_empty() {
            return Outcome::Pass("the snapshot is gone (it named no content)".into());
        }
        match k8s::gone(&self.api, &format!("{SNAP_API}/volumesnapshotcontents/{content}"), within).await {
            Ok(()) => Outcome::Pass(format!("VolumeSnapshot and its content {content} gone")),
            Err(e) => Outcome::Fail(e),
        }
    }
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let sc = match api.get(&format!("/apis/storage.k8s.io/v1/storageclasses/{CLASS}")).await {
        Ok(sc) => sc,
        Err(e) => {
            r.record("pvc-classes", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let vscs = api.list(&format!("{SNAP_API}/volumesnapshotclasses")).await.ok().flatten().unwrap_or_default();
    let suite = Suite { env: env.clone(), api: api.clone() };
    match classes_missing(sc.as_ref(), &vscs) {
        Some(why) => {
            r.record("pvc-classes", Outcome::Skip(why), 0, None);
        }
        None => {
            let class = vscs.iter().find(|v| s(v, "/driver") == PROVISIONER).map(|v| s(v, "/metadata/name").to_string()).unwrap_or_default();
            r.record("pvc-classes", Outcome::Pass(format!("{CLASS} expands; VolumeSnapshotClass {class}")), 0, None);
            let grew = r.run("pvc-grow", suite.grow()).await;
            if grew {
                r.run("pvc-shrink-refused", suite.shrink_refused()).await;
            }
            let cut = grew && r.run("pvc-snapshot", suite.snapshot(&class)).await;
            if cut {
                r.run("pvc-restore", suite.restore()).await;
            }
            // Before the claims go: the snapshot's content is deleted from its claim's node.
            r.run("pvc-snapshot-deleted", suite.snapshot_deleted()).await;
        }
    }
    // Only the built-in class (#469).
    r.run("pvc-volume-stats", suite.volume_stats()).await;
    r.run("pvc-volume-metrics", suite.volume_metrics()).await;
    // The fast tier needs none of #379's classes, only its own (stormcos#1039).
    r.run("pvc-tier-fast", suite.tier_fast()).await;
    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(30)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_claims_series_are_read_and_held_against_its_df() {
        let text = "# HELP kubelet_volume_stats_used_bytes x\n\
kubelet_volume_stats_capacity_bytes{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 67108864\n\
kubelet_volume_stats_used_bytes{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 5242880\n\
kubelet_volume_stats_available_bytes{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 55000000\n\
kubelet_volume_stats_inodes{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 16384\n\
kubelet_volume_stats_inodes_free{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 16370\n\
kubelet_volume_stats_inodes_used{namespace=\"ns\",persistentvolumeclaim=\"stats\"} 14\n\
kubelet_volume_stats_used_bytes{namespace=\"other\",persistentvolumeclaim=\"stats\"} 1\n";
        let series = volume_series(text, "ns", "stats");
        assert_eq!(series.len(), 6);
        assert_eq!(series["used_bytes"], 5242880.0);
        let df = json!({"availableBytes": 55100000u64, "inodes": 16384, "inodesFree": 16370});
        assert!(metrics_match(&series, &df, 64 * MIB, 4 * MIB).is_ok());
        assert!(metrics_match(&series, &df, 64 * MIB, 8 * MIB).unwrap_err().contains("used below"));
        let off = json!({"availableBytes": 55100000u64, "inodes": 16384, "inodesFree": 16000});
        assert!(metrics_match(&series, &off, 64 * MIB, 4 * MIB).unwrap_err().contains("not the pod's df"));
        let mut part = series.clone();
        part.remove("inodes_used");
        assert!(metrics_match(&part, &df, 64 * MIB, 4 * MIB).unwrap_err().contains("inodes_used"));
    }

    #[test]
    fn the_classes_are_judged() {
        let sc = json!({ "provisioner": PROVISIONER, "allowVolumeExpansion": true });
        let vsc = json!({ "metadata": { "name": "stormblock" }, "driver": PROVISIONER });
        assert_eq!(classes_missing(Some(&sc), std::slice::from_ref(&vsc)), None);
        let fixed = json!({ "provisioner": PROVISIONER, "allowVolumeExpansion": false });
        let why = classes_missing(Some(&fixed), &[]).unwrap();
        assert!(why.contains("does not allow volume expansion") && why.contains("no VolumeSnapshotClass"), "{why}");
        assert!(classes_missing(None, &[vsc]).unwrap().contains("no StorageClass"));
    }

    #[test]
    fn the_fast_class_is_judged() {
        let fast = json!({ "provisioner": PROVISIONER, "parameters": { "tier": "fast" } });
        assert_eq!(fast_class_missing(Some(&fast)), None);
        assert!(fast_class_missing(None).unwrap().contains("no StorageClass stormblock-fast"));
        let bulk = json!({ "provisioner": PROVISIONER, "parameters": { "tier": "bulk" } });
        assert!(fast_class_missing(Some(&bulk)).unwrap().contains("names tier \"bulk\""));
        let c = fast_claim(&k8s::tests::env(), "fast");
        assert_eq!(c["spec"]["storageClassName"], "stormblock-fast");
        assert_eq!(c["spec"]["resources"]["requests"]["storage"], "64Mi");
    }

    #[test]
    fn the_pods_df_is_held_against_the_summary() {
        let line = r#"{"workload": "df-hold /data 600", "ok": true, "detail": "{\"availableBytes\":50331648,\"inodes\":16384,\"inodesFree\":16371}"}"#;
        let df = df_said(&format!("noise\n{line}\n")).unwrap();
        assert_eq!(df["inodes"], 16384);
        let summary = json!({"pods": [
            {"podRef": {"name": "other", "namespace": "ns"}, "volume": [{"name": "claim", "availableBytes": 1}]},
            {"podRef": {"name": "stats-pod", "namespace": "ns"}, "volume": [
                {"name": "claim", "capacityBytes": 67108864u64},
            ]},
        ]});
        assert!(summary_volume(&summary, "ns", "stats-pod", "claim").is_none(), "no fs sizes yet");
        let good = json!({"name": "claim", "availableBytes": 50331648u64 + 4096, "inodes": 16384, "inodesFree": 16371, "inodesUsed": 13});
        let summary = json!({"pods": [{"podRef": {"name": "stats-pod", "namespace": "ns"}, "volume": [good.clone()]}]});
        let v = summary_volume(&summary, "ns", "stats-pod", "claim").unwrap();
        assert!(df_matches(&df, v).is_ok());
        let mut off = good.clone();
        off["inodesFree"] = json!(16000);
        assert!(df_matches(&df, &off).unwrap_err().contains("not the pod's"));
        let mut far = good;
        far["availableBytes"] = json!(1);
        assert!(df_matches(&df, &far).is_err());
    }

    #[test]
    fn the_objects_name_what_they_restore() {
        let env = k8s::tests::env();
        let v = snapshot(&env, "grow-snap", "grow", "stormblock");
        assert_eq!(v["spec"]["source"]["persistentVolumeClaimName"], "grow");
        assert_eq!(v["spec"]["volumeSnapshotClassName"], "stormblock");
        let c = restored_claim(&env, "restored", "512Mi", "grow-snap");
        assert_eq!(c["spec"]["dataSource"]["kind"], "VolumeSnapshot");
        assert_eq!(c["spec"]["resources"]["requests"]["storage"], "512Mi");
        let p = grows_pod(&env, "grow-pod", "grow", 200 * MIB);
        let args: Vec<&str> = p["spec"]["containers"][0]["args"].as_array().unwrap().iter().filter_map(Value::as_str).collect();
        assert_eq!(args[0], "grows");
        assert_eq!(args[2], "379");
        assert_eq!(p["spec"]["nodeName"], json!(env.node_name));
    }
}
