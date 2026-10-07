//! Where each of this node's stormblock PVs physically is (#60).
//!
//! Owner, 2026-09-24: "we should be able to attach and update stormvolume /
//! drive / shelf info as well, as well as raid partner info." Two sources,
//! joined here on the drive's WWN (else its serial):
//!
//! - **stormblock** `GET /api/v1/volumes?placement=true` (stormblock#136): per
//!   volume its drives, its legs (policy, health, missing), whether a rebuild
//!   is owed or running, and the drive-level RAID arrays under its slabs with
//!   each member (the RAID partners) and its state;
//! - **stormdrive** `GET https://<node>:9092/api/v1/placement` (stormdrive#10,
//!   #19): each drive's shelf and bay, health and designation.
//!
//! On every PV of a stormblock volume this node holds (the node's own volumes
//! and the built-in driver's claims alike), as annotations `kubectl describe`
//! shows and labels to select by:
//!
//! | annotation | |
//! |---|---|
//! | `storm.io/volume-id` | the engine's id |
//! | `storm.io/golden` | what it was cloned from, when it was |
//! | `storm.io/redundancy`, `storm.io/health`, `storm.io/rebuild` | the policy, `healthy`/`degraded`/`failed`, `none`/`needed`/the rebuild's state |
//! | `storm.io/drives` | JSON: each drive's `wwn`, `serial`, `model`, `node`, `shelf`, `bay`, `health` |
//! | `storm.io/raid-partners` | JSON: each array member's `array`, `level`, `index`, `state`, `wwn`, `serial`, `node`, `shelf`, `bay` |
//!
//! Labels `storm.io/shelf` (when every drive is in one shelf),
//! `storm.io/redundancy` and `storm.io/health`, made label-safe.
//!
//! A change is an Event on the PV: `VolumeDegraded` / `VolumeFailed` /
//! `VolumeHealthy`, `RebuildStarted` / `RebuildFinished`, `VolumeMoved` (its
//! drives changed), `RaidPartnerChanged`. The first sight of a PV writes the
//! annotations and says nothing.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const DRIVER: &str = "stormblock.storm.io";
pub const VOLUME_ID: &str = "storm.io/volume-id";
pub const GOLDEN: &str = "storm.io/golden";
pub const REDUNDANCY: &str = "storm.io/redundancy";
pub const HEALTH: &str = "storm.io/health";
pub const REBUILD: &str = "storm.io/rebuild";
pub const DRIVES: &str = "storm.io/drives";
pub const PARTNERS: &str = "storm.io/raid-partners";
pub const SHELF: &str = "storm.io/shelf";

/// Where one drive is, from stormdrive.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DriveLoc {
    /// The shelf's label (`labels.shelf`), else its key.
    pub shelf: Option<String>,
    pub bay: Option<u64>,
    pub health: Option<String>,
}

/// stormdrive's drives by `wwn:<wwn>` and `serial:<serial>`.
pub fn drive_index(view: &Value) -> HashMap<String, DriveLoc> {
    let mut out = HashMap::new();
    for d in view["drives"].as_array().into_iter().flatten() {
        let loc = DriveLoc {
            shelf: d["labels"]["shelf"]
                .as_str()
                .or_else(|| d["shelf"]["key"].as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            bay: d["bay"].as_u64(),
            health: d["health"].as_str().map(str::to_string),
        };
        for (k, field) in [("wwn", "wwn"), ("serial", "serial")] {
            if let Some(v) = d[field].as_str().filter(|v| !v.is_empty()) {
                out.insert(format!("{k}:{v}"), loc.clone());
            }
        }
    }
    out
}

fn locate<'a>(drive: &Value, index: &'a HashMap<String, DriveLoc>) -> Option<&'a DriveLoc> {
    let key = |k: &str| drive[k].as_str().filter(|v| !v.is_empty()).map(|v| format!("{k}:{v}"));
    key("wwn").and_then(|k| index.get(&k)).or_else(|| key("serial").and_then(|k| index.get(&k)))
}

/// A label value Kubernetes accepts: `[A-Za-z0-9._-]`, at most 63, starting
/// and ending alphanumeric. `mirror:2@shelf` → `mirror-2-shelf`.
pub fn label_safe(v: &str) -> String {
    let mapped: String = v
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else if c == '+' { 'p' } else { '-' })
        .collect();
    let mut s: String = mapped.chars().take(63).collect();
    while s.ends_with(|c: char| !c.is_ascii_alphanumeric()) {
        s.pop();
    }
    s.trim_start_matches(|c: char| !c.is_ascii_alphanumeric()).to_string()
}

/// What a PV should carry for one engine volume.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Placed {
    pub annotations: BTreeMap<String, String>,
    /// `None`: the label goes.
    pub labels: BTreeMap<String, Option<String>>,
}

/// The annotations and labels for `volume` (an entry of the engine's listing
/// with `placement`). `names` maps volume ids to names, for the golden.
pub fn placed(volume: &Value, names: &HashMap<String, String>, drives: &HashMap<String, DriveLoc>) -> Placed {
    let mut a = BTreeMap::new();
    let p = &volume["placement"];
    let s = |v: &Value| v.as_str().filter(|x| !x.is_empty()).map(str::to_string);
    if let Some(id) = s(&volume["id"]) {
        a.insert(VOLUME_ID.to_string(), id);
    }
    if let Some(g) = volume["parent"].as_str().and_then(|p| names.get(p)) {
        a.insert(GOLDEN.to_string(), g.strip_suffix(".golden").unwrap_or(g).to_string());
    }
    let redundancy = s(&p["legs"]["policy"]).or_else(|| s(&volume["redundancy"]));
    let health = s(&p["legs"]["health"]).or_else(|| s(&volume["health"]));
    if let Some(r) = &redundancy {
        a.insert(REDUNDANCY.to_string(), r.clone());
    }
    if let Some(h) = &health {
        a.insert(HEALTH.to_string(), h.clone());
    }
    if let Some(r) = s(&p["rebuild"]) {
        a.insert(REBUILD.to_string(), r);
    }
    let where_ = |drive: &Value, node: &Value, extra: &mut Map<String, Value>| {
        for k in ["wwn", "serial", "model"] {
            if let Some(v) = s(&drive[k]) {
                extra.insert(k.into(), json!(v));
            }
        }
        if let Some(n) = s(node) {
            extra.insert("node".into(), json!(n));
        }
        if let Some(loc) = locate(drive, drives) {
            if let Some(sh) = &loc.shelf {
                extra.insert("shelf".into(), json!(sh));
            }
            if let Some(b) = loc.bay {
                extra.insert("bay".into(), json!(b));
            }
            if let Some(h) = &loc.health {
                extra.insert("health".into(), json!(h));
            }
        }
    };
    let mut shelves = BTreeSet::new();
    if p["drives"].is_array() {
        let list: Vec<Value> = p["drives"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|d| {
                let mut m = Map::new();
                where_(&d["drive"], &d["node"], &mut m);
                if let Some(sh) = m.get("shelf").and_then(Value::as_str) {
                    shelves.insert(sh.to_string());
                }
                Value::Object(m)
            })
            .collect();
        a.insert(DRIVES.to_string(), Value::Array(list).to_string());
    }
    if p["arrays"].is_array() || p.is_object() {
        let mut members = Vec::new();
        for arr in p["arrays"].as_array().into_iter().flatten() {
            for m in arr["members"].as_array().into_iter().flatten() {
                let mut o = Map::new();
                o.insert("array".into(), arr["id"].clone());
                o.insert("level".into(), arr["level"].clone());
                o.insert("index".into(), m["index"].clone());
                o.insert("state".into(), m["state"].clone());
                where_(&m["drive"], &m["node"], &mut o);
                members.push(Value::Object(o));
            }
        }
        a.insert(PARTNERS.to_string(), Value::Array(members).to_string());
    }
    let mut labels = BTreeMap::new();
    labels.insert(
        SHELF.to_string(),
        (shelves.len() == 1).then(|| label_safe(shelves.iter().next().unwrap())).filter(|v| !v.is_empty()),
    );
    labels.insert(REDUNDANCY.to_string(), redundancy.as_deref().map(label_safe).filter(|v| !v.is_empty()));
    labels.insert(HEALTH.to_string(), health.as_deref().map(label_safe).filter(|v| !v.is_empty()));
    Placed { annotations: a, labels }
}

/// The merge patch that brings `pv` to `want`, or `None` when it already says it.
pub fn patch_for(pv: &Value, want: &Placed) -> Option<Value> {
    let have_a = &pv["metadata"]["annotations"];
    let have_l = &pv["metadata"]["labels"];
    let mut annotations = Map::new();
    for (k, v) in &want.annotations {
        if have_a[k].as_str() != Some(v.as_str()) {
            annotations.insert(k.clone(), json!(v));
        }
    }
    let mut labels = Map::new();
    for (k, v) in &want.labels {
        match v {
            Some(v) if have_l[k].as_str() != Some(v.as_str()) => {
                labels.insert(k.clone(), json!(v));
            }
            None if !have_l[k].is_null() => {
                labels.insert(k.clone(), Value::Null);
            }
            _ => {}
        }
    }
    if annotations.is_empty() && labels.is_empty() {
        return None;
    }
    let mut meta = Map::new();
    if !annotations.is_empty() {
        meta.insert("annotations".into(), Value::Object(annotations));
    }
    if !labels.is_empty() {
        meta.insert("labels".into(), Value::Object(labels));
    }
    Some(json!({ "metadata": meta }))
}

/// A rebuild that is under way, not merely owed.
fn rebuilding(r: Option<&str>) -> bool {
    !matches!(r, None | Some("") | Some("none") | Some("needed"))
}

fn drive_keys(json_list: Option<&str>) -> BTreeSet<String> {
    json_list
        .and_then(|t| serde_json::from_str::<Vec<Value>>(t).ok())
        .unwrap_or_default()
        .iter()
        .filter_map(|d| d["wwn"].as_str().or_else(|| d["serial"].as_str()).map(str::to_string))
        .collect()
}

fn partner_states(json_list: Option<&str>) -> BTreeMap<String, String> {
    json_list
        .and_then(|t| serde_json::from_str::<Vec<Value>>(t).ok())
        .unwrap_or_default()
        .iter()
        .map(|m| (format!("{}#{}", m["array"].as_str().unwrap_or(""), m["index"]), m["state"].as_str().unwrap_or("").to_string()))
        .collect()
}

/// The Events a change from the PV's annotations `before` to `after` is: the
/// first sight (no `storm.io/volume-id` yet) is none.
pub fn changes(volume: &str, before: &Value, after: &Placed) -> Vec<(&'static str, &'static str, String)> {
    let mut out = Vec::new();
    if before[VOLUME_ID].is_null() {
        return out;
    }
    let old = |k: &str| before[k].as_str();
    let new = |k: &str| after.annotations.get(k).map(String::as_str);
    if let (Some(was), Some(now)) = (old(HEALTH), new(HEALTH)) {
        if was != now {
            let (t, reason) = match now {
                "healthy" => ("Normal", "VolumeHealthy"),
                "failed" => ("Warning", "VolumeFailed"),
                _ => ("Warning", "VolumeDegraded"),
            };
            out.push((t, reason, format!("volume {volume} is {now} (was {was})")));
        }
    }
    match (rebuilding(old(REBUILD)), rebuilding(new(REBUILD))) {
        (false, true) => out.push(("Normal", "RebuildStarted", format!(
            "volume {volume}: rebuild {}", new(REBUILD).unwrap_or("")
        ))),
        (true, false) => out.push(("Normal", "RebuildFinished", format!(
            "volume {volume}: rebuild finished ({})", new(REBUILD).unwrap_or("none")
        ))),
        _ => {}
    }
    let (was, now) = (drive_keys(old(DRIVES)), drive_keys(new(DRIVES)));
    if !was.is_empty() && was != now {
        out.push(("Normal", "VolumeMoved", format!(
            "volume {volume} is on drives {} (was {})",
            now.iter().cloned().collect::<Vec<_>>().join(", "),
            was.iter().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    let (was, now) = (partner_states(old(PARTNERS)), partner_states(new(PARTNERS)));
    for (member, state) in &now {
        match was.get(member) {
            Some(prev) if prev != state => {
                let bad = ["fail", "fault", "missing", "degraded", "removed"].iter().any(|w| state.contains(w));
                out.push((if bad { "Warning" } else { "Normal" }, "RaidPartnerChanged", format!(
                    "volume {volume}: RAID member {member} is {state} (was {prev})"
                )));
            }
            _ => {}
        }
    }
    out
}

/// The engine volume a PV names, when the PV is this node's stormblock volume.
pub fn ours(pv: &Value, node: &str) -> Option<String> {
    if pv["spec"]["csi"]["driver"].as_str() != Some(DRIVER) {
        return None;
    }
    let annotated = pv["metadata"]["annotations"]["storm.io/node"].as_str() == Some(node);
    let affinity = pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|t| t["matchExpressions"].as_array().into_iter().flatten())
        .any(|e| {
            e["key"].as_str() == Some("kubernetes.io/hostname")
                && e["values"].as_array().is_some_and(|v| v.iter().any(|x| x.as_str() == Some(node)))
        });
    if !(annotated || affinity) {
        return None;
    }
    pv["metadata"]["annotations"]["storm.io/volume"]
        .as_str()
        .or_else(|| pv["spec"]["csi"]["volumeHandle"].as_str())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// One pass: every PV of this node's stormblock volumes brought up to date.
/// `stormdrive` is the node's drive service, the addresses tried in order
/// (TLS since stormdrive#19, plain before it); unreachable, the drives carry
/// stormblock's half alone. Answers how many PVs were written.
pub async fn pass(
    client: &reqwest::Client,
    api_url: &str,
    engine: &crate::engine::EngineClient,
    stormdrive: &[String],
    node: &str,
    events: Option<&crate::events::EventRecorder>,
) -> Result<usize, String> {
    if api_url.is_empty() {
        return Ok(0);
    }
    let vols: Value = match engine.get(&format!("{}/api/v1/volumes?placement=true", engine.url())).await {
        Ok(r) if r.status().is_success() => r.json().await.map_err(|e| format!("engine listing: {e}"))?,
        Ok(r) => return Err(format!("engine listing: {}", r.status())),
        Err(e) => return Err(format!("engine listing: {e}")),
    };
    let items = vols["items"].as_array().cloned().unwrap_or_default();
    let names: HashMap<String, String> = items
        .iter()
        .filter_map(|v| Some((v["id"].as_str()?.to_string(), v["name"].as_str()?.to_string())))
        .collect();
    let by_name: HashMap<&str, &Value> = items.iter().filter_map(|v| Some((v["name"].as_str()?, v))).collect();

    let mut drives = None;
    let mut why = Vec::new();
    for base in stormdrive {
        match client.get(format!("{}/api/v1/placement", base.trim_end_matches('/'))).send().await {
            Ok(r) if r.status().is_success() => {
                drives = Some(drive_index(&r.json().await.unwrap_or(Value::Null)));
                break;
            }
            Ok(r) => why.push(format!("{base}: {}", r.status())),
            Err(e) => why.push(format!("{base}: {e}")),
        }
    }
    let drives = drives.unwrap_or_else(|| {
        static SAID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !SAID.swap(true, std::sync::atomic::Ordering::Relaxed) {
            tracing::warn!("drive locations unavailable (PVs carry stormblock's placement alone): {}", why.join("; "));
        }
        HashMap::new()
    });

    let pvs: Value = match client.get(format!("{api_url}/api/v1/persistentvolumes")).send().await {
        Ok(r) if r.status().is_success() => r.json().await.map_err(|e| format!("PV list: {e}"))?,
        Ok(r) => return Err(format!("PV list: {}", r.status())),
        Err(e) => return Err(format!("PV list: {e}")),
    };
    let mut written = 0;
    for pv in pvs["items"].as_array().into_iter().flatten() {
        let Some(volume) = ours(pv, node) else { continue };
        let Some(v) = by_name.get(volume.as_str()) else { continue };
        if !pv["metadata"]["deletionTimestamp"].is_null() {
            continue;
        }
        let want = placed(v, &names, &drives);
        let Some(patch) = patch_for(pv, &want) else { continue };
        let name = pv["metadata"]["name"].as_str().unwrap_or_default();
        let url = format!("{api_url}/api/v1/persistentvolumes/{name}");
        let ok = client
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&patch)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        if !ok {
            tracing::debug!(pv = %name, "placement not written; next pass");
            continue;
        }
        written += 1;
        if let Some(r) = events {
            for (etype, reason, message) in changes(&volume, &pv["metadata"]["annotations"], &want) {
                r.object_event("v1", "PersistentVolume", pv, etype, reason, &message).await;
            }
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stormdrive() -> Value {
        json!({"node": "n1", "generation": 7, "drives": [
            {"wwn": "naa.5000c500a1", "serial": "S1", "shelf": {"key": "500605b0"}, "labels": {"shelf": "shelf-a"}, "bay": 3, "health": "good"},
            {"wwn": "", "serial": "S2", "shelf": {"key": "500605b1"}, "bay": 9, "health": "warning"},
        ]})
    }

    fn volume(health: &str, rebuild: &str, member_state: &str) -> Value {
        json!({"id": "v-1", "name": "pvc-ns-data", "parent": "g-1", "redundancy": "mirror:2", "health": health,
          "placement": {
            "drives": [
                {"drive": {"serial": "S1", "wwn": "naa.5000c500a1", "model": "ST1200"}, "node": "n1"},
                {"drive": {"serial": "S2", "wwn": "", "model": "ST1200"}, "node": "n1"}],
            "legs": {"policy": "mirror:2@shelf", "health": health, "missing": 0},
            "rebuild": rebuild,
            "arrays": [{"id": "arr-1", "level": "raid1", "members": [
                {"index": 0, "state": "in_sync", "drive": {"serial": "S1", "wwn": "naa.5000c500a1"}, "node": "n1"},
                {"index": 1, "state": member_state, "drive": {"serial": "S2"}, "node": "n1"}]}]}})
    }

    fn names() -> HashMap<String, String> {
        [("g-1".to_string(), "pvc-ext4j-64m.golden".to_string())].into()
    }

    #[test]
    fn a_volume_carries_its_drives_shelves_bays_and_partners() {
        let p = placed(&volume("healthy", "none", "in_sync"), &names(), &drive_index(&stormdrive()));
        let a = &p.annotations;
        assert_eq!(a[VOLUME_ID], "v-1");
        assert_eq!(a[GOLDEN], "pvc-ext4j-64m");
        assert_eq!(a[REDUNDANCY], "mirror:2@shelf");
        assert_eq!(a[HEALTH], "healthy");
        assert_eq!(a[REBUILD], "none");
        let drives: Vec<Value> = serde_json::from_str(&a[DRIVES]).unwrap();
        assert_eq!(drives[0], json!({"wwn": "naa.5000c500a1", "serial": "S1", "model": "ST1200", "node": "n1",
                                      "shelf": "shelf-a", "bay": 3, "health": "good"}), "joined on WWN");
        assert_eq!(drives[1]["shelf"], "500605b1", "joined on serial, the shelf key with no label");
        assert_eq!(drives[1]["bay"], 9);
        let partners: Vec<Value> = serde_json::from_str(&a[PARTNERS]).unwrap();
        assert_eq!(partners.len(), 2);
        assert_eq!((partners[1]["array"].as_str(), partners[1]["state"].as_str(), partners[1]["bay"].as_u64()),
                   (Some("arr-1"), Some("in_sync"), Some(9)));
        // Two shelves: no single shelf to select by. Policy made label-safe.
        assert_eq!(p.labels[SHELF], None);
        assert_eq!(p.labels[REDUNDANCY].as_deref(), Some("mirror-2-shelf"));
        assert_eq!(p.labels[HEALTH].as_deref(), Some("healthy"));
        assert_eq!(label_safe("raid5:4+1"), "raid5-4p1");
    }

    #[test]
    fn a_pv_is_patched_only_with_what_changed() {
        let want = placed(&volume("healthy", "none", "in_sync"), &names(), &drive_index(&stormdrive()));
        let pv = json!({"metadata": {"name": "pv", "annotations": {}, "labels": {"storm.io/shelf": "old"}}});
        let patch = patch_for(&pv, &want).unwrap();
        assert_eq!(patch["metadata"]["annotations"][HEALTH], "healthy");
        assert_eq!(patch["metadata"]["labels"][SHELF], Value::Null, "a shelf label no longer true goes");
        let mut current = pv.clone();
        for (k, v) in &want.annotations {
            current["metadata"]["annotations"][k] = json!(v);
        }
        current["metadata"]["labels"] = json!({});
        for (k, v) in &want.labels {
            if let Some(v) = v {
                current["metadata"]["labels"][k] = json!(v);
            }
        }
        assert_eq!(patch_for(&current, &want), None, "nothing written when current");
    }

    #[test]
    fn a_change_is_an_event_and_the_first_sight_is_not() {
        let idx = drive_index(&stormdrive());
        let before = placed(&volume("healthy", "none", "in_sync"), &names(), &idx);
        let as_pv = |p: &Placed| json!(p.annotations);
        assert!(changes("d", &json!({}), &before).is_empty(), "first sight");

        let degraded = placed(&volume("degraded", "queued", "failed"), &names(), &idx);
        let ev = changes("d", &as_pv(&before), &degraded);
        let reasons: Vec<&str> = ev.iter().map(|e| e.1).collect();
        assert_eq!(reasons, vec!["VolumeDegraded", "RebuildStarted", "RaidPartnerChanged"], "{ev:?}");
        assert_eq!(ev[0].0, "Warning");
        assert_eq!(ev[2].0, "Warning", "a failed member is a warning");

        let mut moved = volume("healthy", "none", "in_sync");
        moved["placement"]["drives"][1]["drive"] = json!({"serial": "S9", "wwn": "naa.9"});
        let healed = placed(&moved, &names(), &idx);
        let ev = changes("d", &as_pv(&degraded), &healed);
        let reasons: Vec<&str> = ev.iter().map(|e| e.1).collect();
        assert_eq!(reasons, vec!["VolumeHealthy", "RebuildFinished", "VolumeMoved", "RaidPartnerChanged"], "{ev:?}");
        assert!(ev[2].2.contains("naa.9"), "{}", ev[2].2);
    }

    #[test]
    fn only_this_nodes_stormblock_pvs_are_ours() {
        let pv = |driver: &str, ann: Value, aff: &str| json!({"metadata": {"annotations": ann},
            "spec": {"csi": {"driver": driver, "volumeHandle": "pvc-ns-data"},
                     "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                        {"key": "kubernetes.io/hostname", "operator": "In", "values": [aff]}]}]}}}});
        assert_eq!(ours(&pv(DRIVER, json!({"storm.io/node": "n1", "storm.io/volume": "fastetcd-data"}), "x"), "n1").as_deref(), Some("fastetcd-data"));
        assert_eq!(ours(&pv(DRIVER, json!({}), "n1"), "n1").as_deref(), Some("pvc-ns-data"), "by its node affinity");
        assert_eq!(ours(&pv(DRIVER, json!({}), "n2"), "n1"), None);
        assert_eq!(ours(&pv("ebs.csi.aws.com", json!({"storm.io/node": "n1"}), "n1"), "n1"), None);
    }
}
