//! A bound built-in claim moved to another node (#338, stormcos#806 tests
//! 1 and 5): its stormblock volume mirrored to the target, cut over once no
//! pod here holds it, and its PV pointed at the target, under the same PVC.
//!
//! **The data** is stormstorage's (stormstorage#44, #106): `migrate` mirrors
//! the volume to a new volume on the target (named like the source, so the
//! PV's `volumeHandle` names it there too), attached here or not; `cutover`
//! makes the copy its own; `complete` retires the source; `abort` deletes the
//! copy and leaves the source as it was.
//!
//! **The names** are the ones stormcluster's drain (stormcluster#104) and
//! stormcos_qa's mobility test (stormcos_qa#84) read, all on the PVC:
//! - [`MOVE_TO`] (and [`MOVE_SET`], a RAID set on the target): the request;
//! - [`STATE`]: [`COPYING`] → [`SYNCED`] (the drain evicts) →
//!   [`CUTTING_OVER`] → [`COMPLETE`], or [`FAILED`] / [`ABORTED`] /
//!   [`REFUSED`] with [`REASON`];
//! - [`PROGRESS`]: what is left of the copy, or what the cutover waits on.
//!
//! The kubelet's own: [`MIGRATION`] (stormstorage's id), [`TARGET`] (the node
//! that state is about) and [`FROM`] (the node driving it). Cut over is read
//! by others from the PV: its `nodeAffinity` (and `storm.io/node`) name the
//! target once it is the claim's.
//!
//! Driven by the kubelet of the node whose storage holds the claim (the PV's
//! node; [`FROM`] while a move is in flight), which owns its attachment.

use serde_json::{json, Value};

pub const MOVE_TO: &str = "storm.io/move-to";
pub const MOVE_SET: &str = "storm.io/move-set";
pub const STATE: &str = "storm.io/move-state";
pub const REASON: &str = "storm.io/move-reason";
pub const PROGRESS: &str = "storm.io/move-progress";
pub const MIGRATION: &str = "storm.io/move-migration";
pub const TARGET: &str = "storm.io/move-target";
pub const FROM: &str = "storm.io/move-from";

pub const COPYING: &str = "copying";
/// In sync; the source is still the claim's. The pod may stop now.
pub const SYNCED: &str = "synced";
/// No pod here holds it: detach, cutover, complete, rebind. A start waits.
pub const CUTTING_OVER: &str = "cutting-over";
/// The claim is the target's: its PV says so.
pub const COMPLETE: &str = "complete";
pub const FAILED: &str = "failed";
pub const ABORTED: &str = "aborted";
pub const REFUSED: &str = "refused";

/// How often a move in flight is looked at with no event.
pub const POLL: std::time::Duration = std::time::Duration::from_secs(3);

/// The built-in driver.
pub const DRIVER: &str = "stormblock.storm.io";

/// What the PVC says about a move.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Move {
    pub to: Option<String>,
    pub set: Option<String>,
    pub state: String,
    pub migration: String,
    pub target: String,
    pub from: String,
}

impl Move {
    pub fn of(pvc: &Value) -> Move {
        let a = &pvc["metadata"]["annotations"];
        let s = |k: &str| a[k].as_str().map(str::trim).unwrap_or("").to_string();
        let opt = |k: &str| Some(s(k)).filter(|v| !v.is_empty());
        Move {
            to: opt(MOVE_TO),
            set: opt(MOVE_SET),
            state: s(STATE),
            migration: s(MIGRATION),
            target: s(TARGET),
            from: s(FROM),
        }
    }

    /// Being worked on by the node in [`FROM`].
    pub fn active(&self) -> bool {
        matches!(self.state.as_str(), COPYING | SYNCED | CUTTING_OVER)
    }

    /// Asked for at all: anything for the kubelet to look at.
    pub fn any(&self) -> bool {
        self.to.is_some() || self.active()
    }
}

/// What a pass does about a claim this node holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Nothing,
    /// Start a move to this node.
    Start(String),
    /// A move in flight: carry on.
    Carry,
    /// A move in flight no longer asked for: abort it, saying why.
    Abort(String),
}

pub fn step(m: &Move, here: &str) -> Step {
    if m.active() {
        return match m.to.as_deref() {
            Some(to) if to == m.target => Step::Carry,
            // Past the cutover there is nothing to abort: finish it.
            _ if m.state == CUTTING_OVER => Step::Carry,
            Some(to) => Step::Abort(format!("the request now names {to}, not {}", m.target)),
            None => Step::Abort(format!("the request ({MOVE_TO}) was taken off")),
        };
    }
    match m.to.as_deref() {
        None => Step::Nothing,
        Some(to) if to == here => Step::Nothing,
        // Not started, or a finished one about another target.
        Some(to) if m.state.is_empty() || m.target != to => Step::Start(to.to_string()),
        // Ended for this target (failed, refused, …): stays until the
        // request is made again (stormcluster clears the state when it is).
        Some(_) => Step::Nothing,
    }
}

/// The node a stormblock PV's volume is on: `storm.io/node`, else its
/// `kubernetes.io/hostname` affinity.
pub fn pv_node(pv: &Value) -> Option<String> {
    if let Some(n) = pv["metadata"]["annotations"]["storm.io/node"].as_str().filter(|n| !n.is_empty()) {
        return Some(n.to_string());
    }
    pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"]
        .as_array()?
        .iter()
        .flat_map(|t| t["matchExpressions"].as_array().cloned().unwrap_or_default())
        .find(|e| e["key"] == "kubernetes.io/hostname")
        .and_then(|e| e["values"][0].as_str().map(String::from))
}

/// Is this PV the built-in driver's?
pub fn is_builtin(pv: &Value) -> bool {
    pv["spec"]["csi"]["driver"].as_str() == Some(DRIVER)
}

/// What a claim needs from the target, for the refusals.
#[derive(Debug, Clone, Default)]
pub struct Facts {
    /// Nodes other than this one where a pod not finished uses the claim.
    pub used_elsewhere: Vec<String>,
    /// The target's Node: `None` not found, `Some(ready)`.
    pub target_ready: Option<bool>,
    /// The target's CSIStorageCapacity for the class (`maximumVolumeSize`,
    /// else `capacity`), when it publishes one.
    pub target_room: Option<u64>,
    /// The PV's capacity.
    pub size: u64,
    pub stormstorage: bool,
}

/// Why a move of `pvc` (bound to `pv`) to `to` is refused, if it is.
pub fn refusal(pvc: &Value, pv: &Value, to: &str, f: &Facts) -> Option<String> {
    let modes = pvc["spec"]["accessModes"].as_array().cloned().unwrap_or_default();
    if modes.iter().any(|m| m == "ReadWriteMany" || m == "ReadOnlyMany") {
        return Some("a claim more than one node mounts (ReadWriteMany/ReadOnlyMany) is not moved".into());
    }
    if pv["metadata"]["labels"][crate::system_claims::LABEL].as_str() == Some("true") {
        return Some("a node service's own data volume is not moved".into());
    }
    if !f.used_elsewhere.is_empty() {
        return Some(format!("pods on {} use it too: a claim used on two nodes is not moved", f.used_elsewhere.join(", ")));
    }
    if !f.stormstorage {
        return Some("this node has no stormstorage (--stormstorage)".into());
    }
    match f.target_ready {
        None => return Some(format!("node {to} does not exist")),
        Some(false) => return Some(format!("node {to} is not Ready")),
        Some(true) => {}
    }
    if let Some(room) = f.target_room {
        if room < f.size {
            return Some(format!("node {to} has room for {room} bytes, the volume is {}", f.size));
        }
    }
    None
}

/// The copy's progress, as [`PROGRESS`] reads.
pub fn progress(total: u64, remaining: u64) -> String {
    if total == 0 {
        return "starting".into();
    }
    let done = 100 - remaining.min(total) * 100 / total;
    format!("{done}% ({} left)", crate::system_claims::quantity(remaining))
}

/// The PVC's annotations patch: `None` values removed.
pub fn annotations(pairs: &[(&str, Option<&str>)]) -> Value {
    let mut a = serde_json::Map::new();
    for (k, v) in pairs {
        a.insert(k.to_string(), v.map(|v| json!(v)).unwrap_or(Value::Null));
    }
    json!({"metadata": {"annotations": a}})
}

/// The PV pointed at node `to`: its `nodeAffinity` and `storm.io/node`.
/// Guarded by the PV's uid.
pub fn rebind_patch(pv: &Value, to: &str) -> Value {
    json!({
        "metadata": {"uid": pv["metadata"]["uid"], "annotations": {"storm.io/node": to}},
        "spec": {"nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": [to]}
        ]}]}}},
    })
}

/// Why a pod on `here` must not start on this claim now, if it must not:
/// it is being cut over, or its PV is another node's (a moved claim: a
/// fresh volume here would be an empty one beside the real one).
pub fn start_hold(pvc: &Value, pv: Option<&Value>, here: &str) -> Option<String> {
    let ns = pvc["metadata"]["namespace"].as_str().unwrap_or("default");
    let name = pvc["metadata"]["name"].as_str().unwrap_or("");
    let m = Move::of(pvc);
    if m.state == CUTTING_OVER {
        return Some(format!("claim {ns}/{name} is being cut over to {} (#338): it starts there", m.target));
    }
    let pv = pv.filter(|pv| is_builtin(pv))?;
    match pv_node(pv) {
        Some(n) if n != here => Some(format!(
            "claim {ns}/{name}'s volume is on {n}'s storage (#338), and this pod is on {here}: it starts on {n}"
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pvc(ann: Value) -> Value {
        json!({"metadata": {"namespace": "db", "name": "data", "annotations": ann},
               "spec": {"accessModes": ["ReadWriteOnce"], "volumeName": "pvc-db-data"}})
    }

    fn pv(node: &str) -> Value {
        json!({"metadata": {"name": "pvc-db-data", "uid": "pv-1"},
               "spec": {"csi": {"driver": DRIVER, "volumeHandle": "pvc-db-data"},
                        "capacity": {"storage": "1Gi"},
                        "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}}}})
    }

    #[test]
    fn the_pvc_says_what_is_asked_and_how_far_it_got() {
        let m = Move::of(&pvc(json!({MOVE_TO: "b", MOVE_SET: "r1", STATE: "copying", MIGRATION: "m1", TARGET: "b", FROM: "a"})));
        assert_eq!(m.to.as_deref(), Some("b"));
        assert_eq!(m.set.as_deref(), Some("r1"));
        assert!(m.active() && m.any());
        assert!(!Move::of(&pvc(json!({}))).any());
        assert!(Move::of(&pvc(json!({MOVE_TO: " "}))).to.is_none());
    }

    #[test]
    fn each_pass_knows_what_to_do() {
        let m = |to: Option<&str>, state: &str, target: &str| Move {
            to: to.map(String::from),
            state: state.into(),
            target: target.into(),
            ..Default::default()
        };
        assert_eq!(step(&m(None, "", ""), "a"), Step::Nothing);
        assert_eq!(step(&m(Some("a"), "", ""), "a"), Step::Nothing, "already here");
        assert_eq!(step(&m(Some("b"), "", ""), "a"), Step::Start("b".into()));
        assert_eq!(step(&m(Some("b"), COPYING, "b"), "a"), Step::Carry);
        assert_eq!(step(&m(Some("b"), SYNCED, "b"), "a"), Step::Carry);
        assert!(matches!(step(&m(Some("c"), COPYING, "b"), "a"), Step::Abort(w) if w.contains("names c")));
        assert!(matches!(step(&m(None, SYNCED, "b"), "a"), Step::Abort(w) if w.contains("taken off")));
        assert_eq!(step(&m(None, CUTTING_OVER, "b"), "a"), Step::Carry, "past the point of aborting");
        assert_eq!(step(&m(Some("b"), FAILED, "b"), "a"), Step::Nothing, "until asked again");
        assert_eq!(step(&m(Some("c"), FAILED, "b"), "a"), Step::Start("c".into()));
        assert_eq!(step(&m(Some("c"), COMPLETE, "a"), "a"), Step::Start("c".into()), "moved here, now onward");
    }

    #[test]
    fn the_pv_names_its_node() {
        assert_eq!(pv_node(&pv("a")).as_deref(), Some("a"));
        let mut p = pv("a");
        p["metadata"]["annotations"] = json!({"storm.io/node": "b"});
        assert_eq!(pv_node(&p).as_deref(), Some("b"));
        assert_eq!(pv_node(&json!({})), None);
        let patch = rebind_patch(&pv("a"), "b");
        let mut moved = pv("a");
        moved["spec"]["nodeAffinity"] = patch["spec"]["nodeAffinity"].clone();
        assert_eq!(pv_node(&moved).as_deref(), Some("b"));
        assert_eq!(patch["metadata"]["uid"], "pv-1");
        assert_eq!(patch["metadata"]["annotations"]["storm.io/node"], "b");
    }

    #[test]
    fn what_is_not_moved_says_why() {
        let ok = Facts { target_ready: Some(true), target_room: Some(2 << 30), size: 1 << 30, stormstorage: true, ..Default::default() };
        assert_eq!(refusal(&pvc(json!({})), &pv("a"), "b", &ok), None);
        let mut rwx = pvc(json!({}));
        rwx["spec"]["accessModes"] = json!(["ReadWriteMany"]);
        assert!(refusal(&rwx, &pv("a"), "b", &ok).unwrap().contains("ReadWriteMany"));
        let mut sys = pv("a");
        sys["metadata"]["labels"] = json!({crate::system_claims::LABEL: "true"});
        assert!(refusal(&pvc(json!({})), &sys, "b", &ok).unwrap().contains("node service"));
        let two = Facts { used_elsewhere: vec!["c".into()], ..ok.clone() };
        assert!(refusal(&pvc(json!({})), &pv("a"), "b", &two).unwrap().contains("pods on c"));
        let gone = Facts { target_ready: None, ..ok.clone() };
        assert!(refusal(&pvc(json!({})), &pv("a"), "b", &gone).unwrap().contains("does not exist"));
        let down = Facts { target_ready: Some(false), ..ok.clone() };
        assert!(refusal(&pvc(json!({})), &pv("a"), "b", &down).unwrap().contains("not Ready"));
        let small = Facts { target_room: Some(1), ..ok.clone() };
        assert!(refusal(&pvc(json!({})), &pv("a"), "b", &small).unwrap().contains("room"));
        let unknown = Facts { target_room: None, ..ok.clone() };
        assert_eq!(refusal(&pvc(json!({})), &pv("a"), "b", &unknown), None, "no capacity published: stormstorage decides");
        let none = Facts { stormstorage: false, ..ok };
        assert!(refusal(&pvc(json!({})), &pv("a"), "b", &none).unwrap().contains("stormstorage"));
    }

    #[test]
    fn a_start_waits_on_a_claim_cut_over_or_moved_away() {
        assert_eq!(start_hold(&pvc(json!({})), Some(&pv("a")), "a"), None);
        assert_eq!(start_hold(&pvc(json!({})), None, "a"), None, "not provisioned yet");
        assert!(start_hold(&pvc(json!({})), Some(&pv("b")), "a").unwrap().contains("on b's storage"));
        let cutting = pvc(json!({STATE: CUTTING_OVER, TARGET: "b"}));
        assert!(start_hold(&cutting, Some(&pv("a")), "a").unwrap().contains("being cut over to b"));
        assert!(start_hold(&cutting, Some(&pv("a")), "b").is_some(), "on b too, until it is complete");
        let mut other = pv("b");
        other["spec"]["csi"]["driver"] = json!("hostpath.csi.k8s.io");
        assert_eq!(start_hold(&pvc(json!({})), Some(&other), "a"), None, "another driver's");
    }

    #[test]
    fn progress_and_patches() {
        assert_eq!(progress(0, 0), "starting");
        assert!(progress(4 << 30, 1 << 30).starts_with("75% ("));
        let p = annotations(&[(STATE, Some("synced")), (REASON, None)]);
        assert_eq!(p["metadata"]["annotations"][STATE], "synced");
        assert!(p["metadata"]["annotations"][REASON].is_null() && p["metadata"]["annotations"].get(REASON).is_some());
    }
}
