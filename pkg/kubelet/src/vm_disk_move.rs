//! A running VMI's disk moved to another node's storage, the VM staying
//! where it is (#302, stormvm#29).
//!
//! **The data** is stormstorage's (stormstorage#44): `migrate` mirrors the
//! disk's volume to a fresh volume on the target node, `cutover` (with the
//! guest paused, once `synced`) makes the copy its own, `complete` retires
//! the source's, `abort` deletes the copy and leaves the source as it was.
//!
//! **The switch** is stormvm's (`Machine::switch_disk`, qemu only): the disk
//! on one device is put on another device holding the same bytes, in place.
//! The guest's device, serial and slot do not change. The new device is the
//! copy attached on this node by its stormblock, which reaches a peer's
//! volume over NVMe/TCP and serves it as ublk (stormblock#328).
//!
//! **How it is asked for:** the VMI's annotation [`REQUEST`], a JSON object
//! naming the node each disk should be on: `{"data": "node-b"}`. Naming the
//! node a disk is on already, or leaving a disk out, asks nothing. The
//! kubelet's account is the annotation [`RECORD`] (one [`Move`] per disk):
//! where the disk is, the stormstorage migration in flight, its progress
//! (what stormconsole shows) and how it ended. It is also what a restarted
//! kubelet resumes from, and what the next start of the VMI attaches.
//!
//! **What is moved:** a disk the machine made and owns (a golden's clone, an
//! emptyDisk). A claim is its driver's to move, a `volume:` is somebody
//! else's, and a seed is made again at every start: each refused, saying
//! why. cloud-hypervisor cannot switch a running disk (stormvm answers
//! Unsupported) and is refused before anything is copied.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The request: `{"<disk>": "<node>"}`.
pub const REQUEST: &str = "storm.io/move-disks";
/// The kubelet's record: `{"<disk>": Move}`.
pub const RECORD: &str = "storm.io/disk-moves";

/// A stopped VirtualMachine's request (#339): the node its disks go to.
pub const MOVE_TO: &str = "storm.io/move-to";
/// The kubelet's one-line account of a stopped VM's move, on the VM.
pub const MOVE_STATUS: &str = "storm.io/move-status";
/// The node label the template's `nodeSelector` names once the disks moved.
pub const HOSTNAME: &str = "kubernetes.io/hostname";

/// How often a move in flight is looked at with no event.
pub const POLL: std::time::Duration = std::time::Duration::from_secs(3);

/// The mirror is running: progress is reported.
pub const COPYING: &str = "copying";
/// The guest is paused for the cutover and the switch. Seen at the start of
/// a pass, it means a kubelet that stopped half way (passes of one VMI never
/// overlap): the registration then says which volume the disk is on.
pub const SWITCHING: &str = "switching";
/// The disk is on the copy; the source's volume is still to be retired.
pub const SWITCHED: &str = "switched";
/// Done: `volume` on `to` is the disk's.
pub const MOVED: &str = "moved";
/// It did not happen; the disk is on `from`, as before.
pub const FAILED: &str = "failed";
/// The switch answered an error after the cutover, or a kubelet stopped
/// during it: which volume the guest writes is not known for sure, so
/// nothing is deleted, nothing is retried, and a person decides.
pub const UNKNOWN: &str = "unknown";

/// One disk's move, as [`RECORD`] holds it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Move {
    /// The node asked for.
    pub to: String,
    /// The node the disk's volume was on when the move began.
    pub node: String,
    /// That volume's id.
    pub from: String,
    /// stormstorage's migration id.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub migration: String,
    pub state: String,
    #[serde(default)]
    pub bytes_total: u64,
    #[serde(default)]
    pub bytes_remaining: u64,
    /// The copy's volume id (on `to`), once stormstorage names it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub volume: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Moved while the VM was stopped (#339): no switch, and the record is
    /// the VirtualMachine's, carried onto each VMI.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stopped: bool,
    /// The disk is on the copy and the guest is not known to run yet (#449,
    /// e2198b663261): paused for the switch, or a kubelet stopped in
    /// between. The move is not done until the guest runs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resume: bool,
}

impl Move {
    /// Still being worked on: the mirror, the switch or the retirement.
    pub fn active(&self) -> bool {
        matches!(self.state.as_str(), COPYING | SWITCHING | SWITCHED)
    }

    /// Where the disk is now: (node, volume). `None` while that is not known
    /// for sure ([`UNKNOWN`]).
    pub fn location(&self) -> Option<(&str, &str)> {
        match self.state.as_str() {
            SWITCHED | MOVED => Some((&self.to, &self.volume)),
            UNKNOWN => None,
            _ => Some((&self.node, &self.from)),
        }
    }
}

/// The request on `vmi`: disk → node. A request that is not such an object
/// is an error naming it, said once by the caller.
pub fn requests(vmi: &Value) -> Result<BTreeMap<String, String>, String> {
    let Some(text) = vmi["metadata"]["annotations"][REQUEST].as_str() else { return Ok(BTreeMap::new()) };
    let v: Value = serde_json::from_str(text).map_err(|e| format!("{REQUEST} is not JSON: {e}"))?;
    let o = v.as_object().ok_or_else(|| format!("{REQUEST} is not an object of disk → node: {text}"))?;
    o.iter()
        .map(|(disk, node)| match node.as_str().filter(|n| !n.is_empty()) {
            Some(n) => Ok((disk.clone(), n.to_string())),
            None => Err(format!("{REQUEST}: disk {disk} names no node")),
        })
        .collect()
}

/// The record on `vmi`. One that does not parse is none.
pub fn records(vmi: &Value) -> BTreeMap<String, Move> {
    vmi["metadata"]["annotations"][RECORD]
        .as_str()
        .and_then(|t| serde_json::from_str(t).ok())
        .unwrap_or_default()
}

/// The metadata patch that writes `records` (all of them: the annotation is
/// one string).
pub fn record_patch(vmi_uid: &str, records: &BTreeMap<String, Move>) -> Value {
    let text = if records.is_empty() { Value::Null } else { json!(serde_json::to_string(records).unwrap_or_default()) };
    json!({"metadata": {"uid": vmi_uid, "annotations": {RECORD: text}}})
}

/// The disks a start attaches by their recorded volume, not by name: each
/// that sits on a volume a move made (disk → volume id). A disk left out is
/// attached by name, so a start first refuses the records whose disk is not
/// known for sure ([`cold_start_hold`]).
pub fn moved_volumes(vmi: &Value) -> BTreeMap<String, String> {
    volumes_of(records(vmi))
}

/// [`moved_volumes`] of these records.
pub fn volumes_of(records: BTreeMap<String, Move>) -> BTreeMap<String, String> {
    records
        .into_iter()
        .filter(|(_, m)| matches!(m.state.as_str(), SWITCHED | MOVED) && !m.volume.is_empty())
        .map(|(d, m)| (d, m.volume))
        .collect()
}

/// Why a machine must not start cold because of a running disk move (#454,
/// 15521beacabe), from the records it starts with. A start attaches each
/// disk by name unless [`moved_volumes`] names its copy, so a disk whose
/// volume is not known for sure must not start at all: the guest may have
/// written the copy after the cutover, and booting the source would give the
/// disk two histories.
///
/// - [`UNKNOWN`]: a person decides, by setting the disk's `state` in
///   [`RECORD`] to [`MOVED`] (the copy) or [`FAILED`] (the source).
/// - [`SWITCHING`] (the kubelet or the machine stopped during the switch):
///   `cutover` has each such record's stormstorage migration state, by
///   migration id. Still `synced` or `copying`: never cut over, the source is
///   the disk, and the start goes on. Any other state: as [`UNKNOWN`]. Not
///   read: held, and asked again.
/// - [`SWITCHED`] or [`MOVED`] with no copy recorded: nothing to attach.
pub fn cold_start_hold(records: &BTreeMap<String, Move>, cutover: &BTreeMap<String, Result<String, String>>) -> Option<String> {
    let decide = |d: &str, m: &Move| {
        format!(
            "a person decides: set disk {d}'s state in {RECORD} to `{MOVED}` (the copy {} on {}) or `{FAILED}` (the source {} on {})",
            if m.volume.is_empty() { "?" } else { m.volume.as_str() },
            m.to,
            m.from,
            m.node
        )
    };
    records.iter().find_map(|(d, m)| match m.state.as_str() {
        UNKNOWN => Some(format!(
            "disk {d}'s move to {}'s storage ended unknown (#302): the guest may have written the copy after the \
             cutover, so it does not start on either volume; {}",
            m.to,
            decide(d, m)
        )),
        SWITCHING => match cutover.get(&m.migration) {
            Some(Ok(s)) if matches!(s.as_str(), "synced" | "copying") => None,
            Some(Ok(s)) => Some(format!(
                "disk {d}'s switch to {}'s storage was left half way and migration {} is {s}: which volume holds \
                 the guest's writes is not known (#302); {}",
                m.to,
                m.migration,
                decide(d, m)
            )),
            Some(Err(e)) => Some(format!(
                "disk {d}'s switch to {}'s storage was left half way; migration {} could not be read ({e}): asked again",
                m.to, m.migration
            )),
            None => Some(format!(
                "disk {d}'s switch to {}'s storage was left half way, and this node has no stormstorage to ask \
                 whether migration {} was cut over (#302); {}",
                m.to,
                m.migration,
                decide(d, m)
            )),
        },
        SWITCHED | MOVED if m.volume.is_empty() => Some(format!(
            "disk {d} is recorded {} on {}'s storage with no volume: there is nothing to attach (#302); {}",
            m.state,
            m.to,
            decide(d, m)
        )),
        _ => None,
    })
}

/// The records a kubelet keeps (`kept`), with a person's verdict on the VMI
/// (`on_vmi`) taken over them (#454): a disk this kubelet holds [`UNKNOWN`]
/// or [`SWITCHING`] that the VMI says is [`MOVED`] or [`FAILED`], for the
/// same migration. The kubelet never writes either after those states for
/// one migration, so that is a person's answer, not a stale write of its own.
pub fn with_verdicts(mut kept: BTreeMap<String, Move>, on_vmi: &BTreeMap<String, Move>) -> BTreeMap<String, Move> {
    for (d, m) in kept.iter_mut() {
        if !matches!(m.state.as_str(), UNKNOWN | SWITCHING) {
            continue;
        }
        if let Some(v) = on_vmi.get(d).filter(|v| v.migration == m.migration && matches!(v.state.as_str(), MOVED | FAILED)) {
            *m = v.clone();
        }
    }
    kept
}

/// Is any disk of `vmi` on another node than `node`, or not known to be
/// here? A live migration (#40) mirrors from this node's engine, so it does
/// not take such a machine yet.
pub fn off_node(vmi: &Value, node: &str) -> Option<String> {
    records(vmi).into_iter().find_map(|(d, m)| match m.location() {
        Some((n, _)) if n == node => None,
        Some((n, _)) => Some(format!("disk {d} is on {n}'s storage (#302)")),
        None => Some(format!("disk {d}'s move ended unknown (#302)")),
    })
}

/// The node a stopped VirtualMachine's disks are asked onto (#339).
pub fn stopped_request(vm: &Value) -> Option<String> {
    vm["metadata"]["annotations"][MOVE_TO].as_str().map(str::trim).filter(|n| !n.is_empty()).map(String::from)
}

/// The record on a VirtualMachine's template (#339). One that does not
/// parse is none.
pub fn template_records(vm: &Value) -> BTreeMap<String, Move> {
    vm["spec"]["template"]["metadata"]["annotations"][RECORD]
        .as_str()
        .and_then(|t| serde_json::from_str(t).ok())
        .unwrap_or_default()
}

/// The merge patch of a stopped VM's move: the record on its template, the
/// account on the VM, and (`place_on`) the template's node. Guarded by the
/// VM's `resourceVersion`: two kubelets each write the disks they hold.
pub fn vm_patch(rv: &str, records: &BTreeMap<String, Move>, status: Option<&str>, place_on: Option<&str>) -> Value {
    let text = if records.is_empty() { Value::Null } else { json!(serde_json::to_string(records).unwrap_or_default()) };
    let mut p = json!({
        "metadata": {"resourceVersion": rv},
        "spec": {"template": {"metadata": {"annotations": {RECORD: text}}}},
    });
    if let Some(s) = status {
        p["metadata"]["annotations"] = json!({ MOVE_STATUS: s });
    }
    if let Some(n) = place_on {
        p["spec"]["template"]["spec"] = json!({"nodeSelector": {HOSTNAME: n}});
    }
    p
}

/// Why a VMI must not start yet because of a stopped VM's move (#339), given
/// the record it is started with: a disk still being moved, or the disks
/// left on more than one node's storage (a move that failed half way).
/// Records of running moves (#302) alone never hold a start.
pub fn start_hold(records: &BTreeMap<String, Move>) -> Option<String> {
    if !records.values().any(|m| m.stopped) {
        return None;
    }
    if let Some((d, m)) = records.iter().find(|(_, m)| m.stopped && m.active()) {
        return Some(format!("disk {d} is being moved to {}'s storage (#339): the VM starts once it is there", m.to));
    }
    let mut on: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (d, m) in records {
        match m.location() {
            Some((n, _)) => on.entry(n).or_default().push(d),
            None => return Some(format!("disk {d}'s move ended unknown (#339): a person decides which volume it is")),
        }
    }
    (on.len() > 1).then(|| {
        let where_: Vec<String> = on.iter().map(|(n, ds)| format!("{} on {n}", ds.join(", "))).collect();
        format!(
            "the disks are on more than one node's storage ({}): move them to one node (storm.io/move-to) first (#339)",
            where_.join("; ")
        )
    })
}

/// Every disk recorded is on `node` and done: where the next start goes.
pub fn all_moved_to(records: &BTreeMap<String, Move>, node: &str) -> bool {
    !records.is_empty() && records.values().all(|m| m.state == MOVED && m.to == node)
}

/// The one-line account of a stopped VM's move, for [`MOVE_STATUS`].
pub fn summary(to: Option<&str>, records: &BTreeMap<String, Move>) -> String {
    if let Some(to) = to.filter(|t| all_moved_to(records, t)) {
        return format!("moved: every disk is on {to}'s storage; the next start is scheduled there");
    }
    let mut parts = Vec::new();
    for (d, m) in records {
        let p = match m.state.as_str() {
            COPYING if m.bytes_total > 0 => format!(
                "{d} copying to {} ({}%)",
                m.to,
                100 - (m.bytes_remaining.min(m.bytes_total) * 100 / m.bytes_total)
            ),
            COPYING => format!("{d} copying to {}", m.to),
            SWITCHED => format!("{d} cut over to {}, retiring the source", m.to),
            MOVED => format!("{d} on {}", m.to),
            FAILED => format!("{d} failed, on {}: {}", m.node, m.message),
            other => format!("{d} {other}: {}", m.message),
        };
        parts.push(p);
    }
    let mut s = if records.values().any(Move::active) {
        format!("moving: {}", parts.join("; "))
    } else if records.values().any(|m| m.state == FAILED) {
        format!("failed: {}", parts.join("; "))
    } else {
        parts.join("; ")
    };
    if let Some(why) = start_hold(records).filter(|_| !records.values().any(Move::active)) {
        s.push_str(&format!(". It will not start: {why}"));
    }
    s
}

/// What a pass does about one disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Nothing to do.
    Nothing,
    /// Start a move to this node.
    Start(String),
    /// A move in flight: carry on.
    Continue,
    /// A move still copying that is no longer asked for (or asked elsewhere):
    /// abort it.
    Cancel,
    /// A record that says nothing any more (failed, nothing asked): dropped.
    Forget,
}

/// What a pass does for a disk asked to go to `want` (None: not asked),
/// with `record` as it stands; `here` is this node.
pub fn step(want: Option<&str>, record: Option<&Move>, here: &str) -> Step {
    let Some(r) = record else {
        return match want {
            Some(to) if to != here => Step::Start(to.to_string()),
            _ => Step::Nothing,
        };
    };
    match r.state.as_str() {
        COPYING if want != Some(r.to.as_str()) => Step::Cancel,
        SWITCHING | SWITCHED | COPYING => Step::Continue,
        // Never acted on without a person (and never forgotten).
        UNKNOWN => Step::Nothing,
        MOVED => match want {
            Some(to) if to != r.to => Step::Start(to.to_string()),
            _ => Step::Nothing,
        },
        // Failed (or cancelled): asked again elsewhere starts again; asked
        // the same, it stays failed until the request changes; not asked,
        // the record goes.
        _ => match want {
            None => Step::Forget,
            Some(to) if to != r.to && to != r.node => Step::Start(to.to_string()),
            _ => Step::Nothing,
        },
    }
}

/// Why disk `disk` of a machine cannot be moved, if it cannot. `kind` is the
/// hypervisor; `owned` whether its volume is one the machine made (a golden's
/// clone or an emptyDisk); `seed` whether it is the cloud-init seed;
/// `migrating` whether a live migration (#40) is active.
pub fn refusal(disk: &str, kind: &str, volume: Option<&str>, owned: bool, seed: bool, migrating: bool) -> Option<String> {
    if stormvm_control::Kind::parse(kind) == stormvm_control::Kind::CloudHypervisor {
        return Some("cloud-hypervisor cannot put a running disk on another device (stormvm#29)".into());
    }
    if migrating {
        return Some("the machine is being migrated (#40); its disks move with it".into());
    }
    if seed {
        return Some(format!("disk {disk} is the cloud-init seed, made again at every start: nothing to move"));
    }
    if volume.is_none() || !owned {
        return Some(format!(
            "disk {disk} is not the machine's own volume (a claim is its driver's to move, a volume it was handed \
             is somebody else's)"
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(state: &str, volume: &str) -> Move {
        Move { to: "b".into(), node: "a".into(), from: "v-src".into(), migration: "mig-1".into(), state: state.into(), volume: volume.into(), ..Default::default() }
    }

    /// #454 (15521beacabe): no cold start while a disk's volume is not known
    /// for sure; a switch left before the cutover starts on the source.
    #[test]
    fn a_cold_start_waits_for_the_disks_known_volume() {
        let one = |m: Move| BTreeMap::from([("root".to_string(), m)]);
        let none = BTreeMap::new();
        let why = cold_start_hold(&one(held(UNKNOWN, "v-copy")), &none).unwrap();
        assert!(why.contains("ended unknown") && why.contains("`moved` (the copy v-copy on b)") && why.contains("`failed` (the source v-src on a)"), "{why}");
        let cut = |s: Result<&str, &str>| BTreeMap::from([("mig-1".to_string(), s.map(String::from).map_err(String::from))]);
        let switching = one(held(SWITCHING, "v-copy"));
        assert_eq!(cold_start_hold(&switching, &cut(Ok("synced"))), None, "never cut over: the source is the disk");
        assert_eq!(cold_start_hold(&switching, &cut(Ok("copying"))), None);
        assert!(cold_start_hold(&switching, &cut(Ok("cutover"))).unwrap().contains("migration mig-1 is cutover"));
        assert!(cold_start_hold(&switching, &cut(Err("503"))).unwrap().contains("asked again"));
        assert!(cold_start_hold(&switching, &none).unwrap().contains("no stormstorage"));
        assert!(cold_start_hold(&one(held(SWITCHED, "")), &none).unwrap().contains("nothing to attach"));
        for ok in [held(SWITCHED, "v-copy"), held(MOVED, "v-copy"), held(COPYING, ""), held(FAILED, "")] {
            assert_eq!(cold_start_hold(&one(ok.clone()), &none), None, "{ok:?}");
        }
        assert_eq!(cold_start_hold(&BTreeMap::new(), &none), None);
        // What is not held starts on a known volume: the copy, or by name.
        assert_eq!(volumes_of(one(held(MOVED, "v-copy"))), BTreeMap::from([("root".to_string(), "v-copy".to_string())]));
        assert!(volumes_of(one(held(COPYING, ""))).is_empty());
    }

    /// #454: a person's `moved` or `failed` on the VMI ends the kubelet's
    /// own unknown or half-switched record, for the same migration only.
    #[test]
    fn a_persons_verdict_replaces_an_unknown_record() {
        let kept = BTreeMap::from([("root".to_string(), held(UNKNOWN, "v-copy")), ("data".to_string(), held(SWITCHING, "v-c2"))]);
        let mut said = held(MOVED, "v-copy");
        said.message = "decided".into();
        let vmi = BTreeMap::from([("root".to_string(), said.clone()), ("data".to_string(), held(FAILED, ""))]);
        let out = with_verdicts(kept.clone(), &vmi);
        assert_eq!(out["root"], said);
        assert_eq!(out["data"].state, FAILED);
        let other = BTreeMap::from([("root".to_string(), Move { migration: "mig-0".into(), ..said.clone() })]);
        assert_eq!(with_verdicts(kept.clone(), &other)["root"].state, UNKNOWN, "another migration's record");
        let stale = BTreeMap::from([("root".to_string(), held(SWITCHING, "v-copy"))]);
        assert_eq!(with_verdicts(kept.clone(), &stale)["root"].state, UNKNOWN, "only moved or failed is a verdict");
        let mut done = kept.clone();
        done.insert("root".into(), held(SWITCHED, "v-copy"));
        assert_eq!(with_verdicts(done, &vmi)["root"].state, SWITCHED, "a record not unknown is the kubelet's");
    }

    fn vmi(request: Option<Value>, record: Option<&BTreeMap<String, Move>>) -> Value {
        let mut a = serde_json::Map::new();
        if let Some(r) = request {
            a.insert(REQUEST.into(), json!(r.to_string()));
        }
        if let Some(r) = record {
            a.insert(RECORD.into(), json!(serde_json::to_string(r).unwrap()));
        }
        json!({"metadata": {"uid": "u1", "annotations": a}})
    }

    fn rec(state: &str) -> Move {
        Move {
            to: "b".into(),
            node: "a".into(),
            from: "v-a".into(),
            migration: "m1".into(),
            state: state.into(),
            volume: "v-b".into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_request_names_a_node_per_disk() {
        assert!(requests(&vmi(None, None)).unwrap().is_empty());
        let r = requests(&vmi(Some(json!({"data": "b"})), None)).unwrap();
        assert_eq!(r, BTreeMap::from([("data".to_string(), "b".to_string())]));
        assert!(requests(&vmi(Some(json!(["b"])), None)).unwrap_err().contains("not an object"));
        assert!(requests(&vmi(Some(json!({"data": ""})), None)).unwrap_err().contains("names no node"));
    }

    #[test]
    fn the_record_round_trips_and_names_the_volumes_a_start_uses() {
        let records = BTreeMap::from([
            ("data".to_string(), rec(MOVED)),
            ("logs".to_string(), rec(COPYING)),
            ("old".to_string(), rec(FAILED)),
        ]);
        let patch = record_patch("u1", &records);
        let mut v = vmi(None, None);
        v["metadata"]["annotations"] = patch["metadata"]["annotations"].clone();
        assert_eq!(super::records(&v), records);
        assert_eq!(moved_volumes(&v), BTreeMap::from([("data".to_string(), "v-b".to_string())]));
        assert_eq!(off_node(&v, "a").unwrap(), "disk data is on b's storage (#302)");
        assert!(record_patch("u1", &BTreeMap::new())["metadata"]["annotations"][RECORD].is_null(), "removed");
        let camel = serde_json::to_value(rec(COPYING)).unwrap();
        assert!(camel["bytesRemaining"].is_u64() && camel["migration"] == "m1");
    }

    #[test]
    fn each_pass_knows_what_to_do() {
        assert_eq!(step(None, None, "a"), Step::Nothing);
        assert_eq!(step(Some("a"), None, "a"), Step::Nothing, "already here");
        assert_eq!(step(Some("b"), None, "a"), Step::Start("b".into()));
        assert_eq!(step(Some("b"), Some(&rec(COPYING)), "a"), Step::Continue);
        assert_eq!(step(Some("c"), Some(&rec(COPYING)), "a"), Step::Cancel, "asked elsewhere mid-copy");
        assert_eq!(step(None, Some(&rec(COPYING)), "a"), Step::Cancel, "no longer asked");
        assert_eq!(step(None, Some(&rec(SWITCHING)), "a"), Step::Continue, "past the point of cancelling");
        assert_eq!(step(None, Some(&rec(SWITCHED)), "a"), Step::Continue);
        assert_eq!(step(Some("b"), Some(&rec(MOVED)), "a"), Step::Nothing);
        assert_eq!(step(None, Some(&rec(MOVED)), "a"), Step::Nothing, "the record is where the disk is");
        assert_eq!(step(Some("a"), Some(&rec(MOVED)), "a"), Step::Start("a".into()), "back home");
        assert_eq!(step(Some("b"), Some(&rec(FAILED)), "a"), Step::Nothing, "not retried as it stands");
        assert_eq!(step(None, Some(&rec(FAILED)), "a"), Step::Forget);
        assert_eq!(step(Some("c"), Some(&rec(FAILED)), "a"), Step::Start("c".into()));
        assert_eq!(step(None, Some(&rec(UNKNOWN)), "a"), Step::Nothing, "a person decides");
    }

    #[test]
    fn where_the_disk_is() {
        assert_eq!(rec(COPYING).location(), Some(("a", "v-a")));
        assert_eq!(rec(FAILED).location(), Some(("a", "v-a")));
        assert_eq!(rec(SWITCHED).location(), Some(("b", "v-b")));
        assert_eq!(rec(MOVED).location(), Some(("b", "v-b")));
        assert_eq!(rec(UNKNOWN).location(), None);
    }

    fn stopped(state: &str, node: &str, to: &str) -> Move {
        Move { node: node.into(), to: to.into(), stopped: true, ..rec(state) }
    }

    #[test]
    fn a_stopped_vms_request_and_record_live_on_the_vm() {
        let vm = json!({"metadata": {"annotations": {MOVE_TO: " b "}}, "spec": {"template": {"metadata": {"annotations": {
            RECORD: serde_json::to_string(&BTreeMap::from([("root".to_string(), stopped(COPYING, "a", "b"))])).unwrap()
        }}}}});
        assert_eq!(stopped_request(&vm).as_deref(), Some("b"));
        assert!(stopped_request(&json!({"metadata": {"annotations": {MOVE_TO: ""}}})).is_none());
        let r = template_records(&vm);
        assert!(r["root"].stopped && r["root"].state == COPYING);
        assert!(serde_json::to_value(rec(COPYING)).unwrap().get("stopped").is_none(), "#302's records unchanged");

        let p = vm_patch("42", &r, Some("moving"), Some("b"));
        assert_eq!(p["metadata"]["resourceVersion"], "42");
        assert_eq!(p["metadata"]["annotations"][MOVE_STATUS], "moving");
        assert_eq!(p["spec"]["template"]["spec"]["nodeSelector"][HOSTNAME], "b");
        let mut carried = json!({"spec": {"template": {"metadata": {}}}});
        carried["spec"]["template"]["metadata"] = p["spec"]["template"]["metadata"].clone();
        assert_eq!(template_records(&carried), r, "round trip");
        let bare = vm_patch("42", &BTreeMap::new(), None, None);
        assert!(bare["spec"]["template"]["metadata"]["annotations"][RECORD].is_null() && bare["spec"]["template"]["spec"].is_null());
    }

    #[test]
    fn a_start_waits_for_a_stopped_vms_move() {
        let one = |d: &str, m: Move| BTreeMap::from([(d.to_string(), m)]);
        assert_eq!(start_hold(&BTreeMap::new()), None);
        assert_eq!(start_hold(&one("root", rec(COPYING))), None, "a running move (#302) never holds a start");
        assert!(start_hold(&one("root", stopped(COPYING, "a", "b"))).unwrap().contains("being moved to b"));
        assert!(start_hold(&one("root", stopped(SWITCHED, "a", "b"))).is_some(), "the source is not retired yet");
        assert_eq!(start_hold(&one("root", stopped(MOVED, "a", "b"))), None);
        assert_eq!(start_hold(&one("root", stopped(FAILED, "a", "b"))), None, "failed: still on a, one node");
        let half = BTreeMap::from([
            ("root".to_string(), stopped(MOVED, "a", "b")),
            ("data".to_string(), stopped(FAILED, "a", "b")),
        ]);
        let why = start_hold(&half).unwrap();
        assert!(why.contains("data on a") && why.contains("root on b"), "{why}");
        assert!(start_hold(&one("root", stopped(UNKNOWN, "a", "b"))).unwrap().contains("unknown"));
    }

    #[test]
    fn a_stopped_vms_move_reads_in_one_line() {
        let mut copying = stopped(COPYING, "a", "b");
        copying.bytes_total = 200;
        copying.bytes_remaining = 50;
        let r = BTreeMap::from([("root".to_string(), copying), ("data".to_string(), stopped(MOVED, "a", "b"))]);
        assert_eq!(summary(Some("b"), &r), "moving: data on b; root copying to b (75%)");
        let done = BTreeMap::from([("root".to_string(), stopped(MOVED, "a", "b"))]);
        assert!(all_moved_to(&done, "b") && !all_moved_to(&done, "c") && !all_moved_to(&BTreeMap::new(), "b"));
        assert!(summary(Some("b"), &done).starts_with("moved: every disk is on b"));
        let mut f = stopped(FAILED, "a", "b");
        f.message = "the copy failed".into();
        let half = BTreeMap::from([("root".to_string(), stopped(MOVED, "a", "b")), ("data".to_string(), f)]);
        let s = summary(Some("b"), &half);
        assert!(s.starts_with("failed: data failed, on a: the copy failed; root on b") && s.contains("will not start"), "{s}");
    }

    #[test]
    fn what_cannot_move_says_why() {
        assert_eq!(refusal("data", "qemu", Some("v"), true, false, false), None);
        assert!(refusal("data", "cloud-hypervisor", Some("v"), true, false, false).unwrap().contains("cloud-hypervisor"));
        assert!(refusal("data", "qemu", Some("v"), true, false, true).unwrap().contains("#40"));
        assert!(refusal("cloudinit", "qemu", Some("v"), true, true, false).unwrap().contains("seed"));
        assert!(refusal("data", "qemu", None, false, false, false).unwrap().contains("claim"));
        assert!(refusal("data", "qemu", Some("v"), false, false, false).unwrap().contains("own volume"));
    }
}
