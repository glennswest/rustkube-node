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
/// that sits on a volume a move made (disk → volume id).
pub fn moved_volumes(vmi: &Value) -> BTreeMap<String, String> {
    records(vmi)
        .into_iter()
        .filter(|(_, m)| matches!(m.state.as_str(), SWITCHED | MOVED) && !m.volume.is_empty())
        .map(|(d, m)| (d, m.volume))
        .collect()
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
