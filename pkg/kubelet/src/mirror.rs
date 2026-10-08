//! The node's own services, as pods.
//!
//! **stormpump runs the things that make a node a node** — the storage engine,
//! the registry, the control plane itself — and none of it is visible to
//! anyone holding `oc`. They are not pods. Nothing ever created an API object
//! for them. Their logs are on volumes a person cannot reach, and their
//! restart counts live in PID 1's memory.
//!
//! That gap cost most of a day: a workload that would not start was diagnosed
//! from a single status string, while the supervisor knew the state, the
//! restart count and the exit code the whole time.
//!
//! This is Kubernetes' own answer to the same problem. A kubelet that runs
//! something directly creates a **mirror pod**: a read-only object in the API
//! that says "this is running here", so `get`, `describe` and `logs` work on
//! it like anything else. Upstream does it for static pods; this does it for
//! whatever PID 1 reports.
//!
//! What a mirror pod is *not* is a scheduling decision. Nothing acts on these:
//! the scheduler does not place them, deleting one does not stop anything, and
//! the annotation says where they came from. They exist to be read.

use serde_json::{json, Value};

/// One asset, as PID 1 reports it in `/run/stormpump/assets.json`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Asset {
    pub name: String,
    pub running: bool,
    pub restarts: u32,
    pub age_secs: u64,
    /// When its running incarnation started, in `CLOCK_BOOTTIME` seconds, the
    /// clock `/proc/uptime` reads (stormpump#67). Unlike `age_secs` it does not
    /// go stale between writes of assets.json. `None` from an older PID 1.
    pub started_secs: Option<f64>,
    /// How it last ended (stormpump#51), once it has: kept across the
    /// restart, so a crash-looping service carries its reason while it is
    /// briefly running again (#82).
    pub last_exit: Option<LastExit>,
}

impl Asset {
    /// How long its running incarnation has been up, in whole seconds (#193):
    /// the node's uptime (`/proc/uptime`, same clock) less `started_secs`,
    /// which holds still while assets.json is not rewritten; else
    /// `age_secs`, the age as of that file's last write (an older PID 1,
    /// which rewrites it often enough).
    pub fn age(&self, uptime: Option<f64>) -> u64 {
        match (self.started_secs, uptime) {
            (Some(started), Some(up)) => (up - started).max(0.0) as u64,
            _ => self.age_secs,
        }
    }
}

/// The node's uptime in seconds, `CLOCK_BOOTTIME` (`/proc/uptime`'s first
/// field): the clock stormpump's `started_secs` is in.
pub fn node_uptime() -> Option<f64> {
    std::fs::read_to_string("/proc/uptime").ok()?.split_whitespace().next()?.parse().ok()
}

/// A node service's last exit, from assets.json (stormpump#51).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct LastExit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
    /// PID 1's words: `exited 1`, `killed by signal 9`.
    pub text: String,
    /// Its last non-blank output lines (20).
    pub output: Vec<String>,
}

/// Upstream's cap on a terminated container's `message`: the last 80 lines,
/// at most 4 KiB.
const MESSAGE_LINES: usize = 80;
const MESSAGE_BYTES: usize = 4096;

impl LastExit {
    fn of(a: &Value) -> Option<Self> {
        let code = a["last_exit_code"].as_i64().map(|c| c as i32);
        let signal = a["last_exit_signal"].as_i64().map(|s| s as i32);
        let text = a["last_exit"].as_str().unwrap_or("").to_string();
        let output: Vec<String> = a["last_output"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| l.as_str().map(str::to_string))
            .collect();
        (code.is_some() || signal.is_some() || !text.is_empty() || !output.is_empty())
            .then_some(Self { code, signal, text, output })
    }

    /// The exit code a terminated container reports: the code, else 128 plus
    /// the signal, as a shell and upstream's runtimes say it.
    pub fn exit_code(&self) -> i32 {
        self.code.or(self.signal.map(|s| 128 + s)).unwrap_or(-1)
    }

    /// The output's tail, within upstream's cap (whole lines, newest kept).
    pub fn tail(&self) -> String {
        let mut lines: Vec<&str> = Vec::new();
        let mut bytes = 0;
        for line in self.output.iter().rev().take(MESSAGE_LINES) {
            if bytes + line.len() + 1 > MESSAGE_BYTES {
                break;
            }
            bytes += line.len() + 1;
            lines.push(line);
        }
        lines.reverse();
        lines.join("\n")
    }

    /// For an Event: what ended it, then the tail.
    pub fn summary(&self) -> String {
        let what = if self.text.is_empty() { format!("exit code {}", self.exit_code()) } else { self.text.clone() };
        match self.tail() {
            t if t.is_empty() => what,
            t => format!("{what}; last output:\n{t}"),
        }
    }

    /// `terminated` for a container status, with `finishedAt` when known.
    fn terminated(&self, finished: Option<&str>) -> Value {
        let mut t = json!({ "exitCode": self.exit_code(), "reason": "Error" });
        if let Some(s) = self.signal {
            t["signal"] = json!(s);
        }
        let message = self.tail();
        t["message"] = json!(if message.is_empty() { self.text.clone() } else { message });
        if let Some(f) = finished {
            t["finishedAt"] = json!(f);
        }
        t
    }
}

/// Parse the asset table.
///
/// Hand-parsed rather than pulled through a schema: the file is written by
/// PID 1 with `write!` and no serialiser, so the two ends are deliberately
/// small and the shape is three fields. A file that cannot be read at all is
/// an empty list — the node still works, it just cannot be seen, and that is
/// not a reason to fail anything.
pub fn parse_assets(text: &str) -> Vec<Asset> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let Some(items) = v["assets"].as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|a| {
            let name = a["name"].as_str()?.to_string();
            if name.is_empty() {
                return None;
            }
            Some(Asset {
                name,
                running: a["running"].as_bool().unwrap_or(false),
                restarts: a["restarts"].as_u64().unwrap_or(0) as u32,
                age_secs: a["age_secs"].as_u64().unwrap_or(0),
                started_secs: a["started_secs"].as_f64(),
                last_exit: LastExit::of(a),
            })
        })
        .collect()
}

/// The pod name for an asset.
///
/// `<asset>-<node>`, which is upstream's convention for a static pod's mirror:
/// the name has to be unique across the cluster, and two nodes both running a
/// storage engine is the normal case rather than a collision.
pub fn mirror_name(asset: &str, node: &str) -> String {
    format!("{asset}-{node}")
}

/// Build the mirror pod for an asset.
///
/// `config.source: stormpump` is the annotation that says this object
/// describes something the API did not schedule — upstream writes `file` or
/// `http` there for the same reason. The owner reference is the Node, so the
/// object is collected when the node goes away and nothing has to remember to
/// clean it up.
pub fn mirror_pod(asset: &Asset, node: &str, node_uid: &str, started: &str) -> Value {
    mirror_pod_with(asset, node, node_uid, started, None)
}

/// [`mirror_pod`], with what the service's own health endpoint says (#96): a
/// running service that does not answer is `Running` but not ready, its
/// container `ready: false` and `Ready=False` with why, as upstream reports a
/// running container failing its readiness probe.
pub fn mirror_pod_with(
    asset: &Asset,
    node: &str,
    node_uid: &str,
    started: &str,
    health: Option<&crate::node_health::ServiceHealth>,
) -> Value {
    let phase = if asset.running { "Running" } else { "Failed" };
    let unhealthy = health.filter(|h| asset.running && !h.ready);
    let ready = asset.running && unhealthy.is_none();
    let state = if asset.running {
        json!({ "running": { "startedAt": started } })
    } else {
        match &asset.last_exit {
            // How it died, from PID 1 (stormpump#51, #82).
            Some(exit) => json!({ "terminated": exit.terminated(Some(started)) }),
            // Not said: never a fabricated 0, which would read as a clean exit.
            None => json!({ "terminated": { "reason": "Error", "finishedAt": started } }),
        }
    };

    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": mirror_name(&asset.name, node),
            "namespace": "kube-system",
            "annotations": {
                "kubernetes.io/config.source": "stormpump",
                // Says plainly that editing it changes nothing, because a
                // read-only object that looks writable invites someone to try.
                "storm.io/mirror": "true",
            },
            "labels": {
                "storm.io/asset": asset.name,
                "storm.io/component": "node-service",
            },
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "Node",
                "name": node,
                "uid": node_uid,
                "controller": true,
            }],
        },
        "spec": {
            "nodeName": node,
            "hostNetwork": true,
            // Never evicted and never rescheduled: this describes something
            // PID 1 is already running, and moving it is not a thing anyone
            // can do from here.
            "priorityClassName": "system-node-critical",
            "tolerations": [{ "operator": "Exists" }],
            "containers": [{
                "name": asset.name,
                "image": format!("stormpump://{}", asset.name),
            }],
        },
        "status": {
            "phase": phase,
            "hostIP": "",
            "startTime": started,
            "conditions": [
                { "type": "PodScheduled", "status": "True" },
                { "type": "Initialized", "status": "True" },
                { "type": "ContainersReady",
                  "status": if ready { "True" } else { "False" } },
                { "type": "Ready",
                  "status": if ready { "True" } else { "False" },
                  "reason": if unhealthy.is_some() { json!("Unhealthy") } else { Value::Null },
                  "message": unhealthy.map_or(Value::Null, |h| json!(format!("health endpoint: {}", h.reason))) },
            ],
            "containerStatuses": [container_status(asset, ready, state)],
        }
    })
}

/// The mirror's one container status: with `lastState.terminated` when PID 1
/// has seen the service exit (#82), as upstream reports a restarted container.
fn container_status(asset: &Asset, ready: bool, state: Value) -> Value {
    let mut cs = json!({
        "name": asset.name,
        "image": format!("stormpump://{}", asset.name),
        "ready": ready,
        "restartCount": asset.restarts,
        "state": state,
    });
    if let Some(exit) = &asset.last_exit {
        cs["lastState"] = json!({ "terminated": exit.terminated(None) });
    }
    cs
}

/// What of the asset table a mirror reflects (#101). PID 1 rewrites the file
/// every pass with fresh ages (stormpump#67); a change is a different name
/// set, a service starting or stopping, or a restart.
pub fn table_key(assets: &[Asset]) -> Vec<(String, bool, u32, Option<LastExit>)> {
    let mut key: Vec<_> = assets
        .iter()
        .map(|a| (a.name.clone(), a.running, a.restarts, a.last_exit.clone()))
        .collect();
    key.sort();
    key
}

/// The state a container status is in: `running`, `terminated` or `waiting`.
fn state_kind(cs: &Value) -> Option<&str> {
    ["running", "terminated", "waiting"].into_iter().find(|k| cs["state"][*k].is_object())
}

/// How far apart two RFC 3339 times are, in seconds; `None` when either is
/// not one.
fn seconds_apart(a: &Value, b: &Value) -> Option<i64> {
    let parse = |v: &Value| chrono::DateTime::parse_from_rfc3339(v.as_str()?).ok();
    Some((parse(a)? - parse(b)?).num_seconds().abs())
}

/// Does the mirror pod already say what `want` would write (#101)? Nothing is
/// written when it does: the mirror watches its own pods, so a write that
/// changes nothing would wake it again, for ever. `startTime` is derived from
/// an age and may move by a second or two between passes; that is not a change.
pub fn status_current(existing: &Value, want: &Value) -> bool {
    let (e, w) = (&existing["status"], &want["status"]);
    let conditions = |s: &Value| -> Vec<(String, String)> {
        let mut c: Vec<_> = s["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|c| (c["type"].as_str().unwrap_or("").to_string(), c["status"].as_str().unwrap_or("").to_string()))
            .collect();
        c.sort();
        c
    };
    let (ec, wc) = (&e["containerStatuses"][0], &w["containerStatuses"][0]);
    let start_same = match (&e["startTime"], &w["startTime"]) {
        (Value::Null, Value::Null) => true,
        (a, b) => seconds_apart(a, b).is_some_and(|d| d <= 5),
    };
    e["phase"] == w["phase"]
        && conditions(e) == conditions(w)
        && ec["ready"] == wc["ready"]
        && ec["restartCount"] == wc["restartCount"]
        && state_kind(ec) == state_kind(wc)
        && ec["lastState"] == wc["lastState"]
        && ec["state"]["terminated"]["message"] == wc["state"]["terminated"]["message"]
        && ec["imageID"] == wc["imageID"]
        && e["hostIP"] == w["hostIP"]
        && start_same
}

/// The release manifest's word on a service's golden (#130): its device
/// digest as the container's `imageID`, its name and provenance as the
/// annotations `storm.io/golden` and `storm.io/golden-provenance` (so a
/// console can look it up), and the node's address as `hostIP`.
pub fn with_provenance(pod: &mut Value, golden: Option<&crate::image_config::Provenance>, host_ip: &str) {
    pod["status"]["hostIP"] = json!(host_ip);
    let Some(g) = golden else { return };
    if let Some(d) = &g.digest {
        pod["status"]["containerStatuses"][0]["imageID"] = json!(d);
    }
    for (key, v) in golden_annotations(g) {
        pod["metadata"]["annotations"][key] = v;
    }
}

/// The annotations [`with_provenance`] sets.
pub fn golden_annotations(g: &crate::image_config::Provenance) -> serde_json::Map<String, Value> {
    let mut out = serde_json::Map::new();
    if let Some(name) = &g.golden {
        out.insert(GOLDEN.into(), json!(name));
    }
    if let Some(p) = g.build.get("provenance") {
        out.insert(GOLDEN_PROVENANCE.into(), p.clone());
    }
    out
}

/// The golden a mirror pod's service runs from (#130).
pub const GOLDEN: &str = "storm.io/golden";
/// Its provenance, as the release manifest records it (`stormlb@b8ba1a7`).
pub const GOLDEN_PROVENANCE: &str = "storm.io/golden-provenance";

/// What a node service's change is, as an Event on its mirror pod (#50):
/// `(type, reason, message)`, or `None` when nothing changed.
///
/// `last` is what this kubelet saw on its previous pass (`None` the first
/// time), `existing` the mirror pod as the API has it, `started` the service's
/// start time (from its age). **First sight is judged against the API**, not
/// skipped: a service whose mirror pod already shows this run (its start time
/// and restart count) is not new, so a kubelet restart announces nothing, but
/// one started since the pod was last written is `Started`, with when.
pub fn transition(
    last: Option<(bool, u32)>,
    a: &Asset,
    existing: Option<&Value>,
    started: &str,
) -> Option<(&'static str, &'static str, String)> {
    let exit = a.last_exit.as_ref().map(|e| format!(": {}", e.summary())).unwrap_or_default();
    let failed = a.last_exit.as_ref().is_some_and(|e| e.code.is_some_and(|c| c != 0) || e.signal.is_some());
    let ended = || {
        if failed {
            ("Warning", "Failed", format!("the service exited{exit}"))
        } else {
            ("Warning", "Stopped", format!("the service is no longer running{exit}"))
        }
    };
    let up = || ("Normal", "Started", format!("the service is running (started {started})"));
    match last {
        Some((was_running, was_restarts)) => {
            if a.restarts > was_restarts {
                Some(("Warning", "BackOff", format!(
                    "restarted {} time(s); PID 1 has restarted it {} times{exit}",
                    a.restarts - was_restarts,
                    a.restarts
                )))
            } else if was_running && !a.running {
                Some(ended())
            } else if !was_running && a.running {
                Some(up())
            } else {
                None
            }
        }
        None => {
            let cs = existing.map(|p| &p["status"]["containerStatuses"][0]);
            let said_running = cs.is_some_and(|c| c["state"]["running"].is_object());
            if a.running {
                let same_run = cs.is_some_and(|c| {
                    said_running
                        && c["restartCount"].as_u64() == Some(u64::from(a.restarts))
                        && seconds_apart(&c["state"]["running"]["startedAt"], &json!(started)).is_some_and(|d| d <= 5)
                });
                (!same_run).then(up)
            } else if said_running {
                Some(ended())
            } else {
                None
            }
        }
    }
}

/// The reason a mirror pod carries when its asset is not in PID 1's table.
pub const NOT_STARTED: &str = "NotStarted";

/// This node's mirror pods whose asset PID 1 did not list on this boot, and
/// that do not say so yet (#87).
///
/// PID 1 lists every asset it tried to start, refused ones included. One it
/// did not try (its `start` line gone, or the boot stopped before its unit)
/// is simply absent, and the mirror used to write only listed assets, so the
/// pod kept the previous boot's `Running` and `startTime` for good.
///
/// `pods` is a PodList. Only pods labelled as node services, on this node,
/// are considered; one already marked [`NOT_STARTED`] is skipped, so a pass
/// writes nothing when nothing changed.
pub fn stale_mirrors<'a>(pods: &'a Value, node: &str, assets: &[Asset]) -> Vec<&'a Value> {
    let Some(items) = pods["items"].as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter(|p| p["metadata"]["labels"]["storm.io/component"] == "node-service")
        .filter(|p| p["spec"]["nodeName"].as_str() == Some(node))
        .filter(|p| {
            let asset = p["metadata"]["labels"]["storm.io/asset"].as_str().unwrap_or("");
            !asset.is_empty() && !assets.iter().any(|a| a.name == asset)
        })
        .filter(|p| p["status"]["containerStatuses"][0]["state"]["waiting"]["reason"] != NOT_STARTED)
        .collect()
}

/// `pod` with the status of an asset that is not running on this boot.
///
/// Pending with the container waiting, not Failed: nothing ran and failed,
/// so there is no exit to report, and a Failed pod reads as a crash. No
/// `startTime`: the one it had was the previous boot's. Not deleted: a
/// service that did not come back is what someone needs to see.
pub fn not_started(pod: &Value) -> Value {
    let mut pod = pod.clone();
    let asset = pod["metadata"]["labels"]["storm.io/asset"].as_str().unwrap_or("").to_string();
    let restarts = pod["status"]["containerStatuses"][0]["restartCount"].as_u64().unwrap_or(0);
    let message = format!("{asset} is not in PID 1's asset table: it was not started on this boot");
    pod["status"] = json!({
        "phase": "Pending",
        "hostIP": "",
        "reason": NOT_STARTED,
        "message": message,
        "conditions": [
            { "type": "PodScheduled", "status": "True" },
            { "type": "Initialized", "status": "False" },
            { "type": "ContainersReady", "status": "False" },
            { "type": "Ready", "status": "False" },
        ],
        "containerStatuses": [{
            "name": asset,
            "image": format!("stormpump://{asset}"),
            "ready": false,
            "restartCount": restarts,
            "state": { "waiting": { "reason": NOT_STARTED, "message": message } },
        }],
    });
    pod
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assets_parse_and_a_bad_file_is_empty_not_fatal() {
        let text = r#"{"assets":[
            {"name":"stormblock","running":true,"restarts":0,"age_secs":120,"domain":1},
            {"name":"registry","running":false,"restarts":7,"age_secs":3,"domain":1}]}"#;
        let a = parse_assets(text);
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], Asset { name: "stormblock".into(), running: true, restarts: 0, age_secs: 120, ..Default::default() });
        assert_eq!(a[1].restarts, 7);
        assert!(!a[1].running);

        // A node that cannot be read is a node that cannot be seen, which is
        // not a reason to fail anything.
        assert!(parse_assets("").is_empty());
        assert!(parse_assets("{").is_empty());
        assert!(parse_assets(r#"{"assets":"nonsense"}"#).is_empty());
        // An entry with no name is skipped rather than named "".
        assert!(parse_assets(r#"{"assets":[{"running":true}]}"#).is_empty());
    }

    /// `<asset>-<node>`, because two nodes both running a storage engine is
    /// the normal case and not a collision.
    #[test]
    fn the_mirror_name_is_scoped_to_the_node() {
        assert_eq!(mirror_name("stormblock", "storm-2c91b3"), "stormblock-storm-2c91b3");
        assert_ne!(
            mirror_name("stormblock", "node-a"),
            mirror_name("stormblock", "node-b")
        );
    }

    /// A service PID 1 did not list on this boot keeps no stale Running (#87).
    #[test]
    fn a_mirror_whose_asset_is_not_listed_is_marked_not_started_once() {
        let running = Asset { name: "fastetcd".into(), running: true, restarts: 0, age_secs: 5, ..Default::default() };
        let listed = mirror_pod(&running, "n1", "u", "2026-09-28T17:32:00Z");
        let old = mirror_pod(
            &Asset { name: "registry".into(), running: true, restarts: 3, age_secs: 9, ..Default::default() },
            "n1",
            "u",
            "2026-09-28T14:02:00Z",
        );
        let other_node = mirror_pod(
            &Asset { name: "registry".into(), running: true, restarts: 0, age_secs: 9, ..Default::default() },
            "n2",
            "u",
            "2026-09-28T14:02:00Z",
        );
        let mut plain = old.clone();
        plain["metadata"]["labels"] = json!({"app": "x"});
        let list = json!({"items": [listed, old, other_node, plain]});

        let stale = stale_mirrors(&list, "n1", &[running.clone()]);
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0]["metadata"]["name"], "registry-n1");

        let marked = not_started(stale[0]);
        assert_eq!(marked["status"]["phase"], "Pending");
        assert!(marked["status"].get("startTime").is_none(), "the old boot's startTime must go");
        let cs = &marked["status"]["containerStatuses"][0];
        assert_eq!(cs["state"]["waiting"]["reason"], NOT_STARTED);
        assert_eq!(cs["ready"], false);
        assert_eq!(cs["restartCount"], 3);
        // Metadata is kept, resourceVersion included, so the write is guarded.
        assert_eq!(marked["metadata"], stale[0]["metadata"]);

        // Once marked, the next pass has nothing to write.
        let list = json!({"items": [marked]});
        assert!(stale_mirrors(&list, "n1", &[running]).is_empty());
    }

    /// Ages are not a change; a restart or a stop is (#101).
    #[test]
    fn only_state_changes_the_table_key() {
        let a = |running, restarts, age_secs| Asset { name: "stormblock".into(), running, restarts, age_secs, ..Default::default() };
        assert_eq!(table_key(&[a(true, 1, 5)]), table_key(&[a(true, 1, 500)]));
        assert_ne!(table_key(&[a(true, 1, 5)]), table_key(&[a(true, 2, 5)]));
        assert_ne!(table_key(&[a(true, 1, 5)]), table_key(&[a(false, 1, 5)]));
    }

    /// The mirror writes nothing when the pod already says it, or its own
    /// watch would wake it for ever (#101).
    #[test]
    fn a_current_mirror_needs_no_write() {
        let a = Asset { name: "stormblock".into(), running: true, restarts: 2, age_secs: 60, ..Default::default() };
        let p = mirror_pod(&a, "n1", "u", "2026-08-29T00:00:00Z");
        let mut stored = p.clone();
        stored["metadata"]["resourceVersion"] = json!("41");
        assert!(status_current(&stored, &mirror_pod(&a, "n1", "u", "2026-08-29T00:00:02Z")), "age jitter");
        assert!(!status_current(&stored, &mirror_pod(&a, "n1", "u", "2026-08-29T01:00:00Z")), "a new incarnation");
        let restarted = Asset { restarts: 3, ..a.clone() };
        assert!(!status_current(&stored, &mirror_pod(&restarted, "n1", "u", "2026-08-29T00:00:00Z")));
        let stopped = Asset { running: false, ..a.clone() };
        assert!(!status_current(&stored, &mirror_pod(&stopped, "n1", "u", "2026-08-29T00:00:00Z")));
        assert!(!status_current(&not_started(&stored), &p), "marked not started, now listed again");
    }

    #[test]
    fn a_running_asset_mirrors_as_a_ready_pod() {
        let a = Asset { name: "stormblock".into(), running: true, restarts: 2, age_secs: 60, ..Default::default() };
        let p = mirror_pod(&a, "n1", "uid-1", "2026-08-29T00:00:00Z");
        assert_eq!(p["status"]["phase"], "Running");
        assert_eq!(p["status"]["containerStatuses"][0]["restartCount"], 2);
        assert_eq!(p["status"]["containerStatuses"][0]["ready"], true);
        assert!(p["status"]["containerStatuses"][0]["state"]["running"].is_object());
        // The annotation is what says the API did not schedule this.
        assert_eq!(p["metadata"]["annotations"]["kubernetes.io/config.source"], "stormpump");
        // Owned by the Node, so it is collected with it.
        assert_eq!(p["metadata"]["ownerReferences"][0]["kind"], "Node");
        assert_eq!(p["metadata"]["ownerReferences"][0]["uid"], "uid-1");
    }

    /// #96: a running service its health endpoint says is down is Running and
    /// not ready, with why; healthy, or never probed, it is ready as before.
    #[test]
    fn a_running_service_that_does_not_answer_is_not_ready() {
        let a = Asset { name: "stormstorage".into(), running: true, restarts: 0, age_secs: 60, ..Default::default() };
        let down = crate::node_health::ServiceHealth { ready: false, failures: 3, reason: "http://127.0.0.1:9093/api/v1/health: connection refused".into() };
        let p = mirror_pod_with(&a, "n1", "u", "2026-10-08T00:00:00Z", Some(&down));
        assert_eq!(p["status"]["phase"], "Running");
        assert_eq!(p["status"]["containerStatuses"][0]["ready"], false);
        let ready = p["status"]["conditions"].as_array().unwrap().iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!((ready["status"].as_str(), ready["reason"].as_str()), (Some("False"), Some("Unhealthy")));
        assert!(ready["message"].as_str().unwrap().contains("connection refused"));
        // The status changes, so it is written.
        assert!(!status_current(&mirror_pod(&a, "n1", "u", "2026-10-08T00:00:00Z"), &p));
        let up = crate::node_health::ServiceHealth { ready: true, ..Default::default() };
        assert_eq!(mirror_pod_with(&a, "n1", "u", "x", Some(&up))["status"]["containerStatuses"][0]["ready"], true);
        assert_eq!(mirror_pod_with(&a, "n1", "u", "x", None)["status"]["containerStatuses"][0]["ready"], true);
    }

    /// #82: PID 1's last exit and output (stormpump#51) reach the mirror pod:
    /// `lastState.terminated` on a service running again, `state.terminated`
    /// on one that stays down, with the exit code and the output's tail.
    #[test]
    fn a_crashed_service_shows_its_exit_and_last_output() {
        let text = r#"{"assets":[
            {"name":"fastetcd","running":true,"restarts":4,"age_secs":2,"domain":1,
             "last_exit_code":1,"last_exit":"exited 1",
             "last_output":["starting","Error: storage io: DB corrupted: bad page"]},
            {"name":"registry","running":false,"restarts":7,"age_secs":3,"domain":1,
             "last_exit_signal":9,"last_exit":"killed by signal 9","last_output":[]},
            {"name":"stormblock","running":true,"restarts":0,"age_secs":9,"domain":1}]}"#;
        let a = parse_assets(text);
        let etcd = a[0].last_exit.clone().unwrap();
        assert_eq!((etcd.code, etcd.signal, etcd.text.as_str()), (Some(1), None, "exited 1"));
        assert_eq!(a[1].last_exit.as_ref().unwrap().exit_code(), 137, "128 + SIGKILL");
        assert!(a[2].last_exit.is_none(), "never exited: nothing invented");

        // Crash-looping, briefly running again: the reason is on lastState.
        let p = mirror_pod(&a[0], "n1", "u", "2026-10-08T00:00:00Z");
        let cs = &p["status"]["containerStatuses"][0];
        assert!(cs["state"]["running"].is_object());
        let last = &cs["lastState"]["terminated"];
        assert_eq!((last["exitCode"].as_i64(), last["reason"].as_str()), (Some(1), Some("Error")));
        assert_eq!(last["message"], "starting\nError: storage io: DB corrupted: bad page");
        assert!(etcd.summary().starts_with("exited 1; last output:\n"));
        assert!(etcd.summary().ends_with("DB corrupted: bad page"));

        // Down for good: state.terminated says how, with a signal and no output.
        let p = mirror_pod(&a[1], "n1", "u", "2026-10-08T00:00:00Z");
        let term = &p["status"]["containerStatuses"][0]["state"]["terminated"];
        assert_eq!((term["exitCode"].as_i64(), term["signal"].as_i64()), (Some(137), Some(9)));
        assert_eq!(term["message"], "killed by signal 9");

        // A new exit is a change to write; the same one is not.
        let before = mirror_pod(&a[0], "n1", "u", "2026-10-08T00:00:00Z");
        let mut again = a[0].clone();
        again.last_exit.as_mut().unwrap().output.push("Error: again".into());
        assert!(!status_current(&before, &mirror_pod(&again, "n1", "u", "2026-10-08T00:00:00Z")));
        assert!(status_current(&before, &mirror_pod(&a[0], "n1", "u", "2026-10-08T00:00:01Z")));
        assert_ne!(table_key(&a[..1]), table_key(&[again]));
    }

    /// #50: a node service's lifecycle as Events. First sight is judged
    /// against the API's mirror pod: the same run is not re-announced after a
    /// kubelet restart, a new one is `Started` with when; a non-zero exit is
    /// `Failed` with its tail, a clean one `Stopped`; restarts are `BackOff`.
    #[test]
    fn a_services_changes_are_events_and_a_kubelet_restart_announces_nothing() {
        let a = Asset { name: "fastetcd".into(), running: true, restarts: 2, age_secs: 60, ..Default::default() };
        let t0 = "2026-10-08T10:00:00Z";
        let pod = mirror_pod(&a, "n1", "u", t0);

        // A kubelet restart: the API already shows this run (±5 s): nothing.
        assert_eq!(transition(None, &a, Some(&pod), "2026-10-08T10:00:03Z"), None);
        // No mirror yet, or the API shows an earlier run: Started, with when.
        let (t, why, m) = transition(None, &a, None, t0).unwrap();
        assert_eq!((t, why), ("Normal", "Started"));
        assert!(m.contains(t0), "{m}");
        assert!(transition(None, &a, Some(&pod), "2026-10-08T11:00:00Z").is_some(), "a later start");
        let restarted = Asset { restarts: 3, ..a.clone() };
        assert!(transition(None, &restarted, Some(&pod), t0).is_some(), "another restart count");

        // Down, while the API still says running: it ended.
        let dead = Asset {
            running: false,
            last_exit: Some(LastExit { code: Some(1), text: "exited 1".into(), output: vec!["Error: DB corrupted".into()], ..Default::default() }),
            ..a.clone()
        };
        let (t, why, m) = transition(None, &dead, Some(&pod), t0).unwrap();
        assert_eq!((t, why), ("Warning", "Failed"));
        assert!(m.contains("exited 1") && m.contains("DB corrupted"), "{m}");
        // Already shown as down: nothing.
        assert_eq!(transition(None, &dead, Some(&mirror_pod(&dead, "n1", "u", t0)), t0), None);

        // Seen before: the edges.
        assert_eq!(transition(Some((true, 2)), &dead, None, t0).unwrap().1, "Failed");
        let clean = Asset { running: false, last_exit: Some(LastExit { code: Some(0), text: "exited 0".into(), ..Default::default() }), ..a.clone() };
        assert_eq!(transition(Some((true, 2)), &clean, None, t0).unwrap().1, "Stopped");
        assert_eq!(transition(Some((true, 2)), &restarted, None, t0).unwrap().1, "BackOff");
        assert_eq!(transition(Some((false, 2)), &a, None, t0).unwrap().1, "Started");
        assert_eq!(transition(Some((true, 2)), &a, None, t0), None, "no change, no event");
    }

    /// Upstream's cap on `message`: the newest lines, at most 80 and 4 KiB.
    #[test]
    fn the_output_tail_keeps_the_newest_lines_within_the_cap() {
        let many = LastExit { output: (0..100).map(|i| format!("line {i}")).collect(), ..Default::default() };
        let tail = many.tail();
        assert_eq!(tail.lines().count(), 80);
        assert!(tail.ends_with("line 99") && tail.starts_with("line 20"));
        let wide = LastExit { output: (0..10).map(|i| format!("{i}{}", "x".repeat(1000))).collect(), ..Default::default() };
        let tail = wide.tail();
        assert!(tail.len() <= 4096, "{}", tail.len());
        assert!(tail.lines().last().unwrap().starts_with('9'), "the newest line is kept");
    }

    /// A stopped asset is not Ready, and does not claim an exit code PID 1
    /// never reported — a fabricated 0 would read as a clean exit.
    #[test]
    fn a_stopped_asset_is_not_ready_and_invents_no_exit_code() {
        let a = Asset { name: "registry".into(), running: false, restarts: 9, age_secs: 1, ..Default::default() };
        let p = mirror_pod(&a, "n1", "uid-1", "2026-08-29T00:00:00Z");
        assert_eq!(p["status"]["phase"], "Failed");
        assert_eq!(p["status"]["containerStatuses"][0]["ready"], false);
        let term = &p["status"]["containerStatuses"][0]["state"]["terminated"];
        assert!(term.is_object());
        assert!(term.get("exitCode").is_none(), "must not invent an exit code");
        for c in p["status"]["conditions"].as_array().unwrap() {
            if c["type"] == "Ready" {
                assert_eq!(c["status"], "False");
            }
        }
    }

    /// #130: a mirror pod names its golden, digest and host; an existing one
    /// without them is not current.
    #[test]
    fn a_mirror_carries_its_goldens_provenance() {
        let a = Asset { name: "stormlb".into(), running: true, ..Default::default() };
        let plain = mirror_pod(&a, "n1", "u", "2026-10-08T00:00:00Z");
        let mut build = serde_json::Map::new();
        build.insert("provenance".into(), json!("stormlb@b8ba1a7"));
        let g = crate::image_config::Provenance {
            digest: Some(format!("sha256:{}", "c".repeat(64))),
            golden: Some("stormlb".into()),
            build,
        };
        let mut p = plain.clone();
        with_provenance(&mut p, Some(&g), "192.168.8.10");
        assert_eq!(p["status"]["hostIP"], "192.168.8.10");
        assert_eq!(p["status"]["containerStatuses"][0]["imageID"], json!(g.digest));
        assert_eq!(p["metadata"]["annotations"][GOLDEN], "stormlb");
        assert_eq!(p["metadata"]["annotations"][GOLDEN_PROVENANCE], "stormlb@b8ba1a7");
        assert!(!status_current(&plain, &p), "imageID and hostIP are news");
        assert!(status_current(&p, &p));
        // No manifest entry: the host only.
        let mut q = plain.clone();
        with_provenance(&mut q, None, "192.168.8.10");
        assert!(q["status"]["containerStatuses"][0].get("imageID").is_none());
    }

    /// #193: a service's age from started_secs and the node's uptime (which
    /// does not go stale between file writes), else age_secs.
    #[test]
    fn age_is_uptime_less_started_secs_else_age_secs() {
        let text = r#"{"assets":[
            {"name":"stormblock","running":true,"restarts":0,"age_secs":812,"node_uptime_secs":840,"started_secs":28},
            {"name":"old","running":true,"restarts":0,"age_secs":7}]}"#;
        let a = parse_assets(text);
        assert_eq!(a[0].started_secs, Some(28.0));
        // Ten minutes after the file was written, the age has moved with it.
        assert_eq!(a[0].age(Some(1440.5)), 1412);
        assert_eq!(a[0].age(None), 812, "no uptime: the file's age");
        assert_eq!(a[1].age(Some(1440.5)), 7, "an older PID 1: age_secs");
        assert_eq!(Asset { started_secs: Some(50.0), ..Default::default() }.age(Some(40.0)), 0, "never negative");
        assert!(node_uptime().is_some_and(|u| u > 0.0));
    }
}
