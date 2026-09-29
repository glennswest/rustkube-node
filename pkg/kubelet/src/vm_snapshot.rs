//! `VirtualMachineSnapshot` (`snapshot.kubevirt.io/v1beta1`) for the VMs this
//! node runs (#53).
//!
//! stormvm reads the object (`stormvm_spec::snapshot`) and takes the snapshot
//! (`stormvm_console::snapshot::take`: freeze → pause → one stormblock group
//! snapshot of every volume → unpause → thaw). This module decides which
//! objects are this node's, and writes what happened onto them.
//!
//! **Whose it is.** The VM a snapshot names is this node's when its
//! registration is here (`/run/stormvm/<ns>/<vm>`): that is the node whose
//! stormblock holds the VM's volumes, and so the only one that can take them.
//! The node that starts a snapshot marks it (`storm.io/snapshot-node`, written
//! against the object's resourceVersion), so exactly one node takes it and a
//! restarted kubelet knows which in-progress snapshots were its own.
//!
//! **A restart mid-take** finds its snapshot `InProgress` and not in flight,
//! and takes it again. That is safe: stormblock is idempotent by name, and the
//! group is named for the VM and the snapshot (`<ns>.<vm>.<snapshot>`), so the
//! second take answers with what the first made — at the cost of the guest
//! held still once more.
//!
//! `VirtualMachineRestore` is not served yet (#53: how a restored volume
//! becomes the VM's disk is an open decision).

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use stormvm_node::console::Registration;
use stormvm_spec::snapshot::{self as spec, Phase, SnapshotResult};
use tracing::{debug, info, warn};

/// Which node took (or is taking) a snapshot.
pub const NODE_ANNOTATION: &str = "storm.io/snapshot-node";

/// KubeVirt's default `failureDeadline`.
const DEFAULT_DEADLINE_SECS: u64 = 300;

const API: &str = "snapshot.kubevirt.io/v1beta1";

/// What a take came to, as the status needs it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Taken {
    /// stormblock's group-snapshot id, when the disks were taken.
    pub group: Option<String>,
    pub indications: Vec<String>,
    /// Every problem, including a guest left paused or frozen. `None` is a
    /// success.
    pub error: Option<String>,
}

impl Taken {
    fn failed(why: impl Into<String>) -> Taken {
        Taken { error: Some(why.into()), ..Default::default() }
    }
}

/// Takes a registered VM's snapshot under a name. A seam for the tests: the
/// real one needs a hypervisor and a stormblock.
pub type TakeFn =
    Arc<dyn Fn(Registration, String) -> Pin<Box<dyn Future<Output = Taken> + Send>> + Send + Sync>;

/// The real take, against this node's stormblock.
pub fn stormvm_take(stormblock: String) -> TakeFn {
    Arc::new(move |reg: Registration, name: String| {
        let stormblock = stormblock.clone();
        Box::pin(async move {
            let opts = stormvm_control::snapshot::Options::default();
            let out = stormvm_console::snapshot::take(&reg, &stormblock, &name, opts).await;
            Taken {
                group: out.disks.as_ref().ok().map(|g| g.id.clone()),
                indications: out.indications.iter().map(|s| s.to_string()).collect(),
                error: out.error(),
            }
        })
    })
}

/// What to do with one snapshot object this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    /// Not this node's, finished, or already being taken.
    Skip,
    /// Unclaimed and its VM is here: claim it, then take it.
    Claim,
    /// Claimed by this node, `InProgress`, and not in flight: take it again.
    Resume,
    /// This node's, and it cannot be taken: `Failed`, with why.
    Fail(String),
}

/// The VM a snapshot names, read loosely: a malformed object whose VM is here
/// is still this node's to fail, with every problem stormvm finds.
fn source_vm(obj: &Value) -> &str {
    obj["spec"]["source"]["name"].as_str().unwrap_or("")
}

fn decide(
    obj: &Value,
    node: &str,
    registered: bool,
    in_flight: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> Action {
    match obj["status"]["phase"].as_str() {
        Some("Succeeded") | Some("Failed") => return Action::Skip,
        _ => {}
    }
    if in_flight {
        return Action::Skip;
    }
    let owner = obj["metadata"]["annotations"][NODE_ANNOTATION].as_str();
    let mine = match owner {
        Some(n) => n == node,
        None => registered,
    };
    if !mine {
        return Action::Skip;
    }
    let req = match spec::snapshot_request(obj) {
        Ok(r) => r,
        Err(rejected) => return Action::Fail(rejected.to_string()),
    };
    let deadline = req.failure_deadline_secs.unwrap_or(DEFAULT_DEADLINE_SECS);
    let created = obj["metadata"]["creationTimestamp"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
    if let Some(created) = created {
        if (now - created.with_timezone(&chrono::Utc)).num_seconds() > deadline as i64 {
            return Action::Fail(format!("not taken within its failureDeadline ({deadline}s)"));
        }
    }
    if !registered {
        return Action::Fail(format!("{} is no longer running on {node}", req.vm));
    }
    if owner.is_some() {
        Action::Resume
    } else {
        Action::Claim
    }
}

pub struct Snapshots {
    api: reqwest::Client,
    api_url: String,
    node: String,
    run_dir: String,
    take: TakeFn,
    events: Option<crate::events::EventRecorder>,
    /// `<ns>/<name>` of the snapshots being taken now.
    in_flight: Mutex<HashSet<String>>,
    /// Said once: a cluster without the CRDs is ordinary (stormcos#170).
    absent_said: std::sync::atomic::AtomicBool,
    /// Told when a take finishes (#101): its status write is an API event
    /// too, but one that failed is not, and the next pass must still run.
    completed: Arc<tokio::sync::Notify>,
}

impl Snapshots {
    pub fn new(
        api: reqwest::Client,
        api_url: &str,
        node: &str,
        run_dir: &str,
        take: TakeFn,
    ) -> Snapshots {
        let api_url = api_url.trim_end_matches('/').to_string();
        let events = (!api_url.is_empty())
            .then(|| crate::events::EventRecorder::new(api.clone(), &api_url, node));
        Snapshots {
            api,
            api_url,
            node: node.to_string(),
            run_dir: run_dir.to_string(),
            take,
            events,
            in_flight: Mutex::new(HashSet::new()),
            absent_said: std::sync::atomic::AtomicBool::new(false),
            completed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Notified each time a take finishes (#101).
    pub fn completions(&self) -> Arc<tokio::sync::Notify> {
        self.completed.clone()
    }

    /// One pass over every snapshot in the cluster.
    ///
    /// A take runs in the background: the guest is held only for one
    /// stormblock call, but freezing waits on the guest agent, and the tick
    /// that syncs every pod on the node must not wait with it.
    pub async fn sync(self: &Arc<Self>) {
        let Some(items) = self.list().await else { return };
        let now = chrono::Utc::now();
        for obj in items {
            let ns = obj["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
            let name = obj["metadata"]["name"].as_str().unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            let vm = source_vm(&obj);
            let reg = (!vm.is_empty())
                .then(|| stormvm_node::console::find(&self.run_dir, &ns, vm))
                .flatten();
            let key = format!("{ns}/{name}");
            let in_flight = self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).contains(&key);
            match decide(&obj, &self.node, reg.is_some(), in_flight, now) {
                Action::Skip => {}
                Action::Fail(why) => {
                    self.finish(&obj, String::new(), Taken::failed(why)).await;
                }
                Action::Claim => {
                    if self.claim(&obj).await {
                        self.start(obj, reg.expect("registered"), key);
                    }
                }
                Action::Resume => self.start(obj, reg.expect("registered"), key),
            }
        }
    }

    /// Every snapshot, or `None` when there is no answer (or no CRD).
    async fn list(&self) -> Option<Vec<Value>> {
        if self.api_url.is_empty() {
            return None;
        }
        let url = format!("{}/apis/{API}/virtualmachinesnapshots", self.api_url);
        let r = match self.api.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                debug!("virtualmachinesnapshots not listed: {e}");
                apimachinery::reactor::failed();
                return None;
            }
        };
        if r.status() == reqwest::StatusCode::NOT_FOUND {
            if !self.absent_said.swap(true, std::sync::atomic::Ordering::Relaxed) {
                info!("no {API} virtualmachinesnapshots on this cluster: snapshots are not served");
            }
            return None;
        }
        if !r.status().is_success() {
            debug!("virtualmachinesnapshots not listed: {}", r.status());
            apimachinery::reactor::failed();
            return None;
        }
        let list: Value = r.json().await.ok()?;
        Some(list["items"].as_array().cloned().unwrap_or_default())
    }

    /// Mark the snapshot as this node's, against the version read. A node
    /// that loses the race gets a conflict and leaves it.
    async fn claim(&self, obj: &Value) -> bool {
        let (ns, name) = ns_name(obj);
        let mut meta = json!({ "annotations": { NODE_ANNOTATION: self.node } });
        if let Some(rv) = obj["metadata"]["resourceVersion"].as_str() {
            meta["resourceVersion"] = json!(rv);
        }
        let url = format!("{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}", self.api_url);
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "metadata": meta }))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                debug!("{ns}/{name}: snapshot not claimed: {}", r.status());
                false
            }
            Err(e) => {
                debug!("{ns}/{name}: snapshot not claimed: {e}");
                // Not an answer: no event will follow, so the pass retries.
                apimachinery::reactor::failed();
                false
            }
        }
    }

    fn start(self: &Arc<Self>, obj: Value, reg: Registration, key: String) {
        self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone());
        let me = self.clone();
        tokio::spawn(async move {
            let source_uid = me.source_uid(&obj, &reg).await;
            me.write_status(
                &obj,
                &SnapshotResult {
                    phase: Some(Phase::InProgress),
                    time: now_rfc3339(),
                    source_uid: source_uid.clone(),
                    ..Default::default()
                },
            )
            .await;
            let (ns, name) = ns_name(&obj);
            info!("{ns}/{name}: taking a snapshot of {}", reg.name);
            let taken = (me.take)(reg, name.to_string()).await;
            me.finish(&obj, source_uid, taken).await;
            me.in_flight.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
            me.completed.notify_one();
        });
    }

    /// The uid of what was snapshotted: the VirtualMachine's when the source
    /// is one, else the VMI's (its registration has it).
    async fn source_uid(&self, obj: &Value, reg: &Registration) -> String {
        if obj["spec"]["source"]["kind"].as_str() != Some("VirtualMachine") {
            return reg.uid.clone();
        }
        let url = format!(
            "{}/apis/kubevirt.io/v1/namespaces/{}/virtualmachines/{}",
            self.api_url, reg.namespace, reg.name
        );
        let vm: Option<Value> = match self.api.get(&url).send().await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            _ => None,
        };
        vm.and_then(|v| v["metadata"]["uid"].as_str().map(String::from)).unwrap_or_default()
    }

    async fn finish(&self, obj: &Value, source_uid: String, taken: Taken) {
        let (ns, name) = ns_name(obj);
        let ok = taken.error.is_none();
        let result = SnapshotResult {
            phase: Some(if ok { Phase::Succeeded } else { Phase::Failed }),
            time: now_rfc3339(),
            source_uid,
            group_snapshot: taken.group.clone(),
            indications: taken.indications,
            error: taken.error.clone(),
        };
        self.write_status(obj, &result).await;
        match &taken.error {
            None => {
                info!("{ns}/{name}: snapshot taken ({})", taken.group.as_deref().unwrap_or(""));
                self.event(obj, "Normal", "SnapshotSucceeded",
                           &format!("group snapshot {}", taken.group.as_deref().unwrap_or("")))
                    .await;
            }
            Some(e) => {
                warn!("{ns}/{name}: snapshot failed: {e}");
                self.event(obj, "Warning", "SnapshotFailed", e).await;
            }
        }
    }

    async fn write_status(&self, obj: &Value, r: &SnapshotResult) {
        let (ns, name) = ns_name(obj);
        let url = format!(
            "{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}/status",
            self.api_url
        );
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "status": spec::snapshot_status(r) }))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) => warn!("{ns}/{name}: snapshot status not written: {}", resp.status()),
            Err(e) => warn!("{ns}/{name}: snapshot status not written: {e}"),
        }
    }

    async fn event(&self, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event(API, "VirtualMachineSnapshot", obj, etype, reason, message).await;
        }
    }
}

fn ns_name(obj: &Value) -> (&str, &str) {
    (
        obj["metadata"]["namespace"].as_str().unwrap_or("default"),
        obj["metadata"]["name"].as_str().unwrap_or(""),
    )
}

fn now_rfc3339() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Path;
    use axum::routing::{get, patch};

    fn snap(name: &str, vm: &str) -> Value {
        json!({
            "apiVersion": API,
            "kind": "VirtualMachineSnapshot",
            "metadata": {
                "name": name, "namespace": "web", "uid": format!("u-{name}"),
                "resourceVersion": "7",
                "creationTimestamp": now_rfc3339(),
            },
            "spec": { "source": { "apiGroup": "kubevirt.io", "kind": "VirtualMachine", "name": vm } },
        })
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    #[test]
    fn an_unclaimed_snapshot_of_a_vm_here_is_claimed() {
        assert_eq!(decide(&snap("s", "web-1"), "n1", true, false, now()), Action::Claim);
    }

    #[test]
    fn a_vm_elsewhere_is_not_this_nodes_business() {
        assert_eq!(decide(&snap("s", "web-1"), "n1", false, false, now()), Action::Skip);
        let mut o = snap("s", "web-1");
        o["metadata"]["annotations"] = json!({ NODE_ANNOTATION: "n2" });
        o["status"] = json!({ "phase": "InProgress" });
        assert_eq!(decide(&o, "n1", true, false, now()), Action::Skip);
    }

    #[test]
    fn a_finished_or_running_take_is_left_alone() {
        for phase in ["Succeeded", "Failed"] {
            let mut o = snap("s", "web-1");
            o["status"] = json!({ "phase": phase });
            assert_eq!(decide(&o, "n1", true, false, now()), Action::Skip, "{phase}");
        }
        assert_eq!(decide(&snap("s", "web-1"), "n1", true, true, now()), Action::Skip);
    }

    #[test]
    fn a_claimed_take_that_a_restart_interrupted_is_taken_again() {
        let mut o = snap("s", "web-1");
        o["metadata"]["annotations"] = json!({ NODE_ANNOTATION: "n1" });
        o["status"] = json!({ "phase": "InProgress" });
        assert_eq!(decide(&o, "n1", true, false, now()), Action::Resume);
        // Its VM has gone from here meanwhile: nobody else can take it.
        match decide(&o, "n1", false, false, now()) {
            Action::Fail(why) => assert!(why.contains("no longer running on n1"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn past_its_failure_deadline_it_fails() {
        let mut o = snap("s", "web-1");
        o["spec"]["failureDeadline"] = json!("1m");
        let later = now() + chrono::Duration::seconds(61);
        match decide(&o, "n1", true, false, later) {
            Action::Fail(why) => assert!(why.contains("failureDeadline (60s)"), "{why}"),
            other => panic!("{other:?}"),
        }
        // The default is five minutes.
        let o = snap("s", "web-1");
        assert_eq!(decide(&o, "n1", true, false, now() + chrono::Duration::seconds(299)), Action::Claim);
        assert!(matches!(decide(&o, "n1", true, false, now() + chrono::Duration::seconds(301)), Action::Fail(_)));
    }

    #[test]
    fn a_malformed_snapshot_of_a_vm_here_fails_with_every_problem() {
        let mut o = snap("s", "web-1");
        o["spec"]["failureDeadline"] = json!("soon");
        match decide(&o, "n1", true, false, now()) {
            Action::Fail(why) => assert!(why.contains("failureDeadline"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    type Log = Arc<Mutex<Vec<(String, Value)>>>;

    /// An apiserver holding snapshots, recording every write, with one
    /// VirtualMachine (`web/web-1`, uid `vm-uid`).
    async fn apiserver(items: Vec<Value>, claim_status: u16) -> (String, Log) {
        let log: Log = Arc::default();
        let (l1, l2, l3) = (log.clone(), log.clone(), log.clone());
        let app = axum::Router::new()
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/virtualmachinesnapshots",
                get(move || {
                    let items = items.clone();
                    async move { axum::Json(json!({ "items": items })) }
                }),
            )
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/namespaces/{ns}/virtualmachinesnapshots/{name}",
                patch(move |Path((_, name)): Path<(String, String)>, axum::Json(b): axum::Json<Value>| {
                    let l = l1.clone();
                    async move {
                        l.lock().unwrap().push((format!("claim {name}"), b));
                        (axum::http::StatusCode::from_u16(claim_status).unwrap(), axum::Json(json!({})))
                    }
                }),
            )
            .route(
                "/apis/snapshot.kubevirt.io/v1beta1/namespaces/{ns}/virtualmachinesnapshots/{name}/status",
                patch(move |Path((_, name)): Path<(String, String)>, axum::Json(b): axum::Json<Value>| {
                    let l = l2.clone();
                    async move {
                        l.lock().unwrap().push((format!("status {name}"), b));
                        axum::Json(json!({}))
                    }
                }),
            )
            .route(
                "/apis/kubevirt.io/v1/namespaces/web/virtualmachines/web-1",
                get(|| async { axum::Json(json!({ "metadata": { "uid": "vm-uid" } })) }),
            )
            .route(
                "/api/v1/namespaces/web/events",
                axum::routing::post(move |axum::Json(b): axum::Json<Value>| {
                    let l = l3.clone();
                    async move {
                        l.lock().unwrap().push(("event".into(), b));
                        axum::Json(json!({}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, log)
    }

    fn register(run_dir: &str, ns: &str, name: &str) {
        let reg: Registration = serde_json::from_value(json!({
            "namespace": ns, "name": name, "uid": "vmi-uid",
        }))
        .unwrap();
        stormvm_node::console::write(run_dir, &reg).unwrap();
    }

    /// A take that records what it was asked and answers `taken`.
    fn fake_take(taken: Taken, asked: Arc<Mutex<Vec<(String, String)>>>) -> TakeFn {
        Arc::new(move |reg: Registration, name: String| {
            asked.lock().unwrap().push((format!("{}/{}", reg.namespace, reg.name), name));
            let t = taken.clone();
            Box::pin(async move { t })
        })
    }

    async fn settled(log: &Log, writes: usize) -> Vec<(String, Value)> {
        for _ in 0..200 {
            if log.lock().unwrap().len() >= writes {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        log.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_snapshot_of_a_vm_here_is_claimed_taken_and_written_succeeded() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) =
            apiserver(vec![snap("before", "web-1"), snap("other", "db-1")], 200).await;
        let asked = Arc::default();
        let taken = Taken {
            group: Some("gs-1".into()),
            indications: vec!["Online".into(), "GuestAgent".into()],
            error: None,
        };
        let s = Arc::new(Snapshots::new(
            reqwest::Client::new(), &url, "n1", run_dir, fake_take(taken, Arc::clone(&asked)),
        ));
        s.sync().await;
        // claim, InProgress, Succeeded, event.
        let log = settled(&log, 4).await;

        assert_eq!(*asked.lock().unwrap(), [("web/web-1".to_string(), "before".to_string())]);
        assert_eq!(log[0].0, "claim before");
        assert_eq!(log[0].1["metadata"]["annotations"][NODE_ANNOTATION], "n1");
        assert_eq!(log[0].1["metadata"]["resourceVersion"], "7");
        assert_eq!(log[1].0, "status before");
        assert_eq!(log[1].1["status"]["phase"], "InProgress");
        let done = &log.iter().find(|(k, b)| k == "status before" && b["status"]["phase"] != "InProgress").unwrap().1;
        assert_eq!(done["status"]["phase"], "Succeeded");
        assert_eq!(done["status"]["readyToUse"], true);
        assert_eq!(done["status"]["virtualMachineSnapshotContentName"], "gs-1");
        assert_eq!(done["status"]["sourceUID"], "vm-uid");
        assert_eq!(done["status"]["indications"], json!(["Online", "GuestAgent"]));
        let ev = &log.iter().find(|(k, _)| k == "event").unwrap().1;
        assert_eq!(ev["involvedObject"]["kind"], "VirtualMachineSnapshot");
        assert_eq!(ev["reason"], "SnapshotSucceeded");
        // `db-1` is not here: nothing was written about `other`.
        assert!(log.iter().all(|(k, _)| !k.ends_with("other")), "{log:?}");
    }

    #[tokio::test]
    async fn a_failed_take_is_written_failed_with_why() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) = apiserver(vec![snap("before", "web-1")], 200).await;
        let taken = Taken {
            group: None,
            indications: vec!["Online".into()],
            error: Some("404 volume vol-1".into()),
        };
        let s = Arc::new(Snapshots::new(reqwest::Client::new(), &url, "n1", run_dir, fake_take(taken, Arc::default())));
        s.sync().await;
        let log = settled(&log, 4).await;
        let done = &log.iter().find(|(k, b)| k == "status before" && b["status"]["phase"] == "Failed").unwrap().1;
        assert_eq!(done["status"]["readyToUse"], false);
        assert!(done["status"]["error"]["message"].as_str().unwrap().contains("404 volume vol-1"));
        let ev = &log.iter().find(|(k, _)| k == "event").unwrap().1;
        assert_eq!(ev["type"], "Warning");
    }

    /// Another node claimed it first: the conflict is a loss, and nothing is
    /// taken here.
    #[tokio::test]
    async fn a_lost_claim_takes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run_dir = dir.path().to_str().unwrap();
        register(run_dir, "web", "web-1");
        let (url, log) = apiserver(vec![snap("before", "web-1")], 409).await;
        let asked: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
        let s = Arc::new(Snapshots::new(
            reqwest::Client::new(), &url, "n1", run_dir, fake_take(Taken::default(), Arc::clone(&asked)),
        ));
        s.sync().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(asked.lock().unwrap().is_empty());
        assert_eq!(log.lock().unwrap().len(), 1, "only the claim");
    }

    /// No CRD: a 404 list is nothing to do, not an error.
    #[tokio::test]
    async fn no_crd_is_nothing_to_do() {
        let app = axum::Router::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let s = Snapshots::new(reqwest::Client::new(), &url, "n1", "/nonexistent", fake_take(Taken::default(), Arc::default()));
        assert!(s.list().await.is_none());
    }
}
