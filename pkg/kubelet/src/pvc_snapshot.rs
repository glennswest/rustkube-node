//! `VolumeSnapshot`s of the built-in class's claims (#379): the
//! csi-snapshotter's part for driver `stormblock.storm.io`, played by the node
//! whose engine holds the volume.
//!
//! The upstream snapshot-controller (stormcos `93-snapshot-controller.yaml`)
//! turns a `VolumeSnapshot` into a `VolumeSnapshotContent` naming the claim's
//! volume (`spec.source.volumeHandle`, the PV's handle) and waits for the
//! driver's sidecar to cut it. The in-kubelet driver has no sidecar, so this
//! does what the sidecar would:
//!
//! - **Cut**: a content of this driver whose volume is in this node's engine
//!   becomes a one-volume stormblock group snapshot named after the content
//!   (asked again after a restart, it is found by name, not taken twice).
//!   `status`: `snapshotHandle` (the group's id), `creationTime`,
//!   `restoreSize`, `readyToUse`. The snapshot-controller copies that onto
//!   the `VolumeSnapshot`. A refusal is `status.error`, which it copies too.
//!   While it is cut the content carries upstream's `being-created`
//!   annotation, so the controller does not delete it from under the cut.
//! - **Delete**: a content being deleted (the controller marks it
//!   `volumesnapshot-being-deleted`, or a pre-provisioned one no snapshot
//!   names) with `deletionPolicy: Delete` has its group deleted, then the
//!   controller's finalizer taken off. `Retain` keeps the group.
//! - **Restore**: a claim with `dataSource` of kind `VolumeSnapshot` is a new
//!   volume from the snapshot ([`restore`]), on the node that holds it; a claim
//!   asking for more than the snapshot is grown to it by `pvc_resize`.
//!
//! Before the first snapshot cut, the flush: `sync(2)` writes back every
//! filesystem's dirty pages, as the clone of a claim does, so the snapshot
//! holds what was written before it was asked for. An application that needs
//! more (a quiesce) freezes itself; the platform's freeze is VMs' (#53).

use retry::RetryExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, info, warn};

pub const DRIVER: &str = "stormblock.storm.io";
const API: &str = "/apis/snapshot.storage.k8s.io/v1";
/// The snapshot-controller's finalizer on a content, which the sidecar takes
/// off once the snapshot is gone.
pub const CONTENT_FINALIZER: &str = "snapshot.storage.kubernetes.io/volumesnapshotcontent-bound-protection";
pub const BEING_DELETED: &str = "snapshot.storage.kubernetes.io/volumesnapshot-being-deleted";
pub const BEING_CREATED: &str = "snapshot.storage.kubernetes.io/volumesnapshot-being-created";
/// Which node cut a content: where its restore runs.
pub const NODE_ANNOTATION: &str = "storm.io/snapshot-node";

/// What one content needs this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Nothing,
    /// Cut the volume `id` (named `volume`).
    Cut { id: String },
    /// Ask the group again: cut, not yet ready.
    Refresh { group: String },
    /// Delete the group (when there is one and the policy says so), then
    /// take the finalizer off.
    Delete { group: Option<String>, keep: bool },
}

/// The group a content names: its status's handle, or a pre-provisioned
/// source's.
pub fn handle(content: &Value) -> Option<String> {
    content["status"]["snapshotHandle"]
        .as_str()
        .or_else(|| content["spec"]["source"]["snapshotHandle"].as_str())
        .filter(|h| !h.is_empty())
        .map(str::to_string)
}

/// Decide one content. `volumes` maps this node's engine volume names to
/// ids; `groups_here` says whether a group id is in this node's engine.
pub fn decide(content: &Value, volumes: &HashMap<String, String>, group_here: impl Fn(&str) -> bool) -> Step {
    if content["spec"]["driver"].as_str() != Some(DRIVER) {
        return Step::Nothing;
    }
    let source_volume = content["spec"]["source"]["volumeHandle"].as_str();
    let group = handle(content);
    let ours = match (source_volume, group.as_deref()) {
        (Some(v), _) if volumes.contains_key(v) => true,
        (_, Some(g)) => group_here(g),
        _ => false,
    };
    if !ours {
        return Step::Nothing;
    }
    let m = &content["metadata"];
    if !m["deletionTimestamp"].is_null() {
        let finalized = m["finalizers"].as_array().is_some_and(|f| f.iter().any(|x| x == CONTENT_FINALIZER));
        let marked = !m["annotations"][BEING_DELETED].is_null();
        let unbound = content["spec"]["volumeSnapshotRef"]["uid"].as_str().unwrap_or("").is_empty();
        if !finalized || !(marked || unbound) {
            return Step::Nothing;
        }
        let keep = content["spec"]["deletionPolicy"].as_str() == Some("Retain");
        return Step::Delete { group, keep };
    }
    match (group, source_volume) {
        (None, Some(v)) => {
            if content["status"]["error"].is_object() && !m["annotations"][BEING_CREATED].is_string() {
                // Refused: said once, on the content, which the controller
                // copies to the snapshot. Deleting it is the way out.
                return Step::Nothing;
            }
            Step::Cut { id: volumes[v].clone() }
        }
        (Some(g), _) if content["status"]["readyToUse"].as_bool() != Some(true) => Step::Refresh { group: g },
        _ => Step::Nothing,
    }
}

/// The content's status for a group: upstream's fields and units
/// (`creationTime` in nanoseconds, `restoreSize` in bytes).
pub fn status_of(group: &stormvm_block::GroupSnapshot) -> Value {
    let size: u64 = group.snapshots.iter().map(|s| s.size_bytes).sum();
    let ready = group.ready && group.snapshots.iter().all(|s| s.ready);
    json!({ "status": {
        "snapshotHandle": group.id,
        "creationTime": group.created_at_ms.saturating_mul(1_000_000),
        "restoreSize": size,
        "readyToUse": ready,
        "error": null,
    }})
}

/// The engine's group snapshots, through stormvm's client (the same calls a
/// VM snapshot makes), off the async threads.
#[derive(Clone)]
pub struct Groups {
    url: String,
}

impl Groups {
    pub fn new(engine: &crate::engine::EngineClient) -> Self {
        Self { url: engine.url().to_string() }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        what: String,
        f: impl Fn(&stormvm_block::Client) -> Result<T, String> + Send + Sync + 'static,
    ) -> Result<T, String> {
        let c = stormvm_block::Client::new(self.url.clone());
        tokio::task::spawn_blocking(move || {
            retry::blocking(retry::Policy::ENGINE, &what, |e: &String| retry::class_of_message(e), || f(&c))
        })
        .await
        .map_err(|e| format!("{e}"))?
    }

    /// The group named `name`, or a new one of volume `id`.
    pub async fn cut(&self, name: &str, id: &str) -> Result<stormvm_block::GroupSnapshot, String> {
        let n = name.to_string();
        let found = self
            .blocking(format!("group snapshots {name}"), move |c| c.group_snapshots(Some(&n)).map_err(|e| e.to_string()))
            .await?;
        if let Some(g) = found.into_iter().find(|g| g.name == name) {
            return Ok(g);
        }
        let _ = tokio::task::spawn_blocking(|| {
            // SAFETY: sync(2) takes no arguments and cannot fail.
            unsafe { libc::sync() }
        })
        .await;
        let (n, ids) = (name.to_string(), vec![id.to_string()]);
        self.blocking(format!("snapshot {id} as {name}"), move |c| c.group_snapshot(&n, &ids).map_err(|e| e.to_string()))
            .await
    }

    pub async fn get(&self, id: &str) -> Result<Option<stormvm_block::GroupSnapshot>, String> {
        let id = id.to_string();
        self.blocking(format!("group snapshot {id}"), move |c| c.group_snapshot_by_id(&id).map_err(|e| e.to_string())).await
    }

    pub async fn delete(&self, id: &str) -> Result<(), String> {
        let id = id.to_string();
        let r = self.blocking(format!("delete group snapshot {id}"), move |c| c.delete_group_snapshot(&id).map_err(|e| e.to_string())).await;
        match r {
            Err(e) if e.contains("404") || e.to_lowercase().contains("not found") => Ok(()),
            other => other,
        }
    }

    /// A new volume `name` holding the group's one snapshot: its `/v1` id.
    pub async fn restore(&self, name: &str, group: &stormvm_block::GroupSnapshot) -> Result<String, String> {
        let snap = group.snapshots.first().cloned().ok_or_else(|| format!("group {} holds no snapshot", group.id))?;
        let n = name.to_string();
        self.blocking(format!("volume {name} from snapshot"), move |c| {
            c.volume_from_snapshot(&n, &snap).map(|v| v.id).map_err(|e| e.to_string())
        })
        .await
    }
}

/// One pass over the cluster's contents. `true` while one is left to try
/// again (not ready yet, an engine or apiserver that did not answer).
pub async fn pass(
    client: &reqwest::Client,
    api_url: &str,
    engine: &crate::engine::EngineClient,
    node: &str,
    events: Option<&crate::events::EventRecorder>,
) -> bool {
    if api_url.is_empty() {
        return false;
    }
    let contents: Value = match client.get(format!("{api_url}{API}/volumesnapshotcontents")).send_retrying(retry::Policy::API).await {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        // No CRD (an install without the snapshot manifests): nothing to do.
        Ok(r) if r.status().as_u16() == 404 => return false,
        _ => return true,
    };
    let mine: Vec<&Value> = contents["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["spec"]["driver"].as_str() == Some(DRIVER))
        .collect();
    if mine.is_empty() {
        return false;
    }
    let list: Value = match engine.get(&format!("{}/api/v1/volumes", engine.url())).await {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        _ => return true,
    };
    let volumes: HashMap<String, String> = list["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| Some((v["name"].as_str()?.to_string(), v["id"].as_str()?.to_string())))
        .collect();
    let groups = Groups::new(engine);
    // Which groups are here: a content cut on this node says so; a
    // pre-provisioned one is asked of the engine.
    let mut here: HashMap<String, bool> = HashMap::new();
    for c in &mine {
        if let Some(g) = handle(c) {
            let said = c["metadata"]["annotations"][NODE_ANNOTATION].as_str();
            let is_here = match said {
                Some(n) => n == node,
                None => matches!(groups.get(&g).await, Ok(Some(_))),
            };
            here.insert(g, is_here);
        }
    }
    let w = Writer { client, api_url, events };
    let mut pending = false;
    for c in mine {
        let step = decide(c, &volumes, |g| here.get(g).copied().unwrap_or(false));
        pending |= match step {
            Step::Nothing => false,
            Step::Cut { id } => w.cut(c, &groups, &id, node).await,
            Step::Refresh { group } => match groups.get(&group).await {
                Ok(Some(g)) => {
                    let ready = status_of(&g)["status"]["readyToUse"].as_bool() == Some(true);
                    !w.status(c, &status_of(&g)).await || !ready
                }
                Ok(None) => {
                    w.status(c, &error(&format!("group snapshot {group} is no longer in the engine"))).await;
                    false
                }
                Err(e) => {
                    debug!("group {group}: {e}");
                    true
                }
            },
            Step::Delete { group, keep } => w.delete(c, &groups, group.as_deref(), keep).await,
        };
    }
    pending
}

fn error(message: &str) -> Value {
    json!({ "status": { "error": {
        "message": message,
        "time": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    }, "readyToUse": false } })
}

struct Writer<'a> {
    client: &'a reqwest::Client,
    api_url: &'a str,
    events: Option<&'a crate::events::EventRecorder>,
}

impl Writer<'_> {
    fn url(&self, c: &Value) -> String {
        format!("{}{API}/volumesnapshotcontents/{}", self.api_url, c["metadata"]["name"].as_str().unwrap_or_default())
    }

    async fn merge(&self, url: String, patch: &Value) -> bool {
        self.client
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(patch)
            .send_retrying(retry::Policy::API)
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    async fn status(&self, c: &Value, status: &Value) -> bool {
        self.merge(format!("{}/status", self.url(c)), status).await
    }

    async fn event(&self, c: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = self.events {
            r.object_event("snapshot.storage.k8s.io/v1", "VolumeSnapshotContent", c, etype, reason, message).await;
        }
    }

    /// Cut the volume; `true` while it is to be tried again.
    async fn cut(&self, c: &Value, groups: &Groups, id: &str, node: &str) -> bool {
        let name = c["metadata"]["name"].as_str().unwrap_or_default().to_string();
        let marked = json!({ "metadata": { "annotations": { BEING_CREATED: "yes", NODE_ANNOTATION: node } } });
        if !self.merge(self.url(c), &marked).await {
            return true;
        }
        let volume = c["spec"]["source"]["volumeHandle"].as_str().unwrap_or_default();
        let result = groups.cut(&name, id).await;
        let pending = match result {
            Ok(g) => {
                let st = status_of(&g);
                if !self.status(c, &st).await {
                    return true;
                }
                info!("VolumeSnapshotContent {name}: {volume} cut as group {} ({} bytes)", g.id, st["status"]["restoreSize"]);
                self.event(c, "Normal", "SnapshotCreated", &format!("Snapshot of {volume} created: group {}", g.id)).await;
                st["status"]["readyToUse"].as_bool() != Some(true)
            }
            Err(e) => {
                let msg = format!("Failed to create snapshot of {volume}: {e}");
                warn!("VolumeSnapshotContent {name}: {msg}");
                self.status(c, &error(&msg)).await;
                self.event(c, "Warning", "SnapshotCreationFailed", &msg).await;
                false
            }
        };
        let unmarked = json!({ "metadata": { "annotations": { BEING_CREATED: null } } });
        self.merge(self.url(c), &unmarked).await;
        pending
    }

    /// The group (unless kept), then the finalizer; `true` while it is to be
    /// tried again.
    async fn delete(&self, c: &Value, groups: &Groups, group: Option<&str>, keep: bool) -> bool {
        let name = c["metadata"]["name"].as_str().unwrap_or_default();
        if let (Some(g), false) = (group, keep) {
            if let Err(e) = groups.delete(g).await {
                warn!("VolumeSnapshotContent {name}: group {g} not deleted: {e}");
                self.event(c, "Warning", "SnapshotDeleteError", &format!("Failed to delete snapshot {g}: {e}")).await;
                return true;
            }
            info!("VolumeSnapshotContent {name}: group {g} deleted");
        }
        let left: Vec<Value> = c["metadata"]["finalizers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|f| f.as_str() != Some(CONTENT_FINALIZER))
            .cloned()
            .collect();
        // A merge patch replaces the list whole; the resourceVersion makes it
        // refuse a list that changed since it was read.
        let patch = json!({ "metadata": { "finalizers": left, "resourceVersion": c["metadata"]["resourceVersion"] } });
        !self.merge(self.url(c), &patch).await
    }
}

/// A claim's snapshot, found: its group in this node's engine and the
/// content that names it.
pub struct Source {
    pub group: stormvm_block::GroupSnapshot,
    pub content: String,
}

/// The snapshot a claim names, resolved to its group in this node's engine.
pub async fn source(
    client: &reqwest::Client,
    api_url: &str,
    groups: &Groups,
    namespace: &str,
    snapshot: &str,
    node: &str,
) -> Result<Source, String> {
    let get = |url: String| async move {
        match client.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.map_err(|e| e.to_string()),
            Ok(r) => Err(format!("{}", r.status())),
            Err(e) => Err(e.to_string()),
        }
    };
    let vs = get(format!("{api_url}{API}/namespaces/{namespace}/volumesnapshots/{snapshot}"))
        .await
        .map_err(|e| format!("VolumeSnapshot {namespace}/{snapshot}: {e}"))?;
    if vs["status"]["readyToUse"].as_bool() != Some(true) {
        return Err(format!("VolumeSnapshot {namespace}/{snapshot} is not ready to use yet"));
    }
    let content_name = vs["status"]["boundVolumeSnapshotContentName"]
        .as_str()
        .ok_or_else(|| format!("VolumeSnapshot {namespace}/{snapshot} is bound to no content"))?;
    let content = get(format!("{api_url}{API}/volumesnapshotcontents/{content_name}"))
        .await
        .map_err(|e| format!("VolumeSnapshotContent {content_name}: {e}"))?;
    if content["spec"]["driver"].as_str() != Some(DRIVER) {
        return Err(format!("VolumeSnapshot {namespace}/{snapshot} is driver {}'s, not {DRIVER}'s", content["spec"]["driver"]));
    }
    let group = handle(&content).ok_or_else(|| format!("VolumeSnapshotContent {content_name} has no snapshot handle yet"))?;
    if let Some(n) = content["metadata"]["annotations"][NODE_ANNOTATION].as_str().filter(|n| *n != node) {
        return Err(format!("VolumeSnapshot {namespace}/{snapshot} is held by node {n}'s storage: a pod restoring it runs there"));
    }
    match groups.get(&group).await? {
        Some(g) => Ok(Source { group: g, content: content_name.to_string() }),
        None => Err(format!("VolumeSnapshot {namespace}/{snapshot}: group {group} is not in this node's engine")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(source: Value) -> Value {
        json!({
            "metadata": { "name": "snapcontent-1", "resourceVersion": "5", "finalizers": [CONTENT_FINALIZER] },
            "spec": { "driver": DRIVER, "deletionPolicy": "Delete", "source": source,
                      "volumeSnapshotRef": { "namespace": "web", "name": "snap", "uid": "vs-1" } },
        })
    }

    fn here() -> HashMap<String, String> {
        HashMap::from([("pvc-web-data".to_string(), "u-1".to_string())])
    }

    #[test]
    fn a_new_content_of_a_volume_here_is_cut() {
        let c = content(json!({ "volumeHandle": "pvc-web-data" }));
        assert_eq!(decide(&c, &here(), |_| false), Step::Cut { id: "u-1".into() });
        // Another node's volume, another driver's content: not ours.
        assert_eq!(decide(&c, &HashMap::new(), |_| false), Step::Nothing);
        let mut other = c.clone();
        other["spec"]["driver"] = json!("csi.example.com");
        assert_eq!(decide(&other, &here(), |_| true), Step::Nothing);
    }

    #[test]
    fn a_cut_content_waits_for_ready_then_rests() {
        let mut c = content(json!({ "volumeHandle": "pvc-web-data" }));
        c["status"] = json!({ "snapshotHandle": "g-1", "readyToUse": false });
        assert_eq!(decide(&c, &here(), |_| true), Step::Refresh { group: "g-1".into() });
        c["status"]["readyToUse"] = json!(true);
        assert_eq!(decide(&c, &here(), |_| true), Step::Nothing);
    }

    #[test]
    fn a_refused_cut_is_not_retried() {
        let mut c = content(json!({ "volumeHandle": "pvc-web-data" }));
        c["status"] = json!({ "error": { "message": "no" } });
        assert_eq!(decide(&c, &here(), |_| false), Step::Nothing);
    }

    #[test]
    fn deletion_follows_the_controller_and_the_policy() {
        let mut c = content(json!({ "volumeHandle": "pvc-web-data" }));
        c["status"] = json!({ "snapshotHandle": "g-1", "readyToUse": true });
        c["metadata"]["deletionTimestamp"] = json!("2026-11-18T00:00:00Z");
        // Not marked by the controller yet, and bound: wait for it.
        assert_eq!(decide(&c, &here(), |_| true), Step::Nothing);
        c["metadata"]["annotations"] = json!({ BEING_DELETED: "yes" });
        assert_eq!(decide(&c, &here(), |_| true), Step::Delete { group: Some("g-1".into()), keep: false });
        c["spec"]["deletionPolicy"] = json!("Retain");
        assert_eq!(decide(&c, &here(), |_| true), Step::Delete { group: Some("g-1".into()), keep: true });
        // The finalizer already off: nothing left.
        c["metadata"]["finalizers"] = json!([]);
        assert_eq!(decide(&c, &here(), |_| true), Step::Nothing);
    }

    #[test]
    fn a_pre_provisioned_content_is_ours_when_its_group_is_here() {
        let mut c = content(json!({ "snapshotHandle": "g-9" }));
        c["spec"]["volumeSnapshotRef"]["uid"] = json!("");
        c["metadata"]["deletionTimestamp"] = json!("2026-11-18T00:00:00Z");
        assert_eq!(decide(&c, &here(), |g| g == "g-9"), Step::Delete { group: Some("g-9".into()), keep: false });
        assert_eq!(decide(&c, &here(), |_| false), Step::Nothing);
    }

    #[test]
    fn the_status_is_in_upstreams_units() {
        let g = stormvm_block::GroupSnapshot {
            id: "g-1".into(),
            name: "snapcontent-1".into(),
            snapshots: vec![stormvm_block::Snapshot {
                id: "s-1".into(),
                name: "s".into(),
                source_volume_id: "u-1".into(),
                size_bytes: 1 << 30,
                ready: true,
                created_at_ms: 1_700_000_000_123,
                group_snapshot_id: Some("g-1".into()),
            }],
            ready: true,
            created_at_ms: 1_700_000_000_123,
        };
        let s = status_of(&g);
        assert_eq!(s["status"]["snapshotHandle"], "g-1");
        assert_eq!(s["status"]["creationTime"], 1_700_000_000_123_000_000i64);
        assert_eq!(s["status"]["restoreSize"], 1u64 << 30);
        assert_eq!(s["status"]["readyToUse"], true);
    }
}
