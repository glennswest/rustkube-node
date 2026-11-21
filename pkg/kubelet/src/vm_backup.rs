//! VM backups off the node (#380, stormvm#31, the owner's decision A): the
//! kubelet's half.
//!
//! **A backup is a VirtualMachineSnapshot with `storm.io/backup-to: <store>`.**
//! It is taken as any snapshot is (`vm_snapshot.rs`, #28). Once it reads
//! `Succeeded` on the node that took it, its group's members are shipped to
//! the store by stormvm's [`Transfer`] (changed extents only, against the last
//! backup to the same store whose group is still on the node; a first backup
//! to a store is a full copy, #458), which is plain data kept in the
//! snapshot's `status["storm.io/backup-transfer"]` and stepped one round of
//! engine calls per pass, off the async threads. `status["storm.io/backup"]`
//! says where it is (`stormvm_spec::backup::backup_status`). `waitingEngine`
//! (an engine without stormblock#399/#400) is asked again, never failed.
//!
//! **Once it is `done`:** the older backups' groups on the node in the same
//! store are deleted (only each store's newest is kept, as that store's next
//! base: `storm.io/backupBaseReleased` on each; a base is never taken from
//! another store, #458), the store keeps the VM's newest `storm.io/backup-keep` points,
//! and a scheduled VirtualMachineSnapshot whose point was pruned is deleted.
//!
//! **Schedule:** a VirtualMachine registered on this node with
//! `storm.io/backup-every` gets a `scheduled_snapshot` when one is due. A bad
//! annotation is one Event on the VM per distinct problem.
//!
//! **Restore from the store** is `vm_restore.rs`: a restore of a snapshot
//! whose group is gone from the node but whose point is on the store copies
//! the point back ([`Transfer::restore`]).
//!
//! **Stores** are named on the command line (`--backup-store
//! name=url@node[,token=<file>]`): the store engine's URL, the node that
//! engine serves its attaches on, and its token. A store is another node's
//! engine, with its own token; with no token file the node's own is
//! presented, which is right only for this node's engine.

use retry::RetryExt;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use stormvm_block::backup::{Phase, Transfer};
use stormvm_block::GroupSnapshot;
use stormvm_spec::backup as policy;
use tracing::{debug, info, warn};

use crate::vm_snapshot::{DISKS_ANNOTATION, NODE_ANNOTATION};

/// Where a backup's copy is kept between passes.
pub const TRANSFER_STATUS: &str = "storm.io/backup-transfer";
/// What `stormvm_spec::backup::backup_status` says of it.
pub const BACKUP_STATUS: &str = "storm.io/backup";
/// On a done backup whose node group was deleted, once a newer one is done.
pub const BASE_RELEASED: &str = "storm.io/backupBaseReleased";
/// On a done backup whose after-steps (release, prune) all went through.
pub const SETTLED: &str = "storm.io/backupSettled";
/// Where a restore from a store keeps its copy.
pub const RESTORE_TRANSFER: &str = "storm.io/restore-transfer";

const API: &str = "snapshot.kubevirt.io/v1beta1";
/// How soon an unfinished copy is stepped again.
pub const STEP_EVERY: std::time::Duration = std::time::Duration::from_secs(5);
/// How soon a store the kubelet does not know is asked about again.
const UNKNOWN_STORE_AGAIN: std::time::Duration = std::time::Duration::from_secs(60);

/// An engine to talk to: its URL and the token file it wants (`None`: the
/// node's own token, found as a node finds it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub url: String,
    pub token_file: Option<String>,
}

/// A backup store (`--backup-store`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Store {
    pub name: String,
    pub endpoint: Endpoint,
    /// The node the store's engine serves its attaches on.
    pub node: String,
}

/// `name=url@node[,token=<file>]`.
pub fn parse_store(s: &str) -> Result<Store, String> {
    let s = s.trim();
    let (main, token) = match s.split_once(",token=") {
        Some((m, t)) if !t.trim().is_empty() => (m, Some(t.trim().to_string())),
        Some(_) => return Err(format!("{s:?}: token= names no file")),
        None => (s, None),
    };
    let (name, rest) = main.split_once('=').ok_or_else(|| format!("{s:?}: not name=url@node"))?;
    let (url, node) = rest.rsplit_once('@').ok_or_else(|| format!("{s:?}: no @node after the URL"))?;
    let (name, url, node) = (name.trim(), url.trim(), node.trim());
    if name.is_empty() || url.is_empty() || node.is_empty() {
        return Err(format!("{s:?}: name, URL and node are all needed"));
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(format!("{s:?}: {url} is not an http(s) URL"));
    }
    Ok(Store { name: name.into(), endpoint: Endpoint { url: url.trim_end_matches('/').into(), token_file: token }, node: node.into() })
}

/// The stormblock calls a backup needs, blocking. A seam for the tests; the
/// real one is stormvm's client.
pub trait BackupEngine: Send + Sync {
    fn group(&self, at: &Endpoint, id: &str) -> Result<Option<GroupSnapshot>, String>;
    fn delete_group(&self, at: &Endpoint, id: &str) -> Result<(), String>;
    /// One round of [`Transfer::step`]: `source` ships into `target`.
    fn step(&self, t: &mut Transfer, source: &Endpoint, target: &Endpoint) -> Result<Phase, String>;
    /// The points of `ns/vm` on a store, oldest first.
    fn points(&self, at: &Endpoint, ns: &str, vm: &str) -> Result<Vec<GroupSnapshot>, String>;
    /// Keep the newest `keep` points; the names deleted.
    fn prune(&self, at: &Endpoint, ns: &str, vm: &str, keep: usize) -> Result<Vec<String>, String>;
}

/// stormvm's client.
pub struct Engines;

fn client(at: &Endpoint) -> stormvm_block::Client {
    match &at.token_file {
        Some(f) => stormvm_block::Client::with_token(
            &at.url,
            stormvm_block::Token::from_sources(None, vec![std::path::PathBuf::from(f)]),
        ),
        None => stormvm_block::Client::new(&at.url),
    }
}

impl BackupEngine for Engines {
    fn group(&self, at: &Endpoint, id: &str) -> Result<Option<GroupSnapshot>, String> {
        client(at).group_snapshot_by_id(id).map_err(|e| e.to_string())
    }
    fn delete_group(&self, at: &Endpoint, id: &str) -> Result<(), String> {
        client(at).delete_group_snapshot(id).map_err(|e| e.to_string())
    }
    fn step(&self, t: &mut Transfer, source: &Endpoint, target: &Endpoint) -> Result<Phase, String> {
        t.step(&client(source), &client(target)).map_err(|e| e.to_string())
    }
    fn points(&self, at: &Endpoint, ns: &str, vm: &str) -> Result<Vec<GroupSnapshot>, String> {
        client(at).points(ns, vm).map_err(|e| e.to_string())
    }
    fn prune(&self, at: &Endpoint, ns: &str, vm: &str, keep: usize) -> Result<Vec<String>, String> {
        client(at).prune_points(ns, vm, keep).map_err(|e| e.to_string())
    }
}

fn ns_name(obj: &Value) -> (String, String) {
    (
        obj["metadata"]["namespace"].as_str().unwrap_or("default").to_string(),
        obj["metadata"]["name"].as_str().unwrap_or("").to_string(),
    )
}

/// The VM a snapshot is of.
pub fn source_vm(obj: &Value) -> &str {
    obj["spec"]["source"]["name"].as_str().unwrap_or("")
}

fn created_ms(obj: &Value) -> i64 {
    obj["metadata"]["creationTimestamp"]
        .as_str()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.timestamp_millis())
        .unwrap_or(0)
}

fn backup_phase(obj: &Value) -> &str {
    obj["status"][BACKUP_STATUS]["phase"].as_str().unwrap_or("")
}

/// The store a backup went to: its status's `store` (where the copy was
/// made), else the `storm.io/backup-to` it asks for.
fn store_of(obj: &Value) -> Option<String> {
    obj["status"][BACKUP_STATUS]["store"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| policy::backup_to(obj).map(|s| s.to_string()))
}

/// The backups of the same VM as `obj`, taken by `node`, to `store`, done,
/// whose group is still on the node, older than `obj`: newest first.
///
/// **Per store** (#458, 59fb97c4517b): a base is only a base in the store that
/// holds its point. Another store's newest backup would be a diff whose
/// unchanged blocks were never copied here, so the first backup to a store is
/// a full copy, and each store's newest group stays on the node as its base
/// until a later backup to that store is done.
pub fn older_done<'a>(items: &'a [Value], obj: &Value, node: &str, store: &str) -> Vec<&'a Value> {
    let (ns, name) = ns_name(obj);
    let vm = source_vm(obj);
    let at = created_ms(obj);
    let mut v: Vec<&Value> = items
        .iter()
        .filter(|o| {
            let (n, m) = ns_name(o);
            n == ns
                && m != name
                && source_vm(o) == vm
                && o["metadata"]["annotations"][NODE_ANNOTATION].as_str() == Some(node)
                && backup_phase(o) == "done"
                && store_of(o).as_deref() == Some(store)
                && o["status"][BASE_RELEASED] != json!(true)
                && o["status"]["virtualMachineSnapshotContentName"].as_str().is_some_and(|g| !g.is_empty())
                && created_ms(o) <= at
        })
        .collect();
    v.sort_by_key(|o| std::cmp::Reverse(created_ms(o)));
    v
}

/// When `vm`'s newest backup (labelled `storm.io/backup-of`) was made.
pub fn last_backup_ms(items: &[Value], ns: &str, vm: &str) -> Option<i64> {
    items
        .iter()
        .filter(|o| ns_name(o).0 == ns && o["metadata"]["labels"]["storm.io/backup-of"].as_str() == Some(vm))
        .map(created_ms)
        .max()
}

/// The disks of a snapshot that its group holds: `(disk, volume id)`.
pub fn group_disks(obj: &Value, group: &GroupSnapshot) -> Vec<(String, String)> {
    let recorded: serde_json::Map<String, Value> = obj["metadata"]["annotations"][DISKS_ANNOTATION]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();
    let mut v: Vec<(String, String)> = recorded
        .into_iter()
        .filter_map(|(d, vol)| vol.as_str().map(|v| (d, v.to_string())))
        .filter(|(_, v)| group.of(v).is_some())
        .collect();
    v.sort();
    v
}

/// The phase's name as the status says it (serde's).
pub fn phase_name(p: Phase) -> String {
    serde_json::to_value(p).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
}

/// `status["storm.io/backup"]` for a transfer.
pub fn status_of(t: &Transfer, store: &str) -> Value {
    let (total, done) = t.progress();
    policy::backup_status(&policy::BackupStatus {
        phase: phase_name(t.phase),
        store: store.to_string(),
        point: (t.phase == Phase::Done).then(|| t.point.clone()).flatten(),
        incremental: t.members.iter().any(|m| m.base.is_some()),
        bytes_total: total,
        bytes_done: done,
        message: t.message.clone(),
    })
}

/// The kubelet's backups.
pub struct Backups {
    api: reqwest::Client,
    api_url: String,
    node: String,
    run_dir: String,
    /// This node's engine.
    pub(crate) local: Endpoint,
    pub(crate) stores: Vec<Store>,
    pub(crate) engine: Arc<dyn BackupEngine>,
    events: Option<crate::events::EventRecorder>,
    /// `<ns>/<vm>` → the policy problem last said, so it is said once.
    said: Mutex<HashMap<String, String>>,
    /// Copies being stepped now, so a pass never steps one twice at once.
    stepping: Mutex<HashSet<String>>,
}

impl Backups {
    pub fn new(api: reqwest::Client, api_url: &str, node: &str, run_dir: &str, local: Endpoint, stores: Vec<Store>) -> Backups {
        let api_url = api_url.trim_end_matches('/').to_string();
        let events = (!api_url.is_empty()).then(|| crate::events::EventRecorder::new(api.clone(), &api_url, node));
        Backups {
            api,
            api_url,
            node: node.into(),
            run_dir: run_dir.into(),
            local,
            stores,
            engine: Arc::new(Engines),
            events,
            said: Default::default(),
            stepping: Default::default(),
        }
    }

    /// Use `engine` instead of stormvm's client (tests).
    pub fn with_engine(mut self, engine: Arc<dyn BackupEngine>) -> Backups {
        self.engine = engine;
        self
    }

    pub fn store(&self, name: &str) -> Option<&Store> {
        self.stores.iter().find(|s| s.name == name)
    }

    /// The stores as stormvm's console lists them: name → URL.
    pub fn console_stores(stores: &[Store]) -> Vec<(String, String)> {
        stores.iter().map(|s| (s.name.clone(), s.endpoint.url.clone())).collect()
    }

    /// One pass over the snapshots (`items`, every one in the cluster): each
    /// backup this node took is stepped, then the schedule.
    pub async fn pass(&self, items: &[Value]) {
        for obj in items {
            if !obj["metadata"]["deletionTimestamp"].is_null() {
                continue;
            }
            let Some(store) = policy::backup_to(obj) else { continue };
            if obj["metadata"]["annotations"][NODE_ANNOTATION].as_str() != Some(self.node.as_str())
                || obj["status"]["phase"].as_str() != Some("Succeeded")
            {
                continue;
            }
            if backup_phase(obj) == "failed" || (backup_phase(obj) == "done" && obj["status"][SETTLED] == json!(true)) {
                continue;
            }
            self.backup(obj, items, &store).await;
        }
        self.schedule(items).await;
    }

    async fn backup(&self, obj: &Value, items: &[Value], store_name: &str) {
        let (ns, name) = ns_name(obj);
        let key = format!("{ns}/{name}");
        if !self.stepping.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone()) {
            return;
        }
        self.backup_once(obj, items, store_name).await;
        self.stepping.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
    }

    async fn backup_once(&self, obj: &Value, items: &[Value], store_name: &str) {
        let (ns, name) = ns_name(obj);
        let vm = source_vm(obj).to_string();
        let Some(store) = self.store(store_name).cloned() else {
            let why = format!(
                "no backup store {store_name:?} on node {}: the kubelet knows {:?} (--backup-store)",
                self.node,
                self.stores.iter().map(|s| s.name.as_str()).collect::<Vec<_>>()
            );
            let st = policy::backup_status(&policy::BackupStatus {
                phase: "waitingEngine".into(),
                store: store_name.into(),
                message: Some(why),
                ..Default::default()
            });
            if obj["status"][BACKUP_STATUS] != st {
                self.patch_status(&ns, &name, json!({ BACKUP_STATUS: st })).await;
            }
            apimachinery::reactor::requeue_after(UNKNOWN_STORE_AGAIN);
            return;
        };
        // Done already, the after-steps not all through: only those.
        if backup_phase(obj) == "done" {
            self.settle(obj, items, &store).await;
            return;
        }
        let kept: Option<Transfer> = serde_json::from_value(obj["status"][TRANSFER_STATUS].clone()).ok();
        let mut t = match kept {
            Some(t) => t,
            None => match self.new_transfer(obj, items, &ns, &name, &vm, &store).await {
                Ok(t) => t,
                Err(why) => {
                    let st = policy::backup_status(&policy::BackupStatus {
                        phase: "failed".into(),
                        store: store.name.clone(),
                        message: Some(why.clone()),
                        ..Default::default()
                    });
                    self.patch_status(&ns, &name, json!({ BACKUP_STATUS: st })).await;
                    self.event(obj, "Warning", "BackupFailed", &why).await;
                    return;
                }
            },
        };
        let (engine, local, target) = (self.engine.clone(), self.local.clone(), store.endpoint.clone());
        let stepped = tokio::task::spawn_blocking(move || {
            let r = engine.step(&mut t, &local, &target);
            (t, r)
        })
        .await;
        let (t, r) = match stepped {
            Ok(x) => x,
            Err(e) => {
                warn!("{ns}/{name}: the backup step did not finish: {e}");
                apimachinery::reactor::requeue_after(STEP_EVERY);
                return;
            }
        };
        let mut st = status_of(&t, &store.name);
        if let Err(e) = &r {
            // Unanswered: the transfer is where it was; asked again.
            st["message"] = json!(format!("the engines did not answer: {e}"));
        }
        let transfer = serde_json::to_value(&t).unwrap_or(Value::Null);
        self.patch_status(&ns, &name, json!({ TRANSFER_STATUS: transfer, BACKUP_STATUS: st })).await;
        match t.phase {
            Phase::Done => {
                info!("{ns}/{name}: backed up to {} as {}", store.name, t.point.as_deref().unwrap_or(""));
                self.event(obj, "Normal", "BackedUp", &format!("backed up to {} as {}", store.name, t.point.as_deref().unwrap_or(""))).await;
                let mut done = obj.clone();
                done["status"][BACKUP_STATUS] = status_of(&t, &store.name);
                self.settle(&done, items, &store).await;
            }
            Phase::Failed => {
                let why = t.message.clone().unwrap_or_else(|| "the copy failed".into());
                self.event(obj, "Warning", "BackupFailed", &why).await;
            }
            _ => apimachinery::reactor::requeue_after(STEP_EVERY),
        }
    }

    async fn new_transfer(&self, obj: &Value, items: &[Value], ns: &str, name: &str, vm: &str, store: &Store) -> Result<Transfer, String> {
        let group_id = obj["status"]["virtualMachineSnapshotContentName"].as_str().unwrap_or("").to_string();
        if group_id.is_empty() {
            return Err("the snapshot names no group".into());
        }
        let base_id = older_done(items, obj, &self.node, &store.name)
            .first()
            .and_then(|o| o["status"]["virtualMachineSnapshotContentName"].as_str().map(str::to_string));
        let (engine, local) = (self.engine.clone(), self.local.clone());
        let (group, base) = tokio::task::spawn_blocking(move || {
            let group = engine.group(&local, &group_id);
            let base = base_id.map(|b| engine.group(&local, &b));
            (group, base)
        })
        .await
        .map_err(|e| format!("the group was not read: {e}"))?;
        let group = group.map_err(|e| format!("the node's engine: {e}"))?.ok_or("the snapshot's group is gone from this node")?;
        // A base that cannot be read is a full copy, never a wrong diff.
        let base = base.and_then(|b| b.ok()).flatten();
        let disks = group_disks(obj, &group);
        if disks.is_empty() {
            return Err("the snapshot records no disk in its group".into());
        }
        Transfer::backup(ns, vm, name, &group, &disks, base.as_ref(), &self.node, &store.node)
    }

    /// After a backup is done: the older backups' node groups in the same
    /// store go (this one is that store's next base; another store's stays,
    /// #458), the store keeps the VM's newest `keep` points, and a
    /// scheduled snapshot whose point went is deleted. All through: settled.
    async fn settle(&self, obj: &Value, items: &[Value], store: &Store) {
        let (ns, name) = ns_name(obj);
        let vm = source_vm(obj).to_string();
        let mut ok = true;
        for old in older_done(items, obj, &self.node, &store.name) {
            let (ons, oname) = ns_name(old);
            let g = old["status"]["virtualMachineSnapshotContentName"].as_str().unwrap_or("").to_string();
            let (engine, local) = (self.engine.clone(), self.local.clone());
            match tokio::task::spawn_blocking(move || engine.delete_group(&local, &g)).await {
                Ok(Ok(())) => self.patch_status(&ons, &oname, json!({ BASE_RELEASED: true })).await,
                Ok(Err(e)) => {
                    warn!("{ons}/{oname}: its node group not released: {e}");
                    ok = false;
                }
                Err(e) => {
                    warn!("{ons}/{oname}: its node group not released: {e}");
                    ok = false;
                }
            }
        }
        if let Some(keep) = self.keep_of(&ns, &vm).await {
            let (engine, at, n, v) = (self.engine.clone(), store.endpoint.clone(), ns.clone(), vm.clone());
            match tokio::task::spawn_blocking(move || engine.prune(&at, &n, &v, keep)).await {
                Ok(Ok(gone)) => {
                    for o in items {
                        let (ons, oname) = ns_name(o);
                        let scheduled = o["metadata"]["labels"]["storm.io/scheduled"].as_str() == Some("true");
                        let point = stormvm_block::backup::point_name(&ns, &vm, &oname);
                        if ons == ns && source_vm(o) == vm && scheduled && gone.contains(&point) {
                            self.delete_snapshot(&ons, &oname).await;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("{ns}/{vm}: the store's points not pruned: {e}");
                    ok = false;
                }
                Err(e) => {
                    warn!("{ns}/{vm}: the store's points not pruned: {e}");
                    ok = false;
                }
            }
        }
        if ok {
            self.patch_status(&ns, &name, json!({ SETTLED: true })).await;
        } else {
            apimachinery::reactor::requeue_after(UNKNOWN_STORE_AGAIN);
        }
    }

    /// The VM's `storm.io/backup-keep`, when it has a schedule.
    async fn keep_of(&self, ns: &str, vm: &str) -> Option<usize> {
        let obj = self.get(&format!("/apis/kubevirt.io/v1/namespaces/{ns}/virtualmachines/{vm}")).await?;
        policy::policy(&obj).ok().flatten().map(|p| p.keep)
    }

    /// Every VirtualMachine registered on this node with a due schedule gets
    /// its snapshot.
    async fn schedule(&self, items: &[Value]) {
        let Some(list) = self.get("/apis/kubevirt.io/v1/virtualmachines").await else { return };
        let now = chrono::Utc::now().timestamp_millis();
        for vm in list["items"].as_array().cloned().unwrap_or_default() {
            let (ns, name) = ns_name(&vm);
            if stormvm_node::console::find(&self.run_dir, &ns, &name).is_none() {
                continue; // another node's machine, or not running: its node schedules it
            }
            let key = format!("{ns}/{name}");
            let p = match policy::policy(&vm) {
                Ok(Some(p)) => p,
                Ok(None) => continue,
                Err(rejected) => {
                    let why = rejected.0.join("; ");
                    let fresh = self.said.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone(), why.clone()) != Some(why.clone());
                    if fresh {
                        warn!("{key}: backup schedule refused: {why}");
                        self.object_event("kubevirt.io/v1", "VirtualMachine", &vm, "Warning", "BackupScheduleInvalid", &why).await;
                    }
                    continue;
                }
            };
            self.said.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
            let last = last_backup_ms(items, &ns, &name);
            if !policy::due(&p, last, now) {
                apimachinery::reactor::requeue_after(std::time::Duration::from_millis(
                    (policy::next_ms(&p, last, now) - now).max(1000) as u64,
                ));
                continue;
            }
            let snap = policy::scheduled_snapshot(&ns, &name, &p.store, now);
            let url = format!("{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots", self.api_url);
            match self.api.post(&url).json(&snap).send_repeatable(retry::Policy::API).await {
                Ok(r) if r.status().is_success() || r.status().as_u16() == 409 => {
                    info!("{key}: scheduled backup {} to {}", policy::scheduled_name(&name, now), p.store);
                }
                Ok(r) => warn!("{key}: scheduled backup not created: {}", r.status()),
                Err(e) => warn!("{key}: scheduled backup not created: {e}"),
            }
        }
    }

    /// A restore from a store (#380): step `t` (store → node) one round.
    pub(crate) async fn step_restore(&self, t: Transfer, store: &Store) -> (Transfer, Result<Phase, String>) {
        let (engine, source, target) = (self.engine.clone(), store.endpoint.clone(), self.local.clone());
        let kept = t.clone();
        let mut t = t;
        match tokio::task::spawn_blocking(move || {
            let r = engine.step(&mut t, &source, &target);
            (t, r)
        })
        .await
        {
            Ok(x) => x,
            // Where it was: stepped again on the next pass.
            Err(e) => (kept, Err(format!("the restore step did not finish: {e}"))),
        }
    }

    /// A store's points of `ns/vm`, oldest first.
    pub(crate) async fn points(&self, store: &Store, ns: &str, vm: &str) -> Result<Vec<GroupSnapshot>, String> {
        let (engine, at, n, v) = (self.engine.clone(), store.endpoint.clone(), ns.to_string(), vm.to_string());
        tokio::task::spawn_blocking(move || engine.points(&at, &n, &v)).await.map_err(|e| e.to_string())?
    }

    pub(crate) fn node(&self) -> &str {
        &self.node
    }

    async fn get(&self, path: &str) -> Option<Value> {
        if self.api_url.is_empty() {
            return None;
        }
        match self.api.get(format!("{}{path}", self.api_url)).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json().await.ok(),
            Ok(r) => {
                debug!("GET {path}: {}", r.status());
                None
            }
            Err(e) => {
                debug!("GET {path}: {e}");
                None
            }
        }
    }

    async fn patch_status(&self, ns: &str, name: &str, status: Value) {
        if self.api_url.is_empty() {
            return;
        }
        let url = format!("{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}/status", self.api_url);
        match self
            .api
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(&json!({ "status": status }))
            .send_retrying(retry::Policy::API)
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => {
                debug!("{ns}/{name}: backup status not written: {}", r.status());
                apimachinery::reactor::failed();
            }
            Err(e) => {
                debug!("{ns}/{name}: backup status not written: {e}");
                apimachinery::reactor::failed();
            }
        }
    }

    async fn delete_snapshot(&self, ns: &str, name: &str) {
        let url = format!("{}/apis/{API}/namespaces/{ns}/virtualmachinesnapshots/{name}", self.api_url);
        match self.api.delete(&url).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() || r.status().as_u16() == 404 => info!("{ns}/{name}: its point was pruned from the store; deleted"),
            Ok(r) => warn!("{ns}/{name}: not deleted after its point was pruned: {}", r.status()),
            Err(e) => warn!("{ns}/{name}: not deleted after its point was pruned: {e}"),
        }
    }

    async fn event(&self, obj: &Value, etype: &str, reason: &str, message: &str) {
        self.object_event(API, "VirtualMachineSnapshot", obj, etype, reason, message).await;
    }

    async fn object_event(&self, api: &str, kind: &str, obj: &Value, etype: &str, reason: &str, message: &str) {
        if let Some(r) = &self.events {
            r.object_event(api, kind, obj, etype, reason, message).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use stormvm_block::Snapshot;

    pub(crate) fn group(id: &str, vols: &[&str]) -> GroupSnapshot {
        GroupSnapshot {
            id: id.into(),
            name: format!("web.web-1.{id}"),
            snapshots: vols
                .iter()
                .map(|v| Snapshot {
                    id: format!("s-{id}-{v}"),
                    name: format!("s-{v}"),
                    source_volume_id: v.to_string(),
                    size_bytes: 1 << 30,
                    ready: true,
                    created_at_ms: 0,
                    group_snapshot_id: Some(id.into()),
                })
                .collect(),
            ready: true,
            created_at_ms: 0,
        }
    }

    /// A stormblock and a store: groups `g-*` on the node; each step lands
    /// every member (`t-<disk>` on the target) and takes the point.
    #[derive(Default)]
    pub(crate) struct Fake {
        pub(crate) calls: Mutex<Vec<String>>,
        pub(crate) points: Mutex<Vec<GroupSnapshot>>,
        pub(crate) pruned: Mutex<Vec<String>>,
    }

    impl BackupEngine for Fake {
        fn group(&self, at: &Endpoint, id: &str) -> Result<Option<GroupSnapshot>, String> {
            self.calls.lock().unwrap().push(format!("group {} {id}", at.url));
            Ok(id.starts_with("g-").then(|| group(id, &["v-root", "v-data"])))
        }
        fn delete_group(&self, at: &Endpoint, id: &str) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("delete {} {id}", at.url));
            Ok(())
        }
        fn step(&self, t: &mut Transfer, source: &Endpoint, target: &Endpoint) -> Result<Phase, String> {
            let bases: Vec<String> = t.members.iter().map(|m| m.base.clone().unwrap_or_default()).collect();
            self.calls.lock().unwrap().push(format!("step {}->{} bases {}", source.url, target.url, bases.join(",")));
            for m in &mut t.members {
                m.target_id = Some(format!("t-{}", m.disk));
                m.bytes_total = 10;
                m.bytes_done = 10;
                m.done = true;
            }
            t.point_id = t.point.as_ref().map(|_| "p-1".to_string());
            t.phase = Phase::Done;
            Ok(Phase::Done)
        }
        fn points(&self, _: &Endpoint, _: &str, _: &str) -> Result<Vec<GroupSnapshot>, String> {
            Ok(self.points.lock().unwrap().clone())
        }
        fn prune(&self, at: &Endpoint, ns: &str, vm: &str, keep: usize) -> Result<Vec<String>, String> {
            self.calls.lock().unwrap().push(format!("prune {} {ns}/{vm} {keep}", at.url));
            Ok(self.pruned.lock().unwrap().clone())
        }
    }

    pub(crate) fn forge() -> Store {
        parse_store("forge=https://forge1.g16.lo:9090@forge1,token=/run/forge/api_token").unwrap()
    }

    const SNAPS: &str = "/apis/snapshot.kubevirt.io/v1beta1/namespaces/web/virtualmachinesnapshots";

    fn backup(name: &str, group: &str, at: &str) -> Value {
        json!({
            "metadata": {
                "name": name, "namespace": "web", "creationTimestamp": at,
                "labels": { "storm.io/backup-of": "web-1" },
                "annotations": {
                    "storm.io/backup-to": "forge",
                    NODE_ANNOTATION: "n1",
                    DISKS_ANNOTATION: r#"{"rootdisk":"v-root","datadisk":"v-data","cloudinitdisk":"v-seed"}"#,
                },
            },
            "spec": { "source": { "apiGroup": "kubevirt.io", "kind": "VirtualMachine", "name": "web-1" } },
            "status": { "phase": "Succeeded", "virtualMachineSnapshotContentName": group },
        })
    }

    fn backups(url: &str, fake: Arc<Fake>, stores: Vec<Store>) -> Backups {
        Backups::new(reqwest::Client::new(), url, "n1", "/nonexistent", Endpoint { url: "http://node".into(), token_file: None }, stores)
            .with_engine(fake)
    }

    #[test]
    fn a_store_is_name_url_at_node_with_its_token() {
        let s = forge();
        assert_eq!((s.name.as_str(), s.endpoint.url.as_str(), s.node.as_str()), ("forge", "https://forge1.g16.lo:9090", "forge1"));
        assert_eq!(s.endpoint.token_file.as_deref(), Some("/run/forge/api_token"));
        assert_eq!(parse_store("local=http://127.0.0.1:9090/@n1").unwrap().endpoint, Endpoint { url: "http://127.0.0.1:9090".into(), token_file: None });
        for bad in ["forge", "forge=https://x", "=https://x@n", "forge=ftp://x@n", "forge=https://x@n,token="] {
            assert!(parse_store(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_base_is_the_newest_done_backup_still_on_the_node() {
        let mut b1 = backup("b1", "g-1", "2026-11-18T01:00:00Z");
        b1["status"][BACKUP_STATUS] = json!({"phase": "done"});
        let mut b0 = backup("b0", "g-0", "2026-11-18T00:00:00Z");
        b0["status"][BACKUP_STATUS] = json!({"phase": "done"});
        let mut released = backup("bx", "g-x", "2026-11-18T01:30:00Z");
        released["status"][BACKUP_STATUS] = json!({"phase": "done"});
        released["status"][BASE_RELEASED] = json!(true);
        let mut other = backup("o1", "g-o", "2026-11-18T01:40:00Z");
        other["spec"]["source"]["name"] = json!("db");
        other["status"][BACKUP_STATUS] = json!({"phase": "done"});
        let b2 = backup("b2", "g-2", "2026-11-18T02:00:00Z");
        let items = vec![b0.clone(), b1.clone(), released, other, b2.clone()];
        let older: Vec<&str> = older_done(&items, &b2, "n1", "forge").iter().map(|o| o["metadata"]["name"].as_str().unwrap()).collect();
        assert_eq!(older, ["b1", "b0"], "newest first; never a released one, another VM's, or itself");
        assert!(older_done(&items, &b2, "n2", "forge").is_empty(), "another node's");
        assert_eq!(last_backup_ms(&items, "web", "web-1"), Some(created_ms(&b2)));
        assert_eq!(last_backup_ms(&items, "web", "nobody"), None);
        let g = group("g-2", &["v-root", "v-data"]);
        assert_eq!(group_disks(&b2, &g), [("datadisk".to_string(), "v-data".to_string()), ("rootdisk".to_string(), "v-root".to_string())],
            "the seed is not in the group: not shipped");
    }

    /// The issue: a backup is shipped once it has Succeeded, the copy kept in
    /// its status, then done and settled; the next is diffed against it, and
    /// its group is released from the node once the next is done.
    #[tokio::test]
    async fn a_backup_is_shipped_settled_and_the_next_one_is_incremental() {
        let b1 = backup("b1", "g-1", "2026-11-18T01:00:00Z");
        let (url, store, _) = crate::vm_restore::tests::api(vec![(&format!("{SNAPS}/b1"), b1)]).await;
        let fake = Arc::new(Fake::default());
        let b = backups(&url, fake.clone(), vec![forge()]);
        let items: Vec<Value> = vec![store.lock().unwrap()[&format!("{SNAPS}/b1")].clone()];
        b.pass(&items).await;
        let b1 = store.lock().unwrap()[&format!("{SNAPS}/b1")].clone();
        assert_eq!(b1["status"][BACKUP_STATUS]["phase"], "done", "{}", b1["status"]);
        assert_eq!(b1["status"][BACKUP_STATUS]["point"], "web.web-1.backup.b1");
        assert_eq!(b1["status"][BACKUP_STATUS]["incremental"], false, "the first is a full copy");
        assert_eq!(b1["status"][TRANSFER_STATUS]["members"][0]["target_id"], "t-datadisk", "the copy is kept in the status");
        assert_eq!(b1["status"][SETTLED], true);
        assert!(fake.calls.lock().unwrap().contains(&"step http://node->https://forge1.g16.lo:9090 bases ,".to_string()), "{:?}", fake.calls.lock().unwrap());

        // Settled: a later pass does nothing to it.
        fake.calls.lock().unwrap().clear();
        b.pass(std::slice::from_ref(&b1)).await;
        assert!(fake.calls.lock().unwrap().is_empty(), "{:?}", fake.calls.lock().unwrap());

        // The next one: diffed against b1's group, which then goes.
        let b2 = backup("b2", "g-2", "2026-11-18T02:00:00Z");
        store.lock().unwrap().insert(format!("{SNAPS}/b2"), b2.clone());
        b.pass(&[b1, b2]).await;
        let calls = fake.calls.lock().unwrap().clone();
        assert!(calls.contains(&"step http://node->https://forge1.g16.lo:9090 bases s-g-1-v-data,s-g-1-v-root".to_string()), "{calls:?}");
        assert!(calls.contains(&"delete http://node g-1".to_string()), "{calls:?}");
        let s = store.lock().unwrap().clone();
        assert_eq!(s[&format!("{SNAPS}/b1")]["status"][BASE_RELEASED], true);
        assert_eq!(s[&format!("{SNAPS}/b2")]["status"][BACKUP_STATUS]["incremental"], true);
    }

    /// #458 (59fb97c4517b): a base is only taken from the same store, by
    /// the store its status records, else the one it asks for.
    #[test]
    fn the_base_is_taken_from_the_same_store_only() {
        let mut a = backup("a1", "g-a", "2026-11-18T01:00:00Z");
        a["status"][BACKUP_STATUS] = json!({"phase": "done", "store": "forge"});
        let mut t = backup("t1", "g-t", "2026-11-18T01:30:00Z");
        t["metadata"]["annotations"]["storm.io/backup-to"] = json!("forge");
        t["status"][BACKUP_STATUS] = json!({"phase": "done", "store": "tape"});
        let mut asked = backup("t0", "g-t0", "2026-11-18T00:30:00Z");
        asked["metadata"]["annotations"]["storm.io/backup-to"] = json!("tape");
        asked["status"][BACKUP_STATUS] = json!({"phase": "done"});
        let now = backup("n", "g-n", "2026-11-18T02:00:00Z");
        let items = vec![a, t, asked, now.clone()];
        let names = |store: &str| -> Vec<String> {
            older_done(&items, &now, "n1", store).iter().map(|o| o["metadata"]["name"].as_str().unwrap().to_string()).collect()
        };
        assert_eq!(names("forge"), ["a1"], "the status's store wins over the annotation");
        assert_eq!(names("tape"), ["t1", "t0"]);
        assert!(names("s3").is_empty(), "a first backup to a store has no base");
    }

    /// #458: a VM whose `backup-to` moves from forge to tape: the first
    /// backup to tape is a full copy and releases nothing; the next to forge
    /// is diffed against forge's base, and releases forge's only.
    #[tokio::test]
    async fn a_new_store_gets_a_full_copy_and_each_store_keeps_its_base() {
        let b1 = backup("b1", "g-1", "2026-11-18T01:00:00Z");
        let (url, store, _) = crate::vm_restore::tests::api(vec![(&format!("{SNAPS}/b1"), b1)]).await;
        let fake = Arc::new(Fake::default());
        let tape = parse_store("tape=https://tape1.g16.lo:9090@tape1").unwrap();
        let b = backups(&url, fake.clone(), vec![forge(), tape]);
        let get = |n: &str| store.lock().unwrap()[&format!("{SNAPS}/{n}")].clone();
        b.pass(&[get("b1")]).await;
        assert_eq!(get("b1")["status"][BACKUP_STATUS]["phase"], "done");

        let mut b2 = backup("b2", "g-2", "2026-11-18T02:00:00Z");
        b2["metadata"]["annotations"]["storm.io/backup-to"] = json!("tape");
        store.lock().unwrap().insert(format!("{SNAPS}/b2"), b2);
        fake.calls.lock().unwrap().clear();
        b.pass(&[get("b1"), get("b2")]).await;
        let calls = fake.calls.lock().unwrap().clone();
        assert!(calls.contains(&"step http://node->https://tape1.g16.lo:9090 bases ,".to_string()), "a full copy to tape: {calls:?}");
        assert!(!calls.iter().any(|c| c.starts_with("delete")), "forge's base stays: {calls:?}");
        assert_eq!(get("b2")["status"][BACKUP_STATUS]["incremental"], false);
        assert_eq!(get("b2")["status"][BACKUP_STATUS]["store"], "tape");
        assert!(get("b1")["status"][BASE_RELEASED].is_null());

        let b3 = backup("b3", "g-3", "2026-11-18T03:00:00Z");
        store.lock().unwrap().insert(format!("{SNAPS}/b3"), b3);
        fake.calls.lock().unwrap().clear();
        b.pass(&[get("b1"), get("b2"), get("b3")]).await;
        let calls = fake.calls.lock().unwrap().clone();
        assert!(calls.contains(&"step http://node->https://forge1.g16.lo:9090 bases s-g-1-v-data,s-g-1-v-root".to_string()), "{calls:?}");
        assert!(calls.contains(&"delete http://node g-1".to_string()), "{calls:?}");
        assert!(!calls.contains(&"delete http://node g-2".to_string()), "tape's base stays: {calls:?}");
        assert_eq!(get("b3")["status"][BACKUP_STATUS]["incremental"], true);
        assert!(get("b2")["status"][BASE_RELEASED].is_null());
    }

    /// A store the kubelet does not know is waited for with why, never failed.
    #[tokio::test]
    async fn an_unknown_store_waits_with_why() {
        let mut b1 = backup("b1", "g-1", "2026-11-18T01:00:00Z");
        b1["metadata"]["annotations"]["storm.io/backup-to"] = json!("tape");
        let (url, store, _) = crate::vm_restore::tests::api(vec![(&format!("{SNAPS}/b1"), b1.clone())]).await;
        let fake = Arc::new(Fake::default());
        backups(&url, fake.clone(), vec![forge()]).pass(&[b1]).await;
        let st = store.lock().unwrap()[&format!("{SNAPS}/b1")]["status"][BACKUP_STATUS].clone();
        assert_eq!(st["phase"], "waitingEngine");
        assert!(st["message"].as_str().unwrap().contains("no backup store \"tape\""), "{st}");
        assert!(fake.calls.lock().unwrap().is_empty());
    }

    /// Not this node's, not Succeeded, or no `backup-to`: left alone.
    #[tokio::test]
    async fn only_this_nodes_succeeded_backups_are_shipped() {
        let fake = Arc::new(Fake::default());
        let b = backups("", fake.clone(), vec![forge()]);
        let mut taking = backup("b1", "g-1", "2026-11-18T01:00:00Z");
        taking["status"]["phase"] = json!("InProgress");
        let mut elsewhere = backup("b2", "g-2", "2026-11-18T01:00:00Z");
        elsewhere["metadata"]["annotations"][NODE_ANNOTATION] = json!("n2");
        let mut plain = backup("b3", "g-3", "2026-11-18T01:00:00Z");
        plain["metadata"]["annotations"].as_object_mut().unwrap().remove("storm.io/backup-to");
        b.pass(&[taking, elsewhere, plain]).await;
        assert!(fake.calls.lock().unwrap().is_empty(), "{:?}", fake.calls.lock().unwrap());
    }
}
