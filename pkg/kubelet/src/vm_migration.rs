//! Live migration of a stormvm-started VMI between nodes (#40): the node
//! half of KubeVirt's `status.migrationState`.
//!
//! **Who writes what** is rustkube's contract
//! (`apimachinery::kubevirt::active_migration`, rustkube#184): the migration
//! controller writes `migrationUid`, `sourceNode` and `mode`, the scheduler
//! `targetNode`; the target kubelet `targetNodeAddress` (and
//! `targetDirectMigrationNodePorts`) once it can receive; the source kubelet
//! `startTimestamp` when it sends, then `completed` or `failed` (with
//! `failureReason`) and `endTimestamp`. After `completed` the controller
//! moves `status.nodeName` to the target.
//!
//! **The disks** (owner on #40/#159: a RAID leg on the destination, let it
//! catch up, then move the memory) are stormstorage's (stormstorage#44): the
//! source asks it to mirror each of the machine's volumes to a fresh volume on
//! the target (`POST /api/v1/nodes/{S}/volumes/{id}/migrate`), and records
//! which migration is which disk's on the VMI ([`DISKS_ANNOTATION`]). The
//! target waits until every one is `synced`, attaches its copies and starts a
//! receiving machine on them. Once the guest runs there, the target cuts each
//! disk over and completes it (S's volume retired); a migration that failed is
//! aborted by the source (T's copy deleted, S's untouched).
//!
//! **The memory** goes over qemu's own TLS (owner on stormvm#30):
//! `-incoming defer`, then `receive_migration_tls` on the target,
//! `send_migration` on the source, both with one [`Tuning`]. The node's
//! certificate pair (serverAuth and clientAuth) is copied into the machine's
//! run directory under the names qemu reads.
//!
//! **What this does not migrate yet**, refused with the reason on the
//! migration rather than half done: cloud-hypervisor (no TLS migration,
//! stormvm#30), a disk the machine does not own (a claim, another driver's
//! claim, a `volume:` it was handed: its PV names the source node), and a VMI
//! on the pod network (its address is the source pod's).

use std::collections::BTreeMap;

use serde_json::{json, Value};
use stormvm_control::migration::{Tls, Tuning};

/// Which stormstorage migration is which disk's, on the VMI: a JSON object
/// `{"migrationUid": "...", "disks": {"<disk>": "<stormstorage id>"}}`. The
/// source writes it once every mirror has started; the target reads it.
pub const DISKS_ANNOTATION: &str = "storm.io/migration-disks";

/// Where a target listens for the memory: the first free port from here
/// (`--migration-port`), one per machine arriving at once.
pub const DEFAULT_PORT: u16 = 49152;
/// How many ports above the first a node uses for machines arriving at once.
pub const PORTS: u16 = 64;

/// How often a migration in flight is looked at again with no event: a disk
/// catching up, the memory transfer.
pub const POLL: std::time::Duration = std::time::Duration::from_secs(2);

/// The node's certificate pair and its CA, as files the kubelet can read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    pub ca: String,
    pub cert: String,
    pub key: String,
}

/// What a node needs to migrate machines: stormstorage, its certificate and
/// the address the target gives the source.
#[derive(Clone, Default)]
pub struct Config {
    /// stormstorage's API (`--stormstorage`), with its token.
    pub stormstorage: Option<crate::engine::EngineClient>,
    /// The node pair (`--migration-tls-cert`, `-key`, `-ca`).
    pub tls: Option<TlsFiles>,
    /// The first port a target listens on.
    pub port: u16,
    /// This node's address, the target's `targetNodeAddress`.
    pub node_ip: String,
}

impl Config {
    pub fn port(&self) -> u16 {
        if self.port == 0 { DEFAULT_PORT } else { self.port }
    }

    /// Why this node cannot take part in a migration, if it cannot.
    pub fn missing(&self) -> Option<String> {
        let mut no = Vec::new();
        if self.stormstorage.is_none() {
            no.push("no stormstorage (--stormstorage)");
        }
        if self.tls.is_none() {
            no.push("no migration certificate (--migration-tls-cert, -key, -ca)");
        }
        if self.node_ip.is_empty() {
            no.push("no node address");
        }
        (!no.is_empty()).then(|| no.join(", "))
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("stormstorage", &self.stormstorage.as_ref().map(|c| c.url().to_string()))
            .field("tls", &self.tls)
            .field("port", &self.port())
            .field("node_ip", &self.node_ip)
            .finish()
    }
}

/// What this node is to a VMI's migration.
#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    /// It sends: `sourceNode` is this node.
    Source(State),
    /// It receives: `targetNode` is this node.
    Target(State),
}

/// One migration, as `status.migrationState` says it is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct State {
    pub uid: String,
    pub source: String,
    pub target: Option<String>,
    pub target_address: Option<String>,
    pub target_port: Option<u16>,
    pub started: bool,
    pub completed: bool,
    pub failed: bool,
    pub abort_requested: bool,
}

impl State {
    pub fn of(vmi: &Value) -> Option<State> {
        let m = &vmi["status"]["migrationState"];
        let text = |k: &str| m[k].as_str().filter(|s| !s.is_empty()).map(str::to_string);
        let uid = text("migrationUid")?;
        Some(State {
            uid,
            source: text("sourceNode").unwrap_or_default(),
            target: text("targetNode"),
            target_address: text("targetNodeAddress"),
            target_port: m["targetDirectMigrationNodePorts"]
                .as_object()
                .and_then(|ports| ports.keys().filter_map(|p| p.parse().ok()).min()),
            started: text("startTimestamp").is_some(),
            completed: m["completed"].as_bool() == Some(true),
            failed: m["failed"].as_bool() == Some(true),
            abort_requested: m["abortRequested"].as_bool() == Some(true),
        })
    }

    /// Neither completed nor failed.
    pub fn active(&self) -> bool {
        !self.completed && !self.failed
    }

    /// Completed, and not failed after all.
    pub fn moved(&self) -> bool {
        self.completed && !self.failed
    }
}

/// This node's part in `vmi`'s migration, if it has one: the source of any
/// migration it recorded (a failed one still has its mirrors to abort, a
/// completed one its machine to let go), the target of one in flight or
/// completed (it adopts the machine and finishes the disks).
pub fn role(vmi: &Value, node: &str) -> Option<Role> {
    let s = State::of(vmi)?;
    if s.target.as_deref() == Some(node) && s.source != node {
        return (!s.failed).then_some(Role::Target(s));
    }
    (s.source == node).then_some(Role::Source(s))
}

/// Is `vmi` this node's business because a migration is bringing it here?
/// Listed by `status.migrationState.targetNode` beside the placements; a
/// failed migration is not (its receiver goes when the VMI leaves the list).
pub fn arriving(vmi: &Value, node: &str) -> bool {
    matches!(role(vmi, node), Some(Role::Target(_)))
}

/// Has the machine moved away from this node, so it must not be started
/// here again while `status.nodeName` still names it?
pub fn moved_away(vmi: &Value, node: &str) -> bool {
    State::of(vmi).is_some_and(|s| s.moved() && s.source == node && s.target.as_deref() != Some(node))
}

/// The disks record ([`DISKS_ANNOTATION`]) for migration `uid`: disk name →
/// stormstorage migration id. `None` when there is none for that migration.
pub fn disks_of(vmi: &Value, uid: &str) -> Option<BTreeMap<String, String>> {
    let text = vmi["metadata"]["annotations"][DISKS_ANNOTATION].as_str()?;
    let v: Value = serde_json::from_str(text).ok()?;
    if v["migrationUid"].as_str() != Some(uid) {
        return None;
    }
    let disks = v["disks"].as_object()?;
    Some(disks.iter().filter_map(|(d, m)| Some((d.clone(), m.as_str()?.to_string()))).collect())
}

/// The metadata patch that records the disks of migration `uid`.
pub fn disks_patch(vmi_uid: &str, uid: &str, disks: &BTreeMap<String, String>) -> Value {
    let record = json!({"migrationUid": uid, "disks": disks}).to_string();
    json!({"metadata": {"uid": vmi_uid, "annotations": {DISKS_ANNOTATION: record}}})
}

/// Why a machine cannot be migrated by this kubelet yet, if it cannot.
/// `kind` is the hypervisor the registration names; `disks` each disk's
/// volume (None: not an engine volume of this node); `owned` the volumes the
/// machine made; `pod_network` whether it is on the pod network.
pub fn refusal(kind: &str, disks: &[(String, Option<String>)], owned: &[String], pod_network: bool) -> Option<String> {
    if stormvm_control::Kind::parse(kind) == stormvm_control::Kind::CloudHypervisor {
        return Some(
            "cloud-hypervisor does not migrate across nodes: it has no TLS migration (stormvm#30)".into(),
        );
    }
    if pod_network {
        return Some("a VMI on the pod network is not migrated yet: its address is its pod's on this node".into());
    }
    for (name, volume) in disks {
        match volume {
            Some(v) if owned.contains(v) => {}
            _ => {
                return Some(format!(
                    "disk {name} is not the machine's own volume (a claim, another driver's claim or a volume it \
                     was handed): only its own disks are migrated yet"
                ))
            }
        }
    }
    None
}

/// The listen URI on the target.
pub fn listen_uri(port: u16) -> String {
    format!("tcp:0.0.0.0:{port}")
}

/// The URI the source sends to.
pub fn send_uri(address: &str, port: u16) -> String {
    match address.parse::<std::net::Ipv6Addr>() {
        Ok(_) => format!("tcp:[{address}]:{port}"),
        Err(_) => format!("tcp:{address}:{port}"),
    }
}

/// The same tuning on both ends: a source with multifd fails at once against
/// a target without it.
pub fn tuning() -> Tuning {
    Tuning::default()
}

/// Where a machine's TLS material is laid out: its run directory under
/// `root` (the run root).
pub fn tls_dir_of(root: &std::path::Path, namespace: &str, name: &str) -> String {
    root.join(namespace).join(name).join("migration-tls").to_string_lossy().into_owned()
}

/// Lay the node's pair out for qemu (stormvm's [`stormvm_control::migration::tls_dir`]
/// names): `ca-cert.pem`, `server-cert.pem`, `server-key.pem`,
/// `client-cert.pem`, `client-key.pem`. Copies, not links: the paths the
/// kubelet reads them by are its own mount view, and the hypervisor runs in
/// the node's. Laid out again for each migration, so a renewed pair is the
/// one used.
pub fn lay_out_tls(dir: &str, files: &TlsFiles) -> std::io::Result<Tls> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    for (name, from) in [
        ("ca-cert.pem", &files.ca),
        ("server-cert.pem", &files.cert),
        ("server-key.pem", &files.key),
        ("client-cert.pem", &files.cert),
        ("client-key.pem", &files.key),
    ] {
        let bytes = std::fs::read(from)?;
        let to = std::path::Path::new(dir).join(name);
        let tmp = to.with_extension("tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&tmp, &to)?;
    }
    Ok(Tls { dir: dir.to_string(), hostname: None })
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// The target can receive: where to send.
pub fn target_ready(address: &str, port: u16) -> Value {
    json!({"targetNodeAddress": address, "targetDirectMigrationNodePorts": {port.to_string(): 0}})
}

/// The source has started sending.
pub fn sending() -> Value {
    json!({"startTimestamp": now()})
}

/// The memory arrived and the guest runs on the target.
pub fn completed() -> Value {
    json!({"completed": true, "endTimestamp": now()})
}

/// It failed, with why; `aborted` when it was an abort asked for.
pub fn failed(why: &str, aborted: bool) -> Value {
    let mut v = json!({"failed": true, "failureReason": why, "endTimestamp": now()});
    if aborted {
        v["abortStatus"] = json!("Succeeded");
    }
    v
}

/// A stormstorage disk migration, as `GET /api/v1/migrations/{id}` says.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DiskMigration {
    pub id: String,
    /// starting | copying | synced | cut_over | completed | aborted | failed
    pub state: String,
    pub message: String,
    /// T's engine-local volume id: what the target attaches.
    pub target_id: Option<String>,
    pub bytes_remaining: u64,
}

impl DiskMigration {
    pub fn of(v: &Value) -> DiskMigration {
        DiskMigration {
            id: v["id"].as_str().unwrap_or_default().to_string(),
            state: v["state"].as_str().unwrap_or_default().to_string(),
            message: v["message"].as_str().unwrap_or_default().to_string(),
            target_id: v["target"]["id"].as_str().or(v["target_local_id"].as_str()).map(str::to_string),
            bytes_remaining: v["bytes_remaining"].as_u64().unwrap_or(0),
        }
    }
}

/// stormstorage's VM-disk migration verbs (stormstorage#44).
pub struct Stormstorage<'a>(pub &'a crate::engine::EngineClient);

impl Stormstorage<'_> {
    fn url(&self, path: &str) -> String {
        format!("{}/api/v1/{path}", self.0.url())
    }

    async fn answer(r: reqwest::Result<reqwest::Response>) -> Result<Value, String> {
        let r = r.map_err(|e| format!("stormstorage: {e}"))?;
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("stormstorage {status}: {}", text.trim()));
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Start mirroring `volume` (on `source`) to a new volume on `target`.
    pub async fn start(&self, source: &str, volume: &str, target: &str) -> Result<DiskMigration, String> {
        let url = self.url(&format!("nodes/{source}/volumes/{volume}/migrate"));
        let v = Self::answer(self.0.post(&url, &json!({"to": target})).await).await?;
        let m = DiskMigration::of(&v);
        if m.id.is_empty() {
            return Err(format!("stormstorage answered no migration id: {v}"));
        }
        Ok(m)
    }

    pub async fn get(&self, id: &str) -> Result<DiskMigration, String> {
        Self::answer(self.0.get(&self.url(&format!("migrations/{id}"))).await).await.map(|v| DiskMigration::of(&v))
    }

    /// `cutover`, `complete` or `abort`.
    pub async fn step(&self, id: &str, step: &str) -> Result<DiskMigration, String> {
        let url = self.url(&format!("migrations/{id}/{step}"));
        Self::answer(self.0.post(&url, &json!({})).await).await.map(|v| DiskMigration::of(&v))
    }
}

/// The migration verbs of one machine, as the kubelet calls them:
/// `stormvm_control::Machine` in production, a fake in tests. Errors are the
/// hypervisor's words: they go on the migration as they are.
#[async_trait::async_trait]
pub trait Migrator: Send + Sync {
    /// Send to a target waiting on `to`.
    async fn send(&self, to: &str, tls: &Tls) -> Result<(), String>;
    /// Arm a machine started with `-incoming defer` to receive on `listen`.
    async fn receive(&self, listen: &str, tls: &Tls) -> Result<(), String>;
    /// How the transfer is going: qemu's word (`active`, `completed`,
    /// `failed`, `cancelled`, `none`).
    async fn progress(&self) -> Result<String, String>;
    /// Stop sending; the source keeps running.
    async fn cancel(&self) -> Result<(), String>;
    /// The machine's run state (`running`, `paused`, `inmigrate`, …).
    async fn state(&self) -> Result<String, String>;
    async fn unpause(&self) -> Result<(), String>;
}

#[async_trait::async_trait]
impl Migrator for stormvm_control::Machine {
    async fn send(&self, to: &str, tls: &Tls) -> Result<(), String> {
        self.send_migration(to, tls, tuning()).await.map_err(|e| e.to_string())
    }
    async fn receive(&self, listen: &str, tls: &Tls) -> Result<(), String> {
        self.receive_migration_tls(listen, tls, tuning()).await.map_err(|e| e.to_string())
    }
    async fn progress(&self) -> Result<String, String> {
        self.migration().await.map(|m| m.state).map_err(|e| e.to_string())
    }
    async fn cancel(&self) -> Result<(), String> {
        let sock = self.control.as_deref().ok_or("no control socket")?;
        stormvm_control::qmp::command(sock, "migrate_cancel", None).await.map(|_| ()).map_err(|e| e.to_string())
    }
    async fn state(&self) -> Result<String, String> {
        self.status().await.map(|s| s.state).map_err(|e| e.to_string())
    }
    async fn unpause(&self) -> Result<(), String> {
        stormvm_control::Machine::unpause(self).await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vmi(state: Value) -> Value {
        json!({"metadata": {"uid": "u1"}, "status": {"nodeName": "a", "migrationState": state}})
    }

    #[test]
    fn each_node_knows_its_part() {
        let mut v = vmi(json!({"migrationUid": "m1", "sourceNode": "a"}));
        assert!(matches!(role(&v, "a"), Some(Role::Source(_))), "source before a target is chosen");
        assert_eq!(role(&v, "b"), None);
        v["status"]["migrationState"]["targetNode"] = json!("b");
        assert!(matches!(role(&v, "b"), Some(Role::Target(_))));
        assert!(arriving(&v, "b") && !arriving(&v, "a") && !arriving(&v, "c"));
        v["status"]["migrationState"]["completed"] = json!(true);
        assert!(arriving(&v, "b"), "completed: the target adopts it");
        assert!(moved_away(&v, "a") && !moved_away(&v, "b"));
        v["status"]["migrationState"]["completed"] = json!(false);
        v["status"]["migrationState"]["failed"] = json!(true);
        assert!(!arriving(&v, "b"), "failed: the receiver goes");
        assert!(matches!(role(&v, "a"), Some(Role::Source(s)) if s.failed), "failed: the source aborts its mirrors");
        assert!(!moved_away(&v, "a"));
        assert_eq!(role(&vmi(Value::Null), "a"), None);
    }

    #[test]
    fn the_state_reads_upstreams_fields() {
        let s = State::of(&vmi(json!({
            "migrationUid": "m1", "sourceNode": "a", "targetNode": "b",
            "targetNodeAddress": "192.168.16.20", "targetDirectMigrationNodePorts": {"49153": 0},
            "startTimestamp": "2026-10-29T12:00:00Z", "abortRequested": true,
        })))
        .unwrap();
        assert_eq!(s.target_address.as_deref(), Some("192.168.16.20"));
        assert_eq!(s.target_port, Some(49153));
        assert!(s.started && s.abort_requested && s.active());
    }

    #[test]
    fn the_disks_record_is_this_migrations_only() {
        let disks: BTreeMap<String, String> = [("root".to_string(), "vm-1-0".to_string())].into();
        let patch = disks_patch("u1", "m1", &disks);
        let mut v = vmi(Value::Null);
        v["metadata"]["annotations"] = patch["metadata"]["annotations"].clone();
        assert_eq!(disks_of(&v, "m1"), Some(disks));
        assert_eq!(disks_of(&v, "m2"), None, "an earlier migration's record is not this one's");
    }

    #[test]
    fn what_cannot_move_yet_says_why() {
        let own = vec!["v-root".to_string()];
        let disks = vec![("root".to_string(), Some("v-root".to_string()))];
        assert_eq!(refusal("qemu", &disks, &own, false), None);
        assert!(refusal("cloud-hypervisor", &disks, &own, false).unwrap().contains("TLS"));
        assert!(refusal("qemu", &disks, &own, true).unwrap().contains("pod network"));
        let claim = vec![("data".to_string(), None)];
        assert!(refusal("qemu", &claim, &own, false).unwrap().contains("disk data"));
    }

    #[test]
    fn uris_and_patches_are_qemus_and_upstreams() {
        assert_eq!(listen_uri(49152), "tcp:0.0.0.0:49152");
        assert_eq!(send_uri("192.168.16.20", 49152), "tcp:192.168.16.20:49152");
        assert_eq!(send_uri("fd00::2", 49152), "tcp:[fd00::2]:49152");
        assert_eq!(target_ready("10.0.0.2", 49152)["targetDirectMigrationNodePorts"], json!({"49152": 0}));
        let f = failed("x", true);
        assert_eq!((f["failed"].clone(), f["abortStatus"].clone()), (json!(true), json!("Succeeded")));
        assert!(failed("x", false)["abortStatus"].is_null());
    }

    #[test]
    fn the_node_pair_is_copied_under_qemus_names() {
        let base = std::env::temp_dir().join(format!("rkn-migtls-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let file = |n: &str, body: &str| {
            let p = base.join(n);
            std::fs::write(&p, body).unwrap();
            p.to_string_lossy().into_owned()
        };
        let files = TlsFiles { ca: file("ca.crt", "CA"), cert: file("n.crt", "CERT"), key: file("n.key", "KEY") };
        let dir = base.join("tls").to_string_lossy().into_owned();
        let tls = lay_out_tls(&dir, &files).unwrap();
        assert_eq!(tls.dir, dir);
        let read = |n: &str| std::fs::read_to_string(std::path::Path::new(&dir).join(n)).unwrap();
        assert_eq!(read("ca-cert.pem"), "CA");
        assert_eq!((read("server-cert.pem"), read("client-cert.pem")), ("CERT".into(), "CERT".into()));
        assert_eq!((read("server-key.pem"), read("client-key.pem")), ("KEY".into(), "KEY".into()));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn a_disk_migration_names_the_targets_volume() {
        let m = DiskMigration::of(&json!({"id": "vm-1-0", "state": "synced",
            "target": {"node": "b", "name": "default.web-1-root", "id": "e7"}}));
        assert_eq!((m.state.as_str(), m.target_id.as_deref()), ("synced", Some("e7")));
        let config = Config::default();
        assert!(config.missing().unwrap().contains("stormstorage"));
        assert_eq!(config.port(), DEFAULT_PORT);
    }
}
