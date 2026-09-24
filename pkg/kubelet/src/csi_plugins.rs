//! CSI driver registration: which external storage drivers this node has.
//!
//! A driver's node plugin runs as a pod (usually a DaemonSet) with the
//! `node-driver-registrar` sidecar, which puts a socket in
//! `/var/lib/kubelet/plugins_registry` and serves the kubelet
//! plugin-registration API on it (`proto/pluginregistration/api.proto`). The
//! upstream protocol, which this follows:
//!
//! 1. the kubelet finds the socket and calls `GetInfo`. The answer is the
//!    plugin's type (`CSIPlugin`), its driver name, the endpoint of the driver
//!    itself (`/var/lib/kubelet/plugins/<driver>/csi.sock`), and the CSI
//!    versions it speaks;
//! 2. the kubelet calls the driver's `NodeGetInfo` for its node ID, volume limit
//!    and topology, and records them where the control plane reads them: the
//!    node's `CSINode` object (the external-attacher takes the node ID from
//!    it) and the topology segments as node labels;
//! 3. `NotifyRegistrationStatus` tells the registrar whether it worked. The
//!    registrar reports that in its own health check, which is where somebody
//!    looking at the driver pod finds it.
//!
//! When the socket goes, the driver is dropped from this node and from
//! `CSINode`.
//!
//! **Polled, not watched.** A scan of one directory every two seconds costs
//! nothing, and a driver arriving two seconds late does not matter. A missed
//! inotify event would leave a driver unregistered until the kubelet restarts,
//! and nobody would think to look there.

use crate::csi::{self, CsiDriverClient, NodeCapabilities, NodeInfo};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// The generated plugin-registration messages and service.
pub mod registration_proto {
    tonic::include_proto!("pluginregistration");
}

use registration_proto::registration_client::RegistrationClient;

/// Where registrars put their sockets.
pub const PLUGINS_REGISTRY: &str = "/var/lib/kubelet/plugins_registry";

/// How long a socket that failed to register waits before it is tried again.
const RETRY_FAILED: Duration = Duration::from_secs(30);

/// Where the kubelet's view of the host root is, when it runs in a container.
/// Same meaning as in `pod_manager.rs`.
const HOST_ROOT: &str = "/hostroot";

/// A driver this node can call.
#[derive(Clone)]
pub struct Registered {
    pub name: String,
    pub client: CsiDriverClient,
    pub node: NodeInfo,
    pub caps: NodeCapabilities,
    /// The registration socket it arrived through. When that goes, so
    /// does the driver.
    pub registration: PathBuf,
}

/// The node's registered CSI drivers, and the loop that keeps them current.
pub struct CsiPlugins {
    node_name: String,
    registry_dir: PathBuf,
    api: Option<(reqwest::Client, String)>,
    drivers: RwLock<HashMap<String, Registered>>,
    /// Sockets that failed, and when, so a broken registrar is retried every
    /// [`RETRY_FAILED`] and not logged every two seconds. A registrar that
    /// recreates its socket (a new mtime) is tried at once. The common failure
    /// is a registrar that is up before its driver, and that one has to be
    /// retried: dropping it would leave the driver unregistered until the kubelet restarts.
    failed: RwLock<HashMap<PathBuf, (std::time::SystemTime, std::time::Instant)>>,
    /// The driver set last written to CSINode, or `None` when that write has
    /// not landed yet. The write is retried until it does.
    published: RwLock<Option<BTreeMap<String, Value>>>,
}

impl CsiPlugins {
    pub fn new(node_name: &str) -> Self {
        Self {
            node_name: node_name.to_string(),
            registry_dir: PathBuf::from(PLUGINS_REGISTRY),
            api: None,
            drivers: RwLock::new(HashMap::new()),
            failed: RwLock::new(HashMap::new()),
            published: RwLock::new(None),
        }
    }

    /// Record registrations in the API (`CSINode`, node labels). Without it,
    /// drivers are registered locally only, which is what tests want.
    pub fn with_api(mut self, client: reqwest::Client, api_url: &str) -> Self {
        if !api_url.is_empty() {
            self.api = Some((client, api_url.trim_end_matches('/').to_string()));
        }
        self
    }

    pub fn with_registry_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.registry_dir = dir.into();
        self
    }

    /// The driver, if it is registered on this node.
    pub async fn get(&self, driver: &str) -> Option<Registered> {
        self.drivers.read().await.get(driver).cloned()
    }

    pub async fn names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.drivers.read().await.keys().cloned().collect();
        v.sort();
        v
    }

    /// Scan for ever.
    pub async fn run(self: Arc<Self>) {
        info!("CSI plugin registration: watching {}", self.registry_dir.display());
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            self.scan_once().await;
        }
    }

    /// One pass: register new sockets, drop vanished ones, and bring CSINode
    /// up to date.
    pub async fn scan_once(&self) {
        let sockets = list_sockets(&self.registry_dir);

        // Gone: the registrar's socket was removed, so the plugin was.
        let gone: Vec<String> = self
            .drivers
            .read()
            .await
            .values()
            .filter(|r| !sockets.iter().any(|(p, _)| p == &r.registration))
            .map(|r| r.name.clone())
            .collect();
        for name in gone {
            info!("CSI driver {name}: registration socket removed, deregistered");
            self.drivers.write().await.remove(&name);
        }
        self.failed.write().await.retain(|p, _| sockets.iter().any(|(s, _)| s == p));

        for (sock, mtime) in &sockets {
            let known = self.drivers.read().await.values().any(|r| &r.registration == sock);
            let backing_off = self
                .failed
                .read()
                .await
                .get(sock)
                .is_some_and(|(m, at)| m == mtime && at.elapsed() < RETRY_FAILED);
            if known || backing_off {
                continue;
            }
            match self.register(sock).await {
                Ok(name) => {
                    self.failed.write().await.remove(sock);
                    info!("CSI driver {name} registered from {}", sock.display());
                }
                Err(e) => {
                    warn!("CSI plugin at {}: not registered: {e:#}", sock.display());
                    self.failed
                        .write()
                        .await
                        .insert(sock.clone(), (*mtime, std::time::Instant::now()));
                }
            }
        }

        self.publish_csinode().await;
    }

    /// Register the plugin behind one registration socket. The driver's name on
    /// success.
    async fn register(&self, sock: &Path) -> anyhow::Result<String> {
        let mut reg = RegistrationClient::new(csi::unix_channel(sock, Duration::from_secs(10)));
        let info = reg
            .get_info(registration_proto::InfoRequest {})
            .await
            .map_err(|s| anyhow::anyhow!("GetInfo: {:?}: {}", s.code(), s.message()))?
            .into_inner();

        let result = self.admit(&info).await;
        // Tell the registrar either way. It shows the outcome in its own
        // health check, which is where somebody debugging the driver looks.
        let status = registration_proto::RegistrationStatus {
            plugin_registered: result.is_ok(),
            error: result.as_ref().err().map(|e| format!("{e:#}")).unwrap_or_default(),
        };
        if let Err(s) = reg.notify_registration_status(status).await {
            debug!("NotifyRegistrationStatus to {}: {}", sock.display(), s.message());
        }
        let registered = result?;
        let name = registered.name.clone();
        self.drivers.write().await.insert(
            name.clone(),
            Registered { registration: sock.to_path_buf(), ..registered },
        );
        Ok(name)
    }

    /// Check what the registrar said, and ask the driver about this node.
    async fn admit(
        &self,
        info: &registration_proto::PluginInfo,
    ) -> anyhow::Result<Registered> {
        if info.r#type != "CSIPlugin" {
            anyhow::bail!(
                "plugin type {:?} is not supported here (only CSIPlugin; device plugins are not)",
                info.r#type
            );
        }
        if info.name.is_empty() {
            anyhow::bail!("the registrar gave no driver name");
        }
        // CSI 1.x. Upstream accepts any version with major 1 and so does this.
        // A 0.x driver speaks a different protocol under the same method names.
        if !info.supported_versions.iter().any(|v| v.trim_start_matches('v').starts_with("1.")) {
            anyhow::bail!(
                "driver {} supports CSI {:?}, and this kubelet speaks 1.x",
                info.name,
                info.supported_versions
            );
        }
        let endpoint = resolve_endpoint(&info.endpoint)
            .ok_or_else(|| anyhow::anyhow!("driver {} gave no endpoint", info.name))?;
        let client = CsiDriverClient::new(&endpoint);
        let node = client.node_info().await?;
        let caps = client.node_capabilities().await?;
        Ok(Registered {
            name: info.name.clone(),
            client,
            node,
            caps,
            registration: PathBuf::new(),
        })
    }

    /// Make `CSINode` list exactly the drivers registered here, and label
    /// the node with their topology. Retried every scan until it lands, and
    /// written only when the set changed.
    async fn publish_csinode(&self) {
        let Some((client, url)) = &self.api else { return };
        let want: BTreeMap<String, Value> = self
            .drivers
            .read()
            .await
            .values()
            .map(|r| (r.name.clone(), csinode_driver(r)))
            .collect();
        if self.published.read().await.as_ref() == Some(&want) {
            return;
        }

        // Topology first. A provisioner reading CSINode's topologyKeys
        // expects the node to carry them as labels.
        let labels: serde_json::Map<String, Value> = self
            .drivers
            .read()
            .await
            .values()
            .flat_map(|r| r.node.topology.iter().map(|(k, v)| (k.clone(), json!(v))))
            .collect();
        if !labels.is_empty() {
            let r = client
                .patch(format!("{url}/api/v1/nodes/{}", self.node_name))
                .header("content-type", "application/merge-patch+json")
                .json(&json!({"metadata": {"labels": labels}}))
                .send()
                .await;
            if !matches!(&r, Ok(r) if r.status().is_success()) {
                debug!("CSI topology labels not written yet: {:?}", r.map(|r| r.status()));
                return;
            }
        }

        match write_csinode(client, url, &self.node_name, want.values().cloned().collect()).await {
            Ok(()) => {
                info!("CSINode {}: drivers {:?}", self.node_name, want.keys().collect::<Vec<_>>());
                *self.published.write().await = Some(want);
            }
            Err(e) => debug!("CSINode {} not written yet: {e}", self.node_name),
        }
    }
}

/// One `CSINode.spec.drivers` entry.
fn csinode_driver(r: &Registered) -> Value {
    let mut keys: Vec<&String> = r.node.topology.keys().collect();
    keys.sort();
    let mut d = json!({
        "name": r.name,
        "nodeID": r.node.node_id,
        "topologyKeys": keys,
    });
    if r.node.max_volumes_per_node > 0 {
        d["allocatable"] = json!({"count": r.node.max_volumes_per_node});
    }
    d
}

/// Create or replace this node's CSINode with `drivers`.
///
/// Replaced whole rather than merged: after a kubelet restart the object
/// still lists the drivers of the last run, and a driver that did not come
/// back must not stay listed, because the attacher would keep publishing
/// volumes to a node that cannot mount them.
async fn write_csinode(
    client: &reqwest::Client,
    url: &str,
    node: &str,
    drivers: Vec<Value>,
) -> Result<(), String> {
    let path = format!("{url}/apis/storage.k8s.io/v1/csinodes/{node}");
    let existing = client.get(&path).send().await.map_err(|e| e.to_string())?;
    let status = existing.status();
    if status.is_success() {
        let mut obj: Value = existing.json().await.map_err(|e| e.to_string())?;
        obj["spec"]["drivers"] = json!(drivers);
        let r = client.put(&path).json(&obj).send().await.map_err(|e| e.to_string())?;
        return if r.status().is_success() { Ok(()) } else { Err(format!("PUT {}", r.status())) };
    }
    if status.as_u16() != 404 {
        return Err(format!("GET {status}"));
    }
    // Owned by the Node, as upstream does, so deleting the node deletes it.
    let owner = match client.get(format!("{url}/api/v1/nodes/{node}")).send().await {
        Ok(r) if r.status().is_success() => r
            .json::<Value>()
            .await
            .ok()
            .and_then(|n| n["metadata"]["uid"].as_str().map(String::from)),
        _ => None,
    };
    let mut obj = json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "CSINode",
        "metadata": {"name": node},
        "spec": {"drivers": drivers},
    });
    if let Some(uid) = owner {
        obj["metadata"]["ownerReferences"] =
            json!([{"apiVersion": "v1", "kind": "Node", "name": node, "uid": uid}]);
    }
    let r = client
        .post(format!("{url}/apis/storage.k8s.io/v1/csinodes"))
        .json(&obj)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if r.status().is_success() {
        Ok(())
    } else {
        Err(format!("POST {}", r.status()))
    }
}

/// The sockets in the registry directory, with their mtimes.
///
/// Anything that is a socket counts, whatever its name. Upstream skips names
/// starting with `.`, and so does this. The directory not existing is the
/// normal state of a node with no drivers.
fn list_sockets(dir: &Path) -> Vec<(PathBuf, std::time::SystemTime)> {
    use std::os::unix::fs::FileTypeExt;
    let Ok(rd) = std::fs::read_dir(dir) else { return Vec::new() };
    let mut v: Vec<_> = rd
        .flatten()
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .filter_map(|e| {
            let md = e.metadata().ok()?;
            md.file_type()
                .is_socket()
                .then(|| (e.path(), md.modified().unwrap_or(std::time::UNIX_EPOCH)))
        })
        .collect();
    v.sort();
    v
}

/// The driver's endpoint as a path this process can open.
///
/// Registrars give a host path (`/var/lib/kubelet/plugins/<driver>/csi.sock`),
/// and some prefix it with `unix://`. `/var/lib/kubelet` is bound into the
/// kubelet at the same path. Anything else is reached through the host root,
/// when the kubelet is containerised.
fn resolve_endpoint(endpoint: &str) -> Option<PathBuf> {
    let p = endpoint.strip_prefix("unix://").unwrap_or(endpoint);
    if p.is_empty() {
        return None;
    }
    let direct = PathBuf::from(p);
    if direct.exists() {
        return Some(direct);
    }
    let via_host = Path::new(HOST_ROOT).join(p.trim_start_matches('/'));
    Some(if via_host.exists() { via_host } else { direct })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A tokio test only because a client's lazy channel spawns its worker.
    #[tokio::test]
    async fn a_csinode_entry_carries_the_drivers_node_id_and_topology() {
        let r = Registered {
            name: "hostpath.csi.k8s.io".into(),
            client: CsiDriverClient::new(Path::new("/nonexistent")),
            node: NodeInfo {
                node_id: "n1-id".into(),
                max_volumes_per_node: 0,
                topology: HashMap::from([("topology.hostpath.csi/node".into(), "n1".into())]),
            },
            caps: NodeCapabilities::default(),
            registration: PathBuf::new(),
        };
        let d = csinode_driver(&r);
        assert_eq!(d["name"], "hostpath.csi.k8s.io");
        assert_eq!(d["nodeID"], "n1-id");
        assert_eq!(d["topologyKeys"], json!(["topology.hostpath.csi/node"]));
        // No limit is no allocatable, not a limit of zero volumes.
        assert!(d.get("allocatable").is_none());
    }

    #[test]
    fn endpoints_accept_the_unix_scheme() {
        assert_eq!(resolve_endpoint(""), None);
        assert_eq!(
            resolve_endpoint("unix:///var/lib/kubelet/plugins/x/csi.sock").map(|p| p.starts_with("/")),
            Some(true)
        );
    }

    #[test]
    fn only_sockets_are_registrations() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("not-a-socket.sock"), b"").unwrap();
        let _l = std::os::unix::net::UnixListener::bind(dir.path().join("d-reg.sock")).unwrap();
        let _h = std::os::unix::net::UnixListener::bind(dir.path().join(".hidden.sock")).unwrap();
        let found = list_sockets(dir.path());
        assert_eq!(found.len(), 1);
        assert!(found[0].0.ends_with("d-reg.sock"));
        assert!(list_sockets(Path::new("/nonexistent/registry")).is_empty());
    }
}
