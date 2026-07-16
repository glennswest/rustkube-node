//! Standard CNI plugin invoker — the caller side of the CNI spec (what
//! libcni does for containerd/CRI-O).
//!
//! Loads the network configuration from a conf dir (`/etc/cni/net.d`),
//! executes plugin binaries from a bin dir (`/opt/cni/bin`) using the CNI
//! exec protocol (env vars + JSON on stdin/stdout), chains conflist plugins
//! threading `prevResult`, and parses the final result for the pod IP.
//!
//! This makes any spec-compliant CNI work unmodified — the default
//! deployment for rustkube nodes is Cilium (which writes
//! `05-cilium.conflist` into the conf dir), but flannel, calico, or the
//! reference plugins work the same way.
//!
//! See <https://www.cni.dev/docs/spec/>.

use crate::cni_types::{CniError, CniResult};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Default directory runtimes read network configs from.
pub const DEFAULT_CONF_DIR: &str = "/etc/cni/net.d";
/// Default directory plugin binaries are installed to.
pub const DEFAULT_BIN_DIR: &str = "/opt/cni/bin";
/// Interface name created inside the pod netns.
pub const DEFAULT_IFNAME: &str = "eth0";
/// CNI spec version we announce when a config doesn't declare one.
const FALLBACK_CNI_VERSION: &str = "1.0.0";

/// A loaded network configuration: a named list of plugin configs.
///
/// A `.conflist` maps directly; a single-plugin `.conf`/`.json` is wrapped
/// into a one-element list.
#[derive(Debug, Clone)]
pub struct NetworkConfigList {
    pub name: String,
    pub cni_version: String,
    /// Raw plugin config objects, in chain order. Kept as JSON values so any
    /// plugin's private fields (Cilium's, flannel's, …) pass through intact.
    pub plugins: Vec<Value>,
    pub source_path: PathBuf,
}

/// Identity of the pod whose network is being set up or torn down.
#[derive(Debug, Clone)]
pub struct PodNetwork {
    /// Sandbox/container ID — becomes CNI_CONTAINERID.
    pub container_id: String,
    /// Network namespace path — becomes CNI_NETNS (e.g. /run/netns/<id>).
    pub netns_path: String,
    /// Interface name inside the netns — CNI_IFNAME (normally eth0).
    pub ifname: String,
    pub pod_namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
}

impl PodNetwork {
    pub fn new(
        container_id: &str,
        netns_path: &str,
        pod_namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Self {
        Self {
            container_id: container_id.to_string(),
            netns_path: netns_path.to_string(),
            ifname: DEFAULT_IFNAME.to_string(),
            pod_namespace: pod_namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
        }
    }

    /// CNI_ARGS in the K8s convention understood by Cilium/calico/flannel.
    fn cni_args(&self) -> String {
        format!(
            "IgnoreUnknown=1;K8S_POD_NAMESPACE={};K8S_POD_NAME={};K8S_POD_INFRA_CONTAINER_ID={};K8S_POD_UID={}",
            self.pod_namespace, self.pod_name, self.container_id, self.pod_uid
        )
    }
}

/// Load the active network config: the lexicographically first
/// `.conflist`/`.conf`/`.json` file in `conf_dir` (the containerd rule, which
/// is why Cilium names its file `05-cilium.conflist`).
pub fn load_network_config(conf_dir: &Path) -> Result<NetworkConfigList, CniError> {
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(conf_dir)
        .map_err(|e| {
            CniError::NetworkNotFound(format!("cannot read CNI conf dir {conf_dir:?}: {e}"))
        })?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("conf") | Some("conflist") | Some("json")
            )
        })
        .collect();
    candidates.sort();

    let path = candidates.into_iter().next().ok_or_else(|| {
        CniError::NetworkNotFound(format!("no CNI network config found in {conf_dir:?}"))
    })?;

    let raw = std::fs::read_to_string(&path)?;
    let value: Value = serde_json::from_str(&raw)?;

    let name = value["name"].as_str().unwrap_or("cni-network").to_string();
    let cni_version = value["cniVersion"]
        .as_str()
        .unwrap_or(FALLBACK_CNI_VERSION)
        .to_string();

    let plugins = if path.extension().and_then(|e| e.to_str()) == Some("conflist") {
        value["plugins"]
            .as_array()
            .cloned()
            .ok_or_else(|| {
                CniError::NetworkNotFound(format!("{path:?} has no plugins array"))
            })?
    } else {
        // Single-plugin config file — the file itself is the plugin config.
        vec![value]
    };

    if plugins.is_empty() {
        return Err(CniError::NetworkNotFound(format!(
            "{path:?} declares an empty plugin chain"
        )));
    }

    Ok(NetworkConfigList {
        name,
        cni_version,
        plugins,
        source_path: path,
    })
}

/// Invokes standard CNI plugins for pod sandbox network setup/teardown.
pub struct CniInvoker {
    conf_dir: PathBuf,
    bin_dirs: Vec<PathBuf>,
}

impl CniInvoker {
    pub fn new(conf_dir: impl Into<PathBuf>, bin_dirs: Vec<PathBuf>) -> Self {
        Self {
            conf_dir: conf_dir.into(),
            bin_dirs,
        }
    }

    /// Invoker over the standard host paths (/etc/cni/net.d, /opt/cni/bin).
    pub fn standard() -> Self {
        Self::new(DEFAULT_CONF_DIR, vec![PathBuf::from(DEFAULT_BIN_DIR)])
    }

    /// Whether a network config is present (e.g. Cilium has written its
    /// conflist). Returns the network name for logging.
    pub fn network_ready(&self) -> Result<String, CniError> {
        load_network_config(&self.conf_dir).map(|c| c.name)
    }

    /// Set up pod networking: run the plugin chain with CNI ADD.
    /// Returns the final plugin's result (pod IP etc).
    pub async fn add(&self, pod: &PodNetwork) -> Result<CniResult, CniError> {
        // Reload per call: the config can appear/change at runtime (Cilium
        // writes its conflist once the agent is up).
        let config = load_network_config(&self.conf_dir)?;
        info!(
            "CNI ADD {}/{} via network '{}' ({:?})",
            pod.pod_namespace, pod.pod_name, config.name, config.source_path
        );

        let mut prev_result: Option<Value> = None;
        for plugin in &config.plugins {
            let output = self
                .exec_plugin("ADD", plugin, &config, pod, prev_result.take())
                .await?;
            prev_result = Some(output);
        }

        let final_result = prev_result.unwrap_or_else(|| json!({}));
        let result: CniResult = serde_json::from_value(final_result)?;
        Ok(result)
    }

    /// Tear down pod networking: run the plugin chain with CNI DEL, in
    /// reverse order. Best-effort per plugin — all plugins are attempted and
    /// the first error (if any) is returned at the end.
    pub async fn del(&self, pod: &PodNetwork) -> Result<(), CniError> {
        let config = load_network_config(&self.conf_dir)?;
        info!(
            "CNI DEL {}/{} via network '{}'",
            pod.pod_namespace, pod.pod_name, config.name
        );

        let mut first_err = None;
        for plugin in config.plugins.iter().rev() {
            if let Err(e) = self.exec_plugin("DEL", plugin, &config, pod, None).await {
                let plugin_type = plugin["type"].as_str().unwrap_or("?");
                warn!("CNI DEL failed for plugin {plugin_type}: {e}");
                first_err.get_or_insert(e);
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Execute one plugin with the CNI exec protocol.
    async fn exec_plugin(
        &self,
        command: &str,
        plugin: &Value,
        config: &NetworkConfigList,
        pod: &PodNetwork,
        prev_result: Option<Value>,
    ) -> Result<Value, CniError> {
        let plugin_type = plugin["type"].as_str().ok_or_else(|| {
            CniError::NetworkNotFound(format!("plugin config missing 'type': {plugin}"))
        })?;
        let binary = self.find_plugin_binary(plugin_type)?;

        // The plugin's stdin: its own config plus the network's name and
        // cniVersion, and the previous plugin's result when chained.
        let mut stdin_config = plugin.clone();
        stdin_config["name"] = json!(config.name);
        stdin_config["cniVersion"] = json!(config.cni_version);
        if let Some(prev) = prev_result {
            stdin_config["prevResult"] = prev;
        }
        let stdin_bytes = serde_json::to_vec(&stdin_config)?;

        let cni_path = std::env::join_paths(&self.bin_dirs)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| DEFAULT_BIN_DIR.to_string());

        debug!("exec CNI plugin {binary:?} {command} for {}", pod.container_id);

        let mut child = tokio::process::Command::new(&binary)
            .env("CNI_COMMAND", command)
            .env("CNI_CONTAINERID", &pod.container_id)
            .env("CNI_NETNS", &pod.netns_path)
            .env("CNI_IFNAME", &pod.ifname)
            .env("CNI_ARGS", pod.cni_args())
            .env("CNI_PATH", &cni_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| CniError::NetnsError(format!("spawn {binary:?}: {e}")))?;

        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child.stdin.take().expect("stdin piped");
            stdin
                .write_all(&stdin_bytes)
                .await
                .map_err(|e| CniError::NetnsError(format!("write plugin stdin: {e}")))?;
            // Drop closes the pipe so the plugin sees EOF.
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| CniError::NetnsError(format!("wait for {binary:?}: {e}")))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            // On failure plugins print a CNI error object to stdout.
            let msg = serde_json::from_str::<Value>(&stdout)
                .ok()
                .and_then(|v| v["msg"].as_str().map(String::from))
                .unwrap_or_else(|| {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    format!("{stdout}{stderr}")
                });
            return Err(CniError::NetnsError(format!(
                "plugin {plugin_type} {command} failed: {msg}"
            )));
        }

        if stdout.trim().is_empty() {
            // DEL and chained no-op plugins may print nothing.
            Ok(json!({}))
        } else {
            Ok(serde_json::from_str(&stdout)?)
        }
    }

    /// Locate a plugin binary in the configured bin dirs (CNI_PATH order).
    fn find_plugin_binary(&self, plugin_type: &str) -> Result<PathBuf, CniError> {
        for dir in &self.bin_dirs {
            let candidate = dir.join(plugin_type);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
        Err(CniError::NetworkNotFound(format!(
            "CNI plugin binary '{plugin_type}' not found in {:?}",
            self.bin_dirs
        )))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Write an executable fake plugin script into `bin_dir`. The script
    /// records its CNI_* env and stdin under `<bin_dir>/out/` and prints
    /// `result` on stdout.
    fn write_fake_plugin(bin_dir: &Path, name: &str, result: &str) {
        let out_dir = bin_dir.join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let script = format!(
            r#"#!/bin/sh
out="$(dirname "$0")/out"
n=$(ls "$out" | grep -c '\.env$')
env | grep '^CNI_' | sort > "$out/$n.{name}.$CNI_COMMAND.env"
cat > "$out/$n.{name}.$CNI_COMMAND.stdin"
printf '%s' '{result}'
"#
        );
        let path = bin_dir.join(name);
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn recorded(bin_dir: &Path, suffix: &str) -> Vec<(String, String)> {
        let out = bin_dir.join("out");
        let mut files: Vec<PathBuf> = std::fs::read_dir(out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().ends_with(suffix))
            .collect();
        files.sort();
        files
            .into_iter()
            .map(|p| {
                (
                    p.file_name().unwrap().to_string_lossy().into_owned(),
                    std::fs::read_to_string(&p).unwrap(),
                )
            })
            .collect()
    }

    fn pod() -> PodNetwork {
        PodNetwork::new("sb-123", "/run/netns/sb-123", "default", "web", "uid-1")
    }

    #[test]
    fn loads_first_conflist_lexicographically() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("99-other.conf"),
            r#"{"cniVersion":"1.0.0","name":"other","type":"bridge"}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("05-cilium.conflist"),
            r#"{"cniVersion":"1.0.0","name":"cilium","plugins":[{"type":"cilium-cni"}]}"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("README.txt"), "ignored").unwrap();

        let config = load_network_config(dir.path()).unwrap();
        assert_eq!(config.name, "cilium");
        assert_eq!(config.plugins.len(), 1);
        assert_eq!(config.plugins[0]["type"], "cilium-cni");
    }

    #[test]
    fn wraps_single_conf_as_chain_of_one() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("10-net.conf"),
            r#"{"cniVersion":"0.4.0","name":"mynet","type":"bridge","bridge":"cni0"}"#,
        )
        .unwrap();

        let config = load_network_config(dir.path()).unwrap();
        assert_eq!(config.name, "mynet");
        assert_eq!(config.cni_version, "0.4.0");
        assert_eq!(config.plugins.len(), 1);
        assert_eq!(config.plugins[0]["bridge"], "cni0");
    }

    #[test]
    fn missing_config_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_network_config(dir.path()).is_err());
    }

    #[tokio::test]
    async fn add_invokes_plugin_with_cni_env_and_returns_ip() {
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-fake.conflist"),
            r#"{"cniVersion":"1.0.0","name":"fakenet","plugins":[{"type":"fakeplugin","mtu":1500}]}"#,
        )
        .unwrap();
        write_fake_plugin(
            bin.path(),
            "fakeplugin",
            r#"{"cniVersion":"1.0.0","ips":[{"address":"10.42.0.7/24","gateway":"10.42.0.1"}]}"#,
        );

        let invoker = CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        let result = invoker.add(&pod()).await.unwrap();

        assert_eq!(result.ips.len(), 1);
        assert_eq!(result.ips[0].address, "10.42.0.7/24");

        let envs = recorded(bin.path(), ".env");
        assert_eq!(envs.len(), 1);
        let env = &envs[0].1;
        assert!(env.contains("CNI_COMMAND=ADD"));
        assert!(env.contains("CNI_CONTAINERID=sb-123"));
        assert!(env.contains("CNI_NETNS=/run/netns/sb-123"));
        assert!(env.contains("CNI_IFNAME=eth0"));
        assert!(env.contains("K8S_POD_NAMESPACE=default"));
        assert!(env.contains("K8S_POD_NAME=web"));
        assert!(env.contains("K8S_POD_UID=uid-1"));

        // Stdin carries the plugin config plus injected name/cniVersion.
        let stdins = recorded(bin.path(), ".stdin");
        let stdin: Value = serde_json::from_str(&stdins[0].1).unwrap();
        assert_eq!(stdin["name"], "fakenet");
        assert_eq!(stdin["cniVersion"], "1.0.0");
        assert_eq!(stdin["mtu"], 1500);
        assert!(stdin["prevResult"].is_null());
    }

    #[tokio::test]
    async fn chained_plugins_receive_prev_result() {
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-chain.conflist"),
            r#"{"cniVersion":"1.0.0","name":"chained","plugins":[{"type":"fake-a"},{"type":"fake-b"}]}"#,
        )
        .unwrap();
        write_fake_plugin(
            bin.path(),
            "fake-a",
            r#"{"cniVersion":"1.0.0","ips":[{"address":"10.42.0.9/24","gateway":null}]}"#,
        );
        write_fake_plugin(
            bin.path(),
            "fake-b",
            r#"{"cniVersion":"1.0.0","ips":[{"address":"10.42.0.9/24","gateway":null}]}"#,
        );

        let invoker = CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        let result = invoker.add(&pod()).await.unwrap();
        assert_eq!(result.ips[0].address, "10.42.0.9/24");

        let stdins = recorded(bin.path(), ".stdin");
        assert_eq!(stdins.len(), 2);
        // First plugin: no prevResult. Second: prevResult = first's output.
        let first: Value = serde_json::from_str(&stdins[0].1).unwrap();
        let second: Value = serde_json::from_str(&stdins[1].1).unwrap();
        assert!(first["prevResult"].is_null());
        assert_eq!(second["prevResult"]["ips"][0]["address"], "10.42.0.9/24");
        // ADD order: fake-a then fake-b.
        assert!(stdins[0].0.contains("fake-a"));
        assert!(stdins[1].0.contains("fake-b"));
    }

    #[tokio::test]
    async fn del_runs_chain_in_reverse() {
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-chain.conflist"),
            r#"{"cniVersion":"1.0.0","name":"chained","plugins":[{"type":"fake-a"},{"type":"fake-b"}]}"#,
        )
        .unwrap();
        write_fake_plugin(bin.path(), "fake-a", "");
        write_fake_plugin(bin.path(), "fake-b", "");

        let invoker = CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        invoker.del(&pod()).await.unwrap();

        let envs = recorded(bin.path(), ".env");
        assert_eq!(envs.len(), 2);
        // DEL order: fake-b first, then fake-a.
        assert!(envs[0].0.contains("fake-b"));
        assert!(envs[0].1.contains("CNI_COMMAND=DEL"));
        assert!(envs[1].0.contains("fake-a"));
    }

    #[tokio::test]
    async fn failing_plugin_surfaces_cni_error_msg() {
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-bad.conf"),
            r#"{"cniVersion":"1.0.0","name":"bad","type":"badplugin"}"#,
        )
        .unwrap();
        let script = r#"#!/bin/sh
cat > /dev/null
printf '%s' '{"cniVersion":"1.0.0","code":7,"msg":"no IP ranges available"}'
exit 1
"#;
        let path = bin.path().join("badplugin");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let invoker = CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        let err = invoker.add(&pod()).await.unwrap_err();
        assert!(err.to_string().contains("no IP ranges available"));
    }

    #[tokio::test]
    async fn missing_binary_is_a_clear_error() {
        let conf = tempfile::tempdir().unwrap();
        let bin = tempfile::tempdir().unwrap();
        std::fs::write(
            conf.path().join("10-net.conf"),
            r#"{"cniVersion":"1.0.0","name":"net","type":"cilium-cni"}"#,
        )
        .unwrap();

        let invoker = CniInvoker::new(conf.path(), vec![bin.path().to_path_buf()]);
        let err = invoker.add(&pod()).await.unwrap_err();
        assert!(err.to_string().contains("cilium-cni"));
    }
}
