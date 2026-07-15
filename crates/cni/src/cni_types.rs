//! CNI specification types.
//!
//! Implements the CNI spec v1.0 input/output formats.
//! See https://www.cni.dev/docs/spec/

use serde::{Deserialize, Serialize};

/// CNI error type.
#[derive(Debug, thiserror::Error)]
pub enum CniError {
    #[error("incompatible CNI version: {0}")]
    IncompatibleVersion(String),

    #[error("network not found: {0}")]
    NetworkNotFound(String),

    #[error("IPAM allocation failed: {0}")]
    IpamError(String),

    #[error("bridge setup failed: {0}")]
    BridgeError(String),

    #[error("VXLAN setup failed: {0}")]
    VxlanError(String),

    #[error("netns error: {0}")]
    NetnsError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// CNI network configuration (input from stdin).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CniConfig {
    pub cni_version: String,
    pub name: String,
    #[serde(rename = "type")]
    pub plugin_type: String,
    #[serde(default)]
    pub bridge: String,
    #[serde(default)]
    pub is_gateway: bool,
    #[serde(default)]
    pub ip_masq: bool,
    #[serde(default)]
    pub hairpin_mode: bool,
    #[serde(default)]
    pub mtu: u32,
    #[serde(default)]
    pub ipam: IpamConfig,
    #[serde(default)]
    pub vxlan: Option<VxlanConfig>,
}

/// IPAM configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpamConfig {
    #[serde(rename = "type")]
    pub ipam_type: String,
    #[serde(default)]
    pub subnet: String,
    #[serde(default)]
    pub range_start: String,
    #[serde(default)]
    pub range_end: String,
    #[serde(default)]
    pub gateway: String,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub data_dir: String,
}

/// VXLAN overlay configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VxlanConfig {
    pub vni: u32,
    pub port: u16,
    #[serde(default = "default_vtep_name")]
    pub vtep_dev_name: String,
}

fn default_vtep_name() -> String {
    "rk-vxlan0".to_string()
}

/// Route configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteConfig {
    pub dst: String,
    #[serde(default)]
    pub gw: String,
}

/// CNI result (output to stdout).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CniResult {
    pub cni_version: String,
    pub interfaces: Vec<CniInterface>,
    pub ips: Vec<CniIpConfig>,
    #[serde(default)]
    pub routes: Vec<RouteConfig>,
    #[serde(default)]
    pub dns: CniDns,
}

/// CNI interface info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniInterface {
    pub name: String,
    #[serde(default)]
    pub mac: String,
    #[serde(default)]
    pub sandbox: String,
}

/// CNI IP configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniIpConfig {
    pub address: String,
    pub gateway: Option<String>,
    #[serde(default)]
    pub interface: Option<usize>,
}

/// CNI DNS configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CniDns {
    #[serde(default)]
    pub nameservers: Vec<String>,
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub search: Vec<String>,
    #[serde(default)]
    pub options: Vec<String>,
}

/// CNI error result (output to stderr).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CniErrorResult {
    pub cni_version: String,
    pub code: u32,
    pub msg: String,
    #[serde(default)]
    pub details: String,
}

impl CniErrorResult {
    pub fn new(version: &str, code: u32, msg: &str) -> Self {
        Self {
            cni_version: version.to_string(),
            code,
            msg: msg.to_string(),
            details: String::new(),
        }
    }
}
