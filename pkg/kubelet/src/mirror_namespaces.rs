//! Which namespace each node service's mirror pod lives in (#418, stormcos#1020).
//!
//! Owner (stormcos#1020): each node service in its own project, as
//! OpenShift's `openshift-<component>`: `storm-block`, `storm-drive`, …, with
//! `kube-system` for the control plane alone. stormcos ships the map in the
//! rustkube-node golden as `/etc/kubernetes/mirror-namespaces.json`, a flat
//! JSON object from a PID 1 asset name to its namespace:
//! `{"stormblock": "storm-block", "rustkube-apiserver": "kube-system", …}`.
//!
//! An asset the map does not name, an absent file, or one that does not
//! parse keeps [`FALLBACK`]. The file is read again whenever its modification
//! time or size changes, so a new golden's map applies without a restart.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use tracing::{info, warn};

/// Where stormcos puts the map.
pub const DEFAULT_PATH: &str = "/etc/kubernetes/mirror-namespaces.json";

/// The namespace of an asset the map does not name.
pub const FALLBACK: &str = "kube-system";

/// The map, parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Namespaces(HashMap<String, String>);

impl Namespaces {
    /// From the file's text. Entries whose value is not a valid namespace
    /// name are left out (the asset then keeps [`FALLBACK`]).
    pub fn parse(text: &str) -> Result<Self, String> {
        let v: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let obj = v.as_object().ok_or("not a JSON object")?;
        let mut map = HashMap::new();
        for (asset, ns) in obj {
            match ns.as_str().filter(|n| valid_namespace(n)) {
                Some(n) => {
                    map.insert(asset.clone(), n.to_string());
                }
                None => warn!("mirror namespaces: {asset}: {ns} is not a namespace name; it stays in {FALLBACK}"),
            }
        }
        Ok(Self(map))
    }

    /// The namespace of `asset`'s mirror pod.
    pub fn of(&self, asset: &str) -> &str {
        self.0.get(asset).map(String::as_str).unwrap_or(FALLBACK)
    }

    /// From a list of pairs (tests, callers that know the map).
    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self(pairs.into_iter().map(|(a, n)| (a.to_string(), n.to_string())).collect())
    }
}

/// An RFC 1123 label, as a Namespace's name must be.
fn valid_namespace(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 63
        && n.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !n.starts_with('-')
        && !n.ends_with('-')
}

struct Cache {
    path: String,
    stamp: Option<(SystemTime, u64)>,
    map: Namespaces,
}

fn cache() -> &'static Mutex<Cache> {
    static C: OnceLock<Mutex<Cache>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Cache { path: DEFAULT_PATH.to_string(), stamp: None, map: Namespaces::default() }))
}

/// Use this file (`--mirror-namespaces`). Set once at start.
pub fn set_path(path: &str) {
    let mut c = cache().lock().unwrap_or_else(|e| e.into_inner());
    c.path = path.to_string();
    c.stamp = None;
    c.map = Namespaces::default();
}

/// The file in use.
pub fn path() -> String {
    cache().lock().unwrap_or_else(|e| e.into_inner()).path.clone()
}

/// The map as the file says it now: read again when its mtime or size moved.
pub fn current() -> Namespaces {
    let mut c = cache().lock().unwrap_or_else(|e| e.into_inner());
    let stamp = std::fs::metadata(&c.path).ok().map(|m| (m.modified().unwrap_or(SystemTime::UNIX_EPOCH), m.len()));
    if stamp != c.stamp {
        c.map = match stamp {
            None => Namespaces::default(),
            Some(_) => match std::fs::read_to_string(&c.path).map_err(|e| e.to_string()).and_then(|t| Namespaces::parse(&t)) {
                Ok(m) => {
                    info!("mirror namespaces: {} names {} node services", c.path, m.0.len());
                    m
                }
                Err(e) => {
                    warn!("mirror namespaces: {}: {e}; every node service stays in {FALLBACK}", c.path);
                    Namespaces::default()
                }
            },
        };
        c.stamp = stamp;
    }
    c.map.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_asset_goes_to_its_namespace_and_the_rest_to_kube_system() {
        let m = Namespaces::parse(r#"{"stormblock": "storm-block", "rustkube-apiserver": "kube-system", "bad": "Not_A_Name", "n": 3}"#)
            .unwrap();
        assert_eq!(m.of("stormblock"), "storm-block");
        assert_eq!(m.of("rustkube-apiserver"), "kube-system");
        assert_eq!(m.of("bad"), FALLBACK, "an invalid name is left out");
        assert_eq!(m.of("n"), FALLBACK);
        assert_eq!(m.of("sshd"), FALLBACK, "not in the map");
        assert!(Namespaces::parse("[1]").is_err());
        assert!(Namespaces::parse("{").is_err());
    }

    #[test]
    fn the_file_is_read_again_when_it_changes() {
        let dir = std::env::temp_dir().join(format!("mirror-ns-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("mirror-namespaces.json");
        set_path(file.to_str().unwrap());
        assert_eq!(current().of("stormblock"), FALLBACK, "no file");
        std::fs::write(&file, r#"{"stormblock": "storm-block"}"#).unwrap();
        assert_eq!(current().of("stormblock"), "storm-block");
        std::fs::write(&file, r#"{"stormblock": "storm-blocks", "cadvisor": "storm-cadvisor"}"#).unwrap();
        assert_eq!(current().of("cadvisor"), "storm-cadvisor", "a changed size is a new read");
        std::fs::write(&file, "{").unwrap();
        assert_eq!(current().of("stormblock"), FALLBACK, "unparsable");
        std::fs::remove_file(&file).unwrap();
        assert_eq!(current().of("cadvisor"), FALLBACK);
        set_path(DEFAULT_PATH);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
