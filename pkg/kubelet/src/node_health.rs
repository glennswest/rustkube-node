//! Readiness for the node's own services (#96).
//!
//! A mirror pod (`mirror.rs`) said `Running` and `Ready` whenever PID 1 had
//! the service's process up, so a service up as a process and serving nothing
//! (stormstorage on 11.50: `:9093` and `:9193` refused) looked healthy to
//! everything that reads pod status: the console, test runs, a release gate.
//!
//! Each service already declares how it is checked. Its golden's stormd config
//! (`/etc/stormd/config.toml`, stormcos `service_golden`) carries
//!
//! ```toml
//! [process.liveness]
//! type = "http"
//! url = "http://127.0.0.1:9093/api/v1/health"
//! ```
//!
//! which stormd itself probes to restart it. The kubelet reads the same URL,
//! found through the boot unit that runs the service (its `root` volume is the
//! golden, seen under `/hostroot`), and asks it on its own clock. Only for a
//! service on the host network (`profile host`): its `127.0.0.1` is the
//! node's, and so the kubelet's.
//!
//! [`FAILURES`] failures in a row make the service not ready; one answer makes
//! it ready again. Not ready is a readiness answer, not a crash: the pod stays
//! `Running`, its container `ready: false`, `Ready=False` with the reason, as
//! upstream reports a running container that fails its readiness probe.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Failures in a row before a service is not ready: stormd's own
/// `failure_threshold`.
pub const FAILURES: u32 = 3;
/// How often each service is asked.
pub const PERIOD: Duration = Duration::from_secs(10);
/// How long one ask may take.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// Each host-network service's liveness URL, by asset (spec) name, from the
/// boot units under `root` and the stormd config in each one's golden.
pub fn health_urls(root: &Path) -> HashMap<String, String> {
    let mut volumes: HashMap<String, String> = HashMap::new();
    let mut specs: Vec<(String, Option<String>, bool)> = Vec::new();
    let dir = root.join("etc/stormpump/boot.d");
    let mut units: Vec<_> = std::fs::read_dir(&dir)
        .map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect())
        .unwrap_or_default();
    units.sort();
    for unit in units {
        let Ok(text) = std::fs::read_to_string(&unit) else { continue };
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("");
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["volume", name, path, ..] => {
                    volumes.insert(name.to_string(), path.to_string());
                }
                ["spec", name, ..] => specs.push((name.to_string(), None, false)),
                ["root", volume, ..] if line.starts_with(char::is_whitespace) => {
                    if let Some(s) = specs.last_mut() {
                        s.1 = Some(volume.to_string());
                    }
                }
                ["profile", "host", ..] if line.starts_with(char::is_whitespace) => {
                    if let Some(s) = specs.last_mut() {
                        s.2 = true;
                    }
                }
                _ => {}
            }
        }
    }
    let mut out = HashMap::new();
    for (name, root_volume, host) in specs {
        let Some(path) = root_volume.and_then(|v| volumes.get(&v).cloned()) else { continue };
        if !host {
            continue;
        }
        let config = root.join(path.trim_start_matches('/')).join("etc/stormd/config.toml");
        if let Some(url) = std::fs::read_to_string(config).ok().and_then(|t| liveness_url(&t)) {
            out.insert(name, url);
        }
    }
    out
}

/// The first `[process.liveness]` of `type = "http"`'s `url`, from a stormd
/// config. Read by line: the two keys are all that is wanted.
pub fn liveness_url(config: &str) -> Option<String> {
    let mut in_liveness = false;
    let (mut kind, mut url) = (None::<String>, None::<String>);
    let value = |l: &str| l.split_once('=').map(|(_, v)| v.trim().trim_matches('"').to_string());
    for line in config.lines().map(str::trim) {
        if line.starts_with('[') {
            if in_liveness && kind.as_deref() == Some("http") && url.is_some() {
                return url;
            }
            in_liveness = line == "[process.liveness]";
            kind = None;
            url = None;
            continue;
        }
        if !in_liveness {
            continue;
        }
        if line.starts_with("type") {
            kind = value(line);
        } else if line.starts_with("url") {
            url = value(line);
        }
    }
    (in_liveness && kind.as_deref() == Some("http")).then_some(url).flatten()
}

/// What the probes say of one service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceHealth {
    pub ready: bool,
    pub failures: u32,
    /// Why it is not ready: the last failure.
    pub reason: String,
}

/// Fold one probe result into a service's health; `true` when its
/// readiness changed.
pub fn observe(h: &mut ServiceHealth, result: Result<(), String>) -> bool {
    let was = h.ready;
    match result {
        Ok(()) => {
            h.failures = 0;
            h.reason.clear();
            h.ready = true;
        }
        Err(e) => {
            h.failures = h.failures.saturating_add(1);
            h.reason = e;
            if h.failures >= FAILURES {
                h.ready = false;
            }
        }
    }
    h.ready != was
}

/// Ask `url` once: any 2xx is an answer.
pub async fn probe(client: &reqwest::Client, url: &str) -> Result<(), String> {
    match client.get(url).timeout(TIMEOUT).send().await {
        Ok(r) if r.status().is_success() => Ok(()),
        Ok(r) => Err(format!("{url}: HTTP {}", r.status())),
        Err(e) if e.is_timeout() => Err(format!("{url}: no answer within {}s", TIMEOUT.as_secs())),
        Err(e) if e.is_connect() => Err(format!("{url}: connection refused")),
        Err(e) => Err(format!("{url}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STORMD: &str = r#"[general]
name = "stormstorage"
[api]
bind = "0.0.0.0:9193"
[[process]]
name = "stormstorage"
command = "/usr/sbin/stormstorage"
[process.liveness]
type = "http"
url = "http://127.0.0.1:9093/api/v1/health"
initial_delay_secs = 5
failure_threshold = 3
"#;

    #[test]
    fn the_liveness_url_is_read_from_a_stormd_config() {
        assert_eq!(liveness_url(STORMD).as_deref(), Some("http://127.0.0.1:9093/api/v1/health"));
        assert_eq!(liveness_url("[process.liveness]\ntype = \"tcp\"\nurl = \"x\"\n"), None, "not http");
        assert_eq!(liveness_url("[general]\nname = \"x\"\n"), None);
    }

    #[test]
    fn a_host_network_service_is_found_through_its_boot_unit_and_golden() {
        let root = tempfile::tempdir().unwrap();
        let bd = root.path().join("etc/stormpump/boot.d");
        std::fs::create_dir_all(&bd).unwrap();
        std::fs::write(
            bd.join("50-services"),
            "volume  sstor  /pallets/stormstorage\nvolume  lbp  /pallets/stormlb\n\
             spec    stormstorage\n  domain container\n  root      sstor\n  profile   host\n  argv /stormd\n\
             spec    stormlb\n  root lbp\n  profile routed\n",
        )
        .unwrap();
        for (p, body) in [("pallets/stormstorage", STORMD), ("pallets/stormlb", STORMD)] {
            let d = root.path().join(p).join("etc/stormd");
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("config.toml"), body).unwrap();
        }
        let urls = health_urls(root.path());
        assert_eq!(urls.get("stormstorage").map(String::as_str), Some("http://127.0.0.1:9093/api/v1/health"));
        assert!(!urls.contains_key("stormlb"), "not on the host network: its loopback is not ours");
    }

    #[test]
    fn three_failures_make_it_not_ready_and_one_answer_ready_again() {
        let mut h = ServiceHealth { ready: true, ..Default::default() };
        assert!(!observe(&mut h, Err("refused".into())));
        assert!(!observe(&mut h, Err("refused".into())));
        assert!(h.ready, "two failures are not three");
        assert!(observe(&mut h, Err("connection refused :9093".into())), "the third flips it");
        assert_eq!((h.ready, h.reason.as_str()), (false, "connection refused :9093"));
        assert!(observe(&mut h, Ok(())));
        assert!(h.ready && h.reason.is_empty() && h.failures == 0);
    }

    #[tokio::test]
    async fn a_probe_tells_an_answer_from_a_refusal() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let e = probe(&reqwest::Client::new(), &format!("http://127.0.0.1:{port}/healthz")).await.unwrap_err();
        assert!(e.contains("connection refused"), "{e}");
    }
}
