//! Readiness for the node's own services (#96).
//!
//! A mirror pod (`mirror.rs`) said `Running` and `Ready` whenever PID 1 had
//! the service's process up, so a service up as a process and serving nothing
//! (stormstorage on 11.50: `:9093` and `:9193` refused) looked healthy to
//! everything that reads pod status: the console, test runs, a release gate.
//!
//! Each service already declares how it is checked, in its golden's stormd
//! config (`/etc/stormd/config.toml`, stormcos `service_golden`). Since
//! stormd#48 that is a Kubernetes-style probe on the `[[process]]` (#219):
//!
//! ```toml
//! [process.readiness_probe]
//! http_get = { path = "/api/v1/health", port = 9093 }   # or tcp_socket = { port = 9093 }
//! period_seconds = 10
//! failure_threshold = 3
//! ```
//!
//! with stormd's fields and defaults (`probes.rs`: `http_get {path "/", port,
//! host 127.0.0.1, scheme HTTP, http_headers}`, `tcp_socket {port, host}`,
//! `exec`, `grpc`; period 10 s, timeout 1 s, failure 3, success 1; the
//! Kubernetes camelCase spellings too). Goldens not moved yet still carry the
//! retired `[process.liveness]` (`type = "http"`, `url`), which stormd parses
//! and no longer acts on. The kubelet takes, from the process named as the
//! service first and then the others in order, the first of
//! `readiness_probe`, `liveness_probe`, the old `liveness` it can run, and
//! runs it on the probe's own period, timeout and thresholds:
//!
//! - `http_get`: a GET of `<scheme>://<host>:<port><path>`; 200–399 is an
//!   answer, redirects are not followed, an HTTPS certificate is not checked
//!   (upstream's and stormd's rules);
//! - `tcp_socket`: a connect;
//! - `exec` and `grpc`: not run from the kubelet (an exec runs in the
//!   service's own root), said once in the log; the mirror pod's readiness is
//!   then PID 1's view, as before #96.
//!
//! `startup_probe` is not read: it gates stormd's restarts, and a readiness
//! failure here restarts nothing. The service is found through the boot unit
//! that runs it (its `root` volume is the golden, seen under `/hostroot`).
//! Only for a service on the host network (`profile host`): its `127.0.0.1`
//! is the node's, and so the kubelet's.
//!
//! Since #215 this is the fallback: a service whose own stormd answers its API
//! (stormd#48) reports its processes' readiness itself (`stormd_api.rs`).
//! This probe is asked only of a service whose stormd does not answer.
//!
//! `failure_threshold` failures in a row make the service not ready;
//! `success_threshold` answers make it ready again. Not ready is a readiness
//! answer, not a crash: the pod stays `Running`, its container `ready: false`,
//! `Ready=False` with the reason, as upstream reports a running container that
//! fails its readiness probe.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Failures in a row before a service is not ready, for the old
/// `[process.liveness]` that names none: stormd's own default.
pub const FAILURES: u32 = 3;
/// How often the old `[process.liveness]` is asked, and how often the
/// configs are read again.
pub const PERIOD: Duration = Duration::from_secs(10);
/// How long one ask of the old `[process.liveness]` may take.
pub const TIMEOUT: Duration = Duration::from_secs(2);
/// The probe loop's clock: each service is asked when its period is up.
pub const TICK: Duration = Duration::from_secs(1);

/// What a probe does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// A GET of `url`, with these headers.
    Http { url: String, headers: Vec<(String, String)> },
    /// A connect to `addr` (`host:port`).
    Tcp { addr: String },
}

/// One service's probe, as the kubelet runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceProbe {
    pub check: Check,
    /// The table it came from: `readiness_probe`, `liveness_probe` or `liveness`.
    pub from: &'static str,
    pub period: Duration,
    pub timeout: Duration,
    pub failure_threshold: u32,
    pub success_threshold: u32,
}

/// A service's probe, or why it has none the kubelet can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Probing {
    Probe(ServiceProbe),
    Unprobed(String),
}

/// Each host-network service's probe, by asset (spec) name, from the boot
/// units under `root` and the stormd config in each one's golden. A service
/// whose config declares no probe at all is left out.
pub fn health_probes(root: &Path) -> HashMap<String, Probing> {
    crate::stormd_api::host_service_roots(root)
        .into_iter()
        .filter_map(|(name, golden)| {
            let text = std::fs::read_to_string(golden.join("etc/stormd/config.toml")).ok()?;
            let probing = service_probe(&text, &name)?;
            Some((name, probing))
        })
        .collect()
}

/// The probe for `service` from its stormd config: the process named
/// `service` first, then the others in order; in each, `readiness_probe`,
/// then `liveness_probe`, then the old `liveness` (http). The first one the
/// kubelet can run wins; when there is none but an exec/grpc (or broken)
/// probe, why. `None`: no probe declared at all.
pub fn service_probe(config: &str, service: &str) -> Option<Probing> {
    let Ok(table) = toml::from_str::<toml::Table>(config) else {
        // Not TOML stormd would load either; the old line reading still
        // finds an old table in it.
        return liveness_url(config).map(|url| Probing::Probe(old_liveness(url, None)));
    };
    let processes: Vec<&toml::Table> = match table.get("process") {
        Some(toml::Value::Array(a)) => a.iter().filter_map(toml::Value::as_table).collect(),
        Some(toml::Value::Table(t)) => vec![t],
        _ => Vec::new(),
    };
    let name_of = |p: &toml::Table| p.get("name").and_then(toml::Value::as_str).unwrap_or("").to_string();
    let (named, rest): (Vec<_>, Vec<_>) = processes.into_iter().partition(|p| name_of(p) == service);
    let mut unprobed: Option<String> = None;
    for p in named.into_iter().chain(rest) {
        for (key, alias) in [("readiness_probe", "readinessProbe"), ("liveness_probe", "livenessProbe")] {
            let Some(v) = p.get(key).or_else(|| p.get(alias)) else { continue };
            match probe_from(v, key) {
                Ok(probe) => return Some(Probing::Probe(probe)),
                Err(why) => {
                    unprobed.get_or_insert_with(|| format!("process {:?} {key}: {why}", name_of(p)));
                }
            }
        }
        if let Some(old) = p.get("liveness").and_then(toml::Value::as_table) {
            let http = old.get("type").and_then(toml::Value::as_str) == Some("http");
            if let (true, Some(url)) = (http, old.get("url").and_then(toml::Value::as_str)) {
                let failures = old.get("failure_threshold").and_then(toml::Value::as_integer);
                return Some(Probing::Probe(old_liveness(url.to_string(), failures)));
            }
        }
    }
    unprobed.map(Probing::Unprobed)
}

fn old_liveness(url: String, failure_threshold: Option<i64>) -> ServiceProbe {
    ServiceProbe {
        check: Check::Http { url, headers: Vec::new() },
        from: "liveness",
        period: PERIOD,
        timeout: TIMEOUT,
        failure_threshold: failure_threshold.and_then(|n| u32::try_from(n).ok()).filter(|n| *n > 0).unwrap_or(FAILURES),
        success_threshold: 1,
    }
}

/// A stormd probe table (stormd `probes.rs`), as far as the kubelet reads it.
#[derive(serde::Deserialize)]
struct Probe {
    #[serde(default, alias = "httpGet")]
    http_get: Option<HttpGet>,
    #[serde(default, alias = "tcpSocket")]
    tcp_socket: Option<TcpSocket>,
    #[serde(default)]
    exec: Option<toml::Value>,
    #[serde(default)]
    grpc: Option<toml::Value>,
    #[serde(default = "ten", alias = "periodSeconds")]
    period_seconds: u64,
    #[serde(default = "one", alias = "timeoutSeconds")]
    timeout_seconds: u64,
    #[serde(default = "three", alias = "failureThreshold")]
    failure_threshold: u32,
    #[serde(default = "one32", alias = "successThreshold")]
    success_threshold: u32,
}

#[derive(serde::Deserialize)]
struct HttpGet {
    #[serde(default = "root_path")]
    path: String,
    port: u16,
    #[serde(default = "loopback")]
    host: String,
    #[serde(default = "http")]
    scheme: String,
    #[serde(default, alias = "httpHeaders")]
    http_headers: Vec<Header>,
}

#[derive(serde::Deserialize)]
struct Header {
    name: String,
    value: String,
}

#[derive(serde::Deserialize)]
struct TcpSocket {
    port: u16,
    #[serde(default = "loopback")]
    host: String,
}

fn ten() -> u64 {
    10
}
fn one() -> u64 {
    1
}
fn three() -> u32 {
    3
}
fn one32() -> u32 {
    1
}
fn root_path() -> String {
    "/".into()
}
fn loopback() -> String {
    "127.0.0.1".into()
}
fn http() -> String {
    "HTTP".into()
}

fn probe_from(v: &toml::Value, from: &'static str) -> Result<ServiceProbe, String> {
    let p: Probe = v.clone().try_into().map_err(|e: toml::de::Error| e.message().to_string())?;
    let check = match (p.http_get, p.tcp_socket, p.exec.is_some(), p.grpc.is_some()) {
        (Some(h), None, false, false) => {
            let scheme = if h.scheme.eq_ignore_ascii_case("https") {
                "https"
            } else if h.scheme.eq_ignore_ascii_case("http") {
                "http"
            } else {
                return Err(format!("http_get.scheme {:?} is not HTTP or HTTPS", h.scheme));
            };
            let path = if h.path.starts_with('/') { h.path } else { format!("/{}", h.path) };
            Check::Http {
                url: format!("{scheme}://{}:{}{path}", h.host, h.port),
                headers: h.http_headers.into_iter().map(|h| (h.name, h.value)).collect(),
            }
        }
        (None, Some(t), false, false) => Check::Tcp { addr: format!("{}:{}", t.host, t.port) },
        (None, None, true, false) => return Err("an exec probe runs in the service's own root, not from the kubelet".into()),
        (None, None, false, true) => return Err("a grpc probe is not run from the kubelet".into()),
        _ => return Err("needs exactly one of http_get, tcp_socket, exec, grpc".into()),
    };
    Ok(ServiceProbe {
        check,
        from,
        period: Duration::from_secs(p.period_seconds.max(1)),
        timeout: Duration::from_secs(p.timeout_seconds.max(1)),
        failure_threshold: p.failure_threshold.max(1),
        success_threshold: p.success_threshold.max(1),
    })
}

/// The first `[process.liveness]` of `type = "http"`'s `url`, read by line:
/// the fallback for a config that does not parse as TOML.
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
    /// Answers in a row since the last failure.
    pub successes: u32,
    /// Why it is not ready: the last failure.
    pub reason: String,
}

/// Fold one probe result into a service's health, under `probe`'s
/// thresholds; `true` when its readiness changed.
pub fn observe(h: &mut ServiceHealth, result: Result<(), String>, probe: &ServiceProbe) -> bool {
    let was = h.ready;
    match result {
        Ok(()) => {
            h.failures = 0;
            h.successes = h.successes.saturating_add(1);
            if h.successes >= probe.success_threshold {
                h.reason.clear();
                h.ready = true;
            }
        }
        Err(e) => {
            h.successes = 0;
            h.failures = h.failures.saturating_add(1);
            h.reason = e;
            if h.failures >= probe.failure_threshold {
                h.ready = false;
            }
        }
    }
    h.ready != was
}

/// The client probes are sent with: no redirects followed, any certificate
/// (upstream's and stormd's HTTP probe).
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap_or_default()
}

/// Run `check` once, within `timeout`.
pub async fn probe(client: &reqwest::Client, check: &Check, timeout: Duration) -> Result<(), String> {
    // Not retried (#211): this is a readiness probe; the failure threshold
    // is the retry.
    match check {
        Check::Http { url, headers } => {
            let mut req = client.get(url).timeout(timeout);
            for (k, v) in headers {
                req = req.header(k, v);
            }
            match req.send().await {
                Ok(r) if (200..400).contains(&r.status().as_u16()) => Ok(()),
                Ok(r) => Err(format!("{url}: HTTP {}", r.status())),
                Err(e) if e.is_timeout() => Err(format!("{url}: no answer within {}s", timeout.as_secs())),
                Err(e) if e.is_connect() => Err(format!("{url}: connection refused")),
                Err(e) => Err(format!("{url}: {e}")),
            }
        }
        Check::Tcp { addr } => match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr.as_str())).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(format!("tcp {addr}: {e}")),
            Err(_) => Err(format!("tcp {addr}: no answer within {}s", timeout.as_secs())),
        },
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
    fn the_liveness_url_is_read_by_line_from_a_stormd_config() {
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
        let probes = health_probes(root.path());
        let Some(Probing::Probe(p)) = probes.get("stormstorage") else { panic!("{probes:?}") };
        assert_eq!(p.check, Check::Http { url: "http://127.0.0.1:9093/api/v1/health".into(), headers: vec![] });
        assert!(!probes.contains_key("stormlb"), "not on the host network: its loopback is not ours");
    }

    fn probe_of(config: &str, service: &str) -> ServiceProbe {
        match service_probe(config, service) {
            Some(Probing::Probe(p)) => p,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_old_liveness_table_is_still_read() {
        let p = probe_of(STORMD, "stormstorage");
        assert_eq!(p.from, "liveness");
        assert_eq!(p.check, Check::Http { url: "http://127.0.0.1:9093/api/v1/health".into(), headers: vec![] });
        assert_eq!((p.period, p.timeout, p.failure_threshold, p.success_threshold), (PERIOD, TIMEOUT, 3, 1));
    }

    #[test]
    fn an_http_get_readiness_probe_becomes_a_url_with_its_own_numbers() {
        let config = r#"[[process]]
name = "stormstorage"
[process.readiness_probe]
http_get = { path = "api/v1/health", port = 9093, scheme = "HTTPS", http_headers = [{ name = "X-A", value = "b" }] }
period_seconds = 5
timeout_seconds = 3
failure_threshold = 4
success_threshold = 2
"#;
        let p = probe_of(config, "stormstorage");
        assert_eq!(p.from, "readiness_probe");
        assert_eq!(p.check, Check::Http { url: "https://127.0.0.1:9093/api/v1/health".into(), headers: vec![("X-A".into(), "b".into())] });
        assert_eq!((p.period, p.timeout, p.failure_threshold, p.success_threshold), (Duration::from_secs(5), Duration::from_secs(3), 4, 2));
    }

    #[test]
    fn a_tcp_socket_liveness_probe_becomes_a_connect_with_stormds_defaults() {
        // As a sub-table, in Kubernetes' spelling.
        let config = "[[process]]\nname = \"x\"\n[process.livenessProbe.tcpSocket]\nport = 9081\n";
        let p = probe_of(config, "x");
        assert_eq!(p.from, "liveness_probe");
        assert_eq!(p.check, Check::Tcp { addr: "127.0.0.1:9081".into() });
        assert_eq!((p.period, p.timeout, p.failure_threshold, p.success_threshold), (Duration::from_secs(10), Duration::from_secs(1), 3, 1));
    }

    #[test]
    fn the_new_tables_win_over_the_old_and_readiness_over_liveness() {
        let both = format!("{STORMD}[process.liveness_probe]\ntcp_socket = {{ port = 1 }}\n[process.readiness_probe]\nhttp_get = {{ port = 2 }}\n");
        let p = probe_of(&both, "stormstorage");
        assert_eq!((p.from, p.check), ("readiness_probe", Check::Http { url: "http://127.0.0.1:2/".into(), headers: vec![] }));
        let liveness_only = format!("{STORMD}[process.liveness_probe]\ntcp_socket = {{ port = 1 }}\n");
        assert_eq!(probe_of(&liveness_only, "stormstorage").from, "liveness_probe");
    }

    #[test]
    fn an_exec_probe_is_not_run_and_said_why_unless_another_can_be() {
        let exec = "[[process]]\nname = \"s\"\n[process.readiness_probe]\nexec = { command = [\"/bin/check\"] }\n";
        let Some(Probing::Unprobed(why)) = service_probe(exec, "s") else { panic!() };
        assert!(why.contains("readiness_probe") && why.contains("exec"), "{why}");
        // An exec readiness and an http liveness: the liveness one is run.
        let mixed = format!("{exec}[process.liveness_probe]\nhttp_get = {{ port = 7 }}\n");
        assert_eq!(probe_of(&mixed, "s").from, "liveness_probe");
        assert_eq!(service_probe("[general]\nname = \"x\"\n", "x"), None, "nothing declared: nothing said");
    }

    #[test]
    fn the_process_named_as_the_service_is_asked_first() {
        let config = "[[process]]\nname = \"helper\"\n[process.readiness_probe]\ntcp_socket = { port = 1 }\n\
                      [[process]]\nname = \"apiserver\"\n[process.readiness_probe]\ntcp_socket = { port = 6443 }\n";
        assert_eq!(probe_of(config, "apiserver").check, Check::Tcp { addr: "127.0.0.1:6443".into() });
        assert_eq!(probe_of(config, "other").check, Check::Tcp { addr: "127.0.0.1:1".into() }, "else the first");
    }

    #[test]
    fn failures_and_answers_flip_it_at_the_probes_thresholds() {
        let p = ServiceProbe { success_threshold: 2, ..probe_of(STORMD, "stormstorage") };
        let mut h = ServiceHealth { ready: true, ..Default::default() };
        assert!(!observe(&mut h, Err("refused".into()), &p));
        assert!(!observe(&mut h, Err("refused".into()), &p));
        assert!(h.ready, "two failures are not three");
        assert!(observe(&mut h, Err("connection refused :9093".into()), &p), "the third flips it");
        assert_eq!((h.ready, h.reason.as_str()), (false, "connection refused :9093"));
        assert!(!observe(&mut h, Ok(()), &p), "one answer of the two needed");
        assert!(observe(&mut h, Ok(()), &p));
        assert!(h.ready && h.reason.is_empty() && h.failures == 0);
    }

    #[tokio::test]
    async fn a_probe_tells_an_answer_from_a_refusal() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let tcp = Check::Tcp { addr: format!("127.0.0.1:{port}") };
        probe(&client(), &tcp, Duration::from_secs(1)).await.unwrap();
        drop(l);
        let e = probe(&client(), &tcp, Duration::from_secs(1)).await.unwrap_err();
        assert!(e.starts_with("tcp 127.0.0.1:"), "{e}");
        let http = Check::Http { url: format!("http://127.0.0.1:{port}/healthz"), headers: vec![] };
        let e = probe(&client(), &http, Duration::from_secs(1)).await.unwrap_err();
        assert!(e.contains("connection refused"), "{e}");
    }
}
