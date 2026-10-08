//! A node service's state as its stormd reports it, for its mirror pod (#215).
//!
//! stormd (stormd#48) supervises its processes the Kubernetes way: startup,
//! liveness and readiness probes, a restart policy with exponential back-off
//! (`CrashLoopBackOff`), and Kubernetes events. Its API, on each service's own
//! port, answers:
//!
//! - `GET /api/v1/processes`: each process's `state` (`running`,
//!   `CrashLoopBackOff`, `stopped`, `failed`, `starting`, …), `ready` (the
//!   readiness probe, else the startup probe), `restarts`, `exit_code`,
//!   `started_at`, `stopped_at`;
//! - `GET /api/v1/events?since=<seq>`: `{items: [{type, reason, message,
//!   process, count, firstTimestamp, lastTimestamp, seq}]}`, upstream's
//!   reasons (`Created`, `Started`, `Unhealthy`, `Killing`, `BackOff`), a
//!   repeat bumping `count`, `lastTimestamp` and `seq`.
//!
//! **Where.** The service's golden (the `root` of the boot unit that runs it,
//! seen under `/hostroot`) carries stormd's config, whose `[api] bind` is the
//! port (stormd README "stormd's API port on a node": 9081 fastetcd, 9082–9084
//! the control plane, 9085 rustkube-node, a service's port + 100). Only for a
//! host-network service (`profile host`): its address is the node's, so the
//! kubelet's `127.0.0.1`. stormcos serves these APIs over plain HTTP with no
//! credential today; one whose `[api]` asks for TLS or a credential (stormd#32)
//! is not read, said once, and its mirror keeps PID 1's view.
//!
//! **What the mirror does with it** (`mirror::with_stormd`): the container
//! status is the process's (Running, Waiting `CrashLoopBackOff`, Terminated
//! with its exit), its restart count and last termination, Ready from its
//! readiness probe; and every stormd event is an Event on the mirror pod
//! ([`event_object`]), so `kubectl describe pod` reads a node service as it
//! reads a Pod.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};

/// How often each service's stormd is asked.
pub const PERIOD: Duration = Duration::from_secs(5);
/// How long one ask may take (loopback).
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// Where a service's stormd answers, or why it is not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// `http://<host>:<port>`
    Plain(String),
    /// Its `[api]` wants TLS or a credential the kubelet does not hold.
    Guarded(String),
}

/// Each host-network service's golden root (by asset name), from the boot
/// units under `root` (`/hostroot`): the `root` volume of a `profile host`
/// spec, mapped through the unit's `volume` lines.
pub fn host_service_roots(root: &Path) -> HashMap<String, std::path::PathBuf> {
    let mut volumes: HashMap<String, String> = HashMap::new();
    let mut specs: Vec<(String, Option<String>, bool)> = Vec::new();
    let mut units: Vec<_> = std::fs::read_dir(root.join("etc/stormpump/boot.d"))
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
    specs
        .into_iter()
        .filter(|(_, _, host)| *host)
        .filter_map(|(name, v, _)| {
            let path = volumes.get(&v?)?;
            Some((name, root.join(path.trim_start_matches('/'))))
        })
        .collect()
}

/// The stormd API of a stormd config, from its `[api]` table. `None` when the
/// config has no `bind` (stormd's default, 0.0.0.0:9080, is no service's).
pub fn endpoint(config: &str) -> Option<Endpoint> {
    let mut in_api = false;
    let mut bind = None::<String>;
    let mut guarded = Vec::new();
    for line in config.lines().map(|l| l.split('#').next().unwrap_or("").trim()) {
        if line.starts_with('[') {
            in_api = line == "[api]";
            continue;
        }
        if !in_api {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else { continue };
        let (k, v) = (k.trim(), v.trim().trim_matches('"'));
        match k {
            "bind" => bind = Some(v.to_string()),
            "tls_cert_file" | "client_ca_file" | "token_file" | "auth_token" | "password" if !v.is_empty() => {
                guarded.push(k.to_string())
            }
            _ => {}
        }
    }
    let bind = bind?;
    let (host, port) = bind.rsplit_once(':')?;
    let host = match host.trim_matches(['[', ']']) {
        "0.0.0.0" | "" | "::" => "127.0.0.1",
        h => h,
    };
    let port: u16 = port.parse().ok()?;
    Some(if guarded.is_empty() {
        Endpoint::Plain(format!("http://{host}:{port}"))
    } else {
        Endpoint::Guarded(format!("[api] sets {}", guarded.join(", ")))
    })
}

/// Every host-network service's stormd endpoint, by asset name.
pub fn endpoints(root: &Path) -> HashMap<String, Endpoint> {
    host_service_roots(root)
        .into_iter()
        .filter_map(|(name, golden)| {
            let text = std::fs::read_to_string(golden.join("etc/stormd/config.toml")).ok()?;
            Some((name, endpoint(&text)?))
        })
        .collect()
}

/// One process, as stormd reports it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Process {
    pub name: String,
    pub state: String,
    pub ready: bool,
    pub restarts: u32,
    pub exit_code: Option<i32>,
    pub started_at: Option<String>,
    pub stopped_at: Option<String>,
}

/// `GET /api/v1/processes`'s array.
pub fn parse_processes(v: &Value) -> Vec<Process> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|p| Process {
            name: p["name"].as_str().unwrap_or("").to_string(),
            state: p["state"].as_str().unwrap_or("").to_string(),
            ready: p["ready"].as_bool().unwrap_or(false),
            restarts: p["restarts"].as_u64().unwrap_or(0) as u32,
            exit_code: p["exit_code"].as_i64().map(|c| c as i32),
            started_at: p["started_at"].as_str().map(str::to_string),
            stopped_at: p["stopped_at"].as_str().map(str::to_string),
        })
        .filter(|p| !p.name.is_empty())
        .collect()
}

/// How bad a state is, for picking the process that speaks for a container
/// of several: the worst one.
fn badness(state: &str) -> u8 {
    match state {
        "CrashLoopBackOff" => 5,
        "failed" => 4,
        "stopped" => 3,
        "pending" | "starting" | "restarting" | "stopping" => 2,
        _ => 1,
    }
}

/// The process a service's one mirror container stands for: the one named
/// after the service; else, for a stormd running several, the worst of them,
/// with every process's restarts and ready only when all are.
pub fn representative(asset: &str, procs: &[Process]) -> Option<Process> {
    if let Some(p) = procs.iter().find(|p| p.name == asset) {
        return Some(p.clone());
    }
    let worst = procs.iter().max_by_key(|p| badness(&p.state))?;
    Some(Process {
        ready: procs.iter().all(|p| p.ready),
        restarts: procs.iter().map(|p| p.restarts).sum(),
        ..worst.clone()
    })
}

/// One stormd event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StormdEvent {
    pub kind: String,
    pub reason: String,
    pub message: String,
    pub process: String,
    pub count: u64,
    pub first: String,
    pub last: String,
    pub seq: u64,
}

/// `GET /api/v1/events`' items.
pub fn parse_events(v: &Value) -> Vec<StormdEvent> {
    v["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            Some(StormdEvent {
                kind: e["type"].as_str().unwrap_or("Normal").to_string(),
                reason: e["reason"].as_str()?.to_string(),
                message: e["message"].as_str().unwrap_or("").to_string(),
                process: e["process"].as_str().unwrap_or("").to_string(),
                count: e["count"].as_u64().unwrap_or(1),
                first: e["firstTimestamp"].as_str().unwrap_or("").to_string(),
                last: e["lastTimestamp"].as_str().unwrap_or("").to_string(),
                seq: e["seq"].as_u64()?,
            })
        })
        .collect()
}

/// An RFC 3339 time to the second, as an Event's `firstTimestamp` wants it.
fn to_secs(t: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(t)
        .map(|t| t.with_timezone(&chrono::Utc).format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|_| t.to_string())
}

/// The Event a stormd event comes to on `pod` (the mirror pod, with its uid).
///
/// Named from the event's identity (process, reason, message, first time),
/// so the same event read again (a kubelet restart reads from `since=0`) is
/// the same object: created once, its `count` and `lastTimestamp` patched as
/// stormd bumps them.
pub fn event_object(pod: &Value, container: &str, node: &str, e: &StormdEvent) -> Value {
    use sha2::{Digest, Sha256};
    let pod_name = pod["metadata"]["name"].as_str().unwrap_or("");
    let digest = Sha256::digest(format!("{}\0{}\0{}\0{}", e.process, e.reason, e.message, e.first).as_bytes());
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let last_micro = chrono::DateTime::parse_from_rfc3339(&e.last)
        .map(|t| t.with_timezone(&chrono::Utc).format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_default();
    // A process other than the one the container is named for says so.
    let message = if e.process.is_empty() || e.process == container || e.message.contains(&e.process) {
        e.message.clone()
    } else {
        format!("{} (process {})", e.message, e.process)
    };
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": format!("{pod_name}.{hex}"), "namespace": "kube-system" },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "namespace": "kube-system",
            "name": pod_name,
            "uid": pod["metadata"]["uid"],
            "fieldPath": format!("spec.containers{{{container}}}"),
        },
        "reason": e.reason,
        "message": message,
        "type": if e.kind == "Warning" { "Warning" } else { "Normal" },
        "source": { "component": "kubelet", "host": node },
        "reportingComponent": "stormd",
        "reportingInstance": node,
        "firstTimestamp": to_secs(&e.first),
        "lastTimestamp": to_secs(&e.last),
        "eventTime": if last_micro.is_empty() { Value::Null } else { json!(last_micro) },
        "count": e.count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_api_port_comes_from_the_configs_api_table() {
        let c = "[general]\nname = \"fastetcd\"\n\n[api]\n# comment\nbind = \"0.0.0.0:9081\"\n\n[[process]]\nname = \"fastetcd\"\n";
        assert_eq!(endpoint(c), Some(Endpoint::Plain("http://127.0.0.1:9081".into())));
        assert_eq!(endpoint("[api]\nbind = \"10.0.0.5:9192\""), Some(Endpoint::Plain("http://10.0.0.5:9192".into())));
        assert!(matches!(
            endpoint("[api]\nbind = \"0.0.0.0:9082\"\ntoken_file = \"/data/t\"\n"),
            Some(Endpoint::Guarded(why)) if why.contains("token_file")
        ));
        assert_eq!(endpoint("[general]\nname = \"x\"\n"), None);
    }

    #[test]
    fn host_services_are_found_through_their_boot_units() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::create_dir_all(root.join("etc/stormpump/boot.d")).unwrap();
        std::fs::write(
            root.join("etc/stormpump/boot.d/40-fastetcd"),
            "volume fe /goldens/fastetcd\nspec fastetcd\n  root fe\n  profile host\nspec isolated\n  root fe\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("goldens/fastetcd/etc/stormd")).unwrap();
        std::fs::write(root.join("goldens/fastetcd/etc/stormd/config.toml"), "[api]\nbind = \"0.0.0.0:9081\"\n").unwrap();
        let e = endpoints(root);
        assert_eq!(e.len(), 1, "{e:?}");
        assert_eq!(e["fastetcd"], Endpoint::Plain("http://127.0.0.1:9081".into()));
    }

    #[test]
    fn the_container_is_its_named_process_else_the_worst() {
        let procs = parse_processes(&json!([
            {"name": "kubelet", "state": "running", "ready": true, "restarts": 1},
            {"name": "kube-proxy", "state": "CrashLoopBackOff", "ready": false, "restarts": 4, "exit_code": 2},
        ]));
        let r = representative("rustkube-node", &procs).unwrap();
        assert_eq!((r.name.as_str(), r.state.as_str(), r.ready, r.restarts), ("kube-proxy", "CrashLoopBackOff", false, 5));
        let r = representative("kubelet", &procs).unwrap();
        assert_eq!((r.state.as_str(), r.ready, r.restarts), ("running", true, 1));
        assert_eq!(representative("x", &[]), None);
    }

    #[test]
    fn a_stormd_event_is_one_event_object_for_good() {
        let items = parse_events(&json!({"items": [{
            "type": "Warning", "reason": "Unhealthy", "message": "Liveness probe failed: connection refused",
            "process": "fastetcd", "count": 3, "firstTimestamp": "2026-10-08T21:13:23.123Z",
            "lastTimestamp": "2026-10-08T21:13:43.5Z", "seq": 17}]}));
        assert_eq!(items.len(), 1);
        let pod = json!({"metadata": {"name": "fastetcd-n1", "uid": "u-1"}});
        let e = event_object(&pod, "fastetcd", "n1", &items[0]);
        assert_eq!(e["involvedObject"]["uid"], "u-1");
        assert_eq!(e["involvedObject"]["fieldPath"], "spec.containers{fastetcd}");
        assert_eq!(e["type"], "Warning");
        assert_eq!(e["count"], 3);
        assert_eq!(e["firstTimestamp"], "2026-10-08T21:13:23Z");
        assert_eq!(e["eventTime"], "2026-10-08T21:13:43.500000Z");
        // Bumped by stormd: the same object.
        let bumped = StormdEvent { count: 4, last: "2026-10-08T21:13:53Z".into(), seq: 18, ..items[0].clone() };
        assert_eq!(event_object(&pod, "fastetcd", "n1", &bumped)["metadata"]["name"], e["metadata"]["name"]);
        // Another process of the same service says which.
        let other = StormdEvent { process: "sidecar".into(), message: "Back-off restarting failed container".into(), ..items[0].clone() };
        assert!(event_object(&pod, "fastetcd", "n1", &other)["message"].as_str().unwrap().ends_with("(process sidecar)"));
    }
}
