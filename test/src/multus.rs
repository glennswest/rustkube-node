//! The `multus` suite (#233): a pod's extra networks by the Multus standard,
//! end to end on the node with the standard CNI plugins stormcos ships in
//! cni-bin (stormcos#495). The kubelet reads `k8s.v1.cni.cncf.io/networks`,
//! runs one CNI ADD per NetworkAttachmentDefinition on its own interface,
//! writes `k8s.v1.cni.cncf.io/network-status`, and DELs them at teardown.
//!
//! - `multus-two-nics`: a `bridge` NAD and a `macvlan` NAD (host-local IPAM)
//!   give the pod `net1` and `net2`, each with its IPv4 address, seen from
//!   inside the pod (`expect-nics`) and in network-status (the default first,
//!   then one entry per NAD, named `<ns>/<nad>`).
//! - `multus-missing-nad`: a pod naming a NAD that does not exist is not
//!   started; its FailedCreatePodSandBox Event names the NAD.
//! - `multus-release`: a NAD whose range holds one address, used by two pods
//!   in turn; the second gets the address only if deleting the first DELed
//!   its attachment.
//!
//! The NADs live in the run's namespace (the runner's Role there is `*`) and
//! go with it. Their host-local stores are named for the run; the bridges
//! (`rknt0`, `rknt1`, no address on the host) are reused by later runs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

pub const NETWORKS: &str = "k8s.v1.cni.cncf.io/networks";
pub const NETWORK_STATUS: &str = "k8s.v1.cni.cncf.io/network-status";

const BRIDGE: &str = "rknt-br";
const MACVLAN: &str = "rknt-mv";
const ONE: &str = "rknt-one";
const GHOST: &str = "rknt-ghost";
/// The one address `ONE`'s range holds.
const ONE_ADDRESS: &str = "172.31.252.10";
/// How long a NIC pod keeps running once it has checked (it is deleted first).
const HOLD: &str = "300";
const MARGIN: Duration = Duration::from_secs(15);

pub fn nads(env: &Env) -> String {
    format!("/apis/k8s.cni.cncf.io/v1/namespaces/{}/network-attachment-definitions", env.namespace)
}

/// A NAD named `name` whose `spec.config` is `config` with its CNI `name`
/// made unique to the run (host-local keeps its leases by that name).
pub fn nad(env: &Env, name: &str, mut config: Value) -> Value {
    config["cniVersion"] = json!("1.0.0");
    config["name"] = json!(format!("{name}-{}", env.run_id.to_ascii_lowercase()));
    json!({
        "apiVersion": "k8s.cni.cncf.io/v1", "kind": "NetworkAttachmentDefinition",
        "metadata": { "name": name, "namespace": env.namespace, "labels": k8s::labels(env) },
        "spec": { "config": config.to_string() },
    })
}

fn host_local(subnet: &str, start: &str, end: &str) -> Value {
    json!({ "type": "host-local", "ranges": [[{ "subnet": subnet, "rangeStart": start, "rangeEnd": end }]] })
}

/// The suite's NADs: a bridge, a macvlan on the host's default-route
/// interface (the plugin's default for no `master`), and a one-address bridge.
pub fn all_nads(env: &Env) -> Vec<Value> {
    vec![
        nad(env, BRIDGE, json!({
            "type": "bridge", "bridge": "rknt0", "isGateway": false, "ipMasq": false,
            "ipam": host_local("172.31.250.0/24", "172.31.250.10", "172.31.250.250"),
        })),
        nad(env, MACVLAN, json!({
            "type": "macvlan", "mode": "bridge",
            "ipam": host_local("172.31.251.0/24", "172.31.251.10", "172.31.251.250"),
        })),
        nad(env, ONE, json!({
            "type": "bridge", "bridge": "rknt1", "isGateway": false, "ipMasq": false,
            "ipam": host_local("172.31.252.0/24", ONE_ADDRESS, ONE_ADDRESS),
        })),
    ]
}

/// A pod of this image annotated with `networks`, checking `ifnames` from
/// inside and then holding (Running) so the suite reads it.
pub fn nic_pod(env: &Env, name: &str, networks: &str, ifnames: &[&str]) -> Value {
    let mut args = vec!["expect-nics", HOLD];
    args.extend_from_slice(ifnames);
    let mut p = k8s::pod(env, name, &args);
    p["metadata"]["annotations"] = json!({ NETWORKS: networks });
    p
}

/// What is wrong with a network-status annotation for `want` (each NAD by
/// name with its interface), or the addresses it gives each interface.
pub fn status_check(status: &str, namespace: &str, want: &[(&str, &str)]) -> Result<Vec<(String, String)>, String> {
    let list: Vec<Value> = serde_json::from_str(status).map_err(|e| format!("{NETWORK_STATUS} is not a JSON list: {e}"))?;
    let first = list.first().ok_or_else(|| format!("{NETWORK_STATUS} is empty"))?;
    if first["default"] != json!(true) {
        return Err(format!("the first network-status entry is not the default: {first}"));
    }
    let mut got = Vec::new();
    for (nad, ifname) in want {
        let name = format!("{namespace}/{nad}");
        let e = list
            .iter()
            .find(|e| e["name"] == name.as_str())
            .ok_or_else(|| format!("network-status has no entry for {name}: {status}"))?;
        if e["interface"] != *ifname {
            return Err(format!("{name} is on {}, not {ifname}", e["interface"]));
        }
        if e["default"] == json!(true) {
            return Err(format!("{name} is marked default"));
        }
        let ip = e["ips"][0].as_str().filter(|ip| !ip.is_empty()).ok_or_else(|| format!("{name} has no ips: {e}"))?;
        got.push((ifname.to_string(), ip.to_string()));
    }
    if list.len() != want.len() + 1 {
        return Err(format!("network-status has {} entries, wanted {}: {status}", list.len(), want.len() + 1));
    }
    Ok(got)
}

/// The `name=address` pairs of an `expect-nics` line's detail.
pub fn nics_seen(detail: &str) -> Vec<(String, String)> {
    detail
        .split_whitespace()
        .filter_map(|w| w.split_once('='))
        .map(|(n, a)| (n.to_string(), a.to_string()))
        .collect()
}

struct Suite {
    env: Arc<Env>,
    api: Api,
}

impl Suite {
    fn within(&self, want: u64) -> Duration {
        self.env.budget(Duration::from_secs(want), MARGIN)
    }

    async fn events(&self, pod: &str) -> String {
        match k8s::events_for(&self.env, &self.api, "Pod", pod).await {
            Ok(ev) if !ev.is_empty() => format!("; events: {}", ev.join(" | ")),
            _ => String::new(),
        }
    }

    /// Wait for NIC pod `name` to say what it saw: Ok(the pod, the detail)
    /// when it checked out and holds, Err once it ended or never ran.
    async fn nics(&self, name: &str, within: Duration) -> Result<(Value, String), String> {
        let path = format!("{}/{name}", k8s::pods(&self.env));
        let r = k8s::until(within, || {
            let path = path.clone();
            async move {
                let p = self.api.get(&path).await?.ok_or_else(|| format!("pod {name} is gone"))?;
                let phase = s(&p, "/status/phase").to_string();
                if phase != "Running" && phase != "Failed" && phase != "Succeeded" {
                    return Err(format!("pod {name} is {phase:?}{}", k8s::waiting(&p)));
                }
                let log = k8s::log(&self.env, &self.api, name, false).await.unwrap_or_default();
                let Some(line) = log.lines().find(|l| l.contains("\"workload\"")) else {
                    return Err(format!("pod {name} is {phase} and has said nothing yet"));
                };
                let v: Value = serde_json::from_str(line).map_err(|e| format!("{line}: {e}"))?;
                Ok((p, v["ok"] == json!(true), s(&v, "/detail").to_string(), phase))
            }
        })
        .await;
        match r {
            Ok((p, true, detail, phase)) if phase == "Running" => Ok((p, detail)),
            Ok((_, true, detail, phase)) => Err(format!("pod {name} saw its NICs ({detail}) but is {phase}, not holding")),
            Ok((_, false, detail, _)) => Err(format!("inside pod {name}: {detail}")),
            Err(e) => Err(format!("{e}{}", self.events(name).await)),
        }
    }

    /// The pod's network-status, waited for (it is written with the start).
    async fn status(&self, name: &str, within: Duration) -> Result<String, String> {
        let path = format!("{}/{name}", k8s::pods(&self.env));
        k8s::until(within, || {
            let path = path.clone();
            async move {
                let p = self.api.get(&path).await?.ok_or_else(|| format!("pod {name} is gone"))?;
                match p["metadata"]["annotations"][NETWORK_STATUS].as_str() {
                    Some(st) => Ok(st.to_string()),
                    None => Err(format!("pod {name} has no {NETWORK_STATUS} annotation")),
                }
            }
        })
        .await
    }

    async fn delete(&self, name: &str) -> Result<(), String> {
        let path = format!("{}/{name}", k8s::pods(&self.env));
        self.api.delete(&path).await?;
        k8s::gone(&self.api, &path, self.within(60)).await
    }

    async fn two_nics(&self) -> Outcome {
        let name = "multus-two";
        let pod = nic_pod(&self.env, name, &format!("{BRIDGE},{MACVLAN}"), &["net1", "net2"]);
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &pod).await {
            return Outcome::Infra(e);
        }
        let seen = match self.nics(name, self.within(180)).await {
            Ok((_, d)) => d,
            Err(e) => return Outcome::Fail(e),
        };
        let status = match self.status(name, self.within(30)).await {
            Ok(st) => st,
            Err(e) => return Outcome::Fail(e),
        };
        let listed = match status_check(&status, &self.env.namespace, &[(BRIDGE, "net1"), (MACVLAN, "net2")]) {
            Ok(l) => l,
            Err(e) => return Outcome::Fail(e),
        };
        // What the pod sees inside is what the status says.
        let inside = nics_seen(&seen);
        for (ifname, ip) in &listed {
            if !inside.iter().any(|(n, a)| n == ifname && a == ip) {
                return Outcome::Fail(format!("network-status gives {ifname} {ip}; inside the pod: {seen}"));
            }
        }
        if let Err(e) = self.delete(name).await {
            return Outcome::Fail(format!("deleting {name}: {e}"));
        }
        Outcome::Pass(format!("bridge on net1, macvlan on net2 ({seen}); network-status agrees, default first"))
    }

    async fn missing_nad(&self) -> Outcome {
        let name = "multus-ghost";
        let pod = nic_pod(&self.env, name, GHOST, &["net1"]);
        if let Err(e) = self.api.create(&k8s::pods(&self.env), &pod).await {
            return Outcome::Infra(e);
        }
        let want = format!("NetworkAttachmentDefinition {}/{GHOST} not found", self.env.namespace);
        let path = format!("{}/{name}", k8s::pods(&self.env));
        let r = k8s::until(self.within(90), || {
            let (want, path) = (want.clone(), path.clone());
            async move {
                let p = self.api.get(&path).await?.ok_or_else(|| format!("pod {name} is gone"))?;
                let phase = s(&p, "/status/phase").to_string();
                if phase == "Running" || phase == "Succeeded" {
                    return Ok(Err(format!("pod {name} ran ({phase}) with a missing NAD")));
                }
                let ev = k8s::events_for(&self.env, &self.api, "Pod", name).await?;
                match ev.iter().find(|e| e.starts_with("FailedCreatePodSandBox") && e.contains(&want)) {
                    Some(e) => Ok(Ok(format!("pod {phase}, {e}"))),
                    None => Err(format!("no FailedCreatePodSandBox naming {GHOST} yet (pod {phase}; events: {ev:?})")),
                }
            }
        })
        .await;
        let o = match r {
            Ok(Ok(d)) => Outcome::Pass(d),
            Ok(Err(e)) | Err(e) => Outcome::Fail(e),
        };
        let _ = self.delete(name).await;
        o
    }

    /// One pod with the one-address NAD, checked; then deleted.
    async fn one(&self, name: &str) -> Result<String, String> {
        self.api.create(&k8s::pods(&self.env), &nic_pod(&self.env, name, ONE, &["net1"])).await?;
        let (_, seen) = self.nics(name, self.within(180)).await?;
        if !nics_seen(&seen).iter().any(|(n, a)| n == "net1" && a == ONE_ADDRESS) {
            return Err(format!("pod {name} has {seen}, not net1={ONE_ADDRESS}"));
        }
        self.delete(name).await.map_err(|e| format!("deleting {name}: {e}"))?;
        Ok(seen)
    }

    async fn release(&self) -> Outcome {
        let t = Instant::now();
        if let Err(e) = self.one("multus-one-a").await {
            return Outcome::Fail(e);
        }
        match self.one("multus-one-b").await {
            Ok(_) => Outcome::Pass(format!(
                "{ONE_ADDRESS} (the range's only address) went to one pod, back on its delete, then to the next ({} ms)",
                t.elapsed().as_millis()
            )),
            Err(e) => Outcome::Fail(format!("the second pod: {e} (was the first's attachment DELed?)")),
        }
    }
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let suite = Suite { env: env.clone(), api: api.clone() };
    for n in all_nads(&env) {
        if let Err(e) = api.create(&nads(&env), &n).await {
            r.record("nads", Outcome::Infra(format!("NetworkAttachmentDefinition {}: {e}", s(&n, "/metadata/name"))), 0, None);
            return;
        }
    }
    // Independent: the ghost waits on its own, the release case on its NAD.
    let (two, ghost, release) = {
        let t = Instant::now();
        let (a, b, c) = tokio::join!(
            async { (suite.two_nics().await, t.elapsed().as_millis()) },
            async { (suite.missing_nad().await, t.elapsed().as_millis()) },
            async { (suite.release().await, t.elapsed().as_millis()) },
        );
        (a, b, c)
    };
    r.record("multus-two-nics", two.0, two.1, None);
    r.record("multus-missing-nad", ghost.0, ghost.1, None);
    r.record("multus-release", release.0, release.1, None);
    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(5)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_status_is_checked_against_the_nads() {
        let st = json!([
            {"name": "cilium", "interface": "eth0", "default": true, "ips": ["10.0.0.5"]},
            {"name": "t/rknt-br", "interface": "net1", "default": false, "ips": ["172.31.250.10"]},
            {"name": "t/rknt-mv", "interface": "net2", "default": false, "ips": ["172.31.251.10"]},
        ])
        .to_string();
        let got = status_check(&st, "t", &[(BRIDGE, "net1"), (MACVLAN, "net2")]).unwrap();
        assert_eq!(got, vec![("net1".into(), "172.31.250.10".into()), ("net2".into(), "172.31.251.10".into())]);
        assert!(status_check(&st, "t", &[(BRIDGE, "net2")]).unwrap_err().contains("not net2"));
        assert!(status_check(&st, "other", &[(BRIDGE, "net1")]).unwrap_err().contains("no entry for other/rknt-br"));
        assert!(status_check(&st, "t", &[(BRIDGE, "net1")]).unwrap_err().contains("3 entries, wanted 2"));
        let no_default = json!([{"name": "t/rknt-br", "interface": "net1", "ips": ["1.2.3.4"]}]).to_string();
        assert!(status_check(&no_default, "t", &[]).unwrap_err().contains("not the default"));
        let no_ip = json!([{"default": true}, {"name": "t/rknt-br", "interface": "net1", "ips": []}]).to_string();
        assert!(status_check(&no_ip, "t", &[(BRIDGE, "net1")]).unwrap_err().contains("has no ips"));
    }

    #[test]
    fn nads_carry_a_run_unique_cni_name_and_their_config() {
        let env = k8s::tests::env();
        let n = all_nads(&env);
        assert_eq!(n.len(), 3);
        let cfg: Value = serde_json::from_str(n[2]["spec"]["config"].as_str().unwrap()).unwrap();
        assert_eq!(cfg["type"], "bridge");
        assert_eq!(cfg["name"], format!("{ONE}-{}", env.run_id.to_ascii_lowercase()));
        assert_eq!(cfg["ipam"]["ranges"][0][0]["rangeStart"], ONE_ADDRESS);
        assert_eq!(cfg["ipam"]["ranges"][0][0]["rangeEnd"], ONE_ADDRESS);
        let mv: Value = serde_json::from_str(n[1]["spec"]["config"].as_str().unwrap()).unwrap();
        assert!(mv.get("master").is_none(), "macvlan takes the default-route interface");
        let p = nic_pod(&env, "x", "a,b", &["net1", "net2"]);
        assert_eq!(p["metadata"]["annotations"][NETWORKS], "a,b");
        assert_eq!(p["spec"]["containers"][0]["args"], json!(["expect-nics", HOLD, "net1", "net2"]));
    }

    #[test]
    fn an_expect_nics_detail_reads_back() {
        assert_eq!(
            nics_seen("net1=172.31.250.10 net2=172.31.251.11"),
            vec![("net1".into(), "172.31.250.10".into()), ("net2".into(), "172.31.251.11".into())]
        );
    }
}
