//! The `metadata` suite (#342, #122): a host-network pod asks its node's
//! kubelet for its own instance metadata with its own ServiceAccount token,
//! the way the node's stormimds forwards it (`X-Storm-Workload-Token` on
//! `GET /vmInstance/<address>`).
//!
//! A host-network pod's address is the node's, which names nobody; its pod's
//! bound token is its identity. The kubelet TokenReviews it, checks the pod it
//! is bound to is on this node, and answers that pod's metadata
//! (`storm.io/kind: Pod`). This asks the kubelet directly
//! (`https://127.0.0.1:10250`), so a refusal's `error` (#342's reasons) is
//! read here without stormimds in between.
//!
//! - `metadata-own-token-loopback`: the address is 127.0.0.1 (a node address
//!   on `lo`): 200, the pod's own name, namespace and uid.
//! - `metadata-own-token-node-ip`: the same at the node's InternalIP.
//! - `metadata-no-workload-token`: a node address and no workload token: 404
//!   saying so (a node address names nobody).
//!
//! Needs `host_network` from the runner (`test/requires.toml`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::api::{s, Api};
use crate::env::Env;
use crate::k8s;
use crate::report::{Outcome, Report};

const MARGIN: Duration = Duration::from_secs(15);

/// A host-network pod of this image asking `/vmInstance/<address>` with its
/// own token (and, unless `workload` is false, as the workload token too).
pub fn asking_pod(env: &Env, name: &str, address: &str, workload: bool) -> Value {
    let mut args = vec!["vm-instance", address];
    if !workload {
        args.push("no-workload-token");
    }
    let mut p = k8s::pod(env, name, &args);
    p["spec"]["hostNetwork"] = json!(true);
    p["spec"]["automountServiceAccountToken"] = json!(true);
    p
}

/// The kubelet's status and body from a `vm-instance` termination message.
pub fn answer(message: &str) -> Result<(u16, Value), String> {
    let line: Value = serde_json::from_str(message.trim()).map_err(|e| format!("not a workload line ({e}): {message}"))?;
    let detail = line["detail"].as_str().unwrap_or("");
    if line["ok"] != json!(true) {
        return Err(format!("the kubelet could not be asked: {detail}"));
    }
    let (status, body) = detail.split_once(' ').unwrap_or((detail, ""));
    let status = status.parse().map_err(|_| format!("no status in {detail:?}"))?;
    Ok((status, serde_json::from_str(body).unwrap_or_else(|_| json!(body))))
}

/// Whether the kubelet answered pod `ns/name` (uid `uid`) its own metadata.
pub fn own_metadata(status: u16, body: &Value, ns: &str, name: &str, uid: &str) -> Result<String, String> {
    if status != 200 {
        return Err(format!("{status}: {}", body.get("error").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| body.to_string())));
    }
    let checks = [
        ("storm.io/kind", s(body, "/storm.io~1kind"), "Pod"),
        ("tags.namespace", s(body, "/tags/namespace"), ns),
        ("tags.name", s(body, "/tags/name"), name),
        ("instance_id", s(body, "/instance_id"), uid),
    ];
    for (what, got, want) in checks {
        if got != want {
            return Err(format!("200, but {what} is {got:?}, not {want:?}: {body}"));
        }
    }
    Ok(format!("200: Pod {ns}/{name}, uid {uid}, zone {}", s(body, "/zone")))
}

struct Suite {
    env: Arc<Env>,
    api: Api,
}

impl Suite {
    /// Run pod `name` and read what the kubelet answered it, with its uid.
    async fn ask(&self, name: &str, address: &str, workload: bool) -> Result<(u16, Value, String), String> {
        let pod = asking_pod(&self.env, name, address, workload);
        let made = self.api.create(&k8s::pods(&self.env), &pod).await?;
        let uid = s(&made, "/metadata/uid").to_string();
        let within = self.env.budget(Duration::from_secs(180), MARGIN);
        let done = k8s::pod_done(&self.env, &self.api, name, within).await?;
        let message = if done.message.is_empty() {
            k8s::log(&self.env, &self.api, name, false).await.unwrap_or_default()
        } else {
            done.message
        };
        let (status, body) = answer(&message)?;
        Ok((status, body, uid))
    }

    async fn own(&self, name: &str, address: &str) -> Outcome {
        match self.ask(name, address, true).await {
            Err(e) => Outcome::Fail(e),
            Ok((status, body, uid)) => match own_metadata(status, &body, &self.env.namespace, name, &uid) {
                Ok(d) => Outcome::Pass(format!("{address}: {d}")),
                Err(e) => Outcome::Fail(format!("{address}: {e}")),
            },
        }
    }

    async fn no_workload_token(&self, address: &str) -> Outcome {
        match self.ask("md-none", address, false).await {
            Err(e) => Outcome::Fail(e),
            Ok((404, body, _)) => Outcome::Pass(format!("{address}: 404: {}", s(&body, "/error"))),
            Ok((status, body, _)) => Outcome::Fail(format!("{address}: wanted 404 for no workload token, got {status}: {body}")),
        }
    }
}

/// The node's InternalIP, else its first address.
async fn node_ip(env: &Env, api: &Api) -> Result<String, String> {
    let n = api
        .get(&format!("/api/v1/nodes/{}", env.node_name))
        .await?
        .ok_or_else(|| format!("Node {} is not there", env.node_name))?;
    let addrs = n["status"]["addresses"].as_array().cloned().unwrap_or_default();
    addrs
        .iter()
        .find(|a| a["type"] == "InternalIP")
        .or_else(|| addrs.first())
        .and_then(|a| a["address"].as_str())
        .map(str::to_string)
        .ok_or_else(|| format!("Node {} has no address", env.node_name))
}

pub async fn run(env: Arc<Env>, api: Api, r: &mut Report) {
    let suite = Suite { env: env.clone(), api: api.clone() };
    let ip = match node_ip(&env, &api).await {
        Ok(ip) => ip,
        Err(e) => {
            r.record("node-address", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let t = Instant::now();
    let (lo, nip, none) = tokio::join!(
        async { (suite.own("md-lo", "127.0.0.1").await, t.elapsed().as_millis()) },
        async { (suite.own("md-ip", &ip).await, t.elapsed().as_millis()) },
        async { (suite.no_workload_token(&ip).await, t.elapsed().as_millis()) },
    );
    r.record("metadata-own-token-loopback", lo.0, lo.1, None);
    r.record("metadata-own-token-node-ip", nip.0, nip.1, None);
    r.record("metadata-no-workload-token", none.0, none.1, None);
    if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(5)).await {
        r.record("cleanup", Outcome::Infra(e), 0, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pod_is_on_the_host_network_with_its_token() {
        let env = k8s::tests::env();
        let p = asking_pod(&env, "md-lo", "127.0.0.1", true);
        assert_eq!(p["spec"]["hostNetwork"], json!(true));
        assert_eq!(p["spec"]["automountServiceAccountToken"], json!(true));
        assert_eq!(p["spec"]["containers"][0]["args"], json!(["vm-instance", "127.0.0.1"]));
        let p = asking_pod(&env, "md-none", "10.0.0.1", false);
        assert_eq!(p["spec"]["containers"][0]["args"], json!(["vm-instance", "10.0.0.1", "no-workload-token"]));
    }

    #[test]
    fn the_answer_is_read_and_judged() {
        let ok = r#"{"workload": "vm-instance 127.0.0.1", "ok": true, "detail": "200 {\"storm.io/kind\":\"Pod\",\"instance_id\":\"u1\",\"tags\":{\"namespace\":\"t\",\"name\":\"md-lo\"},\"zone\":\"n1\"}"}"#;
        let (st, body) = answer(ok).unwrap();
        assert_eq!(st, 200);
        assert!(own_metadata(st, &body, "t", "md-lo", "u1").unwrap().contains("zone n1"));
        assert!(own_metadata(st, &body, "t", "md-lo", "u2").unwrap_err().contains("instance_id"));

        let refused = r#"{"workload": "vm-instance 127.0.0.1", "ok": true, "detail": "404 {\"error\":\"the token is bound to pod t/x on node n2, not this node (n1)\"}"}"#;
        let (st, body) = answer(refused).unwrap();
        let e = own_metadata(st, &body, "t", "md-lo", "u1").unwrap_err();
        assert!(e.starts_with("404: the token is bound"), "{e}");

        let failed = r#"{"workload": "vm-instance 127.0.0.1", "ok": false, "detail": "GET /vmInstance/127.0.0.1: connection refused"}"#;
        assert!(answer(failed).unwrap_err().contains("connection refused"));
    }
}
