//! rustkube-node's test container (stormcentral `docs/test-standard.md`).
//!
//! rustkube-node is the kubelet (and kube-proxy). The suites test it from
//! outside, through the Kubernetes API only:
//!
//! - `short` (< 2 min): the node is Ready and the kubelet runs a pod, reports
//!   its exit code, serves its log and stops it on delete (`short.rs`, #61).
//! - `medium` (< 30 min): claims of the built-in `stormblock` class at every
//!   size class and at arbitrary sizes, end to end (`medium.rs`, #64), the
//!   node's own volumes as complete PV + PVC pairs (`node_volumes.rs`, #59),
//!   and pod features and failure paths (`pods.rs`, #61).
//! - `long` (the night): waves of pods sized from the node's capacity,
//!   measuring start latency and what each wave leaves behind (`long.rs`).
//!
//! The workload pods run this same image in its workload modes, so a run
//! pulls nothing but its own image. Where the built-in class is not there (no
//! StorageClass `stormblock` of `stormblock.storm.io`), medium's storage cases
//! report one skip and long's waves carry no claims.

pub mod api;
pub mod env;
pub mod k8s;
pub mod long;
pub mod medium;
pub mod node_volumes;
pub mod pods;
pub mod report;
pub mod short;
pub mod workload;

use std::sync::Arc;
use std::time::Duration;

use env::Env;
use report::{Outcome, Report};

/// Run the suite `env.suite` names, recording into `r`.
pub async fn run(env: Arc<Env>, r: &mut Report) {
    let api = match api::Api::new(&env) {
        Ok(a) => a,
        Err(e) => {
            r.record("api-client", Outcome::Infra(e), 0, None);
            return;
        }
    };
    // What the runner gives as an address, and the image it does not name
    // (#97), resolved once for the whole run.
    let mut e = (*env).clone();
    match k8s::node_name(&api, &e.node).await {
        Ok(n) => e.node_name = n,
        Err(err) => {
            r.record("node", Outcome::Infra(format!("which Node is {}: {err}", e.node)), 0, None);
            return;
        }
    }
    if e.image.is_empty() {
        e.image = k8s::own_image(&e, &api).await.unwrap_or_else(|| e.standard_image());
    }
    let env = Arc::new(e);
    match env.suite.as_str() {
        "short" => short::run(env, api, r).await,
        "medium" => {
            // The pod cases need nothing but this image: they run whether or
            // not the storage class is here.
            let pod_cases = tokio::spawn(pods::run(env.clone(), api.clone()));
            match k8s::no_builtin_class(&api).await {
                Err(e) => r.record("builtin-class", Outcome::Infra(e), 0, None),
                Ok(Some(why)) => r.record("builtin-class", Outcome::Skip(why), 0, None),
                Ok(None) => {
                    Box::pin(medium::run(env, api, r, pod_cases)).await;
                    return;
                }
            };
            medium::record_pod_cases(pod_cases, r).await;
            if let Err(e) = k8s::drain(&env, &api, Duration::from_secs(5)).await {
                r.record("cleanup", Outcome::Infra(e), 0, None);
            }
        }
        "long" => Box::pin(long::run(env, api, r)).await,
        other => {
            r.record("suite", Outcome::Infra(format!("STORM_SUITE {other:?} is not short, medium or long")), 0, None);
        }
    }
}
