//! rustkube-node's test container (stormcentral `docs/test-standard.md`).
//!
//! rustkube-node is the kubelet (and kube-proxy). The suites test it from
//! outside, through the Kubernetes API only:
//!
//! - `medium` (< 30 min): claims of the built-in `stormblock` class at every
//!   size class and at arbitrary sizes, end to end (`medium.rs`, #64), and
//!   the node's own volumes as complete PV + PVC pairs (`node_volumes.rs`, #59).
//! - `short` and `long`: not written yet (#61). Each reports one skip, which
//!   never counts as a pass.
//!
//! The workload pods run this same image in its workload modes, so a run
//! pulls nothing but its own image. Where the built-in class is not there (no
//! StorageClass `stormblock` of `stormblock.storm.io`), the suite reports one
//! skip.

pub mod api;
pub mod env;
pub mod k8s;
pub mod medium;
pub mod node_volumes;
pub mod report;
pub mod workload;

use std::sync::Arc;

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
    match env.suite.as_str() {
        "medium" => {
            match k8s::no_builtin_class(&api).await {
                Err(e) => r.record("builtin-class", Outcome::Infra(e), 0, None),
                Ok(Some(why)) => r.record("builtin-class", Outcome::Skip(why), 0, None),
                Ok(None) => {
                    Box::pin(medium::run(env, api, r)).await;
                    true
                }
            };
        }
        "short" | "long" => {
            r.record(
                &format!("{}-suite", env.suite),
                Outcome::Skip(format!("rustkube-node's {} suite is not written yet (rustkube-node#61)", env.suite)),
                0,
                None,
            );
        }
        other => {
            r.record("suite", Outcome::Infra(format!("STORM_SUITE {other:?} is not short, medium or long")), 0, None);
        }
    }
}
