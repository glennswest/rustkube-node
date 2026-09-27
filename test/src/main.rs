//! `rustkube-node-test`: the container's entrypoint. With workload arguments
//! it is the program inside a claim-holding pod; otherwise it reads the
//! runner's environment, runs one suite within `STORM_TIMEOUT`, and exits 0,
//! 1 or 2.

use rustkube_node_test::env::Env;
use rustkube_node_test::report::{Outcome, Report};
use rustkube_node_test::{run, workload};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if workload::is_workload(&args) {
        std::process::exit(workload::main(&args));
    }
    // Started as `/test <suite>` (the standard); the argument wins over
    // STORM_SUITE. Set before any thread exists.
    if let Some(suite) = args.first().filter(|a| matches!(a.as_str(), "short" | "medium" | "long")) {
        std::env::set_var("STORM_SUITE", suite);
    }
    let env = Env::read();
    let mut r = Report::new();
    let missing = env.missing();
    if !missing.is_empty() {
        r.record("environment", Outcome::Infra(format!("the runner did not set {}", missing.join(", "))), 0, None);
        std::process::exit(r.finish());
    }
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            r.record("runtime", Outcome::Infra(format!("tokio runtime: {e}")), 0, None);
            std::process::exit(r.finish());
        }
    };
    let deadline = env.timeout;
    let env = std::sync::Arc::new(env);
    let ran = rt.block_on(async { tokio::time::timeout(deadline, run(env.clone(), &mut r)).await });
    if ran.is_err() {
        r.record(
            "timeout",
            Outcome::Fail(format!("the suite did not finish within STORM_TIMEOUT ({} s)", deadline.as_secs())),
            deadline.as_millis(),
            None,
        );
    }
    let code = r.finish();
    rt.shutdown_background();
    std::process::exit(code);
}
