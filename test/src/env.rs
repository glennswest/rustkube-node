//! What the runner hands the container (stormcentral `docs/test-standard.md`),
//! and the knobs rustkube-node's suites add to it.

use std::time::{Duration, Instant};

/// Where the kubelet mounts the Job's ServiceAccount.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The StorageClass the kubelet provisions itself (`pkg/kubelet/src/storage.rs`).
pub const CLASS: &str = "stormblock";

/// Its provisioner (stormcos `deploy/manifests/45-storageclass.yaml`).
pub const PROVISIONER: &str = "stormblock.storm.io";

pub struct Env {
    pub suite: String,
    pub run_id: String,
    /// `STORM_NAMESPACE`: every namespaced object this run makes goes here.
    pub namespace: String,
    /// `STORM_API`: the apiserver.
    pub api: String,
    /// `STORM_NODE`: the node's address. Informational: the kubelet is
    /// reached through the API only.
    pub node: String,
    /// The image the workload pods run: this test image itself, in its
    /// workload modes, so nothing is pulled from outside the cluster.
    /// `RUSTKUBE_NODE_TEST_IMAGE` (the Job sets it).
    pub image: String,
    /// How long the first 1 TiB claim may take, pod created to finished: it
    /// may mint the class's blank (one mkfs of 1 TiB, stormblock#141).
    /// `RUSTKUBE_NODE_TEST_MINT_BUDGET` seconds, default 1200.
    pub mint_budget: Duration,
    pub token: Option<String>,
    pub ca: Option<Vec<u8>>,
    pub timeout: Duration,
    pub started: Instant,
}

impl Env {
    pub fn read() -> Env {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let sa = |f: &str| {
            std::fs::read_to_string(format!("{SA_DIR}/{f}"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        let suite = var("STORM_SUITE").unwrap_or_else(|| "short".into());
        let budget = match suite.as_str() {
            "medium" => 1800,
            "long" => 8 * 3600,
            _ => 120,
        };
        let secs = |k: &str, d: u64| Duration::from_secs(var(k).and_then(|v| v.parse().ok()).unwrap_or(d));
        Env {
            run_id: var("STORM_RUN_ID").unwrap_or_default(),
            namespace: var("STORM_NAMESPACE").or_else(|| sa("namespace")).unwrap_or_default(),
            api: var("STORM_API").unwrap_or_default(),
            node: var("STORM_NODE").unwrap_or_default(),
            image: var("RUSTKUBE_NODE_TEST_IMAGE").unwrap_or_default(),
            mint_budget: secs("RUSTKUBE_NODE_TEST_MINT_BUDGET", 1200),
            token: sa("token"),
            ca: std::fs::read(format!("{SA_DIR}/ca.crt")).ok(),
            timeout: secs("STORM_TIMEOUT", budget),
            started: Instant::now(),
            suite,
        }
    }

    /// What the runner must have set and did not.
    pub fn missing(&self) -> Vec<&'static str> {
        let mut m = Vec::new();
        if self.api.is_empty() {
            m.push("STORM_API");
        }
        if self.run_id.is_empty() {
            m.push("STORM_RUN_ID");
        }
        if self.namespace.is_empty() {
            m.push("STORM_NAMESPACE");
        }
        if self.image.is_empty() {
            m.push("RUSTKUBE_NODE_TEST_IMAGE");
        }
        m
    }

    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.started.elapsed())
    }

    /// `want`, but never past the suite's deadline less `margin` (kept for
    /// cleanup and the summary).
    pub fn budget(&self, want: Duration, margin: Duration) -> Duration {
        want.min(self.remaining().saturating_sub(margin))
    }
}
