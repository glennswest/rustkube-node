//! The kubelet's metrics, under the names upstream uses (#36).
//!
//! Two endpoints, and they are different things:
//!
//! - **`/metrics`**: the kubelet's own. `kubelet_running_pods`,
//!   `kubelet_running_containers{container_state}`,
//!   `kubelet_pod_start_duration_seconds`,
//!   `kubelet_pleg_relist_duration_seconds`, the `process_*` family and
//!   `kubernetes_build_info`. Rendered by the Prometheus recorder, which this
//!   module installs once for the process.
//! - **`/metrics/cadvisor`**: what the node's workloads consume, in cAdvisor's
//!   shape: `container_cpu_usage_seconds_total`,
//!   `container_memory_working_set_bytes`, `container_fs_usage_bytes`,
//!   `container_network_receive_bytes_total` and
//!   `container_network_transmit_bytes_total`. Rendered from what the runtime
//!   reports at the moment of the scrape ([`render_cadvisor`]), not through the
//!   recorder: a container that has gone must drop out of the next scrape,
//!   and a recorder keeps every series it has ever seen.
//!
//! **Absent is not zero.** A runtime that does not report a number (the
//! stormpump runtime has no container stats until #57, and no runtime here
//! knows a stormpump container's filesystem usage) gets no series, not a 0.
//! cAdvisor does the same, and a zero would be read as a measurement.
//!
//! **Restart counts are not here, deliberately.** Upstream has no kubelet
//! metric for them. `kube_pod_container_status_restarts_total` is
//! kube-state-metrics', derived from `status.containerStatuses[].restartCount`,
//! and a second count kept here would drift from the object, which is the one
//! that is right.

use std::fmt::Write as _;
use std::sync::OnceLock;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

use crate::cri::{ContainerState, ContainerStatsInfo, PodNetworkStats};

/// `kubelet_pod_start_duration_seconds`'s buckets, as upstream declares them.
const POD_START_BUCKETS: &[f64] = &[
    0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 20.0, 30.0, 45.0, 60.0, 120.0, 180.0, 240.0,
    300.0, 360.0, 480.0, 600.0, 900.0, 1200.0, 1800.0, 2700.0, 3600.0,
];
/// `kubelet_pleg_relist_duration_seconds`'s: Prometheus's default buckets.
const RELIST_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

const POD_START: &str = "kubelet_pod_start_duration_seconds";
const RELIST: &str = "kubelet_pleg_relist_duration_seconds";

static HANDLE: OnceLock<Option<PrometheusHandle>> = OnceLock::new();

/// The process's recorder, installed on first use.
///
/// Not `apimachinery::metrics::install`, whose `kubernetes_build_info` carries
/// rustkube's version: this is the kubelet, and it reports its own. The
/// `process_*` collector is apimachinery's.
///
/// `None` when another recorder is already installed. The kubelet then serves
/// the series it renders itself and says the rest are unavailable.
pub fn handle() -> Option<&'static PrometheusHandle> {
    HANDLE
        .get_or_init(|| {
            let builder = PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Full(POD_START.into()), POD_START_BUCKETS)
                .and_then(|b| b.set_buckets_for_metric(Matcher::Full(RELIST.into()), RELIST_BUCKETS));
            let handle = match builder.and_then(|b| b.install_recorder()) {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!("metrics recorder not installed: {e}");
                    return None;
                }
            };
            metrics::gauge!(
                "kubernetes_build_info",
                "gitVersion" => env!("CARGO_PKG_VERSION"),
                "component" => "kubelet",
                "goVersion" => "rustc",
            )
            .set(1.0);
            describe();
            Some(handle)
        })
        .as_ref()
}

fn describe() {
    metrics::describe_gauge!("kubelet_running_pods", "Number of pods that have a running pod sandbox");
    metrics::describe_gauge!(
        "kubelet_running_containers",
        "Number of containers currently running"
    );
    metrics::describe_histogram!(
        POD_START,
        metrics::Unit::Seconds,
        "Duration in seconds from kubelet seeing a pod for the first time to the pod starting to run"
    );
    metrics::describe_counter!(
        TIMED,
        "Reconciles scheduled by a deadline or a polling fallback rather than an event"
    );
    metrics::describe_histogram!(
        RELIST,
        metrics::Unit::Seconds,
        "Duration in seconds for relisting pods in PLEG"
    );
}

/// A pod ran for the first time, `seconds` after the kubelet first saw it.
pub fn observe_pod_start(seconds: f64) {
    if handle().is_some() {
        metrics::histogram!(POD_START).record(seconds);
    }
}

/// One pass over the pods this kubelet knows, re-reading their state from the
/// runtime. This kubelet has no separate PLEG: the sync pass is its relist.
pub fn observe_relist(seconds: f64) {
    if handle().is_some() {
        metrics::histogram!(RELIST).record(seconds);
    }
}

/// Work scheduled on a clock rather than by an event (#101).
pub const TIMED: &str = "kubelet_timed_reconciles_total";

/// A worker scheduled work with no event behind it. `worker` names it (`pod`,
/// `vmi`, `system-claims`, …); `cause` is `deadline` (a probe period, a
/// backoff, a pending retry: work that is due) or `fallback` (the source has
/// no event feed, so it is polled). A `fallback` count that grows is the
/// measure of what still polls.
pub fn observe_timed(worker: &'static str, cause: &'static str) {
    if handle().is_some() {
        metrics::counter!(TIMED, "worker" => worker, "cause" => cause).increment(1);
    }
}

/// What `/metrics` reports about pods and containers, taken at scrape time.
#[derive(Debug, Default, Clone)]
pub struct KubeletSnapshot {
    /// Pods with a sandbox.
    pub running_pods: usize,
    /// Containers by state, as the runtime lists them.
    pub containers: Vec<ContainerState>,
}

/// Render `/metrics`: set the scrape-time gauges, refresh `process_*`, render.
pub fn render_kubelet(snap: &KubeletSnapshot) -> String {
    let Some(h) = handle() else {
        return "# the kubelet's metrics recorder is not installed\n".to_string();
    };
    // Set and render as one step. The gauges are process-wide, and two
    // scrapes interleaved here would each render the other's numbers.
    static RENDER: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _one_at_a_time = RENDER.lock().unwrap_or_else(|e| e.into_inner());
    metrics::gauge!("kubelet_running_pods").set(snap.running_pods as f64);
    // Every state, including the ones with no containers: a state that
    // disappears from the scrape reads as "unknown", where 0 is the fact.
    for state in ["created", "running", "exited", "unknown"] {
        let n = snap.containers.iter().filter(|s| state_label(**s) == state).count();
        metrics::gauge!("kubelet_running_containers", "container_state" => state).set(n as f64);
    }
    apimachinery::metrics::refresh_process_metrics();
    h.render()
}

fn state_label(s: ContainerState) -> &'static str {
    match s {
        ContainerState::Created => "created",
        ContainerState::Running => "running",
        ContainerState::Exited => "exited",
        ContainerState::Unknown => "unknown",
    }
}

/// Render `/metrics/cadvisor` from the runtime's container and pod stats.
///
/// Labels are cAdvisor's: `container`, `id`, `namespace`, `pod`. `id` is the
/// runtime's container id (cAdvisor puts the cgroup path there, which no
/// runtime here reports). Network is per pod, as cAdvisor reports it, with
/// `container=""` and an `interface` label.
pub fn render_cadvisor(containers: &[ContainerStatsInfo], pods: &[PodNetworkStats]) -> String {
    let mut out = String::new();
    family(
        &mut out,
        "container_cpu_usage_seconds_total",
        "counter",
        "Cumulative cpu time consumed in seconds.",
        containers.iter().filter_map(|c| {
            c.cpu_usage_core_nanos.map(|n| (container_labels(c), n as f64 / 1e9))
        }),
    );
    family(
        &mut out,
        "container_memory_working_set_bytes",
        "gauge",
        "Current working set in bytes.",
        containers
            .iter()
            .filter_map(|c| c.memory_working_set_bytes.map(|b| (container_labels(c), b as f64))),
    );
    family(
        &mut out,
        "container_fs_usage_bytes",
        "gauge",
        "Number of bytes that are consumed by the container on this filesystem.",
        containers
            .iter()
            .filter_map(|c| c.fs_usage_bytes.map(|b| (container_labels(c), b as f64))),
    );
    let ifaces = || {
        pods.iter().flat_map(|p| p.interfaces.iter().map(move |i| (p, i)))
    };
    family(
        &mut out,
        "container_network_receive_bytes_total",
        "counter",
        "Cumulative count of bytes received.",
        ifaces().map(|(p, i)| (network_labels(p, &i.name), i.rx_bytes as f64)),
    );
    family(
        &mut out,
        "container_network_transmit_bytes_total",
        "counter",
        "Cumulative count of bytes transmitted.",
        ifaces().map(|(p, i)| (network_labels(p, &i.name), i.tx_bytes as f64)),
    );
    out
}

/// One metric family: HELP and TYPE, then its series. A family with no series
/// still gets its HELP and TYPE, so a scraper sees the name exists.
fn family(
    out: &mut String,
    name: &str,
    kind: &str,
    help: &str,
    series: impl Iterator<Item = (String, f64)>,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (labels, v) in series {
        let _ = writeln!(out, "{name}{{{labels}}} {v}");
    }
}

fn container_labels(c: &ContainerStatsInfo) -> String {
    format!(
        "container=\"{}\",id=\"{}\",namespace=\"{}\",pod=\"{}\"",
        escape(&c.name),
        escape(&c.container_id),
        escape(&c.namespace),
        escape(&c.pod)
    )
}

fn network_labels(p: &PodNetworkStats, iface: &str) -> String {
    format!(
        "container=\"\",id=\"{}\",interface=\"{}\",namespace=\"{}\",pod=\"{}\"",
        escape(&p.sandbox_id),
        escape(iface),
        escape(&p.namespace),
        escape(&p.pod)
    )
}

/// A label value in the text format: backslash, quote and newline escaped.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// The interfaces in a `/proc/<pid>/net/dev`, without loopback.
///
/// Loopback is left out as cAdvisor leaves it out: traffic a pod sends itself
/// is not traffic on the node's network.
pub fn parse_net_dev(text: &str) -> Vec<crate::cri::InterfaceStats> {
    text.lines()
        .skip(2)
        .filter_map(|line| {
            let (name, rest) = line.split_once(':')?;
            let name = name.trim();
            if name == "lo" {
                return None;
            }
            let f: Vec<u64> = rest.split_whitespace().filter_map(|v| v.parse().ok()).collect();
            // Receive: bytes packets errs drop fifo frame compressed multicast,
            // then transmit: bytes ...
            Some(crate::cri::InterfaceStats {
                name: name.to_string(),
                rx_bytes: *f.first()?,
                tx_bytes: *f.get(8)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cri::InterfaceStats;

    fn stats(name: &str, cpu: Option<u64>, mem: Option<u64>, fs: Option<u64>) -> ContainerStatsInfo {
        ContainerStatsInfo {
            container_id: format!("c-{name}"),
            name: name.into(),
            pod: "web".into(),
            namespace: "default".into(),
            cpu_usage_core_nanos: cpu,
            memory_working_set_bytes: mem,
            fs_usage_bytes: fs,
        }
    }

    #[test]
    fn cadvisor_series_carry_upstreams_names_and_labels() {
        let out = render_cadvisor(
            &[stats("app", Some(2_500_000_000), Some(1024), Some(4096))],
            &[PodNetworkStats {
                sandbox_id: "sb-1".into(),
                pod: "web".into(),
                namespace: "default".into(),
                interfaces: vec![InterfaceStats { name: "eth0".into(), rx_bytes: 10, tx_bytes: 20 }],
            }],
        );
        let l = r#"container="app",id="c-app",namespace="default",pod="web""#;
        assert!(out.contains(&format!("container_cpu_usage_seconds_total{{{l}}} 2.5\n")), "{out}");
        assert!(out.contains(&format!("container_memory_working_set_bytes{{{l}}} 1024\n")), "{out}");
        assert!(out.contains(&format!("container_fs_usage_bytes{{{l}}} 4096\n")), "{out}");
        let n = r#"container="",id="sb-1",interface="eth0",namespace="default",pod="web""#;
        assert!(out.contains(&format!("container_network_receive_bytes_total{{{n}}} 10\n")), "{out}");
        assert!(out.contains(&format!("container_network_transmit_bytes_total{{{n}}} 20\n")), "{out}");
        assert!(out.contains("# TYPE container_cpu_usage_seconds_total counter\n"));
        assert!(out.contains("# TYPE container_memory_working_set_bytes gauge\n"));
    }

    #[test]
    fn a_number_the_runtime_did_not_report_is_no_series_not_a_zero() {
        let out = render_cadvisor(&[stats("app", Some(1), None, None)], &[]);
        assert!(out.contains("container_cpu_usage_seconds_total{"));
        assert!(!out.contains("container_memory_working_set_bytes{"), "{out}");
        assert!(!out.contains("container_fs_usage_bytes{"), "{out}");
        // The family is still declared.
        assert!(out.contains("# TYPE container_fs_usage_bytes gauge"));
    }

    #[test]
    fn label_values_are_escaped() {
        let mut s = stats("a\"b", Some(1), None, None);
        s.pod = "p\\q".into();
        let out = render_cadvisor(&[s], &[]);
        assert!(out.contains(r#"container="a\"b""#), "{out}");
        assert!(out.contains(r#"pod="p\\q""#), "{out}");
    }

    #[test]
    fn net_dev_is_parsed_without_loopback() {
        let text = "Inter-|   Receive                                                |  Transmit\n \
                    face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    \
                    lo:     100       1    0    0    0     0          0         0      100       1    0    0    0     0       0          0\n  \
                    eth0:    5000      40    0    0    0     0          0         0     7000      50    0    0    0     0       0          0\n";
        let got = parse_net_dev(text);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "eth0");
        assert_eq!(got[0].rx_bytes, 5000);
        assert_eq!(got[0].tx_bytes, 7000);
    }

    #[test]
    fn kubelet_metrics_render_under_upstreams_names() {
        observe_pod_start(1.5);
        observe_relist(0.02);
        let out = render_kubelet(&KubeletSnapshot {
            running_pods: 2,
            containers: vec![ContainerState::Running, ContainerState::Running, ContainerState::Exited],
        });
        assert!(out.contains("kubelet_running_pods 2"), "{out}");
        assert!(out.contains(r#"kubelet_running_containers{container_state="running"} 2"#), "{out}");
        assert!(out.contains(r#"kubelet_running_containers{container_state="exited"} 1"#), "{out}");
        assert!(out.contains(r#"kubelet_running_containers{container_state="created"} 0"#), "{out}");
        // Histograms with buckets, not summaries.
        assert!(out.contains(r#"kubelet_pod_start_duration_seconds_bucket{le="2"}"#), "{out}");
        assert!(out.contains(r#"kubelet_pleg_relist_duration_seconds_bucket{le="0.025"}"#), "{out}");
        assert!(out.contains(r#"component="kubelet""#), "{out}");
        assert!(out.contains(&format!(r#"gitVersion="{}""#, env!("CARGO_PKG_VERSION"))), "{out}");
        #[cfg(target_os = "linux")]
        {
            assert!(out.contains("process_resident_memory_bytes"), "{out}");
            assert!(out.contains("process_start_time_seconds"), "{out}");
        }
    }
}
