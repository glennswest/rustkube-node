//! The node's own usage and its machine, from the cadvisor library (#21).
//!
//! Upstream split cAdvisor in v0.60 into a kubelet-facing library that owns
//! node, machine and filesystem stats, while container stats come from the
//! CRI. glennswest/cadvisor's `cadvisor-host` is that library here: cgroup v2,
//! procfs and sysfs readers with google/cadvisor's semantics.
//!
//! - **Node CPU** is the root cgroup's `cpu.stat` (`usage_usec`), what
//!   upstream's summary reports for the node, rather than the sum of the
//!   containers this kubelet knows of (which left out every node service).
//! - **Node memory**: the root cgroup has no `memory.current` on cgroup v2, so
//!   usage is `MemTotal − MemFree` and the working set is usage minus inactive
//!   file pages, as cAdvisor computes the root container's.
//! - **memory.available** for MemoryPressure is `MemTotal − working set`,
//!   upstream's eviction signal (`memory.available < 100Mi`).
//! - **Machine**: cores, sockets, memory and filesystems, read once.

use cadvisor_model::GoTime;

/// The node's usage now. `None` fields were not readable here.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeUsage {
    pub cpu_usage_ns: Option<u64>,
    pub memory_usage_bytes: Option<u64>,
    pub memory_working_set_bytes: Option<u64>,
    pub memory_rss_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

/// The machine, as `machine_*` and the node status report it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Machine {
    pub cores: i64,
    pub physical_cores: i64,
    pub sockets: i64,
    pub memory_bytes: u64,
    pub cpu_frequency_khz: u64,
}

fn meminfo_kib(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|l| {
        let rest = l.strip_prefix(key)?.strip_prefix(':')?;
        rest.trim().trim_end_matches("kB").trim().parse().ok()
    })
}

fn stat_value(memory_stat: &str, key: &str) -> Option<u64> {
    memory_stat.lines().find_map(|l| {
        let (k, v) = l.split_once(' ')?;
        (k == key).then(|| v.trim().parse().ok()).flatten()
    })
}

/// The root's memory, from `/proc/meminfo` and the root cgroup's
/// `memory.stat` (when the kernel has one there): usage, working set, rss,
/// available, page faults.
pub fn root_memory(meminfo: &str, root_memory_stat: Option<&str>) -> NodeUsage {
    let kib = |k: &str| meminfo_kib(meminfo, k).map(|v| v * 1024);
    let (Some(total), Some(free)) = (kib("MemTotal"), kib("MemFree")) else {
        return NodeUsage::default();
    };
    let usage = total.saturating_sub(free);
    let stat = |k: &str| root_memory_stat.and_then(|s| stat_value(s, k));
    let inactive_file = stat("inactive_file").or_else(|| kib("Inactive(file)")).unwrap_or(0);
    let working_set = usage.saturating_sub(inactive_file);
    NodeUsage {
        memory_usage_bytes: Some(usage),
        memory_working_set_bytes: Some(working_set),
        memory_rss_bytes: stat("anon").or_else(|| kib("AnonPages")),
        memory_available_bytes: Some(total.saturating_sub(working_set)),
        page_faults: stat("pgfault"),
        major_page_faults: stat("pgmajfault"),
        cpu_usage_ns: None,
    }
}

/// The node's usage, read now (blocking: a handful of small files).
#[cfg(target_os = "linux")]
pub fn read_usage() -> NodeUsage {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let stat = std::fs::read_to_string(format!("{}/memory.stat", cadvisor_host::cgroup::UNIFIED_MOUNTPOINT)).ok();
    let mut u = root_memory(&meminfo, stat.as_deref());
    let reader = cadvisor_host::cgroup::CgroupReader::default();
    u.cpu_usage_ns = reader
        .read_stats("/", GoTime::now())
        .ok()
        .map(|s| s.cpu.usage.total)
        .filter(|n| *n > 0);
    u
}

#[cfg(not(target_os = "linux"))]
pub fn read_usage() -> NodeUsage {
    NodeUsage::default()
}

/// The machine, read once (it does not change while the node runs).
pub fn machine() -> Option<&'static Machine> {
    static MACHINE: std::sync::OnceLock<Option<Machine>> = std::sync::OnceLock::new();
    MACHINE.get_or_init(read_machine).as_ref()
}

#[cfg(target_os = "linux")]
fn read_machine() -> Option<Machine> {
    let fs = cadvisor_host::fs::FsService::new().ok()?;
    match cadvisor_host::machine::machine_info(&fs, GoTime::now()) {
        Ok(m) => Some(Machine {
            cores: m.num_cores,
            physical_cores: m.num_physical_cores,
            sockets: m.num_sockets,
            memory_bytes: m.memory_capacity,
            cpu_frequency_khz: m.cpu_frequency,
        }),
        Err(e) => {
            tracing::warn!("machine info unreadable (machine_* not reported): {e}");
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn read_machine() -> Option<Machine> {
    None
}

/// The node's own families for `/metrics/cadvisor`: `machine_*`, and the root
/// container's memory usage and rss (its CPU and working set are in
/// `metrics::render_cadvisor_with_node`'s families, beside the containers').
pub fn render(usage: &NodeUsage, machine: Option<&Machine>) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let mut one = |name: &str, kind: &str, help: &str, labels: &str, v: Option<f64>| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        if let Some(v) = v {
            let _ = writeln!(out, "{name}{labels} {v}");
        }
    };
    let m = machine;
    one("machine_cpu_cores", "gauge", "Number of logical CPU cores.", "", m.map(|m| m.cores as f64));
    one("machine_cpu_physical_cores", "gauge", "Number of physical CPU cores.", "", m.map(|m| m.physical_cores as f64));
    one("machine_cpu_sockets", "gauge", "Number of CPU sockets.", "", m.map(|m| m.sockets as f64));
    one("machine_memory_bytes", "gauge", "Amount of memory installed on the machine.", "", m.map(|m| m.memory_bytes as f64));
    one("machine_cpu_frequency_khz", "gauge", "CPU maximum frequency in kHz.", "", m.map(|m| m.cpu_frequency_khz as f64));
    let root = r#"{container="",id="/",namespace="",pod=""}"#;
    one("container_memory_usage_bytes", "gauge", "Current memory usage in bytes, including all memory regardless of when it was accessed.", root,
        usage.memory_usage_bytes.map(|b| b as f64));
    one("container_memory_rss", "gauge", "Size of RSS in bytes.", root, usage.memory_rss_bytes.map(|b| b as f64));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMINFO: &str = "MemTotal:       16000000 kB\nMemFree:         2000000 kB\nMemAvailable:    9000000 kB\nInactive(file):  5000000 kB\nAnonPages:       3000000 kB\n";

    /// The root's memory as cAdvisor computes it: usage = total − free,
    /// working set = usage − inactive file, available = total − working set.
    #[test]
    fn the_roots_working_set_is_usage_less_inactive_file() {
        let stat = "anon 3145728000\nfile 6000000000\ninactive_file 5242880000\npgfault 77\npgmajfault 3\n";
        let u = root_memory(MEMINFO, Some(stat));
        let total = 16_000_000u64 * 1024;
        let usage = total - 2_000_000 * 1024;
        assert_eq!(u.memory_usage_bytes, Some(usage));
        assert_eq!(u.memory_working_set_bytes, Some(usage - 5_242_880_000));
        assert_eq!(u.memory_available_bytes, Some(total - (usage - 5_242_880_000)));
        assert_eq!(u.memory_rss_bytes, Some(3_145_728_000));
        assert_eq!((u.page_faults, u.major_page_faults), (Some(77), Some(3)));

        // No root memory.stat: meminfo's own Inactive(file) and AnonPages.
        let u = root_memory(MEMINFO, None);
        assert_eq!(u.memory_working_set_bytes, Some(usage - 5_000_000 * 1024));
        assert_eq!(u.memory_rss_bytes, Some(3_000_000 * 1024));
        assert_eq!(u.page_faults, None, "unknown is absent, never 0");

        assert_eq!(root_memory("", None), NodeUsage::default(), "nothing readable: nothing claimed");
    }

    #[test]
    fn the_node_series_are_rendered_and_unknown_ones_left_out() {
        let u = NodeUsage { cpu_usage_ns: Some(2_500_000_000), memory_working_set_bytes: Some(1024), ..Default::default() };
        let m = Machine { cores: 8, physical_cores: 4, sockets: 1, memory_bytes: 16 << 30, cpu_frequency_khz: 3_000_000 };
        let text = render(&u, Some(&m));
        assert!(text.contains("machine_cpu_cores 8\n"), "{text}");
        assert!(text.contains("machine_memory_bytes 17179869184\n"));
        assert!(!text.contains("container_cpu_usage_seconds_total"), "that family is metrics.rs's");
        let u = NodeUsage { memory_usage_bytes: Some(4096), ..u };
        let text = render(&u, Some(&m));
        assert!(text.contains(r#"container_memory_usage_bytes{container="",id="/",namespace="",pod=""} 4096"#), "{text}");
        assert!(!text.contains("container_memory_rss{"), "an unknown value has no series: {text}");
        let all = crate::metrics::render_cadvisor_with_node(&[], &[], Some(&u));
        assert!(all.contains(r#"container_cpu_usage_seconds_total{container="",id="/",namespace="",pod=""} 2.5"#), "{all}");
        assert!(all.contains(r#"container_memory_working_set_bytes{container="",id="/",namespace="",pod=""} 1024"#));
        assert_eq!(all.matches("# TYPE container_cpu_usage_seconds_total").count(), 1, "one family header");
        assert!(text.contains("# TYPE container_memory_rss gauge"), "but its family is named");
        assert!(!render(&NodeUsage::default(), None).contains("machine_cpu_cores "), "no machine, no value");
    }
}
