//! Who each stormpump workload is, for cadvisor (#84).
//!
//! On stormcos a pod's containers are stormpump workloads, each an opaque
//! cgroup (`/sys/fs/cgroup/stormpump/w<tag>-<n>`). cadvisor finds those cgroups
//! but knows a container's pod only from containerd's or CRI-O's API, and
//! stormcos runs neither, so every series it exported had an `id` and nothing
//! else (cadvisor#3). The identity is the kubelet's alone.
//!
//! **One JSON file per workload**, named after its cgroup, in [`DIR`] (the
//! node's `/run`, so gone with a reboot like the cgroups are):
//!
//! ```json
//! {"cgroup": "/stormpump/w4398046511104-7", "pid": 4242, "kind": "container",
//!  "reports_network": false, "namespace": "default", "pod": "web-1", "pod_uid": "…",
//!  "container": "app", "container_id": "ct-0000002c", "image": "busybox",
//!  "labels": {"io.kubernetes.pod.name": "web-1", …}, "annotations": {…}}
//! ```
//!
//! `kind` is `container`, or `sandbox` for the process holding a pod's
//! namespaces: the one that `reports_network`, as a CRI sandbox (pause)
//! container is, so a pod's counters are not repeated by every container of it.
//! `pid` is the workload's init process, for `/proc/<pid>/net/dev` inside its
//! namespace. Written atomically when a container starts or a pod-network
//! sandbox is made, removed when it is removed; files whose pid no longer runs
//! in that cgroup are swept when the kubelet starts. cadvisor watches the
//! directory as it watches cgroups.
//!
//! The engine does not report a workload's cgroup; its pid does
//! (`/proc/<pid>/cgroup`, cgroup v2's `0::<path>`), and the kubelet shares the
//! host's pid namespace.

use serde::Serialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Where the records live.
pub const DIR: &str = "/run/rustkube/workloads";

/// One workload's identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Record {
    /// The cgroup v2 path, as `/proc/<pid>/cgroup` names it.
    pub cgroup: String,
    pub pid: i32,
    /// `container` or `sandbox`.
    pub kind: String,
    /// The pod's network is counted from this one alone.
    pub reports_network: bool,
    pub namespace: String,
    pub pod: String,
    pub pod_uid: String,
    pub container: String,
    pub container_id: String,
    pub image: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

impl Record {
    /// The CRI labels cadvisor maps to `container_label_*`, upstream's names,
    /// over the container's own.
    pub fn with_kubernetes_labels(mut self) -> Self {
        for (k, v) in [
            ("io.kubernetes.pod.name", &self.pod),
            ("io.kubernetes.pod.namespace", &self.namespace),
            ("io.kubernetes.pod.uid", &self.pod_uid),
        ] {
            self.labels.insert(k.to_string(), v.clone());
        }
        if self.kind == "container" {
            self.labels.insert("io.kubernetes.container.name".into(), self.container.clone());
        } else {
            // What containerd marks a pause container with.
            self.labels.insert("io.cri-containerd.kind".into(), "sandbox".into());
        }
        self
    }
}

/// A process's cgroup v2 path, from `/proc/<pid>/cgroup`.
pub fn cgroup_of(pid: i32) -> Option<String> {
    if pid <= 0 {
        return None;
    }
    parse_cgroup(&std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?)
}

/// The unified hierarchy's line (`0::<path>`) of a `/proc/<pid>/cgroup`.
pub fn parse_cgroup(text: &str) -> Option<String> {
    text.lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(str::trim)
        .filter(|p| p.starts_with('/') && *p != "/")
        .map(str::to_string)
}

/// The file a cgroup's record is in: its last component, which is unique on
/// the node (stormpump names never repeat while an engine runs).
pub fn file_name(cgroup: &str) -> Option<String> {
    let base = cgroup.rsplit('/').next()?;
    (!base.is_empty() && base != "." && base != "..").then(|| format!("{base}.json"))
}

/// Writes and removes the records.
#[derive(Debug, Clone)]
pub struct Publisher {
    dir: PathBuf,
}

impl Default for Publisher {
    fn default() -> Self {
        Self::at(DIR)
    }
}

impl Publisher {
    pub fn at(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Write a record (tmp + rename, so a reader never sees half of one).
    /// `None` when the record has no usable cgroup; an error is logged, never
    /// a failed start: this is for observers.
    pub fn publish(&self, record: &Record) -> Option<PathBuf> {
        let name = file_name(&record.cgroup)?;
        let path = self.dir.join(&name);
        let tmp = self.dir.join(format!(".{name}.tmp"));
        let body = match serde_json::to_vec(record) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!("workload identity not encoded: {e}");
                return None;
            }
        };
        let written = std::fs::create_dir_all(&self.dir)
            .and_then(|()| std::fs::write(&tmp, &body))
            .and_then(|()| std::fs::rename(&tmp, &path));
        match written {
            Ok(()) => Some(path),
            Err(e) => {
                tracing::warn!(path = %path.display(), "workload identity not written (cadvisor sees an unnamed cgroup): {e}");
                let _ = std::fs::remove_file(&tmp);
                None
            }
        }
    }

    /// Remove a record; already gone is fine.
    pub fn withdraw(&self, path: &Path) {
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::debug!(path = %path.display(), "workload identity not removed: {e}");
            }
        }
    }

    /// Remove the records whose workload is gone: its pid no longer runs in
    /// that cgroup (`cgroup_of` asks the system). How many went.
    pub fn sweep(&self, cgroup_of: impl Fn(i32) -> Option<String>) -> usize {
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return 0 };
        let mut gone = 0;
        for e in entries.flatten() {
            let path = e.path();
            if path.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let alive = std::fs::read(&path)
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .is_some_and(|v| {
                    let pid = v["pid"].as_i64().unwrap_or(0) as i32;
                    v["cgroup"].as_str().is_some_and(|c| cgroup_of(pid).as_deref() == Some(c))
                });
            if !alive {
                self.withdraw(&path);
                gone += 1;
            }
        }
        gone
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unified_cgroup_line_names_the_workload() {
        assert_eq!(parse_cgroup("0::/stormpump/w4398046511104-7\n").as_deref(), Some("/stormpump/w4398046511104-7"));
        // cgroup v1 lines beside it are ignored.
        assert_eq!(parse_cgroup("12:cpu:/x\n0::/stormpump/w1-2\n").as_deref(), Some("/stormpump/w1-2"));
        assert_eq!(parse_cgroup("0::/\n"), None, "the root is nobody's");
        assert_eq!(parse_cgroup("12:cpu:/x\n"), None);
        assert_eq!(file_name("/stormpump/w1-2").as_deref(), Some("w1-2.json"));
        assert_eq!(file_name("/"), None);
    }

    #[test]
    fn a_container_and_its_sandbox_carry_upstreams_labels() {
        let c = Record {
            kind: "container".into(), namespace: "ns".into(), pod: "web".into(), pod_uid: "u".into(),
            container: "app".into(), ..Default::default()
        }
        .with_kubernetes_labels();
        assert_eq!(c.labels["io.kubernetes.pod.name"], "web");
        assert_eq!(c.labels["io.kubernetes.pod.namespace"], "ns");
        assert_eq!(c.labels["io.kubernetes.container.name"], "app");
        let s = Record { kind: "sandbox".into(), pod: "web".into(), ..Default::default() }.with_kubernetes_labels();
        assert_eq!(s.labels["io.cri-containerd.kind"], "sandbox");
        assert!(!s.labels.contains_key("io.kubernetes.container.name"));
    }

    /// Published whole, found by its cgroup's name, removed with the workload,
    /// and swept once its pid no longer runs in that cgroup.
    #[test]
    fn records_are_written_removed_and_swept() {
        let dir = tempfile::tempdir().unwrap();
        let p = Publisher::at(dir.path().join("workloads"));
        let live = Record { cgroup: "/stormpump/w1-1".into(), pid: 11, kind: "container".into(), pod: "a".into(), ..Default::default() };
        let gone = Record { cgroup: "/stormpump/w1-2".into(), pid: 12, kind: "container".into(), pod: "b".into(), ..Default::default() };
        let reused = Record { cgroup: "/stormpump/w1-3".into(), pid: 13, kind: "sandbox".into(), reports_network: true, ..Default::default() };
        let a = p.publish(&live).unwrap();
        p.publish(&gone).unwrap();
        p.publish(&reused).unwrap();
        assert!(a.ends_with("w1-1.json"));
        let read: serde_json::Value = serde_json::from_slice(&std::fs::read(&a).unwrap()).unwrap();
        assert_eq!((read["pid"].as_i64(), read["pod"].as_str()), (Some(11), Some("a")));
        assert!(p.publish(&Record { cgroup: "/".into(), ..Default::default() }).is_none());

        // pid 11 is still in its cgroup; 12 is gone; 13 is now someone else's.
        let swept = p.sweep(|pid| match pid {
            11 => Some("/stormpump/w1-1".into()),
            13 => Some("/stormpump/w9-9".into()),
            _ => None,
        });
        assert_eq!(swept, 2);
        let left: Vec<_> = std::fs::read_dir(dir.path().join("workloads")).unwrap().flatten().map(|e| e.file_name()).collect();
        assert_eq!(left, vec![std::ffi::OsString::from("w1-1.json")]);
        p.withdraw(&a);
        p.withdraw(&a);
        assert!(std::fs::read_dir(dir.path().join("workloads")).unwrap().next().is_none());
    }
}
