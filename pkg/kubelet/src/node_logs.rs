//! The logs of the node's own services, for `kubectl logs` on their mirror
//! pods (#72).
//!
//! A node service is mirrored as `kube-system/<asset>-<node>` (`mirror.rs`),
//! and nothing about it is a pod this kubelet runs: its output is not under
//! `/var/log/pods`. A service run by stormd keeps it on its own log volume:
//! the boot manifest says so, in two lines of the service's unit —
//!
//! ```text
//! volume  felogs  /logs/fastetcd
//! spec    fastetcd
//!   mount     felogs /var/log/stormd
//! ```
//!
//! — so the volume is a host path, and the kubelet sees the host at
//! `/hostroot`. The manifest is read rather than the path assumed, because the
//! volume's path is the unit's choice, not a rule.
//!
//! In that directory stormd writes, per process it supervises:
//!
//! - `<proc>.log`: the run in progress;
//! - `<proc>.<N>.log`: its rotations, 1 the newest;
//! - `<proc>.<YYYYMMDDTHHMMSS>.failed.log` / `.exited.log`: finished runs.
//!
//! Each line is `<rfc3339> <stream> <severity> <message>`: four fields, as the
//! CRI format is, so the log options (`sinceTime`, `timestamps`, …) apply to it
//! unchanged. A service whose stormd runs several processes (the kubelet's own
//! runs the kubelet and kube-proxy) has one container in its mirror pod, so its
//! current log is every process's, merged in time order, each line marked with
//! the process it came from.
//!
//! A service not run by stormd (stormblock, the registry) has no such volume.
//! Its output is stormpump's `w<id>.log`, and `assets.json` does not say which
//! id is whose, so it cannot be served from here yet.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Where the kubelet sees the host's root.
pub const HOST_ROOT: &str = "/hostroot";

/// Where PID 1's boot units are, under the host root.
const BOOT_D: &str = "etc/stormpump/boot.d";

/// Where stormd writes its logs inside a service's container.
const STORMD_LOG_DIR: &str = "/var/log/stormd";

/// The host path of each spec's `/var/log/stormd`, from boot units read in
/// order.
///
/// Units are read in lexical order and share one namespace of volumes (a
/// volume declared in `15-stormcert` is mounted by `30-kube`), so `volumes`
/// carries across calls. A line that is not a `volume`, `spec` or `mount` is
/// someone else's business and skipped.
pub fn parse_unit(
    text: &str,
    volumes: &mut HashMap<String, String>,
    log_dirs: &mut HashMap<String, String>,
) {
    let mut spec: Option<String> = None;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("");
        let words: Vec<&str> = line.split_whitespace().collect();
        match words.as_slice() {
            ["volume", name, path] => {
                volumes.insert((*name).to_string(), (*path).to_string());
                spec = None;
            }
            ["spec", name] => spec = Some((*name).to_string()),
            ["start", ..] => spec = None,
            ["mount", vol, dst, ..] if *dst == STORMD_LOG_DIR => {
                if let (Some(s), Some(path)) = (&spec, volumes.get(*vol)) {
                    log_dirs.insert(s.clone(), path.clone());
                }
            }
            _ => {}
        }
    }
}

/// The directory holding `asset`'s stormd logs, as the kubelet sees it.
///
/// `None` when no boot unit mounts a volume at the asset's `/var/log/stormd`:
/// a service not run by stormd, or no such asset.
pub fn log_dir(root: &Path, asset: &str) -> Option<PathBuf> {
    let mut units: Vec<PathBuf> = std::fs::read_dir(root.join(BOOT_D))
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_file())
        .collect();
    units.sort();
    let (mut volumes, mut dirs) = (HashMap::new(), HashMap::new());
    for u in units {
        if let Ok(text) = std::fs::read_to_string(&u) {
            parse_unit(&text, &mut volumes, &mut dirs);
        }
    }
    let host = dirs.remove(asset)?;
    Some(root.join(host.trim_start_matches('/')))
}

/// What a log file in a stormd log directory is.
#[derive(Debug, PartialEq)]
enum Kind {
    /// `<proc>.log`
    Live,
    /// `<proc>.<N>.log`
    Rotation(u32),
    /// `<proc>.<run>.failed.log`
    Failed(String),
    /// `<proc>.<run>.exited.log`
    Exited,
}

/// A file name, as a process and what the file is.
fn classify(name: &str) -> Option<(String, Kind)> {
    let stem = name.strip_suffix(".log")?;
    if stem.is_empty() || stem.starts_with('.') {
        return None;
    }
    let parts: Vec<&str> = stem.split('.').collect();
    match parts.as_slice() {
        [p] => Some(((*p).to_string(), Kind::Live)),
        [p @ .., n] if !p.is_empty() && n.parse::<u32>().is_ok() => {
            Some((p.join("."), Kind::Rotation(n.parse().ok()?)))
        }
        [p @ .., run, "failed"] if !p.is_empty() => {
            Some((p.join("."), Kind::Failed((*run).to_string())))
        }
        [p @ .., _, "exited"] if !p.is_empty() => Some((p.join("."), Kind::Exited)),
        _ => None,
    }
}

/// The files of the run in progress, per process: rotations oldest first,
/// then the live file. Processes in name order.
pub fn current_files(dir: &Path) -> Vec<(String, Vec<PathBuf>)> {
    let mut by_proc: HashMap<String, (Vec<(u32, PathBuf)>, Option<PathBuf>)> = HashMap::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    for e in rd.flatten() {
        let Some(name) = e.file_name().to_str().map(str::to_owned) else { continue };
        match classify(&name) {
            Some((p, Kind::Live)) => by_proc.entry(p).or_default().1 = Some(e.path()),
            Some((p, Kind::Rotation(n))) => by_proc.entry(p).or_default().0.push((n, e.path())),
            _ => {}
        }
    }
    let mut out: Vec<(String, Vec<PathBuf>)> = by_proc
        .into_iter()
        .map(|(p, (mut rot, live))| {
            // 1 is the newest rotation, so the oldest is the highest number.
            rot.sort_by(|a, b| b.0.cmp(&a.0));
            let mut files: Vec<PathBuf> = rot.into_iter().map(|(_, f)| f).collect();
            files.extend(live);
            (p, files)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// The live file of each process: what `follow` polls.
pub fn live_files(dir: &Path) -> Vec<(String, PathBuf)> {
    current_files(dir)
        .into_iter()
        .filter_map(|(p, files)| {
            let last = files.last()?;
            (classify(last.file_name()?.to_str()?) == Some((p.clone(), Kind::Live)))
                .then(|| (p, last.clone()))
        })
        .collect()
}

/// The newest failed run of any process: what `--previous` reads.
///
/// Run ids are UTC timestamps (`20260825T115009`), so the largest is the
/// newest. A clean exit is not a previous run worth reading here: the owner's
/// ask (#72) is the last run that failed.
pub fn previous_failed(dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter_map(|e| match classify(e.file_name().to_str()?) {
            Some((_, Kind::Failed(run))) => Some((run, e.path())),
            _ => None,
        })
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, p)| p)
}

/// Lines from several processes, merged in time order.
///
/// Each input is one process's text. With more than one process, each line's
/// message is marked `[<proc>]`, after the four-field prefix so the log
/// options still find the timestamp. A line whose timestamp does not parse
/// keeps its place after the line before it.
pub fn merge(per_proc: &[(String, String)]) -> String {
    let mark = per_proc.len() > 1;
    let mut lines: Vec<(Option<chrono::DateTime<chrono::Utc>>, usize, usize, String)> = Vec::new();
    for (pi, (proc_name, text)) in per_proc.iter().enumerate() {
        let mut last = None;
        for (li, line) in text.lines().enumerate() {
            let ts = line
                .split(' ')
                .next()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.with_timezone(&chrono::Utc));
            let ts = ts.or(last);
            last = ts;
            let line = if mark { tag(line, proc_name) } else { line.to_string() };
            lines.push((ts, pi, li, line));
        }
    }
    if mark {
        lines.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    }
    lines.into_iter().map(|l| l.3 + "\n").collect()
}

/// `line` with `[proc] ` in front of its message.
fn tag(line: &str, proc_name: &str) -> String {
    let mut it = line.splitn(4, ' ');
    match (it.next(), it.next(), it.next(), it.next()) {
        (Some(ts), Some(st), Some(sev), Some(msg)) => format!("{ts} {st} {sev} [{proc_name}] {msg}"),
        _ => format!("[{proc_name}] {line}"),
    }
}

/// The run in progress, every process, merged; and each live file's length,
/// for `follow` to continue from.
pub fn read_current(dir: &Path) -> (String, HashMap<PathBuf, u64>) {
    let mut texts = Vec::new();
    let mut offsets = HashMap::new();
    for (p, files) in current_files(dir) {
        let mut text = String::new();
        for f in &files {
            if let Ok(t) = std::fs::read_to_string(f) {
                // Whole lines only: the live file may be mid-line.
                let whole = t.rfind('\n').map(|i| i + 1).unwrap_or(0);
                text.push_str(&t[..whole]);
                if Some(f) == files.last() {
                    offsets.insert(f.clone(), whole as u64);
                }
            }
        }
        texts.push((p, text));
    }
    (merge(&texts), offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KUBE: &str = "\
# Each is a stormdbase golden.
volume  fe      /pallets/fastetcd
volume  felogs  /logs/fastetcd
spec    fastetcd
  domain    container
  mount     fedata /data
  mount     felogs /var/log/stormd
  logs      combined
  argv      /stormd

volume  nodelogs   /logs/rustkube-node
spec    rustkube-node
  mount     nodelogs /var/log/stormd   # its own
  mount     hostroot /hostroot
";

    #[test]
    fn a_spec_log_dir_is_the_volume_it_mounts_at_var_log_stormd() {
        let (mut v, mut d) = (HashMap::new(), HashMap::new());
        parse_unit("volume sbrun /run/stormblock\nspec stormblock\n  mount sbrun /run/stormblock\n", &mut v, &mut d);
        parse_unit(KUBE, &mut v, &mut d);
        assert_eq!(d.get("fastetcd").map(String::as_str), Some("/logs/fastetcd"));
        assert_eq!(d.get("rustkube-node").map(String::as_str), Some("/logs/rustkube-node"));
        // Not run by stormd: no log volume, so nothing to serve.
        assert!(!d.contains_key("stormblock"));
    }

    #[test]
    fn a_volume_declared_in_an_earlier_unit_resolves() {
        let root = tempfile::tempdir().unwrap();
        let bd = root.path().join(BOOT_D);
        std::fs::create_dir_all(&bd).unwrap();
        std::fs::write(bd.join("15-cert"), "volume  certlogs /logs/stormcert\n").unwrap();
        std::fs::write(bd.join("30-kube"), "spec stormcert\n  mount certlogs /var/log/stormd ro\n").unwrap();
        assert_eq!(log_dir(root.path(), "stormcert"), Some(root.path().join("logs/stormcert")));
        assert_eq!(log_dir(root.path(), "registry"), None);
    }

    #[test]
    fn files_are_told_apart() {
        assert_eq!(classify("etcd.log"), Some(("etcd".into(), Kind::Live)));
        assert_eq!(classify("etcd.2.log"), Some(("etcd".into(), Kind::Rotation(2))));
        assert_eq!(
            classify("etcd.20260825T120000.failed.log"),
            Some(("etcd".into(), Kind::Failed("20260825T120000".into())))
        );
        assert_eq!(classify("etcd.20260825T100000.exited.log"), Some(("etcd".into(), Kind::Exited)));
        assert_eq!(classify(".cloudid"), None);
        assert_eq!(classify("notes.txt"), None);
    }

    #[test]
    fn the_current_run_is_rotations_oldest_first_then_the_live_file() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(d.join("etcd.2.log"), "2026-09-28T10:00:00.000Z stdout info a\n").unwrap();
        std::fs::write(d.join("etcd.1.log"), "2026-09-28T10:00:01.000Z stdout info b\n").unwrap();
        std::fs::write(d.join("etcd.log"), "2026-09-28T10:00:02.000Z stdout info c\npart").unwrap();
        std::fs::write(d.join("etcd.20260827T000000.failed.log"), "old\n").unwrap();
        let (text, offsets) = read_current(d);
        // One process: no marks, and the partial line waits for its newline.
        assert_eq!(
            text,
            "2026-09-28T10:00:00.000Z stdout info a\n\
             2026-09-28T10:00:01.000Z stdout info b\n\
             2026-09-28T10:00:02.000Z stdout info c\n"
        );
        assert_eq!(offsets.get(&d.join("etcd.log")), Some(&39));
        assert_eq!(live_files(d), vec![("etcd".to_string(), d.join("etcd.log"))]);
    }

    #[test]
    fn several_processes_merge_in_time_order_marked() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::write(
            d.join("kubelet.log"),
            "2026-09-28T10:00:00.000Z stdout info k1\n2026-09-28T10:00:02.000Z stderr warn k2\n",
        )
        .unwrap();
        std::fs::write(d.join("kube-proxy.log"), "2026-09-28T10:00:01.000Z stdout info p1\n").unwrap();
        let (text, _) = read_current(d);
        assert_eq!(
            text,
            "2026-09-28T10:00:00.000Z stdout info [kubelet] k1\n\
             2026-09-28T10:00:01.000Z stdout info [kube-proxy] p1\n\
             2026-09-28T10:00:02.000Z stderr warn [kubelet] k2\n"
        );
    }

    #[test]
    fn previous_is_the_newest_failed_run_not_a_clean_exit() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        assert_eq!(previous_failed(d), None);
        std::fs::write(d.join("etcd.20260827T000000.failed.log"), "old\n").unwrap();
        std::fs::write(d.join("etcd.20260828T000000.failed.log"), "new\n").unwrap();
        std::fs::write(d.join("etcd.20260829T000000.exited.log"), "clean\n").unwrap();
        std::fs::write(d.join("etcd.log"), "live\n").unwrap();
        assert_eq!(previous_failed(d), Some(d.join("etcd.20260828T000000.failed.log")));
    }
}
