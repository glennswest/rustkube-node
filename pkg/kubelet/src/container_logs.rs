//! Each container's own log files, numbered by run and rotated as upstream
//! does (#216).
//!
//! **Layout** (upstream's, `kuberuntime`): a pod's log directory
//! `/var/log/pods/<ns>_<pod>_<uid>/` holds one directory per container, init
//! containers and native sidecars included, and in it one file per run of that
//! container: `0.log`, `1.log`, … The highest number is the current run;
//! `kubectl logs --previous` reads the one below it, and `previous=N` (#131)
//! the run N back.
//!
//! **Numbering.** A new run takes the next number after every run already on
//! disk ([`next_run`]), whatever started it: an app container's restart, an
//! init container retried after a failure, a sidecar restarted, or any of them
//! after a kubelet restart that forgot its restart counts. Before #216 init
//! containers and sidecars always wrote `0.log`, so a retried init had no
//! previous run to read.
//!
//! **Retention.** Upstream keeps one dead instance per container
//! (`--maximum-dead-containers-per-container`, default 1) and its log goes
//! with it. Here the current run and the [`KEEP_PREVIOUS`] runs before it are
//! kept (the console reads the last five, #131); older runs, with their
//! rotations, are deleted when a new run starts ([`prune_runs`]). Everything
//! goes with the pod's directory when the pod is deleted.
//!
//! **Rotation** (upstream's `ContainerLogManager`): every
//! [`MONITOR_INTERVAL`], the current run's file of each container that is
//! larger than `--container-log-max-size` (default 10Mi) is rotated to
//! `<N>.log.<YYYYMMDD-HHMMSS>`; rotations older than the newest are gzip'd
//! (`.gz`); and there are at most `--container-log-max-files` (default 5)
//! files for the run, the live one included, the oldest rotations deleted.
//! `kubectl logs` reads the live file, as upstream.
//!
//! Upstream renames the file and asks the runtime to reopen it
//! (`ReopenContainerLog`). stormpump holds the container's O_APPEND
//! descriptor and has no reopen (stormpump#129), so the file is copied and
//! then truncated: O_APPEND puts the next write at offset 0. A line written in
//! the instant between the copy and the truncate is lost; the copy is redone
//! while the file grows under it, so the window is a few syscalls wide.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, warn};

/// Upstream's `containerLogMaxSize` default: 10Mi.
pub const DEFAULT_MAX_SIZE: u64 = 10 << 20;
/// Upstream's `containerLogMaxFiles` default.
pub const DEFAULT_MAX_FILES: usize = 5;
/// Upstream's `containerLogMonitorInterval` default.
pub const MONITOR_INTERVAL: Duration = Duration::from_secs(10);
/// Previous runs kept beside the current one (#131's console window).
pub const KEEP_PREVIOUS: usize = 5;

/// Rotation limits (`--container-log-max-size`, `--container-log-max-files`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rotation {
    pub max_size: u64,
    /// Files per run, the live one included. At least 2, as upstream requires.
    pub max_files: usize,
}

impl Default for Rotation {
    fn default() -> Self {
        Rotation { max_size: DEFAULT_MAX_SIZE, max_files: DEFAULT_MAX_FILES }
    }
}

/// The run numbers in a container's log directory (`<N>.log`), ascending.
pub fn runs(dir: &Path) -> Vec<u32> {
    let mut runs: Vec<u32> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".log")).and_then(|n| n.parse().ok()))
                .collect()
        })
        .unwrap_or_default();
    runs.sort_unstable();
    runs
}

/// The number for a new run: after every run on disk, and not below
/// `at_least` (the container's restart count, when the caller knows it).
pub fn next_run(dir: &Path, at_least: u32) -> u32 {
    runs(dir).last().map_or(0, |n| n + 1).max(at_least)
}

/// Delete the runs before the newest `keep_previous + 1`, with their
/// rotations. Returns what was deleted.
pub fn prune_runs(dir: &Path, keep_previous: usize) -> Vec<PathBuf> {
    let runs = runs(dir);
    let Some(cut) = runs.len().checked_sub(keep_previous + 1).filter(|c| *c > 0) else {
        return Vec::new();
    };
    let old: Vec<String> = runs[..cut].iter().map(|n| format!("{n}.log")).collect();
    let mut gone = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if old.iter().any(|o| name == *o || name.starts_with(&format!("{o}."))) {
            if std::fs::remove_file(e.path()).is_ok() {
                gone.push(e.path());
            }
        }
    }
    gone
}

/// Prepare `<log_directory>/<container>/` for a new run: its number (see
/// [`next_run`]), with old runs pruned. Returns the run number; the CRI
/// `log_path` is `<container>/<N>.log`.
pub fn new_run(log_directory: &str, container: &str, at_least: u32) -> u32 {
    let dir = Path::new(log_directory).join(container);
    let n = next_run(&dir, at_least);
    // Pruned as if the new run were already there: it and the
    // KEEP_PREVIOUS before it stay.
    for p in prune_runs(&dir, KEEP_PREVIOUS - 1) {
        debug!("pruned old container log {}", p.display());
    }
    n
}

/// A run's rotated files (`<N>.log.<stamp>` and `<N>.log.<stamp>.gz`), oldest
/// first. The stamp sorts by time.
fn rotations(dir: &Path, live: &str) -> Vec<PathBuf> {
    let prefix = format!("{live}.");
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            let n = e.file_name().to_string_lossy().into_owned();
            n.starts_with(&prefix) && !n.ends_with(".tmp")
        })
        .map(|e| e.path())
        .collect();
    v.sort_by_key(|p| p.file_name().map(|n| n.to_string_lossy().trim_end_matches(".gz").to_string()));
    v
}

/// Rotate one container's current run if it is past the limit; tidy its
/// rotations either way. Returns whether it rotated.
pub fn rotate_container(dir: &Path, r: Rotation, now: chrono::DateTime<chrono::Utc>) -> std::io::Result<bool> {
    let Some(n) = runs(dir).last().copied() else { return Ok(false) };
    let live_name = format!("{n}.log");
    let live = dir.join(&live_name);
    // Leftovers of a compression cut short.
    for e in std::fs::read_dir(dir)?.flatten() {
        if e.file_name().to_string_lossy().ends_with(".tmp") {
            let _ = std::fs::remove_file(e.path());
        }
    }
    let size = std::fs::metadata(&live)?.len();
    if size <= r.max_size {
        return Ok(false);
    }
    // Make room: after this rotation the run has the live file and at most
    // max_files − 1 rotations.
    let max_rotations = r.max_files.max(2) - 1;
    let existing = rotations(dir, &live_name);
    let excess = (existing.len() + 1).saturating_sub(max_rotations);
    for p in &existing[..excess.min(existing.len())] {
        std::fs::remove_file(p)?;
    }
    // Every remaining rotation is compressed: the one about to be made is
    // the newest, which upstream leaves plain.
    for p in rotations(dir, &live_name) {
        if p.extension().and_then(|e| e.to_str()) != Some("gz") {
            compress(&p)?;
        }
    }
    let mut target = dir.join(format!("{live_name}.{}", now.format("%Y%m%d-%H%M%S")));
    if target.exists() {
        // Two rotations in one second: upstream would overwrite; keep both.
        target = dir.join(format!("{live_name}.{}.{}", now.format("%Y%m%d-%H%M%S"), now.timestamp_subsec_millis()));
    }
    copy_truncate(&live, &target)?;
    Ok(true)
}

/// Copy `live` to `target`, then truncate `live`. The copy is extended while
/// the file grows under it, so only what is written between the last check
/// and the truncate is lost (stormpump#129 will make this a rename + reopen).
fn copy_truncate(live: &Path, target: &Path) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut src = std::fs::File::open(live)?;
    let mut out = std::fs::OpenOptions::new().create_new(true).write(true).open(target)?;
    let mut copied = 0u64;
    for _ in 0..8 {
        let len = src.metadata()?.len();
        if len <= copied {
            break;
        }
        src.seek(SeekFrom::Start(copied))?;
        let mut chunk = Vec::with_capacity((len - copied) as usize);
        (&mut src).take(len - copied).read_to_end(&mut chunk)?;
        out.write_all(&chunk)?;
        copied += chunk.len() as u64;
    }
    out.sync_data()?;
    std::fs::OpenOptions::new().write(true).open(live)?.set_len(0)
}

/// gzip `p` to `p.gz` (through `p.gz.tmp`), then remove `p`.
fn compress(p: &Path) -> std::io::Result<()> {
    let data = std::fs::read(p)?;
    let gz = gzip(&data);
    let name = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let tmp = p.with_file_name(format!("{name}.gz.tmp"));
    std::fs::write(&tmp, gz)?;
    std::fs::rename(&tmp, p.with_file_name(format!("{name}.gz")))?;
    std::fs::remove_file(p)
}

/// A gzip member (RFC 1952) holding `data`, deflated by miniz_oxide.
pub fn gzip(data: &[u8]) -> Vec<u8> {
    let mut out = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
    out.extend(miniz_oxide::deflate::compress_to_vec(data, 6));
    out.extend(crc32(data).to_le_bytes());
    out.extend((data.len() as u32).to_le_bytes());
    out
}

/// CRC-32 (IEEE), as gzip carries it.
fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in data {
        c ^= *b as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 { (c >> 1) ^ 0xEDB8_8320 } else { c >> 1 };
        }
    }
    !c
}

/// One rotation pass over every pod's log directory under `root`.
pub fn rotate_all(root: &Path, r: Rotation) {
    let now = chrono::Utc::now();
    for pod in std::fs::read_dir(root).into_iter().flatten().flatten() {
        for c in std::fs::read_dir(pod.path()).into_iter().flatten().flatten() {
            if !c.path().is_dir() {
                continue;
            }
            match rotate_container(&c.path(), r, now) {
                Ok(true) => debug!("rotated {}", c.path().display()),
                Ok(false) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => warn!("container log rotation in {}: {e}", c.path().display()),
            }
        }
    }
}

/// Rotate for ever, every [`MONITOR_INTERVAL`].
pub async fn monitor(root: PathBuf, r: Rotation) {
    loop {
        let (root, r2) = (root.clone(), r);
        let _ = tokio::task::spawn_blocking(move || rotate_all(&root, r2)).await;
        tokio::time::sleep(MONITOR_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> =
            std::fs::read_dir(dir).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        v.sort();
        v
    }

    #[test]
    fn a_new_run_follows_every_run_on_disk_and_the_restart_count() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(next_run(d.path(), 0), 0);
        std::fs::write(d.path().join("0.log"), "a").unwrap();
        assert_eq!(next_run(d.path(), 0), 1, "an init retried writes 1.log, not 0.log again");
        std::fs::write(d.path().join("3.log"), "a").unwrap();
        std::fs::write(d.path().join("3.log.20260101-000000"), "a").unwrap();
        assert_eq!(next_run(d.path(), 0), 4);
        assert_eq!(next_run(d.path(), 9), 9, "the restart count when it is ahead");
        assert_eq!(runs(d.path()), vec![0, 3]);
    }

    #[test]
    fn old_runs_are_pruned_with_their_rotations() {
        let pod = tempfile::tempdir().unwrap();
        let d = pod.path().join("app");
        std::fs::create_dir(&d).unwrap();
        for n in 0..9 {
            std::fs::write(d.join(format!("{n}.log")), "x").unwrap();
        }
        std::fs::write(d.join("1.log.20260101-000000.gz"), "x").unwrap();
        // A new run (9) is about to start: the 5 before it (4..8) stay.
        assert_eq!(new_run(pod.path().to_str().unwrap(), "app", 0), 9);
        assert_eq!(names(&d), vec!["4.log", "5.log", "6.log", "7.log", "8.log"]);
    }

    #[test]
    fn rotation_keeps_max_files_compresses_older_and_truncates_the_live_file() {
        let d = tempfile::tempdir().unwrap();
        let r = Rotation { max_size: 10, max_files: 3 };
        let live = d.path().join("2.log");
        std::fs::write(d.path().join("1.log"), "previous run").unwrap();
        let t = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&chrono::Utc);

        std::fs::write(&live, "short").unwrap();
        assert!(!rotate_container(d.path(), r, t("2026-10-08T10:00:00Z")).unwrap(), "under the limit");

        std::fs::write(&live, "first generation\n").unwrap();
        assert!(rotate_container(d.path(), r, t("2026-10-08T10:00:00Z")).unwrap());
        assert_eq!(std::fs::read_to_string(&live).unwrap(), "");
        assert_eq!(std::fs::read_to_string(d.path().join("2.log.20261008-100000")).unwrap(), "first generation\n");

        std::fs::write(&live, "second generation\n").unwrap();
        assert!(rotate_container(d.path(), r, t("2026-10-08T10:00:10Z")).unwrap());
        // The older rotation is compressed; the newest stays plain.
        assert_eq!(
            names(d.path()),
            vec!["1.log", "2.log", "2.log.20261008-100000.gz", "2.log.20261008-100010"]
        );
        let gz = std::fs::read(d.path().join("2.log.20261008-100000.gz")).unwrap();
        assert_eq!(&gz[..2], &[0x1f, 0x8b]);
        let body = &gz[10..gz.len() - 8];
        assert_eq!(miniz_oxide::inflate::decompress_to_vec(body).unwrap(), b"first generation\n");
        assert_eq!(&gz[gz.len() - 8..gz.len() - 4], &crc32(b"first generation\n").to_le_bytes());

        std::fs::write(&live, "third generation\n").unwrap();
        assert!(rotate_container(d.path(), r, t("2026-10-08T10:00:20Z")).unwrap());
        // max_files 3: the live file and two rotations; the oldest went.
        assert_eq!(
            names(d.path()),
            vec!["1.log", "2.log", "2.log.20261008-100010.gz", "2.log.20261008-100020"]
        );
        assert_eq!(std::fs::read_to_string(d.path().join("1.log")).unwrap(), "previous run", "other runs untouched");
    }

    #[test]
    fn crc32_matches_the_reference() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }
}
