//! Files the kubelet writes into a pod's API volumes: configMap, secret and
//! projected keys, the ServiceAccount token, `ca.crt` and `namespace`
//! (rustkube-node#428, finding 551d194231b5).
//!
//! The kubelet writes these as root, in its own mount namespace, which sees
//! the node (`/hostroot`). A plain `std::fs::write` on a name a container can
//! see follows whatever is there: a container that replaced a key with a
//! symlink would have the kubelet's next write (a restart's re-materialize,
//! a token refresh) land anywhere the symlink points. So:
//!
//! - a key's path must be relative, made of plain components only (no `..`,
//!   no absolute path), and every directory on the way must be a real
//!   directory, never a symlink;
//! - the bytes go to a new file made with `O_CREAT|O_EXCL` (which never opens
//!   an existing entry, a symlink included) and are renamed over the key. A
//!   rename replaces a symlink; it does not follow it. A reader through the
//!   bind sees the old file or the new one, never half of one.
//!
//! These volumes are also mounted read-only (`ResolvedVolume::api`), so a
//! container cannot plant the symlink in the first place; this is the second
//! wall.

use std::io::{Error, ErrorKind, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The relative path `rel` names inside a volume, if it is made only of
/// plain components: not empty, not absolute, no `..` or leading `.`.
pub fn contained(rel: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in Path::new(rel).components() {
        match c {
            Component::Normal(n) => out.push(n),
            _ => return None,
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

/// Write `bytes` at `rel` under the volume directory `dir`, mode `mode`,
/// making the directories on the way. Refused, writing nothing, when `rel`
/// leaves `dir` or anything on the way is not a real directory.
pub fn write_in(dir: &str, rel: &str, bytes: &[u8], mode: u32) -> std::io::Result<()> {
    let rel_path = contained(rel).ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, format!("{rel:?} is not a path inside the volume"))
    })?;
    let mut cur = PathBuf::from(dir);
    real_dir(&cur)?;
    let parts: Vec<_> = rel_path.components().collect();
    let (last, parents) = parts.split_last().expect("contained() is never empty");
    for part in parents {
        cur.push(part);
        match std::fs::create_dir(&cur) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        real_dir(&cur)?;
    }
    cur.push(last);
    write_atomic(&cur, bytes, mode)
}

/// `path` is a directory itself, not a symlink to one.
fn real_dir(path: &Path) -> std::io::Result<()> {
    let m = std::fs::symlink_metadata(path)?;
    if m.file_type().is_dir() {
        Ok(())
    } else {
        Err(Error::other(format!("{} is not a directory (a symlink or a file)", path.display())))
    }
}

/// Write a file whole: a new file beside it (created exclusively, so no
/// existing entry is opened or followed), renamed over it.
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> std::io::Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().unwrap_or(Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, format!("{} names no file", path.display())))?;
    let tmp = parent.join(format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| {
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(mode).open(&tmp)?;
        f.write_all(bytes)?;
        // The umask may have narrowed the mode `open` was given.
        f.set_permissions(std::fs::Permissions::from_mode(mode))?;
        drop(f);
        std::fs::rename(&tmp, path)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_relative_paths_are_contained() {
        assert_eq!(contained("token"), Some(PathBuf::from("token")));
        assert_eq!(contained("a/b/c"), Some(PathBuf::from("a/b/c")));
        for bad in ["", "/etc/passwd", "../x", "a/../../x", "a/..", ".", "./x", ".."] {
            assert_eq!(contained(bad), None, "{bad:?}");
        }
    }

    /// The finding's attack: a container replaced a key with a symlink to a
    /// file outside the volume. The kubelet's next write replaces the link
    /// and the outside file is untouched.
    #[test]
    fn a_planted_symlink_is_replaced_not_followed() {
        let vol = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("victim");
        std::fs::write(&victim, "host file").unwrap();
        let dir = vol.path().to_str().unwrap();
        write_in(dir, "key", b"v1", 0o644).unwrap();
        std::fs::remove_file(vol.path().join("key")).unwrap();
        std::os::unix::fs::symlink(&victim, vol.path().join("key")).unwrap();

        write_in(dir, "key", b"v2", 0o644).unwrap();

        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "host file");
        let key = vol.path().join("key");
        assert!(!std::fs::symlink_metadata(&key).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read_to_string(&key).unwrap(), "v2");
        let mode = std::fs::metadata(&key).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o644);
        // No temp file is left behind.
        let names: Vec<_> = std::fs::read_dir(vol.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names, vec![std::ffi::OsString::from("key")]);
    }

    /// A planted symlinked directory on a key's path, or a temp name taken
    /// in advance by a symlink, writes nothing outside.
    #[test]
    fn a_symlinked_directory_on_the_way_is_refused() {
        let vol = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), vol.path().join("sub")).unwrap();
        let dir = vol.path().to_str().unwrap();
        let err = write_in(dir, "sub/key", b"x", 0o644).unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
        assert!(std::fs::read_dir(outside.path()).unwrap().next().is_none());
        // `..` and absolute paths never reach the filesystem.
        let target = outside.path().join("abs");
        assert!(write_in(dir, target.to_str().unwrap(), b"x", 0o644).is_err());
        assert!(write_in(dir, "../x", b"x", 0o644).is_err());
        assert!(!target.exists());
        // Subdirectories that are real are made and written.
        write_in(dir, "real/deeper/key", b"ok", 0o600).unwrap();
        assert_eq!(std::fs::read_to_string(vol.path().join("real/deeper/key")).unwrap(), "ok");
    }
}
