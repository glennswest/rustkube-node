//! `fsGroup` on a claim's filesystem (#171): upstream's `SetVolumeOwnership`.
//!
//! A claim is an ext4 that PID 1 mounts at `/run/stormpump/pvc/<dev>` when the
//! container that uses it is started (the kubelet sees that mount: the host's
//! `/run` is bound at its `/run`). A fresh one is `0755 root:root`, so a
//! non-root container could read it and not write it. Before the spawn, the
//! tree is given to the pod's `fsGroup`, as upstream's kubelet does it:
//!
//! - every file and directory's group becomes `fsGroup` (its owner is left),
//! - files gain group read-write (`0660`; read-only `0440` for a read-only
//!   mount), directories also group execute and setgid, so what is made in
//!   them later is the group's too,
//! - symlinks are left alone (a link into the host is not the volume's),
//! - `fsGroupChangePolicy: OnRootMismatch` looks at the root only, and walks
//!   nothing when the root is already right.
//!
//! The container gets `fsGroup` as a supplementary group (`Spec.groups`).

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

/// `fsGroupChangePolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChangePolicy {
    /// Upstream's default: the whole tree, every start.
    #[default]
    Always,
    /// Only when the root's group or mode is not already right.
    OnRootMismatch,
}

impl ChangePolicy {
    pub fn parse(s: Option<&str>) -> ChangePolicy {
        match s {
            Some("OnRootMismatch") => ChangePolicy::OnRootMismatch,
            _ => ChangePolicy::Always,
        }
    }
}

const RW: u32 = 0o660;
const RO: u32 = 0o440;
const EXEC: u32 = 0o110;
const SETGID: u32 = 0o2000;

fn mask(dir: bool, readonly: bool) -> u32 {
    let m = if readonly { RO } else { RW };
    if dir { m | EXEC | SETGID } else { m }
}

/// Whether the tree needs walking: upstream's `requiresPermissionChange`.
fn root_mismatch(root: &Path, gid: u32, readonly: bool) -> std::io::Result<bool> {
    let md = std::fs::metadata(root)?;
    let want = mask(md.is_dir(), readonly);
    Ok(md.gid() != gid || md.mode() & want != want)
}

/// Give `root`'s tree to `gid`. Answers how many entries were changed.
pub fn set(root: &Path, gid: u32, readonly: bool, policy: ChangePolicy) -> std::io::Result<usize> {
    if policy == ChangePolicy::OnRootMismatch && !root_mismatch(root, gid, readonly)? {
        return Ok(0);
    }
    let mut changed = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let md = std::fs::symlink_metadata(&path)?;
        if md.file_type().is_symlink() {
            continue;
        }
        if md.is_dir() {
            for e in std::fs::read_dir(&path)? {
                stack.push(e?.path());
            }
        }
        let want = mask(md.is_dir(), readonly);
        if md.gid() != gid {
            std::os::unix::fs::lchown(&path, None, Some(gid))
                .map_err(|e| std::io::Error::new(e.kind(), format!("chown {}: {e}", path.display())))?;
        }
        // Re-read: a chown of a file clears its setuid/setgid bits.
        let mode = std::fs::symlink_metadata(&path)?.mode() & 0o7777;
        if mode | want != mode || md.gid() != gid {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode | want))
                .map_err(|e| std::io::Error::new(e.kind(), format!("chmod {}: {e}", path.display())))?;
            changed += 1;
        }
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The test's own group: a gid it may chown to without privilege.
    fn my_gid() -> u32 {
        // SAFETY: getegid has no preconditions.
        unsafe { libc::getegid() }
    }

    #[test]
    fn the_tree_gets_the_group_its_rw_bits_and_setgid_on_directories() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::set_permissions(root.join("sub"), std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(root.join("sub/f"), b"x").unwrap();
        std::fs::set_permissions(root.join("sub/f"), std::fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", root.join("link")).unwrap();
        let gid = my_gid();

        let n = set(root, gid, false, ChangePolicy::Always).unwrap();
        assert!(n >= 3, "{n}");
        let mode = |p: &str| std::fs::metadata(root.join(p)).unwrap().mode() & 0o7777;
        assert_eq!(mode(""), 0o2775);
        assert_eq!(mode("sub"), 0o2770);
        assert_eq!(mode("sub/f"), 0o660);
        assert_eq!(std::fs::metadata(root.join("sub/f")).unwrap().gid(), gid);
        // The link is skipped (its target is not the volume's).
        assert!(std::fs::symlink_metadata(root.join("link")).unwrap().file_type().is_symlink());

        // Again: nothing left to change.
        assert_eq!(set(root, gid, false, ChangePolicy::Always).unwrap(), 0);
    }

    #[test]
    fn on_root_mismatch_walks_nothing_when_the_root_is_right() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::write(root.join("f"), b"x").unwrap();
        std::fs::set_permissions(root.join("f"), std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o2775)).unwrap();
        let gid = my_gid();
        assert_eq!(set(root, gid, false, ChangePolicy::OnRootMismatch).unwrap(), 0);
        assert_eq!(std::fs::metadata(root.join("f")).unwrap().mode() & 0o777, 0o600, "left alone");
        // A root that is not right: the whole tree.
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(set(root, gid, false, ChangePolicy::OnRootMismatch).unwrap() >= 2);
        assert_eq!(std::fs::metadata(root.join("f")).unwrap().mode() & 0o777, 0o660);
    }

    #[test]
    fn a_read_only_mount_gets_group_read_only() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("f"), b"x").unwrap();
        std::fs::set_permissions(d.path().join("f"), std::fs::Permissions::from_mode(0o600)).unwrap();
        set(d.path(), my_gid(), true, ChangePolicy::Always).unwrap();
        assert_eq!(std::fs::metadata(d.path().join("f")).unwrap().mode() & 0o777, 0o640);
        assert_eq!(ChangePolicy::parse(Some("OnRootMismatch")), ChangePolicy::OnRootMismatch);
        assert_eq!(ChangePolicy::parse(None), ChangePolicy::Always);
    }
}
