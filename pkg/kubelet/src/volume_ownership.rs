//! `fsGroup` on a claim's filesystem (#171): upstream's `SetVolumeOwnership`.
//!
//! A claim is an ext4 that PID 1 mounts at `/run/stormpump/pvc/<dev>` when the
//! container that uses it is started. The kubelet walks it through PID 1's
//! root (`pvc_resize::claim_dir`, #379): its own `/run` is a private bind of
//! the node's, where that mount is not seen. A fresh one is `0755 root:root`, so a
//! non-root container could read it and not write it. Before the spawn, the
//! tree is given to the pod's `fsGroup`, as upstream's kubelet does it:
//!
//! - every file and directory's group becomes `fsGroup` (its owner is left),
//! - files gain group read-write (`0660`; read-only `0440` for a read-only
//!   mount), directories also group execute and setgid, so what is made in
//!   them later is the group's too,
//! - symlinks are left alone (a link into the host is not the volume's),
//! - `fsGroupChangePolicy: OnRootMismatch` looks at the root only, and walks
//!   nothing when the root is already right. The root is changed **last**,
//!   after every entry under it (#448, 5bcb74c7d170): a walk that failed or
//!   was cut short leaves the root wrong, so the next start walks again.
//!
//! **The walk never resolves a path inside the volume** (#448, 9db8782b9019).
//! A pod already using the claim can rename an entry, or a directory above
//! it, and put a symlink in its place while the walk runs; this kubelet is
//! privileged, so a chmod or chown that followed it would change a file of
//! the node's. So the root is opened once, and every entry is opened from its
//! parent's descriptor by its single name with `O_NOFOLLOW`: a symlink is
//! seen as one and left alone, and a directory swapped for one cannot be
//! entered. Each change is made through the entry's own descriptor
//! (`fchown`, `fchmod`; a non-directory, opened `O_PATH` so a device or fifo
//! is never opened for I/O, through its `/proc/self/fd` link, which names
//! that inode and nothing else).
//!
//! The container gets `fsGroup` as a supplementary group (`Spec.groups`).

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

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

fn is_dir(st: &libc::stat) -> bool {
    st.st_mode & libc::S_IFMT == libc::S_IFDIR
}

fn is_symlink(st: &libc::stat) -> bool {
    st.st_mode & libc::S_IFMT == libc::S_IFLNK
}

fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    // SAFETY: an all-zero stat is valid, and fstat writes it whole on success.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: a descriptor the caller holds open, and a stat to write.
    if unsafe { libc::fstat(fd, &mut st) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st)
}

/// `openat(parent, name, flags | O_NOFOLLOW | O_CLOEXEC)`: one name, never
/// followed if it is a symlink.
fn open_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: a descriptor the caller holds open and a NUL-terminated name.
    let fd = unsafe { libc::openat(parent, name.as_ptr(), flags | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat succeeded, so the descriptor is open and ours.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn with_path(e: io::Error, what: &str, path: &Path) -> io::Error {
    io::Error::new(e.kind(), format!("{what} {}: {e}", path.display()))
}

/// Give the entry `fd` (a directory opened for reading, or a non-directory
/// opened `O_PATH`) the group and bits it wants. `true` when it changed.
fn fix(fd: RawFd, gid: u32, readonly: bool, path: &Path) -> io::Result<bool> {
    let st = fstat(fd).map_err(|e| with_path(e, "stat", path))?;
    let dir = is_dir(&st);
    let want = mask(dir, readonly);
    let regroup = st.st_gid != gid;
    if regroup {
        let empty = c"";
        // SAFETY: an open descriptor; AT_EMPTY_PATH makes the empty name mean
        // the descriptor itself, for a directory and an O_PATH one alike.
        if unsafe { libc::fchownat(fd, empty.as_ptr(), u32::MAX, gid, libc::AT_EMPTY_PATH) } != 0 {
            return Err(with_path(io::Error::last_os_error(), "chown", path));
        }
    }
    // Re-read: a chown of a file clears its setuid/setgid bits.
    let mode = fstat(fd).map_err(|e| with_path(e, "stat", path))?.st_mode & 0o7777;
    if mode | want == mode && !regroup {
        return Ok(false);
    }
    let rc = if dir {
        // SAFETY: a directory descriptor opened for reading.
        unsafe { libc::fchmod(fd, mode | want) }
    } else {
        // An O_PATH descriptor takes no fchmod; its /proc link resolves to
        // exactly the inode it holds.
        let link = CString::new(format!("/proc/self/fd/{fd}")).expect("no NUL");
        // SAFETY: a NUL-terminated path.
        unsafe { libc::chmod(link.as_ptr(), mode | want) }
    };
    if rc != 0 {
        return Err(with_path(io::Error::last_os_error(), "chmod", path));
    }
    Ok(true)
}

/// A directory being read, by its own descriptor.
struct Dir {
    dir: std::ptr::NonNull<libc::DIR>,
    path: PathBuf,
}

impl Dir {
    fn new(fd: OwnedFd, path: PathBuf) -> io::Result<Dir> {
        let raw = fd.as_raw_fd();
        // SAFETY: fdopendir takes ownership of a descriptor it may close.
        let dir = unsafe { libc::fdopendir(raw) };
        let Some(dir) = std::ptr::NonNull::new(dir) else {
            return Err(with_path(io::Error::last_os_error(), "read", &path));
        };
        std::mem::forget(fd); // the stream owns it now
        Ok(Dir { dir, path })
    }

    fn fd(&self) -> RawFd {
        // SAFETY: a stream this value owns.
        unsafe { libc::dirfd(self.dir.as_ptr()) }
    }

    /// The next entry's name, not `.` or `..`.
    fn next(&mut self) -> io::Result<Option<CString>> {
        loop {
            // SAFETY: errno is this thread's, cleared so an end reads as one.
            unsafe { *libc::__errno_location() = 0 };
            // SAFETY: a stream this value owns; the entry is valid until the
            // next readdir on it, and is copied before then.
            let e = unsafe { libc::readdir(self.dir.as_ptr()) };
            if e.is_null() {
                let err = io::Error::last_os_error();
                return match err.raw_os_error() {
                    Some(0) | None => Ok(None),
                    _ => Err(with_path(err, "read", &self.path)),
                };
            }
            // SAFETY: d_name is NUL-terminated within the entry.
            let name = unsafe { CStr::from_ptr((*e).d_name.as_ptr()) };
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            return Ok(Some(name.to_owned()));
        }
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        // SAFETY: a stream this value owns, closed once.
        unsafe { libc::closedir(self.dir.as_ptr()) };
    }
}

/// Whether the tree needs walking: upstream's `requiresPermissionChange`.
fn root_mismatch(root: RawFd, gid: u32, readonly: bool) -> io::Result<bool> {
    let st = fstat(root)?;
    let want = mask(is_dir(&st), readonly);
    Ok(st.st_gid != gid || st.st_mode & want != want)
}

/// Give `root`'s tree to `gid`. Answers how many entries were changed.
///
/// Post-order, by descriptor (see the module comment): each directory once
/// everything in it is done, the root last.
pub fn set(root: &Path, gid: u32, readonly: bool, policy: ChangePolicy) -> io::Result<usize> {
    let c_root = CString::new(root.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "a path with a NUL"))?;
    // The root itself: the claim's mount point, which the kubelet named.
    let opened = open_at(libc::AT_FDCWD, &c_root, libc::O_RDONLY | libc::O_DIRECTORY);
    let root_fd = match opened {
        Ok(fd) => fd,
        // A claim that is one file (not a directory): just itself. A symlink
        // (ELOOP under O_NOFOLLOW) is seen as one there, and left alone.
        Err(e) if matches!(e.raw_os_error(), Some(libc::ENOTDIR | libc::ELOOP)) => {
            let fd = open_at(libc::AT_FDCWD, &c_root, libc::O_PATH).map_err(|e| with_path(e, "open", root))?;
            let st = fstat(fd.as_raw_fd()).map_err(|e| with_path(e, "stat", root))?;
            if is_symlink(&st) {
                return Ok(0);
            }
            if policy == ChangePolicy::OnRootMismatch && !root_mismatch(fd.as_raw_fd(), gid, readonly)? {
                return Ok(0);
            }
            return Ok(usize::from(fix(fd.as_raw_fd(), gid, readonly, root)?));
        }
        Err(e) => return Err(with_path(e, "open", root)),
    };
    if policy == ChangePolicy::OnRootMismatch && !root_mismatch(root_fd.as_raw_fd(), gid, readonly)? {
        return Ok(0);
    }
    let mut changed = 0;
    let mut stack = vec![Dir::new(root_fd, root.to_path_buf())?];
    while let Some(top) = stack.last_mut() {
        let Some(name) = top.next()? else {
            // Everything in it is done: now the directory itself.
            let done = stack.pop().expect("the top");
            if fix(done.fd(), gid, readonly, &done.path)? {
                changed += 1;
            }
            continue;
        };
        let path = top.path.join(std::ffi::OsStr::from_bytes(name.to_bytes()));
        // O_PATH first: it opens a symlink as itself (never its target) and a
        // device or fifo without opening it for I/O.
        let entry = open_at(top.fd(), &name, libc::O_PATH).map_err(|e| with_path(e, "open", &path))?;
        let st = fstat(entry.as_raw_fd()).map_err(|e| with_path(e, "stat", &path))?;
        if is_symlink(&st) {
            continue; // a link's target is not the volume's
        }
        if is_dir(&st) {
            // Reopened to read it, by the same single name, still not
            // following: swapped for a symlink since, it is refused here.
            let dir = open_at(top.fd(), &name, libc::O_RDONLY | libc::O_DIRECTORY).map_err(|e| with_path(e, "open", &path))?;
            let again = fstat(dir.as_raw_fd()).map_err(|e| with_path(e, "stat", &path))?;
            if (again.st_dev, again.st_ino) != (st.st_dev, st.st_ino) {
                return Err(io::Error::other(format!("{} changed while its ownership was being set", path.display())));
            }
            stack.push(Dir::new(dir, path)?);
            continue;
        }
        if fix(entry.as_raw_fd(), gid, readonly, &path)? {
            changed += 1;
        }
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

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

    /// #448 (5bcb74c7d170): a walk that fails part-way leaves the root as
    /// it was, so an OnRootMismatch retry walks again and finishes the tree.
    #[test]
    fn an_interrupted_walk_leaves_the_root_wrong_and_the_retry_finishes() {
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return; // root reads a 0000 directory: nothing would fail
        }
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::create_dir(root.join("a")).unwrap();
        std::fs::write(root.join("a/f"), b"x").unwrap();
        std::fs::set_permissions(root.join("a/f"), std::fs::Permissions::from_mode(0o600)).unwrap();
        // Unreadable: the walk cannot list it, and fails there.
        std::fs::set_permissions(root.join("a"), std::fs::Permissions::from_mode(0o000)).unwrap();
        let gid = my_gid();
        let err = set(root, gid, false, ChangePolicy::OnRootMismatch).unwrap_err();
        assert!(err.to_string().contains("/a"), "{err}");
        let mode = |p: &str| std::fs::metadata(root.join(p)).unwrap().mode() & 0o7777;
        assert_eq!(mode(""), 0o755, "the root is changed last, so not at all");

        std::fs::set_permissions(root.join("a"), std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(set(root, gid, false, ChangePolicy::OnRootMismatch).unwrap() >= 3);
        assert_eq!(mode("a/f"), 0o660, "the retry walked the tree");
        assert_eq!(mode("a"), 0o2770);
        assert_eq!(mode(""), 0o2775);
        assert_eq!(set(root, gid, false, ChangePolicy::OnRootMismatch).unwrap(), 0, "and now the root is right");
    }

    /// #448 (9db8782b9019): a symlink to a directory outside the volume is
    /// neither entered nor changed, nor is its target; a symlink as the
    /// volume's root is not followed.
    #[test]
    fn a_symlinked_directory_is_not_entered() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"x").unwrap();
        std::fs::set_permissions(outside.path().join("secret"), std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(outside.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let d = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), d.path().join("escape")).unwrap();
        set(d.path(), my_gid(), false, ChangePolicy::Always).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().mode() & 0o7777;
        assert_eq!(mode(outside.path()), 0o700);
        assert_eq!(mode(&outside.path().join("secret")), 0o600);
        assert_eq!(mode(d.path()), 0o2770, "tempdir's 0700, plus the group's bits");

        let link = d.path().join("rootlink");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        assert_eq!(set(&link, my_gid(), false, ChangePolicy::Always).unwrap(), 0);
        assert_eq!(mode(outside.path()), 0o700, "a root that is a symlink is not followed");
    }

    /// A fifo is changed through its descriptor without being opened for
    /// I/O (an open of a fifo with no writer would block).
    #[test]
    fn a_fifo_is_changed_without_being_opened() {
        let d = tempfile::tempdir().unwrap();
        let fifo = d.path().join("p");
        let c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: a NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
        set(d.path(), my_gid(), false, ChangePolicy::Always).unwrap();
        assert_eq!(std::fs::symlink_metadata(&fifo).unwrap().mode() & 0o777, 0o660);
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
