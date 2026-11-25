//! A claim's filesystem sizes as its container sees them (#469).
//!
//! A built-in stormblock claim is mounted by the container, in its own mount
//! namespace, not by the kubelet: there is no host path to `statfs`. The
//! container's root is: `/proc/<pid>/root` is the root of that process's
//! mount namespace, and a path resolved under it crosses that namespace's
//! mounts. So the claim's mount path is resolved there, with
//! `openat2(RESOLVE_IN_ROOT)` so that an absolute symlink in the image
//! resolves inside the container's root and never to a host path, and its
//! filesystem is asked with `fstatvfs`.
//!
//! Only a mount point answers: when the path's device is its parent's, the
//! claim is not mounted there (yet, or any more) and the sizes would be the
//! container root's, so none are given.

/// What `statvfs(2)` says of a claim's filesystem, in upstream's
/// `VolumeStats` terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsStats {
    /// Bytes an unprivileged writer may still use (`f_bavail * f_frsize`),
    /// what `df` shows as available.
    pub available_bytes: u64,
    /// Inodes the filesystem has (`f_files`).
    pub inodes: u64,
    /// Inodes free (`f_ffree`).
    pub inodes_free: u64,
}

impl FsStats {
    pub fn inodes_used(&self) -> u64 {
        self.inodes.saturating_sub(self.inodes_free)
    }

    /// The `VolumeStats` fields these fill.
    pub fn fill(&self, v: &mut serde_json::Value) {
        v["availableBytes"] = serde_json::json!(self.available_bytes);
        v["inodes"] = serde_json::json!(self.inodes);
        v["inodesFree"] = serde_json::json!(self.inodes_free);
        v["inodesUsed"] = serde_json::json!(self.inodes_used());
    }
}

/// The filesystem mounted at `mount_path` in the mount namespace of `pid`.
#[cfg(target_os = "linux")]
pub fn statfs_in(pid: i32, mount_path: &str) -> Result<FsStats, String> {
    if pid <= 0 {
        return Err(format!("no process ({pid})"));
    }
    statfs_under(std::path::Path::new(&format!("/proc/{pid}/root")), mount_path)
}

/// [`statfs_in`] with the root given: `mount_path` resolved under `root` as
/// if `root` were `/`.
#[cfg(target_os = "linux")]
pub fn statfs_under(root: &std::path::Path, mount_path: &str) -> Result<FsStats, String> {
    use std::os::fd::AsRawFd;
    let rel = mount_path.trim_start_matches('/');
    if rel.is_empty() {
        return Err("a claim is not mounted at /".into());
    }
    let parent = std::path::Path::new(rel)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| ".".into());
    let root_fd = open_path(None, root)?;
    let target = open_in_root(&root_fd, rel)?;
    let above = open_in_root(&root_fd, &parent)?;
    let (dev, parent_dev) = (fstat_dev(&target)?, fstat_dev(&above)?);
    if dev == parent_dev {
        return Err(format!("nothing is mounted at {mount_path}"));
    }
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: a valid fd and a statvfs to fill.
    if unsafe { libc::fstatvfs(target.as_raw_fd(), &mut st) } != 0 {
        return Err(format!("fstatvfs {mount_path}: {}", std::io::Error::last_os_error()));
    }
    let frsize = (if st.f_frsize > 0 { st.f_frsize } else { st.f_bsize }) as u64;
    Ok(FsStats {
        available_bytes: (st.f_bavail as u64).saturating_mul(frsize),
        inodes: st.f_files as u64,
        inodes_free: st.f_ffree as u64,
    })
}

#[cfg(target_os = "linux")]
fn open_path(dir: Option<&std::os::fd::OwnedFd>, path: &std::path::Path) -> Result<std::os::fd::OwnedFd, String> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| format!("bad path {path:?}"))?;
    let at = dir.map(|d| d.as_raw_fd()).unwrap_or(libc::AT_FDCWD);
    // SAFETY: a NUL-terminated path; the fd returned is owned below.
    let fd = unsafe { libc::openat(at, c.as_ptr(), libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(format!("open {}: {}", path.display(), std::io::Error::last_os_error()));
    }
    // SAFETY: fd was just returned by openat and is owned by nothing else.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

/// `openat2(2)`'s `struct open_how`.
#[cfg(target_os = "linux")]
#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

#[cfg(target_os = "linux")]
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
#[cfg(target_os = "linux")]
const RESOLVE_IN_ROOT: u64 = 0x10;

/// `rel` resolved under `root` as if `root` were `/`: `..` and absolute
/// symlinks stop at it (`RESOLVE_IN_ROOT`, Linux 5.6).
#[cfg(target_os = "linux")]
fn open_in_root(root: &std::os::fd::OwnedFd, rel: &str) -> Result<std::os::fd::OwnedFd, String> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let c = std::ffi::CString::new(rel).map_err(|_| format!("bad path {rel:?}"))?;
    let how = OpenHow {
        flags: (libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_IN_ROOT | RESOLVE_NO_MAGICLINKS,
    };
    // SAFETY: openat2 with a valid dirfd, a NUL-terminated path and an
    // open_how of the size passed.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            c.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(format!("open /{rel}: {}", std::io::Error::last_os_error()));
    }
    // SAFETY: fd was just returned by openat2 and is owned by nothing else.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) })
}

#[cfg(target_os = "linux")]
fn fstat_dev(fd: &std::os::fd::OwnedFd) -> Result<u64, String> {
    use std::os::fd::AsRawFd;
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: a valid fd and a stat to fill.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
        return Err(format!("fstat: {}", std::io::Error::last_os_error()));
    }
    Ok(st.st_dev as u64)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// `/proc` is its own filesystem under `/`, on every Linux this runs on:
    /// a mount point to ask without mounting anything (that needs root).
    #[test]
    fn a_mount_point_under_the_root_gives_its_filesystem() {
        let got = statfs_under(std::path::Path::new("/"), "/proc").unwrap();
        // proc has no blocks to spare; what matters is that it is proc's
        // answer, not the root filesystem's.
        let mut want: libc::statvfs = unsafe { std::mem::zeroed() };
        let c = std::ffi::CString::new("/proc").unwrap();
        assert_eq!(unsafe { libc::statvfs(c.as_ptr(), &mut want) }, 0);
        assert_eq!(got.inodes, want.f_files as u64);
        assert_eq!(got.inodes_free, want.f_ffree as u64);
        assert_eq!(got.available_bytes, want.f_bavail as u64 * want.f_frsize as u64);
    }

    /// The sizes match `statvfs(2)` of the same mount, as `df` reads them:
    /// `/dev` is a devtmpfs or tmpfs with inodes and free space to compare.
    #[test]
    fn sizes_are_what_statvfs_gives_df() {
        let Ok(got) = statfs_under(std::path::Path::new("/"), "/dev") else {
            return; // a build box without /dev as its own mount: nothing to compare
        };
        let mut want: libc::statvfs = unsafe { std::mem::zeroed() };
        let c = std::ffi::CString::new("/dev").unwrap();
        assert_eq!(unsafe { libc::statvfs(c.as_ptr(), &mut want) }, 0);
        assert_eq!(got.inodes, want.f_files as u64);
        assert_eq!(got.available_bytes, want.f_bavail as u64 * want.f_frsize as u64);
        assert_eq!(got.inodes_used(), (want.f_files - want.f_ffree) as u64);
    }

    /// A directory that is no mount point gives nothing: its sizes would be
    /// the root filesystem's, not a claim's.
    #[test]
    fn a_path_that_is_not_a_mount_gives_nothing() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let e = statfs_under(dir.path(), "/data").unwrap_err();
        assert!(e.contains("nothing is mounted at /data"), "{e}");
    }

    /// An absolute symlink in the container's image resolves inside its root,
    /// never to the host's path of the same name.
    #[test]
    fn an_absolute_symlink_stays_inside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("inside")).unwrap();
        // On the host `/proc` is a mount; inside this root, `/data` → `/proc`
        // is the root's own (unmounted) `proc` directory.
        std::fs::create_dir_all(dir.path().join("proc")).unwrap();
        std::os::unix::fs::symlink("/proc", dir.path().join("data")).unwrap();
        let e = statfs_under(dir.path(), "/data").unwrap_err();
        assert!(e.contains("nothing is mounted"), "{e}");
        // `..` stops at the root as well.
        let e = statfs_under(dir.path(), "/../../proc").unwrap_err();
        assert!(e.contains("nothing is mounted"), "{e}");
    }

    #[test]
    fn the_root_itself_and_no_process_are_refused() {
        assert!(statfs_under(std::path::Path::new("/"), "/").is_err());
        assert!(statfs_in(0, "/data").is_err());
    }

    #[test]
    fn fill_sets_upstreams_four_fields() {
        let mut v = serde_json::json!({"name": "data"});
        FsStats { available_bytes: 100, inodes: 10, inodes_free: 4 }.fill(&mut v);
        assert_eq!(v["availableBytes"], 100);
        assert_eq!(v["inodes"], 10);
        assert_eq!(v["inodesFree"], 4);
        assert_eq!(v["inodesUsed"], 6);
    }
}
