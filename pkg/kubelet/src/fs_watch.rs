//! Static-manifest changes wake reconciliation without a directory poll loop.
use std::path::PathBuf;

/// Re-arm before requesting a scan. Watching the nearest existing ancestor
/// catches creation of a previously missing manifest directory; watching its
/// parent catches rename/replacement. Queue overflow also causes a full scan.
#[cfg(target_os = "linux")]
pub async fn watch(path: PathBuf, changed: impl Fn() + Send + Sync) {
    use nix::libc;
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::time::Duration;
    use tokio::io::unix::AsyncFd;

    fn arm(path: &std::path::Path) -> std::io::Result<AsyncFd<OwnedFd>> {
        // SAFETY: no borrowed pointers; a successful fd is owned exactly once.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut existing = path.to_path_buf();
        while !existing.is_dir() {
            if !existing.pop() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no manifest ancestor",
                ));
            }
        }
        let mask = libc::IN_CREATE
            | libc::IN_DELETE
            | libc::IN_MOVED_FROM
            | libc::IN_MOVED_TO
            | libc::IN_CLOSE_WRITE
            | libc::IN_ATTRIB
            | libc::IN_DELETE_SELF
            | libc::IN_MOVE_SELF;
        for directory in std::iter::once(existing.as_path()).chain(existing.parent()) {
            let name = CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "NUL in manifest path")
            })?;
            // SAFETY: name is NUL terminated and remains valid for this call.
            if unsafe { libc::inotify_add_watch(fd.as_raw_fd(), name.as_ptr(), mask) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        AsyncFd::new(fd)
    }

    let mut retry = Duration::from_millis(100);
    loop {
        let fd = match arm(&path) {
            Ok(fd) => fd,
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "manifest watch unavailable; retrying");
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        retry = Duration::from_millis(100);
        // Includes recovery/overflow: a complete scan closes any re-arm gap.
        changed();
        let result = async {
            let mut buffer = [0_u8; 65536];
            loop {
                let mut ready = fd.readable().await?;
                match ready.try_io(|fd| {
                    // SAFETY: buffer is valid for its stated length and read
                    // cannot outlive this synchronous call.
                    let n = unsafe {
                        libc::read(
                            fd.get_ref().as_raw_fd(),
                            buffer.as_mut_ptr().cast(),
                            buffer.len(),
                        )
                    };
                    if n < 0 {
                        Err(std::io::Error::last_os_error())
                    } else {
                        Ok(n)
                    }
                }) {
                    Ok(result) => return result.map(|_| ()),
                    Err(_) => continue,
                }
            }
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "manifest watch read failed");
            tokio::time::sleep(retry).await;
        }
        // Reopen to follow directory replacement or a newly created ancestor.
    }
}

#[cfg(not(target_os = "linux"))]
pub async fn watch(_path: PathBuf, _changed: impl Fn() + Send + Sync) {
    tracing::warn!("static-manifest event monitoring requires Linux");
    std::future::pending::<()>().await;
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[tokio::test]
    async fn creation_of_missing_directory_and_manifest_wakes_reader() {
        let root = std::env::temp_dir().join(format!("kubelet-watch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let manifests = root.join("manifests");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let path = manifests.clone();
        let task = tokio::spawn(watch(path, move || {
            let _ = tx.send(());
        }));
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        std::fs::create_dir(&manifests).unwrap();
        std::fs::write(manifests.join("pod.yaml"), "kind: Pod\n").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        task.abort();
        let _ = task.await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
