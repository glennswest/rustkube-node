//! The release's one stormd, bound read-only at `/.stormd` (#465, stormcos#822).
//!
//! The owner's decision on stormcos#822 (option A, 2026-11-20): a release
//! carries the stormd image once (`golden-stormd-<commit>`, layout
//! `bin/stormd`), mounted on every node at `/pallets/stormd`, and everything
//! that runs a stormd-based root mounts it read-only at `/.stormd`. The
//! contract is stormcos `docs/STORMD-SHARED.md`.
//!
//! The kubelet's half: a container whose own root has a `/.stormd` directory
//! (an image built for this: the control plane's static pods, the operators)
//! gets the node's stormd image bound there read-only. A root without one gets
//! no bind. A node without the stormd image is said, and the container starts
//! as before: until images switch `/stormd` to a link (phase 3) they carry
//! their own.

use std::path::{Path, PathBuf};

use crate::cri::{Mount, MountPropagation};

/// Where a stormd-based image wants the shared stormd.
pub const MOUNT_POINT: &str = "/.stormd";
/// The engine volume the release mounts the stormd image from.
pub const VOLUME: &str = "stormd";
/// Where it is when the node's mount list does not say.
pub const DEFAULT_SOURCE: &str = "/pallets/stormd";
/// What the image must hold to be a stormd image.
pub const BINARY: &str = "bin/stormd";

/// Where the node's mount list (`rd.stormblock.mount=` on `line`, as
/// [`crate::container_roots::with_mount_list`] builds it) mounts the stormd
/// volume, as `/pallets/<path>`; [`DEFAULT_SOURCE`] when it names none.
pub fn source_in(line: &str) -> String {
    line.split_whitespace()
        .filter_map(|w| w.strip_prefix("rd.stormblock.mount="))
        .flat_map(|list| list.split(','))
        .find_map(|e| {
            let (vol, at) = e.trim_start_matches('?').split_once(':')?;
            (vol == VOLUME).then(|| at.to_string())
        })
        .map(|at| match at.strip_prefix("/p/") {
            Some(rest) => format!("/pallets/{rest}"),
            None => at,
        })
        .unwrap_or_else(|| DEFAULT_SOURCE.to_string())
}

/// The node's stormd image path, read now: the mount list (not the carried
/// list: a carried golden is not mounted).
pub fn node_source() -> String {
    let cmdline = std::fs::read_to_string("/proc/cmdline").unwrap_or_default();
    let mounts = crate::container_roots::MOUNT_LISTS.iter().find_map(|p| std::fs::read_to_string(p).ok());
    source_in(&crate::container_roots::with_mount_list(&cmdline, mounts.as_deref()))
}

/// Whether a container root wants the shared stormd: `/.stormd` is a
/// directory in it, itself and not a link (a link would bind wherever it
/// points, in the node's view).
pub fn wanted(root: &Path) -> bool {
    std::fs::symlink_metadata(root.join(MOUNT_POINT.trim_start_matches('/'))).is_ok_and(|m| m.is_dir())
}

/// What happens for one container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    /// The root has no `/.stormd`: nothing to do.
    NotWanted,
    /// A pod volume is already at `/.stormd`: the pod's wins.
    PodOwns,
    /// The root wants it and the node has no stormd image at `source`.
    Missing { source: String },
    /// The read-only bind to add.
    Add(Mount),
}

/// The bind for a container whose own root is `root`, from the stormd image
/// at `source` (in the kubelet's view, which is the node's for `/pallets`).
pub fn bind_for(root: &Path, mounts: &[Mount], source: &str) -> Bind {
    if !wanted(root) {
        return Bind::NotWanted;
    }
    if mounts.iter().any(|m| m.container_path.trim_end_matches('/') == MOUNT_POINT) {
        return Bind::PodOwns;
    }
    if !PathBuf::from(source).join(BINARY).is_file() {
        return Bind::Missing { source: source.to_string() };
    }
    Bind::Add(Mount {
        container_path: MOUNT_POINT.to_string(),
        host_path: source.to_string(),
        readonly: true,
        propagation: MountPropagation::Private,
        selinux_relabel: false,
        fstype: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().join("root");
        let stormd = d.path().join("stormd");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(stormd.join("bin")).unwrap();
        std::fs::write(stormd.join("bin/stormd"), b"\x7fELF").unwrap();
        (d, root, stormd)
    }

    #[test]
    fn the_mount_lists_stormd_entry_is_the_source() {
        assert_eq!(source_in("BOOT_IMAGE=/vmlinuz rd.stormblock.mount=etcd:/p/etcd,stormd:/p/stormd"), "/pallets/stormd");
        assert_eq!(source_in("rd.stormblock.mount=?stormd:/p/stormd-12"), "/pallets/stormd-12");
        assert_eq!(source_in("rd.stormblock.mount=stormd:/srv/stormd"), "/srv/stormd");
        assert_eq!(source_in("rd.stormblock.mount=stormdrive:/p/stormdrive"), DEFAULT_SOURCE, "only the volume stormd");
        assert_eq!(source_in(""), DEFAULT_SOURCE);
        let line = crate::container_roots::with_mount_list("quiet", Some("# list\nstormd:/p/stormd\nkubelet-data:/var/lib/kubelet\n"));
        assert_eq!(source_in(&line), "/pallets/stormd", "the mounts file, as the kubelet reads it");
    }

    #[test]
    fn a_root_with_stormd_gets_a_read_only_bind() {
        let (_d, root, stormd) = tree();
        std::fs::create_dir(root.join(".stormd")).unwrap();
        let src = stormd.to_string_lossy().into_owned();
        match bind_for(&root, &[], &src) {
            Bind::Add(m) => {
                assert_eq!(m.container_path, "/.stormd");
                assert_eq!(m.host_path, src);
                assert!(m.readonly, "read-only");
                assert!(m.fstype.is_none(), "a bind");
                assert_eq!(m.propagation, MountPropagation::Private);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_root_without_stormd_gets_no_bind() {
        let (_d, root, stormd) = tree();
        assert_eq!(bind_for(&root, &[], &stormd.to_string_lossy()), Bind::NotWanted);
        std::fs::write(root.join(".stormd"), b"").unwrap();
        assert_eq!(bind_for(&root, &[], &stormd.to_string_lossy()), Bind::NotWanted, "a file is no mount point");
    }

    #[test]
    fn a_link_at_stormd_is_not_a_mount_point() {
        let (_d, root, stormd) = tree();
        std::os::unix::fs::symlink(&stormd, root.join(".stormd")).unwrap();
        assert_eq!(bind_for(&root, &[], &stormd.to_string_lossy()), Bind::NotWanted);
    }

    #[test]
    fn a_node_without_the_stormd_image_is_said_and_binds_nothing() {
        let (d, root, _) = tree();
        std::fs::create_dir(root.join(".stormd")).unwrap();
        let none = d.path().join("no-stormd").to_string_lossy().into_owned();
        assert_eq!(bind_for(&root, &[], &none), Bind::Missing { source: none.clone() });
        let empty = d.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        let empty = empty.to_string_lossy().into_owned();
        assert_eq!(bind_for(&root, &[], &empty), Bind::Missing { source: empty.clone() }, "no bin/stormd");
    }

    #[test]
    fn a_pod_volume_at_stormd_wins() {
        let (_d, root, stormd) = tree();
        std::fs::create_dir(root.join(".stormd")).unwrap();
        let pod = [Mount { container_path: "/.stormd/".into(), host_path: "/elsewhere".into(), ..Default::default() }];
        assert_eq!(bind_for(&root, &pod, &stormd.to_string_lossy()), Bind::PodOwns);
    }
}
