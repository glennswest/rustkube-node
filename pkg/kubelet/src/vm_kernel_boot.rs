//! Where a direct-kernel VM's kernel and initramfs may come from (#421,
//! stormvm#111, vmcloud-image-operator#62).
//!
//! A VMI opts in with `storm.io/kernel` (and `storm.io/initrd`,
//! `storm.io/cmdline`), which stormvm renders as the hypervisor's `-kernel`
//! and `-initrd` (cloud-hypervisor `--kernel`, `--initramfs`). vmimages
//! reads each golden's kernel and initramfs out of it on every node that has
//! the golden and publishes the paths on its `CloudImage`'s `status.boot`:
//! `/data/vmimages/boot/<golden>/vmlinuz` and `…/initrd`.
//!
//! **No mount is made for them.** The machine's spec is `Root::Inherit`,
//! the host's mount view, and `/data/vmimages` is mounted on the host, so
//! the hypervisor opens those paths as they are named.
//!
//! **That is why the paths are checked.** In the host's view, a VMI naming
//! `/data/stormcert/…` as its initramfs would have the hypervisor read the
//! node's key into guest memory. A path must be absolute, plain (no `.` or
//! `..` component) and under one of the kernel directories
//! (`--vm-kernel-dirs`, default [`DEFAULT_DIR`]); otherwise the start fails
//! and says why.

use std::path::{Component, Path};

use stormvm_spec::VmSpec;

/// vmimages' `boot.nodeDir`.
pub const DEFAULT_DIR: &str = "/data/vmimages/boot";

/// `--vm-kernel-dirs`: a comma-separated list; blanks dropped.
pub fn parse_dirs(text: &str) -> Vec<String> {
    text.split(',').map(str::trim).filter(|d| !d.is_empty()).map(str::to_string).collect()
}

/// Why `path` (`what`: the annotation) may not be read, if it may not.
fn path_refusal(what: &str, path: &str, dirs: &[String]) -> Option<String> {
    let p = Path::new(path);
    let plain = p.is_absolute() && p.components().all(|c| matches!(c, Component::RootDir | Component::Normal(_)));
    let under = dirs.iter().any(|d| {
        let d = Path::new(d);
        d.is_absolute() && p.starts_with(d) && p != d
    });
    if plain && under {
        return None;
    }
    Some(format!(
        "{what} {path:?} is not a file under the kernel directories ({}): the hypervisor reads it in the node's view, \
         so only those are allowed (--vm-kernel-dirs)",
        if dirs.is_empty() { "none".to_string() } else { dirs.join(", ") }
    ))
}

/// Why `vm`'s kernel or initramfs may not be read, if it may not. A VM that
/// boots from firmware names neither and is never refused.
pub fn refusal(vm: &VmSpec, dirs: &[String]) -> Option<String> {
    use stormvm_spec::kube::{ANN_INITRD, ANN_KERNEL};
    let f = &vm.firmware;
    f.kernel
        .as_deref()
        .and_then(|k| path_refusal(ANN_KERNEL, k, dirs))
        .or_else(|| f.initrd.as_deref().and_then(|i| path_refusal(ANN_INITRD, i, dirs)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vm(ann: serde_json::Value) -> VmSpec {
        stormvm_spec::kube::from_kube(&json!({
            "kind": "VirtualMachineInstance",
            "metadata": {"name": "web", "namespace": "default", "annotations": ann},
            "spec": {"domain": {"memory": {"guest": "1Gi"}, "devices": {"disks": [{"name": "root", "disk": {"bus": "virtio"}}]}},
                     "volumes": [{"name": "root", "containerDisk": {"image": "fedora-44"}}]}
        }))
        .unwrap()
    }

    fn dirs() -> Vec<String> {
        vec![DEFAULT_DIR.to_string()]
    }

    #[test]
    fn the_goldens_own_kernel_and_initrd_are_allowed() {
        let v = vm(json!({
            "storm.io/kernel": "/data/vmimages/boot/fedora-44-x86_64/vmlinuz",
            "storm.io/initrd": "/data/vmimages/boot/fedora-44-x86_64/initrd",
            "storm.io/cmdline": "root=UUID=x ro console=ttyS0",
        }));
        assert_eq!(refusal(&v, &dirs()), None);
    }

    #[test]
    fn a_firmware_boot_is_never_refused() {
        assert_eq!(refusal(&vm(json!({})), &dirs()), None);
        assert_eq!(refusal(&vm(json!({})), &[]), None);
    }

    #[test]
    fn a_path_outside_or_escaping_the_kernel_directories_is_refused() {
        let k = |kernel: &str| refusal(&vm(json!({"storm.io/kernel": kernel})), &dirs());
        assert!(k("/data/stormcert/node.key.pem").unwrap().contains("not a file under the kernel directories"));
        assert!(k("/data/vmimages/boot/../../stormcert/node.key.pem").is_some(), "no ..");
        assert!(k("/data/vmimages/boot/./g/vmlinuz").is_some(), "no .");
        assert!(k("data/vmimages/boot/g/vmlinuz").is_some(), "absolute only");
        assert!(k("/data/vmimages/boot").is_some(), "the directory itself is no kernel");
        assert!(k("/data/vmimages/bootleg/vmlinuz").is_some(), "by component, not by prefix");
        // The initramfs as well as the kernel.
        let v = vm(json!({"storm.io/kernel": "/data/vmimages/boot/g/vmlinuz", "storm.io/initrd": "/etc/shadow"}));
        assert!(refusal(&v, &dirs()).unwrap().contains("storm.io/initrd"));
        // No directories: no direct kernel boot at all.
        assert!(refusal(&vm(json!({"storm.io/kernel": "/data/vmimages/boot/g/vmlinuz"})), &[]).unwrap().contains("(none)"));
    }

    #[test]
    fn the_flag_is_a_comma_list() {
        assert_eq!(parse_dirs("/a, /b ,,"), vec!["/a".to_string(), "/b".to_string()]);
        assert!(parse_dirs("").is_empty());
    }
}
