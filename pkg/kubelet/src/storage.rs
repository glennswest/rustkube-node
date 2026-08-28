//! PersistentVolumeClaims, backed by stormblock.
//!
//! A claim becomes a **CoW clone of a blank filesystem template**, and the
//! template is minted the first time a size class is asked for — not baked into
//! the image. Baking would spend image space on classes a node may never use
//! and would decide at build time a question only run time can answer, which is
//! which sizes are actually claimed. The first claim of a class pays one
//! `mkfs`; every claim after it is a clone that occupies no space until written.
//!
//! This is the mechanism stormblock already has for exactly this — templates
//! are created, sealed, and cloned, and sbregistry mints its own the same way.
//!
//! **The mount used to be the hard part and is not any more.** A block device
//! has to be mounted by something in the right mount namespace, and making a
//! host-side mount visible inside a container means mount propagation — which
//! `stormpump/docs/pvc.md` calls the constraint that decides everything, and
//! which fails looking like a missing file. It is avoided rather than solved:
//! the container's own child mounts the device, in its own namespace, in the
//! same loop that already mounts its root and its binds. A container already
//! has a mount; this is one more.
//!
//! The path, end to end:
//!
//! 1. round the claim up to a size class
//! 2. get or mint the template for that class (`mkfs` once, ever)
//! 3. clone it — instant, copy-on-write
//! 4. attach it; the local ublk fast path answers with a `/dev/ublkbN` on this
//!    node, with no NVMe round trip
//! 5. hand stormpump that device with `fstype: ext4`, to mount in the container

use serde_json::Value;

/// Size classes a claim is rounded up to.
///
/// Classes rather than exact sizes, so a claim for 100 MiB uses the 256 MiB
/// template instead of minting a new one — minting per claim would put an
/// `mkfs` back on the pod-start path, which is the whole thing being avoided.
/// A claim larger than the biggest is refused rather than rounded down.
///
/// This list must match the blanks the image actually ships, because a class
/// with no blank is a claim that cannot be satisfied. The image carries
/// 64M/256M/1G — a blank is carried twice, as the golden and the slab's clone
/// of it, so larger classes belong on a node whose data drive is sized for
/// them rather than in a 32 GB test image.
pub const SIZE_CLASSES: &[(&str, u64)] = &[
    ("64M", 64 * 1024 * 1024),
    ("256M", 256 * 1024 * 1024),
    ("1G", 1024 * 1024 * 1024),
];

/// The template name for a size class — the key a claim looks up and, on a
/// miss, mints.
pub fn template_name(class: &str) -> String {
    format!("pvc-{class}")
}

/// The smallest class that holds `want` bytes.
///
/// `None` when the claim exceeds the largest class. Refused rather than rounded
/// down: a volume smaller than the claim is a filesystem that fills up
/// unexpectedly, a long way from here.
pub fn class_for(want: u64) -> Option<(&'static str, u64)> {
    SIZE_CLASSES.iter().copied().find(|(_, size)| *size >= want)
}

/// Parse a Kubernetes quantity (`"1Gi"`, `"512Mi"`, `"1000000"`) into bytes.
///
/// Binary suffixes are powers of 1024 and decimal ones powers of 1000, as
/// upstream defines them. `1Gi` and `1G` are different numbers, and treating
/// them alike under-provisions by 7% without saying so.
pub fn parse_quantity(q: &str) -> Option<u64> {
    let q = q.trim();
    let (num, mult) = if let Some(n) = q.strip_suffix("Ki") {
        (n, 1024u64)
    } else if let Some(n) = q.strip_suffix("Mi") {
        (n, 1024 * 1024)
    } else if let Some(n) = q.strip_suffix("Gi") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = q.strip_suffix("Ti") {
        (n, 1024u64.pow(4))
    } else if let Some(n) = q.strip_suffix('K').or_else(|| q.strip_suffix('k')) {
        (n, 1000)
    } else if let Some(n) = q.strip_suffix('M') {
        (n, 1_000_000)
    } else if let Some(n) = q.strip_suffix('G') {
        (n, 1_000_000_000)
    } else if let Some(n) = q.strip_suffix('T') {
        (n, 1_000_000_000_000)
    } else {
        (q, 1)
    };
    num.trim().parse::<u64>().ok().map(|n| n * mult)
}

/// How much a claim asked for, defaulting to the smallest class.
///
/// A claim with no request is legal and means "whatever you have".
pub fn claim_bytes(pvc: &Value) -> u64 {
    pvc["spec"]["resources"]["requests"]["storage"]
        .as_str()
        .and_then(parse_quantity)
        .unwrap_or(SIZE_CLASSES[0].1)
}

/// The volume name for a claim — stable, so a restarted pod finds its data.
///
/// Keyed on namespace and claim name rather than the pod's UID: a pod is
/// recreated with a new UID and must come back to the same volume, which is the
/// entire difference between a persistent claim and a scratch directory.
pub fn volume_name(namespace: &str, claim: &str) -> String {
    format!("pvc-{namespace}-{claim}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn binary_and_decimal_suffixes_are_different_numbers() {
        assert_eq!(parse_quantity("1Gi"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_quantity("1G"), Some(1_000_000_000));
        assert_ne!(parse_quantity("1Gi"), parse_quantity("1G"));
        assert_eq!(parse_quantity("512Mi"), Some(512 * 1024 * 1024));
        assert_eq!(parse_quantity("1048576"), Some(1048576));
        assert_eq!(parse_quantity("nonsense"), None);
    }

    #[test]
    fn a_claim_rounds_up_to_a_class_never_down() {
        assert_eq!(class_for(1).map(|c| c.0), Some("64M"));
        assert_eq!(class_for(64 * 1024 * 1024).map(|c| c.0), Some("64M"));
        assert_eq!(class_for(64 * 1024 * 1024 + 1).map(|c| c.0), Some("256M"));
        assert_eq!(class_for(1024 * 1024 * 1024).map(|c| c.0), Some("1G"));
    }

    #[test]
    fn a_claim_larger_than_the_largest_class_is_refused() {
        assert_eq!(class_for(100 * 1024 * 1024 * 1024), None);
        assert_eq!(class_for(2 * 1024 * 1024 * 1024), None, "no blank ships for 2 GiB yet");
    }

    #[test]
    fn a_claim_with_no_request_gets_the_smallest_class() {
        assert_eq!(claim_bytes(&json!({"spec":{}})), 64 * 1024 * 1024);
        assert_eq!(
            claim_bytes(&json!({"spec":{"resources":{"requests":{"storage":"1Gi"}}}})),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn a_volume_name_survives_the_pod_it_was_made_for() {
        // Keyed on the claim, not the pod: a recreated pod has a new UID and
        // must find the same data, which is what makes the claim persistent.
        assert_eq!(volume_name("app-one", "data"), "pvc-app-one-data");
        assert_eq!(template_name("256M"), "pvc-256M");
    }
}
