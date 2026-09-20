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
/// blank instead of minting a new one — minting per claim would put an `mkfs`
/// back on the pod-start path, which is the whole thing being avoided.
///
/// **The class is also the quota.** A claim rounds up to one and that class is
/// its ceiling, so the ladder is how a volume's size is limited without anyone
/// writing a quota system — which is why it starts at 1 MiB rather than at a
/// size that would be convenient to allocate. A volume holding one config file
/// or one small state file is a real and common shape, and giving it 64 MiB
/// would not waste space (a blank is sparse) but would waste the limit.
/// A claim larger than the biggest is refused rather than rounded down.
///
/// The list does **not** have to match what the image ships. A class with no
/// blank is minted on first use (#45), so this is the ladder the node offers
/// rather than an inventory of what was baked in. An image that carries the
/// common classes saves the first claim one `mkfs`; one that carries none
/// still works.
pub const SIZE_CLASSES: &[(&str, u64)] = &[
    ("1M", 1024 * 1024),
    ("16M", 16 * 1024 * 1024),
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

/// The StorageClass this node provisions for itself.
pub const STORAGE_CLASS: &str = "stormblock";

/// Does this node provision this claim, or does it belong to someone else?
///
/// Every claim on the node used to become a stormblock clone regardless of
/// who else thought they owned it (#44). That is harmless while this is the
/// only provisioner and fails *silently* the moment it is not: with a CSI
/// driver deployed, one claim is provisioned twice — the driver writes a PV
/// and binds the claim, this node independently clones its own volume, and
/// the pod runs on the node's. The CSI volume is real, allocated and mounted
/// by nobody, so `kubectl` shows a healthy bound claim pointing at a volume
/// holding no data. It surfaces as "my data vanished" after a reschedule.
///
/// The three cases are not symmetrical:
///
/// - **named** — ours, or another provisioner's. Only ours is provisioned here.
/// - **`""`** — an *explicit opt-out* from dynamic provisioning: the claim is
///   asking to be bound to a PV an administrator created, so provisioning one
///   does the opposite of what it asked. rustkube's binder draws the same
///   distinction in `claim_class`, and a kubelet that missed it would
///   provision over static PVs.
/// - **unset** — the control plane stamps the default class onto a claim that
///   has none, so this is the window before that write lands rather than a
///   claim with no class.
///
/// The unset case is right while stormblock is the default class and wrong the
/// day it is not: a claim that would have been stamped with another class gets
/// a stormblock volume if a pod races the binder to it. Closing that needs the
/// StorageClass object and a match on its `provisioner` rather than its name,
/// which is worth doing when the provisioning controller defines the class.
pub fn provisioned_here(pvc: &Value) -> bool {
    match pvc["spec"]["storageClassName"].as_str() {
        Some("") => false,
        Some(class) => class == STORAGE_CLASS,
        None => true,
    }
}

/// Does this claim ask for `ReadWriteOncePod`?
///
/// The mode that `ReadWriteOnce` is routinely mistaken for: RWO is one *node*
/// and lets any number of pods on that node share the volume, RWOP is one
/// *pod* and is the only mode that says so. A claim listing several modes
/// containing RWOP is treated as RWOP, because the strictest mode a claim
/// asks for is the one that has to hold.
pub fn is_rwop(pvc: &Value) -> bool {
    pvc["spec"]["accessModes"]
        .as_array()
        .map(|modes| modes.iter().any(|m| m.as_str() == Some("ReadWriteOncePod")))
        .unwrap_or(false)
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
    fn rwop_is_recognised_and_not_confused_with_rwo() {
        assert!(is_rwop(&json!({"spec": {"accessModes": ["ReadWriteOncePod"]}})));
        // The strictest mode a claim lists is the one that has to hold.
        assert!(is_rwop(
            &json!({"spec": {"accessModes": ["ReadWriteOnce", "ReadWriteOncePod"]}})
        ));
        // RWO is one node, not one pod — several pods on this node may share it.
        assert!(!is_rwop(&json!({"spec": {"accessModes": ["ReadWriteOnce"]}})));
        assert!(!is_rwop(&json!({"spec": {"accessModes": ["ReadWriteMany"]}})));
        // No accessModes at all is not an exclusivity request.
        assert!(!is_rwop(&json!({"spec": {}})));
    }

    #[test]
    fn the_size_class_is_the_name_stormblock_is_asked_for() {
        // Minting and lookup must agree on the name. stormcos once had the
        // registry looking a blank up as `pvc-ext4j-<mib>m` while the image
        // called it `pvc-1M`, and neither side could see the other's name —
        // so both sides going through this one function is the fix, and this
        // asserts the shape the minting body sends as `size`.
        for (class, _) in SIZE_CLASSES {
            assert_eq!(template_name(class), format!("pvc-{class}"));
        }
        // The class string is also what stormblock parses as a size, so it
        // has to stay in the form its `resolve_size` reads.
        assert_eq!(class_for(1024 * 1024).unwrap().0, "1M");
        assert_eq!(class_for(100 * 1024 * 1024).unwrap().0, "256M");
        // A claim above the ladder is refused rather than rounded down.
        assert!(class_for(2 * 1024 * 1024 * 1024).is_none());
    }

    #[test]
    fn only_our_own_class_is_provisioned_here() {
        // Ours, by name.
        assert!(provisioned_here(&json!({"spec": {"storageClassName": "stormblock"}})));
        // Somebody else's driver. Provisioning it is the double-provision that
        // leaves a real, allocated, mounted-by-nobody volume behind.
        assert!(!provisioned_here(&json!({"spec": {"storageClassName": "ebs-gp3"}})));
        // An empty string is an explicit opt-out, not "no opinion": the claim
        // wants an administrator's PV, and provisioning one does the opposite.
        assert!(!provisioned_here(&json!({"spec": {"storageClassName": ""}})));
        // Unset is the window before the control plane stamps the default.
        assert!(provisioned_here(&json!({"spec": {}})));
        assert!(provisioned_here(&json!({})));
    }

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
        assert_eq!(class_for(1).map(|c| c.0), Some("1M"));
        assert_eq!(class_for(1024 * 1024 + 1).map(|c| c.0), Some("16M"));
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
        assert_eq!(claim_bytes(&json!({"spec":{}})), 1024 * 1024);
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
