//! PersistentVolumeClaims, backed by stormblock.
//!
//! A claim becomes a **CoW clone of a sealed, pre-formatted blank** of its size
//! class. The image ships blanks for the common classes; a class with no blank
//! is minted the first time it is asked for, and stormblock formats and seals
//! it once. Every claim after that is a clone that occupies no space until
//! written: no `mkfs`, no copy, no CSI on the claim's path.
//!
//! This is the mechanism stormblock already has for exactly this — templates
//! are created, sealed, and cloned, and sbregistry mints its own the same way.
//!
//! **The mount avoids mount propagation.** A block device has to be mounted by
//! something in the right mount namespace, and making a host-side mount
//! visible inside a container means propagation — which
//! `stormpump/docs/pvc.md` calls the constraint that decides everything, and
//! which fails looking like a missing file. PID 1 mounts the device on the node
//! under `/run/stormpump/pvc/<device>` (`volume_register_device`, idempotent, so
//! two pods sharing a ReadWriteOnce claim get one mount), and the container
//! binds that directory like any other bind (`stormpump_runtime.rs`).
//!
//! The path, end to end:
//!
//! 1. round the claim up to a size class
//! 2. get the sealed blank for that class, or mint it (formatted once, ever)
//! 3. clone it — instant, copy-on-write
//! 4. attach it; the local ublk fast path answers with a `/dev/ublkbN` on this
//!    node, with no NVMe round trip
//! 5. hand stormpump that device with the class's filesystem (`fstype: ext4`):
//!    PID 1 mounts it on the node and the container binds the mount
//!
//! **A raw block claim** (`volumeMode: Block`, #67) skips 2 and 3: it is a plain
//! thin volume of its class's size, attached the same way, and the device
//! itself is bound at the container's `volumeDevices[].devicePath`. Every class
//! up to 1 PiB serves block claims; a filesystem past the largest ext4 class
//! is a raw volume with the application's own layout, or nothing.

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
///
/// **Up to a pebibyte** (#67). The ladder stopped at 1 GiB, so a database
/// asking for 20Gi was refused and an ordinary application could not get a
/// volume at all; then at 1 TiB. A blank is sparse and stormblock's mkfs does
/// not write out inode tables, so a large class costs its metadata and nothing
/// else until a claim writes. The steps are x4: a claim never gets more than
/// four times what it asked for.
///
/// **Each class names its filesystem** ([`ClassFs`]). Every one is ext4 (owner,
/// #67: "ext4 might work"; stormcos#91's measurements found nothing that forces
/// XFS). The largest classes are offered as raw block volumes only, until the
/// formatter stormblock carries can lay them down — see [`ClassFs::BlockOnly`].
pub const SIZE_CLASSES: &[(&str, u64, ClassFs)] = &[
    ("1M", MIB, ClassFs::Ext4),
    ("16M", 16 * MIB, ClassFs::Ext4),
    ("64M", 64 * MIB, ClassFs::Ext4),
    ("256M", 256 * MIB, ClassFs::Ext4),
    ("1G", GIB, ClassFs::Ext4),
    ("4G", 4 * GIB, ClassFs::Ext4),
    ("16G", 16 * GIB, ClassFs::Ext4),
    ("64G", 64 * GIB, ClassFs::Ext4),
    ("256G", 256 * GIB, ClassFs::Ext4),
    ("1T", TIB, ClassFs::Ext4),
    ("4T", 4 * TIB, ClassFs::Ext4),
    ("16T", 16 * TIB, ClassFs::Ext4),
    ("64T", 64 * TIB, ClassFs::BlockOnly(FORMAT_MEMORY)),
    ("256T", 256 * TIB, ClassFs::BlockOnly(INODE_WRAP)),
    ("1P", 1024 * TIB, ClassFs::BlockOnly(INODE_WRAP)),
];

/// What a size class's blank is formatted as.
///
/// A raw block claim (`volumeMode: Block`) of any class is a plain volume and
/// needs no filesystem, so every class is offered for those. A filesystem
/// claim needs the class's blank, which stormblock formats once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassFs {
    Ext4,
    /// No filesystem blank yet, with why. A filesystem claim of the class
    /// waits with this reason; a block claim is served.
    BlockOnly(&'static str),
}

/// stormblock pins mkfs-ext4 v3.0.0, whose format holds about 8 KiB per block
/// group: 1.3 GiB at 16 TiB, 5.1 GiB at 64 TiB (stormcos#91), in the storage
/// engine every pod on the node depends on. Fixed by mkfs.ext4.rs#10, not
/// yet tagged or carried by stormblock.
const FORMAT_MEMORY: &str = "an ext4 blank this large needs about 5 GiB of stormblock's memory to      format until stormblock carries mkfs.ext4.rs#10; ask for volumeMode: Block, or a smaller claim";
/// mkfs.ext4.rs#9: at 256 TiB and over, the default inode count wraps and the
/// filesystem is not clean, and stormblock's template API has no inode ratio.
const INODE_WRAP: &str = "an ext4 blank this large is laid down with a wrapped inode count      (mkfs.ext4.rs#9) and needs mkfs.ext4.rs#10's memory fix; ask for volumeMode: Block";

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;

/// The filesystem a class's blank is made with, or why it has none.
pub fn class_fs(class: &str) -> Result<&'static str, &'static str> {
    match SIZE_CLASSES.iter().find(|(c, _, _)| *c == class).map(|(_, _, f)| *f) {
        Some(ClassFs::BlockOnly(why)) => Err(why),
        Some(ClassFs::Ext4) | None => Ok("ext4"),
    }
}

/// A size for stormblock, which parses `K`/`M`/`G`/`T` (base 1024) and not
/// `P`: every class is a whole number of MiB, so `<MiB>M` says any of them.
pub fn engine_size(bytes: u64) -> String {
    format!("{}M", bytes / MIB)
}

/// Does this claim ask for a raw block device (`volumeMode: Block`)?
///
/// Absent is `Filesystem`, as upstream defaults it.
pub fn is_block(pvc: &Value) -> bool {
    pvc["spec"]["volumeMode"].as_str() == Some("Block")
}

/// The template name for a size class — the key a claim looks up and, on a
/// miss, mints: `pvc-ext4j-<MiB>m` for an ext4 class (`ext4j`: ext4 with a
/// journal), `pvc-<fs>-<MiB>m` for any other filesystem a class names.
///
/// **The image's name, and sbregistry's.** The image ships its blanks as
/// `pvc-ext4j-1m` … `pvc-ext4j-1024m` (stormcos `deploy/image.toml`), named by
/// sbregistry's rule (`stormblock-registry/src/goldenbuild.rs`), and stormblock
/// adopts them as fstemplates under those names. This looked for `pvc-1M`, so
/// every claim missed the shipped blank, failed to mint its own, and fell back
/// to a scratch directory that does not survive the pod.
pub fn template_name(class: &str) -> String {
    let bytes = SIZE_CLASSES
        .iter()
        .find(|(c, _, _)| *c == class)
        .map(|(_, b, _)| *b)
        .unwrap_or(MIB);
    match class_fs(class) {
        Ok("ext4") | Err(_) => format!("pvc-ext4j-{}m", bytes / MIB),
        Ok(fs) => format!("pvc-{fs}-{}m", bytes / MIB),
    }
}

/// The smallest class that holds `want` bytes.
///
/// `None` when the claim exceeds the largest class. Refused rather than rounded
/// down: a volume smaller than the claim is a filesystem that fills up
/// unexpectedly, a long way from here.
pub fn class_for(want: u64) -> Option<(&'static str, u64)> {
    SIZE_CLASSES.iter().find(|(_, size, _)| *size >= want).map(|(c, b, _)| (*c, *b))
}

/// Parse a Kubernetes quantity (`"1Gi"`, `"512Mi"`, `"3.5Gi"`, `"1000000"`) into bytes.
///
/// Binary suffixes are powers of 1024 and decimal ones powers of 1000, as
/// upstream defines them. `1Gi` and `1G` are different numbers, and treating
/// them alike under-provisions by 7% without saying so.
///
/// **Fractions too** (#64). `3.5Gi` did not parse, and [`claim_bytes`] read a
/// claim it could not parse as asking for nothing, so a 3.5 GiB claim got the
/// 1 MiB class. A fraction is rounded up to a whole byte, as upstream rounds a
/// storage request, and computed in integers so `0.1Gi` is not off by one.
pub fn parse_quantity(q: &str) -> Option<u64> {
    let q = q.trim();
    let split = q.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(q.len());
    let (num, suffix) = q.split_at(split);
    let mult: u64 = match suffix {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "k" | "K" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        _ => return None,
    };
    let (whole, frac) = num.split_once('.').unwrap_or((num, ""));
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    let whole: u64 = if whole.is_empty() { 0 } else { whole.parse().ok()? };
    let mut bytes = whole.checked_mul(mult)?;
    let frac = frac.trim_end_matches('0');
    if !frac.is_empty() {
        if frac.len() > 18 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let digits: u128 = frac.parse().ok()?;
        let scale = 10u128.pow(frac.len() as u32);
        let part = (digits * mult as u128).div_ceil(scale);
        bytes = bytes.checked_add(u64::try_from(part).ok()?)?;
    }
    Some(bytes)
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

/// What a claim is cloned from, when it names a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimSource {
    /// Another claim: its namespace when `dataSourceRef` names one (the
    /// cross-namespace form, e.g. cloning `kube-system/fastetcd-data`), and
    /// its name.
    Claim(Option<String>, String),
    /// A golden, by name (`apiGroup: storm.io, kind: Golden`).
    Golden(String),
}

/// The source a claim asks to be cloned from, if any.
///
/// `dataSourceRef` wins over `dataSource`, as upstream defines it. A source
/// this node cannot clone (a VolumeSnapshot, some other API group) is `None`,
/// so the claim gets a blank rather than a failure; snapshots are
/// stormblock#111.
pub fn claim_source(pvc: &Value) -> Option<ClaimSource> {
    let r = if pvc["spec"]["dataSourceRef"].is_object() {
        &pvc["spec"]["dataSourceRef"]
    } else {
        &pvc["spec"]["dataSource"]
    };
    let name = r["name"].as_str().filter(|n| !n.is_empty())?.to_string();
    let group = r["apiGroup"].as_str().unwrap_or("");
    let ns = r["namespace"].as_str().filter(|n| !n.is_empty()).map(String::from);
    match (group, r["kind"].as_str()?) {
        ("", "PersistentVolumeClaim") => Some(ClaimSource::Claim(ns, name)),
        ("storm.io", "Golden") => Some(ClaimSource::Golden(name)),
        _ => None,
    }
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
        for (class, bytes, _) in SIZE_CLASSES {
            assert_eq!(template_name(class), format!("pvc-ext4j-{}m", bytes / MIB));
        }
        // The names the image ships, exactly.
        assert_eq!(template_name("1M"), "pvc-ext4j-1m");
        assert_eq!(template_name("1G"), "pvc-ext4j-1024m");
        // The class string is also what stormblock parses as a size, so it
        // has to stay in the form its `resolve_size` reads.
        assert_eq!(class_for(1024 * 1024).unwrap().0, "1M");
        assert_eq!(class_for(100 * 1024 * 1024).unwrap().0, "256M");
        // A claim above the ladder is refused rather than rounded down.
        assert_eq!(class_for(2 * TIB).unwrap().0, "4T");
        assert!(class_for(1024 * TIB + 1).is_none());
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
    fn fractions_parse_and_round_up() {
        // A 3.5Gi claim used to parse as nothing and get the 1 MiB class.
        assert_eq!(parse_quantity("3.5Gi"), Some(3584 * 1024 * 1024));
        assert_eq!(class_for(parse_quantity("3.5Gi").unwrap()).map(|c| c.0), Some("4G"));
        assert_eq!(parse_quantity("1.5k"), Some(1500));
        assert_eq!(parse_quantity(".5Ki"), Some(512));
        // Rounded up to a whole byte.
        assert_eq!(parse_quantity("0.1"), Some(1));
        assert_eq!(parse_quantity("0.1Gi"), Some(107_374_183));
        assert_eq!(parse_quantity("2.000"), Some(2));
        assert_eq!(parse_quantity("1500M"), Some(1_500_000_000));
        assert_eq!(parse_quantity("."), None);
        assert_eq!(parse_quantity("1.2.3Gi"), None);
        assert_eq!(parse_quantity("1Xi"), None);
        assert_eq!(parse_quantity(""), None);
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
        assert_eq!(class_for(1024 * TIB + 1), None);
    }

    #[test]
    fn an_ordinary_application_gets_a_volume() {
        // A database asking for 20Gi was refused when the ladder stopped at 1G.
        assert_eq!(class_for(parse_quantity("20Gi").unwrap()).map(|c| c.0), Some("64G"));
        assert_eq!(class_for(parse_quantity("2Gi").unwrap()).map(|c| c.0), Some("4G"));
        assert_eq!(class_for(parse_quantity("500Gi").unwrap()).map(|c| c.0), Some("1T"));
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
    fn a_claim_names_what_it_is_cloned_from() {
        let pvc = |ds: Value| json!({"spec": {"dataSource": ds}});
        assert_eq!(
            claim_source(&pvc(json!({"kind": "PersistentVolumeClaim", "name": "db"}))),
            Some(ClaimSource::Claim(None, "db".into()))
        );
        // Across namespaces, the way a service's data is cloned.
        assert_eq!(
            claim_source(&json!({"spec": {"dataSourceRef":
                {"kind": "PersistentVolumeClaim", "name": "fastetcd-data", "namespace": "kube-system"}}})),
            Some(ClaimSource::Claim(Some("kube-system".into()), "fastetcd-data".into()))
        );
        assert_eq!(
            claim_source(&pvc(json!({"apiGroup": "storm.io", "kind": "Golden", "name": "fedora"}))),
            Some(ClaimSource::Golden("fedora".into()))
        );
        // Not ours to clone: a blank instead.
        assert_eq!(
            claim_source(&pvc(json!({"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": "s"}))),
            None
        );
        assert_eq!(claim_source(&json!({"spec": {}})), None);
        // dataSourceRef wins.
        assert_eq!(
            claim_source(&json!({"spec": {
                "dataSource": {"kind": "PersistentVolumeClaim", "name": "a"},
                "dataSourceRef": {"kind": "PersistentVolumeClaim", "name": "b"},
            }})),
            Some(ClaimSource::Claim(None, "b".into()))
        );
    }

    #[test]
    fn a_volume_name_survives_the_pod_it_was_made_for() {
        // Keyed on the claim, not the pod: a recreated pod has a new UID and
        // must find the same data, which is what makes the claim persistent.
        assert_eq!(volume_name("app-one", "data"), "pvc-app-one-data");
        assert_eq!(template_name("256M"), "pvc-ext4j-256m");
    }

    #[test]
    fn the_ladder_reaches_a_pebibyte_in_steps_of_four() {
        let sizes: Vec<u64> = SIZE_CLASSES.iter().map(|(_, b, _)| *b).collect();
        assert!(sizes.windows(2).all(|w| w[1] == 4 * w[0] || (w[0] == MIB && w[1] == 16 * MIB)));
        assert_eq!(class_for(parse_quantity("2Ti").unwrap()).map(|c| c.0), Some("4T"));
        assert_eq!(class_for(parse_quantity("10Ti").unwrap()).map(|c| c.0), Some("16T"));
        assert_eq!(class_for(parse_quantity("17Ti").unwrap()).map(|c| c.0), Some("64T"));
        assert_eq!(class_for(parse_quantity("1Pi").unwrap()).map(|c| c.0), Some("1P"));
        assert_eq!(class_for(parse_quantity("1Pi").unwrap() + 1), None);
    }

    #[test]
    fn each_class_names_its_filesystem_and_the_largest_are_block_only() {
        for c in ["1M", "1T", "4T", "16T"] {
            assert_eq!(class_fs(c), Ok("ext4"), "{c}");
        }
        assert_eq!(template_name("4T"), "pvc-ext4j-4194304m");
        assert_eq!(template_name("16T"), "pvc-ext4j-16777216m");
        for c in ["64T", "256T", "1P"] {
            let why = class_fs(c).unwrap_err();
            assert!(why.contains("mkfs.ext4.rs#") && why.contains("volumeMode: Block"), "{c}: {why}");
        }
    }

    #[test]
    fn sizes_are_said_in_mebibytes_because_stormblock_has_no_p() {
        assert_eq!(engine_size(MIB), "1M");
        assert_eq!(engine_size(1024 * TIB), "1073741824M");
    }

    #[test]
    fn block_is_asked_for_and_filesystem_is_the_default() {
        assert!(is_block(&json!({"spec": {"volumeMode": "Block"}})));
        assert!(!is_block(&json!({"spec": {"volumeMode": "Filesystem"}})));
        assert!(!is_block(&json!({"spec": {}})));
    }
}
