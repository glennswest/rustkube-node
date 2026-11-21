//! Volume expansion for the built-in class (#379): a claim of `stormblock`
//! whose request grows is grown online, the volume by the engine and the
//! filesystem by this node.
//!
//! Upstream splits this between the driver's external-resizer (the volume,
//! then the PV's capacity) and the kubelet's NodeExpand (the filesystem, then
//! the claim's capacity). The in-kubelet driver has no controller of its own,
//! so the node that holds the volume does both halves, writing the same
//! status and events upstream's two do, so `kubectl describe pvc` reads as it
//! would on any cluster:
//!
//! 1. **Resizer**: the claim gets `Resizing`, `allocatedResources` and
//!    `allocatedResourceStatuses.storage: ControllerResizeInProgress`, an event
//!    `Resizing`; the engine grows the volume (`PATCH
//!    /apis/storage.storm.io/v1/volumes/{id}` `spec.sizeBytes`, stormblock#655:
//!    thin, online, the ublk device told the new size); the PV's capacity
//!    follows. A raw block claim is done there (`VolumeResizeSuccessful`).
//!    A filesystem claim is left `FileSystemResizePending` /
//!    `NodeResizePending` with `FileSystemResizeRequired`.
//! 2. **Node**: once the claim's device is mounted on this node, its ext4
//!    (`EXT4_IOC_RESIZE_FS`) or XFS (`XFS_IOC_FSGROWFSDATA`) is grown online
//!    to the device, and the claim's `status.capacity` follows
//!    (`FileSystemResizeSuccessful`). A claim no pod mounts stays pending
//!    until one does, as upstream's does.
//!
//! The engine refuses what it cannot grow (a mirrored or exported volume:
//! 409, a sealed one): the claim says `ControllerResizeInfeasible` with a
//! `VolumeResizeFailed` event, once, and is not tried again until its request
//! changes. A shrink never reaches here: the apiserver refuses it (rustkube
//! `pvc_update`). Admission also refuses a grow unless the class says
//! `allowVolumeExpansion: true` (stormcos#1032).
//!
//! The node's own service volumes (`storm.io/system-volume`) are not grown
//! here: their services mount them, and they grow through their Volume
//! object, which the claim mirror follows.

use retry::RetryExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// The claim's resize conditions, as upstream names them.
pub const RESIZING: &str = "Resizing";
pub const FS_PENDING: &str = "FileSystemResizePending";

/// `allocatedResourceStatuses.storage` values (KEP-1790).
pub const CONTROLLER_IN_PROGRESS: &str = "ControllerResizeInProgress";
pub const CONTROLLER_INFEASIBLE: &str = "ControllerResizeInfeasible";
pub const NODE_PENDING: &str = "NodeResizePending";

/// The size a claim's filesystem is to grow to, when the control plane has
/// grown its volume and left the rest to the node (#42): the claim says
/// `NodeResizePending` (or `FileSystemResizePending`), and the PV's capacity is
/// past the claim's. The PV's quantity as written, and its bytes.
pub fn resize_due(pv: &Value, pvc: &Value) -> Option<(String, u64)> {
    let st = &pvc["status"];
    let pending = st["allocatedResourceStatuses"]["storage"].as_str() == Some(NODE_PENDING)
        || st["conditions"].as_array().is_some_and(|cs| {
            cs.iter().any(|c| c["type"] == FS_PENDING && c["status"] == "True")
        });
    if !pending {
        return None;
    }
    let want = pv["spec"]["capacity"]["storage"].as_str()?;
    let want_bytes = crate::storage::parse_quantity(want)?;
    let have = st["capacity"]["storage"].as_str().and_then(crate::storage::parse_quantity).unwrap_or(0);
    (want_bytes > have).then(|| (want.to_string(), want_bytes))
}

/// The claim's status once its filesystem is grown, as upstream's kubelet
/// writes it: the new capacity, `Resizing` and `FileSystemResizePending` gone,
/// `allocatedResourceStatuses.storage` dropped. A merge patch (conditions
/// replaced whole).
pub fn resized_status(pvc: &Value, quantity: &str) -> Value {
    json!({ "status": {
        "capacity": { "storage": quantity },
        "conditions": conditions_without(pvc, &[RESIZING, FS_PENDING]),
        "allocatedResourceStatuses": null,
    }})
}

/// The claim's conditions with the `drop` types taken out.
fn conditions_without(pvc: &Value, drop: &[&str]) -> Vec<Value> {
    pvc["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| !c["type"].as_str().is_some_and(|t| drop.contains(&t)))
        .cloned()
        .collect()
}

/// One condition, upstream's shape.
fn condition(kind: &str, message: Option<&str>) -> Value {
    let mut c = json!({
        "type": kind,
        "status": "True",
        "lastProbeTime": null,
        "lastTransitionTime": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    });
    if let Some(m) = message {
        c["message"] = json!(m);
    }
    c
}

/// Round up to a MiB: the engine's sizes, a ublk device's and an ext4's block
/// count all divide it.
pub fn round_mib(bytes: u64) -> u64 {
    const MIB: u64 = 1 << 20;
    bytes.div_ceil(MIB) * MIB
}

/// What one claim needs this pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Nothing: the claim has what it asked for, or is waiting on something
    /// that is not this pass's to do.
    Nothing,
    /// The engine is to grow the volume to `to` bytes.
    Grow { to: u64 },
    /// The volume holds the request: write the PV's capacity and the claim's
    /// next state (a pass after the engine grew it, or one that crashed
    /// between the grow and the write).
    Record { bytes: u64 },
    /// The filesystem on this node is to grow to the PV's capacity.
    Expand { quantity: String, bytes: u64 },
}

/// The request, in bytes.
fn requested(pvc: &Value) -> Option<u64> {
    pvc["spec"]["resources"]["requests"]["storage"].as_str().and_then(crate::storage::parse_quantity)
}

fn capacity(pvc: &Value) -> u64 {
    pvc["status"]["capacity"]["storage"].as_str().and_then(crate::storage::parse_quantity).unwrap_or(0)
}

/// Decide a claim's step. `volume_bytes` is the engine's size of its volume.
pub fn decide(pv: &Value, pvc: &Value, volume_bytes: u64) -> Step {
    if pvc["status"]["phase"].as_str() != Some("Bound") || !pvc["metadata"]["deletionTimestamp"].is_null() {
        return Step::Nothing;
    }
    let Some(want) = requested(pvc) else { return Step::Nothing };
    if want > volume_bytes {
        // Refused for this very request already: said once, not again.
        let asked = pvc["spec"]["resources"]["requests"]["storage"].as_str();
        let st = &pvc["status"];
        if st["allocatedResourceStatuses"]["storage"].as_str() == Some(CONTROLLER_INFEASIBLE)
            && st["allocatedResources"]["storage"].as_str() == asked
        {
            return Step::Nothing;
        }
        return Step::Grow { to: round_mib(want) };
    }
    if let Some((quantity, bytes)) = resize_due(pv, pvc) {
        return Step::Expand { quantity, bytes };
    }
    // The volume holds the request but the claim says less, in a grow this
    // started (`allocatedResources` is written before the engine is asked):
    // its later writes did not all land. A claim the binder has not given a
    // capacity yet is not a grow.
    if capacity(pvc) < want && !pvc["status"]["allocatedResources"].is_null() {
        return Step::Record { bytes: volume_bytes };
    }
    Step::Nothing
}

/// The claim's status while the engine grows it.
pub fn resizing_status(pvc: &Value) -> Value {
    let mut cs = conditions_without(pvc, &[RESIZING, FS_PENDING]);
    cs.push(condition(RESIZING, None));
    json!({ "status": {
        "conditions": cs,
        "allocatedResources": { "storage": pvc["spec"]["resources"]["requests"]["storage"] },
        "allocatedResourceStatuses": { "storage": CONTROLLER_IN_PROGRESS },
    }})
}

/// The claim's status once the engine refused the grow for good.
pub fn infeasible_status(pvc: &Value) -> Value {
    json!({ "status": {
        "conditions": conditions_without(pvc, &[RESIZING, FS_PENDING]),
        "allocatedResources": { "storage": pvc["spec"]["resources"]["requests"]["storage"] },
        "allocatedResourceStatuses": { "storage": CONTROLLER_INFEASIBLE },
    }})
}

/// The claim's status once the volume is grown: a block claim is done, a
/// filesystem one waits for the node.
pub fn grown_status(pvc: &Value, quantity: &str) -> Value {
    if crate::storage::is_block(pvc) {
        return resized_status(pvc, quantity);
    }
    let mut cs = conditions_without(pvc, &[RESIZING, FS_PENDING]);
    cs.push(condition(FS_PENDING, Some("Waiting for user to (re-)start a pod to finish file system resize of volume on node.")));
    json!({ "status": {
        "conditions": cs,
        "allocatedResources": { "storage": pvc["spec"]["resources"]["requests"]["storage"] },
        "allocatedResourceStatuses": { "storage": NODE_PENDING },
    }})
}

/// Is this engine refusal for good (the volume cannot be grown as it is),
/// rather than a moment's?
fn infeasible(status: u16) -> bool {
    matches!(status, 400 | 403 | 404 | 409 | 422)
}

/// One pass over this node's built-in claims. `true` while one is left to try
/// again (an engine or apiserver that did not answer, a filesystem not yet
/// grown on a mounted device).
pub async fn pass(
    client: &reqwest::Client,
    api_url: &str,
    engine: &crate::engine::EngineClient,
    node: &str,
    events: Option<&crate::events::EventRecorder>,
) -> bool {
    if api_url.is_empty() {
        return false;
    }
    let get = |url: String| async move {
        match client.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
            _ => None,
        }
    };
    let Some(pvs) = get(format!("{api_url}/api/v1/persistentvolumes")).await else { return true };
    let ours: Vec<(&Value, String)> = pvs["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|pv| pv["metadata"]["labels"][crate::system_claims::LABEL].as_str() != Some("true"))
        .filter(|pv| pv["metadata"]["deletionTimestamp"].is_null())
        .filter(|pv| pv["spec"]["claimRef"]["name"].is_string())
        .filter_map(|pv| Some((pv, crate::pv_placement::ours(pv, node)?)))
        .collect();
    if ours.is_empty() {
        return false;
    }
    let list = match engine.get(&format!("{}/api/v1/volumes", engine.url())).await {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.unwrap_or_default(),
        _ => return true,
    };
    let by_name: HashMap<&str, &Value> =
        list["items"].as_array().into_iter().flatten().filter_map(|v| Some((v["name"].as_str()?, v))).collect();
    let mut pending = false;
    for (pv, volume) in ours {
        let (Some(ns), Some(claim)) = (pv["spec"]["claimRef"]["namespace"].as_str(), pv["spec"]["claimRef"]["name"].as_str())
        else {
            continue;
        };
        // A claim that is gone (404) is nothing to do; one that could not be
        // read is tried again (#457, 571e7c23b8e4): the event that asked for
        // the grow has been taken, and no other may come.
        let url = format!("{api_url}/api/v1/namespaces/{ns}/persistentvolumeclaims/{claim}");
        let pvc = match client.get(&url).send_retrying(retry::Policy::API).await {
            Ok(r) if r.status().is_success() => match r.json::<Value>().await {
                Ok(v) => v,
                Err(e) => {
                    debug!("claim {ns}/{claim}: not read ({e}); tried again");
                    pending = true;
                    continue;
                }
            },
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => continue,
            Ok(r) => {
                debug!("claim {ns}/{claim}: not read ({}); tried again", r.status());
                pending = true;
                continue;
            }
            Err(e) => {
                debug!("claim {ns}/{claim}: not read ({e}); tried again");
                pending = true;
                continue;
            }
        };
        if pvc["spec"]["volumeName"].as_str() != pv["metadata"]["name"].as_str() {
            continue;
        }
        let Some(vol) = by_name.get(volume.as_str()) else {
            // Not in the engine's listing: tried again while the claim asks
            // for work (#457), decided on the PV's own capacity.
            let pv_bytes = pv["spec"]["capacity"]["storage"].as_str().and_then(crate::storage::parse_quantity).unwrap_or(0);
            if decide(pv, &pvc, pv_bytes) != Step::Nothing {
                debug!("claim {ns}/{claim}: volume {volume} is not in the engine's listing; tried again");
                pending = true;
            }
            continue;
        };
        let bytes = vol["virtual_size_bytes"].as_u64().unwrap_or(0);
        let step = decide(pv, &pvc, bytes);
        if step == Step::Nothing {
            continue;
        }
        let w = Writer { client, api_url, events, pv, pvc: &pvc, ns, claim };
        pending |= match step {
            Step::Nothing => false,
            Step::Grow { to } => grow(&w, engine, vol, &volume, to).await,
            Step::Record { bytes } => !w.record(bytes).await,
            Step::Expand { quantity, bytes } => expand(&w, vol, &quantity, bytes).await,
        };
    }
    pending
}

/// What a pass writes for one claim.
struct Writer<'a> {
    client: &'a reqwest::Client,
    api_url: &'a str,
    events: Option<&'a crate::events::EventRecorder>,
    pv: &'a Value,
    pvc: &'a Value,
    ns: &'a str,
    claim: &'a str,
}

impl Writer<'_> {
    fn pv_name(&self) -> &str {
        self.pv["metadata"]["name"].as_str().unwrap_or_default()
    }

    async fn merge(&self, url: String, patch: &Value) -> bool {
        self.client
            .patch(&url)
            .header("content-type", "application/merge-patch+json")
            .json(patch)
            .send_retrying(retry::Policy::API)
            .await
            .is_ok_and(|r| r.status().is_success())
    }

    async fn claim_status(&self, patch: &Value) -> bool {
        let url = format!("{}/api/v1/namespaces/{}/persistentvolumeclaims/{}/status", self.api_url, self.ns, self.claim);
        self.merge(url, patch).await
    }

    async fn event(&self, etype: &str, reason: &str, message: &str) {
        if let Some(r) = self.events {
            r.object_event("v1", "PersistentVolumeClaim", self.pvc, etype, reason, message).await;
        }
    }

    /// The PV's capacity, then the claim's next state, with its event.
    async fn record(&self, bytes: u64) -> bool {
        let quantity = crate::system_claims::quantity(bytes);
        let pv_url = format!("{}/api/v1/persistentvolumes/{}", self.api_url, self.pv_name());
        let pv_has = self.pv["spec"]["capacity"]["storage"].as_str().and_then(crate::storage::parse_quantity).unwrap_or(0);
        if pv_has < bytes && !self.merge(pv_url, &json!({ "spec": { "capacity": { "storage": quantity } } })).await {
            return false;
        }
        if !self.claim_status(&grown_status(self.pvc, &quantity)).await {
            return false;
        }
        let pv = self.pv_name();
        if crate::storage::is_block(self.pvc) {
            info!("claim {}/{}: volume {pv} grown to {quantity} (block)", self.ns, self.claim);
            self.event("Normal", "VolumeResizeSuccessful", &format!("resize volume \"{pv}\" by resizer \"stormblock.storm.io\" succeeded")).await;
        } else {
            info!("claim {}/{}: volume {pv} grown to {quantity}; its filesystem grows on the node", self.ns, self.claim);
            self.event("Normal", "FileSystemResizeRequired", "Require file system resize of volume on node").await;
        }
        true
    }
}

/// The resizer's half. `true` while it is to be tried again.
async fn grow(w: &Writer<'_>, engine: &crate::engine::EngineClient, vol: &Value, volume: &str, to: u64) -> bool {
    let Some(id) = vol["id"].as_str() else { return false };
    if !w.claim_status(&resizing_status(w.pvc)).await {
        return true;
    }
    let pv = w.pv_name().to_string();
    w.event("Normal", "Resizing", &format!("External resizer is resizing volume {pv}")).await;
    let url = format!("{}/apis/storage.storm.io/v1/volumes/{id}", engine.url());
    let body = json!({ "spec": { "sizeBytes": to } });
    let answer = engine.send(reqwest::Method::PATCH, &url, Some(&body)).await;
    match answer {
        Ok(r) if r.status().is_success() => {
            let v: Value = r.json().await.unwrap_or_default();
            debug!(volume, "grown: {}", v["status"]["lastResize"]);
            !w.record(to).await
        }
        Ok(r) => {
            let code = r.status().as_u16();
            let text = r.text().await.unwrap_or_default();
            let why = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v["message"].as_str().map(str::to_string))
                .unwrap_or(text);
            let msg = format!("resize volume \"{pv}\" by resizer \"stormblock.storm.io\" failed: {code}: {why}");
            warn!("claim {}/{}: {msg}", w.ns, w.claim);
            w.event("Warning", "VolumeResizeFailed", &msg).await;
            if infeasible(code) {
                w.claim_status(&infeasible_status(w.pvc)).await;
                false
            } else {
                true
            }
        }
        Err(e) => {
            debug!(volume, "engine did not answer the grow: {e}");
            true
        }
    }
}

/// The node's half: the filesystem on the claim's device, where it is
/// mounted on this node. `true` while it is to be tried again.
async fn expand(w: &Writer<'_>, vol: &Value, quantity: &str, bytes: u64) -> bool {
    let Some(device) = device_of(vol) else {
        // Not attached here: no pod has it. Upstream's kubelet grows it when
        // one mounts it, and so does this (the pod's start is a pass).
        debug!("claim {}/{}: filesystem grow waits for a pod here (the volume is not attached)", w.ns, w.claim);
        return false;
    };
    let mountinfo = std::fs::read_to_string("/proc/1/mountinfo")
        .or_else(|_| std::fs::read_to_string("/proc/self/mountinfo"))
        .unwrap_or_default();
    let Some(mount) = mount_of(&mountinfo, &device, dev_numbers(&device)) else {
        debug!("claim {}/{}: filesystem grow waits for a pod here ({device} is not mounted)", w.ns, w.claim);
        return false;
    };
    let pv = w.pv_name().to_string();
    let grown = tokio::task::spawn_blocking(move || grow_mounted(&device, &mount, bytes))
        .await
        .unwrap_or_else(|e| Err(e.to_string()));
    match grown {
        Ok(how) => {
            info!("claim {}/{}: {how}", w.ns, w.claim);
            if !w.claim_status(&resized_status(w.pvc, quantity)).await {
                return true;
            }
            w.event("Normal", "FileSystemResizeSuccessful", &format!("MountVolume.NodeExpandVolume succeeded for volume \"{pv}\"")).await;
            false
        }
        Err(e) => {
            warn!("claim {}/{}: filesystem not grown: {e}", w.ns, w.claim);
            w.event("Warning", "FileSystemResizeFailed", &format!("MountVolume.NodeExpandVolume failed for volume \"{pv}\": {e}")).await;
            true
        }
    }
}

/// The claim's ublk device on this node, from the engine's listing.
pub fn device_of(vol: &Value) -> Option<String> {
    vol["attachments"]
        .as_array()?
        .iter()
        .find(|a| a["transport"].as_str() == Some("ublk"))?["device"]
        .as_str()
        .filter(|d| !d.is_empty())
        .map(str::to_string)
}

/// Where `device` is mounted, from a mountinfo: the claim's own mount
/// (`/run/stormpump/pvc/<dev>`) first, else the first mount of it. A line is
/// the device's by its source, or by its `maj:min` when that is known (the
/// engine finds a device's mounts the same way, stormblock `mounts_of`).
pub fn mount_of(mountinfo: &str, device: &str, numbers: Option<(u32, u32)>) -> Option<String> {
    let majmin = numbers.map(|(a, b)| format!("{a}:{b}"));
    let mut found: Vec<String> = mountinfo
        .lines()
        .filter_map(|l| {
            let (left, right) = l.split_once(" - ")?;
            let source = right.split(' ').nth(1)?;
            let fields: Vec<&str> = left.split(' ').collect();
            let ours = source == device || majmin.as_deref().is_some_and(|m| fields.get(2) == Some(&m));
            ours.then(|| fields.get(4).map(|m| unescape(m)))?
        })
        .collect();
    let dev = device.rsplit('/').next().unwrap_or(device);
    let own = format!("/run/stormpump/pvc/{dev}");
    if let Some(i) = found.iter().position(|m| *m == own) {
        return Some(found.swap_remove(i));
    }
    found.into_iter().next()
}

fn unescape(s: &str) -> String {
    s.replace("\\040", " ").replace("\\011", "\t").replace("\\012", "\n").replace("\\134", "\\")
}

const EXT4_SUPER_MAGIC: u64 = 0xEF53;
const XFS_SUPER_MAGIC: u64 = 0x5846_5342;
/// `_IOW('f', 16, __u64)`.
const EXT4_IOC_RESIZE_FS: libc::c_ulong = 0x4008_6610;
/// `_IOR('X', 100, struct xfs_fsop_geom_v1)` (112 bytes).
const XFS_IOC_FSGEOMETRY_V1: libc::c_ulong = 0x8070_5864;
/// `_IOW('X', 110, struct xfs_growfs_data)` (16 bytes).
const XFS_IOC_FSGROWFSDATA: libc::c_ulong = 0x4010_586e;

/// The block device's `maj:min`, from sysfs (no device node needed).
pub fn dev_numbers(device: &str) -> Option<(u32, u32)> {
    let dev = device.rsplit('/').next()?;
    let text = std::fs::read_to_string(format!("/sys/class/block/{dev}/dev")).ok()?;
    let (a, b) = text.trim().split_once(':')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Where this process can reach a claim's filesystem that PID 1 mounted at
/// `mount` (`/run/stormpump/pvc/<dev>`), in the order tried.
///
/// The plain path is **not** enough: the kubelet's `/run` is a private bind
/// of the node's (stormcos `mount hostrun /run`), so a claim PID 1 mounts
/// after the kubelet started is, from here, the empty tmpfs directory under
/// it. PID 1's own root sees the mount (the kubelet shares the node's PID
/// namespace); `/hostroot` is the node's root bound `rslave`, which hears it.
pub fn claim_candidates(mount: &str) -> Vec<std::path::PathBuf> {
    vec![format!("/proc/1/root{mount}").into(), mount.into(), format!("/hostroot{mount}").into()]
}

/// The first of `candidates` that is on the device `numbers`: the directory
/// to open for the claim's filesystem. An error names what each one was.
pub fn on_device(numbers: (u32, u32), candidates: &[std::path::PathBuf]) -> Result<std::path::PathBuf, String> {
    use std::os::unix::fs::MetadataExt;
    let mut seen = Vec::new();
    for c in candidates {
        match std::fs::metadata(c) {
            Ok(m) => {
                let have = (libc::major(m.dev()), libc::minor(m.dev()));
                if have == numbers {
                    return Ok(c.clone());
                }
                seen.push(format!("{} is on {}:{}", c.display(), have.0, have.1));
            }
            Err(e) => seen.push(format!("{}: {e}", c.display())),
        }
    }
    Err(format!("the filesystem of device {}:{} is not reachable here ({})", numbers.0, numbers.1, seen.join("; ")))
}

/// The directory of the claim's filesystem on `device`, mounted at `mount`
/// by PID 1, as this process can open it ([`claim_candidates`]).
pub fn claim_dir(device: &str, mount: &str) -> Result<std::path::PathBuf, String> {
    let numbers = dev_numbers(device).ok_or_else(|| format!("{device}: no maj:min in /sys/class/block"))?;
    on_device(numbers, &claim_candidates(mount))
}

/// The bytes the kernel says the block device holds.
fn device_bytes(device: &str) -> Option<u64> {
    let dev = device.rsplit('/').next()?;
    let sectors: u64 = std::fs::read_to_string(format!("/sys/class/block/{dev}/size")).ok()?.trim().parse().ok()?;
    Some(sectors * 512)
}

/// Grow the filesystem mounted at `mount` (on `device`) online to `bytes`,
/// or to the device, whichever is smaller. What was done, in words.
// `statfs`'s field widths differ between libcs (glibc's signed, musl's
// unsigned): the casts are needed on one and not the other.
#[allow(clippy::unnecessary_cast)]
pub fn grow_mounted(device: &str, mount: &str, bytes: u64) -> Result<String, String> {
    use std::os::fd::AsRawFd;
    let target = match device_bytes(device) {
        Some(d) if d < bytes => {
            return Err(format!("{device} holds {d} bytes, not yet {bytes}: the engine has not told the device"))
        }
        _ => bytes,
    };
    // The claim's own filesystem, never the directory under it.
    let path = claim_dir(device, mount)?;
    let dir = std::fs::File::open(&path).map_err(|e| format!("opening {}: {e}", path.display()))?;
    let fd = dir.as_raw_fd();
    let mut sfs: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: fd is open for the call; sfs is a valid out-pointer.
    if unsafe { libc::fstatfs(fd, &mut sfs) } != 0 {
        return Err(format!("statfs {mount}: {}", std::io::Error::last_os_error()));
    }
    match sfs.f_type as u64 {
        EXT4_SUPER_MAGIC => {
            let bsize = sfs.f_bsize as u64;
            if bsize == 0 {
                return Err(format!("{mount}: block size 0"));
            }
            let blocks: u64 = target / bsize;
            // SAFETY: the ioctl reads one u64 the pointer names.
            if unsafe { libc::ioctl(fd, EXT4_IOC_RESIZE_FS as _, &blocks as *const u64) } != 0 {
                return Err(format!("EXT4_IOC_RESIZE_FS {mount} to {blocks} blocks: {}", std::io::Error::last_os_error()));
            }
            Ok(format!("ext4 at {mount} grown online to {blocks} blocks of {bsize}"))
        }
        XFS_SUPER_MAGIC => {
            let mut geom = [0u8; 112];
            // SAFETY: the ioctl writes 112 bytes into geom.
            if unsafe { libc::ioctl(fd, XFS_IOC_FSGEOMETRY_V1 as _, geom.as_mut_ptr()) } != 0 {
                return Err(format!("XFS_IOC_FSGEOMETRY {mount}: {}", std::io::Error::last_os_error()));
            }
            let bsize = u32::from_ne_bytes(geom[0..4].try_into().expect("4 bytes")) as u64;
            let imaxpct = u32::from_ne_bytes(geom[28..32].try_into().expect("4 bytes"));
            if bsize == 0 {
                return Err(format!("{mount}: block size 0"));
            }
            #[repr(C)]
            struct GrowData {
                newblocks: u64,
                imaxpct: u32,
            }
            let data = GrowData { newblocks: target / bsize, imaxpct };
            // SAFETY: the ioctl reads one xfs_growfs_data the pointer names.
            if unsafe { libc::ioctl(fd, XFS_IOC_FSGROWFSDATA as _, &data as *const GrowData) } != 0 {
                return Err(format!("XFS_IOC_FSGROWFSDATA {mount}: {}", std::io::Error::last_os_error()));
            }
            Ok(format!("xfs at {mount} grown online to {} blocks of {bsize}", data.newblocks))
        }
        other => Err(format!("{mount} is filesystem type {other:#x}: only ext4 and xfs grow online here")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(cap: &str) -> Value {
        json!({ "metadata": { "name": "pvc-web-data" }, "spec": { "capacity": { "storage": cap } } })
    }

    fn pvc(req: &str, cap: &str) -> Value {
        json!({
            "metadata": { "name": "data", "namespace": "web" },
            "spec": { "resources": { "requests": { "storage": req } }, "volumeName": "pvc-web-data" },
            "status": { "phase": "Bound", "capacity": { "storage": cap } },
        })
    }

    const GI: u64 = 1 << 30;

    #[test]
    fn a_request_past_the_volume_grows_it_rounded_to_a_mib() {
        assert_eq!(decide(&pv("1Gi"), &pvc("3Gi", "1Gi"), GI), Step::Grow { to: 3 * GI });
        assert_eq!(decide(&pv("1Gi"), &pvc("1500000000", "1Gi"), GI), Step::Grow { to: round_mib(1_500_000_000) });
        assert_eq!(round_mib(1_500_000_000) % (1 << 20), 0);
    }

    #[test]
    fn a_request_the_class_already_holds_is_nothing() {
        // 3Gi asked, a 4G class made: growing to 4Gi needs nothing.
        assert_eq!(decide(&pv("4Gi"), &pvc("4Gi", "4Gi"), 4 * GI), Step::Nothing);
        assert_eq!(decide(&pv("4Gi"), &pvc("3Gi", "4Gi"), 4 * GI), Step::Nothing);
    }

    #[test]
    fn an_unbound_or_deleted_claim_is_left_alone() {
        let mut c = pvc("8Gi", "1Gi");
        c["status"]["phase"] = json!("Pending");
        assert_eq!(decide(&pv("1Gi"), &c, GI), Step::Nothing);
        let mut c = pvc("8Gi", "1Gi");
        c["metadata"]["deletionTimestamp"] = json!("2026-11-18T00:00:00Z");
        assert_eq!(decide(&pv("1Gi"), &c, GI), Step::Nothing);
    }

    #[test]
    fn a_refused_grow_is_not_tried_again_until_the_request_changes() {
        let mut c = pvc("8Gi", "1Gi");
        c["status"]["allocatedResources"] = json!({ "storage": "8Gi" });
        c["status"]["allocatedResourceStatuses"] = json!({ "storage": CONTROLLER_INFEASIBLE });
        assert_eq!(decide(&pv("1Gi"), &c, GI), Step::Nothing);
        c["spec"]["resources"]["requests"]["storage"] = json!("16Gi");
        assert_eq!(decide(&pv("1Gi"), &c, GI), Step::Grow { to: 16 * GI });
    }

    #[test]
    fn a_grown_volume_waits_for_the_node_then_expands() {
        // The engine has it, the PV and claim do not yet: record.
        let mut started = pvc("3Gi", "1Gi");
        started["status"]["allocatedResources"] = json!({ "storage": "3Gi" });
        assert_eq!(decide(&pv("1Gi"), &started, 3 * GI), Step::Record { bytes: 3 * GI });
        // Not a grow this started (a claim the binder has not sized): nothing.
        assert_eq!(decide(&pv("1Gi"), &pvc("3Gi", "1Gi"), 3 * GI), Step::Nothing);
        // Recorded: the claim is pending on the node.
        let mut c = pvc("3Gi", "1Gi");
        let s = grown_status(&c, "3Gi");
        c["status"]["conditions"] = s["status"]["conditions"].clone();
        c["status"]["allocatedResourceStatuses"] = s["status"]["allocatedResourceStatuses"].clone();
        assert_eq!(decide(&pv("3Gi"), &c, 3 * GI), Step::Expand { quantity: "3Gi".into(), bytes: 3 * GI });
        // Grown: the claim says 3Gi and nothing is left.
        let done = resized_status(&c, "3Gi");
        c["status"]["capacity"] = done["status"]["capacity"].clone();
        c["status"]["conditions"] = done["status"]["conditions"].clone();
        c["status"]["allocatedResourceStatuses"] = Value::Null;
        assert_eq!(decide(&pv("3Gi"), &c, 3 * GI), Step::Nothing);
    }

    #[test]
    fn statuses_carry_upstreams_conditions() {
        let c = pvc("3Gi", "1Gi");
        let r = resizing_status(&c);
        assert_eq!(r["status"]["conditions"][0]["type"], RESIZING);
        assert_eq!(r["status"]["allocatedResources"]["storage"], "3Gi");
        assert_eq!(r["status"]["allocatedResourceStatuses"]["storage"], CONTROLLER_IN_PROGRESS);
        let g = grown_status(&c, "3Gi");
        let types: Vec<&str> = g["status"]["conditions"].as_array().unwrap().iter().filter_map(|c| c["type"].as_str()).collect();
        assert_eq!(types, vec![FS_PENDING], "Resizing replaced by FileSystemResizePending");
        assert_eq!(g["status"]["allocatedResourceStatuses"]["storage"], NODE_PENDING);
        // A block claim is done when the engine is.
        let mut b = pvc("3Gi", "1Gi");
        b["spec"]["volumeMode"] = json!("Block");
        let g = grown_status(&b, "3Gi");
        assert_eq!(g["status"]["capacity"]["storage"], "3Gi");
        assert!(g["status"]["allocatedResourceStatuses"].is_null());
    }

    #[test]
    fn the_claims_own_mount_of_its_device_is_found() {
        let mi = "\
36 25 259:1 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw
512 36 259:7 / /run/other rw shared:9 - ext4 /dev/ublkb7 rw
513 36 259:7 / /run/stormpump/pvc/ublkb7 rw shared:10 - ext4 /dev/ublkb7 rw
514 36 259:8 / /run/stormpump/pvc/ublkb8 rw shared:11 - ext4 /dev/ublkb8 rw";
        assert_eq!(mount_of(mi, "/dev/ublkb7", None).as_deref(), Some("/run/stormpump/pvc/ublkb7"));
        assert_eq!(mount_of(mi, "/dev/ublkb8", None).as_deref(), Some("/run/stormpump/pvc/ublkb8"));
        assert_eq!(mount_of(mi, "/dev/ublkb9", None), None);
        let other = "600 36 259:9 / /mnt/a\\040b rw - xfs /dev/ublkb9 rw";
        assert_eq!(mount_of(other, "/dev/ublkb9", None).as_deref(), Some("/mnt/a b"));
    }

    #[test]
    fn a_mount_whose_source_reads_otherwise_is_found_by_its_numbers() {
        let mi = "700 36 259:12 / /run/stormpump/pvc/ublkb12 rw - ext4 /proc/self/fd/9 rw";
        assert_eq!(mount_of(mi, "/dev/ublkb12", None), None);
        assert_eq!(mount_of(mi, "/dev/ublkb12", Some((259, 12))).as_deref(), Some("/run/stormpump/pvc/ublkb12"));
        assert_eq!(mount_of(mi, "/dev/ublkb12", Some((259, 13))), None);
    }

    #[test]
    fn the_claim_is_reached_through_pid_1_first() {
        let c = claim_candidates("/run/stormpump/pvc/ublkb7");
        let c: Vec<String> = c.iter().map(|p| p.display().to_string()).collect();
        assert_eq!(c, ["/proc/1/root/run/stormpump/pvc/ublkb7", "/run/stormpump/pvc/ublkb7", "/hostroot/run/stormpump/pvc/ublkb7"]);
    }

    #[test]
    fn a_directory_on_another_device_is_not_the_claim() {
        use std::os::unix::fs::MetadataExt;
        let dir = std::env::temp_dir();
        let d = std::fs::metadata(&dir).unwrap().dev();
        let here = (libc::major(d), libc::minor(d));
        let missing = std::path::PathBuf::from("/nonexistent/claim");
        // The first that is on the device, past one that is not there.
        assert_eq!(on_device(here, &[missing.clone(), dir.clone()]).unwrap(), dir);
        // None on it: an error that names each, never the directory under it.
        let e = on_device((here.0, here.1.wrapping_add(1)), &[missing, dir.clone()]).unwrap_err();
        assert!(e.contains("/nonexistent/claim:"), "{e}");
        assert!(e.contains(&format!("{} is on {}:{}", dir.display(), here.0, here.1)), "{e}");
    }

    #[test]
    fn the_device_is_the_ublk_attachment() {
        let v = json!({ "attachments": [ { "transport": "nvme-tcp" }, { "transport": "ublk", "device": "/dev/ublkb3" } ] });
        assert_eq!(device_of(&v).as_deref(), Some("/dev/ublkb3"));
        assert_eq!(device_of(&json!({})), None);
    }

    /// One fake for the apiserver and the engine: the claim, its PV, the
    /// engine's volume, and what the engine answers a grow.
    type Calls = std::sync::Arc<std::sync::Mutex<Vec<(String, String, Value)>>>;

    async fn world(claim: Value, grow: (u16, Value)) -> (String, Calls) {
        world_with((200, claim), true, grow).await
    }

    /// [`world`], the claim's GET answering `claim` and the engine listing
    /// the volume only when `listed`.
    async fn world_with(claim: (u16, Value), listed: bool, grow: (u16, Value)) -> (String, Calls) {
        let calls: Calls = Calls::default();
        let log = calls.clone();
        let app = axum::Router::new().fallback(move |method: axum::http::Method, uri: axum::http::Uri, body: axum::body::Bytes| {
            let (log, claim, grow) = (log.clone(), claim.clone(), grow.clone());
            async move {
                use axum::http::StatusCode;
                let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let path = uri.path().to_string();
                log.lock().unwrap().push((method.to_string(), path.clone(), body));
                let ok = |v: Value| (StatusCode::OK, axum::Json(v));
                match (method.as_str(), path.as_str()) {
                    ("GET", "/api/v1/persistentvolumes") => ok(json!({ "items": [ {
                        "metadata": { "name": "pvc-web-data", "annotations": { "storm.io/node": "n1" } },
                        "spec": { "csi": { "driver": "stormblock.storm.io", "volumeHandle": "pvc-web-data" },
                                  "capacity": { "storage": "1Gi" },
                                  "claimRef": { "namespace": "web", "name": "data" } },
                    } ] })),
                    ("GET", "/api/v1/namespaces/web/persistentvolumeclaims/data") => {
                        (StatusCode::from_u16(claim.0).unwrap(), axum::Json(claim.1))
                    }
                    ("GET", "/api/v1/volumes") if listed => ok(json!({ "items": [
                        { "id": "u-1", "name": "pvc-web-data", "virtual_size_bytes": GI },
                    ] })),
                    ("GET", "/api/v1/volumes") => ok(json!({ "items": [] })),
                    ("PATCH", "/apis/storage.storm.io/v1/volumes/u-1") => (StatusCode::from_u16(grow.0).unwrap(), axum::Json(grow.1)),
                    ("PATCH", _) | ("POST", _) => ok(json!({})),
                    _ => (StatusCode::NOT_FOUND, axum::Json(json!({}))),
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, calls)
    }

    #[tokio::test]
    async fn a_grow_goes_engine_then_pv_then_claim_with_upstreams_events() {
        let (url, calls) = world(pvc("3Gi", "1Gi"), (200, json!({ "status": { "lastResize": {} } }))).await;
        let engine = crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none());
        let events = crate::events::EventRecorder::new(reqwest::Client::new(), &url, "n1");
        let pending = pass(&reqwest::Client::new(), &url, &engine, "n1", Some(&events)).await;
        assert!(!pending);
        let calls = calls.lock().unwrap().clone();
        let writes: Vec<(String, String)> = calls
            .iter()
            .filter(|(m, p, _)| m == "PATCH" || p.ends_with("/events"))
            .map(|(m, p, _)| (m.clone(), p.clone()))
            .collect();
        let at = |p: &str| writes.iter().position(|w| w.1 == p).unwrap_or_else(|| panic!("{p} not written: {writes:?}"));
        let status = "/api/v1/namespaces/web/persistentvolumeclaims/data/status";
        assert!(at(status) < at("/apis/storage.storm.io/v1/volumes/u-1"), "Resizing said first");
        assert!(at("/apis/storage.storm.io/v1/volumes/u-1") < at("/api/v1/persistentvolumes/pvc-web-data"));
        let grow = calls.iter().find(|c| c.1 == "/apis/storage.storm.io/v1/volumes/u-1").unwrap();
        assert_eq!(grow.2["spec"]["sizeBytes"], 3 * GI);
        let pvw = calls.iter().find(|c| c.1 == "/api/v1/persistentvolumes/pvc-web-data").unwrap();
        assert_eq!(pvw.2["spec"]["capacity"]["storage"], "3Gi");
        let last = calls.iter().rev().find(|c| c.1 == status).unwrap();
        assert_eq!(last.2["status"]["allocatedResourceStatuses"]["storage"], NODE_PENDING);
        let reasons: Vec<String> = calls
            .iter()
            .filter(|c| c.1.ends_with("/events"))
            .filter_map(|c| c.2["reason"].as_str().map(str::to_string))
            .collect();
        assert_eq!(reasons, vec!["Resizing", "FileSystemResizeRequired"]);
    }

    /// #457 (571e7c23b8e4): a claim that could not be read, or whose volume
    /// is missing from the engine's listing while it asks for a grow, is
    /// tried again; one that is gone, or asks nothing, is not.
    #[tokio::test]
    async fn a_claim_not_read_or_a_volume_not_listed_is_tried_again() {
        let engine_of = |url: &str| crate::engine::EngineClient::new(url, crate::engine::TokenSource::none());
        let run = |claim: (u16, Value), listed: bool| async move {
            let (url, calls) = world_with(claim, listed, (200, json!({}))).await;
            let pending = pass(&reqwest::Client::new(), &url, &engine_of(&url), "n1", None).await;
            let wrote = calls.lock().unwrap().iter().any(|c| c.0 == "PATCH");
            (pending, wrote)
        };
        assert_eq!(run((500, json!({"message": "etcd timeout"})), true).await, (true, false), "a 500: tried again");
        assert_eq!(run((404, json!({})), true).await, (false, false), "a claim that is gone: nothing to do");
        assert_eq!(run((200, pvc("3Gi", "1Gi")), false).await, (true, false), "asks for a grow, volume not listed: again");
        assert_eq!(run((200, pvc("1Gi", "1Gi")), false).await, (false, false), "asks nothing: not retried");
    }

    /// #457: a body that is not JSON is a claim not read.
    #[tokio::test]
    async fn a_claim_that_does_not_decode_is_tried_again() {
        let app = axum::Router::new()
            .route("/api/v1/persistentvolumes", axum::routing::get(|| async { axum::Json(json!({ "items": [ {
                "metadata": { "name": "pvc-web-data", "annotations": { "storm.io/node": "n1" } },
                "spec": { "csi": { "driver": "stormblock.storm.io", "volumeHandle": "pvc-web-data" },
                          "capacity": { "storage": "1Gi" }, "claimRef": { "namespace": "web", "name": "data" } },
            } ] })) }))
            .route("/api/v1/volumes", axum::routing::get(|| async { axum::Json(json!({ "items": [] })) }))
            .route("/api/v1/namespaces/web/persistentvolumeclaims/data", axum::routing::get(|| async { "<html>proxy error</html>" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let engine = crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none());
        assert!(pass(&reqwest::Client::new(), &url, &engine, "n1", None).await);
    }

    #[tokio::test]
    async fn a_refused_grow_says_why_and_marks_the_claim_infeasible() {
        let (url, calls) = world(
            pvc("3Gi", "1Gi"),
            (409, json!({ "message": "volume u-1 is being mirrored to a peer; stop the mirror first" })),
        )
        .await;
        let engine = crate::engine::EngineClient::new(&url, crate::engine::TokenSource::none());
        let events = crate::events::EventRecorder::new(reqwest::Client::new(), &url, "n1");
        assert!(!pass(&reqwest::Client::new(), &url, &engine, "n1", Some(&events)).await, "refused for good: not retried");
        let calls = calls.lock().unwrap().clone();
        assert!(!calls.iter().any(|c| c.1 == "/api/v1/persistentvolumes/pvc-web-data" && c.0 == "PATCH"), "the PV keeps its size");
        let failed = calls.iter().find(|c| c.2["reason"] == "VolumeResizeFailed").expect("an event");
        assert!(failed.2["message"].as_str().unwrap().contains("mirrored"));
        let last = calls.iter().rev().find(|c| c.1.ends_with("/data/status")).unwrap();
        assert_eq!(last.2["status"]["allocatedResourceStatuses"]["storage"], CONTROLLER_INFEASIBLE);
    }
}
