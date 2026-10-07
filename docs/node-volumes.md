# The node's own volumes as PV + PVC sets

Every service on a stormcos node keeps its state in stormblock volumes, cloned
from goldens and mounted at boot: `<component>-data`, `<component>-state` and
`<component>-logs`. The kubelet (`pkg/kubelet/src/system_claims.rs`) shows each
one as a Kubernetes claim: **a PV and its bound PVC** (#49, #59), on every node.

| Object | Name | Notes |
|---|---|---|
| PVC | `kube-system/<volume>-<node>` | bound (`volumeName`), `dataSourceRef` = the golden, annotation `storm.io/volume: <volume>` |
| PV | `storm-<volume>-<node>` | class `stormblock`, reclaim **Retain**, pinned to the node |

Names carry the node (owner, #107): every node has a `fastetcd-data`, and a PV
or claim name exists once per cluster. A service volume is one node's (system
services have no cross-node RAID), so `fastetcd-data-<node>` is its whole
identity. To clone one into a pod's claim, name it in `dataSourceRef`
(`namespace: kube-system`, `name: fastetcd-data-<node>`).

Both carry the labels `storm.io/system-volume=true`, `storm.io/volume-kind`
(`data`, `state` or `logs`) and `storm.io/component`:

    kubectl get pvc -n kube-system -l storm.io/volume-kind=logs
    kubectl get pv -l storm.io/component=fastetcd

The kind comes from the name's suffix. The engine's `role` is only the slab
half (`system` or `data`) and is recorded as the `storm.io/role` annotation.

## What the objects say

- PV: `claimRef` with the claim's uid, `csi.driver` `stormblock.storm.io`,
  `csi.volumeHandle` = the volume, `csi.fsType` from the volume's `fs.kind`,
  `csi.volumeAttributes` `storm.io/golden` and `storm.io/fs-uuid`, annotations
  `pv.kubernetes.io/provisioned-by`, `storm.io/node`, `storm.io/health`,
  `storm.io/access`, `storm.io/role`.
- PVC: `pv.kubernetes.io/bind-completed`, `pv.kubernetes.io/bound-by-controller`,
  `volume.kubernetes.io/storage-provisioner` (and the beta key),
  `volume.kubernetes.io/selected-node`.

Claims made through the built-in driver (`pvc-<ns>-<claim>`, reclaim Delete)
are written by the same builder, without the system labels.

## Kept current

stormblock is the source of truth. Whenever its volumes change (its
`/apis/storage.storm.io/v1/volumes?watch=1` stream; polled every 30 s on an
engine without it), and whenever one of these PVs or PVCs changes, the kubelet:

- creates what is missing (the PVC first, then the PV with its uid), so a claim
  deleted by hand, a wiped etcd or a deleted namespace comes back;
- updates what differs: capacity grows with the volume (a request never
  shrinks), labels and annotations are merged (other tools' are kept), and the
  PV's `claimRef` follows a claim that was made again;
- deletes the claim of a volume that went away, so the binder marks the PV
  `Released`. The PV is never deleted. A listing with none of the node's volumes
  in it lets go of nothing. "Went away" is decided by the volume a claim names
  (`storm.io/volume`), not by the claim's name.

Phases and protection finalizers are the binder's (rustkube). The kubelet only
writes a new claim's first status, so it does not read as Unknown until then.

Only objects annotated `storm.io/node: <this node>` are written. An object of
the same name that is not this node's is left alone.

**No migration** (owner, #107). A cluster that ran an earlier kubelet still
has the first node's pair under the old unqualified names
(`kube-system/fastetcd-data`, `storm-fastetcd-data`). Those objects are left as
they are, and their volume still exists, so the mirror does not delete their
claim; the new `-<node>` pair is written beside them. Deleting the old pair by
hand is safe: the PV is Retain, so the volume is untouched.

## Placement: drives, shelf/bay, RAID partners (#60)

Every PV of a stormblock volume this node holds (these node volumes and the built-in driver's claims alike) carries
where the volume physically is, refreshed on engine volume changes and every minute (`pv_placement.rs`). Two sources
are joined on the drive's WWN, else its serial: stormblock's `GET /api/v1/volumes?placement=true` (drives, legs,
rebuild, drive-level RAID arrays and their members) and stormdrive's `GET https://<node>:9092/api/v1/placement`
(shelf, bay, drive health), read with the kubelet's node-CA client certificate (`STORMDRIVE_URL` overrides the
address; unreachable, the PV carries stormblock's half and the kubelet says so once).

| | |
|---|---|
| `storm.io/volume-id`, `storm.io/golden` | the engine's id; what it was cloned from |
| `storm.io/redundancy`, `storm.io/health`, `storm.io/rebuild` | the policy (`mirror:2@shelf`), `healthy`/`degraded`/`failed`, `none`/`needed`/the rebuild's state |
| `storm.io/drives` | JSON: per drive `wwn`, `serial`, `model`, `node`, `shelf`, `bay`, `health` |
| `storm.io/raid-partners` | JSON: per array member `array`, `level`, `index`, `state`, `wwn`, `serial`, `node`, `shelf`, `bay` |
| labels `storm.io/shelf` (when every drive is in one shelf), `storm.io/redundancy`, `storm.io/health` | label-safe (`mirror-2-shelf`) |

A change is an Event on the PV: `VolumeDegraded`/`VolumeFailed`/`VolumeHealthy`, `RebuildStarted`/`RebuildFinished`,
`VolumeMoved` (its drives changed), `RaidPartnerChanged` (a member's state; a failed one is a Warning). Only what changed
is written (a merge patch); the first sight of a PV writes and says nothing.

## Remaining limitations

Room for claims on the data slabs is #62 (README, "Room on the data slabs").
