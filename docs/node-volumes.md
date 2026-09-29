# The node's own volumes as PV + PVC sets

Every service on a stormcos node keeps its state in stormblock volumes, cloned
from goldens and mounted at boot: `<component>-data`, `<component>-state` and
`<component>-logs`. The kubelet (`pkg/kubelet/src/system_claims.rs`) shows each
one as a Kubernetes claim: **a PV and its bound PVC** (#49, #59), subject to
the cross-node naming limitation below.

| Object | Name | Notes |
|---|---|---|
| PVC | `kube-system/<volume>` | bound (`volumeName`), `dataSourceRef` = the golden |
| PV | `storm-<volume>` | class `stormblock`, reclaim **Retain**, pinned to the node |

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
  in it lets go of nothing.

Phases and protection finalizers are the binder's (rustkube). The kubelet only
writes a new claim's first status, so it does not read as Unknown until then.

Only objects annotated `storm.io/node: <this node>` are written. An object of
the same name from another node is left alone.

## Remaining limitations

Names are not node-qualified. When two nodes both have `fastetcd-data`, the
cluster-scoped PV and kube-system PVC can represent only the first node;
other nodes warn and leave the objects alone (#59). Naming/migration requires
the owner's decision (#107). Therefore this is not yet an inventory of every
node volume in a multi-node cluster.

Drive/shelf/bay/RAID placement joins are not published here (#60). The engine
and stormdrive placement APIs are prerequisites, not proof that this mirror
has consumed them. Capacity reservation/overcommit protection remains #62.
