# External storage drivers (CSI) on a node

A claim of the built-in class, `stormblock`, never goes through CSI. The node
clones, attaches and mounts it itself (`pkg/kubelet/src/storage.rs`), and its
PV records `stormblock.storm.io` as the built-in driver identity. This document
covers **every other StorageClass**: a claim whose PV
was made by another driver's external provisioner, an inline `csi:` volume,
and a generic `ephemeral:` volume whose claim is of such a class (#52).

## Who does what

| Step | Done by |
|---|---|
| CreateVolume, and the PV | the driver's `external-provisioner` sidecar |
| `VolumeAttachment` (when the CSIDriver has `attachRequired: true`) | rustkube's attach/detach controller (`attachdetach.rs`) |
| ControllerPublishVolume, and `status.attached` | the driver's `external-attacher` sidecar |
| Registration, `CSINode`, topology labels | the kubelet (`csi_plugins.rs`) |
| NodeStage, NodePublish, NodeUnpublish, NodeUnstage | the kubelet calling the driver's node plugin (`csi.rs`, `pod_manager/csi_volumes.rs`) |
| The bind into the application container | stormpump |

## Registration

The driver's node plugin runs as a DaemonSet with `node-driver-registrar`,
which puts a socket in `/var/lib/kubelet/plugins_registry`. The kubelet
watches that directory (filesystem notifications, `fs_watch.rs`); a change
queues a scan, and an idle registry with every driver registered causes no
scans at all (#101). For each new socket it:

1. calls `GetInfo`, and accepts `type: CSIPlugin` with a 1.x version;
2. calls the driver's `NodeGetInfo` and `NodeGetCapabilities` at the endpoint
   the registrar gave (normally `/var/lib/kubelet/plugins/<driver>/csi.sock`);
3. writes the node's `CSINode` so that it lists **exactly** the drivers
   registered now (name, the driver's node ID, topology keys, and the
   volume limit when there is one), owned by the Node. It also labels the
   node with the driver's topology segments;
4. calls `NotifyRegistrationStatus` either way, so the registrar's own
   health check reports the outcome.

A socket that goes away deregisters its driver. Each registration (and each
`CSINode` write) is bounded at 10 s. One that fails is retried after 1 s, or
at once if its socket is recreated; a failed `CSINode` write is retried on
the same 1 s deadline. The usual cause is a registrar that is up before its
driver. A successful registration wakes the Pods waiting on that driver.

## Mounting a volume

At pod start, for a volume whose claim is bound to a PV with `csi.driver`
other than `stormblock.storm.io`:

1. The driver must be registered here. If it is not, the pod waits
   (`VolumeNotReady`) and says which driver it is waiting for.
2. When the CSIDriver says `attachRequired` (the default, including when
   there is no CSIDriver object), the kubelet GETs the VolumeAttachment
   named `csi-<sha256(handle+driver+node)>` and waits until it is
   `attached`. Its `attachmentMetadata` becomes the publish context.
3. The record goes on disk first, as
   `/var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~csi/<vol>/vol_data.json`.
4. **NodeStageVolume** (when the driver stages) to
   `/var/lib/kubelet/plugins/kubernetes.io/csi/<driver>/<sha256(handle)>/globalmount`,
   then **NodePublishVolume** to `.../kubernetes.io~csi/<vol>/mount`. The call
   carries the PV's `fsType`, `mountOptions`, `volumeAttributes`, its node
   stage and publish secrets, and the access mode mapped from the PV's first
   mode. When the CSIDriver sets `podInfoOnMount`, it also carries the pod's
   identity.
5. **The kubelet checks that the published directory is a mount point in PID
   1's `/proc/1/mountinfo`**, which is where stormpump resolves the bind. If
   it is not, the pod waits with a message saying so. Without this check the
   pod would get the empty directory the driver created, and its data would go
   to the node's root and be lost with the pod.
6. The published directory goes to stormpump as an ordinary bind.

Inline `csi:` volumes are published without attach or stage, with the
handle `csi-<sha256(pod uid + volume name)>`. The CSIDriver must list
`Ephemeral` in `volumeLifecycleModes`. Raw block PVs (`volumeMode: Block`)
are refused for a pod with a message (a pod gets Filesystem), and published
as a raw block only for a VM's disk (#157: `VolumeCapability.block`, the target
a file the driver puts the device at, recorded under `vm-<vmi uid>`; README,
"A VM disk on another driver's claim"). Raw block
claims of the built-in `stormblock` class are served by the kubelet itself
(README, "Raw block claims", #67).

## Unmounting

When a pod is stopped, after its containers are gone, the kubelet reads the
pod's `vol_data.json` records. For each one it calls **NodeUnpublishVolume**
and removes the directory, but only if it is empty, so a volume that is somehow
still mounted is never deleted through its mount point. When no other pod on
the node has a record for the same volume, it calls **NodeUnstageVolume**.

A sweep repeats this, on this node's Pod events and every 10 s while a
teardown is still pending, for any pod that has records and that the
node is no longer running. That covers a pod deleted while the kubelet was
down, a start that failed after publishing, and an unpublish the driver
refused. A pod the apiserver still shows as bound here and unfinished is
never swept, which keeps the sweep away from a pod that is still starting. If
the apiserver cannot be asked, nothing is swept.

## Growing a volume (#42)

The controller half (rustkube#63, the external resizer) grows the volume and leaves the claim with
`status.allocatedResourceStatuses.storage: NodeResizePending` (or a `FileSystemResizePending` condition) and the PV's
`spec.capacity` past the claim's `status.capacity`. On a claim event (and the sweep's retry), the kubelet finds each
such claim among the volumes published on this node and, when the driver has `EXPAND_VOLUME`, calls
`NodeExpandVolume` on the published path (with the staging path and the new size). Then it writes the claim's status
as upstream's kubelet does: `capacity` = the new size, `Resizing` and `FileSystemResizePending` removed,
`allocatedResourceStatuses` dropped, with a `FileSystemResizeSuccessful` Event on the claim. A driver without the
capability grows it all from the controller, and the status is finished the same way. A refusal is a
`FileSystemResizeFailed` Warning, tried again.

## Mount propagation: the decision

The driver mounts **in its own container's mount namespace**. For the pod to
see the volume, that mount has to appear in PID 1's namespace, where stormpump
makes the bind. So:

- **The driver's node plugin pod** mounts `/var/lib/kubelet` (or both
  `/var/lib/kubelet/pods` and `/var/lib/kubelet/plugins`) as a `hostPath` with
  `mountPropagation: Bidirectional`, and runs `privileged: true`. This is what
  every upstream CSI driver's DaemonSet already does, so no driver manifest
  needs changing.
- **The kubelet** passes Bidirectional through only for a privileged
  container, as upstream does. For any other container it is Private, with
  a warning.
- **stormpump** applies it per mount (stormpump#35): the kubelet copies each
  `cri::Mount.propagation` into `spec::Mount.propagation` (#81), Private as
  `rprivate`, HostToContainer as `rslave`, Bidirectional as `rshared`. The
  engine refuses a propagating mount on anything but a bind in a container,
  and every pod mount is a bind. A spec with a propagating mount is payload
  v6, which an engine older than stormpump#35 refuses rather than running
  private. Not yet seen end to end with a real driver (#52); the mountinfo
  check still keeps a consumer waiting when a mount has not reached the node.
- **The kubelet's own view** of `/var/lib/kubelet` (stormcos: `mount
  kubeletdir /var/lib/kubelet`) does not need to see the mounts. The kubelet
  never reads a published volume, it only creates directories and hands paths
  to the engine. It shares the host PID namespace, which is how it reads
  `/proc/1/mountinfo`.

Mounting in PID 1 instead, the way stormblock claims avoid propagation
entirely, is not an option for a third-party driver. The driver's own code
does the mount, in whatever namespace it runs in.

## Not yet

- End-to-end verification with a real driver (csi-driver-host-path): #81
  is done; the driver's images cannot yet be made goldens on a node
  (stormblock-registry#99), tracked in #52.
- Generic ephemeral volumes: the kubelet resolves them to the claim
  `<pod>-<volume>` and waits for it. rustkube has no controller that creates
  that claim (rustkube#94).
- Raw block volumes, NodeGetVolumeStats (#223).
