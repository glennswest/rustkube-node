# CLAUDE.md — rustkube-node

The node half of rustkube: the kubelet (`pkg/kubelet`, `cmd/kubelet`), kube-proxy
(`pkg/proxy`, `cmd/kube-proxy`) and the CNI helpers (`pkg/cni`). It ships as a
golden (`scripts/build-golden.sh`), not a package. The cross-project rules are in
`../CLAUDE.md`; this file is the project's own context and work plan.

## Version

`0.9.0`. There is one version location: `[workspace.package] version` in
`Cargo.toml` (every crate uses `version.workspace = true`).

## Build and test

`sc-build` from this checkout, after `git push` (see `../CLAUDE.md`). Needs the
`rustkube` repository as a sibling (the `apimachinery` path dependency) and
`protoc` on the build box, for the vendored protos in `pkg/kubelet/proto`:

- `api.proto`: CRI v1 (kubernetes/cri-api, release-1.32)
- `csi/csi.proto`: CSI spec v1.9.0
- `pluginregistration/api.proto`: the kubelet plugin-registration API (k8s.io/kubelet v0.32.0)

## Work plan

### In progress: #52, external StorageClasses (the CSI node side)

Findings, 2026-09-24:
- `csi.rs` was a stub. It logged calls and created directories, and it never
  spoke gRPC. The issue called it complete, and it was not.
- **Blocked outside this repo:** stormpump makes every container's mount namespace
  `MS_PRIVATE`, and its spec has no propagation field. So a CSI driver's
  NodePublish mount cannot reach PID 1's namespace, and the application
  container's bind would find an empty directory. It needs a stormpump issue
  for per-mount propagation, and possibly stormcos for `/var/lib/kubelet` as a
  shared mount on the host.

Steps:
1. [x] Vendor `csi.proto` and the plugin-registration proto; build.rs generates clients (and servers, for tests).
2. [x] `csi.rs`: a real gRPC client over the driver's Unix socket (Identity + Node).
3. [x] `csi_plugins.rs`: watch `/var/lib/kubelet/plugins_registry`, GetInfo, NodeGetInfo,
       write `CSINode` and the topology labels, NotifyRegistrationStatus, and deregister when the socket goes.
4. [x] Mount: a claim bound to a PV of another driver waits for its VolumeAttachment
       (when the CSIDriver has `attachRequired`), then NodeStage and NodePublish, and the published directory is bound in.
       Write `vol_data.json` beside the mount so teardown survives a kubelet restart.
5. [x] Unmount: NodeUnpublish when the pod goes, NodeUnstage when it is the last pod on the node.
6. [ ] Pass `mountPropagation` through to stormpump once it has the field. Filed as stormpump#35. Until then the mountinfo check keeps pods waiting instead of giving them an empty directory.
7. [x] Tests: a mock driver and registrar on a real Unix socket, the full round trip.
8. [x] Docs (`docs/csi.md`), CHANGELOG.
9. [ ] End to end with csi-driver-host-path: blocked on step 6 (stormpump#35).
10. [ ] Build verified with `sc-build scripts/sc-build.sh` (in progress).

Related, filed elsewhere: rustkube#94 (no ephemeral-volume controller).
