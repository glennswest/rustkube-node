# Build, test and ship

Audited against main fecb331 on 2026-10-02. Workspace version: 0.13.0;
subsequent changes are unreleased, not proof of an installed node version.

## Build from a pushed commit

Run from this component checkout, as the session user:

```bash
git push
sc-build 'cargo build --locked && cargo test --locked'
```

`sc-build` fetches the pushed commit onto dev.g8.lo as the unprivileged build
user, builds in an isolated volume and deletes it on success or failure.
There is no persistent checkout on dev to use. Never build on the session VM
or use root to work around a build-host problem. Builds queue for a slot;
allow the existing build to finish instead of starting duplicates.

For release compilation or the separate test crate, pass the command to the
same service:

```bash
sc-build 'cargo build --release --locked --target x86_64-unknown-linux-musl'
sc-build 'cd test && cargo test --locked && cargo build --release --locked'
```

The build environment needs Rust, the selected target/linker and `protoc`
(CRI v1, CSI 1.9 and plugin-registration protobufs). `apimachinery` comes from
a git revision in Cargo.toml and Cargo.lock, not a sibling checkout. Dependency
updates must update the lock and be committed/pushed before validation.
The workspace release profile uses opt-level 3, thin LTO, one codegen unit,
and strips debug information while retaining symbols. No version bump is
needed for documentation-only changes.

## The release artifact is a stage golden

After all implementation commits are pushed and sc-build passes, request once:

```bash
stormcentral component stage rustkube-node --url http://stormcentral.g8.lo
```

stormcentral invokes the authoritative stormcos stage recipe, records an
immutable golden and files its release request. A release composes approved
goldens; a successful component build alone is not a deployment or live-node
acceptance result. Follow the release request, and use `stormcentral shipped`
for completed work whose remaining step is inclusion in a release.

The source recipe is stormcos `deploy/build-goldens.sh`, node-agent section.
It builds x86_64 musl kubelet/kube-proxy binaries onto a stormd base, writes
`/etc/stormd/config.toml`, and supplies the mount targets the service needs.
The generated config runs kubelet with `--runtime stormpump`, ring socket
`/hostrun/stormpump.sock`, the node's HTTPS apiserver, node certificate paths,
and local registry. stormd's management API binds `0.0.0.0:9085`.
The recipe copies kube-proxy but does not start it: Cilium owns Services.
Host mounts and certificates are platform responsibilities; see
[configuration](configuration.md) for the executable defaults and overrides.

Do not use `stormcentral component build rustkube-node` or the legacy
`scripts/build-golden.sh` for releases. Their bin-only output omits the service
base/config. The legacy script remains in the repository pending
[#51](https://github.com/glennswest/rustkube-node/issues/51); it is not a second
supported builder. `packaging/build-packages.sh` is also legacy and is not the
stormcos shipping path. Its packages do not establish a tested deployment.

## Runtime acceptance

`test/` is a separate workspace and test-container build. Its medium suite
exercises the built-in PVC size ladder; short/long remain skip-only (#61),
overcommit is checked against the node's published capacity (#62), and runner
image injection remains #97.
Run Jobs through stormcentral on capability-matched test machines; never infer
live behavior from unit tests or hardcode a machine. See README and
[status](status.md) for outstanding acceptance.

The `turbomode` UID-worker work was merged into main under #114 (600b58a);
main's stage goldens include it. Its live validation runs on C2NR0Q2 first
(owner's decision on #110) and is tracked in #102.
