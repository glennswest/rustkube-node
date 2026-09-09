# Changelog

## [Unreleased]

<!-- New unreleased changes go here -->

## [v0.4.0] — 2026-09-09

### Added
- **feat(kubelet):** a VM the kubelet starts is registered for stormvm's
  console doors — `vm.json` is written into the run directory beside the
  sockets the hypervisor binds, and removed on stop and on every failure path
  after the write (#38). Without it `/api/v1/vms` was empty and every attach
  404'd for machines the kubelet started, though `stormvm start` registered
  its own.

### Fixed
- **fix(kubelet):** NIC naming and the ioctls that make a tap are
  `stormvm-net`'s now, replacing the local `vm_net` module (#39). Two bugs go
  with it: `ifr_ifindex` was read through the `flags` arm of `ifreq` — an
  `int` read as a `short`, correct below 32767 and truncating above it, so a
  node that has churned enough pods enslaves the wrong interface or none, with
  the tap present and no frame reaching the bridge; and the derived tap name
  and **MAC** ignored the namespace, so `default/web-1` and `staging/web-1`
  collided — two guests with one address on a shared segment.

### Breaking
- **BREAKING:** derived MACs change, because the namespace now enters the
  hash. A guest keyed on its old address (a DHCP reservation, an ARP entry)
  sees a new one once.

## [v0.3.0] — 2026-09-09

### Added
- **feat:** kubelet `/containerLogs` honors `follow` — the log so far is sent,
  then whatever is appended to it, until the container leaves this node or the
  client hangs up. `follow` was accepted and ignored, so `kubectl logs -f`
  printed once and stopped (rustkube-node#34).
- **feat:** kubelet `/containerLogs` honors `limitBytes` — a budget spent across
  the initial read and every followed chunk, cut on a character boundary.

### Changed
- **perf:** a followed log streams through a bounded channel instead of being
  read whole into a `String`, so a large or open-ended log does not sit on the
  kubelet's heap.
