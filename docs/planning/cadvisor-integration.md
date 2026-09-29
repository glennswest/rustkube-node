# Planned cadvisor integration

Current source audit: main 5bb1a38, 2026-09-29. Implementation remains
[#21](https://github.com/glennswest/rustkube-node/issues/21).
The intended provider is [glennswest/cadvisor](https://github.com/glennswest/cadvisor);
there is no cadvisor dependency in this checkout yet. This plan does not claim
upstream version/conformance verification or a completed integration.

## What the kubelet already does

`node_status.rs` reads procfs/statfs for memory, filesystem and PID pressure
and ephemeral-storage capacity. Those values are no longer hardcoded healthy
or fabricated 50Gi/45Gi capacities. Thresholds remain fixed; nodefs and imagefs
are not separately modeled for eviction.

`metrics.rs` renders container CPU/memory/filesystem and Pod network families,
according to what each runtime supplies. stormpump QUERY supplies CPU and
memory.current; that memory includes page cache. The CRI backend has no Pod
network collector, and stormpump lacks a per-container writable-layer usage
source. See [the metric reference](../metrics.md).

`server.rs::stats_summary` sums container CPU/memory for its node fields and
reads actual node filesystem usage. It is not a complete host CPU/memory
summary. Machine metrics, PSI and imagefs-aware eviction remain absent.

## Intended integration

Keep container stats with their runtime; consume node/filesystem/machine data
from the cadvisor library instead of maintaining duplicate host collectors.
Preserve optional collection presence, distinguish nodefs/imagefs, and connect
configured eviction signals to node pressure and eviction behavior. Add
contract tests and real-node acceptance before claiming dashboard or Summary
API compatibility. The full upstream metric surface is not promised today.

Before implementation, #21 retains the design questions about the dependency
boundary and packaging: whether to retain the standalone platform daemon as
well, how to consume the library, and the scope of conformance work. The
historical August upstream survey is available in git history; it is not the
current implementation reference or an instruction to upgrade dependencies.
