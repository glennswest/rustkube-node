# Kubelet metrics

The kubelet serves two Prometheus endpoints on `:10250`, both behind its
bearer-token auth, under the names upstream Kubernetes uses. A dashboard or
alert written for an upstream kubelet works here unchanged. The control-plane
half is rustkube's [`docs/metrics.md`](https://github.com/glennswest/rustkube/blob/main/docs/metrics.md).

Code: `pkg/kubelet/src/metrics.rs`, and the handlers in `server.rs`.

## `/metrics`: the kubelet's own

| Metric | Type | What it is |
|---|---|---|
| `kubelet_running_pods` | gauge | Pods with a sandbox |
| `kubelet_running_containers{container_state}` | gauge | Containers the runtime lists, by state: `created`, `running`, `exited`, `unknown`. Every state is always present, 0 included |
| `kubelet_pod_start_duration_seconds` | histogram | From the kubelet first seeing a pod to the pod started. Upstream's buckets, 0.5 s to 1 h |
| `kubelet_pleg_relist_duration_seconds` | histogram | One sync pass over the pods the kubelet already runs, re-reading their state from the runtime. This kubelet has no separate PLEG, so the pass is its relist, and it includes probes. Prometheus's default buckets |
| `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_virtual_memory_bytes`, `process_start_time_seconds`, `process_open_fds`, `process_max_fds` | gauge | Read from `/proc/self` on each scrape, by rustkube's `apimachinery::metrics` collector |
| `kubernetes_build_info{gitVersion,component="kubelet"}` | gauge | The kubelet's version |

## `/metrics/cadvisor`: what the workloads consume

Rendered from the runtime at the moment of the scrape, so a container that has
gone is gone from the next scrape.

| Metric | Type | Labels | Source |
|---|---|---|---|
| `container_cpu_usage_seconds_total` | counter | `container`, `id`, `namespace`, `pod` | CRI `ContainerStats.cpu` |
| `container_memory_working_set_bytes` | gauge | same | CRI `ContainerStats.memory` |
| `container_fs_usage_bytes` | gauge | same | CRI `ContainerStats.writable_layer` |
| `container_network_receive_bytes_total` | counter | `container=""`, `id` (the sandbox), `interface`, `namespace`, `pod` | the pod's network namespace |
| `container_network_transmit_bytes_total` | counter | same | same |

`id` is the runtime's container id. cAdvisor puts the cgroup path there, which
no runtime here reports. Network is per pod, as in cAdvisor, and loopback is
left out.

**A number the runtime did not report is no series, not a 0.** Each family is
still declared (HELP and TYPE), so a scraper can see the name exists.

### By runtime

| Runtime | CPU, memory | Filesystem | Network |
|---|---|---|---|
| CRI (`--runtime=cri`) | yes | yes (writable layer) | not yet |
| stormpump (`--runtime=stormpump`) | yes, from the engine's `QUERY` stats block (#57). Memory is `memory.current`, which includes page cache that upstream's working set leaves out | no: an image's clone is shared by its containers and there is no per-container writable layer yet | yes, from `/proc/<sandbox holder>/net/dev` |

## Not here, on purpose

**Container restart counts.** Upstream has no kubelet metric for them.
`kube_pod_container_status_restarts_total` belongs to kube-state-metrics and
is derived from `status.containerStatuses[].restartCount`. A second count kept
here would drift from the object, and the object is the one that is right.
