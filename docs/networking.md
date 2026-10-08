# Node networking: who owns what

What the node (its boot and the kubelet) does for networking, what the CNI
(Cilium, installed by network-operator; flowsdn in the flowsdn edition) does,
and what a node looks like before the CNI is up (#32). Checked against
stormcos main (`deploy/image.toml`, `deploy/manifests/50-cilium-config.yaml`,
`docs/CLUSTER.md`), network-operator 0.3.0 and this repository.

## The boundary

| What | Owner | How |
|---|---|---|
| The node's own interface: address, netmask, **default route (gateway)**, DNS servers | the node's boot (the kernel) | `ip=dhcp` on the kernel command line: the kernel's DHCP client takes the lease, the router option and the DNS servers from the network's DHCP server (microdns). Nothing after `switch_root` configures an interface, and no CNI touches it |
| Reaching the apiserver, registering the Node, the Lease | kubelet | over the node's own interface (`--apiserver https://${NODE_IP}:6443` on stormcos), never through a Service: it must work before any CNI |
| `Node.status.addresses` | kubelet | the node's own addresses, every heartbeat |
| A Pod's network namespace (its sandbox) | kubelet + runtime | stormpump holds the netns; host-network Pods get none and skip CNI |
| CNI ADD / DEL for a Pod | kubelet | the conflist in `--cni-conf-dir` (`/etc/cni/net.d`) and the plugins in `--cni-bin-dir` (`/opt/cni/bin`), both the host's (stormcos mounts them at those paths); ADD with the sandbox's netns and `K8S_POD_*` args, the address into `status.podIP`; DEL when the Pod finishes (#137) or is removed |
| Installing and reconciling the CNI | network-operator | from the cluster's `Network` CR (`network.storm.io/v1`): Cilium's DaemonSet, operator, RBAC and config; `Available` / `Progressing` / `Degraded` |
| Routing mode, overlay, MTU, IPAM, masquerade | the `Network` CR, applied by Cilium | stormcos's default config: `routing-mode: tunnel` (VXLAN), `enable-ipv4-masquerade: true`. Chosen once per cluster (`stormcos init --network …`, docs/CLUSTER.md in stormcos) |
| Writing the conflist, serving CNI ADD | the Cilium agent | a host-network DaemonSet Pod, so it starts before any Pod network exists |
| Pod-to-Pod routes, Pod egress (masquerade to the node's address) | Cilium | the datapath; Pod egress then leaves by the **node's** default route |
| Services (ClusterIP, NodePort) | Cilium (`kube-proxy-replacement: true`) | the packaged kube-proxy is not run in any stormcos edition (#145, #155) |
| `NetworkUnavailable` condition | the CNI | cilium-operator sets it False (reason `CiliumIsUp`) once the node's Cilium agent Pod is up (`set-cilium-is-up-condition: 'true'` in stormcos's config; cilium `operator/watchers/node_taint.go`). The kubelet never writes it and keeps it across heartbeats, as it keeps every condition it does not own |

In the flowsdn edition flowsdn takes Cilium's rows.

## Before the CNI is up

The owner's decision on #3 (2026-10-07): **no NotReady gating on the CNI**
("we want to get working early"). So:

1. The kubelet starts, registers the Node and reports **Ready** at once.
   rustkube's scheduler places Pods only on Ready nodes, and the CNI's own
   agent is a Pod, so gating Ready on the CNI would keep the CNI off the node.
2. **Host-network Pods start at once**: the Cilium agent, its operator,
   control-plane and node services. They need nothing from the CNI.
3. **A Pod on the Pod network waits** while there is no conflist: Pending, its
   containers `ContainerCreating`, a `NetworkNotReady` Warning Event ("network
   is not ready: …"). It is not polled: the kubelet watches `--cni-conf-dir`
   (inotify) and the conflist appearing wakes exactly the Pods waiting on it
   (#148).
4. Once a conflist exists, an ADD that fails (the agent not serving yet) is a
   `FailedCreatePodSandBox` Event with the plugin's own words, retried on a
   backoff from its first failure. Never `Failed`: the Pod starts when the
   agent answers.

The node does **not** call network-operator or Cilium, and does not need to:
the conflist on disk is the signal that the CNI is up on this node, and the
CNI ADD is the proof that it serves. network-operator's own status
(`kubectl get network -o yaml`) says whether the CNI is being installed.

An **unjoined** stormcos node has no cluster and so no CNI at all (stormcos
docs/CLUSTER.md, "Choosing the network"): its workloads are on the host's
network, and a Pod-network Pod there waits as in step 3 until the node joins a
cluster whose network-operator brings the CNI up.

## Pre-CNI or broken?

| Seen | Means |
|---|---|
| Pod-network Pods Pending with `NetworkNotReady`, no conflist in `/etc/cni/net.d` | the CNI is not installed on this node yet: normal before network-operator has reconciled it, or on an unjoined node |
| `FailedCreatePodSandBox` naming the Cilium agent ("unable to connect to Cilium agent …") | the conflist is there and the agent is not serving: starting, or down (look at the agent Pod) |
| Host-network Pods not starting | not a CNI matter: the runtime, an image or a volume (their Events say which) |
| **The node itself has no default route, or cannot reach its gateway** | **never "the CNI is not up yet"**. The node's gateway is the kernel's DHCP lease (router option) on its own interface. Look at the DHCP server's pool or reservation (`gateway`), the lease the node took, and the router. The CNI adds Pod routes and masquerades Pod egress *through* the node's route; it does not create it |
| Pods have addresses and reach each other but nothing outside | Pod egress masquerades through the node's default route: check the node's own outbound first, then the `Network` CR's masquerade setting |
| `NetworkUnavailable=True` on the Node | something other than the kubelet said the network is not ready here (a cloud provider sets it on some platforms; cilium-operator clears it once its agent is up). No `NetworkUnavailable` at all before the CNI is normal |

What #32 saw on scmaster1–3 (local LAN and DNS fine, gateway 192.168.8.1
unreachable, no cluster) is the gateway row: a lab gateway
question, independent of the CNI.
