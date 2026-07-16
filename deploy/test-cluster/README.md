# rustkube-node integration test fixture

A throwaway two-VM cluster that stands up a real **rustkube** control plane and
a **rustkube-node** worker, then asserts the node goes `Ready` and runs a pod.
This is the repeatable end-to-end test for the node components.

## Isolation

Completely separate from rustkube's real control plane. rustkube owns
vmid **2000–2002** / `master1-3.g8.lo` / `192.168.8.51–.53`; this fixture uses
the top of the automation range so the two can never collide:

| VM          | vmid | IP            | Role                                        |
|-------------|------|---------------|---------------------------------------------|
| `rkmaster1` | 2090 | 192.168.8.96  | fastetcd (single node) + rustkube CP        |
| `rknode1`   | 2091 | 192.168.8.97  | CRI-O + rustkube-node kubelet/kube-proxy    |

Never point a test kubelet at the real masters.

## Pinned artifacts (reproducible)

- rustkube control plane: **v0.6.0** (`kubernetes-rs` RPM)
- fastetcd: **v0.8.1**
- rustkube-node: **v0.1.0**

All are released RPMs installed on the VMs — nothing is built on the nodes.
Bump the versions in `config.sh`.

## Phase / mode

Plaintext HTTP control plane (no TLS/RBAC), because the kubelet is HTTP-only
today. rustkube v0.6.0 ships plaintext defaults in its RPM config, so the
fixture only has to configure single-node fastetcd. TLS is a later phase
(tracked with the kubelet client-auth work).

## Usage

```bash
export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'

deploy/test-cluster/up.sh       # create VMs + install + start everything
deploy/test-cluster/verify.sh   # assert node Ready + pod Running (CI gate)
deploy/test-cluster/down.sh     # destroy both VMs
```

## Known issues exercised here

- rustkube-node#4 — kubelet node name under systemd (worked around by setting
  `NODE_NAME` in `up.sh`).
- rustkube-node#5 — CRI RPC timeouts.
- rustkube#9 — apiserver hardening against client-triggered failure (this
  fixture is how we reproduce/regress it).
