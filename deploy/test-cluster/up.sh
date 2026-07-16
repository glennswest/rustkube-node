#!/usr/bin/env bash
# Bring up the rustkube-node integration test cluster:
#   rkmaster1  — fastetcd (single node) + rustkube control plane (plaintext)
#   rknode1    — CRI-O + rustkube-node kubelet/kube-proxy, pointed at rkmaster1
#
# Idempotent-ish: safe to re-run; terragrunt reconciles, installs are -y.
# Requires: PROXMOX_API_TOKEN in env, terragrunt, ssh access to the VMs.
#
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   deploy/test-cluster/up.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"

echo "==> [1/5] terragrunt apply (rkmaster1 + rknode1)"
( cd "$TG_DIR" && terragrunt apply -input=false -auto-approve >/dev/null )

echo "==> [2/5] waiting for SSH + cloud-init on both VMs"
for host in "$RK_MASTER_FQDN" "$RK_NODE_FQDN"; do
  for _ in $(seq 1 40); do
    rk_ssh "$host" true 2>/dev/null && break
    sleep 8
  done
  rk_ssh "$host" 'cloud-init status --wait >/dev/null 2>&1 || true'
  echo "    $host up"
done

echo "==> [3/5] rkmaster1: fastetcd (single-node, plaintext) + control plane"
rk_ssh "$RK_MASTER_FQDN" "sudo bash -euo pipefail -s" <<EOF
# --- fastetcd single-node member ---
sudo dnf install -y "$FASTETCD_RPM" >/dev/null
sudo mkdir -p /etc/fastetcd /var/lib/fastetcd/data
sudo tee /etc/fastetcd/fastetcd.conf >/dev/null <<CONF
ETCD_NAME=rkmaster1
ETCD_DATA_DIR=/var/lib/fastetcd/data
ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
ETCD_INITIAL_ADVERTISE_PEER_URLS=http://${RK_MASTER_IP}:2380
ETCD_ADVERTISE_CLIENT_URLS=http://${RK_MASTER_IP}:2379
ETCD_INITIAL_CLUSTER=rkmaster1=http://${RK_MASTER_IP}:2380
ETCD_INITIAL_CLUSTER_TOKEN=rknode-test
ETCD_INITIAL_CLUSTER_STATE=new
CONF
sudo systemctl enable --now fastetcd

# --- rustkube control plane (RPM ships plaintext defaults:
#     apiserver ETCD_SERVERS=http://127.0.0.1:2379, controller/scheduler
#     APISERVER_URL=http://127.0.0.1:6443) ---
sudo dnf install -y "$RUSTKUBE_RPM" >/dev/null
sudo systemctl enable --now kube-apiserver kube-controller-manager kube-scheduler
EOF

echo "==> [4/5] rknode1: CRI-O + CNI plugins + rustkube-node kubelet/kube-proxy"
rk_ssh "$RK_NODE_FQDN" "sudo bash -euo pipefail -s" <<EOF
sudo dnf install -y cri-o containernetworking-plugins >/dev/null
sudo systemctl enable --now crio

sudo dnf install -y "$RUSTKUBE_NODE_RPM" >/dev/null
# Point the kubelet at rkmaster1 (plaintext) and give it a real node name
# (workaround for rustkube-node#4: systemd doesn't export HOSTNAME).
sudo tee /etc/kubernetes/kubelet >/dev/null <<CONF
APISERVER_URL=${APISERVER_URL}
NODE_NAME=${RK_NODE_FQDN}
KUBELET_ARGS=--runtime=cri
CONF
sudo tee /etc/kubernetes/kube-proxy >/dev/null <<CONF
APISERVER_URL=${APISERVER_URL}
NODE_NAME=${RK_NODE_FQDN}
KUBE_PROXY_ARGS=
CONF
sudo systemctl enable --now kubelet kube-proxy
EOF

echo "==> [5/5] up. Run deploy/test-cluster/verify.sh to check node Ready + pod Running."
