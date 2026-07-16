#!/usr/bin/env bash
# Shared config for the rustkube-node integration test fixture.
#
# ISOLATION: this throwaway cluster is fully separate from rustkube's real
# control plane. rustkube owns vmid 2000-2002 / master1-3.g8.lo / .51-.53 —
# this fixture must never touch or depend on those. We live at the top of the
# automation range: vmid 2090-2091, IPs .98-.99.
set -euo pipefail

# --- test cluster hosts (see deploy/terragrunt/rknode/terragrunt.hcl) ---
export RK_MASTER_IP="192.168.8.98"
export RK_MASTER_FQDN="rkmaster1.g8.lo"
export RK_NODE_IP="192.168.8.99"
export RK_NODE_FQDN="rknode1.g8.lo"
export CI_USER="fedora"

# --- pinned released artifacts (reproducible; no build on the VMs) ---
# rustkube control plane (kube-apiserver/controller/scheduler) — matches the
# version rustkube's own masters run. Install happens in cloud-init; this is
# kept for reference/verify scripts.
export RUSTKUBE_RPM="https://github.com/glennswest/rustkube/releases/download/v0.7.1/kubernetes-rs-0.7.1-1.x86_64.rpm"
# fastetcd datastore (etcd v3 wire protocol).
export FASTETCD_RPM="https://github.com/glennswest/fastetcd/releases/download/v0.8.1/fastetcd-0.8.1-1.x86_64.rpm"
# rustkube-node (kubelet/kube-proxy) — the thing under test.
export RUSTKUBE_NODE_RPM="https://github.com/glennswest/rustkube-node/releases/download/v0.1.0/rustkube-node-0.1.0-1.fc43.x86_64.rpm"

# Plaintext HTTP control plane for this phase (the kubelet is HTTP-only today).
export APISERVER_URL="http://${RK_MASTER_IP}:6443"

# SSH: test VMs are recreated often, so tolerate changed host keys.
export SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=8 -o BatchMode=yes"

rk_ssh() { local host="$1"; shift; ssh $SSH_OPTS "${CI_USER}@${host}" "$@"; }

# Terragrunt unit that defines the two VMs.
export TG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../terragrunt/rknode" && pwd)"
