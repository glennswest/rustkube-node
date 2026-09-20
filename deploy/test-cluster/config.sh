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

# --- pinned released artifacts ---
#
# **They are in `deploy/terragrunt/rknode/terragrunt.hcl`, not here.** That is
# the file cloud-init templates from, so it is the only one that decides what
# a VM installs.
#
# This file used to carry a second copy "for reference", and the copy drifted:
# it named rustkube-node v0.1.0 and rustkube v0.7.1 long after the rig had
# moved to v0.2.3 and v0.7.33, and nothing read it, so nothing caught it. A
# pin that is documentation of another pin is a pin that will be wrong —
# anyone reading here to find out what the fixture runs was told the wrong
# answer. Read `terragrunt.hcl`:
#
#   grep _rpm_url deploy/terragrunt/rknode/terragrunt.hcl

# Plaintext HTTP control plane for this phase (the kubelet is HTTP-only today).
export APISERVER_URL="http://${RK_MASTER_IP}:6443"

# SSH: test VMs are recreated often, so tolerate changed host keys.
export SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=8 -o BatchMode=yes"

rk_ssh() { local host="$1"; shift; ssh $SSH_OPTS "${CI_USER}@${host}" "$@"; }

# Terragrunt unit that defines the two VMs.
export TG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../terragrunt/rknode" && pwd)"
