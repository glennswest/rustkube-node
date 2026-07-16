#!/usr/bin/env bash
# Tear down the rustkube-node test cluster (destroys both VMs + DNS/DHCP).
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   deploy/test-cluster/down.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"

echo "==> terragrunt destroy (rkmaster1 + rknode1)"
( cd "$TG_DIR" && terragrunt destroy -input=false -auto-approve )
echo "==> down."
