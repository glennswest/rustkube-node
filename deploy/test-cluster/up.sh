#!/usr/bin/env bash
# Bring up the rustkube-node integration test cluster. All install/config is
# baked into cloud-init user_data (see deploy/terragrunt/rknode/templates/),
# so this just applies terraform and waits for the control plane to answer.
#
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   deploy/test-cluster/up.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"

echo "==> terragrunt apply (rkmaster1 + rknode1)"
( cd "$TG_DIR" && terragrunt apply -input=false -auto-approve >/dev/null )

echo "==> waiting for rkmaster1 apiserver at $APISERVER_URL (cloud-init installs fastetcd + control plane)"
code=000
for _ in $(seq 1 90); do
  # curl exits non-zero until the endpoint is up; keep set -e happy with || true.
  code="$(curl -s -o /dev/null -m3 -w '%{http_code}' "$APISERVER_URL/healthz" || true)"
  [ "$code" = "200" ] && break
  sleep 10
done
[ "$code" = "200" ] || { echo "apiserver not healthy (HTTP $code) — check cloud-init on rkmaster1"; exit 1; }
echo "    apiserver healthy"

echo "==> up. Run deploy/test-cluster/verify.sh to check node Ready + pod Running."
