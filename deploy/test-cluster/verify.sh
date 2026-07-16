#!/usr/bin/env bash
# Integration test: assert the rustkube-node kubelet joins the rustkube control
# plane and runs a pod. Exits non-zero on failure (usable in CI).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }
API="$APISERVER_URL"

echo "==> apiserver health"
for _ in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/healthz")" = "200" ] && break
  sleep 2
done
[ "$(curl -s -o /dev/null -w '%{http_code}' "$API/healthz")" = "200" ] \
  || fail "apiserver $API not healthy"
echo "    healthz ok — version: $(curl -s "$API/version" | tr -d '\n' | cut -c1-80)"

echo "==> node ${RK_NODE_FQDN} registered + Ready"
ready=""
for _ in $(seq 1 45); do
  ready=$(curl -s "$API/api/v1/nodes/${RK_NODE_FQDN}" | python3 -c '
import json,sys
try: n=json.load(sys.stdin)
except Exception: sys.exit(0)
for c in n.get("status",{}).get("conditions",[]):
    if c["type"]=="Ready": print(c["status"])
' 2>/dev/null)
  [ "$ready" = "True" ] && break
  sleep 2
done
[ "$ready" = "True" ] || fail "node ${RK_NODE_FQDN} not Ready (got: '${ready:-absent}')"
echo "    node Ready=True"

echo "==> schedule smoke pod"
curl -s -X POST "$API/api/v1/namespaces/default/pods" \
  -H 'content-type: application/json' -d '{
    "apiVersion":"v1","kind":"Pod",
    "metadata":{"name":"rk-smoke","namespace":"default"},
    "spec":{"nodeName":"'"${RK_NODE_FQDN}"'","restartPolicy":"Always",
      "containers":[{"name":"pause","image":"registry.k8s.io/pause:3.9"}]}}' \
  -o /dev/null -w '    create: HTTP %{http_code}\n'

echo "==> wait for pod Running + podIP"
phase=""; podip=""
for _ in $(seq 1 60); do
  read -r phase podip < <(curl -s "$API/api/v1/namespaces/default/pods/rk-smoke" | python3 -c '
import json,sys
try: p=json.load(sys.stdin)
except Exception: print("None None"); sys.exit(0)
s=p.get("status",{}); print(s.get("phase","None"), s.get("podIP","None"))
' 2>/dev/null)
  [ "$phase" = "Running" ] && break
  [ "$phase" = "Failed" ] && break
  sleep 3
done
echo "    phase=$phase podIP=$podip"
[ "$phase" = "Running" ] || fail "pod did not reach Running (phase=$phase)"

echo "==> PASS: node Ready and pod Running on rustkube-node kubelet"
