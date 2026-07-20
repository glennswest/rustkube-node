#!/usr/bin/env bash
#
# Generate the rustkube-node test-rig control-plane PKI (kubeadm/OpenShift
# style) into a dir that terragrunt injects via cloud-init write_files.
# Idempotent: existing files are kept, so re-running an apply reuses the CA.
#
#   ./gen-pki.sh [OUTDIR]   (default: deploy/terragrunt/rknode/pki)
#
# The rig is single-master (rkmaster1, .98) + one worker (rknode1, .99). Unlike
# rustkube's masters (SA-signed bearer tokens), the node authenticates with a
# client cert kubeconfig (O=system:nodes, CN=system:node:<fqdn>) — the flow
# rustkube-node#19 implements and the live rig runs.
#
# Produces (in OUTDIR):
#   ca.crt/ca.key                 cluster CA (CN=rustkube-ca)
#   sa.key/sa.pub                 service-account token signing keypair
#   apiserver.crt/.key            serving cert (SANs for rkmaster1/.98/svc IP)
#   admin.crt/.key                CN=admin, O=system:masters   (kubectl)
#   controller-manager.crt/.key   CN=system:kube-controller-manager
#   scheduler.crt/.key            CN=system:kube-scheduler
#   rknode1.crt/.key              CN=system:node:rknode1.g8.lo, O=system:nodes
#   kubelet.kubeconfig            node kubeconfig (embeds CA + rknode1 client)
#   admin.kubeconfig              admin kubeconfig (embeds CA + admin client)
#   kubelet-server-token          opaque token for the kubelet :10250 server
set -euo pipefail

SCRIPTDIR="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-$SCRIPTDIR/terragrunt/rknode/pki}"
mkdir -p "$OUT"; cd "$OUT"

MASTER_IP=192.168.8.98
MASTER_FQDN=rkmaster1.g8.lo
NODE_FQDN=rknode1.g8.lo
KUBE_SVC_IP=10.96.0.1     # apiserver ClusterIP (first IP of the service CIDR)
APISERVER=https://$MASTER_IP:6443
DAYS=3650

have() { [ -s "$1" ]; }
b64() { openssl base64 -A -in "$1"; }

# --- cluster CA ---
if ! have ca.crt; then
  openssl genrsa -out ca.key 2048
  openssl req -x509 -new -nodes -key ca.key -subj "/CN=rustkube-ca" -days "$DAYS" -out ca.crt
  echo "generated CA"
fi

# --- service-account signing keypair ---
if ! have sa.key; then
  openssl genrsa -out sa.key 2048
  openssl rsa -in sa.key -pubout -out sa.pub
  echo "generated SA signing keypair"
fi

# Sign a client cert: $1=basename $2=CN $3=O(optional)
gen_client() {
  local base="$1" cn="$2" org="${3:-}"
  have "$base.crt" && return 0
  local subj="/CN=$cn"; [ -n "$org" ] && subj="/CN=$cn/O=$org"
  openssl genrsa -out "$base.key" 2048
  openssl req -new -key "$base.key" -subj "$subj" -out "$base.csr"
  openssl x509 -req -in "$base.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days "$DAYS" \
    -extfile <(printf "extendedKeyUsage=clientAuth") -out "$base.crt"
  rm -f "$base.csr"
  echo "generated client cert $base ($subj)"
}

gen_client admin              admin                            system:masters
gen_client controller-manager system:kube-controller-manager
gen_client scheduler          system:kube-scheduler
gen_client rknode1            "system:node:$NODE_FQDN"         system:nodes

# --- apiserver serving cert (SANs match the live rig) ---
if ! have apiserver.crt; then
  openssl genrsa -out apiserver.key 2048
  openssl req -new -key apiserver.key -subj "/CN=kube-apiserver" -out apiserver.csr
  openssl x509 -req -in apiserver.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days "$DAYS" \
    -extfile <(cat <<EOF
subjectAltName=DNS:kubernetes,DNS:kubernetes.default,DNS:kubernetes.default.svc,DNS:kubernetes.default.svc.cluster.local,DNS:$MASTER_FQDN,DNS:localhost,IP:127.0.0.1,IP:$MASTER_IP,IP:$KUBE_SVC_IP
extendedKeyUsage=serverAuth
EOF
) -out apiserver.crt
  rm -f apiserver.csr
  echo "generated apiserver serving cert"
fi

# --- kubeconfigs (embed CA + client cert/key) ---
write_kubeconfig() {
  local out="$1" user="$2" crt="$3" key="$4"
  cat > "$out" <<EOF
apiVersion: v1
kind: Config
current-context: $user
clusters:
- name: rknode-test
  cluster:
    server: $APISERVER
    certificate-authority-data: $(b64 ca.crt)
users:
- name: $user
  user:
    client-certificate-data: $(b64 "$crt")
    client-key-data: $(b64 "$key")
contexts:
- name: $user
  context: {cluster: rknode-test, user: $user}
EOF
  echo "wrote $out"
}
write_kubeconfig kubelet.kubeconfig node  rknode1.crt rknode1.key
write_kubeconfig admin.kubeconfig   admin admin.crt   admin.key

# --- opaque bearer token for the kubelet :10250 server (rustkube-node#9) ---
have kubelet-server-token || { openssl rand -hex 16 > kubelet-server-token; echo "generated kubelet-server-token"; }

echo "PKI ready in $OUT"
