# Unit: rustkube-node test cluster — rkmaster1 + rknode1.
#
# ISOLATION RULE: rustkube's real control plane owns vmid 2000-2002
# (master1/2/3.g8.lo, 192.168.8.51-.53) and may expand upward from 2003.
# rustkube-node tests therefore use the TOP of the automation range:
# vmid 2090-2099, IPs 192.168.8.96+. Never point test kubelets at the real
# masters.
#
# Provisioning is done ENTIRELY in cloud-init user_data (the proven rustkube
# pattern) — no dependency on post-boot SSH before install. rkmaster1 runs a
# single-node fastetcd + rustkube control plane (plaintext); rknode1 runs
# CRI-O + the rustkube-node kubelet/kube-proxy pointed at rkmaster1.
#
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   terragrunt apply
#
# vm_ids/IPs/MACs/hostnames verified free against live qm list + MicroDNS.

include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "git::ssh://git@github.com/glennswest/terraform-modules.git//modules/proxmox-fedora-vm?ref=v0.3.0"
}

locals {
  ssh_key   = trimspace(file(pathexpand("~/.ssh/id_rsa.pub")))
  master_ip = "192.168.8.98"

  # Pinned released artifacts — match what rustkube's masters run.
  fastetcd_rpm_url     = "https://github.com/glennswest/fastetcd/releases/download/v0.8.1/fastetcd-0.8.1-1.x86_64.rpm"
  rustkube_rpm_url     = "https://github.com/glennswest/rustkube/releases/download/v0.7.1/kubernetes-rs-0.7.1-1.x86_64.rpm"
  rustkube_node_rpm_url = "https://github.com/glennswest/rustkube-node/releases/download/v0.1.0/rustkube-node-0.1.0-1.fc43.x86_64.rpm"
}

inputs = {
  dns_zone_id        = "9bed60c8-1664-4183-88f9-a1a21b927edc" # g8.lo
  ci_ssh_public_keys = [local.ssh_key]
  tags               = ["terraform", "fedora", "rustkube-node"]

  vm_datastore      = "test-lvm-thin"
  snippet_datastore = "terraform-snippets"

  vms = {
    # Throwaway control plane for node-level testing.
    rkmaster1 = {
      vm_id  = 2090
      mac    = "BC:24:11:08:00:08"
      ip     = local.master_ip
      cores  = 2
      memory = 4096
      user_data = templatefile("${get_terragrunt_dir()}/templates/master-user-data.yaml.tftpl", {
        hostname         = "rkmaster1"
        fqdn             = "rkmaster1.g8.lo"
        ci_user          = "fedora"
        ssh_keys         = [local.ssh_key]
        node_ip          = local.master_ip
        fastetcd_rpm_url = local.fastetcd_rpm_url
        rustkube_rpm_url = local.rustkube_rpm_url
      })
    }
    # The node under test (kubelet + CRI-O from the rustkube-node packages).
    rknode1 = {
      vm_id  = 2091
      mac    = "BC:24:11:08:00:09"
      ip     = "192.168.8.99"
      cores  = 2
      memory = 4096
      user_data = templatefile("${get_terragrunt_dir()}/templates/node-user-data.yaml.tftpl", {
        hostname             = "rknode1"
        fqdn                 = "rknode1.g8.lo"
        ci_user              = "fedora"
        ssh_keys             = [local.ssh_key]
        apiserver_url        = "http://${local.master_ip}:6443"
        rustkube_node_rpm_url = local.rustkube_node_rpm_url
      })
    }
  }
}
