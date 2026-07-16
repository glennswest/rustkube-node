# Unit: rustkube-node test cluster — rkmaster1 + rknode1.
#
# ISOLATION RULE: rustkube's real control plane owns vmid 2000-2002
# (master1/2/3.g8.lo, 192.168.8.51-.53) and may expand upward from 2003.
# rustkube-node tests therefore use the TOP of the automation range:
# vmid 2090-2099, IPs 192.168.8.96+. Never point test kubelets at the
# real masters.
#
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   terragrunt apply
#
# vm_ids/IPs/MACs/hostnames verified free against live qm list + MicroDNS
# before allocation — never pick by pattern.

include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "git::ssh://git@github.com/glennswest/terraform-modules.git//modules/proxmox-fedora-vm?ref=v0.3.0"
}

inputs = {
  dns_zone_id        = "9bed60c8-1664-4183-88f9-a1a21b927edc" # g8.lo
  ci_ssh_public_keys = [file(pathexpand("~/.ssh/id_rsa.pub"))]
  tags               = ["terraform", "fedora", "rustkube-node"]

  vm_datastore      = "test-lvm-thin"
  snippet_datastore = "terraform-snippets"

  vms = {
    # Throwaway control plane for node-level testing (rustkube apiserver etc).
    rkmaster1 = {
      vm_id  = 2090
      mac    = "BC:24:11:08:00:06"
      ip     = "192.168.8.96"
      cores  = 2
      memory = 4096
    }
    # The node under test (kubelet + CRI-O from the rustkube-node packages).
    rknode1 = {
      vm_id  = 2091
      mac    = "BC:24:11:08:00:07"
      ip     = "192.168.8.97"
      cores  = 2
      memory = 4096
    }
  }
}
