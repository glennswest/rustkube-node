# Unit: rknode1.g8.lo — throwaway test VM for rustkube-node packages.
#
#   export PROXMOX_API_TOKEN='terraform-svc@pve!rustkube-node=<secret>'
#   terragrunt apply
#
# vm_id was verified free via terraform-modules/examples/terragrunt/
# get-free-vmid.sh (live query) — never pick one by pattern. IP/MAC/hostname
# were verified free against MicroDNS DHCP reservations.

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
    rknode1 = {
      vm_id  = 2003
      mac    = "BC:24:11:08:00:05"
      ip     = "192.168.8.95"
      cores  = 2
      memory = 4096
    }
  }
}
