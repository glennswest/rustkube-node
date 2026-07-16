# Root Terragrunt config for rustkube-node test infrastructure.
# Adapted from terraform-modules/examples/terragrunt/root.hcl.
#
# Requires:  export PROXMOX_API_TOKEN='terraform-svc@pve!<name>=<secret>'
# (pool-scoped service-account token — never root@pam).

locals {
  proxmox_endpoint = "https://pve.g8.lo:8006/"
  ssh_private_key  = pathexpand("~/.ssh/id_rsa")
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    provider "proxmox" {
      endpoint  = "${local.proxmox_endpoint}"
      api_token = var.proxmox_api_token
      insecure  = true
      ssh {
        agent       = false
        username    = "root"
        private_key = file("${local.ssh_private_key}")
      }
    }

    variable "proxmox_api_token" {
      type      = string
      sensitive = true
    }
  EOF
}

inputs = {
  proxmox_api_token = get_env("PROXMOX_API_TOKEN")
}
