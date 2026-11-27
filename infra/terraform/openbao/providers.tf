# The OpenBao token is read from VAULT_TOKEN in the environment; it is never stored in Terraform files.
provider "vault" {
  address          = var.openbao_addr
  skip_child_token = true
}

provider "kubernetes" {
  config_path = var.kubeconfig_path != "" ? var.kubeconfig_path : null
}
