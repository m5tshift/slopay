# The provider token is read from the environment; it is never stored in Terraform files.
provider "vault" {
  address          = var.openbao_addr
  skip_child_token = true
}
