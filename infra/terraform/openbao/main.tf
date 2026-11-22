locals {
  eso_namespace  = "secrets"
  apps_namespace = "apps"

  static_roles = merge([
    for cluster, c in var.postgres_clusters : {
      for role, user in c.static_roles : role => { cluster = cluster, user = user }
    }
  ]...)
}

resource "vault_mount" "kv" {
  path    = "secret"
  type    = "kv"
  options = { version = "2" }
}

resource "vault_mount" "transit" {
  path = "transit"
  type = "transit"
}

# OpenBao generates the key; the private part never leaves it.
resource "vault_transit_secret_backend_key" "merchant_jwt" {
  backend          = vault_mount.transit.path
  name             = "merchant-jwt"
  type             = "ecdsa-p256"
  exportable       = false
  deletion_allowed = false
}

resource "vault_mount" "database" {
  path = "database"
  type = "database"
}

resource "vault_database_secret_backend_connection" "pg" {
  for_each = var.postgres_clusters

  backend       = vault_mount.database.path
  name          = each.key
  plugin_name   = "postgresql-database-plugin"
  allowed_roles = keys(each.value.static_roles)

  postgresql {
    connection_url = "postgresql://{{username}}:{{password}}@${each.value.host}:5432/${each.value.database}"
    username       = "vault-admin"
    password       = var.vault_admin_passwords[each.key]
  }
}

resource "vault_database_secret_backend_static_role" "pg" {
  for_each = local.static_roles

  backend         = vault_mount.database.path
  name            = each.key
  db_name         = vault_database_secret_backend_connection.pg[each.value.cluster].name
  username        = each.value.user
  rotation_period = 86400
}

resource "vault_auth_backend" "kubernetes" {
  type = "kubernetes"
  path = "kubernetes"
}

resource "vault_kubernetes_auth_backend_config" "this" {
  backend                = vault_auth_backend.kubernetes.path
  kubernetes_host        = var.kubernetes_host
  disable_iss_validation = true
}

resource "vault_policy" "eso_read" {
  name   = "eso-read"
  policy = <<-EOT
    path "secret/data/garage/*"       { capabilities = ["read"] }
    path "secret/data/grafana/*"      { capabilities = ["read"] }
    path "secret/data/alertmanager/*" { capabilities = ["read"] }
    path "database/static-creds/*"    { capabilities = ["read"] }
  EOT
}

resource "vault_policy" "merchant_service" {
  name   = "merchant-service"
  policy = <<-EOT
    path "secret/data/webhook/*"       { capabilities = ["create", "update"] }
    path "transit/sign/merchant-jwt"   { capabilities = ["update"] }
    path "transit/sign/merchant-jwt/*" { capabilities = ["update"] }
    path "transit/keys/merchant-jwt"   { capabilities = ["read"] }
  EOT
}

resource "vault_policy" "webhook_service" {
  name   = "webhook-service"
  policy = <<-EOT
    path "secret/data/webhook/*" { capabilities = ["read"] }
  EOT
}

resource "vault_kubernetes_auth_backend_role" "external_secrets" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "external-secrets"
  bound_service_account_names      = ["external-secrets"]
  bound_service_account_namespaces = [local.eso_namespace]
  token_policies                   = [vault_policy.eso_read.name]
  token_ttl                        = 3600
}

resource "vault_kubernetes_auth_backend_role" "merchant_service" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "merchant-service"
  bound_service_account_names      = ["merchant-service"]
  bound_service_account_namespaces = [local.apps_namespace]
  token_policies                   = [vault_policy.merchant_service.name]
  token_ttl                        = 3600
}

resource "vault_kubernetes_auth_backend_role" "webhook_service" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "webhook-service"
  bound_service_account_names      = ["webhook-service"]
  bound_service_account_namespaces = [local.apps_namespace]
  token_policies                   = [vault_policy.webhook_service.name]
  token_ttl                        = 3600
}
