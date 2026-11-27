# Initial KV v2 secrets in OpenBao (SRS INF-2.4).
# Each vault_kv_secret_v2 ignores later out-of-band rotations via lifecycle.ignore_changes.
locals {
  garage_buckets   = toset(["pg-backups", "mongo-backups", "tempo-traces"])
  mongodb_users    = toset(["chain-watcher", "webhook-service", "reconciliation-service"])
  valkey_users     = toset(["chain-watcher", "invoice-service", "rate-service", "webhook-service", "envoy-rls"])
  clickhouse_users = toset(["ddl", "reporting"])
}

resource "random_id" "garage_rpc_secret" {
  count       = var.seed_kv_secrets ? 1 : 0
  byte_length = 32
}

resource "random_password" "garage_admin_token" {
  count   = var.seed_kv_secrets ? 1 : 0
  length  = 32
  special = false
}

resource "random_id" "garage_access_key" {
  for_each    = var.seed_kv_secrets ? local.garage_buckets : toset([])
  byte_length = 12
}

resource "random_id" "garage_secret_key" {
  for_each    = var.seed_kv_secrets ? local.garage_buckets : toset([])
  byte_length = 32
}

resource "random_password" "grafana_admin" {
  count   = var.seed_kv_secrets ? 1 : 0
  length  = 18
  special = false
}

resource "random_password" "mongodb_user" {
  for_each = var.seed_kv_secrets ? local.mongodb_users : toset([])
  length   = 24
  special  = false
}

resource "random_password" "valkey_user" {
  for_each = var.seed_kv_secrets ? local.valkey_users : toset([])
  length   = 24
  special  = false
}

resource "random_password" "clickhouse_user" {
  for_each = var.seed_kv_secrets ? local.clickhouse_users : toset([])
  length   = 24
  special  = false
}

resource "vault_kv_secret_v2" "garage_core" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "garage/core"

  data_json = jsonencode({
    "rpc-secret"  = random_id.garage_rpc_secret[0].hex
    "admin-token" = random_password.garage_admin_token[0].result
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "garage_keys" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "garage/keys"

  data_json = jsonencode(merge([
    for b in local.garage_buckets : {
      "${b}-access-key" = "GK${random_id.garage_access_key[b].hex}"
      "${b}-secret-key" = random_id.garage_secret_key[b].hex
    }
  ]...))

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "grafana_admin" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "grafana/admin"

  data_json = jsonencode({
    "admin-user"     = "admin"
    "admin-password" = random_password.grafana_admin[0].result
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "alertmanager_telegram" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "alertmanager/telegram"

  data_json = jsonencode({
    "telegram-bot-token" = var.telegram_bot_token
    "telegram-chat-id"   = var.telegram_chat_id
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "mongodb_users" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "mongodb/users"

  data_json = jsonencode({
    for u in local.mongodb_users : u => random_password.mongodb_user[u].result
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "valkey_users" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "valkey/users"

  data_json = jsonencode({
    for u in local.valkey_users : u => random_password.valkey_user[u].result
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "clickhouse_users" {
  count = var.seed_kv_secrets ? 1 : 0
  mount = vault_mount.kv.path
  name  = "clickhouse/users"

  data_json = jsonencode({
    for u in local.clickhouse_users : u => random_password.clickhouse_user[u].result
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "ci_github" {
  count = var.seed_kv_secrets && var.github_token != "" ? 1 : 0
  mount = vault_mount.kv.path
  name  = "ci/github"

  data_json = jsonencode({
    "github_token" = var.github_token
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "anvil_mnemonic" {
  count = var.seed_kv_secrets && var.anvil_mnemonic != "" ? 1 : 0
  mount = vault_mount.kv.path
  name  = "anvil/mnemonic"

  data_json = jsonencode({
    "mnemonic" = var.anvil_mnemonic
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}

resource "vault_kv_secret_v2" "payer_keys" {
  count = var.seed_kv_secrets && var.payer_keys != "" ? 1 : 0
  mount = vault_mount.kv.path
  name  = "payer-keys/anvil"

  data_json = jsonencode({
    "keys" = var.payer_keys
  })

  lifecycle {
    ignore_changes = [data_json]
  }
}
