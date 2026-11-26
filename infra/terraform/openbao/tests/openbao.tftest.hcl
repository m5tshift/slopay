mock_provider "vault" {}

variables {
  openbao_addr          = "http://127.0.0.1:8200"
  vault_admin_passwords = { merchant-pg = "x", invoice-pg = "y" }
}

run "mounts_and_transit" {
  command = plan

  assert {
    condition     = vault_mount.kv.path == "secret" && vault_mount.kv.options.version == "2"
    error_message = "KV v2 mount secret expected"
  }
  assert {
    condition     = vault_transit_secret_backend_key.merchant_jwt.type == "ecdsa-p256" && !vault_transit_secret_backend_key.merchant_jwt.exportable
    error_message = "merchant-jwt must be a non-exportable ECDSA P-256 key"
  }
}

run "kubernetes_auth_and_policies" {
  command = plan

  assert {
    condition     = vault_auth_backend.kubernetes.path == "kubernetes" && vault_kubernetes_auth_backend_role.external_secrets.role_name == "external-secrets" && contains(vault_kubernetes_auth_backend_role.external_secrets.bound_service_account_namespaces, "secrets")
    error_message = "kubernetes auth role external-secrets in namespace secrets expected"
  }
  assert {
    condition     = strcontains(vault_policy.eso_read.policy, "secret/data/garage/*") && strcontains(vault_policy.eso_read.policy, "secret/data/clickhouse/*") && strcontains(vault_policy.eso_read.policy, "secret/data/mongodb/*") && strcontains(vault_policy.eso_read.policy, "secret/data/valkey/*") && strcontains(vault_policy.eso_read.policy, "secret/data/payer-keys/*") && strcontains(vault_policy.eso_read.policy, "secret/data/ci/*")
    error_message = "eso-read must read garage/*, clickhouse/*, mongodb/*, valkey/*, payer-keys/* and ci/*"
  }
  assert {
    condition     = !strcontains(vault_policy.webhook_service.policy, "create") && strcontains(vault_policy.merchant_service.policy, "secret/data/webhook/*")
    error_message = "merchant-service writes webhook/*, webhook-service only reads"
  }
}

run "static_roles" {
  command = plan

  assert {
    condition     = length(vault_database_secret_backend_static_role.pg) == 2 && alltrue([for r in vault_database_secret_backend_static_role.pg : r.rotation_period == 86400])
    error_message = "Static roles rotate every 24 h"
  }
}

run "static_roles_full" {
  command = plan

  variables {
    postgres_clusters = {
      merchant-pg = {
        host         = "merchant-pg-rw.data.svc"
        database     = "merchant"
        static_roles = { merchant-service = "merchant-service" }
      }
      invoice-pg = {
        host         = "invoice-pg-rw.data.svc"
        database     = "invoice"
        static_roles = { invoice-service = "invoice-service" }
      }
      ledger-pg = {
        host         = "ledger-pg-rw.data.svc"
        database     = "ledger"
        static_roles = { ledger-service = "ledger-service" }
      }
    }
    vault_admin_passwords = { merchant-pg = "x", invoice-pg = "y", ledger-pg = "z" }
  }

  assert {
    condition     = length(vault_database_secret_backend_static_role.pg) == 3 && contains(keys(vault_database_secret_backend_static_role.pg), "ledger-service") && alltrue([for r in vault_database_secret_backend_static_role.pg : r.rotation_period == 86400])
    error_message = "Full stand adds ledger-pg static role rotating every 24 h"
  }
}
