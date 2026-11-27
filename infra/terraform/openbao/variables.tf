variable "openbao_addr" {
  description = "OpenBao address, for example http://127.0.0.1:8200 through kubectl port-forward"
  type        = string
}

variable "kubernetes_host" {
  description = "Kubernetes API address for the kubernetes auth mount"
  type        = string
  default     = "https://kubernetes.default.svc"
}

variable "kubeconfig_path" {
  description = "Path to admin kubeconfig when Terraform manages <cluster>-vault-admin Secrets in namespace data"
  type        = string
  default     = ""
}

variable "manage_vault_admin_secrets" {
  description = "Generate and create <cluster>-vault-admin Kubernetes Secrets in namespace data via Terraform"
  type        = bool
  default     = false
}

variable "postgres_clusters" {
  description = "CloudNativePG clusters for the database secrets engine: connection and static roles"
  type = map(object({
    host         = string
    database     = string
    static_roles = map(string) # OpenBao role name => PostgreSQL user
  }))
  default = {
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
  }
}

variable "vault_admin_passwords" {
  description = "Passwords of the vault-admin role per cluster (used when manage_vault_admin_secrets = false); rotated right after apply"
  type        = map(string)
  sensitive   = true
  default     = {}
}

variable "seed_kv_secrets" {
  description = "Seed initial OpenBao KV v2 secrets (garage, grafana, mongodb, valkey, clickhouse, alertmanager, ci, anvil)"
  type        = bool
  default     = true
}

variable "telegram_bot_token" {
  description = "Telegram bot token for Alertmanager (stored in secret/alertmanager/telegram)"
  type        = string
  sensitive   = true
  default     = "000000:stand-placeholder"
}

variable "telegram_chat_id" {
  description = "Telegram chat ID for Alertmanager (stored in secret/alertmanager/telegram)"
  type        = string
  sensitive   = true
  default     = "0"
}

variable "github_token" {
  description = "GitHub PAT with Administration:write (or classic repo scope) for ARC self-hosted runners (secret/ci/github)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "anvil_mnemonic" {
  description = "12-word BIP-39 mnemonic for Anvil without trailing newline (stored in secret/anvil/mnemonic)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "payer_keys" {
  description = "Newline-separated private keys derived from anvil_mnemonic for payer-sim (stored in secret/payer-keys/anvil)"
  type        = string
  sensitive   = true
  default     = ""
}
