variable "openbao_addr" {
  description = "OpenBao address, for example http://127.0.0.1:8200 through kubectl port-forward"
  type        = string
}

variable "kubernetes_host" {
  description = "Kubernetes API address for the kubernetes auth mount"
  type        = string
  default     = "https://kubernetes.default.svc"
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
  description = "Passwords of the vault-admin role per cluster, from TF_VAR_vault_admin_passwords; rotated right after apply"
  type        = map(string)
  sensitive   = true
}
