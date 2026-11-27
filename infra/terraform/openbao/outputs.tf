output "grafana_admin_password" {
  description = "Initial Grafana admin password seeded into secret/grafana/admin"
  value       = var.seed_kv_secrets ? random_password.grafana_admin[0].result : null
  sensitive   = true
}
