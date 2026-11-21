# Admin credentials go to files outside Git, owner-only.
resource "local_sensitive_file" "kubeconfig" {
  filename             = "${local.config_dir}/kubeconfig-admin"
  content              = talos_cluster_kubeconfig.this.kubeconfig_raw
  file_permission      = "0600"
  directory_permission = "0700"
}

resource "local_sensitive_file" "talosconfig" {
  filename             = "${local.config_dir}/talosconfig"
  content              = data.talos_client_configuration.this.talos_config
  file_permission      = "0600"
  directory_permission = "0700"
}
