output "iso_urls" {
  description = "Per-node Image Factory ISO URL by VM name: input iso_urls of infra/terraform/hyperv."
  value       = { for name, u in data.talos_image_factory_urls.node : name => u.urls.iso }
}

output "installer_images" {
  description = "Installer image by VM name (machine.install.image)."
  value       = { for name, u in data.talos_image_factory_urls.node : name => u.urls.installer }
}

output "cluster_endpoint" {
  value = "https://${local.cp_ip}:6443"
}

output "kubeconfig_path" {
  value = local_sensitive_file.kubeconfig.filename
}

output "talosconfig_path" {
  value = local_sensitive_file.talosconfig.filename
}
