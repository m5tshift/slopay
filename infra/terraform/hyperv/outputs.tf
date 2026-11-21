output "vm_names" {
  description = "VM names."
  value       = sort(keys(hyperv_machine_instance.vm))
}

output "vm_macs" {
  description = "Static MAC by VM name, for the address reservation in the router."
  value       = { for k, vm in hyperv_machine_instance.vm : k => vm.network_adaptors[0].static_mac_address }
}

output "iso_files" {
  description = "ISO path on the stand PC by VM name."
  value       = { for k, u in var.iso_urls : k => local.iso_file[u] }
}
