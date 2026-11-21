resource "hyperv_network_switch" "external" {
  name                = var.switch_name
  switch_type         = "External"
  net_adapter_names   = [var.net_adapter_name]
  allow_management_os = true
}
