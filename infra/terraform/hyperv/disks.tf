resource "hyperv_vhd" "disk" {
  for_each = local.disks

  path     = each.value.path
  size     = each.value.size
  vhd_type = "Dynamic"

  depends_on = [terraform_data.host_dirs]
}
