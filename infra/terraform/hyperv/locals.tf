locals {
  gib = 1073741824

  # One file per unique URL: lb-1 and lb-2 share the Debian ISO.
  iso_urls_unique = toset(values(var.iso_urls))
  iso_file        = { for u in local.iso_urls_unique : u => "${var.iso_dir}\\${substr(sha1(u), 0, 12)}.iso" }

  disks = merge([
    for name, vm in var.vms : {
      for i, size in vm.disks : "${name}-${i}" => {
        vm   = name
        path = "${var.vhd_path}\\${name}-${i}.vhdx"
        size = size * local.gib
      }
    }
  ]...)
}
