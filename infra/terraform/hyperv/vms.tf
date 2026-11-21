resource "hyperv_machine_instance" "vm" {
  for_each = var.vms

  name                   = each.key
  generation             = 2
  path                   = var.vm_path
  processor_count        = each.value.vcpu
  static_memory          = true
  memory_startup_bytes   = floor(each.value.ram_gb * local.gib)
  automatic_start_action = "Start"
  automatic_stop_action  = "ShutDown"
  state                  = "Running"

  vm_firmware {
    enable_secure_boot = "Off"

    # Disk first, then DVD: after Talos is installed the ISO can stay attached.
    boot_order {
      boot_type           = "HardDiskDrive"
      controller_number   = 0
      controller_location = 0
    }
    boot_order {
      boot_type           = "DvdDrive"
      controller_number   = 0
      controller_location = length(each.value.disks)
    }
  }

  dynamic "hard_disk_drives" {
    for_each = each.value.disks
    content {
      controller_type     = "Scsi"
      controller_number   = 0
      controller_location = hard_disk_drives.key
      path                = local.disks["${each.key}-${hard_disk_drives.key}"].path
    }
  }

  dvd_drives {
    controller_number   = 0
    controller_location = length(each.value.disks)
    path                = local.iso_file[local.all_iso_urls[each.key]]
  }

  network_adaptors {
    name                = "eth0"
    switch_name         = hyperv_network_switch.external.name
    dynamic_mac_address = false
    static_mac_address  = each.value.mac
    wait_for_ips        = false
  }

  depends_on = [hyperv_vhd.disk, terraform_data.iso]
}
