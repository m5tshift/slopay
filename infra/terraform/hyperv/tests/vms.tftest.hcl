# Plan-only tests with a mocked provider: no Hyper-V host is contacted.
mock_provider "hyperv" {}

variables {
  winrm_host     = "stand.invalid"
  winrm_password = "x"
  iso_urls = {
    "talos-cp-1" = "https://factory.example/image/cp1/metal-amd64.iso"
    "talos-w-1"  = "https://factory.example/image/w1/metal-amd64.iso"
  }
  vms = {
    "talos-cp-1" = { vcpu = 2, ram_gb = 3, disks = [20], mac = "020000000001" }
    "talos-w-1"  = { vcpu = 6, ram_gb = 14, disks = [20, 50], mac = "020000000002" }
  }
}

run "minimal" {
  command = plan

  assert {
    condition     = toset(keys(hyperv_machine_instance.vm)) == toset(["talos-cp-1", "talos-w-1"])
    error_message = "minimal: expected talos-cp-1 and talos-w-1"
  }
  assert {
    condition     = length(hyperv_vhd.disk) == 3
    error_message = "minimal: expected 3 VHDX"
  }
  assert {
    condition     = hyperv_machine_instance.vm["talos-w-1"].memory_startup_bytes == 14 * 1073741824 && hyperv_machine_instance.vm["talos-w-1"].processor_count == 6
    error_message = "minimal: talos-w-1 must have 14 GiB and 6 vCPU"
  }
  assert {
    condition     = hyperv_network_switch.external.switch_type == "External"
    error_message = "switch must be External"
  }
  assert {
    condition = alltrue([
      for vm in values(hyperv_machine_instance.vm) :
      vm.generation == 2 && vm.static_memory && vm.automatic_start_action == "Start" &&
      vm.vm_firmware[0].enable_secure_boot == "Off"
    ])
    error_message = "all VMs: generation 2, static memory, autostart, Secure Boot off"
  }
  assert {
    condition = alltrue([
      for vm in values(hyperv_machine_instance.vm) :
      vm.vm_firmware[0].boot_order[0].boot_type == "HardDiskDrive" && vm.vm_firmware[0].boot_order[1].boot_type == "DvdDrive" &&
      length(vm.vm_firmware[0].boot_order) == 2 && length(vm.dvd_drives) == 1
    ])
    error_message = "boot order must be disk, then DVD"
  }
  assert {
    condition = alltrue([
      for k, vm in hyperv_machine_instance.vm :
      vm.network_adaptors[0].dynamic_mac_address == false && vm.network_adaptors[0].static_mac_address == var.vms[k].mac
    ])
    error_message = "MAC addresses must be static and equal to the variable"
  }
  assert {
    condition     = length(terraform_data.iso) == 2
    error_message = "minimal: one ISO download per unique URL"
  }
}

run "full" {
  command = plan

  variables {
    vms = {
      "talos-cp-1" = { vcpu = 2, ram_gb = 3, disks = [20], mac = "020000000001" }
      "talos-w-1"  = { vcpu = 3, ram_gb = 7.5, disks = [20, 50], mac = "020000000002" }
      "talos-w-2"  = { vcpu = 3, ram_gb = 7.5, disks = [20, 50], mac = "020000000003" }
      "talos-w-3"  = { vcpu = 3, ram_gb = 7.5, disks = [20, 50], mac = "020000000004" }
      "lb-1"       = { vcpu = 1, ram_gb = 0.5, disks = [8], mac = "020000000005" }
      "lb-2"       = { vcpu = 1, ram_gb = 0.5, disks = [8], mac = "020000000006" }
    }
    iso_urls = {
      "talos-cp-1" = "https://factory.example/image/cp1/metal-amd64.iso"
      "talos-w-1"  = "https://factory.example/image/w1/metal-amd64.iso"
      "talos-w-2"  = "https://factory.example/image/w2/metal-amd64.iso"
      "talos-w-3"  = "https://factory.example/image/w3/metal-amd64.iso"
      "lb-1"       = "https://cdimage.example/debian.iso"
      "lb-2"       = "https://cdimage.example/debian.iso"
    }
  }

  assert {
    condition     = toset(keys(hyperv_machine_instance.vm)) == toset(["talos-cp-1", "talos-w-1", "talos-w-2", "talos-w-3", "lb-1", "lb-2"])
    error_message = "full: expected 6 VMs including lb-1 and lb-2"
  }
  assert {
    condition     = length(hyperv_vhd.disk) == 9
    error_message = "full: expected 9 VHDX"
  }
  assert {
    condition     = hyperv_machine_instance.vm["lb-1"].memory_startup_bytes == 536870912 && hyperv_machine_instance.vm["talos-w-2"].memory_startup_bytes == 8053063680
    error_message = "full: memory of lb (0.5 GiB) and workers (7.5 GiB)"
  }
  assert {
    condition = alltrue([
      for vm in values(hyperv_machine_instance.vm) :
      vm.vm_firmware[0].boot_order[0].boot_type == "HardDiskDrive" && vm.vm_firmware[0].boot_order[1].boot_type == "DvdDrive"
    ])
    error_message = "boot order must be disk, then DVD"
  }
  assert {
    condition     = length(distinct([for vm in values(hyperv_machine_instance.vm) : vm.network_adaptors[0].static_mac_address])) == 6
    error_message = "full: six distinct static MACs"
  }
  assert {
    condition     = length(terraform_data.iso) == 5
    error_message = "full: Debian ISO is shared by lb-1 and lb-2"
  }
}

run "outputs" {
  command = plan

  assert {
    condition     = join(",", output.vm_names) == "talos-cp-1,talos-w-1"
    error_message = "vm_names"
  }
  assert {
    condition     = output.iso_files["talos-cp-1"] != output.iso_files["talos-w-1"]
    error_message = "each Talos node has its own ISO file"
  }
}

run "invalid_mac" {
  command = plan
  variables {
    vms = {
      "talos-cp-1" = { vcpu = 2, ram_gb = 3, disks = [20], mac = "00:15:5D:00:00:01" }
      "talos-w-1"  = { vcpu = 6, ram_gb = 14, disks = [20], mac = "020000000002" }
    }
  }
  expect_failures = [var.vms]
}

run "duplicate_mac" {
  command = plan
  variables {
    vms = {
      "talos-cp-1" = { vcpu = 2, ram_gb = 3, disks = [20], mac = "020000000001" }
      "talos-w-1"  = { vcpu = 6, ram_gb = 14, disks = [20], mac = "020000000001" }
    }
  }
  expect_failures = [var.vms]
}

run "iso_keys_mismatch" {
  command = plan
  variables {
    iso_urls = { "talos-cp-1" = "https://factory.example/image/cp1/metal-amd64.iso" }
  }
  expect_failures = [var.iso_urls]
}
