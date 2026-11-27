# Tests with mocked providers (apply runs against the mocks): no Image Factory and no node are contacted.
# The generated machine configuration is a mock value, so the patches passed to the
# data source are checked instead.
mock_provider "talos" {
  mock_data "talos_image_factory_urls" {
    defaults = {
      urls = {
        iso       = "https://factory.example/image/id/v1.0.0/metal-amd64.iso"
        installer = "factory.example/metal-installer/id:v1.0.0"
      }
    }
  }
}
mock_provider "local" {}

variables {
  talos_version      = "v1.0.0"
  kubernetes_version = "1.0.0"
  nodes = {
    "talos-cp-1" = { role = "controlplane", ip = "192.0.2.11" }
    "talos-w-1"  = { role = "worker", ip = "192.0.2.12" }
  }
  gateway    = "192.0.2.1"
  dns        = "192.0.2.53"
  config_dir = "/tmp/weigate-test"
}

run "patches" {
  command = apply

  assert {
    condition = alltrue([
      for k, n in var.nodes : length(regexall("hostname: ${k}\\n", data.talos_machine_configuration.node[k].config_patches[0])) == 1
    ])
    error_message = "hostname of each node must be in its patch"
  }
  assert {
    condition = alltrue([
      for k, n in var.nodes : (
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.network.interfaces[0].addresses[0] == "${n.ip}/24" &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.network.interfaces[0].dhcp == false &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.network.interfaces[0].routes[0].gateway == "192.0.2.1" &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.network.nameservers[0] == "192.0.2.53"
      )
    ])
    error_message = "static address, gateway and DNS must be in the patches"
  }
  assert {
    condition = alltrue([
      for k, n in var.nodes : (
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.install.disk == "/dev/sda" &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[0]).machine.install.image == "factory.example/metal-installer/id:v1.0.0"
      )
    ])
    error_message = "install disk and installer image of the node"
  }
  assert {
    condition = (
      data.talos_machine_configuration.node["talos-cp-1"].machine_type == "controlplane" &&
      data.talos_machine_configuration.node["talos-w-1"].machine_type == "worker" &&
      yamldecode(data.talos_machine_configuration.node["talos-w-1"].config_patches[0]).machine.sysctls["user.max_user_namespaces"] == "11255"
    )
    error_message = "machine types and worker user.max_user_namespaces sysctl"
  }
}

run "worker_data_volume" {
  command = apply

  assert {
    condition     = length(data.talos_machine_configuration.node["talos-cp-1"].config_patches) == 1
    error_message = "control plane has no data volume"
  }
  assert {
    condition     = length(data.talos_machine_configuration.node["talos-w-1"].config_patches) == 2
    error_message = "worker has the base patch and the data volume patch"
  }
  assert {
    condition = (
      yamldecode(data.talos_machine_configuration.node["talos-w-1"].config_patches[1]).kind == "UserVolumeConfig" &&
      yamldecode(data.talos_machine_configuration.node["talos-w-1"].config_patches[1]).name == "data" &&
      yamldecode(data.talos_machine_configuration.node["talos-w-1"].config_patches[1]).provisioning.diskSelector.match == "!system_disk" &&
      yamldecode(data.talos_machine_configuration.node["talos-w-1"].config_patches[1]).provisioning.minSize == "40GB"
    )
    error_message = "worker data volume: name data, non-system disk, at least 40GB"
  }
}

run "static_addresses" {
  command = apply

  assert {
    condition = alltrue([
      for k, n in var.nodes :
      talos_machine_configuration_apply.node[k].node == n.ip && talos_machine_configuration_apply.node[k].endpoint == n.ip
    ])
    error_message = "node and endpoint are the node address"
  }
  assert {
    condition     = talos_machine_bootstrap.this.node == "192.0.2.11" && talos_machine_bootstrap.this.endpoint == "192.0.2.11"
    error_message = "bootstrap goes to the control plane address"
  }
  assert {
    condition     = output.cluster_endpoint == "https://192.0.2.11:6443"
    error_message = "cluster endpoint"
  }
}

run "schematic_per_node" {
  command = apply

  assert {
    condition     = length(talos_image_factory_schematic.node) == 2
    error_message = "one schematic per node"
  }
  assert {
    condition = alltrue([
      for k, n in var.nodes : (
        strcontains(yamldecode(talos_image_factory_schematic.node[k].schematic).customization.extraKernelArgs[1], "ip=${n.ip}::192.0.2.1:255.255.255.0:${k}:eth0:off:192.0.2.53") &&
        yamldecode(talos_image_factory_schematic.node[k].schematic).customization.extraKernelArgs[0] == "net.ifnames=0"
      )
    ])
    error_message = "extraKernelArgs: net.ifnames=0 and ip= with the node address"
  }
  assert {
    condition     = toset(keys(output.iso_urls)) == toset(["talos-cp-1", "talos-w-1"])
    error_message = "iso_urls has a key for every node"
  }
}

run "credential_files" {
  command = apply

  assert {
    condition = (
      local_sensitive_file.kubeconfig.filename == "/tmp/weigate-test/kubeconfig-admin" &&
      local_sensitive_file.talosconfig.filename == "/tmp/weigate-test/talosconfig" &&
      local_sensitive_file.kubeconfig.file_permission == "0600" &&
      local_sensitive_file.talosconfig.file_permission == "0600"
    )
    error_message = "kubeconfig and talosconfig go to config_dir with mode 0600"
  }
}

run "two_control_planes" {
  command = plan
  variables {
    nodes = {
      "talos-cp-1" = { role = "controlplane", ip = "192.0.2.11" }
      "talos-cp-2" = { role = "controlplane", ip = "192.0.2.13" }
    }
  }
  expect_failures = [var.nodes]
}

run "bad_ip" {
  command = plan
  variables {
    nodes = {
      "talos-cp-1" = { role = "controlplane", ip = "not-an-ip" }
    }
  }
  expect_failures = [var.nodes]
}

run "bad_talos_version" {
  command = plan
  variables {
    talos_version = "1.0.0"
  }
  expect_failures = [var.talos_version]
}

run "full_stand" {
  command = apply
  variables {
    nodes = {
      "talos-cp-1" = { role = "controlplane", ip = "192.0.2.11" }
      "talos-w-1"  = { role = "worker", ip = "192.0.2.12" }
      "talos-w-2"  = { role = "worker", ip = "192.0.2.13" }
      "talos-w-3"  = { role = "worker", ip = "192.0.2.14" }
    }
    full_stand   = true
    zot_endpoint = "http://192.0.2.12:30500"
  }

  assert {
    condition = alltrue([
      for k, n in var.nodes : (
        yamldecode(data.talos_machine_configuration.node[k].config_patches[1]).cluster.network.cni.name == "none" &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[1]).cluster.proxy.disabled == true &&
        toset(keys(yamldecode(data.talos_machine_configuration.node[k].config_patches[1]).machine.registries.mirrors)) == toset(["docker.io", "ghcr.io", "quay.io", "registry.k8s.io"]) &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[1]).machine.registries.mirrors["ghcr.io"].endpoints[0] == "http://192.0.2.12:30500/v2/ghcr.io" &&
        yamldecode(data.talos_machine_configuration.node[k].config_patches[1]).machine.registries.mirrors["ghcr.io"].overridePath == true
      )
    ])
    error_message = "full stand: no CNI and kube-proxy, Zot mirrors on every node"
  }
  assert {
    condition = alltrue([
      for k, n in var.nodes : n.role == "worker" ?
      yamldecode(data.talos_machine_configuration.node[k].config_patches[2]).name == "data" :
      length(data.talos_machine_configuration.node[k].config_patches) == 2
    ])
    error_message = "full stand: data volume on workers only"
  }
}

run "full_stand_without_zot" {
  command = plan
  variables {
    full_stand = true
  }
  expect_failures = [var.zot_endpoint]
}
