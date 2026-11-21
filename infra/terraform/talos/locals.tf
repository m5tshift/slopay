locals {
  controlplane = one([for name, n in var.nodes : name if n.role == "controlplane"])
  cp_ip        = var.nodes[local.controlplane].ip
  netmask      = cidrnetmask("${local.cp_ip}/${var.prefixlen}")
  config_dir   = pathexpand(var.config_dir)

  # Static address through the kernel command line (ADR 0008): the node boots from the ISO already on it.
  kernel_args = {
    for name, n in var.nodes : name => [
      "net.ifnames=0",
      "ip=${n.ip}::${var.gateway}:${local.netmask}:${name}:eth0:off:${var.dns}",
    ]
  }

  # Every patch is its own list element: the UserVolumeConfig document is not merged into another document.
  patches = {
    for name, n in var.nodes : name => concat(
      [templatefile("${path.module}/../../talos/patches/${n.role == "controlplane" ? "controlplane" : "worker"}.yaml.tmpl", {
        hostname      = name
        ip            = n.ip
        prefixlen     = var.prefixlen
        gateway       = var.gateway
        dns           = var.dns
        install_disk  = var.install_disk
        install_image = data.talos_image_factory_urls.node[name].urls.installer
      })],
      n.role == "worker" ? [file("${path.module}/../../talos/patches/worker-data-volume.yaml.tmpl")] : [],
    )
  }
}
