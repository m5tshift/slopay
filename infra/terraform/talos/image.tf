# One schematic, ISO and installer per node: the node address is in extraKernelArgs.
resource "talos_image_factory_schematic" "node" {
  for_each = var.nodes

  schematic = yamlencode({
    customization = {
      extraKernelArgs = local.kernel_args[each.key]
    }
  })
}

data "talos_image_factory_urls" "node" {
  for_each = var.nodes

  talos_version = var.talos_version
  schematic_id  = talos_image_factory_schematic.node[each.key].id
  platform      = "metal"
  architecture  = "amd64"
}
