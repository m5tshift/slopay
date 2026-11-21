terraform {
  required_version = ">= 1.9"

  required_providers {
    talos = {
      source  = "siderolabs/talos"
      version = "0.12.0"
    }
    # Writes kubeconfig and talosconfig to files outside Git.
    local = {
      source  = "hashicorp/local"
      version = "2.9.1"
    }
  }
}
