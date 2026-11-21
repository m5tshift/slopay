variable "cluster_name" {
  type    = string
  default = "weigate"
}

variable "talos_version" {
  description = "Talos version of the ISO and installer, with the leading v (for example v1.x.y)."
  type        = string

  validation {
    condition     = can(regex("^v[0-9]+\\.[0-9]+\\.[0-9]+$", var.talos_version))
    error_message = "talos_version must look like v1.x.y."
  }
}

variable "kubernetes_version" {
  description = "Kubernetes version without the leading v (for example 1.x.y)."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must look like 1.x.y."
  }
}

variable "nodes" {
  description = "Nodes by VM name (same keys as vms in infra/terraform/hyperv): role and static address."
  type = map(object({
    role = string
    ip   = string
  }))

  validation {
    condition     = alltrue([for n in values(var.nodes) : contains(["controlplane", "worker"], n.role)])
    error_message = "role must be controlplane or worker."
  }

  validation {
    condition     = length([for n in values(var.nodes) : n if n.role == "controlplane"]) == 1
    error_message = "Exactly one control plane node is supported."
  }

  validation {
    condition     = alltrue([for n in values(var.nodes) : can(cidrhost("${n.ip}/32", 0))])
    error_message = "ip must be an IPv4 address."
  }

  validation {
    condition     = length(distinct([for n in values(var.nodes) : n.ip])) == length(var.nodes)
    error_message = "Node addresses must be unique."
  }
}

variable "prefixlen" {
  type    = number
  default = 24

  validation {
    condition     = var.prefixlen >= 8 && var.prefixlen <= 30
    error_message = "prefixlen must be between 8 and 30."
  }
}

variable "gateway" {
  type = string
}

variable "dns" {
  type = string
}

variable "install_disk" {
  description = "Disk Talos is installed on. Optional check on a node in maintenance mode: talosctl get disks --insecure -n <ip>."
  type        = string
  default     = "/dev/sda"
}

variable "apply_mode" {
  type    = string
  default = "auto"
}

variable "config_dir" {
  description = "Directory outside the repository for kubeconfig-admin and talosconfig (for example ~/.weigate)."
  type        = string
}

variable "full_stand" {
  description = "Full stand: no flannel and kube-proxy (Cilium is installed later) and Zot as registry mirror."
  type        = bool
  default     = false
}

variable "zot_endpoint" {
  description = "Zot NodePort address for registries.mirrors, for example http://192.0.2.12:30500 (worker address)."
  type        = string
  default     = ""

  validation {
    condition     = !var.full_stand || can(regex("^http://[^/]+$", var.zot_endpoint))
    error_message = "With full_stand, zot_endpoint must look like http://<address>:<port>."
  }
}

variable "registry_mirror_hosts" {
  type    = list(string)
  default = ["docker.io", "ghcr.io", "quay.io", "registry.k8s.io"]
}
