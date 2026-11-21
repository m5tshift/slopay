variable "winrm_host" {
  description = "Name or address of the stand PC (WinRM HTTPS). Set in a *.tfvars file outside Git."
  type        = string
}

variable "winrm_port" {
  type    = number
  default = 5986
}

variable "winrm_user" {
  type    = string
  default = "Administrator"
}

variable "winrm_password" {
  description = "Password of the WinRM account. Set in a *.tfvars file outside Git."
  type        = string
  sensitive   = true
}

variable "winrm_insecure" {
  description = "Skip TLS verification: the stand PC uses a self-signed certificate."
  type        = bool
  default     = true
}

variable "switch_name" {
  type    = string
  default = "slopay-external"
}

variable "net_adapter_name" {
  description = "Ethernet adapter on the stand PC that the External vSwitch binds to."
  type        = string
  default     = "Ethernet"
}

variable "vm_path" {
  type    = string
  default = "D:\\Hyper-V\\VMs"
}

variable "vhd_path" {
  type    = string
  default = "D:\\Hyper-V\\VHDX"
}

variable "iso_dir" {
  description = "Directory on the stand PC where Terraform downloads the ISO files."
  type        = string
  default     = "D:\\ISO"
}

variable "vms" {
  description = "VMs by name: vCPU, static RAM in GB, disk sizes in GB (first is the system disk), static MAC."
  type = map(object({
    vcpu   = number
    ram_gb = number
    disks  = list(number)
    mac    = string
  }))

  validation {
    condition     = alltrue([for v in values(var.vms) : length(v.disks) > 0])
    error_message = "Every VM needs at least one disk."
  }

  validation {
    condition     = alltrue([for v in values(var.vms) : can(regex("^02[0-9A-F]{10}$", v.mac))])
    error_message = "mac: 12 upper-case hex digits without separators, starting with 02 (locally administered)."
  }

  validation {
    condition     = length(distinct([for v in values(var.vms) : v.mac])) == length(var.vms)
    error_message = "MAC addresses must be unique."
  }
}

variable "iso_urls" {
  description = "ISO URL for every VM (same keys as vms). Talos: per-node Image Factory URL with the node address in extraKernelArgs."
  type        = map(string)

  validation {
    condition     = alltrue([for u in values(var.iso_urls) : startswith(u, "https://")])
    error_message = "ISO URLs must be https."
  }

  validation {
    condition     = toset(keys(var.iso_urls)) == toset(keys(var.vms))
    error_message = "iso_urls must have exactly the same keys as vms."
  }
}

variable "iso_sha256" {
  description = "Optional SHA256 of the ISO by URL. When set, the download is verified."
  type        = map(string)
  default     = {}
}
