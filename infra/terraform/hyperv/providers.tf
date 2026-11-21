# WinRM over HTTPS with NTLM to the stand PC (SRS 6.1, ADR 0008).
provider "hyperv" {
  host     = var.winrm_host
  port     = var.winrm_port
  https    = true
  insecure = var.winrm_insecure
  use_ntlm = true
  user     = var.winrm_user
  password = var.winrm_password
  timeout  = "60s"
}
