# The provider cannot download files: Terraform does it on the host over WinRM (ADR 0008).
resource "terraform_data" "host_dirs" {
  triggers_replace = [var.vm_path, var.vhd_path, var.iso_dir]

  connection {
    type     = "winrm"
    host     = var.winrm_host
    port     = var.winrm_port
    user     = var.winrm_user
    password = var.winrm_password
    https    = true
    insecure = var.winrm_insecure
    use_ntlm = true
  }

  provisioner "remote-exec" {
    inline = [
      "powershell -NoProfile -Command \"foreach ($d in @('${var.vm_path}','${var.vhd_path}','${var.iso_dir}')) { New-Item -ItemType Directory -Force -Path $d | Out-Null }\"",
    ]
  }
}

resource "terraform_data" "iso" {
  for_each         = local.iso_urls_unique
  triggers_replace = [each.key, lookup(var.iso_sha256, each.key, "")]

  connection {
    type     = "winrm"
    host     = var.winrm_host
    port     = var.winrm_port
    user     = var.winrm_user
    password = var.winrm_password
    https    = true
    insecure = var.winrm_insecure
    use_ntlm = true
  }

  provisioner "remote-exec" {
    inline = [
      join(" ", [
        "powershell -NoProfile -Command \"$ErrorActionPreference='Stop'; $f='${local.iso_file[each.key]}';",
        "if (-not (Test-Path $f)) { $ProgressPreference='SilentlyContinue'; Invoke-WebRequest -UseBasicParsing -Uri '${each.key}' -OutFile \\\"$f.part\\\"; Move-Item -Force \\\"$f.part\\\" $f };",
        "$want='${lookup(var.iso_sha256, each.key, "")}';",
        "if ($want -and ((Get-FileHash -Algorithm SHA256 $f).Hash -ne $want)) { Remove-Item -Force $f; throw 'SHA256 mismatch' }\"",
      ]),
    ]
  }

  depends_on = [terraform_data.host_dirs]
}
