#Requires -Version 5.1
#Requires -RunAsAdministrator
<#
.SYNOPSIS
Removes stand VMs listed in the parameter file.
.DESCRIPTION
Only VMs from the file are considered; the virtual switch is kept. VHDX files are deleted
only with -RemoveDisks. Asks for confirmation for every VM.
.PARAMETER ConfigPath
Parameter file used to create the VMs.
.PARAMETER Name
Limit removal to these VMs (must be present in the file).
.PARAMETER RemoveDisks
Also delete the VHDX files of the removed VMs.
#>
[CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
param(
    [Parameter(Mandatory)][string]$ConfigPath,
    [string[]]$Name,
    [switch]$RemoveDisks
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $PSScriptRoot 'StandVM.psm1') -Force
Import-Module Hyper-V

$config = Read-StandConfig -Path $ConfigPath
$targets = @($config.Vms)
if ($Name) {
    $unknown = @($Name | Where-Object { $_ -notin $targets.Name })
    if ($unknown.Count -gt 0) { throw "Not in the parameter file: $($unknown -join ', ')" }
    $targets = @($targets | Where-Object { $_.Name -in $Name })
}

foreach ($vm in $targets) {
    $actual = Get-StandVmState -Name $vm.Name
    if (-not $actual) {
        Write-Verbose "$($vm.Name): not found"
        continue
    }
    $action = if ($RemoveDisks) { 'Remove VM and its VHDX files' } else { 'Remove VM (VHDX files are kept)' }
    if (-not $PSCmdlet.ShouldProcess($vm.Name, $action)) { continue }
    if ($actual.State -ne 'Off') { Stop-VM -Name $vm.Name -TurnOff -Force }
    Remove-VM -Name $vm.Name -Force
    if ($RemoveDisks) {
        foreach ($path in $actual.DiskPaths) { Remove-Item -LiteralPath $path -Force }
    }
}
