#Requires -Version 5.1
#Requires -RunAsAdministrator
<#
.SYNOPSIS
Creates or extends the SLOPay stand VMs on Hyper-V from a parameter file (SRS 6.1, 6.4).
.DESCRIPTION
Idempotent: existing VMs are compared with the file and left alone. Differences that can be
fixed in place (vCPU, RAM, a missing disk, Secure Boot, autostart) are applied to stopped VMs
only with -Reconcile; the rest is reported. VMs absent from the file are never touched.
VMs are created but not started. Run with -WhatIf first.
.PARAMETER ConfigPath
Parameter file, a copy of stand.minimal.json.example or stand.full.json.example.
.PARAMETER Reconcile
Apply fixable differences to stopped, already existing VMs.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$ConfigPath,
    [switch]$Reconcile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $PSScriptRoot 'StandVM.psm1') -Force
Import-Module Hyper-V

$config = Read-StandConfig -Path $ConfigPath
$problems = 0

foreach ($vm in $config.Vms) {
    if ($vm.Iso -and -not (Test-Path -LiteralPath $vm.Iso)) {
        throw "$($vm.Name): ISO not found: $($vm.Iso)"
    }
}

# External switch. Creating it briefly interrupts the host network connection.
if (-not (Get-VMSwitch -Name $config.SwitchName -ErrorAction SilentlyContinue)) {
    if ($PSCmdlet.ShouldProcess($config.SwitchName, "Create external switch on '$($config.NetAdapterName)' (host network blips)")) {
        New-VMSwitch -Name $config.SwitchName -NetAdapterName $config.NetAdapterName -AllowManagementOS $true | Out-Null
    }
}

function Add-StandDisk {
    param([string]$VmName, [string]$Path, [long]$SizeGB)
    if (Test-Path -LiteralPath $Path) { throw "${VmName}: disk file already exists: $Path" }
    New-VHD -Path $Path -SizeBytes ($SizeGB * 1GB) -Dynamic | Out-Null
    Add-VMHardDiskDrive -VMName $VmName -Path $Path
}

function Get-StandDiskPath {
    param([string]$VmName, [int]$Index)
    $leaf = if ($Index -eq 0) { "$VmName.vhdx" } else { "$VmName-data$Index.vhdx" }
    return Join-Path $config.VhdPath $leaf
}

foreach ($vm in $config.Vms) {
    $actual = Get-StandVmState -Name $vm.Name

    if (-not $actual) {
        if ($PSCmdlet.ShouldProcess($vm.Name, "Create VM ($($vm.Vcpu) vCPU, $($vm.RamBytes / 1GB) GB, disks $($vm.DiskSizeGB -join '+') GB)")) {
            $systemDisk = Get-StandDiskPath -VmName $vm.Name -Index 0
            if (Test-Path -LiteralPath $systemDisk) { throw "$($vm.Name): disk file already exists: $systemDisk" }
            New-VHD -Path $systemDisk -SizeBytes ($vm.DiskSizeGB[0] * 1GB) -Dynamic | Out-Null
            New-VM -Name $vm.Name -Generation 2 -MemoryStartupBytes $vm.RamBytes `
                -VHDPath $systemDisk -SwitchName $config.SwitchName -Path $config.VmPath | Out-Null
            Set-VMProcessor -VMName $vm.Name -Count $vm.Vcpu
            Set-VMMemory -VMName $vm.Name -DynamicMemoryEnabled $false -StartupBytes $vm.RamBytes
            Set-VMFirmware -VMName $vm.Name -EnableSecureBoot Off
            Set-VM -Name $vm.Name -AutomaticStartAction Start -AutomaticStopAction ShutDown
            for ($i = 1; $i -lt $vm.DiskSizeGB.Count; $i++) {
                Add-StandDisk -VmName $vm.Name -Path (Get-StandDiskPath -VmName $vm.Name -Index $i) -SizeGB $vm.DiskSizeGB[$i]
            }
            if ($vm.Iso) { Add-VMDvdDrive -VMName $vm.Name -Path $vm.Iso }
            Write-Verbose "$($vm.Name): created"
        }
        continue
    }

    $diffs = @(Get-StandVmDiff -Desired $vm -Actual $actual)
    if ($diffs.Count -eq 0) {
        Write-Verbose "$($vm.Name): matches the file"
        continue
    }
    foreach ($d in $diffs) {
        Write-Warning "$($vm.Name): $($d.Property) is '$($d.Actual)', file says '$($d.Desired)'$(if (-not $d.Fixable) { ' (cannot be fixed in place)' })"
    }
    if (@($diffs | Where-Object { -not $_.Fixable }).Count -gt 0) { $problems++ }

    if (-not $Reconcile) { continue }
    if ($actual.State -ne 'Off') {
        Write-Warning "$($vm.Name): is $($actual.State), not reconciled; stop it first"
        continue
    }
    foreach ($d in @($diffs | Where-Object { $_.Fixable })) {
        if (-not $PSCmdlet.ShouldProcess($vm.Name, "Fix $($d.Property)")) { continue }
        switch -Wildcard ($d.Property) {
            'Vcpu' { Set-VMProcessor -VMName $vm.Name -Count $vm.Vcpu }
            'RamBytes' { Set-VMMemory -VMName $vm.Name -StartupBytes $vm.RamBytes }
            'DynamicMemory' { Set-VMMemory -VMName $vm.Name -DynamicMemoryEnabled $false }
            'SecureBoot' { Set-VMFirmware -VMName $vm.Name -EnableSecureBoot Off }
            'AutoStart' { Set-VM -Name $vm.Name -AutomaticStartAction Start }
            'Disk[[]*' {
                $index = [int]($d.Property -replace '\D', '')
                Add-StandDisk -VmName $vm.Name -Path (Get-StandDiskPath -VmName $vm.Name -Index $index) -SizeGB $vm.DiskSizeGB[$index]
            }
        }
    }
}

# MAC addresses are dynamic: record them for DHCP reservations and Talos patches.
Get-VMNetworkAdapter -VMName ($config.Vms | ForEach-Object { $_.Name }) -ErrorAction SilentlyContinue |
    Select-Object @{ n = 'VM'; e = { $_.VMName } }, @{ n = 'MAC'; e = { ($_.MacAddress -replace '(..)(?!$)', '$1-') } }

if ($problems -gt 0) {
    Write-Error "$problems VM(s) differ from the file in ways that cannot be fixed in place"
    exit 1
}
