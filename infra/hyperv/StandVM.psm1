Set-StrictMode -Version Latest

# Pure helpers: no Hyper-V cmdlets except Get-StandVmState, so the rest is testable anywhere.

function ConvertTo-StandByte {
    [CmdletBinding()]
    [OutputType([long])]
    param([Parameter(Mandatory)][double]$GB)
    return [long]($GB * 1GB)
}

function Test-StandConfig {
    <# Returns validation errors; no output means the config is valid. Wrap calls in @(). #>
    [CmdletBinding()]
    [OutputType([string[]])]
    param([Parameter(Mandatory)][AllowNull()]$Config)

    $errors = [System.Collections.Generic.List[string]]::new()
    $props = @(if ($null -ne $Config) { $Config.PSObject.Properties.Name })
    foreach ($key in 'switchName', 'netAdapterName', 'vmPath', 'vhdPath', 'vms') {
        $missing = $key -notin $props -or $null -eq $Config.$key
        if (-not $missing -and $key -ne 'vms') { $missing = [string]::IsNullOrWhiteSpace([string]$Config.$key) }
        if ($missing) {
            $errors.Add("missing '$key'")
        }
    }
    if ($errors.Count -gt 0) { return $errors.ToArray() }

    $vms = @($Config.vms)
    if ($vms.Count -eq 0) { $errors.Add("'vms' is empty") }
    $seen = @{}
    foreach ($vm in $vms) {
        $name = if ($vm.PSObject.Properties['name']) { [string]$vm.name } else { '' }
        if ([string]::IsNullOrWhiteSpace($name)) { $errors.Add('vm without name'); continue }
        if ($seen.ContainsKey($name)) { $errors.Add("duplicate vm '$name'") }
        $seen[$name] = $true
        foreach ($field in 'vcpu', 'ramGB') {
            $value = if ($vm.PSObject.Properties[$field]) { $vm.$field } else { $null }
            if (($value -isnot [int] -and $value -isnot [long] -and $value -isnot [double]) -or $value -le 0) {
                $errors.Add("${name}: '$field' must be a number > 0")
            }
        }
        $disks = @(if ($vm.PSObject.Properties['disks']) { $vm.disks })
        if ($disks.Count -eq 0) { $errors.Add("${name}: 'disks' is empty") }
        foreach ($d in $disks) {
            if (($d -isnot [int] -and $d -isnot [long] -and $d -isnot [double]) -or $d -le 0) { $errors.Add("${name}: disk size must be a number > 0") }
        }
        if ($vm.PSObject.Properties['iso']) {
            $iso = $vm.iso
            if ($iso -is [bool]) {
                if ($iso -and ('isoPath' -notin $props -or [string]::IsNullOrWhiteSpace([string]$Config.isoPath))) {
                    $errors.Add("${name}: 'iso' is true but 'isoPath' is not set")
                }
            }
            elseif ($iso -isnot [string] -or [string]::IsNullOrWhiteSpace($iso)) {
                $errors.Add("${name}: 'iso' must be true, false or a path")
            }
        }
    }
    return $errors.ToArray()
}

function Read-StandConfig {
    <# Loads and validates the parameter file; returns normalized VM descriptions. #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Path)

    $config = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    $errors = @(Test-StandConfig -Config $config)
    if ($errors.Count -gt 0) {
        throw "Invalid parameter file ${Path}: $($errors -join '; ')"
    }
    $vms = foreach ($vm in @($config.vms)) {
        $iso = $null
        if ($vm.PSObject.Properties['iso']) {
            if ($vm.iso -is [string]) { $iso = $vm.iso }
            elseif ($vm.iso) { $iso = [string]$config.isoPath }
        }
        [pscustomobject]@{
            Name       = [string]$vm.name
            Vcpu       = [int]$vm.vcpu
            RamBytes   = ConvertTo-StandByte -GB ([double]$vm.ramGB)
            DiskSizeGB = @($vm.disks | ForEach-Object { [long]$_ })
            Iso        = $iso
        }
    }
    return [pscustomobject]@{
        SwitchName     = [string]$config.switchName
        NetAdapterName = [string]$config.netAdapterName
        VmPath         = [string]$config.vmPath
        VhdPath        = [string]$config.vhdPath
        Vms            = @($vms)
    }
}

function Get-StandVmDiff {
    <#
    Compares a desired VM (from Read-StandConfig) with its actual state (Get-StandVmState).
    Returns differences; Fixable means a stopped VM can be corrected in place.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]$Desired,
        [Parameter(Mandatory)]$Actual
    )

    $diffs = [System.Collections.Generic.List[object]]::new()
    $add = {
        param($Property, $Want, $Have, $Fixable)
        $diffs.Add([pscustomobject]@{ Property = $Property; Desired = $Want; Actual = $Have; Fixable = $Fixable })
    }
    if ($Actual.Generation -ne 2) { & $add 'Generation' 2 $Actual.Generation $false }
    if ($Actual.Vcpu -ne $Desired.Vcpu) { & $add 'Vcpu' $Desired.Vcpu $Actual.Vcpu $true }
    if ($Actual.RamBytes -ne $Desired.RamBytes) { & $add 'RamBytes' $Desired.RamBytes $Actual.RamBytes $true }
    if ($Actual.DynamicMemory) { & $add 'DynamicMemory' $false $true $true }
    if ($Actual.SecureBoot) { & $add 'SecureBoot' $false $true $true }
    if (-not $Actual.AutoStart) { & $add 'AutoStart' $true $false $true }

    $want = @($Desired.DiskSizeGB)
    $have = @($Actual.DiskSizeGB)
    for ($i = 0; $i -lt [Math]::Max($want.Count, $have.Count); $i++) {
        if ($i -ge $have.Count) { & $add "Disk[$i]" $want[$i] $null $true }
        elseif ($i -ge $want.Count) { & $add "Disk[$i]" $null $have[$i] $false }
        elseif ($want[$i] -ne $have[$i]) { & $add "Disk[$i]" $want[$i] $have[$i] $false }
    }
    return $diffs.ToArray()
}

function Get-StandVmState {
    <# Reads the actual state of an existing VM through the Hyper-V module; $null when absent. #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name)

    $vm = Get-VM -Name $Name -ErrorAction SilentlyContinue
    if (-not $vm) { return $null }
    $drives = @(Get-VMHardDiskDrive -VM $vm | Sort-Object ControllerNumber, ControllerLocation)
    $sizes = @($drives | ForEach-Object { [long]((Get-VHD -Path $_.Path).Size / 1GB) })
    $secureBoot = $false
    if ($vm.Generation -eq 2) {
        $secureBoot = (Get-VMFirmware -VM $vm).SecureBoot -eq 'On'
    }
    return [pscustomobject]@{
        Name          = $Name
        Generation    = $vm.Generation
        State         = [string]$vm.State
        Vcpu          = (Get-VMProcessor -VM $vm).Count
        RamBytes      = (Get-VMMemory -VM $vm).Startup
        DynamicMemory = (Get-VMMemory -VM $vm).DynamicMemoryEnabled
        SecureBoot    = $secureBoot
        AutoStart     = $vm.AutomaticStartAction -eq 'Start'
        DiskSizeGB    = $sizes
        DiskPaths     = @($drives | ForEach-Object { $_.Path })
    }
}

Export-ModuleMember -Function ConvertTo-StandByte, Test-StandConfig, Read-StandConfig, Get-StandVmDiff, Get-StandVmState
