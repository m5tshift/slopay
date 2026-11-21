BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'StandVM.psm1') -Force
    $script:ExampleDir = $PSScriptRoot

    function Get-TestActual {
        param($Vcpu = 2, $RamBytes = 3GB, $DiskSizeGB = @(20), $Generation = 2, $SecureBoot = $false, $DynamicMemory = $false, $AutoStart = $true)
        [pscustomobject]@{
            Generation = $Generation; Vcpu = $Vcpu; RamBytes = [long]$RamBytes; DynamicMemory = $DynamicMemory
            SecureBoot = $SecureBoot; AutoStart = $AutoStart; DiskSizeGB = $DiskSizeGB
        }
    }
}

Describe 'Test-StandConfig' {
    It 'accepts a minimal valid config' {
        $c = Get-Content (Join-Path $script:ExampleDir 'stand.minimal.json.example') -Raw | ConvertFrom-Json
        @(Test-StandConfig -Config $c).Count | Should -Be 0
    }
    It 'rejects duplicate names' {
        $c = '{"switchName":"s","netAdapterName":"e","vmPath":"p","vhdPath":"v","vms":[{"name":"a","vcpu":1,"ramGB":1,"disks":[1]},{"name":"a","vcpu":1,"ramGB":1,"disks":[1]}]}' | ConvertFrom-Json
        (Test-StandConfig -Config $c) -join ';' | Should -Match "duplicate vm 'a'"
    }
    It 'rejects bad numbers and empty disks' {
        $c = '{"switchName":"s","netAdapterName":"e","vmPath":"p","vhdPath":"v","vms":[{"name":"a","vcpu":0,"ramGB":"x","disks":[]}]}' | ConvertFrom-Json
        $e = (Test-StandConfig -Config $c) -join ';'
        $e | Should -Match "'vcpu'"
        $e | Should -Match "'ramGB'"
        $e | Should -Match "'disks' is empty"
    }
    It 'requires isoPath when iso is true' {
        $c = '{"switchName":"s","netAdapterName":"e","vmPath":"p","vhdPath":"v","vms":[{"name":"a","vcpu":1,"ramGB":1,"disks":[1],"iso":true}]}' | ConvertFrom-Json
        (Test-StandConfig -Config $c) -join ';' | Should -Match "isoPath"
    }
    It 'reports missing top-level keys' {
        $c = '{"vms":[]}' | ConvertFrom-Json
        (Test-StandConfig -Config $c) -join ';' | Should -Match "missing 'switchName'"
    }
}

Describe 'Read-StandConfig' {
    It 'parses the minimal example per SRS 6.4' {
        $c = Read-StandConfig -Path (Join-Path $script:ExampleDir 'stand.minimal.json.example')
        $c.Vms.Name | Should -Be @('talos-cp-1', 'talos-w-1')
        $c.Vms[0].Vcpu | Should -Be 2
        $c.Vms[0].RamBytes | Should -Be 3GB
        $c.Vms[1].Vcpu | Should -Be 6
        $c.Vms[1].RamBytes | Should -Be 14GB
        $c.Vms[1].DiskSizeGB | Should -Be @(20, 50)
        $c.Vms[1].Iso | Should -Be 'D:\ISO\talos-metal-amd64.iso'
    }
    It 'parses the full example per SRS 6.1' {
        $c = Read-StandConfig -Path (Join-Path $script:ExampleDir 'stand.full.json.example')
        $c.Vms.Name | Should -Be @('talos-cp-1', 'talos-w-1', 'talos-w-2', 'talos-w-3', 'lb-1', 'lb-2')
        $w = $c.Vms | Where-Object Name -eq 'talos-w-2'
        $w.RamBytes | Should -Be ([long](7.5GB))
        $lb = $c.Vms | Where-Object Name -eq 'lb-1'
        $lb.RamBytes | Should -Be ([long](0.5GB))
        $lb.DiskSizeGB | Should -Be @(8)
        $lb.Iso | Should -Be 'D:\ISO\debian-netinst.iso'
    }
    It 'throws on an invalid file' {
        $p = Join-Path $TestDrive 'bad.json'
        '{"vms":[]}' | Set-Content $p
        { Read-StandConfig -Path $p } | Should -Throw '*Invalid parameter file*'
    }
}

Describe 'Get-StandVmDiff' {
    BeforeAll {
        $script:Want = [pscustomobject]@{ Name = 'w'; Vcpu = 6; RamBytes = [long]14GB; DiskSizeGB = @(20, 50) }
    }
    It 'is empty for a matching VM (idempotent)' {
        $a = Get-TestActual -Vcpu 6 -RamBytes 14GB -DiskSizeGB @(20, 50)
        @(Get-StandVmDiff -Desired $script:Want -Actual $a).Count | Should -Be 0
    }
    It 'reports fixable vCPU and RAM differences' {
        $a = Get-TestActual -Vcpu 3 -RamBytes 7GB -DiskSizeGB @(20, 50)
        $d = @(Get-StandVmDiff -Desired $script:Want -Actual $a)
        $d.Property | Should -Be @('Vcpu', 'RamBytes')
        $d.Fixable | Should -Not -Contain $false
    }
    It 'reports a missing data disk as fixable' {
        $a = Get-TestActual -Vcpu 6 -RamBytes 14GB -DiskSizeGB @(20)
        $d = @(Get-StandVmDiff -Desired $script:Want -Actual $a)
        $d.Count | Should -Be 1
        $d[0].Property | Should -Be 'Disk[1]'
        $d[0].Fixable | Should -BeTrue
    }
    It 'reports a size mismatch and generation 1 as not fixable' {
        $a = Get-TestActual -Vcpu 6 -RamBytes 14GB -DiskSizeGB @(20, 40) -Generation 1
        $d = @(Get-StandVmDiff -Desired $script:Want -Actual $a)
        $d.Property | Should -Be @('Generation', 'Disk[1]')
        $d.Fixable | Should -Not -Contain $true
    }
    It 'reports dynamic memory, Secure Boot and missing autostart' {
        $a = Get-TestActual -Vcpu 6 -RamBytes 14GB -DiskSizeGB @(20, 50) -DynamicMemory $true -SecureBoot $true -AutoStart $false
        (@(Get-StandVmDiff -Desired $script:Want -Actual $a)).Property | Should -Be @('DynamicMemory', 'SecureBoot', 'AutoStart')
    }
}
