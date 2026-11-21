<#
.SYNOPSIS
Local and CI checks for the PowerShell scripts in this directory.
.PARAMETER Check
Lint runs PSScriptAnalyzer, Test runs Pester. Neither needs Hyper-V.
#>
[CmdletBinding()]
param([Parameter(Mandatory)][ValidateSet('Lint', 'Test')][string]$Check)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

switch ($Check) {
    'Lint' {
        $results = @(Invoke-ScriptAnalyzer -Path $PSScriptRoot -Recurse `
                -Settings (Join-Path $PSScriptRoot 'PSScriptAnalyzerSettings.psd1'))
        $results | Format-Table RuleName, Severity, ScriptName, Line, Message -AutoSize -Wrap | Out-String | Write-Output
        if ($results.Count -gt 0) { throw "PSScriptAnalyzer: $($results.Count) finding(s)" }
        Write-Output 'PSScriptAnalyzer: clean'
    }
    'Test' {
        $cfg = New-PesterConfiguration
        $cfg.Run.Path = $PSScriptRoot
        $cfg.Run.Exit = $true
        $cfg.Output.Verbosity = 'Detailed'
        Invoke-Pester -Configuration $cfg
    }
}
