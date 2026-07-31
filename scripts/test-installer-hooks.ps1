param(
    [switch]$Compile
)

$ErrorActionPreference = "Stop"

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$hooksPath = Join-Path $repositoryRoot "apps\desktop\src-tauri\nsis\installer-hooks.nsh"
$configPath = Join-Path $repositoryRoot "apps\desktop\src-tauri\tauri.conf.json"
$hooks = Get-Content -LiteralPath $hooksPath -Raw
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$Needle,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if ($Text.IndexOf($Needle, [System.StringComparison]::Ordinal) -lt 0) {
        throw $Message
    }
}

function Assert-Before {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$First,
        [Parameter(Mandatory = $true)]
        [string]$Second,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    $firstIndex = $Text.IndexOf($First, [System.StringComparison]::Ordinal)
    $secondIndex = $Text.IndexOf($Second, [System.StringComparison]::Ordinal)
    if ($firstIndex -lt 0 -or $secondIndex -lt 0 -or $firstIndex -ge $secondIndex) {
        throw $Message
    }
}

Assert-Contains $hooks 'File "/oname=$PLUGINSDIR\ordadb-installer-cli.exe"' `
    "Installer CLI must be extracted only to the NSIS plugin directory"
Assert-Contains $hooks "installer-storage --preflight" `
    "Installer must run storage preflight"
Assert-Contains $hooks "installer-storage --apply" `
    "Installer must apply a safe receipt before service startup"
Assert-Contains $hooks 'MessageBox MB_ICONEXCLAMATION|MB_YESNO|MB_DEFBUTTON2' `
    "Interactive legacy migration must require explicit confirmation"
Assert-Contains $hooks '${AndIfNot} ${Silent}' `
    "Silent installs must bypass the interactive confirmation dialog"
Assert-Contains $hooks '$OrdaInstallerPassive != 1' `
    "Passive installs must bypass the interactive confirmation dialog"
Assert-Contains $hooks 'StrCpy $OrdaInstallerDataDir "$APPDATA\OrdaDB\data"' `
    "Installer must default to the per-machine ProgramData root"
Assert-Contains $hooks '${GetOptions} $CMDLINE "/DATA-DIR="' `
    "Installer must support an explicit non-default data directory"
Assert-Before $hooks "Call OrdaDBRunStoragePreflight" `
    '!insertmacro OrdaDBRunServiceCommand "stop"' `
    "Storage preflight must run before the previous service is stopped"
Assert-Before $hooks "Call OrdaDBConfirmLegacyPlan" `
    '!insertmacro OrdaDBRunServiceCommand "stop"' `
    "Interactive confirmation must run before the previous service is stopped"
Assert-Before $hooks "Call OrdaDBApplyStorage" `
    '!macro NSIS_HOOK_POSTINSTALL' `
    "Storage apply must run before binaries are registered and the service starts"

if ($hooks -match '(?i)RMDir\s+/r\s+.*ProgramData') {
    throw "Installer hooks must not recursively delete ProgramData"
}

$targets = @($config.bundle.targets)
if ($targets.Count -ne 1 -or $targets[0] -ne "nsis") {
    throw "Tauri bundle targets must contain only NSIS"
}

$topLevelResourceExecutables = @(
    $config.bundle.resources.PSObject.Properties |
        Where-Object {
            $_.Value -is [string] -and
            [System.IO.Path]::GetDirectoryName([string]$_.Value) -eq "" -and
            [System.IO.Path]::GetExtension([string]$_.Value) -eq ".exe"
        } |
        ForEach-Object { [string]$_.Value }
)
$expected = @("ordadb-server.exe", "ordadb-cli.exe")
if (
    $topLevelResourceExecutables.Count -ne $expected.Count -or
    @($topLevelResourceExecutables | Where-Object { $_ -notin $expected }).Count -ne 0
) {
    throw "Tauri resources must add exactly the server and CLI beside the main OrdaDB executable"
}

Write-Output "Installer hook ordering, migration safety, NSIS-only target, and three-EXE layout are valid."

if ($Compile) {
    $makeNsis = Join-Path $env:LOCALAPPDATA "tauri\NSIS\makensis.exe"
    if (-not (Test-Path -LiteralPath $makeNsis -PathType Leaf)) {
        throw "Tauri NSIS compiler is missing: $makeNsis"
    }
    $smokeSource = Join-Path $PSScriptRoot "installer-hooks-smoke.nsi"
    & $makeNsis /V2 $smokeSource
    if ($LASTEXITCODE -ne 0) {
        throw "Installer hook NSIS compile smoke failed"
    }
    $smokeInstaller = Join-Path $repositoryRoot "target\installer-hooks-smoke.exe"
    if (-not (Test-Path -LiteralPath $smokeInstaller -PathType Leaf)) {
        throw "Installer hook NSIS compile did not produce its smoke executable"
    }
    Write-Output "Installer hook NSIS compile smoke passed."
}
