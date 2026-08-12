param(
    [switch]$Compile
)

$ErrorActionPreference = "Stop"

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$hooksPath = Join-Path $repositoryRoot "apps\desktop\src-tauri\nsis\installer-hooks.nsh"
$configPath = Join-Path $repositoryRoot "apps\desktop\src-tauri\tauri.conf.json"
$manifestPath = Join-Path $repositoryRoot "apps\desktop\src-tauri\Cargo.toml"
$hooks = Get-Content -LiteralPath $hooksPath -Raw
$config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
$manifest = Get-Content -LiteralPath $manifestPath -Raw

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
Assert-Contains $hooks 'staging\windows-x64\ordadb-cli.exe' `
    "Installer-private CLI must use the distinct staged CLI executable"
Assert-Contains $hooks "installer-storage --preflight" `
    "Installer must run storage preflight"
Assert-Contains $hooks "installer-storage --apply" `
    "Installer must apply a safe receipt before service startup"
Assert-Contains $hooks "installer-service --prepare" `
    "Installer must prepare a service transaction before the service starts"
Assert-Contains $hooks "installer-service --commit" `
    "Installer must commit service recovery only after startup succeeds"
Assert-Contains $hooks "installer-service --rollback" `
    "Installer must restore the previous service configuration on startup failure"
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
Assert-Before $hooks '!macro NSIS_HOOK_POSTINSTALL' `
    "Call OrdaDBPrepareServiceTransaction" `
    "Service transaction preparation must run in post-install"
Assert-Before $hooks "Call OrdaDBPrepareServiceTransaction" `
    'nsExec::ExecToStack ''"$INSTDIR\ordadb-server.exe" service start --data-dir' `
    "Service transaction preparation must run before service startup"
Assert-Before $hooks 'service start --data-dir' `
    'installer-service --commit' `
    "The service must reach Running before recovery actions are committed"
Assert-Before $hooks 'installer-service --rollback' `
    'Abort "OrdaDB service start failed.' `
    "Startup failure must roll back the service configuration before aborting"

if ($hooks -match '(?s)!macro NSIS_HOOK_POSTINSTALL.*OrdaDBRunServiceCommand "install"') {
    throw "Post-install must not use the non-transactional service install command"
}

if ($hooks -match '(?i)RMDir\s+/r\s+.*ProgramData') {
    throw "Installer hooks must not recursively delete ProgramData"
}

$targets = @($config.bundle.targets)
if ($targets.Count -ne 1 -or $targets[0] -ne "nsis") {
    throw "Tauri bundle targets must contain only NSIS"
}

if ($manifest -notmatch '(?ms)\[\[bin\]\]\s*name\s*=\s*"([^"]+)"') {
    throw "Desktop Cargo manifest must declare one explicit binary target"
}
$desktopBinaryName = $Matches[1]
if ($config.mainBinaryName -ne "OrdaDB") {
    throw "Tauri must rename the internal desktop Cargo target to OrdaDB.exe"
}
if (
    $desktopBinaryName -eq "ordadb" -or
    $desktopBinaryName.Replace("-", "_") -eq "ordadb_desktop" -or
    $desktopBinaryName -eq $config.mainBinaryName
) {
    throw "Desktop Cargo target must not collide with the CLI executable or desktop library"
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

$installedExecutableNames = @("$($config.mainBinaryName).exe") +
    $topLevelResourceExecutables
$caseInsensitiveNames = [Collections.Generic.HashSet[string]]::new(
    [StringComparer]::OrdinalIgnoreCase
)
foreach ($name in $installedExecutableNames) {
    if (-not $caseInsensitiveNames.Add($name)) {
        throw "Installed executable names must be unique on a case-insensitive Windows filesystem"
    }
}
if ($caseInsensitiveNames.Count -ne 3) {
    throw "Tauri must install exactly three case-insensitively unique executables"
}

$launcherResources = @(
    $config.bundle.resources.PSObject.Properties |
        Where-Object { $_.Value -eq "ordadb-cli.cmd" }
)
if ($launcherResources.Count -ne 1) {
    throw "Tauri resources must contain exactly one ordadb-cli.cmd compatibility launcher"
}

$launcherPath = Join-Path $repositoryRoot "scripts\ordadb-cli.cmd"
$launcher = Get-Content -LiteralPath $launcherPath -Raw
if ($launcher -notmatch '(?i)%~dp0ordadb-cli\.exe') {
    throw "CLI compatibility launcher must resolve the distinct ordadb-cli.exe"
}

Write-Output "Installer hook ordering, migration safety, unique desktop target, NSIS-only target, and three-EXE layout are valid."

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
