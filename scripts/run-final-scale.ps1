[CmdletBinding()]
param(
    [ValidateSet("Smoke", "Full")]
    [string]$Profile = "Smoke",
    [string]$DataRoot,
    [string]$OutputPath,
    [UInt64]$Rows = 0,
    [UInt64]$TargetBytes = 0,
    [UInt32]$Connections = 0,
    [switch]$ConfirmFullScale,
    [switch]$RetainData
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$timestamp = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$generatedRunRoot = $null

if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    $generatedRunRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $repoRoot "target\final-scale\runs\$timestamp")
    )
    $DataRoot = Join-Path $generatedRunRoot "data"
} else {
    $DataRoot = [System.IO.Path]::GetFullPath($DataRoot)
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = [System.IO.Path]::GetFullPath(
        (Join-Path $repoRoot "target\final-scale\evidence\$timestamp.json")
    )
} else {
    $OutputPath = [System.IO.Path]::GetFullPath($OutputPath)
}

$profileArgument = $Profile.ToLowerInvariant()
$cargoArguments = @(
    "run",
    "--locked",
    "--release",
    "--target",
    "x86_64-pc-windows-msvc",
    "-p",
    "ordadb-engine",
    "--example",
    "final_scale",
    "--",
    "--profile",
    $profileArgument,
    "--data-dir",
    $DataRoot,
    "--output",
    $OutputPath
)

if ($Rows -gt 0) {
    $cargoArguments += @("--rows", $Rows.ToString())
}
if ($TargetBytes -gt 0) {
    $cargoArguments += @("--target-bytes", $TargetBytes.ToString())
}
if ($Connections -gt 0) {
    $cargoArguments += @("--connections", $Connections.ToString())
}

if ($Profile -eq "Full") {
    if (-not $ConfirmFullScale) {
        throw "Full scale requires -ConfirmFullScale because it creates a 20 GiB / 10M-row database."
    }
    if ($Rows -gt 0 -or $TargetBytes -gt 0 -or $Connections -gt 0) {
        throw "The Full profile uses the fixed 20 GiB / 10M-row / 32-connection target and does not accept overrides."
    }

    $driveRoot = [System.IO.Path]::GetPathRoot($DataRoot)
    $driveName = $driveRoot.TrimEnd("\").TrimEnd(":")
    $drive = Get-PSDrive -Name $driveName
    $operatingSystem = Get-CimInstance Win32_OperatingSystem
    $freePhysicalMemory = [UInt64]$operatingSystem.FreePhysicalMemory * 1KB
    $cargoArguments += @(
        "--confirm-full-scale",
        "--available-disk-bytes",
        ([UInt64]$drive.Free).ToString(),
        "--available-memory-bytes",
        $freePhysicalMemory.ToString()
    )
}

Push-Location $repoRoot
try {
    & cargo @cargoArguments
    $cargoExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}

if ($cargoExitCode -ne 0) {
    throw "Final-scale harness failed with exit code $cargoExitCode. Evidence path: $OutputPath"
}

if ($null -ne $generatedRunRoot -and -not $RetainData) {
    $runsRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $repoRoot "target\final-scale\runs")
    ).TrimEnd("\") + "\"
    $resolvedRunRoot = [System.IO.Path]::GetFullPath($generatedRunRoot)
    if (-not $resolvedRunRoot.StartsWith(
        $runsRoot,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Refusing to clean a generated run outside $runsRoot"
    }
    if (Test-Path -LiteralPath $resolvedRunRoot) {
        Remove-Item -LiteralPath $resolvedRunRoot -Recurse -Force
    }
}

Write-Output "Final-scale evidence: $OutputPath"
