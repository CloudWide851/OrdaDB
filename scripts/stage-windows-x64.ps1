$ErrorActionPreference = "Stop"

$target = "x86_64-pc-windows-msvc"
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$packageTargetRoot = Join-Path $repositoryRoot "target\package-windows-x64"
$targetDirectory = Join-Path $packageTargetRoot "$target\release"
$stagingDirectory = Join-Path $repositoryRoot "apps\desktop\src-tauri\staging\windows-x64"

function Assert-Amd64Pe {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $stream = [System.IO.File]::OpenRead($Path)
    try {
        $reader = [System.IO.BinaryReader]::new($stream)
        if ($reader.ReadUInt16() -ne 0x5A4D) {
            throw "$Path is not a PE executable"
        }
        $stream.Position = 0x3C
        $peOffset = $reader.ReadUInt32()
        if ($peOffset -gt ($stream.Length - 6)) {
            throw "$Path has an invalid PE header offset"
        }
        $stream.Position = $peOffset
        if ($reader.ReadUInt32() -ne 0x00004550) {
            throw "$Path has an invalid PE signature"
        }
        if ($reader.ReadUInt16() -ne 0x8664) {
            throw "$Path is not an AMD64 executable"
        }
    }
    finally {
        $stream.Dispose()
    }
}

cargo build --locked --release --target $target --target-dir $packageTargetRoot `
    --package ordadb-server `
    --package ordadb-cli
if ($LASTEXITCODE -ne 0) {
    throw "Windows x64 server and CLI release build failed"
}

cargo build --locked --release --target $target --target-dir $packageTargetRoot `
    --package ordadb-connector-postgresql `
    --package ordadb-connector-mysql `
    --package ordadb-connector-sqlite `
    --package ordadb-connector-sql-server `
    --package ordadb-connector-mongodb `
    --package ordadb-connector-redis `
    --package ordadb-connector-mariadb `
    --package ordadb-connector-clickhouse `
    --package ordadb-connector-oracle `
    --package ordadb-connector-publisher
if ($LASTEXITCODE -ne 0) {
    throw "Windows x64 connector helper release build failed"
}

New-Item -ItemType Directory -Path $stagingDirectory -Force | Out-Null
$binaries = @("ordadb-server.exe", "ordadb.exe")
foreach ($binary in $binaries) {
    $source = Join-Path $targetDirectory $binary
    $destination = Join-Path $stagingDirectory $binary
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Expected release executable was not produced: $source"
    }
    Assert-Amd64Pe -Path $source
    Copy-Item -LiteralPath $source -Destination $destination -Force
    Assert-Amd64Pe -Path $destination
}
$launcherSource = Join-Path $repositoryRoot "scripts\ordadb-cli.cmd"
$launcherDestination = Join-Path $stagingDirectory "ordadb-cli.cmd"
if (-not (Test-Path -LiteralPath $launcherSource -PathType Leaf)) {
    throw "Expected CLI compatibility launcher is missing: $launcherSource"
}
Copy-Item -LiteralPath $launcherSource -Destination $launcherDestination -Force
$launcher = Get-Content -LiteralPath $launcherDestination -Raw
if ($launcher -notmatch '(?i)%~dp0ordadb\.exe') {
    throw "CLI compatibility launcher must resolve ordadb.exe beside itself"
}

$metadata = cargo metadata --locked --no-deps --format-version 1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) {
    throw "Failed to read the connector bundle version"
}
$connectorVersion = ($metadata.packages |
    Where-Object { $_.name -eq "ordadb-connector-publisher" } |
    Select-Object -First 1).version
if (-not $connectorVersion) {
    throw "Connector publisher package version is missing"
}

$connectorDirectory = Join-Path $stagingDirectory "connectors\v1"
$publisher = Join-Path $targetDirectory "ordadb-connector-publisher.exe"
& $publisher sign-bundle `
    --artifacts $targetDirectory `
    --bundle-output $connectorDirectory `
    --public-key (Join-Path $repositoryRoot "connectors\trust\registry-ed25519-v1.pub") `
    --version $connectorVersion `
    --base-url "https://cloudwide851.github.io/OrdaDB/connectors/v1/"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to sign the Windows x64 connector bundle"
}

$connectorBinaries = @(
    "ordadb-connector-postgresql.exe",
    "ordadb-connector-mysql.exe",
    "ordadb-connector-sqlite.exe",
    "ordadb-connector-sql-server.exe",
    "ordadb-connector-mongodb.exe",
    "ordadb-connector-redis.exe",
    "ordadb-connector-mariadb.exe",
    "ordadb-connector-clickhouse.exe",
    "ordadb-connector-oracle.exe"
)
foreach ($binary in $connectorBinaries) {
    Assert-Amd64Pe -Path (Join-Path $connectorDirectory $binary)
}
$expectedConnectorFiles = @($connectorBinaries) + @("catalog-v1.json")
$unexpectedConnectors = Get-ChildItem -LiteralPath $connectorDirectory -File |
    Where-Object { $_.Name -notin $expectedConnectorFiles }
if ($unexpectedConnectors) {
    throw "Connector staging contains unexpected files: $($unexpectedConnectors.Name -join ', ')"
}
if ((Get-ChildItem -LiteralPath $connectorDirectory -File).Count -ne $expectedConnectorFiles.Count) {
    throw "Connector staging does not contain exactly nine helpers and one catalog"
}

$unexpected = Get-ChildItem -LiteralPath $stagingDirectory -File |
    Where-Object { $_.Name -notin (@($binaries) + @("ordadb-cli.cmd")) }
if ($unexpected) {
    throw "Windows staging contains unexpected files: $($unexpected.Name -join ', ')"
}
$unexpectedDirectories = Get-ChildItem -LiteralPath $stagingDirectory -Directory |
    Where-Object { $_.Name -ne "connectors" }
if ($unexpectedDirectories) {
    throw "Windows staging contains unexpected directories: $($unexpectedDirectories.Name -join ', ')"
}

Write-Output "Staged AMD64 product binaries, CLI launcher, and nine signed connector resources in $stagingDirectory"
