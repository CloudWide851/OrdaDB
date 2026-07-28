$ErrorActionPreference = "Stop"

$target = "x86_64-pc-windows-msvc"
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$targetDirectory = Join-Path $repositoryRoot "target\$target\release"
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

cargo build --locked --release --target $target --package ordadb-server --package ordadb-cli
if ($LASTEXITCODE -ne 0) {
    throw "Windows x64 server and CLI release build failed"
}

New-Item -ItemType Directory -Path $stagingDirectory -Force | Out-Null
$binaries = @("ordadb-server.exe", "ordadb-cli.exe")
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

$unexpected = Get-ChildItem -LiteralPath $stagingDirectory -File |
    Where-Object { $_.Name -notin $binaries }
if ($unexpected) {
    throw "Windows staging contains unexpected files: $($unexpected.Name -join ', ')"
}

Write-Output "Staged AMD64 binaries in $stagingDirectory"
