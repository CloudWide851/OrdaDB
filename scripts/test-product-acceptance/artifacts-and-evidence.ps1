function Get-RepositoryState {
    $commit = (& git -C $repoRoot rev-parse HEAD 2>$null)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($commit)) {
        throw "Unable to read the repository commit."
    }
    $status = @(& git -C $repoRoot status --porcelain --untracked-files=no 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to read the repository status."
    }
    return [ordered]@{
        commit = $commit.Trim()
        workingTreeDirty = $status.Count -gt 0
    }
}

function Get-ProductInputs {
    param([Parameter(Mandatory = $true)][object]$Manifest)

    $groups = @()
    foreach ($group in @($Manifest.prerequisiteGroups)) {
        $missing = @()
        $presentCount = 0
        foreach ($name in @($group.requiredInputNames)) {
            if ([string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($name))) {
                $missing += $name
            } else {
                $presentCount += 1
            }
        }
        $groups += [ordered]@{
            id = $group.id
            requiredCount = @($group.requiredInputNames).Count
            presentCount = $presentCount
            missingInputNames = $missing
            ready = $missing.Count -eq 0
        }
    }
    return $groups
}

function Get-FullScaleResources {
    $dataRoot = [IO.Path]::GetFullPath(
        (Join-Path $targetRoot "final-scale\runs\preflight-probe\data")
    )
    $driveRoot = [IO.Path]::GetPathRoot($dataRoot)
    $drive = [IO.DriveInfo]::new($driveRoot)
    $availableMemory = $null
    $memoryDiagnostic = $null
    try {
        $counter = Get-Counter '\Memory\Available Bytes'
        $availableMemory = [UInt64]$counter.CounterSamples[0].CookedValue
    } catch {
        $memoryDiagnostic = Protect-Diagnostic -Value $_.Exception.Message
    }
    $availableDisk = [UInt64]$drive.AvailableFreeSpace
    $memoryReady = $null -ne $availableMemory -and
        $availableMemory -ge $fullScaleRequiredMemoryBytes
    return [ordered]@{
        targetBytes = $fullScaleTargetBytes
        rows = $fullScaleRows
        connections = $fullScaleConnections
        requiredFreeDiskBytes = $fullScaleRequiredDiskBytes
        availableFreeDiskBytes = $availableDisk
        diskReady = $availableDisk -ge $fullScaleRequiredDiskBytes
        requiredFreeMemoryBytes = $fullScaleRequiredMemoryBytes
        availableFreeMemoryBytes = $availableMemory
        memoryReady = $memoryReady
        memoryDiagnostic = $memoryDiagnostic
        ready = ($availableDisk -ge $fullScaleRequiredDiskBytes) -and $memoryReady
        dataCreated = Test-Path -LiteralPath $dataRoot
    }
}

function Get-PeMachine {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "PE file does not exist."
    }
    $stream = [IO.File]::Open(
        $Path,
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        [IO.FileShare]::Read
    )
    try {
        if ($stream.Length -lt 64) {
            throw "PE file is truncated."
        }
        $reader = [IO.BinaryReader]::new($stream)
        if ($reader.ReadUInt16() -ne 0x5A4D) {
            throw "PE file has invalid DOS magic."
        }
        $stream.Position = 0x3C
        $peOffset = $reader.ReadUInt32()
        if ($peOffset -gt ($stream.Length - 6)) {
            throw "PE header offset is invalid."
        }
        $stream.Position = $peOffset
        if ($reader.ReadUInt32() -ne 0x00004550) {
            throw "PE signature is invalid."
        }
        return $reader.ReadUInt16()
    } finally {
        $stream.Dispose()
    }
}

function Assert-Amd64Pe {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Context
    )

    if ((Get-PeMachine -Path $Path) -ne $amd64PeMachine) {
        throw "$Context is not AMD64 PE."
    }
}

function Assert-ArtifactTree {
    param(
        [Parameter(Mandatory = $true)][object]$Manifest,
        [Parameter(Mandatory = $true)][string]$ResolvedProductRoot,
        [Parameter(Mandatory = $true)][string]$ResolvedBundleRoot
    )

    foreach ($root in @($ResolvedProductRoot, $ResolvedBundleRoot)) {
        if (-not (Test-ContainedPath -Root $targetRoot -Candidate $root)) {
            throw "Artifact roots must remain below repository target."
        }
        if (-not (Test-Path -LiteralPath $root -PathType Container)) {
            throw "Artifact root does not exist."
        }
        Assert-NoReparseDirectory -Root $targetRoot -Directory $root
    }

    $topLevelExecutables = @(
        Get-ChildItem -LiteralPath $ResolvedProductRoot -File -Filter "*.exe" |
            Select-Object -ExpandProperty Name
    )
    Assert-ExactStringSet -Actual $topLevelExecutables `
        -Expected $expectedTopLevelExecutables -Context "unpacked top-level executables" `
        -Comparison OrdinalIgnoreCase
    foreach ($name in $expectedTopLevelExecutables) {
        Assert-Amd64Pe -Path (Join-Path $ResolvedProductRoot $name) `
            -Context "top-level executable '$name'"
    }

    $topLevelLaunchers = @(
        Get-ChildItem -LiteralPath $ResolvedProductRoot -File -Filter "*.cmd" |
            Select-Object -ExpandProperty Name
    )
    Assert-ExactStringSet -Actual $topLevelLaunchers `
        -Expected $expectedTopLevelLaunchers -Context "unpacked top-level launchers" `
        -Comparison OrdinalIgnoreCase
    $launcher = Get-Content -Raw -LiteralPath (
        Join-Path $ResolvedProductRoot "ordadb-cli.cmd"
    )
    if ($launcher -notmatch '(?i)%~dp0ordadb-cli\.exe') {
        throw "Compatibility launcher does not resolve ordadb-cli.exe beside itself."
    }

    $connectorRoot = [IO.Path]::GetFullPath(
        (Join-Path $ResolvedProductRoot $Manifest.artifacts.connectorDirectory)
    )
    if (-not (Test-ContainedPath -Root $ResolvedProductRoot -Candidate $connectorRoot) -or
        -not (Test-Path -LiteralPath $connectorRoot -PathType Container)) {
        throw "Connector artifact directory is missing or escaped the product root."
    }
    Assert-NoReparseDirectory -Root $ResolvedProductRoot -Directory $connectorRoot
    $connectorExecutables = @(
        Get-ChildItem -LiteralPath $connectorRoot -File -Filter "*.exe" |
            Select-Object -ExpandProperty Name
    )
    Assert-ExactStringSet -Actual $connectorExecutables `
        -Expected $expectedConnectorExecutables -Context "connector helper inventory" `
        -Comparison OrdinalIgnoreCase
    foreach ($name in $expectedConnectorExecutables) {
        Assert-Amd64Pe -Path (Join-Path $connectorRoot $name) `
            -Context "connector helper '$name'"
    }

    $catalogPath = Join-Path $connectorRoot $Manifest.artifacts.connectorCatalog
    $catalog = Read-BoundedJson -Path $catalogPath -Context "connector catalog"
    $plugins = @($catalog.plugins)
    if ($catalog.schemaVersion -ne 1 -or $plugins.Count -ne 9) {
        throw "Connector catalog must contain exactly nine v1 plugin manifests."
    }
    $catalogEntries = @($plugins | Select-Object -ExpandProperty entry)
    Assert-ExactStringSet -Actual $catalogEntries `
        -Expected $expectedConnectorExecutables -Context "connector catalog entries" `
        -Comparison OrdinalIgnoreCase
    foreach ($plugin in $plugins) {
        if ($plugin.architecture -ne "windowsX64" -or
            $plugin.signature -isnot [string] -or
            [string]::IsNullOrWhiteSpace($plugin.signature)) {
            throw "Connector catalog plugin '$($plugin.id)' lacks Windows x64 signature metadata."
        }
        $helperPath = Join-Path $connectorRoot $plugin.entry
        $helper = Get-Item -LiteralPath $helperPath
        if ([UInt64]$plugin.size -ne [UInt64]$helper.Length) {
            throw "Connector '$($plugin.id)' size does not match its catalog."
        }
        $hash = (Get-FileHash -LiteralPath $helperPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if (-not [string]::Equals(
            $hash,
            [string]$plugin.sha256,
            [StringComparison]::Ordinal
        )) {
            throw "Connector '$($plugin.id)' SHA-256 does not match its catalog."
        }
    }

    $installers = @(
        Get-ChildItem -LiteralPath $ResolvedBundleRoot -Recurse -File |
            Where-Object { $_.Name -like $Manifest.artifacts.installerPattern }
    )
    if ($installers.Count -ne 1) {
        throw "Bundle tree must contain exactly one Windows x64 NSIS installer."
    }
    $foreignBundles = @(
        Get-ChildItem -LiteralPath $ResolvedBundleRoot -Recurse -File |
            Where-Object {
                $_.Extension.ToLowerInvariant() -in $expectedForbiddenBundleExtensions
            }
    )
    if ($foreignBundles.Count -ne 0) {
        throw "Bundle tree contains unsupported platform/package artifacts."
    }

    return [ordered]@{
        topLevelExecutables = $topLevelExecutables.Count
        launchers = $topLevelLaunchers.Count
        connectorExecutables = $connectorExecutables.Count
        connectorCatalogs = 1
        amd64PeFiles = $topLevelExecutables.Count + $connectorExecutables.Count
        nsisInstallers = $installers.Count
        forbiddenBundles = $foreignBundles.Count
    }
}

function Write-AtomicJson {
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $resolvedPath = [IO.Path]::GetFullPath($Path)
    if (-not (Test-ContainedPath -Root $evidenceRoot -Candidate $resolvedPath)) {
        throw "Evidence output must remain below target/product-acceptance/evidence."
    }
    if ([IO.Path]::GetExtension($resolvedPath) -ne ".json") {
        throw "Evidence output must use a .json extension."
    }
    if (Test-Path -LiteralPath $resolvedPath) {
        throw "Acceptance evidence is create-only and already exists."
    }
    $directory = [IO.Path]::GetDirectoryName($resolvedPath)
    Assert-NoReparseDirectory -Root $evidenceRoot -Directory $directory
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    Assert-NoReparseDirectory -Root $evidenceRoot -Directory $directory

    $json = $Value | ConvertTo-Json -Depth 24
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($json)
    if ($bytes.Length -gt $maximumEvidenceBytes) {
        throw "Acceptance evidence exceeded its 512 KiB bound."
    }
    $temporaryPath = Join-Path $directory (
        [IO.Path]::GetFileName($resolvedPath) + "." +
        [guid]::NewGuid().ToString("N") + ".tmp"
    )
    if (-not (Test-ContainedPath -Root $directory -Candidate $temporaryPath)) {
        throw "Evidence temporary sibling escaped its output directory."
    }
    $stream = $null
    try {
        $stream = [IO.FileStream]::new(
            $temporaryPath,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None,
            4096,
            [IO.FileOptions]::WriteThrough
        )
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
        $stream.Dispose()
        $stream = $null
        [IO.File]::Move($temporaryPath, $resolvedPath)
    } finally {
        if ($null -ne $stream) {
            $stream.Dispose()
        }
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
}
