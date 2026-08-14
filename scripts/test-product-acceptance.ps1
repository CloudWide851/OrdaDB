[CmdletBinding()]
param(
    [ValidateSet("Validate", "Preflight", "Artifacts")]
    [string]$Mode = "Validate",

    [string]$ManifestPath,

    [string]$ProductRoot,

    [string]$BundleRoot,

    [string]$EvidencePath,

    [switch]$SelfTest
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$maximumInputBytes = 2MB
$maximumEvidenceBytes = 512KB
$maximumDiagnosticBytes = 4096
$maximumSuites = 64
$maximumCapabilities = 256
$maximumEvidencePathsPerItem = 16
$maximumPrerequisiteNames = 128
$maximumStringBytes = 4096
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$acceptanceRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot "acceptance"))
$targetRoot = [IO.Path]::GetFullPath((Join-Path $repoRoot "target"))
$evidenceRoot = [IO.Path]::GetFullPath(
    (Join-Path $targetRoot "product-acceptance\evidence")
)
$runId = [guid]::NewGuid().ToString("N")
$startedAt = [DateTimeOffset]::UtcNow

$allowedStatuses = @(
    "passed",
    "regressionOnly",
    "unsupported",
    "notRunMissingInputs",
    "notRunManual",
    "resourceBlocked",
    "notApplicable"
)
$expectedTopLevelExecutables = @(
    "OrdaDB.exe",
    "ordadb-server.exe",
    "ordadb-cli.exe"
)
$expectedTopLevelLaunchers = @("ordadb-cli.cmd")
$expectedConnectorExecutables = @(
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
$expectedForbiddenBundleExtensions = @(
    ".msi",
    ".msix",
    ".appx",
    ".appxbundle",
    ".dmg",
    ".deb",
    ".rpm",
    ".appimage"
)
$amd64PeMachine = 0x8664
$fullScaleTargetBytes = [UInt64]21474836480
$fullScaleRows = [UInt64]10000000
$fullScaleConnections = [UInt32]32
$fullScaleRequiredDiskBytes = [UInt64]64424509440
$fullScaleRequiredMemoryBytes = [UInt64]85899345920

. (Join-Path $PSScriptRoot "test-product-acceptance\validation.ps1")
. (Join-Path $PSScriptRoot "test-product-acceptance\manifests.ps1")
. (Join-Path $PSScriptRoot "test-product-acceptance\artifacts-and-evidence.ps1")
. (Join-Path $PSScriptRoot "test-product-acceptance\self-tests.ps1")

if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
    $resolvedManifestPath = Join-Path $acceptanceRoot "product-acceptance.v1.json"
} else {
    $resolvedManifestPath = if ([IO.Path]::IsPathRooted($ManifestPath)) {
        [IO.Path]::GetFullPath($ManifestPath)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $ManifestPath))
    }
}
if (-not (Test-ContainedPath -Root $acceptanceRoot -Candidate $resolvedManifestPath)) {
    throw "ManifestPath must remain below acceptance."
}

if ([string]::IsNullOrWhiteSpace($EvidencePath)) {
    $fileName = "{0}-{1}-{2}.json" -f (
        $startedAt.ToString("yyyyMMddTHHmmssfffZ"),
        $Mode.ToLowerInvariant(),
        $runId
    )
    $resolvedEvidencePath = Join-Path $evidenceRoot $fileName
} else {
    $resolvedEvidencePath = if ([IO.Path]::IsPathRooted($EvidencePath)) {
        [IO.Path]::GetFullPath($EvidencePath)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $EvidencePath))
    }
}
if (-not (Test-ContainedPath -Root $evidenceRoot -Candidate $resolvedEvidencePath)) {
    throw "EvidencePath must remain below target/product-acceptance/evidence."
}

$evidence = [ordered]@{
    schemaVersion = 1
    acceptanceId = "ordadb-postgresql18-windows-x64-product"
    runId = $runId
    mode = $Mode
    startedAtUtc = $startedAt.ToString("o")
    finishedAtUtc = $null
    target = [ordered]@{
        operatingSystem = "Windows"
        architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        rustTarget = "x86_64-pc-windows-msvc"
    }
    repository = $null
    status = "running"
    checks = @()
    prerequisites = @()
    fullScale = $null
    artifacts = $null
    diagnostic = $null
}
$exitCode = 0
$secretNames = @()
try {
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT -or
        [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne
            [Runtime.InteropServices.Architecture]::X64) {
        throw "Product acceptance requires Windows x64."
    }
    $manifest = Read-BoundedJson -Path $resolvedManifestPath -Context "product manifest"
    Assert-ProductManifest -Manifest $manifest
    $conformancePath = Resolve-RepositoryPath `
        -Value $manifest.sources.conformance -Context "conformance source"
    $performancePath = Resolve-RepositoryPath `
        -Value $manifest.sources.performance -Context "performance source"
    $clientMatrixPath = Resolve-RepositoryPath `
        -Value $manifest.sources.clientMatrix -Context "client matrix source"
    $clientCorpusPath = Resolve-RepositoryPath `
        -Value $manifest.sources.clientCorpus -Context "client corpus source"
    $conformance = Read-BoundedJson -Path $conformancePath -Context "conformance matrix"
    $performance = Read-BoundedJson -Path $performancePath -Context "performance evidence"
    $clientMatrix = Read-BoundedJson -Path $clientMatrixPath -Context "client matrix"
    $clientCorpus = Read-BoundedJson -Path $clientCorpusPath -Context "client corpus"
    Assert-ConformanceMatrix -Matrix $conformance
    Assert-PerformanceEvidence -Performance $performance
    Assert-ClientFixtures -Matrix $clientMatrix -Corpus $clientCorpus
    $evidence.repository = Get-RepositoryState
    $evidence.checks += [ordered]@{
        id = "trackedMatrices"
        status = "passed"
        suites = @($manifest.suites).Count
        capabilities = @($conformance.capabilities).Count
        performanceObservations = @($performance.observations).Count
        clientCases = @($clientMatrix.cases).Count
    }

    if ($SelfTest) {
        if ($Mode -ne "Validate") {
            throw "SelfTest is available only in Validate mode."
        }
        $selfTestCount = Invoke-SelfTests -Manifest $manifest `
            -Conformance $conformance -Performance $performance
        $evidence.checks += [ordered]@{
            id = "negativeSelfTests"
            status = "passed"
            tests = $selfTestCount
        }
    }

    switch ($Mode) {
        "Validate" {
            $evidence.status = "passed"
        }
        "Preflight" {
            $secretNames = @(
                $manifest.prerequisiteGroups |
                    ForEach-Object { @($_.requiredInputNames) }
            )
            $evidence.prerequisites = @(Get-ProductInputs -Manifest $manifest)
            $evidence.fullScale = Get-FullScaleResources
            if ($evidence.fullScale.dataCreated) {
                throw "Full-scale preflight unexpectedly found a generated probe data path."
            }
            $blockedGroups = @($evidence.prerequisites | Where-Object { -not $_.ready })
            if ($blockedGroups.Count -gt 0 -or -not $evidence.fullScale.ready) {
                $evidence.status = "blocked"
                $exitCode = 2
            } else {
                $evidence.status = "readyNotExecuted"
            }
        }
        "Artifacts" {
            if ([string]::IsNullOrWhiteSpace($ProductRoot) -or
                [string]::IsNullOrWhiteSpace($BundleRoot)) {
                throw "Artifacts mode requires ProductRoot and BundleRoot."
            }
            $resolvedProductRoot = if ([IO.Path]::IsPathRooted($ProductRoot)) {
                [IO.Path]::GetFullPath($ProductRoot)
            } else {
                [IO.Path]::GetFullPath((Join-Path $repoRoot $ProductRoot))
            }
            $resolvedBundleRoot = if ([IO.Path]::IsPathRooted($BundleRoot)) {
                [IO.Path]::GetFullPath($BundleRoot)
            } else {
                [IO.Path]::GetFullPath((Join-Path $repoRoot $BundleRoot))
            }
            $evidence.artifacts = Assert-ArtifactTree -Manifest $manifest `
                -ResolvedProductRoot $resolvedProductRoot `
                -ResolvedBundleRoot $resolvedBundleRoot
            $evidence.status = "passed"
        }
    }
} catch {
    $evidence.status = "failed"
    $evidence.diagnostic = Protect-Diagnostic `
        -Value $_.Exception.Message -SecretNames $secretNames
    $exitCode = 1
} finally {
    $evidence.finishedAtUtc = [DateTimeOffset]::UtcNow.ToString("o")
    Write-AtomicJson -Value $evidence -Path $resolvedEvidencePath
}

Write-Output "Product acceptance evidence: $resolvedEvidencePath"
if ($exitCode -ne 0) {
    exit $exitCode
}
