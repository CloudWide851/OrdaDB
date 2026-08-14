[CmdletBinding()]
param(
    [ValidateSet("Validate", "Preflight", "Run")]
    [string]$Mode = "Validate",

    [ValidateSet("All", "Psql", "PgJdbc", "DataGrip", "Hibernate")]
    [string]$Client = "All",

    [ValidateRange(5, 120)]
    [UInt32]$TimeoutSeconds = 60,

    [string]$EvidencePath
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$maximumFixtureBytes = 1MB
$maximumEvidenceBytes = 256KB
$maximumDiagnosticBytes = 4096
$maximumCertificateBytes = 1MB
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$fixtureRoot = [IO.Path]::GetFullPath(
    (Join-Path $repoRoot "crates\ordadb-server\tests\client_compat")
)
$matrixPath = Join-Path $fixtureRoot "capability_matrix.v1.json"
$corpusPath = Join-Path $fixtureRoot "sql_corpus.v1.json"
$pgJdbcSource = Join-Path $fixtureRoot "PgJdbcCompat.java"
$hibernateSource = Join-Path $fixtureRoot "HibernateCompat.java"
$evidenceRoot = [IO.Path]::GetFullPath(
    (Join-Path $repoRoot "target\client-compat\evidence")
)
$workRoot = [IO.Path]::GetFullPath(
    (Join-Path $repoRoot "target\client-compat\work")
)
$runId = [guid]::NewGuid().ToString("N")
$startedAt = [DateTimeOffset]::UtcNow
$runtimePaths = @{}

. (Join-Path $PSScriptRoot "test_pg18_clients\safety-and-process.ps1")
. (Join-Path $PSScriptRoot "test_pg18_clients\fixtures-and-inputs.ps1")
. (Join-Path $PSScriptRoot "test_pg18_clients\preflight.ps1")
. (Join-Path $PSScriptRoot "test_pg18_clients\client-runs.ps1")

if ([string]::IsNullOrWhiteSpace($EvidencePath)) {
    $fileName = $startedAt.UtcDateTime.ToString("yyyyMMddTHHmmssfffZ") + "-" + $runId + ".json"
    $resolvedEvidencePath = Join-Path $evidenceRoot $fileName
} else {
    $resolvedEvidencePath = if ([IO.Path]::IsPathRooted($EvidencePath)) {
        [IO.Path]::GetFullPath($EvidencePath)
    } else {
        [IO.Path]::GetFullPath((Join-Path $repoRoot $EvidencePath))
    }
}
if (-not (Test-ContainedPath -Root $evidenceRoot -Candidate $resolvedEvidencePath)) {
    throw "EvidencePath must remain under target\client-compat\evidence."
}

$selectedClients = if ($Client -eq "All") {
    @("psql", "pgjdbc", "datagrip", "hibernate")
} else {
    @($Client.ToLowerInvariant())
}
$evidence = [ordered]@{
    schemaVersion = 1
    suiteId = "ordadb-postgresql-18-windows-x64-clients"
    runId = $runId
    repositoryCommit = Get-RepositoryCommit
    mode = $Mode.ToLowerInvariant()
    selectedClients = $selectedClients
    startedAtUtc = $startedAt.ToString("o")
    finishedAtUtc = $null
    status = "running"
    platform = [ordered]@{
        operatingSystem = "windows"
        architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
        rustTarget = "x86_64-pc-windows-msvc"
    }
    limits = [ordered]@{
        processTimeoutSeconds = [int]$TimeoutSeconds
        evidenceBytes = $maximumEvidenceBytes
        diagnosticBytes = $maximumDiagnosticBytes
        certificateBytes = $maximumCertificateBytes
    }
    fixtureValidation = $null
    connectionInputs = [ordered]@{ status = "not_checked" }
    prerequisites = @()
    clientResults = @()
    diagnostic = $null
}
$exitCode = 0
$runDirectory = $null
try {
    $runningOnWindows = [Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [Runtime.InteropServices.OSPlatform]::Windows
    )
    if (-not $runningOnWindows -or
        [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne
            [Runtime.InteropServices.Architecture]::X64) {
        throw "The PostgreSQL 18 client matrix requires Windows AMD64."
    }

    $fixtures = Read-AndValidateFixtures
    $evidence.fixtureValidation = $fixtures.summary
    if ($Mode -eq "Validate") {
        $evidence.connectionInputs = [ordered]@{
            status = "not_run"
            reason = "Validate mode does not read connection inputs or launch clients."
        }
        $evidence.clientResults = @([ordered]@{
            status = "not_run"
            reason = "Static validation completed; no client case was executed."
        })
        $evidence.status = "static_validation_passed"
    } else {
        try {
            $connectionInputs = Get-ConnectionInputs
            $evidence.connectionInputs = $connectionInputs.evidence
        } catch {
            $evidence.connectionInputs = [ordered]@{
                status = "failed"
                diagnostic = Protect-Diagnostic -Value $_.Exception.Message
            }
            throw
        }

        $preflight = Invoke-SelectedPreflight `
            -Matrix $fixtures.matrix `
            -SelectedClients $selectedClients
        $evidence.prerequisites = $preflight.results
        if ($preflight.failedClients.Count -gt 0) {
            throw "Client preflight failed for: $($preflight.failedClients -join ', ')."
        }
        if ($Mode -eq "Preflight") {
            $evidence.clientResults = @([ordered]@{
                status = "not_run"
                reason = "Preflight validated inputs and tools without connecting to a server."
            })
            $evidence.status = "preflight_passed_clients_not_run"
        } else {
            $runDirectory = [IO.Path]::GetFullPath((Join-Path $workRoot $runId))
            if (-not (Test-ContainedPath -Root $workRoot -Candidate (Join-Path $runDirectory "work.probe"))) {
                throw "The generated client work directory escaped its containment root."
            }
            Assert-NoReparseDirectory -Root $workRoot -Directory $runDirectory
            New-Item -ItemType Directory -Path $runDirectory -Force | Out-Null
            Assert-NoReparseDirectory -Root $workRoot -Directory $runDirectory
            $evidence.clientResults = Invoke-ClientRuns `
                -Corpus $fixtures.corpus `
                -SelectedClients $selectedClients `
                -ConnectionEnvironment $connectionInputs.environment `
                -RunDirectory $runDirectory
            $failedResults = @($evidence.clientResults | Where-Object { $_.status -ne "passed" })
            if ($failedResults.Count -gt 0) {
                throw "One or more selected client cases failed or were explicitly not run."
            }
            $evidence.status = "passed"
        }
    }
} catch {
    $exitCode = if ($Mode -eq "Run") { 3 } else { 2 }
    if ($evidence.status -eq "running") {
        $evidence.status = switch ($Mode) {
            "Validate" { "static_validation_failed" }
            "Preflight" { "preflight_failed" }
            "Run" { "client_run_failed" }
        }
    }
    $evidence.diagnostic = Protect-Diagnostic -Value $_.Exception.Message
} finally {
    if ($null -ne $runDirectory -and (Test-Path -LiteralPath $runDirectory -PathType Container)) {
        $resolvedRunDirectory = (Resolve-Path -LiteralPath $runDirectory).Path
        if (-not (Test-ContainedPath -Root $workRoot -Candidate (Join-Path $resolvedRunDirectory "cleanup.probe"))) {
            throw "Refusing to clean a client work directory outside its containment root."
        }
        $runItem = Get-Item -LiteralPath $resolvedRunDirectory -Force
        if (($runItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Refusing to recursively clean a reparse-point client work directory."
        }
        Remove-Item -LiteralPath $resolvedRunDirectory -Recurse -Force
    }
    $evidence.finishedAtUtc = [DateTimeOffset]::UtcNow.ToString("o")
    Write-AtomicJson -Value $evidence -Path $resolvedEvidencePath
}

Write-Output "PostgreSQL 18 client evidence: $resolvedEvidencePath"
exit $exitCode
