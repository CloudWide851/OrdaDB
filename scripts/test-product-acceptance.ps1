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

function Test-ContainedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [string]$Candidate
    )

    $resolvedRoot = [IO.Path]::GetFullPath($Root).TrimEnd("\", "/")
    $resolvedCandidate = [IO.Path]::GetFullPath($Candidate)
    if ([string]::Equals(
        $resolvedRoot,
        $resolvedCandidate,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        return $true
    }
    $rootPrefix = $resolvedRoot + [IO.Path]::DirectorySeparatorChar
    return $resolvedCandidate.StartsWith(
        $rootPrefix,
        [StringComparison]::OrdinalIgnoreCase
    )
}

function Assert-NoReparseDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [string]$Directory
    )

    $resolvedRoot = [IO.Path]::GetFullPath($Root)
    $resolvedDirectory = [IO.Path]::GetFullPath($Directory)
    if (-not (Test-ContainedPath -Root $resolvedRoot -Candidate $resolvedDirectory)) {
        throw "A validated directory escaped its containment root."
    }
    $rootPrefix = $resolvedRoot.TrimEnd("\", "/") + [IO.Path]::DirectorySeparatorChar
    $relative = if ([string]::Equals(
        $resolvedRoot,
        $resolvedDirectory,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        ""
    } else {
        $resolvedDirectory.Substring($rootPrefix.Length)
    }
    $current = $resolvedRoot
    if (Test-Path -LiteralPath $current) {
        $rootItem = Get-Item -LiteralPath $current -Force
        if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "The validation root must not be a reparse point."
        }
    }
    foreach ($segment in $relative.Split(
        @("\", "/"),
        [StringSplitOptions]::RemoveEmptyEntries
    )) {
        $current = Join-Path $current $segment
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Validated directories must not traverse a reparse point."
            }
        }
    }
}

function Resolve-RepositoryPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    if ([IO.Path]::IsPathRooted($Value) -or $Value.Contains("`0")) {
        throw "$Context must be a repository-relative path."
    }
    $resolved = [IO.Path]::GetFullPath((Join-Path $repoRoot $Value))
    if (-not (Test-ContainedPath -Root $repoRoot -Candidate $resolved)) {
        throw "$Context escaped the repository."
    }
    return $resolved
}

function Limit-Utf8 {
    param(
        [AllowNull()]
        [string]$Value,

        [UInt32]$MaximumBytes
    )

    if ($null -eq $Value) {
        return $null
    }
    $encoding = [Text.UTF8Encoding]::new($false)
    if ($encoding.GetByteCount($Value) -le $MaximumBytes) {
        return $Value
    }
    $low = 0
    $high = $Value.Length
    while ($low -lt $high) {
        $middle = [Math]::Ceiling(($low + $high) / 2)
        if ($encoding.GetByteCount($Value.Substring(0, $middle)) -le ($MaximumBytes - 3)) {
            $low = $middle
        } else {
            $high = $middle - 1
        }
    }
    return $Value.Substring(0, $low) + "..."
}

function Protect-Diagnostic {
    param(
        [AllowNull()]
        [string]$Value,

        [string[]]$SecretNames = @()
    )

    if ($null -eq $Value) {
        return $null
    }
    $protected = $Value
    foreach ($name in $SecretNames) {
        $secret = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrEmpty($secret)) {
            $protected = [Regex]::Replace(
                $protected,
                [Regex]::Escape($secret),
                "<redacted>",
                [Text.RegularExpressions.RegexOptions]::IgnoreCase
            )
        }
    }
    foreach ($path in @($repoRoot, $acceptanceRoot, $targetRoot)) {
        $protected = [Regex]::Replace(
            $protected,
            [Regex]::Escape($path),
            "<workspace>",
            [Text.RegularExpressions.RegexOptions]::IgnoreCase
        )
    }
    $protected = [Regex]::Replace(
        $protected,
        "(?i)(password|passwd|pwd|api[_-]?key|token)\s*[=:]\s*[^\s;]+",
        '$1=<redacted>'
    )
    $protected = [Regex]::Replace(
        $protected,
        "(?i)(postgres(?:ql)?://)[^/@\s]+@",
        '$1<redacted>@'
    )
    return Limit-Utf8 -Value $protected -MaximumBytes $maximumDiagnosticBytes
}

function Assert-BoundedString {
    param(
        [AllowNull()]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context,

        [switch]$AllowEmpty
    )

    if ($Value -isnot [string]) {
        throw "$Context must be a string."
    }
    if (-not $AllowEmpty -and [string]::IsNullOrWhiteSpace($Value)) {
        throw "$Context must not be empty."
    }
    if ([Text.Encoding]::UTF8.GetByteCount($Value) -gt $maximumStringBytes) {
        throw "$Context exceeded the string bound."
    }
}

function Assert-Identifier {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    Assert-BoundedString -Value $Value -Context $Context
    if ($Value -notmatch '^[a-z][a-z0-9-]{0,63}$') {
        throw "$Context must be a lower camel/kebab identifier."
    }
}

function Assert-AllowedProperties {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string[]]$Allowed,

        [Parameter(Mandatory = $true)]
        [string[]]$Required,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    $names = @($Value.PSObject.Properties.Name)
    foreach ($name in $names) {
        if ($name -notin $Allowed) {
            throw "$Context contains unknown field '$name'."
        }
    }
    foreach ($name in $Required) {
        if ($name -notin $names) {
            throw "$Context is missing field '$name'."
        }
    }
}

function Assert-ExactStringSet {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Actual,

        [Parameter(Mandatory = $true)]
        [string[]]$Expected,

        [Parameter(Mandatory = $true)]
        [string]$Context,

        [StringComparison]
        $Comparison = [StringComparison]::Ordinal
    )

    if ($Actual.Count -ne $Expected.Count) {
        throw "$Context must contain exactly $($Expected.Count) entries."
    }
    foreach ($expectedValue in $Expected) {
        $matched = @($Actual | Where-Object {
            $_ -is [string] -and [string]::Equals(
                $_,
                $expectedValue,
                $Comparison
            )
        })
        if ($matched.Count -ne 1) {
            throw "$Context is missing exact entry '$expectedValue'."
        }
    }
}

function Read-BoundedJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    $resolved = [IO.Path]::GetFullPath($Path)
    if (-not (Test-ContainedPath -Root $repoRoot -Candidate $resolved)) {
        throw "$Context escaped the repository."
    }
    if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
        throw "$Context does not exist."
    }
    $item = Get-Item -LiteralPath $resolved -Force
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "$Context must not be a reparse point."
    }
    if ($item.Length -le 0 -or $item.Length -gt $maximumInputBytes) {
        throw "$Context exceeded the bounded input size."
    }
    try {
        return Get-Content -Raw -LiteralPath $resolved | ConvertFrom-Json
    } catch {
        throw "$Context is not valid JSON: $($_.Exception.Message)"
    }
}

function Assert-TrackedEvidencePaths {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Values,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    $paths = @($Values)
    if ($paths.Count -eq 0 -or $paths.Count -gt $maximumEvidencePathsPerItem) {
        throw "$Context must contain 1-$maximumEvidencePathsPerItem paths."
    }
    $seen = [Collections.Generic.HashSet[string]]::new(
        [StringComparer]::OrdinalIgnoreCase
    )
    foreach ($path in $paths) {
        Assert-BoundedString -Value $path -Context "$Context entry"
        if (-not $seen.Add($path)) {
            throw "$Context contains duplicate path '$path'."
        }
        $resolved = Resolve-RepositoryPath -Value $path -Context "$Context entry"
        if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
            throw "$Context references missing file '$path'."
        }
        $item = Get-Item -LiteralPath $resolved -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Context references a reparse point."
        }
    }
}

function Assert-Status {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Value,

        [Parameter(Mandatory = $true)]
        [string]$Context
    )

    Assert-BoundedString -Value $Value -Context $Context
    if ($Value -notin $allowedStatuses) {
        throw "$Context contains unknown status '$Value'."
    }
}

function Assert-ProductManifest {
    param([Parameter(Mandatory = $true)][object]$Manifest)

    Assert-AllowedProperties -Value $Manifest `
        -Allowed @(
            "schemaVersion", "acceptanceId", "recordedAt", "baselineCommit",
            "target", "limits", "statusPolicy", "sources", "suites",
            "prerequisiteGroups", "fullScale", "artifacts"
        ) `
        -Required @(
            "schemaVersion", "acceptanceId", "target", "limits",
            "statusPolicy", "sources", "suites", "prerequisiteGroups",
            "fullScale", "artifacts"
        ) `
        -Context "product manifest"
    if ($Manifest.schemaVersion -ne 1) {
        throw "Product manifest schemaVersion must be 1."
    }
    Assert-Identifier -Value $Manifest.acceptanceId -Context "acceptanceId"
    Assert-BoundedString -Value $Manifest.recordedAt -Context "recordedAt"
    Assert-BoundedString -Value $Manifest.baselineCommit -Context "baselineCommit"
    if ($Manifest.target.operatingSystem -ne "Windows" -or
        $Manifest.target.architecture -ne "AMD64" -or
        $Manifest.target.rustTarget -ne "x86_64-pc-windows-msvc" -or
        $Manifest.target.bundle -ne "nsis") {
        throw "Product target must remain Windows AMD64 / x86_64-pc-windows-msvc / NSIS."
    }
    if ($Manifest.limits.maxSuites -gt $maximumSuites -or
        $Manifest.limits.maxCapabilities -gt $maximumCapabilities -or
        $Manifest.limits.maxEvidencePathsPerItem -gt $maximumEvidencePathsPerItem -or
        $Manifest.limits.maxPrerequisiteNames -gt $maximumPrerequisiteNames -or
        $Manifest.limits.maxStringBytes -gt $maximumStringBytes -or
        $Manifest.limits.maxInputBytes -gt $maximumInputBytes -or
        $Manifest.limits.maxEvidenceBytes -gt $maximumEvidenceBytes) {
        throw "Product manifest limits exceed validator hard bounds."
    }
    Assert-ExactStringSet -Actual @($Manifest.statusPolicy.allowed) `
        -Expected $allowedStatuses -Context "status policy"
    if ($Manifest.statusPolicy.referenceClaimRequires -ne "passed" -or
        -not $Manifest.statusPolicy.presenceIsNotExecution -or
        -not $Manifest.statusPolicy.staticValidationIsNotExecution -or
        -not $Manifest.statusPolicy.preflightIsNotExecution) {
        throw "Product status promotion policy is not fail closed."
    }

    $sourcePaths = @(
        $Manifest.sources.conformance,
        $Manifest.sources.performance,
        $Manifest.sources.clientMatrix,
        $Manifest.sources.clientCorpus
    )
    Assert-TrackedEvidencePaths -Values $sourcePaths -Context "manifest sources"

    $suites = @($Manifest.suites)
    if ($suites.Count -eq 0 -or $suites.Count -gt $maximumSuites) {
        throw "Product suite count is out of bounds."
    }
    $suiteIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($suite in $suites) {
        Assert-AllowedProperties -Value $suite `
            -Allowed @("id", "status", "reason", "command", "evidencePaths") `
            -Required @("id", "status", "reason", "command", "evidencePaths") `
            -Context "product suite"
        Assert-Identifier -Value $suite.id -Context "suite id"
        if (-not $suiteIds.Add($suite.id)) {
            throw "Duplicate product suite id '$($suite.id)'."
        }
        Assert-Status -Value $suite.status -Context "suite '$($suite.id)' status"
        Assert-BoundedString -Value $suite.reason -Context "suite '$($suite.id)' reason"
        Assert-BoundedString -Value $suite.command -Context "suite '$($suite.id)' command"
        Assert-TrackedEvidencePaths -Values $suite.evidencePaths `
            -Context "suite '$($suite.id)' evidence"
    }

    $prerequisites = @($Manifest.prerequisiteGroups)
    $prerequisiteTotal = 0
    $groupIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($group in $prerequisites) {
        Assert-AllowedProperties -Value $group `
            -Allowed @("id", "requiredInputNames") `
            -Required @("id", "requiredInputNames") `
            -Context "prerequisite group"
        Assert-Identifier -Value $group.id -Context "prerequisite group id"
        if (-not $groupIds.Add($group.id)) {
            throw "Duplicate prerequisite group id '$($group.id)'."
        }
        $names = @($group.requiredInputNames)
        if ($names.Count -eq 0) {
            throw "Prerequisite group '$($group.id)' is empty."
        }
        $nameSet = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($name in $names) {
            Assert-BoundedString -Value $name -Context "prerequisite input name"
            if ($name -notmatch '^ORDADB_[A-Z0-9_]+$') {
                throw "Prerequisite input name '$name' is invalid."
            }
            if (-not $nameSet.Add($name)) {
                throw "Prerequisite group '$($group.id)' contains duplicate '$name'."
            }
        }
        $prerequisiteTotal += $names.Count
    }
    if ($prerequisiteTotal -gt $maximumPrerequisiteNames) {
        throw "Prerequisite input count exceeded its hard bound."
    }

    if ([UInt64]$Manifest.fullScale.targetBytes -ne $fullScaleTargetBytes -or
        [UInt64]$Manifest.fullScale.rows -ne $fullScaleRows -or
        [UInt32]$Manifest.fullScale.connections -ne $fullScaleConnections -or
        [UInt64]$Manifest.fullScale.requiredFreeDiskBytes -ne $fullScaleRequiredDiskBytes -or
        [UInt64]$Manifest.fullScale.requiredFreeMemoryBytes -ne $fullScaleRequiredMemoryBytes -or
        $Manifest.fullScale.overridesAllowed) {
        throw "Full-scale contract must remain fixed at 20 GiB / 10M rows / 32 connections."
    }

    Assert-ExactStringSet -Actual @($Manifest.artifacts.topLevelExecutables) `
        -Expected $expectedTopLevelExecutables -Context "top-level executable contract" `
        -Comparison OrdinalIgnoreCase
    Assert-ExactStringSet -Actual @($Manifest.artifacts.topLevelLaunchers) `
        -Expected $expectedTopLevelLaunchers -Context "top-level launcher contract" `
        -Comparison OrdinalIgnoreCase
    Assert-ExactStringSet -Actual @($Manifest.artifacts.connectorExecutables) `
        -Expected $expectedConnectorExecutables -Context "connector executable contract" `
        -Comparison OrdinalIgnoreCase
    Assert-ExactStringSet -Actual @($Manifest.artifacts.forbiddenBundleExtensions) `
        -Expected $expectedForbiddenBundleExtensions -Context "forbidden bundle extensions" `
        -Comparison OrdinalIgnoreCase
    if ($Manifest.artifacts.connectorDirectory -ne "connectors/v1" -or
        $Manifest.artifacts.connectorCatalog -ne "catalog-v1.json" -or
        [UInt32]$Manifest.artifacts.peMachine -ne $amd64PeMachine -or
        $Manifest.artifacts.installerPattern -ne "OrdaDB_*_x64-setup.exe") {
        throw "Artifact manifest does not match the supported Windows layout."
    }
}

function Assert-ConformanceMatrix {
    param([Parameter(Mandatory = $true)][object]$Matrix)

    Assert-AllowedProperties -Value $Matrix `
        -Allowed @("schemaVersion", "matrixId", "semanticTarget", "recordedAt", "policy", "capabilities") `
        -Required @("schemaVersion", "matrixId", "semanticTarget", "policy", "capabilities") `
        -Context "conformance matrix"
    if ($Matrix.schemaVersion -ne 1 -or $Matrix.semanticTarget -ne "PostgreSQL 18.0") {
        throw "Conformance matrix must target schema v1 and PostgreSQL 18.0."
    }
    Assert-Identifier -Value $Matrix.matrixId -Context "conformance matrix id"
    if ($Matrix.policy.referenceConformantRequires -ne "passed" -or
        -not $Matrix.policy.regressionOnlyIsNotConformance -or
        -not $Matrix.policy.unsupportedMustHaveSqlState) {
        throw "Conformance promotion policy is not fail closed."
    }
    $capabilities = @($Matrix.capabilities)
    if ($capabilities.Count -eq 0 -or $capabilities.Count -gt $maximumCapabilities) {
        throw "Conformance capability count is out of bounds."
    }
    $ids = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($capability in $capabilities) {
        Assert-AllowedProperties -Value $capability `
            -Allowed @(
                "id", "category", "capability", "claimLevel", "status",
                "referenceTarget", "reason", "regressionEvidence", "sqlState"
            ) `
            -Required @(
                "id", "category", "capability", "claimLevel", "status",
                "referenceTarget", "reason", "regressionEvidence"
            ) `
            -Context "conformance capability"
        Assert-Identifier -Value $capability.id -Context "capability id"
        if (-not $ids.Add($capability.id)) {
            throw "Duplicate conformance capability id '$($capability.id)'."
        }
        foreach ($field in @("category", "capability", "claimLevel", "referenceTarget", "reason")) {
            Assert-BoundedString -Value $capability.$field `
                -Context "capability '$($capability.id)' $field"
        }
        Assert-Status -Value $capability.status `
            -Context "capability '$($capability.id)' status"
        if ($capability.claimLevel -eq "referenceConformant" -and
            $capability.status -ne "passed") {
            throw "Capability '$($capability.id)' claims reference conformance without passed evidence."
        }
        if ($capability.status -eq "unsupported") {
            if ($capability.claimLevel -ne "notSupported" -or
                "sqlState" -notin @($capability.PSObject.Properties.Name)) {
                throw "Unsupported capability '$($capability.id)' must declare notSupported and SQLSTATE."
            }
            Assert-BoundedString -Value $capability.sqlState `
                -Context "capability '$($capability.id)' SQLSTATE"
            if ($capability.sqlState -notmatch '^[0-9A-Z]{5}$') {
                throw "Capability '$($capability.id)' SQLSTATE is invalid."
            }
        } elseif ($capability.claimLevel -eq "notSupported") {
            throw "Capability '$($capability.id)' is notSupported without unsupported status."
        }
        Assert-TrackedEvidencePaths -Values $capability.regressionEvidence `
            -Context "capability '$($capability.id)' evidence"
    }
}

function Assert-PerformanceEvidence {
    param([Parameter(Mandatory = $true)][object]$Performance)

    Assert-AllowedProperties -Value $Performance `
        -Allowed @(
            "schemaVersion", "evidenceId", "recordedAt", "target",
            "baselineCommit", "storageCandidateCommit", "queryCandidateCommit",
            "policy", "observations"
        ) `
        -Required @(
            "schemaVersion", "evidenceId", "target", "baselineCommit",
            "storageCandidateCommit", "queryCandidateCommit", "policy",
            "observations"
        ) `
        -Context "performance evidence"
    if ($Performance.schemaVersion -ne 1 -or
        $Performance.target -ne "x86_64-pc-windows-msvc") {
        throw "Performance evidence target/version is invalid."
    }
    Assert-Identifier -Value $Performance.evidenceId -Context "performance evidence id"
    if ([double]$Performance.policy.maxProtectedRegressionPercent -ne 5.0 -or
        [double]$Performance.policy.minPageScanImprovementPercent -lt 20.0 -or
        [double]$Performance.policy.minSpillMergeImprovementPercent -lt 20.0 -or
        [UInt64]$Performance.policy.queryMemoryLimitBytes -ne 67108864) {
        throw "Performance acceptance thresholds were weakened."
    }
    $observations = @($Performance.observations)
    if ($observations.Count -eq 0 -or $observations.Count -gt 64) {
        throw "Performance observation count is out of bounds."
    }
    $ids = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($observation in $observations) {
        Assert-Identifier -Value $observation.id -Context "performance observation id"
        if (-not $ids.Add($observation.id)) {
            throw "Duplicate performance observation '$($observation.id)'."
        }
        if ($observation.status -ne "passed") {
            throw "Performance observation '$($observation.id)' is not passed."
        }
        Assert-BoundedString -Value $observation.provenance `
            -Context "performance observation '$($observation.id)' provenance"
        switch ($observation.kind) {
            "comparison" {
                if ($observation.direction -ne "lowerIsBetter") {
                    throw "Performance comparison '$($observation.id)' has unsupported direction."
                }
                $baseline = [double]$observation.baseline
                $candidate = [double]$observation.candidate
                if ($baseline -le 0 -or $candidate -lt 0) {
                    throw "Performance comparison '$($observation.id)' has invalid values."
                }
                $regression = (($candidate - $baseline) / $baseline) * 100.0
                $improvement = (($baseline - $candidate) / $baseline) * 100.0
                if ($regression -gt ([double]$observation.maxRegressionPercent + 0.0001)) {
                    throw "Performance comparison '$($observation.id)' exceeds its regression limit."
                }
                if ([double]$observation.minImprovementPercent -gt 0 -and
                    $improvement -lt ([double]$observation.minImprovementPercent - 0.0001)) {
                    throw "Performance comparison '$($observation.id)' misses its improvement target."
                }
            }
            "limit" {
                if ([double]$observation.observed -gt [double]$observation.maximum) {
                    throw "Performance limit '$($observation.id)' was exceeded."
                }
            }
            "guard" {
                if ([double]$observation.observed -le 0 -or
                    [double]$observation.maximum -le 0) {
                    throw "Performance guard '$($observation.id)' is invalid."
                }
            }
            default {
                throw "Performance observation '$($observation.id)' has unknown kind."
            }
        }
    }
}

function Assert-ClientFixtures {
    param(
        [Parameter(Mandatory = $true)][object]$Matrix,
        [Parameter(Mandatory = $true)][object]$Corpus
    )

    if ($Matrix.schemaVersion -ne 1 -or $Corpus.schemaVersion -ne 1 -or
        -not $Matrix.statusPolicy.passRequiresExecutedEvidence -or
        $Corpus.capturePolicy.liveReferenceCaptureAvailable) {
        throw "Client fixtures must remain a non-pass, execution-required PostgreSQL 18 baseline."
    }
    $corpusIds = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($case in @($Corpus.cases)) {
        if (-not $corpusIds.Add([string]$case.id)) {
            throw "Client corpus contains duplicate case '$($case.id)'."
        }
        if ($case.referenceResult.status -ne "not_captured") {
            throw "Client corpus case '$($case.id)' was promoted without runtime evidence."
        }
    }
    foreach ($case in @($Matrix.cases)) {
        if ($null -ne $case.corpusCaseId -and -not $corpusIds.Contains([string]$case.corpusCaseId)) {
            throw "Client matrix references unknown corpus case '$($case.corpusCaseId)'."
        }
        foreach ($status in $case.statuses.PSObject.Properties.Value) {
            if ($status.status -eq "passed") {
                throw "Tracked client baseline must not contain a passed runtime case."
            }
        }
    }
}

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

function Assert-Throws {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Action,
        [Parameter(Mandatory = $true)][string]$Context
    )

    $threw = $false
    try {
        & $Action
    } catch {
        $threw = $true
    }
    if (-not $threw) {
        throw "Self-test expected failure: $Context."
    }
}

function Copy-JsonObject {
    param([Parameter(Mandatory = $true)][object]$Value)

    return $Value | ConvertTo-Json -Depth 40 | ConvertFrom-Json
}

function Invoke-SelfTests {
    param(
        [Parameter(Mandatory = $true)][object]$Manifest,
        [Parameter(Mandatory = $true)][object]$Conformance,
        [Parameter(Mandatory = $true)][object]$Performance
    )

    $tests = 0
    $duplicate = Copy-JsonObject -Value $Conformance
    $duplicate.capabilities = @($duplicate.capabilities) + @($duplicate.capabilities[0])
    Assert-Throws -Context "duplicate capability" -Action {
        Assert-ConformanceMatrix -Matrix $duplicate
    }
    $tests += 1

    $unknown = Copy-JsonObject -Value $Conformance
    $unknown.capabilities[0].status = "unknown"
    Assert-Throws -Context "unknown status" -Action {
        Assert-ConformanceMatrix -Matrix $unknown
    }
    $tests += 1

    $promotion = Copy-JsonObject -Value $Conformance
    $promotion.capabilities[0].claimLevel = "referenceConformant"
    Assert-Throws -Context "reference promotion without pass" -Action {
        Assert-ConformanceMatrix -Matrix $promotion
    }
    $tests += 1

    $missing = Copy-JsonObject -Value $Manifest
    $missing.suites[0].evidencePaths = @("acceptance/missing-evidence.json")
    Assert-Throws -Context "missing evidence path" -Action {
        Assert-ProductManifest -Manifest $missing
    }
    $tests += 1

    $weakened = Copy-JsonObject -Value $Performance
    $weakened.policy.maxProtectedRegressionPercent = 6.0
    Assert-Throws -Context "weakened performance threshold" -Action {
        Assert-PerformanceEvidence -Performance $weakened
    }
    $tests += 1

    $selfTestRoot = [IO.Path]::GetFullPath(
        (Join-Path $evidenceRoot ("self-test-" + [guid]::NewGuid().ToString("N")))
    )
    if (-not (Test-ContainedPath -Root $evidenceRoot -Candidate $selfTestRoot)) {
        throw "Self-test root escaped the evidence directory."
    }
    New-Item -ItemType Directory -Path $selfTestRoot -Force | Out-Null
    try {
        $evidence = [ordered]@{ schemaVersion = 1; status = "self-test" }
        $path = Join-Path $selfTestRoot "create-only.json"
        Write-AtomicJson -Value $evidence -Path $path
        Assert-Throws -Context "evidence overwrite" -Action {
            Write-AtomicJson -Value $evidence -Path $path
        }
        $tests += 1

        $badPe = Join-Path $selfTestRoot "bad.exe"
        [IO.File]::WriteAllBytes($badPe, [byte[]](0, 1, 2, 3))
        Assert-Throws -Context "bad PE" -Action {
            Get-PeMachine -Path $badPe | Out-Null
        }
        $tests += 1

        $oversized = Join-Path $selfTestRoot "oversized.json"
        [IO.File]::WriteAllBytes(
            $oversized,
            [byte[]]::new($maximumInputBytes + 1)
        )
        Assert-Throws -Context "oversized input" -Action {
            Read-BoundedJson -Path $oversized -Context "oversized self-test" | Out-Null
        }
        $tests += 1

        Assert-Throws -Context "unsafe repository path" -Action {
            Resolve-RepositoryPath -Value "..\outside.json" `
                -Context "unsafe self-test" | Out-Null
        }
        $tests += 1

        Assert-Throws -Context "case-insensitive executable collision" -Action {
            $collidingNames = @(
                "OrdaDB.exe",
                "ordadb.exe",
                "ordadb-server.exe"
            )
            Assert-ExactStringSet `
                -Actual $collidingNames `
                -Expected $collidingNames `
                -Context "case-insensitive executable collision self-test" `
                -Comparison OrdinalIgnoreCase
        }
        $tests += 1

        $wrongProduct = Join-Path $selfTestRoot "wrong-product"
        $wrongBundle = Join-Path $selfTestRoot "wrong-bundle"
        New-Item -ItemType Directory -Path $wrongProduct, $wrongBundle -Force |
            Out-Null
        Assert-Throws -Context "wrong artifact inventory" -Action {
            Assert-ArtifactTree -Manifest $Manifest `
                -ResolvedProductRoot $wrongProduct `
                -ResolvedBundleRoot $wrongBundle | Out-Null
        }
        $tests += 1
    } finally {
        $resolvedSelfTestRoot = [IO.Path]::GetFullPath($selfTestRoot)
        if ((Test-ContainedPath -Root $evidenceRoot -Candidate $resolvedSelfTestRoot) -and
            (Test-Path -LiteralPath $resolvedSelfTestRoot -PathType Container)) {
            Remove-Item -LiteralPath $resolvedSelfTestRoot -Recurse -Force
        }
    }
    return $tests
}

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
