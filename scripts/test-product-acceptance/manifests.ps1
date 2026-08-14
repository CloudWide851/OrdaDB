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
