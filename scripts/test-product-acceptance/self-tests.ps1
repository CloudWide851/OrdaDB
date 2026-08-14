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
