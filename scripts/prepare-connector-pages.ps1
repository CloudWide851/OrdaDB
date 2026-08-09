param(
    [Parameter(Mandatory = $true)]
    [string]$Publisher,

    [Parameter(Mandatory = $true)]
    [string]$Artifacts,

    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,

    [Parameter(Mandatory = $true)]
    [string]$Version,

    [Parameter(Mandatory = $true)]
    [string]$PublicKey,

    [string]$BaseUrl = "https://cloudwide851.github.io/OrdaDB/connectors/v1/"
)

$ErrorActionPreference = "Stop"
$maximumArtifactBytes = 256MB
$maximumHistoryVersions = 128
$legacyConnectorIds = @("postgresql", "mysql", "sqlite", "sql-server")
$officialConnectorIds = @(
    "postgresql",
    "mysql",
    "sqlite",
    "sql-server",
    "mongodb",
    "redis",
    "mariadb",
    "clickhouse",
    "oracle"
)
$baseUri = [Uri]::new($BaseUrl)
if ($baseUri.Scheme -ne "https" -or -not $baseUri.AbsolutePath.EndsWith("/")) {
    throw "Connector Pages base URL must be an absolute HTTPS directory"
}

$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ("ordadb-connector-pages-" + [guid]::NewGuid().ToString("N"))
$historyPath = Join-Path $temporaryRoot "history-v1.json"
$bundleOutput = Join-Path $temporaryRoot "bundle"
$siteOutput = Join-Path $OutputRoot "connectors\v1"
New-Item -ItemType Directory -Path $temporaryRoot | Out-Null

try {
    $historyUri = [Uri]::new($baseUri, "history-v1.json")
    try {
        Invoke-WebRequest `
            -Uri $historyUri `
            -OutFile $historyPath `
            -MaximumRedirection 0 `
            -TimeoutSec 30
    }
    catch {
        if ($_.Exception.Response.StatusCode -ne 404) {
            throw
        }
    }

    $publisherArguments = @(
        "sign-bundle",
        "--artifacts", $Artifacts,
        "--bundle-output", $bundleOutput,
        "--site-output", $siteOutput,
        "--public-key", $PublicKey,
        "--version", $Version,
        "--base-url", $BaseUrl
    )
    if (Test-Path -LiteralPath $historyPath -PathType Leaf) {
        $publisherArguments += @("--previous-history", $historyPath)
    }
    & $Publisher @publisherArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate the signed connector Pages tree"
    }

    $generatedCatalog = Get-Content -Raw -LiteralPath (Join-Path $siteOutput "catalog-v1.json") |
        ConvertFrom-Json
    $generatedIds = @($generatedCatalog.plugins | ForEach-Object { $_.id } | Sort-Object -Unique)
    if ($generatedCatalog.schemaVersion -ne 1 -or
        $generatedCatalog.plugins.Count -ne $officialConnectorIds.Count -or
        (Compare-Object ($officialConnectorIds | Sort-Object) $generatedIds)) {
        throw "The current connector catalog must contain exactly the nine official helpers"
    }

    $generatedHistory = Get-Content -Raw -LiteralPath (Join-Path $siteOutput "history-v1.json") |
        ConvertFrom-Json
    $currentHistory = @($generatedHistory.versions | Where-Object { $_.version -eq $Version })
    $currentHistoryIds = if ($currentHistory.Count -eq 1) {
        @($currentHistory[0].plugins | ForEach-Object { $_.id } | Sort-Object -Unique)
    }
    if ($currentHistory.Count -ne 1 -or
        $currentHistory[0].plugins.Count -ne $officialConnectorIds.Count -or
        (Compare-Object ($officialConnectorIds | Sort-Object) $currentHistoryIds)) {
        throw "The current connector history entry must contain exactly nine artifacts"
    }

    if (Test-Path -LiteralPath $historyPath -PathType Leaf) {
        $history = Get-Content -Raw -LiteralPath $historyPath | ConvertFrom-Json
        if ($history.schemaVersion -ne 1 -or $history.versions.Count -gt $maximumHistoryVersions) {
            throw "Published connector history is unsupported or exceeds its bounded size"
        }
        foreach ($publishedVersion in $history.versions) {
            if ($publishedVersion.version -eq $Version) {
                continue
            }
            $publishedIds = @($publishedVersion.plugins | ForEach-Object { $_.id } | Sort-Object -Unique)
            $isLegacySet = $publishedVersion.plugins.Count -eq $legacyConnectorIds.Count -and
                -not (Compare-Object ($legacyConnectorIds | Sort-Object) $publishedIds)
            $isOfficialSet = $publishedVersion.plugins.Count -eq $officialConnectorIds.Count -and
                -not (Compare-Object ($officialConnectorIds | Sort-Object) $publishedIds)
            if (-not $isLegacySet -and -not $isOfficialSet) {
                throw "Published connector history must contain an immutable four-helper legacy set or nine-helper official set"
            }
            foreach ($plugin in $publishedVersion.plugins) {
                $downloadUri = [Uri]::new($plugin.downloadUrl)
                $expectedPrefix = [Uri]::new(
                    $baseUri,
                    "artifacts/$($publishedVersion.version)/"
                ).AbsoluteUri
                if (-not $downloadUri.AbsoluteUri.StartsWith($expectedPrefix, [StringComparison]::Ordinal) -or
                    $downloadUri.Scheme -ne "https" -or
                    $downloadUri.Host -ne $baseUri.Host -or
                    $downloadUri.Port -ne $baseUri.Port) {
                    throw "Published connector history contains an invalid artifact origin"
                }
                $fileName = [IO.Path]::GetFileName($downloadUri.AbsolutePath)
                if (-not $fileName.EndsWith(".exe") -or $fileName.Contains("..")) {
                    throw "Published connector history contains an invalid artifact name"
                }
                $size = [uint64]$plugin.size
                if ($size -eq 0 -or $size -gt $maximumArtifactBytes) {
                    throw "Published connector history contains an invalid artifact size"
                }
                $destinationDirectory = Join-Path $siteOutput "artifacts\$($publishedVersion.version)"
                New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
                $destination = Join-Path $destinationDirectory $fileName
                Invoke-WebRequest `
                    -Uri $downloadUri `
                    -OutFile $destination `
                    -MaximumRedirection 0 `
                    -TimeoutSec 120
                $file = Get-Item -LiteralPath $destination
                $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
                if ([uint64]$file.Length -ne $size -or $hash -ne $plugin.sha256) {
                    throw "A previously published connector artifact failed integrity verification"
                }
            }
        }
    }
}
finally {
    $resolvedTemporary = if (Test-Path -LiteralPath $temporaryRoot) {
        (Resolve-Path -LiteralPath $temporaryRoot).Path
    }
    $systemTemporary = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if ($resolvedTemporary -and
        $resolvedTemporary.StartsWith($systemTemporary, [StringComparison]::OrdinalIgnoreCase)) {
        Remove-Item -LiteralPath $resolvedTemporary -Recurse -Force
    }
}

Write-Output "Prepared the signed connector Pages tree with bounded verified history"
