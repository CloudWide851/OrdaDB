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

    if (Test-Path -LiteralPath $historyPath -PathType Leaf) {
        $history = Get-Content -Raw -LiteralPath $historyPath | ConvertFrom-Json
        if ($history.schemaVersion -ne 1 -or $history.versions.Count -gt $maximumHistoryVersions) {
            throw "Published connector history is unsupported or exceeds its bounded size"
        }
        foreach ($publishedVersion in $history.versions) {
            if ($publishedVersion.version -eq $Version) {
                continue
            }
            if ($publishedVersion.plugins.Count -ne 4) {
                throw "Every published connector version must contain exactly four artifacts"
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
