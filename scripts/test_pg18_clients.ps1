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

function Test-ContainedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [string]$Candidate
    )

    $resolvedRoot = [IO.Path]::GetFullPath($Root).TrimEnd("\", "/") + [IO.Path]::DirectorySeparatorChar
    $resolvedCandidate = [IO.Path]::GetFullPath($Candidate)
    return $resolvedCandidate.StartsWith($resolvedRoot, [StringComparison]::OrdinalIgnoreCase)
}

function Assert-NoReparseDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [string]$Directory
    )

    if (-not (Test-ContainedPath -Root $Root -Candidate (Join-Path $Directory "containment.probe"))) {
        throw "A generated directory escaped its containment root."
    }
    $resolvedRoot = [IO.Path]::GetFullPath($Root)
    $resolvedDirectory = [IO.Path]::GetFullPath($Directory)
    $relative = [IO.Path]::GetRelativePath($resolvedRoot, $resolvedDirectory)
    $current = $resolvedRoot
    if (Test-Path -LiteralPath $current) {
        $rootItem = Get-Item -LiteralPath $current -Force
        if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "The generated-output root must not be a reparse point."
        }
    }
    foreach ($segment in $relative.Split(@("\", "/"), [StringSplitOptions]::RemoveEmptyEntries)) {
        $current = Join-Path $current $segment
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Generated-output directories must not traverse a reparse point."
            }
        }
    }
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
    param([AllowNull()][string]$Value)

    if ($null -eq $Value) {
        return $null
    }
    $protected = $Value
    $secretNames = @(
        "ORDADB_PG18_PASSWORD",
        "ORDADB_PG18_USER",
        "ORDADB_PG18_HOST",
        "ORDADB_PG18_DATABASE",
        "ORDADB_PG18_ROOT_CERT"
    )
    foreach ($name in $secretNames) {
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
    $protected = [Regex]::Replace(
        $protected,
        "(?i)(password|passwd|pwd)\s*[=:]\s*[^\s;]+",
        '$1=<redacted>'
    )
    $protected = [Regex]::Replace(
        $protected,
        "(?i)(postgres(?:ql)?://)[^/@\s]+@",
        '$1<redacted>@'
    )
    return Limit-Utf8 -Value $protected -MaximumBytes $maximumDiagnosticBytes
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
        throw "Evidence output must remain under target\client-compat\evidence."
    }
    if ([IO.Path]::GetExtension($resolvedPath) -ne ".json") {
        throw "Evidence output must use a .json extension."
    }
    $directory = [IO.Path]::GetDirectoryName($resolvedPath)
    Assert-NoReparseDirectory -Root $evidenceRoot -Directory $directory
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    Assert-NoReparseDirectory -Root $evidenceRoot -Directory $directory

    $json = $Value | ConvertTo-Json -Depth 20
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($json)
    if ($bytes.Length -gt $maximumEvidenceBytes) {
        throw "Client compatibility evidence exceeded its 256 KiB bound."
    }

    $temporaryPath = Join-Path $directory (
        [IO.Path]::GetFileName($resolvedPath) + "." + [guid]::NewGuid().ToString("N") + ".tmp"
    )
    if (-not (Test-ContainedPath -Root $directory -Candidate $temporaryPath)) {
        throw "The evidence temporary sibling escaped its output directory."
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
        [IO.File]::Move($temporaryPath, $resolvedPath, $true)
    } finally {
        if ($null -ne $stream) {
            $stream.Dispose()
        }
        if (Test-Path -LiteralPath $temporaryPath -PathType Leaf) {
            Remove-Item -LiteralPath $temporaryPath -Force
        }
    }
}

function Invoke-BoundedProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,

        [string[]]$Arguments = @(),

        [Parameter(Mandatory = $true)]
        [string]$WorkingDirectory,

        [System.Collections.IDictionary]$Environment = @{},

        [UInt32]$ProcessTimeoutSeconds = 15
    )

    $processInfo = [Diagnostics.ProcessStartInfo]::new()
    $processInfo.FileName = $FilePath
    $processInfo.WorkingDirectory = $WorkingDirectory
    $processInfo.UseShellExecute = $false
    $processInfo.CreateNoWindow = $true
    $processInfo.RedirectStandardOutput = $true
    $processInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        $processInfo.ArgumentList.Add([string]$argument)
    }
    foreach ($entry in $Environment.GetEnumerator()) {
        $processInfo.Environment[[string]$entry.Key] = [string]$entry.Value
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $processInfo
    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    try {
        if (-not $process.Start()) {
            throw "A bounded client process could not be started."
        }
        $standardOutput = $process.StandardOutput.ReadToEndAsync()
        $standardError = $process.StandardError.ReadToEndAsync()
        $completed = $process.WaitForExit([int]($ProcessTimeoutSeconds * 1000))
        $timedOut = -not $completed
        if ($timedOut) {
            try {
                $process.Kill($true)
            } catch {
                # Preserve timeout as the primary controlled result.
            }
            $process.WaitForExit(5000) | Out-Null
        }
        $output = $standardOutput.GetAwaiter().GetResult()
        $errorOutput = $standardError.GetAwaiter().GetResult()
        return [ordered]@{
            exitCode = if ($timedOut) { $null } else { $process.ExitCode }
            timedOut = $timedOut
            durationMs = [UInt64]$stopwatch.ElapsedMilliseconds
            stdout = Protect-Diagnostic -Value $output
            stderr = Protect-Diagnostic -Value $errorOutput
        }
    } finally {
        $stopwatch.Stop()
        $process.Dispose()
    }
}

function Get-RepositoryCommit {
    $gitControlPath = Join-Path $repoRoot ".git"
    if (-not (Test-Path -LiteralPath $gitControlPath)) {
        return "unknown"
    }
    $gitDirectory = $gitControlPath
    if (Test-Path -LiteralPath $gitControlPath -PathType Leaf) {
        $gitControl = (Get-Content -Raw -LiteralPath $gitControlPath).Trim()
        if ($gitControl -notmatch '^gitdir:\s+(.+)$') {
            return "unknown"
        }
        $gitDirectory = if ([IO.Path]::IsPathRooted($Matches[1])) {
            [IO.Path]::GetFullPath($Matches[1])
        } else {
            [IO.Path]::GetFullPath((Join-Path $repoRoot $Matches[1]))
        }
    }
    $headPath = Join-Path $gitDirectory "HEAD"
    if (-not (Test-Path -LiteralPath $headPath -PathType Leaf) -or
        (Get-Item -LiteralPath $headPath).Length -gt 1024) {
        return "unknown"
    }
    $head = (Get-Content -Raw -LiteralPath $headPath).Trim()
    if ($head -match '^[0-9a-f]{40,64}$') {
        return $head
    }
    if ($head -notmatch '^ref:\s+(.+)$') {
        return "unknown"
    }
    $referenceName = $Matches[1]
    if ($referenceName.Contains("..") -or $referenceName -notmatch '^refs/[A-Za-z0-9._/-]+$') {
        return "unknown"
    }
    $referencePath = [IO.Path]::GetFullPath((Join-Path $gitDirectory $referenceName))
    if (-not (Test-ContainedPath -Root $gitDirectory -Candidate $referencePath)) {
        return "unknown"
    }
    if (Test-Path -LiteralPath $referencePath -PathType Leaf) {
        $commit = (Get-Content -Raw -LiteralPath $referencePath).Trim()
        if ($commit -match '^[0-9a-f]{40,64}$') {
            return $commit
        }
    }
    $packedRefsPath = Join-Path $gitDirectory "packed-refs"
    if ((Test-Path -LiteralPath $packedRefsPath -PathType Leaf) -and
        (Get-Item -LiteralPath $packedRefsPath).Length -le 4MB) {
        foreach ($line in Get-Content -LiteralPath $packedRefsPath) {
            if ($line -match "^([0-9a-f]{40,64})\s+$([Regex]::Escape($referenceName))$") {
                return $Matches[1]
            }
        }
    }
    return "unknown"
}

function Resolve-ToolPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EnvironmentName,

        [string]$DefaultPath,

        [string]$CommandName
    )

    $override = [Environment]::GetEnvironmentVariable($EnvironmentName)
    if (-not [string]::IsNullOrWhiteSpace($override)) {
        if (-not [IO.Path]::IsPathRooted($override)) {
            throw "$EnvironmentName must name an absolute file path."
        }
        $candidate = [IO.Path]::GetFullPath($override)
    } elseif (-not [string]::IsNullOrWhiteSpace($DefaultPath) -and
        (Test-Path -LiteralPath $DefaultPath -PathType Leaf)) {
        $candidate = [IO.Path]::GetFullPath($DefaultPath)
    } elseif (-not [string]::IsNullOrWhiteSpace($CommandName)) {
        $command = Get-Command $CommandName -CommandType Application -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -eq $command) {
            throw "$CommandName is unavailable; set $EnvironmentName to the pinned executable."
        }
        $candidate = [IO.Path]::GetFullPath($command.Source)
    } else {
        throw "The pinned artifact for $EnvironmentName is unavailable."
    }
    if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        throw "The pinned artifact for $EnvironmentName is not a file."
    }
    return $candidate
}

function Assert-PinnedHash {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$ExpectedSha256
    )

    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
    if ($actual -ne $ExpectedSha256.ToLowerInvariant()) {
        throw "A pinned client artifact failed SHA-256 validation."
    }
    return $actual
}

function Get-ClientDefinition {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Matrix,

        [Parameter(Mandatory = $true)]
        [string]$Id
    )

    $definition = $Matrix.clients | Where-Object { $_.id -eq $Id } | Select-Object -First 1
    if ($null -eq $definition) {
        throw "The capability matrix is missing a selected client definition."
    }
    return $definition
}

function Read-AndValidateFixtures {
    foreach ($path in @($matrixPath, $corpusPath, $pgJdbcSource, $hibernateSource)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "A required client compatibility fixture is missing."
        }
        $item = Get-Item -LiteralPath $path
        if ($item.Length -le 0 -or $item.Length -gt $maximumFixtureBytes) {
            throw "A client compatibility fixture is empty or exceeds 1 MiB."
        }
    }

    $matrix = Get-Content -Raw -LiteralPath $matrixPath | ConvertFrom-Json
    $corpus = Get-Content -Raw -LiteralPath $corpusPath | ConvertFrom-Json
    if ($matrix.schemaVersion -ne 1 -or
        $matrix.matrixId -ne "ordadb-postgresql-18-windows-x64-clients") {
        throw "The client capability matrix version or identity is unsupported."
    }
    if ($corpus.schemaVersion -ne 1 -or
        $corpus.corpusId -ne "ordadb-postgresql-18-client-replay") {
        throw "The client SQL corpus version or identity is unsupported."
    }
    if ($matrix.target.operatingSystem -ne "Windows" -or
        $matrix.target.architecture -ne "AMD64" -or
        $matrix.target.rustTarget -ne "x86_64-pc-windows-msvc") {
        throw "The client matrix target is not the supported Windows AMD64 target."
    }
    if (-not $matrix.statusPolicy.passRequiresExecutedEvidence) {
        throw "The client matrix must require executed evidence before pass."
    }

    $clientIds = @($matrix.clients | ForEach-Object { [string]$_.id })
    if ($clientIds.Count -ne 4 -or
        @($clientIds | Sort-Object -Unique).Count -ne $clientIds.Count) {
        throw "The client matrix must contain four unique pinned clients."
    }
    foreach ($requiredClient in @("psql", "pgjdbc", "datagrip", "hibernate")) {
        if ($requiredClient -notin $clientIds) {
            throw "The client matrix is missing a required pinned client."
        }
    }
    foreach ($clientDefinition in $matrix.clients) {
        if ([string]::IsNullOrWhiteSpace([string]$clientDefinition.supportedVersion) -or
            [string]::IsNullOrWhiteSpace([string]$clientDefinition.versionMatch)) {
            throw "Every client must pin a supported version and version matcher."
        }
        if ($null -ne $clientDefinition.artifactSha256 -and
            [string]$clientDefinition.artifactSha256 -notmatch '^[0-9a-f]{64}$') {
            throw "A pinned client artifact SHA-256 is malformed."
        }
    }

    if ($matrix.cases.Count -le 0 -or $matrix.cases.Count -gt 64) {
        throw "The capability case count is empty or exceeds 64."
    }
    $matrixCaseIds = @($matrix.cases | ForEach-Object { [string]$_.id })
    if (@($matrixCaseIds | Sort-Object -Unique).Count -ne $matrixCaseIds.Count) {
        throw "The client matrix contains duplicate case IDs."
    }
    $allowedStatuses = @($matrix.statusPolicy.allowedBaselineStatuses)
    foreach ($case in $matrix.cases) {
        foreach ($clientId in $clientIds) {
            $statusProperty = $case.statuses.PSObject.Properties[$clientId]
            if ($null -eq $statusProperty) {
                throw "A capability case is missing an explicit client status."
            }
            $status = [string]$statusProperty.Value.status
            if ($status -notin $allowedStatuses -or $status -eq "passed") {
                throw "A baseline capability status is unsupported or claims an unexecuted pass."
            }
            if ([string]::IsNullOrWhiteSpace([string]$statusProperty.Value.reason)) {
                throw "Every baseline capability status must include a reason."
            }
        }
    }

    if ($corpus.cases.Count -le 0 -or $corpus.cases.Count -gt [int]$corpus.limits.maxCases) {
        throw "The SQL corpus case count is empty or exceeds its declared bound."
    }
    $corpusCaseIds = @($corpus.cases | ForEach-Object { [string]$_.id })
    if (@($corpusCaseIds | Sort-Object -Unique).Count -ne $corpusCaseIds.Count) {
        throw "The SQL corpus contains duplicate case IDs."
    }
    $requiredCategories = @(
        "catalog_introspection",
        "session_startup",
        "ddl_crud",
        "prepared_portal",
        "transactions_savepoints",
        "copy",
        "cancellation",
        "error_recovery"
    )
    $categories = @($corpus.cases | ForEach-Object { [string]$_.category } | Sort-Object -Unique)
    foreach ($category in $requiredCategories) {
        if ($category -notin $categories) {
            throw "The SQL corpus is missing a required compatibility category."
        }
    }
    $utf8 = [Text.UTF8Encoding]::new($false)
    foreach ($case in $corpus.cases) {
        if ($case.steps.Count -le 0 -or $case.steps.Count -gt [int]$corpus.limits.maxStepsPerCase) {
            throw "A SQL corpus case is empty or exceeds its step bound."
        }
        if ($case.referenceResult.status -ne "not_captured" -or
            [string]::IsNullOrWhiteSpace([string]$case.referenceResult.gap)) {
            throw "Every baseline corpus case must label its reference-result gap."
        }
        foreach ($step in $case.steps) {
            if ($null -ne $step.PSObject.Properties["sql"] -and
                $utf8.GetByteCount([string]$step.sql) -gt [int]$corpus.limits.maxSqlBytesPerStep) {
                throw "A SQL corpus statement exceeds its byte bound."
            }
            if ($null -ne $step.PSObject.Properties["payload"] -and
                $utf8.GetByteCount([string]$step.payload) -gt [int]$corpus.limits.maxCopyPayloadBytes) {
                throw "A SQL corpus COPY payload exceeds its byte bound."
            }
        }
    }
    foreach ($case in $matrix.cases) {
        if ($null -ne $case.corpusCaseId -and [string]$case.corpusCaseId -notin $corpusCaseIds) {
            throw "A matrix case references an unknown SQL corpus case."
        }
    }

    return [ordered]@{
        matrix = $matrix
        corpus = $corpus
        summary = [ordered]@{
            status = "passed"
            matrixSchemaVersion = [int]$matrix.schemaVersion
            corpusSchemaVersion = [int]$corpus.schemaVersion
            clients = $matrix.clients.Count
            matrixCases = $matrix.cases.Count
            corpusCases = $corpus.cases.Count
        }
    }
}

function Get-ConnectionInputs {
    $requiredNames = @(
        "ORDADB_PG18_HOST",
        "ORDADB_PG18_PORT",
        "ORDADB_PG18_DATABASE",
        "ORDADB_PG18_USER",
        "ORDADB_PG18_PASSWORD",
        "ORDADB_PG18_SSLMODE",
        "ORDADB_PG18_ROOT_CERT",
        "ORDADB_PG18_ISOLATED_CONFIRM"
    )
    $missing = @($requiredNames | Where-Object {
        [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_))
    })
    if ($missing.Count -gt 0) {
        throw "Required isolated connection inputs are missing: $($missing -join ', ')."
    }

    $hostValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_HOST")
    $portValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_PORT")
    $databaseValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_DATABASE")
    $userValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_USER")
    $sslModeValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_SSLMODE")
    $certificateValue = [Environment]::GetEnvironmentVariable("ORDADB_PG18_ROOT_CERT")
    $isolatedConfirmation = [Environment]::GetEnvironmentVariable("ORDADB_PG18_ISOLATED_CONFIRM")

    if ($hostValue -notmatch '^[A-Za-z0-9.-]{1,253}$') {
        throw "ORDADB_PG18_HOST has an invalid bounded hostname shape."
    }
    [UInt16]$port = 0
    if (-not [UInt16]::TryParse($portValue, [ref]$port) -or $port -eq 0) {
        throw "ORDADB_PG18_PORT must be an integer from 1 through 65535."
    }
    if ($databaseValue -notmatch '^ordadb_compat_[A-Za-z0-9_]{1,48}$') {
        throw "ORDADB_PG18_DATABASE must use the isolated ordadb_compat_* naming contract."
    }
    if ($userValue -notmatch '^[A-Za-z0-9_.-]{1,63}$') {
        throw "ORDADB_PG18_USER has an invalid bounded identifier shape."
    }
    if ($sslModeValue -ne "verify-full") {
        throw "ORDADB_PG18_SSLMODE must be verify-full."
    }
    if ($isolatedConfirmation -cne "YES") {
        throw "ORDADB_PG18_ISOLATED_CONFIRM must be the exact value YES."
    }
    if (-not [IO.Path]::IsPathRooted($certificateValue)) {
        throw "ORDADB_PG18_ROOT_CERT must name an absolute certificate file."
    }
    $certificatePath = [IO.Path]::GetFullPath($certificateValue)
    if (-not (Test-Path -LiteralPath $certificatePath -PathType Leaf)) {
        throw "ORDADB_PG18_ROOT_CERT is not a file."
    }
    $certificate = Get-Item -LiteralPath $certificatePath
    if ($certificate.Length -le 0 -or $certificate.Length -gt $maximumCertificateBytes) {
        throw "ORDADB_PG18_ROOT_CERT is empty or exceeds 1 MiB."
    }
    if ($certificate.Extension.ToLowerInvariant() -notin @(".pem", ".crt")) {
        throw "ORDADB_PG18_ROOT_CERT must use a .pem or .crt extension."
    }
    $parsedCertificate = $null
    try {
        $parsedCertificate = [Security.Cryptography.X509Certificates.X509Certificate2]::CreateFromPemFile(
            $certificatePath
        )
        if ($parsedCertificate.NotAfter.ToUniversalTime() -le [DateTime]::UtcNow) {
            throw "ORDADB_PG18_ROOT_CERT is expired."
        }
    } catch {
        throw "ORDADB_PG18_ROOT_CERT is not a current parseable PEM certificate."
    } finally {
        if ($null -ne $parsedCertificate) {
            $parsedCertificate.Dispose()
        }
    }

    $environment = @{}
    foreach ($name in $requiredNames) {
        $environment[$name] = [Environment]::GetEnvironmentVariable($name)
    }
    return [ordered]@{
        environment = $environment
        evidence = [ordered]@{
            status = "passed"
            host = "<redacted>"
            port = [int]$port
            database = "<redacted>"
            user = "<redacted>"
            tlsMode = "verify-full"
            rootCertificate = "present"
            isolationConfirmed = $true
        }
    }
}

function Ensure-JavaRuntime {
    if ($runtimePaths.ContainsKey("java") -and $runtimePaths.ContainsKey("javac")) {
        return
    }
    $javaPath = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_JAVA_PATH" `
        -CommandName "java.exe"
    $javacPath = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_JAVAC_PATH" `
        -CommandName "javac.exe"
    $javaVersion = Invoke-BoundedProcess `
        -FilePath $javaPath `
        -Arguments @("-version") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds 10
    $javacVersion = Invoke-BoundedProcess `
        -FilePath $javacPath `
        -Arguments @("-version") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds 10
    $versionText = "$($javaVersion.stdout)`n$($javaVersion.stderr)`n$($javacVersion.stdout)`n$($javacVersion.stderr)"
    if ($javaVersion.timedOut -or $javaVersion.exitCode -ne 0 -or
        $javacVersion.timedOut -or $javacVersion.exitCode -ne 0 -or
        $versionText -notmatch '(?m)(?:openjdk version|java version|javac)\s+"?(11|21)(?:\.|\s)') {
        throw "Java and javac must be a bounded JDK 11 or JDK 21 pair."
    }
    $runtimePaths["java"] = $javaPath
    $runtimePaths["javac"] = $javacPath
    $runtimePaths["javaVersion"] = Protect-Diagnostic -Value $versionText.Trim()
}

function Preflight-Psql {
    param([object]$Definition)

    $path = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_PSQL_PATH" `
        -CommandName "psql.exe"
    $version = Invoke-BoundedProcess `
        -FilePath $path `
        -Arguments @("--version") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds 10
    if ($version.timedOut -or $version.exitCode -ne 0) {
        throw "The pinned psql version probe failed or timed out."
    }
    $versionText = "$($version.stdout)`n$($version.stderr)".Trim()
    if ($versionText -notmatch [string]$Definition.versionMatch) {
        throw "psql does not match the pinned version."
    }
    $runtimePaths["psql"] = $path
    return [ordered]@{
        client = "psql"
        status = "passed"
        version = [string]$Definition.supportedVersion
        fileName = [IO.Path]::GetFileName($path)
    }
}

function Preflight-PgJdbc {
    param([object]$Definition)

    Ensure-JavaRuntime
    $path = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_PGJDBC_JAR" `
        -DefaultPath "C:\Users\siyez\.m2\repository\org\postgresql\postgresql\42.7.10\postgresql-42.7.10.jar"
    $hash = Assert-PinnedHash -Path $path -ExpectedSha256 ([string]$Definition.artifactSha256)
    $runtimePaths["pgjdbc"] = $path
    return [ordered]@{
        client = "pgjdbc"
        status = "passed"
        version = [string]$Definition.supportedVersion
        sha256 = $hash
        fileName = [IO.Path]::GetFileName($path)
        java = $runtimePaths["javaVersion"]
    }
}

function Preflight-DataGrip {
    param([object]$Definition)

    $path = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_DATAGRIP_PATH" `
        -DefaultPath "F:\DataGripData\DataGrip 2023.2\bin\datagrip64.exe"
    $hash = Assert-PinnedHash -Path $path -ExpectedSha256 ([string]$Definition.artifactSha256)
    $version = Invoke-BoundedProcess `
        -FilePath $path `
        -Arguments @("-version") `
        -WorkingDirectory $repoRoot `
        -ProcessTimeoutSeconds 15
    if ($version.timedOut -or $version.exitCode -ne 0) {
        throw "The pinned DataGrip version probe failed or timed out."
    }
    $versionText = "$($version.stdout)`n$($version.stderr)"
    if ($versionText -notmatch [string]$Definition.versionMatch -or
        $versionText -notmatch [Regex]::Escape([string]$Definition.supportedVersion)) {
        throw "DataGrip does not match the pinned product version and build."
    }
    $runtimePaths["datagrip"] = $path
    return [ordered]@{
        client = "datagrip"
        status = "passed"
        version = [string]$Definition.supportedVersion
        build = [string]$Definition.supportedBuild
        sha256 = $hash
        fileName = [IO.Path]::GetFileName($path)
    }
}

function Preflight-Hibernate {
    param(
        [object]$Definition,
        [object]$Matrix
    )

    Ensure-JavaRuntime
    if (-not $runtimePaths.ContainsKey("pgjdbc")) {
        Preflight-PgJdbc -Definition (Get-ClientDefinition -Matrix $Matrix -Id "pgjdbc") | Out-Null
    }
    $corePath = Resolve-ToolPath `
        -EnvironmentName "ORDADB_PG18_HIBERNATE_CORE_JAR" `
        -DefaultPath "C:\Users\siyez\.m2\repository\org\hibernate\orm\hibernate-core\6.6.29.Final\hibernate-core-6.6.29.Final.jar"
    $hash = Assert-PinnedHash -Path $corePath -ExpectedSha256 ([string]$Definition.artifactSha256)
    $m2Override = [Environment]::GetEnvironmentVariable("ORDADB_PG18_M2_ROOT")
    $m2Root = if ([string]::IsNullOrWhiteSpace($m2Override)) {
        "C:\Users\siyez\.m2\repository"
    } else {
        if (-not [IO.Path]::IsPathRooted($m2Override)) {
            throw "ORDADB_PG18_M2_ROOT must name an absolute directory."
        }
        [IO.Path]::GetFullPath($m2Override)
    }
    if (-not (Test-Path -LiteralPath $m2Root -PathType Container)) {
        throw "The pinned Maven repository root is unavailable."
    }
    $relativeDependencies = @(
        "jakarta\persistence\jakarta.persistence-api\3.1.0\jakarta.persistence-api-3.1.0.jar",
        "jakarta\transaction\jakarta.transaction-api\2.0.1\jakarta.transaction-api-2.0.1.jar",
        "org\jboss\logging\jboss-logging\3.5.0.Final\jboss-logging-3.5.0.Final.jar",
        "org\hibernate\common\hibernate-commons-annotations\7.0.3.Final\hibernate-commons-annotations-7.0.3.Final.jar",
        "io\smallrye\jandex\3.2.0\jandex-3.2.0.jar",
        "com\fasterxml\classmate\1.5.1\classmate-1.5.1.jar",
        "net\bytebuddy\byte-buddy\1.15.11\byte-buddy-1.15.11.jar",
        "jakarta\xml\bind\jakarta.xml.bind-api\4.0.0\jakarta.xml.bind-api-4.0.0.jar",
        "org\glassfish\jaxb\jaxb-runtime\4.0.2\jaxb-runtime-4.0.2.jar",
        "org\glassfish\jaxb\jaxb-core\4.0.2\jaxb-core-4.0.2.jar",
        "jakarta\inject\jakarta.inject-api\2.0.1\jakarta.inject-api-2.0.1.jar",
        "org\antlr\antlr4-runtime\4.13.0\antlr4-runtime-4.13.0.jar"
    )
    $dependencies = @($corePath, $runtimePaths["pgjdbc"])
    foreach ($relative in $relativeDependencies) {
        $candidate = [IO.Path]::GetFullPath((Join-Path $m2Root $relative))
        if (-not (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            throw "A pinned Hibernate 6.6.29.Final runtime dependency is missing: $([IO.Path]::GetFileName($candidate))."
        }
        $dependencies += $candidate
    }
    $runtimePaths["hibernateClasspath"] = $dependencies -join [IO.Path]::PathSeparator
    return [ordered]@{
        client = "hibernate"
        status = "passed"
        version = [string]$Definition.supportedVersion
        sha256 = $hash
        fileName = [IO.Path]::GetFileName($corePath)
        runtimeArtifacts = $dependencies.Count
        java = $runtimePaths["javaVersion"]
    }
}

function Invoke-SelectedPreflight {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Matrix,

        [Parameter(Mandatory = $true)]
        [string[]]$SelectedClients
    )

    $results = @()
    $failedClients = @()
    foreach ($clientId in $SelectedClients) {
        try {
            $definition = Get-ClientDefinition -Matrix $Matrix -Id $clientId
            $result = switch ($clientId) {
                "psql" { Preflight-Psql -Definition $definition }
                "pgjdbc" { Preflight-PgJdbc -Definition $definition }
                "datagrip" { Preflight-DataGrip -Definition $definition }
                "hibernate" { Preflight-Hibernate -Definition $definition -Matrix $Matrix }
                default { throw "An unsupported client was selected." }
            }
            $results += $result
        } catch {
            $failedClients += $clientId
            $results += [ordered]@{
                client = $clientId
                status = "failed"
                diagnostic = Protect-Diagnostic -Value $_.Exception.Message
            }
        }
    }
    return [ordered]@{
        results = $results
        failedClients = $failedClients
    }
}

function New-PsqlReplayScript {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Corpus,

        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $caseIds = @(
        "session-startup-001",
        "catalog-schemas-001",
        "catalog-relations-001",
        "catalog-columns-001",
        "catalog-visibility-001",
        "ddl-crud-001",
        "transactions-savepoint-001",
        "copy-in-text-001",
        "copy-out-csv-001",
        "error-simple-recovery-001"
    )
    $lines = [Collections.Generic.List[string]]::new()
    $lines.Add("\set ON_ERROR_STOP on")
    $lines.Add("\set VERBOSITY sqlstate")
    $lines.Add("\pset pager off")
    $lines.Add("SET application_name = 'ordadb-pg18-psql-compat';")
    foreach ($caseId in $caseIds) {
        $case = $Corpus.cases | Where-Object { $_.id -eq $caseId } | Select-Object -First 1
        if ($null -eq $case) {
            throw "The psql replay list references an unknown corpus case."
        }
        $lines.Add("\echo CASE $caseId")
        foreach ($step in $case.steps) {
            switch ([string]$step.kind) {
                "sql" {
                    $lines.Add(([string]$step.sql).TrimEnd(';') + ";")
                }
                "sql_expect_error" {
                    $lines.Add("\set ON_ERROR_STOP off")
                    $lines.Add(([string]$step.sql).TrimEnd(';') + ";")
                    $lines.Add("\set ON_ERROR_STOP on")
                }
                "copy_in" {
                    $lines.Add(([string]$step.sql).TrimEnd(';') + ";")
                    foreach ($payloadLine in ([string]$step.payload -split "`n")) {
                        if ($payloadLine.Length -gt 0) {
                            $lines.Add($payloadLine.TrimEnd("`r"))
                        }
                    }
                    $lines.Add("\.")
                }
                "copy_out" {
                    $lines.Add(([string]$step.sql).TrimEnd(';') + ";")
                }
                default {
                    # Raw extended-query and client-action steps are owned by pgJDBC.
                }
            }
        }
    }
    $content = ($lines -join "`r`n") + "`r`n"
    if ([Text.UTF8Encoding]::new($false).GetByteCount($content) -gt 64KB) {
        throw "The generated psql replay script exceeded 64 KiB."
    }
    [IO.File]::WriteAllText($Path, $content, [Text.UTF8Encoding]::new($false))
    return $caseIds
}

function Invoke-PsqlRun {
    param(
        [object]$Corpus,
        [System.Collections.IDictionary]$ConnectionEnvironment,
        [string]$RunDirectory
    )

    $scriptPath = Join-Path $RunDirectory "psql-replay.sql"
    $caseIds = New-PsqlReplayScript -Corpus $Corpus -Path $scriptPath
    $processEnvironment = @{
        "PGPASSWORD" = $ConnectionEnvironment["ORDADB_PG18_PASSWORD"]
        "PGSSLMODE" = "verify-full"
        "PGSSLROOTCERT" = $ConnectionEnvironment["ORDADB_PG18_ROOT_CERT"]
        "PGAPPNAME" = "ordadb-pg18-psql-compat"
    }
    $result = Invoke-BoundedProcess `
        -FilePath $runtimePaths["psql"] `
        -Arguments @(
            "-X",
            "--no-password",
            "--host", $ConnectionEnvironment["ORDADB_PG18_HOST"],
            "--port", $ConnectionEnvironment["ORDADB_PG18_PORT"],
            "--username", $ConnectionEnvironment["ORDADB_PG18_USER"],
            "--dbname", $ConnectionEnvironment["ORDADB_PG18_DATABASE"],
            "--file", $scriptPath
        ) `
        -WorkingDirectory $RunDirectory `
        -Environment $processEnvironment `
        -ProcessTimeoutSeconds $TimeoutSeconds
    $expectedStatesPresent = $result.stderr -match '42703' -and $result.stderr -match '42P01'
    $passed = -not $result.timedOut -and $result.exitCode -eq 0 -and $expectedStatesPresent
    return [ordered]@{
        client = "psql"
        status = if ($passed) { "passed" } else { "failed" }
        caseIds = $caseIds
        durationMs = $result.durationMs
        exitCode = $result.exitCode
        timedOut = $result.timedOut
        diagnostic = if ($passed) {
            $null
        } else {
            Protect-Diagnostic -Value $result.stderr
        }
    }
}

function Invoke-JavaAdapter {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ClientId,

        [Parameter(Mandatory = $true)]
        [string]$SourcePath,

        [Parameter(Mandatory = $true)]
        [string]$ClassName,

        [Parameter(Mandatory = $true)]
        [string]$Classpath,

        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$ConnectionEnvironment,

        [Parameter(Mandatory = $true)]
        [string]$RunDirectory
    )

    $classes = Join-Path $RunDirectory ("classes-" + $ClientId)
    New-Item -ItemType Directory -Path $classes | Out-Null
    $compile = Invoke-BoundedProcess `
        -FilePath $runtimePaths["javac"] `
        -Arguments @("-encoding", "UTF-8", "-proc:none", "-cp", $Classpath, "-d", $classes, $SourcePath) `
        -WorkingDirectory $RunDirectory `
        -ProcessTimeoutSeconds $TimeoutSeconds
    if ($compile.timedOut -or $compile.exitCode -ne 0) {
        return [ordered]@{
            client = $ClientId
            status = "failed"
            phase = "compile"
            durationMs = $compile.durationMs
            exitCode = $compile.exitCode
            timedOut = $compile.timedOut
            diagnostic = Protect-Diagnostic -Value "$($compile.stdout)`n$($compile.stderr)"
        }
    }
    $runtimeClasspath = $classes + [IO.Path]::PathSeparator + $Classpath
    $run = Invoke-BoundedProcess `
        -FilePath $runtimePaths["java"] `
        -Arguments @("-cp", $runtimeClasspath, $ClassName) `
        -WorkingDirectory $RunDirectory `
        -Environment $ConnectionEnvironment `
        -ProcessTimeoutSeconds $TimeoutSeconds
    $adapterEvidence = $null
    if (-not [string]::IsNullOrWhiteSpace($run.stdout)) {
        try {
            $adapterEvidence = $run.stdout | ConvertFrom-Json
        } catch {
            $adapterEvidence = $null
        }
    }
    $passed = -not $run.timedOut -and $run.exitCode -eq 0 -and
        $null -ne $adapterEvidence -and $adapterEvidence.status -eq "completed"
    return [ordered]@{
        client = $ClientId
        status = if ($passed) { "passed" } else { "failed" }
        phase = "execute"
        durationMs = $run.durationMs
        exitCode = $run.exitCode
        timedOut = $run.timedOut
        adapterEvidence = $adapterEvidence
        diagnostic = Protect-Diagnostic -Value $run.stderr
    }
}

function Invoke-ClientRuns {
    param(
        [object]$Corpus,
        [string[]]$SelectedClients,
        [System.Collections.IDictionary]$ConnectionEnvironment,
        [string]$RunDirectory
    )

    $results = @()
    foreach ($clientId in $SelectedClients) {
        switch ($clientId) {
            "psql" {
                $results += Invoke-PsqlRun `
                    -Corpus $Corpus `
                    -ConnectionEnvironment $ConnectionEnvironment `
                    -RunDirectory $RunDirectory
            }
            "pgjdbc" {
                $results += Invoke-JavaAdapter `
                    -ClientId "pgjdbc" `
                    -SourcePath $pgJdbcSource `
                    -ClassName "PgJdbcCompat" `
                    -Classpath $runtimePaths["pgjdbc"] `
                    -ConnectionEnvironment $ConnectionEnvironment `
                    -RunDirectory $RunDirectory
            }
            "hibernate" {
                $results += Invoke-JavaAdapter `
                    -ClientId "hibernate" `
                    -SourcePath $hibernateSource `
                    -ClassName "HibernateCompat" `
                    -Classpath $runtimePaths["hibernateClasspath"] `
                    -ConnectionEnvironment $ConnectionEnvironment `
                    -RunDirectory $RunDirectory
            }
            "datagrip" {
                $results += [ordered]@{
                    client = "datagrip"
                    status = "not_run_manual"
                    diagnostic = "The runner does not launch or automate the DataGrip UI. Follow the bounded manual checklist."
                }
            }
        }
    }
    return $results
}

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
