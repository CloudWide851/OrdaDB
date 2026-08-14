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
