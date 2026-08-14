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
