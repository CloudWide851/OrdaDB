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
