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
        throw "A generated directory escaped its containment root."
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
    if (Test-Path -LiteralPath $resolvedPath) {
        throw "Client compatibility evidence is create-only and already exists."
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
