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
