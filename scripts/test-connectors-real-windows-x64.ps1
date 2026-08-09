$ErrorActionPreference = "Stop"

$target = "x86_64-pc-windows-msvc"
if (-not $IsWindows -or [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne "X64") {
    throw "The real connector matrix requires Windows x64"
}

$required = @(
    "ORDADB_TEST_POSTGRESQL_HOST",
    "ORDADB_TEST_POSTGRESQL_USER",
    "ORDADB_TEST_POSTGRESQL_PASSWORD",
    "ORDADB_TEST_POSTGRESQL_TLS",
    "ORDADB_TEST_MYSQL_HOST",
    "ORDADB_TEST_MYSQL_USER",
    "ORDADB_TEST_MYSQL_PASSWORD",
    "ORDADB_TEST_MYSQL_TLS",
    "ORDADB_TEST_SQL_SERVER_HOST",
    "ORDADB_TEST_SQL_SERVER_USER",
    "ORDADB_TEST_SQL_SERVER_PASSWORD",
    "ORDADB_TEST_SQL_SERVER_TLS",
    "ORDADB_TEST_MONGODB_HOST",
    "ORDADB_TEST_MONGODB_DATABASE",
    "ORDADB_TEST_MONGODB_USER",
    "ORDADB_TEST_MONGODB_PASSWORD",
    "ORDADB_TEST_MONGODB_TLS",
    "ORDADB_TEST_REDIS_HOST",
    "ORDADB_TEST_REDIS_PASSWORD",
    "ORDADB_TEST_REDIS_TLS",
    "ORDADB_TEST_REDIS_MODE",
    "ORDADB_TEST_MARIADB_HOST",
    "ORDADB_TEST_MARIADB_USER",
    "ORDADB_TEST_MARIADB_PASSWORD",
    "ORDADB_TEST_MARIADB_TLS",
    "ORDADB_TEST_CLICKHOUSE_HOST",
    "ORDADB_TEST_CLICKHOUSE_USER",
    "ORDADB_TEST_CLICKHOUSE_PASSWORD",
    "ORDADB_TEST_CLICKHOUSE_TLS",
    "ORDADB_TEST_ORACLE_HOST",
    "ORDADB_TEST_ORACLE_SERVICE",
    "ORDADB_TEST_ORACLE_USERNAME",
    "ORDADB_TEST_ORACLE_PASSWORD",
    "ORDADB_TEST_ORACLE_CLIENT_DIR"
)
$missing = $required | Where-Object { -not [Environment]::GetEnvironmentVariable($_) }
if ($missing) {
    throw "Real connector matrix variables are missing: $($missing -join ', ')"
}

$tlsModes = @{
    "ORDADB_TEST_POSTGRESQL_TLS" = @("require", "verifyFull")
    "ORDADB_TEST_MYSQL_TLS" = @("require", "verifyCa", "verifyFull")
    "ORDADB_TEST_SQL_SERVER_TLS" = @("require", "verifyFull")
    "ORDADB_TEST_MONGODB_TLS" = @("require", "verifyFull")
    "ORDADB_TEST_REDIS_TLS" = @("require", "verifyFull")
    "ORDADB_TEST_MARIADB_TLS" = @("require", "verifyCa", "verifyFull")
    "ORDADB_TEST_CLICKHOUSE_TLS" = @("require", "verifyCa", "verifyFull")
}
foreach ($entry in $tlsModes.GetEnumerator()) {
    $value = [Environment]::GetEnvironmentVariable($entry.Key)
    if ($value -notin $entry.Value) {
        throw "$($entry.Key) must enforce TLS; received an unsupported mode"
    }
}

$redisMode = [Environment]::GetEnvironmentVariable("ORDADB_TEST_REDIS_MODE")
if ($redisMode -notin @("standalone", "cluster")) {
    throw "ORDADB_TEST_REDIS_MODE must be standalone or cluster"
}

$oracleClientDirectory = [Environment]::GetEnvironmentVariable("ORDADB_TEST_ORACLE_CLIENT_DIR")
if (-not (Test-Path -LiteralPath $oracleClientDirectory -PathType Container)) {
    throw "The required Oracle Instant Client directory is unavailable"
}
$oracleClientDll = Join-Path $oracleClientDirectory "oci.dll"
if (-not (Test-Path -LiteralPath $oracleClientDll -PathType Leaf)) {
    throw "The required Oracle Instant Client oci.dll is unavailable"
}
$stream = [System.IO.File]::OpenRead($oracleClientDll)
try {
    $reader = [System.IO.BinaryReader]::new($stream)
    if ($reader.ReadUInt16() -ne 0x5A4D) {
        throw "Oracle Instant Client oci.dll is not a PE image"
    }
    $stream.Position = 0x3C
    $peOffset = $reader.ReadUInt32()
    if ($peOffset -gt ($stream.Length - 6)) {
        throw "Oracle Instant Client oci.dll has an invalid PE header"
    }
    $stream.Position = $peOffset + 4
    if ($reader.ReadUInt16() -ne 0x8664) {
        throw "Oracle Instant Client oci.dll must be AMD64"
    }
}
finally {
    $stream.Dispose()
}
$env:PATH = "$oracleClientDirectory;$env:PATH"

$env:ORDADB_REQUIRE_REAL_CONNECTOR_TESTS = "1"
cargo test --locked `
    --package ordadb-connector-postgresql `
    --package ordadb-connector-mysql `
    --package ordadb-connector-sqlite `
    --package ordadb-connector-sql-server `
    --package ordadb-connector-mongodb `
    --package ordadb-connector-redis `
    --package ordadb-connector-mariadb `
    --package ordadb-connector-clickhouse `
    --package ordadb-connector-oracle `
    --target $target `
    real_ `
    -- --nocapture
if ($LASTEXITCODE -ne 0) {
    throw "The real Windows x64 connector matrix failed"
}

Write-Output "All nine real connector matrices passed on Windows x64"
