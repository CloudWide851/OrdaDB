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
    "ORDADB_TEST_SQL_SERVER_TLS"
)
$missing = $required | Where-Object { -not [Environment]::GetEnvironmentVariable($_) }
if ($missing) {
    throw "Real connector matrix variables are missing: $($missing -join ', ')"
}

$tlsModes = @{
    "ORDADB_TEST_POSTGRESQL_TLS" = @("require", "verifyFull")
    "ORDADB_TEST_MYSQL_TLS" = @("require", "verifyCa", "verifyFull")
    "ORDADB_TEST_SQL_SERVER_TLS" = @("require", "verifyFull")
}
foreach ($entry in $tlsModes.GetEnumerator()) {
    $value = [Environment]::GetEnvironmentVariable($entry.Key)
    if ($value -notin $entry.Value) {
        throw "$($entry.Key) must enforce TLS; received an unsupported mode"
    }
}

$env:ORDADB_REQUIRE_REAL_CONNECTOR_TESTS = "1"
cargo test --locked `
    --package ordadb-connector-postgresql `
    --package ordadb-connector-mysql `
    --package ordadb-connector-sqlite `
    --package ordadb-connector-sql-server `
    --target $target `
    real_ `
    -- --nocapture
if ($LASTEXITCODE -ne 0) {
    throw "The real Windows x64 connector matrix failed"
}

Write-Output "PostgreSQL, MySQL, SQLite, and SQL Server real connector matrices passed on Windows x64"
