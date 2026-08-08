# PostgreSQL 18 client compatibility baseline

This directory is a versioned, non-pass baseline for Windows AMD64 client
acceptance. `capability_matrix.v1.json` pins the client versions, hashes,
cases, and current status. `sql_corpus.v1.json` owns the bounded SQL/protocol
replay fixtures and marks every absent PostgreSQL 18 reference result.

No real client, UI, endpoint, certificate, or credential was used to create
this baseline. In particular:

- psql 18.0 is absent and is recorded as missing;
- the pgJDBC 42.7.10 JAR is present but was not executed;
- DataGrip 2026.1 build DB-261.22158.299 is present but was not launched;
- Hibernate 6.6.29.Final is present, but its pinned Byte Buddy 1.15.11 runtime
  dependency is missing and the ORM adapter was not executed.

The runner never converts tool presence into a passed client case.

## Offline validation

The static path parses and validates both JSON files, enforces their bounds
and status vocabulary, checks that every matrix corpus reference resolves, and
writes redacted atomic evidence without probing a server or launching a
client:

```powershell
pwsh -NoProfile -File scripts/test_pg18_clients.ps1 -Mode Validate
```

`Preflight` validates Windows AMD64, the selected pinned artifact and version,
and all isolated TLS connection inputs, but does not connect. `Run` is the only
mode that may invoke an automated client. Both modes require these environment
variable names; their values are never printed or written to evidence:

- `ORDADB_PG18_HOST`
- `ORDADB_PG18_PORT`
- `ORDADB_PG18_DATABASE` (must match `ordadb_compat_*`)
- `ORDADB_PG18_USER`
- `ORDADB_PG18_PASSWORD`
- `ORDADB_PG18_SSLMODE` (must be `verify-full`)
- `ORDADB_PG18_ROOT_CERT`
- `ORDADB_PG18_ISOLATED_CONFIRM` (must be `YES`)

Optional absolute tool overrides are
`ORDADB_PG18_PSQL_PATH`, `ORDADB_PG18_PGJDBC_JAR`,
`ORDADB_PG18_DATAGRIP_PATH`, `ORDADB_PG18_HIBERNATE_CORE_JAR`,
`ORDADB_PG18_JAVA_PATH`, `ORDADB_PG18_JAVAC_PATH`, and
`ORDADB_PG18_M2_ROOT`. Passwords must remain in the environment; do not put
them in command arguments, shell history, connection URLs, or evidence paths.

## DataGrip manual smoke

The DataGrip row remains `not_run_manual` until a person performs this flow
against a caller-created isolated database:

1. Preflight the pinned DataGrip executable with `-Mode Preflight -Client DataGrip`.
2. Create a PostgreSQL datasource with verify-full TLS and SCRAM using the
   protected DataGrip credential field; do not paste credentials into SQL.
3. Refresh schemas, tables, columns, indexes, constraints, routines, and roles.
4. Run the session, catalog, DDL/CRUD, transaction/savepoint, cancellation,
   and simple-error cases from `sql_corpus.v1.json`.
5. Confirm cancellation returns SQLSTATE `57014`, failed work is recoverable,
   and no role verifier or credential appears in metadata or diagnostics.
6. Record the pinned build, case IDs, timestamps, statuses, and bounded
   redacted diagnostics. Screenshots must exclude connection fields and row
   values. Do not edit the baseline to `passed` without executed evidence.

The runner does not automate or silently launch the DataGrip UI. Selecting
DataGrip in `Run` writes an explicit manual-not-run result and exits nonzero.
