# OrdaDB product acceptance

This directory is the tracked, versioned source for truthful Windows x64
product acceptance. It is deliberately separate from generated evidence below
`target/product-acceptance/`.

## Status vocabulary

- `passed`: the runtime or reference evidence required by that row exists and
  passed.
- `regressionOnly`: OrdaDB regression tests passed, but the capability has not
  been executed against the pinned external reference. This is not a
  PostgreSQL conformance pass.
- `unsupported`: the boundary is deliberately rejected and documented.
- `notRunMissingInputs`: a required isolated endpoint, client, TLS input,
  credential, OCI installation, or model input is absent.
- `notRunManual`: the case requires manual UI/elevation evidence that has not
  been performed.
- `resourceBlocked`: the exact gate failed a resource preflight before
  allocating its target workload.
- `notApplicable`: the suite does not apply to the row.

Only a `passed` row may use `claimLevel: referenceConformant`. Static JSON
validation, tool presence, a successful preflight, local unit tests, or a
smaller workload never promote a row.

## Sources

- `product-acceptance.v1.json` owns product suites, external prerequisites,
  scale requirements, artifact inventory, and evidence references.
- `postgres18-conformance.v1.json` owns native capability-level PostgreSQL 18
  status and explicit unsupported boundaries.
- `performance-evidence.v1.json` preserves the paired performance and hard
  resource measurements from the owning milestones.
- `crates/ordadb-server/tests/client_compat/` remains authoritative for the
  pinned psql, pgJDBC, DataGrip, Hibernate and SQL replay baseline.

## Validation

Offline validation does not connect to a service or launch a client:

```powershell
pnpm acceptance:validate:x64
```

Preflight records only platform/resource observations and the names of missing
inputs. It never records their values. A blocked preflight writes evidence and
returns nonzero by design:

```powershell
pnpm acceptance:preflight:x64
```

Artifact mode validates an already unpacked product tree and bundle tree. It
does not install or execute anything:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/test-product-acceptance.ps1 `
  -Mode Artifacts `
  -ProductRoot target/product-acceptance/unpacked `
  -BundleRoot target/package-windows-x64/x86_64-pc-windows-msvc/release/bundle
```

Generated evidence is create-only, bounded, redacted and atomically published
under `target/product-acceptance/evidence/`.

## External and manual gates

The validator never converts prerequisites into execution. Actual external
runs remain owned by:

- `scripts/test_pg18_clients.ps1 -Mode Run` for pinned automated clients;
  DataGrip remains manual.
- `scripts/test-connectors-real-windows-x64.ps1` for all nine real connector
  matrices.
- `scripts/run-final-scale.ps1 -Profile Full -ConfirmFullScale -RetainData`
  for the fixed 20 GiB / 10M-row / 32-connection gate.

These gates must use caller-created isolated services and must not reuse
production databases, installed OrdaDB ProgramData, or production credentials.
