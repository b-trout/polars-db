# ADR-0015: BigQuery SQL Snapshot Testing (Supersedes ADR-0008)

## Status

Accepted

## Context

ADR-0008 adopted `goccy/bigquery-emulator` for BigQuery integration testing, with workarounds for DML hangs. During CI pipeline implementation (ADR-0007), two additional issues surfaced:

1. **SELECT hangs** — The emulator hangs indefinitely on some SELECT queries involving subqueries, making integration tests unreliable.
2. **CI instability** — The BigQuery integration test job frequently times out or gets stuck in pending, blocking the entire pipeline.

R's `dbplyr` package faces the same problem with vendor DWH backends (BigQuery, Snowflake, Redshift). Their strategy:

- **SQL generation snapshot tests** — Verify generated SQL strings against expected output without executing against a real database.
- **CI tests only against open-source databases** — PostgreSQL, MySQL, SQLite, SQL Server run in containers.
- **Vendor DWH tests gated on credentials** — Only run locally or in scheduled workflows with real service accounts.

### Options Considered

1. **Fix emulator issues** — Investigate and patch the emulator. Rejected: upstream issues, DuckDB-based emulator has fundamental fidelity gaps.
2. **Use real BigQuery in CI** — Requires GCP project, service account secrets, incurs costs. Deferred to future scheduled workflow.
3. **SQL snapshot testing in unit tests** — Verify BigQuery dialect SQL generation at the compiler level. Adopted.

## Decision

Replace BigQuery emulator integration tests with SQL generation unit tests:

1. **Unit tests** (`tests/unit/test_compiler.py::TestBigQueryDialect`) verify that the QueryCompiler produces correct BigQuery SQL for all operation types (SELECT, WHERE, ORDER BY, GROUP BY, LIMIT, combined queries).
2. **CI integration-test matrix** excludes BigQuery — only PostgreSQL, MySQL, SQL Server, DuckDB, and SQLite run as integration tests.
3. **Emulator remains in docker-compose.yml** for optional local testing but is not used in CI.

### What is tested

| Layer | Coverage | How |
|-------|----------|-----|
| Op tree → BigQuery SQL | Full | Unit tests with `BigQueryBackend.render()` |
| BigQuery dialect specifics (NULLS LAST, identifiers) | Full | Unit test assertions |
| BigQuery Client connection | Not in CI | Optional local emulator or real BigQuery |
| BigQuery query execution | Not in CI | Future scheduled workflow with real credentials |

## Consequences

- CI is reliable and fast — no emulator timeout or hang issues.
- BigQuery SQL generation correctness is verified on every PR.
- Trade-off: query execution against BigQuery is not tested in CI. This mirrors dbplyr's approach and is acceptable because:
  - The SQL generation layer is where most bugs occur.
  - The `google-cloud-bigquery` SDK handles execution; we don't need to test Google's client.
  - Real BigQuery testing can be added later as a scheduled workflow with GCP credentials.
