# ADR-0007: CI Pipeline — 3-Stage Architecture

## Status

Accepted (partially implemented)

## Context

The CI pipeline needs to validate code quality, unit tests, and integration tests across multiple database backends. Running all checks in a single job would be slow and make failure diagnosis difficult.

## Decision

Adopt a 3-stage pipeline with early exit:

```
Stage 1: quality (lint + type check)
  → Fail here = no point running tests
Stage 2: unit-test (needs: quality)
  → Python 3.10 + 3.13 matrix
  → Expr AST, Compiler, Optimizer tests (no DB required)
Stage 3: integration-test (needs: unit-test)
  → Python 3.13 only
  → Per-backend matrix with fail-fast: false
```

### Key Design Decisions

1. **Python version matrix limited to unit tests** — Integration test failures from Python version differences are rare (driver compatibility issues surface at unit level). This reduces jobs from 20 to 12.
2. **`fail-fast: false` for integration** — One backend's failure should not cancel others. Each backend is independent.
3. **Per-backend Docker services** — Each integration job starts only the container it needs, avoiding resource waste.

### Current Implementation vs. Design

The CI design document specified 10 backends. The current implementation covers 6:

| Backend | Status | Reason |
|---------|--------|--------|
| PostgreSQL | Implemented | Core target |
| MySQL | Implemented | Core target |
| SQL Server | Implemented | Core target |
| DuckDB | Implemented | In-memory, no container |
| SQLite | Implemented | In-memory, no container |
| BigQuery | Implemented | Emulator with limitations |
| Snowflake | Not yet | Emulator fidelity concerns |
| Redshift | Not yet | PostgreSQL proxy approach |
| Azure Synapse | Not yet | SQL Edge proxy approach |
| Databricks | Not yet | Spark Thrift Server approach |

The remaining 4 backends are deferred due to low emulator fidelity (documented in CI design Section 10). They will be added when real-environment scheduled tests are implemented.

## Consequences

- Fast feedback: quality issues caught in ~1 min before any DB containers start.
- Clear failure isolation: a MySQL-specific bug doesn't block PostgreSQL test results.
- Scalable: adding a new backend is a matrix entry + optional docker-compose service.
