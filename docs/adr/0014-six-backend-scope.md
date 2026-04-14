# ADR-0014: 6-Backend Scope vs. 10-Backend Design

## Status

Accepted

## Context

The CI design document specified 10 database backends: PostgreSQL, MySQL, SQL Server, DuckDB, SQLite, BigQuery, Snowflake, Redshift, Azure Synapse, and Databricks.

The design document itself noted emulator fidelity concerns (Section 10):

| Backend | Emulator | Fidelity | Known Gaps |
|---------|----------|----------|------------|
| Snowflake | snowflake-emulator (DuckDB-based) | Low-Medium | VARIANT type, QUALIFY clause, semi-structured data unsupported |
| Redshift | PostgreSQL proxy | Medium | DISTKEY/SORTKEY behavior, SUPER type not reproducible |
| Azure Synapse | Azure SQL Edge | Low-Medium | DISTRIBUTION/MPP behavior not reproducible |
| Databricks | Spark Thrift Server | Medium | Delta Lake features, Unity Catalog, PIVOT syntax differences |

## Decision

Implement and test 6 backends first: PostgreSQL, MySQL, SQL Server, DuckDB, SQLite, BigQuery.

Defer the remaining 4 (Snowflake, Redshift, Synapse, Databricks) until:

1. Real-environment scheduled tests are implemented (separate CI workflow).
2. Emulator fidelity improves enough for meaningful automated testing.
3. Users request specific backend support.

The 6 implemented backends cover the most common use cases:
- **Local development**: DuckDB, SQLite (in-memory, no container)
- **On-premise RDBMS**: PostgreSQL, MySQL, SQL Server (Docker containers)
- **Cloud DWH**: BigQuery (emulator with SQL generation focus)

## Consequences

- CI runs 6 backend integration tests instead of 10, reducing total CI time.
- The architecture supports adding backends easily (new `Backend` subclass + driver + docker-compose service).
- Cloud DWH backends (Snowflake, Redshift, Synapse, Databricks) will need real credentials for full validation, handled via scheduled workflows with secrets.
