# ADR-0004: Native Database Drivers over ConnectorX

## Status

Accepted (supersedes initial design)

## Context

The original design specified ConnectorX (`connectorx.read_sql()`) as the universal query execution engine. ConnectorX provides fast Arrow-native data transfer for SELECT queries.

During implementation, critical limitations were discovered:

1. **SELECT-only** — ConnectorX only supports SELECT queries. DDL (`CREATE TABLE`) and DML (`INSERT`, `UPDATE`, `DELETE`) raise `RuntimeError`.
2. **No DuckDB support** — ConnectorX does not support DuckDB connections.
3. **No in-memory SQLite** — ConnectorX cannot handle `sqlite:///:memory:` URIs.
4. **No connection persistence** — ConnectorX creates a new connection per query, preventing transaction control.

## Decision

Replace ConnectorX entirely with native database drivers for all backends:

| Backend | Driver | Rationale |
|---------|--------|-----------|
| PostgreSQL | `psycopg2-binary` | User preference; widely adopted |
| MySQL | `pymysql` + `cryptography` | Pure Python; no C build dependency; cryptography needed for MySQL 8 `caching_sha2_password` auth |
| SQL Server | `pymssql` | FreeTDS bundled; no ODBC driver manager required |
| DuckDB | `duckdb` | Native package with Arrow output |
| SQLite | `sqlite3` (stdlib) | No external dependency |
| BigQuery | `google-cloud-bigquery` | Official SDK with `.to_arrow()` support |

All backends follow the same pattern: persistent connection, cursor-based execution, Arrow table construction from `cursor.description` + `fetchall()`.

### Migration Path

The migration was incremental across three PRs:

1. **PR #14**: DuckDB and SQLite rewritten to native drivers (ConnectorX fundamentally incompatible).
2. **PR #17**: PostgreSQL rewritten to psycopg2 (DDL/DML support needed).
3. **PR #18**: MySQL, SQL Server, BigQuery rewritten; ConnectorX removed from dependencies; `ConnectorxBackend` base class deleted.

## Consequences

- All backends support DDL, DML, and SELECT uniformly.
- Each backend manages its own persistent connection with proper transaction control.
- ConnectorX's performance advantage for large SELECT queries is lost, but correctness and full SQL support take priority.
- Backend-specific quirks (e.g., pymssql's `commit()` invalidating result sets — see ADR-0012) must be handled individually.
