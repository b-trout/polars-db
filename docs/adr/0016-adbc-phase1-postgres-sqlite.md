# ADR-0016: ADBC migration — Phase 1 (PostgreSQL + SQLite)

## Status

Accepted (2026-04-18)

## Context

Code review #7 identified the
`cursor.fetchall() → Python dict loop → pa.table(dict)` path in the
PostgreSQL, MySQL, SQL Server, and SQLite backends as a meaningful
inefficiency.  Every row travels through the Python interpreter and is
appended into a per-column `list[object]`, which is then handed to
`pa.table(...)` for type inference.  The result is an extra copy of the
entire result set and unstable typing when a column contains only
`NULL` (the inferred Arrow type collapses to `null`).

An [ADBC](https://arrow.apache.org/adbc/)-based path sidesteps both
problems: the driver returns a `pyarrow.Table` directly, with column
types drawn from the driver's Arrow schema.  The
`Backend.execute_sql(sql, conn_str) -> pa.Table` contract is already
in place, so individual backends can be swapped independently as their
ADBC drivers mature.

ADBC driver maturity as of 2026-04:

| Backend     | ADBC driver                     | Status       | Action         |
|-------------|---------------------------------|--------------|----------------|
| PostgreSQL  | `adbc-driver-postgresql` 1.10+  | Stable       | Migrate        |
| SQLite      | `adbc-driver-sqlite` 1.10+      | Stable       | Migrate        |
| MySQL       | ADBC Driver Foundry             | Beta, no PyPI wheel | Defer |
| SQL Server  | ADBC Driver Foundry             | Beta, no PyPI wheel | Defer |
| DuckDB      | N/A                             | Already Arrow-native (`fetch_arrow_table()`) | Unchanged |
| BigQuery    | `adbc-driver-bigquery` 1.10+    | Experimental | Unchanged (already uses Storage Read API / Arrow via `to_arrow()`) |

## Decision

Migrate **PostgreSQL and SQLite only** to ADBC in Phase 1.  Leave MySQL,
SQL Server, DuckDB, and BigQuery on their current implementations until
their ADBC drivers stabilise and ship on PyPI.

Concretely:

* `PostgresBackend` now opens an `adbc_driver_postgresql.dbapi`
  connection with `autocommit=True` (matching the previous per-call
  `conn.commit()` behaviour under `psycopg2`) and returns
  `cursor.fetch_arrow_table()` directly.
* `SQLiteBackend` now opens an `adbc_driver_sqlite.dbapi` connection.
  The project's `sqlite:///…` URI form is parsed into a bare file path
  (or the `:memory:` literal) before being handed to the driver.
* The `postgres` extras pull `adbc-driver-postgresql` and
  `adbc-driver-manager`; `psycopg2-binary` is removed.
* The new `sqlite` extras pull `adbc-driver-sqlite` and
  `adbc-driver-manager`.  The stdlib `sqlite3` module is no longer
  imported by the backend.

## Consequences

* Result fetching on PostgreSQL and SQLite is Arrow-native, eliminating
  the per-row Python copy that showed up in code review #7.  Columns
  carry the driver-inferred Arrow type, so `NULL`-only columns no
  longer collapse to `null`.
* `psycopg2-binary` is dropped as a dependency.  Downstream users who
  relied on `PostgresBackend` re-exposing a psycopg2 connection object
  must migrate to the ADBC connection (the backend's internal
  `_conn`/`_conn_str` fields remain private).
* Backends are no longer uniform in their transport layer: three paths
  now coexist — ADBC (Postgres, SQLite), native Arrow (DuckDB,
  BigQuery), and native-driver-plus-dict-loop (MySQL, SQL Server).  The
  `execute_sql` contract hides this from the rest of the codebase, so
  higher-level code is unaffected.
* The opt-in `sqlite` extra is new.  Projects that previously depended
  on the implicit stdlib `sqlite3` path now need to install the extra
  (`pip install polars-db[sqlite]`) to use the SQLite backend.
* Future phases can migrate MySQL and SQL Server once Apache-official
  ADBC drivers ship PyPI wheels, and re-evaluate BigQuery once
  `adbc-driver-bigquery` graduates from Experimental.
