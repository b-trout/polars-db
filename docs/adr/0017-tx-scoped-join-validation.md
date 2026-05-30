# ADR-0017: Transaction-scoped JOIN validation

## Status

Accepted (2026-05-30)

## Context

`LazyFrame.collect()` runs two SQL statements when a JOIN carries a
non-default `validate` setting (`"1:1"`, `"1:m"`, `"m:1"`):

1. A `JoinValidator` uniqueness probe
   (`SELECT key FROM left GROUP BY key HAVING COUNT(*) > 1 LIMIT 1`).
2. The main query.

Before this ADR these statements ran in **separate transactions**:

- `PostgresBackend` / `SQLiteBackend` used `autocommit=True` on their
  ADBC connections.
- `MySQLBackend` / `SQLServerBackend` called `conn.commit()` after every
  `execute_sql`.
- `DuckDBBackend` ran statements outside any explicit transaction.

That left a textbook TOCTOU gap: a concurrent INSERT between the probe
and the main query could violate the cardinality contract that
`validate=` is supposed to enforce, and `JoinValidationError` could be
raised for a duplicate that was already rolled back.

A second concern is that the validation contract has a backend-specific
ceiling. BigQuery's standard `client.query()` API does not provide
cross-job snapshot isolation; emulating it via multi-statement scripts
plus child-job navigation would push significant complexity into the
backend for marginal correctness benefit on an analytical DWH workload.

## Decision

Wrap the probe and the main query in a single backend transaction with
an isolation level strong enough to give both statements a consistent
snapshot.

### API

Add `Backend.transaction(conn_str)` returning an
`AbstractContextManager[None]`. Inside the block, all `execute_sql`
calls share a single connection and transaction; on block exit, the
backend commits, or rolls back if the body raised. Expose
`Connection.transaction()` as a thin delegate so that raw-SQL callers
who chain multiple `execute_raw` statements can opt into atomicity too.

`LazyFrame.collect()` opens the transaction only when there is at least
one validating JOIN in the tree; single-statement reads keep the
zero-tx fast path.

### Isolation levels

| Backend     | Open                                                   | Rationale                                          |
|-------------|--------------------------------------------------------|----------------------------------------------------|
| PostgreSQL  | `BEGIN ISOLATION LEVEL REPEATABLE READ` (autocommit off) | Default is READ COMMITTED — too weak for snapshot. |
| SQLite      | `BEGIN` (autocommit off)                               | Default isolation is serializable.                 |
| MySQL       | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION` | InnoDB default is already REPEATABLE READ; set explicitly so the contract is dialect-portable. |
| SQL Server  | `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION` | Default is READ COMMITTED — too weak.              |
| DuckDB      | `BEGIN TRANSACTION`                                    | Snapshot isolation by default.                     |
| BigQuery    | No-op                                                  | See below.                                         |

Backends with explicit per-statement `conn.commit()` (MySQL, SQL Server)
now read an `_in_tx` flag and skip the commit when set, so the outer
transaction is the only commit point.

### BigQuery: skip validation, emit warning

`BigQueryBackend.supports_atomic_validation` is `False`. When
`LazyFrame._run_validations()` is entered on a non-atomic backend, it
emits a `UserWarning` and returns without issuing the validation query.

We considered two alternatives and rejected both:

- **Multi-statement script + `BEGIN TRANSACTION` / `COMMIT TRANSACTION`.**
  BigQuery does support transactions inside a script, but the
  Python client surfaces a script as a parent job whose children
  must be enumerated with `client.list_jobs(parent_job=...)` and
  fetched one by one. The complexity is significant and the
  `execute_sql(sql, conn_str) -> pa.Table` contract would have to be
  extended to return multiple Arrow tables.
- **Run validation sequentially anyway (current behaviour).** Worse
  than skipping: it gives the caller a `JoinValidationError`-or-success
  signal that has no atomic relationship with the main query, which
  is a *false positive guarantee*. Callers who rely on validation
  should know it does not hold and either choose `validate="m:m"` or
  enforce uniqueness via a database UNIQUE constraint.

## Consequences

- JOINs with `validate=` now observe a consistent snapshot on five of
  six backends. Concurrent writers can no longer falsify the probe.
- `Connection.transaction()` is a public method that any raw-SQL caller
  can use to make a sequence of `execute_raw` calls atomic.
- BigQuery callers using `validate=` now see a warning instead of a
  silently-unreliable check. Tests that assert validation behaviour on
  BigQuery need to install a `warnings` filter or expect the skip.
- The `execute_sql` contract is unchanged; each backend reads the
  cached `_conn` and only the lifecycle of the underlying transaction
  changes. Custom backends that subclass `Backend` get the default
  no-op `transaction()` and `supports_atomic_validation = True`; they
  should override at least one if their driver cannot honour the
  contract.
- Performance impact on non-validating queries is zero (the wrapper
  is skipped). Validating queries pay one extra round trip (BEGIN +
  COMMIT) and the snapshot-isolation cost of their underlying engine,
  which for SELECT-only workloads is negligible.
- This ADR closes one of the four TOCTOU sites flagged in the
  2026-05-30 audit. Sister ADRs (0018 schema-cache TTL, 0013 update,
  0019 backend thread safety) address the others.

## Revision 2026-05-30 — implementation note: ADBC autocommit toggle is unreliable

The first implementation kept the ADBC connections in
``autocommit=True`` mode (matching the historical psycopg2 semantics)
and used ``conn.adbc_connection.set_autocommit(False)`` to enter the
tx, an explicit ``BEGIN ISOLATION LEVEL …`` / ``BEGIN`` to set
isolation, ``conn.commit()`` / ``rollback()`` on exit, and a
``set_autocommit(True)`` to restore. Integration CI surfaced two
problems with that design that the unit (mock) tests could not see:

1. The dbapi wrapper does not re-expose ``set_autocommit`` — the call
   must be routed through the low-level ``conn.adbc_connection``
   handle. The unit suite happily accepted ``conn.set_autocommit`` on a
   ``MagicMock``; the real driver raised ``AttributeError`` (the
   driving motivation for the *avoid mocks* feedback memory).
2. Even via the correct handle, the mid-session ``set_autocommit``
   toggle is not honoured uniformly across drivers. On SQLite ADBC,
   flipping autocommit off and then calling ``rollback()`` does **not**
   undo statements issued during that window (verified empirically:
   ``set_autocommit(False)`` followed by INSERT and ``rollback()``
   leaves the row visible). Postgres ADBC's behaviour was not
   confidently characterisable without a live server, so the same
   pattern was applied there for safety.

### Revised design

Each ADBC backend now opens its connection in DBAPI-default
``autocommit=False`` and ``execute_sql`` calls ``conn.commit()`` after
every statement when ``self._in_tx`` is false. This matches what
``MySQLBackend`` and ``SQLServerBackend`` already do and preserves the
historical "one statement = one transaction" externally-visible
semantics. ``transaction()`` simply flips ``_in_tx`` true, optionally
issues the isolation-level SET statement (Postgres only — SQLite
defaults to serializable), runs the body, and commits or rolls back at
the end. No autocommit toggling, no driver-specific quirk to depend on.

### Isolation level table (revised)

| Backend     | Open                                                   | Rationale                                          |
|-------------|--------------------------------------------------------|----------------------------------------------------|
| PostgreSQL  | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` then queries | Default is READ COMMITTED — too weak for snapshot. |
| SQLite      | _no isolation statement_                               | Default is serializable; queries run inside the implicit tx. |
| MySQL       | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION` | InnoDB default is already REPEATABLE READ; set explicitly so the contract is dialect-portable. |
| SQL Server  | `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION` | Default is READ COMMITTED — too weak.              |
| DuckDB      | `BEGIN TRANSACTION`                                    | Snapshot isolation by default.                     |
| BigQuery    | No-op                                                  | See above.                                         |

### Consequence

- The autocommit kwarg passed historically to
  ``adbc_pg.connect(conn_str, autocommit=True)`` is removed in favour of
  the DBAPI default. The unit assertion was relaxed accordingly.
- The contract surface (``Backend.transaction()`` returning a context
  manager, ``supports_atomic_validation``, ``Connection.transaction()``
  delegation) is unchanged. Only the internal mechanism changed.
- Driver-touching coverage now lives in
  ``tests/integration/test_transaction.py`` so the per-backend matrix
  exercises real BEGIN/COMMIT/ROLLBACK round-trips — see the
  *avoid mocks* memory entry for why this matters.
