# ADR-0013: SQL Server Automatic Database Creation

## Status

Accepted

## Context

Unlike PostgreSQL and MySQL, SQL Server does not auto-create databases. When connecting with `mssql://sa:password@localhost:1433/testdb`, the connection fails if `testdb` does not exist.

The docker-compose SQL Server service starts with only system databases (master, tempdb, model, msdb). The test database must be created before the first connection.

## Decision

In `SQLServerBackend._create_connection()`, first connect to `master`, create the database if it does not exist, then reconnect to the target database:

```python
@staticmethod
def _create_connection(conn_str):
    import pymssql
    parsed = urlparse(conn_str)
    server, port, user, password = ...
    database = parsed.path.lstrip("/")

    # Ensure target database exists
    master = pymssql.connect(server=server, port=port, user=user,
                             password=password, database="master")
    master.autocommit(True)
    cursor = master.cursor()
    cursor.execute(f"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]")
    master.close()

    return pymssql.connect(server=server, port=port, user=user,
                           password=password, database=database)
```

## Consequences

- SQL Server integration tests work out of the box without manual database setup.
- The `master` connection is short-lived (created, used for one DDL, closed).
- This runs on every new connection, but `IF DB_ID(...) IS NULL` is a no-op after the first call.
- The approach is similar to how ORMs like Django handle database creation for testing.

## Updated (2026-04-18)

The auto-create behavior is now **opt-in**. `pdb.connect(...)` defaults
`create_if_missing=False` and skips the `master` connection / `CREATE
DATABASE` step entirely. Test suites that require the test database to be
bootstrapped (including this repo's own `tests/conftest.py`) must pass
`create_if_missing=True` explicitly.

### Rationale

The original always-on behavior had two problems that outweighed the
test ergonomics benefit:

1. **Typo safety.** A production connection string with a typo'd
   database name would silently create a new empty database instead
   of failing loudly. With opt-in, the connection fails fast.
2. **Defense in depth alongside PR #37.** PR #37 added identifier
   validation to prevent injection via the `CREATE DATABASE [...]`
   path. Gating the auto-create behind an explicit flag shrinks the
   attack surface further — the DDL simply does not run unless the
   caller asks for it.

### Migration

- Test harnesses: pass `create_if_missing=True` to `pdb.connect(...)`.
- Production callers: no change (default `False` matches pre-existing
  PostgreSQL/MySQL behavior).
