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

## Updated (2026-05-30) — race resolution

The original `IF DB_ID(...) IS NULL CREATE DATABASE` form is itself a
TOCTOU: two callers can both observe `NULL` and one will fail when the
engine reaches the second CREATE. Add to that two more race sites:

1. **Concurrent CREATE.** When several threads with
   `create_if_missing=True` race to open a Connection to the same
   not-yet-existing database, all but one of them hits an error from
   the master CREATE step.
2. **DROP between master step and target connect.** The auto-create
   helper opens master, runs the CREATE, closes master, then opens
   the target. A competing writer that drops the database in between
   surfaces as a "Cannot open database" (4060/4063) error on the
   second connect.

Both became reachable in practice once ADR-0019's per-thread
connection cache made concurrent `Connection` creation a supported
mode.

### Decision

**Outcome-based detection for the master CREATE step.** Instead of
matching error numbers from the failed CREATE (empirically pymssql
surfaces the race as either error 1801 *or* the generic
`(0, b'Unknown error')` DB-Lib wrapper, so a code-based filter is not
reliable), the helper now:

1. Queries `DB_ID(<name>)` first; if the database already exists,
   skip CREATE entirely.
2. Issues `IF DB_ID(...) IS NULL CREATE DATABASE [...]`.
3. If CREATE raises, re-query `DB_ID`; if the database now exists,
   treat the operation as successful (some other writer won the
   race). If it still does not exist, re-raise — the failure was not
   a race.

Each `DB_ID` lookup and the CREATE itself use a fresh master
connection so a failed CREATE on one connection cannot leave the next
cursor in a bad state.

**One-shot retry for the target connect.** If the follow-up connect to
the target database fails with the SQL Server "cannot open database"
codes (4060 or 4063) and `create_if_missing=True`, the helper invokes
the master CREATE step once more and retries the connect. Past that
retry the original error propagates.

### Consequences

- Concurrent `pdb.connect(..., create_if_missing=True)` calls against
  the same not-yet-existing database all succeed (verified by
  `tests/integration/test_sqlserver_create_race.py`, which spawns 8
  threads racing to create the same DB).
- The `_is_db_already_exists` (error 1801) helper introduced earlier
  in the same branch is unused once detection moved to outcome-based
  and was removed; `_is_db_not_found` (4060/4063) stays because the
  follow-up connect still inspects the error code to decide whether
  to retry.
- Cost: the master step now opens up to three short-lived master
  connections per `_ensure_database_exists` call (one pre-check, one
  CREATE, one post-check on failure). For the `create_if_missing` path
  that only runs once per per-thread connection initialisation, the
  overhead is negligible.
- The earlier code-matching design caught the typical race but missed
  the "Unknown error" path — see the integration test which became
  flaky (≈30% failure) under 8-thread concurrency before the switch
  and went to 10/10 passing after.
