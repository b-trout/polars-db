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
