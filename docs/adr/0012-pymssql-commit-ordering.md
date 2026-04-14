# ADR-0012: pymssql commit() / fetchall() Ordering

## Status

Accepted

## Context

During SQL Server integration testing, SELECT queries returned 0 rows despite successful INSERT operations. Investigation revealed that `pymssql` discards the result set when `conn.commit()` is called before `cursor.fetchall()`.

```python
# This returns empty results with pymssql:
cursor.execute("SELECT * FROM users")
conn.commit()       # ← discards result set
rows = cursor.fetchall()  # ← returns []

# This works correctly:
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()  # ← returns data
conn.commit()
```

Other drivers (psycopg2, pymysql) preserve the result set after `commit()`.

## Decision

In `SQLServerBackend.execute_sql()`, call `fetchall()` before `commit()`:

```python
def execute_sql(self, sql, conn_str):
    conn = self._get_connection(conn_str)
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    rows = cursor.fetchall() if columns else []
    conn.commit()  # ← after fetchall
    ...
```

Other backends retain the `commit()` → `fetchall()` order since their drivers are not affected.

## Consequences

- SQL Server SELECT queries correctly return data.
- The fix is isolated to `SQLServerBackend` — no changes to other backends.
- This is a pymssql-specific behavior, not documented in its official docs. Discovered empirically during integration testing.
