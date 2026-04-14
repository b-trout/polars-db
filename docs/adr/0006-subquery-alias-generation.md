# ADR-0006: Auto-Generated Aliases for Derived Tables

## Status

Accepted

## Context

The SQL compiler wraps inner queries as subqueries in the FROM clause when operations like SELECT, GROUP BY, or RENAME are applied. The generated SQL looked like:

```sql
SELECT name, age FROM (SELECT * FROM users)
```

This is valid in PostgreSQL, DuckDB, and SQLite, but fails on MySQL and SQL Server which require all derived tables to have an alias:

```
ERROR 1248 (42000): Every derived table must have its own alias
```

The SQL standard also recommends aliases on derived tables.

Two approaches were considered:

1. **Backend-specific post-processing** — Add aliases only for MySQL/SQL Server dialects in a post-processing step.
2. **Universal alias generation** — Always add aliases to all subqueries, regardless of dialect.

## Decision

Add auto-generated aliases (`_t0`, `_t1`, ...) to all subqueries in `QueryCompiler._ensure_subquery()`:

```python
def _ensure_subquery(self, select: exp.Expression) -> exp.Subquery:
    alias = f"_t{self._subquery_counter}"
    self._subquery_counter += 1
    return select.subquery(alias)
```

## Consequences

- MySQL and SQL Server integration tests pass without dialect-specific workarounds.
- The generated SQL is valid across all 6 supported backends.
- The compiler has a single code path — no backend branching for alias handling.
- SQL output is slightly more verbose (extra `AS _t0` on every subquery), but this has no performance impact.
- One remaining SQL Server limitation: T-SQL forbids ORDER BY inside subqueries unless TOP/OFFSET is also specified. This is a separate issue tracked with an `xfail` marker in tests.
