# ADR-0009: SQL Optimizer Passes

## Status

Accepted

## Context

The Operation Tree → SQL compilation produces nested subqueries that are often unnecessary. For example:

```python
con.table("users").filter(col("age") > 30).filter(col("active") == True)
```

Without optimization, this generates:

```sql
SELECT * FROM (SELECT * FROM users WHERE age > 30) WHERE active = TRUE
```

Which should be simplified to:

```sql
SELECT * FROM users WHERE age > 30 AND active = TRUE
```

## Decision

Implement two optimization passes that operate on the SQLGlot AST after compilation:

### Pass 1: Remove Unnecessary Subqueries

Collapse `SELECT * FROM (inner_query)` wrappers when the outer SELECT adds no columns, expressions, or grouping — it only wraps the inner query for no reason.

### Pass 2: Merge Consecutive Filters

When an outer query's only addition is a WHERE clause, and the inner query also has a WHERE clause, merge them with AND:

```sql
-- Before
SELECT * FROM (SELECT * FROM users WHERE age > 30) AS _t0 WHERE active = TRUE
-- After
SELECT * FROM users WHERE age > 30 AND active = TRUE
```

### Bug Fix (PR #14)

The initial implementation used SQLGlot's `.find()` method to locate WHERE clauses, which recursively descends into subqueries. This caused a filter+select combination to lose the WHERE clause. Fixed by using `.args` for direct clause access instead of recursive search.

## Consequences

- Generated SQL is cleaner and more readable via `show_query()`.
- Fewer nested subqueries improve query plan readability in database EXPLAIN output.
- The optimizer is tested independently with unit tests that verify AST transformations.
- Future passes (e.g., predicate pushdown, projection pruning) can be added to the same pipeline.
