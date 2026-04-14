# ADR-0003: SQLGlot as Intermediate SQL Representation

## Status

Accepted

## Context

The compiler needs to produce SQL strings for multiple database dialects (PostgreSQL, MySQL, DuckDB, SQLite, SQL Server, BigQuery). Options:

1. **Direct string interpolation** — Simple but no AST-level optimization, error-prone escaping, dialect differences handled via string templates.
2. **SQLAlchemy Core** — Heavy dependency, its own expression system would duplicate polars-db's Expr AST.
3. **SQLGlot** — Lightweight Python library with AST representation, 30+ dialect support, pretty printing, and programmatic AST manipulation.

## Decision

Use SQLGlot as the intermediate representation between the Op tree and final SQL strings.

- `ExprCompiler` translates `Expr` nodes → `sqlglot.expressions` nodes.
- `QueryCompiler` translates `Op` trees → `sqlglot.expressions.Select` statements.
- `Optimizer` operates on the SQLGlot AST (subquery removal, filter merging).
- `Backend.render(ast)` calls `ast.sql(dialect=...)` for dialect-specific output.

## Consequences

- Dialect differences are handled by SQLGlot's built-in transpilation (e.g., `LIMIT` vs `TOP`, `ILIKE` availability).
- The Optimizer can inspect and transform the AST before rendering, enabling passes like unnecessary subquery elimination.
- Trade-off: SQLGlot uses dynamic methods (`.find()`, `.args`) that type checkers cannot resolve. Mitigated by setting `ty` rules to `unresolved-attribute = "warn"`.
- SQLGlot is a runtime dependency, adding ~2MB to the install.
