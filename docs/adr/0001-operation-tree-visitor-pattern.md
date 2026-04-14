# ADR-0001: Operation Tree + Visitor Pattern (dbplyr-style)

## Status

Accepted

## Context

polars-db needs to translate Polars-style API calls into SQL for multiple database backends. The core challenge is representing a chain of lazy operations (filter, select, sort, join, etc.) and converting them into optimized SQL at execution time.

Three approaches were considered:

1. **SQLAlchemy Core** — Mature SQL builder, but its API diverges significantly from Polars. Users would need to learn SQLAlchemy's expression system.
2. **Direct SQL string construction** — Simple but makes optimization passes (subquery elimination, filter merging) extremely difficult and error-prone.
3. **Operation Tree + Visitor (dbplyr-style)** — Each Polars operation creates an immutable tree node. At `collect()` time, a compiler (visitor) recursively traverses the tree to produce a SQLGlot AST.

## Decision

Adopt the Operation Tree + Visitor pattern, proven by R's dbplyr.

Each `LazyFrame` method appends a new `Op` node to an immutable tree. The tree is compiled only when `collect()` or `show_query()` is called. This deferred execution model enables:

- Optimization passes between tree construction and SQL generation
- Backend-agnostic tree representation with dialect-specific rendering
- Composable, testable compilation pipeline

### Data Flow

```
User Code (Polars-style API)
  → LazyFrame methods → Op tree (immutable nodes)
  → QueryCompiler (visitor) → SQLGlot AST
  → Optimizer (subquery removal, filter merge)
  → Backend.render(ast) → SQL string (dialect-specific)
  → Native driver execute → Arrow Table
  → polars.from_arrow() → polars.DataFrame
```

## Consequences

- Every Polars operation maps to a well-defined `Op` subclass, making the system easy to extend.
- The compiler can be tested in isolation (Op tree → SQL string) without database connections.
- The optimization layer can evolve independently of the API surface.
- Trade-off: deeply nested operations produce deeply nested subqueries that require optimization passes to flatten.
