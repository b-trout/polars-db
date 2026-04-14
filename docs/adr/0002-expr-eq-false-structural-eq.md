# ADR-0002: `eq=False` and `_structural_eq()` for Expr/Op Nodes

## Status

Accepted

## Context

Polars' `col("age") == 30` returns a lazy expression (not `True`/`False`). To maintain API compatibility, polars-db's `Expr.__eq__` must return a `BinaryExpr`, not a `bool`.

Python's `@dataclass(frozen=True)` auto-generates `__eq__` (returning `bool`) and `__hash__`. These conflict with the Polars API requirement:

- Auto-generated `__eq__` returns `bool`, but we need `BinaryExpr`.
- If `__eq__` returns a non-bool, `__hash__` becomes inconsistent.
- `Op` nodes contain `Expr` fields, so the same problem propagates to `Op.__eq__`.

## Decision

Use `@dataclass(frozen=True, eq=False)` for both `Expr` and `Op` base classes.

- `__eq__` is manually overridden on `Expr` to return `BinaryExpr`.
- `__hash__` is not generated — `Expr` and `Op` are intentionally unhashable (same as Polars).
- For internal/test use, provide `_structural_eq()` method that recursively compares all fields.
- A standalone `_deep_eq()` function handles nested containers (`tuple[Expr, ...]`, `list`, primitives).

```python
@dataclass(frozen=True, eq=False)
class Expr:
    def __eq__(self, other) -> BinaryExpr:  # Polars API
        return BinaryExpr(op="==", left=self, right=_ensure_expr(other))

    def _structural_eq(self, other: "Expr") -> bool:  # test/internal
        if type(self) is not type(other):
            return False
        return all(_deep_eq(getattr(self, f.name), getattr(other, f.name))
                   for f in fields(self))
```

## Consequences

- Polars API compatibility is preserved — `col("x") == 1` works as expected.
- `Expr` cannot be used as dict keys or in sets (unhashable).
- Tests use `_structural_eq()` instead of `==` for assertion comparisons.
- The same pattern is applied to `Op` nodes for consistency.
