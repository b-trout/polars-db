"""Polars-compatible lazy query builder."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from polars_db.compiler.optimizer import Optimizer
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import ColExpr, Expr, _ensure_expr
from polars_db.ops import (
    DistinctOp,
    DropOp,
    FilterOp,
    GroupByOp,
    JoinOp,
    LimitOp,
    Op,
    RenameOp,
    SelectOp,
    SortOp,
    WithColumnsOp,
)

if TYPE_CHECKING:
    import polars as pl

    from polars_db.connection import Connection


def _normalize(exprs: tuple[Expr | str, ...]) -> tuple[Expr, ...]:
    """Convert string arguments to ``ColExpr``."""
    return tuple(ColExpr(name=e) if isinstance(e, str) else e for e in exprs)


def _normalize_opt(
    value: str | Expr | list[str | Expr] | None,
) -> tuple[Expr, ...] | None:
    """Normalize optional join key arguments."""
    if value is None:
        return None
    if isinstance(value, str | Expr):
        return (
            _ensure_expr(value) if isinstance(value, Expr) else ColExpr(name=value),
        )
    return tuple(ColExpr(name=v) if isinstance(v, str) else v for v in value)


class LazyFrame:
    """Polars ``LazyFrame``-compatible query builder.

    Each method returns a new ``LazyFrame`` with an updated Op tree.
    """

    def __init__(self, op: Op, connection: Connection) -> None:
        self._op = op
        self._conn = connection

    # -- basic operations ----------------------------------------------------

    def filter(self, predicate: Expr) -> LazyFrame:
        """Append a WHERE clause."""
        return LazyFrame(FilterOp(child=self._op, predicate=predicate), self._conn)

    def select(self, *exprs: Expr | str) -> LazyFrame:
        """Select specific columns."""
        return LazyFrame(SelectOp(child=self._op, exprs=_normalize(exprs)), self._conn)

    def with_columns(self, *exprs: Expr) -> LazyFrame:
        """Add or overwrite columns."""
        return LazyFrame(
            WithColumnsOp(child=self._op, exprs=_normalize(exprs)), self._conn
        )

    def sort(self, *by: str | Expr, descending: bool | list[bool] = False) -> LazyFrame:
        """Sort rows."""
        normalized = _normalize(by)
        if isinstance(descending, bool):
            desc_tuple = tuple(descending for _ in normalized)
        else:
            desc_tuple = tuple(descending)
        return LazyFrame(
            SortOp(child=self._op, by=normalized, descending=desc_tuple),
            self._conn,
        )

    def limit(self, n: int) -> LazyFrame:
        """Limit the number of rows."""
        return LazyFrame(LimitOp(child=self._op, n=n), self._conn)

    def head(self, n: int = 5) -> LazyFrame:
        """Return the first *n* rows."""
        return self.limit(n)

    def unique(self, subset: list[str] | None = None) -> LazyFrame:
        """Remove duplicate rows."""
        return LazyFrame(
            DistinctOp(child=self._op, subset=tuple(subset) if subset else None),
            self._conn,
        )

    def rename(self, mapping: dict[str, str]) -> LazyFrame:
        """Rename columns."""
        return LazyFrame(
            RenameOp(child=self._op, mapping=tuple(mapping.items())),
            self._conn,
        )

    def drop(self, *columns: str) -> LazyFrame:
        """Drop columns."""
        return LazyFrame(DropOp(child=self._op, columns=columns), self._conn)

    # -- join ----------------------------------------------------------------

    def join(
        self,
        other: LazyFrame,
        on: str | Expr | list[str | Expr] | None = None,
        left_on: str | Expr | list[str | Expr] | None = None,
        right_on: str | Expr | list[str | Expr] | None = None,
        how: str = "inner",
        validate: str = "m:m",
        suffix: str = "_right",
    ) -> LazyFrame:
        """Join with another ``LazyFrame``.

        When both sides share non-key column names, the right-hand columns
        receive ``suffix`` (default ``"_right"``) to disambiguate, matching
        polars semantics.
        """
        if on is not None and (left_on is not None or right_on is not None):
            msg = "Cannot specify both 'on' and 'left_on'/'right_on'"
            raise ValueError(msg)
        if (left_on is None) != (right_on is None):
            msg = "'left_on' and 'right_on' must both be specified"
            raise ValueError(msg)
        if validate not in ("m:m", "1:1", "1:m", "m:1"):
            msg = f"Invalid validate option: {validate}"
            raise ValueError(msg)

        return LazyFrame(
            JoinOp(
                left=self._op,
                right=other._op,
                on=_normalize_opt(on),
                left_on=_normalize_opt(left_on),
                right_on=_normalize_opt(right_on),
                how=how,
                validate=validate,
                suffix=suffix,
            ),
            self._conn,
        )

    # -- aggregation ---------------------------------------------------------

    def group_by(self, *by: str | Expr) -> GroupByProxy:
        """Start a group-by aggregation."""
        return GroupByProxy(self, _normalize(by))

    # -- execution -----------------------------------------------------------

    def collect(self) -> pl.DataFrame:
        """Compile, execute, and return a ``polars.DataFrame``."""
        self._run_validations()
        sql = self._compile()
        return self._conn.execute(sql)

    def show_query(self) -> str:
        """Return the generated SQL string."""
        return self._compile()

    def explain(self) -> str:
        """Return a human-readable representation of the Op tree."""
        return _format_tree(self._op)

    def explain_query(self, *, analyze: bool = False) -> str:
        """Return the database execution plan."""
        sql = self._compile()
        explain_sql = self._conn.backend.build_explain_sql(sql, analyze=analyze)
        result = self._conn.execute(explain_sql)
        return self._conn.backend.format_explain_result(result)

    # -- internal ------------------------------------------------------------

    def _compile(self) -> str:
        compiler = QueryCompiler(self._conn.backend, self._conn)
        ast = compiler.compile(self._op)
        optimized = Optimizer().optimize(ast)
        return self._conn.backend.render(optimized)

    def _run_validations(self) -> None:
        """Execute JOIN cardinality validation queries if needed."""
        from polars_db.compiler.optimizer import JoinValidator
        from polars_db.exceptions import JoinValidationError

        compiler = QueryCompiler(self._conn.backend, self._conn)
        validator = JoinValidator()
        for join_op in self._find_join_ops(self._op):
            for vq in validator.build_validation_queries(join_op, compiler):
                result = self._conn.execute(vq)
                if len(result) > 0:
                    msg = f"Join validate='{join_op.validate}' failed: duplicate keys found"
                    raise JoinValidationError(msg)

    @staticmethod
    def _find_join_ops(op: Op) -> list[Any]:
        """Collect all JoinOp nodes in the tree."""
        result: list[Any] = []
        if isinstance(op, JoinOp):
            result.append(op)
            result.extend(LazyFrame._find_join_ops(op.left))
            result.extend(LazyFrame._find_join_ops(op.right))
        elif hasattr(op, "child"):
            result.extend(LazyFrame._find_join_ops(op.child))  # type: ignore[attr-defined]
        return result


class GroupByProxy:
    """Intermediate object returned by ``group_by()``."""

    def __init__(self, lf: LazyFrame, by: tuple[Expr, ...]) -> None:
        self._lf = lf
        self._by = by

    def agg(self, *exprs: Expr) -> LazyFrame:
        """Apply aggregations and return a ``LazyFrame``."""
        return LazyFrame(
            GroupByOp(child=self._lf._op, by=self._by, agg=_normalize(exprs)),
            self._lf._conn,
        )


# ---------------------------------------------------------------------------
# Tree formatting
# ---------------------------------------------------------------------------


def _format_tree(op: Op, indent: int = 0) -> str:
    prefix = "  " * indent
    name = type(op).__name__
    match op:
        case _ if hasattr(op, "child"):
            child_str = _format_tree(op.child, indent + 1)  # type: ignore[attr-defined]
            return f"{prefix}{name}\n{child_str}"
        case JoinOp(left=left, right=right):
            left_str = _format_tree(left, indent + 1)
            right_str = _format_tree(right, indent + 1)
            return f"{prefix}{name}(how={op.how!r})\n{left_str}\n{right_str}"
        case _:
            return f"{prefix}{name}"
