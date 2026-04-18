"""Compile Expr AST nodes into SQLGlot expressions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from polars_db.exceptions import CompileError
from polars_db.expr import (
    AggExpr,
    AliasExpr,
    BinaryExpr,
    CaseExpr,
    ColExpr,
    Expr,
    FuncExpr,
    LitExpr,
    SortExpr,
    UnaryExpr,
    WindowExpr,
)

if TYPE_CHECKING:
    from polars_db.backends.base import Backend

_BINARY_OPS: dict[str, type[exp.Expression]] = {
    ">": exp.GT,
    "<": exp.LT,
    ">=": exp.GTE,
    "<=": exp.LTE,
    "==": exp.EQ,
    "!=": exp.NEQ,
    "+": exp.Add,
    "-": exp.Sub,
    "*": exp.Mul,
    "/": exp.Div,
    "%": exp.Mod,
    "and": exp.And,
    "or": exp.Or,
}

_AGG_FUNCS: dict[str, type[exp.Expression]] = {
    "sum": exp.Sum,
    "mean": exp.Avg,
    "count": exp.Count,
    "min": exp.Min,
    "max": exp.Max,
    "std": exp.Stddev,
    "var": exp.Variance,
}

_CUMULATIVE_FUNCS: dict[str, type[exp.Expression]] = {
    "cum_sum": exp.Sum,
    "cum_count": exp.Count,
    "cum_max": exp.Max,
    "cum_min": exp.Min,
}

_CUMULATIVE_FUNC_NAMES = frozenset(_CUMULATIVE_FUNCS)

_BUILTIN_FUNCS: dict[str, type[exp.Expression]] = {
    # String namespace functions
    "lower": exp.Lower,
    "upper": exp.Upper,
    "length": exp.Length,
    "substring": exp.Substring,
    "replace": exp.Anonymous,  # handled specially below
    # Null handling
    "coalesce": exp.Coalesce,
    # Type conversion
    "cast": exp.Cast,
}

# Functions that compile to LIKE patterns
_LIKE_FUNCS = {"contains", "starts_with", "ends_with"}

# Date/time extraction functions
_EXTRACT_FUNCS = {
    "extract_year": "year",
    "extract_month": "month",
    "extract_day": "day",
    "extract_hour": "hour",
    "extract_minute": "minute",
    "extract_second": "second",
}


class ExprCompiler:
    """Translate ``Expr`` AST into SQLGlot ``Expression`` trees."""

    def __init__(self, backend: Backend) -> None:
        self._backend = backend

    def compile(self, expr: Expr) -> exp.Expression:
        """Recursively compile an ``Expr`` into a SQLGlot expression."""
        match expr:
            case ColExpr(name=name):
                return exp.Column(this=exp.to_identifier(name))

            # bool must be checked before int (bool is a subclass of int)
            case LitExpr(value=value) if isinstance(value, bool):
                return exp.Boolean(this=value)
            case LitExpr(value=value) if isinstance(value, int | float):
                return exp.Literal.number(value)
            case LitExpr(value=value) if isinstance(value, str):
                return exp.Literal.string(value)
            case LitExpr(value=None):
                return exp.Null()

            case BinaryExpr(op=op, left=left, right=right):
                return self._binary_op(op, self.compile(left), self.compile(right))

            case UnaryExpr(op="not", operand=operand):
                return exp.Not(this=self.compile(operand))
            case UnaryExpr(op="neg", operand=operand):
                return exp.Neg(this=self.compile(operand))

            case AggExpr(func=func, arg=arg):
                return self._agg_func(func, self.compile(arg))

            case WindowExpr(
                expr=inner_expr, partition_by=partition_by, order_by=order_by
            ):
                inner = self.compile(inner_expr)
                pb = [self.compile(e) for e in partition_by]
                ob = [self.compile(e) for e in order_by] if order_by else None
                window = exp.Window(this=inner, partition_by=pb)
                if ob:
                    window.set("order", exp.Order(expressions=ob))
                if _is_cumulative(inner_expr):
                    window.set(
                        "spec",
                        exp.WindowSpec(
                            kind="ROWS",
                            start="UNBOUNDED",
                            start_side="PRECEDING",
                            end="CURRENT ROW",
                        ),
                    )
                return window

            case AliasExpr(expr=inner_expr, alias=alias):
                return exp.Alias(
                    this=self.compile(inner_expr),
                    alias=exp.to_identifier(alias),
                )

            case CaseExpr(cases=cases, otherwise=otherwise):
                ifs = [
                    exp.If(this=self.compile(c), true=self.compile(v)) for c, v in cases
                ]
                default = self.compile(otherwise) if otherwise else None
                return exp.Case(ifs=ifs, default=default)

            case FuncExpr(func_name=func_name, args=args):
                return self._builtin_func(func_name, args)

            case SortExpr(expr=inner_expr, descending=descending):
                return exp.Ordered(this=self.compile(inner_expr), desc=descending)

            case _:
                msg = f"Cannot compile expression: {type(expr).__name__}"
                raise CompileError(msg)

    # -- private helpers -----------------------------------------------------

    def _binary_op(
        self, op: str, left: exp.Expression, right: exp.Expression
    ) -> exp.Expression:
        cls = _BINARY_OPS.get(op)
        if cls is None:
            msg = f"Unknown binary operator: {op!r}"
            raise CompileError(msg)
        return cls(this=left, expression=right)

    def _agg_func(self, func: str, arg: exp.Expression) -> exp.Expression:
        cls = _AGG_FUNCS.get(func)
        if cls is None:
            msg = f"Unknown aggregate function: {func!r}"
            raise CompileError(msg)
        return cls(this=arg)

    def _builtin_func(self, name: str, args: tuple[Expr, ...]) -> exp.Expression:
        compiled_args = [self.compile(a) for a in args]

        # Special-case: is_null / is_not_null
        if name == "is_null":
            return exp.Is(this=compiled_args[0], expression=exp.Null())
        if name == "is_not_null":
            return exp.Not(this=exp.Is(this=compiled_args[0], expression=exp.Null()))

        # Special-case: BETWEEN
        if name == "between":
            return exp.Between(
                this=compiled_args[0],
                low=compiled_args[1],
                high=compiled_args[2],
            )

        # Special-case: IN
        if name == "isin":
            return exp.In(
                this=compiled_args[0],
                expressions=compiled_args[1:],
            )

        # Cumulative window functions
        cum_cls = _CUMULATIVE_FUNCS.get(name)
        if cum_cls is not None:
            return cum_cls(this=compiled_args[0])

        # Window helper functions
        if name == "shift":
            # Resolve the shift amount from the original Expr args
            from polars_db.expr import LitExpr as _LitExpr

            raw_n = (
                args[1].value if len(args) > 1 and isinstance(args[1], _LitExpr) else 1
            )
            abs_n = exp.Literal.number(abs(raw_n))
            if raw_n >= 0:
                return exp.Anonymous(this="LAG", expressions=[compiled_args[0], abs_n])
            return exp.Anonymous(
                this="LEAD",
                expressions=[compiled_args[0], abs_n],
            )
        if name == "rank":
            return exp.Anonymous(this="RANK", expressions=[])
        if name == "row_number":
            return exp.Anonymous(this="ROW_NUMBER", expressions=[])
        if name == "dense_rank":
            return exp.Anonymous(this="DENSE_RANK", expressions=[])

        # String LIKE patterns
        if name in _LIKE_FUNCS:
            return self._compile_like(name, compiled_args)

        # Date/time EXTRACT functions
        if name in _EXTRACT_FUNCS:
            return exp.Extract(
                this=exp.Var(this=_EXTRACT_FUNCS[name]),
                expression=compiled_args[0],
            )

        # DATE() function
        if name == "date":
            return exp.Anonymous(this="DATE", expressions=compiled_args)

        # DATE_TRUNC function
        if name == "date_trunc":
            return exp.Anonymous(this="DATE_TRUNC", expressions=compiled_args)

        # REPLACE function
        if name == "replace":
            return exp.Anonymous(this="REPLACE", expressions=compiled_args)

        # SUBSTRING function
        if name == "substring":
            return exp.Substring(
                this=compiled_args[0],
                start=compiled_args[1],
                length=compiled_args[2],
            )

        cls = _BUILTIN_FUNCS.get(name)
        if cls is not None:
            return cls(this=compiled_args[0], expressions=compiled_args[1:])

        # Fallback: generic function call
        return exp.Anonymous(this=name, expressions=compiled_args)

    @staticmethod
    def _compile_like(name: str, compiled_args: list[exp.Expression]) -> exp.Expression:
        """Compile contains/starts_with/ends_with to LIKE expressions."""
        col = compiled_args[0]
        pattern = compiled_args[1]

        # Build LIKE pattern based on function name
        if name == "contains":
            like_pattern = exp.Concat(
                expressions=[
                    exp.Literal.string("%"),
                    pattern,
                    exp.Literal.string("%"),
                ]
            )
        elif name == "starts_with":
            like_pattern = exp.Concat(expressions=[pattern, exp.Literal.string("%")])
        else:  # ends_with
            like_pattern = exp.Concat(expressions=[exp.Literal.string("%"), pattern])

        return exp.Like(this=col, expression=like_pattern)


def _is_cumulative(expr: Expr) -> bool:
    """Check if an expression is a cumulative window function."""
    from polars_db.expr import FuncExpr as _FuncExpr

    return isinstance(expr, _FuncExpr) and expr.func_name in _CUMULATIVE_FUNC_NAMES
