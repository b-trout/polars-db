"""Expression AST for polars-db.

Each user-facing expression (``col("age") > 30``) builds an immutable
tree of ``Expr`` nodes.  The tree is later walked by ``ExprCompiler``
to produce a SQLGlot AST.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import builtins

# ---------------------------------------------------------------------------
# Structural comparison helpers
# ---------------------------------------------------------------------------


def _deep_eq(a: object, b: object) -> bool:
    """Recursively compare values that may contain ``Expr`` or ``Op`` instances.

    ``Expr.__eq__`` returns ``BinaryExpr`` instead of ``bool``, so the
    built-in ``==`` cannot be used for structural comparison.
    Both ``Expr`` and ``Op`` provide ``_structural_eq()`` for this purpose.
    """
    if hasattr(a, "_structural_eq") and hasattr(b, "_structural_eq"):
        return a._structural_eq(b)  # ty: ignore[call-non-callable]
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return (
            type(a) is type(b)
            and len(a) == len(b)
            and all(_deep_eq(x, y) for x, y in zip(a, b, strict=True))
        )
    return a == b


# ---------------------------------------------------------------------------
# Base Expr
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class Expr:
    """Base class for all expression nodes.

    ``eq=False`` is required because ``__eq__`` must return ``BinaryExpr``
    for Polars API compatibility (``pdb.col("age") == 30``).
    """

    def _structural_eq(self, other: Expr) -> bool:
        """Internal structural comparison for testing."""
        if type(self) is not type(other):
            return False
        return all(
            _deep_eq(getattr(self, f.name), getattr(other, f.name))
            for f in fields(self)
        )

    # -- comparison operators ------------------------------------------------

    def __gt__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op=">", left=self, right=_ensure_expr(other))

    def __lt__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="<", left=self, right=_ensure_expr(other))

    def __ge__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op=">=", left=self, right=_ensure_expr(other))

    def __le__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="<=", left=self, right=_ensure_expr(other))

    def __eq__(self, other: Any) -> BinaryExpr:  # ty: ignore[invalid-method-override]
        return BinaryExpr(op="==", left=self, right=_ensure_expr(other))

    def __ne__(self, other: Any) -> BinaryExpr:  # ty: ignore[invalid-method-override]
        return BinaryExpr(op="!=", left=self, right=_ensure_expr(other))

    # -- arithmetic operators ------------------------------------------------

    def __add__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="+", left=self, right=_ensure_expr(other))

    def __sub__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="-", left=self, right=_ensure_expr(other))

    def __mul__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="*", left=self, right=_ensure_expr(other))

    def __truediv__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="/", left=self, right=_ensure_expr(other))

    def __floordiv__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="//", left=self, right=_ensure_expr(other))

    def __mod__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="%", left=self, right=_ensure_expr(other))

    # -- logical operators ---------------------------------------------------

    def __and__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="and", left=self, right=_ensure_expr(other))

    def __or__(self, other: Any) -> BinaryExpr:
        return BinaryExpr(op="or", left=self, right=_ensure_expr(other))

    def __invert__(self) -> UnaryExpr:
        return UnaryExpr(op="not", operand=self)

    def __neg__(self) -> UnaryExpr:
        return UnaryExpr(op="neg", operand=self)

    # -- aggregation methods -------------------------------------------------

    def sum(self) -> AggExpr:
        return AggExpr(func="sum", arg=self)

    def mean(self) -> AggExpr:
        return AggExpr(func="mean", arg=self)

    def count(self) -> AggExpr:
        return AggExpr(func="count", arg=self)

    def min(self) -> AggExpr:
        return AggExpr(func="min", arg=self)

    def max(self) -> AggExpr:
        return AggExpr(func="max", arg=self)

    def std(self) -> AggExpr:
        return AggExpr(func="std", arg=self)

    def var(self) -> AggExpr:
        return AggExpr(func="var", arg=self)

    def first(self) -> AggExpr:
        return AggExpr(func="first", arg=self)

    def last(self) -> AggExpr:
        return AggExpr(func="last", arg=self)

    # -- window --------------------------------------------------------------

    def over(
        self,
        *partition_by: builtins.str | Expr,
        order_by: builtins.str | Expr | list[builtins.str | Expr] | None = None,
    ) -> WindowExpr:
        pb = tuple(ColExpr(name=e) if isinstance(e, str) else e for e in partition_by)
        ob: tuple[Expr, ...] | None = None
        if order_by is not None:
            if isinstance(order_by, (str, Expr)):
                order_by = [order_by]
            ob = tuple(ColExpr(name=e) if isinstance(e, str) else e for e in order_by)
        return WindowExpr(expr=self, partition_by=pb, order_by=ob)

    def shift(self, n: int = 1) -> FuncExpr:
        return FuncExpr(func_name="shift", args=(self, LitExpr(value=n)))

    def rank(self) -> FuncExpr:
        return FuncExpr(func_name="rank", args=(self,))

    def row_number(self) -> FuncExpr:
        return FuncExpr(func_name="row_number", args=(self,))

    def dense_rank(self) -> FuncExpr:
        return FuncExpr(func_name="dense_rank", args=(self,))

    def cum_sum(self) -> FuncExpr:
        return FuncExpr(func_name="cum_sum", args=(self,))

    def cum_count(self) -> FuncExpr:
        return FuncExpr(func_name="cum_count", args=(self,))

    def cum_max(self) -> FuncExpr:
        return FuncExpr(func_name="cum_max", args=(self,))

    def cum_min(self) -> FuncExpr:
        return FuncExpr(func_name="cum_min", args=(self,))

    # -- transform -----------------------------------------------------------

    def alias(self, name: builtins.str) -> AliasExpr:
        return AliasExpr(expr=self, alias=name)

    def cast(self, dtype: Any) -> FuncExpr:
        return FuncExpr(func_name="cast", args=(self, LitExpr(value=dtype)))

    # -- null handling -------------------------------------------------------

    def is_null(self) -> FuncExpr:
        return FuncExpr(func_name="is_null", args=(self,))

    def is_not_null(self) -> FuncExpr:
        return FuncExpr(func_name="is_not_null", args=(self,))

    def fill_null(self, value: Any) -> FuncExpr:
        return FuncExpr(func_name="coalesce", args=(self, _ensure_expr(value)))

    # -- comparison helpers --------------------------------------------------

    def is_between(self, lower: Any, upper: Any) -> FuncExpr:
        return FuncExpr(
            func_name="between",
            args=(self, _ensure_expr(lower), _ensure_expr(upper)),
        )

    def is_in(self, values: list[Any]) -> FuncExpr:
        return FuncExpr(
            func_name="isin",
            args=(self, *(_ensure_expr(v) for v in values)),
        )

    # -- namespace properties (stubs, implemented in Phase 5) ----------------

    @property
    def str(self) -> StringNamespace:
        return StringNamespace(expr=self)

    @property
    def dt(self) -> DateTimeNamespace:
        return DateTimeNamespace(expr=self)


# ---------------------------------------------------------------------------
# Concrete Expr subclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class ColExpr(Expr):
    """Column reference: ``col("age")``."""

    name: str


@dataclass(frozen=True, eq=False)
class LitExpr(Expr):
    """Literal value: ``lit(30)``."""

    value: Any


@dataclass(frozen=True, eq=False)
class BinaryExpr(Expr):
    """Binary operation: ``col("age") > 30``."""

    op: str
    left: Expr
    right: Expr


@dataclass(frozen=True, eq=False)
class UnaryExpr(Expr):
    """Unary operation: ``~col("active")``, ``-col("amount")``."""

    op: str
    operand: Expr


@dataclass(frozen=True, eq=False)
class AggExpr(Expr):
    """Aggregate function: ``col("x").sum()``."""

    func: str
    arg: Expr


@dataclass(frozen=True, eq=False)
class WindowExpr(Expr):
    """Window function: ``col("x").sum().over("dept")``."""

    expr: Expr
    partition_by: tuple[Expr, ...]
    order_by: tuple[Expr, ...] | None = None


@dataclass(frozen=True, eq=False)
class AliasExpr(Expr):
    """Alias: ``expr.alias("new_name")``."""

    expr: Expr
    alias: str


@dataclass(frozen=True, eq=False)
class CaseExpr(Expr):
    """CASE WHEN: ``when(cond).then(val).otherwise(default)``."""

    cases: tuple[tuple[Expr, Expr], ...]
    otherwise: Expr | None = None


@dataclass(frozen=True, eq=False)
class FuncExpr(Expr):
    """Generic function call: ``cast()``, ``coalesce()``, str/dt ops."""

    func_name: str
    args: tuple[Expr, ...]


@dataclass(frozen=True, eq=False)
class SortExpr(Expr):
    """Sort specification: ``col("x").sort(descending=True)``."""

    expr: Expr
    descending: bool = False


# ---------------------------------------------------------------------------
# Namespace stubs (Phase 5)
# ---------------------------------------------------------------------------


class StringNamespace:
    """``col("name").str.to_lowercase()`` etc."""

    def __init__(self, expr: Expr) -> None:
        self._expr = expr

    def to_lowercase(self) -> FuncExpr:
        return FuncExpr(func_name="lower", args=(self._expr,))

    def to_uppercase(self) -> FuncExpr:
        return FuncExpr(func_name="upper", args=(self._expr,))

    def contains(self, pattern: str) -> FuncExpr:
        return FuncExpr(func_name="contains", args=(self._expr, LitExpr(value=pattern)))

    def starts_with(self, prefix: str) -> FuncExpr:
        return FuncExpr(
            func_name="starts_with", args=(self._expr, LitExpr(value=prefix))
        )

    def ends_with(self, suffix: str) -> FuncExpr:
        return FuncExpr(func_name="ends_with", args=(self._expr, LitExpr(value=suffix)))

    def len_chars(self) -> FuncExpr:
        return FuncExpr(func_name="length", args=(self._expr,))

    def slice(self, offset: int, length: int) -> FuncExpr:
        return FuncExpr(
            func_name="substring",
            args=(self._expr, LitExpr(value=offset), LitExpr(value=length)),
        )

    def replace(self, old: str, new: str) -> FuncExpr:
        return FuncExpr(
            func_name="replace",
            args=(self._expr, LitExpr(value=old), LitExpr(value=new)),
        )


class DateTimeNamespace:
    """``col("date").dt.year()`` etc."""

    def __init__(self, expr: Expr) -> None:
        self._expr = expr

    def year(self) -> FuncExpr:
        return FuncExpr(func_name="extract_year", args=(self._expr,))

    def month(self) -> FuncExpr:
        return FuncExpr(func_name="extract_month", args=(self._expr,))

    def day(self) -> FuncExpr:
        return FuncExpr(func_name="extract_day", args=(self._expr,))

    def hour(self) -> FuncExpr:
        return FuncExpr(func_name="extract_hour", args=(self._expr,))

    def minute(self) -> FuncExpr:
        return FuncExpr(func_name="extract_minute", args=(self._expr,))

    def second(self) -> FuncExpr:
        return FuncExpr(func_name="extract_second", args=(self._expr,))

    def date(self) -> FuncExpr:
        return FuncExpr(func_name="date", args=(self._expr,))

    def truncate(self, every: str) -> FuncExpr:
        return FuncExpr(func_name="date_trunc", args=(self._expr, LitExpr(value=every)))


# ---------------------------------------------------------------------------
# Public factory functions
# ---------------------------------------------------------------------------


def col(name: str) -> ColExpr:
    """Create a column reference expression."""
    return ColExpr(name=name)


def lit(value: Any) -> LitExpr:
    """Create a literal value expression."""
    return LitExpr(value=value)


def when(condition: Expr) -> WhenBuilder:
    """Start a CASE WHEN chain."""
    return WhenBuilder(condition=condition)


# ---------------------------------------------------------------------------
# WhenBuilder for when().then().otherwise() chaining
# ---------------------------------------------------------------------------


class WhenBuilder:
    """Builder for ``when(cond).then(val).otherwise(default)``."""

    def __init__(self, condition: Expr) -> None:
        self._condition = condition
        self._cases: list[tuple[Expr, Expr]] = []

    def then(self, value: Any) -> ThenBuilder:
        self._cases.append((self._condition, _ensure_expr(value)))
        return ThenBuilder(cases=list(self._cases))


class ThenBuilder:
    """Intermediate builder after ``.then()``."""

    def __init__(self, cases: list[tuple[Expr, Expr]]) -> None:
        self._cases = cases

    def when(self, condition: Expr) -> WhenChain:
        return WhenChain(cases=list(self._cases), condition=condition)

    def otherwise(self, value: Any) -> CaseExpr:
        return CaseExpr(
            cases=tuple(self._cases),
            otherwise=_ensure_expr(value),
        )

    def to_expr(self) -> CaseExpr:
        """Finalize without an ``otherwise`` clause."""
        return CaseExpr(cases=tuple(self._cases))


class WhenChain:
    """Handles chained ``when().then().when().then()`` calls."""

    def __init__(self, cases: list[tuple[Expr, Expr]], condition: Expr) -> None:
        self._cases = cases
        self._condition = condition

    def then(self, value: Any) -> ThenBuilder:
        self._cases.append((self._condition, _ensure_expr(value)))
        return ThenBuilder(cases=list(self._cases))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_expr(value: Any) -> Expr:
    """Wrap non-Expr values in ``LitExpr``."""
    if isinstance(value, Expr):
        return value
    return LitExpr(value=value)
