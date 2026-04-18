"""Tests for Expr AST construction."""

import pytest

from polars_db.expr import (
    AggExpr,
    AliasExpr,
    BinaryExpr,
    CaseExpr,
    ColExpr,
    FuncExpr,
    LitExpr,
    UnaryExpr,
    WindowExpr,
    col,
    lit,
    when,
)


@pytest.mark.unit
class TestColAndLit:
    """Tests for ``col()`` and ``lit()`` factory functions."""

    def test_col_creates_col_expr(self) -> None:
        """Verify ``col()`` returns a ``ColExpr`` with the correct name."""
        e = col("age")
        assert isinstance(e, ColExpr)
        assert e.name == "age"

    def test_lit_creates_lit_expr(self) -> None:
        """Verify ``lit()`` returns a ``LitExpr`` with the correct value."""
        e = lit(42)
        assert isinstance(e, LitExpr)
        assert e.value == 42

    def test_lit_none(self) -> None:
        """Verify ``lit(None)`` returns a ``LitExpr`` with ``None`` value."""
        e = lit(None)
        assert isinstance(e, LitExpr)
        assert e.value is None


@pytest.mark.unit
class TestComparisonOperators:
    """Tests for comparison operators (``>``, ``<``, ``>=``, ``<=``, ``==``, ``!=``)."""

    def test_gt(self) -> None:
        """Verify ``>`` produces a ``BinaryExpr`` with op ``>``."""
        e = col("age") > 30
        assert isinstance(e, BinaryExpr)
        assert e.op == ">"

    def test_lt(self) -> None:
        """Verify ``<`` produces a ``BinaryExpr`` with op ``<``."""
        e = col("age") < 30
        assert isinstance(e, BinaryExpr)
        assert e.op == "<"

    def test_ge(self) -> None:
        """Verify ``>=`` produces a ``BinaryExpr`` with op ``>=``."""
        e = col("age") >= 30
        assert isinstance(e, BinaryExpr)
        assert e.op == ">="

    def test_le(self) -> None:
        """Verify ``<=`` produces a ``BinaryExpr`` with op ``<=``."""
        e = col("age") <= 30
        assert isinstance(e, BinaryExpr)
        assert e.op == "<="

    def test_eq(self) -> None:
        """Verify ``==`` produces a ``BinaryExpr`` with op ``==``."""
        e = col("name") == "Alice"
        assert isinstance(e, BinaryExpr)
        assert e.op == "=="

    def test_ne(self) -> None:
        """Verify ``!=`` produces a ``BinaryExpr`` with op ``!=``."""
        e = col("name") != "Bob"
        assert isinstance(e, BinaryExpr)
        assert e.op == "!="


@pytest.mark.unit
class TestArithmeticOperators:
    """Tests for arithmetic operators (``+``, ``-``, ``*``, ``/``, ``%``)."""

    def test_add(self) -> None:
        """Verify ``+`` produces a ``BinaryExpr`` with op ``+``."""
        e = col("a") + col("b")
        assert isinstance(e, BinaryExpr)
        assert e.op == "+"

    def test_sub(self) -> None:
        """Verify ``-`` produces a ``BinaryExpr`` with op ``-``."""
        e = col("a") - 1
        assert isinstance(e, BinaryExpr)
        assert e.op == "-"

    def test_mul(self) -> None:
        """Verify ``*`` produces a ``BinaryExpr`` with op ``*``."""
        e = col("price") * 1.1
        assert isinstance(e, BinaryExpr)
        assert e.op == "*"

    def test_truediv(self) -> None:
        """Verify ``/`` produces a ``BinaryExpr`` with op ``/``."""
        e = col("a") / col("b")
        assert isinstance(e, BinaryExpr)
        assert e.op == "/"

    def test_mod(self) -> None:
        """Verify ``%`` produces a ``BinaryExpr`` with op ``%``."""
        e = col("a") % 2
        assert isinstance(e, BinaryExpr)
        assert e.op == "%"


@pytest.mark.unit
class TestLogicalOperators:
    """Tests for logical operators (``&``, ``|``, ``~``, unary ``-``)."""

    def test_and(self) -> None:
        """Verify ``&`` produces a ``BinaryExpr`` with op ``and``."""
        e = (col("a") > 1) & (col("b") < 2)
        assert isinstance(e, BinaryExpr)
        assert e.op == "and"

    def test_or(self) -> None:
        """Verify ``|`` produces a ``BinaryExpr`` with op ``or``."""
        e = (col("a") > 1) | (col("b") < 2)
        assert isinstance(e, BinaryExpr)
        assert e.op == "or"

    def test_not(self) -> None:
        """Verify ``~`` produces a ``UnaryExpr`` with op ``not``."""
        e = ~col("active")
        assert isinstance(e, UnaryExpr)
        assert e.op == "not"

    def test_neg(self) -> None:
        """Verify unary ``-`` produces a ``UnaryExpr`` with op ``neg``."""
        e = -col("amount")
        assert isinstance(e, UnaryExpr)
        assert e.op == "neg"


@pytest.mark.unit
class TestAggregation:
    """Tests for aggregation methods (``sum``, ``mean``, ``count``, ``min``, ``max``)."""

    def test_sum(self) -> None:
        """Verify ``.sum()`` produces an ``AggExpr`` with func ``sum``."""
        e = col("amount").sum()
        assert isinstance(e, AggExpr)
        assert e.func == "sum"

    def test_mean(self) -> None:
        """Verify ``.mean()`` produces an ``AggExpr`` with func ``mean``."""
        e = col("amount").mean()
        assert isinstance(e, AggExpr)
        assert e.func == "mean"

    def test_count(self) -> None:
        """Verify ``.count()`` produces an ``AggExpr`` with func ``count``."""
        e = col("id").count()
        assert isinstance(e, AggExpr)
        assert e.func == "count"

    def test_min_max(self) -> None:
        """Verify ``.min()`` and ``.max()`` produce correct func names."""
        assert col("x").min().func == "min"
        assert col("x").max().func == "max"


@pytest.mark.unit
class TestAlias:
    """Tests for the ``.alias()`` method."""

    def test_alias(self) -> None:
        """Verify ``.alias()`` wraps the expression in an ``AliasExpr``."""
        e = col("amount").sum().alias("total")
        assert isinstance(e, AliasExpr)
        assert e.alias == "total"
        assert isinstance(e.expr, AggExpr)


@pytest.mark.unit
class TestWindow:
    """Tests for the ``.over()`` window partitioning method."""

    def test_over_string(self) -> None:
        """Verify ``.over()`` with a string partition key creates a ``WindowExpr``."""
        e = col("amount").sum().over("department")
        assert isinstance(e, WindowExpr)
        assert len(e.partition_by) == 1
        assert isinstance(e.partition_by[0], ColExpr)

    def test_over_expr(self) -> None:
        """Verify ``.over()`` with an ``Expr`` partition key creates a ``WindowExpr``."""
        e = col("amount").sum().over(col("dept"))
        assert isinstance(e, WindowExpr)
        assert isinstance(e.partition_by[0], ColExpr)

    def test_over_with_order_by_string(self) -> None:
        """Verify ``.over()`` with order_by string creates WindowExpr with order_by."""
        e = col("amount").sum().over("dept", order_by="date")
        assert isinstance(e, WindowExpr)
        assert e.order_by is not None
        assert len(e.order_by) == 1

    def test_over_with_order_by_list(self) -> None:
        """Verify ``.over()`` with order_by list creates multiple order_by entries."""
        e = col("amount").sum().over("dept", order_by=["date", "id"])
        assert isinstance(e, WindowExpr)
        assert e.order_by is not None
        assert len(e.order_by) == 2

    def test_over_with_order_by_expr(self) -> None:
        """Verify ``.over()`` with order_by Expr creates WindowExpr."""
        e = col("amount").sum().over("dept", order_by=col("date"))
        assert isinstance(e, WindowExpr)
        assert e.order_by is not None

    def test_dense_rank(self) -> None:
        """Verify ``.dense_rank()`` produces a ``FuncExpr``."""
        e = col("score").dense_rank()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "dense_rank"

    def test_cum_sum(self) -> None:
        """Verify ``.cum_sum()`` produces a ``FuncExpr``."""
        e = col("amount").cum_sum()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "cum_sum"

    def test_cum_count(self) -> None:
        """Verify ``.cum_count()`` produces a ``FuncExpr``."""
        e = col("id").cum_count()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "cum_count"

    def test_cum_max(self) -> None:
        """Verify ``.cum_max()`` produces a ``FuncExpr``."""
        e = col("amount").cum_max()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "cum_max"

    def test_cum_min(self) -> None:
        """Verify ``.cum_min()`` produces a ``FuncExpr``."""
        e = col("amount").cum_min()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "cum_min"

    def test_over_with_frame_rows(self) -> None:
        """Verify ``.over()`` with frame spec creates WindowExpr with frame."""
        e = (
            col("amount")
            .sum()
            .over("dept", order_by="date", frame=("rows", "unbounded", 0))
        )
        assert isinstance(e, WindowExpr)
        assert e.frame == ("rows", "unbounded", 0)

    def test_over_with_frame_range(self) -> None:
        """Verify ``.over()`` with range frame spec."""
        e = (
            col("amount")
            .sum()
            .over("dept", order_by="date", frame=("range", "unbounded", "unbounded"))
        )
        assert isinstance(e, WindowExpr)
        assert e.frame == ("range", "unbounded", "unbounded")

    def test_over_without_frame(self) -> None:
        """Verify ``.over()`` without frame defaults to None."""
        e = col("amount").sum().over("dept", order_by="date")
        assert isinstance(e, WindowExpr)
        assert e.frame is None


@pytest.mark.unit
class TestNullHandling:
    """Tests for null-handling methods (``is_null``, ``is_not_null``, ``fill_null``)."""

    def test_is_null(self) -> None:
        """Verify ``.is_null()`` produces a ``FuncExpr`` with name ``is_null``."""
        e = col("x").is_null()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "is_null"

    def test_is_not_null(self) -> None:
        """Verify ``.is_not_null()`` produces a ``FuncExpr`` with name ``is_not_null``."""
        e = col("x").is_not_null()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "is_not_null"

    def test_fill_null(self) -> None:
        """Verify ``.fill_null()`` produces a ``FuncExpr`` with name ``coalesce``."""
        e = col("x").fill_null(0)
        assert isinstance(e, FuncExpr)
        assert e.func_name == "coalesce"


@pytest.mark.unit
class TestWhenThenOtherwise:
    """Tests for ``when().then().otherwise()`` conditional expressions."""

    def test_simple_case(self) -> None:
        """Verify simple ``when().then().otherwise()`` creates a ``CaseExpr``."""
        e = when(col("age") > 30).then("senior").otherwise("junior")
        assert isinstance(e, CaseExpr)
        assert len(e.cases) == 1
        assert e.otherwise is not None

    def test_chained_when(self) -> None:
        """Verify chained ``when()`` calls produce multiple cases."""
        e = (
            when(col("age") > 60)
            .then("senior")
            .when(col("age") > 30)
            .then("mid")
            .otherwise("junior")
        )
        assert isinstance(e, CaseExpr)
        assert len(e.cases) == 2


@pytest.mark.unit
class TestNamespaces:
    """Tests for ``.str`` and ``.dt`` namespace accessors."""

    def test_str_to_lowercase(self) -> None:
        """Verify ``.str.to_lowercase()`` produces a ``FuncExpr`` with name ``lower``."""
        e = col("name").str.to_lowercase()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "lower"

    def test_dt_year(self) -> None:
        """Verify ``.dt.year()`` produces a ``FuncExpr`` with name ``extract_year``."""
        e = col("created_at").dt.year()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "extract_year"
