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
    def test_col_creates_col_expr(self) -> None:
        e = col("age")
        assert isinstance(e, ColExpr)
        assert e.name == "age"

    def test_lit_creates_lit_expr(self) -> None:
        e = lit(42)
        assert isinstance(e, LitExpr)
        assert e.value == 42

    def test_lit_none(self) -> None:
        e = lit(None)
        assert isinstance(e, LitExpr)
        assert e.value is None


@pytest.mark.unit
class TestComparisonOperators:
    def test_gt(self) -> None:
        e = col("age") > 30
        assert isinstance(e, BinaryExpr)
        assert e.op == ">"

    def test_lt(self) -> None:
        e = col("age") < 30
        assert isinstance(e, BinaryExpr)
        assert e.op == "<"

    def test_ge(self) -> None:
        e = col("age") >= 30
        assert isinstance(e, BinaryExpr)
        assert e.op == ">="

    def test_le(self) -> None:
        e = col("age") <= 30
        assert isinstance(e, BinaryExpr)
        assert e.op == "<="

    def test_eq(self) -> None:
        e = col("name") == "Alice"
        assert isinstance(e, BinaryExpr)
        assert e.op == "=="

    def test_ne(self) -> None:
        e = col("name") != "Bob"
        assert isinstance(e, BinaryExpr)
        assert e.op == "!="


@pytest.mark.unit
class TestArithmeticOperators:
    def test_add(self) -> None:
        e = col("a") + col("b")
        assert isinstance(e, BinaryExpr)
        assert e.op == "+"

    def test_sub(self) -> None:
        e = col("a") - 1
        assert isinstance(e, BinaryExpr)
        assert e.op == "-"

    def test_mul(self) -> None:
        e = col("price") * 1.1
        assert isinstance(e, BinaryExpr)
        assert e.op == "*"

    def test_truediv(self) -> None:
        e = col("a") / col("b")
        assert isinstance(e, BinaryExpr)
        assert e.op == "/"

    def test_mod(self) -> None:
        e = col("a") % 2
        assert isinstance(e, BinaryExpr)
        assert e.op == "%"


@pytest.mark.unit
class TestLogicalOperators:
    def test_and(self) -> None:
        e = (col("a") > 1) & (col("b") < 2)
        assert isinstance(e, BinaryExpr)
        assert e.op == "and"

    def test_or(self) -> None:
        e = (col("a") > 1) | (col("b") < 2)
        assert isinstance(e, BinaryExpr)
        assert e.op == "or"

    def test_not(self) -> None:
        e = ~col("active")
        assert isinstance(e, UnaryExpr)
        assert e.op == "not"

    def test_neg(self) -> None:
        e = -col("amount")
        assert isinstance(e, UnaryExpr)
        assert e.op == "neg"


@pytest.mark.unit
class TestAggregation:
    def test_sum(self) -> None:
        e = col("amount").sum()
        assert isinstance(e, AggExpr)
        assert e.func == "sum"

    def test_mean(self) -> None:
        e = col("amount").mean()
        assert isinstance(e, AggExpr)
        assert e.func == "mean"

    def test_count(self) -> None:
        e = col("id").count()
        assert isinstance(e, AggExpr)
        assert e.func == "count"

    def test_min_max(self) -> None:
        assert col("x").min().func == "min"
        assert col("x").max().func == "max"


@pytest.mark.unit
class TestAlias:
    def test_alias(self) -> None:
        e = col("amount").sum().alias("total")
        assert isinstance(e, AliasExpr)
        assert e.alias == "total"
        assert isinstance(e.expr, AggExpr)


@pytest.mark.unit
class TestWindow:
    def test_over_string(self) -> None:
        e = col("amount").sum().over("department")
        assert isinstance(e, WindowExpr)
        assert len(e.partition_by) == 1
        assert isinstance(e.partition_by[0], ColExpr)

    def test_over_expr(self) -> None:
        e = col("amount").sum().over(col("dept"))
        assert isinstance(e, WindowExpr)
        assert isinstance(e.partition_by[0], ColExpr)


@pytest.mark.unit
class TestNullHandling:
    def test_is_null(self) -> None:
        e = col("x").is_null()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "is_null"

    def test_is_not_null(self) -> None:
        e = col("x").is_not_null()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "is_not_null"

    def test_fill_null(self) -> None:
        e = col("x").fill_null(0)
        assert isinstance(e, FuncExpr)
        assert e.func_name == "coalesce"


@pytest.mark.unit
class TestWhenThenOtherwise:
    def test_simple_case(self) -> None:
        e = when(col("age") > 30).then("senior").otherwise("junior")
        assert isinstance(e, CaseExpr)
        assert len(e.cases) == 1
        assert e.otherwise is not None

    def test_chained_when(self) -> None:
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
    def test_str_to_lowercase(self) -> None:
        e = col("name").str.to_lowercase()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "lower"

    def test_dt_year(self) -> None:
        e = col("created_at").dt.year()
        assert isinstance(e, FuncExpr)
        assert e.func_name == "extract_year"
