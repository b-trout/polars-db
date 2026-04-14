"""Tests for ExprCompiler."""

import pytest
import sqlglot.expressions as exp

from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.expr_compiler import ExprCompiler
from polars_db.expr import (
    AggExpr,
    AliasExpr,
    BinaryExpr,
    CaseExpr,
    ColExpr,
    FuncExpr,
    LitExpr,
    SortExpr,
    UnaryExpr,
    WindowExpr,
)


@pytest.fixture
def compiler() -> ExprCompiler:
    return ExprCompiler(PostgresBackend())


@pytest.mark.unit
class TestLiterals:
    def test_int(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(LitExpr(value=42))
        assert isinstance(result, exp.Literal)
        assert result.this == "42"

    def test_float(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(LitExpr(value=3.14))
        assert isinstance(result, exp.Literal)

    def test_string(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(LitExpr(value="hello"))
        assert isinstance(result, exp.Literal)
        assert result.is_string

    def test_bool(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(LitExpr(value=True))
        assert isinstance(result, exp.Boolean)

    def test_none(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(LitExpr(value=None))
        assert isinstance(result, exp.Null)


@pytest.mark.unit
class TestColumns:
    def test_col(self, compiler: ExprCompiler) -> None:
        result = compiler.compile(ColExpr(name="age"))
        assert isinstance(result, exp.Column)
        sql = result.sql(dialect="postgres")
        assert "age" in sql


@pytest.mark.unit
class TestBinaryOps:
    def test_gt(self, compiler: ExprCompiler) -> None:
        expr = BinaryExpr(op=">", left=ColExpr(name="age"), right=LitExpr(value=30))
        result = compiler.compile(expr)
        assert isinstance(result, exp.GT)

    def test_eq(self, compiler: ExprCompiler) -> None:
        expr = BinaryExpr(op="==", left=ColExpr(name="x"), right=LitExpr(value=1))
        result = compiler.compile(expr)
        assert isinstance(result, exp.EQ)

    def test_and(self, compiler: ExprCompiler) -> None:
        expr = BinaryExpr(
            op="and",
            left=BinaryExpr(op=">", left=ColExpr(name="a"), right=LitExpr(value=1)),
            right=BinaryExpr(op="<", left=ColExpr(name="b"), right=LitExpr(value=10)),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.And)


@pytest.mark.unit
class TestUnaryOps:
    def test_not(self, compiler: ExprCompiler) -> None:
        expr = UnaryExpr(op="not", operand=ColExpr(name="active"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Not)

    def test_neg(self, compiler: ExprCompiler) -> None:
        expr = UnaryExpr(op="neg", operand=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Neg)


@pytest.mark.unit
class TestAggregation:
    def test_sum(self, compiler: ExprCompiler) -> None:
        expr = AggExpr(func="sum", arg=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Sum)

    def test_avg(self, compiler: ExprCompiler) -> None:
        expr = AggExpr(func="mean", arg=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Avg)

    def test_count(self, compiler: ExprCompiler) -> None:
        expr = AggExpr(func="count", arg=ColExpr(name="id"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Count)


@pytest.mark.unit
class TestAlias:
    def test_alias(self, compiler: ExprCompiler) -> None:
        expr = AliasExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")), alias="total"
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Alias)
        sql = result.sql(dialect="postgres")
        assert "total" in sql


@pytest.mark.unit
class TestCase:
    def test_case_when(self, compiler: ExprCompiler) -> None:
        expr = CaseExpr(
            cases=(
                (
                    BinaryExpr(
                        op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
                    ),
                    LitExpr(value="senior"),
                ),
            ),
            otherwise=LitExpr(value="junior"),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Case)


@pytest.mark.unit
class TestSpecialFunctions:
    def test_is_null(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(func_name="is_null", args=(ColExpr(name="x"),))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Is)

    def test_is_not_null(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(func_name="is_not_null", args=(ColExpr(name="x"),))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Not)

    def test_between(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(
            func_name="between",
            args=(ColExpr(name="x"), LitExpr(value=1), LitExpr(value=10)),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Between)

    def test_isin(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(
            func_name="isin",
            args=(ColExpr(name="status"), LitExpr(value="a"), LitExpr(value="b")),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.In)


@pytest.mark.unit
class TestWindow:
    def test_sum_over_partition(self, compiler: ExprCompiler) -> None:
        expr = WindowExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
            partition_by=(ColExpr(name="department"),),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Window)
        sql = result.sql(dialect="postgres")
        assert "SUM" in sql.upper()
        assert "PARTITION BY" in sql.upper()

    def test_window_with_order_by(self, compiler: ExprCompiler) -> None:
        expr = WindowExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Window)
        sql = result.sql(dialect="postgres")
        assert "ORDER BY" in sql.upper()

    def test_shift_lag(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(
            func_name="shift", args=(ColExpr(name="value"), LitExpr(value=1))
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "LAG" in sql.upper()

    def test_shift_lead(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(
            func_name="shift", args=(ColExpr(name="value"), LitExpr(value=-2))
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "LEAD" in sql.upper()

    def test_rank(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(func_name="rank", args=(ColExpr(name="score"),))
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "RANK" in sql.upper()

    def test_row_number(self, compiler: ExprCompiler) -> None:
        expr = FuncExpr(func_name="row_number", args=(ColExpr(name="id"),))
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "ROW_NUMBER" in sql.upper()


@pytest.mark.unit
class TestSortExpr:
    def test_sort_asc(self, compiler: ExprCompiler) -> None:
        expr = SortExpr(expr=ColExpr(name="age"), descending=False)
        result = compiler.compile(expr)
        assert isinstance(result, exp.Ordered)

    def test_sort_desc(self, compiler: ExprCompiler) -> None:
        expr = SortExpr(expr=ColExpr(name="age"), descending=True)
        result = compiler.compile(expr)
        assert isinstance(result, exp.Ordered)
        sql = result.sql(dialect="postgres")
        assert "DESC" in sql.upper()
