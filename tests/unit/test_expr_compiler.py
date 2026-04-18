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
    """Test compiling literal expressions to sqlglot AST nodes."""

    def test_int(self, compiler: ExprCompiler) -> None:
        """Verify integer literal compiles to ``exp.Literal``."""
        result = compiler.compile(LitExpr(value=42))
        assert isinstance(result, exp.Literal)
        assert result.this == "42"

    def test_float(self, compiler: ExprCompiler) -> None:
        """Verify float literal compiles to ``exp.Literal``."""
        result = compiler.compile(LitExpr(value=3.14))
        assert isinstance(result, exp.Literal)

    def test_string(self, compiler: ExprCompiler) -> None:
        """Verify string literal compiles to ``exp.Literal`` with string flag."""
        result = compiler.compile(LitExpr(value="hello"))
        assert isinstance(result, exp.Literal)
        assert result.is_string

    def test_bool(self, compiler: ExprCompiler) -> None:
        """Verify boolean literal compiles to ``exp.Boolean``."""
        result = compiler.compile(LitExpr(value=True))
        assert isinstance(result, exp.Boolean)

    def test_none(self, compiler: ExprCompiler) -> None:
        """Verify ``None`` literal compiles to ``exp.Null``."""
        result = compiler.compile(LitExpr(value=None))
        assert isinstance(result, exp.Null)


@pytest.mark.unit
class TestColumns:
    """Tests for compiling column references to sqlglot nodes."""

    def test_col(self, compiler: ExprCompiler) -> None:
        """Verify column reference compiles to ``exp.Column``."""
        result = compiler.compile(ColExpr(name="age"))
        assert isinstance(result, exp.Column)
        sql = result.sql(dialect="postgres")
        assert "age" in sql


@pytest.mark.unit
class TestBinaryOps:
    """Tests for compiling binary operators to sqlglot AST nodes."""

    def test_gt(self, compiler: ExprCompiler) -> None:
        """Verify greater-than compiles to ``exp.GT``."""
        expr = BinaryExpr(op=">", left=ColExpr(name="age"), right=LitExpr(value=30))
        result = compiler.compile(expr)
        assert isinstance(result, exp.GT)

    def test_eq(self, compiler: ExprCompiler) -> None:
        """Verify equality compiles to ``exp.EQ``."""
        expr = BinaryExpr(op="==", left=ColExpr(name="x"), right=LitExpr(value=1))
        result = compiler.compile(expr)
        assert isinstance(result, exp.EQ)

    def test_and(self, compiler: ExprCompiler) -> None:
        """Verify logical AND compiles to ``exp.And``."""
        expr = BinaryExpr(
            op="and",
            left=BinaryExpr(op=">", left=ColExpr(name="a"), right=LitExpr(value=1)),
            right=BinaryExpr(op="<", left=ColExpr(name="b"), right=LitExpr(value=10)),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.And)


@pytest.mark.unit
class TestUnaryOps:
    """Tests for compiling unary operators to sqlglot AST nodes."""

    def test_not(self, compiler: ExprCompiler) -> None:
        """Verify NOT compiles to ``exp.Not``."""
        expr = UnaryExpr(op="not", operand=ColExpr(name="active"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Not)

    def test_neg(self, compiler: ExprCompiler) -> None:
        """Verify negation compiles to ``exp.Neg``."""
        expr = UnaryExpr(op="neg", operand=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Neg)


@pytest.mark.unit
class TestAggregation:
    """Tests for compiling aggregation functions to sqlglot nodes."""

    def test_sum(self, compiler: ExprCompiler) -> None:
        """Verify sum compiles to ``exp.Sum``."""
        expr = AggExpr(func="sum", arg=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Sum)

    def test_avg(self, compiler: ExprCompiler) -> None:
        """Verify mean compiles to ``exp.Avg``."""
        expr = AggExpr(func="mean", arg=ColExpr(name="amount"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Avg)

    def test_count(self, compiler: ExprCompiler) -> None:
        """Verify count compiles to ``exp.Count``."""
        expr = AggExpr(func="count", arg=ColExpr(name="id"))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Count)


@pytest.mark.unit
class TestAlias:
    """Tests for compiling alias expressions to sqlglot nodes."""

    def test_alias(self, compiler: ExprCompiler) -> None:
        """Verify aliased aggregation compiles to ``exp.Alias``."""
        expr = AliasExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")), alias="total"
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Alias)
        sql = result.sql(dialect="postgres")
        assert "total" in sql


@pytest.mark.unit
class TestCase:
    """Tests for compiling CASE WHEN expressions."""

    def test_case_when(self, compiler: ExprCompiler) -> None:
        """Verify CASE WHEN with otherwise compiles to ``exp.Case``."""
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
    """Tests for IS NULL, IS NOT NULL, BETWEEN, and IN compilation."""

    def test_is_null(self, compiler: ExprCompiler) -> None:
        """Verify ``is_null`` compiles to ``exp.Is``."""
        expr = FuncExpr(func_name="is_null", args=(ColExpr(name="x"),))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Is)

    def test_is_not_null(self, compiler: ExprCompiler) -> None:
        """Verify ``is_not_null`` compiles to ``exp.Not``."""
        expr = FuncExpr(func_name="is_not_null", args=(ColExpr(name="x"),))
        result = compiler.compile(expr)
        assert isinstance(result, exp.Not)

    def test_between(self, compiler: ExprCompiler) -> None:
        """Verify ``between`` compiles to ``exp.Between``."""
        expr = FuncExpr(
            func_name="between",
            args=(ColExpr(name="x"), LitExpr(value=1), LitExpr(value=10)),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Between)

    def test_isin(self, compiler: ExprCompiler) -> None:
        """Verify ``isin`` compiles to ``exp.In``."""
        expr = FuncExpr(
            func_name="isin",
            args=(ColExpr(name="status"), LitExpr(value="a"), LitExpr(value="b")),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.In)


@pytest.mark.unit
class TestWindow:
    """Tests for compiling window function expressions."""

    def test_sum_over_partition(self, compiler: ExprCompiler) -> None:
        """Verify SUM with PARTITION BY compiles to ``exp.Window``."""
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
        """Verify window function with ORDER BY clause."""
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
        """Verify shift with positive offset compiles to LAG."""
        expr = FuncExpr(
            func_name="shift", args=(ColExpr(name="value"), LitExpr(value=1))
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "LAG" in sql.upper()

    def test_shift_lead(self, compiler: ExprCompiler) -> None:
        """Verify shift with negative offset compiles to LEAD."""
        expr = FuncExpr(
            func_name="shift", args=(ColExpr(name="value"), LitExpr(value=-2))
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "LEAD" in sql.upper()

    def test_rank(self, compiler: ExprCompiler) -> None:
        """Verify rank function compiles to RANK."""
        expr = FuncExpr(func_name="rank", args=(ColExpr(name="score"),))
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "RANK" in sql.upper()

    def test_row_number(self, compiler: ExprCompiler) -> None:
        """Verify row_number compiles to ROW_NUMBER."""
        expr = FuncExpr(func_name="row_number", args=(ColExpr(name="id"),))
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "ROW_NUMBER" in sql.upper()

    def test_dense_rank(self, compiler: ExprCompiler) -> None:
        """Verify dense_rank compiles to DENSE_RANK."""
        expr = FuncExpr(func_name="dense_rank", args=(ColExpr(name="score"),))
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "DENSE_RANK" in sql.upper()

    def test_cum_sum_with_frame(self, compiler: ExprCompiler) -> None:
        """Verify cum_sum inside WindowExpr compiles to SUM with frame spec."""
        expr = WindowExpr(
            expr=FuncExpr(func_name="cum_sum", args=(ColExpr(name="amount"),)),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
        )
        result = compiler.compile(expr)
        assert isinstance(result, exp.Window)
        sql = result.sql(dialect="postgres")
        assert "SUM" in sql.upper()
        assert "ROWS BETWEEN" in sql.upper()
        assert "UNBOUNDED PRECEDING" in sql.upper()
        assert "CURRENT ROW" in sql.upper()

    def test_cum_count_with_frame(self, compiler: ExprCompiler) -> None:
        """Verify cum_count inside WindowExpr compiles to COUNT with frame spec."""
        expr = WindowExpr(
            expr=FuncExpr(func_name="cum_count", args=(ColExpr(name="id"),)),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "COUNT" in sql.upper()
        assert "ROWS BETWEEN" in sql.upper()

    def test_cum_max_with_frame(self, compiler: ExprCompiler) -> None:
        """Verify cum_max inside WindowExpr compiles to MAX with frame spec."""
        expr = WindowExpr(
            expr=FuncExpr(func_name="cum_max", args=(ColExpr(name="amount"),)),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "MAX" in sql.upper()
        assert "ROWS BETWEEN" in sql.upper()

    def test_cum_min_with_frame(self, compiler: ExprCompiler) -> None:
        """Verify cum_min inside WindowExpr compiles to MIN with frame spec."""
        expr = WindowExpr(
            expr=FuncExpr(func_name="cum_min", args=(ColExpr(name="amount"),)),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "MIN" in sql.upper()
        assert "ROWS BETWEEN" in sql.upper()

    def test_explicit_rows_frame(self, compiler: ExprCompiler) -> None:
        """Verify explicit ROWS frame compiles correctly."""
        expr = WindowExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
            frame=("rows", -3, 0),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "ROWS BETWEEN" in sql.upper()
        assert "3 PRECEDING" in sql.upper()
        assert "CURRENT ROW" in sql.upper()

    def test_explicit_range_frame(self, compiler: ExprCompiler) -> None:
        """Verify explicit RANGE frame compiles correctly."""
        expr = WindowExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
            frame=("range", "unbounded", "unbounded"),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "RANGE BETWEEN" in sql.upper()
        assert "UNBOUNDED PRECEDING" in sql.upper()
        assert "UNBOUNDED FOLLOWING" in sql.upper()

    def test_rows_preceding_following(self, compiler: ExprCompiler) -> None:
        """Verify ROWS BETWEEN N PRECEDING AND M FOLLOWING."""
        expr = WindowExpr(
            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
            partition_by=(ColExpr(name="dept"),),
            order_by=(ColExpr(name="date"),),
            frame=("rows", -1, 1),
        )
        result = compiler.compile(expr)
        sql = result.sql(dialect="postgres")
        assert "ROWS BETWEEN" in sql.upper()
        assert "1 PRECEDING" in sql.upper()
        assert "1 FOLLOWING" in sql.upper()


@pytest.mark.unit
class TestSortExpr:
    """Tests for compiling sort expressions to sqlglot nodes."""

    def test_sort_asc(self, compiler: ExprCompiler) -> None:
        """Verify ascending sort compiles to ``exp.Ordered``."""
        expr = SortExpr(expr=ColExpr(name="age"), descending=False)
        result = compiler.compile(expr)
        assert isinstance(result, exp.Ordered)

    def test_sort_desc(self, compiler: ExprCompiler) -> None:
        """Verify descending sort compiles to ``exp.Ordered`` with DESC."""
        expr = SortExpr(expr=ColExpr(name="age"), descending=True)
        result = compiler.compile(expr)
        assert isinstance(result, exp.Ordered)
        sql = result.sql(dialect="postgres")
        assert "DESC" in sql.upper()
