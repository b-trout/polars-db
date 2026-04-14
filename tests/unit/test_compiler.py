"""Tests for QueryCompiler SQL generation."""

import pytest

from polars_db.backends.bigquery import BigQueryBackend
from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import AggExpr, AliasExpr, BinaryExpr, ColExpr, LitExpr, WindowExpr
from polars_db.ops import (
    FilterOp,
    GroupByOp,
    JoinOp,
    LimitOp,
    SelectOp,
    SortOp,
    TableRef,
    WithColumnsOp,
)


def _compile_sql(op: object) -> str:
    """Compile an Op tree to a SQL string."""
    compiler = QueryCompiler(PostgresBackend())
    ast = compiler.compile(op)  # type: ignore[arg-type]
    return ast.sql(dialect="postgres")


def _compile_bigquery_sql(op: object) -> str:
    """Compile an Op tree to a BigQuery SQL string."""
    backend = BigQueryBackend()
    compiler = QueryCompiler(backend)
    ast = compiler.compile(op)  # type: ignore[arg-type]
    return backend.render(ast)


@pytest.mark.unit
class TestTableRef:
    """Tests for table reference SQL generation."""

    def test_simple_table(self) -> None:
        """Verify SQL for a simple unqualified table reference."""
        sql = _compile_sql(TableRef(name="users"))
        assert "SELECT" in sql.upper()
        assert "users" in sql

    def test_schema_qualified(self) -> None:
        """Verify SQL for a schema-qualified table reference."""
        sql = _compile_sql(TableRef(name="users", schema="public"))
        assert "public" in sql
        assert "users" in sql


@pytest.mark.unit
class TestFilter:
    """Tests for WHERE clause generation."""

    def test_simple_filter(self) -> None:
        """Verify WHERE clause from a single filter predicate."""
        op = FilterOp(
            child=TableRef(name="users"),
            predicate=BinaryExpr(
                op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
            ),
        )
        sql = _compile_sql(op)
        assert "WHERE" in sql.upper()
        assert "age" in sql
        assert "30" in sql

    def test_chained_filters(self) -> None:
        """Verify WHERE clause from chained filter predicates."""
        op = FilterOp(
            child=FilterOp(
                child=TableRef(name="users"),
                predicate=BinaryExpr(
                    op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
                ),
            ),
            predicate=BinaryExpr(
                op="==", left=ColExpr(name="active"), right=LitExpr(value=True)
            ),
        )
        sql = _compile_sql(op)
        assert sql.upper().count("WHERE") >= 1


@pytest.mark.unit
class TestSelect:
    """Tests for SELECT column projection."""

    def test_select_columns(self) -> None:
        """Verify column projection in SELECT clause."""
        op = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"), ColExpr(name="age")),
        )
        sql = _compile_sql(op)
        assert "name" in sql
        assert "age" in sql


@pytest.mark.unit
class TestSort:
    """Tests for ORDER BY clause generation."""

    def test_sort_asc(self) -> None:
        """Verify ORDER BY for ascending sort."""
        op = SortOp(
            child=TableRef(name="users"),
            by=(ColExpr(name="age"),),
            descending=(False,),
        )
        sql = _compile_sql(op)
        assert "ORDER BY" in sql.upper()

    def test_sort_desc(self) -> None:
        """Verify ORDER BY with DESC for descending sort."""
        op = SortOp(
            child=TableRef(name="users"),
            by=(ColExpr(name="age"),),
            descending=(True,),
        )
        sql = _compile_sql(op)
        assert "DESC" in sql.upper()


@pytest.mark.unit
class TestLimit:
    """Tests for LIMIT and OFFSET clause generation."""

    def test_limit(self) -> None:
        """Verify LIMIT clause generation."""
        op = LimitOp(child=TableRef(name="users"), n=10)
        sql = _compile_sql(op)
        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_limit_offset(self) -> None:
        """Verify LIMIT with OFFSET clause generation."""
        op = LimitOp(child=TableRef(name="users"), n=10, offset=5)
        sql = _compile_sql(op)
        assert "LIMIT" in sql.upper()
        assert "OFFSET" in sql.upper()


@pytest.mark.unit
class TestSelectFilterSort:
    """Tests for combined SELECT, WHERE, and ORDER BY queries."""

    def test_combined_query(self) -> None:
        """Verify SQL with SELECT, WHERE, and ORDER BY combined."""
        op = SortOp(
            child=SelectOp(
                child=FilterOp(
                    child=TableRef(name="users"),
                    predicate=BinaryExpr(
                        op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
                    ),
                ),
                exprs=(ColExpr(name="name"), ColExpr(name="age")),
            ),
            by=(ColExpr(name="age"),),
            descending=(False,),
        )
        sql = _compile_sql(op)
        assert "SELECT" in sql.upper()
        assert "WHERE" in sql.upper()
        assert "ORDER BY" in sql.upper()


@pytest.mark.unit
class TestGroupBy:
    """Tests for GROUP BY with aggregation."""

    def test_group_by_agg(self) -> None:
        """Verify GROUP BY with SUM aggregation."""
        op = GroupByOp(
            child=TableRef(name="orders"),
            by=(ColExpr(name="user_id"),),
            agg=(
                AliasExpr(
                    expr=AggExpr(func="sum", arg=ColExpr(name="amount")), alias="total"
                ),
            ),
        )
        sql = _compile_sql(op)
        assert "GROUP BY" in sql.upper()
        assert "SUM" in sql.upper()


@pytest.mark.unit
class TestJoin:
    """Tests for JOIN SQL generation."""

    def test_inner_join(self) -> None:
        """Verify INNER JOIN SQL generation."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(op)
        assert "JOIN" in sql.upper()
        assert "users" in sql
        assert "orders" in sql

    def test_left_join(self) -> None:
        """Verify LEFT JOIN SQL generation."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="left",
        )
        sql = _compile_sql(op)
        assert "LEFT" in sql.upper()

    def test_semi_join(self) -> None:
        """Verify semi join compiles as EXISTS subquery."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="semi",
        )
        sql = _compile_sql(op)
        assert "EXISTS" in sql.upper()

    def test_anti_join(self) -> None:
        """Verify anti join compiles as NOT EXISTS subquery."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="anti",
        )
        sql = _compile_sql(op)
        assert "NOT" in sql.upper()
        assert "EXISTS" in sql.upper()


@pytest.mark.unit
class TestWithColumnsWindow:
    """Tests for with_columns window function SQL generation."""

    def test_with_columns_window_function(self) -> None:
        """Test with_columns adding a window function.

        Use SelectOp as child so column resolution works without Connection.
        """
        op = WithColumnsOp(
            child=SelectOp(
                child=TableRef(name="sales"),
                exprs=(
                    ColExpr(name="id"),
                    ColExpr(name="amount"),
                    ColExpr(name="department"),
                ),
            ),
            exprs=(
                AliasExpr(
                    expr=WindowExpr(
                        expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
                        partition_by=(ColExpr(name="department"),),
                    ),
                    alias="dept_total",
                ),
            ),
        )
        sql = _compile_sql(op)
        assert "SUM" in sql.upper()
        assert "OVER" in sql.upper()
        assert "PARTITION BY" in sql.upper()
        assert "dept_total" in sql


@pytest.mark.unit
class TestComplexQuery:
    """Tests for complex multi-operation query compilation."""

    def test_filter_join_groupby_sort_limit(self) -> None:
        """Test a complex multi-operation query chain."""
        op = LimitOp(
            child=SortOp(
                child=GroupByOp(
                    child=JoinOp(
                        left=FilterOp(
                            child=TableRef(name="users"),
                            predicate=BinaryExpr(
                                op=">",
                                left=ColExpr(name="age"),
                                right=LitExpr(value=18),
                            ),
                        ),
                        right=TableRef(name="orders"),
                        on=(ColExpr(name="user_id"),),
                        how="left",
                    ),
                    by=(ColExpr(name="name"),),
                    agg=(
                        AliasExpr(
                            expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
                            alias="total",
                        ),
                    ),
                ),
                by=(ColExpr(name="total"),),
                descending=(True,),
            ),
            n=10,
        )
        sql = _compile_sql(op)
        assert "WHERE" in sql.upper()
        assert "LEFT JOIN" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()
        assert "LIMIT" in sql.upper()


@pytest.mark.unit
class TestBigQueryDialect:
    """Tests for BigQuery SQL dialect generation.

    BigQuery integration tests use SQL snapshot testing (no emulator)
    following the dbplyr strategy for vendor DWH backends.
    """

    def test_select(self) -> None:
        """Verify BigQuery SELECT column projection."""
        op = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"), ColExpr(name="age")),
        )
        sql = _compile_bigquery_sql(op)
        assert "name" in sql
        assert "age" in sql

    def test_filter(self) -> None:
        """Verify BigQuery WHERE clause generation."""
        op = FilterOp(
            child=TableRef(name="users"),
            predicate=BinaryExpr(
                op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
            ),
        )
        sql = _compile_bigquery_sql(op)
        assert "WHERE" in sql.upper()
        assert "age" in sql
        assert "30" in sql

    def test_sort(self) -> None:
        """Verify BigQuery ORDER BY generation."""
        op = SortOp(
            child=TableRef(name="users"),
            by=(ColExpr(name="age"),),
            descending=(True,),
        )
        sql = _compile_bigquery_sql(op)
        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()

    def test_group_by_agg(self) -> None:
        """Verify BigQuery GROUP BY with SUM aggregation."""
        op = GroupByOp(
            child=TableRef(name="orders"),
            by=(ColExpr(name="user_id"),),
            agg=(
                AliasExpr(
                    expr=AggExpr(func="sum", arg=ColExpr(name="amount")),
                    alias="total",
                ),
            ),
        )
        sql = _compile_bigquery_sql(op)
        assert "GROUP BY" in sql.upper()
        assert "SUM" in sql.upper()
        assert "total" in sql

    def test_limit(self) -> None:
        """Verify BigQuery LIMIT clause generation."""
        op = LimitOp(child=TableRef(name="users"), n=10)
        sql = _compile_bigquery_sql(op)
        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_combined_filter_sort_select(self) -> None:
        """Verify combined query generates valid BigQuery SQL."""
        op = SortOp(
            child=SelectOp(
                child=FilterOp(
                    child=TableRef(name="users"),
                    predicate=BinaryExpr(
                        op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
                    ),
                ),
                exprs=(ColExpr(name="name"), ColExpr(name="age")),
            ),
            by=(ColExpr(name="age"),),
            descending=(False,),
        )
        sql = _compile_bigquery_sql(op)
        assert "WHERE" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "NULLS LAST" in sql.upper()
        assert "name" in sql
        assert "age" in sql
