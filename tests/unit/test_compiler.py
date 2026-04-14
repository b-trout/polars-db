"""Tests for QueryCompiler SQL generation."""

import pytest

from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import AggExpr, AliasExpr, BinaryExpr, ColExpr, LitExpr
from polars_db.ops import (
    FilterOp,
    GroupByOp,
    JoinOp,
    LimitOp,
    SelectOp,
    SortOp,
    TableRef,
)


def _compile_sql(op: object) -> str:
    """Compile an Op tree to a SQL string."""
    compiler = QueryCompiler(PostgresBackend())
    ast = compiler.compile(op)  # type: ignore[arg-type]
    return ast.sql(dialect="postgres")


@pytest.mark.unit
class TestTableRef:
    def test_simple_table(self) -> None:
        sql = _compile_sql(TableRef(name="users"))
        assert "SELECT" in sql.upper()
        assert "users" in sql

    def test_schema_qualified(self) -> None:
        sql = _compile_sql(TableRef(name="users", schema="public"))
        assert "public" in sql
        assert "users" in sql


@pytest.mark.unit
class TestFilter:
    def test_simple_filter(self) -> None:
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
    def test_select_columns(self) -> None:
        op = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"), ColExpr(name="age")),
        )
        sql = _compile_sql(op)
        assert "name" in sql
        assert "age" in sql


@pytest.mark.unit
class TestSort:
    def test_sort_asc(self) -> None:
        op = SortOp(
            child=TableRef(name="users"),
            by=(ColExpr(name="age"),),
            descending=(False,),
        )
        sql = _compile_sql(op)
        assert "ORDER BY" in sql.upper()

    def test_sort_desc(self) -> None:
        op = SortOp(
            child=TableRef(name="users"),
            by=(ColExpr(name="age"),),
            descending=(True,),
        )
        sql = _compile_sql(op)
        assert "DESC" in sql.upper()


@pytest.mark.unit
class TestLimit:
    def test_limit(self) -> None:
        op = LimitOp(child=TableRef(name="users"), n=10)
        sql = _compile_sql(op)
        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_limit_offset(self) -> None:
        op = LimitOp(child=TableRef(name="users"), n=10, offset=5)
        sql = _compile_sql(op)
        assert "LIMIT" in sql.upper()
        assert "OFFSET" in sql.upper()


@pytest.mark.unit
class TestSelectFilterSort:
    def test_combined_query(self) -> None:
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
    def test_group_by_agg(self) -> None:
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
    def test_inner_join(self) -> None:
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
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="left",
        )
        sql = _compile_sql(op)
        assert "LEFT" in sql.upper()

    def test_semi_join(self) -> None:
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="semi",
        )
        sql = _compile_sql(op)
        assert "EXISTS" in sql.upper()

    def test_anti_join(self) -> None:
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="anti",
        )
        sql = _compile_sql(op)
        assert "NOT" in sql.upper()
        assert "EXISTS" in sql.upper()
