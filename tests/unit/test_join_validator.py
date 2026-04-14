"""Tests for JoinValidator."""

import pytest

from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.optimizer import JoinValidator
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import ColExpr
from polars_db.ops import JoinOp, TableRef


@pytest.fixture
def validator() -> JoinValidator:
    return JoinValidator()


@pytest.fixture
def compiler() -> QueryCompiler:
    return QueryCompiler(PostgresBackend())


@pytest.mark.unit
class TestJoinValidator:
    """Tests for join cardinality validation query generation."""

    def test_mm_returns_empty(
        self, validator: JoinValidator, compiler: QueryCompiler
    ) -> None:
        """Verify ``m:m`` validation produces no validation queries."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
            validate="m:m",
        )
        assert validator.build_validation_queries(op, compiler) == []

    def test_1m_checks_left(
        self, validator: JoinValidator, compiler: QueryCompiler
    ) -> None:
        """Verify ``1:m`` validation produces a uniqueness check on the left side."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
            validate="1:m",
        )
        queries = validator.build_validation_queries(op, compiler)
        assert len(queries) == 1
        sql = queries[0].upper()
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "COUNT" in sql

    def test_m1_checks_right(
        self, validator: JoinValidator, compiler: QueryCompiler
    ) -> None:
        """Verify ``m:1`` validation produces a uniqueness check on the right side."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="left",
            validate="m:1",
        )
        queries = validator.build_validation_queries(op, compiler)
        assert len(queries) == 1

    def test_11_checks_both(
        self, validator: JoinValidator, compiler: QueryCompiler
    ) -> None:
        """Verify ``1:1`` validation produces uniqueness checks on both sides."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
            validate="1:1",
        )
        queries = validator.build_validation_queries(op, compiler)
        assert len(queries) == 2

    def test_left_on_right_on(
        self, validator: JoinValidator, compiler: QueryCompiler
    ) -> None:
        """Verify validation with ``left_on``/``right_on`` key specification."""
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            left_on=(ColExpr(name="id"),),
            right_on=(ColExpr(name="user_id"),),
            how="inner",
            validate="1:m",
        )
        queries = validator.build_validation_queries(op, compiler)
        assert len(queries) == 1
        assert "id" in queries[0]
