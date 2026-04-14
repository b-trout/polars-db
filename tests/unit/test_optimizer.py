"""Tests for Optimizer."""

import pytest
import sqlglot

from polars_db.compiler.optimizer import Optimizer


@pytest.fixture
def optimizer() -> Optimizer:
    return Optimizer()


@pytest.mark.unit
class TestRemoveUnnecessarySubqueries:
    def test_collapse_select_star_wrapper(self, optimizer: Optimizer) -> None:
        sql = "SELECT * FROM (SELECT name, age FROM users WHERE age > 30) AS _q"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        result_sql = result.sql()
        assert "name" in result_sql
        assert "WHERE" in result_sql.upper()

    def test_keep_outer_where(self, optimizer: Optimizer) -> None:
        sql = "SELECT * FROM (SELECT * FROM users) AS _q WHERE age > 30"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        result_sql = result.sql()
        assert "WHERE" in result_sql.upper()

    def test_no_subquery(self, optimizer: Optimizer) -> None:
        sql = "SELECT * FROM users"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        assert result.sql() == ast.sql()


@pytest.mark.unit
class TestMergeConsecutiveFilters:
    def test_merge_two_wheres(self, optimizer: Optimizer) -> None:
        sql = "SELECT * FROM (SELECT * FROM users WHERE age > 30) AS _q WHERE name = 'Alice'"
        ast = sqlglot.parse_one(sql)
        result = optimizer._merge_consecutive_filters(ast)
        result_sql = result.sql()
        assert "AND" in result_sql.upper()

    def test_no_inner_where(self, optimizer: Optimizer) -> None:
        sql = "SELECT * FROM (SELECT * FROM users) AS _q WHERE age > 30"
        ast = sqlglot.parse_one(sql)
        result = optimizer._merge_consecutive_filters(ast)
        result_sql = result.sql()
        assert "WHERE" in result_sql.upper()


@pytest.mark.unit
class TestOptimizeFull:
    def test_optimize_passthrough(self, optimizer: Optimizer) -> None:
        sql = "SELECT name FROM users WHERE age > 30 ORDER BY name"
        ast = sqlglot.parse_one(sql)
        result = optimizer.optimize(ast)
        assert "name" in result.sql()
        assert "ORDER BY" in result.sql().upper()
