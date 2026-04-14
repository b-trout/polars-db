"""Tests for Optimizer."""

import pytest
import sqlglot

from polars_db.compiler.optimizer import Optimizer


@pytest.fixture
def optimizer() -> Optimizer:
    return Optimizer()


@pytest.mark.unit
class TestRemoveUnnecessarySubqueries:
    """Tests for collapsing unnecessary SELECT * subquery wrappers."""

    def test_collapse_select_star_wrapper(self, optimizer: Optimizer) -> None:
        """Verify SELECT * wrapper is collapsed into inner query."""
        sql = "SELECT * FROM (SELECT name, age FROM users WHERE age > 30) AS _q"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        result_sql = result.sql()
        assert "name" in result_sql
        assert "WHERE" in result_sql.upper()

    def test_keep_outer_where(self, optimizer: Optimizer) -> None:
        """Verify outer WHERE clause is preserved after collapse."""
        sql = "SELECT * FROM (SELECT * FROM users) AS _q WHERE age > 30"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        result_sql = result.sql()
        assert "WHERE" in result_sql.upper()

    def test_no_subquery(self, optimizer: Optimizer) -> None:
        """Verify pass-through when no subquery is present."""
        sql = "SELECT * FROM users"
        ast = sqlglot.parse_one(sql)
        result = optimizer._remove_unnecessary_subqueries(ast)
        assert result.sql() == ast.sql()


@pytest.mark.unit
class TestMergeConsecutiveFilters:
    """Tests for merging consecutive WHERE clauses into AND."""

    def test_merge_two_wheres(self, optimizer: Optimizer) -> None:
        """Verify two WHERE clauses merge with AND."""
        sql = "SELECT * FROM (SELECT * FROM users WHERE age > 30) AS _q WHERE name = 'Alice'"
        ast = sqlglot.parse_one(sql)
        result = optimizer._merge_consecutive_filters(ast)
        result_sql = result.sql()
        assert "AND" in result_sql.upper()

    def test_no_inner_where(self, optimizer: Optimizer) -> None:
        """Verify pass-through when inner query has no WHERE."""
        sql = "SELECT * FROM (SELECT * FROM users) AS _q WHERE age > 30"
        ast = sqlglot.parse_one(sql)
        result = optimizer._merge_consecutive_filters(ast)
        result_sql = result.sql()
        assert "WHERE" in result_sql.upper()


@pytest.mark.unit
class TestOptimizeFull:
    """Tests for the full optimization pipeline."""

    def test_optimize_passthrough(self, optimizer: Optimizer) -> None:
        """Verify simple query passes through optimization unchanged."""
        sql = "SELECT name FROM users WHERE age > 30 ORDER BY name"
        ast = sqlglot.parse_one(sql)
        result = optimizer.optimize(ast)
        assert "name" in result.sql()
        assert "ORDER BY" in result.sql().upper()
