"""Tests for Optimizer."""

import pytest
import sqlglot
from sqlglot import expressions as exp

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

    def test_does_not_rewrite_outer_from_in_place(self, optimizer: Optimizer) -> None:
        """Verify the outer AST's FROM clause is not mutated in place.

        The method returns the inner SELECT. The previous implementation
        also performed ``ast.set("from", exp.From(this=inner))`` on the
        outer AST as dead work — confirm we no longer mutate the caller's
        FROM clause (which still references the original Subquery).
        """
        sql = (
            "SELECT * FROM (SELECT * FROM users WHERE age > 30) AS _q "
            "WHERE name = 'Alice'"
        )
        ast = sqlglot.parse_one(sql)
        assert isinstance(ast, exp.Select)

        # sqlglot v30+ uses the ``from_`` key; older releases used ``from``.
        # Mirror the production code's tolerant lookup.
        original_from = ast.args.get("from") or ast.args.get("from_")
        assert original_from is not None
        original_from_this = original_from.this

        result = optimizer._merge_consecutive_filters(ast)

        # The collapsed query is a new root — the returned value is the
        # inner SELECT, not the outer AST.
        assert result is not ast
        # The outer AST's FROM clause still points to the original
        # Subquery node (i.e. was not replaced by a fresh exp.From).
        current_from = ast.args.get("from") or ast.args.get("from_")
        assert current_from is original_from
        assert original_from.this is original_from_this


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

    def test_optimize_three_level_nesting(self, optimizer: Optimizer) -> None:
        """Verify 3-level nested WHERE subqueries collapse in one call.

        ``SELECT * FROM (SELECT * FROM (SELECT * FROM t WHERE a)
        WHERE b) WHERE c`` should fold into ``SELECT * FROM t
        WHERE a AND b AND c`` in a single ``optimize()`` invocation.
        """
        sql = (
            "SELECT * FROM ("
            "SELECT * FROM ("
            "SELECT * FROM t WHERE a = 1"
            ") AS _q1 WHERE b = 2"
            ") AS _q2 WHERE c = 3"
        )
        ast = sqlglot.parse_one(sql)
        result = optimizer.optimize(ast)
        result_sql = result.sql().upper()

        # All three predicates survive and combine with AND.
        assert "A = 1" in result_sql
        assert "B = 2" in result_sql
        assert "C = 3" in result_sql
        assert result_sql.count(" AND ") >= 2
        # Subquery wrappers have fully collapsed — exactly one SELECT.
        assert result_sql.count("SELECT") == 1
        # The final FROM is a bare table, not a Subquery.
        assert isinstance(result, exp.Select)
        from_clause = result.args.get("from") or result.args.get("from_")
        assert from_clause is not None
        assert not isinstance(from_clause.this, exp.Subquery)
