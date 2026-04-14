"""Tests for QueryCompiler._resolve_columns()."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import AliasExpr, ColExpr
from polars_db.ops import (
    FilterOp,
    LimitOp,
    SelectOp,
    SortOp,
    TableRef,
)


def _make_compiler(schema: dict[str, list[str]] | None = None) -> QueryCompiler:
    """Create a compiler with a mocked connection for schema resolution."""
    conn = MagicMock()
    if schema:
        conn.get_schema = lambda table: schema.get(table, [])
    return QueryCompiler(PostgresBackend(), connection=conn)


@pytest.mark.unit
class TestResolveColumnsTableRef:
    """Tests for column resolution from ``TableRef`` via schema lookup."""

    def test_table_ref(self) -> None:
        """Verify columns are resolved from the mocked schema."""
        compiler = _make_compiler({"users": ["id", "name", "age"]})
        cols = compiler._resolve_columns(TableRef(name="users"))
        assert cols == ["id", "name", "age"]


@pytest.mark.unit
class TestResolveColumnsSelect:
    """Tests for column resolution from ``SelectOp``."""

    def test_select(self) -> None:
        """Verify column names extracted from ``ColExpr`` selections."""
        compiler = _make_compiler()
        op = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"), ColExpr(name="age")),
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["name", "age"]

    def test_select_with_alias(self) -> None:
        """Verify aliased column names are resolved correctly."""
        compiler = _make_compiler()
        op = SelectOp(
            child=TableRef(name="users"),
            exprs=(
                ColExpr(name="name"),
                AliasExpr(expr=ColExpr(name="age"), alias="user_age"),
            ),
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["name", "user_age"]


@pytest.mark.unit
class TestResolveColumnsTransparent:
    """Ops that pass through child columns unchanged."""

    def test_filter(self) -> None:
        """Verify ``FilterOp`` passes through child columns."""
        compiler = _make_compiler()
        inner = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"), ColExpr(name="age")),
        )
        op = FilterOp(child=inner, predicate=ColExpr(name="age"))
        cols = compiler._resolve_columns(op)
        assert cols == ["name", "age"]

    def test_sort(self) -> None:
        """Verify ``SortOp`` passes through child columns."""
        compiler = _make_compiler()
        inner = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="name"),),
        )
        op = SortOp(child=inner, by=(ColExpr(name="name"),), descending=(False,))
        cols = compiler._resolve_columns(op)
        assert cols == ["name"]

    def test_limit(self) -> None:
        """Verify ``LimitOp`` passes through child columns."""
        compiler = _make_compiler()
        inner = SelectOp(
            child=TableRef(name="users"),
            exprs=(ColExpr(name="id"),),
        )
        op = LimitOp(child=inner, n=10)
        cols = compiler._resolve_columns(op)
        assert cols == ["id"]
