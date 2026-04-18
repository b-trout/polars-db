"""Tests for T-SQL JOIN compilation.

SQL Server (T-SQL) does not support the ``USING (...)`` JOIN clause;
sqlglot happily emits it for every dialect and the server replies with
``syntax error 102``. The compiler therefore has to emit an explicit
``ON left.k = right.k`` condition whenever the backend dialect is
``tsql`` and the caller passed ``on=`` keys, while still preserving the
polars-style USING merge semantics (the key column appears once, taken
from the left side; the right-side copy is dropped).

Other dialects must keep the simple ``USING (...)`` form.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from polars_db.backends.duckdb import DuckDBBackend
from polars_db.backends.mysql import MySQLBackend
from polars_db.backends.postgres import PostgresBackend
from polars_db.backends.sqlserver import SQLServerBackend
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import ColExpr
from polars_db.ops import JoinOp, TableRef


def _make_compiler(backend: object, schema: dict[str, list[str]]) -> QueryCompiler:
    conn = MagicMock()
    conn.get_schema = lambda table: schema.get(table, [])
    return QueryCompiler(backend, connection=conn)  # type: ignore[arg-type]


def _compile_sql(backend: object, compiler: QueryCompiler, op: object) -> str:
    ast = compiler.compile(op)  # type: ignore[arg-type]
    return backend.render(ast)  # type: ignore[attr-defined]


@pytest.mark.unit
class TestTSQLJoinUsingReplacement:
    """T-SQL emits ON instead of USING and still merges the key column."""

    def test_simple_join_on_emits_on_not_using(self) -> None:
        """No collision, but T-SQL still needs ON; ``USING`` must not leak."""
        backend = SQLServerBackend()
        compiler = _make_compiler(
            backend,
            {
                "users": ["user_id", "name"],
                "orders": ["user_id", "total"],
            },
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(backend, compiler, op)

        assert "USING" not in sql.upper()
        # Qualified equality on the subquery aliases.
        normalized = sql.replace('"', "")
        assert "_t0.user_id = _t1.user_id" in normalized

    def test_simple_join_drops_right_key_from_projection(self) -> None:
        """Polars-style USING merge: the right-side key must not appear."""
        backend = SQLServerBackend()
        compiler = _make_compiler(
            backend,
            {
                "users": ["user_id", "name"],
                "orders": ["user_id", "total"],
            },
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["user_id", "name", "total"]

        sql = _compile_sql(backend, compiler, op)
        normalized = sql.replace('"', "")
        # Right-side ``user_id`` must not be selected.
        assert "_t1.user_id AS" not in normalized
        # The key is taken from the left side, qualified with the alias.
        assert "_t0.user_id" in normalized

    def test_collision_with_on_uses_on_and_suffixes_right_duplicate(
        self,
    ) -> None:
        """Collisions on non-key columns still suffix on T-SQL too."""
        backend = SQLServerBackend()
        compiler = _make_compiler(
            backend,
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            },
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(backend, compiler, op)
        normalized = sql.replace('"', "")

        assert "USING" not in normalized.upper()
        assert "_t0.user_id = _t1.user_id" in normalized
        # ``id`` collides; right copy is aliased with the default suffix.
        assert "_t1.id AS id_right" in normalized

    def test_multi_key_on_produces_anded_equalities(self) -> None:
        """Multiple USING keys translate to AND-chained equalities on T-SQL."""
        backend = SQLServerBackend()
        compiler = _make_compiler(
            backend,
            {
                "orders": ["region", "order_id", "total"],
                "regions": ["region", "order_id", "country"],
            },
        )
        op = JoinOp(
            left=TableRef(name="orders"),
            right=TableRef(name="regions"),
            on=(ColExpr(name="region"), ColExpr(name="order_id")),
            how="inner",
        )
        sql = _compile_sql(backend, compiler, op)
        normalized = sql.replace('"', "")

        assert "USING" not in normalized.upper()
        assert "_t0.region = _t1.region" in normalized
        assert "_t0.order_id = _t1.order_id" in normalized
        assert " AND " in normalized


@pytest.mark.unit
class TestOtherDialectsKeepUsing:
    """Non-T-SQL dialects must keep the simple ``USING (...)`` form."""

    @pytest.mark.parametrize(
        ("backend_cls", "dialect"),
        [
            (PostgresBackend, "postgres"),
            (DuckDBBackend, "duckdb"),
            (MySQLBackend, "mysql"),
        ],
    )
    def test_simple_join_keeps_using(self, backend_cls: type, dialect: str) -> None:
        backend = backend_cls()
        compiler = _make_compiler(
            backend,
            {
                "users": ["user_id", "name"],
                "orders": ["user_id", "total"],
            },
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = backend.render(compiler.compile(op))  # type: ignore[arg-type]
        normalized = sql.replace('"', "").replace("`", "")
        assert "USING (user_id)" in normalized
        # The explicit ``ON _t0.user_id = _t1.user_id`` path must NOT be
        # taken for non-T-SQL dialects without collisions.
        assert "_t0.user_id = _t1.user_id" not in normalized
