"""Tests for JOIN column name collision handling."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from polars_db.backends.postgres import PostgresBackend
from polars_db.compiler.query_compiler import QueryCompiler
from polars_db.expr import ColExpr
from polars_db.ops import JoinOp, RenameOp, TableRef


def _make_compiler(schema: dict[str, list[str]]) -> QueryCompiler:
    """Create a compiler with a mocked connection for schema resolution."""
    conn = MagicMock()
    conn.get_schema = lambda table: schema.get(table, [])
    return QueryCompiler(PostgresBackend(), connection=conn)


def _compile_sql(compiler: QueryCompiler, op: object) -> str:
    ast = compiler.compile(op)  # type: ignore[arg-type]
    return ast.sql(dialect="postgres")


@pytest.mark.unit
class TestResolveColumnsJoinCollision:
    """Tests for ``_resolve_columns`` on JOIN with name collisions."""

    def test_using_keys_merged_and_duplicates_suffixed(self) -> None:
        """USING keys appear once, other right duplicates gain the suffix."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["user_id", "id", "name", "id_right", "total"]

    def test_right_unique_columns_kept_unchanged(self) -> None:
        """Non-colliding right columns keep their original name."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "name"],
                "orders": ["user_id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["user_id", "name", "total"]

    def test_left_on_right_on_all_right_keys_emitted(self) -> None:
        """With left_on/right_on there is no USING merge; key columns remain.

        Collisions on non-key columns still receive the suffix.
        """
        compiler = _make_compiler(
            {
                "users": ["id", "created_at", "name"],
                "orders": ["user_id", "created_at", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            left_on=(ColExpr(name="id"),),
            right_on=(ColExpr(name="user_id"),),
            how="inner",
        )
        cols = compiler._resolve_columns(op)
        assert cols == [
            "id",
            "created_at",
            "name",
            "user_id",
            "created_at_right",
            "total",
        ]

    def test_semi_join_returns_only_left_columns(self) -> None:
        """Semi joins expose only the left schema."""
        compiler = _make_compiler(
            {
                "users": ["id", "name"],
                "orders": ["id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="id"),),
            how="semi",
        )
        assert compiler._resolve_columns(op) == ["id", "name"]

    def test_anti_join_returns_only_left_columns(self) -> None:
        """Anti joins expose only the left schema."""
        compiler = _make_compiler(
            {
                "users": ["id", "name"],
                "orders": ["id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="id"),),
            how="anti",
        )
        assert compiler._resolve_columns(op) == ["id", "name"]

    def test_custom_suffix(self) -> None:
        """A user-supplied suffix is applied to right-side duplicates."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
            suffix="_r",
        )
        cols = compiler._resolve_columns(op)
        assert cols == ["user_id", "id", "name", "id_r", "total"]


@pytest.mark.unit
class TestCompileJoinCollision:
    """Tests for the SQL generated for JOINs with name collisions."""

    def test_collision_emits_qualified_projection_with_alias(self) -> None:
        """Collisions trigger table-qualified cols and a suffixed alias."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(compiler, op)
        # USING key stays unqualified; left non-key qualified by _t0; right
        # duplicate ``id`` aliased with the default ``_right`` suffix.
        assert "USING (user_id)" in sql or 'USING ("user_id")' in sql
        assert "id AS id_right" in sql or '"id" AS "id_right"' in sql
        # Right non-colliding column is qualified but not aliased.
        assert "total" in sql

    def test_no_collision_preserves_simple_join_sql(self) -> None:
        """Without collisions the compiler keeps the simple USING form."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "name"],
                "orders": ["user_id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(compiler, op)
        # No explicit qualified projection; the simple form shows up.
        assert " AS id_right" not in sql
        assert " AS name_right" not in sql
        # Still a USING JOIN.
        assert "JOIN" in sql.upper()
        assert "USING" in sql.upper()

    def test_rename_after_collision_join_is_unambiguous(self) -> None:
        """A rename following a colliding join must not produce bare ``id``."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            }
        )
        join = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
        )
        op = RenameOp(child=join, mapping=(("id", "user_id_renamed"),))
        sql = _compile_sql(compiler, op)
        # The inner query must already disambiguate ``id`` via the suffix,
        # otherwise the outer ``SELECT id AS user_id_renamed`` references
        # an ambiguous column.
        assert "id AS id_right" in sql or '"id" AS "id_right"' in sql
        assert "id AS user_id_renamed" in sql or '"id" AS "user_id_renamed"' in sql

    def test_left_on_right_on_collision_uses_on_condition(self) -> None:
        """left_on/right_on with collisions builds qualified ON equality."""
        compiler = _make_compiler(
            {
                "users": ["id", "created_at", "name"],
                "orders": ["user_id", "created_at", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            left_on=(ColExpr(name="id"),),
            right_on=(ColExpr(name="user_id"),),
            how="inner",
        )
        sql = _compile_sql(compiler, op)
        # ``created_at`` collides; right copy is aliased.
        assert "created_at AS created_at_right" in sql or (
            '"created_at" AS "created_at_right"' in sql
        )
        # ON condition should reference the subquery aliases.
        assert "_t0" in sql and "_t1" in sql

    def test_custom_suffix_in_sql(self) -> None:
        """Custom suffix flows through to the emitted alias."""
        compiler = _make_compiler(
            {
                "users": ["user_id", "id", "name"],
                "orders": ["user_id", "id", "total"],
            }
        )
        op = JoinOp(
            left=TableRef(name="users"),
            right=TableRef(name="orders"),
            on=(ColExpr(name="user_id"),),
            how="inner",
            suffix="_r",
        )
        sql = _compile_sql(compiler, op)
        assert "id AS id_r" in sql or '"id" AS "id_r"' in sql
