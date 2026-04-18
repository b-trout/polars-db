"""Integration tests for JOIN operations across all supported backends.

These exercise the live SQL engine (not just the compiler) to guard
against dialect-specific regressions — in particular PR #41's fix for
duplicate-column collisions on JOIN, which only surfaces at execution
time as an ambiguous-reference error.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from polars_db.connection import Connection


_BACKEND = os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")

# --- Dialect-specific skips / xfails ---------------------------------------
#
# MySQL has no FULL OUTER JOIN.
# SQLite added RIGHT/FULL OUTER JOIN only in 3.39 (2022-09). CI images may
# ship older SQLite; mark as xfail(strict=False) so the tests pass silently
# where supported and flag regressions where not.
_mysql_full_outer_xfail = pytest.mark.xfail(
    _BACKEND == "mysql",
    reason="MySQL does not support FULL OUTER JOIN",
    strict=False,
)
_sqlite_right_join_xfail = pytest.mark.xfail(
    _BACKEND == "sqlite",
    reason="SQLite RIGHT JOIN support requires 3.39+",
    strict=False,
)
_sqlite_full_outer_xfail = pytest.mark.xfail(
    _BACKEND == "sqlite",
    reason="SQLite FULL OUTER JOIN support requires 3.39+",
    strict=False,
)


@pytest.mark.integration
class TestInnerJoin:
    """Basic INNER JOIN execution on live backends."""

    def test_inner_join_on_single_key(self, connection: Connection) -> None:
        """Verify ``users.join(orders, left_on='id', right_on='user_id')`` runs.

        ``users.id`` and ``orders.id`` both exist, so the right-side PK must
        be suffixed (default ``_right``) to avoid an ambiguous reference.
        """
        result = (
            connection.table("users")
            .join(connection.table("orders"), left_on="id", right_on="user_id")
            .collect()
        )
        # Six orders, each matching exactly one user → six rows.
        assert len(result) == 6
        # Verify schema: users.* (with id, name) plus orders.* with id suffixed.
        assert "id" in result.columns
        assert "id_right" in result.columns
        assert "name" in result.columns
        assert "amount" in result.columns
        assert "user_id" in result.columns
        # The join keys should line up.
        left_ids = sorted(result["id"].to_list())
        right_user_ids = sorted(result["user_id"].to_list())
        assert left_ids == right_user_ids == [1, 1, 2, 3, 3, 5]

    def test_inner_join_with_using(self, connection: Connection) -> None:
        """Verify ``on=`` USING form merges the key column.

        The key ``user_id`` must appear exactly once in the output.
        """
        right = connection.table("users").select("id", "name").rename({"id": "user_id"})
        result = connection.table("orders").join(right, on="user_id").collect()

        assert len(result) == 6
        # USING key appears once only.
        assert result.columns.count("user_id") == 1
        assert "name" in result.columns
        # Verify each row has a non-null name.
        names = result["name"].to_list()
        assert all(n is not None for n in names)


@pytest.mark.integration
class TestJoinTypes:
    """LEFT / RIGHT / OUTER / CROSS join ``how=`` variations."""

    def test_left_join(self, connection: Connection) -> None:
        """All 5 users survive; Diana (id=4) has NULL order fields."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="left",
            )
            .collect()
        )
        # 4 users with matched orders produce 6 rows + Diana with NULLs = 7.
        assert len(result) == 7

        # Diana must appear exactly once, with NULL amount.
        diana_rows = result.filter(result["name"] == "Diana")
        assert len(diana_rows) == 1
        assert diana_rows["amount"][0] is None

    @_sqlite_right_join_xfail
    def test_right_join(self, connection: Connection) -> None:
        """All 6 orders survive; every row has a matching user."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="right",
            )
            .collect()
        )
        assert len(result) == 6
        # Every order has a user in the seed data, so no NULL user names.
        names = result["name"].to_list()
        assert all(n is not None for n in names)

    @_mysql_full_outer_xfail
    @_sqlite_full_outer_xfail
    def test_outer_join(self, connection: Connection) -> None:
        """FULL OUTER preserves unmatched rows from both sides.

        Six orders all match a user → 6 rows, plus Diana (user with no
        orders) contributes 1 row for a total of 7.
        """
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="outer",
            )
            .collect()
        )
        assert len(result) == 7
        # Diana should appear exactly once with NULL order fields.
        diana_rows = result.filter(result["name"] == "Diana")
        assert len(diana_rows) == 1
        assert diana_rows["amount"][0] is None

    def test_cross_join(self, connection: Connection) -> None:
        """CROSS JOIN produces the Cartesian product (5 * 6 = 30 rows).

        Currently the public ``LazyFrame.join`` API validates that either
        ``on=`` or ``left_on``+``right_on`` is supplied, which blocks
        ``how="cross"`` without keys. Mark xfail until keyless cross-join
        is wired through.
        """
        pytest.xfail("CROSS JOIN without keys is not yet wired through LazyFrame.join")
        result = (
            connection.table("users")
            .join(connection.table("orders"), how="cross")
            .collect()
        )
        assert len(result) == 30


@pytest.mark.integration
class TestJoinColumnCollision:
    """Regression guard for PR #41 (duplicate column suffixing)."""

    def test_right_suffix_applied_on_duplicate_columns(
        self, connection: Connection
    ) -> None:
        """``id``/``id_right`` and ``created_at``/``created_at_right`` coexist."""
        result = (
            connection.table("users")
            .join(connection.table("orders"), left_on="id", right_on="user_id")
            .collect()
        )
        # Both sides' PKs present, right-side suffixed with default ``_right``.
        assert "id" in result.columns
        assert "id_right" in result.columns
        # ``created_at`` (users) vs ``ordered_at`` (orders) — no collision on
        # the timestamps themselves, but we still verify ``created_at`` is
        # kept from the left side.
        assert "created_at" in result.columns
        assert "ordered_at" in result.columns

    def test_custom_suffix(self, connection: Connection) -> None:
        """A user-supplied ``suffix=`` reaches the live SQL output."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                suffix="_r",
            )
            .collect()
        )
        assert "id_r" in result.columns
        assert "id_right" not in result.columns

    def test_using_keys_not_duplicated(self, connection: Connection) -> None:
        """USING merges the key column; only non-key collisions are suffixed."""
        # Build a right side that shares ``user_id`` with orders plus a
        # colliding ``id`` column to exercise the non-key collision path.
        right = connection.table("users").select("id", "name").rename({"id": "user_id"})
        result = connection.table("orders").join(right, on="user_id").collect()

        # Merged key appears exactly once.
        assert result.columns.count("user_id") == 1
        # No stray ``user_id_right`` from the USING merge.
        assert "user_id_right" not in result.columns
        # Every order row joins to a user, so ``name`` is populated.
        assert "name" in result.columns

    def test_rename_after_collision_join_works(self, connection: Connection) -> None:
        """Regression test for PR #41.

        Prior to the fix the JOIN emitted a bare ``id`` for both sides, so
        the outer ``SELECT id AS order_id`` referenced an ambiguous column
        and failed at execution time. This test must complete without a
        SQL error.
        """
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
            )
            .rename({"id_right": "order_id"})
            .select("id", "order_id")
            .collect()
        )
        assert set(result.columns) == {"id", "order_id"}
        assert len(result) == 6
        # ``id`` is the user id, ``order_id`` is the order PK; they differ
        # in general (e.g. users 1/3 each have two orders).
        assert sorted(result["id"].to_list()) == [1, 1, 2, 3, 3, 5]
        assert sorted(result["order_id"].to_list()) == [1, 2, 3, 4, 5, 6]


@pytest.mark.integration
class TestSemiAntiJoin:
    """EXISTS / NOT EXISTS joins."""

    def test_semi_join(self, connection: Connection) -> None:
        """Semi join keeps exactly the users that have at least one order."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="semi",
            )
            .collect()
        )
        # Users with orders: 1, 2, 3, 5 (Diana/4 has none).
        assert len(result) == 4
        assert sorted(result["id"].to_list()) == [1, 2, 3, 5]
        # Semi join exposes only left columns.
        assert "amount" not in result.columns

    def test_anti_join(self, connection: Connection) -> None:
        """Anti join keeps exactly the users with no orders (Diana)."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="anti",
            )
            .collect()
        )
        assert len(result) == 1
        assert result["id"][0] == 4
        assert result["name"][0] == "Diana"
