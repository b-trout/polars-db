"""Integration tests for ``JoinValidator`` cardinality enforcement."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from polars_db.exceptions import JoinValidationError

if TYPE_CHECKING:
    from polars_db.connection import Connection


@pytest.mark.integration
class TestJoinValidate:
    """Tests for ``LazyFrame.join(validate=...)`` on live database."""

    def test_validate_one_to_many_passes(self, connection: Connection) -> None:
        """Verify ``validate="1:m"`` passes when left keys are unique.

        Seed: ``users.id`` is the PK → unique. ``orders.user_id`` has
        duplicates (user_id=1 appears twice, user_id=3 appears twice) but
        that is permitted by 1:m.
        """
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="inner",
                validate="1:m",
            )
            .select("name", "amount")
            .collect()
        )
        assert len(result) == 6  # all orders match a user

    def test_validate_one_to_one_fails_on_duplicates(
        self, connection: Connection
    ) -> None:
        """Verify ``validate="1:1"`` raises when right side has duplicates.

        ``orders.user_id`` is NOT unique (user_id=1 → 2 rows, user_id=3
        → 2 rows), so a 1:1 validation must fail.
        """
        lazy = connection.table("users").join(
            connection.table("orders"),
            left_on="id",
            right_on="user_id",
            how="inner",
            validate="1:1",
        )
        with pytest.raises(JoinValidationError):
            lazy.collect()

    def test_validate_many_to_one_passes(self, connection: Connection) -> None:
        """Verify ``validate="m:1"`` passes when right key is unique.

        orders → users. ``orders.user_id`` may duplicate; ``users.id``
        is unique, satisfying m:1.
        """
        result = (
            connection.table("orders")
            .join(
                connection.table("users"),
                left_on="user_id",
                right_on="id",
                how="inner",
                validate="m:1",
            )
            .select("id", "name", "amount")
            .collect()
        )
        assert len(result) == 6

    def test_validate_m_to_m_always_passes(self, connection: Connection) -> None:
        """Verify the default ``validate="m:m"`` skips all validation."""
        result = (
            connection.table("users")
            .join(
                connection.table("orders"),
                left_on="id",
                right_on="user_id",
                how="inner",
                # validate="m:m" is the default — no check performed.
            )
            .select("name", "amount")
            .collect()
        )
        assert len(result) == 6

    def test_validate_many_to_one_fails_on_duplicate_right(
        self, connection: Connection
    ) -> None:
        """Verify ``validate="m:1"`` raises when right key has duplicates.

        Reversed direction: users → orders with m:1 requires
        ``orders.user_id`` to be unique, which it is not.
        """
        lazy = connection.table("users").join(
            connection.table("orders"),
            left_on="id",
            right_on="user_id",
            how="inner",
            validate="m:1",
        )
        with pytest.raises(JoinValidationError):
            lazy.collect()
