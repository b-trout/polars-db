"""Integration tests for ``fill_null``, ``is_between``, ``is_in``, ``execute_raw``."""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from polars_db.connection import Connection

_BACKEND = os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")
_IS_BIGQUERY = _BACKEND == "bigquery"


@pytest.mark.integration
class TestFillNull:
    """Tests for ``Expr.fill_null()`` (``COALESCE``) on live database."""

    def test_fill_null_string(self, connection: Connection) -> None:
        """Verify ``fill_null("unknown@x")`` replaces NULL emails."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("email").fill_null("unknown@x").alias("email_safe"),
            )
            .select("name", "email_safe")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["email_safe"].to_list(), strict=True)
        )
        # Charlie's email is NULL in the seed → replaced with "unknown@x".
        assert rows["Charlie"] == "unknown@x"
        assert rows["Alice"] == "alice@example.com"

    def test_fill_null_numeric(self, connection: Connection) -> None:
        """Verify ``fill_null(0)`` replaces NULL ages."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("age").fill_null(0).alias("age_safe"),
            )
            .select("name", "age_safe")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["age_safe"].to_list(), strict=True)
        )
        # Diana's age is NULL in the seed → replaced with 0.
        assert rows["Diana"] == 0
        assert rows["Alice"] == 30
        assert rows["Charlie"] == 35


@pytest.mark.integration
class TestIsBetween:
    """Tests for ``Expr.is_between()`` (``BETWEEN`` SQL) on live database."""

    def test_is_between_inclusive(self, connection: Connection) -> None:
        """Verify ``is_between(25, 30)`` matches rows inclusive of both bounds.

        Seed users with age in [25, 30]: Alice (30), Bob (25), Eve (28).
        Charlie (35) and Diana (NULL) are excluded.
        """
        result = (
            connection.table("users")
            .filter(pdb.col("age").is_between(25, 30))
            .select("name")
            .collect()
        )
        names = set(result["name"].to_list())
        assert names == {"Alice", "Bob", "Eve"}

    def test_is_between_numeric_amount(self, connection: Connection) -> None:
        """Verify ``is_between`` works against DECIMAL amount column."""
        result = (
            connection.table("orders")
            .filter(pdb.col("amount").is_between(100, 200))
            .select("id")
            .collect()
        )
        # orders with amount in [100, 200]: id=1 (100.50), id=2 (200.00),
        # id=5 (150.25). The others are out of range.
        assert set(result["id"].to_list()) == {1, 2, 5}


@pytest.mark.integration
class TestIsIn:
    """Tests for ``Expr.is_in()`` (``IN (...)``) on live database."""

    def test_is_in_strings(self, connection: Connection) -> None:
        """Verify ``is_in`` on a VARCHAR column filters to the given set."""
        result = (
            connection.table("orders")
            .filter(pdb.col("status").is_in(["completed", "pending"]))
            .select("id")
            .collect()
        )
        # completed: ids 1, 2, 4  | pending: ids 3, 6 → 5 rows total.
        assert set(result["id"].to_list()) == {1, 2, 3, 4, 6}

    def test_is_in_numbers(self, connection: Connection) -> None:
        """Verify ``is_in`` on an integer column filters to the given set."""
        result = (
            connection.table("orders")
            .filter(pdb.col("user_id").is_in([1, 2]))
            .select("id")
            .collect()
        )
        # user_id=1 → orders 1, 2 ; user_id=2 → order 3.
        assert set(result["id"].to_list()) == {1, 2, 3}


_skip_bigquery = pytest.mark.skipif(
    _IS_BIGQUERY,
    reason="BigQuery emulator does not support interactive DDL/DML cycles "
    "over the standard client used by this test.",
)


@pytest.mark.integration
class TestExecuteRaw:
    """Tests for ``Connection.execute_raw()`` DDL/DML escape hatch."""

    def test_execute_raw_select(self, connection: Connection) -> None:
        """Verify a raw ``SELECT`` returns a ``polars.DataFrame`` with data."""
        result = connection.execute_raw("SELECT COUNT(*) AS n FROM users")
        # Different backends may uppercase the column name (e.g. ``N``); use
        # positional access to stay dialect-agnostic.
        assert len(result) == 1
        assert result.to_series(0).to_list()[0] == 5

    @_skip_bigquery
    def test_execute_raw_ddl_dml(self, connection: Connection) -> None:
        """Verify CREATE/INSERT/SELECT/DROP cycle via ``execute_raw``.

        Uses a regular (non-TEMP) table with a backend-unique name to
        side-step the TEMP TABLE syntax differences across SQL dialects.
        """
        table = "_misc_exec_raw_tmp"
        with contextlib.suppress(Exception):
            connection.execute_raw(f"DROP TABLE IF EXISTS {table}")
        try:
            connection.execute_raw(f"CREATE TABLE {table} (k INTEGER, v VARCHAR(50))")
            connection.execute_raw(f"INSERT INTO {table} VALUES (1, 'one')")
            connection.execute_raw(f"INSERT INTO {table} VALUES (2, 'two')")

            result = connection.execute_raw(f"SELECT COUNT(*) AS n FROM {table}")
            assert result.to_series(0).to_list()[0] == 2
        finally:
            with contextlib.suppress(Exception):
                connection.execute_raw(f"DROP TABLE IF EXISTS {table}")
