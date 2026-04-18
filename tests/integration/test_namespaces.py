"""Integration tests for ``str.*`` and ``dt.*`` namespace functions."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from polars_db.connection import Connection

_BACKEND = os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")


@pytest.mark.integration
class TestStringNamespace:
    """Tests for ``col().str.*`` functions on live database."""

    def test_str_contains(self, connection: Connection) -> None:
        """Verify ``str.contains`` compiles to LIKE and filters rows."""
        result = (
            connection.table("users")
            .filter(pdb.col("email").str.contains("example"))
            .select("name")
            .collect()
        )
        names = set(result["name"].to_list())
        # Charlie's email is NULL → NULL LIKE ... is NULL → excluded.
        # All remaining users have an ``@example.com`` email.
        assert names == {"Alice", "Bob", "Diana", "Eve"}

    def test_str_starts_with(self, connection: Connection) -> None:
        """Verify ``str.starts_with`` matches only names starting with "A"."""
        result = (
            connection.table("users")
            .filter(pdb.col("name").str.starts_with("A"))
            .select("name")
            .collect()
        )
        assert result["name"].to_list() == ["Alice"]

    def test_str_ends_with(self, connection: Connection) -> None:
        """Verify ``str.ends_with`` matches the common email suffix."""
        result = (
            connection.table("users")
            .filter(pdb.col("email").str.ends_with("@example.com"))
            .select("name")
            .collect()
        )
        names = set(result["name"].to_list())
        assert names == {"Alice", "Bob", "Diana", "Eve"}

    def test_str_upper_lower(self, connection: Connection) -> None:
        """Verify ``str.to_uppercase()`` and ``str.to_lowercase()`` evaluate."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("name").str.to_uppercase().alias("name_upper"),
                pdb.col("name").str.to_lowercase().alias("name_lower"),
            )
            .select("name", "name_upper", "name_lower")
            .filter(pdb.col("name") == "Alice")
            .collect()
        )
        assert result["name_upper"][0] == "ALICE"
        assert result["name_lower"][0] == "alice"

    def test_str_length(self, connection: Connection) -> None:
        """Verify ``str.len_chars()`` returns the character count."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("name").str.len_chars().alias("name_len"),
            )
            .select("name", "name_len")
            .filter(pdb.col("name") == "Charlie")
            .collect()
        )
        assert result["name_len"][0] == len("Charlie")


_dt_extract_xfail = pytest.mark.xfail(
    _BACKEND in ("sqlserver", "sqlite"),
    reason="T-SQL uses DATEPART(year, ...) and SQLite uses strftime; "
    "neither accepts the ANSI ``EXTRACT(YEAR FROM ...)`` emitted by "
    "compiler/expr_compiler.py ``_EXTRACT_FUNCS``.",
    strict=False,
)


@pytest.mark.integration
class TestDateNamespace:
    """Tests for ``col().dt.*`` functions on live database.

    The seed data populates ``created_at`` with ``CURRENT_TIMESTAMP``;
    we therefore assert against *types*/*ranges* rather than exact
    values since the value depends on when the suite runs.
    """

    @_dt_extract_xfail
    def test_dt_year(self, connection: Connection) -> None:
        """Verify ``dt.year()`` returns an integer in the expected range."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("created_at").dt.year().alias("yr"),
            )
            .select("name", "yr")
            .filter(pdb.col("name") == "Alice")
            .collect()
        )
        # PostgreSQL's ``EXTRACT`` returns ``NUMERIC`` → polars maps it to
        # ``Decimal``; other backends return ``int``. Accept either.
        year_value = int(result["yr"][0])
        # Seed rows are inserted with CURRENT_TIMESTAMP — year must be >= 2024.
        assert year_value >= 2024

    @_dt_extract_xfail
    def test_dt_month(self, connection: Connection) -> None:
        """Verify ``dt.month()`` returns a valid 1-12 month integer."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("created_at").dt.month().alias("mo"),
            )
            .select("name", "mo")
            .filter(pdb.col("name") == "Alice")
            .collect()
        )
        month_value = int(result["mo"][0])
        assert 1 <= month_value <= 12

    @_dt_extract_xfail
    def test_dt_day(self, connection: Connection) -> None:
        """Verify ``dt.day()`` returns a valid 1-31 day integer."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.col("created_at").dt.day().alias("d"),
            )
            .select("name", "d")
            .filter(pdb.col("name") == "Alice")
            .collect()
        )
        day_value = int(result["d"][0])
        assert 1 <= day_value <= 31
