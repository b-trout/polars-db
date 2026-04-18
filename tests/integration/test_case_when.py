"""Integration tests for ``when().then().otherwise()`` CASE chains."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from polars_db.connection import Connection


@pytest.mark.integration
class TestCaseWhen:
    """Tests for CASE WHEN expressions on live database."""

    def test_simple_when_otherwise(self, connection: Connection) -> None:
        """Verify a 2-branch ``when().then().otherwise()`` classifies rows.

        Seed data has age > 30 only for Charlie (35). Alice (30),
        Bob (25), Eve (28), and Diana (NULL) fall into the otherwise branch.
        """
        result = (
            connection.table("users")
            .with_columns(
                pdb.when(pdb.col("age") > 30)
                .then("senior")
                .otherwise("junior")
                .alias("tier"),
            )
            .select("name", "tier")
            .sort("name")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["tier"].to_list(), strict=True)
        )
        assert rows["Charlie"] == "senior"
        assert rows["Alice"] == "junior"
        assert rows["Bob"] == "junior"
        assert rows["Eve"] == "junior"
        # Diana has age NULL; NULL > 30 is NULL → otherwise branch.
        assert rows["Diana"] == "junior"

    def test_multiple_when_branches(self, connection: Connection) -> None:
        """Verify a 3-branch chain ``senior / adult / junior``."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.when(pdb.col("age") >= 30)
                .then("senior")
                .when(pdb.col("age") >= 25)
                .then("adult")
                .otherwise("junior")
                .alias("tier"),
            )
            .select("name", "tier")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["tier"].to_list(), strict=True)
        )
        assert rows["Alice"] == "senior"  # 30
        assert rows["Charlie"] == "senior"  # 35
        assert rows["Bob"] == "adult"  # 25
        assert rows["Eve"] == "adult"  # 28
        # Diana's age is NULL → both WHEN conditions NULL → otherwise.
        assert rows["Diana"] == "junior"

    def test_when_returning_numeric(self, connection: Connection) -> None:
        """Verify ``when()`` can return numeric values (0/1 flag pattern)."""
        result = (
            connection.table("users")
            .with_columns(
                pdb.when(pdb.col("age") > 30).then(1).otherwise(0).alias("is_senior"),
            )
            .select("name", "is_senior")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["is_senior"].to_list(), strict=True)
        )
        assert rows["Charlie"] == 1
        assert rows["Alice"] == 0
        assert rows["Bob"] == 0
        assert rows["Diana"] == 0
        assert rows["Eve"] == 0

    def test_nested_column_reference(self, connection: Connection) -> None:
        """Verify a CASE WHEN condition referencing multiple columns.

        Only Charlie has age > 30, but Charlie's email IS NULL, so the
        combined AND evaluates false — the otherwise branch wins for
        every row.
        """
        result = (
            connection.table("users")
            .with_columns(
                pdb.when((pdb.col("age") > 30) & pdb.col("email").is_not_null())
                .then("senior_with_email")
                .otherwise("other")
                .alias("bucket"),
            )
            .select("name", "bucket")
            .collect()
        )
        rows = dict(
            zip(result["name"].to_list(), result["bucket"].to_list(), strict=True)
        )
        assert rows["Charlie"] == "other"  # age > 30 but email NULL
        assert rows["Alice"] == "other"
        assert rows["Diana"] == "other"  # email present but age NULL
        assert all(v == "other" for v in rows.values())
