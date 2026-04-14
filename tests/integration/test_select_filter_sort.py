"""Integration tests for SELECT + WHERE + ORDER BY."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from polars_db.connection import Connection


@pytest.mark.integration
class TestSimpleFilter:
    def test_filter_age_gt(self, connection: Connection) -> None:
        result = connection.table("users").filter(pdb.col("age") > 30).collect()
        names = result["name"].to_list()
        assert "Charlie" in names
        assert "Bob" not in names

    def test_filter_eq(self, connection: Connection) -> None:
        result = connection.table("users").filter(pdb.col("name") == "Alice").collect()
        assert len(result) == 1
        assert result["name"][0] == "Alice"


@pytest.mark.integration
class TestSelect:
    def test_select_columns(self, connection: Connection) -> None:
        result = connection.table("users").select("name", "age").collect()
        assert "name" in result.columns
        assert "age" in result.columns
        assert len(result) == 5


@pytest.mark.integration
class TestSort:
    def test_sort_asc(self, connection: Connection) -> None:
        result = (
            connection.table("users")
            .filter(pdb.col("age").is_not_null())
            .sort("age")
            .select("name", "age")
            .collect()
        )
        ages = result["age"].to_list()
        assert ages == sorted(ages)

    def test_sort_desc(self, connection: Connection) -> None:
        result = (
            connection.table("users")
            .filter(pdb.col("age").is_not_null())
            .sort("age", descending=True)
            .select("name", "age")
            .collect()
        )
        ages = result["age"].to_list()
        assert ages == sorted(ages, reverse=True)


@pytest.mark.integration
class TestLimit:
    def test_limit(self, connection: Connection) -> None:
        result = connection.table("users").limit(2).collect()
        assert len(result) == 2

    def test_head(self, connection: Connection) -> None:
        result = connection.table("users").head(3).collect()
        assert len(result) == 3
