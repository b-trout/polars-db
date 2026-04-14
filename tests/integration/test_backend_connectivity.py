"""Integration tests to verify DDL/DML/SELECT work on each backend."""

from __future__ import annotations

import contextlib
import os

import pytest

import polars_db as pdb
from tests.conftest import BACKEND_CONFIG

_BACKEND = os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")
_tsql_order_by_xfail = pytest.mark.xfail(
    _BACKEND == "sqlserver",
    reason="T-SQL forbids ORDER BY in subqueries without TOP/OFFSET",
)


@pytest.mark.integration
class TestBackendConnectivity:
    """Verify DDL -> INSERT -> SELECT -> DROP works on every backend."""

    TABLE = "_connectivity_test"

    @pytest.fixture(autouse=True, scope="class")
    def conn(self, backend_name: str) -> pdb.Connection:
        """Create a bare connection without seed data."""
        config = dict(BACKEND_CONFIG[backend_name])
        c = pdb.connect(**config)
        yield c
        # Ensure cleanup even if a test fails
        with contextlib.suppress(Exception):
            c.execute_raw(f"DROP TABLE IF EXISTS {self.TABLE}")
        c.close()

    def test_create_table(self, conn: pdb.Connection) -> None:
        """Verify CREATE TABLE DDL executes without error."""
        conn.execute_raw(
            f"CREATE TABLE {self.TABLE} (id INTEGER, name VARCHAR(100), value INTEGER)"
        )

    def test_insert_data(self, conn: pdb.Connection) -> None:
        """Verify INSERT DML executes without error."""
        conn.execute_raw(f"INSERT INTO {self.TABLE} VALUES (1, 'Alice', 10)")
        conn.execute_raw(f"INSERT INTO {self.TABLE} VALUES (2, 'Bob', 20)")
        conn.execute_raw(f"INSERT INTO {self.TABLE} VALUES (3, 'Charlie', 30)")

    def test_select_all(self, conn: pdb.Connection) -> None:
        """Verify SELECT * returns all inserted rows with correct columns."""
        result = conn.table(self.TABLE).collect()
        assert len(result) == 3
        assert set(result.columns) == {"id", "name", "value"}

    def test_filter(self, conn: pdb.Connection) -> None:
        """Verify WHERE clause filters rows correctly."""
        result = conn.table(self.TABLE).filter(pdb.col("value") > 15).collect()
        names = sorted(result["name"].to_list())
        assert names == ["Bob", "Charlie"]

    def test_select_columns(self, conn: pdb.Connection) -> None:
        """Verify SELECT with specific columns returns correct schema."""
        result = conn.table(self.TABLE).select("name", "value").collect()
        assert set(result.columns) == {"name", "value"}
        assert len(result) == 3

    @_tsql_order_by_xfail
    def test_sort(self, conn: pdb.Connection) -> None:
        """Verify ORDER BY sorts results correctly."""
        result = (
            conn.table(self.TABLE)
            .sort("value", descending=True)
            .select("name")
            .collect()
        )
        assert result["name"].to_list() == ["Charlie", "Bob", "Alice"]

    def test_group_by_agg(self, conn: pdb.Connection) -> None:
        """Verify GROUP BY with aggregation returns grouped results."""
        result = (
            conn.table(self.TABLE)
            .group_by("name")
            .agg(pdb.col("value").sum())
            .sort("name")
            .collect()
        )
        assert len(result) == 3

    def test_show_query(self, conn: pdb.Connection) -> None:
        """Verify ``show_query()`` returns valid SQL with WHERE and ORDER BY."""
        query = (
            conn.table(self.TABLE)
            .filter(pdb.col("value") > 10)
            .sort("name")
            .show_query()
        )
        assert self.TABLE in query
        assert "WHERE" in query.upper()
        assert "ORDER BY" in query.upper()

    def test_cleanup(self, conn: pdb.Connection) -> None:
        """Verify DROP TABLE cleans up the test table."""
        conn.execute_raw(f"DROP TABLE {self.TABLE}")
