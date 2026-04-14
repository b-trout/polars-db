"""Integration tests to verify DDL/DML/SELECT work on each backend."""

from __future__ import annotations

import contextlib
import os

import pytest

import polars_db as pdb
from tests.conftest import BACKEND_CONFIG

_BACKEND = os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")
_IS_BIGQUERY = _BACKEND == "bigquery"
_skip_bigquery = pytest.mark.skipif(_IS_BIGQUERY, reason="BigQuery tested separately")
_tsql_order_by_xfail = pytest.mark.xfail(
    _BACKEND == "sqlserver",
    reason="T-SQL forbids ORDER BY in subqueries without TOP/OFFSET",
)


@pytest.mark.integration
@_skip_bigquery
class TestBackendConnectivity:
    """Verify DDL -> INSERT -> SELECT -> DROP works on every backend."""

    TABLE = "_connectivity_test"

    @pytest.fixture(autouse=True, scope="class")
    def conn(self, backend_name: str) -> pdb.Connection:
        """Create a bare connection without seed data."""
        config = dict(BACKEND_CONFIG[backend_name])
        c = pdb.connect(**config)
        yield c
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


@pytest.mark.integration
@pytest.mark.skipif(not _IS_BIGQUERY, reason="BigQuery-specific tests")
class TestBigQueryConnectivity:
    """Verify BigQuery SQL generation and SELECT execution on emulator."""

    TABLE = "test_dataset._bq_connectivity"

    @pytest.fixture(autouse=True, scope="class")
    def conn(self, backend_name: str) -> pdb.Connection:
        """Create a BigQuery connection with emulator env set."""
        if not os.environ.get("BIGQUERY_EMULATOR_HOST"):
            os.environ["BIGQUERY_EMULATOR_HOST"] = "localhost:9050"
        config = dict(BACKEND_CONFIG[backend_name])
        c = pdb.connect(**config)
        yield c
        with contextlib.suppress(Exception):
            c.execute_raw(f"DROP TABLE IF EXISTS {self.TABLE}")
        c.close()

    def test_select_literal(self, conn: pdb.Connection) -> None:
        """Verify SELECT with computed values works on the emulator."""
        result = conn.execute_raw("SELECT 1 AS id, 'hello' AS name")
        assert len(result) == 1
        assert "id" in result.columns

    def test_create_table(self, conn: pdb.Connection) -> None:
        """Verify CREATE TABLE DDL executes without error."""
        conn.execute_raw(
            f"CREATE TABLE {self.TABLE} (id INT64, name STRING, value INT64)"
        )

    def test_show_query_filter(self, conn: pdb.Connection) -> None:
        """Verify SQL generation for filter produces valid BigQuery SQL."""
        query = conn.table(self.TABLE).filter(pdb.col("value") > 10).show_query()
        assert self.TABLE in query
        assert "WHERE" in query.upper()

    def test_show_query_select(self, conn: pdb.Connection) -> None:
        """Verify SQL generation for column selection."""
        query = conn.table(self.TABLE).select("name", "value").show_query()
        assert "name" in query
        assert "value" in query

    def test_show_query_sort(self, conn: pdb.Connection) -> None:
        """Verify SQL generation for ORDER BY."""
        query = conn.table(self.TABLE).sort("name").show_query()
        assert "ORDER BY" in query.upper()

    def test_show_query_group_by(self, conn: pdb.Connection) -> None:
        """Verify SQL generation for GROUP BY with aggregation."""
        query = (
            conn.table(self.TABLE)
            .group_by("name")
            .agg(pdb.col("value").sum())
            .show_query()
        )
        assert "GROUP BY" in query.upper()
        assert "SUM" in query.upper()

    def test_cleanup(self, conn: pdb.Connection) -> None:
        """Verify DROP TABLE cleans up the test table."""
        conn.execute_raw(f"DROP TABLE IF EXISTS {self.TABLE}")
