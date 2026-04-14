"""DuckDB backend using native duckdb driver.

connectorx does not support DuckDB, so we use the duckdb package
directly which provides native Arrow output.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from polars_db.backends.base import Backend

if TYPE_CHECKING:
    import pyarrow as pa


class DuckDBBackend(Backend):
    """DuckDB via native duckdb driver."""

    def __init__(self) -> None:
        self._conn: object | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        import pyarrow

        conn = self._get_connection(conn_str)
        result = conn.execute(sql)  # type: ignore[union-attr]
        desc = result.description
        if not desc:
            # DDL statements don't produce results
            return pyarrow.table({})
        return result.fetch_arrow_table()

    def _get_connection(self, conn_str: str) -> object:
        """Lazy-initialise the DuckDB connection."""
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> object:
        import duckdb

        # duckdb:///:memory: -> :memory:
        # duckdb:///path/to/db -> path/to/db
        path = conn_str.replace("duckdb:///", "").replace("duckdb://", "")
        if not path:
            path = ":memory:"
        return duckdb.connect(path)

    def schema_query(self, table: str) -> str:
        """DuckDB uses information_schema like PostgreSQL."""
        return (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table}'"
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()  # type: ignore[union-attr]
            self._conn = None
            self._conn_str = None
