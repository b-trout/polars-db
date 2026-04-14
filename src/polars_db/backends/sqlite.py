"""SQLite backend using Python stdlib sqlite3."""

from __future__ import annotations

import sqlite3

import pyarrow as pa

from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError


class SQLiteBackend(Backend):
    """SQLite via Python stdlib sqlite3."""

    def __init__(self) -> None:
        self._conn: sqlite3.Connection | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "sqlite"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.execute(sql)
        conn.commit()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        if not columns:
            return pa.table({})

        # Build columnar data for Arrow
        col_data: dict[str, list[object]] = {c: [] for c in columns}
        for row in rows:
            for col_name, value in zip(columns, row, strict=True):
                col_data[col_name].append(value)

        return pa.table(col_data)

    def _get_connection(self, conn_str: str) -> sqlite3.Connection:
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> sqlite3.Connection:
        # sqlite:///:memory: -> :memory:
        # sqlite:///path/to/db -> path/to/db
        path = conn_str.replace("sqlite:///", "").replace("sqlite://", "")
        if not path:
            path = ":memory:"
        return sqlite3.connect(path)

    def schema_query(self, table: str) -> str:
        return f"SELECT name AS column_name FROM pragma_table_info('{table}')"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            msg = "SQLite does not support EXPLAIN ANALYZE."
            raise BackendNotSupportedError(msg)
        return f"EXPLAIN QUERY PLAN {sql}"

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None
