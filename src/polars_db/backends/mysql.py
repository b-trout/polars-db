"""MySQL backend using native PyMySQL driver."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa

from polars_db.backends.base import Backend

if TYPE_CHECKING:
    from pymysql.connections import Connection


class MySQLBackend(Backend):
    """MySQL via native PyMySQL driver."""

    def __init__(self) -> None:
        self._conn: Connection | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "mysql"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall() if columns else []
        if not columns:
            return pa.table({})

        col_data: dict[str, list[object]] = {c: [] for c in columns}
        for row in rows:
            for col_name, value in zip(columns, row, strict=True):
                col_data[col_name].append(value)

        return pa.table(col_data)

    def _get_connection(self, conn_str: str) -> Connection:
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> Connection:
        import pymysql

        parsed = urlparse(conn_str)
        return pymysql.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            database=parsed.path.lstrip("/"),
        )

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "GROUP_CONCAT"}

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None
