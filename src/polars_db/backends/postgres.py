"""PostgreSQL backend using native psycopg2 driver."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from polars_db.backends.base import Backend

if TYPE_CHECKING:
    from psycopg2.extensions import connection


class PostgresBackend(Backend):
    """PostgreSQL via native psycopg2 driver."""

    def __init__(self) -> None:
        self._conn: connection | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "postgres"

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

    def _get_connection(self, conn_str: str) -> connection:
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> connection:
        import psycopg2

        return psycopg2.connect(conn_str)

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None
