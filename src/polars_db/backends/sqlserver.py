"""SQL Server backend using native pymssql driver."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa

from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError

if TYPE_CHECKING:
    from pymssql import Connection


class SQLServerBackend(Backend):
    """SQL Server via native pymssql driver."""

    def __init__(self) -> None:
        self._conn: Connection | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "tsql"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall() if columns else []
        conn.commit()
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
        import pymssql

        parsed = urlparse(conn_str)
        server = parsed.hostname or "localhost"
        port = str(parsed.port or 1433)
        user = parsed.username or "sa"
        password = parsed.password or ""
        database = parsed.path.lstrip("/")

        # Ensure the target database exists
        master = pymssql.connect(
            server=server, port=port, user=user, password=password, database="master"
        )
        master.autocommit(True)
        cursor = master.cursor()
        cursor.execute(f"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]")
        master.close()

        return pymssql.connect(
            server=server,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        msg = (
            "SQL Server does not support EXPLAIN. "
            "Use SET SHOWPLAN_XML ON via execute_raw() as a workaround."
        )
        raise BackendNotSupportedError(msg)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None
