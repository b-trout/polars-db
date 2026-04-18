"""SQL Server backend using native pymssql driver."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import sqlglot.expressions as exp

from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError

if TYPE_CHECKING:
    from pymssql import Connection


_VALID_DB_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")


def _validate_db_identifier(name: str) -> str:
    """Validate a SQL Server database identifier for safe DDL embedding.

    Only allows ``[A-Za-z_][A-Za-z0-9_]{0,127}`` to prevent T-SQL injection
    via a crafted connection string (e.g. ``foo]; DROP DATABASE master; --``
    which could otherwise break out of the bracketed identifier in the
    auto-create ``CREATE DATABASE [...]`` path).
    """
    if not _VALID_DB_NAME.fullmatch(name):
        msg = f"Invalid SQL Server database name: {name!r}"
        raise ValueError(msg)
    return name


class SQLServerBackend(Backend):
    """SQL Server via native pymssql driver."""

    def __init__(self, *, create_if_missing: bool = False) -> None:
        self._conn: Connection | None = None
        self._conn_str: str | None = None
        self._create_if_missing = create_if_missing

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

    def _create_connection(self, conn_str: str) -> Connection:
        import pymssql

        parsed = urlparse(conn_str)
        server = parsed.hostname or "localhost"
        port = str(parsed.port or 1433)
        user = parsed.username or "sa"
        password = parsed.password or ""
        database = _validate_db_identifier(parsed.path.lstrip("/"))

        if self._create_if_missing:
            # Ensure the target database exists.
            # database is regex-restricted above, but apply T-SQL escaping as
            # defence-in-depth: ] -> ]] inside brackets and ' -> '' inside strings.
            bracketed = database.replace("]", "]]")
            quoted = database.replace("'", "''")
            master = pymssql.connect(
                server=server,
                port=port,
                user=user,
                password=password,
                database="master",
            )
            master.autocommit(True)
            cursor = master.cursor()
            cursor.execute(
                f"IF DB_ID('{quoted}') IS NULL CREATE DATABASE [{bracketed}]"
            )
            master.close()

        return pymssql.connect(
            server=server,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    def render(self, ast: exp.Expression) -> str:
        """Render AST to T-SQL, adding OFFSET 0 ROWS to subquery ORDER BY.

        SQL Server forbids ORDER BY inside derived tables unless TOP or
        OFFSET is also present.  Walk the tree and patch any subquery
        whose inner SELECT has ORDER BY but no OFFSET/LIMIT.
        """
        for subquery in ast.find_all(exp.Subquery):
            inner = subquery.this
            if not isinstance(inner, exp.Select):
                continue
            has_order = inner.args.get("order") is not None
            has_limit = inner.args.get("limit") is not None
            has_offset = inner.args.get("offset") is not None
            if has_order and not has_limit and not has_offset:
                inner.set("offset", exp.Offset(expression=exp.Literal.number(0)))
        return ast.sql(dialect=self.dialect, pretty=True)

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def current_schema_sql_expr(self) -> exp.Expression:
        """SQL Server uses ``SCHEMA_NAME()`` for the current default schema."""
        return exp.Anonymous(this="SCHEMA_NAME")

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
