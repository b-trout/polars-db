"""MySQL backend using native PyMySQL driver."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import sqlglot.expressions as exp

from polars_db.backends._thread_local import PerThreadConnections
from polars_db.backends.base import Backend

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pymysql.connections import Connection


class MySQLBackend(Backend):
    """MySQL via native PyMySQL driver.

    Per-thread connection caching (ADR-0019) gives each thread its own
    pymysql connection so concurrent ``collect()`` calls cannot
    cross-contaminate cursor state.
    """

    def __init__(self) -> None:
        self._state = PerThreadConnections()

    @property
    def dialect(self) -> str:
        return "mysql"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        # Per-statement commit normally preserves the historical
        # autocommit-like semantics. Inside a ``transaction()`` block
        # the outer context commits/rolls back as a whole, so skip the
        # per-statement commit to keep the snapshot intact.
        if not self._state.in_tx:
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

    @contextmanager
    def transaction(self, conn_str: str) -> Iterator[None]:
        """Open a REPEATABLE READ MySQL transaction on the cached connection.

        InnoDB's default isolation level is already REPEATABLE READ, but
        we set it explicitly so the contract is dialect-portable and not
        a function of the server's ``tx_isolation`` global. See ADR-0017
        for the TOCTOU rationale.
        """
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        try:
            # ``SET TRANSACTION`` applies to the *next* tx when issued
            # outside one; ``START TRANSACTION`` then opens that tx.
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION")
        finally:
            cursor.close()
        self._state.in_tx = True
        try:
            yield
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            self._state.in_tx = False

    def _get_connection(self, conn_str: str) -> Connection:
        return self._state.get_or_create(conn_str, self._create_connection)

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

    def current_schema_sql_expr(self) -> exp.Expression:
        """MySQL uses ``DATABASE()`` to return the current database name."""
        return exp.Anonymous(this="DATABASE")

    def close(self) -> None:
        self._state.close_all()
