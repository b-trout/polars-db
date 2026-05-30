"""PostgreSQL backend using the ADBC driver for native Arrow transport."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from polars_db.backends.base import Backend

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa
    from adbc_driver_manager.dbapi import Connection as ADBCConnection


class PostgresBackend(Backend):
    """PostgreSQL via ADBC (Arrow Database Connectivity).

    Result rows are fetched directly as a :class:`pyarrow.Table` via
    :meth:`cursor.fetch_arrow_table`, eliminating the per-row Python copy
    that the previous psycopg2-based implementation required.  Column
    types come from the driver's Arrow schema, so NULL-only columns no
    longer collapse to ``null``.

    .. warning::
        A single backend instance caches one connection in
        ``self._conn``/``self._conn_str``. The cache is not thread-safe —
        concurrent ``collect()`` calls from multiple threads on the same
        :class:`~polars_db.connection.Connection` can race on the cached
        cursor and cross-contaminate result sets. Use one connection per
        thread, or serialize access externally.
    """

    def __init__(self) -> None:
        self._conn: ADBCConnection | None = None
        self._conn_str: str | None = None
        self._in_tx: bool = False

    @property
    def dialect(self) -> str:
        return "postgres"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            # ADBC returns an empty table with zero columns for DDL/DML
            # (no result set), which matches the ``pa.table({})`` contract
            # the previous implementation produced.
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    @contextmanager
    def transaction(self, conn_str: str) -> Iterator[None]:
        """Open a REPEATABLE READ transaction on the cached connection.

        The default ADBC connection is ``autocommit=True`` so each
        ``execute_sql`` is its own transaction. Inside this block we
        flip autocommit off via the low-level ADBC handle (the dbapi
        ``Connection`` wrapper does not re-expose ``set_autocommit``),
        then set the isolation level as the first statement of the
        implicit transaction so JoinValidator's probe and the main
        query observe the same snapshot. Closes the TOCTOU gap
        documented in ADR-0017.
        """
        conn = self._get_connection(conn_str)
        conn.adbc_connection.set_autocommit(False)
        cursor = conn.cursor()
        try:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        finally:
            cursor.close()
        self._in_tx = True
        try:
            yield
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            self._in_tx = False
            conn.adbc_connection.set_autocommit(True)

    def _get_connection(self, conn_str: str) -> ADBCConnection:
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> ADBCConnection:
        import adbc_driver_postgresql.dbapi as adbc_pg

        # ``autocommit=True`` preserves the previous psycopg2 semantics in
        # which every ``execute_sql`` call is its own transaction.  DDL
        # and DML are committed immediately, matching the behaviour callers
        # relied on before the ADBC migration.
        return adbc_pg.connect(conn_str, autocommit=True)

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None
            self._in_tx = False
