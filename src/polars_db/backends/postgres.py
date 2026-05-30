"""PostgreSQL backend using the ADBC driver for native Arrow transport."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from polars_db.backends._thread_local import PerThreadConnections
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

    .. note::
        The cached connection runs in DBAPI default ``autocommit=False``
        mode and we explicitly call ``conn.commit()`` after every
        ``execute_sql`` so callers see the same one-statement-one-commit
        semantics psycopg2 used to provide. Inside a :meth:`transaction`
        block the per-statement commit is suppressed; the outer
        ``commit()`` / ``rollback()`` defines the snapshot. We avoid the
        ``set_autocommit`` toggle because ADBC's behaviour around
        toggling mid-session is driver-dependent (SQLite for instance
        ignores it for rollback purposes).

    Per-thread connection caching (ADR-0019) makes concurrent
    ``collect()`` from multiple threads safe — each thread gets its own
    driver connection and its own ``in_tx`` flag, so there is no
    cross-thread cursor contention. The original psycopg2-era warning
    about thread safety has been retired.
    """

    def __init__(self) -> None:
        self._state = PerThreadConnections()

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
            result = cursor.fetch_arrow_table()
        finally:
            cursor.close()
        # Per-statement commit preserves the historical psycopg2
        # autocommit-True semantics. Inside a ``transaction()`` block the
        # outer context commits/rolls back as a whole, so skip here to
        # keep the snapshot intact (ADR-0017).
        if not self._state.in_tx:
            conn.commit()
        return result

    @contextmanager
    def transaction(self, conn_str: str) -> Iterator[None]:
        """Open a REPEATABLE READ transaction on the cached connection.

        Issues ``SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`` as the
        first statement of the next implicit transaction; subsequent
        ``execute_sql`` calls then share that snapshot until block exit
        commits or rolls back. Closes the TOCTOU gap documented in
        ADR-0017.
        """
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        try:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
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

    def _get_connection(self, conn_str: str) -> ADBCConnection:
        return self._state.get_or_create(conn_str, self._create_connection)

    @staticmethod
    def _create_connection(conn_str: str) -> ADBCConnection:
        import adbc_driver_postgresql.dbapi as adbc_pg

        # Open in DBAPI-default ``autocommit=False``; ``execute_sql``
        # commits after every statement when not in a tx, and
        # ``transaction()`` suppresses that commit so the whole block
        # runs as one snapshot.
        return adbc_pg.connect(conn_str)

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def close(self) -> None:
        self._state.close_all()
