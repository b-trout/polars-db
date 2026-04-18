"""SQLite backend using the ADBC driver for native Arrow transport."""

from __future__ import annotations

from typing import TYPE_CHECKING

from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError

if TYPE_CHECKING:
    import pyarrow as pa
    from adbc_driver_manager.dbapi import Connection as ADBCConnection


class SQLiteBackend(Backend):
    """SQLite via ADBC (Arrow Database Connectivity).

    Result rows are fetched directly as a :class:`pyarrow.Table` via
    :meth:`cursor.fetch_arrow_table`, eliminating the per-row Python copy
    required by the previous stdlib ``sqlite3`` implementation.

    Connection strings follow the ``sqlite:///`` URI form used by the
    rest of the project; the path (or ``:memory:`` literal) is extracted
    before being passed to ADBC, which expects a bare file path.

    .. warning::
        A single backend instance caches one connection.  The cache is
        not thread-safe.
    """

    def __init__(self) -> None:
        self._conn: ADBCConnection | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "sqlite"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            # ADBC returns an empty Arrow table (zero columns) for DDL/DML
            # statements that do not produce a result set, matching the
            # ``pa.table({})`` contract of the previous implementation.
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    def _get_connection(self, conn_str: str) -> ADBCConnection:
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @staticmethod
    def _create_connection(conn_str: str) -> ADBCConnection:
        import adbc_driver_sqlite.dbapi as adbc_sqlite

        path = _extract_sqlite_path(conn_str)
        return adbc_sqlite.connect(path)

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


def _extract_sqlite_path(conn_str: str) -> str:
    """Extract a bare path from a ``sqlite://`` connection string.

    Recognised forms:

    * ``sqlite:///:memory:``          → ``":memory:"``
    * ``sqlite:///path/to/file.db``   → ``"path/to/file.db"`` (relative)
    * ``sqlite:////abs/path.db``      → ``"/abs/path.db"`` (absolute)
    * ``sqlite://`` / no scheme       → ``":memory:"``

    ADBC SQLite expects a file system path (or the ``":memory:"``
    literal) rather than the SQLAlchemy-style URI used elsewhere in
    the project.
    """
    if not conn_str.startswith("sqlite://"):
        # Fallback for callers that already pass a bare path.
        return conn_str or ":memory:"

    # Strip the scheme; any number of leading slashes (2, 3, or 4)
    # collapses into an optional leading slash for absolute paths.
    body = conn_str[len("sqlite://") :]
    # ``sqlite:///:memory:`` → body == "/:memory:"
    if body.lstrip("/") == ":memory:":
        return ":memory:"
    # Three-slash form (``sqlite:///relative/file.db``) → relative path
    # Four-slash form (``sqlite:////abs/file.db``) → absolute path
    # After stripping exactly one leading slash, the remainder is the
    # path as the user intended.
    if body.startswith("/"):
        body = body[1:]
    return body or ":memory:"
