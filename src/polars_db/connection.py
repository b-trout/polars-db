"""Database connection management."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlparse, urlunparse

if TYPE_CHECKING:
    import polars as pl

    from polars_db.backends.base import Backend
    from polars_db.lazy_frame import LazyFrame


class Connection:
    """Manage a database connection and provide table references."""

    def __init__(self, conn_str: str, backend: Backend | None = None) -> None:
        self._conn_str = conn_str
        self.backend = backend or detect_backend(conn_str)
        self._schema_cache: dict[str, list[str]] = {}

    def __repr__(self) -> str:
        return f"Connection({self._masked_conn_str()!r})"

    def _masked_conn_str(self) -> str:
        """Mask password in the connection string."""
        parsed = urlparse(self._conn_str)
        if parsed.password:
            masked = parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            )
            return urlunparse(masked)
        return self._conn_str

    def table(self, name: str, schema: str | None = None) -> LazyFrame:
        """Return a lazy reference to a database table."""
        from polars_db.lazy_frame import LazyFrame as _LazyFrame
        from polars_db.ops.table import TableRef

        return _LazyFrame(op=TableRef(name=name, schema=schema), connection=self)

    def execute(self, sql: str) -> pl.DataFrame:
        """Execute SQL and return a ``polars.DataFrame``."""
        import polars

        arrow_table = self.backend.execute_sql(sql, self._conn_str)
        if arrow_table.num_columns == 0:
            return polars.DataFrame()
        result = polars.from_arrow(arrow_table)
        if isinstance(result, polars.Series):
            return result.to_frame()
        return result

    def execute_raw(self, sql: str) -> pl.DataFrame:
        """Execute raw SQL directly.

        Escape hatch for queries that cannot be expressed via the Expr API.

        .. warning::
            This method executes SQL as-is.  Do not pass unsanitised
            external input via string concatenation.
        """
        return self.execute(sql)

    # -- schema cache --------------------------------------------------------

    def get_schema(self, table: str) -> list[str]:
        """Return column names for *table* (cached)."""
        if table not in self._schema_cache:
            self._schema_cache[table] = self._fetch_schema(table)
        return self._schema_cache[table]

    def _fetch_schema(self, table: str) -> list[str]:
        """Query ``INFORMATION_SCHEMA`` for column names."""
        sql = self.backend.schema_query(table)
        result = self.execute(sql)
        # Use positional access: some backends return COLUMN_NAME (uppercase)
        return result.to_series(0).to_list()

    def refresh_schema(self, table: str | None = None) -> None:
        """Invalidate schema cache."""
        if table:
            self._schema_cache.pop(table, None)
        else:
            self._schema_cache.clear()

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the connection and release resources."""
        close_fn = getattr(self.backend, "close", None)
        if callable(close_fn):
            close_fn()
        self._schema_cache.clear()

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def connect(conn_str: str, **kwargs: object) -> Connection:
    """Create a database connection."""
    return Connection(conn_str, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------


def detect_backend(conn_str: str) -> Backend:
    """Auto-detect the backend from a connection string."""
    from polars_db.backends.bigquery import BigQueryBackend
    from polars_db.backends.duckdb import DuckDBBackend
    from polars_db.backends.mysql import MySQLBackend
    from polars_db.backends.postgres import PostgresBackend
    from polars_db.backends.sqlite import SQLiteBackend
    from polars_db.backends.sqlserver import SQLServerBackend

    if conn_str.startswith(("postgresql://", "postgres://")):
        return PostgresBackend()
    if "duckdb" in conn_str:
        return DuckDBBackend()
    if conn_str.startswith("mysql://"):
        return MySQLBackend()
    if conn_str.startswith("sqlite://"):
        return SQLiteBackend()
    if conn_str.startswith("mssql://"):
        return SQLServerBackend()
    if "bigquery" in conn_str or conn_str.startswith("bigquery://"):
        return BigQueryBackend()

    msg = f"Unsupported connection string: {conn_str}"
    raise ValueError(msg)
