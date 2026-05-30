"""Database connection management."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse, urlunparse

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    import polars as pl

    from polars_db.backends.base import Backend
    from polars_db.lazy_frame import LazyFrame


class Connection:
    """Manage a database connection and provide table references."""

    def __init__(
        self,
        conn_str: str,
        backend: Backend | None = None,
        *,
        create_if_missing: bool = False,
        schema_cache_ttl: float | None = None,
    ) -> None:
        """Open a database connection.

        Parameters
        ----------
        conn_str:
            Database connection string (e.g. ``"postgresql://..."``).
        backend:
            Optional :class:`Backend` override. When ``None`` (default)
            the backend is auto-detected from ``conn_str``.
        create_if_missing:
            SQL Server only. See :func:`connect`.
        schema_cache_ttl:
            How long to cache ``INFORMATION_SCHEMA`` lookups, in seconds.

            * ``None`` (default) — cache forever. Backwards-compatible
              behaviour; suitable when the schema is known to be stable
              for the lifetime of the connection.
            * ``0`` — never cache. Every ``get_schema`` call queries
              the database. Useful while iterating on DDL.
            * a positive number — cache entries expire that many
              seconds after their fetch time. Closes the TOCTOU gap
              where a concurrent ``ALTER TABLE`` between cache fetch
              and query execution would leave the compiled SQL out of
              sync with the actual columns (ADR-0018).
        """
        self._conn_str = conn_str
        self.backend = backend or detect_backend(
            conn_str, create_if_missing=create_if_missing
        )
        self._schema_cache_ttl = schema_cache_ttl
        self._schema_cache: dict[str, tuple[float, list[str]]] = {}

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

    def transaction(self) -> AbstractContextManager[None]:
        """Open a backend transaction so subsequent calls share a snapshot.

        Delegates to :meth:`polars_db.backends.base.Backend.transaction`.
        Used by :meth:`LazyFrame.collect` to make JOIN validation and the
        main query observe the same database state (ADR-0017). Callers
        running multi-step ``execute_raw`` sequences that must be atomic
        can use this context manager directly.
        """
        return self.backend.transaction(self._conn_str)

    # -- schema cache --------------------------------------------------------

    def get_schema(self, table: str) -> list[str]:
        """Return column names for *table*, using the schema cache when fresh.

        Cache freshness is controlled by ``schema_cache_ttl`` passed to
        :class:`Connection` / :func:`connect`. With the default
        ``None`` the cached entry never expires; with ``0`` the cache
        is bypassed entirely; with a positive value entries older than
        that many seconds are re-fetched. See ADR-0018 for the TOCTOU
        rationale.
        """
        if self._schema_cache_ttl == 0:
            return self._fetch_schema(table)

        entry = self._schema_cache.get(table)
        if entry is not None:
            fetched_at, columns = entry
            ttl = self._schema_cache_ttl
            if ttl is None or (time.monotonic() - fetched_at) < ttl:
                return columns

        columns = self._fetch_schema(table)
        self._schema_cache[table] = (time.monotonic(), columns)
        return columns

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


def connect(
    conn_str: str,
    *,
    create_if_missing: bool = False,
    schema_cache_ttl: float | None = None,
    **kwargs: object,
) -> Connection:
    """Create a database connection.

    Parameters
    ----------
    conn_str:
        Database connection string (e.g. ``"postgresql://..."``).
    create_if_missing:
        SQL Server only. When ``True``, connect to ``master`` first and
        issue ``CREATE DATABASE [<name>]`` if the target database does not
        exist. Default ``False`` -- silently off to prevent accidental
        database creation from typos in production connection strings.
    schema_cache_ttl:
        How long (seconds) to cache ``INFORMATION_SCHEMA`` lookups.
        ``None`` (default) caches forever; ``0`` disables caching;
        a positive value sets a per-entry TTL. See
        :class:`Connection` for details and ADR-0018 for the rationale.
    """
    return Connection(  # type: ignore[arg-type]
        conn_str,
        create_if_missing=create_if_missing,
        schema_cache_ttl=schema_cache_ttl,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------


def detect_backend(conn_str: str, *, create_if_missing: bool = False) -> Backend:
    """Auto-detect the backend from a connection string.

    The ``create_if_missing`` flag is only meaningful for the SQL Server
    backend; it is silently ignored for other backends.
    """
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
        return SQLServerBackend(create_if_missing=create_if_missing)
    if "bigquery" in conn_str or conn_str.startswith("bigquery://"):
        return BigQueryBackend()

    msg = f"Unsupported connection string: {conn_str}"
    raise ValueError(msg)
