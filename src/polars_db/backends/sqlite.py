"""SQLite backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend
from polars_db.exceptions import BackendNotSupportedError


class SQLiteBackend(ConnectorxBackend):
    """SQLite via connectorx."""

    @property
    def dialect(self) -> str:
        return "sqlite"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            msg = "SQLite does not support EXPLAIN ANALYZE."
            raise BackendNotSupportedError(msg)
        return f"EXPLAIN QUERY PLAN {sql}"
