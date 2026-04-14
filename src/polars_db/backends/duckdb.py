"""DuckDB backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend


class DuckDBBackend(ConnectorxBackend):
    """DuckDB via connectorx."""

    @property
    def dialect(self) -> str:
        return "duckdb"
