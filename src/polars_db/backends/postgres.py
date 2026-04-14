"""PostgreSQL backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend


class PostgresBackend(ConnectorxBackend):
    """PostgreSQL via connectorx."""

    @property
    def dialect(self) -> str:
        return "postgres"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}
