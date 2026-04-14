"""MySQL backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend


class MySQLBackend(ConnectorxBackend):
    """MySQL via connectorx."""

    @property
    def dialect(self) -> str:
        return "mysql"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "GROUP_CONCAT"}
