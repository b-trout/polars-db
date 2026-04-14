"""SQL Server backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend
from polars_db.exceptions import BackendNotSupportedError


class SQLServerBackend(ConnectorxBackend):
    """SQL Server via connectorx."""

    @property
    def dialect(self) -> str:
        return "tsql"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        msg = (
            "SQL Server does not support EXPLAIN. "
            "Use SET SHOWPLAN_XML ON via execute_raw() as a workaround."
        )
        raise BackendNotSupportedError(msg)
