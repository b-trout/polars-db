"""BigQuery backend."""

from __future__ import annotations

from polars_db.backends.base import ConnectorxBackend
from polars_db.exceptions import BackendNotSupportedError


class BigQueryBackend(ConnectorxBackend):
    """BigQuery via connectorx or google-cloud-bigquery."""

    @property
    def dialect(self) -> str:
        return "bigquery"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        msg = (
            "BigQuery does not support EXPLAIN. "
            "Query plans are only available after execution via the Jobs API."
        )
        raise BackendNotSupportedError(msg)
