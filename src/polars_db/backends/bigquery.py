"""BigQuery backend using google-cloud-bigquery client."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import sqlglot.expressions as exp

from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError

if TYPE_CHECKING:
    from google.cloud.bigquery import Client


class BigQueryBackend(Backend):
    """BigQuery via google-cloud-bigquery client."""

    def __init__(self) -> None:
        self._client: Client | None = None
        self._conn_str: str | None = None

    @property
    def dialect(self) -> str:
        return "bigquery"

    _DML_PREFIXES = ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER")

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        client = self._get_client(conn_str)
        job = client.query(sql)
        # The BigQuery emulator hangs on job.result() for DML/DDL
        # statements even though job.state is already DONE.
        # Skip result fetching for non-SELECT statements.
        stripped = sql.strip().upper()
        if any(stripped.startswith(p) for p in self._DML_PREFIXES):
            return pa.table({})
        result = job.result(timeout=30)
        if result.total_rows == 0 and not result.schema:
            return pa.table({})
        return result.to_arrow()

    def _get_client(self, conn_str: str) -> Client:
        if self._client is None or self._conn_str != conn_str:
            self.close()
            self._client = self._create_client(conn_str)
            self._conn_str = conn_str
        return self._client

    @staticmethod
    def _create_client(conn_str: str) -> Client:
        from google.cloud import bigquery

        parsed = urlparse(conn_str)
        project = parsed.hostname or ""

        emulator_host = os.environ.get("BIGQUERY_EMULATOR_HOST")
        if emulator_host:
            from google.api_core.client_options import ClientOptions
            from google.auth.credentials import AnonymousCredentials

            return bigquery.Client(
                project=project,
                credentials=AnonymousCredentials(),
                client_options=ClientOptions(api_endpoint=f"http://{emulator_host}"),
            )

        return bigquery.Client(project=project)

    def schema_query(self, table: str) -> str:
        # BigQuery requires ``INFORMATION_SCHEMA`` to be referenced in
        # uppercase, so we cannot fall back to the base-class implementation
        # (which emits lowercase identifiers). Build the statement with the
        # sqlglot AST so that ``table`` is emitted as a properly escaped
        # string literal instead of being interpolated via f-string.
        return (
            exp.Select(expressions=[exp.Column(this=exp.to_identifier("column_name"))])
            .from_(
                exp.Table(
                    db=exp.to_identifier("INFORMATION_SCHEMA"),
                    this=exp.to_identifier("COLUMNS"),
                )
            )
            .where(
                exp.EQ(
                    this=exp.Column(this=exp.to_identifier("table_name")),
                    expression=exp.Literal.string(table),
                )
            )
            .sql(dialect=self.dialect)
        )

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        msg = (
            "BigQuery does not support EXPLAIN. "
            "Query plans are only available after execution via the Jobs API."
        )
        raise BackendNotSupportedError(msg)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._conn_str = None
