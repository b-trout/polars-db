"""Unit tests for :meth:`Backend.schema_query` across backends.

``schema_query`` is used by :class:`polars_db.connection.Connection` to
fetch column names when resolving JOIN projections.  Historically it
filtered ``information_schema.columns`` only by ``table_name``, which
caused columns from identically named system tables (most notably
MySQL's ``mysql.user``) to bleed into the result and corrupt the
projection.

These tests lock in:

* the per-dialect ``table_schema`` filter (``CURRENT_SCHEMA()`` /
  ``DATABASE()`` / ``SCHEMA_NAME()``);
* a deterministic ``ORDER BY ordinal_position`` clause;
* that dialects with a custom ``schema_query`` (SQLite, DuckDB,
  BigQuery) keep their bespoke behaviour.
"""

from __future__ import annotations

import pytest
import sqlglot

from polars_db.backends.bigquery import BigQueryBackend
from polars_db.backends.duckdb import DuckDBBackend
from polars_db.backends.mysql import MySQLBackend
from polars_db.backends.postgres import PostgresBackend
from polars_db.backends.sqlite import SQLiteBackend
from polars_db.backends.sqlserver import SQLServerBackend


@pytest.mark.unit
def test_postgres_schema_query_filters_by_current_schema() -> None:
    sql = PostgresBackend().schema_query("users")
    assert "table_name = 'users'" in sql
    assert "table_schema = CURRENT_SCHEMA()" in sql
    assert "ORDER BY ordinal_position" in sql
    # Must still be valid SQL for the dialect.
    sqlglot.parse_one(sql, dialect="postgres")


@pytest.mark.unit
def test_mysql_schema_query_filters_by_database() -> None:
    sql = MySQLBackend().schema_query("users")
    assert "table_name = 'users'" in sql
    assert "table_schema = DATABASE()" in sql
    assert "ORDER BY ordinal_position" in sql
    sqlglot.parse_one(sql, dialect="mysql")


@pytest.mark.unit
def test_sqlserver_schema_query_filters_by_schema_name() -> None:
    sql = SQLServerBackend().schema_query("users")
    assert "table_name = 'users'" in sql
    assert "table_schema = SCHEMA_NAME()" in sql
    assert "ORDER BY ordinal_position" in sql
    sqlglot.parse_one(sql, dialect="tsql")


@pytest.mark.unit
def test_bigquery_schema_query_preserves_override() -> None:
    """BigQuery has its own override; confirm uppercase INFORMATION_SCHEMA
    is preserved and ordinal_position ordering is present."""
    sql = BigQueryBackend().schema_query("users")
    assert "INFORMATION_SCHEMA.COLUMNS" in sql
    assert "table_name = 'users'" in sql
    assert "ORDER BY ordinal_position" in sql
    sqlglot.parse_one(sql, dialect="bigquery")


@pytest.mark.unit
def test_sqlite_schema_query_uses_pragma() -> None:
    """SQLite does not expose ``information_schema``; ensure the
    existing pragma_table_info override is untouched."""
    sql = SQLiteBackend().schema_query("users")
    assert "pragma_table_info('users')" in sql
    assert "information_schema" not in sql.lower()


@pytest.mark.unit
def test_duckdb_schema_query_unchanged() -> None:
    """DuckDB has its own override using information_schema without a
    schema filter.  The existing behaviour is left untouched by this fix."""
    sql = DuckDBBackend().schema_query("users")
    assert "information_schema.columns" in sql
    assert "table_name = 'users'" in sql


@pytest.mark.unit
def test_schema_query_escapes_table_name_literal() -> None:
    """The injected ``table_name`` must be emitted as a quoted literal,
    mirroring the BigQuery safety contract."""
    backend = MySQLBackend()
    malicious = "users'; DROP TABLE foo; --"
    sql = backend.schema_query(malicious)
    parsed_all = sqlglot.parse(sql, dialect="mysql")
    non_empty = [p for p in parsed_all if p is not None]
    assert len(non_empty) == 1
    drop_nodes = list(non_empty[0].find_all(sqlglot.expressions.Drop))
    assert drop_nodes == []
