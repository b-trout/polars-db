"""Unit tests for the BigQuery backend helpers.

These tests cover :meth:`BigQueryBackend.schema_query`, which must remain
safe against SQL injection even when called with user-supplied table names.
``google-cloud-bigquery`` is an optional extra, so this file only exercises
code paths that do not require the client to be installed.
"""

from __future__ import annotations

import pytest
import sqlglot

from polars_db.backends.bigquery import BigQueryBackend


@pytest.mark.unit
def test_schema_query_imports_without_bigquery_client() -> None:
    """Instantiating the backend and calling ``schema_query`` must not
    require the ``google-cloud-bigquery`` dependency."""
    backend = BigQueryBackend()
    sql = backend.schema_query("orders")
    assert "INFORMATION_SCHEMA.COLUMNS" in sql
    # The statement must be parseable as valid BigQuery SQL.
    sqlglot.parse_one(sql, dialect="bigquery")


@pytest.mark.unit
def test_schema_query_produces_valid_sql_for_normal_name() -> None:
    backend = BigQueryBackend()
    sql = backend.schema_query("orders")
    parsed = sqlglot.parse_one(sql, dialect="bigquery")
    # The literal should appear exactly once, as a quoted string.
    assert "'orders'" in sql
    # The AST should be a SELECT against INFORMATION_SCHEMA.COLUMNS.
    assert parsed.key == "select"


@pytest.mark.unit
def test_schema_query_escapes_quote_based_injection() -> None:
    """A crafted table name must be emitted as a single quoted literal.

    The f-string implementation used to break out of the quotes and turn
    ``DROP TABLE`` into a second statement. After the fix, the payload
    is escaped and remains inside the literal.
    """
    malicious = "users'; DROP TABLE foo; --"
    backend = BigQueryBackend()
    sql = backend.schema_query(malicious)

    # Must still parse as a single BigQuery statement.
    parsed_all = sqlglot.parse(sql, dialect="bigquery")
    non_empty = [p for p in parsed_all if p is not None]
    assert len(non_empty) == 1, f"Expected a single statement, got {len(non_empty)}"

    # The dangerous tokens must not appear as raw SQL keywords — they
    # have to be contained inside the (escaped) string literal.
    parsed = non_empty[0]
    drop_nodes = list(parsed.find_all(sqlglot.expressions.Drop))
    assert drop_nodes == [], "DROP statement leaked outside the string literal"

    # Sanity check: the escaped payload is present as a string literal.
    literals = [lit.this for lit in parsed.find_all(sqlglot.expressions.Literal)]
    assert malicious in literals


@pytest.mark.unit
def test_schema_query_preserves_information_schema_case() -> None:
    """BigQuery requires ``INFORMATION_SCHEMA`` in uppercase."""
    backend = BigQueryBackend()
    sql = backend.schema_query("orders")
    assert "INFORMATION_SCHEMA" in sql
    assert "COLUMNS" in sql
