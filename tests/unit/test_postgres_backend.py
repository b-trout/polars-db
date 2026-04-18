"""Unit tests for the ADBC-backed PostgreSQL backend.

``adbc-driver-postgresql`` is an optional extra, so these tests only
exercise code paths that do not require an actual PostgreSQL server.
Driver interaction is verified via a mocked ``_create_connection``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from polars_db.backends.postgres import PostgresBackend


@pytest.mark.unit
def test_postgres_dialect() -> None:
    assert PostgresBackend().dialect == "postgres"


@pytest.mark.unit
def test_postgres_function_mapping_includes_string_agg() -> None:
    """``string_agg`` must remain mapped so existing rewrites still work."""
    assert PostgresBackend().function_mapping() == {"string_agg": "STRING_AGG"}


@pytest.mark.unit
def test_execute_sql_returns_fetch_arrow_table_result() -> None:
    """``execute_sql`` must delegate to ``cursor.fetch_arrow_table``
    rather than build a dict-of-lists and hand it to ``pa.table``."""
    expected = pa.table({"id": [1, 2], "name": ["a", "b"]})
    cursor = MagicMock()
    cursor.fetch_arrow_table.return_value = expected
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = PostgresBackend()
    with patch.object(
        PostgresBackend, "_create_connection", staticmethod(lambda _cs: conn)
    ):
        result = backend.execute_sql("SELECT 1", "postgresql://x/y")

    assert result is expected
    cursor.execute.assert_called_once_with("SELECT 1")
    cursor.fetch_arrow_table.assert_called_once_with()
    cursor.close.assert_called_once()


@pytest.mark.unit
def test_execute_sql_closes_cursor_on_failure() -> None:
    """Even when ``execute`` raises, the cursor must be closed."""
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("boom")
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = PostgresBackend()
    with (
        patch.object(
            PostgresBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        backend.execute_sql("SELECT 1", "postgresql://x/y")

    cursor.close.assert_called_once()


@pytest.mark.unit
def test_connection_is_cached_across_calls() -> None:
    """Repeated calls with the same conn_str must reuse one connection."""
    cursor = MagicMock()
    cursor.fetch_arrow_table.return_value = pa.table({})
    conn = MagicMock()
    conn.cursor.return_value = cursor
    create = MagicMock(return_value=conn)

    backend = PostgresBackend()
    with patch.object(PostgresBackend, "_create_connection", staticmethod(create)):
        backend.execute_sql("SELECT 1", "postgresql://x/y")
        backend.execute_sql("SELECT 2", "postgresql://x/y")

    assert create.call_count == 1


@pytest.mark.unit
def test_connection_reopens_when_conn_str_changes() -> None:
    """Changing the connection string must close the old connection
    and open a new one."""
    cursor = MagicMock()
    cursor.fetch_arrow_table.return_value = pa.table({})
    conn_a = MagicMock()
    conn_b = MagicMock()
    conn_a.cursor.return_value = cursor
    conn_b.cursor.return_value = cursor
    conns = iter([conn_a, conn_b])

    backend = PostgresBackend()
    with patch.object(
        PostgresBackend, "_create_connection", staticmethod(lambda _cs: next(conns))
    ):
        backend.execute_sql("SELECT 1", "postgresql://a")
        backend.execute_sql("SELECT 2", "postgresql://b")

    conn_a.close.assert_called_once()
    conn_b.close.assert_not_called()


@pytest.mark.unit
def test_close_releases_cached_connection() -> None:
    conn = MagicMock()
    conn.cursor.return_value.fetch_arrow_table.return_value = pa.table({})

    backend = PostgresBackend()
    with patch.object(
        PostgresBackend, "_create_connection", staticmethod(lambda _cs: conn)
    ):
        backend.execute_sql("SELECT 1", "postgresql://x")

    backend.close()
    conn.close.assert_called_once()
    assert backend._conn is None
    assert backend._conn_str is None


@pytest.mark.unit
def test_create_connection_calls_adbc_with_autocommit() -> None:
    """``_create_connection`` must delegate to
    ``adbc_driver_postgresql.dbapi.connect`` with ``autocommit=True`` so
    that DDL/DML callers see the same "one statement = one transaction"
    semantics they had under psycopg2."""
    fake_mod = MagicMock()
    fake_mod.connect.return_value = "conn-sentinel"

    # ``import adbc_driver_postgresql.dbapi as adbc_pg`` binds the parent
    # module's ``dbapi`` attribute, not sys.modules directly — so we must
    # expose ``fake_mod`` as the parent's ``dbapi`` attribute. Placing it
    # in both sys.modules entries keeps ``importlib`` consistent when the
    # real ``postgres`` extra is not installed (CI unit-test job).
    parent_mod = MagicMock()
    parent_mod.dbapi = fake_mod

    with patch.dict(
        "sys.modules",
        {
            "adbc_driver_postgresql": parent_mod,
            "adbc_driver_postgresql.dbapi": fake_mod,
        },
    ):
        result = PostgresBackend._create_connection("postgresql://u:p@host/db")

    assert result == "conn-sentinel"
    fake_mod.connect.assert_called_once_with(
        "postgresql://u:p@host/db", autocommit=True
    )
