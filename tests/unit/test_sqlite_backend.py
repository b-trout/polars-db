"""Unit tests for the ADBC-backed SQLite backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from polars_db.backends.sqlite import SQLiteBackend, _extract_sqlite_path
from polars_db.exceptions import BackendNotSupportedError


@pytest.mark.unit
def test_sqlite_dialect() -> None:
    assert SQLiteBackend().dialect == "sqlite"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("conn_str", "expected"),
    [
        ("sqlite:///:memory:", ":memory:"),
        ("sqlite://", ":memory:"),
        ("sqlite:///", ":memory:"),
        ("sqlite:///path/to/file.db", "path/to/file.db"),
        ("sqlite:////abs/path.db", "/abs/path.db"),
        (":memory:", ":memory:"),
        ("", ":memory:"),
    ],
)
def test_extract_sqlite_path(conn_str: str, expected: str) -> None:
    assert _extract_sqlite_path(conn_str) == expected


@pytest.mark.unit
def test_schema_query_uses_pragma_table_info() -> None:
    sql = SQLiteBackend().schema_query("users")
    assert "pragma_table_info('users')" in sql


@pytest.mark.unit
def test_build_explain_sql_default() -> None:
    assert (
        SQLiteBackend().build_explain_sql("SELECT 1") == "EXPLAIN QUERY PLAN SELECT 1"
    )


@pytest.mark.unit
def test_build_explain_sql_analyze_raises() -> None:
    with pytest.raises(BackendNotSupportedError):
        SQLiteBackend().build_explain_sql("SELECT 1", analyze=True)


@pytest.mark.unit
def test_execute_sql_returns_fetch_arrow_table_result() -> None:
    """``execute_sql`` must delegate to ``cursor.fetch_arrow_table``."""
    expected = pa.table({"id": [1, 2]})
    cursor = MagicMock()
    cursor.fetch_arrow_table.return_value = expected
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = SQLiteBackend()
    with patch.object(
        SQLiteBackend, "_create_connection", staticmethod(lambda _cs: conn)
    ):
        result = backend.execute_sql("SELECT 1", "sqlite:///:memory:")

    assert result is expected
    cursor.execute.assert_called_once_with("SELECT 1")
    cursor.close.assert_called_once()


@pytest.mark.unit
def test_execute_sql_closes_cursor_on_failure() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("boom")
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = SQLiteBackend()
    with (
        patch.object(
            SQLiteBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        backend.execute_sql("SELECT 1", "sqlite:///:memory:")

    cursor.close.assert_called_once()


@pytest.mark.unit
def test_connection_is_cached_and_reopened() -> None:
    cursor = MagicMock()
    cursor.fetch_arrow_table.return_value = pa.table({})
    conn_a = MagicMock()
    conn_b = MagicMock()
    conn_a.cursor.return_value = cursor
    conn_b.cursor.return_value = cursor
    conns = iter([conn_a, conn_b])

    backend = SQLiteBackend()
    with patch.object(
        SQLiteBackend, "_create_connection", staticmethod(lambda _cs: next(conns))
    ):
        backend.execute_sql("SELECT 1", "sqlite:///:memory:")
        backend.execute_sql("SELECT 2", "sqlite:///:memory:")  # cached
        backend.execute_sql("SELECT 3", "sqlite:///other.db")  # reopens

    conn_a.close.assert_called_once()
    conn_b.close.assert_not_called()


@pytest.mark.unit
def test_close_releases_cached_connection() -> None:
    conn = MagicMock()
    conn.cursor.return_value.fetch_arrow_table.return_value = pa.table({})

    backend = SQLiteBackend()
    with patch.object(
        SQLiteBackend, "_create_connection", staticmethod(lambda _cs: conn)
    ):
        backend.execute_sql("SELECT 1", "sqlite:///:memory:")

    backend.close()
    conn.close.assert_called_once()
    assert backend._conn is None
    assert backend._conn_str is None


@pytest.mark.unit
def test_create_connection_extracts_path_for_adbc() -> None:
    """``_create_connection`` must strip the ``sqlite://`` scheme before
    handing the path to :func:`adbc_driver_sqlite.dbapi.connect`."""
    fake_mod = MagicMock()
    fake_mod.connect.return_value = "conn-sentinel"

    with patch.dict("sys.modules", {"adbc_driver_sqlite.dbapi": fake_mod}):
        result = SQLiteBackend._create_connection("sqlite:///:memory:")

    assert result == "conn-sentinel"
    fake_mod.connect.assert_called_once_with(":memory:")


@pytest.mark.unit
def test_create_connection_passes_absolute_path() -> None:
    fake_mod = MagicMock()
    fake_mod.connect.return_value = "conn-sentinel"

    with patch.dict("sys.modules", {"adbc_driver_sqlite.dbapi": fake_mod}):
        SQLiteBackend._create_connection("sqlite:////var/data/app.db")

    fake_mod.connect.assert_called_once_with("/var/data/app.db")


@pytest.mark.unit
def test_execute_sql_end_to_end_in_memory() -> None:
    """End-to-end exercise against a real ADBC in-memory SQLite.

    ``adbc-driver-sqlite`` is installed as part of the ``sqlite`` extra,
    which is picked up by ``uv sync --all-extras`` in CI, so this test
    always has the driver available in the test environment.
    """
    pytest.importorskip("adbc_driver_sqlite.dbapi")
    backend = SQLiteBackend()
    try:
        backend.execute_sql(
            "CREATE TABLE foo (id INTEGER, name TEXT)", "sqlite:///:memory:"
        )
        backend.execute_sql(
            "INSERT INTO foo VALUES (1, 'a'), (2, 'b')", "sqlite:///:memory:"
        )
        result = backend.execute_sql("SELECT * FROM foo", "sqlite:///:memory:")
        assert result.num_columns == 2
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 2
    finally:
        backend.close()
