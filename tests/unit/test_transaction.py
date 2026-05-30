"""Tests for the Backend.transaction() contract and LazyFrame.collect wrapping.

The transaction context manager is the mechanism through which
JoinValidator's uniqueness check and the main query observe the same
database snapshot (ADR-0017). These tests pin down:

* Each backend's ``supports_atomic_validation`` flag.
* The BEGIN -> body -> COMMIT call order on success.
* The BEGIN -> body -> ROLLBACK call order on exception.
* LazyFrame.collect wraps validating joins in ``transaction()`` and
  skips the wrapper otherwise.
* Backends without atomic support (BigQuery) skip validation with a
  warning rather than running it sequentially.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import polars_db as pdb
from polars_db.backends.bigquery import BigQueryBackend
from polars_db.backends.duckdb import DuckDBBackend
from polars_db.backends.mysql import MySQLBackend
from polars_db.backends.postgres import PostgresBackend
from polars_db.backends.sqlite import SQLiteBackend
from polars_db.backends.sqlserver import SQLServerBackend
from polars_db.connection import Connection

# ---------------------------------------------------------------------------
# supports_atomic_validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("backend_cls", "expected"),
    [
        (PostgresBackend, True),
        (SQLiteBackend, True),
        (DuckDBBackend, True),
        (MySQLBackend, True),
        (SQLServerBackend, True),
        (BigQueryBackend, False),
    ],
)
def test_supports_atomic_validation(backend_cls: type, expected: bool) -> None:
    """Each backend declares whether its tx provides snapshot atomicity."""
    assert backend_cls().supports_atomic_validation is expected


# ---------------------------------------------------------------------------
# Postgres / SQLite (ADBC): set_autocommit + BEGIN + commit/rollback
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_postgres_transaction_sets_repeatable_read_via_adbc_handle() -> None:
    """Postgres tx flips autocommit on the low-level ADBC handle (the dbapi
    Connection wrapper does not re-expose ``set_autocommit``) and sets
    REPEATABLE READ as the first statement of the implicit tx."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = PostgresBackend()
    with (
        patch.object(
            PostgresBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        backend.transaction("postgresql://x"),
    ):
        assert backend._in_tx is True
    assert backend._in_tx is False

    # autocommit toggled off then restored on the low-level handle
    conn.adbc_connection.set_autocommit.assert_any_call(False)
    conn.adbc_connection.set_autocommit.assert_any_call(True)
    cursor.execute.assert_called_with("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


@pytest.mark.unit
def test_postgres_transaction_rolls_back_on_exception() -> None:
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = PostgresBackend()
    with (
        patch.object(
            PostgresBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        pytest.raises(RuntimeError, match="boom"),
        backend.transaction("postgresql://x"),
    ):
        raise RuntimeError("boom")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    # autocommit must still be restored on the low-level handle
    conn.adbc_connection.set_autocommit.assert_any_call(True)
    assert backend._in_tx is False


@pytest.mark.unit
def test_sqlite_transaction_toggles_autocommit_on_adbc_handle() -> None:
    """SQLite tx flips autocommit off via the low-level ADBC handle; the
    driver opens the implicit tx on the first query (no explicit BEGIN
    needed in autocommit-off mode)."""
    conn = MagicMock()

    backend = SQLiteBackend()
    with (
        patch.object(
            SQLiteBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        backend.transaction("sqlite:///:memory:"),
    ):
        pass

    conn.adbc_connection.set_autocommit.assert_any_call(False)
    conn.adbc_connection.set_autocommit.assert_any_call(True)
    conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# MySQL: SET TRANSACTION + START TRANSACTION, _in_tx suppresses per-stmt commit
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_mysql_transaction_sets_isolation_then_starts() -> None:
    cursor = MagicMock()
    cursor.description = None
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = MySQLBackend()
    with (
        patch.object(
            MySQLBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        backend.transaction("mysql://x"),
    ):
        pass

    issued = [c.args[0] for c in cursor.execute.call_args_list]
    assert issued == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "START TRANSACTION",
    ]
    conn.commit.assert_called_once()


@pytest.mark.unit
def test_mysql_execute_sql_skips_commit_inside_transaction() -> None:
    """During a tx, execute_sql must not call conn.commit() — only the
    outer ``transaction()`` may commit, otherwise the snapshot ends."""
    cursor = MagicMock()
    cursor.description = None
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = MySQLBackend()
    with patch.object(
        MySQLBackend, "_create_connection", staticmethod(lambda _cs: conn)
    ):
        with backend.transaction("mysql://x"):
            conn.commit.reset_mock()
            backend.execute_sql("SELECT 1", "mysql://x")
            assert conn.commit.call_count == 0
        # On block exit, the outer tx commits once.
        assert conn.commit.call_count == 1


# ---------------------------------------------------------------------------
# SQL Server: SET ISOLATION SERIALIZABLE + BEGIN TRANSACTION
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sqlserver_transaction_sets_serializable_then_begins() -> None:
    cursor = MagicMock()
    cursor.description = None
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = SQLServerBackend()
    with (
        patch.object(SQLServerBackend, "_create_connection", lambda _self, _cs: conn),
        backend.transaction("mssql://x"),
    ):
        pass

    issued = [c.args[0] for c in cursor.execute.call_args_list]
    assert issued == [
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "BEGIN TRANSACTION",
    ]
    conn.commit.assert_called_once()


@pytest.mark.unit
def test_sqlserver_execute_sql_skips_commit_inside_transaction() -> None:
    cursor = MagicMock()
    cursor.description = None
    conn = MagicMock()
    conn.cursor.return_value = cursor

    backend = SQLServerBackend()
    with (
        patch.object(SQLServerBackend, "_create_connection", lambda _self, _cs: conn),
        backend.transaction("mssql://x"),
    ):
        conn.commit.reset_mock()
        backend.execute_sql("SELECT 1", "mssql://x")
        assert conn.commit.call_count == 0


# ---------------------------------------------------------------------------
# DuckDB: BEGIN TRANSACTION / COMMIT via conn.execute
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_duckdb_transaction_uses_sql_begin_commit() -> None:
    conn = MagicMock()
    # ``conn.execute`` is called both for BEGIN/COMMIT and for queries.
    # Track only the strings, not the result objects.

    backend = DuckDBBackend()
    with (
        patch.object(
            DuckDBBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        backend.transaction("duckdb:///:memory:"),
    ):
        pass

    issued = [c.args[0] for c in conn.execute.call_args_list]
    assert issued == ["BEGIN TRANSACTION", "COMMIT"]


@pytest.mark.unit
def test_duckdb_transaction_rolls_back_on_exception() -> None:
    conn = MagicMock()

    backend = DuckDBBackend()
    with (
        patch.object(
            DuckDBBackend, "_create_connection", staticmethod(lambda _cs: conn)
        ),
        pytest.raises(RuntimeError, match="boom"),
        backend.transaction("duckdb:///:memory:"),
    ):
        raise RuntimeError("boom")

    issued = [c.args[0] for c in conn.execute.call_args_list]
    assert issued == ["BEGIN TRANSACTION", "ROLLBACK"]


# ---------------------------------------------------------------------------
# BigQuery: transaction() is a no-op; collect skips validation with warning
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bigquery_transaction_is_noop() -> None:
    """BigQuery's transaction() must be a no-op (no client interaction)."""
    backend = BigQueryBackend()
    # Should not require a client; never call _get_client.
    with backend.transaction("bigquery://proj/ds"):
        pass


# ---------------------------------------------------------------------------
# LazyFrame.collect wrapping behaviour
# ---------------------------------------------------------------------------


def _make_connection(backend: object) -> Connection:
    """Build a Connection backed by a fake backend for collect() tests."""
    conn = Connection.__new__(Connection)
    conn._conn_str = "fake://x"
    conn.backend = backend  # type: ignore[assignment]
    conn._schema_cache = {"users": ["id", "user_id"], "orders": ["id", "user_id"]}
    return conn


class _FakeBackend:
    """Minimal backend stub usable by Connection/LazyFrame in unit tests."""

    dialect = "postgres"
    supports_atomic_validation = True

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.tx_calls: list[str] = []

    def execute_sql(self, sql: str, _conn_str: str) -> pa.Table:
        self.executed.append(sql)
        return pa.table({})

    def render(self, ast: object) -> str:
        return ast.sql(dialect=self.dialect)  # type: ignore[attr-defined]

    def function_mapping(self) -> dict[str, str]:
        return {}

    def transaction(self, _conn_str: str):  # type: ignore[no-untyped-def]
        from contextlib import contextmanager

        @contextmanager
        def _cm():  # type: ignore[no-untyped-def]
            self.tx_calls.append("enter")
            try:
                yield
                self.tx_calls.append("commit")
            except BaseException:
                self.tx_calls.append("rollback")
                raise

        return _cm()


@pytest.mark.unit
def test_collect_skips_transaction_when_no_validating_join() -> None:
    """No JOIN -> no tx wrapping; default ``m:m`` JOIN -> no tx wrapping."""
    backend = _FakeBackend()
    conn = _make_connection(backend)

    # No join at all
    conn.table("users").select("id").collect()
    assert backend.tx_calls == []

    # JOIN with default validate='m:m' must also skip the wrapper
    backend.tx_calls.clear()
    backend.executed.clear()
    conn.table("users").join(conn.table("orders"), on="user_id", how="inner").collect()
    assert backend.tx_calls == []


@pytest.mark.unit
def test_collect_wraps_validating_join_in_transaction() -> None:
    """validate != 'm:m' opens a tx and runs both validation + main query
    inside it."""
    backend = _FakeBackend()
    conn = _make_connection(backend)

    conn.table("users").join(
        conn.table("orders"), on="user_id", how="left", validate="1:m"
    ).collect()

    assert backend.tx_calls == ["enter", "commit"]
    # At least one validation SQL plus the main query.
    assert len(backend.executed) >= 2


@pytest.mark.unit
def test_collect_skips_validation_with_warning_on_non_atomic_backend() -> None:
    """Backends without atomic validation must emit a warning and skip
    the validation rather than run it sequentially (which would give a
    falsely-confident result)."""
    backend = _FakeBackend()
    backend.supports_atomic_validation = False  # type: ignore[assignment]
    conn = _make_connection(backend)

    lf = conn.table("users").join(
        conn.table("orders"), on="user_id", how="left", validate="1:m"
    )
    with pytest.warns(UserWarning, match="does not support atomic"):
        lf.collect()

    # No validation queries should have run: only the main query.
    assert len(backend.executed) == 1
    # Transaction was still entered (no-op for BigQuery) so the wrapper
    # works regardless of atomic support — the skip is decided inside.
    assert backend.tx_calls == ["enter", "commit"]


@pytest.mark.unit
def test_connection_transaction_delegates_to_backend() -> None:
    """Connection.transaction() must forward to backend.transaction()."""
    backend = _FakeBackend()
    conn = _make_connection(backend)

    with conn.transaction():
        pass

    assert backend.tx_calls == ["enter", "commit"]


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_public_api_exposes_transaction_on_connection() -> None:
    """``pdb.Connection`` must expose ``transaction()`` for raw-SQL users
    who want atomic ``execute_raw`` sequences."""
    assert hasattr(pdb.Connection, "transaction")
