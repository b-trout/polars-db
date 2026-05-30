"""Tests for the Backend.transaction() contract and LazyFrame.collect wrapping.

The transaction context manager is the mechanism through which
JoinValidator's uniqueness check and the main query observe the same
database snapshot (ADR-0017). These tests pin down what is testable
without a database server:

* Each backend's ``supports_atomic_validation`` flag.
* ``LazyFrame.collect`` opens a transaction only when there is a JOIN
  with ``validate != "m:m"``.
* Backends with ``supports_atomic_validation == False`` (BigQuery)
  skip the validation with a warning rather than running it
  sequentially.
* The in-process ADBC drivers (SQLite, DuckDB) execute real BEGIN /
  COMMIT / ROLLBACK round-trips so the autocommit-toggle + isolation
  contract is exercised against the actual driver API, not a mock.

Per-backend driver-API behaviour for the server-backed backends
(Postgres, MySQL, SQL Server, BigQuery) lives in
``tests/integration/test_transaction.py`` so the per-backend CI
matrix exercises real driver connections.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import polars_db as pdb
from polars_db.backends.base import Backend
from polars_db.backends.bigquery import BigQueryBackend
from polars_db.backends.duckdb import DuckDBBackend
from polars_db.backends.mysql import MySQLBackend
from polars_db.backends.postgres import PostgresBackend
from polars_db.backends.sqlite import SQLiteBackend
from polars_db.backends.sqlserver import SQLServerBackend
from polars_db.connection import Connection
from polars_db.exceptions import JoinValidationError

if TYPE_CHECKING:
    from collections.abc import Iterator

    import sqlglot.expressions as exp


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
# RecordingBackend: a real Backend subclass used to exercise LazyFrame.collect
# wrapping decisions without touching any driver. Not a mock — the methods
# implement the Backend protocol and record what they were asked to do so
# tests can assert against actual call sequences.
# ---------------------------------------------------------------------------


class RecordingBackend(Backend):
    """Minimal in-memory backend that records calls without touching a DB.

    Implements ``execute_sql`` / ``transaction`` for real (no driver
    library involved). Used to verify ``LazyFrame.collect`` wrapping
    behaviour: when does it open a tx, when does it skip, and how does
    it react to ``supports_atomic_validation == False``.
    """

    dialect = "postgres"  # any sqlglot-supported dialect; we never run the SQL

    def __init__(self, *, atomic_validation: bool = True) -> None:
        self._atomic_validation = atomic_validation
        self.executed: list[str] = []
        self.tx_events: list[str] = []

    @property
    def supports_atomic_validation(self) -> bool:
        return self._atomic_validation

    def execute_sql(self, sql: str, _conn_str: str) -> pa.Table:
        self.executed.append(sql)
        return pa.table({})

    def render(self, ast: exp.Expression) -> str:
        return ast.sql(dialect=self.dialect)

    @contextmanager
    def transaction(self, _conn_str: str) -> Iterator[None]:
        self.tx_events.append("enter")
        try:
            yield
            self.tx_events.append("commit")
        except BaseException:
            self.tx_events.append("rollback")
            raise


def _connection_with(backend: Backend) -> Connection:
    """Build a Connection wired to *backend* with a pre-warmed schema cache."""
    conn = Connection("fake://x", backend=backend)
    conn._schema_cache = {
        "users": ["id", "user_id"],
        "orders": ["id", "user_id"],
    }
    return conn


# ---------------------------------------------------------------------------
# LazyFrame.collect wrapping behaviour
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_collect_skips_transaction_when_no_validating_join() -> None:
    """No JOIN -> no tx wrapping; default ``m:m`` JOIN -> no tx wrapping."""
    backend = RecordingBackend()
    conn = _connection_with(backend)

    conn.table("users").select("id").collect()
    assert backend.tx_events == []

    backend.tx_events.clear()
    backend.executed.clear()
    conn.table("users").join(conn.table("orders"), on="user_id", how="inner").collect()
    assert backend.tx_events == []


@pytest.mark.unit
def test_collect_wraps_validating_join_in_transaction() -> None:
    """validate != 'm:m' opens a tx and runs both validation + main query
    inside it."""
    backend = RecordingBackend()
    conn = _connection_with(backend)

    conn.table("users").join(
        conn.table("orders"), on="user_id", how="left", validate="1:m"
    ).collect()

    assert backend.tx_events == ["enter", "commit"]
    # At least one validation query plus the main query.
    assert len(backend.executed) >= 2


@pytest.mark.unit
def test_collect_skips_validation_with_warning_on_non_atomic_backend() -> None:
    """Backends without atomic validation must emit a warning and skip the
    validation rather than running it sequentially (which would give a
    falsely-confident result)."""
    backend = RecordingBackend(atomic_validation=False)
    conn = _connection_with(backend)

    lf = conn.table("users").join(
        conn.table("orders"), on="user_id", how="left", validate="1:m"
    )
    with pytest.warns(UserWarning, match="does not support atomic"):
        lf.collect()

    # No validation queries should have run: only the main query.
    assert len(backend.executed) == 1
    # The wrapper still enters/commits — the skip is decided inside.
    assert backend.tx_events == ["enter", "commit"]


@pytest.mark.unit
def test_connection_transaction_delegates_to_backend() -> None:
    """Connection.transaction() must forward to backend.transaction()."""
    backend = RecordingBackend()
    conn = _connection_with(backend)

    with conn.transaction():
        pass

    assert backend.tx_events == ["enter", "commit"]


@pytest.mark.unit
def test_public_api_exposes_transaction_on_connection() -> None:
    """``pdb.Connection`` must expose ``transaction()`` for raw-SQL users
    who want atomic ``execute_raw`` sequences."""
    assert hasattr(pdb.Connection, "transaction")


# ---------------------------------------------------------------------------
# Real in-process driver round-trips (SQLite, DuckDB).
#
# These backends run inside the test process — no container, no mock — so
# they exercise the actual autocommit toggle / BEGIN / COMMIT / ROLLBACK
# path against a live driver while still living in the unit suite. The
# server-backed backends (Postgres, MySQL, SQL Server, BigQuery) get the
# same coverage from tests/integration/test_transaction.py.
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sqlite_real_transaction_commits_writes() -> None:
    """Writes inside the tx must persist after commit."""
    pytest.importorskip("adbc_driver_sqlite.dbapi")
    conn_str = "sqlite:///:memory:"
    backend = SQLiteBackend()
    try:
        backend.execute_sql("CREATE TABLE t (id INTEGER)", conn_str)
        with backend.transaction(conn_str):
            backend.execute_sql("INSERT INTO t VALUES (1)", conn_str)
            backend.execute_sql("INSERT INTO t VALUES (2)", conn_str)
        result = backend.execute_sql("SELECT COUNT(*) AS n FROM t", conn_str)
        assert result.to_pylist() == [{"n": 2}]
    finally:
        backend.close()


@pytest.mark.unit
def test_sqlite_real_transaction_rolls_back_on_exception() -> None:
    """Writes inside a tx that raises must be discarded."""
    pytest.importorskip("adbc_driver_sqlite.dbapi")
    conn_str = "sqlite:///:memory:"
    backend = SQLiteBackend()
    try:
        backend.execute_sql("CREATE TABLE t (id INTEGER)", conn_str)
        with (
            pytest.raises(RuntimeError, match="boom"),
            backend.transaction(conn_str),
        ):
            backend.execute_sql("INSERT INTO t VALUES (1)", conn_str)
            raise RuntimeError("boom")
        result = backend.execute_sql("SELECT COUNT(*) AS n FROM t", conn_str)
        assert result.to_pylist() == [{"n": 0}]
    finally:
        backend.close()


@pytest.mark.unit
def test_sqlite_real_collect_validate_one_to_many_passes() -> None:
    """End-to-end: collect() with validate='1:m' uses the tx wrapper and
    succeeds when the left key is unique."""
    pytest.importorskip("adbc_driver_sqlite.dbapi")
    conn = pdb.connect("sqlite:///:memory:")
    try:
        conn.execute_raw("CREATE TABLE u (id INT, name TEXT)")
        conn.execute_raw("INSERT INTO u VALUES (1,'a'),(2,'b')")
        conn.execute_raw("CREATE TABLE o (id INT, user_id INT)")
        conn.execute_raw("INSERT INTO o VALUES (1,1),(2,2),(3,2)")
        df = (
            conn.table("u")
            .join(
                conn.table("o"),
                left_on="id",
                right_on="user_id",
                how="left",
                validate="1:m",
            )
            .collect()
        )
        assert df.height == 3
    finally:
        conn.close()


@pytest.mark.unit
def test_sqlite_real_collect_validate_one_to_one_raises_on_duplicate() -> None:
    """End-to-end: collect() with validate='1:1' raises when the left key
    has duplicates (read inside the tx still sees them)."""
    pytest.importorskip("adbc_driver_sqlite.dbapi")
    conn = pdb.connect("sqlite:///:memory:")
    try:
        conn.execute_raw("CREATE TABLE u (id INT, name TEXT)")
        conn.execute_raw("INSERT INTO u VALUES (1,'a'),(1,'dup')")
        conn.execute_raw("CREATE TABLE o (id INT, user_id INT)")
        conn.execute_raw("INSERT INTO o VALUES (1,1)")
        lf = conn.table("u").join(
            conn.table("o"),
            left_on="id",
            right_on="user_id",
            how="left",
            validate="1:1",
        )
        with pytest.raises(JoinValidationError):
            lf.collect()
    finally:
        conn.close()


@pytest.mark.unit
def test_duckdb_real_transaction_commits_writes() -> None:
    """DuckDB's transaction() must round-trip BEGIN/COMMIT against the
    actual driver, not a mock."""
    pytest.importorskip("duckdb")
    conn_str = "duckdb:///:memory:"
    backend = DuckDBBackend()
    try:
        backend.execute_sql("CREATE TABLE t (id INTEGER)", conn_str)
        with backend.transaction(conn_str):
            backend.execute_sql("INSERT INTO t VALUES (1)", conn_str)
            backend.execute_sql("INSERT INTO t VALUES (2)", conn_str)
        result = backend.execute_sql("SELECT COUNT(*) AS n FROM t", conn_str)
        assert result.to_pylist() == [{"n": 2}]
    finally:
        backend.close()


@pytest.mark.unit
def test_duckdb_real_transaction_rolls_back_on_exception() -> None:
    pytest.importorskip("duckdb")
    conn_str = "duckdb:///:memory:"
    backend = DuckDBBackend()
    try:
        backend.execute_sql("CREATE TABLE t (id INTEGER)", conn_str)
        with (
            pytest.raises(RuntimeError, match="boom"),
            backend.transaction(conn_str),
        ):
            backend.execute_sql("INSERT INTO t VALUES (1)", conn_str)
            raise RuntimeError("boom")
        result = backend.execute_sql("SELECT COUNT(*) AS n FROM t", conn_str)
        assert result.to_pylist() == [{"n": 0}]
    finally:
        backend.close()
