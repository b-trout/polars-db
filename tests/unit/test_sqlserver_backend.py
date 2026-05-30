"""Unit tests for the SQL Server backend helpers.

These tests focus on pure helpers that do not require ``pymssql`` to be
installed (the driver is an optional extra). Driver-touching
behaviour (the actual auto-create race resolution) is covered in
``tests/integration/test_sqlserver_create_race.py``.
"""

from __future__ import annotations

import pytest

import polars_db as pdb
from polars_db.backends.sqlserver import (
    SQLServerBackend,
    _is_db_not_found,
    _mssql_error_code,
    _validate_db_identifier,
)
from polars_db.connection import detect_backend


@pytest.mark.unit
@pytest.mark.parametrize(
    "name",
    [
        "foo",
        "_foo",
        "foo_bar123",
        "A",
        "_",
        "MyDatabase",
        "db_1",
        "a" * 128,
    ],
)
def test_validate_db_identifier_accepts_valid_names(name: str) -> None:
    assert _validate_db_identifier(name) == name


@pytest.mark.unit
@pytest.mark.parametrize(
    "name",
    [
        "foo]; DROP DATABASE master; --",
        "foo'; DROP DATABASE master; --",
        "",
        "123foo",
        "foo-bar",
        "foo bar",
        "foo;bar",
        "foo]",
        "foo'",
        'foo"bar',
        "foo.bar",
        "foo/bar",
        "a" * 129,
    ],
)
def test_validate_db_identifier_rejects_invalid_names(name: str) -> None:
    with pytest.raises(ValueError, match="Invalid SQL Server database name"):
        _validate_db_identifier(name)


@pytest.mark.unit
def test_sqlserver_backend_create_if_missing_default_false() -> None:
    """By default, the backend must not auto-create databases."""
    backend = SQLServerBackend()
    assert backend._create_if_missing is False


@pytest.mark.unit
def test_sqlserver_backend_create_if_missing_true() -> None:
    """The flag round-trips onto the backend instance."""
    backend = SQLServerBackend(create_if_missing=True)
    assert backend._create_if_missing is True


@pytest.mark.unit
def test_detect_backend_mssql_default_false() -> None:
    """``detect_backend`` on an ``mssql://`` URL defaults to opt-out."""
    backend = detect_backend("mssql://sa:pw@localhost:1433/testdb")
    assert isinstance(backend, SQLServerBackend)
    assert backend._create_if_missing is False


@pytest.mark.unit
def test_detect_backend_mssql_forwards_create_if_missing() -> None:
    """``detect_backend`` propagates ``create_if_missing`` to the SQL Server backend."""
    backend = detect_backend(
        "mssql://sa:pw@localhost:1433/testdb", create_if_missing=True
    )
    assert isinstance(backend, SQLServerBackend)
    assert backend._create_if_missing is True


@pytest.mark.unit
def test_connect_mssql_default_create_if_missing_false() -> None:
    """``pdb.connect`` defaults to opt-out for SQL Server."""
    conn = pdb.connect("mssql://sa:pw@localhost:1433/testdb")
    assert isinstance(conn.backend, SQLServerBackend)
    assert conn.backend._create_if_missing is False


@pytest.mark.unit
def test_connect_mssql_create_if_missing_true() -> None:
    """``pdb.connect(..., create_if_missing=True)`` opts in."""
    conn = pdb.connect("mssql://sa:pw@localhost:1433/testdb", create_if_missing=True)
    assert isinstance(conn.backend, SQLServerBackend)
    assert conn.backend._create_if_missing is True


# ---------------------------------------------------------------------------
# Race-resolution helpers (ADR-0013 update)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_mssql_error_code_extracts_first_arg() -> None:
    """pymssql wraps the DB-Lib error number in ``exc.args[0]`` — the
    helper must return that as an int."""

    class _FakeError(Exception):
        pass

    assert _mssql_error_code(_FakeError(1801, b"already exists")) == 1801


@pytest.mark.unit
def test_mssql_error_code_returns_none_for_non_numeric_args() -> None:
    """An exception with no int code (e.g. a plain message) must return
    ``None`` so the caller's ``code in {...}`` checks short-circuit
    cleanly."""

    class _FakeError(Exception):
        pass

    assert _mssql_error_code(_FakeError("not a code")) is None
    assert _mssql_error_code(_FakeError()) is None


@pytest.mark.unit
def test_is_db_not_found_matches_4060_and_4063() -> None:
    """Race-resolution must recognise both "cannot open database" codes."""

    class _FakeError(Exception):
        pass

    assert _is_db_not_found(_FakeError(4060, b"...")) is True
    assert _is_db_not_found(_FakeError(4063, b"...")) is True
    assert _is_db_not_found(_FakeError(1801, b"...")) is False
    assert _is_db_not_found(_FakeError("no code")) is False
