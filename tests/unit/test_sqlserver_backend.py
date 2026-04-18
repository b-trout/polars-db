"""Unit tests for the SQL Server backend helpers.

These tests focus on pure helpers that do not require ``pymssql`` to be
installed (the driver is an optional extra).
"""

from __future__ import annotations

import pytest

from polars_db.backends.sqlserver import _validate_db_identifier


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
