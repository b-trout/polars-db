"""Shared test fixtures and auto-marker logic for integration tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from collections.abc import Iterator

    from polars_db.connection import Connection

# Immutable backend connection configuration
BACKEND_CONFIG: dict[str, dict[str, object]] = {
    "postgres": {"conn_str": "postgresql://test:test@localhost:5432/testdb"},
    "mysql": {"conn_str": "mysql://root:test@localhost:3306/testdb"},
    "sqlserver": {
        "conn_str": "mssql://sa:Test@12345@localhost:1433/testdb",
        # CI needs the test DB to be auto-created on a fresh container.
        # See ADR-0013 for the rationale behind the opt-in flag.
        "create_if_missing": True,
    },
    "duckdb": {"conn_str": "duckdb:///:memory:"},
    "sqlite": {"conn_str": "sqlite:///:memory:"},
    "bigquery": {
        "conn_str": "bigquery://test-project/test_dataset",
    },
}


def _resolve_seed_statements(backend: str) -> tuple[str, ...]:
    """Resolve seed SQL statements for a backend."""
    seed_file = Path(__file__).parent / "fixtures" / f"seed_{backend}.sql"
    if seed_file.exists():
        return (seed_file.read_text(),)
    from tests.fixtures.test_data import SEED_STATEMENTS

    return SEED_STATEMENTS.get(backend, SEED_STATEMENTS["default"])


def _execute_all(conn: Connection, statements: tuple[str, ...]) -> None:
    """Execute a tuple of SQL statements sequentially."""
    for stmt in statements:
        conn.execute_raw(stmt)


@pytest.fixture(scope="session")
def backend_name() -> str:
    return os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")


@pytest.fixture(scope="session")
def connection(backend_name: str) -> Iterator[pdb.Connection]:
    if backend_name == "bigquery" and not os.environ.get("BIGQUERY_EMULATOR_HOST"):
        os.environ["BIGQUERY_EMULATOR_HOST"] = "localhost:9050"
    config = dict(BACKEND_CONFIG[backend_name])
    conn = pdb.connect(**config)
    _execute_all(conn, _resolve_seed_statements(backend_name))
    yield conn
    conn.close()


# ---------- Auto-marker logic ----------


def _parse_backend_names(raw_markers: list[str]) -> frozenset[str]:
    """Extract backend_ names from pytest marker strings."""
    return frozenset(
        m.split(":")[0].strip() for m in raw_markers if m.startswith("backend_")
    )


def _is_integration(item: pytest.Item) -> bool:
    return "integration" in frozenset(m.name for m in item.iter_markers())


def _has_backend_marker(item: pytest.Item, all_backends: frozenset[str]) -> bool:
    return any(m.name in all_backends for m in item.iter_markers())


def _missing_markers(item: pytest.Item, all_backends: frozenset[str]) -> frozenset[str]:
    """Return backend markers that should be added to an integration test."""
    if not _is_integration(item):
        return frozenset()
    if _has_backend_marker(item, all_backends):
        return frozenset()
    return all_backends


def pytest_collection_modifyitems(
    items: list[pytest.Item], config: pytest.Config
) -> None:
    """Add all backend markers to integration tests without specific backend markers."""
    all_backends = _parse_backend_names(config.getini("markers"))
    for item in items:
        for backend in _missing_markers(item, all_backends):
            item.add_marker(getattr(pytest.mark, backend))
