"""Tests for Connection.get_schema TTL caching (ADR-0018).

Uses a small in-process RecordingBackend rather than mocks so the
real ``Connection`` code path — query compilation, polars conversion,
cache lookup — is exercised. The backend counts how many times
``schema_query`` was issued so each test asserts on call count rather
than peeking at private cache state.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import polars_db as pdb
from polars_db.backends.base import Backend
from polars_db.connection import Connection

if TYPE_CHECKING:
    from collections.abc import Iterator


class RecordingBackend(Backend):
    """Real Backend subclass that records every schema_query it runs.

    Returns a fixed two-column schema for every table so the tests can
    focus on cache behaviour. Not a mock — the methods are real
    implementations.
    """

    dialect = "postgres"  # any sqlglot dialect; we never run the SQL

    def __init__(self) -> None:
        self.schema_queries: list[str] = []

    def execute_sql(self, sql: str, _conn_str: str) -> pa.Table:
        # The Connection always invokes schema_query via execute(), so
        # we recognise the schema-query SQL and return a fake result.
        self.schema_queries.append(sql)
        # Return a two-row Arrow table representing column names.
        return pa.table({"column_name": ["id", "name"]})

    @contextmanager
    def transaction(self, _conn_str: str) -> Iterator[None]:
        yield


def _make_conn(*, ttl: float | None) -> tuple[Connection, RecordingBackend]:
    backend = RecordingBackend()
    return Connection("fake://x", backend=backend, schema_cache_ttl=ttl), backend


@pytest.mark.unit
def test_default_ttl_caches_forever() -> None:
    """``schema_cache_ttl=None`` — the historical default. A second call
    for the same table must not re-issue ``schema_query``."""
    conn, backend = _make_conn(ttl=None)
    conn.get_schema("users")
    conn.get_schema("users")
    conn.get_schema("users")
    assert len(backend.schema_queries) == 1


@pytest.mark.unit
def test_zero_ttl_disables_caching() -> None:
    """``schema_cache_ttl=0`` — every call re-fetches. Useful during DDL
    iteration when the schema is changing under the connection."""
    conn, backend = _make_conn(ttl=0)
    conn.get_schema("users")
    conn.get_schema("users")
    conn.get_schema("users")
    assert len(backend.schema_queries) == 3


@pytest.mark.unit
def test_positive_ttl_expires_entries() -> None:
    """A fresh entry within the TTL window is reused; once the TTL
    elapses, the next call re-fetches."""
    conn, backend = _make_conn(ttl=0.05)
    conn.get_schema("users")
    # Within the TTL window — still cached.
    conn.get_schema("users")
    assert len(backend.schema_queries) == 1
    # Wait past the TTL and call again — must re-fetch.
    time.sleep(0.1)
    conn.get_schema("users")
    assert len(backend.schema_queries) == 2


@pytest.mark.unit
def test_refresh_schema_invalidates_one_entry() -> None:
    """``refresh_schema(table)`` drops just that table's cache entry."""
    conn, backend = _make_conn(ttl=None)
    conn.get_schema("users")
    conn.get_schema("orders")
    assert len(backend.schema_queries) == 2

    conn.refresh_schema("users")
    conn.get_schema("users")  # refetched
    conn.get_schema("orders")  # still cached
    assert len(backend.schema_queries) == 3


@pytest.mark.unit
def test_refresh_schema_clears_all_when_called_without_table() -> None:
    """``refresh_schema()`` with no argument drops every cached entry."""
    conn, backend = _make_conn(ttl=None)
    conn.get_schema("users")
    conn.get_schema("orders")
    assert len(backend.schema_queries) == 2

    conn.refresh_schema()
    conn.get_schema("users")
    conn.get_schema("orders")
    assert len(backend.schema_queries) == 4


@pytest.mark.unit
def test_close_clears_cache() -> None:
    """Closing the connection drops cached entries so a subsequent
    schema query — were the connection somehow reused — would re-fetch."""
    conn, backend = _make_conn(ttl=None)
    conn.get_schema("users")
    conn.close()
    conn.get_schema("users")
    assert len(backend.schema_queries) == 2


@pytest.mark.unit
def test_connect_factory_forwards_ttl() -> None:
    """``pdb.connect`` must pass ``schema_cache_ttl`` through to the
    underlying Connection so the public API supports it."""
    conn = pdb.connect("sqlite:///:memory:", schema_cache_ttl=10)
    try:
        assert conn._schema_cache_ttl == 10
    finally:
        conn.close()


@pytest.mark.unit
def test_default_factory_keeps_permanent_cache() -> None:
    """No explicit ``schema_cache_ttl`` argument leaves the cache permanent
    (None) — matches pre-ADR-0018 behaviour."""
    conn = pdb.connect("sqlite:///:memory:")
    try:
        assert conn._schema_cache_ttl is None
    finally:
        conn.close()
