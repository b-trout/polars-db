"""Integration tests for per-thread backend connection caching (ADR-0019).

Each backend instance now keeps a thread-local connection slot so
concurrent ``collect()`` from multiple threads does not race on the
shared cursor that earlier versions cached. These tests exercise the
real per-backend driver through ``polars_db.connect`` from N worker
threads sharing one :class:`Connection`, asserting that:

* Concurrent SELECTs from several threads all succeed and return the
  rows each thread wrote.
* JOIN cardinality validation runs correctly per-thread (each thread
  opens its own transaction without interfering with the others).
* Each thread gets a distinct driver connection object so cursor
  state cannot leak across threads.

Each thread does its own ``CREATE TABLE`` / ``INSERT`` / ``DROP TABLE``
against a thread-namespaced table name so the tests work for both
server-backed backends (Postgres / MySQL / SQL Server) — where threads
share the underlying database — and in-memory backends (DuckDB /
SQLite ``:memory:``) — where the per-thread connection cache gives
each thread its own private database. Either way, the per-thread
cursor isolation is the property under test.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from polars_db.connection import Connection


@pytest.mark.integration
class TestConcurrentCollect:
    """Two threads, one Connection, real backend — no cross-contamination."""

    def test_parallel_select_each_thread_sees_its_own_writes(
        self, connection: Connection
    ) -> None:
        """Each thread creates its own table, writes rows, and reads them
        back. A single cached cursor leaking between threads would
        surface as wrong counts or driver errors mid-fetch."""

        def run(i: int) -> int:
            table = f"_tt_select_{i}"
            connection.execute_raw(f"CREATE TABLE {table} (n INT)")
            try:
                connection.execute_raw(f"INSERT INTO {table} VALUES (1),(2),(3)")
                return connection.table(table).collect().height
            finally:
                with suppress(Exception):
                    connection.execute_raw(f"DROP TABLE {table}")

        with ThreadPoolExecutor(max_workers=8) as pool:
            counts = list(pool.map(run, range(8)))

        assert counts == [3] * 8

    def test_parallel_validated_join_isolated_per_thread(
        self, connection: Connection
    ) -> None:
        """Validated JOIN opens a tx; running it from several threads at
        once must not have one thread's tx interfere with another's."""

        def run(i: int) -> int:
            left = f"_tt_join_l_{i}"
            right = f"_tt_join_r_{i}"
            connection.execute_raw(f"CREATE TABLE {left} (id INT)")
            connection.execute_raw(f"CREATE TABLE {right} (id INT, lid INT)")
            try:
                connection.execute_raw(f"INSERT INTO {left} VALUES (1),(2)")
                connection.execute_raw(
                    f"INSERT INTO {right} VALUES (10,1),(20,1),(30,2)"
                )
                return (
                    connection.table(left)
                    .join(
                        connection.table(right),
                        left_on="id",
                        right_on="lid",
                        how="inner",
                        validate="1:m",
                    )
                    .collect()
                    .height
                )
            finally:
                with suppress(Exception):
                    connection.execute_raw(f"DROP TABLE {right}")
                    connection.execute_raw(f"DROP TABLE {left}")

        with ThreadPoolExecutor(max_workers=4) as pool:
            heights = list(pool.map(run, range(8)))

        assert heights == [3] * 8


@pytest.mark.integration
class TestPerThreadConnections:
    """Each thread should get its own distinct driver connection."""

    def test_each_thread_gets_a_distinct_driver_connection(
        self, connection: Connection
    ) -> None:
        """The per-thread cache must give each worker thread a different
        underlying driver-connection object so cursor state cannot leak
        across them. Verified without relying on shared seed data —
        each thread does its own no-op DDL to materialise a connection
        and then reports the cached one."""

        def grab_conn(i: int) -> object:
            table = f"_tt_conn_{i}"
            connection.execute_raw(f"CREATE TABLE {table} (n INT)")
            try:
                return connection.backend._state.conn  # type: ignore[attr-defined]
            finally:
                with suppress(Exception):
                    connection.execute_raw(f"DROP TABLE {table}")

        with ThreadPoolExecutor(max_workers=4) as pool:
            seen = list(pool.map(grab_conn, range(4)))

        # All worker-thread connections must be materialised (non-None)
        # and pairwise distinct (no two threads share one).
        assert all(s is not None for s in seen)
        assert len({id(s) for s in seen}) == len(seen)
