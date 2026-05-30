"""Integration test for SQL Server ``create_if_missing`` race (ADR-0013).

Spawns N threads that simultaneously open Connections with
``create_if_missing=True`` against a database that does not exist yet.
Before the race-resolution fix one thread would win the
``IF DB_ID(...) IS NULL CREATE DATABASE`` step and the others would
raise error 1801 ("database already exists"). After the fix every
thread either creates the database or sees the 1801 and proceeds to
the follow-up connect, so all N callers succeed.

Only runs against the SQL Server matrix entry; other backends do not
have the auto-create mechanism this fix is about.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

import pytest

import polars_db as pdb

_RACE_DB = "polarsdb_create_race_test"


def _mssql_credentials() -> dict[str, str]:
    """Return pymssql kwargs that match the integration session config."""
    return {
        "server": os.environ.get("POLARS_DB_TEST_MSSQL_HOST", "localhost"),
        "port": os.environ.get("POLARS_DB_TEST_MSSQL_PORT", "1433"),
        "user": os.environ.get("POLARS_DB_TEST_MSSQL_USER", "sa"),
        "password": os.environ.get("POLARS_DB_TEST_MSSQL_PASSWORD", "Test@12345"),
    }


def _admin_conn_str() -> str:
    """Return a ``mssql://`` URL for ``master`` so callers can target a
    DB they want to create or drop."""
    creds = _mssql_credentials()
    return (
        f"mssql://{creds['user']}:{creds['password']}@"
        f"{creds['server']}:{creds['port']}/master"
    )


def _drop_race_db() -> None:
    """Drop the race database via a fresh pymssql autocommit=True master conn.

    ``ALTER DATABASE`` and ``DROP DATABASE`` cannot run inside the
    implicit transaction pymssql opens when autocommit=False, so we
    bypass the polars-db Connection wrapper here and talk to pymssql
    directly. Best-effort: silently skips if the DB doesn't exist.
    """
    pymssql = pytest.importorskip("pymssql")
    creds = _mssql_credentials()
    master = pymssql.connect(database="master", **creds)
    try:
        master.autocommit(True)
        cursor = master.cursor()
        cursor.execute(
            f"IF DB_ID('{_RACE_DB}') IS NOT NULL "
            f"ALTER DATABASE {_RACE_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
        )
        cursor.execute(f"IF DB_ID('{_RACE_DB}') IS NOT NULL DROP DATABASE {_RACE_DB}")
    finally:
        master.close()


@pytest.fixture()
def race_db_clean() -> None:
    """Ensure the race database is gone before and after the test."""
    _drop_race_db()
    yield
    _drop_race_db()


@pytest.mark.integration
@pytest.mark.backend_sqlserver
class TestCreateIfMissingRace:
    """Verify the auto-create path no longer falls over under concurrency."""

    def test_concurrent_create_if_missing_all_succeed(
        self, race_db_clean: None
    ) -> None:
        """8 threads racing to create the same database must all return a
        usable connection — the loser of the IF DB_ID/CREATE race
        should see error 1801 swallowed and proceed to the follow-up
        connect."""
        # Connection string targets the race DB (which does not exist yet).
        admin = _admin_conn_str()
        # Replace the ``/master`` tail with our race DB name.
        race_conn_str = admin.rsplit("/", 1)[0] + f"/{_RACE_DB}"

        def open_one(_i: int) -> bool:
            conn = pdb.connect(race_conn_str, create_if_missing=True)
            try:
                # Issue a trivial query to confirm the connection works.
                result = conn.execute_raw("SELECT 1 AS n")
                return result.to_dicts() == [{"n": 1}]
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(open_one, range(8)))

        assert all(outcomes), "every thread must have produced a usable connection"
