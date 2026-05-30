"""Integration tests for backend transaction() behaviour.

Exercises the real BEGIN/COMMIT/ROLLBACK path on the per-backend matrix
that the CI integration job spins up via docker-compose. Mock-based
coverage of the same contract lives in ``tests/unit/test_transaction.py``
but is intentionally minimal — the unit suite would not have caught the
``conn.set_autocommit`` / ``conn.adbc_connection.set_autocommit`` API
mismatch that broke PR #51's first push (per ADR-0017).
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pytest

from polars_db.backends.bigquery import BigQueryBackend

if TYPE_CHECKING:
    from polars_db.connection import Connection


def _table_name(prefix: str, backend_name: str) -> str:
    """Return a backend-namespaced temp table name.

    Keeping table names disjoint per backend lets these tests share the
    ``connection`` session fixture without colliding with the seed
    fixtures (``users`` / ``orders``) used elsewhere.
    """
    return f"_tx_{prefix}_{backend_name}"


@pytest.mark.integration
class TestTransactionCommitRollback:
    """Verify real BEGIN/COMMIT/ROLLBACK round-trips per backend."""

    def test_commit_persists_writes(
        self, connection: Connection, backend_name: str
    ) -> None:
        """Writes inside the tx must survive after commit."""
        if not connection.backend.supports_atomic_validation:
            pytest.skip(f"{backend_name} does not support atomic transactions")

        table = _table_name("commit", backend_name)
        connection.execute_raw(f"CREATE TABLE {table} (id INT)")
        try:
            with connection.transaction():
                connection.execute_raw(f"INSERT INTO {table} VALUES (1)")
                connection.execute_raw(f"INSERT INTO {table} VALUES (2)")
            result = connection.execute_raw(f"SELECT COUNT(*) AS n FROM {table}")
            assert result.to_dicts() == [{"n": 2}]
        finally:
            connection.execute_raw(f"DROP TABLE {table}")

    def test_rollback_discards_writes_on_exception(
        self, connection: Connection, backend_name: str
    ) -> None:
        """Writes inside a tx that raises must be discarded."""
        if not connection.backend.supports_atomic_validation:
            pytest.skip(f"{backend_name} does not support atomic transactions")

        table = _table_name("rollback", backend_name)
        connection.execute_raw(f"CREATE TABLE {table} (id INT)")
        try:
            with pytest.raises(RuntimeError, match="boom"):  # noqa: SIM117
                with connection.transaction():
                    connection.execute_raw(f"INSERT INTO {table} VALUES (1)")
                    raise RuntimeError("boom")
            result = connection.execute_raw(f"SELECT COUNT(*) AS n FROM {table}")
            assert result.to_dicts() == [{"n": 0}]
        finally:
            connection.execute_raw(f"DROP TABLE {table}")

    def test_read_inside_transaction_returns_committed_data(
        self, connection: Connection, backend_name: str
    ) -> None:
        """A SELECT inside the tx must observe data committed before the
        tx began (basic snapshot sanity)."""
        if not connection.backend.supports_atomic_validation:
            pytest.skip(f"{backend_name} does not support atomic transactions")

        table = _table_name("read", backend_name)
        connection.execute_raw(f"CREATE TABLE {table} (id INT)")
        try:
            connection.execute_raw(f"INSERT INTO {table} VALUES (1),(2),(3)")
            with connection.transaction():
                result = connection.execute_raw(f"SELECT COUNT(*) AS n FROM {table}")
                assert result.to_dicts() == [{"n": 3}]
        finally:
            connection.execute_raw(f"DROP TABLE {table}")


@pytest.mark.integration
@pytest.mark.backend_bigquery
class TestBigQueryNonAtomicValidation:
    """BigQuery is the only backend whose ``transaction()`` is a no-op."""

    def test_supports_atomic_validation_is_false(self, connection: Connection) -> None:
        assert connection.backend.supports_atomic_validation is False

    def test_transaction_is_a_noop(self, connection: Connection) -> None:
        """Entering and exiting transaction() must not raise even though
        BigQuery does not actually open one."""
        with connection.transaction():
            pass

    def test_collect_with_validate_emits_warning(self, connection: Connection) -> None:
        """A validating JOIN on BigQuery must skip the probe and warn."""
        lf = connection.table("users").join(
            connection.table("orders"),
            left_on="id",
            right_on="user_id",
            how="inner",
            validate="1:m",
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            lf.collect()
        atomic_warnings = [
            w for w in caught if "does not support atomic" in str(w.message)
        ]
        assert atomic_warnings, "expected a UserWarning about atomic validation"


@pytest.mark.unit
def test_bigquery_supports_atomic_validation_flag_without_client() -> None:
    """The flag is a pure property and must be inspectable without a
    BigQuery client — callers reading it before connect() should not
    pay an import cost or trigger driver discovery."""
    assert BigQueryBackend().supports_atomic_validation is False
