"""Integration tests for window functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars_db as pdb

if TYPE_CHECKING:
    from polars_db.connection import Connection


@pytest.mark.integration
class TestWindowPartition:
    """Tests for basic window partitioning on live database."""

    def test_sum_over_partition(self, connection: Connection) -> None:
        """Verify SUM() OVER (PARTITION BY ...) returns partition totals."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over("status").alias("status_total"),
            )
            .select("id", "status", "amount", "status_total")
            .collect()
        )
        assert "status_total" in result.columns
        assert len(result) == 6

        completed_rows = result.filter(result["status"] == "completed")
        totals = completed_rows["status_total"].to_list()
        assert all(t == totals[0] for t in totals)

    def test_count_over_partition(self, connection: Connection) -> None:
        """Verify COUNT() OVER (PARTITION BY ...) returns partition counts."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("id").count().over("status").alias("status_count"),
            )
            .select("id", "status", "status_count")
            .collect()
        )
        assert "status_count" in result.columns
        completed_rows = result.filter(result["status"] == "completed")
        assert completed_rows["status_count"][0] == 3

    def test_multiple_partition_keys(self, connection: Connection) -> None:
        """Verify window function with multiple partition keys."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .sum()
                .over("user_id", "status")
                .alias("user_status_total"),
            )
            .select("id", "user_id", "status", "amount", "user_status_total")
            .collect()
        )
        assert "user_status_total" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowRanking:
    """Tests for ranking window functions on live database."""

    def test_rank_over_partition(self, connection: Connection) -> None:
        """Verify RANK() OVER (PARTITION BY ...) executes correctly."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .rank()
                .over("status", order_by="id")
                .alias("amount_rank"),
            )
            .select("id", "status", "amount", "amount_rank")
            .collect()
        )
        assert "amount_rank" in result.columns
        assert len(result) == 6

    def test_row_number_over_partition(self, connection: Connection) -> None:
        """Verify ROW_NUMBER() OVER (PARTITION BY ...) executes correctly."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("id")
                .row_number()
                .over("status", order_by="id")
                .alias("row_num"),
            )
            .select("id", "status", "row_num")
            .collect()
        )
        assert "row_num" in result.columns
        assert len(result) == 6

    def test_dense_rank_over_partition(self, connection: Connection) -> None:
        """Verify DENSE_RANK() OVER (PARTITION BY ...) executes correctly."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .dense_rank()
                .over("status", order_by="id")
                .alias("dense_rnk"),
            )
            .select("id", "status", "amount", "dense_rnk")
            .collect()
        )
        assert "dense_rnk" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowShift:
    """Tests for LAG/LEAD window functions on live database."""

    def test_shift_lag(self, connection: Connection) -> None:
        """Verify .shift(1) compiles to LAG and executes correctly."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .shift(1)
                .over("status", order_by="id")
                .alias("prev_amount"),
            )
            .select("id", "status", "amount", "prev_amount")
            .collect()
        )
        assert "prev_amount" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowOrderBy:
    """Tests for window functions with ORDER BY clause."""

    def test_sum_over_with_order_by(self, connection: Connection) -> None:
        """Verify SUM() OVER (PARTITION BY ... ORDER BY ...) executes correctly."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .sum()
                .over("status", order_by="id")
                .alias("running_total"),
            )
            .select("id", "status", "amount", "running_total")
            .collect()
        )
        assert "running_total" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowCumulative:
    """Tests for cumulative window functions on live database."""

    def test_cum_sum(self, connection: Connection) -> None:
        """Verify cum_sum produces cumulative sums within partitions."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .cum_sum()
                .over("status", order_by="id")
                .alias("cum_total"),
            )
            .select("id", "status", "amount", "cum_total")
            .collect()
        )
        assert "cum_total" in result.columns
        assert len(result) == 6

    def test_cum_count(self, connection: Connection) -> None:
        """Verify cum_count produces cumulative counts within partitions."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("id")
                .cum_count()
                .over("status", order_by="id")
                .alias("cum_cnt"),
            )
            .select("id", "status", "cum_cnt")
            .collect()
        )
        assert "cum_cnt" in result.columns
        assert len(result) == 6

    def test_cum_max(self, connection: Connection) -> None:
        """Verify cum_max produces cumulative maximums within partitions."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .cum_max()
                .over("status", order_by="id")
                .alias("running_max"),
            )
            .select("id", "status", "amount", "running_max")
            .collect()
        )
        assert "running_max" in result.columns
        assert len(result) == 6

    def test_cum_min(self, connection: Connection) -> None:
        """Verify cum_min produces cumulative minimums within partitions."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount")
                .cum_min()
                .over("status", order_by="id")
                .alias("running_min"),
            )
            .select("id", "status", "amount", "running_min")
            .collect()
        )
        assert "running_min" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowGlobal:
    """Tests for window functions without PARTITION BY."""

    def test_sum_over_all_rows(self, connection: Connection) -> None:
        """Verify SUM() OVER () computes across all rows."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over().alias("grand_total"),
            )
            .select("id", "amount", "grand_total")
            .collect()
        )
        assert "grand_total" in result.columns
        assert len(result) == 6
        # All rows should have the same grand total
        totals = result["grand_total"].to_list()
        assert all(t == totals[0] for t in totals)

    def test_count_over_all_rows(self, connection: Connection) -> None:
        """Verify COUNT() OVER () returns total row count for every row."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("id").count().over().alias("total_count"),
            )
            .select("id", "total_count")
            .collect()
        )
        assert "total_count" in result.columns
        counts = result["total_count"].to_list()
        assert all(c == 6 for c in counts)


@pytest.mark.integration
class TestWindowMultiple:
    """Tests for multiple window functions in a single query."""

    def test_two_windows_same_partition(self, connection: Connection) -> None:
        """Verify two window functions with the same partition key."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over("status").alias("status_sum"),
                pdb.col("amount").max().over("status").alias("status_max"),
            )
            .select("id", "status", "amount", "status_sum", "status_max")
            .collect()
        )
        assert "status_sum" in result.columns
        assert "status_max" in result.columns
        assert len(result) == 6

    def test_two_windows_different_partitions(self, connection: Connection) -> None:
        """Verify two window functions with different partition keys."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over("status").alias("status_sum"),
                pdb.col("amount").sum().over("user_id").alias("user_sum"),
            )
            .select("id", "status", "user_id", "amount", "status_sum", "user_sum")
            .collect()
        )
        assert "status_sum" in result.columns
        assert "user_sum" in result.columns
        assert len(result) == 6


@pytest.mark.integration
class TestWindowInSubquery:
    """Tests for window functions used in subqueries."""

    def test_filter_after_window(self, connection: Connection) -> None:
        """Verify filtering on a window function result works.

        This forces the window into a subquery, which is important
        for SQL Server compatibility (ORDER BY in subqueries).
        """
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over("status").alias("status_total"),
            )
            .filter(pdb.col("status_total") > 100)
            .select("id", "status", "amount", "status_total")
            .collect()
        )
        assert "status_total" in result.columns
        # Only partitions with total > 100 should remain
        assert len(result) > 0

    def test_sort_after_window(self, connection: Connection) -> None:
        """Verify sorting by a window function result works."""
        result = (
            connection.table("orders")
            .with_columns(
                pdb.col("amount").sum().over("status").alias("status_total"),
            )
            .sort("status_total", descending=True)
            .select("id", "status", "amount", "status_total")
            .collect()
        )
        assert "status_total" in result.columns
        assert len(result) == 6
