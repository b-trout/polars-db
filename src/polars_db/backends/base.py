"""Abstract base classes for database backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import sqlglot.expressions as exp

if TYPE_CHECKING:
    import pyarrow as pa


class Backend(ABC):
    """Database backend base class.

    Handles SQL dialect control and query execution.
    """

    @property
    @abstractmethod
    def dialect(self) -> str:
        """SQLGlot dialect name (e.g. ``"postgres"``, ``"duckdb"``)."""
        ...

    @abstractmethod
    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        """Execute SQL and return an Arrow Table."""
        ...

    def render(self, ast: exp.Expression) -> str:
        """Render a SQLGlot AST to a SQL string."""
        return ast.sql(dialect=self.dialect, pretty=True)

    def function_mapping(self) -> dict[str, str]:
        """DB-specific function name overrides."""
        return {}

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        """Build an EXPLAIN statement."""
        prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        return f"{prefix} {sql}"

    def format_explain_result(self, df: object) -> str:
        """Format EXPLAIN output as text."""
        import polars as pl

        if not isinstance(df, pl.DataFrame):
            msg = f"Expected polars.DataFrame, got {type(df)}"
            raise TypeError(msg)
        return "\n".join(builtins_str(v) for v in df.to_series(0).to_list())

    def schema_query(self, table: str) -> str:
        """Build a query to fetch column names for *table*.

        The query filters ``information_schema.columns`` by both
        ``table_name`` and the current database/schema so that columns
        from system tables with the same name (e.g. MySQL's
        ``mysql.user`` vs. a user-defined ``users`` table) do not bleed
        into the result.  The current-schema expression is produced by
        :meth:`current_schema_sql_expr`, which subclasses may override.

        Results are ordered by ``ordinal_position`` for deterministic
        column ordering across drivers.
        """
        return (
            exp.Select(expressions=[exp.Column(this=exp.to_identifier("column_name"))])
            .from_(
                exp.Table(
                    db=exp.to_identifier("information_schema"),
                    this=exp.to_identifier("columns"),
                )
            )
            .where(
                exp.And(
                    this=exp.EQ(
                        this=exp.Column(this=exp.to_identifier("table_name")),
                        expression=exp.Literal.string(table),
                    ),
                    expression=exp.EQ(
                        this=exp.Column(this=exp.to_identifier("table_schema")),
                        expression=self.current_schema_sql_expr(),
                    ),
                )
            )
            .order_by(exp.Column(this=exp.to_identifier("ordinal_position")))
            .sql(dialect=self.dialect)
        )

    def current_schema_sql_expr(self) -> exp.Expression:
        """Return an AST node that evaluates to the current schema/database.

        The default is ANSI SQL ``CURRENT_SCHEMA()``, which works for
        PostgreSQL.  Dialects that identify the current namespace
        differently (e.g. MySQL's ``DATABASE()`` or SQL Server's
        ``SCHEMA_NAME()``) should override this hook.
        """
        return exp.Anonymous(this="CURRENT_SCHEMA")


# Alias to avoid shadowing inside Backend methods
builtins_str = str
