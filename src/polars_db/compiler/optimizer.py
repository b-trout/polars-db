"""SQL AST optimizer and JOIN validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlglot.expressions as exp

    from polars_db.ops.join import JoinOp

    from .query_compiler import QueryCompiler


class Optimizer:
    """Optimize a SQLGlot AST before rendering."""

    def optimize(self, ast: exp.Expression) -> exp.Expression:
        ast = self._remove_unnecessary_subqueries(ast)
        ast = self._merge_consecutive_filters(ast)
        return ast

    @staticmethod
    def _remove_unnecessary_subqueries(
        ast: exp.Expression,
    ) -> exp.Expression:
        """Remove ``SELECT * FROM (subquery)`` wrappers."""
        # Placeholder — real implementation in Phase 2
        return ast

    @staticmethod
    def _merge_consecutive_filters(
        ast: exp.Expression,
    ) -> exp.Expression:
        """Merge adjacent WHERE clauses with AND."""
        # Placeholder — real implementation in Phase 2
        return ast


class JoinValidator:
    """Validate JOIN cardinality constraints before execution."""

    def build_validation_queries(
        self, join_op: JoinOp, compiler: QueryCompiler
    ) -> list[str]:
        """Return validation SQL queries (empty for ``m:m``)."""
        if join_op.validate == "m:m":
            return []
        # Placeholder — real implementation in Phase 2
        return []
