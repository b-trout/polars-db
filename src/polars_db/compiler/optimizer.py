"""SQL AST optimizer and JOIN validator."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

if TYPE_CHECKING:
    from polars_db.ops.join import JoinOp

    from .query_compiler import QueryCompiler


class Optimizer:
    """Optimize a SQLGlot AST before rendering."""

    def optimize(self, ast: exp.Expression) -> exp.Expression:
        ast = self._remove_unnecessary_subqueries(ast)
        ast = self._merge_consecutive_filters(ast)
        return ast

    @staticmethod
    def _remove_unnecessary_subqueries(ast: exp.Expression) -> exp.Expression:
        """Remove ``SELECT * FROM (subquery)`` wrappers.

        Transforms ``SELECT * FROM (SELECT ... FROM t)`` into the inner
        select when the outer select adds nothing.
        """
        if not isinstance(ast, exp.Select):
            return ast

        # Walk the tree bottom-up
        for subquery in ast.find_all(exp.Subquery):
            parent = subquery.parent
            if not isinstance(parent, exp.From):
                continue
            grandparent = parent.parent
            if not isinstance(grandparent, exp.Select):
                continue

            # Only collapse if the outer SELECT is `SELECT *` with no
            # additional clauses (no WHERE, GROUP BY, ORDER BY, etc.)
            outer_exprs = grandparent.expressions
            is_star = len(outer_exprs) == 1 and isinstance(outer_exprs[0], exp.Star)
            has_where = grandparent.find(exp.Where) is not None
            has_group = grandparent.find(exp.Group) is not None
            has_order = grandparent.find(exp.Order) is not None
            has_limit = grandparent.find(exp.Limit) is not None
            has_join = grandparent.find(exp.Join) is not None

            if is_star and not any(
                [has_where, has_group, has_order, has_limit, has_join]
            ):
                inner = subquery.this
                if isinstance(inner, exp.Select):
                    grandparent.replace(inner)

        return ast

    @staticmethod
    def _merge_consecutive_filters(ast: exp.Expression) -> exp.Expression:
        """Merge adjacent WHERE clauses with AND.

        Transforms ``SELECT * FROM (SELECT * FROM t WHERE a) WHERE b``
        into ``SELECT * FROM t WHERE a AND b``.
        """
        if not isinstance(ast, exp.Select):
            return ast

        outer_where = ast.find(exp.Where)
        if outer_where is None:
            return ast

        from_clause = ast.find(exp.From)
        if from_clause is None:
            return ast

        subquery = from_clause.find(exp.Subquery)
        if subquery is None:
            return ast

        inner = subquery.this
        if not isinstance(inner, exp.Select):
            return ast

        inner_where = inner.find(exp.Where)
        if inner_where is None:
            return ast

        # Merge: inner WHERE cond AND outer WHERE cond
        merged = exp.And(this=inner_where.this, expression=outer_where.this)
        inner_where.set("this", merged)

        # Remove outer WHERE and collapse subquery
        outer_where.pop()
        ast.set("from", exp.From(this=inner))

        return inner

    @staticmethod
    def _is_select_star(select: exp.Select) -> bool:
        exprs = select.expressions
        return len(exprs) == 1 and isinstance(exprs[0], exp.Star)


class JoinValidator:
    """Validate JOIN cardinality constraints before execution."""

    def build_validation_queries(
        self, join_op: JoinOp, compiler: QueryCompiler
    ) -> list[str]:
        """Return SQL queries to validate key uniqueness.

        For ``validate="1:m"``, check left key uniqueness.
        For ``validate="m:1"``, check right key uniqueness.
        For ``validate="1:1"``, check both.
        """
        if join_op.validate == "m:m":
            return []

        queries: list[str] = []
        check_left = join_op.validate in ("1:1", "1:m")
        check_right = join_op.validate in ("1:1", "m:1")

        if check_left:
            left_keys = join_op.on or join_op.left_on
            if left_keys:
                queries.append(
                    self._uniqueness_query(join_op.left, left_keys, compiler)
                )

        if check_right:
            right_keys = join_op.on or join_op.right_on
            if right_keys:
                queries.append(
                    self._uniqueness_query(join_op.right, right_keys, compiler)
                )

        return queries

    @staticmethod
    def _uniqueness_query(
        op: object, keys: tuple[object, ...], compiler: QueryCompiler
    ) -> str:
        """Generate ``GROUP BY key HAVING COUNT(*) > 1 LIMIT 1``."""
        from polars_db.expr import ColExpr

        key_cols = [
            compiler._expr_compiler.compile(k)
            if isinstance(k, ColExpr)
            else exp.Column(this=exp.to_identifier(str(k)))
            for k in keys
        ]
        inner = compiler.compile(op)  # type: ignore[arg-type]
        sub = inner.subquery() if isinstance(inner, exp.Select) else inner

        select = exp.Select(expressions=key_cols).from_(sub)
        select = select.group_by(*key_cols)
        select = select.having(
            exp.GT(
                this=exp.Count(this=exp.Star()),
                expression=exp.Literal.number(1),
            )
        )
        select = select.limit(1)
        return select.sql(dialect=compiler._expr_compiler._backend.dialect)
