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
        # Apply passes until the AST stops changing so that deeply nested
        # subqueries (e.g. 3+ levels of filter wrappers) fully collapse in
        # a single ``optimize()`` call. A bounded iteration count provides
        # a defensive guard against pathological cases.
        max_iterations = 10
        prev_sql: str | None = None
        for _ in range(max_iterations):
            ast = self._remove_unnecessary_subqueries(ast)
            ast = self._merge_consecutive_filters(ast)
            cur_sql = ast.sql()
            if prev_sql == cur_sql:
                break
            prev_sql = cur_sql
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
            # Use .args to check direct clauses only, not recursive .find()
            outer_exprs = grandparent.expressions
            is_star = len(outer_exprs) == 1 and isinstance(outer_exprs[0], exp.Star)
            has_where = grandparent.args.get("where") is not None
            has_group = grandparent.args.get("group") is not None
            has_order = grandparent.args.get("order") is not None
            has_limit = grandparent.args.get("limit") is not None
            has_join = bool(grandparent.args.get("joins"))

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

        # Use args to get the direct WHERE clause, not find() which recurses
        outer_where = ast.args.get("where")
        if outer_where is None:
            return ast

        # sqlglot v30+ uses "from_" as the key
        from_clause = ast.args.get("from") or ast.args.get("from_")
        if from_clause is None:
            return ast

        # Check if FROM contains a subquery
        from_this = from_clause.this
        if not isinstance(from_this, exp.Subquery):
            return ast

        inner = from_this.this
        if not isinstance(inner, exp.Select):
            return ast

        inner_where = inner.args.get("where")
        if inner_where is None:
            return ast

        # Merge: inner WHERE cond AND outer WHERE cond
        merged = exp.And(this=inner_where.this, expression=outer_where.this)
        inner_where.set("this", merged)

        # Remove the outer WHERE. The collapsed subquery is returned
        # directly — mutating ``ast`` here would be dead work because the
        # caller reassigns from the return value.
        outer_where.pop()

        return inner


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
        # MySQL rejects unaliased derived tables ("Every derived table must
        # have its own alias", error 1248); T-SQL tolerates them only in
        # some contexts. Use a fixed private alias so emitted SQL is valid
        # across all dialects.
        sub = inner.subquery("_v") if isinstance(inner, exp.Select) else inner

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
