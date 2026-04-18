"""Compile Op trees into SQLGlot SELECT statements."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from polars_db.compiler.expr_compiler import ExprCompiler
from polars_db.exceptions import CompileError
from polars_db.expr import AliasExpr, ColExpr, Expr, WindowExpr
from polars_db.ops import (
    DistinctOp,
    DropOp,
    FilterOp,
    GroupByOp,
    JoinOp,
    LimitOp,
    Op,
    RenameOp,
    SelectOp,
    SortOp,
    TableRef,
    WithColumnsOp,
)

if TYPE_CHECKING:
    from polars_db.backends.base import Backend
    from polars_db.connection import Connection


class QueryCompiler:
    """Translate an Op tree into a SQLGlot AST."""

    def __init__(self, backend: Backend, connection: Connection | None = None) -> None:
        self._expr_compiler = ExprCompiler(backend)
        self._connection = connection
        self._subquery_counter = 0

    def compile(self, op: Op) -> exp.Expression:
        """Recursively compile an ``Op`` tree."""
        match op:
            case TableRef(name=name, schema=schema):
                table = exp.Table(this=exp.to_identifier(name))
                if schema:
                    table.set("db", exp.to_identifier(schema))
                return exp.Select(expressions=[exp.Star()]).from_(table)

            case FilterOp(child=child, predicate=predicate):
                inner = self.compile(child)
                condition = self._expr_compiler.compile(predicate)
                # SQL forbids window functions in WHERE; wrap in subquery
                if self._op_has_window(child):
                    inner = self._ensure_subquery(inner)
                    return (
                        exp.Select(expressions=[exp.Star()])
                        .from_(inner)
                        .where(condition)
                    )
                return inner.where(condition)

            case SelectOp(child=child, exprs=exprs):
                inner = self._ensure_subquery(self.compile(child))
                columns = [self._expr_compiler.compile(e) for e in exprs]
                return exp.Select(expressions=columns).from_(inner)

            case WithColumnsOp(child=child, exprs=exprs):
                inner = self.compile(child)
                new_cols = {
                    e.alias: self._expr_compiler.compile(e)
                    for e in exprs
                    if isinstance(e, AliasExpr)
                }
                new_only = [
                    self._expr_compiler.compile(e)
                    for e in exprs
                    if not isinstance(e, AliasExpr)
                ]

                if not new_cols and not new_only:
                    return inner

                if new_cols:
                    all_columns = self._resolve_columns(child)
                    result_cols: list[exp.Expression] = []
                    for col_name in all_columns:
                        if col_name in new_cols:
                            result_cols.append(new_cols[col_name])
                        else:
                            result_cols.append(
                                exp.Column(this=exp.to_identifier(col_name))
                            )
                    for e in exprs:
                        alias = e.alias if isinstance(e, AliasExpr) else None
                        if alias and alias not in set(all_columns):
                            result_cols.append(new_cols[alias])
                    result_cols.extend(new_only)
                    inner_sub = self._ensure_subquery(inner)
                    return exp.Select(expressions=result_cols).from_(inner_sub)

                compiled = [self._expr_compiler.compile(e) for e in exprs]
                return inner.select(*compiled, append=True)

            case GroupByOp(child=child, by=by, agg=agg):
                inner = self._ensure_subquery(self.compile(child))
                group_cols = [self._expr_compiler.compile(e) for e in by]
                agg_cols = [self._expr_compiler.compile(e) for e in agg]
                select = exp.Select(expressions=[*group_cols, *agg_cols]).from_(inner)
                return select.group_by(*group_cols)

            case JoinOp(how="semi") | JoinOp(how="anti"):
                return self._compile_semi_anti_join(op)

            case JoinOp():
                return self._compile_join(op)

            case SortOp(child=child, by=by, descending=descending):
                inner = self.compile(child)
                order_exprs = [
                    exp.Ordered(this=self._expr_compiler.compile(e), desc=d)
                    for e, d in zip(by, descending, strict=True)
                ]
                return inner.order_by(*order_exprs)

            case LimitOp(child=child, n=n, offset=offset):
                inner = self.compile(child)
                result = inner.limit(n)
                if offset > 0:
                    result = result.offset(offset)
                return result

            case DistinctOp(child=child):
                inner = self.compile(child)
                return inner.distinct()

            case RenameOp(child=child, mapping=mapping):
                all_columns = self._resolve_columns(child)
                rename_map = dict(mapping)
                inner = self._ensure_subquery(self.compile(child))
                cols = [
                    exp.Alias(
                        this=exp.Column(this=exp.to_identifier(c)),
                        alias=exp.to_identifier(rename_map[c]),
                    )
                    if c in rename_map
                    else exp.Column(this=exp.to_identifier(c))
                    for c in all_columns
                ]
                return exp.Select(expressions=cols).from_(inner)

            case DropOp(child=child, columns=columns):
                all_columns = self._resolve_columns(child)
                drop_set = set(columns)
                inner = self._ensure_subquery(self.compile(child))
                cols = [
                    exp.Column(this=exp.to_identifier(c))
                    for c in all_columns
                    if c not in drop_set
                ]
                return exp.Select(expressions=cols).from_(inner)

            case _:
                msg = f"Cannot compile operation: {type(op).__name__}"
                raise CompileError(msg)

    # -- column resolution ---------------------------------------------------

    def _resolve_columns(self, op: Op) -> list[str]:
        """Walk the Op tree to determine output column names."""
        match op:
            case TableRef(name=name):
                if self._connection is None:
                    msg = f"Connection required to resolve columns for table {name!r}"
                    raise CompileError(msg)
                return self._connection.get_schema(name)
            case SelectOp(exprs=exprs):
                return [self._extract_alias(e) for e in exprs]
            case WithColumnsOp(child=child, exprs=exprs):
                parent_cols = self._resolve_columns(child)
                new_aliases = {e.alias for e in exprs if isinstance(e, AliasExpr)}
                result = [c for c in parent_cols if c not in new_aliases]
                result.extend(self._extract_alias(e) for e in exprs)
                return result
            case RenameOp(child=child, mapping=mapping):
                parent_cols = self._resolve_columns(child)
                rename_map = dict(mapping)
                return [rename_map.get(c, c) for c in parent_cols]
            case DropOp(child=child, columns=columns):
                parent_cols = self._resolve_columns(child)
                drop_set = set(columns)
                return [c for c in parent_cols if c not in drop_set]
            case (
                FilterOp(child=child)
                | SortOp(child=child)
                | LimitOp(child=child)
                | DistinctOp(child=child)
            ):
                return self._resolve_columns(child)
            case GroupByOp(by=by, agg=agg):
                return [self._extract_alias(e) for e in (*by, *agg)]
            case JoinOp(how="semi") | JoinOp(how="anti"):
                return list(self._resolve_columns(op.left))
            case JoinOp():
                return self._resolve_join_columns(op)
            case _:
                msg = f"Cannot resolve columns for {type(op).__name__}"
                raise CompileError(msg)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _extract_alias(expr: Expr) -> str:
        if isinstance(expr, AliasExpr):
            return expr.alias
        if isinstance(expr, ColExpr):
            return expr.name
        msg = f"Cannot determine column name for {type(expr).__name__}"
        raise CompileError(msg)

    def _ensure_subquery(self, select: exp.Expression) -> exp.Subquery:
        alias = f"_t{self._subquery_counter}"
        self._subquery_counter += 1
        if isinstance(select, exp.Select):
            return select.subquery(alias)
        return select.subquery(alias)  # type: ignore[union-attr]

    @staticmethod
    def _join_type(how: str) -> str:
        # sqlglot .join() uses f"FROM _ {join_type} JOIN _" template
        mapping = {
            "inner": "",
            "left": "LEFT",
            "right": "RIGHT",
            "outer": "FULL OUTER",
            "cross": "CROSS",
        }
        if how in mapping:
            return mapping[how]
        msg = f"Unknown join type: {how!r}"
        raise ValueError(msg)

    def _compile_join_on_different(
        self,
        left_on: tuple[Expr, ...] | None,
        right_on: tuple[Expr, ...] | None,
    ) -> exp.Expression:
        if left_on is None or right_on is None:
            msg = "'left_on' and 'right_on' must both be specified"
            raise CompileError(msg)
        conditions = [
            exp.EQ(
                this=self._expr_compiler.compile(lk),
                expression=self._expr_compiler.compile(rk),
            )
            for lk, rk in zip(left_on, right_on, strict=True)
        ]
        return self._and_chain(conditions)

    def _compile_join(self, op: JoinOp) -> exp.Expression:
        """Compile a non-semi/anti JOIN, suffixing duplicate right columns."""
        left, right = op.left, op.right
        on, left_on, right_on = op.on, op.left_on, op.right_on
        join_type = self._join_type(op.how)

        # Detect whether the left/right outputs share any non-USING column
        # names. If not, fall back to the simple form so existing SQL tests
        # and integrations continue to emit the minimal JOIN form.
        collisions = self._collision_columns(op)
        if not collisions:
            left_sql = self.compile(left)
            right_sql = self._ensure_subquery(self.compile(right))
            if on is not None:
                using_cols = [self._expr_compiler.compile(e) for e in on]
                return left_sql.join(right_sql, using=using_cols, join_type=join_type)
            on_expr = self._compile_join_on_different(left_on, right_on)
            return left_sql.join(right_sql, on=on_expr, join_type=join_type)

        # Collision present: wrap both sides in aliased subqueries and
        # emit an explicit projection that qualifies each column and
        # suffixes the right-side duplicates.
        left_sub = self._ensure_subquery(self.compile(left))
        right_sub = self._ensure_subquery(self.compile(right))
        left_alias = left_sub.alias_or_name
        right_alias = right_sub.alias_or_name

        projection = self._build_join_projection(
            op=op,
            left_alias=left_alias,
            right_alias=right_alias,
            collisions=collisions,
        )

        select = exp.Select(expressions=projection).from_(left_sub)
        if on is not None:
            using_cols = [self._expr_compiler.compile(e) for e in on]
            return select.join(right_sub, using=using_cols, join_type=join_type)

        on_expr = self._compile_join_on_qualified(
            left_on, right_on, left_alias, right_alias
        )
        return select.join(right_sub, on=on_expr, join_type=join_type)

    def _compile_join_on_qualified(
        self,
        left_on: tuple[Expr, ...] | None,
        right_on: tuple[Expr, ...] | None,
        left_alias: str,
        right_alias: str,
    ) -> exp.Expression:
        """Compile ON condition with table-qualified column refs."""
        if left_on is None or right_on is None:
            msg = "'left_on' and 'right_on' must both be specified"
            raise CompileError(msg)
        conditions = [
            exp.EQ(
                this=exp.Column(
                    table=exp.to_identifier(left_alias),
                    this=exp.to_identifier(self._extract_col_name(lk)),
                ),
                expression=exp.Column(
                    table=exp.to_identifier(right_alias),
                    this=exp.to_identifier(self._extract_col_name(rk)),
                ),
            )
            for lk, rk in zip(left_on, right_on, strict=True)
        ]
        return self._and_chain(conditions)

    def _build_join_projection(
        self,
        *,
        op: JoinOp,
        left_alias: str,
        right_alias: str,
        collisions: set[str],
    ) -> list[exp.Expression]:
        """Build the explicit SELECT list for a collision-resolving JOIN."""
        left_cols = self._resolve_columns(op.left)
        right_cols = self._resolve_columns(op.right)
        using_keys = self._using_key_names(op)
        suffix = op.suffix

        cols: list[exp.Expression] = []

        # Left side: USING keys stay unqualified (they refer to the merged
        # column produced by USING); the remainder are qualified with the
        # left alias.
        for c in left_cols:
            if c in using_keys:
                cols.append(exp.Column(this=exp.to_identifier(c)))
            else:
                cols.append(
                    exp.Column(
                        table=exp.to_identifier(left_alias),
                        this=exp.to_identifier(c),
                    )
                )

        # Right side: skip USING keys (already emitted), qualify the rest,
        # alias duplicates to `<name><suffix>` so downstream ops see a
        # unique output name.
        for c in right_cols:
            if c in using_keys:
                continue
            qualified = exp.Column(
                table=exp.to_identifier(right_alias),
                this=exp.to_identifier(c),
            )
            if c in collisions:
                cols.append(
                    exp.Alias(this=qualified, alias=exp.to_identifier(c + suffix))
                )
            else:
                cols.append(qualified)
        return cols

    def _resolve_join_columns(self, op: JoinOp) -> list[str]:
        """Resolve output column names for a non-semi/anti JOIN."""
        left_cols = self._resolve_columns(op.left)
        right_cols = self._resolve_columns(op.right)
        using_keys = self._using_key_names(op)
        left_set = set(left_cols)
        suffix = op.suffix

        result = list(left_cols)
        for c in right_cols:
            if c in using_keys:
                continue
            result.append(c + suffix if c in left_set else c)
        return result

    def _collision_columns(self, op: JoinOp) -> set[str]:
        """Names that appear in both left and right outputs excluding USING keys.

        If column resolution is impossible (e.g. no connection is bound, as
        in lightweight compile-only tests), return an empty set so we emit
        the simple JOIN form. Downstream ambiguity only becomes reachable
        via ``.collect()``, which always has a connection.
        """
        try:
            left_cols = set(self._resolve_columns(op.left))
            right_cols = set(self._resolve_columns(op.right))
        except CompileError:
            return set()
        using_keys = self._using_key_names(op)
        return (left_cols & right_cols) - using_keys

    def _using_key_names(self, op: JoinOp) -> set[str]:
        """Column names merged by USING (empty when left_on/right_on is used)."""
        if op.on is None:
            return set()
        return {self._extract_col_name(e) for e in op.on}

    @staticmethod
    def _extract_col_name(expr: Expr) -> str:
        """Return the underlying column name for a key expression."""
        if isinstance(expr, ColExpr):
            return expr.name
        if isinstance(expr, AliasExpr):
            return expr.alias
        msg = f"Join key must be a column reference, got {type(expr).__name__}"
        raise CompileError(msg)

    def _compile_semi_anti_join(self, op: JoinOp) -> exp.Expression:
        left_sql = self.compile(op.left)
        left_table = self._resolve_table_alias(op.left)
        right_table = self._resolve_table_alias(op.right)
        correlated_cond = self._compile_correlated_join_condition(
            on=op.on,
            left_on=op.left_on,
            right_on=op.right_on,
            left_table=left_table,
            right_table=right_table,
        )
        exists_subquery = (
            exp.Select(expressions=[exp.Literal.number(1)])
            .from_(self._table_expr(op.right))
            .where(correlated_cond)
        )
        exists = exp.Exists(this=exists_subquery)
        condition = exists if op.how == "semi" else exp.Not(this=exists)
        return left_sql.where(condition)

    def _compile_correlated_join_condition(
        self,
        *,
        on: tuple[Expr, ...] | None,
        left_on: tuple[Expr, ...] | None,
        right_on: tuple[Expr, ...] | None,
        left_table: str,
        right_table: str,
    ) -> exp.Expression:
        if on is not None:
            left_keys = right_keys = on
        else:
            if left_on is None or right_on is None:
                msg = "Join keys must be specified"
                raise CompileError(msg)
            left_keys, right_keys = left_on, right_on

        conditions = []
        for lk, rk in zip(left_keys, right_keys, strict=True):
            lk_name = lk.name if isinstance(lk, ColExpr) else str(lk)
            rk_name = rk.name if isinstance(rk, ColExpr) else str(rk)
            conditions.append(
                exp.EQ(
                    this=exp.Column(
                        table=exp.to_identifier(left_table),
                        this=exp.to_identifier(lk_name),
                    ),
                    expression=exp.Column(
                        table=exp.to_identifier(right_table),
                        this=exp.to_identifier(rk_name),
                    ),
                )
            )
        return self._and_chain(conditions)

    def _resolve_table_alias(self, op: Op) -> str:
        match op:
            case TableRef(name=name):
                return name
            case JoinOp(left=left):
                return self._resolve_table_alias(left)
            case _ if hasattr(op, "child"):
                return self._resolve_table_alias(op.child)  # type: ignore[attr-defined]
            case _:
                msg = f"Cannot resolve table alias for {type(op).__name__}"
                raise CompileError(msg)

    def _table_expr(self, op: Op) -> exp.Table:
        match op:
            case TableRef(name=name, schema=schema):
                table = exp.Table(this=exp.to_identifier(name))
                if schema:
                    table.set("db", exp.to_identifier(schema))
                return table
            case _ if hasattr(op, "child"):
                return self._table_expr(op.child)  # type: ignore[attr-defined]
            case _:
                msg = f"Cannot resolve table for {type(op).__name__}"
                raise CompileError(msg)

    @staticmethod
    def _and_chain(conditions: list[exp.Expression]) -> exp.Expression:
        result = conditions[0]
        for c in conditions[1:]:
            result = exp.And(this=result, expression=c)
        return result

    @staticmethod
    def _expr_has_window(expr: Expr) -> bool:
        """Check if an Expr tree contains a WindowExpr."""
        if isinstance(expr, WindowExpr):
            return True
        if isinstance(expr, AliasExpr):
            return QueryCompiler._expr_has_window(expr.expr)
        return False

    @staticmethod
    def _op_has_window(op: Op) -> bool:
        """Check if an Op introduces window expressions in its output."""
        if isinstance(op, WithColumnsOp):
            return any(QueryCompiler._expr_has_window(e) for e in op.exprs)
        return False
