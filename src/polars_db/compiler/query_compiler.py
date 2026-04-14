"""Compile Op trees into SQLGlot SELECT statements."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlglot.expressions as exp

from polars_db.compiler.expr_compiler import ExprCompiler
from polars_db.exceptions import CompileError
from polars_db.expr import AliasExpr, ColExpr, Expr
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

            case JoinOp(
                left=left,
                right=right,
                on=on,
                left_on=left_on,
                right_on=right_on,
                how=how,
            ):
                left_sql = self.compile(left)
                right_sql = self._ensure_subquery(self.compile(right))
                join_type = self._join_type(how)
                if on is not None:
                    on_expr = self._compile_join_on_same(on)
                else:
                    on_expr = self._compile_join_on_different(left_on, right_on)
                return left_sql.join(right_sql, on=on_expr, join_type=join_type)

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
            case JoinOp(left=left, right=right):
                return self._resolve_columns(left) + self._resolve_columns(right)
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

    @staticmethod
    def _ensure_subquery(select: exp.Expression) -> exp.Subquery:
        if isinstance(select, exp.Select):
            return select.subquery()
        return select.subquery()  # type: ignore[union-attr]

    @staticmethod
    def _join_type(how: str) -> str:
        mapping = {
            "inner": "JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "outer": "FULL OUTER JOIN",
            "cross": "CROSS JOIN",
        }
        if how in mapping:
            return mapping[how]
        msg = f"Unknown join type: {how!r}"
        raise ValueError(msg)

    def _compile_join_on_same(self, on: tuple[Expr, ...]) -> exp.Expression:
        conditions = [
            exp.EQ(
                this=self._expr_compiler.compile(e),
                expression=self._expr_compiler.compile(e),
            )
            for e in on
        ]
        return self._and_chain(conditions)

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
