"""Tests for _structural_eq() and _deep_eq()."""

import pytest

from polars_db.expr import (
    AggExpr,
    AliasExpr,
    BinaryExpr,
    ColExpr,
    LitExpr,
    _deep_eq,
)
from polars_db.ops import FilterOp, SelectOp, SortOp, TableRef


@pytest.mark.unit
class TestDeepEq:
    def test_primitives(self) -> None:
        assert _deep_eq(1, 1)
        assert _deep_eq("a", "a")
        assert not _deep_eq(1, 2)

    def test_none(self) -> None:
        assert _deep_eq(None, None)
        assert not _deep_eq(None, 1)

    def test_expr_nodes(self) -> None:
        a = ColExpr(name="x")
        b = ColExpr(name="x")
        c = ColExpr(name="y")
        assert _deep_eq(a, b)
        assert not _deep_eq(a, c)

    def test_nested_tuples(self) -> None:
        a = (ColExpr(name="x"), LitExpr(value=1))
        b = (ColExpr(name="x"), LitExpr(value=1))
        c = (ColExpr(name="x"), LitExpr(value=2))
        assert _deep_eq(a, b)
        assert not _deep_eq(a, c)

    def test_mixed_types(self) -> None:
        assert not _deep_eq(ColExpr(name="x"), LitExpr(value="x"))

    def test_list_vs_tuple(self) -> None:
        assert not _deep_eq([ColExpr(name="x")], (ColExpr(name="x"),))


@pytest.mark.unit
class TestExprStructuralEq:
    def test_col_eq(self) -> None:
        assert ColExpr(name="a")._structural_eq(ColExpr(name="a"))
        assert not ColExpr(name="a")._structural_eq(ColExpr(name="b"))

    def test_lit_eq(self) -> None:
        assert LitExpr(value=42)._structural_eq(LitExpr(value=42))
        assert not LitExpr(value=42)._structural_eq(LitExpr(value=0))

    def test_binary_eq(self) -> None:
        a = BinaryExpr(op=">", left=ColExpr(name="x"), right=LitExpr(value=1))
        b = BinaryExpr(op=">", left=ColExpr(name="x"), right=LitExpr(value=1))
        c = BinaryExpr(op="<", left=ColExpr(name="x"), right=LitExpr(value=1))
        assert a._structural_eq(b)
        assert not a._structural_eq(c)

    def test_alias_eq(self) -> None:
        inner = AggExpr(func="sum", arg=ColExpr(name="x"))
        a = AliasExpr(expr=inner, alias="total")
        b = AliasExpr(expr=inner, alias="total")
        c = AliasExpr(expr=inner, alias="other")
        assert a._structural_eq(b)
        assert not a._structural_eq(c)

    def test_different_types(self) -> None:
        assert not ColExpr(name="x")._structural_eq(LitExpr(value="x"))


@pytest.mark.unit
class TestOpStructuralEq:
    def test_table_ref(self) -> None:
        a = TableRef(name="users")
        b = TableRef(name="users")
        c = TableRef(name="orders")
        assert a._structural_eq(b)
        assert not a._structural_eq(c)

    def test_filter_op(self) -> None:
        pred = BinaryExpr(op=">", left=ColExpr(name="age"), right=LitExpr(value=30))
        a = FilterOp(child=TableRef(name="users"), predicate=pred)
        b = FilterOp(child=TableRef(name="users"), predicate=pred)
        assert a._structural_eq(b)

    def test_nested_ops(self) -> None:
        base = TableRef(name="users")
        pred = BinaryExpr(op=">", left=ColExpr(name="age"), right=LitExpr(value=30))
        filtered = FilterOp(child=base, predicate=pred)
        selected = SelectOp(child=filtered, exprs=(ColExpr(name="name"),))
        sorted_op = SortOp(
            child=selected, by=(ColExpr(name="name"),), descending=(False,)
        )

        # Rebuild the same tree
        sorted_op2 = SortOp(
            child=SelectOp(
                child=FilterOp(
                    child=TableRef(name="users"),
                    predicate=BinaryExpr(
                        op=">", left=ColExpr(name="age"), right=LitExpr(value=30)
                    ),
                ),
                exprs=(ColExpr(name="name"),),
            ),
            by=(ColExpr(name="name"),),
            descending=(False,),
        )
        assert sorted_op._structural_eq(sorted_op2)
