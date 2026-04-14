"""Select and WithColumns operation nodes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from polars_db.ops.base import Op

if TYPE_CHECKING:
    from polars_db.expr import Expr


@dataclass(frozen=True, eq=False)
class SelectOp(Op):
    """SELECT clause: ``lf.select(col("name"), col("age"))``."""

    child: Op
    exprs: tuple[Expr, ...]


@dataclass(frozen=True, eq=False)
class WithColumnsOp(Op):
    """Add or overwrite columns while keeping existing ones.

    When an alias matches an existing column name the expression replaces
    that column in-place; otherwise it is appended.
    """

    child: Op
    exprs: tuple[Expr, ...]
