"""Filter operation node."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from polars_db.ops.base import Op

if TYPE_CHECKING:
    from polars_db.expr import Expr


@dataclass(frozen=True, eq=False)
class FilterOp(Op):
    """WHERE clause: ``lf.filter(col("age") > 30)``."""

    child: Op
    predicate: Expr
