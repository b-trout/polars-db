"""GroupBy operation node."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from polars_db.ops.base import Op

if TYPE_CHECKING:
    from polars_db.expr import Expr


@dataclass(frozen=True, eq=False)
class GroupByOp(Op):
    """GROUP BY + aggregation: ``lf.group_by("x").agg(col("y").sum())``."""

    child: Op
    by: tuple[Expr, ...]
    agg: tuple[Expr, ...]
