"""Sort operation node."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from polars_db.ops.base import Op

if TYPE_CHECKING:
    from polars_db.expr import Expr


@dataclass(frozen=True, eq=False)
class SortOp(Op):
    """ORDER BY: ``lf.sort("age", descending=True)``."""

    child: Op
    by: tuple[Expr, ...]
    descending: tuple[bool, ...]
