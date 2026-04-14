"""Join operation node."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from polars_db.ops.base import Op

if TYPE_CHECKING:
    from polars_db.expr import Expr


@dataclass(frozen=True, eq=False)
class JoinOp(Op):
    """JOIN: ``lf.join(other, on=..., how="left")``.

    Three key-specification patterns:
      1. ``on="key"``           -- same-name key (single)
      2. ``on=["k1", "k2"]``   -- same-name key (multiple)
      3. ``left_on / right_on`` -- different key names
    """

    left: Op
    right: Op
    on: tuple[Expr, ...] | None = None
    left_on: tuple[Expr, ...] | None = None
    right_on: tuple[Expr, ...] | None = None
    how: str = "inner"
    validate: str = "m:m"
