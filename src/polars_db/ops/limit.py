"""Limit operation node."""

from __future__ import annotations

from dataclasses import dataclass

from polars_db.ops.base import Op


@dataclass(frozen=True, eq=False)
class LimitOp(Op):
    """LIMIT / OFFSET: ``lf.limit(10)``."""

    child: Op
    n: int
    offset: int = 0
