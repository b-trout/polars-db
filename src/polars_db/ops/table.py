"""Table reference node (tree root)."""

from __future__ import annotations

from dataclasses import dataclass

from polars_db.ops.base import Op


@dataclass(frozen=True, eq=False)
class TableRef(Op):
    """Reference to a database table."""

    name: str
    schema: str | None = None
