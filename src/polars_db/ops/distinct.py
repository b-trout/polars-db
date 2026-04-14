"""Distinct, Rename, and Drop operation nodes."""

from __future__ import annotations

from dataclasses import dataclass

from polars_db.ops.base import Op


@dataclass(frozen=True, eq=False)
class DistinctOp(Op):
    """DISTINCT: ``lf.unique()``."""

    child: Op
    subset: tuple[str, ...] | None = None


@dataclass(frozen=True, eq=False)
class RenameOp(Op):
    """Column rename: ``lf.rename({"old": "new"})``."""

    child: Op
    mapping: tuple[tuple[str, str], ...]


@dataclass(frozen=True, eq=False)
class DropOp(Op):
    """Column drop: ``lf.drop("col1", "col2")``."""

    child: Op
    columns: tuple[str, ...]
