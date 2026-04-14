"""Base class for operation tree nodes."""

from __future__ import annotations

from dataclasses import dataclass, fields

from polars_db.expr import _deep_eq


@dataclass(frozen=True, eq=False)
class Op:
    """Base operation node.

    ``eq=False`` mirrors the ``Expr`` design: Op nodes contain ``Expr``
    fields whose ``__eq__`` returns ``BinaryExpr``, making auto-generated
    equality unreliable.
    """

    def _structural_eq(self, other: Op) -> bool:
        """Internal structural comparison for testing."""
        if type(self) is not type(other):
            return False
        return all(
            _deep_eq(getattr(self, f.name), getattr(other, f.name))
            for f in fields(self)
        )
