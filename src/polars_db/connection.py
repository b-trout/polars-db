"""Database connection management.

Full implementation in the next step (feature/phase1-connection-lazyframe).
"""

from __future__ import annotations


class Connection:
    """Database connection placeholder."""

    def get_schema(self, table: str) -> list[str]:
        """Return column names for *table*."""
        raise NotImplementedError
