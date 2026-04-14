"""polars-db: Query databases using Polars syntax."""

from polars_db.connection import Connection, connect
from polars_db.exceptions import (
    BackendNotSupportedError,
    CompileError,
    JoinValidationError,
    PolarsDbError,
    SchemaResolutionError,
    UnsupportedOperationError,
)
from polars_db.expr import col, lit, when
from polars_db.lazy_frame import LazyFrame

__all__ = [
    "BackendNotSupportedError",
    "CompileError",
    "Connection",
    "JoinValidationError",
    "LazyFrame",
    "PolarsDbError",
    "SchemaResolutionError",
    "UnsupportedOperationError",
    "col",
    "connect",
    "lit",
    "when",
]
