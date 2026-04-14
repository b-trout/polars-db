"""Exception hierarchy for polars-db."""


class PolarsDbError(Exception):
    """Base exception for polars-db."""


class UnsupportedOperationError(PolarsDbError):
    """Raised when an operation cannot be translated to SQL.

    User action: call ``collect()`` first, then apply the operation
    on the resulting ``polars.DataFrame``.
    """


class BackendNotSupportedError(PolarsDbError):
    """Raised when the connected database does not support a given SQL construct.

    User action: use a compatible backend or apply a workaround.
    """


class JoinValidationError(PolarsDbError):
    """Raised when JOIN ``validate`` cardinality check fails."""


class CompileError(PolarsDbError):
    """Raised during Op Tree to SQL compilation."""


class SchemaResolutionError(PolarsDbError):
    """Raised when table schema cannot be resolved."""
