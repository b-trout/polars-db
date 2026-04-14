"""Polars dtype <-> SQL type mapping.

Defines bidirectional mappings between SQL types and Polars data types.
"""

from __future__ import annotations

# SQL type string -> Polars dtype
SQL_TO_POLARS: dict[str, str] = {
    # Integer types
    "INTEGER": "Int32",
    "INT": "Int32",
    "BIGINT": "Int64",
    "SMALLINT": "Int16",
    "TINYINT": "Int8",
    # Boolean
    "BOOLEAN": "Boolean",
    "BOOL": "Boolean",
    # Floating point
    "REAL": "Float32",
    "FLOAT": "Float32",
    "FLOAT4": "Float32",
    "DOUBLE PRECISION": "Float64",
    "DOUBLE": "Float64",
    "FLOAT8": "Float64",
    # String types
    "TEXT": "Utf8",
    "VARCHAR": "Utf8",
    "CHAR": "Utf8",
    "STRING": "Utf8",
    # Date/time
    "DATE": "Date",
    "TIMESTAMP": "Datetime",
    "TIMESTAMP WITH TIME ZONE": "Datetime",
    # Binary
    "BLOB": "Binary",
    "BYTEA": "Binary",
    # Decimal
    "NUMERIC": "Decimal",
    "DECIMAL": "Decimal",
}

# Polars dtype string -> SQL type string
POLARS_TO_SQL: dict[str, str] = {
    "Int8": "TINYINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "UInt8": "SMALLINT",
    "UInt16": "INTEGER",
    "UInt32": "BIGINT",
    "UInt64": "BIGINT",
    "Float32": "REAL",
    "Float64": "DOUBLE PRECISION",
    "Boolean": "BOOLEAN",
    "Utf8": "TEXT",
    "Date": "DATE",
    "Datetime": "TIMESTAMP",
    "Binary": "BYTEA",
    "Decimal": "NUMERIC",
}
