"""Shared test data definitions.

All data is immutable (tuples / MappingProxyType).
Functions are pure: input -> output only.
"""

from __future__ import annotations

from types import MappingProxyType

# ---------- DDL ----------

USERS_DDL: MappingProxyType[str, str] = MappingProxyType(
    {
        "default": """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER,
            email VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
        "bigquery": """
        CREATE TABLE IF NOT EXISTS users (
            id INT64 NOT NULL,
            name STRING NOT NULL,
            age INT64,
            email STRING,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
        "sqlserver": """
        IF OBJECT_ID('users', 'U') IS NULL
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER,
            email VARCHAR(200),
            created_at DATETIME DEFAULT GETDATE()
        )
    """,
    }
)

ORDERS_DDL: MappingProxyType[str, str] = MappingProxyType(
    {
        "default": """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(50),
            ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
        "bigquery": """
        CREATE TABLE IF NOT EXISTS orders (
            id INT64 NOT NULL,
            user_id INT64 NOT NULL,
            amount FLOAT64 NOT NULL,
            status STRING,
            ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """,
        "sqlserver": """
        IF OBJECT_ID('orders', 'U') IS NULL
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(50),
            ordered_at DATETIME DEFAULT GETDATE()
        )
    """,
    }
)

# ---------- Data (immutable tuples) ----------

USERS_DATA: tuple[tuple[object, ...], ...] = (
    (1, "Alice", 30, "alice@example.com"),
    (2, "Bob", 25, "bob@example.com"),
    (3, "Charlie", 35, None),
    (4, "Diana", None, "diana@example.com"),
    (5, "Eve", 28, "eve@example.com"),
)

ORDERS_DATA: tuple[tuple[object, ...], ...] = (
    (1, 1, 100.50, "completed"),
    (2, 1, 200.00, "completed"),
    (3, 2, 50.75, "pending"),
    (4, 3, 300.00, "completed"),
    (5, 3, 150.25, "cancelled"),
    (6, 5, 75.00, "pending"),
)


# ---------- Pure functions ----------


def _sql_val(v: object) -> str:
    match v:
        case None:
            return "NULL"
        case str():
            return f"'{v}'"
        case _:
            return str(v)


def _format_row(row: tuple[object, ...]) -> str:
    return ", ".join(_sql_val(v) for v in row)


def _insert_statements(
    table: str,
    columns: tuple[str, ...],
    rows: tuple[tuple[object, ...], ...],
) -> tuple[str, ...]:
    cols = ", ".join(columns)
    return tuple(
        f"INSERT INTO {table} ({cols}) VALUES ({_format_row(row)})" for row in rows
    )


def _resolve_ddl(ddl_map: MappingProxyType[str, str], backend: str) -> str:
    return ddl_map.get(backend, ddl_map["default"])


def build_seed_statements(backend: str = "default") -> tuple[str, ...]:
    """Generate DDL + INSERT statements for a backend (pure function)."""
    return (
        _resolve_ddl(USERS_DDL, backend),
        _resolve_ddl(ORDERS_DDL, backend),
        *_insert_statements("users", ("id", "name", "age", "email"), USERS_DATA),
        *_insert_statements(
            "orders", ("id", "user_id", "amount", "status"), ORDERS_DATA
        ),
    )


SEED_STATEMENTS: MappingProxyType[str, tuple[str, ...]] = MappingProxyType(
    {
        "default": build_seed_statements("default"),
        "bigquery": build_seed_statements("bigquery"),
        "sqlserver": build_seed_statements("sqlserver"),
    }
)
