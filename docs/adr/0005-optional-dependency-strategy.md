# ADR-0005: Optional Dependency Strategy

## Status

Accepted (extends initial design)

## Context

The original design specified BigQuery as the only optional dependency. After migrating all backends to native drivers (ADR-0004), every database driver became a direct dependency, making `pip install polars-db` pull in psycopg2, pymysql, pymssql, and google-cloud-bigquery — even for users who only need DuckDB or SQLite.

Additionally, some drivers include C extensions (`psycopg2-binary`, `pymssql`) that can fail to build on certain platforms.

## Decision

Make all database drivers optional except DuckDB and SQLite (stdlib):

```toml
[project]
dependencies = [
    "polars>=1.39.3",
    "sqlglot>=30.4.3",
    "pyarrow>=23.0.1",
    "duckdb>=1.5.2",
]

[project.optional-dependencies]
postgres = ["psycopg2-binary>=2.9.0"]
mysql = ["pymysql>=1.1.0", "cryptography>=44.0.0"]
sqlserver = ["pymssql>=2.3.0"]
bigquery = ["google-cloud-bigquery>=3.0.0"]
```

Usage: `pip install polars-db[postgres,mysql]`

## Consequences

- Minimal install (`pip install polars-db`) provides DuckDB + SQLite — sufficient for local development and prototyping.
- Users add only the drivers they need, avoiding unnecessary build failures.
- Backend code uses lazy imports (`import psycopg2` inside `_create_connection()`) so missing optional packages produce clear `ImportError` messages at connection time, not at import time.
- CI/test environments use `uv sync --all-extras` to install everything.
