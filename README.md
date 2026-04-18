# polars-db

[![CI](https://github.com/b-trout/polars-db/actions/workflows/pr-check.yml/badge.svg)](https://github.com/b-trout/polars-db/actions/workflows/pr-check.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![codecov](https://codecov.io/gh/b-trout/polars-db/branch/main/graph/badge.svg)](https://codecov.io/gh/b-trout/polars-db)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A Python library that lets you query relational databases using Polars syntax.
Write expressions like `pdb.col("age") > 30` and polars-db translates them into
the appropriate SQL dialect for your backend.

## Motivation

In Python, there is no standard way to manipulate database tables using
DataFrame syntax. You either write raw SQL strings or learn a library-specific
API like SQLAlchemy or Ibis — none of which feel like the DataFrame code you
already write. R solved this years ago with
[dbplyr](https://dbplyr.tidyverse.org/), which lets dplyr code run transparently
against databases. polars-db brings the same idea to the Polars ecosystem.

| Library | API style | Target audience |
|---|---|---|
| [SQLAlchemy](https://www.sqlalchemy.org/) | SQLAlchemy's own API | General Python → SQL |
| [Ibis](https://ibis-project.org/) | Ibis's own API | General DataFrame → SQL |
| [SQLFrame](https://github.com/eakmanrq/sqlframe) | PySpark API | PySpark users → SQL |
| **polars-db** | **Polars API** | **Polars users → SQL** |

**Zero learning cost** — If you already know Polars, you can query databases
immediately. No new API to learn.

**Type-safe unified pipelines** — Local processing (Polars) and database queries
(polars-db) share the same API, eliminating the implicit type coercion, NaN/None
confusion, and dtype mismatches that plague pandas-based workflows.

## Installation

```bash
pip install polars-db
```

DuckDB works out of the box. For other databases, install the corresponding
extras:

```bash
# PostgreSQL
pip install polars-db[postgres]

# SQLite
pip install polars-db[sqlite]

# MySQL
pip install polars-db[mysql]

# SQL Server
pip install polars-db[sqlserver]

# BigQuery
pip install polars-db[bigquery]

# Multiple backends at once
pip install polars-db[postgres,mysql]
```

## Supported Databases

| Database | Extras | Driver | Connection string example |
|---|---|---|---|
| DuckDB | *(none)* | duckdb | `duckdb:///:memory:` |
| SQLite | `sqlite` | adbc-driver-sqlite | `sqlite:///path/to/db.sqlite` |
| PostgreSQL | `postgres` | adbc-driver-postgresql | `postgresql://user:pass@host:5432/dbname` |
| MySQL | `mysql` | PyMySQL | `mysql://user:pass@host:3306/dbname` |
| SQL Server | `sqlserver` | pymssql | `mssql://user:pass@host:1433/dbname` |
| BigQuery | `bigquery` | google-cloud-bigquery | `bigquery://project/dataset` |

## Usage

### Connecting

```python
import polars_db as pdb

conn = pdb.connect("postgresql://user:pass@localhost:5432/mydb")
```

### SELECT Queries

Build queries with the same API as Polars `LazyFrame`, then call `collect()` to
execute.

```python
# Basic select / filter / sort / limit
df = (
    conn.table("users")
    .filter(pdb.col("age") > 30)
    .select("name", "age")
    .sort("age", descending=True)
    .limit(10)
    .collect()
)
```

```python
# GROUP BY with aggregation
df = (
    conn.table("sales")
    .group_by("product_id")
    .agg(
        pdb.col("amount").sum().alias("total"),
        pdb.col("id").count().alias("num_sales"),
    )
    .sort("total", descending=True)
    .collect()
)
```

```python
# JOIN
users = conn.table("users")
orders = conn.table("orders")

df = (
    users.join(orders, on="user_id", how="left")
    .select("name", "amount")
    .collect()
)
```

```python
# Window functions
df = (
    conn.table("sales")
    .with_columns(
        pdb.col("amount").sum().over("dept").alias("dept_total"),
    )
    .collect()
)
```

```python
# Cumulative sum with ordering
df = (
    conn.table("sales")
    .with_columns(
        pdb.col("amount")
        .cum_sum()
        .over("dept", order_by="date")
        .alias("running_total"),
    )
    .collect()
)
```

```python
# CASE WHEN
df = (
    conn.table("users")
    .with_columns(
        pdb.when(pdb.col("age") >= 18)
        .then(pdb.lit("adult"))
        .otherwise(pdb.lit("minor"))
        .alias("category"),
    )
    .collect()
)
```

#### Inspecting Generated SQL

```python
query = conn.table("users").filter(pdb.col("age") > 30)
print(query.show_query())
# SELECT * FROM users WHERE age > 30
```

### DDL / Raw SQL

The query builder focuses on SELECT statements. For DDL (`CREATE TABLE`, etc.)
and DML (`INSERT`, etc.), use `execute_raw()`:

```python
conn.execute_raw("CREATE TABLE users (id INT, name TEXT, age INT)")
conn.execute_raw("INSERT INTO users VALUES (1, 'Alice', 30)")
conn.execute_raw("DROP TABLE IF EXISTS users")
```

> **Note:** `execute_raw()` executes SQL as-is. Never pass unsanitized external
> input via string concatenation.

## Development

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- Docker (for integration tests)

### Setup

```bash
git clone https://github.com/b-trout/polars-db.git
cd polars-db
uv sync --all-groups --all-extras
```

### Common Commands

This project uses [Poe the Poet](https://poethepoet.nabertech.io/) as a task
runner.

```bash
# Lint & format
uv run poe lint          # ruff check
uv run poe format        # ruff format
uv run poe type-check    # ty check

# Unit tests (no database required)
uv run poe test-unit

# Integration tests (per backend)
docker compose up -d
POLARS_DB_TEST_BACKEND=postgres uv run poe test-integration

# Run all pre-commit checks (format -> lint -> type-check -> yaml -> docker-lint)
uv run poe pre-commit
```

## License

This project is licensed under the [MIT License](LICENSE).
