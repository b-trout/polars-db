# polars-db

A Python library that lets you query relational databases using Polars syntax.
Write expressions like `pdb.col("age") > 30` and polars-db translates them into
the appropriate SQL dialect for your backend.

## Installation

```bash
pip install polars-db
```

DuckDB and SQLite work out of the box. For other databases, install the
corresponding extras:

```bash
# PostgreSQL
pip install polars-db[postgres]

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
| SQLite | *(none)* | sqlite3 (stdlib) | `sqlite:///path/to/db.sqlite` |
| PostgreSQL | `postgres` | psycopg2 | `postgresql://user:pass@host:5432/dbname` |
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
