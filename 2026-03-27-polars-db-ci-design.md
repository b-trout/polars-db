# polars-db CI Design Document

## 1. Overview

### 目的

polars-db の PR チェック用 CI パイプラインを GitHub Actions で構築する。10 バックエンド（PostgreSQL, DuckDB, MySQL, SQLite, SQL Server, BigQuery, Snowflake, Redshift, Azure Synapse, Databricks）に対して、lint・型チェック・ユニットテスト・インテグレーションテストを自動実行する。

### スコープ

- PR チェック（`pull_request` イベント）のみ
- リリースパイプライン・定期実環境テストは対象外（将来の拡張として別途設計）

### 前提

- 本ドキュメントは `2026-03-27-polars-db-design.md` Section 7 (Dependencies) および Section 8 (Testing Strategy) を前提とする
- `requires-python = ">=3.10"`
- dev 依存: pytest, pytest-cov, ruff, mypy

## 2. Workflow Architecture

3ステージのパイプラインで、早期フィードバックと障害切り分けを両立する。

```
PR opened / synchronize / reopened
  │
  ├─ Stage 1: quality (lint + type check)
  │    ├─ ruff check + ruff format --check
  │    └─ mypy --strict
  │    → 失敗すれば後続スキップ（コード品質が担保されない状態でDB起動しない）
  │
  ├─ Stage 2: unit-test (needs: quality)
  │    ├─ Python 3.10
  │    └─ Python 3.13
  │    → Expr AST構築、Compiler SQL生成テスト（DB不要）
  │    → カバレッジレポート生成
  │
  └─ Stage 3: integration-test (needs: unit-test)
       Python 3.13 のみ
       matrix.backend で10バックエンド並列
       各ジョブで必要なDockerコンテナのみ起動
       fail-fast: false（1つのDB障害で他を止めない）
```

**設計判断: Python マトリクスは unit のみ**

integration テストで Python バージョン差が顕在化するケースは稀（DB ドライバの互換性は unit レベルで検出可能）。Python 3.10 / 3.13 の2バージョンマトリクスを unit に限定し、integration は 3.13 のみとすることで、ジョブ数を 20（2×10）→ 12（2+10）に削減する。

## 3. Stage 1: Quality

### ruff

```yaml
- name: Lint
  run: |
    ruff check src/ tests/
    ruff format --check src/ tests/
```

ruff の設定は `pyproject.toml` で管理:

```toml
[tool.ruff]
target-version = "py310"
line-length = 120

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "SIM", "TCH"]
ignore = ["E501"]  # line-length は formatter に任せる
```

### mypy

```yaml
- name: Type check
  run: mypy src/polars_db --strict
```

```toml
[tool.mypy]
python_version = "3.10"
strict = true
warn_return_any = true
warn_unused_configs = true

[[tool.mypy.overrides]]
module = ["connectorx.*", "snowflake.*", "databricks.*"]
ignore_missing_imports = true
```

外部ドライバ（connectorx, snowflake-connector-python, databricks-sql-connector）は型スタブが不完全なため `ignore_missing_imports` で除外。

## 4. Stage 2: Unit Test

```yaml
unit-test:
  needs: quality
  strategy:
    matrix:
      python-version: ["3.10", "3.13"]
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}
        cache: "pip"
        cache-dependency-path: "pyproject.toml"
    - name: Install dependencies
      run: pip install -e ".[dev]"
    - name: Run unit tests
      run: pytest -m unit --cov=polars_db --cov-report=xml -n auto
    - name: Upload coverage
      if: matrix.python-version == '3.13'
      uses: actions/upload-artifact@v4
      with:
        name: coverage-report
        path: coverage.xml
```

**対象テスト:**
- Expr AST が正しく構築されるか（`test_expr.py`）
- Op Tree → SQL 文字列の変換（`test_compiler.py`）
- ExprCompiler の各 Expr 型の処理（`test_expr_compiler.py`）
- `_resolve_columns()` の全 Op パターン（`test_resolve_columns.py`）
- `_structural_eq()` / `_deep_eq()` の構造比較（`test_structural_eq.py`）

**pytest-xdist**: unit テストは DB 接続不要で副作用がないため、`-n auto` で並列実行可能。

## 5. Stage 3: Integration Test

### ジョブマトリクス

```yaml
integration-test:
  needs: unit-test
  strategy:
    fail-fast: false
    matrix:
      backend:
        - postgres
        - mysql
        - sqlserver
        - duckdb
        - sqlite
        - bigquery
        - snowflake
        - redshift
        - synapse
        - databricks
  runs-on: ubuntu-latest
```

### コンテナ起動マッピング

各ジョブは `matrix.backend` に応じて必要なサービスだけを起動する。

| matrix.backend | 起動コマンド | ヘルスチェック | 備考 |
|---|---|---|---|
| postgres | `docker compose up -d --wait postgres` | `pg_isready -U test` | |
| mysql | `docker compose up -d --wait mysql` | `mysqladmin ping -h localhost -u root -ptest` | |
| sqlserver | `docker compose up -d --wait sqlserver` | `/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Test@12345' -Q 'SELECT 1' -C` | 起動に30-60秒 |
| duckdb | なし | — | インメモリ |
| sqlite | なし | — | インメモリ |
| bigquery | `docker compose up -d --wait bigquery` | `curl -sf http://localhost:9050/` | goccy/bigquery-emulator |
| snowflake | `docker compose up -d --wait snowflake` | `curl -sf http://localhost:8080/` | 非公式エミュレータ (DuckDBベース) |
| redshift | `docker compose --profile redshift up -d --wait postgres-redshift` | `pg_isready -U test -p 5433` | PostgreSQL で代替 |
| synapse | `docker compose up -d --wait synapse` | `/opt/mssql-tools18/bin/sqlcmd -S localhost,1444 -U sa -P 'Test@12345' -Q 'SELECT 1' -C` | Azure SQL Edge |
| databricks | `docker compose up -d --wait spark` | `nc -z localhost 10000` | Spark Thrift Server |

### コンテナ起動スクリプト

```python
#!/usr/bin/env python3
"""scripts/compose_up.py — docker compose サービスの起動"""
from __future__ import annotations

import subprocess
import sys


def _build_command(service: str, profile: str | None) -> tuple[str, ...]:
    base = ("docker", "compose")
    profile_args = ("--profile", profile) if profile else ()
    return (*base, *profile_args, "up", "-d", "--wait", service)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    cmd = _build_command(args.service, args.profile)
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
```

### ヘルスチェック待機スクリプト

`docker compose up -d --wait` は `healthcheck` 定義に基づき起動完了を待つ。ただしエミュレータによっては healthcheck 通過後もクエリ受付可能になるまでラグがある。安全のため、接続テストスクリプトを実行:

```python
#!/usr/bin/env python3
"""scripts/wait_for_backend.py — バックエンド接続待機スクリプト"""
from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Never


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 30
    interval_sec: float = 2.0


@dataclass(frozen=True)
class AttemptResult:
    success: bool
    attempt: int
    message: str


def try_connect(backend: str, config: dict[str, str]) -> AttemptResult | None:
    """1回の接続試行。成功なら AttemptResult、失敗なら None。"""
    import polars_db as pdb
    try:
        conn = pdb.connect(**config)
        conn.execute_raw("SELECT 1")
        conn.close()
        return AttemptResult(success=True, attempt=0, message="Connection OK")
    except Exception as e:
        return AttemptResult(success=False, attempt=0, message=str(e))


def wait_for_ready(
    backend: str,
    config: dict[str, str],
    retry: RetryConfig = RetryConfig(),
) -> AttemptResult:
    """リトライループ。副作用（sleep/print）はここに集約。"""
    results = (
        _attempt_with_delay(backend, config, attempt, retry.interval_sec)
        for attempt in range(1, retry.max_attempts + 1)
    )
    return next(
        (r for r in results if r.success),
        AttemptResult(
            success=False,
            attempt=retry.max_attempts,
            message=f"Backend {backend} failed to become ready",
        ),
    )


def _attempt_with_delay(
    backend: str, config: dict[str, str], attempt: int, interval: float
) -> AttemptResult:
    result = try_connect(backend, config)
    match result:
        case AttemptResult(success=True):
            print(f"Backend {backend} ready after {attempt} attempts")
            return AttemptResult(success=True, attempt=attempt, message=result.message)
        case AttemptResult(success=False, message=msg):
            print(f"Attempt {attempt}: {msg}")
            time.sleep(interval)
            return AttemptResult(success=False, attempt=attempt, message=msg)


def main() -> Never:
    backend = sys.argv[1]

    sys.path.insert(0, ".")
    from tests.conftest import BACKEND_CONFIG

    config = BACKEND_CONFIG[backend]
    result = wait_for_ready(backend, config)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
```

### テスト実行

```yaml
- name: Run integration tests
  env:
    POLARS_DB_TEST_BACKEND: ${{ matrix.backend }}
  run: |
    pytest -m "integration and backend_${{ matrix.backend }}" \
      --tb=short \
      --junit-xml=results-${{ matrix.backend }}.xml
```

### テストデータセットアップ

integration テストの `session`-scoped fixture でテストテーブルを作成:

```python
# tests/conftest.py
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from types import MappingProxyType

import pytest
import polars_db as pdb

# イミュータブルな設定マップ
BACKEND_CONFIG: MappingProxyType[str, MappingProxyType[str, str]] = MappingProxyType({
    "postgres":   MappingProxyType({"conn_str": "postgresql://test:test@localhost:5432/testdb"}),
    "mysql":      MappingProxyType({"conn_str": "mysql://root:test@localhost:3306/testdb"}),
    "sqlserver":  MappingProxyType({"conn_str": "mssql://sa:Test@12345@localhost:1433/testdb"}),
    "duckdb":     MappingProxyType({"conn_str": "duckdb:///:memory:"}),
    "sqlite":     MappingProxyType({"conn_str": "sqlite:///:memory:"}),
    "bigquery":   MappingProxyType({"conn_str": "bigquery://test-project/test_dataset",
                                    "endpoint": "http://localhost:9050"}),
    "snowflake":  MappingProxyType({"conn_str": "snowflake://test:test@localhost:8080/testdb"}),
    "redshift":   MappingProxyType({"conn_str": "postgresql://test:test@localhost:5433/testdb"}),
    "synapse":    MappingProxyType({"conn_str": "mssql://sa:Test@12345@localhost:1444/testdb"}),
    "databricks": MappingProxyType({"conn_str": "databricks://localhost:10000/default"}),
})


def _resolve_seed_statements(backend: str) -> tuple[str, ...]:
    """バックエンドに対応するSEED文を純粋に解決する（IO は seed_file.read_text() のみ）"""
    seed_file = Path(__file__).parent / "fixtures" / f"seed_{backend}.sql"
    if seed_file.exists():
        return (seed_file.read_text(),)
    from tests.fixtures.test_data import SEED_STATEMENTS
    return SEED_STATEMENTS.get(backend, SEED_STATEMENTS["default"])


def _execute_all(conn: pdb.Connection, statements: tuple[str, ...]) -> None:
    """文のタプルを順次実行する（副作用をここに集約）"""
    for stmt in statements:
        conn.execute_raw(stmt)


@pytest.fixture(scope="session")
def backend_name() -> str:
    return os.environ.get("POLARS_DB_TEST_BACKEND", "duckdb")


@pytest.fixture(scope="session")
def connection(backend_name: str) -> Iterator[pdb.Connection]:
    config = dict(BACKEND_CONFIG[backend_name])  # MappingProxy → dict for **展開
    conn = pdb.connect(**config)
    _execute_all(conn, _resolve_seed_statements(backend_name))
    yield conn
    conn.close()
```

### テストデータ定義

```python
# tests/fixtures/test_data.py
"""
共通テストデータ。バックエンドの SQL 方言差は
seed_{backend}.sql で吸収するが、フォールバックとして
ANSI SQL ベースの共通定義も用意する。

全データはイミュータブル（tuple / MappingProxyType）。
関数は純粋関数で、入力 → 出力のみ。
"""
from __future__ import annotations

from types import MappingProxyType

# ---------- DDL ----------

USERS_DDL: MappingProxyType[str, str] = MappingProxyType({
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
})

ORDERS_DDL: MappingProxyType[str, str] = MappingProxyType({
    "default": """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(50),
            ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
})

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
        case None:      return "NULL"
        case str():     return f"'{v}'"
        case _:         return str(v)


def _format_row(row: tuple[object, ...]) -> str:
    return ", ".join(_sql_val(v) for v in row)


def _insert_statements(table: str, rows: tuple[tuple[object, ...], ...]) -> tuple[str, ...]:
    return tuple(f"INSERT INTO {table} VALUES ({_format_row(row)})" for row in rows)


def _resolve_ddl(ddl_map: MappingProxyType[str, str], backend: str) -> str:
    return ddl_map.get(backend, ddl_map["default"])


def build_seed_statements(backend: str = "default") -> tuple[str, ...]:
    """バックエンド用の DDL + INSERT 文のタプルを生成（純粋関数）"""
    return (
        _resolve_ddl(USERS_DDL, backend),
        _resolve_ddl(ORDERS_DDL, backend),
        *_insert_statements("users", USERS_DATA),
        *_insert_statements("orders", ORDERS_DATA),
    )


# conftest.py から参照されるイミュータブル辞書
SEED_STATEMENTS: MappingProxyType[str, tuple[str, ...]] = MappingProxyType({
    "default": build_seed_statements("default"),
    "bigquery": build_seed_statements("bigquery"),
})
```

## 6. docker-compose.yml (CI向け完全版)

```yaml
# docker-compose.yml — CI テスト環境
# 使用方法: docker compose up -d --wait <service_name>

services:
  # ===== Core DB =====

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
      POSTGRES_DB: testdb
    ports: ["5432:5432"]
    tmpfs: /var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test -d testdb"]
      interval: 5s
      timeout: 3s
      retries: 10

  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: test
      MYSQL_DATABASE: testdb
    ports: ["3306:3306"]
    tmpfs: /var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-ptest"]
      interval: 5s
      timeout: 3s
      retries: 10

  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Test@12345"
    ports: ["1433:1433"]
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Test@12345' -Q 'SELECT 1' -C -b"]
      interval: 10s
      timeout: 5s
      retries: 15
      start_period: 30s

  # ===== DWH Emulators =====

  bigquery:
    image: ghcr.io/goccy/bigquery-emulator:latest
    command: --project=test-project --dataset=test_dataset
    ports: ["9050:9050"]
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9050/ || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 10

  snowflake:
    image: ghcr.io/nnnkkk7/snowflake-emulator:latest
    ports: ["8080:8080"]
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080/ || exit 1"]
      interval: 5s
      timeout: 3s
      retries: 15
      start_period: 10s

  synapse:
    image: mcr.microsoft.com/azure-sql-edge
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Test@12345"
    ports: ["1444:1433"]
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Test@12345' -Q 'SELECT 1' -C -b"]
      interval: 10s
      timeout: 5s
      retries: 15
      start_period: 30s

  spark:
    image: bitnami/spark:latest
    environment:
      SPARK_MODE: master
    ports: ["10000:10000"]
    healthcheck:
      test: ["CMD-SHELL", "nc -z localhost 10000 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 20
      start_period: 45s

  # ===== Redshift (PostgreSQL代替、profileで分離) =====

  postgres-redshift:
    image: postgres:16
    profiles: [redshift]
    environment:
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
      POSTGRES_DB: testdb
    ports: ["5433:5432"]
    tmpfs: /var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test -d testdb"]
      interval: 5s
      timeout: 3s
      retries: 10
```

### DuckDB / SQLite

コンテナ不要。テストコード内でインメモリ接続を使用。

## 7. pytest Configuration

### pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "unit: DB不要のユニットテスト",
    "integration: DB接続が必要なインテグレーションテスト",
    "backend_postgres: PostgreSQL backend",
    "backend_mysql: MySQL backend",
    "backend_sqlserver: SQL Server backend",
    "backend_duckdb: DuckDB backend",
    "backend_sqlite: SQLite backend",
    "backend_bigquery: BigQuery emulator backend",
    "backend_snowflake: Snowflake emulator backend",
    "backend_redshift: Redshift (PostgreSQL) backend",
    "backend_synapse: Azure Synapse (SQL Edge) backend",
    "backend_databricks: Databricks (Spark) backend",
    "slow: 実行に時間がかかるテスト",
]
addopts = "--strict-markers"
```

### テストファイル構成

```
tests/
├── conftest.py                      # BACKEND_CONFIG, connection fixture
├── fixtures/
│   ├── test_data.py                 # 共通テストデータ定義
│   ├── seed_postgres.sql
│   ├── seed_mysql.sql
│   ├── seed_sqlserver.sql
│   ├── seed_bigquery.sql
│   ├── seed_snowflake.sql
│   ├── seed_synapse.sql
│   └── seed_spark.sql
├── unit/
│   ├── test_expr.py                 # Expr AST構築
│   ├── test_expr_compiler.py        # ExprCompiler
│   ├── test_compiler.py             # QueryCompiler (Op → SQL)
│   ├── test_resolve_columns.py      # _resolve_columns() 全Opパターン
│   └── test_structural_eq.py        # _structural_eq() / _deep_eq()
└── integration/
    ├── test_select_filter_sort.py   # SELECT + WHERE + ORDER BY
    ├── test_join.py                 # 全JOINタイプ (inner, left, semi, anti等)
    ├── test_group_by_agg.py         # GROUP BY + 集約関数
    ├── test_with_columns.py         # with_columns (同名上書き含む)
    ├── test_rename_drop.py          # rename / drop
    ├── test_window.py               # ウィンドウ関数
    ├── test_null_semantics.py       # NULL + GROUP BY/JOIN/ソート/DISTINCT
    ├── test_type_precision.py       # NUMERIC精度, BIGINT上限
    └── test_explain.py              # explain_query()
```

### インテグレーションテストのマーカー付与パターン

```python
# tests/integration/test_select_filter_sort.py

import pytest

# 全バックエンド共通テスト — 個別マーカー不要、connection fixture で制御
@pytest.mark.integration
class TestSelectFilterSort:
    def test_simple_filter(self, connection):
        result = connection.table("users").filter(
            pdb.col("age") > 30
        ).collect()
        expected = pl.DataFrame({"id": [3], "name": ["Charlie"], ...})
        assert result.frame_equal(expected)

# 特定バックエンドでのみ実行するテスト
@pytest.mark.integration
@pytest.mark.backend_postgres
@pytest.mark.backend_duckdb
def test_explode_unnest(connection):
    """UNNEST は PostgreSQL/DuckDB のみ対応"""
    ...
```

マーカーの組み合わせ:
- `@pytest.mark.integration` — 全バックエンドで実行
- `@pytest.mark.integration` + `@pytest.mark.backend_X` — 特定バックエンドのみ

CI 側の `pytest -m "integration and backend_postgres"` は:
1. `integration` マーカーがついている **かつ**
2. `backend_postgres` がついている、**または** バックエンド固有マーカーがついていない

を実行する。これを実現するため、conftest.py で自動マーカー付与:

```python
# conftest.py

def _parse_backend_names(raw_markers: list[str]) -> frozenset[str]:
    """pytest marker 文字列から backend_ 名を抽出（純粋関数）"""
    return frozenset(
        m.split(":")[0].strip()
        for m in raw_markers
        if m.startswith("backend_")
    )


def _is_integration(item: pytest.Item) -> bool:
    return "integration" in frozenset(m.name for m in item.iter_markers())


def _has_backend_marker(item: pytest.Item, all_backends: frozenset[str]) -> bool:
    return any(m.name in all_backends for m in item.iter_markers())


def _missing_markers(item: pytest.Item, all_backends: frozenset[str]) -> frozenset[str]:
    """item に付与すべき backend マーカーの集合を返す（純粋関数）"""
    if not _is_integration(item):
        return frozenset()
    if _has_backend_marker(item, all_backends):
        return frozenset()
    return all_backends


def pytest_collection_modifyitems(items: list[pytest.Item], config: pytest.Config) -> None:
    """バックエンド固有マーカーがないintegrationテストに全backendマーカーを付与。
    副作用（add_marker）は pytest API の要求。ロジックは純粋関数に分離。"""
    all_backends = _parse_backend_names(config.getini("markers"))
    for item in items:
        for backend in _missing_markers(item, all_backends):
            item.add_marker(getattr(pytest.mark, backend))
```

## 8. Cache & Performance

### pip キャッシュ

```yaml
- uses: actions/setup-python@v5
  with:
    python-version: ${{ matrix.python-version }}
    cache: "pip"
    cache-dependency-path: "pyproject.toml"
```

`actions/setup-python` の組み込みキャッシュで `~/.cache/pip` を `pyproject.toml` のハッシュをキーにキャッシュ。

### Docker イメージキャッシュ

GitHub Actions ランナーには Docker イメージのキャッシュが効かない（エフェメラルランナー）。対策:

1. **GitHub Container Registry (ghcr.io) からの pull** — GitHub のネットワーク内なので高速（1-3秒/イメージ）
2. **Docker Hub 以外のイメージは事前 pull しない** — `docker compose up` が自動で pull する
3. **`docker compose pull` のバックグラウンド実行** — pip install と並列化

```yaml
- name: Pull Docker image & install deps
  run: python scripts/parallel_setup.py --service ${{ env.COMPOSE_SERVICE }} --extras "${{ env.EXTRAS }}"
```

```python
#!/usr/bin/env python3
"""scripts/parallel_setup.py — Docker pull と pip install を並列実行"""
from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass


@dataclass(frozen=True)
class SetupTask:
    name: str
    cmd: tuple[str, ...]


def _build_tasks(service: str, extras: str) -> tuple[SetupTask, ...]:
    pip_target = f".[dev,{extras}]" if extras else ".[dev]"
    return (
        SetupTask("docker-pull", ("docker", "compose", "pull", service)),
        SetupTask("pip-install", ("pip", "install", "-e", pip_target)),
    )


def _run_task(task: SetupTask) -> tuple[str, int]:
    result = subprocess.run(task.cmd, capture_output=True, text=True)
    return (task.name, result.returncode)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--extras", default="")
    args = parser.parse_args()

    tasks = _build_tasks(args.service, args.extras)
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        results = tuple(executor.map(_run_task, tasks))

    failures = tuple(name for name, code in results if code != 0)
    if failures:
        print(f"Failed: {', '.join(failures)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

### tmpfs

PostgreSQL と MySQL のデータディレクトリを tmpfs にマウント。CI 環境ではデータ永続化が不要なため、ディスクIOを排除して高速化。

### pytest-xdist

- **unit テスト**: `-n auto` で CPU コア数分の並列実行（GitHub Actions ランナーは 2 コア）
- **integration テスト**: 使用しない。DB 接続テストは共有状態（テストテーブル）への書き込みがあるため、テスト間の順序依存を排除しきれない

### 推定 CI 時間

| ステージ | ジョブ数 | 推定時間 | ボトルネック |
|---|---|---|---|
| quality | 1 | 1-2分 | mypy の型解析 |
| unit-test | 2 (Python 3.10, 3.13) | 2-3分 | テスト実行自体は30秒程度、大半はセットアップ |
| integration-test | 10 (backend別) | 5-15分 | Spark Thrift Server の起動 (45秒) + SQL Server の起動 (30秒) |
| **合計（クリティカルパス）** | — | **8-20分** | |

全ステージが順次実行だが、各ステージ内のジョブは並列。クリティカルパスは quality → unit(3.13) → integration(spark) の直列部分。

## 9. GitHub Actions Workflow (完全版)

```yaml
# .github/workflows/pr-check.yml
name: PR Check

on:
  pull_request:
    branches: [main]
    types: [opened, synchronize, reopened]

concurrency:
  group: ${{ github.workflow }}-${{ github.head_ref || github.ref }}
  cancel-in-progress: true

jobs:
  # ===== Stage 1: Quality =====
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
          cache: "pip"
          cache-dependency-path: "pyproject.toml"
      - name: Install dependencies
        run: pip install -e ".[dev]"
      - name: Lint
        run: |
          ruff check src/ tests/
          ruff format --check src/ tests/
      - name: Type check
        run: mypy src/polars_db --strict

  # ===== Stage 2: Unit Test =====
  unit-test:
    needs: quality
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.13"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: "pip"
          cache-dependency-path: "pyproject.toml"
      - name: Install dependencies
        run: pip install -e ".[dev]"
      - name: Run unit tests
        run: pytest -m unit --cov=polars_db --cov-report=xml -n auto
      - name: Upload coverage
        if: matrix.python-version == '3.13'
        uses: actions/upload-artifact@v4
        with:
          name: coverage-report
          path: coverage.xml

  # ===== Stage 3: Integration Test =====
  integration-test:
    needs: unit-test
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        backend:
          - postgres
          - mysql
          - sqlserver
          - duckdb
          - sqlite
          - bigquery
          - snowflake
          - redshift
          - synapse
          - databricks
        include:
          # コンテナが必要なバックエンドのサービス名と追加依存
          - backend: postgres
            compose_service: postgres
            extras: ""
          - backend: mysql
            compose_service: mysql
            extras: ""
          - backend: sqlserver
            compose_service: sqlserver
            extras: ""
          - backend: duckdb
            compose_service: ""
            extras: ""
          - backend: sqlite
            compose_service: ""
            extras: ""
          - backend: bigquery
            compose_service: bigquery
            extras: "bigquery"
          - backend: snowflake
            compose_service: snowflake
            extras: "snowflake"
          - backend: redshift
            compose_service: "postgres-redshift"
            compose_profiles: "redshift"
            extras: ""
          - backend: synapse
            compose_service: synapse
            extras: ""
          - backend: databricks
            compose_service: spark
            extras: "databricks"
    env:
      POLARS_DB_TEST_BACKEND: ${{ matrix.backend }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
          cache: "pip"
          cache-dependency-path: "pyproject.toml"

      - name: Start backend service
        if: matrix.compose_service != ''
        run: >-
          python scripts/compose_up.py
          --service ${{ matrix.compose_service }}
          ${{ matrix.compose_profiles && format('--profile {0}', matrix.compose_profiles) }}

      - name: Install dependencies
        run: >-
          pip install -e ".[dev${{ matrix.extras && format(',{0}', matrix.extras) }}]"

      - name: Wait for backend readiness
        if: matrix.compose_service != ''
        run: python scripts/wait_for_backend.py ${{ matrix.backend }}

      - name: Run integration tests
        run: >-
          pytest -m "integration and backend_${{ matrix.backend }}"
          --tb=short
          --junit-xml=results-${{ matrix.backend }}.xml

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: test-results-${{ matrix.backend }}
          path: results-${{ matrix.backend }}.xml

      - name: Stop backend service
        if: always() && matrix.compose_service != ''
        run: docker compose down -v
```

## 10. エミュレータの既知の制限

CI で使用するエミュレータは本番環境と挙動が異なる場合がある。既知の差異を記録し、テストの期待値を調整する。

| バックエンド | エミュレータ | 忠実度 | 既知の差異 |
|---|---|---|---|
| BigQuery | `goccy/bigquery-emulator` | 中 | `STRUCT`/`ARRAY`型の一部操作、`SAFE_DIVIDE`等の関数が未対応の場合あり |
| Snowflake | `snowflake-emulator` | 低〜中 | DuckDBベース。`VARIANT`型、`QUALIFY`句、セミ構造化データは未対応 |
| Redshift | PostgreSQL代替 | 中 | `DISTKEY`/`SORTKEY`の影響、`SUPER`型、一部ウィンドウ関数の制限は再現不可 |
| Databricks | Spark Thrift Server | 中 | Delta Lake固有機能、Unity Catalog、`PIVOT`構文の差異 |
| Synapse | Azure SQL Edge | 低〜中 | DISTRIBUTION/MPP挙動は再現不可。SQL構文レベルではSQL Serverとほぼ同じ |

エミュレータで検出できない差異は、将来の実環境テスト（scheduled workflow）で補完する。エミュレータと実環境の差異が判明した場合は `tests/known_divergences.yaml` に記録:

```yaml
# tests/known_divergences.yaml
bigquery:
  - test: test_safe_divide
    reason: "bigquery-emulator does not support SAFE_DIVIDE"
    action: skip  # pytest.mark.skipで除外
snowflake:
  - test: test_variant_type
    reason: "snowflake-emulator does not support VARIANT"
    action: skip
```
