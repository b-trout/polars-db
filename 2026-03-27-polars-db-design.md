# polars-db: dbplyr for Python/Polars — Design Document

## 1. Overview

### Problem

Polarsユーザーがデータベースのデータを扱う際、SQLを直接書くかIbis等の別APIを学ぶ必要がある。R言語ではdbplyrがdplyr構文のままDBを透過的に操作できるが、Python/Polarsには同等のものが存在しない。

### Solution

Polars LazyFrameのAPIを忠実に再現するラッパーライブラリを構築し、裏側でSQLを生成・実行して結果をpolars.DataFrameで返す。

### Differentiation

| ライブラリ | API | 対象ユーザー |
|---|---|---|
| **Ibis** | Ibis独自API | 汎用DataFrame→SQL |
| **SQLFrame** | PySpark API | PySpark→SQL |
| **polars-db (本ライブラリ)** | **Polars API互換** | **Polarsユーザー → SQL** |

### Value Proposition

> Polars users can query databases using the exact same API — no ibis, no pandas, no type conversion surprises.

**Layer 1: 学習コストゼロ** — 既存PolarsユーザーがPolarsの知識だけでDBを操作できる。新しいAPIの学習が不要。

**Layer 2: 型安全なパイプラインの統一** — ローカル処理（Polars）とDB処理（polars-db）を同一APIで統一することで、pandasで頻発する暗黙の型変換・NaN/None混在・dtype不整合を排除できる。pandasの型トラップに悩み、型安全なパイプラインを構築したいデータエンジニアにとって、「DB操作もPolarsで統一できる」はPolars移行の決定打になる。

## 2. User-Facing API

```python
import polars_db as pdb

# DB接続
con = pdb.connect("postgresql://user:pass@host/db")

# テーブル参照（LazyFrame風オブジェクトを返す）
lf = con.table("users")

# Polarsと同じ構文でクエリを構築
result = (
    lf.filter(pdb.col("age") > 30)
      .join(con.table("orders"), on="user_id", how="left")
      .group_by("name")
      .agg(pdb.col("amount").sum().alias("total"))
      .sort("total", descending=True)
      .limit(10)
)

# 生成されたSQLを確認
print(result.show_query())
# SELECT "name", SUM("amount") AS "total"
# FROM "users"
# LEFT JOIN "orders" ON "users"."user_id" = "orders"."user_id"
# WHERE "age" > 30
# GROUP BY "name"
# ORDER BY "total" DESC
# LIMIT 10

# 実行してpolars.DataFrameを取得
df = result.collect()

# ウィンドウ関数
lf2 = (
    con.table("sales")
    .with_columns(
        pdb.col("amount").sum().over("department").alias("dept_total"),
        pdb.col("amount").rank().over("department").alias("dept_rank"),
    )
)
```

## 3. Architecture

### Design Pattern: Operation Tree + Visitor (dbplyr式)

dbplyrで実証済みのアーキテクチャを採用する。各Polars操作がツリーのノードとなり、collect()時にVisitorパターンでSQLに変換する。

### Data Flow

```
User Code (Polars風API)
    |
    v
LazyFrame メソッド -> 新しいOpノードを生成（イミュータブルチェーン）
    |
    v
Operation Tree（ネストしたOpノード群）
    |
    v
collect() / show_query() がトリガー
    |
    v
QueryCompiler (Visitor) がツリーを再帰走査 -> SQLGlot AST
    |
    v
Optimizer が不要サブクエリ除去等
    |
    v
Backend.render(ast) -> SQL文字列（方言別）
    |
    v
connectorx.read_sql() -> Arrow Table
    |
    v
polars.from_arrow() -> polars.DataFrame
```

### Operation Tree Example

```python
# ユーザーコード
lf = con.table("users")
result = lf.filter(col("age") > 30).select(col("name"), col("age")).sort("age")

# 内部のツリー構造（最新の操作が外側）
SortOp(by=["age"])
  +-- SelectOp(exprs=[col("name"), col("age")])
        +-- FilterOp(predicate=col("age") > 30)
              +-- TableRef("users")
```

Compilerは内側（TableRef）から外側（SortOp）へ再帰的に走査し、SQLGlotのSELECT文を組み立てる。

## 4. Module Structure

```
src/polars_db/
|-- __init__.py              # 公開API: connect(), col(), lit(), when()
|-- connection.py            # Connection（DB接続管理、SQL実行）
|-- lazy_frame.py            # LazyFrame（Polars互換API、ツリー構築）
|-- expr.py                  # Expr（式AST: col, lit, 比較, 算術, 集約, ウィンドウ）
|-- ops/                     # オペレーションツリーのノード
|   |-- base.py              # Op基底クラス
|   |-- table.py             # TableRef（ルートノード）
|   |-- select.py            # SelectOp, WithColumnsOp
|   |-- filter.py            # FilterOp
|   |-- group_by.py          # GroupByOp
|   |-- join.py              # JoinOp
|   |-- sort.py              # SortOp
|   |-- limit.py             # LimitOp
|   +-- distinct.py          # DistinctOp
|-- compiler/
|   |-- expr_compiler.py     # Expr -> SQLGlot Expression
|   |-- query_compiler.py    # Op Tree -> SQLGlot Select
|   +-- optimizer.py         # 不要サブクエリ除去等
|-- backends/
|   |-- base.py              # Backend ABC（方言設定 + 型マッピング）
|   |-- postgres.py          # PostgreSQL固有設定
|   |-- duckdb.py            # DuckDB固有設定
|   +-- mysql.py             # MySQL固有設定
+-- types.py                 # Polars dtype <-> SQL型マッピング
```

## 5. Core Components

### 5.1 Expr (Expression AST)

frozen dataclassで不変な式ツリーを構築する。Pythonの演算子オーバーロードでPolars風の記述を実現。

**設計判断: `eq=False`**
Polars API互換のため `__eq__` は BinaryExpr を返す必要がある（`pdb.col("age") == 30`）。
`frozen=True` が自動生成する `__eq__`（bool返却）と衝突するため `eq=False` を指定し、
`__eq__` は手動でオーバーライドする。`__hash__` も生成されないため Expr は unhashable になる
（Polars本体と同じ設計）。内部・テスト用の構造比較には `_structural_eq()` を使用する。

```python
@dataclass(frozen=True, eq=False)
class Expr:
    """式の基底クラス"""

    def _structural_eq(self, other: "Expr") -> bool:
        """内部・テスト用の構造比較。フィールド値の再帰的一致を検証する。
        list[Expr], tuple[Expr, ...] 等のコレクション内の Expr も再帰的に比較する。"""
        if type(self) is not type(other):
            return False
        from dataclasses import fields
        return all(
            _deep_eq(getattr(self, f.name), getattr(other, f.name))
            for f in fields(self)
        )


def _deep_eq(a: object, b: object) -> bool:
    """Expr を含む任意のフィールド値を再帰的に構造比較する。
    Expr.__eq__ が BinaryExpr を返すため、通常の == は使えない。"""
    if isinstance(a, Expr) and isinstance(b, Expr):
        return a._structural_eq(b)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return type(a) is type(b) and len(a) == len(b) and all(
            _deep_eq(x, y) for x, y in zip(a, b)
        )
    return a == b  # str, int, bool, None 等のプリミティブ値

# --- 基本式 ---

@dataclass(frozen=True, eq=False)
class ColExpr(Expr):
    """カラム参照: col("age")"""
    name: str

@dataclass(frozen=True, eq=False)
class LitExpr(Expr):
    """リテラル値: lit(30)"""
    value: Any

@dataclass(frozen=True, eq=False)
class BinaryExpr(Expr):
    """二項演算: col("age") > 30"""
    op: str                          # ">", "<", ">=", "<=", "==", "!=",
                                     # "+", "-", "*", "/", "//", "%",
                                     # "and", "or"
    left: Expr
    right: Expr

@dataclass(frozen=True, eq=False)
class UnaryExpr(Expr):
    """単項演算: ~col("active"), -col("amount")"""
    op: str                          # "not", "neg"
    operand: Expr

# --- 集約 ---

@dataclass(frozen=True, eq=False)
class AggExpr(Expr):
    """集約関数: col("x").sum()"""
    func: str                        # "sum", "mean", "count", "min", "max",
                                     # "std", "var", "first", "last"
    arg: Expr

# --- ウィンドウ ---

@dataclass(frozen=True, eq=False)
class WindowExpr(Expr):
    """ウィンドウ関数: col("x").sum().over("dept")"""
    expr: Expr
    partition_by: tuple[Expr, ...]
    order_by: tuple[Expr, ...] | None = None

# --- エイリアス ---

@dataclass(frozen=True, eq=False)
class AliasExpr(Expr):
    """エイリアス: expr.alias("new_name")"""
    expr: Expr
    alias: str

# --- 条件分岐 ---

@dataclass(frozen=True, eq=False)
class CaseExpr(Expr):
    """CASE WHEN: when(cond).then(val).otherwise(default)"""
    cases: tuple[tuple[Expr, Expr], ...]   # ((condition, result), ...)
    otherwise: Expr | None = None

# --- 汎用関数 ---

@dataclass(frozen=True, eq=False)
class FuncExpr(Expr):
    """関数呼び出し: cast(), coalesce(), str/dt操作"""
    func_name: str
    args: tuple[Expr, ...]

# --- ソート指定 ---

@dataclass(frozen=True, eq=False)
class SortExpr(Expr):
    """ソート式: col("x").sort(descending=True)"""
    expr: Expr
    descending: bool = False
```

#### Expr のユーザー向けメソッド

```python
class Expr:
    # 演算子オーバーロード
    def __gt__(self, other) -> BinaryExpr: ...
    def __lt__(self, other) -> BinaryExpr: ...
    def __ge__(self, other) -> BinaryExpr: ...
    def __le__(self, other) -> BinaryExpr: ...
    def __eq__(self, other) -> BinaryExpr: ...
    def __ne__(self, other) -> BinaryExpr: ...
    def __add__(self, other) -> BinaryExpr: ...
    def __sub__(self, other) -> BinaryExpr: ...
    def __mul__(self, other) -> BinaryExpr: ...
    def __truediv__(self, other) -> BinaryExpr: ...
    def __mod__(self, other) -> BinaryExpr: ...
    def __and__(self, other) -> BinaryExpr: ...
    def __or__(self, other) -> BinaryExpr: ...
    def __invert__(self) -> UnaryExpr: ...
    def __neg__(self) -> UnaryExpr: ...

    # 集約メソッド
    def sum(self) -> AggExpr: ...
    def mean(self) -> AggExpr: ...
    def count(self) -> AggExpr: ...
    def min(self) -> AggExpr: ...
    def max(self) -> AggExpr: ...
    def std(self) -> AggExpr: ...
    def var(self) -> AggExpr: ...
    def first(self) -> AggExpr: ...
    def last(self) -> AggExpr: ...

    # ウィンドウ
    def over(self, *partition_by: str | Expr) -> WindowExpr: ...
    def shift(self, n: int = 1) -> FuncExpr: ...   # LAG/LEAD
    def rank(self) -> FuncExpr: ...
    def row_number(self) -> FuncExpr: ...

    # 変換
    def alias(self, name: str) -> AliasExpr: ...
    def cast(self, dtype) -> FuncExpr: ...

    # NULL処理
    def is_null(self) -> FuncExpr: ...
    def is_not_null(self) -> FuncExpr: ...
    def fill_null(self, value) -> FuncExpr: ...     # COALESCE

    # 比較
    def is_between(self, lower, upper) -> FuncExpr: ...  # BETWEEN
    def is_in(self, values: list) -> FuncExpr: ...       # IN

    # 名前空間
    @property
    def str(self) -> StringNamespace: ...   # .str.to_lowercase() 等
    @property
    def dt(self) -> DateTimeNamespace: ...  # .dt.year() 等
```

#### StringNamespace / DateTimeNamespace

```python
class StringNamespace:
    """col("name").str.to_lowercase() -> FuncExpr("lower", ...)"""
    def to_lowercase(self) -> FuncExpr: ...
    def to_uppercase(self) -> FuncExpr: ...
    def contains(self, pattern: str) -> FuncExpr: ...  # LIKE '%pattern%'
    def starts_with(self, prefix: str) -> FuncExpr: ...
    def ends_with(self, suffix: str) -> FuncExpr: ...
    def len_chars(self) -> FuncExpr: ...               # LENGTH / CHAR_LENGTH
    def slice(self, offset: int, length: int) -> FuncExpr: ...  # SUBSTRING
    def replace(self, old: str, new: str) -> FuncExpr: ...

class DateTimeNamespace:
    """col("date").dt.year() -> FuncExpr("extract_year", ...)"""
    def year(self) -> FuncExpr: ...
    def month(self) -> FuncExpr: ...
    def day(self) -> FuncExpr: ...
    def hour(self) -> FuncExpr: ...
    def minute(self) -> FuncExpr: ...
    def second(self) -> FuncExpr: ...
    def date(self) -> FuncExpr: ...       # DATE(timestamp)
    def truncate(self, every: str) -> FuncExpr: ...  # DATE_TRUNC
```

### 5.2 Op (Operation Tree Nodes)

**設計判断: `eq=False`**
Op ノードは Expr フィールドを含む。Expr は `eq=False`（unhashable）であり、
`__eq__` が BinaryExpr を返すため、Op の auto-generated `__eq__` と `__hash__` は
いずれも正しく動作しない。Expr と同様に `eq=False` を指定し、
構造比較には `_deep_eq()` を使う `_structural_eq()` を提供する。

```python
@dataclass(frozen=True, eq=False)
class Op:
    """オペレーションの基底クラス"""

    def _structural_eq(self, other: "Op") -> bool:
        """内部・テスト用の構造比較。Expr フィールドも再帰的に比較する。"""
        if type(self) is not type(other):
            return False
        from dataclasses import fields
        return all(
            _deep_eq(getattr(self, f.name), getattr(other, f.name))
            for f in fields(self)
        )

@dataclass(frozen=True, eq=False)
class TableRef(Op):
    """テーブル参照（ツリーのルート）"""
    name: str
    schema: str | None = None

@dataclass(frozen=True, eq=False)
class FilterOp(Op):
    """WHERE句: lf.filter(col("age") > 30)"""
    child: Op
    predicate: Expr

@dataclass(frozen=True, eq=False)
class SelectOp(Op):
    """SELECT句: lf.select(col("name"), col("age"))"""
    child: Op
    exprs: tuple[Expr, ...]

@dataclass(frozen=True, eq=False)
class WithColumnsOp(Op):
    """既存カラムを保持しつつ新カラムを追加/上書きする。
    同名エイリアスがある場合はSELECT *ではなく全カラム列挙で上書きを実現する。

    例: users テーブル (id, name, price, category)
      lf.with_columns(
          pdb.col("price") * 1.1).alias("price"),   # 同名 → 上書き
          pdb.lit("JP").alias("region"),              # 新規 → 追加
      )
    生成SQL:
      SELECT id, name, price * 1.1 AS price, category, 'JP' AS region FROM users
    """
    child: Op
    exprs: tuple[Expr, ...]

@dataclass(frozen=True, eq=False)
class GroupByOp(Op):
    """GROUP BY + 集約: lf.group_by("x").agg(col("y").sum())"""
    child: Op
    by: tuple[Expr, ...]
    agg: tuple[Expr, ...]

@dataclass(frozen=True, eq=False)
class JoinOp(Op):
    """JOIN: lf.join(other, on=..., left_on=..., right_on=..., how="left")

    3つのJOINキー指定パターン:
      1. on="key"           → 左右同名キー (単一)
      2. on=["k1", "k2"]    → 左右同名キー (複数)
      3. left_on/right_on   → 左右異なるキー名
    """
    left: Op
    right: Op
    on: tuple[Expr, ...] | None = None           # 同名キー
    left_on: tuple[Expr, ...] | None = None       # 左テーブルのキー
    right_on: tuple[Expr, ...] | None = None      # 右テーブルのキー
    how: str = "inner"  # "inner", "left", "right", "outer", "cross", "semi", "anti"
    validate: str = "m:m"  # "1:1", "1:m", "m:1", "m:m"

@dataclass(frozen=True, eq=False)
class SortOp(Op):
    """ORDER BY: lf.sort("age", descending=True)"""
    child: Op
    by: tuple[Expr, ...]
    descending: tuple[bool, ...]

@dataclass(frozen=True, eq=False)
class LimitOp(Op):
    """LIMIT / OFFSET: lf.limit(10) / lf.head(5)"""
    child: Op
    n: int
    offset: int = 0

@dataclass(frozen=True, eq=False)
class DistinctOp(Op):
    """DISTINCT: lf.unique()"""
    child: Op
    subset: tuple[str, ...] | None = None  # DISTINCT ON (PostgreSQL)

@dataclass(frozen=True, eq=False)
class RenameOp(Op):
    """列名変更: lf.rename({"old": "new"})"""
    child: Op
    mapping: tuple[tuple[str, str], ...]

@dataclass(frozen=True, eq=False)
class DropOp(Op):
    """列削除: lf.drop("col1", "col2")"""
    child: Op
    columns: tuple[str, ...]
```

### 5.3 LazyFrame

```python
class LazyFrame:
    """Polars LazyFrame互換のクエリビルダー。イミュータブル。"""

    def __init__(self, op: Op, connection: "Connection"):
        self._op = op
        self._conn = connection

    # --- 基本操作 ---

    def filter(self, predicate: Expr) -> "LazyFrame":
        return LazyFrame(FilterOp(child=self._op, predicate=predicate), self._conn)

    def select(self, *exprs: Expr | str) -> "LazyFrame":
        return LazyFrame(SelectOp(child=self._op, exprs=_normalize(exprs)), self._conn)

    def with_columns(self, *exprs: Expr) -> "LazyFrame":
        return LazyFrame(WithColumnsOp(child=self._op, exprs=_normalize(exprs)), self._conn)

    def sort(self, *by: str | Expr, descending: bool | list[bool] = False) -> "LazyFrame":
        ...

    def limit(self, n: int) -> "LazyFrame":
        return LazyFrame(LimitOp(child=self._op, n=n), self._conn)

    def head(self, n: int = 5) -> "LazyFrame":
        return self.limit(n)

    def unique(self, subset: list[str] | None = None) -> "LazyFrame":
        return LazyFrame(DistinctOp(child=self._op, subset=subset), self._conn)

    def rename(self, mapping: dict[str, str]) -> "LazyFrame":
        return LazyFrame(RenameOp(child=self._op, mapping=tuple(mapping.items())), self._conn)

    def drop(self, *columns: str) -> "LazyFrame":
        return LazyFrame(DropOp(child=self._op, columns=columns), self._conn)

    # --- 結合 ---

    def join(
        self,
        other: "LazyFrame",
        on: str | Expr | list[str | Expr] | None = None,
        left_on: str | Expr | list[str | Expr] | None = None,
        right_on: str | Expr | list[str | Expr] | None = None,
        how: str = "inner",
        validate: str = "m:m",
    ) -> "LazyFrame":
        """Polars互換のJOIN。on, left_on/right_onの3パターン + validate対応。

        Examples:
            lf.join(other, on="user_id")
            lf.join(other, left_on="user_id", right_on="id")
            lf.join(other, on="user_id", validate="1:m")  # 左キーの一意性を検証
        """
        if on is not None and (left_on is not None or right_on is not None):
            raise ValueError("Cannot specify both 'on' and 'left_on'/'right_on'")
        if (left_on is None) != (right_on is None):
            raise ValueError("'left_on' and 'right_on' must both be specified")
        if validate not in ("m:m", "1:1", "1:m", "m:1"):
            raise ValueError(f"Invalid validate option: {validate}")

        return LazyFrame(
            JoinOp(
                left=self._op,
                right=other._op,
                on=_normalize_opt(on),
                left_on=_normalize_opt(left_on),
                right_on=_normalize_opt(right_on),
                how=how,
                validate=validate,
            ),
            self._conn,
        )

    # --- 集約 ---

    def group_by(self, *by: str | Expr) -> "GroupByProxy":
        return GroupByProxy(self, _normalize(by))

    # --- 実行 ---

    def collect(self) -> "pl.DataFrame":
        """SQLを生成・実行し、polars.DataFrameを返す。
        JoinOpにvalidate != "m:m"が含まれる場合、JOIN前にバリデーションクエリを実行。"""
        self._run_validations()
        sql = self._compile()
        return self._conn.execute(sql)

    def _run_validations(self) -> None:
        """Opツリー内のJoinOp.validateを検査し、必要ならバリデーションクエリを実行。

        注意: バリデーションとメインクエリは別トランザクションで実行されるため、
        バリデーション通過後にデータが変更される可能性がある（TOCTOU）。
        これはPolars本体のvalidateと同様、データモデリング上の仮定を検証する
        ベストエフォートのアサーションであり、トランザクション整合性の保証ではない。"""
        compiler = QueryCompiler(self._conn.backend, self._conn)
        validator = JoinValidator()
        for join_op in self._find_join_ops(self._op):
            for vq in validator.build_validation_queries(join_op, compiler):
                result = self._conn.execute(vq)
                if len(result) > 0:
                    raise JoinValidationError(
                        f"Join validate='{join_op.validate}' failed: "
                        f"duplicate keys found"
                    )

    def show_query(self) -> str:
        """生成されるSQLを文字列で返す"""
        return self._compile()

    def explain(self) -> str:
        """オペレーションツリーの人間可読な表現を返す"""
        return _format_tree(self._op)

    def explain_query(self, *, analyze: bool = False) -> str:
        """DBの実行プランを取得する。

        Args:
            analyze: Falseの場合、推定プラン（クエリを実行しない）。
                     Trueの場合、実行プラン（クエリを実際に実行し、
                     実行時間・実際の行数等の統計を含む）。

        Returns:
            DBが返す実行プランのテキスト表現。

        Raises:
            BackendNotSupportedError: SQL Server, BigQuery等
                EXPLAIN構文を持たないDBで呼び出した場合。
                analyzeに非対応のDBでanalyze=Trueを指定した場合。

        Examples:
            >>> print(result.explain_query())
            Seq Scan on users  (cost=0.00..1.05 rows=1 width=...)
              Filter: (age > 30)

            >>> print(result.explain_query(analyze=True))
            Seq Scan on users  (cost=0.00..1.05 rows=1 width=...) (actual time=0.01..0.02 rows=1 loops=1)
              Filter: (age > 30)
        """
        sql = self._compile()
        explain_sql = self._conn.backend.build_explain_sql(sql, analyze=analyze)
        result = self._conn.execute(explain_sql)
        return self._conn.backend.format_explain_result(result)

    # --- 内部 ---

    def _compile(self) -> str:
        compiler = QueryCompiler(self._conn.backend, self._conn)
        ast = compiler.compile(self._op)
        optimized = Optimizer().optimize(ast)
        return self._conn.backend.render(optimized)


class GroupByProxy:
    """group_by()の戻り値。agg()を呼ぶまでの中間オブジェクト。"""

    def __init__(self, lf: LazyFrame, by: tuple[Expr, ...]):
        self._lf = lf
        self._by = by

    def agg(self, *exprs: Expr) -> LazyFrame:
        return LazyFrame(
            GroupByOp(child=self._lf._op, by=self._by, agg=_normalize(exprs)),
            self._lf._conn
        )
```

### 5.4 Connection

```python
class Connection:
    """データベース接続を管理する"""

    def __init__(self, conn_str: str, backend: Backend | None = None):
        self._conn_str = conn_str
        self.backend = backend or detect_backend(conn_str)
        self._schema_cache: dict[str, list[str]] = {}  # テーブル名 → カラム名リスト

    def __repr__(self) -> str:
        return f"Connection({self._masked_conn_str()!r})"

    def _masked_conn_str(self) -> str:
        """接続文字列のパスワード部分を *** にマスクする。
        スタックトレースやログへの意図しない漏洩を防止する。"""
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(self._conn_str)
        if parsed.password:
            masked = parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            )
            return urlunparse(masked)
        return self._conn_str

    def table(self, name: str, schema: str | None = None) -> LazyFrame:
        """テーブルへの遅延参照を返す"""
        return LazyFrame(TableRef(name=name, schema=schema), self)

    def execute(self, sql: str) -> pl.DataFrame:
        """SQLを実行してpolars.DataFrameを返す。Backendに実行を委譲。"""
        arrow_table = self.backend.execute_sql(sql, self._conn_str)
        return pl.from_arrow(arrow_table)

    def execute_raw(self, sql: str) -> pl.DataFrame:
        """生SQLを直接実行する。

        polars-db の式APIでは表現できないクエリ用のエスケープハッチ。

        .. warning::
            このメソッドはSQLをそのまま実行します。
            外部入力を文字列結合で渡さないでください。
            パラメータが必要な場合は、呼び出し元でサニタイズしてください。
        """
        return self.execute(sql)

    # --- スキーマキャッシュ (Option D: 遅延取得 + Connectionレベルキャッシュ) ---

    def get_schema(self, table: str) -> list[str]:
        """テーブルのカラム一覧を返す。未取得なら INFORMATION_SCHEMA から遅延取得しキャッシュする。
        drop(), rename(), with_columns(同名上書き) など、カラム列挙が必要な操作から呼ばれる。"""
        if table not in self._schema_cache:
            self._schema_cache[table] = self._fetch_schema(table)
        return self._schema_cache[table]

    def _fetch_schema(self, table: str) -> list[str]:
        """INFORMATION_SCHEMA (または同等のメタデータテーブル) からカラム一覧を取得する。
        バックエンドごとにクエリが異なる場合は Backend.schema_query() に委譲。"""
        sql = self.backend.schema_query(table)
        result = self.execute(sql)
        return result["column_name"].to_list()

    def refresh_schema(self, table: str | None = None) -> None:
        """スキーマキャッシュを無効化する。テーブルのDDL変更後に使用。"""
        if table:
            self._schema_cache.pop(table, None)
        else:
            self._schema_cache.clear()

    def close(self) -> None:
        """接続を閉じてリソースを解放する。
        NativeDriverBackend（Snowflake/Databricks）では保持中のDB接続を閉じる。
        ConnectorxBackendでは何もしない（connectorxが接続を管理するため）。"""
        if hasattr(self.backend, "close"):
            self.backend.close()
        self._schema_cache.clear()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def connect(conn_str: str, **kwargs) -> Connection:
    """公開API: 接続を作成する"""
    return Connection(conn_str, **kwargs)


def detect_backend(conn_str: str) -> Backend:
    """接続文字列からバックエンドを自動検出"""
    if conn_str.startswith("postgresql://") or conn_str.startswith("postgres://"):
        return PostgresBackend()
    elif "duckdb" in conn_str:
        return DuckDBBackend()
    elif conn_str.startswith("mysql://"):
        return MySQLBackend()
    elif conn_str.startswith("sqlite://"):
        return SQLiteBackend()
    elif conn_str.startswith("mssql://"):
        if "azuresynapse.net" in conn_str or "synapse" in conn_str:
            return SynapseBackend()
        return SQLServerBackend()
    elif "bigquery" in conn_str or conn_str.startswith("bigquery://"):
        return BigQueryBackend()
    elif "redshift" in conn_str:
        return RedshiftBackend()
    elif "snowflake" in conn_str:
        return SnowflakeBackend()
    elif "databricks" in conn_str:
        return DatabricksBackend()
    else:
        raise ValueError(f"Unsupported connection string: {conn_str}")
```

### 5.5 Compiler

#### ExprCompiler (Expr -> SQLGlot Expression)

```python
import sqlglot
import sqlglot.expressions as exp

class ExprCompiler:
    """Expr ASTをSQLGlot Expressionに変換する"""

    def __init__(self, backend: Backend):
        self._backend = backend

    def compile(self, expr: Expr) -> exp.Expression:
        match expr:
            case ColExpr(name):
                return exp.Column(this=exp.to_identifier(name))

            case LitExpr(value) if isinstance(value, bool):
                return exp.Boolean(this=value)  # bool は int のサブクラスなので先に判定
            case LitExpr(value) if isinstance(value, (int, float)):
                return exp.Literal.number(value)
            case LitExpr(value) if isinstance(value, str):
                return exp.Literal.string(value)
            case LitExpr(value) if value is None:
                return exp.Null()

            case BinaryExpr(op, left, right):
                left_sql = self.compile(left)
                right_sql = self.compile(right)
                return self._binary_op(op, left_sql, right_sql)

            case UnaryExpr("not", operand):
                return exp.Not(this=self.compile(operand))
            case UnaryExpr("neg", operand):
                return exp.Neg(this=self.compile(operand))

            case AggExpr(func, arg):
                return self._agg_func(func, self.compile(arg))

            case WindowExpr(inner_expr, partition_by, order_by):
                inner = self.compile(inner_expr)
                pb = [self.compile(e) for e in partition_by]
                ob = [self.compile(e) for e in order_by] if order_by else None
                return exp.Window(this=inner, partition_by=pb, order=ob)

            case AliasExpr(inner_expr, alias):
                return exp.Alias(this=self.compile(inner_expr),
                                 alias=exp.to_identifier(alias))

            case CaseExpr(cases, otherwise):
                ifs = [exp.If(this=self.compile(c), true=self.compile(v))
                       for c, v in cases]
                default = self.compile(otherwise) if otherwise else None
                return exp.Case(ifs=ifs, default=default)

            case FuncExpr(func_name, args):
                return self._builtin_func(func_name, args)

    def _binary_op(self, op: str, left, right) -> exp.Expression:
        ops = {
            ">": exp.GT, "<": exp.LT, ">=": exp.GTE, "<=": exp.LTE,
            "==": exp.EQ, "!=": exp.NEQ,
            "+": exp.Add, "-": exp.Sub, "*": exp.Mul, "/": exp.Div,
            "%": exp.Mod,
            "and": exp.And, "or": exp.Or,
        }
        return ops[op](this=left, expression=right)

    def _agg_func(self, func: str, arg) -> exp.Expression:
        funcs = {
            "sum": exp.Sum, "mean": exp.Avg, "count": exp.Count,
            "min": exp.Min, "max": exp.Max,
            "std": exp.Stddev, "var": exp.Variance,
        }
        return funcs[func](this=arg)

    def _builtin_func(self, name: str, args: tuple[Expr, ...]) -> exp.Expression:
        compiled_args = [self.compile(a) for a in args]
        mapping = {
            "lower": exp.Lower, "upper": exp.Upper,
            "length": exp.Length,
            "coalesce": exp.Coalesce,
            "cast": exp.Cast,
            # ... DB固有の関数はbackend.function_mapping()で上書き
        }
        if name in mapping:
            return mapping[name](this=compiled_args[0],
                                 expressions=compiled_args[1:])
        # フォールバック: 汎用関数呼び出し
        return exp.Anonymous(this=name, expressions=compiled_args)
```

#### QueryCompiler (Op Tree -> SQLGlot Select)

```python
class QueryCompiler:
    """オペレーションツリーをSQLGlot ASTに変換する"""

    def __init__(self, backend: Backend, connection: "Connection | None" = None):
        """connection は _resolve_columns() でスキーマ取得が必要な場合のみ使用。
        SQL生成のみのユニットテストでは None で構わない。"""
        self._expr_compiler = ExprCompiler(backend)
        self._connection = connection

    def compile(self, op: Op) -> exp.Expression:
        match op:
            case TableRef(name, schema):
                table = exp.Table(this=exp.to_identifier(name))
                if schema:
                    table.set("db", exp.to_identifier(schema))
                return exp.Select(expressions=[exp.Star()]).from_(table)

            case FilterOp(child, predicate):
                inner = self.compile(child)
                condition = self._expr_compiler.compile(predicate)
                return inner.where(condition)

            case SelectOp(child, exprs):
                inner = self._ensure_subquery(self.compile(child))
                columns = [self._expr_compiler.compile(e) for e in exprs]
                return exp.Select(expressions=columns).from_(inner)

            case WithColumnsOp(child, exprs):
                inner = self.compile(child)
                new_cols = {
                    e.alias: self._expr_compiler.compile(e)
                    for e in exprs if isinstance(e, AliasExpr)
                }
                # 新規カラム（既存カラムと同名でないもの）
                new_only = [
                    self._expr_compiler.compile(e)
                    for e in exprs
                    if not isinstance(e, AliasExpr)
                ]

                if not new_cols and not new_only:
                    return inner

                if new_cols:
                    # 同名カラムがある → スキーマからカラム一覧を取得し、
                    # 1パスで各カラムを上書きor保持する
                    all_columns = self._resolve_columns(child)
                    result_cols = []
                    for col_name in all_columns:
                        if col_name in new_cols:
                            # 同名 → 新しい式で置換
                            result_cols.append(new_cols[col_name])
                        else:
                            # 既存カラムをそのまま残す
                            result_cols.append(exp.Column(this=exp.to_identifier(col_name)))
                    # 新規カラム（既存に同名がないもの）を末尾に追加
                    added_aliases = set(new_cols.keys())
                    for e in exprs:
                        alias = e.alias if isinstance(e, AliasExpr) else None
                        if alias and alias not in {c for c in all_columns}:
                            result_cols.append(new_cols[alias])
                    result_cols.extend(new_only)
                    inner_sub = self._ensure_subquery(inner)
                    return exp.Select(expressions=result_cols).from_(inner_sub)
                else:
                    # 同名上書きなし → 従来の SELECT *, new_cols
                    compiled = [self._expr_compiler.compile(e) for e in exprs]
                    return inner.select(*compiled, append=True)

            case GroupByOp(child, by, agg):
                inner = self._ensure_subquery(self.compile(child))
                group_cols = [self._expr_compiler.compile(e) for e in by]
                agg_cols = [self._expr_compiler.compile(e) for e in agg]
                select = exp.Select(expressions=group_cols + agg_cols).from_(inner)
                return select.group_by(*group_cols)

            case JoinOp(left, right, on, left_on, right_on, how) if how in ("semi", "anti"):
                # semi/anti join → WHERE [NOT] EXISTS 相関サブクエリに変換
                # 例: SELECT * FROM users WHERE EXISTS (
                #       SELECT 1 FROM orders WHERE users.user_id = orders.user_id)
                left_sql = self.compile(left)
                right_sql = self.compile(right)
                left_table = self._resolve_table_alias(left)
                right_table = self._resolve_table_alias(right)
                correlated_cond = self._compile_correlated_join_condition(
                    on=on, left_on=left_on, right_on=right_on,
                    left_table=left_table, right_table=right_table,
                )
                exists_subquery = (
                    exp.Select(expressions=[exp.Literal.number(1)])
                    .from_(self._table_expr(right))
                    .where(correlated_cond)
                )
                exists = exp.Exists(this=exists_subquery)
                condition = exists if how == "semi" else exp.Not(this=exists)
                return left_sql.where(condition)

            case JoinOp(left, right, on, left_on, right_on, how):
                left_sql = self.compile(left)
                right_sql = self._ensure_subquery(self.compile(right))
                join_type = self._join_type(how)
                if on is not None:
                    on_expr = self._compile_join_on_same(on)
                else:
                    on_expr = self._compile_join_on_different(left_on, right_on)
                return left_sql.join(right_sql, on=on_expr, join_type=join_type)

            case SortOp(child, by, descending):
                inner = self.compile(child)
                order_exprs = [
                    exp.Ordered(this=self._expr_compiler.compile(e),
                                desc=d)
                    for e, d in zip(by, descending)
                ]
                return inner.order_by(*order_exprs)

            case LimitOp(child, n, offset):
                inner = self.compile(child)
                result = inner.limit(n)
                if offset > 0:
                    result = result.offset(offset)
                return result

            case DistinctOp(child, subset):
                inner = self.compile(child)
                return inner.distinct()

            case RenameOp(child, mapping):
                inner = self.compile(child)
                # SELECT old AS new, ... のエイリアスとして処理
                ...

    def _resolve_columns(self, op: Op) -> list[str]:
        """Op ツリーを辿ってカラム一覧を解決する。
        - TableRef → Connection.get_schema(table_name) でDB問い合わせ（キャッシュ付き）
        - SelectOp → exprs のエイリアス名リスト
        - WithColumnsOp → 親カラム + 新カラム（同名は上書き）
        - GroupByOp → by + agg のエイリアス名リスト
        - RenameOp → マッピング適用後のカラム名リスト
        - DropOp → 除外後のカラム名リスト
        - FilterOp/SortOp/LimitOp/DistinctOp → 子のカラムをそのまま透過
        """
        match op:
            case TableRef(name, schema):
                return self._connection.get_schema(name)
            case SelectOp(child, exprs):
                return [self._extract_alias(e) for e in exprs]
            case WithColumnsOp(child, exprs):
                parent_cols = self._resolve_columns(child)
                new_aliases = {e.alias for e in exprs if isinstance(e, AliasExpr)}
                result = [c for c in parent_cols if c not in new_aliases]
                result.extend(self._extract_alias(e) for e in exprs)
                return result
            case RenameOp(child, mapping):
                parent_cols = self._resolve_columns(child)
                rename_map = dict(mapping)  # tuple[tuple[str, str], ...] → dict
                return [rename_map.get(c, c) for c in parent_cols]
            case DropOp(child, columns):
                parent_cols = self._resolve_columns(child)
                drop_set = set(columns)
                return [c for c in parent_cols if c not in drop_set]
            case FilterOp(child, _) | SortOp(child, _, _) | LimitOp(child, _, _) | DistinctOp(child, _):
                return self._resolve_columns(child)
            case GroupByOp(child, by, agg):
                return [self._extract_alias(e) for e in (*by, *agg)]
            case JoinOp(left, right, *_):
                return self._resolve_columns(left) + self._resolve_columns(right)

    def _extract_alias(self, expr: Expr) -> str:
        """式からカラム名/エイリアス名を抽出する"""
        if isinstance(expr, AliasExpr):
            return expr.alias
        elif isinstance(expr, ColExpr):
            return expr.name
        else:
            raise CompileError(f"Cannot determine column name for {expr}")

    def _ensure_subquery(self, select: exp.Select) -> exp.Subquery:
        """必要に応じてサブクエリでラップする"""
        return select.subquery()

    def _join_type(self, how: str) -> str:
        """JOIN種別をSQLキーワードに変換する。
        semi/anti は compile() 側で事前に分岐されるため、ここには到達しない。"""
        mapping = {
            "inner": "JOIN", "left": "LEFT JOIN", "right": "RIGHT JOIN",
            "outer": "FULL OUTER JOIN", "cross": "CROSS JOIN",
        }
        if how in mapping:
            return mapping[how]
        raise ValueError(f"Unknown join type: {how!r}")

    def _resolve_table_alias(self, op: Op) -> str:
        """Op ツリーを辿り、ルートの TableRef からテーブル名を取得する。
        semi/anti JOIN の相関サブクエリでテーブル修飾子として使用。"""
        match op:
            case TableRef(name, _):
                return name
            case JoinOp(left, *_):
                return self._resolve_table_alias(left)
            case _ if hasattr(op, "child"):
                return self._resolve_table_alias(op.child)
            case _:
                raise CompileError(f"Cannot resolve table alias for {type(op).__name__}")

    def _table_expr(self, op: Op) -> exp.Table:
        """Op ツリーのルート TableRef から SQLGlot の Table 式を生成する。"""
        match op:
            case TableRef(name, schema):
                table = exp.Table(this=exp.to_identifier(name))
                if schema:
                    table.set("db", exp.to_identifier(schema))
                return table
            case _ if hasattr(op, "child"):
                return self._table_expr(op.child)
            case _:
                raise CompileError(f"Cannot resolve table for {type(op).__name__}")

    def _compile_correlated_join_condition(
        self, *, on, left_on, right_on, left_table: str, right_table: str,
    ) -> exp.Expression:
        """テーブル修飾付きの JOIN 条件を生成する（相関サブクエリ用）。

        例: on=("user_id",), left_table="users", right_table="orders"
         → users.user_id = orders.user_id
        """
        if on is not None:
            left_keys = right_keys = on
        else:
            left_keys, right_keys = left_on, right_on

        conditions = []
        for lk, rk in zip(left_keys, right_keys):
            lk_name = lk.name if isinstance(lk, ColExpr) else lk
            rk_name = rk.name if isinstance(rk, ColExpr) else rk
            conditions.append(exp.EQ(
                this=exp.Column(
                    table=exp.to_identifier(left_table),
                    this=exp.to_identifier(lk_name),
                ),
                expression=exp.Column(
                    table=exp.to_identifier(right_table),
                    this=exp.to_identifier(rk_name),
                ),
            ))
        result = conditions[0]
        for c in conditions[1:]:
            result = exp.And(this=result, expression=c)
        return result
```

#### Optimizer

```python
class JoinValidator:
    """JOIN前にカーディナリティを検証する（validate パラメータ対応）。

    validate != "m:m" の場合、JOIN実行前にバリデーションクエリを発行し、
    キーの一意性を確認する。違反があれば JoinValidationError を送出。

    これはデータモデリング上の仮定を検証するベストエフォートのアサーションであり、
    トランザクション整合性を保証するものではない。バリデーションとメインクエリは
    別トランザクションで実行されるため、間にデータが変更される可能性がある。

    validate="1:m" の例:
        SELECT user_id FROM users GROUP BY user_id HAVING COUNT(*) > 1 LIMIT 1
        → 結果があれば "左テーブルにキーの重複あり" でエラー
    """

    def build_validation_queries(
        self, join_op: JoinOp, compiler: "QueryCompiler"
    ) -> list[str]:
        """バリデーションに必要なSQLクエリのリストを返す。"""
        if join_op.validate == "m:m":
            return []

        queries = []
        keys = join_op.on or join_op.left_on  # どちらかは必ずある

        check_left = join_op.validate in ("1:1", "1:m")
        check_right = join_op.validate in ("1:1", "m:1")

        if check_left:
            # 左テーブルのキー一意性チェック
            left_keys = join_op.on or join_op.left_on
            queries.append(self._uniqueness_query(join_op.left, left_keys, compiler))

        if check_right:
            # 右テーブルのキー一意性チェック
            right_keys = join_op.on or join_op.right_on
            queries.append(self._uniqueness_query(join_op.right, right_keys, compiler))

        return queries

    def _uniqueness_query(self, op: Op, keys: tuple, compiler: "QueryCompiler") -> str:
        """GROUP BY key HAVING COUNT(*) > 1 LIMIT 1 のSQLを生成"""
        ...


class Optimizer:
    """SQLGlot ASTを最適化する"""

    def optimize(self, ast: exp.Expression) -> exp.Expression:
        ast = self._remove_unnecessary_subqueries(ast)
        ast = self._merge_consecutive_filters(ast)
        return ast

    def _remove_unnecessary_subqueries(self, ast):
        """不要なサブクエリを除去する。
        例: SELECT * FROM (SELECT * FROM users WHERE age > 30)
         -> SELECT * FROM users WHERE age > 30
        """
        ...

    def _merge_consecutive_filters(self, ast):
        """連続するWHERE句をANDで統合する。
        例: WHERE a > 1 + WHERE b < 2 -> WHERE a > 1 AND b < 2
        """
        ...
```

### 5.6 Backend (Pluggable Connection Design)

SQL方言の制御と接続・実行の両方を各バックエンドが担う。connectorx対応DBはConnectorxBackendを継承し、
未対応DB（Snowflake, Databricks）は各ネイティブドライバのArrow出力を使用する。

#### DB接続マトリクス

| DB | コネクタ | SQLGlot方言 | 備考 |
|---|---|---|---|
| PostgreSQL | connectorx | `postgres` | 最速。Polars内部でも利用 |
| DuckDB | connectorx | `duckdb` | ローカル分析用途 |
| MySQL | connectorx | `mysql` | |
| SQLite3 | connectorx | `sqlite` | ローカルDB。`sqlite:///path/to/db.sqlite` |
| SQL Server | connectorx | `tsql` | `mssql://user:pass@server:1433/db` |
| BigQuery | connectorx or `google-cloud-bigquery` (`to_arrow()`) | `bigquery` | |
| Redshift | connectorx (PostgreSQLプロトコル) | `redshift` | |
| Azure Synapse | connectorx (MSSQLプロトコル) or pyodbc | `tsql` | TDS互換。`mssql://user:pass@{server}.sql.azuresynapse.net:1433/{db}` |
| Snowflake | `snowflake-connector-python` (`fetch_arrow_all()`) | `snowflake` | connectorx未対応 |
| Databricks | `databricks-sql-connector` (`fetchmany_arrow()`) | `databricks` | connectorx未対応 |

```python
from abc import ABC, abstractmethod
import pyarrow as pa

class Backend(ABC):
    """データベースバックエンドの抽象基底クラス。
    SQL方言の制御と、接続・実行の両方を担う。"""

    @property
    @abstractmethod
    def dialect(self) -> str:
        """SQLGlotの方言名 ("postgres", "duckdb", "mysql" 等)"""
        ...

    @abstractmethod
    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        """SQLを実行してArrow Tableを返す。各バックエンドが最適なコネクタを使う。"""
        ...

    def render(self, ast: exp.Expression) -> str:
        """SQLGlot ASTをSQL文字列に変換"""
        return ast.sql(dialect=self.dialect, pretty=True)

    def function_mapping(self) -> dict[str, str]:
        """DB固有の関数名マッピング（オーバーライド用）"""
        return {}

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        """EXPLAIN文を生成する。デフォルトは EXPLAIN [ANALYZE] sql。
        DB固有の構文が必要な場合はサブクラスでオーバーライド。"""
        prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        return f"{prefix} {sql}"

    def format_explain_result(self, df: "pl.DataFrame") -> str:
        """EXPLAIN結果のDataFrameをテキストに変換する。
        大半のDBは単一カラムでプランテキストを返すため、行を改行で連結。"""
        return "\n".join(str(v) for v in df.to_series(0).to_list())

    def schema_query(self, table: str) -> str:
        """テーブルのカラム一覧を取得するSQLを生成する。
        テーブル名はexp.Literal.string()でエスケープし、
        本線クエリパスと同じくSQLGlot経由で安全にSQL構築する。"""
        return (
            exp.Select(expressions=[exp.Column(this=exp.to_identifier("column_name"))])
            .from_(exp.Table(
                db=exp.to_identifier("information_schema"),
                this=exp.to_identifier("columns"),
            ))
            .where(exp.EQ(
                this=exp.Column(this=exp.to_identifier("table_name")),
                expression=exp.Literal.string(table),
            ))
            .sql(dialect=self.dialect)
        )


# --- connectorx対応DB ---

class ConnectorxBackend(Backend):
    """connectorx対応DBの共通基底クラス"""

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        import connectorx as cx
        return cx.read_sql(conn=conn_str, query=sql, return_type="arrow2")


class PostgresBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "postgres"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}


class DuckDBBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "duckdb"


class MySQLBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "mysql"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "GROUP_CONCAT"}


class BigQueryBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "bigquery"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        raise BackendNotSupportedError(
            "BigQuery does not support EXPLAIN. "
            "Query plans are only available after execution via the Jobs API."
        )


class RedshiftBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "redshift"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            raise BackendNotSupportedError(
                "Redshift does not support EXPLAIN ANALYZE."
            )
        return f"EXPLAIN {sql}"


class SQLiteBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "sqlite"

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            raise BackendNotSupportedError(
                "SQLite does not support EXPLAIN ANALYZE."
            )
        return f"EXPLAIN QUERY PLAN {sql}"


class SQLServerBackend(ConnectorxBackend):
    @property
    def dialect(self) -> str:
        return "tsql"

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}  # SQL Server 2017+

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        raise BackendNotSupportedError(
            "SQL Server does not support EXPLAIN. "
            "Use SET SHOWPLAN_XML ON via execute_raw() as a workaround."
        )


class SynapseBackend(SQLServerBackend):
    """Azure Synapse Analytics: SQL ServerとTDS互換。SQLServerBackendを継承。
    SQL認証はconnectorx経由、Entra ID認証が必要な場合はpyodbc経由にフォールバック。"""
    pass


# --- ネイティブドライバ対応DB ---
#
# Snowflake / Databricks は接続確立コストが高い（JWT/OAuth認証で1〜3秒）。
# connectorx系と異なり、毎クエリ新規接続では validate 付きJOINなどで
# 複数回の接続確立が発生しパフォーマンスが致命的に悪化する。
# そのため接続を lazy singleton として保持し、close() で明示的に解放する。


class NativeDriverBackend(Backend):
    """ネイティブドライバ系バックエンドの共通基底クラス。
    接続を lazy singleton として保持し、接続確立コストを初回のみに抑える。"""

    def __init__(self):
        self._conn = None
        self._conn_str: str | None = None

    def _get_connection(self, conn_str: str):
        """接続を遅延初期化して返す。同一conn_strなら既存接続を再利用。"""
        if self._conn is None or self._conn_str != conn_str:
            self.close()
            self._conn = self._create_connection(conn_str)
            self._conn_str = conn_str
        return self._conn

    @abstractmethod
    def _create_connection(self, conn_str: str):
        """DB固有の接続を確立する。サブクラスで実装。"""
        ...

    def close(self) -> None:
        """接続を明示的に閉じる。Connection.close() から呼ばれる。"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._conn_str = None


class SnowflakeBackend(NativeDriverBackend):
    """Snowflake: snowflake-connector-pythonのArrow出力を使用"""

    @property
    def dialect(self) -> str:
        return "snowflake"

    def _create_connection(self, conn_str: str):
        import snowflake.connector
        return snowflake.connector.connect(**self._parse_conn_str(conn_str))

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetch_arrow_all()

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            raise BackendNotSupportedError(
                "Snowflake does not support EXPLAIN ANALYZE."
            )
        return f"EXPLAIN {sql}"

    def _parse_conn_str(self, conn_str: str) -> dict:
        """接続文字列をSnowflakeのパラメータ辞書に変換"""
        ...


class DatabricksBackend(NativeDriverBackend):
    """Databricks: databricks-sql-connectorのArrow出力を使用"""

    @property
    def dialect(self) -> str:
        return "databricks"

    def _create_connection(self, conn_str: str):
        from databricks import sql as dbsql
        return dbsql.connect(**self._parse_conn_str(conn_str))

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall_arrow()

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        if analyze:
            raise BackendNotSupportedError(
                "Databricks does not support EXPLAIN ANALYZE."
            )
        return f"EXPLAIN {sql}"

    def _parse_conn_str(self, conn_str: str) -> dict:
        """接続文字列をDatabricksのパラメータ辞書に変換"""
        ...
```

### 5.7 Type Mapping (types.py)

Polars dtype と SQL 型の双方向マッピングを定義する。パイプライン全体の型安全性を左右する重要モジュール。

#### 基本マッピング

| SQL 型 | Polars dtype | 備考 |
|---|---|---|
| `INTEGER` / `INT` | `Int32` | |
| `BIGINT` | `Int64` | |
| `SMALLINT` | `Int16` | |
| `BOOLEAN` / `BOOL` | `Boolean` | |
| `REAL` / `FLOAT4` | `Float32` | |
| `DOUBLE PRECISION` / `FLOAT8` | `Float64` | |
| `TEXT` / `VARCHAR` / `CHAR(n)` | `Utf8` | |
| `DATE` | `Date` | |
| `TIMESTAMP` | `Datetime("us")` | マイクロ秒精度 |
| `TIMESTAMP WITH TIME ZONE` | `Datetime("us", tz)` | tz はDBセッション設定に依存 |
| `BLOB` / `BYTEA` | `Binary` | |

#### 精度が問題になる型（設計方針）

以下の型はナイーブな変換で精度が失われるため、特別な扱いが必要。

| SQL 型 | ナイーブなマッピング | 問題 | 採用方針 |
|---|---|---|---|
| `NUMERIC(p,s)` / `DECIMAL(p,s)` | `Float64` | 有効桁数15桁を超える精度が失われる（金融データで致命的） | `Decimal(p,s)` を使用（Polars 0.19+ で対応）。connectorx が Decimal を返す場合はそのまま保持 |
| `BIGINT UNSIGNED` | `Int64` | 2^63 超の値がオーバーフロー | `UInt64` を使用。非対応バックエンドでは警告を出して `Float64` にフォールバック |
| `TIMESTAMP(6)` | `Datetime("us")` | マイクロ秒精度は保持される | 精度パラメータに応じて `"ns"` / `"us"` / `"ms"` を選択 |
| `INTERVAL` | なし | Polars に直接対応する型がない | `Duration` にマップ（DB依存の差異あり）。非対応の場合は文字列として返す |

#### 設計原則

1. **精度を落とすより型エラーを出す**: 暗黙の精度喪失は避ける。`DECIMAL(38,18)` を `Float64` に暗黙変換しない
2. **connectorx / ネイティブドライバの型変換を尊重する**: コネクタが Arrow 型として返した結果をそのまま `pl.from_arrow()` に渡す。polars-db 側での追加変換は最小限にする
3. **Backend.type_mapping() でDB固有の上書きを許容する**: 例えば MySQL の `TINYINT(1)` → `Boolean` はMySQL固有のマッピング

## 6. Supported Operations

### Fully Supported (maps cleanly to SQL)

| Category | Operations |
|---|---|
| **Basic** | select, filter, sort, limit, head, slice, distinct, rename, drop, with_columns |
| **Aggregation** | group_by + agg (sum, mean, count, min, max, std, var, first, last) |
| **Join** | inner, left, right, outer, cross, semi, anti join |
| **Window** | over(partition_by), shift (LAG/LEAD), rank, row_number |
| **Expression** | arithmetic, comparison, logical, when/then/otherwise, cast, is_null, fill_null, is_between, is_in |
| **String** | str.to_lowercase, to_uppercase, contains, starts_with, ends_with, len_chars, slice, replace |
| **DateTime** | dt.year, month, day, hour, minute, second, date, truncate |

### Backend-Dependent (raises `BackendNotSupportedError`)

| Operation | Note |
|---|---|
| `explode` | UNNEST (PostgreSQL/DuckDB only) |
| `unpivot` | UNPIVOT (limited DB support) |
| `sample` | TABLESAMPLE (syntax varies) |
| `str.contains(regex)` | REGEXP (syntax varies) |
| `explain_query()` | 下表参照。SQL Server/BigQuery は非対応 |
| `explain_query(analyze=True)` | PostgreSQL/DuckDB/MySQL のみ対応 |

#### explain_query() 対応マトリクス

| Backend | `explain_query()` | `analyze=True` | 備考 |
|---|---|---|---|
| PostgreSQL | ✓ | ✓ | EXPLAIN / EXPLAIN ANALYZE |
| DuckDB | ✓ | ✓ | EXPLAIN / EXPLAIN ANALYZE |
| MySQL | ✓ | ✓ | EXPLAIN / EXPLAIN ANALYZE (8.0.18+) |
| SQLite | ✓ | ✗ | EXPLAIN QUERY PLAN |
| Snowflake | ✓ | ✗ | EXPLAIN（コンパイルのみ、ウェアハウス不要） |
| Redshift | ✓ | ✗ | EXPLAIN（PostgreSQL互換） |
| Databricks | ✓ | ✗ | EXPLAIN（Spark SQL） |
| SQL Server | ✗ | ✗ | SET SHOWPLAN_XML が必要。execute_raw() で代替 |
| Azure Synapse | ✗ | ✗ | SQL Server と同様 |
| BigQuery | ✗ | ✗ | EXPLAIN なし。実行後 Jobs API でのみプラン取得可 |

### Not Supported (raises `UnsupportedOperationError`)

| Operation | Reason |
|---|---|
| `map_elements` / `apply` | Python UDFs cannot run on DB |
| `pipe(custom_fn)` | Arbitrary Python functions cannot be translated |
| Complex `list.*` / `struct.*` | Nested type operations are too DB-dependent |

### Error Hierarchy

「変換不能」と「DB非対応」を区別する2種類のエラーを提供する。ユーザーが取るべきアクションが異なるため。

```python
class PolarsDbError(Exception):
    """polars-db の基底例外"""

class UnsupportedOperationError(PolarsDbError):
    """SQLへの変換が原理的に不可能な操作（Python UDF等）。
    ユーザーアクション: collect()してからローカルのPolarsで処理する。"""

class BackendNotSupportedError(PolarsDbError):
    """SQL変換は可能だが、接続先DBが該当SQL構文をサポートしない操作。
    ユーザーアクション: 対応するDBバックエンドを使うか、回避策を取る。"""

class JoinValidationError(PolarsDbError):
    """JOIN validate パラメータによるカーディナリティ検証の失敗。"""

class CompileError(PolarsDbError):
    """Op Tree → SQL 変換中の内部エラー。"""

class SchemaResolutionError(PolarsDbError):
    """スキーマ取得に失敗（テーブルが存在しない、接続切れ等）。"""
```

エラーメッセージ例:

```python
# パターン1: 変換不能 → ローカル処理を案内
UnsupportedOperationError(
    "map_elements() cannot be translated to SQL. "
    "Use .collect() first, then apply the operation on the resulting polars.DataFrame."
)

# パターン2: DB非対応 → 対応DBを案内
BackendNotSupportedError(
    "explode() (UNNEST) is not supported on MySQL. "
    "Supported backends: PostgreSQL, DuckDB, BigQuery."
)

# パターン3: スキーマ解決失敗
SchemaResolutionError(
    "Could not resolve schema for table 'orders'. "
    "Ensure the table exists and the connection is active."
)
```

## 7. Dependencies

```toml
[project]
requires-python = ">=3.10"

[project.dependencies]
polars = ">=1.0.0"
sqlglot = ">=23.0.0"
connectorx = ">=0.3.0"      # PostgreSQL, DuckDB, MySQL, BigQuery, Redshift

[project.optional-dependencies]
snowflake = ["snowflake-connector-python>=3.0.0"]
databricks = ["databricks-sql-connector>=3.0.0"]
bigquery = ["google-cloud-bigquery>=3.0.0"]   # connectorxの代替
all = ["polars-db[snowflake,databricks,bigquery]"]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "ruff>=0.4",
    "mypy>=1.10",
]
```

インストール例:
- `pip install polars-db` — PostgreSQL, DuckDB, MySQL, BigQuery, Redshift対応
- `pip install polars-db[snowflake]` — Snowflake追加
- `pip install polars-db[all]` — 全DB対応

## 8. Testing Strategy

### Three-Layer Testing

| Layer | Content | Requires DB |
|---|---|---|
| **Unit: Expr** | Expr AST is correctly built from user code | No |
| **Unit: Compiler** | Op Tree -> expected SQL string | No |
| **Integration** | Execute SQL against real DB (Docker), compare results with Polars | Yes |

### Unit Test Example (Compiler)

```python
def test_select_filter_sort():
    """select + filter + sort が正しいSQLを生成するか"""
    op = SortOp(
        child=SelectOp(
            child=FilterOp(
                child=TableRef("users"),
                predicate=BinaryExpr(">", ColExpr("age"), LitExpr(30))
            ),
            exprs=(ColExpr("name"), ColExpr("age"))
        ),
        by=(ColExpr("age"),),
        descending=(False,)
    )
    compiler = QueryCompiler(PostgresBackend())  # connection は省略可（SQL生成のみのテスト）
    sql = compiler.compile(op).sql(dialect="postgres")
    assert "SELECT" in sql
    assert "WHERE" in sql
    assert "ORDER BY" in sql
```

### Integration Test

```python
@pytest.fixture
def pg_connection():
    return pdb.connect("postgresql://test:test@localhost:5432/testdb")

@pytest.mark.parametrize("backend", ["postgres", "duckdb"])
def test_filter_returns_correct_results(backend, connection):
    """フィルタ結果がPolarsのローカル実行と一致するか"""
    # DB経由
    db_result = connection.table("users").filter(pdb.col("age") > 30).collect()

    # ローカルPolars（正解）
    local_df = pl.read_csv("test_data/users.csv")
    expected = local_df.lazy().filter(pl.col("age") > 30).collect()

    assert db_result.frame_equal(expected)
```

### NULL セマンティクス・型精度のエッジケーステスト

DB と Polars のローカル実行で挙動が微妙に異なるケースを明示的にテストする。
インテグレーションテストの「Polarsの結果を正解として比較」で差異を検出する。

| カテゴリ | テストケース | 差異が出る理由 |
|---|---|---|
| **NULL + GROUP BY** | `GROUP BY nullable_col` でNULL行が1グループにまとまるか | SQLは NULL = NULL を TRUE として扱う（GROUP BY限定）。Polarsも同様だが、ソート順が異なる場合がある |
| **NULL + 集約** | `SUM(col)` で全行NULLの場合にNULLを返すか | SQL: NULL。Polars: NULL（一致するが確認必要） |
| **NULL + JOIN** | `NULL` キーでの JOIN がマッチしないか | SQL: NULLキー同士はマッチしない。Polarsも同様 |
| **NULL + ソート** | `ORDER BY nullable_col` でNULLの位置 | PostgreSQL: NULLS LAST（デフォルト）。MySQL: NULLS FIRST。Polars: NULLS LAST |
| **NULL + DISTINCT** | `DISTINCT` でNULL行が1行にまとまるか | SQL/Polars共に1行。確認用 |
| **型精度** | `NUMERIC(10,2)` の小数演算結果が一致するか | Float64変換時の丸め誤差 |
| **型精度** | `BIGINT` の上限値付近の演算 | 2^53超の整数値がFloat64で丸められないか |

### Test Infrastructure

docker-composeで基本DB + DWHエミュレータを起動し、pytest parametrizeで全バックエンドに対して同一テストスイートを実行。
Polarsのローカル実行結果を「正解」として比較。

#### docker-compose.yml テスト環境

```yaml
services:
  # --- 基本DB ---
  postgres:
    image: postgres:16
    environment: { POSTGRES_USER: test, POSTGRES_PASSWORD: test, POSTGRES_DB: testdb }
    ports: ["5432:5432"]

  mysql:
    image: mysql:8
    environment: { MYSQL_ROOT_PASSWORD: test, MYSQL_DATABASE: testdb }
    ports: ["3306:3306"]

  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment: { ACCEPT_EULA: "Y", MSSQL_SA_PASSWORD: "Test@12345" }
    ports: ["1433:1433"]

  # --- DWHエミュレータ ---
  bigquery:
    image: ghcr.io/goccy/bigquery-emulator:latest
    command: --project=test-project --dataset=test_dataset
    ports: ["9050:9050"]

  snowflake:
    image: ghcr.io/nnnkkk7/snowflake-emulator:latest
    ports: ["8080:8080"]

  synapse:
    image: mcr.microsoft.com/azure-sql-edge
    environment: { ACCEPT_EULA: "Y", MSSQL_SA_PASSWORD: "Test@12345" }
    ports: ["1444:1433"]

  # --- Spark (Databricks代替) ---
  spark:
    image: bitnami/spark:latest
    environment: { SPARK_MODE: master }
    ports: ["10000:10000"]  # Thrift Server (JDBC/ODBC)
```

**DuckDB / SQLite3**: ファイルベースのためDockerコンテナ不要。テストコード内でインメモリまたは一時ファイルを使用。
**Redshift**: PostgreSQLコンテナ + connectorxで代替（Redshiftの基盤がPostgreSQL。専用エミュレータはLocalStackだが有料）。

#### エミュレータ対応マトリクス

| バックエンド | テスト方法 | エミュレータ | 接続方法 |
|---|---|---|---|
| PostgreSQL | Docker | `postgres:16` | connectorx |
| DuckDB | ローカル | 不要（インメモリ） | connectorx |
| MySQL | Docker | `mysql:8` | connectorx |
| SQLite3 | ローカル | 不要（インメモリ） | connectorx |
| SQL Server | Docker | `mssql/server:2022` | connectorx |
| BigQuery | Docker | `goccy/bigquery-emulator` | `google-cloud-bigquery` (endpoint指定) |
| Snowflake | Docker | `snowflake-emulator` (DuckDBベース) | `snowflake-connector-python` |
| Redshift | Docker | PostgreSQLで代替 | connectorx |
| Azure Synapse | Docker | Azure SQL Edge (T-SQL互換) | connectorx (MSSQL) |
| Databricks | Docker | Apache Spark Thrift Server | Spark SQL (JDBC/ODBC) |

#### pytest マーカーによるテスト分類

```python
# conftest.py
CORE_BACKENDS = ["postgres", "duckdb", "mysql", "sqlite", "sqlserver"]
DWH_BACKENDS = ["bigquery", "snowflake", "redshift", "synapse", "spark"]
ALL_BACKENDS = CORE_BACKENDS + DWH_BACKENDS

@pytest.fixture(params=ALL_BACKENDS)
def connection(request):
    """全バックエンドをパラメタライズ"""
    return get_test_connection(request.param)
```

## 9. Implementation Phases

1. **Phase 1: Foundation** -- Expr AST, Op nodes, ExprCompiler, QueryCompiler (select/filter/sort only)
2. **Phase 2: Core Operations** -- group_by, join, with_columns, limit, distinct, rename, drop
3. **Phase 3: Analytics** -- Window functions (over, shift, rank), CTE, subqueries
4. **Phase 4: Backends** -- connectorx系 (PostgreSQL, DuckDB, MySQL, BigQuery, Redshift) + ネイティブドライバ系 (Snowflake, Databricks)
5. **Phase 5: Polish** -- Error handling, type mapping, StringNamespace, DateTimeNamespace, documentation

## 10. Design Decisions & Open Questions

### 解決済み: スキーマ取得タイミング (Option D: 遅延取得 + キャッシュ)

`drop()`, `with_columns()`(同名上書き), `rename()` はテーブルのカラム一覧を必要とする（標準SQLに `SELECT * EXCEPT(col)` が無いため）。

**採用方針**: スキーマが必要な操作が初めて呼ばれた時に遅延取得し、Connectionレベルでキャッシュする。

- **遅延取得**: `drop()`, `rename()`, `with_columns()`（同名上書き）が呼ばれた時点で `INFORMATION_SCHEMA` からカラム一覧を取得
- **キャッシュ**: `Connection._schema_cache: dict[str, list[str]]` にテーブル名をキーとして保持。同じテーブルへの2回目以降のアクセスはキャッシュヒット
- **手動無効化**: `con.refresh_schema("table_name")` でDDL変更後にキャッシュをクリア可能（引数なしで全テーブルクリア）
- **show_query() フォールバック**: EXCEPT対応DB (BigQuery/DuckDB) では `SELECT * EXCEPT(col)` を使用。非対応DBではスキーマ未取得時にエラーで案内

**不採用理由**:
- A. Eager（`con.table()` 時に即取得）: 使わないテーブルのスキーマも取得してしまう
- B. ユーザー明示指定: UXが悪い
- C. `collect()` 時に解決: 2つのSQL生成パスが必要で複雑

### 未決事項

- **パッケージ名**: `polars-db` は暫定。PyPIでの名前の空き状況を確認する必要がある
- **Snowflake/Databricksの接続文字列設計**: これらのDBは単純なURI形式では表現しにくい認証情報を持つ（OAuth, トークン等）。`connect()`にkwargsも渡せる設計にするか、設定辞書を受け取るか
- **Polars バージョン追従**: PolarsのAPIは活発に変更される。追従戦略が必要

### 実装フェーズの注意事項

- **`_resolve_table_alias()` の Op ツリー走査テスト**: semi/anti JOIN は `_resolve_table_alias()` でルートの TableRef まで再帰的に辿れることに依存する。深くネストしたケース（例: `FilterOp → SelectOp → TableRef`）や、`JoinOp` の左右分岐を含むケースで正しく動作するか、全 Op パターンのテストケースを書くこと
- **`NativeDriverBackend` のスレッドセーフティ**: Phase 4 でネイティブドライバを実装する際、`_get_connection()` に `threading.Lock` を追加すること。Jupyter のセル並列実行やマルチスレッド環境で、接続の二重作成やレースコンディションが発生する可能性がある
