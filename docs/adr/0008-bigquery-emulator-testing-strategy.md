# ADR-0008: BigQuery Emulator Testing Strategy

## Status

Superseded by [ADR-0015](0015-bigquery-snapshot-testing.md)

## Context

The BigQuery backend uses `goccy/bigquery-emulator` for integration testing. During implementation, two issues were discovered:

1. **Healthcheck failure** — The emulator image does not include `curl` or `wget`, so the original `curl -sf http://localhost:9050/` healthcheck always fails.
2. **DML hang** — `job.result()` hangs indefinitely for INSERT statements, even though `job.state` reports `DONE`. SELECT and DDL (CREATE/DROP TABLE) work correctly.

The emulator's DML polling implementation appears incomplete — it marks the job as done but never resolves the result iterator for non-SELECT statements.

### Options Considered

1. **Timeout-based workaround** — Set `job.result(timeout=N)` to eventually fail and retry. Rejected: produces flaky tests.
2. **Skip DML, test SQL generation only** — Use `show_query()` to verify correct BigQuery SQL generation, and test SELECT execution with literal queries. Adopted.
3. **Use real BigQuery** — Requires GCP project, service account, costs money. Not suitable for local dev or CI.

## Decision

### Healthcheck

Use bash TCP check instead of curl:

```yaml
healthcheck:
  test: [CMD, bash, -c, "echo > /dev/tcp/localhost/9050"]
```

### Backend DML Detection

In `BigQueryBackend.execute_sql()`, detect DML/DDL statements by SQL prefix and skip `job.result()`:

```python
_DML_PREFIXES = ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER")

def execute_sql(self, sql, conn_str):
    client = self._get_client(conn_str)
    job = client.query(sql)
    stripped = sql.strip().upper()
    if any(stripped.startswith(p) for p in self._DML_PREFIXES):
        return pa.table({})
    result = job.result(timeout=30)
    ...
```

### Test Scope

BigQuery integration tests are separated into `TestBigQueryConnectivity`:

- `test_select_literal` — Verify SELECT execution on emulator
- `test_create_table` / `test_cleanup` — Verify DDL works
- `test_show_query_*` — Verify SQL generation (filter, select, sort, group_by)

DML data insertion tests are skipped for BigQuery.

## Consequences

- BigQuery emulator tests are stable and fast (~1s).
- SQL generation correctness is verified without depending on emulator DML support.
- Trade-off: INSERT/UPDATE/DELETE execution paths are not tested. These should be validated against real BigQuery in future scheduled tests.
- The SQL prefix detection may miss edge cases (e.g., `WITH ... INSERT`), but the current API does not generate such queries.
