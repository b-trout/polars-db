# ADR-0018: Schema cache TTL

## Status

Accepted (2026-05-30)

## Context

``Connection.get_schema(table)`` queries ``INFORMATION_SCHEMA`` once per
table and stores the column list in a per-connection dict
(``self._schema_cache``). Several call sites in the compiler depend on
this lookup to resolve ``*``-style projections, JOIN column collisions,
``WithColumnsOp`` ordering, etc. Cached entries never expire —
``refresh_schema(table=None)`` is the only way to drop them.

This is the second of the four TOCTOU sites flagged in the
2026-05-30 audit:

> ``get_schema()`` reads cached column lists; ``QueryCompiler._resolve_columns``
> uses them to generate ``WithColumnsOp``/``RenameOp``/``DropOp``/JOIN
> projection SQL. If a concurrent ALTER TABLE adds/drops a column between
> cache fetch and query execution, the generated SQL references columns
> that no longer exist (or omits new ones). ``refresh_schema()`` is the
> manual escape hatch.

For long-lived applications whose target tables can change under them
(migrations rolling out, dashboards pointing at evolving warehouse
tables, ETL pipelines that ``CREATE TABLE`` upstream of the read), the
"refresh-or-restart" workaround is awkward.

## Decision

Add a ``schema_cache_ttl`` parameter to ``Connection.__init__`` (and
``connect()``) controlling when cached entries expire. The default is
``None`` — cache forever, matching the historical behaviour —
so existing code is unaffected. Concretely:

| ``schema_cache_ttl`` | Behaviour                                                    |
|----------------------|--------------------------------------------------------------|
| ``None`` (default)   | Entries cached forever. Same as pre-0.2 behaviour.           |
| ``0``                | Cache bypassed entirely; every ``get_schema`` queries the DB. |
| ``>0`` (seconds)     | Entries older than the TTL are re-fetched on next access.    |

Cache storage changes from ``dict[str, list[str]]`` to
``dict[str, tuple[float, list[str]]]`` — the float is
``time.monotonic()`` at fetch time so TTL comparisons are immune to
wall-clock shifts.

``refresh_schema(table)`` still works as before: callers who know
their schema just changed can invalidate explicitly without setting a
TTL. The two mechanisms compose — TTL is the background guarantee,
explicit refresh is the precise tool.

### Default rationale

``None`` (forever) keeps the default behaviour identical to existing
deployments. Users who care about schema drift opt in with an explicit
``schema_cache_ttl=60`` (or whatever cadence matches their workload).
A "sensible default" like 60 seconds would change behaviour for every
existing user without warning, and the cost of a stale schema is
silent miscompilation, not a loud error.

### Thread safety

Cache reads and writes go through a Python ``dict``. Under CPython's
GIL the read/insert pairs are atomic, so concurrent ``get_schema``
calls from multiple threads (now possible after ADR-0019) cannot
corrupt the dict — at worst two threads can race to populate the same
table key and both call ``_fetch_schema``, but the last write wins
and both threads see a correct list. A finer-grained lock would
eliminate the rare duplicate fetch but is not needed for correctness.

## Consequences

- Callers can now opt into TTL-bounded schema staleness with a single
  constructor argument. Existing callers see no behavioural change.
- ``Connection._schema_cache`` type changes from
  ``dict[str, list[str]]`` to ``dict[str, tuple[float, list[str]]]``.
  Callers reaching into the private cache (one test fixture in this
  repo did) must adapt. No public-API consumer depends on the
  internal type.
- ``refresh_schema`` is unchanged.
- ``close()`` still clears the cache, releasing memory deterministically.
- Unit coverage lives in ``tests/unit/test_schema_cache_ttl.py`` —
  a ``RecordingBackend`` (real ``Backend`` subclass) counts the
  underlying ``schema_query`` invocations so the assertions are about
  behaviour, not about probing the cache directly.
- This ADR closes the second of the four TOCTOU sites flagged in the
  2026-05-30 audit. ADR-0017 closed the first (atomic JOIN
  validation); ADR-0019 closed the third (backend thread safety);
  the ADR-0013 update closes the fourth (SQL Server auto-create
  race).
