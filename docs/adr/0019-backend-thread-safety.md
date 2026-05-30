# ADR-0019: Backend thread safety via per-thread connection caching

## Status

Accepted (2026-05-30)

## Context

Before this ADR, every concrete ``Backend`` subclass cached a single
driver connection on the instance:

```python
class PostgresBackend(Backend):
    def __init__(self):
        self._conn = None
        self._conn_str = None
        self._in_tx = False
    ...
```

``PostgresBackend``'s class docstring even explicitly warned that this
made concurrent ``collect()`` calls from multiple threads unsafe — a
race on the shared cursor could cross-contaminate result sets, and a
race on the ``_in_tx`` flag could leak one thread's transaction into
another's per-statement commit decision.

This was one of the four TOCTOU-flavoured issues flagged in the
2026-05-30 audit. The three others (atomic JOIN validation, schema
cache TTL, SQL Server auto-create race) all assume a thread-safe
backend underneath; without that assumption their fixes were narrow.

## Decision

Promote the per-instance connection slot to a **per-thread** slot so
each thread that calls into a backend gets its own driver connection
and its own transaction flag. Implementation lives in a small helper
to avoid copy-pasting threading boilerplate across six backends:

```python
class PerThreadConnections:
    def __init__(self):
        self._tls = threading.local()
        self._lock = threading.Lock()
        self._tracker: list[object] = []

    def get_or_create(self, conn_str, factory):
        ...  # returns the calling thread's cached conn, re-opens on conn_str change
    @property
    def in_tx(self) -> bool: ...
    @in_tx.setter
    def in_tx(self, value: bool) -> None: ...
    def close_all(self) -> None: ...
```

Each backend now does:

```python
class PostgresBackend(Backend):
    def __init__(self):
        self._state = PerThreadConnections()
    def _get_connection(self, conn_str):
        return self._state.get_or_create(conn_str, self._create_connection)
    def execute_sql(self, sql, conn_str):
        ...
        if not self._state.in_tx:
            conn.commit()
        ...
    def close(self):
        self._state.close_all()
```

All six backends (Postgres, SQLite, MySQL, SQL Server, DuckDB,
BigQuery) use the same helper. The tx-context manager (``transaction``)
sets ``self._state.in_tx = True`` / ``False`` and is therefore also
per-thread — one thread inside a tx no longer suppresses a sibling
thread's per-statement commit.

### Connection lifetime and ``close()``

The helper maintains a tracker list of every connection it has ever
opened so ``close_all()`` can drop them deterministically regardless of
which thread is doing the close. The trade-off: connections opened
from threads that die without ``close()`` linger in the tracker until
the next explicit close. Long-running applications with many
short-lived threads should call ``Connection.close()`` periodically
(or use a connection pool); for the typical "thread pool of N workers,
all long-lived" case there is no leak.

### Why not ``threading.RLock`` + single connection

The other shortlist option was wrapping the cached connection in a
re-entrant lock so concurrent calls serialise on a single driver
connection. Rejected because:

- It throws away parallelism — any DB-bound query holds the lock for
  the duration of its round-trip, so worker threads block each other
  even when the backend would happily run their queries in parallel.
- Tx semantics still get tangled — one thread inside ``transaction()``
  would block every other thread's reads, including read-only ones
  that have no business being serialised.
- Driver cursor objects are not generally re-entrant; even a serialised
  shared connection can crash on overlapping cursor lifecycles
  (interleaved ``execute`` / ``fetch_arrow_table``).

Per-thread connections trade memory (one driver connection per active
thread, vs one total) for correctness and parallelism. For the
typical Polars workload — a handful of analyst threads, each issuing
a long-running query — that trade-off is clearly favourable.

### In-memory backends note

DuckDB and SQLite under the ``:memory:`` connection string have
**per-connection** databases by driver design. Per-thread connection
caching therefore gives each thread its own private in-memory
database. This is the historically-implicit behaviour (the bug was
that concurrent threads sharing a single ``:memory:`` connection had
race conditions, not that they should see each other's writes) and is
now an explicit property. File-backed DuckDB / SQLite and the
server-backed backends behave conventionally.

## Consequences

- ``Backend.execute_sql`` and ``Backend.transaction`` are now safe to
  call concurrently from multiple threads on the same
  :class:`~polars_db.connection.Connection`. The ``PostgresBackend``
  docstring's prior "thread-unsafe" warning is retired.
- ``Connection.close`` still releases every per-thread connection
  via ``close_all()``, so the user-visible lifecycle is unchanged.
- The new module ``polars_db/backends/_thread_local.py`` is a private
  implementation detail. Custom-backend authors who subclass
  ``Backend`` can use it themselves but are not required to — the
  base class makes no thread-safety guarantee for subclasses that
  manage their own connection state.
- Integration coverage lives in
  ``tests/integration/test_thread_safety.py`` — each thread does its
  own ``CREATE TABLE`` / ``INSERT`` / ``SELECT`` against a thread-
  namespaced table, so the suite works for both server-backed and
  ``:memory:`` backends.
- Memory cost grows linearly with the number of active threads
  touching a Connection. For applications spawning many short-lived
  worker threads, ``Connection.close`` (or eventual GC of the
  Connection itself, which triggers ``close``) is the cleanup point.
  A proper connection pool with idle-timeout eviction is left as
  future work — current usage patterns do not need it.
- This ADR closes the fourth and final TOCTOU site flagged in the
  2026-05-30 audit (ADR-0017 closed the first; ADR-0018 — schema
  cache TTL — and ADR-0013 update — SQL Server create-race — address
  the remaining two).
