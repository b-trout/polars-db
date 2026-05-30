"""SQL Server backend using native pymssql driver."""

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa
import sqlglot.expressions as exp

from polars_db.backends._thread_local import PerThreadConnections
from polars_db.backends.base import Backend
from polars_db.exceptions import BackendNotSupportedError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pymssql import Connection


_VALID_DB_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")

# SQL Server error codes used by the auto-create race resolution.
#
# 4060 / 4063 — "Cannot open database ..." Raised when the target
#        database does not exist (or the login lacks access). With
#        ``create_if_missing=True``, this signals that a competing
#        writer dropped the target between our master CREATE step and
#        our follow-up connect, so we retry the master step once.
#
# Note: pymssql's "race during CREATE DATABASE" surface is not a
# specific error code — empirically it shows up as either 1801 or the
# generic ``(0, b'Unknown error')`` DB-Lib wrapper, so
# :meth:`SQLServerBackend._ensure_database_exists` uses outcome-based
# detection (``DB_ID`` lookup) instead of matching error numbers.
_DB_NOT_FOUND_CODES = (4060, 4063)


def _validate_db_identifier(name: str) -> str:
    """Validate a SQL Server database identifier for safe DDL embedding.

    Only allows ``[A-Za-z_][A-Za-z0-9_]{0,127}`` to prevent T-SQL injection
    via a crafted connection string (e.g. ``foo]; DROP DATABASE master; --``
    which could otherwise break out of the bracketed identifier in the
    auto-create ``CREATE DATABASE [...]`` path).
    """
    if not _VALID_DB_NAME.fullmatch(name):
        msg = f"Invalid SQL Server database name: {name!r}"
        raise ValueError(msg)
    return name


def _mssql_error_code(exc: BaseException) -> int | None:
    """Return the SQL Server error code from a pymssql exception, if present.

    pymssql wraps the underlying DB-Lib error number in ``exc.args[0]``.
    """
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int):
        return args[0]
    return None


def _is_db_not_found(exc: BaseException) -> bool:
    """Whether *exc* is a SQL Server "database not found" (4060/4063) error."""
    return _mssql_error_code(exc) in _DB_NOT_FOUND_CODES


class SQLServerBackend(Backend):
    """SQL Server via native pymssql driver.

    Per-thread connection caching (ADR-0019) gives each thread its own
    pymssql connection so concurrent ``collect()`` calls cannot
    cross-contaminate cursor state.
    """

    def __init__(self, *, create_if_missing: bool = False) -> None:
        self._state = PerThreadConnections()
        self._create_if_missing = create_if_missing

    @property
    def dialect(self) -> str:
        return "tsql"

    def execute_sql(self, sql: str, conn_str: str) -> pa.Table:
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall() if columns else []
        # Per-statement commit normally preserves the historical
        # autocommit-like semantics. Inside a ``transaction()`` block
        # the outer context commits/rolls back as a whole, so skip the
        # per-statement commit to keep the snapshot intact.
        if not self._state.in_tx:
            conn.commit()
        if not columns:
            return pa.table({})

        col_data: dict[str, list[object]] = {c: [] for c in columns}
        for row in rows:
            for col_name, value in zip(columns, row, strict=True):
                col_data[col_name].append(value)

        return pa.table(col_data)

    @contextmanager
    def transaction(self, conn_str: str) -> Iterator[None]:
        """Open a SERIALIZABLE T-SQL transaction on the cached connection.

        SQL Server's default isolation is READ COMMITTED, which would let
        a concurrent INSERT slip in between JoinValidator and the main
        query. SERIALIZABLE provides snapshot stability for the duration
        of the block.

        Uses ``SAVE TRANSACTION`` instead of a nested ``BEGIN TRANSACTION``
        because T-SQL's ``ROLLBACK TRANSACTION`` (without a savepoint
        name) unconditionally unwinds to ``@@TRANCOUNT = 0``. pymssql
        runs in ``autocommit=False`` which keeps an implicit tx open
        across statements, so a bare rollback would wipe DDL/DML that
        had already been committed before this block was entered
        (verified empirically — see ADR-0017 revision). Rolling back to
        a savepoint unwinds only the body's changes, leaving the
        surrounding tx state intact.
        """
        conn = self._get_connection(conn_str)
        cursor = conn.cursor()
        try:
            # SQL Server's SET TRANSACTION ISOLATION LEVEL is a session
            # setting, not a per-tx one — it affects the currently-open
            # implicit tx as well as future ones, which is what we want.
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cursor.execute("SAVE TRANSACTION polars_db_sp")
        finally:
            cursor.close()
        self._state.in_tx = True
        try:
            yield
        except BaseException:
            cursor = conn.cursor()
            try:
                cursor.execute("ROLLBACK TRANSACTION polars_db_sp")
            finally:
                cursor.close()
            raise
        finally:
            self._state.in_tx = False
            # Flush the surrounding implicit tx — on success this
            # commits the body's writes; on failure it commits the
            # (now empty) tx state left after the savepoint rollback so
            # @@TRANCOUNT does not drift between transaction() calls.
            conn.commit()

    def _get_connection(self, conn_str: str) -> Connection:
        return self._state.get_or_create(conn_str, self._create_connection)

    def _create_connection(self, conn_str: str) -> Connection:
        import pymssql

        parsed = urlparse(conn_str)
        server = parsed.hostname or "localhost"
        port = str(parsed.port or 1433)
        user = parsed.username or "sa"
        password = parsed.password or ""
        database = _validate_db_identifier(parsed.path.lstrip("/"))

        if self._create_if_missing:
            self._ensure_database_exists(
                pymssql, server, port, user, password, database
            )

        try:
            return pymssql.connect(
                server=server,
                port=port,
                user=user,
                password=password,
                database=database,
            )
        except Exception as exc:
            # If the target DB vanished between our master-CREATE step
            # and this connect (a competing process dropped it), retry
            # the master step exactly once. With ``create_if_missing=False``
            # the original error is the right thing to surface.
            if not (self._create_if_missing and _is_db_not_found(exc)):
                raise
            self._ensure_database_exists(
                pymssql, server, port, user, password, database
            )
            return pymssql.connect(
                server=server,
                port=port,
                user=user,
                password=password,
                database=database,
            )

    @staticmethod
    def _ensure_database_exists(
        pymssql: object,
        server: str,
        port: str,
        user: str,
        password: str,
        database: str,
    ) -> None:
        """Idempotently create *database* via the master connection.

        The ``IF DB_ID(...) IS NULL CREATE DATABASE`` form is itself
        racy under concurrency: two callers can both see ``NULL`` and
        one will get an error from the engine. Empirically pymssql
        surfaces this race as either error 1801 ("database already
        exists") or the generic ``(0, b'Unknown error')`` DB-Lib
        wrapper — the latter has no programmable code so matching on
        error numbers is not reliable.

        We use outcome-based detection instead: if the CREATE step
        raises, query ``DB_ID`` again and proceed when the database
        now exists (some other writer won the race; the end state is
        what we wanted). Any other failure mode re-raises. See
        ADR-0013 for the full rationale.

        ``database`` is regex-restricted by :func:`_validate_db_identifier`,
        but apply T-SQL escaping as defence-in-depth: ``]`` -> ``]]``
        inside brackets and ``'`` -> ``''`` inside strings.
        """
        bracketed = database.replace("]", "]]")
        quoted = database.replace("'", "''")

        if SQLServerBackend._database_exists(
            pymssql, server, port, user, password, quoted
        ):
            return

        try:
            SQLServerBackend._issue_create_database(
                pymssql, server, port, user, password, quoted, bracketed
            )
        except Exception:
            if not SQLServerBackend._database_exists(
                pymssql, server, port, user, password, quoted
            ):
                raise

    @staticmethod
    def _database_exists(
        pymssql: object,
        server: str,
        port: str,
        user: str,
        password: str,
        quoted: str,
    ) -> bool:
        """Whether ``DB_ID(<database>)`` is non-NULL right now.

        Uses a dedicated short-lived master connection so a previous
        failed CREATE on a different connection cannot leave the
        cursor in a bad state.
        """
        master = pymssql.connect(  # type: ignore[attr-defined]
            server=server,
            port=port,
            user=user,
            password=password,
            database="master",
        )
        try:
            cursor = master.cursor()
            cursor.execute(f"SELECT DB_ID('{quoted}')")
            row = cursor.fetchone()
            return bool(row) and row[0] is not None
        finally:
            master.close()

    @staticmethod
    def _issue_create_database(
        pymssql: object,
        server: str,
        port: str,
        user: str,
        password: str,
        quoted: str,
        bracketed: str,
    ) -> None:
        """Run the IF DB_ID/CREATE DATABASE statement on a fresh master conn."""
        master = pymssql.connect(  # type: ignore[attr-defined]
            server=server,
            port=port,
            user=user,
            password=password,
            database="master",
        )
        try:
            master.autocommit(True)
            cursor = master.cursor()
            cursor.execute(
                f"IF DB_ID('{quoted}') IS NULL CREATE DATABASE [{bracketed}]"
            )
        finally:
            master.close()

    def render(self, ast: exp.Expression) -> str:
        """Render AST to T-SQL, adding OFFSET 0 ROWS to subquery ORDER BY.

        SQL Server forbids ORDER BY inside derived tables unless TOP or
        OFFSET is also present.  Walk the tree and patch any subquery
        whose inner SELECT has ORDER BY but no OFFSET/LIMIT.
        """
        for subquery in ast.find_all(exp.Subquery):
            inner = subquery.this
            if not isinstance(inner, exp.Select):
                continue
            has_order = inner.args.get("order") is not None
            has_limit = inner.args.get("limit") is not None
            has_offset = inner.args.get("offset") is not None
            if has_order and not has_limit and not has_offset:
                inner.set("offset", exp.Offset(expression=exp.Literal.number(0)))
        return ast.sql(dialect=self.dialect, pretty=True)

    def function_mapping(self) -> dict[str, str]:
        return {"string_agg": "STRING_AGG"}

    def current_schema_sql_expr(self) -> exp.Expression:
        """SQL Server uses ``SCHEMA_NAME()`` for the current default schema."""
        return exp.Anonymous(this="SCHEMA_NAME")

    def build_explain_sql(self, sql: str, *, analyze: bool = False) -> str:
        msg = (
            "SQL Server does not support EXPLAIN. "
            "Use SET SHOWPLAN_XML ON via execute_raw() as a workaround."
        )
        raise BackendNotSupportedError(msg)

    def close(self) -> None:
        self._state.close_all()
