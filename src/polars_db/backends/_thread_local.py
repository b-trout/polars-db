"""Per-thread connection cache shared by Backend implementations.

Backwards-compatible thread-safety primitive for the backend layer
(ADR-0019). Each thread that calls into a backend gets its own driver
connection so concurrent ``collect()`` from different threads can no
longer race on a shared cursor and cross-contaminate result sets. The
cache transparently re-creates the connection when the connection
string changes mid-thread, and ``close_all()`` drops every tracked
connection across every thread so :meth:`Connection.close` still
releases resources deterministically.
"""

from __future__ import annotations

import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class PerThreadConnections:
    """Per-thread DB connection + tx flag store.

    Each backend instantiates one of these and reads / writes the
    thread-local ``conn`` / ``in_tx`` slots instead of plain instance
    attributes. The store also tracks every connection it ever opened
    so :meth:`close_all` can drop them all on ``Backend.close()``
    regardless of which thread is doing the close.

    .. note::
        Connections opened from threads that subsequently die remain in
        the tracker until ``close_all`` runs. Long-lived applications
        that spawn many short-lived threads should ``close()`` the
        :class:`~polars_db.connection.Connection` (or the underlying
        backend) periodically; for the typical "thread pool of N
        workers, all long-lived" case there is no leak.
    """

    def __init__(self) -> None:
        self._tls = threading.local()
        self._lock = threading.Lock()
        self._tracker: list[object] = []

    # -- connection slot -----------------------------------------------------

    def get_or_create(self, conn_str: str, factory: Callable[[str], Any]) -> Any:
        """Return the calling thread's cached connection, opening one if needed.

        If the cached connection's ``conn_str`` differs from *conn_str*,
        the cached one is closed first (matching the historical
        re-connect-on-change semantics) and a fresh one is opened via
        *factory*.
        """
        cached_conn = getattr(self._tls, "conn", None)
        cached_str = getattr(self._tls, "conn_str", None)
        if cached_conn is not None and cached_str == conn_str:
            return cached_conn

        if cached_conn is not None:
            self._safe_close(cached_conn)
            self._untrack(cached_conn)
            self._tls.conn = None
            self._tls.conn_str = None
            self._tls.in_tx = False

        new_conn = factory(conn_str)
        self._tls.conn = new_conn
        self._tls.conn_str = conn_str
        self._tls.in_tx = False
        with self._lock:
            self._tracker.append(new_conn)
        return new_conn

    @property
    def conn(self) -> Any:
        """Return the calling thread's cached connection, or ``None``."""
        return getattr(self._tls, "conn", None)

    @property
    def conn_str(self) -> str | None:
        """Return the connection string the cached conn was opened with."""
        return getattr(self._tls, "conn_str", None)

    # -- transaction flag ----------------------------------------------------

    @property
    def in_tx(self) -> bool:
        """Whether the calling thread is currently inside ``transaction()``."""
        return getattr(self._tls, "in_tx", False)

    @in_tx.setter
    def in_tx(self, value: bool) -> None:
        self._tls.in_tx = value

    # -- shutdown ------------------------------------------------------------

    def close_all(self) -> None:
        """Close every tracked connection across every thread.

        Called from ``Backend.close()``. After this returns the
        tracker is empty and any subsequent ``get_or_create`` from any
        thread opens a fresh connection.
        """
        # Drop the calling thread's TLS slot first so a re-entrant
        # get_or_create after close_all does not see a half-closed conn.
        self._tls.__dict__.pop("conn", None)
        self._tls.__dict__.pop("conn_str", None)
        self._tls.__dict__.pop("in_tx", None)

        with self._lock:
            connections = list(self._tracker)
            self._tracker.clear()
        for c in connections:
            self._safe_close(c)

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _safe_close(conn: object) -> None:
        """Close *conn* swallowing driver-specific shutdown errors.

        Driver ``close()`` methods occasionally raise when the
        connection is already in a bad state (e.g. server gone away).
        We are doing best-effort shutdown here; raising would mask the
        original close-caller's intent.
        """
        close = getattr(conn, "close", None)
        if not callable(close):
            return
        with suppress(Exception):
            close()

    def _untrack(self, conn: object) -> None:
        with self._lock, suppress(ValueError):
            self._tracker.remove(conn)
