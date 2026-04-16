"""
SqliteMultiThread
=================

Runs all SQLite operations through a single background thread.

- One thread owns the sqlite3.Connection; every query is queued to it.
- WAL mode lets multiple readers work safely at the OS level.
- Requests are typed dataclasses, dispatched with `match`.
- Results are returned via a lightweight queue/event slot.
- Errors are stored and re-raised on the next blocking call.
- Supports execute, executemany, select, select_one, commit, and close.
- `transaction_depth` tracks active transactions; autocommit only runs
  when depth is zero.
"""


from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from queue import SimpleQueue
from threading import Event, Thread
from typing import Any

from .logger import logging

log = logging.getLogger("db86.SqliteMultiThread")


# ---------------------------------------------------------------------------
# Request types
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _ExecReq:
    sql:    str
    args:   tuple
    result: SimpleQueue | None   # None → fire-and-forget


@dataclass(slots=True)
class _ManyReq:
    sql:  str
    rows: list[tuple]
    result: SimpleQueue | None


@dataclass(slots=True)
class _CommitReq:
    result: SimpleQueue | None   # None → non-blocking


@dataclass(slots=True)
class _CloseReq:
    result: SimpleQueue


_DONE = object()   # sentinel pushed into result queues


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class SqliteMultiThread(Thread):
    """
    Serialise all sqlite3 operations through one daemon thread.

    Public surface is identical to the original so all db86 callsites
    (Database, JSONStorage, Table, Transaction) work unchanged.
    """

    def __init__(self, filename: str, autocommit: bool,
                 journal_mode: str, timeout: float = 5.0):
        super().__init__(daemon=True, name=f"db86-sqlite-{filename}")
        self.filename         = filename
        self.autocommit       = autocommit
        self.journal_mode     = journal_mode
        self.timeout          = timeout
        self.transaction_depth = 0

        self._reqs: SimpleQueue = SimpleQueue()
        self._ready             = Event()
        self._error: BaseException | None = None

        self.start()
        if not self._ready.wait(timeout):
            err = self._error
            msg = f"Worker failed to start within {timeout}s"
            raise TimeoutError(msg) from err

        if self._error:
            raise self._error

    # ------------------------------------------------------------------
    # Worker thread body
    # ------------------------------------------------------------------

    def run(self) -> None:
        try:
            conn = sqlite3.connect(
                self.filename,
                isolation_level=None if self.autocommit else "",
                check_same_thread=False,
            )
            conn.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.text_factory = str
            if not self.autocommit:
                conn.commit()
        except BaseException as exc:
            self._error = exc
            self._ready.set()
            return

        self._ready.set()
        cur = conn.cursor()

        while True:
            req = self._reqs.get()
            log.debug(req)
            match req:
                case _CloseReq(result=q):
                    try:
                        conn.commit()
                        self.transaction_depth = 0
                    finally:
                        conn.close()
                    q.put(_DONE)
                    return

                case _CommitReq(result=q):
                    try:
                        conn.commit()
                    except BaseException as exc:
                        self._error = exc
                    if q is not None:
                        q.put(_DONE)

                case _ExecReq(sql=sql, args=args, result=q):
                    try:
                        cur.execute(sql, args)
                        if q is not None:
                            q.put(cur.fetchall())
                        elif self.autocommit and self.transaction_depth == 0:
                            conn.commit()
                    except BaseException as exc:
                        self._error = exc
                        log.error("sqlite error: %s | sql=%r args=%r",
                                  exc, sql, args)
                        if q is not None:
                            q.put(_DONE)

                case _ManyReq(sql=sql, rows=rows, result=q):
                    try:
                        cur.executemany(sql, rows)
                        if q is not None:
                            q.put(cur.fetchall())
                        elif self.autocommit and self.transaction_depth == 0:
                            conn.commit()
                    except BaseException as exc:
                        self._error = exc
                        log.error("sqlite executemany error: %s | sql=%r",
                                  exc, sql)
                        if q is not None:
                            q.put(_DONE)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _raise_if_error(self) -> None:
        if (exc := self._error) is not None:
            self._error = None
            raise exc

    def execute(self, sql: str, args: tuple | None = None,
                res: Any = None) -> None:
        """Non-blocking enqueue. Pass res=<SimpleQueue> to collect results."""
        self._raise_if_error()
        self._reqs.put(_ExecReq(sql=sql, args=args or (), result=res))

    def executemany(self, sql: str, items: list[tuple]) -> None:
        self._raise_if_error()
        q: SimpleQueue = SimpleQueue()
        self._reqs.put(_ManyReq(sql=sql, rows=list(items), result=q))
        q.get()
        self._raise_if_error()

    def select(self, sql: str, args: tuple | None = None) -> list:
        """Return all matching rows as a list (one round-trip)."""
        self._raise_if_error()
        q: SimpleQueue = SimpleQueue()
        self._reqs.put(_ExecReq(sql=sql, args=args or (), result=q))
        rows = q.get()
        self._raise_if_error()
        return rows if rows is not _DONE else []

    def select_one(self, sql: str, args: tuple | None = None) -> tuple | None:
        """Return first row or None."""
        rows = self.select(sql, args)
        return rows[0] if rows else None

    def commit(self, blocking: bool = True) -> None:
        q: SimpleQueue | None = SimpleQueue() if blocking else None
        self._reqs.put(_CommitReq(result=q))
        if blocking:
            q.get()   # type: ignore[union-attr]
            self._raise_if_error()

    def close(self, force: bool = False) -> None:
        q: SimpleQueue = SimpleQueue()
        self._reqs.put(_CloseReq(result=q))
        if not force:
            q.get()
            self.join()