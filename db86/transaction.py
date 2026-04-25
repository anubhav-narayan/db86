"""
Transaction — rewritten for the new SqliteMultiThread
======================================================
The old implementation monkey-patched conn.execute/select/commit with
ContextVar-based dispatchers and buffered writes in Python.  That was
needed when the thread model required special coordination.

The new SqliteMultiThread already serialises every operation through
one worker queue, so transactions just need to:

  1. Increment/decrement conn.transaction_depth so the worker skips
     its own auto-commit while we own the transaction.
  2. Send BEGIN / SAVEPOINT / COMMIT / ROLLBACK as plain SQL through
     the normal execute() path — no monkey-patching, no buffer, no
     ContextVar.`

Savepoints are used for nesting; the outermost BEGIN is the real
transaction boundary.
"""

from __future__ import annotations

import time
from typing import Literal

from .threads import SqliteMultiThread
from .logger import logger


class Transaction:
    def __init__(
        self,
        name: str,
        connection: SqliteMultiThread,
        tx_type: Literal["TRANSACTION", "IMMEDIATE", "EXCLUSIVE"] = "TRANSACTION",
        retries: int = 5,
        retry_delay: float = 0.1,
    ):
        """Create a new transaction handle.

        Parameters
        ----------
        name : str
            Human-readable name used in log messages and savepoints.
        connection : SqliteMultiThread
            The worker-thread connection that serialises all SQL.
        tx_type : str
            SQLite ``BEGIN`` variant: ``'TRANSACTION'``, ``'IMMEDIATE'``,
            or ``'EXCLUSIVE'``.
        retries : int
            Number of attempts when the database is locked.
        retry_delay : float
            Seconds to sleep between lock-retry attempts.
        """
        self.name        = name.replace('"', '""')
        self.conn        = connection
        self.tx_type     = tx_type
        self.retries     = retries
        self.retry_delay = retry_delay
        self.active      = False
        self._savepoints: list[str] = []

    # ------------------------------------------------------------------
    # Core ops — all go straight through conn.execute()
    # ------------------------------------------------------------------

    def begin(self) -> None:
        """Start the transaction, retrying on ``database is locked``.

        Raises
        ------
        RuntimeError
            If the transaction is already active or all retries fail.
        """
        if self.active:
            raise RuntimeError("Transaction already active")
        for attempt in range(self.retries):
            try:
                self.conn.execute(f"BEGIN {self.tx_type}")
                self.conn.transaction_depth += 1
                self.active = True
                self._savepoints.clear()
                logger.debug("Transaction '%s' begun.", self.name)
                return
            except Exception as exc:
                if "locked" in str(exc).lower() and attempt < self.retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
        raise RuntimeError(
            f"Transaction '{self.name}' failed to start after {self.retries} retries"
        )

    def commit(self) -> None:
        """Commit the transaction and release all savepoints.

        Raises
        ------
        RuntimeError
            If no transaction is currently active.
        """
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.execute("COMMIT")
        self.conn.transaction_depth = 0
        self.active = False
        self._savepoints.clear()
        logger.debug("Transaction '%s' committed.", self.name)

    def rollback(self) -> None:
        """Roll back the entire transaction and discard all savepoints.

        Raises
        ------
        RuntimeError
            If no transaction is currently active.
        """
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.execute("ROLLBACK")
        self.conn.transaction_depth = 0
        self.active = False
        self._savepoints.clear()
        logger.debug("Transaction '%s' rolled back.", self.name)

    # ------------------------------------------------------------------
    # Savepoints
    # ------------------------------------------------------------------

    def savepoint(self, name: str) -> None:
        """Create a named savepoint inside the active transaction.

        Parameters
        ----------
        name : str
            Name for the savepoint (will be SQL-escaped).
        """
        sp = name.replace('"', '""')
        self.conn.execute(f'SAVEPOINT "{sp}"')
        self._savepoints.append(sp)
        self.conn.transaction_depth += 1
        logger.debug("Savepoint '%s' created.", sp)

    def release(self, name: str) -> None:
        """Release (commit) a named savepoint and all savepoints after it.

        Parameters
        ----------
        name : str
            Name of the savepoint to release.
        """
        sp = name.replace('"', '""')
        self.conn.execute(f'RELEASE SAVEPOINT "{sp}"')
        if sp in self._savepoints:
            idx = self._savepoints.index(sp)
            released = self._savepoints[idx:]
            self._savepoints = self._savepoints[:idx]
            self.conn.transaction_depth = max(
                0, self.conn.transaction_depth - len(released)
            )
        logger.debug("Savepoint '%s' released.", sp)

    def rollback_to(self, name: str) -> None:
        """Roll back to a previously created savepoint without ending it.

        Parameters
        ----------
        name : str
            Name of the savepoint to roll back to.
        """
        sp = name.replace('"', '""')
        self.conn.execute(f'ROLLBACK TO SAVEPOINT "{sp}"')
        if sp in self._savepoints:
            idx = self._savepoints.index(sp)
            self._savepoints = self._savepoints[: idx + 1]
            self.conn.transaction_depth = max(1, idx + 2)  # BEGIN + savepoints up to sp
        logger.debug("Rolled back to savepoint '%s'.", sp)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "Transaction":
        """Enter the context manager and begin the transaction."""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit the context manager; roll back on exception, commit otherwise."""
        if exc_type and self.active:
            self.rollback()
        else:
            self.commit()
        return False