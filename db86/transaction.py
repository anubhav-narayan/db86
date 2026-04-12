from .threads import SqliteMultiThread
from typing import List
from contextvars import ContextVar
from .logger import logger


# Global ContextVar for transaction state
tx_context: ContextVar["TxContext | None"] = ContextVar("tx_context", default=None)


class TxContext:
    def __init__(self, tx: "Transaction"):
        self.tx = tx
        self.active = False
        self.execute = None
        self.select = None
        self.commit = None


class Transaction:
    def __init__(self, name: str, connection: SqliteMultiThread):
        self.name = name.replace('"', '""')
        self.conn = connection
        self.active = False
        self.sp_list: List[str] = []
        self.sp_last = None
        self.__buffer: List[tuple[str, tuple, str | None]] = []
        self._READ_PREFIXES = (
            "SELECT", "PRAGMA", "EXPLAIN", "WITH",
            "ANALYZE", "ATTACH", "DETACH", "VACUUM",
        )

    def patch(self):
        self.__original_execute = self.conn.execute
        self.__original_select = self.conn.select
        self.__original_commit = self.conn.commit

        ctx = TxContext(self)

        def patched_execute(req: str, arg=None, res=None):
            logger.debug(f"Intercepted execute: {req} with arg: {arg} and res: {res}")
            if req.lstrip().upper().startswith(self._READ_PREFIXES):
                self.flush()
                self.__original_execute(req, arg, res)
            else:
                self.__buffer.append((req, arg, res))

        def patched_select(req: str, arg=None):
            logger.debug(f"Intercepted select: {req} with arg: {arg}")
            self.flush()
            return self.__original_select(req, arg)

        def patched_commit(blocking=True):
            self.flush()

        ctx.execute = patched_execute
        ctx.select = patched_select
        ctx.commit = patched_commit
        ctx.active = True

        # Set context for this transaction
        tx_context.set(ctx)

        # Install dispatcher once on conn — idempotent
        if not getattr(self.conn, "_dispatching", False):
            _orig_execute = self.conn.execute
            _orig_select = self.conn.select
            _orig_commit = self.conn.commit

            def dispatch_execute(req, arg=None, res=None):
                ctx = tx_context.get()
                if ctx and ctx.active and ctx.execute:
                    return ctx.execute(req, arg, res)
                return _orig_execute(req, arg, res)

            def dispatch_select(req, arg=None):
                ctx = tx_context.get()
                if ctx and ctx.active and ctx.select:
                    return ctx.select(req, arg)
                return _orig_select(req, arg)

            def dispatch_commit(blocking=True):
                ctx = tx_context.get()
                if ctx and ctx.active and ctx.commit:
                    return ctx.commit(blocking)
                return _orig_commit(blocking)

            self.conn.execute = dispatch_execute
            self.conn.select = dispatch_select
            self.conn.commit = dispatch_commit
            self.conn._dispatching = True

    def unpatch(self):
        # Reset context to None
        tx_context.set(None)
        self.flush()

    def flush(self):
        for req, arg, res in self.__buffer:
            logger.debug(f"Flushing buffered query: {req} with arg: {arg}")
            self.__original_execute(req, arg, res)
        self.__buffer.clear()

    def begin(self):
        if self.active:
            raise RuntimeError("Transaction already active")
        logger.debug(f"Transaction '{self.name}' started.")
        self.conn.execute("BEGIN TRANSACTION;")
        self.savepoint(self.name)
        self.active = True

    def commit(self):
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.execute("COMMIT;")
        self.conn.transaction_depth = 0
        self.active = False
        logger.debug(f"Transaction '{self.name}' committed.")

    def savepoint(self, name: str = ""):
        sp_name = name.replace('"', '""') if name else self.name
        self.conn.transaction_depth += 1
        self.sp_list.append(sp_name)
        self.sp_last = sp_name
        self.conn.execute(f'SAVEPOINT "{sp_name}";')
        logger.debug(f"Savepoint '{sp_name}' created.")

    def rollback(self):
        self.conn.execute("ROLLBACK;")
        self.conn.transaction_depth = 0
        self.active = False
        logger.debug(f"Transaction '{self.name}' rolled back.")

    def rollback_to(self, to: str):
        self.conn.execute(f'ROLLBACK TO SAVEPOINT "{to}";')
        self.sp_list = self.sp_list[: self.sp_list.index(to) + 1]
        self.conn.transaction_depth = len(self.sp_list)
        self.sp_last = self.sp_list[-1] if self.sp_list else self.name
        logger.debug(f"Rolled back to savepoint '{to}'.")

    def release(self, from_: str = ""):
        target = from_ or self.name
        self.conn.execute(f'RELEASE SAVEPOINT "{target}";')
        self.sp_list = self.sp_list[self.sp_list.index(target):]
        self.conn.transaction_depth = len(self.sp_list)
        self.sp_last = self.sp_list[-1] if self.sp_list else self.name
        logger.debug(f"Released savepoint '{target}'.")

    def __enter__(self):
        self.begin()
        self.patch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.__buffer.clear()
            self.rollback()
        else:
            self.commit()
        self.unpatch()
        return False
