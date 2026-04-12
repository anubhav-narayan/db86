from .threads import SqliteMultiThread
from typing import List
from threading import Lock


class Transaction:
    def __init__(self, name: str, connection: SqliteMultiThread):
        self.name = name.replace('"', '""')
        self.conn = connection
        self.active = False
        self.sp_list: List[str] = []
        self.sp_last = None
        self.__buffer: List[tuple[str, tuple, str | None]] = []
        self.__lock = Lock()
        self._READ_PREFIXES = (
            "SELECT",
            "PRAGMA",
            "EXPLAIN",
            "WITH",       # CTEs — may write via WITH...INSERT but rare; treat as read
            "ANALYZE",    # reads schema/stat tables, writes sqlite_stat* — borderline
            "ATTACH",     # read-only attach
            "DETACH",
            "VACUUM",     # read-only VACUUM INTO
        )
    
    def patch(self):
        self.__lock.acquire()
        self.__original_execute = self.conn.execute
        self.__original_select = self.conn.select
        self.__original_commit = self.conn.commit
        def patched_execute(req: str, arg=None, res=None):
            if req.lstrip().upper().startswith(self._READ_PREFIXES):
                self.flush()
                self.__original_execute(req, arg, res)
            else:
                self.__buffer.append((req, arg, res))
        def patched_select(req: str, arg=None) -> list[tuple]:
            self.flush()
            res = self.__original_select(req, arg)
            return res
        def patched_commit(blocking=True):
            self.flush()
        self.conn.execute = patched_execute
        self.conn.select = patched_select
        self.conn.commit = patched_commit
    
    def unpatch(self):
        self.conn.execute = self.__original_execute
        self.conn.select = self.__original_select
        self.conn.commit = self.__original_commit
        self.flush()
        self.__lock.release() 
    
    def flush(self):
        for req, arg, res in self.__buffer:
            self.__original_execute(req, arg, res)
        self.__buffer.clear()

    def begin(self):
        if self.active:
            raise RuntimeError("Transaction already active")
        self.conn.execute("BEGIN TRANSACTION;")
        self.savepoint(self.name)
        self.active = True

    def commit(self):
        if not self.active:
            raise RuntimeError("No active transaction")
        self.conn.execute("COMMIT;")
        self.conn.transaction_depth = 0
        self.active = False
    
    def savepoint(self, name: str = ""):
        sp_name = name.replace('"', '""') if name else self.name
        self.conn.transaction_depth += 1
        self.sp_list.append(sp_name)
        self.sp_last = sp_name
        self.conn.execute(f'SAVEPOINT "{sp_name}";')

    def rollback(self):
        self.conn.execute(f'ROLLBACK;')
        self.conn.transaction_depth = 0
        self.active = False
    
    def rollback_to(self, to: str):
        self.conn.execute(f'ROLLBACK TO SAVEPOINT "{to}";')
        self.sp_list = self.sp_list[:self.sp_list.index(to) + 1]
        self.conn.transaction_depth = len(self.sp_list)
        self.sp_last = self.sp_list[-1] if self.sp_list else self.name

    def release(self, from_: str = ""):
        target = from_ or self.name
        self.conn.execute(f'RELEASE SAVEPOINT "{target}";')
        self.sp_list = self.sp_list[self.sp_list.index(target):]
        self.conn.transaction_depth = len(self.sp_list)
        self.sp_last = self.sp_list[-1] if self.sp_list else self.name

    def __enter__(self):
        self.begin()
        self.patch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.__buffer.clear()
            self.rollback()
            self.unpatch()
            return False
        self.commit()
        self.unpatch()
        return False