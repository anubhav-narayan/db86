"""
NoSQLite3 Database Class
"""
import os
from collections import UserDict
from .threads import SqliteMultiThread
from .logger import logger
from .storages import Table, JSONStorage


class Database(UserDict):
    """
    Initialize a thread-safe SQLite3 Database. The dictionary will
    be a database file `filename` containing multiple tables.
    This class provides an upper level hierarchy of the SqliteDict
    by using a similar structure, modifications are limited.

    If no `filename` is given, the database is in memory.

    If you enable `autocommit`, changes will be committed after each
    operation (more inefficient but safer). Otherwise, changes are
    committed on `self.commit()`, `self.clear()` and `self.close()`.

    Set `journal_mode` to 'OFF' if you're experiencing sqlite I/O problems
    or if you need performance and don't care about crash-consistency.

    The `flag` parameter. Exactly one of:
      'c': default mode, open for read/write, creating the db if necessary.
      'w': open for r/w, but drop contents first (start with empty table)
      'r': open as read-only

    The `timeout` defines the maximum time (in seconds) to wait for
    initial Thread startup.
    """
    VALID_FLAGS = ['c', 'r', 'w']

    def __init__(self, filename=':memory:', flag='c',
                 autocommit=False, journal_mode="DELETE", timeout=5):
        """Open (or create) a thread-safe SQLite3 database.

        Parameters
        ----------
        filename : str
            Path to the database file, or ``':memory:'`` for an
            in-memory database.
        flag : str
            ``'c'`` (default) – open for read/write, create if needed.
            ``'w'`` – open for read/write, dropping existing data.
            ``'r'`` – open read-only.
        autocommit : bool
            Commit after every write when ``True``.
        journal_mode : str
            SQLite journal mode pragma.
        timeout : float
            Seconds to wait for the worker thread to start.

        Raises
        ------
        RuntimeError
            If *flag* is invalid or the parent directory does not exist.
        """
        if flag not in Database.VALID_FLAGS:
            raise RuntimeError(f"Unrecognized flag: {flag}")
        self.flag = flag
        if flag == 'w':
            if os.path.exists(filename):
                os.remove(filename)
        dir_ = os.path.dirname(filename)
        if dir_:
            if not os.path.exists(dir_):
                raise RuntimeError(
                    f'Error! The directory does not exist, {dir_}'
                )
        self.filename = filename if filename == ':memory:'\
            else os.path.abspath(filename)
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.timeout = timeout
        self.conn = self.__connect()

    def __connect(self):
        """Create and return a new ``SqliteMultiThread`` connection."""
        return SqliteMultiThread(self.filename, autocommit=self.autocommit,
                                 journal_mode=self.journal_mode,
                                 timeout=self.timeout)

    def __enter__(self):
        """Enter the context manager, reconnecting if necessary."""
        if not hasattr(self, 'conn') or self.conn is None:
            self.conn = self._new_conn()
        return self

    def __exit__(self, *exc_info):
        """Exit the context manager and close the database."""
        self.close()

    def __str__(self):
        """Return a human-readable string representation of the database."""
        return f'Database: {self.filename}'

    def __getitem__(self, *args):
        """Retrieve a storage by name or by ``(name, type)`` tuple.

        Parameters
        ----------
        args
            A single string returns a ``JSONStorage``.
            A ``(name, 'table')`` tuple returns a ``Table``.
            A ``(name, 'json')`` tuple returns a ``JSONStorage``.

        Returns
        -------
        Table | JSONStorage
        """
        if type(args[0]) == str:  # Single arg
            table_name = args[0]
            return JSONStorage(table_name, self.conn, self.flag)
        elif type(args[0]) == tuple:  # Type arg
            table_name = args[0][0]
            astype = args[0][1]
            if astype == 'table':
                return Table(table_name, self.conn, self.flag)
            elif astype == 'json':
                return JSONStorage(table_name, self.conn, self.flag)

    def __setitem__(self, key, item):
        """Copy a storage schema into this database under a new *key*.

        Only the schema is copied, not the data.  If *item* belongs to a
        different database file its CREATE statement is replayed here.
        """
        if type(item) is Table or JSONStorage:
            # Only copy schema not data
            if item.filename is not self.filename:
                COPY = item.xschema['sql']
                key = key.replace('"', '""')
                COPY.replace(f'{item.name}', f'{key}')
                self.conn.execute(COPY)
            elif item.name not in self:
                # Fancy rename without data
                COPY = item.xschema['sql']
                key = key.replace('"', '""')
                COPY.replace(f'{item.name}', f'{key}')
                self.conn.execute(COPY)

    def __repr__(self):
        """Return the formal string representation (same as ``__str__``)."""
        return self.__str__()

    def describe(self):
        """Return a formatted table of all objects in ``sqlite_master``.

        Returns
        -------
        str
            Grid-formatted table produced by ``tabulate``.
        """
        GET_ALL = 'SELECT * FROM sqlite_master\
                   ORDER BY rowid'
        head = ['type', 'name', 'tbl_name', 'rootpage']
        items = self.conn.select(GET_ALL)
        from tabulate import tabulate
        return tabulate([x[:4] for x in items], head, tablefmt='grid')

    def __iter__(self):
        """Iterate over all table names in the database."""
        GET_TABLES = 'SELECT name FROM sqlite_master WHERE type="table"\
                      ORDER BY rowid'
        for key in self.conn.select(GET_TABLES):
            yield key[0]
    
    def __len__(self):
        """Return the number of tables in the database."""
        GET_TABLES = 'SELECT COUNT(rowid) FROM sqlite_master WHERE type="table"\
                      ORDER BY rowid'
        return self.conn.select_one(GET_TABLES)[0]

    def __contains__(self, name):
        """Check whether a table or storage named *name* exists."""
        HAS_ITEM = 'SELECT 1 FROM sqlite_master WHERE name = ?'
        return self.conn.select_one(HAS_ITEM, (name,)) is not None

    @property
    def storages(self):
        """Return a list of all table (storage) names in the database."""
        return [x for x in self.keys()]

    @property
    def indices(self):
        """Return a list of all index names in the database."""
        GET_INDEX = 'SELECT name FROM sqlite_master WHERE type="index"\
                     ORDER BY rowid'
        items = self.conn.select(GET_INDEX)
        return [x[0] for x in items]

    @property
    def views(self):
        """Return a list of all view names in the database."""
        GET_VIEW = 'SELECT name FROM sqlite_master WHERE type="view"\
                    ORDER BY rowid'
        items = self.conn.select(GET_VIEW)
        return [x[0] for x in items]

    def close(self, do_log=True, force=False):
        """Close the database connection and release resources.

        Parameters
        ----------
        do_log : bool
            Emit a debug log message when ``True`` (default).
        force : bool
            When ``True``, skip the final blocking commit and force-close
            the underlying thread.
        """
        if do_log:
            logger.debug(f"Closing {self}")
        if hasattr(self, 'conn') and self.conn is not None:
            if self.conn.autocommit and not force and self.conn.transaction_depth == 0:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit(blocking=True)
            self.conn.close(force=force)
            self.conn = None

    def __delitem__(self, table_name: str):
        """Drop the table *table_name* from the database.

        Raises
        ------
        RuntimeError
            If the database was opened in read-only mode.
        """
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        DEL_ITEM = f'DROP TABLE "{table_name}"'
        self.conn.execute(DEL_ITEM)
        if self.conn.autocommit and self.conn.transaction_depth == 0:
            self.conn.commit()
