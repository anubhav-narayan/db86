"""
Connector classes for SQLite collections, tables, JSON storage, and views.

This module provides Pythonic, dict-like wrappers around SQLite structures,
making them easier to use in applications that need lightweight persistence
or hybrid relational/document storage.

Classes:
    Table:
        Wraps a standard SQLite table as a `UserDict`.
        Supports dict-like access, insertion via tuples or dicts,
        column operations, slicing, filtering, and schema inspection.

    JSONStorage:
        Stores JSON-serializable Python dictionaries in SQLite.
        Provides dict-like access, recursive merging, path-based queries,
        and export to dict/JSON.

    TableView:
        Wraps a SQLite view as a `UserDict`.
        Allows dict-like querying of views, including slices, column
        selection, and filtering.
"""
from typing import Any, Dict, Optional, Union, List

from .threads import SqliteMultiThread
from collections import UserDict


class Table(UserDict):
    """
    Connector Class for a SQLite Standard Table as UserDict

    Usage:
        db = Database()
        tab = db['some_tab']
    """

    def __init__(self, name: str, connection: SqliteMultiThread, flag: str,
                 primary_key_dtype: str = 'TEXT'):
        self.__conn = connection
        self.flag = flag
        self.name = name.replace('"', '""')
        self.filename = self.__conn.filename
        # Check for the table or create new with
        # two columns named key(Primary Key) and
        # col1
        GET_ITEM = 'SELECT name FROM sqlite_master WHERE name = ?'
        item = self.__conn.select_one(GET_ITEM, (name,))
        if item is None:
            MAKE_TABLE = f'''\
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "key" {primary_key_dtype} PRIMARY KEY,
                "col1" TEXT
            )
            '''
            self.__conn.execute(MAKE_TABLE)
            self.__conn.commit()

    def describe(self) -> str:
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        head = ['cid', 'name', 'type', 'notnull', 'default', 'primary key']
        from tabulate import tabulate
        return tabulate([x for x in data], head, tablefmt='grid')

    @property
    def xschema(self) -> dict:
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        schema = self.__conn.select_one(GET_SQL)[0]
        return {
            'name': self.name,
            'cols': self.columns,
            'sql': schema
        }

    @property
    def columns(self) -> list:
        '''
        Return s a list of column names
        '''
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        return [x[1] for x in data]

    def __enter__(self):
        if not hasattr(self, 'conn') or self.conn is None:
            raise RuntimeError('Instance not connected')
        return self

    def __exit__(self, *exc_info):
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def __repr__(self):
        return f'Table: {self.name}'

    def __setitem__(self, key, value):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write in read-only mode')

        if type(value) == tuple:
            if len(value) != len(self.columns) - 1:
                raise ValueError("Incorrect number of values")
            data = [key]
            data += [x for x in value]
            ADD_ITEM = f'REPLACE INTO "{self.name}" {tuple(self.columns)}'\
                + f' VALUES ({", ".join(["?" for x in data])})'
        elif type(value) == dict:
            if key not in self:
                value[self.columns[0]] = key
                refs = [x for x in value.keys()]
                data = [x for x in value.values()]
                ADD_ITEM = f'REPLACE INTO "{self.name}" {tuple(refs)}'\
                    + f' VALUES ({", ".join(["?" for x in data])})'
            else:
                data = [f'{x} = ?' for x in value]
                ADD_ITEM = f'UPDATE "{self.name}" SET {", ".join(data)}'\
                    + f' WHERE {self.columns[0]} = ?'
                data = [x for x in value.values()]
                data.append(key)
        else:
            raise TypeError("Incorrect value format, use tuple or dict")
        self.__conn.execute(ADD_ITEM, tuple(data))
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def get_idx(self, idx):
        '''
        Get a single value by `rowid`
        '''
        GET_ITEM = f'SELECT "_rowid_", * FROM "{self.name}" WHERE "_rowid_" = ?'
        item = self.__conn.select_one(GET_ITEM, (idx, ))
        if item is None:
            raise KeyError(idx)
        return item

    def get_slice(self, slc):
        '''
        Get a range of values by range
        '''
        GET_ITEM = f'SELECT * FROM "{self.name}"'\
            + f'WHERE "_rowid_" BETWEEN ? AND ?'
        item = self.__conn.select(GET_ITEM, ((slc.start), (slc.stop-1)))
        if item is None:
            raise KeyError(slc[0])
        return [x for x in item][::slc.step]

    def get_col(self, col):
        GET_ITEM = f'SELECT "{col}" FROM "{self.name}"'\
                 + 'ORDER BY rowid'
        item = self.__conn.select(GET_ITEM)
        return [x[0] for x in item]

    def get_col_sel(self, col, idx):
        cols = ', '.join([x for x in col])
        GET_ITEM = f'SELECT {cols} FROM "{self.name}"'\
            + f'WHERE "{self.columns[0]}" = ?'\
            + 'ORDER BY rowid'
        item = self.__conn.select_one(GET_ITEM, (idx, ))
        if item is None:
            raise KeyError(idx)
        return item

    def get_col_filt(self, col, slc):
        """
        Get data filtered by column
        """
        if slc.stop is None:
            GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                     + f'"{col}" >= {slc.start} ORDER BY _rowid_'
            item = self.__conn.select(GET_ITEM)
            if item is None:
                raise KeyError("No entry for given condition")
            return [x for x in item][::slc.step]
        elif slc.start is None:
            GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                     + f'"{col}" < {slc.stop} ORDER BY _rowid_'
            item = self.__conn.select(GET_ITEM)
            if item is None:
                raise KeyError("No entry for given condition")
            return [x for x in item][::slc.step]
        else:
            GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                     + f'"{col}" BETWEEN {slc.start} '\
                     + f'AND {slc.stop-1} ORDER BY _rowid_'
            item = self.__conn.select(GET_ITEM)
            if item is None:
                raise KeyError("No entry for given condition")
            return [x for x in item][::slc.step]

    def __getitem__(self, args):
        # args is key
        if type(args) is not tuple:
            if isinstance(args, slice):
                return self.get_slice(args)
            elif isinstance(args, int):
                return self.get_idx(args)
            elif isinstance(args, str) and args in self.columns:
                return self.get_col(args)
            elif isinstance(args, str) and args in self.keys():
                GET_ITEM = f'SELECT * FROM "{self.name}" WHERE "{self.columns[0]}" = ? ORDER BY rowid'
                item = self.__conn.select_one(GET_ITEM, (args,))
                if item is None:
                    raise KeyError(args)
                return item
            else:
                raise KeyError(args)
        # column select
        if len(args) >= 2:
            if args[0] in self:
                return self.get_col_sel(args[1:], args[0])
            elif args[0] in self.columns:
                if isinstance(args[1], slice):
                    return self.get_col_filt(args[0], args[1])
                else:
                    GET_ITEM = f'SELECT * FROM "{self.name}" WHERE '\
                             + f'"{args[0]}" = ?'
                    item = self.__conn.select(GET_ITEM, (args[1], ))
                    if item is None:
                        raise KeyError("No Entry for given condition")
                    return [x for x in item]

    def __contains__(self, key):
        HAS_ITEM = f'SELECT 1 FROM "{self.name}" WHERE "{self.columns[0]}" = ?'
        return self.__conn.select_one(HAS_ITEM, (key,)) is not None

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')
        if key not in self:
            raise KeyError(key)
        DEL_ITEM = f'DELETE FROM "{self.name}" WHERE {self.columns[0]} = ?'
        self.__conn.execute(DEL_ITEM, (key,))
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def rename(self):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

    def __iter__(self):
        GET_KEYS = f'SELECT "{self.columns[0]}" FROM "{self.name}"\
                     ORDER BY rowid'
        for x in self.__conn.select(GET_KEYS):
            yield x[0]

    def add_foreign_key(self, colname: str, references: str):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        FRN_KEY = f'ALTER TABLE "{self.name}" ADD COLUMN "{colname}" TEXT'\
            + f' REFERENCES "{references}"'
        self.__conn.execute(FRN_KEY)
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def add_column(self, colname: str, dtype: str = 'TEXT'):
        '''
        Simply add a new column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        NEW_COL = f'ALTER TABLE "{self.name}" ADD COLUMN "{colname}" {dtype}'
        self.__conn.execute(NEW_COL)
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def drop_column(self, colname: str):
        '''
        Drop a Column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        DROP_COL = f'ALTER TABLE "{self.name}" DROP COLUMN "{colname}"'
        self.__conn.execute(DROP_COL)
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def rename_column(self, colname: str, new_colname: str,
                      dtype: str = 'TEXT'):
        '''
        Rename a column
        '''
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')

        REM_COL = f'ALTER TABLE "{self.name}"\
         RENAME COLUMN "{colname}" TO "{new_colname}"'
        self.__conn.execute(REM_COL)
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def __len__(self):
        GET_LEN = f'SELECT COUNT(*) FROM "{self.name}"'
        rows = self.__conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def commit(self, blocking=True):
        '''
        From sqlitedict
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        '''
        if self.__conn is not None:
            self.__conn.commit(blocking)

    def to_dict(self, otype: str = 'column'):
        '''
        Return the Table is a dict
        otype: str - dict format to use
        '''
        cols = self.columns
        ret_dict = {}
        # Much faster
        if otype == 'column':
            for x in cols:
                ret_dict[x] = self[x]
            return ret_dict
        elif otype == 'list':
            ret_dict[self.name] = []
            temp_dict = {}
            for x in cols:
                temp_dict[x] = self[x]
            for x in range(0, len(temp_dict[cols[0]])):
                _add_temp = {}
                for y in cols:
                    _add_temp[y] = temp_dict[y][x]
                ret_dict[self.name].append(_add_temp)
            return ret_dict
        # Very VERY Slow
        # But true dict
        elif otype == 'dict':
            for x in self:
                ret_dict[x] = {
                    k: v for k, v in zip(cols[1:], self[x][1:])
                }
            return ret_dict
        else:
            raise TypeError(
                'Please use \'column\', \'list\' or \'dict\' as otype'
            )

    def to_sql(self) -> str:
        '''
        Returns the table in SQLite Syntax
        '''
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        CREATE = self.__conn.select_one(GET_SQL)[0]
        if CREATE.find('IF NOT EXISTS') == -1:
            idx = CREATE.find("TABLE")
            CREATE = CREATE[:idx+5] + ' IF NOT EXISTS ' + CREATE[idx+5:]
        VALUES = ',\n'.join([str(self[x]) for x in self.keys()])
        INSERT = f'INSERT INTO "{self.name}" VALUES\n' + VALUES
        return f'{CREATE};\n{INSERT};'


class JSONStorage(UserDict):
    def __init__(self, name: str, connection: SqliteMultiThread, flag: str,
                 primary_key_dtype: str = 'TEXT'):
        self.__conn = connection
        self.flag = flag
        self.name = name.replace('"', '""')
        # Check for the table or create new with
        # two columns named key(Primary Key) and
        # object
        GET_ITEM = 'SELECT name FROM sqlite_master WHERE name = ?;'
        item = self.__conn.select_one(GET_ITEM, (name,))
        if item is None:
            MAKE_TABLE = f'''\
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "key" {primary_key_dtype} PRIMARY KEY,
                "object" TEXT
            );
            '''
            self.__conn.execute(MAKE_TABLE)
            self.__conn.commit(False)

    def describe(self):
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        head = ['cid', 'name', 'type', 'notnull', 'default', 'primary key']
        from tabulate import tabulate
        return tabulate([x for x in data], head, tablefmt='grid')

    @property
    def xschema(self):
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        schema = self.__conn.select_one(GET_SQL)[0]
        return {
            'name': self.name,
            'cols': self.columns,
            'sql': schema
        }

    def __enter__(self):
        if not hasattr(self, 'conn') or self.conn is None:
            raise RuntimeError('Instance not connected')
        return self

    def __exit__(self, *exc_info):
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def __repr__(self):
        return f'JSON Storage: {self.name}'

    def __len__(self):
        GET_LEN = f'SELECT COUNT(rowid) FROM "{self.name}"'
        rows = self.__conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __contains__(self, key):
        HAS_ITEM = f'SELECT 1 FROM "{self.name}" WHERE "key" = ?'
        return self.__conn.select_one(HAS_ITEM, (key,)) is not None

    def __iter__(self):
        GET_KEYS = f'SELECT "key" FROM "{self.name}" ORDER BY rowid'
        for x in self.__conn.select(GET_KEYS):
            yield x[0]

    def __setitem__(self, key, value: dict):
        if self.flag == 'r':
            raise RuntimeError('Refusing to write in read-only mode')

        if "/" in key:
            self.set_path(key, value)
            return

        if type(value) == dict:
            import json
            if key not in self:
                data = (key, json.dumps(value))
                ADD_ITEM = f'REPLACE INTO "{self.name}"\
                 ("key", "object") VALUES (?, ?)'
            else:
                data = (json.dumps(value), key)
                ADD_ITEM = f'UPDATE "{self.name}"\
                 SET "object" = ? WHERE "key" = ?'
        else:
            raise TypeError("Incorrect value format, use dict")
        self.__conn.execute(ADD_ITEM, data)
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def __getitem__(self, key):
        import json
        if "/" in key:
            return [value for value in self.get_path(key)][0]
        GET_ITEM = f'SELECT "object" FROM "{self.name}" WHERE "key" = ?'
        item = self.__conn.select_one(GET_ITEM, (key,))
        if item is None:
            raise KeyError(key)
        return json.loads(item[0])

    def __delitem__(self, key):
        if self.flag == 'r':
            raise RuntimeError('Refusing to delete in read-only mode')
        if key not in self:
            raise KeyError(key)
        DEL_ITEM = f'DELETE FROM "{self.name}" WHERE "key" = ?'
        self.__conn.execute(DEL_ITEM, (key,))
        if self.__conn.autocommit and self.__conn.transaction_depth == 0:
            self.commit()

    def get_path(self, path, default=None, delimiter="/"):
        """
        Retrieve a value from nested dict-like storage using a path expression.
        Supports wildcards (*) and skips entries where keys are missing.

        Args:
            path (str): Delimited path string (e.g., "team/members/*/name").
            default (Any): Value to return if path is not found.
            delimiter (str): Delimiter used to split the path.

        Returns:
            Any: Value(s) at the path or default if not found.
        """

        def resolve(current,
                    keys,
                    default=None,
                    path_prefix= None,
                    delimiter: str = "/"):
            """
            Lazily yield (full_path, value) for every matched path, even if value == default.

            - '*' matches all keys or list indices at this level.
            - Supports dict, UserDict, and list traversal.
            - Always yields exactly once for each path implied by keys sequence.
            """
            if path_prefix is None:
                path_prefix = []

            if not keys:
                # End of path expression — yield whatever we have (or default if missing)
                yield {delimiter.join(path_prefix): current}
                return

            key = keys[0]
            rest = keys[1:]

            if key == "*":
                if isinstance(current, (dict, UserDict)):
                    for subkey, subval in current.items():
                        yield from resolve(subval, rest, default, path_prefix + [str(subkey)], delimiter)
                elif isinstance(current, list):
                    for idx, item in enumerate(current):
                        yield from resolve(item, rest, default, path_prefix + [str(idx)], delimiter)
                else:
                    # Can't expand '*' — just propagate without keys.
                    yield from resolve(current, [], default, path_prefix, delimiter)

            else:
                if isinstance(current, (dict, UserDict)):
                    if key in current:
                        yield from resolve(current[key], rest, default, path_prefix + [key], delimiter)
                    else:
                        # Key missing — substitute default and still yield
                        yield from resolve(default, rest, default, path_prefix + [key], delimiter)
                else:
                    # Not a mapping — treat as missing
                    yield from resolve(default, rest, default, path_prefix + [key], delimiter)


        path = path.strip(delimiter)
        keys = path.split(delimiter)
        return resolve(self, keys, default, delimiter=delimiter)
    
    def set_path(self, path: str, value: Any, delimiter: str = "/"):
        """
        Set a value in nested dict-like storage using a path expression.
        Supports wildcards (*) for lists/dicts. Creates intermediate dicts/lists
        as needed.

        Args:
            path (str): Delimited path string (e.g., "team/members/0/name").
            value (Any): Value to assign at the path.
            delimiter (str): Delimiter used to split the path.
        """

        def assign(current, keys, value):
            if not keys:
                return value

            key = keys[0]
            rest = keys[1:]

            if key == "*":
                if isinstance(current, (dict, UserDict)):
                    for subkey in current:
                        current[subkey] = assign(current[subkey], rest, value)
                elif isinstance(current, list):
                    for idx in range(len(current)):
                        current[idx] = assign(current[idx], rest, value)
                else:
                    # Can't expand '*' on non-iterable
                    return current
                return current

            else:
                if isinstance(current, (dict, UserDict)):
                    if key not in current or not isinstance(current[key], (dict, list, UserDict)):
                        # Create intermediate dict if missing
                        current[key] = {} if rest else None
                    current[key] = assign(current[key], rest, value)
                elif isinstance(current, list):
                    idx = int(key)
                    while len(current) <= idx:
                        current.append({})
                    current[idx] = assign(current[idx], rest, value)
                else:
                    # Replace non-container with dict if more keys remain
                    if rest:
                        current = {}
                        current[key] = assign({}, rest, value)
                    else:
                        current = {key: value}
                return current

        path = path.strip(delimiter)
        keys = path.split(delimiter)
        assign(self, keys, value)

    def get_path_value(self, key, path, default=None, delimiter="/"):
        """Get a single value at the given path for the specified key."""
        if key not in self:
            return default
        obj = self[key]
        keys = path.split(delimiter)
        current = obj
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return default
        return current

    def _set_path(self, key, path, value, delimiter="/"):
        """Set a value at the given path for the specified key."""
        if key not in self:
            raise KeyError(f"Key {key} not found")
        obj = self[key]
        keys = path.split(delimiter)
        current = obj
        for k in keys[:-1]:
            if not isinstance(current, dict):
                raise TypeError(f"Cannot set path {path}: {k} is not a dict")
            if k not in current:
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value
        self[key] = obj

    def del_path(self, key, path, delimiter="/"):
        """Delete the value at the given path for the specified key."""
        if key not in self:
            raise KeyError(f"Key {key} not found")
        obj = self[key]
        keys = path.split(delimiter)
        current = obj
        for k in keys[:-1]:
            if not isinstance(current, dict) or k not in current:
                raise KeyError(f"Path {path} not found")
            current = current[k]
        if not isinstance(current, dict) or keys[-1] not in current:
            raise KeyError(f"Path {path} not found")
        del current[keys[-1]]
        self[key] = obj

    def add_path(self, key, path, value, delimiter="/"):
        """Add value at path: set if not exists, append if list."""
        if key not in self:
            raise KeyError(f"Key {key} not found")
        obj = self[key]
        keys = path.split(delimiter)
        current = obj
        for k in keys[:-1]:
            if not isinstance(current, dict):
                raise TypeError(f"Cannot add to path {path}: {k} is not a dict")
            if k not in current:
                current[k] = {}
            current = current[k]
        last_key = keys[-1]
        if last_key not in current:
            current[last_key] = value
        else:
            if isinstance(current[last_key], list):
                current[last_key].append(value)
            else:
                current[last_key] = value  # overwrite
        self[key] = obj

    @property
    def columns(self) -> list:
        '''
        Return s a list of column names
        '''
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        return [x[1] for x in data]

    def commit(self, blocking=True):
        '''
        From sqlitedict
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        '''
        if self.__conn is not None:
            self.__conn.commit(blocking)

    def to_dict(self):
        ret_dict = {}
        for x in self.keys():
            ret_dict[x] = self[x]
        return ret_dict

    def to_json(self):
        import json
        return json.dumps(self.to_dict())
    
    def query(self, recipe: Dict[str, Any], delimiter: str = "/") -> Union[List[Dict], Dict, Any]:
        """
        Query the storage using a declarative recipe dict.
 
        Recipe keys (all optional):
            filter     - filter expression (leaf or logical combinator)
            select     - list of slash-delimited paths to extract per item
            aggregate  - aggregation spec: {op, field?, by?, sub_op?}
            sort       - list of {field, order} dicts  (order: "asc" | "desc")
            limit      - int
            offset     - int  (default 0)
 
        Filter expressions
        ------------------
        Leaf (single condition):
            {"path": "address/city", "op": "eq", "value": "NYC"}
 
        Logical combinators (nest arbitrarily):
            {"and": [<expr>, ...]}
            {"or":  [<expr>, ...]}
            {"not": <expr>}
 
        Operators:
            eq, ne                  - equality / inequality
            gt, gte, lt, lte        - numeric / string comparison
            in, not_in              - membership  (value must be a list)
            contains                - substring (str) or item presence (list)
            startswith, endswith    - string prefix / suffix
            exists                  - True → field present and not None;
                                      False → field absent or None
            regex                   - re.search match; value is the pattern
 
        Aggregate spec
        --------------
        Scalar reductions (return a single value):
            {"op": "count"}
            {"op": "sum",  "field": "salary"}
            {"op": "avg",  "field": "score"}
            {"op": "min",  "field": "age"}
            {"op": "max",  "field": "age"}
 
        Group-by (returns a dict keyed by group value):
            {"op": "group_by", "by": "department"}
            {"op": "group_by", "by": "department",
             "field": "salary", "sub_op": "avg"}
 
        Return values
        -------------
        list[dict]  - no aggregate, or group_by without sub_op (dict of lists)
        scalar      - count / sum / avg / min / max
        dict        - group_by  {group_value: [items] | scalar}
 
        Examples
        --------
        # Filter, project, sort, page
        storage.query({
            "filter": {"path": "age", "op": "gte", "value": 18},
            "select": ["name", "address/city"],
            "sort":   [{"field": "name", "order": "asc"}],
            "limit":  10,
        })
 
        # Compound filter
        storage.query({
            "filter": {
                "and": [
                    {"path": "active",   "op": "eq",  "value": True},
                    {"path": "score",    "op": "gte", "value": 50},
                    {"not": {"path": "role", "op": "in", "value": ["guest"]}},
                ]
            }
        })
 
        # Scalar aggregate
        storage.query({
            "filter":    {"path": "dept", "op": "eq", "value": "eng"},
            "aggregate": {"op": "avg", "field": "salary"},
        })
 
        # Group-by with per-group aggregation
        storage.query({
            "aggregate": {
                "op": "group_by", "by": "dept",
                "field": "salary", "sub_op": "avg",
            },
            "sort": [{"field": "dept", "order": "asc"}],
        })
        """
 
        # ------------------------------------------------------------------ #
        #  Internal helpers                                                  #
        # ------------------------------------------------------------------ #

        import re
 
        def _get_path(obj: Any, path: str) -> Any:
            """Resolve a slash-delimited path into a nested value."""
            if not path:
                return obj
            parts = path.strip(delimiter).split(delimiter)
            cur = obj
            for part in parts:
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return None
            return cur
 
        def _path_exists(obj: Any, path: str) -> bool:
            """Return True iff the path leads to a non-None value."""
            parts = path.strip(delimiter).split(delimiter)
            cur = obj
            for part in parts:
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    return False
            return cur is not None
 
        def _eval_leaf(obj: Any, expr: Dict) -> bool:
            path   = expr.get("path", "")
            op     = expr.get("op", "eq")
            value  = expr.get("value")
            actual = _get_path(obj, path)
 
            if op == "eq":         return actual == value
            if op == "ne":         return actual != value
            if op == "gt":         return actual is not None and actual >  value
            if op == "gte":        return actual is not None and actual >= value
            if op == "lt":         return actual is not None and actual <  value
            if op == "lte":        return actual is not None and actual <= value
            if op == "in":         return actual in value
            if op == "not_in":     return actual not in value
            if op == "exists":     return _path_exists(obj, path) if value else not _path_exists(obj, path)
            if op == "regex":      return bool(re.search(value, str(actual))) if actual is not None else False
            if op == "contains":
                if isinstance(actual, list): return value in actual
                if isinstance(actual, str):  return value in actual
                return False
            if op == "startswith": return isinstance(actual, str) and actual.startswith(value)
            if op == "endswith":   return isinstance(actual, str) and actual.endswith(value)
 
            raise ValueError(f"Unknown filter operator: {op!r}")
 
        def _eval_filter(obj: Any, expr: Any) -> bool:
            if not isinstance(expr, dict):
                raise TypeError(f"Filter expression must be a dict, got {type(expr)}")
            if "and" in expr: return all(_eval_filter(obj, sub) for sub in expr["and"])
            if "or"  in expr: return any(_eval_filter(obj, sub) for sub in expr["or"])
            if "not" in expr: return not _eval_filter(obj, expr["not"])
            return _eval_leaf(obj, expr)
 
        def _apply_select(obj: Any, paths: List[str]) -> Dict:
            """Extract only the requested paths; keys are the leaf name or full
            path when two paths share the same leaf name."""
            result: Dict[str, Any] = {}
            for path in paths:
                val  = _get_path(obj, path)
                leaf = path.split(delimiter)[-1]
                key  = path if leaf in result else leaf
                result[key] = val
            return result
 
        def _reduce(values: List[Any], op: str) -> Any:
            nums = [v for v in values if isinstance(v, (int, float))]
            if op == "count": return len(values)
            if not nums:      return None
            if op == "sum":   return sum(nums)
            if op == "min":   return min(nums)
            if op == "max":   return max(nums)
            if op == "avg":   return sum(nums) / len(nums)
            raise ValueError(f"Unknown reduce op: {op!r}")
 
        def _sort_key(row: Dict, field: str):
            """None always sorts last regardless of direction."""
            val = _get_path(row, field)
            return (val is None, val)
 
        # ------------------------------------------------------------------ #
        #  Parse recipe                                                      #
        # ------------------------------------------------------------------ #
 
        filter_expr:  Optional[Dict]       = recipe.get("filter")
        select_paths: Optional[List[str]]  = recipe.get("select")
        agg_spec:     Optional[Dict]       = recipe.get("aggregate")
        sort_spec:    Optional[List[Dict]] = recipe.get("sort")
        limit:        Optional[int]        = recipe.get("limit")
        offset:       int                  = recipe.get("offset", 0)
 
        # ------------------------------------------------------------------ #
        #  Step 1 - filter                                                   #
        # ------------------------------------------------------------------ #
 
        rows: List[Dict] = []
        for key in self:
            try:
                item = self[key]
            except Exception:
                continue
 
            if filter_expr is not None and not _eval_filter(item, filter_expr):
                continue
 
            rows.append({"_key": key, **item})
 
        # ------------------------------------------------------------------ #
        #  Step 2 - aggregate                                                #
        # ------------------------------------------------------------------ #
 
        if agg_spec:
            op     = agg_spec.get("op")
            field  = agg_spec.get("field")
            by     = agg_spec.get("by")
            sub_op = agg_spec.get("sub_op")
 
            if op == "group_by":
                if not by:
                    raise ValueError("aggregate.op='group_by' requires a 'by' path")

                from collections import defaultdict
                buckets: Dict[Any, List] = defaultdict(list)
                for row in rows:
                    buckets[_get_path(row, by)].append(row)
 
                if field and sub_op:
                    result: Dict = {
                        k: _reduce([_get_path(r, field) for r in v], sub_op)
                        for k, v in buckets.items()
                    }
                else:
                    result = {
                        k: ([_apply_select(r, select_paths) for r in v]
                            if select_paths else v)
                        for k, v in buckets.items()
                    }
 
                if sort_spec:
                    for spec in reversed(sort_spec):
                        reverse = spec.get("order", "asc") == "desc"
                        result = dict(
                            sorted(
                                result.items(),
                                key=lambda kv: (kv[0] is None, kv[0]),
                                reverse=reverse,
                            )
                        )
                return result
 
            # Scalar reduction
            if op == "count":
                return len(rows)
            if not field:
                raise ValueError(f"aggregate.op={op!r} requires a 'field' path")
            return _reduce([_get_path(r, field) for r in rows], op)
 
        # ------------------------------------------------------------------ #
        #  Step 3 - select projection                                        #
        # ------------------------------------------------------------------ #
 
        if select_paths:
            rows = [
                {"_key": r["_key"], **_apply_select(r, select_paths)}
                for r in rows
            ]
 
        # ------------------------------------------------------------------ #
        #  Step 4 - sort                                                     #
        # ------------------------------------------------------------------ #
 
        if sort_spec:
            for spec in reversed(sort_spec):
                reverse = spec.get("order", "asc") == "desc"
                rows.sort(
                    key=lambda r, f=spec.get("field", "_key"): _sort_key(r, f),
                    reverse=reverse,
                )
 
        # ------------------------------------------------------------------ #
        #  Step 5 - paginate                                                 #
        # ------------------------------------------------------------------ #
 
        rows = rows[offset: offset + limit] if limit else rows[offset:]
        ret = {}
        for r in rows:
            key = r.pop("_key")
            ret[key] = r
        return ret
    
    def merge(self, dict2: dict):
        '''
        Update the Storage
        '''
        def dict_merge(a, b):
            '''
            Recursive Dict Merge
            '''
            from copy import deepcopy
            if not isinstance(b, dict):
                return b
            result = deepcopy(a)
            for k, v in b.items():
                if k in result and isinstance(result[k], dict):
                    result[k] = dict_merge(result[k], v)
                else:
                    result[k] = deepcopy(v)
            return result

        res_dict = dict_merge(self.to_dict(), dict2)
        for x in res_dict.keys():
            self[x] = res_dict[x]

    def to_sql(self):
        '''
        Returns the table in SQLite Syntax
        '''
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        CREATE = self.__conn.select_one(GET_SQL)[0]
        VALUES = ',\n'.join([str((x, self[x])) for x in self])
        INSERT = f'INSERT INTO "{self.name}" VALUES\n' + VALUES
        return f'{CREATE};\n{INSERT};'


class TableView(UserDict):
    """
    Connector Class for a SQLite View as UserDict.

    Usage:
        db = Database()
        view = db[('my_view', 'view')]
    """

    def __init__(self, name: str, connection: SqliteMultiThread, flag: str,
                 create_sql: str | None = None):
        self.__conn = connection
        self.flag = flag
        self.name = name.replace('"', '""')
        self.filename = self.__conn.filename

        GET_VIEW = 'SELECT name FROM sqlite_master WHERE type="view" AND name = ?'
        item = self.__conn.select_one(GET_VIEW, (name,))
        if item is None:
            if create_sql is None:
                raise RuntimeError(f'View "{name}" does not exist and no SQL provided to create it.')
            CREATE_VIEW = f'CREATE VIEW "{self.name}" AS {create_sql}'
            self.__conn.execute(CREATE_VIEW)
            self.__conn.commit()

    def describe(self):
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        head = ['cid', 'name', 'type', 'notnull', 'default', 'primary key']
        from tabulate import tabulate
        return tabulate(data, head, tablefmt='grid')

    @property
    def xschema(self):
        GET_SQL = f'SELECT sql FROM sqlite_master WHERE "name" = "{self.name}"'
        schema = self.__conn.select_one(GET_SQL)[0]
        return {
            'name': self.name,
            'cols': self.columns,
            'sql': schema
        }

    @property
    def columns(self):
        GET_COLS = f'PRAGMA TABLE_INFO("{self.name}")'
        data = self.__conn.select(GET_COLS)
        return [x[1] for x in data]

    def __getitem__(self, args):
        if isinstance(args, slice):
            GET = f'SELECT * FROM "{self.name}" LIMIT ? OFFSET ?'
            result = self.__conn.select(GET, (args.stop - args.start, args.start))
            return result[::args.step] if args.step else result

        if isinstance(args, int):
            GET = f'SELECT * FROM "{self.name}" LIMIT 1 OFFSET ?'
            item = self.__conn.select_one(GET, (args,))
            if item is None:
                raise KeyError(args)
            return item

        if isinstance(args, str) and args in self.columns:
            GET = f'SELECT "{args}" FROM "{self.name}" ORDER BY rowid'
            result = self.__conn.select(GET)
            return [x[0] for x in result]

        if isinstance(args, tuple) and len(args) >= 2:
            col, value = args[0], args[1]
            if col not in self.columns:
                raise KeyError(f'Unknown column: {col}')
            GET = f'SELECT * FROM "{self.name}" WHERE "{col}" = ?'
            result = self.__conn.select(GET, (value,))
            return result

        raise TypeError("Unsupported key type")

    def __contains__(self, key):
        CHECK = f'SELECT 1 FROM "{self.name}" WHERE _rowid_ = ?'
        return self.__conn.select_one(CHECK, (key,)) is not None

    def keys(self):
        GET_KEYS = f'SELECT rowid FROM "{self.name}" ORDER BY rowid'
        for x in self.__conn.select(GET_KEYS):
            yield x[0]

    def __iter__(self):
        return self.keys()

    def __repr__(self):
        return f'TableView: {self.name}'

    def __len__(self):
        GET_LEN = f'SELECT COUNT(*) FROM "{self.name}"'
        count = self.__conn.select_one(GET_LEN)
        return count[0] if count else 0
