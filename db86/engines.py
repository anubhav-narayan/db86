from __future__ import annotations
from typing import Any, Dict, List, Union
from .storages import JSONStorage, Table
from abc import ABC, abstractmethod


class Engine(ABC):
    """
    Abstract Base Class for all Engines.

    Every Engine must implement:
    - query: declarative query interface (filters, selects, aggregates).
    - insert: bulk insertion interface (single or multiple rows/documents).
    - update: bulk update interface with conditional filters.

    This enforces a consistent API across different storage backends
    (e.g., Table, JSONStorage).
    """

    @abstractmethod
    def query(self, recipe: Dict[str, Any]) -> Union[List[Dict], Dict, Any]:
        """
        Execute a query based on a declarative recipe.

        Parameters
        ----------
        recipe : dict
            Declarative query specification. Must support keys like:
            - filter
            - select
            - aggregate
            - sort
            - limit
            - offset

        Returns
        -------
        list[dict] | dict | Any
            Query results, depending on recipe.
        """
        raise NotImplementedError

    @abstractmethod
    def insert(self, items: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """
        Insert or replace one or more rows/documents in the storage.

        Parameters
        ----------
        items : dict | list[dict]
            Either a single {key: value} mapping or a list of such mappings.
            Each mapping must include a primary key and associated data.

        Returns
        -------
        int
            Number of rows/documents updated
        """
        raise NotImplementedError

    @abstractmethod
    def update(self, updates: Dict[str, Any], filter_expr: Dict[str, Any]) -> int:
        """
        Bulk update rows/documents that match a filter condition.

        Parameters
        ----------
        updates : dict
            Fields and values to update.
        filter_expr : dict
            Declarative filter expression (same format as in query).
            Determines which rows/documents are updated.

        Returns
        -------
        int
            Number of rows/documents updated.
        """
        raise NotImplementedError



class Engine4A_C(Engine):
    """
    4A-C Query Engine — baseline CRUD and query orchestration with SQLite offload.

    This engine provides a declarative query interface over both `Table` and
    `JSONStorage` backends. It leverages SQLite 3.38+ features, including
    `json_extract`, to offload filtering, selection, aggregation, and sorting
    directly to the database engine whenever possible.

    Design Philosophy
    -----------------
    - "4A-C spec": simple, reliable, sequential execution.
    - Single-table only (no joins or unions).
    - Pushes heavy lifting into SQLite for performance.
    - Unified interface for both relational columns and JSON paths.

    Parameters
    ----------
    storage : Table | JSONStorage
        The underlying storage object. Determines whether queries operate
        on relational columns (Table) or JSON blobs (JSONStorage).

    Recipe Keys
    -----------
    filter : dict
        A filter expression, either a leaf condition or a logical combinator.
        Supported operators: eq, ne, gt, gte, lt, lte, contains, startswith,
        endswith, in, not_in, exists, and/or/not.
    select : list[str]
        List of column names or slash-delimited JSON paths to project.
        The primary key (`key`) is always included automatically.
    aggregate : dict
        Aggregation specification. Supported ops:
            - count
            - sum, avg, min, max (require 'field')
            - group_by (requires 'by'; optional 'field' + 'sub_op')
    sort : list[dict]
        Sorting specification. Each dict has {"field": ..., "order": "asc|desc"}.
    limit : int
        Maximum number of rows to return.
    offset : int
        Number of rows to skip before returning results (default 0).

    Returns
    -------
    list[dict]
        For standard queries with select or full rows.
    dict
        For group_by aggregates, keyed by group value.
    scalar
        For scalar aggregates (count, sum, avg, min, max).

    Examples
    --------
    # Select names of adults
    engine.query({
        "filter": {"path": "age", "op": "gte", "value": 18},
        "select": ["name"]
    })

    # Count active profiles in JSONStorage
    engine.query({
        "filter": {"path": "active", "op": "eq", "value": 1},
        "aggregate": {"op": "count"}
    })

    # Group salaries by department and average
    engine.query({
        "aggregate": {"op": "group_by", "by": "dept", "field": "salary", "sub_op": "avg"}
    })
    """

    def __init__(self, storage: Union[Table, JSONStorage]):
        self.storage = storage
        if isinstance(storage, JSONStorage):
            self.conn = storage._JSONStorage__conn
        else:
            self.conn = storage._Table__conn
        self.is_json = isinstance(storage, JSONStorage)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _col(self, path: str) -> str:
        """Return the SQL column expression for a field/path."""
        if self.is_json:
            dot = path.replace("/", ".")
            return f"json_extract(object, '$.{dot}')"
        return f'"{path}"'

    def _compile_filter(self, expr: Dict) -> tuple[str, list]:
        """
        Recursively compile a filter expression to (sql_fragment, params).
        Supports: and, or, not, and leaf ops.
        """
        if "and" in expr:
            parts, params = [], []
            for sub in expr["and"]:
                s, p = self._compile_filter(sub)
                parts.append(f"({s})")
                params.extend(p)
            return " AND ".join(parts), params

        if "or" in expr:
            parts, params = [], []
            for sub in expr["or"]:
                s, p = self._compile_filter(sub)
                parts.append(f"({s})")
                params.extend(p)
            return " OR ".join(parts), params

        if "not" in expr:
            s, p = self._compile_filter(expr["not"])
            return f"NOT ({s})", p

        # leaf
        path  = expr.get("path", "")
        op    = expr.get("op", "eq")
        value = expr.get("value")
        col   = self._col(path)

        match op:
            case "eq":         return f"{col} = ?", [value]
            case "ne":         return f"{col} != ?", [value]
            case "gt":         return f"{col} > ?", [value]
            case "gte":        return f"{col} >= ?", [value]
            case "lt":         return f"{col} < ?", [value]
            case "lte":        return f"{col} <= ?", [value]
            case "contains":   return f"{col} LIKE ?", [f"%{value}%"]
            case "startswith": return f"{col} LIKE ?", [f"{value}%"]
            case "endswith":   return f"{col} LIKE ?", [f"%{value}"]
            case "in":
                ph = ",".join("?" * len(value))
                return f"{col} IN ({ph})", list(value)
            case "not_in":
                ph = ",".join("?" * len(value))
                return f"{col} NOT IN ({ph})", list(value)
            case "exists":
                return f"{col} IS NOT NULL", []
            case _:
                raise ValueError(f"Unsupported operator: {op!r}")

    def _col_names(self) -> list[str]:
        """Return column names for the storage (used to build result dicts)."""
        if self.is_json:
            return ["key", "object"]
        return self.storage.columns

    # ------------------------------------------------------------------
    # Public query
    # ------------------------------------------------------------------

    def query(
        self, recipe: Dict[str, Any]
    ) -> Union[List[Dict], Dict, Any]:
        """
        Recipe keys
        -----------
        filter     - leaf or logical combinator (and/or/not)
        select     - list of field names or slash-delimited JSON paths
        aggregate  - {op, field?, by?, sub_op?}
        sort       - [{field, order}]
        limit      - int
        offset     - int (default 0)
        """
        table        = self.storage.name
        filter_expr  = recipe.get("filter")
        select_paths = recipe.get("select")
        agg_spec     = recipe.get("aggregate")
        sort_spec    = recipe.get("sort")
        limit        = recipe.get("limit")
        offset       = recipe.get("offset", 0)
        params: list = []

        # ── SELECT clause ────────────────────────────────────────────
        if agg_spec:
            op     = agg_spec.get("op")
            field  = agg_spec.get("field")
            by     = agg_spec.get("by")
            sub_op = agg_spec.get("sub_op")

            match op:
                case "count":
                    select_clause = "COUNT(*)"
                case "sum" | "avg" | "min" | "max":
                    if not field:
                        raise ValueError(f"aggregate op={op!r} requires 'field'")
                    select_clause = f"{op.upper()}({self._col(field)})"
                case "group_by":
                    if not by:
                        raise ValueError("group_by requires 'by'")
                    by_expr = self._col(by)
                    if field and sub_op:
                        select_clause = f"{by_expr}, {sub_op.upper()}({self._col(field)})"
                    elif field:
                        select_clause = f"{by_expr}, {self._col(field)}"
                    else:
                        select_clause = by_expr
                case _:
                    raise ValueError(f"Unknown aggregate op: {op!r}")
        elif select_paths:
            select_clause =  ", ".join(self._col(p) for p in select_paths)
            if isinstance(self.storage, JSONStorage):
                select_paths = ["key"] + select_paths
                select_clause = "key, " + select_clause
            else:
                pk = self.storage.columns[0]
                select_paths = [f"{pk}"] + select_paths
                select_clause = f"{pk}, " + select_clause
        else:
            select_clause = "*"

        sql = f'SELECT {select_clause} FROM "{table}"'

        # ── WHERE clause ─────────────────────────────────────────────
        if filter_expr:
            where_sql, where_params = self._compile_filter(filter_expr)
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        # ── GROUP BY ─────────────────────────────────────────────────
        if agg_spec and agg_spec.get("op") == "group_by":
            sql += f" GROUP BY {self._col(agg_spec['by'])}"

        # ── ORDER BY ─────────────────────────────────────────────────
        if sort_spec:
            clauses = [
                f"{self._col(s['field'])} {s.get('order', 'asc').upper()}"
                for s in sort_spec
            ]
            sql += " ORDER BY " + ", ".join(clauses)

        # ── LIMIT / OFFSET ───────────────────────────────────────────
        if limit is not None:
            sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        elif offset:
            sql += f" LIMIT -1 OFFSET {int(offset)}"

        # ── Execute ──────────────────────────────────────────────────
        rows = self.conn.select(sql, tuple(params))

        # ── Return ───────────────────────────────────────────────────
        if agg_spec:
            op = agg_spec.get("op")
            if op == "group_by":
                # rows are (group_val, agg_val) or (group_val,) tuples
                return {r[0]: (r[1] if len(r) > 1 else None) for r in rows}
            # scalar
            return rows[0][0] if rows else None

        if select_paths:
            return [dict(zip(select_paths, row)) for row in rows]

        # full rows — build dicts from storage column names
        cols = self._col_names()
        return [dict(zip(cols, row)) for row in rows]
    
    def insert(self, items: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """Bulk insert one or more rows/documents."""
        if isinstance(items, dict):
            items = [items]

        for item in items:
            key = item.get("key")
            value = item.get("value")
            if key is None or value is None:
                raise ValueError("insert requires 'key' and 'value'")

            if self.is_json:
                import json
                sql = f'''
                INSERT INTO "{self.storage.name}" (key, object)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET object = excluded.object
                '''
                self.conn.execute(sql, (key, json.dumps(value)))
            else:
                cols = list(value.keys())
                vals = list(value.values())
                placeholders = ", ".join(["?"] * (len(vals) + 1))
                sql = f'''
                REPLACE INTO "{self.storage.name}" ({",".join(["key"] + cols)})
                VALUES ({placeholders})
                '''
                self.conn.execute(sql, (key, *vals))
        return len(items)
    
    def update(self, updates: Dict[str, Any], filter_expr: Dict[str, Any]) -> int:
        """Bulk update rows/documents matching a filter."""
        if not updates:
            raise ValueError("updates dict cannot be empty")

        set_clause = ", ".join(f"{self._col(k)} = ?" for k in updates.keys())
        params = list(updates.values())

        where_sql, where_params = self._compile_filter(filter_expr)
        sql = f'UPDATE "{self.storage.name}" SET {set_clause} WHERE {where_sql}'
        params.extend(where_params)

        cur = self.conn.execute(sql, tuple(params))
        return cur.rowcount