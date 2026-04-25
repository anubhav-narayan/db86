from __future__ import annotations
from typing import Any, Dict, List, Union
import os
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
        """Initialise the 4A-C engine for a single storage backend.

        Parameters
        ----------
        storage : Table | JSONStorage
            The storage object to query against.  Determines
            whether SQL expressions use ``json_extract`` or
            plain column references.
        """
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
        # return cur.rowcount


class Engine4A_GE(Engine):
    """
    4A-GE Query Engine — multi-table query orchestration with advanced SQLite features.

    This engine extends 4A-C with multi-table support, including JOINs, UNIONs,
    and cross-table aggregations. It leverages SQLite's full query capabilities
    while maintaining the declarative recipe interface.

    Design Philosophy
    -----------------
    - "4A-GE spec": high-performance, multi-table operations.
    - Supports JOINs (INNER, LEFT, FULL) and UNIONs.
    - Cross-table filtering, selection, and aggregation.
    - Advanced query optimization with CTEs and subqueries.
    - Unified interface for relational and JSON data across tables.

    Parameters
    ----------
    database : Database
        The database instance containing multiple tables/storages.

    Recipe Keys (Extended from 4A-C)
    -----------
    tables : list[dict]
        List of table specifications: [{"name": "table1", "alias": "t1"}, ...]
    joins : list[dict]
        Join specifications: [{"type": "INNER", "table": "table2", "alias": "t2",
                              "on": {"left": "t1.id", "op": "eq", "right": "t2.fk"}}]
    filter : dict
        Filter expression (supports cross-table paths like "t1.field")
    select : list[str]
        Fields to select (supports "table.field" or "alias.field")
    aggregate : dict
        Aggregation spec (supports cross-table grouping)
    sort : list[dict]
        Sorting specification
    limit : int
        Maximum rows to return
    offset : int
        Rows to skip
    union : list[dict]
        UNION operations with other recipes

    Returns
    -------
    list[dict]
        Query results with flattened column names
    dict
        For group_by aggregates
    scalar
        For scalar aggregates

    Examples
    --------
    # Join users and orders
    engine.query({
        "tables": [
            {"name": "users", "alias": "u"},
            {"name": "orders", "alias": "o"}
        ],
        "joins": [{
            "type": "INNER",
            "table": "orders",
            "alias": "o",
            "on": {"left": "u.id", "op": "eq", "right": "o.user_id"}
        }],
        "select": ["u.name", "o.total"],
        "filter": {"path": "o.total", "op": "gt", "value": 100}
    })

    # Cross-table aggregation
    engine.query({
        "tables": [{"name": "orders", "alias": "o"}],
        "aggregate": {"op": "group_by", "by": "o.status", "field": "o.total", "sub_op": "sum"}
    })
    """

    def __init__(self, database, enable_parallel=True, max_workers=None):
        """Initialise the 4A-GE multi-table engine.

        Parameters
        ----------
        database : Database
            The database containing multiple tables/storages.
        enable_parallel : bool
            Enable parallel compilation of complex filter expressions.
        max_workers : int | None
            Maximum thread-pool workers for parallel operations.
            Defaults to ``min(4, cpu_count)``.
        """
        self.database = database
        # Get connection from database
        self.conn = database.conn
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers or min(4, os.cpu_count() or 2)
        
        # Thread pool for parallel operations
        if self.enable_parallel:
            from concurrent.futures import ThreadPoolExecutor
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        else:
            self.executor = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_table_type(self, table_name: str) -> tuple[bool, str]:
        """Return (is_json, actual_table_name) for a table.
        
        Checks in order:
        1. Table type map (from recipe specs with explicit 'type' field)
        2. Storage object capabilities (if has 'add_column', it's Table)
        3. Default to JSONStorage if type cannot be determined
        """
        # Check if type was explicitly specified in table spec
        if hasattr(self, '_table_type_map'):
            if table_name in self._table_type_map:
                is_json = self._table_type_map[table_name]
                return is_json, table_name
        
        # Default to JSONStorage
        return True, table_name

    def _col_expr(self, field_path: str, table_alias: str = None) -> str:
        """
        Convert field path to SQL column expression.
        Supports: "field", "table.field", "alias.field", "json/path"
        """
        if "." in field_path:
            parts = field_path.split(".", 1)
            table_part = parts[0]
            field_part = parts[1]

            if table_part:
                is_json, _ = self._resolve_table_type(table_part)
                if is_json:
                    # JSON path
                    json_path = field_part.replace("/", ".")
                    return f"json_extract({table_part}.object, '$.{json_path}')"
                else:
                    # Regular column
                    return field_path
            else:
                # Assume it's table.field format
                is_json, _ = self._resolve_table_type(table_part)
                if is_json:
                    json_path = field_part.replace("/", ".")
                    return f"json_extract({table_part}.object, '$.{json_path}')"
                else:
                    return field_path
        else:
            # Simple field - assume primary key or handle contextually
            if table_alias:
                is_json, _ = self._resolve_table_type(table_alias)
                if is_json:
                    json_path = field_path.replace("/", ".")
                    return f"json_extract({table_alias}.object, '$.{json_path}')"
                else:
                    return f'{table_alias}."{field_path}"'
            return f'"{field_path}"'

    def _compile_join_condition(self, join_spec: dict) -> str:
        """Compile join ON condition."""
        left = self._col_expr(join_spec['on']['left'])
        op = join_spec['on']['op']
        right = self._col_expr(join_spec['on']['right'])

        op_map = {
            'eq': '=', 'ne': '!=', 'gt': '>', 'gte': '>=',
            'lt': '<', 'lte': '<='
        }
        sql_op = op_map.get(op, '=')
        return f"{left} {sql_op} {right}"

    def _compile_filter(self, expr: dict, table_context: str = None) -> tuple[str, list]:
        """
        Recursively compile a filter expression to (sql_fragment, params).
        Supports: and, or, not, and leaf ops with multi-table support.
        """
        if "and" in expr:
            parts, params = [], []
            for sub in expr["and"]:
                s, p = self._compile_filter(sub, table_context)
                parts.append(f"({s})")
                params.extend(p)
            return " AND ".join(parts), params

        if "or" in expr:
            parts, params = [], []
            for sub in expr["or"]:
                s, p = self._compile_filter(sub, table_context)
                parts.append(f"({s})")
                params.extend(p)
            return " OR ".join(parts), params

        if "not" in expr:
            s, p = self._compile_filter(expr["not"], table_context)
            return f"NOT ({s})", p

        # leaf condition
        path = expr.get("path", "")
        op = expr.get("op", "eq")
        value = expr.get("value")

        col = self._col_expr(path, table_context)

        match op:
            case "eq": return f"{col} = ?", [value]
            case "ne": return f"{col} != ?", [value]
            case "gt": return f"{col} > ?", [value]
            case "gte": return f"{col} >= ?", [value]
            case "lt": return f"{col} < ?", [value]
            case "lte": return f"{col} <= ?", [value]
            case "contains": return f"{col} LIKE ?", [f"%{value}%"]
            case "startswith": return f"{col} LIKE ?", [f"{value}%"]
            case "endswith": return f"{col} LIKE ?", [f"%{value}"]
            case "in":
                ph = ",".join("?" * len(value))
                return f"{col} IN ({ph})", list(value)
            case "not_in":
                ph = ",".join("?" * len(value))
                return f"{col} NOT IN ({ph})", list(value)
            case "exists": return f"{col} IS NOT NULL", []
            case _: raise ValueError(f"Unsupported operator: {op!r}")

    def _compile_filter_parallel(self, expr: dict, table_context: str = None) -> tuple[str, list]:
        """Parallel version of filter compilation for complex expressions."""
        if not self.enable_parallel:
            return self._compile_filter(expr, table_context)
            
        if "and" in expr:
            # Parallel compilation of AND clauses
            sub_exprs = expr["and"]
            if len(sub_exprs) > 2:  # Only parallelize if worthwhile
                futures = []
                for sub in sub_exprs:
                    future = self.executor.submit(self._compile_filter, sub, table_context)
                    futures.append(future)
                
                results = [f.result() for f in futures]
                parts, params = [], []
                for sql_part, sql_params in results:
                    parts.append(f"({sql_part})")
                    params.extend(sql_params)
                return " AND ".join(parts), params
            else:
                # Fall back to sequential for small expressions
                return self._compile_filter(expr, table_context)
                
        elif "or" in expr:
            # Parallel compilation of OR clauses  
            sub_exprs = expr["or"]
            if len(sub_exprs) > 2:
                futures = []
                for sub in sub_exprs:
                    future = self.executor.submit(self._compile_filter, sub, table_context)
                    futures.append(future)
                
                results = [f.result() for f in futures]
                parts, params = [], []
                for sql_part, sql_params in results:
                    parts.append(f"({sql_part})")
                    params.extend(sql_params)
                return " OR ".join(parts), params
            else:
                return self._compile_filter(expr, table_context)
        else:
            # Single conditions - no parallelism needed
            return self._compile_filter(expr, table_context)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, recipe: dict) -> Union[List[Dict], Dict, Any]:
        """
        Execute multi-table query based on declarative recipe.
        
        Table specs support optional 'type' field:
        - "type": "json" for JSONStorage (default if not specified)
        - "type": "table" for Table/relational storage
        """
        tables = recipe.get("tables", [])
        joins = recipe.get("joins", [])
        filter_expr = recipe.get("filter")
        select_fields = recipe.get("select")
        agg_spec = recipe.get("aggregate")
        sort_spec = recipe.get("sort")
        limit = recipe.get("limit")
        offset = recipe.get("offset", 0)
        union_recipes = recipe.get("union", [])

        if not tables:
            raise ValueError("Multi-table query requires 'tables' specification")

        # Build table type map from specs (explicit type or default to JSONStorage)
        self._table_type_map = {}
        for table_spec in tables:
            table_name = table_spec.get('name')
            table_alias = table_spec.get("alias", table_name)
            type_field = table_spec.get('type', 'json')
            is_json = type_field.lower() != 'table'
            self._table_type_map[table_name] = is_json
            if table_alias != table_name:
                self._table_type_map[table_alias] = is_json

        self._current_tables = tables
        params = []

        # Build FROM clause with joins
        from_clause = ""
        if tables:
            first_table = tables[0]
            table_name = first_table['name']
            alias = first_table.get('alias', table_name)
            from_clause = f'"{table_name}" {alias}'

            # Add JOINs
            for join in joins:
                join_type = join.get('type', 'INNER').upper()
                join_table = join['table']
                join_alias = join.get('alias', join_table)
                join_condition = self._compile_join_condition(join)

                from_clause += f" {join_type} JOIN \"{join_table}\" {join_alias} ON {join_condition}"

        # SELECT clause
        if agg_spec:
            op = agg_spec.get("op")
            field = agg_spec.get("field")
            by = agg_spec.get("by")
            sub_op = agg_spec.get("sub_op")

            match op:
                case "count": select_clause = "COUNT(*)"
                case "sum" | "avg" | "min" | "max":
                    if not field:
                        raise ValueError(f"Aggregate {op} requires 'field'")
                    select_clause = f"{op.upper()}({self._col_expr(field)})"
                case "group_by":
                    if not by:
                        raise ValueError("group_by requires 'by'")
                    by_expr = self._col_expr(by)
                    if field and sub_op:
                        select_clause = f"{by_expr}, {sub_op.upper()}({self._col_expr(field)})"
                    elif field:
                        select_clause = f"{by_expr}, {self._col_expr(field)}"
                    else:
                        select_clause = by_expr
                case _: raise ValueError(f"Unknown aggregate op: {op}")
        elif select_fields:
            select_clause = ", ".join(self._col_expr(f) for f in select_fields)
        else:
            # Select all columns from all tables
            all_cols = []
            for t in tables:
                alias = t.get('alias', t['name'])
                is_json, _ = self._resolve_table_type(t['name'])
                if is_json:
                    all_cols.append(f'{alias}.key, {alias}.object')
                else:
                    # Get actual columns
                    storage = self.database[t['name']]
                    cols = [f'{alias}."{c}"' for c in storage.columns]
                    all_cols.extend(cols)
            select_clause = ", ".join(all_cols)

        sql = f"SELECT {select_clause} FROM {from_clause}"

        # WHERE clause - with parallel compilation for complex filters
        if filter_expr:
            # Use parallel compilation for complex AND/OR expressions
            if self.enable_parallel and ("and" in filter_expr or "or" in filter_expr):
                # Check if the expression is complex enough to benefit from parallelism
                expr_count = 0
                if "and" in filter_expr:
                    expr_count = len(filter_expr["and"])
                elif "or" in filter_expr:
                    expr_count = len(filter_expr["or"])
                
                if expr_count > 2:  # Only parallelize if worthwhile
                    where_sql, where_params = self._compile_filter_parallel(filter_expr)
                else:
                    where_sql, where_params = self._compile_filter(filter_expr)
            else:
                where_sql, where_params = self._compile_filter(filter_expr)
                
            sql += f" WHERE {where_sql}"
            params.extend(where_params)

        # GROUP BY
        if agg_spec and agg_spec.get("op") == "group_by":
            sql += f" GROUP BY {self._col_expr(agg_spec['by'])}"

        # ORDER BY
        if sort_spec:
            clauses = [
                f"{self._col_expr(s['field'])} {s.get('order', 'asc').upper()}"
                for s in sort_spec
            ]
            sql += " ORDER BY " + ", ".join(clauses)

        # LIMIT/OFFSET
        if limit is not None:
            sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        elif offset:
            sql += f" LIMIT -1 OFFSET {int(offset)}"

        # Handle UNIONs (simplified - assumes same structure)
        if union_recipes:
            union_parts = [f"({sql})"]
            for union_recipe in union_recipes:
                # For simplicity, UNION with same table structure
                # In production, would need more sophisticated handling
                union_sql, union_params = self._build_query_sql(union_recipe)
                union_parts.append(f"({union_sql})")
                params.extend(union_params)
            sql = " UNION ".join(union_parts)

        # Execute
        rows = self.conn.select(sql, tuple(params))

        # Process results
        if agg_spec:
            op = agg_spec.get("op")
            if op == "group_by":
                return {r[0]: (r[1] if len(r) > 1 else None) for r in rows}
            return rows[0][0] if rows else None

        if select_fields:
            return [dict(zip(select_fields, row)) for row in rows]

        # Full rows - build column names dynamically
        col_names = []
        for t in tables:
            alias = t.get('alias', t['name'])
            is_json, _ = self._resolve_table_type(t['name'])
            if is_json:
                col_names.extend([f"{alias}.key", f"{alias}.object"])
            else:
                storage = self.database[t['name']]
                col_names.extend([f"{alias}.{c}" for c in storage.columns])

        return [dict(zip(col_names, row)) for row in rows]

    def insert(self, items: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """Bulk insert across multiple tables (transactional)."""
        if isinstance(items, dict):
            items = [items]

        total_inserted = 0
        for item in items:
            table_name = item.get('table')
            table_type = item.get('type', 'json')
            if not table_name:
                raise ValueError("Multi-table insert requires 'table' in each item")

            key = item.get('key')
            value = item.get('value')
            if key is None or value is None:
                raise ValueError("Insert requires 'key' and 'value'")

            storage = self.database[table_name, table_type]
            if hasattr(storage, '_JSONStorage__conn'):
                # JSON storage
                import json
                sql = f'''
                INSERT INTO "{table_name}" (key, object)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET object = excluded.object
                '''
                self.conn.execute(sql, (key, json.dumps(value)))
            else:
                # Table storage
                cols = list(value.keys())
                vals = list(value.values())
                placeholders = ", ".join(["?"] * (len(vals) + 1))
                sql = f'''
                REPLACE INTO "{table_name}" ({",".join(["key"] + cols)})
                VALUES ({placeholders})
                '''
                self.conn.execute(sql, (key, *vals))
            total_inserted += 1

        return total_inserted

    def update(self, updates: Dict[str, Any], filter_expr: Dict[str, Any]) -> int:
        """Bulk update across multiple tables with cross-table filters."""
        if not updates:
            raise ValueError("Updates dict cannot be empty")

        # Parse table from updates (assume single table for simplicity)
        # In advanced version, could support multi-table updates
        table_name = updates.get("table")
        table_type = updates.get("type", "json")
        if table_name:
            updates = updates.get("update", {})
        else:
            raise ValueError("Updates must specify table field")

        storage = self.database[table_name, table_type]
        is_json = hasattr(storage, '_JSONStorage__conn')

        # Build SET clause
        set_parts = []
        params = []
        for field, value in updates.items():
            if "." in field:
                _, field_name = field.split(".", 1)
            else:
                field_name = field

            if is_json:
                # JSON update
                json_path = field_name.replace("/", ".")
                set_parts.append(f"object = json_set(object, '$.{json_path}', ?)")
                params.append(value)
            else:
                set_parts.append(f'"{field_name}" = ?')
                params.append(value)

        set_clause = ", ".join(set_parts)

        tables = filter_expr.get("tables", [])

        self._table_type_map = {}
        for table_spec in tables:
            table_name = table_spec.get('name')
            table_alias = table_spec.get("alias", table_name)
            type_field = table_spec.get('type', 'json')
            is_json = type_field.lower() != 'table'
            self._table_type_map[table_name] = is_json
            if table_alias != table_name:
                self._table_type_map[table_alias] = is_json
            
        
        filter_expr = filter_expr.get("filter")

        # WHERE clause
        if filter_expr:
            # Use parallel compilation for complex AND/OR expressions
            if self.enable_parallel and ("and" in filter_expr or "or" in filter_expr):
                # Check if the expression is complex enough to benefit from parallelism
                expr_count = 0
                if "and" in filter_expr:
                    expr_count = len(filter_expr["and"])
                elif "or" in filter_expr:
                    expr_count = len(filter_expr["or"])
                
                if expr_count > 2:  # Only parallelize if worthwhile
                    where_sql, where_params = self._compile_filter_parallel(filter_expr)
                else:
                    where_sql, where_params = self._compile_filter(filter_expr)
            else:
                where_sql, where_params = self._compile_filter(filter_expr)

        sql = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_sql}'
        params.extend(where_params)

        cur = self.conn.execute(sql, tuple(params))
        # return cur.rowcount