import datetime
import logging
import multiprocessing
import sys
import os
import threading
from typing import Any, Dict, Optional
import click
from daemonocle import Daemon
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel

from db86.database import Database
from db86.storages import JSONStorage, Table

store: Dict[str, Database] = {}

logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s:\t[%(name)s] %(message)s'
)
log = logging.getLogger("DB86 REST Service")


def validate_storage_type(storage_type: str) -> str:
    """
    Validates and normalizes the storage type.

    Args:
        storage_type (str): The storage type to validate ('json' or 'table').

    Returns:
        str: The normalized storage type in lowercase.

    Raises:
        HTTPException: If the storage type is not 'json' or 'table'.
    """
    storage_type = storage_type.lower()
    if storage_type not in {"json", "table"}:
        raise HTTPException(status_code=400, detail="storage_type must be 'json' or 'table'")
    return storage_type


def get_database(name: str) -> Database:
    """
    Retrieves a database from the store by name.

    Args:
        name (str): The name of the database.

    Returns:
        Database: The database instance.

    Raises:
        HTTPException: If the database is not found.
    """
    try:
        return store[name]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Database '{name}' not found")

def store_close_database(name: str) -> None:
    """
    Closes a database and removes it from the store.

    Args:
        name (str): The name of the database to close.

    Returns:
        Database: The database instance.

    Raises:
        HTTPException: If the database is not found.
    """
    try:
        db = store.pop(name)
        db.close()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Database '{name}' not found")


app_start_time = datetime.datetime.now(datetime.timezone.utc)


def _format_uptime(start: datetime.datetime) -> Dict[str, Any]:
    """Return uptime metadata from the given start time."""
    now = datetime.datetime.now(datetime.timezone.utc)
    uptime = now - start
    seconds = int(uptime.total_seconds())
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    formatted = f"{days}d {hours:02}:{minutes:02}:{secs:02}"
    return {
        "started_at": start.replace(microsecond=0).isoformat() + "Z",
        "uptime": formatted,
        "uptime_seconds": seconds,
        "current_time": now.replace(microsecond=0).isoformat() + "Z",
    }

def _get_system_metrics() -> Dict[str, Any]:
    """Gather system resource metrics."""
    import psutil
    try:
        process = psutil.Process(os.getpid())
        
        # Memory info
        mem_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        # CPU info
        cpu_percent = process.cpu_percent(interval=0.1)
        
        # System-wide info
        vm = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            "process": {
                "memory_mb": round(mem_info.rss / 1024 / 1024, 2),
                "memory_percent": round(memory_percent, 2),
                "cpu_percent": round(cpu_percent, 2),
            },
            "system": {
                "memory_available_mb": round(vm.available / 1024 / 1024, 2),
                "memory_percent": round(vm.percent, 2),
                "disk_usage_percent": round(disk.percent, 2),
            }
        }
    except Exception as e:
        log.warning(f"Failed to gather system metrics: {e}")
        return {"error": "Unable to gather system metrics"}

def _configure_service_logging(logfile: Optional[str], loglevel: str) -> None:
    """Configure default logging handlers for the REST service."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    root.setLevel(loglevel.upper())
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t[%(name)s] %(message)s")

    if logfile is None or logfile == "-":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(logfile)

    handler.setFormatter(formatter)
    handler.setLevel(loglevel.upper())
    root.addHandler(handler)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.propagate = True
        logger.setLevel(loglevel.upper())


def infer_storage_type(db: Database, storage_name: str) -> str:
    """
    Infers the storage type of a storage in the database.

    Args:
        db (Database): The database instance.
        storage_name (str): The name of the storage.

    Returns:
        str: 'json' if the storage has columns ['key', 'object'], otherwise 'table'.
    """
    GET_COLS = f'PRAGMA TABLE_INFO("{storage_name}")'
    schema = db.conn.select(GET_COLS)
    cols = [x[1] for x in schema]
    if cols == ["key", "object"]:
        return "json"
    return "table"


def get_storage(db: Database, storage_name: str, storage_type: Optional[str] = None):
    """
    Retrieves a storage instance from the database.

    Args:
        db (Database): The database instance.
        storage_name (str): The name of the storage.
        storage_type (Optional[str]): The expected storage type ('json' or 'table'). If None, infers the type.

    Returns:
        JSONStorage or Table: The storage instance.

    Raises:
        HTTPException: If the storage is not found or type mismatch.
    """
    if storage_name not in db:
        raise HTTPException(status_code=404, detail=f"Storage '{storage_name}' not found")

    actual_type = infer_storage_type(db, storage_name)
    if storage_type is not None:
        storage_type = validate_storage_type(storage_type)
        if actual_type != storage_type:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Storage '{storage_name}' already exists as '{actual_type}'. "
                    f"Use storage_type='{actual_type}' or delete and recreate it."
                ),
            )
    else:
        storage_type = actual_type

    if storage_type == "table":
        return Table(storage_name, db.conn, db.flag)
    return JSONStorage(storage_name, db.conn, db.flag)


def table_row_as_dict(table: Table, key: str):
    """
    Converts a table row to a dictionary.

    Args:
        table (Table): The table instance.
        key (str): The key of the row.

    Returns:
        dict: The row as a dictionary with column names as keys.

    Raises:
        KeyError: If the key is not found in the table.
    """
    if key not in table:
        raise KeyError(key)

    cols = table.columns
    if len(cols) == 1:
        return {cols[0]: key}

    row = table[(key, *cols[1:])]
    return {cols[0]: key, **dict(zip(cols[1:], row))}


class DatabaseCreateRequest(BaseModel):
    """
    Request model for creating a database.
    """
    name: str
    autocommit: bool = True
    journal_mode: str = "WAL"
    flag: str = "c"
    memory: bool = False


class StorageCreateRequest(BaseModel):
    """
    Request model for creating a storage.
    """
    name: str
    storage_type: str = "json"


class ItemPayload(BaseModel):
    """
    Payload model for a single item.
    """
    value: Any


class BulkItemsPayload(BaseModel):
    """
    Payload model for bulk items.
    """
    items: Dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.

    Handles startup and shutdown events, including closing databases.
    """
    log.info("Starting Up!")
    yield
    log.info("Shutting Down...")
    for db_name, db in list(store.items()):
        log.info(f"Closing {db_name}...")
        db.close()
    log.info("Goodbye!")


app = FastAPI(
    title="DB86 REST Service",
    description="REST service for DB86 based SQLite3 databases",
    version='0.2.0',
    lifespan=lifespan,
)


@app.get("/")
def health_check():
    """
    Health check endpoint.

    Returns the status of the service, uptime, and number of open databases.
    """
    log.info("Health check requested")
    uptime_info = _format_uptime(app_start_time)
    system_metric = _get_system_metrics()
    return {
        "status": "Success",
        "message": "DB86 REST Service is running",
        "service_version": "0.1.0",
        "databases_open": len(store),
        "system_metrics": system_metric,
        **uptime_info
    }


@app.post("/databases", status_code=201)
def create_database(request: DatabaseCreateRequest):
    """
    Creates a new database.

    Args:
        request (DatabaseCreateRequest): The database creation parameters.

    Returns:
        dict: Success response with database details.

    Raises:
        HTTPException: If the database already exists or creation fails.
    """
    log.info(f"Create database request: {request.model_dump()}")
    if request.name in store:
        log.warning("Attempt to create existing database: %s", request.name)
        raise HTTPException(status_code=409, detail=f"Database '{request.name}' already exists")

    filename = ":memory:" if request.memory else f"{request.name}.db"
    try:
        store[request.name] = Database(
            filename,
            autocommit=request.autocommit,
            journal_mode=request.journal_mode,
            flag=request.flag,
        )
        log.info("Created database '%s'", request.name)
        return {
            "status": "Success",
            "message": f"Created database '{request.name}'",
            "database": request.name,
        }
    except Exception as exc:
        log.error("Failed to create database '%s': %s", request.name, exc)
        raise HTTPException(status_code=500, detail="Failed to create database")


@app.get("/databases")
def list_databases():
    """
    Lists all open databases.

    Returns:
        dict: List of database names.
    """
    log.info("List databases request")
    return {"databases": list(store.keys())}


@app.get("/databases/{db_name}")
def get_database_metadata(db_name: str):
    """
    Retrieves metadata for a specific database.

    Args:
        db_name (str): The name of the database.

    Returns:
        dict: Database metadata including storages, indices, views, etc.

    Raises:
        HTTPException: If the database is not found.
    """
    log.info("Get database metadata request: %s", db_name)
    db = get_database(db_name)
    return {
        "name": db_name,
        "filename": db.filename,
        "autocommit": db.autocommit,
        "journal_mode": db.journal_mode,
        "flag": db.flag,
        "storages": db.storages,
        "indices": db.indices,
        "views": db.views,
    }

@app.post("/databases/{db_name}/close")
def close_database(db_name: str):
    """
    Closes a specific database.

    Args:
        db_name (str): The name of the database.

    Returns:
        dict: Database closure confirmation.

    Raises:
        HTTPException: If the database is not found.
    """
    log.info("Closing database: %s", db_name)
    store_close_database(db_name)
    return {
        "name": db_name,
        "message": f"Database '{db_name}' closed successfully",
        "status": "Success"
    }


@app.delete("/databases/{db_name}")
def delete_database(db_name: str):
    """
    Deletes a database and closes it.

    Args:
        db_name (str): The name of the database to delete.

    Returns:
        dict: Success response.

    Raises:
        HTTPException: If the database is not found or deletion fails.
    """
    log.info("Delete database request: %s", db_name)
    db = get_database(db_name)
    try:
        db.close()
        del store[db_name]
        log.info("Deleted database '%s'", db_name)
        return {"status": "Success", "message": f"Deleted database '{db_name}'"}
    except Exception as exc:
        log.error("Failed to delete database '%s': %s", db_name, exc)
        raise HTTPException(status_code=500, detail="Failed to delete database")


@app.post("/databases/{db_name}/storages", status_code=201)
def create_storage(db_name: str, request: StorageCreateRequest):
    """
    Creates a new storage in a database.

    Args:
        db_name (str): The name of the database.
        request (StorageCreateRequest): The storage creation parameters.

    Returns:
        dict: Success response with storage details.

    Raises:
        HTTPException: If the database or storage already exists, or creation fails.
    """
    log.info(f"Create storage request for database '{db_name}': {request.model_dump()}")
    db = get_database(db_name)
    storage_type = validate_storage_type(request.storage_type)
    if request.name in db:
        log.warning(f"Storage '{request.name}' already exists in database '{db_name}'")
        raise HTTPException(status_code=409, detail=f"Storage '{request.name}' already exists")

    try:
        if storage_type == "table":
            db[request.name, "table"]
        else:
            db[request.name, "json"]
        log.info(f"Created {storage_type} storage '{request.name}' in database '{db_name}'")
        return {
            "status": "Success",
            "message": f"Created {storage_type} storage '{request.name}'",
            "storage": request.name,
            "storage_type": storage_type,
        }
    except Exception as exc:
        log.error(f"Failed to create storage '{request.name}' in database '{db_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to create storage")


@app.get("/databases/{db_name}/storages")
def list_storages(db_name: str):
    """
    Lists all storages in a database.

    Args:
        db_name (str): The name of the database.

    Returns:
        dict: List of storages with their types.

    Raises:
        HTTPException: If the database is not found.
    """
    log.info("List storages request for database '%s'", db_name)
    db = get_database(db_name)
    storages = []
    for storage_name in db.storages:
        try:
            storage_type = infer_storage_type(db, storage_name)
        except Exception:
            storage_type = "unknown"
        storages.append({"name": storage_name, "storage_type": storage_type})
    return {"storages": storages}


@app.get("/databases/{db_name}/storages/{storage_name}")
def get_storage_metadata(
    db_name: str,
    storage_name: str,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Retrieves metadata for a specific storage.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: Storage metadata including type, entries, and columns (for tables).

    Raises:
        HTTPException: If the database or storage is not found.
    """
    log.info(f"Get storage metadata request for '{storage_name}' in database '{db_name}'")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)
    metadata = {
        "name": storage_name,
        "storage_type": infer_storage_type(db, storage_name),
        "entries": len(storage),
    }
    if isinstance(storage, Table):
        metadata["columns"] = storage.columns
    return metadata


@app.delete("/databases/{db_name}/storages/{storage_name}")
def delete_storage(db_name: str, storage_name: str):
    """
    Deletes a storage from a database.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage to delete.

    Returns:
        dict: Success response.

    Raises:
        HTTPException: If the database or storage is not found, or deletion fails.
    """
    log.info("Delete storage request for '%s' in database '%s'", storage_name, db_name)
    db = get_database(db_name)
    if storage_name not in db:
        log.warning(f"Storage '{storage_name}' not found in database '{db_name}'")
        raise HTTPException(status_code=404, detail=f"Storage '{storage_name}' not found")

    try:
        del db[storage_name]
        log.info(f"Deleted storage '{storage_name}' from database '{db_name}'")
        return {"status": "Success", "message": f"Deleted storage '{storage_name}'"}
    except Exception as exc:
        log.error(f"Failed to delete storage '{storage_name}' from database '{db_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to delete storage")


@app.get("/databases/{db_name}/storages/{storage_name}/items")
def list_storage_items(
    db_name: str,
    storage_name: str,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
):
    """
    Lists items in a storage with optional pagination.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        storage_type (Optional[str]): Optional storage type override.
        limit (Optional[int]): Maximum number of items to return.
        offset (int): Number of items to skip.

    Returns:
        dict: List of items.

    Raises:
        HTTPException: If the database or storage is not found.
    """
    log.info(f"List items request for storage '{storage_name}' in database '{db_name}' (limit={limit} offset={offset})")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)

    if isinstance(storage, JSONStorage):
        items = list(storage.to_dict().items())
    else:
        items = []
        for key in storage:
            try:
                items.append(table_row_as_dict(storage, key))
            except KeyError:
                continue

    if limit is not None:
        items = items[offset : offset + limit]
    elif offset:
        items = items[offset:]

    return {"items": items}


@app.get("/databases/{db_name}/storages/{storage_name}/items/{item_key}")
def get_storage_item(
    db_name: str,
    storage_name: str,
    item_key: str,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Retrieves a specific item from a storage.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        item_key (str): The key of the item.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: The item data.

    Raises:
        HTTPException: If the database, storage, or item is not found.
    """
    log.info(f"Get item request '{item_key}' from storage '{storage_name}' in database '{db_name}'")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)

    try:
        if isinstance(storage, JSONStorage):
            result = {"key": item_key, "value": storage[item_key]}
        else:
            result = {"key": item_key, "value": table_row_as_dict(storage, item_key)}
        log.info(f"Fetched item '{item_key}' from storage '{storage_name}'")
        return result
    except KeyError:
        log.warning(f"Item '{item_key}' not found in storage '{storage_name}'")
        raise HTTPException(status_code=404, detail=f"Item '{item_key}' not found")
    except Exception as exc:
        log.error(f"Failed to read item '{item_key}' from storage '{storage_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to read item")


@app.put("/databases/{db_name}/storages/{storage_name}/items/{item_key:path}")
def upsert_storage_item(
    db_name: str,
    storage_name: str,
    item_key: str,
    payload: ItemPayload,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Inserts or updates an item in a storage.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        item_key (str): The key of the item.
        payload (ItemPayload): The item data.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: Success response.

    Raises:
        HTTPException: If the database or storage is not found, or invalid data.
    """
    log.info(f"Upsert item request '{item_key}' in storage '{storage_name}' of database '{db_name}'")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)
    try:
        if isinstance(storage, JSONStorage):
            if not isinstance(payload.value, dict):
                log.warning(f"Invalid value for JSON storage on item '{item_key}'")
                raise HTTPException(status_code=400, detail="JSON storage requires a JSON object for value")
            storage[item_key] = payload.value
        else:
            if isinstance(payload.value, list):
                storage[item_key] = tuple(payload.value)
            elif isinstance(payload.value, dict):
                if storage.columns and storage.columns[0] not in payload.value:
                    payload.value[storage.columns[0]] = item_key
                storage[item_key] = payload.value
            else:
                log.warning(f"Invalid value type for table storage item '{item_key}'")
                raise HTTPException(status_code=400, detail="Table storage requires a dict or list value")
        log.info(f"Stored item '{item_key}' in storage '{storage_name}'")
        return {"status": "Success", "message": f"Stored item '{item_key}'"}
    except HTTPException:
        raise
    except Exception as exc:
        log.error(f"Failed to store item '{item_key}' in storage '{storage_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to store item")


@app.post("/databases/{db_name}/storages/{storage_name}/items", status_code=200)
def bulk_upsert_storage_items(
    db_name: str,
    storage_name: str,
    payload: BulkItemsPayload,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Bulk inserts or updates multiple items in a storage.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        payload (BulkItemsPayload): The bulk item data.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: Success response with list of upserted items.

    Raises:
        HTTPException: If the database or storage is not found, or invalid data.
    """
    log.info(
        "Bulk upsert request for storage '%s' in database '%s' with %d items",
        storage_name,
        db_name,
        len(payload.items),
    )
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)

    if not payload.items:
        log.info(f"Bulk upsert request contained no items for storage '{storage_name}' in database '{db_name}'")
        return {"status": "Success", "message": "No items to upsert", "items": []}

    try:
        items_written = []
        if isinstance(storage, JSONStorage):
            for item_key, item_value in payload.items.items():
                if not isinstance(item_value, dict):
                    log.warning(f"Invalid JSON storage value for item '{item_key}' in storage '{storage_name}' in database '{db_name}'")
                    raise HTTPException(
                        status_code=400,
                        detail="JSON storage requires objects for all item values",
                    )
                storage[item_key] = item_value
                items_written.append(item_key)
        else:
            for item_key, item_value in payload.items.items():
                if isinstance(item_value, list):
                    storage[item_key] = tuple(item_value)
                elif isinstance(item_value, dict):
                    if storage.columns and storage.columns[0] not in item_value:
                        item_value = {**item_value, storage.columns[0]: item_key}
                    storage[item_key] = item_value
                else:
                    log.warning(f"Invalid table storage value for item '{item_key}'")
                    raise HTTPException(
                        status_code=400,
                        detail="Table storage requires a dict or list for all item values",
                    )
                items_written.append(item_key)

        log.info(
            f"Bulk upsert completed for storage '{storage_name}' in database '{db_name}'; items_written={len(items_written)}",
        )
        return {
            "status": "Success",
            "message": f"Upserted {len(items_written)} items",
            "items": items_written,
        }
    except HTTPException:
        raise
    except Exception as exc:
        log.error(f"Failed bulk upsert for storage '{storage_name}' in database '{db_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to upsert storage items")


@app.delete("/databases/{db_name}/storages/{storage_name}/items/{item_key}")
def delete_storage_item(
    db_name: str,
    storage_name: str,
    item_key: str,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Deletes an item from a storage.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage.
        item_key (str): The key of the item to delete.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: Success response.

    Raises:
        HTTPException: If the database, storage, or item is not found.
    """
    log.info(f"Delete item request '{item_key}' from storage '{storage_name}' in database '{db_name}'")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)
    try:
        del storage[item_key]
        log.info(f"Deleted item '{item_key}' from storage '{storage_name}' in database '{db_name}'")
        return {"status": "Success", "message": f"Deleted item '{item_key}'"}
    except KeyError:
        log.warning(f"Item '{item_key}' not found in storage '{storage_name}' in database '{db_name}'")
        raise HTTPException(status_code=404, detail=f"Item '{item_key}' not found")
    except Exception as exc:
        log.error(f"Failed to delete item '{item_key}' from storage '{storage_name}' in database '{db_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to delete item")


@app.get("/databases/{db_name}/storages/{storage_name}/{query:path}")
def query_storage_path(
    db_name: str,
    storage_name: str,
    query: str,
    storage_type: Optional[str] = Query(None, description="Optional storage type override: json or table"),
):
    """
    Queries a JSON storage using a path expression.

    Args:
        db_name (str): The name of the database.
        storage_name (str): The name of the storage (must be JSON).
        query (str): The path query string.
        storage_type (Optional[str]): Optional storage type override.

    Returns:
        dict: Query results.

    Raises:
        HTTPException: If the database or storage is not found, or not JSON storage.
    """
    log.info(f"Path query request '{query}' for storage '{storage_name}' in database '{db_name}'")
    db = get_database(db_name)
    storage = get_storage(db, storage_name, storage_type)
    if not isinstance(storage, JSONStorage):
        log.warning(f"Path queries requested for non-JSON storage '{storage_name}' in database '{db_name}'")
        raise HTTPException(status_code=400, detail="Path queries are only supported for JSON storage")
    try:
        results = list(storage.get_path(query, None))
        log.info(f"Path query returned {len(results)} results for storage '{storage_name}' in database '{db_name}'")
        return {"path": query, "results": results}
    except Exception as exc:
        log.error(f"Failed to query path '{query}' in storage '{storage_name}' in database '{db_name}': {exc}")
        raise HTTPException(status_code=500, detail="Failed to query storage path")

# Server daemon CLI using Click

@click.group()
@click.pass_context
def cli(ctx):
    """DB86 REST Service daemon manager."""
    ctx.obj = {}

@cli.command(name="run")
@click.option("--host", default="127.0.0.1", show_default=True, help="Host address to bind the REST service.")
@click.option("--port", default=8000, show_default=True, type=int, help="Port to bind the REST service.")
@click.option("--workers", default=1, show_default=True, type=int, help="Number of worker processes to run.")
@click.option(
    "--logfile",
    type=click.Path(dir_okay=False, writable=True, allow_dash=True),
    default="-",
    show_default=True,
    help="Path to a log file for the service, or '-' to write logs to stdout.",
)
@click.option("--loglevel", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error", "critical"], case_sensitive=False), help="Logging level for the REST service.")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload for development (not recommended with workers > 1).")
@click.pass_context
def run(ctx, host: str, port: int, workers: int, logfile: Optional[str], loglevel: str, reload: bool):
    """Run the DB86 REST service as a daemon with logging and worker configuration."""
    if reload and workers != 1:
        raise click.ClickException("Reload mode cannot be used with multiple workers.")

    _configure_service_logging(logfile, loglevel)
    log.info("Starting DB86 REST service daemon on %s:%s with %s worker(s)", host, port, workers)
    if logfile:
        log.info("Service logs will be written to %s", logfile)

    config = uvicorn.Config(
        "db86.service.rest_service:app",
        host=host,
        port=port,
        log_level=loglevel,
        workers=workers,
        reload=reload,
    )
    server = uvicorn.Server(config)
    server.run()

@cli.command(name="start")
@click.option("--host", default="127.0.0.1", show_default=True, help="Host address to bind the REST service.")
@click.option("--port", default=8000, show_default=True, type=int, help="Port to bind the REST service.")
@click.option("--workers", default=1, show_default=True, type=int, help="Number of worker processes to run.")
@click.option(
    "--logfile",
    type=click.Path(dir_okay=False, writable=True, allow_dash=True),
    default="-",
    show_default=True,
    help="Path to a log file for the service, or '-' to write logs to stdout.",
)
@click.option("--loglevel", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error", "critical"], case_sensitive=False), help="Logging level for the REST service.")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload for development (not recommended with workers > 1).")
@click.pass_context
def start(ctx, host: str, port: int, workers: int, logfile: Optional[str], loglevel: str, reload: bool):
    """Run the DB86 REST service as a daemon with logging and worker configuration."""
    if reload and workers != 1:
        raise click.ClickException("Reload mode cannot be used with multiple workers.")

    _configure_service_logging(logfile, loglevel)
    log.info("Starting DB86 REST service daemon on %s:%s with %s worker(s)", host, port, workers)
    if logfile:
        log.info("Service logs will be written to %s", logfile)

    config = uvicorn.Config(
        "db86.service.rest_service:app",
        host=host,
        port=port,
        log_level=loglevel,
        workers=workers,
        reload=reload,
    )
    server = uvicorn.Server(config)
    runner = Daemon(
        'DB86 REST Service',
        worker=server.run,
        pidfile='db86_rest_service.pid',
        work_dir='.',
        stdout_file=logfile if logfile and logfile != "-" else None,
        stderr_file=logfile if logfile and logfile != "-" else None,
        uid=os.getuid(), gid=os.getgid()
    )
    runner.do_action('start')

@cli.command('stop', short_help='Stop the DB86 REST Service')
@click.option('-f', '--force', help='Force Stop', is_flag=True, default=False)
def stop(force):
    """Stop the DB86 REST Service daemon."""
    daemon = Daemon('DB86 REST Service', pidfile='db86_rest_service.pid')
    daemon.stop(force=force)


@cli.command('status', short_help='DB86 REST Service Status')
@click.option('-j', '--json', help='Return Status JSON', default=False, is_flag=True)
def status(json):
    """Print the status of the DB86 REST Service daemon."""
    daemon = Daemon('DB86 REST Service', pidfile='db86_rest_service.pid')
    daemon.status(json=json)


@cli.command('restart', short_help='Restart DB86 REST Service')
@click.option('-f', '--force', help='Force Stop', is_flag=True, default=False)
@click.option("--host", default="127.0.0.1", show_default=True, help="Host address to bind the REST service.")
@click.option("--port", default=8000, show_default=True, type=int, help="Port to bind the REST service.")
@click.option("--workers", default=1, show_default=True, type=int, help="Number of worker processes to run.")
@click.option(
    "--logfile",
    type=click.Path(dir_okay=False, writable=True, allow_dash=True),
    default="-",
    show_default=True,
    help="Path to a log file for the service, or '-' to write logs to stdout.",
)
@click.option("--loglevel", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error", "critical"], case_sensitive=False), help="Logging level for the REST service.")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload for development (not recommended with workers > 1).")
@click.pass_context
def restart(ctx, host, port, workers, logfile, loglevel, reload, force):
    """Restart the DB86 REST Service daemon (stop then start)."""
    ctx.invoke(stop, force=force)
    ctx.invoke(start, host=host, port=port, workers=workers, logfile=logfile, loglevel=loglevel, reload=reload)



if __name__ == "__main__":
    cli()

