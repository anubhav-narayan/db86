#!/usr/bin/env python3

import ast
import json
import pprint

import click
import click_shell
from .database import Database
from .storages import JSONStorage, Table


def pretty_print(value):
    """Pretty-print a value to the terminal using ``pprint`` for containers."""
    if value is None:
        click.echo('None')
        return
    if isinstance(value, (dict, list, tuple, set)):
        click.echo(pprint.pformat(value, indent=2, width=80, compact=False))
    else:
        click.echo(str(value))


def parse_value(value):
    """Attempt to parse *value* as JSON, then as a Python literal, falling back to a raw string."""
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        pass
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        pass
    return value


def ensure_db_store(ctx):
    """Ensure that ``ctx.obj['db_store']`` exists and return it."""
    ctx.ensure_object(dict)
    if 'db_store' not in ctx.obj:
        ctx.obj['db_store'] = {}
    return ctx.obj['db_store']


def get_db(ctx, db_name):
    """Retrieve an open database by name, or raise a ``ClickException``."""
    db_store = ensure_db_store(ctx)
    if db_name not in db_store:
        raise click.ClickException(f"Database '{db_name}' is not open.")
    return db_store[db_name]


def get_storage(db, storage_name):
    """Retrieve a storage from *db* by name, auto-detecting its type."""
    if storage_name not in db.storages:
        raise click.ClickException(f"Storage '{storage_name}' does not exist in database '{db.filename}'.")
    cols = db.conn.select(f'PRAGMA TABLE_INFO("{storage_name}")')
    headers = [row[1] for row in cols]
    if headers == ['key', 'object']:
        return db[storage_name, 'json']
    return db[storage_name, 'table']


def items_from_table(storage):
    """Return all rows from a ``Table`` storage as a list of dicts."""
    rows = []
    for key in storage:
        values = storage[key]
        if isinstance(values, tuple):
            rows.append({storage.columns[0]: key, **{col: val for col, val in zip(storage.columns[1:], values[1:])}})
        else:
            rows.append({storage.columns[0]: key, storage.columns[1]: values})
    return rows


def items_from_json(storage):
    """Return all items from a ``JSONStorage`` as a dict."""
    return storage.to_dict()


def cleanup(ctx):
    """Close all open databases and print a goodbye message."""
    click.echo("Autoclean...")
    if 'db_store' in ctx.obj:
        for name, db in ctx.obj['db_store'].items():
            click.echo(f"Cleaning {name}")
            if isinstance(db, Database):
                db.close()
    click.echo("Goodbye!")


@click_shell.shell(
    prompt='dbx> ',
    intro='DB86 Management Shell v1.0',
    on_finished=cleanup,
)
@click.pass_context
def cli(ctx):
    """DB86 interactive management shell entry point."""
    ctx.ensure_object(dict)
    if 'db_store' not in ctx.obj:
        ctx.obj['db_store'] = {}


@cli.command('databases', short_help='List open databases')
@click.pass_context
def list_databases(ctx):
    """List the names of all currently open databases."""
    db_store = ensure_db_store(ctx)
    pretty_print(list(db_store.keys()))


@cli.command('create', short_help='Create/Open a database')
@click.option('--autocommit/--no-autocommit', default=True, show_default=True)
@click.option('--journal-mode', default='DELETE', show_default=True)
@click.option('--flag', default='c', show_default=True, type=click.Choice(['c', 'r', 'w']))
@click.option('--memory', is_flag=True, default=False, show_default=True)
@click.argument('db', type=str)
@click.pass_context
def create_database(ctx, autocommit, journal_mode, flag, memory, db):
    """Create or open a database and add it to the shell session."""
    db_store = ensure_db_store(ctx)
    if db in db_store:
        click.echo(f"Database '{db}' is already open.")
        return
    filename = ':memory:' if memory else f'{db}.db'
    db_store[db] = Database(filename, flag=flag, autocommit=autocommit, journal_mode=journal_mode)
    click.echo(f"Opened database '{db}'.")


@cli.command('close', short_help='Close an open database')
@click.argument('db', default='', required=False)
@click.pass_context
def close_database(ctx, db):
    """Close one or all open databases in the shell session."""
    db_store = ensure_db_store(ctx)
    if db == '':
        for name, database in list(db_store.items()):
            database.close()
            del db_store[name]
        click.echo('Closed all databases.')
        return
    if db not in db_store:
        raise click.ClickException(f"No open database found with name '{db}'.")
    db_store[db].close()
    del db_store[db]
    click.echo(f"Closed database '{db}'.")


@cli.command('ls', short_help='List databases, storages, or items')
@click.argument('path', default='/', required=False)
@click.option('--limit', type=int, default=None)
@click.option('--offset', type=int, default=0)
@click.pass_context
def list_contents(ctx, path, limit, offset):
    """Navigate and list databases, storages, or storage items by path."""
    db_store = ensure_db_store(ctx)
    if path in ('/', ''):
        pretty_print(sorted(db_store.keys()))
        return
    path = [part for part in path.split('/') if part]
    if len(path) == 1:
        db_name = path[0]
        db = get_db(ctx, db_name)
        pretty_print(sorted(db.storages))
        return
    if len(path) == 2:
        db_name, storage_name = path
        db = get_db(ctx, db_name)
        storage = get_storage(db, storage_name)
        if isinstance(storage, JSONStorage):
            items = items_from_json(storage)
            items = list(items.items())
        else:
            items = items_from_table(storage)
        if offset:
            items = items[offset:]
        if limit is not None:
            items = items[:limit]
        pretty_print(items)
        return
    raise click.ClickException('Path must be /, <db>, or <db>/<storage>.')


@cli.command('storages', short_help='List storages for a database')
@click.argument('db', required=True)
@click.pass_context
def list_storages(ctx, db):
    """List all storages in the specified database."""
    db = get_db(ctx, db)
    pretty_print(sorted(db.storages))


@cli.command('create-storage', short_help='Create a new storage')
@click.option('--storage-type', default='json', show_default=True, type=click.Choice(['json', 'table']))
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def create_storage(ctx, storage_type, db, storage):
    """Create a new storage (JSON or Table) inside a database."""
    db = get_db(ctx, db)
    if storage in db.storages:
        raise click.ClickException(f"Storage '{storage}' already exists in database '{db.filename}'.")
    if storage_type == 'json':
        db[storage, 'json']
    else:
        db[storage, 'table']
    click.echo(f"Created {storage_type} storage '{storage}' in database '{db.filename}'.")


@cli.command('delete-storage', short_help='Delete a storage from a database')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def delete_storage(ctx, db, storage):
    """Delete a storage and its data from a database."""
    db = get_db(ctx, db)
    if storage not in db.storages:
        raise click.ClickException(f"Storage '{storage}' does not exist in database '{db.filename}'.")
    del db[storage]
    click.echo(f"Deleted storage '{storage}' from database '{db.filename}'.")


@cli.command('storage', short_help='Get storage metadata')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def storage_info(ctx, db, storage):
    """Display metadata (type, columns, length) for a storage."""
    db = get_db(ctx, db)
    storage_obj = get_storage(db, storage)
    metadata = {
        'name': storage,
        'storage_type': 'json' if isinstance(storage_obj, JSONStorage) else 'table',
        'columns': storage_obj.columns,
        'length': len(storage_obj),
    }
    pretty_print(metadata)


@cli.command('items', short_help='List all items in a storage')
@click.argument('db', required=True)
@click.argument('storage', required=True)
@click.option('--limit', type=int, default=None)
@click.option('--offset', type=int, default=0)
@click.pass_context
def list_items(ctx, db, storage, limit, offset):
    """List all items in a storage with optional pagination."""
    db = get_db(ctx, db)
    storage_obj = get_storage(db, storage)
    if isinstance(storage_obj, JSONStorage):
        items = items_from_json(storage_obj)
        items = list(items.items())
    else:
        items = items_from_table(storage_obj)
    if offset:
        items = items[offset:]
    if limit is not None:
        items = items[:limit]
    pretty_print(items)


@cli.command('get', short_help='Query JSON storage path or get item by key')
@click.option('--db', default='', help='Database name')
@click.option('--store', default='', help='Storage name')
@click.argument('query', type=str)
@click.pass_context
def get_query(ctx, db, store, query):
    """Query a JSON storage by path or retrieve an item by key across databases."""
    db_store = ensure_db_store(ctx)
    if not db_store:
        raise click.ClickException('No open databases.')

    def query_storage(db_obj, storage_name):
        storage_obj = get_storage(db_obj, storage_name)
        if isinstance(storage_obj, JSONStorage):
            return list(storage_obj.get_path(query, None))
        if query in storage_obj:
            return storage_obj[query]
        raise click.ClickException(f"No item with key '{query}' in storage '{storage_name}'.")

    results = {}
    if db == '':
        for db_name, db_obj in db_store.items():
            results[db_name] = {}
            candidates = [store] if store else db_obj.storages
            for storage_name in candidates:
                try:
                    results[db_name][storage_name] = query_storage(db_obj, storage_name)
                except click.ClickException as exc:
                    results[db_name][storage_name] = str(exc)
    else:
        db_obj = get_db(ctx, db)
        candidates = [store] if store else db_obj.storages
        for storage_name in candidates:
            try:
                results[storage_name] = query_storage(db_obj, storage_name)
            except click.ClickException as exc:
                results[storage_name] = str(exc)
    pretty_print(results)


@cli.command('get-item', short_help='Read a single item from storage')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.pass_context
def get_item(ctx, db, storage, key):
    """Read and display a single item from a storage by key."""
    db = get_db(ctx, db)
    storage_obj = get_storage(db, storage)
    item = storage_obj[key]
    pretty_print(item)


@cli.command('put-item', short_help='Create or update a storage item')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.argument('value', type=str)
@click.option('--storage-type', default=None, type=click.Choice(['json', 'table']), help='If storage does not exist, create it as this type.')
@click.pass_context
def put_item(ctx, db, storage, key, value, storage_type):
    """Create or update a storage item, auto-creating the storage if needed."""
    db = get_db(ctx, db)
    if storage not in db.storages:
        if storage_type is None:
            raise click.ClickException(f"Storage '{storage}' does not exist. Use --storage-type to create it.")
        if storage_type == 'json':
            db[storage, 'json']
        else:
            db[storage, 'table']
    storage_obj = get_storage(db, storage)
    parsed = parse_value(value)
    if isinstance(storage_obj, JSONStorage):
        if not isinstance(parsed, dict):
            parsed = {'value': parsed}
        storage_obj[key] = parsed
    else:
        if isinstance(parsed, dict):
            storage_obj[key] = parsed
        elif isinstance(parsed, (list, tuple)):
            storage_obj[key] = tuple(parsed)
        elif len(storage_obj.columns) == 2:
            storage_obj[key] = (parsed,)
        else:
            raise click.ClickException('Table storage requires a JSON object or tuple/list value for multiple columns.')
    click.echo(f"Upserted item '{key}' into storage '{storage}' in database '{db.filename}'.")


@cli.command('delete-item', short_help='Delete a single item from storage')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.pass_context
def delete_item(ctx, db, storage, key):
    """Delete a single item from a storage by key."""
    db = get_db(ctx, db)
    storage_obj = get_storage(db, storage)
    del storage_obj[key]
    click.echo(f"Deleted item '{key}' from storage '{storage}' in database '{db.filename}'.")

