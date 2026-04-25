"""Interactive REST shell for DB86 REST service."""

import json
import urllib.error
import urllib.parse
import urllib.request

import click
import click_shell


local_storage: list = []

def cleanup(ctx):
    """Close all databases opened during this shell session and shut down."""
    for x in local_storage:
        api_request(ctx.obj['base_url'], 'POST', f'/databases/{urllib.parse.quote(x)}/close')
    local_storage.clear()
    click.echo("Shutting down REST shell...")


def api_request(base_url: str, method: str, path: str, data=None, params=None):
    """Send an HTTP request to the REST service and return parsed JSON."""
    base_url = base_url.rstrip("/")
    path = path.lstrip("/")
    url = f"{base_url}/{path}"
    if params:
        query_string = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{query_string}"

    body = None
    headers = {"Content-Type": "application/json"}
    if data is not None:
        body = json.dumps(data).encode("utf-8")

    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read().decode("utf-8")
            if not payload:
                return {}
            return json.loads(payload)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8")
        message = error_body
        try:
            error_json = json.loads(error_body)
            message = error_json.get("detail") or error_json.get("message") or error_body
        except Exception:
            pass
        raise click.ClickException(f"HTTP {exc.code}: {message}")
    except urllib.error.URLError as exc:
        raise click.ClickException(f"Connection error: {exc.reason}")


def print_response(response):
    """Pretty-print JSON responses in the shell."""
    if response is None:
        return
    try:
        click.echo(json.dumps(response, indent=2, ensure_ascii=False))
    except (TypeError, ValueError):
        click.echo(str(response))

prompt_base = "restx"
prompt_suffix = "> "
current_db = None
current_storage = None

@click_shell.shell(
    prompt=lambda: f"{prompt_base}{':' + current_db if current_db else ''}{'/' + current_storage if current_storage else ''}{prompt_suffix}",
    intro='DB86 REST Shell v1.0',
    on_finished=cleanup,
)
@click.option(
    '--base-url',
    default='http://127.0.0.1:8000',
    show_default=True,
    help='Base URL for the DB86 REST service.',
)
@click.pass_context
def cli(ctx, base_url):
    """Start the DB86 REST shell."""
    ctx.ensure_object(dict)
    ctx.obj['base_url'] = base_url.rstrip('/')
    ctx.obj['current_db'] = None
    ctx.obj['current_storage'] = None
    click.echo(f"Using REST service at {ctx.obj['base_url']}")

@cli.command('health', short_help='Check REST service health')
@click.pass_context
def health(ctx):
    """Check the health / uptime of the REST service."""
    response = api_request(ctx.obj['base_url'], 'GET', '/')
    print_response(response)


@cli.command('databases', short_help='List open databases')
@click.pass_context
def list_databases(ctx):
    """List all databases currently open on the REST service."""
    response = api_request(ctx.obj['base_url'], 'GET', '/databases')
    print_response(response)


@cli.command('create', short_help='Create a database')
@click.option('--autocommit/--no-autocommit', default=True, show_default=True)
@click.option('--journal-mode', default='WAL', show_default=True)
@click.option('--flag', default='c', show_default=True)
@click.option('--memory', is_flag=True, default=False, show_default=True)
@click.argument('db', type=str)
@click.pass_context
def create_database(ctx, autocommit, journal_mode, flag, memory, db):
    """Create a new database via the REST service."""
    payload = {
        'name': db,
        'autocommit': autocommit,
        'journal_mode': journal_mode,
        'flag': flag,
        'memory': memory,
    }
    response = api_request(ctx.obj['base_url'], 'POST', '/databases', data=payload)
    print_response(response)
    local_storage.append(db) if db not in local_storage else None


@cli.command('close', short_help='Close and remove a database')
@click.argument('db', type=str)
@click.pass_context
def close_database(ctx, db):
    """Close a database via the REST service."""
    response = api_request(ctx.obj['base_url'], 'POST', f'/databases/{urllib.parse.quote(db)}/close')
    print_response(response)
    local_storage.remove(db)


@cli.command('ls', short_help='List contents of current context')
@click.argument('path', type=str, default='/')
@click.pass_context
def list_contents(ctx, path):
    """Navigate REST resources by path: /, db, db/storage, or db/storage/query."""
    if path == '/' or path == '':
        response = api_request(ctx.obj['base_url'], 'GET', '/databases')
        print_response(response)
    else:
        path = path.split('/', 2)
        if len(path) == 1:
            response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(path[0])}/storages')
            print_response(response)
        elif len(path) == 2:
            response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(path[0])}/storages/{urllib.parse.quote(path[1])}/items')
            print_response(response)
        else:
            ctx.invoke(get, db=path[0], storage=path[1], query=path[2])

@cli.command('storages', short_help='List storages for a database')
@click.argument('db', type=str, required=False)
@click.pass_context
def list_storages(ctx, db):
    """List storages for a database via the REST service."""
    if db is None:
        db = ctx.obj['current_db']
        if not db:
            click.echo("No current database. Use 'cd <db>' or specify database name.")
            return
    response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(db)}/storages')
    print_response(response)


@cli.command('create-storage', short_help='Create a new storage')
@click.option('--storage-type', default='json', show_default=True, type=click.Choice(['json', 'table']))
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def create_storage(ctx, storage_type, db, storage):
    """Create a new storage in a database via the REST service."""
    payload = {'name': storage, 'storage_type': storage_type}
    response = api_request(ctx.obj['base_url'], 'POST', f'/databases/{urllib.parse.quote(db)}/storages', data=payload)
    print_response(response)


@cli.command('delete-storage', short_help='Delete a storage from a database')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def delete_storage(ctx, db, storage):
    """Delete a storage from a database via the REST service."""
    response = api_request(ctx.obj['base_url'], 'DELETE', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}')
    print_response(response)


@cli.command('storage', short_help='Get storage metadata')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.pass_context
def storage_info(ctx, db, storage):
    """Display metadata for a specific storage via the REST service."""
    response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}')
    print_response(response)


@cli.command('items', short_help='List all items in a storage')
@click.argument('db', type=str, required=False)
@click.argument('storage', type=str, required=False)
@click.option('--limit', type=int, default=None)
@click.option('--offset', type=int, default=0)
@click.pass_context
def list_items(ctx, db, storage, limit, offset):
    """List items in a storage with optional pagination via the REST service."""
    if db is None:
        db = ctx.obj['current_db']
        if not db:
            click.echo("No current database. Use 'cd <db>' or specify database name.")
            return
    if storage is None:
        storage = ctx.obj['current_storage']
        if not storage:
            click.echo("No current storage. Use 'cd <storage>' or specify storage name.")
            return
    params = {}
    if limit is not None:
        params['limit'] = limit
    if offset:
        params['offset'] = offset
    response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/items', params=params)
    print_response(response)


@cli.command('get', short_help='Query JSON storage path')
@click.option('--db', default='', help='Database name')
@click.option('--storage', default='', help='Storage name')
@click.argument('query', type=str)
@click.pass_context
def get(ctx, db, storage, query):
    """Query a JSON storage path across one or all databases and storages."""
    if db == '':
        db = ctx.obj['current_db'] or ''
    if storage == '':
        storage = ctx.obj['current_storage'] or ''
    base_url = ctx.obj['base_url']
    if db == '':
        response = api_request(base_url, 'GET', '/databases')
        databases = response.get('databases', [])
        results = {}
        for database in databases:
            results[database] = {}
            storage_response = api_request(base_url, 'GET', f'/databases/{urllib.parse.quote(database)}/storages')
            for store in [storage] if storage else [s['name'] for s in storage_response.get('storages', [])]:
                item_response = api_request(base_url, 'GET', f'/databases/{urllib.parse.quote(database)}/storages/{urllib.parse.quote(store)}/{urllib.parse.quote(query, safe="/")}')
                results[database][store] = item_response.get('results', item_response)
        print_response(results)
    elif storage == '':
        storages_response = api_request(base_url, 'GET', f'/databases/{urllib.parse.quote(db)}/storages')
        results = {}
        for store in [s['name'] for s in storages_response.get('storages', [])]:
            item_response = api_request(base_url, 'GET', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(store)}/{urllib.parse.quote(query, safe="/")}')
            results[store] = item_response.get('results', item_response)
        print_response({db: results})
    else:
        response = api_request(base_url, 'GET', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/{urllib.parse.quote(query, safe="/")}')
        print_response(response)
    local_storage.append(db) if db and db not in local_storage else None


@cli.command('get-item', short_help='Read a single item from storage')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.pass_context
def get_item(ctx, db, storage, key):
    """Read a single item from a storage by key via the REST service."""
    response = api_request(ctx.obj['base_url'], 'GET', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/items/{urllib.parse.quote(key)}')
    print_response(response)


@cli.command('insert', short_help='Bulk upsert multiple items into a storage')
@click.option('--storage-type', default=None, type=click.Choice(['json', 'table']), help='Optional storage type override')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('items', type=str)
@click.pass_context
def bulk_upsert(ctx, db, storage, items, storage_type):
    """Bulk upsert multiple items (given as a JSON object) into a storage."""
    try:
        parsed = json.loads(items)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f'Invalid JSON for bulk upsert items: {exc}')

    if not isinstance(parsed, dict):
        raise click.ClickException('Bulk upsert items must be a JSON object keyed by item key.')

    params = {}
    if storage_type:
        params['storage_type'] = storage_type

    payload = {'items': parsed}
    response = api_request(ctx.obj['base_url'], 'POST', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/items', data=payload, params=params)
    print_response(response)


@cli.command('put-item', short_help='Create or update a storage item')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.argument('value', type=str)
@click.pass_context
def put_item(ctx, db, storage, key, value):
    """Create or update a single item in a storage via the REST service."""
    try:
        item_value = json.loads(value)
    except json.JSONDecodeError:
        raise click.ClickException('Item value must be valid JSON.')
    payload = {'value': item_value}
    response = api_request(ctx.obj['base_url'], 'PUT', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/items/{urllib.parse.quote(key)}', data=payload)
    print_response(response)


@cli.command('delete-item', short_help='Delete a storage item')
@click.argument('db', type=str)
@click.argument('storage', type=str)
@click.argument('key', type=str)
@click.pass_context
def delete_item(ctx, db, storage, key):
    """Delete a single item from a storage via the REST service."""
    response = api_request(ctx.obj['base_url'], 'DELETE', f'/databases/{urllib.parse.quote(db)}/storages/{urllib.parse.quote(storage)}/items/{urllib.parse.quote(key)}')
    print_response(response)


if __name__ == '__main__':
    cli()
