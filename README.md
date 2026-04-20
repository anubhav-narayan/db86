# DB86 — the AE‑86 of databases

![License](https://img.shields.io/badge/licence-MIT-green)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Version](https://img.shields.io/badge/version-0.7.0-brightgreen)

DB86 is a robust Python 3.10+ wrapper around SQLite3 that provides a powerful, Pythonic interface for database operations. It supports both traditional relational tables and JSON document storage with a dict-like API, comprehensive multi-threading support, and optional REST service endpoints.

Built on lessons from [sqlitedict](https://github.com/RaRe-Technologies/sqlitedict), DB86 modernizes the approach with enhanced features, thread safety, and flexible storage backends.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Guide](#usage-guide)
  - [Basic Database Operations](#basic-database-operations)
  - [JSON Storage](#json-storage)
  - [Tables (Structured Storage)](#tables-structured-storage)
  - [Database Configuration](#database-configuration)
- [CLI Tool](#cli-tool)
- [REST Service](#rest-service)
- [Advanced Examples](#advanced-examples)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Features

✨ **Core Capabilities**
- 🗄️ **Dict-like Interface**: Intuitive Python dict API for all database operations
- 🔄 **Multi-threaded**: Thread-safe database access with built-in synchronization
- 💾 **Dual Storage Modes**: 
  - JSON storage for document-style data
  - Relational tables for structured data
- ⚙️ **Flexible Configuration**: Autocommit, journal modes, and timeout controls
- 🚀 **In-Memory & File-Based**: Support for both `:memory:` and persistent databases
- 🔌 **REST API**: Built-in FastAPI service for remote database access
- 🎮 **CLI Management**: Interactive shells for database & REST administration
- 📤 **Multiple Flags**: Read-only ('r'), read-write ('c'), and write-fresh ('w') modes
- 🔍 **Schema Inspection**: Database describe(), table metadata, indices, and views
- 💪 **Context Manager Support**: Automatic resource cleanup with `with` statements
- ⚡ **Advanced Engine Support**: Pluggable storage engines for custom backends
- 🔐 **Contextual Transactions**: Thread-safe transaction context management

---

## Requirements

- **Python**: 3.10 or higher
- **Dependencies** (installed automatically):
  - `click-shell` - Interactive CLI framework
  - `fastapi` - REST service framework
  - `uvicorn` - ASGI server
  - `tabulate` - Pretty-print database metadata

---

## Installation

### Via Poetry (Recommended)

```bash
poetry add db86
```

### Via pip

```bash
pip install db86
```

### From Source

```bash
git clone https://github.com/anubhav-narayan/db86.git
cd db86
poetry install
```

---

## Quick Start

### Basic Example: Key-Value Storage

```python
from db86 import Database

# Create or open a database
db = Database('./my_db.sqlite', autocommit=True)

# Get a JSON storage table
mydict = db['mytab']

# Store and retrieve data
mydict['some_key'] = {'nested': 'any_picklable_object'}
print(mydict['some_key'])  # {'nested': 'any_picklable_object'}

# Iterate over items
for key, value in mydict.items():
    print(key, value)

# Standard dict operations
print(len(mydict))
if 'some_key' in mydict:
    del mydict['some_key']

# Don't forget to close
mydict.close()
```

### Using Context Manager (Recommended)

```python
from db86 import Database

with Database('./my_db.sqlite', autocommit=True) as db:
    storage = db['data']
    storage['key1'] = 'value1'
    storage['key2'] = {'nested': 'data'}
    # Automatically closes on exit
```

### In-Memory Database

```python
from db86 import Database

db = Database(':memory:')  # No file created
table = db['temp_data', 'json']
table['data'] = [1, 2, 3, 4, 5]
db.close()
```

---

## Usage Guide

### Basic Database Operations

#### Creating and Opening Databases

```python
from db86 import Database

# File-based database (creates if not exists)
db = Database('./data.sqlite', flag='c', autocommit=True)

# Read-only database
db_read = Database('./data.sqlite', flag='r')

# Fresh database (overwrites existing)
db_new = Database('./fresh.sqlite', flag='w')

# In-memory database
db_mem = Database(':memory:')
```

#### Database Configuration Options

```python
db = Database(
    filename='./my_db.sqlite',
    flag='c',                    # 'c'=read/write, 'r'=read-only, 'w'=overwrite
    autocommit=False,            # Auto-save after each operation
    journal_mode='DELETE',       # SQLite journal mode (DELETE, WAL, OFF)
    timeout=5                    # Seconds to wait for thread startup
)
```

#### Inspecting Database Structure

```python
with Database('./db.sqlite') as db:
    # List all tables (storages)
    print(db.storages)  # ['table1', 'table2', 'table3']
    
    # List all indices
    print(db.indices)   # ['idx1', 'idx2']
    
    # List all views
    print(db.views)     # ['view1', 'view2']
    
    # Pretty-print database schema
    print(db.describe())
    
    # Check if table exists
    if 'users' in db:
        print("Users table exists")
```

### JSON Storage

JSON Storage is the default mode for flexible, document-style data storage:

```python
from db86 import Database

db = Database('./db.sqlite')

# Create or access JSON storage (default mode)
users = db['users']  # Equivalent to db['users', 'json']

# Store dictionaries and lists
users['user_001'] = {
    'name': 'Alice',
    'email': 'alice@example.com',
    'tags': ['admin', 'developer']
}

users['user_002'] = {
    'name': 'Bob',
    'email': 'bob@example.com',
    'tags': ['user']
}

# Retrieve data
alice = users['user_001']
print(alice['name'])  # 'Alice'

# List all keys
for user_id in users.keys():
    print(user_id)

# Iterate over key-value pairs
for user_id, user_data in users.items():
    print(f"{user_data['name']} ({user_id})")

# Update data
users['user_001']['tags'].append('reviewer')

# Delete entries
del users['user_002']

# Serialize to dict or JSON
all_data = dict(users)  # Convert to Python dict
db.close()
```

### Tables (Structured Storage)

Tables provide relational storage with columns and schema:

```python
from db86 import Database

db = Database('./db.sqlite')

# Create or access a structured table
products = db['products', 'table']

# Tables work like UserDict with additional schema capabilities
products['prod_001'] = {'name': 'Laptop', 'price': 999.99, 'stock': 5}
products['prod_002'] = {'name': 'Mouse', 'price': 29.99, 'stock': 150}

# Inspect table schema
print(products.describe())    # Pretty-print columns and types
print(products.columns)       # List column names
print(products.xschema)       # Get table definition and SQL

# Access like a dictionary
for prod_id, product in products.items():
    print(f"{product['name']}: ${product['price']}")

db.close()
```

### Database Configuration

#### Autocommit vs Manual Commit

```python
from db86 import Database

# Autocommit (safer, slower)
db_auto = Database('./db.sqlite', autocommit=True)
storage = db_auto['data']
storage['key'] = 'value'  # Automatically saved
db_auto.close()

# Manual commit (faster, requires explicit save)
db_manual = Database('./db.sqlite', autocommit=False)
storage = db_manual['data']
storage['key'] = 'value'
storage['key2'] = 'value2'
db_manual.commit()  # Save all changes at once
db_manual.close()
```

#### Journal Modes

```python
# DELETE (default, safest)
db = Database('./db.sqlite', journal_mode='DELETE')

# WAL (Write-Ahead Logging, good for concurrent access)
db = Database('./db.sqlite', journal_mode='WAL')

# OFF (fastest, risky - disables crash recovery)
db = Database('./db.sqlite', journal_mode='OFF')
```

---

## CLI Tool

DB86 includes an interactive management shell:

```bash
sdbx
```

### Available Commands

```
dbx> create -a -m mydatabase      # Create in-memory database
dbx> list                          # List open databases
dbx> help                           # Show help
dbx> exit                           # Close and exit
```

---

## REST Service

DB86 provides a RESTful API for remote database access.

```python
from db86.service.rest_service import app
import uvicorn

# Run the service
uvicorn.run(app, host="0.0.0.0", port=8000)
```

### Endpoint Overview

- `GET /` — health check and list open databases
- `POST /databases` — create a new database
- `GET /databases` — list open databases
- `GET /databases/{db_name}` — get database metadata
- `DELETE /databases/{db_name}` — close and remove a database
- `POST /databases/{db_name}/storages` — create a storage (json or table)
- `GET /databases/{db_name}/storages` — list storages
- `GET /databases/{db_name}/storages/{storage_name}` — get storage metadata
- `DELETE /databases/{db_name}/storages/{storage_name}` — delete a storage
- `GET /databases/{db_name}/storages/{storage_name}/items` — list storage items
- `GET /databases/{db_name}/storages/{storage_name}/items/{item_key}` — read an item
- `PUT /databases/{db_name}/storages/{storage_name}/items/{item_key}` — create or update an item
- `DELETE /databases/{db_name}/storages/{storage_name}/items/{item_key}` — delete an item
- `GET /databases/{db_name}/storages/{storage_name}/{query:path}` — query JSON storage by nested path

### Example API Calls

```bash
# Create a database
curl -X POST http://localhost:8000/databases \
  -H "Content-Type: application/json" \
  -d '{"name": "mydb", "autocommit": true, "journal_mode": "WAL", "flag": "c"}'

# Create JSON storage
curl -X POST http://localhost:8000/databases/mydb/storages \
  -H "Content-Type: application/json" \
  -d '{"name": "items", "storage_type": "json"}'

# Store an item
curl -X PUT http://localhost:8000/databases/mydb/storages/items/items/123 \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Alice", "email": "alice@example.com"}}'

# Read an item
curl http://localhost:8000/databases/mydb/storages/items/items/123

# Delete an item
curl -X DELETE http://localhost:8000/databases/mydb/storages/items/items/123
```

Access the interactive API documentation at `http://localhost:8000/docs`

## Advanced Examples

### Transactions

```python
from db86 import Database, Transaction

db = Database('./db.sqlite')

# Create a transaction context
with Transaction(db) as txn:
    storage = txn['data']
    storage['key1'] = 'value1'
    storage['key2'] = 'value2'
    # Auto-commits on success, rolls back on error
```

### Working with Multiple Storages

```python
db = Database('./db.sqlite', autocommit=True)

users = db['users', 'json']
orders = db['orders', 'table']
logs = db['logs', 'json']

users['alice'] = {'name': 'Alice', 'join_date': '2024-01-15'}
orders['order_001'] = {'user': 'alice', 'total': 99.99}
logs['entry_001'] = {'action': 'user_created', 'user_id': 'alice'}

db.close()
```

### Large Dataset Handling

```python
db = Database('./large.sqlite', autocommit=False, journal_mode='WAL')
data = db['large_dataset']

# Batch inserts without autocommit (faster)
for i in range(100000):
    data[f'key_{i}'] = {'index': i, 'value': f'value_{i}'}
    
    # Commit every 1000 records
    if i % 1000 == 0:
        db.commit()

db.commit()  # Final commit
db.close()
```

### Read-Only Database Access

```python
# Multiple readers can access simultaneously
db_read1 = Database('./data.sqlite', flag='r')
db_read2 = Database('./data.sqlite', flag='r')

# Read operations
storage1 = db_read1['data']
print(storage1['key'])

storage2 = db_read2['data']
print(storage2['key'])

db_read1.close()
db_read2.close()
```

---

## Testing

Run the test suite:

```bash
poetry run pytest
```

Run specific test categories:

```bash
poetry run pytest tests/ -m unit -v
```

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

### Development Setup

```bash
git clone https://github.com/anubhav-narayan/db86.git
cd db86
poetry install
poetry run pytest tests/ -v
```

---

## License

DB86 is released under the MIT License. See [LICENSE.md](LICENSE.md) for full details.

```
# MIT License

Copyright (c) 2021-2026 Anubhav Mattoo.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

---

## Acknowledgments

- Inspired by and derived from [sqlitedict](https://github.com/RaRe-Technologies/sqlitedict)
- Built with [FastAPI](https://fastapi.tiangolo.com/), [Click](https://click.palletsprojects.com/), and [SQLite3](https://www.sqlite.org/)

---

**Questions?** Open an issue on [GitHub](https://github.com/anubhav-narayan/DB86/issues)