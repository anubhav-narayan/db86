# DB86 — the AE‑86 of databases

![License](https://img.shields.io/badge/licence-MIT-green)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Version](https://img.shields.io/badge/version-0.7.0-brightgreen)

A robust SQLite3 wrapper with dict-like API, multi-threading support, and optional REST service endpoints.

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

- **Dict-like Interface**: Intuitive Python dict API for database operations
- **Multi-threaded**: Thread-safe access with built-in synchronization
- **Dual Storage Modes**: JSON storage for documents, tables for structured data
- **In-Memory & File-Based**: Support for both `:memory:` and persistent databases
- **REST API**: Built-in FastAPI service for remote database access
- **CLI Tools**: Interactive shells for database & REST administration
- **Context Managers**: Automatic resource cleanup with `with` statements
- **Transaction Support**: Thread-safe transaction context management

---

## Requirements

- **Python**: 3.10 or higher

---

## Installation

```bash
pip install db86
# or
poetry add db86
```

---

## Quick Start

```python
from db86 import Database

with Database('./my_db.sqlite', autocommit=True) as db:
    storage = db['users']
    storage['alice'] = {'name': 'Alice', 'email': 'alice@example.com'}
    print(storage['alice'])
    for key, val in storage.items():
        print(key, val)
```

---

## Usage Guide

### Basic Database Operations

```python
from db86 import Database

# File-based (flag: 'c'=read/write, 'r'=read-only, 'w'=overwrite)
db = Database('./data.sqlite', flag='c', autocommit=True)

# In-memory
db_mem = Database(':memory:')

# Inspect structure
print(db.storages)   # List all tables
print(db.indices)    # List indices
print(db.describe()) # Print schema
```

### JSON Storage

Default mode for flexible, document-style data:

```python
db = Database('./db.sqlite')
users = db['users']  # JSON storage

# Store, retrieve, update
users['alice'] = {'name': 'Alice', 'tags': ['admin', 'dev']}
print(users['alice']['name'])

# Iterate and delete
for uid, user in users.items():
    print(f"{user['name']} ({uid})")
del users['alice']

db.close()
```

### Tables (Structured Storage)

Relational storage with schema:

```python
db = Database('./db.sqlite')
products = db['products', 'table']

# Store structured data
products['prod_001'] = {'name': 'Laptop', 'price': 999.99}
products['prod_002'] = {'name': 'Mouse', 'price': 29.99}

# Inspect schema
print(products.describe())
print(products.columns)

# Access like a dictionary
for pid, prod in products.items():
    print(f"{prod['name']}: ${prod['price']}")
```

### Database Configuration

```python
# Autocommit: save after each operation (safer, slower)
db = Database('./db.sqlite', autocommit=True)

# Manual commit: batch saves (faster)
db = Database('./db.sqlite', autocommit=False)
db['data']['key'] = 'value'
db.commit()

# Journal modes: 'DELETE' (default), 'WAL' (concurrent), 'OFF' (fast, risky)
db = Database('./db.sqlite', journal_mode='WAL')
```

---

## CLI Tool

```bash
db86-shell          # Database management shell
db86-restx          # REST service management
db86-server         # Start REST API server
```

---

## REST Service

RESTful API for remote database access:

```bash
db86-server  # Start at http://localhost:8000
```

Core endpoints:
- `GET /` — health check
- `POST /databases` — create database
- `GET /databases/{db_name}/storages/{storage_name}/items/{key}` — read item
- `PUT /databases/{db_name}/storages/{storage_name}/items/{key}` — write item
- `DELETE /databases/{db_name}/storages/{storage_name}/items/{key}` — delete item

Interactive API docs: `http://localhost:8000/docs`

## Advanced Examples

```python
# Transactions
from db86 import Transaction
with Transaction(db) as txn:
    txn['data']['key'] = 'value'
    # Auto-commits on success, rolls back on error

# Multiple storages
users = db['users', 'json']
orders = db['orders', 'table']
users['alice'] = {'name': 'Alice'}
orders['order_001'] = {'user': 'alice'}

# Batch inserts
db = Database('./large.sqlite', autocommit=False, journal_mode='WAL')
for i in range(100000):
    db['data'][f'key_{i}'] = {'index': i}
    if i % 1000 == 0:
        db.commit()
db.commit()
```

---

## Testing

```bash
pytest              # Run all tests
pytest tests/ -v    # Verbose output
```

## Contributing

Contributions welcome! Submit PRs or open issues for bugs and features.

```bash
git clone https://github.com/anubhav-narayan/db86.git
cd db86
poetry install && pytest
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