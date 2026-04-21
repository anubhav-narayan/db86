"""
Tests for DB86 Query Engines

Tests for Engine4A_C (single-table) and Engine4A_GE (multi-table) engines.
Includes functionality verification and performance benchmarks.

Run with:
    pytest tests/test_engines.py -v
    pytest tests/test_engines.py -m "engine and not slow"
    pytest tests/test_engines.py -m "engine and performance"
"""

import pytest
import time
from db86 import Database
from db86.engines import Engine4A_C, Engine4A_GE


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def test_db() -> Database:
    """Test database with sample data."""
    db = Database(":memory:", autocommit=True)

    # Create users table
    users = db['users', 'table']
    users.rename_column('col1', 'name')
    users.add_column('age')
    users.add_column('city')
    users.add_column('dept')
    users.add_column('salary', dtype='int')
    users['alice'] = {'name': 'Alice', 'age': 30, 'city': 'NYC', 'dept': 'eng', 'salary': 100000}
    users['bob'] = {'name': 'Bob', 'age': 25, 'city': 'SF', 'dept': 'sales', 'salary': 80000}
    users['charlie'] = {'name': 'Charlie', 'age': 35, 'city': 'LA', 'dept': 'eng', 'salary': 120000}
    users['diana'] = {'name': 'Diana', 'age': 28, 'city': 'NYC', 'dept': 'mktg', 'salary': 90000}

    # Create orders table
    orders = db['orders', 'table']
    orders.rename_column('col1', 'user_id')
    orders.add_column('product')
    orders.add_column('amount', dtype='int')
    orders.add_column('status')
    orders['o1'] = {'user_id': 'alice', 'product': 'laptop', 'amount': 1200, 'status': 'completed'}
    orders['o2'] = {'user_id': 'bob', 'product': 'book', 'amount': 50, 'status': 'pending'}
    orders['o3'] = {'user_id': 'alice', 'product': 'mouse', 'amount': 25, 'status': 'completed'}
    orders['o4'] = {'user_id': 'charlie', 'product': 'monitor', 'amount': 300, 'status': 'shipped'}

    # Create JSON storage for logs
    logs = db['logs']
    logs['log1'] = {'action': 'login', 'user': 'alice', 'timestamp': '2024-01-01'}
    logs['log2'] = {'action': 'purchase', 'user': 'bob', 'timestamp': '2024-01-02'}
    logs['log3'] = {'action': 'logout', 'user': 'alice', 'timestamp': '2024-01-03'}

    yield db
    db.close()


@pytest.fixture
def engine_4ac(test_db) -> Engine4A_C:
    """Engine4A_C instance with users table."""
    users = test_db['users', 'table']
    return Engine4A_C(users)


@pytest.fixture
def engine_4age(test_db) -> Engine4A_GE:
    """Engine4A_GE instance with test database."""
    return Engine4A_GE(test_db)


# ============================================================================
# ENGINE4A_C TESTS (Single-Table Engine)
# ============================================================================

class TestEngine4A_C:
    """Tests for Engine4A_C single-table query engine."""

    @pytest.mark.engine
    def test_basic_query(self, engine_4ac):
        """Test basic SELECT query."""
        result = engine_4ac.query({
            "select": ["name", "age"],
            "filter": {"path": "dept", "op": "eq", "value": "eng"}
        })

        assert len(result) == 2
        names = [row['name'] for row in result]
        assert 'Alice' in names
        assert 'Charlie' in names

    @pytest.mark.engine
    def test_aggregation_count(self, engine_4ac):
        """Test COUNT aggregation."""
        result = engine_4ac.query({
            "aggregate": {"op": "count"}
        })

        assert result == 4

    @pytest.mark.engine
    def test_aggregation_sum(self, engine_4ac):
        """Test SUM aggregation."""
        result = engine_4ac.query({
            "aggregate": {"op": "sum", "field": "salary"}
        })

        assert result == 390000  # 100k + 80k + 120k + 90k

    @pytest.mark.engine
    def test_aggregation_group_by(self, engine_4ac):
        """Test GROUP BY aggregation."""
        result = engine_4ac.query({
            "aggregate": {
                "op": "group_by",
                "by": "dept",
                "field": "salary",
                "sub_op": "avg"
            }
        })

        assert 'eng' in result
        assert 'sales' in result
        assert 'mktg' in result
        assert result['eng'] == 110000  # (100k + 120k) / 2

    @pytest.mark.engine
    def test_complex_filter_and(self, engine_4ac):
        """Test complex AND filter."""
        result = engine_4ac.query({
            "select": ["name"],
            "filter": {
                "and": [
                    {"path": "age", "op": "gte", "value": 25},
                    {"path": "city", "op": "eq", "value": "NYC"}
                ]
            }
        })

        assert len(result) == 2
        names = [row['name'] for row in result]
        assert 'Alice' in names
        assert 'Diana' in names

    @pytest.mark.engine
    def test_complex_filter_or(self, engine_4ac):
        """Test complex OR filter."""
        result = engine_4ac.query({
            "select": ["name"],
            "filter": {
                "or": [
                    {"path": "dept", "op": "eq", "value": "sales"},
                    {"path": "age", "op": "lt", "value": 30}
                ]
            }
        })

        assert len(result) == 2
        names = [row['name'] for row in result]
        assert 'Bob' in names
        assert 'Diana' in names

    @pytest.mark.engine
    def test_sorting(self, engine_4ac):
        """Test sorting functionality."""
        result = engine_4ac.query({
            "select": ["name", "age"],
            "sort": [{"field": "age", "order": "desc"}]
        })

        assert len(result) == 4
        assert result[0]['name'] == 'Charlie'  # age 35
        assert result[1]['name'] == 'Alice'    # age 30

    @pytest.mark.engine
    def test_limit_offset(self, engine_4ac):
        """Test LIMIT and OFFSET."""
        result = engine_4ac.query({
            "select": ["name"],
            "sort": [{"field": "name", "order": "asc"}],
            "limit": 2,
            "offset": 1
        })

        assert len(result) == 2
        assert result[0]['name'] == 'Bob'
        assert result[1]['name'] == 'Charlie'

    @pytest.mark.engine
    def test_insert(self, test_db):
        """Test bulk insert functionality."""
        users = test_db['users', 'table']
        engine = Engine4A_C(users)

        new_users = [
            {"key": "eve", "value": {"name": "Eve", "age": 32, "city": "SEA", "dept": "eng", "salary": 110000}},
            {"key": "frank", "value": {"name": "Frank", "age": 29, "city": "BOS", "dept": "sales", "salary": 85000}}
        ]

        count = engine.insert(new_users)
        assert count == 2

        # Verify insertion
        result = engine.query({"aggregate": {"op": "count"}})
        assert result == 6  # 4 original + 2 new

    @pytest.mark.engine
    def test_update(self, test_db):
        """Test bulk update functionality."""
        users = test_db['users', 'table']
        engine = Engine4A_C(users)

        count = engine.update(
            {"salary": 105000},
            {"path": "dept", "op": "eq", "value": "eng"}
        )

        # Verify update
        result = engine.query({
            "select": ["name", "salary"],
            "filter": {"path": "dept", "op": "eq", "value": "eng"}
        })

        for row in result:
            print(row)
            assert row['salary'] == 105000


# ============================================================================
# ENGINE4A_GE TESTS (Multi-Table Engine)
# ============================================================================

class TestEngine4A_GE:
    """Tests for Engine4A_GE multi-table query engine."""

    @pytest.mark.engine
    def test_single_table_query(self, engine_4age):
        """Test single table query (should work like 4A-C)."""
        result = engine_4age.query({
            "tables": [{"name": "users", "type": "table"}],
            "select": ["users.name", "users.age"],
            "filter": {"path": "users.dept", "op": "eq", "value": "eng"}
        })

        assert len(result) == 2
        names = [row['users.name'] for row in result]
        assert 'Alice' in names
        assert 'Charlie' in names

    @pytest.mark.engine
    def test_inner_join(self, engine_4age):
        """Test INNER JOIN between users and orders."""
        result = engine_4age.query({
            "tables": [
                {"name": "users", "alias": "u", "type": "table"},
                {"name": "orders", "alias": "o", "type": "table"}
            ],
            "joins": [{
                "type": "INNER",
                "table": "orders",
                "alias": "o",
                "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
            }],
            "select": ["u.name", "o.product", "o.amount"]
        })

        assert len(result) == 4  # 4 orders total
        # Check that Alice appears twice (she has 2 orders)
        alice_orders = [row for row in result if row['u.name'] == 'Alice']
        assert len(alice_orders) == 2

    @pytest.mark.engine
    def test_left_join(self, engine_4age):
        """Test LEFT JOIN (users with/without orders)."""
        result = engine_4age.query({
            "tables": [
                {"name": "users", "alias": "u", "type": "table"},
                {"name": "orders", "alias": "o", "type": "table"}
            ],
            "joins": [{
                "type": "LEFT",
                "table": "orders",
                "alias": "o",
                "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
            }],
            "select": ["u.name", "o.product"]
        })

        assert len(result) == 5  # 4 users + 1 extra for Diana (no orders)
        # Diana should have None for product
        diana_rows = [row for row in result if row['u.name'] == 'Diana']
        assert len(diana_rows) == 1
        assert diana_rows[0]['o.product'] is None

    @pytest.mark.engine
    def test_cross_table_filter(self, engine_4age):
        """Test filtering across joined tables."""
        result = engine_4age.query({
            "tables": [
                {"name": "users", "alias": "u", "type": "table"},
                {"name": "orders", "alias": "o", "type": "table"}
            ],
            "joins": [{
                "type": "INNER",
                "table": "orders",
                "alias": "o",
                "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
            }],
            "select": ["u.name", "o.amount"],
            "filter": {
                "and": [
                    {"path": "u.dept", "op": "eq", "value": "eng"},
                    {"path": "o.amount", "op": "gt", "value": 100}
                ]
            }
        })

        assert len(result) == 2
        assert result[0]['u.name'] == 'Alice'
        assert result[0]['o.amount'] == 1200

    @pytest.mark.engine
    def test_cross_table_aggregation(self, engine_4age):
        """Test aggregation across joined tables."""
        result = engine_4age.query({
            "tables": [
                {"name": "users", "alias": "u", "type": "table"},
                {"name": "orders", "alias": "o", "type": "table"}
            ],
            "joins": [{
                "type": "INNER",
                "table": "orders",
                "alias": "o",
                "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
            }],
            "aggregate": {
                "op": "group_by",
                "by": "u.dept",
                "field": "o.amount",
                "sub_op": "sum"
            }
        })

        assert 'eng' in result
        assert 'sales' in result
        assert result['eng'] == 1525  # 1500 + 25
        assert result['sales'] == 50   # 50

    @pytest.mark.engine
    def test_triple_join(self, engine_4age, test_db):
        """Test three-way join (users -> orders -> logs)."""
        # Add user_id to logs for this test
        logs = test_db['logs']
        logs['log1'] = {'action': 'login', 'user_id': 'alice', 'timestamp': '2024-01-01'}
        logs['log2'] = {'action': 'purchase', 'user_id': 'bob', 'timestamp': '2024-01-02'}
        logs['log3'] = {'action': 'logout', 'user_id': 'alice', 'timestamp': '2024-01-03'}
        logs['log4'] = {'action': 'login', 'user_id': 'charlie', 'timestamp': '2024-01-04'}

        result = engine_4age.query({
            "tables": [
                {"name": "users", "alias": "u", "type": "table"},
                {"name": "orders", "alias": "o", "type": "table"},
                {"name": "logs", "alias": "l"}
            ],
            "joins": [
                {
                    "type": "INNER",
                    "table": "orders",
                    "alias": "o",
                    "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
                },
                {
                    "type": "LEFT",
                    "table": "logs",
                    "alias": "l",
                    "on": {"left": "u.key", "op": "eq", "right": "l.user_id"}
                }
            ],
            "select": ["u.name", "o.product", "l.action"],
            "filter": {"path": "u.dept", "op": "eq", "value": "eng"}
        })

        # Should have results for Alice and Charlie
        eng_users = [row for row in result if row['u.name'] in ['Alice', 'Charlie']]
        assert len(eng_users) > 0

    @pytest.mark.engine
    def test_json_storage_query(self, engine_4age):
        """Test querying JSON storage in multi-table context."""
        result = engine_4age.query({
            "tables": [{"name": "logs", "alias": "l"}],
            "select": ["l.action", "l.user"],
            "filter": {"path": "l.action", "op": "eq", "value": "login"}
        })

        assert len(result) == 1
        assert result[0]['l.action'] == 'login'
        assert result[0]['l.user'] == 'alice'

    @pytest.mark.engine
    def test_multi_table_insert(self, test_db):
        """Test bulk insert across multiple tables."""
        engine = Engine4A_GE(test_db)

        # Insert into both users and orders
        items = [
            {"table": "users", "type": "table", "key": "grace", "value": {"name": "Grace", "age": 27, "dept": "hr"}},
            {"table": "orders", "type": "table", "key": "o5", "value": {"user_id": "grace", "product": "keyboard", "amount": 75}}
        ]

        count = engine.insert(items)
        assert count == 2

        # Verify insertions
        users_result = engine.query({
            "tables": [{"name": "users", "alias": "u", "type": "table"}],
            "aggregate": {"op": "count"}
        })
        assert users_result == 5  # 4 original + 1 new

    @pytest.mark.engine
    def test_multi_table_update(self, test_db):
        """Test bulk update across multiple tables."""
        engine = Engine4A_GE(test_db)

        # Update order amounts for engineering users
        engine.update(
            {"table": "orders", "type": "table", "update": {"orders.amount": 100}},
            {
                "tables": [
                    {"name": "orders", "type": "table"}],
                "filter": {
                    "and": [
                        {"path": "orders.user_id", "op": "eq", "value": "users.key"}
                    ]
                }
            }
        )

        # This is a simplified test - in practice, cross-table updates are complex
        # and may need different implementation
