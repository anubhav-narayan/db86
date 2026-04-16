from typing import Generator
import pytest
from db86 import Database, Transaction
from db86.storages import JSONStorage, Table

@pytest.fixture
def mem_db() -> Generator[Database, None, None]:
    """In-memory db86 Database, closed after each test."""
    db = Database(":memory:", autocommit=True, journal_mode="WAL")
    yield db
    db.close(do_log=False, force=True)


@pytest.mark.unit
class TestDatabase:
    """Database container — creation, metadata, and flag handling."""
 
    def test_in_memory_creates_without_error(self):
        db = Database(":memory:")
        db.close(do_log=False, force=True)
 
    def test_contains_false_before_storage_creation(self, mem_db):
        assert "nosuch" not in mem_db
 
    def test_contains_true_after_storage_creation(self, mem_db):
        mem_db["things", "json"]
        assert "things" in mem_db
 
    def test_storages_property_lists_tables(self, mem_db):
        mem_db["alpha", "json"]
        mem_db["beta",  "json"]
        assert set(mem_db.storages) >= {"alpha", "beta"}
 
    def test_indices_property_is_list(self, mem_db):
        assert isinstance(mem_db.indices, list)
 
    def test_views_property_is_list(self, mem_db):
        assert isinstance(mem_db.views, list)
 
    def test_str_includes_filename(self, mem_db):
        assert ":memory:" in str(mem_db)
 
    def test_repr_equals_str(self, mem_db):
        assert repr(mem_db) == str(mem_db)
 
    def test_describe_returns_string(self, mem_db):
        mem_db["t", "json"]
        result = mem_db.describe()
        assert isinstance(result, str)
        assert len(result) > 0
 
    def test_context_manager_closes_on_exit(self):
        with Database(":memory:") as db:
            db["t", "json"]
        assert db.conn is None
 
    def test_invalid_flag_raises(self):
        with pytest.raises(RuntimeError, match="Unrecognized flag"):
            Database(":memory:", flag="x")
 
    def test_getitem_json_type(self, mem_db):
        st = mem_db["stuff", "json"]
        assert isinstance(st, JSONStorage)
 
    def test_getitem_table_type(self, mem_db):
        tbl = mem_db["rows", "table"]
        assert isinstance(tbl, Table)

    def test_transaction_context_manager_suppresses_autocommit(self):
        db = Database(":memory:", autocommit=True, journal_mode="WAL")
        storage = db["txn_test", "json"]
        assert db.conn.transaction_depth == 0

        with Transaction("txn", db.conn):
            assert db.conn.transaction_depth == 1
            storage["a"] = {"value": 1}
            assert db.conn.transaction_depth == 1
            assert storage["a"]["value"] == 1

        assert db.conn.transaction_depth == 0
        assert storage["a"]["value"] == 1
        db.close(do_log=False, force=True)

    def test_transaction_context_manager_rolls_back_on_error(self):
        db = Database(":memory:", autocommit=True, journal_mode="WAL")
        storage = db["txn_rollback"]

        with pytest.raises(RuntimeError, match="boom"):
            with Transaction("txn", db.conn):
                assert db.conn.transaction_depth > 0
                storage["a"] = {"value": 1}
                assert "a" in storage # Sync to DB, essential for rollback test
                raise RuntimeError("boom")

        assert db.conn.transaction_depth == 0
        assert "a" not in storage
        db.close(do_log=False, force=True)
    
    def test_transaction_context_manager_savepoint(self):
        db = Database(":memory:", autocommit=False, journal_mode="WAL")
        storage = db["txn_rollback"]

        with pytest.raises(RuntimeError, match="boom_2"):
            # storage.commit()
            with Transaction("txn", db.conn) as txn:
                try:
                    storage["a"] = {"value": 1}
                    txn.savepoint("sp1")
                    storage["a"] = {"value": 2}
                    raise RuntimeError("boom")
                except RuntimeError:
                    txn.rollback_to("sp1")
                assert storage["a"]["value"] == 1
                storage["b"] = {"value": 3}
                raise RuntimeError("boom_2")
                

        assert db.conn.transaction_depth == 0
        assert "a" not in storage
        assert "b" not in storage
        db.close(do_log=False, force=True)
    
    def test_transaction_context_manager_commit(self):
        db = Database(":memory:", autocommit=False, journal_mode="WAL")
        storage = db["txn_rollback"]

        storage.commit()
        with Transaction("txn", db.conn, "IMMEDIATE") as txn:
            try:
                storage["a"] = {"value": 1}
                txn.savepoint("sp1")
                storage["a"] = {"value": 2}
            except Exception:
                txn.rollback_to("sp1")
            txn.release("sp1")
            storage["b"] = {"value": 3}

        assert db.conn.transaction_depth == 0
        assert "a" in storage
        assert storage["a"]["value"] == 2
        assert "b" in storage
        assert storage["b"]["value"] == 3
        db.close(do_log=False, force=True)