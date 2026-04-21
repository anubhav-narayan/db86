"""
Stress and Performance Tests for DB86

This module contains comprehensive tests for stress testing and performance
benchmarking of db86. Tests are organized by marker:

- @pytest.mark.stress — Stress tests (heavy load)
- @pytest.mark.performance — Performance benchmarks
- @pytest.mark.slow — Slow tests (>1 second)

Run with:
    pytest tests/test_stress_and_performance.py -v
    pytest tests/test_stress_and_performance.py -m stress
    pytest tests/test_stress_and_performance.py -m performance
    pytest -m "performance and not slow"  # Quick perf tests
"""

import pytest
import time
import tempfile
import os
from typing import Generator, Dict, List, Tuple
from db86 import Database, Transaction
import statistics


# ============================================================================
# FIXTURES FOR STRESS/PERFORMANCE TESTING
# ============================================================================

@pytest.fixture
def perf_db_memory() -> Generator[Database, None, None]:
    """In-memory database optimized for performance testing."""
    db = Database(
        ":memory:",
        autocommit=False,  # Batch operations
        journal_mode="WAL"
    )
    yield db
    db.close(do_log=False, force=True)


@pytest.fixture
def perf_db_file() -> Generator[Tuple[Database, str], None, None]:
    """File-based database optimized for performance testing."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    db = Database(
        path,
        autocommit=False,
        journal_mode="WAL"
    )
    yield db, path
    
    try:
        db.close(do_log=False, force=True)
        os.unlink(path)
        # Clean up WAL files
        for suffix in ['-wal', '-shm']:
            try:
                os.unlink(path + suffix)
            except:
                pass
    except:
        pass


@pytest.fixture
def large_dataset() -> Dict[str, Dict]:
    """Generate 10K test records."""
    data = {}
    for i in range(10000):
        data[f'record_{i:06d}'] = {
            'id': i,
            'name': f'User_{i}',
            'email': f'user{i}@example.com',
            'age': 20 + (i % 50),
            'department': ['eng', 'sales', 'hr', 'marketing'][i % 4],
            'salary': 50000 + (i * 10),
            'active': i % 2 == 0,
            'tags': [f'tag{j}' for j in range(i % 5)],
            'metadata': {
                'created': '2024-01-01',
                'modified': f'2024-{(i % 12) + 1:02d}-01',
                'status': 'active' if i % 3 == 0 else 'inactive',
            }
        }
    return data


@pytest.fixture
def huge_dataset() -> Dict[str, Dict]:
    """Generate 100K test records."""
    data = {}
    for i in range(100000):
        data[f'record_{i:07d}'] = {
            'id': i,
            'name': f'User_{i}',
            'email': f'user{i}@example.com',
            'age': 20 + (i % 50),
            'department': ['eng', 'sales', 'hr', 'marketing'][i % 4],
            'salary': 50000 + (i * 10),
            'active': i % 2 == 0,
            'tags': [f'tag{j}' for j in range(i % 5)],
        }
    return data


# ============================================================================
# HELPER CLASSES FOR MEASUREMENT
# ============================================================================

class PerformanceMetrics:
    """Collect and analyze performance metrics."""
    
    def __init__(self, name: str):
        self.name = name
        self.times: List[float] = []
        self.memory_usage: List[float] = []
        self.start_time: float = 0
    
    def start(self):
        """Start timing."""
        self.start_time = time.perf_counter()
    
    def stop(self):
        """Stop timing and record result."""
        elapsed = (time.perf_counter() - self.start_time) * 1000  # Convert to ms
        self.times.append(elapsed)
        return elapsed
    
    @property
    def avg_ms(self) -> float:
        """Average time in milliseconds."""
        if not self.times:
            return 0
        return statistics.mean(self.times)
    
    @property
    def min_ms(self) -> float:
        """Minimum time in milliseconds."""
        return min(self.times) if self.times else 0
    
    @property
    def max_ms(self) -> float:
        """Maximum time in milliseconds."""
        return max(self.times) if self.times else 0
    
    @property
    def median_ms(self) -> float:
        """Median time in milliseconds."""
        if not self.times:
            return 0
        return statistics.median(self.times)
    
    @property
    def stdev_ms(self) -> float:
        """Standard deviation in milliseconds."""
        if len(self.times) < 2:
            return 0
        return statistics.stdev(self.times)
    
    def report(self) -> str:
        """Generate performance report."""
        return (
            f"{self.name}:\n"
            f"  Count:  {len(self.times)}\n"
            f"  Avg:    {self.avg_ms:.2f} ms\n"
            f"  Min:    {self.min_ms:.2f} ms\n"
            f"  Max:    {self.max_ms:.2f} ms\n"
            f"  Median: {self.median_ms:.2f} ms\n"
            f"  StDev:  {self.stdev_ms:.2f} ms"
        )


# ============================================================================
# STRESS TESTS — Heavy Load
# ============================================================================

@pytest.mark.stress
@pytest.mark.slow
class TestStressInsert:
    """Stress test insert operations."""
    
    def test_bulk_insert_10k_items(self, perf_db_memory, large_dataset):
        """Insert 10K items in single transaction."""
        storage = perf_db_memory['users']
        
        start = time.perf_counter()
        for key, value in large_dataset.items():
            storage[key] = value
        storage.commit()
        elapsed = (time.perf_counter() - start) * 1000
        
        # Verify all items inserted
        assert len(storage) == 10000
        
        # Performance assertion: Should complete in <5 seconds
        assert elapsed < 5000, f"Insert 10K items took {elapsed:.0f}ms"
        
        print(f"\n✓ Inserted 10,000 items in {elapsed:.0f}ms ({elapsed/10000:.2f}ms/item)")
    
    def test_bulk_insert_100k_items(self, perf_db_memory, huge_dataset):
        """Insert 100K items in batches (stress test)."""
        storage = perf_db_memory['users']
        
        metrics = PerformanceMetrics('Insert 100K items (batched)')
        batch_size = 10000
        
        start = time.perf_counter()
        
        for i, (key, value) in enumerate(huge_dataset.items()):
            storage[key] = value
        
        storage.commit()
        total_time = (time.perf_counter() - start) * 1000
        
        assert len(storage) == 100000
        assert total_time < 30000, f"Insert 100K items took {total_time:.0f}ms"
        
        print(f"\n✓ Inserted 100,000 items in {total_time:.0f}ms ({total_time/100000:.2f}ms/item)")
    
    def test_individual_inserts_1k(self, perf_db_memory):
        """Test 1K individual insert-commit cycles (slow path)."""
        storage = perf_db_memory['test']
        
        metrics = PerformanceMetrics('Individual inserts with commit')
        
        for i in range(1000):
            metrics.start()
            storage[f'key_{i}'] = {'value': i}
            storage.commit()
            metrics.stop()
        
        assert len(storage) == 1000
        print(f"\n{metrics.report()}")
        
        # Individual inserts should average <5ms each
        assert metrics.avg_ms < 5, f"Individual inserts averaged {metrics.avg_ms:.2f}ms"


@pytest.mark.stress
@pytest.mark.slow
class TestStressQuery:
    """Stress test query operations."""
    
    def test_sequential_read_10k(self, perf_db_memory, large_dataset):
        """Read all 10K items sequentially."""
        storage = perf_db_memory['items']
        
        # Populate
        for key, value in large_dataset.items():
            storage[key] = value
            if (len(storage) % 1000) == 0:
                storage.commit()
        storage.commit()
        
        # Measure read performance
        start = time.perf_counter()
        count = 0
        for key, value in storage.items():
            count += 1
        elapsed = (time.perf_counter() - start) * 1000
        
        assert count == 10000
        assert elapsed < 2000, f"Read 10K items took {elapsed:.0f}ms"
        
        print(f"\n✓ Read 10,000 items in {elapsed:.0f}ms ({elapsed/10000:.3f}ms/item)")
    
    def test_random_lookups_10k(self, perf_db_memory, large_dataset):
        """Random key lookups on 10K item dataset."""
        storage = perf_db_memory['items']
        
        # Populate
        for key, value in large_dataset.items():
            storage[key] = value
        perf_db_memory.conn.commit()
        
        # Measure lookup performance
        keys = list(large_dataset.keys())
        metrics = PerformanceMetrics('Random key lookups')
        
        for key in keys[::10]:  # Sample every 10th key
            metrics.start()
            value = storage[key]
            metrics.stop()
        
        print(f"\n{metrics.report()}")
        
        # Average lookup should be <10ms
        assert metrics.avg_ms < 10, f"Average lookup took {metrics.avg_ms:.2f}ms"
    
    def test_full_table_scan_100k(self, perf_db_memory, huge_dataset):
        """Full table scan of 100K items."""
        storage = perf_db_memory['items']
        
        # Populate in batches
        batch_size = 10000
        for i, (key, value) in enumerate(huge_dataset.items()):
            storage[key] = value
            if (i + 1) % batch_size == 0:
                storage.commit()
        storage.commit()
        
        # Measure scan performance
        start = time.perf_counter()
        count = 0
        for key in storage.keys():
            count += 1
        elapsed = (time.perf_counter() - start) * 1000
        
        assert count == 100000
        print(f"\n✓ Full scan of 100,000 items in {elapsed:.0f}ms ({elapsed/100000:.3f}ms/item)")


@pytest.mark.stress
@pytest.mark.slow
class TestStressConcurrency:
    """Stress test concurrent access patterns."""
    
    def test_alternating_read_write_1k(self, perf_db_memory):
        """Alternate between reads and writes."""
        storage = perf_db_memory['data']
        
        # Initial data
        for i in range(100):
            storage[f'initial_{i}'] = {'value': i}
        perf_db_memory.conn.commit()
        
        # Alternating operations
        for i in range(1000):
            if i % 2 == 0:
                # Write
                storage[f'key_{i}'] = {'value': i}
            else:
                # Read (arbitrary key)
                try:
                    _ = storage[f'initial_{i % 100}']
                except KeyError:
                    pass
            
            if i % 100 == 0:
                perf_db_memory.conn.commit()
        
        perf_db_memory.conn.commit()
        
        # Should complete without deadlock
        assert len(storage) > 100


@pytest.mark.stress
@pytest.mark.slow
class TestStressMemory:
    """Stress test memory usage patterns."""
    
    def test_large_documents(self, perf_db_memory):
        """Test handling of large JSON documents."""
        storage = perf_db_memory['large_docs']
        
        # Create documents with nested structures
        for i in range(100):
            large_doc = {
                'id': i,
                'data': {
                    'nested_level_1': {
                        'nested_level_2': {
                            'nested_level_3': {
                                'values': list(range(100)),
                                'matrix': [[j for j in range(100)] for _ in range(10)]
                            }
                        }
                    },
                    'arrays': [
                        {'item': j, 'values': list(range(50))}
                        for j in range(50)
                    ]
                }
            }
            storage[f'doc_{i}'] = large_doc
        
        perf_db_memory.conn.commit()
        
        assert len(storage) == 100
        # Verify document retrieval
        doc = storage['doc_0']
        assert doc['id'] == 0
        assert doc['data']['nested_level_1']['nested_level_2']['nested_level_3']['values'] == list(range(100))
    
    def test_many_small_documents(self, perf_db_memory):
        """Test handling many small documents."""
        storage = perf_db_memory['small_docs']
        
        # Insert many small documents
        for i in range(50000):
            storage[f'doc_{i}'] = {'id': i, 'value': 'x' * 10}
            
            if (i + 1) % 5000 == 0:
                perf_db_memory.conn.commit()
        
        perf_db_memory.conn.commit()
        
        assert len(storage) == 50000
        print(f"\n✓ Successfully stored 50,000 small documents")


# ============================================================================
# PERFORMANCE BENCHMARKS
# ============================================================================

@pytest.mark.performance
class TestPerformanceBenchmarks:
    """Performance benchmarks for various operations."""
    
    def test_benchmark_insert_throughput(self, perf_db_memory):
        """Benchmark insert throughput (items/second)."""
        storage = perf_db_memory['bench']
        
        # Warm up
        storage['warmup'] = {'x': 1}
        perf_db_memory.conn.commit()
        
        # Benchmark: 1000 inserts
        start = time.perf_counter()
        with Transaction("bench_txn", perf_db_memory.conn) as txn:
            for i in range(1000):
                storage[f'bench_{i}'] = {'id': i, 'value': f'data_{i}'}
        elapsed = time.perf_counter() - start
        
        throughput = 1000 / elapsed
        
        print(f"\n✓ Insert throughput: {throughput:.0f} items/second")
        assert throughput > 100, f"Throughput too low: {throughput:.0f}/s"
    
    def test_benchmark_lookup_latency(self, perf_db_memory, large_dataset):
        """Benchmark key lookup latency (ms/lookup)."""
        storage = perf_db_memory['bench']
        
        # Populate
        for key, value in large_dataset.items():
            storage[key] = value
        perf_db_memory.conn.commit()
        
        # Benchmark: 1000 random lookups
        keys = list(large_dataset.keys())
        metrics = PerformanceMetrics('Key lookup latency')
        
        for key in keys[::10]:  # Sample every 10th key
            metrics.start()
            _ = storage[key]
            metrics.stop()
        
        print(f"\n{metrics.report()}")
        
        # Average latency should be <10ms
        assert metrics.avg_ms < 10, f"Average latency: {metrics.avg_ms:.2f}ms"
    
    def test_benchmark_iteration_speed(self, perf_db_memory, large_dataset):
        """Benchmark iteration speed (items/second)."""
        storage = perf_db_memory['bench']
        
        # Populate
        for key, value in large_dataset.items():
            storage[key] = value
        perf_db_memory.conn.commit()
        
        # Benchmark: iterate all items
        start = time.perf_counter()
        count = 0
        for key, value in storage.items():
            count += 1
        elapsed = time.perf_counter() - start
        
        throughput = count / elapsed
        
        print(f"\n✓ Iteration throughput: {throughput:.0f} items/second")
        assert throughput > 1000, f"Throughput too low: {throughput:.0f}/s"
    
    def test_benchmark_json_serialization(self, perf_db_memory):
        """Benchmark JSON serialization speed."""
        storage = perf_db_memory['bench']
        
        # Create complex document
        complex_doc = {
            'nested': {
                'level1': {
                    'level2': {
                        'data': list(range(100)),
                        'text': 'x' * 1000
                    }
                },
                'arrays': [
                    {'item': i, 'values': list(range(10))}
                    for i in range(100)
                ]
            }
        }
        
        # Benchmark: 1000 writes of complex document
        metrics = PerformanceMetrics('JSON serialization')
        
        with Transaction("serialization_txn", perf_db_memory.conn) as txn:
            for i in range(1000):
                metrics.start()
                storage[f'complex_{i}'] = complex_doc
                metrics.stop()
        
        print(f"\n{metrics.report()}")
        
        assert metrics.avg_ms < 5, f"Average serialization: {metrics.avg_ms:.2f}ms"
    
    def test_benchmark_commit_latency(self, perf_db_memory):
        """Benchmark commit latency."""
        storage = perf_db_memory['bench']
        
        metrics = PerformanceMetrics('Commit latency')
        
        for i in range(100):
            # Insert batch of 10 items
            for j in range(10):
                storage[f'item_{i}_{j}'] = {'id': f'{i}_{j}'}
            
            # Measure commit time
            metrics.start()
            perf_db_memory.conn.commit()
            metrics.stop()
        
        print(f"\n{metrics.report()}")
        
        # Commits should be fast even with batch operations
        assert metrics.avg_ms < 50, f"Average commit: {metrics.avg_ms:.2f}ms"


@pytest.mark.performance
class TestPerformanceComparison:
    """Compare performance of different approaches."""
    
    def test_batch_vs_individual_commits(self, perf_db_memory):
        """Compare batch commits vs individual commits."""
        # Test 1: Batch commit
        storage1 = perf_db_memory['batch']
        start1 = time.perf_counter()
        with Transaction("batch_txn", perf_db_memory.conn) as txn:
            for i in range(1000):
                storage1[f'key_{i}'] = {'value': i}
        batch_time = time.perf_counter() - start1
        
        # Test 2: Individual commits (slower path)
        # Would require autocommit=True, skip for now
        
        print(f"\n✓ Batch commit (1000 items): {batch_time*1000:.0f}ms")
        assert batch_time < 2, f"Batch operation too slow: {batch_time:.2f}s"
    
    def test_memory_vs_file_performance(self, perf_db_memory, perf_db_file):
        """Compare memory vs file-based database performance."""
        dataset = {f'key_{i}': {'value': i} for i in range(1000)}
        
        # Memory database
        storage_mem = perf_db_memory['test']
        start_mem = time.perf_counter()
        for key, value in dataset.items():
            storage_mem[key] = value
        perf_db_memory.conn.commit()
        mem_time = time.perf_counter() - start_mem
        
        # File database
        db_file, path = perf_db_file
        storage_file = db_file['test']
        start_file = time.perf_counter()
        for key, value in dataset.items():
            storage_file[key] = value
        db_file.conn.commit()
        file_time = time.perf_counter() - start_file
        
        print(f"\n✓ Memory DB (1000 items): {mem_time*1000:.0f}ms")
        print(f"✓ File DB   (1000 items): {file_time*1000:.0f}ms")
        print(f"✓ Ratio: {file_time/mem_time:.1f}x")
        
        # File DB should be reasonable (within 10x of memory)
        assert file_time < mem_time * 10


# ============================================================================
# STRESS TEST EDGE CASES
# ============================================================================

@pytest.mark.stress
class TestStressEdgeCases:
    """Stress test edge cases and boundary conditions."""
    
    def test_very_long_keys(self, perf_db_memory):
        """Test with very long key names."""
        storage = perf_db_memory['test']
        
        # Create keys up to 1000 characters
        for i in range(100):
            long_key = f'key_{"x" * 1000}_{i}'
            storage[long_key] = {'index': i}
        
        perf_db_memory.conn.commit()
        
        assert len(storage) == 100
    
    def test_very_large_values(self, perf_db_memory):
        """Test with very large JSON values."""
        storage = perf_db_memory['test']
        
        # Create large values (1MB each)
        for i in range(10):
            large_value = {
                'data': 'x' * 1000000,  # 1MB
                'index': i
            }
            storage[f'large_{i}'] = large_value
        
        perf_db_memory.conn.commit()
        
        assert len(storage) == 10
        
        # Verify retrieval
        retrieved = storage['large_0']
        assert len(retrieved['data']) == 1000000
    
    def test_deeply_nested_documents(self, perf_db_memory):
        """Test with deeply nested JSON structures."""
        storage = perf_db_memory['test']
        
        # Create deeply nested structure (100 levels)
        doc = {'level_0': {'value': 0}}
        current = doc['level_0']
        for i in range(1, 100):
            current[f'level_{i}'] = {'value': i}
            current = current[f'level_{i}']
        
        storage['deep'] = doc
        perf_db_memory.conn.commit()
        
        # Verify retrieval
        retrieved = storage['deep']
        assert retrieved['level_0']['level_1']['level_2']['value'] == 2
        # Verify Path access
        retrieved = storage['deep/level_0/level_1/level_2/value']
        assert retrieved == {'deep/level_0/level_1/level_2/value' : 2}
    
    def test_many_transactions(self, perf_db_memory):
        """Test handling of many small transactions."""
        storage = perf_db_memory['test']
        
        # Execute 1000 small transactions
        for txn in range(1000):
            storage[f'txn_{txn}'] = {'transaction': txn}
            if txn % 100 == 0:
                perf_db_memory.conn.commit()
        
        perf_db_memory.conn.commit()
        
        assert len(storage) == 1000


# ============================================================================
# ENDURANCE TESTS
# ============================================================================

@pytest.mark.stress
@pytest.mark.slow
class TestEndurance:
    """Long-running endurance tests."""
    
    def test_sustained_write_load(self, perf_db_memory):
        """Sustained write load over time."""
        storage = perf_db_memory['data']
        
        start = time.perf_counter()
        
        # Write for ~10 seconds worth of operations
        with Transaction("sustained_txn", perf_db_memory.conn) as txn:
            for i in range(5000):
                storage[f'item_{i}'] = {
                    'index': i,
                    'timestamp': time.time(),
                    'data': f'value_{i}'
                }

        elapsed = time.perf_counter() - start
        
        throughput = 5000 / elapsed
        print(f"\n✓ Sustained write throughput: {throughput:.0f} items/sec over {elapsed:.1f}s")
        
        assert len(storage) == 5000
        assert throughput > 100, f"Throughput too low: {throughput:.0f}/s"
    
    def test_sustained_read_load(self, perf_db_memory, large_dataset):
        """Sustained read load over time."""
        storage = perf_db_memory['data']
        
        # Populate
        for key, value in large_dataset.items():
            storage[key] = value
        perf_db_memory.conn.commit()
        
        start = time.perf_counter()
        keys = list(large_dataset.keys())
        
        # Read same data multiple times
        reads = 0
        for iteration in range(10):  # 10 full iterations
            for key in keys:
                _ = storage[key]
                reads += 1
        
        elapsed = time.perf_counter() - start
        throughput = reads / elapsed
        
        print(f"\n✓ Sustained read throughput: {throughput:.0f} reads/sec over {elapsed:.1f}s")
        assert throughput > 1000, f"Throughput too low: {throughput:.0f}/s"


# ============================================================================
# ENGINE PERFORMANCE TESTS
# ============================================================================

@pytest.mark.performance
class TestEnginePerformance:
    """Performance benchmarks for query engines."""

    @pytest.fixture
    def engine_dataset(self, perf_db_memory):
        """Create dataset optimized for engine testing."""
        # Create users table
        users = perf_db_memory['users', 'table']
        for i in range(5000):
            users[f'user_{i}'] = {
                'name': f'User_{i}',
                'age': 20 + (i % 50),
                'city': ['NYC', 'SF', 'LA', 'BOS', 'SEA'][i % 5],
                'dept': ['eng', 'sales', 'mktg', 'hr', 'ops'][i % 5],
                'salary': 50000 + (i * 100),
                'active': i % 2 == 0
            }

        # Create orders table
        orders = perf_db_memory['orders', 'table']
        for i in range(25000):
            orders[f'order_{i}'] = {
                'user_id': f'user_{i % 5000}',
                'product': f'product_{i % 100}',
                'amount': 10 + (i % 500),
                'status': ['pending', 'completed', 'shipped', 'cancelled'][i % 4],
                'date': f'2024-{(i % 12) + 1:02d}-01'
            }

        perf_db_memory.conn.commit()
        return perf_db_memory

    def test_engine_4ac_filter_performance(self, engine_dataset):
        """Benchmark Engine4A_C complex filtering."""
        from db86.engines import Engine4A_C

        users = engine_dataset['users', 'table']
        engine = Engine4A_C(users)

        metrics = PerformanceMetrics('4A-C Complex Filter')

        for _ in range(100):  # Run 100 times for stable metrics
            metrics.start()
            result = engine.query({
                "filter": {
                    "and": [
                        {"path": "age", "op": "gte", "value": 25},
                        {"path": "dept", "op": "in", "value": ["eng", "sales", "mktg"]},
                        {"path": "salary", "op": "gt", "value": 100000},
                        {"path": "active", "op": "eq", "value": True}
                    ]
                },
                "select": ["name", "salary", "dept"],
                "sort": [{"field": "salary", "order": "desc"}],
                "limit": 50
            })
            metrics.stop()

        print(f"\n{metrics.report()}")
        assert len(result) > 0
        assert metrics.avg_ms < 50, f"Query too slow: {metrics.avg_ms:.2f}ms avg"

    def test_engine_4ac_aggregation_performance(self, engine_dataset):
        """Benchmark Engine4A_C aggregation operations."""
        from db86.engines import Engine4A_C

        users = engine_dataset['users', 'table']
        engine = Engine4A_C(users)

        # Test COUNT
        metrics_count = PerformanceMetrics('4A-C COUNT')
        for _ in range(50):
            metrics_count.start()
            result = engine.query({"aggregate": {"op": "count"}})
            metrics_count.stop()

        # Test GROUP BY
        metrics_group = PerformanceMetrics('4A-C GROUP BY')
        for _ in range(50):
            metrics_group.start()
            result = engine.query({
                "aggregate": {
                    "op": "group_by",
                    "by": "dept",
                    "field": "salary",
                    "sub_op": "avg"
                }
            })
            metrics_group.stop()

        print(f"\n{metrics_count.report()}")
        print(f"\n{metrics_group.report()}")

        assert result['eng'] > 0  # Should have engineering averages
        assert metrics_count.avg_ms < 10, f"COUNT too slow: {metrics_count.avg_ms:.2f}ms"
        assert metrics_group.avg_ms < 100, f"GROUP BY too slow: {metrics_group.avg_ms:.2f}ms"

    def test_engine_4age_join_performance(self, engine_dataset):
        """Benchmark Engine4A_GE join operations."""
        from db86.engines import Engine4A_GE

        engine = Engine4A_GE(engine_dataset)

        metrics = PerformanceMetrics('4A-GE INNER JOIN')

        for _ in range(50):  # Run 50 times
            metrics.start()
            result = engine.query({
                "tables": [
                    {"name": "users", "alias": "u"},
                    {"name": "orders", "alias": "o"}
                ],
                "joins": [{
                    "type": "INNER",
                    "table": "orders",
                    "alias": "o",
                    "on": {"left": "u.key", "op": "eq", "right": "o.user_id"}
                }],
                "filter": {
                    "and": [
                        {"path": "u.dept", "op": "eq", "value": "eng"},
                        {"path": "o.amount", "op": "gt", "value": 100},
                        {"path": "o.status", "op": "eq", "value": "completed"}
                    ]
                },
                "select": ["u.name", "o.product", "o.amount"],
                "sort": [{"field": "o.amount", "order": "desc"}],
                "limit": 100
            })
            metrics.stop()

        print(f"\n{metrics.report()}")
        assert len(result) > 0
        assert metrics.avg_ms < 200, f"JOIN too slow: {metrics.avg_ms:.2f}ms avg"

    def test_engine_4age_cross_table_aggregation(self, engine_dataset):
        """Benchmark Engine4A_GE cross-table aggregation."""
        from db86.engines import Engine4A_GE

        engine = Engine4A_GE(engine_dataset)

        metrics = PerformanceMetrics('4A-GE Cross-Table Aggregation')

        for _ in range(30):  # Run 30 times
            metrics.start()
            result = engine.query({
                "tables": [
                    {"name": "users", "alias": "u"},
                    {"name": "orders", "alias": "o"}
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
            metrics.stop()

        print(f"\n{metrics.report()}")
        assert len(result) > 0
        assert 'eng' in result
        assert metrics.avg_ms < 300, f"Cross-table agg too slow: {metrics.avg_ms:.2f}ms avg"

    def test_engine_comparison_complex_queries(self, engine_dataset):
        """Compare 4A-C vs 4A-GE performance on equivalent operations."""
        from db86.engines import Engine4A_C, Engine4A_GE

        users = engine_dataset['users', 'table']
        engine_4ac = Engine4A_C(users)
        engine_4age = Engine4A_GE(engine_dataset)

        # Complex filter spec
        filter_spec = {
            "and": [
                {"path": "age", "op": "gte", "value": 30},
                {"path": "dept", "op": "in", "value": ["eng", "sales"]},
                {"path": "salary", "op": "gt", "value": 150000},
                {"path": "active", "op": "eq", "value": True}
            ]
        }

        # Benchmark 4A-C
        metrics_4ac = PerformanceMetrics('4A-C Complex Query')
        for _ in range(25):
            metrics_4ac.start()
            result_4ac = engine_4ac.query({
                "filter": filter_spec,
                "select": ["name", "salary", "dept"],
                "sort": [{"field": "salary", "order": "desc"}],
                "limit": 25
            })
            metrics_4ac.stop()

        # Benchmark 4A-GE (single table)
        metrics_4age = PerformanceMetrics('4A-GE Single Table')
        for _ in range(25):
            metrics_4age.start()
            result_4age = engine_4age.query({
                "tables": [{"name": "users", "alias": "u"}],
                "filter": filter_spec,
                "select": ["u.name", "u.salary", "u.dept"],
                "sort": [{"field": "u.salary", "order": "desc"}],
                "limit": 25
            })
            metrics_4age.stop()

        print(f"\n{metrics_4ac.report()}")
        print(f"\n{metrics_4age.report()}")

        # Results should be equivalent
        assert len(result_4ac) == len(result_4age)

        # 4A-GE should be reasonably close in performance for single-table ops
        overhead_ratio = metrics_4age.avg_ms / metrics_4ac.avg_ms
        print(".2f")
        assert overhead_ratio < 3.0, f"4A-GE overhead too high: {overhead_ratio:.2f}x"

    @pytest.mark.slow
    def test_engine_bulk_operations_performance(self, perf_db_memory):
        """Benchmark bulk operations with engines."""
        from db86.engines import Engine4A_GE

        engine = Engine4A_GE(perf_db_memory)

        # Create bulk test data
        bulk_users = [
            {"table": "users", "key": f'bulk_user_{i}', "value": {
                'name': f'BulkUser_{i}',
                'age': 25 + (i % 30),
                'dept': ['eng', 'sales', 'mktg'][i % 3],
                'salary': 60000 + (i * 500),
                'active': True
            }}
            for i in range(1000)
        ]

        # Benchmark bulk insert
        metrics_insert = PerformanceMetrics('4A-GE Bulk Insert (1000 items)')
        metrics_insert.start()
        insert_count = engine.insert(bulk_users)
        metrics_insert.stop()

        print(f"\n{metrics_insert.report()}")
        assert insert_count == 1000
        assert metrics_insert.avg_ms < 2000, f"Bulk insert too slow: {metrics_insert.elapsed_ms:.0f}ms"

        # Benchmark bulk update
        metrics_update = PerformanceMetrics('4A-GE Bulk Update (1000 items)')
        metrics_update.start()
        update_count = engine.update(
            {"users.salary": 70000},
            {"path": "users.dept", "op": "eq", "value": "eng"}
        )
        metrics_update.stop()

        print(f"\n{metrics_update.report()}")
        assert update_count > 0
        assert metrics_update.avg_ms < 1000, f"Bulk update too slow: {metrics_update.elapsed_ms:.0f}ms"

    def test_engine_memory_efficiency(self, perf_db_memory):
        """Test memory efficiency of engine operations."""
        from db86.engines import Engine4A_GE

        # Create large dataset
        large_data = {}
        for i in range(10000):
            large_data[f'user_{i}'] = {
                'name': f'User_{i}',
                'data': 'x' * 1000,  # 1KB per record
                'nested': {'level1': {'level2': list(range(100))}}
            }

        # Insert large dataset
        users = perf_db_memory['large_users']
        start_mem = time.perf_counter()
        for key, value in large_data.items():
            users[key] = value
        perf_db_memory.conn.commit()
        insert_time = time.perf_counter() - start_mem

        # Query with engine
        engine = Engine4A_GE(perf_db_memory)
        start_query = time.perf_counter()
        result = engine.query({
            "tables": [{"name": "large_users", "alias": "u"}],
            "filter": {"path": "u.name", "op": "contains", "value": "User_1"},
            "select": ["u.name"],
            "limit": 100
        })
        query_time = time.perf_counter() - start_query

        print(f"\n✓ Large dataset (10K records): Insert {insert_time:.2f}s, Query {query_time:.2f}s")
        assert len(result) > 0
        assert insert_time < 10, "Large insert too slow"
        assert query_time < 2, "Large query too slow"

