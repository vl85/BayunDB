# BayunDB Testing Guide

This guide provides comprehensive information about testing BayunDB, including test organization, running tests, writing new tests, and testing best practices.

## Table of Contents

- [Test Organization](#test-organization)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
  - [Performance Tests](#performance-tests)
- [Test Utilities](#test-utilities)
- [Mocking and Fixtures](#mocking-and-fixtures)
- [Continuous Integration](#continuous-integration)
- [Testing Best Practices](#testing-best-practices)

## Test Organization

BayunDB's test suite is structured as follows:

```
tests/                      # Integration and unit tests
├── integration/            # End-to-end integration tests
│   ├── parser_test.rs      # SQL parser integration tests
│   ├── planner_test.rs     # Query planner tests
│   ├── join_test.rs        # Join operation tests
│   ├── execution_test.rs   # Query execution tests
│   └── recovery_test.rs    # Recovery process tests
├── buffer_pool_test.rs     # Buffer pool manager unit tests
├── page_manager_test.rs    # Page layout and management unit tests
├── btree_index_test.rs     # B+Tree index unit tests
├── database_test.rs        # End-to-end DB integration tests
└── common/                 # Shared test utilities
    └── mod.rs              # Common functions for test setup

benches/                    # Performance benchmarks
├── buffer_pool_bench.rs    # Buffer pool performance tests
├── btree_bench.rs          # B+Tree operation benchmarks
├── transaction_bench.rs    # Transaction operation benchmarks
├── query_parser_bench.rs   # Query parser benchmarks
├── query_execution_bench.rs # Query execution benchmarks
├── table_operations_bench.rs # Table operation benchmarks
└── recovery_bench.rs       # Recovery performance benchmarks
```

Tests are categorized into:

- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test components working together
- **Performance tests**: Measure and track performance metrics

## Running Tests

### Basic Test Execution

Run all tests with:

```bash
cargo test
```

Run a specific test file:

```bash
cargo test --test buffer_pool_test
```

Run a specific test function:

```bash
cargo test test_buffer_pool_create_page
```

Run tests with specific features:

```bash
cargo test --features="metrics"
```

### Running Integration Tests

Integration tests validate how components work together:

```bash
cargo test --test integration
```

### Running Performance Benchmarks

Performance benchmarks use the Criterion.rs framework:

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench buffer_pool_bench

# Run with HTML report generation
cargo bench -- --output-format=bencher

# Run a specific benchmark function
cargo bench --bench buffer_pool_bench sequential_access
```

### Test Output Options

Control test output verbosity:

```bash
# Show output from tests (including passed tests)
cargo test -- --nocapture

# Show test progress
cargo test -- --show-output

# Run tests in parallel (default)
cargo test

# Run tests serially
cargo test -- --test-threads=1
```

## Writing Tests

### Unit Tests

Unit tests verify individual components in isolation. They should be:
- Fast
- Independent
- Focused on a single unit of functionality

Example unit test structure:

```rust
// In src/storage/buffer/tests.rs or tests/buffer_pool_test.rs
use crate::storage::buffer::BufferPoolManager;

#[test]
fn test_buffer_pool_create_page() -> Result<()> {
    // Setup
    let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
    
    // Execute operation
    let (page, page_id) = buffer_pool.new_page()?;
    
    // Verify results
    assert!(page_id > 0);
    assert!(buffer_pool.pin_count(page_id)? == 1);
    
    // Cleanup
    buffer_pool.unpin_page(page_id, false)?;
    
    Ok(())
}
```

### Integration Tests

Integration tests verify how components work together, focusing on complete workflows:

```rust
// In tests/integration/execution_test.rs
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::query::executor::engine::ExecutionEngine;

#[test]
fn test_query_execution_with_btree_lookup() -> Result<()> {
    // Setup test environment
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
    let page_manager = PageManager::new();
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    
    // Insert test data
    setup_test_data(&buffer_pool, &page_manager, &btree)?;
    
    // Create and execute test query
    let engine = ExecutionEngine::new();
    let query = "SELECT id, name FROM test_table WHERE id = 5";
    let result = engine.execute_query(query)?;
    
    // Verify results
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("id")?, 5);
    
    Ok(())
}
```

### Performance Tests

Performance benchmarks measure execution time and throughput:

```rust
// In benches/buffer_pool_bench.rs
use criterion::{criterion_group, criterion_main, Criterion};
use bayundb::storage::buffer::BufferPoolManager;

fn sequential_access(c: &mut Criterion) {
    let (buffer_pool, _temp_file) = create_test_buffer_pool(100).unwrap();
    
    // Create test pages
    let page_ids: Vec<_> = (0..50)
        .map(|_| buffer_pool.new_page().unwrap().1)
        .collect();
    
    // Benchmark sequential access
    c.bench_function("buffer_pool_sequential_100", |b| {
        b.iter(|| {
            for page_id in &page_ids {
                let page = buffer_pool.fetch_page(*page_id).unwrap();
                let _ = page.read();
                buffer_pool.unpin_page(*page_id, false).unwrap();
            }
        })
    });
}

criterion_group!(benches, sequential_access);
criterion_main!(benches);
```

## Test Utilities

BayunDB provides common test utilities in `tests/common/mod.rs`:

```rust
// Create a buffer pool with a temporary database file
pub fn create_test_buffer_pool(size: usize) -> Result<(Arc<BufferPoolManager>, NamedTempFile)> {
    let temp_file = NamedTempFile::new()?;
    let buffer_pool = Arc::new(BufferPoolManager::new(size, temp_file.path().to_str().unwrap())?);
    Ok((buffer_pool, temp_file))
}

// Generate deterministic test data
pub fn generate_test_data(size: usize) -> Vec<Vec<u8>> {
    (0..size)
        .map(|i| {
            let data = format!("test-data-{}", i);
            data.as_bytes().to_vec()
        })
        .collect()
}

// Set up a test table with specified records
pub fn setup_test_table(
    buffer_pool: &Arc<BufferPoolManager>, 
    record_count: usize
) -> Result<(PageId, Vec<RecordId>)> {
    // Implementation...
}
```

## Mocking and Fixtures

### Creating Test Fixtures

For complex test environments, create fixtures that can be reused:

```rust
pub struct TestDatabase {
    pub buffer_pool: Arc<BufferPoolManager>,
    pub page_manager: PageManager,
    pub btree_index: BTreeIndex<i32>,
    pub temp_file: NamedTempFile,
}

impl TestDatabase {
    pub fn new() -> Result<Self> {
        let (buffer_pool, temp_file) = create_test_buffer_pool(100)?;
        let page_manager = PageManager::new();
        let btree_index = BTreeIndex::<i32>::new(buffer_pool.clone())?;
        
        Ok(Self {
            buffer_pool,
            page_manager,
            btree_index,
            temp_file,
        })
    }
    
    pub fn with_sample_data(record_count: usize) -> Result<Self> {
        let db = Self::new()?;
        // Insert sample data
        // ...
        Ok(db)
    }
}
```

### Mocking Components

For unit testing, mock dependencies:

```rust
// Mock buffer pool for testing B+Tree operations in isolation
struct MockBufferPool {
    pages: HashMap<PageId, Vec<u8>>,
    next_id: AtomicU64,
}

impl BufferPoolAPI for MockBufferPool {
    fn new_page(&self) -> Result<(Arc<RwLock<Page>>, PageId)> {
        // Implementation...
    }
    
    fn fetch_page(&self, page_id: PageId) -> Result<Arc<RwLock<Page>>> {
        // Implementation...
    }
    
    // Other required methods...
}
```

## Continuous Integration

All tests run in CI when changes are pushed:

1. **Unit and integration tests**:
   - Run automatically on pull requests
   - Must pass before merging

2. **Performance benchmarks**:
   - Run on scheduled intervals
   - Generate reports to track performance changes
   - Alert on significant performance regressions

## Testing Best Practices

### General Best Practices

1. **Test one thing per test**:
   - Focus each test on verifying a single behavior
   - Use clear test names that describe what's being tested

2. **Use setup and teardown properly**:
   - Create test fixtures for common setup
   - Clean up resources (even if tests fail) using Rust's `Drop` trait

3. **Make tests deterministic**:
   - Avoid random data that can cause flaky tests
   - Use seed values when randomness is required
   - Control external dependencies like time and file system

### BayunDB-Specific Best Practices

1. **Buffer pool testing**:
   - Test page creation, fetching, pinning, and eviction
   - Verify LRU policy works correctly under pressure
   - Test concurrent access patterns

2. **B+Tree testing**:
   - Test key operations: insertion, lookup, range scans, deletion
   - Verify tree balancing with large data sets
   - Test edge cases: duplicate keys, empty trees, etc.

3. **Transaction testing**:
   - Verify ACID properties
   - Test concurrent transactions with different isolation levels
   - Test recovery after simulated crashes

4. **Query execution testing**:
   - Test query parsing accuracy
   - Verify execution plan generation
   - Test complex queries with joins, filters, etc.

### Performance Testing Guidelines

1. **Establish baselines**:
   - Document expected performance characteristics
   - Compare against baselines when running benchmarks

2. **Test with realistic workloads**:
   - Create benchmarks that mimic real-world usage
   - Vary data sizes and access patterns

3. **Monitor key metrics**:
   - Throughput (operations per second)
   - Latency (average and percentiles)
   - Resource usage (memory, CPU, disk I/O)

4. **Performance regression analysis**:
   - Track benchmark results over time
   - Investigate significant changes in performance

## Troubleshooting Common Test Issues

### Debugging Test Failures

1. **Run specific failing tests with verbose output**:
   ```bash
   cargo test test_name -- --nocapture
   ```

2. **Use logging for detailed information**:
   ```rust
   env_logger::init();
   log::debug!("Page ID: {}", page_id);
   ```

3. **Isolate failing components**:
   - Replace real implementations with mocks
   - Simplify test case to minimal reproduction

### Resolving Common Issues

1. **Resource leaks**:
   - Ensure proper cleanup in tests (file handles, etc.)
   - Use Rust's `Drop` trait for automatic resource cleanup

2. **Race conditions**:
   - Add synchronization points for concurrent tests
   - Use deterministic approaches instead of relying on timing

3. **Disk space issues**:
   - Clean up temporary files after tests
   - Use smaller data sets for regular testing 