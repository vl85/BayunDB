# BayunDB

A custom database management system written in Rust, focusing on classic database architecture with B+Tree based indexing.

## Project Structure

The project follows a modular architecture with clear separation of concerns:

```
src/
├── common/         # Common types and utilities
├── storage/        # Storage management
│   ├── page/       # Page layout and management
│   ├── buffer/     # Buffer pool management
│   ├── disk/       # Disk I/O operations
├── index/          # Index structures
│   ├── btree/      # B+Tree implementation
├── catalog/        # (Future) Schema management
├── transaction/    # (Future) Transaction management
├── query/          # (Future) Query processing
```

## Key Components

- **Page Manager**: Manages the internal layout of database pages, including record insertion, deletion, and compaction.
- **Buffer Pool Manager**: Handles caching of database pages in memory, with LRU replacement policy.
- **B+Tree Index**: Provides efficient key-based lookups and range scans.

## Building and Running

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run the database
cargo run
```

## Testing

BayunDB includes comprehensive test suites to verify correctness and functionality:

### Test Organization

The test suite is organized as follows:

```
tests/
├── buffer_pool_test.rs   # Buffer pool manager unit tests
├── page_manager_test.rs  # Page layout and management unit tests
├── btree_index_test.rs   # B+Tree index unit tests
├── database_test.rs      # End-to-end integration tests
└── common/               # Shared test utilities
    └── mod.rs            # Common functions for test environment setup
```

### Unit Tests

Individual components are tested in isolation:

- **Page Manager Tests**: Tests for page initialization, record operations (insert/update/delete), and page compaction
- **Buffer Pool Tests**: Tests for page creation, fetching, eviction, and flushing operations
- **B+Tree Index Tests**: Tests for index creation, key insertion, lookup, range scans, and deletion

### Integration Tests

The `tests/database_test.rs` file contains end-to-end integration tests that validate the entire database stack working together:

1. **Data Flow**: Tests storing records in pages, indexing them with B+Trees, and retrieving them via lookups
2. **CRUD Operations**: Tests creating, reading, updating, and deleting records through the full database stack
3. **Persistence**: Validates that changes are correctly persisted to disk

The integration test demonstrates:
- Creating pages using the buffer pool
- Storing records in pages using the page manager
- Indexing records with B+Tree indices
- Retrieving records via index lookups
- Updating and deleting both indexed data and page records
- Ensuring data consistency across the entire stack

### Common Test Utilities

The `tests/common` module provides shared utilities to simplify test setup:

- `create_test_buffer_pool()`: Creates a buffer pool with a temporary database file
- `generate_test_data()`: Generates deterministic test data of specified size

### Writing New Tests

When adding new features to BayunDB, you should include both unit tests and integration tests:

1. **Unit Tests**: Create or extend test files for individual components
   ```rust
   #[test]
   fn test_my_new_feature() -> Result<()> {
       // Use common utilities for test setup
       let (buffer_pool, _temp_file) = create_test_buffer_pool(10)?;
       
       // Test the feature
       // ...
       
       Ok(())
   }
   ```

2. **Integration Tests**: Add test cases to `database_test.rs` or create new integration test files
   ```rust
   #[test]
   fn test_my_feature_integration() -> Result<()> {
       // Set up the complete database stack
       let (buffer_pool, _temp_file) = create_test_buffer_pool(100)?;
       let page_manager = PageManager::new();
       let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
       
       // Test end-to-end functionality
       // ...
       
       Ok(())
   }
   ```

Run all tests with:

```bash
# Run all tests
cargo test

# Run a specific test
cargo test test_database_end_to_end
```

## Benchmarks

BayunDB includes performance benchmarks using Criterion.rs to measure and track component performance.

### Buffer Pool Manager Benchmarks

The buffer pool benchmarks measure page access patterns and analyze the effectiveness of the LRU caching strategy:

- **Sequential Access**: Tests accessing pages in sequential order with different buffer pool sizes (10, 100, 1000 pages)
- **Random Access**: Tests random page access patterns with different buffer pool sizes

Run the benchmarks:

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench buffer_pool_bench

# Run with HTML report generation
cargo bench -- --output-format=bencher
```

Benchmark results show performance characteristics under different access patterns:

- The LRU replacement policy provides better performance for sequential access with larger buffer pools
- Random access patterns benefit from larger buffer pools but show higher latency compared to sequential patterns when the working set exceeds buffer size
- Performance scales linearly with buffer pool size for workloads that fit in the buffer pool

### Benchmark Implementation Details

The benchmarks are implemented in the `benches/` directory:

```
benches/
└── buffer_pool_bench.rs    # Buffer pool access pattern benchmarks
```

Each benchmark:
1. Initializes a test environment with a temporary database
2. Creates and populates pages with test data
3. Measures performance of repeated operations
4. Reports statistics on operation latency

### Analyzing Results

Criterion generates HTML reports in `target/criterion/` with:
- Detailed statistical analysis
- Performance comparisons between runs
- Visualization of performance distributions

Look for:
- **Mean execution time**: The average time per operation
- **Throughput**: Operations per second
- **Consistency**: Standard deviation and outliers

### Adding Custom Benchmarks

To add your own benchmarks:

1. Create a new file in the `benches/` directory
2. Configure Cargo.toml with a new benchmark target:
   ```toml
   [[bench]]
   name = "my_new_benchmark"
   harness = false
   ```
3. Implement your benchmark using the Criterion framework
4. Run with `cargo bench --bench my_new_benchmark`

For realistic database workloads, consider implementing:
- B+Tree index performance benchmarks
- Insert/update/delete operation benchmarks
- Full-scan vs. indexed scan comparisons

## License

[MIT](/LICENSE) 