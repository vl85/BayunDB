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
├── catalog/        # Schema management
├── transaction/    # Transaction management
├── query/          # Query processing
```

## Documentation

Comprehensive documentation is available in the `docs/` directory, organized by component:

- **[Overview and Organization](docs/README.md)**: Documentation structure and main topics
- **Component Documentation**:
  - [Storage Engine](docs/components/storage/): Page management, buffer pool, disk I/O
  - [Index Structures](docs/components/index/): B+Tree implementation details
  - [Transaction Management](docs/components/transaction/): WAL, recovery, concurrency
  - [Query Processing](docs/components/query/): Parser, planner, execution
- **Project Information**:
  - [Development Roadmap](docs/project/roadmap.md): Completed features and future plans
  - [Documentation Guide](docs/project/documentation_guide.md): Documentation standards and practices
  - [Design Decisions](docs/project/): Key architectural choices and rationales

## Key Components

- **Page Manager**: Manages the internal layout of database pages, including record insertion, deletion, and compaction.
- **Buffer Pool Manager**: Handles caching of database pages in memory, with LRU replacement policy.
- **B+Tree Index**: Provides efficient key-based lookups and range scans.
- **Write-Ahead Log (WAL)**: Ensures durability of transactions through logging.

## Building and Running

```bash
# Build the project
cargo build

# Run tests
cargo test

# Run the database
cargo run
```

## Command-Line Interface

BayunDB comes with a command-line interface for interacting with the database directly.

### Installation

The CLI is built along with the main project:

```bash
# Build the CLI
cargo build --bin bnql
```

### Usage

```bash
# Start an interactive shell
./target/debug/bnql

# Specify a database file
./target/debug/bnql --db-path my_database.db

# Execute a query directly
./target/debug/bnql query "SELECT * FROM users"

# Show database information
./target/debug/bnql info

# Helper scripts
# On Unix/Linux/macOS:
./scripts/run_cli.sh

# On Windows:
scripts\run_cli.bat
```

### Available Commands

- `shell` - Start an interactive SQL shell (default if no command specified)
- `query <sql>` - Execute a SQL query and display results
- `create` - Create a new database
- `info` - Display database information

### CLI Options

- `--db-path <path>` - Path to the database file (default: `database.db`)
- `--log-dir <path>` - Directory for WAL log files (default: `logs`)
- `--buffer-size <size>` - Buffer pool size in pages (default: 1000)

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

BayunDB includes performance benchmarks using Criterion.rs to measure and track component performance across key subsystems.

### Benchmark Types

The benchmarks are implemented in the `benches/` directory:

```
benches/
├── buffer_pool_bench.rs    # Buffer pool access pattern benchmarks
├── btree_bench.rs          # B+Tree operation benchmarks
├── transaction_bench.rs    # Transaction operation benchmarks
└── recovery_bench.rs       # WAL and recovery performance benchmarks
```

Each benchmark measures different aspects of the system:

- **Buffer Pool**: Tests the effectiveness of the LRU caching strategy with sequential and random access patterns using various buffer pool sizes (10, 100, 1000 pages)
- **B+Tree Index**: Measures performance of key operations (insertion, lookup, range scans) with different data distributions
- **Transaction and Recovery**:
  - WAL throughput with different operation counts and sync settings
  - Recovery time with varying workload sizes
  - Checkpoint overhead with different log sizes
  - Recovery performance with varying checkpoint frequencies

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench buffer_pool_bench
cargo bench --bench recovery_bench

# Run with HTML report generation
cargo bench -- --output-format=bencher
```

### Analyzing Results

Criterion generates HTML reports in `target/criterion/` that include detailed statistical analysis and performance comparisons. Key metrics to look for:

- **Mean execution time**: The average time per operation
- **Throughput**: Operations per second
- **Consistency**: Standard deviation and outliers

Performance characteristics vary across components:
- Buffer pool performance depends on access patterns and cache size
- WAL performance is significantly affected by synchronous flushing settings
- Recovery time improves with checkpoint frequency (with diminishing returns)
- Transaction throughput varies based on logging and checkpoint frequency

### Creating Custom Benchmarks

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

## License

[MIT](/LICENSE) 