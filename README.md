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
  - [Documentation Guide](docs/development/documentation_guide.md): Documentation standards and practices
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

## Testing and Benchmarks

BayunDB employs a comprehensive testing and benchmarking strategy to ensure correctness, robustness, and performance. This includes unit tests, integration tests, and performance benchmarks using Criterion.rs.

For detailed information on test organization, how to run tests and benchmarks, guidelines for writing new tests, and best practices, please refer to the **[BayunDB Testing Guide](docs/development/testing.md)**.

## License

[MIT](/LICENSE) 