# SQLite Comparison Benchmarks

This directory contains benchmarks that compare BayunDB with SQLite, focusing on feature parity and performance.

## Overview

The goal of these benchmarks is to:

1. Verify BayunDB correctly implements SQLite's most commonly used features
2. Compare performance between BayunDB and SQLite for key operations
3. Establish a baseline for ongoing performance tracking
4. Identify areas for optimization

## Benchmark Structure

The benchmarks are organized as follows:

- `mod.rs`: Core benchmark framework and utilities
- `basic_operations.rs`: Basic CRUD operations benchmarks
- Additional benchmark files for specific feature areas
- `reports/`: Generated benchmark results and visualizations

## Running the Benchmarks

To run all SQLite comparison benchmarks:

```bash
cargo bench --bench sqlite_comparison_bench
```

To run the full benchmark suite with report generation:

```bash
./scripts/run_sqlite_comparison.sh
```

## Benchmark Categories

### Basic Operations

- Table creation and population
- Simple SELECT queries
- SELECT with WHERE conditions
- Simple aggregate queries

### Additional Categories (Planned)

- Joins (2-table, 3-table, complex joins)
- Complex aggregations
- Transaction performance
- Index utilization
- Data modification operations

## Output Format

Benchmarks generate two types of output:

1. **JSON Results**: Raw benchmark data saved to `reports/*.json`
2. **Markdown Reports**: Human-readable reports with charts in `reports/*.md`

## Dependencies

The benchmarks require:

- Rust with Cargo
- SQLite (via the `rusqlite` crate)
- Python with matplotlib and pandas (for chart generation)

## Adding New Benchmarks

To add a new benchmark:

1. Create a new file `benches/sqlite_comparison/your_benchmark.rs`
2. Import the framework: `use super::*;`
3. Implement benchmark functions using the provided utilities
4. Add your benchmark to `sqlite_comparison_bench.rs`

## Embedding in Applications

The framework can also be used to benchmark embedded usage patterns by simulating application code that uses both BayunDB and SQLite. 