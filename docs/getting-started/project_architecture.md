# BayunDB Project Architecture & Code Organization

This document outlines the high-level architecture of BayunDB and the organization of its codebase.

## 1. Architectural Layers

BayunDB is designed with a layered architecture, promoting separation of concerns and modularity:

*   **SQL Processing Layer:** Responsible for parsing, planning, and executing SQL queries.
*   **Data Management Layer:** Handles schema, data storage, indexing, and access.
*   **Transaction and Recovery Layer:** Ensures atomicity, consistency, isolation, and durability (ACID properties).

## 2. Code Organization (Project Structure Guideline)

The project follows a standard Rust project layout.

*   **`Cargo.toml` & `Cargo.lock`:** Manage project metadata, dependencies, and build configurations.
*   **`README.md` (root):** Provides an overview of the project, setup instructions, and usage examples.
*   **`src/`:** Contains all core library and binary source code.
    *   **`lib.rs`:** Root of the BayunDB library crate.
    *   **`bin/cli.rs`:** Defines the `bnql` command-line interface binary.
    *   **Major Component Directories:**
        *   **`catalog/`:** Manages metadata and schema information (tables, columns, types).
            *   `schema.rs`: Defines table and schema structures.
            *   `table.rs`: Table metadata.
            *   `validation.rs`: Data type and constraint validation.
            *   `mod.rs`: Module exports.
        *   **`index/`:** Implements indexing structures for efficient data retrieval.
            *   `btree/`: B+Tree implementation (node management, operations, serialization).
            *   `mod.rs`: Module exports.
        *   **`query/`:** Handles all aspects of SQL query processing.
            *   `parser/`: Lexes SQL strings into tokens and parses them into an Abstract Syntax Tree (AST).
                *   `ast.rs`: Defines AST node types.
                *   `lexer.rs`: Tokenizer.
                *   `parser.rs`: Main parsing logic.
                *   `components/`: Sub-parsers for different SQL statement parts (DDL, DML, expressions, SELECT clauses).
            *   `planner/`: Converts the AST into logical and then physical query plans.
                *   `logical.rs`: Logical plan representation.
                *   `physical_plan.rs`: Physical plan representation.
                *   `physical.rs`: Logic for converting logical to physical plans.
                *   `operator_builder.rs`: Converts physical plans into an executable operator tree.
                *   `cost_model.rs`, `physical_optimizer.rs`: (Future/WIP) Query optimization logic.
            *   `executor/`: Executes the physical query plan.
                *   `engine.rs`: `ExecutionEngine` to run the operator tree.
                *   `operators/`: Concrete implementations of query operators (Scan, Filter, Project, Join, Aggregate) following the iterator model.
                *   `result.rs`: Defines `Row` and `DataValue` for query results.
        *   **`storage/`:** Manages low-level data storage, page I/O, and memory buffering.
            *   `buffer/`: Buffer pool manager (`manager.rs`, `frame.rs`) for caching pages.
            *   `disk/manager.rs`: Handles reading and writing pages to disk.
            *   `page/`: Page layout (`layout.rs`) and record management within pages (`page_manager.rs`).
        *   **`transaction/`:** Manages transactions, concurrency, and crash recovery.
            *   `wal/`: Write-Ahead Logging implementation.
                *   `log_record.rs`: Defines log record types.
                *   `log_manager.rs`: Core log manager.
                *   `log_buffer.rs`: In-memory buffer for log records.
                *   `log_components/`: Helpers like `log_file_manager.rs`, `log_iterator.rs`.
            *   `recovery.rs`: `RecoveryManager` for restoring database state.
            *   `mod.rs`: Module exports.
    *   **Unit Tests:** Co-located within each module using `#[cfg(test)] mod tests { ... }`.
*   **`tests/`:** Contains integration tests that test interactions between multiple components.
    *   **`common/mod.rs`:** Shared helper functions, data setup, and utilities for integration tests.
    *   Test files are typically named after the primary component or functionality they test (e.g., `execution_test.rs`, `recovery_test.rs`).
*   **`examples/`:** Contains runnable example programs demonstrating the usage of the BayunDB library.
*   **`benches/`:** Contains performance benchmark tests, typically using Criterion.rs.
*   **`docs/`:** Contains project documentation, including this structure document, design documents, and tutorials.
    *   `DOCUMENTATION_STRUCTURE.md`: Outlines the overall organization of the documentation.
*   **`scripts/`:** Helper scripts for development, building, or running (e.g., `run_cli.sh`).

## 3. Key Architectural Concepts

*   **Iterator Model (Volcano Model):** Query operators implement an `next()` interface, allowing data to be pulled through the query plan one row or batch at a time.
*   **Shared Catalog:** A central `Catalog` component, accessed via `Arc<RwLock<Catalog>>`, provides schema information to various parts of the system.
*   **Write-Ahead Logging (WAL):** Ensures data durability and supports recovery from crashes.
*   **Buffer Pool Management:** Manages caching of disk pages in memory to reduce I/O.

This document provides a snapshot of the current architecture and organization. It will be updated as the project evolves. 