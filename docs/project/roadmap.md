# BayunDB Development Roadmap

This document outlines the planned features, improvements, and long-term vision for BayunDB.

## Current Status (as of 2024-07-28)

*Replace this section with a summary of the current state of the project, key completed features, and any known major limitations.*

### Core Features Implemented:

*   SQL Parsing (SELECT, CREATE TABLE, INSERT, UPDATE, DELETE)
*   Logical and Physical Query Planning
*   Query Execution Engine (Iterator Model)
    *   Operators: TableScan, Filter, Projection, NestedLoopJoin, HashJoin, HashAggregate
*   Catalog Management (Schema, Tables)
*   Storage Engine:
    *   Page-based storage
    *   Buffer Pool Manager (LRU eviction)
    *   Disk Manager
*   B+Tree Indexing (Basic operations)
*   Write-Ahead Logging (WAL) for durability (Basic operations)
*   Basic Recovery Mechanism
*   Command-Line Interface (`bnql`)
*   [x] Implement `ALTER TABLE` statements (ADD COLUMN, DROP COLUMN, RENAME COLUMN) in parser, catalog, and execution engine.
*   [x] Integration tests for ALTER TABLE execution, including row-level data migration verification.
*   [x] Phase 6: Comprehensive debugging and fixing of compiler warnings.
*   [x] Aggregation Operators: Full support for GROUP BY, HAVING, and aggregate functions (COUNT, SUM, AVG, MIN, MAX) with both hash and sort-based aggregation, including cost model integration and performance tests.
*   [x] Query Optimizer Improvements: Rule-based predicate pushdown for filters across joins, with robust test coverage.

### Known Limitations:

*   Limited SQL dialect support (e.g., no complex subqueries, advanced JOIN types, window functions).
*   Simple query optimizer; no advanced cost-based optimization yet.
*   Limited data type support.
*   No comprehensive concurrency control (e.g., full transaction isolation levels beyond basic locking).
*   Basic error handling and reporting in some areas.
*   Performance characteristics not yet fully optimized across all components.
*   No network server for remote client connections.

## Near-Term Goals (Next 3-6 Months)

**Priorities:**
- Broaden SQL support and data types
- Improve query performance and reliability
- Expand testing and documentation

1.  **Enhanced SQL Support:**
    *   [ ] Support for more `JOIN` types (e.g., `OUTER JOIN`).
    *   [ ] Basic subquery support in `WHERE` clauses.
    *   [x] Comprehensive `GROUP BY` and `HAVING` clause functionality (DONE)
    *   [ ] Add support for more data types (e.g., `DATE`, `TIMESTAMP`, `DECIMAL`).
    *   [ ] Support default values for new columns in ALTER TABLE ADD COLUMN.
    *   [x] Support column type changes (ALTER COLUMN ... TYPE ...).
    *   [ ] More complex data migration scenarios (multiple rows, non-null constraints, etc).
2.  **Query Optimizer Enhancements:**
    *   [ ] Introduce a basic cost-based optimizer.
    *   [x] Implement rule-based optimizations (e.g., predicate pushdown, join reordering) (DONE for predicate pushdown)
3.  **Improved Transaction Management:**
    *   [ ] Implement stricter transaction isolation levels (e.g., Serializable Snapshot Isolation or Two-Phase Locking).
    *   [ ] Robust deadlock detection and resolution.
4.  **Testing & Reliability:**
    *   [x] Expand integration test coverage, especially for aggregation, joins, and new SQL features (DONE for aggregation and joins)
    *   [ ] Implement more comprehensive stress testing.
    *   [ ] Set up regular benchmarking and performance tracking.
5.  **Documentation & Community:**
    *   [x] Add more tutorials and example applications for aggregation and grouping
    *   [ ] Create a `CONTRIBUTING.md` and issue templates to encourage community contributions.

## Mid-Term Goals (6-12 Months)

*   **Network Protocol & Server:** Design and implement a basic server to allow remote client connections.
*   **Advanced Indexing:** Explore other index types (e.g., Hash Index, GiST for extensibility).
*   **User-Defined Functions (UDFs):** Basic support for UDFs.
*   **Views:** Implement SQL Views.
*   **Security:** Basic authentication and authorization mechanisms.

## Long-Term Vision (12+ Months)

*   Highly performant and scalable SQL database competitive with existing open-source solutions.
*   Extensible architecture allowing for new storage engines, index types, and features.
*   Potential for distributed query processing capabilities.
*   Strong community and adoption.

## How to Contribute

Interested in contributing? Please check the [Contribution Guidelines](CONTRIBUTING.md) (or link to the relevant section in `documentation_guide.md` or `DEVELOPMENT.md`) and the open issues on the project tracker.

---
*This roadmap is a living document and will be updated regularly to reflect the project's progress and priorities.* 