# BayunDB Development Roadmap

This document outlines the planned features, improvements, and long-term vision for BayunDB.

## Current Status (as of YYYY-MM-DD)

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

### Known Limitations:

*   Limited SQL dialect support (e.g., no complex subqueries, advanced JOIN types, window functions).
*   Simple query optimizer; no advanced cost-based optimization yet.
*   Limited data type support.
*   No comprehensive concurrency control (e.g., full transaction isolation levels beyond basic locking).
*   Basic error handling and reporting in some areas.
*   Performance characteristics not yet fully optimized across all components.
*   No network server for remote client connections.

## Near-Term Goals (Next 3-6 Months)

*Replace this section with specific, actionable goals for the near future. Be as concrete as possible.* 

1.  **Enhanced SQL Support:**
    *   [ ] Implement `ALTER TABLE` statements.
    *   [ ] Support for more `JOIN` types (e.g., `OUTER JOIN`).
    *   [ ] Basic subquery support in `WHERE` clauses.
    *   [ ] More comprehensive `GROUP BY` and `HAVING` clause functionality.
2.  **Improved Transaction Management:**
    *   [ ] Implement stricter transaction isolation levels (e.g., Serializable Snapshot Isolation or Two-Phase Locking).
    *   [ ] Robust deadlock detection and resolution.
3.  **Query Optimizer Enhancements:**
    *   [ ] Introduce a more capable cost model.
    *   [ ] Implement basic rule-based optimizations (e.g., predicate pushdown).
4.  **Storage & Performance:**
    *   [ ] Improve B+Tree concurrency and performance.
    *   [ ] Investigate and implement more efficient page layouts or compression for specific data types.
    *   [ ] Add support for more data types (e.g., `DATE`, `TIMESTAMP`, `DECIMAL`).
5.  **Testing & Reliability:**
    *   [ ] Expand integration test coverage, especially for transactions and recovery.
    *   [ ] Implement more comprehensive stress testing.
    *   [ ] Set up regular benchmarking and performance tracking.

## Mid-Term Goals (6-12 Months)

*Replace this section with broader goals for the medium term.*

*   **Network Protocol & Server:** Design and implement a basic server to allow remote client connections.
*   **Advanced Indexing:** Explore other index types (e.g., Hash Index, GiST for extensibility).
*   **User-Defined Functions (UDFs):** Basic support for UDFs.
*   **Views:** Implement SQL Views.
*   **Security:** Basic authentication and authorization mechanisms.

## Long-Term Vision (12+ Months)

*Replace this section with the aspirational, long-term vision for BayunDB.*

*   Highly performant and scalable SQL database competitive with existing open-source solutions.
*   Extensible architecture allowing for new storage engines, index types, and features.
*   Potential for distributed query processing capabilities.
*   Strong community and adoption.

## How to Contribute

Interested in contributing? Please check the [Contribution Guidelines](CONTRIBUTING.md) (or link to the relevant section in `documentation_guide.md` or `DEVELOPMENT.md`) and the open issues on the project tracker.

---
*This roadmap is a living document and will be updated regularly to reflect the project's progress and priorities.* 